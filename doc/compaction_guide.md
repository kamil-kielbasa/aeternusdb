# Minor Compaction — Size-Tiered, Bucket-Based (Implementation Guide)

## 1. What Is Minor Compaction?

Minor compaction takes a group of similarly-sized SSTables and merges them into a single, larger SSTable. The goal is to:

- Reduce the total number of SSTables (faster reads — fewer files to search).
- Consolidate duplicate keys (keep only the latest version).
- Drop expired tombstones when safe.

This is the standard compaction path that runs during normal database operation.

---

## 2. Bucketing — How SSTables Are Grouped

Size-Tiered Compaction Strategy (STCS) groups SSTables into **buckets** by file size. A bucket is a set of SSTables whose sizes are "close enough" to each other.

### 2.1 Algorithm

Given all current SSTables, bucketing works as follows:

1. **Sort** all SSTables by file size (ascending).

2. **Compute the average size** across all SSTables.

3. **Assign to buckets:**
   - SSTables smaller than `min_sstable_size` go into a special **"small" bucket** (bucket 0).
   - For remaining SSTables, group them so that within each bucket, every SSTable's size falls within `[avg × bucket_low, avg × bucket_high]` of the bucket's own average size.

### 2.2 Practical Bucketing Approach

Rather than a single global average, we use an **iterative grouping** strategy:

```
function bucket_sstables(sstables, config) -> Vec<Bucket>:
    sort sstables by file_size ascending

    small_bucket = []
    regular = []

    for each sst in sstables:
        if sst.file_size < config.min_sstable_size:
            small_bucket.push(sst)
        else:
            regular.push(sst)

    buckets = []
    if small_bucket is not empty:
        buckets.push(small_bucket)

    current_bucket = []
    current_avg = 0.0

    for each sst in regular:
        if current_bucket is empty:
            current_bucket.push(sst)
            current_avg = sst.file_size
        else:
            low  = current_avg * config.bucket_low
            high = current_avg * config.bucket_high
            if sst.file_size >= low AND sst.file_size <= high:
                current_bucket.push(sst)
                current_avg = average size of current_bucket
            else:
                buckets.push(current_bucket)
                current_bucket = [sst]
                current_avg = sst.file_size

    if current_bucket is not empty:
        buckets.push(current_bucket)

    return buckets
```

### 2.3 Example

Config: `min_sstable_size = 50 MB`, `bucket_low = 0.5`, `bucket_high = 1.5`

SSTables on disk (sorted by size):
```
  10 KB, 12 KB, 15 KB      → small bucket (all < 50 MB)
  52 MB, 55 MB, 60 MB      → bucket A  (avg ~55 MB, range [27.5, 82.5])
  110 MB, 120 MB            → bucket B  (avg ~115 MB, range [57.5, 172.5])
  400 MB                    → bucket C  (singleton)
```

### 2.4 Relevant Config Parameters

| Parameter         | Your Default | Purpose                                              |
|-------------------|-------------|------------------------------------------------------|
| `bucket_low`      | 0.5         | Lower multiplier: `avg × 0.5` = bucket floor        |
| `bucket_high`     | 1.5         | Upper multiplier: `avg × 1.5` = bucket ceiling      |
| `min_sstable_size`| 50 MB       | SSTables smaller than this go to the small bucket    |

---

## 3. Trigger — When Does Compaction Fire?

A bucket is eligible for compaction when:

```
bucket.len() >= config.min_threshold    (default: 4)
```

That's the only trigger for minor compaction. We check this condition **after every memtable flush** (since a flush is the only operation that creates a new SSTable in the current architecture).

### 3.1 Where Exactly in the Code?

Currently, `flush_frozen_to_sstable_inner()` in [src/engine/mod.rs](src/engine/mod.rs) creates a new SSTable and adds it to the manifest. Right after that succeeds, we should call the compaction check:

```
flush_frozen_to_sstable_inner(inner)?;
// ↑ SSTable now exists in inner.sstables and manifest

maybe_compact(inner)?;
// ↑ Check if any bucket now has >= min_threshold SSTables
```

Since we're doing inline (non-threaded) compaction, `maybe_compact` runs synchronously and blocks the caller until compaction is done.

### 3.2 Relevant Config Parameters

| Parameter       | Your Default | Purpose                                        |
|-----------------|-------------|------------------------------------------------|
| `min_threshold` | 4           | Min SSTables in a bucket to trigger compaction |
| `max_threshold` | 32          | Max SSTables to compact at once                |

---

## 4. Selection — Which SSTables to Compact?

Once a bucket triggers (≥ `min_threshold` SSTables):

1. **Select** up to `max_threshold` SSTables from that bucket.
2. If multiple buckets trigger, pick the bucket with the **most SSTables** (to get the best compaction ratio).

```
function select_for_compaction(buckets, config) -> Option<Vec<SSTable>>:
    best_bucket = None
    best_count  = 0

    for bucket in buckets:
        if bucket.len() >= config.min_threshold:
            if bucket.len() > best_count:
                best_bucket = Some(bucket)
                best_count  = bucket.len()

    if best_bucket is None:
        return None    // nothing to compact

    selected = best_bucket.take(config.max_threshold)  // first N by size
    return Some(selected)
```

---

## 5. Execution — The Merge

This is the core of compaction. We take the selected SSTables and produce a single new SSTable.

### 5.1 High-Level Steps

```
function execute_compaction(inner, selected_sstables) -> Result:
    // 1. Create a merge iterator over all selected SSTables
    //    - Each SSTable contributes a scan iterator (full range)
    //    - Use the existing EngineScanIterator (min-heap merge)
    //      to yield records in (key ASC, LSN DESC) order

    // 2. Deduplicate and resolve versions
    //    - For each unique key, keep only the latest version (highest LSN)
    //    - If the latest version is a tombstone:
    //        → Keep it (tombstone expiry is a separate concern)
    //    - If the latest version is a Put:
    //        → Keep the Put, discard older versions

    // 3. Build a new SSTable from the merged, deduplicated stream
    //    - Use the existing build_from_iterators()

    // 4. Atomically update the manifest:
    //    - Add the new SSTable
    //    - Remove all old (compacted) SSTables

    // 5. Delete old SSTable files from disk

    // 6. Update the in-memory sstables list
```

### 5.2 Merge Iterator Detail

We already have `EngineScanIterator` which does a min-heap merge of multiple `Record` iterators, yielding records in `(key ASC, LSN DESC)` order. For compaction, we use the same mechanism but with a **full key-range scan** (`start_key = b""`, `end_key = b"\xff\xff\xff..."`) over each selected SSTable.

```
function create_merge_iterator(selected_sstables) -> EngineScanIterator:
    iters = []
    for sst in selected_sstables:
        // Scan the entire key range of this SSTable
        let records: Vec<Record> = sst.scan(b"", b"\xff\xff\xff\xff")?.collect()
        iters.push(Box::new(records.into_iter()))
    return EngineScanIterator::new(iters)
```

**Important**: The scan must use a start key that is before all possible keys and an end key that is after all possible keys. Since SSTable `scan()` requires `start_key < end_key`, we can use the actual min/max key bounds from the selected SSTables' properties:

```
let min_key = selected.iter().map(|s| &s.properties.min_key).min()
let max_key = selected.iter().map(|s| &s.properties.max_key).max()
// extend max_key by one byte (0xFF) to make it exclusive
```

### 5.3 Deduplication Logic

The merge iterator yields records in `(key ASC, LSN DESC)` order. We process them with a simple dedup filter:

```
function dedup_merge(merge_iter) -> Vec<MemtablePointEntry / MemtableRangeTombstone>:
    point_entries = []
    range_tombstones = []
    last_key: Option<Vec<u8>> = None

    for record in merge_iter:
        match record:
            RangeDelete { start, end, lsn, timestamp }:
                range_tombstones.push(MemtableRangeTombstone { start, end, lsn, timestamp })

            Put { key, value, lsn, timestamp }:
                if last_key == Some(key):
                    continue           // older version → skip
                last_key = Some(key.clone())
                point_entries.push(MemtablePointEntry {
                    key, value: Some(value), lsn, timestamp
                })

            Delete { key, lsn, timestamp }:
                if last_key == Some(key):
                    continue           // older version → skip
                last_key = Some(key.clone())
                point_entries.push(MemtablePointEntry {
                    key, value: None, lsn, timestamp
                })

    return (point_entries, range_tombstones)
```

**Why keep tombstones?** Tombstones must survive compaction until they have been propagated to all SSTables that might contain the deleted key. In a size-tiered strategy, we're only compacting a subset of SSTables (one bucket), so older SSTables in other buckets may still reference the deleted key. Dropping the tombstone would cause the old data to "resurrect." The architecture document mentions a `gc_grace_seconds` TTL mechanism for safe tombstone removal — that is a separate feature (tombstone-specific compaction, Scenario 2).

### 5.4 Building the Output SSTable

We already have `build_from_iterators()` which takes point entries and range tombstones and writes a complete, checksummed SSTable atomically. We reuse it directly:

```
let new_sst_id = next_sstable_id(inner);
let new_sst_path = format!("{}/sstables/sstable-{}.sst", inner.data_dir, new_sst_id);

build_from_iterators(
    &new_sst_path,
    point_entries.len(),
    point_entries.into_iter(),
    range_tombstones.len(),
    range_tombstones.into_iter(),
)?;
```

### 5.5 Manifest Update (Atomic Swap)

After the new SSTable is written and fsync'd (which `build_from_iterators` already handles via `.tmp` rename):

```
// 1. Add new SSTable to manifest
inner.manifest.add_sstable(ManifestSstEntry {
    id: new_sst_id,
    path: new_sst_path.into(),
})?;

// 2. Remove old SSTables from manifest
for old_sst in &selected_sstables:
    inner.manifest.remove_sstable(old_sst.id)?;

// 3. Checkpoint manifest (snapshot + WAL truncate)
inner.manifest.checkpoint()?;

// 4. Delete old SSTable files from disk
for old_sst in &selected_sstables:
    fs::remove_file(&old_sst.path)?;

// 5. Update in-memory sstables list:
//    - Remove old SSTables from inner.sstables
//    - Insert new SSTable (loaded via SSTable::open)
//    - Re-sort by creation_timestamp DESC
```

**Ordering is critical:**
1. Write new SSTable file → 2. Add to manifest → 3. Remove old from manifest → 4. Checkpoint → 5. Delete old files → 6. Update in-memory state.

If we crash between steps 2 and 4, on recovery we'll have both old and new SSTables in the manifest. That's okay — the data is correct (duplicated but consistent). We need to handle this by re-running compaction or by detecting duplicate key ranges on startup. However, since we do step 2 (add) before step 3 (remove), a crash after step 2 means the old SSTables are still in the manifest too — the data is safe. On next open, the orphan cleanup already in `Engine::open()` handles SSTable files not in the manifest.

Actually, a better approach: we should do both the add and remove in the manifest first, then checkpoint, then delete files:

```
// 1. Add new SSTable to manifest (WAL append)
// 2. Remove all old SSTables from manifest (WAL appends)
// 3. Checkpoint manifest (atomic snapshot)
// 4. Delete old SSTable files
// 5. Update in-memory state
```

If crash happens after step 3 but before step 4: old files are on disk but not in manifest → the orphan cleanup in `Engine::open()` will remove them. Safe!

---

## 6. SSTable ID and File Path — Practical Concern

Currently, `flush_frozen_to_sstable_inner` generates the SSTable ID by scanning the sstable directory for the max existing ID. The same approach works for compaction. However, the manifest doesn't store a global SSTable ID counter, which means concurrent operations could race on ID assignment.

For now (single-threaded, inline compaction), this is fine. The existing approach in `flush_frozen_to_sstable_inner` works:

```rust
let mut max_id = 0u64;
for entry in fs::read_dir(&sstable_dir) {
    // parse "sstable-{id}.sst" → id
    max_id = max_id.max(id);
}
let sstable_id = max_id + 1;
```

**Future improvement**: Store a monotonic SSTable ID counter in the manifest.

---

## 7. How SSTable File Size Is Available

To bucket SSTables by size, we need each SSTable's file size. This is already available:

- `SSTable.footer.total_file_size` — stored in the SSTable footer.
- Alternatively: `SSTable.mmap.len()` — the memory-mapped file length.

Both give the same value. We'll use `footer.total_file_size` since it's the canonical on-disk size.

---

## 8. Integration: Where Compaction Plugs Into the Engine

### 8.1 Current Call Flow (Without Compaction)

```
put() / delete() / delete_range()
  → if frozen memtables exist:
      flush_frozen_to_sstable_inner()
        → builds SSTable from frozen memtable
        → adds to manifest
        → removes frozen WAL from manifest
  → execute the write operation
```

### 8.2 Updated Call Flow (With Compaction)

```
put() / delete() / delete_range()
  → if frozen memtables exist:
      flush_frozen_to_sstable_inner()
        → builds SSTable from frozen memtable
        → adds to manifest
        → removes frozen WAL from manifest
      maybe_compact_inner()              // ← NEW
        → bucket all SSTables by size
        → if any bucket.len() >= min_threshold:
            → select best bucket
            → merge selected SSTables
            → build new SSTable
            → update manifest
            → delete old files
            → update in-memory state
  → execute the write operation
```

### 8.3 New Functions to Implement

| Function                          | Responsibility                                                   |
|-----------------------------------|------------------------------------------------------------------|
| `maybe_compact_inner(inner)`      | Entry point: bucket, check trigger, call execute if needed       |
| `bucket_sstables(sstables, config)` | Group SSTables into buckets by file size                       |
| `select_compaction_bucket(buckets, config)` | Pick the best bucket (most SSTables ≥ threshold)       |
| `execute_compaction(inner, selected)` | Merge, build new SSTable, update manifest, cleanup           |
| `next_sstable_id(inner)`          | Extract into helper (shared between flush and compact)           |

---

## 9. Edge Cases and Safety

### 9.1 Empty SSTable After Compaction

If all entries in the selected SSTables are deduped down to zero (unlikely but possible if everything was shadowed by range deletes), `build_from_iterators` will return an error. Handle this: skip building and just remove the old SSTables.

### 9.2 Single SSTable in Bucket

A bucket with 1-3 SSTables (below `min_threshold`) is never compacted. This is by design — no wasted I/O.

### 9.3 Compaction During Recovery

On `Engine::open()`, after recovery, we don't run compaction automatically. The user's first write will trigger the flush → compact path if needed.

### 9.4 Scan Key Range for Full-SSTable Merge

When scanning an SSTable for compaction, we need the full key range. The SSTable properties provide `min_key` and `max_key`. We compute scan bounds as:

```
start_key = smallest min_key across all selected
end_key   = largest max_key across all selected + [0xFF]
```

The `+ [0xFF]` ensures the end key is exclusive and past the max key. Alternatively, we can introduce a `scan_all()` method on SSTable that doesn't require bounds.

### 9.5 Reads During Compaction

Since compaction is inline (synchronous, single-threaded), no concurrent reads can happen during compaction. The write lock is held. This is acceptable for the initial implementation.

---

## 10. What We Are NOT Doing (Explicitly Out of Scope)

| Feature                       | Why deferred                                                 |
|-------------------------------|--------------------------------------------------------------|
| Tombstone expiry/GC           | Separate feature (Scenario 2 in architecture.md)             |
| Background compaction threads | Will be added later; this guide covers inline-only           |
| Major compaction              | Full merge of all SSTables (Scenario 3); separate feature    |
| Bloom filter checks for tombs | Used during tombstone-specific compaction, not minor          |
| Level-based compaction        | We use STCS, not leveled                                     |

---

## 11. Summary: Step-by-Step Implementation Checklist

1. **Add `file_size()` helper to SSTable** (returns `footer.total_file_size`).

2. **Extract `next_sstable_id()` helper** from `flush_frozen_to_sstable_inner` into a shared utility.

3. **Implement `bucket_sstables()`**:
   - Input: `&[SSTable]`, `&EngineConfig`
   - Output: `Vec<Vec<usize>>` (vec of buckets, each containing indices into the sstable array)

4. **Implement `select_compaction_bucket()`**:
   - Input: `Vec<Vec<usize>>`, `&EngineConfig`
   - Output: `Option<Vec<usize>>` (indices of SSTables to compact)

5. **Implement `execute_compaction()`**:
   - Merge selected SSTables using scan iterators + heap merge
   - Deduplicate records (keep highest LSN per key)
   - Build new SSTable via `build_from_iterators`
   - Update manifest (add new, remove old, checkpoint)
   - Delete old SSTable files
   - Update `inner.sstables` in-memory

6. **Implement `maybe_compact_inner()`**:
   - Call `bucket_sstables`
   - Call `select_compaction_bucket`
   - If selected: call `execute_compaction`

7. **Wire into the engine**:
   - Call `maybe_compact_inner()` after `flush_frozen_to_sstable_inner()` in `put()`, `delete()`, `delete_range()`.

8. **Add tests**:
   - Unit test for `bucket_sstables` with known sizes
   - Integration test: write enough data to trigger flush + compaction, verify data integrity
   - Test that reads return correct data after compaction
   - Test compaction with tombstones (tombstones preserved)
   - Test compaction with range deletes

---

## 12. Worked Example

Starting state: 5 SSTables on disk, `min_threshold = 4`, `min_sstable_size = 50 MB`

```
SSTable  | File Size  | Bucket Assignment
---------|-----------|-------------------
sst-1    | 12 KB     | small bucket
sst-2    | 15 KB     | small bucket
sst-3    | 18 KB     | small bucket
sst-4    | 20 KB     | small bucket
sst-5    | 95 MB     | bucket-1
```

1. **Bucket**: small bucket has 4 SSTables → triggers! Bucket-1 has 1 → no.
2. **Select**: small bucket, take all 4 (< max_threshold=32).
3. **Merge**: scan sst-1..sst-4, heap-merge, dedup by key.
4. **Build**: new sst-6 (~60 KB).
5. **Manifest**: add sst-6, remove sst-1..sst-4, checkpoint.
6. **Cleanup**: delete sst-1..sst-4 files.

Result: 2 SSTables on disk (sst-5 at 95 MB, sst-6 at ~60 KB).

If more flushes produce small SSTables again, the cycle repeats — eventually the small bucket grows again, triggers, and merges into a larger SSTable that moves into a regular bucket.

---
---

# Tombstone-Specific Compaction (Garbage Collection)

## 13. What Is Tombstone Compaction?

Tombstone compaction is a targeted cleanup pass that rewrites a **single** SSTable to remove expired tombstones. Unlike minor compaction (which merges multiple SSTables by size), tombstone compaction focuses purely on reclaiming space wasted by deletion markers that are no longer needed.

It coexists with minor compaction:
- **Minor compaction**: triggered by SSTable count in a size bucket → merges N files into 1.
- **Tombstone compaction**: triggered by tombstone ratio in a single SSTable → rewrites 1 file into 1 (smaller) file.

Both run inline (synchronous) in this initial implementation.

---

## 14. When Is a Tombstone Safe to Drop?

A tombstone (point delete or range delete) is **droppable** when two conditions hold:

1. **Grace period expired**: The tombstone's timestamp is old enough.
   ```
   current_time > tombstone_timestamp + gc_grace_seconds
   ```
   Where `gc_grace_seconds` is a config parameter (default: 3,600 seconds = 1 hour).

2. **No older SSTable might still hold the deleted key**: After the tombstone is dropped, no surviving SSTable should contain an older version of that key without a corresponding tombstone — otherwise the old data would "resurrect."

   We check this using bloom filters of **all other SSTables** (those not being compacted):
   - If **no** bloom filter says "maybe" → the key doesn't exist anywhere else → safe to drop.
   - If **any** bloom filter says "maybe" → the key *might* exist elsewhere → must keep the tombstone (or do an expensive full scan if `bloom_fallback_scan = true`).

### Why `gc_grace_seconds` Matters

Even on a single node, WAL replay after a crash could re-introduce data that was written before the tombstone. The grace period ensures the tombstone outlives any WAL segment that might contain the overwritten data. Once all WAL segments from before the tombstone have been flushed to SSTables (and those SSTables are included in the bloom filter check), the tombstone is safe to drop.

---

## 15. Trigger — When Does Tombstone Compaction Fire?

An SSTable is a candidate for tombstone compaction when **all** of these hold:

```
tombstone_ratio  >= config.tombstone_threshold       (default: 0.2 = 20%)
sstable_age      >= config.tombstone_compaction_interval  (default: 86,400 sec = 24h)
```

Where:
```
tombstone_ratio = (sst.properties.tombstone_count + sst.properties.range_tombstones_count)
                  / sst.properties.record_count

sstable_age     = current_time_nanos - sst.properties.creation_timestamp
```

### 15.1 Where in the Code?

Tombstone compaction check runs **after minor compaction** (or after a flush if minor compaction wasn't triggered). The full call chain becomes:

```
put() / delete() / delete_range()
  → flush_frozen_to_sstable_inner()   (if frozen memtables exist)
  → maybe_minor_compact_inner()       (bucket-based, existing)
  → maybe_tombstone_compact_inner()   // ← NEW
```

We check each SSTable individually. If multiple SSTables qualify, we compact the one with the **highest tombstone ratio** first (one at a time per call — inline, synchronous).

### 15.2 Relevant Config Parameters

| Parameter                        | Default  | Purpose                                                  |
|----------------------------------|---------|----------------------------------------------------------|
| `tombstone_threshold`            | 0.2     | Min tombstone ratio to qualify                           |
| `tombstone_compaction_interval`  | 86,400s | Min SSTable age (seconds) before eligible                |
| `gc_grace_seconds`               | 3,600s  | Tombstone TTL: how long to keep tombstones after delete  |
| `bloom_fallback_scan`            | false   | If bloom says "maybe", do expensive full scan? (new config field) |
| `unchecked_tombstone_compaction` | false   | Skip bloom checks entirely, always drop expired tombstones (new config field) |

**New config fields needed** in `EngineConfig`:
```rust
pub gc_grace_seconds: u64,              // default: 3_600
pub bloom_fallback_scan: bool,          // default: false
pub unchecked_tombstone_compaction: bool, // default: false
```

---

## 16. Selection — Which SSTable to Compact?

```
function select_for_tombstone_compaction(sstables, config) -> Option<usize>:
    now = current_time_nanos()
    best_idx = None
    best_ratio = 0.0

    for (idx, sst) in sstables.iter().enumerate():
        if sst.properties.record_count == 0:
            continue

        tomb_count = sst.properties.tombstone_count + sst.properties.range_tombstones_count
        ratio = tomb_count as f64 / sst.properties.record_count as f64

        age_seconds = (now - sst.properties.creation_timestamp) / 1_000_000_000

        if ratio >= config.tombstone_threshold
           AND age_seconds >= config.tombstone_compaction_interval:
            if ratio > best_ratio:
                best_ratio = ratio
                best_idx = Some(idx)

    return best_idx
```

Only one SSTable is selected per invocation — this keeps the operation bounded and predictable.

---

## 17. Execution — Single-SSTable Rewrite

### 17.1 High-Level Steps

```
function execute_tombstone_compaction(inner, target_idx) -> Result:
    target_sst = &inner.sstables[target_idx]
    now = current_time_nanos()

    // Collect bloom filters from ALL OTHER SSTables
    other_blooms = []
    other_sstables = []   // for fallback scan
    for (idx, sst) in inner.sstables.iter().enumerate():
        if idx == target_idx:
            continue
        other_blooms.push(Bloom::from_slice(&sst.bloom.data))
        other_sstables.push(sst)

    // Scan the target SSTable (full key range)
    merge_iter = target_sst.scan(min_key, max_key_extended)?

    point_entries = []
    range_tombstones = []
    last_key = None
    dropped_count = 0

    for record in merge_iter:
        match record:
            RangeDelete { start, end, lsn, timestamp }:
                if is_tombstone_expired(timestamp, now, config.gc_grace_seconds):
                    if config.unchecked_tombstone_compaction:
                        dropped_count += 1
                        continue   // drop without checking
                    // Check bloom filters: does any other SSTable potentially have
                    // keys in this range? For range tombstones, we check start key.
                    if !any_bloom_says_maybe(other_blooms, &start):
                        dropped_count += 1
                        continue   // safe to drop
                    if config.bloom_fallback_scan:
                        if !any_sstable_has_key_in_range(other_sstables, &start, &end):
                            dropped_count += 1
                            continue
                    // else: keep the tombstone
                range_tombstones.push(MemtableRangeTombstone { start, end, lsn, timestamp })

            Delete { key, lsn, timestamp }:
                if last_key == Some(&key):
                    continue   // older version, skip (same dedup as minor)
                last_key = Some(key.clone())

                if is_tombstone_expired(timestamp, now, config.gc_grace_seconds):
                    if config.unchecked_tombstone_compaction:
                        dropped_count += 1
                        continue
                    if !any_bloom_says_maybe(other_blooms, &key):
                        dropped_count += 1
                        continue
                    if config.bloom_fallback_scan:
                        if !any_sstable_has_key(other_sstables, &key):
                            dropped_count += 1
                            continue
                    // else: keep
                point_entries.push(MemtablePointEntry { key, value: None, lsn, timestamp })

            Put { key, value, lsn, timestamp }:
                if last_key == Some(&key):
                    continue
                last_key = Some(key.clone())
                point_entries.push(MemtablePointEntry { key, value: Some(value), lsn, timestamp })

    // If nothing was dropped, skip building a new SSTable
    if dropped_count == 0:
        return Ok(())   // no-op

    // Build new SSTable (or skip if everything was dropped)
    if point_entries.is_empty() AND range_tombstones.is_empty():
        // Everything was tombstones and all were dropped
        // Just remove the old SSTable from manifest
        manifest.remove_sstable(target_sst.id)?
        manifest.checkpoint()?
        fs::remove_file(target_sst.path)?
        inner.sstables.remove(target_idx)
        return Ok(())

    new_sst_id = next_sstable_id(inner)
    new_sst_path = format!("sstables/sstable-{}.sst", new_sst_id)

    build_from_iterators(new_sst_path, ...)?

    // Manifest update (same pattern as minor compaction)
    manifest.add_sstable(new_sst_id, new_sst_path)?
    manifest.remove_sstable(target_sst.id)?
    manifest.checkpoint()?
    fs::remove_file(target_sst.path)?

    // Update in-memory: remove old, insert new, re-sort
```

### 17.2 Helper: `is_tombstone_expired`

```
function is_tombstone_expired(tombstone_timestamp_nanos, now_nanos, gc_grace_seconds) -> bool:
    tombstone_age_seconds = (now_nanos - tombstone_timestamp_nanos) / 1_000_000_000
    return tombstone_age_seconds >= gc_grace_seconds
```

Note: `timestamp` in the SSTable is stored as nanoseconds (from `UNIX_EPOCH.as_nanos()`), so we convert to seconds for comparison with `gc_grace_seconds`.

### 17.3 Helper: `any_bloom_says_maybe`

```
function any_bloom_says_maybe(other_blooms, key) -> bool:
    for bloom in other_blooms:
        if bloom.check(key):
            return true   // at least one says "maybe present"
    return false           // all say "definitely not"
```

This is the critical safety check. If any bloom filter reports the key *might* exist in another SSTable, we conservatively keep the tombstone. Bloom filters can have false positives (we use a 1% FP rate), so some tombstones will be retained unnecessarily — **but data is never lost**.

### 17.4 Helper: `any_sstable_has_key` (Expensive Fallback)

Only called when `bloom_fallback_scan = true` and a bloom said "maybe":

```
function any_sstable_has_key(other_sstables, key) -> bool:
    for sst in other_sstables:
        match sst.get(key):
            Put { .. } => return true    // key genuinely exists
            Delete { .. } => return true // another tombstone exists (fine, but key is referenced)
            RangeDelete { .. } => return true
            NotFound => continue
    return false
```

This resolves bloom filter false positives at the cost of reading data blocks from other SSTables. Only enable for SSTables with very high tombstone ratios where the space savings justify the I/O cost.

---

## 18. Bloom Filter False Positives — Impact Analysis

| Bloom FP Rate | Effect on Tombstone Compaction                                    |
|---------------|-------------------------------------------------------------------|
| 1% (current)  | ~1% of expired tombstones falsely retained per other SSTable      |
| With N other SSTables | Probability of keeping = `1 - (1 - 0.01)^N` ≈ `N%` for small N |
| 10 other SSTs | ~10% of droppable tombstones retained unnecessarily              |
| 100 other SSTs| ~63% of droppable tombstones retained unnecessarily              |

This means tombstone compaction becomes less effective as the number of SSTables grows. Mitigations:
- Run minor compaction first (reduces SSTable count).
- Use `bloom_fallback_scan = true` for SSTables with very high tombstone ratios.
- Periodically run major compaction (see below) which can drop all expired tombstones unconditionally.

---

## 19. Interaction with Minor Compaction

The two compaction types run in sequence and complement each other:

```
after flush:
  1. maybe_minor_compact()       → reduces SSTable count (size-based)
  2. maybe_tombstone_compact()   → reclaims tombstone space (per-SSTable)
```

| Concern                        | Resolution                                              |
|-------------------------------|--------------------------------------------------------|
| Minor compact changes sstables list | Tombstone check runs on the updated list             |
| Minor compact already deduplicates | Yes, but it keeps tombstones (doesn't check expiry)  |
| Could both compact same SSTable? | No — minor removes old SSTables first; tombstone operates on survivors |
| Ordering matters?              | Yes — minor first reduces SSTable count, making bloom checks cheaper |

---

## 20. Edge Cases

### 20.1 SSTable Has Only Tombstones, All Expired

If every record in the SSTable is a droppable tombstone and all bloom checks pass (key not found elsewhere), the result is an empty SSTable. We skip `build_from_iterators` (which would error) and instead just remove the old SSTable from the manifest.

### 20.2 Range Tombstones and Bloom Filters

Bloom filters are keyed on individual point keys, not ranges. For range tombstones, we check the start key in bloom filters as a heuristic. If any bloom says "maybe" for the start key, we keep the range tombstone.

A more thorough approach (future) would sample multiple keys within the range, but for now the start-key check is conservative enough (errs on the side of keeping tombstones).

### 20.3 Tombstone Compaction Produces No Change

If the tombstone ratio qualifies but all tombstones are either not expired or have bloom hits, `dropped_count` stays 0 and we skip the rewrite. No I/O wasted.

### 20.4 Fresh SSTables

The `tombstone_compaction_interval` (default: 24 hours) prevents thrashing: we won't repeatedly try to compact a young SSTable whose tombstones aren't expired yet.

---

## 21. New Functions to Implement (Tombstone Compaction)

| Function                                   | Responsibility                                                 |
|--------------------------------------------|----------------------------------------------------------------|
| `maybe_tombstone_compact_inner(inner)`     | Entry point: scan SSTables, find best candidate, call execute  |
| `select_tombstone_candidate(sstables, config)` | Pick the SSTable with highest tombstone ratio that qualifies |
| `execute_tombstone_compaction(inner, idx)`  | Scan, filter tombstones, rebuild, manifest update             |
| `is_tombstone_expired(ts, now, gc_grace)`  | Check if a tombstone has aged out                             |
| `check_bloom_filters(others, key) -> bool` | Query all other SSTables' blooms for a key                    |

---

## 22. Implementation Checklist (Tombstone Compaction)

1. **Add new config fields** to `EngineConfig`:
   - `gc_grace_seconds: u64` (default: 3_600)
   - `bloom_fallback_scan: bool` (default: false)
   - `unchecked_tombstone_compaction: bool` (default: false)

2. **Implement `is_tombstone_expired()`** — simple timestamp comparison.

3. **Implement `check_bloom_filters()`** — iterate other SSTables' blooms, return true if any says "maybe."

4. **Implement `select_tombstone_candidate()`** — find SSTable with highest ratio that meets age threshold.

5. **Implement `execute_tombstone_compaction()`** — scan, filter, rebuild/remove, manifest update.

6. **Implement `maybe_tombstone_compact_inner()`** — orchestrates selection + execution.

7. **Wire into engine** — call after `maybe_minor_compact_inner()`.

8. **Tests**:
   - Tombstones dropped when expired and no bloom hit
   - Tombstones kept when bloom says "maybe"
   - Tombstones kept when not expired yet
   - SSTable removed entirely when all entries are droppable tombstones
   - Data integrity after tombstone compaction (no resurrection)
   - `unchecked_tombstone_compaction` mode
   - Interaction: minor + tombstone compaction in sequence

---
---

# Major Compaction (Full Merge, User-Triggered)

## 23. What Is Major Compaction?

Major compaction merges **all** SSTables into one (or a few) new SSTables. Because the merge set is complete — every SSTable participates — there is no risk of data resurrection, so **all expired tombstones can be unconditionally dropped** without bloom filter checks.

Major compaction is expensive (reads and rewrites all data) and is **triggered explicitly by the user** via an API call. It is never triggered automatically.

### Use Cases

- Periodic maintenance (e.g., nightly batch job).
- Reclaiming space after a large delete workload.
- Reducing SSTable count to speed up reads after extensive writes.
- Cleaning up all expired tombstones in one pass.

---

## 24. API

```rust
impl Engine {
    /// Triggers a full compaction of all SSTables.
    ///
    /// This merges every SSTable into a single new SSTable,
    /// dropping all expired tombstones unconditionally.
    /// The operation runs synchronously and blocks the caller.
    pub fn major_compact(&self) -> Result<(), EngineError> { ... }
}
```

Usage:
```rust
let db = Engine::open("/data/mydb", config)?;
// ... writes, deletes ...
db.major_compact()?;  // full compaction
```

---

## 25. Trigger

No automatic trigger. The user calls `major_compact()` directly.

Internally, the method:
1. Acquires the write lock.
2. Flushes any frozen memtables first (to include all data).
3. If there are 0 or 1 SSTables, returns early (nothing to compact).
4. Runs the full merge.

---

## 26. Execution

### 26.1 High-Level Steps

```
function major_compact(inner) -> Result:
    // 1. Flush all frozen memtables
    while inner.frozen is not empty:
        flush_frozen_to_sstable_inner(inner)?

    // 2. If <= 1 SSTable, nothing to do
    if inner.sstables.len() <= 1:
        return Ok(())

    // 3. Create merge iterator over ALL SSTables
    all_sstables = inner.sstables (indices 0..N)
    merge_iter = create_full_merge_iterator(all_sstables)

    // 4. Deduplicate + drop expired tombstones (no bloom check needed!)
    now = current_time_nanos()
    point_entries = []
    range_tombstones = []
    last_key = None

    for record in merge_iter:
        match record:
            RangeDelete { start, end, lsn, timestamp }:
                if is_tombstone_expired(timestamp, now, config.gc_grace_seconds):
                    continue   // unconditionally drop — full set is being compacted
                range_tombstones.push(...)

            Delete { key, lsn, timestamp }:
                if last_key == Some(&key):
                    continue   // older version
                last_key = Some(key.clone())
                if is_tombstone_expired(timestamp, now, config.gc_grace_seconds):
                    continue   // drop — all data is in the merge
                point_entries.push(MemtablePointEntry { key, value: None, lsn, timestamp })

            Put { key, value, lsn, timestamp }:
                if last_key == Some(&key):
                    continue   // older version
                last_key = Some(key.clone())
                point_entries.push(MemtablePointEntry { key, value: Some(value), lsn, timestamp })

    // 5. Build new SSTable(s)
    //    If the result is empty (everything was expired tombstones), skip building.
    if point_entries.is_empty() AND range_tombstones.is_empty():
        // Remove all SSTables from manifest
        for sst in all_sstables:
            manifest.remove_sstable(sst.id)?
        manifest.checkpoint()?
        for sst in all_sstables:
            fs::remove_file(sst.path)?
        inner.sstables.clear()
        return Ok(())

    new_sst_id = next_sstable_id(inner)
    new_sst_path = format!("sstables/sstable-{}.sst", new_sst_id)
    build_from_iterators(new_sst_path, ...)?

    // 6. Manifest update
    manifest.add_sstable(new_sst_id, new_sst_path)?
    for old_sst in all_sstables:
        manifest.remove_sstable(old_sst.id)?
    manifest.checkpoint()?

    // 7. Delete old files
    for old_sst in all_sstables:
        fs::remove_file(old_sst.path)?

    // 8. Update in-memory state
    inner.sstables = [SSTable::open(new_sst_path)?]
```

### 26.2 Key Difference from Minor Compaction: Tombstone Handling

| Aspect                         | Minor Compaction          | Major Compaction              |
|-------------------------------|--------------------------|-------------------------------|
| Tombstone drop?               | No (keeps all)           | Yes — expired ones dropped    |
| Bloom check needed?           | N/A                      | No — full set is merged       |
| `gc_grace_seconds` respected? | N/A                      | Yes — only expired are dropped|

Because major compaction merges *every* SSTable, there is no "other SSTable" that could contain an older version of a deleted key. Once all data is in the merge, any expired tombstone can be safely discarded.

---

## 27. Interaction with Other Compaction Types

```
User calls major_compact()
  → Acquires write lock
  → Flushes frozen memtables
  → Full merge of all SSTables (with tombstone GC)
  → Returns

Subsequent put()/delete()/etc.:
  → Normal flow: maybe_minor_compact, maybe_tombstone_compact
  → But there's now only 1 SSTable, so no bucket triggers
```

After major compaction, the database has at most 1 SSTable. Minor compaction won't trigger (no bucket with ≥ 4 SSTables). Tombstone compaction won't trigger (the new SSTable has no expired tombstones). Over time, as new flushes create SSTables, the normal minor/tombstone compaction cycle resumes.

---

## 28. Edge Cases

### 28.1 Major Compaction on Empty Database

If there are 0 SSTables (everything is in the memtable), the method flushes frozen memtables first. If there's still ≤ 1 SSTable after flushing, it returns early.

### 28.2 Very Large Database

Major compaction reads and writes all data. For a database with hundreds of gigabytes, this could take a long time and consume significant disk space (old + new SSTables exist simultaneously). The user should be aware of this.

Future improvement: split the output into multiple SSTables at size boundaries (e.g., produce one SSTable per 256 MB of output).

### 28.3 Crash During Major Compaction

Same crash semantics as minor compaction:
- After manifest add + checkpoint but before old file deletion → orphaned old files cleaned up by `Engine::open()`.
- Before manifest checkpoint → new file is an orphan, old files are still in manifest → `Engine::open()` cleans up the new orphan file. Data intact.

---

## 29. New Function to Implement

| Function                       | Responsibility                                           |
|-------------------------------|----------------------------------------------------------|
| `Engine::major_compact(&self)` | Public API: acquires lock, flushes, runs full merge      |
| `execute_major_compaction(inner)` | Internal: merge all SSTables, drop expired tombstones, rebuild |

Most helpers (`next_sstable_id`, `is_tombstone_expired`, dedup logic) are shared with minor and tombstone compaction.

---

## 30. Implementation Checklist (Major Compaction)

1. **Implement `execute_major_compaction(inner)`**:
   - Full merge via scan iterators + heap
   - Dedup + expired tombstone drop (unconditional, no bloom)
   - Build new SSTable
   - Manifest update (add new, remove all old, checkpoint)
   - Delete old files, update in-memory state

2. **Implement `Engine::major_compact(&self)`**:
   - Acquire write lock
   - Flush all frozen memtables
   - Early return if ≤ 1 SSTable
   - Call `execute_major_compaction`

3. **Tests**:
   - Major compaction reduces N SSTables to 1
   - Expired tombstones dropped, non-expired kept
   - Data integrity after major compaction
   - Major compaction on empty database (no-op)
   - Major compaction with 1 SSTable (no-op)
   - `gc_grace_seconds` respected
   - Reads correct after major compaction
   - Crash safety (simulate crash mid-compaction)
   - Interaction: put → flush → minor compact → major compact → verify

---

## 31. Full Compaction Call Chain — All Three Types Together

```
put() / delete() / delete_range()
  │
  ├→ flush_frozen_to_sstable_inner()    [if frozen memtables exist]
  ├→ maybe_minor_compact_inner()        [bucket-based, size-tiered]
  └→ maybe_tombstone_compact_inner()    [per-SSTable tombstone GC]

major_compact()                          [user-triggered, explicit]
  │
  ├→ flush all frozen memtables
  └→ execute_major_compaction()          [full merge + tombstone GC]
```

Each type serves a different purpose:
- **Minor**: control SSTable count, consolidate similarly-sized files
- **Tombstone**: reclaim space from deletion-heavy SSTables
- **Major**: full reset — one SSTable, all garbage collected

---
---

# Background Compaction — Moving to a Dedicated Thread

## 32. Why Move Compaction Off the Write Path?

In the inline (synchronous) design, every `put()` / `delete()` call that triggers a compaction **blocks** until the compaction finishes. For small SSTables this is tolerable, but as SSTable sizes grow (megabytes, gigabytes), compaction can take seconds to minutes — stalling all writes and reads.

Moving compaction to a background thread gives us:
- **Non-blocking writes**: `put()` returns immediately after flush; compaction happens concurrently.
- **Non-blocking reads**: readers can continue using the old SSTables while compaction runs.
- **Predictable latency**: write latency is no longer a function of compaction I/O.

This section describes how to evolve the current inline design into a background-thread model in incremental steps, without rewriting the engine.

---

## 33. Architecture Overview — Before and After

### 33.1 Current (Inline)

```
put()  ─── write lock ──→  flush  →  compact  →  release lock  →  return
                           ^^^^^^^^^^^^^^^^^^^^^^
                           caller is blocked here
```

All operations — writes, reads, flush, compaction — share a single `Arc<RwLock<EngineInner>>`. The write lock is held for the entire duration.

### 33.2 Target (Background Thread)

```
put()  ─── write lock ──→  flush  →  signal compaction  →  release lock  →  return
                                           │
                        background thread ◄─┘
                           │
                           ├→ read SSTable list (no lock or read lock)
                           ├→ merge + build new SSTable on disk (no lock)
                           ├→ write lock: swap SSTable list + manifest update
                           └→ release lock
```

The write path only **signals** that compaction may be needed. The actual merging happens on a separate thread that holds no lock for most of its work. It only acquires the write lock briefly at the end to atomically install the result.

---

## 34. Step-by-Step Migration Plan

### Step 1: Separate "Compaction Input Collection" from "Compaction Execution"

Currently, everything is in `execute_compaction(inner, selected)` which requires `&mut EngineInner`. Refactor into two phases:

**Phase A — Snapshot (requires lock):**
```rust
struct CompactionJob {
    job_type: CompactionType,          // Minor, Tombstone, or Major
    input_sst_ids: Vec<u64>,           // SSTable IDs to compact
    input_sst_paths: Vec<PathBuf>,     // Paths to read from
    output_sst_id: u64,                // Pre-allocated ID for the new SSTable
    output_sst_path: PathBuf,          // Where to write the result
    other_bloom_data: Vec<Vec<u8>>,    // Bloom filters of non-participating SSTables
                                       //   (only for tombstone compaction)
    gc_grace_seconds: u64,
    bloom_fallback_scan: bool,
    unchecked_tombstone_compaction: bool,
}

enum CompactionType {
    Minor,
    Tombstone,
    Major,
}
```

Phase A runs under the write lock:
```
function plan_compaction(inner) -> Option<CompactionJob>:
    // 1. Bucket SSTables, check triggers (existing logic)
    // 2. Select SSTables for compaction
    // 3. Pre-allocate the next SSTable ID
    // 4. Copy bloom filter data from non-participating SSTables
    //    (for tombstone compaction only)
    // 5. Return CompactionJob with all info needed to compact
    //    WITHOUT holding any reference to EngineInner
```

**Phase B — Execute (no lock needed):**
```
function run_compaction(job: CompactionJob) -> Result<CompactionResult>:
    // 1. Open input SSTable files by path (independent of EngineInner)
    // 2. Create merge iterator, deduplicate, filter tombstones
    // 3. Build new SSTable to job.output_sst_path
    // 4. Return CompactionResult describing what was done
```

**Phase C — Install (requires lock):**
```
function install_compaction(inner, result: CompactionResult) -> Result<()>:
    // 1. Add new SSTable to manifest
    // 2. Remove old SSTables from manifest
    // 3. Checkpoint manifest
    // 4. Delete old SSTable files
    // 5. Reload new SSTable into inner.sstables
    // 6. Remove old SSTables from inner.sstables
    // 7. Re-sort
```

**Key insight**: Phase B is the expensive part (all the I/O). It opens SSTables independently by file path — it does not hold any reference to `EngineInner`. The lock is only held during Phase A (fast — just reading metadata) and Phase C (fast — just swapping pointers and writing manifest).

### Step 2: Open SSTables by Path in Phase B

Currently, `SSTable::open(path)` returns a fully independent, immutable `SSTable` struct backed by its own `mmap`. This is already perfect for background compaction — opening an SSTable by path does not require any lock.

The compaction thread opens the input SSTables from their file paths:
```rust
let input_sstables: Vec<SSTable> = job.input_sst_paths.iter()
    .map(|p| SSTable::open(p))
    .collect::<Result<_, _>>()?;
```

Since SSTables are immutable, it's safe for the background thread to read them while the foreground continues using its own mmap'd handles.

### Step 3: Introduce a Compaction Channel

Add a channel to signal the background thread:

```rust
use std::sync::mpsc;

pub struct Engine {
    inner: Arc<RwLock<EngineInner>>,
    compact_tx: mpsc::Sender<CompactionSignal>,
    compact_handle: Option<std::thread::JoinHandle<()>>,
}

enum CompactionSignal {
    /// A flush happened — check if compaction is needed.
    MaybeCompact,
    /// User requested major compaction.
    MajorCompact,
    /// Engine is shutting down — exit the thread.
    Shutdown,
}
```

### Step 4: Background Thread Loop

```rust
fn compaction_thread(
    inner: Arc<RwLock<EngineInner>>,
    rx: mpsc::Receiver<CompactionSignal>,
) {
    loop {
        match rx.recv() {
            Ok(CompactionSignal::MaybeCompact) => {
                // Phase A: plan (acquire write lock briefly)
                let job = {
                    let mut guard = inner.write().unwrap();
                    plan_compaction(&mut guard)
                };
                // guard is dropped here — lock released

                if let Some(job) = job {
                    // Phase B: execute (no lock)
                    match run_compaction(job) {
                        Ok(result) => {
                            // Phase C: install (acquire write lock briefly)
                            let mut guard = inner.write().unwrap();
                            if let Err(e) = install_compaction(&mut guard, result) {
                                tracing::error!("Compaction install failed: {}", e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Compaction failed: {}", e);
                        }
                    }
                }
            }

            Ok(CompactionSignal::MajorCompact) => {
                // Same 3-phase pattern but for major compaction
                // ...
            }

            Ok(CompactionSignal::Shutdown) | Err(_) => {
                break;
            }
        }
    }
}
```

### Step 5: Signal from the Write Path

Replace the inline `maybe_compact_inner()` calls with a channel send:

```rust
// Before (inline):
Self::flush_frozen_to_sstable_inner(&mut inner)?;
Self::maybe_minor_compact_inner(&mut inner)?;
Self::maybe_tombstone_compact_inner(&mut inner)?;

// After (background):
Self::flush_frozen_to_sstable_inner(&mut inner)?;
drop(inner);  // release lock before signaling
self.compact_tx.send(CompactionSignal::MaybeCompact).ok();
```

Flush remains inline (it's fast — one memtable → one SSTable), but compaction is deferred.

### Step 6: Graceful Shutdown

```rust
impl Engine {
    pub fn close(&self) -> Result<(), EngineError> {
        // Signal compaction thread to stop
        self.compact_tx.send(CompactionSignal::Shutdown).ok();

        // Wait for it to finish current work
        if let Some(handle) = self.compact_handle.take() {
            handle.join().map_err(|_| EngineError::Internal("thread panic".into()))?;
        }

        // Existing close logic: flush, checkpoint, fsync
        let mut inner = self.inner.write().unwrap();
        // ...
    }
}
```

### Step 7: Major Compaction via Channel

```rust
impl Engine {
    pub fn major_compact(&self) -> Result<(), EngineError> {
        // Option A: synchronous (block until done)
        //   Send signal + wait on a oneshot channel for completion
        //
        // Option B: fire-and-forget
        //   Just send the signal
        //
        // For the initial version, use Option A:

        let (done_tx, done_rx) = mpsc::sync_channel(1);
        self.compact_tx.send(CompactionSignal::MajorCompact(done_tx))?;
        done_rx.recv()??;  // block until compaction completes
        Ok(())
    }
}
```

---

## 35. Concurrency Hazards and How to Handle Them

### 35.1 Reads During Background Compaction

**Problem**: While the background thread is merging SSTables, a reader might scan the same SSTables.

**Not a problem because**: SSTables are immutable and memory-mapped. Multiple threads can read the same mmap'd file safely. The reader holds its own `SSTable` reference (via the `RwLock` read guard on `EngineInner`). The background thread opens its own separate `SSTable` handles by path.

### 35.2 Write During Background Compaction

**Problem**: A new flush creates a new SSTable while compaction is running.

**Not a problem because**: The new SSTable was not part of the compaction input set. During Phase C (install), we remove only the old SSTables that were in the compaction job, and add the new merged one. The freshly-flushed SSTable is unaffected.

### 35.3 Stale Compaction Plan

**Problem**: Between Phase A (plan) and Phase C (install), the SSTable list might have changed (new flush happened).

**Solution**: During Phase C, verify that all `input_sst_ids` still exist in `inner.sstables`. If any were removed (e.g., by another compaction — not possible in single-thread, but future-proofing), abort the install and discard the output SSTable.

```rust
fn install_compaction(inner: &mut EngineInner, result: CompactionResult) -> Result<()> {
    // Verify all input SSTables still exist
    for id in &result.input_sst_ids {
        if !inner.sstables.iter().any(|s| s.id() == *id) {
            // SSTable was removed by something else — abort
            fs::remove_file(&result.output_sst_path)?;  // cleanup orphan
            return Ok(());
        }
    }
    // Proceed with install...
}
```

### 35.4 Double Compaction of the Same SSTables

**Problem**: The channel receives `MaybeCompact` twice quickly; both try to compact the same bucket.

**Solution**: Use a simple `compacting: HashSet<u64>` in `EngineInner` that records which SSTable IDs are currently being compacted. Phase A skips SSTables in this set. Phase C clears them.

```rust
struct EngineInner {
    // ... existing fields ...
    compacting: HashSet<u64>,  // SSTable IDs currently in a compaction job
}
```

Phase A:
```rust
// When selecting SSTables for compaction, exclude those already being compacted
let candidates: Vec<_> = inner.sstables.iter()
    .filter(|sst| !inner.compacting.contains(&sst.id()))
    .collect();
// ... bucket and select from candidates ...

// Mark selected as compacting
for id in &selected_ids {
    inner.compacting.insert(*id);
}
```

Phase C:
```rust
// After install, clear the compacting set
for id in &result.input_sst_ids {
    inner.compacting.remove(id);
}
```

### 35.5 Channel Backpressure

**Problem**: Many flushes happen; channel fills up with `MaybeCompact` signals.

**Solution**: Use a bounded channel (size 1) with try_send. If the channel is full, the signal is dropped — that's fine, because the compaction thread will re-evaluate the full SSTable list when it processes the existing signal.

```rust
let (compact_tx, compact_rx) = mpsc::sync_channel(1);
// ...
self.compact_tx.try_send(CompactionSignal::MaybeCompact).ok();  // non-blocking, drop if full
```

---

## 36. SSTable Identity — Adding an ID Field

The current `SSTable` struct does not store its ID (the numeric identifier from the manifest). For background compaction, we need to match compaction results back to the SSTable list. Two options:

**Option A**: Add an `id: u64` field to `SSTable` (set during `SSTable::open` or by the engine after loading).

**Option B**: Maintain a parallel `Vec<(u64, PathBuf)>` mapping IDs to loaded SSTables.

**Recommended**: Option A — cleaner, avoids parallel bookkeeping:

```rust
pub struct SSTable {
    pub id: u64,          // ← NEW: set by engine after open
    pub mmap: Mmap,
    pub header: SSTableHeader,
    // ... rest unchanged
}
```

Set it after opening:
```rust
let mut sst = SSTable::open(&path)?;
sst.id = sstable_id;
inner.sstables.push(sst);
```

---

## 37. Thread Pool vs. Single Thread

The `EngineConfig` already has a `thread_pool_size` field. For the initial background implementation:

| Approach          | Pros                          | Cons                                |
|-------------------|-------------------------------|-------------------------------------|
| **Single thread** | Simple, no coordination needed | Only one compaction at a time       |
| **Thread pool**   | Parallel compactions possible  | Need `compacting` set, more complex |

**Recommended progression**:
1. **Phase 1**: Single dedicated compaction thread (this guide).
2. **Phase 2**: Thread pool with `thread_pool_size` workers, each pulling jobs from the channel. The `compacting` set (Section 35.4) prevents conflicts.

---

## 38. Migration Checklist — Inline to Background

1. **Refactor compaction into 3 phases** (plan / execute / install):
   - Extract Phase A: `plan_compaction(&mut EngineInner) -> Option<CompactionJob>`
   - Extract Phase B: `run_compaction(CompactionJob) -> Result<CompactionResult>`
   - Extract Phase C: `install_compaction(&mut EngineInner, CompactionResult) -> Result<()>`
   - Each phase works for all three compaction types (minor, tombstone, major).

2. **Add `id: u64` field to `SSTable`** struct and set it during load.

3. **Add `compacting: HashSet<u64>`** to `EngineInner`.

4. **Create the compaction channel**:
   - `mpsc::sync_channel(1)` with `CompactionSignal` enum.

5. **Spawn the background thread** in `Engine::open()`:
   - Receives `Arc<RwLock<EngineInner>>` and the channel receiver.
   - Runs the loop from Section 34.

6. **Replace inline compaction calls** in `put()`, `delete()`, `delete_range()`:
   - Keep `flush_frozen_to_sstable_inner()` inline.
   - Replace `maybe_minor_compact_inner()` / `maybe_tombstone_compact_inner()` with `compact_tx.try_send(MaybeCompact)`.

7. **Update `major_compact()`** to send through channel and wait for completion.

8. **Update `close()`** to send `Shutdown` and join the thread.

9. **Add stale-plan check** in Phase C (Section 35.3).

10. **Tests**:
    - Concurrent writes during compaction
    - Read correctness while compaction is in-flight
    - Shutdown waits for in-progress compaction
    - Major compact blocks caller until done
    - No double-compaction of same SSTables
    - Crash during background compaction → clean recovery

---

## 39. Data Flow Diagram — Background Compaction

```
┌─────────────────────────────────────────────────────────────┐
│                      WRITE THREAD                           │
│                                                             │
│  put(key, val)                                              │
│    │                                                        │
│    ├── [write lock] flush frozen → new SSTable              │
│    │                                                        │
│    ├── [release lock]                                       │
│    │                                                        │
│    └── compact_tx.try_send(MaybeCompact) ──────────────┐    │
│                                                        │    │
└────────────────────────────────────────────────────────┼────┘
                                                         │
                    ┌────────────────────────────────────┘
                    ▼
┌─────────────────────────────────────────────────────────────┐
│                   COMPACTION THREAD                          │
│                                                             │
│  recv(MaybeCompact)                                         │
│    │                                                        │
│    ├── Phase A [write lock]: plan_compaction()               │
│    │     → bucket SSTables                                  │
│    │     → select candidates                                │
│    │     → allocate output ID                               │
│    │     → mark compacting                                  │
│    │     → release lock                                     │
│    │                                                        │
│    ├── Phase B [NO LOCK]: run_compaction()                   │
│    │     → open input SSTs by path                          │
│    │     → merge + dedup + build new SST                    │
│    │     → (this is the slow part — minutes for large data) │
│    │                                                        │
│    └── Phase C [write lock]: install_compaction()            │
│          → verify inputs still exist                        │
│          → manifest: add new, remove old, checkpoint        │
│          → delete old files                                 │
│          → reload new SST into memory                       │
│          → clear compacting set                             │
│          → release lock                                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                    ▲
                    │
┌───────────────────┴─────────────────────────────────────────┐
│                    READ THREADS                              │
│                                                             │
│  get(key) / scan(start, end)                                │
│    │                                                        │
│    └── [read lock]: query memtable → frozen → SSTables      │
│         (unaffected by compaction — SSTables are immutable, │
│          old mmaps remain valid until install swaps list)    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 40. Lock Hold Times — Before vs. After

| Operation           | Inline (current)            | Background (target)           |
|---------------------|-----------------------------|-------------------------------|
| Flush               | ~ms (write one SSTable)     | ~ms (unchanged, stays inline) |
| Minor compaction    | seconds–minutes (full I/O)  | ~μs plan + ~μs install        |
| Tombstone compaction| seconds (rewrite one SST)   | ~μs plan + ~μs install        |
| Major compaction    | minutes–hours               | ~μs plan + ~μs install        |
| Read during compact | **blocked** (write lock)    | **unblocked** (read lock OK)  |

The expensive merge+build step moves entirely off the lock path, reducing write lock hold time from seconds/minutes to microseconds.
