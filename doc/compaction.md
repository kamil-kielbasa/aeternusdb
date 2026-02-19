# Size-Tiered Compaction Strategy (STCS)

## Overview

AeternusDB uses a **Size-Tiered Compaction Strategy** (STCS) as its compaction framework. STCS is a natural fit for write-heavy workloads: it groups SSTables by file size and merges similarly-sized tables, progressively consolidating data into fewer, larger files.

Within STCS, three complementary compaction passes address different concerns:

| Pass | Trigger | Scope | Tombstone Handling |
|------|---------|-------|--------------------|
| **Minor** | Size bucket meets threshold | Multiple similarly-sized SSTables | Preserves all tombstones |
| **Tombstone** | Single SSTable has high tombstone ratio | One SSTable | Drops provably-unnecessary tombstones |
| **Major** | User-initiated | All SSTables | Drops all tombstones, applies range deletes |

These are not independent strategies — they are three aspects of a single STCS implementation. A different strategy (e.g., Leveled Compaction) would not need separate tombstone or major passes because every leveled compaction inherently propagates changes across levels.

## Bucketing — How SSTables Are Grouped

STCS groups SSTables into **size buckets**. A bucket is a set of SSTables whose file sizes are "close enough" to each other.

### Algorithm

1. **Sort** all SSTables by file size (ascending).
2. SSTables smaller than `min_sstable_size` go into a special **"small" bucket**.
3. For remaining SSTables, group iteratively: each SSTable joins the current bucket if its size falls within `[avg × bucket_low, avg × bucket_high]` of the bucket's running average. Otherwise, a new bucket is started.

### Example

Config: `min_sstable_size = 50`, `bucket_low = 0.5`, `bucket_high = 1.5`

```
SSTables on disk (sorted by size):
  10 KB, 12 KB, 15 KB      → small bucket (all < 50)
  52 MB, 55 MB, 60 MB      → bucket A  (avg ~55 MB, range [27.5, 82.5])
  110 MB, 120 MB            → bucket B  (avg ~115 MB, range [57.5, 172.5])
  400 MB                    → bucket C  (singleton)
```

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bucket_low` | 0.5 | Lower multiplier: `avg × 0.5` = bucket floor. |
| `bucket_high` | 1.5 | Upper multiplier: `avg × 1.5` = bucket ceiling. |
| `min_sstable_size` | 50 | SSTables smaller than this go to the small bucket. |

---

## Minor Compaction

Minor compaction is the standard compaction path that runs during normal database operation. It takes a group of similarly-sized SSTables and merges them into a single, larger SSTable.

### Goals

- Reduce the total number of SSTables (fewer files to search during reads).
- Consolidate duplicate keys (keep only the latest version per key).
- Produce a larger SSTable that moves into a higher size bucket.

### Trigger

A bucket is eligible for compaction when:

```
bucket.len() >= min_threshold    (default: 4)
```

If multiple buckets qualify, the bucket with the **most SSTables** is selected. Up to `max_threshold` (default: 32) SSTables are taken from that bucket.

Minor compaction is triggered automatically after every memtable flush as part of the background task pipeline.

### Execution

1. Create scan iterators over all selected SSTables (full key range).
2. Feed them into a `MergeIterator` (min-heap merge in `key ASC, LSN DESC` order).
3. **Deduplicate**: for each unique key, keep only the highest-LSN entry.
4. **Preserve all tombstones**: both point and range tombstones survive minor compaction because SSTables outside the merge set may still hold data that the tombstones need to suppress.
5. Build a new SSTable via `build_from_iterators()`.
6. Atomically update the manifest (add new, remove old, checkpoint).
7. Delete old SSTable files.

### Why Tombstones Are Preserved

Minor compaction only merges a subset of SSTables (one bucket). Older SSTables in other buckets may contain entries for keys that were deleted. Dropping a tombstone prematurely would cause the old data to "resurrect." Tombstone cleanup is the job of tombstone compaction and major compaction.

---

## Tombstone Compaction

Tombstone compaction is a targeted cleanup pass that rewrites a **single** SSTable to remove expired tombstones. It reclaims space wasted by deletion markers that are no longer needed.

### Trigger

An SSTable is a candidate when:

```
tombstone_ratio >= tombstone_ratio_threshold    (default: 0.3)
```

Where:

```
tombstone_ratio = (tombstone_count + range_tombstones_count) / record_count
```

If the SSTable also meets the `tombstone_compaction_interval` age requirement, it is eligible. The SSTable with the **highest tombstone ratio** among all eligible candidates is selected.

Tombstone compaction runs automatically as the last step of the background flush pipeline, after minor compaction.

### Safety — When Is a Tombstone Droppable?

A point tombstone for key `K` can be dropped if no other SSTable might still hold an older version of `K`. This is checked using **bloom filters** from all other SSTables:

1. Query each other SSTable's bloom filter for `K`.
2. If **all** bloom filters say "definitely not present" → safe to drop.
3. If **any** bloom filter says "maybe present" → conservatively keep the tombstone.
4. **Fallback** (`tombstone_bloom_fallback = true`): if a bloom says "maybe", perform an actual `get()` on that SSTable. If the key is truly absent, drop the tombstone.

Range tombstones follow a similar approach: check whether any older SSTable contains live keys in the covered range (`tombstone_range_drop = true`).

### Bloom Filter False Positives

Bloom filters have a ~1% false positive rate. With `N` other SSTables, the probability of falsely retaining a tombstone is approximately `1 - (1 - 0.01)^N`:

| Other SSTables | Retention rate |
|---------------|---------------|
| 1 | ~1% |
| 10 | ~10% |
| 100 | ~63% |

This means tombstone compaction becomes less effective as the SSTable count grows. Mitigations:

- Run minor compaction first to reduce SSTable count.
- Enable `tombstone_bloom_fallback` for accurate checks.
- Periodically run major compaction, which can drop all tombstones unconditionally.

### Execution

1. Scan the target SSTable (full key range).
2. For each record:
   - **Puts**: keep as-is (deduplicate by key — highest LSN wins).
   - **Point tombstones**: check bloom filters of other SSTables. Drop if safe.
   - **Range tombstones**: check older SSTables for covered live keys. Drop if safe.
3. If nothing was dropped → skip (no I/O wasted).
4. If everything was dropped → remove the SSTable entirely.
5. Otherwise → build a new SSTable from the surviving records, update manifest, delete old file.

---

## Major Compaction

Major compaction merges **all** SSTables into a single new SSTable. Because the merge set is complete — every SSTable participates — there is no risk of data resurrection, so **all tombstones can be unconditionally processed**.

### Trigger

Major compaction is **never triggered automatically**. It must be invoked explicitly by the user:

```rust
db.major_compact().unwrap();
```

This is a blocking operation that acquires the engine's write lock for the merge phase.

### Execution

1. Flush any remaining frozen memtables to ensure all data is on disk.
2. If there are fewer than 2 SSTables, return early (nothing to do).
3. Collect all range tombstones from all SSTables upfront.
4. Create a `MergeIterator` over all SSTables.
5. For each record:
   - **Deduplicate** by key (highest LSN wins).
   - **Drop all `Delete` records** — no tombstone preservation needed.
   - **Drop all `RangeDelete` records** — they are applied, not preserved.
   - For each `Put`, check if it is **suppressed** by a range tombstone with a higher LSN. If so, drop it.
6. Build a new SSTable from only the surviving `Put` entries (no tombstones in output).
7. Update manifest, delete all old SSTable files.

### Result

After major compaction, the database has exactly one SSTable containing only live key-value pairs. The automatic minor/tombstone compaction cycle resumes as new writes produce SSTables.

### Use Cases

- Periodic maintenance (e.g., nightly batch job).
- Reclaiming space after a large delete workload.
- Reducing SSTable count to speed up reads after extensive write bursts.

---

## Background Execution

Compaction runs on a dedicated background thread pool managed by the `Db` layer. The pipeline for each frozen memtable is:

```text
Memtable frozen
  │
  ├─► Flush oldest frozen → SSTable
  │
  ├─► Minor compaction (loop until no bucket qualifies)
  │
  └─► Tombstone compaction (single pass)
```

Major compaction bypasses this pipeline and runs synchronously when the user calls `Db::major_compact()`.

The background thread pool uses a `crossbeam` unbounded channel. Tasks are dispatched non-blockingly from the write path. The expensive I/O (merge + build SSTable) does not hold the engine lock — it is only acquired briefly to install the compaction result.

---

## Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_threshold` | 4 | Min SSTables in a bucket to trigger minor compaction. |
| `max_threshold` | 32 | Max SSTables to merge in a single minor compaction. |
| `bucket_low` | 0.5 | Lower bound multiplier for bucket size range. |
| `bucket_high` | 1.5 | Upper bound multiplier for bucket size range. |
| `min_sstable_size` | 50 | SSTables smaller than this go to the small bucket. |
| `tombstone_ratio_threshold` | 0.3 | Tombstone ratio to trigger tombstone compaction. |
| `tombstone_compaction_interval` | 0 | Min SSTable age (seconds) for tombstone compaction eligibility. |
| `tombstone_bloom_fallback` | true | Resolve bloom false positives via actual `get()` during tombstone compaction. |
| `tombstone_range_drop` | true | Check older SSTables to safely drop range tombstones. |
