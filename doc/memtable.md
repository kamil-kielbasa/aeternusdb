# Memtable

## Overview

The memtable is the **in-memory write buffer** of the storage engine. It accepts all mutations (`put`, `delete`, `delete_range`), persists them to a WAL for crash safety, and serves reads from its sorted in-memory structure. When the buffer reaches the configured size limit, it is **frozen** (made read-only) and eventually **flushed** to an SSTable on disk.

## Data Structure

The memtable stores entries in a **multi-version** layout using nested `BTreeMap`s:

```text
Point entries:
  BTreeMap<key, BTreeMap<Reverse<lsn>, MemtablePointEntry>>

Range tombstones:
  BTreeMap<start_key, BTreeMap<Reverse<lsn>, MemtableRangeTombstone>>
```

- The outer `BTreeMap` provides sorted key order for scans.
- The inner `BTreeMap` stores multiple versions per key, ordered by **descending LSN** (`Reverse<lsn>`), so the highest-LSN entry — the latest version — is always first.

### Entry Types

**`MemtablePointEntry`** — a versioned point entry enum:

| Variant | Fields | Description |
|---------|--------|-------------|
| `Put` | `value: Vec<u8>`, `timestamp: u64`, `lsn: u64` | A live key-value pair. |
| `Delete` | `timestamp: u64`, `lsn: u64` | A point tombstone (deletion marker). |

**`MemtableRangeTombstone`** — a range deletion marker:

| Field | Type | Description |
|-------|------|-------------|
| `start` | `Vec<u8>` | Inclusive start key. |
| `end` | `Vec<u8>` | Exclusive end key. |
| `lsn` | `u64` | Log sequence number. |
| `timestamp` | `u64` | Wall-clock timestamp. |

## Write Path

Every mutation follows a strict **WAL-first** protocol:

1. Assign a monotonic **LSN** via `AtomicU64::fetch_add`.
2. Check if the in-memory buffer would exceed `write_buffer_size` — if so, return `FlushRequired` before writing anything.
3. **Append** the record to the WAL (with `fsync`).
4. **Insert** the entry into the in-memory `BTreeMap`.
5. Increment the approximate size counter.

This ordering guarantees that a crash after step 3 but before step 4 is safe — WAL replay will reconstruct the entry on restart.

### Operations

| Operation | WAL Record | In-Memory Effect |
|-----------|-----------|-----------------|
| `put(key, value)` | `Record::Put` | Inserts a `MemtablePointEntry::Put` with the value. |
| `delete(key)` | `Record::Delete` | Inserts a `MemtablePointEntry::Delete` tombstone. |
| `delete_range(start, end)` | `Record::RangeDelete` | Inserts a `MemtableRangeTombstone` covering `[start, end)`. |

## Read Path

### Point Lookup — `get(key)`

1. Find the highest-LSN point entry for `key` in the `BTreeMap`.
2. Scan all range tombstones whose `start_key ≤ key`. For each, check if `key < end_key`. Track the highest-LSN covering tombstone.
3. Resolve:

| Point Entry | Range Tombstone | Result |
|-------------|----------------|--------|
| None | None | `NotFound` |
| None | Covers key | `RangeDelete` |
| `Put(value)` | None or lower LSN | `Put(value)` |
| `Delete` | None or lower LSN | `Delete` |
| Any | Higher LSN | `RangeDelete` |

### Range Scan — `scan(start, end)`

1. Collect all point entries (all versions) in the key range `[start, end)`.
2. Collect all range tombstones that overlap the scan range.
3. Sort the combined records by `(key ASC, LSN DESC)`.
4. Return the sorted record stream.

The scan does **not** apply tombstone filtering — that is the responsibility of the engine's `VisibilityFilter`, which wraps the merged iterator from all layers.

## Flush Semantics

### `iter_for_flush()`

Returns a logical snapshot of the memtable suitable for building an SSTable:

- For each key, emits only the **latest version** (highest LSN) — either a `Put` or `Delete`.
- Emits **all** range tombstones (all versions).
- Does **not** filter based on tombstone interaction — range tombstones are preserved as-is for the SSTable.
- Does **not** mutate or clear in-memory state.

### Freeze

`memtable.frozen()` consumes the mutable `Memtable` and produces a `FrozenMemtable`:

- The `FrozenMemtable` is **read-only** — it exposes only `get`, `scan`, and `iter_for_flush`.
- It retains ownership of the WAL to guarantee durability until the flush to SSTable is complete and the manifest is updated.
- A `creation_timestamp` is recorded for ordering.

## LSN Management

The memtable owns an `AtomicU64` counter (`next_lsn`) that assigns monotonically increasing LSNs to every mutation:

- On startup (`Memtable::new`), the WAL is replayed to reconstruct in-memory state. The maximum LSN observed during replay becomes `next_lsn - 1`.
- `inject_max_lsn(lsn)` allows the engine to override the counter after cross-layer LSN reconciliation during recovery. This is called **before any writes**.
- `max_lsn()` returns `next_lsn - 1` — the highest assigned LSN.

## Size Tracking

The memtable tracks an `approximate_size` counter that is incremented on every write. The size estimate includes:

- `size_of::<MemtablePointEntry>()` + key length + value length (for puts).
- `size_of::<MemtablePointEntry>()` + key length (for deletes).
- `size_of::<MemtableRangeTombstone>()` + start length + end length (for range deletes).

When the next write would cause `approximate_size` to exceed `write_buffer_size`, the memtable returns `FlushRequired`. The engine then freezes the memtable and swaps in a fresh one.

## Concurrency

| Component | Lock | Notes |
|-----------|------|-------|
| `MemtableInner` | `Arc<RwLock<...>>` | Writers hold an exclusive lock; readers hold a shared lock. |
| WAL file | `Arc<Mutex<File>>` | Serializes file I/O. |
| LSN counter | `AtomicU64` | Lock-free, monotonically increasing. |

Reads and writes to the memtable can proceed concurrently (readers do not block writers and vice versa), with the WAL serializing the durable append.
