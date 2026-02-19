# Architecture

## Overview

AeternusDB is a single-node, embeddable key-value storage engine built on a **Log-Structured Merge Tree (LSM-tree)** design. It is written in pure Rust with zero unsafe code in the database layer and uses only stable, well-known crates for serialization, checksumming, and memory mapping.

The engine is optimized for **write-heavy workloads**: writes are sequential appends to a WAL and an in-memory buffer, while reads merge results across multiple layers. Background compaction consolidates on-disk data to bound read amplification.

## Glossary

| Term | Definition |
|------|------------|
| **LSM-tree** | Log-Structured Merge Tree — a write-optimized data structure that buffers writes in memory and flushes sorted runs to disk. |
| **SSTable** | Sorted String Table — an immutable, sorted, on-disk file containing key-value pairs and tombstones. |
| **WAL** | Write-Ahead Log — an append-only, CRC-protected log that ensures durability before in-memory state is updated. |
| **Manifest** | Persistent metadata log tracking which SSTables, WALs, and LSNs constitute the current database state. |
| **Memtable** | In-memory write buffer backed by a `BTreeMap`, holding multiple versions per key ordered by LSN. |
| **Tombstone** | A deletion marker. Point tombstones delete a single key; range tombstones delete all keys in `[start, end)`. |
| **Bloom filter** | Probabilistic data structure for fast negative point lookups — if the filter says "no", the key is definitely absent. |
| **STCS** | Size-Tiered Compaction Strategy — groups SSTables by file size and merges similarly-sized tables. |
| **LSN** | Log Sequence Number — a monotonically increasing counter assigned to every mutation for version ordering. |

## Architecture Diagram

```text
┌───────────────────────────────────────────────────────┐
│                         Db                            │
│          (public API + background thread pool)        │
│                                                       │
│  ┌──────────────────────────────────────────────────┐ │
│  │                    Engine                        │ │
│  │                                                  │ │
│  │  ┌─────────────┐  ┌──────────────┐  ┌────────┐  │ │
│  │  │  Active     │  │   Frozen     │  │ SSTs   │  │ │
│  │  │  Memtable   │  │  Memtables   │  │(disk)  │  │ │
│  │  │  + WAL      │  │  + WALs      │  │        │  │ │
│  │  └──────┬──────┘  └──────┬───────┘  └───┬────┘  │ │
│  │         │ freeze         │ flush        │       │ │
│  │         └───────►        └──────►       │       │ │
│  │                                         │       │ │
│  │  ┌──────────────────────────────────────┘       │ │
│  │  │  Compaction (STCS)                           │ │
│  │  │  minor → tombstone → major                   │ │
│  │  └──────────────────────────────────────────────│ │
│  │                                                  │ │
│  │  ┌──────────────────────────────────────────────┐│ │
│  │  │         Manifest (WAL + snapshot)            ││ │
│  │  └──────────────────────────────────────────────┘│ │
│  └──────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────┘
```

## Data Flow

### Write Path

1. The caller invokes `Db::put(key, value)`.
2. The `Db` layer validates the input and delegates to `Engine::put()`.
3. The engine acquires a **write lock** on `EngineInner`.
4. The active memtable assigns a monotonic **LSN** and appends a `Record::Put` to its **WAL** (with `fsync`).
5. The entry is inserted into the in-memory `BTreeMap`.
6. If the memtable exceeds `write_buffer_size`, it returns `FlushRequired`. The engine **freezes** the memtable (swaps in a fresh memtable + WAL) and the `Db` layer dispatches a background flush task.

Point deletes (`delete`) and range deletes (`delete_range`) follow the same path, inserting `Record::Delete` or `Record::RangeDelete` respectively.

### Background Flush & Compaction

When a memtable is frozen, the `Db` submits a task to the background thread pool. The task:

1. **Flushes** the oldest frozen memtable to a new SSTable via `build_from_iterators()` (atomic `.tmp` → rename).
2. Updates the **manifest** (add SSTable, remove frozen WAL).
3. Runs one or more rounds of **minor compaction** if any size bucket meets the threshold.
4. Runs a single pass of **tombstone compaction** if any SSTable exceeds the tombstone ratio threshold.

Major compaction is triggered explicitly by the user via `Db::major_compact()`.

### Read Path — Point Lookup

`Db::get(key)` searches three layers, newest-first:

1. **Active memtable** — resolves the highest-LSN point entry against covering range tombstones.
2. **Frozen memtables** — same resolution, newest WAL sequence first.
3. **SSTables** — sorted by `max_lsn` descending. For each SSTable:
   - Check key range (`min_key..max_key`) — skip if out of range.
   - Check **bloom filter** — skip if definitely absent.
   - Binary-search the **index block** to find the data block.
   - Seek within the data block for the key.
   - Check **range tombstones** stored in the SSTable.
   - Track the highest-LSN result. Once an SSTable's `max_lsn` is ≤ the best result's LSN, early-terminate.

### Read Path — Range Scan

`Db::scan(start, end)` creates a **merge iterator** (min-heap) across all layers:

1. Collect scan iterators from: active memtable, frozen memtables, all SSTables.
2. Feed them into a `MergeIterator` that yields `Record`s in `(key ASC, LSN DESC)` order.
3. Wrap with a `VisibilityFilter` that applies point and range tombstone semantics to emit only live `(key, value)` pairs.

## Concurrency Model

| Component | Synchronization | Notes |
|-----------|----------------|-------|
| `Engine` | `Arc<RwLock<EngineInner>>` | Reads take a shared lock; writes and flushes take an exclusive lock. |
| `Memtable` | `Arc<RwLock<MemtableInner>>` | WAL appends are serialized via `Arc<Mutex<File>>`. |
| `Manifest` | `Mutex<ManifestData>` + WAL mutex | All metadata mutations are serialized. |
| `Db` | Background thread pool via `crossbeam` channel | Flush and compaction tasks run on dedicated threads. Write path dispatches tasks without blocking. |

The write lock on `EngineInner` is held for the duration of a single write or flush operation. Compaction acquires the lock twice: briefly to obtain the strategy, then briefly to install the result. The expensive merge and I/O phase runs without any engine lock.

## Crash Recovery

On `Engine::open()`:

1. **Load manifest** — reads the snapshot (if present) and replays the manifest WAL to reconstruct the set of live SSTables, active WAL, and frozen WALs.
2. **Replay frozen WALs** — rebuilds each frozen memtable's in-memory state.
3. **Replay active WAL** — rebuilds the active memtable.
4. **Open SSTables** — memory-maps each SSTable referenced by the manifest, loads bloom filters and indices.
5. **Clean up orphans** — deletes any `.sst` files on disk that are not referenced in the manifest (e.g., from a crash during compaction).
6. **Reconcile LSN** — computes the maximum LSN across all layers and seeds the active memtable's counter to ensure monotonicity.

The design guarantees that no acknowledged write is lost after a crash, and no partial SSTable or manifest update is visible.

## Module Overview

| Module | Responsibility |
|--------|---------------|
| `lib.rs` (`Db`) | Public API, input validation, background thread pool management, graceful shutdown. |
| `engine` | Core LSM engine — open, close, put, get, delete, scan, flush, compact. Owns the `RwLock<EngineInner>`. |
| `memtable` | In-memory write buffer with multi-version `BTreeMap`, WAL-first writes, point/range tombstone resolution. |
| `wal` | Generic, CRC-protected, append-only WAL. Used by both the memtable and the manifest. |
| `sstable` | Immutable on-disk sorted tables. Includes reader, writer (`build_from_iterators`), block iterator, scan iterator, bloom filter, and range tombstone support. |
| `manifest` | Persistent metadata manager using a WAL + snapshot model. Tracks SSTables, WAL segments, LSN, and SSTable ID allocation. |
| `compaction` | Trait-based compaction framework with STCS implementation: minor (bucket merge), tombstone (per-SSTable GC), and major (full merge). |

## On-Disk Directory Layout

```
<data_dir>/
├── manifest/
│   ├── wal-000001.log         # Manifest WAL
│   └── manifest.snapshot      # Latest manifest snapshot
├── memtables/
│   ├── wal-000001.log         # Active memtable WAL
│   ├── wal-000002.log         # Frozen memtable WAL (pending flush)
│   └── ...
└── sstables/
    ├── sstable-1.sst
    ├── sstable-2.sst
    └── ...
```

## Configuration Reference

### `DbConfig` (public API)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `write_buffer_size` | `usize` | 64 KiB | Max memtable size in bytes before freeze. Must be ≥ 1024. |
| `min_compaction_threshold` | `usize` | 4 | Min SSTables in a size bucket to trigger minor compaction. Must be ≥ 2. |
| `max_compaction_threshold` | `usize` | 32 | Max SSTables to merge in a single minor compaction. Must be ≥ `min_compaction_threshold`. |
| `tombstone_compaction_ratio` | `f64` | 0.3 | Tombstone-to-record ratio that triggers tombstone compaction. Must be in (0.0, 1.0]. |
| `thread_pool_size` | `usize` | 2 | Number of background worker threads for flushing and compaction. Must be ≥ 1. |

### `EngineConfig` (internal)

The `DbConfig` is converted to an `EngineConfig` with additional STCS-specific parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bucket_low` | 0.5 | Lower bound multiplier for size bucket range. |
| `bucket_high` | 1.5 | Upper bound multiplier for size bucket range. |
| `min_sstable_size` | 50 bytes | SSTables smaller than this go to the "small" bucket. |
| `tombstone_compaction_interval` | 0 seconds | Min SSTable age before eligible for tombstone compaction. |
| `tombstone_bloom_fallback` | true | Resolve bloom filter false positives via actual `get()`. |
| `tombstone_range_drop` | true | Scan older SSTables to safely drop range tombstones. |

## Architecture Decisions

### Pure Rust, no unsafe

The entire codebase uses safe Rust. Memory-mapped I/O is provided by the `memmap2` crate, and serialization by a custom `encoding` module with fixed-integer encoding.

### WAL-first writes

Every mutation is appended to the WAL and `fsync`'d before updating in-memory state. This ensures that a crash at any point does not lose acknowledged data.

### Multi-version concurrency via LSN

Each key may have multiple versions in the memtable, ordered by descending LSN. Resolution is deferred to read time — the highest-LSN entry always wins. This avoids in-place updates and simplifies concurrent access.

### Immutable SSTables with memory mapping

SSTables are never modified after creation. They are memory-mapped for efficient random reads. The atomic `.tmp`-rename write pattern guarantees that only complete, valid SSTables are visible.

### Single compaction strategy (STCS) with three passes

Rather than implementing multiple independent compaction strategies, AeternusDB uses a single **Size-Tiered Compaction Strategy** (STCS) with three complementary passes:

- **Minor** — merges similarly-sized SSTables to reduce file count.
- **Tombstone** — rewrites a single high-tombstone-ratio SSTable to reclaim space.
- **Major** — merges all SSTables into one, dropping all spent tombstones.

This is a deliberate design choice: STCS is the natural fit for write-heavy workloads. A different strategy (e.g., Leveled Compaction) would require a fundamentally different approach where tombstone and major passes are unnecessary because every compaction propagates changes across levels.

### Manifest WAL + snapshot

The manifest uses the same WAL infrastructure as the memtable. Periodic `checkpoint()` writes a full snapshot and truncates the manifest WAL, bounding recovery time.

### Background thread pool

Flush and compaction run on a dedicated `crossbeam`-based thread pool. The write path only signals the pool; the actual I/O happens asynchronously. This keeps write latency predictable regardless of compaction load.
