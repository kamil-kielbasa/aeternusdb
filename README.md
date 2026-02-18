# AeternusDB

[![CI](https://github.com/kamil-kielbasa/aeternusdb/actions/workflows/ci.yml/badge.svg)](https://github.com/kamil-kielbasa/aeternusdb/actions/workflows/ci.yml)
[![Docs](https://github.com/kamil-kielbasa/aeternusdb/actions/workflows/docs.yml/badge.svg)](https://github.com/kamil-kielbasa/aeternusdb/actions/workflows/docs.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An embeddable, persistent key-value storage engine built on a **Log-Structured Merge Tree (LSM-tree)** architecture. Written in Rust with a focus on durability, crash safety, and correctness.

## Features

- **Write-ahead logging (WAL)** — every mutation is persisted before acknowledgement, guaranteeing durability and crash recovery.
- **Multi-version concurrency** — multiple versions per key, ordered by log sequence number (LSN); reads always see the latest committed version.
- **Point & range tombstones** — efficient delete semantics for individual keys and key ranges.
- **Bloom filter lookups** — each SSTable carries a bloom filter for fast negative point-lookup responses.
- **Block-level CRC32 integrity** — every on-disk structure is checksummed.
- **Three compaction strategies** — minor (size-tiered merge), tombstone (per-SSTable GC), and major (full merge).
- **Configurable thresholds** — buffer sizes, compaction triggers, tombstone ratios, and bloom filter policies are all tunable.

## Architecture

```text
┌───────────────────────────────────────────────────────┐
│                       Engine                          │
│                                                       │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Active     │  │   Frozen     │  │   SSTables   │  │
│  │  Memtable   │  │  Memtables   │  │  (on disk)   │  │
│  │  + WAL      │  │  + WALs      │  │              │  │
│  └──────┬──────┘  └──────┬───────┘  └──────┬───────┘  │
│         │  freeze        │  flush          │          │
│         └───────►        └────────►        │          │
│                                            │          │
│  ┌─────────────────────────────────────────┘          │
│  │  Compaction (minor / tombstone / major)            │
│  └────────────────────────────────────────────────────│
│                                                       │
│  ┌──────────────────────────────────────────────────┐ │
│  │              Manifest (WAL + snapshot)           │ │
│  └──────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────┘
```

Data flows through three layers, queried newest-first:

1. **Active memtable** — in-memory sorted map backed by a WAL.
2. **Frozen memtables** — read-only snapshots awaiting flush to disk.
3. **SSTables** — immutable, sorted, on-disk files with bloom filters and block indices.

### Modules

| Module | Description |
|--------|-------------|
| `engine` | Core storage engine: open, close, put, get, delete, scan, flush, compact |
| `memtable` | In-memory write buffer with multi-version entries and range tombstones |
| `wal` | Generic, CRC-protected, append-only write-ahead log |
| `sstable` | Immutable sorted tables with bloom filters, range tombstones, and block indices |
| `manifest` | Persistent metadata manager using a WAL + snapshot model |
| `compaction` | Size-tiered (STCS) compaction with minor, tombstone, and major strategies |

## Getting Started

### Prerequisites

- [Rust](https://rustup.rs/) (edition 2024)

### Build

```bash
cargo build
```

### Test

```bash
# Run all unit + integration tests (250 tests)
cargo test --lib

# Run stress tests (11 tests, ~2 min)
cargo test --lib -- --ignored
```

### Generate Documentation

```bash
cargo doc --no-deps --open
```

## Usage

Public high-level API is planned for `lib.rs`. Currently the engine internals are `pub(crate)` — see `cargo doc --no-deps --open` for the crate-level documentation and module overview.

## Configuration

| Parameter | Type | Description |
|-----------|------|-------------|
| `write_buffer_size` | `usize` | Max memtable size (bytes) before freeze |
| `compaction_strategy` | `CompactionStrategyType` | Compaction family (`Stcs`) |
| `bucket_low` | `f64` | Lower bound multiplier for size bucket range |
| `bucket_high` | `f64` | Upper bound multiplier for size bucket range |
| `min_sstable_size` | `usize` | Below this, SSTables go to the "small" bucket |
| `min_threshold` | `usize` | Min SSTables in bucket to trigger minor compaction |
| `max_threshold` | `usize` | Max SSTables to compact at once |
| `tombstone_ratio_threshold` | `f64` | Tombstone ratio to trigger tombstone compaction |
| `tombstone_compaction_interval` | `usize` | Min SSTable age (seconds) for tombstone compaction |
| `tombstone_bloom_fallback` | `bool` | Resolve bloom false-positives via actual `get()` |
| `tombstone_range_drop` | `bool` | Scan older SSTables to drop range tombstones |
| `thread_pool_size` | `usize` | Thread pool size for background operations |

## Compaction Strategies

### Minor Compaction (Size-Tiered)

Groups SSTables into size buckets and merges similarly-sized tables. Deduplicates point entries (keeps highest LSN) but preserves all tombstones.

### Tombstone Compaction (Per-SSTable GC)

Rewrites a single high-tombstone-ratio SSTable, dropping point and range tombstones that are provably unnecessary. Uses bloom filters and optional fallback `get()` calls to safely determine which tombstones can be removed.

### Major Compaction (Full Merge)

Merges **all** SSTables into one, actively applying range tombstones to suppress covered puts. All spent tombstones are dropped from the output.

## On-Disk Format

### SSTable Layout

```text
[Header]
[Data Block 1][CRC32]
[Data Block 2][CRC32]
...
[Bloom Filter][CRC32]
[Range Tombstones][CRC32]
[Properties][CRC32]
[Metaindex][CRC32]
[Index][CRC32]
[Footer]
```

### WAL Layout

```text
[Header][CRC32]
[Record Length][Record Bytes][CRC32]
[Record Length][Record Bytes][CRC32]
...
```

## Project Structure

```
src/
├── lib.rs              # Crate root with module documentation
├── engine/
│   ├── mod.rs          # Core engine (open, get, put, scan, compact)
│   └── utils.rs        # Record type and MergeIterator
├── memtable/
│   └── mod.rs          # In-memory write buffer
├── wal/
│   └── mod.rs          # Write-ahead log
├── sstable/
│   └── mod.rs          # Sorted string table (reader, writer, iterators)
├── manifest/
│   └── mod.rs          # Metadata persistence
└── compaction/
    ├── mod.rs           # Shared traits and helpers
    └── stcs/
        ├── mod.rs       # Size-tiered bucketing
        ├── minor.rs     # Minor compaction
        ├── tombstone.rs # Tombstone compaction
        └── major.rs     # Major compaction
```

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.