# Changelog

All notable changes to AeternusDB are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.0] — 2025-02-18

### Added

#### Storage Engine
- LSM-tree based key-value storage engine with `put`, `get`, `delete`, `delete_range`, and `scan` operations.
- Public database API in `lib.rs` — `Db`, `DbConfig`, `DbError`, and `KeyValue` types.
- Automatic memtable freeze and background flush when the write buffer is full.
- Graceful shutdown via `Db::close()` with `Drop` fallback.
- Thread-safe design — `Db` is `Send + Sync`.

#### Write-Ahead Log (WAL)
- Generic, CRC32-protected, append-only WAL (`Wal<T>`) for crash recovery.
- Used by both the memtable and the manifest.
- Per-record checksumming with corruption detection on replay.
- WAL rotation on memtable freeze.

#### Memtable
- In-memory write buffer backed by `BTreeMap` with multi-version entries (LSN-ordered).
- WAL-first write protocol — every mutation is durable before in-memory update.
- Point tombstones and range tombstones (`[start, end)`) for deletion.
- Approximate size tracking with configurable `write_buffer_size`.

#### SSTable
- Immutable, sorted, on-disk tables with block-level CRC32 checksums.
- Fixed 32-byte header and 44-byte footer for fast validation.
- ~4 KiB data blocks with separator-key index for binary search.
- Bloom filter per SSTable (~1% false positive rate) for fast negative lookups.
- Range tombstone block for efficient range deletion persistence.
- Properties block with min/max key, LSN, timestamp, and record counts.
- Metaindex block for extensible metadata discovery.
- Atomic writes via `.tmp` file + `rename` + `fsync`.
- Memory-mapped I/O for efficient reads.

#### Manifest
- Persistent metadata manager tracking live SSTables, WAL segments, and global LSN.
- WAL + snapshot model — every metadata mutation is logged; periodic `checkpoint()` writes a full snapshot and truncates the WAL.
- Atomic snapshot writes with CRC32 integrity.
- Orphan SSTable cleanup on recovery.
- Monotonic SSTable ID allocation.

#### Compaction (Size-Tiered Compaction Strategy)
- **Minor compaction** — groups SSTables into size buckets and merges similarly-sized tables. Deduplicates point entries (highest LSN wins), preserves all tombstones.
- **Tombstone compaction** — rewrites a single high-tombstone-ratio SSTable, dropping provably-unnecessary point and range tombstones using bloom filter checks and optional fallback `get()`.
- **Major compaction** — merges all SSTables into one, dropping all tombstones and applying range deletes to suppress covered puts. Triggered explicitly via `Db::major_compact()`.
- Background thread pool (`crossbeam` channel) for non-blocking flush and compaction.
- Configurable thresholds for bucket sizing, compaction triggers, and tombstone ratios.

#### Crash Recovery
- Automatic recovery on `Db::open()` — replays manifest, frozen WALs, active WAL, and reconciles LSN across all layers.
- Orphan SSTable cleanup (unreferenced `.sst` files deleted, `.tmp` files removed).

#### Testing
- ~250 unit tests covering all modules.
- ~11 stress tests (marked `#[ignore]`) for concurrency, crash recovery, and compaction under load.
- ~27 integration tests covering library api.

#### CI / CD
- GitHub Actions CI workflow — `cargo check`, `rustfmt`, `clippy`, `cargo test`.
- GitHub Actions documentation workflow — builds `cargo doc` and deploys to GitHub Pages.
