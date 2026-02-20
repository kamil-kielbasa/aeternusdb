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

#### Encoding
- Custom, zero-dependency binary encoding module replacing `bincode` — full ownership of the on-disk wire format.
- `Encode` and `Decode` traits with fallible, deterministic serialization (no panics, no `unwrap`).
- Little-endian wire format with `u32` length/count prefixes.
- Primitive implementations: `u8`, `u16`, `u32`, `u64`, `i64`, `bool`.
- Fixed-size byte arrays (`[u8; N]`) — raw bytes, no length prefix.
- Variable-length types: `Vec<u8>`, `&[u8]`, `String`, `&str`, `PathBuf` — all length-prefixed.
- `Option<T>` — tag-based encoding (`0x00` = `None`, `0x01` = `Some`).
- Generic `encode_vec` / `decode_vec` for `Vec<T>` with per-element encoding.
- `EncodingError` enum with six variants: `UnexpectedEof`, `InvalidTag`, `InvalidBool`, `InvalidUtf8`, `LengthOverflow`, `Custom`.
- Safety limits to prevent allocation bombs: `MAX_BYTE_LEN` (256 MiB) and `MAX_VEC_ELEMENTS` (16 M).
- Zero-panic guarantee — all errors propagated via `Result`.
- Convenience helpers: `encode_to_vec` and `decode_from_slice`.
- Platform-native `PathBuf` encoding via `OsStr` raw bytes (Unix).

#### Crash Recovery
- Automatic recovery on `Db::open()` — replays manifest, frozen WALs, active WAL, and reconciles LSN across all layers.
- Orphan SSTable cleanup (unreferenced `.sst` files deleted, `.tmp` files removed).

#### Testing
- 421 tests total (372 unit + 27 integration + 22 integration hardening); 0 failures, 0 warnings.
- 11 stress tests (marked `#[ignore]`) for concurrency, crash recovery, and compaction under load.

**Priority 1 — Critical correctness (29 tests across 5 files)**
- `tests_crash_recovery` — WAL truncation at various offsets, partial flush crash, crash-on-reopen idempotency.
- `tests_crash_flush` — crash-during-flush recovery, data visible after recovery.
- `tests_crash_compaction` — crash-during-compaction with orphan `.tmp` cleanup.
- `tests_multi_crash` — multiple consecutive crashes with data integrity.
- `tests_lsn_crash` — LSN monotonicity preserved across crash and recovery.

**Priority 2 — Robustness (≈45 tests across 7 files)**
- `tests_concurrent_ops` — parallel readers and writers, scan under concurrent mutation.
- `tests_boundary_values` — 1-byte keys/values, max-size keys, binary/non-UTF-8 keys, u64 sentinel values.
- `tests_file_cleanup` — orphan `.tmp` removal, unreferenced SSTable cleanup on open.
- `tests_compaction_edge` — compaction on empty DB returns false, single-SSTable major compact, tombstone compaction idempotency, range tombstone drop after major compact, minor compact with mismatched sizes.
- `tests_corruption` (SSTable) — truncated file, wrong magic, corrupted block CRC.
- `tests_rotation_edge` (WAL) — rotate-on-every-write, replay across many segments.
- `tests_checkpoint` (manifest) — checkpoint truncates WAL, recovery after checkpoint restores full state.

**Priority 3 — Hardening / edge cases (46 tests across 5 files)**
- `tests_hardening_edge` (engine) — empty-engine compaction and stats, tombstone-only flush and recovery, get/scan across all three layers simultaneously, 0xFF byte key scans, double-close with writes between.
- `tests_hardening` (SSTable) — single-entry SSTable, all-point-tombstones SSTable, range-tombstones-only SSTable, minimal mixed, duplicate keys highest-LSN wins.
- `tests_scan_edge` (engine) — prefix-key scan boundaries in memtable and SSTable, exactly-one-match range, adjacent non-overlapping ranges, deleted key at scan start.
- `tests_hardening` (memtable) — WAL replay with only range-deletes, interleaved point-delete and range-delete recovery, resurrect after range-delete survives replay, overlapping range tombstones, LSN counter resumption after replay.
- `integration_hardening` — exact boundary values accepted/rejected for all six `DbConfig` fields (`write_buffer_size`, `min_compaction_threshold`, `max_compaction_threshold`, `tombstone_compaction_ratio`, `tombstone_compaction_interval`, `thread_pool_size`); `scan` with `start == end` returns empty; `delete_range` with empty keys rejected; `major_compact` on empty DB; reopen after deleting all keys.

#### CI / CD
- GitHub Actions CI workflow — `cargo check`, `rustfmt`, `clippy`, `cargo test`, `cargo doc -D warnings`, `cargo machete`.
- GitHub Actions documentation workflow — builds `cargo doc` and deploys to GitHub Pages.
- GitHub Actions benchmark workflow — Criterion micro & YCSB benchmarks with historical tracking via `github-action-benchmark`, HTML reports published to GitHub Pages.
- GitHub Actions audit workflow — `cargo deny` for dependency vulnerability scanning, license compliance, and supply chain checks (on push, PR, and weekly schedule).
- GitHub Actions coverage workflow — `cargo-llvm-cov` with Codecov integration for line coverage tracking and PR comments.
- GitHub Actions semver workflow — `cargo semver-checks` on pull requests to catch accidental breaking API changes.
- GitHub Actions Miri workflow — runs memtable and encoding tests under Miri on nightly to detect undefined behavior (weekly schedule + manual dispatch).
- `deny.toml` — cargo-deny configuration allowing MIT, Apache-2.0, ISC, and Unicode-3.0 licenses.
