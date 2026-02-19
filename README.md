# AeternusDB

[![CI](https://github.com/kamil-kielbasa/aeternusdb/actions/workflows/ci.yml/badge.svg)](https://github.com/kamil-kielbasa/aeternusdb/actions/workflows/ci.yml)
[![Docs](https://github.com/kamil-kielbasa/aeternusdb/actions/workflows/docs.yml/badge.svg)](https://github.com/kamil-kielbasa/aeternusdb/actions/workflows/docs.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An embeddable, persistent key-value storage engine built on a **Log-Structured Merge Tree (LSM-tree)** architecture. Written in pure Rust with a focus on durability, crash safety, and correctness.

> **Aeternus** — Latin for *eternal, everlasting*. A fitting name for a database engine designed to preserve data durably across crashes and restarts.

## Quick Start

```rust
use aeternusdb::{Db, DbConfig};

let db = Db::open("/tmp/my_db", DbConfig::default()).unwrap();

db.put(b"hello", b"world").unwrap();
assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));

db.delete(b"hello").unwrap();
assert_eq!(db.get(b"hello").unwrap(), None);

db.close().unwrap();
```

## Features

- **Write-ahead logging** — every mutation is persisted before acknowledgement
- **Automatic background compaction** — size-tiered compaction with minor, tombstone, and major passes
- **Point and range deletes** — efficient tombstone-based deletion semantics
- **Bloom filter lookups** — fast negative lookups on SSTables
- **CRC32 integrity** — all on-disk blocks are checksummed
- **Crash recovery** — automatic recovery from WAL on restart

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](doc/architecture.md) | High-level design, data flow, concurrency model, and configuration reference |
| [Getting Started](doc/getting_started.md) | Build, test, usage guide, and local development |
| [WAL](doc/wal.md) | Write-ahead log format, guarantees, and recovery |
| [Memtable](doc/memtable.md) | In-memory write buffer, multi-version storage, and flush semantics |
| [SSTable](doc/sstable.md) | On-disk sorted table format, block layout, and read/write process |
| [Manifest](doc/manifest.md) | Metadata persistence, WAL + snapshot model, and crash safety |
| [Compaction](doc/compaction.md) | Size-Tiered Compaction Strategy (STCS) — minor, tombstone, and major |
| [Encoding](doc/encoding.md) | Custom binary encoding format, wire layout, safety limits, and type support |
| [Changelog](CHANGELOG.md) | Release history and feature notes |

**API Reference (rustdoc):** [kamil-kielbasa.github.io/aeternusdb](https://kamil-kielbasa.github.io/aeternusdb/)

## Build & Test

```bash
cargo build
cargo test --lib                     # unit tests
cargo test --lib -- --ignored        # stress tests
cargo doc --no-deps --open           # local API docs
```

## Contact

📧 kamkie1996@gmail.com

## License

MIT — see [LICENSE](LICENSE).
