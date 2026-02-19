# Getting Started

## Prerequisites

- [Rust](https://rustup.rs/) toolchain (edition 2024)
- Linux (tested), macOS, or Windows

```bash
# Install Rust if needed
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Ensure formatting and linting tools are available
rustup component add rustfmt clippy
```

## Build

```bash
cargo build
```

## Test

```bash
# Run all unit tests
cargo test --lib

# Run including stress tests (slower, ~2 min)
cargo test --lib -- --ignored

# Run all tests (unit + stress)
cargo test -- --include-ignored
```

## Lint & Format

```bash
# Check formatting
cargo fmt --all -- --check

# Auto-fix formatting
cargo fmt --all

# Run clippy
cargo clippy --all-targets -- -D warnings
```

## Generate Documentation

```bash
# Build and open API docs locally
cargo doc --no-deps --open

# Build with private items visible (matches CI)
RUSTDOCFLAGS="--cfg docsrs" cargo doc --no-deps --document-private-items
```

The CI-deployed API reference is available at [kamil-kielbasa.github.io/aeternusdb](https://kamil-kielbasa.github.io/aeternusdb/).

## Usage

### Basic Operations

```rust
use aeternusdb::{Db, DbConfig};

// Open or create a database
let db = Db::open("/tmp/my_db", DbConfig::default()).unwrap();

// Write a key-value pair
db.put(b"user:1", b"Alice").unwrap();

// Read it back
let value = db.get(b"user:1").unwrap();
assert_eq!(value, Some(b"Alice".to_vec()));

// Delete a key
db.delete(b"user:1").unwrap();
assert_eq!(db.get(b"user:1").unwrap(), None);

// Range delete — deletes all keys in [start, end)
db.put(b"log:001", b"entry1").unwrap();
db.put(b"log:002", b"entry2").unwrap();
db.put(b"log:003", b"entry3").unwrap();
db.delete_range(b"log:001", b"log:003").unwrap();

// Scan a key range
db.put(b"a", b"1").unwrap();
db.put(b"b", b"2").unwrap();
db.put(b"c", b"3").unwrap();
let results = db.scan(b"a", b"d").unwrap();
// results: [("a", "1"), ("b", "2"), ("c", "3")]

// Major compaction (explicit, merges all SSTables)
db.major_compact().unwrap();

// Graceful shutdown
db.close().unwrap();
```

### Custom Configuration

```rust
use aeternusdb::{Db, DbConfig};

let config = DbConfig {
    write_buffer_size: 128 * 1024,         // 128 KiB buffer
    min_compaction_threshold: 4,            // compact when 4+ SSTables in bucket
    max_compaction_threshold: 32,           // merge at most 32 at once
    tombstone_compaction_ratio: 0.3,        // trigger at 30% tombstones
    thread_pool_size: 4,                    // 4 background workers
};

let db = Db::open("/tmp/my_db_custom", config).unwrap();
```

### Thread Safety

`Db` is `Send + Sync` and can be shared across threads via `Arc`:

```rust
use std::sync::Arc;
use aeternusdb::{Db, DbConfig};

let db = Arc::new(Db::open("/tmp/shared_db", DbConfig::default()).unwrap());

let writer = {
    let db = Arc::clone(&db);
    std::thread::spawn(move || {
        db.put(b"key", b"value").unwrap();
    })
};

let reader = {
    let db = Arc::clone(&db);
    std::thread::spawn(move || {
        let _ = db.get(b"key");
    })
};

writer.join().unwrap();
reader.join().unwrap();
db.close().unwrap();
```

## Project Structure

```
src/
├── lib.rs              # Public API (Db, DbConfig, DbError) + background pool
├── engine/
│   ├── mod.rs          # Core LSM engine (open, get, put, scan, compact)
│   └── utils.rs        # Record enum and MergeIterator
├── memtable/
│   └── mod.rs          # In-memory write buffer
├── wal/
│   └── mod.rs          # Write-ahead log
├── sstable/
│   └── mod.rs          # Sorted string table (reader, writer, iterators)
├── manifest/
│   └── mod.rs          # Metadata persistence
└── compaction/
    ├── mod.rs           # CompactionStrategy trait and shared helpers
    └── stcs/
        ├── mod.rs       # Size-tiered bucketing and strategy dispatch
        ├── minor.rs     # Minor compaction (bucket merge)
        ├── tombstone.rs # Tombstone compaction (per-SSTable GC)
        └── major.rs     # Major compaction (full merge)
```

## CI / CD

The project uses GitHub Actions for continuous integration:

- **CI** ([`.github/workflows/ci.yml`](../.github/workflows/ci.yml)) — runs `cargo check`, `rustfmt`, `clippy`, and `cargo test` on every push and PR.
- **Docs** ([`.github/workflows/docs.yml`](../.github/workflows/docs.yml)) — builds `cargo doc` and deploys to GitHub Pages on push to `main`.
