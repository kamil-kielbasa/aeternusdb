//! YCSB-style macro-benchmarks for AeternusDB.
//!
//! Measures sustained throughput and latency distributions under
//! realistic mixed workloads inspired by the Yahoo Cloud Serving
//! Benchmark (YCSB).
//!
//! # Workloads
//!
//! | Name | Mix | Description |
//! |------|-----|-------------|
//! | **A** | 50% read, 50% update | Session store — heavy read/write |
//! | **B** | 95% read, 5% update | Photo tagging — read-mostly |
//! | **C** | 100% read | User profile cache — read-only |
//! | **D** | 95% read, 5% insert | Read-latest — status updates |
//! | **E** | 95% scan, 5% insert | Short ranges — threaded conversations |
//! | **F** | 50% read, 50% read-modify-write | User database — RMW |
//!
//! # Running
//!
//! ```bash
//! cargo bench --bench ycsb               # all workloads
//! cargo bench --bench ycsb -- "load"      # load phase only
//! cargo bench --bench ycsb -- "A"         # workload A only
//! ```

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};

use aeternusdb::{Db, DbConfig};
use rand::Rng;
use std::hint::black_box;
use tempfile::TempDir;

// ------------------------------------------------------------------------------------------------
// Constants
// ------------------------------------------------------------------------------------------------

/// Number of records loaded into the database before running workloads.
const RECORD_COUNT: u64 = 10_000;

/// Number of operations per workload run.
const OPS_PER_RUN: u64 = 5_000;

/// Value size in bytes.
const VALUE_SIZE: usize = 256;

/// Scan length for workload E.
const SCAN_LENGTH: u64 = 50;

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

fn make_key(i: u64) -> Vec<u8> {
    format!("user{i:012}").into_bytes()
}

fn make_value(rng: &mut impl Rng) -> Vec<u8> {
    let mut buf = vec![0u8; VALUE_SIZE];
    rng.fill(&mut buf[..]);
    // Ensure no zero bytes (value must not be empty and we avoid
    // accidental empty-looking payloads).
    for b in &mut buf {
        if *b == 0 {
            *b = 1;
        }
    }
    buf
}

/// Open a database with settings tuned for benchmarking.
fn open_bench_db(dir: &std::path::Path) -> Db {
    Db::open(
        dir,
        DbConfig {
            write_buffer_size: 256 * 1024, // 256 KiB — moderate buffer.
            thread_pool_size: 2,
            ..DbConfig::default()
        },
    )
    .expect("open")
}

/// Load phase: insert [`RECORD_COUNT`] sequential records.
///
/// **Scenario:** Writes 10,000 key-value pairs (256 B values) sequentially, simulating the
/// initial bulk-load of a dataset.
///
/// **What it measures:** Sustained sequential write throughput through the entire engine
/// pipeline — WAL append, memtable insertion, and background flushes triggered as the
/// write buffer fills up.
///
/// **Expected behaviour:** Millisecond range for the full load. Performance is dominated
/// by the number of flushes triggered by the 256 KiB write buffer.
fn load_database(db: &Db) {
    let mut rng = rand::rng();
    for i in 0..RECORD_COUNT {
        let key = make_key(i);
        let value = make_value(&mut rng);
        db.put(&key, &value).unwrap();
    }
}

// ------------------------------------------------------------------------------------------------
// Workloads
// ------------------------------------------------------------------------------------------------

/// Workload A — 50% read, 50% update.
///
/// **Real-world analogy:** Session store. A web server reads and updates session data
/// equally — e.g., checking user login state and refreshing session tokens.
///
/// **What it measures:** Performance under a balanced read/write mix with uniform random
/// key access. Updates overwrite existing keys, exercising the WAL, memtable, and any
/// background flushes triggered by accumulated writes.
///
/// **Expected behaviour:** Dominated by write cost. Latency should sit between the pure-
/// read (Workload C) and pure-write baselines. Variance may be higher than read-only
/// workloads due to occasional flush pauses.
fn run_workload_a(db: &Db) {
    let mut rng = rand::rng();
    for _ in 0..OPS_PER_RUN {
        let key_id = rng.random_range(0..RECORD_COUNT);
        let key = make_key(key_id);

        if rng.random_bool(0.5) {
            // Read.
            let _ = black_box(db.get(&key).unwrap());
        } else {
            // Update.
            let value = make_value(&mut rng);
            db.put(&key, &value).unwrap();
        }
    }
}

/// Workload B — 95% read, 5% update.
///
/// **Real-world analogy:** Photo tagging or social-media metadata. The vast majority of
/// accesses are reads (viewing tags/likes), with occasional writes (adding a tag).
///
/// **What it measures:** Read-dominated throughput with light write pressure. Verifies
/// that infrequent writes do not disproportionately affect read latency.
///
/// **Expected behaviour:** Close to Workload C (pure read), with a small overhead from
/// the 5% writes. If a flush happens to coincide with a measured iteration, a latency
/// spike may appear.
fn run_workload_b(db: &Db) {
    let mut rng = rand::rng();
    for _ in 0..OPS_PER_RUN {
        let key_id = rng.random_range(0..RECORD_COUNT);
        let key = make_key(key_id);

        if rng.random_bool(0.95) {
            let _ = black_box(db.get(&key).unwrap());
        } else {
            let value = make_value(&mut rng);
            db.put(&key, &value).unwrap();
        }
    }
}

/// Workload C — 100% read.
///
/// **Real-world analogy:** User profile cache. A CDN or application reads user profile
/// data from the database with no modifications during the measured window.
///
/// **What it measures:** Peak read throughput with zero write contention. This is the
/// theoretical ceiling for point-read performance.
///
/// **Expected behaviour:** The fastest of all workloads. Latency is determined entirely
/// by how many keys reside in the memtable vs. SSTables.
fn run_workload_c(db: &Db) {
    let mut rng = rand::rng();
    for _ in 0..OPS_PER_RUN {
        let key_id = rng.random_range(0..RECORD_COUNT);
        let key = make_key(key_id);
        let _ = black_box(db.get(&key).unwrap());
    }
}

/// Workload D — 95% read, 5% insert (append-only new keys).
///
/// **Real-world analogy:** Status/timeline feed. Most operations read recent posts, while
/// a small fraction inserts new posts. New keys are appended beyond the initial range.
///
/// **What it measures:** The engine's ability to handle a growing keyspace. Unlike
/// Workload B (which updates existing keys), the 5% inserts create new keys that extend
/// the key range, potentially affecting bloom filters and SSTable boundaries.
///
/// **Expected behaviour:** Similar to Workload B, but with slightly higher write cost
/// because inserts create new entries rather than overwriting. The growing keyspace may
/// lead to marginally slower reads over time as bloom filters become less effective.
fn run_workload_d(db: &Db, insert_base: &mut u64) {
    let mut rng = rand::rng();
    for _ in 0..OPS_PER_RUN {
        if rng.random_bool(0.95) {
            let key_id = rng.random_range(0..RECORD_COUNT + *insert_base);
            let key = make_key(key_id);
            let _ = black_box(db.get(&key).unwrap());
        } else {
            let key = make_key(RECORD_COUNT + *insert_base);
            let value = make_value(&mut rng);
            db.put(&key, &value).unwrap();
            *insert_base += 1;
        }
    }
}

/// Workload E — 95% scan (short range), 5% insert.
///
/// **Real-world analogy:** Threaded conversations or messaging. Reading a thread requires
/// scanning a range of messages ([`SCAN_LENGTH`] = 50 keys), while posting adds new
/// entries.
///
/// **What it measures:** Short-range scan throughput under light write pressure. Each scan
/// reads 50 consecutive keys, exercising ordered iteration and merge logic across
/// memtable and SSTables.
///
/// **Expected behaviour:** Significantly slower per-operation than point-read workloads
/// because each scan touches 50 keys. The scan cost dominates; the 5% inserts add
/// minimal overhead. Cross-SSTable merging may add latency variation.
fn run_workload_e(db: &Db, insert_base: &mut u64) {
    let mut rng = rand::rng();
    for _ in 0..OPS_PER_RUN {
        if rng.random_bool(0.95) {
            let start_id = rng.random_range(0..RECORD_COUNT.saturating_sub(SCAN_LENGTH));
            let start = make_key(start_id);
            let end = make_key(start_id + SCAN_LENGTH);
            let _ = black_box(db.scan(&start, &end).unwrap());
        } else {
            let key = make_key(RECORD_COUNT + *insert_base);
            let value = make_value(&mut rng);
            db.put(&key, &value).unwrap();
            *insert_base += 1;
        }
    }
}

/// Workload F — 50% read, 50% read-modify-write (RMW).
///
/// **Real-world analogy:** User database with counters. Half the operations read a user
/// record; the other half read a record, modify it (e.g., increment a counter), and
/// write it back.
///
/// **What it measures:** Read-modify-write (RMW) pattern cost. Each RMW operation
/// performs a `get` followed by a `put` — effectively two operations per logical unit.
///
/// **Expected behaviour:** Slower than Workload A because the 50% RMW operations are
/// each more expensive than a simple update (they include a read). Total operation count
/// is effectively 1.5× that of Workload A (5,000 operations where half require 2 engine
/// calls).
fn run_workload_f(db: &Db) {
    let mut rng = rand::rng();
    for _ in 0..OPS_PER_RUN {
        let key_id = rng.random_range(0..RECORD_COUNT);
        let key = make_key(key_id);

        if rng.random_bool(0.5) {
            // Pure read.
            let _ = black_box(db.get(&key).unwrap());
        } else {
            // Read-modify-write: read existing, then overwrite.
            let _ = db.get(&key).unwrap();
            let value = make_value(&mut rng);
            db.put(&key, &value).unwrap();
        }
    }
}

// ================================================================================================
// Criterion benchmark functions
// ================================================================================================

/// Criterion registration for the load phase.
///
/// Measures the time to insert [`RECORD_COUNT`] records into a fresh database.
/// Sample size is reduced to 10 because each iteration creates and fills an entire
/// database from scratch.
fn bench_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb/load");
    group.sample_size(10);
    group.bench_function(BenchmarkId::new("sequential", RECORD_COUNT), |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let db = open_bench_db(dir.path());
                (dir, db)
            },
            |(_dir, db)| {
                load_database(&db);
                db.close().unwrap();
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Criterion registration for Workload A (50% read / 50% update).
fn bench_workload_a(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb/workload");
    group.sample_size(10);
    group.bench_function("A_50read_50update", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let db = open_bench_db(dir.path());
                load_database(&db);
                (dir, db)
            },
            |(_dir, db)| run_workload_a(&db),
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Criterion registration for Workload B (95% read / 5% update).
fn bench_workload_b(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb/workload");
    group.sample_size(10);
    group.bench_function("B_95read_5update", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let db = open_bench_db(dir.path());
                load_database(&db);
                (dir, db)
            },
            |(_dir, db)| run_workload_b(&db),
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Criterion registration for Workload C (100% read).
fn bench_workload_c(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb/workload");
    group.sample_size(10);
    group.bench_function("C_100read", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let db = open_bench_db(dir.path());
                load_database(&db);
                (dir, db)
            },
            |(_dir, db)| run_workload_c(&db),
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Criterion registration for Workload D (95% read / 5% insert).
fn bench_workload_d(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb/workload");
    group.sample_size(10);
    group.bench_function("D_95read_5insert", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let db = open_bench_db(dir.path());
                load_database(&db);
                let insert_base = 0u64;
                (dir, db, insert_base)
            },
            |(_dir, db, mut insert_base)| run_workload_d(&db, &mut insert_base),
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Criterion registration for Workload E (95% scan / 5% insert).
fn bench_workload_e(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb/workload");
    group.sample_size(10);
    group.bench_function("E_95scan_5insert", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let db = open_bench_db(dir.path());
                load_database(&db);
                let insert_base = 0u64;
                (dir, db, insert_base)
            },
            |(_dir, db, mut insert_base)| run_workload_e(&db, &mut insert_base),
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Criterion registration for Workload F (50% read / 50% RMW).
fn bench_workload_f(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb/workload");
    group.sample_size(10);
    group.bench_function("F_50read_50rmw", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let db = open_bench_db(dir.path());
                load_database(&db);
                (dir, db)
            },
            |(_dir, db)| run_workload_f(&db),
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

// ================================================================================================
// Group registration
// ================================================================================================

criterion_group!(
    benches,
    bench_load,
    bench_workload_a,
    bench_workload_b,
    bench_workload_c,
    bench_workload_d,
    bench_workload_e,
    bench_workload_f,
);

criterion_main!(benches);
