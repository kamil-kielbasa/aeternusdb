//! Micro-benchmarks for AeternusDB core operations.
//!
//! Uses Criterion for statistically rigorous measurement with regression
//! detection and HTML reports.
//!
//! # Running
//!
//! ```bash
//! cargo bench --bench micro              # run all micro-benchmarks
//! cargo bench --bench micro -- put       # filter by name
//! ```
//!
//! Reports are generated in `target/criterion/report/index.html`.

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};

use aeternusdb::{Db, DbConfig};
use tempfile::TempDir;

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

/// Default value payload for benchmarks (128 bytes).
const VALUE_128B: &[u8; 128] = &[0xAB; 128];

/// Larger value payload (1 KiB).
const VALUE_1K: &[u8; 1024] = &[0xCD; 1024];

/// Format a zero-padded key.
fn make_key(i: u64) -> Vec<u8> {
    format!("key-{i:012}").into_bytes()
}

/// Open a fresh database with a small write buffer so flushes happen
/// quickly during sustained-write benchmarks.
fn open_small_buffer(dir: &std::path::Path) -> Db {
    Db::open(
        dir,
        DbConfig {
            write_buffer_size: 4 * 1024,
            thread_pool_size: 1,
            ..DbConfig::default()
        },
    )
    .expect("open")
}

/// Open a database with a large write buffer so all data stays in the
/// memtable (no background flushes).
fn open_memtable_only(dir: &std::path::Path) -> Db {
    Db::open(
        dir,
        DbConfig {
            write_buffer_size: 64 * 1024 * 1024, // 64 MiB — everything fits in memory.
            thread_pool_size: 1,
            ..DbConfig::default()
        },
    )
    .expect("open")
}

/// Pre-populate a database with `count` sequential keys and close it,
/// so SSTables exist on disk.
fn prepopulate(dir: &std::path::Path, count: u64, value: &[u8]) {
    let db = open_small_buffer(dir);
    for i in 0..count {
        db.put(&make_key(i), value).unwrap();
    }
    db.close().unwrap();
}

// ================================================================================================
// Write benchmarks
// ================================================================================================

/// Benchmark group for write (`put`) operations.
///
/// # Sub-benchmarks
///
/// ## `memtable_only/128B` and `memtable_only/1K`
///
/// **Scenario:** Inserts a single key-value pair into a database configured with a 64 MiB
/// write buffer, ensuring no background flushes occur during measurement.
///
/// **What it measures:** The raw cost of writing to the WAL and inserting into the skip-list
/// memtable. Two payload sizes (128 B and 1 KiB) reveal how throughput scales with value size.
///
/// **Expected behaviour:** Each put takes ~1–3 ms on SATA SSD (dominated by WAL fsync).
/// 1 KiB values should be only marginally slower than 128 B because the fsync cost
/// dwarfs the memcpy.
///
/// ## `sequential_with_flush`
///
/// **Scenario:** Continuously writes 128 B values with a tiny 4 KiB write buffer that forces
/// frequent memtable flushes and SSTable creation.
///
/// **What it measures:** Sustained write throughput including the amortised cost of background
/// flushes and I/O. This reflects real-world write-heavy workloads.
///
/// **Expected behaviour:** Similar to memtable-only puts because both are dominated by the
/// per-write WAL fsync. Variance will be higher because some iterations coincide with a flush.
fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("put");

    // --- put: memtable-only (no flush, measures pure WAL + memtable path) ---
    for &(label, value) in &[("128B", VALUE_128B.as_slice()), ("1K", VALUE_1K.as_slice())] {
        group.bench_function(BenchmarkId::new("memtable_only", label), |b| {
            let dir = TempDir::new().unwrap();
            let db = open_memtable_only(dir.path());
            let mut seq = 0u64;

            b.iter(|| {
                let key = make_key(seq);
                db.put(black_box(&key), black_box(value)).unwrap();
                seq += 1;
            });

            db.close().unwrap();
        });
    }

    // --- put: sequential keys with small buffer (triggers flushes) ---
    group.bench_function("sequential_with_flush", |b| {
        let dir = TempDir::new().unwrap();
        let db = open_small_buffer(dir.path());
        let mut seq = 0u64;

        b.iter(|| {
            let key = make_key(seq);
            db.put(black_box(&key), black_box(VALUE_128B.as_slice()))
                .unwrap();
            seq += 1;
        });

        db.close().unwrap();
    });

    group.finish();
}

// ================================================================================================
// Read benchmarks
// ================================================================================================

/// Benchmark group for read (`get`) operations.
///
/// # Sub-benchmarks
///
/// ## `memtable_hit`
///
/// **Scenario:** Reads randomly from 10,000 keys that all reside in the active memtable
/// (64 MiB buffer, nothing flushed).
///
/// **What it measures:** Pure in-memory skip-list lookup latency. This is the fastest read
/// path in the engine.
///
/// **Expected behaviour:** Sub-microsecond. Performance is dominated by key comparison cost,
/// not I/O.
///
/// ## `memtable_miss`
///
/// **Scenario:** Queries keys that were never inserted while the memtable contains 10,000
/// entries.
///
/// **What it measures:** The overhead of the negative-lookup path — traversing the memtable
/// skip list to confirm absence.
///
/// **Expected behaviour:** Similar or slightly faster than a hit, since a miss can
/// short-circuit once the skip-list search overshoots.
///
/// ## `sstable_hit`
///
/// **Scenario:** Reads randomly from 5,000 keys that have been flushed to SSTables. The
/// database is reopened so the memtable is empty.
///
/// **What it measures:** Full on-disk read path: bloom filter probe → index lookup →
/// data-block read → decompress → binary search within the block.
///
/// **Expected behaviour:** Low-microsecond range; significantly slower than memtable reads
/// due to disk I/O, but still fast because data fits in OS page cache.
///
/// ## `sstable_miss`
///
/// **Scenario:** Queries keys that do not exist in any SSTable (5,000 keys on disk, queries
/// target IDs above that range).
///
/// **What it measures:** Bloom filter effectiveness. A well-tuned bloom filter should reject
/// the vast majority of non-existent queries without touching the data blocks.
///
/// **Expected behaviour:** Faster than `sstable_hit` because the bloom filter rejects the
/// query before any data-block I/O.
fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");

    // --- get: from memtable (all data in memory) ---
    {
        let dir = TempDir::new().unwrap();
        let db = open_memtable_only(dir.path());
        let n = 10_000u64;
        for i in 0..n {
            db.put(&make_key(i), VALUE_128B).unwrap();
        }

        group.bench_function("memtable_hit", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key(i % n);
                let _ = black_box(db.get(black_box(&key)).unwrap());
                i += 1;
            });
        });

        group.bench_function("memtable_miss", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key(n + i);
                let _ = black_box(db.get(black_box(&key)).unwrap());
                i += 1;
            });
        });

        db.close().unwrap();
    }

    // --- get: from SSTables (data flushed to disk) ---
    {
        let dir = TempDir::new().unwrap();
        let n = 5_000u64;
        prepopulate(dir.path(), n, VALUE_128B);
        // Reopen — memtable is empty, all data in SSTables.
        let db = Db::open(dir.path(), DbConfig::default()).unwrap();

        group.bench_function("sstable_hit", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key(i % n);
                let _ = black_box(db.get(black_box(&key)).unwrap());
                i += 1;
            });
        });

        group.bench_function("sstable_miss", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let key = make_key(n + i);
                let _ = black_box(db.get(black_box(&key)).unwrap());
                i += 1;
            });
        });

        db.close().unwrap();
    }

    group.finish();
}

// ================================================================================================
// Delete benchmarks
// ================================================================================================

/// Benchmark group for delete operations.
///
/// # Sub-benchmarks
///
/// ## `point`
///
/// **Scenario:** Deletes a single unique key per iteration using a large write buffer
/// (no flushes).
///
/// **What it measures:** The cost of inserting a tombstone marker into the WAL and memtable.
/// Structurally identical to a `put` but writes a deletion sentinel.
///
/// **Expected behaviour:** Nearly identical to `put/memtable_only/128B` since the write
/// path is the same — only the value type differs.
///
/// ## `range`
///
/// **Scenario:** Issues a range-delete covering 100 consecutive keys per iteration.
///
/// **What it measures:** Range tombstone insertion cost. Unlike point deletes, a range
/// delete records a single `[start, end)` interval rather than one tombstone per key.
///
/// **Expected behaviour:** Should be comparable to a single point delete because the
/// engine records one range-tombstone entry regardless of how many keys the range covers.
fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");

    // --- point delete ---
    group.bench_function("point", |b| {
        let dir = TempDir::new().unwrap();
        let db = open_memtable_only(dir.path());
        let mut seq = 0u64;

        b.iter(|| {
            let key = make_key(seq);
            db.delete(black_box(&key)).unwrap();
            seq += 1;
        });

        db.close().unwrap();
    });

    // --- range delete ---
    group.bench_function("range", |b| {
        let dir = TempDir::new().unwrap();
        let db = open_memtable_only(dir.path());
        let mut seq = 0u64;

        b.iter(|| {
            let start = make_key(seq);
            let end = make_key(seq + 100);
            db.delete_range(black_box(&start), black_box(&end)).unwrap();
            seq += 100;
        });

        db.close().unwrap();
    });

    group.finish();
}

// ================================================================================================
// Scan benchmarks
// ================================================================================================

/// Benchmark group for ordered range-scan operations.
///
/// Tests scan performance across two storage layers (memtable and SSTable) and three range
/// sizes (10, 100, 1,000 keys). Criterion's `Throughput::Elements` annotation enables
/// per-key throughput reporting in the output.
///
/// # Sub-benchmarks
///
/// ## `memtable/{10,100,1000}_keys`
///
/// **Scenario:** Scans a range of N keys from a memtable containing 10,000 entries.
///
/// **What it measures:** In-memory ordered iteration cost. The skip-list cursor advances
/// through N consecutive entries and collects results into a `Vec`.
///
/// **Expected behaviour:** Near-linear scaling with range size. Per-key cost should be
/// very low because the scan is a sequential walk through the skip list.
///
/// ## `sstable/{10,100,1000}_keys`
///
/// **Scenario:** Scans a range of N keys from SSTables (5,000 keys flushed to disk,
/// memtable empty after reopen).
///
/// **What it measures:** On-disk sequential read performance including block decoding
/// and merge iteration across potentially multiple SSTables.
///
/// **Expected behaviour:** Slower than memtable scans due to block decompression and
/// possible cross-SSTable merging. Per-key cost should still decrease with larger ranges
/// due to amortisation of seek overhead.
fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");

    // --- scan from memtable ---
    {
        let dir = TempDir::new().unwrap();
        let db = open_memtable_only(dir.path());
        let n = 10_000u64;
        for i in 0..n {
            db.put(&make_key(i), VALUE_128B).unwrap();
        }

        for &range_size in &[10u64, 100, 1000] {
            group.throughput(Throughput::Elements(range_size));
            group.bench_function(
                BenchmarkId::new("memtable", format!("{range_size}_keys")),
                |b| {
                    let mut offset = 0u64;
                    b.iter(|| {
                        let start = make_key(offset % (n - range_size));
                        let end = make_key(offset % (n - range_size) + range_size);
                        let results = db.scan(black_box(&start), black_box(&end)).unwrap();
                        black_box(&results);
                        offset += 1;
                    });
                },
            );
        }

        db.close().unwrap();
    }

    // --- scan from SSTables ---
    {
        let dir = TempDir::new().unwrap();
        let n = 5_000u64;
        prepopulate(dir.path(), n, VALUE_128B);
        let db = Db::open(dir.path(), DbConfig::default()).unwrap();

        for &range_size in &[10u64, 100, 1000] {
            group.throughput(Throughput::Elements(range_size));
            group.bench_function(
                BenchmarkId::new("sstable", format!("{range_size}_keys")),
                |b| {
                    let mut offset = 0u64;
                    b.iter(|| {
                        let start = make_key(offset % (n - range_size));
                        let end = make_key(offset % (n - range_size) + range_size);
                        let results = db.scan(black_box(&start), black_box(&end)).unwrap();
                        black_box(&results);
                        offset += 1;
                    });
                },
            );
        }

        db.close().unwrap();
    }

    group.finish();
}

// ================================================================================================
// Compaction benchmarks
// ================================================================================================

/// Benchmark group for compaction operations.
///
/// # Sub-benchmarks
///
/// ## `major/1000` and `major/5000`
///
/// **Scenario:** Prepopulates N keys (1,000 or 5,000) via a small buffer (triggering
/// multiple flushes and creating several SSTables), then reopens the database and runs a
/// full major compaction.
///
/// **What it measures:** End-to-end major compaction latency — reading all SSTables,
/// performing a K-way merge, removing obsolete entries, and writing a single compacted
/// SSTable. This is the most expensive background operation in an LSM-tree engine.
///
/// **Expected behaviour:** Millisecond range. The 5,000-key case should be roughly
/// proportional to the 1,000-key case (slightly less than 5× due to fixed per-compaction
/// overhead). Sample size is reduced to 10 because each iteration is slow.
fn bench_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction");
    // Major compaction is slow — reduce sample count.
    group.sample_size(10);

    for &count in &[1_000u64, 5_000] {
        group.bench_function(BenchmarkId::new("major", count), |b| {
            b.iter_batched(
                || {
                    let dir = TempDir::new().unwrap();
                    prepopulate(dir.path(), count, VALUE_128B);
                    let db = Db::open(dir.path(), DbConfig::default()).unwrap();
                    (dir, db)
                },
                |(_dir, db)| {
                    let _ = black_box(db.major_compact().unwrap());
                    db.close().unwrap();
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ================================================================================================
// Recovery benchmark
// ================================================================================================

/// Benchmark group for database recovery (open) latency.
///
/// # Sub-benchmarks
///
/// ## `open_existing/1000` and `open_existing/10000`
///
/// **Scenario:** A database is prepopulated with N keys and closed. Each iteration opens
/// the database from that existing state, which replays the WAL and rebuilds the manifest.
///
/// **What it measures:** Cold-start recovery time — manifest loading, SSTable catalogue
/// reconstruction, WAL replay for any data that was not flushed, and bloom filter
/// initialisation. This is critical for services that do rolling restarts.
///
/// **Expected behaviour:** Scales with the number of SSTables and any un-flushed WAL
/// segments. The 10,000-key case should be noticeably slower because it produces more
/// SSTables and a larger manifest.
fn bench_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery");
    group.sample_size(10);

    for &count in &[1_000u64, 10_000] {
        group.bench_function(BenchmarkId::new("open_existing", count), |b| {
            let dir = TempDir::new().unwrap();
            prepopulate(dir.path(), count, VALUE_128B);

            b.iter(|| {
                let db = Db::open(dir.path(), DbConfig::default()).unwrap();
                black_box(&db);
                db.close().unwrap();
            });
        });
    }

    group.finish();
}

// ================================================================================================
// Value-size scaling
// ================================================================================================

/// Benchmark group for value-size scaling analysis.
///
/// # Sub-benchmarks
///
/// ## `put/{64B,256B,1K,4K}`
///
/// **Scenario:** Writes a single key with a value of the specified size into a memtable-
/// only database (64 MiB buffer). Criterion's `Throughput::Bytes` annotation enables
/// bytes-per-second reporting.
///
/// **What it measures:** How write latency and throughput scale with value size. Isolates
/// the cost of serialising and copying larger payloads through the WAL and memtable
/// without interference from background flushes.
///
/// **Expected behaviour:** Latency increases roughly linearly with value size for small
/// values, but the relationship flattens at larger sizes because fixed overheads (key
/// encoding, skip-list node allocation) become proportionally smaller. Bytes/second
/// throughput should increase for larger values.
fn bench_value_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_size");

    let sizes: &[(&str, usize)] = &[("64B", 64), ("256B", 256), ("1K", 1024), ("4K", 4096)];

    for &(label, size) in sizes {
        let value = vec![0xEF_u8; size];

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(BenchmarkId::new("put", label), |b| {
            let dir = TempDir::new().unwrap();
            let db = open_memtable_only(dir.path());
            let mut seq = 0u64;
            b.iter(|| {
                let key = make_key(seq);
                db.put(black_box(&key), black_box(&value)).unwrap();
                seq += 1;
            });
            db.close().unwrap();
        });
    }

    group.finish();
}

// ================================================================================================
// Group registration
// ================================================================================================

criterion_group!(
    benches,
    bench_put,
    bench_get,
    bench_delete,
    bench_scan,
    bench_compaction,
    bench_recovery,
    bench_value_sizes,
);

criterion_main!(benches);
