# Benchmarking Guide

This document explains how to run, read, and extend the AeternusDB benchmark
suite. The suite is built on [Criterion.rs](https://bheisler.github.io/criterion.rs/)
and covers both **micro-benchmarks** (individual operations) and **YCSB-style
macro-benchmarks** (mixed workloads).

---

## Quick Start

```bash
# Run everything (takes several minutes)
cargo bench

# Run only micro-benchmarks
cargo bench --bench micro

# Run only YCSB workloads
cargo bench --bench ycsb

# Filter by pattern
cargo bench --bench micro -- "put"
cargo bench --bench micro -- "get/sstable"
cargo bench --bench ycsb -- "A"

# Quick mode (fewer samples, faster turnaround)
cargo bench -- --quick

# Reduce measurement time for faster local iteration
cargo bench --bench micro -- --warm-up-time 1 --measurement-time 3
```

HTML reports are generated at `target/criterion/report/index.html`.

---

## Benchmark Suites

### Micro-benchmarks (`benches/micro.rs`)

| Group | Sub-benchmark | Description |
|-------|---------------|-------------|
| **put** | `memtable_only/128B` | Single put, 128 B value, large buffer (no flush) |
| | `memtable_only/1K` | Single put, 1 KiB value, large buffer (no flush) |
| | `sequential_with_flush` | Sequential 128 B puts, tiny buffer (triggers flushes) |
| **get** | `memtable_hit` | Random read from 10 K in-memory keys |
| | `memtable_miss` | Read non-existent key from populated memtable |
| | `sstable_hit` | Random read from 5 K keys on disk |
| | `sstable_miss` | Read non-existent key (bloom filter path) |
| **delete** | `point` | Single tombstone insert |
| | `range` | Range-delete covering 100 keys |
| **scan** | `memtable/{10,100,1000}_keys` | In-memory ordered scan |
| | `sstable/{10,100,1000}_keys` | On-disk ordered scan |
| **compaction** | `major/1000` | Full merge of ~1 K keys |
| | `major/5000` | Full merge of ~5 K keys |
| **recovery** | `open_existing/1000` | Reopen DB with 1 K keys |
| | `open_existing/10000` | Reopen DB with 10 K keys |
| **value_size** | `put/{64B,256B,1K,4K}` | Write throughput vs. value size |
| **concurrent** | `readers/{1,2,4}` | Multi-threaded read scaling |
| | `read_under_write/{1,2}_writer` | Read latency under concurrent write pressure |
| **overwrite** | `update_memtable` | Overwrite existing keys in memtable |
| | `update_sstable` | Overwrite keys that exist in SSTables |
| **dataset_scaling** | `get/{1K,10K,50K,100K}` | Point-read latency vs. dataset size |
| **tombstone_scan** | `dense_tombstones/{0%,25%,50%,75%}` | Scan throughput with varying tombstone density |
| **close** | `empty` | Shutdown latency (empty DB) |
| | `with_data/{1000,5000}` | Shutdown latency with pending data |
| **key_size** | `put/{16B,64B,256B,512B}` | Write latency vs. key size |
| | `get/{16B,64B,256B,512B}` | Read latency vs. key size |

### YCSB workloads (`benches/ycsb.rs`)

| Workload | Mix | Real-world analogy |
|----------|-----|--------------------|
| **Load** | 100% insert (10 K records) | Initial bulk load |
| **A** | 50% read, 50% update | Session store |
| **B** | 95% read, 5% update | Photo tagging |
| **C** | 100% read | User profile cache |
| **D** | 95% read, 5% insert | Timeline / status feed |
| **E** | 95% scan, 5% insert | Threaded conversations |
| **F** | 50% read, 50% RMW | User DB with counters |

---

## Reading the Output

Criterion prints a summary for each benchmark:

```
put/memtable_only/128B
                        time:   [245.12 ns 248.67 ns 252.35 ns]
                        change: [-1.2345% +0.1234% +1.5678%] (p = 0.12 > 0.05)
                        No change in performance detected.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild
```

| Field | Meaning |
|-------|---------|
| `time: [low est high]` | 95% confidence interval for the mean execution time |
| `change: [low mid high]` | Percentage change vs. the saved baseline |
| `p = ...` | Statistical significance — `p < 0.05` means the change is likely real |
| `No change` / `Performance has regressed` | Criterion's verdict |

### Reference Latency Ranges

These are rough expectations on modern hardware (NVMe SSD, recent x86-64):

| Operation | Expected range |
|-----------|---------------|
| `put/memtable_only` | 200–800 ns |
| `get/memtable_hit` | 100–500 ns |
| `get/sstable_hit` | 1–10 µs |
| `get/sstable_miss` (bloom) | 0.5–3 µs |
| `scan/memtable/100_keys` | 5–30 µs |
| `scan/sstable/100_keys` | 20–100 µs |
| `compaction/major/1000` | 5–50 ms |
| `recovery/open_existing/1000` | 1–20 ms |
| YCSB workload (5 K ops) | 20–200 ms |

---

## Comparing Against a Baseline

```bash
# Save the current results as a baseline
cargo bench -- --save-baseline before

# Make your changes, then compare
cargo bench -- --baseline before
```

Criterion will report the percentage change for every benchmark.

---

## CI Integration

The GitHub Actions workflow (`.github/workflows/bench.yml`) runs benchmarks on
every push to `main` and every PR:

1. Runs `cargo bench` with `--output-format bencher`.
2. Stores results via
   [github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark).
3. On `main` pushes, results are auto-pushed to the `gh-pages` branch.
4. On PRs, a comment shows the comparison against the latest `main` results.
5. An alert is raised if any benchmark regresses by more than 15%.

### Viewing Historical Results

After the first push to `main`, benchmark charts are available at:

```
https://<owner>.github.io/aeternusdb/dev/bench/
```

For this repository:

```
https://kamil-kielbasa.github.io/aeternusdb/dev/bench/
```

### Criterion HTML Reports

In addition to the interactive chart, the full Criterion HTML reports (violin
plots, regression analysis, per-benchmark comparisons) are published on every
push to `main`:

```
https://kamil-kielbasa.github.io/aeternusdb/criterion/report/index.html
```

Each benchmark group has its own sub-page — e.g.:

- `.../criterion/put/report/index.html`
- `.../criterion/get/report/index.html`
- `.../criterion/ycsb/workload/report/index.html`

> **Setup note:** GitHub Pages must be configured to deploy from the `gh-pages`
> branch. Go to *Settings → Pages → Source* and select `gh-pages` / `/ (root)`.

---

## Profiling Hot Spots

### With `flamegraph`

```bash
cargo install flamegraph

# Profile a specific benchmark
cargo flamegraph --bench micro -- --bench "put/memtable_only/128B" --profile-time 10
```

### With `perf`

```bash
# Build benchmarks without running them
cargo bench --bench micro --no-run

# Find the binary
BENCH_BIN=$(find target/release/deps -name 'micro-*' -executable | head -1)

# Record
perf record -g "$BENCH_BIN" --bench "put/memtable_only/128B" --profile-time 10
perf report
```

### With `samply`

```bash
cargo install samply
cargo bench --bench micro --no-run
BENCH_BIN=$(find target/release/deps -name 'micro-*' -executable | head -1)
samply record "$BENCH_BIN" --bench "put/memtable_only/128B" --profile-time 10
```

---

## Adding a New Benchmark

1. Add a function in `benches/micro.rs` or `benches/ycsb.rs`.
2. Register it in the `criterion_group!` macro at the bottom of the file.
3. Run `cargo bench --bench <suite> -- "<new_name>"` to verify.
4. Update this document's tables if relevant.

---

## Tips

- **Filter aggressively** during development: `cargo bench -- "put"` is much
  faster than running the full suite.
- **Use `--quick`** for smoke tests — Criterion skips statistical analysis.
- **Adjust `--sample-size`** for slow benchmarks:
  `cargo bench -- "compaction" --sample-size 10`.
- **Warm the OS page cache** before SSTable benchmarks if you want to measure
  cached performance (the default). For cold-cache measurements, drop caches
  between runs (`echo 3 | sudo tee /proc/sys/vm/drop_caches`).
