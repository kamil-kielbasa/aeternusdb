//! Stress tests: large-scale mixed-operation workloads that exercise the engine
//! under realistic churn — multiple SSTables with realistic sizes, heavy overwrites,
//! tombstone accumulation, compaction, and recovery after thousands of operations.
//!
//! These tests are the "big-gun" verification layer: they execute thousands of
//! randomised operations against a predictable PRNG sequence and verify that
//! every key's final state matches an in-memory oracle (`HashMap`). Categories:
//!
//! 1. **Heavy mixed CRUD** — 8 000 random put/delete/range-delete/overwrite ops
//!    on 500 keys, then point-get every key.
//! 2. **Scan consistency** — same random workload, verified with a full-range
//!    `scan()` against a sorted expected-live set.
//! 3. **Write-delete-rewrite churn** — 50 deterministic rounds of
//!    "write all → delete half → resurrect quarter" on 200 keys.
//! 4. **Recovery after massive session** — graceful `close()` → `reopen` →
//!    full verify (get + scan).
//! 5. **Crash recovery after massive session** — drop without `close()` →
//!    `reopen` → full verify.
//! 6. **Scan with many range tombstones** — 1 000 keys, 50 overlapping
//!    range-deletes, selective resurrections, verified via scan + get.
//! 7. **Minor compaction multi-round** — 3 rounds of heavy writes →
//!    `flush_all_frozen()` → `minor_compact()` loop → oracle verify.
//! 8. **Tombstone compaction under heavy deletes** — write 500 keys, delete
//!    ~80 %, `tombstone_compact()` → verify size reduction + correctness.
//! 9. **Full lifecycle (minor → major)** — heavy writes → flush → minor →
//!    major → verify everything merged into minimal SSTables.
//! 10. **Interleaved writes and compaction** — 10 rounds of write → flush →
//!     minor compact → verify, then final major compact + scan.
//! 11. **Compaction + recovery** — full compaction pipeline → close → reopen →
//!     verify durability of compacted state.
//!
//! Tests 1–6 use `default_config()` (4 KB write buffer) to produce
//! realistic multi-KB SSTables.
//! Tests 7–11 use `compaction_stress_config()` (512 B write buffer,
//! `min_threshold = 2`) to generate many small SSTables that trigger
//! compaction readily.
//!
//! These tests use deterministic pseudo-random sequences (simple LCG) so failures
//! are reproducible without external RNG dependencies.
//!
//! ## Layer coverage
//! - All tests use `memtable_sstable` (many SSTables from heavy writes)
//!
//! ## Run
//! All tests in this module are `#[ignore]`d by default (slow).
//! ```sh
//! cargo test -- --ignored tests_stress   # run only stress tests
//! cargo test -- --include-ignored         # run everything
//! ```
//!
//! ## See also
//! - [`tests_hardening`] — concurrency and extreme config tests
//! - [`tests_crash_recovery`] — basic crash-recovery tests

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::EngineConfig;
    use crate::engine::tests::helpers::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    /// Config tuned for compaction stress tests:
    /// - 512-byte write buffer → many small SSTables
    /// - min_threshold = 2 → compaction triggers with just 2 same-bucket SSTables
    /// - tombstone_bloom_fallback + tombstone_range_drop enabled for tombstone compaction
    fn compaction_stress_config() -> EngineConfig {
        init_tracing();
        EngineConfig {
            write_buffer_size: 512,
            compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 256,
            min_threshold: 2,
            max_threshold: 32,
            tombstone_ratio_threshold: 0.15,
            tombstone_compaction_interval: 0, // no age gate for stress tests
            tombstone_bloom_fallback: true,
            tombstone_range_drop: true,
            thread_pool_size: 2,
        }
    }

    // ----------------------------------------------------------------
    // Deterministic pseudo-random number generator (LCG)
    // ----------------------------------------------------------------

    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed)
        }

        fn next_u64(&mut self) -> u64 {
            // LCG parameters from Numerical Recipes
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0
        }

        fn next_usize(&mut self, bound: usize) -> usize {
            (self.next_u64() % bound as u64) as usize
        }
    }

    // ----------------------------------------------------------------
    // Helper: verify every key in the expected map against the engine
    // ----------------------------------------------------------------

    fn verify_all(engine: &Engine, expected: &HashMap<Vec<u8>, Option<Vec<u8>>>) {
        for (key, expected_val) in expected {
            let actual = engine.get(key.clone()).expect("get must not error");
            assert_eq!(
                &actual,
                expected_val,
                "Mismatch for key {:?}: expected {:?}, got {:?}",
                String::from_utf8_lossy(key),
                expected_val
                    .as_ref()
                    .map(|v| String::from_utf8_lossy(v).to_string()),
                actual
                    .as_ref()
                    .map(|v| String::from_utf8_lossy(v).to_string()),
            );
        }
    }

    /// Apply a range-delete to the expected map (mark keys in [start, end) as None).
    fn apply_range_delete(
        expected: &mut HashMap<Vec<u8>, Option<Vec<u8>>>,
        start: &[u8],
        end: &[u8],
    ) {
        let keys: Vec<Vec<u8>> = expected
            .keys()
            .filter(|k| k.as_slice() >= start && k.as_slice() < end)
            .cloned()
            .collect();
        for k in keys {
            expected.insert(k, None);
        }
    }

    // ================================================================
    // 1. Heavy mixed CRUD — 8000+ ops on 500 keys, verify every key via get
    // ================================================================

    /// # Scenario
    /// Exercises the engine with 8 000 randomised operations (put 60 %,
    /// point-delete 20 %, range-delete 15 %, overwrite 5 %) across 500
    /// keys, then verifies every key against an in-memory oracle.
    ///
    /// # Starting environment
    /// Fresh engine with `default_config()` (4 KB write buffer) — flushes
    /// occur naturally during the workload, producing multiple SSTables.
    ///
    /// # Actions
    /// 1. Execute 8 000 PRNG-driven operations (seed `42`), tracking the
    ///    expected state in a `HashMap<Key, Option<Value>>`.
    /// 2. Get each of the 500 keys and compare to the oracle.
    /// 3. Assert at least 2 SSTables were created.
    ///
    /// # Expected behavior
    /// Every key's `get()` result matches the oracle. Multiple SSTables
    /// exist, confirming cross-layer merges were exercised.
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_heavy_mixed_crud() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let mut rng = Rng::new(42);

        let num_keys = 500;
        let num_ops = 8000;

        for _ in 0..num_ops {
            let op = rng.next_usize(100);
            let idx = rng.next_usize(num_keys);
            let key = format!("key_{:04}", idx).into_bytes();

            if op < 60 {
                // 60%: put
                let round = rng.next_u64();
                let value = format!("val_{}_r{}", idx, round).into_bytes();
                engine.put(key.clone(), value.clone()).unwrap();
                expected.insert(key, Some(value));
            } else if op < 80 {
                // 20%: point delete
                engine.delete(key.clone()).unwrap();
                expected.insert(key, None);
            } else if op < 95 {
                // 15%: range delete over a small window
                let end_idx = (idx + rng.next_usize(20) + 1).min(num_keys);
                let start_key = format!("key_{:04}", idx).into_bytes();
                let end_key = format!("key_{:04}", end_idx).into_bytes();
                engine
                    .delete_range(start_key.clone(), end_key.clone())
                    .unwrap();
                apply_range_delete(&mut expected, &start_key, &end_key);
            } else {
                // 5%: overwrite with different value
                let value = format!("overwrite_{}_r{}", idx, rng.next_u64()).into_bytes();
                engine.put(key.clone(), value.clone()).unwrap();
                expected.insert(key, Some(value));
            }
        }

        // Verify every key
        verify_all(&engine, &expected);

        engine.flush_all_frozen().unwrap();
        let stats = engine.stats().unwrap();
        assert!(
            stats.sstables_count >= 2,
            "Heavy workload should produce multiple SSTables, got {}",
            stats.sstables_count
        );
    }

    // ================================================================
    // 2. Scan consistency after heavy writes
    // ================================================================

    /// # Scenario
    /// Same randomised workload as test 1, but verified via `scan()` instead
    /// of individual `get()` calls.
    ///
    /// # Starting environment
    /// Fresh engine with `default_config()` — no data.
    ///
    /// # Actions
    /// 1. Execute 8 000 PRNG-driven operations (seed `0xDEAD`) with the same
    ///    op-distribution (55 % put, 20 % delete, 15 % range-delete, 10 % overwrite).
    /// 2. Build sorted expected-live set from the oracle.
    /// 3. Full-range scan `[key_, key_\xff)`.
    /// 4. Compare count, sort order, and each key-value pair.
    ///
    /// # Expected behavior
    /// Scan result length matches expected, entries are strictly sorted,
    /// and every key-value pair is identical to the oracle.
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_scan_consistency() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let mut rng = Rng::new(0xDEAD);

        let num_keys = 500;
        let num_ops = 8000;

        for _ in 0..num_ops {
            let op = rng.next_usize(100);
            let idx = rng.next_usize(num_keys);
            let key = format!("key_{:04}", idx).into_bytes();

            if op < 55 {
                let value = format!("v{}_{}", idx, rng.next_u64()).into_bytes();
                engine.put(key.clone(), value.clone()).unwrap();
                expected.insert(key, Some(value));
            } else if op < 75 {
                engine.delete(key.clone()).unwrap();
                expected.insert(key, None);
            } else if op < 90 {
                let end_idx = (idx + rng.next_usize(15) + 1).min(num_keys);
                let start_key = format!("key_{:04}", idx).into_bytes();
                let end_key = format!("key_{:04}", end_idx).into_bytes();
                engine
                    .delete_range(start_key.clone(), end_key.clone())
                    .unwrap();
                apply_range_delete(&mut expected, &start_key, &end_key);
            } else {
                let value = format!("ow{}_{}", idx, rng.next_u64()).into_bytes();
                engine.put(key.clone(), value.clone()).unwrap();
                expected.insert(key, Some(value));
            }
        }

        // Collect surviving keys from expected map
        let mut expected_live: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
            .collect();
        expected_live.sort_by(|a, b| a.0.cmp(&b.0));

        // Full scan
        let scan_results = collect_scan(&engine, b"key_", b"key_\xff");

        assert_eq!(
            scan_results.len(),
            expected_live.len(),
            "scan count mismatch: got {} expected {}",
            scan_results.len(),
            expected_live.len()
        );

        // Verify sorted order
        for pair in scan_results.windows(2) {
            assert!(pair[0].0 < pair[1].0, "scan must be strictly sorted");
        }

        // Verify each key-value pair
        for (actual, expected) in scan_results.iter().zip(expected_live.iter()) {
            assert_eq!(
                actual,
                expected,
                "scan mismatch at key {:?}",
                String::from_utf8_lossy(&expected.0)
            );
        }
    }

    // ================================================================
    // 3. Write-delete-rewrite churn on same keys
    // ================================================================

    /// # Scenario
    /// 50 deterministic rounds of "write all → delete even → resurrect
    /// every 4th" on 200 keys, then verify final state and scan count.
    ///
    /// # Starting environment
    /// Fresh engine with `default_config()` — no data.
    ///
    /// # Actions
    /// For each of 50 rounds:
    /// 1. Put all 200 keys with round-tagged values.
    /// 2. Delete all even-indexed keys.
    /// 3. Re-insert every 4th key (a subset of the deleted even keys) with
    ///    `"resurrected_r{round}_{i}"`.
    ///
    /// After round 49, get all 200 keys and run a full scan.
    ///
    /// # Expected behavior
    /// - Keys `i % 4 == 0`: `Some("resurrected_r49_{i}")`.
    /// - Keys `i % 2 == 0 && i % 4 != 0`: `None`.
    /// - Odd keys: `Some("val_r49_{i}")`.
    /// - Scan returns exactly 150 surviving entries (100 odd + 50 every-4th).
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_write_delete_rewrite() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), default_config()).unwrap();

        let num_keys = 200;
        let num_rounds = 50;

        // Each round: write all keys, delete half, rewrite a quarter
        for round in 0..num_rounds {
            for i in 0..num_keys {
                engine
                    .put(
                        format!("key_{:04}", i).into_bytes(),
                        format!("val_r{}_{}", round, i).into_bytes(),
                    )
                    .unwrap();
            }

            // Delete even keys
            for i in (0..num_keys).step_by(2) {
                engine.delete(format!("key_{:04}", i).into_bytes()).unwrap();
            }

            // Rewrite every 4th key (a subset of deleted even keys)
            for i in (0..num_keys).step_by(4) {
                engine
                    .put(
                        format!("key_{:04}", i).into_bytes(),
                        format!("resurrected_r{}_{}", round, i).into_bytes(),
                    )
                    .unwrap();
            }
        }

        // After the final round (49):
        // - keys divisible by 4 → "resurrected_r49_*"
        // - keys divisible by 2 but not 4 → deleted
        // - odd keys → "val_r49_*"
        let last = num_rounds - 1;
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let actual = engine.get(key).unwrap();
            if i % 4 == 0 {
                assert_eq!(
                    actual,
                    Some(format!("resurrected_r{}_{}", last, i).into_bytes()),
                    "key_{:04} (mod 4 == 0) should be resurrected",
                    i
                );
            } else if i % 2 == 0 {
                assert_eq!(
                    actual, None,
                    "key_{:04} (even, not mod 4) should be deleted",
                    i
                );
            } else {
                assert_eq!(
                    actual,
                    Some(format!("val_r{}_{}", last, i).into_bytes()),
                    "key_{:04} (odd) should have latest round value",
                    i
                );
            }
        }

        // Scan should be sorted and match get results
        let results = collect_scan(&engine, b"key_", b"key_\xff");
        for pair in results.windows(2) {
            assert!(pair[0].0 < pair[1].0, "scan must be sorted");
        }
        // Count: odd keys (100) + every 4th key (50) = 150
        assert_eq!(results.len(), 150, "expected 150 surviving keys");
    }

    // ================================================================
    // 4. Recovery after massive session (close → reopen → full verify)
    // ================================================================

    /// # Scenario
    /// Graceful recovery: run 8 000 random operations, close the engine,
    /// reopen, and verify every key (get + scan).
    ///
    /// # Starting environment
    /// Fresh engine with `default_config()` — no data.
    ///
    /// # Actions
    /// 1. Execute 8 000 PRNG-driven operations (seed `0xCAFE`).
    /// 2. `close()` the engine (flushes active memtable).
    /// 3. `reopen()` the engine from the same directory.
    /// 4. Verify every key against the oracle via `get()`.
    /// 5. Full-range scan — compare count and pairs to oracle.
    ///
    /// # Expected behavior
    /// All data survives the close/reopen cycle. Every key matches the
    /// oracle, and the scan result is sorted and complete.
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_recovery_massive() {
        let dir = TempDir::new().unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let mut rng = Rng::new(0xCAFE);

        {
            let engine = Engine::open(dir.path(), default_config()).unwrap();

            let num_keys = 500;
            let num_ops = 8000;

            for _ in 0..num_ops {
                let op = rng.next_usize(100);
                let idx = rng.next_usize(num_keys);
                let key = format!("key_{:04}", idx).into_bytes();

                if op < 60 {
                    let value = format!("v{}_{}", idx, rng.next_u64()).into_bytes();
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, Some(value));
                } else if op < 80 {
                    engine.delete(key.clone()).unwrap();
                    expected.insert(key, None);
                } else if op < 95 {
                    let end_idx = (idx + rng.next_usize(20) + 1).min(num_keys);
                    let start_key = format!("key_{:04}", idx).into_bytes();
                    let end_key = format!("key_{:04}", end_idx).into_bytes();
                    engine
                        .delete_range(start_key.clone(), end_key.clone())
                        .unwrap();
                    apply_range_delete(&mut expected, &start_key, &end_key);
                } else {
                    let value = format!("ow{}_{}", idx, rng.next_u64()).into_bytes();
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, Some(value));
                }
            }

            engine.close().unwrap();
        }

        // Reopen and verify every key
        let engine = reopen(dir.path());
        verify_all(&engine, &expected);

        // Also verify scan
        let mut expected_live: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
            .collect();
        expected_live.sort_by(|a, b| a.0.cmp(&b.0));

        let scan_results = collect_scan(&engine, b"key_", b"key_\xff");
        assert_eq!(scan_results.len(), expected_live.len());
        for (actual, exp) in scan_results.iter().zip(expected_live.iter()) {
            assert_eq!(actual, exp);
        }
    }

    // ================================================================
    // 5. Crash recovery after massive session (drop without close)
    // ================================================================

    /// # Scenario
    /// Crash recovery: run 8 000 random operations, drop the engine
    /// *without* calling `close()`, reopen, and verify every key.
    ///
    /// # Starting environment
    /// Fresh engine with `default_config()` — no data.
    ///
    /// # Actions
    /// 1. Execute 8 000 PRNG-driven operations (seed `0xBEEF`).
    /// 2. Drop the engine (simulates crash — active memtable not flushed).
    /// 3. `reopen()` from the same directory (WAL replay recovers the
    ///    active memtable).
    /// 4. Verify every key against the oracle via `get()`.
    ///
    /// # Expected behavior
    /// All data survives the crash. WAL replay restores the active
    /// memtable contents, and every key matches the oracle.
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_crash_recovery_massive() {
        let dir = TempDir::new().unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let mut rng = Rng::new(0xBEEF);

        {
            let engine = Engine::open(dir.path(), default_config()).unwrap();

            let num_keys = 500;
            let num_ops = 8000;

            for _ in 0..num_ops {
                let op = rng.next_usize(100);
                let idx = rng.next_usize(num_keys);
                let key = format!("key_{:04}", idx).into_bytes();

                if op < 60 {
                    let value = format!("v{}_{}", idx, rng.next_u64()).into_bytes();
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, Some(value));
                } else if op < 80 {
                    engine.delete(key.clone()).unwrap();
                    expected.insert(key, None);
                } else if op < 95 {
                    let end_idx = (idx + rng.next_usize(20) + 1).min(num_keys);
                    let start_key = format!("key_{:04}", idx).into_bytes();
                    let end_key = format!("key_{:04}", end_idx).into_bytes();
                    engine
                        .delete_range(start_key.clone(), end_key.clone())
                        .unwrap();
                    apply_range_delete(&mut expected, &start_key, &end_key);
                } else {
                    let value = format!("ow{}_{}", idx, rng.next_u64()).into_bytes();
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, Some(value));
                }
            }

            // Drop without close — simulates crash
        }

        let engine = reopen(dir.path());
        verify_all(&engine, &expected);
    }

    // ================================================================
    // 6. Multi-SSTable scan with many range tombstones
    // ================================================================

    /// # Scenario
    /// A dense tombstone landscape: 1 000 keys, 50 overlapping range-deletes
    /// (stride-of-7 vs window-of-10 creates complex overlaps), selective
    /// resurrections every 13th key, verified via both scan and individual gets.
    ///
    /// # Starting environment
    /// Fresh engine with `default_config()` — no data.
    ///
    /// # Actions
    /// 1. Insert 1 000 keys (`key_0000`–`key_0999`).
    /// 2. Issue 50 range-deletes: for `r` in 0..50, delete `[key_{r*7}, key_{r*7+10})`.
    /// 3. Resurrect every 13th key (`key_0000`, `key_0013`, …) with
    ///    `"resurrected_{:04}"`.
    /// 4. Build ground-truth expected-live set.
    /// 5. Full-range scan — compare count, sort order, and each pair.
    /// 6. Individual `get()` for all 1 000 keys.
    ///
    /// # Expected behavior
    /// - Resurrected keys: latest value is `"resurrected_{:04}"`.
    /// - Deleted (and not resurrected) keys: `None`.
    /// - Untouched keys: original `"val_{:04}"`.
    ///
    /// Scan is strictly sorted, count matches oracle, and every key-value
    /// pair is identical.
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_scan_range_tombstones() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), default_config()).unwrap();

        let num_keys = 1000;

        // Phase 1: populate 1000 keys (will span multiple SSTables with 4 KB buffer)
        for i in 0..num_keys {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("val_{:04}", i).into_bytes(),
                )
                .unwrap();
        }

        // Phase 2: issue 50 overlapping range-delete tombstones in strides of 10
        // Each deletes 10 keys, but some overlap, creating a complex tombstone landscape.
        let mut deleted: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for r in 0..50 {
            let start = r * 7; // stride of 7 creates overlaps with stride-of-10 windows
            let end = (start + 10).min(num_keys);
            engine
                .delete_range(
                    format!("key_{:04}", start).into_bytes(),
                    format!("key_{:04}", end).into_bytes(),
                )
                .unwrap();
            for i in start..end {
                deleted.insert(i);
            }
        }

        // Phase 3: resurrect some keys inside deleted ranges
        let mut resurrected: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for i in (0..num_keys).step_by(13) {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("resurrected_{:04}", i).into_bytes(),
                )
                .unwrap();
            resurrected.insert(i);
        }

        // Build ground truth
        let mut expected_live: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            if resurrected.contains(&i) {
                expected_live.push((key, format!("resurrected_{:04}", i).into_bytes()));
            } else if !deleted.contains(&i) {
                expected_live.push((key, format!("val_{:04}", i).into_bytes()));
            }
        }
        expected_live.sort_by(|a, b| a.0.cmp(&b.0));

        // Verify scan
        let scan_results = collect_scan(&engine, b"key_", b"key_\xff");

        assert_eq!(
            scan_results.len(),
            expected_live.len(),
            "scan count mismatch: got {} expected {}",
            scan_results.len(),
            expected_live.len()
        );

        for pair in scan_results.windows(2) {
            assert!(pair[0].0 < pair[1].0, "scan must be sorted");
        }

        for (actual, exp) in scan_results.iter().zip(expected_live.iter()) {
            assert_eq!(
                actual,
                exp,
                "mismatch at key {:?}",
                String::from_utf8_lossy(&exp.0)
            );
        }

        // Also verify via individual gets
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let actual = engine.get(key).unwrap();
            if resurrected.contains(&i) {
                assert_eq!(
                    actual,
                    Some(format!("resurrected_{:04}", i).into_bytes()),
                    "key_{:04} should be resurrected",
                    i
                );
            } else if deleted.contains(&i) {
                assert_eq!(actual, None, "key_{:04} should be range-deleted", i);
            } else {
                assert_eq!(
                    actual,
                    Some(format!("val_{:04}", i).into_bytes()),
                    "key_{:04} should have original value",
                    i
                );
            }
        }
    }

    // ================================================================
    // 7. Minor compaction correctness under multi-round heavy writes
    // ================================================================

    /// # Scenario
    /// Three rounds of heavy PRNG-driven writes, each followed by
    /// `flush_all_frozen()` + `minor_compact()`, with a full oracle
    /// verify after every round.
    ///
    /// # Starting environment
    /// Fresh engine with `compaction_stress_config()` (512 B write buffer,
    /// `min_threshold = 2`) — many small SSTables trigger compaction easily.
    ///
    /// # Actions
    /// For each of 3 rounds:
    /// 1. Execute 3 000 PRNG-driven operations (put 60 %, delete 20 %,
    ///    range-delete 15 %, overwrite 5 %) on 300 keys.
    /// 2. `flush_all_frozen()` — materialise all frozen memtables to SSTables.
    /// 3. Record pre-compaction SSTable count.
    /// 4. Loop `minor_compact()` until it returns `false` (no more work).
    /// 5. Verify every oracle key via `get()`.
    /// 6. Full-range scan — compare to oracle.
    ///
    /// # Expected behavior
    /// Every key matches the oracle after each round. Minor compaction
    /// reduces or keeps constant the SSTable count. Scan results are sorted
    /// and identical to the expected-live set.
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_minor_compaction_multi_round() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), compaction_stress_config()).unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let mut rng = Rng::new(0xC04C);

        let num_keys = 300;
        let ops_per_round = 3000;

        for round in 0..3_u32 {
            // Heavy writes
            for _ in 0..ops_per_round {
                let op = rng.next_usize(100);
                let idx = rng.next_usize(num_keys);
                let key = format!("key_{:04}", idx).into_bytes();

                if op < 60 {
                    let value = format!("v{}_r{}_{}", idx, round, rng.next_u64()).into_bytes();
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, Some(value));
                } else if op < 80 {
                    engine.delete(key.clone()).unwrap();
                    expected.insert(key, None);
                } else if op < 95 {
                    let end_idx = (idx + rng.next_usize(15) + 1).min(num_keys);
                    let start_key = format!("key_{:04}", idx).into_bytes();
                    let end_key = format!("key_{:04}", end_idx).into_bytes();
                    engine
                        .delete_range(start_key.clone(), end_key.clone())
                        .unwrap();
                    apply_range_delete(&mut expected, &start_key, &end_key);
                } else {
                    let value = format!("ow{}_r{}_{}", idx, round, rng.next_u64()).into_bytes();
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, Some(value));
                }
            }

            // Flush all frozen memtables to SSTables
            engine.flush_all_frozen().unwrap();

            let pre = engine.stats().unwrap().sstables_count;

            // Run minor compaction until no more work
            while engine.minor_compact().unwrap() {}

            let post = engine.stats().unwrap().sstables_count;
            // With min_threshold = 2 and many small SSTables, minor
            // compaction should merge aggressively — at least halve the count.
            assert!(
                post <= pre / 2,
                "round {}: minor compaction should at least halve SSTable count ({} → {}, expected ≤ {})",
                round,
                pre,
                post,
                pre / 2
            );

            // Full verify via get
            verify_all(&engine, &expected);

            // Scan verify
            let mut expected_live: Vec<(Vec<u8>, Vec<u8>)> = expected
                .iter()
                .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
                .collect();
            expected_live.sort_by(|a, b| a.0.cmp(&b.0));

            let scan_results = collect_scan(&engine, b"key_", b"key_\xff");
            assert_eq!(
                scan_results.len(),
                expected_live.len(),
                "round {}: scan count mismatch",
                round
            );
            for (actual, exp) in scan_results.iter().zip(expected_live.iter()) {
                assert_eq!(
                    actual,
                    exp,
                    "round {}: scan mismatch at {:?}",
                    round,
                    String::from_utf8_lossy(&exp.0)
                );
            }
        }
    }

    // ================================================================
    // 8. Tombstone compaction under heavy deletes
    // ================================================================

    /// # Scenario
    /// Write 500 keys, flush, then issue point-deletes and range-deletes
    /// for 500 *non-existent* keys (disjoint key-space). The resulting
    /// delete-only SSTables contain tombstones that shadow nothing in any
    /// older SSTable, so tombstone compaction can drop them entirely.
    ///
    /// # Starting environment
    /// Fresh engine with `compaction_stress_config()` — bloom fallback and
    /// range-drop enabled, `tombstone_ratio_threshold = 0.15`,
    /// `tombstone_compaction_interval = 0` (no age gate).
    ///
    /// # Actions
    /// 1. Insert keys `dat_0000`–`dat_0499` (data set A) and flush.
    /// 2. Issue 500 point-deletes for `ghost_0000`–`ghost_0499` — keys that
    ///    were never written and don't appear in any SSTable.
    /// 3. Issue 5 range-deletes over `ghost_*` sub-ranges.
    /// 4. Flush again — producing SSTables purely filled with tombstones
    ///    for non-existent keys.
    /// 5. Record pre-compaction total size.
    /// 6. Loop `tombstone_compact()` until `false`.
    /// 7. Record post-compaction total size.
    /// 8. Verify every key via `get()` and scan.
    ///
    /// # Expected behavior
    /// Tombstone compaction drops all ghost-key tombstones (bloom filters
    /// confirm they don't exist in older SSTables). Expect ≥ 20 % total
    /// size reduction. Data-set-A keys retain their values.
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_tombstone_compaction_heavy_deletes() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), compaction_stress_config()).unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();

        // Phase 1: write data-set A (keys dat_0000–dat_0499).
        for i in 0..500_usize {
            let key = format!("dat_{:04}", i).into_bytes();
            let value = format!("value_{:04}_pad_for_size_{:06}", i, i * 17).into_bytes();
            engine.put(key.clone(), value.clone()).unwrap();
            expected.insert(key, Some(value));
        }
        engine.flush_all_frozen().unwrap();

        // Phase 2: issue tombstones for completely disjoint "ghost" keys
        // that were NEVER written — bloom filters will say "definitely not
        // in older SSTables", letting tombstone compaction drop them.
        for i in 0..500_usize {
            let key = format!("ghost_{:04}", i).into_bytes();
            engine.delete(key.clone()).unwrap();
            expected.insert(key, None);
        }
        // Range-deletes over ghost sub-ranges.
        for w in 0..5 {
            let start = w * 100;
            let end = start + 100;
            let sk = format!("ghost_{:04}", start).into_bytes();
            let ek = format!("ghost_{:04}", end).into_bytes();
            engine.delete_range(sk.clone(), ek.clone()).unwrap();
            apply_range_delete(&mut expected, &sk, &ek);
        }
        engine.flush_all_frozen().unwrap();

        let pre_size = engine.stats().unwrap().total_sst_size_bytes;

        // Run tombstone compaction
        while engine.tombstone_compact().unwrap() {}

        let post_size = engine.stats().unwrap().total_sst_size_bytes;

        // Tombstone compaction should drop all ghost-key tombstones,
        // giving at least 20 % total size reduction.
        let max_allowed = (pre_size as f64 * 0.80) as u64;
        assert!(
            post_size <= max_allowed,
            "tombstone compaction should reduce size by ≥ 20 % ({} → {}, max allowed {})",
            pre_size,
            post_size,
            max_allowed
        );

        // Verify correctness
        verify_all(&engine, &expected);

        // Scan verify — only data-set A should appear.
        let mut expected_live: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
            .collect();
        expected_live.sort_by(|a, b| a.0.cmp(&b.0));

        let scan_results = collect_scan(&engine, b"dat_", b"ghost_\xff");
        assert_eq!(
            scan_results.len(),
            expected_live.len(),
            "scan count mismatch after tombstone compaction"
        );
        for (actual, exp) in scan_results.iter().zip(expected_live.iter()) {
            assert_eq!(
                actual,
                exp,
                "scan mismatch after tombstone compaction at {:?}",
                String::from_utf8_lossy(&exp.0)
            );
        }
    }

    // ================================================================
    // 9. Full lifecycle: writes → minor → major → verify
    // ================================================================

    /// # Scenario
    /// Complete compaction lifecycle: heavy writes → flush → iterative
    /// minor compaction → single major compaction → full verify.
    ///
    /// # Starting environment
    /// Fresh engine with `compaction_stress_config()`.
    ///
    /// # Actions
    /// 1. Execute 6 000 PRNG-driven operations (seed `0xF1FA`) on 400 keys.
    /// 2. `flush_all_frozen()`.
    /// 3. Loop `minor_compact()` until `false`.
    /// 4. Record post-minor SSTable count.
    /// 5. `major_compact()` — merges everything into a single run.
    /// 6. Assert SSTable count ≤ post-minor count.
    /// 7. Full `get()` verify against oracle.
    /// 8. Full-range scan verify.
    ///
    /// # Expected behavior
    /// Major compaction merges all remaining SSTables. All live keys survive
    /// intact. Deleted keys return `None`. Scan is sorted and complete.
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_full_lifecycle_minor_then_major() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), compaction_stress_config()).unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let mut rng = Rng::new(0xF1FA);

        let num_keys = 400;
        let num_ops = 6000;

        for _ in 0..num_ops {
            let op = rng.next_usize(100);
            let idx = rng.next_usize(num_keys);
            let key = format!("key_{:04}", idx).into_bytes();

            if op < 55 {
                let value = format!("v{}_{}", idx, rng.next_u64()).into_bytes();
                engine.put(key.clone(), value.clone()).unwrap();
                expected.insert(key, Some(value));
            } else if op < 75 {
                engine.delete(key.clone()).unwrap();
                expected.insert(key, None);
            } else if op < 90 {
                let end_idx = (idx + rng.next_usize(20) + 1).min(num_keys);
                let start_key = format!("key_{:04}", idx).into_bytes();
                let end_key = format!("key_{:04}", end_idx).into_bytes();
                engine
                    .delete_range(start_key.clone(), end_key.clone())
                    .unwrap();
                apply_range_delete(&mut expected, &start_key, &end_key);
            } else {
                let value = format!("ow{}_{}", idx, rng.next_u64()).into_bytes();
                engine.put(key.clone(), value.clone()).unwrap();
                expected.insert(key, Some(value));
            }
        }

        engine.flush_all_frozen().unwrap();

        // Minor compaction
        while engine.minor_compact().unwrap() {}
        let post_minor = engine.stats().unwrap().sstables_count;

        // Major compaction — merges everything into exactly 1 SSTable.
        engine.major_compact().unwrap();
        let post_major = engine.stats().unwrap();

        assert_eq!(
            post_major.sstables_count, 1,
            "after minor + major, should have exactly 1 SSTable (was {} post-minor)",
            post_minor
        );

        // Verify via get
        verify_all(&engine, &expected);

        // Verify via scan
        let mut expected_live: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
            .collect();
        expected_live.sort_by(|a, b| a.0.cmp(&b.0));

        let scan_results = collect_scan(&engine, b"key_", b"key_\xff");
        assert_eq!(scan_results.len(), expected_live.len());
        for (actual, exp) in scan_results.iter().zip(expected_live.iter()) {
            assert_eq!(actual, exp);
        }
    }

    // ================================================================
    // 10. Interleaved writes and compaction (10 rounds)
    // ================================================================

    /// # Scenario
    /// Ten rounds of "write batch → flush → minor compact → verify",
    /// testing that compaction remains correct when new data continuously
    /// arrives between compaction runs.
    ///
    /// # Starting environment
    /// Fresh engine with `compaction_stress_config()`.
    ///
    /// # Actions
    /// For each of 10 rounds:
    /// 1. Execute 1 000 PRNG-driven operations (seed `0xABCD`) on 200 keys.
    /// 2. `flush_all_frozen()`.
    /// 3. Loop `minor_compact()` until `false`.
    /// 4. Verify every oracle key via `get()`.
    ///
    /// At the end: one `major_compact()`, then final scan verify.
    ///
    /// # Expected behavior
    /// Each intermediate verify passes. After the final major compaction
    /// the engine is fully merged and scan matches the oracle.
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_interleaved_writes_and_compaction() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), compaction_stress_config()).unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let mut rng = Rng::new(0xABCD);

        let num_keys = 200;
        let ops_per_round = 1000;
        let num_rounds = 10;

        for round in 0..num_rounds {
            for _ in 0..ops_per_round {
                let op = rng.next_usize(100);
                let idx = rng.next_usize(num_keys);
                let key = format!("key_{:04}", idx).into_bytes();

                if op < 55 {
                    let value = format!("v{}_r{}_{}", idx, round, rng.next_u64()).into_bytes();
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, Some(value));
                } else if op < 75 {
                    engine.delete(key.clone()).unwrap();
                    expected.insert(key, None);
                } else if op < 90 {
                    let end_idx = (idx + rng.next_usize(10) + 1).min(num_keys);
                    let start_key = format!("key_{:04}", idx).into_bytes();
                    let end_key = format!("key_{:04}", end_idx).into_bytes();
                    engine
                        .delete_range(start_key.clone(), end_key.clone())
                        .unwrap();
                    apply_range_delete(&mut expected, &start_key, &end_key);
                } else {
                    let value = format!("ow{}_r{}_{}", idx, round, rng.next_u64()).into_bytes();
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, Some(value));
                }
            }

            engine.flush_all_frozen().unwrap();

            // Minor compact
            while engine.minor_compact().unwrap() {}

            // Verify after each round
            verify_all(&engine, &expected);
        }

        // Final major compaction — should merge everything into exactly 1 SSTable.
        engine.major_compact().unwrap();
        let final_stats = engine.stats().unwrap();
        assert_eq!(
            final_stats.sstables_count, 1,
            "final major compaction should produce exactly 1 SSTable, got {}",
            final_stats.sstables_count
        );

        // Final scan verify
        let mut expected_live: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
            .collect();
        expected_live.sort_by(|a, b| a.0.cmp(&b.0));

        let scan_results = collect_scan(&engine, b"key_", b"key_\xff");
        assert_eq!(scan_results.len(), expected_live.len());
        for pair in scan_results.windows(2) {
            assert!(pair[0].0 < pair[1].0, "scan must be sorted");
        }
        for (actual, exp) in scan_results.iter().zip(expected_live.iter()) {
            assert_eq!(actual, exp);
        }
    }

    // ================================================================
    // 11. Compaction + recovery (close → reopen → verify)
    // ================================================================

    /// # Scenario
    /// Full compaction pipeline followed by close/reopen, verifying that
    /// the compacted state is durable and survives recovery.
    ///
    /// # Starting environment
    /// Fresh engine with `compaction_stress_config()`.
    ///
    /// # Actions
    /// 1. Execute 5 000 PRNG-driven operations (seed `0xDISK`) on 300 keys.
    /// 2. `flush_all_frozen()`.
    /// 3. Loop `minor_compact()` → loop `tombstone_compact()` → `major_compact()`.
    /// 4. `close()` the engine.
    /// 5. `reopen()` from the same directory.
    /// 6. Verify every oracle key via `get()`.
    /// 7. Full-range scan — compare to oracle.
    ///
    /// # Expected behavior
    /// All data survives the compaction + close + reopen cycle. Every key
    /// matches the oracle and the scan is sorted and complete.
    #[test]
    #[ignore] // Slow. Run with: cargo test -- --ignored
    fn memtable_sstable__stress_compaction_then_recovery() {
        let dir = TempDir::new().unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let mut rng = Rng::new(0xD15C);

        {
            let engine = Engine::open(dir.path(), compaction_stress_config()).unwrap();

            let num_keys = 300;
            let num_ops = 5000;

            for _ in 0..num_ops {
                let op = rng.next_usize(100);
                let idx = rng.next_usize(num_keys);
                let key = format!("key_{:04}", idx).into_bytes();

                if op < 55 {
                    let value = format!("v{}_{}", idx, rng.next_u64()).into_bytes();
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, Some(value));
                } else if op < 75 {
                    engine.delete(key.clone()).unwrap();
                    expected.insert(key, None);
                } else if op < 90 {
                    let end_idx = (idx + rng.next_usize(20) + 1).min(num_keys);
                    let start_key = format!("key_{:04}", idx).into_bytes();
                    let end_key = format!("key_{:04}", end_idx).into_bytes();
                    engine
                        .delete_range(start_key.clone(), end_key.clone())
                        .unwrap();
                    apply_range_delete(&mut expected, &start_key, &end_key);
                } else {
                    let value = format!("ow{}_{}", idx, rng.next_u64()).into_bytes();
                    engine.put(key.clone(), value.clone()).unwrap();
                    expected.insert(key, Some(value));
                }
            }

            engine.flush_all_frozen().unwrap();

            // Full compaction pipeline
            while engine.minor_compact().unwrap() {}
            while engine.tombstone_compact().unwrap() {}
            engine.major_compact().unwrap();

            // After full pipeline, everything should be merged into 1 SSTable.
            let final_stats = engine.stats().unwrap();
            assert_eq!(
                final_stats.sstables_count, 1,
                "full compaction pipeline should produce exactly 1 SSTable, got {}",
                final_stats.sstables_count
            );

            engine.close().unwrap();
        }

        // Reopen and verify
        let engine = reopen(dir.path());
        verify_all(&engine, &expected);

        // Scan verify
        let mut expected_live: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
            .collect();
        expected_live.sort_by(|a, b| a.0.cmp(&b.0));

        let scan_results = collect_scan(&engine, b"key_", b"key_\xff");
        assert_eq!(
            scan_results.len(),
            expected_live.len(),
            "scan count mismatch after compaction + recovery"
        );
        for (actual, exp) in scan_results.iter().zip(expected_live.iter()) {
            assert_eq!(actual, exp);
        }
    }
}
