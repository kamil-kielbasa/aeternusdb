//! Stress tests: large-scale mixed-operation workloads that exercise the engine
//! under realistic churn — multiple SSTables with realistic sizes, heavy overwrites,
//! tombstone accumulation, and recovery after thousands of operations.
//!
//! All stress tests use `default_config()` (4 KB write buffer) to produce
//! realistic multi-KB SSTables, matching what compaction would actually operate on.
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
    use crate::engine::tests::helpers::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

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
}
