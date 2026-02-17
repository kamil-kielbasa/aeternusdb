//! Tests for the explicit flush API (`flush_oldest_frozen`, `flush_all_frozen`).
//!
//! These tests verify the new public flush methods that replace the previous
//! inline-flush behaviour. Writes no longer auto-flush frozen memtables;
//! callers (or a background worker) must invoke these methods explicitly.

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    // ================================================================
    // flush_oldest_frozen: basic contract
    // ================================================================

    /// # Scenario
    /// `flush_oldest_frozen()` returns `false` when there are no frozen
    /// memtables.
    ///
    /// # Expected behavior
    /// Returns `Ok(false)` — nothing to flush.
    #[test]
    fn flush_oldest_frozen_noop_when_empty() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        // No data written, no frozen memtables
        let flushed = engine.flush_oldest_frozen().unwrap();
        assert!(!flushed, "Should return false when no frozen memtables");
        assert_eq!(engine.stats().unwrap().sstables_count, 0);
    }

    /// # Scenario
    /// `flush_oldest_frozen()` flushes exactly one frozen memtable per call.
    ///
    /// # Actions
    /// 1. Write keys with a small buffer until frozen_count >= 2.
    /// 2. Call `flush_oldest_frozen()` once.
    /// 3. Check that frozen_count decreased by 1 and sstables_count increased.
    ///
    /// # Expected behavior
    /// One frozen memtable is converted to an SSTable; the rest remain frozen.
    #[test]
    fn flush_oldest_frozen_flushes_one() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        // Write enough to create multiple frozen memtables
        for i in 0..200u32 {
            engine
                .put(
                    format!("k_{:04}", i).into_bytes(),
                    format!("v_{:04}", i).into_bytes(),
                )
                .unwrap();
        }

        let before = engine.stats().unwrap();
        assert!(
            before.frozen_count >= 2,
            "Need at least 2 frozen, got {}",
            before.frozen_count
        );

        let flushed = engine.flush_oldest_frozen().unwrap();
        assert!(flushed, "Should return true when a frozen was flushed");

        let after = engine.stats().unwrap();
        assert_eq!(
            after.frozen_count,
            before.frozen_count - 1,
            "frozen_count should decrease by 1"
        );
        assert_eq!(
            after.sstables_count,
            before.sstables_count + 1,
            "sstables_count should increase by 1"
        );
    }

    // ================================================================
    // flush_all_frozen: basic contract
    // ================================================================

    /// # Scenario
    /// `flush_all_frozen()` returns 0 when there are no frozen memtables.
    ///
    /// # Expected behavior
    /// Returns `Ok(0)` — nothing to flush.
    #[test]
    fn flush_all_frozen_noop_when_empty() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        let count = engine.flush_all_frozen().unwrap();
        assert_eq!(count, 0);
        assert_eq!(engine.stats().unwrap().sstables_count, 0);
    }

    /// # Scenario
    /// `flush_all_frozen()` drains all frozen memtables to SSTables.
    ///
    /// # Actions
    /// 1. Write many keys with a small buffer → accumulate frozen memtables.
    /// 2. Call `flush_all_frozen()`.
    /// 3. Check frozen_count = 0 and sstables_count equals the returned count.
    ///
    /// # Expected behavior
    /// All frozen memtables are flushed; the returned count matches the
    /// number that were pending.
    #[test]
    fn flush_all_frozen_drains_all() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        for i in 0..200u32 {
            engine
                .put(
                    format!("k_{:04}", i).into_bytes(),
                    format!("v_{:04}", i).into_bytes(),
                )
                .unwrap();
        }

        let before = engine.stats().unwrap();
        assert!(before.frozen_count > 0, "Should have frozen memtables");

        let flushed_count = engine.flush_all_frozen().unwrap();
        assert_eq!(
            flushed_count, before.frozen_count,
            "Should flush exactly the number of frozen memtables"
        );

        let after = engine.stats().unwrap();
        assert_eq!(after.frozen_count, 0, "All frozen should be drained");
        assert_eq!(
            after.sstables_count, flushed_count,
            "Each frozen should become one SSTable"
        );
    }

    // ================================================================
    // Data integrity: writes without flush are still readable
    // ================================================================

    /// # Scenario
    /// Writes that accumulate in frozen memtables (without any flush) must
    /// still be readable via `get()` and `scan()`.
    ///
    /// # Actions
    /// 1. Write 100 keys with a small buffer → many frozen memtables.
    /// 2. Do NOT call any flush method.
    /// 3. Get every key.
    ///
    /// # Expected behavior
    /// All 100 keys are readable from frozen memtables. No SSTables exist.
    #[test]
    fn reads_work_without_flush() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        for i in 0..100u32 {
            engine
                .put(
                    format!("k_{:04}", i).into_bytes(),
                    format!("v_{:04}", i).into_bytes(),
                )
                .unwrap();
        }

        let stats = engine.stats().unwrap();
        assert!(stats.frozen_count > 0, "Should have frozen memtables");
        assert_eq!(stats.sstables_count, 0, "No flush called, no SSTables");

        // All keys must be readable from frozen memtables
        for i in 0..100u32 {
            let key = format!("k_{:04}", i).into_bytes();
            let expected = format!("v_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "k_{:04} should be readable from frozen memtables",
                i
            );
        }
    }

    // ================================================================
    // put/delete/delete_range return value
    // ================================================================

    /// # Scenario
    /// Verify that put/delete/delete_range return `true` when a freeze
    /// occurs, and `false` otherwise.
    ///
    /// # Actions
    /// 1. Write a key with memtable-only config (large buffer) — no freeze.
    /// 2. Write keys with small buffer until a freeze is signalled.
    ///
    /// # Expected behavior
    /// The first put returns `false`; eventually a put returns `true`.
    #[test]
    fn write_methods_return_freeze_signal() {
        // Large buffer — no freeze expected
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        let froze = engine.put(b"hello".to_vec(), b"world".to_vec()).unwrap();
        assert!(!froze, "Large buffer should not trigger freeze");

        let froze = engine.delete(b"hello".to_vec()).unwrap();
        assert!(!froze, "Large buffer delete should not trigger freeze");

        let froze = engine.delete_range(b"a".to_vec(), b"z".to_vec()).unwrap();
        assert!(
            !froze,
            "Large buffer range-delete should not trigger freeze"
        );

        // Small buffer — freeze expected eventually
        let dir2 = TempDir::new().unwrap();
        let engine2 = Engine::open(dir2.path(), small_buffer_config()).unwrap();

        let mut saw_freeze = false;
        for i in 0..100u32 {
            let froze = engine2
                .put(
                    format!("k_{:04}", i).into_bytes(),
                    format!("v_{:04}", i).into_bytes(),
                )
                .unwrap();
            if froze {
                saw_freeze = true;
                break;
            }
        }
        assert!(
            saw_freeze,
            "Small buffer should trigger at least one freeze"
        );
    }

    // ================================================================
    // Flush after mixed operations
    // ================================================================

    /// # Scenario
    /// Flush correctly persists puts, deletes, and range-deletes that
    /// accumulated in frozen memtables.
    ///
    /// # Actions
    /// 1. Put 5 keys with small buffer.
    /// 2. Delete one key.
    /// 3. Range-delete two keys.
    /// 4. Flush all frozen.
    /// 5. Verify data via get.
    ///
    /// # Expected behavior
    /// After flush, reads still return correct results — the SSTable
    /// correctly encodes puts, tombstones, and range tombstones.
    #[test]
    fn flush_preserves_mixed_operations() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        for i in 0..20u32 {
            engine
                .put(
                    format!("mk_{:04}", i).into_bytes(),
                    format!("mv_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        engine.delete(b"mk_0005".to_vec()).unwrap();
        engine
            .delete_range(b"mk_0010".to_vec(), b"mk_0015".to_vec())
            .unwrap();

        engine.flush_all_frozen().unwrap();

        // Point-deleted key
        assert_eq!(engine.get(b"mk_0005".to_vec()).unwrap(), None);

        // Range-deleted keys
        for i in 10..15u32 {
            assert_eq!(
                engine.get(format!("mk_{:04}", i).into_bytes()).unwrap(),
                None,
                "mk_{:04} should be range-deleted",
                i
            );
        }

        // Surviving keys
        for i in [0u32, 1, 2, 3, 4, 6, 7, 8, 9, 15, 16, 17, 18, 19] {
            assert_eq!(
                engine.get(format!("mk_{:04}", i).into_bytes()).unwrap(),
                Some(format!("mv_{:04}", i).into_bytes()),
                "mk_{:04} should survive flush",
                i
            );
        }
    }

    // ================================================================
    // Multiple flush rounds
    // ================================================================

    /// # Scenario
    /// Write → flush → write → flush produces an increasing SSTable count,
    /// and all data across both rounds is readable.
    ///
    /// # Expected behavior
    /// SSTable count grows with each flush round; data integrity maintained.
    #[test]
    fn multiple_flush_rounds() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        // Round 1
        for i in 0..50u32 {
            engine
                .put(
                    format!("r1_{:04}", i).into_bytes(),
                    format!("v1_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        let c1 = engine.flush_all_frozen().unwrap();
        assert!(c1 > 0);
        let s1 = engine.stats().unwrap().sstables_count;

        // Round 2
        for i in 0..50u32 {
            engine
                .put(
                    format!("r2_{:04}", i).into_bytes(),
                    format!("v2_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        let c2 = engine.flush_all_frozen().unwrap();
        assert!(c2 > 0);
        let s2 = engine.stats().unwrap().sstables_count;
        assert!(s2 > s1, "More SSTables after round 2");

        // All data from both rounds must be readable
        for i in 0..50u32 {
            assert_eq!(
                engine.get(format!("r1_{:04}", i).into_bytes()).unwrap(),
                Some(format!("v1_{:04}", i).into_bytes()),
            );
            assert_eq!(
                engine.get(format!("r2_{:04}", i).into_bytes()).unwrap(),
                Some(format!("v2_{:04}", i).into_bytes()),
            );
        }
    }

    // ================================================================
    // Crash recovery with many frozen memtables (no inline flushing)
    // ================================================================

    /// # Scenario
    /// Many frozen memtables accumulate without flushing, then the engine
    /// crashes (drop without close). On reopen, all data must be recovered.
    ///
    /// This tests the crash recovery code path when there are potentially
    /// hundreds of frozen WALs to replay (the new behaviour since inline
    /// flushing was removed from the write path).
    #[test]
    fn crash_recovery_many_frozen_no_flush() {
        let dir = TempDir::new().unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();

        {
            let engine = Engine::open(dir.path(), default_config()).unwrap();

            // Put 500 keys
            for i in 0..500u32 {
                let key = format!("key_{:04}", i).into_bytes();
                let value = format!("val_{:04}", i).into_bytes();
                engine.put(key.clone(), value.clone()).unwrap();
                expected.insert(key, Some(value));
            }

            // Point-delete some keys
            for i in (0..100).step_by(3) {
                let key = format!("key_{:04}", i).into_bytes();
                engine.delete(key.clone()).unwrap();
                expected.insert(key, None);
            }

            // Range-delete
            let start = b"key_0200".to_vec();
            let end = b"key_0250".to_vec();
            engine.delete_range(start.clone(), end.clone()).unwrap();
            for (k, v) in expected.iter_mut() {
                if k.as_slice() >= start.as_slice() && k.as_slice() < end.as_slice() {
                    *v = None;
                }
            }

            let stats = engine.stats().unwrap();
            assert!(
                stats.frozen_count > 0,
                "Should have accumulated frozen memtables"
            );
            assert_eq!(
                stats.sstables_count, 0,
                "No flush called, should have 0 SSTables"
            );

            // Drop without close — simulates crash
        }

        let engine = reopen(dir.path());

        // Verify every key
        for (key, expected_val) in &expected {
            let actual = engine.get(key.clone()).unwrap();
            assert_eq!(
                &actual,
                expected_val,
                "Mismatch for key {:?}",
                String::from_utf8_lossy(key)
            );
        }
    }

    /// Range delete hides puts in older frozen memtables (live engine, no flush).
    #[test]
    fn range_delete_across_frozen_memtables() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        // Put target key — with 128-byte buffer it'll be frozen quickly
        engine
            .put(b"key_0488".to_vec(), b"original".to_vec())
            .unwrap();

        // Write padding to push key_0488 into frozen memtables
        for i in 0..200u32 {
            engine
                .put(
                    format!("pad_{:04}", i).into_bytes(),
                    format!("v_{}", i).into_bytes(),
                )
                .unwrap();
        }

        let stats = engine.stats().unwrap();
        assert!(
            stats.frozen_count > 0,
            "key_0488 should be in a frozen memtable"
        );

        // key_0488 should be in a frozen memtable now
        assert_eq!(
            engine.get(b"key_0488".to_vec()).unwrap(),
            Some(b"original".to_vec()),
            "key_0488 must be readable from frozen memtable"
        );

        // Issue range delete covering key_0488
        engine
            .delete_range(b"key_0473".to_vec(), b"key_0490".to_vec())
            .unwrap();

        // Range delete should hide the put
        let result = engine.get(b"key_0488".to_vec()).unwrap();
        assert_eq!(result, None, "key_0488 should be hidden by range delete");
    }

    /// Reproduce crash recovery stress test with random ops and no inline flush.
    #[test]
    fn crash_recovery_stress_random_ops() {
        let dir = TempDir::new().unwrap();
        let mut expected: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();

        // Simple LCG PRNG
        struct Rng(u64);
        impl Rng {
            fn new(seed: u64) -> Self {
                Self(seed)
            }
            fn next_u64(&mut self) -> u64 {
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

        let mut rng = Rng::new(0xBEEF);
        {
            let engine = Engine::open(dir.path(), default_config()).unwrap();

            let num_keys = 500;
            let num_ops = 8000;

            for _op_num in 0..num_ops {
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

            // Verify all keys BEFORE crash (live engine, no flush)
            for (key, expected_val) in &expected {
                let actual = engine.get(key.clone()).unwrap();
                assert_eq!(
                    &actual,
                    expected_val,
                    "Pre-crash mismatch for key {:?}",
                    String::from_utf8_lossy(key)
                );
            }

            // DROP without close — simulates crash
        }

        // Reopen and verify all keys survived crash recovery
        let engine = reopen(dir.path());

        for (key, expected_val) in &expected {
            let actual = engine.get(key.clone()).unwrap();
            assert_eq!(
                &actual,
                expected_val,
                "Post-crash mismatch for key {:?}",
                String::from_utf8_lossy(key)
            );
        }
    }
}
