//! Multiple consecutive crash tests.
//!
//! These tests verify that the engine survives repeated crash cycles
//! (drop without close → reopen → write → drop → reopen). Each crash
//! must be recovered independently, and data written between crashes
//! must accumulate correctly.
//!
//! This catches regressions where recovery leaves internal state
//! partially initialized, causing the next crash cycle to lose data.
//!
//! ## See also
//! - [`tests_crash_recovery`] — single crash cycle
//! - [`tests_crash_flush`] — crash during flush
//! - [`tests_crash_compaction`] — crash during compaction

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ================================================================
    // 1. Two consecutive crashes — all data survives
    // ================================================================

    /// # Scenario
    /// Two crash cycles with writes in between.
    ///
    /// # Actions
    /// 1. Open, write keys 0..10, drop (crash 1).
    /// 2. Open, verify keys 0..10, write keys 10..20, drop (crash 2).
    /// 3. Open, verify all 20 keys.
    ///
    /// # Expected behavior
    /// All 20 keys recovered after the second crash.
    #[test]
    fn memtable__two_consecutive_crashes() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // Crash 1: write 10 keys.
        {
            let engine = Engine::open(path, memtable_only_config()).unwrap();
            for i in 0..10 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            // Drop without close.
        }

        // Crash 2: verify first 10, write 10 more, drop.
        {
            let engine = Engine::open(path, memtable_only_config()).unwrap();
            for i in 0..10 {
                let key = format!("key_{i:04}").into_bytes();
                assert!(
                    engine.get(key).unwrap().is_some(),
                    "key_{i:04} lost after crash 1"
                );
            }
            for i in 10..20 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            // Drop without close.
        }

        // Final open: all 20 keys must be present.
        let engine = Engine::open(path, memtable_only_config()).unwrap();
        for i in 0..20 {
            let key = format!("key_{i:04}").into_bytes();
            let result = engine.get(key).unwrap();
            assert!(result.is_some(), "key_{i:04} lost after two crashes");
            assert_eq!(
                result.unwrap(),
                format!("val_{i:04}").into_bytes(),
                "Wrong value for key_{i:04}"
            );
        }
    }

    // ================================================================
    // 2. Three crashes with mixed operations
    // ================================================================

    /// # Scenario
    /// Three crash cycles with puts, deletes, and overwrites.
    ///
    /// # Actions
    /// 1. Write keys 0..20, drop (crash 1).
    /// 2. Delete keys 5..10, overwrite keys 0..5, drop (crash 2).
    /// 3. Range-delete keys 15..20, write keys 20..25, drop (crash 3).
    /// 4. Verify final state.
    ///
    /// # Expected behavior
    /// Keys 0-4: overwritten values. Keys 5-9: deleted.
    /// Keys 10-14: original values. Keys 15-19: range-deleted.
    /// Keys 20-24: new values.
    #[test]
    fn memtable__three_crashes_mixed_operations() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // Crash 1: write 20 keys.
        {
            let engine = Engine::open(path, memtable_only_config()).unwrap();
            for i in 0..20 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
        }

        // Crash 2: delete + overwrite.
        {
            let engine = Engine::open(path, memtable_only_config()).unwrap();
            for i in 5..10 {
                engine.delete(format!("key_{i:04}").into_bytes()).unwrap();
            }
            for i in 0..5 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("updated_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
        }

        // Crash 3: range-delete + new keys.
        {
            let engine = Engine::open(path, memtable_only_config()).unwrap();
            engine
                .delete_range(b"key_0015".to_vec(), b"key_0020".to_vec())
                .unwrap();
            for i in 20..25 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
        }

        // Final verification.
        let engine = Engine::open(path, memtable_only_config()).unwrap();

        // Keys 0-4: overwritten.
        for i in 0..5 {
            let val = engine
                .get(format!("key_{i:04}").into_bytes())
                .unwrap()
                .expect("key_{i} should exist");
            assert_eq!(val, format!("updated_{i:04}").into_bytes());
        }

        // Keys 5-9: deleted.
        for i in 5..10 {
            assert!(
                engine
                    .get(format!("key_{i:04}").into_bytes())
                    .unwrap()
                    .is_none(),
                "key_{i:04} should be deleted"
            );
        }

        // Keys 10-14: original.
        for i in 10..15 {
            let val = engine
                .get(format!("key_{i:04}").into_bytes())
                .unwrap()
                .expect("key_{i} should exist");
            assert_eq!(val, format!("val_{i:04}").into_bytes());
        }

        // Keys 15-19: range-deleted.
        for i in 15..20 {
            assert!(
                engine
                    .get(format!("key_{i:04}").into_bytes())
                    .unwrap()
                    .is_none(),
                "key_{i:04} should be range-deleted"
            );
        }

        // Keys 20-24: new.
        for i in 20..25 {
            let val = engine
                .get(format!("key_{i:04}").into_bytes())
                .unwrap()
                .expect("key_{i} should exist");
            assert_eq!(val, format!("val_{i:04}").into_bytes());
        }
    }

    // ================================================================
    // 3. Crash cycles with SSTables + frozen memtables
    // ================================================================

    /// # Scenario
    /// Multiple crash cycles where each cycle produces SSTables
    /// (via small buffer + flush) and then crashes with unflushed data.
    ///
    /// # Actions
    /// 1. Write 30 keys with small buffer (creates SSTables + frozen).
    ///    Flush some frozen, leave some. Drop (crash 1).
    /// 2. Reopen, write 30 more keys. Flush some. Drop (crash 2).
    /// 3. Reopen, verify all 60 keys. Scan must return them sorted
    ///    without duplicates.
    ///
    /// # Expected behavior
    /// All 60 keys present. Scan returns sorted, deduplicated results.
    #[test]
    fn memtable_sstable__two_crashes_with_sstables() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // Crash 1: write 30 keys with small buffer.
        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();
            for i in 0..30 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();
            // Write a few more to create new frozen memtable.
            for i in 30..35 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            // Drop with unflushed data.
        }

        // Crash 2: verify, write more, drop.
        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();
            // Verify first batch.
            for i in 0..35 {
                assert!(
                    engine
                        .get(format!("key_{i:04}").into_bytes())
                        .unwrap()
                        .is_some(),
                    "key_{i:04} lost after crash 1"
                );
            }
            // Write more.
            for i in 35..60 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();
            // Drop.
        }

        // Final: all 60.
        let engine = Engine::open(path, small_buffer_config()).unwrap();
        for i in 0..60 {
            let key = format!("key_{i:04}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{i:04} lost after two crashes with SSTables"
            );
        }

        // Scan must be sorted and deduplicated.
        let scan: Vec<_> = engine.scan(b"key_0000", b"key_9999").unwrap().collect();
        assert_eq!(scan.len(), 60, "Scan should return exactly 60 keys");
        for w in scan.windows(2) {
            assert!(w[0].0 < w[1].0, "Scan must be sorted");
        }
    }

    // ================================================================
    // 4. Five crashes — long sequence
    // ================================================================

    /// # Scenario
    /// Five consecutive crash cycles, each writing new keys.
    ///
    /// # Expected behavior
    /// All keys from all five cycles present on final recovery.
    #[test]
    fn memtable__five_consecutive_crashes() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        for cycle in 0..5u32 {
            let engine = Engine::open(path, memtable_only_config()).unwrap();
            for i in 0..5 {
                let key = format!("c{cycle}_k{i}").into_bytes();
                let val = format!("c{cycle}_v{i}").into_bytes();
                engine.put(key, val).unwrap();
            }
            // Drop — crash.
        }

        // Final verification.
        let engine = Engine::open(path, memtable_only_config()).unwrap();
        for cycle in 0..5u32 {
            for i in 0..5 {
                let key = format!("c{cycle}_k{i}").into_bytes();
                let result = engine.get(key).unwrap();
                assert!(result.is_some(), "c{cycle}_k{i} lost after 5 crashes");
                assert_eq!(result.unwrap(), format!("c{cycle}_v{i}").into_bytes());
            }
        }
    }
}
