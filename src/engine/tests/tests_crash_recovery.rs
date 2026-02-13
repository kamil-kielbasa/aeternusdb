//! Crash-recovery tests: verify data survives drop without close().
//!
//! These tests exercise the frozen-WAL-replay path in `Engine::open()` that
//! is **never** reached by the normal close-then-reopen tests (because
//! `close()` flushes all frozen memtables to SSTables first).
//!
//! ## Layer coverage
//! - `memtable__*`: active WAL only (no freeze triggered)
//! - `memtable_sstable__*`: frozen memtables + SSTables survive crash
//!
//! ## See also
//! - [`tests_recovery`] — clean close → reopen path
//! - [`tests_stress`] `*crash*` — crash recovery under heavy load

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Local helpers
    // ----------------------------------------------------------------

    /// Write puts in a loop until the engine has at least one frozen memtable.
    /// Returns how many keys were written (0..count).
    fn fill_until_frozen(engine: &Engine, prefix: &str) -> usize {
        let mut i = 0;
        loop {
            let key = format!("{}_{:04}", prefix, i).into_bytes();
            let value = format!("val_{:04}", i).into_bytes();
            engine.put(key, value).expect("put");
            let stats = engine.stats().expect("stats");
            if stats.frozen_count > 0 {
                return i + 1;
            }
            i += 1;
            assert!(i < 10_000, "Expected FlushRequired within 10 000 puts");
        }
    }

    /// Write puts in a loop until the engine has at least one SSTable AND
    /// at least one frozen memtable (i.e. all three layers are populated).
    fn fill_until_sstable_and_frozen(engine: &Engine, prefix: &str) -> usize {
        let mut i = 0;
        loop {
            let key = format!("{}_{:04}", prefix, i).into_bytes();
            let value = format!("val_{:04}", i).into_bytes();
            engine.put(key, value).expect("put");
            let stats = engine.stats().expect("stats");
            if stats.sstables_count > 0 && stats.frozen_count >= 1 {
                return i + 1;
            }
            i += 1;
            assert!(i < 10_000, "Expected SSTable + frozen within 10 000 puts");
        }
    }

    /// Keep writing tiny filler puts until frozen_count drops to 0
    /// (each put triggers flush_frozen_to_sstable at its start).
    fn drain_frozen(engine: &Engine) {
        let mut idx = 0u64;
        loop {
            let stats = engine.stats().expect("stats");
            if stats.frozen_count == 0 {
                break;
            }
            engine
                .put(format!("_drain_{:06}", idx).into_bytes(), b"x".to_vec())
                .expect("drain put");
            idx += 1;
            assert!(idx < 1_000, "Failed to drain frozen memtables");
        }
    }

    // ================================================================
    // 1. Active WAL only — no freeze triggered
    // ================================================================

    #[test]
    fn memtable__crash_recovery_active_wal_puts() {
        let dir = TempDir::new().unwrap();
        {
            let engine = Engine::open(dir.path(), memtable_only_config()).expect("open");
            engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
            engine.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
            engine.put(b"k3".to_vec(), b"v3".to_vec()).unwrap();

            let stats = engine.stats().unwrap();
            assert_eq!(stats.frozen_count, 0);
            assert_eq!(stats.sstables_count, 0);
            // Drop without close — simulates crash
        }

        let engine = reopen(dir.path());
        assert_eq!(engine.get(b"k1".to_vec()).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get(b"k2".to_vec()).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(engine.get(b"k3".to_vec()).unwrap(), Some(b"v3".to_vec()));
    }

    // ================================================================
    // 2. Frozen memtable survives crash (frozen-WAL replay path)
    // ================================================================

    #[test]
    fn memtable__crash_recovery_with_frozen() {
        let dir = TempDir::new().unwrap();
        let count;
        {
            let engine = Engine::open(dir.path(), small_buffer_config()).expect("open");
            count = fill_until_frozen(&engine, "key");

            let stats = engine.stats().unwrap();
            assert!(
                stats.frozen_count >= 1,
                "Expected at least 1 frozen memtable, got {}",
                stats.frozen_count
            );
            // Drop without close — frozen memtable NOT flushed to SSTable
        }

        let engine = reopen(dir.path());
        for i in 0..count {
            let key = format!("key_{:04}", i).into_bytes();
            let expected = format!("val_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "Missing key_{:04} after crash recovery",
                i
            );
        }
    }

    // ================================================================
    // 3. Frozen memtable + SSTables survive crash
    // ================================================================

    #[test]
    fn memtable_sstable__crash_recovery_frozen_and_sstable() {
        let dir = TempDir::new().unwrap();
        let count;
        {
            let engine = Engine::open(dir.path(), small_buffer_config()).expect("open");
            count = fill_until_sstable_and_frozen(&engine, "key");

            let stats = engine.stats().unwrap();
            assert!(stats.sstables_count > 0, "Expected at least 1 SSTable");
            assert!(
                stats.frozen_count >= 1,
                "Expected at least 1 frozen memtable, got {}",
                stats.frozen_count
            );
            // Drop without close — all three layers populated
        }

        let engine = reopen(dir.path());
        for i in 0..count {
            let key = format!("key_{:04}", i).into_bytes();
            let expected = format!("val_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "Missing key_{:04} after crash with SSTable + frozen",
                i
            );
        }
    }

    // ================================================================
    // 4. Delete tombstones in active WAL survive crash
    // ================================================================

    #[test]
    fn memtable__crash_recovery_delete_in_active_wal() {
        let dir = TempDir::new().unwrap();
        {
            let engine = Engine::open(dir.path(), memtable_only_config()).expect("open");
            engine.put(b"keep".to_vec(), b"yes".to_vec()).unwrap();
            engine.put(b"gone".to_vec(), b"bye".to_vec()).unwrap();
            engine.delete(b"gone".to_vec()).unwrap();

            let stats = engine.stats().unwrap();
            assert_eq!(stats.frozen_count, 0);
            // Drop without close
        }

        let engine = reopen(dir.path());
        assert_eq!(engine.get(b"keep".to_vec()).unwrap(), Some(b"yes".to_vec()));
        assert_eq!(engine.get(b"gone".to_vec()).unwrap(), None);
    }

    // ================================================================
    // 5. Range-delete tombstones in active WAL survive crash
    // ================================================================

    #[test]
    fn memtable__crash_recovery_range_delete_in_wal() {
        let dir = TempDir::new().unwrap();
        {
            let engine = Engine::open(dir.path(), memtable_only_config()).expect("open");
            for i in 0..10u8 {
                engine
                    .put(vec![b'k', i], format!("v{}", i).into_bytes())
                    .unwrap();
            }
            // Range-delete keys [k\x03, k\x07)
            engine.delete_range(vec![b'k', 3], vec![b'k', 7]).unwrap();

            let stats = engine.stats().unwrap();
            assert_eq!(stats.frozen_count, 0);
            // Drop without close
        }

        let engine = reopen(dir.path());
        for i in 0..10u8 {
            let val = engine.get(vec![b'k', i]).unwrap();
            if (3..7).contains(&i) {
                assert_eq!(val, None, "k{} should be range-deleted", i);
            } else {
                assert_eq!(
                    val,
                    Some(format!("v{}", i).into_bytes()),
                    "k{} should survive",
                    i
                );
            }
        }
    }

    // ================================================================
    // 6. Delete tombstones in frozen memtable survive crash
    // ================================================================

    #[test]
    fn memtable_sstable__crash_recovery_frozen_with_deletes() {
        let dir = TempDir::new().unwrap();
        let mut deleted_keys = Vec::new();
        {
            let engine = Engine::open(dir.path(), small_buffer_config()).expect("open");

            // Phase 1: write keys into SSTables
            for i in 0..50u32 {
                engine
                    .put(
                        format!("key_{:04}", i).into_bytes(),
                        format!("val_{:04}", i).into_bytes(),
                    )
                    .unwrap();
            }

            // Phase 2: drain any lingering frozen memtable
            drain_frozen(&engine);
            let stats = engine.stats().unwrap();
            assert!(stats.sstables_count > 0, "Expected SSTables from phase 1");
            assert_eq!(stats.frozen_count, 0);

            // Phase 3: issue deletes until a frozen memtable with tombstones appears
            for i in 0..50u32 {
                let key = format!("key_{:04}", i).into_bytes();
                engine.delete(key).unwrap();
                deleted_keys.push(i);
                if engine.stats().unwrap().frozen_count > 0 {
                    break;
                }
            }

            let stats = engine.stats().unwrap();
            assert!(
                stats.frozen_count >= 1,
                "Expected at least 1 frozen memtable with delete tombstones, got {}",
                stats.frozen_count
            );
            // Drop without close
        }

        let engine = reopen(dir.path());
        for i in 0..50u32 {
            let key = format!("key_{:04}", i).into_bytes();
            if deleted_keys.contains(&i) {
                assert_eq!(
                    engine.get(key).unwrap(),
                    None,
                    "key_{:04} should be deleted after crash",
                    i
                );
            } else {
                assert_eq!(
                    engine.get(key).unwrap(),
                    Some(format!("val_{:04}", i).into_bytes()),
                    "key_{:04} should still exist after crash",
                    i
                );
            }
        }
    }

    // ================================================================
    // 7. Range-delete tombstones in frozen memtable survive crash
    // ================================================================

    #[test]
    fn memtable_sstable__crash_recovery_frozen_with_range_deletes() {
        let dir = TempDir::new().unwrap();
        {
            let engine = Engine::open(dir.path(), small_buffer_config()).expect("open");

            // Phase 1: populate SSTables
            for i in 0..50u32 {
                engine
                    .put(
                        format!("key_{:04}", i).into_bytes(),
                        format!("val_{:04}", i).into_bytes(),
                    )
                    .unwrap();
            }

            // Phase 2: drain frozen
            drain_frozen(&engine);
            assert!(engine.stats().unwrap().sstables_count > 0);
            assert_eq!(engine.stats().unwrap().frozen_count, 0);

            // Phase 3: issue range delete, then fill until frozen_count >= 1
            engine
                .delete_range(b"key_0010".to_vec(), b"key_0020".to_vec())
                .unwrap();

            let mut filler = 0u32;
            while engine.stats().unwrap().frozen_count == 0 {
                engine
                    .put(
                        format!("fill_{:04}", filler).into_bytes(),
                        format!("fval_{:04}", filler).into_bytes(),
                    )
                    .unwrap();
                filler += 1;
                assert!(filler < 1_000, "Expected FlushRequired for filler puts");
            }

            let stats = engine.stats().unwrap();
            assert!(
                stats.frozen_count >= 1,
                "Expected at least 1 frozen memtable with range tombstone, got {}",
                stats.frozen_count
            );
            // Drop without close
        }

        let engine = reopen(dir.path());
        // Keys in the range-deleted interval should be gone
        for i in 10..20u32 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{:04} should be range-deleted after crash",
                i
            );
        }
        // Keys outside the interval should survive
        for i in (0..10).chain(20..50) {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(format!("val_{:04}", i).into_bytes()),
                "key_{:04} should survive crash",
                i
            );
        }
    }

    // ================================================================
    // 8. Scan returns correct results after crash (no close)
    // ================================================================

    #[test]
    fn memtable_sstable__crash_recovery_scan_correct() {
        let dir = TempDir::new().unwrap();
        let total;
        {
            let engine = Engine::open(dir.path(), small_buffer_config()).expect("open");
            total = fill_until_sstable_and_frozen(&engine, "sk");

            let stats = engine.stats().unwrap();
            assert!(stats.sstables_count > 0);
            assert!(
                stats.frozen_count >= 1,
                "Expected at least 1 frozen memtable, got {}",
                stats.frozen_count
            );
            // Drop without close
        }

        let engine = reopen(dir.path());
        let results = collect_scan(&engine, b"sk_", b"sk_\xff");
        assert_eq!(
            results.len(),
            total,
            "scan should return all {} keys after crash",
            total
        );
        // Verify sorted order
        for pair in results.windows(2) {
            assert!(pair[0].0 < pair[1].0, "scan must be sorted after crash");
        }
    }

    // ================================================================
    // 9. Mixed ops (puts + deletes + range deletes) across all layers,
    //    crash, reopen, full verification via get + scan.
    // ================================================================

    #[test]
    fn memtable_sstable__crash_recovery_mixed_ops() {
        let dir = TempDir::new().unwrap();
        {
            let engine = Engine::open(dir.path(), small_buffer_config()).expect("open");

            // Puts: key_0000..key_0049
            for i in 0..50u32 {
                engine
                    .put(
                        format!("key_{:04}", i).into_bytes(),
                        format!("val_{:04}", i).into_bytes(),
                    )
                    .unwrap();
            }

            // Point-delete a few
            engine.delete(b"key_0005".to_vec()).unwrap();
            engine.delete(b"key_0015".to_vec()).unwrap();

            // Range-delete [key_0030, key_0040)
            engine
                .delete_range(b"key_0030".to_vec(), b"key_0040".to_vec())
                .unwrap();

            // Overwrite one key
            engine
                .put(b"key_0002".to_vec(), b"new_val_0002".to_vec())
                .unwrap();

            // Ensure we have SSTables + frozen
            let stats = engine.stats().unwrap();
            // With 128-byte buffer and 50+ ops, we should have SSTables.
            // If we don't yet have a frozen, write fillers until we do.
            if stats.frozen_count == 0 {
                let mut j = 0u32;
                while engine.stats().unwrap().frozen_count == 0 {
                    engine
                        .put(
                            format!("pad_{:04}", j).into_bytes(),
                            format!("pval_{:04}", j).into_bytes(),
                        )
                        .unwrap();
                    j += 1;
                }
            }

            let stats = engine.stats().unwrap();
            assert!(stats.sstables_count > 0, "Expected SSTables");
            assert!(
                stats.frozen_count >= 1,
                "Expected at least 1 frozen memtable, got {}",
                stats.frozen_count
            );
            // Drop without close
        }

        let engine = reopen(dir.path());

        // Verify point deletes
        assert_eq!(engine.get(b"key_0005".to_vec()).unwrap(), None);
        assert_eq!(engine.get(b"key_0015".to_vec()).unwrap(), None);

        // Verify range deletes
        for i in 30..40u32 {
            assert_eq!(
                engine.get(format!("key_{:04}", i).into_bytes()).unwrap(),
                None,
                "key_{:04} should be range-deleted",
                i
            );
        }

        // Verify overwrite
        assert_eq!(
            engine.get(b"key_0002".to_vec()).unwrap(),
            Some(b"new_val_0002".to_vec())
        );

        // Verify surviving keys
        for i in [0u32, 1, 3, 4, 6, 7, 10, 20, 25, 40, 41, 49] {
            assert_eq!(
                engine.get(format!("key_{:04}", i).into_bytes()).unwrap(),
                Some(format!("val_{:04}", i).into_bytes()),
                "key_{:04} should survive crash",
                i
            );
        }

        // Scan should be sorted and consistent
        let results = collect_scan(&engine, b"key_", b"key_\xff");
        for pair in results.windows(2) {
            assert!(pair[0].0 < pair[1].0, "scan must be sorted");
        }
        // Deleted keys must not appear in scan
        let keys_in_scan: Vec<&Vec<u8>> = results.iter().map(|(k, _)| k).collect();
        assert!(!keys_in_scan.contains(&&b"key_0005".to_vec()));
        assert!(!keys_in_scan.contains(&&b"key_0015".to_vec()));
        for i in 30..40u32 {
            assert!(!keys_in_scan.contains(&&format!("key_{:04}", i).into_bytes()));
        }
    }
}
