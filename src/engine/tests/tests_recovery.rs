//! Recovery / reopen tests: verify durability across close → reopen.
//!
//! ## Layer coverage
//! - All tests use `memtable_sstable` (close flushes WAL/frozen → SSTable)
//! - `memtable_sstable__wal_replay_*`: WAL-only recovery (large buffer, no SSTable flush)
//!
//! ## See also
//! - [`tests_crash_recovery`] — drop without close() (frozen WAL replay path)
//! - [`tests_lsn_continuity`] — LSN ordering correctness after reopen

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Basic: put, close, reopen → data survives
    // ----------------------------------------------------------------

    #[test]
    fn memtable_sstable__data_survives_close_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            engine.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
            engine.put(b"key2".to_vec(), b"val2".to_vec()).unwrap();
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        assert_eq!(
            engine.get(b"key1".to_vec()).unwrap(),
            Some(b"val1".to_vec())
        );
        assert_eq!(
            engine.get(b"key2".to_vec()).unwrap(),
            Some(b"val2".to_vec())
        );
    }

    // ----------------------------------------------------------------
    // Overwrite survives reopen
    // ----------------------------------------------------------------

    #[test]
    fn memtable_sstable__overwrite_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            engine.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
            engine.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v2".to_vec()));
    }

    // ----------------------------------------------------------------
    // Delete survives reopen
    // ----------------------------------------------------------------

    #[test]
    fn memtable_sstable__delete_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            engine.put(b"k".to_vec(), b"val".to_vec()).unwrap();
            engine.delete(b"k".to_vec()).unwrap();
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), None);
    }

    // ----------------------------------------------------------------
    // Range delete survives reopen
    // ----------------------------------------------------------------

    #[test]
    fn memtable_sstable__range_delete_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            for i in 0..20 {
                let key = format!("key_{:02}", i).into_bytes();
                let val = format!("val_{:02}", i).into_bytes();
                engine.put(key, val).unwrap();
            }
            engine
                .delete_range(b"key_05".to_vec(), b"key_15".to_vec())
                .unwrap();
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        for i in 0..5 {
            let key = format!("key_{:02}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{:02} should survive",
                i
            );
        }
        for i in 5..15 {
            let key = format!("key_{:02}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{:02} should be range-deleted",
                i
            );
        }
        for i in 15..20 {
            let key = format!("key_{:02}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{:02} should survive",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Large dataset → SSTable flush → reopen → data intact
    // ----------------------------------------------------------------

    #[test]
    fn memtable_sstable__sstable_data_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = engine_with_sstables(tmp.path(), 200, "key");
            assert!(engine.stats().unwrap().sstables_count > 0);
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        for i in 0..200 {
            let key = format!("key_{:04}", i).into_bytes();
            let expected = format!("value_with_some_padding_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key.clone()).unwrap(),
                Some(expected),
                "key_{:04} missing after reopen",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Multiple close-reopen cycles
    // ----------------------------------------------------------------

    #[test]
    fn memtable_sstable__multiple_reopen_cycles() {
        let tmp = TempDir::new().unwrap();

        for cycle in 0..3 {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            for i in 0..20 {
                let key = format!("c{}_{:02}", cycle, i).into_bytes();
                let val = format!("val_{}_{:02}", cycle, i).into_bytes();
                engine.put(key, val).unwrap();
            }
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        for cycle in 0..3 {
            for i in 0..20 {
                let key = format!("c{}_{:02}", cycle, i).into_bytes();
                let expected = format!("val_{}_{:02}", cycle, i).into_bytes();
                assert_eq!(
                    engine.get(key.clone()).unwrap(),
                    Some(expected),
                    "cycle {} key {} missing",
                    cycle,
                    i
                );
            }
        }
    }

    // ----------------------------------------------------------------
    // WAL replay: data in active memtable (not yet flushed) is recovered
    // ----------------------------------------------------------------

    #[test]
    fn memtable_sstable__wal_replay_recovers_data() {
        let tmp = TempDir::new().unwrap();

        {
            // Use large buffer so nothing flushes — data stays in WAL only
            let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
            engine
                .put(b"wal_key".to_vec(), b"wal_val".to_vec())
                .unwrap();
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        assert_eq!(
            engine.get(b"wal_key".to_vec()).unwrap(),
            Some(b"wal_val".to_vec())
        );
    }

    // ----------------------------------------------------------------
    // Scan correctness after reopen
    // ----------------------------------------------------------------

    #[test]
    fn memtable_sstable__scan_works_after_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            for i in 0..50 {
                let key = format!("sk_{:04}", i).into_bytes();
                let val = format!("sv_{:04}", i).into_bytes();
                engine.put(key, val).unwrap();
            }
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        let results = collect_scan(&engine, b"sk_", b"sk_\xff");

        assert_eq!(results.len(), 50);
        // Verify sorted order
        for i in 1..results.len() {
            assert!(results[i - 1].0 < results[i].0, "Keys should be sorted");
        }
    }

    // ----------------------------------------------------------------
    // Delete + reopen + verify tombstone is durable
    // ----------------------------------------------------------------

    #[test]
    fn memtable_sstable__delete_tombstone_durable_after_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = engine_with_sstables(tmp.path(), 200, "dt");
            // Delete some SSTable keys
            for i in 0..50 {
                let key = format!("dt_{:04}", i).into_bytes();
                engine.delete(key).unwrap();
            }
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        for i in 0..50 {
            let key = format!("dt_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "dt_{:04} should still be deleted after reopen",
                i
            );
        }
        for i in 50..200 {
            let key = format!("dt_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "dt_{:04} should still exist after reopen",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Multiple overwrites → reopen → latest value
    // ----------------------------------------------------------------

    #[test]
    fn memtable_sstable__overwrite_chain_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            for round in 0..5 {
                engine
                    .put(b"chain".to_vec(), format!("v{}", round).into_bytes())
                    .unwrap();
            }
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        assert_eq!(engine.get(b"chain".to_vec()).unwrap(), Some(b"v4".to_vec()));
    }
}
