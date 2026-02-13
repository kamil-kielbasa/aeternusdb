//! Edge-case tests: empty keys/values, scan boundaries, stats correctness,
//! close semantics, large/binary keys, and misc corner cases.
//!
//! ## Layer coverage
//! - `memtable__*`: memtable-only edge cases (validation, boundaries)
//! - `memtable_sstable__*`: edge cases involving SSTable flush and recovery
//!
//! ## See also
//! - [`tests_hardening`] — concurrency, extreme configs, orphan cleanup

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::tests::helpers::*;
    use crate::engine::{Engine, EngineConfig};
    use tempfile::TempDir;

    // ================================================================
    // Empty key / empty value
    // ================================================================

    #[test]
    fn memtable__empty_key_is_rejected() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        let result = engine.put(vec![], b"value".to_vec());
        assert!(result.is_err(), "empty key should be rejected");
    }

    #[test]
    fn memtable__empty_value_is_rejected() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        let result = engine.put(b"key".to_vec(), vec![]);
        assert!(result.is_err(), "empty value should be rejected");
    }

    #[test]
    fn memtable__empty_key_and_value_rejected() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        let result = engine.put(vec![], vec![]);
        assert!(result.is_err(), "empty key+value should be rejected");
    }

    // ================================================================
    // Scan boundary edge cases
    // ================================================================

    #[test]
    fn memtable__scan_start_equals_end_empty() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();
        engine.put(b"aaa".to_vec(), b"v".to_vec()).unwrap();

        let results = collect_scan(&engine, b"aaa", b"aaa");
        assert!(results.is_empty(), "scan(x, x) should return nothing");
    }

    #[test]
    fn memtable__scan_start_gt_end_empty() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();
        engine.put(b"aaa".to_vec(), b"v1".to_vec()).unwrap();
        engine.put(b"zzz".to_vec(), b"v2".to_vec()).unwrap();

        let results = collect_scan(&engine, b"zzz", b"aaa");
        assert!(
            results.is_empty(),
            "scan(high, low) should return nothing (or at least not panic)"
        );
    }

    #[test]
    fn memtable__scan_exact_boundary_inclusivity() {
        // Verify: start key is INCLUSIVE, end key is EXCLUSIVE.
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();
        for i in 0..10u8 {
            engine
                .put(vec![b'k', i], format!("v{}", i).into_bytes())
                .unwrap();
        }

        // scan([k, 3], [k, 7]) should include k3, k4, k5, k6 but NOT k7
        let results = collect_scan(&engine, &[b'k', 3], &[b'k', 7]);
        let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();

        assert!(keys.contains(&vec![b'k', 3]), "start key must be inclusive");
        assert!(
            keys.contains(&vec![b'k', 6]),
            "key just before end must be included"
        );
        assert!(!keys.contains(&vec![b'k', 7]), "end key must be exclusive");
        assert!(
            !keys.contains(&vec![b'k', 2]),
            "key before start must be excluded"
        );
    }

    #[test]
    fn memtable__scan_full_keyspace() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();
        for i in 0..20u32 {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("val_{:04}", i).into_bytes(),
                )
                .unwrap();
        }

        let results = collect_scan(&engine, b"\x00", b"\xff");
        assert_eq!(
            results.len(),
            20,
            "full keyspace scan should return all keys"
        );
    }

    // ================================================================
    // Stats correctness
    // ================================================================

    #[test]
    fn memtable_sstable__stats_frozen_count_transitions() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        // Initially: no frozen, no SSTables
        let s = engine.stats().unwrap();
        assert_eq!(s.frozen_count, 0);
        assert_eq!(s.sstables_count, 0);

        // Write until we get a frozen memtable
        let mut i = 0u32;
        loop {
            engine
                .put(
                    format!("k_{:04}", i).into_bytes(),
                    format!("v_{:04}", i).into_bytes(),
                )
                .unwrap();
            let s = engine.stats().unwrap();
            if s.frozen_count > 0 {
                // Frozen memtable exists; may or may not have SSTables yet
                break;
            }
            i += 1;
        }

        let before_frozen = engine.stats().unwrap().frozen_count;
        assert!(before_frozen >= 1, "Should have at least 1 frozen");

        // Next put should flush the frozen → SSTable
        engine.put(b"trigger".to_vec(), b"flush".to_vec()).unwrap();
        let after = engine.stats().unwrap();
        // The frozen that existed should now be an SSTable
        assert!(
            after.sstables_count > 0,
            "Frozen should have been flushed to SSTable"
        );
    }

    // ================================================================
    // Close semantics
    // ================================================================

    #[test]
    fn memtable_sstable__operations_after_close_work() {
        // close() flushes frozen and checkpoints, but the engine struct
        // remains usable. Verify reads still work.
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.close().unwrap();

        // Reads after close should still see data
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            Some(b"v".to_vec()),
            "get after close should still return data"
        );

        // Writes after close
        engine.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
        assert_eq!(
            engine.get(b"k2".to_vec()).unwrap(),
            Some(b"v2".to_vec()),
            "put after close should succeed"
        );

        // Scan after close
        let results = collect_scan(&engine, b"k", b"k\xff");
        assert!(
            !results.is_empty(),
            "scan after close should return results"
        );
    }

    #[test]
    fn memtable_sstable__multiple_close_calls_safe() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();

        engine.close().unwrap();
        engine.close().unwrap(); // Second close should not panic or error

        let engine = reopen(dir.path());
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v".to_vec()),);
    }

    #[test]
    fn memtable_sstable__close_no_writes_reopen() {
        // open → write → close → reopen → close (no writes) → reopen
        let dir = TempDir::new().unwrap();

        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.close().unwrap();

        let engine = reopen(dir.path());
        engine.close().unwrap(); // No new writes, just close

        let engine = reopen(dir.path());
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            Some(b"v".to_vec()),
            "data must survive close-with-no-writes cycle"
        );
    }

    // ================================================================
    // Reopen after only deletes
    // ================================================================

    #[test]
    fn memtable_sstable__reopen_after_only_deletes() {
        let dir = TempDir::new().unwrap();

        // Session 1: put some keys
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        engine.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        engine.close().unwrap();

        // Session 2: only deletes, no puts
        let engine = reopen(dir.path());
        engine.delete(b"b".to_vec()).unwrap();
        engine.delete_range(b"c".to_vec(), b"d".to_vec()).unwrap();
        engine.close().unwrap();

        // Session 3: verify
        let engine = reopen(dir.path());
        assert_eq!(
            engine.get(b"a".to_vec()).unwrap(),
            Some(b"1".to_vec()),
            "a should survive"
        );
        assert_eq!(engine.get(b"b".to_vec()).unwrap(), None, "b was deleted");
        assert_eq!(
            engine.get(b"c".to_vec()).unwrap(),
            None,
            "c was range-deleted"
        );
    }

    // ================================================================
    // Very large keys
    // ================================================================

    #[test]
    fn memtable_sstable__very_large_key_recovery() {
        let dir = TempDir::new().unwrap();

        let big_key = vec![0xAB; 8192]; // 8 KB key
        let value = b"big_key_value".to_vec();

        // Use a 16 KB buffer so the single 8 KB key fits in one memtable
        let config = EngineConfig {
            write_buffer_size: 16 * 1024,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 1024,
            min_threshold: 4,
            max_threshold: 32,
            tombstone_threshold: 0.2,
            tombstone_compaction_interval: 3600,
            thread_pool_size: 2,
        };

        let engine = Engine::open(dir.path(), config).unwrap();
        engine.put(big_key.clone(), value.clone()).unwrap();
        // Write enough padding to exceed the 16 KB buffer → SSTable flush
        for i in 0..600u32 {
            engine
                .put(
                    format!("pad_{:04}", i).into_bytes(),
                    format!("padding_value_with_extra_bytes_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        let stats = engine.stats().unwrap();
        assert!(stats.sstables_count > 0, "Expected SSTables");
        assert_eq!(
            engine.get(big_key.clone()).unwrap(),
            Some(value.clone()),
            "large key readable before close"
        );
        engine.close().unwrap();

        // Verify after reopen
        let engine = reopen(dir.path());
        assert_eq!(
            engine.get(big_key).unwrap(),
            Some(value),
            "8 KB key must survive SSTable flush + reopen"
        );
    }

    // ================================================================
    // Binary keys (0x00/0xFF) through SSTable and recovery
    // ================================================================

    #[test]
    fn memtable_sstable__binary_keys_recovery() {
        let dir = TempDir::new().unwrap();

        let key_zeros = vec![0x00; 32];
        let key_ones = vec![0xFF; 32];
        let key_mixed = vec![0x00, 0xFF, 0x00, 0xFF, 0x01, 0xFE];

        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(key_zeros.clone(), b"zeros".to_vec()).unwrap();
        engine.put(key_ones.clone(), b"ones".to_vec()).unwrap();
        engine.put(key_mixed.clone(), b"mixed".to_vec()).unwrap();

        // Force SSTable flush
        for i in 0..200u32 {
            engine
                .put(
                    format!("pad_{:04}", i).into_bytes(),
                    format!("pval_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        let stats = engine.stats().unwrap();
        assert!(stats.sstables_count > 0, "Expected SSTables");

        // Verify before close
        assert_eq!(
            engine.get(key_zeros.clone()).unwrap(),
            Some(b"zeros".to_vec())
        );
        assert_eq!(
            engine.get(key_ones.clone()).unwrap(),
            Some(b"ones".to_vec())
        );
        assert_eq!(
            engine.get(key_mixed.clone()).unwrap(),
            Some(b"mixed".to_vec())
        );
        engine.close().unwrap();

        // Verify after reopen
        let engine = reopen(dir.path());
        assert_eq!(
            engine.get(key_zeros).unwrap(),
            Some(b"zeros".to_vec()),
            "0x00 key must survive SSTable + reopen"
        );
        assert_eq!(
            engine.get(key_ones).unwrap(),
            Some(b"ones".to_vec()),
            "0xFF key must survive SSTable + reopen"
        );
        assert_eq!(
            engine.get(key_mixed).unwrap(),
            Some(b"mixed".to_vec()),
            "mixed binary key must survive SSTable + reopen"
        );
    }

    // ================================================================
    // Range-delete / delete on empty database
    // ================================================================

    #[test]
    fn memtable__range_delete_on_empty_db() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        // Should not panic or error
        engine
            .delete_range(b"start".to_vec(), b"end".to_vec())
            .unwrap();
        assert_eq!(
            engine.get(b"anything".to_vec()).unwrap(),
            None,
            "empty DB returns None"
        );

        // Scan should be empty
        let results = collect_scan(&engine, b"\x00", b"\xff");
        assert!(results.is_empty(), "empty DB scan should return nothing");
    }

    #[test]
    fn memtable__delete_on_empty_db() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        engine.delete(b"nonexistent".to_vec()).unwrap();
        assert_eq!(engine.get(b"nonexistent".to_vec()).unwrap(), None);
    }

    // ================================================================
    // Scan where all keys in range are deleted
    // ================================================================

    #[test]
    fn memtable__scan_all_keys_deleted() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        for i in 0..10u32 {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("val_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        // Delete everything via range
        engine
            .delete_range(b"key_0000".to_vec(), b"key_9999".to_vec())
            .unwrap();

        let results = collect_scan(&engine, b"key_", b"key_\xff");
        assert!(
            results.is_empty(),
            "scan should return empty when all keys in range are deleted"
        );
    }
}
