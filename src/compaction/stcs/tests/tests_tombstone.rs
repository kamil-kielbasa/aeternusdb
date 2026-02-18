//! Tombstone compaction tests.

#[cfg(test)]
mod tests {
    use crate::engine::{Engine, EngineConfig};
    use std::fs;

    fn tombstone_config() -> EngineConfig {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        EngineConfig {
            write_buffer_size: 256,
            compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 50,
            min_threshold: 100,
            max_threshold: 200,
            tombstone_ratio_threshold: 0.1,
            tombstone_compaction_interval: 0,
            tombstone_bloom_fallback: true,
            tombstone_range_drop: true,
            thread_pool_size: 2,
        }
    }

    fn tombstone_config_no_bloom_fallback() -> EngineConfig {
        let mut c = tombstone_config();
        c.tombstone_bloom_fallback = false;
        c
    }

    fn fresh_dir(name: &str) -> String {
        let path = format!("/tmp/aeternusdb_test_compaction_tombstone_{}", name);
        let _ = fs::remove_dir_all(&path);
        path
    }

    /// # Scenario
    /// Tombstone compaction is a no-op when no SSTable has a tombstone
    /// ratio exceeding `tombstone_ratio_threshold`.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer, `tombstone_ratio_threshold = 0.1`.
    ///
    /// # Actions
    /// 1. Write 30 keys, flush.
    /// 2. Record `before` stats.
    /// 3. `tombstone_compact()`.
    ///
    /// # Expected behavior
    /// - Returns `false`.
    /// - SSTable count and total size unchanged.
    #[test]
    fn tombstone_compact_noop_when_no_tombstones() {
        let dir = fresh_dir("noop");
        let engine = Engine::open(&dir, tombstone_config()).unwrap();

        for i in 0..30 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();

        let compacted = engine.tombstone_compact().unwrap();
        assert!(!compacted, "should not compact when no tombstones");

        let after = engine.stats().unwrap();
        assert_eq!(
            after.sstables_count, before.sstables_count,
            "SSTable count should be unchanged"
        );
        assert_eq!(
            after.total_sst_size_bytes, before.total_sst_size_bytes,
            "total size should be unchanged"
        );
    }

    /// # Scenario
    /// With `tombstone_bloom_fallback = true`, tombstone compaction can use
    /// bloom filters to prove tombstones are unnecessary when no older
    /// SSTable contains the deleted key.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer, `tombstone_bloom_fallback = true`.
    ///
    /// # Actions
    /// 1. Write disjoint set A (`key_0000`..`key_0019`) → flush.
    /// 2. Write disjoint set B (`key_0100`..`key_0109`) that was **never
    ///    written** — each delete, followed by a padding put, so that the
    ///    resulting SSTable contains a mix of tombstones and live data and
    ///    exceeds the tombstone ratio threshold.
    /// 3. Flush.
    /// 4. `tombstone_compact()`.
    ///
    /// # Expected behavior
    /// - Returns `true`.
    /// - Set A keys intact; set B keys remain `None`.
    #[test]
    fn tombstone_compact_drops_point_tombstones_with_bloom_fallback() {
        let dir = fresh_dir("bloom_fallback");
        let engine = Engine::open(&dir, tombstone_config()).unwrap();

        // SSTable(s): keys 0..20
        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Write deletes for keys 100..110 that were NEVER written,
        // plus padding puts on disjoint keys (prefix "pad_").
        // The padding ensures a realistic SSTable (not 100% tombstones)
        // and keep the tombstones and padding entries together in the
        // same freeze cycles so the delete SSTable has the tombstones.
        for i in 100..110 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
            // Padding put with a unique key that doesn't collide.
            let pad = format!("pad_{:04}", i).into_bytes();
            engine.put(pad, b"x".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();

        let compacted = engine.tombstone_compact().unwrap();
        assert!(compacted, "should have compacted");

        let after = engine.stats().unwrap();
        // Tombstones for non-existent keys should be dropped — expect strict decrease.
        assert!(
            after.total_sst_size_bytes < before.total_sst_size_bytes,
            "total SST size should strictly decrease: before={} B, after={} B",
            before.total_sst_size_bytes,
            after.total_sst_size_bytes,
        );

        // Original data still intact.
        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }

        // Deleted keys still deleted.
        for i in 100..110 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), None);
        }

        // Padding keys still intact.
        for i in 100..110 {
            let pad = format!("pad_{:04}", i).into_bytes();
            assert_eq!(engine.get(pad).unwrap(), Some(b"x".to_vec()));
        }
    }

    /// # Scenario
    /// Without bloom fallback, the "maybe present" result from the bloom
    /// filter cannot be resolved. Tombstones must be conservatively kept
    /// to avoid resurrecting deleted keys.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer, `tombstone_bloom_fallback = false`.
    ///
    /// # Actions
    /// 1. Write keys 0..20, flush.
    /// 2. Delete keys 0..10, flush.
    /// 3. Record `before` stats.
    /// 4. `tombstone_compact()`.
    /// 5. Record `after` stats.
    ///
    /// # Expected behavior
    /// - SSTable count does not increase.
    /// - Deleted keys 0..10 still return `None` (tombstones kept).
    /// - Live keys 10..20 still return their values.
    #[test]
    fn tombstone_compact_conservative_without_bloom_fallback() {
        let dir = fresh_dir("no_fallback");
        let engine = Engine::open(&dir, tombstone_config_no_bloom_fallback()).unwrap();

        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        for i in 0..10 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();

        let _compacted = engine.tombstone_compact().unwrap();

        let after = engine.stats().unwrap();
        assert!(
            after.sstables_count <= before.sstables_count,
            "SSTable count should not increase: before={}, after={}",
            before.sstables_count,
            after.sstables_count,
        );

        // All deletes still visible (conservative — tombstones kept).
        for i in 0..10 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{i:04} should stay deleted"
            );
        }
        for i in 10..20 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
    }

    /// # Scenario
    /// With `tombstone_range_drop = true`, tombstone compaction evaluates
    /// range tombstones for possible removal when no older SSTable overlaps.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer, `tombstone_range_drop = true`,
    /// `tombstone_bloom_fallback = true`.
    ///
    /// # Actions
    /// 1. Write disjoint set A (keys 0..20), flush.
    /// 2. Write disjoint set B (keys 50..70), flush.
    /// 3. `delete_range("key_0050", "key_0070")` + padding keys 200..220, flush.
    /// 4. Record `before` stats.
    /// 5. `tombstone_compact()`.
    /// 6. Record `after` stats.
    ///
    /// # Expected behavior
    /// - SSTable count does not increase.
    /// - Set A keys intact; set B keys `None`; padding keys intact.
    #[test]
    fn tombstone_compact_with_range_tombstone() {
        let dir = fresh_dir("range_tombstone");
        let engine = Engine::open(&dir, tombstone_config()).unwrap();

        // SSTable 1: keys 0..20
        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // SSTable 2: keys 50..70
        for i in 50..70 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Now range-delete 50..70 and also write padding to force freeze.
        engine
            .delete_range(b"key_0050".to_vec(), b"key_0070".to_vec())
            .unwrap();
        // Pad to force freeze (buffer=256 bytes).
        for i in 200..220 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"pad".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();

        let compacted = engine.tombstone_compact().unwrap();

        let after = engine.stats().unwrap();
        assert!(
            after.sstables_count <= before.sstables_count,
            "SSTable count should not increase: before={}, after={}",
            before.sstables_count,
            after.sstables_count,
        );
        if compacted {
            // When compaction ran, size should strictly decrease.
            assert!(
                after.total_sst_size_bytes < before.total_sst_size_bytes,
                "compacted tombstone SSTable should be smaller: before={} B, after={} B",
                before.total_sst_size_bytes,
                after.total_sst_size_bytes,
            );
        }

        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
        for i in 50..70 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{i:04} should be deleted"
            );
        }
        if compacted {
            // If it did compact, data pad keys should be intact.
            for i in 200..220 {
                let key = format!("key_{:04}", i).into_bytes();
                assert_eq!(engine.get(key).unwrap(), Some(b"pad".to_vec()));
            }
        }
    }

    /// # Scenario
    /// Tombstone compaction result is durable across engine close/reopen.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer, `tombstone_ratio_threshold = 0.1`.
    ///
    /// # Actions
    /// 1. Write 30 keys, flush.
    /// 2. Delete 15 keys, flush.
    /// 3. `tombstone_compact()`.
    /// 4. Close engine (drop).
    /// 5. Reopen engine.
    /// 6. Read all 30 keys.
    ///
    /// # Expected behavior
    /// - Deleted keys 0..15 return `None`.
    /// - Live keys 15..30 return their values.
    #[test]
    fn tombstone_compact_survives_reopen() {
        let dir = fresh_dir("reopen");

        {
            let engine = Engine::open(&dir, tombstone_config()).unwrap();

            for i in 0..30 {
                let key = format!("key_{:04}", i).into_bytes();
                engine.put(key, b"val".to_vec()).unwrap();
            }
            engine.flush_all_frozen().unwrap();

            for i in 0..15 {
                let key = format!("key_{:04}", i).into_bytes();
                engine.delete(key).unwrap();
            }
            engine.flush_all_frozen().unwrap();

            engine.tombstone_compact().unwrap();
        }

        let engine = Engine::open(&dir, tombstone_config()).unwrap();

        for i in 0..15 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), None);
        }
        for i in 15..30 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
    }

    /// # Scenario
    /// Tombstone compaction skips SSTables whose tombstone ratio is just
    /// below the configured threshold.
    ///
    /// # Starting environment
    /// Empty engine with large write buffer (4 KiB) so that puts and deletes
    /// land in the same SSTable. `tombstone_ratio_threshold = 0.5` (50%).
    ///
    /// # Actions
    /// 1. Write 20 keys then delete 5 of them *without flushing in between*.
    /// 2. Flush → one SSTable with 25 records (20 puts + 5 deletes),
    ///    tombstone ratio = 5 / 25 = 20%.
    /// 3. `tombstone_compact()`.
    ///
    /// # Expected behavior
    /// - Returns `false` — 20% < 50% threshold.
    /// - SSTable count and total size unchanged.
    /// - Deleted keys still return `None`; live keys intact.
    #[test]
    fn tombstone_compact_below_threshold_noop() {
        let dir = fresh_dir("below_threshold");
        let mut cfg = tombstone_config();
        cfg.tombstone_ratio_threshold = 0.5;
        cfg.write_buffer_size = 4096; // large enough for all 25 records
        let engine = Engine::open(&dir, cfg).unwrap();

        // Write puts and deletes without flushing between — they end up
        // in the same memtable and thus the same SSTable.
        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        for i in 0..5 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();

        let compacted = engine.tombstone_compact().unwrap();
        assert!(
            !compacted,
            "tombstone ratio below threshold — should not compact"
        );

        let after = engine.stats().unwrap();
        assert_eq!(
            after.sstables_count, before.sstables_count,
            "SSTable count unchanged"
        );
        assert_eq!(
            after.total_sst_size_bytes, before.total_sst_size_bytes,
            "total size unchanged"
        );

        for i in 0..5 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{i:04} should be deleted"
            );
        }
        for i in 5..20 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
    }
}
