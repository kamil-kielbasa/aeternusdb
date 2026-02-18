//! Major compaction tests.

#[cfg(test)]
mod tests {
    use crate::engine::{Engine, EngineConfig};
    use std::fs;

    fn compaction_config() -> EngineConfig {
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
            tombstone_ratio_threshold: 0.2,
            tombstone_compaction_interval: 0,
            tombstone_bloom_fallback: false,
            tombstone_range_drop: false,
            thread_pool_size: 2,
        }
    }

    fn fresh_dir(name: &str) -> String {
        let path = format!("/tmp/aeternusdb_test_compaction_major_{}", name);
        let _ = fs::remove_dir_all(&path);
        path
    }

    /// # Scenario
    /// Major compaction merges all SSTables into exactly one,
    /// deduplicating and dropping all tombstones.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer.
    ///
    /// # Actions
    /// 1. Write 100 keys (8 B key + 8 B value), flush.
    /// 2. Record `before` stats.
    /// 3. `major_compact()`.
    /// 4. Record `after` stats.
    ///
    /// # Expected behavior
    /// - Returns `true`.
    /// - Exactly 1 SSTable remains.
    /// - Total SST size decreases (per-SSTable overhead eliminated).
    /// - All 100 keys readable.
    #[test]
    fn major_compact_merges_all_sstables_into_one() {
        let dir = fresh_dir("basic");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            let val = format!("val_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();
        assert!(
            before.sstables_count >= 2,
            "need at least 2 SSTables, got {}",
            before.sstables_count
        );

        let compacted = engine.major_compact().unwrap();
        assert!(compacted, "major_compact should have run");

        let after = engine.stats().unwrap();
        assert_eq!(
            after.sstables_count, 1,
            "should have exactly 1 SSTable after major compaction, got {} (was {})",
            after.sstables_count, before.sstables_count,
        );
        // Merging many small SSTables into 1 eliminates per-SSTable overhead;
        // expect at least 20 % size reduction.
        let max_size = (before.total_sst_size_bytes as f64 * 0.80) as u64;
        assert!(
            after.total_sst_size_bytes <= max_size,
            "total SST size should decrease by ≥ 20 %: before={} B, after={} B, max allowed={} B",
            before.total_sst_size_bytes,
            after.total_sst_size_bytes,
            max_size,
        );

        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            let expected = format!("val_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(expected));
        }
    }

    /// # Scenario
    /// Major compaction is a no-op with 0 or 1 SSTables.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer.
    ///
    /// # Actions
    /// 1. `major_compact()` on empty engine.
    /// 2. Write 5 keys (fits in 1 SSTable), flush.
    /// 3. `major_compact()` again.
    ///
    /// # Expected behavior
    /// - Both calls return `false`.
    /// - SSTable count and total size unchanged after each call.
    #[test]
    fn major_compact_returns_false_with_zero_or_one_sstable() {
        let dir = fresh_dir("noop_empty");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        let compacted = engine.major_compact().unwrap();
        assert!(!compacted, "no SSTables — should not compact");
        assert_eq!(engine.stats().unwrap().sstables_count, 0);

        for i in 0..5 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();
        assert_eq!(before.sstables_count, 1);

        let compacted = engine.major_compact().unwrap();
        assert!(!compacted, "1 SSTable — should not compact");

        let after = engine.stats().unwrap();
        assert_eq!(after.sstables_count, 1, "SSTable count should be unchanged");
        assert_eq!(
            after.total_sst_size_bytes, before.total_sst_size_bytes,
            "total size should be unchanged"
        );
    }

    /// # Scenario
    /// Major compaction drops all point tombstones because the merged
    /// output is the single authoritative SSTable — nothing to shadow.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer.
    ///
    /// # Actions
    /// 1. Write 30 keys (8 B key + 3 B value), flush.
    /// 2. Delete keys 0..15, flush.
    /// 3. Record `before` stats.
    /// 4. `major_compact()`.
    /// 5. Record `after` stats.
    ///
    /// # Expected behavior
    /// - Exactly 1 SSTable remains.
    /// - Total SST size decreases (tombstones + dead data eliminated).
    /// - Deleted keys 0..15 return `None`; live keys 15..30 return values.
    #[test]
    fn major_compact_drops_point_tombstones() {
        let dir = fresh_dir("drops_tombstones");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

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

        let before = engine.stats().unwrap();
        assert!(
            before.sstables_count >= 2,
            "need at least 2 SSTables, got {}",
            before.sstables_count
        );

        engine.major_compact().unwrap();

        let after = engine.stats().unwrap();
        assert_eq!(
            after.sstables_count, 1,
            "should have 1 SSTable after major compaction (was {})",
            before.sstables_count
        );
        // Half the keys were deleted — expect at least 30 % size reduction
        // (dead puts + tombstones removed).
        let max_size = (before.total_sst_size_bytes as f64 * 0.70) as u64;
        assert!(
            after.total_sst_size_bytes <= max_size,
            "total SST size should decrease by ≥ 30 % (tombstones + dead data dropped): before={} B, after={} B, max allowed={} B",
            before.total_sst_size_bytes,
            after.total_sst_size_bytes,
            max_size,
        );

        for i in 0..15 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), None, "key_{i:04} should be gone");
        }
        for i in 15..30 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
    }

    /// # Scenario
    /// Major compaction applies range tombstones, suppressing all covered
    /// Puts with lower LSN and producing a single clean SSTable.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer.
    ///
    /// # Actions
    /// 1. Write 50 keys, flush.
    /// 2. `delete_range("key_0020", "key_0040")`, flush.
    /// 3. Record `before` stats.
    /// 4. `major_compact()`.
    /// 5. Record `after` stats.
    ///
    /// # Expected behavior
    /// - Exactly 1 SSTable.
    /// - Total SST size decreases (dead data + range tombstone dropped).
    /// - Keys 20..40 return `None`; other keys intact.
    #[test]
    fn major_compact_applies_range_tombstones() {
        let dir = fresh_dir("range_tombstones");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        engine
            .delete_range(b"key_0020".to_vec(), b"key_0040".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();
        assert!(
            before.sstables_count >= 2,
            "need at least 2 SSTables, got {}",
            before.sstables_count
        );

        engine.major_compact().unwrap();

        let after = engine.stats().unwrap();
        assert_eq!(
            after.sstables_count, 1,
            "should have 1 SSTable after major compaction (was {})",
            before.sstables_count
        );
        // 20 of 50 keys range-deleted — expect at least 25 % size reduction.
        let max_size = (before.total_sst_size_bytes as f64 * 0.75) as u64;
        assert!(
            after.total_sst_size_bytes <= max_size,
            "total SST size should decrease by ≥ 25 %: before={} B, after={} B, max allowed={} B",
            before.total_sst_size_bytes,
            after.total_sst_size_bytes,
            max_size,
        );

        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
        for i in 20..40 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{i:04} should be suppressed"
            );
        }
        for i in 40..50 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
    }

    /// # Scenario
    /// Major compaction deduplicates overwritten keys, keeping only the
    /// newest version.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer.
    ///
    /// # Actions
    /// 1. Write 20 keys with value `"v1"`, flush.
    /// 2. Overwrite same 20 keys with `"v2"`, flush.
    /// 3. Record `before` stats.
    /// 4. `major_compact()`.
    /// 5. Record `after` stats.
    ///
    /// # Expected behavior
    /// - Exactly 1 SSTable.
    /// - Total SST size decreases (v1 copies eliminated).
    /// - All 20 keys return `"v2"`.
    #[test]
    fn major_compact_deduplicates_and_keeps_newest() {
        let dir = fresh_dir("dedup");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"v1".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"v2".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();
        assert!(
            before.sstables_count >= 2,
            "need at least 2 SSTables, got {}",
            before.sstables_count
        );

        engine.major_compact().unwrap();

        let after = engine.stats().unwrap();
        assert_eq!(
            after.sstables_count, 1,
            "should have 1 SSTable after major compaction (was {})",
            before.sstables_count
        );
        // Two complete copies deduped into one — expect at least 25 % reduction.
        let max_size = (before.total_sst_size_bytes as f64 * 0.75) as u64;
        assert!(
            after.total_sst_size_bytes <= max_size,
            "total SST size should decrease by ≥ 25 % (duplicates eliminated): before={} B, after={} B, max allowed={} B",
            before.total_sst_size_bytes,
            after.total_sst_size_bytes,
            max_size,
        );

        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"v2".to_vec()));
        }
    }

    /// # Scenario
    /// A range tombstone must not suppress Puts with a higher LSN.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer.
    ///
    /// # Actions
    /// 1. `delete_range("key_0000", "key_0100")`, flush (lower LSN).
    /// 2. Write keys 0..20 with value `"new"`, flush (higher LSN).
    /// 3. `major_compact()`.
    ///
    /// # Expected behavior
    /// - Exactly 1 SSTable.
    /// - All 20 keys return `"new"` (newer Puts survive the older range tombstone).
    #[test]
    fn major_compact_range_tombstone_doesnt_suppress_newer_put() {
        let dir = fresh_dir("newer_put_survives");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        engine
            .delete_range(b"key_0000".to_vec(), b"key_0100".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"new".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap().sstables_count;
        assert!(before >= 2, "need at least 2 SSTables, got {before}");

        engine.major_compact().unwrap();

        let after = engine.stats().unwrap().sstables_count;
        assert_eq!(
            after, 1,
            "should have 1 SSTable after major compaction (was {before})"
        );

        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(b"new".to_vec()),
                "key_{i:04} should survive — put LSN > range tombstone LSN"
            );
        }
    }

    /// # Scenario
    /// When every key has been deleted, major compaction produces 0 SSTables.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer.
    ///
    /// # Actions
    /// 1. Write 20 keys, flush.
    /// 2. Delete all 20 keys individually, write padding key, flush.
    /// 3. Record `before` stats.
    /// 4. `major_compact()`.
    /// 5. Record `after` stats.
    ///
    /// # Expected behavior
    /// - At most 1 SSTable remains (pad key may survive, or all SSTables empty).
    /// - Total SST size drastically reduced (\u2265 80 %).
    #[test]
    fn major_compact_everything_deleted() {
        let dir = fresh_dir("all_deleted");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        for i in 0..20 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }
        engine.put(b"zzz_pad".to_vec(), b"x".to_vec()).unwrap();
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();
        assert!(
            before.sstables_count >= 2,
            "need at least 2 SSTables, got {}",
            before.sstables_count
        );

        engine.major_compact().unwrap();

        let after = engine.stats().unwrap();
        // 20 of 21 entries eliminated \u2014 at most 1 SSTable (pad key) survives.
        assert!(
            after.sstables_count <= 1,
            "should have at most 1 SSTable after major compaction, got {} (was {})",
            after.sstables_count,
            before.sstables_count,
        );
        // At least 80 % size reduction.
        let max_size = (before.total_sst_size_bytes as f64 * 0.20) as u64;
        assert!(
            after.total_sst_size_bytes <= max_size,
            "total SST size should decrease by >= 80 %: before={} B, after={} B, max allowed={} B",
            before.total_sst_size_bytes,
            after.total_sst_size_bytes,
            max_size,
        );
    }

    /// # Scenario
    /// Major compaction result is durable across engine close/reopen.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer.
    ///
    /// # Actions
    /// 1. Write 60 keys, flush.
    /// 2. Delete keys 40..60, flush.
    /// 3. `major_compact()`, close engine.
    /// 4. Reopen engine, read all 60 keys.
    ///
    /// # Expected behavior
    /// - Keys 0..40 return their values.
    /// - Keys 40..60 return `None`.
    #[test]
    fn major_compact_survives_reopen() {
        let dir = fresh_dir("reopen");

        {
            let engine = Engine::open(&dir, compaction_config()).unwrap();

            for i in 0..60 {
                let key = format!("key_{:04}", i).into_bytes();
                engine.put(key, b"val".to_vec()).unwrap();
            }
            engine.flush_all_frozen().unwrap();

            for i in 40..60 {
                let key = format!("key_{:04}", i).into_bytes();
                engine.delete(key).unwrap();
            }
            engine.flush_all_frozen().unwrap();

            engine.major_compact().unwrap();
        }

        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..40 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
        for i in 40..60 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), None);
        }
    }

    /// # Scenario
    /// After major compaction, a full-range scan returns only live keys.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer.
    ///
    /// # Actions
    /// 1. Write 30 keys, flush.
    /// 2. `delete_range("key_0010", "key_0020")`, flush.
    /// 3. Record `before` stats.
    /// 4. `major_compact()`.
    /// 5. Record `after` stats.
    /// 6. Scan full range.
    ///
    /// # Expected behavior
    /// - Exactly 1 SSTable.
    /// - Scan returns exactly 20 live keys (0..10 + 20..30).
    #[test]
    fn major_compact_scan_after() {
        let dir = fresh_dir("scan");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..30 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        engine
            .delete_range(b"key_0010".to_vec(), b"key_0020".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();
        assert!(
            before.sstables_count >= 2,
            "need at least 2 SSTables, got {}",
            before.sstables_count
        );

        engine.major_compact().unwrap();

        let after = engine.stats().unwrap();
        assert_eq!(
            after.sstables_count, 1,
            "should have 1 SSTable after major compaction (was {})",
            before.sstables_count
        );
        // 10 of 30 keys range-deleted — expect at least 20 % size reduction.
        let max_size = (before.total_sst_size_bytes as f64 * 0.80) as u64;
        assert!(
            after.total_sst_size_bytes <= max_size,
            "total SST size should decrease by ≥ 20 %: before={} B, after={} B, max allowed={} B",
            before.total_sst_size_bytes,
            after.total_sst_size_bytes,
            max_size,
        );

        let results: Vec<_> = engine.scan(b"key_0000", b"key_9999").unwrap().collect();
        assert_eq!(
            results.len(),
            20,
            "expected 20 live keys after major compact"
        );
    }

    /// # Scenario
    /// Major compaction correctly handles a mix of point tombstones and
    /// range tombstones in the same merge, dropping both.
    ///
    /// # Starting environment
    /// Empty engine, 256 B write buffer.
    ///
    /// # Actions
    /// 1. Write keys 0..50, flush.
    /// 2. Point-delete keys 0..10, flush.
    /// 3. `delete_range("key_0030", "key_0040")`, flush.
    /// 4. Record `before` stats.
    /// 5. `major_compact()`.
    /// 6. Record `after` stats.
    ///
    /// # Expected behavior
    /// - Exactly 1 SSTable.
    /// - Total SST size decreases (dead data + both tombstone types dropped).
    /// - Keys 0..10 return `None` (point-deleted).
    /// - Keys 10..30 return values (live).
    /// - Keys 30..40 return `None` (range-deleted).
    /// - Keys 40..50 return values (live).
    /// - Scan returns exactly 30 live keys.
    #[test]
    fn major_compact_mixed_point_and_range_tombstones() {
        let dir = fresh_dir("mixed_tombstones");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Point-delete keys 0..10
        for i in 0..10 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Range-delete keys 30..40
        engine
            .delete_range(b"key_0030".to_vec(), b"key_0040".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();
        assert!(
            before.sstables_count >= 2,
            "need at least 2 SSTables, got {}",
            before.sstables_count
        );

        engine.major_compact().unwrap();

        let after = engine.stats().unwrap();
        assert_eq!(
            after.sstables_count, 1,
            "should have 1 SSTable (was {})",
            before.sstables_count
        );
        // 20 of 50 keys deleted (10 point + 10 range) — expect at least 25 % reduction.
        let max_size = (before.total_sst_size_bytes as f64 * 0.75) as u64;
        assert!(
            after.total_sst_size_bytes <= max_size,
            "total SST size should decrease by ≥ 25 %: before={} B, after={} B, max allowed={} B",
            before.total_sst_size_bytes,
            after.total_sst_size_bytes,
            max_size,
        );

        // Point-deleted
        for i in 0..10 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{i:04} should be point-deleted"
            );
        }
        // Live
        for i in 10..30 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
        // Range-deleted
        for i in 30..40 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{i:04} should be range-deleted"
            );
        }
        // Live
        for i in 40..50 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }

        let scan_results: Vec<_> = engine.scan(b"key_0000", b"key_9999").unwrap().collect();
        assert_eq!(
            scan_results.len(),
            30,
            "expected 30 live keys after mixed tombstone major compact"
        );
    }
}
