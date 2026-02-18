//! Minor compaction tests.

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
            write_buffer_size: 256, // tiny — forces many SSTables
            compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 50, // everything goes into regular buckets
            min_threshold: 2,     // trigger compaction with just 2 SSTables
            max_threshold: 32,
            tombstone_ratio_threshold: 0.2,
            tombstone_compaction_interval: 0,
            tombstone_bloom_fallback: false,
            tombstone_range_drop: false,
            thread_pool_size: 2,
        }
    }

    fn fresh_dir(name: &str) -> String {
        let path = format!("/tmp/aeternusdb_test_compaction_minor_{}", name);
        let _ = fs::remove_dir_all(&path);
        path
    }

    /// # Scenario
    /// Minor compaction merges similarly-sized SSTables into one,
    /// deduplicating entries and reducing SSTable count.
    ///
    /// # Starting environment
    /// Empty engine with 256 B write buffer, `min_threshold = 2`.
    ///
    /// # Actions
    /// 1. Write 100 keys (`key_0000`..`key_0099`, 8 B key + 8 B value = 16 B each).
    /// 2. `flush_all_frozen()`.
    /// 3. Record `before` SSTable count and total size.
    /// 4. `minor_compact()`.
    /// 5. Record `after` SSTable count and total size.
    ///
    /// # Expected behavior
    /// - `minor_compact` returns `true`.
    /// - SSTable count decreases (e.g. from ~7 to fewer).
    /// - Total SSTable size decreases (merged output has no per-SSTable overhead duplication).
    /// - All 100 keys remain readable with correct values.
    #[test]
    fn minor_compact_merges_sstables() {
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
        let before_size = before.total_sst_size_bytes;

        let compacted = engine.minor_compact().unwrap();
        assert!(compacted, "minor_compact should have run");

        let after = engine.stats().unwrap();
        assert!(
            after.sstables_count < before.sstables_count,
            "SSTable count should decrease: before={}, after={}",
            before.sstables_count,
            after.sstables_count,
        );
        // A single minor compaction round on a small bucket should reduce
        // the count by at least 1 (merge ≥ 2 into 1).
        assert!(
            after.sstables_count < before.sstables_count,
            "minor compaction should reduce SSTable count by at least 1: {} → {}",
            before.sstables_count,
            after.sstables_count,
        );
        assert!(
            after.total_sst_size_bytes <= before_size,
            "total SST size should not increase: before={before_size} B, after={} B",
            after.total_sst_size_bytes,
        );

        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            let expected = format!("val_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(expected));
        }
    }

    /// # Scenario
    /// Minor compaction is a no-op when no bucket meets `min_threshold`.
    ///
    /// # Starting environment
    /// Empty engine with 256 B write buffer, `min_threshold = 2`.
    ///
    /// # Actions
    /// 1. Write 5 small keys (fit in ≤ 1 SSTable).
    /// 2. `flush_all_frozen()`.
    /// 3. `minor_compact()`.
    ///
    /// # Expected behavior
    /// - Returns `false` — fewer than 2 SSTables in any bucket.
    /// - SSTable count and total size unchanged.
    #[test]
    fn minor_compact_returns_false_when_nothing_to_do() {
        let dir = fresh_dir("noop");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..5 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();
        if before.sstables_count < 2 {
            let compacted = engine.minor_compact().unwrap();
            assert!(
                !compacted,
                "should not compact with fewer than min_threshold SSTables"
            );

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
    }

    /// # Scenario
    /// Minor compaction merges SSTables but preserves point tombstones
    /// that may still be needed to shadow data in unmerged SSTables.
    ///
    /// # Starting environment
    /// Empty engine with 256 B write buffer, `min_threshold = 2`.
    ///
    /// # Actions
    /// 1. Write 50 keys (`key_0000`..`key_0049`, 8 B key + 3 B value).
    /// 2. `flush_all_frozen()`.
    /// 3. Delete keys `key_0000`..`key_0024`.
    /// 4. `flush_all_frozen()`.
    /// 5. Record `before` stats.
    /// 6. `minor_compact()`.
    /// 7. Record `after` stats.
    ///
    /// # Expected behavior
    /// - SSTable count may decrease but tombstones are preserved.
    /// - Deleted keys return `None`; live keys return their values.
    #[test]
    fn minor_compact_preserves_tombstones() {
        let dir = fresh_dir("tombstones");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        for i in 0..25 {
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

        engine.minor_compact().unwrap();

        let after = engine.stats().unwrap();
        assert!(
            after.sstables_count < before.sstables_count,
            "SSTable count should decrease after tombstone merge: before={}, after={}",
            before.sstables_count,
            after.sstables_count,
        );

        for i in 0..25 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{i:04} should be deleted"
            );
        }
        for i in 25..50 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
    }

    /// # Scenario
    /// Minor compaction preserves range tombstones across a merge so
    /// that the deleted range still shadows older data.
    ///
    /// # Starting environment
    /// Empty engine with 256 B write buffer, `min_threshold = 2`.
    ///
    /// # Actions
    /// 1. Write 50 keys, flush.
    /// 2. `delete_range("key_0010", "key_0030")`, flush.
    /// 3. Record `before` stats.
    /// 4. `minor_compact()`.
    /// 5. Record `after` stats.
    ///
    /// # Expected behavior
    /// - SSTable count does not increase.
    /// - Keys 10..30 return `None`; other keys are intact.
    #[test]
    fn minor_compact_preserves_range_tombstones() {
        let dir = fresh_dir("range_tombstones");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        engine
            .delete_range(b"key_0010".to_vec(), b"key_0030".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();

        engine.minor_compact().unwrap();

        let after = engine.stats().unwrap();
        assert!(
            after.sstables_count < before.sstables_count,
            "SSTable count should decrease: before={}, after={}",
            before.sstables_count,
            after.sstables_count,
        );

        for i in 10..30 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{i:04} should be range-deleted"
            );
        }
        for i in 0..10 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
        for i in 30..50 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
    }

    /// # Scenario
    /// Minor compaction deduplicates overwritten keys, keeping only the
    /// newest version and reducing total on-disk size.
    ///
    /// # Starting environment
    /// Empty engine with 256 B write buffer, `min_threshold = 2`.
    ///
    /// # Actions
    /// 1. Write 30 keys with value `"v1"` (8 B key + 2 B value), flush.
    /// 2. Overwrite same 30 keys with `"v2"`, flush.
    /// 3. Record `before` stats (should have duplicate versions on disk).
    /// 4. `minor_compact()`.
    /// 5. Record `after` stats.
    ///
    /// # Expected behavior
    /// - SSTable count does not increase.
    /// - Total SST size decreases (duplicates eliminated).
    /// - All 30 keys return `"v2"`.
    #[test]
    fn minor_compact_deduplicates_versions() {
        let dir = fresh_dir("dedup");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..30 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"v1".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        for i in 0..30 {
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

        engine.minor_compact().unwrap();

        let after = engine.stats().unwrap();
        assert!(
            after.sstables_count < before.sstables_count,
            "SSTable count should decrease: before={}, after={}",
            before.sstables_count,
            after.sstables_count,
        );
        // Merging two copies of the same keys should eliminate ~50 % of data.
        let max_size = (before.total_sst_size_bytes as f64 * 0.75) as u64;
        assert!(
            after.total_sst_size_bytes <= max_size,
            "total SST size should decrease by ≥ 25 % (duplicates eliminated): before={} B, after={} B, max allowed={} B",
            before.total_sst_size_bytes,
            after.total_sst_size_bytes,
            max_size,
        );

        for i in 0..30 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"v2".to_vec()));
        }
    }

    /// # Scenario
    /// Minor compaction result is durable across engine close/reopen.
    ///
    /// # Starting environment
    /// Empty engine with 256 B write buffer, `min_threshold = 2`.
    ///
    /// # Actions
    /// 1. Write 80 keys, flush, `minor_compact()`.
    /// 2. Close engine (drop).
    /// 3. Reopen engine.
    /// 4. Read all 80 keys.
    ///
    /// # Expected behavior
    /// All 80 keys still readable with correct values after reopen.
    #[test]
    fn minor_compact_survives_reopen() {
        let dir = fresh_dir("reopen");

        {
            let engine = Engine::open(&dir, compaction_config()).unwrap();
            for i in 0..80 {
                let key = format!("key_{:04}", i).into_bytes();
                engine.put(key, b"val".to_vec()).unwrap();
            }
            engine.flush_all_frozen().unwrap();
            engine.minor_compact().unwrap();
        }

        let engine = Engine::open(&dir, compaction_config()).unwrap();
        for i in 0..80 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
    }

    /// # Scenario
    /// Running minor compaction in a loop converges (no infinite loop)
    /// and reduces SSTable count monotonically.
    ///
    /// # Starting environment
    /// Empty engine with 256 B write buffer, `min_threshold = 2`.
    ///
    /// # Actions
    /// 1. Write 200 keys (produces many small SSTables), flush.
    /// 2. Record `before` stats.
    /// 3. Loop: `minor_compact()` until it returns `false`.
    /// 4. Record `after` stats.
    ///
    /// # Expected behavior
    /// - Loop terminates in < 20 rounds.
    /// - SSTable count does not increase.
    /// - Total SST size does not increase.
    /// - All 200 keys readable.
    #[test]
    fn minor_compact_multiple_rounds() {
        let dir = fresh_dir("multi_round");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..200 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"val".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();

        let mut rounds = 0;
        while engine.minor_compact().unwrap() {
            rounds += 1;
            assert!(rounds < 20, "infinite compaction loop?");
        }

        let after = engine.stats().unwrap();
        // Multi-round minor compaction on 200 keys (many small SSTables)
        // should reduce the count significantly — at least halve it.
        assert!(
            after.sstables_count <= before.sstables_count / 2,
            "multi-round minor compaction should at least halve SSTable count: {} → {} (expected ≤ {})",
            before.sstables_count,
            after.sstables_count,
            before.sstables_count / 2,
        );
        assert!(
            after.total_sst_size_bytes <= before.total_sst_size_bytes,
            "total SST size should not increase: before={} B, after={} B",
            before.total_sst_size_bytes,
            after.total_sst_size_bytes,
        );

        for i in 0..200 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"val".to_vec()));
        }
    }

    /// # Scenario
    /// After minor compaction, a full-range scan returns exactly the same
    /// set of live keys as before compaction.
    ///
    /// # Starting environment
    /// Empty engine with 256 B write buffer, `min_threshold = 2`.
    ///
    /// # Actions
    /// 1. Write 60 keys, flush.
    /// 2. Scan full range, collect `before` result set.
    /// 3. `minor_compact()`.
    /// 4. Scan full range again, collect `after` result set.
    ///
    /// # Expected behavior
    /// Both scans return identical key-value pairs in the same order.
    #[test]
    fn minor_compact_scan_correctness() {
        let dir = fresh_dir("scan");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..60 {
            let key = format!("key_{:04}", i).into_bytes();
            let val = format!("val_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before_entries: Vec<_> = engine.scan(b"key_0000", b"key_9999").unwrap().collect();
        assert_eq!(before_entries.len(), 60);

        engine.minor_compact().unwrap();

        let after_entries: Vec<_> = engine.scan(b"key_0000", b"key_9999").unwrap().collect();
        assert_eq!(
            after_entries, before_entries,
            "scan results must be identical after minor compaction"
        );
    }

    /// # Scenario
    /// Minor compaction correctly handles interleaved overwrites and deletes
    /// across multiple SSTables.
    ///
    /// # Starting environment
    /// Empty engine with 256 B write buffer, `min_threshold = 2`.
    ///
    /// # Actions
    /// 1. Write keys 0..40 with value `"v1"`, flush.
    /// 2. Overwrite keys 10..20 with `"v2"`, flush.
    /// 3. Delete keys 30..35, flush.
    /// 4. Record `before` stats.
    /// 5. `minor_compact()`.
    /// 6. Record `after` stats.
    ///
    /// # Expected behavior
    /// - SSTable count does not increase.
    /// - Keys 0..10 return `"v1"`, keys 10..20 return `"v2"`, keys 20..30
    ///   return `"v1"`, keys 30..35 return `None`, keys 35..40 return `"v1"`.
    #[test]
    fn minor_compact_mixed_overwrites_and_deletes() {
        let dir = fresh_dir("mixed");
        let engine = Engine::open(&dir, compaction_config()).unwrap();

        for i in 0..40 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"v1".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        for i in 10..20 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.put(key, b"v2".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        for i in 30..35 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let before = engine.stats().unwrap();

        engine.minor_compact().unwrap();

        let after = engine.stats().unwrap();
        assert!(
            after.sstables_count < before.sstables_count,
            "SSTable count should decrease: before={}, after={}",
            before.sstables_count,
            after.sstables_count,
        );

        for i in 0..10 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"v1".to_vec()));
        }
        for i in 10..20 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"v2".to_vec()));
        }
        for i in 20..30 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"v1".to_vec()));
        }
        for i in 30..35 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{i:04} should be deleted"
            );
        }
        for i in 35..40 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(b"v1".to_vec()));
        }
    }
}
