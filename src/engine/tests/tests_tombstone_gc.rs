//! Tombstone compaction flag coverage tests.
//!
//! These tests exercise two configuration flags that are normally disabled
//! in the standard test configs:
//! - `tombstone_bloom_fallback = true` — resolves bloom false-positives
//!   with actual `get()` lookups.
//! - `tombstone_range_drop = true` — enables range tombstone garbage
//!   collection.

#[cfg(test)]
mod tests {
    use crate::engine::tests::helpers::init_tracing;
    use crate::engine::{Engine, EngineConfig};
    use tempfile::TempDir;

    /// Config that enables bloom fallback and range tombstone dropping,
    /// with immediate eligibility (interval = 0, low ratio threshold).
    fn tombstone_gc_config() -> EngineConfig {
        EngineConfig {
            write_buffer_size: 512,
            min_sstable_size: 64,
            tombstone_ratio_threshold: 0.1,
            tombstone_compaction_interval: 0,
            tombstone_bloom_fallback: true,
            tombstone_range_drop: true,
            min_threshold: 4,
            max_threshold: 32,
            ..EngineConfig::default()
        }
    }

    /// Config with bloom fallback disabled but range drop enabled.
    fn range_drop_only_config() -> EngineConfig {
        EngineConfig {
            write_buffer_size: 512,
            min_sstable_size: 64,
            tombstone_ratio_threshold: 0.1,
            tombstone_compaction_interval: 0,
            tombstone_bloom_fallback: false,
            tombstone_range_drop: true,
            min_threshold: 4,
            max_threshold: 32,
            ..EngineConfig::default()
        }
    }

    /// Config with bloom fallback enabled but range drop disabled.
    fn bloom_fallback_only_config() -> EngineConfig {
        EngineConfig {
            write_buffer_size: 512,
            min_sstable_size: 64,
            tombstone_ratio_threshold: 0.1,
            tombstone_compaction_interval: 0,
            tombstone_bloom_fallback: true,
            tombstone_range_drop: false,
            min_threshold: 4,
            max_threshold: 32,
            ..EngineConfig::default()
        }
    }

    // ----------------------------------------------------------------
    // Bloom fallback path
    // ----------------------------------------------------------------

    /// Write some data, flush to SSTable, then delete the same keys,
    /// flush again, and run tombstone compaction with bloom fallback ON.
    ///
    /// The bloom filter will say "maybe" for the deleted keys (they exist
    /// in the older SSTable), so the fallback path does an actual `get()`
    /// to confirm — the tombstones cannot be dropped because the older
    /// SSTable holds live data they suppress.
    #[test]
    fn tombstone_compaction_with_bloom_fallback() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), bloom_fallback_only_config()).unwrap();

        // Write initial data and flush to SSTable
        for i in 0..20 {
            let key = format!("bf_key_{:04}", i).into_bytes();
            let val = format!("bf_val_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Delete some keys and flush again — creates tombstones
        for i in 0..10 {
            let key = format!("bf_key_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let stats_before = engine.stats().unwrap();
        assert!(stats_before.sstables_count >= 2);

        // Run tombstone compaction — should exercise bloom fallback path
        let compacted = engine.tombstone_compact().unwrap();
        // Whether it compacted or not, the engine should still be consistent
        let _ = compacted;

        // Verify data integrity: deleted keys should still be gone,
        // remaining keys should still be readable
        for i in 0..10 {
            let key = format!("bf_key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), None);
        }
        for i in 10..20 {
            let key = format!("bf_key_{:04}", i).into_bytes();
            let expected = format!("bf_val_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(expected));
        }
    }

    // ----------------------------------------------------------------
    // Range tombstone drop path
    // ----------------------------------------------------------------

    /// Write data, flush, then delete a range, flush again, and run
    /// tombstone compaction with range_drop = true.
    #[test]
    fn tombstone_compaction_with_range_drop_covering_older() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), range_drop_only_config()).unwrap();

        // Write keys and flush to SSTable
        for i in 0..15 {
            let key = format!("rd_key_{:04}", i).into_bytes();
            let val = format!("rd_val_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Range delete that covers some of the flushed keys
        engine
            .delete_range(b"rd_key_0000".to_vec(), b"rd_key_0005".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        // Run tombstone compaction — exercises range tombstone drop logic
        let _ = engine.tombstone_compact().unwrap();

        // Verify: range-deleted keys are gone, others survive
        for i in 0..5 {
            let key = format!("rd_key_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), None);
        }
        for i in 5..15 {
            let key = format!("rd_key_{:04}", i).into_bytes();
            let expected = format!("rd_val_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(expected));
        }
    }

    /// Range tombstone that does NOT cover any older SSTable data.
    /// With range_drop = true, the tombstone should be droppable.
    #[test]
    fn tombstone_compaction_range_drop_no_overlap() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), tombstone_gc_config()).unwrap();

        // Write some data and flush
        for i in 0..10 {
            let key = format!("no_key_{:04}", i).into_bytes();
            let val = format!("no_val_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Range delete on a range that doesn't overlap the flushed data
        engine
            .delete_range(b"zzz_start".to_vec(), b"zzz_zzend".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        // Tombstone compaction: the range tombstone covers no live data
        // in older SSTables, so it should be droppable
        let _ = engine.tombstone_compact();

        // All original keys should still be retrievable
        for i in 0..10 {
            let key = format!("no_key_{:04}", i).into_bytes();
            let expected = format!("no_val_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(expected));
        }
    }

    // ----------------------------------------------------------------
    // Both flags enabled together
    // ----------------------------------------------------------------

    /// Tests tombstone compaction with both bloom_fallback and range_drop
    /// enabled, exercising the full GC pipeline.
    #[test]
    fn tombstone_compaction_both_flags() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), tombstone_gc_config()).unwrap();

        // Write initial data
        for i in 0..20 {
            let key = format!("both_{:04}", i).into_bytes();
            let val = format!("bval_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Point deletes + range delete
        for i in 0..5 {
            let key = format!("both_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }
        engine
            .delete_range(b"both_0010".to_vec(), b"both_0015".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        // Run tombstone compaction
        let _ = engine.tombstone_compact();

        // Verify correctness
        for i in 0..5 {
            let key = format!("both_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), None, "key {i} should be deleted");
        }
        for i in 5..10 {
            let key = format!("both_{:04}", i).into_bytes();
            assert!(engine.get(key).unwrap().is_some(), "key {i} should exist");
        }
        for i in 10..15 {
            let key = format!("both_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key {i} should be range-deleted"
            );
        }
        for i in 15..20 {
            let key = format!("both_{:04}", i).into_bytes();
            assert!(engine.get(key).unwrap().is_some(), "key {i} should exist");
        }
    }
}
