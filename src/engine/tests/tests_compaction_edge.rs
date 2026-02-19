//! Compaction edge-case and idempotency tests.
//!
//! These tests verify compaction behaviors that are not covered by the
//! main compaction test suite: idempotent re-runs, single-entry SSTables,
//! compaction with minimal (1-byte) values, overlapping range tombstones
//! across SSTables, and tombstone compaction threshold behavior.
//!
//! ## See also
//! - [`compaction::stcs::tests`] — core compaction correctness
//! - [`tests_crash_compaction`] — crash during compaction
//! - [`tests_file_cleanup`] — file-count verification after compaction

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::tests::helpers::*;
    use crate::engine::{Engine, EngineConfig};
    use tempfile::TempDir;

    fn compaction_config() -> EngineConfig {
        init_tracing();
        EngineConfig {
            write_buffer_size: 128, // Very small — each key gets its own SSTable.
            compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 64, // Very low — all SSTables qualify as "small bucket".
            min_threshold: 2,     // Compact with just 2 SSTables.
            max_threshold: 32,
            tombstone_ratio_threshold: 0.2,
            tombstone_compaction_interval: 0, // No age requirement.
            tombstone_bloom_fallback: true,
            tombstone_range_drop: true,
            thread_pool_size: 2,
        }
    }

    // ================================================================
    // 1. Idempotent compaction re-run
    // ================================================================

    /// # Scenario
    /// Run minor compaction twice in a row. The second run should find
    /// nothing to do and return `Ok(false)` (or compact remaining if any).
    ///
    /// # Expected behavior
    /// No panic, no data loss. All keys remain accessible.
    #[test]
    fn memtable_sstable__minor_compact_idempotent() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, compaction_config()).unwrap();
        for i in 0..30u32 {
            engine
                .put(
                    format!("key_{i:04}").into_bytes(),
                    format!("val_{i:04}_padding").into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // First minor compaction.
        let did_compact = engine.minor_compact().unwrap();
        assert!(did_compact, "First minor compaction should find work");

        // Second minor compaction — should be a no-op or compact remaining.
        let _ = engine.minor_compact();

        // Verify data integrity.
        for i in 0..30u32 {
            let key = format!("key_{i:04}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{i:04} must be readable after double compaction"
            );
        }
    }

    // ================================================================
    // 2. Major compaction idempotent re-run
    // ================================================================

    /// # Scenario
    /// Run major compaction twice. The second run should see only 1
    /// SSTable and return `Ok(false)`.
    ///
    /// # Expected behavior
    /// Second `major_compact()` returns `false`. Data intact.
    #[test]
    fn memtable_sstable__major_compact_idempotent() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = engine_with_multi_sstables(path, 200, "key");

        let first = engine.major_compact().unwrap();
        assert!(first, "First major compaction should do work");

        let second = engine.major_compact().unwrap();
        assert!(
            !second,
            "Second major compaction should be no-op (1 SSTable)"
        );

        for i in 0..200u32 {
            let key = format!("key_{i:04}").into_bytes();
            assert!(engine.get(key).unwrap().is_some());
        }
    }

    // ================================================================
    // 3. Compaction with minimal 1-byte values
    // ================================================================

    /// # Scenario
    /// Write keys with 1-byte values, flush, compact. Very small SSTables
    /// test the edge case where record overhead dominates payload.
    ///
    /// # Expected behavior
    /// Compaction produces a valid merged SSTable. All keys accessible.
    #[test]
    fn memtable_sstable__compaction_with_1byte_values() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, compaction_config()).unwrap();
        for i in 0..20u32 {
            engine
                .put(format!("k{i:02}").into_bytes(), vec![i as u8])
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();

        engine.minor_compact().unwrap();

        for i in 0..20u32 {
            let val = engine
                .get(format!("k{i:02}").into_bytes())
                .unwrap()
                .unwrap();
            assert_eq!(val, vec![i as u8]);
        }
    }

    // ================================================================
    // 4. Compaction with overlapping range tombstones
    // ================================================================

    /// # Scenario
    /// Two SSTables contain overlapping range tombstones covering the
    /// same key space. After compaction, the merged range tombstone
    /// should correctly hide keys in the overlap region.
    ///
    /// # Expected behavior
    /// Keys in the overlapping range are deleted. Keys outside are intact.
    #[test]
    fn memtable_sstable__compaction_overlapping_range_tombstones() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, compaction_config()).unwrap();

        // Write some keys.
        for i in 0..20u32 {
            engine
                .put(
                    format!("key_{i:04}").into_bytes(),
                    format!("val_{i:04}").into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Range delete [key_0005, key_0012).
        engine
            .delete_range(b"key_0005".to_vec(), b"key_0012".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        // Overlapping range delete [key_0008, key_0018).
        engine
            .delete_range(b"key_0008".to_vec(), b"key_0018".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        // Compact.
        engine.major_compact().unwrap();

        // Keys 0-4: alive (outside both ranges).
        for i in 0..5u32 {
            assert!(
                engine
                    .get(format!("key_{i:04}").into_bytes())
                    .unwrap()
                    .is_some(),
                "key_{i:04} should survive (outside range)"
            );
        }

        // Keys 5-17: deleted (covered by one or both range tombstones).
        for i in 5..18u32 {
            assert!(
                engine
                    .get(format!("key_{i:04}").into_bytes())
                    .unwrap()
                    .is_none(),
                "key_{i:04} should be deleted (in overlapping range)"
            );
        }

        // Keys 18-19: alive (outside both ranges).
        for i in 18..20u32 {
            assert!(
                engine
                    .get(format!("key_{i:04}").into_bytes())
                    .unwrap()
                    .is_some(),
                "key_{i:04} should survive (outside range)"
            );
        }
    }

    // ================================================================
    // 5. Tombstone compaction with bloom fallback
    // ================================================================

    /// # Scenario
    /// Delete keys and trigger tombstone compaction with `tombstone_bloom_fallback`
    /// enabled. Tombstones for keys only in older SSTables should be dropped
    /// when the bloom filter confirms no false-positive references.
    ///
    /// # Expected behavior
    /// After tombstone compaction, the tombstones are removed (or pruned),
    /// but the deleted keys remain invisible.
    #[test]
    fn memtable_sstable__tombstone_compaction_with_bloom_fallback() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, compaction_config()).unwrap();

        // Write keys.
        for i in 0..20u32 {
            engine
                .put(
                    format!("key_{i:04}").into_bytes(),
                    format!("val_{i:04}").into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Delete half.
        for i in 0..10u32 {
            engine.delete(format!("key_{i:04}").into_bytes()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Run tombstone compaction.
        let _ = engine.tombstone_compact();

        // Verify: deleted keys stay deleted, alive keys stay alive.
        for i in 0..10u32 {
            assert!(
                engine
                    .get(format!("key_{i:04}").into_bytes())
                    .unwrap()
                    .is_none(),
                "key_{i:04} should remain deleted"
            );
        }
        for i in 10..20u32 {
            assert!(
                engine
                    .get(format!("key_{i:04}").into_bytes())
                    .unwrap()
                    .is_some(),
                "key_{i:04} should survive"
            );
        }
    }

    // ================================================================
    // 6. Tombstone compaction with range tombstone drop
    // ================================================================

    /// # Scenario
    /// Range-delete keys and trigger tombstone compaction with
    /// `tombstone_range_drop` enabled. Range tombstones should be
    /// dropped when older SSTables have no live keys in the range.
    ///
    /// # Expected behavior
    /// Range-deleted keys remain invisible. Alive keys unaffected.
    #[test]
    fn memtable_sstable__tombstone_compaction_range_drop() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, compaction_config()).unwrap();

        for i in 0..20u32 {
            engine
                .put(
                    format!("key_{i:04}").into_bytes(),
                    format!("val_{i:04}").into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();

        engine
            .delete_range(b"key_0005".to_vec(), b"key_0015".to_vec())
            .unwrap();
        engine.flush_all_frozen().unwrap();

        let _ = engine.tombstone_compact();

        // Verify: range-deleted keys still deleted.
        for i in 5..15u32 {
            assert!(
                engine
                    .get(format!("key_{i:04}").into_bytes())
                    .unwrap()
                    .is_none(),
                "key_{i:04} should remain range-deleted"
            );
        }

        // Alive keys.
        for i in (0..5).chain(15..20) {
            assert!(
                engine
                    .get(format!("key_{i:04}").into_bytes())
                    .unwrap()
                    .is_some(),
                "key_{i:04} should survive"
            );
        }
    }

    // ================================================================
    // 7. Minor compaction then major — full pipeline
    // ================================================================

    /// # Scenario
    /// Run minor compaction first, then major compaction on the result.
    /// Verifies that the two compaction levels compose correctly.
    ///
    /// # Expected behavior
    /// After both compactions, exactly 1 SSTable remains.
    /// All data intact.
    #[test]
    fn memtable_sstable__minor_then_major_compaction() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, compaction_config()).unwrap();

        for i in 0..40u32 {
            engine
                .put(
                    format!("key_{i:04}").into_bytes(),
                    format!("val_{i:04}_padding").into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Minor compact first.
        let _ = engine.minor_compact();

        // Major compact to merge everything.
        engine.major_compact().unwrap();

        let stats = engine.stats().unwrap();
        assert_eq!(
            stats.sstables_count, 1,
            "Should have exactly 1 SSTable after minor + major"
        );

        for i in 0..40u32 {
            let key = format!("key_{i:04}").into_bytes();
            assert!(engine.get(key).unwrap().is_some());
        }
    }
}
