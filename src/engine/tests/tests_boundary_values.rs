//! Boundary-value and large-record tests.
//!
//! These tests exercise values near size limits: large values that push
//! against the `write_buffer_size` and WAL `max_record_size`, values at
//! exact buffer boundaries, and the full pipeline (write → WAL → flush →
//! SSTable → recovery) with large payloads.
//!
//! ## See also
//! - [`tests_put_get`] — standard put/get including `memtable__large_value` (1KB)
//! - [`tests_edge_cases`] — `very_large_key_recovery` (10KB key)
//! - [`wal::tests::tests_edge_cases`] — `max_record_size_rejects_oversized_record`

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::tests::helpers::*;
    use crate::engine::{Engine, EngineConfig};
    use tempfile::TempDir;

    /// Config with 4KB write buffer — large enough for one big value.
    fn large_value_config() -> EngineConfig {
        init_tracing();
        EngineConfig {
            write_buffer_size: 64 * 1024, // 64KB
            compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 1024,
            min_threshold: 4,
            max_threshold: 32,
            tombstone_ratio_threshold: 0.2,
            tombstone_compaction_interval: 3600,
            tombstone_bloom_fallback: false,
            tombstone_range_drop: false,
            thread_pool_size: 2,
        }
    }

    // ================================================================
    // 1. Large value through full pipeline
    // ================================================================

    /// # Scenario
    /// Write a 50KB value, flush to SSTable, reopen, verify.
    ///
    /// # Expected behavior
    /// The value survives the full pipeline: WAL → memtable → flush → SSTable → recovery.
    #[test]
    fn memtable_sstable__large_50kb_value_round_trip() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();
        let big_value = vec![0xABu8; 50 * 1024]; // 50KB

        let engine = Engine::open(path, large_value_config()).unwrap();
        engine.put(b"big_key".to_vec(), big_value.clone()).unwrap();
        engine.flush_all_frozen().unwrap();
        engine.close().unwrap();

        let engine = Engine::open(path, large_value_config()).unwrap();
        let retrieved = engine.get(b"big_key".to_vec()).unwrap().unwrap();
        assert_eq!(retrieved.len(), big_value.len());
        assert_eq!(retrieved, big_value);
    }

    // ================================================================
    // 2. Large value survives crash
    // ================================================================

    /// # Scenario
    /// Write a 50KB value, drop (crash), reopen, verify.
    ///
    /// # Expected behavior
    /// The value is recovered from the WAL after crash.
    #[test]
    fn memtable__large_value_survives_crash() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();
        let big_value = vec![0xCDu8; 50 * 1024];

        {
            let engine = Engine::open(path, large_value_config()).unwrap();
            engine
                .put(b"crash_key".to_vec(), big_value.clone())
                .unwrap();
            // Drop — crash.
        }

        let engine = Engine::open(path, large_value_config()).unwrap();
        let retrieved = engine.get(b"crash_key".to_vec()).unwrap().unwrap();
        assert_eq!(retrieved, big_value);
    }

    // ================================================================
    // 3. Value exactly at write buffer boundary
    // ================================================================

    /// # Scenario
    /// Write two values whose combined in-memory size exceeds `write_buffer_size`,
    /// causing the memtable to freeze on the second put.
    ///
    /// # Expected behavior
    /// The second put triggers a freeze (returns `true`), and the
    /// frozen memtable can be flushed to SSTable. Both values survive.
    #[test]
    fn memtable_sstable__value_triggering_exact_freeze() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // default_config: 4096 bytes buffer.
        let engine = Engine::open(path, default_config()).unwrap();

        // First put: ~3000 bytes value fits in the 4KB buffer.
        let val_a = vec![0x41u8; 3000];
        let freeze_a = engine.put(b"key_a".to_vec(), val_a.clone()).unwrap();
        assert!(!freeze_a, "First put should not trigger freeze");

        // Second put: combined size > 4KB → freeze triggered.
        let val_b = vec![0x42u8; 3000];
        let freeze_b = engine.put(b"key_b".to_vec(), val_b.clone()).unwrap();
        assert!(freeze_b, "Second put should trigger freeze");

        engine.flush_all_frozen().unwrap();

        let retrieved_a = engine.get(b"key_a".to_vec()).unwrap().unwrap();
        let retrieved_b = engine.get(b"key_b".to_vec()).unwrap().unwrap();
        assert_eq!(retrieved_a, val_a);
        assert_eq!(retrieved_b, val_b);
    }

    // ================================================================
    // 4. Multiple large values across SSTables
    // ================================================================

    /// # Scenario
    /// Write multiple values each close to the write buffer size,
    /// forcing multiple freezes and SSTables.
    ///
    /// # Expected behavior
    /// All values survive and are retrievable via `get()` and `scan()`.
    #[test]
    fn memtable_sstable__multiple_large_values() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, default_config()).unwrap();

        let mut expected = Vec::new();
        for i in 0..10u32 {
            let key = format!("large_{i:02}").into_bytes();
            let val = vec![i as u8; 2000]; // Each ~2KB in a 4KB buffer → freeze every ~2 keys
            engine.put(key.clone(), val.clone()).unwrap();
            expected.push((key, val));
        }

        engine.flush_all_frozen().unwrap();

        for (key, val) in &expected {
            let retrieved = engine.get(key.clone()).unwrap().unwrap();
            assert_eq!(&retrieved, val);
        }

        // Scan should return all sorted.
        let scan_results = collect_scan(&engine, b"large_", b"large_\xff");
        assert_eq!(scan_results.len(), 10);
    }

    // ================================================================
    // 5. Large value overwrite
    // ================================================================

    /// # Scenario
    /// Write a large value, then overwrite with a different large value,
    /// flush, and verify only the new value is returned.
    ///
    /// # Expected behavior
    /// `get()` returns the overwritten value, not the original.
    #[test]
    fn memtable_sstable__large_value_overwrite() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, default_config()).unwrap();

        let original = vec![0xAAu8; 2000];
        let updated = vec![0xBBu8; 3000];

        engine.put(b"k".to_vec(), original).unwrap();
        engine.flush_all_frozen().unwrap();

        engine.put(b"k".to_vec(), updated.clone()).unwrap();
        engine.flush_all_frozen().unwrap();

        let retrieved = engine.get(b"k".to_vec()).unwrap().unwrap();
        assert_eq!(retrieved, updated);
    }

    // ================================================================
    // 6. Large value survives compaction
    // ================================================================

    /// # Scenario
    /// Write many large values to create multiple SSTables, compact them,
    /// and verify all data intact.
    ///
    /// # Expected behavior
    /// Major compaction merges all SSTables. All keys remain accessible.
    #[test]
    fn memtable_sstable__large_values_survive_compaction() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, multi_sstable_config()).unwrap();

        for i in 0..20u32 {
            let key = format!("cmp_{i:02}").into_bytes();
            let val = vec![i as u8; 150];
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let stats = engine.stats().unwrap();
        assert!(stats.sstables_count >= 2, "Need multiple SSTables");

        engine.major_compact().unwrap();

        for i in 0..20u32 {
            let key = format!("cmp_{i:02}").into_bytes();
            let val = engine.get(key).unwrap().unwrap();
            assert_eq!(val, vec![i as u8; 150]);
        }
    }

    // ================================================================
    // 7. Binary keys with all byte values
    // ================================================================

    /// # Scenario
    /// Write keys containing every byte value (0x00..0xFF) to verify
    /// the engine handles non-UTF8 binary keys correctly through
    /// the full pipeline.
    ///
    /// # Expected behavior
    /// All 256 keys survive flush → close → reopen.
    #[test]
    fn memtable_sstable__all_byte_value_keys() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, small_buffer_config()).unwrap();

        for byte in 0..=255u8 {
            let key = vec![b'K', byte];
            let val = vec![b'V', byte];
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();
        engine.close().unwrap();

        let engine = Engine::open(path, small_buffer_config()).unwrap();
        for byte in 0..=255u8 {
            let key = vec![b'K', byte];
            let val = engine.get(key).unwrap().unwrap();
            assert_eq!(val, vec![b'V', byte], "Byte 0x{byte:02X} key failed");
        }
    }
}
