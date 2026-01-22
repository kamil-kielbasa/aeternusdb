#[cfg(test)]
mod tests {
    use crate::engine::{Engine, EngineConfig};
    use tempfile::TempDir;

    fn create_test_config() -> EngineConfig {
        EngineConfig {
            write_buffer_size: 4096, // 4KB
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 1024,
            min_threshold: 4,
            max_threshold: 32,
            tombstone_threshold: 0.2,
            tombstone_compaction_interval: 3600,
            thread_pool_size: 2,
        }
    }

    /// 1. Test suites for basic put/get/delete operations only on memtable (no SSTables involved).

    #[test]
    fn test_put_and_get_single_key() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Put a key-value pair
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        engine
            .put(key.clone(), value.clone())
            .expect("Failed to put");

        // Assert no SSTables or frozen memtables created yet
        let stats = engine.stats().expect("Failed to get stats");
        assert_eq!(
            stats.sstables_count, 0,
            "Engine should not have SSTables yet"
        );
        assert_eq!(
            stats.frozen_count, 0,
            "Engine should not have frozen memtables yet"
        );

        // Get the value back
        let result = engine.get(key.clone()).expect("Failed to get");
        assert_eq!(result, Some(value.clone()));

        // Get a non-existent key
        let missing_key = b"missing".to_vec();
        let result = engine.get(missing_key).expect("Failed to get");
        assert_eq!(result, None);
    }

    #[test]
    fn test_put_and_get_multiple_keys() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Put multiple key-value pairs
        for i in 0..10 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        // Assert no SSTables or frozen memtables created yet
        let stats = engine.stats().expect("Failed to get stats");
        assert_eq!(
            stats.sstables_count, 0,
            "Engine should not have SSTables yet"
        );
        assert_eq!(
            stats.frozen_count, 0,
            "Engine should not have frozen memtables yet"
        );

        // Get all values back
        for i in 0..10 {
            let key = format!("key_{}", i).into_bytes();
            let expected_value = format!("value_{}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, Some(expected_value));
        }
    }

    #[test]
    fn test_put_overwrite() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        let key = b"key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        // Put initial value
        engine
            .put(key.clone(), value1.clone())
            .expect("Failed to put");
        let result = engine.get(key.clone()).expect("Failed to get");
        assert_eq!(result, Some(value1));

        // Overwrite with new value
        engine
            .put(key.clone(), value2.clone())
            .expect("Failed to put");
        let result = engine.get(key.clone()).expect("Failed to get");
        assert_eq!(result, Some(value2));

        // Assert no SSTables or frozen memtables created yet
        let stats = engine.stats().expect("Failed to get stats");
        assert_eq!(
            stats.sstables_count, 0,
            "Engine should not have SSTables yet"
        );
        assert_eq!(
            stats.frozen_count, 0,
            "Engine should not have frozen memtables yet"
        );
    }

    #[test]
    fn test_delete() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        let key = b"key".to_vec();
        let value = b"value".to_vec();

        // Put a value
        engine
            .put(key.clone(), value.clone())
            .expect("Failed to put");
        let result = engine.get(key.clone()).expect("Failed to get");
        assert_eq!(result, Some(value));

        // Delete the key
        engine.delete(key.clone()).expect("Failed to delete");
        let result = engine.get(key.clone()).expect("Failed to get");
        assert_eq!(result, None);

        // Assert no SSTables or frozen memtables created yet
        let stats = engine.stats().expect("Failed to get stats");
        assert_eq!(
            stats.sstables_count, 0,
            "Engine should not have SSTables yet"
        );
        assert_eq!(
            stats.frozen_count, 0,
            "Engine should not have frozen memtables yet"
        );
    }

    #[test]
    fn test_delete_then_reinsert() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        let key = b"key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        // Put, delete, then put again
        engine
            .put(key.clone(), value1.clone())
            .expect("Failed to put");
        engine.delete(key.clone()).expect("Failed to delete");
        engine
            .put(key.clone(), value2.clone())
            .expect("Failed to put");

        let result = engine.get(key).expect("Failed to get");
        assert_eq!(result, Some(value2));
    }

    #[test]
    fn test_multiple_deletes() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Insert multiple keys
        for i in 0..10 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        // Delete every other key
        for i in (0..10).step_by(2) {
            let key = format!("key_{}", i).into_bytes();
            engine.delete(key).expect("Failed to delete");
        }

        // Verify deleted keys return None and existing keys return values
        for i in 0..10 {
            let key = format!("key_{}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            if i % 2 == 0 {
                assert_eq!(result, None, "key_{} should be deleted", i);
            } else {
                let expected_value = format!("value_{}", i).into_bytes();
                assert_eq!(result, Some(expected_value), "key_{} should exist", i);
            }
        }
    }

    #[test]
    fn test_range_delete_basic() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Insert keys: key_00 to key_09
        for i in 0..10 {
            let key = format!("key_{:02}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        // Delete range from key_03 to key_07 (exclusive end)
        let start = b"key_03".to_vec();
        let end = b"key_07".to_vec();
        engine
            .delete_range(start, end)
            .expect("Failed to range delete");

        // Verify keys outside range still exist
        for i in 0..3 {
            let key = format!("key_{:02}", i).into_bytes();
            let expected_value = format!("value_{}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, Some(expected_value));
        }

        // Verify keys in range are deleted
        for i in 3..7 {
            let key = format!("key_{:02}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, None, "key_{:02} should be deleted", i);
        }

        // Verify keys after range still exist
        for i in 7..10 {
            let key = format!("key_{:02}", i).into_bytes();
            let expected_value = format!("value_{}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, Some(expected_value), "key_{:02} should exist", i);
        }
    }

    /// 2. Test suites for operations involving frozen memtable.

    #[test]
    fn test_get_from_frozen_memtable() {
        let temp = TempDir::new().unwrap();
        let mut config = create_test_config();
        // Set a very small write buffer to force freezing
        config.write_buffer_size = 128;
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Put enough data to trigger memtable freezing
        let key1 = b"key1".to_vec();
        let value1 = b"this_is_a_long_value_to_fill_buffer_1".to_vec();
        engine
            .put(key1.clone(), value1.clone())
            .expect("Failed to put");

        let key2 = b"key2".to_vec();
        let value2 = b"this_is_a_long_value_to_fill_buffer_2".to_vec();
        engine
            .put(key2.clone(), value2.clone())
            .expect("Failed to put");

        // Assert frozen memtables are flushed to SSTables
        let stats = engine.stats().expect("Failed to get stats");
        assert_eq!(
            stats.frozen_count, 1,
            "Frozen memtables should be flushed to SSTables"
        );
        assert_eq!(
            stats.sstables_count, 0,
            "Engine should have SSTables after flushing"
        );

        // Verify we can still read from SSTables
        let result = engine.get(key1.clone()).expect("Failed to get");
        assert_eq!(result, Some(value1));

        let result = engine.get(key2.clone()).expect("Failed to get");
        assert_eq!(result, Some(value2));
    }

    /// 3. Test suites for basic put/get/delete operations with SSTables involved.

    #[test]
    fn test_put_and_get_multiple_keys_with_sstable() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Put many key-value pairs that will trigger SSTable creation
        let num_keys = 200;
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_with_some_data_{:04}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        // Assert SSTables were created
        let stats = engine.stats().expect("Failed to get stats");
        assert!(
            stats.sstables_count > 0,
            "Engine should have SSTables after multiple puts"
        );

        // Get all values back from SSTables
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let expected_value = format!("value_with_some_data_{:04}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, Some(expected_value));
        }
    }

    #[test]
    fn test_put_overwrite_with_sstable() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Put many keys to create SSTables
        let num_keys = 150;
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("initial_value_{:04}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        let stats_after_first = engine.stats().expect("Failed to get stats");
        assert!(
            stats_after_first.sstables_count > 0,
            "First batch should create SSTables"
        );

        // Overwrite some keys with new values
        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("updated_value_{:04}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        // Verify overwritten keys have new values
        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            let expected_value = format!("updated_value_{:04}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, Some(expected_value));
        }

        // Verify non-overwritten keys still have original values
        for i in 50..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let expected_value = format!("initial_value_{:04}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, Some(expected_value));
        }
    }

    #[test]
    fn test_delete_with_sstable() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Put many keys to create SSTables
        let num_keys = 150;
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_{:04}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        let stats = engine.stats().expect("Failed to get stats");
        assert!(stats.sstables_count > 0, "Puts should create SSTables");

        // Verify a key exists
        let key_to_delete = b"key_0075".to_vec();
        let result = engine.get(key_to_delete.clone()).expect("Failed to get");
        assert!(result.is_some());

        // Delete the key
        engine
            .delete(key_to_delete.clone())
            .expect("Failed to delete");
        let result = engine.get(key_to_delete.clone()).expect("Failed to get");
        assert_eq!(result, None);
    }

    #[test]
    fn test_delete_then_reinsert_with_sstable() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Put many keys to create SSTables
        let num_keys = 150;
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("initial_value_{:04}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        let stats_after_put = engine.stats().expect("Failed to get stats");
        assert!(stats_after_put.sstables_count > 0);

        // Delete some keys
        for i in 40..60 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.delete(key).expect("Failed to delete");
        }

        // Reinsert with new values
        for i in 40..60 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("reinserted_value_{:04}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        // Verify reinserted keys have new values
        for i in 40..60 {
            let key = format!("key_{:04}", i).into_bytes();
            let expected_value = format!("reinserted_value_{:04}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, Some(expected_value));
        }
    }

    #[test]
    fn test_multiple_deletes_with_sstable() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Insert many keys to create SSTables
        let num_keys = 200;
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_{:04}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        let stats = engine.stats().expect("Failed to get stats");
        assert!(
            stats.sstables_count > 0,
            "Multiple puts should create SSTables"
        );

        // Delete every other key
        for i in (0..num_keys).step_by(2) {
            let key = format!("key_{:04}", i).into_bytes();
            engine.delete(key).expect("Failed to delete");
        }

        // Verify deleted keys return None and existing keys return values
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            if i % 2 == 0 {
                assert_eq!(result, None, "key_{:04} should be deleted", i);
            } else {
                let expected_value = format!("value_{:04}", i).into_bytes();
                assert_eq!(result, Some(expected_value), "key_{:04} should exist", i);
            }
        }
    }

    #[test]
    fn test_range_delete_with_sstable() {
        let temp = TempDir::new().unwrap();
        let config = create_test_config();
        let engine = Engine::open(temp.path(), config).expect("Failed to open engine");

        // Insert many keys to create SSTables
        let num_keys = 200;
        for i in 0..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_{:04}", i).into_bytes();
            engine.put(key, value).expect("Failed to put");
        }

        let stats = engine.stats().expect("Failed to get stats");
        assert!(
            stats.sstables_count > 0,
            "Multiple puts should create SSTables"
        );

        // Delete range from key_0050 to key_0150 (exclusive end)
        let start = b"key_0050".to_vec();
        let end = b"key_0150".to_vec();
        engine
            .delete_range(start, end)
            .expect("Failed to range delete");

        // Verify keys outside range still exist (0-49)
        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            let expected_value = format!("value_{:04}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, Some(expected_value));
        }

        // Verify keys in range are deleted (50-149)
        for i in 50..150 {
            let key = format!("key_{:04}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, None, "key_{:04} should be deleted", i);
        }

        // Verify keys after range still exist (150-199)
        for i in 150..num_keys {
            let key = format!("key_{:04}", i).into_bytes();
            let expected_value = format!("value_{:04}", i).into_bytes();
            let result = engine.get(key).expect("Failed to get");
            assert_eq!(result, Some(expected_value), "key_{:04} should exist", i);
        }
    }
}
