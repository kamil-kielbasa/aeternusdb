#[cfg(test)]
mod scan_tests {
    use crate::memtable::Memtable;
    use tempfile::TempDir;

    #[test]
    fn test_scan_full_range() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        let results: Vec<_> = memtable.scan(b"key0", b"key9\xff").unwrap().collect();
        assert_eq!(results.len(), 10);
        assert_eq!(results[0].0, b"key0".to_vec());
        assert_eq!(results[9].0, b"key9".to_vec());
    }

    #[test]
    fn test_scan_partial_range() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        let results: Vec<_> = memtable.scan(b"key3", b"key7").unwrap().collect();
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, b"key3".to_vec());
        assert_eq!(results[3].0, b"key6".to_vec());
    }

    #[test]
    fn test_scan_skips_deleted() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        memtable.delete(b"key1".to_vec()).unwrap();
        memtable.delete(b"key3".to_vec()).unwrap();

        let results: Vec<_> = memtable.scan(b"key0", b"key4\xff").unwrap().collect();
        let keys: Vec<_> = results.into_iter().map(|(k, _)| k).collect();

        assert_eq!(
            keys,
            vec![b"key0".to_vec(), b"key2".to_vec(), b"key4".to_vec()]
        );
    }

    #[test]
    fn test_scan_empty_memtable() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        let results: Vec<_> = memtable.scan(b"key0", b"key9").unwrap().collect();
        assert!(results.is_empty());
    }

    #[test]
    fn test_scan_no_matching_keys() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        for i in 0..5 {
            let key = format!("key{:003}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        let results: Vec<_> = memtable.scan(b"key100", b"key200").unwrap().collect();
        assert!(results.is_empty());
    }

    #[test]
    fn test_scan_with_range_tombstones() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        // Insert keys key0 through key9
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        // Add range tombstone that deletes key3 through key6
        memtable
            .delete_range(b"key3".to_vec(), b"key7".to_vec())
            .unwrap();

        // Scan the full range
        let results: Vec<_> = memtable.scan(b"key0", b"key9\xff").unwrap().collect();
        let keys: Vec<_> = results.into_iter().map(|(k, _)| k).collect();

        // Should only contain key0, key1, key2, key7, key8, key9
        assert_eq!(
            keys,
            vec![
                b"key0".to_vec(),
                b"key1".to_vec(),
                b"key2".to_vec(),
                b"key7".to_vec(),
                b"key8".to_vec(),
                b"key9".to_vec()
            ]
        );
    }

    #[test]
    fn test_scan_with_mixed_operations() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        // Insert initial keys key0 through key9
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        // Delete range key2 through key5
        memtable
            .delete_range(b"key2".to_vec(), b"key6".to_vec())
            .unwrap();

        // Re-insert key3 and key4 with new values (should override the range tombstone)
        memtable
            .put(b"key3".to_vec(), b"new_value3".to_vec())
            .unwrap();
        memtable
            .put(b"key4".to_vec(), b"new_value4".to_vec())
            .unwrap();

        // Delete range key7 through key9
        memtable
            .delete_range(b"key7".to_vec(), b"key9\xff".to_vec())
            .unwrap();

        // Insert key8 after range deletion
        memtable
            .put(b"key8".to_vec(), b"new_value8".to_vec())
            .unwrap();

        // Scan the full range
        let results: Vec<_> = memtable.scan(b"key0", b"key9\xff").unwrap().collect();

        // Should contain key0, key1, key3 (new), key4 (new), key6, key8 (new)
        assert_eq!(results.len(), 6);

        assert_eq!(results[0].0, b"key0".to_vec());
        assert_eq!(results[0].1.value, Some(b"value0".to_vec()));

        assert_eq!(results[1].0, b"key1".to_vec());
        assert_eq!(results[1].1.value, Some(b"value1".to_vec()));

        assert_eq!(results[2].0, b"key3".to_vec());
        assert_eq!(results[2].1.value, Some(b"new_value3".to_vec()));

        assert_eq!(results[3].0, b"key4".to_vec());
        assert_eq!(results[3].1.value, Some(b"new_value4".to_vec()));

        assert_eq!(results[4].0, b"key6".to_vec());
        assert_eq!(results[4].1.value, Some(b"value6".to_vec()));

        assert_eq!(results[5].0, b"key8".to_vec());
        assert_eq!(results[5].1.value, Some(b"new_value8".to_vec()));
    }
}
