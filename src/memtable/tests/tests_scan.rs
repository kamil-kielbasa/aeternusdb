#[cfg(test)]
mod scan_tests {
    use crate::memtable::Memtable;
    use tempfile::TempDir;

    #[test]
    fn test_scan_full_range() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal.bin");
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
        let path = tmp.path().join("wal.bin");
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
        let path = tmp.path().join("wal.bin");
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
        let path = tmp.path().join("wal.bin");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        let results: Vec<_> = memtable.scan(b"key0", b"key9").unwrap().collect();
        assert!(results.is_empty());
    }

    #[test]
    fn test_scan_no_matching_keys() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal.bin");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        for i in 0..5 {
            let key = format!("key{:003}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        let results: Vec<_> = memtable.scan(b"key100", b"key200").unwrap().collect();
        assert!(results.is_empty());
    }
}
