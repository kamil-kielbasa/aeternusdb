#[cfg(test)]
mod tests {
    use crate::memtable::{Memtable, MemtableError};
    use tempfile::TempDir;
    use tracing::Level;
    use tracing_subscriber::fmt::Subscriber;

    fn init_tracing() {
        let _ = Subscriber::builder()
            .with_max_level(Level::TRACE)
            .try_init();
    }

    #[test]
    fn test_put_and_get() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        memtable.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        let value = memtable.get(b"key1").unwrap();

        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_delete_key() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        memtable.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        memtable.delete(b"key1".to_vec()).unwrap();

        let value = memtable.get(b"key1").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_flush() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        memtable.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        memtable.put(b"b".to_vec(), b"2".to_vec()).unwrap();

        let mut flushed: Vec<_> = memtable.flush().unwrap().collect();
        flushed.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(flushed[0].0, b"a".to_vec());
        assert_eq!(flushed[1].0, b"b".to_vec());

        assert_eq!(memtable.get(b"a").unwrap(), None);
    }

    #[test]
    fn test_scan_range() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        memtable.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        memtable.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        memtable.put(b"c".to_vec(), b"3".to_vec()).unwrap();

        let scanned: Vec<_> = memtable.scan(b"a", b"c").unwrap().collect();
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].0, b"a".to_vec());
        assert_eq!(scanned[1].0, b"b".to_vec());
    }

    #[test]
    fn test_multiple_versions() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        memtable.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        memtable.put(b"a".to_vec(), b"2".to_vec()).unwrap();

        let value = memtable.get(b"a").unwrap();
        assert_eq!(value, Some(b"2".to_vec()));
    }

    #[test]
    fn test_write_buffer_limit() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 16).unwrap();

        let res = memtable.put(b"a".to_vec(), b"1234567890".to_vec());
        assert!(matches!(res, Err(MemtableError::FlushRequired)));
    }

    #[test]
    fn test_wal_recovery() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");

        {
            let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();
            memtable.put(b"x".to_vec(), b"y".to_vec()).unwrap();
        }

        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();
        let value = memtable.get(b"x").unwrap();
        assert_eq!(value, Some(b"y".to_vec()));
    }

    #[test]
    fn test_empty_get_and_scan() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        assert_eq!(memtable.get(b"nonexistent").unwrap(), None);
        assert_eq!(memtable.scan(b"a", b"z").unwrap().count(), 0);
    }
}
