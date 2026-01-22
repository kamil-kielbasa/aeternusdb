#[cfg(test)]
mod frozen_tests {
    use crate::memtable::{FrozenMemtable, Memtable, MemtableGetResult, MemtableRecord, Wal};
    use tempfile::TempDir;

    #[test]
    fn frozen_memtable_get_matches_memtable() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");

        let memtable = Memtable::new(&path, None, 1024).unwrap();

        memtable.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        memtable.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        memtable.delete(b"b".to_vec()).unwrap();

        let frozen = memtable.frozen().unwrap();

        assert_eq!(
            frozen.get(b"a").unwrap(),
            MemtableGetResult::Put(b"1".to_vec())
        );
        assert_eq!(frozen.get(b"b").unwrap(), MemtableGetResult::Delete);
        assert_eq!(frozen.get(b"c").unwrap(), MemtableGetResult::NotFound);
    }

    #[test]
    fn frozen_memtable_scan_matches_memtable() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");

        let memtable = Memtable::new(&path, None, 1024).unwrap();

        memtable.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        memtable.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        memtable.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        memtable.delete_range(b"b".to_vec(), b"d".to_vec()).unwrap();

        let frozen = memtable.frozen().unwrap();

        let results: Vec<_> = frozen.scan(b"a", b"z").unwrap().map(|(k, _)| k).collect();

        assert_eq!(results, vec![b"a".to_vec()]);
    }

    #[test]
    fn frozen_memtable_iter_for_flush_returns_all_records() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");

        let memtable = Memtable::new(&path, None, 4096).unwrap();

        memtable.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        memtable.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        memtable.delete(b"a".to_vec()).unwrap();
        memtable.delete_range(b"c".to_vec(), b"e".to_vec()).unwrap();

        let frozen = memtable.frozen().unwrap();

        let records: Vec<_> = frozen.iter_for_flush().unwrap().collect();

        assert_eq!(records.len(), 3);

        assert!(records.iter().any(|r| matches!(
            r,
            MemtableRecord::Put { key, .. } if key == b"b"
        )));

        assert!(records.iter().any(|r| matches!(
            r,
            MemtableRecord::Delete { key, .. } if key == b"a"
        )));

        assert!(records.iter().any(|r| matches!(
            r,
            MemtableRecord::RangeDelete { start, end, .. }
                if start == b"c" && end == b"e"
        )));
    }

    #[test]
    fn frozen_memtable_keeps_wal_alive() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal-000000.log");

        {
            let memtable = Memtable::new(&wal_path, None, 4096).unwrap();
            memtable.put(b"a".to_vec(), b"1".to_vec()).unwrap();
            memtable.put(b"b".to_vec(), b"2".to_vec()).unwrap();
            memtable.delete(b"a".to_vec()).unwrap();

            let _frozen = memtable.frozen().unwrap();
            // frozen dropped later, WAL must still exist now
        }

        assert!(wal_path.exists(), "WAL file was removed prematurely");

        // WAL must still be replayable
        let wal = Wal::<MemtableRecord>::open(&wal_path, None).unwrap();
        let records: Vec<_> = wal.replay_iter().unwrap().map(|r| r.unwrap()).collect();

        assert_eq!(records.len(), 3);
    }
}
