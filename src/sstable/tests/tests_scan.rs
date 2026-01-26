#[cfg(test)]
mod tests {
    use crate::sstable::{self, MemtablePointEntry, MemtableRangeTombstone, Record, SSTable};
    use tempfile::TempDir;
    use tracing::Level;
    use tracing_subscriber::fmt::Subscriber;

    fn init_tracing() {
        let _ = Subscriber::builder()
            .with_max_level(Level::TRACE)
            .try_init();
    }

    fn point(key: &[u8], value: &[u8], lsn: u64, timestamp: u64) -> MemtablePointEntry {
        MemtablePointEntry {
            key: key.to_vec(),
            value: Some(value.to_vec()),
            lsn,
            timestamp,
        }
    }

    fn del(key: &[u8], lsn: u64, timestamp: u64) -> MemtablePointEntry {
        MemtablePointEntry {
            key: key.to_vec(),
            value: None,
            lsn,
            timestamp,
        }
    }

    fn rdel(start: &[u8], end: &[u8], lsn: u64, timestamp: u64) -> MemtableRangeTombstone {
        MemtableRangeTombstone {
            start: start.to_vec(),
            end: end.to_vec(),
            lsn,
            timestamp,
        }
    }

    #[test]
    fn test_scan_only_points() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("scan_points.sst");

        let points = vec![
            point(b"a", b"1", 10, 100),
            point(b"b", b"2", 11, 101),
            point(b"c", b"3", 12, 102),
        ];

        let ranges = vec![];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.clone().into_iter(),
            ranges.len(),
            ranges.clone().into_iter(),
        )
        .unwrap();

        let sst = SSTable::open(&path).unwrap();
        let scanned: Vec<Record> = sst.scan(b"a", b"z").unwrap().collect();

        assert_eq!(scanned.len(), points.len() + ranges.len());

        match &scanned[0] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"a");
                assert_eq!(value.as_slice(), b"1");
                assert_eq!(*lsn, 10);
                assert_eq!(*timestamp, 100);
            }
            other => panic!("Expected Put(a), got {:?}", other),
        }

        match &scanned[1] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"b");
                assert_eq!(value.as_slice(), b"2");
                assert_eq!(*lsn, 11);
                assert_eq!(*timestamp, 101);
            }
            other => panic!("Expected Put(b), got {:?}", other),
        }

        match &scanned[2] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"c");
                assert_eq!(value.as_slice(), b"3");
                assert_eq!(*lsn, 12);
                assert_eq!(*timestamp, 102);
            }
            other => panic!("Expected Put(c), got {:?}", other),
        }
    }

    #[test]
    fn test_scan_point_deletes() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("scan_point_deletes.sst");

        let mut points = vec![
            point(b"a", b"1", 1, 10),
            del(b"b", 2, 11),
            point(b"c", b"3", 3, 12),
        ];

        let ranges = vec![];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.clone().into_iter(),
            ranges.len(),
            ranges.clone().into_iter(),
        )
        .unwrap();

        let sst = SSTable::open(&path).unwrap();
        let scanned: Vec<Record> = sst.scan(b"a", b"z").unwrap().collect();

        assert_eq!(scanned.len(), points.len() + ranges.len());

        match &scanned[0] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"a");
                assert_eq!(value.as_slice(), b"1");
                assert_eq!(*lsn, 1);
                assert_eq!(*timestamp, 10);
            }
            other => panic!("Expected Put(a), got {:?}", other),
        }

        match &scanned[1] {
            Record::Delete {
                key,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"b");
                assert_eq!(*lsn, 2);
                assert_eq!(*timestamp, 11);
            }
            other => panic!("Expected Delete(b), got {:?}", other),
        }

        match &scanned[2] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"c");
                assert_eq!(value.as_slice(), b"3");
                assert_eq!(*lsn, 3);
                assert_eq!(*timestamp, 12);
            }
            other => panic!("Expected Put(c), got {:?}", other),
        }
    }

    #[test]
    fn test_scan_only_range_delete() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("scan_ranges_only.sst");

        let points = vec![];
        let ranges = vec![rdel(b"a", b"z", 50, 999)];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.clone().into_iter(),
            ranges.len(),
            ranges.clone().into_iter(),
        )
        .unwrap();

        let sst = SSTable::open(&path).unwrap();
        let scanned: Vec<Record> = sst.scan(b"a", b"z").unwrap().collect();

        assert_eq!(scanned.len(), points.len() + ranges.len());

        match &scanned[0] {
            Record::RangeDelete {
                start,
                end,
                lsn,
                timestamp,
            } => {
                assert_eq!(start.as_slice(), b"a");
                assert_eq!(end.as_slice(), b"z");
                assert_eq!(*lsn, 50);
                assert_eq!(*timestamp, 999);
            }
            other => panic!("Expected RangeDelete(a..z), got {:?}", other),
        }
    }

    #[test]
    fn test_scan_range_delete() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("scan_range_delete.sst");

        let points = vec![
            point(b"a", b"1", 1, 10),
            point(b"b", b"2", 2, 11),
            point(b"c", b"3", 3, 12),
            point(b"d", b"4", 4, 13),
        ];

        let ranges = vec![
            rdel(b"b", b"d", 5, 50), // deletes b and c
        ];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.clone().into_iter(),
            ranges.len(),
            ranges.clone().into_iter(),
        )
        .unwrap();

        let sst = SSTable::open(&path).unwrap();
        let scanned: Vec<Record> = sst.scan(b"a", b"z").unwrap().collect();

        assert_eq!(scanned.len(), points.len() + ranges.len());

        match &scanned[0] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"a");
                assert_eq!(value.as_slice(), b"1");
                assert_eq!(*lsn, 1);
                assert_eq!(*timestamp, 10);
            }
            other => panic!("Expected Put(a), got {:?}", other),
        }

        match &scanned[1] {
            Record::RangeDelete {
                start,
                end,
                lsn,
                timestamp,
            } => {
                assert_eq!(start.as_slice(), b"b");
                assert_eq!(end.as_slice(), b"d");
                assert_eq!(*lsn, 5);
                assert_eq!(*timestamp, 50);
            }
            other => panic!("Expected RangeDelete(b..d), got {:?}", other),
        }

        match &scanned[2] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"b");
                assert_eq!(value.as_slice(), b"2");
                assert_eq!(*lsn, 2);
                assert_eq!(*timestamp, 11);
            }
            other => panic!("Expected Put(b), got {:?}", other),
        }

        match &scanned[3] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"c");
                assert_eq!(value.as_slice(), b"3");
                assert_eq!(*lsn, 3);
                assert_eq!(*timestamp, 12);
            }
            other => panic!("Expected Put(c), got {:?}", other),
        }

        match &scanned[4] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"d");
                assert_eq!(value.as_slice(), b"4");
                assert_eq!(*lsn, 4);
                assert_eq!(*timestamp, 13);
            }
            other => panic!("Expected Put(d), got {:?}", other),
        }
    }

    #[test]
    fn test_scan_mixed() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("scan_mixed.sst");

        // Flow of inserting points and range deletes into memtable:
        let mut points = vec![
            point(b"a", b"1", 1, 10),
            point(b"b", b"2", 2, 11),
            point(b"c", b"3", 3, 12),
            del(b"c", 4, 13),
            point(b"d", b"4", 5, 14),
            del(b"a", 6, 15),
            point(b"a", b"99", 8, 17),
            point(b"a", b"100", 9, 18),
            point(b"e", b"1000", 11, 20),
        ];
        points.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| b.lsn.cmp(&a.lsn)));

        // Flow of inserting range deletes into memtable:
        let mut ranges = vec![rdel(b"b", b"f", 7, 16), rdel(b"d", b"z", 10, 19)];
        ranges.sort_by(|a, b| a.start.cmp(&b.start).then_with(|| b.lsn.cmp(&a.lsn)));

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.clone().into_iter(),
            ranges.len(),
            ranges.clone().into_iter(),
        )
        .unwrap();

        let sst = SSTable::open(&path).unwrap();
        let scanned: Vec<Record> = sst.scan(b"a", b"z").unwrap().collect();

        assert_eq!(scanned.len(), points.len() + ranges.len());

        match &scanned[0] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"a");
                assert_eq!(value.as_slice(), b"100");
                assert_eq!(*lsn, 9);
                assert_eq!(*timestamp, 18);
            }
            other => panic!("Expected Put, got {:?}", other),
        }

        match &scanned[1] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"a");
                assert_eq!(value.as_slice(), b"99");
                assert_eq!(*lsn, 8);
                assert_eq!(*timestamp, 17);
            }
            other => panic!("Expected Put, got {:?}", other),
        }

        match &scanned[2] {
            Record::Delete {
                key,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"a");
                assert_eq!(*lsn, 6);
                assert_eq!(*timestamp, 15);
            }
            other => panic!("Expected Delete, got {:?}", other),
        }

        match &scanned[3] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"a");
                assert_eq!(value.as_slice(), b"1");
                assert_eq!(*lsn, 1);
                assert_eq!(*timestamp, 10);
            }
            other => panic!("Expected Put, got {:?}", other),
        }

        match &scanned[4] {
            Record::RangeDelete {
                start,
                end,
                lsn,
                timestamp,
            } => {
                assert_eq!(start.as_slice(), b"b");
                assert_eq!(end.as_slice(), b"f");
                assert_eq!(*lsn, 7);
                assert_eq!(*timestamp, 16);
            }
            other => panic!("Expected RangeDelete, got {:?}", other),
        }

        match &scanned[5] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"b");
                assert_eq!(value.as_slice(), b"2");
                assert_eq!(*lsn, 2);
                assert_eq!(*timestamp, 11);
            }
            other => panic!("Expected Put, got {:?}", other),
        }

        match &scanned[6] {
            Record::Delete {
                key,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"c");
                assert_eq!(*lsn, 4);
                assert_eq!(*timestamp, 13);
            }
            other => panic!("Expected Delete, got {:?}", other),
        }

        match &scanned[7] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"c");
                assert_eq!(value.as_slice(), b"3");
                assert_eq!(*lsn, 3);
                assert_eq!(*timestamp, 12);
            }
            other => panic!("Expected Put, got {:?}", other),
        }

        match &scanned[8] {
            Record::RangeDelete {
                start,
                end,
                lsn,
                timestamp,
            } => {
                assert_eq!(start.as_slice(), b"d");
                assert_eq!(end.as_slice(), b"z");
                assert_eq!(*lsn, 10);
                assert_eq!(*timestamp, 19);
            }
            other => panic!("Expected RangeDelete, got {:?}", other),
        }

        match &scanned[9] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"d");
                assert_eq!(value.as_slice(), b"4");
                assert_eq!(*lsn, 5);
                assert_eq!(*timestamp, 14);
            }
            other => panic!("Expected Put, got {:?}", other),
        }

        match &scanned[10] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"e");
                assert_eq!(value.as_slice(), b"1000");
                assert_eq!(*lsn, 11);
                assert_eq!(*timestamp, 20);
            }
            other => panic!("Expected Put, got {:?}", other),
        }
    }

    #[test]
    fn test_scan_range_delete_after_end() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("scan_after_end.sst");

        let points = vec![point(b"a", b"1", 1, 10), point(b"b", b"2", 2, 11)];
        let ranges = vec![rdel(b"z", b"zz", 10, 99)];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.clone().into_iter(),
            ranges.len(),
            ranges.clone().into_iter(),
        )
        .unwrap();

        let sst = SSTable::open(&path).unwrap();
        let scanned: Vec<Record> = sst.scan(b"a", b"d").unwrap().collect();

        assert_eq!(scanned.len(), points.len());

        match &scanned[0] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"a");
                assert_eq!(value.as_slice(), b"1");
                assert_eq!(*lsn, 1);
                assert_eq!(*timestamp, 10);
            }
            other => panic!("Expected Put, got {:?}", other),
        }

        match &scanned[1] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"b");
                assert_eq!(value.as_slice(), b"2");
                assert_eq!(*lsn, 2);
                assert_eq!(*timestamp, 11);
            }
            other => panic!("Expected Put, got {:?}", other),
        }
    }

    #[test]
    fn test_scan_range_delete_before_start() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("scan_before_start.sst");

        let points = vec![point(b"d", b"4", 4, 10), point(b"e", b"5", 5, 11)];
        let ranges = vec![rdel(b"a", b"c", 99, 3)];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.clone().into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();

        let sst = SSTable::open(&path).unwrap();
        let scanned: Vec<Record> = sst.scan(b"d", b"z").unwrap().collect();

        assert_eq!(scanned.len(), points.len());

        match &scanned[0] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"d");
                assert_eq!(value.as_slice(), b"4");
                assert_eq!(*lsn, 4);
                assert_eq!(*timestamp, 10);
            }
            other => panic!("Expected Put(d), got {:?}", other),
        }

        match &scanned[1] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"e");
                assert_eq!(value.as_slice(), b"5");
                assert_eq!(*lsn, 5);
                assert_eq!(*timestamp, 11);
            }
            other => panic!("Expected Put(e), got {:?}", other),
        }
    }

    #[test]
    fn test_scan_mid_range() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("scan_mid_range.sst");

        // a, b, c, d, e — all puts
        let points = vec![
            point(b"a", b"1", 1, 10),
            point(b"b", b"2", 2, 11),
            point(b"c", b"3", 3, 12),
            point(b"d", b"4", 4, 13),
            point(b"e", b"5", 5, 14),
        ];

        let ranges = vec![];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.clone().into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();

        let sst = SSTable::open(&path).unwrap();
        let scanned: Vec<Record> = sst.scan(b"c", b"e").unwrap().collect();

        // Should return c and d only
        assert_eq!(scanned.len(), 2);

        match &scanned[0] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"c");
                assert_eq!(value.as_slice(), b"3");
                assert_eq!(*lsn, 3);
                assert_eq!(*timestamp, 12);
            }
            other => panic!("Expected Put(c), got {:?}", other),
        }

        match &scanned[1] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"d");
                assert_eq!(value.as_slice(), b"4");
                assert_eq!(*lsn, 4);
                assert_eq!(*timestamp, 13);
            }
            other => panic!("Expected Put(d), got {:?}", other),
        }
    }
}
