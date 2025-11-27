#[cfg(test)]
mod tests {
    use crate::sstable::{
        self, MemtablePointEntry, MemtableRangeTombstone, SSTScanResult, SSTable,
        SSTableScanIterator,
    };
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

    fn collect_results(mut it: SSTableScanIterator) -> Vec<SSTScanResult> {
        let mut out = Vec::new();

        while let Some(res) = it.next() {
            match res {
                Ok(v) => out.push(v),
                Err(e) => panic!("Unexpected scan error: {:?}", e),
            }
        }

        out
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
        let it = sst.scan(b"a", b"z").unwrap();
        let out = collect_results(it);

        assert_eq!(out.len(), points.len() + ranges.len());

        // Put a
        match &out[0] {
            SSTScanResult::Put {
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

        // Put b
        match &out[1] {
            SSTScanResult::Put {
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

        // Put c
        match &out[2] {
            SSTScanResult::Put {
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

        let points = vec![
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
        let it = sst.scan(b"a", b"z").unwrap();
        let out = collect_results(it);

        assert_eq!(out.len(), points.len() + ranges.len());

        // 0: put a
        match &out[0] {
            SSTScanResult::Put {
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

        // 1: delete b
        match &out[1] {
            SSTScanResult::Delete {
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

        // 2: put c
        match &out[2] {
            SSTScanResult::Put {
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
        let it = sst.scan(b"a", b"z").unwrap();
        let out = collect_results(it);

        assert_eq!(out.len(), points.len() + ranges.len());

        match &out[0] {
            SSTScanResult::RangeDelete {
                start_key,
                end_key,
                lsn,
                timestamp,
            } => {
                assert_eq!(start_key.as_slice(), b"a");
                assert_eq!(end_key.as_slice(), b"z");
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
            rdel(b"b", b"d", 100, 50), // deletes b and c
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
        let it = sst.scan(b"a", b"z").unwrap();
        let out = collect_results(it);

        assert_eq!(out.len(), points.len() + ranges.len());

        // 0: range tombstone [b, d)
        match &out[0] {
            SSTScanResult::RangeDelete {
                start_key,
                end_key,
                lsn,
                timestamp,
            } => {
                assert_eq!(start_key.as_slice(), b"b");
                assert_eq!(end_key.as_slice(), b"d");
                assert_eq!(*lsn, 100);
                assert_eq!(*timestamp, 50);
            }
            other => panic!("Expected RangeDelete(b..d), got {:?}", other),
        }

        // 1: put a
        match &out[1] {
            SSTScanResult::Put {
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

        // 2: put b
        match &out[2] {
            SSTScanResult::Put {
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

        // 3: put c
        match &out[3] {
            SSTScanResult::Put {
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

        // 4: put d
        match &out[4] {
            SSTScanResult::Put {
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

        let points = vec![
            point(b"a", b"1", 1, 10),
            point(b"b", b"2", 2, 11),
            del(b"c", 3, 12),
            point(b"d", b"4", 4, 13),
        ];

        let ranges = vec![rdel(b"b", b"f", 50, 9)];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.clone().into_iter(),
            ranges.len(),
            ranges.clone().into_iter(),
        )
        .unwrap();

        let sst = SSTable::open(&path).unwrap();
        let it = sst.scan(b"a", b"z").unwrap();
        let out = collect_results(it);

        assert_eq!(out.len(), points.len() + ranges.len());

        // 0: range delete b..f
        match &out[0] {
            SSTScanResult::RangeDelete {
                start_key,
                end_key,
                lsn,
                timestamp,
            } => {
                assert_eq!(start_key.as_slice(), b"b");
                assert_eq!(end_key.as_slice(), b"f");
                assert_eq!(*lsn, 50);
                assert_eq!(*timestamp, 9);
            }
            other => panic!("Expected RangeDelete(b..f), got {:?}", other),
        }

        // 1: put a
        match &out[1] {
            SSTScanResult::Put {
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

        // 2: put b
        match &out[2] {
            SSTScanResult::Put {
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

        // 3: delete c
        match &out[3] {
            SSTScanResult::Delete {
                key,
                lsn,
                timestamp,
            } => {
                assert_eq!(key.as_slice(), b"c");
                assert_eq!(*lsn, 3);
                assert_eq!(*timestamp, 12);
            }
            other => panic!("Expected Delete(c), got {:?}", other),
        }

        // 4: put d
        match &out[4] {
            SSTScanResult::Put {
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
        let it = sst.scan(b"a", b"d").unwrap();
        let out = collect_results(it);

        assert_eq!(out.len(), points.len());

        // a
        match &out[0] {
            SSTScanResult::Put {
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

        // b
        match &out[1] {
            SSTScanResult::Put {
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
        let it = sst.scan(b"d", b"z").unwrap();
        let out = collect_results(it);

        assert_eq!(out.len(), points.len());

        // d
        match &out[0] {
            SSTScanResult::Put {
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

        // e
        match &out[1] {
            SSTScanResult::Put {
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

        // Scan only c..e (e is exclusive)
        let it = sst.scan(b"c", b"e").unwrap();
        let out = collect_results(it);

        // Should return c and d only
        assert_eq!(out.len(), 2);

        // c
        match &out[0] {
            SSTScanResult::Put {
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

        // d
        match &out[1] {
            SSTScanResult::Put {
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
