#[cfg(test)]
mod tests {
    use crate::sstable::{self, MemtablePointEntry, MemtableRangeTombstone, SSTGetResult, SSTable};
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
    fn get_single_put() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_single_put.bin");

        let points = vec![point(b"a", b"val1", 10, 100)];
        let ranges: Vec<MemtableRangeTombstone> = vec![];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();
        let sst = SSTable::open(&path).unwrap();

        assert_eq!(
            sst.get(b"a").unwrap(),
            SSTGetResult::Put {
                value: b"val1".to_vec(),
                lsn: 10,
                timestamp: 100
            }
        );
    }

    #[test]
    fn get_point_delete() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_point_delete.bin");

        let points = vec![point(b"a", b"val1", 10, 100), del(b"a", 20, 110)];
        let ranges: Vec<MemtableRangeTombstone> = vec![];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();
        let sst = SSTable::open(&path).unwrap();

        assert_eq!(
            sst.get(b"a").unwrap(),
            SSTGetResult::Delete {
                lsn: 20,
                timestamp: 110
            }
        );
    }

    #[test]
    fn get_range_delete_only() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_range_delete.bin");

        let points = vec![];
        let ranges = vec![rdel(b"a", b"z", 30, 200)];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();
        let sst = SSTable::open(&path).unwrap();

        assert_eq!(
            sst.get(b"m").unwrap(),
            SSTGetResult::RangeDelete {
                lsn: 30,
                timestamp: 200
            }
        );
    }

    #[test]
    fn get_point_and_range_delete_point_wins() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_point_range.bin");

        let points = vec![point(b"a", b"val1", 50, 100)];
        let ranges = vec![rdel(b"a", b"z", 40, 90)];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();
        let sst = SSTable::open(&path).unwrap();

        assert_eq!(
            sst.get(b"a").unwrap(),
            SSTGetResult::Put {
                value: b"val1".to_vec(),
                lsn: 50,
                timestamp: 100
            }
        );
    }

    #[test]
    fn get_point_and_range_delete_range_wins() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_point_range2.bin");

        let points = vec![point(b"a", b"val1", 50, 100)];
        let ranges = vec![rdel(b"a", b"z", 60, 110)];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();
        let sst = SSTable::open(&path).unwrap();

        assert_eq!(
            sst.get(b"a").unwrap(),
            SSTGetResult::RangeDelete {
                lsn: 60,
                timestamp: 110
            }
        );
    }

    #[test]
    fn get_point_delete_and_range_delete_range_wins() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_point_range_delete.bin");

        let points = vec![
            del(b"a", 50, 100), // point delete
        ];
        let ranges = vec![
            rdel(b"a", b"z", 60, 110), // range delete with newer LSN
        ];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();
        let sst = SSTable::open(&path).unwrap();

        // Range delete wins because its LSN is higher
        assert_eq!(
            sst.get(b"a").unwrap(),
            SSTGetResult::RangeDelete {
                lsn: 60,
                timestamp: 110
            }
        );
    }

    #[test]
    fn get_point_delete_and_range_delete_point_wins() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_point_range_delete2.bin");

        let points = vec![
            del(b"a", 70, 120), // point delete with newer LSN
        ];
        let ranges = vec![
            rdel(b"a", b"z", 60, 110), // older range delete
        ];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();
        let sst = SSTable::open(&path).unwrap();

        // Point delete wins because its LSN is higher
        assert_eq!(
            sst.get(b"a").unwrap(),
            SSTGetResult::Delete {
                lsn: 70,
                timestamp: 120
            }
        );
    }

    #[test]
    fn get_multiple_versions_point_pick_max_lsn() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_multi_point.bin");

        let points = vec![
            point(b"a", b"v1", 10, 100),
            point(b"a", b"v2", 20, 120),
            point(b"a", b"v3", 15, 110),
        ];
        let ranges: Vec<MemtableRangeTombstone> = vec![];

        sstable::build_from_iterators(
            &path,
            points.len(),
            points.into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();
        let sst = SSTable::open(&path).unwrap();

        assert_eq!(
            sst.get(b"a").unwrap(),
            SSTGetResult::Put {
                value: b"v2".to_vec(),
                lsn: 20,
                timestamp: 120
            }
        );
    }
}
