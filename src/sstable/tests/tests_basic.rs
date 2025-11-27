#[cfg(test)]
mod tests {
    use crate::sstable::{self, MemtablePointEntry, MemtableRangeTombstone, SSTable, SSTableError};
    use bloomfilter::Bloom;
    use std::fs;
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
    fn test_sstable_build_and_open() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let sstable_path = tmp.path().join("sstable_1.bin");

        let point_entries = vec![
            point(b"apple", b"red", 1, 100),
            point(b"banana", b"yellow", 2, 101),
            point(b"cherry", b"dark-red", 3, 102),
            del(b"strawberry", 4, 103),
        ];

        let range_tombstones = vec![
            rdel(b"grape", b"kiwi", 5, 110),
            rdel(b"orange", b"plum", 6, 120),
        ];

        sstable::build_from_iterators(
            &sstable_path,
            point_entries.len(),
            point_entries.into_iter(),
            range_tombstones.len(),
            range_tombstones.into_iter(),
        )
        .expect("Failed to build SSTable");

        let meta = fs::metadata(&sstable_path).unwrap();
        assert!(meta.len() > 128, "SSTable should be non-trivial in size");

        assert!(sstable_path.exists());
        let size = fs::metadata(&sstable_path).unwrap().len();
        assert!(size > 0, "SSTable file should not be empty");

        let sstable = SSTable::open(&sstable_path).expect("Failed to open SSTable");

        // --- HEADER CHECKS ---
        assert_eq!(sstable.header.magic, *b"SST0");
        assert_eq!(sstable.header.version, 1);

        // --- PROPERTIES CHECKS ---
        let props = &sstable.properties;
        assert_eq!(props.record_count, 4);
        assert_eq!(props.tombstone_count, 1);
        assert_eq!(props.range_tombstones_count, 2);

        assert_eq!(props.min_key, b"apple");
        assert_eq!(props.max_key, b"strawberry");

        assert_eq!(props.min_lsn, 1);
        assert_eq!(props.max_lsn, 6);

        assert_eq!(props.min_timestamp, 100);
        assert_eq!(props.max_timestamp, 120);

        // --- RANGE TOMBSTONES ---
        assert_eq!(sstable.range_deletes.data.len(), 2);
        assert_eq!(sstable.range_deletes.data[0].start_key, b"grape");
        assert_eq!(sstable.range_deletes.data[0].end_key, b"kiwi");
        assert_eq!(sstable.range_deletes.data[1].start_key, b"orange");
        assert_eq!(sstable.range_deletes.data[1].end_key, b"plum");

        // --- INDEX VALIDITY ---
        assert!(!sstable.index.is_empty());
        for ent in &sstable.index {
            assert!(!ent.separator_key.is_empty());
            assert!(ent.handle.offset > 0);
            assert!(ent.handle.size > 0);
        }

        // --- BLOOM FILTER CHECK ---
        let bloom_block = &sstable.bloom;
        assert!(!bloom_block.data.is_empty());

        // This confirms that bloom filter was populated
        let bloom = Bloom::from_slice(&bloom_block.data).expect("Bloom decode");
        assert!(bloom.check(&b"apple".to_vec()));
        assert!(bloom.check(&b"banana".to_vec()));
        assert!(bloom.check(&b"cherry".to_vec()));
        assert!(bloom.check(&b"strawberry".to_vec()));

        // --- FULL FILE SIZE CHECK ---
        assert_eq!(
            meta.len(),
            sstable.footer.total_file_size,
            "File footer `total_file_size` mismatch"
        );
    }

    #[test]
    fn test_sstable_try_build_empty() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let sstable_path = tmp.path().join("sstable_empty.bin");

        let points = vec![];
        let ranges = vec![];

        let result = sstable::build_from_iterators(
            &sstable_path,
            points.len(),
            points.into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap_err();

        assert!(matches!(result, SSTableError::Internal(_)));
        assert!(
            result
                .to_string()
                .contains("Empty iterators cannot build SSTable")
        );
    }

    #[test]
    fn test_sstable_try_build_range_deletes_only_no_points() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let sstable_path = tmp.path().join("sstable_empty.bin");

        let points = vec![];
        let ranges = vec![rdel(b"a", b"f", 30, 200), rdel(b"f", b"z", 31, 201)];

        sstable::build_from_iterators(
            &sstable_path,
            points.len(),
            points.into_iter(),
            ranges.len(),
            ranges.clone().into_iter(),
        )
        .unwrap();
        let sst = SSTable::open(&sstable_path).unwrap();

        assert_eq!(sst.properties.record_count, 0);

        // Check metadata
        assert_eq!(sst.properties.record_count, 0);
        assert_eq!(sst.properties.range_tombstones_count, ranges.len() as u64);

        // Check min/max keys
        assert!(sst.properties.min_key.is_empty());
        assert!(sst.properties.max_key.is_empty());
    }

    #[test]
    fn test_sstable_try_build_points_only_no_range_deletes() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let sstable_path = tmp.path().join("sstable_points_only.bin");

        let points = vec![
            point(b"a", b"1", 10, 100),
            point(b"b", b"2", 20, 110),
            point(b"c", b"3", 30, 120),
        ];
        let ranges = vec![];

        sstable::build_from_iterators(
            &sstable_path,
            points.len(),
            points.clone().into_iter(),
            ranges.len(),
            ranges.into_iter(),
        )
        .unwrap();

        let sst = SSTable::open(&sstable_path).unwrap();

        // Check metadata
        assert_eq!(sst.properties.record_count, points.len() as u64);
        assert_eq!(sst.properties.range_tombstones_count, 0);

        // Check min/max keys
        assert_eq!(sst.properties.min_key, b"a");
        assert_eq!(sst.properties.max_key, b"c");
    }
}
