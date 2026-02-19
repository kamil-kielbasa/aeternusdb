//! SSTable build / open / format-verification tests.
//!
//! These tests exercise the lowest-level SSTable lifecycle: building a file
//! from memtable iterators, re-opening it, and validating every on-disk
//! structural block (header, properties, index, bloom filter, range-delete
//! block, and footer).
//!
//! Coverage:
//! - Round-trip build → open with points + range tombstones
//! - Rejection of empty iterators (no data at all)
//! - Range-deletes-only SSTable (no point entries)
//! - Points-only SSTable (no range tombstones)
//!
//! ## See also
//! - [`tests_get`]  — intra-SSTable `get()` with LSN resolution
//! - [`tests_scan`] — raw unresolved SSTable scan output

#[cfg(test)]
mod tests {
    use crate::sstable::{self, PointEntry, RangeTombstone, SSTable, SSTableError};
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

    fn point(key: &[u8], value: &[u8], lsn: u64, timestamp: u64) -> PointEntry {
        PointEntry {
            key: key.to_vec(),
            value: Some(value.to_vec()),
            lsn,
            timestamp,
        }
    }

    fn del(key: &[u8], lsn: u64, timestamp: u64) -> PointEntry {
        PointEntry {
            key: key.to_vec(),
            value: None,
            lsn,
            timestamp,
        }
    }

    fn rdel(start: &[u8], end: &[u8], lsn: u64, timestamp: u64) -> RangeTombstone {
        RangeTombstone {
            start: start.to_vec(),
            end: end.to_vec(),
            lsn,
            timestamp,
        }
    }

    // ----------------------------------------------------------------
    // Build + open round-trip
    // ----------------------------------------------------------------

    /// # Scenario
    /// Build an SSTable from a mix of puts, a point-delete, and two range
    /// tombstones, then re-open and verify every structural block.
    ///
    /// # Starting environment
    /// No SSTable file on disk.
    ///
    /// # Actions
    /// 1. `build_from_iterators` with 3 puts + 1 point-delete + 2 range
    ///    tombstones.
    /// 2. `SSTable::open` the resulting file.
    ///
    /// # Expected behavior
    /// - Header: magic = `SST0`, version = 1.
    /// - Properties: 4 records, 1 tombstone, 2 range tombstones;
    ///   correct min/max key/LSN/timestamp.
    /// - Range-delete block contains both tombstones.
    /// - Index entries have non-empty keys and valid offsets.
    /// - Bloom filter recognises all four point keys.
    /// - Footer `total_file_size` matches actual file size.
    #[test]
    fn build_and_open() {
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

        let pt_count = point_entries.len();

        let rt_count = range_tombstones.len();

        sstable::SstWriter::new(&sstable_path)
            .build(
                point_entries.into_iter(),
                pt_count,
                range_tombstones.into_iter(),
                rt_count,
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

    // ----------------------------------------------------------------
    // Empty SSTable rejected
    // ----------------------------------------------------------------

    /// # Scenario
    /// Attempt to build an SSTable with zero points and zero range
    /// tombstones — the builder must reject the request.
    ///
    /// # Starting environment
    /// No SSTable file on disk.
    ///
    /// # Actions
    /// 1. `build_from_iterators` with empty point and range iterators.
    ///
    /// # Expected behavior
    /// Returns `SSTableError::Internal` with message
    /// `"Empty iterators cannot build SSTable"`.
    #[test]
    fn build_empty_fails() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let sstable_path = tmp.path().join("sstable_empty.bin");

        let points = vec![];
        let ranges = vec![];

        let pt_count = points.len();
        let rt_count = ranges.len();
        let result = sstable::SstWriter::new(&sstable_path)
            .build(points.into_iter(), pt_count, ranges.into_iter(), rt_count)
            .unwrap_err();

        assert!(matches!(result, SSTableError::Internal(_)));
        assert!(
            result
                .to_string()
                .contains("Empty iterators cannot build SSTable")
        );
    }

    // ----------------------------------------------------------------
    // Range-deletes only (no points)
    // ----------------------------------------------------------------

    /// # Scenario
    /// Build an SSTable that contains only range tombstones and no point
    /// entries — the builder should succeed.
    ///
    /// # Starting environment
    /// No SSTable file on disk.
    ///
    /// # Actions
    /// 1. `build_from_iterators` with two range tombstones, zero points.
    /// 2. Open and inspect properties.
    ///
    /// # Expected behavior
    /// `record_count == 0`, `range_tombstones_count == 2`,
    /// `min_key` / `max_key` are empty (no point entries to derive them from).
    #[test]
    fn build_range_deletes_only() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let sstable_path = tmp.path().join("sstable_empty.bin");

        let points = vec![];
        let ranges = vec![rdel(b"a", b"f", 30, 200), rdel(b"f", b"z", 31, 201)];

        let pt_count = points.len();

        let rt_count = ranges.len();

        sstable::SstWriter::new(&sstable_path)
            .build(
                points.into_iter(),
                pt_count,
                ranges.clone().into_iter(),
                rt_count,
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

    // ----------------------------------------------------------------
    // Points only (no range deletes)
    // ----------------------------------------------------------------

    /// # Scenario
    /// Build an SSTable with only point entries — no range tombstones.
    ///
    /// # Starting environment
    /// No SSTable file on disk.
    ///
    /// # Actions
    /// 1. `build_from_iterators` with 3 point entries, zero range tombstones.
    /// 2. Open and inspect properties.
    ///
    /// # Expected behavior
    /// `record_count == 3`, `range_tombstones_count == 0`,
    /// `min_key == "a"`, `max_key == "c"`.
    #[test]
    fn build_points_only() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let sstable_path = tmp.path().join("sstable_points_only.bin");

        let points = vec![
            point(b"a", b"1", 10, 100),
            point(b"b", b"2", 20, 110),
            point(b"c", b"3", 30, 120),
        ];
        let ranges = vec![];

        let pt_count = points.len();

        let rt_count = ranges.len();

        sstable::SstWriter::new(&sstable_path)
            .build(
                points.clone().into_iter(),
                pt_count,
                ranges.into_iter(),
                rt_count,
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
