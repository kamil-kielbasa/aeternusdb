//! SSTable structural boundary tests — Priority 3.
//!
//! These tests cover SSTable edge cases at the builder/reader level:
//! a single-entry SSTable, an SSTable with only point tombstones,
//! an SSTable with only range tombstones, and scan/get boundary
//! precision when the index has a single block handle.
//!
//! ## See also
//! - [`tests_edge_cases`]  — corrupted files, multi-block, wrong magic
//! - [`tests_corruption`]  — block-level CRC corruption detection
//! - [`tests_basic`]       — standard build/open/property validation
//! - [`tests_get`]         — LSN resolution in get()

#[cfg(test)]
mod tests {
    use crate::sstable::{self, GetResult, PointEntry, RangeTombstone, Record, SSTable};
    use tempfile::TempDir;
    use tracing::Level;
    use tracing_subscriber::fmt::Subscriber;

    fn init_tracing() {
        let _ = Subscriber::builder()
            .with_max_level(Level::TRACE)
            .try_init();
    }

    fn point(key: &[u8], value: Option<&[u8]>, lsn: u64, timestamp: u64) -> PointEntry {
        PointEntry {
            key: key.to_vec(),
            value: value.map(|v| v.to_vec()),
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

    // ================================================================
    // 1. Single-entry SSTable (one point entry)
    // ================================================================

    /// # Scenario
    /// Build an SSTable from exactly one `PointEntry`. Verify the bloom
    /// filter, index, properties, `get()`, and `scan()` all work.
    ///
    /// # Expected behavior
    /// - `properties.min_key == max_key == "only_key"`
    /// - `get("only_key")` returns `Put`
    /// - `get("other")` returns `NotFound`
    /// - `scan()` yields exactly 1 record
    /// - Index has exactly 1 entry (single data block)
    #[test]
    fn single_entry_sstable_get_and_scan() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("single_entry.sst");

        let points = vec![point(b"only_key", Some(b"only_val"), 1, 100)];
        let ranges: Vec<RangeTombstone> = vec![];

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), 1, ranges.into_iter(), 0)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        // Properties.
        assert_eq!(sst.properties.min_key, b"only_key");
        assert_eq!(sst.properties.max_key, b"only_key");

        // Index should have exactly 1 entry.
        assert_eq!(sst.index.len(), 1, "Single data block expected");

        // Get the key.
        match sst.get(b"only_key").unwrap() {
            GetResult::Put { value, lsn, .. } => {
                assert_eq!(value, b"only_val");
                assert_eq!(lsn, 1);
            }
            other => panic!("Expected Put, got {:?}", other),
        }

        // Missing key.
        assert_eq!(sst.get(b"other").unwrap(), GetResult::NotFound);

        // Scan.
        let records: Vec<Record> = sst.scan(b"\x00", b"\xff").unwrap().collect();
        assert_eq!(records.len(), 1);
        match &records[0] {
            Record::Put {
                key, value, lsn, ..
            } => {
                assert_eq!(key, b"only_key");
                assert_eq!(value, b"only_val");
                assert_eq!(*lsn, 1);
            }
            other => panic!("Expected Put, got {:?}", other),
        }
    }

    // ================================================================
    // 2. All point tombstones SSTable (no live values)
    // ================================================================

    /// # Scenario
    /// Build an SSTable from only `PointEntry { value: None }` entries
    /// (point deletions). The SSTable is structurally valid and the bloom
    /// contains tombstone keys.
    ///
    /// # Expected behavior
    /// - `get(key)` returns `GetResult::Delete`
    /// - `scan()` returns only `Record::Delete` entries
    #[test]
    fn all_point_tombstones_sstable_get_and_scan() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("tombstones_only.sst");

        let points = vec![
            point(b"del_a", None, 1, 100),
            point(b"del_b", None, 2, 101),
            point(b"del_c", None, 3, 102),
        ];
        let ranges: Vec<RangeTombstone> = vec![];

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), 3, ranges.into_iter(), 0)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        // Get returns Delete for each tombstone key.
        for key in [b"del_a", b"del_b", b"del_c"] {
            match sst.get(key).unwrap() {
                GetResult::Delete { lsn, .. } => {
                    assert!(lsn > 0);
                }
                other => panic!("Expected Delete for {:?}, got {:?}", key, other),
            }
        }

        // Get for absent key.
        assert_eq!(sst.get(b"del_z").unwrap(), GetResult::NotFound);

        // Scan returns all 3 as Delete records.
        let records: Vec<Record> = sst.scan(b"del_", b"del_\xff").unwrap().collect();
        assert_eq!(records.len(), 3);
        for rec in &records {
            match rec {
                Record::Delete { .. } => {}
                other => panic!("Expected Delete, got {:?}", other),
            }
        }
    }

    // ================================================================
    // 3. Range-tombstones-only SSTable (no point entries)
    // ================================================================

    /// # Scenario
    /// Build an SSTable with only range tombstones (no point entries).
    /// The data section is empty; only the range-delete section exists.
    ///
    /// # Expected behavior
    /// - `get(covered_key)` returns `RangeDelete`
    /// - `get(uncovered_key)` returns `NotFound`
    /// - `scan()` produces `Record::RangeDelete` entries
    #[test]
    fn range_tombstones_only_sstable() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("range_only.sst");

        let points: Vec<PointEntry> = vec![];
        let ranges = vec![
            rdel(b"aaa", b"bbb", 10, 1000),
            rdel(b"ccc", b"ddd", 11, 1001),
        ];

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), 0, ranges.into_iter(), 2)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        // Keys covered by range tombstones.
        match sst.get(b"abc").unwrap() {
            GetResult::RangeDelete { lsn, .. } => assert_eq!(lsn, 10),
            other => panic!("Expected RangeDelete for 'abc', got {:?}", other),
        }
        match sst.get(b"ccd").unwrap() {
            GetResult::RangeDelete { lsn, .. } => assert_eq!(lsn, 11),
            other => panic!("Expected RangeDelete for 'ccd', got {:?}", other),
        }

        // Key outside range tombstones.
        assert_eq!(sst.get(b"zzz").unwrap(), GetResult::NotFound);

        // Scan should produce range-delete records in the output.
        let records: Vec<Record> = sst.scan(b"\x00", b"\xff").unwrap().collect();
        let range_deletes: Vec<_> = records
            .iter()
            .filter(|r| matches!(r, Record::RangeDelete { .. }))
            .collect();
        assert_eq!(
            range_deletes.len(),
            2,
            "Should have 2 range tombstone records"
        );
    }

    // ================================================================
    // 4. SSTable with mixed points and tombstones at minimum scale
    // ================================================================

    /// # Scenario
    /// Build an SSTable with one live entry, one point tombstone, and one
    /// range tombstone — the minimal mixed case.
    ///
    /// # Expected behavior
    /// - `get("alive")` → `Put`
    /// - `get("dead")` → `Delete`
    /// - `get("covered")` → `RangeDelete`
    /// - `get("other")` → `NotFound`
    #[test]
    fn minimal_mixed_sstable() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("minimal_mixed.sst");

        let points = vec![
            point(b"alive", Some(b"value"), 1, 100),
            point(b"dead", None, 2, 101),
        ];
        let ranges = vec![rdel(b"range_a", b"range_z", 3, 102)];

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), 2, ranges.into_iter(), 1)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        match sst.get(b"alive").unwrap() {
            GetResult::Put { value, .. } => assert_eq!(value, b"value"),
            other => panic!("Expected Put, got {:?}", other),
        }

        match sst.get(b"dead").unwrap() {
            GetResult::Delete { .. } => {}
            other => panic!("Expected Delete, got {:?}", other),
        }

        match sst.get(b"range_m").unwrap() {
            GetResult::RangeDelete { .. } => {}
            other => panic!("Expected RangeDelete, got {:?}", other),
        }

        assert_eq!(sst.get(b"other").unwrap(), GetResult::NotFound);
    }

    // ================================================================
    // 5. SSTable with duplicate keys at different LSNs
    // ================================================================

    /// # Scenario
    /// Build an SSTable where the same key appears multiple times
    /// with different LSNs. `get()` should return the highest-LSN entry.
    ///
    /// # Expected behavior
    /// `get("key")` returns the value from the highest LSN.
    #[test]
    fn duplicate_keys_highest_lsn_wins() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("dup_keys.sst");

        let points = vec![
            point(b"key", Some(b"old"), 1, 100),
            point(b"key", Some(b"mid"), 5, 500),
            point(b"key", Some(b"new"), 10, 1000),
        ];
        let ranges: Vec<RangeTombstone> = vec![];

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), 3, ranges.into_iter(), 0)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        match sst.get(b"key").unwrap() {
            GetResult::Put { value, lsn, .. } => {
                assert_eq!(value, b"new");
                assert_eq!(lsn, 10);
            }
            other => panic!("Expected Put with lsn=10, got {:?}", other),
        }
    }
}
