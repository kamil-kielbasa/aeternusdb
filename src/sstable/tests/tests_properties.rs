//! SSTable property accessor coverage tests.
//!
//! These tests build a representative SSTable and exercise every public
//! accessor method on [`SSTable`] to ensure full code coverage of the
//! property delegation layer.

#[cfg(test)]
mod tests {
    use crate::sstable::{self, PointEntry, RangeTombstone, SSTable};
    use tempfile::TempDir;

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

    /// Builds and opens an SSTable, then verifies every property accessor.
    #[test]
    fn all_property_accessors() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("props.sst");

        let points = vec![
            point(b"aaa", b"v1", 10, 1000),
            point(b"bbb", b"v2", 20, 2000),
            del(b"ccc", 30, 3000),
        ];
        let ranges = vec![rdel(b"ddd", b"eee", 40, 4000)];

        let pt_count = points.len();
        let rt_count = ranges.len();

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), pt_count, ranges.into_iter(), rt_count)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        // LSN accessors
        assert_eq!(sst.min_lsn(), 10);
        assert_eq!(sst.max_lsn(), 40);

        // Record counts
        assert_eq!(sst.record_count(), 3);
        assert_eq!(sst.tombstone_count(), 1);
        assert_eq!(sst.range_tombstone_count(), 1);

        // Key range (only point entries, not range tombstones)
        assert_eq!(sst.min_key(), b"aaa");
        assert_eq!(sst.max_key(), b"ccc");

        // Timestamps
        assert_eq!(sst.min_timestamp(), 1000);
        assert_eq!(sst.max_timestamp(), 4000);

        // Creation timestamp should be non-zero (set at build time)
        assert!(sst.creation_timestamp() > 0);

        // File size should match footer
        assert!(sst.file_size() > 0);

        // ID defaults to 0 before engine assigns it
        assert_eq!(sst.id(), 0);
    }

    /// Tests the `set_id` + `id` round-trip.
    #[test]
    fn set_and_get_id() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("id.sst");

        let points = vec![point(b"key", b"val", 1, 100)];
        sstable::SstWriter::new(&path)
            .build(points.into_iter(), 1, std::iter::empty(), 0)
            .unwrap();

        let mut sst = SSTable::open(&path).unwrap();
        assert_eq!(sst.id(), 0);
        sst.set_id(42);
        assert_eq!(sst.id(), 42);
    }

    /// Tests bloom filter check on a known-present and known-absent key.
    #[test]
    fn bloom_may_contain() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bloom.sst");

        let points = vec![point(b"exists", b"yes", 1, 100)];
        sstable::SstWriter::new(&path)
            .build(points.into_iter(), 1, std::iter::empty(), 0)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        // Key that was inserted should be "maybe" present
        assert!(sst.bloom_may_contain(b"exists"));

        // Key that was never inserted — bloom *may* say false (likely)
        // We can't guarantee false for a single key, but we test the call succeeds
        let _ = sst.bloom_may_contain(b"definitely_not_here_xyz_12345");
    }

    /// Tests `range_tombstone_iter()` returns the expected tombstones.
    #[test]
    fn range_tombstone_iter_coverage() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("rt_iter.sst");

        let points = vec![point(b"a", b"v", 1, 100)];
        let ranges = vec![rdel(b"m", b"p", 10, 200), rdel(b"x", b"z", 20, 300)];

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), 1, ranges.into_iter(), 2)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();
        let tombstones: Vec<_> = sst.range_tombstone_iter().collect();
        assert_eq!(tombstones.len(), 2);
        assert_eq!(tombstones[0].start, b"m");
        assert_eq!(tombstones[0].end, b"p");
        assert_eq!(tombstones[0].lsn, 10);
        assert_eq!(tombstones[1].start, b"x");
        assert_eq!(tombstones[1].end, b"z");
        assert_eq!(tombstones[1].lsn, 20);
    }

    /// Tests `find_block_for_key` with an empty index edge case.
    #[test]
    fn get_on_range_deletes_only_sst() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("no_points.sst");

        // SSTable with only range tombstones, no point entries
        let ranges = vec![rdel(b"a", b"z", 5, 500)];
        sstable::SstWriter::new(&path)
            .build(std::iter::empty(), 0, ranges.into_iter(), 1)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        // get() on a key inside the range tombstone
        let result = sst.get(b"m").unwrap();
        assert!(
            matches!(
                result,
                crate::sstable::GetResult::RangeDelete { lsn: 5, .. }
            ),
            "expected RangeDelete, got {:?}",
            result
        );

        // get() on a key outside the range
        let result = sst.get(b"\xff").unwrap();
        assert_eq!(result, crate::sstable::GetResult::NotFound);
    }
}
