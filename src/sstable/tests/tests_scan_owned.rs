//! Tests for `SSTable::scan_owned` — the `Arc`-based owned scan iterator.
//!
//! `scan_owned` returns a `ScanIterator<Arc<SSTable>>` that keeps the SSTable
//! alive via `Arc`, enabling `'static` iterators that survive past lock-guard
//! drops.  This is the foundation of the MVCC snapshot scan path.
//!
//! Coverage:
//! - Owned scan produces identical results to borrowed scan.
//! - `Arc<SSTable>` keeps data alive after the original handle is dropped.
//! - Owned scan with range tombstones interleaved.
//! - Owned scan with mixed puts, deletes, and range deletes.
//! - Empty range owned scan yields nothing.
//! - Owned scan sstable iterator is `'static` (compile-time proof).

#[cfg(test)]
mod tests {
    use crate::sstable::{self, PointEntry, RangeTombstone, Record, SSTable};
    use std::sync::Arc;
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

    /// Build an SSTable from points + range tombstones and return as Arc.
    fn build_arc_sst(
        points: Vec<PointEntry>,
        ranges: Vec<RangeTombstone>,
    ) -> (TempDir, Arc<SSTable>) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.sst");
        let pt_count = points.len();
        let rt_count = ranges.len();
        sstable::SstWriter::new(&path)
            .build(points.into_iter(), pt_count, ranges.into_iter(), rt_count)
            .unwrap();
        let sst = SSTable::open(&path).unwrap();
        (tmp, Arc::new(sst))
    }

    // ----------------------------------------------------------------
    // Equivalence: scan_owned == scan for points-only SSTable
    // ----------------------------------------------------------------

    /// # Scenario
    /// Owned scan must produce exactly the same records as borrowed scan.
    ///
    /// # Starting environment
    /// SSTable with 4 put entries.
    ///
    /// # Actions
    /// 1. Collect all records from `sst.scan(b"a", b"z")`.
    /// 2. Collect all records from `SSTable::scan_owned(&arc, b"a", b"z")`.
    ///
    /// # Expected behavior
    /// Both record sequences are identical.
    #[test]
    fn scan_owned_matches_borrowed_scan() {
        let points = vec![
            point(b"a", b"1", 10, 100),
            point(b"b", b"2", 11, 101),
            point(b"c", b"3", 12, 102),
            point(b"d", b"4", 13, 103),
        ];
        let (_tmp, arc) = build_arc_sst(points, vec![]);

        let borrowed: Vec<Record> = arc.scan(b"a", b"z").unwrap().collect();
        let owned: Vec<Record> = SSTable::scan_owned(&arc, b"a", b"z").unwrap().collect();

        assert_eq!(borrowed.len(), owned.len());
        for (b, o) in borrowed.iter().zip(owned.iter()) {
            assert_eq!(b.key(), o.key());
            assert_eq!(b.lsn(), o.lsn());
        }
    }

    // ----------------------------------------------------------------
    // Arc keeps SSTable alive after original Arc is the only handle
    // ----------------------------------------------------------------

    /// # Scenario
    /// The owned scan iterator must keep the SSTable alive even when
    /// no other `Arc` handle exists.
    ///
    /// # Starting environment
    /// SSTable with 3 put entries, held as `Arc<SSTable>`.
    ///
    /// # Actions
    /// 1. Create `scan_owned` iterator.
    /// 2. Drop the original `Arc<SSTable>`.
    /// 3. Consume the iterator.
    ///
    /// # Expected behavior
    /// All 3 records are returned successfully — the iterator's internal
    /// `Arc` clone keeps the mmap alive.
    #[test]
    fn scan_owned_survives_arc_drop() {
        let points = vec![
            point(b"x", b"10", 1, 1),
            point(b"y", b"20", 2, 2),
            point(b"z", b"30", 3, 3),
        ];
        let (_tmp, arc) = build_arc_sst(points, vec![]);

        // Create owned iterator, then drop the original Arc.
        let iter = SSTable::scan_owned(&arc, b"x", b"zz").unwrap();
        drop(arc);

        let records: Vec<Record> = iter.collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key(), b"x");
        assert_eq!(records[1].key(), b"y");
        assert_eq!(records[2].key(), b"z");
    }

    // ----------------------------------------------------------------
    // Owned scan with range tombstones interleaved
    // ----------------------------------------------------------------

    /// # Scenario
    /// Owned scan must correctly yield range tombstones merged with points.
    ///
    /// # Starting environment
    /// SSTable with 2 puts and 1 range tombstone.
    ///
    /// # Actions
    /// 1. `SSTable::scan_owned(&arc, b"a", b"z")`.
    ///
    /// # Expected behavior
    /// 3 records: the range delete and the 2 puts, in sorted order.
    #[test]
    fn scan_owned_with_range_tombstones() {
        let points = vec![point(b"a", b"1", 10, 100), point(b"d", b"4", 13, 103)];
        let ranges = vec![rdel(b"b", b"d", 11, 101)];
        let (_tmp, arc) = build_arc_sst(points, ranges);

        let records: Vec<Record> = SSTable::scan_owned(&arc, b"a", b"z").unwrap().collect();
        assert_eq!(records.len(), 3);

        assert!(matches!(&records[0], Record::Put { key, .. } if key == b"a"));
        assert!(
            matches!(&records[1], Record::RangeDelete { start, end, .. } if start == b"b" && end == b"d")
        );
        assert!(matches!(&records[2], Record::Put { key, .. } if key == b"d"));
    }

    // ----------------------------------------------------------------
    // Mixed: puts, point-deletes, range-deletes
    // ----------------------------------------------------------------

    /// # Scenario
    /// Owned scan must yield all record types correctly.
    ///
    /// # Starting environment
    /// SSTable with 1 put, 1 point-delete, and 1 range-delete.
    ///
    /// # Actions
    /// 1. `SSTable::scan_owned(&arc, b"a", b"z")`.
    ///
    /// # Expected behavior
    /// All three record types present in the output.
    #[test]
    fn scan_owned_mixed_record_types() {
        let points = vec![point(b"a", b"1", 10, 100), del(b"b", 11, 101)];
        let ranges = vec![rdel(b"c", b"f", 12, 102)];
        let (_tmp, arc) = build_arc_sst(points, ranges);

        let records: Vec<Record> = SSTable::scan_owned(&arc, b"a", b"z").unwrap().collect();

        let has_put = records.iter().any(|r| matches!(r, Record::Put { .. }));
        let has_del = records.iter().any(|r| matches!(r, Record::Delete { .. }));
        let has_rdel = records
            .iter()
            .any(|r| matches!(r, Record::RangeDelete { .. }));

        assert!(has_put, "expected a Put record");
        assert!(has_del, "expected a Delete record");
        assert!(has_rdel, "expected a RangeDelete record");
    }

    // ----------------------------------------------------------------
    // Empty range yields nothing
    // ----------------------------------------------------------------

    /// # Scenario
    /// Owned scan on a range with no keys returns an empty iterator.
    ///
    /// # Starting environment
    /// SSTable with puts at keys `a`, `b`, `c`.
    ///
    /// # Actions
    /// 1. `SSTable::scan_owned(&arc, b"m", b"z")` — no keys in range.
    ///
    /// # Expected behavior
    /// Zero records.
    #[test]
    fn scan_owned_empty_range() {
        let points = vec![
            point(b"a", b"1", 10, 100),
            point(b"b", b"2", 11, 101),
            point(b"c", b"3", 12, 102),
        ];
        let (_tmp, arc) = build_arc_sst(points, vec![]);

        let records: Vec<Record> = SSTable::scan_owned(&arc, b"m", b"z").unwrap().collect();
        assert!(records.is_empty());
    }

    // ----------------------------------------------------------------
    // Compile-time proof: owned scan iterator is 'static
    // ----------------------------------------------------------------

    /// # Scenario
    /// The owned scan iterator must be `'static` — it can be stored in
    /// a struct, sent across threads, or returned from a function that
    /// drops all local state.
    ///
    /// # Actions
    /// 1. Create an owned scan iterator inside a closure that drops the Arc.
    /// 2. Return the iterator from the closure.
    /// 3. Consume it outside.
    ///
    /// # Expected behavior
    /// Compiles and produces correct results — proves the iterator is `'static`.
    #[test]
    fn scan_owned_is_static() {
        let points = vec![point(b"a", b"1", 10, 100), point(b"b", b"2", 11, 101)];
        let (_tmp, arc) = build_arc_sst(points, vec![]);

        // This function signature proves the iterator is 'static.
        fn make_iter(sst: &Arc<SSTable>) -> Box<dyn Iterator<Item = Record>> {
            let iter = SSTable::scan_owned(sst, b"a", b"z").unwrap();
            Box::new(iter)
        }

        let boxed = make_iter(&arc);
        drop(arc); // drop Arc — iterator must survive

        let records: Vec<Record> = boxed.collect();
        assert_eq!(records.len(), 2);
    }
}
