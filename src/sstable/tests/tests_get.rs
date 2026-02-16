//! SSTable `get()` — intra-file LSN-resolution tests.
//!
//! `SSTable::get(key)` returns the **single winning record** for a key by
//! comparing points, point-deletes, and range-deletes at their LSNs.
//! These tests verify every precedence combination:
//!
//! Coverage:
//! - Single put → `SSTGetResult::Put`
//! - Point-delete over put → `SSTGetResult::Delete`
//! - Range-delete with no point entries → `SSTGetResult::RangeDelete`
//! - Point put vs range-delete — point wins (higher LSN)
//! - Point put vs range-delete — range wins (higher LSN)
//! - Point-delete vs range-delete — range wins
//! - Point-delete vs range-delete — point wins
//! - Multiple versions of same key — max LSN wins
//!
//! ## See also
//! - [`tests_basic`] — SSTable build / open / structural validation
//! - [`tests_scan`] — raw unresolved SSTable scan output

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

    // ----------------------------------------------------------------
    // Single put
    // ----------------------------------------------------------------

    /// # Scenario
    /// A single put entry is retrieved by exact key lookup.
    ///
    /// # Starting environment
    /// SSTable with one point entry `("a", "val1", lsn=10)`.
    ///
    /// # Actions
    /// 1. `sst.get(b"a")`.
    ///
    /// # Expected behavior
    /// `SSTGetResult::Put { value: "val1", lsn: 10, timestamp: 100 }`.
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

    // ----------------------------------------------------------------
    // Point-delete over put
    // ----------------------------------------------------------------

    /// # Scenario
    /// A point-delete at a higher LSN shadows an earlier put.
    ///
    /// # Starting environment
    /// SSTable with `put("a", lsn=10)` and `del("a", lsn=20)`.
    ///
    /// # Actions
    /// 1. `sst.get(b"a")`.
    ///
    /// # Expected behavior
    /// `SSTGetResult::Delete { lsn: 20, timestamp: 110 }`.
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

    // ----------------------------------------------------------------
    // Range-delete only (no point data for key)
    // ----------------------------------------------------------------

    /// # Scenario
    /// A range-delete covers the queried key and there are no point
    /// entries at all.
    ///
    /// # Starting environment
    /// SSTable with only `range_delete("a".."z", lsn=30)`.
    ///
    /// # Actions
    /// 1. `sst.get(b"m")` — key inside the range.
    ///
    /// # Expected behavior
    /// `SSTGetResult::RangeDelete { lsn: 30, timestamp: 200 }`.
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

    // ----------------------------------------------------------------
    // Point put vs range-delete — point wins (higher LSN)
    // ----------------------------------------------------------------

    /// # Scenario
    /// A point put at LSN 50 and a range-delete at LSN 40 compete.
    /// The put has the higher LSN and wins.
    ///
    /// # Starting environment
    /// SSTable with `put("a", lsn=50)` and `range_delete("a".."z", lsn=40)`.
    ///
    /// # Actions
    /// 1. `sst.get(b"a")`.
    ///
    /// # Expected behavior
    /// `SSTGetResult::Put { value: "val1", lsn: 50, timestamp: 100 }`.
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

    // ----------------------------------------------------------------
    // Point put vs range-delete — range wins (higher LSN)
    // ----------------------------------------------------------------

    /// # Scenario
    /// A range-delete at LSN 60 shadows a point put at LSN 50.
    ///
    /// # Starting environment
    /// SSTable with `put("a", lsn=50)` and `range_delete("a".."z", lsn=60)`.
    ///
    /// # Actions
    /// 1. `sst.get(b"a")`.
    ///
    /// # Expected behavior
    /// `SSTGetResult::RangeDelete { lsn: 60, timestamp: 110 }`.
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

    // ----------------------------------------------------------------
    // Point-delete vs range-delete — range wins (higher LSN)
    // ----------------------------------------------------------------

    /// # Scenario
    /// A point-delete (lsn=50) competes with a range-delete (lsn=60).
    /// The range-delete has the higher LSN and wins.
    ///
    /// # Starting environment
    /// SSTable with `del("a", lsn=50)` and `range_delete("a".."z", lsn=60)`.
    ///
    /// # Actions
    /// 1. `sst.get(b"a")`.
    ///
    /// # Expected behavior
    /// `SSTGetResult::RangeDelete { lsn: 60, timestamp: 110 }`.
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

    // ----------------------------------------------------------------
    // Point-delete vs range-delete — point wins (higher LSN)
    // ----------------------------------------------------------------

    /// # Scenario
    /// A point-delete (lsn=70) competes with a range-delete (lsn=60).
    /// The point-delete has the higher LSN and wins.
    ///
    /// # Starting environment
    /// SSTable with `del("a", lsn=70)` and `range_delete("a".."z", lsn=60)`.
    ///
    /// # Actions
    /// 1. `sst.get(b"a")`.
    ///
    /// # Expected behavior
    /// `SSTGetResult::Delete { lsn: 70, timestamp: 120 }`.
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

    // ----------------------------------------------------------------
    // Multiple versions — max LSN wins
    // ----------------------------------------------------------------

    /// # Scenario
    /// Three versions of the same key at different LSNs.
    /// `get()` must return the version with the highest LSN.
    ///
    /// # Starting environment
    /// SSTable with `put("a", "v1", lsn=10)`, `put("a", "v2", lsn=20)`,
    /// and `put("a", "v3", lsn=15)`.
    ///
    /// # Actions
    /// 1. `sst.get(b"a")`.
    ///
    /// # Expected behavior
    /// `SSTGetResult::Put { value: "v2", lsn: 20, timestamp: 120 }`.
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
