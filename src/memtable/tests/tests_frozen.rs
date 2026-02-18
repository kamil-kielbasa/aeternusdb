//! Frozen memtable tests.
//!
//! A `FrozenMemtable` is an immutable snapshot of an active `Memtable`.
//! It exposes `get()`, `scan()`, and `iter_for_flush()` with the same
//! semantics as the original, but no further mutations are allowed.
//!
//! These tests verify that freezing a memtable preserves the data
//! faithfully and that the underlying WAL file remains on disk as long
//! as the frozen memtable is alive.
//!
//! ## See also
//! - [`tests_basic`] — active `Memtable` API tests
//! - [`tests_scan`] — raw multi-version scan output

#[cfg(test)]
mod tests {
    use crate::memtable::{Memtable, MemtableGetResult, Record, Wal};
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // get — frozen matches active
    // ----------------------------------------------------------------

    /// # Scenario
    /// `get()` on a `FrozenMemtable` returns the same results as the
    /// active memtable it was derived from.
    ///
    /// # Starting environment
    /// Active memtable with one put (`a`), one put-then-delete (`b`).
    ///
    /// # Actions
    /// 1. `put("a", "1")`, `put("b", "2")`, `delete("b")`.
    /// 2. `frozen()` → get `a`, `b`, `c`.
    ///
    /// # Expected behavior
    /// - `a` → `Put("1")`
    /// - `b` → `Delete`
    /// - `c` → `NotFound`
    #[test]
    fn get_matches_active_memtable() {
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

    // ----------------------------------------------------------------
    // scan — frozen matches active
    // ----------------------------------------------------------------

    /// # Scenario
    /// `scan()` on a `FrozenMemtable` produces the same raw multi-version
    /// output (including tombstones) as the active memtable.
    ///
    /// # Starting environment
    /// Active memtable with puts `a`, `b`, `c` and a range-delete `[b, d)`.
    ///
    /// # Actions
    /// 1. Insert 3 keys + range-delete.
    /// 2. `frozen()` → `scan("a", "z")`.
    /// 3. Compare each record to the expected list.
    ///
    /// # Expected behavior
    /// 4 records: `Put(a)`, `RangeDelete(b..d)`, `Put(b)`, `Put(c)`.
    /// Keys, values, and LSNs match; timestamps are non-zero.
    #[test]
    fn scan_matches_active_memtable() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");

        let memtable = Memtable::new(&path, None, 1024).unwrap();

        memtable.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        memtable.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        memtable.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        memtable.delete_range(b"b".to_vec(), b"d".to_vec()).unwrap();

        let frozen = memtable.frozen().unwrap();
        let results: Vec<_> = frozen.scan(b"a", b"z").unwrap().collect();

        let expected = [
            Record::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                lsn: 1,
                timestamp: 0,
            },
            Record::RangeDelete {
                start: b"b".to_vec(),
                end: b"d".to_vec(),
                lsn: 4,
                timestamp: 0,
            },
            Record::Put {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
                lsn: 2,
                timestamp: 0,
            },
            Record::Put {
                key: b"c".to_vec(),
                value: b"3".to_vec(),
                lsn: 3,
                timestamp: 0,
            },
        ];

        assert_eq!(results.len(), expected.len());
        for (res, exp) in results.iter().zip(expected.iter()) {
            match (res, exp) {
                (
                    Record::Put {
                        key: rk,
                        value: rv,
                        lsn: rlsn,
                        timestamp: rts,
                    },
                    Record::Put {
                        key: ek,
                        value: ev,
                        lsn: elsn,
                        timestamp: _ets,
                    },
                ) => {
                    assert_eq!(rk, ek);
                    assert_eq!(rv, ev);
                    assert_eq!(rlsn, elsn);
                    assert!(*rts > 0);
                }
                (
                    Record::Delete {
                        key: rk,
                        lsn: rlsn,
                        timestamp: rts,
                    },
                    Record::Delete {
                        key: ek,
                        lsn: elsn,
                        timestamp: _ets,
                    },
                ) => {
                    assert_eq!(rk, ek);
                    assert_eq!(rlsn, elsn);
                    assert!(*rts > 0);
                }
                (
                    Record::RangeDelete {
                        start: rk,
                        end: rks,
                        lsn: rlsn,
                        timestamp: rts,
                    },
                    Record::RangeDelete {
                        start: ek,
                        end: eks,
                        lsn: elsn,
                        timestamp: _ets,
                    },
                ) => {
                    assert_eq!(rk, ek);
                    assert_eq!(rks, eks);
                    assert_eq!(rlsn, elsn);
                    assert!(*rts > 0);
                }
                _ => panic!("Mismatched scan result types"),
            }
        }
    }

    // ----------------------------------------------------------------
    // iter_for_flush — all records present
    // ----------------------------------------------------------------

    /// # Scenario
    /// `iter_for_flush()` on a `FrozenMemtable` returns every latest-
    /// version record including tombstones.
    ///
    /// # Starting environment
    /// Active memtable: `put(a)`, `put(b)`, `delete(a)`, `delete_range(c, e)`.
    ///
    /// # Actions
    /// 1. `frozen()` → `iter_for_flush()`.
    ///
    /// # Expected behavior
    /// 3 records: `Put(b)`, `Delete(a)`, `RangeDelete(c..e)`.
    /// (The earlier `Put(a)` is superseded by `Delete(a)`.)
    #[test]
    fn iter_for_flush_returns_all_records() {
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
            Record::Put { key, .. } if key == b"b"
        )));

        assert!(records.iter().any(|r| matches!(
            r,
            Record::Delete { key, .. } if key == b"a"
        )));

        assert!(records.iter().any(|r| matches!(
            r,
            Record::RangeDelete { start, end, .. }
                if start == b"c" && end == b"e"
        )));
    }

    // ----------------------------------------------------------------
    // WAL file lifetime guarantee
    // ----------------------------------------------------------------

    /// # Scenario
    /// The WAL file must remain on disk as long as its `FrozenMemtable`
    /// is alive (the frozen memtable holds an `Arc` to the WAL).
    ///
    /// # Starting environment
    /// Active memtable with several operations.
    ///
    /// # Actions
    /// 1. Put + delete.
    /// 2. `frozen()`, drop the active memtable scope.
    /// 3. Verify WAL file still exists.
    /// 4. Reopen WAL and replay → verify 3 records.
    ///
    /// # Expected behavior
    /// The WAL file is not deleted prematurely; replay returns all
    /// 3 operations (put, put, delete).
    #[test]
    fn keeps_wal_alive() {
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
        let wal = Wal::<Record>::open(&wal_path, None).unwrap();
        let records: Vec<_> = wal.replay_iter().unwrap().map(|r| r.unwrap()).collect();

        assert_eq!(records.len(), 3);
    }
}
