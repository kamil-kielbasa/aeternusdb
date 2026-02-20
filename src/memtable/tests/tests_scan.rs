//! Memtable raw scan output tests.
//!
//! Unlike the engine's `scan()` (which resolves tombstones and deduplicates),
//! the memtable's `scan()` returns **all versions** of every key in the
//! requested range — puts, point-deletes, and range-deletes — ordered by
//! `(key ASC, lsn DESC)`.  This raw output is the input to the engine's
//! visibility filter and to `iter_for_flush()` / SSTable building.
//!
//! Coverage:
//! - Full-range scan with verified LSNs and timestamps
//! - Partial-range scan
//! - Scan with point-deletes (both put and delete versions emitted)
//! - Empty memtable scan
//! - Scan with no matching keys
//! - Scan with range tombstones (tombstone + covered puts emitted)
//! - Mixed operations: range-deletes, point-deletes, overwrites, and
//!   re-insertions — full raw output verified
//!
//! ## See also
//! - [`tests_basic`] — active `Memtable` API tests
//! - [`tests_frozen`] — `FrozenMemtable` API correctness

#[cfg(test)]
mod tests {
    use crate::memtable::{Memtable, Record};
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Full-range scan
    // ----------------------------------------------------------------

    /// # Scenario
    /// Scan the entire key space and verify every record has the correct
    /// key, value, LSN, and a non-zero timestamp.
    ///
    /// # Starting environment
    /// Fresh memtable with 10 keys (`key0`–`key9`).
    ///
    /// # Actions
    /// 1. `scan("key0", "key9\xff")` — covers all 10 keys.
    ///
    /// # Expected behavior
    /// 10 `Put` records in ascending key order, LSNs 1–10,
    /// `timestamp > 0` on each.
    #[test]
    fn scan_full_range() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        let results: Vec<_> = memtable.scan(b"key0", b"key9\xff").unwrap().collect();
        assert_eq!(results.len(), 10);

        for (i, result) in results.iter().enumerate() {
            match result {
                Record::Put {
                    key,
                    value,
                    lsn,
                    timestamp,
                } => {
                    let expected_key = format!("key{}", i).into_bytes();
                    let expected_value = format!("value{}", i).into_bytes();
                    assert_eq!(key, &expected_key);
                    assert_eq!(value, &expected_value);
                    assert_eq!(*lsn, (i + 1) as u64);
                    assert!(*timestamp > 0);
                }
                other => panic!("Expected Put, got {:?}", other),
            }
        }
    }

    // ----------------------------------------------------------------
    // Partial-range scan
    // ----------------------------------------------------------------

    /// # Scenario
    /// A partial scan returns only keys in `[start, end)`.
    ///
    /// # Starting environment
    /// 10 keys (`key0`–`key9`) in the memtable.
    ///
    /// # Actions
    /// 1. `scan("key3", "key7")`.
    ///
    /// # Expected behavior
    /// 4 records for `key3`–`key6` with correct LSNs (4–7).
    #[test]
    fn scan_partial_range() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        let results: Vec<_> = memtable.scan(b"key3", b"key7").unwrap().collect();
        assert_eq!(results.len(), 4);

        for (i, result) in results.iter().enumerate() {
            match result {
                Record::Put {
                    key,
                    value,
                    lsn,
                    timestamp,
                } => {
                    let expected_key = format!("key{}", i + 3).into_bytes();
                    let expected_value = format!("value{}", i + 3).into_bytes();
                    assert_eq!(key, &expected_key);
                    assert_eq!(value, &expected_value);
                    assert_eq!(*lsn, (i + 4) as u64);
                    assert!(*timestamp > 0);
                }
                other => panic!("Expected Put, got {:?}", other),
            }
        }
    }

    // ----------------------------------------------------------------
    // Scan with point deletes
    // ----------------------------------------------------------------

    /// # Scenario
    /// After deleting some keys, scan emits both the `Delete` tombstone
    /// and the older `Put` for each deleted key (raw multi-version output).
    ///
    /// # Starting environment
    /// 5 keys (`key0`–`key4`), then `key1` and `key3` deleted.
    ///
    /// # Actions
    /// 1. `scan("key0", "key4\xff")`.
    ///
    /// # Expected behavior
    /// 7 records total:
    /// `Put(key0)`, `Delete(key1)`, `Put(key1)`, `Put(key2)`,
    /// `Delete(key3)`, `Put(key3)`, `Put(key4)`.
    /// Deletes have higher LSNs than the corresponding puts.
    #[test]
    fn scan_with_deletions() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        memtable.delete(b"key1".to_vec()).unwrap();
        memtable.delete(b"key3".to_vec()).unwrap();

        let results: Vec<_> = memtable.scan(b"key0", b"key4\xff").unwrap().collect();
        assert_eq!(results.len(), 7);

        let expected = [
            Record::Put {
                key: b"key0".to_vec(),
                value: b"value0".to_vec(),
                lsn: 1,
                timestamp: 0,
            },
            Record::Delete {
                key: b"key1".to_vec(),
                lsn: 6,
                timestamp: 0,
            },
            Record::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                lsn: 2,
                timestamp: 0,
            },
            Record::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
                lsn: 3,
                timestamp: 0,
            },
            Record::Delete {
                key: b"key3".to_vec(),
                lsn: 7,
                timestamp: 0,
            },
            Record::Put {
                key: b"key3".to_vec(),
                value: b"value3".to_vec(),
                lsn: 4,
                timestamp: 0,
            },
            Record::Put {
                key: b"key4".to_vec(),
                value: b"value4".to_vec(),
                lsn: 5,
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
                _ => panic!("Mismatched scan result types"),
            }
        }
    }

    // ----------------------------------------------------------------
    // Empty memtable scan
    // ----------------------------------------------------------------

    /// # Scenario
    /// Scan on an empty memtable yields zero records.
    ///
    /// # Starting environment
    /// Fresh memtable — no writes.
    ///
    /// # Actions
    /// 1. `scan("key0", "key9")`.
    ///
    /// # Expected behavior
    /// Empty result set.
    #[test]
    fn scan_empty_memtable() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        let results: Vec<_> = memtable.scan(b"key0", b"key9").unwrap().collect();
        assert!(results.is_empty());
    }

    // ----------------------------------------------------------------
    // Scan when no keys match the range
    // ----------------------------------------------------------------

    /// # Scenario
    /// Scan with bounds that don't overlap any existing key.
    ///
    /// # Starting environment
    /// 5 keys (`key000`–`key004`).
    ///
    /// # Actions
    /// 1. `scan("key100", "key200")` — no overlap.
    ///
    /// # Expected behavior
    /// Empty result set.
    #[test]
    fn scan_no_matching_keys() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        for i in 0..5 {
            let key = format!("key{:003}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        let results: Vec<_> = memtable.scan(b"key100", b"key200").unwrap().collect();
        assert!(results.is_empty());
    }

    // ----------------------------------------------------------------
    // Scan with range tombstones
    // ----------------------------------------------------------------

    /// # Scenario
    /// After a `delete_range`, the scan emits the `RangeDelete` tombstone
    /// **and** the covered `Put` records (all raw versions).
    ///
    /// # Starting environment
    /// 5 keys (`key0`–`key4`), then `delete_range("key3", "key5")`.
    ///
    /// # Actions
    /// 1. `scan("key0", "key5\xff")`.
    ///
    /// # Expected behavior
    /// 6 records: `Put(key0)`, `Put(key1)`, `Put(key2)`,
    /// `RangeDelete(key3..key5)`, `Put(key3)`, `Put(key4)`.
    /// The range tombstone appears before the covered puts (higher LSN,
    /// same start key).
    #[test]
    fn scan_with_range_tombstones() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        // Insert keys key0 through key9
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        // Add range tombstone that deletes key3 through key6
        memtable
            .delete_range(b"key3".to_vec(), b"key5".to_vec())
            .unwrap();

        // Scan the full range
        let results: Vec<_> = memtable.scan(b"key0", b"key9\xff").unwrap().collect();

        let expected = [
            Record::Put {
                key: b"key0".to_vec(),
                value: b"value0".to_vec(),
                lsn: 1,
                timestamp: 0,
            },
            Record::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                lsn: 2,
                timestamp: 0,
            },
            Record::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
                lsn: 3,
                timestamp: 0,
            },
            Record::RangeDelete {
                start: b"key3".to_vec(),
                end: b"key5".to_vec(),
                lsn: 6,
                timestamp: 0,
            },
            Record::Put {
                key: b"key3".to_vec(),
                value: b"value3".to_vec(),
                lsn: 4,
                timestamp: 0,
            },
            Record::Put {
                key: b"key4".to_vec(),
                value: b"value4".to_vec(),
                lsn: 5,
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
    // Scan with mixed operations
    // ----------------------------------------------------------------

    /// # Scenario
    /// A complex sequence of puts, range-deletes, re-inserts, and
    /// point-deletes. The raw scan must emit every version for each key.
    ///
    /// # Starting environment
    /// 10 keys (`key0`–`key9`).
    ///
    /// # Actions
    /// 1. `delete_range("key2", "key6")`.
    /// 2. Re-insert `key3` and `key4` with new values.
    /// 3. `delete_range("key7", "key10")`.
    /// 4. Re-insert `key8` with new value.
    /// 5. Point-delete `key0` and `key1`.
    /// 6. `scan("key0", "key9\xff")`.
    ///
    /// # Expected behavior
    /// 17 records covering all raw versions in `(key ASC, lsn DESC)`
    /// order, including both old and new values for overwritten keys
    /// and both point-delete tombstones and their original puts.
    #[test]
    fn scan_with_mixed_operations() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        // Insert initial keys key0 through key9
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        // Delete range key2 through key5
        memtable
            .delete_range(b"key2".to_vec(), b"key6".to_vec())
            .unwrap();

        // Re-insert key3 and key4 with new values (should override the range tombstone)
        memtable
            .put(b"key3".to_vec(), b"new_value3".to_vec())
            .unwrap();
        memtable
            .put(b"key4".to_vec(), b"new_value4".to_vec())
            .unwrap();

        // Delete range key7 through key9
        memtable
            .delete_range(b"key7".to_vec(), b"key:".to_vec())
            .unwrap();

        // Insert key8 after range deletion
        memtable
            .put(b"key8".to_vec(), b"new_value8".to_vec())
            .unwrap();

        // Delete key0 and key1 individually
        memtable.delete(b"key0".to_vec()).unwrap();
        memtable.delete(b"key1".to_vec()).unwrap();

        // Scan the full range
        let results: Vec<_> = memtable.scan(b"key0", b"key9\xff").unwrap().collect();

        let expected = vec![
            Record::Delete {
                key: b"key0".to_vec(),
                lsn: 16,
                timestamp: 0,
            },
            Record::Put {
                key: b"key0".to_vec(),
                value: b"value0".to_vec(),
                lsn: 1,
                timestamp: 0,
            },
            Record::Delete {
                key: b"key1".to_vec(),
                lsn: 17,
                timestamp: 0,
            },
            Record::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                lsn: 2,
                timestamp: 0,
            },
            Record::RangeDelete {
                start: b"key2".to_vec(),
                end: b"key6".to_vec(),
                lsn: 11,
                timestamp: 0,
            },
            Record::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
                lsn: 3,
                timestamp: 0,
            },
            Record::Put {
                key: b"key3".to_vec(),
                value: b"new_value3".to_vec(),
                lsn: 12,
                timestamp: 0,
            },
            Record::Put {
                key: b"key3".to_vec(),
                value: b"value3".to_vec(),
                lsn: 4,
                timestamp: 0,
            },
            Record::Put {
                key: b"key4".to_vec(),
                value: b"new_value4".to_vec(),
                lsn: 13,
                timestamp: 0,
            },
            Record::Put {
                key: b"key4".to_vec(),
                value: b"value4".to_vec(),
                lsn: 5,
                timestamp: 0,
            },
            Record::Put {
                key: b"key5".to_vec(),
                value: b"value5".to_vec(),
                lsn: 6,
                timestamp: 0,
            },
            Record::Put {
                key: b"key6".to_vec(),
                value: b"value6".to_vec(),
                lsn: 7,
                timestamp: 0,
            },
            Record::RangeDelete {
                start: b"key7".to_vec(),
                end: b"key:".to_vec(),
                lsn: 14,
                timestamp: 0,
            },
            Record::Put {
                key: b"key7".to_vec(),
                value: b"value7".to_vec(),
                lsn: 8,
                timestamp: 0,
            },
            Record::Put {
                key: b"key8".to_vec(),
                value: b"new_value8".to_vec(),
                lsn: 15,
                timestamp: 0,
            },
            Record::Put {
                key: b"key8".to_vec(),
                value: b"value8".to_vec(),
                lsn: 9,
                timestamp: 0,
            },
            Record::Put {
                key: b"key9".to_vec(),
                value: b"value9".to_vec(),
                lsn: 10,
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
}
