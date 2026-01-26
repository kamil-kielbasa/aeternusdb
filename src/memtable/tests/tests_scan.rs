#[cfg(test)]
mod scan_tests {
    use crate::memtable::{Memtable, Record};
    use tempfile::TempDir;

    #[test]
    fn test_scan_full_range() {
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

    #[test]
    fn test_scan_partial_range() {
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

    #[test]
    fn test_scan_with_deletions() {
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

        let expected = vec![
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
                        timestamp: ets,
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
                        timestamp: ets,
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

    #[test]
    fn test_scan_empty_memtable() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        let results: Vec<_> = memtable.scan(b"key0", b"key9").unwrap().collect();
        assert!(results.is_empty());
    }

    #[test]
    fn test_scan_no_matching_keys() {
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

    #[test]
    fn test_scan_with_range_tombstones() {
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
        let results: Vec<_> = memtable.scan(b"key0", b"key5\xff").unwrap().collect();

        let expected = vec![
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
                        timestamp: ets,
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
                        timestamp: ets,
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

    #[test]
    fn test_scan_with_mixed_operations() {
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
            .delete_range(b"key7".to_vec(), b"key10".to_vec())
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
                end: b"key10".to_vec(),
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
                        timestamp: ets,
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
                        timestamp: ets,
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
                        timestamp: ets,
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
