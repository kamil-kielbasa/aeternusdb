//! Coverage tests for engine utility types.
//!
//! Exercises:
//! - `PointEntry::new` / `PointEntry::new_delete` constructors
//! - `RangeTombstone::new` constructor
//! - `Record` `Eq` / `Ord` / `PartialOrd` trait impls
//! - `Record` `Encode` / `Decode` round-trips (all three variants incl. RangeDelete)
//! - `RangeTombstone` `Encode` / `Decode` round-trip
//! - Invalid tag decode error path
//! - `Record::into_entry` for all variants

#[cfg(test)]
mod tests {
    use crate::encoding::{self, Decode, Encode};
    use crate::engine::utils::{
        MergeIterator, PointEntry, RangeTombstone, Record, RecordEntry, record_cmp,
    };
    use std::cmp::Ordering;

    // ----------------------------------------------------------------
    // PointEntry constructors
    // ----------------------------------------------------------------

    #[test]
    fn point_entry_new_creates_put() {
        let pe = PointEntry::new(b"hello".to_vec(), b"world".to_vec(), 5, 100);
        assert_eq!(pe.key, b"hello");
        assert_eq!(pe.value, Some(b"world".to_vec()));
        assert_eq!(pe.lsn, 5);
        assert_eq!(pe.timestamp, 100);
    }

    #[test]
    fn point_entry_new_accepts_slices() {
        let pe = PointEntry::new(b"k", b"v", 1, 2);
        assert_eq!(pe.key, b"k");
        assert_eq!(pe.value, Some(b"v".to_vec()));
    }

    #[test]
    fn point_entry_new_delete_creates_tombstone() {
        let pe = PointEntry::new_delete(b"gone".to_vec(), 10, 200);
        assert_eq!(pe.key, b"gone");
        assert!(pe.value.is_none());
        assert_eq!(pe.lsn, 10);
        assert_eq!(pe.timestamp, 200);
    }

    // ----------------------------------------------------------------
    // RangeTombstone constructor
    // ----------------------------------------------------------------

    #[test]
    fn range_tombstone_new() {
        let rt = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 42, 999);
        assert_eq!(rt.start, b"a");
        assert_eq!(rt.end, b"z");
        assert_eq!(rt.lsn, 42);
        assert_eq!(rt.timestamp, 999);
    }

    #[test]
    fn range_tombstone_new_accepts_slices() {
        let rt = RangeTombstone::new(b"start", b"end", 1, 2);
        assert_eq!(rt.start, b"start");
        assert_eq!(rt.end, b"end");
    }

    // ----------------------------------------------------------------
    // Record Eq / Ord
    // ----------------------------------------------------------------

    #[test]
    fn record_eq_same_key_and_lsn() {
        let a = Record::Put {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
            lsn: 1,
            timestamp: 100,
        };
        let b = Record::Delete {
            key: b"k".to_vec(),
            lsn: 1,
            timestamp: 200,
        };
        // Eq compares only key + LSN, not variant or value
        assert_eq!(a, b);
    }

    #[test]
    fn record_ne_different_lsn() {
        let a = Record::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lsn: 1,
            timestamp: 100,
        };
        let b = Record::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lsn: 2,
            timestamp: 100,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn record_ord_key_ascending() {
        let a = Record::Put {
            key: b"a".to_vec(),
            value: b"v".to_vec(),
            lsn: 1,
            timestamp: 0,
        };
        let b = Record::Put {
            key: b"b".to_vec(),
            value: b"v".to_vec(),
            lsn: 1,
            timestamp: 0,
        };
        assert_eq!(a.cmp(&b), Ordering::Less);
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));
    }

    #[test]
    fn record_ord_lsn_descending_for_same_key() {
        let older = Record::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lsn: 1,
            timestamp: 0,
        };
        let newer = Record::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lsn: 5,
            timestamp: 0,
        };
        // Higher LSN should sort FIRST (less) for same key
        assert_eq!(newer.cmp(&older), Ordering::Less);
    }

    #[test]
    fn record_cmp_delegates_to_ord() {
        let a = Record::Delete {
            key: b"x".to_vec(),
            lsn: 3,
            timestamp: 0,
        };
        let b = Record::Delete {
            key: b"x".to_vec(),
            lsn: 1,
            timestamp: 0,
        };
        assert_eq!(record_cmp(&a, &b), a.cmp(&b));
    }

    // ----------------------------------------------------------------
    // Record::into_entry
    // ----------------------------------------------------------------

    #[test]
    fn into_entry_put() {
        let r = Record::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lsn: 1,
            timestamp: 10,
        };
        match r.into_entry() {
            RecordEntry::Point(pe) => {
                assert_eq!(pe.key, b"k");
                assert_eq!(pe.value, Some(b"v".to_vec()));
                assert_eq!(pe.lsn, 1);
            }
            RecordEntry::Range(_) => panic!("expected Point"),
        }
    }

    #[test]
    fn into_entry_delete() {
        let r = Record::Delete {
            key: b"k".to_vec(),
            lsn: 2,
            timestamp: 20,
        };
        match r.into_entry() {
            RecordEntry::Point(pe) => {
                assert_eq!(pe.key, b"k");
                assert!(pe.value.is_none());
            }
            RecordEntry::Range(_) => panic!("expected Point"),
        }
    }

    #[test]
    fn into_entry_range_delete() {
        let r = Record::RangeDelete {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            lsn: 3,
            timestamp: 30,
        };
        match r.into_entry() {
            RecordEntry::Range(rt) => {
                assert_eq!(rt.start, b"a");
                assert_eq!(rt.end, b"z");
                assert_eq!(rt.lsn, 3);
            }
            RecordEntry::Point(_) => panic!("expected Range"),
        }
    }

    // ----------------------------------------------------------------
    // Record Encode / Decode round-trips
    // ----------------------------------------------------------------

    #[test]
    fn encode_decode_put() {
        let original = Record::Put {
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
            lsn: 10,
            timestamp: 1000,
        };
        let bytes = encoding::encode_to_vec(&original).unwrap();
        let (decoded, _) = Record::decode_from(&bytes).unwrap();
        assert_eq!(original, decoded);
        // Also verify fields directly since Eq only checks key + LSN
        if let Record::Put {
            key,
            value,
            lsn,
            timestamp,
        } = &decoded
        {
            assert_eq!(key, b"key1");
            assert_eq!(value, b"val1");
            assert_eq!(*lsn, 10);
            assert_eq!(*timestamp, 1000);
        } else {
            panic!("expected Put");
        }
    }

    #[test]
    fn encode_decode_delete() {
        let original = Record::Delete {
            key: b"del_key".to_vec(),
            lsn: 20,
            timestamp: 2000,
        };
        let bytes = encoding::encode_to_vec(&original).unwrap();
        let (decoded, _) = Record::decode_from(&bytes).unwrap();
        assert_eq!(original, decoded);
        if let Record::Delete {
            key,
            lsn,
            timestamp,
        } = &decoded
        {
            assert_eq!(key, b"del_key");
            assert_eq!(*lsn, 20);
            assert_eq!(*timestamp, 2000);
        } else {
            panic!("expected Delete");
        }
    }

    #[test]
    fn encode_decode_range_delete() {
        let original = Record::RangeDelete {
            start: b"abc".to_vec(),
            end: b"xyz".to_vec(),
            lsn: 30,
            timestamp: 3000,
        };
        let bytes = encoding::encode_to_vec(&original).unwrap();
        let (decoded, _) = Record::decode_from(&bytes).unwrap();
        // Eq checks key (start) + LSN
        assert_eq!(original, decoded);
        if let Record::RangeDelete {
            start,
            end,
            lsn,
            timestamp,
        } = &decoded
        {
            assert_eq!(start, b"abc");
            assert_eq!(end, b"xyz");
            assert_eq!(*lsn, 30);
            assert_eq!(*timestamp, 3000);
        } else {
            panic!("expected RangeDelete");
        }
    }

    #[test]
    fn decode_invalid_tag_returns_error() {
        // Encode a valid record, then corrupt the tag byte
        let valid = Record::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lsn: 1,
            timestamp: 1,
        };
        let mut bytes = encoding::encode_to_vec(&valid).unwrap();
        // First 4 bytes are the tag (u32 LE) — set to 99
        bytes[0] = 99;
        bytes[1] = 0;
        bytes[2] = 0;
        bytes[3] = 0;
        let result = Record::decode_from(&bytes);
        assert!(result.is_err(), "should return error for invalid tag");
    }

    // ----------------------------------------------------------------
    // RangeTombstone Encode / Decode round-trip
    // ----------------------------------------------------------------

    #[test]
    fn encode_decode_range_tombstone() {
        let original = RangeTombstone {
            start: b"from".to_vec(),
            end: b"to".to_vec(),
            lsn: 55,
            timestamp: 5500,
        };
        let mut buf = Vec::new();
        original.encode_to(&mut buf).unwrap();
        let (decoded, consumed) = RangeTombstone::decode_from(&buf).unwrap();
        assert_eq!(decoded.start, b"from");
        assert_eq!(decoded.end, b"to");
        assert_eq!(decoded.lsn, 55);
        assert_eq!(decoded.timestamp, 5500);
        assert_eq!(consumed, buf.len());
    }

    // ----------------------------------------------------------------
    // Record accessors
    // ----------------------------------------------------------------

    #[test]
    fn record_accessors_range_delete() {
        let r = Record::RangeDelete {
            start: b"s".to_vec(),
            end: b"e".to_vec(),
            lsn: 7,
            timestamp: 77,
        };
        assert_eq!(r.key(), b"s"); // start key for RangeDelete
        assert_eq!(r.lsn(), 7);
        assert_eq!(r.timestamp(), 77);
    }

    // ----------------------------------------------------------------
    // MergeIterator — empty input
    // ----------------------------------------------------------------

    #[test]
    fn merge_iterator_empty() {
        let iters: Vec<Box<dyn Iterator<Item = Record>>> = vec![];
        let mut merge = MergeIterator::new(iters);
        assert!(merge.next().is_none());
    }
}
