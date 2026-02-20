//! Coverage tests for memtable internals.
//!
//! Targets code paths not exercised by the main test suites:
//! - `MemtablePointEntry` accessor methods (`lsn`, `timestamp`, `is_delete`, `value`)
//! - `MemtablePointEntry` `Encode` / `Decode` round-trips (both variants + invalid tag)
//! - `ReadMemtable` trait implementations for `Memtable` and `FrozenMemtable`
//! - `FrozenMemtable::wal_seq()` delegation
//! - `HexKey` Display (short and long keys)

#[cfg(test)]
mod tests {
    use crate::encoding::{Decode, Encode};
    use crate::memtable::{
        FrozenMemtable, Memtable, MemtableGetResult, MemtablePointEntry, ReadMemtable,
    };
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // MemtablePointEntry accessors
    // ----------------------------------------------------------------

    #[test]
    fn point_entry_put_accessors() {
        let entry = MemtablePointEntry::Put {
            value: b"hello".to_vec(),
            timestamp: 1000,
            lsn: 5,
        };
        assert_eq!(entry.lsn(), 5);
        assert_eq!(entry.timestamp(), 1000);
        assert!(!entry.is_delete());
        assert_eq!(entry.value(), Some(b"hello".as_slice()));
    }

    #[test]
    fn point_entry_delete_accessors() {
        let entry = MemtablePointEntry::Delete {
            timestamp: 2000,
            lsn: 10,
        };
        assert_eq!(entry.lsn(), 10);
        assert_eq!(entry.timestamp(), 2000);
        assert!(entry.is_delete());
        assert_eq!(entry.value(), None);
    }

    // ----------------------------------------------------------------
    // MemtablePointEntry Encode / Decode
    // ----------------------------------------------------------------

    #[test]
    fn encode_decode_put_round_trip() {
        let original = MemtablePointEntry::Put {
            value: b"data".to_vec(),
            timestamp: 42,
            lsn: 7,
        };
        let mut buf = Vec::new();
        original.encode_to(&mut buf).unwrap();

        let (decoded, consumed) = MemtablePointEntry::decode_from(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn encode_decode_delete_round_trip() {
        let original = MemtablePointEntry::Delete {
            timestamp: 99,
            lsn: 3,
        };
        let mut buf = Vec::new();
        original.encode_to(&mut buf).unwrap();

        let (decoded, consumed) = MemtablePointEntry::decode_from(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn decode_invalid_tag_returns_error() {
        // Tag byte = 0xFF, not 0 (Put) or 1 (Delete)
        let invalid = vec![0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = MemtablePointEntry::decode_from(&invalid);
        assert!(result.is_err());
    }

    // ----------------------------------------------------------------
    // ReadMemtable trait impl — Memtable
    // ----------------------------------------------------------------

    #[test]
    fn read_memtable_trait_memtable() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mt = Memtable::new(path.to_str().unwrap(), None, 4096).unwrap();

        mt.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        mt.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
        mt.delete(b"k3".to_vec()).unwrap();

        // Use the trait reference
        let reader: &dyn ReadMemtable = &mt;

        // get()
        assert_eq!(
            reader.get(b"k1").unwrap(),
            MemtableGetResult::Put(b"v1".to_vec())
        );
        assert_eq!(reader.get(b"k3").unwrap(), MemtableGetResult::Delete);
        assert_eq!(reader.get(b"missing").unwrap(), MemtableGetResult::NotFound);

        // scan()
        let records: Vec<_> = reader.scan(b"k1", b"k3").unwrap().collect();
        assert_eq!(records.len(), 2); // k1 and k2

        // max_lsn()
        assert!(reader.max_lsn().is_some());
        assert!(reader.max_lsn().unwrap() >= 3);
    }

    // ----------------------------------------------------------------
    // ReadMemtable trait impl — FrozenMemtable
    // ----------------------------------------------------------------

    #[test]
    fn read_memtable_trait_frozen() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mt = Memtable::new(path.to_str().unwrap(), None, 4096).unwrap();

        mt.put(b"fk1".to_vec(), b"fv1".to_vec()).unwrap();
        mt.put(b"fk2".to_vec(), b"fv2".to_vec()).unwrap();

        let frozen = FrozenMemtable::new(mt);

        let reader: &dyn ReadMemtable = &frozen;

        // get()
        assert_eq!(
            reader.get(b"fk1").unwrap(),
            MemtableGetResult::Put(b"fv1".to_vec())
        );

        // scan()
        let records: Vec<_> = reader.scan(b"fk1", b"fk3").unwrap().collect();
        assert_eq!(records.len(), 2);

        // max_lsn()
        assert!(reader.max_lsn().is_some());
    }

    // ----------------------------------------------------------------
    // FrozenMemtable::wal_seq()
    // ----------------------------------------------------------------

    #[test]
    fn frozen_memtable_wal_seq() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mt = Memtable::new(path.to_str().unwrap(), None, 4096).unwrap();

        // Memtable WAL should have seq 0 (first file)
        let seq = mt.wal_seq();

        let frozen = FrozenMemtable::new(mt);
        assert_eq!(frozen.wal_seq(), seq);
    }

    // ----------------------------------------------------------------
    // HexKey Display
    // ----------------------------------------------------------------

    #[test]
    fn hex_key_short() {
        // HexKey is private, but we can trigger it through tracing.
        // Instead, test it indirectly via put/get with tracing enabled.
        // The key is ≤ 32 bytes.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mt = Memtable::new(path.to_str().unwrap(), None, 4096).unwrap();

        // Short key — exercises the ≤32 byte path in HexKey::fmt
        mt.put(b"short".to_vec(), b"v".to_vec()).unwrap();
        let _ = mt.get(b"short").unwrap();
    }

    #[test]
    fn hex_key_long() {
        // Key > 32 bytes exercises the truncated HexKey::fmt path
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mt = Memtable::new(path.to_str().unwrap(), None, 4096).unwrap();

        let long_key = vec![0xAB; 64]; // 64 bytes > 32
        mt.put(long_key.clone(), b"v".to_vec()).unwrap();
        let _ = mt.get(&long_key).unwrap();
    }

    // ----------------------------------------------------------------
    // FrozenMemtable::creation_timestamp()
    // ----------------------------------------------------------------

    #[test]
    fn frozen_creation_timestamp() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mt = Memtable::new(path.to_str().unwrap(), None, 4096).unwrap();
        let frozen = FrozenMemtable::new(mt);
        assert!(frozen.creation_timestamp() > 0);
    }
}
