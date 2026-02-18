//! Memtable basic operation tests.
//!
//! These tests verify the core `Memtable` API — put, get, delete,
//! overwrite, scan, `iter_for_flush()`, write-buffer overflow, and
//! WAL-based crash recovery.
//!
//! The memtable is an in-memory sorted structure backed by a WAL.
//! `get()` returns `MemtableGetResult::Put(val)`, `Delete`, or
//! `NotFound`. These tests exercise the API directly without the engine
//! layer.
//!
//! ## See also
//! - [`tests_frozen`] — `FrozenMemtable` API correctness
//! - [`tests_scan`] — raw multi-version scan output

#[cfg(test)]
mod tests {
    use crate::memtable::{Memtable, MemtableError, MemtableGetResult, Record};
    use tempfile::TempDir;
    use tracing::Level;
    use tracing_subscriber::fmt::Subscriber;

    fn init_tracing() {
        let _ = Subscriber::builder()
            .with_max_level(Level::TRACE)
            .try_init();
    }

    // ----------------------------------------------------------------
    // Put + Get
    // ----------------------------------------------------------------

    /// # Scenario
    /// A single put followed by a get returns the inserted value.
    ///
    /// # Starting environment
    /// Fresh memtable (1 KB buffer) — empty.
    ///
    /// # Actions
    /// 1. `put("key1", "value1")`.
    /// 2. `get("key1")`.
    ///
    /// # Expected behavior
    /// Returns `MemtableGetResult::Put("value1")`.
    #[test]
    fn put_and_get() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        memtable.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        let value = memtable.get(b"key1").unwrap();

        assert_eq!(value, MemtableGetResult::Put(b"value1".to_vec()));
    }

    // ----------------------------------------------------------------
    // Delete
    // ----------------------------------------------------------------

    /// # Scenario
    /// Deleting an existing key makes `get()` return `Delete`.
    ///
    /// # Starting environment
    /// Fresh memtable — empty.
    ///
    /// # Actions
    /// 1. `put("key1", "value1")`.
    /// 2. `delete("key1")`.
    /// 3. `get("key1")`.
    ///
    /// # Expected behavior
    /// Returns `MemtableGetResult::Delete` (tombstone present, not `NotFound`).
    #[test]
    fn delete_key() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        memtable.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        memtable.delete(b"key1".to_vec()).unwrap();

        let value = memtable.get(b"key1").unwrap();
        assert_eq!(value, MemtableGetResult::Delete);
    }

    // ----------------------------------------------------------------
    // iter_for_flush — produces all record types
    // ----------------------------------------------------------------

    /// # Scenario
    /// `iter_for_flush()` yields every record in the memtable —
    /// puts, point deletes, and range deletes — suitable for SSTable
    /// building. The memtable state is unchanged after iteration.
    ///
    /// # Starting environment
    /// Fresh memtable — empty.
    ///
    /// # Actions
    /// 1. Put keys 1, 2, 3, 8 + 4.
    /// 2. Delete keys 2, 9, 10.
    /// 3. Range-delete `[key5, key7)`, `[key11, key13)`, `[key15, key17)`.
    /// 4. Call `iter_for_flush()` and collect.
    /// 5. Verify memtable state is unchanged via `get()`.
    ///
    /// # Expected behavior
    /// - 10 records total: 4 surviving puts + 3 deletes + 3 range deletes.
    ///   (key2 was put then deleted → only the delete survives as the
    ///   latest version.)
    /// - Memtable contents are still readable after flush iteration.
    #[test]
    fn iter_for_flush() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        // Insert various operations - at least 3 of each type
        memtable.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        memtable.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        memtable.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();
        memtable.put(b"key8".to_vec(), b"value8".to_vec()).unwrap();

        memtable.delete(b"key2".to_vec()).unwrap();
        memtable.delete(b"key9".to_vec()).unwrap();
        memtable.delete(b"key10".to_vec()).unwrap();

        memtable
            .delete_range(b"key5".to_vec(), b"key7".to_vec())
            .unwrap();
        memtable
            .delete_range(b"key11".to_vec(), b"key13".to_vec())
            .unwrap();
        memtable
            .delete_range(b"key15".to_vec(), b"key17".to_vec())
            .unwrap();

        memtable.put(b"key4".to_vec(), b"value4".to_vec()).unwrap();

        // Get all records from flush iterator
        let flushed: Vec<_> = memtable.iter_for_flush().unwrap().collect();

        // Verify we have all operations (5 puts + 3 deletes + 3 range_deletes)
        assert_eq!(flushed.len(), 10);

        // Verify each record has correct key and value/tombstone
        let mut found_key1 = false;
        let mut found_key2_delete = false;
        let mut found_key3 = false;
        let mut found_key4 = false;
        let mut found_key8 = false;
        let mut found_key9_delete = false;
        let mut found_key10_delete = false;
        let mut found_range_delete_1 = false;
        let mut found_range_delete_2 = false;
        let mut found_range_delete_3 = false;

        for record in &flushed {
            match record {
                Record::Put { key, value, .. } => match key.as_slice() {
                    b"key1" => {
                        assert_eq!(value, b"value1");
                        found_key1 = true;
                    }
                    b"key3" => {
                        assert_eq!(value, b"value3");
                        found_key3 = true;
                    }
                    b"key4" => {
                        assert_eq!(value, b"value4");
                        found_key4 = true;
                    }
                    b"key8" => {
                        assert_eq!(value, b"value8");
                        found_key8 = true;
                    }
                    _ => panic!("Unexpected put key: {:?}", String::from_utf8_lossy(key)),
                },
                Record::Delete { key, .. } => match key.as_slice() {
                    b"key2" => found_key2_delete = true,
                    b"key9" => found_key9_delete = true,
                    b"key10" => found_key10_delete = true,
                    _ => panic!("Unexpected delete key: {:?}", String::from_utf8_lossy(key)),
                },
                Record::RangeDelete { start, .. } => match start.as_slice() {
                    b"key5" => found_range_delete_1 = true,
                    b"key11" => found_range_delete_2 = true,
                    b"key15" => found_range_delete_3 = true,
                    _ => panic!(
                        "Unexpected range delete start: {:?}",
                        String::from_utf8_lossy(start)
                    ),
                },
            }
        }

        assert!(found_key1, "key1 not found in flush");
        assert!(found_key2_delete, "key2 delete not found in flush");
        assert!(found_key3, "key3 not found in flush");
        assert!(found_key4, "key4 not found in flush");
        assert!(found_key8, "key8 not found in flush");
        assert!(found_key9_delete, "key9 delete not found in flush");
        assert!(found_key10_delete, "key10 delete not found in flush");
        assert!(found_range_delete_1, "delete_range 1 not found in flush");
        assert!(found_range_delete_2, "delete_range 2 not found in flush");
        assert!(found_range_delete_3, "delete_range 3 not found in flush");

        // Verify memtable state is unchanged after flush iteration
        assert_eq!(
            memtable.get(b"key1").unwrap(),
            MemtableGetResult::Put(b"value1".to_vec())
        );
        assert_eq!(memtable.get(b"key2").unwrap(), MemtableGetResult::Delete);
        assert_eq!(
            memtable.get(b"key3").unwrap(),
            MemtableGetResult::Put(b"value3".to_vec())
        );
        assert_eq!(
            memtable.get(b"key4").unwrap(),
            MemtableGetResult::Put(b"value4".to_vec())
        );
        assert_eq!(
            memtable.get(b"key8").unwrap(),
            MemtableGetResult::Put(b"value8".to_vec())
        );
        assert_eq!(memtable.get(b"key9").unwrap(), MemtableGetResult::Delete);
        assert_eq!(memtable.get(b"key10").unwrap(), MemtableGetResult::Delete);
    }

    // ----------------------------------------------------------------
    // Scan — basic range
    // ----------------------------------------------------------------

    /// # Scenario
    /// `scan(start, end)` returns records in `[start, end)` with correct
    /// keys, values, LSNs, and non-zero timestamps.
    ///
    /// # Starting environment
    /// Fresh memtable with 3 keys: `a`, `b`, `c`.
    ///
    /// # Actions
    /// 1. `scan("a", "c")` — start-inclusive, end-exclusive.
    ///
    /// # Expected behavior
    /// Returns 2 records for `a` and `b` (not `c`), each with correct
    /// LSN (1, 2) and `timestamp > 0`.
    #[test]
    fn scan_range() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        memtable.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        memtable.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        memtable.put(b"c".to_vec(), b"3".to_vec()).unwrap();

        let scanned: Vec<_> = memtable.scan(b"a", b"c").unwrap().collect();
        assert_eq!(scanned.len(), 2);

        // Put a
        match &scanned[0] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key, &b"a".to_vec());
                assert_eq!(value, &b"1".to_vec());
                assert_eq!(*lsn, 1);
                assert!(*timestamp > 0);
            }
            other => panic!("Expected Put(a), got {:?}", other),
        }

        match &scanned[1] {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                assert_eq!(key, &b"b".to_vec());
                assert_eq!(value, &b"2".to_vec());
                assert_eq!(*lsn, 2);
                assert!(*timestamp > 0);
            }
            other => panic!("Expected Put(b), got {:?}", other),
        }
    }

    // ----------------------------------------------------------------
    // Overwrite — latest value wins
    // ----------------------------------------------------------------

    /// # Scenario
    /// Overwriting a key makes `get()` return the latest value.
    ///
    /// # Starting environment
    /// Fresh memtable — empty.
    ///
    /// # Actions
    /// 1. `put("a", "1")`.
    /// 2. `put("a", "2")`.
    /// 3. `get("a")`.
    ///
    /// # Expected behavior
    /// Returns `Put("2")` — the latest version.
    #[test]
    fn multiple_versions() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        memtable.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        memtable.put(b"a".to_vec(), b"2".to_vec()).unwrap();

        let value = memtable.get(b"a").unwrap();
        assert_eq!(value, MemtableGetResult::Put(b"2".to_vec()));
    }

    // ----------------------------------------------------------------
    // Write-buffer overflow → FlushRequired
    // ----------------------------------------------------------------

    /// # Scenario
    /// Writing beyond the configured buffer size triggers
    /// `MemtableError::FlushRequired`.
    ///
    /// # Starting environment
    /// Fresh memtable with a tiny 16-byte write buffer.
    ///
    /// # Actions
    /// 1. `put("a", "1234567890")` — exceeds the 16-byte budget.
    ///
    /// # Expected behavior
    /// Returns `Err(MemtableError::FlushRequired)`.
    #[test]
    fn write_buffer_limit() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 16).unwrap();

        let res = memtable.put(b"a".to_vec(), b"1234567890".to_vec());
        assert!(matches!(res, Err(MemtableError::FlushRequired)));
    }

    // ----------------------------------------------------------------
    // WAL recovery — basic
    // ----------------------------------------------------------------

    /// # Scenario
    /// Dropping a memtable (without explicit close) and reopening it
    /// recovers the data from the WAL.
    ///
    /// # Starting environment
    /// Fresh memtable with one put.
    ///
    /// # Actions
    /// 1. `put("x", "y")`, drop memtable.
    /// 2. Reopen memtable from the same WAL path.
    /// 3. `get("x")`.
    ///
    /// # Expected behavior
    /// Returns `Put("y")` — data was recovered from the WAL.
    #[test]
    fn wal_recovery() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");

        {
            let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();
            memtable.put(b"x".to_vec(), b"y".to_vec()).unwrap();
        }

        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();
        let value = memtable.get(b"x").unwrap();
        assert_eq!(value, MemtableGetResult::Put(b"y".to_vec()));
    }

    // ----------------------------------------------------------------
    // WAL recovery — LSN continuity
    // ----------------------------------------------------------------

    /// # Scenario
    /// After WAL recovery the LSN counter resumes from where it left off,
    /// preventing LSN gaps or reuse.
    ///
    /// # Starting environment
    /// Memtable with two puts (`alpha`, `beta`).
    ///
    /// # Actions
    /// 1. `put("alpha", "value1")`, `put("beta", "value2")` → `max_lsn = 2`.
    /// 2. Drop memtable.
    /// 3. Reopen from same WAL → verify `max_lsn` is still 2.
    /// 4. Verify data is intact.
    /// 5. `put("gamma", "value3")` → verify `max_lsn = 3`.
    ///
    /// # Expected behavior
    /// LSN is restored to the pre-crash value; new writes continue
    /// from `max_lsn + 1`.
    #[test]
    fn recovery_from_wal_preserves_lsn() {
        let tmp_dir = TempDir::new().unwrap();
        let wal_path = tmp_dir.path().join("wal-000001.log");

        let memtable = Memtable::new(&wal_path, None, 1024 * 1024).unwrap();
        memtable.put(b"alpha".to_vec(), b"value1".to_vec()).unwrap();
        memtable.put(b"beta".to_vec(), b"value2".to_vec()).unwrap();
        let lsn_before = memtable.max_lsn();

        drop(memtable);

        let recovered = Memtable::new(&wal_path, None, 1024 * 1024).unwrap();
        let lsn_after = recovered.max_lsn();

        assert_eq!(lsn_before, lsn_after);

        assert_eq!(
            recovered.get(b"alpha").unwrap(),
            MemtableGetResult::Put(b"value1".to_vec())
        );
        assert_eq!(
            recovered.get(b"beta").unwrap(),
            MemtableGetResult::Put(b"value2".to_vec())
        );

        recovered
            .put(b"gamma".to_vec(), b"value3".to_vec())
            .unwrap();
        assert_eq!(recovered.max_lsn(), lsn_after + 1);
        assert_eq!(
            recovered.get(b"gamma").unwrap(),
            MemtableGetResult::Put(b"value3".to_vec())
        );
    }

    // ----------------------------------------------------------------
    // Empty memtable — get and scan
    // ----------------------------------------------------------------

    /// # Scenario
    /// Operations on an empty memtable return appropriate "not found"
    /// results.
    ///
    /// # Starting environment
    /// Fresh memtable — no writes.
    ///
    /// # Actions
    /// 1. `get("nonexistent")`.
    /// 2. `scan("a", "z")`.
    ///
    /// # Expected behavior
    /// - `get` returns `MemtableGetResult::NotFound`.
    /// - `scan` yields 0 records.
    #[test]
    fn empty_get_and_scan() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(path.to_str().unwrap(), None, 1024).unwrap();

        assert_eq!(
            memtable.get(b"nonexistent").unwrap(),
            MemtableGetResult::NotFound
        );
        assert_eq!(memtable.scan(b"a", b"z").unwrap().count(), 0);
    }
}
