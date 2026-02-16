//! Memtable edge-case and boundary-condition tests.
//!
//! These tests cover behaviors not exercised by the basic / frozen / scan
//! test suites — specifically LSN injection, empty and reversed range
//! deletes, write-buffer overflow during a write, and concurrent access.
//!
//! Coverage:
//! - `inject_max_lsn()` sets the LSN counter so subsequent writes
//!   continue from the injected value
//! - Reversed range-delete (`start > end`) — treated as a no-op / error
//! - Empty-key range-delete — returns `MemtableError::Internal`
//! - Concurrent put / get from multiple threads
//!
//! ## See also
//! - [`tests_basic`]  — core memtable API tests
//! - [`tests_frozen`] — FrozenMemtable correctness
//! - [`tests_scan`]   — raw multi-version scan output

#[cfg(test)]
mod tests {
    use crate::memtable::{Memtable, MemtableError, MemtableGetResult};
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // inject_max_lsn sets the LSN counter
    // ----------------------------------------------------------------

    /// # Scenario
    /// After calling `inject_max_lsn(100)`, the next write must receive
    /// LSN 101 (i.e. the counter is set to `lsn + 1`).
    ///
    /// # Starting environment
    /// Fresh memtable with default LSN counter (starts at 1).
    ///
    /// # Actions
    /// 1. `inject_max_lsn(100)`.
    /// 2. Verify `max_lsn() == 100`.
    /// 3. Put one key.
    /// 4. Check `max_lsn()` advanced to 101.
    ///
    /// # Expected behavior
    /// `max_lsn()` reflects the injected value before any write and
    /// increments normally after a write.
    #[test]
    fn inject_max_lsn_sets_counter() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        // Default counter starts at 0 (no writes yet)
        assert_eq!(memtable.max_lsn(), 0);

        // Inject recovered LSN
        memtable.inject_max_lsn(100);
        assert_eq!(memtable.max_lsn(), 100);

        // Next write gets LSN 101
        memtable.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        assert_eq!(memtable.max_lsn(), 101);
    }

    // ----------------------------------------------------------------
    // Empty-key range-delete rejected
    // ----------------------------------------------------------------

    /// # Scenario
    /// A range-delete with an empty start or end key is invalid and
    /// must be rejected.
    ///
    /// # Starting environment
    /// Fresh memtable.
    ///
    /// # Actions
    /// 1. `delete_range(b"", b"z")` — empty start key.
    /// 2. `delete_range(b"a", b"")` — empty end key.
    ///
    /// # Expected behavior
    /// Both calls return `MemtableError::Internal`.
    #[test]
    fn empty_key_range_delete_rejected() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        // Empty start key
        let err = memtable
            .delete_range(b"".to_vec(), b"z".to_vec())
            .unwrap_err();
        assert!(
            matches!(err, MemtableError::Internal(_)),
            "Expected Internal error for empty start key, got: {:?}",
            err
        );

        // Empty end key
        let err = memtable
            .delete_range(b"a".to_vec(), b"".to_vec())
            .unwrap_err();
        assert!(
            matches!(err, MemtableError::Internal(_)),
            "Expected Internal error for empty end key, got: {:?}",
            err
        );
    }

    // ----------------------------------------------------------------
    // Reversed range-delete (start > end)
    // ----------------------------------------------------------------

    /// # Scenario
    /// A range-delete where `start > end` (e.g. `"z".."a"`) should not
    /// hide any keys — the empty interval covers nothing.
    ///
    /// # Starting environment
    /// Memtable with 5 keys (`key0`–`key4`).
    ///
    /// # Actions
    /// 1. `delete_range(b"key4", b"key0")` — reversed bounds.
    /// 2. Query all 5 keys.
    ///
    /// # Expected behavior
    /// All 5 keys remain visible (the reversed range covers no keys).
    #[test]
    fn reversed_range_delete_is_noop() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 1024 * 1024).unwrap();

        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        // Reversed range — start > end
        memtable
            .delete_range(b"key4".to_vec(), b"key0".to_vec())
            .unwrap();

        // All keys should still be visible
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let result = memtable.get(&key).unwrap();
            assert!(
                matches!(result, MemtableGetResult::Put(_)),
                "key{} should still be visible, got {:?}",
                i,
                result
            );
        }
    }

    // ----------------------------------------------------------------
    // Write-buffer overflow mid-put
    // ----------------------------------------------------------------

    /// # Scenario
    /// A put that would push `approximate_size` beyond the configured
    /// `write_buffer_size` must be rejected with `FlushRequired`.
    ///
    /// # Starting environment
    /// Memtable with a tiny 128-byte write buffer.
    ///
    /// # Actions
    /// 1. Keep writing small records until `FlushRequired` is returned.
    ///
    /// # Expected behavior
    /// At least one write succeeds; the write that overflows returns
    /// `MemtableError::FlushRequired`.
    #[test]
    fn write_buffer_overflow_returns_flush_required() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Memtable::new(&path, None, 128).unwrap();

        let mut succeeded = 0;
        for i in 0..1000 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("val_{:04}", i).into_bytes();
            match memtable.put(key, value) {
                Ok(()) => succeeded += 1,
                Err(MemtableError::FlushRequired) => break,
                Err(other) => panic!("Unexpected error: {:?}", other),
            }
        }

        assert!(succeeded > 0, "At least one write should succeed");
        assert!(
            succeeded < 1000,
            "Buffer should overflow before 1000 writes"
        );
    }

    // ----------------------------------------------------------------
    // Concurrent put / get safety
    // ----------------------------------------------------------------

    /// # Scenario
    /// Multiple writer and reader threads operate on the same memtable
    /// concurrently. No panics, data races, or poisoned locks should occur.
    ///
    /// # Starting environment
    /// Fresh memtable wrapped in an `Arc`.
    ///
    /// # Actions
    /// 1. Spawn 4 writer threads, each performing 100 puts.
    /// 2. Spawn 2 reader threads, each performing 200 gets on random keys.
    /// 3. Join all threads.
    ///
    /// # Expected behavior
    /// All threads complete without panic. After joining, every key
    /// written is retrievable.
    #[test]
    fn concurrent_put_get_no_data_race() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Arc::new(Memtable::new(&path, None, 1024 * 1024).unwrap());

        let num_writers = 4;
        let writes_per_thread = 100;

        // Writer threads
        let writer_handles: Vec<_> = (0..num_writers)
            .map(|t| {
                let mt = Arc::clone(&memtable);
                thread::spawn(move || {
                    for i in 0..writes_per_thread {
                        let key = format!("t{}_k{:04}", t, i).into_bytes();
                        let val = format!("t{}_v{:04}", t, i).into_bytes();
                        mt.put(key, val).unwrap();
                    }
                })
            })
            .collect();

        // Reader threads (read while writes may still be happening)
        let reader_handles: Vec<_> = (0..2)
            .map(|_| {
                let mt = Arc::clone(&memtable);
                thread::spawn(move || {
                    for i in 0..200 {
                        let key = format!("t0_k{:04}", i % writes_per_thread).into_bytes();
                        let _ = mt.get(&key); // may or may not find key yet
                    }
                })
            })
            .collect();

        for h in writer_handles {
            h.join().unwrap();
        }
        for h in reader_handles {
            h.join().unwrap();
        }

        // After all writers finish, every key should be present
        for t in 0..num_writers {
            for i in 0..writes_per_thread {
                let key = format!("t{}_k{:04}", t, i).into_bytes();
                let result = memtable.get(&key).unwrap();
                assert!(
                    matches!(result, MemtableGetResult::Put(_)),
                    "t{}_k{:04} should exist after all writers join",
                    t,
                    i
                );
            }
        }
    }
}
