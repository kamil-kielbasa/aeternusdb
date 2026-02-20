//! WAL edge-case and boundary-condition tests.
//!
//! These tests cover scenarios that fall outside happy-path append/replay
//! and corruption detection — specifically the enforcement of limits,
//! behaviour on empty state, concurrent access, and filesystem error paths.
//!
//! Coverage:
//! - `max_record_size` enforcement (append rejected with `RecordTooLarge`)
//! - Open on a path whose parent directory does not exist (I/O error)
//! - Empty WAL replay (zero records appended → iterator yields nothing)
//! - Concurrent multi-threaded append safety
//!
//! ## See also
//! - [`tests_basic`]      — basic append / replay / truncate cycle
//! - [`tests_corruption`] — corruption detection
//! - [`tests_rotation`]   — file rotation and sequence validation

#[cfg(test)]
mod tests {
    use crate::wal::tests::helpers::*;
    use crate::wal::{Wal, WalError};
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // max_record_size enforcement
    // ----------------------------------------------------------------

    /// # Scenario
    /// Appending a record whose serialized size exceeds the configured
    /// `max_record_size` must be rejected.
    ///
    /// # Starting environment
    /// WAL opened with `max_record_size = 32` bytes.
    ///
    /// # Actions
    /// 1. Append a small record (fits within 32 bytes) — should succeed.
    /// 2. Append a record with a large `key` field (> 32 bytes) — must fail.
    ///
    /// # Expected behavior
    /// The first append succeeds; the second returns
    /// `WalError::RecordTooLarge`.
    #[test]
    fn max_record_size_rejects_oversized_record() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");

        // Open with a very small max_record_size limit
        let wal: Wal<MemTableRecord> = Wal::open(&path, Some(32)).unwrap();

        // Small record — should fit within 32 bytes
        let small = MemTableRecord {
            key: b"k".to_vec(),
            value: Some(b"v".to_vec()),
            timestamp: 1,
            deleted: false,
        };
        wal.append(&small).unwrap();

        // Large record — exceeds 32-byte limit
        let large = MemTableRecord {
            key: vec![b'X'; 100],
            value: Some(vec![b'Y'; 100]),
            timestamp: 2,
            deleted: false,
        };
        let err = wal.append(&large).unwrap_err();
        assert!(
            matches!(err, WalError::RecordTooLarge(_)),
            "Expected RecordTooLarge, got: {:?}",
            err
        );

        // Verify the WAL still contains only the first record
        let records = collect_iter(&wal).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"k");
    }

    // ----------------------------------------------------------------
    // Open on nonexistent parent directory
    // ----------------------------------------------------------------

    /// # Scenario
    /// Opening a WAL at a path whose parent directory does not exist
    /// must return an I/O error.
    ///
    /// # Starting environment
    /// No directory at `/tmp/.../nonexistent_dir/`.
    ///
    /// # Actions
    /// 1. `Wal::open("/tmp/.../nonexistent_dir/000000.log")`.
    ///
    /// # Expected behavior
    /// Returns `WalError::Io` (directory not found).
    #[test]
    fn open_nonexistent_directory_fails() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let bad_path = tmp.path().join("nonexistent_dir").join("000000.log");

        let result = Wal::<MemTableRecord>::open(&bad_path, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, WalError::Io(_)),
            "Expected Io error, got: {:?}",
            err
        );
    }

    // ----------------------------------------------------------------
    // Empty WAL replay
    // ----------------------------------------------------------------

    /// # Scenario
    /// A WAL that was opened but never appended to must produce zero
    /// records on replay.
    ///
    /// # Starting environment
    /// Freshly created WAL (header written, no records).
    ///
    /// # Actions
    /// 1. `replay_iter()` and collect all items.
    ///
    /// # Expected behavior
    /// Iterator yields zero items.
    #[test]
    fn empty_wal_replay_yields_nothing() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");

        let wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();

        let records = collect_iter(&wal).unwrap();
        assert!(
            records.is_empty(),
            "Expected 0 records, got {}",
            records.len()
        );
    }

    // ----------------------------------------------------------------
    // Concurrent append safety
    // ----------------------------------------------------------------

    /// # Scenario
    /// Multiple threads append records to the same WAL concurrently.
    /// All records must survive and be replayed without corruption.
    ///
    /// # Starting environment
    /// Fresh WAL wrapped in an `Arc` for sharing.
    ///
    /// # Actions
    /// 1. Spawn 4 threads, each appending 50 records.
    /// 2. Join all threads.
    /// 3. Replay the WAL.
    ///
    /// # Expected behavior
    /// Exactly 200 records are replayed (4 × 50), all with valid data
    /// (no checksum errors, no corruption).
    #[test]
    fn concurrent_append_all_records_survive() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");

        let wal: Arc<Wal<MemTableRecord>> = Arc::new(Wal::open(&path, None).unwrap());

        let num_threads = 4;
        let records_per_thread = 50;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let wal_clone = Arc::clone(&wal);
                thread::spawn(move || {
                    for i in 0..records_per_thread {
                        let rec = MemTableRecord {
                            key: format!("t{}_k{}", t, i).into_bytes(),
                            value: Some(format!("t{}_v{}", t, i).into_bytes()),
                            timestamp: (t * 1000 + i) as u64,
                            deleted: false,
                        };
                        wal_clone.append(&rec).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let records = collect_iter(&wal).unwrap();
        assert_eq!(
            records.len(),
            num_threads * records_per_thread,
            "Expected {} records, got {}",
            num_threads * records_per_thread,
            records.len()
        );
    }
}
