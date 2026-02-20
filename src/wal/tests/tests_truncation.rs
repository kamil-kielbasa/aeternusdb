//! WAL truncation recovery tests.
//!
//! These tests cover the most common real-world WAL corruption scenario:
//! a write that is interrupted mid-record, leaving the file truncated at
//! various points within the record frame.
//!
//! A record frame is `[4B len][N bytes payload][4B crc32]`. A crash can
//! truncate the file at any point within this structure:
//!
//! - **Partial length field** — only 1-3 bytes of the 4-byte length prefix
//! - **Partial payload** — length is readable but payload is incomplete
//! - **Missing checksum** — payload fully written but trailing CRC32 absent
//! - **Partial checksum** — only 1-3 bytes of the 4-byte CRC32
//!
//! In all cases the WAL must:
//!
//! 1. Recover all *complete* records written before the truncated one.
//! 2. Signal an error (not silently skip) for the incomplete trailing record.
//! 3. Not panic or corrupt internal state.
//!
//! ## See also
//! - [`tests_corruption`] — byte-flip corruption (different from truncation)
//! - [`tests_basic`] — happy-path append / replay / truncate cycle

#[cfg(test)]
mod tests {
    use crate::wal::tests::helpers::*;
    use crate::wal::{Wal, WalError};
    use std::fs::{self, OpenOptions};
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------

    /// Write N records to a WAL, then return the file size.
    fn write_records(path: &std::path::Path, count: usize) -> u64 {
        let wal: Wal<MemTableRecord> = Wal::open(path, None).unwrap();
        for i in 0..count {
            wal.append(&MemTableRecord {
                key: format!("key_{i:04}").into_bytes(),
                value: Some(format!("val_{i:04}").into_bytes()),
                timestamp: i as u64,
                deleted: false,
            })
            .unwrap();
        }
        drop(wal);
        fs::metadata(path).unwrap().len()
    }

    /// Truncate the file to the given size.
    fn truncate_file(path: &std::path::Path, size: u64) {
        let f = OpenOptions::new().write(true).open(path).unwrap();
        f.set_len(size).unwrap();
        f.sync_all().unwrap();
    }

    /// Replay all records from a freshly opened WAL, returning Ok records
    /// and the first error (if any).
    fn replay_results(path: &std::path::Path) -> (Vec<MemTableRecord>, Option<WalError>) {
        let wal: Wal<MemTableRecord> = Wal::open(path, None).unwrap();
        let iter = wal.replay_iter().unwrap();
        let mut ok_records = Vec::new();
        let mut first_err = None;
        for item in iter {
            match item {
                Ok(rec) => ok_records.push(rec),
                Err(e) => {
                    first_err = Some(e);
                    break;
                }
            }
        }
        (ok_records, first_err)
    }

    // ----------------------------------------------------------------
    // Tests
    // ----------------------------------------------------------------

    /// # Scenario
    /// WAL file is truncated to exactly the header+CRC size (no records).
    ///
    /// # Starting environment
    /// WAL with 3 records appended.
    ///
    /// # Actions
    /// 1. Write 3 records.
    /// 2. Truncate file to `WAL_HDR_SIZE + WAL_CRC32_SIZE` (header only).
    /// 3. Replay.
    ///
    /// # Expected behavior
    /// Zero records recovered; no error (clean empty WAL).
    #[test]
    fn truncated_to_header_only_yields_zero_records() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        write_records(&path, 3);

        let header_end = (WAL_HDR_SIZE + WAL_CRC32_SIZE) as u64;
        truncate_file(&path, header_end);

        let (records, err) = replay_results(&path);
        assert_eq!(records.len(), 0);
        assert!(err.is_none(), "Expected clean EOF, got: {err:?}");
    }

    /// # Scenario
    /// File is truncated mid-way through the length prefix of the first
    /// record (only 2 of the 4 length bytes written).
    ///
    /// # Starting environment
    /// WAL with 3 records.
    ///
    /// # Actions
    /// 1. Write 3 records.
    /// 2. Truncate to `header_end + 2` (partial length field).
    /// 3. Replay.
    ///
    /// # Expected behavior
    /// Zero valid records. Iterator signals EOF or I/O error for the
    /// partial read (no panic).
    #[test]
    fn truncated_mid_length_field() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        write_records(&path, 3);

        let header_end = (WAL_HDR_SIZE + WAL_CRC32_SIZE) as u64;
        truncate_file(&path, header_end + 2);

        let (records, _err) = replay_results(&path);
        // No complete records should be recovered.
        assert_eq!(records.len(), 0);
    }

    /// # Scenario
    /// File is truncated mid-way through the first record's payload
    /// (length field is intact, but payload is incomplete).
    ///
    /// # Starting environment
    /// WAL with 3 records.
    ///
    /// # Actions
    /// 1. Write 3 records.
    /// 2. Truncate to `header_end + 4 + 3` (length field + 3 payload bytes).
    /// 3. Replay.
    ///
    /// # Expected behavior
    /// Zero valid records. `WalError::UnexpectedEof` for the truncated read.
    #[test]
    fn truncated_mid_payload() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        write_records(&path, 3);

        let header_end = (WAL_HDR_SIZE + WAL_CRC32_SIZE) as u64;
        // 4 bytes for length prefix + 3 bytes into payload
        truncate_file(&path, header_end + 4 + 3);

        let (records, err) = replay_results(&path);
        assert_eq!(records.len(), 0);
        assert!(err.is_some(), "Expected UnexpectedEof error");
        assert!(
            matches!(err.unwrap(), WalError::UnexpectedEof),
            "Expected WalError::UnexpectedEof"
        );
    }

    /// # Scenario
    /// File is truncated so the last record's checksum is entirely missing.
    /// The payload is fully written but the trailing 4-byte CRC32 is absent.
    ///
    /// # Starting environment
    /// WAL with 3 records.
    ///
    /// # Actions
    /// 1. Write 3 records, note total file size.
    /// 2. Truncate by removing the last 4 bytes (CRC32 of record 3).
    /// 3. Replay.
    ///
    /// # Expected behavior
    /// First 2 records recovered intact. Third record yields
    /// `WalError::UnexpectedEof`.
    #[test]
    fn truncated_missing_checksum_on_last_record() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let full_size = write_records(&path, 3);

        // Remove the last 4 bytes (trailing CRC32 of the third record).
        truncate_file(&path, full_size - 4);

        let (records, err) = replay_results(&path);
        assert_eq!(records.len(), 2, "First two records should be recovered");
        assert!(err.is_some(), "Third record should yield an error");
        assert!(
            matches!(err.unwrap(), WalError::UnexpectedEof),
            "Expected UnexpectedEof for missing checksum"
        );
    }

    /// # Scenario
    /// File is truncated so only 2 of the 4 checksum bytes of the last
    /// record are present.
    ///
    /// # Starting environment
    /// WAL with 3 records.
    ///
    /// # Actions
    /// 1. Write 3 records, note total file size.
    /// 2. Truncate by removing the last 2 bytes (partial CRC32).
    /// 3. Replay.
    ///
    /// # Expected behavior
    /// First 2 records recovered. Third record yields
    /// `WalError::UnexpectedEof`.
    #[test]
    fn truncated_partial_checksum_on_last_record() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let full_size = write_records(&path, 3);

        // Remove the last 2 bytes (partial CRC32).
        truncate_file(&path, full_size - 2);

        let (records, err) = replay_results(&path);
        assert_eq!(records.len(), 2, "First two records should be recovered");
        assert!(err.is_some(), "Third record should yield an error");
        assert!(
            matches!(err.unwrap(), WalError::UnexpectedEof),
            "Expected UnexpectedEof for partial checksum"
        );
    }

    /// # Scenario
    /// Second of three records is truncated mid-payload. The first record
    /// should still be fully recoverable.
    ///
    /// # Starting environment
    /// WAL with 3 records.
    ///
    /// # Actions
    /// 1. Write 1 record, note file size → `size_after_1`.
    /// 2. Write 2 more records.
    /// 3. Truncate to `size_after_1 + 4 + 5` (into the 2nd record's payload).
    /// 4. Replay.
    ///
    /// # Expected behavior
    /// 1 valid record recovered. Second yields `WalError::UnexpectedEof`.
    #[test]
    fn truncated_second_record_first_survives() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");

        // Write 1 record and capture size.
        let size_after_1 = write_records(&path, 1);

        // Write 2 more records (WAL re-open appends).
        {
            let wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();
            for i in 1..3 {
                wal.append(&MemTableRecord {
                    key: format!("key_{i:04}").into_bytes(),
                    value: Some(format!("val_{i:04}").into_bytes()),
                    timestamp: i as u64,
                    deleted: false,
                })
                .unwrap();
            }
        }

        // Truncate into 2nd record's payload: length(4) + 5 payload bytes.
        truncate_file(&path, size_after_1 + 4 + 5);

        let (records, err) = replay_results(&path);
        assert_eq!(records.len(), 1, "Only the first record should survive");
        assert_eq!(records[0].key, b"key_0000");
        assert!(err.is_some());
        assert!(matches!(err.unwrap(), WalError::UnexpectedEof));
    }

    /// # Scenario
    /// A completely empty file (0 bytes) that has the `.log` extension.
    /// This simulates a crash immediately after `open()` creates the file
    /// but before the header is written.
    ///
    /// # Starting environment
    /// A 0-byte file at the WAL path.
    ///
    /// # Actions
    /// 1. Create a 0-byte file.
    /// 2. Open WAL (should write a fresh header).
    /// 3. Replay.
    ///
    /// # Expected behavior
    /// WAL opens successfully (writes a fresh header) and replay yields
    /// zero records.
    #[test]
    fn zero_length_file_opens_as_fresh_wal() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");

        // Create a 0-byte file.
        {
            let _ = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap();
        }

        // Opening should succeed (file is empty → fresh header written).
        let wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();
        let records: Vec<_> = wal
            .replay_iter()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 0);
    }

    /// # Scenario
    /// WAL file is truncated to a size smaller than the header (e.g., 5 bytes).
    /// This simulates a crash during initial header write.
    ///
    /// # Starting environment
    /// A valid WAL file.
    ///
    /// # Actions
    /// 1. Create a valid WAL (writes header).
    /// 2. Truncate to 5 bytes (partial header).
    /// 3. Attempt to open.
    ///
    /// # Expected behavior
    /// `Wal::open()` returns an error (cannot read full header).
    #[test]
    fn truncated_header_fails_to_open() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");

        // Create a valid WAL.
        write_records(&path, 1);

        // Truncate to partial header.
        truncate_file(&path, 5);

        let result = Wal::<MemTableRecord>::open(&path, None);
        assert!(result.is_err(), "Partial header should fail to open");
    }

    /// # Scenario
    /// Append to a WAL whose last record was truncated (simulating
    /// crash recovery followed by continued operation).
    ///
    /// # Starting environment
    /// WAL with 3 records, last one truncated.
    ///
    /// # Actions
    /// 1. Write 3 records.
    /// 2. Truncate (remove last record's checksum).
    /// 3. Reopen WAL and append a new record.
    /// 4. Replay all.
    ///
    /// # Expected behavior
    /// The first 2 original records + the new appended record are all
    /// recoverable (3 total). The truncated 3rd record is lost but the
    /// new append is at the file's tail end and is intact.
    ///
    /// Note: This behavior depends on the WAL's append mode — since it
    /// opens with O_APPEND, the new record is appended after the
    /// truncation point, and the truncated 3rd record's partial bytes
    /// remain in the middle. The WAL stops at the first corruption,
    /// so only 2 records are recoverable.
    #[test]
    fn append_after_truncation_recovers_prior_records() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let full_size = write_records(&path, 3);

        // Truncate: remove last record's CRC.
        truncate_file(&path, full_size - 4);

        // Reopen and append a new record.
        let wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();
        wal.append(&MemTableRecord {
            key: b"new_key".to_vec(),
            value: Some(b"new_val".to_vec()),
            timestamp: 999,
            deleted: false,
        })
        .unwrap();
        drop(wal);

        // Replay: the WAL stops at the corrupted 3rd record.
        // The new appended record comes AFTER the corruption in the byte
        // stream, so it is not reachable.
        let (records, err) = replay_results(&path);
        assert_eq!(records.len(), 2, "Only first two intact records survive");
        assert!(err.is_some(), "Truncated 3rd record should error");
    }
}
