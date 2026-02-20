//! WAL corruption detection tests.
//!
//! These tests verify that the WAL correctly detects and reports corruption
//! in both the file header and individual record frames. Corruption is
//! simulated by directly writing invalid bytes to the WAL file on disk.
//!
//! Coverage:
//! - Header checksum mismatch → `WalError::InvalidHeader`
//! - Record length field overwritten with huge value → `WalError::RecordTooLarge`
//! - Record data checksum mismatch → `WalError::ChecksumMismatch`
//! - Record data corruption mid-payload → `WalError::ChecksumMismatch`
//! - Partial replay: valid records before a corrupted final record
//!
//! ## See also
//! - [`tests_basic`] — basic append / replay / truncate cycle
//! - [`tests_rotation`] — file rotation and sequence validation

#[cfg(test)]
mod tests {
    use crate::wal::tests::helpers::*;
    use crate::wal::{Wal, WalError};
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Header corruption
    // ----------------------------------------------------------------

    /// # Scenario
    /// A single byte inside the WAL header is flipped, making the header
    /// CRC32 mismatch.
    ///
    /// # Starting environment
    /// A freshly opened WAL file (header already written).
    ///
    /// # Actions
    /// 1. Open a WAL, drop it.
    /// 2. Overwrite byte at offset 2 with `0x99`.
    /// 3. Attempt to reopen the same WAL file.
    ///
    /// # Expected behavior
    /// `Wal::open()` returns `WalError::InvalidHeader` with a message
    /// containing "Header checksum mismatch".
    #[test]
    fn corrupted_header_checksum() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let _wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();

        // Corrupt a single byte inside header bytes (not checksum).
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::Start(2)).unwrap();
        f.write_all(&[0x99]).unwrap();
        f.sync_all().unwrap();

        let err = Wal::<MemTableRecord>::open(&path, None).unwrap_err();
        assert!(matches!(err, WalError::InvalidHeader(_)));
        assert!(err.to_string().contains("header checksum mismatch"));
    }

    // ----------------------------------------------------------------
    // Record length corruption
    // ----------------------------------------------------------------

    /// # Scenario
    /// The length field of a record is overwritten with `0xFFFFFFFF`,
    /// causing the WAL to reject the record as too large.
    ///
    /// # Starting environment
    /// WAL file with one `MemTableRecord` appended.
    ///
    /// # Actions
    /// 1. Open WAL, append one record.
    /// 2. Overwrite the 4-byte length field of the first record with
    ///    `0xFFFFFFFF`.
    /// 3. Attempt to replay.
    ///
    /// # Expected behavior
    /// `replay_iter()` yields `WalError::RecordTooLarge`.
    #[test]
    fn corrupted_record_length() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let wal = Wal::open(&path, None).unwrap();

        let record = MemTableRecord {
            key: b"a".to_vec(),
            value: Some(b"v1".to_vec()),
            timestamp: 1,
            deleted: false,
        };
        wal.append(&record).unwrap();
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        // Overwrite length with very large value (0xFFFFFFFF)
        f.seek(SeekFrom::Start((WAL_HDR_SIZE + WAL_CRC32_SIZE) as u64))
            .unwrap();
        f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        f.sync_all().unwrap();

        let err = collect_iter(&wal).unwrap_err();
        assert!(matches!(err, WalError::RecordTooLarge(_)));
    }

    // ----------------------------------------------------------------
    // Record data checksum corruption
    // ----------------------------------------------------------------

    /// # Scenario
    /// The last few bytes of a record (near the trailing checksum) are
    /// overwritten, making the CRC32 mismatch.
    ///
    /// # Starting environment
    /// WAL file with one `ManifestRecord` appended.
    ///
    /// # Actions
    /// 1. Open WAL, append one `ManifestRecord`.
    /// 2. Overwrite the last 3 bytes of the file with `[0xAA, 0xBB, 0xCC]`.
    /// 3. Attempt to replay.
    ///
    /// # Expected behavior
    /// `replay_iter()` yields `WalError::ChecksumMismatch`.
    #[test]
    fn corrupted_record_data_checksum() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let wal = Wal::open(&path, None).unwrap();

        let record = ManifestRecord {
            id: 999,
            path: "/db/table-999".to_string(),
            creation_timestamp: 9999,
        };
        wal.append(&record).unwrap();
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::End(-3)).unwrap(); // corrupt last few bytes before checksum
        f.write_all(&[0xAA, 0xBB, 0xCC]).unwrap();
        f.sync_all().unwrap();

        let err = collect_iter(&wal).unwrap_err();
        assert!(matches!(err, WalError::ChecksumMismatch));
    }

    // ----------------------------------------------------------------
    // Record payload corruption
    // ----------------------------------------------------------------

    /// # Scenario
    /// Bytes inside a record's serialized payload are flipped, causing
    /// the CRC32 to fail even though the length field is valid.
    ///
    /// # Starting environment
    /// WAL file with two `MemTableRecord`s appended.
    ///
    /// # Actions
    /// 1. Append two records.
    /// 2. Overwrite 3 bytes inside the first record's payload
    ///    (offset = header + CRC + 5).
    /// 3. Attempt to replay.
    ///
    /// # Expected behavior
    /// `replay_iter()` yields `WalError::ChecksumMismatch` when reading
    /// the corrupted first record.
    #[test]
    fn corrupted_record_data() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let wal = Wal::open(&path, None).unwrap();

        let insert = vec![
            MemTableRecord {
                key: b"a".to_vec(),
                value: Some(b"v1".to_vec()),
                timestamp: 1,
                deleted: false,
            },
            MemTableRecord {
                key: b"b".to_vec(),
                value: None,
                timestamp: 2,
                deleted: true,
            },
        ];

        for record in &insert {
            wal.append(record).unwrap();
        }

        // Corrupt middle of file (inside record bytes)
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        // Skip header + checksum + first record length (4B) + some payload bytes
        // The first record is small, so we'll flip a few bytes within its serialized data.
        let corrupt_offset = (WAL_HDR_SIZE + WAL_CRC32_SIZE + 5) as u64;
        f.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        f.write_all(&[0xFF, 0x00, 0xEE]).unwrap();
        f.sync_all().unwrap();

        // Attempt replay
        let err = collect_iter(&wal).unwrap_err();
        assert!(matches!(err, WalError::ChecksumMismatch));
    }

    // ----------------------------------------------------------------
    // Partial replay — corruption in last record only
    // ----------------------------------------------------------------

    /// # Scenario
    /// Three records are valid except the last one whose checksum is
    /// corrupted. The WAL should replay the first two and then report
    /// the corruption.
    ///
    /// # Starting environment
    /// WAL file with three `ManifestRecord`s appended.
    ///
    /// # Actions
    /// 1. Append 3 records.
    /// 2. Corrupt the last 2 bytes of the file (inside the third
    ///    record's trailing checksum).
    /// 3. Manually iterate `replay_iter()`, collecting `Ok` records
    ///    and breaking on the first `Err`.
    ///
    /// # Expected behavior
    /// Two valid records are recovered; the third yields
    /// `WalError::ChecksumMismatch`.
    #[test]
    fn partial_replay_after_last_record_corrupted() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let wal = Wal::open(&path, None).unwrap();

        let records = vec![
            ManifestRecord {
                id: 100,
                path: "/db/table-100".to_string(),
                creation_timestamp: 1000,
            },
            ManifestRecord {
                id: 101,
                path: "/db/table-101".to_string(),
                creation_timestamp: 1001,
            },
            ManifestRecord {
                id: 102,
                path: "/db/table-102".to_string(),
                creation_timestamp: 1002,
            },
        ];

        for record in &records {
            wal.append(record).unwrap();
        }

        // Corrupt *last record's checksum* only
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::End(-2)).unwrap(); // last bytes of checksum
        f.write_all(&[0x99, 0x77]).unwrap();
        f.sync_all().unwrap();

        // Replay should read 2 valid records, then hit corruption
        let iter = wal.replay_iter().unwrap();

        let mut replayed = vec![];
        for res in iter {
            match res {
                Ok(record) => replayed.push(record),
                Err(WalError::ChecksumMismatch) => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        assert_eq!(replayed.len(), 2, "Only first two records should be valid");
        assert_eq!(replayed[0].path, "/db/table-100".to_string());
        assert_eq!(replayed[1].path, "/db/table-101".to_string());
    }
}
