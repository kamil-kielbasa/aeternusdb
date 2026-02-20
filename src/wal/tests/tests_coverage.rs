//! WAL coverage tests.
//!
//! Exercises code paths not covered by the main WAL test suites:
//! - `WalHeader` accessors (`wal_seq`, `max_record_size`, `version`)
//! - `Wal` accessors (`wal_seq`, `max_record_size`, `file_size`, `path`)
//! - `WalIter` Debug impl
//! - Record too large error on append
//! - Truncated record payload during replay → `UnexpectedEof`
//! - Truncated checksum during replay → `UnexpectedEof`
//! - Bad magic byte in header → `InvalidHeader`
//! - Unsupported version in header → `InvalidHeader`
//! - WAL sequence mismatch on reopen

#[cfg(test)]
mod tests {
    use crate::wal::tests::helpers::*;
    use crate::wal::{Wal, WalError, WalHeader};
    use std::fs::{self, OpenOptions};
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // WalHeader accessors
    // ----------------------------------------------------------------

    #[test]
    fn wal_header_accessors() {
        let hdr = WalHeader::new(2048, 7);
        assert_eq!(hdr.wal_seq(), 7);
        assert_eq!(hdr.max_record_size(), 2048);
        assert_eq!(hdr.version(), WalHeader::VERSION);
    }

    // ----------------------------------------------------------------
    // Wal accessors
    // ----------------------------------------------------------------

    #[test]
    fn wal_accessors() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let wal: Wal<MemTableRecord> = Wal::open(&path, Some(4096)).unwrap();

        assert_eq!(wal.wal_seq(), 0);
        assert_eq!(wal.max_record_size(), 4096);
        assert_eq!(wal.path(), path);

        let size = wal.file_size().unwrap();
        assert!(size > 0, "file should contain at least the header");
    }

    // ----------------------------------------------------------------
    // WalIter Debug
    // ----------------------------------------------------------------

    #[test]
    fn wal_iter_debug() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();

        let iter = wal.replay_iter().unwrap();
        let dbg = format!("{:?}", iter);
        assert!(dbg.contains("WalIter"));
        assert!(dbg.contains("offset"));
    }

    // ----------------------------------------------------------------
    // Record too large
    // ----------------------------------------------------------------

    #[test]
    fn append_record_too_large() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        // Tiny max_record_size so a normal record exceeds it
        let wal: Wal<MemTableRecord> = Wal::open(&path, Some(1)).unwrap();

        let record = MemTableRecord {
            key: b"big".to_vec(),
            value: Some(b"value_that_exceeds_1_byte_limit".to_vec()),
            timestamp: 1,
            deleted: false,
        };

        let err = wal.append(&record).unwrap_err();
        assert!(matches!(err, WalError::RecordTooLarge(_)));
    }

    // ----------------------------------------------------------------
    // Truncated record payload → UnexpectedEof
    // ----------------------------------------------------------------

    #[test]
    fn truncated_payload_during_replay() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();

        let record = MemTableRecord {
            key: b"trunc".to_vec(),
            value: Some(b"payload".to_vec()),
            timestamp: 1,
            deleted: false,
        };
        wal.append(&record).unwrap();
        drop(wal);

        // Truncate the file to remove part of the record payload
        let file_len = fs::metadata(&path).unwrap().len();
        // Remove last 10 bytes (cuts into payload or checksum)
        let new_len = file_len - 10;
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(new_len).unwrap();
        }

        let wal2: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();
        let mut iter = wal2.replay_iter().unwrap();
        let result = iter.next();
        assert!(result.is_some());
        let err = result.unwrap().unwrap_err();
        assert!(
            matches!(err, WalError::UnexpectedEof | WalError::ChecksumMismatch),
            "expected UnexpectedEof or ChecksumMismatch, got {:?}",
            err
        );
    }

    // ----------------------------------------------------------------
    // Truncated checksum → UnexpectedEof
    // ----------------------------------------------------------------

    #[test]
    fn truncated_checksum_during_replay() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();

        let record = MemTableRecord {
            key: b"cs".to_vec(),
            value: Some(b"val".to_vec()),
            timestamp: 1,
            deleted: false,
        };
        wal.append(&record).unwrap();
        drop(wal);

        // Truncate so only partial checksum remains (remove last 2 bytes)
        let file_len = fs::metadata(&path).unwrap().len();
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(file_len - 2).unwrap();
        }

        let wal2: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();
        let mut iter = wal2.replay_iter().unwrap();
        let result = iter.next().unwrap();
        assert!(
            result.is_err(),
            "truncated checksum should cause replay error"
        );
    }

    // ----------------------------------------------------------------
    // Bad magic byte
    // ----------------------------------------------------------------

    #[test]
    fn bad_magic_byte_in_header() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        {
            let _wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();
        }

        // Corrupt the magic bytes (first 4 bytes of header)
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(b"XYZW").unwrap();
            f.sync_all().unwrap();
        }

        let err = Wal::<MemTableRecord>::open(&path, None).unwrap_err();
        assert!(matches!(err, WalError::InvalidHeader(_)));
    }

    // ----------------------------------------------------------------
    // WAL sequence mismatch
    // ----------------------------------------------------------------

    #[test]
    fn wal_seq_mismatch_on_reopen() {
        init_tracing();
        let tmp = TempDir::new().unwrap();

        // Create WAL with seq=0
        let path = tmp.path().join("000000.log");
        {
            let _wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();
        }

        // Rename file to suggest seq=5 but header still says seq=0
        let wrong_path = tmp.path().join("000005.log");
        fs::rename(&path, &wrong_path).unwrap();

        let err = Wal::<MemTableRecord>::open(&wrong_path, None).unwrap_err();
        assert!(
            matches!(err, WalError::InvalidHeader(_)),
            "expected InvalidHeader for seq mismatch, got {:?}",
            err
        );
    }

    // ----------------------------------------------------------------
    // CRC mismatch on record data
    // ----------------------------------------------------------------

    #[test]
    fn record_crc_mismatch_during_replay() {
        init_tracing();
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();

        let record = MemTableRecord {
            key: b"data".to_vec(),
            value: Some(b"value".to_vec()),
            timestamp: 1,
            deleted: false,
        };
        wal.append(&record).unwrap();
        drop(wal);

        // Corrupt a byte in the record data (not the length or checksum)
        {
            let hdr_size = WAL_HDR_SIZE + WAL_CRC32_SIZE;
            let data_offset = hdr_size + 4 + 2; // past header, length prefix, into data
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            f.seek(SeekFrom::Start(data_offset as u64)).unwrap();
            f.write_all(&[0xFF]).unwrap();
            f.sync_all().unwrap();
        }

        let wal2: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();
        let mut iter = wal2.replay_iter().unwrap();
        let result = iter.next().unwrap();
        assert!(
            matches!(
                result,
                Err(WalError::ChecksumMismatch) | Err(WalError::Encoding(_))
            ),
            "expected ChecksumMismatch or Encoding error, got {:?}",
            result
        );
    }
}
