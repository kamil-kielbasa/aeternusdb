#[cfg(test)]
mod tests {
    use crate::wal::{Wal, WalData, WalError};
    use std::fs::{self, OpenOptions};
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::TempDir;
    use tracing_subscriber::EnvFilter;

    const WAL_CRC32_SIZE: usize = std::mem::size_of::<u32>();
    const WAL_HDR_SIZE: usize = 20;

    #[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
    struct MemTableRecord {
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        timestamp: u64,
        deleted: bool,
    }

    #[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
    struct ManifestRecord {
        id: u64,
        path: String,
        creation_timestamp: u64,
    }

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    }

    fn collect_iter<T: WalData>(wal: &Wal<T>) -> Result<Vec<T>, WalError> {
        wal.replay_iter()?.collect()
    }

    #[test]
    fn test_one_append_and_replay() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let insert = vec![MemTableRecord {
            key: b"a".to_vec(),
            value: Some(b"v1".to_vec()),
            timestamp: 1,
            deleted: false,
        }];

        for record in &insert {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(insert, replayed);
    }

    #[test]
    fn test_many_append_and_replay() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let insert = vec![
            ManifestRecord {
                id: 0,
                path: "/db/table-0".to_string(),
                creation_timestamp: 100,
            },
            ManifestRecord {
                id: 1,
                path: "/db/table-1".to_string(),
                creation_timestamp: 101,
            },
            ManifestRecord {
                id: 2,
                path: "/db/table-2".to_string(),
                creation_timestamp: 102,
            },
        ];

        for record in &insert {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(insert, replayed);
    }

    #[test]
    fn test_many_append_with_replay_and_truncate() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let mut wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let insert = vec![
            MemTableRecord {
                key: b"a".to_vec(),
                value: Some(b"v1".to_vec()),
                timestamp: 1,
                deleted: false,
            },
            MemTableRecord {
                key: b"b".to_vec(),
                value: Some(b"v2".to_vec()),
                timestamp: 2,
                deleted: false,
            },
            MemTableRecord {
                key: b"c".to_vec(),
                value: Some(b"v3".to_vec()),
                timestamp: 3,
                deleted: false,
            },
        ];

        for record in &insert {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(insert, replayed);

        wal.truncate().unwrap();
        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(replayed.len(), 0);
    }

    #[test]
    fn test_full_cycle_of_wal() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let mut wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let batch1 = vec![
            ManifestRecord {
                id: 0,
                path: "/db/table-0".to_string(),
                creation_timestamp: 100,
            },
            ManifestRecord {
                id: 1,
                path: "/db/table-1".to_string(),
                creation_timestamp: 101,
            },
        ];

        let batch2 = vec![
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

        for record in &batch1 {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(batch1, replayed);

        wal.truncate().unwrap();
        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(replayed.len(), 0);

        for record in &batch2 {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(batch2, replayed);

        wal.truncate().unwrap();
        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(replayed.len(), 0);
    }

    #[test]
    fn test_corrupted_header_checksum() {
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
        assert!(err.to_string().contains("Header checksum mismatch"));
    }

    #[test]
    fn test_corrupted_record_length() {
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

    #[test]
    fn test_corrupted_record_data_checksum() {
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

    #[test]
    fn test_corrupted_record_data() {
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
        // The first record is small, so we’ll flip a few bytes within its serialized data.
        let corrupt_offset = (WAL_HDR_SIZE + WAL_CRC32_SIZE + 5) as u64;
        f.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        f.write_all(&[0xFF, 0x00, 0xEE]).unwrap();
        f.sync_all().unwrap();

        // Attempt replay
        let err = collect_iter(&wal).unwrap_err();
        assert!(matches!(err, WalError::ChecksumMismatch));
    }

    #[test]
    fn test_partial_replay_after_last_record_corrupted() {
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

        // Corrupt *last record’s checksum* only
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::End(-2)).unwrap(); // last bytes of checksum
        f.write_all(&[0x99, 0x77]).unwrap();
        f.sync_all().unwrap();

        // Replay should read 2 valid records, then hit corruption
        let mut iter = wal.replay_iter().unwrap();

        let mut replayed = vec![];
        while let Some(res) = iter.next() {
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

    #[test]
    fn rotate_next_increments_seq() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let mut wal: Wal<u64> = Wal::open(&path, None).unwrap();

        let seq1 = wal.header.wal_seq;
        let seq2 = wal.rotate_next().expect("rotate failed");

        assert_eq!(seq2, seq1 + 1);
        assert_eq!(wal.header.wal_seq, seq1 + 1);

        let mut files: Vec<String> = fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();

        files.sort();

        assert!(
            files.contains(&"wal-000000.log".to_string()),
            "Missing wal-00.log after rotate"
        );
        assert!(
            files.contains(&"wal-000001.log".to_string()),
            "Missing wal-01.log after rotate"
        );
    }

    #[test]
    fn wal_multi_rotation_persists_all_data() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");

        let mut wal: Wal<u64> = Wal::open(&path, None).unwrap();

        let mut input_data = Vec::new();

        let rotations = 3; // after this → wal-000000 .. wal-000003
        let writes_per_rotation = 10;

        for _ in 0..=rotations {
            for _ in 0..writes_per_rotation {
                let value = input_data.len() as u64;
                input_data.push(value);
                wal.append(&value).expect("append failed");
            }

            wal.rotate_next().expect("rotation failed");
        }

        let mut files: Vec<String> = fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|f| f.starts_with("wal-") && f.ends_with(".log"))
            .collect();

        files.sort();

        let expected: Vec<String> = (0..=rotations + 1)
            .map(|seq| format!("wal-{seq:06}.log"))
            .collect();

        assert_eq!(files, expected, "Unexpected set of WAL files");

        let mut replayed = Vec::new();

        for seq in 0..=rotations + 1 {
            let p = tmp.path().join(format!("wal-{seq:06}.log"));
            if !p.exists() {
                panic!("Missing WAL file: {}", p.display());
            }

            let wal_reader: Wal<u64> =
                Wal::open(&p, None).expect("Failed to reopen WAL for replay");

            let iter = wal_reader.replay_iter().expect("Failed to get replay_iter");

            for record in iter {
                let record = record.expect("Record decode error");
                replayed.push(record);
            }
        }

        assert_eq!(
            replayed, input_data,
            "Replayed WAL contents do not match original input"
        );
    }

    #[test]
    fn wal_open_fails_if_file_was_renamed() {
        use std::fs;

        let tmp = TempDir::new().unwrap();

        let original_path = tmp.path().join("wal-000000.log");
        let wal: Wal<u64> = Wal::open(&original_path, None).unwrap();
        let original_seq = wal.header.wal_seq;
        assert_eq!(original_seq, 0);

        let renamed_path = tmp.path().join("wal-000005.log");
        fs::rename(&original_path, &renamed_path).expect("Failed to rename WAL file");

        let files: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "wal-000005.log");

        let result = Wal::<u64>::open(&renamed_path, None);

        assert!(
            result.is_err(),
            "Wal::open() should reject renamed WAL due to sequence mismatch"
        );
    }
}
