//! WAL rotation and file-naming validation tests.
//!
//! These tests verify the `rotate_next()` mechanism that creates a new WAL
//! segment file with an incremented sequence number, as well as the
//! safety check that rejects WAL files whose on-disk name does not match
//! the sequence stored in the header.
//!
//! Coverage:
//! - `rotate_next()` increments the internal sequence and creates a new file
//! - Multiple rotations persist data across all segments
//! - Opening a renamed WAL file fails (sequence mismatch)
//!
//! ## See also
//! - [`tests_basic`] — basic append / replay / truncate cycle
//! - [`tests_corruption`] — corruption detection and partial replay

#[cfg(test)]
mod tests {
    use crate::wal::Wal;
    use std::fs;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Single rotation
    // ----------------------------------------------------------------

    /// # Scenario
    /// A single `rotate_next()` call creates a new WAL segment file
    /// with the next sequence number.
    ///
    /// # Starting environment
    /// Fresh WAL at sequence 0 (`wal-000000.log`).
    ///
    /// # Actions
    /// 1. Open WAL, note initial sequence.
    /// 2. Call `rotate_next()`.
    /// 3. List directory contents.
    ///
    /// # Expected behavior
    /// - `rotate_next()` returns `seq + 1`.
    /// - Both `wal-000000.log` and `wal-000001.log` exist on disk.
    #[test]
    fn rotate_next_increments_seq() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let mut wal: Wal<u64> = Wal::open(&path, None).unwrap();

        let seq1 = wal.wal_seq();
        let seq2 = wal.rotate_next().expect("rotate failed");

        assert_eq!(seq2, seq1 + 1);
        assert_eq!(wal.wal_seq(), seq1 + 1);

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

    // ----------------------------------------------------------------
    // Multi-rotation data persistence
    // ----------------------------------------------------------------

    /// # Scenario
    /// Multiple rotations produce a chain of WAL segment files, and
    /// replaying all segments in order recovers all appended data.
    ///
    /// # Starting environment
    /// Fresh WAL at sequence 0.
    ///
    /// # Actions
    /// 1. For each of 4 segments (seq 0 → 3):
    ///    a. Append 10 `u64` values.
    ///    b. Rotate to next segment.
    /// 2. List directory → expect `wal-000000.log` through `wal-000004.log`.
    /// 3. Reopen each segment and replay its records.
    /// 4. Concatenate all replayed records.
    ///
    /// # Expected behavior
    /// The concatenated replay matches the original input sequence:
    /// `[0, 1, 2, …, 39]`.
    #[test]
    fn multi_rotation_persists_all_data() {
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

    // ----------------------------------------------------------------
    // Renamed WAL file rejection
    // ----------------------------------------------------------------

    /// # Scenario
    /// If a WAL file is renamed on disk so its filename-derived sequence
    /// number no longer matches the sequence stored in the header, the
    /// WAL must refuse to open it.
    ///
    /// # Starting environment
    /// Fresh WAL at sequence 0 (`wal-000000.log`).
    ///
    /// # Actions
    /// 1. Open and close a WAL (seq 0 stored in header).
    /// 2. Rename `wal-000000.log` → `wal-000005.log` on disk.
    /// 3. Attempt to open `wal-000005.log`.
    ///
    /// # Expected behavior
    /// `Wal::open()` returns an error due to the sequence mismatch
    /// between the filename (5) and the header (0).
    #[test]
    fn open_fails_if_file_was_renamed() {
        use std::fs;

        let tmp = TempDir::new().unwrap();

        let original_path = tmp.path().join("wal-000000.log");
        let wal: Wal<u64> = Wal::open(&original_path, None).unwrap();
        let original_seq = wal.wal_seq();
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
