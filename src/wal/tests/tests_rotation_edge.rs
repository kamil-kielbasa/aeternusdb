//! WAL rotation robustness tests.
//!
//! These tests exercise edge cases in the WAL rotation mechanism that
//! is triggered during memtable freeze. When `freeze_active()` is called,
//! it creates a new WAL with `wal_seq + 1` and adds the old WAL to the
//! frozen list. These tests verify correct behavior under rapid rotation
//! sequences and crash scenarios that leave intermediate WAL files.
//!
//! ## See also
//! - [`wal::tests::tests_rotation`] — unit-level rotation tests
//! - [`tests_crash_flush`] — crash during flush (post-freeze)
//! - [`tests_multi_crash`] — crash cycles with accumulated WAL state

#[cfg(test)]
mod tests {
    use crate::wal::Wal;
    use std::fs;
    use tempfile::TempDir;

    // ================================================================
    // 1. Rapid successive rotations
    // ================================================================

    /// # Scenario
    /// Perform many rapid rotations to verify sequence numbering and
    /// file creation remain consistent.
    ///
    /// # Actions
    /// 1. Open WAL at seq 0.
    /// 2. Rotate 20 times, appending 5 records each time.
    /// 3. Verify all 21 WAL files exist with correct naming.
    /// 4. Replay all and verify record count.
    ///
    /// # Expected behavior
    /// 21 WAL files (`000000.log` .. `000020.log`), 100 total records.
    #[test]
    fn rapid_successive_rotations() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mut wal: Wal<u64> = Wal::open(&path, None).unwrap();

        let rotations = 20;
        let records_per_segment = 5;

        for _ in 0..=rotations {
            for j in 0..records_per_segment {
                wal.append(&(j as u64)).unwrap();
            }
            if wal.wal_seq() < rotations as u64 {
                wal.rotate_next().unwrap();
            }
        }

        // Verify file listing.
        let mut files: Vec<String> = fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| {
                let name = e.ok()?.file_name().to_string_lossy().to_string();
                if name.ends_with(".log") {
                    Some(name)
                } else {
                    None
                }
            })
            .collect();
        files.sort();
        assert_eq!(
            files.len(),
            rotations + 1,
            "Expected {} WAL files",
            rotations + 1
        );

        // Verify sequential naming.
        for (i, f) in files.iter().enumerate() {
            assert_eq!(f, &format!("{i:06}.log"), "Unexpected WAL filename");
        }

        // Replay all and count records.
        let mut total = 0;
        for seq in 0..=rotations {
            let p = tmp.path().join(format!("{seq:06}.log"));
            let reader: Wal<u64> = Wal::open(&p, None).unwrap();
            for record in reader.replay_iter().unwrap() {
                record.unwrap();
                total += 1;
            }
        }
        assert_eq!(
            total,
            (rotations + 1) * records_per_segment,
            "Expected {} total records",
            (rotations + 1) * records_per_segment
        );
    }

    // ================================================================
    // 2. Append after rotation recovers correctly
    // ================================================================

    /// # Scenario
    /// After rotation, the new WAL segment must be immediately usable
    /// for appending and replaying.
    ///
    /// # Actions
    /// 1. Open WAL, append 3 records, rotate.
    /// 2. Append 3 more records to the new segment.
    /// 3. Reopen the new segment, replay.
    ///
    /// # Expected behavior
    /// The new segment replays exactly 3 records.
    #[test]
    fn append_works_after_rotation() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mut wal: Wal<u64> = Wal::open(&path, None).unwrap();

        for i in 0..3u64 {
            wal.append(&i).unwrap();
        }
        let new_seq = wal.rotate_next().unwrap();
        assert_eq!(new_seq, 1);

        for i in 10..13u64 {
            wal.append(&i).unwrap();
        }

        // Reopen new segment and replay.
        let new_path = tmp.path().join("000001.log");
        let reader: Wal<u64> = Wal::open(&new_path, None).unwrap();
        let records: Vec<u64> = reader.replay_iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(records, vec![10, 11, 12]);
    }

    // ================================================================
    // 3. WAL sequence gap is handled
    // ================================================================

    /// # Scenario
    /// If a WAL file in the middle of a sequence is deleted (simulating
    /// partial cleanup), WAL files with higher sequences must still open.
    ///
    /// # Actions
    /// 1. Create WAL segments 0, 1, 2, 3 with data.
    /// 2. Delete segment 2 (000002.log).
    /// 3. Reopen segments 0, 1, 3 — all must open and replay.
    ///
    /// # Expected behavior
    /// Segments with valid headers open independently. The gap doesn't
    /// prevent opening non-contiguous segments.
    #[test]
    fn wal_sequence_gap_segments_open_independently() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mut wal: Wal<u64> = Wal::open(&path, None).unwrap();

        // Create 4 segments.
        for seg in 0..4u64 {
            for i in 0..3 {
                wal.append(&(seg * 10 + i)).unwrap();
            }
            if seg < 3 {
                wal.rotate_next().unwrap();
            }
        }

        // Delete segment 2.
        let gap_path = tmp.path().join("000002.log");
        assert!(gap_path.exists());
        fs::remove_file(&gap_path).unwrap();

        // Remaining segments must open independently.
        for seq in [0, 1, 3] {
            let p = tmp.path().join(format!("{seq:06}.log"));
            let reader: Wal<u64> = Wal::open(&p, None).unwrap();
            let records: Vec<u64> = reader.replay_iter().unwrap().map(|r| r.unwrap()).collect();
            assert_eq!(records.len(), 3, "Segment {seq} should have 3 records");
        }
    }

    // ================================================================
    // 4. Rotation preserves max_record_size
    // ================================================================

    /// # Scenario
    /// `max_record_size` from the original WAL header must be preserved
    /// across rotations.
    ///
    /// # Actions
    /// 1. Open WAL with custom max_record_size = 512.
    /// 2. Rotate several times.
    /// 3. Check header.max_record_size on each new segment.
    ///
    /// # Expected behavior
    /// All rotated WAL segments preserve the original max_record_size.
    #[test]
    fn rotation_preserves_max_record_size() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let custom_max = 512u32;
        let mut wal: Wal<u64> = Wal::open(&path, Some(custom_max)).unwrap();
        assert_eq!(wal.max_record_size(), custom_max);

        for _ in 0..5 {
            wal.rotate_next().unwrap();
            assert_eq!(
                wal.max_record_size(),
                custom_max,
                "max_record_size must be preserved across rotation"
            );
        }

        // Reopen last segment and verify.
        let last_path = tmp.path().join("000005.log");
        let reader: Wal<u64> = Wal::open(&last_path, None).unwrap();
        assert_eq!(reader.max_record_size(), custom_max);
    }
}
