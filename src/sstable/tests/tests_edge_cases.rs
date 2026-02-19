//! SSTable edge-case and boundary-condition tests.
//!
//! These tests cover scenarios not exercised by the basic / get / scan
//! suites — specifically nonexistent-key lookups, bloom filter rejection,
//! corrupted file handling, multi-block SSTables, and truncated files.
//!
//! Coverage:
//! - `get()` for a key that has no entry and no covering range-delete
//!   → `GetResult::NotFound`
//! - Bloom filter rejects absent keys (skips data block search)
//! - Corrupted SSTable file (flipped bytes) → error on `open()`
//! - Multi-block SSTable with many entries across block boundaries
//! - `SSTable::open` on a truncated / too-short file → error
//! - `SSTable::open` on a file with wrong magic
//!
//! ## See also
//! - [`tests_basic`] — SSTable build / open / structural validation
//! - [`tests_get`]   — intra-SSTable `get()` with LSN resolution
//! - [`tests_scan`]  — raw unresolved SSTable scan output

#[cfg(test)]
mod tests {
    use crate::sstable::{self, GetResult, PointEntry, RangeTombstone, Record, SSTable};
    use std::fs;
    use tempfile::TempDir;
    use tracing::Level;
    use tracing_subscriber::fmt::Subscriber;

    fn init_tracing() {
        let _ = Subscriber::builder()
            .with_max_level(Level::TRACE)
            .try_init();
    }

    fn point(key: &[u8], value: &[u8], lsn: u64, timestamp: u64) -> PointEntry {
        PointEntry {
            key: key.to_vec(),
            value: Some(value.to_vec()),
            lsn,
            timestamp,
        }
    }

    fn rdel(start: &[u8], end: &[u8], lsn: u64, timestamp: u64) -> RangeTombstone {
        RangeTombstone {
            start: start.to_vec(),
            end: end.to_vec(),
            lsn,
            timestamp,
        }
    }

    // ----------------------------------------------------------------
    // get() for nonexistent key
    // ----------------------------------------------------------------

    /// # Scenario
    /// Query a key that does not exist in the SSTable and is not covered
    /// by any range tombstone.
    ///
    /// # Starting environment
    /// SSTable with 3 point entries (`a`, `c`, `e`).
    ///
    /// # Actions
    /// 1. `sst.get(b"b")` — key between existing entries.
    /// 2. `sst.get(b"z")` — key beyond all entries.
    ///
    /// # Expected behavior
    /// Both return `GetResult::NotFound`.
    #[test]
    fn get_nonexistent_key_returns_not_found() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_notfound.bin");

        let points = vec![
            point(b"a", b"1", 10, 100),
            point(b"c", b"3", 11, 101),
            point(b"e", b"5", 12, 102),
        ];
        let ranges: Vec<RangeTombstone> = vec![];

        let pt_count = points.len();

        let rt_count = ranges.len();

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), pt_count, ranges.into_iter(), rt_count)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        assert_eq!(sst.get(b"b").unwrap(), GetResult::NotFound);
        assert_eq!(sst.get(b"z").unwrap(), GetResult::NotFound);
    }

    // ----------------------------------------------------------------
    // Bloom filter rejects absent keys
    // ----------------------------------------------------------------

    /// # Scenario
    /// The bloom filter must reject keys that were never inserted,
    /// causing `get()` to return `NotFound` without scanning data blocks.
    ///
    /// # Starting environment
    /// SSTable with 3 point entries (`apple`, `banana`, `cherry`).
    ///
    /// # Actions
    /// 1. `sst.get(b"dragonfruit")` — absent key.
    /// 2. `sst.get(b"elderberry")` — absent key.
    ///
    /// # Expected behavior
    /// Both return `GetResult::NotFound`. (The bloom filter may or may
    /// not reject — this test verifies the semantic result is correct
    /// regardless of bloom false-positive behaviour.)
    #[test]
    fn bloom_filter_absent_key_returns_not_found() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_bloom.bin");

        let points = vec![
            point(b"apple", b"red", 1, 100),
            point(b"banana", b"yellow", 2, 101),
            point(b"cherry", b"dark-red", 3, 102),
        ];
        let ranges: Vec<RangeTombstone> = vec![];

        let pt_count = points.len();

        let rt_count = ranges.len();

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), pt_count, ranges.into_iter(), rt_count)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        // These keys were never inserted — bloom should (usually) reject them.
        // Either way, the result must be NotFound.
        assert_eq!(sst.get(b"dragonfruit").unwrap(), GetResult::NotFound);
        assert_eq!(sst.get(b"elderberry").unwrap(), GetResult::NotFound);
        assert_eq!(sst.get(b"zebra_fruit").unwrap(), GetResult::NotFound);
    }

    // ----------------------------------------------------------------
    // Corrupted SSTable file
    // ----------------------------------------------------------------

    /// # Scenario
    /// Corrupting bytes in an SSTable file must cause `SSTable::open`
    /// to return an error (checksum mismatch or decode failure).
    ///
    /// # Starting environment
    /// A valid SSTable file on disk.
    ///
    /// # Actions
    /// 1. Build a valid SSTable.
    /// 2. Flip several bytes at offset 4 in the file.
    /// 3. Attempt to reopen.
    ///
    /// # Expected behavior
    /// `SSTable::open()` returns an error.
    #[test]
    fn open_corrupted_file_fails() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_corrupt.bin");

        let points = vec![point(b"a", b"1", 1, 100), point(b"b", b"2", 2, 101)];
        let ranges: Vec<RangeTombstone> = vec![];

        let pt_count = points.len();

        let rt_count = ranges.len();

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), pt_count, ranges.into_iter(), rt_count)
            .unwrap();

        // Corrupt the header region
        let mut bytes = fs::read(&path).unwrap();
        bytes[4] ^= 0xFF;
        bytes[5] ^= 0xFF;
        bytes[6] ^= 0xFF;
        fs::write(&path, bytes).unwrap();

        let result = SSTable::open(&path);
        assert!(result.is_err(), "Opening a corrupted SSTable should fail");
    }

    // ----------------------------------------------------------------
    // Multi-block SSTable
    // ----------------------------------------------------------------

    /// # Scenario
    /// Create an SSTable with enough entries to span multiple data blocks
    /// (block size is 4096 bytes). Verify that `get()` and `scan()`
    /// correctly locate keys across block boundaries.
    ///
    /// # Starting environment
    /// No SSTable file on disk.
    ///
    /// # Actions
    /// 1. Build an SSTable with 500 entries (large enough values to
    ///    exceed a single 4 KiB block).
    /// 2. `get()` keys from the beginning, middle, and end.
    /// 3. Full `scan()` to verify all entries are returned.
    ///
    /// # Expected behavior
    /// - At least 2 index entries (confirming multiple blocks).
    /// - All 500 keys are retrievable via `get()`.
    /// - `scan()` returns exactly 500 records in sorted order.
    #[test]
    fn multi_block_get_and_scan() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_multiblock.bin");

        let num_entries = 500;

        let points: Vec<PointEntry> = (0..num_entries)
            .map(|i| {
                let key = format!("key_{:06}", i).into_bytes();
                // Value large enough to push total beyond 4096-byte block boundary
                let value = format!("value_{:06}_padding_{}", i, "X".repeat(50)).into_bytes();
                point(&key, &value, i as u64 + 1, (i as u64 + 1) * 100)
            })
            .collect();

        let ranges: Vec<RangeTombstone> = vec![];

        let pt_count = points.len();

        let rt_count = ranges.len();

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), pt_count, ranges.into_iter(), rt_count)
            .unwrap();

        let sst = SSTable::open(&path).unwrap();

        // Confirm multiple index entries (= multiple blocks)
        assert!(
            sst.index.len() >= 2,
            "Expected at least 2 index entries (multiple blocks), got {}",
            sst.index.len()
        );

        // Verify get() works across block boundaries
        // First key
        let r = sst.get(b"key_000000").unwrap();
        assert!(
            matches!(r, GetResult::Put { .. }),
            "First key should be found"
        );

        // Middle key
        let mid = format!("key_{:06}", num_entries / 2).into_bytes();
        let r = sst.get(&mid).unwrap();
        assert!(
            matches!(r, GetResult::Put { .. }),
            "Middle key should be found"
        );

        // Last key
        let last = format!("key_{:06}", num_entries - 1).into_bytes();
        let r = sst.get(&last).unwrap();
        assert!(
            matches!(r, GetResult::Put { .. }),
            "Last key should be found"
        );

        // Nonexistent key between blocks
        assert_eq!(sst.get(b"key_999999").unwrap(), GetResult::NotFound);

        // Full scan
        let scanned: Vec<Record> = sst.scan(b"key_", b"key_\xff").unwrap().collect();
        assert_eq!(
            scanned.len(),
            num_entries,
            "Scan should return all {} entries",
            num_entries
        );

        // Verify sorted order
        for (i, entry) in scanned.iter().enumerate().take(num_entries) {
            let expected_key = format!("key_{:06}", i).into_bytes();
            match entry {
                Record::Put { key, .. } => {
                    assert_eq!(key, &expected_key, "Entry {} should be key_{:06}", i, i);
                }
                other => panic!("Expected Put at index {}, got {:?}", i, other),
            }
        }
    }

    // ----------------------------------------------------------------
    // Open truncated file
    // ----------------------------------------------------------------

    /// # Scenario
    /// A file that is too short to contain even the footer must be
    /// rejected.
    ///
    /// # Starting environment
    /// A file containing only 10 bytes of garbage.
    ///
    /// # Actions
    /// 1. Write 10 bytes to a file.
    /// 2. `SSTable::open(path)`.
    ///
    /// # Expected behavior
    /// Returns an error (file too small for footer).
    #[test]
    fn open_truncated_file_fails() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_truncated.bin");

        // Write a file that's too short for any valid SSTable
        fs::write(&path, [0u8; 10]).unwrap();

        let result = SSTable::open(&path);
        assert!(result.is_err(), "Opening a truncated SSTable should fail");
    }

    // ----------------------------------------------------------------
    // Open file with wrong magic
    // ----------------------------------------------------------------

    /// # Scenario
    /// A file that has the right size structure but wrong magic bytes
    /// in the header must be rejected.
    ///
    /// # Starting environment
    /// A valid SSTable file on disk.
    ///
    /// # Actions
    /// 1. Build a valid SSTable.
    /// 2. Overwrite the first 4 bytes (magic) with `b"XXXX"`.
    /// 3. Also fix the header CRC to avoid a checksum error first
    ///    — actually, we can just flip the magic and expect either
    ///    a checksum error or a magic mismatch error.
    /// 4. `SSTable::open(path)`.
    ///
    /// # Expected behavior
    /// Returns an error (checksum mismatch or magic mismatch).
    #[test]
    fn open_wrong_magic_fails() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sst_bad_magic.bin");

        let points = vec![point(b"a", b"1", 1, 100)];
        let ranges: Vec<RangeTombstone> = vec![];

        let pt_count = points.len();

        let rt_count = ranges.len();

        sstable::SstWriter::new(&path)
            .build(points.into_iter(), pt_count, ranges.into_iter(), rt_count)
            .unwrap();

        // Overwrite magic bytes
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] = b'X';
        bytes[1] = b'X';
        bytes[2] = b'X';
        bytes[3] = b'X';
        fs::write(&path, bytes).unwrap();

        let result = SSTable::open(&path);
        assert!(
            result.is_err(),
            "Opening an SSTable with wrong magic should fail"
        );
    }
}
