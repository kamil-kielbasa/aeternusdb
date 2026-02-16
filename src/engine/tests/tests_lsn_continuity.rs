//! LSN-continuity tests: verify that writes after reopen get higher LSNs
//! than pre-reopen data, and that the VisibilityFilter correctly uses LSN
//! ordering for range-tombstone resolution during scans.
//!
//! The Log Sequence Number (LSN) is the engine's monotonic write counter.
//! When the engine is reopened, it must resume from the highest LSN seen
//! in the persisted data so that all new writes receive LSNs strictly
//! greater than any pre-existing entry. This is critical for the LSM-tree
//! merge: the entry with the highest LSN for a given key always wins.
//! These tests verify that overwrites, deletes, and range tombstones
//! issued after a reopen correctly shadow older data, and that the
//! `VisibilityFilter` respects LSN ordering when deciding whether a
//! range tombstone hides a point put (or vice versa).
//!
//! ## Layer coverage
//! - All tests use `memtable_sstable` (cross-session LSN ordering)
//!
//! ## See also
//! - [`tests_precedence`] — intra-session LSN ordering
//! - [`tests_recovery`] — durability after clean close
//! - [`tests_crash_recovery`] — durability after crash

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ================================================================
    // 1. Overwrite after reopen shadows old value
    // ================================================================

    /// # Scenario
    /// A write issued after reopen shadows the value from the previous session.
    ///
    /// # Starting environment
    /// Engine with key `"k"` = `"old"` written and closed.
    ///
    /// # Actions
    /// 1. Reopen the engine.
    /// 2. Overwrite `"k"` = `"new"`.
    /// 3. Get `"k"`.
    ///
    /// # Expected behavior
    /// Returns `Some("new")` — the post-reopen write has a higher LSN
    /// than the pre-reopen entry, so it takes precedence.
    #[test]
    fn memtable_sstable__writes_after_reopen_shadow_old() {
        let dir = TempDir::new().unwrap();

        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"old".to_vec()).unwrap();
        engine.close().unwrap();

        let engine = reopen(dir.path());
        engine.put(b"k".to_vec(), b"new".to_vec()).unwrap();
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            Some(b"new".to_vec()),
            "new write after reopen must shadow old data"
        );
    }

    // ================================================================
    // 2. Delete after reopen hides old put
    // ================================================================

    /// # Scenario
    /// A delete issued after reopen hides the value from the previous session.
    ///
    /// # Starting environment
    /// Engine with key `"k"` = `"v"` written and closed.
    ///
    /// # Actions
    /// 1. Reopen the engine.
    /// 2. Delete `"k"`.
    /// 3. Get `"k"`.
    ///
    /// # Expected behavior
    /// Returns `None` — the post-reopen tombstone has a higher LSN and
    /// hides the old put.
    #[test]
    fn memtable_sstable__delete_after_reopen_hides_old() {
        let dir = TempDir::new().unwrap();

        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.close().unwrap();

        let engine = reopen(dir.path());
        engine.delete(b"k".to_vec()).unwrap();
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            None,
            "delete after reopen must hide old put"
        );
    }

    // ================================================================
    // 3. Range-delete after reopen hides old puts
    // ================================================================

    /// # Scenario
    /// A range-delete issued after reopen hides multiple keys from the
    /// previous session.
    ///
    /// # Starting environment
    /// Engine with keys `key_00`..`key_09` written and closed.
    ///
    /// # Actions
    /// 1. Reopen the engine.
    /// 2. Range-delete `["key_03", "key_07")`.
    /// 3. Get all 10 keys.
    ///
    /// # Expected behavior
    /// Keys 3–6: `None` (range-deleted with higher LSN).
    /// All others: original values.
    #[test]
    fn memtable_sstable__range_delete_after_reopen_hides_old() {
        let dir = TempDir::new().unwrap();

        let engine = Engine::open(dir.path(), default_config()).unwrap();
        for i in 0..10u8 {
            engine
                .put(
                    format!("key_{:02}", i).into_bytes(),
                    format!("val_{:02}", i).into_bytes(),
                )
                .unwrap();
        }
        engine.close().unwrap();

        let engine = reopen(dir.path());
        engine
            .delete_range(b"key_03".to_vec(), b"key_07".to_vec())
            .unwrap();

        for i in 0..10u8 {
            let key = format!("key_{:02}", i).into_bytes();
            let val = engine.get(key).unwrap();
            if (3..7).contains(&i) {
                assert_eq!(
                    val, None,
                    "key_{:02} should be range-deleted after reopen",
                    i
                );
            } else {
                assert_eq!(
                    val,
                    Some(format!("val_{:02}", i).into_bytes()),
                    "key_{:02} should survive",
                    i
                );
            }
        }
    }

    // ================================================================
    // 4. LSN continuity across multiple reopen cycles
    // ================================================================

    /// # Scenario
    /// LSN counter correctly resumes across 3 reopen cycles, ensuring the
    /// latest overwrite always wins.
    ///
    /// # Starting environment
    /// Empty temporary directory.
    ///
    /// # Actions
    /// 1. Cycle 1: put `"k"` = `"v1"`, close.
    /// 2. Cycle 2: reopen, overwrite `"k"` = `"v2"`, close.
    /// 3. Cycle 3: reopen, overwrite `"k"` = `"v3"`, close.
    /// 4. Final reopen: get `"k"`.
    ///
    /// # Expected behavior
    /// Returns `Some("v3")` — each cycle’s write gets a higher LSN than
    /// the previous, so `"v3"` (from cycle 3) is the winner.
    #[test]
    fn memtable_sstable__lsn_continuity_across_reopens() {
        let dir = TempDir::new().unwrap();

        // Cycle 1: write initial value
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
        engine.close().unwrap();

        // Cycle 2: overwrite
        let engine = reopen(dir.path());
        engine.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
        engine.close().unwrap();

        // Cycle 3: overwrite again
        let engine = reopen(dir.path());
        engine.put(b"k".to_vec(), b"v3".to_vec()).unwrap();
        engine.close().unwrap();

        // Final verify
        let engine = reopen(dir.path());
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            Some(b"v3".to_vec()),
            "Most recent write must win across 3 reopen cycles"
        );
    }

    // ================================================================
    // 5. Scan respects LSN after reopen (overwrite in memtable
    //    shadows SSTable value)
    // ================================================================

    /// # Scenario
    /// Scan correctly resolves a post-reopen overwrite against SSTable data.
    ///
    /// # Starting environment
    /// Engine with 30 keys pushed to SSTables (128-byte buffer), then closed.
    ///
    /// # Actions
    /// 1. Reopen and overwrite `key_0010` = `"NEW"` in the active memtable.
    /// 2. Scan the full range.
    ///
    /// # Expected behavior
    /// The scan entry for `key_0010` has value `"NEW"` (from the post-reopen
    /// put), not the SSTable value. LSN ordering is respected in scan merging.
    #[test]
    fn memtable_sstable__scan_respects_lsn_after_reopen() {
        let dir = TempDir::new().unwrap();

        // Write enough to push data to SSTables
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();
        for i in 0..30u32 {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("old_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        engine.close().unwrap();

        // Reopen and overwrite a key
        let engine = reopen(dir.path());
        engine.put(b"key_0010".to_vec(), b"NEW".to_vec()).unwrap();

        let results = collect_scan(&engine, b"key_", b"key_\xff");
        let entry = results.iter().find(|(k, _)| k == b"key_0010").unwrap();
        assert_eq!(
            entry.1,
            b"NEW".to_vec(),
            "scan must show the post-reopen overwrite, not the SSTable value"
        );
    }

    // ================================================================
    // 6. Lower-LSN range tombstone does NOT hide higher-LSN put in scan
    //
    //    Setup: range-delete first (low LSN) → flush to SSTable →
    //           put inside the range (high LSN) → flush to SSTable.
    //    Scan should show the put.
    // ================================================================

    /// # Scenario
    /// A range tombstone with a LOWER LSN does NOT hide a put with a
    /// HIGHER LSN, even when both are in SSTables.
    ///
    /// # Starting environment
    /// Engine with 128-byte buffer.
    ///
    /// # Actions
    /// 1. Write a range-delete `[key_0003, key_0008)` FIRST (gets low LSN).
    /// 2. Write 40 puts (including keys 3–7) which get higher LSNs.
    ///    By now the range tombstone is in an older SSTable.
    /// 3. Scan `[key_0003, key_0008)`.
    ///
    /// # Expected behavior
    /// All 5 keys (3–7) are visible in the scan — their puts have higher
    /// LSNs than the range tombstone, so they are NOT hidden.
    #[test]
    fn memtable_sstable__older_tombstone_no_hide_newer_put() {
        let dir = TempDir::new().unwrap();

        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        // Step 1: Write the range delete FIRST (it gets a low LSN).
        engine
            .delete_range(b"key_0003".to_vec(), b"key_0008".to_vec())
            .unwrap();

        // Step 2: Write enough puts to push the range delete into an SSTable.
        for i in 0..40u32 {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("val_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        // By now the range delete (low LSN) is in an older SSTable.
        // The puts (higher LSNs) that landed in newer SSTables / active memtable
        // should NOT be hidden by the old range tombstone.

        let results = collect_scan(&engine, b"key_0003", b"key_0008");
        // All keys key_0003..key_0007 were put AFTER the range delete,
        // so they should be visible.
        let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();
        for i in 3..8u32 {
            let expected = format!("key_{:04}", i).into_bytes();
            assert!(
                keys.contains(&expected),
                "key_{:04} was put after the range delete and must be visible in scan",
                i
            );
        }
    }

    // ================================================================
    // 7. Higher-LSN range tombstone DOES hide lower-LSN put in scan
    //
    //    Setup: puts first (low LSN) → flush to SSTable →
    //           range-delete (high LSN) over the same range.
    //    Scan should hide those puts.
    // ================================================================

    /// # Scenario
    /// A range tombstone with a HIGHER LSN DOES hide puts with LOWER LSNs.
    ///
    /// # Starting environment
    /// Engine with 128-byte buffer.
    ///
    /// # Actions
    /// 1. Write 30 puts FIRST (keys 0–29, low LSNs → flushed to SSTables).
    /// 2. Range-delete `[key_0010, key_0020)` (higher LSN than any put).
    /// 3. Scan the full range.
    ///
    /// # Expected behavior
    /// Keys 10–19 are ABSENT from the scan — the range tombstone has a
    /// higher LSN than the puts it covers. Keys outside the range (0–9,
    /// 20–29) remain visible.
    #[test]
    fn memtable_sstable__newer_tombstone_hides_older_put() {
        let dir = TempDir::new().unwrap();

        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        // Step 1: Write puts FIRST (low LSNs).
        for i in 0..30u32 {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("val_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        // Several SSTables should exist by now (128-byte buffer).

        // Step 2: Range-delete a subset (higher LSN than any put).
        engine
            .delete_range(b"key_0010".to_vec(), b"key_0020".to_vec())
            .unwrap();

        let results = collect_scan(&engine, b"key_", b"key_\xff");
        let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();
        for i in 10..20u32 {
            let k = format!("key_{:04}", i).into_bytes();
            assert!(
                !keys.contains(&k),
                "key_{:04} should be hidden by the newer range tombstone in scan",
                i
            );
        }
        // Keys outside the range should still be visible
        for i in [0u32, 1, 5, 9, 20, 25, 29] {
            let k = format!("key_{:04}", i).into_bytes();
            assert!(
                keys.contains(&k),
                "key_{:04} outside range should be visible in scan",
                i
            );
        }
    }
}
