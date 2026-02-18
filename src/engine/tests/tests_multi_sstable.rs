//! Multi-SSTable correctness tests.
//!
//! Every test in this module guarantees that data is spread across at least 2
//! SSTables (1 KB write buffer → ~1 KB+ per SSTable) so that reads, deletes,
//! and scans must merge results from multiple on-disk tables. This is the
//! realistic scenario for any non-trivial database: the engine must correctly
//! resolve key lookups, tombstones, range tombstones, overwrites, and scans
//! when the answer requires consulting several SSTable files simultaneously.
//!
//! ## Layer coverage
//! - All tests use `memtable_sstable` with ≥2 SSTables (1 KB buffer)
//!
//! ## See also
//! - [`tests_layers`] — layer shadowing with single SSTable
//! - [`tests_scan`] — scan basics before multi-SSTable merge
//! - [`tests_precedence`] — LSN ordering across layers

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Get across multiple SSTables
    // ----------------------------------------------------------------

    /// # Scenario
    /// All 100 keys are readable when spread across ≥2 SSTables.
    ///
    /// # Starting environment
    /// Engine with 1 KB buffer; 100 keys inserted via `engine_with_multi_sstables`,
    /// guaranteeing ≥2 SSTables on disk.
    ///
    /// # Actions
    /// 1. Get each of the 100 keys.
    ///
    /// # Expected behavior
    /// Every key returns its correct padded value — the multi-SSTable merge
    /// path resolves lookups correctly.
    #[test]
    fn memtable_sstable__get_all_keys_across_multi() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "mg");

        for i in 0..100 {
            let key = format!("mg_{:04}", i).into_bytes();
            let expected = format!("value_with_some_padding_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "mg_{:04} should be readable",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Overwrite across multiple SSTables
    // ----------------------------------------------------------------

    /// # Scenario
    /// Overwriting all keys creates additional SSTables; latest values win.
    ///
    /// # Starting environment
    /// Engine with 1 KB buffer.
    ///
    /// # Actions
    /// 1. Round 1: insert 80 keys with `"round1_*"` values → ≥2 SSTables.
    /// 2. Round 2: overwrite all 80 keys with `"round2_*"` values → more
    ///    SSTables.
    /// 3. Get all 80 keys.
    ///
    /// # Expected behavior
    /// All keys return `"round2_*"` values. `sstables_count` increases after
    /// each round. The engine merges across all SSTables and returns the
    /// latest value per key.
    #[test]
    fn memtable_sstable__overwrite_across_multi() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), multi_sstable_config()).unwrap();

        // Round 1: initial values → spread across SSTables
        for i in 0..80 {
            let key = format!("ov_{:04}", i).into_bytes();
            let val = format!("round1_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();
        let s1 = engine.stats().unwrap().sstables_count;
        assert!(s1 >= 2, "Expected >= 2 SSTables after round 1, got {}", s1);

        // Round 2: overwrite all keys → creates additional SSTables
        for i in 0..80 {
            let key = format!("ov_{:04}", i).into_bytes();
            let val = format!("round2_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();
        let s2 = engine.stats().unwrap().sstables_count;
        assert!(
            s2 > s1,
            "Expected more SSTables after round 2 ({} > {})",
            s2,
            s1
        );

        // Latest values must win across all SSTables
        for i in 0..80 {
            let key = format!("ov_{:04}", i).into_bytes();
            let expected = format!("round2_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "ov_{:04} should have round 2 value",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Point delete across multiple SSTables
    // ----------------------------------------------------------------

    /// # Scenario
    /// Point-delete tombstones hide keys in older SSTables.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Delete the first 50 keys.
    /// 2. Get all 100 keys.
    ///
    /// # Expected behavior
    /// Keys 0–49: `None`. Keys 50–99: present. The tombstones written to
    /// newer SSTables/memtable correctly shadow older SSTable entries.
    #[test]
    fn memtable_sstable__point_delete_hides_in_older() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "pd");

        // Delete first half — tombstones go into memtable / newer SSTables
        for i in 0..50 {
            let key = format!("pd_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }

        // Deleted keys gone
        for i in 0..50 {
            let key = format!("pd_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "pd_{:04} should be deleted",
                i
            );
        }

        // Surviving keys still readable
        for i in 50..100 {
            let key = format!("pd_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "pd_{:04} should exist",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Delete then re-insert across multiple SSTables
    // ----------------------------------------------------------------

    /// # Scenario
    /// Delete a range of keys across multiple SSTables, then re-insert them.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Delete keys 20–39.
    /// 2. Re-insert keys 20–39 with `"reinserted_*"` values.
    /// 3. Get keys from all three groups.
    ///
    /// # Expected behavior
    /// - Re-inserted keys (20–39): `"reinserted_*"` (newest LSN wins).
    /// - Untouched keys (0–19): original padded values.
    #[test]
    fn memtable_sstable__delete_and_reinsert() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "dr");

        // Delete a range of keys
        for i in 20..40 {
            let key = format!("dr_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }

        // Re-insert with new values
        for i in 20..40 {
            let key = format!("dr_{:04}", i).into_bytes();
            let val = format!("reinserted_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        // Re-inserted keys have new values
        for i in 20..40 {
            let key = format!("dr_{:04}", i).into_bytes();
            let expected = format!("reinserted_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(expected), "dr_{:04}", i);
        }

        // Untouched keys still have original values
        for i in 0..20 {
            let key = format!("dr_{:04}", i).into_bytes();
            let expected = format!("value_with_some_padding_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(expected), "dr_{:04}", i);
        }
    }

    // ----------------------------------------------------------------
    // Range delete across multiple SSTables
    // ----------------------------------------------------------------

    /// # Scenario
    /// A single range-delete spans keys distributed across multiple SSTables.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Range-delete `[rr_0020, rr_0060)`.
    /// 2. Get all 100 keys.
    ///
    /// # Expected behavior
    /// Keys 0–19 and 60–99: present. Keys 20–59: `None`.
    /// The range tombstone correctly covers keys in multiple SSTables.
    #[test]
    fn memtable_sstable__range_delete_spans_multi() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "rr");

        // Range delete that spans keys in different SSTables
        engine
            .delete_range(b"rr_0020".to_vec(), b"rr_0060".to_vec())
            .unwrap();

        for i in 0..20 {
            let key = format!("rr_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "rr_{:04} should survive",
                i
            );
        }
        for i in 20..60 {
            let key = format!("rr_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "rr_{:04} should be deleted",
                i
            );
        }
        for i in 60..100 {
            let key = format!("rr_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "rr_{:04} should survive",
                i
            );
        }
    }

    /// # Scenario
    /// Two overlapping range-deletes produce the correct union of deleted keys.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Range-delete `[or_0010, or_0040)` and `[or_0030, or_0070)`.
    /// 2. Get all 100 keys.
    ///
    /// # Expected behavior
    /// The effective deleted interval is the union `[10, 70)`.
    /// Keys 0–9 and 70–99: present. Keys 10–69: `None`.
    #[test]
    fn memtable_sstable__overlapping_range_deletes() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "or");

        // Two overlapping range deletes
        engine
            .delete_range(b"or_0010".to_vec(), b"or_0040".to_vec())
            .unwrap();
        engine
            .delete_range(b"or_0030".to_vec(), b"or_0070".to_vec())
            .unwrap();

        // Union: [10, 70)
        for i in 0..10 {
            let key = format!("or_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "or_{:04} should survive",
                i
            );
        }
        for i in 10..70 {
            let key = format!("or_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "or_{:04} should be deleted",
                i
            );
        }
        for i in 70..100 {
            let key = format!("or_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "or_{:04} should survive",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Precedence across multiple SSTables
    // ----------------------------------------------------------------

    /// # Scenario
    /// Range-delete then re-insert a subset: the re-inserted keys (higher LSN)
    /// are resurrected while the rest remain deleted.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Range-delete `[rp_0030, rp_0050)` → confirm keys 30–49 are gone.
    /// 2. Re-insert keys 35–44 with `"resurrected_*"` values.
    /// 3. Get keys in all sub-ranges.
    ///
    /// # Expected behavior
    /// - 30–34: `None` (still range-deleted).
    /// - 35–44: `"resurrected_*"` (re-inserted with higher LSN).
    /// - 45–49: `None` (still range-deleted).
    #[test]
    fn memtable_sstable__range_delete_then_put() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "rp");

        // Range delete — goes to memtable, may flush to SSTable
        engine
            .delete_range(b"rp_0030".to_vec(), b"rp_0050".to_vec())
            .unwrap();

        // Confirm keys are deleted
        for i in 30..50 {
            let key = format!("rp_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "rp_{:04} should be deleted",
                i
            );
        }

        // Re-insert some keys inside the deleted range — newer LSN wins
        for i in 35..45 {
            let key = format!("rp_{:04}", i).into_bytes();
            let val = format!("resurrected_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        // 30-34: still deleted
        for i in 30..35 {
            let key = format!("rp_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "rp_{:04} should stay deleted",
                i
            );
        }
        // 35-44: resurrected
        for i in 35..45 {
            let key = format!("rp_{:04}", i).into_bytes();
            let expected = format!("resurrected_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "rp_{:04} should be resurrected",
                i
            );
        }
        // 45-49: still deleted
        for i in 45..50 {
            let key = format!("rp_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "rp_{:04} should stay deleted",
                i
            );
        }
    }

    /// # Scenario
    /// Overwrite then delete the same key — the delete (highest LSN) wins.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Overwrite `np_0050` = `"updated"`.
    /// 2. Delete `np_0050`.
    /// 3. Get `np_0050`.
    ///
    /// # Expected behavior
    /// Returns `None` — the delete tombstone has the highest LSN.
    #[test]
    fn memtable_sstable__newer_delete_beats_older_put() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "np");

        // Overwrite key in older SSTable, then delete
        engine
            .put(b"np_0050".to_vec(), b"updated".to_vec())
            .unwrap();
        engine.delete(b"np_0050".to_vec()).unwrap();

        assert_eq!(engine.get(b"np_0050".to_vec()).unwrap(), None);
    }

    /// # Scenario
    /// Interleaved deletes and re-inserts: delete even keys, then re-insert
    /// every 4th key.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Delete even keys (0, 2, 4, …, 98).
    /// 2. Re-insert every 4th key (0, 4, 8, …, 96) with `"revived_*"`.
    /// 3. Get all 100 keys.
    ///
    /// # Expected behavior
    /// - `i % 4 == 0`: `"revived_*"` (re-inserted).
    /// - `i % 2 == 0, i % 4 != 0`: `None` (deleted, not re-inserted).
    /// - `i` odd: original padded value (never touched).
    #[test]
    fn memtable_sstable__interleaved_deletes_and_puts() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "ip");

        // Delete even keys
        for i in (0..100).step_by(2) {
            let key = format!("ip_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }

        // Re-insert every 4th key (0, 4, 8, ...)
        for i in (0..100).step_by(4) {
            let key = format!("ip_{:04}", i).into_bytes();
            let val = format!("revived_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        for i in 0..100 {
            let key = format!("ip_{:04}", i).into_bytes();
            let result = engine.get(key).unwrap();
            if i % 4 == 0 {
                // Re-inserted
                let expected = format!("revived_{:04}", i).into_bytes();
                assert_eq!(result, Some(expected), "ip_{:04} should be revived", i);
            } else if i % 2 == 0 {
                // Deleted and not re-inserted
                assert_eq!(result, None, "ip_{:04} should be deleted", i);
            } else {
                // Odd keys were never touched
                let expected = format!("value_with_some_padding_{:04}", i).into_bytes();
                assert_eq!(
                    result,
                    Some(expected),
                    "ip_{:04} should have original value",
                    i
                );
            }
        }
    }

    // ----------------------------------------------------------------
    // Scan merges across multiple SSTables
    // ----------------------------------------------------------------

    /// # Scenario
    /// Scan merges keys from ≥2 SSTables into a sorted, deduplicated result.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Scan the full range `["ss_", "ss_\xff")`.
    ///
    /// # Expected behavior
    /// Returns exactly 100 keys, in strictly sorted order, each with its
    /// correct padded value.
    #[test]
    fn memtable_sstable__scan_sorted_deduped() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "ss");

        let results = collect_scan(&engine, b"ss_", b"ss_\xff");
        assert_eq!(results.len(), 100, "Should return all 100 keys");

        // Verify sorted order
        for i in 1..results.len() {
            assert!(results[i - 1].0 < results[i].0, "Keys must be sorted");
        }

        // Verify correct values
        for (i, (k, v)) in results.iter().enumerate() {
            let expected_key = format!("ss_{:04}", i).into_bytes();
            let expected_val = format!("value_with_some_padding_{:04}", i).into_bytes();
            assert_eq!(k, &expected_key);
            assert_eq!(v, &expected_val);
        }
    }

    /// # Scenario
    /// Scan correctly excludes point-deleted keys spread across ≥2 SSTables.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Delete keys 20–39.
    /// 2. Scan the full range.
    ///
    /// # Expected behavior
    /// Returns 80 keys. None of the deleted keys (20–39) appear in the scan.
    #[test]
    fn memtable_sstable__scan_excludes_deletes() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "sd");

        // Delete keys 20-39
        for i in 20..40 {
            let key = format!("sd_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }

        let results = collect_scan(&engine, b"sd_", b"sd_\xff");
        assert_eq!(
            results.len(),
            80,
            "Should return 80 keys (100 - 20 deleted)"
        );

        for (k, _) in &results {
            assert!(
                k.as_slice() < b"sd_0020" || k.as_slice() >= b"sd_0040",
                "Deleted key {:?} should not appear",
                String::from_utf8_lossy(k)
            );
        }
    }

    /// # Scenario
    /// Scan correctly excludes range-deleted keys spread across ≥2 SSTables.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Range-delete `[sr_0025, sr_0075)`.
    /// 2. Scan the full range.
    ///
    /// # Expected behavior
    /// Returns 50 keys (100 – 50 range-deleted). Keys 25–74 are excluded.
    #[test]
    fn memtable_sstable__scan_excludes_range_deletes() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "sr");

        engine
            .delete_range(b"sr_0025".to_vec(), b"sr_0075".to_vec())
            .unwrap();

        let results = collect_scan(&engine, b"sr_", b"sr_\xff");
        assert_eq!(
            results.len(),
            50,
            "Should return 50 keys (100 - 50 range-deleted)"
        );

        for (k, _) in &results {
            assert!(
                k.as_slice() < b"sr_0025" || k.as_slice() >= b"sr_0075",
                "Range-deleted key {:?} should not appear",
                String::from_utf8_lossy(k)
            );
        }
    }

    /// # Scenario
    /// Scan shows overwritten values (latest wins) across multiple SSTables.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables.
    ///
    /// # Actions
    /// 1. Overwrite the first 30 keys with `"updated_*"` values.
    /// 2. Scan the full range.
    ///
    /// # Expected behavior
    /// Returns 100 keys. Keys 0–29 have `"updated_*"` values; keys 30–99
    /// have original padded values.
    #[test]
    fn memtable_sstable__scan_shows_overwrites() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "so");

        // Overwrite first 30 keys
        for i in 0..30 {
            let key = format!("so_{:04}", i).into_bytes();
            let val = format!("updated_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        let results = collect_scan(&engine, b"so_", b"so_\xff");
        assert_eq!(results.len(), 100);

        // First 30 should have updated values
        for (i, result) in results.iter().enumerate().take(30) {
            let expected_val = format!("updated_{:04}", i).into_bytes();
            assert_eq!(result.1, expected_val, "so_{:04} should be updated", i);
        }
        // Rest should have original values
        for (i, result) in results.iter().enumerate().take(100).skip(30) {
            let expected_val = format!("value_with_some_padding_{:04}", i).into_bytes();
            assert_eq!(result.1, expected_val, "so_{:04} should be original", i);
        }
    }

    // ----------------------------------------------------------------
    // Recovery with multiple SSTables
    // ----------------------------------------------------------------

    /// # Scenario
    /// Data spread across ≥2 SSTables survives close → reopen.
    ///
    /// # Starting environment
    /// Engine with 100 keys across ≥2 SSTables, then closed.
    ///
    /// # Actions
    /// 1. Reopen the engine.
    /// 2. Get all 100 keys.
    ///
    /// # Expected behavior
    /// Every key returns its correct padded value — all SSTable files are
    /// correctly reopened and indexed.
    #[test]
    fn memtable_sstable__reopen_preserves_data() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = engine_with_multi_sstables(tmp.path(), 100, "rc");
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        for i in 0..100 {
            let key = format!("rc_{:04}", i).into_bytes();
            let expected = format!("value_with_some_padding_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "rc_{:04} should survive reopen across multiple SSTables",
                i
            );
        }
    }
}
