//! Multi-SSTable correctness tests.
//!
//! Every test in this module guarantees that data is spread across at least 2
//! SSTables (1 KB write buffer → ~1 KB+ per SSTable) so that reads, deletes,
//! and scans must merge results from multiple on-disk tables.

#[cfg(test)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Get across multiple SSTables
    // ----------------------------------------------------------------

    #[test]
    fn get_all_keys_across_multiple_sstables() {
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

    #[test]
    fn overwrite_across_multiple_sstables() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), multi_sstable_config()).unwrap();

        // Round 1: initial values → spread across SSTables
        for i in 0..80 {
            let key = format!("ov_{:04}", i).into_bytes();
            let val = format!("round1_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        let s1 = engine.stats().unwrap().sstables_count;
        assert!(s1 >= 2, "Expected >= 2 SSTables after round 1, got {}", s1);

        // Round 2: overwrite all keys → creates additional SSTables
        for i in 0..80 {
            let key = format!("ov_{:04}", i).into_bytes();
            let val = format!("round2_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
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

    #[test]
    fn point_delete_hides_key_in_older_sstable() {
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

    #[test]
    fn delete_and_reinsert_across_sstables() {
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

    #[test]
    fn range_delete_spans_multiple_sstables() {
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

    #[test]
    fn overlapping_range_deletes_across_sstables() {
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

    #[test]
    fn range_delete_in_sstable_then_put_in_memtable() {
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

    #[test]
    fn newer_point_delete_beats_older_sstable_put() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "np");

        // Overwrite key in older SSTable, then delete
        engine
            .put(b"np_0050".to_vec(), b"updated".to_vec())
            .unwrap();
        engine.delete(b"np_0050".to_vec()).unwrap();

        assert_eq!(engine.get(b"np_0050".to_vec()).unwrap(), None);
    }

    #[test]
    fn interleaved_deletes_and_puts_across_sstables() {
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

    #[test]
    fn scan_sorted_and_deduped_across_sstables() {
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

    #[test]
    fn scan_excludes_deletes_across_sstables() {
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

    #[test]
    fn scan_excludes_range_deletes_across_sstables() {
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

    #[test]
    fn scan_shows_overwrites_across_sstables() {
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
        for i in 0..30 {
            let expected_val = format!("updated_{:04}", i).into_bytes();
            assert_eq!(results[i].1, expected_val, "so_{:04} should be updated", i);
        }
        // Rest should have original values
        for i in 30..100 {
            let expected_val = format!("value_with_some_padding_{:04}", i).into_bytes();
            assert_eq!(results[i].1, expected_val, "so_{:04} should be original", i);
        }
    }

    // ----------------------------------------------------------------
    // Recovery with multiple SSTables
    // ----------------------------------------------------------------

    #[test]
    fn reopen_preserves_data_across_multiple_sstables() {
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
