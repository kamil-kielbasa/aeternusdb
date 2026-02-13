//! Scan correctness tests: ordering, dedup, tombstone filtering.

#[cfg(test)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Scan returns keys in sorted order
    // ----------------------------------------------------------------

    #[test]
    fn scan_returns_sorted_keys() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // Insert in random-ish order
        engine.put(b"d".to_vec(), b"4".to_vec()).unwrap();
        engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        engine.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        engine.put(b"e".to_vec(), b"5".to_vec()).unwrap();

        let results = collect_scan(&engine, b"a", b"f");
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();

        assert_eq!(keys, vec![b"a", b"b", b"c", b"d", b"e"]);
    }

    // ----------------------------------------------------------------
    // Scan: no duplicate keys
    // ----------------------------------------------------------------

    #[test]
    fn scan_no_duplicate_keys() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // Overwrite same key multiple times
        engine.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
        engine.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
        engine.put(b"k".to_vec(), b"v3".to_vec()).unwrap();

        let results = collect_scan(&engine, b"k", b"l");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (b"k".to_vec(), b"v3".to_vec()));
    }

    // ----------------------------------------------------------------
    // Scan: deleted keys are not returned
    // ----------------------------------------------------------------

    #[test]
    fn scan_excludes_point_deleted_keys() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        engine.put(b"c".to_vec(), b"3".to_vec()).unwrap();

        engine.delete(b"b".to_vec()).unwrap();

        let results = collect_scan(&engine, b"a", b"d");
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();

        assert_eq!(keys, vec![b"a", b"c"]);
    }

    // ----------------------------------------------------------------
    // Scan: range-deleted keys are excluded
    // ----------------------------------------------------------------

    #[test]
    fn scan_excludes_range_deleted_keys() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        for i in 0..20 {
            let key = format!("key_{:02}", i).into_bytes();
            let val = format!("val_{:02}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        // Delete [05, 15)
        engine
            .delete_range(b"key_05".to_vec(), b"key_15".to_vec())
            .unwrap();

        let results = collect_scan(&engine, b"key_00", b"key_99");
        let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();

        // Should have keys 00-04 and 15-19 = 10 keys
        assert_eq!(keys.len(), 10);
        for (k, _) in &results {
            // None of the deleted keys should appear
            assert!(k.as_slice() < b"key_05" || k.as_slice() >= b"key_15");
        }
    }

    // ----------------------------------------------------------------
    // Scan: resurrected key in range shows latest value
    // ----------------------------------------------------------------

    #[test]
    fn scan_shows_resurrected_key() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        engine.put(b"c".to_vec(), b"3".to_vec()).unwrap();

        engine.delete(b"b".to_vec()).unwrap();
        engine.put(b"b".to_vec(), b"revived".to_vec()).unwrap();

        let results = collect_scan(&engine, b"a", b"d");
        assert_eq!(results.len(), 3);
        assert_eq!(results[1], (b"b".to_vec(), b"revived".to_vec()));
    }

    // ----------------------------------------------------------------
    // Scan: empty range returns nothing
    // ----------------------------------------------------------------

    #[test]
    fn scan_empty_range() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();

        // Range with no keys
        let results = collect_scan(&engine, b"x", b"z");
        assert!(results.is_empty());
    }

    // ----------------------------------------------------------------
    // Scan: prefix range returns correct subset
    // ----------------------------------------------------------------

    #[test]
    fn scan_prefix_range() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"user:001".to_vec(), b"alice".to_vec()).unwrap();
        engine.put(b"user:002".to_vec(), b"bob".to_vec()).unwrap();
        engine.put(b"user:003".to_vec(), b"carol".to_vec()).unwrap();
        engine.put(b"item:001".to_vec(), b"phone".to_vec()).unwrap();
        engine
            .put(b"item:002".to_vec(), b"laptop".to_vec())
            .unwrap();

        let results = collect_scan(&engine, b"user:", b"user:\xff");
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"user:001".to_vec());
        assert_eq!(results[2].0, b"user:003".to_vec());
    }

    // ----------------------------------------------------------------
    // Scan across memtable + SSTable
    // ----------------------------------------------------------------

    #[test]
    fn scan_merges_memtable_and_sstable() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "sk");

        // Add fresh data to active memtable
        engine.put(b"sk_9990".to_vec(), b"fresh".to_vec()).unwrap();

        // Scan entire range
        let results = collect_scan(&engine, b"sk_", b"sk_\xff");
        assert!(results.len() >= 201, "Should merge SSTable + memtable data");

        // Verify fresh key is in the results
        assert!(
            results
                .iter()
                .any(|(k, v)| k == b"sk_9990" && v == b"fresh")
        );
    }

    // ----------------------------------------------------------------
    // Scan: overwrite in memtable shows latest across SSTable
    // ----------------------------------------------------------------

    #[test]
    fn scan_overwrite_shows_latest_value_across_layers() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "ow");

        // Overwrite key that's already in SSTable
        engine
            .put(b"ow_0050".to_vec(), b"updated".to_vec())
            .unwrap();

        let results = collect_scan(&engine, b"ow_0050", b"ow_0051");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (b"ow_0050".to_vec(), b"updated".to_vec()));
    }

    // ----------------------------------------------------------------
    // Scan: delete in memtable hides SSTable key
    // ----------------------------------------------------------------

    #[test]
    fn scan_delete_hides_sstable_key() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "sd");

        // Delete a key that exists in SSTable
        engine.delete(b"sd_0050".to_vec()).unwrap();

        let results = collect_scan(&engine, b"sd_0049", b"sd_0052");
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();

        // sd_0050 should be absent
        assert!(!keys.contains(&b"sd_0050".as_slice()));
        assert!(keys.contains(&b"sd_0049".as_slice()));
        assert!(keys.contains(&b"sd_0051".as_slice()));
    }

    // ----------------------------------------------------------------
    // Scan: range delete in memtable hides SSTable keys in scan
    // ----------------------------------------------------------------

    #[test]
    fn scan_range_delete_hides_sstable_keys() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "sr");

        // Range delete
        engine
            .delete_range(b"sr_0040".to_vec(), b"sr_0060".to_vec())
            .unwrap();

        let results = collect_scan(&engine, b"sr_0030", b"sr_0070");

        for (k, _) in &results {
            assert!(
                k.as_slice() < b"sr_0040" || k.as_slice() >= b"sr_0060",
                "Key {:?} should not appear in scan",
                String::from_utf8_lossy(k)
            );
        }

        // Should have keys 30-39 and 60-69 = 20 keys
        assert_eq!(results.len(), 20);
    }

    // ----------------------------------------------------------------
    // Scan: many overwrites → only latest value per key
    // ----------------------------------------------------------------

    #[test]
    fn scan_many_overwrites_shows_latest() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        for round in 0..5 {
            for i in 0..10 {
                let key = format!("mk_{:02}", i).into_bytes();
                let val = format!("r{}_{:02}", round, i).into_bytes();
                engine.put(key, val).unwrap();
            }
        }

        let results = collect_scan(&engine, b"mk_", b"mk_\xff");
        assert_eq!(results.len(), 10, "Should have exactly 10 unique keys");

        for (i, (k, v)) in results.iter().enumerate() {
            let expected_key = format!("mk_{:02}", i).into_bytes();
            let expected_val = format!("r4_{:02}", i).into_bytes(); // round 4 = last
            assert_eq!(k, &expected_key);
            assert_eq!(v, &expected_val);
        }
    }
}
