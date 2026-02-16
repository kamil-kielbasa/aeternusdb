//! Scan correctness tests: ordering, dedup, tombstone filtering.
//!
//! These tests verify the `scan()` range-query API. A correct scan must:
//! 1. Return keys in strictly sorted (lexicographic) order.
//! 2. Deduplicate keys that have been overwritten, returning only the latest value.
//! 3. Exclude keys hidden by point-delete or range-delete tombstones.
//! 4. Correctly merge results from the active memtable, frozen memtables,
//!    and SSTables when data spans multiple storage layers.
//!
//! ## Layer coverage
//! - `memtable__*`: memtable only (64 KB buffer — all data in memory)
//! - `memtable_sstable__*`: memtable + SSTable merge path (4 KB buffer)
//!
//! ## See also
//! - [`tests_multi_sstable`] — scan across ≥2 SSTables
//! - [`tests_hardening`] `visibility_*` — edge cases for scan visibility
//! - [`tests_edge_cases`] — scan boundary semantics (start=end, inverted ranges)

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Scan returns keys in sorted order
    // ----------------------------------------------------------------

    /// # Scenario
    /// Scan must return keys in ascending lexicographic order regardless of
    /// insertion order.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Insert keys `"d"`, `"a"`, `"c"`, `"b"`, `"e"` (deliberately unordered).
    /// 2. Scan range `["a", "f")`.
    ///
    /// # Expected behavior
    /// Keys are returned in sorted order: `["a", "b", "c", "d", "e"]`.
    #[test]
    fn memtable__scan_returns_sorted_keys() {
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

    /// # Scenario
    /// Overwriting a key multiple times must not produce duplicate entries
    /// in the scan output.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Put key `"k"` three times: `"v1"`, `"v2"`, `"v3"`.
    /// 2. Scan range `["k", "l")`.
    ///
    /// # Expected behavior
    /// Scan returns exactly 1 entry: `("k", "v3")` — only the latest value.
    #[test]
    fn memtable__scan_no_duplicate_keys() {
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

    /// # Scenario
    /// Point-deleted keys must be excluded from scan results.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Put keys `"a"`, `"b"`, `"c"`.
    /// 2. Delete key `"b"`.
    /// 3. Scan range `["a", "d")`.
    ///
    /// # Expected behavior
    /// Scan returns `["a", "c"]` — `"b"` is hidden by its tombstone.
    #[test]
    fn memtable__scan_excludes_point_deleted_keys() {
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

    /// # Scenario
    /// Range-deleted keys must be excluded from scan results.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Put 20 keys (`key_00`..`key_19`).
    /// 2. Range-delete `["key_05", "key_15")` — covers keys 05–14.
    /// 3. Scan range `["key_00", "key_99")`.
    ///
    /// # Expected behavior
    /// Returns 10 keys: `key_00`–`key_04` and `key_15`–`key_19`.
    /// All keys within the range-delete interval are excluded.
    #[test]
    fn memtable__scan_excludes_range_deleted_keys() {
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

    /// # Scenario
    /// A key that was deleted and then re-inserted (resurrected) must appear
    /// in the scan with its latest value.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Put `"a"`, `"b"`, `"c"`.
    /// 2. Delete `"b"`.
    /// 3. Re-insert `"b"` = `"revived"`.
    /// 4. Scan range `["a", "d")`.
    ///
    /// # Expected behavior
    /// Scan returns 3 keys; `"b"` appears with value `"revived"` — the
    /// re-insert (highest LSN) overrides the tombstone.
    #[test]
    fn memtable__scan_shows_resurrected_key() {
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

    /// # Scenario
    /// Scan over a range that contains no keys.
    ///
    /// # Starting environment
    /// Engine with two keys `"a"` and `"b"`.
    ///
    /// # Actions
    /// 1. Scan range `["x", "z")` — no keys exist in this range.
    ///
    /// # Expected behavior
    /// Returns an empty result.
    #[test]
    fn memtable__scan_empty_range() {
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

    /// # Scenario
    /// Scan using a prefix range to retrieve a logical subset of keys.
    ///
    /// # Starting environment
    /// Engine with 3 `"user:"` keys and 2 `"item:"` keys.
    ///
    /// # Actions
    /// 1. Scan range `["user:", "user:\xff")` — prefix scan for user keys.
    ///
    /// # Expected behavior
    /// Returns exactly the 3 `"user:"` keys in sorted order; `"item:"` keys
    /// are outside the range and excluded.
    #[test]
    fn memtable__scan_prefix_range() {
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

    /// # Scenario
    /// Scan merges data from SSTables and the active memtable.
    ///
    /// # Starting environment
    /// Engine with 200 keys flushed to SSTables (via `engine_with_sstables`).
    ///
    /// # Actions
    /// 1. Insert a fresh key `"sk_9990"` = `"fresh"` into the active memtable.
    /// 2. Scan range `["sk_", "sk_\xff")`.
    ///
    /// # Expected behavior
    /// Scan returns ≥201 keys (200 from SSTables + 1 from memtable),
    /// including `("sk_9990", "fresh")` — the merge correctly combines
    /// data from both layers.
    #[test]
    fn memtable_sstable__scan_merges_layers() {
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

    /// # Scenario
    /// Overwriting a key that exists in an SSTable — scan must show
    /// the latest (memtable) value.
    ///
    /// # Starting environment
    /// Engine with 200 keys flushed to SSTables; `ow_0050` has an older
    /// value in the SSTable.
    ///
    /// # Actions
    /// 1. Overwrite `ow_0050` = `"updated"` in the active memtable.
    /// 2. Scan the narrow range `["ow_0050", "ow_0051")`.
    ///
    /// # Expected behavior
    /// Returns exactly 1 entry: `("ow_0050", "updated")` — the memtable
    /// value (higher LSN) wins over the SSTable value.
    #[test]
    fn memtable_sstable__scan_overwrite_shows_latest() {
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

    /// # Scenario
    /// A point-delete in the memtable hides an SSTable key from scan.
    ///
    /// # Starting environment
    /// Engine with 200 keys flushed to SSTables.
    ///
    /// # Actions
    /// 1. Delete `sd_0050` from the active memtable.
    /// 2. Scan range `["sd_0049", "sd_0052")`.
    ///
    /// # Expected behavior
    /// `sd_0050` is absent from the results; `sd_0049` and `sd_0051` are present.
    /// The memtable tombstone correctly masks the SSTable entry during scan.
    #[test]
    fn memtable_sstable__scan_delete_hides_key() {
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

    /// # Scenario
    /// A range-delete in the memtable hides multiple SSTable keys from scan.
    ///
    /// # Starting environment
    /// Engine with 200 keys flushed to SSTables.
    ///
    /// # Actions
    /// 1. Range-delete `["sr_0040", "sr_0060")` from the memtable.
    /// 2. Scan range `["sr_0030", "sr_0070")`.
    ///
    /// # Expected behavior
    /// Returns 20 keys: `sr_0030`–`sr_0039` and `sr_0060`–`sr_0069`.
    /// The 20 keys inside the range-delete interval are excluded.
    #[test]
    fn memtable_sstable__scan_range_delete_hides_keys() {
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

    /// # Scenario
    /// Multiple overwrites of the same keys — scan must show only the
    /// latest value per key.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Overwrite 10 keys (`mk_00`..`mk_09`) across 5 rounds (round 0–4).
    /// 2. Scan range `["mk_", "mk_\xff")`.
    ///
    /// # Expected behavior
    /// Returns exactly 10 entries, each with the value from round 4
    /// (the last round). No duplicates from earlier rounds.
    #[test]
    fn memtable__scan_many_overwrites_shows_latest() {
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
