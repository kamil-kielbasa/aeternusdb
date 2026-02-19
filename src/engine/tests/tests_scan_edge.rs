//! Scan boundary precision tests — Priority 3.
//!
//! These tests verify tricky scan boundary conditions that can reveal
//! off-by-one bugs in the range iterator: prefix keys that are byte-
//! prefixes of each other, overlapping scan ranges across multiple
//! SSTables, and scan with exactly one matching key.
//!
//! ## See also
//! - [`tests_scan`]        — standard scan ordering, dedup, tombstone filtering
//! - [`tests_edge_cases`]  — start=end, inverted range, full-keyspace scan

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ================================================================
    // 1. Prefix keys at scan boundary
    // ================================================================

    /// # Scenario
    /// Keys "key", "key1", "key10", "key2" are inserted.
    /// Scan `["key", "key1")` should return exactly `[("key", ...)]` —
    /// it includes "key" (start-inclusive) but NOT "key1" (end-exclusive),
    /// even though "key" is a byte-prefix of "key1".
    ///
    /// # Expected behavior
    /// The scan boundary separates "key" from "key1" precisely.
    #[test]
    fn memtable__scan_prefix_keys_boundary() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"key".to_vec(), b"base".to_vec()).unwrap();
        engine.put(b"key1".to_vec(), b"one".to_vec()).unwrap();
        engine.put(b"key10".to_vec(), b"ten".to_vec()).unwrap();
        engine.put(b"key2".to_vec(), b"two".to_vec()).unwrap();

        // Scan [key, key1) — exactly "key" should match.
        let results = collect_scan(&engine, b"key", b"key1");
        assert_eq!(results.len(), 1, "Only 'key' should match [key, key1)");
        assert_eq!(results[0].0, b"key");

        // Scan [key1, key2) — "key1" and "key10" should match.
        let results2 = collect_scan(&engine, b"key1", b"key2");
        assert_eq!(
            results2.len(),
            2,
            "'key1' and 'key10' should match [key1, key2)"
        );
        assert_eq!(results2[0].0, b"key1");
        assert_eq!(results2[1].0, b"key10");
    }

    // ================================================================
    // 2. Same prefix boundary through SSTable
    // ================================================================

    /// # Scenario
    /// Same as above but data is flushed to SSTables, verifying the
    /// SSTable index and iterator boundaries are equally precise.
    ///
    /// # Expected behavior
    /// Identical results after flush.
    #[test]
    fn memtable_sstable__scan_prefix_keys_boundary_through_sst() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, default_config()).unwrap();

        engine.put(b"key".to_vec(), b"base".to_vec()).unwrap();
        engine.put(b"key1".to_vec(), b"one".to_vec()).unwrap();
        engine.put(b"key10".to_vec(), b"ten".to_vec()).unwrap();
        engine.put(b"key2".to_vec(), b"two".to_vec()).unwrap();

        // Pad to force SSTable flush.
        for i in 0..200u32 {
            engine
                .put(
                    format!("pad_{i:04}").into_bytes(),
                    format!("pval_{i:04}").into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let results = collect_scan(&engine, b"key", b"key1");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"key");

        let results2 = collect_scan(&engine, b"key1", b"key2");
        assert_eq!(results2.len(), 2);
        assert_eq!(results2[0].0, b"key1");
        assert_eq!(results2[1].0, b"key10");
    }

    // ================================================================
    // 3. Scan with exactly one matching key
    // ================================================================

    /// # Scenario
    /// Scan range is set so exactly one key falls within `[start, end)`.
    ///
    /// # Expected behavior
    /// Exactly one result returned.
    #[test]
    fn memtable__scan_exactly_one_match() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"aaa".to_vec(), b"1".to_vec()).unwrap();
        engine.put(b"bbb".to_vec(), b"2".to_vec()).unwrap();
        engine.put(b"ccc".to_vec(), b"3".to_vec()).unwrap();

        // Scan [bbb, bbc) — only "bbb" matches.
        let results = collect_scan(&engine, b"bbb", b"bbc");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (b"bbb".to_vec(), b"2".to_vec()));
    }

    // ================================================================
    // 4. Scan with adjacent ranges — no overlap, no gap
    // ================================================================

    /// # Scenario
    /// Two consecutive scans with adjacen ranges should together cover
    /// all keys without overlap or gap.
    ///
    /// # Expected behavior
    /// Union of both scans == full scan. No key appears in both.
    #[test]
    fn memtable__scan_adjacent_ranges_no_gap() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        for i in 0..10u32 {
            engine
                .put(
                    format!("k_{i:02}").into_bytes(),
                    format!("v_{i:02}").into_bytes(),
                )
                .unwrap();
        }

        // Split at "k_05": [k_00, k_05) and [k_05, k_99).
        let left = collect_scan(&engine, b"k_00", b"k_05");
        let right = collect_scan(&engine, b"k_05", b"k_99");

        assert_eq!(left.len(), 5, "k_00..k_04");
        assert_eq!(right.len(), 5, "k_05..k_09");

        // No overlap.
        let left_keys: Vec<_> = left.iter().map(|(k, _)| k.clone()).collect();
        let right_keys: Vec<_> = right.iter().map(|(k, _)| k.clone()).collect();
        for k in &left_keys {
            assert!(!right_keys.contains(k), "Overlap detected for {:?}", k);
        }

        // Together they equal the full scan.
        let full = collect_scan(&engine, b"k_00", b"k_99");
        assert_eq!(full.len(), 10);
    }

    // ================================================================
    // 5. Scan with deleted key at boundary
    // ================================================================

    /// # Scenario
    /// The scan start key is itself deleted. The scan should skip it
    /// and return the next live key.
    ///
    /// # Expected behavior
    /// Deleted start key is not returned.
    #[test]
    fn memtable__scan_deleted_start_key_skipped() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        engine.put(b"c".to_vec(), b"3".to_vec()).unwrap();

        engine.delete(b"a".to_vec()).unwrap();

        // Scan starting at "a" — "a" is deleted, should not appear.
        let results = collect_scan(&engine, b"a", b"d");
        let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"b", b"c"]);
    }
}
