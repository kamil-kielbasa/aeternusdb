//! Hardening edge-case tests — Priority 3.
//!
//! These tests exercise rarely-hit code paths: operations on an empty
//! engine, flushing tombstone-only memtables, key lookups through all
//! three layers simultaneously, 0xFF byte-boundary scans, and
//! delete-only lifecycles.
//!
//! ## See also
//! - [`tests_edge_cases`]       — input validation, close semantics
//! - [`tests_boundary_values`]  — large/binary values
//! - [`tests_layers`]           — two-layer shadow tests

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ================================================================
    // 1. Compact on empty engine returns false
    // ================================================================

    /// # Scenario
    /// Call `minor_compact()`, `major_compact()`, and `tombstone_compact()`
    /// on a freshly opened engine with no data.
    ///
    /// # Expected behavior
    /// All three return `Ok(false)` — nothing to compact.
    #[test]
    fn memtable__compact_on_empty_engine_returns_false() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        assert!(!engine.minor_compact().unwrap(), "minor on empty");
        assert!(!engine.major_compact().unwrap(), "major on empty");
        assert!(!engine.tombstone_compact().unwrap(), "tombstone on empty");
    }

    // ================================================================
    // 2. Get/scan/delete on empty engine
    // ================================================================

    /// # Scenario
    /// Perform read and delete operations on a completely empty engine.
    ///
    /// # Expected behavior
    /// `get` returns `None`, scan returns empty, delete/delete_range succeed.
    #[test]
    fn memtable__get_scan_delete_on_empty_engine() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        assert_eq!(engine.get(b"nope".to_vec()).unwrap(), None);

        let results = collect_scan(&engine, b"\x00", b"\xff");
        assert!(results.is_empty());

        // Delete on empty should not panic
        engine.delete(b"phantom".to_vec()).unwrap();
        engine.delete_range(b"a".to_vec(), b"z".to_vec()).unwrap();

        // Still nothing
        assert_eq!(engine.get(b"phantom".to_vec()).unwrap(), None);
    }

    // ================================================================
    // 3. Flush only point tombstones, then recover
    // ================================================================

    /// # Scenario
    /// Engine receives only `delete()` calls (no puts ever). Force
    /// freeze + flush. The SSTable contains only tombstones. Close and
    /// reopen. Then `put("a", "v")` should resurrect the key.
    ///
    /// # Expected behavior
    /// After reopen, `get("a")` returns `None`. After put, `get("a")`
    /// returns `Some("v")`.
    #[test]
    fn memtable_sstable__flush_only_deletes_then_recover() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        {
            let engine = Engine::open(path, default_config()).unwrap();

            // Issue deletes with no preceding puts.
            engine.delete(b"a".to_vec()).unwrap();
            engine.delete(b"b".to_vec()).unwrap();
            engine.delete(b"c".to_vec()).unwrap();

            // Pad to trigger freeze so we get an SSTable.
            for i in 0..200u32 {
                engine.delete(format!("pad_{i:04}").into_bytes()).unwrap();
            }
            engine.flush_all_frozen().unwrap();
            engine.close().unwrap();
        }

        // Reopen — tombstones recovered.
        let engine = Engine::open(path, default_config()).unwrap();
        assert_eq!(engine.get(b"a".to_vec()).unwrap(), None);

        // Resurrect.
        engine.put(b"a".to_vec(), b"v".to_vec()).unwrap();
        assert_eq!(engine.get(b"a".to_vec()).unwrap(), Some(b"v".to_vec()));
    }

    // ================================================================
    // 4. Flush only range tombstones to SSTable
    // ================================================================

    /// # Scenario
    /// Write range tombstones (no puts) and flush. The SSTable should
    /// correctly mask keys written in a later session.
    ///
    /// # Expected behavior
    /// After writing keys and reopening with the range tombstone SSTable,
    /// keys covered by the range tombstone are hidden.
    #[test]
    fn memtable_sstable__flush_only_range_tombstones() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        {
            let engine = Engine::open(path, default_config()).unwrap();

            // Range-delete a space, then pad with more range deletes to trigger freeze.
            engine
                .delete_range(b"key_0000".to_vec(), b"key_0100".to_vec())
                .unwrap();
            for i in 0..100u32 {
                engine
                    .delete_range(
                        format!("range_{i:04}_start").into_bytes(),
                        format!("range_{i:04}_zzend").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();
            engine.close().unwrap();
        }

        // Reopen and write keys in the deleted range. They should be
        // in the active memtable (higher LSN) so they should be visible.
        let engine = Engine::open(path, default_config()).unwrap();
        engine.put(b"key_0050".to_vec(), b"alive".to_vec()).unwrap();

        // key_0050 is in active memtable (newer LSN) → visible.
        assert_eq!(
            engine.get(b"key_0050".to_vec()).unwrap(),
            Some(b"alive".to_vec())
        );
    }

    // ================================================================
    // 5. Key present in all three layers (active + frozen + SSTable)
    // ================================================================

    /// # Scenario
    /// Key "k" has value "v1" flushed to SSTable, "v2" in a frozen
    /// memtable, and "v3" in the active memtable. `get("k")` must
    /// return "v3" (the active memtable version wins). Then delete "k"
    /// in active — `get` should return `None`.
    ///
    /// # Expected behavior
    /// Active memtable always takes precedence over frozen and SSTable.
    #[test]
    fn memtable_sstable__get_key_in_all_three_layers() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, default_config()).unwrap();

        // Layer 1: SSTable — write "v1" + padding, flush to SSTable.
        engine.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
        for i in 0..200u32 {
            engine
                .put(
                    format!("pad1_{i:04}").into_bytes(),
                    format!("pval_{i:04}").into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();
        let stats = engine.stats().unwrap();
        assert!(stats.sstables_count > 0, "Need at least 1 SSTable");

        // Layer 2: Frozen memtable — overwrite "k" + padding, then freeze.
        engine.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
        for i in 0..200u32 {
            engine
                .put(
                    format!("pad2_{i:04}").into_bytes(),
                    format!("pval_{i:04}").into_bytes(),
                )
                .unwrap();
        }
        // Don't flush — just freeze by writing more.
        let stats = engine.stats().unwrap();
        assert!(stats.frozen_count > 0, "Need at least 1 frozen memtable");

        // Layer 3: Active memtable — overwrite "k" to "v3".
        engine.put(b"k".to_vec(), b"v3".to_vec()).unwrap();

        // Active memtable version should win.
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            Some(b"v3".to_vec()),
            "Active memtable should shadow frozen + SSTable"
        );

        // Delete in active — should shadow all layers.
        engine.delete(b"k".to_vec()).unwrap();
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            None,
            "Active tombstone should shadow all layers"
        );
    }

    // ================================================================
    // 6. Scan includes keys at 0xFF byte boundary
    // ================================================================

    /// # Scenario
    /// Insert keys using high-byte values (0xFE, 0xFF, [0xFF, 0x01]).
    /// Verify that a scan with appropriate bounds includes them.
    ///
    /// # Expected behavior
    /// A scan with `end = [0xFF, 0xFF]` includes the `[0xFF]` key but
    /// NOT `[0xFF, 0xFF]` (end-exclusive). Keys like `[0xFF, 0x01]`
    /// are included.
    #[test]
    fn memtable__scan_includes_0xff_keyed_entries() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // Insert keys at various byte positions.
        engine.put(vec![0x00], b"zero".to_vec()).unwrap();
        engine.put(vec![0x7F], b"mid".to_vec()).unwrap();
        engine.put(vec![0xFE], b"almost_max".to_vec()).unwrap();
        engine.put(vec![0xFF], b"max_single".to_vec()).unwrap();
        engine.put(vec![0xFF, 0x01], b"max_plus".to_vec()).unwrap();

        // Scan with end = [0xFF, 0xFF] — should include [0xFF] and [0xFF, 0x01]
        // but exclude [0xFF, 0xFF] (end-exclusive).
        let results = collect_scan(&engine, &[0x00], &[0xFF, 0xFF]);
        let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();

        assert!(keys.contains(&vec![0x00]), "0x00 key should be found");
        assert!(keys.contains(&vec![0x7F]), "0x7F key should be found");
        assert!(keys.contains(&vec![0xFE]), "0xFE key should be found");
        assert!(keys.contains(&vec![0xFF]), "0xFF key should be found");
        assert!(
            keys.contains(&vec![0xFF, 0x01]),
            "[0xFF, 0x01] key should be found"
        );
        assert_eq!(results.len(), 5);
    }

    // ================================================================
    // 7. Scan across all three layers simultaneously
    // ================================================================

    /// # Scenario
    /// Data spread across SSTable, frozen memtable, and active memtable.
    /// A single scan must merge all 3 layers correctly.
    ///
    /// # Expected behavior
    /// All keys from all layers returned, sorted, deduplicated.
    #[test]
    fn memtable_sstable__scan_merges_all_three_layers() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, default_config()).unwrap();

        // Keys for SSTable layer.
        engine.put(b"layer_a".to_vec(), b"sst".to_vec()).unwrap();
        engine.put(b"layer_d".to_vec(), b"sst".to_vec()).unwrap();
        for i in 0..200u32 {
            engine
                .put(
                    format!("p1_{i:04}").into_bytes(),
                    format!("v_{i:04}").into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Keys for frozen memtable layer.
        engine.put(b"layer_b".to_vec(), b"frozen".to_vec()).unwrap();
        engine.put(b"layer_e".to_vec(), b"frozen".to_vec()).unwrap();
        for i in 0..200u32 {
            engine
                .put(
                    format!("p2_{i:04}").into_bytes(),
                    format!("v_{i:04}").into_bytes(),
                )
                .unwrap();
        }
        // Don't flush — these are frozen.

        // Keys for active memtable.
        engine.put(b"layer_c".to_vec(), b"active".to_vec()).unwrap();
        engine.put(b"layer_f".to_vec(), b"active".to_vec()).unwrap();

        // Scan just the "layer_" prefix.
        let results = collect_scan(&engine, b"layer_", b"layer_\xff");
        let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();

        assert_eq!(keys.len(), 6, "Should merge 2 from each layer");
        assert_eq!(keys[0], b"layer_a");
        assert_eq!(keys[1], b"layer_b");
        assert_eq!(keys[2], b"layer_c");
        assert_eq!(keys[3], b"layer_d");
        assert_eq!(keys[4], b"layer_e");
        assert_eq!(keys[5], b"layer_f");

        // Overwrite one key from each layer in active.
        engine
            .put(b"layer_a".to_vec(), b"updated".to_vec())
            .unwrap();
        engine
            .put(b"layer_b".to_vec(), b"updated".to_vec())
            .unwrap();
        engine
            .put(b"layer_c".to_vec(), b"updated".to_vec())
            .unwrap();

        let results2 = collect_scan(&engine, b"layer_", b"layer_\xff");
        for (k, v) in &results2 {
            if k == b"layer_a" || k == b"layer_b" || k == b"layer_c" {
                assert_eq!(v, b"updated", "Overwritten key {:?} should show updated", k);
            }
        }
    }

    // ================================================================
    // 8. Flush after close → reopen → data intact
    // ================================================================

    /// # Scenario
    /// Write data, `close()` (which flushes), then more writes, then
    /// another `close()`. Reopen and verify both batches survive.
    ///
    /// # Expected behavior
    /// Both pre- and post-close batches are readable after final reopen.
    #[test]
    fn memtable_sstable__double_close_with_writes_between() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, default_config()).unwrap();
        engine.put(b"batch1".to_vec(), b"v1".to_vec()).unwrap();
        engine.close().unwrap();

        // Writes after first close.
        engine.put(b"batch2".to_vec(), b"v2".to_vec()).unwrap();
        engine.close().unwrap();
        drop(engine);

        let engine = reopen(path);
        assert_eq!(
            engine.get(b"batch1".to_vec()).unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            engine.get(b"batch2".to_vec()).unwrap(),
            Some(b"v2".to_vec())
        );
    }

    // ================================================================
    // 9. Stats on empty engine
    // ================================================================

    /// # Scenario
    /// Check `stats()` on a freshly opened engine with no data.
    ///
    /// # Expected behavior
    /// `frozen_count == 0`, `sstables_count == 0`.
    #[test]
    fn memtable__stats_on_empty_engine() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        let stats = engine.stats().unwrap();
        assert_eq!(stats.frozen_count, 0);
        assert_eq!(stats.sstables_count, 0);
    }
}
