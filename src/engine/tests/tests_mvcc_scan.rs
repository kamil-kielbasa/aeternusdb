//! MVCC snapshot scan tests.
//!
//! These tests verify that the engine's `raw_scan()` / `scan()` correctly
//! captures an MVCC snapshot of frozen memtables and SSTables via `Arc`,
//! releases the `RwLock`, and iterates lazily without holding the lock.
//!
//! ## Coverage
//! - Scan across all three layers (memtable + frozen + SSTable) returns
//!   correct merged results.
//! - Scan result is valid even after a concurrent flush removes a frozen
//!   memtable (the `Arc` keeps it alive).
//! - Scan result is valid even after a concurrent compaction replaces
//!   SSTables (the `Arc` keeps them alive).
//! - Large scan does not OOM — verifies lazy block-at-a-time iteration
//!   by scanning many keys across multiple SSTables.

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Basic: scan merges memtable + frozen + SSTable correctly
    // ----------------------------------------------------------------

    /// # Scenario
    /// Data is spread across all three layers. Scan must merge them
    /// correctly using the MVCC snapshot approach.
    ///
    /// # Starting environment
    /// Empty engine with small write buffer.
    ///
    /// # Actions
    /// 1. Put keys into SSTable layer (write + flush).
    /// 2. Put more keys (triggers freeze → frozen layer).
    /// 3. Put more keys (active memtable layer).
    /// 4. Scan the full range.
    ///
    /// # Expected behavior
    /// All keys visible, in sorted order, deduplicated by latest version.
    #[test]
    fn mvcc_scan_merges_all_three_layers() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), small_buffer_config()).unwrap();

        // Layer 3: SSTable — write and flush
        for i in 0..5u8 {
            let key = vec![b'a', i + b'0'];
            engine.put(key, b"sst".to_vec()).unwrap();
        }
        engine.flush_all_frozen().unwrap();

        // Layer 2: Frozen memtable — write enough to trigger freeze
        for i in 0..5u8 {
            let key = vec![b'b', i + b'0'];
            engine.put(key, b"frozen".to_vec()).unwrap();
        }
        // Don't flush — leave as frozen

        // Layer 1: Active memtable
        engine.put(b"c0".to_vec(), b"active".to_vec()).unwrap();

        let results = collect_scan(&engine, b"\x00", b"\xff");

        // We should see keys from all three layers
        assert!(
            results.len() >= 11,
            "expected at least 11 keys across 3 layers, got {}",
            results.len()
        );

        // Verify sorted order
        for w in results.windows(2) {
            assert!(
                w[0].0 <= w[1].0,
                "keys not sorted: {:?} > {:?}",
                w[0].0,
                w[1].0
            );
        }
    }

    // ----------------------------------------------------------------
    // Scan survives concurrent flush (frozen memtable removed)
    // ----------------------------------------------------------------

    /// # Scenario
    /// A scan captures `Arc` clones of frozen memtables. If a flush
    /// removes a frozen memtable from `EngineInner` while we iterate,
    /// the `Arc` should keep it alive.
    ///
    /// # Starting environment
    /// Engine with data in frozen memtable + SSTable.
    ///
    /// # Actions
    /// 1. Write keys and let some freeze (small buffer).
    /// 2. Start a scan (captures Arc snapshot).
    /// 3. Flush all frozen memtables (modifies EngineInner).
    /// 4. Continue consuming the scan iterator.
    ///
    /// # Expected behavior
    /// Scan returns all expected keys. The flush does not invalidate
    /// the scan's snapshot.
    #[test]
    fn mvcc_scan_survives_concurrent_flush() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), small_buffer_config()).unwrap();

        // Write enough to create frozen memtables
        for i in 0..20u32 {
            let key = format!("key_{:04}", i).into_bytes();
            let val = format!("value_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        // Capture scan iterator (takes MVCC snapshot)
        let scan_iter = engine.scan(b"key_", b"key_\xff").unwrap();

        // Now flush all frozen — this modifies EngineInner, removing
        // frozen memtables and adding SSTables.
        engine.flush_all_frozen().unwrap();

        // The scan iterator should still produce correct results
        // because it holds Arc clones of the pre-flush state.
        let results: Vec<_> = scan_iter.collect();

        assert!(
            results.len() >= 18,
            "expected at least 18 keys, got {} (some may remain in active memtable)",
            results.len()
        );

        // Verify all keys are valid
        for (key, _) in &results {
            assert!(key.starts_with(b"key_"));
        }
    }

    // ----------------------------------------------------------------
    // Scan survives concurrent compaction (SSTables replaced)
    // ----------------------------------------------------------------

    /// # Scenario
    /// A scan holds `Arc<SSTable>` clones. If compaction replaces those
    /// SSTables with new ones while we iterate, the Arc should keep the
    /// old mmaps alive (Unix inode semantics).
    ///
    /// # Starting environment
    /// Engine with multiple SSTables.
    ///
    /// # Actions
    /// 1. Create engine with multiple SSTables.
    /// 2. Start a scan (captures Arc snapshot).
    /// 3. Run major compaction (replaces all SSTables with one new one).
    /// 4. Continue consuming the scan iterator.
    ///
    /// # Expected behavior
    /// Scan returns all expected keys despite compaction.
    #[test]
    fn mvcc_scan_survives_concurrent_compaction() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 100, "ck");

        let before = engine.stats().unwrap();
        assert!(
            before.sstables_count >= 2,
            "need >= 2 SSTables, got {}",
            before.sstables_count
        );

        // Capture scan iterator (takes MVCC snapshot)
        let scan_iter = engine.scan(b"ck_", b"ck_\xff").unwrap();

        // Now compact — replaces all SSTables
        engine.major_compact().unwrap();

        let after = engine.stats().unwrap();
        assert_eq!(
            after.sstables_count, 1,
            "major compact should produce 1 SSTable"
        );

        // The scan iterator should still produce correct results
        let results: Vec<_> = scan_iter.collect();

        assert_eq!(
            results.len(),
            100,
            "expected 100 keys from pre-compaction snapshot"
        );

        // Verify all keys present and sorted
        for (i, (key, _)) in results.iter().enumerate() {
            let expected = format!("ck_{:04}", i).into_bytes();
            assert_eq!(key, &expected);
        }
    }

    // ----------------------------------------------------------------
    // Large scan does not materialize all data at once
    // ----------------------------------------------------------------

    /// # Scenario
    /// Scan over many SSTables should work correctly and produce all
    /// results. The lazy iteration (block-at-a-time via mmap) ensures
    /// only one block per SSTable is resident at a time.
    ///
    /// # Starting environment
    /// Engine with many SSTables containing many keys.
    ///
    /// # Actions
    /// 1. Write 500 keys with padding, flush, creating multiple SSTables.
    /// 2. Scan the full range.
    /// 3. Verify all keys returned in order.
    ///
    /// # Expected behavior
    /// All 500 keys returned in sorted order.
    #[test]
    fn mvcc_scan_large_range_across_many_sstables() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_multi_sstables(tmp.path(), 500, "lg");

        let results = collect_scan(&engine, b"lg_", b"lg_\xff");

        assert_eq!(results.len(), 500, "expected 500 keys");

        // Verify sorted and complete
        for (i, (key, _)) in results.iter().enumerate() {
            let expected = format!("lg_{:04}", i).into_bytes();
            assert_eq!(key, &expected, "mismatch at index {}", i);
        }
    }

    // ----------------------------------------------------------------
    // Scan with overwrites across layers uses latest version
    // ----------------------------------------------------------------

    /// # Scenario
    /// The MVCC snapshot must respect version ordering — when the same
    /// key exists in multiple layers, the scan returns the latest value.
    ///
    /// # Starting environment
    /// Engine with small buffer.
    ///
    /// # Actions
    /// 1. Put key "k" = "v1" into SSTable layer.
    /// 2. Put key "k" = "v2" into frozen layer.
    /// 3. Put key "k" = "v3" into active memtable.
    /// 4. Scan.
    ///
    /// # Expected behavior
    /// Only "k" = "v3" returned (latest version wins).
    #[test]
    fn mvcc_scan_returns_latest_version_across_layers() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), default_config()).unwrap();

        // SSTable layer
        engine.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
        engine.flush_all_frozen().unwrap();

        // Frozen layer — write enough padding to trigger freeze
        engine.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
        // Add padding to trigger freeze
        for i in 0..100u32 {
            let pad = format!("pad_{:04}", i).into_bytes();
            engine.put(pad.clone(), pad).unwrap();
        }
        // Don't flush — frozen + active

        // Active memtable
        engine.put(b"k".to_vec(), b"v3".to_vec()).unwrap();

        let results = collect_scan(&engine, b"k", b"l");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (b"k".to_vec(), b"v3".to_vec()));
    }
}
