//! LSN-continuity-after-crash tests.
//!
//! These tests verify that when the engine is dropped without `close()`
//! (simulating a crash), the next `Engine::open()` resumes the LSN
//! counter above the maximum LSN found in any persisted data source
//! (WAL replay, frozen memtables, SSTables, manifest).
//!
//! Without correct LSN resumption, post-crash writes could reuse LSNs
//! already assigned to pre-crash data, breaking the "highest-LSN wins"
//! merge invariant and causing data corruption.
//!
//! ## See also
//! - [`tests_lsn_continuity`] — LSN continuity across clean reopens
//! - [`tests_crash_recovery`] — single crash durability
//! - [`tests_multi_crash`] — multiple consecutive crash cycles

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ================================================================
    // 1. Overwrite after crash shadows old value
    // ================================================================

    /// # Scenario
    /// After a crash (drop without close), a post-recovery overwrite
    /// must shadow the pre-crash value because it receives a higher LSN.
    ///
    /// # Actions
    /// 1. Put `"k"` = `"old"`, drop (crash).
    /// 2. Reopen, put `"k"` = `"new"`, verify.
    ///
    /// # Expected behavior
    /// `get("k")` returns `"new"`.
    #[test]
    fn memtable__overwrite_after_crash_shadows_old() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        {
            let engine = Engine::open(path, default_config()).unwrap();
            engine.put(b"k".to_vec(), b"old".to_vec()).unwrap();
            // Drop — crash.
        }

        let engine = Engine::open(path, default_config()).unwrap();
        engine.put(b"k".to_vec(), b"new".to_vec()).unwrap();
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            Some(b"new".to_vec()),
            "post-crash overwrite must shadow old value"
        );
    }

    // ================================================================
    // 2. Delete after crash hides old put
    // ================================================================

    /// # Scenario
    /// A tombstone written after crash recovery hides a pre-crash put.
    ///
    /// # Actions
    /// 1. Put `"k"` = `"v"`, drop (crash).
    /// 2. Reopen, delete `"k"`, verify.
    ///
    /// # Expected behavior
    /// `get("k")` returns `None`.
    #[test]
    fn memtable__delete_after_crash_hides_old_put() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        {
            let engine = Engine::open(path, default_config()).unwrap();
            engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        }

        let engine = Engine::open(path, default_config()).unwrap();
        engine.delete(b"k".to_vec()).unwrap();
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            None,
            "post-crash delete must hide old put"
        );
    }

    // ================================================================
    // 3. Range-delete after crash hides old puts
    // ================================================================

    /// # Scenario
    /// A range-delete written after crash recovery hides multiple
    /// pre-crash puts.
    ///
    /// # Actions
    /// 1. Write keys 0..10, drop (crash).
    /// 2. Reopen, range-delete [key_03, key_07), verify.
    ///
    /// # Expected behavior
    /// Keys 3-6 return `None`. Others return original values.
    #[test]
    fn memtable__range_delete_after_crash_hides_old_puts() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        {
            let engine = Engine::open(path, default_config()).unwrap();
            for i in 0..10u8 {
                engine
                    .put(
                        format!("key_{i:02}").into_bytes(),
                        format!("val_{i:02}").into_bytes(),
                    )
                    .unwrap();
            }
        }

        let engine = Engine::open(path, default_config()).unwrap();
        engine
            .delete_range(b"key_03".to_vec(), b"key_07".to_vec())
            .unwrap();

        for i in 0..10u8 {
            let key = format!("key_{i:02}").into_bytes();
            let val = engine.get(key).unwrap();
            if (3..7).contains(&i) {
                assert_eq!(val, None, "key_{i:02} should be range-deleted after crash");
            } else {
                assert_eq!(
                    val,
                    Some(format!("val_{i:02}").into_bytes()),
                    "key_{i:02} should survive"
                );
            }
        }
    }

    // ================================================================
    // 4. LSN continuity across multiple crash cycles
    // ================================================================

    /// # Scenario
    /// LSN counter correctly resumes across three crash (drop) cycles,
    /// ensuring the latest overwrite always wins.
    ///
    /// # Actions
    /// 1. Cycle 1: put `"k"` = `"v1"`, drop.
    /// 2. Cycle 2: reopen, put `"k"` = `"v2"`, drop.
    /// 3. Cycle 3: reopen, put `"k"` = `"v3"`, drop.
    /// 4. Final: reopen, get `"k"`.
    ///
    /// # Expected behavior
    /// Returns `"v3"` — each crash cycle gets a higher base LSN.
    #[test]
    fn memtable__lsn_continuity_across_crash_cycles() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        for i in 1..=3 {
            let engine = Engine::open(path, default_config()).unwrap();
            engine
                .put(b"k".to_vec(), format!("v{i}").into_bytes())
                .unwrap();
            // Drop — crash.
        }

        let engine = Engine::open(path, default_config()).unwrap();
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            Some(b"v3".to_vec()),
            "Most recent crash-cycle write must win"
        );
    }

    // ================================================================
    // 5. LSN continuity after crash with SSTables
    // ================================================================

    /// # Scenario
    /// After crash with data in SSTables (flushed) + unflushed memtable,
    /// a post-recovery overwrite must shadow SSTable values.
    ///
    /// # Actions
    /// 1. Write 30 keys with small buffer (creates SSTables).
    /// 2. Flush all frozen, write key_0010 = "pre-crash", drop (crash).
    /// 3. Reopen, put key_0010 = "post-crash", scan.
    ///
    /// # Expected behavior
    /// Scan shows key_0010 = "post-crash" (post-crash LSN > SSTable LSN).
    #[test]
    fn memtable_sstable__overwrite_after_crash_shadows_sstable() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();
            for i in 0..30u32 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("old_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();
            // Write a final value that stays in memtable.
            engine
                .put(b"key_0010".to_vec(), b"pre-crash".to_vec())
                .unwrap();
            // Drop — crash.
        }

        let engine = Engine::open(path, small_buffer_config()).unwrap();
        engine
            .put(b"key_0010".to_vec(), b"post-crash".to_vec())
            .unwrap();

        let results = collect_scan(&engine, b"key_", b"key_\xff");
        let entry = results.iter().find(|(k, _)| k == b"key_0010").unwrap();
        assert_eq!(
            entry.1,
            b"post-crash".to_vec(),
            "post-crash overwrite must shadow SSTable & WAL values"
        );
    }

    // ================================================================
    // 6. LSN monotonicity: post-crash writes get strictly higher LSNs
    // ================================================================

    /// # Scenario
    /// Verify that the LSN assigned to the first post-crash value is
    /// strictly higher than the max LSN of any pre-crash record,
    /// indirectly validated by overwrite semantics on multiple keys.
    ///
    /// # Actions
    /// 1. Write keys a, b, c with values v1. Drop (crash).
    /// 2. Reopen. Overwrite only b="v2". Read all three.
    ///
    /// # Expected behavior
    /// a="v1", b="v2", c="v1". The overwrite of b works only if the
    /// post-crash LSN is strictly higher than the recovered LSN of b.
    #[test]
    fn memtable__post_crash_lsn_strictly_higher() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        {
            let engine = Engine::open(path, default_config()).unwrap();
            engine.put(b"a".to_vec(), b"v1".to_vec()).unwrap();
            engine.put(b"b".to_vec(), b"v1".to_vec()).unwrap();
            engine.put(b"c".to_vec(), b"v1".to_vec()).unwrap();
        }

        let engine = Engine::open(path, default_config()).unwrap();
        engine.put(b"b".to_vec(), b"v2".to_vec()).unwrap();

        assert_eq!(
            engine.get(b"a".to_vec()).unwrap(),
            Some(b"v1".to_vec()),
            "a untouched"
        );
        assert_eq!(
            engine.get(b"b".to_vec()).unwrap(),
            Some(b"v2".to_vec()),
            "b must be overwritten — post-crash LSN > recovered LSN"
        );
        assert_eq!(
            engine.get(b"c".to_vec()).unwrap(),
            Some(b"v1".to_vec()),
            "c untouched"
        );
    }

    // ================================================================
    // 7. Multiple crash cycles with flush in between
    // ================================================================

    /// # Scenario
    /// Three crash cycles where data moves to SSTables between crashes.
    /// Each cycle's overwrite to the same key must win.
    ///
    /// # Actions
    /// 1. Write key="k", value="sstable-1", flush to SSTable. Drop.
    /// 2. Reopen. Overwrite key="k", value="sstable-2", flush. Drop.
    /// 3. Reopen. Overwrite key="k", value="memtable-3" (no flush). Drop.
    /// 4. Final: reopen, get "k".
    ///
    /// # Expected behavior
    /// Returns "memtable-3" — the latest overwrite wins across all layers.
    #[test]
    fn memtable_sstable__lsn_across_crash_cycles_with_flush() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // Cycle 1: write + flush to SSTable + crash.
        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();
            engine.put(b"k".to_vec(), b"sstable-1".to_vec()).unwrap();
            // Write enough to trigger a frozen memtable with small buffer.
            for i in 0..20u32 {
                engine
                    .put(format!("pad_{i:04}").into_bytes(), b"x".repeat(64))
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();
        }

        // Cycle 2: overwrite + flush + crash.
        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();
            engine.put(b"k".to_vec(), b"sstable-2".to_vec()).unwrap();
            for i in 20..40u32 {
                engine
                    .put(format!("pad_{i:04}").into_bytes(), b"x".repeat(64))
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();
        }

        // Cycle 3: overwrite in memtable only + crash.
        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();
            engine.put(b"k".to_vec(), b"memtable-3".to_vec()).unwrap();
        }

        // Final verification.
        let engine = Engine::open(path, small_buffer_config()).unwrap();
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            Some(b"memtable-3".to_vec()),
            "Latest crash-cycle overwrite (memtable) must win over SSTable data"
        );
    }
}
