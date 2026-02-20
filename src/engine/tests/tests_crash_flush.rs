//! Crash-during-flush tests.
//!
//! These tests simulate crashes at critical points during the SSTable flush
//! pipeline. A flush converts a frozen memtable into an on-disk SSTable and
//! updates the manifest. A crash can strike at several points:
//!
//! 1. **Before rename** — `.tmp` file exists but final `.sst` does not.
//! 2. **After rename, before manifest** — `.sst` exists on disk but the
//!    manifest does not reference it (orphan SSTable).
//! 3. **Frozen WALs survive** — the frozen memtable's WAL on disk was
//!    never removed, so WAL replay recreates the data.
//!
//! In all cases the engine must recover all committed data by replaying
//! the frozen WALs and cleaning up any debris (`.tmp` files, orphan SSTables).
//!
//! ## See also
//! - [`tests_crash_recovery`] — drop-without-close crash simulation
//! - [`tests_recovery`] — clean close → reopen path

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::tests::helpers::*;
    use crate::engine::{Engine, SSTABLE_DIR};
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    // ================================================================
    // 1. Leftover .tmp file — crash before rename
    // ================================================================

    /// # Scenario
    /// A crash occurs while writing an SSTable, leaving a `.tmp` file
    /// in the sstables directory. On restart the engine must:
    /// - Not crash or error on the `.tmp` file
    /// - Recover all data from frozen WALs
    /// - Not include the partial `.tmp` as a valid SSTable
    ///
    /// # Starting environment
    /// Engine with data written and frozen memtable(s) created.
    ///
    /// # Actions
    /// 1. Write data until at least one frozen memtable exists.
    /// 2. Drop engine (simulating crash — frozen not flushed).
    /// 3. Plant a fake `.tmp` file in the sstables directory.
    /// 4. Reopen engine.
    /// 5. Verify all data is recovered.
    /// 6. Verify the `.tmp` file is either ignored or still present
    ///    (engine does not crash on it).
    ///
    /// # Expected behavior
    /// All committed data is recovered via WAL replay. The `.tmp` file
    /// does not interfere with operation.
    #[test]
    fn crash_before_rename_tmp_file_ignored() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // Phase 1: Write data, trigger freeze, then drop (crash).
        let keys_written;
        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();
            let mut i = 0;
            loop {
                let key = format!("key_{i:04}").into_bytes();
                let value = format!("val_{i:04}").into_bytes();
                engine.put(key, value).unwrap();
                i += 1;
                let stats = engine.stats().unwrap();
                if stats.frozen_count > 0 {
                    break;
                }
                assert!(i < 10_000, "Expected freeze within 10000 puts");
            }
            keys_written = i;
            // Drop without close — simulates crash.
        }

        // Phase 2: Plant a fake .tmp file (simulating incomplete flush).
        let sst_dir = path.join(SSTABLE_DIR);
        let tmp_file_path = sst_dir.join("999999.tmp");
        {
            let mut f = File::create(&tmp_file_path).unwrap();
            f.write_all(b"incomplete sstable data garbage").unwrap();
            f.sync_all().unwrap();
        }
        assert!(
            tmp_file_path.exists(),
            ".tmp file should exist before reopen"
        );

        // Phase 3: Reopen — must not crash.
        let engine = Engine::open(path, small_buffer_config()).unwrap();

        // Phase 4: All data must be recovered from WAL replay.
        for i in 0..keys_written {
            let key = format!("key_{i:04}").into_bytes();
            let result = engine.get(key).unwrap();
            assert!(
                result.is_some(),
                "Key key_{i:04} should be recovered after crash"
            );
        }
    }

    // ================================================================
    // 2. Orphan .sst file — crash after rename, before manifest
    // ================================================================

    /// # Scenario
    /// Crash occurs after the SSTable file was atomically renamed from
    /// `.tmp` to `.sst`, but before the manifest recorded the `AddSst`
    /// event. This leaves an orphan `.sst` file that the manifest
    /// doesn't know about.
    ///
    /// # Starting environment
    /// Engine with some SSTables already flushed and recorded in manifest.
    ///
    /// # Actions
    /// 1. Write data, flush to SSTables, close cleanly.
    /// 2. Plant an orphan `.sst` file with a high ID not in manifest.
    /// 3. Reopen engine.
    /// 4. Verify orphan `.sst` is cleaned up (deleted by orphan cleanup).
    /// 5. Verify all legitimate data is intact.
    ///
    /// # Expected behavior
    /// Orphan `.sst` file is removed during `Engine::open()`.
    /// All manifest-tracked data is untouched.
    #[test]
    fn crash_after_rename_orphan_sst_cleaned_up() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // Phase 1: Write data, flush, close cleanly.
        {
            let engine = engine_with_sstables(path, 100, "key");
            engine.close().unwrap();
        }

        // Phase 2: Plant orphan SSTable.
        let sst_dir = path.join(SSTABLE_DIR);
        let orphan_path = sst_dir.join("999999.sst");
        {
            let mut f = File::create(&orphan_path).unwrap();
            f.write_all(b"fake sstable content").unwrap();
            f.sync_all().unwrap();
        }
        assert!(orphan_path.exists(), "Orphan should exist before reopen");

        // Phase 3: Reopen — orphan should be cleaned up.
        let engine = Engine::open(path, default_config()).unwrap();

        assert!(
            !orphan_path.exists(),
            "Orphan 999999.sst should be deleted on recovery"
        );

        // Phase 4: All data intact.
        for i in 0..100 {
            let key = format!("key_{i:04}").into_bytes();
            let result = engine.get(key).unwrap();
            assert!(result.is_some(), "Key key_{i:04} should survive");
        }
    }

    // ================================================================
    // 3. Frozen WAL survives crash — data recovered via replay
    // ================================================================

    /// # Scenario
    /// Engine freezes a memtable (creating a frozen WAL on disk), then
    /// crashes before the frozen memtable is flushed to an SSTable.
    /// On reopen the frozen WAL must be replayed.
    ///
    /// # Starting environment
    /// Engine with data in active memtable, frozen memtable, and SSTables.
    ///
    /// # Actions
    /// 1. Write data until frozen memtable exists.
    /// 2. Flush some frozen memtables to create SSTables.
    /// 3. Write more data to create another frozen memtable.
    /// 4. Drop engine (crash — unflushed frozen memtable).
    /// 5. Reopen.
    /// 6. Verify all data across all layers.
    ///
    /// # Expected behavior
    /// All data from SSTables, frozen WALs, and active WAL is recovered.
    #[test]
    fn crash_with_unflushed_frozen_all_data_recovered() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let total_keys;
        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();

            // Write until frozen, then flush to create SSTables.
            let mut i = 0;
            loop {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
                i += 1;
                if engine.stats().unwrap().frozen_count > 0 {
                    break;
                }
            }
            engine.flush_all_frozen().unwrap();

            // Write more until another freeze.
            loop {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
                i += 1;
                if engine.stats().unwrap().frozen_count > 0 {
                    break;
                }
            }
            total_keys = i;

            // Confirm we have all three layers.
            let stats = engine.stats().unwrap();
            assert!(stats.sstables_count > 0, "Should have SSTables");
            assert!(stats.frozen_count > 0, "Should have frozen memtable");

            // Drop without close — crash!
        }

        // Reopen and verify everything.
        let engine = Engine::open(path, small_buffer_config()).unwrap();
        for i in 0..total_keys {
            let key = format!("key_{i:04}").into_bytes();
            let result = engine.get(key).unwrap();
            assert!(
                result.is_some(),
                "Key key_{i:04} should be recovered (total: {total_keys})"
            );
        }
    }

    // ================================================================
    // 4. Crash during flush with deletes — tombstones recovered
    // ================================================================

    /// # Scenario
    /// Engine has point deletes and range deletes in a frozen memtable
    /// that was never flushed. After crash recovery, the deletes must
    /// still hide the original puts.
    ///
    /// # Starting environment
    /// Engine with puts flushed to SSTables, then deletes issued and
    /// frozen.
    ///
    /// # Actions
    /// 1. Write 20 keys, flush to SSTable.
    /// 2. Delete keys 5..10, range-delete keys 15..20.
    /// 3. Write enough to freeze the memtable.
    /// 4. Drop engine (crash — frozen memtable with deletes).
    /// 5. Reopen and verify.
    ///
    /// # Expected behavior
    /// Keys 5-9 and 15-19 are hidden (deleted). Other keys are intact.
    #[test]
    fn crash_with_tombstones_in_frozen_memtable() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();

            // Write 20 keys and flush to SSTables.
            for i in 0..20 {
                engine
                    .put(
                        format!("key_{i:02}").into_bytes(),
                        format!("val_{i:02}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();

            // Point-delete keys 05..10.
            for i in 5..10 {
                engine.delete(format!("key_{i:02}").into_bytes()).unwrap();
            }

            // Range-delete keys 15..20.
            engine
                .delete_range(b"key_15".to_vec(), b"key_20".to_vec())
                .unwrap();

            // Write padding to trigger freeze.
            let mut pad = 100;
            loop {
                engine
                    .put(
                        format!("pad_{pad:04}").into_bytes(),
                        format!("padval_{pad:04}").into_bytes(),
                    )
                    .unwrap();
                pad += 1;
                if engine.stats().unwrap().frozen_count > 0 {
                    break;
                }
            }

            // Drop — crash with unflushed frozen containing tombstones.
        }

        // Reopen and verify.
        let engine = Engine::open(path, small_buffer_config()).unwrap();

        // Keys 0-4: should exist.
        for i in 0..5 {
            let key = format!("key_{i:02}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{i:02} should exist"
            );
        }

        // Keys 5-9: point-deleted.
        for i in 5..10 {
            let key = format!("key_{i:02}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_none(),
                "key_{i:02} should be deleted"
            );
        }

        // Keys 10-14: should exist.
        for i in 10..15 {
            let key = format!("key_{i:02}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{i:02} should exist"
            );
        }

        // Keys 15-19: range-deleted.
        for i in 15..20 {
            let key = format!("key_{i:02}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_none(),
                "key_{i:02} should be range-deleted"
            );
        }
    }

    // ================================================================
    // 5. Mixed: .tmp debris + orphan .sst + frozen WAL
    // ================================================================

    /// # Scenario
    /// Worst-case crash scenario: the sstables directory has both `.tmp`
    /// debris and an orphan `.sst` file, AND there are unflushed frozen
    /// WALs. The engine must correctly ignore `.tmp` files, clean up
    /// orphan `.sst` files, replay frozen WALs, and present a consistent
    /// view.
    ///
    /// # Expected behavior
    /// All committed data recovered. Orphan `.sst` removed. `.tmp` does
    /// not cause errors.
    #[test]
    fn crash_mixed_debris_and_frozen_wals() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let total_keys;
        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();
            let mut i = 0;
            loop {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
                i += 1;
                if engine.stats().unwrap().frozen_count > 0 {
                    break;
                }
            }
            total_keys = i;
            // Drop — crash with frozen memtable.
        }

        // Plant debris.
        let sst_dir = path.join(SSTABLE_DIR);
        {
            let mut f = File::create(sst_dir.join("888888.tmp")).unwrap();
            f.write_all(b"incomplete data").unwrap();
        }
        {
            let mut f = File::create(sst_dir.join("999999.sst")).unwrap();
            f.write_all(b"orphan sstable").unwrap();
        }

        // Reopen.
        let engine = Engine::open(path, small_buffer_config()).unwrap();

        // Orphan .sst should be cleaned up.
        assert!(
            !sst_dir.join("999999.sst").exists(),
            "Orphan .sst should be removed"
        );

        // All data recovered.
        for i in 0..total_keys {
            let key = format!("key_{i:04}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "Key key_{i:04} should be recovered"
            );
        }
    }
}
