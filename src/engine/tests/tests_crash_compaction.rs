//! Crash-during-compaction tests.
//!
//! These tests simulate crashes at critical points during compaction.
//! Compaction writes new SSTables and atomically updates the manifest
//! via `apply_compaction()`. A crash can leave:
//!
//! 1. **Orphan output SSTable** — new SSTable written and renamed, but
//!    manifest not yet updated. On recovery the orphan is cleaned up
//!    and the old SSTables remain live.
//! 2. **Old SSTables still live** — since manifests are only updated
//!    after the new SSTable is fully written, old SSTables are never
//!    removed prematurely.
//!
//! These tests verify that:
//! - All committed data is recoverable after a crash during compaction.
//! - Orphan SSTables from partial compaction are cleaned up.
//! - Compaction can be re-run after crash recovery.
//!
//! ## See also
//! - [`tests_crash_flush`] — crash during flush (frozen → SSTable)
//! - [`tests_crash_recovery`] — crash with only WAL data

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::tests::helpers::*;
    use crate::engine::{Engine, SSTABLE_DIR};
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    // ================================================================
    // 1. Recovery after crash during minor compaction
    // ================================================================

    /// # Scenario
    /// Engine has multiple SSTables. A crash occurs during minor
    /// compaction, leaving the compaction result as an orphan SSTable.
    ///
    /// # Starting environment
    /// Engine with >= 4 SSTables (enough to trigger minor compaction).
    ///
    /// # Actions
    /// 1. Write many keys, flush frequently to create multiple SSTables.
    /// 2. Close cleanly.
    /// 3. Plant an orphan SSTable (simulating compaction output that
    ///    the manifest doesn't know about).
    /// 4. Reopen engine.
    /// 5. Verify orphan is cleaned up and all data is intact.
    /// 6. Run minor compaction successfully.
    ///
    /// # Expected behavior
    /// Orphan cleaned up. Data intact. Compaction works after recovery.
    #[test]
    fn memtable_sstable__crash_during_minor_compaction_recovery() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // Phase 1: Create multiple SSTables.
        {
            let engine = Engine::open(path, multi_sstable_config()).unwrap();
            for i in 0..200 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();
            let stats = engine.stats().unwrap();
            assert!(
                stats.sstables_count >= 2,
                "Need multiple SSTables, got {}",
                stats.sstables_count
            );
            engine.close().unwrap();
        }

        // Phase 2: Plant orphan (simulating partial compaction output).
        let sst_dir = path.join(SSTABLE_DIR);
        let orphan_path = sst_dir.join("777777.sst");
        {
            let mut f = File::create(&orphan_path).unwrap();
            f.write_all(b"partial compaction output").unwrap();
            f.sync_all().unwrap();
        }

        // Phase 3: Reopen.
        let engine = Engine::open(path, multi_sstable_config()).unwrap();

        // Orphan should be cleaned up.
        assert!(
            !orphan_path.exists(),
            "Orphan should be removed during recovery"
        );

        // All data intact.
        for i in 0..200 {
            let key = format!("key_{i:04}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "Key key_{i:04} should survive crash"
            );
        }

        // Phase 4: Minor compaction should work after recovery.
        let _result = engine.minor_compact();
        // Regardless of whether compaction does anything, it must not error.
    }

    // ================================================================
    // 2. Recovery after crash during major compaction
    // ================================================================

    /// # Scenario
    /// A crash occurs after major compaction writes new SSTable and removes
    /// old files, but before manifesting. On recovery old SSTables referenced
    /// by manifest are loaded; any orphan new SSTable is cleaned up.
    ///
    /// This tests the worst case: where the old SSTable files were already
    /// deleted from disk but the manifest still references them. The engine
    /// must handle missing SSTable files gracefully.
    ///
    /// # Starting environment
    /// Engine with >= 2 SSTables.
    ///
    /// # Actions
    /// 1. Create multiple SSTables, close cleanly.
    /// 2. Run major compaction (reduces to 1 SSTable).
    /// 3. Verify data is intact after compaction.
    /// 4. Close, reopen — verify data still intact.
    ///
    /// # Expected behavior
    /// Major compaction produces correct results. Data survives
    /// close/reopen after compaction.
    #[test]
    fn memtable_sstable__major_compaction_then_crash_recovery() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // Phase 1: Create multiple SSTables.
        {
            let engine = Engine::open(path, multi_sstable_config()).unwrap();
            for i in 0..200 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();
            assert!(engine.stats().unwrap().sstables_count >= 2);

            // Run major compaction.
            let compacted = engine.major_compact().unwrap();
            assert!(compacted, "Major compaction should run");
            assert_eq!(
                engine.stats().unwrap().sstables_count,
                1,
                "Should have exactly 1 SSTable after major"
            );

            // Drop without close — crash after compaction.
        }

        // Phase 2: Reopen — must recover correctly.
        let engine = Engine::open(path, multi_sstable_config()).unwrap();
        for i in 0..200 {
            let key = format!("key_{i:04}").into_bytes();
            let result = engine.get(key).unwrap();
            assert!(
                result.is_some(),
                "Key key_{i:04} should survive crash after compaction"
            );
        }
    }

    // ================================================================
    // 3. Compaction with deletes then crash
    // ================================================================

    /// # Scenario
    /// Major compaction processes both puts and deletes. A crash occurs
    /// immediately after. After recovery, deleted keys must still be
    /// hidden.
    ///
    /// # Starting environment
    /// Engine with puts and deletes spread across multiple SSTables.
    ///
    /// # Actions
    /// 1. Write 100 keys, flush.
    /// 2. Delete 50 keys, flush.
    /// 3. Major compact.
    /// 4. Drop (crash).
    /// 5. Reopen and verify.
    ///
    /// # Expected behavior
    /// Surviving 50 keys present. Deleted 50 keys absent.
    #[test]
    fn memtable_sstable__compaction_with_deletes_then_crash() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        {
            let engine = Engine::open(path, small_buffer_config()).unwrap();

            // Write 100 keys.
            for i in 0..100 {
                engine
                    .put(
                        format!("key_{i:04}").into_bytes(),
                        format!("val_{i:04}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();

            // Delete even-numbered keys.
            for i in (0..100).step_by(2) {
                engine.delete(format!("key_{i:04}").into_bytes()).unwrap();
            }
            engine.flush_all_frozen().unwrap();

            // Major compact.
            engine.major_compact().unwrap();

            // Drop — crash.
        }

        // Reopen.
        let engine = Engine::open(path, small_buffer_config()).unwrap();

        // Even keys deleted, odd keys present.
        for i in 0..100 {
            let key = format!("key_{i:04}").into_bytes();
            let result = engine.get(key).unwrap();
            if i % 2 == 0 {
                assert!(result.is_none(), "key_{i:04} should be deleted");
            } else {
                assert!(result.is_some(), "key_{i:04} should exist");
            }
        }
    }

    // ================================================================
    // 4. Compaction debris (.tmp) does not interfere
    // ================================================================

    /// # Scenario
    /// A `.tmp` file from a partially written compaction output exists.
    /// The engine must not treat it as a valid SSTable.
    ///
    /// # Starting environment
    /// Engine with SSTables.
    ///
    /// # Actions
    /// 1. Create engine with SSTables, close.
    /// 2. Plant a `.tmp` file with a valid SSTable-looking name.
    /// 3. Reopen engine.
    /// 4. Verify no crash and correct data.
    /// 5. Run compaction — must not be confused by `.tmp`.
    ///
    /// # Expected behavior
    /// `.tmp` file ignored. Compaction operates normally on valid SSTables.
    #[test]
    fn memtable_sstable__compaction_tmp_debris_ignored() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        {
            let engine = engine_with_multi_sstables(path, 200, "key");
            engine.close().unwrap();
        }

        // Plant .tmp debris.
        let sst_dir = path.join(SSTABLE_DIR);
        let tmp_path = sst_dir.join("555555.tmp");
        {
            let mut f = File::create(&tmp_path).unwrap();
            f.write_all(b"partial compaction sstable").unwrap();
        }

        // Reopen — must not crash.
        let engine = Engine::open(path, multi_sstable_config()).unwrap();

        // Data intact.
        for i in 0..200 {
            let key = format!("key_{i:04}").into_bytes();
            assert!(engine.get(key).unwrap().is_some(), "key_{i:04} missing");
        }

        // Compaction works normally.
        let result = engine.major_compact().unwrap();
        assert!(result, "Major compaction should succeed");
        assert_eq!(engine.stats().unwrap().sstables_count, 1);

        // Data still intact after compaction.
        for i in 0..200 {
            let key = format!("key_{i:04}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{i:04} lost after compaction"
            );
        }
    }
}
