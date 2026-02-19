//! Resource cleanup verification tests.
//!
//! These tests verify that the engine correctly manages files on disk:
//! SSTable files are removed after compaction, temp files are cleaned up,
//! and orphan SSTables are removed on open. WAL files are **not** deleted
//! after flush (they remain on disk; only the manifest's frozen list is
//! updated). These tests check actual on-disk file counts.
//!
//! ## See also
//! - [`tests_hardening`] — orphan SSTable cleanup on open
//! - [`tests_crash_flush`] — crash debris: `.tmp` files cleaned

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::tests::helpers::*;
    use crate::engine::{Engine, SSTABLE_DIR};
    use std::fs;
    use tempfile::TempDir;

    /// Count SSTable files in the sstables directory.
    fn count_sst_files(path: &std::path::Path) -> usize {
        let sst_dir = path.join(SSTABLE_DIR);
        if !sst_dir.exists() {
            return 0;
        }
        fs::read_dir(&sst_dir)
            .unwrap()
            .filter(|e| {
                let name = e
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .to_string();
                name.ends_with(".sst")
            })
            .count()
    }

    /// Count .tmp files in the sstables directory.
    fn count_tmp_files(path: &std::path::Path) -> usize {
        let sst_dir = path.join(SSTABLE_DIR);
        if !sst_dir.exists() {
            return 0;
        }
        fs::read_dir(&sst_dir)
            .unwrap()
            .filter(|e| {
                let name = e
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .to_string();
                name.ends_with(".tmp")
            })
            .count()
    }

    // ================================================================
    // 1. SSTable created after flush
    // ================================================================

    /// # Scenario
    /// After flushing frozen memtables, SSTable files must appear on disk.
    ///
    /// # Expected behavior
    /// At least one `.sst` file exists after `flush_all_frozen()`.
    #[test]
    fn memtable_sstable__sst_files_created_after_flush() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, small_buffer_config()).unwrap();

        for i in 0..40u32 {
            engine
                .put(
                    format!("key_{i:04}").into_bytes(),
                    format!("value_padding_{i:04}").into_bytes(),
                )
                .unwrap();
        }

        let sst_before = count_sst_files(path);
        engine.flush_all_frozen().unwrap();
        let sst_after = count_sst_files(path);

        assert!(
            sst_after > sst_before,
            "SSTable count should increase after flush: before={sst_before}, after={sst_after}"
        );
    }

    // ================================================================
    // 2. SSTable file count decreases after major compaction
    // ================================================================

    /// # Scenario
    /// After major compaction, old SSTables are deleted and replaced by
    /// a single merged SSTable.
    ///
    /// # Expected behavior
    /// After compaction: exactly 1 SSTable on disk.
    #[test]
    fn memtable_sstable__sst_file_count_decreases_after_major_compaction() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = engine_with_multi_sstables(path, 200, "key");

        let sst_before = count_sst_files(path);
        assert!(
            sst_before >= 2,
            "Expected >= 2 SSTables before compaction, got {sst_before}"
        );

        engine.major_compact().unwrap();

        let sst_after = count_sst_files(path);
        assert_eq!(
            sst_after, 1,
            "Expected 1 SSTable after major compaction, got {sst_after}"
        );
    }

    // ================================================================
    // 3. No .tmp files after successful flush
    // ================================================================

    /// # Scenario
    /// After a successful flush, no `.tmp` files should remain in the
    /// sstables directory. The SstWriter uses `.tmp` during build and
    /// atomically renames to `.sst`.
    ///
    /// # Expected behavior
    /// Zero `.tmp` files in the sstables directory.
    #[test]
    fn memtable_sstable__no_tmp_files_after_flush() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, small_buffer_config()).unwrap();
        for i in 0..30u32 {
            engine
                .put(
                    format!("key_{i:04}").into_bytes(),
                    format!("val_{i:04}").into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();

        let tmp_count = count_tmp_files(path);
        assert_eq!(tmp_count, 0, "No .tmp files should remain after flush");
    }

    // ================================================================
    // 4. No .tmp files after successful compaction
    // ================================================================

    /// # Scenario
    /// After a successful major compaction, no `.tmp` files should remain.
    ///
    /// # Expected behavior
    /// Zero `.tmp` files in the sstables directory.
    #[test]
    fn memtable_sstable__no_tmp_files_after_compaction() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = engine_with_multi_sstables(path, 200, "key");
        engine.major_compact().unwrap();

        let tmp_count = count_tmp_files(path);
        assert_eq!(tmp_count, 0, "No .tmp files should remain after compaction");
    }

    // ================================================================
    // 5. Orphan SSTables removed on reopen
    // ================================================================

    /// # Scenario
    /// Place a stray `.sst` file in the sstables directory that is not
    /// tracked by the manifest. On reopen, the engine should remove it.
    ///
    /// # Expected behavior
    /// After reopening, the orphan SSTable file is removed.
    #[test]
    fn memtable_sstable__orphan_sst_removed_on_reopen() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // Create an engine and write + flush to get true SSTables.
        let engine = engine_with_sstables(path, 50, "key");
        let sst_count = count_sst_files(path);
        engine.close().unwrap();

        // Plant an orphan SSTable that the manifest does not know about.
        let sst_dir = path.join(SSTABLE_DIR);
        let orphan_path = sst_dir.join("sstable-999999.sst");
        fs::write(&orphan_path, b"fake sst data").unwrap();
        assert_eq!(
            count_sst_files(path),
            sst_count + 1,
            "Orphan should be on disk"
        );

        // Reopen the engine — orphan should be cleaned up.
        let engine2 = Engine::open(path, default_config()).unwrap();
        let sst_after = count_sst_files(path);
        assert_eq!(
            sst_after, sst_count,
            "Orphan SSTable should be removed on reopen"
        );

        // Data should still be intact.
        assert!(engine2.get(b"key_0000".to_vec()).unwrap().is_some());
    }

    // ================================================================
    // 6. Multiple flush/compaction cycles — bounded SSTable count
    // ================================================================

    /// # Scenario
    /// Run multiple write -> flush -> compaction cycles and verify that
    /// the SSTable count stays bounded and no temp files leak.
    ///
    /// # Expected behavior
    /// After each cycle with compaction, <= a few SSTables remain.
    /// No `.tmp` files on disk.
    #[test]
    fn memtable_sstable__multiple_cycles_no_file_leak() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Engine::open(path, small_buffer_config()).unwrap();

        for cycle in 0..3u32 {
            for i in 0..30u32 {
                engine
                    .put(
                        format!("c{cycle}_k{i:02}").into_bytes(),
                        format!("c{cycle}_v{i:02}").into_bytes(),
                    )
                    .unwrap();
            }
            engine.flush_all_frozen().unwrap();

            let stats = engine.stats().unwrap();
            if stats.sstables_count >= 2 {
                engine.major_compact().unwrap();
            }
        }

        let sst_count = count_sst_files(path);
        let tmp_count = count_tmp_files(path);

        assert!(
            sst_count <= 5,
            "SSTable count should be bounded, got {sst_count}"
        );
        assert_eq!(tmp_count, 0, "No temp files should remain");
    }
}
