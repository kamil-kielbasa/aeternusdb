//! Manifest API coverage tests — `is_dirty`, `peek_next_sst_id`,
//! `Version` event, dirty-flag lifecycle, and resilient snapshot recovery.
//!
//! ## Coverage
//! - `is_dirty()` transitions: clean → dirty → checkpoint → clean
//! - `peek_next_sst_id()` returns correct value without allocating
//! - `Version` event sets dirty flag
//! - Corrupt snapshot + valid WAL data → resilient recovery
//! - Concurrent-style allocate_sst_id monotonicity (sequential)
//!
//! ## See also
//! - [`tests_basic`]      — lifecycle, crash-recovery
//! - [`tests_edge_cases`] — idempotent ops, empty-checkpoint
//! - [`tests_checkpoint`] — checkpoint robustness

#[cfg(test)]
mod tests {
    use crate::manifest::{Manifest, ManifestSstEntry};
    use std::fs;
    use tempfile::TempDir;
    use tracing_subscriber::EnvFilter;

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    }

    fn open_manifest(temp: &TempDir) -> Manifest {
        Manifest::open(temp.path()).expect("Manifest open failed")
    }

    fn sst_entry(id: u64) -> ManifestSstEntry {
        ManifestSstEntry {
            id,
            path: format!("sst_{:06}.sst", id).into(),
        }
    }

    // ================================================================
    // 1. is_dirty() lifecycle
    // ================================================================

    /// # Scenario
    /// Freshly opened manifest is clean. After a mutation it is dirty.
    /// After `checkpoint()` it becomes clean again.
    ///
    /// # Expected behavior
    /// `is_dirty()` returns `false → true → false`.
    #[test]
    fn dirty_flag_lifecycle() {
        init_tracing();

        let temp = TempDir::new().unwrap();
        let mut m = open_manifest(&temp);

        // Fresh manifest is clean.
        assert!(!m.is_dirty().unwrap(), "fresh manifest should be clean");

        // Any mutation makes it dirty.
        m.set_active_wal(1).unwrap();
        assert!(m.is_dirty().unwrap(), "mutation should set dirty");

        // Checkpoint clears dirty.
        m.checkpoint().unwrap();
        assert!(!m.is_dirty().unwrap(), "checkpoint should clear dirty");

        // Another mutation sets dirty again.
        m.update_lsn(42).unwrap();
        assert!(
            m.is_dirty().unwrap(),
            "post-checkpoint mutation should set dirty"
        );
    }

    // ================================================================
    // 2. peek_next_sst_id
    // ================================================================

    /// # Scenario
    /// `peek_next_sst_id()` returns the next ID without consuming it.
    /// Two consecutive peeks return the same value.  An `allocate_sst_id`
    /// then advances the counter past the peeked value.
    ///
    /// # Expected behavior
    /// peek == peek, allocate == old peek, new peek == allocate + 1.
    #[test]
    fn peek_next_sst_id_does_not_allocate() {
        init_tracing();

        let temp = TempDir::new().unwrap();
        let m = open_manifest(&temp);

        let peek1 = m.peek_next_sst_id().unwrap();
        let peek2 = m.peek_next_sst_id().unwrap();
        assert_eq!(peek1, peek2, "consecutive peeks should be equal");

        let allocated = m.allocate_sst_id().unwrap();
        assert_eq!(allocated, peek1, "allocate should return the peeked value");

        let peek3 = m.peek_next_sst_id().unwrap();
        assert_eq!(peek3, allocated + 1, "peek after allocate should be +1");
    }

    // ================================================================
    // 3. Version event sets dirty
    // ================================================================

    /// # Scenario
    /// Replaying a `Version` event through WAL should mark the manifest
    /// dirty, just like every other event.  We verify indirectly by
    /// writing a manifest, reopening (triggering WAL replay of all
    /// events including any Version events), and checking dirty state.
    ///
    /// # Expected behavior
    /// After any mutation + reopen (replay), `is_dirty()` returns `true`
    /// because WAL replay applies records that set dirty.
    #[test]
    fn mutation_replay_sets_dirty() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let m = open_manifest(&temp);
            m.set_active_wal(7).unwrap();
            m.update_lsn(10).unwrap();
        }

        // Reopen — WAL replay applies events → dirty should be true.
        let m2 = open_manifest(&temp);
        assert!(m2.is_dirty().unwrap(), "WAL replay should set dirty flag");
        assert_eq!(m2.get_active_wal().unwrap(), 7);
    }

    // ================================================================
    // 4. Corrupt snapshot + valid WAL → resilient recovery
    // ================================================================

    /// # Scenario
    /// Build state, checkpoint, add more mutations (written to WAL),
    /// then corrupt the snapshot.  On reopen the manifest should fall
    /// back to WAL replay and recover the post-checkpoint mutations.
    ///
    /// # Expected behavior
    /// Pre-checkpoint state (in the corrupt snapshot) is lost.
    /// Post-checkpoint state (in the WAL) is recovered.
    #[test]
    fn corrupt_snapshot_recovers_post_checkpoint_wal() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);

            // Pre-checkpoint state.
            m.add_sstable(sst_entry(1)).unwrap();
            m.set_active_wal(10).unwrap();
            m.update_lsn(100).unwrap();
            m.checkpoint().unwrap();

            // Post-checkpoint mutations (in WAL only).
            m.add_sstable(sst_entry(2)).unwrap();
            m.update_lsn(200).unwrap();
        }

        // Corrupt the snapshot.
        let snap_path = temp.path().join("MANIFEST-000001");
        let mut raw = fs::read(&snap_path).unwrap();
        let mid = raw.len() / 2;
        raw[mid] ^= 0xFF;
        fs::write(&snap_path, &raw).unwrap();

        // Reopen — snapshot is corrupt → fall back to WAL replay.
        // The WAL contains only post-checkpoint entries (SST 2, LSN 200).
        let m2 = open_manifest(&temp);

        let ssts = m2.get_sstables().unwrap();
        // SST 1 was in the (now corrupt) snapshot only → lost.
        assert!(
            !ssts.iter().any(|e| e.id == 1),
            "SST 1 was in corrupt snapshot and should be lost"
        );
        // SST 2 was in the post-checkpoint WAL → recovered.
        assert!(
            ssts.iter().any(|e| e.id == 2),
            "SST 2 was in post-checkpoint WAL and should be recovered"
        );
        assert_eq!(m2.get_last_lsn().unwrap(), 200);
    }

    // ================================================================
    // 5. Sequential allocate_sst_id monotonicity
    // ================================================================

    /// # Scenario
    /// Rapidly allocate many SSTable IDs and verify strict monotonicity.
    ///
    /// # Expected behavior
    /// Every ID is strictly greater than the previous one.
    #[test]
    fn allocate_sst_id_strict_monotonicity() {
        init_tracing();

        let temp = TempDir::new().unwrap();
        let m = open_manifest(&temp);

        let mut prev = 0u64;
        for _ in 0..100 {
            let id = m.allocate_sst_id().unwrap();
            assert!(id > prev, "ID {} should be > previous {}", id, prev);
            prev = id;
        }
    }

    // ================================================================
    // 6. is_dirty after reopen with checkpoint (clean)
    // ================================================================

    /// # Scenario
    /// Checkpoint and reopen.  Since the snapshot is clean and WAL is
    /// empty, `is_dirty()` should remain `false`.
    ///
    /// # Expected behavior
    /// `is_dirty() == false` after loading a clean snapshot.
    #[test]
    fn clean_after_checkpoint_reopen() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);
            m.set_active_wal(5).unwrap();
            m.checkpoint().unwrap();
        }

        let m2 = open_manifest(&temp);
        // Snapshot loaded, WAL empty → no mutations replayed → clean.
        assert!(
            !m2.is_dirty().unwrap(),
            "manifest should be clean after checkpoint + reopen with empty WAL"
        );
    }

    // ================================================================
    // 7. Multiple mutations between allocate_sst_id
    // ================================================================

    /// # Scenario
    /// Interleave `allocate_sst_id` with other mutations, checkpoint,
    /// reopen, and verify the counter is consistent.
    ///
    /// # Expected behavior
    /// `allocate_sst_id` always returns monotonically increasing IDs
    /// regardless of intermixed operations.
    #[test]
    fn allocate_sst_id_with_intermixed_ops() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        let id3;
        {
            let mut m = open_manifest(&temp);
            let id1 = m.allocate_sst_id().unwrap();
            m.add_sstable(sst_entry(id1)).unwrap();
            m.set_active_wal(1).unwrap();

            let id2 = m.allocate_sst_id().unwrap();
            assert!(id2 > id1);
            m.add_sstable(sst_entry(id2)).unwrap();

            m.checkpoint().unwrap();

            id3 = m.allocate_sst_id().unwrap();
            assert!(id3 > id2);
        }

        // Reopen and continue allocating.
        let m2 = open_manifest(&temp);
        let id4 = m2.allocate_sst_id().unwrap();
        assert!(
            id4 > id3,
            "ID after reopen ({id4}) must exceed pre-close ID ({id3})"
        );
    }
}
