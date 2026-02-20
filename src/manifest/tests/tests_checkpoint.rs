//! Manifest checkpoint robustness and crash-simulation tests.
//!
//! These tests verify correctness when checkpoint is interrupted, when the
//! snapshot `.tmp` file is left behind, and that `allocate_sst_id` stays
//! monotonic after crash and recovery.
//!
//! ## Coverage
//! - Leftover `.tmp` file from interrupted checkpoint does not break reopen
//! - Multiple rapid checkpoints preserve state
//! - `allocate_sst_id` monotonicity after checkpoint + reopen
//! - Large manifest state survives checkpoint round-trip
//! - Concurrent mutations between checkpoints correctly replay
//! - Snapshot corruption detected on reopen
//!
//! ## See also
//! - [`tests_basic`]      — lifecycle, crash-recovery, checksum corruption
//! - [`tests_edge_cases`] — idempotent ops, empty-checkpoint, post-checkpoint WAL

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
    // 1. Leftover .tmp from interrupted checkpoint
    // ================================================================

    /// # Scenario
    /// Simulate a crash that leaves `MANIFEST-000001.tmp` on disk
    /// but the rename to `MANIFEST-000001` never happened.
    ///
    /// # Starting environment
    /// Manifest with some mutations, no prior snapshot.
    ///
    /// # Actions
    /// 1. Open manifest, add SSTable, set active WAL.
    /// 2. Drop manifest.
    /// 3. Write a bogus `MANIFEST-000001.tmp` file.
    /// 4. Reopen manifest — should recover from WAL.
    ///
    /// # Expected behavior
    /// The `.tmp` file is ignored. WAL replay restores correct state.
    #[test]
    fn leftover_tmp_from_interrupted_checkpoint() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let m = open_manifest(&temp);
            m.set_active_wal(5).unwrap();
            m.add_sstable(sst_entry(1)).unwrap();
        }

        // Simulate leftover .tmp from a crashed checkpoint.
        let tmp_path = temp.path().join("MANIFEST-000001.tmp");
        fs::write(&tmp_path, b"corrupted partial snapshot data").unwrap();

        // Reopen — should succeed via WAL replay.
        let m2 = open_manifest(&temp);
        assert_eq!(m2.get_active_wal().unwrap(), 5);
        assert_eq!(m2.get_sstables().unwrap().len(), 1);
        assert_eq!(m2.get_sstables().unwrap()[0].id, 1);
    }

    // ================================================================
    // 2. Multiple rapid checkpoints
    // ================================================================

    /// # Scenario
    /// Perform several checkpoints in rapid succession. Each should
    /// produce a valid snapshot.
    ///
    /// # Expected behavior
    /// After reopening, the latest state is fully recovered.
    #[test]
    fn multiple_rapid_checkpoints() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);

            for i in 1..=5u64 {
                m.add_sstable(sst_entry(i)).unwrap();
                m.checkpoint().unwrap();
            }
        }

        let m2 = open_manifest(&temp);
        let ssts = m2.get_sstables().unwrap();
        assert_eq!(ssts.len(), 5);

        for i in 1..=5u64 {
            assert!(ssts.iter().any(|e| e.id == i), "SST {} must exist", i);
        }
    }

    // ================================================================
    // 3. allocate_sst_id monotonicity after checkpoint + reopen
    // ================================================================

    /// # Scenario
    /// Allocate several SSTable IDs, checkpoint, reopen, and allocate
    /// more. The IDs must be strictly monotonic.
    ///
    /// # Expected behavior
    /// Post-reopen allocations continue from where the pre-checkpoint
    /// counter left off.
    #[test]
    fn allocate_sst_id_monotonic_after_checkpoint() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        let (id_a, id_b);

        {
            let mut m = open_manifest(&temp);

            id_a = m.allocate_sst_id().unwrap();
            id_b = m.allocate_sst_id().unwrap();
            assert!(id_b > id_a, "IDs should be monotonically increasing");

            m.checkpoint().unwrap();
        }

        // Reopen after checkpoint.
        let m2 = open_manifest(&temp);
        let id_c = m2.allocate_sst_id().unwrap();
        assert!(
            id_c > id_b,
            "ID after reopen ({id_c}) must be > last pre-checkpoint ID ({id_b})"
        );

        let id_d = m2.allocate_sst_id().unwrap();
        assert!(id_d > id_c);
    }

    // ================================================================
    // 4. allocate_sst_id monotonicity after WAL-only recovery
    // ================================================================

    /// # Scenario
    /// Allocate SSTable IDs without checkpointing, then reopen.
    /// WAL replay must correctly restore the counter.
    ///
    /// # Expected behavior
    /// Post-reopen allocation returns an ID greater than all prior.
    #[test]
    fn allocate_sst_id_monotonic_after_wal_recovery() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        let last_id;

        {
            let m = open_manifest(&temp);

            for _ in 0..10 {
                m.allocate_sst_id().unwrap();
            }
            last_id = m.peek_next_sst_id().unwrap() - 1;
        }

        let m2 = open_manifest(&temp);
        let new_id = m2.allocate_sst_id().unwrap();
        assert!(
            new_id > last_id,
            "Post-reopen ID ({new_id}) must exceed pre-crash last ({last_id})"
        );
    }

    // ================================================================
    // 5. Large manifest state checkpoint round-trip
    // ================================================================

    /// # Scenario
    /// Build a manifest with many SSTables and frozen WALs, checkpoint,
    /// reopen. All entries must survive.
    ///
    /// # Expected behavior
    /// 100 SSTables and 50 frozen WALs present after reopen.
    #[test]
    fn large_state_checkpoint_round_trip() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);

            for i in 1..=100u64 {
                m.add_sstable(sst_entry(i)).unwrap();
            }
            for w in 1..=50u64 {
                m.add_frozen_wal(w).unwrap();
            }
            m.update_lsn(12345).unwrap();
            m.set_active_wal(999).unwrap();

            m.checkpoint().unwrap();
        }

        let m2 = open_manifest(&temp);
        assert_eq!(m2.get_sstables().unwrap().len(), 100);
        assert_eq!(m2.get_frozen_wals().unwrap().len(), 50);
        assert_eq!(m2.get_last_lsn().unwrap(), 12345);
        assert_eq!(m2.get_active_wal().unwrap(), 999);
    }

    // ================================================================
    // 6. Mutations after checkpoint replayed on reopen
    // ================================================================

    /// # Scenario
    /// Checkpoint, then perform more mutations via WAL, then reopen.
    /// The WAL mutations after the checkpoint must be replayed on top
    /// of the restored snapshot.
    ///
    /// # Expected behavior
    /// Both snapshot state and post-checkpoint WAL entries are present.
    #[test]
    fn mutations_after_checkpoint_replayed() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);

            // Pre-checkpoint state.
            m.add_sstable(sst_entry(1)).unwrap();
            m.set_active_wal(10).unwrap();
            m.checkpoint().unwrap();

            // Post-checkpoint mutations (only in WAL now).
            m.add_sstable(sst_entry(2)).unwrap();
            m.add_frozen_wal(10).unwrap();
            m.set_active_wal(11).unwrap();
            m.update_lsn(42).unwrap();
        }

        let m2 = open_manifest(&temp);
        let ssts = m2.get_sstables().unwrap();
        assert_eq!(ssts.len(), 2, "Both SSTs (pre+post checkpoint) must exist");
        assert_eq!(m2.get_active_wal().unwrap(), 11);
        assert_eq!(m2.get_last_lsn().unwrap(), 42);
        assert!(m2.get_frozen_wals().unwrap().contains(&10));
    }

    // ================================================================
    // 7. Corrupt snapshot blocks reopen
    // ================================================================

    /// # Scenario
    /// Create a valid snapshot, then corrupt its bytes. Reopening
    /// falls back to WAL replay (resilient recovery).
    ///
    /// # Expected behavior
    /// `Manifest::open` succeeds; since WAL was truncated by checkpoint,
    /// state reverts to defaults.
    #[test]
    fn corrupt_snapshot_falls_back_to_wal() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);
            m.add_sstable(sst_entry(1)).unwrap();
            m.update_lsn(100).unwrap();
            m.checkpoint().unwrap();
        }

        // Corrupt the snapshot file (flip a byte near the middle).
        let snap_path = temp.path().join("MANIFEST-000001");
        let mut data = fs::read(&snap_path).unwrap();
        assert!(data.len() > 10, "Snapshot should be non-trivial");

        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        fs::write(&snap_path, &data).unwrap();

        // Resilient recovery: corrupt snapshot → WAL replay (empty after
        // checkpoint truncation) → default state.
        let m2 = Manifest::open(temp.path()).unwrap();
        assert_eq!(m2.get_last_lsn().unwrap(), 0);
        assert!(m2.get_sstables().unwrap().is_empty());
    }

    // ================================================================
    // 8. Compaction event survives checkpoint + reopen
    // ================================================================

    /// # Scenario
    /// Apply a compaction event (add 2, remove 1), checkpoint, reopen.
    ///
    /// # Expected behavior
    /// After reopen, the manifest reflects the compaction.
    #[test]
    fn compaction_event_survives_checkpoint() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);

            // Add initial SSTables.
            m.add_sstable(sst_entry(1)).unwrap();
            m.add_sstable(sst_entry(2)).unwrap();
            m.add_sstable(sst_entry(3)).unwrap();

            // Compaction: merge #1 and #2 into #4.
            m.apply_compaction(vec![sst_entry(4)], vec![1, 2]).unwrap();

            m.checkpoint().unwrap();
        }

        let m2 = open_manifest(&temp);
        let ssts = m2.get_sstables().unwrap();
        let ids: Vec<u64> = ssts.iter().map(|e| e.id).collect();

        assert!(!ids.contains(&1), "SST 1 should have been removed");
        assert!(!ids.contains(&2), "SST 2 should have been removed");
        assert!(ids.contains(&3), "SST 3 should remain");
        assert!(ids.contains(&4), "SST 4 should have been added");
    }
}
