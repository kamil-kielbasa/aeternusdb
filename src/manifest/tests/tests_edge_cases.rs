//! Manifest edge-case and boundary-condition tests.
//!
//! These tests cover scenarios not exercised by the basic lifecycle suite —
//! specifically silent removal of nonexistent IDs, idempotent SSTable
//! addition, checkpointing with empty state, and incremental WAL replay
//! after a checkpoint.
//!
//! Coverage:
//! - `remove_sstable()` on a nonexistent ID → silent no-op
//! - `remove_frozen_wal()` on a nonexistent ID → silent no-op
//! - `add_sstable()` with duplicate ID → idempotent (1 entry)
//! - `checkpoint()` immediately after open (empty state)
//! - Post-checkpoint WAL mutations survive reopen
//!
//! ## See also
//! - [`tests_basic`] — full lifecycle, persistence and crash-recovery
//! - Engine-level `tests_recovery` / `tests_crash_recovery` for
//!   end-to-end manifest integration

#[cfg(test)]
mod tests {
    use crate::manifest::{Manifest, ManifestSstEntry};
    use tempfile::TempDir;
    use tracing_subscriber::EnvFilter;

    fn open_manifest(temp: &TempDir) -> Manifest {
        Manifest::open(temp.path()).expect("Manifest open failed")
    }

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    }

    fn sst_entry(id: u64) -> ManifestSstEntry {
        ManifestSstEntry {
            id,
            path: format!("sst_{:06}.bin", id).into(),
        }
    }

    // ----------------------------------------------------------------
    // Remove nonexistent SSTable
    // ----------------------------------------------------------------

    /// # Scenario
    /// Removing an SSTable ID that was never added must succeed silently.
    ///
    /// # Starting environment
    /// Fresh manifest with no SSTables.
    ///
    /// # Actions
    /// 1. `remove_sstable(999)`.
    ///
    /// # Expected behavior
    /// No error. SSTable list remains empty.
    #[test]
    fn remove_nonexistent_sstable_is_silent() {
        init_tracing();

        let temp = TempDir::new().unwrap();
        let mut m = open_manifest(&temp);

        m.remove_sstable(999).unwrap();

        assert!(
            m.get_sstables().unwrap().is_empty(),
            "SSTable list should still be empty"
        );
    }

    // ----------------------------------------------------------------
    // Remove nonexistent frozen WAL
    // ----------------------------------------------------------------

    /// # Scenario
    /// Removing a frozen WAL ID that was never added must succeed silently.
    ///
    /// # Starting environment
    /// Fresh manifest with no frozen WALs.
    ///
    /// # Actions
    /// 1. `remove_frozen_wal(999)`.
    ///
    /// # Expected behavior
    /// No error. Frozen WAL list remains empty.
    #[test]
    fn remove_nonexistent_frozen_wal_is_silent() {
        init_tracing();

        let temp = TempDir::new().unwrap();
        let mut m = open_manifest(&temp);

        m.remove_frozen_wal(999).unwrap();

        assert!(
            m.get_frozen_wals().unwrap().is_empty(),
            "Frozen WAL list should still be empty"
        );
    }

    // ----------------------------------------------------------------
    // Double-add same SSTable ID
    // ----------------------------------------------------------------

    /// # Scenario
    /// Adding an SSTable with the same ID twice must be idempotent:
    /// the manifest stores only one entry.
    ///
    /// # Starting environment
    /// Fresh manifest.
    ///
    /// # Actions
    /// 1. `add_sstable(id=10)`.
    /// 2. `add_sstable(id=10)` again.
    ///
    /// # Expected behavior
    /// `get_sstables()` contains exactly 1 entry with `id == 10`.
    #[test]
    fn double_add_same_sstable_id_is_idempotent() {
        init_tracing();

        let temp = TempDir::new().unwrap();
        let mut m = open_manifest(&temp);

        m.add_sstable(sst_entry(10)).unwrap();
        m.add_sstable(sst_entry(10)).unwrap();

        let sstables = m.get_sstables().unwrap();
        assert_eq!(
            sstables.len(),
            1,
            "Duplicate SSTable ID should be deduplicated"
        );
        assert_eq!(sstables[0].id, 10);
    }

    // ----------------------------------------------------------------
    // Checkpoint with empty state
    // ----------------------------------------------------------------

    /// # Scenario
    /// Calling `checkpoint()` immediately after opening a fresh manifest
    /// (empty state) must succeed and produce a valid snapshot.
    ///
    /// # Starting environment
    /// Fresh manifest (no prior snapshot or WAL).
    ///
    /// # Actions
    /// 1. `Manifest::open(dir)`.
    /// 2. `checkpoint()`.
    /// 3. Drop and reopen.
    ///
    /// # Expected behavior
    /// Reopen succeeds. All getters return defaults.
    #[test]
    fn checkpoint_empty_state() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);
            m.checkpoint().unwrap();
        }

        let m2 = open_manifest(&temp);
        assert_eq!(m2.get_last_lsn().unwrap(), 0);
        assert_eq!(m2.get_active_wal().unwrap(), 0);
        assert!(m2.get_frozen_wals().unwrap().is_empty());
        assert!(m2.get_sstables().unwrap().is_empty());
    }

    // ----------------------------------------------------------------
    // Snapshot + incremental WAL replay
    // ----------------------------------------------------------------

    /// # Scenario
    /// After a checkpoint, new mutations written to the WAL must survive
    /// a close/reopen cycle. The manifest replays the snapshot first,
    /// then the incremental WAL.
    ///
    /// # Starting environment
    /// Manifest with some initial state that has been checkpointed.
    ///
    /// # Actions
    /// 1. Add SSTable id=1, set active_wal=5, update LSN=100 → checkpoint.
    /// 2. Add SSTable id=2, add frozen WAL=5, update LSN=200 (no checkpoint).
    /// 3. Drop and reopen.
    ///
    /// # Expected behavior
    /// After reopen, manifest contains both SSTables (id=1 and id=2),
    /// frozen WAL list includes 5, active_wal=5, last_lsn=200.
    #[test]
    fn snapshot_plus_incremental_wal_replay() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);

            // Phase 1: Build initial state and checkpoint
            m.add_sstable(sst_entry(1)).unwrap();
            m.set_active_wal(5).unwrap();
            m.update_lsn(100).unwrap();
            m.checkpoint().unwrap();

            // Phase 2: Additional mutations (NOT checkpointed)
            m.add_sstable(sst_entry(2)).unwrap();
            m.add_frozen_wal(5).unwrap();
            m.update_lsn(200).unwrap();
        }

        // Reopen — should replay snapshot (SST 1, active=5, lsn=100)
        // then WAL (SST 2, frozen [5], lsn=200)
        let m2 = open_manifest(&temp);

        let sstables = m2.get_sstables().unwrap();
        assert_eq!(sstables.len(), 2, "Both SSTables should be present");
        assert!(sstables.iter().any(|e| e.id == 1));
        assert!(sstables.iter().any(|e| e.id == 2));

        let frozen = m2.get_frozen_wals().unwrap();
        assert!(frozen.contains(&5), "Frozen WAL 5 should be present");

        assert_eq!(m2.get_last_lsn().unwrap(), 200);
    }
}
