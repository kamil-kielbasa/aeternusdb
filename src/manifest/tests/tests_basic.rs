//! Manifest lifecycle, persistence, and crash-recovery tests.
//!
//! The manifest tracks durable metadata — active WAL sequence, frozen WAL
//! list, SSTable entries, and last committed LSN — via a WAL-backed event
//! log with periodic snapshot checkpoints.  These tests exercise the full
//! lifecycle at the component boundary.
//!
//! Coverage:
//! - Open on an empty directory (no prior state)
//! - `set_active_wal` persistence across close/reopen
//! - Frozen-WAL add/remove round-trip
//! - SSTable entry add/remove round-trip
//! - LSN update persistence
//! - Checkpoint creation and WAL truncation
//! - Snapshot checksum-corruption detection
//! - Crash-style recovery via WAL replay (no checkpoint)
//!
//! ## See also
//! - Engine-level `tests_recovery` / `tests_crash_recovery` for
//!   end-to-end manifest integration

#[cfg(test)]
mod tests {
    use crate::manifest::{Manifest, ManifestError, ManifestSstEntry};
    use std::fs;
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

    // ----------------------------------------------------------------
    // Open without prior state
    // ----------------------------------------------------------------

    /// # Scenario
    /// Open a manifest on a fresh directory that has no snapshot or WAL.
    ///
    /// # Starting environment
    /// Empty temp directory.
    ///
    /// # Actions
    /// 1. `Manifest::open(dir)`.
    ///
    /// # Expected behavior
    /// All getters return defaults: `last_lsn == 0`, `active_wal == 0`,
    /// `frozen_wals` and `sstables` are empty.
    #[test]
    fn opens_without_snapshot() {
        let temp = TempDir::new().unwrap();

        let m = open_manifest(&temp);

        assert_eq!(m.get_last_lsn().unwrap(), 0);
        assert_eq!(m.get_active_wal().unwrap(), 0);
        assert!(m.get_frozen_wals().unwrap().is_empty());
        assert!(m.get_sstables().unwrap().is_empty());
    }

    // ----------------------------------------------------------------
    // Active WAL persistence
    // ----------------------------------------------------------------

    /// # Scenario
    /// `set_active_wal` is persisted through close and reopen.
    ///
    /// # Starting environment
    /// Fresh manifest.
    ///
    /// # Actions
    /// 1. `set_active_wal(42)`.
    /// 2. Drop the manifest (simulates close).
    /// 3. Reopen the manifest from the same directory.
    ///
    /// # Expected behavior
    /// `get_active_wal() == 42` after reopen.
    #[test]
    fn set_active_wal_persists() {
        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);
            m.set_active_wal(42).unwrap();
        }

        // Reopen → WAL replay restores state
        let m2 = open_manifest(&temp);
        assert_eq!(m2.get_active_wal().unwrap(), 42);
    }

    // ----------------------------------------------------------------
    // Frozen WAL add / remove
    // ----------------------------------------------------------------

    /// # Scenario
    /// Add three frozen WALs, remove the middle one, and verify
    /// persistence across reopen.
    ///
    /// # Starting environment
    /// Fresh manifest.
    ///
    /// # Actions
    /// 1. `add_frozen_wal(1)`, `add_frozen_wal(2)`, `add_frozen_wal(3)`.
    /// 2. `remove_frozen_wal(2)`.
    /// 3. Reopen and query `get_frozen_wals()`.
    ///
    /// # Expected behavior
    /// `[1, 3]` both before and after reopen.
    #[test]
    fn frozen_wal_list_works() {
        let temp = TempDir::new().unwrap();
        let mut m = open_manifest(&temp);

        m.add_frozen_wal(1).unwrap();
        m.add_frozen_wal(2).unwrap();
        m.add_frozen_wal(3).unwrap();
        m.remove_frozen_wal(2).unwrap();

        let frozen = m.get_frozen_wals().unwrap();
        assert_eq!(frozen, vec![1, 3]);

        let m2 = open_manifest(&temp);
        assert_eq!(m2.get_frozen_wals().unwrap(), vec![1, 3]);
    }

    // ----------------------------------------------------------------
    // SSTable add / remove
    // ----------------------------------------------------------------

    /// # Scenario
    /// Add two SSTable entries, remove the first, and verify persistence
    /// across reopen.
    ///
    /// # Starting environment
    /// Fresh manifest.
    ///
    /// # Actions
    /// 1. `add_sstable(id=10, "a.sst")`, `add_sstable(id=11, "b.sst")`.
    /// 2. `remove_sstable(10)`.
    /// 3. Reopen and query `get_sstables()`.
    ///
    /// # Expected behavior
    /// Only entry `id=11` survives, both before and after reopen.
    #[test]
    fn sstables_persist() {
        let temp = TempDir::new().unwrap();
        let mut m = open_manifest(&temp);

        let e1 = ManifestSstEntry {
            id: 10,
            path: "a.sst".into(),
        };
        let e2 = ManifestSstEntry {
            id: 11,
            path: "b.sst".into(),
        };

        m.add_sstable(e1.clone()).unwrap();
        m.add_sstable(e2.clone()).unwrap();
        m.remove_sstable(10).unwrap();

        let ssts = m.get_sstables().unwrap();
        assert_eq!(ssts.len(), 1);
        assert_eq!(ssts[0].id, 11);

        let m2 = open_manifest(&temp);
        let ssts2 = m2.get_sstables().unwrap();
        assert_eq!(ssts2.len(), 1);
        assert_eq!(ssts2[0].id, 11);
    }

    // ----------------------------------------------------------------
    // LSN update persistence
    // ----------------------------------------------------------------

    /// # Scenario
    /// `update_lsn` is persisted through close and reopen.
    ///
    /// # Starting environment
    /// Fresh manifest.
    ///
    /// # Actions
    /// 1. `update_lsn(777)`.
    /// 2. Drop and reopen.
    ///
    /// # Expected behavior
    /// `get_last_lsn() == 777`.
    #[test]
    fn updates_lsn() {
        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);
            m.update_lsn(777).unwrap();
        }

        let m2 = open_manifest(&temp);
        assert_eq!(m2.get_last_lsn().unwrap(), 777);
    }

    // ----------------------------------------------------------------
    // Checkpoint creation + WAL truncation
    // ----------------------------------------------------------------

    /// # Scenario
    /// After a checkpoint, the snapshot file exists, the WAL is truncated,
    /// and all state is recoverable from the snapshot alone.
    ///
    /// # Starting environment
    /// Fresh manifest with several mutations logged.
    ///
    /// # Actions
    /// 1. `set_active_wal(5)`, `add_frozen_wal(9)`, `update_lsn(44)`.
    /// 2. `checkpoint()`.
    /// 3. Drop and reopen.
    ///
    /// # Expected behavior
    /// - Snapshot file exists on disk.
    /// - WAL file is truncated (size ≈ 0).
    /// - Reopened manifest returns `active_wal == 5`,
    ///   `frozen_wals == [9]`, `last_lsn == 44`.
    #[test]
    fn checkpoint_truncates_wal() {
        init_tracing();

        let temp = TempDir::new().unwrap();

        let wal_path = temp.path().join("wal-000000.log");
        let snapshot_path = temp.path().join("manifest.snapshot");

        {
            let mut m = open_manifest(&temp);

            m.set_active_wal(5).unwrap();
            m.add_frozen_wal(9).unwrap();
            m.update_lsn(44).unwrap();

            let size_before = fs::metadata(&wal_path).unwrap().len();
            assert!(size_before > 0);

            m.checkpoint().unwrap();
        }

        assert!(snapshot_path.exists());

        let size_after = fs::metadata(&wal_path).unwrap().len();
        assert!(size_after == 0 || size_after < 32);

        let m2 = open_manifest(&temp);
        assert_eq!(m2.get_active_wal().unwrap(), 5);
        assert_eq!(m2.get_frozen_wals().unwrap(), vec![9]);
        assert_eq!(m2.get_last_lsn().unwrap(), 44);
    }

    // ----------------------------------------------------------------
    // Snapshot checksum corruption
    // ----------------------------------------------------------------

    /// # Scenario
    /// Corrupting a single byte in the snapshot file makes the manifest
    /// refuse to open.
    ///
    /// # Starting environment
    /// Manifest with a valid checkpoint.
    ///
    /// # Actions
    /// 1. Create checkpoint.
    /// 2. Flip one byte in the snapshot file.
    /// 3. Attempt to reopen.
    ///
    /// # Expected behavior
    /// `Manifest::open` returns `ManifestError::SnapshotChecksumMismatch`.
    #[test]
    fn detects_corrupted_snapshot() {
        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);
            m.update_lsn(123).unwrap();
            m.checkpoint().unwrap();
        }

        let snapshot_path = temp.path().join("manifest.snapshot");

        {
            let mut bytes = fs::read(&snapshot_path).unwrap();
            bytes[5] ^= 0xFF;
            fs::write(&snapshot_path, bytes).unwrap();
        }

        let err = Manifest::open(temp.path()).unwrap_err();
        match err {
            ManifestError::SnapshotChecksumMismatch => {}
            other => panic!("Snapshot checksum mismatch, got {:?}", other),
        }
    }

    // ----------------------------------------------------------------
    // Crash recovery via WAL replay
    // ----------------------------------------------------------------

    /// # Scenario
    /// Simulate a crash by dropping the manifest without calling
    /// `checkpoint()`.  On reopen, the WAL replay must restore all state.
    ///
    /// # Starting environment
    /// Fresh manifest.
    ///
    /// # Actions
    /// 1. `set_active_wal(55)`, `add_frozen_wal(88)`, `update_lsn(9)`.
    /// 2. Drop without checkpointing.
    /// 3. Reopen from the same directory.
    ///
    /// # Expected behavior
    /// `active_wal == 55`, `frozen_wals == [88]`, `last_lsn == 9`.
    #[test]
    fn reopens_after_crash_using_wal() {
        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);
            m.set_active_wal(55).unwrap();
            m.add_frozen_wal(88).unwrap();
            m.update_lsn(9).unwrap();
        }

        let m2 = open_manifest(&temp);
        assert_eq!(m2.get_active_wal().unwrap(), 55);
        assert_eq!(m2.get_frozen_wals().unwrap(), vec![88]);
        assert_eq!(m2.get_last_lsn().unwrap(), 9);
    }
}
