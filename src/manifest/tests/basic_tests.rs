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

    // -----------------------------------------
    // Test: open without snapshot
    // -----------------------------------------
    #[test]
    fn manifest_opens_without_snapshot() {
        let temp = TempDir::new().unwrap();

        let m = open_manifest(&temp);

        assert_eq!(m.get_last_lsn().unwrap(), 0);
        assert_eq!(m.get_active_wal().unwrap(), 0);
        assert!(m.get_frozen_wals().unwrap().is_empty());
        assert!(m.get_sstables().unwrap().is_empty());
    }

    // -----------------------------------------
    // Test: set_active_wal + reopen
    // -----------------------------------------
    #[test]
    fn manifest_set_active_wal_persists() {
        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);
            m.set_active_wal(42).unwrap();
        }

        // Reopen → WAL replay restores state
        let m2 = open_manifest(&temp);
        assert_eq!(m2.get_active_wal().unwrap(), 42);
    }

    // -----------------------------------------
    // Test: frozen WAL list persists
    // -----------------------------------------
    #[test]
    fn manifest_frozen_wal_list_works() {
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

    // -----------------------------------------
    // Test: SSTable add/remove
    // -----------------------------------------
    #[test]
    fn manifest_sstables_persist() {
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

    // -----------------------------------------
    // Test: update LSN
    // -----------------------------------------
    #[test]
    fn manifest_updates_lsn() {
        let temp = TempDir::new().unwrap();

        {
            let mut m = open_manifest(&temp);
            m.update_lsn(777).unwrap();
        }

        let m2 = open_manifest(&temp);
        assert_eq!(m2.get_last_lsn().unwrap(), 777);
    }

    // -----------------------------------------
    // Test: checkpoint creation and WAL truncation
    // -----------------------------------------
    #[test]
    fn manifest_checkpoint_truncates_wal() {
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

    // -----------------------------------------
    // Test: snapshot checksum protection
    // -----------------------------------------
    #[test]
    fn manifest_detects_corrupted_snapshot() {
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

    // -----------------------------------------
    // Test: crash -> WAL replay
    // -----------------------------------------
    #[test]
    fn manifest_reopens_after_crash_using_wal() {
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
