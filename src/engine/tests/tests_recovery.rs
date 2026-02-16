//! Recovery / reopen tests: verify durability across close → reopen.
//!
//! These tests exercise the clean-shutdown recovery path: the engine is closed
//! (which flushes frozen memtables to SSTables and checkpoints the WAL), then
//! reopened. Every test verifies that data written before close is fully
//! accessible after reopen. Coverage includes single puts, overwrites, point
//! deletes, range deletes, large SSTable datasets, multiple reopen cycles,
//! WAL-only replay, scan correctness after reopen, and overwrite chains.
//!
//! ## Layer coverage
//! - All tests use `memtable_sstable` (close flushes WAL/frozen → SSTable)
//! - `memtable_sstable__wal_replay_*`: WAL-only recovery (large buffer —
//!   data stays in the WAL and is replayed on reopen, never reaching SSTables)
//!
//! ## See also
//! - [`tests_crash_recovery`] — drop without close() (frozen WAL replay path)
//! - [`tests_lsn_continuity`] — LSN ordering correctness after reopen
//! - [`tests_put_get`] — basic put/get correctness (no reopen)

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Basic: put, close, reopen → data survives
    // ----------------------------------------------------------------

    /// # Scenario
    /// Basic durability: data written before close is readable after reopen.
    ///
    /// # Starting environment
    /// Fresh engine with 4 KB buffer — no prior data.
    ///
    /// # Actions
    /// 1. Put `"key1"` = `"val1"` and `"key2"` = `"val2"`.
    /// 2. Close the engine.
    /// 3. Reopen the engine.
    /// 4. Get both keys.
    ///
    /// # Expected behavior
    /// Both keys return their original values after reopen.
    #[test]
    fn memtable_sstable__data_survives_close_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            engine.put(b"key1".to_vec(), b"val1".to_vec()).unwrap();
            engine.put(b"key2".to_vec(), b"val2".to_vec()).unwrap();
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        assert_eq!(
            engine.get(b"key1".to_vec()).unwrap(),
            Some(b"val1".to_vec())
        );
        assert_eq!(
            engine.get(b"key2".to_vec()).unwrap(),
            Some(b"val2".to_vec())
        );
    }

    // ----------------------------------------------------------------
    // Overwrite survives reopen
    // ----------------------------------------------------------------

    /// # Scenario
    /// The latest overwrite value survives close → reopen.
    ///
    /// # Starting environment
    /// Fresh engine with 4 KB buffer.
    ///
    /// # Actions
    /// 1. Put `"k"` = `"v1"`, then overwrite with `"v2"`.
    /// 2. Close and reopen.
    /// 3. Get `"k"`.
    ///
    /// # Expected behavior
    /// Returns `Some("v2")` — only the most recent write persists.
    #[test]
    fn memtable_sstable__overwrite_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            engine.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
            engine.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v2".to_vec()));
    }

    // ----------------------------------------------------------------
    // Delete survives reopen
    // ----------------------------------------------------------------

    /// # Scenario
    /// A point-delete tombstone survives close → reopen.
    ///
    /// # Starting environment
    /// Fresh engine with 4 KB buffer.
    ///
    /// # Actions
    /// 1. Put `"k"` = `"val"`, then delete `"k"`.
    /// 2. Close and reopen.
    /// 3. Get `"k"`.
    ///
    /// # Expected behavior
    /// Returns `None` — the tombstone is persisted and still hides the key.
    #[test]
    fn memtable_sstable__delete_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            engine.put(b"k".to_vec(), b"val".to_vec()).unwrap();
            engine.delete(b"k".to_vec()).unwrap();
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), None);
    }

    // ----------------------------------------------------------------
    // Range delete survives reopen
    // ----------------------------------------------------------------

    /// # Scenario
    /// A range-delete tombstone survives close → reopen.
    ///
    /// # Starting environment
    /// Fresh engine with 4 KB buffer.
    ///
    /// # Actions
    /// 1. Put 20 keys (`key_00`..`key_19`).
    /// 2. Range-delete `["key_05", "key_15")`.
    /// 3. Close and reopen.
    /// 4. Get all 20 keys.
    ///
    /// # Expected behavior
    /// Keys 0–4 and 15–19 survive; keys 5–14 return `None` (range-deleted).
    /// The range tombstone is correctly persisted.
    #[test]
    fn memtable_sstable__range_delete_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            for i in 0..20 {
                let key = format!("key_{:02}", i).into_bytes();
                let val = format!("val_{:02}", i).into_bytes();
                engine.put(key, val).unwrap();
            }
            engine
                .delete_range(b"key_05".to_vec(), b"key_15".to_vec())
                .unwrap();
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        for i in 0..5 {
            let key = format!("key_{:02}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{:02} should survive",
                i
            );
        }
        for i in 5..15 {
            let key = format!("key_{:02}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{:02} should be range-deleted",
                i
            );
        }
        for i in 15..20 {
            let key = format!("key_{:02}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{:02} should survive",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Large dataset → SSTable flush → reopen → data intact
    // ----------------------------------------------------------------

    /// # Scenario
    /// A large dataset (200 keys) that spans multiple SSTables survives reopen.
    ///
    /// # Starting environment
    /// Engine with 200 keys flushed to SSTables (`engine_with_sstables`).
    ///
    /// # Actions
    /// 1. Verify `sstables_count > 0`.
    /// 2. Close and reopen.
    /// 3. Get all 200 keys.
    ///
    /// # Expected behavior
    /// Every key returns its correct padded value — SSTable data is fully
    /// intact after reopen.
    #[test]
    fn memtable_sstable__sstable_data_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = engine_with_sstables(tmp.path(), 200, "key");
            assert!(engine.stats().unwrap().sstables_count > 0);
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        for i in 0..200 {
            let key = format!("key_{:04}", i).into_bytes();
            let expected = format!("value_with_some_padding_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key.clone()).unwrap(),
                Some(expected),
                "key_{:04} missing after reopen",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Multiple close-reopen cycles
    // ----------------------------------------------------------------

    /// # Scenario
    /// Data accumulated across 3 separate open/write/close cycles is all
    /// available after a final reopen.
    ///
    /// # Starting environment
    /// Temporary directory with no prior data.
    ///
    /// # Actions
    /// 1. For each cycle 0–2: open engine, put 20 keys (`c{cycle}_{i}`), close.
    /// 2. Reopen and get all 60 keys.
    ///
    /// # Expected behavior
    /// All 60 keys (20 per cycle) are present with correct values — data
    /// from earlier cycles is not lost by subsequent cycles.
    #[test]
    fn memtable_sstable__multiple_reopen_cycles() {
        let tmp = TempDir::new().unwrap();

        for cycle in 0..3 {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            for i in 0..20 {
                let key = format!("c{}_{:02}", cycle, i).into_bytes();
                let val = format!("val_{}_{:02}", cycle, i).into_bytes();
                engine.put(key, val).unwrap();
            }
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        for cycle in 0..3 {
            for i in 0..20 {
                let key = format!("c{}_{:02}", cycle, i).into_bytes();
                let expected = format!("val_{}_{:02}", cycle, i).into_bytes();
                assert_eq!(
                    engine.get(key.clone()).unwrap(),
                    Some(expected),
                    "cycle {} key {} missing",
                    cycle,
                    i
                );
            }
        }
    }

    // ----------------------------------------------------------------
    // WAL replay: data in active memtable (not yet flushed) is recovered
    // ----------------------------------------------------------------

    /// # Scenario
    /// Data that stayed only in the WAL (never flushed to SSTable) is
    /// recovered on reopen via WAL replay.
    ///
    /// # Starting environment
    /// Engine with 64 KB buffer (memtable_only_config) — no flush triggered.
    ///
    /// # Actions
    /// 1. Put `"wal_key"` = `"wal_val"` (stays in WAL, not flushed).
    /// 2. Close and reopen.
    /// 3. Get `"wal_key"`.
    ///
    /// # Expected behavior
    /// Returns `Some("wal_val")` — the WAL is replayed during open(),
    /// reconstructing the memtable from the WAL entries.
    #[test]
    fn memtable_sstable__wal_replay_recovers_data() {
        let tmp = TempDir::new().unwrap();

        {
            // Use large buffer so nothing flushes — data stays in WAL only
            let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
            engine
                .put(b"wal_key".to_vec(), b"wal_val".to_vec())
                .unwrap();
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        assert_eq!(
            engine.get(b"wal_key".to_vec()).unwrap(),
            Some(b"wal_val".to_vec())
        );
    }

    // ----------------------------------------------------------------
    // Scan correctness after reopen
    // ----------------------------------------------------------------

    /// # Scenario
    /// Scan returns correct, sorted results after reopen.
    ///
    /// # Starting environment
    /// Engine with 50 keys inserted and then closed.
    ///
    /// # Actions
    /// 1. Put 50 keys, close.
    /// 2. Reopen, scan range `["sk_", "sk_\xff")`.
    ///
    /// # Expected behavior
    /// Returns all 50 keys in strictly sorted order.
    #[test]
    fn memtable_sstable__scan_works_after_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            for i in 0..50 {
                let key = format!("sk_{:04}", i).into_bytes();
                let val = format!("sv_{:04}", i).into_bytes();
                engine.put(key, val).unwrap();
            }
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        let results = collect_scan(&engine, b"sk_", b"sk_\xff");

        assert_eq!(results.len(), 50);
        // Verify sorted order
        for i in 1..results.len() {
            assert!(results[i - 1].0 < results[i].0, "Keys should be sorted");
        }
    }

    // ----------------------------------------------------------------
    // Delete + reopen + verify tombstone is durable
    // ----------------------------------------------------------------

    /// # Scenario
    /// Delete tombstones of SSTable keys survive close → reopen.
    ///
    /// # Starting environment
    /// Engine with 200 keys flushed to SSTables.
    ///
    /// # Actions
    /// 1. Delete the first 50 keys (`dt_0000`..`dt_0049`).
    /// 2. Close and reopen.
    /// 3. Get all 200 keys.
    ///
    /// # Expected behavior
    /// Keys 0–49: `None` (tombstones persisted). Keys 50–199: present.
    #[test]
    fn memtable_sstable__delete_tombstone_durable_after_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = engine_with_sstables(tmp.path(), 200, "dt");
            // Delete some SSTable keys
            for i in 0..50 {
                let key = format!("dt_{:04}", i).into_bytes();
                engine.delete(key).unwrap();
            }
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        for i in 0..50 {
            let key = format!("dt_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "dt_{:04} should still be deleted after reopen",
                i
            );
        }
        for i in 50..200 {
            let key = format!("dt_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "dt_{:04} should still exist after reopen",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Multiple overwrites → reopen → latest value
    // ----------------------------------------------------------------

    /// # Scenario
    /// A key overwritten 5 times returns only the latest value after reopen.
    ///
    /// # Starting environment
    /// Fresh engine with 4 KB buffer.
    ///
    /// # Actions
    /// 1. Put `"chain"` with values `"v0"` through `"v4"` (5 overwrites).
    /// 2. Close and reopen.
    /// 3. Get `"chain"`.
    ///
    /// # Expected behavior
    /// Returns `Some("v4")` — the most recent overwrite wins after recovery.
    #[test]
    fn memtable_sstable__overwrite_chain_survives_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let engine = Engine::open(tmp.path(), default_config()).unwrap();
            for round in 0..5 {
                engine
                    .put(b"chain".to_vec(), format!("v{}", round).into_bytes())
                    .unwrap();
            }
            engine.close().unwrap();
        }

        let engine = reopen(tmp.path());
        assert_eq!(engine.get(b"chain".to_vec()).unwrap(), Some(b"v4".to_vec()));
    }
}
