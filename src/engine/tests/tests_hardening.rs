//! Hardening tests: concurrency, internal iterator edge cases, `record_cmp`
//! unit tests, orphan SSTable cleanup, extreme configs, and misc robustness.
//!
//! This module exercises robustness scenarios that go beyond basic CRUD
//! correctness. It verifies that the engine is safe under concurrent reads
//! and writes (parallel get/put/scan/delete from multiple threads), that the
//! internal `record_cmp` comparator sorts records correctly (key ascending,
//! then LSN descending), that orphan `.sst` files left over from incomplete
//! operations are cleaned up on open, that extreme configurations (e.g. a
//! 64-byte write buffer) still produce correct results, and that the
//! `VisibilityFilter` correctly handles complex interleaved tombstone patterns.
//!
//! ## Layer coverage
//! - `memtable__*`: memtable-only unit tests and edge cases
//! - `memtable_sstable__*`: concurrency, extreme configs, SSTable cleanup,
//!   heavy-overwrite scan stability, complex visibility interleave
//! - `record_cmp_*` / `record_accessors`: pure unit tests (no storage layers)
//!
//! ## See also
//! - [`tests_edge_cases`] — boundary and validation tests
//! - [`tests_stress`] — large-scale load testing

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::tests::helpers::*;
    use crate::engine::utils::{Record, record_cmp};
    use crate::engine::{Engine, EngineConfig, SSTABLE_DIR};
    use std::sync::Arc;
    use tempfile::TempDir;

    // ================================================================
    // Concurrency: parallel reads during writes
    // ================================================================

    /// # Scenario
    /// Multiple threads perform concurrent reads, writes, and scans.
    ///
    /// # Starting environment
    /// Engine with 4 KB buffer; 10 keys (`key_0000`..`key_0009`) pre-seeded.
    ///
    /// # Actions
    /// 1. Spawn 1 writer thread inserting keys 10..109.
    /// 2. Spawn 4 reader threads each getting the 10 seeded keys.
    /// 3. Spawn 2 scanner threads each scanning the full range.
    /// 4. Wait for all threads to finish.
    /// 5. Get all 110 keys from the main thread.
    ///
    /// # Expected behavior
    /// No panics, no errors. After all threads complete, all 110 keys are
    /// present. Scans remain sorted under concurrency.
    #[test]
    fn memtable_sstable__concurrent_reads_during_writes() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::open(dir.path(), default_config()).unwrap());

        // Seed a few keys so readers have something to find
        for i in 0..10u32 {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("val_{:04}", i).into_bytes(),
                )
                .unwrap();
        }

        let mut handles = Vec::new();

        // Writer thread: puts keys 10..110
        let w_engine = Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            for i in 10..110u32 {
                w_engine
                    .put(
                        format!("key_{:04}", i).into_bytes(),
                        format!("val_{:04}", i).into_bytes(),
                    )
                    .unwrap();
            }
        }));

        // Reader threads: concurrent gets
        for _ in 0..4 {
            let r_engine = Arc::clone(&engine);
            handles.push(std::thread::spawn(move || {
                for i in 0..10u32 {
                    // Seeded keys must always be readable (never panic)
                    let result = r_engine.get(format!("key_{:04}", i).into_bytes());
                    assert!(result.is_ok(), "get must not error under concurrency");
                }
            }));
        }

        // Scanner threads: concurrent scans
        for _ in 0..2 {
            let s_engine = Arc::clone(&engine);
            handles.push(std::thread::spawn(move || {
                let results = s_engine.scan(b"key_", b"key_\xff").unwrap();
                let collected: Vec<_> = results.collect();
                // Must get *some* results (at least the 10 seeded keys)
                assert!(
                    !collected.is_empty(),
                    "scan under concurrency must return results"
                );
                // Must be sorted
                for pair in collected.windows(2) {
                    assert!(
                        pair[0].0 <= pair[1].0,
                        "scan must remain sorted under concurrency"
                    );
                }
            }));
        }

        for h in handles {
            h.join().expect("thread must not panic");
        }

        // After all threads finish, every key 0..110 should exist
        for i in 0..110u32 {
            assert_eq!(
                engine.get(format!("key_{:04}", i).into_bytes()).unwrap(),
                Some(format!("val_{:04}", i).into_bytes()),
                "key_{:04} must be present after concurrent writes",
                i
            );
        }
    }

    // ================================================================
    // Concurrency: scan during concurrent deletes
    // ================================================================

    /// # Scenario
    /// Scans run concurrently with deletes and still return sorted data.
    ///
    /// # Starting environment
    /// Engine with 4 KB buffer; 100 keys pre-seeded.
    ///
    /// # Actions
    /// 1. Spawn 1 deleter thread deleting even-indexed keys (0, 2, 4, …, 98).
    /// 2. Spawn 3 scanner threads running scans concurrently.
    /// 3. Wait for all threads.
    ///
    /// # Expected behavior
    /// No panics. Every scan returns sorted results — the engine maintains
    /// consistency even when deletes happen concurrently with scans.
    #[test]
    fn memtable_sstable__scan_during_concurrent_deletes() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(Engine::open(dir.path(), default_config()).unwrap());

        // Seed 100 keys
        for i in 0..100u32 {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("val_{:04}", i).into_bytes(),
                )
                .unwrap();
        }

        let mut handles = Vec::new();

        // Deleter thread: delete even keys
        let d_engine = Arc::clone(&engine);
        handles.push(std::thread::spawn(move || {
            for i in (0..100u32).step_by(2) {
                d_engine
                    .delete(format!("key_{:04}", i).into_bytes())
                    .unwrap();
            }
        }));

        // Scanner threads: run scans concurrently with deletes
        for _ in 0..3 {
            let s_engine = Arc::clone(&engine);
            handles.push(std::thread::spawn(move || {
                // Scan should never panic or return unsorted data
                let results = s_engine.scan(b"key_", b"key_\xff").unwrap();
                let collected: Vec<_> = results.collect();
                for pair in collected.windows(2) {
                    assert!(pair[0].0 <= pair[1].0, "scan must remain sorted");
                }
            }));
        }

        for h in handles {
            h.join().expect("thread must not panic");
        }
    }

    // ================================================================
    // VisibilityFilter: scan range with only tombstones (no Puts)
    // ================================================================

    /// # Scenario
    /// Scan a range that contains only tombstones (no corresponding puts).
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Issue point deletes for `"ghost_a"` and `"ghost_b"` (never put).
    /// 2. Issue range-delete `["ghost_c", "ghost_z")`.
    /// 3. Scan range `["ghost_", "ghost_\xff")`.
    ///
    /// # Expected behavior
    /// Scan returns empty — tombstones without corresponding puts produce
    /// no visible entries.
    #[test]
    fn memtable__visibility_filter_only_tombstones() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        // Write only deletes (no puts for these keys)
        engine.delete(b"ghost_a".to_vec()).unwrap();
        engine.delete(b"ghost_b".to_vec()).unwrap();
        engine
            .delete_range(b"ghost_c".to_vec(), b"ghost_z".to_vec())
            .unwrap();

        let results = collect_scan(&engine, b"ghost_", b"ghost_\xff");
        assert!(
            results.is_empty(),
            "scan over only tombstones must return empty"
        );
    }

    /// # Scenario
    /// Insert keys and then individually point-delete every one of them.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Put 20 keys.
    /// 2. Point-delete all 20 keys.
    /// 3. Scan the full range.
    ///
    /// # Expected behavior
    /// Scan returns empty — every key is hidden by its individual tombstone.
    #[test]
    fn memtable__visibility_all_puts_then_deleted() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        for i in 0..20u32 {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("val_{:04}", i).into_bytes(),
                )
                .unwrap();
        }

        // Point-delete every key
        for i in 0..20u32 {
            engine.delete(format!("key_{:04}", i).into_bytes()).unwrap();
        }

        let results = collect_scan(&engine, b"key_", b"key_\xff");
        assert!(
            results.is_empty(),
            "scan must be empty when every key is individually point-deleted"
        );
    }

    // ================================================================
    // EngineScanIterator / MergeIterator: empty sources (scan on empty engine)
    // ================================================================

    /// # Scenario
    /// Scan on an engine that has never received any data.
    ///
    /// # Starting environment
    /// Freshly opened engine with memtable-only config — completely empty.
    ///
    /// # Actions
    /// 1. Scan the widest possible range `["\x00", "\xff")`.
    ///
    /// # Expected behavior
    /// Returns empty — no keys exist, and no internal structure produces
    /// spurious entries.
    #[test]
    fn memtable__scan_empty_engine() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        let results = collect_scan(&engine, b"\x00", b"\xff");
        assert!(results.is_empty(), "scan on empty engine must return empty");
    }

    /// # Scenario
    /// Scan on an engine that was opened, closed (with no data), and reopened.
    ///
    /// # Starting environment
    /// Engine opened with default config, then immediately closed (creates
    /// WAL files but no actual data).
    ///
    /// # Actions
    /// 1. Open engine, close (no writes).
    /// 2. Reopen, scan `["\x00", "\xff")`.
    ///
    /// # Expected behavior
    /// Returns empty — the WAL and manifest are initialized but contain
    /// no user data.
    #[test]
    fn memtable_sstable__scan_empty_engine_reopened() {
        // Close and reopen an empty engine — creates WAL but no data
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.close().unwrap();

        let engine = reopen(dir.path());
        let results = collect_scan(&engine, b"\x00", b"\xff");
        assert!(
            results.is_empty(),
            "scan on empty reopened engine must return empty"
        );
    }

    // ================================================================
    // record_cmp unit tests
    // ================================================================

    /// # Scenario
    /// Unit test for `record_cmp`: records with different keys sort by key
    /// in ascending lexicographic order.
    ///
    /// # Starting environment
    /// Two `Record::Put` structs with keys `"aaa"` and `"bbb"` (same LSN).
    ///
    /// # Actions
    /// 1. Compare `("aaa", lsn=1)` vs `("bbb", lsn=1)`.
    ///
    /// # Expected behavior
    /// `"aaa" < "bbb"` — `record_cmp` returns `Less` for the smaller key.
    #[test]
    fn record_cmp_sorts_by_key_ascending() {
        let a = Record::Put {
            key: b"aaa".to_vec(),
            value: vec![],
            lsn: 1,
            timestamp: 0,
        };
        let b = Record::Put {
            key: b"bbb".to_vec(),
            value: vec![],
            lsn: 1,
            timestamp: 0,
        };
        assert_eq!(record_cmp(&a, &b), std::cmp::Ordering::Less);
        assert_eq!(record_cmp(&b, &a), std::cmp::Ordering::Greater);
    }

    /// # Scenario
    /// Unit test for `record_cmp`: for the same key, higher LSN sorts first.
    ///
    /// # Starting environment
    /// Two `Record::Put` structs with the same key `"key"` but different LSNs.
    ///
    /// # Actions
    /// 1. Compare `("key", lsn=5)` vs `("key", lsn=1)`.
    ///
    /// # Expected behavior
    /// The record with LSN 5 sorts before LSN 1 — `record_cmp` returns
    /// `Less` for larger LSN, ensuring newest entries are seen first
    /// during merge iteration.
    #[test]
    fn record_cmp_same_key_higher_lsn_first() {
        let old = Record::Put {
            key: b"key".to_vec(),
            value: vec![],
            lsn: 1,
            timestamp: 0,
        };
        let new = Record::Put {
            key: b"key".to_vec(),
            value: vec![],
            lsn: 5,
            timestamp: 0,
        };
        // record_cmp returns Less when `a` should come first.
        // For same key, higher LSN should come first → new < old
        assert_eq!(
            record_cmp(&new, &old),
            std::cmp::Ordering::Less,
            "higher LSN must sort before lower LSN for same key"
        );
        assert_eq!(
            record_cmp(&old, &new),
            std::cmp::Ordering::Greater,
            "lower LSN must sort after higher LSN for same key"
        );
    }

    /// # Scenario
    /// Unit test for `record_cmp`: same key and same LSN are `Equal`
    /// regardless of record type.
    ///
    /// # Starting environment
    /// A `Record::Delete` and a `Record::Put` with identical key `"key"`
    /// and LSN 3.
    ///
    /// # Actions
    /// 1. Compare the two records.
    ///
    /// # Expected behavior
    /// `record_cmp` returns `Equal` — the comparator does not distinguish
    /// by record type when key and LSN match.
    #[test]
    fn record_cmp_same_key_same_lsn_is_equal() {
        let a = Record::Delete {
            key: b"key".to_vec(),
            lsn: 3,
            timestamp: 0,
        };
        let b = Record::Put {
            key: b"key".to_vec(),
            value: vec![1],
            lsn: 3,
            timestamp: 0,
        };
        assert_eq!(
            record_cmp(&a, &b),
            std::cmp::Ordering::Equal,
            "same key + same LSN = Equal regardless of record type"
        );
    }

    /// # Scenario
    /// Unit test for `record_cmp`: `RangeDelete` records sort by their
    /// `start` key, not `end`.
    ///
    /// # Starting environment
    /// A `Record::RangeDelete { start: "ccc", end: "zzz" }` and a
    /// `Record::Put { key: "aaa" }`, both with LSN 1.
    ///
    /// # Actions
    /// 1. Compare the range-delete vs the point put.
    ///
    /// # Expected behavior
    /// RangeDelete sorts as `Greater` because `"ccc" > "aaa"` — the start
    /// key is used for ordering, not the end key.
    #[test]
    fn record_cmp_range_delete_uses_start_key() {
        let range = Record::RangeDelete {
            start: b"ccc".to_vec(),
            end: b"zzz".to_vec(),
            lsn: 1,
            timestamp: 0,
        };
        let point = Record::Put {
            key: b"aaa".to_vec(),
            value: vec![],
            lsn: 1,
            timestamp: 0,
        };
        assert_eq!(
            record_cmp(&range, &point),
            std::cmp::Ordering::Greater,
            "RangeDelete sorts by start key"
        );
    }

    // ================================================================
    // Record accessor methods
    // ================================================================

    /// # Scenario
    /// Unit test for `Record` accessor methods (`key()`, `lsn()`, `timestamp()`).
    ///
    /// # Starting environment
    /// Three records: `Put`, `Delete`, and `RangeDelete` with known field values.
    ///
    /// # Actions
    /// 1. Call `key()`, `lsn()`, and `timestamp()` on each record variant.
    ///
    /// # Expected behavior
    /// - `Put`: key = `"pk"`, lsn = 10, timestamp = 100.
    /// - `Delete`: key = `"dk"`, lsn = 20, timestamp = 200.
    /// - `RangeDelete`: `key()` returns `start` = `"rs"`, lsn = 30,
    ///   timestamp = 300.
    #[test]
    fn record_accessors() {
        let put = Record::Put {
            key: b"pk".to_vec(),
            value: b"pv".to_vec(),
            lsn: 10,
            timestamp: 100,
        };
        assert_eq!(put.key(), &b"pk".to_vec());
        assert_eq!(put.lsn(), 10);
        assert_eq!(put.timestamp(), 100);

        let del = Record::Delete {
            key: b"dk".to_vec(),
            lsn: 20,
            timestamp: 200,
        };
        assert_eq!(del.key(), &b"dk".to_vec());
        assert_eq!(del.lsn(), 20);
        assert_eq!(del.timestamp(), 200);

        let rd = Record::RangeDelete {
            start: b"rs".to_vec(),
            end: b"re".to_vec(),
            lsn: 30,
            timestamp: 300,
        };
        assert_eq!(rd.key(), &b"rs".to_vec(), "RangeDelete.key() returns start");
        assert_eq!(rd.lsn(), 30);
        assert_eq!(rd.timestamp(), 300);
    }

    // ================================================================
    // Orphan SSTable cleanup
    //
    // open() scans the root path for `*.sst` files not tracked
    // by the manifest and removes them.
    // ================================================================

    /// # Scenario
    /// Orphan SSTable file (not tracked by the manifest) is removed on
    /// engine open.
    ///
    /// # Starting environment
    /// Session 1: engine opened, one key written, engine closed normally.
    /// An orphan file `sst-999.sst` is manually created in the data directory.
    ///
    /// # Actions
    /// 1. Verify the orphan file exists on disk.
    /// 2. Reopen the engine.
    /// 3. Check if the orphan file still exists.
    /// 4. Get the original key.
    ///
    /// # Expected behavior
    /// The orphan `sst-999.sst` file is deleted during `open()`. The
    /// legitimate data remains intact.
    #[test]
    fn memtable_sstable__orphan_sst_cleanup() {
        let dir = TempDir::new().unwrap();

        // Session 1: write some data and close normally
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.close().unwrap();

        // Create an orphan file matching the cleanup pattern (*.sst)
        // in the sstables/ subdirectory where the engine actually scans
        let orphan_path = dir.path().join("sstables/000999.sst");
        std::fs::write(&orphan_path, b"orphan data").unwrap();
        assert!(
            orphan_path.exists(),
            "orphan file should exist before reopen"
        );

        // Session 2: reopen — cleanup should remove the orphan
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        assert!(
            !orphan_path.exists(),
            "orphan *.sst file should be removed on open"
        );

        // Original data should still work
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v".to_vec()),);
    }

    /// # Scenario
    /// Files that do NOT match the orphan pattern (`*.sst`) are preserved.
    ///
    /// # Starting environment
    /// Engine opened and closed. A file named `notes.sst` (does not have
    /// the numeric ID) is created in the sstables directory.
    ///
    /// # Actions
    /// 1. Reopen the engine.
    /// 2. Check if `notes.sst` still exists.
    ///
    /// # Expected behavior
    /// `notes.sst` is preserved — the orphan cleanup only targets files
    /// matching the `*.sst` naming pattern.
    #[test]
    fn memtable_sstable__non_orphan_sst_preserved() {
        let dir = TempDir::new().unwrap();

        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.close().unwrap();

        // File that does NOT match pattern (no numeric-only .sst)
        let safe_path = dir.path().join("sstables/notes.sst");
        std::fs::write(&safe_path, b"not an orphan").unwrap();

        let _engine = Engine::open(dir.path(), default_config()).unwrap();
        assert!(
            safe_path.exists(),
            "non-matching .sst file should NOT be removed"
        );
    }

    // ================================================================
    // flush_frozen_to_sstable_inner: closing empty engine is safe
    // ================================================================

    /// # Scenario
    /// Closing an engine that never received any data is safe.
    ///
    /// # Starting environment
    /// Freshly opened engine with default config — no puts, no deletes.
    ///
    /// # Actions
    /// 1. Call `close()` immediately (no frozen memtable, no checkpoint data).
    /// 2. Reopen and get a nonexistent key.
    ///
    /// # Expected behavior
    /// No error or panic on close. Reopen succeeds, get returns `None`.
    #[test]
    fn memtable__close_empty_engine_safe() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        // close() with no data: no frozen to flush, no checkpoint data
        engine.close().unwrap();

        // Reopen should also work fine
        let engine = reopen(dir.path());
        assert_eq!(engine.get(b"anything".to_vec()).unwrap(), None);
    }

    /// # Scenario
    /// Close after performing only reads (no writes in the current session).
    ///
    /// # Starting environment
    /// Session 1: one key written and engine closed.
    ///
    /// # Actions
    /// 1. Session 2: reopen, perform get and scan (read-only), then close.
    /// 2. Session 3: reopen and get the key.
    ///
    /// # Expected behavior
    /// Data remains intact — a read-only session followed by close does not
    /// corrupt or lose previously persisted data.
    #[test]
    fn memtable_sstable__close_after_only_reads() {
        let dir = TempDir::new().unwrap();

        // Session 1: write data
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.close().unwrap();

        // Session 2: only reads, then close
        let engine = reopen(dir.path());
        let _ = engine.get(b"k".to_vec()).unwrap();
        let _ = collect_scan(&engine, b"\x00", b"\xff");
        engine.close().unwrap();

        // Session 3: data still intact
        let engine = reopen(dir.path());
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v".to_vec()),);
    }

    // ================================================================
    // Extreme configs: write_buffer_size = 64 (near-minimum)
    // ================================================================

    /// # Scenario
    /// Extreme config: 64-byte write buffer forces a freeze/flush cycle on
    /// nearly every put.
    ///
    /// # Starting environment
    /// Engine with `write_buffer_size = 64` (near-minimum).
    ///
    /// # Actions
    /// 1. Insert 10 keys.
    /// 2. Get all 10 keys.
    /// 3. Check stats.
    ///
    /// # Expected behavior
    /// All 10 keys are readable. `sstables_count > 0` — the tiny buffer
    /// produces SSTables on nearly every write, but data integrity is maintained.
    #[test]
    fn memtable_sstable__extreme_tiny_buffer() {
        init_tracing();
        let dir = TempDir::new().unwrap();
        let config = EngineConfig {
            write_buffer_size: 64,
            compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 1024,
            min_threshold: 4,
            max_threshold: 32,
            tombstone_ratio_threshold: 0.2,
            tombstone_compaction_interval: 3600,
            tombstone_bloom_fallback: false,
            tombstone_range_drop: false,
            thread_pool_size: 2,
        };

        let engine = Engine::open(dir.path(), config).unwrap();

        // Every single put should trigger a freeze/flush cycle
        for i in 0..10u32 {
            engine
                .put(
                    format!("k{}", i).into_bytes(),
                    format!("v{}", i).into_bytes(),
                )
                .unwrap();
        }

        // All keys should be readable
        for i in 0..10u32 {
            assert_eq!(
                engine.get(format!("k{}", i).into_bytes()).unwrap(),
                Some(format!("v{}", i).into_bytes()),
                "k{} must be readable with tiny buffer",
                i
            );
        }

        // Should have many SSTables
        engine.flush_all_frozen().unwrap();
        let stats = engine.stats().unwrap();
        assert!(
            stats.sstables_count > 0,
            "tiny buffer should produce SSTables"
        );
    }

    // ================================================================
    // Extreme config: tiny buffer with deletes
    // ================================================================

    /// # Scenario
    /// Extreme config with 64-byte buffer: deletes work correctly when every
    /// operation triggers a freeze cycle.
    ///
    /// # Starting environment
    /// Engine with `write_buffer_size = 64`.
    ///
    /// # Actions
    /// 1. Put `"a"` = `"1"` and `"b"` = `"2"`.
    /// 2. Delete `"a"`.
    /// 3. Get `"a"` and `"b"`.
    ///
    /// # Expected behavior
    /// `"a"` returns `None` (deleted); `"b"` returns `Some("2")`. Tombstones
    /// work correctly even with aggressive freeze/flush cycles.
    #[test]
    fn memtable_sstable__extreme_tiny_buffer_deletes() {
        init_tracing();
        let dir = TempDir::new().unwrap();
        let config = EngineConfig {
            write_buffer_size: 64,
            compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 1024,
            min_threshold: 4,
            max_threshold: 32,
            tombstone_ratio_threshold: 0.2,
            tombstone_compaction_interval: 3600,
            tombstone_bloom_fallback: false,
            tombstone_range_drop: false,
            thread_pool_size: 2,
        };

        let engine = Engine::open(dir.path(), config).unwrap();
        engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        engine.delete(b"a".to_vec()).unwrap();

        assert_eq!(engine.get(b"a".to_vec()).unwrap(), None);
        assert_eq!(engine.get(b"b".to_vec()).unwrap(), Some(b"2".to_vec()),);
    }

    // ================================================================
    // Extreme config: tiny buffer with recovery
    // ================================================================

    /// # Scenario
    /// Extreme config with 64-byte buffer: data survives close → reopen.
    ///
    /// # Starting environment
    /// Engine with `write_buffer_size = 64`.
    ///
    /// # Actions
    /// 1. Insert 5 keys.
    /// 2. Close and reopen (using `default_config` for reopen).
    /// 3. Get all 5 keys.
    ///
    /// # Expected behavior
    /// All 5 keys are present after reopen — frequent SSTable flushes from
    /// the tiny buffer do not compromise recovery.
    #[test]
    fn memtable_sstable__extreme_tiny_buffer_recovery() {
        init_tracing();
        let dir = TempDir::new().unwrap();
        let config = EngineConfig {
            write_buffer_size: 64,
            compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 1024,
            min_threshold: 4,
            max_threshold: 32,
            tombstone_ratio_threshold: 0.2,
            tombstone_compaction_interval: 3600,
            tombstone_bloom_fallback: false,
            tombstone_range_drop: false,
            thread_pool_size: 2,
        };

        let engine = Engine::open(dir.path(), config).unwrap();
        for i in 0..5u32 {
            engine
                .put(
                    format!("k{}", i).into_bytes(),
                    format!("v{}", i).into_bytes(),
                )
                .unwrap();
        }
        engine.close().unwrap();

        let engine = reopen(dir.path());
        for i in 0..5u32 {
            assert_eq!(
                engine.get(format!("k{}", i).into_bytes()).unwrap(),
                Some(format!("v{}", i).into_bytes()),
            );
        }
    }

    // ================================================================
    // SSTables directory: legitimate SSTable files survive reopen
    // ================================================================

    /// # Scenario
    /// Legitimate SSTable files tracked by the manifest are preserved across
    /// close → reopen (not mistakenly cleaned up as orphans).
    ///
    /// # Starting environment
    /// Engine with 200 keys flushed to SSTables.
    ///
    /// # Actions
    /// 1. Record sstables_count and count `.sst` files on disk.
    /// 2. Close and reopen.
    /// 3. Compare stats and verify all 200 keys.
    ///
    /// # Expected behavior
    /// `sstables_count` after reopen equals the count before close. All data
    /// is intact — manifest-tracked SSTables are never removed.
    #[test]
    fn memtable_sstable__legitimate_sst_survive_reopen() {
        let dir = TempDir::new().unwrap();

        // Write enough data to create SSTables
        let engine = engine_with_sstables(dir.path(), 200, "key");
        let stats_before = engine.stats().unwrap();
        assert!(stats_before.sstables_count > 0);

        // Count actual SSTable files in the sstables directory
        let sst_dir = dir.path().join(SSTABLE_DIR);
        let file_count_before = std::fs::read_dir(&sst_dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .ok()
                    .and_then(|e| e.path().extension().map(|ext| ext == "sst"))
                    .unwrap_or(false)
            })
            .count();
        assert!(file_count_before > 0, "SSTable files must exist on disk");

        engine.close().unwrap();

        // Reopen — legitimate SSTables should not be removed
        let engine = reopen(dir.path());
        let stats_after = engine.stats().unwrap();
        assert_eq!(
            stats_before.sstables_count, stats_after.sstables_count,
            "SSTable count must be preserved across reopen"
        );

        // Verify data is intact
        for i in 0..200usize {
            assert!(
                engine
                    .get(format!("key_{:04}", i).into_bytes())
                    .unwrap()
                    .is_some(),
                "key_{:04} must survive reopen",
                i
            );
        }
    }

    // ================================================================
    // Scan stability: unsorted duplicate overwrites yield correct order
    // ================================================================

    /// # Scenario
    /// Heavy overwrites crossing SSTable boundaries — scan must still return
    /// exactly one entry per key with the latest value, in sorted order.
    ///
    /// # Starting environment
    /// Engine with 128-byte buffer (small_buffer_config).
    ///
    /// # Actions
    /// 1. Write 10 keys, each overwritten 20 times (200 total puts), crossing
    ///    many SSTable boundaries.
    /// 2. Scan the full range.
    ///
    /// # Expected behavior
    /// Returns exactly 10 unique keys, all with round-19 values, in sorted
    /// order. No duplicates from earlier overwrites leak through.
    #[test]
    fn memtable_sstable__scan_stability_heavy_overwrites() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        // Write 10 keys, each overwritten 20 times, crossing SSTable boundaries
        for round in 0..20u32 {
            for i in 0..10u32 {
                engine
                    .put(
                        format!("key_{:04}", i).into_bytes(),
                        format!("val_r{}_{:04}", round, i).into_bytes(),
                    )
                    .unwrap();
            }
        }

        let results = collect_scan(&engine, b"key_", b"key_\xff");
        // Exactly 10 unique keys
        assert_eq!(results.len(), 10, "must have exactly 10 unique keys");

        // All should show the latest round (19)
        for (key, val) in &results {
            let key_str = String::from_utf8_lossy(key);
            let val_str = String::from_utf8_lossy(val);
            assert!(
                val_str.starts_with("val_r19_"),
                "key={} should have round 19 value, got {}",
                key_str,
                val_str
            );
        }

        // Must be sorted
        for pair in results.windows(2) {
            assert!(pair[0].0 < pair[1].0, "scan must be sorted");
        }
    }

    // ================================================================
    // Scan consistency: interleaved point + range deletes with puts
    // result in correct VisibilityFilter behavior
    // ================================================================

    /// # Scenario
    /// Complex interleaved point deletes, range deletes, re-inserts, and
    /// overwrites — validates the `VisibilityFilter`’s correctness.
    ///
    /// # Starting environment
    /// Engine with 128-byte buffer.
    ///
    /// # Actions
    /// 1. Put keys `a`..`z`.
    /// 2. Range-delete `[d, h)` → removes `d`, `e`, `f`, `g`.
    /// 3. Re-insert `f` = `"resurrected_f"`.
    /// 4. Point-delete `j`.
    /// 5. Range-delete `[p, t)` → removes `p`, `q`, `r`, `s`.
    /// 6. Overwrite `a` = `"new_a"`.
    /// 7. Scan `[a, z+1)`.
    ///
    /// # Expected behavior
    /// - `a` → `"new_a"` (overwritten).
    /// - `d`, `e`, `g` → absent (range-deleted).
    /// - `f` → `"resurrected_f"` (re-inserted after range-delete).
    /// - `j` → absent (point-deleted).
    /// - `p`, `q`, `r`, `s` → absent (range-deleted).
    /// - All other letters present with original values.
    #[test]
    fn memtable_sstable__visibility_complex_interleave() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        // Phase 1: put keys a..z
        for c in b'a'..=b'z' {
            engine.put(vec![c], vec![c, c]).unwrap();
        }

        // Phase 2: range-delete [d, h) → removes d,e,f,g
        engine.delete_range(vec![b'd'], vec![b'h']).unwrap();

        // Phase 3: re-insert 'f' (inside the deleted range)
        engine.put(vec![b'f'], b"resurrected_f".to_vec()).unwrap();

        // Phase 4: point-delete 'j'
        engine.delete(vec![b'j']).unwrap();

        // Phase 5: range-delete [p, t) → removes p,q,r,s
        engine.delete_range(vec![b'p'], vec![b't']).unwrap();

        // Phase 6: overwrite 'a' to verify it appears with new value
        engine.put(vec![b'a'], b"new_a".to_vec()).unwrap();

        let results = collect_scan(&engine, b"a", &[b'z' + 1]);
        let keys: Vec<u8> = results.iter().map(|(k, _)| k[0]).collect();
        let map: std::collections::HashMap<u8, Vec<u8>> =
            results.into_iter().map(|(k, v)| (k[0], v)).collect();

        // 'a' should be present with new value
        assert_eq!(map.get(&b'a').unwrap(), b"new_a");

        // d,e,g should be gone (range-deleted)
        for c in [b'd', b'e', b'g'] {
            assert!(
                !keys.contains(&c),
                "'{}' should be range-deleted",
                c as char
            );
        }

        // 'f' should be resurrected
        assert_eq!(map.get(&b'f').unwrap(), b"resurrected_f");

        // 'j' should be point-deleted
        assert!(!keys.contains(&b'j'), "'j' should be point-deleted");

        // p,q,r,s should be gone
        for c in [b'p', b'q', b'r', b's'] {
            assert!(
                !keys.contains(&c),
                "'{}' should be range-deleted",
                c as char
            );
        }

        // Remaining letters should be present
        for c in [
            b'b', b'c', b'h', b'i', b'k', b'l', b'm', b'n', b'o', b't', b'u', b'v', b'w', b'x',
            b'y', b'z',
        ] {
            assert!(keys.contains(&c), "'{}' should be present", c as char);
        }
    }
}
