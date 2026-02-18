//! Edge-case tests: empty keys/values, scan boundaries, stats correctness,
//! close semantics, large/binary keys, and misc corner cases.
//!
//! This module exercises non-happy-path scenarios that a production database must
//! handle gracefully. It covers input validation (empty keys/values are rejected),
//! scan boundary semantics (start-inclusive / end-exclusive, inverted ranges),
//! stats counter transitions during freeze/flush, engine behavior after `close()`,
//! recovery of very large and binary keys through SSTables, and operations on
//! an empty database. These tests ensure the engine is robust against unusual
//! but valid (or intentionally invalid) usage patterns.
//!
//! ## Layer coverage
//! - `memtable__*`: memtable-only edge cases (validation, boundaries, empty DB)
//! - `memtable_sstable__*`: edge cases involving SSTable flush, recovery, and
//!   engine lifecycle (close, reopen, stats)
//!
//! ## See also
//! - [`tests_hardening`] — concurrency, extreme configs, orphan cleanup
//! - [`tests_scan`] — standard scan correctness tests

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::tests::helpers::*;
    use crate::engine::{Engine, EngineConfig};
    use tempfile::TempDir;

    // ================================================================
    // Empty key / empty value
    // ================================================================

    /// # Scenario
    /// Attempt to insert a key with an empty byte vector.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Call `put(vec![], b"value")` with an empty key.
    ///
    /// # Expected behavior
    /// The engine returns an error — empty keys are rejected at the API level.
    #[test]
    fn memtable__empty_key_is_rejected() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        let result = engine.put(vec![], b"value".to_vec());
        assert!(result.is_err(), "empty key should be rejected");
    }

    /// # Scenario
    /// Attempt to insert a value with an empty byte vector.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Call `put(b"key", vec![])` with an empty value.
    ///
    /// # Expected behavior
    /// The engine returns an error — empty values are rejected at the API level.
    #[test]
    fn memtable__empty_value_is_rejected() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        let result = engine.put(b"key".to_vec(), vec![]);
        assert!(result.is_err(), "empty value should be rejected");
    }

    /// # Scenario
    /// Attempt to insert both an empty key and an empty value.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Call `put(vec![], vec![])` with both key and value empty.
    ///
    /// # Expected behavior
    /// The engine returns an error — at least the empty key triggers rejection.
    #[test]
    fn memtable__empty_key_and_value_rejected() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        let result = engine.put(vec![], vec![]);
        assert!(result.is_err(), "empty key+value should be rejected");
    }

    // ================================================================
    // Scan boundary edge cases
    // ================================================================

    /// # Scenario
    /// Scan where start key equals end key (zero-width range).
    ///
    /// # Starting environment
    /// Engine with one key `"aaa"` inserted.
    ///
    /// # Actions
    /// 1. Scan with `start = "aaa"` and `end = "aaa"`.
    ///
    /// # Expected behavior
    /// Returns an empty result — a zero-width range `[x, x)` contains no keys
    /// because the end key is exclusive.
    #[test]
    fn memtable__scan_start_equals_end_empty() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();
        engine.put(b"aaa".to_vec(), b"v".to_vec()).unwrap();

        let results = collect_scan(&engine, b"aaa", b"aaa");
        assert!(results.is_empty(), "scan(x, x) should return nothing");
    }

    /// # Scenario
    /// Scan where start key is greater than end key (inverted range).
    ///
    /// # Starting environment
    /// Engine with two keys `"aaa"` and `"zzz"`.
    ///
    /// # Actions
    /// 1. Scan with `start = "zzz"` and `end = "aaa"` (start > end).
    ///
    /// # Expected behavior
    /// Returns an empty result and does not panic — inverted ranges are
    /// treated as empty.
    #[test]
    fn memtable__scan_start_gt_end_empty() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();
        engine.put(b"aaa".to_vec(), b"v1".to_vec()).unwrap();
        engine.put(b"zzz".to_vec(), b"v2".to_vec()).unwrap();

        let results = collect_scan(&engine, b"zzz", b"aaa");
        assert!(
            results.is_empty(),
            "scan(high, low) should return nothing (or at least not panic)"
        );
    }

    /// # Scenario
    /// Verify the exact boundary semantics of scan: start-inclusive, end-exclusive.
    ///
    /// # Starting environment
    /// Engine with 10 two-byte keys `[b'k', 0]` through `[b'k', 9]`.
    ///
    /// # Actions
    /// 1. Scan with `start = [k, 3]` and `end = [k, 7]`.
    ///
    /// # Expected behavior
    /// Result includes keys `[k,3]`, `[k,4]`, `[k,5]`, `[k,6]` but NOT `[k,7]`
    /// (end is exclusive) and NOT `[k,2]` (before start).
    #[test]
    fn memtable__scan_exact_boundary_inclusivity() {
        // Verify: start key is INCLUSIVE, end key is EXCLUSIVE.
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();
        for i in 0..10u8 {
            engine
                .put(vec![b'k', i], format!("v{}", i).into_bytes())
                .unwrap();
        }

        // scan([k, 3], [k, 7]) should include k3, k4, k5, k6 but NOT k7
        let results = collect_scan(&engine, &[b'k', 3], &[b'k', 7]);
        let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();

        assert!(keys.contains(&vec![b'k', 3]), "start key must be inclusive");
        assert!(
            keys.contains(&vec![b'k', 6]),
            "key just before end must be included"
        );
        assert!(!keys.contains(&vec![b'k', 7]), "end key must be exclusive");
        assert!(
            !keys.contains(&vec![b'k', 2]),
            "key before start must be excluded"
        );
    }

    /// # Scenario
    /// Full-keyspace scan using the widest possible byte range.
    ///
    /// # Starting environment
    /// Engine with 20 keys (`key_0000`..`key_0019`).
    ///
    /// # Actions
    /// 1. Scan with `start = "\x00"` and `end = "\xff"`.
    ///
    /// # Expected behavior
    /// All 20 keys are returned — the scan range covers the entire keyspace.
    #[test]
    fn memtable__scan_full_keyspace() {
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

        let results = collect_scan(&engine, b"\x00", b"\xff");
        assert_eq!(
            results.len(),
            20,
            "full keyspace scan should return all keys"
        );
    }

    // ================================================================
    // Stats correctness
    // ================================================================

    /// # Scenario
    /// Track the `frozen_count` and `sstables_count` stat counters as
    /// writes transition data through the freeze/flush lifecycle.
    ///
    /// # Starting environment
    /// Engine with small buffer config (128 bytes) — no data yet.
    /// Initial stats: frozen_count = 0, sstables_count = 0.
    ///
    /// # Actions
    /// 1. Write keys in a loop until `frozen_count > 0` (a memtable was frozen).
    /// 2. Write one more key (`"trigger"`) to flush the frozen memtable.
    /// 3. Check stats again.
    ///
    /// # Expected behavior
    /// After the trigger put, `sstables_count > 0` — the frozen memtable has
    /// been flushed to an SSTable.
    #[test]
    fn memtable_sstable__stats_frozen_count_transitions() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), small_buffer_config()).unwrap();

        // Initially: no frozen, no SSTables
        let s = engine.stats().unwrap();
        assert_eq!(s.frozen_count, 0);
        assert_eq!(s.sstables_count, 0);

        // Write until we get a frozen memtable
        let mut i = 0u32;
        loop {
            engine
                .put(
                    format!("k_{:04}", i).into_bytes(),
                    format!("v_{:04}", i).into_bytes(),
                )
                .unwrap();
            let s = engine.stats().unwrap();
            if s.frozen_count > 0 {
                // Frozen memtable exists; may or may not have SSTables yet
                break;
            }
            i += 1;
        }

        let before_frozen = engine.stats().unwrap().frozen_count;
        assert!(before_frozen >= 1, "Should have at least 1 frozen");

        // Explicitly flush frozen memtables → SSTables
        engine.flush_all_frozen().unwrap();
        let after = engine.stats().unwrap();
        // The frozen that existed should now be an SSTable
        assert!(
            after.sstables_count > 0,
            "Frozen should have been flushed to SSTable"
        );
    }

    // ================================================================
    // Close semantics
    // ================================================================

    /// # Scenario
    /// Verify that the engine remains usable after `close()` is called.
    ///
    /// # Starting environment
    /// Engine with 4 KB buffer; one key `"k"` = `"v"` inserted.
    ///
    /// # Actions
    /// 1. Call `close()` (flushes frozen memtables and checkpoints).
    /// 2. Perform a `get("k")` after close.
    /// 3. Perform a `put("k2", "v2")` and `get("k2")` after close.
    /// 4. Perform a scan after close.
    ///
    /// # Expected behavior
    /// All operations succeed — `close()` checkpoints state but the engine
    /// struct remains usable for subsequent reads and writes.
    #[test]
    fn memtable_sstable__operations_after_close_work() {
        // close() flushes frozen and checkpoints, but the engine struct
        // remains usable. Verify reads still work.
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.close().unwrap();

        // Reads after close should still see data
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            Some(b"v".to_vec()),
            "get after close should still return data"
        );

        // Writes after close
        engine.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
        assert_eq!(
            engine.get(b"k2".to_vec()).unwrap(),
            Some(b"v2".to_vec()),
            "put after close should succeed"
        );

        // Scan after close
        let results = collect_scan(&engine, b"k", b"k\xff");
        assert!(
            !results.is_empty(),
            "scan after close should return results"
        );
    }

    /// # Scenario
    /// Calling `close()` multiple times in a row does not panic or corrupt data.
    ///
    /// # Starting environment
    /// Engine with one key inserted.
    ///
    /// # Actions
    /// 1. Call `close()` (first close — flushes and checkpoints).
    /// 2. Call `close()` again (second close — should be a no-op).
    /// 3. Reopen the engine and get the key.
    ///
    /// # Expected behavior
    /// No error or panic on the second close, and the data is intact after reopen.
    #[test]
    fn memtable_sstable__multiple_close_calls_safe() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();

        engine.close().unwrap();
        engine.close().unwrap(); // Second close should not panic or error

        let engine = reopen(dir.path());
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v".to_vec()),);
    }

    /// # Scenario
    /// Open → write → close → reopen → close (no writes) → reopen → verify.
    ///
    /// # Starting environment
    /// Temporary directory with no prior database files.
    ///
    /// # Actions
    /// 1. Open engine, put `"k"` = `"v"`, close.
    /// 2. Reopen engine, close immediately (no new writes).
    /// 3. Reopen engine, get `"k"`.
    ///
    /// # Expected behavior
    /// `get("k")` returns `Some("v")` — a close-with-no-writes cycle must not
    /// lose previously persisted data.
    #[test]
    fn memtable_sstable__close_no_writes_reopen() {
        // open → write → close → reopen → close (no writes) → reopen
        let dir = TempDir::new().unwrap();

        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.close().unwrap();

        let engine = reopen(dir.path());
        engine.close().unwrap(); // No new writes, just close

        let engine = reopen(dir.path());
        assert_eq!(
            engine.get(b"k".to_vec()).unwrap(),
            Some(b"v".to_vec()),
            "data must survive close-with-no-writes cycle"
        );
    }

    // ================================================================
    // Reopen after only deletes
    // ================================================================

    /// # Scenario
    /// A session that performs only deletes (no puts) followed by a reopen.
    ///
    /// # Starting environment
    /// Session 1: three keys `"a"`, `"b"`, `"c"` inserted and engine closed.
    ///
    /// # Actions
    /// 1. Session 2: reopen, delete `"b"` (point), range-delete `["c", "d")`, close.
    /// 2. Session 3: reopen, get `"a"`, `"b"`, `"c"`.
    ///
    /// # Expected behavior
    /// `"a"` survives, `"b"` is `None` (point-deleted), `"c"` is `None`
    /// (range-deleted). Delete-only sessions are correctly persisted.
    #[test]
    fn memtable_sstable__reopen_after_only_deletes() {
        let dir = TempDir::new().unwrap();

        // Session 1: put some keys
        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        engine.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        engine.close().unwrap();

        // Session 2: only deletes, no puts
        let engine = reopen(dir.path());
        engine.delete(b"b".to_vec()).unwrap();
        engine.delete_range(b"c".to_vec(), b"d".to_vec()).unwrap();
        engine.close().unwrap();

        // Session 3: verify
        let engine = reopen(dir.path());
        assert_eq!(
            engine.get(b"a".to_vec()).unwrap(),
            Some(b"1".to_vec()),
            "a should survive"
        );
        assert_eq!(engine.get(b"b".to_vec()).unwrap(), None, "b was deleted");
        assert_eq!(
            engine.get(b"c".to_vec()).unwrap(),
            None,
            "c was range-deleted"
        );
    }

    // ================================================================
    // Very large keys
    // ================================================================

    /// # Scenario
    /// Store and recover an 8 KB key through SSTable flush and engine reopen.
    ///
    /// # Starting environment
    /// Engine with 16 KB write buffer (custom config so the 8 KB key fits in
    /// one memtable).
    ///
    /// # Actions
    /// 1. Put an 8192-byte key (`0xAB` repeated) with a small value.
    /// 2. Write 600 padding keys to exceed the 16 KB buffer and force SSTable
    ///    flush.
    /// 3. Verify the large key is readable before close.
    /// 4. Close and reopen the engine.
    /// 5. Get the large key again.
    ///
    /// # Expected behavior
    /// The 8 KB key is correctly stored in the SSTable and survives reopen —
    /// the engine handles very large keys without truncation or corruption.
    #[test]
    fn memtable_sstable__very_large_key_recovery() {
        let dir = TempDir::new().unwrap();

        let big_key = vec![0xAB; 8192]; // 8 KB key
        let value = b"big_key_value".to_vec();

        // Use a 16 KB buffer so the single 8 KB key fits in one memtable
        let config = EngineConfig {
            write_buffer_size: 16 * 1024,
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
        engine.put(big_key.clone(), value.clone()).unwrap();
        // Write enough padding to exceed the 16 KB buffer → SSTable flush
        for i in 0..600u32 {
            engine
                .put(
                    format!("pad_{:04}", i).into_bytes(),
                    format!("padding_value_with_extra_bytes_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();
        let stats = engine.stats().unwrap();
        assert!(stats.sstables_count > 0, "Expected SSTables");
        assert_eq!(
            engine.get(big_key.clone()).unwrap(),
            Some(value.clone()),
            "large key readable before close"
        );
        engine.close().unwrap();

        // Verify after reopen
        let engine = reopen(dir.path());
        assert_eq!(
            engine.get(big_key).unwrap(),
            Some(value),
            "8 KB key must survive SSTable flush + reopen"
        );
    }

    // ================================================================
    // Binary keys (0x00/0xFF) through SSTable and recovery
    // ================================================================

    /// # Scenario
    /// Store and recover keys composed entirely of `0x00`, `0xFF`, and mixed
    /// binary bytes through SSTable flush and reopen.
    ///
    /// # Starting environment
    /// Engine with 4 KB buffer.
    ///
    /// # Actions
    /// 1. Put three binary keys: 32×`0x00`, 32×`0xFF`, and a mixed
    ///    `[0x00, 0xFF, 0x00, 0xFF, 0x01, 0xFE]` sequence.
    /// 2. Write 200 padding keys to force SSTable flush.
    /// 3. Verify all three keys before close.
    /// 4. Close and reopen the engine.
    /// 5. Get all three binary keys.
    ///
    /// # Expected behavior
    /// All binary keys are correctly stored, flushed to SSTable, and recovered
    /// after reopen — the engine correctly handles boundary byte values.
    #[test]
    fn memtable_sstable__binary_keys_recovery() {
        let dir = TempDir::new().unwrap();

        let key_zeros = vec![0x00; 32];
        let key_ones = vec![0xFF; 32];
        let key_mixed = vec![0x00, 0xFF, 0x00, 0xFF, 0x01, 0xFE];

        let engine = Engine::open(dir.path(), default_config()).unwrap();
        engine.put(key_zeros.clone(), b"zeros".to_vec()).unwrap();
        engine.put(key_ones.clone(), b"ones".to_vec()).unwrap();
        engine.put(key_mixed.clone(), b"mixed".to_vec()).unwrap();

        // Force SSTable flush
        for i in 0..200u32 {
            engine
                .put(
                    format!("pad_{:04}", i).into_bytes(),
                    format!("pval_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        engine.flush_all_frozen().unwrap();
        let stats = engine.stats().unwrap();
        assert!(stats.sstables_count > 0, "Expected SSTables");

        // Verify before close
        assert_eq!(
            engine.get(key_zeros.clone()).unwrap(),
            Some(b"zeros".to_vec())
        );
        assert_eq!(
            engine.get(key_ones.clone()).unwrap(),
            Some(b"ones".to_vec())
        );
        assert_eq!(
            engine.get(key_mixed.clone()).unwrap(),
            Some(b"mixed".to_vec())
        );
        engine.close().unwrap();

        // Verify after reopen
        let engine = reopen(dir.path());
        assert_eq!(
            engine.get(key_zeros).unwrap(),
            Some(b"zeros".to_vec()),
            "0x00 key must survive SSTable + reopen"
        );
        assert_eq!(
            engine.get(key_ones).unwrap(),
            Some(b"ones".to_vec()),
            "0xFF key must survive SSTable + reopen"
        );
        assert_eq!(
            engine.get(key_mixed).unwrap(),
            Some(b"mixed".to_vec()),
            "mixed binary key must survive SSTable + reopen"
        );
    }

    // ================================================================
    // Range-delete / delete on empty database
    // ================================================================

    /// # Scenario
    /// Issue a range delete on a completely empty database.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data whatsoever.
    ///
    /// # Actions
    /// 1. Call `delete_range("start", "end")` on the empty engine.
    /// 2. Get `"anything"` and scan the full keyspace.
    ///
    /// # Expected behavior
    /// No error or panic; get returns `None` and scan returns empty.
    /// Range-deleting on an empty database is a safe no-op.
    #[test]
    fn memtable__range_delete_on_empty_db() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        // Should not panic or error
        engine
            .delete_range(b"start".to_vec(), b"end".to_vec())
            .unwrap();
        assert_eq!(
            engine.get(b"anything".to_vec()).unwrap(),
            None,
            "empty DB returns None"
        );

        // Scan should be empty
        let results = collect_scan(&engine, b"\x00", b"\xff");
        assert!(results.is_empty(), "empty DB scan should return nothing");
    }

    /// # Scenario
    /// Issue a point delete on a completely empty database.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Delete `"nonexistent"` (never inserted).
    /// 2. Get `"nonexistent"`.
    ///
    /// # Expected behavior
    /// No error; get returns `None`. Point-deleting on an empty database is safe.
    #[test]
    fn memtable__delete_on_empty_db() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        engine.delete(b"nonexistent".to_vec()).unwrap();
        assert_eq!(engine.get(b"nonexistent".to_vec()).unwrap(), None);
    }

    // ================================================================
    // Scan where all keys in range are deleted
    // ================================================================

    /// # Scenario
    /// Scan a range where every key has been deleted via range-delete.
    ///
    /// # Starting environment
    /// Engine with 10 keys (`key_0000`..`key_0009`) inserted.
    ///
    /// # Actions
    /// 1. Range-delete `["key_0000", "key_9999")` — covers all 10 keys.
    /// 2. Scan `["key_", "key_\xff")`.
    ///
    /// # Expected behavior
    /// Scan returns an empty result — all keys in the scan range have been
    /// range-deleted, and tombstones correctly suppress them.
    #[test]
    fn memtable__scan_all_keys_deleted() {
        let dir = TempDir::new().unwrap();
        let engine = Engine::open(dir.path(), memtable_only_config()).unwrap();

        for i in 0..10u32 {
            engine
                .put(
                    format!("key_{:04}", i).into_bytes(),
                    format!("val_{:04}", i).into_bytes(),
                )
                .unwrap();
        }
        // Delete everything via range
        engine
            .delete_range(b"key_0000".to_vec(), b"key_9999".to_vec())
            .unwrap();

        let results = collect_scan(&engine, b"key_", b"key_\xff");
        assert!(
            results.is_empty(),
            "scan should return empty when all keys in range are deleted"
        );
    }
}
