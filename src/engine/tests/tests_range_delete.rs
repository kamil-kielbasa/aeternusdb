//! Range-delete correctness tests.
//!
//! This module exhaustively tests the `delete_range(start, end)` operation
//! for boundary correctness, composability, and cross-layer visibility.
//! A range-delete creates a tombstone `[start, end)` (start-inclusive,
//! end-exclusive) that hides every key `k` where `start <= k < end`.
//!
//! Test groups:
//! - **Single-key range** — `[k, k+1)` acts like a point delete.
//! - **Partial range** — only a subset of existing keys is deleted.
//! - **Empty / reversed range** — rejected with an error; nothing is deleted.
//! - **Overlapping ranges** — the union of intervals is deleted.
//! - **Nested ranges** — inner range is redundant, outer governs.
//! - **Adjacent ranges** — touching but non-overlapping intervals.
//! - **Range beyond existing keys** — no error; keys within range are deleted.
//! - **Delete-all** — `[\x00, \xff)` wipes every key.
//! - **Cross-layer** — a memtable range-delete hides SSTable keys.
//!
//! Local helpers: `populate(n)` inserts `key_00`..`key_{n-1}` with
//! corresponding `val_*` values; `assert_exists(i)` / `assert_deleted(i)`
//! assert presence or absence of `key_{i}`.
//!
//! ## Layer coverage
//! - `memtable__*`: memtable only (64 KB buffer)
//! - `memtable_sstable__*`: range tombstones hiding SSTable keys
//!
//! ## See also
//! - [`tests_precedence`] — range vs point delete/put LSN ordering
//! - [`tests_layers`] `range_*` — range tombstone interaction across layers
//! - [`tests_lsn_continuity`] — range tombstone ordering after reopen

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    /// Populate keys `key_00` .. `key_{n-1}` with corresponding values.
    fn populate(engine: &Engine, n: usize) {
        for i in 0..n {
            let key = format!("key_{:02}", i).into_bytes();
            let val = format!("val_{:02}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
    }

    fn assert_exists(engine: &Engine, i: usize) {
        let key = format!("key_{:02}", i).into_bytes();
        let expected = format!("val_{:02}", i).into_bytes();
        assert_eq!(
            engine.get(key).unwrap(),
            Some(expected),
            "key_{:02} should exist",
            i
        );
    }

    fn assert_deleted(engine: &Engine, i: usize) {
        let key = format!("key_{:02}", i).into_bytes();
        assert_eq!(
            engine.get(key).unwrap(),
            None,
            "key_{:02} should be deleted",
            i
        );
    }

    // ----------------------------------------------------------------
    // Single-key range [k, k+1) — equivalent to point delete
    // ----------------------------------------------------------------

    /// # Scenario
    /// A range `[key_05, key_06)` deletes exactly one key, behaving
    /// like a point delete.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config; keys `key_00`–`key_09`
    /// inserted via `populate(10)`.
    ///
    /// # Actions
    /// 1. `delete_range("key_05", "key_06")`.
    /// 2. Get each key 0–9.
    ///
    /// # Expected behavior
    /// Only `key_05` returns `None`; all other keys remain present.
    #[test]
    fn memtable__range_delete_single_key() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
        populate(&engine, 10);

        // Delete exactly key_05 using range [key_05, key_06)
        engine
            .delete_range(b"key_05".to_vec(), b"key_06".to_vec())
            .unwrap();

        for i in 0..10 {
            if i == 5 {
                assert_deleted(&engine, i);
            } else {
                assert_exists(&engine, i);
            }
        }
    }

    // ----------------------------------------------------------------
    // Partial range
    // ----------------------------------------------------------------

    /// # Scenario
    /// A partial range-delete removes a subset of keys.
    ///
    /// # Starting environment
    /// Fresh engine; 10 keys inserted.
    ///
    /// # Actions
    /// 1. `delete_range("key_03", "key_07")` — deletes keys 3, 4, 5, 6.
    /// 2. Get each key 0–9.
    ///
    /// # Expected behavior
    /// Keys 0–2 and 7–9 present; keys 3–6 return `None`.
    #[test]
    fn memtable__range_delete_partial() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
        populate(&engine, 10);

        // Delete [key_03, key_07) — keys 3,4,5,6 deleted
        engine
            .delete_range(b"key_03".to_vec(), b"key_07".to_vec())
            .unwrap();

        for i in 0..3 {
            assert_exists(&engine, i);
        }
        for i in 3..7 {
            assert_deleted(&engine, i);
        }
        for i in 7..10 {
            assert_exists(&engine, i);
        }
    }

    // ----------------------------------------------------------------
    // Empty range (start >= end)
    // ----------------------------------------------------------------

    /// # Scenario
    /// Range-deletes where `start == end` or `start > end` are rejected
    /// with an error — invalid ranges are not silently accepted.
    ///
    /// # Starting environment
    /// Fresh engine; 5 keys inserted.
    ///
    /// # Actions
    /// 1. `delete_range("key_02", "key_02")` — empty (start == end).
    /// 2. `delete_range("key_04", "key_01")` — reversed (start > end).
    /// 3. Get each key 0–4.
    ///
    /// # Expected behavior
    /// Both calls return an error. All 5 keys remain present.
    #[test]
    fn memtable__range_delete_empty_range_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
        populate(&engine, 5);

        // Empty range: start == end → error
        assert!(
            engine
                .delete_range(b"key_02".to_vec(), b"key_02".to_vec())
                .is_err(),
            "start == end should be rejected"
        );

        // Reversed range: start > end → error
        assert!(
            engine
                .delete_range(b"key_04".to_vec(), b"key_01".to_vec())
                .is_err(),
            "start > end should be rejected"
        );

        // Everything still exists
        for i in 0..5 {
            assert_exists(&engine, i);
        }
    }

    // ----------------------------------------------------------------
    // Overlapping ranges
    // ----------------------------------------------------------------

    /// # Scenario
    /// Two overlapping range-deletes produce the union of their intervals.
    ///
    /// # Starting environment
    /// Fresh engine; 20 keys inserted.
    ///
    /// # Actions
    /// 1. `delete_range("key_03", "key_10")`.
    /// 2. `delete_range("key_07", "key_15")`.
    /// 3. Get each key 0–19.
    ///
    /// # Expected behavior
    /// Union `[key_03, key_15)` is deleted — keys 3–14 return `None`.
    /// Keys 0–2 and 15–19 remain present.
    #[test]
    fn memtable__range_delete_overlapping() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
        populate(&engine, 20);

        // Two overlapping ranges: [03,10) and [07,15)
        engine
            .delete_range(b"key_03".to_vec(), b"key_10".to_vec())
            .unwrap();
        engine
            .delete_range(b"key_07".to_vec(), b"key_15".to_vec())
            .unwrap();

        // Union is [03,15)
        for i in 0..3 {
            assert_exists(&engine, i);
        }
        for i in 3..15 {
            assert_deleted(&engine, i);
        }
        for i in 15..20 {
            assert_exists(&engine, i);
        }
    }

    // ----------------------------------------------------------------
    // Nested ranges
    // ----------------------------------------------------------------

    /// # Scenario
    /// A smaller (inner) range-delete inside a larger (outer) one is
    /// redundant — the outer range already covers it.
    ///
    /// # Starting environment
    /// Fresh engine; 20 keys inserted.
    ///
    /// # Actions
    /// 1. `delete_range("key_02", "key_18")` (outer).
    /// 2. `delete_range("key_05", "key_10")` (inner, redundant).
    /// 3. Get each key 0–19.
    ///
    /// # Expected behavior
    /// Keys 2–17 return `None` (outer interval); keys 0–1 and 18–19 present.
    #[test]
    fn memtable__range_delete_nested() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
        populate(&engine, 20);

        // Outer: [02,18)
        engine
            .delete_range(b"key_02".to_vec(), b"key_18".to_vec())
            .unwrap();
        // Inner (redundant): [05,10)
        engine
            .delete_range(b"key_05".to_vec(), b"key_10".to_vec())
            .unwrap();

        for i in 0..2 {
            assert_exists(&engine, i);
        }
        for i in 2..18 {
            assert_deleted(&engine, i);
        }
        for i in 18..20 {
            assert_exists(&engine, i);
        }
    }

    // ----------------------------------------------------------------
    // Adjacent ranges
    // ----------------------------------------------------------------

    /// # Scenario
    /// Two adjacent (touching, non-overlapping) range-deletes together
    /// cover a contiguous interval.
    ///
    /// # Starting environment
    /// Fresh engine; 20 keys inserted.
    ///
    /// # Actions
    /// 1. `delete_range("key_03", "key_07")`.
    /// 2. `delete_range("key_07", "key_12")`.
    /// 3. Get each key 0–19.
    ///
    /// # Expected behavior
    /// Keys 3–11 return `None`; keys 0–2 and 12–19 present.
    /// No gap at the boundary — `key_07` is deleted by the second range.
    #[test]
    fn memtable__range_delete_adjacent() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
        populate(&engine, 20);

        // [03,07) and [07,12) — adjacent, no gap, no overlap
        engine
            .delete_range(b"key_03".to_vec(), b"key_07".to_vec())
            .unwrap();
        engine
            .delete_range(b"key_07".to_vec(), b"key_12".to_vec())
            .unwrap();

        for i in 0..3 {
            assert_exists(&engine, i);
        }
        for i in 3..12 {
            assert_deleted(&engine, i);
        }
        for i in 12..20 {
            assert_exists(&engine, i);
        }
    }

    // ----------------------------------------------------------------
    // Range delete that covers keys beyond what exists
    // ----------------------------------------------------------------

    /// # Scenario
    /// A range-delete whose end extends past the last existing key.
    ///
    /// # Starting environment
    /// Fresh engine; 10 keys inserted (`key_00`–`key_09`).
    ///
    /// # Actions
    /// 1. `delete_range("key_05", "key_99")` — extends well past `key_09`.
    /// 2. Get each key 0–9.
    ///
    /// # Expected behavior
    /// Keys 0–4 present; keys 5–9 return `None`.
    /// No error for the non-existent portion of the range.
    #[test]
    fn memtable__range_delete_beyond_existing() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
        populate(&engine, 10);

        // Range extends beyond existing keys
        engine
            .delete_range(b"key_05".to_vec(), b"key_99".to_vec())
            .unwrap();

        for i in 0..5 {
            assert_exists(&engine, i);
        }
        for i in 5..10 {
            assert_deleted(&engine, i);
        }
    }

    /// # Scenario
    /// A full-keyspace range-delete `[\x00, \xff)` wipes every key.
    ///
    /// # Starting environment
    /// Fresh engine; 10 keys inserted.
    ///
    /// # Actions
    /// 1. `delete_range("\x00", "\xff")`.
    /// 2. Get each key 0–9.
    ///
    /// # Expected behavior
    /// All keys return `None`.
    #[test]
    fn memtable__range_delete_all_keys() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
        populate(&engine, 10);

        // Delete everything
        engine
            .delete_range(b"\x00".to_vec(), b"\xff".to_vec())
            .unwrap();

        for i in 0..10 {
            assert_deleted(&engine, i);
        }
    }

    // ----------------------------------------------------------------
    // Range delete with SSTables
    // ----------------------------------------------------------------

    /// # Scenario
    /// A range-delete in the active memtable hides keys that have already
    /// been flushed to an SSTable.
    ///
    /// # Starting environment
    /// Engine created via `engine_with_sstables(200, "key")` — 200 keys
    /// flushed to disk (small-buffer config triggers flushes during insert).
    ///
    /// # Actions
    /// 1. `delete_range("key_0050", "key_0100")` (in memtable).
    /// 2. Get each key 0–199.
    ///
    /// # Expected behavior
    /// Keys 50–99 return `None` (range-deleted); keys 0–49 and 100–199
    /// remain present. The memtable range tombstone correctly shadows
    /// the older SSTable point records.
    #[test]
    fn memtable_sstable__range_delete_hides_keys() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "key");

        // Range delete in memtable should hide SSTable keys
        engine
            .delete_range(b"key_0050".to_vec(), b"key_0100".to_vec())
            .unwrap();

        for i in 0..50 {
            let key = format!("key_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{:04} should exist",
                i
            );
        }
        for i in 50..100 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{:04} should be deleted",
                i
            );
        }
        for i in 100..200 {
            let key = format!("key_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{:04} should exist",
                i
            );
        }
    }
}
