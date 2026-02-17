//! Layer-interaction tests: memtable ↔ frozen ↔ SSTable ordering and shadowing.
//!
//! The engine resolves every key by consulting layers in recency order:
//! **active memtable → frozen memtables → SSTables (newest first)**.
//! A write in a newer layer shadows any entry in an older layer, regardless
//! of the operation type (put, point-delete, range-delete).
//!
//! Coverage:
//! - Range delete in memtable hides SSTable value
//! - Newer SSTable shadows older SSTable
//! - Active memtable overrides frozen memtable
//! - Point delete in memtable hides SSTable value
//! - Range tombstone in newer SSTable masks older puts
//! - Put in active memtable resurrects a deleted key
//! - Mixed operations across multiple flushes
//! - Multiple SSTables created and readable
//! - Overwrite across multiple SSTables
//! - Delete across multiple SSTables
//! - Scan across multiple SSTables
//! - Range delete across multiple SSTables
//!
//! ## See also
//! - [`tests_precedence`]   — put / delete / range-delete precedence rules
//! - [`tests_multi_sstable`] — multi-SSTable merge correctness

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Range delete in memtable hides SSTable value
    // ----------------------------------------------------------------

    /// # Scenario
    /// A range-delete issued against the active memtable must hide keys
    /// that physically reside in an older SSTable.
    ///
    /// # Starting environment
    /// Engine with 200 keys (`key_0000`–`key_0199`) flushed to SSTables.
    ///
    /// # Actions
    /// 1. Confirm `key_0075` is readable.
    /// 2. `delete_range("key_0070", "key_0080")` in the active memtable.
    ///
    /// # Expected behavior
    /// Keys 70–79 return `None`; keys 69 and 80 remain visible.
    #[test]
    fn memtable_sstable__range_delete_hides_sstable_value() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "key");

        // Confirm the key exists in SSTable
        assert!(engine.get(b"key_0075".to_vec()).unwrap().is_some());

        // Range delete in active memtable
        engine
            .delete_range(b"key_0070".to_vec(), b"key_0080".to_vec())
            .unwrap();

        // Keys in range are hidden
        for i in 70..80 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{:04} should be hidden",
                i
            );
        }

        // Keys outside range still visible
        assert!(engine.get(b"key_0069".to_vec()).unwrap().is_some());
        assert!(engine.get(b"key_0080".to_vec()).unwrap().is_some());
    }

    // ----------------------------------------------------------------
    // Newer SSTable shadows older SSTable
    // ----------------------------------------------------------------

    /// # Scenario
    /// When the same key exists in two SSTables, the value from the newer
    /// SSTable must win.
    ///
    /// # Starting environment
    /// Empty engine.
    ///
    /// # Actions
    /// 1. Insert 150 keys with `old_*` values → flushed to SSTables.
    /// 2. Overwrite keys 0–79 with `new_*` values → more SSTables created.
    ///
    /// # Expected behavior
    /// Keys 0–79 return `new_*` values; keys 80–149 return `old_*` values.
    #[test]
    fn memtable_sstable__newer_sstable_shadows_older() {
        let tmp = TempDir::new().unwrap();

        // First batch → creates SSTables with "old" values
        let engine = Engine::open(tmp.path(), default_config()).unwrap();
        for i in 0..150 {
            let key = format!("k_{:04}", i).into_bytes();
            let val = format!("old_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();
        let s1 = engine.stats().unwrap().sstables_count;
        assert!(s1 > 0, "First batch should create SSTables");

        // Overwrite a subset → new SSTables created
        for i in 0..80 {
            let key = format!("k_{:04}", i).into_bytes();
            let val = format!("new_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();
        let s2 = engine.stats().unwrap().sstables_count;
        assert!(s2 > s1, "Overwrites should create more SSTables");

        // Verify newest values win
        for i in 0..80 {
            let key = format!("k_{:04}", i).into_bytes();
            let expected = format!("new_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "k_{:04} should have new value",
                i
            );
        }
        for i in 80..150 {
            let key = format!("k_{:04}", i).into_bytes();
            let expected = format!("old_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "k_{:04} should have old value",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Active memtable overrides frozen memtable
    // ----------------------------------------------------------------

    /// # Scenario
    /// A write in the active memtable must shadow a value sitting in a
    /// frozen (pending-flush) memtable.
    ///
    /// # Starting environment
    /// Engine with a small write-buffer (triggers frequent freezes).
    ///
    /// # Actions
    /// 1. Write 100 keys → some land in frozen memtables.
    /// 2. Overwrite keys 0–9 with `override_*` values in the active memtable.
    ///
    /// # Expected behavior
    /// Keys 0–9 return `override_*` values.
    #[test]
    fn memtable_sstable__active_memtable_overrides_frozen() {
        let tmp = TempDir::new().unwrap();
        // Use small buffer so writes fill the memtable quickly → freeze
        let engine = Engine::open(tmp.path(), small_buffer_config()).unwrap();

        // Write enough keys to trigger at least one freeze+flush cycle
        for i in 0..100 {
            let key = format!("fz_{:04}", i).into_bytes();
            let val = format!("first_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        // Now overwrite a few in the (presumably new) active memtable
        for i in 0..10 {
            let key = format!("fz_{:04}", i).into_bytes();
            let val = format!("override_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        // The overwritten keys should return the latest values
        for i in 0..10 {
            let key = format!("fz_{:04}", i).into_bytes();
            let expected = format!("override_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "fz_{:04} should have override value",
                i
            );
        }
    }

    // ----------------------------------------------------------------
    // Delete in newer layer hides put in older SSTable
    // ----------------------------------------------------------------

    /// # Scenario
    /// A point-delete in the active memtable hides a put that lives in an
    /// older SSTable.
    ///
    /// # Starting environment
    /// Engine with 200 keys flushed to SSTables.
    ///
    /// # Actions
    /// 1. Confirm `key_0010` is readable.
    /// 2. `delete("key_0010")` in the active memtable.
    ///
    /// # Expected behavior
    /// `get("key_0010")` returns `None`.
    #[test]
    fn memtable_sstable__delete_hides_older_sstable() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "key");

        // SSTable has key_0010
        assert!(engine.get(b"key_0010".to_vec()).unwrap().is_some());

        // Point delete in active memtable
        engine.delete(b"key_0010".to_vec()).unwrap();
        assert_eq!(engine.get(b"key_0010".to_vec()).unwrap(), None);
    }

    // ----------------------------------------------------------------
    // Range tombstone in newer SSTable masks point puts in older SSTable
    // ----------------------------------------------------------------

    /// # Scenario
    /// A range-delete that is flushed (or still in memtable) must mask
    /// point puts residing in older SSTables.
    ///
    /// # Starting environment
    /// Engine with 150 keys flushed to SSTables.
    ///
    /// # Actions
    /// 1. `delete_range("rk_0020", "rk_0040")`.
    ///
    /// # Expected behavior
    /// Keys 20–39 return `None`; keys 19 and 40 remain visible.
    #[test]
    fn memtable_sstable__range_masks_older_puts() {
        let tmp = TempDir::new().unwrap();

        // Populate first batch
        let engine = Engine::open(tmp.path(), default_config()).unwrap();
        for i in 0..150 {
            let key = format!("rk_{:04}", i).into_bytes();
            let val = format!("val_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();
        let count_before = engine.stats().unwrap().sstables_count;
        assert!(count_before > 0);

        // Issue a range delete — this will go into memtable (maybe flushed later)
        engine
            .delete_range(b"rk_0020".to_vec(), b"rk_0040".to_vec())
            .unwrap();

        // Verify: range-deleted keys are gone
        for i in 20..40 {
            let key = format!("rk_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "rk_{:04} should be range-deleted",
                i
            );
        }

        // Keys outside range unaffected
        assert!(engine.get(b"rk_0019".to_vec()).unwrap().is_some());
        assert!(engine.get(b"rk_0040".to_vec()).unwrap().is_some());
    }

    // ----------------------------------------------------------------
    // Put in active memtable resurrects key deleted in SSTable
    // ----------------------------------------------------------------

    /// # Scenario
    /// A put after a delete for the same key must make the key visible again.
    ///
    /// # Starting environment
    /// Engine with 150 keys flushed to SSTables.
    ///
    /// # Actions
    /// 1. `delete("x_0042")` → key is gone.
    /// 2. `put("x_0042", "resurrected")` → key comes back.
    ///
    /// # Expected behavior
    /// `get("x_0042")` returns `Some("resurrected")`.
    #[test]
    fn memtable_sstable__put_resurrects_deleted_key() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), default_config()).unwrap();

        // Populate → flush
        for i in 0..150 {
            let key = format!("x_{:04}", i).into_bytes();
            let val = format!("v_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();
        assert!(engine.stats().unwrap().sstables_count > 0);

        // Delete from active memtable
        engine.delete(b"x_0042".to_vec()).unwrap();
        assert_eq!(engine.get(b"x_0042".to_vec()).unwrap(), None);

        // Re-insert the same key
        engine
            .put(b"x_0042".to_vec(), b"resurrected".to_vec())
            .unwrap();
        assert_eq!(
            engine.get(b"x_0042".to_vec()).unwrap(),
            Some(b"resurrected".to_vec())
        );
    }

    // ----------------------------------------------------------------
    // Mixed operations across multiple flushes
    // ----------------------------------------------------------------

    /// # Scenario
    /// A complex sequence of puts, point-deletes, range-deletes, and
    /// re-inserts across multiple flush cycles must resolve correctly.
    ///
    /// # Starting environment
    /// Empty engine.
    ///
    /// # Actions
    /// 1. Insert 200 keys (phase 1 values) → SSTables.
    /// 2. Point-delete all even keys.
    /// 3. `delete_range("m_0150", "m_0180")`.
    /// 4. Re-insert `m_0010` and `m_0160` with new values.
    ///
    /// # Expected behavior
    /// - `m_0010` → `"revived"` (even-deleted then re-inserted).
    /// - `m_0011` → phase 1 value (odd, untouched).
    /// - `m_0050` → `None` (even, deleted).
    /// - `m_0155` → `None` (odd but inside range-delete).
    /// - `m_0160` → `"revived_range"` (even + range-deleted + re-inserted).
    /// - `m_0185` → phase 1 value (odd, outside range).
    #[test]
    fn memtable_sstable__mixed_ops_across_flushes() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), default_config()).unwrap();

        // Phase 1: bulk insert → SSTables
        for i in 0..200 {
            let key = format!("m_{:04}", i).into_bytes();
            let val = format!("p1_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }
        engine.flush_all_frozen().unwrap();
        assert!(engine.stats().unwrap().sstables_count > 0);

        // Phase 2: delete even keys
        for i in (0..200).step_by(2) {
            let key = format!("m_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }

        // Phase 3: range delete [150, 180)
        engine
            .delete_range(b"m_0150".to_vec(), b"m_0180".to_vec())
            .unwrap();

        // Phase 4: re-insert a few keys
        engine.put(b"m_0010".to_vec(), b"revived".to_vec()).unwrap();
        engine
            .put(b"m_0160".to_vec(), b"revived_range".to_vec())
            .unwrap();

        // Verify
        // m_0010 was even (deleted), then re-inserted → should be "revived"
        assert_eq!(
            engine.get(b"m_0010".to_vec()).unwrap(),
            Some(b"revived".to_vec())
        );

        // m_0011 is odd, not deleted → should have phase 1 value
        assert_eq!(
            engine.get(b"m_0011".to_vec()).unwrap(),
            Some(b"p1_0011".to_vec())
        );

        // m_0050 is even, deleted → None
        assert_eq!(engine.get(b"m_0050".to_vec()).unwrap(), None);

        // m_0155 is odd but in range [150,180) → None
        assert_eq!(engine.get(b"m_0155".to_vec()).unwrap(), None);

        // m_0160 is even AND in range, but re-inserted → "revived_range"
        assert_eq!(
            engine.get(b"m_0160".to_vec()).unwrap(),
            Some(b"revived_range".to_vec())
        );

        // m_0185 is odd and outside range → should have phase 1 value
        assert_eq!(
            engine.get(b"m_0185".to_vec()).unwrap(),
            Some(b"p1_0185".to_vec())
        );
    }

    // ----------------------------------------------------------------
    // Multiple SSTables: verify count and reads merge correctly
    // ----------------------------------------------------------------

    /// # Scenario
    /// With a small write-buffer, many puts create multiple SSTables;
    /// reads must merge across all of them.
    ///
    /// # Starting environment
    /// Engine with 128-byte write-buffer.
    ///
    /// # Actions
    /// 1. Insert 50 keys with padding values.
    ///
    /// # Expected behavior
    /// At least 2 SSTables are created; all 50 keys are readable with
    /// correct values.
    #[test]
    fn multiple_sstables_created_and_readable() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), small_buffer_config()).unwrap();

        // Write enough data in separate batches to create multiple SSTables.
        // With 128-byte buffer, each put triggers freeze+flush quickly.
        for i in 0..50 {
            let key = format!("ms_{:04}", i).into_bytes();
            let val = format!("value_with_padding_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        engine.flush_all_frozen().unwrap();
        let stats = engine.stats().unwrap();
        assert!(
            stats.sstables_count >= 2,
            "Expected at least 2 SSTables, got {}",
            stats.sstables_count
        );

        // All keys should be readable across multiple SSTables
        for i in 0..50 {
            let key = format!("ms_{:04}", i).into_bytes();
            let expected = format!("value_with_padding_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "ms_{:04} should be readable across {} SSTables",
                i,
                stats.sstables_count
            );
        }
    }

    /// # Scenario
    /// Overwriting the same keys in a second round creates additional
    /// SSTables; the newest values must win on read.
    ///
    /// # Starting environment
    /// Engine with 128-byte write-buffer.
    ///
    /// # Actions
    /// 1. Insert 30 keys (`round1_*`) → multiple SSTables.
    /// 2. Overwrite the same 30 keys (`round2_*`) → more SSTables.
    ///
    /// # Expected behavior
    /// All 30 keys return `round2_*` values.
    #[test]
    fn overwrite_across_multiple_sstables() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), small_buffer_config()).unwrap();

        // Round 1: initial values → flushed to SSTables
        for i in 0..30 {
            let key = format!("om_{:04}", i).into_bytes();
            let val = format!("round1_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        engine.flush_all_frozen().unwrap();
        let s1 = engine.stats().unwrap().sstables_count;
        assert!(
            s1 >= 2,
            "Expected at least 2 SSTables after round 1, got {}",
            s1
        );

        // Round 2: overwrite same keys → creates more SSTables
        for i in 0..30 {
            let key = format!("om_{:04}", i).into_bytes();
            let val = format!("round2_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        engine.flush_all_frozen().unwrap();
        let s2 = engine.stats().unwrap().sstables_count;
        assert!(
            s2 > s1,
            "Expected more SSTables after round 2 ({} should be > {})",
            s2,
            s1
        );

        // Latest values should win
        for i in 0..30 {
            let key = format!("om_{:04}", i).into_bytes();
            let expected = format!("round2_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "om_{:04} should have round 2 value across {} SSTables",
                i,
                s2
            );
        }
    }

    /// # Scenario
    /// Point-deletes issued after data has been flushed to multiple
    /// SSTables must hide the correct keys.
    ///
    /// # Starting environment
    /// Engine with 128-byte write-buffer.
    ///
    /// # Actions
    /// 1. Insert 40 keys → multiple SSTables.
    /// 2. Point-delete keys 0–19.
    ///
    /// # Expected behavior
    /// Keys 0–19 return `None`; keys 20–39 remain readable.
    #[test]
    fn delete_across_multiple_sstables() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), small_buffer_config()).unwrap();

        // Populate → multiple SSTables
        for i in 0..40 {
            let key = format!("dm_{:04}", i).into_bytes();
            let val = format!("val_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        engine.flush_all_frozen().unwrap();
        let s1 = engine.stats().unwrap().sstables_count;
        assert!(s1 >= 2, "Expected at least 2 SSTables, got {}", s1);

        // Delete half the keys → tombstones land in newer SSTables
        for i in 0..20 {
            let key = format!("dm_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }

        // Deleted keys are gone
        for i in 0..20 {
            let key = format!("dm_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "dm_{:04} should be deleted",
                i
            );
        }

        // Surviving keys still readable
        for i in 20..40 {
            let key = format!("dm_{:04}", i).into_bytes();
            let expected = format!("val_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                Some(expected),
                "dm_{:04} should exist",
                i
            );
        }
    }

    /// # Scenario
    /// A full-range scan must merge records from multiple SSTables
    /// into a single sorted, deduplicated result set.
    ///
    /// # Starting environment
    /// Engine with 128-byte write-buffer.
    ///
    /// # Actions
    /// 1. Insert 50 keys → multiple SSTables.
    /// 2. `scan("sc_", "sc_\xff")`.
    ///
    /// # Expected behavior
    /// 50 key-value pairs in sorted order with correct values.
    #[test]
    fn scan_across_multiple_sstables() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), small_buffer_config()).unwrap();

        for i in 0..50 {
            let key = format!("sc_{:04}", i).into_bytes();
            let val = format!("val_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        engine.flush_all_frozen().unwrap();
        let stats = engine.stats().unwrap();
        assert!(
            stats.sstables_count >= 2,
            "Expected at least 2 SSTables for scan test, got {}",
            stats.sstables_count
        );

        let results = collect_scan(&engine, b"sc_", b"sc_\xff");
        assert_eq!(
            results.len(),
            50,
            "Scan should return all 50 keys across {} SSTables",
            stats.sstables_count
        );

        // Verify sorted order and correct values
        for (i, (k, v)) in results.iter().enumerate() {
            let expected_key = format!("sc_{:04}", i).into_bytes();
            let expected_val = format!("val_{:04}", i).into_bytes();
            assert_eq!(k, &expected_key);
            assert_eq!(v, &expected_val);
        }
    }

    /// # Scenario
    /// A range-delete that spans keys stored in different SSTables
    /// must hide all affected keys.
    ///
    /// # Starting environment
    /// Engine with 128-byte write-buffer.
    ///
    /// # Actions
    /// 1. Insert 40 keys → multiple SSTables.
    /// 2. `delete_range("rd_0010", "rd_0030")`.
    ///
    /// # Expected behavior
    /// Keys 0–9 and 30–39 survive; keys 10–29 return `None`.
    #[test]
    fn range_delete_across_multiple_sstables() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), small_buffer_config()).unwrap();

        for i in 0..40 {
            let key = format!("rd_{:04}", i).into_bytes();
            let val = format!("val_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        engine.flush_all_frozen().unwrap();
        let stats = engine.stats().unwrap();
        assert!(
            stats.sstables_count >= 2,
            "Need multiple SSTables, got {}",
            stats.sstables_count
        );

        // Range delete across SSTable boundaries
        engine
            .delete_range(b"rd_0010".to_vec(), b"rd_0030".to_vec())
            .unwrap();

        // Keys 0-9 survive
        for i in 0..10 {
            let key = format!("rd_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "rd_{:04} should survive",
                i
            );
        }
        // Keys 10-29 deleted
        for i in 10..30 {
            let key = format!("rd_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "rd_{:04} should be deleted",
                i
            );
        }
        // Keys 30-39 survive
        for i in 30..40 {
            let key = format!("rd_{:04}", i).into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "rd_{:04} should survive",
                i
            );
        }
    }
}
