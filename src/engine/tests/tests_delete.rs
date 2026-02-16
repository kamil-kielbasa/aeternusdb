//! Point-delete correctness tests.
//!
//! These tests verify that `delete()` correctly removes individual keys from the
//! database. The memtable-only group validates tombstone semantics in memory:
//! deleting a key inserts a tombstone that shadows the previous value, a second
//! delete of the same key is harmless, and a subsequent `put()` resurrects the
//! key. The memtable+SSTable group ensures that a tombstone written to the
//! active memtable properly shadows an older value stored on disk in an SSTable.
//!
//! ## Layer coverage
//! - `memtable__*`: memtable only (64 KB buffer — no flushes triggered)
//! - `memtable_sstable__*`: memtable + SSTable (4 KB buffer — forces flush to disk)
//!
//! ## See also
//! - [`tests_range_delete`] — range-delete coverage
//! - [`tests_precedence`] — delete vs range-delete LSN ordering
//! - [`tests_recovery`] — tombstone durability across close → reopen

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Memtable-only
    // ----------------------------------------------------------------

    /// # Scenario
    /// Delete an existing key and confirm it becomes invisible.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data on disk.
    ///
    /// # Actions
    /// 1. Put key `"k"` with value `"v"`.
    /// 2. Confirm the key is readable.
    /// 3. Delete key `"k"`.
    /// 4. Get key `"k"` again.
    ///
    /// # Expected behavior
    /// After deletion, `get("k")` returns `None` — the tombstone shadows the put.
    #[test]
    fn memtable__delete_existing_key() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v".to_vec()));

        engine.delete(b"k".to_vec()).unwrap();
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), None);
    }

    /// # Scenario
    /// Delete a key that was never inserted.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Delete key `"ghost"` (never inserted).
    /// 2. Get key `"ghost"`.
    ///
    /// # Expected behavior
    /// The delete does not error and `get("ghost")` returns `None`.
    /// Deleting a nonexistent key is a harmless no-op.
    #[test]
    fn memtable__delete_nonexistent_key_is_noop() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // Should not error
        engine.delete(b"ghost".to_vec()).unwrap();
        assert_eq!(engine.get(b"ghost".to_vec()).unwrap(), None);
    }

    /// # Scenario
    /// Delete a key and then re-insert it with a new value.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data on disk.
    ///
    /// # Actions
    /// 1. Put `"k"` = `"v1"`.
    /// 2. Delete `"k"` → verify it returns `None`.
    /// 3. Put `"k"` = `"v2"` (resurrect).
    /// 4. Get `"k"`.
    ///
    /// # Expected behavior
    /// `get("k")` returns `Some("v2")` — a put after a delete resurrects the
    /// key because the new put has a higher LSN than the tombstone.
    #[test]
    fn memtable__delete_then_put_resurrects_key() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
        engine.delete(b"k".to_vec()).unwrap();
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), None);

        engine.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v2".to_vec()));
    }

    /// # Scenario
    /// Put followed immediately by delete — key must become invisible.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Put `"k"` = `"v"`.
    /// 2. Delete `"k"`.
    /// 3. Get `"k"`.
    ///
    /// # Expected behavior
    /// `get("k")` returns `None` — the delete tombstone (higher LSN) hides
    /// the preceding put.
    #[test]
    fn memtable__put_then_delete_hides_key() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.delete(b"k".to_vec()).unwrap();
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), None);
    }

    /// # Scenario
    /// Deleting the same key twice does not cause errors.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Put `"k"` = `"v"`.
    /// 2. Delete `"k"` (first delete).
    /// 3. Delete `"k"` again (second delete — redundant).
    /// 4. Get `"k"`.
    ///
    /// # Expected behavior
    /// No error on the second delete, and `get("k")` returns `None`.
    /// Issuing multiple tombstones for the same key is idempotent.
    #[test]
    fn memtable__double_delete() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.delete(b"k".to_vec()).unwrap();
        engine.delete(b"k".to_vec()).unwrap(); // second delete
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), None);
    }

    /// # Scenario
    /// Selectively delete alternating (even-indexed) keys from a batch.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no data.
    ///
    /// # Actions
    /// 1. Put 20 keys (`key_00`..`key_19`).
    /// 2. Delete even-indexed keys (0, 2, 4, …, 18).
    /// 3. Get every key.
    ///
    /// # Expected behavior
    /// Even keys return `None`; odd keys return their original values.
    /// Point deletes are precise and do not affect neighboring keys.
    #[test]
    fn memtable__delete_alternating_keys() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        for i in 0..20 {
            let key = format!("key_{:02}", i).into_bytes();
            let val = format!("val_{:02}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        // Delete even keys
        for i in (0..20).step_by(2) {
            engine.delete(format!("key_{:02}", i).into_bytes()).unwrap();
        }

        for i in 0..20 {
            let key = format!("key_{:02}", i).into_bytes();
            let result = engine.get(key).unwrap();
            if i % 2 == 0 {
                assert_eq!(result, None, "key_{:02} should be deleted", i);
            } else {
                assert_eq!(
                    result,
                    Some(format!("val_{:02}", i).into_bytes()),
                    "key_{:02} should exist",
                    i
                );
            }
        }
    }

    // ----------------------------------------------------------------
    // With SSTables
    // ----------------------------------------------------------------

    /// # Scenario
    /// Delete a key whose value resides in an SSTable on disk.
    ///
    /// # Starting environment
    /// Engine with 200 keys already flushed to SSTables via `engine_with_sstables`.
    ///
    /// # Actions
    /// 1. Confirm `key_0050` is readable (it lives in an SSTable).
    /// 2. Delete `key_0050` from the active memtable.
    /// 3. Get `key_0050`.
    ///
    /// # Expected behavior
    /// `get("key_0050")` returns `None` — the memtable tombstone shadows
    /// the older SSTable entry.
    #[test]
    fn memtable_sstable__delete_key_in_sstable() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "key");

        // Key exists in SSTable
        assert!(engine.get(b"key_0050".to_vec()).unwrap().is_some());

        // Delete it from the active memtable — should shadow the SSTable entry
        engine.delete(b"key_0050".to_vec()).unwrap();
        assert_eq!(engine.get(b"key_0050".to_vec()).unwrap(), None);
    }

    /// # Scenario
    /// Delete an SSTable key, then re-insert it with a new value.
    ///
    /// # Starting environment
    /// Engine with 200 keys already flushed to SSTables.
    ///
    /// # Actions
    /// 1. Delete `key_0050` → verify it returns `None`.
    /// 2. Put `key_0050` = `"resurrected"`.
    /// 3. Get `key_0050`.
    ///
    /// # Expected behavior
    /// `get("key_0050")` returns `Some("resurrected")` — the new put (highest
    /// LSN) overrides both the SSTable value and the intermediate tombstone.
    #[test]
    fn memtable_sstable__delete_then_put_resurrects() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "key");

        engine.delete(b"key_0050".to_vec()).unwrap();
        assert_eq!(engine.get(b"key_0050".to_vec()).unwrap(), None);

        engine
            .put(b"key_0050".to_vec(), b"resurrected".to_vec())
            .unwrap();
        assert_eq!(
            engine.get(b"key_0050".to_vec()).unwrap(),
            Some(b"resurrected".to_vec())
        );
    }

    /// # Scenario
    /// Bulk-delete half the keys from a multi-SSTable dataset.
    ///
    /// # Starting environment
    /// Engine with 200 keys flushed to SSTables.
    ///
    /// # Actions
    /// 1. Delete the first 100 keys (`key_0000`..`key_0099`).
    /// 2. Get each of the 200 keys.
    ///
    /// # Expected behavior
    /// Keys 0..99 return `None` (deleted); keys 100..199 return their
    /// original values (untouched).
    #[test]
    fn memtable_sstable__delete_many_keys() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "key");

        // Delete first 100 keys
        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            engine.delete(key).unwrap();
        }

        // Deleted keys are gone
        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key).unwrap(),
                None,
                "key_{:04} should be deleted",
                i
            );
        }

        // Remaining keys still exist
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
