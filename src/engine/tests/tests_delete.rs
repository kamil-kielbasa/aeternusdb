//! Point-delete correctness tests.
//!
//! ## Layer coverage
//! - `memtable__*`: memtable only (64 KB buffer, no flushes)
//! - `memtable_sstable__*`: memtable + SSTable (4 KB buffer, triggers flush)
//!
//! ## See also
//! - [`tests_range_delete`] — range-delete coverage
//! - [`tests_precedence`] — delete vs range-delete LSN ordering

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Memtable-only
    // ----------------------------------------------------------------

    #[test]
    fn memtable__delete_existing_key() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v".to_vec()));

        engine.delete(b"k".to_vec()).unwrap();
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), None);
    }

    #[test]
    fn memtable__delete_nonexistent_key_is_noop() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // Should not error
        engine.delete(b"ghost".to_vec()).unwrap();
        assert_eq!(engine.get(b"ghost".to_vec()).unwrap(), None);
    }

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

    #[test]
    fn memtable__put_then_delete_hides_key() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.delete(b"k".to_vec()).unwrap();
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), None);
    }

    #[test]
    fn memtable__double_delete() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        engine.delete(b"k".to_vec()).unwrap();
        engine.delete(b"k".to_vec()).unwrap(); // second delete
        assert_eq!(engine.get(b"k".to_vec()).unwrap(), None);
    }

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
