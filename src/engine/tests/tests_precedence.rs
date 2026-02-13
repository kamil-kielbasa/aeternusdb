//! Precedence tests: range vs point delete/put, LSN ordering.
//!
//! ## Layer coverage
//! - All tests use `memtable` only (64 KB buffer, no flushes)
//!
//! ## See also
//! - [`tests_layers`] — same precedence rules across memtable ↔ SSTable layers
//! - [`tests_lsn_continuity`] — precedence across reopen cycles

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Newer range tombstone beats older point put
    // ----------------------------------------------------------------

    #[test]
    fn memtable__newer_range_delete_beats_older_put() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // Put first (lower LSN)
        engine.put(b"key_05".to_vec(), b"v".to_vec()).unwrap();

        // Range delete second (higher LSN) — should shadow the put
        engine
            .delete_range(b"key_00".to_vec(), b"key_10".to_vec())
            .unwrap();

        assert_eq!(engine.get(b"key_05".to_vec()).unwrap(), None);
    }

    // ----------------------------------------------------------------
    // Newer point put beats older range tombstone
    // ----------------------------------------------------------------

    #[test]
    fn memtable__newer_put_beats_older_range_delete() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // Put initial data
        engine.put(b"key_05".to_vec(), b"old".to_vec()).unwrap();

        // Range delete (middle LSN)
        engine
            .delete_range(b"key_00".to_vec(), b"key_10".to_vec())
            .unwrap();
        assert_eq!(engine.get(b"key_05".to_vec()).unwrap(), None);

        // New put after range delete (highest LSN) — should win
        engine.put(b"key_05".to_vec(), b"new".to_vec()).unwrap();

        assert_eq!(
            engine.get(b"key_05".to_vec()).unwrap(),
            Some(b"new".to_vec())
        );
    }

    // ----------------------------------------------------------------
    // Point delete inside an existing range
    // ----------------------------------------------------------------

    #[test]
    fn memtable__point_delete_inside_range() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // Put several keys
        for i in 0..20 {
            engine
                .put(format!("key_{:02}", i).into_bytes(), b"v".to_vec())
                .unwrap();
        }

        // Range delete [05, 15)
        engine
            .delete_range(b"key_05".to_vec(), b"key_15".to_vec())
            .unwrap();

        // Explicit point delete of key_03 (outside the range)
        engine.delete(b"key_03".to_vec()).unwrap();

        // key_03 deleted by point tombstone
        assert_eq!(engine.get(b"key_03".to_vec()).unwrap(), None);

        // key_10 deleted by range tombstone
        assert_eq!(engine.get(b"key_10".to_vec()).unwrap(), None);

        // key_02 survives
        assert_eq!(engine.get(b"key_02".to_vec()).unwrap(), Some(b"v".to_vec()));

        // key_15 survives (end is exclusive)
        assert_eq!(engine.get(b"key_15".to_vec()).unwrap(), Some(b"v".to_vec()));
    }

    // ----------------------------------------------------------------
    // Put inside range after delete — resurrects the key
    // ----------------------------------------------------------------

    #[test]
    fn memtable__put_inside_range_after_delete() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // Fill keys
        for i in 0..10 {
            engine
                .put(format!("key_{:02}", i).into_bytes(), b"old".to_vec())
                .unwrap();
        }

        // Range delete [03, 08)
        engine
            .delete_range(b"key_03".to_vec(), b"key_08".to_vec())
            .unwrap();

        // Reinsert key_05 inside the deleted range
        engine
            .put(b"key_05".to_vec(), b"resurrected".to_vec())
            .unwrap();

        // key_05 should be visible (newer LSN than range tombstone)
        assert_eq!(
            engine.get(b"key_05".to_vec()).unwrap(),
            Some(b"resurrected".to_vec())
        );

        // key_04 should still be deleted
        assert_eq!(engine.get(b"key_04".to_vec()).unwrap(), None);
    }

    // ----------------------------------------------------------------
    // Multiple range deletes with interleaved puts
    // ----------------------------------------------------------------

    #[test]
    fn memtable__interleaved_ranges_and_puts() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // Initial put
        engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        engine.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        engine.put(b"d".to_vec(), b"4".to_vec()).unwrap();
        engine.put(b"e".to_vec(), b"5".to_vec()).unwrap();

        // Range delete [b, d) — kills b, c
        engine.delete_range(b"b".to_vec(), b"d".to_vec()).unwrap();

        // Re-insert c
        engine.put(b"c".to_vec(), b"revived".to_vec()).unwrap();

        // Another range delete [c, e) — kills c (again), d
        engine.delete_range(b"c".to_vec(), b"e".to_vec()).unwrap();

        // Final re-insert d
        engine.put(b"d".to_vec(), b"final".to_vec()).unwrap();

        assert_eq!(engine.get(b"a".to_vec()).unwrap(), Some(b"1".to_vec()));
        assert_eq!(engine.get(b"b".to_vec()).unwrap(), None); // first range
        assert_eq!(engine.get(b"c".to_vec()).unwrap(), None); // second range
        assert_eq!(engine.get(b"d".to_vec()).unwrap(), Some(b"final".to_vec())); // re-inserted after second range
        assert_eq!(engine.get(b"e".to_vec()).unwrap(), Some(b"5".to_vec()));
    }

    // ----------------------------------------------------------------
    // Newer point delete beats older range that tried to "keep" it
    // (range has lower LSN, but point delete has higher LSN)
    // ----------------------------------------------------------------

    #[test]
    fn memtable__point_delete_after_range_and_put() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"x".to_vec(), b"v1".to_vec()).unwrap();

        // Range delete
        engine.delete_range(b"w".to_vec(), b"z".to_vec()).unwrap();

        // Put after range
        engine.put(b"x".to_vec(), b"v2".to_vec()).unwrap();
        assert_eq!(engine.get(b"x".to_vec()).unwrap(), Some(b"v2".to_vec()));

        // Point delete after the put
        engine.delete(b"x".to_vec()).unwrap();
        assert_eq!(engine.get(b"x".to_vec()).unwrap(), None);
    }
}
