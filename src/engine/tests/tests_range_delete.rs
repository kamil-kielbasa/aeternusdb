//! Range-delete correctness tests.

#[cfg(test)]
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

    #[test]
    fn range_delete_single_key_via_range() {
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

    #[test]
    fn range_delete_partial() {
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

    #[test]
    fn range_delete_empty_range_is_noop() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();
        populate(&engine, 5);

        // Empty range: start == end
        engine
            .delete_range(b"key_02".to_vec(), b"key_02".to_vec())
            .unwrap();

        // Reversed range: start > end
        engine
            .delete_range(b"key_04".to_vec(), b"key_01".to_vec())
            .unwrap();

        // Everything still exists
        for i in 0..5 {
            assert_exists(&engine, i);
        }
    }

    // ----------------------------------------------------------------
    // Overlapping ranges
    // ----------------------------------------------------------------

    #[test]
    fn range_delete_overlapping_ranges() {
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

    #[test]
    fn range_delete_nested_ranges() {
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

    #[test]
    fn range_delete_adjacent_ranges() {
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

    #[test]
    fn range_delete_beyond_existing_keys() {
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

    #[test]
    fn range_delete_all_keys() {
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

    #[test]
    fn range_delete_hides_sstable_keys() {
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
