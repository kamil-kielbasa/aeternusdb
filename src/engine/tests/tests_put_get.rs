//! Put/Get correctness tests — memtable-only and with SSTables.

#[cfg(test)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Memtable-only
    // ----------------------------------------------------------------

    #[test]
    fn put_get_single_key() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"hello".to_vec(), b"world".to_vec()).unwrap();
        assert_eq!(
            engine.get(b"hello".to_vec()).unwrap(),
            Some(b"world".to_vec())
        );
    }

    #[test]
    fn get_missing_key_returns_none() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        assert_eq!(engine.get(b"nope".to_vec()).unwrap(), None);
    }

    #[test]
    fn overwrite_key_returns_latest_value() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
        engine.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
        engine.put(b"k".to_vec(), b"v3".to_vec()).unwrap();

        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn many_keys() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        for i in 0u32..100 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("val_{:04}", i).into_bytes();
            engine.put(key, value).unwrap();
        }

        for i in 0u32..100 {
            let key = format!("key_{:04}", i).into_bytes();
            let expected = format!("val_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(expected));
        }
    }

    #[test]
    fn mixed_key_sizes() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        // 1-byte key
        engine.put(vec![0x01], b"tiny".to_vec()).unwrap();
        // 256-byte key
        let big_key: Vec<u8> = (0..256).map(|i| (i % 256) as u8).collect();
        engine.put(big_key.clone(), b"big".to_vec()).unwrap();
        // Key with 0x00 bytes
        engine.put(vec![0, 0, 1], b"nulls".to_vec()).unwrap();

        assert_eq!(engine.get(vec![0x01]).unwrap(), Some(b"tiny".to_vec()));
        assert_eq!(engine.get(big_key).unwrap(), Some(b"big".to_vec()));
        assert_eq!(engine.get(vec![0, 0, 1]).unwrap(), Some(b"nulls".to_vec()));
    }

    #[test]
    fn large_value() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        let value = vec![0xAB; 8192]; // 8KB value
        engine.put(b"big_val".to_vec(), value.clone()).unwrap();
        assert_eq!(engine.get(b"big_val".to_vec()).unwrap(), Some(value));
    }

    // ----------------------------------------------------------------
    // With SSTables — data crosses memtable → SSTable boundary
    // ----------------------------------------------------------------

    #[test]
    fn put_get_across_sstable_flush() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "key");

        for i in 0..200 {
            let key = format!("key_{:04}", i).into_bytes();
            let expected = format!("value_with_some_padding_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(expected));
        }
    }

    #[test]
    fn overwrite_across_sstable_boundary() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), default_config()).unwrap();

        // First pass: fill enough to create SSTables
        for i in 0..150 {
            let key = format!("k_{:04}", i).into_bytes();
            let val = format!("old_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        let stats = engine.stats().unwrap();
        assert!(stats.sstables_count > 0);

        // Second pass: overwrite a subset — these go to the active memtable
        for i in 0..50 {
            let key = format!("k_{:04}", i).into_bytes();
            let val = format!("new_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        // Verify: overwritten keys should have new value
        for i in 0..50 {
            let key = format!("k_{:04}", i).into_bytes();
            let expected = format!("new_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key.clone()).unwrap(),
                Some(expected),
                "key k_{:04}",
                i
            );
        }

        // Non-overwritten keys should still have old value (read from SSTable)
        for i in 50..150 {
            let key = format!("k_{:04}", i).into_bytes();
            let expected = format!("old_{:04}", i).into_bytes();
            assert_eq!(
                engine.get(key.clone()).unwrap(),
                Some(expected),
                "key k_{:04}",
                i
            );
        }
    }
}
