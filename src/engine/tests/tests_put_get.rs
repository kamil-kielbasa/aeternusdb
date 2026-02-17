//! Put/Get correctness tests — memtable-only and with SSTables.
//!
//! These tests verify the fundamental read/write contract of the storage engine:
//! inserting a key-value pair via `put()` must make it retrievable via `get()`.
//! Tests cover single keys, bulk inserts, overwrites, mixed key sizes, and
//! large values. The memtable-only group validates in-memory correctness
//! without any SSTable involvement, while the memtable+SSTable group
//! ensures data remains correct after the write buffer flushes to disk.
//!
//! ## Layer coverage
//! - `memtable__*`: memtable only (64 KB buffer — no flushes triggered)
//! - `memtable_sstable__*`: memtable + SSTable (4 KB buffer — forces flush to disk)
//!
//! ## See also
//! - [`tests_delete`] — point-delete correctness
//! - [`tests_recovery`] — put/get durability across close → reopen
//! - [`tests_scan`] — range-query correctness over put data

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
    /// Basic put/get round-trip for a single key.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config (64 KB buffer) — no data on disk.
    ///
    /// # Actions
    /// 1. Put key `"hello"` with value `"world"`.
    /// 2. Immediately get the same key.
    ///
    /// # Expected behavior
    /// `get("hello")` returns `Some("world")` — the value just written.
    #[test]
    fn memtable__put_get_single_key() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"hello".to_vec(), b"world".to_vec()).unwrap();
        assert_eq!(
            engine.get(b"hello".to_vec()).unwrap(),
            Some(b"world".to_vec())
        );
    }

    /// # Scenario
    /// Get on a key that was never inserted.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — completely empty, no data.
    ///
    /// # Actions
    /// 1. Get key `"nope"` without any prior puts.
    ///
    /// # Expected behavior
    /// `get("nope")` returns `None` — missing keys must not produce errors.
    #[test]
    fn memtable__get_missing_key_returns_none() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        assert_eq!(engine.get(b"nope".to_vec()).unwrap(), None);
    }

    /// # Scenario
    /// Overwriting the same key multiple times returns only the latest value.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no prior data.
    ///
    /// # Actions
    /// 1. Put key `"k"` with value `"v1"`.
    /// 2. Overwrite with `"v2"`, then `"v3"`.
    /// 3. Get key `"k"`.
    ///
    /// # Expected behavior
    /// `get("k")` returns `Some("v3")` — only the most recent write is visible.
    #[test]
    fn memtable__overwrite_key_returns_latest_value() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        engine.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
        engine.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
        engine.put(b"k".to_vec(), b"v3".to_vec()).unwrap();

        assert_eq!(engine.get(b"k".to_vec()).unwrap(), Some(b"v3".to_vec()));
    }

    /// # Scenario
    /// Bulk insert and retrieval of 100 sequentially-named keys.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no prior data.
    ///
    /// # Actions
    /// 1. Put 100 keys (`key_0000`..`key_0099`) with corresponding values.
    /// 2. Get each of the 100 keys.
    ///
    /// # Expected behavior
    /// Every key returns its matching value — no data loss or cross-contamination.
    #[test]
    fn memtable__many_keys() {
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

    /// # Scenario
    /// Keys of different sizes and with binary content (including null bytes).
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config — no prior data.
    ///
    /// # Actions
    /// 1. Put a 1-byte key (`0x01`).
    /// 2. Put a 256-byte key (cycling byte values).
    /// 3. Put a key containing null bytes (`[0, 0, 1]`).
    /// 4. Get all three keys.
    ///
    /// # Expected behavior
    /// Each key returns its correct value — the engine handles arbitrary key
    /// sizes and binary content (including embedded `0x00` bytes) correctly.
    #[test]
    fn memtable__mixed_key_sizes() {
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

    /// # Scenario
    /// Storing and retrieving a large (8 KB) value.
    ///
    /// # Starting environment
    /// Fresh engine with memtable-only config (64 KB buffer) — no prior data.
    ///
    /// # Actions
    /// 1. Put key `"big_val"` with an 8192-byte value (all `0xAB`).
    /// 2. Get the same key.
    ///
    /// # Expected behavior
    /// The full 8 KB value is returned intact — large values must not be
    /// truncated or corrupted.
    #[test]
    fn memtable__large_value() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), memtable_only_config()).unwrap();

        let value = vec![0xAB; 8192]; // 8KB value
        engine.put(b"big_val".to_vec(), value.clone()).unwrap();
        assert_eq!(engine.get(b"big_val".to_vec()).unwrap(), Some(value));
    }

    // ----------------------------------------------------------------
    // With SSTables — data crosses memtable → SSTable boundary
    // ----------------------------------------------------------------

    /// # Scenario
    /// Read-back of all keys after the write buffer has flushed to SSTables.
    ///
    /// # Starting environment
    /// Engine opened with 4 KB buffer (`default_config`); 200 keys are inserted,
    /// exceeding the buffer and forcing at least one SSTable flush.
    ///
    /// # Actions
    /// 1. Insert 200 padded keys via the `engine_with_sstables` helper.
    /// 2. Get each of the 200 keys.
    ///
    /// # Expected behavior
    /// Every key returns its correct padded value — data that crossed the
    /// memtable → SSTable boundary is fully readable.
    #[test]
    fn memtable_sstable__put_get_across_flush() {
        let tmp = TempDir::new().unwrap();
        let engine = engine_with_sstables(tmp.path(), 200, "key");

        for i in 0..200 {
            let key = format!("key_{:04}", i).into_bytes();
            let expected = format!("value_with_some_padding_{:04}", i).into_bytes();
            assert_eq!(engine.get(key).unwrap(), Some(expected));
        }
    }

    /// # Scenario
    /// Overwriting keys that have already been flushed to SSTables.
    ///
    /// # Starting environment
    /// Engine opened with 4 KB buffer; 150 keys inserted — some already
    /// flushed to SSTables (verified by `stats.sstables_count > 0`).
    ///
    /// # Actions
    /// 1. Insert 150 keys with `"old_*"` values (first pass).
    /// 2. Overwrite the first 50 keys with `"new_*"` values (second pass);
    ///    these go into the active memtable.
    /// 3. Get each key.
    ///
    /// # Expected behavior
    /// - Keys 0..49: return the `"new_*"` value (memtable overrides SSTable).
    /// - Keys 50..149: return the `"old_*"` value (unchanged in SSTable).
    #[test]
    fn memtable_sstable__overwrite_across_boundary() {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(tmp.path(), default_config()).unwrap();

        // First pass: fill enough to create SSTables
        for i in 0..150 {
            let key = format!("k_{:04}", i).into_bytes();
            let val = format!("old_{:04}", i).into_bytes();
            engine.put(key, val).unwrap();
        }

        engine.flush_all_frozen().unwrap();
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
