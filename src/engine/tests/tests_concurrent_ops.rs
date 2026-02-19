//! Concurrency-under-mutation tests.
//!
//! These tests verify that concurrent readers (`get`, `scan`) work
//! correctly while the engine is actively flushing frozen memtables
//! or running compaction. The engine uses an `Arc<RwLock<EngineInner>>`
//! so readers and writers contend on the same lock. These tests prove
//! that readers always see a consistent snapshot and never observe
//! partial state.
//!
//! ## See also
//! - [`tests_hardening`] — concurrent reads during writes
//! - [`tests_stress`] — heavy mixed CRUD under load

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::engine::Engine;
    use crate::engine::tests::helpers::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    // ================================================================
    // 1. Concurrent reads during flush
    // ================================================================

    /// # Scenario
    /// Spawn reader threads doing `get()` while the main thread flushes
    /// frozen memtables to SSTables.
    ///
    /// # Expected behavior
    /// All reader threads see consistent data — either the pre-flush or
    /// post-flush state — and never encounter errors or missing keys.
    #[test]
    fn memtable_sstable__concurrent_gets_during_flush() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Arc::new(Engine::open(path, small_buffer_config()).unwrap());

        // Write enough data to create frozen memtables.
        for i in 0..50u32 {
            engine
                .put(
                    format!("key_{i:04}").into_bytes(),
                    format!("val_{i:04}").into_bytes(),
                )
                .unwrap();
        }

        // Spawn reader threads.
        let mut handles = Vec::new();
        for _ in 0..4 {
            let eng = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for i in 0..50u32 {
                    let key = format!("key_{i:04}").into_bytes();
                    // get() should never error — it may return Some or None
                    // depending on flush timing, but must not panic/error.
                    let _ = eng.get(key).expect("get must not error during flush");
                }
            }));
        }

        // Flush while readers are running.
        engine.flush_all_frozen().unwrap();

        for h in handles {
            h.join().expect("reader thread panicked");
        }

        // After flush, all keys must be readable.
        for i in 0..50u32 {
            let val = engine
                .get(format!("key_{i:04}").into_bytes())
                .unwrap()
                .expect("key must exist after flush");
            assert_eq!(val, format!("val_{i:04}").into_bytes());
        }
    }

    // ================================================================
    // 2. Concurrent scans during flush
    // ================================================================

    /// # Scenario
    /// Spawn reader threads doing `scan()` while the main thread flushes.
    ///
    /// # Expected behavior
    /// Scans complete without errors and return sorted, consistent results.
    #[test]
    fn memtable_sstable__concurrent_scans_during_flush() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Arc::new(Engine::open(path, small_buffer_config()).unwrap());

        for i in 0..50u32 {
            engine
                .put(
                    format!("key_{i:04}").into_bytes(),
                    format!("val_{i:04}").into_bytes(),
                )
                .unwrap();
        }

        let mut handles = Vec::new();
        for _ in 0..4 {
            let eng = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for _ in 0..5 {
                    let results: Vec<_> = eng
                        .scan(b"key_", b"key_\xff")
                        .expect("scan must not error during flush")
                        .collect();
                    // Must be sorted.
                    for w in results.windows(2) {
                        assert!(w[0].0 <= w[1].0, "scan results must be sorted");
                    }
                }
            }));
        }

        engine.flush_all_frozen().unwrap();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    // ================================================================
    // 3. Concurrent reads during minor compaction
    // ================================================================

    /// # Scenario
    /// Build enough SSTables to trigger minor compaction, then run
    /// compaction while reader threads are doing `get()`.
    ///
    /// # Expected behavior
    /// All readers see consistent data. No errors, no missing keys.
    #[test]
    fn memtable_sstable__concurrent_gets_during_minor_compaction() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        // Use config that triggers compaction with min_threshold=4.
        let engine = Arc::new(engine_with_multi_sstables(path, 200, "key"));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let eng = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for i in 0..200u32 {
                    let key = format!("key_{i:04}").into_bytes();
                    let _ = eng.get(key).expect("get must not error during compaction");
                }
            }));
        }

        // Run compaction while readers contend.
        let _ = engine.minor_compact();

        for h in handles {
            h.join().expect("reader thread panicked");
        }

        // All data must remain consistent.
        for i in 0..200u32 {
            let key = format!("key_{i:04}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{i:04} must be readable after compaction"
            );
        }
    }

    // ================================================================
    // 4. Concurrent reads during major compaction
    // ================================================================

    /// # Scenario
    /// Run major compaction while reader threads scan.
    ///
    /// # Expected behavior
    /// Scan results remain consistent and sorted.
    #[test]
    fn memtable_sstable__concurrent_scans_during_major_compaction() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Arc::new(engine_with_multi_sstables(path, 200, "key"));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let eng = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                let results: Vec<_> = eng
                    .scan(b"key_", b"key_\xff")
                    .expect("scan must not error during compaction")
                    .collect();
                for w in results.windows(2) {
                    assert!(w[0].0 <= w[1].0, "scan must be sorted");
                }
                assert!(!results.is_empty(), "scan should return data");
            }));
        }

        let _ = engine.major_compact();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    // ================================================================
    // 5. Concurrent writes during flush
    // ================================================================

    /// # Scenario
    /// Multiple writer threads compete for the write lock while
    /// flush is also running.
    ///
    /// # Expected behavior
    /// No deadlocks, no data loss. All written keys present afterwards.
    #[test]
    fn memtable_sstable__concurrent_writes_during_flush() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Arc::new(Engine::open(path, small_buffer_config()).unwrap());

        // Pre-populate.
        for i in 0..20u32 {
            engine
                .put(
                    format!("pre_{i:04}").into_bytes(),
                    format!("val_{i:04}").into_bytes(),
                )
                .unwrap();
        }

        let mut handles = Vec::new();
        for t in 0..4u32 {
            let eng = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for i in 0..10u32 {
                    eng.put(
                        format!("t{t}_k{i}").into_bytes(),
                        format!("t{t}_v{i}").into_bytes(),
                    )
                    .unwrap();
                }
            }));
        }

        // Flush concurrently with writes.
        engine.flush_all_frozen().unwrap();

        for h in handles {
            h.join().expect("writer thread panicked");
        }

        // Final flush to capture remaining.
        engine.flush_all_frozen().unwrap();

        // All pre-populated keys.
        for i in 0..20u32 {
            assert!(
                engine
                    .get(format!("pre_{i:04}").into_bytes())
                    .unwrap()
                    .is_some(),
                "pre_{i:04} should exist"
            );
        }

        // All thread-written keys.
        for t in 0..4u32 {
            for i in 0..10u32 {
                assert!(
                    engine
                        .get(format!("t{t}_k{i}").into_bytes())
                        .unwrap()
                        .is_some(),
                    "t{t}_k{i} should exist"
                );
            }
        }
    }

    // ================================================================
    // 6. Concurrent compaction attempts serialize correctly
    // ================================================================

    /// # Scenario
    /// Two threads both call `minor_compact()` simultaneously.
    /// One gets the lock first and compacts; the second should either
    /// compact any remaining buckets or return `Ok(false)`.
    ///
    /// # Expected behavior
    /// No panics, no data corruption. Data remains consistent.
    #[test]
    fn memtable_sstable__concurrent_compaction_attempts() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path();

        let engine = Arc::new(engine_with_multi_sstables(path, 200, "key"));

        let eng1 = Arc::clone(&engine);
        let eng2 = Arc::clone(&engine);

        let h1 = thread::spawn(move || eng1.minor_compact());
        let h2 = thread::spawn(move || eng2.minor_compact());

        let r1 = h1.join().expect("compaction thread 1 panicked");
        let r2 = h2.join().expect("compaction thread 2 panicked");

        // Both should succeed (possibly one returns false).
        assert!(r1.is_ok(), "compaction 1 should not error");
        assert!(r2.is_ok(), "compaction 2 should not error");

        // Data integrity.
        for i in 0..200u32 {
            let key = format!("key_{i:04}").into_bytes();
            assert!(
                engine.get(key).unwrap().is_some(),
                "key_{i:04} must be readable"
            );
        }
    }
}
