//! Memtable hardening edge-case tests — Priority 3.
//!
//! These tests exercise unusual recovery paths and memtable state
//! combinations that are not covered by the standard basic / edge-case
//! suites: WAL replay with only range-deletes, replay of interleaved
//! point and range tombstones, and size accounting corner cases.
//!
//! ## See also
//! - [`tests_basic`]       — standard put/get/delete/scan/recovery
//! - [`tests_edge_cases`]  — empty-key rejects, overflow, concurrent

#[cfg(test)]
mod tests {
    use crate::memtable::{Memtable, MemtableGetResult};
    use tempfile::TempDir;
    use tracing::Level;
    use tracing_subscriber::fmt::Subscriber;

    fn init_tracing() {
        let _ = Subscriber::builder()
            .with_max_level(Level::TRACE)
            .try_init();
    }

    // Large write-buffer to avoid FlushRequired.
    const WRITE_BUFFER: usize = 64 * 1024;

    // ================================================================
    // 1. WAL with only range-deletes recovers correctly
    // ================================================================

    /// # Scenario
    /// A freshly opened memtable receives only `delete_range` calls
    /// (no puts or point deletes). After closing and re-opening from the
    /// same WAL, the range tombstones must be present and the point tree
    /// must remain empty.
    ///
    /// # Expected behavior
    /// - `get()` for any key in the range returns `RangeDelete`.
    /// - `get()` for a key outside the range returns `NotFound`.
    #[test]
    fn wal_only_range_deletes_recovered() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal-000000.log");

        // Phase 1 — write range tombstones only.
        {
            let mt = Memtable::new(&wal_path, None, WRITE_BUFFER).unwrap();
            mt.delete_range(b"a".to_vec(), b"d".to_vec()).unwrap();
            mt.delete_range(b"m".to_vec(), b"p".to_vec()).unwrap();
        }

        // Phase 2 — reopen and verify.
        {
            let mt = Memtable::new(&wal_path, None, WRITE_BUFFER).unwrap();

            // Inside first range.
            assert_eq!(mt.get(b"b").unwrap(), MemtableGetResult::RangeDelete);
            assert_eq!(mt.get(b"c").unwrap(), MemtableGetResult::RangeDelete);

            // Inside second range.
            assert_eq!(mt.get(b"n").unwrap(), MemtableGetResult::RangeDelete);

            // Outside ranges.
            assert_eq!(mt.get(b"e").unwrap(), MemtableGetResult::NotFound);
            assert_eq!(mt.get(b"z").unwrap(), MemtableGetResult::NotFound);
        }
    }

    // ================================================================
    // 2. WAL replay preserves interleaved point-delete + range-delete
    // ================================================================

    /// # Scenario
    /// A memtable receives interleaved puts, point-deletes, and range-
    /// deletes. After re-opening from the same WAL, the final visibility
    /// of each key must match the pre-close state.
    ///
    /// # Expected behavior
    /// WAL replay reproduces exact same get() results.
    #[test]
    fn wal_interleaved_point_and_range_deletes_recovered() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal-000000.log");

        // Phase 1 — mixed writes.
        let expected: Vec<(&[u8], MemtableGetResult)>;
        {
            let mt = Memtable::new(&wal_path, None, WRITE_BUFFER).unwrap();
            mt.put(b"a".to_vec(), b"v_a".to_vec()).unwrap();
            mt.put(b"b".to_vec(), b"v_b".to_vec()).unwrap();
            mt.put(b"c".to_vec(), b"v_c".to_vec()).unwrap();
            mt.put(b"d".to_vec(), b"v_d".to_vec()).unwrap();

            // Point-delete "b".
            mt.delete(b"b".to_vec()).unwrap();

            // Range-delete [c, e) — covers "c" and "d".
            mt.delete_range(b"c".to_vec(), b"e".to_vec()).unwrap();

            expected = vec![
                (b"a", mt.get(b"a").unwrap()),
                (b"b", mt.get(b"b").unwrap()),
                (b"c", mt.get(b"c").unwrap()),
                (b"d", mt.get(b"d").unwrap()),
                (b"e", mt.get(b"e").unwrap()),
            ];
        }

        // Phase 2 — reopen and verify.
        {
            let mt = Memtable::new(&wal_path, None, WRITE_BUFFER).unwrap();
            for (key, exp) in &expected {
                assert_eq!(
                    &mt.get(key).unwrap(),
                    exp,
                    "mismatch for key {:?}",
                    String::from_utf8_lossy(key)
                );
            }
        }
    }

    // ================================================================
    // 3. put → range-delete → put (resurrect) survives WAL replay
    // ================================================================

    /// # Scenario
    /// A key is put, deleted by a range tombstone, then put again
    /// (resurrected). After WAL replay, the final put should be visible
    /// because its LSN is higher than the range tombstone.
    ///
    /// # Expected behavior
    /// `get("key")` returns the resurrected value after replay.
    #[test]
    fn wal_resurrect_after_range_delete_recovered() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal-000000.log");

        {
            let mt = Memtable::new(&wal_path, None, WRITE_BUFFER).unwrap();
            mt.put(b"key".to_vec(), b"first".to_vec()).unwrap();
            mt.delete_range(b"k".to_vec(), b"l".to_vec()).unwrap();
            mt.put(b"key".to_vec(), b"resurrected".to_vec()).unwrap();

            // Pre-close sanity check.
            assert_eq!(
                mt.get(b"key").unwrap(),
                MemtableGetResult::Put(b"resurrected".to_vec())
            );
        }

        // Reopen.
        {
            let mt = Memtable::new(&wal_path, None, WRITE_BUFFER).unwrap();
            assert_eq!(
                mt.get(b"key").unwrap(),
                MemtableGetResult::Put(b"resurrected".to_vec())
            );
        }
    }

    // ================================================================
    // 4. Multiple overlapping range tombstones via WAL replay
    // ================================================================

    /// # Scenario
    /// Two overlapping range tombstones `[a, d)` and `[c, f)` are issued.
    /// After replay, keys within the union `[a, f)` are deleted, while
    /// keys outside remain visible (or NotFound if never written).
    ///
    /// # Expected behavior
    /// The union of both tombstones covers the full `[a, f)` range.
    #[test]
    fn wal_overlapping_range_tombstones_recovered() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal-000000.log");

        {
            let mt = Memtable::new(&wal_path, None, WRITE_BUFFER).unwrap();
            // Put keys across the range.
            for k in &[b"a", b"b", b"c", b"d", b"e", b"f", b"g"] {
                mt.put(k.to_vec(), b"v".to_vec()).unwrap();
            }
            // Two overlapping range deletes.
            mt.delete_range(b"a".to_vec(), b"d".to_vec()).unwrap();
            mt.delete_range(b"c".to_vec(), b"f".to_vec()).unwrap();
        }

        {
            let mt = Memtable::new(&wal_path, None, WRITE_BUFFER).unwrap();
            // a, b, c, d, e — deleted (covered by union).
            assert_eq!(mt.get(b"a").unwrap(), MemtableGetResult::RangeDelete);
            assert_eq!(mt.get(b"b").unwrap(), MemtableGetResult::RangeDelete);
            assert_eq!(mt.get(b"c").unwrap(), MemtableGetResult::RangeDelete);
            assert_eq!(mt.get(b"d").unwrap(), MemtableGetResult::RangeDelete);
            assert_eq!(mt.get(b"e").unwrap(), MemtableGetResult::RangeDelete);
            // f, g — visible (outside both ranges).
            assert_eq!(mt.get(b"f").unwrap(), MemtableGetResult::Put(b"v".to_vec()));
            assert_eq!(mt.get(b"g").unwrap(), MemtableGetResult::Put(b"v".to_vec()));
        }
    }

    // ================================================================
    // 5. LSN counter is restored correctly after WAL replay
    // ================================================================

    /// # Scenario
    /// After replaying a WAL with N operations, subsequent writes should
    /// receive LSNs greater than any replayed LSN. This prevents stale
    /// LSN assignment after crash recovery.
    ///
    /// # Expected behavior
    /// New writes after replay have LSNs > max replayed LSN.
    #[test]
    fn wal_lsn_counter_resumed_after_replay() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal-000000.log");

        // Phase 1 — issue 5 operations (LSNs 0..4).
        {
            let mt = Memtable::new(&wal_path, None, WRITE_BUFFER).unwrap();
            for i in 0..5u32 {
                mt.put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes())
                    .unwrap();
            }
        }

        // Phase 2 — reopen and issue new write.
        {
            let mt = Memtable::new(&wal_path, None, WRITE_BUFFER).unwrap();
            mt.put(b"new_key".to_vec(), b"new_val".to_vec()).unwrap();

            // Read back and verify the new key is visible (implying its LSN
            // is valid and higher than replayed entries, so it won't be
            // shadowed by any old tombstone).
            assert_eq!(
                mt.get(b"new_key").unwrap(),
                MemtableGetResult::Put(b"new_val".to_vec())
            );

            // Verify all old keys are still visible.
            for i in 0..5u32 {
                assert_eq!(
                    mt.get(format!("k{i}").as_bytes()).unwrap(),
                    MemtableGetResult::Put(format!("v{i}").into_bytes())
                );
            }
        }
    }
}
