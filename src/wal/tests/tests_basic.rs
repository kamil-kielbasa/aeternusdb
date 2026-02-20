//! WAL basic operation tests.
//!
//! These tests verify the fundamental append → replay → truncate cycle of the
//! Write-Ahead Log using two structurally different record types
//! (`MemTableRecord` and `ManifestRecord`).
//!
//! Coverage:
//! - Single-record append + replay
//! - Multi-record append + replay
//! - Append → replay → truncate → verify empty
//! - Full lifecycle: write → replay → truncate → rewrite → replay → truncate
//!
//! ## See also
//! - [`tests_corruption`] — corruption detection and partial replay
//! - [`tests_rotation`] — file rotation and sequence validation

#[cfg(test)]
mod tests {
    use crate::wal::Wal;
    use crate::wal::tests::helpers::*;
    use tempfile::TempDir;

    // ----------------------------------------------------------------
    // Single-record round-trip
    // ----------------------------------------------------------------

    /// # Scenario
    /// Append a single memtable-style record and replay it.
    ///
    /// # Starting environment
    /// Fresh WAL file — no prior records.
    ///
    /// # Actions
    /// 1. Open a new WAL.
    /// 2. Append one `MemTableRecord`.
    /// 3. Replay via `replay_iter()`.
    ///
    /// # Expected behavior
    /// The replayed vector contains exactly the one appended record,
    /// with all fields preserved.
    #[test]
    fn one_append_and_replay() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let insert = vec![MemTableRecord {
            key: b"a".to_vec(),
            value: Some(b"v1".to_vec()),
            timestamp: 1,
            deleted: false,
        }];

        for record in &insert {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(insert, replayed);
    }

    // ----------------------------------------------------------------
    // Multi-record round-trip
    // ----------------------------------------------------------------

    /// # Scenario
    /// Append multiple manifest-style records and replay them.
    ///
    /// # Starting environment
    /// Fresh WAL file — no prior records.
    ///
    /// # Actions
    /// 1. Open a new WAL.
    /// 2. Append three `ManifestRecord`s.
    /// 3. Replay via `replay_iter()`.
    ///
    /// # Expected behavior
    /// All three records are replayed in insertion order with identical
    /// field values.
    #[test]
    fn many_appends_and_replay() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let insert = vec![
            ManifestRecord {
                id: 0,
                path: "/db/table-0".to_string(),
                creation_timestamp: 100,
            },
            ManifestRecord {
                id: 1,
                path: "/db/table-1".to_string(),
                creation_timestamp: 101,
            },
            ManifestRecord {
                id: 2,
                path: "/db/table-2".to_string(),
                creation_timestamp: 102,
            },
        ];

        for record in &insert {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(insert, replayed);
    }

    // ----------------------------------------------------------------
    // Append → replay → truncate → verify empty
    // ----------------------------------------------------------------

    /// # Scenario
    /// Append records, replay, then truncate and verify the WAL is empty.
    ///
    /// # Starting environment
    /// Fresh WAL file — no prior records.
    ///
    /// # Actions
    /// 1. Append three `MemTableRecord`s.
    /// 2. Replay → expect 3 records.
    /// 3. `truncate()`.
    /// 4. Replay again.
    ///
    /// # Expected behavior
    /// After truncate the WAL replays zero records.
    #[test]
    fn append_replay_and_truncate() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mut wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let insert = vec![
            MemTableRecord {
                key: b"a".to_vec(),
                value: Some(b"v1".to_vec()),
                timestamp: 1,
                deleted: false,
            },
            MemTableRecord {
                key: b"b".to_vec(),
                value: Some(b"v2".to_vec()),
                timestamp: 2,
                deleted: false,
            },
            MemTableRecord {
                key: b"c".to_vec(),
                value: Some(b"v3".to_vec()),
                timestamp: 3,
                deleted: false,
            },
        ];

        for record in &insert {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(insert, replayed);

        wal.truncate().unwrap();
        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(replayed.len(), 0);
    }

    // ----------------------------------------------------------------
    // Full lifecycle: write → truncate → rewrite → truncate
    // ----------------------------------------------------------------

    /// # Scenario
    /// A full write → truncate → rewrite cycle proving truncation
    /// genuinely clears the log for subsequent writes.
    ///
    /// # Starting environment
    /// Fresh WAL file — no prior records.
    ///
    /// # Actions
    /// 1. Append `batch1` (2 records) → replay → expect `batch1`.
    /// 2. `truncate()` → replay → expect 0 records.
    /// 3. Append `batch2` (3 records) → replay → expect `batch2`.
    /// 4. `truncate()` → replay → expect 0 records.
    ///
    /// # Expected behavior
    /// Each truncation resets the log completely; subsequent appends
    /// only contain the new data.
    #[test]
    fn full_cycle_write_truncate_rewrite() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("000000.log");
        let mut wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let batch1 = vec![
            ManifestRecord {
                id: 0,
                path: "/db/table-0".to_string(),
                creation_timestamp: 100,
            },
            ManifestRecord {
                id: 1,
                path: "/db/table-1".to_string(),
                creation_timestamp: 101,
            },
        ];

        let batch2 = vec![
            ManifestRecord {
                id: 100,
                path: "/db/table-100".to_string(),
                creation_timestamp: 1000,
            },
            ManifestRecord {
                id: 101,
                path: "/db/table-101".to_string(),
                creation_timestamp: 1001,
            },
            ManifestRecord {
                id: 102,
                path: "/db/table-102".to_string(),
                creation_timestamp: 1002,
            },
        ];

        for record in &batch1 {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(batch1, replayed);

        wal.truncate().unwrap();
        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(replayed.len(), 0);

        for record in &batch2 {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(batch2, replayed);

        wal.truncate().unwrap();
        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(replayed.len(), 0);
    }
}
