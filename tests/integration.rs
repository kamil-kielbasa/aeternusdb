//! Integration tests for the public `Db` API.
//!
//! These tests exercise the full storage stack (WAL → memtable → SSTable →
//! compaction) through the public `aeternusdb::{Db, DbConfig, DbError}` surface
//! only. No internal modules are referenced.
//!
//! ## Coverage areas
//! - **Lifecycle**: open, close, idempotent close, Drop-based cleanup
//! - **CRUD**: put, get, delete, delete_range, overwrite, nonexistent keys
//! - **Scan**: range queries, empty ranges, tombstone filtering
//! - **Persistence**: data survives close → reopen, deletes survive reopen
//! - **Compaction**: major compaction preserves data, removes deleted keys
//! - **Config validation**: all `DbConfig` constraint violations rejected
//! - **Error handling**: closed-db operations, empty-key rejection, invalid ranges
//! - **Concurrency**: multi-thread writes, concurrent readers during writes
//! - **Full-stack**: end-to-end lifecycle with writes, deletes, range-deletes,
//!   compaction, and scan verification
//!
//! ## See also
//! - [`engine::tests`] — internal engine-level unit tests
//! - [`sstable::tests`] — SSTable read/write unit tests
//! - [`memtable::tests`] — memtable unit tests

use aeternusdb::{Db, DbConfig, DbError};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

/// Small write buffer to trigger frequent freezes and background flushes.
fn small_buffer_config() -> DbConfig {
    DbConfig {
        write_buffer_size: 1024,
        min_compaction_threshold: 4,
        max_compaction_threshold: 32,
        tombstone_compaction_ratio: 0.3,
        thread_pool_size: 2,
        ..DbConfig::default()
    }
}

/// Reopen a database at the same path with default config.
fn reopen(path: &std::path::Path) -> Db {
    Db::open(path, DbConfig::default()).expect("reopen")
}

// ================================================================================================
// Lifecycle
// ================================================================================================

/// # Scenario
/// Open a fresh database and immediately close it.
///
/// # Starting environment
/// Empty temporary directory — no prior data.
///
/// # Actions
/// 1. `Db::open` with default config.
/// 2. `db.close()`.
///
/// # Expected behavior
/// Both operations succeed without error.
#[test]
fn open_close_empty() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// Calling `close()` twice must not panic or return an error.
///
/// # Starting environment
/// Freshly opened database with default config.
///
/// # Actions
/// 1. `db.close()` — first close.
/// 2. `db.close()` — second close (should be a no-op).
///
/// # Expected behavior
/// Both calls return `Ok(())`.
#[test]
fn close_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();
    db.close().unwrap();
    db.close().unwrap(); // second close is a no-op
}

/// # Scenario
/// Dropping the handle without calling `close()` must still persist data.
///
/// # Starting environment
/// Freshly opened database with default config.
///
/// # Actions
/// 1. Put key `"key"` → `"value"`.
/// 2. `drop(db)` without calling `close()`.
/// 3. Reopen database from the same directory.
/// 4. `get("key")`.
///
/// # Expected behavior
/// The `Drop` impl flushes state; reopened `get` returns `Some("value")`.
#[test]
fn drop_without_close() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();
    db.put(b"key", b"value").unwrap();
    drop(db); // Drop handles cleanup

    // Reopen should recover the data.
    let db = reopen(dir.path());
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));
    db.close().unwrap();
}

// ================================================================================================
// Basic CRUD
// ================================================================================================

/// # Scenario
/// Basic put/get round-trip for a single key.
///
/// # Starting environment
/// Freshly opened database — no data.
///
/// # Actions
/// 1. Put `"hello"` → `"world"`.
/// 2. `get("hello")`.
///
/// # Expected behavior
/// `get` returns `Some("world")`.
#[test]
fn put_get_single() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    db.put(b"hello", b"world").unwrap();
    assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));

    db.close().unwrap();
}

/// # Scenario
/// Overwriting a key must return the latest value.
///
/// # Starting environment
/// Freshly opened database — no data.
///
/// # Actions
/// 1. Put `"key"` → `"v1"`.
/// 2. Put `"key"` → `"v2"` (overwrite).
/// 3. `get("key")`.
///
/// # Expected behavior
/// `get` returns `Some("v2")` — the second write wins.
#[test]
fn put_overwrite() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    db.put(b"key", b"v1").unwrap();
    db.put(b"key", b"v2").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"v2".to_vec()));

    db.close().unwrap();
}

/// # Scenario
/// Deleting a key makes it invisible to subsequent reads.
///
/// # Starting environment
/// Freshly opened database — no data.
///
/// # Actions
/// 1. Put `"key"` → `"value"`.
/// 2. Verify `get("key")` returns `Some("value")`.
/// 3. `delete("key")`.
/// 4. `get("key")`.
///
/// # Expected behavior
/// After deletion, `get` returns `None`.
#[test]
fn delete_key() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    db.put(b"key", b"value").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value".to_vec()));

    db.delete(b"key").unwrap();
    assert_eq!(db.get(b"key").unwrap(), None);

    db.close().unwrap();
}

/// # Scenario
/// Range-delete hides keys in `[start, end)` while leaving others intact.
///
/// # Starting environment
/// Freshly opened database — no data.
///
/// # Actions
/// 1. Put keys `"a"` through `"e"` with single-byte values.
/// 2. `delete_range("b", "d")` — removes `"b"` and `"c"`.
/// 3. Get each key.
///
/// # Expected behavior
/// `"a"`, `"d"`, `"e"` survive; `"b"` and `"c"` return `None`.
#[test]
fn delete_range_basic() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    for c in b'a'..=b'e' {
        db.put(&[c], &[c]).unwrap();
    }

    // Delete [b, d)
    db.delete_range(b"b", b"d").unwrap();

    assert_eq!(db.get(b"a").unwrap(), Some(vec![b'a']));
    assert_eq!(db.get(b"b").unwrap(), None);
    assert_eq!(db.get(b"c").unwrap(), None);
    assert_eq!(db.get(b"d").unwrap(), Some(vec![b'd']));
    assert_eq!(db.get(b"e").unwrap(), Some(vec![b'e']));

    db.close().unwrap();
}

/// # Scenario
/// Getting a key that was never inserted returns `None`.
///
/// # Starting environment
/// Freshly opened database — no data.
///
/// # Actions
/// 1. `get("missing")` without any prior writes.
///
/// # Expected behavior
/// Returns `Ok(None)` — not an error.
#[test]
fn get_nonexistent_key() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    assert_eq!(db.get(b"missing").unwrap(), None);

    db.close().unwrap();
}

// ================================================================================================
// Scan
// ================================================================================================

/// # Scenario
/// Scan returns key-value pairs in the half-open range `[start, end)`,
/// sorted by key.
///
/// # Starting environment
/// Freshly opened database — no data.
///
/// # Actions
/// 1. Put keys `"a"` through `"d"` with values `"1"` through `"4"`.
/// 2. `scan("b", "d")` — should return `"b"` and `"c"` only.
///
/// # Expected behavior
/// Two key-value pairs returned in sorted order; `"a"` and `"d"` excluded.
#[test]
fn scan_basic() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.put(b"d", b"4").unwrap();

    let results = db.scan(b"b", b"d").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (b"b".to_vec(), b"2".to_vec()));
    assert_eq!(results[1], (b"c".to_vec(), b"3".to_vec()));

    db.close().unwrap();
}

/// # Scenario
/// Scanning an empty or inverted range returns an empty result.
///
/// # Starting environment
/// Database with one key `"a"` → `"1"`.
///
/// # Actions
/// 1. `scan("z", "a")` — start > end (inverted).
/// 2. `scan("x", "z")` — valid range but no keys fall within it.
///
/// # Expected behavior
/// Both scans return an empty `Vec`.
#[test]
fn scan_empty_range() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    db.put(b"a", b"1").unwrap();

    // start >= end → empty result
    let results = db.scan(b"z", b"a").unwrap();
    assert!(results.is_empty());

    // No keys in range
    let results = db.scan(b"x", b"z").unwrap();
    assert!(results.is_empty());

    db.close().unwrap();
}

/// # Scenario
/// Scan must exclude keys hidden by a point-delete tombstone.
///
/// # Starting environment
/// Freshly opened database — no data.
///
/// # Actions
/// 1. Put `"a"`, `"b"`, `"c"`.
/// 2. `delete("b")`.
/// 3. `scan("a", "d")`.
///
/// # Expected behavior
/// Only `"a"` and `"c"` appear; `"b"` is filtered out.
#[test]
fn scan_excludes_deleted_keys() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.delete(b"b").unwrap();

    let results = db.scan(b"a", b"d").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b"a".to_vec());
    assert_eq!(results[1].0, b"c".to_vec());

    db.close().unwrap();
}

// ================================================================================================
// Persistence
// ================================================================================================

/// # Scenario
/// Data written before `close()` is readable after reopening.
///
/// # Starting environment
/// Empty temporary directory.
///
/// # Actions
/// 1. Open database, put `"persist_key"` → `"persist_value"`, close.
/// 2. Reopen database from the same directory.
/// 3. `get("persist_key")`.
///
/// # Expected behavior
/// The reopened database returns `Some("persist_value")`.
#[test]
fn persistence_across_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let db = Db::open(dir.path(), DbConfig::default()).unwrap();
        db.put(b"persist_key", b"persist_value").unwrap();
        db.close().unwrap();
    }

    {
        let db = reopen(dir.path());
        assert_eq!(
            db.get(b"persist_key").unwrap(),
            Some(b"persist_value".to_vec())
        );
        db.close().unwrap();
    }
}

/// # Scenario
/// Hundreds of writes survive close → reopen with a small write buffer
/// that triggers multiple flushes.
///
/// # Starting environment
/// Empty temporary directory, 1 KiB write buffer (forces frequent flushes).
///
/// # Actions
/// 1. Write 500 sequentially-named keys, close.
/// 2. Reopen and verify all 500 keys.
///
/// # Expected behavior
/// Every key is present with its original value after reopen.
#[test]
fn persistence_many_writes() {
    let dir = TempDir::new().unwrap();

    {
        let db = Db::open(dir.path(), small_buffer_config()).unwrap();
        for i in 0..500u32 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.close().unwrap();
    }

    {
        let db = Db::open(dir.path(), small_buffer_config()).unwrap();
        for i in 0..500u32 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "key_{:04} should be present after reopen",
                i
            );
        }
        db.close().unwrap();
    }
}

/// # Scenario
/// Point-delete tombstones survive close → reopen.
///
/// # Starting environment
/// Empty temporary directory.
///
/// # Actions
/// 1. Put `"alive"` → `"yes"` and `"dead"` → `"soon"`, then `delete("dead")`, close.
/// 2. Reopen and get both keys.
///
/// # Expected behavior
/// `"alive"` returns `Some("yes")`; `"dead"` returns `None`.
#[test]
fn persistence_deletes_survive_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let db = Db::open(dir.path(), DbConfig::default()).unwrap();
        db.put(b"alive", b"yes").unwrap();
        db.put(b"dead", b"soon").unwrap();
        db.delete(b"dead").unwrap();
        db.close().unwrap();
    }

    {
        let db = reopen(dir.path());
        assert_eq!(db.get(b"alive").unwrap(), Some(b"yes".to_vec()));
        assert_eq!(db.get(b"dead").unwrap(), None);
        db.close().unwrap();
    }
}

// ================================================================================================
// Compaction
// ================================================================================================

/// # Scenario
/// Major compaction merges multiple SSTables into one while preserving
/// all live data.
///
/// # Starting environment
/// 1 KiB write buffer — 200 writes produce multiple SSTables.
///
/// # Actions
/// 1. Write 200 keys, close (flushes all frozen memtables).
/// 2. Reopen, run `major_compact()`.
/// 3. Verify all 200 keys are still readable.
///
/// # Expected behavior
/// `major_compact` returns `true` (compaction happened). All keys survive.
#[test]
fn major_compaction() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), small_buffer_config()).unwrap();

    // Write enough data to create multiple SSTables.
    for i in 0..200u32 {
        let key = format!("mc_{:04}", i);
        let val = format!("val_{:04}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    // Close flushes everything.
    db.close().unwrap();

    // Reopen and run major compaction.
    let db = Db::open(dir.path(), small_buffer_config()).unwrap();
    let compacted = db.major_compact().unwrap();
    assert!(compacted, "should have compacted multiple SSTables");

    // All data should still be present.
    for i in 0..200u32 {
        let key = format!("mc_{:04}", i);
        let val = format!("val_{:04}", i);
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "mc_{:04} should survive major compaction",
            i
        );
    }

    db.close().unwrap();
}

/// # Scenario
/// Major compaction physically removes point-deleted keys from SSTables.
///
/// # Starting environment
/// 1 KiB write buffer — writes produce multiple SSTables.
///
/// # Actions
/// 1. Write 100 keys, point-delete even-indexed keys, close.
/// 2. Reopen, run `major_compact()`.
/// 3. Verify even keys return `None`, odd keys return their values.
///
/// # Expected behavior
/// Tombstones are applied during compaction; deleted keys are gone.
#[test]
fn major_compaction_removes_deleted_keys() {
    let dir = TempDir::new().unwrap();

    {
        let db = Db::open(dir.path(), small_buffer_config()).unwrap();
        for i in 0..100u32 {
            let key = format!("del_{:04}", i);
            let val = format!("val_{:04}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        // Delete half the keys.
        for i in (0..100u32).step_by(2) {
            let key = format!("del_{:04}", i);
            db.delete(key.as_bytes()).unwrap();
        }
        db.close().unwrap();
    }

    {
        let db = Db::open(dir.path(), small_buffer_config()).unwrap();
        db.major_compact().unwrap();

        for i in 0..100u32 {
            let key = format!("del_{:04}", i);
            if i % 2 == 0 {
                assert_eq!(db.get(key.as_bytes()).unwrap(), None);
            } else {
                let val = format!("val_{:04}", i);
                assert_eq!(db.get(key.as_bytes()).unwrap(), Some(val.into_bytes()));
            }
        }
        db.close().unwrap();
    }
}

// ================================================================================================
// Config validation
// ================================================================================================

/// # Scenario
/// `write_buffer_size` below the 1024-byte minimum is rejected.
///
/// # Starting environment
/// Empty temporary directory.
///
/// # Actions
/// 1. `Db::open` with `write_buffer_size: 100`.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_write_buffer_too_small() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        write_buffer_size: 100,
        ..DbConfig::default()
    };
    let err = Db::open(dir.path(), config).unwrap_err();
    assert!(matches!(err, DbError::InvalidConfig(_)));
}

/// # Scenario
/// `min_compaction_threshold` below 2 is rejected.
///
/// # Starting environment
/// Empty temporary directory.
///
/// # Actions
/// 1. `Db::open` with `min_compaction_threshold: 1`.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_min_threshold_too_small() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        min_compaction_threshold: 1,
        ..DbConfig::default()
    };
    let err = Db::open(dir.path(), config).unwrap_err();
    assert!(matches!(err, DbError::InvalidConfig(_)));
}

/// # Scenario
/// `max_compaction_threshold` below `min_compaction_threshold` is rejected.
///
/// # Starting environment
/// Empty temporary directory.
///
/// # Actions
/// 1. `Db::open` with `min: 8, max: 4`.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_max_below_min() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        min_compaction_threshold: 8,
        max_compaction_threshold: 4,
        ..DbConfig::default()
    };
    let err = Db::open(dir.path(), config).unwrap_err();
    assert!(matches!(err, DbError::InvalidConfig(_)));
}

/// # Scenario
/// `tombstone_compaction_ratio` outside `(0.0, 1.0]` is rejected.
///
/// # Starting environment
/// Empty temporary directory.
///
/// # Actions
/// 1. `Db::open` with ratio `0.0` (boundary — exclusive).
/// 2. `Db::open` with ratio `1.5` (above upper bound).
///
/// # Expected behavior
/// Both return `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_tombstone_ratio_out_of_range() {
    let dir = TempDir::new().unwrap();

    let config = DbConfig {
        tombstone_compaction_ratio: 0.0,
        ..DbConfig::default()
    };
    assert!(matches!(
        Db::open(dir.path(), config).unwrap_err(),
        DbError::InvalidConfig(_)
    ));

    let config = DbConfig {
        tombstone_compaction_ratio: 1.5,
        ..DbConfig::default()
    };
    assert!(matches!(
        Db::open(dir.path(), config).unwrap_err(),
        DbError::InvalidConfig(_)
    ));
}

/// # Scenario
/// `thread_pool_size` of 0 is rejected (at least 1 thread required).
///
/// # Starting environment
/// Empty temporary directory.
///
/// # Actions
/// 1. `Db::open` with `thread_pool_size: 0`.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_zero_threads() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        thread_pool_size: 0,
        ..DbConfig::default()
    };
    let err = Db::open(dir.path(), config).unwrap_err();
    assert!(matches!(err, DbError::InvalidConfig(_)));
}

// ================================================================================================
// Error handling
// ================================================================================================

/// # Scenario
/// Every operation on a closed database returns `DbError::Closed`.
///
/// # Starting environment
/// Database opened then immediately closed.
///
/// # Actions
/// 1. Call `put`, `get`, `delete`, `delete_range`, `scan`, `major_compact`
///    on the closed handle.
///
/// # Expected behavior
/// All six calls return `Err(DbError::Closed)`.
#[test]
fn operations_after_close() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();
    db.close().unwrap();

    assert!(matches!(db.put(b"k", b"v"), Err(DbError::Closed)));
    assert!(matches!(db.get(b"k"), Err(DbError::Closed)));
    assert!(matches!(db.delete(b"k"), Err(DbError::Closed)));
    assert!(matches!(db.delete_range(b"a", b"z"), Err(DbError::Closed)));
    assert!(matches!(db.scan(b"a", b"z"), Err(DbError::Closed)));
    assert!(matches!(db.major_compact(), Err(DbError::Closed)));
}

/// # Scenario
/// Passing an empty key or empty value returns `DbError::InvalidArgument`.
///
/// # Starting environment
/// Freshly opened database.
///
/// # Actions
/// 1. `put("", "v")`, `put("k", "")` — empty key and empty value.
/// 2. `get("")`, `delete("")` — empty key.
/// 3. `scan("", "z")`, `scan("a", "")` — empty start / end.
///
/// # Expected behavior
/// All return `Err(DbError::InvalidArgument(_))`.
#[test]
fn empty_key_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    assert!(matches!(
        db.put(b"", b"v"),
        Err(DbError::InvalidArgument(_))
    ));
    assert!(matches!(
        db.put(b"k", b""),
        Err(DbError::InvalidArgument(_))
    ));
    assert!(matches!(db.get(b""), Err(DbError::InvalidArgument(_))));
    assert!(matches!(db.delete(b""), Err(DbError::InvalidArgument(_))));
    assert!(matches!(
        db.scan(b"", b"z"),
        Err(DbError::InvalidArgument(_))
    ));
    assert!(matches!(
        db.scan(b"a", b""),
        Err(DbError::InvalidArgument(_))
    ));

    db.close().unwrap();
}

/// # Scenario
/// `delete_range` with `start >= end` returns `DbError::InvalidArgument`.
///
/// # Starting environment
/// Freshly opened database.
///
/// # Actions
/// 1. `delete_range("z", "a")` — start > end.
/// 2. `delete_range("x", "x")` — start == end.
///
/// # Expected behavior
/// Both return `Err(DbError::InvalidArgument(_))`.
#[test]
fn delete_range_invalid_args() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    // start >= end
    assert!(matches!(
        db.delete_range(b"z", b"a"),
        Err(DbError::InvalidArgument(_))
    ));
    // start == end
    assert!(matches!(
        db.delete_range(b"x", b"x"),
        Err(DbError::InvalidArgument(_))
    ));

    db.close().unwrap();
}

// ================================================================================================
// Concurrency
// ================================================================================================

/// # Scenario
/// Four threads write 100 disjoint keys each; all 400 are readable
/// after the threads join.
///
/// # Starting environment
/// Freshly opened database shared via `Arc<Db>`.
///
/// # Actions
/// 1. Spawn 4 writer threads, each writing `t{id}_k{0..99}`.
/// 2. Join all threads.
/// 3. Read all 400 keys from the main thread.
///
/// # Expected behavior
/// All 400 keys return their corresponding values — no data loss.
#[test]
fn concurrent_writes_and_reads() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Db::open(dir.path(), DbConfig::default()).unwrap());

    let mut handles = vec![];

    // 4 writer threads, 100 keys each.
    for t in 0..4u32 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..100u32 {
                let key = format!("t{}_k{:04}", t, i);
                let val = format!("t{}_v{:04}", t, i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify all 400 keys.
    for t in 0..4u32 {
        for i in 0..100u32 {
            let key = format!("t{}_k{:04}", t, i);
            let val = format!("t{}_v{:04}", t, i);
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "missing: {key}"
            );
        }
    }

    db.close().unwrap();
}

/// # Scenario
/// Reader threads observe previously-written keys while a writer thread
/// adds new keys concurrently.
///
/// # Starting environment
/// Database pre-populated with 50 keys `pre_0000..pre_0049`.
///
/// # Actions
/// 1. Spawn 1 writer adding `pre_0050..pre_0149`.
/// 2. Spawn 3 reader threads each reading all 50 pre-existing keys.
/// 3. Join all threads.
///
/// # Expected behavior
/// Readers never see a `None` for pre-existing keys — writes do not
/// interfere with concurrent reads of stable data.
#[test]
fn concurrent_reads_during_writes() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Db::open(dir.path(), DbConfig::default()).unwrap());

    // Pre-populate some keys.
    for i in 0..50u32 {
        let key = format!("pre_{:04}", i);
        let val = format!("val_{:04}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    let mut handles = vec![];

    // Writer thread adds new keys.
    {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 50..150u32 {
                let key = format!("pre_{:04}", i);
                let val = format!("val_{:04}", i);
                db.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
        }));
    }

    // Reader threads read pre-existing keys concurrently.
    for _ in 0..3 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..50u32 {
                let key = format!("pre_{:04}", i);
                let val = format!("val_{:04}", i);
                assert_eq!(
                    db.get(key.as_bytes()).unwrap(),
                    Some(val.into_bytes()),
                    "reader couldn't find {key}"
                );
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    db.close().unwrap();
}

// ================================================================================================
// Full-stack orchestration
// ================================================================================================

/// # Scenario
/// End-to-end lifecycle: bulk writes, point-deletes, range-deletes,
/// close → reopen, major compaction, and full scan verification.
///
/// # Starting environment
/// Empty directory, 1 KiB write buffer (many flushes).
///
/// # Actions
/// **Phase 1** — populate and mutate:
/// 1. Write 300 sequentially-named keys.
/// 2. Point-delete all even-indexed keys.
/// 3. Range-delete `[life_0200, life_0250)`.
/// 4. Close.
///
/// **Phase 2** — compact and verify:
/// 1. Reopen, run `major_compact()`.
/// 2. Verify each key: even → `None` (point-deleted),
///    odd in `[200..250)` → `None` (range-deleted),
///    remaining odd → original value.
/// 3. Scan all surviving keys and assert count = 125.
///
/// # Expected behavior
/// 125 odd keys outside the range-deleted interval survive.
#[test]
fn full_lifecycle_with_compaction() {
    let dir = TempDir::new().unwrap();

    // Phase 1: Write, delete, range-delete with small buffer (triggers flushes).
    {
        let db = Db::open(dir.path(), small_buffer_config()).unwrap();

        for i in 0..300u32 {
            let key = format!("life_{:04}", i);
            let val = format!("val_{:04}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Point-delete even keys.
        for i in (0..300u32).step_by(2) {
            let key = format!("life_{:04}", i);
            db.delete(key.as_bytes()).unwrap();
        }

        // Range-delete [life_0200, life_0250).
        db.delete_range(b"life_0200", b"life_0250").unwrap();

        db.close().unwrap();
    }

    // Phase 2: Reopen, major compact, verify.
    {
        let db = Db::open(dir.path(), small_buffer_config()).unwrap();
        db.major_compact().unwrap();

        for i in 0..300u32 {
            let key = format!("life_{:04}", i);
            let result = db.get(key.as_bytes()).unwrap();

            if i % 2 == 0 {
                // Even keys were point-deleted.
                assert_eq!(result, None, "{key} should be deleted (even)");
            } else if (200..250).contains(&i) {
                // Range-deleted (but odd keys in this range were NOT point-deleted,
                // they were range-deleted).
                assert_eq!(result, None, "{key} should be range-deleted");
            } else {
                let val = format!("val_{:04}", i);
                assert_eq!(result, Some(val.into_bytes()), "{key} should exist");
            }
        }

        // Scan surviving keys.
        let scan = db.scan(b"life_0000", b"life_9999").unwrap();
        // Odd keys outside [200,250) range: there are 150 odd keys total,
        // minus those in [200..250) that are odd (201,203,...,249 = 25 keys).
        let expected_count = 150 - 25;
        assert_eq!(
            scan.len(),
            expected_count,
            "scan should return {expected_count} surviving keys"
        );

        db.close().unwrap();
    }
}
