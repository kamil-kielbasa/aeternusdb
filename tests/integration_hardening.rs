//! Public API hardening tests — Priority 3.
//!
//! These tests exercise exact boundary values for every `DbConfig` field
//! (the smallest valid, smallest invalid, largest valid, largest invalid)
//! and additional edge-case error paths not covered by the base
//! integration suite.
//!
//! ## See also
//! - [`integration`] — basic config rejection, CRUD, concurrency

use aeternusdb::{Db, DbConfig, DbError};
use tempfile::TempDir;

// ================================================================================================
// DbConfig — write_buffer_size exact boundaries
// ================================================================================================

/// # Scenario
/// `write_buffer_size` at the exact minimum (1024) should be accepted.
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_write_buffer_size_exact_min_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        write_buffer_size: 1024,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// `write_buffer_size` one below the minimum (1023) is rejected.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_write_buffer_size_below_min_rejected() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        write_buffer_size: 1023,
        ..DbConfig::default()
    };
    assert!(matches!(
        Db::open(dir.path(), config).unwrap_err(),
        DbError::InvalidConfig(_)
    ));
}

/// # Scenario
/// `write_buffer_size` at the exact maximum (256 MiB = 268435456) is accepted.
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_write_buffer_size_exact_max_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        write_buffer_size: 256 * 1024 * 1024,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// `write_buffer_size` one above the maximum is rejected.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_write_buffer_size_above_max_rejected() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        write_buffer_size: 256 * 1024 * 1024 + 1,
        ..DbConfig::default()
    };
    assert!(matches!(
        Db::open(dir.path(), config).unwrap_err(),
        DbError::InvalidConfig(_)
    ));
}

// ================================================================================================
// DbConfig — min_compaction_threshold exact boundaries
// ================================================================================================

/// # Scenario
/// `min_compaction_threshold` at the exact minimum (2) is accepted.
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_min_threshold_exact_min_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        min_compaction_threshold: 2,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// `min_compaction_threshold` at the exact maximum (64) is accepted.
/// `max_compaction_threshold` must be >= min, so set it to 64 as well.
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_min_threshold_exact_max_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        min_compaction_threshold: 64,
        max_compaction_threshold: 64,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// `min_compaction_threshold` above the maximum (65) is rejected.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_min_threshold_above_max_rejected() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        min_compaction_threshold: 65,
        max_compaction_threshold: 65,
        ..DbConfig::default()
    };
    assert!(matches!(
        Db::open(dir.path(), config).unwrap_err(),
        DbError::InvalidConfig(_)
    ));
}

// ================================================================================================
// DbConfig — max_compaction_threshold exact boundaries
// ================================================================================================

/// # Scenario
/// `max_compaction_threshold` at exact max (256) is accepted.
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_max_threshold_exact_max_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        max_compaction_threshold: 256,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// `max_compaction_threshold` above 256 is rejected.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_max_threshold_above_max_rejected() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        max_compaction_threshold: 257,
        ..DbConfig::default()
    };
    assert!(matches!(
        Db::open(dir.path(), config).unwrap_err(),
        DbError::InvalidConfig(_)
    ));
}

/// # Scenario
/// `max_compaction_threshold` exactly equal to `min_compaction_threshold`
/// is accepted (tightest valid window).
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_max_equals_min_threshold_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        min_compaction_threshold: 10,
        max_compaction_threshold: 10,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

// ================================================================================================
// DbConfig — tombstone_compaction_ratio exact boundaries
// ================================================================================================

/// # Scenario
/// `tombstone_compaction_ratio` at 1.0 (upper bound inclusive) is accepted.
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_tombstone_ratio_exact_upper_bound_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        tombstone_compaction_ratio: 1.0,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// `tombstone_compaction_ratio` at a very small positive value (0.001)
/// is accepted (lower bound is exclusive 0.0).
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_tombstone_ratio_small_positive_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        tombstone_compaction_ratio: 0.001,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// `tombstone_compaction_ratio` at negative value is rejected.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_tombstone_ratio_negative_rejected() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        tombstone_compaction_ratio: -0.1,
        ..DbConfig::default()
    };
    assert!(matches!(
        Db::open(dir.path(), config).unwrap_err(),
        DbError::InvalidConfig(_)
    ));
}

// ================================================================================================
// DbConfig — tombstone_compaction_interval exact boundaries
// ================================================================================================

/// # Scenario
/// `tombstone_compaction_interval` at exact max (604800 — 7 days) is accepted.
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_tombstone_interval_exact_max_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        tombstone_compaction_interval: 604_800,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// `tombstone_compaction_interval` above max is rejected.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_tombstone_interval_above_max_rejected() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        tombstone_compaction_interval: 604_801,
        ..DbConfig::default()
    };
    assert!(matches!(
        Db::open(dir.path(), config).unwrap_err(),
        DbError::InvalidConfig(_)
    ));
}

// ================================================================================================
// DbConfig — thread_pool_size exact boundaries
// ================================================================================================

/// # Scenario
/// `thread_pool_size` at exact min (1) is accepted.
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_thread_pool_size_exact_min_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        thread_pool_size: 1,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// `thread_pool_size` at exact max (32) is accepted.
///
/// # Expected behavior
/// `Db::open` succeeds.
#[test]
fn config_thread_pool_size_exact_max_accepted() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        thread_pool_size: 32,
        ..DbConfig::default()
    };
    let db = Db::open(dir.path(), config).unwrap();
    db.close().unwrap();
}

/// # Scenario
/// `thread_pool_size` above max (33) is rejected.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidConfig(_))`.
#[test]
fn config_thread_pool_size_above_max_rejected() {
    let dir = TempDir::new().unwrap();
    let config = DbConfig {
        thread_pool_size: 33,
        ..DbConfig::default()
    };
    assert!(matches!(
        Db::open(dir.path(), config).unwrap_err(),
        DbError::InvalidConfig(_)
    ));
}

// ================================================================================================
// Public API — scan with start == end returns empty
// ================================================================================================

/// # Scenario
/// `scan` with `start == end` returns an empty vec (not an error).
///
/// # Expected behavior
/// `db.scan(b"x", b"x")` returns `Ok(vec![])`.
#[test]
fn scan_start_equals_end_returns_empty() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();
    db.put(b"x", b"v").unwrap();

    let results = db.scan(b"x", b"x").unwrap();
    assert!(results.is_empty());

    db.close().unwrap();
}

// ================================================================================================
// Public API — delete_range with empty keys
// ================================================================================================

/// # Scenario
/// `delete_range` with empty start or end key is rejected.
///
/// # Expected behavior
/// Returns `Err(DbError::InvalidArgument(_))`.
#[test]
fn delete_range_empty_keys_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    assert!(matches!(
        db.delete_range(b"", b"z"),
        Err(DbError::InvalidArgument(_))
    ));
    assert!(matches!(
        db.delete_range(b"a", b""),
        Err(DbError::InvalidArgument(_))
    ));

    db.close().unwrap();
}

// ================================================================================================
// Public API — major_compact on empty DB
// ================================================================================================

/// # Scenario
/// `major_compact()` on a database with no SSTables returns `false`.
///
/// # Expected behavior
/// Returns `Ok(false)` — nothing to compact.
#[test]
fn major_compact_empty_db_returns_false() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(dir.path(), DbConfig::default()).unwrap();

    let compacted = db.major_compact().unwrap();
    assert!(!compacted);

    db.close().unwrap();
}

// ================================================================================================
// Public API — reopen after only deletes (no live data)
// ================================================================================================

/// # Scenario
/// Write some keys, delete them all, close, reopen. All gets should
/// return `None`.
///
/// # Expected behavior
/// No data is visible after reopen.
#[test]
fn reopen_after_delete_all_keys() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::open(dir.path(), DbConfig::default()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.delete(b"a").unwrap();
        db.delete(b"b").unwrap();
        db.close().unwrap();
    }
    {
        let db = Db::open(dir.path(), DbConfig::default()).unwrap();
        assert_eq!(db.get(b"a").unwrap(), None);
        assert_eq!(db.get(b"b").unwrap(), None);
        let scan = db.scan(b"\x00", b"\xff").unwrap();
        assert!(scan.is_empty());
        db.close().unwrap();
    }
}
