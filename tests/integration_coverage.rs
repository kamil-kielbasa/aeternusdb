//! Extra integration tests targeting uncovered code paths in `lib.rs`.
//!
//! These tests exercise:
//! - `Db::Debug` impl
//! - `Drop`-based cleanup (no explicit `close()`)
//! - `schedule_flush` background path (flush + minor + tombstone)
//! - `delete_range` freeze trigger
//! - Config validation edge cases not yet covered

use aeternusdb::{Db, DbConfig, DbError};
use tempfile::TempDir;

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

/// Tiny buffer to force frequent flushes.
fn tiny_config() -> DbConfig {
    DbConfig {
        write_buffer_size: 1024,
        min_compaction_threshold: 2,
        max_compaction_threshold: 4,
        tombstone_compaction_ratio: 0.1,
        thread_pool_size: 2,
        ..DbConfig::default()
    }
}

// ================================================================================================
// Debug impl
// ================================================================================================

/// Verify the `Debug` impl on `Db` outputs expected fields.
#[test]
fn db_debug_impl() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open(tmp.path(), DbConfig::default()).unwrap();

    let debug_str = format!("{db:?}");
    assert!(debug_str.contains("Db"), "should contain struct name");
    assert!(debug_str.contains("closed"), "should contain closed field");
    assert!(debug_str.contains("false"), "should show closed = false");

    db.close().unwrap();

    // After close the handle is consumed, so we can't re-check.
    // But the pre-close Debug was exercised.
}

// ================================================================================================
// Drop-based cleanup (no explicit close)
// ================================================================================================

/// Open a database, write data, then drop without calling `close()`.
/// Reopen and verify data is durable.
#[test]
fn drop_without_close_is_safe() {
    let tmp = TempDir::new().unwrap();

    // Phase 1: write and drop (no close)
    {
        let db = Db::open(tmp.path(), DbConfig::default()).unwrap();
        db.put(b"key1", b"val1").unwrap();
        db.put(b"key2", b"val2").unwrap();
        // Drop runs: shutdown_pool → engine.close()
    }

    // Phase 2: reopen and verify
    {
        let db = Db::open(tmp.path(), DbConfig::default()).unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(db.get(b"key2").unwrap(), Some(b"val2".to_vec()));
        db.close().unwrap();
    }
}

// ================================================================================================
// Background flush path — schedule_flush exercises the full cycle
// ================================================================================================

/// Write enough data to trigger multiple background flushes, which
/// exercises `schedule_flush` → `flush_oldest_frozen` → `minor_compact`
/// → `tombstone_compact` in the background pool.
#[test]
fn background_flush_cycle() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open(tmp.path(), tiny_config()).unwrap();

    // Write enough to trigger multiple freezes
    for i in 0..200u32 {
        let key = format!("k{i:04}");
        let val = format!("v{i:04}");
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Delete some keys to create tombstones
    for i in 0..100u32 {
        let key = format!("k{i:04}");
        db.delete(key.as_bytes()).unwrap();
    }

    // Write more to flush the tombstones
    for i in 200..300u32 {
        let key = format!("k{i:04}");
        let val = format!("v{i:04}");
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Give background pool time to process
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify data integrity
    for i in 100..300u32 {
        let key = format!("k{i:04}");
        let val = format!("v{i:04}");
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(val.into_bytes()),
            "key {key} should exist"
        );
    }

    db.close().unwrap();
}

// ================================================================================================
// delete_range triggering freeze
// ================================================================================================

/// Use a tiny write buffer plus a large range delete to trigger
/// the freeze → `schedule_flush` path through `delete_range`.
#[test]
fn delete_range_triggers_flush() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open(tmp.path(), tiny_config()).unwrap();

    // Fill the buffer to near capacity
    for i in 0..50u32 {
        let key = format!("r{i:04}");
        let val = format!("v{i:04}");
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    // Range delete should push over the buffer size limit
    db.delete_range(b"r0000", b"r0050").unwrap();

    // Wait for background flush
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Verify deletions
    for i in 0..50u32 {
        let key = format!("r{i:04}");
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            None,
            "{key} should be deleted"
        );
    }

    db.close().unwrap();
}

// ================================================================================================
// Config validation edge cases
// ================================================================================================

/// `tombstone_compaction_interval` at max boundary (604_800 = 7 days).
#[test]
fn config_tombstone_interval_at_max() {
    let tmp = TempDir::new().unwrap();
    let config = DbConfig {
        tombstone_compaction_interval: 604_800,
        ..DbConfig::default()
    };
    let db = Db::open(tmp.path(), config).unwrap();
    db.close().unwrap();
}

/// `tombstone_compaction_interval` over max is rejected.
#[test]
fn config_tombstone_interval_over_max() {
    let tmp = TempDir::new().unwrap();
    let config = DbConfig {
        tombstone_compaction_interval: 604_801,
        ..DbConfig::default()
    };
    let err = Db::open(tmp.path(), config).unwrap_err();
    assert!(matches!(err, DbError::InvalidConfig(_)));
}

/// `tombstone_compaction_ratio` at boundary (exactly 1.0 is valid).
#[test]
fn config_tombstone_ratio_at_one() {
    let tmp = TempDir::new().unwrap();
    let config = DbConfig {
        tombstone_compaction_ratio: 1.0,
        ..DbConfig::default()
    };
    let db = Db::open(tmp.path(), config).unwrap();
    db.close().unwrap();
}

/// `tombstone_compaction_ratio` at zero is invalid (must be > 0).
#[test]
fn config_tombstone_ratio_at_zero() {
    let tmp = TempDir::new().unwrap();
    let config = DbConfig {
        tombstone_compaction_ratio: 0.0,
        ..DbConfig::default()
    };
    let err = Db::open(tmp.path(), config).unwrap_err();
    assert!(matches!(err, DbError::InvalidConfig(_)));
}

/// `thread_pool_size` at maximum (32).
#[test]
fn config_thread_pool_max() {
    let tmp = TempDir::new().unwrap();
    let config = DbConfig {
        thread_pool_size: 32,
        ..DbConfig::default()
    };
    let db = Db::open(tmp.path(), config).unwrap();
    db.close().unwrap();
}

/// `thread_pool_size` over max (33) is rejected.
#[test]
fn config_thread_pool_over_max() {
    let tmp = TempDir::new().unwrap();
    let config = DbConfig {
        thread_pool_size: 33,
        ..DbConfig::default()
    };
    let err = Db::open(tmp.path(), config).unwrap_err();
    assert!(matches!(err, DbError::InvalidConfig(_)));
}

/// `write_buffer_size` at minimum (1024) is valid.
#[test]
fn config_write_buffer_at_min() {
    let tmp = TempDir::new().unwrap();
    let config = DbConfig {
        write_buffer_size: 1024,
        ..DbConfig::default()
    };
    let db = Db::open(tmp.path(), config).unwrap();
    db.close().unwrap();
}

/// `write_buffer_size` below minimum (1023) is rejected.
#[test]
fn config_write_buffer_below_min() {
    let tmp = TempDir::new().unwrap();
    let config = DbConfig {
        write_buffer_size: 1023,
        ..DbConfig::default()
    };
    let err = Db::open(tmp.path(), config).unwrap_err();
    assert!(matches!(err, DbError::InvalidConfig(_)));
}
