//! # AeternusDB
//!
//! An embeddable, persistent key-value storage engine built on a
//! **Log-Structured Merge Tree (LSM-tree)** architecture. Designed for
//! fast writes, crash safety, and automatic background compaction.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use aeternusdb::{Db, DbConfig};
//!
//! let db = Db::open("/tmp/my_db", DbConfig::default()).unwrap();
//!
//! // Write
//! db.put(b"hello", b"world").unwrap();
//!
//! // Read
//! assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
//!
//! // Delete
//! db.delete(b"hello").unwrap();
//! assert_eq!(db.get(b"hello").unwrap(), None);
//!
//! // Scan
//! db.put(b"a", b"1").unwrap();
//! db.put(b"b", b"2").unwrap();
//! let results = db.scan(b"a", b"c").unwrap();
//! assert_eq!(results.len(), 2);
//!
//! // Graceful shutdown
//! db.close().unwrap();
//! ```
//!
//! ## Features
//!
//! - **Write-ahead logging** — every mutation is persisted before acknowledgement.
//! - **Automatic compaction** — background threads merge SSTables and clean up tombstones.
//! - **Point and range deletes** — efficient tombstone-based deletion.
//! - **Bloom filters** — fast negative lookups on SSTables.
//! - **CRC32 integrity** — all on-disk blocks are checksummed.
//! - **Crash recovery** — automatic recovery from WAL on restart.

#![allow(dead_code)]

pub(crate) mod compaction;
pub(crate) mod encoding;
pub(crate) mod engine;
pub(crate) mod manifest;
pub(crate) mod memtable;
pub(crate) mod sstable;
pub(crate) mod wal;

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use engine::{Engine, EngineConfig, EngineError};
use thiserror::Error;
use tracing::{debug, error, info};

/// A single key-value pair returned by [`Db::scan`].
pub type KeyValue = (Vec<u8>, Vec<u8>);

/// Re-export the compaction strategy selector so callers can configure it
/// without reaching into internal modules.
pub use compaction::CompactionStrategyType;

// ------------------------------------------------------------------------------------------------
// Configuration
// ------------------------------------------------------------------------------------------------

/// Configuration for a [`Db`] instance.
///
/// All fields have sensible defaults via [`DbConfig::default()`].
/// The configuration is validated when passed to [`Db::open`].
///
/// # Example
///
/// ```rust
/// use aeternusdb::DbConfig;
///
/// // Use defaults (64 KiB buffer, 2 background threads)
/// let config = DbConfig::default();
///
/// // Or customize
/// let config = DbConfig {
///     write_buffer_size: 128 * 1024,
///     thread_pool_size: 4,
///     ..DbConfig::default()
/// };
/// ```
pub struct DbConfig {
    /// Maximum size of the in-memory write buffer in bytes.
    ///
    /// When the buffer is full, it is frozen and flushed to an SSTable
    /// in the background.
    ///
    /// **Bounds:** 1 024 ≤ `write_buffer_size` ≤ 268 435 456 (256 MiB).
    ///
    /// Default: `65 536` (64 KiB).
    pub write_buffer_size: usize,

    /// Compaction strategy family.
    ///
    /// Determines how SSTables are grouped and merged during minor,
    /// tombstone, and major compaction.
    ///
    /// Default: [`CompactionStrategyType::Stcs`] (Size-Tiered).
    pub compaction_strategy: CompactionStrategyType,

    /// Minimum number of similarly-sized SSTables required to trigger
    /// background minor compaction.
    ///
    /// **Bounds:** 2 ≤ `min_compaction_threshold` ≤ 64.
    ///
    /// Default: `4`.
    pub min_compaction_threshold: usize,

    /// Maximum number of SSTables to merge in a single minor compaction.
    ///
    /// **Bounds:** `min_compaction_threshold` ≤ `max_compaction_threshold` ≤ 256.
    ///
    /// Default: `32`.
    pub max_compaction_threshold: usize,

    /// Tombstone-to-total-record ratio that triggers background tombstone
    /// compaction on an SSTable.
    ///
    /// **Bounds:** 0.0 < `tombstone_compaction_ratio` ≤ 1.0.
    ///
    /// Default: `0.3`.
    pub tombstone_compaction_ratio: f64,

    /// Minimum SSTable age (in seconds) before it is eligible for
    /// tombstone compaction. Set to `0` to disable the age gate.
    ///
    /// **Bounds:** 0 ≤ `tombstone_compaction_interval` ≤ 604 800 (7 days).
    ///
    /// Default: `0` (no minimum age).
    pub tombstone_compaction_interval: usize,

    /// When `true`, tombstone compaction resolves bloom filter false
    /// positives by performing an actual `get()` against older SSTables
    /// for each point tombstone. This enables more aggressive tombstone
    /// removal at the cost of extra I/O.
    ///
    /// Default: `true`.
    pub tombstone_bloom_fallback: bool,

    /// When `true`, tombstone compaction scans older SSTables to check
    /// whether a range tombstone still covers any live keys, allowing
    /// aggressive range tombstone removal.
    ///
    /// Default: `true`.
    pub tombstone_range_drop: bool,

    /// Number of background worker threads for flushing and compaction.
    ///
    /// **Bounds:** 1 ≤ `thread_pool_size` ≤ 32.
    ///
    /// Default: `2`.
    pub thread_pool_size: usize,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: 64 * 1024,
            compaction_strategy: CompactionStrategyType::Stcs,
            min_compaction_threshold: 4,
            max_compaction_threshold: 32,
            tombstone_compaction_ratio: 0.3,
            tombstone_compaction_interval: 0,
            tombstone_bloom_fallback: true,
            tombstone_range_drop: true,
            thread_pool_size: 2,
        }
    }
}

impl DbConfig {
    /// Validates all configuration parameters against their documented bounds.
    fn validate(&self) -> Result<(), DbError> {
        if self.write_buffer_size < 1024 || self.write_buffer_size > 256 * 1024 * 1024 {
            return Err(DbError::InvalidConfig(
                "write_buffer_size must be in [1024, 268435456]".into(),
            ));
        }
        if self.min_compaction_threshold < 2 || self.min_compaction_threshold > 64 {
            return Err(DbError::InvalidConfig(
                "min_compaction_threshold must be in [2, 64]".into(),
            ));
        }
        if self.max_compaction_threshold < self.min_compaction_threshold
            || self.max_compaction_threshold > 256
        {
            return Err(DbError::InvalidConfig(
                "max_compaction_threshold must be in [min_compaction_threshold, 256]".into(),
            ));
        }
        if self.tombstone_compaction_ratio <= 0.0 || self.tombstone_compaction_ratio > 1.0 {
            return Err(DbError::InvalidConfig(
                "tombstone_compaction_ratio must be in (0.0, 1.0]".into(),
            ));
        }
        if self.tombstone_compaction_interval > 604_800 {
            return Err(DbError::InvalidConfig(
                "tombstone_compaction_interval must be in [0, 604800]".into(),
            ));
        }
        if self.thread_pool_size < 1 || self.thread_pool_size > 32 {
            return Err(DbError::InvalidConfig(
                "thread_pool_size must be in [1, 32]".into(),
            ));
        }
        Ok(())
    }

    /// Converts to the internal engine configuration.
    fn to_engine_config(&self) -> EngineConfig {
        EngineConfig {
            write_buffer_size: self.write_buffer_size,
            compaction_strategy: self.compaction_strategy,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 50,
            min_threshold: self.min_compaction_threshold,
            max_threshold: self.max_compaction_threshold,
            tombstone_ratio_threshold: self.tombstone_compaction_ratio,
            tombstone_compaction_interval: self.tombstone_compaction_interval,
            tombstone_bloom_fallback: self.tombstone_bloom_fallback,
            tombstone_range_drop: self.tombstone_range_drop,
            thread_pool_size: self.thread_pool_size,
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Error type
// ------------------------------------------------------------------------------------------------

/// Errors returned by [`Db`] operations.
#[derive(Debug, Error)]
pub enum DbError {
    /// The database has been closed.
    #[error("database is closed")]
    Closed,

    /// Invalid configuration parameter.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Key or value constraint violated.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// An engine-internal error occurred.
    #[error("{0}")]
    Engine(#[from] EngineError),
}

// ------------------------------------------------------------------------------------------------
// Background worker state
// ------------------------------------------------------------------------------------------------

/// Holds the thread pool sender and worker handles.
/// Taken (`Option::take`) on shutdown to ensure single cleanup.
struct BackgroundPool {
    sender: crossbeam::channel::Sender<Box<dyn FnOnce() + Send>>,
    workers: Vec<thread::JoinHandle<()>>,
}

// ------------------------------------------------------------------------------------------------
// Database handle
// ------------------------------------------------------------------------------------------------

/// The main database handle.
///
/// Provides a high-level, thread-safe API for reading and writing
/// key-value pairs with automatic background flushing and compaction.
///
/// # Thread safety
///
/// `Db` is `Send + Sync` — it can be shared across threads via
/// `Arc<Db>`.
///
/// # Background compaction
///
/// When the write buffer fills, the active memtable is frozen and a
/// background task is dispatched to:
///
/// 1. Flush the frozen memtable to a new SSTable.
/// 2. Run minor compaction if size-tiered thresholds are met.
/// 3. Run tombstone compaction if the tombstone ratio is high enough.
///
/// Major compaction must be triggered explicitly via [`Db::major_compact`].
///
/// # Shutdown
///
/// Call [`Db::close`] for a graceful shutdown. If the handle is dropped
/// without calling `close`, the destructor will attempt cleanup, but
/// errors are silently ignored.
pub struct Db {
    engine: Engine,
    bg: Mutex<Option<BackgroundPool>>,
    closed: AtomicBool,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Db {
    /// Opens (or creates) a database at the given directory.
    ///
    /// On a fresh directory the required sub-directories are created
    /// automatically. On an existing directory, the manifest and WALs
    /// are replayed to recover the last durable state.
    ///
    /// # Errors
    ///
    /// - [`DbError::InvalidConfig`] — a configuration parameter is out of
    ///   its documented bounds.
    /// - [`DbError::Engine`] — the directory could not be created, the
    ///   manifest/WAL could not be opened or replayed, or I/O failed
    ///   during recovery.
    pub fn open(path: impl AsRef<Path>, config: DbConfig) -> Result<Self, DbError> {
        config.validate()?;

        let pool_size = config.thread_pool_size;
        let engine_config = config.to_engine_config();
        let engine = Engine::open(&path, engine_config)?;

        // Spawn background worker thread pool.
        let (sender, receiver) = crossbeam::channel::unbounded::<Box<dyn FnOnce() + Send>>();

        let mut workers = Vec::with_capacity(pool_size);
        for id in 0..pool_size {
            let rx = receiver.clone();
            let handle = thread::Builder::new()
                .name(format!("aeternusdb-bg-{id}"))
                .spawn(move || {
                    while let Ok(task) = rx.recv() {
                        task();
                    }
                })
                .expect("failed to spawn background thread");
            workers.push(handle);
        }
        // Workers hold their own receiver clones; drop ours.
        drop(receiver);

        info!(path = %path.as_ref().display(), pool_size, "database opened");

        Ok(Self {
            engine,
            bg: Mutex::new(Some(BackgroundPool { sender, workers })),
            closed: AtomicBool::new(false),
        })
    }

    /// Gracefully shuts down the database.
    ///
    /// Waits for all in-flight background tasks to complete, flushes
    /// remaining frozen memtables, checkpoints the manifest, and
    /// fsyncs all directories.
    ///
    /// Subsequent operations on this handle return [`DbError::Closed`].
    /// Calling `close` more than once is harmless.
    ///
    /// # Errors
    ///
    /// - [`DbError::Engine`] — flush or manifest checkpoint failed
    ///   during shutdown.
    pub fn close(&self) -> Result<(), DbError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(()); // Already closed.
        }

        self.shutdown_pool();
        self.engine.close()?;

        info!("database closed");
        Ok(())
    }

    // --------------------------------------------------------------------------------------------
    // Write operations
    // --------------------------------------------------------------------------------------------

    /// Inserts or updates a key-value pair.
    ///
    /// The write is persisted to the WAL before being applied in memory.
    /// If the write buffer is full, the active memtable is frozen and a
    /// background flush is scheduled automatically.
    ///
    /// # Errors
    ///
    /// - [`DbError::Closed`] — the database has been closed.
    /// - [`DbError::InvalidArgument`] — `key` or `value` is empty.
    /// - [`DbError::Engine`] — WAL write or memtable operation failed.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), DbError> {
        self.check_open()?;

        if key.is_empty() {
            return Err(DbError::InvalidArgument("key must not be empty".into()));
        }
        if value.is_empty() {
            return Err(DbError::InvalidArgument("value must not be empty".into()));
        }

        let frozen = self.engine.put(key.to_vec(), value.to_vec())?;
        if frozen {
            self.schedule_flush();
        }
        Ok(())
    }

    /// Deletes a key by inserting a point tombstone.
    ///
    /// Subsequent reads return `None` until a new value is written.
    ///
    /// # Errors
    ///
    /// - [`DbError::Closed`] — the database has been closed.
    /// - [`DbError::InvalidArgument`] — `key` is empty.
    /// - [`DbError::Engine`] — WAL write or memtable operation failed.
    pub fn delete(&self, key: &[u8]) -> Result<(), DbError> {
        self.check_open()?;

        if key.is_empty() {
            return Err(DbError::InvalidArgument("key must not be empty".into()));
        }

        let frozen = self.engine.delete(key.to_vec())?;
        if frozen {
            self.schedule_flush();
        }
        Ok(())
    }

    /// Deletes all keys in the half-open range `[start, end)`.
    ///
    /// # Errors
    ///
    /// - [`DbError::Closed`] — the database has been closed.
    /// - [`DbError::InvalidArgument`] — `start` or `end` is empty, or
    ///   `start >= end`.
    /// - [`DbError::Engine`] — WAL write or memtable operation failed.
    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), DbError> {
        self.check_open()?;

        if start.is_empty() || end.is_empty() {
            return Err(DbError::InvalidArgument(
                "start and end keys must not be empty".into(),
            ));
        }
        if start >= end {
            return Err(DbError::InvalidArgument(
                "start must be less than end".into(),
            ));
        }

        let frozen = self.engine.delete_range(start.to_vec(), end.to_vec())?;
        if frozen {
            self.schedule_flush();
        }
        Ok(())
    }

    // --------------------------------------------------------------------------------------------
    // Read operations
    // --------------------------------------------------------------------------------------------

    /// Retrieves the value associated with a key.
    ///
    /// Returns `Ok(None)` if the key does not exist or has been deleted.
    ///
    /// # Errors
    ///
    /// - [`DbError::Closed`] — the database has been closed.
    /// - [`DbError::InvalidArgument`] — `key` is empty.
    /// - [`DbError::Engine`] — SSTable read or I/O failed.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DbError> {
        self.check_open()?;

        if key.is_empty() {
            return Err(DbError::InvalidArgument("key must not be empty".into()));
        }

        Ok(self.engine.get(key.to_vec())?)
    }

    /// Scans all live key-value pairs in the half-open range `[start, end)`.
    ///
    /// Returns pairs sorted by key in ascending order. Deleted keys
    /// are excluded.
    ///
    /// Returns an empty `Vec` if the range contains no live keys.
    ///
    /// # Errors
    ///
    /// - [`DbError::Closed`] — the database has been closed.
    /// - [`DbError::InvalidArgument`] — `start` or `end` is empty.
    /// - [`DbError::Engine`] — SSTable read or I/O failed.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<KeyValue>, DbError> {
        self.check_open()?;

        if start.is_empty() || end.is_empty() {
            return Err(DbError::InvalidArgument(
                "start and end keys must not be empty".into(),
            ));
        }
        if start >= end {
            return Ok(Vec::new());
        }

        let results: Vec<_> = self.engine.scan(start, end)?.collect();
        Ok(results)
    }

    // --------------------------------------------------------------------------------------------
    // Compaction
    // --------------------------------------------------------------------------------------------

    /// Runs a full **major compaction**, merging all SSTables into one.
    ///
    /// This is a **blocking** operation. All range tombstones are applied
    /// and all spent tombstones are dropped from the output.
    ///
    /// Returns `true` if compaction was performed, `false` if there
    /// were fewer than 2 SSTables.
    ///
    /// # Errors
    ///
    /// - [`DbError::Closed`] — the database has been closed.
    /// - [`DbError::Engine`] — SSTable merge, manifest update, or I/O
    ///   failed during compaction.
    pub fn major_compact(&self) -> Result<bool, DbError> {
        self.check_open()?;
        Ok(self.engine.major_compact()?)
    }

    // --------------------------------------------------------------------------------------------
    // Internal helpers
    // --------------------------------------------------------------------------------------------

    /// Returns `Err(DbError::Closed)` if the database has been closed.
    fn check_open(&self) -> Result<(), DbError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DbError::Closed);
        }
        Ok(())
    }

    /// Dispatches a background task to flush the oldest frozen memtable
    /// and run minor + tombstone compaction.
    fn schedule_flush(&self) {
        let guard = self.bg.lock().unwrap();
        if let Some(bg) = guard.as_ref() {
            let engine = self.engine.clone();
            let _ = bg.sender.send(Box::new(move || {
                // 1. Flush oldest frozen memtable to SSTable.
                match engine.flush_oldest_frozen() {
                    Ok(true) => debug!("background: flushed frozen memtable"),
                    Ok(false) => return,
                    Err(e) => {
                        error!("background flush failed: {e}");
                        return;
                    }
                }

                // 2. Minor compaction — loop until no bucket meets threshold.
                loop {
                    match engine.minor_compact() {
                        Ok(true) => debug!("background: minor compaction round"),
                        Ok(false) => break,
                        Err(e) => {
                            error!("background minor compaction failed: {e}");
                            break;
                        }
                    }
                }

                // 3. Tombstone compaction — single pass.
                match engine.tombstone_compact() {
                    Ok(true) => debug!("background: tombstone compaction"),
                    Ok(false) => {}
                    Err(e) => {
                        error!("background tombstone compaction failed: {e}");
                    }
                }
            }));
        }
    }

    /// Drains the background task queue and joins all worker threads.
    fn shutdown_pool(&self) {
        if let Some(bg) = self.bg.lock().unwrap().take() {
            // Drop sender → workers drain remaining tasks then exit.
            drop(bg.sender);
            for worker in bg.workers {
                let _ = worker.join();
            }
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::Acquire) {
            self.shutdown_pool();
            let _ = self.engine.close();
        }
    }
}
