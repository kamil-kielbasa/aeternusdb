//! # Memtable Module
//!
//! Implements an **in-memory key-value store** backed by a **write-ahead log (WAL)**.
//!
//! The `Memtable` is the mutable, in-memory layer of an LSM-based storage engine.
//! It accepts writes, persists them to a WAL for durability, and provides fast
//! point lookups and range scans. When the in-memory buffer exceeds a configured
//! threshold, it is flushed to immutable on-disk SSTables.
//!
//! ## Features
//! - Durable via WAL-first writes
//! - Concurrent readers with `RwLock`
//! - In-memory versioned key tracking (supports deletes)
//! - Range scans with inclusive start and exclusive end
//! - Safe recovery on restart via WAL replay
//!
//! ## Crash Safety
//!
//! Every `put` and `delete` operation is appended to the WAL before being applied
//! to the in-memory tree. Upon restart, the WAL is replayed to reconstruct the
//! last consistent memtable state.

// ------------------------------------------------------------------------------------------------
// Unit tests
// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests;

// ------------------------------------------------------------------------------------------------
// Includes
// ------------------------------------------------------------------------------------------------

use std::{
    collections::BTreeMap,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::wal::{Wal, WalError};
use thiserror::Error;
use tracing::{error, info, trace};

// ------------------------------------------------------------------------------------------------
// Constants
// ------------------------------------------------------------------------------------------------

const U32_SIZE: usize = std::mem::size_of::<u32>();

// ------------------------------------------------------------------------------------------------
// Error Types
// ------------------------------------------------------------------------------------------------

/// Represents possible errors returned by [`Memtable`] operations.
#[derive(Debug, Error)]
pub enum MemtableError {
    /// Underlying WAL I/O failure.
    #[error("WAL error: {0}")]
    WAL(#[from] WalError),

    /// Write buffer limit reached; a flush is required before further writes.
    #[error("Flush required")]
    FlushRequired,

    /// Internal invariant violation or poisoned lock.
    #[error("Internal error: {0}")]
    Internal(String),
}

// ------------------------------------------------------------------------------------------------
// Memtable Core
// ------------------------------------------------------------------------------------------------

pub struct FrozenMemtable {}

/// An in-memory, WAL-backed key-value store.
///
/// The memtable maintains key-value pairs in a sorted [`BTreeMap`], allowing
/// fast range queries. Every mutation is logged to the WAL to guarantee
/// crash recovery.
///
/// Internally, each key may have multiple [`MemtableEntry`] versions
/// (representing updates and deletes).
pub struct Memtable {
    /// Thread-safe container for in-memory data and metadata.
    inner: Arc<RwLock<MemtableInner>>,

    /// Associated write-ahead log for durability.
    wal: Wal<MemtableRecord>,

    /// Monotonic log sequence number (LSN) for version ordering.
    next_lsn: AtomicU64,
}

/// A single version of a key in the memtable.
///
/// Each entry represents either a live value or a tombstone (deletion).
#[derive(Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
pub struct MemtableEntry {
    /// The stored value. `None` indicates a deletion (tombstone).
    pub value: Option<Vec<u8>>,

    /// Logical timestamp in nanoseconds since UNIX epoch.
    pub timestamp: u64,

    /// Whether this entry represents a deletion.
    pub is_delete: bool,

    /// Log sequence number for ordering updates.
    pub lsn: u64,
}

/// A record stored in the WAL and replayed into the memtable.
///
/// Each record consists of a key and its associated versioned value entry.
#[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
struct MemtableRecord {
    /// The record key (user key).
    key: Vec<u8>,

    /// The versioned entry metadata and value.
    value: MemtableEntry,
}

/// Internal shared memtable state.
///
/// Wrapped in an `RwLock` for concurrent readers.
struct MemtableInner {
    /// Ordered key-value mapping.
    tree: BTreeMap<Vec<u8>, Vec<MemtableEntry>>,

    /// Approximate total size of all entries in memory.
    approximate_size: usize,

    /// Maximum allowed buffer size before a flush is required.
    write_buffer_size: usize,
}

impl Memtable {
    /// Creates a new [`Memtable`] instance backed by a WAL.
    ///
    /// # Arguments
    /// - `wal_path` — Path to the WAL file used for durability.
    /// - `max_record_size` — Optional per-record size limit for the WAL.
    /// - `write_buffer_size` — Maximum in-memory buffer size before requiring a flush.
    ///
    /// # Behavior
    /// On creation, the memtable replays the WAL file (if present) to restore
    /// any previously persisted state. Each recovered record is inserted into
    /// the in-memory B-tree, and the `next_lsn` is advanced accordingly.
    pub fn new<P: AsRef<Path>>(
        wal_path: P,
        max_record_size: Option<u32>,
        write_buffer_size: usize,
    ) -> Result<Self, MemtableError> {
        info!("Initializing Memtable with WAL replay");

        let wal = Wal::open(&wal_path, max_record_size)?;

        let mut inner = MemtableInner {
            tree: BTreeMap::new(),
            approximate_size: 0,
            write_buffer_size,
        };

        let mut max_lsn_seen: u64 = 0;

        let records = wal.replay_iter()?;
        for record in records {
            let record: MemtableRecord = record?;

            if record.value.lsn > max_lsn_seen {
                max_lsn_seen = record.value.lsn;
            }

            trace!("Replaying WAL record");

            let record_size = U32_SIZE + std::mem::size_of::<MemtableRecord>() + U32_SIZE;

            let key = record.key;
            let value = record.value;

            inner.tree.entry(key).or_insert_with(Vec::new).push(value);

            inner.approximate_size += record_size;
        }

        info!(
            "Memtable initialized successfully with LSN: {}",
            max_lsn_seen
        );

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            wal,
            next_lsn: AtomicU64::new(max_lsn_seen.saturating_add(1)),
        })
    }

    /// Inserts or updates a key-value pair in the memtable.
    ///
    /// # Behavior
    /// - The operation is first appended to the WAL (write-ahead).
    /// - Then, it is applied to the in-memory B-tree.
    /// - Each record is assigned a unique LSN for ordering.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemtableError> {
        trace!("put() started, key: {}", HexKey(&key));

        if key.is_empty() || value.is_empty() {
            return Err(MemtableError::Internal("Key or value is empty".to_string()));
        }

        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        let record_size = std::mem::size_of::<MemtableEntry>() + key.len() + value.len();
        let record = MemtableRecord {
            key,
            value: MemtableEntry {
                value: Some(value),
                timestamp: Self::current_timestamp(),
                is_delete: false,
                lsn,
            },
        };

        let mut guard = self.inner.write().map_err(|_| {
            error!("Read-write lock poisoned during put");
            MemtableError::Internal("Read-write lock poisoned".into())
        })?;

        if guard.approximate_size + record_size > guard.write_buffer_size {
            return Err(MemtableError::FlushRequired);
        }

        // 1. Wal first (crash safety)
        self.wal.append(&record)?;

        // 2. In-memory update
        let key = record.key;
        let value = record.value;

        guard
            .tree
            .entry(key.clone())
            .or_insert_with(Vec::new)
            .push(value);

        guard.approximate_size += record_size;

        trace!(
            "Put operation completed with LSN: {}, key: {}",
            lsn,
            HexKey(&key)
        );

        Ok(())
    }

    /// Deletes a key (inserts a tombstone entry).
    ///
    /// # Behavior
    /// A tombstone is written to the WAL and inserted in memory.
    /// The key will still exist in the B-tree but its latest version will be
    /// marked with `is_delete = true`.
    pub fn delete(&self, key: Vec<u8>) -> Result<(), MemtableError> {
        trace!("delete() started, key: {}", HexKey(&key));

        if key.is_empty() {
            return Err(MemtableError::Internal("Key is empty".to_string()));
        }

        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        let record_size = std::mem::size_of::<MemtableEntry>() + key.len();
        let record = MemtableRecord {
            key,
            value: MemtableEntry {
                value: None,
                timestamp: Self::current_timestamp(),
                is_delete: true,
                lsn,
            },
        };

        let mut guard = self.inner.write().map_err(|_| {
            error!("Read-write lock poisoned during delete");
            MemtableError::Internal("Read-write lock poisoned".into())
        })?;

        if guard.approximate_size + record_size > guard.write_buffer_size {
            return Err(MemtableError::FlushRequired);
        }

        // 1. Wal first (crash safety)
        self.wal.append(&record)?;

        // 2. In-memory update
        let key = record.key;
        let value = record.value;

        guard
            .tree
            .entry(key.clone())
            .or_insert_with(Vec::new)
            .push(value);

        guard.approximate_size += record_size;

        trace!(
            "Delete operation completed with LSN: {}, key: {}",
            lsn,
            HexKey(&key)
        );

        Ok(())
    }

    /// Retrieves the latest value for a given key.
    ///
    /// # Returns
    /// - `Ok(Some(value))` if the key exists and is not deleted.
    /// - `Ok(None)` if the key does not exist or has been deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemtableError> {
        trace!("get() started, key: {}", HexKey(key));

        let guard = self.inner.read().map_err(|_| {
            error!("Read-write lock poisoned during scan");
            MemtableError::Internal("RwLock poisoned".into())
        })?;

        let maybe_latest = guard
            .tree
            .get(key)
            .and_then(|versions| versions.iter().max_by_key(|e| e.lsn).cloned());

        Ok(maybe_latest.and_then(|e| if !e.is_delete { e.value } else { None }))
    }

    /// Scans a range of keys between `start` and `end`.
    ///
    /// # Range Semantics
    /// - **Inclusive start**, **exclusive end**
    /// - Keys are returned in sorted order.
    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<impl Iterator<Item = (Vec<u8>, MemtableEntry)>, MemtableError> {
        trace!(
            "scan() started with range. Start key: {} end key: {}",
            HexKey(start),
            HexKey(end)
        );

        if start >= end {
            return Ok(Vec::new().into_iter());
        }

        let guard = self.inner.read().map_err(|_| {
            error!("Read-write lock poisoned during scan");
            MemtableError::Internal("RwLock poisoned".into())
        })?;

        let records: Vec<_> = guard
            .tree
            .range(start.to_vec()..end.to_vec())
            .filter_map(|(key, versions)| {
                versions
                    .iter()
                    .max_by_key(|e| e.lsn)
                    .cloned()
                    .map(|latest| (key.clone(), latest))
            })
            .filter(|(_, entry)| !entry.is_delete)
            .collect();

        Ok(records.into_iter())
    }

    /// Flushes the current memtable contents, consuming all in-memory data.
    ///
    /// # Behavior
    /// - Returns an iterator over all latest (non-deleted and deleted) entries.
    /// - Clears the internal tree and resets memory usage tracking.
    ///
    /// This operation is typically followed by writing entries to
    /// an immutable SSTable on disk.
    pub fn flush(&self) -> Result<impl Iterator<Item = (Vec<u8>, MemtableEntry)>, MemtableError> {
        info!("Flushing memtable");

        let mut guard = self.inner.write().map_err(|_| {
            error!("Read-write lock poisoned during flush");
            MemtableError::Internal("Read-write lock poisoned".into())
        })?;

        let old_tree = std::mem::take(&mut guard.tree);
        guard.approximate_size = 0;

        info!("Memtable flushed successfully");

        Ok(old_tree
            .into_iter()
            .filter_map(|(key, versions)| versions.last().cloned().map(|latest| (key, latest))))
    }

    /// Returns the current system timestamp in nanoseconds.
    ///
    /// Used to tag entries for ordering and diagnostics.
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_nanos() as u64
    }
}

// ------------------------------------------------------------------------------------------------
// Tracing Helper
// ------------------------------------------------------------------------------------------------

struct HexKey<'a>(&'a [u8]);

impl<'a> std::fmt::Display for HexKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.len() <= 32 {
            for byte in self.0 {
                write!(f, "{:02x}", byte)?;
            }
        } else {
            for byte in &self.0[..16] {
                write!(f, "{:02x}", byte)?;
            }
            write!(f, "...[{} bytes]", self.0.len())?;
        }
        Ok(())
    }
}
