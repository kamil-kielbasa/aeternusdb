//! # Memtable Module
//!
//! ## Design Invariants
//!
//! - All writes are WAL-first and assigned a monotonically increasing LSN.
//! - The memtable may contain multiple versions per key; the highest-LSN
//!   version is considered authoritative.
//! - Deletes are represented via tombstones, not physical removal.
//! - Range tombstones logically delete all keys in `[start, end)`
//!   with lower LSNs.
//! - Reads (`get`, `scan`) always resolve point entries against
//!   range tombstones.
//!
//! ## Flush Semantics
//!
//! - `iter_for_flush` returns a *logical snapshot* of the memtable state.
//! - Returned records are sufficient to reconstruct the same memtable
//!   state via WAL replay.
//! - Flush iteration does **not** mutate or clear in-memory state.
//!
//! ## Frozen Memtable
//!
//! - A `FrozenMemtable` is read-only.
//! - It retains ownership of the WAL to guarantee durability until
//!   data is persisted to SSTables.

// ------------------------------------------------------------------------------------------------
// Unit tests
// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests;

// ------------------------------------------------------------------------------------------------
// Includes
// ------------------------------------------------------------------------------------------------

use std::{
    cmp::Reverse,
    collections::BTreeMap,
    os::unix::raw::pid_t,
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use crate::wal::{Wal, WalError};
use thiserror::Error;
use tracing::{error, info, trace};

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

/// The mutable, in-memory write buffer of the storage engine.
///
/// The memtable:
/// - Accepts writes (`put`, `delete`, `delete_range`)
/// - Persists all mutations to a WAL
/// - Serves reads (`get`, `scan`)
/// - Can be logically flushed via `iter_for_flush`
///
/// Internally, the memtable stores **multiple versions per key** ordered
/// by descending LSN. Resolution is deferred to read time.
///
/// # Concurrency
/// - Writers acquire an exclusive lock
/// - Readers may proceed concurrently
///
/// # Durability
/// - Every mutation is appended to the WAL *before* being applied in memory
pub struct Memtable {
    /// Thread-safe container for in-memory data and metadata.
    inner: Arc<RwLock<MemtableInner>>,

    /// Associated write-ahead log for durability.
    pub wal: Wal<MemtableRecord>,

    /// Monotonic log sequence number (LSN) for version ordering.
    next_lsn: AtomicU64,
}

/// A single versioned point entry stored in the memtable.
///
/// A key may have multiple `MemtableSingleEntry` versions, ordered by LSN.
/// The highest-LSN entry is considered the latest.
///
/// Deletions are represented by tombstones (`is_delete = true`).
#[derive(Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
pub struct MemtableSingleEntry {
    /// The stored value. `None` indicates a deletion (tombstone).
    pub value: Option<Vec<u8>>,

    /// Logical timestamp in nanoseconds since UNIX epoch.
    pub timestamp: u64,

    /// Whether this entry represents a deletion.
    pub is_delete: bool,

    /// Log sequence number for ordering updates.
    pub lsn: u64,
}

/// A range tombstone that logically deletes keys in `[start, end)`.
///
/// Range tombstones are versioned via LSN and may overlap.
/// During reads, the highest-LSN tombstone covering a key
/// takes precedence.
#[derive(Clone, Debug, bincode::Encode, bincode::Decode)]
pub struct MemtableRangeTombstone {
    /// Inclusive start key of the deleted range.
    pub start: Vec<u8>,

    /// Exclusive end key of the deleted range.
    pub end: Vec<u8>,

    /// Log Sequence Number of this tombstone.
    pub lsn: u64,

    /// Timestamp associated with this mutation.
    pub timestamp: u64,
}

/// A logical WAL record representing a memtable mutation.
///
/// These records:
/// - Are appended to the WAL
/// - Are replayed during recovery
/// - Are emitted during memtable flush
///
/// Together, they form a complete, replayable history.
#[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub enum MemtableRecord {
    /// Insert or update a single key.
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lsn: u64,
        timestamp: u64,
    },

    /// Delete a single key.
    Delete {
        key: Vec<u8>,
        lsn: u64,
        timestamp: u64,
    },

    /// Delete all keys in `[start, end)`.
    RangeDelete {
        start: Vec<u8>,
        end: Vec<u8>,
        lsn: u64,
        timestamp: u64,
    },
}

/// Result of a `get` operation on the memtable.
#[derive(Debug, PartialEq)]
pub enum MemtableGetResult {
    /// Value found for the key.
    Put(Vec<u8>),

    /// Key was deleted by a point tombstone.
    Delete,

    /// Key was deleted by a range tombstone.
    RangeDelete,

    /// Key not found in the memtable.
    NotFound,
}

/// Internal shared state of the memtable.
///
/// This structure is protected by an `RwLock` and must never be
/// accessed directly outside the memtable implementation.
struct MemtableInner {
    /// Point entries grouped by key, then ordered by descending LSN.
    tree: BTreeMap<Vec<u8>, BTreeMap<Reverse<u64>, MemtableSingleEntry>>,

    /// Range tombstones indexed by start key and ordered by descending LSN.
    range_tombstones: BTreeMap<Vec<u8>, BTreeMap<Reverse<u64>, MemtableRangeTombstone>>,

    /// Approximate in-memory footprint.
    approximate_size: usize,

    /// Configured maximum buffer size before flush is required.
    write_buffer_size: usize,
}

impl Memtable {
    /// Creates a new mutable [`Memtable`] backed by a write-ahead log (WAL).
    ///
    /// # Arguments
    /// - `wal_path` — Path to the WAL file used for durability.
    /// - `max_record_size` — Optional maximum size of a single WAL record.
    /// - `write_buffer_size` — Maximum in-memory size before a flush is required.
    ///
    /// # Behavior
    /// - Replays the WAL (if present) to reconstruct the in-memory state.
    /// - Restores the highest observed LSN and advances the internal counter.
    /// - Subsequent writes will continue with monotonically increasing LSNs.
    ///
    /// # Crash Safety
    /// WAL replay guarantees recovery to the last durable state after a crash.
    pub fn new<P: AsRef<Path>>(
        wal_path: P,
        max_record_size: Option<u32>,
        write_buffer_size: usize,
    ) -> Result<Self, MemtableError> {
        info!("Initializing Memtable with WAL replay");

        let wal = Wal::open(&wal_path, max_record_size)?;

        let mut inner = MemtableInner {
            tree: BTreeMap::new(),
            range_tombstones: BTreeMap::new(),
            approximate_size: 0,
            write_buffer_size,
        };

        let mut max_lsn_seen: u64 = 0;

        let records = wal.replay_iter()?;
        for record in records {
            let record: MemtableRecord = record?;

            match record {
                MemtableRecord::Put {
                    key,
                    value,
                    lsn,
                    timestamp,
                } => {
                    let record_size =
                        std::mem::size_of::<MemtableSingleEntry>() + key.len() + value.len();
                    inner.approximate_size += record_size;

                    if lsn > max_lsn_seen {
                        max_lsn_seen = lsn;
                    }

                    let record_value = MemtableSingleEntry {
                        value: Some(value),
                        timestamp,
                        is_delete: false,
                        lsn,
                    };

                    inner
                        .tree
                        .entry(key)
                        .or_insert_with(BTreeMap::new)
                        .insert(Reverse(record_value.lsn), record_value);
                }

                MemtableRecord::Delete {
                    key,
                    lsn,
                    timestamp,
                } => {
                    let record_size = std::mem::size_of::<MemtableSingleEntry>() + key.len();
                    inner.approximate_size += record_size;

                    if lsn > max_lsn_seen {
                        max_lsn_seen = lsn;
                    }

                    let record_value = MemtableSingleEntry {
                        value: None,
                        timestamp,
                        is_delete: true,
                        lsn,
                    };

                    inner
                        .tree
                        .entry(key)
                        .or_insert_with(BTreeMap::new)
                        .insert(Reverse(record_value.lsn), record_value);
                }

                MemtableRecord::RangeDelete {
                    start,
                    end,
                    lsn,
                    timestamp,
                } => {
                    let record_size =
                        std::mem::size_of::<MemtableRangeTombstone>() + start.len() + end.len();
                    inner.approximate_size += record_size;

                    if lsn > max_lsn_seen {
                        max_lsn_seen = lsn;
                    }

                    let record_value = MemtableRangeTombstone {
                        start,
                        end,
                        lsn,
                        timestamp,
                    };

                    inner
                        .range_tombstones
                        .entry(record_value.start.clone())
                        .or_insert_with(BTreeMap::new)
                        .insert(Reverse(record_value.lsn), record_value);
                }
            }
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

    /// Inserts or updates a key with a new value.
    ///
    /// # Behavior
    /// - The mutation is first appended to the WAL (write-ahead).
    /// - The entry is then applied to the in-memory balanced tree.
    /// - A unique, monotonically increasing LSN is assigned.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemtableError> {
        trace!("put() started, key: {}", HexKey(&key));

        if key.is_empty() || value.is_empty() {
            return Err(MemtableError::Internal("Key or value is empty".to_string()));
        }

        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let timestamp = Self::current_timestamp();

        let record_size = std::mem::size_of::<MemtableSingleEntry>() + key.len() + value.len();
        let record = MemtableRecord::Put {
            key: key.clone(),
            value: value.clone(),
            timestamp,
            lsn,
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
        let value = MemtableSingleEntry {
            value: Some(value),
            timestamp,
            is_delete: false,
            lsn,
        };

        guard
            .tree
            .entry(key.clone())
            .or_insert_with(BTreeMap::new)
            .insert(Reverse(value.lsn), value);

        guard.approximate_size += record_size;

        trace!(
            "Put operation completed with LSN: {}, key: {}",
            lsn,
            HexKey(&key)
        );

        Ok(())
    }

    /// Deletes a key by inserting a tombstone entry.
    ///
    /// # Behavior
    /// - Writes a delete record to the WAL.
    /// - Inserts a tombstone with a higher LSN than any previous value.
    /// - The key remains in the memtable but resolves to `None`.
    pub fn delete(&self, key: Vec<u8>) -> Result<(), MemtableError> {
        trace!("delete() started, key: {}", HexKey(&key));

        if key.is_empty() {
            return Err(MemtableError::Internal("Key is empty".to_string()));
        }

        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let timestamp = Self::current_timestamp();

        let record_size = std::mem::size_of::<MemtableSingleEntry>() + key.len();
        let record = MemtableRecord::Delete {
            key: key.clone(),
            lsn,
            timestamp,
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
        let value = MemtableSingleEntry {
            value: None,
            timestamp,
            is_delete: true,
            lsn,
        };

        guard
            .tree
            .entry(key.clone())
            .or_insert_with(BTreeMap::new)
            .insert(Reverse(value.lsn), value);

        guard.approximate_size += record_size;

        trace!(
            "Delete operation completed with LSN: {}, key: {}",
            lsn,
            HexKey(&key)
        );

        Ok(())
    }

    /// Deletes all keys in the range `[start, end)`.
    ///
    /// # Range Semantics
    /// - Inclusive `start`
    /// - Exclusive `end`
    ///
    /// # Behavior
    /// - Writes a range tombstone to the WAL.
    /// - The tombstone shadows point entries with lower LSNs.
    pub fn delete_range(&self, start: Vec<u8>, end: Vec<u8>) -> Result<(), MemtableError> {
        trace!(
            "delete_range() started, start key: {}, end key: {}",
            HexKey(&start),
            HexKey(&end)
        );

        if start.is_empty() || end.is_empty() {
            return Err(MemtableError::Internal(
                "Start or end key is empty".to_string(),
            ));
        }

        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let timestamp = Self::current_timestamp();

        let record_size = std::mem::size_of::<MemtableRangeTombstone>() + start.len() + end.len();
        let record = MemtableRecord::RangeDelete {
            start: start.clone(),
            end: end.clone(),
            lsn,
            timestamp,
        };

        let mut guard = self.inner.write().unwrap();

        if guard.approximate_size + record_size > guard.write_buffer_size {
            return Err(MemtableError::FlushRequired);
        }

        // 1. Wal first (crash safety)
        self.wal.append(&record)?;

        // 2. In-memory update
        let value = MemtableRangeTombstone {
            start: start.to_vec(),
            end: end.to_vec(),
            lsn,
            timestamp,
        };

        guard
            .range_tombstones
            .entry(start.to_vec())
            .or_insert_with(BTreeMap::new)
            .insert(Reverse(value.lsn), value);

        guard.approximate_size += record_size;

        trace!(
            "Delete operation completed with LSN: {}, start key: {}, end key: {}",
            lsn,
            HexKey(&start),
            HexKey(&end),
        );

        Ok(())
    }

    /// Retrieves the latest visible value for a key.
    ///
    /// Resolution rules:
    /// 1. Select highest-LSN point entry
    /// 2. Check all covering range tombstones
    /// 3. If a tombstone has a higher LSN, the key is considered deleted
    ///
    /// # Returns
    /// - `Ok(Some(value))` if visible
    /// - `Ok(None)` if deleted or not present
    pub fn get(&self, key: &[u8]) -> Result<MemtableGetResult, MemtableError> {
        trace!("get() started, key: {}", HexKey(key));

        let guard = self.inner.read().map_err(|_| {
            error!("Read-write lock poisoned during scan");
            MemtableError::Internal("RwLock poisoned".into())
        })?;

        // Check if key exists as a point entry
        let point_opt = guard
            .tree
            .get(key)
            .and_then(|versions| versions.values().next());

        // Check if key matches any range tombstones
        let mut covering_tombstone_lsn: Option<u64> = None;
        for (_start, versions) in guard.range_tombstones.range(..=key.to_vec()) {
            if let Some(tombstone) = versions.values().next() {
                if tombstone.start.as_slice() <= key && key < tombstone.end.as_slice() {
                    covering_tombstone_lsn = Some(
                        covering_tombstone_lsn
                            .map(|lsn| lsn.max(tombstone.lsn))
                            .unwrap_or(tombstone.lsn),
                    );
                }
            }
        }

        match (point_opt, covering_tombstone_lsn) {
            // No point entry and no tombstone → key not found
            (None, None) => Ok(MemtableGetResult::NotFound),

            // No point entry but covered by range tombstone
            (None, Some(_)) => Ok(MemtableGetResult::RangeDelete),

            // Point entry exists, no covering tombstone
            (Some(point), None) => {
                if point.is_delete {
                    Ok(MemtableGetResult::Delete)
                } else {
                    Ok(MemtableGetResult::Put(
                        point
                            .value
                            .clone()
                            .expect("Non-delete point entry must have a value"),
                    ))
                }
            }

            // Both point entry and tombstone exist → compare LSNs
            (Some(point), Some(tombstone_lsn)) => {
                if tombstone_lsn > point.lsn {
                    Ok(MemtableGetResult::RangeDelete)
                } else if point.is_delete {
                    Ok(MemtableGetResult::Delete)
                } else {
                    Ok(MemtableGetResult::Put(
                        point
                            .value
                            .clone()
                            .expect("Non-delete point entry must have a value"),
                    ))
                }
            }
        }
    }

    /// Performs an ordered range scan over `[start, end)`.
    ///
    /// Each key is resolved against:
    /// - its latest point entry
    /// - all applicable range tombstones
    ///
    /// Deleted keys are omitted from the result.
    ///
    /// # Complexity
    /// O(N * R) where R is the number of overlapping range tombstones.
    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<impl Iterator<Item = (Vec<u8>, MemtableSingleEntry)>, MemtableError> {
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

        let mut records = Vec::new();

        for (key, versions) in guard.tree.range(start.to_vec()..end.to_vec()) {
            let Some(point) = versions.values().next() else {
                continue;
            };

            if point.is_delete {
                continue;
            }

            let mut tombstone_lsn = 0;
            for (_start, t_versions) in guard.range_tombstones.range(..=key.clone()) {
                // highest LSN tombstone for this start
                if let Some(t) = t_versions.values().next() {
                    if t.start.as_slice() <= key.as_slice() && key.as_slice() < t.end.as_slice() {
                        tombstone_lsn = tombstone_lsn.max(t.lsn);
                    }
                }
            }

            if tombstone_lsn > point.lsn {
                continue; // deleted by tombstone
            }

            records.push((key.clone(), point.clone()));
        }

        Ok(records.into_iter())
    }

    /// Returns a logical snapshot of the memtable suitable for flushing.
    ///
    /// The iterator emits:
    /// - The latest version of every point key (put or delete)
    /// - **All** range tombstones
    ///
    /// # Guarantees
    /// - No filtering based on tombstone interaction
    /// - Returned records are sufficient to rebuild the same state
    /// - Does not mutate in-memory state
    ///
    /// # Intended Use
    /// This iterator is consumed by the SSTable writer.
    pub fn iter_for_flush(&self) -> Result<impl Iterator<Item = MemtableRecord>, MemtableError> {
        let guard = self.inner.read().map_err(|_| {
            error!("Read-write lock poisoned during iter_for_flush");
            MemtableError::Internal("Read-write lock poisoned".into())
        })?;

        let mut records = Vec::new();

        for (key, versions) in guard.tree.iter() {
            if let Some(entry) = versions.values().next() {
                let record = if entry.is_delete {
                    MemtableRecord::Delete {
                        key: key.clone(),
                        lsn: entry.lsn,
                        timestamp: entry.timestamp,
                    }
                } else {
                    MemtableRecord::Put {
                        key: key.clone(),
                        value: entry.value.clone().unwrap(),
                        lsn: entry.lsn,
                        timestamp: entry.timestamp,
                    }
                };
                records.push(record);
            }
        }

        for (start, versions) in guard.range_tombstones.iter() {
            for entry in versions.values() {
                let record = MemtableRecord::RangeDelete {
                    start: start.clone(),
                    end: entry.end.clone(),
                    lsn: entry.lsn,
                    timestamp: entry.timestamp,
                };
                records.push(record);
            }
        }

        Ok(records.into_iter())
    }

    /// Converts this mutable memtable into an immutable [`FrozenMemtable`].
    ///
    /// # Behavior
    /// - Consumes `self`, preventing any further writes.
    /// - Preserves ownership of the WAL to keep it alive during flushing.
    /// - Exposes only read-only operations.
    pub fn frozen(self) -> Result<FrozenMemtable, MemtableError> {
        Ok(FrozenMemtable::new(self))
    }

    /// Override the current LSN counter with a recovered value.
    ///
    /// # Safety / Rules
    /// - Must only be called during recovery **before any writes**.
    /// - Ensures that future LSNs always increase beyond recovered state.
    pub fn inject_max_lsn(&self, lsn: u64) {
        // next_lsn always points to the *next available* LSN
        self.next_lsn.store(lsn.saturating_add(1), Ordering::SeqCst);
    }

    /// Returns the highest assigned LSN so far.
    ///
    /// This returns `next_lsn - 1`, since `next_lsn` always stores the next unused sequence number.
    pub fn max_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::SeqCst).saturating_sub(1)
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
// Frozen Memtable
// ------------------------------------------------------------------------------------------------

/// An immutable, read-only view of a memtable.
///
/// A frozen memtable:
/// - Exposes only read APIs
/// - Retains ownership of the WAL
/// - Prevents further mutation by construction
///
/// This type represents a memtable that is in the process of being flushed
/// to an on-disk SSTable.
pub struct FrozenMemtable {
    pub memtable: Memtable,
    pub creation_timestamp: u64,
}

impl FrozenMemtable {
    /// Creates a new frozen memtable by opening and replaying a WAL.
    pub fn new(memtable: Memtable) -> Self {
        Self {
            memtable,
            creation_timestamp: Memtable::current_timestamp(),
        }
    }

    /// Retrieves the latest visible value for a key.
    pub fn get(&self, key: &[u8]) -> Result<MemtableGetResult, MemtableError> {
        self.memtable.get(key)
    }

    /// Performs a range scan over the frozen memtable.
    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<impl Iterator<Item = (Vec<u8>, MemtableSingleEntry)>, MemtableError> {
        self.memtable.scan(start, end)
    }

    /// Returns all records required to materialize this memtable into an SSTable.
    pub fn iter_for_flush(&self) -> Result<impl Iterator<Item = MemtableRecord>, MemtableError> {
        self.memtable.iter_for_flush()
    }

    /// Returns the highest assigned LSN so far.
    pub fn max_lsn(&self) -> u64 {
        self.memtable.max_lsn()
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
