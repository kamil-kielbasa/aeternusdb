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
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use crate::engine::Record;
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
    Wal(#[from] WalError),

    /// Write buffer limit reached; a flush is required before further writes.
    #[error("Flush required")]
    FlushRequired,

    /// Caller-supplied argument is invalid (empty key, reversed range, etc.).
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

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
    pub wal: Wal<Record>,

    /// Monotonic log sequence number (LSN) for version ordering.
    next_lsn: AtomicU64,
}

/// A single versioned point entry stored in the memtable.
///
/// A key may have multiple `MemtablePointEntry` versions, ordered by LSN.
/// The highest-LSN entry is considered the latest.
///
/// Deletions are represented by the `Delete` variant (tombstone);
/// live values by `Put`.
#[derive(Debug, PartialEq, Clone)]
pub enum MemtablePointEntry {
    /// A live key-value pair.
    Put {
        /// The stored value.
        value: Vec<u8>,
        /// Logical timestamp in nanoseconds since UNIX epoch.
        timestamp: u64,
        /// Log sequence number for ordering updates.
        lsn: u64,
    },
    /// A point tombstone (deletion marker).
    Delete {
        /// Logical timestamp in nanoseconds since UNIX epoch.
        timestamp: u64,
        /// Log sequence number for ordering updates.
        lsn: u64,
    },
}

impl MemtablePointEntry {
    /// Returns the LSN of this entry, regardless of variant.
    pub fn lsn(&self) -> u64 {
        match self {
            Self::Put { lsn, .. } | Self::Delete { lsn, .. } => *lsn,
        }
    }

    /// Returns the timestamp of this entry, regardless of variant.
    #[allow(dead_code)]
    pub fn timestamp(&self) -> u64 {
        match self {
            Self::Put { timestamp, .. } | Self::Delete { timestamp, .. } => *timestamp,
        }
    }

    /// Returns `true` if this entry is a deletion tombstone.
    #[allow(dead_code)]
    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }

    /// Returns the value if this is a `Put`, or `None` for a `Delete`.
    #[allow(dead_code)]
    pub fn value(&self) -> Option<&[u8]> {
        match self {
            Self::Put { value, .. } => Some(value),
            Self::Delete { .. } => None,
        }
    }
}

/// Discriminant tag used in the binary encoding of [`MemtablePointEntry`].
const POINT_ENTRY_TAG_PUT: u8 = 0;
const POINT_ENTRY_TAG_DELETE: u8 = 1;

impl crate::encoding::Encode for MemtablePointEntry {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), crate::encoding::EncodingError> {
        match self {
            Self::Put {
                value,
                timestamp,
                lsn,
            } => {
                crate::encoding::Encode::encode_to(&POINT_ENTRY_TAG_PUT, buf)?;
                crate::encoding::Encode::encode_to(value, buf)?;
                crate::encoding::Encode::encode_to(timestamp, buf)?;
                crate::encoding::Encode::encode_to(lsn, buf)?;
            }
            Self::Delete { timestamp, lsn } => {
                crate::encoding::Encode::encode_to(&POINT_ENTRY_TAG_DELETE, buf)?;
                crate::encoding::Encode::encode_to(timestamp, buf)?;
                crate::encoding::Encode::encode_to(lsn, buf)?;
            }
        }
        Ok(())
    }
}

impl crate::encoding::Decode for MemtablePointEntry {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), crate::encoding::EncodingError> {
        let (tag, mut offset) = <u8 as crate::encoding::Decode>::decode_from(buf)?;
        match tag {
            POINT_ENTRY_TAG_PUT => {
                let (value, n) = <Vec<u8> as crate::encoding::Decode>::decode_from(&buf[offset..])?;
                offset += n;
                let (timestamp, n) = <u64 as crate::encoding::Decode>::decode_from(&buf[offset..])?;
                offset += n;
                let (lsn, n) = <u64 as crate::encoding::Decode>::decode_from(&buf[offset..])?;
                offset += n;
                Ok((
                    Self::Put {
                        value,
                        timestamp,
                        lsn,
                    },
                    offset,
                ))
            }
            POINT_ENTRY_TAG_DELETE => {
                let (timestamp, n) = <u64 as crate::encoding::Decode>::decode_from(&buf[offset..])?;
                offset += n;
                let (lsn, n) = <u64 as crate::encoding::Decode>::decode_from(&buf[offset..])?;
                offset += n;
                Ok((Self::Delete { timestamp, lsn }, offset))
            }
            _ => Err(crate::encoding::EncodingError::InvalidTag {
                tag: tag as u32,
                type_name: "MemtablePointEntry",
            }),
        }
    }
}

use crate::engine::RangeTombstone;

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

/// Lightweight snapshot of memtable statistics.
///
/// Returned by [`Memtable::stats`] under a short read lock.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct MemtableStats {
    /// Approximate in-memory size in bytes (keys + values + metadata).
    pub size_bytes: usize,

    /// Number of distinct keys with at least one point version.
    pub key_count: usize,

    /// Total number of point entry versions across all keys.
    pub entry_count: usize,

    /// Number of point entry versions that are tombstones (`Delete`).
    pub tombstone_count: usize,

    /// Number of range tombstone versions.
    pub range_tombstone_count: usize,
}

/// Internal shared state of the memtable.
///
/// This structure is protected by an `RwLock` and must never be
/// accessed directly outside the memtable implementation.
struct MemtableInner {
    /// Point entries grouped by key, then ordered by descending LSN.
    tree: BTreeMap<Vec<u8>, BTreeMap<Reverse<u64>, MemtablePointEntry>>,

    /// Range tombstones indexed by start key and ordered by descending LSN.
    range_tombstones: BTreeMap<Vec<u8>, BTreeMap<Reverse<u64>, RangeTombstone>>,

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
            let record: Record = record?;

            match record {
                Record::Put {
                    key,
                    value,
                    lsn,
                    timestamp,
                } => {
                    let record_size =
                        std::mem::size_of::<MemtablePointEntry>() + key.len() + value.len();
                    inner.approximate_size += record_size;

                    if lsn > max_lsn_seen {
                        max_lsn_seen = lsn;
                    }

                    let entry = MemtablePointEntry::Put {
                        value,
                        timestamp,
                        lsn,
                    };

                    inner
                        .tree
                        .entry(key)
                        .or_default()
                        .insert(Reverse(lsn), entry);
                }

                Record::Delete {
                    key,
                    lsn,
                    timestamp,
                } => {
                    let record_size = std::mem::size_of::<MemtablePointEntry>() + key.len();
                    inner.approximate_size += record_size;

                    if lsn > max_lsn_seen {
                        max_lsn_seen = lsn;
                    }

                    let entry = MemtablePointEntry::Delete { timestamp, lsn };

                    inner
                        .tree
                        .entry(key)
                        .or_default()
                        .insert(Reverse(lsn), entry);
                }

                Record::RangeDelete {
                    start,
                    end,
                    lsn,
                    timestamp,
                } => {
                    let record_size =
                        std::mem::size_of::<RangeTombstone>() + start.len() + end.len();
                    inner.approximate_size += record_size;

                    if lsn > max_lsn_seen {
                        max_lsn_seen = lsn;
                    }

                    let record_value = RangeTombstone {
                        start,
                        end,
                        lsn,
                        timestamp,
                    };

                    inner
                        .range_tombstones
                        .entry(record_value.start.clone())
                        .or_default()
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
    /// - The write buffer is checked under a short read lock.
    /// - An LSN is allocated only after the budget check passes.
    /// - The record is appended to the WAL with **no lock held**.
    /// - The in-memory tree is updated under a short write lock.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemtableError> {
        trace!("put() started, key: {}", HexKey(&key));

        if key.is_empty() || value.is_empty() {
            return Err(MemtableError::InvalidArgument(
                "Key or value is empty".to_string(),
            ));
        }

        let record_size = std::mem::size_of::<MemtablePointEntry>() + key.len() + value.len();
        let key_for_wal = key.clone();
        let value_for_wal = value.clone();

        let lsn = self.apply_write(
            record_size,
            "put",
            |lsn, timestamp| Record::Put {
                key: key_for_wal,
                value: value_for_wal,
                timestamp,
                lsn,
            },
            |inner, lsn, timestamp| {
                let entry = MemtablePointEntry::Put {
                    value,
                    timestamp,
                    lsn,
                };
                inner
                    .tree
                    .entry(key)
                    .or_default()
                    .insert(Reverse(lsn), entry);
            },
        )?;

        trace!("Put operation completed with LSN: {}", lsn);
        Ok(())
    }

    /// Deletes a key by inserting a tombstone entry.
    ///
    /// # Behavior
    /// - The write buffer is checked under a short read lock.
    /// - An LSN is allocated only after the budget check passes.
    /// - The record is appended to the WAL with **no lock held**.
    /// - The in-memory tree is updated under a short write lock.
    pub fn delete(&self, key: Vec<u8>) -> Result<(), MemtableError> {
        trace!("delete() started, key: {}", HexKey(&key));

        if key.is_empty() {
            return Err(MemtableError::InvalidArgument("Key is empty".to_string()));
        }

        let record_size = std::mem::size_of::<MemtablePointEntry>() + key.len();
        let key_for_wal = key.clone();

        let lsn = self.apply_write(
            record_size,
            "delete",
            |lsn, timestamp| Record::Delete {
                key: key_for_wal,
                lsn,
                timestamp,
            },
            |inner, lsn, timestamp| {
                let entry = MemtablePointEntry::Delete { timestamp, lsn };
                inner
                    .tree
                    .entry(key)
                    .or_default()
                    .insert(Reverse(lsn), entry);
            },
        )?;

        trace!("Delete operation completed with LSN: {}", lsn);
        Ok(())
    }

    /// Deletes all keys in the range `[start, end)`.
    ///
    /// # Range Semantics
    /// - Inclusive `start`
    /// - Exclusive `end`
    ///
    /// # Behavior
    /// - The write buffer is checked under a short read lock.
    /// - An LSN is allocated only after the budget check passes.
    /// - The range tombstone is appended to the WAL with **no lock held**.
    /// - The in-memory tombstone map is updated under a short write lock.
    pub fn delete_range(&self, start: Vec<u8>, end: Vec<u8>) -> Result<(), MemtableError> {
        trace!(
            "delete_range() started, start key: {}, end key: {}",
            HexKey(&start),
            HexKey(&end)
        );

        if start.is_empty() || end.is_empty() {
            return Err(MemtableError::InvalidArgument(
                "Start or end key is empty".to_string(),
            ));
        }

        if start >= end {
            return Err(MemtableError::InvalidArgument(
                "Start key must be less than end key".to_string(),
            ));
        }

        let record_size = std::mem::size_of::<RangeTombstone>() + start.len() + end.len();
        let start_for_wal = start.clone();
        let end_for_wal = end.clone();

        let lsn = self.apply_write(
            record_size,
            "delete_range",
            |lsn, timestamp| Record::RangeDelete {
                start: start_for_wal,
                end: end_for_wal,
                lsn,
                timestamp,
            },
            |inner, lsn, timestamp| {
                let entry_key = start.clone();
                let tombstone = RangeTombstone {
                    start,
                    end,
                    lsn,
                    timestamp,
                };
                inner
                    .range_tombstones
                    .entry(entry_key)
                    .or_default()
                    .insert(Reverse(lsn), tombstone);
            },
        )?;

        trace!("delete_range completed with LSN: {}", lsn);
        Ok(())
    }

    /// Shared write path: budget check → LSN allocation → WAL append → in-memory update.
    ///
    /// # Arguments
    /// - `record_size` — estimated byte cost of this write for budget tracking.
    /// - `op_name` — operation label used in error messages and tracing.
    /// - `build_record` — closure that receives `(lsn, timestamp)` and returns
    ///   the WAL [`Record`] to be durably appended.
    /// - `apply_to_inner` — closure that performs the in-memory insertion;
    ///   invoked under a write lock with `(inner, lsn, timestamp)`.
    ///
    /// # Returns
    /// The allocated LSN on success.
    fn apply_write<F, G>(
        &self,
        record_size: usize,
        op_name: &str,
        build_record: F,
        apply_to_inner: G,
    ) -> Result<u64, MemtableError>
    where
        F: FnOnce(u64, u64) -> Record,
        G: FnOnce(&mut MemtableInner, u64, u64),
    {
        // 1. Buffer check — short read lock, released immediately.
        {
            let guard = self.inner.read().map_err(|_| {
                error!("Read-write lock poisoned during {}", op_name);
                MemtableError::Internal("Read-write lock poisoned".into())
            })?;
            if guard.approximate_size + record_size > guard.write_buffer_size {
                return Err(MemtableError::FlushRequired);
            }
        }

        // 2. Allocate LSN only after confirming budget.
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let timestamp = Self::current_timestamp();

        // 3. WAL append — durable write with no lock held.
        let record = build_record(lsn, timestamp);
        self.wal.append(&record)?;

        // 4. In-memory update — write lock held only for the insert.
        let mut guard = self.inner.write().map_err(|_| {
            error!("Read-write lock poisoned during {}", op_name);
            MemtableError::Internal("Read-write lock poisoned".into())
        })?;

        apply_to_inner(&mut guard, lsn, timestamp);
        guard.approximate_size += record_size;

        Ok(lsn)
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
            error!("Read-write lock poisoned during get");
            MemtableError::Internal("RwLock poisoned".into())
        })?;

        // Check if key exists as a point entry
        let point_opt = guard
            .tree
            .get(key)
            .and_then(|versions| versions.values().next());

        // Check if key matches any range tombstones.
        // For each start key, we check ALL versions (not just the highest-LSN)
        // because a narrower tombstone with a higher LSN might not cover the
        // queried key while a wider tombstone with a lower LSN does.
        let mut covering_tombstone_lsn: Option<u64> = None;
        for (_start, versions) in guard.range_tombstones.range(..=key.to_vec()) {
            for tombstone in versions.values() {
                if tombstone.start.as_slice() <= key && key < tombstone.end.as_slice() {
                    covering_tombstone_lsn = Some(
                        covering_tombstone_lsn
                            .map(|lsn| lsn.max(tombstone.lsn))
                            .unwrap_or(tombstone.lsn),
                    );
                    // Found the highest-LSN covering tombstone for this start
                    // key — no need to check lower-LSN versions for the same
                    // start key (they can only have equal or lower LSN).
                    break;
                }
            }
        }

        match (point_opt, covering_tombstone_lsn) {
            // No point entry and no tombstone → key not found
            (None, None) => Ok(MemtableGetResult::NotFound),

            // No point entry but covered by range tombstone
            (None, Some(_)) => Ok(MemtableGetResult::RangeDelete),

            // Point entry exists, no covering tombstone
            (Some(point), None) => match point {
                MemtablePointEntry::Delete { .. } => Ok(MemtableGetResult::Delete),
                MemtablePointEntry::Put { value, .. } => Ok(MemtableGetResult::Put(value.clone())),
            },

            // Both point entry and tombstone exist → compare LSNs
            (Some(point), Some(tombstone_lsn)) => {
                if tombstone_lsn > point.lsn() {
                    Ok(MemtableGetResult::RangeDelete)
                } else {
                    match point {
                        MemtablePointEntry::Delete { .. } => Ok(MemtableGetResult::Delete),
                        MemtablePointEntry::Put { value, .. } => {
                            Ok(MemtableGetResult::Put(value.clone()))
                        }
                    }
                }
            }
        }
    }

    /// Performs an ordered range scan over `[start, end)`.
    ///
    /// Returns all records (puts, point tombstones, range tombstones) that
    /// overlap `[start, end)`, sorted by key ASC, LSN DESC. Callers are
    /// responsible for tombstone resolution.
    ///
    /// # Complexity
    /// O((N + R) log(N + R)) — all matching entries are collected into a
    /// `Vec` and sorted before iteration.
    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<impl Iterator<Item = Record>, MemtableError> {
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

        let mut out = Vec::new();

        // 1) Collect point entries
        for (key, versions) in guard.tree.range(start.to_vec()..end.to_vec()) {
            for entry in versions.values() {
                let record = match entry {
                    MemtablePointEntry::Delete { lsn, timestamp } => Record::Delete {
                        key: key.clone(),
                        lsn: *lsn,
                        timestamp: *timestamp,
                    },
                    MemtablePointEntry::Put {
                        value,
                        lsn,
                        timestamp,
                    } => Record::Put {
                        key: key.clone(),
                        value: value.clone(),
                        lsn: *lsn,
                        timestamp: *timestamp,
                    },
                };

                out.push(record);
            }
        }

        // 2) Collect range tombstones
        for (_tombstone_start, versions) in guard.range_tombstones.iter() {
            for tombstone in versions.values() {
                // Check if tombstone overlaps scan range
                if tombstone.end.as_slice() <= start || tombstone.start.as_slice() >= end {
                    continue;
                }

                let record = Record::RangeDelete {
                    start: tombstone.start.clone(),
                    end: tombstone.end.clone(),
                    lsn: tombstone.lsn,
                    timestamp: tombstone.timestamp,
                };

                out.push(record);
            }
        }

        // 3) Sort stream: key ASC, lsn DESC
        out.sort_by(|a, b| {
            let ka = a.key();
            let kb = b.key();

            match ka.cmp(kb) {
                std::cmp::Ordering::Equal => b.lsn().cmp(&a.lsn()), // Descending LSN
                other => other,
            }
        });

        Ok(out.into_iter())
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
    pub fn iter_for_flush(&self) -> Result<impl Iterator<Item = Record>, MemtableError> {
        let guard = self.inner.read().map_err(|_| {
            error!("Read-write lock poisoned during iter_for_flush");
            MemtableError::Internal("Read-write lock poisoned".into())
        })?;

        let mut records = Vec::new();

        for (key, versions) in guard.tree.iter() {
            if let Some(entry) = versions.values().next() {
                let record = match entry {
                    MemtablePointEntry::Delete { lsn, timestamp } => Record::Delete {
                        key: key.clone(),
                        lsn: *lsn,
                        timestamp: *timestamp,
                    },
                    MemtablePointEntry::Put {
                        value,
                        lsn,
                        timestamp,
                    } => Record::Put {
                        key: key.clone(),
                        value: value.clone(),
                        lsn: *lsn,
                        timestamp: *timestamp,
                    },
                };
                records.push(record);
            }
        }

        for (start, versions) in guard.range_tombstones.iter() {
            for entry in versions.values() {
                let record = Record::RangeDelete {
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

    /// Returns a snapshot of memtable statistics under a short read lock.
    #[allow(dead_code)]
    pub fn stats(&self) -> Result<MemtableStats, MemtableError> {
        let guard = self.inner.read().map_err(|_| {
            error!("Read-write lock poisoned during stats");
            MemtableError::Internal("Read-write lock poisoned".into())
        })?;

        let mut entry_count: usize = 0;
        let mut tombstone_count: usize = 0;

        for versions in guard.tree.values() {
            for entry in versions.values() {
                entry_count += 1;
                if entry.is_delete() {
                    tombstone_count += 1;
                }
            }
        }

        let range_tombstone_count: usize = guard
            .range_tombstones
            .values()
            .map(|versions| versions.len())
            .sum();

        Ok(MemtableStats {
            size_bytes: guard.approximate_size,
            key_count: guard.tree.len(),
            entry_count,
            tombstone_count,
            range_tombstone_count,
        })
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

    /// Returns the highest assigned LSN, or `None` if no writes have occurred.
    ///
    /// A return of `None` unambiguously means the memtable is empty;
    /// `Some(n)` means the last write was assigned LSN `n`.
    pub fn max_lsn(&self) -> Option<u64> {
        let next = self.next_lsn.load(Ordering::SeqCst);
        if next <= 1 { None } else { Some(next - 1) }
    }

    /// Returns the WAL sequence number for this memtable.
    pub fn wal_seq(&self) -> u64 {
        self.wal.wal_seq()
    }

    /// Returns the current system timestamp in nanoseconds.
    ///
    /// Used to tag entries for ordering and diagnostics.
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
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
    memtable: Memtable,
    #[allow(dead_code)]
    creation_timestamp: u64,
}

impl FrozenMemtable {
    /// Creates a new frozen memtable by opening and replaying a WAL.
    pub fn new(memtable: Memtable) -> Self {
        Self {
            memtable,
            creation_timestamp: Memtable::current_timestamp(),
        }
    }

    /// Returns the WAL sequence number for this frozen memtable.
    pub fn wal_seq(&self) -> u64 {
        self.memtable.wal.wal_seq()
    }

    /// Returns the timestamp at which this memtable was frozen.
    #[allow(dead_code)]
    pub fn creation_timestamp(&self) -> u64 {
        self.creation_timestamp
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
    ) -> Result<impl Iterator<Item = Record>, MemtableError> {
        self.memtable.scan(start, end)
    }

    /// Returns all records required to materialize this memtable into an SSTable.
    pub fn iter_for_flush(&self) -> Result<impl Iterator<Item = Record>, MemtableError> {
        self.memtable.iter_for_flush()
    }

    /// Returns the highest assigned LSN, or `None` if empty.
    pub fn max_lsn(&self) -> Option<u64> {
        self.memtable.max_lsn()
    }
}

// ------------------------------------------------------------------------------------------------
// ReadMemtable trait
// ------------------------------------------------------------------------------------------------

/// Shared read interface for mutable and frozen memtables.
///
/// Allows engine code to treat an active [`Memtable`] and a [`FrozenMemtable`]
/// uniformly for all read operations, without exposing the mutable write API.
///
/// # Object safety
/// The trait is object-safe — all methods take `&self` and return concrete
/// types, so `Box<dyn ReadMemtable>` is valid.
#[allow(dead_code)]
pub trait ReadMemtable {
    /// Retrieves the latest visible state of a key.
    fn get(&self, key: &[u8]) -> Result<MemtableGetResult, MemtableError>;

    /// Returns all records overlapping `[start, end)`, sorted by key ASC / LSN DESC.
    fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Record>>, MemtableError>;

    /// Returns the highest LSN assigned so far, or `None` if no writes have occurred.
    fn max_lsn(&self) -> Option<u64>;
}

impl ReadMemtable for Memtable {
    fn get(&self, key: &[u8]) -> Result<MemtableGetResult, MemtableError> {
        // Calls the inherent Memtable::get — not recursive.
        self.get(key)
    }

    fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Record>>, MemtableError> {
        let records: Vec<_> = self.scan(start, end)?.collect();
        Ok(Box::new(records.into_iter()))
    }

    fn max_lsn(&self) -> Option<u64> {
        self.max_lsn()
    }
}

impl ReadMemtable for FrozenMemtable {
    fn get(&self, key: &[u8]) -> Result<MemtableGetResult, MemtableError> {
        self.get(key)
    }

    fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Record>>, MemtableError> {
        let records: Vec<_> = self.scan(start, end)?.collect();
        Ok(Box::new(records.into_iter()))
    }

    fn max_lsn(&self) -> Option<u64> {
        self.max_lsn()
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
