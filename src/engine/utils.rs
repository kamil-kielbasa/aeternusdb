//! Engine utilities — shared record types, tombstones, and merge primitives.
//!
//! This module defines:
//!
//! - [`Record`] — the unified representation of a point put, point delete,
//!   or range delete used across all engine layers (memtable, SSTable,
//!   compaction, scan).
//! - [`RangeTombstone`] — a versioned range deletion marker shared across
//!   memtable, SSTable, and compaction subsystems.
//! - [`MergeIterator`] — a heap-based k-way merge iterator that combines
//!   multiple sorted record streams into a single globally-sorted stream.

/// Represents a single item emitted by the storage engine.
#[derive(Debug, Clone)]
pub enum Record {
    /// A concrete key-value pair (point put).
    Put {
        /// The key.
        key: Vec<u8>,

        /// The value associated with the key.
        value: Vec<u8>,

        /// The log sequence number (LSN) of this record.
        lsn: u64,

        /// The timestamp of this record.
        timestamp: u64,
    },

    /// A point deletion of a specific key.
    Delete {
        /// The key to be deleted.
        key: Vec<u8>,

        /// The log sequence number (LSN) of this record.
        lsn: u64,

        /// The timestamp of this record.
        timestamp: u64,
    },

    /// A range tombstone representing deletion of a key interval `[start_key, end_key)`.
    RangeDelete {
        /// Start key of the deleted interval (inclusive).
        start: Vec<u8>,

        /// End key of the deleted interval (exclusive).
        end: Vec<u8>,

        /// The log sequence number (LSN) of this record.
        lsn: u64,

        /// The timestamp of this record.
        timestamp: u64,
    },
}

impl Record {
    /// Returns the log sequence number (LSN) of this record.
    pub fn lsn(&self) -> u64 {
        match self {
            Record::Put { lsn, .. } => *lsn,
            Record::Delete { lsn, .. } => *lsn,
            Record::RangeDelete { lsn, .. } => *lsn,
        }
    }

    /// Returns the primary key of this record.
    ///
    /// For `RangeDelete` records this returns the **start** key of the range.
    pub fn key(&self) -> &[u8] {
        match self {
            Record::Put { key, .. } => key,
            Record::Delete { key, .. } => key,
            Record::RangeDelete { start, .. } => start,
        }
    }

    /// Returns the wall-clock timestamp (nanoseconds since UNIX epoch)
    /// associated with this record.
    pub fn timestamp(&self) -> u64 {
        match self {
            Record::Put { timestamp, .. } => *timestamp,
            Record::Delete { timestamp, .. } => *timestamp,
            Record::RangeDelete { timestamp, .. } => *timestamp,
        }
    }

    /// Converts this record into its SSTable-level representation.
    ///
    /// Point puts and point deletes become [`PointEntry`] values;
    /// range deletes become [`RangeTombstone`] values.
    pub fn into_entry(self) -> RecordEntry {
        match self {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => RecordEntry::Point(PointEntry {
                key,
                value: Some(value),
                lsn,
                timestamp,
            }),
            Record::Delete {
                key,
                lsn,
                timestamp,
            } => RecordEntry::Point(PointEntry {
                key,
                value: None,
                lsn,
                timestamp,
            }),
            Record::RangeDelete {
                start,
                end,
                lsn,
                timestamp,
            } => RecordEntry::Range(RangeTombstone {
                start,
                end,
                lsn,
                timestamp,
            }),
        }
    }
}

/// Result of splitting a [`Record`] into its SSTable-level representation.
///
/// Used when flushing memtables or compacting SSTables: point mutations
/// (puts and deletes) travel as [`PointEntry`], while range tombstones
/// travel separately.
pub enum RecordEntry {
    /// A point put or point delete.
    Point(PointEntry),
    /// A range tombstone.
    Range(RangeTombstone),
}

// ------------------------------------------------------------------------------------------------
// Ord / Eq — ordering by (key ASC, LSN DESC)
// ------------------------------------------------------------------------------------------------

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key() && self.lsn() == other.lsn()
    }
}

impl Eq for Record {}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    /// Compares by `(key ASC, LSN DESC)`.
    ///
    /// For a given key the highest-LSN (most recent) record sorts first,
    /// ensuring it is seen before older versions during merge iteration.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.key().cmp(other.key()) {
            std::cmp::Ordering::Equal => other.lsn().cmp(&self.lsn()),
            ord => ord,
        }
    }
}

/// Compares two records by `(key ASC, LSN DESC)`.
///
/// Equivalent to `a.cmp(b)` via the [`Ord`] implementation on [`Record`].
#[allow(dead_code)]
pub fn record_cmp(a: &Record, b: &Record) -> std::cmp::Ordering {
    a.cmp(b)
}

// ------------------------------------------------------------------------------------------------
// PointEntry — input type for SSTable construction
// ------------------------------------------------------------------------------------------------

/// A point mutation to be written into an SSTable: a Put or Delete.
///
/// This is the common currency type used when flushing memtables and
/// compacting SSTables. It is intentionally simpler than [`Record`] —
/// range deletes travel separately as [`RangeTombstone`] values.
#[derive(Debug, Clone)]
pub struct PointEntry {
    /// Key of the entry.
    pub key: Vec<u8>,

    /// Value of the entry; `None` indicates a point deletion.
    pub value: Option<Vec<u8>>,

    /// Log sequence number of this mutation.
    pub lsn: u64,

    /// Timestamp associated with this mutation.
    pub timestamp: u64,
}

impl PointEntry {
    /// Creates a new point put entry.
    pub fn new(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        lsn: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
            lsn,
            timestamp,
        }
    }

    /// Creates a new point delete (tombstone) entry.
    pub fn new_delete(key: impl Into<Vec<u8>>, lsn: u64, timestamp: u64) -> Self {
        Self {
            key: key.into(),
            value: None,
            lsn,
            timestamp,
        }
    }
}

// ------------------------------------------------------------------------------------------------
// RangeTombstone — shared across all layers
// ------------------------------------------------------------------------------------------------

/// A range tombstone that logically deletes all keys in `[start, end)`.
///
/// Range tombstones are versioned via LSN and may overlap. During reads,
/// the highest-LSN tombstone covering a key takes precedence.
///
/// This type is shared across the memtable, SSTable, and compaction
/// subsystems.
#[derive(Clone, Debug)]
pub struct RangeTombstone {
    /// Inclusive start key of the deleted range.
    pub start: Vec<u8>,

    /// Exclusive end key of the deleted range.
    pub end: Vec<u8>,

    /// Log Sequence Number of this tombstone.
    pub lsn: u64,

    /// Timestamp associated with this mutation.
    pub timestamp: u64,
}

impl RangeTombstone {
    /// Creates a new range tombstone covering `[start, end)`.
    pub fn new(
        start: impl Into<Vec<u8>>,
        end: impl Into<Vec<u8>>,
        lsn: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
            lsn,
            timestamp,
        }
    }
}

// ------------------------------------------------------------------------------------------------
// MergeIterator — heap-based k-way merge over Record streams
// ------------------------------------------------------------------------------------------------

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// A heap-based merge iterator that yields [`Record`]s from multiple
/// sorted sources in `(key ASC, LSN DESC)` order.
///
/// Used by both the engine scan path and the compaction module.
/// The lifetime `'a` bounds any borrowed state inside the source
/// iterators; pass `'static` when the sources own their data.
pub struct MergeIterator<'a> {
    iters: Vec<Box<dyn Iterator<Item = Record> + 'a>>,
    heap: BinaryHeap<MergeHeapEntry<'a>>,
}

struct MergeHeapEntry<'a> {
    record: Record,
    source_idx: usize,
    /// Marker so the struct is invariant over `'a` without storing a
    /// reference — the actual borrowed data lives inside the iterator.
    _marker: std::marker::PhantomData<&'a ()>,
}

impl Ord for MergeHeapEntry<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: reverse so smallest key / highest LSN pops first.
        self.record.cmp(&other.record).reverse()
    }
}

impl PartialOrd for MergeHeapEntry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MergeHeapEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.record == other.record
    }
}

impl Eq for MergeHeapEntry<'_> {}

impl<'a> MergeIterator<'a> {
    pub fn new(mut iters: Vec<Box<dyn Iterator<Item = Record> + 'a>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some(record) = iter.next() {
                heap.push(MergeHeapEntry {
                    record,
                    source_idx: idx,
                    _marker: std::marker::PhantomData,
                });
            }
        }

        Self { iters, heap }
    }
}

impl Iterator for MergeIterator<'_> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.heap.pop()?;
        let result = entry.record;
        let idx = entry.source_idx;

        if let Some(next_record) = self.iters[idx].next() {
            self.heap.push(MergeHeapEntry {
                record: next_record,
                source_idx: idx,
                _marker: std::marker::PhantomData,
            });
        }

        Some(result)
    }
}
