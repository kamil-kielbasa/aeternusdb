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

use crate::encoding::{self, EncodingError};

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
    pub fn key(&self) -> &Vec<u8> {
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
}

/// Compares two records by `(key ASC, LSN DESC)`.
///
/// This ordering ensures that for any given key, the highest-LSN
/// (most recent) record appears first in a sorted stream.
pub fn record_cmp(a: &Record, b: &Record) -> std::cmp::Ordering {
    match a.key().cmp(b.key()) {
        std::cmp::Ordering::Equal => b.lsn().cmp(&a.lsn()),
        other => other,
    }
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

// ------------------------------------------------------------------------------------------------
// Encode / Decode — Record
// ------------------------------------------------------------------------------------------------

impl encoding::Encode for Record {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        match self {
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                0u32.encode_to(buf)?;
                key.encode_to(buf)?;
                value.encode_to(buf)?;
                lsn.encode_to(buf)?;
                timestamp.encode_to(buf)?;
            }
            Record::Delete {
                key,
                lsn,
                timestamp,
            } => {
                1u32.encode_to(buf)?;
                key.encode_to(buf)?;
                lsn.encode_to(buf)?;
                timestamp.encode_to(buf)?;
            }
            Record::RangeDelete {
                start,
                end,
                lsn,
                timestamp,
            } => {
                2u32.encode_to(buf)?;
                start.encode_to(buf)?;
                end.encode_to(buf)?;
                lsn.encode_to(buf)?;
                timestamp.encode_to(buf)?;
            }
        }
        Ok(())
    }
}

impl encoding::Decode for Record {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (tag, mut offset) = u32::decode_from(buf)?;
        match tag {
            0 => {
                let (key, n) = Vec::<u8>::decode_from(&buf[offset..])?;
                offset += n;
                let (value, n) = Vec::<u8>::decode_from(&buf[offset..])?;
                offset += n;
                let (lsn, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                let (timestamp, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                Ok((
                    Record::Put {
                        key,
                        value,
                        lsn,
                        timestamp,
                    },
                    offset,
                ))
            }
            1 => {
                let (key, n) = Vec::<u8>::decode_from(&buf[offset..])?;
                offset += n;
                let (lsn, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                let (timestamp, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                Ok((
                    Record::Delete {
                        key,
                        lsn,
                        timestamp,
                    },
                    offset,
                ))
            }
            2 => {
                let (start, n) = Vec::<u8>::decode_from(&buf[offset..])?;
                offset += n;
                let (end, n) = Vec::<u8>::decode_from(&buf[offset..])?;
                offset += n;
                let (lsn, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                let (timestamp, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                Ok((
                    Record::RangeDelete {
                        start,
                        end,
                        lsn,
                        timestamp,
                    },
                    offset,
                ))
            }
            _ => Err(EncodingError::InvalidTag {
                tag,
                type_name: "Record",
            }),
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Encode / Decode — RangeTombstone
// ------------------------------------------------------------------------------------------------

impl encoding::Encode for RangeTombstone {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        self.start.encode_to(buf)?;
        self.end.encode_to(buf)?;
        self.lsn.encode_to(buf)?;
        self.timestamp.encode_to(buf)?;
        Ok(())
    }
}

impl encoding::Decode for RangeTombstone {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (start, mut offset) = Vec::<u8>::decode_from(buf)?;
        let (end, n) = Vec::<u8>::decode_from(&buf[offset..])?;
        offset += n;
        let (lsn, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        let (timestamp, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        Ok((
            RangeTombstone {
                start,
                end,
                lsn,
                timestamp,
            },
            offset,
        ))
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
        record_cmp(&self.record, &other.record).reverse()
    }
}

impl PartialOrd for MergeHeapEntry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MergeHeapEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.record.lsn() == other.record.lsn() && self.record.key() == other.record.key()
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
