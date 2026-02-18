//! Engine utilities — shared types and merge primitives.

/// Represents a single item emitted by the storage engine.
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
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
    pub fn lsn(&self) -> u64 {
        match self {
            Record::Put { lsn, .. } => *lsn,
            Record::Delete { lsn, .. } => *lsn,
            Record::RangeDelete { lsn, .. } => *lsn,
        }
    }

    pub fn key(&self) -> &Vec<u8> {
        match self {
            Record::Put { key, .. } => key,
            Record::Delete { key, .. } => key,
            Record::RangeDelete { start, .. } => start,
        }
    }

    pub fn timestamp(&self) -> u64 {
        match self {
            Record::Put { timestamp, .. } => *timestamp,
            Record::Delete { timestamp, .. } => *timestamp,
            Record::RangeDelete { timestamp, .. } => *timestamp,
        }
    }
}

pub fn record_cmp(a: &Record, b: &Record) -> std::cmp::Ordering {
    match a.key().cmp(b.key()) {
        std::cmp::Ordering::Equal => b.lsn().cmp(&a.lsn()),
        other => other,
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
