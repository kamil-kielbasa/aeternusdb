//! SSTable iterators — block-level and multi-block scan.
//!
//! This module provides two iterator types:
//!
//! - [`BlockIterator`] — decodes a single data block and yields [`BlockEntry`]
//!   values. It supports `seek_to_first()` and `seek_to(key)` for positioning.
//! - [`ScanIterator`] — walks multiple data blocks plus range tombstones,
//!   yielding a merged stream of [`Record`] items in `(key ASC, LSN DESC)` order.
//!
//! # Block Iterator
//!
//! The block iterator operates on the raw bytes of a single data block.
//! Each entry is an encoded `SSTableCell` header followed by key and value bytes.
//!
//! ```text
//! [SSTableCell header][KEY_BYTES][VALUE_BYTES]
//! ```
//!
//! The header contains fixed-integer-encoded metadata:
//!
//! - `key_len` (u32)
//! - `value_len` (u32)
//! - `lsn` (u64)
//! - `timestamp` (u64)
//! - `is_delete` (bool)
//!
//! Seeking is linear within a block. Blocks are intentionally small (typically
//! 4 KiB), so linear search is efficient. If corruption or truncation is
//! detected, the iterator treats the block as exhausted.
//!
//! # Scan Iterator
//!
//! [`ScanIterator`] provides a **sorted forward scan** over a single SSTable,
//! yielding all point entries (`Put`, `Delete`) and range tombstones
//! (`RangeDelete`) that overlap a user-specified key range `[start_key, end_key)`.
//!
//! Blocks are decoded lazily and sequentially. When a block is exhausted, the
//! iterator advances to the next one automatically. Range tombstones are
//! interleaved with point entries in key order.
//!
//! The scan iterator does **not** perform visibility resolution — that is the
//! responsibility of upper layers (engine merge iterator, visibility filter).

use std::ops::Deref;

use crate::encoding;

use crate::engine::Record;

use super::{SSTable, SSTableCell, SSTableDataBlock, SSTableError};

// ------------------------------------------------------------------------------------------------
// Block Entry
// ------------------------------------------------------------------------------------------------

/// A fully decoded entry from a data block.
///
/// This is the in-memory representation of a single SSTable cell after decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockEntry {
    /// The user key bytes.
    pub key: Vec<u8>,

    /// The value bytes. Empty for tombstones.
    pub value: Vec<u8>,

    /// Whether this entry represents a point delete.
    pub is_delete: bool,

    /// Log sequence number associated with this version.
    pub lsn: u64,

    /// Commit timestamp supplied by the storage engine.
    pub timestamp: u64,
}

// ------------------------------------------------------------------------------------------------
// Block Iterator
// ------------------------------------------------------------------------------------------------

/// Iterator over the entries contained within a single SSTable data block.
///
/// This iterator:
///
/// - Decodes `SSTableCell` boundaries using custom encoding with fixed-int encoding.
/// - Provides block-local forward iteration.
/// - Supports basic key seeking within the block.
///
/// It **does not** handle merging multiple blocks, range tombstones, bloom filter lookups,
/// or other higher-level SSTable mechanics—those are implemented in the outer SSTable layer.
pub struct BlockIterator {
    /// Raw, decompressed block payload (entries only).
    data: Vec<u8>,

    /// Cursor into `data`, always pointing at the next header to decode.
    cursor: usize,
}

impl BlockIterator {
    /// Create a new iterator from already-decoded block bytes.
    ///
    /// The provided `data` slice must contain a concatenation of encoded `SSTableCell`s.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, cursor: 0 }
    }

    /// Reset the iterator to the first entry in the block.
    pub fn seek_to_first(&mut self) {
        self.cursor = 0;
    }

    /// Seek to the first entry whose key is **≥ `search_key`**.
    ///
    /// This performs a **linear scan**. If corruption or truncation is detected,
    /// the iterator stops at the end of the block.
    pub fn seek_to(&mut self, search_key: &[u8]) {
        self.cursor = 0;
        while self.cursor < self.data.len() {
            match encoding::decode_from_slice::<SSTableCell>(&self.data[self.cursor..]) {
                Ok((cell, cell_len)) => {
                    let pos = self.cursor + cell_len;

                    let key_len = cell.key_len as usize;
                    let value_len = cell.value_len as usize;

                    if pos + key_len + value_len > self.data.len() {
                        // truncated -> treat as end
                        self.cursor = self.data.len();
                        return;
                    }

                    let key_bytes = &self.data[pos..pos + key_len];
                    if key_bytes >= search_key {
                        // leave cursor at start of this cell
                        return;
                    }

                    // advance to next cell
                    self.cursor = pos + key_len + value_len;
                }
                Err(e) => {
                    tracing::warn!(cursor = self.cursor, ?e, "decode error during seek");
                    self.cursor = self.data.len();
                    return;
                }
            }
        }
    }

    /// Decode and return the next entry, advancing the cursor.
    ///
    /// Returns `None` if:
    /// - the cursor is at or past the end of the block,
    /// - decoding fails,
    /// - the block appears truncated.
    pub fn next_entry(&mut self) -> Option<BlockEntry> {
        if self.cursor >= self.data.len() {
            return None;
        }

        match encoding::decode_from_slice::<SSTableCell>(&self.data[self.cursor..]) {
            Ok((cell, cell_len)) => {
                self.cursor += cell_len;

                let key_len = cell.key_len as usize;
                let value_len = cell.value_len as usize;

                if self.cursor + key_len + value_len > self.data.len() {
                    // truncated -> treat as end
                    self.cursor = self.data.len();
                    return None;
                }

                let key = self.data[self.cursor..self.cursor + key_len].to_vec();
                self.cursor += key_len;
                let value = self.data[self.cursor..self.cursor + value_len].to_vec();
                self.cursor += value_len;

                Some(BlockEntry {
                    key,
                    value,
                    is_delete: cell.is_delete,
                    lsn: cell.lsn,
                    timestamp: cell.timestamp,
                })
            }
            Err(_) => {
                // invalid encoding -> treat as end
                self.cursor = self.data.len();
                None
            }
        }
    }

    /// Returns `true` if the iterator has reached the end of the block or encountered corruption.
    #[allow(dead_code)]
    pub fn is_end(&self) -> bool {
        self.cursor >= self.data.len()
    }
}

/// Implements idiomatic Rust iteration over block entries.
impl Iterator for BlockIterator {
    type Item = BlockEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
    }
}

// ------------------------------------------------------------------------------------------------
// Scan Iterator
// ------------------------------------------------------------------------------------------------

/// Iterator over all SSTable entries (point or range tombstones)
/// within the half-open interval:
///
/// ```text
/// [start_key, end_key)
/// ```
///
/// This iterator yields items of type [`Record`].
///
/// Internally, it:
///
/// - Tracks the current data-block index (`current_block_index`)
/// - Holds a block-local iterator (`BlockIterator`)
/// - Iterates through range tombstones stored in a separate structure
///
/// Errors during block loading or decoding are returned via the iterator.
pub struct ScanIterator<S: Deref<Target = SSTable> = &'static SSTable> {
    /// Reference to (or owned handle on) the SSTable being scanned.
    sstable: S,

    /// Current index into the SSTable block index.
    current_block_index: usize,

    /// Iterator over the entries in the current data block.
    current_block_iter: Option<BlockIterator>,

    /// Left bound of the user scan (inclusive).
    start_key: Vec<u8>,

    /// Right bound of the user scan (exclusive).
    end_key: Vec<u8>,

    /// Index into the SSTable range tombstone array.
    pending_range_idx: usize,

    /// Next range tombstone to yield.
    next_range: Option<Record>,

    /// Next point entry (Put/Delete) to yield.
    next_point: Option<Record>,
}

impl<S: Deref<Target = SSTable>> ScanIterator<S> {
    /// Create a new SSTable scan iterator for the half-open range
    /// `start_key <= key < end_key`.
    pub fn new(sstable: S, start_key: Vec<u8>, end_key: Vec<u8>) -> Result<Self, SSTableError> {
        if start_key >= end_key {
            return Err(SSTableError::Internal("scan start >= end".to_string()));
        }

        let current_block_index = sstable.find_block_for_key(start_key.as_slice());

        let block_iter = if current_block_index < sstable.index.len() {
            let entry = &sstable.index[current_block_index];
            let block_bytes = SSTable::read_block_bytes(&sstable.mmap, &entry.handle)?;
            let (block, _) = encoding::decode_from_slice::<SSTableDataBlock>(&block_bytes)?;
            let mut it = BlockIterator::new(block.data);
            it.seek_to(start_key.as_slice());
            Some(it)
        } else {
            None
        };

        Ok(Self {
            sstable,
            current_block_index,
            current_block_iter: block_iter,
            start_key,
            end_key,
            pending_range_idx: 0,
            next_range: None,
            next_point: None,
        })
    }

    /// Load the next data block and create a fresh `BlockIterator`.
    fn load_next_block(&mut self) -> Result<bool, SSTableError> {
        self.current_block_index += 1;

        if self.current_block_index >= self.sstable.index.len() {
            self.current_block_iter = None;
            return Ok(false);
        }

        let entry = &self.sstable.index[self.current_block_index];
        let block_bytes = SSTable::read_block_bytes(&self.sstable.mmap, &entry.handle)?;

        let (block, _) = encoding::decode_from_slice::<SSTableDataBlock>(&block_bytes)?;
        let mut it = BlockIterator::new(block.data);
        it.seek_to_first();
        self.current_block_iter = Some(it);

        Ok(true)
    }

    /// Return the next *point entry* (Put/Delete) in the scan key range,
    /// automatically advancing to the next block as needed.
    fn next_point_or_delete(&mut self) -> Option<Record> {
        loop {
            let it = self.current_block_iter.as_mut()?;

            if let Some(item) = it.next_entry() {
                // Stop when out of scan range
                if item.key.as_slice() >= self.end_key.as_slice() {
                    return None;
                }

                if item.is_delete {
                    return Some(Record::Delete {
                        key: item.key,
                        lsn: item.lsn,
                        timestamp: item.timestamp,
                    });
                }

                return Some(Record::Put {
                    key: item.key,
                    value: item.value,
                    lsn: item.lsn,
                    timestamp: item.timestamp,
                });
            }

            // end of block - load next
            match self.load_next_block() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(e) => {
                    tracing::warn!(?e, "error loading next block during scan");
                    return None;
                }
            }
        }
    }

    /// Return the next range tombstone that overlaps the scan range.
    fn next_range_delete(&mut self) -> Option<Record> {
        while self.pending_range_idx < self.sstable.range_deletes.data.len() {
            let r = &self.sstable.range_deletes.data[self.pending_range_idx];

            // Skip ranges completely left of scan window
            if r.end_key.as_slice() <= self.start_key.as_slice() {
                self.pending_range_idx += 1;
                continue;
            }

            // Stop when range start is beyond end of scan range
            if r.start_key.as_slice() >= self.end_key.as_slice() {
                return None;
            }

            // Emit range
            self.pending_range_idx += 1;

            return Some(Record::RangeDelete {
                start: r.start_key.clone(),
                end: r.end_key.clone(),
                lsn: r.lsn,
                timestamp: r.timestamp,
            });
        }

        None
    }

    /// Ensure that `next_range` is populated.
    fn fill_range(&mut self) {
        if self.next_range.is_none() {
            self.next_range = self.next_range_delete();
        }
    }

    /// Ensure that `next_point` is populated.
    fn fill_point(&mut self) {
        if self.next_point.is_none() {
            self.next_point = self.next_point_or_delete();
        }
    }
}

impl<S: Deref<Target = SSTable>> Iterator for ScanIterator<S> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        self.fill_range();
        self.fill_point();

        match (&self.next_range, &self.next_point) {
            (None, None) => None, // end of scan

            (Some(_), None) => self.next_range.take(),
            (None, Some(_)) => self.next_point.take(),

            (Some(r), Some(p)) => {
                if r.key()
                    .cmp(p.key())
                    .then_with(|| p.lsn().cmp(&r.lsn()))
                    .is_le()
                {
                    self.next_range.take()
                } else {
                    self.next_point.take()
                }
            }
        }
    }
}
