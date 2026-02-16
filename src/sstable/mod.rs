//! Sorted String Table (SSTable) Module
//!
//! This module implements an **immutable**, **disk-backed**, and **versioned** sorted string table (SSTable)
//! suitable for embedded databases and key-value storage engines.  
//! It provides **multi-version support**, **range tombstones**, **bloom filter-based point lookups**,
//! and **LSN+timestamp ordering** for crash-safe reads and merges.
//!
//! ## Design Overview
//!
//! SSTables store key-value data in **sorted blocks**, allowing efficient point queries and range scans.
//! Each SSTable is immutable once written. Updates (including deletes) are represented as new entries
//! with higher **LSN** (Log Sequence Number) and **timestamp**, enabling multiple versions of the same key.
//!
//! **Point deletes** and **range tombstones** are stored as special entries to allow fast pruning
//! during reads and merges. Bloom filters are maintained per SSTable for quick existence checks
//! before scanning blocks.
//!
//! Data is serialized using [`bincode`] with **fixed integer encoding**, and block-level CRC32
//! checksums ensure corruption detection.
//!
//! # On-disk layout
//!
//! ```text
//! [HEADER_BYTES]
//! [DATA_BLOCK_LEN_LE][DATA_BLOCK_BYTES][DATA_BLOCK_CRC32_LE]
//! [DATA_BLOCK_LEN_LE][DATA_BLOCK_BYTES][DATA_BLOCK_CRC32_LE]
//! ...
//! [BLOOM_FILTER_LEN_LE][BLOOM_FILTER_BYTES][BLOOM_FILTER_CRC32_LE]
//! [RANGE_DELETES_LEN_LE][RANGE_DELETES_BYTES][RANGE_DELETES_CRC32_LE]
//! [PROPERTIES_LEN_LE][PROPERTIES_BYTES][PROPERTIES_CRC32_LE]
//! [METAINDEX_LEN_LE][METAINDEX_BYTES][METAINDEX_CRC32_LE]
//! [INDEX_LEN_LE][INDEX_BYTES][INDEX_CRC32_LE]
//! [FOOTER_BYTES]
//! ```
//!
//! - **Header** — [`SSTableHeader`] structure with CRC32 checksum.  
//! - **Data blocks** — store serialized [`SSTableCell`] entries (key-value or tombstone).  
//! - **Bloom filter block** — fast existence checks for point keys.  
//! - **Range deletes block** — serialized [`SSTableRangeTombstoneCell`] entries.  
//! - **Properties block** — table metadata such as min/max key, LSNs, timestamps, record counts.  
//! - **Metaindex block** — directory of blocks (bloom, properties, range deletes) for easy lookup.  
//! - **Index block** — directory of data blocks, allowing binary search for keys.  
//! - **Footer** — [`SSTableFooter`] structure containing offsets, sizes, and CRC32 checksum.
//!
//! # Concurrency model
//!
//! - SSTables are **immutable**, so reads are lock-free and thread-safe.  
//! - Multiple readers can safely access the same SSTable concurrently.  
//! - No writes occur in-place; updates are appended via **new SSTables**.  
//! - Multi-versioning ensures that readers always see a consistent snapshot.
//!
//! # Guarantees
//!
//! - **Immutability:** Once written, an SSTable is never modified.  
//! - **Multi-version support:** Multiple versions of the same key are preserved with LSN+timestamp ordering.  
//! - **Range deletes:** Efficient representation and merging of point/range deletions.  
//! - **Integrity:** Each block and footer contains CRC32 checksums to detect corruption.  
//! - **Fast point lookups:** Bloom filter reduces unnecessary block scans.  
//! - **Safe merges:** SSTables can be safely merged without affecting existing readers.  
//! - **Crash recovery:** Files are written atomically using temporary paths and rename-on-success.
//!
//!
//! -----------------------------------------------------------------------------------------------
//!
//!
//! # SSTable Data Block Iterator
//!
//! This module defines the low-level, block-local iterator used by the SSTable reader.
//! Unlike the higher-level SSTable scan interface, this iterator only walks a single
//! **data block** and returns fully decoded `BlockItem` entries.
//!
//! ## Purpose
//!
//! - Decode the compact on-disk representation of block entries (`SSTableCell`).
//! - Support forward iteration through a data block.
//! - Provide efficient `seek_to_first()` and `seek_to(key)` operations.
//! - Form the basis for higher-level SSTable iterators (`Record`, merging, etc.).
//!
//! ## Block Format Overview
//!
//! Each data block contains a consecutive sequence of encoded cells.  
//! The layout of each entry is:
//!
//! ```text
//! [SSTableCell header][KEY_BYTES][VALUE_BYTES]
//! ```
//!
//! The header (`SSTableCell`) includes fixed-integer-encoded metadata:
//!
//! - `key_len` (u32)
//! - `value_len` (u32)
//! - `lsn` (u64)
//! - `timestamp` (u64)
//! - `is_delete` (bool)
//!
//! After decoding the header, the iterator reads `key_len` bytes and then `value_len` bytes.
//!
//! ## Characteristics
//!
//! - **Linear seek**: `seek_to()` performs a linear scan within the block.  
//!   Blocks are intentionally small (typically 4–32 KB), so linear search is efficient.
//!
//! - **Safe failure behavior**: If corruption or truncation is encountered, the iterator
//!   stops and treats the block as exhausted.
//!
//! - **Independent state**: A `BlockIterator` holds its own cursor and does not share
//!   mutable state with other iterators.
//!
//!
//! The iterator also implements `Iterator<Item = BlockItem>` for idiomatic for-loops.
//!
//!
//! -----------------------------------------------------------------------------------------------
//!
//!
//! # SSTable Scan Iterator
//!
//! `SSTableScanIterator` provides a **sorted forward scan** over a single SSTable,
//! yielding all point entries (`Put`, `Delete`) and range tombstones (`RangeDelete`)
//! that overlap a user-specified key range.
//!
//! ## Responsibilities
//!
//! - Perform **sorted iteration** across multiple data blocks.
//! - Respect a user-defined half-open range:  
//!   `start_key <= key < end_key`.
//! - Yield:
//!   - point inserts (`Put`),
//!   - point deletions (`Delete`),
//!   - range deletions (`RangeDelete`).
//! - Decode blocks lazily and sequentially.
//! - Automatically advance to subsequent blocks.
//!
//! ## Ordering Guarantees
//!
//! The iterator returns items in **key order**, with this rule:
//!
//! 1. All **range tombstones** that start before `start_key` are emitted *first*  
//!    (but only if they overlap the scan range).
//! 2. All **point entries** (`Put` or `Delete`) appear in increasing key order.
//! 3. Additional range tombstones inside the scan window are emitted as encountered
//!    before point entries at later keys.
//!
//! The iterator **does not** merge multiple SSTables; merging is performed by higher
//! levels (e.g. LSM compaction or a merging iterator).
//!
//! ## Block Loading Strategy
//!
//! - At creation, the iterator uses `find_block_for_key(start_key)` to position at the
//!   correct block for the beginning of the range.
//! - Each block is decoded on demand using the `BlockIterator`.
//! - When a block becomes exhausted, the iterator loads the next block automatically.
//!
//! ## Range Tombstone Semantics
//!
//! Range tombstones are separate from point entries and live in the SSTable’s
//! range-tombstone block. The scan iterator does **not** attempt to filter or
//! suppress entries based on tombstone coverage.  
//! This is deliberate: **upper layers perform visibility resolution** (e.g. merging
//! with newer levels, applying MVCC rules).
//!
//! The iterator simply guarantees that all tombstones whose interval intersects the
//! scan window are emitted in sorted order with respect to `start_key`.
//!
//!
//! -----------------------------------------------------------------------------------------------
//!
//!
//! # SSTable Builder
//!
//! This module provides the logic for constructing a new SSTable file from two
//! sorted streams:
//!
//! - **Point entries** (`MemtablePointEntry`): key/value pairs or point tombstones.
//! - **Range tombstones** (`MemtableRangeTombstone`): delete intervals covering key ranges.
//!
//! The function [`build_from_iterators`] consumes both iterators in sorted order and
//! produces a fully-structured SSTable containing:
//!
//! - Header block
//! - Data blocks (encoded `SSTableCell` objects)
//! - Bloom filter
//! - Range tombstone block
//! - Properties block
//! - Metaindex block
//! - Index block
//! - Footer
//!
//! All on-disk structures strictly follow the format defined by the `SSTable*`
//! structs elsewhere in this module.
//!
//! ## Input Requirements
//!
//! - `point_entries` **must be sorted by key** so that all entries for a given key are
//!   **grouped (adjacent)**. Duplicate keys **are allowed** — SSTables may store
//!   multiple versions of the same logical key. The builder accepts duplicates and
//!   writes them into data blocks — resolution between versions (for `get`) is done
//!   by picking the highest `lsn` (tie-breaker: `timestamp`) when reading an SST.
//!
//! - `range_tombstones` **must be sorted by start key**. Overlapping range tombstones
//!   are allowed; per-key resolution prefers the tombstone with the highest LSN
//!   (tie-breaker: timestamp). The builder will store range tombstones as supplied;
//!   merging/deduplication is a job for compaction if desired.
//!
//! ## Output Guarantees
//! - All point entries are grouped into data blocks and written with per-block CRC32.
//! - Bloom filter is built from keys (including point tombstones).
//! - Properties capture min/max keys, LSNs, timestamps and counts.
//! - The final file is written atomically using a `.tmp -> final` rename.
//!
//! ## Notes
//! - The builder assumes iterators are logically sorted as described above.
//! - Duplicate point keys are supported and **must be adjacent** in the `point_entries` iterator.
//!
//! ## Error Conditions
//!
//! - Attempting to build an SSTable from *both* empty iterators returns an error.
//! - I/O errors and bincode encode errors are propagated.
//! - CRC calculations are checked and stored for every block.

// ------------------------------------------------------------------------------------------------
// Unit tests
// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests;

// ------------------------------------------------------------------------------------------------
// Includes
// ------------------------------------------------------------------------------------------------

use std::{
    fs::{File, OpenOptions, rename},
    io::{self, BufWriter, Seek, SeekFrom, Write},
    mem,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::engine::Record;
use bincode::{
    config::{Configuration, Fixint, LittleEndian, standard},
    decode_from_slice, encode_to_vec,
};
use bloomfilter::Bloom;
use crc32fast::Hasher as Crc32;
use memmap2::Mmap;
use thiserror::Error;
use tracing::error;

// ------------------------------------------------------------------------------------------------
// Constants
// ------------------------------------------------------------------------------------------------

const SST_HDR_MAGIC: [u8; 4] = *b"SST0";
const SST_HDR_VERSION: u32 = 1;
const SST_BLOOM_FILTER_FALSE_POSITIVE_RATE: f64 = 0.01;
const SST_DATA_BLOCK_MAX_SIZE: usize = 4096;
const SST_FOOTER_SIZE: usize = 44;
const SST_HDR_SIZE: usize = 12;
const SST_DATA_BLOCK_LEN_SIZE: usize = 4;
const SST_DATA_BLOCK_CHECKSUM_SIZE: usize = 4;

// ------------------------------------------------------------------------------------------------
// Error Types
// ------------------------------------------------------------------------------------------------

/// Represents possible errors returned by [`Memtable`] operations.
#[derive(Debug, Error)]
pub enum SSTableError {
    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Serialization error.
    #[error("Serialization (encode) error: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    /// Deserialization error.
    #[error("Deserialization (decode) error: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    /// Internal invariant violation or poisoned lock.
    #[error("Internal error: {0}")]
    Internal(String),

    /// Checksum mistmatch.
    #[error("Checksum mismatch")]
    ChecksumMismatch,
}

// ------------------------------------------------------------------------------------------------
// Sorted String Table structures
// ------------------------------------------------------------------------------------------------

/// SSTable file header, written at the beginning of the SSTable.
/// Contains a magic number, version, and CRC32 checksum for integrity.
#[derive(Default, bincode::Encode, bincode::Decode)]
pub(crate) struct SSTableHeader {
    /// Magic bytes to identify SSTable format (`b"SST0"`).
    magic: [u8; 4],

    /// SSTable format version.
    version: u32,

    /// CRC32 checksum of the header (excluding this field).
    header_crc: u32,
}

/// Represents a data block in the SSTable, which contains serialized key-value entries.
#[derive(bincode::Encode, bincode::Decode)]
struct SSTableDataBlock {
    /// Raw serialized block data.
    data: Vec<u8>,
}

/// Represents a Bloom filter block used to quickly check the presence of point keys.
#[derive(bincode::Encode, bincode::Decode)]
pub(crate) struct SSTableBloomBlock {
    /// Serialized bloom filter bytes.
    data: Vec<u8>,
}

/// Represents a block containing range tombstones.
#[derive(bincode::Encode, bincode::Decode)]
pub(crate) struct SSTableRangeTombstoneDataBlock {
    /// List of serialized range tombstone cells.
    data: Vec<SSTableRangeTombstoneCell>,
}

/// Metadata block containing SSTable-level properties and statistics.
#[derive(bincode::Encode, bincode::Decode)]
pub struct SSTablePropertiesBlock {
    /// Creation timestamp (UNIX epoch nanos).
    pub creation_timestamp: u64,

    /// Total number of records in the SSTable.
    pub record_count: u64,

    /// Number of point deletions.
    pub tombstone_count: u64,

    /// Number of range tombstones.
    pub range_tombstones_count: u64,

    /// Minimum LSN present in this SSTable.
    pub min_lsn: u64,

    /// Maximum LSN present in this SSTable.
    pub max_lsn: u64,

    /// Minimum timestamp in this SSTable.
    pub min_timestamp: u64,

    /// Maximum timestamp in this SSTable.
    pub max_timestamp: u64,

    /// Minimum key in the SSTable.
    pub min_key: Vec<u8>,

    /// Maximum key in the SSTable.
    pub max_key: Vec<u8>,
}

/// Index entry pointing to a specific data block.
#[derive(bincode::Encode, bincode::Decode)]
pub(crate) struct SSTableIndexEntry {
    /// Key that separates this block from the next in sorted order.
    separator_key: Vec<u8>,

    /// Block handle containing offset and size of the data block.
    handle: BlockHandle,
}

/// SSTable footer, stored at the very end of the file.
#[derive(bincode::Encode, bincode::Decode)]
pub(crate) struct SSTableFooter {
    /// Handle of the metaindex block, containing references to:
    /// - bloom filter block
    /// - properties block
    /// - range tombstone blocks
    metaindex: BlockHandle,

    /// Handle of the main index block, mapping separator keys to data blocks.
    index: BlockHandle,

    /// Total size of the SSTable file, including this footer.
    total_file_size: u64,

    /// CRC32 checksum computed over the footer fields except this one.
    footer_crc32: u32,
}

/// Represents a single key-value entry (or tombstone) in a data block.
#[derive(bincode::Encode, bincode::Decode)]
struct SSTableCell {
    /// Length of the key in bytes.
    key_len: u32,

    /// Length of the value in bytes (0 if deleted).
    value_len: u32,

    /// Timestamp of the operation.
    timestamp: u64,

    /// Whether this entry represents a deletion.
    is_delete: bool,

    /// Log Sequence Number for versioning.
    lsn: u64,
}

/// Represents a range tombstone marking deletion of keys in `[start_key, end_key)`.
#[derive(bincode::Encode, bincode::Decode)]
struct SSTableRangeTombstoneCell {
    /// Start key of the deleted range (inclusive).
    start_key: Vec<u8>,

    /// End key of the deleted range (exclusive).
    end_key: Vec<u8>,

    /// Timestamp of the deletion.
    timestamp: u64,

    /// LSN of the deletion.
    lsn: u64,
}

/// Handle to a block in the SSTable file, specifying its offset and size.
#[derive(Debug, bincode::Encode, bincode::Decode)]
struct BlockHandle {
    /// Offset of the block in the SSTable file.
    offset: u64,

    /// Size of the block in bytes, including length prefix and checksum.
    size: u64,
}

/// Represents a single entry in the metaindex block.
#[derive(Debug, bincode::Encode, bincode::Decode)]
struct MetaIndexEntry {
    /// Name of the block (e.g., "filter.bloom", "meta.properties").
    name: String,

    /// Handle pointing to the block location.
    handle: BlockHandle,
}

/// Result of a single key lookup in an SSTable.
#[derive(Debug, PartialEq, Clone)]
pub enum SSTGetResult {
    /// A value stored in this SST.
    Put {
        /// Stored value.
        value: Vec<u8>,
        /// LSN of this version.
        lsn: u64,
        /// Timestamp of this version.
        timestamp: u64,
    },

    /// A point delete for this key.
    Delete {
        /// LSN of the delete.
        lsn: u64,
        /// Timestamp of the delete.
        timestamp: u64,
    },

    /// The key falls inside a range deletion.
    RangeDelete {
        /// LSN of the range tombstone.
        lsn: u64,
        /// Timestamp of the range tombstone.
        timestamp: u64,
    },

    /// This SST has no information about the key.
    NotFound,
}

impl SSTGetResult {
    /// Returns the **LSN** (logical sequence number) associated with this get item.
    pub fn lsn(&self) -> u64 {
        match self {
            Self::Put { lsn, .. } => *lsn,
            SSTGetResult::Delete { lsn, .. } => *lsn,
            SSTGetResult::RangeDelete { lsn, .. } => *lsn,
            SSTGetResult::NotFound => 0,
        }
    }

    /// Returns the **timestamp** associated with this get item.
    pub fn timestamp(&self) -> u64 {
        match self {
            Self::Put { timestamp, .. } => *timestamp,
            SSTGetResult::Delete { timestamp, .. } => *timestamp,
            SSTGetResult::RangeDelete { timestamp, .. } => *timestamp,
            SSTGetResult::NotFound => 0,
        }
    }
}

/// A fully memory-mapped, immutable **Sorted String Table (SSTable)**.
pub struct SSTable {
    /// Memory-mapped file containing the full SSTable bytes.
    pub mmap: Mmap,

    /// Parsed header block containing magic/version information.
    pub header: SSTableHeader,

    /// Bloom filter block for fast membership tests.
    pub bloom: SSTableBloomBlock,

    /// Properties block with statistics and metadata.
    pub properties: SSTablePropertiesBlock,

    /// Range delete tombstone block.
    pub range_deletes: SSTableRangeTombstoneDataBlock,

    /// Index entries mapping key ranges to data blocks.
    pub index: Vec<SSTableIndexEntry>,

    /// Footer containing block handles and file integrity data.
    pub footer: SSTableFooter,
}

// ------------------------------------------------------------------------------------------------
// Sorted String Table Core
// ------------------------------------------------------------------------------------------------

impl SSTable {
    /// Opens an SSTable from disk, verifies its integrity, and loads all top-level
    /// metadata structures.
    ///
    /// # Overview
    ///
    /// This method performs the full SSTable loading pipeline:
    ///
    /// 1. **Open and mmap the file**  
    ///    The entire table is memory-mapped for fast zero-copy block access.
    ///
    /// 2. **Decode and verify the header**  
    ///    - Deserialized using `bincode`  
    ///    - Header CRC verified after zeroing the `header_crc` field  
    ///    - Magic string and version must match engine constants
    ///
    /// 3. **Decode and verify the footer**  
    ///    - Footer CRC is verified similarly  
    ///    - Contains block handles for `metaindex` and `index`
    ///
    /// 4. **Load the metaindex block**  
    ///    This tells us where the bloom filter, properties block,
    ///    and range deletions block are stored.
    ///
    /// 5. **Load individual blocks**  
    ///    - Bloom filter (optional; missing filter → empty bloom)  
    ///    - Properties block (required)  
    ///    - Range tombstones block (optional)  
    ///    - Index block (required)
    ///
    /// 6. **Return a fully initialized `SSTable` instance**
    ///
    /// # Errors
    ///
    /// - [`SSTableError::ChecksumMismatch`]  
    ///   If header or footer checksums fail.
    ///
    /// - [`SSTableError::Internal`]  
    ///   For malformed blocks, mismatched magic/version, missing properties block,
    ///   out-of-bounds reads, truncated block data, or unrecognized metaindex entries.
    ///
    /// # Safety
    ///
    /// Uses `unsafe { Mmap::map(...) }` but is memory-safe because:
    ///
    /// - The file is never written after creation (immutable)  
    /// - The mmap is read-only  
    /// - All block boundaries are verified before slicing
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SSTableError> {
        let file = File::open(path)?;

        let mmap = unsafe { Mmap::map(&file)? };
        let config = standard().with_fixed_int_encoding();

        let file_len = mmap.len();
        if file_len < SST_FOOTER_SIZE {
            return Err(SSTableError::Internal("File too small".into()));
        }

        let (mut header, _) = decode_from_slice::<SSTableHeader, _>(&mmap[..SST_HDR_SIZE], config)?;
        let header_checksum = header.header_crc;

        header.header_crc = 0;

        let header_bytes = encode_to_vec(&header, config)?;

        let mut hasher = Crc32::new();
        hasher.update(&header_bytes);
        let header_comp_checksum = hasher.finalize();

        if header_checksum != header_comp_checksum {
            return Err(SSTableError::ChecksumMismatch);
        }

        if header.magic != SST_HDR_MAGIC {
            return Err(SSTableError::Internal(
                "SSTable header magic mismatch".into(),
            ));
        }

        if header.version != SST_HDR_VERSION {
            return Err(SSTableError::Internal(
                "SSTable header version mismatch".into(),
            ));
        }

        let footer_start = file_len - SST_FOOTER_SIZE;
        let (mut footer, _) = decode_from_slice::<SSTableFooter, _>(&mmap[footer_start..], config)?;

        let footer_checksum = footer.footer_crc32;
        footer.footer_crc32 = 0;

        let footer_bytes = encode_to_vec(&footer, config)?;

        let mut hasher = Crc32::new();
        hasher.update(&footer_bytes);
        let footer_comp_checksum = hasher.finalize();

        if footer_checksum != footer_comp_checksum {
            return Err(SSTableError::ChecksumMismatch);
        }

        let metaindex_data = Self::read_block_bytes(&mmap, &footer.metaindex)?;
        let (meta_entries, _) =
            decode_from_slice::<Vec<MetaIndexEntry>, _>(&metaindex_data, config)?;

        let mut bloom_block: Option<BlockHandle> = None;
        let mut propertires_block: Option<BlockHandle> = None;
        let mut range_deletes_block: Option<BlockHandle> = None;

        for entry in meta_entries {
            match entry.name.as_str() {
                "filter.bloom" => bloom_block = Some(entry.handle),
                "meta.properties" => propertires_block = Some(entry.handle),
                "meta.range_deletes" => range_deletes_block = Some(entry.handle),
                _ => return Err(SSTableError::Internal("Unexpected match".into())),
            }
        }

        let bloom = if let Some(bh) = bloom_block {
            let bloom_bytes = Self::read_block_bytes(&mmap, &bh)?;
            let (bloom, _) = decode_from_slice::<SSTableBloomBlock, _>(&bloom_bytes, config)
                .map_err(|e| SSTableError::Internal(e.to_string()))?;
            bloom
        } else {
            let bloom: Bloom<Vec<u8>> =
                Bloom::new_for_fp_rate(1, SST_BLOOM_FILTER_FALSE_POSITIVE_RATE)
                    .map_err(|e| SSTableError::Internal(e.to_string()))?;
            SSTableBloomBlock {
                data: bloom.as_slice().to_vec(),
            }
        };

        let properties = if let Some(pb) = propertires_block {
            let pbytes = Self::read_block_bytes(&mmap, &pb)?;
            let (properties, _) = decode_from_slice::<SSTablePropertiesBlock, _>(&pbytes, config)?;
            properties
        } else {
            return Err(SSTableError::Internal("SSTable missing properties".into()));
        };

        let range_deletes = if let Some(rh) = range_deletes_block {
            let rbytes = Self::read_block_bytes(&mmap, &rh)?;
            let (ranges, _) =
                decode_from_slice::<Vec<SSTableRangeTombstoneCell>, _>(&rbytes, config)?;
            SSTableRangeTombstoneDataBlock { data: ranges }
        } else {
            SSTableRangeTombstoneDataBlock { data: Vec::new() }
        };

        let index_bytes = Self::read_block_bytes(&mmap, &footer.index)?;
        let (index_entries, _) =
            decode_from_slice::<Vec<SSTableIndexEntry>, _>(&index_bytes, config)?;

        Ok(Self {
            mmap,
            header,
            bloom,
            properties,
            range_deletes,
            index: index_entries,
            footer,
        })
    }

    /// Performs a **single-SST lookup** of a key.
    ///
    /// Returns the “raw MVCC” result from this SSTable alone. Higher-level LSM
    /// layers apply merging across tables.
    ///
    /// # Lookup pipeline
    ///
    /// 1. **Check range tombstones**  
    ///    Determines whether the key is inside a range deletion.
    ///
    /// 2. **Bloom filter check**  
    ///    If the bloom filter says the key is impossible, skip data block search.
    ///
    /// 3. **Find data block using the index**  
    ///    Binary search on separator keys.
    ///
    /// 4. **Search inside the block**  
    ///    Using `BlockIterator`, seek to the key and collect the newest version.
    ///
    /// 5. **Merge point entries with range tombstone**  
    ///    Range deletes override older point entries.
    ///
    /// # Returns
    ///
    /// An [`SSTGetResult`] variant:  
    /// - `Put` – newest put  
    /// - `Delete` – newest point delete  
    /// - `RangeDelete` – covered by a tombstone  
    /// - `NotFound` – no information in this SSTable
    ///
    /// # MVCC rules
    ///
    /// Version comparison uses:
    /// - Primary: LSN  
    /// - Secondary: timestamp (tie-breaking)
    pub fn get(&self, key: &[u8]) -> Result<SSTGetResult, SSTableError> {
        // 1) Check range tombstones first
        let range_info = self.covering_range_for_key(key);

        // 2) Bloom filter check (only point keys)
        let bloom_maybe_present = if !self.bloom.data.is_empty() {
            match Bloom::from_slice(&self.bloom.data) {
                Ok(bloom) => bloom.check(key),
                Err(_) => true, // corrupted bloom → fallback to full search
            }
        } else {
            true // no bloom → always search block
        };

        if !bloom_maybe_present {
            return Ok(match range_info {
                Some((lsn, timestamp)) => SSTGetResult::RangeDelete { lsn, timestamp },
                None => SSTGetResult::NotFound,
            });
        }

        // 3) Find the block (if any)
        if self.index.is_empty() {
            return Ok(match range_info {
                Some((lsn, timestamp)) => SSTGetResult::RangeDelete { lsn, timestamp },
                None => SSTGetResult::NotFound,
            });
        }

        let block_idx = self.find_block_for_key(key);
        let entry = &self.index[block_idx];

        let cfg = standard().with_fixed_int_encoding();
        let raw = Self::read_block_bytes(&self.mmap, &entry.handle)?;
        let (block, _) = decode_from_slice::<SSTableDataBlock, _>(&raw, cfg)?;

        // 4) Scan block using BlockIterator (point keys)
        let mut iter = BlockIterator::new(block.data);
        iter.seek_to(key);
        let mut latest: Option<SSTGetResult> = None;

        while let Some(item) = iter.next() {
            if item.key != key {
                break;
            }

            let candidate = if item.is_delete {
                SSTGetResult::Delete {
                    lsn: item.lsn,
                    timestamp: item.timestamp,
                }
            } else {
                SSTGetResult::Put {
                    value: item.value.to_vec(),
                    lsn: item.lsn,
                    timestamp: item.timestamp,
                }
            };

            latest = Some(match &latest {
                Some(existing) => {
                    if candidate.lsn() > existing.lsn() {
                        candidate
                    } else if candidate.lsn() == existing.lsn() {
                        // tie-breaker by timestamp
                        if candidate.timestamp() > existing.timestamp() {
                            candidate
                        } else {
                            existing.clone()
                        }
                    } else {
                        existing.clone()
                    }
                }
                None => candidate,
            });
        }

        // 5) Merge point vs range tombstone (LSN + timestamp)
        match (latest, range_info) {
            // No point, no range delete → not found
            (None, None) => Ok(SSTGetResult::NotFound),

            // Point exists, no range delete → point result wins
            (Some(r), None) => Ok(r),

            // No point entry, but we have a range delete
            (None, Some((lsn, timestamp))) => Ok(SSTGetResult::RangeDelete { lsn, timestamp }),

            // Everything else: point_result = Some(_), range_lsn = Some(_)
            (Some(point), Some((r_lsn, r_ts))) => {
                let result = match point {
                    SSTGetResult::Put {
                        value,
                        lsn: p_lsn,
                        timestamp: p_ts,
                    } => {
                        if r_lsn > p_lsn || (r_lsn == p_lsn && r_ts > p_ts) {
                            SSTGetResult::RangeDelete {
                                lsn: r_lsn,
                                timestamp: r_ts,
                            }
                        } else {
                            SSTGetResult::Put {
                                value,
                                lsn: p_lsn,
                                timestamp: p_ts,
                            }
                        }
                    }
                    SSTGetResult::Delete {
                        lsn: d_lsn,
                        timestamp: d_ts,
                    } => {
                        if r_lsn > d_lsn || (r_lsn == d_lsn && r_ts > d_ts) {
                            SSTGetResult::RangeDelete {
                                lsn: r_lsn,
                                timestamp: r_ts,
                            }
                        } else {
                            SSTGetResult::Delete {
                                lsn: d_lsn,
                                timestamp: d_ts,
                            }
                        }
                    }
                    SSTGetResult::RangeDelete {
                        lsn: rd_lsn,
                        timestamp: rd_ts,
                    } => {
                        let (lsn, ts) = if r_lsn > rd_lsn || (r_lsn == rd_lsn && r_ts > rd_ts) {
                            (r_lsn, r_ts)
                        } else {
                            (rd_lsn, rd_ts)
                        };
                        SSTGetResult::RangeDelete { lsn, timestamp: ts }
                    }
                    SSTGetResult::NotFound => SSTGetResult::RangeDelete {
                        lsn: r_lsn,
                        timestamp: r_ts,
                    },
                };

                Ok(result)
            }
        }
    }

    /// Returns a range-scan iterator over this SSTable.
    ///
    /// The iterator yields **raw MVCC entries** (Put/Delete/RangeDelete) in key order.
    /// Key ordered ascending with LSN ordered descending within each key.
    /// Higher layers of the LSM tree (merging iterators) are responsible for
    /// de-duplicating versions and reconciling deletes.
    ///
    /// # Parameters
    ///
    /// - `start_key` — inclusive start of scan  
    /// - `end_key` — exclusive upper bound of scan  
    ///
    /// # Returns
    ///
    /// [`SSTableScanIterator`] which merges:
    ///
    /// - data blocks covering the range  
    /// - range tombstone iterator  
    ///
    /// to produce sorted MVCC entries.
    pub fn scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<impl Iterator<Item = Record>, SSTableError> {
        SSTableScanIterator::new(self, start_key.to_vec(), end_key.to_vec())
    }

    /// Reads a block referenced by a [`BlockHandle`] from the mmap and verifies
    /// its checksum.
    fn read_block_bytes(mmap: &Mmap, handle: &BlockHandle) -> Result<Vec<u8>, SSTableError> {
        let start = handle.offset as usize;
        let size = handle.size as usize;

        if start + size > mmap.len() {
            return Err(SSTableError::Internal("Block out of range".into()));
        }

        let mut cursor = start;

        let len_bytes: [u8; SST_DATA_BLOCK_LEN_SIZE] = mmap
            [cursor..cursor + SST_DATA_BLOCK_LEN_SIZE]
            .try_into()
            .map_err(|_| SSTableError::Internal("Short block length".into()))?;
        let content_len = u32::from_le_bytes(len_bytes) as usize;
        cursor += SST_DATA_BLOCK_LEN_SIZE;

        if start + content_len > mmap.len() {
            return Err(SSTableError::Internal("Block out of range".into()));
        }

        let content = &mmap[cursor..cursor + content_len];
        cursor += content_len;

        let checksum_bytes: [u8; SST_DATA_BLOCK_CHECKSUM_SIZE] = mmap
            [cursor..cursor + SST_DATA_BLOCK_CHECKSUM_SIZE]
            .try_into()
            .map_err(|_| SSTableError::Internal("Short checksum".into()))?;
        let stored_checksum = u32::from_le_bytes(checksum_bytes);

        let mut hasher = Crc32::new();
        hasher.update(content);
        let computed_checksum = hasher.finalize();

        if computed_checksum != stored_checksum {
            return Err(SSTableError::ChecksumMismatch);
        }

        Ok(content.to_vec())
    }

    /// Locates the index entry whose block may contain the given `key`.
    ///
    /// Uses binary search over `separator_key`, which stores the first key in each
    /// block.
    fn find_block_for_key(&self, key: &[u8]) -> usize {
        if self.index.is_empty() {
            return 0;
        }

        match self
            .index
            .binary_search_by(|entry| entry.separator_key.as_slice().cmp(key))
        {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        }
    }

    /// Returns the newest (highest LSN, then highest timestamp) range tombstone
    /// that covers the given `key`, if any.
    fn covering_range_for_key(&self, key: &[u8]) -> Option<(u64, u64)> {
        let mut res: Option<(u64, u64)> = None;
        for rd in &self.range_deletes.data {
            if key >= rd.start_key.as_slice() && key < rd.end_key.as_slice() {
                res = Some(match res {
                    Some((prev_lsn, prev_ts)) => {
                        if rd.lsn > prev_lsn || (rd.lsn == prev_lsn && rd.timestamp > prev_ts) {
                            (rd.lsn, rd.timestamp)
                        } else {
                            (prev_lsn, prev_ts)
                        }
                    }
                    None => (rd.lsn, rd.timestamp),
                });
            }
        }
        res
    }
}

// ------------------------------------------------------------------------------------------------
// Sorted String Table - Data Block Iterator
// ------------------------------------------------------------------------------------------------

/// A fully decoded entry from a data block.
///
/// This is the in-memory representation of a single SSTable cell after decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockItem {
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

/// Iterator over the entries contained within a single SSTable data block.
///
/// This iterator:
///
/// - Decodes `SSTableCell` boundaries using [`bincode`] with fixed-int encoding.
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

    /// bincode decoding configuration using little-endian + fixed-int encoding.
    config: Configuration<LittleEndian, Fixint>,
}
impl BlockIterator {
    /// Create a new iterator from already-decoded block bytes.
    ///
    /// The provided `data` slice must contain a concatenation of encoded `SSTableCell`s.
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            cursor: 0,
            config: standard().with_fixed_int_encoding(),
        }
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
            match decode_from_slice::<SSTableCell, _>(&self.data[self.cursor..], self.config) {
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
                    eprintln!("decode error at cursor {}: {:?}", self.cursor, e);
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
    pub fn next_item(&mut self) -> Option<BlockItem> {
        if self.cursor >= self.data.len() {
            return None;
        }

        match decode_from_slice::<SSTableCell, _>(&self.data[self.cursor..], self.config) {
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

                Some(BlockItem {
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
    pub fn is_end(&self) -> bool {
        self.cursor >= self.data.len()
    }
}

/// Implements idiomatic Rust iteration over block entries.
impl Iterator for BlockIterator {
    type Item = BlockItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_item()
    }
}

// ------------------------------------------------------------------------------------------------
// Sorted String Table - Scan Iterator
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
pub struct SSTableScanIterator<'a> {
    /// Reference to the SSTable being scanned.
    sstable: &'a SSTable,

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

impl<'a> SSTableScanIterator<'a> {
    /// Create a new SSTable scan iterator for the half-open range  
    /// `start_key <= key < end_key`.
    pub fn new(
        sstable: &'a SSTable,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<Self, SSTableError> {
        if start_key >= end_key {
            return Err(SSTableError::Internal("scan start >= end".to_string()));
        }

        let current_block_index = sstable.find_block_for_key(start_key.as_slice());

        let block_iter = if current_block_index < sstable.index.len() {
            let entry = &sstable.index[current_block_index];
            let block_bytes = SSTable::read_block_bytes(&sstable.mmap, &entry.handle)?;
            let (block, _) = decode_from_slice::<SSTableDataBlock, _>(
                &block_bytes,
                standard().with_fixed_int_encoding(),
            )?;
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

        let (block, _) = decode_from_slice::<SSTableDataBlock, _>(
            &block_bytes,
            standard().with_fixed_int_encoding(),
        )?;
        let mut it = BlockIterator::new(block.data);
        it.seek_to_first();
        self.current_block_iter = Some(it);

        Ok(true)
    }

    /// Return the next *point entry* (Put/Delete) in the scan key range,
    /// automatically advancing to the next block as needed.
    fn next_point_or_delete(&mut self) -> Option<Record> {
        loop {
            let Some(it) = self.current_block_iter.as_mut() else {
                return None;
            };

            if let Some(item) = it.next_item() {
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
                } else {
                    return Some(Record::Put {
                        key: item.key,
                        value: item.value,
                        lsn: item.lsn,
                        timestamp: item.timestamp,
                    });
                }
            }

            // end of block - load next
            match self.load_next_block() {
                Ok(true) => continue,
                Ok(false) | Err(_) => return None,
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

impl<'a> Iterator for SSTableScanIterator<'a> {
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

// ------------------------------------------------------------------------------------------------
// Memtable structures
// ------------------------------------------------------------------------------------------------

/// A point mutation from the memtable: a Put or Delete.
#[derive(Debug, Clone)]
pub struct MemtablePointEntry {
    /// Key of the entry.
    pub key: Vec<u8>,

    /// Value of the entry; `None` indicates a point deletion.
    pub value: Option<Vec<u8>>,

    /// Log sequence number of this mutation.
    pub lsn: u64,

    /// Timestamp associated with this mutation.
    pub timestamp: u64,
}

/// A range deletion covering keys in the interval `[start, end)`.
#[derive(Debug, Clone)]
pub struct MemtableRangeTombstone {
    /// Inclusive start key of the range.
    pub start: Vec<u8>,

    /// Exclusive end key of the range.
    pub end: Vec<u8>,

    /// Log sequence number of this range deletion.
    pub lsn: u64,

    /// Timestamp associated with this mutation.
    pub timestamp: u64,
}

// ------------------------------------------------------------------------------------------------
// SSTable builder
// ------------------------------------------------------------------------------------------------

/// Build a complete SSTable file from two sorted iterators:
///
/// - `point_entries`: sorted by key, containing Put/Delete.
/// - `range_tombstones`: sorted by start key.
///
/// The function writes all required SSTable sections and atomically moves the
/// temporary file into place at the end.
///
/// # Parameters
///
/// - `path`: Output file path for the final SSTable. A temporary file with `.tmp`
///   extension is written first.
/// - `point_entries_count`: Expected number of point entries (used to size bloom filter).
/// - `point_entries`: Iterator yielding [`MemtablePointEntry`] in sorted order.
/// - `range_tombstones_count`: Expected number of range tombstones.
/// - `range_tombstones`: Iterator yielding [`MemtableRangeTombstone`] in sorted order.
///
/// # Input Requirements
///
/// - Both iterators **must be sorted** in ascending order:
///   - Point entries by key.
///   - Range tombstones by start key.
/// - Keys must be unique within point entries.
/// - Range tombstones must be non-overlapping or already merged if necessary.
///
/// # Output
///
/// Produces a valid SSTable on disk containing:
///
/// - Header block with CRC
/// - Data blocks (point entries)
/// - Bloom filter block
/// - Range tombstone block
/// - Properties block
/// - Metaindex block
/// - Index block
/// - Footer with CRC
///
/// # Errors
///
/// Returns:
///
/// - `Err(SSTableError::Internal(..))` if both iterators are empty.
/// - Various I/O errors from writing or seeking.
/// - `bincode` encode errors.
/// - Any internal CRC computation issues (should be unreachable).
///
/// # Atomicity
///
/// The write procedure is:
///
/// 1. Write everything to `path.tmp`.
/// 2. Flush and sync the file.
/// 3. Rename `path.tmp` → `path` atomically.
///
/// A crash cannot produce a partially-written SSTable.
///
/// # Performance Notes
///
/// - Uses `BufWriter` for efficient writes.
/// - Minimizes allocations by reusing block buffers.
/// - Bloom filter is sized using expected total count of keys and range tombstones.
pub fn build_from_iterators(
    path: impl AsRef<Path>,
    point_entries_count: usize,
    point_entries: impl Iterator<Item = MemtablePointEntry>,
    range_tombstones_count: usize,
    range_tombstones: impl Iterator<Item = MemtableRangeTombstone>,
) -> Result<(), SSTableError> {
    let mut point_entries_peekable = point_entries.peekable();
    let mut range_tombstones_peekable = range_tombstones.peekable();

    if point_entries_count == 0
        && point_entries_peekable.peek().is_none()
        && range_tombstones_count == 0
        && range_tombstones_peekable.peek().is_none()
    {
        return Err(SSTableError::Internal(
            "Empty iterators cannot build SSTable".into(),
        ));
    }

    let final_path = path.as_ref();
    let tmp_path = final_path.with_extension("tmp");

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;

    let mut writer = BufWriter::new(&mut file);
    let config = standard().with_fixed_int_encoding();

    // 1. Build and write header block
    let header = SSTableHeader {
        magic: SST_HDR_MAGIC,
        version: SST_HDR_VERSION,
        header_crc: 0,
    };

    let header_bytes = encode_to_vec(&header, config)?;

    let mut hasher = Crc32::new();
    hasher.update(&header_bytes);
    let header_checksum = hasher.finalize();

    let header_with_crc = SSTableHeader {
        header_crc: header_checksum,
        ..header
    };

    let header_bytes = encode_to_vec(&header_with_crc, config)?;
    let mut hasher = Crc32::new();
    hasher.update(&header_bytes);
    let header_checksum = hasher.finalize();

    writer.write_all(&header_bytes)?;
    writer.write_all(&header_checksum.to_le_bytes())?;

    let mut record_count: u64 = 0;
    let mut tombstone_count: u64 = 0;

    let mut min_lsn = u64::MAX;
    let mut max_lsn = 0;

    let mut min_timestamp = u64::MAX;
    let mut max_timestamp = 0;

    let mut min_key: Option<Vec<u8>> = None;
    let mut max_key: Option<Vec<u8>> = None;

    let mut current_block = Vec::<u8>::new();

    let mut index_entries: Vec<SSTableIndexEntry> = Vec::new();
    let mut bloom = Bloom::new_for_fp_rate(
        point_entries_count + range_tombstones_count,
        SST_BLOOM_FILTER_FALSE_POSITIVE_RATE,
    )
    .map_err(|e| SSTableError::Internal(e.to_string()))?;

    let mut block_last_key = Vec::new();
    let mut block_first_key: Option<Vec<u8>> = None;

    // 2. Build and write data blocks
    if let Some(next_entry) = point_entries_peekable.peek() {
        min_key = Some(next_entry.key.clone());
    }

    while let Some(entry) = point_entries_peekable.next() {
        record_count += 1;
        if entry.value.is_none() {
            tombstone_count += 1;
        }

        if entry.timestamp < min_timestamp {
            min_timestamp = entry.timestamp;
        }

        if entry.timestamp > max_timestamp {
            max_timestamp = entry.timestamp;
        }

        if entry.lsn < min_lsn {
            min_lsn = entry.lsn;
        }

        if entry.lsn > max_lsn {
            max_lsn = entry.lsn;
        }

        if block_first_key.is_none() {
            block_first_key = Some(entry.key.clone());
        }

        bloom.set(&entry.key);

        block_last_key = entry.key.clone();

        let cell = SSTableCell {
            key_len: entry.key.len() as u32,
            value_len: entry.value.as_ref().map_or(0, |v| v.len()) as u32,
            timestamp: entry.timestamp,
            is_delete: entry.value.is_none(),
            lsn: entry.lsn,
        };

        let mut cell_bytes = encode_to_vec(&cell, config)?;
        cell_bytes.extend_from_slice(&entry.key);
        if let Some(value) = entry.value {
            cell_bytes.extend_from_slice(&value);
        }

        current_block.extend_from_slice(&cell_bytes);

        if current_block.len() >= SST_DATA_BLOCK_MAX_SIZE {
            let block_offset = writer.seek(SeekFrom::Current(0))?;

            let block = SSTableDataBlock {
                data: mem::take(&mut current_block),
            };
            let block_bytes = encode_to_vec(&block, config)?;
            let block_size = block_bytes.len() as u32;

            let mut hasher = Crc32::new();
            hasher.update(&block_bytes);
            let block_checksum = hasher.finalize();

            writer.write_all(&block_size.to_le_bytes())?;
            writer.write_all(&block_bytes)?;
            writer.write_all(&block_checksum.to_le_bytes())?;

            index_entries.push(SSTableIndexEntry {
                separator_key: block_first_key.take().unwrap(),
                handle: BlockHandle {
                    offset: block_offset,
                    size: (SST_DATA_BLOCK_LEN_SIZE
                        + block_size as usize
                        + SST_DATA_BLOCK_CHECKSUM_SIZE) as u64,
                },
            });

            block_first_key = None;
        }
    }

    if !block_last_key.is_empty() {
        max_key = Some(block_last_key.clone());
    }

    if !current_block.is_empty() {
        let block_offset: u64 = writer.seek(SeekFrom::Current(0))?;

        let block = SSTableDataBlock {
            data: mem::take(&mut current_block),
        };
        let block_bytes = encode_to_vec(&block, config)?;
        let block_size = block_bytes.len() as u32;

        let mut hasher = Crc32::new();
        hasher.update(&block_bytes);
        let block_checksum = hasher.finalize();

        writer.write_all(&block_size.to_le_bytes())?;
        writer.write_all(&block_bytes)?;
        writer.write_all(&block_checksum.to_le_bytes())?;

        index_entries.push(SSTableIndexEntry {
            separator_key: block_first_key.take().unwrap(),
            handle: BlockHandle {
                offset: block_offset,
                size: (SST_DATA_BLOCK_LEN_SIZE + block_size as usize + SST_DATA_BLOCK_CHECKSUM_SIZE)
                    as u64,
            },
        });

        let _ = block_first_key;
    }

    // 3. Write bloom filter
    let bloom_offset = writer.seek(SeekFrom::Current(0))?;
    let bloom_data = bloom.as_slice().to_vec();
    let bloom_block = SSTableBloomBlock { data: bloom_data };
    let bloom_bytes = encode_to_vec(&bloom_block, config)?;
    let bloom_size = bloom_bytes.len() as u32;

    let mut hasher = Crc32::new();
    hasher.update(&bloom_bytes);
    let bloom_checksum = hasher.finalize();

    writer.write_all(&bloom_size.to_le_bytes())?;
    writer.write_all(&bloom_bytes)?;
    writer.write_all(&bloom_checksum.to_le_bytes())?;

    // 4. Build and write range deletes block
    let range_deletes_offset = writer.seek(SeekFrom::Current(0))?;
    let mut range_deletes_block = SSTableRangeTombstoneDataBlock { data: Vec::new() };

    while let Some(entry) = range_tombstones_peekable.next() {
        if entry.timestamp < min_timestamp {
            min_timestamp = entry.timestamp;
        }

        if entry.timestamp > max_timestamp {
            max_timestamp = entry.timestamp;
        }

        if entry.lsn < min_lsn {
            min_lsn = entry.lsn;
        }

        if entry.lsn > max_lsn {
            max_lsn = entry.lsn;
        }

        let cell = SSTableRangeTombstoneCell {
            start_key: entry.start,
            end_key: entry.end,
            timestamp: entry.timestamp,
            lsn: entry.lsn,
        };

        range_deletes_block.data.push(cell);
    }

    let range_deletes_bytes = encode_to_vec(&range_deletes_block, config)?;
    let range_deletes_size = range_deletes_bytes.len() as u32;

    let mut hasher = Crc32::new();
    hasher.update(&range_deletes_bytes);
    let range_deletes_checksum = hasher.finalize();

    writer.write_all(&range_deletes_size.to_le_bytes())?;
    writer.write_all(&range_deletes_bytes)?;
    writer.write_all(&range_deletes_checksum.to_le_bytes())?;

    // 5. Build and write properties block
    let properties = SSTablePropertiesBlock {
        creation_timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_nanos() as u64,
        record_count,
        tombstone_count,
        range_tombstones_count: range_tombstones_count as u64,
        min_lsn,
        max_lsn,
        min_timestamp,
        max_timestamp,
        min_key: min_key.unwrap_or_else(Vec::new),
        max_key: max_key.unwrap_or_else(Vec::new),
    };

    let properties_offset = writer.seek(SeekFrom::Current(0))?;
    let properties_bytes = encode_to_vec(&properties, config)?;
    let properties_size = properties_bytes.len() as u32;

    let mut hasher = Crc32::new();
    hasher.update(&properties_bytes);
    let properties_checksum = hasher.finalize();

    writer.write_all(&properties_size.to_le_bytes())?;
    writer.write_all(&properties_bytes)?;
    writer.write_all(&properties_checksum.to_le_bytes())?;

    // 6. Build and write metaindex block
    let metaindex_offset = writer.seek(SeekFrom::Current(0))?;

    let mut meta_entries: Vec<MetaIndexEntry> = Vec::new();

    meta_entries.push(MetaIndexEntry {
        name: "filter.bloom".to_string(),
        handle: BlockHandle {
            offset: bloom_offset,
            size: bloom_bytes.len() as u64,
        },
    });
    meta_entries.push(MetaIndexEntry {
        name: "meta.properties".to_string(),
        handle: BlockHandle {
            offset: properties_offset,
            size: properties_bytes.len() as u64,
        },
    });
    meta_entries.push(MetaIndexEntry {
        name: "meta.range_deletes".to_string(),
        handle: BlockHandle {
            offset: range_deletes_offset,
            size: range_deletes_bytes.len() as u64,
        },
    });

    let metaindex_bytes = encode_to_vec(&meta_entries, config)?;
    let metaindex_size = metaindex_bytes.len() as u32;

    let mut hasher = Crc32::new();
    hasher.update(&metaindex_bytes);
    let metaindex_checksum = hasher.finalize();

    writer.write_all(&metaindex_size.to_le_bytes())?;
    writer.write_all(&metaindex_bytes)?;
    writer.write_all(&metaindex_checksum.to_le_bytes())?;

    // 7. Write index block
    let index_offset = writer.seek(SeekFrom::Current(0))?;

    let index_bytes = encode_to_vec(&index_entries, config)?;
    let index_size = index_bytes.len() as u32;

    let mut hasher = Crc32::new();
    hasher.update(&index_bytes);
    let index_checksum = hasher.finalize();

    writer.write_all(&index_size.to_le_bytes())?;
    writer.write_all(&index_bytes)?;
    writer.write_all(&index_checksum.to_le_bytes())?;

    // 8. Writer footer
    writer.flush()?;
    drop(writer);
    file.sync_all()?;

    let current_pos = file.metadata()?.len();
    let metaindex_size = metaindex_bytes.len() as u64;
    let index_size = index_bytes.len() as u64;
    let footer = SSTableFooter {
        metaindex: BlockHandle {
            offset: metaindex_offset,
            size: metaindex_size,
        },
        index: BlockHandle {
            offset: index_offset,
            size: index_size,
        },
        total_file_size: current_pos + SST_FOOTER_SIZE as u64,
        footer_crc32: 0,
    };

    let mut footer_bytes = encode_to_vec(&footer, config)?;

    let mut hasher = Crc32::new();
    hasher.update(&footer_bytes);
    let footer_crc = hasher.finalize();

    let footer_with_crc = SSTableFooter {
        footer_crc32: footer_crc,
        ..footer
    };

    footer_bytes = encode_to_vec(&footer_with_crc, config)?;

    let mut writer = BufWriter::new(&mut file);
    writer.write_all(&footer_bytes)?;

    writer.flush()?;
    drop(writer);
    file.sync_all()?;

    rename(&tmp_path, &final_path)?;

    Ok(())
}
