//! Sorted String Table (SSTable) Module
//!
//! This module implements an **immutable**, **disk-backed**, and **versioned** sorted string table
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
//! Data is serialized using a custom [`encoding`] module with **fixed integer encoding**, and block-level CRC32
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
//! - **Header** — `SSTableHeader` structure with CRC32 checksum.
//! - **Data blocks** — store serialized `SSTableCell` entries (key-value or tombstone).
//! - **Bloom filter block** — fast existence checks for point keys.
//! - **Range deletes block** — serialized `SSTableRangeTombstoneCell` entries.
//! - **Properties block** — table metadata such as min/max key, LSNs, timestamps, record counts.
//! - **Metaindex block** — directory of blocks (bloom, properties, range deletes) for easy lookup.
//! - **Index block** — directory of data blocks, allowing binary search for keys.
//! - **Footer** — `SSTableFooter` structure containing offsets, sizes, and CRC32 checksum.
//!
//! # Sub-modules
//!
//! - [`builder`] — [`SstWriter`] for building SSTables from sorted streams.
//! - [`iterator`] — [`BlockIterator`], [`BlockEntry`], and [`ScanIterator`] for reading.
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

// ------------------------------------------------------------------------------------------------
// Sub-modules
// ------------------------------------------------------------------------------------------------

pub mod builder;
pub mod iterator;

#[cfg(test)]
mod tests;

// ------------------------------------------------------------------------------------------------
// Re-exports — public API surface
// ------------------------------------------------------------------------------------------------

#[allow(unused_imports)] // public API surface for downstream consumers
pub use crate::engine::{PointEntry, RangeTombstone, Record};
pub use builder::SstWriter;
#[allow(unused_imports)] // public API surface for downstream consumers
pub use iterator::{BlockEntry, BlockIterator, ScanIterator};

// ------------------------------------------------------------------------------------------------
// Includes
// ------------------------------------------------------------------------------------------------

use std::{fs::File, io, path::Path};

use crate::encoding::{self, EncodingError};
use bloomfilter::Bloom;
use crc32fast::Hasher as Crc32;
use memmap2::Mmap;
use thiserror::Error;

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

/// Errors returned by SSTable operations (read, write, build).
#[derive(Debug, Error)]
pub enum SSTableError {
    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Encoding / decoding error.
    #[error("Encoding error: {0}")]
    Encoding(#[from] EncodingError),

    /// Internal invariant violation or poisoned lock.
    #[error("Internal error: {0}")]
    Internal(String),

    /// Checksum mistmatch.
    #[error("Checksum mismatch")]
    ChecksumMismatch,
}

// ------------------------------------------------------------------------------------------------
// On-disk format structures
// ------------------------------------------------------------------------------------------------

/// SSTable file header, written at the beginning of the SSTable.
/// Contains a magic number, version, and CRC32 checksum for integrity.
#[derive(Default)]
pub(crate) struct SSTableHeader {
    /// Magic bytes to identify SSTable format (`b"SST0"`).
    magic: [u8; 4],

    /// SSTable format version.
    version: u32,

    /// CRC32 checksum of the header (excluding this field).
    header_crc: u32,
}

/// Represents a data block in the SSTable, which contains serialized key-value entries.
pub(crate) struct SSTableDataBlock {
    /// Raw serialized block data.
    pub(crate) data: Vec<u8>,
}

/// Represents a Bloom filter block used to quickly check the presence of point keys.
pub(crate) struct SSTableBloomBlock {
    /// Serialized bloom filter bytes.
    pub(crate) data: Vec<u8>,
}

/// Represents a block containing range tombstones.
pub(crate) struct SSTableRangeTombstoneDataBlock {
    /// List of serialized range tombstone cells.
    pub(crate) data: Vec<SSTableRangeTombstoneCell>,
}

/// Metadata block containing SSTable-level properties and statistics.
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
pub(crate) struct SSTableIndexEntry {
    /// Key that separates this block from the next in sorted order.
    pub(crate) separator_key: Vec<u8>,

    /// Block handle containing offset and size of the data block.
    pub(crate) handle: BlockHandle,
}

/// SSTable footer, stored at the very end of the file.
pub(crate) struct SSTableFooter {
    /// Handle of the metaindex block, containing references to:
    /// - bloom filter block
    /// - properties block
    /// - range tombstone blocks
    pub(crate) metaindex: BlockHandle,

    /// Handle of the main index block, mapping separator keys to data blocks.
    pub(crate) index: BlockHandle,

    /// Total size of the SSTable file, including this footer.
    pub(crate) total_file_size: u64,

    /// CRC32 checksum computed over the footer fields except this one.
    pub(crate) footer_crc32: u32,
}

/// Represents a single key-value entry (or tombstone) in a data block.
pub(crate) struct SSTableCell {
    /// Length of the key in bytes.
    pub(crate) key_len: u32,

    /// Length of the value in bytes (0 if deleted).
    pub(crate) value_len: u32,

    /// Timestamp of the operation.
    pub(crate) timestamp: u64,

    /// Whether this entry represents a deletion.
    pub(crate) is_delete: bool,

    /// Log Sequence Number for versioning.
    pub(crate) lsn: u64,
}

/// Represents a range tombstone marking deletion of keys in `[start_key, end_key)`.
pub(crate) struct SSTableRangeTombstoneCell {
    /// Start key of the deleted range (inclusive).
    pub(crate) start_key: Vec<u8>,

    /// End key of the deleted range (exclusive).
    pub(crate) end_key: Vec<u8>,

    /// Timestamp of the deletion.
    pub(crate) timestamp: u64,

    /// LSN of the deletion.
    pub(crate) lsn: u64,
}

/// Handle to a block in the SSTable file, specifying its offset and size.
#[derive(Debug)]
pub(crate) struct BlockHandle {
    /// Offset of the block in the SSTable file.
    pub(crate) offset: u64,

    /// Size of the block in bytes, including length prefix and checksum.
    pub(crate) size: u64,
}

/// Represents a single entry in the metaindex block.
#[derive(Debug)]
pub(crate) struct MetaIndexEntry {
    /// Name of the block (e.g., "filter.bloom", "meta.properties").
    pub(crate) name: String,

    /// Handle pointing to the block location.
    pub(crate) handle: BlockHandle,
}

// ------------------------------------------------------------------------------------------------
// Encoding implementations
// ------------------------------------------------------------------------------------------------

impl encoding::Encode for BlockHandle {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.offset, buf)?;
        encoding::Encode::encode_to(&self.size, buf)?;
        Ok(())
    }
}

impl encoding::Decode for BlockHandle {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut off = 0;
        let (offset, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (size, n) = u64::decode_from(&buf[off..])?;
        off += n;
        Ok((Self { offset, size }, off))
    }
}

impl encoding::Encode for SSTableHeader {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.magic, buf)?;
        encoding::Encode::encode_to(&self.version, buf)?;
        encoding::Encode::encode_to(&self.header_crc, buf)?;
        Ok(())
    }
}

impl encoding::Decode for SSTableHeader {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut off = 0;
        let (magic, n) = <[u8; 4]>::decode_from(&buf[off..])?;
        off += n;
        let (version, n) = u32::decode_from(&buf[off..])?;
        off += n;
        let (header_crc, n) = u32::decode_from(&buf[off..])?;
        off += n;
        Ok((
            Self {
                magic,
                version,
                header_crc,
            },
            off,
        ))
    }
}

impl encoding::Encode for SSTableDataBlock {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.data, buf)?;
        Ok(())
    }
}

impl encoding::Decode for SSTableDataBlock {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (data, n) = <Vec<u8>>::decode_from(buf)?;
        Ok((Self { data }, n))
    }
}

impl encoding::Encode for SSTableBloomBlock {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.data, buf)?;
        Ok(())
    }
}

impl encoding::Decode for SSTableBloomBlock {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (data, n) = <Vec<u8>>::decode_from(buf)?;
        Ok((Self { data }, n))
    }
}

impl encoding::Encode for SSTableCell {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.key_len, buf)?;
        encoding::Encode::encode_to(&self.value_len, buf)?;
        encoding::Encode::encode_to(&self.timestamp, buf)?;
        encoding::Encode::encode_to(&self.is_delete, buf)?;
        encoding::Encode::encode_to(&self.lsn, buf)?;
        Ok(())
    }
}

impl encoding::Decode for SSTableCell {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut off = 0;
        let (key_len, n) = u32::decode_from(&buf[off..])?;
        off += n;
        let (value_len, n) = u32::decode_from(&buf[off..])?;
        off += n;
        let (timestamp, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (is_delete, n) = bool::decode_from(&buf[off..])?;
        off += n;
        let (lsn, n) = u64::decode_from(&buf[off..])?;
        off += n;
        Ok((
            Self {
                key_len,
                value_len,
                timestamp,
                is_delete,
                lsn,
            },
            off,
        ))
    }
}

impl encoding::Encode for SSTableRangeTombstoneCell {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.start_key, buf)?;
        encoding::Encode::encode_to(&self.end_key, buf)?;
        encoding::Encode::encode_to(&self.timestamp, buf)?;
        encoding::Encode::encode_to(&self.lsn, buf)?;
        Ok(())
    }
}

impl encoding::Decode for SSTableRangeTombstoneCell {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut off = 0;
        let (start_key, n) = <Vec<u8>>::decode_from(&buf[off..])?;
        off += n;
        let (end_key, n) = <Vec<u8>>::decode_from(&buf[off..])?;
        off += n;
        let (timestamp, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (lsn, n) = u64::decode_from(&buf[off..])?;
        off += n;
        Ok((
            Self {
                start_key,
                end_key,
                timestamp,
                lsn,
            },
            off,
        ))
    }
}

impl encoding::Encode for SSTableRangeTombstoneDataBlock {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::encode_vec(&self.data, buf)?;
        Ok(())
    }
}

impl encoding::Decode for SSTableRangeTombstoneDataBlock {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (data, n) = encoding::decode_vec::<SSTableRangeTombstoneCell>(buf)?;
        Ok((Self { data }, n))
    }
}

impl encoding::Encode for SSTablePropertiesBlock {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.creation_timestamp, buf)?;
        encoding::Encode::encode_to(&self.record_count, buf)?;
        encoding::Encode::encode_to(&self.tombstone_count, buf)?;
        encoding::Encode::encode_to(&self.range_tombstones_count, buf)?;
        encoding::Encode::encode_to(&self.min_lsn, buf)?;
        encoding::Encode::encode_to(&self.max_lsn, buf)?;
        encoding::Encode::encode_to(&self.min_timestamp, buf)?;
        encoding::Encode::encode_to(&self.max_timestamp, buf)?;
        encoding::Encode::encode_to(&self.min_key, buf)?;
        encoding::Encode::encode_to(&self.max_key, buf)?;
        Ok(())
    }
}

impl encoding::Decode for SSTablePropertiesBlock {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut off = 0;
        let (creation_timestamp, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (record_count, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (tombstone_count, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (range_tombstones_count, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (min_lsn, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (max_lsn, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (min_timestamp, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (max_timestamp, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (min_key, n) = <Vec<u8>>::decode_from(&buf[off..])?;
        off += n;
        let (max_key, n) = <Vec<u8>>::decode_from(&buf[off..])?;
        off += n;
        Ok((
            Self {
                creation_timestamp,
                record_count,
                tombstone_count,
                range_tombstones_count,
                min_lsn,
                max_lsn,
                min_timestamp,
                max_timestamp,
                min_key,
                max_key,
            },
            off,
        ))
    }
}

impl encoding::Encode for SSTableIndexEntry {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.separator_key, buf)?;
        encoding::Encode::encode_to(&self.handle, buf)?;
        Ok(())
    }
}

impl encoding::Decode for SSTableIndexEntry {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut off = 0;
        let (separator_key, n) = <Vec<u8>>::decode_from(&buf[off..])?;
        off += n;
        let (handle, n) = BlockHandle::decode_from(&buf[off..])?;
        off += n;
        Ok((
            Self {
                separator_key,
                handle,
            },
            off,
        ))
    }
}

impl encoding::Encode for MetaIndexEntry {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.name, buf)?;
        encoding::Encode::encode_to(&self.handle, buf)?;
        Ok(())
    }
}

impl encoding::Decode for MetaIndexEntry {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut off = 0;
        let (name, n) = String::decode_from(&buf[off..])?;
        off += n;
        let (handle, n) = BlockHandle::decode_from(&buf[off..])?;
        off += n;
        Ok((Self { name, handle }, off))
    }
}

impl encoding::Encode for SSTableFooter {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.metaindex, buf)?;
        encoding::Encode::encode_to(&self.index, buf)?;
        encoding::Encode::encode_to(&self.total_file_size, buf)?;
        encoding::Encode::encode_to(&self.footer_crc32, buf)?;
        Ok(())
    }
}

impl encoding::Decode for SSTableFooter {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut off = 0;
        let (metaindex, n) = BlockHandle::decode_from(&buf[off..])?;
        off += n;
        let (index, n) = BlockHandle::decode_from(&buf[off..])?;
        off += n;
        let (total_file_size, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (footer_crc32, n) = u32::decode_from(&buf[off..])?;
        off += n;
        Ok((
            Self {
                metaindex,
                index,
                total_file_size,
                footer_crc32,
            },
            off,
        ))
    }
}

// ------------------------------------------------------------------------------------------------
// GetResult
// ------------------------------------------------------------------------------------------------

/// Result of a single key lookup in an SSTable.
#[derive(Debug, PartialEq, Clone)]
pub enum GetResult {
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

impl GetResult {
    /// Returns the **LSN** (logical sequence number) associated with this get result.
    pub fn lsn(&self) -> u64 {
        match self {
            Self::Put { lsn, .. } => *lsn,
            Self::Delete { lsn, .. } => *lsn,
            Self::RangeDelete { lsn, .. } => *lsn,
            Self::NotFound => 0,
        }
    }

    /// Returns the **timestamp** associated with this get result.
    pub fn timestamp(&self) -> u64 {
        match self {
            Self::Put { timestamp, .. } => *timestamp,
            Self::Delete { timestamp, .. } => *timestamp,
            Self::RangeDelete { timestamp, .. } => *timestamp,
            Self::NotFound => 0,
        }
    }
}

// ------------------------------------------------------------------------------------------------
// SSTable — immutable reader
// ------------------------------------------------------------------------------------------------

/// A fully memory-mapped, immutable **Sorted String Table (SSTable)**.
pub struct SSTable {
    /// Unique identifier assigned by the engine (from the manifest).
    /// Set to 0 by `SSTable::open()` — the engine sets the correct value after loading.
    pub id: u64,

    /// Memory-mapped file containing the full SSTable bytes.
    pub mmap: Mmap,

    /// Parsed header block containing magic/version information.
    pub(crate) header: SSTableHeader,

    /// Bloom filter block for fast membership tests.
    pub(crate) bloom: SSTableBloomBlock,

    /// Properties block with statistics and metadata.
    pub properties: SSTablePropertiesBlock,

    /// Range delete tombstone block.
    pub(crate) range_deletes: SSTableRangeTombstoneDataBlock,

    /// Index entries mapping key ranges to data blocks.
    pub(crate) index: Vec<SSTableIndexEntry>,

    /// Footer containing block handles and file integrity data.
    pub(crate) footer: SSTableFooter,
}

impl SSTable {
    /// Returns the on-disk file size of this SSTable in bytes.
    pub fn file_size(&self) -> u64 {
        self.footer.total_file_size
    }

    /// Checks whether `key` *might* exist in this SSTable according to the
    /// bloom filter.
    ///
    /// Returns `true` if the bloom says "maybe present" or no bloom exists.
    /// Returns `false` only when the bloom definitively says "not present".
    pub fn bloom_may_contain(&self, key: &[u8]) -> bool {
        if self.bloom.data.is_empty() {
            return true; // no bloom → cannot exclude
        }
        match Bloom::from_slice(&self.bloom.data) {
            Ok(bloom) => bloom.check(key),
            Err(_) => true, // corrupted bloom → assume present
        }
    }

    /// Returns an iterator over the range tombstones stored in this SSTable.
    pub fn range_tombstone_iter(&self) -> impl Iterator<Item = crate::engine::RangeTombstone> + '_ {
        self.range_deletes
            .data
            .iter()
            .map(|rd| crate::engine::RangeTombstone {
                start: rd.start_key.clone(),
                end: rd.end_key.clone(),
                lsn: rd.lsn,
                timestamp: rd.timestamp,
            })
    }

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
    ///    - Deserialized using custom encoding
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

        let file_len = mmap.len();
        if file_len < SST_FOOTER_SIZE {
            return Err(SSTableError::Internal("File too small".into()));
        }

        let (mut header, _) = encoding::decode_from_slice::<SSTableHeader>(&mmap[..SST_HDR_SIZE])?;
        let header_checksum = header.header_crc;

        header.header_crc = 0;

        let header_bytes = encoding::encode_to_vec(&header)?;

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
        let (mut footer, _) = encoding::decode_from_slice::<SSTableFooter>(&mmap[footer_start..])?;

        let footer_checksum = footer.footer_crc32;
        footer.footer_crc32 = 0;

        let footer_bytes = encoding::encode_to_vec(&footer)?;

        let mut hasher = Crc32::new();
        hasher.update(&footer_bytes);
        let footer_comp_checksum = hasher.finalize();

        if footer_checksum != footer_comp_checksum {
            return Err(SSTableError::ChecksumMismatch);
        }

        let metaindex_data = Self::read_block_bytes(&mmap, &footer.metaindex)?;
        let (meta_entries, _) = encoding::decode_vec::<MetaIndexEntry>(&metaindex_data)?;

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
            let (bloom, _) = encoding::decode_from_slice::<SSTableBloomBlock>(&bloom_bytes)
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
            let (properties, _) = encoding::decode_from_slice::<SSTablePropertiesBlock>(&pbytes)?;
            properties
        } else {
            return Err(SSTableError::Internal("SSTable missing properties".into()));
        };

        let range_deletes = if let Some(rh) = range_deletes_block {
            let rbytes = Self::read_block_bytes(&mmap, &rh)?;
            let (ranges, _) = encoding::decode_vec::<SSTableRangeTombstoneCell>(&rbytes)?;
            SSTableRangeTombstoneDataBlock { data: ranges }
        } else {
            SSTableRangeTombstoneDataBlock { data: Vec::new() }
        };

        let index_bytes = Self::read_block_bytes(&mmap, &footer.index)?;
        let (index_entries, _) = encoding::decode_vec::<SSTableIndexEntry>(&index_bytes)?;

        Ok(Self {
            id: 0,
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
    /// Returns the "raw MVCC" result from this SSTable alone. Higher-level LSM
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
    /// A [`GetResult`] variant:
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
    pub fn get(&self, key: &[u8]) -> Result<GetResult, SSTableError> {
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
                Some((lsn, timestamp)) => GetResult::RangeDelete { lsn, timestamp },
                None => GetResult::NotFound,
            });
        }

        // 3) Find the block (if any)
        if self.index.is_empty() {
            return Ok(match range_info {
                Some((lsn, timestamp)) => GetResult::RangeDelete { lsn, timestamp },
                None => GetResult::NotFound,
            });
        }

        let block_idx = self.find_block_for_key(key);
        let entry = &self.index[block_idx];

        let raw = Self::read_block_bytes(&self.mmap, &entry.handle)?;
        let (block, _) = encoding::decode_from_slice::<SSTableDataBlock>(&raw)?;

        // 4) Scan block using BlockIterator (point keys)
        let mut iter = BlockIterator::new(block.data);
        iter.seek_to(key);
        let mut latest: Option<GetResult> = None;

        for item in iter {
            if item.key != key {
                break;
            }

            let candidate = if item.is_delete {
                GetResult::Delete {
                    lsn: item.lsn,
                    timestamp: item.timestamp,
                }
            } else {
                GetResult::Put {
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
            (None, None) => Ok(GetResult::NotFound),

            // Point exists, no range delete → point result wins
            (Some(r), None) => Ok(r),

            // No point entry, but we have a range delete
            (None, Some((lsn, timestamp))) => Ok(GetResult::RangeDelete { lsn, timestamp }),

            // Everything else: point_result = Some(_), range_lsn = Some(_)
            (Some(point), Some((r_lsn, r_ts))) => {
                let result = match point {
                    GetResult::Put {
                        value,
                        lsn: p_lsn,
                        timestamp: p_ts,
                    } => {
                        if r_lsn > p_lsn || (r_lsn == p_lsn && r_ts > p_ts) {
                            GetResult::RangeDelete {
                                lsn: r_lsn,
                                timestamp: r_ts,
                            }
                        } else {
                            GetResult::Put {
                                value,
                                lsn: p_lsn,
                                timestamp: p_ts,
                            }
                        }
                    }
                    GetResult::Delete {
                        lsn: d_lsn,
                        timestamp: d_ts,
                    } => {
                        if r_lsn > d_lsn || (r_lsn == d_lsn && r_ts > d_ts) {
                            GetResult::RangeDelete {
                                lsn: r_lsn,
                                timestamp: r_ts,
                            }
                        } else {
                            GetResult::Delete {
                                lsn: d_lsn,
                                timestamp: d_ts,
                            }
                        }
                    }
                    GetResult::RangeDelete {
                        lsn: rd_lsn,
                        timestamp: rd_ts,
                    } => {
                        let (lsn, ts) = if r_lsn > rd_lsn || (r_lsn == rd_lsn && r_ts > rd_ts) {
                            (r_lsn, r_ts)
                        } else {
                            (rd_lsn, rd_ts)
                        };
                        GetResult::RangeDelete { lsn, timestamp: ts }
                    }
                    GetResult::NotFound => GetResult::RangeDelete {
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
    /// [`ScanIterator`] which merges:
    ///
    /// - data blocks covering the range
    /// - range tombstone iterator
    ///
    /// to produce sorted MVCC entries.
    pub fn scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<impl Iterator<Item = Record> + use<'_>, SSTableError> {
        ScanIterator::new(self, start_key.to_vec(), end_key.to_vec())
    }

    /// Reads a block referenced by a [`BlockHandle`] from the mmap and verifies
    /// its checksum.
    pub(crate) fn read_block_bytes(
        mmap: &Mmap,
        handle: &BlockHandle,
    ) -> Result<Vec<u8>, SSTableError> {
        let start = usize::try_from(handle.offset)
            .map_err(|_| SSTableError::Internal("block offset exceeds addressable range".into()))?;
        let size = usize::try_from(handle.size)
            .map_err(|_| SSTableError::Internal("block size exceeds addressable range".into()))?;

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
    pub(crate) fn find_block_for_key(&self, key: &[u8]) -> usize {
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
