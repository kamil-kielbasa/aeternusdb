//! SSTable writer — builds a complete SSTable file from sorted iterators.
//!
//! The [`SstWriter`] struct accepts two sorted streams:
//!
//! - **Point entries** ([`PointEntry`](crate::engine::PointEntry)): key/value pairs or point tombstones.
//! - **Range tombstones** ([`RangeTombstone`](crate::engine::RangeTombstone)):
//!   delete intervals covering key ranges.
//!
//! and writes a fully-structured SSTable containing header, data blocks, bloom
//! filter, range tombstone block, properties block, metaindex block, index
//! block, and footer.
//!
//! # Input Requirements
//!
//! - `point_entries` **must be sorted by key** so that all entries for a given
//!   key are **grouped (adjacent)**. Duplicate keys are allowed — SSTables may
//!   store multiple versions of the same logical key.
//! - `range_tombstones` **must be sorted by start key**. Overlapping range
//!   tombstones are allowed; per-key resolution prefers the tombstone with the
//!   highest LSN (tie-breaker: timestamp).
//!
//! # Output Guarantees
//!
//! - All point entries are grouped into data blocks and written with per-block CRC32.
//! - Bloom filter is built from keys (including point tombstones).
//! - Properties capture min/max keys, LSNs, timestamps and counts.
//! - The final file is written atomically using a `.tmp` → final rename.
//!
//! # Atomicity
//!
//! 1. Write everything to `path.tmp`.
//! 2. Flush and sync the file.
//! 3. Rename `path.tmp` → `path` atomically.
//!
//! A crash cannot produce a partially-written SSTable.

use std::{
    fs::{File, OpenOptions, rename},
    io::{BufWriter, Seek, Write},
    mem,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use bincode::{config::standard, encode_to_vec};
use bloomfilter::Bloom;
use crc32fast::Hasher as Crc32;

use crate::engine::{PointEntry, RangeTombstone};

use super::{
    BlockHandle, MetaIndexEntry, SST_BLOOM_FILTER_FALSE_POSITIVE_RATE,
    SST_DATA_BLOCK_CHECKSUM_SIZE, SST_DATA_BLOCK_LEN_SIZE, SST_DATA_BLOCK_MAX_SIZE,
    SST_FOOTER_SIZE, SST_HDR_MAGIC, SST_HDR_VERSION, SSTableBloomBlock, SSTableCell,
    SSTableDataBlock, SSTableError, SSTableFooter, SSTableHeader, SSTableIndexEntry,
    SSTablePropertiesBlock, SSTableRangeTombstoneCell, SSTableRangeTombstoneDataBlock,
};

// ------------------------------------------------------------------------------------------------
// BuildStats — accumulates metadata during SSTable construction
// ------------------------------------------------------------------------------------------------

/// Statistics gathered while iterating point entries and range tombstones.
///
/// Fed into [`SSTablePropertiesBlock`] at the end of construction.
struct BuildStats {
    record_count: u64,
    tombstone_count: u64,
    min_lsn: u64,
    max_lsn: u64,
    min_timestamp: u64,
    max_timestamp: u64,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
}

impl BuildStats {
    fn new() -> Self {
        Self {
            record_count: 0,
            tombstone_count: 0,
            min_lsn: u64::MAX,
            max_lsn: 0,
            min_timestamp: u64::MAX,
            max_timestamp: 0,
            min_key: None,
            max_key: None,
        }
    }

    /// Update min/max LSN and timestamp bounds.
    fn track(&mut self, lsn: u64, timestamp: u64) {
        self.min_lsn = self.min_lsn.min(lsn);
        self.max_lsn = self.max_lsn.max(lsn);
        self.min_timestamp = self.min_timestamp.min(timestamp);
        self.max_timestamp = self.max_timestamp.max(timestamp);
    }

    /// Convert collected statistics into an [`SSTablePropertiesBlock`].
    fn into_properties(self, range_count: usize) -> SSTablePropertiesBlock {
        SSTablePropertiesBlock {
            creation_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before UNIX epoch")
                .as_nanos() as u64,
            record_count: self.record_count,
            tombstone_count: self.tombstone_count,
            range_tombstones_count: range_count as u64,
            min_lsn: self.min_lsn,
            max_lsn: self.max_lsn,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            min_key: self.min_key.unwrap_or_default(),
            max_key: self.max_key.unwrap_or_default(),
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Block I/O helpers
// ------------------------------------------------------------------------------------------------

/// Writes a checksummed block: `[len_le (4 B)][data][crc32_le (4 B)]`.
///
/// Returns `(block_offset, data_byte_len)` — the offset where the block
/// starts in the file, and the length of the encoded `data` slice.
fn write_checksummed_block(
    writer: &mut (impl Write + Seek),
    data: &[u8],
) -> Result<(u64, usize), SSTableError> {
    let offset = writer.stream_position()?;
    let len = data.len() as u32;

    let mut hasher = Crc32::new();
    hasher.update(data);
    let checksum = hasher.finalize();

    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(data)?;
    writer.write_all(&checksum.to_le_bytes())?;

    Ok((offset, data.len()))
}

/// Writes the SSTable header with embedded and trailing CRC32.
///
/// On-disk layout: `[SSTableHeader (12 B)][outer_crc32 (4 B)]` = 16 bytes.
fn write_header(writer: &mut impl Write) -> Result<(), SSTableError> {
    let config = standard().with_fixed_int_encoding();

    // Step 1: encode with crc = 0, compute inner CRC.
    let header = SSTableHeader {
        magic: SST_HDR_MAGIC,
        version: SST_HDR_VERSION,
        header_crc: 0,
    };
    let zeroed_bytes = encode_to_vec(&header, config)?;
    let mut hasher = Crc32::new();
    hasher.update(&zeroed_bytes);
    let inner_crc = hasher.finalize();

    // Step 2: re-encode with inner CRC embedded, compute outer CRC.
    let header = SSTableHeader {
        header_crc: inner_crc,
        ..header
    };
    let header_bytes = encode_to_vec(&header, config)?;
    let mut hasher = Crc32::new();
    hasher.update(&header_bytes);
    let outer_crc = hasher.finalize();

    writer.write_all(&header_bytes)?;
    writer.write_all(&outer_crc.to_le_bytes())?;

    Ok(())
}

/// Encodes and flushes the current data-block buffer to disk, pushing a
/// new index entry.
fn flush_data_block(
    writer: &mut (impl Write + Seek),
    current_block: &mut Vec<u8>,
    block_first_key: &mut Option<Vec<u8>>,
    index_entries: &mut Vec<SSTableIndexEntry>,
) -> Result<(), SSTableError> {
    let config = standard().with_fixed_int_encoding();

    let block = SSTableDataBlock {
        data: mem::take(current_block),
    };
    let block_bytes = encode_to_vec(&block, config)?;
    let (offset, data_len) = write_checksummed_block(writer, &block_bytes)?;

    index_entries.push(SSTableIndexEntry {
        separator_key: block_first_key.take().unwrap(),
        handle: BlockHandle {
            offset,
            size: (SST_DATA_BLOCK_LEN_SIZE + data_len + SST_DATA_BLOCK_CHECKSUM_SIZE) as u64,
        },
    });

    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Phase helpers — one per logical section of the SSTable
// ------------------------------------------------------------------------------------------------

/// Iterates point entries, encodes them into data blocks, populates the
/// bloom filter, and tracks statistics.
///
/// Returns the accumulated stats and the block-index entries.
fn write_data_blocks(
    writer: &mut (impl Write + Seek),
    entries: impl Iterator<Item = PointEntry>,
    bloom: &mut Bloom<Vec<u8>>,
) -> Result<(BuildStats, Vec<SSTableIndexEntry>), SSTableError> {
    let config = standard().with_fixed_int_encoding();
    let mut stats = BuildStats::new();
    let mut index_entries = Vec::new();
    let mut current_block = Vec::<u8>::new();
    let mut block_first_key: Option<Vec<u8>> = None;

    for entry in entries {
        stats.record_count += 1;
        if entry.value.is_none() {
            stats.tombstone_count += 1;
        }
        stats.track(entry.lsn, entry.timestamp);

        // Track min/max key (entries are sorted, so first = min, last = max).
        if stats.min_key.is_none() {
            stats.min_key = Some(entry.key.clone());
        }
        stats.max_key = Some(entry.key.clone());

        if block_first_key.is_none() {
            block_first_key = Some(entry.key.clone());
        }
        bloom.set(&entry.key);

        // Encode point cell.
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

        // Flush block when it reaches target size.
        if current_block.len() >= SST_DATA_BLOCK_MAX_SIZE {
            flush_data_block(
                writer,
                &mut current_block,
                &mut block_first_key,
                &mut index_entries,
            )?;
        }
    }

    // Flush remaining partial block.
    if !current_block.is_empty() {
        flush_data_block(
            writer,
            &mut current_block,
            &mut block_first_key,
            &mut index_entries,
        )?;
    }

    Ok((stats, index_entries))
}

/// Iterates range tombstones, updates stats, and writes the range-delete
/// block to disk.
///
/// Returns `(block_offset, data_byte_len)`.
fn write_range_tombstones(
    writer: &mut (impl Write + Seek),
    entries: impl Iterator<Item = RangeTombstone>,
    stats: &mut BuildStats,
) -> Result<(u64, usize), SSTableError> {
    let config = standard().with_fixed_int_encoding();
    let mut block = SSTableRangeTombstoneDataBlock { data: Vec::new() };

    for entry in entries {
        stats.track(entry.lsn, entry.timestamp);
        block.data.push(SSTableRangeTombstoneCell {
            start_key: entry.start,
            end_key: entry.end,
            timestamp: entry.timestamp,
            lsn: entry.lsn,
        });
    }

    let bytes = encode_to_vec(&block, config)?;
    write_checksummed_block(writer, &bytes)
}

/// Builds and writes the metaindex block pointing to bloom, properties,
/// and range-delete blocks.
///
/// Returns `(block_offset, data_byte_len)`.
fn write_metaindex(
    writer: &mut (impl Write + Seek),
    bloom: BlockHandle,
    properties: BlockHandle,
    range_deletes: BlockHandle,
) -> Result<(u64, usize), SSTableError> {
    let config = standard().with_fixed_int_encoding();

    let meta_entries = vec![
        MetaIndexEntry {
            name: "filter.bloom".to_string(),
            handle: bloom,
        },
        MetaIndexEntry {
            name: "meta.properties".to_string(),
            handle: properties,
        },
        MetaIndexEntry {
            name: "meta.range_deletes".to_string(),
            handle: range_deletes,
        },
    ];

    let bytes = encode_to_vec(&meta_entries, config)?;
    write_checksummed_block(writer, &bytes)
}

/// Writes the SSTable footer (with CRC) and syncs the file.
fn write_footer(
    file: &mut File,
    metaindex: BlockHandle,
    index: BlockHandle,
) -> Result<(), SSTableError> {
    let config = standard().with_fixed_int_encoding();
    let current_pos = file.metadata()?.len();

    let footer = SSTableFooter {
        metaindex,
        index,
        total_file_size: current_pos + SST_FOOTER_SIZE as u64,
        footer_crc32: 0,
    };

    let footer_bytes = encode_to_vec(&footer, config)?;
    let mut hasher = Crc32::new();
    hasher.update(&footer_bytes);
    let footer_crc = hasher.finalize();

    let footer_with_crc = SSTableFooter {
        footer_crc32: footer_crc,
        ..footer
    };
    let footer_bytes = encode_to_vec(&footer_with_crc, config)?;

    let mut writer = BufWriter::new(&mut *file);
    writer.write_all(&footer_bytes)?;
    writer.flush()?;
    drop(writer);
    file.sync_all()?;

    Ok(())
}

// ------------------------------------------------------------------------------------------------
// SstWriter — public entry point
// ------------------------------------------------------------------------------------------------

/// Builds a complete SSTable file on disk.
///
/// # Example
///
/// ```rust,ignore
/// SstWriter::new(&path).build(points, point_count, ranges, range_count)?;
/// ```
pub struct SstWriter<P: AsRef<Path>> {
    path: P,
}

impl<P: AsRef<Path>> SstWriter<P> {
    /// Create a writer targeting the given output path.
    pub fn new(path: P) -> Self {
        Self { path }
    }

    /// Consume sorted iterators and write a complete SSTable.
    ///
    /// # Parameters
    ///
    /// - `point_entries` — sorted iterator of [`PointEntry`] values.
    /// - `point_count` — expected number of point entries (sizes bloom filter).
    /// - `range_tombstones` — sorted iterator of [`RangeTombstone`] values.
    /// - `range_count` — expected number of range tombstones.
    ///
    /// # Errors
    ///
    /// - [`SSTableError::Internal`] if both iterators are empty.
    /// - I/O errors from writing or seeking.
    /// - `bincode` encode errors.
    pub fn build(
        self,
        point_entries: impl Iterator<Item = PointEntry>,
        point_count: usize,
        range_tombstones: impl Iterator<Item = RangeTombstone>,
        range_count: usize,
    ) -> Result<(), SSTableError> {
        let config = standard().with_fixed_int_encoding();
        let mut point_entries = point_entries.peekable();
        let mut range_tombstones = range_tombstones.peekable();

        // Reject when both streams are empty.
        if point_count == 0
            && point_entries.peek().is_none()
            && range_count == 0
            && range_tombstones.peek().is_none()
        {
            return Err(SSTableError::Internal(
                "Empty iterators cannot build SSTable".into(),
            ));
        }

        // Open temp file for atomic write.
        let final_path = self.path.as_ref();
        let tmp_path = final_path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut writer = BufWriter::new(&mut file);

        // 1. Header
        write_header(&mut writer)?;

        // 2. Data blocks (point entries → blocks + bloom filter + stats)
        let mut bloom = Bloom::new_for_fp_rate(
            point_count + range_count,
            SST_BLOOM_FILTER_FALSE_POSITIVE_RATE,
        )
        .map_err(|e| SSTableError::Internal(e.to_string()))?;

        let (mut stats, index_entries) = write_data_blocks(&mut writer, point_entries, &mut bloom)?;

        // 3. Bloom filter block
        let bloom_block = SSTableBloomBlock {
            data: bloom.as_slice().to_vec(),
        };
        let bloom_bytes = encode_to_vec(&bloom_block, config)?;
        let (bloom_off, bloom_len) = write_checksummed_block(&mut writer, &bloom_bytes)?;

        // 4. Range tombstones block
        let (rt_off, rt_len) = write_range_tombstones(&mut writer, range_tombstones, &mut stats)?;

        // 5. Properties block
        let properties = stats.into_properties(range_count);
        let props_bytes = encode_to_vec(&properties, config)?;
        let (props_off, props_len) = write_checksummed_block(&mut writer, &props_bytes)?;

        // 6. Metaindex block
        let (meta_off, meta_len) = write_metaindex(
            &mut writer,
            BlockHandle {
                offset: bloom_off,
                size: bloom_len as u64,
            },
            BlockHandle {
                offset: props_off,
                size: props_len as u64,
            },
            BlockHandle {
                offset: rt_off,
                size: rt_len as u64,
            },
        )?;

        // 7. Index block
        let index_bytes = encode_to_vec(&index_entries, config)?;
        let (idx_off, idx_len) = write_checksummed_block(&mut writer, &index_bytes)?;

        // 8. Flush buffered data before footer (footer reads file length).
        writer.flush()?;
        drop(writer);
        file.sync_all()?;

        // 9. Footer + final sync
        write_footer(
            &mut file,
            BlockHandle {
                offset: meta_off,
                size: meta_len as u64,
            },
            BlockHandle {
                offset: idx_off,
                size: idx_len as u64,
            },
        )?;

        rename(&tmp_path, final_path)?;
        Ok(())
    }
}
