//! Encode / Decode implementations for SSTable on-disk structures.
//!
//! These are split into a separate file for readability — the types
//! themselves live in `super` (i.e., `src/sstable/mod.rs`).

use crate::encoding::{self, EncodingError};

use super::{
    BlockHandle, MetaIndexEntry, SSTableBloomBlock, SSTableCell, SSTableDataBlock, SSTableFooter,
    SSTableHeader, SSTableIndexEntry, SSTablePropertiesBlock, SSTableRangeTombstoneCell,
    SSTableRangeTombstoneDataBlock,
};

// ------------------------------------------------------------------------------------------------
// BlockHandle
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

// ------------------------------------------------------------------------------------------------
// SSTableHeader
// ------------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------------
// SSTableDataBlock
// ------------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------------
// SSTableBloomBlock
// ------------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------------
// SSTableCell
// ------------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------------
// SSTableRangeTombstoneCell
// ------------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------------
// SSTableRangeTombstoneDataBlock
// ------------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------------
// SSTablePropertiesBlock
// ------------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------------
// SSTableIndexEntry
// ------------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------------
// MetaIndexEntry
// ------------------------------------------------------------------------------------------------

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

// ------------------------------------------------------------------------------------------------
// SSTableFooter
// ------------------------------------------------------------------------------------------------

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
