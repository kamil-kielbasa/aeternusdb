//! Encode / Decode implementations for engine record types.
//!
//! Extracted from `utils.rs` for readability — the implementations are
//! purely mechanical serialisation logic.

use super::utils::{RangeTombstone, Record};
use crate::encoding::{Decode, Encode, EncodingError};

// ------------------------------------------------------------------------------------------------
// Encode / Decode — Record
// ------------------------------------------------------------------------------------------------

impl Encode for Record {
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

impl Decode for Record {
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

impl Encode for RangeTombstone {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        self.start.encode_to(buf)?;
        self.end.encode_to(buf)?;
        self.lsn.encode_to(buf)?;
        self.timestamp.encode_to(buf)?;
        Ok(())
    }
}

impl Decode for RangeTombstone {
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
