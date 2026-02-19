use crate::encoding::{self, EncodingError};
use crate::wal::{Wal, WalData, WalError};
use tracing_subscriber::EnvFilter;

/// WAL header CRC32 size in bytes.
pub const WAL_CRC32_SIZE: usize = std::mem::size_of::<u32>();

/// WAL header size in bytes (everything before records start).
pub const WAL_HDR_SIZE: usize = 20;

/// Dummy record that models a memtable entry — used to verify WAL
/// round-trips of record types with `Option` fields.
#[derive(Debug, PartialEq)]
pub struct MemTableRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u64,
    pub deleted: bool,
}

impl encoding::Encode for MemTableRecord {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.key, buf)?;
        encoding::Encode::encode_to(&self.value, buf)?;
        encoding::Encode::encode_to(&self.timestamp, buf)?;
        encoding::Encode::encode_to(&self.deleted, buf)?;
        Ok(())
    }
}

impl encoding::Decode for MemTableRecord {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut offset = 0;
        let (key, n) = <Vec<u8>>::decode_from(&buf[offset..])?;
        offset += n;
        let (value, n) = <Option<Vec<u8>>>::decode_from(&buf[offset..])?;
        offset += n;
        let (timestamp, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        let (deleted, n) = bool::decode_from(&buf[offset..])?;
        offset += n;
        Ok((
            Self {
                key,
                value,
                timestamp,
                deleted,
            },
            offset,
        ))
    }
}

/// Dummy record that models a manifest entry — used to verify WAL
/// round-trips of a structurally different record type.
#[derive(Debug, PartialEq)]
pub struct ManifestRecord {
    pub id: u64,
    pub path: String,
    pub creation_timestamp: u64,
}

impl encoding::Encode for ManifestRecord {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.id, buf)?;
        encoding::Encode::encode_to(&self.path, buf)?;
        encoding::Encode::encode_to(&self.creation_timestamp, buf)?;
        Ok(())
    }
}

impl encoding::Decode for ManifestRecord {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut offset = 0;
        let (id, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        let (path, n) = String::decode_from(&buf[offset..])?;
        offset += n;
        let (creation_timestamp, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        Ok((
            Self {
                id,
                path,
                creation_timestamp,
            },
            offset,
        ))
    }
}

/// Initialize tracing subscriber controlled by `RUST_LOG` env var.
/// Safe to call multiple times — only the first call takes effect.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// Replay every record from the WAL into a `Vec`.
pub fn collect_iter<T: WalData>(wal: &Wal<T>) -> Result<Vec<T>, WalError> {
    wal.replay_iter()?.collect()
}
