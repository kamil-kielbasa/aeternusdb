use crate::wal::{Wal, WalData, WalError};
use tracing_subscriber::EnvFilter;

/// WAL header CRC32 size in bytes.
pub const WAL_CRC32_SIZE: usize = std::mem::size_of::<u32>();

/// WAL header size in bytes (everything before records start).
pub const WAL_HDR_SIZE: usize = 20;

/// Dummy record that models a memtable entry — used to verify WAL
/// round-trips of record types with `Option` fields.
#[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub struct MemTableRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u64,
    pub deleted: bool,
}

/// Dummy record that models a manifest entry — used to verify WAL
/// round-trips of a structurally different record type.
#[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub struct ManifestRecord {
    pub id: u64,
    pub path: String,
    pub creation_timestamp: u64,
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
