//! Write-Ahead Logging (WAL) Module
//!
//! This module implements a **durable**, **append-only**, and **generic** Write-Ahead Log (WAL)
//! suitable for embedded databases and key-value storage engines.  
//! It provides **type-safe**, **CRC-protected**, and **thread-safe** persistence of arbitrary records
//! that implement the [`WalData`] trait.
//!
//! //! ## Design Overview
//!
//! The WAL ensures crash recovery and corruption detection for any serializable record type
//! (`MemTableRecord`, `ManifestRecord`, etc.). It uses [`bincode`] for compact serialization
//! and [`crc32fast`] for data integrity.
//!
//! Each record is appended sequentially to disk with atomic file syncs to ensure durability.
//! The file handle is shared safely between threads using `Arc<Mutex<File>>`.
//!
//! # On-disk layout
//!
//! ```text
//! [HEADER_BYTES][HEADER_CRC32_LE]
//! [REC_LEN_LE][REC_BYTES][REC_CRC32_LE]
//! [REC_LEN_LE][REC_BYTES][REC_CRC32_LE]
//! ...
//! ```
//!
//! - **Header** — a [`WalHeader`] structure followed by a 4-byte CRC32 checksum.
//! - **Record** — consists of:
//!   - 4-byte little-endian length prefix
//!   - serialized record bytes (`bincode` format)
//!   - 4-byte CRC32 checksum computed over `len || record_bytes`
//!
//! # Concurrency model
//!
//! - WAL access is **synchronized** via `Arc<Mutex<File>>`, ensuring consistent reads and writes.
//! - Multiple components can safely share the same WAL — e.g. background compaction, recovery, and
//!   write threads.
//! - [`WalIter`] tracks its own logical offset, seeking before each read to avoid race conditions
//!   with concurrent appenders.
//!
//! # Guarantees
//!
//! - **Durability:** Every `append()` is followed by an `fsync()` via [`File::sync_all`].  
//! - **Integrity:** Both header and record checksums are verified during replay.  
//! - **Corruption detection:** Replay stops at first failed checksum or truncated write.  
//! - **Safety:** Thread-safe, generic over any [`bincode`] `Encode`/`Decode` type.

// ------------------------------------------------------------------------------------------------
// Unit tests
// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests;

// ------------------------------------------------------------------------------------------------
// Includes
// ------------------------------------------------------------------------------------------------

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use bincode::{config::standard, decode_from_slice, encode_to_vec};
use crc32fast::Hasher as Crc32;
use std::ffi::OsStr;
use thiserror::Error;
use tracing::{error, info, trace, warn};

const U32_SIZE: usize = std::mem::size_of::<u32>();

// ------------------------------------------------------------------------------------------------
// Error Types
// ------------------------------------------------------------------------------------------------

/// Errors returned by WAL operations.
#[derive(Debug, Error)]
pub enum WalError {
    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Serialization error.
    #[error("Serialization (encode) error: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    /// Deserialization error.
    #[error("Deserialization (decode) error: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    /// Data integrity failure — checksum did not match.
    #[error("Checksum mismatch")]
    ChecksumMismatch,

    /// Record exceeds the configured maximum size.
    #[error("Record size exceeds limit ({0} bytes)")]
    RecordTooLarge(usize),

    /// Unexpected end-of-file during read.
    #[error("Unexpected end of file")]
    UnexpectedEof,

    /// WAL header failed integrity validation.
    #[error("Internal header: {0}")]
    InvalidHeader(String),

    /// Internal consistency or locking error.
    #[error("Internal error: {0}")]
    Internal(String),
}

// ------------------------------------------------------------------------------------------------
// Header / Record structures
// ------------------------------------------------------------------------------------------------

/// Metadata written at the start of the WAL file.
///
/// This section validates the WAL’s identity and constraints.
/// It is followed by a CRC32 checksum to protect against corruption.
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct WalHeader {
    /// Magic constant to identify WAL files (`b"AWAL"`).
    pub magic: [u8; 4],

    /// WAL format version.
    pub version: u32,

    /// Maximum record size (in bytes).
    pub max_record_size: u32,

    /// Monotonically-increasing WAL sequence number (segment id).
    pub wal_seq: u64,
}

impl WalHeader {
    /// Expected 4-byte magic constant.
    pub const MAGIC: [u8; 4] = *b"AWAL";

    /// Current supported version number.
    pub const VERSION: u32 = 1;

    /// Default maximum record size (1 MiB).
    pub const DEFAULT_MAX_RECORD_SIZE: u32 = 1024 * 1024;

    /// Creates a new [`WalHeader`] instance.
    ///
    /// # Parameters
    /// - `max_record_size`: Maximum record size limit.
    pub fn new(max_record_size: u32, wal_seq: u64) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            max_record_size,
            wal_seq,
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Traits
// ------------------------------------------------------------------------------------------------

/// Trait for data types that can be written to and read from the WAL.
///
/// Any record type used with [`Wal`] must implement this trait,
/// which acts as a marker requiring [`bincode`] serialization.
///
/// # Required Traits
/// - [`bincode::Encode`]
/// - [`bincode::Decode`]
/// - [`Send`] + [`Sync`] + [`Debug`]
pub trait WalData: bincode::Encode + bincode::Decode<()> + std::fmt::Debug + Send + Sync {}
impl<T> WalData for T where T: bincode::Encode + bincode::Decode<()> + std::fmt::Debug + Send + Sync {}

// ------------------------------------------------------------------------------------------------
// WAL Core
// ------------------------------------------------------------------------------------------------

/// A generic, thread-safe Write-Ahead Log for durable record storage.
///
/// See the [module-level documentation](self) for more details on format,
/// concurrency, and guarantees.
///
/// # Type Parameters
///
/// * `T` — Any record type implementing [`WalData`].
#[derive(Debug)]
pub struct Wal<T: WalData> {
    /// Thread-safe file handle for WAL operations.
    inner_file: Arc<Mutex<File>>,

    /// Path to the WAL file on disk.
    path: String,

    /// Persistent header with metadata and integrity info.
    header: WalHeader,

    /// Marker field to associate this WAL with the generic record type `T`.
    _phantom: std::marker::PhantomData<T>,
}

impl<T: WalData> Wal<T> {
    /// Open or create a WAL file at the given path.
    ///
    /// # Parameters
    /// - `path`: Path to the WAL file.
    /// - `max_record_size`: Optional custom maximum record size.
    ///
    /// # Returns
    /// A [`Wal`] instance, ready for appending or replaying records.
    pub fn open<P: AsRef<Path>>(path: P, max_record_size: Option<u32>) -> Result<Self, WalError> {
        let path_ref = path.as_ref();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path_ref)?;

        let config = standard().with_fixed_int_encoding();

        let wal_seq = Self::parse_seq_from_path(path_ref)
            .ok_or(WalError::Internal("WAL name incorrect".into()))?;

        // If file is empty, create and write a new header.
        let header = if file.metadata()?.len() == 0 {
            let header = WalHeader::new(
                max_record_size.unwrap_or(WalHeader::DEFAULT_MAX_RECORD_SIZE),
                wal_seq,
            );

            let header_bytes = encode_to_vec(&header, config).map_err(WalError::Encode)?;

            let mut hasher = Crc32::new();
            hasher.update(&header_bytes);
            let checksum = hasher.finalize();

            file.write_all(&header_bytes)?;
            file.write_all(&checksum.to_le_bytes())?;
            file.sync_all()?;

            info!("Created new WAL header at {}", path_ref.display());

            header
        } else {
            // Existing WAL → read and validate header + checksum.
            file.seek(SeekFrom::Start(0))?;

            let sample = WalHeader::new(WalHeader::DEFAULT_MAX_RECORD_SIZE, 0);
            let sample_bytes = encode_to_vec(&sample, config).map_err(WalError::Encode)?;
            let header_len = sample_bytes.len();

            let mut header_bytes = vec![0u8; header_len];
            file.read_exact(&mut header_bytes)?;

            let mut checksum_bytes = [0u8; U32_SIZE];
            file.read_exact(&mut checksum_bytes)?;
            let stored_checksum = u32::from_le_bytes(checksum_bytes);

            let mut hasher = Crc32::new();
            hasher.update(&header_bytes);
            let computed_checksum = hasher.finalize();

            if stored_checksum != computed_checksum {
                return Err(WalError::InvalidHeader("Header checksum mismatched".into()));
            }

            let (header, _) = decode_from_slice::<WalHeader, _>(&header_bytes, config)?;

            if header.magic != WalHeader::MAGIC {
                return Err(WalError::InvalidHeader("Bad magic".into()));
            }
            if header.version != WalHeader::VERSION {
                return Err(WalError::InvalidHeader(format!(
                    "Unsupported version {}",
                    header.version
                )));
            }

            if header.wal_seq != wal_seq {
                return Err(WalError::InvalidHeader("Invalid sequence number".into()));
            }

            info!(
                "Loaded WAL header from {} (max_record_size={})",
                path_ref.display(),
                header.max_record_size
            );

            header
        };

        info!("Opened WAL file at {}", path_ref.display());

        Ok(Self {
            inner_file: Arc::new(Mutex::new(file)),
            path: path_ref.display().to_string(),
            header,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Parse `wal_seq` from filename if it matches `wal-<seq>.log`.
    fn parse_seq_from_path(path: &Path) -> Option<u64> {
        let name = path.file_name().and_then(OsStr::to_str)?;
        // Expect pattern wal-000001.log or wal-1.log etc.
        if let Some(stripped) = name.strip_prefix("wal-") {
            if let Some(seq_str) = stripped.strip_suffix(".log") {
                return seq_str.parse::<u64>().ok();
            }
        }
        None
    }

    /// Appends a single record to the WAL.
    ///
    /// The record is serialized using [`bincode`] and written as:
    /// `[u32 len LE][record_bytes][u32 crc32 LE]`,
    /// where the CRC is computed over the `len || record_bytes`.
    ///
    /// # Parameters
    /// - `record`: Reference to the record implementing [`WalData`].
    pub fn append(&self, record: &T) -> Result<(), WalError> {
        trace!("Appending record: {:?}", record,);

        let config = standard().with_fixed_int_encoding();

        let record_bytes = encode_to_vec(record, config).map_err(WalError::Encode)?;
        let record_len = record_bytes.len() as u32;

        if record_len > self.header.max_record_size {
            return Err(WalError::RecordTooLarge(record_len as usize));
        }

        // Compute checksum over [len_le || record_bytes]
        let mut hasher = Crc32::new();
        hasher.update(&record_len.to_le_bytes());
        hasher.update(&record_bytes);
        let checksum = hasher.finalize();

        // Lock and append atomically (from user's perspective).
        let mut guard = self
            .inner_file
            .lock()
            .map_err(|_| WalError::Internal("Mutex poisoned".into()))?;

        guard.write_all(&record_len.to_le_bytes())?;
        guard.write_all(&record_bytes)?;
        guard.write_all(&checksum.to_le_bytes())?;
        guard.sync_all()?;

        info!(
            "Appended record of length {} with checksum {:08x}",
            record_len, checksum
        );
        Ok(())
    }

    /// Returns an iterator that replays all valid records from the WAL.
    ///
    /// The iterator reads the WAL sequentially, verifies CRC checksums,
    /// and decodes each entry into its original record type `T`.
    pub fn replay_iter(&self) -> Result<WalIter<T>, WalError> {
        info!("Starting WAL replay from file: {}", self.path);

        let config = standard().with_fixed_int_encoding();
        let header_bytes = encode_to_vec(&self.header, config).map_err(WalError::Encode)?;
        let start_offset = (header_bytes.len() + U32_SIZE) as u64;

        Ok(WalIter {
            file: Arc::clone(&self.inner_file),
            config,
            offset: start_offset,
            max_record_size: self.header.max_record_size as usize,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Truncate (clear) the WAL and rewrite header.
    ///
    /// After truncation, WAL contains only the header and its checksum.
    pub fn truncate(&mut self) -> Result<(), WalError> {
        let mut guard = self
            .inner_file
            .lock()
            .map_err(|_| WalError::Internal("Mutex poisoned".into()))?;

        guard.set_len(0)?;
        guard.seek(SeekFrom::Start(0))?;

        let config = standard().with_fixed_int_encoding();
        let header_bytes = encode_to_vec(&self.header, config).map_err(WalError::Encode)?;

        let mut hasher = Crc32::new();
        hasher.update(&header_bytes);
        let checksum = hasher.finalize();

        guard.write_all(&header_bytes)?;
        guard.write_all(&checksum.to_le_bytes())?;
        guard.sync_all()?;

        info!("Truncated WAL file: {}", self.path);
        Ok(())
    }

    pub fn rotate_next(&mut self) -> Result<u64, WalError> {
        {
            let guard = self
                .inner_file
                .lock()
                .map_err(|_| WalError::Internal("Mutex poisoned".into()))?;
            guard.sync_all()?;
        }

        let next_seq = self.header.wal_seq.saturating_add(1);

        let cur_path = PathBuf::from(&self.path);
        let dir = cur_path.parent().unwrap_or_else(|| Path::new("."));
        let next_path = dir.join(format!("wal-{next_seq:06}.log"));

        let mut new_wal = Wal::<T>::open(&next_path, Some(self.header.max_record_size))?;
        new_wal.header.wal_seq = next_seq;
        *self = new_wal;

        Ok(next_seq)
    }

    /// Get the path of the underlying WAL file.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl<T: WalData> Drop for Wal<T> {
    fn drop(&mut self) {
        match self.inner_file.lock() {
            Ok(guard) => {
                if let Err(e) = guard.sync_all() {
                    error!("Failed to sync WAL on drop: {}", e);
                }
            }
            Err(poisoned) => {
                let file = poisoned.into_inner();
                if let Err(e) = file.sync_all() {
                    error!("Failed to sync WAL (poisoned) on drop: {}", e);
                } else {
                    warn!("Recovered and synced WAL after poisoned lock");
                }
            }
        }
    }
}

// ------------------------------------------------------------------------------------------------
// WalIter
// ------------------------------------------------------------------------------------------------

/// Streaming WAL replay iterator.
///
/// `WalIter` reads records sequentially from the WAL file and yields decoded `T` values.
/// It is designed to:
///
/// - **Stream** records without allocating the entire WAL into memory (one record at a time).
/// - **Share** the WAL file safely with appenders by holding an `Arc<Mutex<File>>`.
/// - **Detect corruption** and truncated writes using CRC32 checksums and length bounds.
pub struct WalIter<T: WalData> {
    /// Shared file handle protected by a mutex.
    file: Arc<Mutex<File>>,

    /// Bincode configuration for decoding.
    config: bincode::config::Configuration<
        bincode::config::LittleEndian,
        bincode::config::Fixint,
        bincode::config::NoLimit,
    >,

    /// Current byte offset within WAL file.
    offset: u64,

    /// Maximum allowed record size.
    max_record_size: usize,

    /// Marker field to associate this WAL iterator with the generic record type `T`.
    _phantom: std::marker::PhantomData<T>,
}

impl<T: WalData> Iterator for WalIter<T> {
    type Item = Result<T, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Lock only during the read of one record to reduce contention.
        let mut guard = match self.file.lock() {
            Ok(g) => g,
            Err(_) => return Some(Err(WalError::Internal("Mutex poisoned".into()))),
        };

        // Seek to our logical offset for deterministic reads.
        if let Err(e) = guard.seek(SeekFrom::Start(self.offset)) {
            return Some(Err(WalError::Io(e)));
        }

        // Read length prefix (4 bytes).
        let mut len_bytes = [0u8; U32_SIZE];

        match guard.read_exact(&mut len_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                trace!("End of WAL reached");
                return None;
            }
            Err(e) => return Some(Err(WalError::Io(e))),
        }

        let record_len = u32::from_le_bytes(len_bytes) as usize;
        if record_len > self.max_record_size {
            return Some(Err(WalError::RecordTooLarge(record_len)));
        }

        trace!("Reading record of length {}", record_len);

        // Read record bytes.
        let mut record_bytes = vec![0u8; record_len];
        if let Err(e) = guard.read_exact(&mut record_bytes) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                error!("Truncated WAL record detected");
                return Some(Err(WalError::UnexpectedEof));
            }
            return Some(Err(WalError::Io(e)));
        }

        // Read stored checksum.
        let mut checksum_bytes = [0u8; U32_SIZE];
        if let Err(e) = guard.read_exact(&mut checksum_bytes) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                error!("Truncated WAL record detected");
                return Some(Err(WalError::UnexpectedEof));
            }
            return Some(Err(WalError::Io(e)));
        }
        let stored_checksum = u32::from_le_bytes(checksum_bytes);

        // Update offset for next iteration using current file cursor position.
        if let Ok(pos) = guard.stream_position() {
            self.offset = pos;
        }

        // Verify checksum over [len || record_bytes].
        let mut hasher = Crc32::new();
        hasher.update(&len_bytes);
        hasher.update(&record_bytes);
        let computed_checksum = hasher.finalize();

        if stored_checksum != computed_checksum {
            error!("Checksum mismatch for record of length {}", record_len);
            return Some(Err(WalError::ChecksumMismatch));
        }

        // Decode the record payload.
        match decode_from_slice::<T, _>(&record_bytes, self.config) {
            Ok((record, _)) => Some(Ok(record)),
            Err(e) => Some(Err(WalError::Decode(e))),
        }
    }
}
