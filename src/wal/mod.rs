//! Write-Ahead Logging (WAL) Module
//!
//! This module implements a **durable**, **append-only**, and **generic** Write-Ahead Log (WAL)
//! suitable for embedded databases and key-value storage engines.  
//! It provides **type-safe**, **CRC-protected**, and **thread-safe** persistence of arbitrary records
//! that implement the [`WalData`] trait.
//!
//! ## Design Overview
//!
//! The WAL ensures crash recovery and corruption detection for any serializable record type
//! (`MemTableRecord`, `ManifestRecord`, etc.). It uses [`crate::encoding`] for compact serialization
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
//!   - serialized record bytes (custom encoding format)
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
//! - **Safety:** Thread-safe, generic over any [`crate::encoding`] `Encode`/`Decode` type.

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

use crate::encoding::{self, EncodingError};
use crc32fast::Hasher as Crc32;
use std::ffi::OsStr;
use thiserror::Error;
use tracing::{debug, error, info, trace, warn};

const U32_SIZE: usize = std::mem::size_of::<u32>();

// ------------------------------------------------------------------------------------------------
// Error Types
// ------------------------------------------------------------------------------------------------

/// Errors returned by WAL operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WalError {
    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Encoding / decoding error.
    #[error("Encoding error: {0}")]
    Encoding(#[from] EncodingError),

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
#[derive(Debug)]
pub struct WalHeader {
    /// Magic constant to identify WAL files (`b"AWAL"`).
    magic: [u8; 4],

    /// WAL format version.
    version: u32,

    /// Maximum record size (in bytes).
    max_record_size: u32,

    /// Monotonically-increasing WAL sequence number (segment id).
    wal_seq: u64,
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
    /// - `wal_seq`: WAL segment sequence number.
    pub fn new(max_record_size: u32, wal_seq: u64) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            max_record_size,
            wal_seq,
        }
    }

    /// Encoded size of the header in bytes (without the trailing CRC).
    ///
    /// Layout: `magic(4) + version(4) + max_record_size(4) + wal_seq(8)` = 20.
    pub const ENCODED_SIZE: usize = 4 + 4 + 4 + 8;

    /// Total on-disk size of the header *including* its trailing CRC32.
    pub const HEADER_DISK_SIZE: usize = Self::ENCODED_SIZE + U32_SIZE;

    /// Returns the WAL segment sequence number.
    pub fn wal_seq(&self) -> u64 {
        self.wal_seq
    }

    /// Returns the maximum record size (in bytes).
    pub fn max_record_size(&self) -> u32 {
        self.max_record_size
    }

    /// Returns the WAL format version.
    pub fn version(&self) -> u32 {
        self.version
    }
}

impl encoding::Encode for WalHeader {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.magic, buf)?;
        encoding::Encode::encode_to(&self.version, buf)?;
        encoding::Encode::encode_to(&self.max_record_size, buf)?;
        encoding::Encode::encode_to(&self.wal_seq, buf)?;
        Ok(())
    }
}

impl encoding::Decode for WalHeader {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut offset = 0;
        let (magic, n) = <[u8; 4]>::decode_from(&buf[offset..])?;
        offset += n;
        let (version, n) = u32::decode_from(&buf[offset..])?;
        offset += n;
        let (max_record_size, n) = u32::decode_from(&buf[offset..])?;
        offset += n;
        let (wal_seq, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        Ok((
            Self {
                magic,
                version,
                max_record_size,
                wal_seq,
            },
            offset,
        ))
    }
}

// ------------------------------------------------------------------------------------------------
// Traits
// ------------------------------------------------------------------------------------------------

/// Trait for data types that can be written to and read from the WAL.
///
/// Any record type used with [`Wal`] must implement this trait,
/// which acts as a marker requiring [`crate::encoding`] serialization.
///
/// # Required Traits
/// - [`crate::encoding::Encode`]
/// - [`crate::encoding::Decode`]
/// - [`Send`] + [`Sync`] + [`Debug`]
pub trait WalData: encoding::Encode + encoding::Decode + std::fmt::Debug + Send + Sync {}
impl<T> WalData for T where T: encoding::Encode + encoding::Decode + std::fmt::Debug + Send + Sync {}

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
    path: PathBuf,

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

        let wal_seq = Self::parse_seq_from_path(path_ref)
            .ok_or(WalError::Internal("WAL name incorrect".into()))?;

        // If file is empty, create and write a new header.
        let header = if file.metadata()?.len() == 0 {
            let header = WalHeader::new(
                max_record_size.unwrap_or(WalHeader::DEFAULT_MAX_RECORD_SIZE),
                wal_seq,
            );

            write_header(&mut file, &header)?;
            file.sync_all()?;

            info!(path = %path_ref.display(), seq = wal_seq, "WAL created with new header");

            header
        } else {
            // Existing WAL → read and validate header + checksum.
            file.seek(SeekFrom::Start(0))?;

            let header = read_and_validate_header(&mut file)?;

            if header.wal_seq != wal_seq {
                return Err(WalError::InvalidHeader("sequence number mismatch".into()));
            }

            debug!(
                path = %path_ref.display(),
                max_record_size = header.max_record_size,
                seq = header.wal_seq,
                "WAL header validated"
            );

            header
        };

        info!(path = %path_ref.display(), seq = header.wal_seq, "WAL opened");

        Ok(Self {
            inner_file: Arc::new(Mutex::new(file)),
            path: path_ref.to_path_buf(),
            header,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Parse `wal_seq` from filename if it matches `wal-<seq>.log`.
    fn parse_seq_from_path(path: &Path) -> Option<u64> {
        let name = path.file_name().and_then(OsStr::to_str)?;
        // Expect pattern wal-000001.log or wal-1.log etc.
        if let Some(seq_str) = name
            .strip_prefix("wal-")
            .and_then(|s| s.strip_suffix(".log"))
        {
            return seq_str.parse::<u64>().ok();
        }
        None
    }

    /// Appends a single record to the WAL.
    ///
    /// The record is serialized using [`crate::encoding`] and written as:
    /// `[u32 len LE][record_bytes][u32 crc32 LE]`,
    /// where the CRC is computed over the `len || record_bytes`.
    ///
    /// # Parameters
    /// - `record`: Reference to the record implementing [`WalData`].
    pub fn append(&self, record: &T) -> Result<(), WalError> {
        let record_bytes = encoding::encode_to_vec(record)?;
        let record_len = u32::try_from(record_bytes.len())
            .map_err(|_| WalError::RecordTooLarge(record_bytes.len()))?;

        if record_len > self.header.max_record_size {
            return Err(WalError::RecordTooLarge(record_len as usize));
        }

        let len_bytes = record_len.to_le_bytes();
        let checksum = compute_crc(&[&len_bytes, &record_bytes]);

        // Lock and append atomically (from user's perspective).
        let mut guard = self
            .inner_file
            .lock()
            .map_err(|_| WalError::Internal("Mutex poisoned".into()))?;

        guard.write_all(&len_bytes)?;
        guard.write_all(&record_bytes)?;
        guard.write_all(&checksum.to_le_bytes())?;
        guard.sync_all()?;

        trace!(
            len = record_len,
            crc = format_args!("{checksum:08x}"),
            "WAL record appended"
        );
        Ok(())
    }

    /// Returns an iterator that replays all valid records from the WAL.
    ///
    /// The iterator reads the WAL sequentially, verifies CRC checksums,
    /// and decodes each entry into its original record type `T`.
    pub fn replay_iter(&self) -> Result<WalIter<T>, WalError> {
        debug!(path = %self.path.display(), "WAL replay started");

        let start_offset = WalHeader::HEADER_DISK_SIZE as u64;

        Ok(WalIter {
            file: Arc::clone(&self.inner_file),
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

        write_header(&mut *guard, &self.header)?;
        guard.sync_all()?;

        info!(path = %self.path.display(), "WAL truncated");
        Ok(())
    }

    /// Rotates to a new WAL segment with the next sequence number.
    ///
    /// Syncs the current WAL, opens a new WAL file with `wal_seq + 1`,
    /// and replaces `self` with the new instance.
    ///
    /// Returns the new WAL sequence number.
    pub fn rotate_next(&mut self) -> Result<u64, WalError> {
        {
            let guard = self
                .inner_file
                .lock()
                .map_err(|_| WalError::Internal("Mutex poisoned".into()))?;
            guard.sync_all()?;
        }

        let next_seq = self
            .header
            .wal_seq
            .checked_add(1)
            .ok_or_else(|| WalError::Internal("WAL sequence number overflow".into()))?;

        let cur_path = PathBuf::from(&self.path);
        let dir = cur_path.parent().unwrap_or_else(|| Path::new("."));
        let next_path = dir.join(format!("wal-{next_seq:06}.log"));

        let new_wal = Wal::<T>::open(&next_path, Some(self.header.max_record_size))?;
        *self = new_wal;

        Ok(next_seq)
    }

    /// Get the path of the underlying WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the WAL segment sequence number.
    pub fn wal_seq(&self) -> u64 {
        self.header.wal_seq
    }

    /// Returns the configured maximum record size.
    pub fn max_record_size(&self) -> u32 {
        self.header.max_record_size
    }

    /// Returns the current on-disk file size in bytes.
    pub fn file_size(&self) -> Result<u64, WalError> {
        let guard = self
            .inner_file
            .lock()
            .map_err(|_| WalError::Internal("Mutex poisoned".into()))?;
        Ok(guard.metadata()?.len())
    }
}

impl<T: WalData> Drop for Wal<T> {
    fn drop(&mut self) {
        match self.inner_file.lock() {
            Ok(guard) => {
                if let Err(e) = guard.sync_all() {
                    error!(path = %self.path.display(), error = %e, "WAL sync failed on drop");
                }
            }
            Err(poisoned) => {
                let file = poisoned.into_inner();
                if let Err(e) = file.sync_all() {
                    error!(path = %self.path.display(), error = %e, "WAL sync failed on drop (poisoned lock)");
                } else {
                    warn!(path = %self.path.display(), "WAL recovered and synced after poisoned lock");
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
///
/// # Lifetime & ownership
///
/// The iterator holds an `Arc` reference to the underlying file handle. This means
/// it can **outlive** the [`Wal`] that created it — the file will remain open until
/// all iterators (and the WAL itself) are dropped.
pub struct WalIter<T: WalData> {
    /// Shared file handle protected by a mutex.
    file: Arc<Mutex<File>>,

    /// Current byte offset within WAL file.
    offset: u64,

    /// Maximum allowed record size.
    max_record_size: usize,

    /// Marker field to associate this WAL iterator with the generic record type `T`.
    _phantom: std::marker::PhantomData<T>,
}

impl<T: WalData> std::fmt::Debug for WalIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalIter")
            .field("offset", &self.offset)
            .field("max_record_size", &self.max_record_size)
            .finish_non_exhaustive()
    }
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
                trace!(offset = self.offset, "WAL replay reached end of file");
                return None;
            }
            Err(e) => return Some(Err(WalError::Io(e))),
        }

        let record_len = u32::from_le_bytes(len_bytes) as usize;
        if record_len > self.max_record_size {
            return Some(Err(WalError::RecordTooLarge(record_len)));
        }

        trace!(offset = self.offset, len = record_len, "WAL reading record");

        // Read record bytes.
        let mut record_bytes = vec![0u8; record_len];
        if let Err(e) = guard.read_exact(&mut record_bytes) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                warn!(
                    offset = self.offset,
                    len = record_len,
                    "WAL truncated record (partial payload)"
                );
                return Some(Err(WalError::UnexpectedEof));
            }
            return Some(Err(WalError::Io(e)));
        }

        // Read stored checksum.
        let mut checksum_bytes = [0u8; U32_SIZE];
        if let Err(e) = guard.read_exact(&mut checksum_bytes) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                warn!(
                    offset = self.offset,
                    len = record_len,
                    "WAL truncated record (partial checksum)"
                );
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
        if let Err(e) = verify_crc(&[&len_bytes, &record_bytes], stored_checksum) {
            warn!(
                offset = self.offset,
                len = record_len,
                "WAL record checksum mismatch"
            );
            return Some(Err(e));
        }

        // Decode the record payload.
        match encoding::decode_from_slice::<T>(&record_bytes) {
            Ok((record, _)) => Some(Ok(record)),
            Err(e) => Some(Err(WalError::Encoding(e))),
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Header I/O helpers
// ------------------------------------------------------------------------------------------------

/// Writes a [`WalHeader`] followed by its CRC32 checksum, then syncs.
fn write_header<W: Write>(writer: &mut W, header: &WalHeader) -> Result<(), WalError> {
    let header_bytes = encoding::encode_to_vec(header)?;
    let checksum = compute_crc(&[&header_bytes]);

    writer.write_all(&header_bytes)?;
    writer.write_all(&checksum.to_le_bytes())?;

    // Sync if the writer is a File (trait objects won't have sync_all, but
    // our callers always follow up with their own sync when needed).
    Ok(())
}

/// Reads and validates a [`WalHeader`] from the current file position.
///
/// Checks CRC, magic, and version. Does **not** validate `wal_seq` (the
/// caller must do that, since the expected sequence depends on context).
fn read_and_validate_header<R: Read>(reader: &mut R) -> Result<WalHeader, WalError> {
    let mut header_bytes = vec![0u8; WalHeader::ENCODED_SIZE];
    reader.read_exact(&mut header_bytes)?;

    let mut checksum_bytes = [0u8; U32_SIZE];
    reader.read_exact(&mut checksum_bytes)?;
    let stored_checksum = u32::from_le_bytes(checksum_bytes);

    verify_crc(&[&header_bytes], stored_checksum)
        .map_err(|_| WalError::InvalidHeader("header checksum mismatch".into()))?;

    let (header, _) = encoding::decode_from_slice::<WalHeader>(&header_bytes)?;

    if header.magic != WalHeader::MAGIC {
        return Err(WalError::InvalidHeader("bad magic".into()));
    }
    if header.version != WalHeader::VERSION {
        return Err(WalError::InvalidHeader(format!(
            "unsupported version {}",
            header.version
        )));
    }

    Ok(header)
}

// ------------------------------------------------------------------------------------------------
// CRC helpers
// ------------------------------------------------------------------------------------------------

/// Computes a CRC32 checksum over one or more byte slices.
fn compute_crc(parts: &[&[u8]]) -> u32 {
    let mut hasher = Crc32::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize()
}

/// Verifies that the CRC32 over the given byte slices matches `expected`.
fn verify_crc(parts: &[&[u8]], expected: u32) -> Result<(), WalError> {
    let computed = compute_crc(parts);
    if computed != expected {
        return Err(WalError::ChecksumMismatch);
    }
    Ok(())
}
