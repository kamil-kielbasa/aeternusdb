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
// Includes
// ------------------------------------------------------------------------------------------------

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use bincode::{config::standard, decode_from_slice, encode_to_vec};
use crc32fast::Hasher as Crc32;
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
    pub fn new(max_record_size: u32) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            max_record_size,
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

        // If file is empty, create and write a new header.
        let header = if file.metadata()?.len() == 0 {
            let header =
                WalHeader::new(max_record_size.unwrap_or(WalHeader::DEFAULT_MAX_RECORD_SIZE));

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

            let sample = WalHeader::new(WalHeader::DEFAULT_MAX_RECORD_SIZE);
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

// ------------------------------------------------------------------------------------------------
// Unit tests
// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    //! Tests for WAL module.
    //!
    //! Tests use `tempfile::TempDir` so files are ephemeral.
    //!
    //! Each corruption test manipulates the WAL bytes on disk to simulate disk errors.

    use super::*;
    use tempfile::TempDir;
    use tracing::Level;
    use tracing_subscriber::fmt::Subscriber;

    const WAL_CRC32_SIZE: usize = std::mem::size_of::<u32>();
    const WAL_HDR_SIZE: usize = 12;

    #[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
    struct MemTableRecord {
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        timestamp: u64,
        deleted: bool,
    }

    #[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
    struct ManifestRecord {
        id: u64,
        path: String,
        creation_timestamp: u64,
    }

    fn init_tracing() {
        let _ = Subscriber::builder()
            .with_max_level(Level::TRACE)
            .try_init();
    }

    fn collect_iter<T: WalData>(wal: &Wal<T>) -> Result<Vec<T>, WalError> {
        wal.replay_iter()?.collect()
    }

    #[test]
    fn test_one_append_and_replay() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal_1.bin");
        let wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let insert = vec![MemTableRecord {
            key: b"a".to_vec(),
            value: Some(b"v1".to_vec()),
            timestamp: 1,
            deleted: false,
        }];

        for record in &insert {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(insert, replayed);
    }

    #[test]
    fn test_many_append_and_replay() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal_1.bin");
        let wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let insert = vec![
            ManifestRecord {
                id: 0,
                path: "/db/table-0".to_string(),
                creation_timestamp: 100,
            },
            ManifestRecord {
                id: 1,
                path: "/db/table-1".to_string(),
                creation_timestamp: 101,
            },
            ManifestRecord {
                id: 2,
                path: "/db/table-2".to_string(),
                creation_timestamp: 102,
            },
        ];

        for record in &insert {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(insert, replayed);
    }

    #[test]
    fn test_many_append_with_replay_and_truncate() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal_1.log");
        let mut wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let insert = vec![
            MemTableRecord {
                key: b"a".to_vec(),
                value: Some(b"v1".to_vec()),
                timestamp: 1,
                deleted: false,
            },
            MemTableRecord {
                key: b"b".to_vec(),
                value: Some(b"v2".to_vec()),
                timestamp: 2,
                deleted: false,
            },
            MemTableRecord {
                key: b"c".to_vec(),
                value: Some(b"v3".to_vec()),
                timestamp: 3,
                deleted: false,
            },
        ];

        for record in &insert {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(insert, replayed);

        wal.truncate().unwrap();
        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(replayed.len(), 0);
    }

    #[test]
    fn test_full_cycle_of_wal() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal_1.log");
        let mut wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let batch1 = vec![
            ManifestRecord {
                id: 0,
                path: "/db/table-0".to_string(),
                creation_timestamp: 100,
            },
            ManifestRecord {
                id: 1,
                path: "/db/table-1".to_string(),
                creation_timestamp: 101,
            },
        ];

        let batch2 = vec![
            ManifestRecord {
                id: 100,
                path: "/db/table-100".to_string(),
                creation_timestamp: 1000,
            },
            ManifestRecord {
                id: 101,
                path: "/db/table-101".to_string(),
                creation_timestamp: 1001,
            },
            ManifestRecord {
                id: 102,
                path: "/db/table-102".to_string(),
                creation_timestamp: 1002,
            },
        ];

        for record in &batch1 {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(batch1, replayed);

        wal.truncate().unwrap();
        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(replayed.len(), 0);

        for record in &batch2 {
            wal.append(record).unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(batch2, replayed);

        wal.truncate().unwrap();
        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(replayed.len(), 0);
    }

    #[test]
    fn test_corrupted_header_checksum() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad_header.bin");
        let _wal: Wal<MemTableRecord> = Wal::open(&path, None).unwrap();

        // Corrupt a single byte inside header bytes (not checksum).
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::Start(2)).unwrap();
        f.write_all(&[0x99]).unwrap();
        f.sync_all().unwrap();

        let err = Wal::<MemTableRecord>::open(&path, None).unwrap_err();
        assert!(matches!(err, WalError::InvalidHeader(_)));
        assert!(err.to_string().contains("Header checksum mismatch"));
    }

    #[test]
    fn test_corrupted_record_length() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad_len.bin");
        let wal = Wal::open(&path, None).unwrap();

        let record = MemTableRecord {
            key: b"a".to_vec(),
            value: Some(b"v1".to_vec()),
            timestamp: 1,
            deleted: false,
        };
        wal.append(&record).unwrap();
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        // Overwrite length with very large value (0xFFFFFFFF)
        f.seek(SeekFrom::Start((WAL_HDR_SIZE + WAL_CRC32_SIZE) as u64))
            .unwrap();
        f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        f.sync_all().unwrap();

        let err = collect_iter(&wal).unwrap_err();
        assert!(matches!(err, WalError::RecordTooLarge(_)));
    }

    #[test]
    fn test_corrupted_record_data_checksum() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad_record.bin");
        let wal = Wal::open(&path, None).unwrap();

        let record = ManifestRecord {
            id: 999,
            path: "/db/table-999".to_string(),
            creation_timestamp: 9999,
        };
        wal.append(&record).unwrap();
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::End(-3)).unwrap(); // corrupt last few bytes before checksum
        f.write_all(&[0xAA, 0xBB, 0xCC]).unwrap();
        f.sync_all().unwrap();

        let err = collect_iter(&wal).unwrap_err();
        assert!(matches!(err, WalError::ChecksumMismatch));
    }

    #[test]
    fn test_corrupted_record_data() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("corrupted_data.bin");
        let wal = Wal::open(&path, None).unwrap();

        let insert = vec![
            MemTableRecord {
                key: b"a".to_vec(),
                value: Some(b"v1".to_vec()),
                timestamp: 1,
                deleted: false,
            },
            MemTableRecord {
                key: b"b".to_vec(),
                value: None,
                timestamp: 2,
                deleted: true,
            },
        ];

        for record in &insert {
            wal.append(record).unwrap();
        }

        // Corrupt middle of file (inside record bytes)
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        // Skip header + checksum + first record length (4B) + some payload bytes
        // The first record is small, so we’ll flip a few bytes within its serialized data.
        let corrupt_offset = (WAL_HDR_SIZE + WAL_CRC32_SIZE + 5) as u64;
        f.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        f.write_all(&[0xFF, 0x00, 0xEE]).unwrap();
        f.sync_all().unwrap();

        // Attempt replay
        let err = collect_iter(&wal).unwrap_err();
        assert!(matches!(err, WalError::ChecksumMismatch));
    }

    #[test]
    fn test_partial_replay_after_last_record_corrupted() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("partial_replay.bin");
        let wal = Wal::open(&path, None).unwrap();

        let records = vec![
            ManifestRecord {
                id: 100,
                path: "/db/table-100".to_string(),
                creation_timestamp: 1000,
            },
            ManifestRecord {
                id: 101,
                path: "/db/table-101".to_string(),
                creation_timestamp: 1001,
            },
            ManifestRecord {
                id: 102,
                path: "/db/table-102".to_string(),
                creation_timestamp: 1002,
            },
        ];

        for record in &records {
            wal.append(record).unwrap();
        }

        // Corrupt *last record’s checksum* only
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::End(-2)).unwrap(); // last bytes of checksum
        f.write_all(&[0x99, 0x77]).unwrap();
        f.sync_all().unwrap();

        // Replay should read 2 valid records, then hit corruption
        let mut iter = wal.replay_iter().unwrap();

        let mut replayed = vec![];
        while let Some(res) = iter.next() {
            match res {
                Ok(record) => replayed.push(record),
                Err(WalError::ChecksumMismatch) => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        assert_eq!(replayed.len(), 2, "Only first two records should be valid");
        assert_eq!(replayed[0].path, "/db/table-100".to_string());
        assert_eq!(replayed[1].path, "/db/table-101".to_string());
    }
}
