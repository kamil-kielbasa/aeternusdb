//! Write-Ahead Logging (WAL) Module
//!
//! This module implements a durable, append-only write-ahead log (WAL) suitable for
//! simple key-value storage engines. It emphasizes correctness, detectability of corruption,
//! and safe concurrent access via `Arc<Mutex<File>>`.
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
//! - Header is serialized with `bincode` and followed by a 4-byte CRC32 checksum (little-endian).
//! - Each record contains a 4-byte little-endian length, the `bincode`-serialized `WalRecord`,
//!   and a 4-byte CRC32 checksum computed over `length || record_bytes`.
//!
//! # Concurrency model
//!
//! - The WAL file handle is stored as `Arc<Mutex<File>>` so multiple owners (the `Wal` instance,
//!   replay iterators, or background tasks) can share the file safely.
//! - The iterator seeks to its own tracked `offset` before each read to avoid races with other appenders.
//!
//! # Guarantees
//!
//! - Per-record and header-level CRC32 checksums detect disk corruption and truncated writes.
//! - Replay stops at first detected corruption; earlier records remain available.
//! - `max_record_size` bounds memory usage during replay.

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

/// A single entry stored in the WAL.
///
/// An `Entry` may represent an insertion/update (with `value`) or a deletion (tombstone).
#[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub struct Entry {
    /// Logical timestamp or sequence number for this record.
    ///
    /// Used for ordering or conflict resolution during recovery.
    pub timestamp: u64,

    /// Whether this entry represents a key deletion.
    pub is_delete: bool,

    /// Optional binary value. Absent if `is_delete = true`.
    pub value: Option<Vec<u8>>,
}

/// WAL record containing key and entry.
///
/// Serialized via `bincode` as the payload for each record.
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct WalRecord {
    /// Binary key for this record.
    key: Vec<u8>,

    /// Metadata and value payload.
    entry: Entry,
}

// ------------------------------------------------------------------------------------------------
// WAL Core
// ------------------------------------------------------------------------------------------------

/// Thread-safe Write-Ahead Log.
///
/// Use `Arc<Wal>` to share WAL across threads. The underlying file handle is shared via
/// `Arc<Mutex<File>>`, permitting concurrent iterators and writers (I/O is serialized by the mutex).
#[derive(Debug)]
pub struct Wal {
    /// Thread-safe file handle for WAL operations.
    inner_file: Arc<Mutex<File>>,

    /// Path to the WAL file on disk.
    path: String,

    /// Persistent header with metadata and integrity info.
    header: WalHeader,
}

impl Wal {
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
        })
    }

    /// Append a record to the WAL.
    ///
    /// Each record is serialized with `bincode` and written as:
    /// `[u32 len LE][record_bytes][u32 crc32 LE]` where CRC32 is over `len || record_bytes`.
    ///
    /// # Parameters
    ///
    /// - `key`: Key bytes.
    /// - `value`: Optional value bytes (None for delete).
    /// - `timestamp`: Logical timestamp or sequence number.
    /// - `is_delete`: Whether this record is a deletion.
    pub fn append(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        timestamp: u64,
        is_delete: bool,
    ) -> Result<(), WalError> {
        trace!(
            "Appending record: key={:?}, timestamp={}, is_delete={}",
            key, timestamp, is_delete
        );

        let entry = Entry {
            timestamp,
            is_delete,
            value: value.map(|v| v.to_vec()),
        };

        let record = WalRecord {
            key: key.to_vec(),
            entry,
        };

        let config = standard().with_fixed_int_encoding();

        let record = encode_to_vec(&record, config).map_err(WalError::Encode)?;
        let record_len = record.len() as u32;

        if record_len > self.header.max_record_size {
            return Err(WalError::RecordTooLarge(record_len as usize));
        }

        // Compute checksum over [len_le || record_bytes]
        let mut hasher = Crc32::new();
        hasher.update(&record_len.to_le_bytes());
        hasher.update(&record);
        let checksum = hasher.finalize();

        // Lock and append atomically (from user's perspective).
        let mut guard = self
            .inner_file
            .lock()
            .map_err(|_| WalError::Internal("Mutex poisoned".into()))?;

        guard.write_all(&record_len.to_le_bytes())?;
        guard.write_all(&record)?;
        guard.write_all(&checksum.to_le_bytes())?;
        guard.sync_all()?;

        info!(
            "Appended record of length {} with checksum {:08x}",
            record_len, checksum
        );
        Ok(())
    }

    /// Create a replay iterator starting immediately after the header.
    ///
    /// The iterator holds an `Arc` clone of the internal file handle and manages its own
    /// `offset`. The iterator seeks to `offset` before each record read to avoid races
    /// with concurrent appenders.
    pub fn replay_iter(&self) -> Result<WalIter, WalError> {
        info!("Starting WAL replay from file: {}", self.path);

        let config = standard().with_fixed_int_encoding();
        let header_bytes = encode_to_vec(&self.header, config).map_err(WalError::Encode)?;
        let start_offset = (header_bytes.len() + U32_SIZE) as u64;

        Ok(WalIter {
            file: Arc::clone(&self.inner_file),
            config,
            offset: start_offset,
            max_record_size: self.header.max_record_size as usize,
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

impl Drop for Wal {
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

/// Iterator for replaying WAL records.
///
/// Holds a cloned `Arc<Mutex<File>>`. Each `next()` locks the file briefly,
/// seeks to the iterator's current `offset`, reads one record, validates checksum,
/// decodes it, updates `offset`, and returns it.
///
/// On corruption or I/O error the iterator yields `Err(WalError)` (not panic).
pub struct WalIter {
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
}

impl Iterator for WalIter {
    type Item = Result<(Vec<u8>, Entry), WalError>;

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
        match decode_from_slice::<WalRecord, _>(&record_bytes, self.config) {
            Ok((rec, _)) => Some(Ok((rec.key, rec.entry))),
            Err(e) => Some(Err(WalError::Decode(e))),
        }
    }
}

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

    fn init_tracing() {
        let _ = Subscriber::builder()
            .with_max_level(Level::TRACE)
            .try_init();
    }

    fn make_entry(ts: u64, del: bool, val: Option<Vec<u8>>) -> Entry {
        Entry {
            timestamp: ts,
            is_delete: del,
            value: val,
        }
    }

    fn collect_iter(wal: &Wal) -> Result<Vec<(Vec<u8>, Entry)>, WalError> {
        wal.replay_iter()?.collect()
    }

    #[test]
    fn test_one_append_and_replay() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal_1.bin");
        let wal = Wal::open(path.to_str().unwrap(), None).unwrap();

        let insert = vec![(vec![0, 0, 0, 1], make_entry(841, false, Some(vec![255; 4])))];
        for (k, e) in &insert {
            wal.append(k, e.value.as_deref(), e.timestamp, e.is_delete)
                .unwrap();
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
            (vec![0, 0, 0, 1], make_entry(841, false, Some(vec![255; 4]))),
            (
                vec![0, 0, 0, 2],
                make_entry(842, false, Some(vec![1, 2, 3, 4])),
            ),
            (vec![0, 0, 0, 3], make_entry(843, true, None)),
        ];

        for (k, e) in &insert {
            wal.append(k, e.value.as_deref(), e.timestamp, e.is_delete)
                .unwrap();
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
            (vec![0, 0, 0, 1], make_entry(124, false, Some(vec![255; 4]))),
            (
                vec![0, 0, 0, 2],
                make_entry(125, false, Some(vec![1, 2, 3, 4])),
            ),
            (vec![0, 0, 0, 3], make_entry(126, true, None)),
            (
                vec![0, 0, 0, 4],
                make_entry(127, false, Some(vec![11, 13, 17, 19])),
            ),
        ];

        for (k, e) in &insert {
            wal.append(k, e.value.as_deref(), e.timestamp, e.is_delete)
                .unwrap();
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
            (vec![0, 0, 0, 1], make_entry(124, false, Some(vec![255; 4]))),
            (
                vec![0, 0, 0, 2],
                make_entry(125, false, Some(vec![1, 2, 3, 4])),
            ),
        ];

        let batch2 = vec![
            (vec![0, 0, 0, 3], make_entry(126, true, None)),
            (
                vec![0, 0, 0, 4],
                make_entry(127, false, Some(vec![11, 13, 17, 19])),
            ),
            (
                vec![0, 0, 0, 5],
                make_entry(128, false, Some(vec![16, 32, 64, 128])),
            ),
        ];

        for (k, e) in &batch1 {
            wal.append(k, e.value.as_deref(), e.timestamp, e.is_delete)
                .unwrap();
        }

        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(batch1, replayed);

        wal.truncate().unwrap();
        let replayed = collect_iter(&wal).unwrap();
        assert_eq!(replayed.len(), 0);

        for (k, e) in &batch2 {
            wal.append(k, e.value.as_deref(), e.timestamp, e.is_delete)
                .unwrap();
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
        let _wal = Wal::open(&path, None).unwrap();

        // Corrupt a single byte inside header bytes (not checksum).
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::Start(2)).unwrap();
        f.write_all(&[0x99]).unwrap();
        f.sync_all().unwrap();

        let err = Wal::open(&path, None).unwrap_err();
        assert!(matches!(err, WalError::InvalidHeader(_)));
        assert!(err.to_string().contains("Header checksum mismatch"));
    }

    #[test]
    fn test_corrupted_record_length() {
        init_tracing();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad_len.bin");
        let wal = Wal::open(&path, None).unwrap();

        wal.append(b"k", Some(b"v"), 1, false).unwrap();
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

        wal.append(b"k", Some(b"v"), 1, false).unwrap();
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

        wal.append(b"key1", Some(b"value1"), 1, false).unwrap();
        wal.append(b"key2", Some(b"value2"), 2, false).unwrap();

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
            (b"k1".to_vec(), make_entry(1, false, Some(b"v1".to_vec()))),
            (b"k2".to_vec(), make_entry(2, false, Some(b"v2".to_vec()))),
            (b"k3".to_vec(), make_entry(3, false, Some(b"v3".to_vec()))),
        ];

        for (k, e) in &records {
            wal.append(k, e.value.as_deref(), e.timestamp, e.is_delete)
                .unwrap();
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
                Ok((k, e)) => replayed.push((k, e)),
                Err(WalError::ChecksumMismatch) => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        assert_eq!(replayed.len(), 2, "Only first two records should be valid");
        assert_eq!(replayed[0].0, b"k1");
        assert_eq!(replayed[1].0, b"k2");
    }
}
