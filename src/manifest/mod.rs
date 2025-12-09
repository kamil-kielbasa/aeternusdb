//! # Manifest Component
//!
//! The **Manifest** is the central metadata authority for the LSM storage engine.
//! It tracks the engine’s durable state, including:
//!
//! - active WAL segment ID,
//! - frozen (older) WAL segments,
//! - list of existing SSTables,
//! - latest durable global LSN,
//! - manifest version number.
//!
//! The manifest acts as a *miniature WAL-driven metadata database*.
//!
//! ## Data durability strategy
//!
//! Manifest metadata is persisted using a **WAL + periodic snapshot** model:
//!
//! 1. **Manifest WAL** (`manifest.wal`) records mutation operations:
//!    - switching active WAL,
//!    - promoting WALs to frozen,
//!    - adding/removing SSTables,
//!    - updating LSN.
//!
//! 2. **Manifest snapshot** (`manifest.snapshot`) is a compact bincode-encoded
//!    dump of the whole metadata structure. Checksum ensures corruption detection.
//!
//! 3. On startup:
//!    - If a valid snapshot exists → load snapshot, replay WAL.
//!    - If snapshot corrupted → return error.
//!
//! This ensures crash recovery is always correct and consistent.
//!
//! ## Thread safety
//!
//! - **WAL** is internally synchronized — no external lock is required.
//! - **ManifestData** is wrapped in a `Mutex` to coordinate concurrent metadata operations.
//!
//! The manifest itself is fully thread-safe and can be accessed from any engine thread.

// ------------------------------------------------------------------------------------------------
// Unit tests
// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests;

// ------------------------------------------------------------------------------------------------
// Includes
// ------------------------------------------------------------------------------------------------

use crate::wal::{Wal, WalError};
use bincode::{config::standard, decode_from_slice, encode_to_vec};
use crc32fast::Hasher as Crc32;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};
use thiserror::Error;
use tracing::{error, info, trace, warn};

const SNAPSHOT_TMP_SUFFIX: &str = ".tmp";
const SNAPSHOT_FILENAME: &str = "manifest.snapshot";
const WAL_FILENAME: &str = "manifest.wal";
const U32_SIZE: usize = std::mem::size_of::<u32>();

// ------------------------------------------------------------------------------------------------
// Error Types
// ------------------------------------------------------------------------------------------------

/// Errors returned by manifest operations.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// Underlying WAL I/O failure.
    #[error("WAL error: {0}")]
    WAL(#[from] WalError),

    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Serialization error.
    #[error("Serialization (encode) error: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    /// Deserialization error.
    #[error("Deserialization (decode) error: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    /// Snapshot file is corrupted or checksum mismatched.
    #[error("Snapshot checksum mismatch")]
    SnapshotChecksumMismatch,

    /// Internal invariant violation or poisoned lock.
    #[error("Internal error: {0}")]
    Internal(String),
}

// ------------------------------------------------------------------------------------------------
// Manifest data structures
// ------------------------------------------------------------------------------------------------

/// Immutable in-memory representation of the manifest durable state.
///
/// This structure stores the persistent metadata describing
/// the layout of the LSM tree.
#[derive(Debug, PartialEq, Clone, bincode::Encode, bincode::Decode)]
pub struct ManifestData {
    /// Monotonically increasing manifest version.
    pub version: u64,

    /// Last globally assigned LSN (Log Sequence Number).
    pub last_lsn: u64,

    /// Identifier of current active WAL segment.
    pub active_wal: u64,

    /// Identifiers of frozen WAL segments (older, ready for flush).
    pub frozen_wals: Vec<u64>,

    /// List of all SSTables belonging to the LSM tree.
    pub sstables: Vec<ManifestSstEntry>,

    /// Indicates whether the current manifest differs from the recorded snapshot.   
    pub dirty: bool,
}

/// Entry describing a single SSTable known to the manifest.
///
/// Identifies table by unique ID and on-disk path.
#[derive(Debug, Clone, PartialEq, bincode::Encode, bincode::Decode)]
pub struct ManifestSstEntry {
    /// Globally unique SSTable ID.
    pub id: u64,

    /// Filesystem path to SSTable file.
    pub path: PathBuf,
}

impl Default for ManifestData {
    fn default() -> Self {
        Self {
            version: 1,
            last_lsn: 0,
            active_wal: 0,
            frozen_wals: Vec::new(),
            sstables: Vec::new(),
            dirty: false,
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Manifest record types
// ------------------------------------------------------------------------------------------------

/// Record stored in manifest WAL. Each variant describes
/// a single metadata mutation applied to ManifestData.
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub enum ManifestEvent {
    /// Sets a new version of manifest
    Version { version: u64 },

    /// Sets a new WAL segment as active.
    SetActiveWal { wal: u64 },

    /// Adds a WAL segment to the frozen list.
    AddFrozenWal { wal: u64 },

    /// Removes a frozen WAL from manifest state.
    RemoveFrozenWal { wal: u64 },

    /// Adds a new SSTable entry.
    AddSst { entry: ManifestSstEntry },

    /// Removes an SSTable by ID.
    RemoveSst { id: u64 },

    /// Updates the global last known LSN.
    UpdateLsn { last_lsn: u64 },
}

/// Serialized snapshot stored in `manifest.snapshot`.
///
/// Contains full manifest data and a checksum for corruption detection.
#[derive(Debug, bincode::Encode, bincode::Decode)]
struct ManifestSnapshot {
    /// Snapshot version number (matches manifest version).
    version: u64,

    /// The LSN at the time of snapshot creation.
    snapshot_lsn: u64,

    /// Full metadata (active WAL, frozen WALs, SSTables, etc.).
    manifest_data: ManifestData,

    /// CRC32 checksum of the entire serialized payload.
    checksum: u32,
}

// ------------------------------------------------------------------------------------------------
// Manifest core
// ------------------------------------------------------------------------------------------------

/// Persistent metadata manager of the LSM engine.
///
/// Provides crash-safe metadata operations using a WAL-driven model.
/// Allows concurrent updates.
///
/// # Durability rules
///
/// For every metadata mutation:
/// - Append a record to manifest WAL.
/// - Update in-memory state.
/// - Optionally, WAL may be fsync'ed (policy-dependent).
///
/// Checkpoint compacts state into a snapshot and truncates WAL.
#[derive(Debug)]
pub struct Manifest {
    /// Path to engine root directory.
    path: PathBuf,

    /// Manifest WAL storing metadata operations.
    ///
    /// The WAL ensures crash recovery consistency and is internally thread-safe.
    wal: Wal<ManifestEvent>,

    /// In-memory manifest state protected by a mutex.
    ///
    /// Concurrent threads update metadata safely using this lock.
    data: Mutex<ManifestData>,
}

impl Manifest {
    /// Opens the manifest from the given engine directory.
    ///
    /// # Behavior
    /// - Loads snapshot if present.
    /// - Replays manifest WAL to recover latest consistent state.
    /// - Initializes empty manifest if neither snapshot nor WAL exist.
    ///
    /// # Returns
    /// Loaded `Manifest` with fully reconstructed state.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;

        // 1. Load snapshot if present
        let snapshot_path = path.join(SNAPSHOT_FILENAME);
        let mut data = ManifestData::default();
        let mut snapshot_lsn: u64 = 0;

        if snapshot_path.exists() {
            match Self::read_snapshot(&snapshot_path) {
                Ok((snap, slsn)) => {
                    data = snap;
                    snapshot_lsn = slsn;
                    info!("Loaded manifest snapfrom from {:?}", snapshot_path);
                }
                Err(e) => {
                    warn!(
                        "Failed to read manifest snapshot {:?}: {}; falling back to WAL replay",
                        snapshot_path, e
                    );
                    // ignore snapshot and continue to replay WALs from start

                    return Err(e);
                }
            }
        }

        // 2. Open manifest WAL file (create if missing)
        let wal_path = path.join(WAL_FILENAME);
        let wal = Wal::<ManifestEvent>::open(&wal_path, None)?;

        // 3. Replay WAL entries (only those after snapshot_lsn if snapshot exists)
        //    The manifest WAL records are small; we iterate all records and apply.
        let mut manifest = Manifest {
            path,
            wal,
            data: Mutex::new(data),
        };

        manifest.replay_wal(snapshot_lsn)?;

        Ok(manifest)
    }

    /// Returns the active WAL segment ID.
    pub fn get_active_wal(&self) -> Result<u64, ManifestError> {
        let data = self.data.lock().map_err(|_| {
            error!("Mutex poisoned");
            ManifestError::Internal("Mutex poisoned".into())
        })?;

        Ok(data.active_wal)
    }

    /// Returns the frozen WAL segment list.
    pub fn get_frozen_wals(&self) -> Result<Vec<u64>, ManifestError> {
        let data = self.data.lock().map_err(|_| {
            error!("Mutex poisoned");
            ManifestError::Internal("Mutex poisoned".into())
        })?;

        Ok(data.frozen_wals.clone())
    }

    /// Returns list of SSTable entries.
    pub fn get_sstables(&self) -> Result<Vec<ManifestSstEntry>, ManifestError> {
        let data = self.data.lock().map_err(|_| {
            error!("Mutex poisoned");
            ManifestError::Internal("Mutex poisoned".into())
        })?;

        Ok(data.sstables.clone())
    }

    /// Returns the last persistent LSN.
    pub fn get_last_lsn(&self) -> Result<u64, ManifestError> {
        let data = self.data.lock().map_err(|_| {
            error!("Mutex poisoned");
            ManifestError::Internal("Mutex poisoned".into())
        })?;

        Ok(data.last_lsn)
    }

    /// Updates the active WAL segment.
    pub fn set_active_wal(&mut self, wal_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestEvent::SetActiveWal { wal: wal_id };
        self.wal.append(&rec)?; // durable via wal.append() which calls sync_all() in your WAL
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Adds a WAL segment to frozen list.
    pub fn add_frozen_wal(&mut self, wal_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestEvent::AddFrozenWal { wal: wal_id };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Removes a frozen WAL.
    pub fn remove_frozen_wal(&mut self, wal_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestEvent::RemoveFrozenWal { wal: wal_id };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Adds an SSTable entry to manifest.
    pub fn add_sstable(&mut self, entry: ManifestSstEntry) -> Result<(), ManifestError> {
        let rec = ManifestEvent::AddSst {
            entry: entry.clone(),
        };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Removes SSTable entry by ID.
    pub fn remove_sstable(&mut self, sst_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestEvent::RemoveSst { id: sst_id };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Updates last durable LSN.
    pub fn update_lsn(&mut self, last_lsn: u64) -> Result<(), ManifestError> {
        let rec = ManifestEvent::UpdateLsn { last_lsn };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Creates a manifest snapshot.
    ///
    /// # Behavior
    /// - Serializes ManifestData and writes it to `manifest.snapshot`.
    /// - Computes a checksum for corruption detection.
    /// - Resets/truncates manifest WAL to reduce recovery cost.
    ///
    /// # Safety
    /// Safe to call during concurrent metadata mutations.
    pub fn checkpoint(&mut self) -> Result<(), ManifestError> {
        // 1. Build snapshot structure (capture current state)
        let snapshot_no_csum = {
            let data = self
                .data
                .lock()
                .map_err(|e| ManifestError::Internal(format!("Mutex poisoned: {}", e)))?
                .clone();

            ManifestSnapshot {
                version: data.version,
                snapshot_lsn: data.last_lsn,
                manifest_data: data,
                checksum: 0,
            }
        };

        // 2. Serialize snapshot_no_csum and compute checksum over it
        let config = standard().with_fixed_int_encoding();
        let without_csum_bytes = encode_to_vec(&snapshot_no_csum, config)?;

        let mut hasher = Crc32::new();
        hasher.update(&without_csum_bytes);
        let checksum = hasher.finalize();

        // 3. Build final snapshot object (with checksum) and serialize
        let final_snapshot = ManifestSnapshot {
            version: snapshot_no_csum.version,
            snapshot_lsn: snapshot_no_csum.snapshot_lsn,
            manifest_data: snapshot_no_csum.manifest_data.clone(),
            checksum,
        };

        let final_bytes = encode_to_vec(&final_snapshot, config)?;

        // 4. Write to temp file
        let tmp_name = format!("{}{}", SNAPSHOT_FILENAME, SNAPSHOT_TMP_SUFFIX);
        let tmp_path = self.path.join(&tmp_name);
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            f.write_all(&final_bytes)?;
            f.sync_all()?; // ensure snapshot content durable
        }

        // 5. Atomic rename
        let final_path = self.path.join(SNAPSHOT_FILENAME);
        fs::rename(&tmp_path, &final_path)?;

        // 6. fsync parent directory so rename is durable
        Self::fsync_dir(&self.path)?;

        info!("Manifest snapshot written to {:?}", final_path);

        // 7. Truncate manifest WAL to header-only (safe after snapshot durability)
        self.wal.truncate()?; // resets WAL

        // 8. Mark in-memory data as clean
        {
            let mut data = self
                .data
                .lock()
                .map_err(|e| ManifestError::Internal(format!("Mutex poisoned: {}", e)))?;
            data.dirty = false;
        }

        Ok(())
    }

    fn fsync_dir(dir: &Path) -> Result<(), ManifestError> {
        let dir_file = File::open(dir)?;
        dir_file.sync_all()?;
        Ok(())
    }

    // TODO: refactor logic!
    fn read_snapshot(p: &Path) -> Result<(ManifestData, u64), ManifestError> {
        let mut f = File::open(p)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        let config = standard().with_fixed_int_encoding();
        let (mut snap, _) = decode_from_slice::<ManifestSnapshot, _>(buf.as_slice(), config)?;

        let stored_checksum = snap.checksum;
        snap.checksum = 0;

        let snapshot_bytes = encode_to_vec(&snap, config)?;

        let mut hasher = Crc32::new();
        hasher.update(snapshot_bytes.as_slice());
        let computed_checksum = hasher.finalize();

        if stored_checksum != computed_checksum {
            return Err(ManifestError::SnapshotChecksumMismatch);
        }

        Ok((snap.manifest_data, snap.snapshot_lsn))
    }

    fn replay_wal(&mut self, _snapshot_lsn: u64) -> Result<(), ManifestError> {
        let iter = match self.wal.replay_iter() {
            Ok(i) => i,
            Err(e) => {
                return Err(ManifestError::WAL(e));
            }
        };

        for item in iter {
            match item {
                Ok(rec) => {
                    self.apply_record(&rec)?;
                }
                Err(e) => {
                    // stop at first corruption / unexpected EOF as recommended earlier
                    warn!("Manifest WAL replay stopped due to WAL error: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    fn apply_record(&mut self, rec: &ManifestEvent) -> Result<(), ManifestError> {
        let mut data = self.data.lock().map_err(|_| {
            error!("Mutex poisoned");
            ManifestError::Internal("Mutex poisoned".into())
        })?;

        match rec {
            ManifestEvent::Version { version } => {
                data.version = *version;
            }

            ManifestEvent::SetActiveWal { wal } => {
                data.active_wal = *wal;
                data.frozen_wals.retain(|w| w != wal);
                data.dirty = true;
            }

            ManifestEvent::AddFrozenWal { wal } => {
                if !data.frozen_wals.contains(wal) {
                    data.frozen_wals.push(*wal);
                }
                data.dirty = true;
            }

            ManifestEvent::RemoveFrozenWal { wal } => {
                data.frozen_wals.retain(|w| w != wal);
                data.dirty = true;
            }

            ManifestEvent::AddSst { entry } => {
                // Avoid duplicate SST IDs (idempotent)
                if !data.sstables.iter().any(|e| e.id == entry.id) {
                    data.sstables.push(entry.clone());
                }
                data.dirty = true;
            }

            ManifestEvent::RemoveSst { id } => {
                data.sstables.retain(|e| e.id != *id);
                data.dirty = true;
            }

            ManifestEvent::UpdateLsn { last_lsn } => {
                if *last_lsn > data.last_lsn {
                    data.last_lsn = *last_lsn;
                }
                data.dirty = true;
            }
        }

        Ok(())
    }
}
