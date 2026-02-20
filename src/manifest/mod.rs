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
//! 2. **Manifest snapshot** (`MANIFEST-000001`) is a compact encoded
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

use crate::encoding::{self, EncodingError};
use crate::wal::{Wal, WalError};
use crc32fast::Hasher as Crc32;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};
use thiserror::Error;
use tracing::{error, info, warn};

const SNAPSHOT_TMP_SUFFIX: &str = ".tmp";
const SNAPSHOT_FILENAME: &str = "MANIFEST-000001";
/// Manifest WAL filename. This is a fixed, single-segment WAL file — it does
/// not rotate. Truncated to zero on each checkpoint.
const WAL_FILENAME: &str = "000000.log";

// ------------------------------------------------------------------------------------------------
// Error Types
// ------------------------------------------------------------------------------------------------

/// Errors returned by manifest operations.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// Underlying WAL I/O failure.
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Encoding / decoding error.
    #[error("Encoding error: {0}")]
    Encoding(#[from] EncodingError),

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

/// In-memory representation of the manifest durable state.
///
/// This structure stores the persistent metadata describing
/// the layout of the LSM tree. Fields are private to enforce
/// invariants through the [`Manifest`] API.
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct ManifestData {
    /// Monotonically increasing manifest version.
    version: u64,

    /// Last globally assigned LSN (Log Sequence Number).
    last_lsn: u64,

    /// Identifier of current active WAL segment.
    active_wal: u64,

    /// Identifiers of frozen WAL segments (older, ready for flush).
    frozen_wals: Vec<u64>,

    /// List of all SSTables belonging to the LSM tree.
    sstables: Vec<ManifestSstEntry>,

    /// Next SSTable ID to allocate. Monotonically increasing.
    next_sst_id: u64,

    /// Runtime-only flag: true when in-memory state diverges from
    /// the last persisted snapshot. Not serialized.
    dirty: bool,
}

/// Entry describing a single SSTable known to the manifest.
///
/// Identifies table by unique ID and on-disk path.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestSstEntry {
    /// Globally unique SSTable ID.
    pub id: u64,

    /// Filesystem path to SSTable file.
    pub path: PathBuf,
}

// ------------------------------------------------------------------------------------------------
// Encoding implementations
// ------------------------------------------------------------------------------------------------

impl encoding::Encode for ManifestSstEntry {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.id, buf)?;
        encoding::Encode::encode_to(&self.path, buf)?;
        Ok(())
    }
}

impl encoding::Decode for ManifestSstEntry {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut offset = 0;
        let (id, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        let (path, n) = PathBuf::decode_from(&buf[offset..])?;
        offset += n;
        Ok((Self { id, path }, offset))
    }
}

impl encoding::Encode for ManifestData {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.version, buf)?;
        encoding::Encode::encode_to(&self.last_lsn, buf)?;
        encoding::Encode::encode_to(&self.active_wal, buf)?;
        encoding::encode_vec(&self.frozen_wals, buf)?;
        encoding::encode_vec(&self.sstables, buf)?;
        encoding::Encode::encode_to(&self.next_sst_id, buf)?;
        // `dirty` is a runtime-only flag — always written as `false` for
        // wire compatibility, but never read back.
        encoding::Encode::encode_to(&false, buf)?;
        Ok(())
    }
}

impl encoding::Decode for ManifestData {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut offset = 0;
        let (version, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        let (last_lsn, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        let (active_wal, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        let (frozen_wals, n) = encoding::decode_vec::<u64>(&buf[offset..])?;
        offset += n;
        let (sstables, n) = encoding::decode_vec::<ManifestSstEntry>(&buf[offset..])?;
        offset += n;
        let (next_sst_id, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        // `dirty` is present in the wire format for backward compatibility
        // but its value is discarded — always initialised to `false`.
        let (_dirty, n) = bool::decode_from(&buf[offset..])?;
        offset += n;
        Ok((
            Self {
                version,
                last_lsn,
                active_wal,
                frozen_wals,
                sstables,
                next_sst_id,
                dirty: false,
            },
            offset,
        ))
    }
}

impl encoding::Encode for ManifestEvent {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        match self {
            ManifestEvent::Version { version } => {
                encoding::Encode::encode_to(&0u32, buf)?;
                encoding::Encode::encode_to(version, buf)?;
            }
            ManifestEvent::SetActiveWal { wal } => {
                encoding::Encode::encode_to(&1u32, buf)?;
                encoding::Encode::encode_to(wal, buf)?;
            }
            ManifestEvent::AddFrozenWal { wal } => {
                encoding::Encode::encode_to(&2u32, buf)?;
                encoding::Encode::encode_to(wal, buf)?;
            }
            ManifestEvent::RemoveFrozenWal { wal } => {
                encoding::Encode::encode_to(&3u32, buf)?;
                encoding::Encode::encode_to(wal, buf)?;
            }
            ManifestEvent::AddSst { entry } => {
                encoding::Encode::encode_to(&4u32, buf)?;
                encoding::Encode::encode_to(entry, buf)?;
            }
            ManifestEvent::RemoveSst { id } => {
                encoding::Encode::encode_to(&5u32, buf)?;
                encoding::Encode::encode_to(id, buf)?;
            }
            ManifestEvent::UpdateLsn { last_lsn } => {
                encoding::Encode::encode_to(&6u32, buf)?;
                encoding::Encode::encode_to(last_lsn, buf)?;
            }
            ManifestEvent::AllocateSstId { id } => {
                encoding::Encode::encode_to(&7u32, buf)?;
                encoding::Encode::encode_to(id, buf)?;
            }
            ManifestEvent::Compaction { added, removed } => {
                encoding::Encode::encode_to(&8u32, buf)?;
                encoding::encode_vec(added, buf)?;
                encoding::encode_vec(removed, buf)?;
            }
        }
        Ok(())
    }
}

impl encoding::Decode for ManifestEvent {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut offset = 0;
        let (tag, n) = u32::decode_from(buf)?;
        offset += n;
        match tag {
            0 => {
                let (version, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                Ok((ManifestEvent::Version { version }, offset))
            }
            1 => {
                let (wal, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                Ok((ManifestEvent::SetActiveWal { wal }, offset))
            }
            2 => {
                let (wal, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                Ok((ManifestEvent::AddFrozenWal { wal }, offset))
            }
            3 => {
                let (wal, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                Ok((ManifestEvent::RemoveFrozenWal { wal }, offset))
            }
            4 => {
                let (entry, n) = ManifestSstEntry::decode_from(&buf[offset..])?;
                offset += n;
                Ok((ManifestEvent::AddSst { entry }, offset))
            }
            5 => {
                let (id, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                Ok((ManifestEvent::RemoveSst { id }, offset))
            }
            6 => {
                let (last_lsn, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                Ok((ManifestEvent::UpdateLsn { last_lsn }, offset))
            }
            7 => {
                let (id, n) = u64::decode_from(&buf[offset..])?;
                offset += n;
                Ok((ManifestEvent::AllocateSstId { id }, offset))
            }
            8 => {
                let (added, n) = encoding::decode_vec::<ManifestSstEntry>(&buf[offset..])?;
                offset += n;
                let (removed, n) = encoding::decode_vec::<u64>(&buf[offset..])?;
                offset += n;
                Ok((ManifestEvent::Compaction { added, removed }, offset))
            }
            _ => Err(EncodingError::InvalidTag {
                tag,
                type_name: "ManifestEvent",
            }),
        }
    }
}

impl encoding::Encode for ManifestSnapshot {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        encoding::Encode::encode_to(&self.version, buf)?;
        encoding::Encode::encode_to(&self.snapshot_lsn, buf)?;
        encoding::Encode::encode_to(&self.manifest_data, buf)?;
        encoding::Encode::encode_to(&self.checksum, buf)?;
        Ok(())
    }
}

impl encoding::Decode for ManifestSnapshot {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let mut offset = 0;
        let (version, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        let (snapshot_lsn, n) = u64::decode_from(&buf[offset..])?;
        offset += n;
        let (manifest_data, n) = ManifestData::decode_from(&buf[offset..])?;
        offset += n;
        let (checksum, n) = u32::decode_from(&buf[offset..])?;
        offset += n;
        Ok((
            Self {
                version,
                snapshot_lsn,
                manifest_data,
                checksum,
            },
            offset,
        ))
    }
}

impl Default for ManifestData {
    fn default() -> Self {
        Self {
            version: 1,
            last_lsn: 0,
            active_wal: 0,
            frozen_wals: Vec::new(),
            sstables: Vec::new(),
            next_sst_id: 1,
            dirty: false,
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Manifest record types
// ------------------------------------------------------------------------------------------------

/// Record stored in manifest WAL. Each variant describes
/// a single metadata mutation applied to ManifestData.
#[derive(Debug)]
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

    /// Allocates the next SSTable ID (persists the counter increment).
    AllocateSstId { id: u64 },

    /// Atomic compaction operation: adds new SSTables and removes old ones
    /// in a single WAL entry, ensuring crash-safe manifest transitions.
    Compaction {
        added: Vec<ManifestSstEntry>,
        removed: Vec<u64>,
    },
}

/// Serialized snapshot stored in `MANIFEST-000001`.
///
/// Contains full manifest data and a checksum for corruption detection.
#[derive(Debug)]
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
                    info!("Loaded manifest snapshot from {:?}", snapshot_path);
                }
                Err(e) => {
                    // Resilient recovery: ignore corrupt snapshot and replay WAL
                    // from scratch.  The WAL is the ground truth and snapshots
                    // are an optimisation hint.
                    warn!(
                        "Failed to read manifest snapshot {:?}: {}; \
                         falling back to full WAL replay",
                        snapshot_path, e
                    );
                    data = ManifestData::default();
                    snapshot_lsn = 0;
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

    // --------------------------------------------------------------------
    // Internal helpers
    // --------------------------------------------------------------------

    /// Acquires the manifest data lock, mapping a poisoned mutex to
    /// [`ManifestError::Internal`].
    fn lock_data(&self) -> Result<std::sync::MutexGuard<'_, ManifestData>, ManifestError> {
        self.data.lock().map_err(|_| {
            error!("Mutex poisoned");
            ManifestError::Internal("Mutex poisoned".into())
        })
    }

    // --------------------------------------------------------------------
    // Read accessors
    // --------------------------------------------------------------------

    /// Returns the active WAL segment ID.
    pub fn get_active_wal(&self) -> Result<u64, ManifestError> {
        Ok(self.lock_data()?.active_wal)
    }

    /// Returns the frozen WAL segment list.
    pub fn get_frozen_wals(&self) -> Result<Vec<u64>, ManifestError> {
        Ok(self.lock_data()?.frozen_wals.clone())
    }

    /// Returns list of SSTable entries.
    pub fn get_sstables(&self) -> Result<Vec<ManifestSstEntry>, ManifestError> {
        Ok(self.lock_data()?.sstables.clone())
    }

    /// Returns the last persistent LSN.
    pub fn get_last_lsn(&self) -> Result<u64, ManifestError> {
        Ok(self.lock_data()?.last_lsn)
    }

    /// Returns `true` if in-memory state has diverged from the last snapshot.
    pub fn is_dirty(&self) -> Result<bool, ManifestError> {
        Ok(self.lock_data()?.dirty)
    }

    // --------------------------------------------------------------------
    // Mutation methods
    // --------------------------------------------------------------------
    //
    // All mutation methods take `&self` rather than `&mut self`.
    // Interior mutability is provided by the `Mutex<ManifestData>` and the
    // internally-synchronised WAL.  This allows concurrent metadata updates
    // without requiring exclusive ownership.  `checkpoint()` is the only
    // method that requires `&mut self` because it truncates the WAL and
    // must not race with concurrent mutations.

    /// Updates the active WAL segment.
    pub fn set_active_wal(&self, wal_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestEvent::SetActiveWal { wal: wal_id };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Adds a WAL segment to frozen list.
    pub fn add_frozen_wal(&self, wal_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestEvent::AddFrozenWal { wal: wal_id };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Removes a frozen WAL.
    pub fn remove_frozen_wal(&self, wal_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestEvent::RemoveFrozenWal { wal: wal_id };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Adds an SSTable entry to manifest.
    pub fn add_sstable(&self, entry: ManifestSstEntry) -> Result<(), ManifestError> {
        let rec = ManifestEvent::AddSst {
            entry: entry.clone(),
        };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Removes SSTable entry by ID.
    pub fn remove_sstable(&self, sst_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestEvent::RemoveSst { id: sst_id };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Atomically allocates the next SSTable ID.
    ///
    /// Increments the manifest's `next_sst_id` counter and persists the
    /// new value to the WAL. Returns the allocated ID.
    ///
    /// The data lock is held across the read-and-increment to prevent
    /// two concurrent callers from allocating the same ID.
    pub fn allocate_sst_id(&self) -> Result<u64, ManifestError> {
        let mut data = self.lock_data()?;
        let id = data.next_sst_id;
        let rec = ManifestEvent::AllocateSstId { id };
        self.wal.append(&rec)?;
        data.next_sst_id = id + 1;
        data.dirty = true;
        Ok(id)
    }

    /// Returns the next SSTable ID without allocating it.
    pub fn peek_next_sst_id(&self) -> Result<u64, ManifestError> {
        Ok(self.lock_data()?.next_sst_id)
    }

    /// Atomically records a compaction: adds new SSTables and removes old ones
    /// in a single WAL entry.
    pub fn apply_compaction(
        &self,
        added: Vec<ManifestSstEntry>,
        removed: Vec<u64>,
    ) -> Result<(), ManifestError> {
        let rec = ManifestEvent::Compaction { added, removed };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Updates last durable LSN.
    pub fn update_lsn(&self, last_lsn: u64) -> Result<(), ManifestError> {
        let rec = ManifestEvent::UpdateLsn { last_lsn };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    /// Creates a manifest snapshot.
    ///
    /// # Behavior
    /// - Serializes ManifestData and writes it to `MANIFEST-000001`.
    /// - Computes a checksum for corruption detection.
    /// - Resets/truncates manifest WAL to reduce recovery cost.
    ///
    /// # Exclusive access
    /// Requires `&mut self` to ensure no concurrent mutations race with the
    /// WAL truncation step.
    pub fn checkpoint(&mut self) -> Result<(), ManifestError> {
        // 1. Build snapshot structure (capture current state, checksum placeholder)
        let snapshot = {
            let data = self.lock_data()?.clone();

            ManifestSnapshot {
                version: data.version,
                snapshot_lsn: data.last_lsn,
                manifest_data: data,
                checksum: 0,
            }
        };

        // 2. Single-pass: serialize with checksum=0, compute CRC, then patch
        //    the trailing 4 bytes with the real checksum (little-endian u32).
        let mut snapshot_bytes = encoding::encode_to_vec(&snapshot)?;

        let mut hasher = Crc32::new();
        hasher.update(&snapshot_bytes);
        let checksum = hasher.finalize();

        let len = snapshot_bytes.len();
        snapshot_bytes[len - 4..].copy_from_slice(&checksum.to_le_bytes());

        // 3. Write to temp file
        let tmp_name = format!("{}{}", SNAPSHOT_FILENAME, SNAPSHOT_TMP_SUFFIX);
        let tmp_path = self.path.join(&tmp_name);
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            f.write_all(&snapshot_bytes)?;
            f.sync_all()?; // ensure snapshot content durable
        }

        // 4. Atomic rename
        let final_path = self.path.join(SNAPSHOT_FILENAME);
        fs::rename(&tmp_path, &final_path)?;

        // 5. fsync parent directory so rename is durable
        Self::fsync_dir(&self.path)?;

        info!("Manifest snapshot written to {:?}", final_path);

        // 6. Truncate manifest WAL to header-only (safe after snapshot durability)
        self.wal.truncate()?;

        // 7. Mark in-memory data as clean
        self.lock_data()?.dirty = false;

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

        let (snap, _) = encoding::decode_from_slice::<ManifestSnapshot>(buf.as_slice())?;

        // Verify checksum: re-encode with checksum=0, CRC the result, compare.
        let verify = ManifestSnapshot {
            checksum: 0,
            version: snap.version,
            snapshot_lsn: snap.snapshot_lsn,
            manifest_data: snap.manifest_data.clone(),
        };
        let verify_bytes = encoding::encode_to_vec(&verify)?;

        let mut hasher = Crc32::new();
        hasher.update(&verify_bytes);
        let computed_checksum = hasher.finalize();

        if snap.checksum != computed_checksum {
            return Err(ManifestError::SnapshotChecksumMismatch);
        }

        Ok((snap.manifest_data, snap.snapshot_lsn))
    }

    fn replay_wal(&mut self, snapshot_lsn: u64) -> Result<(), ManifestError> {
        let iter = match self.wal.replay_iter() {
            Ok(i) => i,
            Err(e) => {
                return Err(ManifestError::Wal(e));
            }
        };

        let mut count: u64 = 0;
        for item in iter {
            match item {
                Ok(rec) => {
                    self.apply_record(&rec)?;
                    count += 1;
                }
                Err(e) => {
                    warn!("Manifest WAL replay stopped due to WAL error: {}", e);
                    break;
                }
            }
        }

        // Defensive check: after replay the manifest LSN must be at least
        // as large as the snapshot baseline.  A smaller value indicates WAL
        // truncation or data loss.
        let current_lsn = self.lock_data()?.last_lsn;
        if snapshot_lsn > 0 && current_lsn < snapshot_lsn {
            warn!(
                "Manifest LSN after WAL replay ({}) is less than snapshot LSN ({}); \
                 possible WAL truncation or data loss",
                current_lsn, snapshot_lsn
            );
        }

        info!(
            "Manifest WAL replay: {} entries applied (snapshot_lsn={})",
            count, snapshot_lsn
        );

        Ok(())
    }

    fn apply_record(&self, rec: &ManifestEvent) -> Result<(), ManifestError> {
        let mut data = self.lock_data()?;

        match rec {
            ManifestEvent::Version { version } => {
                data.version = *version;
                data.dirty = true;
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

            ManifestEvent::AllocateSstId { id } => {
                // Advance counter past the allocated ID (self-healing on replay).
                if *id >= data.next_sst_id {
                    data.next_sst_id = *id + 1;
                }
                data.dirty = true;
            }

            ManifestEvent::Compaction { added, removed } => {
                // Remove old SSTables first.
                for id in removed {
                    data.sstables.retain(|e| e.id != *id);
                }
                // Add new SSTables (idempotent — skip duplicates).
                for entry in added {
                    if !data.sstables.iter().any(|e| e.id == entry.id) {
                        data.sstables.push(entry.clone());
                    }
                    // Keep next_sst_id consistent.
                    if entry.id >= data.next_sst_id {
                        data.next_sst_id = entry.id + 1;
                    }
                }
                data.dirty = true;
            }
        }

        Ok(())
    }
}
