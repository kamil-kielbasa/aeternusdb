//! Manifest component

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
const SNAPSHOT_FILENAME: &str = "MANIFEST.snapshot";
const WAL_FILENAME: &str = "MANIFEST.wal";
const U32_SIZE: usize = std::mem::size_of::<u32>();

// ----------------------------- Errors -------------------------------------

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

    #[error("Snapshot checksum mismatch")]
    SnapshotChecksumMismatch,

    /// Internal invariant violation or poisoned lock.
    #[error("Internal error: {0}")]
    Internal(String),
}

// --------------------------- Manifest Data --------------------------------

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct ManifestSstEntry {
    pub id: u64,
    pub path: PathBuf,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct ManifestData {
    pub version: u64,
    pub last_lsn: u64,

    /// active WAL id (engine-defined numeric id)
    pub active_wal: u64,

    /// frozen WAL ids waiting to be flushed (older WALs)
    pub frozen_wals: Vec<u64>,

    /// List of sstables known to engine
    pub sstables: Vec<ManifestSstEntry>,

    /// Whether there are uncheckpointed WAL records
    pub dirty: bool,
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

// ------------------------- Manifest Record types ---------------------------

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub enum ManifestRecord {
    Version { version: u64 },

    SetActiveWal { wal: u64 },
    AddFrozenWal { wal: u64 },
    RemoveFrozenWal { wal: u64 },

    AddSst { entry: ManifestSstEntry },
    RemoveSst { id: u64 },

    UpdateLsn { last_lsn: u64 },
}

#[derive(Debug, bincode::Encode, bincode::Decode)]
struct ManifestSnapshot {
    version: u64,
    snapshot_lsn: u64,
    manifest_data: ManifestData,
    checksum: u32,
}

// --------------------------- Manifest struct --------------------------------

pub struct Manifest {
    path: PathBuf,
    wal: Wal<ManifestRecord>,
    data: Mutex<ManifestData>,
}

impl Manifest {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;

        // 1) Load snapshot if present
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
                }
            }
        }

        // 2) Open manifest WAL file (create if missing)
        let wal_path = path.join(WAL_FILENAME);
        let wal = Wal::<ManifestRecord>::open(&wal_path, None)?;

        // 3) Replay WAL entries (only those after snapshot_lsn if snapshot exists)
        //    The manifest WAL records are small; we iterate all records and apply.
        let mut manifest = Manifest {
            path,
            wal,
            data: Mutex::new(data),
        };

        manifest.replay_wal(snapshot_lsn)?;

        Ok(manifest)
    }

    // -------------------- getters --------------------

    pub fn get_active_wal(&self) -> Result<u64, ManifestError> {
        let data = self.data.lock().map_err(|_| {
            error!("Read-write lock poisoned during put");
            ManifestError::Internal("Read-write lock poisoned".into())
        })?;

        Ok(data.active_wal)
    }

    pub fn get_frozen_wals(&self) -> Result<Vec<u64>, ManifestError> {
        let data = self.data.lock().map_err(|_| {
            error!("Read-write lock poisoned during put");
            ManifestError::Internal("Read-write lock poisoned".into())
        })?;

        Ok(data.frozen_wals.clone())
    }

    pub fn get_sstables(&self) -> Result<Vec<ManifestSstEntry>, ManifestError> {
        let data = self.data.lock().map_err(|_| {
            error!("Read-write lock poisoned during put");
            ManifestError::Internal("Read-write lock poisoned".into())
        })?;

        Ok(data.sstables.clone())
    }

    pub fn get_last_lsn(&self) -> Result<u64, ManifestError> {
        let data = self.data.lock().map_err(|_| {
            error!("Read-write lock poisoned during put");
            ManifestError::Internal("Read-write lock poisoned".into())
        })?;

        Ok(data.last_lsn)
    }

    // -------------------- mutating operations --------------------

    pub fn set_active_wal(&mut self, wal_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestRecord::SetActiveWal { wal: wal_id };
        self.wal.append(&rec)?; // durable via wal.append() which calls sync_all() in your WAL
        self.apply_record(&rec)?;
        Ok(())
    }

    pub fn add_frozen_wal(&mut self, wal_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestRecord::AddFrozenWal { wal: wal_id };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    pub fn remove_frozen_wal(&mut self, wal_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestRecord::RemoveFrozenWal { wal: wal_id };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    pub fn add_sstable(&mut self, entry: ManifestSstEntry) -> Result<(), ManifestError> {
        let rec = ManifestRecord::AddSst {
            entry: entry.clone(),
        };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    pub fn remove_sstable(&mut self, sst_id: u64) -> Result<(), ManifestError> {
        let rec = ManifestRecord::RemoveSst { id: sst_id };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    pub fn update_lsn(&mut self, last_lsn: u64) -> Result<(), ManifestError> {
        let rec = ManifestRecord::UpdateLsn { last_lsn };
        self.wal.append(&rec)?;
        self.apply_record(&rec)?;
        Ok(())
    }

    // -------------------- checkpoint --------------------

    // Create a durable manifest snapshot and truncate the manifest WAL.
    //
    // Steps:
    // 1. Serialize in-memory ManifestData into a temp file (path/MANIFEST.snapshot.tmp.<pid>)
    // 2. fsync the temp file
    // 3. atomically rename tmp -> MANIFEST.snapshot
    // 4. fsync parent directory
    // 5. truncate the manifest WAL (reset to header-only)
    pub fn checkpoint(&mut self) -> Result<(), ManifestError> {
        // 1) Build snapshot structure (capture current state)
        let snapshot_no_csum = {
            let data = self
                .data
                .lock()
                .map_err(|e| ManifestError::Internal(format!("data mutex poisoned: {}", e)))?
                .clone();

            ManifestSnapshot {
                version: data.version,
                snapshot_lsn: data.last_lsn,
                manifest_data: data,
                checksum: 0,
            }
        };

        // 2) Serialize snapshot_no_csum and compute checksum over it
        let config = standard().with_fixed_int_encoding();
        let without_csum_bytes = encode_to_vec(&snapshot_no_csum, config)?;

        let mut hasher = Crc32::new();
        hasher.update(&without_csum_bytes);
        let checksum = hasher.finalize();

        // 3) Build final snapshot object (with checksum) and serialize
        let final_snapshot = ManifestSnapshot {
            version: snapshot_no_csum.version,
            snapshot_lsn: snapshot_no_csum.snapshot_lsn,
            manifest_data: snapshot_no_csum.manifest_data.clone(),
            checksum,
        };

        let final_bytes = encode_to_vec(&final_snapshot, config)?;

        // 4) Write to temp file
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

        // 5) Atomic rename
        let final_path = self.path.join(SNAPSHOT_FILENAME);
        fs::rename(&tmp_path, &final_path)?;

        // 6) fsync parent directory so rename is durable
        Self::fsync_dir(&self.path)?;

        info!("Manifest snapshot written to {:?}", final_path);

        // 7) Truncate manifest WAL to header-only (safe after snapshot durability)
        self.wal.truncate()?; // resets WAL

        // 8) Mark in-memory data as clean
        {
            let mut data = self
                .data
                .lock()
                .map_err(|e| ManifestError::Internal(format!("data mutex poisoned: {}", e)))?;
            data.dirty = false;
        }

        Ok(())
    }

    fn fsync_dir(dir: &Path) -> Result<(), ManifestError> {
        // On unix, opening a directory and calling sync_all ensures rename is durable.
        // On Windows, behavior differs; for cross-platform you may need alternative APIs.
        let dir_file = File::open(dir)?;
        dir_file.sync_all()?;
        Ok(())
    }

    fn read_snapshot(p: &Path) -> Result<(ManifestData, u64), ManifestError> {
        let mut f = File::open(p)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        if buf.len() < U32_SIZE {
            return Err(ManifestError::SnapshotChecksumMismatch);
        }

        let payload_len = buf.len() - U32_SIZE;
        let (snapshot_bytes, checksum_bytes) = buf.split_at(payload_len);
        let stored_checksum = u32::from_le_bytes([
            checksum_bytes[0],
            checksum_bytes[1],
            checksum_bytes[2],
            checksum_bytes[3],
        ]);

        let mut hasher = Crc32::new();
        hasher.update(snapshot_bytes);
        let computed = hasher.finalize();
        if computed != stored_checksum {
            return Err(ManifestError::SnapshotChecksumMismatch);
        }

        let config = standard().with_fixed_int_encoding();
        let (snap, _) = decode_from_slice::<ManifestSnapshot, _>(snapshot_bytes, config)?;

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

    fn apply_record(&mut self, rec: &ManifestRecord) -> Result<(), ManifestError> {
        let mut data = self.data.lock().map_err(|_| {
            error!("Read-write lock poisoned during put");
            ManifestError::Internal("Read-write lock poisoned".into())
        })?;

        match rec {
            ManifestRecord::Version { version } => {
                data.version = *version;
            }

            ManifestRecord::SetActiveWal { wal } => {
                data.active_wal = *wal;
                data.frozen_wals.retain(|w| w != wal);
                data.dirty = true;
            }

            ManifestRecord::AddFrozenWal { wal } => {
                if !data.frozen_wals.contains(wal) {
                    data.frozen_wals.push(*wal);
                }
                data.dirty = true;
            }

            ManifestRecord::RemoveFrozenWal { wal } => {
                data.frozen_wals.retain(|w| w != wal);
                data.dirty = true;
            }

            ManifestRecord::AddSst { entry } => {
                // Avoid duplicate SST IDs (idempotent)
                if !data.sstables.iter().any(|e| e.id == entry.id) {
                    data.sstables.push(entry.clone());
                }
                data.dirty = true;
            }

            ManifestRecord::RemoveSst { id } => {
                data.sstables.retain(|e| e.id != *id);
                data.dirty = true;
            }

            ManifestRecord::UpdateLsn { last_lsn } => {
                if *last_lsn > data.last_lsn {
                    data.last_lsn = *last_lsn;
                }
                data.dirty = true;
            }
        }

        Ok(())
    }
}
