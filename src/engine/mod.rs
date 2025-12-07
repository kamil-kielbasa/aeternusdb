//! LSM Engine API (spec & pseudocode)
//!
//!

use std::path::Path;

use crossbeam;
use thiserror::Error;

use crate::manifest::Manifest;
use crate::memtable::{FrozenMemtable, Memtable, MemtableError};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Memtable error: {0}")]
    Memtable(#[from] MemtableError),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub struct EngineConfig {
    /// max memtable size (MB) before flush; threshold for oversized records.
    pub write_buffer_size: usize,

    /// Lower bound multiplier for bucket size range ([avg × bucket_low, avg × bucket_high])
    pub bucket_low: usize,

    /// Upper bound multiplier for bucket size range.
    pub bucket_high: usize,

    /// Min size (MB) for regular buckets; smaller go to "small" bucket.
    pub min_sstable_size: usize,

    /// Min SSTables in bucket for minor compaction.
    pub min_threshold: usize,

    /// Max SSTables per minor compaction.
    pub max_threshold: usize,

    /// Ratio of droppable tombstones to trigger tombstone compaction.
    pub tombstone_threshold: usize,

    /// Min SSTable age (seconds) for tombstone compaction.
    pub tombstone_compaction_interval: usize,

    /// Thread pool for flushing memtables and compcations.
    pub thread_pool_size: usize,
}

pub struct Engine {
    /// Persistent manifest for this engine (keeps track of SSTables, generations, etc).
    manifest: Manifest,

    /// Active memtable that accepts writes.
    active: Memtable,

    /// Frozen memtables waiting to be flushed to SSTable.
    /// We keep them in memory for reads until flush completes.
    frozen: crossbeam::queue::SegQueue<FrozenMemtable>,

    /// Path where engine will be mounted.
    data_dir: String,

    /// A short config for thresholds, sizes, etc.
    config: EngineConfig,
}

impl Engine {
    pub fn open(path: impl AsRef<Path>, config: EngineConfig) -> Result<Self, EngineError> {
        /// 1. Load or create manifest.
        //
        //    - Parse manifest file and rebuild list of SSTables and their levels.
        //    - Read last persisted LSN from manifest.
        //
        //    IMPORTANT:
        //    The manifest is authoritative for:
        //      - which SSTables exist
        //      - file UUIDs and levels
        //      - last_flushed_lsn or last_committed_lsn
        //
        //    You **cannot** rely on directory listing for SSTables' validity.

        // 2. Discover existing WAL files.
        //
        //    - Scan directory for wal-*.log.
        //    - Sort by increasing UUID (or timestamp).
        //    - Read each WAL header (optional but typical: contains start_lsn).
        //    - Peek into the tail of each WAL to determine last LSN recorded.
        //
        //    NOTE:
        //      WALs MUST be scanned **before** SSTables,
        //      because WALs determine how much of state must be replayed.

        // 3. Determine WAL roles.
        //
        //    - For each WAL:
        //        let wal_end_lsn = wal.compute_end_lsn().
        //
        //        if wal_end_lsn <= manifest.last_persisted_lsn:
        //            safe to delete: all updates are already flushed
        //            delete WAL file.
        //            continue;
        //
        //        // WAL contains unflushed data:
        //        keep this WAL.
        //
        //    - After filtering, sort remaining WALs:
        //        last WAL → active WAL → active memtable
        //        any others → frozen WALs → frozen memtables
        //
        //    NOTE:
        //      It's okay if you have zero frozen WALs most of the time.
        //      Frozen WALs appear only if engine crashed
        //      while there were multiple memtables awaiting flush.

        // 4. Load active memtable and frozen memtables.
        //
        //    - For each frozen WAL:
        //        create Memtable::from_wal() (full replay)
        //    - For active WAL:
        //        create active memtable and replay WAL
        //
        //    IMPORTANT:
        //      memtable ← WAL is NOT optional.
        //      Memtable must reflect exactly the contents of its WAL.

        // 5. Discover existing SSTable files.
        //
        //    - List *.sst in directory.
        //    - If SSTable exists on disk but not listed in manifest:
        //        delete it (orphan file).
        //
        //    NOTE:
        //      Manifest is ALWAYS authoritative.
        //      If SSTable is not in manifest → treat as garbage.

        // 6. Load list of SSTables from manifest.
        //
        //    - For each SSTable UUID in manifest:
        //        SSTable::open(uuid)
        //
        //    OPTIONAL BUT RECOMMENDED:
        //      Validate SSTable metadata header (start_lsn, end_lsn)
        //      to ensure it matches manifest.

        // 7. Compute global LSN.
        //
        //    global_lsn = max(
        //        manifest.last_lsn,
        //        max_lsn_in_active_memtable,
        //        max_lsn_in_frozen_memtables,
        //        max_lsn_in_sstable_headers (optional, but correct)
        //    )
        //
        //    next_lsn = global_lsn + 1
        //
        //    Set engine.next_lsn = AtomicU64(next_lsn).
        //
        //    NOTE:
        //      next_lsn MUST be greater than everything that already exists
        //      in WALs or SSTables.
        //
        //      LSN must be strictly monotonic across engine restarts.

        // 8. Create thread pool for flushes and compaction.
        //
        //    - flush workers: apply memtable → WAL → SSTable
        //    - compaction workers: merge SSTables according to LSM policy
        unimplemented!()
    }

    pub fn close(&self) -> Result<(), EngineError> {
        // PSEUDOCODE:
        // 1. Gracefully shutdown:
        //    - wait for flushes
        //    - fsync manifest
        //    - close WAL's
        unimplemented!()
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), EngineError> {
        unimplemented!()
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<(), EngineError> {
        unimplemented!()
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, EngineError> {
        unimplemented!()
    }

    pub fn scan(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<EngineScanIterator, EngineError> {
        unimplemented!()
    }

    fn trigger_flush(&self) -> Result<(), EngineError> {
        unimplemented!()
    }

    fn trigger_compcation(&self) -> Result<(), EngineError> {
        unimplemented!()
    }
}

pub struct EngineScanIterator {}
