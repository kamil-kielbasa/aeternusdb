//! # LSM Storage Engine
//!
//! This module implements a **synchronous**, **crash-safe** LSM-tree storage engine
//! with multi-version concurrency, point and range tombstones, and pluggable
//! compaction strategies.
//!
//! ## Design Overview
//!
//! The engine organises data across three layers, queried newest-first:
//!
//! 1. **Active memtable** — an in-memory sorted map backed by a write-ahead log (WAL).
//! 2. **Frozen memtables** — read-only snapshots of previously active memtables,
//!    awaiting flush to persistent SSTables.
//! 3. **SSTables** — immutable, sorted, on-disk files with bloom filters and block
//!    indices for efficient point lookups and range scans.
//!
//! Writes go through the WAL first, then into the active memtable. When the
//! memtable exceeds [`EngineConfig::write_buffer_size`] it is frozen and a
//! fresh memtable + WAL is created. Frozen memtables are flushed to SSTables
//! via [`Engine::flush_oldest_frozen`] / [`Engine::flush_all_frozen`].
//!
//! ## Concurrency Model
//!
//! All engine state is protected by a single `Arc<RwLock<EngineInner>>`.
//! Reads acquire a **read lock**; writes and flushes acquire a **write lock**.
//! Compaction first acquires a short read lock to obtain the strategy, then
//! acquires a write lock for the merge/swap phase.
//!
//! ## Compaction
//!
//! Three compaction operations are exposed:
//!
//! - [`Engine::minor_compact`] — merges similarly-sized SSTables within a
//!   bucket, deduplicating point entries while preserving tombstones.
//! - [`Engine::tombstone_compact`] — rewrites a single high-tombstone-ratio
//!   SSTable, dropping provably-unnecessary tombstones.
//! - [`Engine::major_compact`] — merges *all* SSTables into one, actively
//!   applying range tombstones and dropping all spent tombstones.
//!
//! The concrete strategy implementations are selected via
//! [`EngineConfig::compaction_strategy`].
//!
//! ## Guarantees
//!
//! - **Durability:** Every write is persisted to WAL before acknowledgement.
//! - **Crash recovery:** On [`Engine::open`], the manifest, WALs, and SSTables
//!   are replayed to reconstruct the last durable state.
//! - **Multi-version reads:** Point lookups and scans always see the latest
//!   committed version of each key, respecting tombstones.
//! - **Atomic flushes:** Each frozen memtable is flushed to a single SSTable
//!   and the manifest is updated atomically.

use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};

use thiserror::Error;

use crate::manifest::{Manifest, ManifestError, ManifestSstEntry};
use crate::memtable::{FrozenMemtable, Memtable, MemtableError, MemtableGetResult};
use crate::sstable::{self, SSTable, SSTableError};

pub mod utils;
pub use utils::{PointEntry, RangeTombstone, Record};

#[cfg(test)]
mod tests;

pub const MANIFEST_DIR: &str = "manifest";
pub const MEMTABLE_DIR: &str = "memtables";
pub const SSTABLE_DIR: &str = "sstables";

/// Errors that can occur during engine operations.
#[derive(Debug, Error)]
pub enum EngineError {
    /// Error originating from the manifest subsystem.
    #[error("Manifest error: {0}")]
    Manifest(#[from] ManifestError),

    /// Error originating from the memtable subsystem.
    #[error("Memtable error: {0}")]
    Memtable(#[from] MemtableError),

    /// Error originating from the SSTable subsystem.
    #[error("SSTable error: {0}")]
    SSTable(#[from] SSTableError),

    /// Underlying filesystem I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Internal invariant violation (poisoned lock, unexpected state, etc.).
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Configuration for an [`Engine`] instance.
///
/// Controls memtable sizing, compaction strategy selection, and all
/// compaction-related thresholds. Passed to [`Engine::open`].
pub struct EngineConfig {
    /// Max memtable size (bytes) before freeze.
    pub write_buffer_size: usize,

    /// Compaction strategy to use for this engine instance.
    ///
    /// Determines which [`CompactionStrategy`](crate::compaction::CompactionStrategy)
    /// implementations back the `minor_compact`, `tombstone_compact`, and
    /// `major_compact` methods.
    pub compaction_strategy: crate::compaction::CompactionStrategyType,

    /// Lower bound multiplier for bucket size range ([avg × bucket_low, avg × bucket_high]).
    pub bucket_low: f64,

    /// Upper bound multiplier for bucket size range.
    pub bucket_high: f64,

    /// Min size (bytes) for regular buckets; smaller SSTables go to the "small" bucket.
    pub min_sstable_size: usize,

    /// Min SSTables in a bucket to trigger minor compaction.
    pub min_threshold: usize,

    /// Max SSTables to compact at once in minor compaction.
    pub max_threshold: usize,

    /// Ratio of tombstones to total records to trigger tombstone compaction.
    pub tombstone_ratio_threshold: f64,

    /// Min SSTable age (seconds) before eligible for tombstone compaction.
    pub tombstone_compaction_interval: usize,

    /// When true, tombstone compaction resolves bloom filter false positives
    /// by doing an actual `get()` on other SSTables for point tombstones.
    pub tombstone_bloom_fallback: bool,

    /// When true, tombstone compaction will scan older SSTables to check
    /// whether a range tombstone still covers any live keys, allowing
    /// aggressive range tombstone removal.
    pub tombstone_range_drop: bool,

    /// Thread pool size for flushing memtables and compactions.
    pub thread_pool_size: usize,
}

/// Snapshot of engine statistics returned by [`Engine::stats`].
pub struct EngineStats {
    /// Number of frozen memtables pending flush.
    pub frozen_count: usize,
    /// Total number of SSTables on disk.
    pub sstables_count: usize,
    /// Sum of all SSTable file sizes in bytes.
    pub total_sst_size_bytes: u64,
    /// Per-SSTable file sizes in bytes (newest-first order).
    pub sst_sizes: Vec<u64>,
}

struct EngineInner {
    /// Persistent manifest for this engine (keeps track of SSTables, generations, etc).
    manifest: Manifest,

    /// Active memtable that accepts writes.
    active: Memtable,

    /// Frozen memtables waiting to be flushed to SSTable.
    /// We keep them in memory for reads until flush completes.
    frozen: Vec<FrozenMemtable>,

    /// Loaded SSTables.
    sstables: Vec<SSTable>,

    /// Path where engine will be mounted.
    data_dir: String,

    /// A short config for thresholds, sizes, etc.
    config: EngineConfig,
}

/// The main LSM storage engine handle.
///
/// Thread-safe — can be cloned and shared across threads via the
/// internal `Arc<RwLock<_>>`.
pub struct Engine {
    inner: Arc<RwLock<EngineInner>>,
}

impl Clone for Engine {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Engine {
    /// Opens (or creates) an engine rooted at the given directory.
    ///
    /// On a fresh directory the manifest, WAL, and SSTable sub-directories
    /// are created automatically. On an existing directory the manifest is
    /// replayed, frozen WALs are loaded, and SSTables are opened.
    pub fn open(path: impl AsRef<Path>, config: EngineConfig) -> Result<Self, EngineError> {
        // 0. Create necessary directories
        let path_str = path.as_ref().to_string_lossy();
        let manifest_dir = format!("{}/{}", path_str, MANIFEST_DIR);
        let memtable_dir = format!("{}/{}", path_str, MEMTABLE_DIR);
        let sstable_dir = format!("{}/{}", path_str, SSTABLE_DIR);

        fs::create_dir_all(&manifest_dir)?;
        fs::create_dir_all(&memtable_dir)?;
        fs::create_dir_all(&sstable_dir)?;

        // 1. Load or create manifest.
        let manifest_path = format!("{}/{}", path.as_ref().to_string_lossy(), MANIFEST_DIR);
        let manifest = Manifest::open(&manifest_path)?;
        let manifest_last_lsn = manifest.get_last_lsn()?;

        // 2. Discover existing WAL files and load active/frozen WAL info from manifest.
        let active_wal_nr = manifest.get_active_wal()?;
        let active_wal_path = format!(
            "{}/{}/wal-{:06}.log",
            path.as_ref().to_string_lossy(),
            MEMTABLE_DIR,
            active_wal_nr
        );
        let memtable = Memtable::new(active_wal_path, None, config.write_buffer_size)?;

        let frozen_wals = manifest.get_frozen_wals()?;
        let mut frozen_memtables = Vec::new();
        for wal_nr in frozen_wals {
            let frozen_wal_path = format!(
                "{}/{}/wal-{:06}.log",
                path.as_ref().to_string_lossy(),
                MEMTABLE_DIR,
                wal_nr
            );
            let memtable = Memtable::new(frozen_wal_path, None, config.write_buffer_size)?;
            frozen_memtables.push(memtable.frozen()?);
        }

        // 3. Discover existing SSTables on disk and remove orphans.
        let sstables = manifest.get_sstables()?;

        for entry in fs::read_dir(&sstable_dir)? {
            let entry = entry?;
            let file_path = entry.path();

            if file_path.is_file()
                && file_path.extension().and_then(|s| s.to_str()) == Some("sst")
                && let Some(file_name) = file_path.file_name().and_then(|s| s.to_str())
                && let Some(id) = file_name
                    .strip_prefix("sstable-")
                    .and_then(|s| s.strip_suffix(".sst"))
                    .and_then(|s| s.parse::<u64>().ok())
                && !sstables.iter().any(|entry| entry.id == id)
            {
                fs::remove_file(&file_path)?;
            }
        }

        // 4. Load SSTables from manifest.
        let mut sstable_handles = Vec::new();
        for sstable_entry in sstables {
            let mut sstable = SSTable::open(&sstable_entry.path)?;
            sstable.id = sstable_entry.id;
            sstable_handles.push(sstable);
        }

        // 5. Compute max LSN in active memtable.
        let mut max_lsn = manifest_last_lsn;

        if memtable.max_lsn() > max_lsn {
            max_lsn = memtable.max_lsn();
        }

        for frozen in frozen_memtables.iter() {
            if frozen.max_lsn() > max_lsn {
                max_lsn = frozen.max_lsn();
            }
        }

        for sstable in sstable_handles.iter() {
            if sstable.properties.max_lsn > max_lsn {
                max_lsn = sstable.properties.max_lsn;
            }
        }

        if memtable.max_lsn() != max_lsn {
            memtable.inject_max_lsn(max_lsn + 1);
        }

        // Sort frozen memtables by WAL sequence number, newest first.
        // We use wal_seq rather than creation_timestamp because on crash
        // recovery all frozen are replayed at nearly the same instant,
        // making timestamps unreliable for ordering.
        frozen_memtables.sort_by(|a, b| b.memtable.wal.wal_seq().cmp(&a.memtable.wal.wal_seq()));

        // Sort SSTables by max_lsn descending.  This lets get()
        // early-terminate: once we find a result at LSN L, any SSTable
        // whose max_lsn ≤ L cannot contain a newer version of any key.
        sstable_handles.sort_by(|a, b| b.properties.max_lsn.cmp(&a.properties.max_lsn));

        let inner = EngineInner {
            manifest,
            active: memtable,
            frozen: frozen_memtables,
            sstables: sstable_handles,
            data_dir: path.as_ref().to_string_lossy().to_string(),
            config,
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    /// Gracefully shuts down the engine.
    ///
    /// Flushes all remaining frozen memtables, checkpoints the manifest,
    /// and fsyncs all directories to ensure full durability.
    pub fn close(&self) -> Result<(), EngineError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        // 1. Flush any remaining frozen memtables to SSTables
        while !inner.frozen.is_empty() {
            Self::flush_frozen_to_sstable_inner(&mut inner)?;
        }

        // 2. Checkpoint the manifest to create a snapshot
        let max_lsn = inner.active.max_lsn();
        inner.manifest.update_lsn(max_lsn)?;
        inner.manifest.checkpoint()?;

        // 3. Fsync directories to ensure metadata is durable
        let manifest_dir = format!("{}/{}", inner.data_dir, MANIFEST_DIR);
        let memtable_dir = format!("{}/{}", inner.data_dir, MEMTABLE_DIR);
        let sstable_dir = format!("{}/{}", inner.data_dir, SSTABLE_DIR);

        // Fsync each directory
        for dir_path in [&manifest_dir, &memtable_dir, &sstable_dir] {
            if let Ok(dir) = fs::File::open(dir_path) {
                dir.sync_all()?;
            }
        }

        // 4. Fsync the root data directory
        if let Ok(root) = fs::File::open(&inner.data_dir) {
            root.sync_all()?;
        }

        Ok(())
    }

    /// Insert a key-value pair.
    ///
    /// Returns `Ok(true)` if the active memtable was frozen (caller should
    /// arrange a flush), `Ok(false)` otherwise.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<bool, EngineError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        match inner.active.put(key.clone(), value.clone()) {
            Ok(()) => Ok(false),

            Err(MemtableError::FlushRequired) => {
                Self::freeze_active(&mut inner)?;
                inner.active.put(key, value)?;

                let max_lsn = inner.active.max_lsn();
                inner.manifest.update_lsn(max_lsn)?;

                Ok(true)
            }

            Err(e) => Err(e.into()),
        }
    }

    /// Delete a key (insert a point tombstone).
    ///
    /// Returns `Ok(true)` if the active memtable was frozen, `Ok(false)` otherwise.
    pub fn delete(&self, key: Vec<u8>) -> Result<bool, EngineError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        match inner.active.delete(key.clone()) {
            Ok(()) => Ok(false),

            Err(MemtableError::FlushRequired) => {
                Self::freeze_active(&mut inner)?;
                inner.active.delete(key)?;

                let max_lsn = inner.active.max_lsn();
                inner.manifest.update_lsn(max_lsn)?;

                Ok(true)
            }

            Err(e) => Err(e.into()),
        }
    }

    /// Delete all keys in `[start_key, end_key)` (insert a range tombstone).
    ///
    /// Returns `Ok(true)` if the active memtable was frozen, `Ok(false)` otherwise.
    pub fn delete_range(&self, start_key: Vec<u8>, end_key: Vec<u8>) -> Result<bool, EngineError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        match inner
            .active
            .delete_range(start_key.clone(), end_key.clone())
        {
            Ok(()) => Ok(false),

            Err(MemtableError::FlushRequired) => {
                Self::freeze_active(&mut inner)?;
                inner.active.delete_range(start_key, end_key)?;

                let max_lsn = inner.active.max_lsn();
                inner.manifest.update_lsn(max_lsn)?;

                Ok(true)
            }

            Err(e) => Err(e.into()),
        }
    }

    /// Look up a single key.
    ///
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` if it has
    /// been deleted or was never written, or `Err` on I/O failure.
    ///
    /// The lookup order is: active memtable → frozen memtables → SSTables
    /// (all newest-first). The first definitive result wins.
    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, EngineError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        // --------------------------------------------------
        // 1. Active memtable (newest)
        // --------------------------------------------------
        match inner.active.get(&key)? {
            MemtableGetResult::Put(value) => return Ok(Some(value)),
            MemtableGetResult::Delete | MemtableGetResult::RangeDelete => return Ok(None),
            MemtableGetResult::NotFound => {}
        }

        // --------------------------------------------------
        // 2. Frozen memtables (newest → oldest)
        // --------------------------------------------------
        for frozen in &inner.frozen {
            match frozen.get(&key)? {
                MemtableGetResult::Put(value) => return Ok(Some(value)),
                MemtableGetResult::Delete | MemtableGetResult::RangeDelete => {
                    return Ok(None);
                }
                MemtableGetResult::NotFound => {}
            }
        }

        // --------------------------------------------------
        // 3. SSTables (sorted by max_lsn descending)
        //
        //    After size-tiered compaction, a merged SSTable may
        //    span a wide LSN range. We track the best (highest-LSN)
        //    result found so far. Once an SSTable's max_lsn is ≤
        //    the best LSN, no subsequent SSTable can beat it, so
        //    we break early.
        // --------------------------------------------------
        let mut best_sst: Option<sstable::GetResult> = None;
        let mut best_lsn: u64 = 0;

        for sst in &inner.sstables {
            // Early termination: this SSTable (and all after it) have
            // max_lsn ≤ best_lsn, so they can't contain a newer version.
            if sst.properties.max_lsn <= best_lsn {
                break;
            }

            match sst.get(&key)? {
                sstable::GetResult::NotFound => {}
                result => {
                    let lsn = result.lsn();
                    if lsn > best_lsn {
                        best_lsn = lsn;
                        best_sst = Some(result);
                    }
                }
            }
        }

        match best_sst {
            Some(sstable::GetResult::Put { value, .. }) => Ok(Some(value)),
            Some(sstable::GetResult::Delete { .. } | sstable::GetResult::RangeDelete { .. }) => {
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Scan all live key-value pairs in `[start_key, end_key)`.
    ///
    /// Returns an iterator of `(key, value)` pairs, merging entries from
    /// all layers and applying point/range tombstones to filter out
    /// deleted keys.
    pub fn scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>, EngineError> {
        let merged = self.raw_scan(start_key, end_key)?;
        Ok(VisibilityFilter::new(merged))
    }

    fn raw_scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<utils::MergeIterator<'static>, EngineError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        let mut iters: Vec<Box<dyn Iterator<Item = Record>>> = Vec::new();

        // Active memtable - collect to own the data
        let active_records: Vec<_> = inner.active.scan(start_key, end_key)?.collect();
        iters.push(Box::new(active_records.into_iter()));

        // Frozen memtables - collect to own the data
        for frozen in &inner.frozen {
            let records: Vec<_> = frozen.scan(start_key, end_key)?.collect();
            iters.push(Box::new(records.into_iter()));
        }

        // SSTables - collect to own the data
        for sstable in &inner.sstables {
            let records: Vec<_> = sstable.scan(start_key, end_key)?.collect();
            iters.push(Box::new(records.into_iter()));
        }

        Ok(utils::MergeIterator::new(iters))
    }

    /// Returns a snapshot of engine statistics.
    ///
    /// Includes frozen memtable count, SSTable count, per-SSTable file
    /// sizes, and total on-disk SSTable size.
    pub fn stats(&self) -> Result<EngineStats, EngineError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        let sst_sizes: Vec<u64> = inner.sstables.iter().map(|s| s.file_size()).collect();
        let total_sst_size_bytes: u64 = sst_sizes.iter().sum();

        Ok(EngineStats {
            frozen_count: inner.frozen.len(),
            sstables_count: inner.sstables.len(),
            total_sst_size_bytes,
            sst_sizes,
        })
    }

    /// Freeze the current active memtable and swap in a fresh one.
    /// The old memtable is pushed to the front of `inner.frozen`.
    fn freeze_active(inner: &mut EngineInner) -> Result<(), EngineError> {
        let frozen_wal_id = inner.active.wal.wal_seq();
        let current_max_lsn = inner.active.max_lsn();
        let new_active_wal_id = frozen_wal_id + 1;

        let new_active = Memtable::new(
            format!(
                "{}/{}/wal-{:06}.log",
                inner.data_dir, MEMTABLE_DIR, new_active_wal_id
            ),
            None,
            inner.config.write_buffer_size,
        )?;

        let old_active = std::mem::replace(&mut inner.active, new_active);
        let frozen = old_active.frozen()?;
        // Insert at beginning to maintain sorted order (newest first)
        inner.frozen.insert(0, frozen);

        // Ensure LSN continuity
        inner.active.inject_max_lsn(current_max_lsn);

        inner.manifest.add_frozen_wal(frozen_wal_id)?;
        inner.manifest.set_active_wal(new_active_wal_id)?;

        Ok(())
    }

    /// Flush the oldest frozen memtable to a new SSTable.
    ///
    /// Returns `Ok(true)` if a frozen memtable was flushed, `Ok(false)` if
    /// there were no frozen memtables to flush.
    pub fn flush_oldest_frozen(&self) -> Result<bool, EngineError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        if inner.frozen.is_empty() {
            return Ok(false);
        }
        Self::flush_frozen_to_sstable_inner(&mut inner)?;
        Ok(true)
    }

    /// Flush **all** frozen memtables to SSTables.
    ///
    /// Returns the number of frozen memtables that were flushed.
    pub fn flush_all_frozen(&self) -> Result<usize, EngineError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        let mut count = 0usize;
        while !inner.frozen.is_empty() {
            Self::flush_frozen_to_sstable_inner(&mut inner)?;
            count += 1;
        }
        Ok(count)
    }

    /// Allocates the next unique SSTable ID from the manifest's monotonic counter.
    fn next_sstable_id(inner: &mut EngineInner) -> Result<u64, EngineError> {
        Ok(inner.manifest.allocate_sst_id()?)
    }

    fn flush_frozen_to_sstable_inner(inner: &mut EngineInner) -> Result<(), EngineError> {
        if inner.frozen.is_empty() {
            return Ok(());
        }

        // Take the oldest frozen memtable (last in the newest-first vec).
        // We flush oldest first so that `insert(0, sstable)` keeps the
        // sstables list in newest-first order after a batch flush.
        let frozen = inner
            .frozen
            .pop()
            .ok_or_else(|| EngineError::Internal("frozen list became empty unexpectedly".into()))?;
        let frozen_wal_id = frozen.memtable.wal.wal_seq();

        // Get all records from the frozen memtable
        let records: Vec<_> = frozen.iter_for_flush()?.collect();

        // Separate into point entries and range tombstones
        let mut point_entries = Vec::new();
        let mut range_tombstones = Vec::new();

        for record in records {
            match record {
                Record::Put {
                    key,
                    value,
                    lsn,
                    timestamp,
                } => {
                    point_entries.push(PointEntry {
                        key,
                        value: Some(value),
                        lsn,
                        timestamp,
                    });
                }
                Record::Delete {
                    key,
                    lsn,
                    timestamp,
                } => {
                    point_entries.push(PointEntry {
                        key,
                        value: None,
                        lsn,
                        timestamp,
                    });
                }
                Record::RangeDelete {
                    start,
                    end,
                    lsn,
                    timestamp,
                } => {
                    range_tombstones.push(RangeTombstone {
                        start,
                        end,
                        lsn,
                        timestamp,
                    });
                }
            }
        }

        // Generate unique SSTable ID and path
        let sstable_id = Self::next_sstable_id(inner)?;
        let sstable_path = format!(
            "{}/{}/sstable-{}.sst",
            inner.data_dir, SSTABLE_DIR, sstable_id
        );

        // Build the SSTable
        let point_count = point_entries.len();
        let range_count = range_tombstones.len();

        sstable::SstWriter::new(&sstable_path).build(
            point_entries.into_iter(),
            point_count,
            range_tombstones.into_iter(),
            range_count,
        )?;

        // Load the newly created SSTable
        let mut sstable = SSTable::open(&sstable_path)?;
        sstable.id = sstable_id;
        // Insert at beginning to maintain sorted order (newest first)
        inner.sstables.insert(0, sstable);

        // Update manifest
        inner.manifest.add_sstable(ManifestSstEntry {
            id: sstable_id,
            path: sstable_path.into(),
        })?;

        // Remove the frozen WAL from manifest
        inner.manifest.remove_frozen_wal(frozen_wal_id)?;

        Ok(())
    }

    // --------------------------------------------------------------------------------------------
    // Compaction API
    // --------------------------------------------------------------------------------------------

    /// Execute a compaction strategy, applying the result to the engine.
    ///
    /// Returns `Ok(true)` if compaction was performed, `Ok(false)` if
    /// the strategy decided there was nothing to do.
    fn run_compaction(
        &self,
        strategy: &dyn crate::compaction::CompactionStrategy,
    ) -> Result<bool, EngineError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        let inner = &mut *inner; // reborrow to split fields
        let sst_count = inner.sstables.len();
        let result = strategy
            .compact(
                &inner.sstables,
                &mut inner.manifest,
                &inner.data_dir,
                &inner.config,
            )
            .map_err(|e| EngineError::Internal(format!("Compaction failed: {e}")))?;

        match result {
            None => {
                tracing::debug!(sst_count, "compaction strategy found nothing to do");
                Ok(false)
            }
            Some(cr) => {
                tracing::info!(
                    sst_count_before = sst_count,
                    removed = cr.removed_ids.len(),
                    new_id = ?cr.new_sst_id,
                    "compaction applied"
                );
                Self::apply_compaction_result(inner, cr)?;
                Ok(true)
            }
        }
    }

    /// Runs one round of **minor compaction** (size-tiered).
    ///
    /// Selects the best bucket whose size exceeds `min_threshold` and merges
    /// those SSTables into a single new SSTable, deduplicating point entries
    /// and preserving all tombstones.
    ///
    /// Returns `Ok(true)` if compaction was performed, `Ok(false)` if no
    /// bucket met the threshold.
    pub fn minor_compact(&self) -> Result<bool, EngineError> {
        let strategy = {
            let inner = self
                .inner
                .read()
                .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;
            inner.config.compaction_strategy.minor()
        };
        self.run_compaction(strategy.as_ref())
    }

    /// Runs one round of **tombstone compaction** (per-SSTable GC).
    ///
    /// Selects the SSTable with the highest tombstone ratio that exceeds
    /// `tombstone_ratio_threshold` and rewrites it, dropping provably-unnecessary
    /// tombstones.
    ///
    /// Returns `Ok(true)` if compaction was performed, `Ok(false)` if no
    /// SSTable was eligible.
    pub fn tombstone_compact(&self) -> Result<bool, EngineError> {
        let strategy = {
            let inner = self
                .inner
                .read()
                .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;
            inner.config.compaction_strategy.tombstone()
        };
        self.run_compaction(strategy.as_ref())
    }

    /// Runs **major compaction** — merges all SSTables into one.
    ///
    /// Actively applies range tombstones to suppress covered Puts, and
    /// drops all spent tombstones from the output.
    ///
    /// Returns `Ok(true)` if compaction was performed, `Ok(false)` if
    /// there are fewer than 2 SSTables.
    pub fn major_compact(&self) -> Result<bool, EngineError> {
        let strategy = {
            let inner = self
                .inner
                .read()
                .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;
            inner.config.compaction_strategy.major()
        };
        self.run_compaction(strategy.as_ref())
    }

    /// Applies a `CompactionResult` to the in-memory engine state.
    ///
    /// Removes consumed SSTables, inserts the newly built one, and
    /// re-sorts by `max_lsn` descending so that `get()` can
    /// early-terminate correctly.
    fn apply_compaction_result(
        inner: &mut EngineInner,
        cr: crate::compaction::CompactionResult,
    ) -> Result<(), EngineError> {
        // Remove consumed SSTables.
        inner
            .sstables
            .retain(|sst| !cr.removed_ids.contains(&sst.id));

        // Load and insert new SSTable if one was produced.
        if let Some(ref path) = cr.new_sst_path {
            let mut new_sst = SSTable::open(path)?;
            new_sst.id = cr.new_sst_id.unwrap_or(0);
            inner.sstables.push(new_sst);
        }

        // Re-sort by max_lsn descending to maintain the early-termination
        // invariant used by get().
        inner
            .sstables
            .sort_by(|a, b| b.properties.max_lsn.cmp(&a.properties.max_lsn));

        Ok(())
    }
}

/// Type alias preserving the public scan iterator name.
pub type EngineScanIterator = utils::MergeIterator<'static>;

/// Filters a sorted record stream to yield only **visible** key-value pairs.
///
/// Applies point tombstone and range tombstone semantics:
/// - A `Delete` record suppresses the same key in later (lower-LSN) records.
/// - A `RangeDelete` suppresses any `Put` whose key falls within `[start, end)`
///   and whose LSN is lower than the tombstone's LSN.
///
/// The input iterator **must** be sorted by `(key ASC, LSN DESC)` — the order
/// produced by [`MergeIterator`](utils::MergeIterator).
pub struct VisibilityFilter<I>
where
    I: Iterator<Item = Record>,
{
    /// Underlying merged record stream.
    input: I,
    /// The key most recently emitted or suppressed (used for dedup).
    current_key: Option<Vec<u8>>,
    /// Accumulated range tombstones that may cover upcoming keys.
    active_ranges: Vec<RangeTombstone>,
}

impl<I> VisibilityFilter<I>
where
    I: Iterator<Item = Record>,
{
    pub fn new(input: I) -> Self {
        Self {
            input,
            current_key: None,
            active_ranges: Vec::new(),
        }
    }
}

impl<I> Iterator for VisibilityFilter<I>
where
    I: Iterator<Item = Record>,
{
    type Item = (Vec<u8>, Vec<u8>); // (key, value)

    fn next(&mut self) -> Option<Self::Item> {
        for record in self.input.by_ref() {
            match record {
                Record::RangeDelete {
                    start,
                    end,
                    lsn,
                    timestamp,
                } => {
                    self.active_ranges.push(RangeTombstone {
                        start,
                        end,
                        lsn,
                        timestamp,
                    });
                    // Range tombstone itself is not returned
                }

                Record::Delete { key, .. } => {
                    self.current_key = Some(key.clone());
                }

                Record::Put {
                    key, value, lsn, ..
                } => {
                    // Skip if we've already handled this key
                    if self.current_key.as_deref() == Some(&key) {
                        continue;
                    }

                    // Check range tombstones
                    let deleted = self.active_ranges.iter().any(|r| {
                        r.start.as_slice() <= key.as_slice()
                            && key.as_slice() < r.end.as_slice()
                            && r.lsn > lsn
                    });

                    self.current_key = Some(key.clone());

                    if deleted {
                        continue; // This record is shadowed by a range tombstone
                    }

                    return Some((key, value));
                }
            }
        }

        None
    }
}
