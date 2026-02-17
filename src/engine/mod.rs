//! LSM Engine API (spec & pseudocode)
//!
//!

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};

use thiserror::Error;

use crate::manifest::{Manifest, ManifestError, ManifestSstEntry};
use crate::memtable::{FrozenMemtable, Memtable, MemtableError, MemtableGetResult};
use crate::sstable::{self, SSTable, SSTableError};

pub mod utils;
pub use utils::Record;

#[cfg(test)]
mod tests;

pub const MANIFEST_DIR: &str = "manifest";
pub const MEMTABLE_DIR: &str = "memtables";
pub const SSTABLE_DIR: &str = "sstables";

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Manifest error: {0}")]
    Manifest(#[from] ManifestError),

    #[error("Memtable error: {0}")]
    Memtable(#[from] MemtableError),

    #[error("SSTable error: {0}")]
    SSTable(#[from] SSTableError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub struct EngineConfig {
    /// max memtable size (MB) before flush; threshold for oversized records.
    pub write_buffer_size: usize,

    /// Lower bound multiplier for bucket size range ([avg × bucket_low, avg × bucket_high])
    pub bucket_low: f64,

    /// Upper bound multiplier for bucket size range.
    pub bucket_high: f64,

    /// Min size (MB) for regular buckets; smaller go to "small" bucket.
    pub min_sstable_size: usize,

    /// Min SSTables in bucket for minor compaction.
    pub min_threshold: usize,

    /// Max SSTables per minor compaction.
    pub max_threshold: usize,

    /// Ratio of droppable tombstones to trigger tombstone compaction.
    pub tombstone_threshold: f64,

    /// Min SSTable age (seconds) for tombstone compaction.
    pub tombstone_compaction_interval: usize,

    /// Thread pool for flushing memtables and compcations.
    pub thread_pool_size: usize,
}

pub struct EngineStats {
    pub frozen_count: usize,
    pub sstables_count: usize,
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

pub struct Engine {
    inner: Arc<RwLock<EngineInner>>,
}

impl Engine {
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

        // 3. Diccover existing SSTables on disk and remove orphans.
        let sstables = manifest.get_sstables()?;

        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let file_path = entry.path();

            if file_path.is_file() && file_path.extension().and_then(|s| s.to_str()) == Some("sst")
            {
                if let Some(file_name) = file_path.file_name().and_then(|s| s.to_str()) {
                    if let Some(id) = file_name
                        .strip_prefix("sst-")
                        .and_then(|s| s.strip_suffix(".sst"))
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        if !sstables.iter().any(|entry| entry.id == id) {
                            fs::remove_file(&file_path)?;
                        }
                    }
                }
            }
        }

        // 4. Load SSTables from manifest.
        let mut sstable_handles = Vec::new();
        for sstable_entry in sstables {
            let sstable = SSTable::open(&sstable_entry.path)?;
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
        frozen_memtables.sort_by(|a, b| {
            b.memtable
                .wal
                .header
                .wal_seq
                .cmp(&a.memtable.wal.header.wal_seq)
        });

        // Sort sstables by creation timestamp, newest first
        sstable_handles.sort_by(|a, b| {
            b.properties
                .creation_timestamp
                .cmp(&a.properties.creation_timestamp)
        });

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
        // 3. SSTables (newest → oldest)
        // --------------------------------------------------
        for sstable in &inner.sstables {
            match sstable.get(&key)? {
                sstable::SSTGetResult::Put { value, .. } => return Ok(Some(value)),
                sstable::SSTGetResult::Delete { .. }
                | sstable::SSTGetResult::RangeDelete { .. } => return Ok(None),
                sstable::SSTGetResult::NotFound => {}
            }
        }

        // --------------------------------------------------
        // 4. Not found anywhere
        // --------------------------------------------------
        Ok(None)
    }

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
    ) -> Result<EngineScanIterator, EngineError> {
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

        Ok(EngineScanIterator::new(iters))
    }

    pub fn stats(&self) -> Result<EngineStats, EngineError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| EngineError::Internal("RwLock poisoned".into()))?;

        Ok(EngineStats {
            frozen_count: inner.frozen.len(),
            sstables_count: inner.sstables.len(),
        })
    }

    /// Freeze the current active memtable and swap in a fresh one.
    /// The old memtable is pushed to the front of `inner.frozen`.
    fn freeze_active(inner: &mut EngineInner) -> Result<(), EngineError> {
        let frozen_wal_id = inner.active.wal.header.wal_seq;
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

    fn flush_frozen_to_sstable_inner(inner: &mut EngineInner) -> Result<(), EngineError> {
        if inner.frozen.is_empty() {
            return Ok(());
        }

        // Take the oldest frozen memtable (last in the newest-first vec).
        // We flush oldest first so that `insert(0, sstable)` keeps the
        // sstables list in newest-first order after a batch flush.
        let frozen = inner.frozen.pop().unwrap();
        let frozen_wal_id = frozen.memtable.wal.header.wal_seq;

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
                    point_entries.push(sstable::MemtablePointEntry {
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
                    point_entries.push(sstable::MemtablePointEntry {
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
                    range_tombstones.push(sstable::MemtableRangeTombstone {
                        start,
                        end,
                        lsn,
                        timestamp,
                    });
                }
            }
        }

        // Generate unique SSTable ID by finding max ID from existing SSTable files
        let mut max_id = 0u64;
        let sstable_dir = format!("{}/{}", inner.data_dir, SSTABLE_DIR);

        if let Ok(entries) = fs::read_dir(&sstable_dir) {
            for entry in entries.flatten() {
                if let Some(file_name) = entry.file_name().to_str() {
                    if let Some(id) = file_name
                        .strip_prefix("sstable-")
                        .and_then(|s| s.strip_suffix(".sst"))
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        max_id = max_id.max(id);
                    }
                }
            }
        }

        let sstable_id = max_id + 1;
        let sstable_path = format!(
            "{}/{}/sstable-{}.sst",
            inner.data_dir, SSTABLE_DIR, sstable_id
        );

        // Build the SSTable
        let point_count = point_entries.len();
        let range_count = range_tombstones.len();

        sstable::build_from_iterators(
            &sstable_path,
            point_count,
            point_entries.into_iter(),
            range_count,
            range_tombstones.into_iter(),
        )?;

        // Load the newly created SSTable
        let sstable = SSTable::open(&sstable_path)?;
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

    fn trigger_compcation(&self) -> Result<(), EngineError> {
        unimplemented!()
    }
}

pub struct EngineScanIterator {
    iters: Vec<Box<dyn Iterator<Item = Record>>>,
    heap: BinaryHeap<ScanHeapEntry>,
}

struct ScanHeapEntry {
    record: Record,
    source_idx: usize,
}

impl Ord for ScanHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // We want the heap to be a min-heap based on key and LSN, so we reverse the order
        utils::record_cmp(&self.record, &other.record).reverse()
    }
}

impl PartialOrd for ScanHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScanHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.record.lsn() == other.record.lsn() && self.record.key() == other.record.key()
    }
}

impl Eq for ScanHeapEntry {}

impl EngineScanIterator {
    pub fn new(mut iters: Vec<Box<dyn Iterator<Item = Record>>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some(record) = iter.next() {
                heap.push(ScanHeapEntry {
                    record,
                    source_idx: idx,
                });
            }
        }

        Self { iters, heap }
    }
}

impl Iterator for EngineScanIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.heap.pop()?;

        let result = item.record;
        let idx = item.source_idx;

        if let Some(next_record) = self.iters[idx].next() {
            self.heap.push(ScanHeapEntry {
                record: next_record,
                source_idx: idx,
            });
        }

        Some(result)
    }
}

pub struct VisibilityFilter<I>
where
    I: Iterator<Item = Record>,
{
    input: I, // The underlying iterator of records (already sorted by key and LSN)
    current_key: Option<Vec<u8>>, // The key currently being processed
    active_ranges: Vec<RangeTombstone>, // Only range tombstones that cover the current key
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

struct RangeTombstone {
    start: Vec<u8>,
    end: Vec<u8>,
    lsn: u64,
    timestamp: u64,
}

impl<I> Iterator for VisibilityFilter<I>
where
    I: Iterator<Item = Record>,
{
    type Item = (Vec<u8>, Vec<u8>); // (key, value)

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(record) = self.input.next() {
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
                    continue; // Range tombstone itself is not returned
                }

                Record::Delete { key, .. } => {
                    self.current_key = Some(key.clone());
                    continue;
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
