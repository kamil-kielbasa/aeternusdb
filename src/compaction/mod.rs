//! # Compaction Module
//!
//! Implements three compaction strategies for the LSM storage engine:
//!
//! ## Minor Compaction (Size-Tiered)
//!
//! Groups SSTables into **size buckets** and merges similarly-sized tables
//! when a bucket exceeds `min_threshold` entries. Deduplicates point entries
//! (keeps highest LSN per key) but **preserves all tombstones** — both point
//! and range — because other SSTables outside the merge set may still hold
//! covered data.
//!
//! ## Tombstone Compaction (Per-SSTable GC)
//!
//! Rewrites a single SSTable to remove **point tombstones** that are provably
//! unnecessary. Uses bloom filters on other SSTables to determine safety:
//!
//! - If no other SSTable's bloom says "maybe" for the key → drop the tombstone.
//! - If bloom says "maybe" and `tombstone_bloom_fallback = true` → do actual `get()`
//!   to resolve false positives.
//! - If `tombstone_range_drop = true` → scan older SSTables to check
//!   whether a range tombstone still covers any live keys.
//!
//! ## Major Compaction (Full Merge)
//!
//! User-triggered via `Engine::major_compact()`. Merges **all** SSTables into
//! one, applying range tombstones actively to suppress covered point entries.
//! All spent tombstones (both point and range) are dropped from the output
//! since the entire SSTable set is merged — no data can resurrect.
//!
//! ## Code organization
//!
//! The module separates strategy-specific logic (bucketing, selection) from
//! shared execution primitives (merge, dedup, build). This allows future
//! strategies (e.g., leveled compaction) to reuse the merge/build plumbing.

pub mod stcs;

use std::sync::Arc;

use crate::engine::RangeTombstone;
pub use crate::engine::utils::MergeIterator;
use crate::engine::utils::Record;
use crate::sstable::{self, PointEntry, SSTable, SSTableError};

use crate::engine::{EngineConfig, SSTABLE_DIR};
use crate::manifest::{Manifest, ManifestError, ManifestSstEntry};
use tracing::{debug, info};

// ------------------------------------------------------------------------------------------------
// CompactionStrategy trait
// ------------------------------------------------------------------------------------------------

/// A uniform interface for compaction strategies.
///
/// Each strategy receives the current set of SSTables (by reference), a
/// mutable manifest for atomic metadata updates, the data directory, and
/// the engine configuration. It returns:
///
/// - `Ok(Some(result))` — compaction was performed; the caller should
///   update in-memory state using [`CompactionResult`].
/// - `Ok(None)` — nothing to compact (thresholds not met, etc.).
pub trait CompactionStrategy {
    /// Execute one round of compaction, if the strategy's preconditions
    /// are met. Implementations must be idempotent — calling when there
    /// is nothing to do should simply return `Ok(None)`.
    fn compact(
        &self,
        sstables: &[Arc<SSTable>],
        manifest: &mut Manifest,
        data_dir: &str,
        config: &EngineConfig,
    ) -> Result<Option<CompactionResult>, CompactionError>;
}

// ------------------------------------------------------------------------------------------------
// CompactionStrategyType — config-level strategy selector
// ------------------------------------------------------------------------------------------------

/// Selects which compaction strategy family the engine should use.
///
/// Stored in [`DbConfig`](crate::DbConfig) and used by the engine to
/// obtain the concrete strategy implementations for minor, tombstone,
/// and major compaction.
///
/// # Example
///
/// ```rust
/// use aeternusdb::{DbConfig, CompactionStrategyType};
///
/// let config = DbConfig {
///     compaction_strategy: CompactionStrategyType::Stcs,
///     ..DbConfig::default()
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStrategyType {
    /// Size-Tiered Compaction Strategy (STCS).
    ///
    /// Groups SSTables into size buckets and merges similarly-sized tables.
    /// Good for write-heavy workloads with moderate space amplification.
    Stcs,
}

impl CompactionStrategyType {
    /// Returns the minor compaction strategy for this family.
    pub fn minor(&self) -> Box<dyn CompactionStrategy> {
        match self {
            Self::Stcs => Box::new(stcs::MinorCompaction),
        }
    }

    /// Returns the tombstone compaction strategy for this family.
    pub fn tombstone(&self) -> Box<dyn CompactionStrategy> {
        match self {
            Self::Stcs => Box::new(stcs::TombstoneCompaction),
        }
    }

    /// Returns the major compaction strategy for this family.
    pub fn major(&self) -> Box<dyn CompactionStrategy> {
        match self {
            Self::Stcs => Box::new(stcs::MajorCompaction),
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Shared types
// ------------------------------------------------------------------------------------------------

/// Result of a compaction execution — enough information to update the
/// manifest and in-memory SSTable list.
pub struct CompactionResult {
    /// SSTable IDs that were consumed (to be removed from manifest).
    pub removed_ids: Vec<u64>,

    /// Path of the newly built SSTable (if any). `None` when all entries
    /// were eliminated (e.g., all tombstones dropped in major compaction).
    pub new_sst_path: Option<String>,

    /// The ID allocated for the new SSTable (if one was produced).
    pub new_sst_id: Option<u64>,
}

// ------------------------------------------------------------------------------------------------
// Dedup logic — shared between minor and tombstone compaction
// ------------------------------------------------------------------------------------------------

/// Deduplicates a merge iterator stream into separate point entries
/// and range tombstones.
///
/// For each unique key, keeps only the version with the highest LSN.
/// **All tombstones (point and range) are preserved** — this is safe
/// for minor compaction where other SSTables may hold covered data.
pub fn dedup_records(
    merge_iter: impl Iterator<Item = Record>,
) -> (Vec<PointEntry>, Vec<RangeTombstone>) {
    let mut point_entries = Vec::new();
    let mut range_tombstones = Vec::new();
    let mut last_key: Option<Vec<u8>> = None;

    for record in merge_iter {
        match record {
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
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                if last_key.as_ref() == Some(&key) {
                    continue; // Older version — skip
                }
                last_key = Some(key.clone());
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
                if last_key.as_ref() == Some(&key) {
                    continue; // Older version — skip
                }
                last_key = Some(key.clone());
                point_entries.push(PointEntry {
                    key,
                    value: None,
                    lsn,
                    timestamp,
                });
            }
        }
    }

    (point_entries, range_tombstones)
}

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

/// Creates scan iterators for the given SSTables covering their full key range.
///
/// Computes the min/max key bounds across all selected SSTables and returns
/// a vec of boxed iterators that can be fed into [`MergeIterator`].
///
/// The returned iterators borrow the SSTables; iteration is **streaming**
/// (block-by-block via mmap) so only one data block per SSTable is
/// resident in memory at a time.
pub fn full_range_scan_iters<'a>(
    sstables: &'a [&'a SSTable],
) -> Result<Vec<Box<dyn Iterator<Item = Record> + 'a>>, SSTableError> {
    if sstables.is_empty() {
        return Ok(Vec::new());
    }

    // Compute scan bounds from properties.
    let min_key = sstables
        .iter()
        .map(|s| &s.properties.min_key)
        .min()
        .ok_or_else(|| SSTableError::Internal("empty sstables in full_range_scan".into()))?
        .clone();

    let mut max_key = sstables
        .iter()
        .map(|s| &s.properties.max_key)
        .max()
        .ok_or_else(|| SSTableError::Internal("empty sstables in full_range_scan".into()))?
        .clone();
    // Extend max_key past the actual max key to make it exclusive.
    max_key.push(0xFF);

    let mut iters: Vec<Box<dyn Iterator<Item = Record> + 'a>> = Vec::new();
    for sst in sstables {
        let scan = sst.scan(&min_key, &max_key)?;
        iters.push(Box::new(scan));
    }

    Ok(iters)
}

// ------------------------------------------------------------------------------------------------
// Shared error type
// ------------------------------------------------------------------------------------------------

/// Unified error type for all compaction strategies.
#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    #[error("SSTable error: {0}")]
    SSTable(#[from] SSTableError),

    #[error("Manifest error: {0}")]
    Manifest(#[from] ManifestError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

// ------------------------------------------------------------------------------------------------
// Finalize — shared build + manifest + cleanup
// ------------------------------------------------------------------------------------------------

/// Builds a new SSTable from the given entries, atomically updates the
/// manifest, and deletes old SSTable files.
///
/// If both `point_entries` and `range_tombstones` are empty, no new SSTable
/// is produced — old SSTables are simply removed.
///
/// This is the common tail shared by minor, tombstone, and major compaction.
pub(crate) fn finalize_compaction(
    manifest: &mut Manifest,
    data_dir: &str,
    removed_ids: Vec<u64>,
    point_entries: Vec<PointEntry>,
    range_tombstones: Vec<RangeTombstone>,
) -> Result<CompactionResult, CompactionError> {
    use std::fs;
    use std::path::PathBuf;

    if point_entries.is_empty() && range_tombstones.is_empty() {
        // Nothing survived — just remove old SSTables from manifest.
        info!(
            removed_count = removed_ids.len(),
            ?removed_ids,
            "finalize: all entries eliminated, removing old SSTables"
        );
        manifest.apply_compaction(Vec::new(), removed_ids.clone())?;
        manifest.checkpoint()?;

        for id in &removed_ids {
            let path = format!("{}/{}/{:06}.sst", data_dir, SSTABLE_DIR, id);
            if let Err(e) = fs::remove_file(&path) {
                tracing::warn!(id, %e, "failed to remove old SSTable file during compaction");
            }
        }

        return Ok(CompactionResult {
            removed_ids,
            new_sst_path: None,
            new_sst_id: None,
        });
    }

    // Build new SSTable.
    let new_sst_id = manifest.allocate_sst_id()?;
    let new_sst_path = format!("{}/{}/{:06}.sst", data_dir, SSTABLE_DIR, new_sst_id);

    let point_count = point_entries.len();
    let range_count = range_tombstones.len();

    debug!(
        new_sst_id,
        point_count,
        range_count,
        removed_count = removed_ids.len(),
        path = %new_sst_path,
        "finalize: building new SSTable"
    );

    sstable::SstWriter::new(&new_sst_path).build(
        point_entries.into_iter(),
        point_count,
        range_tombstones.into_iter(),
        range_count,
    )?;

    // Atomic manifest update: add new, remove old.
    let new_entry = ManifestSstEntry {
        id: new_sst_id,
        path: PathBuf::from(&new_sst_path),
    };
    manifest.apply_compaction(vec![new_entry], removed_ids.clone())?;
    manifest.checkpoint()?;

    // Delete old SSTable files.
    for id in &removed_ids {
        let path = format!("{}/{}/{:06}.sst", data_dir, SSTABLE_DIR, id);
        if let Err(e) = fs::remove_file(&path) {
            tracing::warn!(id, %e, "failed to remove old SSTable file during compaction");
        }
    }

    Ok(CompactionResult {
        removed_ids,
        new_sst_path: Some(new_sst_path),
        new_sst_id: Some(new_sst_id),
    })
}
