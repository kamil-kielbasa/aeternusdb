//! # Size-Tiered Compaction Strategy (STCS)
//!
//! Groups SSTables into **size buckets** and provides three compaction
//! operations:
//!
//! - **Minor** — merges similarly-sized SSTables within a bucket, deduplicates
//!   point entries, preserves all tombstones.
//! - **Tombstone** — rewrites a single high-tombstone-ratio SSTable, dropping
//!   point and range tombstones that are provably unnecessary.
//! - **Major** — merges *all* SSTables into one, actively applying range
//!   tombstones and dropping all spent tombstones.

#[cfg(test)]
mod tests;

pub mod major;
pub mod minor;
pub mod tombstone;

use std::sync::Arc;

use crate::engine::EngineConfig;
use crate::sstable::SSTable;

use crate::compaction::{CompactionError, CompactionResult, CompactionStrategy};
use crate::manifest::Manifest;

// ------------------------------------------------------------------------------------------------
// Bucketing
// ------------------------------------------------------------------------------------------------

/// Groups SSTables into size buckets for minor compaction.
///
/// SSTables smaller than `config.min_sstable_size` go into a special
/// "small" bucket. Remaining SSTables are grouped so that within each
/// bucket, every SSTable's file size falls within
/// `[bucket_avg × bucket_low, bucket_avg × bucket_high]`.
///
/// Returns a vec of buckets, where each bucket is a vec of indices
/// into the input `sstables` slice.
pub fn bucket_sstables(sstables: &[Arc<SSTable>], config: &EngineConfig) -> Vec<Vec<usize>> {
    if sstables.is_empty() {
        return Vec::new();
    }

    // Sort indices by file size ascending.
    let mut indices: Vec<usize> = (0..sstables.len()).collect();
    indices.sort_by_key(|&i| sstables[i].file_size());

    let mut small_bucket: Vec<usize> = Vec::new();
    let mut regular: Vec<usize> = Vec::new();

    for &idx in &indices {
        if sstables[idx].file_size() < config.min_sstable_size as u64 {
            small_bucket.push(idx);
        } else {
            regular.push(idx);
        }
    }

    let mut buckets: Vec<Vec<usize>> = Vec::new();
    if !small_bucket.is_empty() {
        buckets.push(small_bucket);
    }

    let mut current_bucket: Vec<usize> = Vec::new();
    let mut current_avg: f64 = 0.0;

    for &idx in &regular {
        let size = sstables[idx].file_size() as f64;

        if current_bucket.is_empty() {
            current_bucket.push(idx);
            current_avg = size;
        } else {
            let low = current_avg * config.bucket_low;
            let high = current_avg * config.bucket_high;

            if size >= low && size <= high {
                current_bucket.push(idx);
                // Recompute average.
                let total: f64 = current_bucket
                    .iter()
                    .map(|&i| sstables[i].file_size() as f64)
                    .sum();
                current_avg = total / current_bucket.len() as f64;
            } else {
                buckets.push(std::mem::take(&mut current_bucket));
                current_bucket.push(idx);
                current_avg = size;
            }
        }
    }

    if !current_bucket.is_empty() {
        buckets.push(current_bucket);
    }

    buckets
}

/// Selects the best bucket for minor compaction.
///
/// Returns the indices of SSTables to compact, or `None` if no bucket
/// meets `min_threshold`. If multiple buckets qualify, picks the one
/// with the most SSTables (to maximize compaction ratio). Limits the
/// selection to `max_threshold` SSTables.
pub fn select_compaction_bucket(
    buckets: &[Vec<usize>],
    config: &EngineConfig,
) -> Option<Vec<usize>> {
    let mut best_bucket: Option<&Vec<usize>> = None;
    let mut best_count = 0usize;

    for bucket in buckets {
        if bucket.len() >= config.min_threshold && bucket.len() > best_count {
            best_bucket = Some(bucket);
            best_count = bucket.len();
        }
    }

    best_bucket.map(|bucket| bucket.iter().take(config.max_threshold).copied().collect())
}

// ------------------------------------------------------------------------------------------------
// CompactionStrategy implementations
// ------------------------------------------------------------------------------------------------

/// STCS minor compaction — merges similarly-sized SSTables within a bucket.
pub struct MinorCompaction;

impl CompactionStrategy for MinorCompaction {
    fn compact(
        &self,
        sstables: &[Arc<SSTable>],
        manifest: &mut Manifest,
        data_dir: &str,
        config: &EngineConfig,
    ) -> Result<Option<CompactionResult>, CompactionError> {
        minor::maybe_compact(sstables, manifest, data_dir, config)
    }
}

/// STCS tombstone compaction — rewrites a single SSTable to drop safe tombstones.
pub struct TombstoneCompaction;

impl CompactionStrategy for TombstoneCompaction {
    fn compact(
        &self,
        sstables: &[Arc<SSTable>],
        manifest: &mut Manifest,
        data_dir: &str,
        config: &EngineConfig,
    ) -> Result<Option<CompactionResult>, CompactionError> {
        tombstone::maybe_compact(sstables, manifest, data_dir, config)
    }
}

/// STCS major compaction — full merge of all SSTables.
pub struct MajorCompaction;

impl CompactionStrategy for MajorCompaction {
    fn compact(
        &self,
        sstables: &[Arc<SSTable>],
        manifest: &mut Manifest,
        data_dir: &str,
        config: &EngineConfig,
    ) -> Result<Option<CompactionResult>, CompactionError> {
        major::compact(sstables, manifest, data_dir, config)
    }
}
