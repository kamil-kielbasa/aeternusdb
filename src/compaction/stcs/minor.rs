//! Minor compaction — size-tiered, bucket-based.
//!
//! Merges a group of similarly-sized SSTables into one, keeping the
//! highest-LSN version of each key. **All tombstones are preserved.**

use super::{bucket_sstables, select_compaction_bucket};
use crate::compaction::{
    CompactionError, CompactionResult, MergeIterator, dedup_records, finalize_compaction,
    full_range_scan_iters,
};
use crate::engine::EngineConfig;
use crate::manifest::Manifest;
use crate::sstable::SSTable;
use tracing::{debug, info};

/// Checks if minor compaction is needed and executes it if so.
///
/// Returns `Ok(Some(result))` if compaction was performed, or
/// `Ok(None)` if no bucket met the threshold.
pub fn maybe_compact(
    sstables: &[SSTable],
    manifest: &mut Manifest,
    data_dir: &str,
    config: &EngineConfig,
) -> Result<Option<CompactionResult>, CompactionError> {
    let buckets = bucket_sstables(sstables, config);
    let selected = match select_compaction_bucket(&buckets, config) {
        Some(s) => s,
        None => {
            debug!(
                sstable_count = sstables.len(),
                "minor compaction: no bucket met threshold"
            );
            return Ok(None);
        }
    };

    let selected_ids: Vec<u64> = selected.iter().map(|&i| sstables[i].id()).collect();
    info!(
        selected_count = selected.len(),
        ?selected_ids,
        "minor compaction: starting merge"
    );

    let result = execute(sstables, &selected, manifest, data_dir)?;

    info!(
        new_sst_id = ?result.new_sst_id,
        removed_count = result.removed_ids.len(),
        "minor compaction: complete"
    );

    Ok(Some(result))
}

/// Executes minor compaction on the selected SSTable indices.
///
/// Merges the selected SSTables into a single new SSTable, deduplicating
/// point entries (keeping highest LSN per key) and preserving all tombstones.
fn execute(
    sstables: &[SSTable],
    selected_indices: &[usize],
    manifest: &mut Manifest,
    data_dir: &str,
) -> Result<CompactionResult, CompactionError> {
    let selected_ssts: Vec<&SSTable> = selected_indices.iter().map(|&i| &sstables[i]).collect();

    let removed_ids: Vec<u64> = selected_ssts.iter().map(|s| s.id()).collect();

    // Streaming merge over all selected SSTables.
    let iters = full_range_scan_iters(&selected_ssts)?;
    let merge_iter = MergeIterator::new(iters);

    // Deduplicate — keeps highest LSN per key, preserves all tombstones.
    let (point_entries, range_tombstones) = dedup_records(merge_iter);

    finalize_compaction(
        manifest,
        data_dir,
        removed_ids,
        point_entries,
        range_tombstones,
    )
}
