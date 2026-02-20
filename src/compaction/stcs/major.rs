//! Major compaction — full merge of all SSTables.
//!
//! Merges **every** SSTable into a single new SSTable, applying range
//! tombstones actively to suppress covered point entries. Since the
//! entire dataset is being merged, all spent tombstones (both point
//! and range) are dropped from the output — there's nothing left for
//! them to suppress.
//!
//! ## Option B implementation
//!
//! During the merge, each point entry is checked against the collected
//! range tombstones. If a Put has a lower LSN than a covering range
//! tombstone, it is suppressed (not written to the output).
//!
//! After all entries are processed:
//! - Point tombstones (Delete) are dropped entirely — the corresponding
//!   Put (if any) has already been suppressed or isn't present.
//! - Range tombstones are dropped entirely — all covered data was
//!   suppressed during the merge.

use crate::compaction::{
    CompactionError, CompactionResult, MergeIterator, finalize_compaction, full_range_scan_iters,
};
use crate::engine::EngineConfig;
use crate::engine::RangeTombstone;
use crate::engine::utils::Record;
use crate::manifest::Manifest;
use crate::sstable::{PointEntry, SSTable};
use tracing::{debug, info, trace};

// ------------------------------------------------------------------------------------------------
// Public API
// ------------------------------------------------------------------------------------------------

/// Executes a major compaction, merging all SSTables into one.
///
/// This is always user-triggered (via `Engine::major_compact()`). It
/// will refuse to run if there are fewer than 2 SSTables.
///
/// Returns `Ok(None)` if nothing to compact (0–1 SSTables).
pub fn compact(
    sstables: &[SSTable],
    manifest: &mut Manifest,
    data_dir: &str,
    _config: &EngineConfig,
) -> Result<Option<CompactionResult>, CompactionError> {
    if sstables.len() < 2 {
        debug!(
            sstable_count = sstables.len(),
            "major compaction: fewer than 2 SSTables, skipping"
        );
        return Ok(None);
    }

    let ids: Vec<u64> = sstables.iter().map(|s| s.id()).collect();
    info!(
        sstable_count = sstables.len(),
        ?ids,
        "major compaction: starting full merge"
    );

    let result = execute(sstables, manifest, data_dir)?;

    info!(
        new_sst_id = ?result.new_sst_id,
        removed_count = result.removed_ids.len(),
        "major compaction: complete"
    );

    Ok(Some(result))
}

// ------------------------------------------------------------------------------------------------
// Execution
// ------------------------------------------------------------------------------------------------

fn execute(
    sstables: &[SSTable],
    manifest: &mut Manifest,
    data_dir: &str,
) -> Result<CompactionResult, CompactionError> {
    let sst_refs: Vec<&SSTable> = sstables.iter().collect();
    let removed_ids: Vec<u64> = sstables.iter().map(|s| s.id()).collect();

    // Phase 1: Collect all range tombstones upfront from all SSTables.
    // We need them before processing point entries so we can check coverage.
    let mut all_range_tombstones: Vec<RangeTombstone> = Vec::new();
    for sst in sstables {
        all_range_tombstones.extend(sst.range_tombstone_iter());
    }

    // Phase 2: Create merge iterator over all SSTables.
    let iters = full_range_scan_iters(&sst_refs)?;
    let merge_iter = MergeIterator::new(iters);

    // Phase 3: Process records — dedup point entries, apply range tombstones,
    // drop all tombstones.
    let mut point_entries: Vec<PointEntry> = Vec::new();
    let mut last_key: Option<Vec<u8>> = None;

    for record in merge_iter {
        match record {
            Record::RangeDelete { .. } => {
                // In major compaction, range tombstones are dropped entirely.
                // Their effect was applied when we suppressed covered Puts below.
            }
            Record::Delete { key, lsn, .. } => {
                // Dedup: skip older versions.
                if last_key.as_ref() == Some(&key) {
                    continue;
                }
                last_key = Some(key.clone());
                // Point deletes are dropped in major compaction — the covered
                // Put (if any) was already suppressed or isn't present in any
                // SSTable.
                trace!(key = ?key, lsn, "major: dropping point tombstone");
            }
            Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                // Dedup: skip older versions.
                if last_key.as_ref() == Some(&key) {
                    continue;
                }
                last_key = Some(key.clone());

                // Check if this Put is suppressed by a range tombstone with
                // higher LSN.
                if is_suppressed_by_range(&key, lsn, &all_range_tombstones) {
                    trace!(key = ?key, lsn, "major: Put suppressed by range tombstone");
                    continue;
                }

                point_entries.push(PointEntry {
                    key,
                    value: Some(value),
                    lsn,
                    timestamp,
                });
            }
        }
    }

    // Major compaction produces no tombstones in the output.
    finalize_compaction(manifest, data_dir, removed_ids, point_entries, Vec::new())
}

// ------------------------------------------------------------------------------------------------
// Range tombstone helpers
// ------------------------------------------------------------------------------------------------

/// Returns `true` if the given key+lsn is suppressed by any range
/// tombstone with a strictly higher LSN.
fn is_suppressed_by_range(key: &[u8], put_lsn: u64, range_tombstones: &[RangeTombstone]) -> bool {
    for rt in range_tombstones {
        if key >= rt.start.as_slice() && key < rt.end.as_slice() && rt.lsn > put_lsn {
            return true;
        }
    }
    false
}
