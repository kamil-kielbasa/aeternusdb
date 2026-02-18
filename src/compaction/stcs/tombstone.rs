//! Tombstone compaction — per-SSTable garbage collection.
//!
//! Rewrites a single SSTable to remove tombstones that are provably
//! unnecessary:
//!
//! **Point tombstones:** A point delete `Delete(key)` can be dropped when
//! no other SSTable *could* contain a live version of `key`.
//! - Bloom filter check across all *other* SSTables.
//!   - If no bloom says "maybe" → safe to drop.
//!   - If bloom says "maybe" and `tombstone_bloom_fallback` is enabled → do
//!     actual `get()` to resolve the false positive.
//!
//! **Range tombstones:** A range tombstone `[start, end)` can be dropped when
//! `tombstone_range_drop` is enabled and scanning all older SSTables
//! confirms that no live keys exist within that range.

use crate::compaction::{CompactionError, CompactionResult, finalize_compaction};
use crate::engine::EngineConfig;
use crate::manifest::Manifest;
use crate::sstable::{
    MemtablePointEntry, MemtableRangeTombstone, SSTGetResult, SSTable, SSTableError,
};
use tracing::{debug, info, trace};

// ------------------------------------------------------------------------------------------------
// Public API
// ------------------------------------------------------------------------------------------------

/// Selects an SSTable eligible for tombstone compaction and executes it.
///
/// Returns `Ok(Some(result))` if compaction was performed, or
/// `Ok(None)` if no SSTable was eligible.
pub fn maybe_compact(
    sstables: &[SSTable],
    manifest: &mut Manifest,
    data_dir: &str,
    config: &EngineConfig,
) -> Result<Option<CompactionResult>, CompactionError> {
    let target_idx = match select_candidate(sstables, config) {
        Some(idx) => idx,
        None => {
            debug!(
                sstable_count = sstables.len(),
                "tombstone compaction: no candidate met threshold"
            );
            return Ok(None);
        }
    };

    let target = &sstables[target_idx];
    let tombstone_total =
        target.properties.tombstone_count + target.properties.range_tombstones_count;
    info!(
        target_id = target.id,
        tombstone_count = tombstone_total,
        record_count = target.properties.record_count,
        "tombstone compaction: starting rewrite"
    );

    let result = execute(sstables, target_idx, manifest, data_dir, config)?;

    // If execute() found a candidate but could not drop any tombstones,
    // the result has empty removed_ids.  Treat that as "nothing to do"
    // so the caller's `while compact() {}` loop terminates.
    if result.removed_ids.is_empty() {
        debug!(
            target_id = target.id,
            "tombstone compaction: candidate selected but no tombstones could be dropped"
        );
        return Ok(None);
    }

    info!(
        new_sst_id = ?result.new_sst_id,
        removed_count = result.removed_ids.len(),
        "tombstone compaction: complete"
    );

    Ok(Some(result))
}

// ------------------------------------------------------------------------------------------------
// Selection
// ------------------------------------------------------------------------------------------------

/// Selects the single best SSTable for tombstone compaction.
///
/// Picks the SSTable with the highest tombstone ratio that exceeds
/// `config.tombstone_ratio_threshold` and meets the minimum age.
fn select_candidate(sstables: &[SSTable], config: &EngineConfig) -> Option<usize> {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut best: Option<(usize, f64)> = None;

    for (i, sst) in sstables.iter().enumerate() {
        let props = &sst.properties;

        // Check age requirement.
        let creation_secs = props.creation_timestamp / 1_000_000_000; // nanos → secs
        let age_secs = now_secs.saturating_sub(creation_secs);
        if age_secs < config.tombstone_compaction_interval as u64 {
            continue;
        }

        // Total tombstones (point + range).
        let tombstone_total = props.tombstone_count + props.range_tombstones_count;
        if tombstone_total == 0 {
            continue;
        }

        let ratio = tombstone_total as f64 / props.record_count.max(1) as f64;
        if ratio < config.tombstone_ratio_threshold {
            continue;
        }

        match &best {
            Some((_, best_ratio)) if ratio <= *best_ratio => {}
            _ => {
                best = Some((i, ratio));
            }
        }
    }

    best.map(|(idx, _)| idx)
}

// ------------------------------------------------------------------------------------------------
// Execution
// ------------------------------------------------------------------------------------------------

/// Rewrites the target SSTable, dropping tombstones that are provably safe
/// to remove.
fn execute(
    sstables: &[SSTable],
    target_idx: usize,
    manifest: &mut Manifest,
    data_dir: &str,
    config: &EngineConfig,
) -> Result<CompactionResult, CompactionError> {
    let target = &sstables[target_idx];
    // Only check SSTables that are **older** (lower ID) than the target.
    // A tombstone only needs to suppress data in older SSTables — if a
    // newer SSTable has the same key, that version already shadows the
    // tombstone's target and the tombstone is irrelevant to it.
    let older_sstables: Vec<&SSTable> = sstables
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != target_idx && sstables[*i].id < target.id)
        .map(|(_, s)| s)
        .collect();

    // Full scan of the target SSTable.
    let min_key = target.properties.min_key.clone();
    let mut max_key = target.properties.max_key.clone();
    max_key.push(0xFF);

    let scan_iter = target.scan(&min_key, &max_key)?;

    let mut point_entries: Vec<MemtablePointEntry> = Vec::new();
    let mut range_tombstones: Vec<MemtableRangeTombstone> = Vec::new();
    // Range tombstones that are candidates for dropping.  We collect
    // them during the scan and resolve them in a second pass once
    // all point entries have been gathered, so we can detect coverage
    // of puts inside the same SSTable.
    let mut range_candidates: Vec<MemtableRangeTombstone> = Vec::new();
    let mut last_key: Option<Vec<u8>> = None;
    let mut dropped_anything = false;

    for record in scan_iter {
        match record {
            crate::engine::utils::Record::Put {
                key,
                value,
                lsn,
                timestamp,
            } => {
                // Dedup: keep only highest LSN per key.
                if last_key.as_ref() == Some(&key) {
                    dropped_anything = true;
                    continue;
                }
                last_key = Some(key.clone());
                point_entries.push(MemtablePointEntry {
                    key,
                    value: Some(value),
                    lsn,
                    timestamp,
                });
            }
            crate::engine::utils::Record::Delete {
                key,
                lsn,
                timestamp,
            } => {
                // Dedup: keep only highest LSN per key.
                if last_key.as_ref() == Some(&key) {
                    dropped_anything = true;
                    continue;
                }
                last_key = Some(key.clone());

                // Can we drop this point tombstone?
                if can_drop_point_tombstone(&key, &older_sstables, config)? {
                    trace!(key = ?key, lsn, "dropping point tombstone — no older data found");
                    dropped_anything = true;
                    continue;
                }

                point_entries.push(MemtablePointEntry {
                    key,
                    value: None,
                    lsn,
                    timestamp,
                });
            }
            crate::engine::utils::Record::RangeDelete {
                start,
                end,
                lsn,
                timestamp,
            } => {
                // Defer the drop decision to a second pass so that we
                // can check collected point_entries for covered puts.
                if config.tombstone_range_drop {
                    range_candidates.push(MemtableRangeTombstone {
                        start,
                        end,
                        lsn,
                        timestamp,
                    });
                } else {
                    range_tombstones.push(MemtableRangeTombstone {
                        start,
                        end,
                        lsn,
                        timestamp,
                    });
                }
            }
        }
    }

    // --- Second pass: resolve range tombstone candidates ---
    //
    // A range tombstone can only be dropped when:
    //   (a) no older SSTable contains live keys in the range, AND
    //   (b) no Put in *this* SSTable falls within the range with a
    //       lower LSN (the tombstone still suppresses it).
    for rt in range_candidates {
        let safe_in_older = can_drop_range_tombstone(&rt.start, &rt.end, rt.lsn, &older_sstables)?;

        let covers_own_puts = safe_in_older
            && point_entries.iter().any(|pe| {
                pe.value.is_some()
                    && pe.key.as_slice() >= rt.start.as_slice()
                    && pe.key.as_slice() < rt.end.as_slice()
                    && pe.lsn < rt.lsn
            });

        if safe_in_older && !covers_own_puts {
            trace!(
                start = ?rt.start, end = ?rt.end, lsn = rt.lsn,
                "dropping range tombstone — no covered keys in older SSTables or same SSTable"
            );
            dropped_anything = true;
        } else {
            range_tombstones.push(rt);
        }
    }

    // If nothing was dropped, no need to rewrite.
    if !dropped_anything {
        return Ok(CompactionResult {
            removed_ids: Vec::new(),
            new_sst_path: None,
            new_sst_id: None,
        });
    }

    let removed_ids = vec![target.id];
    finalize_compaction(
        manifest,
        data_dir,
        removed_ids,
        point_entries,
        range_tombstones,
    )
}

// ------------------------------------------------------------------------------------------------
// Tombstone safety checks
// ------------------------------------------------------------------------------------------------

/// Determines whether a point tombstone for `key` can be safely dropped.
///
/// A tombstone is safe to drop when no other SSTable *could* contain a
/// live version of `key` that this tombstone is suppressing.
fn can_drop_point_tombstone(
    key: &[u8],
    others: &[&SSTable],
    config: &EngineConfig,
) -> Result<bool, SSTableError> {
    for sst in others {
        // Quick bloom filter check.
        if !sst.bloom_may_contain(key) {
            // Bloom definitively says "not present" → this SSTable is safe.
            continue;
        }

        // Bloom says "maybe present".
        if config.tombstone_bloom_fallback {
            // Resolve the false positive via actual get().
            let result = sst.get(key)?;
            match result {
                SSTGetResult::NotFound => continue, // false positive → safe
                _ => return Ok(false),              // actually present → keep tombstone
            }
        } else {
            // Without fallback scan, we must conservatively keep the tombstone.
            return Ok(false);
        }
    }

    // No other SSTable could contain this key → safe to drop.
    Ok(true)
}

/// Determines whether a range tombstone `[start, end)` can be safely dropped.
///
/// Scans all other SSTables for live keys that fall within the range and
/// have an LSN lower than the tombstone's LSN (i.e., keys that this
/// tombstone is actively suppressing).
fn can_drop_range_tombstone(
    start: &[u8],
    end: &[u8],
    tombstone_lsn: u64,
    others: &[&SSTable],
) -> Result<bool, SSTableError> {
    for sst in others {
        // Quick check: does this SSTable's key range overlap with the tombstone?
        if sst.properties.max_key.as_slice() < start || sst.properties.min_key.as_slice() >= end {
            continue; // No overlap.
        }

        // Scan the overlapping range.
        let scan_iter = sst.scan(start, end)?;
        for record in scan_iter {
            match &record {
                crate::engine::utils::Record::Put { lsn, .. }
                | crate::engine::utils::Record::Delete { lsn, .. } => {
                    if *lsn < tombstone_lsn {
                        // There's a live key with lower LSN that this tombstone
                        // is suppressing → cannot drop.
                        return Ok(false);
                    }
                }
                crate::engine::utils::Record::RangeDelete { .. } => {
                    // Range tombstones in other SSTables don't affect safety
                    // of *this* range tombstone.
                }
            }
        }
    }

    // No older live keys found in any other SSTable → safe to drop.
    Ok(true)
}
