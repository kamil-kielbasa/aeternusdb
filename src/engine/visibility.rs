//! Visibility filter for merged record streams.
//!
//! [`VisibilityFilter`] wraps a sorted `(key ASC, LSN DESC)` record
//! stream and yields only the **live** key-value pairs, applying point
//! and range tombstone semantics.

use super::{RangeTombstone, Record};

/// Filters a sorted record stream to yield only **visible** key-value pairs.
///
/// Applies point tombstone and range tombstone semantics:
/// - A `Delete` record suppresses the same key in later (lower-LSN) records.
/// - A `RangeDelete` suppresses any `Put` whose key falls within `[start, end)`
///   and whose LSN is lower than the tombstone's LSN.
///
/// The input iterator **must** be sorted by `(key ASC, LSN DESC)` — the order
/// produced by [`MergeIterator`](super::utils::MergeIterator).
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
