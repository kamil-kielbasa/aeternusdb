//! # AeternusDB
//!
//! An embeddable, persistent key-value storage engine built on a
//! **Log-Structured Merge Tree (LSM-tree)** architecture. Designed for
//! fast writes and crash-safe operation.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │                     Engine                           │
//! │  ┌────────────┐  ┌──────────────┐  ┌─────────────┐  │
//! │  │  Active     │  │   Frozen     │  │  SSTables   │  │
//! │  │  Memtable   │  │  Memtables   │  │  (on disk)  │  │
//! │  │  + WAL      │  │  + WALs      │  │             │  │
//! │  └─────┬───────┘  └──────┬───────┘  └──────┬──────┘  │
//! │        │   freeze        │   flush         │         │
//! │        └─────────►       └────────►        │         │
//! │                                            │         │
//! │  ┌──────────────────────────────────────────┘         │
//! │  │  Compaction (minor / tombstone / major)           │
//! │  └───────────────────────────────────────────────────┘│
//! │                                                      │
//! │  ┌──────────────────────────────────────────────────┐ │
//! │  │              Manifest (WAL + snapshot)           │ │
//! │  └──────────────────────────────────────────────────┘ │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! ## Modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`engine`] | Core storage engine — open, read, write, scan, flush, compact |
//! | [`memtable`] | In-memory write buffer with multi-version entries and range tombstones |
//! | [`wal`] | Generic, CRC-protected write-ahead log for crash recovery |
//! | [`sstable`] | Immutable, sorted, on-disk tables with bloom filters and block indices |
//! | [`manifest`] | Persistent metadata manager (WAL + snapshot model) |
//! | [`compaction`] | Size-tiered, tombstone, and major compaction strategies |
//!
//! ## Key Features
//!
//! - **Write-ahead logging** — every mutation is persisted to a WAL before
//!   being acknowledged, guaranteeing durability and crash recovery.
//! - **Multi-version concurrency** — multiple versions per key, ordered by
//!   log sequence number (LSN). Reads always see the latest committed version.
//! - **Point and range tombstones** — efficient delete semantics for both
//!   individual keys and key ranges.
//! - **Bloom filter lookups** — each SSTable carries a bloom filter for
//!   fast negative point-lookup responses.
//! - **Block-level CRC32 integrity** — every on-disk block (WAL records,
//!   SSTable data blocks, headers, footers) is checksummed.
//! - **Pluggable compaction** — three strategies (minor, tombstone, major)
//!   with configurable thresholds.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use aeternusdb::engine::{Engine, EngineConfig};
//! use aeternusdb::compaction::CompactionStrategyType;
//!
//! let config = EngineConfig {
//!     write_buffer_size: 4096,
//!     compaction_strategy: CompactionStrategyType::Stcs,
//!     bucket_low: 0.5,
//!     bucket_high: 1.5,
//!     min_sstable_size: 50,
//!     min_threshold: 4,
//!     max_threshold: 32,
//!     tombstone_ratio_threshold: 0.3,
//!     tombstone_compaction_interval: 0,
//!     tombstone_bloom_fallback: true,
//!     tombstone_range_drop: true,
//!     thread_pool_size: 2,
//! };
//!
//! let engine = Engine::open("/tmp/my_db", config).unwrap();
//!
//! // Write
//! engine.put(b"hello".to_vec(), b"world".to_vec()).unwrap();
//!
//! // Read
//! assert_eq!(engine.get(b"hello".to_vec()).unwrap(), Some(b"world".to_vec()));
//!
//! // Delete
//! engine.delete(b"hello".to_vec()).unwrap();
//! assert_eq!(engine.get(b"hello".to_vec()).unwrap(), None);
//!
//! // Scan
//! engine.put(b"a".to_vec(), b"1".to_vec()).unwrap();
//! engine.put(b"b".to_vec(), b"2".to_vec()).unwrap();
//! let results: Vec<_> = engine.scan(b"a", b"c").unwrap().collect();
//!
//! // Graceful shutdown
//! engine.close().unwrap();
//! ```

#![allow(dead_code)]

pub mod compaction;
pub mod engine;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod wal;
