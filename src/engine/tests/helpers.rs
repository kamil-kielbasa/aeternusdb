use crate::engine::{Engine, EngineConfig};
use std::path::Path;
use tracing_subscriber::EnvFilter;

/// Initialize tracing subscriber controlled by `RUST_LOG` env var.
/// Safe to call multiple times — only the first call takes effect.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// Standard config for tests that should NOT trigger SSTable flushes.
pub fn memtable_only_config() -> EngineConfig {
    init_tracing();
    EngineConfig {
        write_buffer_size: 64 * 1024, // 64KB — large enough to avoid flushes
        compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
        bucket_low: 0.5,
        bucket_high: 1.5,
        min_sstable_size: 1024,
        min_threshold: 4,
        max_threshold: 32,
        tombstone_ratio_threshold: 0.2,
        tombstone_compaction_interval: 3600,
        tombstone_bloom_fallback: false,
        tombstone_range_drop: false,
        thread_pool_size: 2,
    }
}

/// Small write buffer that triggers memtable freezing / SSTable flushing quickly.
pub fn small_buffer_config() -> EngineConfig {
    init_tracing();
    EngineConfig {
        write_buffer_size: 128,
        compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
        bucket_low: 0.5,
        bucket_high: 1.5,
        min_sstable_size: 1024,
        min_threshold: 4,
        max_threshold: 32,
        tombstone_ratio_threshold: 0.2,
        tombstone_compaction_interval: 3600,
        tombstone_bloom_fallback: false,
        tombstone_range_drop: false,
        thread_pool_size: 2,
    }
}

/// Standard 4KB config (matches original basic_tests).
pub fn default_config() -> EngineConfig {
    init_tracing();
    EngineConfig {
        write_buffer_size: 4096,
        compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
        bucket_low: 0.5,
        bucket_high: 1.5,
        min_sstable_size: 1024,
        min_threshold: 4,
        max_threshold: 32,
        tombstone_ratio_threshold: 0.2,
        tombstone_compaction_interval: 3600,
        tombstone_bloom_fallback: false,
        tombstone_range_drop: false,
        thread_pool_size: 2,
    }
}

/// 1KB write buffer — produces ~1KB+ SSTables, guaranteed multiple with moderate data.
pub fn multi_sstable_config() -> EngineConfig {
    init_tracing();
    EngineConfig {
        write_buffer_size: 1024,
        compaction_strategy: crate::compaction::CompactionStrategyType::Stcs,
        bucket_low: 0.5,
        bucket_high: 1.5,
        min_sstable_size: 1024,
        min_threshold: 4,
        max_threshold: 32,
        tombstone_ratio_threshold: 0.2,
        tombstone_compaction_interval: 3600,
        tombstone_bloom_fallback: false,
        tombstone_range_drop: false,
        thread_pool_size: 2,
    }
}

/// Helper: open engine, put enough data to force at least one SSTable flush.
/// Returns the engine with data already flushed to SSTables.
pub fn engine_with_sstables(path: &Path, num_keys: usize, prefix: &str) -> Engine {
    let engine = Engine::open(path, default_config()).expect("open");
    for i in 0..num_keys {
        let key = format!("{}_{:04}", prefix, i).into_bytes();
        let value = format!("value_with_some_padding_{:04}", i).into_bytes();
        engine.put(key, value).expect("put");
    }
    engine.flush_all_frozen().expect("flush");
    let stats = engine.stats().expect("stats");
    assert!(stats.sstables_count > 0, "Expected SSTables to be created");
    engine
}

/// Helper: open engine with 1KB buffer, put enough data to force at least 2 SSTables.
/// Returns the engine with data spread across multiple SSTables.
pub fn engine_with_multi_sstables(path: &Path, num_keys: usize, prefix: &str) -> Engine {
    let engine = Engine::open(path, multi_sstable_config()).expect("open");
    for i in 0..num_keys {
        let key = format!("{}_{:04}", prefix, i).into_bytes();
        let value = format!("value_with_some_padding_{:04}", i).into_bytes();
        engine.put(key, value).expect("put");
    }
    engine.flush_all_frozen().expect("flush");
    let stats = engine.stats().expect("stats");
    assert!(
        stats.sstables_count >= 2,
        "Expected at least 2 SSTables, got {}",
        stats.sstables_count
    );
    engine
}

/// Helper: force a flush cycle by closing and reopening the engine.
pub fn reopen(path: &Path) -> Engine {
    Engine::open(path, default_config()).expect("reopen")
}

/// Collect scan results into a Vec.
pub fn collect_scan(engine: &Engine, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    engine.scan(start, end).expect("scan").collect()
}
