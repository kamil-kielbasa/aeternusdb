#[cfg(test)]
mod tests {
    use crate::engine::{Engine, EngineConfig, EngineError};
    use tempfile::TempDir;
    use tracing::Level;
    use tracing_subscriber::fmt::Subscriber;

    fn init_tracing() {
        let _ = Subscriber::builder()
            .with_max_level(Level::TRACE)
            .try_init();
    }

    #[test]
    fn engine_open_fresh_database() {
        init_tracing();
        let temp = TempDir::new().unwrap();

        let config = EngineConfig {
            write_buffer_size: 1024,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 1024,
            min_threshold: 4,
            max_threshold: 32,
            tombstone_threshold: 0.2,
            tombstone_compaction_interval: 3600,
            thread_pool_size: 2,
        };

        let engine = Engine::open(temp.path(), config).expect("Engine open failed");

        // Verify initial state
        assert_eq!(engine.active.max_lsn(), 0);
        assert_eq!(engine.frozen.len(), 0);
        assert_eq!(engine.sstables.len(), 0);
        assert_eq!(engine.data_dir, temp.path().to_string_lossy());
        assert_eq!(engine.config.write_buffer_size, 1024);
        assert_eq!(engine.config.bucket_low, 0.5);
        assert_eq!(engine.config.bucket_high, 1.5);
        assert_eq!(engine.config.min_sstable_size, 1024);
        assert_eq!(engine.config.min_threshold, 4);
        assert_eq!(engine.config.max_threshold, 32);
        assert_eq!(engine.config.tombstone_threshold, 0.2);
        assert_eq!(engine.config.tombstone_compaction_interval, 3600);
        assert_eq!(engine.config.thread_pool_size, 2);
    }
}
