//!
//! Memtable module
//!

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::wal::{Wal, WalError};
use thiserror::Error;

const U32_SIZE: usize = std::mem::size_of::<u32>();

#[derive(Debug, Error)]
pub enum MemtableError {
    #[error("WAL error: {0}")]
    WAL(#[from] WalError),

    #[error("Flush required")]
    FlushRequired,

    #[error("Internal error: {0}")]
    Internal(String),
}

pub struct Memtable {
    inner: Arc<RwLock<MemtableInner>>,
    wal: Wal<MemtableRecord>,
    wal_path: String,
}

#[derive(Debug, PartialEq, bincode::Encode, bincode::Decode, Clone)]
struct MemtableEntry {
    value: Option<Vec<u8>>,
    timestamp: u64,
    is_delete: bool,
}

#[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
struct MemtableRecord {
    key: Vec<u8>,
    value: MemtableEntry,
}

struct MemtableInner {
    tree: BTreeMap<Vec<u8>, Vec<MemtableEntry>>,
    approximate_size: usize,
    write_buffer_size: usize,
}

impl Memtable {
    pub fn new<P: AsRef<Path>>(
        wal_path: P,
        max_record_size: Option<u32>,
        write_buffer_size: usize,
    ) -> Result<Self, MemtableError> {
        let wal = Wal::open(&wal_path, max_record_size)?;

        let mut inner = MemtableInner {
            tree: BTreeMap::new(),
            approximate_size: 0,
            write_buffer_size,
        };

        let records = wal.replay_iter()?;
        for record in records {
            let record: MemtableRecord = record?;
            let record_size = U32_SIZE + std::mem::size_of::<MemtableRecord>() + U32_SIZE;

            let key = record.key;
            let value = record.value;

            inner.tree.entry(key).or_insert_with(Vec::new).push(value);

            inner.approximate_size += record_size;
        }

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            wal,
            wal_path: wal_path.as_ref().to_string_lossy().to_string(),
        })
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemtableError> {
        if key.is_empty() || value.is_empty() {
            return Err(MemtableError::Internal("Key or value is empty".to_string()));
        }

        let record_size = std::mem::size_of::<MemtableEntry>() + key.len() + value.len();
        let record = MemtableRecord {
            key,
            value: MemtableEntry {
                value: Some(value),
                timestamp: Self::current_timestamp(),
                is_delete: false,
            },
        };

        let mut guard = self
            .inner
            .write()
            .map_err(|_| MemtableError::Internal("Read-write lock poisoned".into()))?;

        if guard.approximate_size + record_size > guard.write_buffer_size {
            return Err(MemtableError::FlushRequired);
        }

        // 1. Wal first (crash safety)
        self.wal.append(&record)?;

        // 2. In-memory update
        let key = record.key;
        let value = record.value;

        guard.tree.entry(key).or_insert_with(Vec::new).push(value);

        guard.approximate_size += record_size;

        Ok(())
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<(), MemtableError> {
        if key.is_empty() {
            return Err(MemtableError::Internal("Key is empty".to_string()));
        }

        let record_size = std::mem::size_of::<MemtableEntry>() + key.len();
        let record = MemtableRecord {
            key,
            value: MemtableEntry {
                value: None,
                timestamp: Self::current_timestamp(),
                is_delete: true,
            },
        };

        let mut guard = self
            .inner
            .write()
            .map_err(|_| MemtableError::Internal("Read-write lock poisoned".into()))?;

        if guard.approximate_size + record_size > guard.write_buffer_size {
            return Err(MemtableError::FlushRequired);
        }

        // 1. Wal first (crash safety)
        self.wal.append(&record)?;

        // 2. In-memory update
        let key = record.key;
        let value = record.value;

        guard.tree.entry(key).or_insert_with(Vec::new).push(value);

        guard.approximate_size += record_size;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemtableError> {
        let guard = self
            .inner
            .read()
            .map_err(|_| MemtableError::Internal("Read-write lock poisoned".into()))?;

        Ok(guard
            .tree
            .get(key)
            .and_then(|versions| versions.last())
            .filter(|e| !e.is_delete)
            .and_then(|e| e.value.clone()))
    }

    pub fn flush(&self) -> Result<impl Iterator<Item = (Vec<u8>, MemtableEntry)>, MemtableError> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| MemtableError::Internal("Read-write lock poisoned".into()))?;

        let old_tree = std::mem::take(&mut guard.tree);
        guard.approximate_size = 0;

        Ok(old_tree
            .into_iter()
            .filter_map(|(key, versions)| versions.last().cloned().map(|latest| (key, latest))))
    }

    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_nanos() as u64
    }
}
