//!
//! Memtable module
//!

use crate::wal::{Entry, Wal};
use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, RwLock};

pub struct Memtable {
    inner: Arc<RwLock<MemtableInner>>,
    wal: Wal,
}

struct MemtableInner {
    tree: BTreeMap<Vec<u8>, Vec<Entry>>,
    approximate_size: usize,
    write_buffer_size: usize,
}

impl Memtable {
    pub fn new(write_buffer_size: usize) -> io::Result<Self> {
        todo!();
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> io::Result<String> {
        todo!();
    }

    pub fn delete(&self, key: Vec<u8>) -> io::Result<String> {
        todo!();
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        todo!();
    }

    pub fn is_full(&self) -> bool {
        todo!();
    }

    pub fn flush(&self) -> impl Iterator<Item = (Vec<u8>, Entry)> + '_ {
        std::iter::empty()
    }

    fn current_timestamp() -> u64 {
        todo!();
    }
}
