/// Represents a single item emitted by the storage engine.
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub enum Record {
    /// A concrete key-value pair (point put).
    Put {
        /// The key.
        key: Vec<u8>,

        /// The value associated with the key.
        value: Vec<u8>,

        /// The log sequence number (LSN) of this record.
        lsn: u64,

        /// The timestamp of this record.
        timestamp: u64,
    },

    /// A point deletion of a specific key.
    Delete {
        /// The key to be deleted.
        key: Vec<u8>,

        /// The log sequence number (LSN) of this record.
        lsn: u64,

        /// The timestamp of this record.
        timestamp: u64,
    },

    /// A range tombstone representing deletion of a key interval `[start_key, end_key)`.
    RangeDelete {
        /// Start key of the deleted interval (inclusive).
        start: Vec<u8>,

        /// End key of the deleted interval (exclusive).
        end: Vec<u8>,

        /// The log sequence number (LSN) of this record.
        lsn: u64,

        /// The timestamp of this record.
        timestamp: u64,
    },
}

impl Record {
    pub fn lsn(&self) -> u64 {
        match self {
            Record::Put { lsn, .. } => *lsn,
            Record::Delete { lsn, .. } => *lsn,
            Record::RangeDelete { lsn, .. } => *lsn,
        }
    }

    pub fn key(&self) -> &Vec<u8> {
        match self {
            Record::Put { key, .. } => key,
            Record::Delete { key, .. } => key,
            Record::RangeDelete { start, .. } => start,
        }
    }

    pub fn timestamp(&self) -> u64 {
        match self {
            Record::Put { timestamp, .. } => *timestamp,
            Record::Delete { timestamp, .. } => *timestamp,
            Record::RangeDelete { timestamp, .. } => *timestamp,
        }
    }
}

pub fn record_cmp(a: &Record, b: &Record) -> std::cmp::Ordering {
    match a.key().cmp(b.key()) {
        std::cmp::Ordering::Equal => b.lsn().cmp(&a.lsn()),
        other => other,
    }
}
