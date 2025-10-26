# Database Design and Architecture

## Overview
This document outlines the high-level design and architecture of a single-node Log-Structured Merge-Tree (LSM-tree) database implemented in Rust, utilizing the Size-Tiered Compaction Strategy (STCS). The system functions as a key-value store optimized for write-heavy workloads, supporting deletions via tombstones, with optimizations such as bloom filters for key presence checks and a TTL mechanism (`gc_grace_seconds`) for tombstone management.

The design is optimized for single-node operation, with replication noted as a future feature. It prioritizes durability, crash recovery, and write efficiency.

## Glossary

| Term             | Definition                                                                                                                |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **LSM-tree**     | Log-Structured Merge-Tree: a write-optimized data structure that appends data to logs and merges sorted files (SSTables). |
| **SSTable**      | Sorted String Table: immutable, on-disk file containing sorted key-value pairs and tombstones.                            |
| **WAL**          | Write-Ahead Log: append-only log for durability and crash recovery.                                                       |
| **Manifest**     | Metadata file tracking all SSTables and system state.                                                                     |
| **Memtable**     | In-memory buffer (e.g., balanced tree, e.g., B-tree) for recent writes; flushed to disk when full.                        |
| **Tombstone**    | Deletion marker for a key, with a timestamp (`local_delete_time`).                                                        |
| **Bloom Filter** | Probabilistic structure to test if a key might exist in an SSTable.                                                       |
| **STCS**         | Size-Tiered Compaction Strategy: groups SSTables by size for merging.                                                     |
| **Cell**         | Basic unit of data in SSTables, consisting of key, value, timestamp, flags, and checksum.                                 |

## Configuration Parameters

| Parameter                        | Description                                                                                     | Default       | Notes                                                             |
| -------------------------------- | ----------------------------------------------------------------------------------------------- | ------------- | ----------------------------------------------------------------- |
| `write_buffer_size`              | Max memtable size (MB) before flush; threshold for oversized records.                           | 50            | Aligns with SSTable size; handles large records via direct spill. |
| `bucket_low`                     | Lower bound multiplier for bucket size range ([avg × bucket_low, avg × bucket_high]).           | 0.5           | Lower values widen buckets, reducing count.                       |
| `bucket_high`                    | Upper bound multiplier for bucket size range.                                                   | 1.5           | Higher values reduce cascading compactions.                       |
| `min_sstable_size`               | Min size (MB) for regular buckets; smaller go to "small" bucket.                                | 50            | Matches `write_buffer_size`.                                      |
| `min_threshold`                  | Min SSTables in bucket for minor compaction.                                                    | 4             | Higher delays compaction.                                         |
| `max_threshold`                  | Max SSTables per minor compaction.                                                              | 32            | Caps I/O load.                                                    |
| `tombstone_threshold`            | Ratio of droppable tombstones to trigger tombstone compaction.                                  | 0.2           | 20% garbage triggers cleanup.                                     |
| `tombstone_compaction_interval`  | Min SSTable age (seconds) for tombstone compaction.                                             | 86,400        | Reduce for faster cleanup.                                        |
| `gc_grace_seconds`               | TTL (seconds) for tombstones; droppable if current_time > local_delete_time + gc_grace_seconds. | 3,600         | Essential for crash recovery and safe partial compactions.        |
| `local_delete_time`              | Timestamp (Unix seconds) of tombstone creation.                                                 | Set on delete | Used with `gc_grace_seconds`.                                     |
| `unchecked_tombstone_compaction` | Skip pre-checks for tombstone compaction.                                                       | false         | Enable for aggressive cleanup.                                    |
| `bloom_fallback_scan`            | Perform full SSTable scan if bloom says "maybe" for tombstone drop.                             | false         | Expensive; use for high-ratio SSTables.                           |

**Why `gc_grace_seconds` is Essential**:
In single-node mode, it prevents data resurrection during crash recovery (WAL replay) or partial compactions by ensuring tombstones persist until older data is consolidated, maintaining deletion consistency.

---

## Database Design and Architecture

### Components

#### In-Memory (RAM)

* **Memtable**: Buffers writes/deletes; flushed when size exceeds `write_buffer_size`.
* **Page Cache**: Optional LRU cache for SSTable blocks (future).

#### On-Disk

* **WAL**: Appends operations for durability.
* **SSTables**: Immutable sorted files with key-value pairs, tombstones, metadata (size, creation time, tombstone count, key range), and persisted bloom filters.
* **Manifest**: Tracks SSTables and system state.

### Core Flow

1. **Write Path**

   * Append to WAL (key, value, timestamp, type: put/delete).
   * If record size ≤ `write_buffer_size`: Insert into memtable.
   * If record size > `write_buffer_size`:

     * Flush current memtable (if non-empty).
     * Write record to a special SSTable (single-entry, with bloom filter and metadata).
     * Add to manifest.
   * Acknowledge success.

2. **Read Path**

   * Query memtable, then SSTables (newest first) using bloom filters.
   * Merge results (latest wins).

3. **Compaction**

   * Background merges to consolidate SSTables and drop tombstones (see Compaction Scenarios).

4. **Recovery**

   * Replay WAL to rebuild memtable; load manifest.

### Architecture Diagram

```text
+-------------------+
|    Client API     |
|  PUT/GET/DELETE   |
+-------------------+
         |
         v
+-------------------+    +-------------------+
|       WAL         |--->|    Memtable       |
|   (Append-Only)   |    | (Balanced Tree,   |
|                   |    |  e.g., B-tree)    |
+-------------------+    +-------------------+
                            | Flush
                            v
+-------------------+    +-------------------+
|     SSTables      |<---|     Manifest      |
| (Immutable +      |    | (SSTable Metadata)|
|  Bloom Filters)   |    +-------------------+
|                   |<---| Compaction        |
+-------------------+    | Scheduler         |
         ^              +-------------------+
         | Read
         | (Miss)
+-------------------+
|    Page Cache     |
|      (LRU)        |
+-------------------+
         ^
         | Crash
         | Recovery
+-------------------+
|  Crash Recovery   |
| (WAL Replay,      |
|  Manifest Load)   |
+-------------------+
```

### Data Layout and Serialization

* **Cell format:**
```
+-------------------+-------------------+-------------------+-------------------+
| key_length (u32)  | key bytes         | value_length (u32)| value bytes       |
+-------------------+-------------------+-------------------+-------------------+
| timestamp (u64)   | flags (u8)        | checksum (u32)                        |
+-------------------+-----------------------------------------------------------+
```
* **Flags:** 0x01 = value, 0x02 = tombstone, 0x04 = TTL expired (optional)
* **Serialization:** Byte arrays (bincode), CRC32 checksum, optional compression (Zstd).
* **SSTable:**
```
+-------------------------------+
| Header (version, creation_ts) |
| Bloom filter metadata         |
| Data blocks (cells)           |
| Index (key -> block offset)   |
| Footer (checksum, offsets)    |
+-------------------------------+
```

### Data Consistency and Ordering

* WAL append ensures write order and durability.
* Memtable flush preserves sequence numbers.
* SSTables flushed in commit order; manifest updates atomic.
* Compaction maintains timestamp order.
* Crash recovery replays WAL; partial/incomplete records ignored.

### Concurrency Model

* Thread-based, synchronized access using `Mutex`, `Arc`, `RwLock`, and `Condvar`.
* Single writer thread for WAL/memtable operations.
* Background compaction threads for merging SSTables.
* Readers acquire snapshot-based read locks; no blocking of writers.
* No async runtime used; all concurrency is via threads.

## Compaction Scenarios

Using example SSTables (64 B, 128 B, 512 B, 1 KiB, 4 KiB, 8 KiB, 16 KiB, 32 KiB, 64 KiB, 128 KiB, 256 KiB, 512 KiB) with buckets (scaled to small units for illustration; e.g., tiny flushes at 64 B merging to larger):
- Bucket 1: [64 B]
- Bucket 2: [128 B]
- Bucket 3: [512 B, 1 KiB]
- Bucket 4: [4 KiB]
- Bucket 5: [8 KiB, 16 KiB, 32 KiB]
- Bucket 6: [64 KiB, 128 KiB, 256 KiB]
- Bucket 7: [512 KiB]

### Scenario 1: Minor Compaction (Size-Tiered, Bucket-Based)

* **Trigger:** Bucket has ≥ `min_threshold=4` SSTables.
* **Selection:** Up to `max_threshold=32` SSTables.
* **Execution:**

  * Merge iterator over selected SSTables.
  * For each key:

    * Keep latest entry.
    * If tombstone expired, check bloom filters of older SSTables; drop if safe.
    * Else keep value or tombstone.
  * Write new SSTable (~bucket-size).
  * Atomic update.
* **Post-Execution:** Re-bucket and update metrics.

### Scenario 2: Tombstone-Specific Compaction

- **Trigger**: SSTable ratio > `tombstone_threshold=0.2` and age ≥ `tombstone_compaction_interval=86400`.
- **Selection**: Single SSTable (e.g., 16 KiB).
- **Execution**:
  - Read stream.
  - For each key:
    - Keep latest.
    - If latest is tombstone and grace expired:
      - Check bloom filters of older SSTables.
      - If none indicate key, drop.
      - If "maybe" and `bloom_fallback_scan=true` → scan; else keep.
    - If value, keep it, discard older.
  - Write new SSTable (~12 KiB).
  - **False Positives**: Bloom filters might retain tombstones unnecessarily. **Refinement**: For high-ratio SSTables, add optional full SSTable scan if bloom says "maybe" (expensive, toggle via `bloom_fallback_scan`).
- **Post-Execution**: Re-bucket; re-trigger if needed.

### Scenario 3: Major Compaction (Full, Manual)

- **Trigger**: User-initiated via API call.
- **Selection**: All SSTables.
- **Execution**:
  - Full merge of all SSTables.
  - Drop tombstones if grace expired (no bloom needed — full set is complete).
  - Write one or more new SSTables.
- **Post-Execution**: Replace all in manifest.

## Implementation Steps

1. Pure Storage Engine: Memtable, WAL, SSTable flush, manifest, read/write.
2. Concurrency: Background compaction threads, locks for manifest/memtable.
3. Caching: LRU page cache for SSTable blocks.
4. Multiple Tables: per-table WAL/manifest/config.
5. Query Language: CLI/API with PUT, GET, DELETE, SCAN.

## Minimal API / Interface Sketch

### Rust API

```rust
let mut db = Database::open("/var/lib/mydb")?;
db.put(b"user:1", b"John Doe")?;
let value = db.get(b"user:1")?;
db.delete(b"user:1")?;
```

### CLI API

```bash
$ mydb put user:1 "John Doe"
$ mydb get user:1
John Doe
$ mydb delete user:1
```

## Limitations

* STCS may lead to cascading compactions if bucket count grows high; monitor and tune `bucket_high`.
* Single-node limits scalability; replication/sharding needed for high availability.
* Oversized records increase SSTable count; mitigate with larger `write_buffer_size`.

## Missing Components (for Production Readiness)

* Testing: Unit, integration, property-based (`proptest`)
* Benchmarking: `criterion`, throughput, latency
* Error handling: Custom enums (`DbError`)
* Configuration: TOML/YAML + `serde`
* Metrics/Telemetry: `tracing`, Prometheus
* Documentation: `/docs`, diagrams, examples
* CI/CD: GitHub Actions, linting
* Examples/Tools: CLI utilities, SSTable inspectors

---