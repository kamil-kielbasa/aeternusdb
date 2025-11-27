# Manifest Complete Guide

## 1. Overview

The manifest is the authoritative metadata log for the storage engine. It tracks:
* Which SSTables currently exist and are part of the live dataset
* Each SSTable's metadata (key ranges, LSN ranges, sequence numbers, level, size, etc.)
* Compaction events (add new SSTables, remove compacted ones)
* Checkpoints snapshots of the manifest to speed startup

The manifest guarantees crash-safe atomicity across flushes and compactions so that the storage engine can always reconstruct a consistent set of SSTables on restart.

The manifest works together with:
* WAL (Write-Ahead Log) — durable record of unflushed mutations
* Memtable — in-memory state built from WAL
* SSTables — immutable disk structures created from memtable flushes or compaction outputs

Correct ordering between these components is essential for durability.

## 2. Why the Manifest Exists

Without a manifest, SSTables created during a flush or compaction have no authoritative metadata telling the system:
* whether they are valid
* whether they belong to the current LSM tree
* whether they are obsolete
* whether they were fully created before a crash

Because SSTables are immutable and produced frequently, the database must remember which SSTables form the current dataset, which were removed by compaction, and which are new.

The manifest acts as a WAL of metadata, giving the DB a way to:
* reconstruct the set of live SSTables on startup
* detect orphan or incomplete SSTables
* ensure that no data is lost even if the crash happens at the worst moment

## 3. SSTable Publication: Principles

An SSTable should become visible to the system only when it is:
1. fully written
2. fsynced
3. atomically renamed
4. directory-fsynced
5. added to the manifest WAL
6. the manifest WAL is fsynced

Only then is it safe to drop WAL entries that contributed to this SSTable.

These ordering rules prevent:
* publishing incomplete data
* losing data during crashes
* leaving untracked orphan SSTables

## 4. Durable Flush Workflow (Safe Ordering)

This section describes the canonical safe sequence for producing a new SSTable from a memtable. The ordering must be followed for correctness.

### Step 1 — Create SSTable to a temp file

Write to:

```bash
sstables/sst-<id>.tmp
```

Write data blocks, bloom filter, index, footer, header. Do not expose the file yet.

### Step 2 — Flush buffers and fsync the file

```rust
writer.flush()?;
file.sync_all()?;
```

Ensures contents are durable.

### Step 3 — Atomically rename temp → final

```rust
rename("sst-<id>.tmp", "sst-<id>.sst")
```

POSIX `rename` is atomic.

### Step 4 — fsync the directory

Ensures rename survives power loss:

```rust
dir.sync_all()?;
```

### Step 5 — Append `ADD sstable` to manifest WAL

Write a manifest WAL record containing:
* filename (`sst-<id>.sst`)
* min_lsn, max_lsn
* min_key, max_key
* size, checksum, creation time
* compaction level

This record is appended to the manifest WAL (append-only log).

### Step 6 — fsync manifest WAL

Durably persist the metadata change:

```rust
manifest_wal.sync_all()?;
```

### Step 7 — Truncate or rotate memtable WAL

Only now is it safe to remove WAL records that were flushed, because:
* SSTable exists durably
* Manifest durably references it

### Step 8 — Optional: Periodic Manifest Snapshot

To reduce startup time, occasionally write a full manifest snapshot and then truncate manifest WAL up to that snapshot.

## 5. Startup Recovery Procedure

On process restart, the database reconstructs the correct set of SSTables by following this sequence:

### 1. Load manifest snapshot (if exists)

This gives a base state of all SSTables known as of the last checkpoint.

### 2. Replay manifest WAL

Replay all metadata records newer than the snapshot. This reconstructs:
* which SSTables are live
* which were deleted
* compaction outputs
* levels
* LSN boundaries

### 3. List all SSTable files on disk

Walk the SSTable directory and classify each file:
* If referenced by manifest → load it
* If `.tmp` file → delete it (incomplete write)
* If unreferenced → handle as orphan

### 4. Handle orphan SSTables

Safe policies:
* Delete orphan SSTables that are older than WAL-replay LSN (WAL will recreate them).
* Move to orphan/ directory for manual/auto GC if unsure.
* Import orphan SSTables only with strict rules (rarely needed).

### 5. Replay WAL(s)

Rebuild the memtable by replaying log entries that occurred after:
* the manifest checkpoint's last_lsn, or
* the max_lsn found among SSTables

### 6. Initialize query engine

Load bloom filters, indexes, block metadata, etc., for all SSTables listed in manifest.

## 6. Manifest WAL Design

### Record Types

Typical records stored in manifest WAL:
* `ADD {sstable metadata}`
* `DELETE {filename}`
* `COMPACT {inputs → outputs}`
* `SNAPSHOT {full manifest image}`

### Durable Append Rules

* Append record
* Flush buffers
* `fsync` manifest file
* Optionally fsync manifest directory if a new file/snapshot is created

### Snapshotting Manifest

Periodically produce a compact, full manifest snapshot containing:
* list of all live SSTables
* metadata for each SSTable
* last_applied_lsn

Then:
* fsync snapshot file
* atomically rename into place
* truncate manifest WAL up to snapshot

## 7. LSN Management

The engine must maintain a global, monotonic Log Sequence Number (LSN).

### How LSNs are assigned

* Every user mutation (put/delete/delete_range) gets an LSN.
* Assigned before writing to WAL and Memtable.

### Persistence strategy

LSN does not need its own file. It can be reconstructed on startup from:
* max_lsn in SSTables from manifest
* max_lsn in WAL records

Then:

```rust
next_lsn = max(max_sstable_lsn, max_wal_lsn) + 1
```

### In Manifest Snapshot

Including `last_applied_lsn` in snapshots accelerates recovery because startup only needs to replay WAL entries after this value.

## 8. SSTable Naming & Metadata Recommendation

Use filenames that encode ordering, e.g.:

```bash
sst-<max_lsn>-<uuid>.sst
```

or 

```bash
sst-level-<level>-seq-<sequence>.sst
```

This helps:
* debugging
* orphan detection
* compaction ordering

Every SSTable must store:
* min_key / max_key
* min_lsn / max_lsn
* checksum
* creation timestamp
* bloom block
* index block
* range tombstones (if present)
* block offsets
* footer with full-file checksum

## 9. Compaction and Manifest Interaction

Compaction creates new SSTables and deletes old ones. Safe sequence mirrors flush:

1. Write new SSTables to temp files
2. fsync files
3. rename + fsync directory
4. Append manifest WAL record:

```rust
COMPACT {
    deleted: [sst1, sst2, ...],
    added:   [sst_new1, sst_new2]
}
```

5. fsync manifest WAL
6. Update in-memory manifest
7. Delete old SSTables only after manifest WAL is durable

This ensures that after a crash:
* old files are still referenced (unless deletion was durable)
* new SSTables are visible only if they were published and manifest-durable

## 10. Orphan SSTable Handling

On startup, SSTables that exist but are not listed in manifest are orphans. Typical policy:

### Delete `.tmp` files

These are always incomplete.

### Delete or quarantine final-name orphan SSTables

A simple safe rule:

If an orphan SSTable's `max_lsn` is ≤ highest-WAL-LSN, then WAL replay will reproduce its contents → it is safe to delete.

Otherwise:
* move to `orphan/`
* or import only under strict conditions

## 11. Correctness Guarantees from This Design

### Atomicity

Either an SSTable is fully published and in the manifest, or it is ignored.

### Durability

A flush is durable once:
* SSTable is fsynced
* rename is fsynced
* manifest ADD is fsynced

### Crash Safety

No crash can:
* expose partial SSTables
* lose data that was previously acknowledged
* produce inconsistent SSTable sets

### Idempotent Restart

Startup replay reconstructs all state deterministically from:
* manifest snapshot
* manifest WAL
* remaining SSTables
* WAL records
