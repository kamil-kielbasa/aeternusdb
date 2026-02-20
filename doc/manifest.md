# Manifest

The manifest is the metadata authority for the storage engine. It tracks which WAL
segments and SSTables form the current dataset and ensures crash-safe transitions
during flushes and compactions.

---

## Tracked State

The manifest maintains a single `ManifestData` structure:

| Field          | Type                   | Description                                      |
|----------------|------------------------|--------------------------------------------------|
| `version`      | `u64`                  | Monotonically increasing manifest version        |
| `last_lsn`     | `u64`                  | Last globally assigned LSN                       |
| `active_wal`   | `u64`                  | Current active WAL segment ID                    |
| `frozen_wals`  | `Vec<u64>`             | Frozen WAL segment IDs (awaiting flush)          |
| `sstables`     | `Vec<ManifestSstEntry>`| Live SSTable entries (ID + path)                 |
| `next_sst_id`  | `u64`                  | Next SSTable ID to allocate (monotonically increasing) |
| `dirty`        | `bool`                 | Whether in-memory state differs from snapshot    |

Each SSTable entry (`ManifestSstEntry`) records only:

| Field  | Type      | Description                    |
|--------|-----------|--------------------------------|
| `id`   | `u64`     | Globally unique SSTable ID     |
| `path` | `PathBuf` | Filesystem path to `.sst` file |

Detailed SSTable metadata (key ranges, bloom filters, index) lives inside the
SSTable file itself and is loaded when the engine opens.

---

## Durability Model

Manifest metadata is persisted using a **WAL + periodic snapshot** design:

1. **Manifest WAL** (`000000.log`) — append-only log of `ManifestEvent` records.
   Every mutation is appended and fsynced before the in-memory state is updated.

2. **Manifest snapshot** (`MANIFEST-000001`) — encoded dump of the entire
   `ManifestData` structure with a CRC32 checksum for corruption detection.

3. **Recovery** — on startup the manifest loads the snapshot (if present), then
   replays the WAL to reach the latest consistent state.

```
┌───────────────────────────────────────────┐
│              Manifest Recovery             │
│                                            │
│  1. Load MANIFEST-000001 (if valid)      │
│  2. Open manifest WAL                      │
│  3. Replay WAL entries → apply to state    │
│  4. In-memory ManifestData is consistent   │
└───────────────────────────────────────────┘
```

If the snapshot is corrupted (checksum mismatch), the manifest returns an error
rather than silently ignoring corruption.

---

## Event Types

Every metadata change is recorded as a `ManifestEvent` in the manifest WAL:

| Event              | Fields                          | Effect                                                      |
|--------------------|---------------------------------|-------------------------------------------------------------|
| `Version`          | `version: u64`                  | Sets manifest version                                       |
| `SetActiveWal`     | `wal: u64`                      | Switches active WAL; removes ID from frozen list if present |
| `AddFrozenWal`     | `wal: u64`                      | Adds WAL segment to frozen list (idempotent)                |
| `RemoveFrozenWal`  | `wal: u64`                      | Removes WAL segment from frozen list                        |
| `AddSst`           | `entry: ManifestSstEntry`       | Adds an SSTable entry (skips duplicates by ID)              |
| `RemoveSst`        | `id: u64`                       | Removes an SSTable entry by ID                              |
| `UpdateLsn`        | `last_lsn: u64`                 | Advances global LSN (only if higher than current)           |
| `AllocateSstId`    | `id: u64`                       | Persists SSTable ID allocation; advances `next_sst_id`      |
| `Compaction`       | `added: Vec<…>, removed: Vec<…>`| Atomic add + remove in a single WAL entry                   |

All event application is **idempotent** — replaying the same WAL twice produces
the same result because:
- `AddSst` / `AddFrozenWal` skip duplicates.
- `UpdateLsn` only advances (never decreases).
- `AllocateSstId` advances past the allocated ID.

---

## Operations

### Mutation Pattern

Every public method follows the same protocol:

```
1. Construct ManifestEvent
2. Append to manifest WAL (durable write + fsync)
3. Apply to in-memory ManifestData
```

### SSTable ID Allocation

The manifest owns the SSTable ID counter. `allocate_sst_id()` atomically:
1. Reads current `next_sst_id`.
2. Persists `AllocateSstId { id }` to the WAL.
3. Increments `next_sst_id` in memory.
4. Returns the allocated ID.

This guarantees unique IDs even across crashes.

### Atomic Compaction

The `Compaction` event atomically records the addition of new SSTables and removal
of old ones in a single WAL entry. This ensures that after a crash:
- Either the full compaction is visible (both adds and removes applied).
- Or none of it is visible (WAL entry was not fully written).

Old SSTable files are deleted only after the manifest WAL entry is durable.

---

## Checkpoint (Snapshotting)

`checkpoint()` reduces startup recovery cost by writing a full snapshot:

```
1. Serialize ManifestData (with checksum = 0)
2. Compute CRC32 over serialized bytes
3. Build final ManifestSnapshot { version, snapshot_lsn, manifest_data, checksum }
4. Write to MANIFEST-000001.tmp
5. fsync the temp file
6. Atomic rename → MANIFEST-000001
7. fsync parent directory
8. Truncate manifest WAL (reset to header-only)
9. Mark in-memory data as clean (dirty = false)
```

The atomic rename ensures that a crash during snapshotting never corrupts the
existing snapshot.

---

## Concurrency

| Component       | Synchronization | Notes                                     |
|-----------------|-----------------|-------------------------------------------|
| `ManifestData`  | `Mutex`         | Coordinates concurrent metadata updates   |
| Manifest WAL    | Internal sync   | `Wal<T>` handles its own file locking     |

The manifest is fully thread-safe and can be called from any engine thread.

---

## File Layout

```
<db_dir>/
  manifest/
    000000.log       ← Manifest WAL (append-only event log)
    MANIFEST-000001     ← Latest checkpoint (custom encoding + CRC32)
    MANIFEST-000001.tmp ← Temporary file during checkpoint (deleted on success)
```

---

## Integration with Engine

The manifest does not manage SSTable files or WAL segments directly — it only
records metadata decisions. The engine coordinates the full workflow:

1. **Flush**: engine writes SSTable → fsyncs → records `AddSst` in manifest →
   removes frozen WAL via `RemoveFrozenWal`.

2. **Compaction**: engine writes new SSTables → records `Compaction` event →
   deletes old SSTable files.

3. **Recovery**: engine calls `Manifest::open()` to reconstruct metadata, then
   uses the SSTable list and WAL info to rebuild the full engine state.

---

## Error Handling

| Error                      | Cause                                    |
|----------------------------|------------------------------------------|
| `Wal`                      | Underlying WAL I/O or format error       |
| `Io`                       | Filesystem error during snapshot I/O     |
| `Encoding`                 | Encoding / decoding failure              |
| `SnapshotChecksumMismatch` | Snapshot file corrupted or tampered       |
| `Internal`                 | Mutex poisoned or invariant violation    |
