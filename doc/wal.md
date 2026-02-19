# Write-Ahead Log (WAL)

## Overview

The Write-Ahead Log is a **durable**, **append-only**, **generic** persistence layer used throughout AeternusDB. Every mutation — whether to the memtable or the manifest — is appended to a WAL and `fsync`'d before the in-memory state is updated. This guarantees crash recovery: on restart, replaying the WAL reconstructs the exact state that was durable at the time of the crash.

The WAL is generic over its record type via the `WalData` trait, allowing the same implementation to be reused for:

- **Memtable WAL** — stores `Record` variants (`Put`, `Delete`, `RangeDelete`).
- **Manifest WAL** — stores `ManifestEvent` variants (`AddSst`, `RemoveSst`, etc.).

## On-Disk Format

```text
┌──────────────────────────────────────────────┐
│ [HEADER_BYTES] [HEADER_CRC32_LE]             │  ← Fixed-size header
├──────────────────────────────────────────────┤
│ [REC_LEN_LE] [REC_BYTES] [REC_CRC32_LE]     │  ← Record 0
│ [REC_LEN_LE] [REC_BYTES] [REC_CRC32_LE]     │  ← Record 1
│ ...                                          │
└──────────────────────────────────────────────┘
```

### Header

The header is written once when a WAL file is created and validated on every open.

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | `magic` | `b"AWAL"` — identifies the file as a WAL. |
| 4 | 4 | `version` | Format version (`1`). |
| 8 | 4 | `max_record_size` | Maximum allowed record size in bytes (default: 1 MiB). |
| 12 | 8 | `wal_seq` | Monotonic sequence number parsed from the filename. |

The header is followed by a 4-byte CRC32 checksum computed over the serialized header bytes.

### Record

Each record is a self-contained, checksummed unit:

| Component | Size | Description |
|-----------|------|-------------|
| `len` | 4 bytes (LE) | Length of the serialized record in bytes. |
| `record_bytes` | `len` bytes | Record payload serialized with `bincode` (fixed-int encoding). |
| `crc32` | 4 bytes (LE) | CRC32 checksum computed over `len ‖ record_bytes`. |

The checksum covers both the length prefix and the payload, protecting against both data corruption and length field corruption.

## File Naming

WAL files follow the pattern `wal-NNNNNN.log`, where `NNNNNN` is a zero-padded sequence number. The sequence number is parsed from the filename on open and stored in the header for validation.

Examples:
```
memtables/wal-000001.log   # Active memtable WAL (seq 1)
memtables/wal-000002.log   # Frozen memtable WAL (seq 2)
manifest/wal-000001.log    # Manifest WAL
```

## Guarantees

| Property | Mechanism |
|----------|-----------|
| **Durability** | Every `append()` calls `File::sync_all()` after writing. |
| **Integrity** | Header and every record are CRC32-checksummed. |
| **Corruption detection** | Replay stops at the first invalid checksum or truncated record — partial writes from a crash are silently discarded. |
| **Thread safety** | The file handle is wrapped in `Arc<Mutex<File>>`. Multiple threads can safely share a WAL instance. |
| **Drop safety** | `Wal` implements `Drop` with a final `sync_all()`, recovering from poisoned mutexes. |

## Operations

### Append

```
append(record) → Result<(), WalError>
```

1. Serialize `record` with `bincode`.
2. Check that the serialized size does not exceed `max_record_size`.
3. Compute CRC32 over `[len_le ‖ record_bytes]`.
4. Acquire the file mutex.
5. Write `[len_le][record_bytes][crc32_le]`.
6. Call `sync_all()`.

### Replay

```
replay_iter() → Result<WalIter<T>, WalError>
```

Returns a streaming iterator that reads records sequentially from the WAL file. Each record is verified against its CRC32 checksum before being decoded. The iterator terminates at:

- End of file (normal).
- First checksum mismatch (corruption or partial write).
- Truncated record (crash during write).

The iterator seeks to its current offset before each read to avoid race conditions with concurrent appenders.

### Truncate

```
truncate() → Result<(), WalError>
```

Resets the WAL file to contain only the header. Used by the manifest after a checkpoint to reclaim space.

### Rotate

```
rotate_next() → Result<u64, WalError>
```

Syncs the current WAL, opens a new WAL file with `wal_seq + 1`, and replaces `self` with the new instance. Used during memtable freeze to create a fresh WAL for the new active memtable.

## Concurrency

The WAL uses `Arc<Mutex<File>>` for thread-safe access. The `WalIter` holds its own `Arc` clone and tracks a logical byte offset, seeking before each read. This allows concurrent append and replay without external synchronization.

```text
Thread A (writer)        Thread B (reader)
    │                        │
    ├── lock → append ──┐    │
    │                   │    ├── lock → seek → read ──┐
    │   unlock ◄────────┘    │                        │
    │                        │   unlock ◄─────────────┘
```

## Error Handling

The `WalError` enum covers all failure modes:

| Variant | Cause |
|---------|-------|
| `Io` | Underlying filesystem error. |
| `Encode` / `Decode` | Bincode serialization failure. |
| `ChecksumMismatch` | CRC32 verification failed — data corruption or partial write. |
| `RecordTooLarge` | Record exceeds `max_record_size`. |
| `UnexpectedEof` | Record was truncated (crash during write). |
| `InvalidHeader` | Header magic, version, or sequence number mismatch. |
| `Internal` | Mutex poisoning or other invariant violation. |
