# TODO

## 1. Memtable must support injecting and querying the max LSN

Recovery requires restoring the correct sequence number.

The Memtable therefore exposes:
```rust
memtable.inject_max_lsn(lsn);
let lsn = memtable.max_lsn();
```

**Why?**
- WAL replay must restore the largest seen LSN
- Future writes must generate last_lsn + 1
- Prevents sequence reuse after crash

This keeps WAL and memtable perfectly synchronized.

## 2. Memtable WAL Recovery (Replay Path)

During engine startup:
1. List all WAL files
2. Validate UUID + sequence numbers
3. Replay each WAL record into memtable
4. Track max LSN observed

After replay:
```rust
memtable.inject_max_lsn(last_lsn_from_wal);
```

**Guarantees:**
- monotonic sequence generation
- perfect reconstruction of in-memory state
- flush/compaction invariants hold

## 3. FrozenMemtable

When memtable exceeds size threshold:
- It becomes FrozenMemtable
- It is immutable
- No new writes are allowed
- Reads + flush can proceed concurrently

Only one memtable is writable at any moment:
- Active Memtable (writable)
- Frozen Memtables (read-only, flushing)

FrozenMemtable contents remain stable and deterministic until flushed to SST.

## 4. Flush all versions exactly as they appear in the memtable

- Emit all point entries (PUTs and DELETEs) sorted by **(key ASC, seq DESC)**
```rust
BTreeMap<Key, BTreeMap<Reverse<Seq>, Value>>
```

- Emit all range tombstones sorted by **(start_key ASC, seq DESC)**
```rust
BTreeMap<StartKey, BTreeMap<Reverse<Seq>, Value>>
```

- Do **not** aggregate or collapse versions during flush.
- Version collapsing happens **only during compaction**.

## 5. Maintain two separate sorted structures in the memtable

- **Point entries**: skiplist ordered by **(key ASC, seq DESC)**
- **Range tombstones**: skiplist or interval tree ordered by **(start_key ASC, seq DESC)**

These remain separate until read time (merged by iterators).

## 6. Memtable: unify public structures

- Memtable must expose public structures used directly by:
  - memtable flush iterators
  - SSTable builder (as input)
- Avoid translating between memtable flush output and SSTable build input — this is inefficient.

## 7. Sorted String Table modularization

SST should be split into the following modules:
- **SST Core**
- **SST Data Block Iterator**
- **SST Scan Iterator**
- **SST Builder**
