# TODO

## 1. Flush all versions exactly as they appear in the memtable

- Emit all point entries (PUTs and DELETEs) sorted by **(key ASC, seq DESC)**  

```rust
BTreeMap<Key, BTreeMap<Reverse<Seq>, Value>>
```

- Emit all range tombstones sorted by **(start_key ASC, seq DESC)**  

```rust
BTreeMap<StartKey, BTreeMap<Reverse<Seq>, Value>>
```

- Do **not** aggregate or collapse versions during flush.  
Version collapsing happens **only during compaction**.

---

## 2. Maintain two separate sorted structures in the memtable

- **Point entries**: skiplist ordered by **(key ASC, seq DESC)**  
- **Range tombstones**: skiplist or interval tree ordered by **(start_key ASC, seq DESC)**  

These remain separate until read time (merged by iterators).

---

## 3. Memtable: unify public structures

- Memtable must expose public structures used directly by:
- memtable flush iterators  
- SSTable builder (as input)

- Avoid translating between memtable flush output and SSTable build input — this is inefficient.

---

## 4. Sorted String Table modularization

SST should be split into the following modules:

- **SST Core**
- **SST Data Block Iterator**
- **SST Scan Iterator**
- **SST Builder**
