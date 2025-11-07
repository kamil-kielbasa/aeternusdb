# SSTable Format — AeternusDB

## Overview
This document specifies the **Sorted String Table (SSTable)** format used by AeternusDB.  
SSTables are **immutable**, **sorted**, and **checksummed** on-disk data files that store flushed or compacted key-value pairs (and tombstones).

Design improvements based on RocksDB/LevelDB best practices for:
- ✅ Sequential write optimization (no backward seeking)
- ✅ Fixed-size header (no rewrites during build)
- ✅ Improved extensibility via metaindex
- ✅ Simplified block layout with internal trailers
- ✅ Standardized metadata format
- ✅ Forward compatibility with future features

---

## File Layout Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ SSTABLE FILE                                                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│ 1. HEADER BLOCK (fixed 32 bytes)                                             │
│ 2. DATA BLOCKS (#0..N)                                                       │
│ 3. BLOOM FILTER BLOCK                                                        │
│ 4. PROPERTIES BLOCK                                                          │
│ 5. RANGE DELETES BLOCK                                                       │
│ 7. METAINDEX BLOCK                                                           │
│ 8. INDEX BLOCK                                                               │
│ 9. FOOTER BLOCK (fixed 48 bytes at end)                                      │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Key Principles:**
- **Fixed-size header**: 32 bytes, no variable-length fields
- **Sequential writes**: Data → Meta blocks → Metaindex → Index → Footer
- **Fixed footer position**: Always at `file_size - 48 bytes`
- **Block trailers**: Each block contains internal metadata at end
- **No backward seeking**: All offsets known at write time

---

## 1. Header Block

**Fixed 32-byte header** for fast validation without variable-length parsing.

```
Offset  Size  Field
------  ----  -----
0       4     magic = 0x53535430 (b"SST0")
4       4     version = 1
8       8     record_count (total key-value pairs)
16      8     tombstone_count (deletion markers)
24      8     creation_timestamp (Unix nanoseconds)
------  ----
Total:  32 bytes (FIXED)
```

**Rationale:**
- Fixed size eliminates header rewrites during build
- Quick magic/version validation without parsing
- LSN ranges and key ranges in Properties Block (more flexible)
- Simplifies write process (write once, no updates)

---

## 2. Data Blocks

Each block stores multiple cells (key-value pairs or tombstones).  
Target size: ~4KiB uncompressed.

### Block Structure

```
┌────────────────────────────────────────────────────────────┐
│ BLOCK CONTENT                                              │
│   Cell #0:                                                 │
│     [u32] key_len                                          │
│     [bytes] key                                            │
│     [u32] value_len                                        │
│     [bytes] value                                          │
│     [u64] timestamp                                        │
│     [u8] flags (bit 0: is_delete)                          │
│     [u64] lsn                                              │
│   Cell #1:                                                 │
│     ...                                                    │
│   ... more cells ...                                       │
├────────────────────────────────────────────────────────────┤
│ BLOCK TRAILER (internal metadata)                          │
│   [u32] uncompressed_size (original size before compress)  │
│   [u32] crc32 (checksum over content + trailer)            │
├────────────────────────────────────────────────────────────┤
│ Total: variable size (~4KiB typical)                       │
└────────────────────────────────────────────────────────────┘
```

### Block Trailer Format

```
Offset from end  Size  Field
---------------  ----  -----
-8               4     uncompressed_size
-4               4     crc32
---------------  ----
Total:           8 bytes (FIXED)
```

**Design rationale:**
- Trailer at end enables streaming reads (read content, then trailer)
- CRC32 checksum covers entire block including trailer

---

## 3. Bloom Filter Block

Probabilistic data structure for fast negative lookups.

### Block Structure

```
┌────────────────────────────────────────────────────────────┐
│ BLOOM CONTENT                                              │
│   [u64] num_bits (bit array size)                          │
│   [u32] num_hash_functions (typically 3-7)                 │
│   [bytes] bit_array ((num_bits + 7) / 8 bytes)             │
├────────────────────────────────────────────────────────────┤
│ BLOCK TRAILER                                              │
│   [u32] crc32 (checksum over content)                      │
└────────────────────────────────────────────────────────────┘
```

**Configuration:**
- Default: ~10 bits per key (1-2% false positive rate)
- Loaded entirely into memory on SSTable open

---

## 4. Properties Block

Standardized key-value metadata for SSTable statistics and configuration.

### Block Structure

```
┌────────────────────────────────────────────────────────────┐
│ PROPERTIES CONTENT                                         │
│   [u32] num_properties                                     │
│   Property #0:                                             │
│     [u32] key_len                                          │
│     [bytes] key (UTF-8 string)                             │
│     [u32] value_len                                        │
│     [bytes] value (UTF-8 string)                           │
│   Property #1:                                             │
│     ...                                                    │
│   ... more properties ...                                  │
├────────────────────────────────────────────────────────────┤
│ BLOCK TRAILER                                              │
│   [u32] crc32 (checksum over content)                      │
└────────────────────────────────────────────────────────────┘
```

### Standard Properties

**Required properties** (every SSTable must include):

| Key | Type | Description | Example |
|-----|------|-------------|---------|
| `creation.time` | u64 | Unix timestamp (nanos) | `"1704067200000000000"` |
| `num.entries` | u64 | Total key-value pairs | `"100000"` |
| `num.deletions` | u64 | Tombstone count | `"1500"` |
| `num.range_deletions` | u32 | Range tombstone count | `"5"` |
| `min.lsn` | u64 | Lowest LSN in file | `"1000"` |
| `max.lsn` | u64 | Highest LSN in file | `"2000"` |
| `min.timestamp` | u64 | Earliest timestamp | `"1704067200000000000"` |
| `max.timestamp` | u64 | Latest timestamp | `"1704153600000000000"` |
| `min.key` | bytes | Smallest key (hex or base64) | `"6170706c65"` (hex for "apple") |
| `max.key` | bytes | Largest key (hex or base64) | `"7a65627261"` (hex for "zebra") |

**Format Notes:**
- All values are UTF-8 strings for simplicity and interoperability
- Numbers stored as little-endian integers
- Binary data (min.key, max.key) encoded as hex strings
- Tools can parse without schema knowledge

---

## 5. Range Deletes Block

Efficient representation of large-range deletions.

### Block Structure

```
┌────────────────────────────────────────────────────────────┐
│ RANGE DELETES CONTENT                                      │
│   [u32] num_ranges                                         │
│   Range #0:                                                │
│     [u32] start_key_len                                    │
│     [bytes] start_key (inclusive)                          │
│     [u32] end_key_len                                      │
│     [bytes] end_key (exclusive)                            │
│     [u64] timestamp                                        │
│     [u64] lsn                                              │
│   Range #1:                                                │
│     ...                                                    │
│   ... more ranges ...                                      │
├────────────────────────────────────────────────────────────┤
│ BLOCK TRAILER                                              │
│   [u32] crc32 (checksum over content)                      │
└────────────────────────────────────────────────────────────┘
```

**Semantics:**
- A key `k` is deleted if: `start_key ≤ k < end_key` AND `range_lsn > key_lsn`
- Checked during `get()` and `scan()` operations
- Compacted away when all covered keys are removed

**Example:**
```
Range: ["user:1000:", "user:2000:"), LSN=100
Deletes: user:1000:profile, user:1500:settings, etc.
```

---

## 6. Metaindex Block

Registry of all meta blocks in the file.

### Block Structure

```
┌────────────────────────────────────────────────────────────┐
│ METAINDEX CONTENT                                          │
│   [u32] num_entries                                        │
│   Entry #0:                                                │
│     [u32] name_len                                         │
│     [bytes] name (UTF-8 string)                            │
│     [u64] offset (byte offset in file)                     │
│     [u64] size (block size including trailer)              │
│   Entry #1:                                                │
│     ...                                                    │
│   ... more entries ...                                     │
├────────────────────────────────────────────────────────────┤
│ BLOCK TRAILER                                              │
│   [u32] crc32 (checksum over content)                      │
└────────────────────────────────────────────────────────────┘
```

**Standard meta block names:**

| Name | Description | Required |
|------|-------------|----------|
| `filter.bloom` | Bloom filter block | Yes |
| `meta.properties` | Properties block | Yes |
| `meta.range_deletions` | Range deletes block | Optional |

**Design rationale:**
- Written AFTER all meta blocks (offsets are known)
- Enables adding new meta blocks without format version bump

---

## 7. Index Block

Maps key ranges to data block locations using separator keys.

### Block Structure

```
┌────────────────────────────────────────────────────────────┐
│ INDEX CONTENT                                              │
│   [u32] num_entries                                        │
│   Entry #0:                                                │
│     [u32] separator_key_len                                │
│     [bytes] separator_key                                  │
│     [u64] block_offset (byte offset in file)               │
│     [u64] block_size (bytes including trailer)             │
│   Entry #1:                                                │
│     ...                                                    │
│   ... more entries ...                                     │
├────────────────────────────────────────────────────────────┤
│ BLOCK TRAILER                                              │
│   [u32] crc32 (checksum over content)                      │
└────────────────────────────────────────────────────────────┘
```

### Separator Keys

**Definition:** A separator key is the **shortest key** that satisfies:
- `separator_key ≥ last_key_in_block[i]`
- `separator_key < first_key_in_block[i+1]`

**Example:**
```
Block 0: keys ["apple", "banana", "cherry"]
Block 1: keys ["dog", "elephant", "fox"]
Block 2: keys ["grape", "honey", "ice"]

Index:
  Entry 0: separator="d",     offset=32,   size=4109  (points to Block 0)
  Entry 1: separator="g",     offset=4141, size=4109  (points to Block 1)
  Entry 2: separator="j",     offset=8250, size=4109  (points to Block 2)

Lookup("eagle"):
  Binary search: "d" ≤ "eagle" < "g" → Block 1 ✓ (single block read!)
```

**Note on BlockHandle:**
- `(offset, size)` pair forms a BlockHandle (RocksDB concept)
- Offset points to start of block content
- Size includes content + trailer (entire block)

---

## 8 Footer Block

**Fixed 48-byte trailer** at end of file for integrity verification.

```
Position: file_size - 44 bytes (FIXED)

Offset  Size  Field
------  ----  -----
0       8     metaindex_offset (byte offset of metaindex block)
8       8     metaindex_size (bytes, including trailer)
16      8     index_offset (byte offset of index block)
24      8     index_size (bytes, including trailer)
32      8     total_file_size (including footer)
40      4     footer_crc32 (CRC32 over bytes 0-39)
------  ----
Total:  44 bytes (FIXED)

```

**Design rationale:**
- Fixed position enables fast access without reading header
- No circular dependency (footer doesn't reference header)
- Footer CRC32 checksums footer itself only
- Reserved field for future extensions
- Position at `file_size - 44` serves as implicit magic validation

---

## Write Process

Sequential write flow with no backward seeking:

```
1. Write Header (fixed 32 bytes)
   ↓
2. Write Data Blocks (sequentially)
   For each block:
     - Write block content (cells)
     - Write block trailer (compression_type, sizes, crc32)
   Record: block_offset, block_size for each
   ↓
3. Write Bloom Filter Block
   - Write bloom content
   - Write block trailer (crc32)
   Record: bloom_offset, bloom_size
   ↓
4. Write Properties Block
   - Build properties (including min.key, max.key, min.lsn, max.lsn)
   - Write properties content
   - Write block trailer (crc32)
   Record: properties_offset, properties_size
   ↓
5. Write Range Deletes Block (if any)
   - Write range deletes content
   - Write block trailer (crc32)
   Record: range_deletes_offset, range_deletes_size
   ↓
6. Build and Write Metaindex Block
   - Add entries: ("filter.bloom", bloom_offset, bloom_size)
   - Add entries: ("meta.properties", properties_offset, properties_size)
   - Add entries: ("meta.range_deletions", ...) if exists
   - Write metaindex content
   - Write block trailer (crc32)
   Record: metaindex_offset, metaindex_size
   ↓
7. Build and Write Index Block
   - For each data block: compute separator key
   - Add entries: (separator_key, block_offset, block_size)
   - Write index content
   - Write block trailer (crc32)
   Record: index_offset, index_size
   ↓
8. Write Footer (fixed 48 bytes)
   - metaindex_offset, metaindex_size
   - index_offset, index_size
   - total_file_size
   - reserved = 0
   - footer_crc32 (computed over footer fields)
   ↓
9. fsync() → Done! ✓
```

**Key advantages:**
- ✅ Pure sequential writes (optimal for SSDs)
- ✅ Fixed-size header (write once, no updates)
- ✅ No reserved space or placeholder values
- ✅ All offsets known at write time
- ✅ Single fsync at end

---

## Read/Open Process

```
1. Open file, get file_size
   ↓
2. Read Header (first 32 bytes)
   Validate: magic = 0x53535430, version = 2
   ↓
3. Seek to: file_size - 48
   ↓
4. Read Footer (48 bytes)
   Validate: footer_crc32
   ↓
5. Seek to footer.metaindex_offset
   Read Metaindex Block
   - Read content (num_entries + entries)
   - Read trailer (crc32)
   - Validate crc32
   ↓
6. Discover meta blocks:
   - "filter.bloom" → offset, size
   - "meta.properties" → offset, size
   - "meta.range_deletions" → offset, size (if exists)
   ↓
7. Load essential blocks:
   - Read Bloom Filter (into memory)
     • Read content + trailer
     • Validate crc32
   - Read Properties (parse metadata)
     • Read content + trailer
     • Validate crc32
     • Extract: min.key, max.key, min.lsn, max.lsn, etc.
   - Read Index (into memory or mmap)
     • Read content + trailer
     • Validate crc32
   ↓
8. Optionally preload:
   - Range Deletes (if present)
   ↓
9. SSTable ready for queries ✓
```

---

## Lookup Algorithm

```
get(key) → Option<Value>:

1. Check Properties: min.key ≤ key ≤ max.key
   If out of range: return None
   
2. Check Bloom Filter: might_contain(key)
   If false: return None (fast negative lookup)
   
3. Load Range Deletes (if present):
   If key is in deleted range: return None
   
4. Binary search Index using separator keys:
   Find block_offset, block_size where key might exist
   
5. Read Data Block from disk:
   - Seek to block_offset
   - Read block_size bytes (content + trailer)
   - Read trailer: uncompressed_size, crc32
   - Validate crc32
   
6. Linear search within block:
   For each cell:
     If cell.key == key AND cell.lsn is visible:
       If cell.is_delete: return None
       Else: return Some(cell.value)
   
7. return None (key not found)
```

**Typical case performance:**
- Header check: O(1) in-memory (from Properties)
- Bloom filter check: O(1) in-memory  
- Index binary search: O(log N) in-memory  
- Block read: 1 disk I/O (~4KiB)
- Within-block scan: O(M) where M ≈ 50-100 entries

---

## Range Scan Algorithm

```
scan(start_key, end_key) → Iterator<(Key, Value)>:

1. Check Properties: Overlap check with [min.key, max.key]
   If no overlap: return empty iterator
   
2. Binary search Index: Find first block containing start_key
   
3. Load Range Deletes (if present):
   Get overlapping ranges with [start_key, end_key)
   
4. Sequential block iteration:
   For each block from start to end:
     - Read block (offset, size from index)
     - Read trailer, validate crc32
     - Decompress if needed
     For each cell in block:
       If start_key ≤ cell.key < end_key:
         If NOT deleted by range tombstone:
           If NOT cell.is_delete:
             yield (cell.key, cell.value)
   
5. Continue until cell.key ≥ end_key
```

---

## Integrity Guarantees

| Level | Mechanism | Scope | Purpose |
|-------|-----------|-------|---------|
| **Header** | Magic + version | 4 bytes | Fast format validation |
| **Data Block** | CRC32 in trailer | ~4KiB block | Detect corruption in data |
| **Meta Blocks** | CRC32 in trailer | Each block | Detect corruption in metadata |
| **Footer** | CRC32 | Footer fields | Validate footer integrity |

**Design philosophy:**
- Each block self-contained (content + trailer with CRC32)
- No full-file checksums (performance cost on large files)
- Block-level granularity enables partial recovery
- Industry standard approach

---

## Block Layout Philosophy

**Unified block structure** across all block types:

```
┌─────────────────────────────────────┐
│ CONTENT (variable)                  │  ← Block-specific data
├─────────────────────────────────────┤
│ TRAILER (fixed per block type)      │  ← Metadata + CRC32
└─────────────────────────────────────┘

BlockHandle = (offset, size)
  offset → points to start of CONTENT
  size → includes CONTENT + TRAILER
```

**Benefits:**
- Consistent I/O pattern (read size bytes from offset)
- Trailer enables validation after read
- Simplifies implementation (all blocks follow same pattern)

---

## Future Extensions (Optional Features)

### Restart Points (Prefix Compression)

**Status:** Planned for next version

**Description:** Store keys with prefix compression + restart points for 30-50% space savings.

**Format changes needed:**
```
Data Block Content:
  Cell with prefix compression:
    [varint] shared_bytes      ← NEW
    [varint] unshared_bytes    ← NEW (replaces key_len)
    [bytes] unshared_key       ← Only unique suffix
    [u32] value_len
    [bytes] value
    [u64] timestamp
    [u8] flags
    [u64] lsn

Data Block Trailer:
  [u32[]] restart_offsets      ← NEW (every 16 entries)
  [u32] num_restarts           ← Currently 0, will be N
  [u32] crc32
```

**Benefits:**
- 30-50% reduction in key storage
- Binary search within blocks via restart points

### Column Families

**Status:** Planned for next version

**Description:** Multiple logical namespaces within single storage engine.

**Format changes needed:**
```
Cell format:
  [u32] column_family_id       ← NEW (prepended to cell)
  [u32] key_len
  [bytes] key
  ... rest unchanged ...

Properties:
  cf.id = "1"
  cf.name = "users"
```

**Use cases:**
- Multi-tenant isolation
- Separate data types with different settings
- Efficient bulk deletion (drop entire CF)

### Partitioned Index/Bloom

**Status:** Planned for next version

**Description:** Split large index/bloom into multiple blocks for files >64MB.

**Benefits:**
- Lazy loading of index partitions
- Lower memory footprint
- Better for very large SSTables

---

## Summary

**SSTable** is a production-ready format that:

✅ **Fixed-size header** - 32 bytes, no variable fields, no rewrites  
✅ **Block trailers** - Unified structure, metadata at end  
✅ **Sequential writes** - No backward seeking, optimal for SSDs  
✅ **Reduced overhead** - Block-level checksums only  
✅ **Standardized metadata** - Key-value properties, min/max keys in properties  
✅ **Improved extensibility** - Metaindex enables new features  
✅ **Industry alignment** - BlockHandle concept, separator keys  
✅ **Future-ready** - Clear path to restart points, column families  

The format balances simplicity for initial implementation with extensibility for future enhancements.
