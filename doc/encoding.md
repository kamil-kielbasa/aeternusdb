# Encoding

Custom, zero-dependency binary encoding for on-disk persistence in AeternusDB.

## Overview

AeternusDB uses a hand-written binary encoding instead of external serialization libraries (e.g. `bincode`). By owning the wire format entirely, the on-disk representation **never** changes due to a dependency upgrade — every byte is under the project's control.

The encoding module provides two core traits (`Encode` and `Decode`), convenience functions, and safety limits. All functions are **fallible** (return `Result`) and uphold a **zero-panic guarantee**: no `unwrap()`, `expect()`, or other panicking paths exist anywhere in the module.

## Wire Format

All multi-byte integers are **little-endian**. Lengths and counts are encoded as `u32`, limiting individual items to 4 GiB.

| Rust Type          | Wire Encoding                                    | Size        |
|--------------------|--------------------------------------------------|-------------|
| `u8`               | 1 raw byte                                       | 1 byte      |
| `u16`              | 2 bytes, little-endian                           | 2 bytes     |
| `u32`              | 4 bytes, little-endian                           | 4 bytes     |
| `u64`              | 8 bytes, little-endian                           | 8 bytes     |
| `i64`              | 8 bytes, little-endian                           | 8 bytes     |
| `bool`             | 1 byte (`0x00` = false, `0x01` = true)           | 1 byte      |
| `[u8; N]`          | `N` raw bytes (no length prefix)                 | N bytes     |
| `Vec<u8>` / `&[u8]`| `[u32 len][bytes]`                               | 4 + len     |
| `String` / `&str`  | `[u32 len][UTF-8 bytes]`                         | 4 + len     |
| `PathBuf`          | `[u32 len][raw OS bytes]`                        | 4 + len     |
| `Option<T>`        | `[u8 tag: 0=None, 1=Some][T if Some]`            | 1 (+ T)     |
| `Vec<T>`           | `[u32 count][T₁][T₂]…`                           | 4 + Σ Tᵢ    |
| `enum`             | `[u32 variant][fields…]` (hand-written per type) | 4 + fields  |

### Encoding Examples

**A `u32` value (42):**

```
Offset  Bytes
0x00    2A 00 00 00     ← 42 in little-endian u32
```

**A `Vec<u8>` with 3 bytes (`[0xAA, 0xBB, 0xCC]`):**

```
Offset  Bytes
0x00    03 00 00 00     ← length = 3 (u32 LE)
0x04    AA BB CC        ← raw payload
```

**An `Option<u64>` with `Some(1)` vs `None`:**

```
Some(1):
0x00    01              ← tag = Some
0x01    01 00 00 00 00 00 00 00   ← 1 in LE u64

None:
0x00    00              ← tag = None
```

## Core Traits

### `Encode`

Serializes a value into a byte buffer. Implementations **must** produce deterministic output — the same logical value always yields the exact same byte sequence.

```rust
pub trait Encode {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError>;
}
```

### `Decode`

Deserializes a value from a byte slice. Returns `(value, bytes_consumed)` so callers can advance a cursor through a buffer containing multiple encoded items.

```rust
pub trait Decode: Sized {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError>;
}
```

## Convenience Functions

### `encode_to_vec`

Encodes a value into a freshly allocated `Vec<u8>`:

```rust
pub fn encode_to_vec<T: Encode>(value: &T) -> Result<Vec<u8>, EncodingError>;
```

### `decode_from_slice`

Decodes a value from the beginning of a byte slice:

```rust
pub fn decode_from_slice<T: Decode>(buf: &[u8]) -> Result<(T, usize), EncodingError>;
```

### `encode_vec` / `decode_vec`

Encode and decode a `Vec<T>` (for non-`u8` element types) as `[u32 count][T₁][T₂]…`:

```rust
pub fn encode_vec<T: Encode>(items: &[T], buf: &mut Vec<u8>) -> Result<(), EncodingError>;
pub fn decode_vec<T: Decode>(buf: &[u8]) -> Result<(Vec<T>, usize), EncodingError>;
```

> **Note:** `Vec<u8>` has a specialised `Decode` impl that reads raw bytes without per-element overhead. The `encode_vec` / `decode_vec` functions are for vectors of structs.

## Error Handling

All encoding and decoding errors are represented by the `EncodingError` enum:

| Variant           | Description                                                        |
|-------------------|--------------------------------------------------------------------|
| `UnexpectedEof`   | Buffer ran out of bytes (reports `needed` vs `available`)          |
| `InvalidTag`      | Enum discriminant or option tag not recognised                     |
| `InvalidBool`     | Bool byte was not `0x00` or `0x01`                                 |
| `InvalidUtf8`     | Byte sequence decoded as `String` is not valid UTF-8               |
| `LengthOverflow`  | A length or count exceeded its safety limit                        |
| `Custom`          | Application-level decode error (free-form message)                 |

## Safety Limits

To prevent denial-of-service via crafted inputs (allocation bombs), all variable-length decoders enforce upper bounds before allocating memory:

| Constant            | Value     | Applies To                            |
|---------------------|-----------|---------------------------------------|
| `MAX_BYTE_LEN`      | 256 MiB   | `Vec<u8>`, `String`, `PathBuf`        |
| `MAX_VEC_ELEMENTS`  | 16 M      | `Vec<T>` (non-`u8` element types)     |

When a decoded length or count exceeds these limits, `EncodingError::LengthOverflow` is returned immediately — no allocation is attempted.

## PathBuf Encoding

`PathBuf` is encoded using the raw OS byte representation via `OsStr::as_bytes()` (Unix). This means:

- No lossy UTF-8 conversion — non-UTF-8 paths survive encoding/decoding.
- Fully deterministic — round-trips perfectly on the same platform.
- If cross-platform portability is ever needed, paths should be normalised to UTF-8 at the application layer before encoding.

## Usage in AeternusDB

The encoding module is used throughout the storage engine:

| Component   | Encoded Types                                                        |
|-------------|----------------------------------------------------------------------|
| **WAL**     | `WalHeader`, `Record`, `RangeTombstone`, `MemtablePointEntry`       |
| **SSTable** | Block headers, index entries, bloom filter, properties, range tombstones, footer |
| **Manifest**| `ManifestEvent`, `ManifestSnapshot`, `ManifestSstEntry`              |

All on-disk data flows through the `Encode` / `Decode` traits, ensuring a single consistent wire format across every persistence layer.

## Implementing Encode / Decode for New Types

To encode a new struct, implement both traits by delegating to field-level encoding:

```rust
use crate::encoding::{Encode, Decode, EncodingError};

struct MyEntry {
    key: Vec<u8>,
    lsn: u64,
    deleted: bool,
}

impl Encode for MyEntry {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        self.key.encode_to(buf)?;
        self.lsn.encode_to(buf)?;
        self.deleted.encode_to(buf)?;
        Ok(())
    }
}

impl Decode for MyEntry {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (key, mut off) = Vec::<u8>::decode_from(buf)?;
        let (lsn, n) = u64::decode_from(&buf[off..])?;
        off += n;
        let (deleted, n) = bool::decode_from(&buf[off..])?;
        off += n;
        Ok((Self { key, lsn, deleted }, off))
    }
}
```

For enums, use a `u32` discriminant tag followed by variant fields:

```rust
impl Encode for MyEnum {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        match self {
            MyEnum::VariantA(val) => {
                0u32.encode_to(buf)?;
                val.encode_to(buf)?;
            }
            MyEnum::VariantB { x, y } => {
                1u32.encode_to(buf)?;
                x.encode_to(buf)?;
                y.encode_to(buf)?;
            }
        }
        Ok(())
    }
}
```

## Testing

The encoding module has comprehensive tests across three test files:

| Test File                | Coverage                                                    |
|--------------------------|-------------------------------------------------------------|
| `tests_primitives.rs`    | Round-trip for all primitive types, edge values, endianness |
| `tests_collections.rs`   | `Vec<u8>`, `String`, `PathBuf`, `Option<T>`, nested types   |
| `tests_limits.rs`        | Safety limit enforcement, truncation, invalid tags/bools    |

## Design Decisions

1. **No dependency** — the encoding module has zero external dependencies (only `std`). This eliminates supply-chain risk and ensures the wire format is fully stable.

2. **Fallible everywhere** — every `encode_to` and `decode_from` returns `Result`. There are no panicking paths, making the module safe to use with untrusted or corrupted data.

3. **Little-endian** — matches the native byte order of x86-64 and ARM, so encoding/decoding on these platforms can often compile down to a no-op or a simple `memcpy`.

4. **`u32` lengths** — chosen as a pragmatic balance between range (4 GiB max) and overhead (4 bytes per length prefix). No AeternusDB value needs to exceed 4 GiB.

5. **Specialised `Vec<u8>`** — byte vectors use a raw-copy path (`[u32 len][bytes]`) rather than per-element encoding, avoiding the overhead of encoding each byte individually.

6. **`bytes_consumed` return** — `Decode::decode_from` returns `(value, bytes_consumed)` instead of mutating a cursor, keeping the API simple and composable without requiring mutable state.
