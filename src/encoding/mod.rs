//! Deterministic, zero-dependency binary encoding for on-disk persistence.
//!
//! This module provides the [`Encode`] and [`Decode`] traits that replace
//! external serialization libraries (e.g. bincode) with a hand-written,
//! byte-stable wire format.  Because AeternusDB owns this format, the
//! on-disk representation **never** changes due to a dependency upgrade.
//!
//! # Wire format
//!
//! | Rust type          | Encoding                                     |
//! |--------------------|----------------------------------------------|
//! | `u8`               | 1 byte                                       |
//! | `u16`              | 2 bytes, little-endian                       |
//! | `u32`              | 4 bytes, little-endian                       |
//! | `u64`              | 8 bytes, little-endian                       |
//! | `i64`              | 8 bytes, little-endian                       |
//! | `bool`             | 1 byte (`0x00` = false, `0x01` = true)       |
//! | `[u8; N]`          | `N` raw bytes (fixed-size, no length prefix) |
//! | `Vec<u8>` / bytes  | `[u32 len][bytes]`                           |
//! | `String`           | `[u32 len][utf-8 bytes]`                     |
//! | `PathBuf`          | `[u32 len][raw bytes]`                       |
//! | `Option<T>`        | `[u8 tag: 0=None, 1=Some][T if Some]`        |
//! | `Vec<T>`           | `[u32 count][T₁][T₂]…`                      |
//! | `enum`             | `[u32 variant][fields…]` (hand-written)      |
//!
//! All multi-byte integers are **little-endian**.  Lengths and counts
//! are encoded as `u32`, limiting individual items to 4 GiB.
//!
//! # Safety limits
//!
//! To prevent denial-of-service via crafted inputs, all variable-length
//! decoders enforce upper bounds:
//!
//! - [`MAX_BYTE_LEN`]: maximum byte length for `Vec<u8>`, `String`, `PathBuf`
//!   (default: 256 MiB).
//! - [`MAX_VEC_ELEMENTS`]: maximum element count for `Vec<T>` (default: 16 M).
//!
//! # Zero-panic guarantee
//!
//! No function in this module uses `unwrap()`, `expect()`, or any other
//! panicking path.  All errors are propagated via [`EncodingError`].
//!
//! # Convenience helpers
//!
//! ```rust,ignore
//! use aeternusdb::encoding::{encode_to_vec, decode_from_slice};
//!
//! let bytes = encode_to_vec(&my_struct)?;
//! let (decoded, consumed) = decode_from_slice::<MyStruct>(&bytes)?;
//! ```

#[cfg(test)]
mod tests;

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

use thiserror::Error;

// ------------------------------------------------------------------------------------------------
// Safety limits
// ------------------------------------------------------------------------------------------------

/// Maximum byte length for a single `Vec<u8>`, `String`, or `PathBuf`
/// during decoding (256 MiB).
///
/// Any decoded length field exceeding this value is rejected immediately,
/// preventing allocation bombs from corrupted or malicious data.
pub const MAX_BYTE_LEN: u32 = 256 * 1024 * 1024;

/// Maximum element count for `Vec<T>` (non-`u8`) during decoding (16 M).
///
/// This prevents allocation bombs when decoding vectors of structs.
pub const MAX_VEC_ELEMENTS: u32 = 16 * 1024 * 1024;

// ------------------------------------------------------------------------------------------------
// Error type
// ------------------------------------------------------------------------------------------------

/// Errors produced during encoding or decoding.
#[derive(Debug, Error)]
pub enum EncodingError {
    /// The buffer ran out of bytes before decoding completed.
    #[error("unexpected end of buffer (need {needed} bytes, have {available})")]
    UnexpectedEof {
        /// Bytes required to continue decoding.
        needed: usize,
        /// Bytes actually remaining.
        available: usize,
    },

    /// An enum discriminant was not recognised.
    #[error("invalid tag {tag} for {type_name}")]
    InvalidTag {
        /// The tag value that was read.
        tag: u32,
        /// The Rust type being decoded.
        type_name: &'static str,
    },

    /// A bool field contained a byte other than `0x00` or `0x01`.
    #[error("invalid bool byte: 0x{0:02X} (expected 0x00 or 0x01)")]
    InvalidBool(u8),

    /// A byte-sequence decoded as a string was not valid UTF-8.
    #[error("invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    /// A length or count exceeded its safety limit.
    #[error("length overflow: {0}")]
    LengthOverflow(String),

    /// Application-level decode error.
    #[error("{0}")]
    Custom(String),
}

// ------------------------------------------------------------------------------------------------
// Core traits
// ------------------------------------------------------------------------------------------------

/// Serialize `self` into a byte buffer.
///
/// Implementations **must** produce deterministic output: the same
/// logical value always yields the exact same byte sequence.
///
/// Returns `Err` if a value cannot be represented in the wire format
/// (e.g. a `Vec<u8>` longer than `u32::MAX` bytes).
pub trait Encode {
    /// Append the encoded representation of `self` to `buf`.
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError>;
}

/// Deserialize a value from a byte slice.
///
/// Returns `(value, bytes_consumed)` on success so that callers can
/// advance a cursor through a buffer containing multiple encoded items.
pub trait Decode: Sized {
    /// Decode one value starting at `buf[0]`.
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError>;
}

// ------------------------------------------------------------------------------------------------
// Convenience functions
// ------------------------------------------------------------------------------------------------

/// Encode a value into a freshly-allocated `Vec<u8>`.
pub fn encode_to_vec<T: Encode>(value: &T) -> Result<Vec<u8>, EncodingError> {
    let mut buf = Vec::new();
    value.encode_to(&mut buf)?;
    Ok(buf)
}

/// Decode a value from the beginning of `buf`.
///
/// Returns `(value, bytes_consumed)`.
pub fn decode_from_slice<T: Decode>(buf: &[u8]) -> Result<(T, usize), EncodingError> {
    T::decode_from(buf)
}

// ------------------------------------------------------------------------------------------------
// Internal helpers
// ------------------------------------------------------------------------------------------------

/// Verify that `buf` has at least `needed` bytes, returning
/// [`EncodingError::UnexpectedEof`] if not.
#[inline]
fn require(buf: &[u8], needed: usize) -> Result<(), EncodingError> {
    if buf.len() < needed {
        Err(EncodingError::UnexpectedEof {
            needed,
            available: buf.len(),
        })
    } else {
        Ok(())
    }
}

/// Convert a `usize` length to `u32`, returning [`EncodingError::LengthOverflow`]
/// if the value exceeds `u32::MAX`.
#[inline]
fn len_to_u32(len: usize) -> Result<u32, EncodingError> {
    u32::try_from(len)
        .map_err(|_| EncodingError::LengthOverflow(format!("length {len} exceeds u32::MAX")))
}

// ------------------------------------------------------------------------------------------------
// Primitive implementations — unsigned integers
// ------------------------------------------------------------------------------------------------

impl Encode for u8 {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        buf.push(*self);
        Ok(())
    }
}

impl Decode for u8 {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        require(buf, 1)?;
        Ok((buf[0], 1))
    }
}

impl Encode for u16 {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        buf.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl Decode for u16 {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        require(buf, 2)?;
        Ok((u16::from_le_bytes([buf[0], buf[1]]), 2))
    }
}

impl Encode for u32 {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        buf.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl Decode for u32 {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        require(buf, 4)?;
        // SAFETY: `require` guarantees `buf.len() >= 4`, so indexing
        // and the `try_into` on a 4-element slice cannot fail.
        let bytes: [u8; 4] = match buf[..4].try_into() {
            Ok(b) => b,
            Err(_) => {
                return Err(EncodingError::Custom(
                    "internal: slice-to-array conversion failed for u32".into(),
                ));
            }
        };
        Ok((u32::from_le_bytes(bytes), 4))
    }
}

impl Encode for u64 {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        buf.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl Decode for u64 {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        require(buf, 8)?;
        let bytes: [u8; 8] = match buf[..8].try_into() {
            Ok(b) => b,
            Err(_) => {
                return Err(EncodingError::Custom(
                    "internal: slice-to-array conversion failed for u64".into(),
                ));
            }
        };
        Ok((u64::from_le_bytes(bytes), 8))
    }
}

// ------------------------------------------------------------------------------------------------
// Primitive implementations — signed integers
// ------------------------------------------------------------------------------------------------

impl Encode for i64 {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        buf.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl Decode for i64 {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        require(buf, 8)?;
        let bytes: [u8; 8] = match buf[..8].try_into() {
            Ok(b) => b,
            Err(_) => {
                return Err(EncodingError::Custom(
                    "internal: slice-to-array conversion failed for i64".into(),
                ));
            }
        };
        Ok((i64::from_le_bytes(bytes), 8))
    }
}

// ------------------------------------------------------------------------------------------------
// Primitive implementations — bool
// ------------------------------------------------------------------------------------------------

impl Encode for bool {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        buf.push(u8::from(*self));
        Ok(())
    }
}

impl Decode for bool {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        require(buf, 1)?;
        match buf[0] {
            0 => Ok((false, 1)),
            1 => Ok((true, 1)),
            other => Err(EncodingError::InvalidBool(other)),
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Fixed-size byte arrays
// ------------------------------------------------------------------------------------------------

impl<const N: usize> Encode for [u8; N] {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        buf.extend_from_slice(self);
        Ok(())
    }
}

impl<const N: usize> Decode for [u8; N] {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        require(buf, N)?;
        let mut arr = [0u8; N];
        arr.copy_from_slice(&buf[..N]);
        Ok((arr, N))
    }
}

// ------------------------------------------------------------------------------------------------
// Variable-length byte vectors: [u32 len][bytes]
// ------------------------------------------------------------------------------------------------

impl Encode for Vec<u8> {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        len_to_u32(self.len())?.encode_to(buf)?;
        buf.extend_from_slice(self);
        Ok(())
    }
}

impl Decode for Vec<u8> {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (len, mut offset) = u32::decode_from(buf)?;
        if len > MAX_BYTE_LEN {
            return Err(EncodingError::LengthOverflow(format!(
                "byte vector length {len} exceeds MAX_BYTE_LEN ({MAX_BYTE_LEN})"
            )));
        }
        let len = len as usize;
        require(&buf[offset..], len)?;
        let data = buf[offset..offset + len].to_vec();
        offset += len;
        Ok((data, offset))
    }
}

/// Encode a byte slice as `[u32 len][bytes]`.
///
/// Useful for encoding `&[u8]` fields without owning a `Vec`.
impl Encode for &[u8] {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        len_to_u32(self.len())?.encode_to(buf)?;
        buf.extend_from_slice(self);
        Ok(())
    }
}

// ------------------------------------------------------------------------------------------------
// Strings: [u32 len][utf-8 bytes]
// ------------------------------------------------------------------------------------------------

impl Encode for String {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        len_to_u32(self.len())?.encode_to(buf)?;
        buf.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

impl Decode for String {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (raw, consumed) = Vec::<u8>::decode_from(buf)?;
        let s = String::from_utf8(raw)?;
        Ok((s, consumed))
    }
}

impl Encode for &str {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        len_to_u32(self.len())?.encode_to(buf)?;
        buf.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

// ------------------------------------------------------------------------------------------------
// PathBuf: [u32 len][raw OS bytes]
//
// On Unix, `OsStr` is an arbitrary byte sequence (via `OsStrExt`).
// We store the raw bytes verbatim — no lossy conversion, fully
// deterministic, and round-trips perfectly on the same platform.
//
// If cross-platform portability is ever needed, paths should be
// normalised to UTF-8 at the application layer before encoding.
// ------------------------------------------------------------------------------------------------

impl Encode for PathBuf {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        let raw = self.as_os_str().as_bytes();
        len_to_u32(raw.len())?.encode_to(buf)?;
        buf.extend_from_slice(raw);
        Ok(())
    }
}

impl Decode for PathBuf {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (len, mut offset) = u32::decode_from(buf)?;
        if len > MAX_BYTE_LEN {
            return Err(EncodingError::LengthOverflow(format!(
                "path length {len} exceeds MAX_BYTE_LEN ({MAX_BYTE_LEN})"
            )));
        }
        let len = len as usize;
        require(&buf[offset..], len)?;
        let os_str = OsStr::from_bytes(&buf[offset..offset + len]);
        offset += len;
        Ok((PathBuf::from(os_str), offset))
    }
}

// ------------------------------------------------------------------------------------------------
// Option<T>: [u8 tag][T if Some]
// ------------------------------------------------------------------------------------------------

impl<T: Encode> Encode for Option<T> {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        match self {
            None => buf.push(0),
            Some(val) => {
                buf.push(1);
                val.encode_to(buf)?;
            }
        }
        Ok(())
    }
}

impl<T: Decode> Decode for Option<T> {
    #[inline]
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        require(buf, 1)?;
        match buf[0] {
            0 => Ok((None, 1)),
            1 => {
                let (val, consumed) = T::decode_from(&buf[1..])?;
                Ok((Some(val), 1 + consumed))
            }
            other => Err(EncodingError::InvalidTag {
                tag: other as u32,
                type_name: "Option<T>",
            }),
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Vec<T>: [u32 count][T₁][T₂]…
//
// NOTE: `Vec<u8>` has a specialised impl above (raw bytes, no per-element
// overhead).  Rust's coherence rules prevent a direct blanket impl from
// overlapping with `Vec<u8>`, so we provide free functions that
// higher-level code calls for vectors of structs.
// ------------------------------------------------------------------------------------------------

/// Encode a slice of `T` as `[u32 count][T₁][T₂]…`.
pub fn encode_vec<T: Encode>(items: &[T], buf: &mut Vec<u8>) -> Result<(), EncodingError> {
    len_to_u32(items.len())?.encode_to(buf)?;
    for item in items {
        item.encode_to(buf)?;
    }
    Ok(())
}

/// Decode a `Vec<T>` from `[u32 count][T₁][T₂]…`.
///
/// The element count is capped at [`MAX_VEC_ELEMENTS`] to prevent
/// allocation bombs from corrupted data.
pub fn decode_vec<T: Decode>(buf: &[u8]) -> Result<(Vec<T>, usize), EncodingError> {
    let (count, mut offset) = u32::decode_from(buf)?;
    if count > MAX_VEC_ELEMENTS {
        return Err(EncodingError::LengthOverflow(format!(
            "vector element count {count} exceeds MAX_VEC_ELEMENTS ({MAX_VEC_ELEMENTS})"
        )));
    }
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let (item, consumed) = T::decode_from(&buf[offset..])?;
        offset += consumed;
        items.push(item);
    }
    Ok((items, offset))
}
