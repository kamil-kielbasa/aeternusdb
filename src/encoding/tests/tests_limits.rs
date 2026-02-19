//! Tests for safety limits: MAX_BYTE_LEN, MAX_VEC_ELEMENTS,
//! and LengthOverflow error paths.

use crate::encoding::*;

// ------------------------------------------------------------------------------------------------
// Vec<u8> decode — length exceeds MAX_BYTE_LEN
// ------------------------------------------------------------------------------------------------

#[test]
fn vec_u8_decode_exceeds_max_byte_len() {
    // Craft a buffer that claims length = MAX_BYTE_LEN + 1
    let bogus_len = MAX_BYTE_LEN + 1;
    let buf = bogus_len.to_le_bytes();
    let err = decode_from_slice::<Vec<u8>>(&buf).unwrap_err();
    assert!(
        matches!(err, EncodingError::LengthOverflow(_)),
        "expected LengthOverflow, got: {err:?}"
    );
}

#[test]
fn vec_u8_decode_at_max_byte_len_needs_data() {
    // length = MAX_BYTE_LEN (valid limit) but no data follows → UnexpectedEof
    let buf = MAX_BYTE_LEN.to_le_bytes();
    let err = decode_from_slice::<Vec<u8>>(&buf).unwrap_err();
    assert!(
        matches!(err, EncodingError::UnexpectedEof { .. }),
        "expected UnexpectedEof, got: {err:?}"
    );
}

// ------------------------------------------------------------------------------------------------
// String decode — length exceeds MAX_BYTE_LEN (goes through Vec<u8>)
// ------------------------------------------------------------------------------------------------

#[test]
fn string_decode_exceeds_max_byte_len() {
    let bogus_len = MAX_BYTE_LEN + 1;
    let buf = bogus_len.to_le_bytes();
    let err = decode_from_slice::<String>(&buf).unwrap_err();
    assert!(matches!(err, EncodingError::LengthOverflow(_)));
}

// ------------------------------------------------------------------------------------------------
// PathBuf decode — length exceeds MAX_BYTE_LEN
// ------------------------------------------------------------------------------------------------

#[test]
fn pathbuf_decode_exceeds_max_byte_len() {
    let bogus_len = MAX_BYTE_LEN + 1;
    let buf = bogus_len.to_le_bytes();
    let err = decode_from_slice::<std::path::PathBuf>(&buf).unwrap_err();
    assert!(matches!(err, EncodingError::LengthOverflow(_)));
}

// ------------------------------------------------------------------------------------------------
// decode_vec — count exceeds MAX_VEC_ELEMENTS
// ------------------------------------------------------------------------------------------------

/// Simple type for testing.
#[derive(Debug, PartialEq)]
struct Dummy(u8);

impl Encode for Dummy {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        self.0.encode_to(buf)
    }
}

impl Decode for Dummy {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (v, n) = u8::decode_from(buf)?;
        Ok((Dummy(v), n))
    }
}

#[test]
fn decode_vec_exceeds_max_elements() {
    let bogus_count = MAX_VEC_ELEMENTS + 1;
    let buf = bogus_count.to_le_bytes();
    let err = decode_vec::<Dummy>(&buf).unwrap_err();
    assert!(
        matches!(err, EncodingError::LengthOverflow(_)),
        "expected LengthOverflow, got: {err:?}"
    );
}

#[test]
fn decode_vec_at_max_elements_needs_data() {
    // count = MAX_VEC_ELEMENTS (valid) but no data follows → UnexpectedEof
    let buf = MAX_VEC_ELEMENTS.to_le_bytes();
    let err = decode_vec::<Dummy>(&buf).unwrap_err();
    assert!(
        matches!(err, EncodingError::UnexpectedEof { .. }),
        "expected UnexpectedEof, got: {err:?}"
    );
}

// ------------------------------------------------------------------------------------------------
// Crafted u32::MAX length — LengthOverflow
// ------------------------------------------------------------------------------------------------

#[test]
fn vec_u8_decode_u32_max_length() {
    let buf = u32::MAX.to_le_bytes();
    let err = decode_from_slice::<Vec<u8>>(&buf).unwrap_err();
    assert!(matches!(err, EncodingError::LengthOverflow(_)));
}

#[test]
fn decode_vec_u32_max_count() {
    let buf = u32::MAX.to_le_bytes();
    let err = decode_vec::<Dummy>(&buf).unwrap_err();
    assert!(matches!(err, EncodingError::LengthOverflow(_)));
}
