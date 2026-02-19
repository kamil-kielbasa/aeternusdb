//! Tests for primitive type encoding/decoding: integers, bool, fixed arrays,
//! byte slices, strings, PathBuf.

use std::path::PathBuf;

use crate::encoding::*;

// ------------------------------------------------------------------------------------------------
// u8
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_u8() {
    let val: u8 = 0xAB;
    let bytes = encode_to_vec(&val).unwrap();
    assert_eq!(bytes, [0xAB]);
    let (decoded, consumed) = decode_from_slice::<u8>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 1);
}

// ------------------------------------------------------------------------------------------------
// u16
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_u16() {
    let val: u16 = 0x1234;
    let bytes = encode_to_vec(&val).unwrap();
    assert_eq!(bytes, [0x34, 0x12]); // little-endian
    let (decoded, consumed) = decode_from_slice::<u16>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 2);
}

// ------------------------------------------------------------------------------------------------
// u32
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_u32() {
    let val: u32 = 0xDEAD_BEEF;
    let bytes = encode_to_vec(&val).unwrap();
    assert_eq!(bytes, [0xEF, 0xBE, 0xAD, 0xDE]);
    let (decoded, consumed) = decode_from_slice::<u32>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 4);
}

// ------------------------------------------------------------------------------------------------
// u64
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_u64() {
    let val: u64 = 0x0102_0304_0506_0708;
    let bytes = encode_to_vec(&val).unwrap();
    assert_eq!(bytes, [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
    let (decoded, consumed) = decode_from_slice::<u64>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 8);
}

// ------------------------------------------------------------------------------------------------
// i64
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_i64() {
    for val in [0i64, 1, -1, i64::MIN, i64::MAX] {
        let bytes = encode_to_vec(&val).unwrap();
        let (decoded, consumed) = decode_from_slice::<i64>(&bytes).unwrap();
        assert_eq!(decoded, val);
        assert_eq!(consumed, 8);
    }
}

// ------------------------------------------------------------------------------------------------
// bool
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_bool() {
    let bytes_true = encode_to_vec(&true).unwrap();
    let bytes_false = encode_to_vec(&false).unwrap();
    assert_eq!(bytes_true, [1]);
    assert_eq!(bytes_false, [0]);
    assert_eq!(decode_from_slice::<bool>(&bytes_true).unwrap(), (true, 1));
    assert_eq!(decode_from_slice::<bool>(&bytes_false).unwrap(), (false, 1));
}

#[test]
fn bool_invalid_byte() {
    let err = decode_from_slice::<bool>(&[2]).unwrap_err();
    assert!(matches!(err, EncodingError::InvalidBool(2)));

    let err = decode_from_slice::<bool>(&[0xFF]).unwrap_err();
    assert!(matches!(err, EncodingError::InvalidBool(0xFF)));
}

// ------------------------------------------------------------------------------------------------
// [u8; N] — fixed-size byte arrays
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_fixed_array() {
    let val: [u8; 4] = *b"SST0";
    let bytes = encode_to_vec(&val).unwrap();
    assert_eq!(bytes, b"SST0");
    let (decoded, consumed) = decode_from_slice::<[u8; 4]>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 4);
}

#[test]
fn roundtrip_fixed_array_empty() {
    let val: [u8; 0] = [];
    let bytes = encode_to_vec(&val).unwrap();
    assert!(bytes.is_empty());
    let (decoded, consumed) = decode_from_slice::<[u8; 0]>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 0);
}

// ------------------------------------------------------------------------------------------------
// Vec<u8>
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_vec_u8() {
    let val: Vec<u8> = vec![10, 20, 30];
    let bytes = encode_to_vec(&val).unwrap();
    // 4-byte length (3, LE) + 3 data bytes
    assert_eq!(bytes, [3, 0, 0, 0, 10, 20, 30]);
    let (decoded, consumed) = decode_from_slice::<Vec<u8>>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 7);
}

#[test]
fn roundtrip_vec_u8_empty() {
    let val: Vec<u8> = vec![];
    let bytes = encode_to_vec(&val).unwrap();
    assert_eq!(bytes, [0, 0, 0, 0]);
    let (decoded, consumed) = decode_from_slice::<Vec<u8>>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 4);
}

// ------------------------------------------------------------------------------------------------
// &[u8] encode
// ------------------------------------------------------------------------------------------------

#[test]
fn encode_byte_slice() {
    let data: &[u8] = b"hello";
    let mut buf = Vec::new();
    data.encode_to(&mut buf).unwrap();
    let (decoded, consumed) = decode_from_slice::<Vec<u8>>(&buf).unwrap();
    assert_eq!(decoded, b"hello");
    assert_eq!(consumed, 4 + 5);
}

// ------------------------------------------------------------------------------------------------
// String
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_string() {
    let val = String::from("AeternusDB");
    let bytes = encode_to_vec(&val).unwrap();
    let (decoded, consumed) = decode_from_slice::<String>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 4 + val.len());
}

#[test]
fn roundtrip_string_empty() {
    let val = String::new();
    let bytes = encode_to_vec(&val).unwrap();
    let (decoded, consumed) = decode_from_slice::<String>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 4);
}

#[test]
fn string_invalid_utf8() {
    // length = 2, then two invalid UTF-8 bytes
    let buf = [2, 0, 0, 0, 0xFF, 0xFE];
    let err = decode_from_slice::<String>(&buf).unwrap_err();
    assert!(matches!(err, EncodingError::InvalidUtf8(_)));
}

// ------------------------------------------------------------------------------------------------
// &str encode
// ------------------------------------------------------------------------------------------------

#[test]
fn encode_str_ref() {
    let s = "test";
    let mut buf = Vec::new();
    s.encode_to(&mut buf).unwrap();
    let (decoded, consumed) = decode_from_slice::<String>(&buf).unwrap();
    assert_eq!(decoded, "test");
    assert_eq!(consumed, 4 + 4);
}

// ------------------------------------------------------------------------------------------------
// PathBuf — raw OS bytes, deterministic
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_pathbuf() {
    let val = PathBuf::from("/data/sst/000001.sst");
    let bytes = encode_to_vec(&val).unwrap();
    let (decoded, consumed) = decode_from_slice::<PathBuf>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, bytes.len());
}

#[test]
fn roundtrip_pathbuf_empty() {
    let val = PathBuf::from("");
    let bytes = encode_to_vec(&val).unwrap();
    let (decoded, consumed) = decode_from_slice::<PathBuf>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 4);
}

#[test]
fn pathbuf_non_utf8_roundtrips() {
    // On Unix, paths can contain arbitrary bytes.
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let raw_bytes: &[u8] = &[0xFF, 0xFE, b'/', b'a'];
    let val = PathBuf::from(OsStr::from_bytes(raw_bytes));
    let bytes = encode_to_vec(&val).unwrap();
    let (decoded, consumed) = decode_from_slice::<PathBuf>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, bytes.len());
}

// ------------------------------------------------------------------------------------------------
// Truncated buffer errors
// ------------------------------------------------------------------------------------------------

#[test]
fn decode_u32_truncated() {
    let err = decode_from_slice::<u32>(&[1, 2]).unwrap_err();
    match err {
        EncodingError::UnexpectedEof {
            needed: 4,
            available: 2,
        } => {}
        other => panic!("expected UnexpectedEof, got: {other:?}"),
    }
}

#[test]
fn decode_vec_u8_truncated_payload() {
    let err = decode_from_slice::<Vec<u8>>(&[10, 0, 0, 0, 1, 2]).unwrap_err();
    match err {
        EncodingError::UnexpectedEof { needed: 10, .. } => {}
        other => panic!("expected UnexpectedEof, got: {other:?}"),
    }
}

#[test]
fn decode_empty_buffer() {
    let err = decode_from_slice::<u8>(&[]).unwrap_err();
    assert!(matches!(
        err,
        EncodingError::UnexpectedEof {
            needed: 1,
            available: 0,
        }
    ));
}

// ------------------------------------------------------------------------------------------------
// Multiple values back-to-back
// ------------------------------------------------------------------------------------------------

#[test]
fn multiple_values_sequential() {
    let mut buf = Vec::new();
    42u32.encode_to(&mut buf).unwrap();
    true.encode_to(&mut buf).unwrap();
    b"key".as_slice().encode_to(&mut buf).unwrap();
    99u64.encode_to(&mut buf).unwrap();

    let mut offset = 0;

    let (v1, n) = u32::decode_from(&buf[offset..]).unwrap();
    offset += n;
    assert_eq!(v1, 42);

    let (v2, n) = bool::decode_from(&buf[offset..]).unwrap();
    offset += n;
    assert!(v2);

    let (v3, n) = Vec::<u8>::decode_from(&buf[offset..]).unwrap();
    offset += n;
    assert_eq!(v3, b"key");

    let (v4, n) = u64::decode_from(&buf[offset..]).unwrap();
    offset += n;
    assert_eq!(v4, 99);

    assert_eq!(offset, buf.len());
}

// ------------------------------------------------------------------------------------------------
// Determinism
// ------------------------------------------------------------------------------------------------

#[test]
fn encoding_is_deterministic() {
    let val: Option<Vec<u8>> = Some(vec![1, 2, 3]);
    let a = encode_to_vec(&val).unwrap();
    let b = encode_to_vec(&val).unwrap();
    assert_eq!(a, b, "encoding must be deterministic");
}
