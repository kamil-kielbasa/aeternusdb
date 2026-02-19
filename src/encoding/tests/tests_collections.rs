//! Tests for collection types: Option<T>, Vec<T> (encode_vec/decode_vec).

use crate::encoding::*;

// ------------------------------------------------------------------------------------------------
// Option<T>
// ------------------------------------------------------------------------------------------------

#[test]
fn roundtrip_option_some() {
    let val: Option<u32> = Some(42);
    let bytes = encode_to_vec(&val).unwrap();
    assert_eq!(bytes, [1, 42, 0, 0, 0]); // tag=1, then LE u32
    let (decoded, consumed) = decode_from_slice::<Option<u32>>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 5);
}

#[test]
fn roundtrip_option_none() {
    let val: Option<u32> = None;
    let bytes = encode_to_vec(&val).unwrap();
    assert_eq!(bytes, [0]);
    let (decoded, consumed) = decode_from_slice::<Option<u32>>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 1);
}

#[test]
fn roundtrip_option_vec_u8_some() {
    let val: Option<Vec<u8>> = Some(vec![1, 2, 3]);
    let bytes = encode_to_vec(&val).unwrap();
    let (decoded, consumed) = decode_from_slice::<Option<Vec<u8>>>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, bytes.len());
}

#[test]
fn roundtrip_option_vec_u8_none() {
    let val: Option<Vec<u8>> = None;
    let bytes = encode_to_vec(&val).unwrap();
    let (decoded, consumed) = decode_from_slice::<Option<Vec<u8>>>(&bytes).unwrap();
    assert_eq!(decoded, val);
    assert_eq!(consumed, 1);
}

#[test]
fn option_invalid_tag() {
    let err = decode_from_slice::<Option<u32>>(&[5]).unwrap_err();
    assert!(matches!(err, EncodingError::InvalidTag { tag: 5, .. }));
}

// ------------------------------------------------------------------------------------------------
// encode_vec / decode_vec  (Vec<T> for non-u8 T)
// ------------------------------------------------------------------------------------------------

/// Small struct for testing composite vec encoding.
#[derive(Debug, PartialEq)]
struct Pair {
    a: u32,
    b: u64,
}

impl Encode for Pair {
    fn encode_to(&self, buf: &mut Vec<u8>) -> Result<(), EncodingError> {
        self.a.encode_to(buf)?;
        self.b.encode_to(buf)?;
        Ok(())
    }
}

impl Decode for Pair {
    fn decode_from(buf: &[u8]) -> Result<(Self, usize), EncodingError> {
        let (a, off1) = u32::decode_from(buf)?;
        let (b, off2) = u64::decode_from(&buf[off1..])?;
        Ok((Pair { a, b }, off1 + off2))
    }
}

#[test]
fn roundtrip_vec_of_structs() {
    let items = vec![
        Pair { a: 1, b: 100 },
        Pair { a: 2, b: 200 },
        Pair { a: 3, b: 300 },
    ];
    let mut buf = Vec::new();
    encode_vec(&items, &mut buf).unwrap();
    let (decoded, consumed) = decode_vec::<Pair>(&buf).unwrap();
    assert_eq!(decoded, items);
    assert_eq!(consumed, buf.len());
}

#[test]
fn roundtrip_vec_of_structs_empty() {
    let items: Vec<Pair> = vec![];
    let mut buf = Vec::new();
    encode_vec(&items, &mut buf).unwrap();
    assert_eq!(buf, [0, 0, 0, 0]); // count = 0
    let (decoded, consumed) = decode_vec::<Pair>(&buf).unwrap();
    assert_eq!(decoded, items);
    assert_eq!(consumed, 4);
}

#[test]
fn roundtrip_vec_of_strings() {
    let items = vec!["hello".to_string(), "world".to_string()];
    let mut buf = Vec::new();
    encode_vec(&items, &mut buf).unwrap();
    let (decoded, consumed) = decode_vec::<String>(&buf).unwrap();
    assert_eq!(decoded, items);
    assert_eq!(consumed, buf.len());
}
