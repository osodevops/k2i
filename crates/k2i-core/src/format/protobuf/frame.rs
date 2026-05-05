//! Confluent Schema Registry Protobuf wire-frame parsing.
//!
//! Protobuf frames are `0x00` magic, a 4-byte big-endian schema ID, message
//! indexes, then the protobuf payload. The common top-level first-message
//! path `[0]` is optimized to one zero byte.

use crate::{Error, Result};

/// Parsed Confluent Protobuf frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfluentProtobufFrame<'a> {
    /// Schema Registry ID.
    pub schema_id: i32,
    /// Message index path inside the schema.
    pub message_indexes: Vec<i32>,
    /// Raw protobuf payload bytes.
    pub payload: &'a [u8],
}

impl<'a> ConfluentProtobufFrame<'a> {
    /// Parse a Confluent Protobuf frame.
    pub fn parse(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() < 6 {
            return Err(Error::Serialization(format!(
                "protobuf frame too short: {} bytes",
                bytes.len()
            )));
        }

        if bytes[0] != 0 {
            return Err(Error::Serialization(format!(
                "invalid Confluent magic byte: expected 0, got {}",
                bytes[0]
            )));
        }

        let schema_id = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        let index_start = 5;

        if bytes[index_start] == 0 {
            return Ok(Self {
                schema_id,
                message_indexes: vec![0],
                payload: &bytes[index_start + 1..],
            });
        }

        let (len, mut consumed) = read_signed_varint(&bytes[index_start..])?;
        if len <= 0 {
            return Err(Error::Serialization(format!(
                "invalid protobuf message index length: {}",
                len
            )));
        }

        let mut message_indexes = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let (index, read) = read_signed_varint(&bytes[index_start + consumed..])?;
            if index < 0 {
                return Err(Error::Serialization(format!(
                    "invalid negative protobuf message index: {}",
                    index
                )));
            }
            message_indexes.push(index);
            consumed += read;
        }

        Ok(Self {
            schema_id,
            message_indexes,
            payload: &bytes[index_start + consumed..],
        })
    }
}

fn read_signed_varint(bytes: &[u8]) -> Result<(i32, usize)> {
    let (raw, consumed) = read_unsigned_varint(bytes)?;
    let value = ((raw >> 1) as i32) ^ (-((raw & 1) as i32));
    Ok((value, consumed))
}

fn read_unsigned_varint(bytes: &[u8]) -> Result<(u32, usize)> {
    let mut value = 0u32;
    let mut shift = 0u32;

    for (idx, byte) in bytes.iter().copied().enumerate() {
        let part = u32::from(byte & 0x7f);
        if shift >= 32 && part != 0 {
            return Err(Error::Serialization(
                "protobuf message index varint overflow".to_string(),
            ));
        }
        value |= part << shift;
        if byte & 0x80 == 0 {
            return Ok((value, idx + 1));
        }
        shift += 7;
    }

    Err(Error::Serialization(
        "truncated protobuf message index varint".to_string(),
    ))
}

/// Encode a signed int using protobuf zig-zag varint.
#[cfg(test)]
pub(crate) fn encode_signed_varint(value: i32) -> Vec<u8> {
    let mut raw = ((value << 1) ^ (value >> 31)) as u32;
    let mut out = Vec::new();
    loop {
        if raw < 0x80 {
            out.push(raw as u8);
            break;
        }
        out.push((raw as u8 & 0x7f) | 0x80);
        raw >>= 7;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(schema_id: i32, index_bytes: &[u8], payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0];
        out.extend(schema_id.to_be_bytes());
        out.extend(index_bytes);
        out.extend(payload);
        out
    }

    #[test]
    fn parses_optimized_first_message_index() {
        let bytes = frame(42, &[0], &[8, 150, 1]);
        let parsed = ConfluentProtobufFrame::parse(&bytes).unwrap();
        assert_eq!(parsed.schema_id, 42);
        assert_eq!(parsed.message_indexes, vec![0]);
        assert_eq!(parsed.payload, &[8, 150, 1]);
    }

    #[test]
    fn parses_nested_message_indexes() {
        let mut indexes = encode_signed_varint(2);
        indexes.extend(encode_signed_varint(1));
        indexes.extend(encode_signed_varint(0));

        let bytes = frame(7, &indexes, &[1, 2, 3]);
        let parsed = ConfluentProtobufFrame::parse(&bytes).unwrap();
        assert_eq!(parsed.schema_id, 7);
        assert_eq!(parsed.message_indexes, vec![1, 0]);
        assert_eq!(parsed.payload, &[1, 2, 3]);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = frame(1, &[0], &[1]);
        bytes[0] = 1;
        let err = ConfluentProtobufFrame::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("magic"));
    }

    #[test]
    fn rejects_truncated_frame() {
        let err = ConfluentProtobufFrame::parse(&[0, 1]).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn rejects_truncated_varint() {
        let bytes = frame(1, &[0x80], &[]);
        let err = ConfluentProtobufFrame::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("truncated"));
    }
}
