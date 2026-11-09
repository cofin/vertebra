//! Oracle TTC piggybacked session attributes and 26ai `VECTOR` and `OSON` binary codecs.

pub mod tns;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// Oracle 26ai binary `OSON` magic header (`\xffJZ`).
pub const OSON_MAGIC: &[u8; 3] = b"\xff\x4a\x5a";

/// Oracle 26ai binary `VECTOR` magic byte (`0xDB`).
pub const ORACLE_VECTOR_MAGIC: u8 = 0xDB;

/// Oracle 26ai binary `VECTOR` wire version (`0x01`).
pub const ORACLE_VECTOR_VERSION: u8 = 0x01;

/// Zero-RTT piggybacked Two-Task Common (TTC) end-to-end tracing and session attributes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleSessionAttrs {
    pub client_identifier: Option<String>,
    pub module: Option<String>,
    pub action: Option<String>,
    pub ecid: Option<String>,
}

impl OracleSessionAttrs {
    /// Encode a TTC piggyback frame (`0x11` `TTCFUN_PIGGYBACK_SESSION_ATTRS`) carrying modified session attributes.
    pub fn encode_ttc_piggyback_session_attrs(&self) -> Bytes {
        let mut fields: Vec<(u8, &str)> = Vec::with_capacity(4);
        if let Some(ref cid) = self.client_identifier {
            fields.push((1, cid));
        }
        if let Some(ref module) = self.module {
            fields.push((2, module));
        }
        if let Some(ref action) = self.action {
            fields.push((3, action));
        }
        if let Some(ref ecid) = self.ecid {
            fields.push((4, ecid));
        }

        let mut buf = BytesMut::with_capacity(64);
        buf.put_u8(0x11); // TTC piggyback marker
        buf.put_u8(0x6e); // Session attribute piggyback function code
        buf.put_u8(fields.len() as u8);
        for (attr_id, val) in fields {
            let bytes = val.as_bytes();
            buf.put_u8(attr_id);
            buf.put_u16(bytes.len() as u16);
            buf.extend_from_slice(bytes);
        }
        buf.freeze()
    }
}

/// Oracle 26ai `VECTOR` element storage format codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OracleVectorFormat {
    Float32 = 2,
    Float64 = 3,
    Int8 = 4,
    Binary = 5,
}

fn encode_vector_header(buf: &mut BytesMut, format: OracleVectorFormat, dims: u32) {
    buf.put_u8(ORACLE_VECTOR_MAGIC);
    buf.put_u8(ORACLE_VECTOR_VERSION);
    buf.put_u8(format as u8);
    buf.put_u8(0); // flags
    buf.put_u32(dims);
}

fn decode_vector_header(
    buf: &[u8],
    expected_format: OracleVectorFormat,
) -> Result<(u32, &[u8]), String> {
    if buf.len() < 8 {
        return Err("buffer too short for 8-byte Oracle VECTOR header".to_string());
    }
    if buf[0] != ORACLE_VECTOR_MAGIC {
        return Err(format!(
            "invalid Oracle VECTOR magic byte: 0x{:02x} (expected 0xDB)",
            buf[0]
        ));
    }
    if buf[1] != ORACLE_VECTOR_VERSION {
        return Err(format!(
            "unsupported Oracle VECTOR wire version: {}",
            buf[1]
        ));
    }
    if buf[2] != expected_format as u8 {
        return Err(format!(
            "Oracle VECTOR format mismatch: expected {}, got {}",
            expected_format as u8, buf[2]
        ));
    }
    let dims = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    Ok((dims, &buf[8..]))
}

/// Encode an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame.
pub fn encode_oracle_vector_f32(values: &[f32]) -> Bytes {
    let mut buf = BytesMut::with_capacity(8 + values.len() * 4);
    encode_vector_header(&mut buf, OracleVectorFormat::Float32, values.len() as u32);
    for &v in values {
        buf.put_f32(v);
    }
    buf.freeze()
}

/// Decode an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame.
pub fn decode_oracle_vector_f32(buf: &[u8]) -> Result<Vec<f32>, String> {
    let (dims, body) = decode_vector_header(buf, OracleVectorFormat::Float32)?;
    let expected_bytes = (dims as usize) * 4;
    if body.len() != expected_bytes {
        return Err(format!(
            "Oracle FLOAT32 VECTOR payload length mismatch: expected {expected_bytes} bytes, got {}",
            body.len()
        ));
    }
    let mut out = Vec::with_capacity(dims as usize);
    for &chunk in body.as_chunks::<4>().0 {
        out.push(f32::from_be_bytes(chunk));
    }
    Ok(out)
}

/// Encode an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame.
pub fn encode_oracle_vector_f64(values: &[f64]) -> Bytes {
    let mut buf = BytesMut::with_capacity(8 + values.len() * 8);
    encode_vector_header(&mut buf, OracleVectorFormat::Float64, values.len() as u32);
    for &v in values {
        buf.put_f64(v);
    }
    buf.freeze()
}

/// Decode an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame.
pub fn decode_oracle_vector_f64(buf: &[u8]) -> Result<Vec<f64>, String> {
    let (dims, body) = decode_vector_header(buf, OracleVectorFormat::Float64)?;
    let expected_bytes = (dims as usize) * 8;
    if body.len() != expected_bytes {
        return Err(format!(
            "Oracle FLOAT64 VECTOR payload length mismatch: expected {expected_bytes} bytes, got {}",
            body.len()
        ));
    }
    let mut out = Vec::with_capacity(dims as usize);
    for &chunk in body.as_chunks::<8>().0 {
        out.push(f64::from_be_bytes(chunk));
    }
    Ok(out)
}

/// Encode an Oracle 26ai `VECTOR(*, INT8)` binary frame.
pub fn encode_oracle_vector_i8(values: &[i8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(8 + values.len());
    encode_vector_header(&mut buf, OracleVectorFormat::Int8, values.len() as u32);
    for &v in values {
        buf.put_i8(v);
    }
    buf.freeze()
}

/// Decode an Oracle 26ai `VECTOR(*, INT8)` binary frame.
pub fn decode_oracle_vector_i8(buf: &[u8]) -> Result<Vec<i8>, String> {
    let (dims, body) = decode_vector_header(buf, OracleVectorFormat::Int8)?;
    if body.len() != dims as usize {
        return Err(format!(
            "Oracle INT8 VECTOR payload length mismatch: expected {dims} bytes, got {}",
            body.len()
        ));
    }
    Ok(body.iter().map(|&b| b as i8).collect())
}

/// Encode an Oracle 26ai `VECTOR(*, BINARY)` packed bit-vector binary frame (`bits.len() * 8` dimensions).
pub fn encode_oracle_vector_binary(bits: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(8 + bits.len());
    encode_vector_header(
        &mut buf,
        OracleVectorFormat::Binary,
        (bits.len() * 8) as u32,
    );
    buf.extend_from_slice(bits);
    buf.freeze()
}

/// Decode an Oracle 26ai `VECTOR(*, BINARY)` packed bit-vector binary frame.
pub fn decode_oracle_vector_binary(buf: &[u8]) -> Result<Vec<u8>, String> {
    let (dims, body) = decode_vector_header(buf, OracleVectorFormat::Binary)?;
    let expected_bytes = (dims as usize) / 8;
    if body.len() != expected_bytes {
        return Err(format!(
            "Oracle BINARY VECTOR payload length mismatch: expected {expected_bytes} bytes, got {}",
            body.len()
        ));
    }
    Ok(body.to_vec())
}

/// Encode JSON bytes with an Oracle 26ai `OSON` binary header (`\xff\x4a\x5a\x01` + 4-byte BE length).
pub fn encode_oson_payload(json_bytes: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(8 + json_bytes.len());
    buf.extend_from_slice(OSON_MAGIC);
    buf.put_u8(0x01); // OSON version 1
    buf.put_u32(json_bytes.len() as u32);
    buf.extend_from_slice(json_bytes);
    buf.freeze()
}

/// Validate an Oracle 26ai `OSON` binary header (`\xff\x4a\x5a`) and return the underlying JSON payload slice.
pub fn decode_oson_payload(buf: &[u8]) -> Result<&[u8], String> {
    if buf.len() < 8 {
        return Err("buffer too short for 8-byte OSON header".to_string());
    }
    if &buf[0..3] != OSON_MAGIC {
        return Err("invalid OSON magic header (expected \\xff\\x4a\\x5a)".to_string());
    }
    let payload_len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    if buf.len() < 8 + payload_len {
        return Err(format!(
            "truncated OSON payload: expected {payload_len} bytes, got {}",
            buf.len() - 8
        ));
    }
    Ok(&buf[8..8 + payload_len])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttc_piggyback_session_attrs() {
        let attrs = OracleSessionAttrs {
            client_identifier: Some("user-42".to_string()),
            module: Some("vertebra-api".to_string()),
            action: Some("GET /orders".to_string()),
            ecid: Some("00af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
        };
        let encoded = attrs.encode_ttc_piggyback_session_attrs();
        assert_eq!(encoded[0], 0x11);
        assert_eq!(encoded[1], 0x6e);
        assert_eq!(encoded[2], 4);
    }

    #[test]
    fn test_oracle_26ai_vector_codecs() {
        let v_f32 = vec![0.25f32, -0.5, 1.0, 3.5];
        let enc_f32 = encode_oracle_vector_f32(&v_f32);
        assert_eq!(decode_oracle_vector_f32(&enc_f32).unwrap(), v_f32);

        let v_f64 = vec![1.125f64, -2.5, 42.0];
        let enc_f64 = encode_oracle_vector_f64(&v_f64);
        assert_eq!(decode_oracle_vector_f64(&enc_f64).unwrap(), v_f64);

        let v_i8 = vec![-128i8, -1, 0, 1, 127];
        let enc_i8 = encode_oracle_vector_i8(&v_i8);
        assert_eq!(decode_oracle_vector_i8(&enc_i8).unwrap(), v_i8);

        let v_bin = vec![0b1010_0101u8, 0b1111_0000];
        let enc_bin = encode_oracle_vector_binary(&v_bin);
        assert_eq!(decode_oracle_vector_binary(&enc_bin).unwrap(), v_bin);
    }

    #[test]
    fn test_oracle_26ai_oson_codec() {
        let json = br#"{"doc_id":101,"title":"Oracle 26ai Duality View"}"#;
        let oson = encode_oson_payload(json);
        assert_eq!(&oson[0..3], OSON_MAGIC);
        let decoded = decode_oson_payload(&oson).unwrap();
        assert_eq!(decoded, json);
        assert!(decode_oson_payload(b"bad").is_err());
    }
}
