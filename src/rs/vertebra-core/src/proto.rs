//! Compact Protobuf v3 wire reader and writer primitives.
//!
//! Provides zero-dependency varint, field tag, length-delimited slice, and field-skipping
//! codecs shared across `vertebra-core`, `vertebra-db`, and `vertebra-py`.

use bytes::BufMut;

/// Protobuf wire type 0: base-128 varint (`int32`, `int64`, `uint32`, `uint64`, `sint32`, `sint64`, `bool`, `enum`).
pub const WIRE_TYPE_VARINT: u8 = 0;

/// Protobuf wire type 1: 64-bit fixed (`fixed64`, `sfixed64`, `double`).
pub const WIRE_TYPE_FIXED64: u8 = 1;

/// Protobuf wire type 2: length-delimited (`string`, `bytes`, embedded messages, packed repeated fields).
pub const WIRE_TYPE_LENGTH_DELIMITED: u8 = 2;

/// Protobuf wire type 5: 32-bit fixed (`fixed32`, `sfixed32`, `float`).
pub const WIRE_TYPE_FIXED32: u8 = 5;

/// Encode an unsigned 64-bit integer as a base-128 Protobuf varint (1 to 10 bytes).
#[inline]
pub fn put_varint<B: BufMut>(buf: &mut B, mut val: u64) {
    while val >= 0x80 {
        buf.put_u8(((val as u8) & 0x7F) | 0x80);
        val >>= 7;
    }
    buf.put_u8(val as u8);
}

/// Encode a Protobuf field tag `(field_number << 3) | wire_type`.
#[inline]
pub fn put_tag<B: BufMut>(buf: &mut B, field_number: u32, wire_type: u8) {
    let tag = ((field_number as u64) << 3) | ((wire_type & 0x07) as u64);
    put_varint(buf, tag);
}

/// Encode a varint field (`wire_type = 0`).
#[inline]
pub fn put_varint_field<B: BufMut>(buf: &mut B, field_number: u32, val: u64) {
    put_tag(buf, field_number, WIRE_TYPE_VARINT);
    put_varint(buf, val);
}

/// Encode a length-delimited byte slice field (`wire_type = 2`).
#[inline]
pub fn put_bytes_field<B: BufMut>(buf: &mut B, field_number: u32, payload: &[u8]) {
    put_tag(buf, field_number, WIRE_TYPE_LENGTH_DELIMITED);
    put_varint(buf, payload.len() as u64);
    buf.put_slice(payload);
}

/// Decode a base-128 Protobuf varint from `data` starting at `*pos`, advancing `*pos`.
///
/// Rejects truncated sequences and values exceeding 64 bits (including 10th-byte values `> 1`).
#[inline]
pub fn read_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result = 0u64;
    let mut shift = 0u32;
    while *pos < data.len() {
        let byte = data[*pos];
        *pos += 1;
        if shift == 63 && byte > 1 {
            return Err("protobuf varint overflow".to_string());
        }
        result |= ((byte & 0x7F) as u64) << shift;
        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err("protobuf varint overflow".to_string());
        }
    }
    Err("unexpected EOF while reading protobuf varint".to_string())
}

/// Decode a Protobuf field tag into `(field_number, wire_type)`, rejecting field number `0`.
#[inline]
pub fn read_tag(data: &[u8], pos: &mut usize) -> Result<(u32, u8), String> {
    let raw = read_varint(data, pos)?;
    let field_number =
        u32::try_from(raw >> 3).map_err(|_| "protobuf field number overflow".to_string())?;
    if field_number == 0 {
        return Err("invalid protobuf field number 0".to_string());
    }
    let wire_type = (raw & 0x07) as u8;
    Ok((field_number, wire_type))
}

/// Decode a zero-copy length-delimited byte slice (`wire_type = 2`) from `data` at `*pos`.
#[inline]
pub fn read_length_delimited<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8], String> {
    let raw_len = read_varint(data, pos)?;
    let len = usize::try_from(raw_len)
        .map_err(|_| "protobuf length-delimited field length overflow".to_string())?;
    let end = pos
        .checked_add(len)
        .ok_or_else(|| "protobuf length-delimited field offset overflow".to_string())?;
    if end > data.len() {
        return Err("unexpected EOF reading length-delimited protobuf field".to_string());
    }
    let slice = &data[*pos..end];
    *pos = end;
    Ok(slice)
}

/// Skip a single Protobuf field payload of `wire_type` starting at `*pos`.
#[inline]
pub fn skip_field(data: &[u8], pos: &mut usize, wire_type: u8) -> Result<(), String> {
    match wire_type {
        WIRE_TYPE_VARINT => {
            let _ = read_varint(data, pos)?;
            Ok(())
        }
        WIRE_TYPE_FIXED64 => {
            let end = pos
                .checked_add(8)
                .ok_or_else(|| "protobuf 64-bit field offset overflow".to_string())?;
            if end > data.len() {
                return Err("unexpected EOF skipping 64-bit protobuf field".to_string());
            }
            *pos = end;
            Ok(())
        }
        WIRE_TYPE_LENGTH_DELIMITED => {
            let _ = read_length_delimited(data, pos)?;
            Ok(())
        }
        WIRE_TYPE_FIXED32 => {
            let end = pos
                .checked_add(4)
                .ok_or_else(|| "protobuf 32-bit field offset overflow".to_string())?;
            if end > data.len() {
                return Err("unexpected EOF skipping 32-bit protobuf field".to_string());
            }
            *pos = end;
            Ok(())
        }
        other => Err(format!("unsupported protobuf wire_type {other}")),
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn varint_roundtrip_across_1_to_10_bytes() {
        let cases: [(u64, usize); 11] = [
            (0, 1),
            (0x7F, 1),
            (0x80, 2),
            ((1u64 << 14) - 1, 2),
            (1u64 << 14, 3),
            (1u64 << 21, 4),
            (1u64 << 28, 5),
            (1u64 << 35, 6),
            (1u64 << 42, 7),
            (1u64 << 56, 9),
            (u64::MAX, 10),
        ];

        for (value, expected_len) in cases {
            let mut vec_buf = Vec::new();
            put_varint(&mut vec_buf, value);
            assert_eq!(
                vec_buf.len(),
                expected_len,
                "unexpected varint byte length for {value:#x}"
            );

            let mut bytes_buf = BytesMut::new();
            put_varint(&mut bytes_buf, value);
            assert_eq!(bytes_buf.as_ref(), vec_buf.as_slice());

            let mut pos = 0usize;
            let decoded = read_varint(&vec_buf, &mut pos).expect("decode valid varint");
            assert_eq!(decoded, value);
            assert_eq!(pos, expected_len);
        }
    }

    #[test]
    fn varint_rejects_truncated_and_overflow_inputs() {
        let mut pos = 0usize;
        let err_empty = read_varint(&[], &mut pos).expect_err("empty slice must fail");
        assert!(err_empty.contains("unexpected EOF"));

        let mut pos_trunc = 0usize;
        let err_trunc =
            read_varint(&[0x80, 0xFF, 0x81], &mut pos_trunc).expect_err("truncated varint");
        assert!(err_trunc.contains("unexpected EOF"));

        let mut tenth_byte_two = [0xFFu8; 10];
        tenth_byte_two[9] = 0x02;
        let mut pos_ovf10 = 0usize;
        let err_ovf10 =
            read_varint(&tenth_byte_two, &mut pos_ovf10).expect_err("2^64 must overflow");
        assert!(err_ovf10.contains("overflow"));

        let mut eleven_bytes = [0x80u8; 11];
        eleven_bytes[10] = 0x01;
        let mut pos_ovf11 = 0usize;
        let err_ovf11 =
            read_varint(&eleven_bytes, &mut pos_ovf11).expect_err("11-byte varint must overflow");
        assert!(err_ovf11.contains("overflow"));
    }

    #[test]
    fn tag_and_length_delimited_and_skip_field_roundtrip() {
        let mut buf = Vec::new();
        put_varint_field(&mut buf, 1, 42);
        put_bytes_field(&mut buf, 16, b"vertebra-proto");
        put_tag(&mut buf, 3, WIRE_TYPE_FIXED64);
        buf.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        put_tag(&mut buf, 4, WIRE_TYPE_FIXED32);
        buf.extend_from_slice(&0x1122_3344u32.to_le_bytes());
        put_bytes_field(&mut buf, 536_870_911, b"max-field");

        let mut pos = 0usize;
        let (f1, w1) = read_tag(&buf, &mut pos).expect("tag 1");
        assert_eq!((f1, w1), (1, WIRE_TYPE_VARINT));
        assert_eq!(read_varint(&buf, &mut pos).expect("field 1"), 42);

        let (f2, w2) = read_tag(&buf, &mut pos).expect("tag 16");
        assert_eq!((f2, w2), (16, WIRE_TYPE_LENGTH_DELIMITED));
        assert_eq!(
            read_length_delimited(&buf, &mut pos).expect("field 16"),
            b"vertebra-proto"
        );

        let (f3, w3) = read_tag(&buf, &mut pos).expect("tag 3");
        assert_eq!((f3, w3), (3, WIRE_TYPE_FIXED64));
        skip_field(&buf, &mut pos, w3).expect("skip fixed64");

        let (f4, w4) = read_tag(&buf, &mut pos).expect("tag 4");
        assert_eq!((f4, w4), (4, WIRE_TYPE_FIXED32));
        skip_field(&buf, &mut pos, w4).expect("skip fixed32");

        let (f5, w5) = read_tag(&buf, &mut pos).expect("tag max");
        assert_eq!((f5, w5), (536_870_911, WIRE_TYPE_LENGTH_DELIMITED));
        assert_eq!(
            read_length_delimited(&buf, &mut pos).expect("field max"),
            b"max-field"
        );
        assert_eq!(pos, buf.len());

        let mut zero_tag_pos = 0usize;
        assert!(read_tag(&[0x00], &mut zero_tag_pos).is_err());

        let mut trunc_ld_pos = 0usize;
        assert!(read_length_delimited(&[0x05, b'a', b'b'], &mut trunc_ld_pos).is_err());

        let mut bad_wire_pos = 0usize;
        assert!(skip_field(&[0x00], &mut bad_wire_pos, 3).is_err());
    }
}
