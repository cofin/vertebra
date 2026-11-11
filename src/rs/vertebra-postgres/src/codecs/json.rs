//! Strict RFC 8259 `JSON` (`OID 114`) and version-validated `JSONB` (`OID 3802`, `0x01`) codecs.

use bytes::{BufMut, BytesMut};
use vertebra_db::postgres::{encode_pg_jsonb, strip_pg_jsonb_header};

use crate::codecs::CodecContext;
use crate::errors::PostgresError;

/// PostgreSQL `JSON` type OID (`114`).
pub const JSON_OID: u32 = 114;
/// PostgreSQL `JSONB` type OID (`3802`).
pub const JSONB_OID: u32 = 3802;

/// Validate that `raw` is a complete UTF-8 RFC 8259 JSON document within `max_depth`.
///
/// Numbers are validated lexically so integers > `2^53` never lose precision through `f64`.
/// If `forbid_unicode_nul` is `true` (`JSONB`), `\u0000` escapes inside strings are rejected.
pub fn validate_json_bytes(raw: &[u8], max_depth: usize) -> Result<&str, PostgresError> {
    validate_json_bytes_inner(raw, max_depth, false)
}

fn validate_json_bytes_inner(
    raw: &[u8],
    max_depth: usize,
    forbid_unicode_nul: bool,
) -> Result<&str, PostgresError> {
    let text = std::str::from_utf8(raw)
        .map_err(|e| PostgresError::Data(format!("JSON payload is not valid UTF-8: {e}")))?;
    if text.as_bytes().contains(&0) {
        return Err(PostgresError::Data(
            "JSON payload contains embedded NUL byte".to_string(),
        ));
    }
    let bytes = text.as_bytes();
    let mut pos = skip_ws(bytes, 0);
    if pos >= bytes.len() {
        return Err(PostgresError::Data("JSON payload is empty".to_string()));
    }
    pos = parse_json_value(bytes, pos, 0, max_depth, forbid_unicode_nul)?;
    pos = skip_ws(bytes, pos);
    if pos != bytes.len() {
        return Err(PostgresError::Data(
            "JSON payload has trailing tokens after root value".to_string(),
        ));
    }
    Ok(text)
}

fn skip_ws(bytes: &[u8], mut pos: usize) -> usize {
    while pos < bytes.len() && matches!(bytes[pos], b' ' | b'\t' | b'\r' | b'\n') {
        pos += 1;
    }
    pos
}

fn parse_json_value(
    bytes: &[u8],
    pos: usize,
    depth: usize,
    max_depth: usize,
    forbid_unicode_nul: bool,
) -> Result<usize, PostgresError> {
    if pos >= bytes.len() {
        return Err(PostgresError::Data("unexpected EOF in JSON".to_string()));
    }
    match bytes[pos] {
        b'"' => parse_json_string(bytes, pos, forbid_unicode_nul),
        b'{' => {
            let next_depth = depth + 1;
            if next_depth > max_depth {
                return Err(PostgresError::Data(format!(
                    "JSON nesting depth {next_depth} exceeds max_nesting_depth {max_depth}"
                )));
            }
            let mut idx = skip_ws(bytes, pos + 1);
            if idx < bytes.len() && bytes[idx] == b'}' {
                return Ok(idx + 1);
            }
            loop {
                if idx >= bytes.len() || bytes[idx] != b'"' {
                    return Err(PostgresError::Data(
                        "expected string key in JSON object".to_string(),
                    ));
                }
                idx = parse_json_string(bytes, idx, forbid_unicode_nul)?;
                idx = skip_ws(bytes, idx);
                if idx >= bytes.len() || bytes[idx] != b':' {
                    return Err(PostgresError::Data(
                        "expected ':' after JSON object key".to_string(),
                    ));
                }
                idx = skip_ws(bytes, idx + 1);
                idx = parse_json_value(bytes, idx, next_depth, max_depth, forbid_unicode_nul)?;
                idx = skip_ws(bytes, idx);
                if idx >= bytes.len() {
                    return Err(PostgresError::Data("unterminated JSON object".to_string()));
                }
                match bytes[idx] {
                    b',' => {
                        idx = skip_ws(bytes, idx + 1);
                        if idx < bytes.len() && bytes[idx] == b'}' {
                            return Err(PostgresError::Data(
                                "trailing comma in JSON object".to_string(),
                            ));
                        }
                    }
                    b'}' => return Ok(idx + 1),
                    _ => {
                        return Err(PostgresError::Data(
                            "expected ',' or '}' in JSON object".to_string(),
                        ));
                    }
                }
            }
        }
        b'[' => {
            let next_depth = depth + 1;
            if next_depth > max_depth {
                return Err(PostgresError::Data(format!(
                    "JSON nesting depth {next_depth} exceeds max_nesting_depth {max_depth}"
                )));
            }
            let mut idx = skip_ws(bytes, pos + 1);
            if idx < bytes.len() && bytes[idx] == b']' {
                return Ok(idx + 1);
            }
            loop {
                idx = parse_json_value(bytes, idx, next_depth, max_depth, forbid_unicode_nul)?;
                idx = skip_ws(bytes, idx);
                if idx >= bytes.len() {
                    return Err(PostgresError::Data("unterminated JSON array".to_string()));
                }
                match bytes[idx] {
                    b',' => {
                        idx = skip_ws(bytes, idx + 1);
                        if idx < bytes.len() && bytes[idx] == b']' {
                            return Err(PostgresError::Data(
                                "trailing comma in JSON array".to_string(),
                            ));
                        }
                    }
                    b']' => return Ok(idx + 1),
                    _ => {
                        return Err(PostgresError::Data(
                            "expected ',' or ']' in JSON array".to_string(),
                        ));
                    }
                }
            }
        }
        b't' => {
            if bytes.get(pos..pos + 4) == Some(b"true") {
                Ok(pos + 4)
            } else {
                Err(PostgresError::Data("invalid JSON literal".to_string()))
            }
        }
        b'f' => {
            if bytes.get(pos..pos + 5) == Some(b"false") {
                Ok(pos + 5)
            } else {
                Err(PostgresError::Data("invalid JSON literal".to_string()))
            }
        }
        b'n' => {
            if bytes.get(pos..pos + 4) == Some(b"null") {
                Ok(pos + 4)
            } else {
                Err(PostgresError::Data("invalid JSON literal".to_string()))
            }
        }
        b'-' | b'0'..=b'9' => parse_json_number(bytes, pos),
        _ => Err(PostgresError::Data(
            "unexpected token in JSON payload".to_string(),
        )),
    }
}

fn parse_hex4(slice: &[u8]) -> Result<u16, PostgresError> {
    if slice.len() != 4 {
        return Err(PostgresError::Data(
            "truncated \\uXXXX escape in JSON string".to_string(),
        ));
    }
    let mut val = 0u16;
    for &b in slice {
        let d = match b {
            b'0'..=b'9' => u16::from(b - b'0'),
            b'a'..=b'f' => u16::from(b - b'a' + 10),
            b'A'..=b'F' => u16::from(b - b'A' + 10),
            _ => {
                return Err(PostgresError::Data(
                    "invalid hex digit in \\uXXXX JSON escape".to_string(),
                ));
            }
        };
        val = (val << 4) | d;
    }
    Ok(val)
}

fn parse_json_string(
    bytes: &[u8],
    pos: usize,
    forbid_unicode_nul: bool,
) -> Result<usize, PostgresError> {
    let mut idx = pos + 1;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b'"' {
            return Ok(idx + 1);
        }
        if b < 0x20 {
            return Err(PostgresError::Data(
                "unescaped control character in JSON string".to_string(),
            ));
        }
        if b == b'\\' {
            idx += 1;
            if idx >= bytes.len() {
                return Err(PostgresError::Data(
                    "unterminated escape in JSON string".to_string(),
                ));
            }
            match bytes[idx] {
                b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {
                    idx += 1;
                }
                b'u' => {
                    let code = parse_hex4(bytes.get(idx + 1..idx + 5).unwrap_or(&[]))?;
                    idx += 5;
                    if forbid_unicode_nul && code == 0 {
                        return Err(PostgresError::Data(
                            "PostgreSQL JSONB forbids \\u0000 inside strings".to_string(),
                        ));
                    }
                    if (0xD800..=0xDBFF).contains(&code) {
                        if bytes.get(idx..idx + 2) != Some(b"\\u") {
                            return Err(PostgresError::Data(
                                "unpaired high UTF-16 surrogate in JSON string".to_string(),
                            ));
                        }
                        let low = parse_hex4(bytes.get(idx + 2..idx + 6).unwrap_or(&[]))?;
                        if !(0xDC00..=0xDFFF).contains(&low) {
                            return Err(PostgresError::Data(
                                "invalid low UTF-16 surrogate in JSON string".to_string(),
                            ));
                        }
                        idx += 6;
                    } else if (0xDC00..=0xDFFF).contains(&code) {
                        return Err(PostgresError::Data(
                            "unpaired low UTF-16 surrogate in JSON string".to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(PostgresError::Data(
                        "invalid escape sequence in JSON string".to_string(),
                    ));
                }
            }
        } else {
            idx += 1;
        }
    }
    Err(PostgresError::Data("unterminated JSON string".to_string()))
}

fn parse_json_number(bytes: &[u8], mut idx: usize) -> Result<usize, PostgresError> {
    if bytes[idx] == b'-' {
        idx += 1;
        if idx >= bytes.len() {
            return Err(PostgresError::Data("incomplete JSON number".to_string()));
        }
    }
    match bytes[idx] {
        b'0' => {
            idx += 1;
            if idx < bytes.len() && bytes[idx].is_ascii_digit() {
                return Err(PostgresError::Data(
                    "JSON numbers cannot have leading zeros".to_string(),
                ));
            }
        }
        b'1'..=b'9' => {
            idx += 1;
            while idx < bytes.len() && bytes[idx].is_ascii_digit() {
                idx += 1;
            }
        }
        _ => {
            return Err(PostgresError::Data("invalid JSON number".to_string()));
        }
    }
    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        let frac_start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if idx == frac_start {
            return Err(PostgresError::Data(
                "JSON number missing digits after decimal point".to_string(),
            ));
        }
    }
    if idx < bytes.len() && (bytes[idx] == b'e' || bytes[idx] == b'E') {
        idx += 1;
        if idx < bytes.len() && (bytes[idx] == b'+' || bytes[idx] == b'-') {
            idx += 1;
        }
        let exp_start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if idx == exp_start {
            return Err(PostgresError::Data(
                "JSON number missing exponent digits".to_string(),
            ));
        }
    }
    Ok(idx)
}

/// Decode a PostgreSQL binary `JSON` (`OID 114`) payload.
pub fn decode_json_binary(raw: &[u8], ctx: &CodecContext) -> Result<Vec<u8>, PostgresError> {
    validate_json_bytes_inner(raw, ctx.max_nesting_depth, false)?;
    Ok(raw.to_vec())
}

/// Decode a PostgreSQL text `JSON` (`OID 114`) or `JSONB` (`OID 3802`) payload.
pub fn decode_json_text(raw: &[u8], ctx: &CodecContext) -> Result<Vec<u8>, PostgresError> {
    validate_json_bytes_inner(raw, ctx.max_nesting_depth, false)?;
    Ok(raw.to_vec())
}

/// Decode a PostgreSQL binary `JSONB` (`OID 3802`, `0x01` version prefix) payload.
pub fn decode_jsonb_binary(raw: &[u8], ctx: &CodecContext) -> Result<Vec<u8>, PostgresError> {
    let body = strip_pg_jsonb_header(raw)
        .map_err(|e| PostgresError::Data(format!("invalid JSONB binary header: {e}")))?;
    validate_json_bytes_inner(body, ctx.max_nesting_depth, true)?;
    Ok(body.to_vec())
}

/// Encode a validated JSON payload for PostgreSQL binary `JSON` (`OID 114`).
pub fn encode_json_binary(
    json: &[u8],
    out: &mut BytesMut,
    ctx: &CodecContext,
) -> Result<(), PostgresError> {
    validate_json_bytes_inner(json, ctx.max_nesting_depth, false)?;
    out.put_slice(json);
    Ok(())
}

/// Encode a validated JSON payload for PostgreSQL binary `JSONB` (`OID 3802`, prepending `0x01`).
pub fn encode_jsonb_binary(
    json: &[u8],
    out: &mut BytesMut,
    ctx: &CodecContext,
) -> Result<(), PostgresError> {
    validate_json_bytes_inner(json, ctx.max_nesting_depth, true)?;
    let encoded = encode_pg_jsonb(json);
    out.put_slice(&encoded);
    Ok(())
}

/// Encode a validated JSON payload for PostgreSQL text `JSON` (`114`) or `JSONB` (`3802`).
pub fn encode_json_text(
    json: &[u8],
    for_jsonb: bool,
    out: &mut BytesMut,
    ctx: &CodecContext,
) -> Result<(), PostgresError> {
    validate_json_bytes_inner(json, ctx.max_nesting_depth, for_jsonb)?;
    out.put_slice(json);
    Ok(())
}
