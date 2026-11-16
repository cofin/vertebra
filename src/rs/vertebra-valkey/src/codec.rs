//! Pure-Rust RESP3 (+ RESP2 fallback) codec, handshake commands, and Valkey 9.0 hash-field TTL builders.

use std::borrow::Cow;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyList, PySet};

use crate::error::ValkeyError;

const MAX_RESP_DEPTH: usize = 128;

/// Decoded RESP2 / RESP3 protocol frame.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(Bytes),
    SimpleError(String),
    Integer(i64),
    Double(f64),
    Boolean(bool),
    Null,
    BigNumber(String),
    BulkString(Bytes),
    BulkError(String),
    VerbatimString {
        format: [u8; 3],
        data: Bytes,
    },
    Array(Vec<RespValue>),
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
    Push(Vec<RespValue>),
    Attribute {
        attrs: Vec<(RespValue, RespValue)>,
        inner: Box<RespValue>,
    },
}

impl RespValue {
    /// Strip outer `Attribute` envelopes to inspect the inner value.
    #[must_use]
    pub fn unattributed(&self) -> &Self {
        let mut cur = self;
        while let Self::Attribute { inner, .. } = cur {
            cur = inner.as_ref();
        }
        cur
    }

    /// Consume outer `Attribute` envelopes and return the inner value.
    #[must_use]
    pub fn into_unattributed(self) -> Self {
        let mut cur = self;
        while let Self::Attribute { inner, .. } = cur {
            cur = *inner;
        }
        cur
    }

    /// Extract error if this value is a `SimpleError` or `BulkError`.
    pub fn into_result(self) -> Result<Self, ValkeyError> {
        let inner = self.into_unattributed();
        match inner {
            Self::SimpleError(msg) | Self::BulkError(msg) => {
                Err(ValkeyError::from_server_error(msg))
            }
            other => Ok(other),
        }
    }

    /// Return byte slice view if value is a string/bulk/verbatim/simple error/big number.
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self.unattributed() {
            Self::SimpleString(b) | Self::BulkString(b) => Some(b.as_ref()),
            Self::VerbatimString { data, .. } => Some(data.as_ref()),
            Self::SimpleError(s) | Self::BulkError(s) | Self::BigNumber(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// Return UTF-8 lossy string view if value represents string-like data.
    #[must_use]
    pub fn as_str(&self) -> Option<Cow<'_, str>> {
        self.as_bytes().map(String::from_utf8_lossy)
    }

    /// Return integer if value is `Integer` or an ASCII integer string.
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self.unattributed() {
            Self::Integer(n) => Some(*n),
            Self::SimpleString(b) | Self::BulkString(b) => {
                std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
            }
            _ => None,
        }
    }

    /// Return floating-point number if value is `Double`, `Integer`, or ASCII float string.
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self.unattributed() {
            Self::Double(f) => Some(*f),
            Self::Integer(n) => Some(*n as f64),
            Self::SimpleString(b) | Self::BulkString(b) => {
                std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
            }
            _ => None,
        }
    }

    /// Return slice if value is an `Array`, `Set`, or `Push`.
    #[must_use]
    pub fn as_slice(&self) -> Option<&[RespValue]> {
        match self.unattributed() {
            Self::Array(items) | Self::Set(items) | Self::Push(items) => Some(items.as_slice()),
            _ => None,
        }
    }

    /// Return bool interpretation for RESP3 booleans, `OK`, or integer `1`/`0`.
    #[must_use]
    pub fn as_bool_ok(&self) -> Option<bool> {
        match self.unattributed() {
            Self::Boolean(b) => Some(*b),
            Self::Null => Some(false),
            Self::Integer(n) => Some(*n != 0),
            Self::SimpleString(b) | Self::BulkString(b) => {
                if b.eq_ignore_ascii_case(b"OK") || b.as_ref() == b"1" {
                    Some(true)
                } else if b.as_ref() == b"0" {
                    Some(false)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Encode a RESP array command from borrowed byte slices into `dst`.
pub fn encode_command(dst: &mut BytesMut, args: &[&[u8]]) {
    let header = format!("*{}\r\n", args.len());
    dst.put_slice(header.as_bytes());
    for arg in args {
        let prefix = format!("${}\r\n", arg.len());
        dst.put_slice(prefix.as_bytes());
        dst.put_slice(arg);
        dst.put_slice(b"\r\n");
    }
}

/// Encode a RESP array command from owned byte vectors into `dst`.
pub fn encode_owned_command(dst: &mut BytesMut, args: &[Vec<u8>]) {
    let header = format!("*{}\r\n", args.len());
    dst.put_slice(header.as_bytes());
    for arg in args {
        let prefix = format!("${}\r\n", arg.len());
        dst.put_slice(prefix.as_bytes());
        dst.put_slice(arg);
        dst.put_slice(b"\r\n");
    }
}

/// Attempt to decode a single complete RESP2/RESP3 frame from `buf`.
///
/// Leaves `buf` untouched and returns `Ok(None)` when additional bytes are needed.
pub fn try_decode_frame(buf: &mut BytesMut) -> Result<Option<RespValue>, ValkeyError> {
    if buf.is_empty() {
        return Ok(None);
    }
    match parse_at(buf.as_ref(), 0, 0)? {
        Some((val, consumed)) => {
            buf.advance(consumed);
            Ok(Some(val))
        }
        None => Ok(None),
    }
}

fn find_crlf(slice: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i + 1 < slice.len() {
        if slice[i] == b'\r' && slice[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn parse_at(
    data: &[u8],
    offset: usize,
    depth: usize,
) -> Result<Option<(RespValue, usize)>, ValkeyError> {
    if depth > MAX_RESP_DEPTH {
        return Err(ValkeyError::operational(
            "RESP3 maximum nesting depth exceeded",
            false,
        ));
    }
    if offset >= data.len() {
        return Ok(None);
    }
    let tag = data[offset];
    let Some(line_end) = find_crlf(data, offset + 1) else {
        return Ok(None);
    };
    let line = &data[offset + 1..line_end];
    let after_line = line_end + 2;

    match tag {
        b'+' => {
            let val = Bytes::copy_from_slice(line);
            Ok(Some((RespValue::SimpleString(val), after_line)))
        }
        b'-' => {
            let msg = String::from_utf8_lossy(line).into_owned();
            Ok(Some((RespValue::SimpleError(msg), after_line)))
        }
        b':' => {
            let n = parse_i64(line, "RESP3 integer")?;
            Ok(Some((RespValue::Integer(n), after_line)))
        }
        b',' => {
            let s = std::str::from_utf8(line)
                .map_err(|_| ValkeyError::operational("Invalid UTF-8 in RESP3 double", false))?;
            let f = match s {
                "inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                "nan" => f64::NAN,
                other => other.parse::<f64>().map_err(|_| {
                    ValkeyError::operational(format!("Invalid RESP3 double '{other}'"), false)
                })?,
            };
            Ok(Some((RespValue::Double(f), after_line)))
        }
        b'#' => {
            let b = match line {
                b"t" => true,
                b"f" => false,
                _ => {
                    return Err(ValkeyError::operational(
                        "Invalid RESP3 boolean token",
                        false,
                    ));
                }
            };
            Ok(Some((RespValue::Boolean(b), after_line)))
        }
        b'_' => {
            if !line.is_empty() {
                return Err(ValkeyError::operational("Invalid RESP3 null line", false));
            }
            Ok(Some((RespValue::Null, after_line)))
        }
        b'(' => {
            let s = String::from_utf8_lossy(line).into_owned();
            Ok(Some((RespValue::BigNumber(s), after_line)))
        }
        b'$' => {
            let len = parse_i64(line, "RESP3 bulk length")?;
            if len < 0 {
                return Ok(Some((RespValue::Null, after_line)));
            }
            let len_usize = usize::try_from(len)
                .map_err(|_| ValkeyError::operational("RESP3 bulk length overflow", false))?;
            let end_payload = after_line
                .checked_add(len_usize)
                .and_then(|v| v.checked_add(2))
                .ok_or_else(|| ValkeyError::operational("RESP3 bulk offset overflow", false))?;
            if end_payload > data.len() {
                return Ok(None);
            }
            if &data[after_line + len_usize..end_payload] != b"\r\n" {
                return Err(ValkeyError::operational(
                    "Missing CRLF terminator after RESP3 bulk string",
                    false,
                ));
            }
            let payload = Bytes::copy_from_slice(&data[after_line..after_line + len_usize]);
            Ok(Some((RespValue::BulkString(payload), end_payload)))
        }
        b'!' => {
            let len = parse_i64(line, "RESP3 blob error length")?;
            if len < 0 {
                return Ok(Some((RespValue::Null, after_line)));
            }
            let len_usize = usize::try_from(len)
                .map_err(|_| ValkeyError::operational("RESP3 blob error overflow", false))?;
            let end_payload = after_line
                .checked_add(len_usize)
                .and_then(|v| v.checked_add(2))
                .ok_or_else(|| ValkeyError::operational("RESP3 blob offset overflow", false))?;
            if end_payload > data.len() {
                return Ok(None);
            }
            if &data[after_line + len_usize..end_payload] != b"\r\n" {
                return Err(ValkeyError::operational(
                    "Missing CRLF terminator after RESP3 blob error",
                    false,
                ));
            }
            let msg =
                String::from_utf8_lossy(&data[after_line..after_line + len_usize]).into_owned();
            Ok(Some((RespValue::BulkError(msg), end_payload)))
        }
        b'=' => {
            let len = parse_i64(line, "RESP3 verbatim string length")?;
            if len < 4 {
                return Err(ValkeyError::operational(
                    "RESP3 verbatim string must be at least 4 bytes",
                    false,
                ));
            }
            let len_usize = usize::try_from(len)
                .map_err(|_| ValkeyError::operational("RESP3 verbatim overflow", false))?;
            let end_payload = after_line
                .checked_add(len_usize)
                .and_then(|v| v.checked_add(2))
                .ok_or_else(|| ValkeyError::operational("RESP3 verbatim offset overflow", false))?;
            if end_payload > data.len() {
                return Ok(None);
            }
            let raw = &data[after_line..after_line + len_usize];
            if raw[3] != b':' {
                return Err(ValkeyError::operational(
                    "Invalid RESP3 verbatim string format prefix",
                    false,
                ));
            }
            if &data[after_line + len_usize..end_payload] != b"\r\n" {
                return Err(ValkeyError::operational(
                    "Missing CRLF terminator after RESP3 verbatim string",
                    false,
                ));
            }
            let format = [raw[0], raw[1], raw[2]];
            let payload = Bytes::copy_from_slice(&raw[4..]);
            Ok(Some((
                RespValue::VerbatimString {
                    format,
                    data: payload,
                },
                end_payload,
            )))
        }
        b'*' | b'~' | b'>' => {
            let count = parse_i64(line, "RESP3 aggregate count")?;
            if count < 0 {
                return Ok(Some((RespValue::Null, after_line)));
            }
            let n = usize::try_from(count)
                .map_err(|_| ValkeyError::operational("RESP3 aggregate count overflow", false))?;
            let mut items = Vec::with_capacity(n.min(1024));
            let mut cur = after_line;
            for _ in 0..n {
                let Some((elem, next_cur)) = parse_at(data, cur, depth + 1)? else {
                    return Ok(None);
                };
                items.push(elem);
                cur = next_cur;
            }
            let val = match tag {
                b'*' => RespValue::Array(items),
                b'~' => RespValue::Set(items),
                _ => RespValue::Push(items),
            };
            Ok(Some((val, cur)))
        }
        b'%' => {
            let count = parse_i64(line, "RESP3 map pair count")?;
            if count < 0 {
                return Ok(Some((RespValue::Null, after_line)));
            }
            let n = usize::try_from(count)
                .map_err(|_| ValkeyError::operational("RESP3 map count overflow", false))?;
            let mut pairs = Vec::with_capacity(n.min(1024));
            let mut cur = after_line;
            for _ in 0..n {
                let Some((k, after_k)) = parse_at(data, cur, depth + 1)? else {
                    return Ok(None);
                };
                let Some((v, after_v)) = parse_at(data, after_k, depth + 1)? else {
                    return Ok(None);
                };
                pairs.push((k, v));
                cur = after_v;
            }
            Ok(Some((RespValue::Map(pairs), cur)))
        }
        b'|' => {
            let count = parse_i64(line, "RESP3 attribute pair count")?;
            if count < 0 {
                return Err(ValkeyError::operational(
                    "RESP3 attribute length cannot be negative",
                    false,
                ));
            }
            let n = usize::try_from(count)
                .map_err(|_| ValkeyError::operational("RESP3 attribute count overflow", false))?;
            let mut attrs = Vec::with_capacity(n.min(64));
            let mut cur = after_line;
            for _ in 0..n {
                let Some((k, after_k)) = parse_at(data, cur, depth + 1)? else {
                    return Ok(None);
                };
                let Some((v, after_v)) = parse_at(data, after_k, depth + 1)? else {
                    return Ok(None);
                };
                attrs.push((k, v));
                cur = after_v;
            }
            let Some((inner, final_cur)) = parse_at(data, cur, depth + 1)? else {
                return Ok(None);
            };
            Ok(Some((
                RespValue::Attribute {
                    attrs,
                    inner: Box::new(inner),
                },
                final_cur,
            )))
        }
        other => Err(ValkeyError::operational(
            format!("Unknown RESP3 frame prefix byte 0x{other:02x}"),
            false,
        )),
    }
}

fn parse_i64(bytes: &[u8], label: &str) -> Result<i64, ValkeyError> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| ValkeyError::operational(format!("Invalid UTF-8 in {label}"), false))?;
    s.parse::<i64>()
        .map_err(|_| ValkeyError::operational(format!("Invalid {label}: '{s}'"), false))
}

/// Convert a [`RespValue`] into a generic Python object for `eval`, `evalsha`, and `execute_command`.
pub fn resp_to_py(py: Python<'_>, val: RespValue) -> PyResult<Py<PyAny>> {
    match val.into_unattributed() {
        RespValue::Null => Ok(py.None()),
        RespValue::Boolean(b) => Ok(PyBool::new(py, b).to_owned().into_any().unbind()),
        RespValue::Integer(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
        RespValue::Double(f) => Ok(PyFloat::new(py, f).into_any().unbind()),
        RespValue::BigNumber(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        RespValue::SimpleString(b) => {
            if let Ok(s) = std::str::from_utf8(&b) {
                Ok(pyo3::types::PyString::new(py, s).into_any().unbind())
            } else {
                Ok(PyBytes::new(py, &b).into_any().unbind())
            }
        }
        RespValue::BulkString(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        RespValue::VerbatimString { data, .. } => Ok(PyBytes::new(py, &data).into_any().unbind()),
        RespValue::Array(items) | RespValue::Push(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(resp_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        RespValue::Set(items) => {
            let set = PySet::empty(py)?;
            let mut iter = items.into_iter();
            while let Some(item) = iter.next() {
                let obj = resp_to_py(py, item)?;
                if let Err(err) = set.add(&obj) {
                    if !err.is_instance_of::<pyo3::exceptions::PyTypeError>(py) {
                        return Err(err);
                    }
                    let list = PyList::empty(py);
                    for existing in set.iter() {
                        list.append(existing)?;
                    }
                    list.append(obj)?;
                    for rest in iter {
                        list.append(resp_to_py(py, rest)?)?;
                    }
                    return Ok(list.into_any().unbind());
                }
            }
            Ok(set.into_any().unbind())
        }
        RespValue::Map(pairs) => {
            let dict = PyDict::new(py);
            for (k, v) in pairs {
                let pk = resp_key_to_py(py, k)?;
                let pv = resp_to_py(py, v)?;
                dict.set_item(pk, pv)?;
            }
            Ok(dict.into_any().unbind())
        }
        RespValue::SimpleError(msg) | RespValue::BulkError(msg) => {
            Err(ValkeyError::from_server_error(msg).to_py_err(py))
        }
        RespValue::Attribute { .. } => Ok(py.None()),
    }
}

fn resp_key_to_py(py: Python<'_>, val: RespValue) -> PyResult<Py<PyAny>> {
    match val.into_unattributed() {
        RespValue::SimpleString(b) | RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            Ok(s.into_pyobject(py)?.into_any().unbind())
        }
        other => resp_to_py(py, other),
    }
}

/// Build `HELLO 3 [AUTH <username_or_default> <password>] [SETNAME <name>]`.
#[must_use]
pub fn build_hello_3(
    username: Option<&str>,
    password: Option<&str>,
    client_name: Option<&str>,
) -> Vec<Vec<u8>> {
    let mut cmd = vec![b"HELLO".to_vec(), b"3".to_vec()];
    if let Some(pass) = password {
        cmd.push(b"AUTH".to_vec());
        cmd.push(username.unwrap_or("default").as_bytes().to_vec());
        cmd.push(pass.as_bytes().to_vec());
    }
    if let Some(name) = client_name
        && !name.is_empty()
    {
        cmd.push(b"SETNAME".to_vec());
        cmd.push(name.as_bytes().to_vec());
    }
    cmd
}

/// Build RESP2 fallback `AUTH [username] <password>`.
#[must_use]
pub fn build_auth(username: Option<&str>, password: &str) -> Vec<Vec<u8>> {
    let mut cmd = vec![b"AUTH".to_vec()];
    if let Some(u) = username
        && !u.is_empty()
    {
        cmd.push(u.as_bytes().to_vec());
    }
    cmd.push(password.as_bytes().to_vec());
    cmd
}

/// Build Valkey `CLIENT CAPA redirect`.
#[must_use]
pub fn build_client_capa_redirect() -> Vec<Vec<u8>> {
    vec![b"CLIENT".to_vec(), b"CAPA".to_vec(), b"redirect".to_vec()]
}

/// Validate Valkey 9.0 hash-field expiration condition (`None`, `"NX"`, `"XX"`, `"GT"`, `"LT"`).
pub fn validate_hash_condition(
    condition: Option<&str>,
) -> Result<Option<&'static str>, ValkeyError> {
    let Some(raw) = condition else {
        return Ok(None);
    };
    match raw.trim().to_ascii_uppercase().as_str() {
        "NX" => Ok(Some("NX")),
        "XX" => Ok(Some("XX")),
        "GT" => Ok(Some("GT")),
        "LT" => Ok(Some("LT")),
        other => Err(ValkeyError::programming(format!(
            "Invalid Valkey hash-field expiration condition '{other}'; expected one of NX, XX, GT, LT"
        ))),
    }
}

/// Build a Valkey 9.0 `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, or `HPEXPIREAT` command.
pub fn build_hash_expire_cmd(
    cmd_name: &str,
    key: &[u8],
    ttl_value: i64,
    condition: Option<&str>,
    fields: &[Vec<u8>],
) -> Result<Vec<Vec<u8>>, ValkeyError> {
    if fields.is_empty() {
        return Err(ValkeyError::programming(format!(
            "{cmd_name} requires at least one field"
        )));
    }
    let validated_cond = validate_hash_condition(condition)?;
    let mut cmd = Vec::with_capacity(6 + fields.len());
    cmd.push(cmd_name.as_bytes().to_vec());
    cmd.push(key.to_vec());
    cmd.push(ttl_value.to_string().into_bytes());
    if let Some(cond) = validated_cond {
        cmd.push(cond.as_bytes().to_vec());
    }
    cmd.push(b"FIELDS".to_vec());
    cmd.push(fields.len().to_string().into_bytes());
    for field in fields {
        cmd.push(field.clone());
    }
    Ok(cmd)
}

/// Build a Valkey 9.0 `HTTL`, `HPTTL`, or `HPERSIST` command.
pub fn build_hash_field_query_cmd(
    cmd_name: &str,
    key: &[u8],
    fields: &[Vec<u8>],
) -> Result<Vec<Vec<u8>>, ValkeyError> {
    if fields.is_empty() {
        return Err(ValkeyError::programming(format!(
            "{cmd_name} requires at least one field"
        )));
    }
    let mut cmd = Vec::with_capacity(4 + fields.len());
    cmd.push(cmd_name.as_bytes().to_vec());
    cmd.push(key.to_vec());
    cmd.push(b"FIELDS".to_vec());
    cmd.push(fields.len().to_string().into_bytes());
    for field in fields {
        cmd.push(field.clone());
    }
    Ok(cmd)
}

/// Decode an array of integers (returned by `HEXPIRE`/`HTTL`/`HPERSIST`).
pub fn decode_int_list(resp: RespValue) -> Result<Vec<i64>, ValkeyError> {
    let val = resp.into_result()?;
    match val {
        RespValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let checked = item.into_result()?;
                let n = checked.as_i64().ok_or_else(|| {
                    ValkeyError::data("Expected integer status element in Valkey array response")
                })?;
                out.push(n);
            }
            Ok(out)
        }
        other => Err(ValkeyError::data(format!(
            "Expected RESP array of integers, got {other:?}"
        ))),
    }
}

/// Compute lowercase 40-hex-char SHA-1 digest (RFC 3174) for Valkey `EVALSHA`.
#[must_use]
pub fn sha1_hex(data: &[u8]) -> String {
    let mut h0: u32 = 0x6745_2301;
    let mut h1: u32 = 0xEFCD_AB89;
    let mut h2: u32 = 0x98BA_DCFE;
    let mut h3: u32 = 0x1032_5476;
    let mut h4: u32 = 0xC3D2_E1F0;

    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = data.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 80];
        for (i, &word) in chunk.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(word);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1u32),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    format!("{h0:08x}{h1:08x}{h2:08x}{h3:08x}{h4:08x}")
}
