//! Central PostgreSQL wire codec dispatcher (`CodecContext`, `format_for_oid`, `decode_value`, `encode_value`).

use std::collections::{BTreeMap, HashMap};

use bytes::{BufMut, BytesMut};
use postgres_protocol::types;

use crate::errors::PostgresError;
use crate::result::{Format, PgValue, TemporalKind};

pub mod datetime_text;
pub mod interval_text;
pub mod json;
pub mod numeric;
pub mod temporal;

/// PostgreSQL `BOOL` type OID (`16`).
pub const BOOL_OID: u32 = 16;
/// PostgreSQL `BYTEA` type OID (`17`).
pub const BYTEA_OID: u32 = 17;
/// PostgreSQL `NAME` type OID (`19`).
pub const NAME_OID: u32 = 19;
/// PostgreSQL `INT8` (`BIGINT`) type OID (`20`).
pub const INT8_OID: u32 = 20;
/// PostgreSQL `INT2` (`SMALLINT`) type OID (`21`).
pub const INT2_OID: u32 = 21;
/// PostgreSQL `INT4` (`INTEGER`) type OID (`23`).
pub const INT4_OID: u32 = 23;
/// PostgreSQL `TEXT` type OID (`25`).
pub const TEXT_OID: u32 = 25;
/// PostgreSQL `OID` type OID (`26`).
pub const OID_OID: u32 = 26;
/// PostgreSQL `JSON` type OID (`114`).
pub const JSON_OID: u32 = json::JSON_OID;
/// PostgreSQL `FLOAT4` (`REAL`) type OID (`700`).
pub const FLOAT4_OID: u32 = 700;
/// PostgreSQL `FLOAT8` (`DOUBLE PRECISION`) type OID (`701`).
pub const FLOAT8_OID: u32 = 701;
/// PostgreSQL `BPCHAR` (`CHAR(n)`) type OID (`1042`).
pub const BPCHAR_OID: u32 = 1042;
/// PostgreSQL `VARCHAR` type OID (`1043`).
pub const VARCHAR_OID: u32 = 1043;
/// PostgreSQL `DATE` type OID (`1082`).
pub const DATE_OID: u32 = temporal::DATE_OID;
/// PostgreSQL `TIME` type OID (`1083`).
pub const TIME_OID: u32 = temporal::TIME_OID;
/// PostgreSQL `TIMESTAMP` type OID (`1114`).
pub const TIMESTAMP_OID: u32 = temporal::TIMESTAMP_OID;
/// PostgreSQL `TIMESTAMPTZ` type OID (`1184`).
pub const TIMESTAMPTZ_OID: u32 = temporal::TIMESTAMPTZ_OID;
/// PostgreSQL `INTERVAL` type OID (`1186`).
pub const INTERVAL_OID: u32 = temporal::INTERVAL_OID;
/// PostgreSQL `NUMERIC` type OID (`1700`).
pub const NUMERIC_OID: u32 = 1700;
/// PostgreSQL `UUID` type OID (`2950`).
pub const UUID_OID: u32 = 2950;
/// PostgreSQL `JSONB` type OID (`3802`).
pub const JSONB_OID: u32 = json::JSONB_OID;

/// Default per-field wire payload ceiling (`16 MiB`).
pub const DEFAULT_MAX_FIELD_BYTES: usize = 16 * 1024 * 1024;
/// Default container/JSON recursion ceiling (`32`).
pub const DEFAULT_MAX_NESTING_DEPTH: usize = 32;

/// Trait abstracting session parameter lookups across `HashMap` and `BTreeMap`.
pub trait ParameterMap {
    /// Lookup a parameter by exact or case-insensitive key.
    fn lookup_param(&self, key: &str) -> Option<&str>;
}

impl ParameterMap for HashMap<String, String> {
    fn lookup_param(&self, key: &str) -> Option<&str> {
        self.get(key).map(String::as_str).or_else(|| {
            self.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(key))
                .map(|(_, v)| v.as_str())
        })
    }
}

impl ParameterMap for BTreeMap<String, String> {
    fn lookup_param(&self, key: &str) -> Option<&str> {
        self.get(key).map(String::as_str).or_else(|| {
            self.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(key))
                .map(|(_, v)| v.as_str())
        })
    }
}

/// Session-aware codec execution context derived from `ParameterStatus`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodecContext {
    /// Captured `DateStyle` setting (default `"ISO, MDY"`).
    pub date_style: String,
    /// Captured `IntervalStyle` setting (default `"postgres"`).
    pub interval_style: String,
    /// Captured `TimeZone` setting (default `"UTC"`).
    pub time_zone: String,
    /// Maximum allowed byte length for a single field (`16 MiB` default).
    pub max_field_bytes: usize,
    /// Maximum allowed recursion depth for JSON/array/composite containers (`32` default).
    pub max_nesting_depth: usize,
}

impl Default for CodecContext {
    fn default() -> Self {
        Self {
            date_style: "ISO, MDY".to_string(),
            interval_style: "postgres".to_string(),
            time_zone: "UTC".to_string(),
            max_field_bytes: DEFAULT_MAX_FIELD_BYTES,
            max_nesting_depth: DEFAULT_MAX_NESTING_DEPTH,
        }
    }
}

impl CodecContext {
    /// Construct a [`CodecContext`] from a session parameter map (`ParameterStatus`).
    pub fn from_parameters<M: ParameterMap + ?Sized>(params: &M) -> Self {
        Self {
            date_style: params
                .lookup_param("DateStyle")
                .unwrap_or("ISO, MDY")
                .to_string(),
            interval_style: params
                .lookup_param("IntervalStyle")
                .unwrap_or("postgres")
                .to_string(),
            time_zone: params.lookup_param("TimeZone").unwrap_or("UTC").to_string(),
            max_field_bytes: DEFAULT_MAX_FIELD_BYTES,
            max_nesting_depth: DEFAULT_MAX_NESTING_DEPTH,
        }
    }
}

/// Return the preferred wire [`Format`] for a PostgreSQL type `oid` (`Format::Text` for unknown OIDs).
pub const fn format_for_oid(oid: u32) -> Format {
    match oid {
        BOOL_OID | BYTEA_OID | NAME_OID | INT8_OID | INT2_OID | INT4_OID | TEXT_OID | OID_OID
        | JSON_OID | FLOAT4_OID | FLOAT8_OID | BPCHAR_OID | VARCHAR_OID | DATE_OID | TIME_OID
        | TIMESTAMP_OID | TIMESTAMPTZ_OID | INTERVAL_OID | NUMERIC_OID | UUID_OID | JSONB_OID => {
            Format::Binary
        }
        _ => Format::Text,
    }
}

fn validate_no_nul_utf8(raw: &[u8]) -> Result<String, PostgresError> {
    if raw.contains(&0) {
        return Err(PostgresError::Data(
            "PostgreSQL text payload cannot contain embedded NUL bytes".to_string(),
        ));
    }
    let s = std::str::from_utf8(raw)
        .map_err(|e| PostgresError::Data(format!("invalid UTF-8 in PostgreSQL text field: {e}")))?;
    Ok(s.to_owned())
}

fn decode_bool_text(s: &str) -> Result<bool, PostgresError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "on" | "1" => Ok(true),
        "f" | "false" | "n" | "no" | "off" | "0" => Ok(false),
        _ => Err(PostgresError::Data(format!(
            "invalid PostgreSQL bool text {s:?}"
        ))),
    }
}

fn decode_float4_text(s: &str) -> Result<f32, PostgresError> {
    let trimmed = s.trim();
    if trimmed.eq_ignore_ascii_case("nan") {
        return Ok(f32::NAN);
    }
    if trimmed.eq_ignore_ascii_case("infinity")
        || trimmed.eq_ignore_ascii_case("+infinity")
        || trimmed.eq_ignore_ascii_case("inf")
        || trimmed.eq_ignore_ascii_case("+inf")
    {
        return Ok(f32::INFINITY);
    }
    if trimmed.eq_ignore_ascii_case("-infinity") || trimmed.eq_ignore_ascii_case("-inf") {
        return Ok(f32::NEG_INFINITY);
    }
    let parsed: f32 = trimmed
        .parse()
        .map_err(|_| PostgresError::Data(format!("invalid float4 text {s:?}")))?;
    if !parsed.is_finite() {
        return Err(PostgresError::Data(format!(
            "float4 text {s:?} overflowed finite IEEE-754 binary32"
        )));
    }
    Ok(parsed)
}

fn decode_float8_text(s: &str) -> Result<f64, PostgresError> {
    let trimmed = s.trim();
    if trimmed.eq_ignore_ascii_case("nan") {
        return Ok(f64::NAN);
    }
    if trimmed.eq_ignore_ascii_case("infinity")
        || trimmed.eq_ignore_ascii_case("+infinity")
        || trimmed.eq_ignore_ascii_case("inf")
        || trimmed.eq_ignore_ascii_case("+inf")
    {
        return Ok(f64::INFINITY);
    }
    if trimmed.eq_ignore_ascii_case("-infinity") || trimmed.eq_ignore_ascii_case("-inf") {
        return Ok(f64::NEG_INFINITY);
    }
    let parsed: f64 = trimmed
        .parse()
        .map_err(|_| PostgresError::Data(format!("invalid float8 text {s:?}")))?;
    if !parsed.is_finite() {
        return Err(PostgresError::Data(format!(
            "float8 text {s:?} overflowed finite IEEE-754 binary64"
        )));
    }
    Ok(parsed)
}

fn hex_nibble(b: u8) -> Result<u8, PostgresError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(PostgresError::Data(format!(
            "invalid hex digit 0x{b:02x} in bytea/uuid text"
        ))),
    }
}

fn decode_bytea_text(raw: &[u8]) -> Result<Vec<u8>, PostgresError> {
    if let Some(hex_body) = raw.strip_prefix(b"\\x") {
        let mut out = Vec::with_capacity(hex_body.len() / 2);
        let mut iter = hex_body
            .iter()
            .copied()
            .filter(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'));
        while let Some(hi) = iter.next() {
            let lo = iter.next().ok_or_else(|| {
                PostgresError::Data("odd number of hex digits in bytea \\x payload".to_string())
            })?;
            out.push((hex_nibble(hi)? << 4) | hex_nibble(lo)?);
        }
        return Ok(out);
    }

    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'\\' {
            if i + 1 < raw.len() && raw[i + 1] == b'\\' {
                out.push(b'\\');
                i += 2;
            } else if i + 3 < raw.len() {
                let d0 = raw[i + 1];
                let d1 = raw[i + 2];
                let d2 = raw[i + 3];
                if !(b'0'..=b'3').contains(&d0)
                    || !(b'0'..=b'7').contains(&d1)
                    || !(b'0'..=b'7').contains(&d2)
                {
                    return Err(PostgresError::Data(
                        "invalid octal escape in bytea text".to_string(),
                    ));
                }
                let val = ((d0 - b'0') << 6) | ((d1 - b'0') << 3) | (d2 - b'0');
                out.push(val);
                i += 4;
            } else {
                return Err(PostgresError::Data(
                    "truncated escape sequence in bytea text".to_string(),
                ));
            }
        } else {
            out.push(raw[i]);
            i += 1;
        }
    }
    Ok(out)
}

fn encode_bytea_hex(bytes: &[u8], out: &mut BytesMut) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.reserve(2 + bytes.len() * 2);
    out.put_slice(b"\\x");
    for &b in bytes {
        out.put_u8(HEX[(b >> 4) as usize]);
        out.put_u8(HEX[(b & 0x0f) as usize]);
    }
}

fn decode_uuid_text(s: &str) -> Result<[u8; 16], PostgresError> {
    let trimmed = s.trim();
    let hex_bytes: Vec<u8> = if trimmed.len() == 36 {
        let bytes = trimmed.as_bytes();
        if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
            return Err(PostgresError::Data(format!(
                "invalid hyphen positions in UUID {s:?}"
            )));
        }
        bytes
            .iter()
            .enumerate()
            .filter_map(|(i, &b)| (!matches!(i, 8 | 13 | 18 | 23)).then_some(b))
            .collect()
    } else if trimmed.len() == 32 {
        trimmed.as_bytes().to_vec()
    } else {
        return Err(PostgresError::Data(format!("invalid UUID text {s:?}")));
    };

    let mut out = [0u8; 16];
    for (idx, &pair) in hex_bytes.as_chunks::<2>().0.iter().enumerate() {
        out[idx] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(out)
}

fn format_uuid_hyphenated(uuid: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        uuid[0],
        uuid[1],
        uuid[2],
        uuid[3],
        uuid[4],
        uuid[5],
        uuid[6],
        uuid[7],
        uuid[8],
        uuid[9],
        uuid[10],
        uuid[11],
        uuid[12],
        uuid[13],
        uuid[14],
        uuid[15]
    )
}

/// Decode a single PostgreSQL cell payload into an owned [`PgValue`].
pub fn decode_value(
    oid: u32,
    format: Format,
    raw: Option<&[u8]>,
    ctx: &CodecContext,
) -> Result<PgValue, PostgresError> {
    let Some(bytes) = raw else {
        return Ok(PgValue::Null);
    };
    if bytes.len() > ctx.max_field_bytes {
        return Err(PostgresError::Data(format!(
            "field length {} exceeds max_field_bytes {}",
            bytes.len(),
            ctx.max_field_bytes
        )));
    }

    match (oid, format) {
        (BOOL_OID, Format::Binary) => match bytes {
            [0] => Ok(PgValue::Bool(false)),
            [1] => Ok(PgValue::Bool(true)),
            other => Err(PostgresError::Data(format!(
                "invalid binary bool payload {other:?} (expected [0] or [1])"
            ))),
        },
        (BOOL_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            Ok(PgValue::Bool(decode_bool_text(&s)?))
        }
        (INT2_OID, Format::Binary) => {
            let v = types::int2_from_sql(bytes)
                .map_err(|e| PostgresError::Data(format!("invalid binary int2: {e}")))?;
            Ok(PgValue::Int2(v))
        }
        (INT2_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            let v: i16 = s
                .trim()
                .parse()
                .map_err(|_| PostgresError::Data(format!("invalid int2 text {s:?}")))?;
            Ok(PgValue::Int2(v))
        }
        (INT4_OID, Format::Binary) => {
            let v = types::int4_from_sql(bytes)
                .map_err(|e| PostgresError::Data(format!("invalid binary int4: {e}")))?;
            Ok(PgValue::Int4(v))
        }
        (INT4_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            let v: i32 = s
                .trim()
                .parse()
                .map_err(|_| PostgresError::Data(format!("invalid int4 text {s:?}")))?;
            Ok(PgValue::Int4(v))
        }
        (INT8_OID, Format::Binary) => {
            let v = types::int8_from_sql(bytes)
                .map_err(|e| PostgresError::Data(format!("invalid binary int8: {e}")))?;
            Ok(PgValue::Int8(v))
        }
        (INT8_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            let v: i64 = s
                .trim()
                .parse()
                .map_err(|_| PostgresError::Data(format!("invalid int8 text {s:?}")))?;
            Ok(PgValue::Int8(v))
        }
        (OID_OID, Format::Binary) => {
            let v = types::oid_from_sql(bytes)
                .map_err(|e| PostgresError::Data(format!("invalid binary oid: {e}")))?;
            Ok(PgValue::Int8(i64::from(v)))
        }
        (OID_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            let v: u32 = s
                .trim()
                .parse()
                .map_err(|_| PostgresError::Data(format!("invalid oid text {s:?}")))?;
            Ok(PgValue::Int8(i64::from(v)))
        }
        (FLOAT4_OID, Format::Binary) => {
            let v = types::float4_from_sql(bytes)
                .map_err(|e| PostgresError::Data(format!("invalid binary float4: {e}")))?;
            Ok(PgValue::Float4(v))
        }
        (FLOAT4_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            Ok(PgValue::Float4(decode_float4_text(&s)?))
        }
        (FLOAT8_OID, Format::Binary) => {
            let v = types::float8_from_sql(bytes)
                .map_err(|e| PostgresError::Data(format!("invalid binary float8: {e}")))?;
            Ok(PgValue::Float8(v))
        }
        (FLOAT8_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            Ok(PgValue::Float8(decode_float8_text(&s)?))
        }
        (TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID, Format::Binary | Format::Text) => {
            Ok(PgValue::Text(validate_no_nul_utf8(bytes)?))
        }
        (BYTEA_OID, Format::Binary) => Ok(PgValue::Bytes(bytes.to_vec())),
        (BYTEA_OID, Format::Text) => Ok(PgValue::Bytes(decode_bytea_text(bytes)?)),
        (UUID_OID, Format::Binary) => {
            let arr: [u8; 16] = bytes.try_into().map_err(|_| {
                PostgresError::Data(format!("UUID binary must be 16 bytes, got {}", bytes.len()))
            })?;
            Ok(PgValue::Uuid(arr))
        }
        (UUID_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            Ok(PgValue::Uuid(decode_uuid_text(&s)?))
        }
        (NUMERIC_OID, Format::Binary) => {
            Ok(PgValue::Numeric(numeric::decode_numeric_binary(bytes)?))
        }
        (NUMERIC_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            Ok(PgValue::Numeric(numeric::decode_numeric_text(&s)?))
        }
        (DATE_OID, Format::Binary) => Ok(PgValue::Date(temporal::decode_date_binary(bytes)?)),
        (DATE_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            Ok(PgValue::Date(datetime_text::decode_date_text(&s, ctx)?))
        }
        (TIME_OID, Format::Binary) => Ok(PgValue::Time(temporal::decode_time_binary(bytes)?)),
        (TIME_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            Ok(PgValue::Time(datetime_text::decode_time_text(&s)?))
        }
        (TIMESTAMP_OID, Format::Binary) => Ok(PgValue::Timestamp(
            temporal::decode_timestamp_binary(bytes)?,
        )),
        (TIMESTAMP_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            datetime_text::decode_timestamp_text(&s, TemporalKind::Timestamp, ctx)
        }
        (TIMESTAMPTZ_OID, Format::Binary) => Ok(PgValue::TimestampTz(
            temporal::decode_timestamp_binary(bytes)?,
        )),
        (TIMESTAMPTZ_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            datetime_text::decode_timestamp_text(&s, TemporalKind::TimestampTz, ctx)
        }
        (INTERVAL_OID, Format::Binary) => {
            let (months, days, microseconds) = temporal::decode_interval_binary(bytes)?;
            Ok(PgValue::Interval {
                months,
                days,
                microseconds,
            })
        }
        (INTERVAL_OID, Format::Text) => {
            let s = validate_no_nul_utf8(bytes)?;
            let (months, days, microseconds) = interval_text::decode_interval_text(&s, ctx)?;
            Ok(PgValue::Interval {
                months,
                days,
                microseconds,
            })
        }
        (JSON_OID, Format::Binary) => Ok(PgValue::Json(json::decode_json_binary(bytes, ctx)?)),
        (JSON_OID, Format::Text) => Ok(PgValue::Json(json::decode_json_text(bytes, ctx)?)),
        (JSONB_OID, Format::Binary) => Ok(PgValue::Json(json::decode_jsonb_binary(bytes, ctx)?)),
        (JSONB_OID, Format::Text) => Ok(PgValue::Json(json::decode_json_text(bytes, ctx)?)),
        (_, Format::Text) => Ok(PgValue::Text(validate_no_nul_utf8(bytes)?)),
        (unknown_oid, Format::Binary) => Err(PostgresError::Data(format!(
            "cannot decode unknown PostgreSQL OID {unknown_oid} from binary format"
        ))),
    }
}

/// Encode an owned [`PgValue`] into `out` for `target_oid` and `format`.
///
/// Returns `Ok(false)` when `value` is [`PgValue::Null`], and `Ok(true)` when bytes were written.
pub fn encode_value(
    value: &PgValue,
    target_oid: u32,
    format: Format,
    out: &mut BytesMut,
    ctx: &CodecContext,
) -> Result<bool, PostgresError> {
    if matches!(value, PgValue::Null) {
        return Ok(false);
    }
    if target_oid == 0 && format == Format::Binary {
        return Err(PostgresError::Data(
            "cannot encode binary parameter with unspecified target_oid == 0".to_string(),
        ));
    }
    let start_len = out.len();

    match (value, format) {
        (PgValue::Bool(v), Format::Binary) => {
            if target_oid != BOOL_OID {
                return Err(PostgresError::Data(format!(
                    "cannot encode Bool as binary OID {target_oid}"
                )));
            }
            types::bool_to_sql(*v, out);
        }
        (PgValue::Bool(v), Format::Text) => {
            out.put_slice(if *v { b"t" } else { b"f" });
        }
        (PgValue::Int2(v), Format::Binary) => match target_oid {
            INT2_OID => types::int2_to_sql(*v, out),
            INT4_OID => types::int4_to_sql(i32::from(*v), out),
            INT8_OID => types::int8_to_sql(i64::from(*v), out),
            _ => {
                return Err(PostgresError::Data(format!(
                    "cannot encode Int2 as binary OID {target_oid}"
                )));
            }
        },
        (PgValue::Int2(v), Format::Text) => {
            out.put_slice(v.to_string().as_bytes());
        }
        (PgValue::Int4(v), Format::Binary) => match target_oid {
            INT2_OID => {
                let n = i16::try_from(*v)
                    .map_err(|_| PostgresError::Data(format!("Int4 value {v} overflows INT2")))?;
                types::int2_to_sql(n, out);
            }
            INT4_OID => types::int4_to_sql(*v, out),
            INT8_OID => types::int8_to_sql(i64::from(*v), out),
            OID_OID => {
                let u = u32::try_from(*v).map_err(|_| {
                    PostgresError::Data(format!("Int4 value {v} is invalid for OID"))
                })?;
                types::oid_to_sql(u, out);
            }
            _ => {
                return Err(PostgresError::Data(format!(
                    "cannot encode Int4 as binary OID {target_oid}"
                )));
            }
        },
        (PgValue::Int4(v), Format::Text) => {
            out.put_slice(v.to_string().as_bytes());
        }
        (PgValue::Int8(v), Format::Binary) => match target_oid {
            INT2_OID => {
                let n = i16::try_from(*v)
                    .map_err(|_| PostgresError::Data(format!("Int8 value {v} overflows INT2")))?;
                types::int2_to_sql(n, out);
            }
            INT4_OID => {
                let n = i32::try_from(*v)
                    .map_err(|_| PostgresError::Data(format!("Int8 value {v} overflows INT4")))?;
                types::int4_to_sql(n, out);
            }
            INT8_OID => types::int8_to_sql(*v, out),
            OID_OID => {
                let u = u32::try_from(*v).map_err(|_| {
                    PostgresError::Data(format!("Int8 value {v} overflows unsigned 32-bit OID"))
                })?;
                types::oid_to_sql(u, out);
            }
            _ => {
                return Err(PostgresError::Data(format!(
                    "cannot encode Int8 as binary OID {target_oid}"
                )));
            }
        },
        (PgValue::Int8(v), Format::Text) => {
            if target_oid == OID_OID {
                let u = u32::try_from(*v).map_err(|_| {
                    PostgresError::Data(format!("Int8 value {v} overflows unsigned 32-bit OID"))
                })?;
                out.put_slice(u.to_string().as_bytes());
            } else {
                out.put_slice(v.to_string().as_bytes());
            }
        }
        (PgValue::Float4(v), Format::Binary) => match target_oid {
            FLOAT4_OID => types::float4_to_sql(*v, out),
            FLOAT8_OID => types::float8_to_sql(f64::from(*v), out),
            _ => {
                return Err(PostgresError::Data(format!(
                    "cannot encode Float4 as binary OID {target_oid}"
                )));
            }
        },
        (PgValue::Float4(v), Format::Text) => {
            let s = if v.is_nan() {
                "NaN".to_string()
            } else if *v == f32::INFINITY {
                "Infinity".to_string()
            } else if *v == f32::NEG_INFINITY {
                "-Infinity".to_string()
            } else if *v == 0.0 && v.is_sign_negative() {
                "-0.0".to_string()
            } else {
                v.to_string()
            };
            out.put_slice(s.as_bytes());
        }
        (PgValue::Float8(v), Format::Binary) => match target_oid {
            FLOAT8_OID => types::float8_to_sql(*v, out),
            FLOAT4_OID => {
                let narrowed = *v as f32;
                if v.is_finite() && !narrowed.is_finite() {
                    return Err(PostgresError::Data(format!(
                        "Float8 value {v} overflows FLOAT4"
                    )));
                }
                types::float4_to_sql(narrowed, out);
            }
            _ => {
                return Err(PostgresError::Data(format!(
                    "cannot encode Float8 as binary OID {target_oid}"
                )));
            }
        },
        (PgValue::Float8(v), Format::Text) => {
            let s = if v.is_nan() {
                "NaN".to_string()
            } else if *v == f64::INFINITY {
                "Infinity".to_string()
            } else if *v == f64::NEG_INFINITY {
                "-Infinity".to_string()
            } else if *v == 0.0 && v.is_sign_negative() {
                "-0.0".to_string()
            } else {
                v.to_string()
            };
            out.put_slice(s.as_bytes());
        }
        (PgValue::Text(s), Format::Binary) => {
            if s.as_bytes().contains(&0) {
                return Err(PostgresError::Data(
                    "PostgreSQL text value cannot contain embedded NUL byte".to_string(),
                ));
            }
            match target_oid {
                TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID => {
                    out.put_slice(s.as_bytes());
                }
                _ => {
                    return Err(PostgresError::Data(format!(
                        "cannot encode Text as binary OID {target_oid}"
                    )));
                }
            }
        }
        (PgValue::Text(s), Format::Text) => {
            if s.as_bytes().contains(&0) {
                return Err(PostgresError::Data(
                    "PostgreSQL text value cannot contain embedded NUL byte".to_string(),
                ));
            }
            out.put_slice(s.as_bytes());
        }
        (PgValue::Bytes(b), Format::Binary) => {
            if target_oid != BYTEA_OID {
                return Err(PostgresError::Data(format!(
                    "cannot encode Bytes as binary OID {target_oid}"
                )));
            }
            out.put_slice(b);
        }
        (PgValue::Bytes(b), Format::Text) => {
            encode_bytea_hex(b, out);
        }
        (PgValue::Uuid(u), Format::Binary) => {
            if target_oid != UUID_OID {
                return Err(PostgresError::Data(format!(
                    "cannot encode Uuid as binary OID {target_oid}"
                )));
            }
            out.put_slice(u);
        }
        (PgValue::Uuid(u), Format::Text) => {
            out.put_slice(format_uuid_hyphenated(u).as_bytes());
        }
        (PgValue::Numeric(n), Format::Binary) => {
            if target_oid != NUMERIC_OID {
                return Err(PostgresError::Data(format!(
                    "cannot encode Numeric as binary OID {target_oid}"
                )));
            }
            numeric::encode_numeric_binary(n, out)?;
        }
        (PgValue::Numeric(n), Format::Text) => {
            out.put_slice(numeric::encode_numeric_text(n).as_bytes());
        }
        (PgValue::Date(days), Format::Binary) => {
            if target_oid != DATE_OID {
                return Err(PostgresError::Data(format!(
                    "cannot encode Date as binary OID {target_oid}"
                )));
            }
            temporal::encode_date_binary(*days, out)?;
        }
        (PgValue::Time(micros), Format::Binary) => {
            if target_oid != TIME_OID {
                return Err(PostgresError::Data(format!(
                    "cannot encode Time as binary OID {target_oid}"
                )));
            }
            temporal::encode_time_binary(*micros, out)?;
        }
        (PgValue::Timestamp(micros), Format::Binary) => {
            if target_oid != TIMESTAMP_OID {
                return Err(PostgresError::Data(format!(
                    "cannot encode naive Timestamp as binary OID {target_oid} (expected {TIMESTAMP_OID})"
                )));
            }
            temporal::encode_timestamp_binary(*micros, out)?;
        }
        (PgValue::TimestampTz(micros), Format::Binary) => {
            if target_oid != TIMESTAMPTZ_OID {
                return Err(PostgresError::Data(format!(
                    "cannot encode TimestampTz as binary OID {target_oid} (expected {TIMESTAMPTZ_OID})"
                )));
            }
            temporal::encode_timestamp_binary(*micros, out)?;
        }
        (
            PgValue::Date(_)
            | PgValue::Time(_)
            | PgValue::Timestamp(_)
            | PgValue::TimestampTz(_)
            | PgValue::TemporalText { .. },
            Format::Text,
        ) => {
            if matches!(value, PgValue::Timestamp(_))
                && target_oid != 0
                && target_oid != TIMESTAMP_OID
            {
                return Err(PostgresError::Data(format!(
                    "cannot encode naive Timestamp as text OID {target_oid}"
                )));
            }
            if matches!(value, PgValue::TimestampTz(_))
                && target_oid != 0
                && target_oid != TIMESTAMPTZ_OID
            {
                return Err(PostgresError::Data(format!(
                    "cannot encode TimestampTz as text OID {target_oid}"
                )));
            }
            let rendered = datetime_text::encode_temporal_text(value)?;
            out.put_slice(rendered.as_bytes());
        }
        (PgValue::TemporalText { .. }, Format::Binary) => {
            return Err(PostgresError::Data(
                "unresolved TemporalText (wire_text) values are output-only and cannot be bound"
                    .to_string(),
            ));
        }
        (
            PgValue::Interval {
                months,
                days,
                microseconds,
            },
            Format::Binary,
        ) => {
            if target_oid != INTERVAL_OID {
                return Err(PostgresError::Data(format!(
                    "cannot encode Interval as binary OID {target_oid}"
                )));
            }
            temporal::encode_interval_binary(*months, *days, *microseconds, out)?;
        }
        (
            PgValue::Interval {
                months,
                days,
                microseconds,
            },
            Format::Text,
        ) => {
            temporal::validate_interval_parts(*months, *days, *microseconds)?;
            let s = interval_text::encode_interval_text(*months, *days, *microseconds);
            out.put_slice(s.as_bytes());
        }
        (PgValue::Json(raw_json), Format::Binary) => match target_oid {
            JSON_OID => json::encode_json_binary(raw_json, out, ctx)?,
            JSONB_OID => json::encode_jsonb_binary(raw_json, out, ctx)?,
            _ => {
                return Err(PostgresError::Data(format!(
                    "cannot encode Json as binary OID {target_oid}"
                )));
            }
        },
        (PgValue::Json(raw_json), Format::Text) => {
            let for_jsonb = target_oid == JSONB_OID || target_oid == 0;
            json::encode_json_text(raw_json, for_jsonb, out, ctx)?;
        }
        (other, fmt) => {
            return Err(PostgresError::Data(format!(
                "unsupported encode for value {other:?} in format {fmt:?}"
            )));
        }
    }

    if out.len() - start_len > ctx.max_field_bytes {
        out.truncate(start_len);
        return Err(PostgresError::Data(format!(
            "encoded parameter exceeds max_field_bytes {}",
            ctx.max_field_bytes
        )));
    }
    Ok(true)
}
