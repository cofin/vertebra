//! PostgreSQL v3 frontend frame encoders, pooler topology rules, binary `JSONB`, `PGCOPY`, and pgvector codecs.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// Canonical 11-byte PostgreSQL binary `COPY` stream header signature.
pub const PGCOPY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

/// PostgreSQL connection pooler topology mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PoolerMode {
    /// Direct connection to PostgreSQL primary/replica (`5432`) or session-mode pooler.
    Direct,
    /// Traditional PgBouncer in `pool_mode = transaction` without protocol-level prepared statement multiplexing.
    PgBouncerTransaction,
    /// PgCat transaction pooler supporting protocol-level prepared statement rewriting.
    PgCat,
    /// Supavisor transaction pooler supporting protocol-level prepared statement multiplexing.
    Supavisor,
}

impl PoolerMode {
    /// Parse a pooler mode name (`"direct"`, `"transaction"`, `"pgbouncer_transaction"`, `"pgcat"`, `"supavisor"`).
    pub fn parse(mode: &str) -> Self {
        match mode.trim().to_ascii_lowercase().as_str() {
            "transaction" | "pgbouncer" | "pgbouncer_transaction" | "statement" => {
                Self::PgBouncerTransaction
            }
            "pgcat" => Self::PgCat,
            "supavisor" | "pgdog" => Self::Supavisor,
            _ => Self::Direct,
        }
    }

    /// Return `true` if the pooler mode safely supports protocol-level (`Parse`/`Bind`) prepared statements.
    pub const fn supports_protocol_prepared_statements(&self) -> bool {
        match self {
            Self::Direct | Self::PgCat | Self::Supavisor => true,
            Self::PgBouncerTransaction => false,
        }
    }

    /// Return `true` if the connection requires `DISCARD ALL` when returned to the pool.
    pub const fn requires_discard_all_on_release(&self) -> bool {
        matches!(self, Self::Direct)
    }

    /// Return the connection reset SQL statement for this pooler mode.
    pub const fn reset_query(&self) -> &'static str {
        if self.requires_discard_all_on_release() {
            "DISCARD ALL"
        } else {
            ""
        }
    }

    /// Return the effective prepared statement cache capacity for this pooler mode.
    pub fn effective_statement_cache_capacity(
        self,
        configured: Option<usize>,
        protocol_level_pooler_cache: bool,
    ) -> usize {
        if !self.supports_protocol_prepared_statements() && !protocol_level_pooler_cache {
            0
        } else {
            configured.unwrap_or(256)
        }
    }
}

/// Compute per-worker database pool size from a total cluster connection budget.
pub fn compute_worker_pool_budget(
    total_budget: usize,
    replica_count: usize,
    workers_per_replica: usize,
    reserved_admin_connections: usize,
) -> usize {
    let usable = total_budget.saturating_sub(reserved_admin_connections);
    let total_shards = replica_count
        .max(1)
        .saturating_mul(workers_per_replica.max(1));
    (usable / total_shards).max(1)
}

/// Strip the 1-byte PostgreSQL binary `JSONB` version header (`0x01`) with zero copy.
pub fn strip_pg_jsonb_header(raw: &[u8]) -> Result<&[u8], &'static str> {
    if raw.is_empty() {
        return Err("empty PostgreSQL binary JSONB payload");
    }
    if raw[0] != 0x01 {
        return Err("unsupported PostgreSQL binary JSONB version byte (expected 0x01)");
    }
    Ok(&raw[1..])
}

/// Prepend the 1-byte PostgreSQL binary `JSONB` version header (`0x01`) to UTF-8 JSON bytes.
pub fn encode_pg_jsonb(json_bytes: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(1 + json_bytes.len());
    buf.put_u8(0x01);
    buf.extend_from_slice(json_bytes);
    buf.freeze()
}

/// Encode a PostgreSQL v3.0 (`196608`) `StartupMessage` wire frame.
pub fn encode_startup_message(user: &str, database: &str, application_name: &str) -> Bytes {
    let mut body = BytesMut::with_capacity(128);
    body.put_u32(0x0003_0000);
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(database.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(b"application_name\0");
    body.extend_from_slice(application_name.as_bytes());
    body.put_u8(0);
    body.put_u8(0);

    let total_len = (4 + body.len()) as u32;
    let mut frame = BytesMut::with_capacity(total_len as usize);
    frame.put_u32(total_len);
    frame.extend_from_slice(&body);
    frame.freeze()
}

/// Encode a PostgreSQL v3 extended-query `Parse` (`'P'`) wire frame.
pub fn encode_parse_frame(stmt_name: &str, sql: &str, param_oids: &[u32]) -> Bytes {
    let body_len = 4 + stmt_name.len() + 1 + sql.len() + 1 + 2 + (param_oids.len() * 4);
    let mut buf = BytesMut::with_capacity(1 + body_len);
    buf.put_u8(b'P');
    buf.put_u32(body_len as u32);
    buf.extend_from_slice(stmt_name.as_bytes());
    buf.put_u8(0);
    buf.extend_from_slice(sql.as_bytes());
    buf.put_u8(0);
    buf.put_i16(param_oids.len() as i16);
    for &oid in param_oids {
        buf.put_u32(oid);
    }
    buf.freeze()
}

/// Encode a PostgreSQL v3 extended-query `Bind` (`'B'`) wire frame.
pub fn encode_bind_frame(portal: &str, stmt_name: &str, params: &[Option<Vec<u8>>]) -> Bytes {
    let mut payload = BytesMut::with_capacity(64 + params.len() * 16);
    payload.extend_from_slice(portal.as_bytes());
    payload.put_u8(0);
    payload.extend_from_slice(stmt_name.as_bytes());
    payload.put_u8(0);
    // Single binary parameter format code (1) applied to all parameters
    payload.put_i16(1);
    payload.put_i16(1);
    payload.put_i16(params.len() as i16);
    for param in params {
        match param {
            Some(bytes) => {
                payload.put_i32(bytes.len() as i32);
                payload.extend_from_slice(bytes);
            }
            None => {
                payload.put_i32(-1);
            }
        }
    }
    // Request binary result format (1) for all output columns
    payload.put_i16(1);
    payload.put_i16(1);

    let mut frame = BytesMut::with_capacity(5 + payload.len());
    frame.put_u8(b'B');
    frame.put_u32((4 + payload.len()) as u32);
    frame.extend_from_slice(&payload);
    frame.freeze()
}

/// Encode a PostgreSQL v3 extended-query `Execute` (`'E'`) wire frame.
pub fn encode_execute_frame(portal: &str, max_rows: u32) -> Bytes {
    let body_len = 4 + portal.len() + 1 + 4;
    let mut buf = BytesMut::with_capacity(1 + body_len);
    buf.put_u8(b'E');
    buf.put_u32(body_len as u32);
    buf.extend_from_slice(portal.as_bytes());
    buf.put_u8(0);
    buf.put_u32(max_rows);
    buf.freeze()
}

/// Encode a PostgreSQL v3 extended-query `Sync` (`'S'`) wire frame.
pub fn encode_sync_frame() -> Bytes {
    Bytes::from_static(b"S\x00\x00\x00\x04")
}

/// Encode a PostgreSQL v3 simple `Query` (`'Q'`) wire frame.
pub fn encode_query_frame(sql: &str) -> Bytes {
    let body_len = 4 + sql.len() + 1;
    let mut buf = BytesMut::with_capacity(1 + body_len);
    buf.put_u8(b'Q');
    buf.put_u32(body_len as u32);
    buf.extend_from_slice(sql.as_bytes());
    buf.put_u8(0);
    buf.freeze()
}

/// Encode rows into a PostgreSQL binary `COPY` stream (`PGCOPY\n\xff\r\n\0` + flags + rows + `-1i16` trailer).
pub fn encode_pgcopy_binary_rows(rows: &[Vec<Option<Vec<u8>>>]) -> Bytes {
    let mut buf = BytesMut::with_capacity(64 + rows.len() * 32);
    buf.extend_from_slice(PGCOPY_SIGNATURE);
    buf.put_u32(0); // flags field
    buf.put_u32(0); // header extension area length

    for row in rows {
        buf.put_i16(row.len() as i16);
        for col in row {
            match col {
                Some(bytes) => {
                    buf.put_i32(bytes.len() as i32);
                    buf.extend_from_slice(bytes);
                }
                None => {
                    buf.put_i32(-1);
                }
            }
        }
    }
    buf.put_i16(-1); // file trailer
    buf.freeze()
}

/// Decode a PostgreSQL binary `COPY` stream (`PGCOPY\n\xff\r\n\0`) into rows of optional byte vectors.
pub fn decode_pgcopy_binary_rows(buf: &[u8]) -> Result<Vec<Vec<Option<Vec<u8>>>>, String> {
    if buf.len() < PGCOPY_SIGNATURE.len() || &buf[..PGCOPY_SIGNATURE.len()] != PGCOPY_SIGNATURE {
        return Err("invalid PGCOPY binary signature".to_string());
    }
    if buf.len() < PGCOPY_SIGNATURE.len() + 8 {
        return Err("truncated PGCOPY binary header".to_string());
    }

    let sig_len = PGCOPY_SIGNATURE.len();
    let _flags = u32::from_be_bytes([
        buf[sig_len],
        buf[sig_len + 1],
        buf[sig_len + 2],
        buf[sig_len + 3],
    ]);
    let ext_len = u32::from_be_bytes([
        buf[sig_len + 4],
        buf[sig_len + 5],
        buf[sig_len + 6],
        buf[sig_len + 7],
    ]) as usize;

    let mut pos = sig_len + 8 + ext_len;
    if pos > buf.len() {
        return Err("corrupted PGCOPY header extension length".to_string());
    }

    let mut rows = Vec::new();
    while pos + 2 <= buf.len() {
        let field_count = i16::from_be_bytes([buf[pos], buf[pos + 1]]);
        pos += 2;
        if field_count == -1 {
            return Ok(rows);
        }
        if field_count < -1 {
            return Err(format!("invalid PGCOPY field count: {field_count}"));
        }

        let count = field_count as usize;
        let mut row = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 4 > buf.len() {
                return Err("truncated PGCOPY field length".to_string());
            }
            let col_len = i32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
            pos += 4;
            if col_len == -1 {
                row.push(None);
            } else if col_len < -1 {
                return Err(format!("invalid negative PGCOPY column length: {col_len}"));
            } else {
                let len = col_len as usize;
                if pos + len > buf.len() {
                    return Err("truncated PGCOPY column payload".to_string());
                }
                row.push(Some(buf[pos..pos + len].to_vec()));
                pos += len;
            }
        }
        rows.push(row);
    }

    Err("missing PGCOPY -1 trailer".to_string())
}

#[inline]
fn f32_to_f16_bits(val: f32) -> u16 {
    let bits = val.to_bits();
    let sign = ((bits >> 16) & 0x8000) as u16;
    let exp = ((bits >> 23) & 0xff) as i32 - 127 + 15;
    let mant = bits & 0x007f_ffff;
    if exp <= 0 {
        if exp < -10 {
            return sign;
        }
        let m = (mant | 0x0080_0000) >> (1 - exp);
        return sign | ((m + 0x0000_1000) >> 13) as u16;
    } else if exp == 0xff - (127 - 15) {
        if mant == 0 {
            return sign | 0x7c00;
        }
        return sign | 0x7c00 | ((mant >> 13) as u16).max(1);
    } else if exp > 30 {
        return sign | 0x7c00;
    }
    sign | ((exp as u16) << 10) | ((mant + 0x0000_1000) >> 13) as u16
}

#[inline]
fn f16_bits_to_f32(bits: u16) -> f32 {
    let sign = ((bits & 0x8000) as u32) << 16;
    let exp = ((bits >> 10) & 0x1f) as u32;
    let mant = (bits & 0x03ff) as u32;
    let f_bits = if exp == 0 {
        if mant == 0 {
            sign
        } else {
            let mut m = mant;
            let mut e = 127u32 - 14;
            while (m & 0x0400) == 0 {
                m <<= 1;
                e -= 1;
            }
            m &= 0x03ff;
            sign | (e << 23) | (m << 13)
        }
    } else if exp == 31 {
        sign | 0x7f80_0000 | (mant << 13)
    } else {
        sign | ((exp + 127 - 15) << 23) | (mant << 13)
    };
    f32::from_bits(f_bits)
}

/// Encode a `pgvector` binary `vector` (`[u16 dim BE][u16 unused=0 BE][f32 BE; dim]`).
pub fn encode_pgvector_f32(values: &[f32]) -> Result<Vec<u8>, String> {
    if values.is_empty() {
        return Err("pgvector must have at least 1 dimension".to_string());
    }
    if values.len() > u16::MAX as usize {
        return Err(format!(
            "pgvector dimension {} exceeds u16::MAX",
            values.len()
        ));
    }
    let mut out = Vec::with_capacity(4 + values.len() * 4);
    out.extend_from_slice(&(values.len() as u16).to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    for &v in values {
        if v.is_nan() {
            return Err("pgvector does not allow NaN values".to_string());
        }
        if v.is_infinite() {
            return Err("pgvector does not allow infinite values".to_string());
        }
        out.extend_from_slice(&v.to_be_bytes());
    }
    Ok(out)
}

/// Decode a `pgvector` binary `vector` (`[u16 dim BE][u16 unused=0 BE][f32 BE; dim]`).
pub fn decode_pgvector_f32(buf: &[u8]) -> Result<Vec<f32>, String> {
    if buf.len() < 4 {
        return Err("pgvector binary buffer too short (< 4 bytes)".to_string());
    }
    let dim = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let unused = u16::from_be_bytes([buf[2], buf[3]]);
    if unused != 0 {
        return Err(format!(
            "pgvector unused header field must be 0, got {unused}"
        ));
    }
    let expected = 4 + dim * 4;
    if buf.len() != expected {
        return Err(format!(
            "pgvector byte length {} does not match expected {expected} for dim {dim}",
            buf.len()
        ));
    }
    let mut out = Vec::with_capacity(dim);
    for &chunk in buf[4..].as_chunks::<4>().0 {
        out.push(f32::from_be_bytes(chunk));
    }
    Ok(out)
}

/// Encode a `pgvector` binary `halfvec` (`[u16 dim BE][u16 unused=0 BE][f16 BE; dim]`).
pub fn encode_pgvector_f16(values: &[f32]) -> Result<Vec<u8>, String> {
    if values.is_empty() {
        return Err("halfvec must have at least 1 dimension".to_string());
    }
    if values.len() > u16::MAX as usize {
        return Err(format!(
            "halfvec dimension {} exceeds u16::MAX",
            values.len()
        ));
    }
    let mut out = Vec::with_capacity(4 + values.len() * 2);
    out.extend_from_slice(&(values.len() as u16).to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    for &v in values {
        if v.is_nan() || v.is_infinite() {
            return Err("halfvec does not allow NaN or infinite values".to_string());
        }
        out.extend_from_slice(&f32_to_f16_bits(v).to_be_bytes());
    }
    Ok(out)
}

/// Decode a `pgvector` binary `halfvec` (`[u16 dim BE][u16 unused=0 BE][f16 BE; dim]`).
pub fn decode_pgvector_f16(buf: &[u8]) -> Result<Vec<f32>, String> {
    if buf.len() < 4 {
        return Err("halfvec binary buffer too short (< 4 bytes)".to_string());
    }
    let dim = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let unused = u16::from_be_bytes([buf[2], buf[3]]);
    if unused != 0 {
        return Err(format!(
            "halfvec unused header field must be 0, got {unused}"
        ));
    }
    let expected = 4 + dim * 2;
    if buf.len() != expected {
        return Err(format!(
            "halfvec byte length {} does not match expected {expected} for dim {dim}",
            buf.len()
        ));
    }
    let mut out = Vec::with_capacity(dim);
    for &chunk in buf[4..].as_chunks::<2>().0 {
        let bits = u16::from_be_bytes(chunk);
        out.push(f16_bits_to_f32(bits));
    }
    Ok(out)
}

/// Encode a PostgreSQL / `pgvector` binary `bit` (`VARBIT`: `[i32 bit_len BE][u8; ceil(bit_len / 8)]`).
pub fn encode_pgvector_bit(packed_bytes: &[u8], bit_len: usize) -> Result<Vec<u8>, String> {
    if bit_len == 0 {
        return Err("pgvector bit_len must be > 0".to_string());
    }
    let expected_bytes = bit_len.div_ceil(8);
    if packed_bytes.len() != expected_bytes {
        return Err(format!(
            "packed_bytes length {} does not match bit_len {bit_len} (expected {expected_bytes})",
            packed_bytes.len()
        ));
    }
    let mut out = Vec::with_capacity(4 + expected_bytes);
    out.extend_from_slice(&(bit_len as i32).to_be_bytes());
    out.extend_from_slice(packed_bytes);
    Ok(out)
}

/// Decode a PostgreSQL / `pgvector` binary `bit` (`VARBIT`: `[i32 bit_len BE][u8; ceil(bit_len / 8)]`).
pub fn decode_pgvector_bit(buf: &[u8]) -> Result<(Vec<u8>, usize), String> {
    if buf.len() < 4 {
        return Err("pgvector bit buffer too short (< 4 bytes)".to_string());
    }
    let bit_len_i32 = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if bit_len_i32 <= 0 {
        return Err(format!("invalid pgvector bit_len {bit_len_i32}"));
    }
    let bit_len = bit_len_i32 as usize;
    let expected_bytes = bit_len.div_ceil(8);
    if buf.len() != 4 + expected_bytes {
        return Err(format!(
            "pgvector bit buffer length {} does not match expected {}",
            buf.len(),
            4 + expected_bytes
        ));
    }
    Ok((buf[4..].to_vec(), bit_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pooler_mode_rules() {
        assert!(PoolerMode::Direct.supports_protocol_prepared_statements());
        assert!(PoolerMode::Direct.requires_discard_all_on_release());
        assert_eq!(PoolerMode::Direct.reset_query(), "DISCARD ALL");

        assert!(!PoolerMode::PgBouncerTransaction.supports_protocol_prepared_statements());
        assert!(!PoolerMode::PgBouncerTransaction.requires_discard_all_on_release());
        assert_eq!(PoolerMode::PgBouncerTransaction.reset_query(), "");

        assert!(PoolerMode::PgCat.supports_protocol_prepared_statements());
        assert!(PoolerMode::Supavisor.supports_protocol_prepared_statements());
        assert_eq!(compute_worker_pool_budget(200, 5, 4, 20), 9);
    }

    #[test]
    fn test_pg_jsonb_encode_and_strip() {
        let raw_json = br#"{"id":42,"active":true}"#;
        let encoded = encode_pg_jsonb(raw_json);
        assert_eq!(encoded[0], 0x01);
        let stripped = strip_pg_jsonb_header(&encoded).unwrap();
        assert_eq!(stripped, raw_json);
        assert!(strip_pg_jsonb_header(b"").is_err());
        assert!(strip_pg_jsonb_header(b"\x02{}").is_err());
    }

    #[test]
    fn test_postgres_wire_frames() {
        let startup = encode_startup_message("postgres", "appdb", "vertebra");
        let len = u32::from_be_bytes([startup[0], startup[1], startup[2], startup[3]]) as usize;
        assert_eq!(len, startup.len());

        let parse = encode_parse_frame("s1", "SELECT $1::int4", &[23]);
        assert_eq!(parse[0], b'P');

        let bind = encode_bind_frame("", "s1", &[Some(vec![0, 0, 0, 7]), None]);
        assert_eq!(bind[0], b'B');

        let exec = encode_execute_frame("", 0);
        assert_eq!(exec[0], b'E');

        let sync = encode_sync_frame();
        assert_eq!(&sync[..], b"S\x00\x00\x00\x04");

        let query = encode_query_frame("SELECT 1");
        assert_eq!(query[0], b'Q');
    }

    #[test]
    fn test_pgcopy_binary_roundtrip() {
        let rows = vec![
            vec![Some(b"hello".to_vec()), None, Some(vec![1, 2, 3, 4])],
            vec![None, Some(b"world".to_vec()), Some(vec![])],
        ];
        let encoded = encode_pgcopy_binary_rows(&rows);
        let decoded = decode_pgcopy_binary_rows(&encoded).unwrap();
        assert_eq!(decoded, rows);
    }

    #[test]
    fn test_pgvector_f32_f16_and_bit_codecs() {
        let vals = [0.25f32, -0.5, 1.0, -2.0];
        let enc32 = encode_pgvector_f32(&vals).expect("encode f32");
        assert_eq!(enc32.len(), 20);
        let dec32 = decode_pgvector_f32(&enc32).expect("decode f32");
        assert_eq!(dec32, vals);

        let enc16 = encode_pgvector_f16(&vals).expect("encode f16");
        assert_eq!(enc16.len(), 12);
        let dec16 = decode_pgvector_f16(&enc16).expect("decode f16");
        for (a, b) in vals.iter().zip(dec16.iter()) {
            assert!((a - b).abs() < 1e-3);
        }

        let bits = [0b1010_0101u8, 0b1100_0011];
        let enc_bit = encode_pgvector_bit(&bits, 16).expect("encode bit");
        let (dec_bits, bit_len) = decode_pgvector_bit(&enc_bit).expect("decode bit");
        assert_eq!(dec_bits, bits);
        assert_eq!(bit_len, 16);

        assert!(encode_pgvector_f32(&[f32::NAN]).is_err());
        assert!(encode_pgvector_f32(&[f32::INFINITY]).is_err());
    }
}
