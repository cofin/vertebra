//! Pure-Rust SQL Server Tabular Data Stream (TDS 7.4) framing, `@P1` parameter rewriter, and SHM pool.

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use vertebra_core::broker::{
    LeaseGrant, LeaseTableStats, ShmLeaseTable, ShmStatementCache, StatementCacheStats,
    StatementPlanEntry,
};
use vertebra_core::shm::ShmRegion;

/// SQL Server (`MSSQL`) dialect tag for `ShmStatementCache`.
pub const MSSQL_DIALECT_TAG: u8 = 5;

/// Tabular Data Stream (TDS) packet type codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum TdsPacketType {
    SqlBatch = 0x01,
    Rpc = 0x03,
    TabularResult = 0x04,
    Attention = 0x06,
    BulkLoad = 0x07,
    TransMgrReq = 0x0E,
    Tds7Login = 0x10,
    PreLogin = 0x12,
}

impl TdsPacketType {
    /// Convert a raw TDS packet type byte into `TdsPacketType`.
    pub const fn from_u8(val: u8) -> Option<Self> {
        match val {
            0x01 => Some(Self::SqlBatch),
            0x03 => Some(Self::Rpc),
            0x04 => Some(Self::TabularResult),
            0x06 => Some(Self::Attention),
            0x07 => Some(Self::BulkLoad),
            0x0E => Some(Self::TransMgrReq),
            0x10 => Some(Self::Tds7Login),
            0x12 => Some(Self::PreLogin),
            _ => None,
        }
    }
}

/// Encode an 8-byte TDS packet header (`[type, status, len_be_u16, spid_be_u16, packet_id, window]`) + payload.
pub fn encode_tds_packet(
    packet_type: TdsPacketType,
    packet_id: u8,
    is_eom: bool,
    payload: &[u8],
) -> Bytes {
    let total_len = 8 + payload.len();
    let mut buf = BytesMut::with_capacity(total_len);
    buf.put_u8(packet_type as u8);
    buf.put_u8(if is_eom { 0x01 } else { 0x00 });
    buf.put_u16(total_len as u16);
    buf.put_u16(0); // SPID
    buf.put_u8(packet_id);
    buf.put_u8(0); // window
    buf.extend_from_slice(payload);
    buf.freeze()
}

/// Decode a TDS packet, returning `(packet_type, is_eom, packet_id, payload_slice)`.
pub fn decode_tds_packet(buf: &[u8]) -> Result<(TdsPacketType, bool, u8, &[u8]), String> {
    if buf.len() < 8 {
        return Err("buffer too short for 8-byte TDS packet header".to_string());
    }
    let pkt_type = TdsPacketType::from_u8(buf[0])
        .ok_or_else(|| format!("unsupported TDS packet type byte: 0x{:02x}", buf[0]))?;
    let is_eom = (buf[1] & 0x01) != 0;
    let total_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    if total_len < 8 {
        return Err(format!("invalid TDS packet length {total_len} (< 8)"));
    }
    if buf.len() < total_len {
        return Err(format!(
            "truncated TDS packet: expected {total_len} bytes, got {}",
            buf.len()
        ));
    }
    let packet_id = buf[6];
    Ok((pkt_type, is_eom, packet_id, &buf[8..total_len]))
}

/// Encode a TDS 7.4 `SQLBatch` packet containing the 22-byte `ALL_HEADERS` `TransactionDescriptor`
/// followed by UTF-16LE encoded SQL text.
pub fn encode_tds_sql_batch(sql: &str, transaction_descriptor: u64) -> Bytes {
    let utf16_units: Vec<u16> = sql.encode_utf16().collect();
    let mut payload = BytesMut::with_capacity(22 + utf16_units.len() * 2);

    // TDS 7.2+ ALL_HEADERS (22 bytes total)
    payload.put_u32_le(22); // TotalLength of ALL_HEADERS
    payload.put_u32_le(18); // HeaderLength of TransactionDescriptor header
    payload.put_u16_le(0x0002); // HeaderType = 0x0002 (TransactionDescriptor)
    payload.put_u64_le(transaction_descriptor);
    payload.put_u32_le(1); // OutstandingRequestCount = 1

    for unit in utf16_units {
        payload.put_u16_le(unit);
    }

    encode_tds_packet(TdsPacketType::SqlBatch, 1, true, &payload)
}

/// Single-pass state machine rewriting `?` and `$1`/`$2` placeholders to T-SQL `@P1, @P2, ...`
/// while preserving placeholders inside single-quoted literals (`'...'`), double-quoted identifiers (`"..."`),
/// bracketed identifiers (`[...]`), `--` line comments, and `/* ... */` block comments.
pub fn rewrite_mssql_placeholders(sql: &str) -> (String, usize) {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum LexState {
        Normal,
        SingleQuote,
        DoubleQuote,
        BracketIdent,
        LineComment,
        BlockComment,
    }

    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 16);
    let mut state = LexState::Normal;
    let mut i = 0usize;
    let mut seq_index = 0usize;
    let mut max_param = 0usize;

    while i < bytes.len() {
        let b = bytes[i];
        match state {
            LexState::Normal => {
                if b == b'\'' {
                    out.push('\'');
                    state = LexState::SingleQuote;
                    i += 1;
                } else if b == b'"' {
                    out.push('"');
                    state = LexState::DoubleQuote;
                    i += 1;
                } else if b == b'[' {
                    out.push('[');
                    state = LexState::BracketIdent;
                    i += 1;
                } else if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    out.push_str("--");
                    state = LexState::LineComment;
                    i += 2;
                } else if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    out.push_str("/*");
                    state = LexState::BlockComment;
                    i += 2;
                } else if b == b'?' {
                    seq_index += 1;
                    max_param = max_param.max(seq_index);
                    out.push_str(&format!("@P{seq_index}"));
                    i += 1;
                } else if b == b'$'
                    && i + 1 < bytes.len()
                    && bytes[i + 1].is_ascii_digit()
                    && bytes[i + 1] != b'0'
                {
                    let mut j = i + 1;
                    let mut num = 0usize;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        num = num * 10 + ((bytes[j] - b'0') as usize);
                        j += 1;
                    }
                    seq_index = seq_index.max(num);
                    max_param = max_param.max(num);
                    out.push_str(&format!("@P{num}"));
                    i = j;
                } else {
                    let ch = sql[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
            LexState::SingleQuote => {
                if b == b'\'' {
                    out.push('\'');
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        out.push('\'');
                        i += 2;
                    } else {
                        state = LexState::Normal;
                        i += 1;
                    }
                } else {
                    let ch = sql[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
            LexState::DoubleQuote => {
                if b == b'"' {
                    out.push('"');
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        out.push('"');
                        i += 2;
                    } else {
                        state = LexState::Normal;
                        i += 1;
                    }
                } else {
                    let ch = sql[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
            LexState::BracketIdent => {
                if b == b']' {
                    out.push(']');
                    if i + 1 < bytes.len() && bytes[i + 1] == b']' {
                        out.push(']');
                        i += 2;
                    } else {
                        state = LexState::Normal;
                        i += 1;
                    }
                } else {
                    let ch = sql[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
            LexState::LineComment => {
                let ch = sql[i..].chars().next().unwrap();
                out.push(ch);
                i += ch.len_utf8();
                if ch == '\n' {
                    state = LexState::Normal;
                }
            }
            LexState::BlockComment => {
                if b == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    out.push_str("*/");
                    state = LexState::Normal;
                    i += 2;
                } else {
                    let ch = sql[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
        }
    }

    (out, max_param)
}

/// Shared-memory coordinated SQL Server (`MSSQL`) connection pool and statement cache.
#[derive(Clone)]
pub struct MssqlPool {
    pub dsn: String,
    lease_table: Arc<ShmLeaseTable>,
    stmt_cache: Arc<ShmStatementCache>,
}

impl MssqlPool {
    /// Initialize a new SHM-backed `MssqlPool`.
    pub fn new(
        dsn: String,
        pool_size: u32,
        max_overflow: u32,
        stmt_cache_capacity: u32,
    ) -> Result<Self, String> {
        let lease_size = ShmLeaseTable::required_size(pool_size, max_overflow);
        let lease_region = ShmRegion::create(lease_size)
            .map_err(|e| format!("failed to allocate MSSQL lease ShmRegion: {e}"))?;
        let lease_table = ShmLeaseTable::init_in_region(lease_region, pool_size, max_overflow)
            .map_err(|e| format!("failed to init MSSQL ShmLeaseTable: {e}"))?;

        let cap = stmt_cache_capacity.max(4);
        let cache_size = ShmStatementCache::required_size(cap);
        let cache_region = ShmRegion::create(cache_size)
            .map_err(|e| format!("failed to allocate MSSQL stmt cache ShmRegion: {e}"))?;
        let stmt_cache = ShmStatementCache::init_in_region(cache_region, cap)
            .map_err(|e| format!("failed to init MSSQL ShmStatementCache: {e}"))?;

        Ok(Self {
            dsn,
            lease_table: Arc::new(lease_table),
            stmt_cache: Arc::new(stmt_cache),
        })
    }

    /// Acquire a connection lease from the shared-memory lease table.
    pub fn acquire_lease(
        &self,
        worker_pid: u32,
        ttl_ns: u64,
        timeout: Duration,
    ) -> Result<LeaseGrant, String> {
        self.lease_table
            .acquire(worker_pid, ttl_ns, timeout, 0)
            .map_err(|e| e.to_string())
    }

    /// Release a previously acquired connection lease.
    pub fn release_lease(&self, slot_index: u32, generation: u64) -> Result<(), String> {
        self.lease_table
            .release(slot_index, generation)
            .map_err(|e| e.to_string())
    }

    /// Look up or insert a rewritten T-SQL statement plan in the shared-memory statement cache.
    pub fn lookup_or_compile_statement(&self, sql: &str) -> Option<StatementPlanEntry> {
        if let Some(entry) = self.stmt_cache.lookup(MSSQL_DIALECT_TAG, sql.as_bytes()) {
            return Some(entry);
        }
        let (rewritten, count) = rewrite_mssql_placeholders(sql);
        self.stmt_cache.insert(
            MSSQL_DIALECT_TAG,
            sql.as_bytes(),
            count as u16,
            rewritten.as_bytes(),
            &[],
        );
        self.stmt_cache.lookup(MSSQL_DIALECT_TAG, sql.as_bytes())
    }

    /// Return current lease table and statement cache telemetry.
    pub fn stats(&self) -> (LeaseTableStats, StatementCacheStats) {
        (self.lease_table.stats(), self.stmt_cache.stats())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tds_packet_and_sql_batch_framing() {
        let pkt = encode_tds_packet(
            TdsPacketType::PreLogin,
            1,
            true,
            b"\x00\x00\x1a\x00\x06\x04",
        );
        let (pkt_type, is_eom, pkt_id, payload) = decode_tds_packet(&pkt).unwrap();
        assert_eq!(pkt_type, TdsPacketType::PreLogin);
        assert!(is_eom);
        assert_eq!(pkt_id, 1);
        assert_eq!(payload, b"\x00\x00\x1a\x00\x06\x04");

        let batch = encode_tds_sql_batch("SELECT 1", 0x1122_3344_5566_7788);
        let (b_type, b_eom, _, b_payload) = decode_tds_packet(&batch).unwrap();
        assert_eq!(b_type, TdsPacketType::SqlBatch);
        assert!(b_eom);
        assert_eq!(
            u32::from_le_bytes([b_payload[0], b_payload[1], b_payload[2], b_payload[3]]),
            22
        );
        assert_eq!(
            u64::from_le_bytes([
                b_payload[10],
                b_payload[11],
                b_payload[12],
                b_payload[13],
                b_payload[14],
                b_payload[15],
                b_payload[16],
                b_payload[17]
            ]),
            0x1122_3344_5566_7788
        );
        // "SELECT 1" in UTF-16LE is 16 bytes
        assert_eq!(b_payload.len(), 22 + 16);
    }

    #[test]
    fn test_rewrite_mssql_placeholders_preserves_quotes_and_comments() {
        let sql = "SELECT 'a?b''$1', \"col?name\", [col$2?], * FROM t WHERE a = ? AND b = $2 -- ? $3 comment\n AND /* ? $4 */ c = ?";
        let (rewritten, count) = rewrite_mssql_placeholders(sql);
        assert_eq!(
            rewritten,
            "SELECT 'a?b''$1', \"col?name\", [col$2?], * FROM t WHERE a = @P1 AND b = @P2 -- ? $3 comment\n AND /* ? $4 */ c = @P3"
        );
        assert_eq!(count, 3);
    }
}
