//! Pure-Rust Oracle Thin TNS/TTC wire protocol, 26ai `VECTOR`/`OSON` binary codecs, and SHM pool.

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use vertebra_core::broker::{
    LeaseGrant, LeaseTableStats, ShmLeaseTable, ShmStatementCache, StatementCacheStats,
    StatementPlanEntry,
};
use vertebra_core::shm::ShmRegion;

/// Oracle dialect tag for `ShmStatementCache`.
pub const ORACLE_DIALECT_TAG: u8 = 4;

/// Oracle 26ai binary `OSON` magic header (`\xffJZ`).
pub const OSON_MAGIC: &[u8; 3] = b"\xff\x4a\x5a";

/// Oracle 26ai binary `VECTOR` magic byte (`0xDB`).
pub const ORACLE_VECTOR_MAGIC: u8 = 0xDB;

/// Oracle 26ai binary `VECTOR` wire version (`0x01`).
pub const ORACLE_VECTOR_VERSION: u8 = 0x01;

/// Oracle TNS wire packet type codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum TnsPacketType {
    Connect = 1,
    Accept = 2,
    Ack = 3,
    Refuse = 4,
    Redirect = 5,
    Data = 6,
    Marker = 12,
}

impl TnsPacketType {
    /// Convert a raw TNS packet type byte into `TnsPacketType`.
    pub const fn from_u8(val: u8) -> Option<Self> {
        match val {
            1 => Some(Self::Connect),
            2 => Some(Self::Accept),
            3 => Some(Self::Ack),
            4 => Some(Self::Refuse),
            5 => Some(Self::Redirect),
            6 => Some(Self::Data),
            12 => Some(Self::Marker),
            _ => None,
        }
    }
}

/// Encode an Oracle TNS `Connect` (`type = 1`) packet wrapping a `(DESCRIPTION=...)` connect descriptor.
pub fn encode_tns_connect_packet(descriptor: &str) -> Bytes {
    let desc_bytes = descriptor.as_bytes();
    let connect_header_len = 58usize;
    let total_len = connect_header_len + desc_bytes.len();
    let mut buf = BytesMut::with_capacity(total_len);

    // 8-byte TNS packet header
    buf.put_u16(total_len as u16);
    buf.put_u16(0); // packet checksum
    buf.put_u8(TnsPacketType::Connect as u8);
    buf.put_u8(0); // reserved
    buf.put_u16(0); // header checksum

    // TNS Connect body (bytes 8..58)
    buf.put_u16(318); // TNS version (19c/23ai/26ai)
    buf.put_u16(300); // minimum compatible version
    buf.put_u16(0); // service options
    buf.put_u16(8192); // session data unit (SDU)
    buf.put_u16(65535); // maximum transmission data unit (TDU)
    buf.put_u16(0x4f98); // NT protocol characteristics
    buf.put_u16(0); // line turnaround
    buf.put_u16(1); // value of 1 in hardware
    buf.put_u16(desc_bytes.len() as u16); // connect data length
    buf.put_u16(connect_header_len as u16); // offset to connect data
    buf.put_u32(0); // max receivable connect data
    buf.put_u8(0x41); // connect flags 0
    buf.put_u8(0x41); // connect flags 1
    // Fill remaining 24 bytes of the 58-byte connect header with zeros
    buf.put_bytes(0, 24);

    buf.extend_from_slice(desc_bytes);
    buf.freeze()
}

/// Encode an Oracle TNS `Data` (`type = 6`) packet with 2-byte big-endian `data_flags`.
pub fn encode_tns_data_packet(data_flags: u16, payload: &[u8]) -> Bytes {
    let total_len = 10 + payload.len();
    let mut buf = BytesMut::with_capacity(total_len);
    buf.put_u16(total_len as u16);
    buf.put_u16(0);
    buf.put_u8(TnsPacketType::Data as u8);
    buf.put_u8(0);
    buf.put_u16(0);
    buf.put_u16(data_flags);
    buf.extend_from_slice(payload);
    buf.freeze()
}

/// Decode an Oracle TNS packet, returning `(packet_type, data_flags, payload_slice)`.
pub fn decode_tns_packet(buf: &[u8]) -> Result<(TnsPacketType, u16, &[u8]), String> {
    if buf.len() < 8 {
        return Err("buffer too short for 8-byte TNS packet header".to_string());
    }
    let total_len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if total_len < 8 {
        return Err(format!("invalid TNS packet length {total_len} (< 8)"));
    }
    if buf.len() < total_len {
        return Err(format!(
            "truncated TNS packet: header specifies {total_len} bytes, got {}",
            buf.len()
        ));
    }
    let pkt_type = TnsPacketType::from_u8(buf[4])
        .ok_or_else(|| format!("unknown TNS packet type byte: {}", buf[4]))?;

    if pkt_type == TnsPacketType::Data {
        if total_len < 10 {
            return Err("TNS Data packet too short for 2-byte data flags".to_string());
        }
        let flags = u16::from_be_bytes([buf[8], buf[9]]);
        Ok((pkt_type, flags, &buf[10..total_len]))
    } else if pkt_type == TnsPacketType::Connect && total_len >= 58 {
        let offset = u16::from_be_bytes([buf[26], buf[27]]) as usize;
        if offset <= total_len {
            Ok((pkt_type, 0, &buf[offset..total_len]))
        } else {
            Ok((pkt_type, 0, &buf[8..total_len]))
        }
    } else {
        Ok((pkt_type, 0, &buf[8..total_len]))
    }
}

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

/// Shared-memory coordinated Oracle Thin connection pool and statement cache.
#[derive(Clone)]
pub struct OraclePool {
    pub descriptor: String,
    lease_table: Arc<ShmLeaseTable>,
    stmt_cache: Arc<ShmStatementCache>,
}

impl OraclePool {
    /// Initialize a new SHM-backed `OraclePool`.
    pub fn new(
        descriptor: String,
        pool_size: u32,
        max_overflow: u32,
        stmt_cache_capacity: u32,
    ) -> Result<Self, String> {
        let lease_size = ShmLeaseTable::required_size(pool_size, max_overflow);
        let lease_region = ShmRegion::create(lease_size)
            .map_err(|e| format!("failed to allocate Oracle lease ShmRegion: {e}"))?;
        let lease_table = ShmLeaseTable::init_in_region(lease_region, pool_size, max_overflow)
            .map_err(|e| format!("failed to init Oracle ShmLeaseTable: {e}"))?;

        let cap = stmt_cache_capacity.max(4);
        let cache_size = ShmStatementCache::required_size(cap);
        let cache_region = ShmRegion::create(cache_size)
            .map_err(|e| format!("failed to allocate Oracle stmt cache ShmRegion: {e}"))?;
        let stmt_cache = ShmStatementCache::init_in_region(cache_region, cap)
            .map_err(|e| format!("failed to init Oracle ShmStatementCache: {e}"))?;

        Ok(Self {
            descriptor,
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

    /// Look up or insert a statement cursor plan in the shared-memory statement cache.
    pub fn lookup_or_insert_statement(
        &self,
        sql: &str,
        cursor_id: u16,
        param_count: u16,
    ) -> Option<StatementPlanEntry> {
        if let Some(entry) = self.stmt_cache.lookup(ORACLE_DIALECT_TAG, sql.as_bytes()) {
            return Some(entry);
        }
        let cursor_bytes = cursor_id.to_be_bytes();
        self.stmt_cache.insert(
            ORACLE_DIALECT_TAG,
            sql.as_bytes(),
            param_count,
            sql.as_bytes(),
            &cursor_bytes,
        );
        self.stmt_cache.lookup(ORACLE_DIALECT_TAG, sql.as_bytes())
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
    fn test_tns_connect_and_data_framing() {
        let desc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))";
        let connect_pkt = encode_tns_connect_packet(desc);
        let (pkt_type, flags, payload) = decode_tns_packet(&connect_pkt).unwrap();
        assert_eq!(pkt_type, TnsPacketType::Connect);
        assert_eq!(flags, 0);
        assert_eq!(payload, desc.as_bytes());

        let data_pkt = encode_tns_data_packet(0x0040, b"TTC_PAYLOAD");
        let (d_type, d_flags, d_payload) = decode_tns_packet(&data_pkt).unwrap();
        assert_eq!(d_type, TnsPacketType::Data);
        assert_eq!(d_flags, 0x0040);
        assert_eq!(d_payload, b"TTC_PAYLOAD");
    }

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
