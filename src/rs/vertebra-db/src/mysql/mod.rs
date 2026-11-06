//! Native Rust MySQL 4.1+ / 8.0+ / 9.0+ binary wire protocol, `caching_sha2_password`, and SHM pool.

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vertebra_core::broker::{
    LeaseGrant, LeaseTableStats, ShmLeaseTable, ShmStatementCache, StatementCacheStats,
    StatementPlanEntry,
};
use vertebra_core::shm::ShmRegion;

/// MySQL dialect tag for `ShmStatementCache`.
pub const MYSQL_DIALECT_TAG: u8 = 2;

/// MySQL command byte for `COM_QUERY` (`0x03`).
pub const COM_QUERY: u8 = 0x03;

/// MySQL command byte for `COM_STMT_PREPARE` (`0x16`).
pub const COM_STMT_PREPARE: u8 = 0x16;

/// MySQL command byte for `COM_STMT_EXECUTE` (`0x17`).
pub const COM_STMT_EXECUTE: u8 = 0x17;

/// MySQL command byte for `COM_STMT_CLOSE` (`0x19`).
pub const COM_STMT_CLOSE: u8 = 0x19;

/// Encode a MySQL 4.1+ wire packet with a 3-byte little-endian payload length and 1-byte `seq_id`.
pub fn encode_mysql_packet(seq_id: u8, payload: &[u8]) -> Bytes {
    let len = payload.len();
    let mut buf = BytesMut::with_capacity(4 + len);
    buf.put_u8((len & 0xff) as u8);
    buf.put_u8(((len >> 8) & 0xff) as u8);
    buf.put_u8(((len >> 16) & 0xff) as u8);
    buf.put_u8(seq_id);
    buf.extend_from_slice(payload);
    buf.freeze()
}

/// Decode a MySQL 4.1+ wire packet, returning `(seq_id, payload, total_consumed_bytes)`.
pub fn decode_mysql_packet(buf: &[u8]) -> Result<(u8, &[u8], usize), String> {
    if buf.len() < 4 {
        return Err("buffer too short for 4-byte MySQL packet header".to_string());
    }
    let payload_len = (buf[0] as usize) | ((buf[1] as usize) << 8) | ((buf[2] as usize) << 16);
    let seq_id = buf[3];
    let total_len = 4 + payload_len;
    if buf.len() < total_len {
        return Err(format!(
            "truncated MySQL packet: expected {payload_len} payload bytes, got {}",
            buf.len() - 4
        ));
    }
    Ok((seq_id, &buf[4..total_len], total_len))
}

/// Compute the MySQL 8.0+ `caching_sha2_password` scramble:
/// `XOR(SHA256(password), SHA256(SHA256(SHA256(password)) || nonce))`.
pub fn compute_caching_sha2_scramble(password: &[u8], nonce: &[u8]) -> [u8; 32] {
    if password.is_empty() {
        return [0u8; 32];
    }
    let digest1 = Sha256::digest(password);
    let digest2 = Sha256::digest(digest1);

    let mut stage3_hasher = Sha256::new();
    stage3_hasher.update(digest2);
    stage3_hasher.update(nonce);
    let digest3 = stage3_hasher.finalize();

    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = digest1[i] ^ digest3[i];
    }
    out
}

/// Encode a MySQL `COM_QUERY` (`0x03`) packet with `seq_id = 0`.
pub fn encode_com_query(sql: &str) -> Bytes {
    let mut payload = BytesMut::with_capacity(1 + sql.len());
    payload.put_u8(COM_QUERY);
    payload.extend_from_slice(sql.as_bytes());
    encode_mysql_packet(0, &payload)
}

/// Encode a MySQL `COM_STMT_PREPARE` (`0x16`) packet with `seq_id = 0`.
pub fn encode_com_stmt_prepare(sql: &str) -> Bytes {
    let mut payload = BytesMut::with_capacity(1 + sql.len());
    payload.put_u8(COM_STMT_PREPARE);
    payload.extend_from_slice(sql.as_bytes());
    encode_mysql_packet(0, &payload)
}

/// Encode a MySQL `COM_STMT_CLOSE` (`0x19`) packet with `seq_id = 0` and 4-byte little-endian `stmt_id`.
pub fn encode_com_stmt_close(stmt_id: u32) -> Bytes {
    let mut payload = [0u8; 5];
    payload[0] = COM_STMT_CLOSE;
    payload[1..5].copy_from_slice(&stmt_id.to_le_bytes());
    encode_mysql_packet(0, &payload)
}

/// Configuration for `MysqlPool`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlPoolConfig {
    pub dsn: String,
    pub pool_size: u32,
    pub max_overflow: u32,
    pub statement_cache_capacity: u32,
    pub cloud_sql_instance: Option<String>,
}

impl Default for MysqlPoolConfig {
    fn default() -> Self {
        Self {
            dsn: "mysql://localhost:3306/mysql".to_string(),
            pool_size: 20,
            max_overflow: 10,
            statement_cache_capacity: 256,
            cloud_sql_instance: None,
        }
    }
}

/// Shared-memory coordinated MySQL connection pool and binary prepared statement cache.
#[derive(Clone)]
pub struct MysqlPool {
    pub config: MysqlPoolConfig,
    lease_table: Arc<ShmLeaseTable>,
    stmt_cache: Arc<ShmStatementCache>,
}

impl MysqlPool {
    /// Initialize a new SHM-backed `MysqlPool`.
    pub fn new(config: MysqlPoolConfig) -> Result<Self, String> {
        let lease_size = ShmLeaseTable::required_size(config.pool_size, config.max_overflow);
        let lease_region = ShmRegion::create(lease_size)
            .map_err(|e| format!("failed to allocate MySQL lease ShmRegion: {e}"))?;
        let lease_table =
            ShmLeaseTable::init_in_region(lease_region, config.pool_size, config.max_overflow)
                .map_err(|e| format!("failed to initialize MySQL ShmLeaseTable: {e}"))?;

        let cache_cap = config.statement_cache_capacity.max(4);
        let cache_size = ShmStatementCache::required_size(cache_cap);
        let cache_region = ShmRegion::create(cache_size)
            .map_err(|e| format!("failed to allocate MySQL stmt cache ShmRegion: {e}"))?;
        let stmt_cache = ShmStatementCache::init_in_region(cache_region, cache_cap)
            .map_err(|e| format!("failed to initialize MySQL ShmStatementCache: {e}"))?;

        Ok(Self {
            config,
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

    /// Look up or insert a binary prepared statement plan in the shared-memory statement cache.
    pub fn lookup_or_insert_statement(
        &self,
        sql: &str,
        stmt_id: u32,
        param_count: u16,
    ) -> Option<StatementPlanEntry> {
        if let Some(entry) = self.stmt_cache.lookup(MYSQL_DIALECT_TAG, sql.as_bytes()) {
            return Some(entry);
        }
        let id_bytes = stmt_id.to_le_bytes();
        self.stmt_cache.insert(
            MYSQL_DIALECT_TAG,
            sql.as_bytes(),
            param_count,
            sql.as_bytes(),
            &id_bytes,
        );
        self.stmt_cache.lookup(MYSQL_DIALECT_TAG, sql.as_bytes())
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
    fn test_mysql_packet_framing_and_commands() {
        let pkt = encode_mysql_packet(3, b"SELECT 1");
        let (seq, payload, consumed) = decode_mysql_packet(&pkt).unwrap();
        assert_eq!(seq, 3);
        assert_eq!(payload, b"SELECT 1");
        assert_eq!(consumed, pkt.len());

        let q = encode_com_query("SELECT 42");
        let (q_seq, q_payload, _) = decode_mysql_packet(&q).unwrap();
        assert_eq!(q_seq, 0);
        assert_eq!(q_payload[0], COM_QUERY);
        assert_eq!(&q_payload[1..], b"SELECT 42");

        let prep = encode_com_stmt_prepare("SELECT * FROM items WHERE id = ?");
        let (_, prep_payload, _) = decode_mysql_packet(&prep).unwrap();
        assert_eq!(prep_payload[0], COM_STMT_PREPARE);

        let close = encode_com_stmt_close(77);
        let (_, close_payload, _) = decode_mysql_packet(&close).unwrap();
        assert_eq!(close_payload[0], COM_STMT_CLOSE);
        assert_eq!(
            u32::from_le_bytes([
                close_payload[1],
                close_payload[2],
                close_payload[3],
                close_payload[4]
            ]),
            77
        );
    }

    #[test]
    fn test_caching_sha2_scramble() {
        let empty = compute_caching_sha2_scramble(b"", b"01234567890123456789");
        assert_eq!(empty, [0u8; 32]);

        let scramble =
            compute_caching_sha2_scramble(b"correct-horse-battery", b"12345678901234567890");
        assert_ne!(scramble, [0u8; 32]);
        assert_eq!(
            scramble,
            compute_caching_sha2_scramble(b"correct-horse-battery", b"12345678901234567890")
        );
    }

    #[test]
    fn test_mysql_pool_shm_leasing() {
        let pool = MysqlPool::new(MysqlPoolConfig::default()).unwrap();
        let grant = pool
            .acquire_lease(999, 1_000_000_000, Duration::from_millis(50))
            .unwrap();
        let plan = pool
            .lookup_or_insert_statement("SELECT * FROM orders WHERE id = ?", 12, 1)
            .unwrap();
        assert_eq!(plan.param_count, 1);
        assert_eq!(plan.param_plan, 12u32.to_le_bytes());
        pool.release_lease(grant.slot_index, grant.generation)
            .unwrap();
    }
}
