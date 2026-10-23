//! Native Rust PostgreSQL v3 wire protocol, binary `JSONB`, `PGCOPY` framing, and SHM pool manager.

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use vertebra_core::broker::{
    LeaseGrant, LeaseTableStats, ShmLeaseTable, ShmStatementCache, StatementCacheStats,
    StatementPlanEntry,
};
use vertebra_core::shm::ShmRegion;

/// Canonical 11-byte PostgreSQL binary `COPY` stream header signature.
pub const PGCOPY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

/// PostgreSQL dialect tag for `ShmStatementCache`.
pub const POSTGRES_DIALECT_TAG: u8 = 1;

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

/// Configuration for `PostgresPool`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresPoolConfig {
    pub dsn: String,
    pub direct_dsn: Option<String>,
    pub pool_size: u32,
    pub max_overflow: u32,
    pub pooler_mode: PoolerMode,
    pub statement_cache_capacity: u32,
}

impl Default for PostgresPoolConfig {
    fn default() -> Self {
        Self {
            dsn: "postgresql://localhost:5432/postgres".to_string(),
            direct_dsn: None,
            pool_size: 20,
            max_overflow: 10,
            pooler_mode: PoolerMode::Direct,
            statement_cache_capacity: 256,
        }
    }
}

/// Shared-memory coordinated PostgreSQL connection pool and statement cache.
#[derive(Clone)]
pub struct PostgresPool {
    pub config: PostgresPoolConfig,
    lease_table: Arc<ShmLeaseTable>,
    stmt_cache: Option<Arc<ShmStatementCache>>,
}

impl PostgresPool {
    /// Initialize a new SHM-backed `PostgresPool`.
    pub fn new(config: PostgresPoolConfig) -> Result<Self, String> {
        let lease_size = ShmLeaseTable::required_size(config.pool_size, config.max_overflow);
        let lease_region = ShmRegion::create(lease_size)
            .map_err(|e| format!("failed to allocate Postgres lease ShmRegion: {e}"))?;
        let lease_table =
            ShmLeaseTable::init_in_region(lease_region, config.pool_size, config.max_overflow)
                .map_err(|e| format!("failed to initialize Postgres ShmLeaseTable: {e}"))?;

        let stmt_cache = if config.pooler_mode.supports_protocol_prepared_statements()
            && config.statement_cache_capacity > 0
        {
            let cache_size = ShmStatementCache::required_size(config.statement_cache_capacity);
            let cache_region = ShmRegion::create(cache_size)
                .map_err(|e| format!("failed to allocate Postgres stmt cache ShmRegion: {e}"))?;
            let cache =
                ShmStatementCache::init_in_region(cache_region, config.statement_cache_capacity)
                    .map_err(|e| format!("failed to initialize Postgres ShmStatementCache: {e}"))?;
            Some(Arc::new(cache))
        } else {
            None
        };

        Ok(Self {
            config,
            lease_table: Arc::new(lease_table),
            stmt_cache,
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

    /// Look up or insert a prepared statement plan in the shared-memory statement cache.
    pub fn lookup_or_insert_statement(
        &self,
        sql: &str,
        compiled_sql: &[u8],
        param_count: u16,
    ) -> Option<StatementPlanEntry> {
        let cache = self.stmt_cache.as_ref()?;
        if let Some(entry) = cache.lookup(POSTGRES_DIALECT_TAG, sql.as_bytes()) {
            return Some(entry);
        }
        cache.insert(
            POSTGRES_DIALECT_TAG,
            sql.as_bytes(),
            param_count,
            compiled_sql,
            &[],
        );
        cache.lookup(POSTGRES_DIALECT_TAG, sql.as_bytes())
    }

    /// Return current lease table and statement cache telemetry.
    pub fn stats(&self) -> (LeaseTableStats, Option<StatementCacheStats>) {
        (
            self.lease_table.stats(),
            self.stmt_cache.as_ref().map(|c| c.stats()),
        )
    }
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
    fn test_postgres_pool_shm_integration() {
        let pool = PostgresPool::new(PostgresPoolConfig::default()).unwrap();
        let grant = pool
            .acquire_lease(1234, 5_000_000_000, Duration::from_millis(50))
            .unwrap();
        let plan = pool
            .lookup_or_insert_statement("SELECT * FROM users WHERE id = $1", b"stmt_1", 1)
            .unwrap();
        assert_eq!(plan.param_count, 1);
        assert_eq!(plan.compiled_sql, b"stmt_1");
        pool.release_lease(grant.slot_index, grant.generation)
            .unwrap();
    }
}
