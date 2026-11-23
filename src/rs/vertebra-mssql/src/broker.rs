//! Shared-memory connection lease broker and seqlock statement plan cache for `vertebra-mssql`.
//!
//! Coordinates cross-worker connection slot leases (`ShmLeaseTable`) and caches `@P1`-rewritten
//! SQL plans in `ShmStatementCache` under [`DIALECT_TAG_MSSQL`] (`5`).

use std::sync::Arc;
use std::time::Duration;

pub use vertebra_core::broker::DIALECT_TAG_MSSQL;
use vertebra_core::broker::{
    LeaseGrant, LeaseTableStats, ShmLeaseTable, ShmStatementCache, StatementCacheStats,
};
use vertebra_core::dbrpc::DbRpcOp;
use vertebra_core::shm::ShmRegion;
use vertebra_core::text::classify_mssql_statement;
use vertebra_db::py_async::block_on_db;
use vertebra_db::py_broker::BrokerPoolRegistry;
use vertebra_db::{
    BrokerEngineBackend, BrokerPoolBackend, DbRpcEncodedResponse, DbRpcScalar,
    decode_dbrpc_request_frame, encode_dbrpc_response_frame, rewrite_mssql_placeholders,
};

use crate::client::MssqlClient;
use crate::codecs::{MssqlCellValue, format_numeric};
use crate::config::MssqlClientConfig;
use crate::errors::MssqlError;
use crate::params::MssqlParam;

/// Shared-memory broker backend managing MSSQL connection leases, `@P1` statement plans, and
/// broker-hosted TDS connection pools.
#[derive(Clone)]
pub struct MssqlBrokerBackend {
    lease_table: Option<Arc<ShmLeaseTable>>,
    stmt_cache: Option<Arc<ShmStatementCache>>,
    pools: Arc<BrokerPoolRegistry<MssqlClient>>,
}

impl MssqlBrokerBackend {
    /// Initialize a new in-process/shared-memory [`MssqlBrokerBackend`].
    pub fn new(pool_size: u32, max_overflow: u32, stmt_capacity: u32) -> Result<Self, MssqlError> {
        let eff_pool = pool_size.max(1);
        let req_size =
            ShmLeaseTable::required_size_with_statements(eff_pool, max_overflow, stmt_capacity);
        let region = ShmRegion::create(req_size).map_err(|e| {
            MssqlError::config(format!("failed to allocate MSSQL broker ShmRegion: {e}"))
        })?;
        let table = ShmLeaseTable::init_in_region_with_statements(
            region,
            eff_pool,
            max_overflow,
            stmt_capacity,
        )?;
        let cache = table.statement_cache().map(Arc::new);
        Ok(Self {
            lease_table: Some(Arc::new(table)),
            stmt_cache: cache,
            pools: Arc::new(BrokerPoolRegistry::new()),
        })
    }

    /// Construct a lightweight capsule backend that manages broker-hosted TDS pools lazily without
    /// eagerly allocating a client-side `ShmLeaseTable`.
    #[must_use]
    pub fn new_capsule_backend() -> Self {
        Self {
            lease_table: None,
            stmt_cache: None,
            pools: Arc::new(BrokerPoolRegistry::new()),
        }
    }

    fn require_lease_table(&self) -> Result<&Arc<ShmLeaseTable>, MssqlError> {
        self.lease_table.as_ref().ok_or_else(|| {
            MssqlError::config("MssqlBrokerBackend capsule instance has no client ShmLeaseTable")
        })
    }

    /// Acquire a connection lease from the shared-memory broker table.
    pub fn acquire_lease(
        &self,
        worker_pid: u32,
        ttl_ns: u64,
        timeout: Duration,
    ) -> Result<LeaseGrant, MssqlError> {
        self.require_lease_table()?
            .acquire(worker_pid, ttl_ns, timeout, u64::from(DIALECT_TAG_MSSQL))
            .map_err(MssqlError::from)
    }

    /// Release a previously acquired connection lease.
    pub fn release_lease(&self, grant: LeaseGrant) -> Result<(), MssqlError> {
        self.require_lease_table()?
            .release(grant.slot_index, grant.generation)
            .map_err(MssqlError::from)
    }

    /// Look up or compile a SQL statement into `@P1` form, storing it in the seqlock statement cache
    /// under [`DIALECT_TAG_MSSQL`] when it fits within a 224-byte slot.
    pub fn resolve_or_rewrite_sql(&self, raw_sql: &str) -> (String, usize) {
        let dialect = DIALECT_TAG_MSSQL as u8;
        if let Some(ref cache) = self.stmt_cache
            && let Some(entry) = cache.lookup(dialect, raw_sql.as_bytes())
            && let Ok(compiled) = String::from_utf8(entry.compiled_sql)
        {
            return (compiled, usize::from(entry.param_count));
        }

        let (owned, count) = rewrite_mssql_placeholders(raw_sql);
        if let Some(ref cache) = self.stmt_cache
            && let Ok(param_count_u16) = u16::try_from(count)
        {
            let _ = cache.insert(
                dialect,
                raw_sql.as_bytes(),
                param_count_u16,
                owned.as_bytes(),
                &[],
            );
        }
        (owned, count)
    }

    /// Return current lease table telemetry counters.
    pub fn lease_stats(&self) -> LeaseTableStats {
        match self.lease_table.as_ref() {
            Some(table) => table.stats(),
            None => LeaseTableStats {
                pool_size: 0,
                max_overflow: 0,
                active_leases: 0,
                primary_in_use: 0,
                overflow_in_use: 0,
                waiting_threads: 0,
                quarantined: 0,
                total_acquired: 0,
                total_released: 0,
                total_expired: 0,
                total_timeouts: 0,
                total_reclaimed: 0,
                total_quarantined: 0,
                total_disposed: 0,
            },
        }
    }

    /// Return current statement cache telemetry counters, if statement caching is enabled.
    pub fn statement_cache_stats(&self) -> Option<StatementCacheStats> {
        self.stmt_cache.as_ref().map(|c| c.stats())
    }

    /// Return the broker dialect tag (`DIALECT_TAG_MSSQL = 5`).
    pub const fn dialect_tag(&self) -> u16 {
        DIALECT_TAG_MSSQL
    }

    /// Return the broker dialect name (`"mssql"`).
    pub const fn dialect(&self) -> &'static str {
        "mssql"
    }
}

fn scalar_to_mssql_param(scalar: &DbRpcScalar) -> MssqlParam {
    match scalar {
        DbRpcScalar::Null => MssqlParam::null(),
        DbRpcScalar::Bool(b) => MssqlParam::Bool(*b),
        DbRpcScalar::Int(v) => MssqlParam::I64(*v),
        DbRpcScalar::UInt(v) => match i64::try_from(*v) {
            Ok(i) => MssqlParam::I64(i),
            Err(_) => MssqlParam::String(v.to_string()),
        },
        DbRpcScalar::Float(v) => MssqlParam::F64(*v),
        DbRpcScalar::Str(s) => MssqlParam::String(s.clone()),
        DbRpcScalar::Bytes(b) => MssqlParam::Bytes(b.clone()),
    }
}

fn mssql_cell_to_scalar(cell: &MssqlCellValue) -> DbRpcScalar {
    match cell {
        MssqlCellValue::Null => DbRpcScalar::Null,
        MssqlCellValue::Bool(b) => DbRpcScalar::Bool(*b),
        MssqlCellValue::Int64(v) => DbRpcScalar::Int(*v),
        MssqlCellValue::Float64(v) => DbRpcScalar::Float(*v),
        MssqlCellValue::String(s) | MssqlCellValue::Xml(s) => DbRpcScalar::Str(s.clone()),
        MssqlCellValue::Bytes(b) => DbRpcScalar::Bytes(b.clone()),
        MssqlCellValue::Decimal(d) => DbRpcScalar::Str(format_numeric(*d)),
        MssqlCellValue::Uuid(u) => DbRpcScalar::Str(u.to_string()),
        MssqlCellValue::Date(d) => DbRpcScalar::Str(d.to_string()),
        MssqlCellValue::Time(t) => DbRpcScalar::Str(t.to_string()),
        MssqlCellValue::DateTime(dt) => DbRpcScalar::Str(dt.to_string()),
        MssqlCellValue::DateTimeOffset(dto) => DbRpcScalar::Str(dto.to_rfc3339()),
    }
}

fn mssql_sql_returns_rows(sql: &str) -> bool {
    classify_mssql_statement(sql).0
}

fn encode_mssql_err(request_id: u64, err: &MssqlError) -> Vec<u8> {
    encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
        request_id,
        "MssqlError",
        err.to_string(),
    ))
}

impl BrokerEngineBackend for MssqlBrokerBackend {
    fn dialect_tag(&self) -> u16 {
        DIALECT_TAG_MSSQL
    }

    fn dialect_name(&self) -> &'static str {
        "mssql"
    }

    fn normalize_statement(&self, sql: &str) -> (String, usize) {
        self.resolve_or_rewrite_sql(sql)
    }
}

impl BrokerPoolBackend for MssqlBrokerBackend {
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String> {
        if max_size == 0 {
            return Err("MSSQL pool max_size must be >= 1".to_string());
        }
        let mut cfg = MssqlClientConfig::from_dsn(dsn).map_err(|e| e.to_string())?;
        cfg.pool_size = max_size;
        let client = Arc::new(MssqlClient::new(cfg).map_err(|e| e.to_string())?);
        Ok(self.pools.register(client))
    }

    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String> {
        let client = self
            .pools
            .get(pool_id)
            .ok_or_else(|| format!("unknown pool id {pool_id}"))?;

        let req = decode_dbrpc_request_frame(frame)?;
        let req_id = req.request_id;
        match req.op {
            DbRpcOp::Execute => {
                let raw_sql = req
                    .sql
                    .ok_or_else(|| "DBRPC Execute frame missing SQL".to_string())?;
                let params: Vec<MssqlParam> =
                    req.params.iter().map(scalar_to_mssql_param).collect();
                if mssql_sql_returns_rows(&raw_sql) {
                    match block_on_db(async move { client.query(raw_sql, params).await }) {
                        Ok(res) => {
                            let cols = res.columns.into_iter().map(|c| c.name).collect();
                            let rows = res
                                .rows
                                .iter()
                                .map(|r| r.iter().map(mssql_cell_to_scalar).collect())
                                .collect();
                            Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::ok_rows(
                                req_id,
                                i64::try_from(res.rows_affected).unwrap_or(i64::MAX),
                                cols,
                                rows,
                            )))
                        }
                        Err(err) => Ok(encode_mssql_err(req_id, &err)),
                    }
                } else {
                    match block_on_db(async move { client.execute(raw_sql, params).await }) {
                        Ok(affected) => Ok(encode_dbrpc_response_frame(
                            &DbRpcEncodedResponse::ok_count(
                                req_id,
                                i64::try_from(affected).unwrap_or(i64::MAX),
                                None,
                            ),
                        )),
                        Err(err) => Ok(encode_mssql_err(req_id, &err)),
                    }
                }
            }
            DbRpcOp::Begin => {
                match block_on_db(async move { client.begin_transaction(None).await }) {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_mssql_err(req_id, &err)),
                }
            }
            DbRpcOp::Commit => {
                match block_on_db(async move { client.commit_transaction().await }) {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_mssql_err(req_id, &err)),
                }
            }
            DbRpcOp::Rollback => {
                let res = if let Some(sp) = req.savepoint {
                    block_on_db(async move { client.rollback_to_savepoint(sp).await })
                } else {
                    block_on_db(async move { client.rollback_transaction().await })
                };
                match res {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_mssql_err(req_id, &err)),
                }
            }
            DbRpcOp::Savepoint => {
                let sp = req
                    .savepoint
                    .ok_or_else(|| "DBRPC Savepoint frame missing savepoint name".to_string())?;
                match block_on_db(async move { client.savepoint(sp).await }) {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_mssql_err(req_id, &err)),
                }
            }
            DbRpcOp::Cancel => match block_on_db(async move { client.cancel().await }) {
                Ok(()) => Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                )),
                Err(err) => Ok(encode_mssql_err(req_id, &err)),
            },
            DbRpcOp::Close => {
                self.close_pool(pool_id);
                Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                ))
            }
            other => Err(format!(
                "unsupported DBRPC op {other:?} in MSSQL broker pool"
            )),
        }
    }

    fn close_pool(&self, pool_id: u64) {
        if let Some(client) = self.pools.remove(pool_id) {
            let _ = block_on_db(async move { client.close().await });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_backend_caches_rewritten_mssql_statements_and_tracks_leases() {
        let broker = MssqlBrokerBackend::new(2, 1, 16).expect("create broker");
        let (sql1, count1) = broker
            .resolve_or_rewrite_sql("SELECT id, name FROM users WHERE id = $1 AND active = ?");
        assert_eq!(
            sql1,
            "SELECT id, name FROM users WHERE id = @P1 AND active = @P2"
        );
        assert_eq!(count1, 2);

        let (sql2, count2) = broker
            .resolve_or_rewrite_sql("SELECT id, name FROM users WHERE id = $1 AND active = ?");
        assert_eq!(sql2, sql1);
        assert_eq!(count2, 2);

        let cache_stats = broker.statement_cache_stats().expect("cache stats");
        assert_eq!(cache_stats.hits, 1);
        assert_eq!(cache_stats.inserts, 1);

        let grant = broker
            .acquire_lease(std::process::id(), 5_000_000_000, Duration::from_millis(50))
            .expect("acquire lease");
        assert_eq!(broker.lease_stats().active_leases, 1);
        broker.release_lease(grant).expect("release lease");
        assert_eq!(broker.lease_stats().active_leases, 0);
    }

    #[test]
    fn mssql_sql_returns_rows_handles_comments_quotes_and_output() {
        assert!(mssql_sql_returns_rows("/* lead */ SELECT 1"));
        assert!(mssql_sql_returns_rows(
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ));
        assert!(mssql_sql_returns_rows(
            "INSERT INTO t OUTPUT INSERTED.id VALUES (1)"
        ));
        assert!(!mssql_sql_returns_rows("INSERT INTO t VALUES (' OUTPUT ')"));
        assert!(!mssql_sql_returns_rows(
            "UPDATE t SET t.output = 1 WHERE t.output = 2"
        ));
    }
}
