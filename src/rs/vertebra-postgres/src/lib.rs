//! Native Rust PostgreSQL Protocol 3.0 client and pool extension (`vertebra._postgres`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use url::Url;
use vertebra_core::broker::DIALECT_TAG_POSTGRES;
use vertebra_core::dbrpc::DbRpcOp;
use vertebra_db::py_async::block_on_db;
use vertebra_db::{
    BrokerEngineBackend, BrokerPoolBackend, DbRpcEncodedResponse, DbRpcScalar, PlaceholderStyle,
    decode_dbrpc_request_frame, encode_dbrpc_response_frame, new_broker_engine_pycapsule,
    normalize_placeholders,
};

use crate::config::{PostgresConfig, RawPostgresOptions};
use crate::errors::PostgresError;

pub mod arrow;
pub mod async_client;
pub mod auth;
pub mod binding;
pub mod cloud;
pub mod codecs;
pub mod config;
pub mod copy;
pub mod credentials;
pub mod errors;
pub mod extended;
pub mod metadata;
pub mod notifications;
pub mod pipeline;
mod pipeline_guard;
pub mod pool;
pub mod portal;
pub mod protocol;
pub mod python_pool;
pub mod python_values;
pub mod query;
pub mod replication;
pub mod result;
pub mod session;
pub mod sync_client;
pub mod tls;
pub mod transaction;
pub mod types;

pub use crate::async_client::{
    PyAsyncPostgresArrowStream, PyAsyncPostgresCopyIn, PyAsyncPostgresCopyOut,
    PyAsyncPostgresListener, PyAsyncPostgresPortal,
};
pub use crate::replication::PyAsyncPostgresReplication;
pub use crate::session::statements::{
    CachedStatement, PoolerCapabilities, PoolerMode, StatementCache, StatementKey,
};
pub use crate::session::{CommandBudget, CommandDeadlineGuard};
pub use crate::sync_client::{
    PySyncPostgresArrowStream, PySyncPostgresCopyIn, PySyncPostgresCopyOut, PySyncPostgresPortal,
};
pub use codecs::{
    CodecContext, decode_value, decode_value_with_catalog, encode_value, encode_value_with_catalog,
    format_for_oid, format_for_oid_with_catalog,
};
pub use copy::{
    BinaryCopyDecoder, BinaryCopyEncoder, COPY_DATA_CHUNK_BYTES, CopyFromStream, CopyOut,
    DEFAULT_MAX_COPY_EXTENSION_BYTES, DEFAULT_MAX_COPY_FIELD_BYTES, DEFAULT_MAX_COPY_ROW_BYTES,
    MAX_COPY_FAIL_REASON_BYTES, PGCOPY_SIGNATURE,
};
pub use extended::{ExtendedQueryCollector, Parameter, PreparedDescription, encode_bind};
pub use metadata::{
    OwnedTextField, OwnedTextResult, ParameterCandidate, ResolvedParameters,
    normalize_materialized_result, resolve_parameter_metadata,
};
pub use pipeline::{
    PipelineBatch, PipelineFailure, PipelineLimits, PipelineOperation, PipelineOutcome,
    SubmittedPipeline,
};
pub use pool::{PoolStats, PooledConnection, PostgresPool, PostgresPoolOptions};
pub use portal::{ArrowPortalStream, Portal, PortalEvent};
pub use python_pool::{PyAsyncPostgresPool, PySyncPostgresPool};
pub use python_values::{
    extract_parameters, extract_pg_value, extract_pipeline_operations, pg_value_to_python,
    pipeline_outcomes_to_python, query_result_to_python, server_error_to_python,
};
pub use query::SimpleQueryCollector;
pub use result::{
    Column, Format, PgNumeric, PgValue, QueryResult, TemporalKind, TransactionStatus, VectorKind,
};
pub use transaction::{
    TransactionDisposition, TransactionOwner, TransactionScope, TransactionScopeKind,
    TransactionState,
};
pub use types::{CompositeField, TypeCatalog, TypeInfo, TypeKind};

/// Whether this build targets free-threaded CPython (`Py_GIL_DISABLED`).
pub const FREE_THREADED: bool = cfg!(Py_GIL_DISABLED);

/// Whether the `vertebra._postgres` module requires the GIL on import (`false`).
pub const GIL_USED: bool = false;

fn parse_postgres_broker_dsn(dsn: &str) -> Result<PostgresConfig, String> {
    let trimmed = dsn.trim();
    if trimmed.is_empty() || trimmed.contains('\0') {
        return Err("PostgreSQL DSN must not be empty or contain NUL bytes".to_string());
    }
    if trimmed.eq_ignore_ascii_case(":memory:") || trimmed.starts_with("memory://") {
        return Err(
            "PostgreSQL DSN `memory://` is unsupported; configure a real PostgreSQL endpoint"
                .to_string(),
        );
    }

    let Some((scheme, rest)) = trimmed.split_once("://") else {
        return Err(format!(
            "invalid PostgreSQL DSN `{trimmed}`; expected postgresql:// or postgres://"
        ));
    };
    let lower_scheme = scheme.to_ascii_lowercase();
    let rewritten = match lower_scheme.as_str() {
        "postgresql"
        | "postgres"
        | "postgresql+vertebra"
        | "postgres+vertebra"
        | "postgresql+vtb"
        | "postgres+vtb"
        | "postgresql+asyncpg"
        | "postgres+asyncpg"
        | "postgresql+psycopg"
        | "postgres+psycopg" => format!("postgresql://{rest}"),
        other => return Err(format!("unsupported PostgreSQL DSN scheme `{other}://`")),
    };

    let parsed = Url::parse(&rewritten).map_err(|e| format!("invalid PostgreSQL DSN: {e}"))?;
    let user = if parsed.username().is_empty() {
        "postgres".to_string()
    } else {
        parsed.username().to_string()
    };
    let password = parsed.password().map(ToOwned::to_owned);
    let path_db = parsed.path().trim_start_matches('/');
    let database = if path_db.is_empty() {
        "postgres".to_string()
    } else {
        path_db.to_string()
    };

    let mut raw = RawPostgresOptions::new(
        parsed
            .host_str()
            .unwrap_or("localhost")
            .trim_matches(['[', ']']),
        user,
        database,
    );
    raw.password = password;
    raw.port = i64::from(parsed.port().unwrap_or(5432));
    raw.sslmode = "prefer".to_string();

    for (k, v) in parsed.query_pairs() {
        match k.to_ascii_lowercase().as_str() {
            "host" if v.starts_with('/') => {
                raw.unix_socket = Some(v.into_owned());
                raw.sslmode = "disable".to_string();
            }
            "sslmode" if !v.is_empty() => raw.sslmode = v.into_owned(),
            "application_name" if !v.is_empty() => raw.application_name = v.into_owned(),
            "channel_binding" if !v.is_empty() => raw.channel_binding = v.into_owned(),
            _ => {}
        }
    }

    if parsed.host_str().is_none_or(str::is_empty) && raw.unix_socket.is_none() {
        return Err("PostgreSQL DSN is missing a host or unix_socket".to_string());
    }

    PostgresConfig::validate(raw).map_err(|e| e.to_string())
}

fn scalar_to_pg_param(scalar: &DbRpcScalar) -> Parameter {
    match scalar {
        DbRpcScalar::Null => Parameter::new(PgValue::Null),
        DbRpcScalar::Bool(b) => Parameter::new(PgValue::Bool(*b)),
        DbRpcScalar::Int(v) => Parameter::new(PgValue::Int8(*v)),
        DbRpcScalar::UInt(v) => match i64::try_from(*v) {
            Ok(i) => Parameter::new(PgValue::Int8(i)),
            Err(_) => Parameter::new(PgValue::Text(v.to_string())),
        },
        DbRpcScalar::Float(v) => Parameter::new(PgValue::Float8(*v)),
        DbRpcScalar::Str(s) => Parameter::new(PgValue::Text(s.clone())),
        DbRpcScalar::Bytes(b) => Parameter::new(PgValue::Bytes(b.clone())),
    }
}

fn pg_value_to_scalar(val: &PgValue) -> DbRpcScalar {
    match val {
        PgValue::Null => DbRpcScalar::Null,
        PgValue::Bool(b) => DbRpcScalar::Bool(*b),
        PgValue::Int2(v) => DbRpcScalar::Int(i64::from(*v)),
        PgValue::Int4(v) => DbRpcScalar::Int(i64::from(*v)),
        PgValue::Int8(v) => DbRpcScalar::Int(*v),
        PgValue::Float4(v) => DbRpcScalar::Float(f64::from(*v)),
        PgValue::Float8(v) => DbRpcScalar::Float(*v),
        PgValue::Text(s) => DbRpcScalar::Str(s.clone()),
        PgValue::Bytes(b) => DbRpcScalar::Bytes(b.clone()),
        PgValue::Numeric(num) => DbRpcScalar::Str(num.to_canonical_string()),
        PgValue::Json(raw) => DbRpcScalar::Str(String::from_utf8_lossy(raw).into_owned()),
        other => DbRpcScalar::Str(format!("{other:?}")),
    }
}

fn is_valid_savepoint_ident(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

fn encode_postgres_err(request_id: u64, err: &PostgresError) -> Vec<u8> {
    encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
        request_id,
        "PostgresError",
        err.to_string(),
    ))
}

type PinnedPgConn = Arc<tokio::sync::Mutex<Option<(u64, PooledConnection)>>>;

/// Shared-memory broker backend managing broker-hosted `PostgresPool` instances (`DIALECT_TAG_POSTGRES = 1`).
pub struct PostgresBrokerBackend {
    pools: RwLock<HashMap<u64, Arc<PostgresPool>>>,
    pinned: Mutex<HashMap<(u64, u32), PinnedPgConn>>,
    next_pool_id: AtomicU64,
}

impl Default for PostgresBrokerBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl PostgresBrokerBackend {
    /// Construct a new [`PostgresBrokerBackend`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            pinned: Mutex::new(HashMap::new()),
            next_pool_id: AtomicU64::new(1),
        }
    }

    fn has_pool(&self, pool_id: u64) -> bool {
        self.pools
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&pool_id)
    }

    fn get_pinned(&self, pool_id: u64, lease_slot: u32) -> Option<PinnedPgConn> {
        self.pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(pool_id, lease_slot))
            .cloned()
    }

    fn take_pinned(&self, pool_id: u64, lease_slot: u32) -> Option<PinnedPgConn> {
        self.pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&(pool_id, lease_slot))
    }

    fn take_pinned_if_same(
        &self,
        pool_id: u64,
        lease_slot: u32,
        target: &PinnedPgConn,
    ) -> Option<PinnedPgConn> {
        let mut guard = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard
            .get(&(pool_id, lease_slot))
            .is_some_and(|existing| Arc::ptr_eq(existing, target))
        {
            guard.remove(&(pool_id, lease_slot))
        } else {
            None
        }
    }

    fn set_pinned_entry(
        &self,
        pool_id: u64,
        lease_slot: u32,
        entry: PinnedPgConn,
    ) -> Option<PinnedPgConn> {
        self.pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((pool_id, lease_slot), entry)
    }

    fn drain_pool_pinned(&self, pool_id: u64) -> Vec<PinnedPgConn> {
        let mut guard = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let keys: Vec<(u64, u32)> = guard
            .keys()
            .copied()
            .filter(|(pid, _)| *pid == pool_id)
            .collect();
        keys.into_iter().filter_map(|k| guard.remove(&k)).collect()
    }

    async fn release_pinned_entry(entry: Option<PinnedPgConn>) {
        if let Some(pinned_conn) = entry {
            let mut guard = pinned_conn.lock().await;
            if let Some((_, mut conn)) = guard.take() {
                let _ = conn.rollback().await;
                let _ = conn.release().await;
            }
        }
    }
}

impl BrokerEngineBackend for PostgresBrokerBackend {
    fn dialect_tag(&self) -> u16 {
        DIALECT_TAG_POSTGRES
    }

    fn dialect_name(&self) -> &'static str {
        "postgresql"
    }

    fn normalize_statement(&self, sql: &str) -> (String, usize) {
        normalize_placeholders(sql, PlaceholderStyle::DollarNumbered)
    }
}

impl BrokerPoolBackend for PostgresBrokerBackend {
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String> {
        if max_size == 0 {
            return Err("PostgreSQL pool max_size must be >= 1".to_string());
        }
        let cfg = parse_postgres_broker_dsn(dsn)?;
        let pool_max = max_size as usize;
        let opts = PostgresPoolOptions {
            min_size: 1usize.min(pool_max),
            max_size: pool_max,
            ..PostgresPoolOptions::default()
        };
        let pool = PostgresPool::new(cfg, opts).map_err(|e| e.to_string())?;
        let pool_id = self.next_pool_id.fetch_add(1, Ordering::Relaxed);
        let mut guard = self
            .pools
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.insert(pool_id, pool);
        Ok(pool_id)
    }

    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String> {
        let pool = {
            let guard = self
                .pools
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.get(&pool_id).cloned()
        }
        .ok_or_else(|| format!("unknown pool id {pool_id}"))?;

        let req = decode_dbrpc_request_frame(frame)?;
        let req_id = req.request_id;
        match req.op {
            DbRpcOp::Close => {
                self.close_pool(pool_id);
                Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                ))
            }
            DbRpcOp::Execute => {
                let raw_sql = req
                    .sql
                    .as_deref()
                    .ok_or_else(|| "DBRPC Execute frame missing SQL".to_string())?;
                let (norm_sql, _) = self.normalize_statement(raw_sql);
                let params: Vec<Parameter> = req.params.iter().map(scalar_to_pg_param).collect();
                let pinned = self.get_pinned(pool_id, req.lease_slot);
                let outcome: Result<QueryResult, PostgresError> = block_on_db(async move {
                    if let Some(pinned_conn) = pinned {
                        let mut stale_to_evict: Option<PooledConnection> = None;
                        {
                            let mut guard = pinned_conn.lock().await;
                            if let Some((pinned_gen, conn)) = guard.as_mut() {
                                if *pinned_gen == req.lease_generation && req.lease_generation > 0 {
                                    return conn.query(&norm_sql, &params, None).await;
                                }
                                if req.lease_generation > *pinned_gen && *pinned_gen > 0 {
                                    if let Some((_, c)) = guard.take() {
                                        stale_to_evict = Some(c);
                                    }
                                } else if req.lease_generation > 0 {
                                    return Err(PostgresError::Interface(
                                        "stale lease generation for active pinned transaction"
                                            .to_string(),
                                    ));
                                }
                            }
                        }
                        if let Some(mut stale) = stale_to_evict {
                            let _ = self.take_pinned_if_same(pool_id, req.lease_slot, &pinned_conn);
                            let _ = stale.rollback().await;
                            let _ = stale.release().await;
                        }
                    }
                    let mut conn = pool.acquire().await?;
                    let res = conn.query(&norm_sql, &params, None).await;
                    let _ = conn.release().await;
                    res
                });
                match outcome {
                    Ok(qr) => {
                        let resp = if qr.columns.is_empty() {
                            DbRpcEncodedResponse::ok_count(
                                req_id,
                                i64::try_from(qr.affected_rows).unwrap_or(i64::MAX),
                                None,
                            )
                        } else {
                            let cols = qr.columns.into_iter().map(|c| c.name).collect();
                            let rows = qr
                                .rows
                                .iter()
                                .map(|row| row.iter().map(pg_value_to_scalar).collect())
                                .collect();
                            DbRpcEncodedResponse::ok_rows(
                                req_id,
                                i64::try_from(qr.affected_rows).unwrap_or(i64::MAX),
                                cols,
                                rows,
                            )
                        };
                        Ok(encode_dbrpc_response_frame(&resp))
                    }
                    Err(err) => Ok(encode_postgres_err(req_id, &err)),
                }
            }
            DbRpcOp::Begin => {
                if req.lease_generation == 0 {
                    return Err("DBRPC Begin frame requires non-zero lease_generation".to_string());
                }
                let previous = self.take_pinned(pool_id, req.lease_slot);
                let outcome: Result<PooledConnection, PostgresError> = block_on_db(async move {
                    Self::release_pinned_entry(previous).await;
                    let mut conn = pool.acquire().await?;
                    if let Err(err) = conn.begin().await {
                        let _ = conn.release().await;
                        return Err(err);
                    }
                    Ok(conn)
                });
                match outcome {
                    Ok(conn) => {
                        let entry =
                            Arc::new(tokio::sync::Mutex::new(Some((req.lease_generation, conn))));
                        let stale =
                            self.set_pinned_entry(pool_id, req.lease_slot, Arc::clone(&entry));
                        if !self.has_pool(pool_id) {
                            let raced = self.take_pinned_if_same(pool_id, req.lease_slot, &entry);
                            block_on_db(async move {
                                Self::release_pinned_entry(raced).await;
                                Self::release_pinned_entry(stale).await;
                            });
                            return Err(format!("unknown pool id {pool_id}"));
                        }
                        if stale.is_some() {
                            block_on_db(async move {
                                Self::release_pinned_entry(stale).await;
                            });
                        }
                        Ok(encode_dbrpc_response_frame(
                            &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                        ))
                    }
                    Err(err) => Ok(encode_postgres_err(req_id, &err)),
                }
            }
            DbRpcOp::Commit => {
                let pinned = self.get_pinned(pool_id, req.lease_slot);
                let outcome = block_on_db(async move {
                    if let Some(pinned_conn) = pinned {
                        let mut guard = pinned_conn.lock().await;
                        if let Some((pinned_gen, _)) = guard.as_ref() {
                            if *pinned_gen == req.lease_generation && req.lease_generation > 0 {
                                let (_, mut conn) = guard.take().expect("checked");
                                drop(guard);
                                let _ =
                                    self.take_pinned_if_same(pool_id, req.lease_slot, &pinned_conn);
                                let res = conn.commit().await;
                                let _ = conn.release().await;
                                return res;
                            }
                            if req.lease_generation > *pinned_gen && *pinned_gen > 0 {
                                let (_, mut stale) = guard.take().expect("checked");
                                drop(guard);
                                let _ =
                                    self.take_pinned_if_same(pool_id, req.lease_slot, &pinned_conn);
                                let _ = stale.rollback().await;
                                let _ = stale.release().await;
                                return Err(PostgresError::Interface(
                                    "cannot COMMIT: no transaction is in progress for lease generation"
                                        .to_string(),
                                ));
                            }
                            return Err(PostgresError::Interface(
                                "stale lease generation for active pinned transaction".to_string(),
                            ));
                        }
                    }
                    Err(PostgresError::Interface(
                        "cannot COMMIT: no transaction is in progress".to_string(),
                    ))
                });
                match outcome {
                    Ok(_) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_postgres_err(req_id, &err)),
                }
            }
            DbRpcOp::Rollback => {
                let outcome = if let Some(sp) = req.savepoint {
                    if !is_valid_savepoint_ident(&sp) {
                        return Err(format!("invalid savepoint identifier `{sp}`"));
                    }
                    let stmt = format!("ROLLBACK TO SAVEPOINT {sp}");
                    let pinned = self.get_pinned(pool_id, req.lease_slot);
                    block_on_db(async move {
                        if let Some(pinned_conn) = pinned {
                            let mut guard = pinned_conn.lock().await;
                            if let Some((pinned_gen, conn)) = guard.as_mut() {
                                if *pinned_gen == req.lease_generation && req.lease_generation > 0 {
                                    return conn.simple_query(&stmt).await.map(|_| ());
                                }
                                if req.lease_generation > *pinned_gen && *pinned_gen > 0 {
                                    let (_, mut stale) = guard.take().expect("checked");
                                    drop(guard);
                                    let _ = self.take_pinned_if_same(
                                        pool_id,
                                        req.lease_slot,
                                        &pinned_conn,
                                    );
                                    let _ = stale.rollback().await;
                                    let _ = stale.release().await;
                                    return Err(PostgresError::Interface(
                                        "cannot ROLLBACK TO SAVEPOINT: no transaction is in progress for lease generation"
                                            .to_string(),
                                    ));
                                }
                                return Err(PostgresError::Interface(
                                    "stale lease generation for active pinned transaction"
                                        .to_string(),
                                ));
                            }
                        }
                        Err(PostgresError::Interface(
                            "cannot ROLLBACK TO SAVEPOINT: no transaction is in progress"
                                .to_string(),
                        ))
                    })
                } else {
                    let pinned = self.get_pinned(pool_id, req.lease_slot);
                    block_on_db(async move {
                        if let Some(pinned_conn) = pinned {
                            let mut guard = pinned_conn.lock().await;
                            if let Some((pinned_gen, _)) = guard.as_ref() {
                                if *pinned_gen > 0 && req.lease_generation >= *pinned_gen {
                                    let (_, mut conn) = guard.take().expect("checked");
                                    drop(guard);
                                    let _ = self.take_pinned_if_same(
                                        pool_id,
                                        req.lease_slot,
                                        &pinned_conn,
                                    );
                                    let res = conn.rollback().await.map(|_| ());
                                    let _ = conn.release().await;
                                    return res;
                                }
                                if req.lease_generation == 0 {
                                    return Ok(());
                                }
                                return Err(PostgresError::Interface(
                                    "stale lease generation for active pinned transaction"
                                        .to_string(),
                                ));
                            }
                        }
                        Ok(())
                    })
                };
                match outcome {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_postgres_err(req_id, &err)),
                }
            }
            DbRpcOp::Savepoint => {
                let sp = req
                    .savepoint
                    .ok_or_else(|| "DBRPC Savepoint frame missing savepoint name".to_string())?;
                if !is_valid_savepoint_ident(&sp) {
                    return Err(format!("invalid savepoint identifier `{sp}`"));
                }
                let stmt = format!("SAVEPOINT {sp}");
                let pinned = self.get_pinned(pool_id, req.lease_slot);
                let outcome = block_on_db(async move {
                    if let Some(pinned_conn) = pinned {
                        let mut guard = pinned_conn.lock().await;
                        if let Some((pinned_gen, conn)) = guard.as_mut() {
                            if *pinned_gen == req.lease_generation && req.lease_generation > 0 {
                                return conn.simple_query(&stmt).await.map(|_| ());
                            }
                            if req.lease_generation > *pinned_gen && *pinned_gen > 0 {
                                let (_, mut stale) = guard.take().expect("checked");
                                drop(guard);
                                let _ =
                                    self.take_pinned_if_same(pool_id, req.lease_slot, &pinned_conn);
                                let _ = stale.rollback().await;
                                let _ = stale.release().await;
                                return Err(PostgresError::Interface(
                                    "cannot SAVEPOINT: no transaction is in progress for lease generation"
                                        .to_string(),
                                ));
                            }
                            return Err(PostgresError::Interface(
                                "stale lease generation for active pinned transaction".to_string(),
                            ));
                        }
                    }
                    Err(PostgresError::Interface(
                        "cannot SAVEPOINT: no transaction is in progress".to_string(),
                    ))
                });
                match outcome {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_postgres_err(req_id, &err)),
                }
            }
            other => Err(format!(
                "unsupported DBRPC op {other:?} in PostgreSQL broker pool"
            )),
        }
    }

    fn close_pool(&self, pool_id: u64) {
        let removed = {
            let mut guard = self
                .pools
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.remove(&pool_id)
        };
        let drained = self.drain_pool_pinned(pool_id);
        if !drained.is_empty() {
            block_on_db(async move {
                for pinned in drained {
                    Self::release_pinned_entry(Some(pinned)).await;
                }
            });
        }
        if let Some(pool) = removed {
            let _ = block_on_db(async move { pool.close(Duration::from_secs(5)).await });
        }
    }
}

/// Export the `vertebra.BrokerEngineCapsule` PyCapsule for PostgreSQL (`DIALECT_TAG_POSTGRES = 1`).
#[pyfunction]
pub fn broker_engine_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyCapsule>> {
    new_broker_engine_pycapsule(py, Arc::new(PostgresBrokerBackend::new()))
}

#[pymodule(gil_used = false)]
fn _postgres(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", GIL_USED)?;
    m.add_function(wrap_pyfunction!(broker_engine_capsule, m)?)?;
    sync_client::PySyncPostgresClient::register(m)?;
    async_client::PyAsyncPostgresClient::register(m)?;
    python_pool::register(m)?;
    replication::register_pymodule(m)?;
    Ok(())
}
