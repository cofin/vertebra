//! Native Rust PostgreSQL Protocol 3.0 client and pool extension (`vertebra._postgres`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use url::Url;
use vertebra_core::broker::DIALECT_TAG_POSTGRES;
use vertebra_core::dbrpc::DbRpcOp;
use vertebra_db::py_async::block_on_db;
use vertebra_db::py_broker::BrokerPoolRegistry;
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
pub mod bind_plan;
pub mod binding;
pub mod cloud;
pub mod codecs;
pub mod config;
pub mod copy;
pub mod credentials;
mod cursor;
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
    PyAsyncPostgresListener, PyAsyncPostgresPortal, PySyncPostgresListener,
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

pub use vertebra_db::py_broker::FREE_THREADED;

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

#[derive(Clone, Copy)]
struct LeaseTarget {
    pool_id: u64,
    slot: u32,
    generation: u64,
}

enum PinnedOpControl {
    Keep,
    Take,
}

enum PinnedSqlOp {
    Commit,
    Rollback,
    SimpleQuery(String),
}

struct PinnedActionSpec {
    op_name: &'static str,
    allow_missing_or_unleased: bool,
    control: PinnedOpControl,
    sql_op: PinnedSqlOp,
}

#[derive(Default)]
struct PinnedSlotState {
    high_water_gen: u64,
    begin_seq: u64,
    entry: Option<PinnedPgConn>,
}

enum PinnedActionLookup {
    Missing {
        allow: bool,
    },
    Active {
        pinned_conn: Option<PinnedPgConn>,
        evicted: Option<PinnedPgConn>,
        newer_evicted: bool,
    },
    Stale,
}

/// Shared-memory broker backend managing broker-hosted `PostgresPool` instances (`DIALECT_TAG_POSTGRES = 1`).
pub struct PostgresBrokerBackend {
    pools: BrokerPoolRegistry<PostgresPool>,
    pinned: Mutex<HashMap<(u64, u32), PinnedSlotState>>,
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
            pools: BrokerPoolRegistry::new(),
            pinned: Mutex::new(HashMap::new()),
        }
    }

    fn stale_generation_err() -> PostgresError {
        PostgresError::Interface("stale lease generation for active pinned transaction".to_string())
    }

    fn encode_stale_generation_err(req_id: u64) -> Vec<u8> {
        encode_postgres_err(req_id, &Self::stale_generation_err())
    }

    fn has_pool(&self, pool_id: u64) -> bool {
        self.pools.get(pool_id).is_some()
    }

    fn begin_pinned_slot(
        &self,
        pool_id: u64,
        lease_slot: u32,
        lease_generation: u64,
    ) -> Result<(u64, Option<PinnedPgConn>), ()> {
        let mut guard = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = guard.entry((pool_id, lease_slot)).or_default();
        if lease_generation < state.high_water_gen {
            return Err(());
        }
        state.high_water_gen = lease_generation;
        state.begin_seq = state.begin_seq.wrapping_add(1);
        Ok((state.begin_seq, state.entry.take()))
    }

    fn inspect_pinned_for_execute(
        &self,
        pool_id: u64,
        lease_slot: u32,
        lease_generation: u64,
    ) -> Result<Option<(u64, PinnedPgConn)>, ()> {
        let mut guard = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(state) = guard.get_mut(&(pool_id, lease_slot)) else {
            return Ok(None);
        };
        if lease_generation < state.high_water_gen {
            return Err(());
        }
        if let Some(entry) = state.entry.as_ref() {
            return Ok(Some((state.begin_seq, Arc::clone(entry))));
        }
        if lease_generation > state.high_water_gen {
            state.high_water_gen = lease_generation;
            state.begin_seq = state.begin_seq.wrapping_add(1);
        }
        Ok(None)
    }

    fn clear_stale_pinned_after_eviction(
        &self,
        pool_id: u64,
        lease_slot: u32,
        new_gen: u64,
        target: &PinnedPgConn,
    ) {
        let mut guard = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(state) = guard.get_mut(&(pool_id, lease_slot)) {
            state.high_water_gen = state.high_water_gen.max(new_gen);
            state.begin_seq = state.begin_seq.wrapping_add(1);
            if state
                .entry
                .as_ref()
                .is_some_and(|existing| Arc::ptr_eq(existing, target))
            {
                state.entry = None;
            }
        }
    }

    fn error_on_drained_execute(
        &self,
        pool_id: u64,
        lease_slot: u32,
        lease_generation: u64,
    ) -> PostgresError {
        let guard = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard
            .get(&(pool_id, lease_slot))
            .is_some_and(|state| lease_generation < state.high_water_gen)
        {
            Self::stale_generation_err()
        } else {
            PostgresError::Interface("pinned transaction was closed or superseded".to_string())
        }
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
        let state = guard.get_mut(&(pool_id, lease_slot))?;
        if state
            .entry
            .as_ref()
            .is_some_and(|existing| Arc::ptr_eq(existing, target))
        {
            state.entry.take()
        } else {
            None
        }
    }

    fn install_pinned_entry(
        &self,
        pool_id: u64,
        lease_slot: u32,
        lease_generation: u64,
        begin_seq: u64,
        entry: PinnedPgConn,
    ) -> Result<Option<PinnedPgConn>, ()> {
        let mut guard = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(state) = guard.get_mut(&(pool_id, lease_slot)) else {
            return Err(());
        };
        if state.high_water_gen > lease_generation || state.begin_seq != begin_seq {
            return Err(());
        }
        Ok(state.entry.replace(entry))
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
        keys.into_iter()
            .filter_map(|k| guard.remove(&k).and_then(|state| state.entry))
            .collect()
    }

    async fn rollback_and_release(mut conn: PooledConnection) -> Result<(), PostgresError> {
        let rb_res = conn.rollback().await;
        let rel_res = conn.release().await;
        rb_res.map(|_| ())?;
        rel_res?;
        Ok(())
    }

    async fn release_pinned_entry(entry: Option<PinnedPgConn>) -> Result<(), PostgresError> {
        if let Some(pinned_conn) = entry {
            let mut guard = pinned_conn.lock().await;
            if let Some((_, conn)) = guard.take() {
                drop(guard);
                return Self::rollback_and_release(conn).await;
            }
        }
        Ok(())
    }

    fn lookup_pinned_action(
        &self,
        target: &LeaseTarget,
        allow_missing_or_unleased: bool,
        control: &PinnedOpControl,
    ) -> PinnedActionLookup {
        let mut guard = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(state) = guard.get_mut(&(target.pool_id, target.slot)) else {
            return PinnedActionLookup::Missing {
                allow: allow_missing_or_unleased,
            };
        };
        if state.high_water_gen > 0 && target.generation == 0 && allow_missing_or_unleased {
            return PinnedActionLookup::Missing { allow: true };
        }
        if target.generation < state.high_water_gen {
            return PinnedActionLookup::Stale;
        }
        if state.entry.is_none() {
            if allow_missing_or_unleased {
                if target.generation >= state.high_water_gen && target.generation > 0 {
                    state.high_water_gen = target.generation;
                    state.begin_seq = state.begin_seq.wrapping_add(1);
                }
                return PinnedActionLookup::Missing { allow: true };
            }
            return PinnedActionLookup::Missing { allow: false };
        }
        if target.generation > state.high_water_gen && state.high_water_gen > 0 {
            state.high_water_gen = target.generation;
            state.begin_seq = state.begin_seq.wrapping_add(1);
            let evicted = state.entry.take();
            return PinnedActionLookup::Active {
                pinned_conn: None,
                evicted,
                newer_evicted: true,
            };
        }
        let pinned_conn = match control {
            PinnedOpControl::Keep => state.entry.as_ref().map(Arc::clone),
            PinnedOpControl::Take => state.entry.take(),
        };
        PinnedActionLookup::Active {
            pinned_conn,
            evicted: None,
            newer_evicted: false,
        }
    }

    async fn run_pinned_action(
        &self,
        target: LeaseTarget,
        spec: PinnedActionSpec,
    ) -> Result<(), PostgresError> {
        let PinnedActionSpec {
            op_name,
            allow_missing_or_unleased,
            control,
            sql_op,
        } = spec;
        let lookup = self.lookup_pinned_action(&target, allow_missing_or_unleased, &control);
        match lookup {
            PinnedActionLookup::Missing { allow: true } => Ok(()),
            PinnedActionLookup::Missing { allow: false } => Err(PostgresError::Interface(format!(
                "cannot {op_name}: no transaction is in progress"
            ))),
            PinnedActionLookup::Stale => Err(Self::stale_generation_err()),
            PinnedActionLookup::Active {
                evicted: Some(evicted),
                newer_evicted: true,
                ..
            } => {
                Self::release_pinned_entry(Some(evicted)).await?;
                if allow_missing_or_unleased {
                    Ok(())
                } else {
                    Err(PostgresError::Interface(format!(
                        "cannot {op_name}: no transaction is in progress for lease generation"
                    )))
                }
            }
            PinnedActionLookup::Active {
                pinned_conn: None, ..
            } => {
                if allow_missing_or_unleased {
                    Ok(())
                } else {
                    Err(PostgresError::Interface(format!(
                        "cannot {op_name}: no transaction is in progress"
                    )))
                }
            }
            PinnedActionLookup::Active {
                pinned_conn: Some(pinned_conn),
                ..
            } => {
                let mut guard = pinned_conn.lock().await;
                let Some(pinned_gen) = guard.as_ref().map(|(g, _)| *g) else {
                    return if allow_missing_or_unleased {
                        Ok(())
                    } else {
                        Err(PostgresError::Interface(format!(
                            "cannot {op_name}: no transaction is in progress"
                        )))
                    };
                };
                if target.generation < pinned_gen {
                    return Err(Self::stale_generation_err());
                }
                match control {
                    PinnedOpControl::Keep => {
                        if let Some((_, conn)) = guard.as_mut() {
                            Self::apply_pinned_sql_op(conn, &sql_op).await?;
                        }
                        Ok(())
                    }
                    PinnedOpControl::Take => {
                        if let Some((_, mut conn)) = guard.take() {
                            drop(guard);
                            let out = Self::apply_pinned_sql_op(&mut conn, &sql_op).await;
                            let _ = conn.release().await;
                            out
                        } else {
                            Ok(())
                        }
                    }
                }
            }
        }
    }

    async fn apply_pinned_sql_op(
        conn: &mut PooledConnection,
        sql_op: &PinnedSqlOp,
    ) -> Result<(), PostgresError> {
        match sql_op {
            PinnedSqlOp::Commit => conn.commit().await.map(|_| ()),
            PinnedSqlOp::Rollback => conn.rollback().await.map(|_| ()),
            PinnedSqlOp::SimpleQuery(stmt) => conn.simple_query(stmt).await.map(|_| ()),
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
        Ok(self.pools.register(pool))
    }

    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String> {
        let pool = self
            .pools
            .get(pool_id)
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
                let pinned = if req.lease_generation > 0 {
                    match self.inspect_pinned_for_execute(
                        pool_id,
                        req.lease_slot,
                        req.lease_generation,
                    ) {
                        Ok(found) => found,
                        Err(()) => {
                            return Ok(Self::encode_stale_generation_err(req_id));
                        }
                    }
                } else {
                    None
                };
                let outcome: Result<QueryResult, PostgresError> = block_on_db(async move {
                    if let Some((_, pinned_conn)) = pinned {
                        let mut stale_to_evict: Option<PooledConnection> = None;
                        {
                            let mut guard = pinned_conn.lock().await;
                            if let Some((pinned_gen, conn)) = guard.as_mut() {
                                if *pinned_gen == req.lease_generation {
                                    return conn.query(&norm_sql, &params, None).await;
                                }
                                if req.lease_generation > *pinned_gen && *pinned_gen > 0 {
                                    if let Some((_, c)) = guard.take() {
                                        stale_to_evict = Some(c);
                                    }
                                } else {
                                    return Err(Self::stale_generation_err());
                                }
                            } else {
                                drop(guard);
                                return Err(self.error_on_drained_execute(
                                    pool_id,
                                    req.lease_slot,
                                    req.lease_generation,
                                ));
                            }
                        }
                        if let Some(stale) = stale_to_evict {
                            self.clear_stale_pinned_after_eviction(
                                pool_id,
                                req.lease_slot,
                                req.lease_generation,
                                &pinned_conn,
                            );
                            let _ = Self::rollback_and_release(stale).await;
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
                let (begin_seq, previous) =
                    match self.begin_pinned_slot(pool_id, req.lease_slot, req.lease_generation) {
                        Ok(res) => res,
                        Err(()) => return Ok(Self::encode_stale_generation_err(req_id)),
                    };
                let outcome: Result<PooledConnection, PostgresError> = block_on_db(async move {
                    let _ = Self::release_pinned_entry(previous).await;
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
                        let pool_alive = self.has_pool(pool_id);
                        let stale = if pool_alive {
                            self.install_pinned_entry(
                                pool_id,
                                req.lease_slot,
                                req.lease_generation,
                                begin_seq,
                                Arc::clone(&entry),
                            )
                            .ok()
                        } else {
                            None
                        };
                        let installed = stale.is_some();
                        let stale_entry = stale.flatten();
                        let pool_still_alive = installed && self.has_pool(pool_id);
                        let raced = if installed && !pool_still_alive {
                            self.take_pinned_if_same(pool_id, req.lease_slot, &entry)
                        } else {
                            None
                        };
                        let reject_entry = if !installed { Some(entry) } else { None };
                        if reject_entry.is_some() || raced.is_some() || stale_entry.is_some() {
                            block_on_db(async move {
                                let _ = Self::release_pinned_entry(reject_entry).await;
                                let _ = Self::release_pinned_entry(raced).await;
                                let _ = Self::release_pinned_entry(stale_entry).await;
                            });
                        }
                        if !pool_alive || (installed && !pool_still_alive) {
                            return Err(format!("unknown pool id {pool_id}"));
                        }
                        if !installed {
                            return Ok(Self::encode_stale_generation_err(req_id));
                        }
                        Ok(encode_dbrpc_response_frame(
                            &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                        ))
                    }
                    Err(err) => Ok(encode_postgres_err(req_id, &err)),
                }
            }
            DbRpcOp::Commit => {
                let target = LeaseTarget {
                    pool_id,
                    slot: req.lease_slot,
                    generation: req.lease_generation,
                };
                let outcome = block_on_db(async move {
                    self.run_pinned_action(
                        target,
                        PinnedActionSpec {
                            op_name: "COMMIT",
                            allow_missing_or_unleased: false,
                            control: PinnedOpControl::Take,
                            sql_op: PinnedSqlOp::Commit,
                        },
                    )
                    .await
                });
                match outcome {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_postgres_err(req_id, &err)),
                }
            }
            DbRpcOp::Rollback => {
                let target = LeaseTarget {
                    pool_id,
                    slot: req.lease_slot,
                    generation: req.lease_generation,
                };
                let outcome = if let Some(sp) = req.savepoint {
                    if !is_valid_savepoint_ident(&sp) {
                        return Err(format!("invalid savepoint identifier `{sp}`"));
                    }
                    let stmt = format!("ROLLBACK TO SAVEPOINT {sp}");
                    block_on_db(async move {
                        self.run_pinned_action(
                            target,
                            PinnedActionSpec {
                                op_name: "ROLLBACK TO SAVEPOINT",
                                allow_missing_or_unleased: false,
                                control: PinnedOpControl::Keep,
                                sql_op: PinnedSqlOp::SimpleQuery(stmt),
                            },
                        )
                        .await
                    })
                } else {
                    block_on_db(async move {
                        self.run_pinned_action(
                            target,
                            PinnedActionSpec {
                                op_name: "ROLLBACK",
                                allow_missing_or_unleased: true,
                                control: PinnedOpControl::Take,
                                sql_op: PinnedSqlOp::Rollback,
                            },
                        )
                        .await
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
                let target = LeaseTarget {
                    pool_id,
                    slot: req.lease_slot,
                    generation: req.lease_generation,
                };
                let outcome = block_on_db(async move {
                    self.run_pinned_action(
                        target,
                        PinnedActionSpec {
                            op_name: "SAVEPOINT",
                            allow_missing_or_unleased: false,
                            control: PinnedOpControl::Keep,
                            sql_op: PinnedSqlOp::SimpleQuery(stmt),
                        },
                    )
                    .await
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
        let removed = self.pools.remove(pool_id);
        let drained = self.drain_pool_pinned(pool_id);
        if !drained.is_empty() {
            block_on_db(async move {
                for pinned in drained {
                    let _ = Self::release_pinned_entry(Some(pinned)).await;
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
    m.add_function(wrap_pyfunction!(bind_plan::compile_binds, m)?)?;
    sync_client::PySyncPostgresClient::register(m)?;
    async_client::PyAsyncPostgresClient::register(m)?;
    python_pool::register(m)?;
    replication::register_pymodule(m)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_rejects_suspended_begin_cancelled_by_rollback_and_drained_execute() {
        let backend = PostgresBrokerBackend::new();
        let pool_id = 42u64;
        let slot = 0u32;

        // Simulate Begin(slot=0, gen=1) acquiring its begin_seq before awaiting pool.acquire().
        let (seq_1, prev_1) = backend.begin_pinned_slot(pool_id, slot, 1).unwrap();
        assert!(prev_1.is_none());

        // While Begin(gen=1) is suspended, Rollback(slot=0, gen=2) arrives and advances watermark.
        block_on_db(backend.run_pinned_action(
            LeaseTarget {
                pool_id,
                slot,
                generation: 2,
            },
            PinnedActionSpec {
                op_name: "ROLLBACK",
                allow_missing_or_unleased: true,
                control: PinnedOpControl::Take,
                sql_op: PinnedSqlOp::Rollback,
            },
        ))
        .unwrap();

        // Resumed Begin(gen=1) must fail watermark installation rather than leaking an open tx.
        let dummy_conn: PinnedPgConn = Arc::new(tokio::sync::Mutex::new(None));
        assert!(
            backend
                .install_pinned_entry(pool_id, slot, 1, seq_1, Arc::clone(&dummy_conn))
                .is_err()
        );

        // Newer Begin(gen=2) installs cleanly, while concurrent Execute(gen=2) whose connection
        // is drained by gen=3 fails with stale generation rather than falling through to autocommit.
        let (seq_2, prev_2) = backend.begin_pinned_slot(pool_id, slot, 2).unwrap();
        assert!(prev_2.is_none());
        let active_conn: PinnedPgConn = Arc::new(tokio::sync::Mutex::new(None));
        assert!(
            backend
                .install_pinned_entry(pool_id, slot, 2, seq_2, Arc::clone(&active_conn))
                .is_ok()
        );
        let (_, inspected) = backend
            .inspect_pinned_for_execute(pool_id, slot, 2)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&inspected, &active_conn));

        backend.clear_stale_pinned_after_eviction(pool_id, slot, 3, &active_conn);
        let err = backend.error_on_drained_execute(pool_id, slot, 2);
        assert!(err.to_string().contains("stale lease generation"));
    }
}
