//! `vertebra-oracle` crate and `vertebra._oracle` PyO3 extension module.

pub mod arrow;
pub mod client;
pub mod config;
pub mod dialer;
pub mod errors;
pub mod notify;
pub mod pool;
pub mod types;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use url::Url;
use vertebra_core::broker::DIALECT_TAG_ORACLE;
use vertebra_core::dbrpc::DbRpcOp;
use vertebra_core::text::first_sql_keyword;
use vertebra_db::{
    BrokerEngineBackend, BrokerPoolBackend, DbRpcEncodedResponse, DbRpcScalar, PlaceholderStyle,
    decode_dbrpc_request_frame, encode_dbrpc_response_frame, new_broker_engine_pycapsule,
    normalize_placeholders,
};

use crate::client::{
    AsyncOracleClient, BindArgs, ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED, OraclePipelineOutcome,
    OutBindSpec, SyncOracleClient, WorkerCommand, WorkerReply,
};
use crate::config::{NativeOracleConfig, RawOracleOptions};
use crate::errors::OracleError;
use crate::notify::{AsyncOracleListener, OracleNotification, SyncOracleListener};
use crate::pool::{AsyncOraclePool, OraclePoolCore, OraclePoolOptions, SyncOraclePool};
use crate::types::{OracleCellValue, OracleColumn, OracleQueryResult};

/// Whether this build targets free-threaded Python (3.14t+).
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

/// Whether this extension requires the Python GIL (`false` for free-threaded safety).
pub const GIL_USED: bool = false;

fn parse_oracle_broker_dsn(dsn: &str) -> Result<NativeOracleConfig, String> {
    let trimmed = dsn.trim();
    if trimmed.is_empty() || trimmed.contains('\0') {
        return Err("Oracle DSN must not be empty or contain NUL bytes".to_string());
    }
    if trimmed.eq_ignore_ascii_case(":memory:") || trimmed.eq_ignore_ascii_case("memory") {
        return Err(
            "Oracle DSN `:memory:` is unsupported; configure a real Oracle TNS/EZConnect endpoint"
                .to_string(),
        );
    }

    let mut raw = RawOracleOptions {
        user: None,
        password: None,
        dsn: None,
        host: "localhost".to_string(),
        port: 1521,
        service_name: "FREEPDB1".to_string(),
        sid: None,
        tns_admin: None,
        connect_descriptor: None,
        protocol: "tcp".to_string(),
        ssl_server_dn_match: true,
        ssl_server_cert_dn: None,
        wallet_location: None,
        wallet_password: None,
        pem_cert_data: None,
        cwallet_sso_data: None,
        auth_mode: "password".to_string(),
        access_token: None,
        token_auth_options: None,
        cclass: None,
        purity: "SELF".to_string(),
        connection_tag: None,
        stmtcachesize: 64,
        prefetchrows: 256,
        arraysize: 256,
        call_timeout_ms: None,
        connect_timeout: 10.0,
        enable_lowercase_column_names: true,
        client_identifier: None,
        module: None,
        action: None,
        ecid: None,
    };

    if let Some((scheme, _)) = trimmed.split_once("://") {
        let lower_scheme = scheme.to_ascii_lowercase();
        let rewritten = match lower_scheme.as_str() {
            "oracle" | "oracle+thin" | "oracle+vertebra" | "oracle+vtb" => {
                format!("oracle://{}", &trimmed[scheme.len() + 3..])
            }
            other => return Err(format!("unsupported Oracle DSN scheme `{other}://`")),
        };
        let parsed = Url::parse(&rewritten).map_err(|e| format!("invalid Oracle DSN: {e}"))?;
        let host = parsed
            .host_str()
            .filter(|h| !h.is_empty())
            .ok_or_else(|| "Oracle DSN is missing a host".to_string())?;
        raw.host = host.trim_matches(['[', ']']).to_string();
        if let Some(p) = parsed.port() {
            raw.port = i32::from(p);
        }
        if !parsed.username().is_empty() {
            raw.user = Some(parsed.username().to_string());
        }
        if let Some(pw) = parsed.password() {
            raw.password = Some(pw.to_string());
        }
        let svc = parsed.path().trim_start_matches('/');
        if !svc.is_empty() {
            raw.service_name = svc.to_string();
        }
        for (k, v) in parsed.query_pairs() {
            match k.to_ascii_lowercase().as_str() {
                "service_name" if !v.is_empty() => raw.service_name = v.into_owned(),
                "sid" if !v.is_empty() => raw.sid = Some(v.into_owned()),
                "protocol" if !v.is_empty() => raw.protocol = v.into_owned(),
                "purity" if !v.is_empty() => raw.purity = v.into_owned(),
                "auth_mode" if !v.is_empty() => raw.auth_mode = v.into_owned(),
                _ => {}
            }
        }
        raw.dsn = Some(trimmed.to_string());
    } else {
        raw.dsn = Some(trimmed.to_string());
    }

    NativeOracleConfig::validate(raw).map_err(|e| e.to_string())
}

fn scalar_to_oracle_cell(scalar: &DbRpcScalar) -> OracleCellValue {
    match scalar {
        DbRpcScalar::Null => OracleCellValue::Null,
        DbRpcScalar::Bool(b) => OracleCellValue::Bool(*b),
        DbRpcScalar::Int(v) => OracleCellValue::Int(*v),
        DbRpcScalar::UInt(v) => match i64::try_from(*v) {
            Ok(i) => OracleCellValue::Int(i),
            Err(_) => OracleCellValue::DecimalStr(v.to_string()),
        },
        DbRpcScalar::Float(v) => OracleCellValue::Float(*v),
        DbRpcScalar::Str(s) => OracleCellValue::Str(s.clone()),
        DbRpcScalar::Bytes(b) => OracleCellValue::Bytes(b.clone()),
    }
}

fn oracle_cell_to_scalar(cell: &OracleCellValue) -> DbRpcScalar {
    match cell {
        OracleCellValue::Null => DbRpcScalar::Null,
        OracleCellValue::Bool(b) => DbRpcScalar::Bool(*b),
        OracleCellValue::Int(v) => DbRpcScalar::Int(*v),
        OracleCellValue::Float(v) => DbRpcScalar::Float(*v),
        OracleCellValue::DecimalStr(s) | OracleCellValue::Str(s) => DbRpcScalar::Str(s.clone()),
        OracleCellValue::Bytes(b) | OracleCellValue::Oson(b) => DbRpcScalar::Bytes(b.clone()),
        OracleCellValue::Date { year, month, day } => {
            DbRpcScalar::Str(format!("{year:04}-{month:02}-{day:02}"))
        }
        OracleCellValue::Timestamp {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            tz_offset_seconds,
        } => DbRpcScalar::Str(match tz_offset_seconds {
            Some(off) => format!(
                "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanosecond:09}{:+03}:00",
                off / 3600
            ),
            None => format!(
                "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanosecond:09}"
            ),
        }),
        OracleCellValue::Vector(vec) => DbRpcScalar::Str(format!("{vec:?}")),
    }
}

fn oracle_sql_returns_rows(sql: &str) -> bool {
    matches!(
        first_sql_keyword(sql),
        Some(kw) if kw.eq_ignore_ascii_case("SELECT") || kw.eq_ignore_ascii_case("WITH")
    )
}

fn encode_oracle_err(request_id: u64, err: &OracleError) -> Vec<u8> {
    encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
        request_id,
        "OracleError",
        err.to_string(),
    ))
}

/// Shared-memory broker backend managing broker-hosted Oracle pools (`DIALECT_TAG_ORACLE = 4`).
pub struct OracleBrokerBackend {
    pools: RwLock<HashMap<u64, Arc<OraclePoolCore>>>,
    next_pool_id: AtomicU64,
}

impl Default for OracleBrokerBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleBrokerBackend {
    /// Construct a new [`OracleBrokerBackend`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            next_pool_id: AtomicU64::new(1),
        }
    }
}

impl BrokerEngineBackend for OracleBrokerBackend {
    fn dialect_tag(&self) -> u16 {
        DIALECT_TAG_ORACLE
    }

    fn dialect_name(&self) -> &'static str {
        "oracle"
    }

    fn normalize_statement(&self, sql: &str) -> (String, usize) {
        normalize_placeholders(sql, PlaceholderStyle::ColonNumbered)
    }
}

impl BrokerPoolBackend for OracleBrokerBackend {
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String> {
        if max_size == 0 {
            return Err("Oracle pool max_size must be >= 1".to_string());
        }
        let cfg = parse_oracle_broker_dsn(dsn)?;
        let opts = OraclePoolOptions::validate(
            cfg,
            0,
            i64::from(max_size),
            30.0,
            10.0,
            30.0,
            3600.0,
            600.0,
            30.0,
        )
        .map_err(|e| e.to_string())?;
        let core = Arc::new(OraclePoolCore::new(opts));
        let pool_id = self.next_pool_id.fetch_add(1, Ordering::Relaxed);
        let mut guard = self
            .pools
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.insert(pool_id, core);
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
        if req.op == DbRpcOp::Close {
            self.close_pool(pool_id);
            return Ok(encode_dbrpc_response_frame(
                &DbRpcEncodedResponse::ok_count(req_id, 0, None),
            ));
        }

        let worker = match pool.acquire_sync(None, None, None, None, None, None, None) {
            Ok(w) => w,
            Err(err) => return Ok(encode_oracle_err(req_id, &err)),
        };

        let result = match req.op {
            DbRpcOp::Execute => {
                let raw_sql = req
                    .sql
                    .as_deref()
                    .ok_or_else(|| "DBRPC Execute frame missing SQL".to_string())?;
                let (norm_sql, _) = self.normalize_statement(raw_sql);
                let bind_args =
                    BindArgs::Positional(req.params.iter().map(scalar_to_oracle_cell).collect());
                let cmd = if oracle_sql_returns_rows(&norm_sql) {
                    WorkerCommand::Query {
                        sql: norm_sql,
                        params: bind_args,
                        out_binds: OutBindSpec::None,
                        fetch_decimals: false,
                        lowercase_column_names: true,
                        max_rows: None,
                    }
                } else {
                    WorkerCommand::Execute {
                        sql: norm_sql,
                        params: bind_args,
                        out_binds: OutBindSpec::None,
                        fetch_decimals: false,
                        lowercase_column_names: true,
                    }
                };
                worker.dispatch_sync(cmd)
            }
            DbRpcOp::Begin => worker.dispatch_sync(WorkerCommand::Begin),
            DbRpcOp::Commit => worker.dispatch_sync(WorkerCommand::Commit),
            DbRpcOp::Rollback => {
                if let Some(sp) = req.savepoint {
                    worker.dispatch_sync(WorkerCommand::RollbackToSavepoint(sp))
                } else {
                    worker.dispatch_sync(WorkerCommand::Rollback)
                }
            }
            DbRpcOp::Savepoint => {
                let sp = req
                    .savepoint
                    .ok_or_else(|| "DBRPC Savepoint frame missing savepoint name".to_string())?;
                worker.dispatch_sync(WorkerCommand::Savepoint(sp))
            }
            DbRpcOp::Cancel => {
                worker.break_handle().break_execution();
                Ok(WorkerReply::Unit)
            }
            other => {
                let _ = pool.release_sync(&worker, None, false);
                return Err(format!(
                    "unsupported DBRPC op {other:?} in Oracle broker pool"
                ));
            }
        };

        let discard = result.is_err();
        let _ = pool.release_sync(&worker, None, discard);

        match result {
            Ok(WorkerReply::QueryOutcome(outcome)) => {
                let resp = if outcome.columns.is_empty() {
                    DbRpcEncodedResponse::ok_count(
                        req_id,
                        i64::try_from(outcome.rowcount).unwrap_or(i64::MAX),
                        None,
                    )
                } else {
                    let cols = outcome.columns.into_iter().map(|c| c.name).collect();
                    let rows = outcome
                        .rows
                        .iter()
                        .map(|r| r.iter().map(oracle_cell_to_scalar).collect())
                        .collect();
                    DbRpcEncodedResponse::ok_rows(
                        req_id,
                        i64::try_from(outcome.rowcount).unwrap_or(i64::MAX),
                        cols,
                        rows,
                    )
                };
                Ok(encode_dbrpc_response_frame(&resp))
            }
            Ok(WorkerReply::RowCount(n)) => Ok(encode_dbrpc_response_frame(
                &DbRpcEncodedResponse::ok_count(req_id, i64::try_from(n).unwrap_or(i64::MAX), None),
            )),
            Ok(
                WorkerReply::Unit | WorkerReply::ArrowIpc { .. } | WorkerReply::PipelineOutcomes(_),
            ) => Ok(encode_dbrpc_response_frame(
                &DbRpcEncodedResponse::ok_count(req_id, 0, None),
            )),
            Err(err) => Ok(encode_oracle_err(req_id, &err)),
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
        if let Some(pool) = removed {
            let _ = pool.close_sync();
        }
    }
}

/// Export the `vertebra.BrokerEngineCapsule` PyCapsule for Oracle (`DIALECT_TAG_ORACLE = 4`).
#[pyfunction]
pub fn broker_engine_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyCapsule>> {
    new_broker_engine_pycapsule(py, Arc::new(OracleBrokerBackend::new()))
}

#[pymodule(gil_used = false)]
pub fn _oracle(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", GIL_USED)?;
    m.add(
        "ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED",
        ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED,
    )?;
    m.add_function(wrap_pyfunction!(broker_engine_capsule, m)?)?;
    m.add_class::<OracleColumn>()?;
    m.add_class::<OracleQueryResult>()?;
    m.add_class::<OraclePipelineOutcome>()?;
    m.add_class::<OracleNotification>()?;
    m.add_class::<AsyncOracleListener>()?;
    m.add_class::<SyncOracleListener>()?;
    m.add_class::<SyncOracleClient>()?;
    m.add_class::<AsyncOracleClient>()?;
    m.add_class::<SyncOraclePool>()?;
    m.add_class::<AsyncOraclePool>()?;
    Ok(())
}
