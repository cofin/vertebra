//! `vertebra-mysql` — Standalone PyO3 extension (`vertebra._mysql`) for MySQL 8.4/9 and MariaDB.

pub mod arrow_ipc;
pub mod client;
pub mod codecs;
pub mod config;
mod cursor;
pub mod errors;
pub mod loopback;
pub mod protocol;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use mysql_async::{Params, Value};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use vertebra_core::broker::DIALECT_TAG_MYSQL;
use vertebra_core::dbrpc::DbRpcOp;
use vertebra_db::py_async::block_on_db;
use vertebra_db::{
    BrokerEngineBackend, BrokerPoolBackend, DbRpcEncodedResponse, DbRpcScalar,
    decode_dbrpc_request_frame, encode_dbrpc_response_frame, new_broker_engine_pycapsule,
};

use crate::client::{DecodedCell, MySQLClientCore};

pub use arrow_ipc::{
    DEFAULT_MYSQL_ARROW_SHM_CAPACITY, MYSQL_ARROW_DECIMAL_SCALE, ShmArrowBatchHandle,
    decode_arrow_ipc_to_mysql_rows, encode_result_set_to_arrow_ipc, encode_result_set_to_shm,
    encode_result_set_to_shm_batches, next_mysql_shm_name, open_mysql_shm_writer,
    write_mysql_shm_frame,
};
pub use client::{AsyncMySQLClient, MYSQL_COMPLETION_DRAINER_ATTR, SyncMySQLClient};
pub use config::{MySQLConnectConfig, MySQLSslMode};
pub use errors::{MySQLError, MySQLErrorKind, classify_mysql_error, map_mysql_error};
pub use loopback::MySQLWireTestServer;
pub use protocol::{
    HandshakeV10, PlaceholderPlan, compute_caching_sha2_scramble,
    compute_caching_sha2_scramble_bytes, compute_mysql_native_scramble,
    compute_mysql_native_scramble_bytes, decode_mysql_packet, encode_mysql_packet,
    encrypt_caching_sha2_rsa, encrypt_caching_sha2_rsa_password, normalize_mysql_placeholders,
    split_sql_statements,
};

#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

fn scalar_to_mysql_value(scalar: &DbRpcScalar) -> Value {
    match scalar {
        DbRpcScalar::Null => Value::NULL,
        DbRpcScalar::Bool(b) => Value::Int(i64::from(*b)),
        DbRpcScalar::Int(v) => Value::Int(*v),
        DbRpcScalar::UInt(v) => Value::UInt(*v),
        DbRpcScalar::Float(v) => Value::Double(*v),
        DbRpcScalar::Str(s) => Value::Bytes(s.as_bytes().to_vec()),
        DbRpcScalar::Bytes(b) => Value::Bytes(b.clone()),
    }
}

fn mysql_cell_to_scalar(cell: &DecodedCell) -> DbRpcScalar {
    match cell {
        DecodedCell::Null => DbRpcScalar::Null,
        DecodedCell::Bool(b) => DbRpcScalar::Bool(*b),
        DecodedCell::Int(v) => DbRpcScalar::Int(*v),
        DecodedCell::UInt(v) => DbRpcScalar::UInt(*v),
        DecodedCell::Float(v) => DbRpcScalar::Float(*v),
        DecodedCell::Text(s) | DecodedCell::Decimal(s) | DecodedCell::Json(s) => {
            DbRpcScalar::Str(s.clone())
        }
        DecodedCell::Bytes(b) => DbRpcScalar::Bytes(b.clone()),
        DecodedCell::Date(y, m, d) => DbRpcScalar::Str(format!("{y:04}-{m:02}-{d:02}")),
        DecodedCell::DateTime(y, m, d, hh, mm, ss, us) => DbRpcScalar::Str(format!(
            "{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{us:06}Z"
        )),
        DecodedCell::Time(is_neg, days, hh, mm, ss, us) => {
            let sign = if *is_neg { "-" } else { "" };
            let total_hours = u64::from(*days) * 24 + u64::from(*hh);
            DbRpcScalar::Str(format!("{sign}{total_hours:02}:{mm:02}:{ss:02}.{us:06}"))
        }
    }
}

fn encode_mysql_err(request_id: u64, err: &MySQLError) -> Vec<u8> {
    encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
        request_id,
        "MySQLError",
        err.to_string(),
    ))
}

/// Shared-memory broker backend managing broker-hosted MySQL pools (`DIALECT_TAG_MYSQL = 2`).
pub struct MySQLBrokerBackend {
    pools: RwLock<HashMap<u64, Arc<MySQLClientCore>>>,
    next_pool_id: AtomicU64,
}

impl Default for MySQLBrokerBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MySQLBrokerBackend {
    /// Construct a new [`MySQLBrokerBackend`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            next_pool_id: AtomicU64::new(1),
        }
    }
}

impl BrokerEngineBackend for MySQLBrokerBackend {
    fn dialect_tag(&self) -> u16 {
        DIALECT_TAG_MYSQL
    }

    fn dialect_name(&self) -> &'static str {
        "mysql"
    }

    fn normalize_statement(&self, sql: &str) -> (String, usize) {
        let (norm, plan) = normalize_mysql_placeholders(sql);
        let param_count = match plan {
            PlaceholderPlan::Named(names) => names.len(),
            PlaceholderPlan::Positional => norm.bytes().filter(|&b| b == b'?').count(),
        };
        (norm, param_count)
    }
}

impl BrokerPoolBackend for MySQLBrokerBackend {
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String> {
        if max_size == 0 {
            return Err("MySQL pool max_size must be >= 1".to_string());
        }
        let pool_max = max_size as usize;
        let pool_min = 1usize.min(pool_max);
        let config = MySQLConnectConfig::from_dsn_and_kwargs(
            dsn,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("prefer"),
            None,
            None,
            None,
            10_000,
            30_000,
            30_000,
            pool_min,
            pool_max,
            128,
            "utf8mb4",
            true,
            None,
            None,
            "PUBLIC",
            false,
            None,
            None,
            None,
        )
        .map_err(|e| e.to_string())?;
        let core = MySQLClientCore::new(config);
        let pool_id = self.next_pool_id.fetch_add(1, Ordering::Relaxed);
        let mut guard = self
            .pools
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.insert(pool_id, core);
        Ok(pool_id)
    }

    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String> {
        let core = {
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
            DbRpcOp::Execute => {
                let raw_sql = req
                    .sql
                    .as_deref()
                    .ok_or_else(|| "DBRPC Execute frame missing SQL".to_string())?;
                let (norm_sql, _) = self.normalize_statement(raw_sql);
                let params = if req.params.is_empty() {
                    Params::Empty
                } else {
                    Params::Positional(req.params.iter().map(scalar_to_mysql_value).collect())
                };
                let outcome =
                    block_on_db(async move { core.execute_result_set(norm_sql, params).await });
                match outcome {
                    Ok(rs) => {
                        let resp = if rs.columns.is_empty() {
                            DbRpcEncodedResponse::ok_count(
                                req_id,
                                i64::try_from(rs.affected_rows).unwrap_or(i64::MAX),
                                rs.last_insert_id,
                            )
                        } else {
                            let columns = rs.columns.into_iter().map(|(n, _)| n).collect();
                            let rows = rs
                                .rows
                                .iter()
                                .map(|row| row.iter().map(mysql_cell_to_scalar).collect())
                                .collect();
                            DbRpcEncodedResponse::ok_rows(
                                req_id,
                                i64::try_from(rs.affected_rows).unwrap_or(i64::MAX),
                                columns,
                                rows,
                            )
                        };
                        Ok(encode_dbrpc_response_frame(&resp))
                    }
                    Err(err) => Ok(encode_mysql_err(req_id, &err)),
                }
            }
            DbRpcOp::Begin => match block_on_db(async move { core.begin(None, false).await }) {
                Ok(()) => Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                )),
                Err(err) => Ok(encode_mysql_err(req_id, &err)),
            },
            DbRpcOp::Commit => match block_on_db(async move { core.commit().await }) {
                Ok(()) => Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                )),
                Err(err) => Ok(encode_mysql_err(req_id, &err)),
            },
            DbRpcOp::Rollback => {
                let res = if let Some(sp) = req.savepoint {
                    block_on_db(async move { core.rollback_to_savepoint(sp).await })
                } else {
                    block_on_db(async move { core.rollback().await })
                };
                match res {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_mysql_err(req_id, &err)),
                }
            }
            DbRpcOp::Savepoint => {
                let sp = req
                    .savepoint
                    .ok_or_else(|| "DBRPC Savepoint frame missing savepoint name".to_string())?;
                match block_on_db(async move { core.savepoint(sp).await }) {
                    Ok(_) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_mysql_err(req_id, &err)),
                }
            }
            DbRpcOp::Cancel => match block_on_db(async move { core.cancel_active_query().await }) {
                Ok(_) => Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                )),
                Err(err) => Ok(encode_mysql_err(req_id, &err)),
            },
            DbRpcOp::Close => {
                self.close_pool(pool_id);
                Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                ))
            }
            other => Err(format!(
                "unsupported DBRPC op {other:?} in MySQL broker pool"
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
        if let Some(core) = removed {
            let _ = block_on_db(async move { core.close().await });
        }
    }
}

/// Export the `vertebra.BrokerEngineCapsule` PyCapsule for MySQL (`DIALECT_TAG_MYSQL = 2`).
#[pyfunction]
pub fn broker_engine_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyCapsule>> {
    new_broker_engine_pycapsule(py, Arc::new(MySQLBrokerBackend::new()))
}

#[pymodule(gil_used = false)]
fn _mysql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    vertebra_db::install_rustls_ring_provider();
    m.add_function(wrap_pyfunction!(broker_engine_capsule, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;
    m.add_class::<SyncMySQLClient>()?;
    m.add_class::<AsyncMySQLClient>()?;
    m.add_class::<MySQLWireTestServer>()?;
    m.add_function(wrap_pyfunction!(compute_caching_sha2_scramble, m)?)?;
    m.add_function(wrap_pyfunction!(compute_mysql_native_scramble, m)?)?;
    m.add_function(wrap_pyfunction!(encrypt_caching_sha2_rsa_password, m)?)?;
    m.add_function(wrap_pyfunction!(map_mysql_error, m)?)?;
    m.add_function(wrap_pyfunction!(next_mysql_shm_name, m)?)?;
    cursor::register(m)?;
    Ok(())
}
