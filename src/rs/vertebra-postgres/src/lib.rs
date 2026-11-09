//! Native Rust PostgreSQL Protocol 3.0 client and pool extension (`vertebra._postgres`).

use pyo3::prelude::*;

pub mod async_client;
pub mod auth;
pub mod binding;
pub mod cloud;
pub mod codecs;
pub mod config;
pub mod credentials;
pub mod errors;
pub mod extended;
pub mod metadata;
pub mod pipeline;
mod pipeline_guard;
pub mod pool;
pub mod portal;
pub mod protocol;
pub mod python_pool;
pub mod python_values;
pub mod query;
pub mod result;
pub mod session;
pub mod sync_client;
pub mod tls;
pub mod transaction;
pub mod types;

pub use crate::async_client::PyAsyncPostgresPortal;
pub use crate::session::statements::{
    CachedStatement, PoolerCapabilities, PoolerMode, StatementCache, StatementKey,
};
pub use crate::session::{CommandBudget, CommandDeadlineGuard};
pub use crate::sync_client::PySyncPostgresPortal;
pub use codecs::{
    CodecContext, decode_value, decode_value_with_catalog, encode_value, encode_value_with_catalog,
    format_for_oid, format_for_oid_with_catalog,
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
pub use portal::{Portal, PortalEvent};
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

#[pymodule(gil_used = false)]
fn _postgres(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", GIL_USED)?;
    sync_client::PySyncPostgresClient::register(m)?;
    async_client::PyAsyncPostgresClient::register(m)?;
    python_pool::register(m)?;
    Ok(())
}
