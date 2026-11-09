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
pub mod protocol;
pub mod query;
pub mod result;
pub mod session;
pub mod sync_client;
pub mod tls;
pub mod types;

pub use codecs::{
    CodecContext, decode_value, decode_value_with_catalog, encode_value, encode_value_with_catalog,
    format_for_oid, format_for_oid_with_catalog,
};
pub use extended::{ExtendedQueryCollector, Parameter, PreparedDescription, encode_bind};
pub use metadata::{
    OwnedTextField, OwnedTextResult, ParameterCandidate, ResolvedParameters,
    normalize_materialized_result, resolve_parameter_metadata,
};
pub use query::SimpleQueryCollector;
pub use result::{
    Column, Format, PgNumeric, PgValue, QueryResult, TemporalKind, TransactionStatus, VectorKind,
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
    m.add_class::<sync_client::PySyncPostgresClient>()?;
    m.add_class::<async_client::PyAsyncPostgresClient>()?;
    Ok(())
}
