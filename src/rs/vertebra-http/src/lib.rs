//! vertebra-http: Native HTTP server, router, and batch collector.
//!
//! Free-threaded Python (3.14t) detection lives in the `vertebra-py` crate
//! as `vertebra_py::FREE_THREADED` — it is the only crate in the workspace
//! that currently needs to branch on `#[cfg(Py_GIL_DISABLED)]`. HTTP-level
//! code does not vary across GIL / free-threaded builds; if that changes,
//! mirror the constant here via `pyo3-build-config`.

pub mod access_log;
pub mod assets;
pub mod backpressure;
pub mod collector;
pub mod compression;
pub mod config;
pub mod correlation;
pub mod direct_worker;
pub mod dispatcher;
pub mod health;
pub mod hmr;
pub mod multipart;
pub mod payload;
pub mod policy;
pub mod profile;
pub mod response;
pub mod response_reader;
pub mod ring_factory;
pub mod route_table;
pub mod router;
pub mod server;

#[cfg(feature = "tls")]
pub mod tls;

pub use collector::RequestBatcher;
pub use correlation::InFlightRequests;
pub use policy::BatchPolicy;
pub use route_table::RouteTable;
pub use router::Router;
