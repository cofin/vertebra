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
pub mod batcher;
pub mod body;
pub mod compression;
pub mod config;
pub mod cors;
pub mod discovery;
pub mod dispatcher;
pub mod grpc;
pub mod h2c;
pub mod health;
pub mod hmr;
pub mod http3;
pub mod mcp_bridge;
pub mod multipart;
pub mod payload;
pub mod profile;
pub mod pubsub;
pub mod response;
pub mod routing;
pub mod server;
pub mod sse;
pub mod tls;
pub mod workers;
pub mod ws_mount;

pub use batcher::{Batch, BatchLimit, PendingRequest, RequestBatcher};
pub use dispatcher::correlation::{CorrelationId, InFlightRequests};
pub use routing::{HandlerId, RouteTable, Router, RouterError};
