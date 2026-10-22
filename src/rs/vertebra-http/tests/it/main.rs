//! Consolidated integration test binary for `vertebra-http`.

use std::sync::{LazyLock, Mutex as StdMutex};

/// Process-wide lock serializing tests that share the global `BatchBridge` singleton.
pub static BRIDGE_TEST_LOCK: LazyLock<StdMutex<()>> = LazyLock::new(|| StdMutex::new(()));

mod assets_and_observability;
mod backpressure_streams;
mod collector;
mod dispatcher;
mod grpc_wire;
mod h2c_and_mtls;
mod hmr;
#[cfg(feature = "http3")]
mod http3_quic;
mod ingress_fastpath;
mod mcp_discovery_bridge;
mod pubsub_broker;
mod router;
mod server;
mod spsc_violation;
mod sse_hub;
mod streaming_body;
#[cfg(feature = "tls")]
mod tls;
mod ws_mount;
