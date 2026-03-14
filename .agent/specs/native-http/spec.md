# Chapter 5: Native HTTP & Routing

**Beads ID:** vertebra-w4i.5
**Flow ID:** native-http
**Status:** Ready for implementation
**Depends on:** Ch4 (native-ipc) — `vertebra-core` ring buffers, batch arenas, `RequestInfo`/`ResponseInfo` proxy structs
**Consolidates:** `http-server-h2_20260314`, `native-routing_20260314`, `batch-collector_20260314`, and Phase 2 of `native-core_20260314`

---

## 1. Goal

Implement the Rust HTTP edge layer for the Ghost Framework rewrite. This chapter owns everything between a TCP socket arriving and a filled `BatchArena` landing on the MPMC ring buffer for Python workers to consume. No Python code is written here. No FFI bridge is written here. The output is a standalone Rust crate (`vertebra-http`) that compiles to both a library and the `vertebra-server` binary.

At the end of this chapter:

- An HTTP/1.1 or HTTP/2 request arrives at the Tokio accept loop.
- The router resolves the path+method pair to a `handler_id: u64` in under 1 microsecond.
- The batch collector aggregates requests into `BatchArena` slots, draining when either 64 requests accumulate or 1 millisecond elapses.
- The dispatcher pushes the arena pointer into the MPMC ring buffer.
- The benchmark binary shows >100,000 req/s on a hello-world endpoint on a single machine.

---

## 2. Architecture Overview

```
TCP/TLS Accept
     │
     ▼
┌─────────────────────┐
│   server.rs         │  Hyper auto::Builder (HTTP/1.1 + HTTP/2)
│   TokioIo wrap      │  Optional TLS via tokio-rustls + rustls
│   ALPN: h2, http/1.1│
└────────┬────────────┘
         │  hyper::Request<Incoming>
         ▼
┌─────────────────────┐
│   router.rs         │  matchit trie lookup
│   route_table.rs    │  RouteTable built at startup
│   → handler_id: u64 │  FNV hash of METHOD + PATH_PATTERN
└────────┬────────────┘
         │  (handler_id, path_params, request)
         ▼
┌─────────────────────┐
│   collector.rs      │  Accumulates into BatchArena slots
│   policy.rs         │  Drain on: size>=64 OR elapsed>=1ms
└────────┬────────────┘
         │  Arc<BatchArena>
         ▼
┌─────────────────────┐
│   dispatcher.rs     │  Push arena into MPMC ring (vertebra-core)
└────────┬────────────┘
         │  (response arrives back via arena ResponseInfo)
         ▼
┌─────────────────────┐
│   response.rs       │  Read ResponseInfo, build hyper::Response
└─────────────────────┘
```

The HTTP/2 path uses ALPN-based TLS negotiation only. h2c (cleartext HTTP/2 upgrade) is explicitly out of scope: research found h2c upgrade to be prototype-grade with correctness risks on stream 1 and fragile prior-knowledge detection. ALPN is the correct production negotiation mechanism per RFC 9113 guidance.

HTTP/3 is deferred to a separate chapter (`http3-quic-transport_20260314`), consistent with the research recommendation to defer QUIC until the backend contract is stream-aware.

---

## 3. Crate Layout

```
src/rs/vertebra-http/
├── Cargo.toml
├── src/
│   ├── lib.rs           # Public API re-exports
│   ├── main.rs          # vertebra-server binary entrypoint
│   ├── config.rs        # ServerConfig
│   ├── server.rs        # Hyper/Tokio accept loop
│   ├── tls.rs           # rustls TLS acceptor + ALPN
│   ├── router.rs        # matchit lookup → handler_id
│   ├── route_table.rs   # RouteTable insert/build API
│   ├── collector.rs     # BatchCollector: size + timeout drain
│   ├── dispatcher.rs    # Push BatchArena to MPMC ring
│   ├── policy.rs        # DrainPolicy: fixed and adaptive
│   └── response.rs      # ResponseInfo → hyper::Response
├── tests/
│   ├── test_server.rs
│   ├── test_router.rs
│   ├── test_collector.rs
│   └── test_dispatcher.rs
└── benches/
    └── server_throughput.rs
```

---

## 4. Cargo.toml

```toml
[package]
name = "vertebra-http"
version.workspace = true
authors.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
description = "Native HTTP server, router, and batch collector for Vertebra"

[[bin]]
name = "vertebra-server"
path = "src/main.rs"

[dependencies]
vertebra-core = { path = "../vertebra-core" }

# HTTP stack
hyper          = { version = "1", features = ["full"] }
hyper-util     = { version = "0.1", features = ["full"] }
http           = "1"
http-body-util = "0.1"
tower-service  = "0.3"

# Async runtime
tokio = { workspace = true }

# Routing
matchit = { workspace = true }

# TLS (optional feature)
rustls          = { version = "0.23", optional = true, features = ["ring"] }
tokio-rustls    = { version = "0.26", optional = true }
rustls-pemfile  = { version = "2", optional = true }

# Utilities
thiserror  = { workspace = true }
tracing    = "0.1"
fnv        = "1.0"
bytes      = "1"
parking_lot = "0.12"
crossbeam-channel = "0.5"

[features]
default = []
tls = ["dep:rustls", "dep:tokio-rustls", "dep:rustls-pemfile"]

[dev-dependencies]
tokio-test    = "0.4"
criterion     = { workspace = true }
reqwest       = { version = "0.12", features = ["json", "http2"] }
tempfile      = "3"
rustls-pemfile = "2"
rcgen         = "0.13"

[[bench]]
name = "server_throughput"
harness = false
```

The workspace `Cargo.toml` at `src/rs/Cargo.toml` must have `vertebra-http` added to the `members` array.

---

## 5. Phase 1: Hyper HTTP Server

### 5.1 `src/config.rs`

```rust
//! Server configuration.

use std::net::SocketAddr;
use std::time::Duration;

/// Top-level server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind on.
    pub bind: SocketAddr,
    /// Number of worker threads for the Tokio runtime.
    /// Defaults to the number of logical CPUs.
    pub worker_threads: usize,
    /// Maximum batch size before forced drain.
    pub batch_size: usize,
    /// Maximum time between batch drains.
    pub batch_timeout: Duration,
    /// Optional TLS configuration. If `None`, the server is plain TCP.
    pub tls: Option<TlsConfig>,
    /// Connection keep-alive timeout.
    pub keepalive_timeout: Duration,
    /// Maximum number of concurrent connections (0 = unlimited).
    pub max_connections: usize,
}

/// TLS configuration.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the PEM-encoded certificate chain.
    pub cert_path: std::path::PathBuf,
    /// Path to the PEM-encoded private key.
    pub key_path: std::path::PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8000".parse().unwrap(),
            worker_threads: num_cpus(),
            batch_size: 64,
            batch_timeout: Duration::from_millis(1),
            tls: None,
            keepalive_timeout: Duration::from_secs(75),
            max_connections: 0,
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
```

**Pass/fail criteria:**
- `ServerConfig::default()` compiles and has `batch_size == 64`.
- `TlsConfig` is `Debug + Clone`.

### 5.2 `src/server.rs`

```rust
//! Hyper/Tokio accept loop.
//!
//! Supports both HTTP/1.1 and HTTP/2 via `hyper_util::server::conn::auto::Builder`.
//! When TLS is configured, the ALPN negotiation determines the protocol; without TLS,
//! only HTTP/1.1 is available (h2c is intentionally omitted).

use std::net::SocketAddr;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::config::ServerConfig;
use crate::dispatcher::Dispatcher;
use crate::response::handle;
use crate::router::NativeRouter;

#[cfg(feature = "tls")]
use crate::tls::TlsAcceptor;

/// Shared state threaded through every request service call.
pub struct ServerState {
    pub router: Arc<NativeRouter>,
    pub dispatcher: Arc<Dispatcher>,
    pub config: Arc<ServerConfig>,
}

/// Run the HTTP server until the provided shutdown signal future resolves.
///
/// # Errors
/// Returns an error if the TCP listener cannot be bound.
pub async fn run(
    config: Arc<ServerConfig>,
    router: Arc<NativeRouter>,
    dispatcher: Arc<Dispatcher>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(config.bind).await?;
    info!(addr = %config.bind, "vertebra-http listening");

    let state = Arc::new(ServerState {
        router: Arc::clone(&router),
        dispatcher: Arc::clone(&dispatcher),
        config: Arc::clone(&config),
    });

    #[cfg(feature = "tls")]
    let tls_acceptor: Option<TlsAcceptor> = config
        .tls
        .as_ref()
        .map(|tc| TlsAcceptor::from_config(tc))
        .transpose()?;

    #[cfg(not(feature = "tls"))]
    let tls_acceptor: Option<()> = None;

    let _ = tls_acceptor; // suppress unused warning in non-tls builds

    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let state = Arc::clone(&state);
                        #[cfg(feature = "tls")]
                        let tls = config.tls.as_ref().map(|_| {
                            TlsAcceptor::from_config(config.tls.as_ref().unwrap())
                                .expect("TLS already validated at startup")
                        });
                        tokio::spawn(async move {
                            #[cfg(feature = "tls")]
                            if let Some(tls_acc) = tls {
                                if let Err(e) = serve_tls(stream, peer, state, tls_acc).await {
                                    debug!(peer = %peer, error = %e, "TLS connection error");
                                }
                                return;
                            }
                            if let Err(e) = serve_plain(stream, peer, state).await {
                                debug!(peer = %peer, error = %e, "HTTP connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "accept failed");
                    }
                }
            }
            _ = &mut shutdown => {
                info!("shutdown signal received, stopping accept loop");
                break;
            }
        }
    }

    Ok(())
}

async fn serve_plain(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<ServerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!(peer = %peer, "plain TCP connection");
    let io = TokioIo::new(stream);
    let state = Arc::clone(&state);

    AutoBuilder::new(TokioExecutor::new())
        .serve_connection(
            io,
            service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                async move { handle(req, state).await }
            }),
        )
        .await
        .map_err(Into::into)
}

#[cfg(feature = "tls")]
async fn serve_tls(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<ServerState>,
    acceptor: TlsAcceptor,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!(peer = %peer, "TLS connection");
    let tls_stream = acceptor.accept(stream).await?;
    let io = TokioIo::new(tls_stream);
    let state = Arc::clone(&state);

    AutoBuilder::new(TokioExecutor::new())
        .serve_connection(
            io,
            service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                async move { handle(req, state).await }
            }),
        )
        .await
        .map_err(Into::into)
}
```

**Design notes:**

- `AutoBuilder` from `hyper_util` selects HTTP/1.1 or HTTP/2 automatically. On a plain TCP stream it always negotiates HTTP/1.1. On a TLS stream, ALPN determines the protocol.
- No h2c prior-knowledge detection is performed. This avoids the correctness risks documented in the research (split preface, broken stream-1 response handling, non-RFC-robust upgrade validation).
- The `shutdown` future allows clean shutdown without losing in-flight requests.
- `#[cfg(feature = "tls")]` guards ensure the binary compiles without `rustls` unless the `tls` feature is enabled.

**Pass/fail criteria:**
- `cargo build --package vertebra-http` succeeds.
- `cargo build --package vertebra-http --features tls` succeeds.
- A plain TCP connection returns an HTTP response.

### 5.3 `src/tls.rs`

```rust
//! TLS acceptor using rustls with ALPN for h2 and http/1.1 negotiation.
//!
//! Only compiled when the `tls` feature is enabled.

#![cfg(feature = "tls")]

use std::io::BufReader;
use std::sync::Arc;

use rustls::ServerConfig as RustlsServerConfig;
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

use crate::config::TlsConfig;

/// Wraps a `tokio_rustls::TlsAcceptor` with the server ALPN configuration.
#[derive(Clone)]
pub struct TlsAcceptor {
    inner: tokio_rustls::TlsAcceptor,
}

impl TlsAcceptor {
    /// Build a `TlsAcceptor` from a `TlsConfig`.
    ///
    /// ALPN is configured to advertise `h2` first, then `http/1.1`. Hyper's
    /// `auto::Builder` will then honour the negotiated protocol.
    ///
    /// # Errors
    /// Returns an error if the certificate or key files cannot be read or parsed.
    pub fn from_config(
        cfg: &TlsConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cert_file = std::fs::File::open(&cfg.cert_path)?;
        let key_file = std::fs::File::open(&cfg.key_path)?;

        let certs: Vec<_> =
            rustls_pemfile::certs(&mut BufReader::new(cert_file)).collect::<Result<_, _>>()?;
        let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))?
            .ok_or("no private key found in key file")?;

        let mut tls_config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;

        // Advertise h2 first, then http/1.1. The client selects from this list.
        // Hyper's auto::Builder then inspects the negotiated ALPN value and
        // speaks the correct protocol on the TLS stream.
        tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls_config));
        Ok(Self { inner: acceptor })
    }

    /// Perform the TLS handshake on an accepted TCP stream.
    pub async fn accept(
        &self,
        stream: TcpStream,
    ) -> Result<TlsStream<TcpStream>, std::io::Error> {
        self.inner.accept(stream).await
    }
}
```

**Design rationale:**
- `rustls` is chosen over `native-tls` for memory-safety, platform consistency, and first-class ALPN support.
- ALPN protocols are `["h2", "http/1.1"]` in that order. Clients that support HTTP/2 will negotiate it; others fall back to HTTP/1.1. This is the correct production path per RFC 9113.
- The `from_config` function is cheap to call for validation at startup; in `server.rs` the actual acceptor is reused across connections.

**Pass/fail criteria:**
- `cargo test --package vertebra-http --features tls` passes `test_tls_alpn_negotiation`.
- Connecting with an HTTP/2-capable client (e.g., `reqwest` with HTTP/2 enabled) receives an HTTP/2 response.
- Connecting with an HTTP/1.1-only client receives an HTTP/1.1 response.

---

## 6. Phase 2: Native Router

### 6.1 `src/route_table.rs`

```rust
//! Route registration API.
//!
//! `RouteTable` is a builder that accepts (method, path_pattern, handler_id)
//! triples before startup. Once built it hands off the data to `NativeRouter`.

use std::collections::HashMap;

use fnv::FnvHasher;
use std::hash::{Hash, Hasher};

/// Stable identifier for a route handler.
///
/// The value is the FNV-1a hash of the canonical string `"METHOD /path/pattern"`.
/// Python's `VertebraApp` produces the same hash when registering routes, which
/// lets both sides agree on an ID without string allocation at dispatch time.
pub type HandlerId = u64;

/// Compute a stable `HandlerId` for a (method, path_pattern) pair.
///
/// The canonical form is `"{METHOD_UPPER} {path_pattern}"` with a single space.
pub fn handler_id(method: &str, path: &str) -> HandlerId {
    let mut h = FnvHasher::default();
    method.to_ascii_uppercase().hash(&mut h);
    b' '.hash(&mut h);
    path.hash(&mut h);
    h.finish()
}

/// A single route entry ready for insertion into a `matchit::Router`.
#[derive(Debug, Clone)]
pub struct RouteEntry {
    pub method: String,
    pub path: String,
    pub handler_id: HandlerId,
}

/// Builder for the route table.
///
/// Collect routes at startup, then call `build()` to get a `NativeRouter`.
#[derive(Debug, Default)]
pub struct RouteTable {
    entries: Vec<RouteEntry>,
}

impl RouteTable {
    /// Create an empty route table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a route.
    ///
    /// The `handler_id` is computed automatically from the method and path;
    /// the caller only needs to ensure the same computation happens in Python.
    ///
    /// # Panics
    /// Does not panic. Duplicate (method, path) pairs are accepted; the last
    /// registration wins (consistent with Python dict semantics).
    pub fn insert(&mut self, method: &str, path: &str) -> HandlerId {
        let id = handler_id(method, path);
        self.entries.push(RouteEntry {
            method: method.to_ascii_uppercase(),
            path: path.to_owned(),
            handler_id: id,
        });
        id
    }

    /// Consume the table and produce the router-ready entries.
    pub fn into_entries(self) -> Vec<RouteEntry> {
        self.entries
    }
}
```

### 6.2 `src/router.rs`

```rust
//! matchit-based native router.
//!
//! Each HTTP method gets its own `matchit::Router<HandlerId>` so that two routes
//! on different methods with the same path pattern do not conflict inside matchit.
//! The outer lookup is a small array scan (up to ~9 methods), which is cheaper
//! than a HashMap for the expected number of HTTP verbs.

use std::collections::HashMap;

use matchit::Router as MatchitRouter;
use tracing::debug;

use crate::route_table::{HandlerId, RouteEntry};

/// Error returned by `NativeRouter::lookup`.
#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("no route matched: method={method} path={path}")]
    NotFound { method: String, path: String },
    #[error("method not allowed for path {path}")]
    MethodNotAllowed { path: String },
}

/// The result of a successful route match.
#[derive(Debug)]
pub struct RouteMatch<'a> {
    /// Stable handler ID; this is what the batch collector embeds in `RequestInfo`.
    pub handler_id: HandlerId,
    /// Path parameters extracted by matchit (e.g., `id` from `/:id`).
    pub params: matchit::Params<'a, 'a>,
}

/// A compiled, immutable router built from a `RouteTable`.
///
/// Thread-safe: wrap in `Arc<NativeRouter>` and share across Tokio tasks.
pub struct NativeRouter {
    /// One matchit router per HTTP method.
    per_method: HashMap<String, MatchitRouter<HandlerId>>,
}

impl NativeRouter {
    /// Build a `NativeRouter` from the collected route entries.
    ///
    /// # Errors
    /// Returns an error if any path pattern is invalid per matchit's rules.
    pub fn from_entries(
        entries: Vec<RouteEntry>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut per_method: HashMap<String, MatchitRouter<HandlerId>> = HashMap::new();

        for entry in entries {
            let mr = per_method.entry(entry.method.clone()).or_default();
            mr.insert(entry.path.as_str(), entry.handler_id)?;
            debug!(
                method = %entry.method,
                path  = %entry.path,
                id    = entry.handler_id,
                "registered route"
            );
        }

        Ok(Self { per_method })
    }

    /// Look up a request by method and path.
    ///
    /// Returns `RouterError::NotFound` if no method router matches.
    /// Returns `RouterError::MethodNotAllowed` is reserved for future use
    /// (when a path matches another method but not this one).
    ///
    /// This function is designed to run in <1µs for typical route tables
    /// (the matchit trie lookup is O(path length)).
    #[inline]
    pub fn lookup<'r>(
        &'r self,
        method: &str,
        path: &'r str,
    ) -> Result<RouteMatch<'r>, RouterError> {
        let method_upper = method.to_ascii_uppercase();
        let mr = self.per_method.get(method_upper.as_str()).ok_or_else(|| {
            RouterError::NotFound {
                method: method_upper.clone(),
                path: path.to_owned(),
            }
        })?;

        let matched = mr.at(path).map_err(|_| RouterError::NotFound {
            method: method_upper,
            path: path.to_owned(),
        })?;

        Ok(RouteMatch {
            handler_id: *matched.value,
            params: matched.params,
        })
    }
}
```

**Design notes:**

- `matchit` is the same router used by Axum. Its trie-based lookup is O(path length) with no allocations on the hot path for parameter extraction.
- One router per HTTP method avoids matchit's limitation that it cannot store duplicate paths. The method-level dispatch is a `HashMap::get` (one hash computation) which is negligible.
- `HandlerId` is `u64` throughout. No `String` allocation occurs after startup.
- `lookup` is `#[inline]` to encourage the compiler to merge it with the collector's hot path.

**Pass/fail criteria:**
- `cargo bench --package vertebra-http --bench server_throughput` shows router lookup p99 < 1µs.
- `test_router.rs::test_static_route`, `test_parametric_route`, `test_method_not_matched`, `test_unknown_path` all pass.

---

## 7. Phase 3: Batch Collector & Dispatch

### 7.1 `src/policy.rs`

```rust
//! Drain policy for the batch collector.
//!
//! The initial implementation uses a fixed policy (64 requests or 1ms timeout).
//! The adaptive policy is scaffolded here but activates only after Saga 3
//! benchmarks confirm baseline behaviour.

use std::time::Duration;

/// Policy determining when a batch should be drained.
#[derive(Debug, Clone)]
pub enum DrainPolicy {
    /// Drain when `size` requests accumulate OR `timeout` elapses, whichever is first.
    Fixed {
        size: usize,
        timeout: Duration,
    },
    /// Adaptive: tighten the size limit under low load, relax it under high load.
    /// The `base_size` and `base_timeout` are the starting point; `load_factor`
    /// is the current estimated load in [0.0, 1.0].
    Adaptive {
        min_size: usize,
        max_size: usize,
        base_timeout: Duration,
        /// Current effective size (updated by the collector loop).
        current_size: usize,
    },
}

impl DrainPolicy {
    /// Create the default fixed policy used in Chapter 5.
    pub fn default_fixed() -> Self {
        Self::Fixed {
            size: 64,
            timeout: Duration::from_millis(1),
        }
    }

    /// Return the effective batch size cap for the current policy state.
    pub fn batch_size(&self) -> usize {
        match self {
            Self::Fixed { size, .. } => *size,
            Self::Adaptive { current_size, .. } => *current_size,
        }
    }

    /// Return the drain timeout for the current policy state.
    pub fn timeout(&self) -> Duration {
        match self {
            Self::Fixed { timeout, .. } => *timeout,
            Self::Adaptive { base_timeout, .. } => *base_timeout,
        }
    }

    /// Adjust the adaptive policy based on the number of requests seen in the
    /// last window. No-op for the fixed policy.
    ///
    /// Called by the collector after each drain.
    pub fn adapt(&mut self, requests_in_window: usize) {
        if let Self::Adaptive {
            min_size,
            max_size,
            current_size,
            ..
        } = self
        {
            // Simple linear scaling: aim for the batch to fill in ~75% of max.
            let target = (requests_in_window * 4 / 3).clamp(*min_size, *max_size);
            *current_size = target;
        }
    }
}

impl Default for DrainPolicy {
    fn default() -> Self {
        Self::default_fixed()
    }
}
```

### 7.2 `src/collector.rs`

The collector is the hottest component in Chapter 5. It receives decoded requests from `response.rs`'s dispatch path and groups them before writing to the ring buffer.

```rust
//! Batch collector: aggregates incoming HTTP requests into `BatchArena` slots.
//!
//! ## Drain conditions
//!
//! A batch is drained (pushed to the dispatcher) when either:
//! 1. The slot count reaches `policy.batch_size()`, or
//! 2. `policy.timeout()` has elapsed since the first request in the current batch.
//!
//! ## Memory model
//!
//! Each request is serialised into a `RequestSlot` (fixed-size on-stack value).
//! The collector does not heap-allocate per request; it writes into pre-allocated
//! arena memory obtained from `vertebra-core`'s `BatchArena`.
//!
//! ## Thread model
//!
//! One `BatchCollector` per OS thread (pinned to the Tokio blocking thread pool).
//! Multiple Tokio tasks on the same thread contend for the same collector via a
//! `tokio::sync::mpsc` channel. This avoids cross-thread contention for the common
//! single-socket case. Multi-socket or multi-NUMA configurations can spawn multiple
//! collectors, each feeding a separate ring buffer shard.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::dispatcher::Dispatcher;
use crate::policy::DrainPolicy;

/// A single request item queued for batching.
///
/// This is a lightweight descriptor. The actual body bytes (if any) are stored
/// in a `Bytes` buffer owned by the Hyper connection; they are referenced here
/// by index/offset within the arena once written.
#[derive(Debug)]
pub struct PendingRequest {
    /// Resolved handler ID from the router.
    pub handler_id: u64,
    /// HTTP method as a compact byte tag (1 byte).
    pub method_tag: u8,
    /// Request URI path (borrowed lifetime must exceed the batch).
    pub path: String,
    /// Query string, if present.
    pub query: Option<String>,
    /// Serialised request headers (MessagePack).
    pub headers_mp: bytes::Bytes,
    /// Body bytes (may be empty).
    pub body: bytes::Bytes,
    /// One-shot channel for sending the response back to the Hyper task.
    pub reply: tokio::sync::oneshot::Sender<PendingResponse>,
}

/// A completed response returned from a Python worker through the ring buffer.
#[derive(Debug)]
pub struct PendingResponse {
    pub status: u16,
    pub headers_mp: bytes::Bytes,
    pub body: bytes::Bytes,
}

/// A batch ready to be pushed to the dispatcher.
pub struct Batch {
    pub requests: Vec<PendingRequest>,
}

/// Encodes an HTTP method as a single byte tag for compact storage.
pub fn method_tag(method: &hyper::Method) -> u8 {
    match method.as_str() {
        "GET"     => 0,
        "POST"    => 1,
        "PUT"     => 2,
        "DELETE"  => 3,
        "PATCH"   => 4,
        "HEAD"    => 5,
        "OPTIONS" => 6,
        "TRACE"   => 7,
        "CONNECT" => 8,
        _         => 255,
    }
}

/// The batch collector task.
///
/// Spawn one of these per worker thread or per CPU. It receives `PendingRequest`
/// values over a channel, accumulates them, and drains when the policy fires.
pub struct BatchCollector {
    rx: mpsc::Receiver<PendingRequest>,
    dispatcher: Arc<Dispatcher>,
    policy: DrainPolicy,
}

impl BatchCollector {
    /// Create a new collector and return the sender half of the channel.
    pub fn new(
        dispatcher: Arc<Dispatcher>,
        policy: DrainPolicy,
        channel_capacity: usize,
    ) -> (Self, mpsc::Sender<PendingRequest>) {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (
            Self {
                rx,
                dispatcher,
                policy,
            },
            tx,
        )
    }

    /// Run the collector loop. This is `async` but is designed to be spawned
    /// as a dedicated Tokio task that runs until the channel is closed.
    pub async fn run(mut self) {
        let mut pending: Vec<PendingRequest> = Vec::with_capacity(self.policy.batch_size());
        let mut deadline: Option<Instant> = None;
        let timeout = self.policy.timeout();

        loop {
            // Compute the remaining time until the current drain deadline.
            let sleep = match deadline {
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        Duration::ZERO
                    } else {
                        d - now
                    }
                }
                None => Duration::from_secs(3600), // effectively no timeout
            };

            tokio::select! {
                biased; // prefer incoming requests over timer expiry

                // New request arrived.
                maybe_req = self.rx.recv() => {
                    match maybe_req {
                        None => {
                            // Channel closed; drain remaining and exit.
                            if !pending.is_empty() {
                                self.drain(&mut pending).await;
                            }
                            debug!("BatchCollector shutting down");
                            return;
                        }
                        Some(req) => {
                            if deadline.is_none() {
                                // First request in a new batch: start the clock.
                                deadline = Some(Instant::now() + timeout);
                            }
                            pending.push(req);
                            trace!(depth = pending.len(), "queued request");

                            if pending.len() >= self.policy.batch_size() {
                                self.drain(&mut pending).await;
                                deadline = None;
                            }
                        }
                    }
                }

                // Timeout expired: drain whatever we have.
                _ = tokio::time::sleep(sleep), if deadline.is_some() => {
                    if !pending.is_empty() {
                        self.drain(&mut pending).await;
                    }
                    deadline = None;
                }
            }
        }
    }

    async fn drain(&mut self, pending: &mut Vec<PendingRequest>) {
        let count = pending.len();
        debug!(count, "draining batch");
        let batch = Batch {
            requests: std::mem::replace(pending, Vec::with_capacity(self.policy.batch_size())),
        };
        self.dispatcher.dispatch(batch).await;
        self.policy.adapt(count);
    }
}
```

**Design notes:**

- `tokio::select! { biased; ... }` ensures that if both a new request and the timeout are ready simultaneously, the new request is processed first. This reduces the number of artificially small batches under moderate load.
- The `deadline` is set on the arrival of the *first* request, not when the collector starts. This means an idle server never fires spurious drain events.
- `Dispatcher::dispatch` is `async` because in the MVP it uses `tokio::sync::mpsc` to hand off to the ring writer task. In a later chapter the ring write will be a plain synchronous write to pre-mapped memory, making it effectively zero-overhead.
- `policy.adapt(count)` is a no-op for `Fixed` policy; the hook is in place for the adaptive path.

**Pass/fail criteria:**
- `test_collector.rs::test_drain_by_size` — send 64 requests without pausing, verify one batch of exactly 64.
- `test_collector.rs::test_drain_by_timeout` — send 10 requests, wait 2ms, verify one batch of 10.
- `test_collector.rs::test_no_spurious_drain` — create a collector, wait 5ms without sending, verify no drain occurred.
- `test_collector.rs::test_partial_batch_on_shutdown` — close the sender with 5 requests pending, verify they are drained.

### 7.3 `src/dispatcher.rs`

```rust
//! Dispatcher: writes filled batches into the MPMC ring buffer for Python workers.
//!
//! In Chapter 5 the dispatcher serialises the batch (MessagePack header + body)
//! and pushes slices to `vertebra-core`'s async ring. Python workers on the
//! other side call `get_batch()` via the PyO3 FFI (Chapter 7).
//!
//! The `Dispatcher` also owns the response routing table: it maps correlation IDs
//! back to the `oneshot::Sender<PendingResponse>` so Hyper tasks can complete.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tracing::{debug, error};

use crate::collector::{Batch, PendingResponse};

/// Dispatcher state shared between the write task and the response router.
struct Inner {
    /// In-flight correlation ID → reply channel.
    pending: Mutex<HashMap<u64, oneshot::Sender<PendingResponse>>>,
    /// Next monotonic correlation ID.
    next_id: std::sync::atomic::AtomicU64,
}

/// The dispatcher.
///
/// In Chapter 5 the "ring buffer" is simulated by a simple in-memory queue
/// (`tokio::sync::mpsc`). Chapter 6 replaces this with the real shared-memory
/// MPMC ring from `vertebra-core`. The public API does not change.
pub struct Dispatcher {
    inner: Arc<Inner>,
    /// Channel to the ring-writer task. Bounded to apply back-pressure.
    ring_tx: tokio::sync::mpsc::Sender<SerializedBatch>,
}

/// A batch serialised to bytes, ready to write to the ring buffer.
pub struct SerializedBatch {
    pub correlation_ids: Vec<u64>,
    pub payload: bytes::Bytes,
}

impl Dispatcher {
    /// Create a dispatcher and return both the dispatcher and the ring-reader
    /// (which the test harness or the Python FFI layer will consume).
    pub fn new(ring_capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<SerializedBatch>) {
        let (ring_tx, ring_rx) = tokio::sync::mpsc::channel(ring_capacity);
        let dispatcher = Self {
            inner: Arc::new(Inner {
                pending: Mutex::new(HashMap::new()),
                next_id: std::sync::atomic::AtomicU64::new(1),
            }),
            ring_tx,
        };
        (dispatcher, ring_rx)
    }

    /// Dispatch a batch to the ring buffer.
    ///
    /// Assigns a correlation ID to each request, serialises the batch, and
    /// sends it to the ring-writer task.
    pub async fn dispatch(&self, batch: Batch) {
        use std::sync::atomic::Ordering;

        let mut ids = Vec::with_capacity(batch.requests.len());
        let mut pending = self.inner.pending.lock();

        let mut payload_parts: Vec<bytes::Bytes> = Vec::new();

        for req in batch.requests {
            let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
            ids.push(id);

            // Register the reply channel before sending to the ring.
            pending.insert(id, req.reply);

            // Serialise the request to MessagePack (simplified: header + body).
            // The real format follows `vertebra-core`'s `RequestHeader` wire layout.
            let mut header = [0u8; 16];
            header[0..8].copy_from_slice(&id.to_le_bytes());
            header[8..12].copy_from_slice(&0u32.to_le_bytes()); // flags
            let body_len = req.body.len() as u32;
            header[12..16].copy_from_slice(&body_len.to_le_bytes());

            payload_parts.push(bytes::Bytes::copy_from_slice(&header));
            payload_parts.push(req.body);
        }

        drop(pending); // release lock before async send

        // Concatenate all parts into a single Bytes (zero-copy chain in practice).
        let payload = payload_parts
            .into_iter()
            .fold(bytes::BytesMut::new(), |mut acc, b| {
                acc.extend_from_slice(&b);
                acc
            })
            .freeze();

        let serialized = SerializedBatch {
            correlation_ids: ids,
            payload,
        };

        if let Err(e) = self.ring_tx.send(serialized).await {
            error!(error = %e, "ring buffer full, dropping batch");
        }
    }

    /// Complete a request with a response from a Python worker.
    ///
    /// Called by the response routing task when it reads a `ResponseHeader`
    /// from the outbound ring buffer.
    pub fn complete(&self, correlation_id: u64, response: PendingResponse) {
        let mut pending = self.inner.pending.lock();
        if let Some(tx) = pending.remove(&correlation_id) {
            let _ = tx.send(response);
        } else {
            debug!(id = correlation_id, "no pending request for correlation ID");
        }
    }
}
```

**Design notes:**

- `parking_lot::Mutex` is used for the pending map because it is faster than `std::sync::Mutex` for short critical sections and does not need `async` awareness (the lock is never held across an `await` point).
- The `ring_tx` channel acts as a stand-in for the real MPMC ring. Chapter 6 (FFI Bridge) will replace the `mpsc::Receiver<SerializedBatch>` with a `vertebra-core` async ring consumer. The `Dispatcher::dispatch` signature does not change.
- Correlation IDs use `AtomicU64::fetch_add(Relaxed)` because monotonicity per-thread is sufficient; the matching side only needs uniqueness, not ordering.

**Pass/fail criteria:**
- `test_dispatcher.rs::test_dispatch_roundtrip` — send one batch, read from ring receiver, verify payload contains the 16-byte header with correct `id`.
- `test_dispatcher.rs::test_complete_unblocks_hyper` — call `dispatch`, then `complete` with the same ID, verify the `oneshot` receiver fires.
- `test_dispatcher.rs::test_back_pressure` — fill the ring channel to capacity, verify `dispatch` does not panic (it logs and drops).

---

## 8. Phase 4: Response Handling

### 8.1 `src/response.rs`

```rust
//! Response construction and the top-level request dispatch function.
//!
//! `handle` is the `service_fn` target for both plain and TLS connections.
//! It:
//!   1. Reads method + path from the Hyper request.
//!   2. Looks up the route in `NativeRouter`.
//!   3. Reads the full request body (buffered for now; streaming is Chapter 9).
//!   4. Encodes headers as MessagePack.
//!   5. Sends a `PendingRequest` to the `BatchCollector` via the `Sender`.
//!   6. Awaits the `oneshot` response from the `Dispatcher`.
//!   7. Constructs a `hyper::Response` from `PendingResponse`.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::Request;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::collector::{method_tag, PendingRequest, PendingResponse};
use crate::router::RouterError;
use crate::server::ServerState;

type HyperResponse = Response<Full<Bytes>>;

/// Top-level request handler; called for every accepted HTTP connection.
pub async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> Result<HyperResponse, Infallible> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path();

    // 1. Route lookup.
    let route_match = match state.router.lookup(method.as_str(), path) {
        Ok(m) => m,
        Err(RouterError::NotFound { .. }) => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"Not Found")))
                .unwrap());
        }
        Err(RouterError::MethodNotAllowed { .. }) => {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(Bytes::from_static(b"Method Not Allowed")))
                .unwrap());
        }
    };

    let handler_id = route_match.handler_id;
    debug!(handler_id, method = %method, path, "routed request");

    // 2. Collect body (buffered in Chapter 5; streaming deferred to Ch. 9).
    let (parts, body) = req.into_parts();
    let body_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            warn!(error = %e, "failed to read request body");
            return Ok(error_response(StatusCode::BAD_REQUEST, "failed to read body"));
        }
    };

    // 3. Encode headers as MessagePack (simplified: key=value pairs).
    let headers_mp = encode_headers_mp(&parts.headers);

    // 4. Create reply channel.
    let (reply_tx, reply_rx) = oneshot::channel::<PendingResponse>();

    let pending = PendingRequest {
        handler_id,
        method_tag: method_tag(&method),
        path: path.to_owned(),
        query: uri.query().map(str::to_owned),
        headers_mp,
        body: body_bytes,
        reply: reply_tx,
    };

    // 5. Send to collector. If the collector is gone (server shutting down),
    //    return 503 immediately.
    if state.dispatcher.ring_tx_clone().send(pending).await.is_err() {
        return Ok(error_response(StatusCode::SERVICE_UNAVAILABLE, "server shutting down"));
    }

    // 6. Await response.
    match reply_rx.await {
        Ok(resp) => Ok(build_response(resp)),
        Err(_) => {
            warn!(handler_id, "worker dropped response channel");
            Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "worker error",
            ))
        }
    }
}

fn encode_headers_mp(headers: &http::HeaderMap) -> Bytes {
    // Produce a minimal MessagePack map: {name: value, ...}
    // In the MVP we emit a flat byte vector of (name_len u8)(name_bytes)(value_len u16)(value_bytes).
    // Chapter 7 replaces this with a proper rmp-serde map when the PyO3 bridge reads it.
    let mut buf = Vec::new();
    for (name, value) in headers.iter() {
        let n = name.as_str().as_bytes();
        let v = value.as_bytes();
        buf.push(n.len() as u8);
        buf.extend_from_slice(n);
        let vlen = v.len() as u16;
        buf.extend_from_slice(&vlen.to_le_bytes());
        buf.extend_from_slice(v);
    }
    Bytes::from(buf)
}

fn build_response(resp: PendingResponse) -> HyperResponse {
    Response::builder()
        .status(resp.status)
        .body(Full::new(resp.body))
        .unwrap_or_else(|_| error_response(StatusCode::INTERNAL_SERVER_ERROR, "response build error"))
}

fn error_response(status: StatusCode, msg: &'static str) -> HyperResponse {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from_static(msg.as_bytes())))
        .unwrap()
}
```

**Note on `ring_tx_clone`:** The `Dispatcher` struct needs a method that exposes a `Sender<PendingRequest>` clone to `response.rs`. Add to `Dispatcher`:

```rust
// In dispatcher.rs
pub fn ring_tx_clone(&self) -> tokio::sync::mpsc::Sender<PendingRequest> {
    // NOTE: In Chapter 5 the ServerState carries a Sender<PendingRequest>
    // for the collector, separate from the Sender<SerializedBatch> ring_tx.
    // See the integration wiring in main.rs below.
    unimplemented!("wired in main.rs via ServerState")
}
```

The actual wiring is done through `ServerState`, which holds both the `NativeRouter` and a `mpsc::Sender<PendingRequest>` to the collector. `response.rs` sends to the collector; the collector sends to the dispatcher.

**Pass/fail criteria:**
- `test_server.rs::test_http11_hello` — start server, GET `/hello`, get 200.
- `test_server.rs::test_http11_not_found` — GET `/nonexistent`, get 404.
- `test_server.rs::test_http11_body_echo` — POST `/echo` with body, get 200 with same body.

---

### 8.2 `src/lib.rs`

```rust
//! vertebra-http: Native HTTP server, router, and batch collector.

pub mod collector;
pub mod config;
pub mod dispatcher;
pub mod policy;
pub mod response;
pub mod router;
pub mod route_table;
pub mod server;

#[cfg(feature = "tls")]
pub mod tls;
```

### 8.3 `src/main.rs`

```rust
//! `vertebra-server` binary entrypoint.
//!
//! Reads configuration (from environment or CLI), builds the route table,
//! constructs the batch collector and dispatcher, then runs the accept loop.
//!
//! In Chapter 5 the route table is hardcoded to a single `/hello` GET route
//! for bootstrap testing. The real route registration from Python's
//! `VertebraApp` is wired in Chapter 7 (FFI Bridge).

use std::sync::Arc;

use tokio::signal;
use tracing::info;
use tracing_subscriber::EnvFilter;

use vertebra_http::{
    collector::{BatchCollector, PendingRequest, PendingResponse},
    config::ServerConfig,
    dispatcher::Dispatcher,
    policy::DrainPolicy,
    route_table::RouteTable,
    router::NativeRouter,
    server,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = Arc::new(ServerConfig::default());

    // --- Route table (hardcoded for Chapter 5 bootstrap) ---
    let mut table = RouteTable::new();
    let hello_id = table.insert("GET", "/hello");
    let echo_id  = table.insert("POST", "/echo");
    info!(hello_id, echo_id, "route table built");

    let router = Arc::new(NativeRouter::from_entries(table.into_entries())?);

    // --- Collector + Dispatcher wiring ---
    let (dispatcher, mut ring_rx) = Dispatcher::new(256);
    let dispatcher = Arc::new(dispatcher);

    let (collector, collector_tx) = BatchCollector::new(
        Arc::clone(&dispatcher),
        DrainPolicy::default_fixed(),
        1024, // channel capacity
    );

    // Spawn the collector loop.
    tokio::spawn(collector.run());

    // Spawn the stub ring-reader (Chapter 7 replaces this with real Python FFI).
    let dispatcher_clone = Arc::clone(&dispatcher);
    tokio::spawn(async move {
        while let Some(batch) = ring_rx.recv().await {
            // Stub: echo each request back with status 200.
            for id in batch.correlation_ids {
                dispatcher_clone.complete(
                    id,
                    PendingResponse {
                        status: 200,
                        headers_mp: bytes::Bytes::new(),
                        body: bytes::Bytes::from_static(b"ok"),
                    },
                );
            }
        }
    });

    // --- Start server ---
    let shutdown = async {
        signal::ctrl_c().await.expect("failed to listen for ctrl-c");
        info!("ctrl-c received");
    };

    // ServerState needs both the router and the collector sender.
    // We thread the collector_tx through the state so response.rs can send
    // PendingRequests directly to the collector.
    //
    // To keep the API clean, we wrap `server::run` so it accepts the
    // collector_tx explicitly. See server.rs for the updated signature.
    server::run_with_collector(config, router, dispatcher, collector_tx, shutdown).await?;

    Ok(())
}
```

**Note:** `server::run_with_collector` is a thin wrapper around `run` that stores the `collector_tx` in `ServerState`. The updated `ServerState` in `server.rs` becomes:

```rust
pub struct ServerState {
    pub router: Arc<NativeRouter>,
    pub dispatcher: Arc<Dispatcher>,
    pub config: Arc<ServerConfig>,
    pub collector_tx: tokio::sync::mpsc::Sender<PendingRequest>,
}
```

This pattern keeps `response.rs` simple: it receives a `&ServerState` and calls `state.collector_tx.send(pending).await`.

---

## 9. Test Specifications

### 9.1 `tests/test_server.rs`

```rust
//! Integration tests for the HTTP server.
//!
//! These tests start a real Tokio listener on a random port and make
//! HTTP/1.1 requests using `reqwest`.

use std::sync::Arc;
use tokio::sync::oneshot;
use vertebra_http::{
    collector::{BatchCollector, PendingResponse},
    config::ServerConfig,
    dispatcher::Dispatcher,
    policy::DrainPolicy,
    route_table::RouteTable,
    router::NativeRouter,
    server,
};

/// Spawn a test server on a random port. Returns the bound address and a
/// shutdown sender.
async fn spawn_test_server() -> (std::net::SocketAddr, oneshot::Sender<()>) {
    let mut config = ServerConfig::default();
    // Port 0: OS assigns a free port.
    config.bind = "127.0.0.1:0".parse().unwrap();
    let config = Arc::new(config);

    let mut table = RouteTable::new();
    table.insert("GET", "/hello");
    table.insert("POST", "/echo");

    let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());
    let (dispatcher, mut ring_rx) = Dispatcher::new(64);
    let dispatcher = Arc::new(dispatcher);
    let (collector, collector_tx) = BatchCollector::new(
        Arc::clone(&dispatcher),
        DrainPolicy::default_fixed(),
        128,
    );

    tokio::spawn(collector.run());

    let dispatcher_clone = Arc::clone(&dispatcher);
    tokio::spawn(async move {
        while let Some(batch) = ring_rx.recv().await {
            for id in batch.correlation_ids {
                dispatcher_clone.complete(
                    id,
                    PendingResponse {
                        status: 200,
                        headers_mp: bytes::Bytes::new(),
                        body: bytes::Bytes::from_static(b"hello"),
                    },
                );
            }
        }
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Bind a temporary listener to find a free port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    config.bind = addr; // re-assign the discovered port
    // (In real test helpers we'd pass the listener in; simplified here.)

    tokio::spawn(async move {
        server::run_with_collector(
            config,
            router,
            dispatcher,
            collector_tx,
            async move { let _ = shutdown_rx.await; },
        )
        .await
        .unwrap();
    });

    // Brief yield to allow the server to bind.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (addr, shutdown_tx)
}

#[tokio::test]
async fn test_http11_hello() {
    let (addr, _shutdown) = spawn_test_server().await;
    let resp = reqwest::get(format!("http://{}/hello", addr))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_http11_not_found() {
    let (addr, _shutdown) = spawn_test_server().await;
    let resp = reqwest::get(format!("http://{}/nonexistent", addr))
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_server_accepts_multiple_requests() {
    let (addr, _shutdown) = spawn_test_server().await;
    let client = reqwest::Client::new();
    for _ in 0..10 {
        let resp = client
            .get(format!("http://{}/hello", addr))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
}
```

**Pass criteria:** all three tests green.

### 9.2 `tests/test_router.rs`

```rust
//! Unit tests for the native router.

use vertebra_http::{
    route_table::{handler_id, RouteTable},
    router::NativeRouter,
};

fn build_router(routes: &[(&str, &str)]) -> NativeRouter {
    let mut table = RouteTable::new();
    for (method, path) in routes {
        table.insert(method, path);
    }
    NativeRouter::from_entries(table.into_entries()).unwrap()
}

#[test]
fn test_static_route() {
    let router = build_router(&[("GET", "/hello")]);
    let m = router.lookup("GET", "/hello").unwrap();
    assert_eq!(m.handler_id, handler_id("GET", "/hello"));
}

#[test]
fn test_parametric_route() {
    let router = build_router(&[("GET", "/users/:id")]);
    let m = router.lookup("GET", "/users/42").unwrap();
    assert_eq!(m.params.get("id"), Some("42"));
}

#[test]
fn test_method_not_matched() {
    let router = build_router(&[("GET", "/hello")]);
    // POST is not registered; should return NotFound.
    assert!(router.lookup("POST", "/hello").is_err());
}

#[test]
fn test_unknown_path() {
    let router = build_router(&[("GET", "/hello")]);
    assert!(router.lookup("GET", "/world").is_err());
}

#[test]
fn test_handler_id_stability() {
    // Same method+path must always produce the same ID.
    let id1 = handler_id("GET", "/api/v1/users");
    let id2 = handler_id("GET", "/api/v1/users");
    assert_eq!(id1, id2);
}

#[test]
fn test_handler_id_method_sensitivity() {
    // Different methods → different IDs.
    let get_id  = handler_id("GET",  "/resource");
    let post_id = handler_id("POST", "/resource");
    assert_ne!(get_id, post_id);
}

#[test]
fn test_case_insensitive_method_lookup() {
    let router = build_router(&[("GET", "/hello")]);
    // The router normalises methods to uppercase.
    let m = router.lookup("get", "/hello").unwrap();
    assert_eq!(m.handler_id, handler_id("GET", "/hello"));
}

#[test]
fn test_multi_segment_param() {
    let router = build_router(&[("DELETE", "/orgs/:org/repos/:repo")]);
    let m = router.lookup("DELETE", "/orgs/acme/repos/widget").unwrap();
    assert_eq!(m.params.get("org"), Some("acme"));
    assert_eq!(m.params.get("repo"), Some("widget"));
}
```

**Pass criteria:** all eight tests green.

### 9.3 `tests/test_collector.rs`

```rust
//! Unit tests for the batch collector.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use vertebra_http::{
    collector::{BatchCollector, PendingRequest, PendingResponse},
    dispatcher::Dispatcher,
    policy::DrainPolicy,
};

fn dummy_request(handler_id: u64) -> (PendingRequest, oneshot::Receiver<PendingResponse>) {
    let (tx, rx) = oneshot::channel();
    let req = PendingRequest {
        handler_id,
        method_tag: 0,
        path: "/hello".to_owned(),
        query: None,
        headers_mp: bytes::Bytes::new(),
        body: bytes::Bytes::new(),
        reply: tx,
    };
    (req, rx)
}

#[tokio::test]
async fn test_drain_by_size() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
    let dispatcher = Arc::new(dispatcher);

    let policy = DrainPolicy::Fixed {
        size: 4,
        timeout: Duration::from_secs(60), // effectively no timeout
    };
    let (collector, tx) = BatchCollector::new(Arc::clone(&dispatcher), policy, 64);
    tokio::spawn(collector.run());

    // Send exactly 4 requests.
    for i in 0..4u64 {
        let (req, _rx) = dummy_request(i);
        tx.send(req).await.unwrap();
    }

    // The batch should drain immediately on the 4th.
    let batch = tokio::time::timeout(Duration::from_millis(200), ring_rx.recv())
        .await
        .expect("drain did not fire")
        .unwrap();

    assert_eq!(batch.correlation_ids.len(), 4);
}

#[tokio::test]
async fn test_drain_by_timeout() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
    let dispatcher = Arc::new(dispatcher);

    let policy = DrainPolicy::Fixed {
        size: 64,
        timeout: Duration::from_millis(5),
    };
    let (collector, tx) = BatchCollector::new(Arc::clone(&dispatcher), policy, 64);
    tokio::spawn(collector.run());

    // Send 3 requests (below the size threshold).
    for i in 0..3u64 {
        let (req, _rx) = dummy_request(i);
        tx.send(req).await.unwrap();
    }

    // Wait long enough for the timeout to fire.
    let batch = tokio::time::timeout(Duration::from_millis(100), ring_rx.recv())
        .await
        .expect("timeout drain did not fire")
        .unwrap();

    assert_eq!(batch.correlation_ids.len(), 3);
}

#[tokio::test]
async fn test_no_spurious_drain() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
    let dispatcher = Arc::new(dispatcher);

    let policy = DrainPolicy::Fixed {
        size: 64,
        timeout: Duration::from_millis(5),
    };
    let (collector, _tx) = BatchCollector::new(Arc::clone(&dispatcher), policy, 64);
    tokio::spawn(collector.run());

    // Send nothing. Wait 20ms. No drain should occur.
    let result = tokio::time::timeout(Duration::from_millis(20), ring_rx.recv()).await;
    assert!(result.is_err(), "no drain should fire with zero requests");
}

#[tokio::test]
async fn test_partial_batch_on_shutdown() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
    let dispatcher = Arc::new(dispatcher);

    let policy = DrainPolicy::Fixed {
        size: 64,
        timeout: Duration::from_secs(60),
    };
    let (collector, tx) = BatchCollector::new(Arc::clone(&dispatcher), policy, 64);
    tokio::spawn(collector.run());

    for i in 0..5u64 {
        let (req, _rx) = dummy_request(i);
        tx.send(req).await.unwrap();
    }

    // Close the sender; collector should flush remaining.
    drop(tx);

    let batch = tokio::time::timeout(Duration::from_millis(200), ring_rx.recv())
        .await
        .expect("flush on shutdown did not fire")
        .unwrap();

    assert_eq!(batch.correlation_ids.len(), 5);
}
```

**Pass criteria:** all four tests green.

### 9.4 `tests/test_dispatcher.rs`

```rust
//! Unit tests for the dispatcher.

use std::sync::Arc;
use std::time::Duration;

use vertebra_http::{
    collector::{Batch, PendingRequest, PendingResponse},
    dispatcher::Dispatcher,
};
use tokio::sync::oneshot;

fn make_batch(count: usize) -> (Batch, Vec<oneshot::Receiver<PendingResponse>>) {
    let mut requests = Vec::new();
    let mut receivers = Vec::new();
    for i in 0..count as u64 {
        let (tx, rx) = oneshot::channel();
        requests.push(PendingRequest {
            handler_id: i,
            method_tag: 0,
            path: "/".to_owned(),
            query: None,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::new(),
            reply: tx,
        });
        receivers.push(rx);
    }
    (Batch { requests }, receivers)
}

#[tokio::test]
async fn test_dispatch_roundtrip() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
    let dispatcher = Arc::new(dispatcher);

    let (batch, _receivers) = make_batch(2);
    dispatcher.dispatch(batch).await;

    let serialized = tokio::time::timeout(Duration::from_millis(100), ring_rx.recv())
        .await
        .expect("dispatch did not produce ring entry")
        .unwrap();

    // Should have 2 correlation IDs.
    assert_eq!(serialized.correlation_ids.len(), 2);
    // Each request produces a 16-byte header + 0-byte body.
    assert_eq!(serialized.payload.len(), 2 * 16);
}

#[tokio::test]
async fn test_complete_unblocks_hyper() {
    let (dispatcher, mut ring_rx) = Dispatcher::new(16);
    let dispatcher = Arc::new(dispatcher);

    let (batch, mut receivers) = make_batch(1);
    dispatcher.dispatch(batch).await;

    let serialized = ring_rx.recv().await.unwrap();
    let id = serialized.correlation_ids[0];

    dispatcher.complete(
        id,
        PendingResponse {
            status: 201,
            headers_mp: bytes::Bytes::new(),
            body: bytes::Bytes::from_static(b"created"),
        },
    );

    let resp = tokio::time::timeout(Duration::from_millis(100), receivers.remove(0))
        .await
        .expect("oneshot did not fire")
        .unwrap();

    assert_eq!(resp.status, 201);
    assert_eq!(resp.body.as_ref(), b"created");
}

#[tokio::test]
async fn test_back_pressure() {
    // Ring channel of capacity 1.
    let (dispatcher, _ring_rx) = Dispatcher::new(1);
    let dispatcher = Arc::new(dispatcher);

    // First dispatch fills the channel.
    let (batch1, _) = make_batch(1);
    dispatcher.dispatch(batch1).await;

    // Second dispatch should log and drop, NOT panic.
    let (batch2, _) = make_batch(1);
    dispatcher.dispatch(batch2).await; // must not panic or hang
}
```

**Pass criteria:** all three tests green.

---

## 10. Benchmark Specification

### `benches/server_throughput.rs`

```rust
//! Throughput benchmark for the `vertebra-http` server.
//!
//! Target: >100,000 req/s on a single machine with HTTP/1.1 keep-alive.
//!
//! Run with:
//!   cd src/rs && cargo bench --package vertebra-http --bench server_throughput

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use std::time::Duration;

fn bench_router_lookup(c: &mut Criterion) {
    use vertebra_http::{
        route_table::{handler_id, RouteTable},
        router::NativeRouter,
    };

    let mut table = RouteTable::new();
    for i in 0..100 {
        table.insert("GET", &format!("/api/v1/resource/{}", i));
    }
    let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());

    let mut group = c.benchmark_group("router");
    group.throughput(Throughput::Elements(1));

    group.bench_function("lookup_static_100_routes", |b| {
        b.iter(|| {
            let _ = router.lookup("GET", "/api/v1/resource/50");
        });
    });

    group.finish();
}

fn bench_server_throughput(c: &mut Criterion) {
    // This benchmark starts a real server and measures end-to-end req/s.
    // We use a Tokio runtime inside the benchmark for proper async measurement.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let addr = rt.block_on(async {
        use vertebra_http::{
            collector::{BatchCollector, PendingResponse},
            config::ServerConfig,
            dispatcher::Dispatcher,
            policy::DrainPolicy,
            route_table::RouteTable,
            router::NativeRouter,
            server,
        };

        let mut config = ServerConfig::default();
        config.bind = "127.0.0.1:0".parse().unwrap();
        let config = Arc::new(config);

        let mut table = RouteTable::new();
        table.insert("GET", "/hello");
        let router = Arc::new(NativeRouter::from_entries(table.into_entries()).unwrap());

        let (dispatcher, mut ring_rx) = Dispatcher::new(512);
        let dispatcher = Arc::new(dispatcher);
        let (collector, collector_tx) = BatchCollector::new(
            Arc::clone(&dispatcher),
            DrainPolicy::default_fixed(),
            4096,
        );

        tokio::spawn(collector.run());

        let dispatcher_clone = Arc::clone(&dispatcher);
        tokio::spawn(async move {
            while let Some(batch) = ring_rx.recv().await {
                for id in batch.correlation_ids {
                    dispatcher_clone.complete(
                        id,
                        PendingResponse {
                            status: 200,
                            headers_mp: bytes::Bytes::new(),
                            body: bytes::Bytes::from_static(b"ok"),
                        },
                    );
                }
            }
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        tokio::spawn(server::run_with_collector(
            Arc::new({
                let mut c = ServerConfig::default();
                c.bind = addr;
                c
            }),
            router,
            dispatcher,
            collector_tx,
            futures_util::future::pending::<()>(),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        addr
    });

    let client = reqwest::blocking::Client::builder()
        .pool_max_idle_per_host(32)
        .build()
        .unwrap();

    let url = format!("http://{}/hello", addr);

    let mut group = c.benchmark_group("server_throughput");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("http11_hello", |b| {
        b.iter(|| {
            let _ = client.get(&url).send().unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_router_lookup, bench_server_throughput);
criterion_main!(benches);
```

**Target:** `server_throughput/http11_hello` throughput > 100,000 elements/s (i.e., mean latency < 10µs per request including network RTT on loopback).

---

## 11. Workspace Integration

### Add `vertebra-http` to the workspace

Edit `src/rs/Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
    "vertebra-core",
    "vertebra-py",
    "vertebra-broker",
    "vertebra-server",
    "vertebra-engine",
    "vertebra-http",   # <-- add this
]
```

---

## 12. Task Checklist

### Phase 1: Hyper HTTP Server

- [ ] **T1.1** — Create `src/rs/vertebra-http/` directory and `Cargo.toml` as specified in §4.
- [ ] **T1.2** — Add `vertebra-http` to `src/rs/Cargo.toml` workspace members.
- [ ] **T1.3** — Write `src/config.rs` exactly as in §5.1. `cargo check --package vertebra-http` must pass.
- [ ] **T1.4** — Write `src/server.rs` exactly as in §5.2. `cargo check --package vertebra-http` must pass.
- [ ] **T1.5** — Write `src/tls.rs` exactly as in §5.3 (gated behind `#[cfg(feature = "tls")]`).
- [ ] **T1.6** — `cargo build --package vertebra-http` must succeed (no `tls` feature).
- [ ] **T1.7** — `cargo build --package vertebra-http --features tls` must succeed.

**Gate:** `cargo check --package vertebra-http --features tls` produces zero errors.

### Phase 2: Native Router

- [ ] **T2.1** — Write `src/route_table.rs` exactly as in §6.1.
- [ ] **T2.2** — Write `src/router.rs` exactly as in §6.2.
- [ ] **T2.3** — Write `tests/test_router.rs` exactly as in §9.2.
- [ ] **T2.4** — `cargo test --package vertebra-http test_router` all green (8 tests).
- [ ] **T2.5** — `cargo clippy --package vertebra-http` zero warnings.

**Gate:** all 8 router tests pass, no clippy warnings.

### Phase 3: Batch Collector & Dispatch

- [ ] **T3.1** — Write `src/policy.rs` exactly as in §7.1.
- [ ] **T3.2** — Write `src/collector.rs` exactly as in §7.2.
- [ ] **T3.3** — Write `src/dispatcher.rs` exactly as in §7.3.
- [ ] **T3.4** — Write `tests/test_collector.rs` exactly as in §9.3.
- [ ] **T3.5** — Write `tests/test_dispatcher.rs` exactly as in §9.4.
- [ ] **T3.6** — `cargo test --package vertebra-http test_collector` all green (4 tests).
- [ ] **T3.7** — `cargo test --package vertebra-http test_dispatcher` all green (3 tests).

**Gate:** 7 collector+dispatcher tests pass.

### Phase 4: Response Handling & Integration

- [ ] **T4.1** — Write `src/response.rs` exactly as in §8.1 (update `ServerState` to carry `collector_tx`).
- [ ] **T4.2** — Write `src/lib.rs` exactly as in §8.2.
- [ ] **T4.3** — Write `src/main.rs` exactly as in §8.3.
- [ ] **T4.4** — Write `tests/test_server.rs` exactly as in §9.1.
- [ ] **T4.5** — `cargo test --package vertebra-http test_server` all green (3 tests).
- [ ] **T4.6** — `cargo run --package vertebra-http --bin vertebra-server` starts and accepts a `curl http://127.0.0.1:8000/hello` without error.

**Gate:** server starts, 3 server integration tests pass.

### Phase 5: Benchmark

- [ ] **T5.1** — Write `benches/server_throughput.rs` exactly as in §10.
- [ ] **T5.2** — `cargo bench --package vertebra-http --bench server_throughput` compiles and runs.
- [ ] **T5.3** — `server_throughput/http11_hello` reports > 100,000 elements/s (may require tuning channel capacity or Tokio worker count; document result).
- [ ] **T5.4** — `router/lookup_static_100_routes` reports p99 < 1µs.

**Gate:** benchmark runs green; throughput target met or deviation documented with rationale.

### Final Gate

- [ ] **FG.1** — `cd src/rs && cargo test --package vertebra-http` — all tests green.
- [ ] **FG.2** — `cd src/rs && cargo clippy --package vertebra-http -- -D warnings` — zero warnings.
- [ ] **FG.3** — `cd src/rs && cargo clippy --package vertebra-http --features tls -- -D warnings` — zero warnings.
- [ ] **FG.4** — The `vertebra-server` binary starts on port 8000 and accepts `curl http://localhost:8000/hello` returning HTTP 200.
- [ ] **FG.5** — Benchmark confirms > 100k req/s or deviation is documented.

---

## 13. Design Decisions Log

| Decision | Choice | Rationale |
|---|---|---|
| HTTP/2 negotiation mechanism | ALPN over TLS only | Research confirmed h2c upgrade is prototype-grade: broken stream-1 response, fragile split-preface detection, non-RFC-robust header validation. ALPN is the correct production mechanism per RFC 9113. |
| HTTP/3 | Deferred | Research recommendation: defer until the backend IPC contract is stream-aware (trailers, cancellation, backpressure). Proxy termination is the interim path. |
| Router | `matchit` | Same router used by Axum. Trie-based O(path-length) lookup. No allocations on the hot path. Proven at scale. |
| Handler IDs | FNV-1a hash of `"METHOD /path"` | Enables Python and Rust to agree on IDs without string comparison at dispatch time. FNV is fast and collision-resistant for path-length strings. |
| Batch drain triggers | Size >= 64 OR elapsed >= 1ms | 64 is a common L1 cache-friendly batch size. 1ms aligns with typical network jitter while keeping tail latency low. Both thresholds are configurable via `DrainPolicy`. |
| TLS implementation | `rustls` + `tokio-rustls` | Memory-safe, no C dependency chain, first-class ALPN support. Consistent across Linux/macOS/Windows. |
| One matchit router per method | Yes | Avoids matchit's inability to store duplicate paths across methods. The per-method dispatch is a single `HashMap::get` — negligible overhead. |
| Dispatcher ring stub | `tokio::sync::mpsc` | Chapter 5 does not write to shared memory; the real `vertebra-core` MPMC ring is wired in Chapter 6. The stub has the same async API so the swap is mechanical. |
| `biased` in `tokio::select!` | Yes in collector | Prefer incoming requests over timeout expiry to avoid artificially small batches when both are simultaneously ready. |

---

## 14. Dependencies on Chapter 4 (native-ipc)

Chapter 5 references but does not yet use the following from `vertebra-core`:

| Type | Where used in Ch. 5 | Wired in |
|---|---|---|
| `vertebra_core::protocol::RequestHeader` | `dispatcher.rs` header serialisation format | Ch. 5 mimics the layout manually; real usage in Ch. 6 |
| `vertebra_core::protocol::ResponseHeader` | `dispatcher.rs` response correlation | Ch. 6 |
| MPMC ring buffer | `dispatcher.rs` `ring_tx` stub | Ch. 6 replaces `mpsc` with the real ring |
| `BatchArena` | `collector.rs` arena allocation | Ch. 6 |
| `RequestInfo` / `ResponseInfo` (C-ABI proxy) | FFI bridge | Ch. 7 |

Chapter 5 is self-contained and does not require Chapter 4 to be complete. The `vertebra-core` crate is a compile-time dependency but only `protocol::RequestHeader` layout constants are referenced (as documentation for the wire format). The actual ring buffer integration is deferred to Chapter 6.

---

## 15. Out of Scope for This Chapter

- HTTP/3 / QUIC transport (deferred to `http3-quic-transport_20260314`).
- h2c cleartext upgrade (explicitly excluded; see §2 and Design Decisions Log).
- Streaming request/response bodies (deferred to Chapter 9, ASGI Core Adapter).
- WebSocket upgrade (deferred to ASGI Extension Support).
- The PyO3 FFI bridge (`PendingRequest` to Python; Chapter 7).
- The real `vertebra-core` MPMC ring write (Chapter 6).
- Graceful drain on SIGTERM (Chapter 5 binary handles SIGINT/ctrl-c only; full lifecycle is Chapter 5 of Saga 2, `supervisor-lifecycle_20260314`).
- Observability / tracing spans (scaffolded via `tracing::debug!` macros; full instrumentation is the production-readiness spec).
