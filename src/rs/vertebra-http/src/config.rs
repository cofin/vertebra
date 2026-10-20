//! Server configuration.

use std::net::SocketAddr;
use std::time::Duration;

use crate::backpressure::{BackpressureConfig, CircuitBreakerConfig};
use crate::http3::Http3Config;
use crate::tls::{MtlsConfig, TrustedProxyConfig};

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
    /// Enable dev-only control surfaces such as HMR.
    pub dev_mode: bool,
    /// Runtime topology (embedded or shm).
    pub topology: Topology,
    /// Multi-process worker configuration.
    pub workers: WorkerConfig,
    /// Number of Python threads per process (for free-threading).
    pub thread_count: usize,
    /// Admission control thresholds for the dispatcher ingress path.
    pub backpressure: BackpressureConfig,
    /// Enable adaptive HTTP/2 flow-control windows.
    pub h2_adaptive_window: bool,
    /// Initial HTTP/2 stream-level window size in bytes.
    pub h2_initial_stream_window_size: Option<u32>,
    /// Initial HTTP/2 connection-level window size in bytes.
    pub h2_initial_connection_window_size: Option<u32>,
    /// Maximum concurrent HTTP/2 streams per connection.
    pub h2_max_concurrent_streams: Option<u32>,
    /// HTTP/2 PING keep-alive interval in milliseconds.
    pub h2_keep_alive_interval_ms: Option<u64>,
    /// Enable RFC 9113 HTTP/1.1 `Upgrade: h2c` negotiation on cleartext sockets.
    pub enable_h2c_upgrade: bool,
    /// CIDR-verified reverse proxies permitted to supply identity headers.
    pub trusted_proxies: TrustedProxyConfig,
    /// HTTP/3 over QUIC configuration and `Alt-Svc` advertisement settings.
    pub http3: Http3Config,
}

/// Runtime topology for Vertebra.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Topology {
    /// Single-process, Python in-process via PyO3.
    #[default]
    Embedded,
    /// Multi-process, Python workers via SHM IPC.
    Shm,
}

impl std::str::FromStr for Topology {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "embedded" => Ok(Self::Embedded),
            "shm" => Ok(Self::Shm),
            _ => Err(format!("invalid topology: {s}")),
        }
    }
}

/// Configuration for multi-process workers.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Number of worker processes (0 = in-process only, default).
    pub count: usize,
    /// Path to Python interpreter.
    pub python_bin: String,
    /// App import path (e.g. "myapp:app").
    pub app_path: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            count: 0,
            python_bin: "python3".to_string(),
            app_path: String::new(),
        }
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to the PEM-encoded certificate chain.
    pub cert_path: std::path::PathBuf,
    /// Path to the PEM-encoded private key.
    pub key_path: std::path::PathBuf,
    /// Mutual TLS (mTLS) client certificate verification settings.
    pub mtls: MtlsConfig,
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
            dev_mode: false,
            topology: Topology::default(),
            workers: WorkerConfig::default(),
            thread_count: num_cpus(),
            backpressure: BackpressureConfig::default(),
            h2_adaptive_window: true,
            h2_initial_stream_window_size: None,
            h2_initial_connection_window_size: None,
            h2_max_concurrent_streams: None,
            h2_keep_alive_interval_ms: None,
            enable_h2c_upgrade: true,
            trusted_proxies: TrustedProxyConfig::default(),
            http3: Http3Config::default(),
        }
    }
}

impl ServerConfig {
    /// Build config from environment variables, falling back to defaults.
    ///
    /// Reads `VERTEBRA_HOST` and `VERTEBRA_PORT` to construct the bind address.
    pub fn from_env() -> Self {
        let mut config = Self::default();

        let host = std::env::var("VERTEBRA_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("VERTEBRA_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(8000);

        if let Ok(addr) = format!("{host}:{port}").parse() {
            config.bind = addr;
        }

        config.dev_mode = matches!(
            std::env::var("VERTEBRA_DEV").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("True")
        );

        // TLS config
        let tls_cert = std::env::var("VERTEBRA_TLS_CERT").ok();
        let tls_key = std::env::var("VERTEBRA_TLS_KEY").ok();
        match (tls_cert, tls_key) {
            (Some(cert), Some(key)) => {
                config.tls = Some(TlsConfig {
                    cert_path: cert.into(),
                    key_path: key.into(),
                    mtls: MtlsConfig::default(),
                });
            }
            (Some(_), None) | (None, Some(_)) => {
                eprintln!(
                    "[vertebra] warning: both VERTEBRA_TLS_CERT and VERTEBRA_TLS_KEY must be set for TLS"
                );
            }
            _ => {}
        }

        // Topology config
        if let Ok(top) = std::env::var("VERTEBRA_TOPOLOGY")
            && let Ok(t) = top.parse::<Topology>()
        {
            config.topology = t;
        }

        // Thread scaling config
        if let Ok(threads) = std::env::var("VERTEBRA_THREADS")
            && let Ok(n) = threads.parse::<usize>()
        {
            config.thread_count = n;
        }

        // Worker config
        if let Ok(count) = std::env::var("VERTEBRA_WORKERS")
            && let Ok(n) = count.parse::<usize>()
        {
            config.workers.count = n;
            // If workers > 0 and topology was not explicitly set to Embedded,
            // assume SHM for backward compatibility.
            if n > 0 && std::env::var("VERTEBRA_TOPOLOGY").is_err() {
                config.topology = Topology::Shm;
            }
        }
        if let Ok(python) = std::env::var("VERTEBRA_PYTHON_BIN") {
            config.workers.python_bin = python;
        }
        if let Ok(app) = std::env::var("VERTEBRA_APP") {
            config.workers.app_path = app;
        }

        // Backpressure controller knobs.
        if let Ok(v) = std::env::var("VERTEBRA_BP_MAX_IN_FLIGHT")
            && let Ok(n) = v.parse::<usize>()
        {
            config.backpressure.max_in_flight = n;
        }
        if let Ok(v) = std::env::var("VERTEBRA_BP_MAX_QUEUE_DEPTH")
            && let Ok(n) = v.parse::<usize>()
        {
            config.backpressure.max_queue_depth = n;
        }
        if let Ok(v) = std::env::var("VERTEBRA_BP_MAX_QUEUE_TIME_MS")
            && let Ok(n) = v.parse::<u64>()
        {
            config.backpressure.max_queue_time = Duration::from_millis(n);
        }
        // Circuit breaker knobs.
        let cb: &mut CircuitBreakerConfig = &mut config.backpressure.circuit_breaker;
        if let Ok(v) = std::env::var("VERTEBRA_BP_CB_FAILURE_RATE")
            && let Ok(f) = v.parse::<f64>()
            && (0.0..=1.0).contains(&f)
        {
            cb.failure_rate_threshold = f;
        }
        if let Ok(v) = std::env::var("VERTEBRA_BP_CB_MIN_SAMPLES")
            && let Ok(n) = v.parse::<u64>()
        {
            cb.min_samples = n;
        }
        if let Ok(v) = std::env::var("VERTEBRA_BP_CB_WINDOW_MS")
            && let Ok(n) = v.parse::<u64>()
        {
            cb.window = Duration::from_millis(n);
        }
        if let Ok(v) = std::env::var("VERTEBRA_BP_CB_HALF_OPEN_PROBES")
            && let Ok(n) = v.parse::<u64>()
        {
            cb.half_open_probe_count = n;
        }

        config
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
