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
    /// Enable dev-only control surfaces such as HMR.
    pub dev_mode: bool,
    /// Runtime topology (embedded or shm).
    pub topology: Topology,
    /// Multi-process worker configuration.
    pub workers: WorkerConfig,
    /// Number of Python threads per process (for free-threading).
    pub thread_count: usize,
}

/// Runtime topology for Vertebra.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topology {
    /// Single-process, Python in-process via PyO3.
    Embedded,
    /// Multi-process, Python workers via SHM IPC.
    Shm,
}

impl Default for Topology {
    fn default() -> Self {
        Self::Embedded
    }
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
            dev_mode: false,
            topology: Topology::default(),
            workers: WorkerConfig::default(),
            thread_count: num_cpus(),
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
                });
            }
            (Some(_), None) | (None, Some(_)) => {
                eprintln!("[vertebra] warning: both VERTEBRA_TLS_CERT and VERTEBRA_TLS_KEY must be set for TLS");
            }
            _ => {}
        }

        // Topology config
        if let Ok(top) = std::env::var("VERTEBRA_TOPOLOGY") {
            if let Ok(t) = top.parse::<Topology>() {
                config.topology = t;
            }
        }

        // Thread scaling config
        if let Ok(threads) = std::env::var("VERTEBRA_THREADS") {
            if let Ok(n) = threads.parse::<usize>() {
                config.thread_count = n;
            }
        }

        // Worker config
        if let Ok(count) = std::env::var("VERTEBRA_WORKERS") {
            if let Ok(n) = count.parse::<usize>() {
                config.workers.count = n;
                // If workers > 0 and topology was not explicitly set to Embedded,
                // assume SHM for backward compatibility.
                if n > 0 && std::env::var("VERTEBRA_TOPOLOGY").is_err() {
                    config.topology = Topology::Shm;
                }
            }
        }
        if let Ok(python) = std::env::var("VERTEBRA_PYTHON_BIN") {
            config.workers.python_bin = python;
        }
        if let Ok(app) = std::env::var("VERTEBRA_APP") {
            config.workers.app_path = app;
        }

        config
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
