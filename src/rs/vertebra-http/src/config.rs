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

        config
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
