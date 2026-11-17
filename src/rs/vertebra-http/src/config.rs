//! Server configuration.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::access_log::AccessLogFormat;
use crate::backpressure::{BackpressureConfig, CircuitBreakerConfig};
use crate::compression::CompressionOffloadConfig;
use crate::cors::CorsOffloadConfig;
use crate::grpc::DEFAULT_MAX_GRPC_MESSAGE_SIZE;
use crate::http3::Http3Config;
use crate::multipart::MultipartLimits;
use crate::payload::PayloadLimits;
use crate::profile::ProfileConfig;
use crate::tls::{MtlsConfig, TrustedProxyConfig};

/// Static asset mount specification offloaded from Python frameworks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StaticMountConfig {
    pub path_prefix: String,
    pub directories: Vec<String>,
    pub html_mode: bool,
    pub send_as_attachment: bool,
    pub headers: HashMap<String, String>,
}

/// Pre-rendered OpenAPI JSON payload specification offloaded to Rust.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct OpenApiCacheConfig {
    pub schema_path: String,
    pub body: String,
    pub etag: String,
    pub content_type: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct OffloadPlanPayload {
    pub compression: Option<CompressionOffloadConfig>,
    pub cors: Option<CorsOffloadConfig>,
    pub static_mounts: Vec<StaticMountConfig>,
    pub openapi: Option<OpenApiCacheConfig>,
    pub discovery: HashMap<String, String>,
    pub payload_limits: Option<PayloadLimits>,
}

/// Top-level server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind on.
    pub bind: SocketAddr,
    /// Enable `SO_REUSEPORT` on Unix TCP listeners for multi-process worker scaling.
    pub reuse_port: bool,
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
    /// Maximum size of received HTTP/2 header blocks in bytes (`SETTINGS_MAX_HEADER_LIST_SIZE`).
    pub h2_max_header_list_size: Option<u32>,
    /// Timeout for reading complete HTTP/1 request headers.
    pub header_read_timeout: Duration,
    /// Maximum duration to wait for active connections to drain on shutdown.
    pub drain_timeout: Duration,
    /// Bounded streaming queue depth per streaming response.
    pub stream_queue_depth: usize,
    /// Enable cleartext HTTP/2 prior-knowledge `MSG_PEEK` detection (disabled by default for zero-peek HTTP/1.1 fastpath).
    pub enable_h2c_prior_knowledge: bool,
    /// CIDR-verified reverse proxies permitted to supply identity headers.
    pub trusted_proxies: TrustedProxyConfig,
    /// HTTP/3 over QUIC configuration and `Alt-Svc` advertisement settings.
    pub http3: Http3Config,
    /// Optional Rust CORS preflight and response header offload configuration.
    pub cors: Option<CorsOffloadConfig>,
    /// Optional Rust response compression offload configuration.
    pub compression: Option<CompressionOffloadConfig>,
    /// Optional Rust static file mount configurations.
    pub static_mounts: Vec<StaticMountConfig>,
    /// Optional pre-rendered OpenAPI schema cache served directly in Rust.
    pub openapi_cache: Option<OpenApiCacheConfig>,
    /// Request body and decompression byte limits (`413 Payload Too Large`).
    pub payload_limits: PayloadLimits,
    /// Multipart form-data field and byte limits.
    pub multipart_limits: MultipartLimits,
    /// Maximum gRPC message size in bytes (default: 4 MiB).
    pub max_grpc_message_size: usize,
    /// Token-guarded `/debug/pprof/*` diagnostic endpoint configuration.
    pub profile: ProfileConfig,
    /// Optional compiled HTTP access log format.
    pub access_log: Option<AccessLogFormat>,
    /// Pre-registered `/.well-known/*` discovery documents served in Rust.
    pub discovery: HashMap<String, String>,
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
        match s.trim().to_lowercase().as_str() {
            "auto" | "embedded" => Ok(Self::Embedded),
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
    /// Broker connection pool size (default 20).
    pub broker_pool_size: u32,
    /// Broker connection max overflow (default 10).
    pub broker_max_overflow: u32,
    /// Broker statement cache capacity (default 512).
    pub broker_stmt_capacity: u32,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            count: 0,
            python_bin: "python3".to_string(),
            app_path: String::new(),
            broker_pool_size: 20,
            broker_max_overflow: 10,
            broker_stmt_capacity: 512,
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
            reuse_port: false,
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
            h2_max_header_list_size: Some(65_536),
            header_read_timeout: Duration::from_secs(10),
            drain_timeout: Duration::from_secs(15),
            stream_queue_depth: 64,
            enable_h2c_prior_knowledge: false,
            trusted_proxies: TrustedProxyConfig::default(),
            http3: Http3Config::default(),
            cors: None,
            compression: None,
            static_mounts: Vec::new(),
            openapi_cache: None,
            payload_limits: PayloadLimits::default(),
            multipart_limits: MultipartLimits::default(),
            max_grpc_message_size: DEFAULT_MAX_GRPC_MESSAGE_SIZE,
            profile: ProfileConfig::default(),
            access_log: None,
            discovery: HashMap::new(),
        }
    }
}

impl ServerConfig {
    /// Parse a JSON middleware offload payload (`VERTEBRA_OFFLOAD_CONFIG`) into this `ServerConfig`.
    pub fn apply_offload_json(&mut self, raw: &str) -> Result<(), serde_json::Error> {
        let parsed: OffloadPlanPayload = serde_json::from_str(raw)?;
        if let Some(comp) = parsed.compression {
            self.compression = Some(comp);
        }
        if let Some(cors) = parsed.cors {
            self.cors = Some(cors);
        }
        if !parsed.static_mounts.is_empty() {
            self.static_mounts = parsed.static_mounts;
        }
        if let Some(openapi) = parsed.openapi {
            self.openapi_cache = Some(openapi);
        }
        if !parsed.discovery.is_empty() {
            self.discovery.extend(parsed.discovery);
        }
        if let Some(limits) = parsed.payload_limits {
            self.payload_limits = limits;
        }
        Ok(())
    }

    /// Return `true` when HTTP/3 over QUIC is enabled in configuration.
    pub fn enable_http3(&self) -> bool {
        self.http3.enabled
    }

    /// Validate that enabled HTTP/3 settings are backed by TLS configuration and the `http3` feature.
    pub fn validate_http3(&self) -> Result<(), String> {
        if !self.http3.enabled {
            return Ok(());
        }
        if self.tls.is_none() {
            return Err(
                "HTTP/3 requires TLS configuration (set VERTEBRA_TLS_CERT and VERTEBRA_TLS_KEY)"
                    .to_string(),
            );
        }
        #[cfg(not(feature = "http3"))]
        {
            Err(
                "HTTP/3 is enabled in ServerConfig, but vertebra-http was compiled without `--features http3`"
                    .to_string(),
            )
        }
        #[cfg(feature = "http3")]
        Ok(())
    }

    fn apply_http3_env<F>(&mut self, get_env: F)
    where
        F: Fn(&str) -> Option<String>,
    {
        if let Some(raw) = get_env("VERTEBRA_HTTP3") {
            self.http3.enabled = matches!(raw.trim().to_ascii_lowercase().as_str(), "1" | "true");
        }
        if let Some(raw) = get_env("VERTEBRA_HTTP3_ALT_SVC_MAX_AGE")
            && let Ok(n) = raw.trim().parse::<u64>()
        {
            self.http3.alt_svc_max_age = n;
        }
        if let Some(raw) = get_env("VERTEBRA_HTTP3_MAX_STREAMS")
            && let Ok(n) = raw.trim().parse::<u32>()
            && n > 0
        {
            self.http3.max_concurrent_bidi_streams = n;
        }
        if let Some(raw) = get_env("VERTEBRA_HTTP3_KEEP_ALIVE_MS")
            && let Ok(n) = raw.trim().parse::<u64>()
        {
            self.http3.keep_alive_interval_ms = n;
        }
        if let Some(raw) = get_env("VERTEBRA_WEBTRANSPORT") {
            self.http3.enable_webtransport =
                matches!(raw.trim().to_ascii_lowercase().as_str(), "1" | "true");
        }
        if let Some(raw) = get_env("VERTEBRA_WEBTRANSPORT_MAX_SESSIONS")
            && let Ok(n) = raw.trim().parse::<u64>()
            && n > 0
        {
            self.http3.max_webtransport_sessions = n;
        }
    }

    /// Build config from environment variables, falling back to defaults.
    ///
    /// Reads `VERTEBRA_HOST`, `VERTEBRA_PORT`, TLS, and HTTP/3 settings from environment variables.
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
        config.reuse_port = matches!(
            std::env::var("VERTEBRA_REUSE_PORT").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("True")
        );
        config.enable_h2c_prior_knowledge = matches!(
            std::env::var("VERTEBRA_H2C").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("True")
        );
        if let Ok(raw_offload) = std::env::var("VERTEBRA_OFFLOAD_CONFIG")
            && !raw_offload.trim().is_empty()
            && let Err(e) = config.apply_offload_json(&raw_offload)
        {
            eprintln!("[vertebra] warning: failed to parse VERTEBRA_OFFLOAD_CONFIG: {e}");
        }

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
        let explicit_topology = std::env::var("VERTEBRA_TOPOLOGY").ok().and_then(|v| {
            let trimmed = v.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_lowercase())
        });
        if let Some(ref top) = explicit_topology
            && let Ok(t) = top.parse::<Topology>()
        {
            config.topology = t;
        }

        // Thread scaling config (clamped to available_parallelism / taskset affinity for Tokio)
        let available_cpus = num_cpus();
        let mut explicit_tokio_threads = false;
        if let Ok(threads) = std::env::var("VERTEBRA_THREADS")
            && let Ok(n) = threads.parse::<usize>()
            && n > 0
        {
            config.thread_count = n;
            config.worker_threads = n.min(available_cpus).max(1);
        }
        if let Ok(tokio_threads) = std::env::var("VERTEBRA_TOKIO_THREADS")
            && let Ok(n) = tokio_threads.parse::<usize>()
            && n > 0
        {
            config.worker_threads = n;
            explicit_tokio_threads = true;
        }

        // Worker config
        if let Ok(count) = std::env::var("VERTEBRA_WORKERS")
            && let Ok(n) = count.parse::<usize>()
        {
            config.workers.count = n;
            #[cfg(all(unix, not(Py_GIL_DISABLED)))]
            if n > 1 && explicit_topology.as_deref() != Some("embedded") {
                config.topology = Topology::Shm;
            }
            #[cfg(unix)]
            if n > 1 && config.topology == Topology::Embedded {
                config.reuse_port = true;
            }
        }
        if config.topology == Topology::Shm && config.workers.count > 1 && !explicit_tokio_threads {
            config.worker_threads = available_cpus
                .saturating_sub(config.workers.count)
                .clamp(1, 4)
                .min(available_cpus);
        }
        if let Ok(python) = std::env::var("VERTEBRA_PYTHON_BIN") {
            config.workers.python_bin = python;
        }
        if let Ok(app) = std::env::var("VERTEBRA_APP") {
            config.workers.app_path = app;
        }
        if let Ok(v) = std::env::var("VERTEBRA_BROKER_POOL_SIZE")
            && let Ok(n) = v.parse::<u32>()
            && n > 0
        {
            config.workers.broker_pool_size = n;
        }
        if let Ok(v) = std::env::var("VERTEBRA_BROKER_MAX_OVERFLOW")
            && let Ok(n) = v.parse::<u32>()
        {
            config.workers.broker_max_overflow = n;
        }
        if let Ok(v) = std::env::var("VERTEBRA_BROKER_STMT_CAPACITY")
            && let Ok(n) = v.parse::<u32>()
            && n > 0
        {
            config.workers.broker_stmt_capacity = n;
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

        // Payload, multipart, gRPC, profiling, and access log knobs.
        if let Ok(v) = std::env::var("VERTEBRA_MAX_BODY_SIZE")
            && let Ok(n) = v.parse::<usize>()
            && n > 0
        {
            config.payload_limits.max_body_size = n;
        }
        if let Ok(v) = std::env::var("VERTEBRA_MAX_DECOMPRESSED_SIZE")
            && let Ok(n) = v.parse::<usize>()
            && n > 0
        {
            config.payload_limits.max_decompressed_size = n;
        }
        if let Ok(v) = std::env::var("VERTEBRA_MULTIPART_MAX_TOTAL_SIZE")
            && let Ok(n) = v.parse::<usize>()
            && n > 0
        {
            config.multipart_limits.max_total_size = n;
        }
        if let Ok(v) = std::env::var("VERTEBRA_MULTIPART_MAX_FILE_SIZE")
            && let Ok(n) = v.parse::<usize>()
            && n > 0
        {
            config.multipart_limits.max_file_size = n;
        }
        if let Ok(v) = std::env::var("VERTEBRA_MULTIPART_MAX_FIELDS")
            && let Ok(n) = v.parse::<usize>()
            && n > 0
        {
            config.multipart_limits.max_fields = n;
        }
        if let Ok(v) = std::env::var("VERTEBRA_GRPC_MAX_MESSAGE_SIZE")
            && let Ok(n) = v.parse::<usize>()
            && n > 0
        {
            config.max_grpc_message_size = n;
        }
        config.profile.enabled = matches!(
            std::env::var("VERTEBRA_PPROF_ENABLED").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("True")
        );
        if let Ok(token) = std::env::var("VERTEBRA_PPROF_TOKEN")
            && !token.trim().is_empty()
        {
            config.profile.bearer_token = Some(token);
        }
        if let Ok(raw_log) = std::env::var("VERTEBRA_ACCESS_LOG") {
            let trimmed = raw_log.trim();
            if !trimmed.is_empty() && !matches!(trimmed, "0" | "false" | "FALSE" | "off") {
                config.access_log = Some(match trimmed.to_ascii_lowercase().as_str() {
                    "1" | "true" | "combined" => AccessLogFormat::Combined,
                    "common" => AccessLogFormat::Common,
                    "json" => AccessLogFormat::Json,
                    _ => AccessLogFormat::Custom(trimmed.to_string()),
                });
            }
        }

        config.apply_http3_env(|key| std::env::var(key).ok());
        config.apply_limits_env(|key| std::env::var(key).ok());
        config
    }

    fn apply_limits_env<F>(&mut self, get_env: F)
    where
        F: Fn(&str) -> Option<String>,
    {
        if let Some(raw) = get_env("VERTEBRA_H2_MAX_HEADER_LIST_SIZE")
            && let Ok(n) = raw.trim().parse::<u32>()
            && n > 0
        {
            self.h2_max_header_list_size = Some(n);
        }
        if let Some(raw) = get_env("VERTEBRA_HEADER_READ_TIMEOUT_MS")
            && let Ok(n) = raw.trim().parse::<u64>()
        {
            self.header_read_timeout = Duration::from_millis(n);
        }
        if let Some(raw) = get_env("VERTEBRA_DRAIN_TIMEOUT_MS")
            && let Ok(n) = raw.trim().parse::<u64>()
        {
            self.drain_timeout = Duration::from_millis(n);
        }
        if let Some(raw) = get_env("VERTEBRA_STREAM_QUEUE_DEPTH")
            && let Ok(n) = raw.trim().parse::<usize>()
            && n > 0
        {
            self.stream_queue_depth = n;
        }
    }
}

fn num_cpus() -> usize {
    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    #[cfg(target_os = "linux")]
    if let Ok(raw) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut parts = raw.split_whitespace();
        if let (Some(quota_str), Some(period_str)) = (parts.next(), parts.next())
            && quota_str != "max"
            && let (Ok(quota), Ok(period)) = (quota_str.parse::<u64>(), period_str.parse::<u64>())
            && quota > 0
            && period > 0
        {
            let cgroup_cpus = (quota.div_ceil(period) as usize).max(1);
            return available.min(cgroup_cpus).max(1);
        }
    }
    available
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionBackend;

    #[test]
    fn apply_offload_json_parses_litestar_offload_plan_payload() {
        let payload = r#"{
            "compression": {
                "backend": "zstd",
                "minimum_size": 256,
                "gzip_compress_level": 6,
                "brotli_quality": 4,
                "zstd_level": 3,
                "exclude_patterns": ["/healthz"],
                "excluded_paths": ["/raw"]
            },
            "cors": {
                "allow_origins": ["https://app.example.com"],
                "allow_methods": ["GET", "POST"],
                "allow_headers": ["Authorization"],
                "allow_credentials": true,
                "expose_headers": ["X-Trace-Id"],
                "max_age": 600,
                "allow_origin_regex": null
            },
            "static_mounts": [
                {
                    "path_prefix": "/static",
                    "directories": ["/var/www/static"],
                    "html_mode": true,
                    "send_as_attachment": false,
                    "headers": {"cache-control": "public, max-age=3600"}
                }
            ],
            "openapi": {
                "schema_path": "/schema/openapi.json",
                "body": "{\"openapi\":\"3.1.0\"}",
                "etag": "\"abc123\"",
                "content_type": "application/vnd.oai.openapi+json"
            }
        }"#;

        let mut cfg = ServerConfig::default();
        cfg.apply_offload_json(payload)
            .expect("valid offload payload");

        let comp = cfg.compression.expect("compression populated");
        assert_eq!(comp.backend, CompressionBackend::Zstd);
        assert_eq!(comp.minimum_size, 256);
        assert_eq!(comp.excluded_paths, vec!["/raw".to_string()]);

        let cors = cfg.cors.expect("cors populated");
        assert_eq!(
            cors.allow_origins,
            vec!["https://app.example.com".to_string()]
        );
        assert!(cors.allow_credentials);
        assert_eq!(cors.max_age, 600);

        assert_eq!(cfg.static_mounts.len(), 1);
        assert_eq!(cfg.static_mounts[0].path_prefix, "/static");
        assert!(cfg.static_mounts[0].html_mode);

        let openapi = cfg.openapi_cache.expect("openapi cache populated");
        assert_eq!(openapi.schema_path, "/schema/openapi.json");
        assert_eq!(openapi.etag, "\"abc123\"");
    }

    #[test]
    fn http3_env_parsing_and_validation() {
        let mut cfg = ServerConfig::default();
        assert!(!cfg.enable_http3());
        assert!(cfg.http3.enable_webtransport);
        assert_eq!(cfg.http3.max_webtransport_sessions, 64);
        assert!(cfg.validate_http3().is_ok());

        cfg.apply_http3_env(|key| match key {
            "VERTEBRA_HTTP3" => Some("true".to_string()),
            "VERTEBRA_HTTP3_ALT_SVC_MAX_AGE" => Some("3600".to_string()),
            "VERTEBRA_HTTP3_MAX_STREAMS" => Some("512".to_string()),
            "VERTEBRA_HTTP3_KEEP_ALIVE_MS" => Some("2500".to_string()),
            "VERTEBRA_WEBTRANSPORT" => Some("false".to_string()),
            "VERTEBRA_WEBTRANSPORT_MAX_SESSIONS" => Some("128".to_string()),
            _ => None,
        });
        assert!(cfg.enable_http3());
        assert_eq!(cfg.http3.alt_svc_max_age, 3600);
        assert_eq!(cfg.http3.max_concurrent_bidi_streams, 512);
        assert_eq!(cfg.http3.keep_alive_interval_ms, 2500);
        assert!(!cfg.http3.enable_webtransport);
        assert_eq!(cfg.http3.max_webtransport_sessions, 128);

        cfg.apply_http3_env(|key| match key {
            "VERTEBRA_HTTP3" => Some("1".to_string()),
            "VERTEBRA_HTTP3_ALT_SVC_MAX_AGE" => Some("not-a-number".to_string()),
            "VERTEBRA_HTTP3_MAX_STREAMS" => Some("0".to_string()),
            "VERTEBRA_HTTP3_KEEP_ALIVE_MS" => Some("invalid".to_string()),
            "VERTEBRA_WEBTRANSPORT" => Some("1".to_string()),
            "VERTEBRA_WEBTRANSPORT_MAX_SESSIONS" => Some("0".to_string()),
            _ => None,
        });
        assert!(cfg.enable_http3());
        assert_eq!(cfg.http3.alt_svc_max_age, 3600);
        assert_eq!(cfg.http3.max_concurrent_bidi_streams, 512);
        assert_eq!(cfg.http3.keep_alive_interval_ms, 2500);
        assert!(cfg.http3.enable_webtransport);
        assert_eq!(cfg.http3.max_webtransport_sessions, 128);

        cfg.apply_http3_env(|key| match key {
            "VERTEBRA_HTTP3" => Some("false".to_string()),
            _ => None,
        });
        assert!(!cfg.enable_http3());
        assert!(cfg.validate_http3().is_ok());

        cfg.apply_http3_env(|key| match key {
            "VERTEBRA_HTTP3" => Some("invalid".to_string()),
            _ => None,
        });
        assert!(!cfg.enable_http3());

        cfg.http3.enabled = true;
        let err = cfg
            .validate_http3()
            .expect_err("http3 without tls must fail loud");
        assert!(err.contains("TLS"));

        cfg.tls = Some(TlsConfig::default());
        #[cfg(feature = "http3")]
        assert!(cfg.validate_http3().is_ok());
        #[cfg(not(feature = "http3"))]
        assert!(
            cfg.validate_http3()
                .expect_err("http3 without feature must fail loud")
                .contains("http3")
        );
    }

    #[test]
    fn limits_and_drain_defaults_and_env_parsing() {
        let mut cfg = ServerConfig::default();
        assert_eq!(cfg.h2_max_header_list_size, Some(65_536));
        assert_eq!(cfg.header_read_timeout, Duration::from_secs(10));
        assert_eq!(cfg.drain_timeout, Duration::from_secs(15));
        assert_eq!(cfg.stream_queue_depth, 64);

        cfg.apply_limits_env(|key| match key {
            "VERTEBRA_H2_MAX_HEADER_LIST_SIZE" => Some("131072".to_string()),
            "VERTEBRA_HEADER_READ_TIMEOUT_MS" => Some("2500".to_string()),
            "VERTEBRA_DRAIN_TIMEOUT_MS" => Some("4500".to_string()),
            "VERTEBRA_STREAM_QUEUE_DEPTH" => Some("128".to_string()),
            _ => None,
        });
        assert_eq!(cfg.h2_max_header_list_size, Some(131_072));
        assert_eq!(cfg.header_read_timeout, Duration::from_millis(2500));
        assert_eq!(cfg.drain_timeout, Duration::from_millis(4500));
        assert_eq!(cfg.stream_queue_depth, 128);

        cfg.apply_limits_env(|key| match key {
            "VERTEBRA_H2_MAX_HEADER_LIST_SIZE" => Some("0".to_string()),
            "VERTEBRA_HEADER_READ_TIMEOUT_MS" => Some("invalid".to_string()),
            "VERTEBRA_DRAIN_TIMEOUT_MS" => Some("invalid".to_string()),
            "VERTEBRA_STREAM_QUEUE_DEPTH" => Some("0".to_string()),
            _ => None,
        });
        assert_eq!(cfg.h2_max_header_list_size, Some(131_072));
        assert_eq!(cfg.header_read_timeout, Duration::from_millis(2500));
        assert_eq!(cfg.drain_timeout, Duration::from_millis(4500));
        assert_eq!(cfg.stream_queue_depth, 128);
    }
}
