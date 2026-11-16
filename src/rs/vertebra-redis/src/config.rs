//! Redis client connection and topology configuration.

use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use vertebra_core::gcp_auth::{
    GCP_TOKEN_CACHE_CAPSULE_NAME, GcpTokenCacheCapsule, SharedGcpTokenCacheHandle,
};

use crate::errors::RedisDriverError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RedisTopologyMode {
    #[default]
    Standalone,
    Cluster,
    Sentinel,
}

impl RedisTopologyMode {
    pub fn parse(s: &str) -> Result<Self, RedisDriverError> {
        match s.to_ascii_lowercase().as_str() {
            "standalone" => Ok(Self::Standalone),
            "cluster" => Ok(Self::Cluster),
            "sentinel" => Ok(Self::Sentinel),
            other => Err(RedisDriverError::config(format!(
                "Invalid Redis topology mode '{other}'; expected 'standalone', 'cluster', or 'sentinel'"
            ))),
        }
    }
}

/// Normalized Redis client configuration.
#[derive(Clone)]
pub struct RedisClientConfig {
    pub raw_url: String,
    pub normalized_url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_name: String,
    pub protocol: u8,
    pub topology: RedisTopologyMode,
    pub sentinel_master_name: Option<String>,
    pub sentinel_nodes: Vec<(String, u16)>,
    pub cluster_nodes: Vec<(String, u16)>,
    pub max_redirections: usize,
    pub enable_client_side_caching: bool,
    pub client_tracking_prefixes: Vec<String>,
    pub client_cache_capacity: usize,
    pub client_cache_ttl_seconds: Option<f64>,
    pub enable_gcp_iam_auth: bool,
    pub iam_token_handle: Option<SharedGcpTokenCacheHandle>,
    pub max_connections: usize,
}

impl std::fmt::Debug for RedisClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisClientConfig")
            .field("raw_url", &self.raw_url)
            .field("normalized_url", &self.normalized_url)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("client_name", &self.client_name)
            .field("protocol", &self.protocol)
            .field("topology", &self.topology)
            .field("max_redirections", &self.max_redirections)
            .field(
                "enable_client_side_caching",
                &self.enable_client_side_caching,
            )
            .field("enable_gcp_iam_auth", &self.enable_gcp_iam_auth)
            .finish()
    }
}

/// Normalize Redis connection URL schemes.
pub fn normalize_redis_url(raw: &str) -> Result<String, RedisDriverError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(RedisDriverError::config(
            "Redis connection URL cannot be empty",
        ));
    }

    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("valkey://") || lower.starts_with("valkeys://") {
        return Err(RedisDriverError::config(
            "Valkey URLs ('valkey://', 'valkeys://') are not supported by vertebra._redis; install and use vertebra._valkey via vertebra-core[valkey].",
        ));
    }

    if lower.starts_with("memory://") {
        return Err(RedisDriverError::config(
            "In-memory scheme 'memory://' is not supported by vertebra._redis.",
        ));
    }

    if trimmed.starts_with("unix://") {
        let path_part = &trimmed["unix://".len()..];
        return Ok(format!("redis+unix:///{path_part}"));
    }

    if trimmed.starts_with("unix:") {
        let path_part = trimmed.trim_start_matches("unix:");
        return Ok(format!("redis+unix:///{path_part}"));
    }

    if trimmed.starts_with("redis+unix://") || trimmed.starts_with("redis+unix:") {
        return Ok(trimmed.to_string());
    }

    if trimmed.starts_with("redis://") || trimmed.starts_with("rediss://") {
        return Ok(trimmed.to_string());
    }

    Err(RedisDriverError::config(format!(
        "Unsupported Redis URL scheme in '{trimmed}'; expected redis://, rediss://, or unix://"
    )))
}

impl RedisClientConfig {
    pub fn new(raw_url: &str) -> Result<Self, RedisDriverError> {
        let normalized_url = normalize_redis_url(raw_url)?;
        Ok(Self {
            raw_url: raw_url.to_string(),
            normalized_url,
            username: None,
            password: None,
            client_name: "vertebra".to_string(),
            protocol: 3,
            topology: RedisTopologyMode::Standalone,
            sentinel_master_name: None,
            sentinel_nodes: Vec::new(),
            cluster_nodes: Vec::new(),
            max_redirections: 5,
            enable_client_side_caching: false,
            client_tracking_prefixes: Vec::new(),
            client_cache_capacity: 4096,
            client_cache_ttl_seconds: None,
            enable_gcp_iam_auth: false,
            iam_token_handle: None,
            max_connections: 32,
        })
    }

    pub fn validate(&self) -> Result<(), RedisDriverError> {
        if self.is_unix_socket() && self.topology != RedisTopologyMode::Standalone {
            return Err(RedisDriverError::config(
                "Unix domain socket URLs cannot be used with cluster or sentinel topology.",
            ));
        }

        if self.topology == RedisTopologyMode::Sentinel
            && self
                .sentinel_master_name
                .as_deref()
                .unwrap_or("")
                .is_empty()
        {
            return Err(RedisDriverError::config(
                "sentinel_master_name is required when topology is 'sentinel'.",
            ));
        }

        Ok(())
    }

    pub fn is_unix_socket(&self) -> bool {
        self.normalized_url.starts_with("redis+unix:")
    }

    pub fn uses_tls(&self) -> bool {
        self.normalized_url.starts_with("rediss://")
    }

    pub fn parse_token_cache_capsule(
        capsule_any: &Bound<'_, PyAny>,
    ) -> Result<SharedGcpTokenCacheHandle, RedisDriverError> {
        let capsule = capsule_any.cast::<PyCapsule>().map_err(|_| {
            RedisDriverError::config("token_cache_capsule must be a valid GcpTokenCache PyCapsule")
        })?;
        let ptr = capsule
            .pointer_checked(Some(GCP_TOKEN_CACHE_CAPSULE_NAME))
            .map_err(|_| {
                RedisDriverError::config(
                    "token_cache_capsule name mismatch; expected vertebra.GcpTokenCacheCapsule",
                )
            })?
            .as_ptr()
            .cast::<GcpTokenCacheCapsule>();
        unsafe { SharedGcpTokenCacheHandle::from_capsule_ptr(ptr) }
            .map_err(RedisDriverError::config)
    }
}
