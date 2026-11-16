//! Core Redis connection management, command dispatch, and pipeline execution.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use redis::sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};
use redis::{Cmd, Pipeline, PushInfo, PushKind, Value};
use tokio::sync::Mutex as TokioMutex;
use tokio::task::AbortHandle;

use crate::caching::ClientLruCache;
use crate::config::{RedisClientConfig, RedisTopologyMode};
use crate::errors::{RedisDriverError, classify_redis_error};
use crate::topology::{ClusterSlotMap, parse_sentinel_master_addr};

enum CoreConnection {
    Standalone(redis::aio::ConnectionManager),
    Cluster(redis::cluster_async::ClusterConnection),
}

fn extract_url_credentials_and_db(normalized_url: &str) -> (Option<String>, Option<String>, i64) {
    if let Ok(parsed) = url::Url::parse(normalized_url) {
        let user = (!parsed.username().is_empty()).then(|| parsed.username().to_string());
        let pass = parsed.password().map(str::to_string);
        let db = parsed
            .path()
            .trim_start_matches('/')
            .parse::<i64>()
            .unwrap_or(0);
        (user, pass, db)
    } else {
        (None, None, 0)
    }
}

fn apply_redis_settings(
    mut conn_info: redis::ConnectionInfo,
    protocol: u8,
    username: Option<&str>,
    password: Option<&str>,
) -> redis::ConnectionInfo {
    let mut settings = conn_info.redis_settings().clone();
    settings = settings.set_protocol(if protocol == 3 {
        redis::ProtocolVersion::RESP3
    } else {
        redis::ProtocolVersion::RESP2
    });
    if let Some(u) = username {
        settings = settings.set_username(u);
    }
    if let Some(p) = password {
        settings = settings.set_password(p);
    }
    conn_info = conn_info.set_redis_settings(settings);
    conn_info
}

pub(crate) async fn resolve_effective_credentials(
    config: &RedisClientConfig,
    password_override: Option<String>,
) -> Result<(Option<String>, Option<String>, i64), RedisDriverError> {
    let (url_user, url_pass, db) = extract_url_credentials_and_db(&config.normalized_url);
    let effective_username = config.username.clone().or(url_user);
    let mut effective_password = password_override
        .or_else(|| config.password.clone())
        .or(url_pass);

    if config.enable_gcp_iam_auth
        && let Some(ref handle) = config.iam_token_handle
    {
        let snap = handle
            .get_or_refresh()
            .await
            .map_err(|e| RedisDriverError::operational(e, true))?;
        effective_password = Some(snap.access_token.clone());
    }

    Ok((effective_username, effective_password, db))
}

pub(crate) async fn open_redis_client_for_config(
    config: &RedisClientConfig,
    effective_username: Option<&str>,
    effective_password: Option<&str>,
    db: i64,
) -> Result<redis::Client, RedisDriverError> {
    match config.topology {
        RedisTopologyMode::Standalone | RedisTopologyMode::Cluster => {
            let conn_info =
                redis::IntoConnectionInfo::into_connection_info(config.normalized_url.as_str())
                    .map_err(|e| classify_redis_error(&e))?;
            let conn_info = apply_redis_settings(
                conn_info,
                config.protocol,
                effective_username,
                effective_password,
            );
            redis::Client::open(conn_info).map_err(|e| classify_redis_error(&e))
        }
        RedisTopologyMode::Sentinel => {
            let master_name = config
                .sentinel_master_name
                .as_deref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    RedisDriverError::config(
                        "sentinel_master_name is required when topology is 'sentinel'.",
                    )
                })?;
            let scheme = if config.uses_tls() { "rediss" } else { "redis" };
            let sentinel_urls: Vec<String> = if !config.sentinel_nodes.is_empty() {
                config
                    .sentinel_nodes
                    .iter()
                    .map(|(h, p)| {
                        if h.contains(':') && !h.starts_with('[') {
                            format!("{scheme}://[{h}]:{p}")
                        } else {
                            format!("{scheme}://{h}:{p}")
                        }
                    })
                    .collect()
            } else {
                vec![config.normalized_url.clone()]
            };

            let mut master_redis_info = redis::RedisConnectionInfo::default().set_db(db);
            master_redis_info = master_redis_info.set_protocol(if config.protocol == 3 {
                redis::ProtocolVersion::RESP3
            } else {
                redis::ProtocolVersion::RESP2
            });
            if let Some(u) = effective_username {
                master_redis_info = master_redis_info.set_username(u);
            }
            if let Some(p) = effective_password {
                master_redis_info = master_redis_info.set_password(p);
            }
            let mut node_info =
                SentinelNodeConnectionInfo::default().set_redis_connection_info(master_redis_info);
            if config.uses_tls() {
                node_info = node_info.set_tls_mode(redis::TlsMode::Secure);
            }

            let mut primary_err = match SentinelClient::build(
                sentinel_urls.clone(),
                master_name,
                Some(node_info),
                SentinelServerType::Master,
            ) {
                Ok(mut sc) => {
                    match tokio::time::timeout(Duration::from_secs(2), sc.async_get_client()).await
                    {
                        Ok(Ok(master_client)) => return Ok(master_client),
                        Ok(Err(e)) => Some(classify_redis_error(&e)),
                        Err(_) => Some(RedisDriverError::operational(
                            "Sentinel master discovery timed out",
                            true,
                        )),
                    }
                }
                Err(e) => Some(classify_redis_error(&e)),
            };

            // Fallback direct `SENTINEL get-master-addr-by-name` probe (supports minimal Sentinel
            // responders that implement get-master-addr-by-name without ROLE).
            for s_url in &sentinel_urls {
                let Ok(s_info) = redis::IntoConnectionInfo::into_connection_info(s_url.as_str())
                else {
                    continue;
                };
                let Ok(s_client) = redis::Client::open(s_info) else {
                    continue;
                };
                let Ok(Ok(mut s_conn)) = tokio::time::timeout(
                    Duration::from_secs(1),
                    s_client.get_multiplexed_async_connection(),
                )
                .await
                else {
                    continue;
                };
                let mut sentinel_cmd = redis::cmd("SENTINEL");
                sentinel_cmd.arg("get-master-addr-by-name").arg(master_name);
                if let Ok(Ok(resp)) = tokio::time::timeout(
                    Duration::from_secs(1),
                    sentinel_cmd.query_async::<Value>(&mut s_conn),
                )
                .await
                {
                    match parse_sentinel_master_addr(&resp) {
                        Ok((m_host, m_port)) => {
                            let master_url = if m_host.contains(':') && !m_host.starts_with('[') {
                                format!("{scheme}://[{m_host}]:{m_port}/{db}")
                            } else {
                                format!("{scheme}://{m_host}:{m_port}/{db}")
                            };
                            let m_info = redis::IntoConnectionInfo::into_connection_info(
                                master_url.as_str(),
                            )
                            .map_err(|e| classify_redis_error(&e))?;
                            let m_info = apply_redis_settings(
                                m_info,
                                config.protocol,
                                effective_username,
                                effective_password,
                            );
                            return redis::Client::open(m_info)
                                .map_err(|e| classify_redis_error(&e));
                        }
                        Err(err) => primary_err = Some(err),
                    }
                }
            }

            Err(primary_err.unwrap_or_else(|| {
                RedisDriverError::operational("Unable to resolve master from Redis Sentinel", true)
            }))
        }
    }
}

fn infer_command_timeout(args: &[Vec<u8>]) -> Option<Duration> {
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
    if args.is_empty() {
        return Some(DEFAULT_TIMEOUT);
    }
    let cmd_upper = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    match cmd_upper.as_str() {
        "XREADGROUP" | "XREAD" => {
            for i in 1..args.len().saturating_sub(1) {
                if args[i].eq_ignore_ascii_case(b"BLOCK")
                    && let Ok(s) = std::str::from_utf8(&args[i + 1])
                    && let Ok(ms) = s.parse::<u64>()
                {
                    return if ms == 0 {
                        None
                    } else {
                        Some(Duration::from_millis(ms).saturating_add(DEFAULT_TIMEOUT))
                    };
                }
            }
            Some(DEFAULT_TIMEOUT)
        }
        "BLPOP" | "BRPOP" | "BRPOPLPUSH" | "BLMOVE" | "BZPOPMIN" | "BZPOPMAX" => {
            if let Some(last) = args.last()
                && let Ok(s) = std::str::from_utf8(last)
                && let Ok(secs) = s.parse::<f64>()
                && secs.is_finite()
                && secs >= 0.0
            {
                return if secs == 0.0 {
                    None
                } else {
                    Some(Duration::from_secs_f64(secs).saturating_add(DEFAULT_TIMEOUT))
                };
            }
            Some(DEFAULT_TIMEOUT)
        }
        _ => Some(DEFAULT_TIMEOUT),
    }
}

/// Core thread-safe Redis client supporting Standalone, Sentinel, and Cluster topologies.
pub struct RedisCoreClient {
    pub config: RedisClientConfig,
    pub client_cache: Arc<ClientLruCache>,
    pub slot_map: Arc<ClusterSlotMap>,
    conn: Arc<TokioMutex<Option<CoreConnection>>>,
    closed: AtomicBool,
    tracking_enabled: AtomicBool,
    active_password: RwLock<Option<String>>,
    push_abort: Mutex<Option<AbortHandle>>,
}

impl RedisCoreClient {
    pub fn new(config: RedisClientConfig) -> Result<Arc<Self>, RedisDriverError> {
        config.validate()?;
        let client_cache = Arc::new(ClientLruCache::new(config.client_cache_capacity));
        let slot_map = Arc::new(ClusterSlotMap::new());
        let active_password = RwLock::new(config.password.clone());

        Ok(Arc::new(Self {
            config,
            client_cache,
            slot_map,
            conn: Arc::new(TokioMutex::new(None)),
            closed: AtomicBool::new(false),
            tracking_enabled: AtomicBool::new(false),
            active_password,
            push_abort: Mutex::new(None),
        }))
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn has_active_connection(&self) -> bool {
        self.conn.try_lock().map(|g| g.is_some()).unwrap_or(false)
    }

    pub fn is_caching_active(&self) -> bool {
        self.config.enable_client_side_caching && self.tracking_enabled.load(Ordering::Acquire)
    }

    pub fn pubsub_config(&self) -> RedisClientConfig {
        let mut cfg = self.config.clone();
        if let Some(pw) = self.active_password.read().clone() {
            cfg.password = Some(pw);
        }
        cfg
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.tracking_enabled.store(false, Ordering::Release);
        self.client_cache.invalidate_all();
        if let Some(abort) = self.push_abort.lock().take() {
            abort.abort();
        }
        if let Ok(mut guard) = self.conn.try_lock() {
            *guard = None;
        } else {
            let conn_arc = Arc::clone(&self.conn);
            vertebra_db::py_async::db_runtime_handle().spawn(async move {
                let mut guard = conn_arc.lock().await;
                *guard = None;
            });
        }
    }

    /// Update dynamic credentials (e.g. IAM token) and force reconnect.
    pub async fn reconnect(&self, new_password: Option<&str>) -> Result<(), RedisDriverError> {
        if self.is_closed() {
            return Err(RedisDriverError::interface("Redis client is closed", false));
        }
        if let Some(pw) = new_password {
            *self.active_password.write() = Some(pw.to_string());
        }
        if let Some(abort) = self.push_abort.lock().take() {
            abort.abort();
        }
        {
            let mut guard = self.conn.lock().await;
            *guard = None;
        }
        self.tracking_enabled.store(false, Ordering::Release);
        self.ensure_connected().await?;
        Ok(())
    }

    fn build_client_tracking_cmd(&self) -> Cmd {
        let mut cmd = redis::cmd("CLIENT");
        cmd.arg("TRACKING").arg("ON");
        for prefix in &self.config.client_tracking_prefixes {
            cmd.arg("PREFIX").arg(prefix.as_str());
        }
        cmd.arg("BCAST");
        cmd
    }

    /// Ensure the client connection is open, establishing it lazily if needed.
    pub async fn ensure_connected(&self) -> Result<(), RedisDriverError> {
        if self.is_closed() {
            return Err(RedisDriverError::interface("Redis client is closed", false));
        }

        let mut guard = self.conn.lock().await;
        if self.is_closed() {
            *guard = None;
            return Err(RedisDriverError::interface("Redis client is closed", false));
        }
        if guard.is_some() {
            return Ok(());
        }

        if self.config.uses_tls() {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }

        let current_pw = self.active_password.read().clone();
        let (effective_username, effective_password, db) =
            resolve_effective_credentials(&self.config, current_pw).await?;
        if let Some(ref pw) = effective_password {
            *self.active_password.write() = Some(pw.clone());
        }

        match self.config.topology {
            RedisTopologyMode::Standalone | RedisTopologyMode::Sentinel => {
                let client = open_redis_client_for_config(
                    &self.config,
                    effective_username.as_deref(),
                    effective_password.as_deref(),
                    db,
                )
                .await?;

                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
                let cache_ref = Arc::clone(&self.client_cache);
                let jh = tokio::spawn(async move {
                    while let Some(push) = rx.recv().await {
                        if push.kind == PushKind::Invalidate {
                            if push.data.is_empty() || push.data.first() == Some(&Value::Nil) {
                                cache_ref.invalidate_all();
                            } else {
                                let mut keys = Vec::new();
                                for item in &push.data {
                                    match item {
                                        Value::Array(sub_items) => {
                                            for sub in sub_items {
                                                if let Value::BulkString(b) = sub {
                                                    keys.push(
                                                        String::from_utf8_lossy(b).into_owned(),
                                                    );
                                                }
                                            }
                                        }
                                        Value::BulkString(b) => {
                                            keys.push(String::from_utf8_lossy(b).into_owned());
                                        }
                                        Value::SimpleString(s) => {
                                            keys.push(s.clone());
                                        }
                                        _ => {}
                                    }
                                }
                                cache_ref.invalidate_keys(&keys);
                            }
                        }
                    }
                });
                if let Some(prev) = self.push_abort.lock().replace(jh.abort_handle()) {
                    prev.abort();
                }

                let mut mgr_config = redis::aio::ConnectionManagerConfig::new();
                mgr_config = mgr_config.set_number_of_retries(1);
                mgr_config = mgr_config.set_connection_timeout(Some(Duration::from_millis(500)));
                mgr_config = mgr_config.set_response_timeout(None);
                if self.config.protocol == 3 {
                    mgr_config = mgr_config.set_push_sender(tx);
                }
                let mut conn_mgr = tokio::time::timeout(
                    Duration::from_millis(1500),
                    client.get_connection_manager_with_config(mgr_config),
                )
                .await
                .map_err(|_| RedisDriverError::operational("connection attempt timed out", true))?
                .map_err(|e| classify_redis_error(&e))?;

                if self.config.enable_client_side_caching && self.config.protocol == 3 {
                    let track_cmd = self.build_client_tracking_cmd();
                    let ok = matches!(
                        tokio::time::timeout(
                            Duration::from_secs(2),
                            track_cmd.query_async::<Value>(&mut conn_mgr),
                        )
                        .await,
                        Ok(Ok(_))
                    );
                    self.tracking_enabled.store(ok, Ordering::Release);
                } else {
                    self.tracking_enabled.store(false, Ordering::Release);
                }

                if self.is_closed() {
                    *guard = None;
                    return Err(RedisDriverError::interface("Redis client is closed", false));
                }
                *guard = Some(CoreConnection::Standalone(conn_mgr));
            }
            RedisTopologyMode::Cluster => {
                let scheme = if self.config.uses_tls() {
                    "rediss"
                } else {
                    "redis"
                };
                let nodes: Vec<String> = if !self.config.cluster_nodes.is_empty() {
                    self.config
                        .cluster_nodes
                        .iter()
                        .map(|(h, p)| {
                            if h.contains(':') && !h.starts_with('[') {
                                format!("{scheme}://[{h}]:{p}")
                            } else {
                                format!("{scheme}://{h}:{p}")
                            }
                        })
                        .collect()
                } else {
                    vec![self.config.normalized_url.clone()]
                };

                let mut builder = redis::cluster::ClusterClient::builder(nodes).use_protocol(
                    if self.config.protocol == 3 {
                        redis::ProtocolVersion::RESP3
                    } else {
                        redis::ProtocolVersion::RESP2
                    },
                );
                if let Some(ref u) = effective_username {
                    builder = builder.username(u);
                }
                if let Some(ref p) = effective_password {
                    builder = builder.password(p);
                }
                if self.config.uses_tls() {
                    builder = builder.tls(redis::TlsMode::Secure);
                }

                let cluster_client = builder.build().map_err(|e| classify_redis_error(&e))?;

                let mut cluster_conn = cluster_client
                    .get_async_connection()
                    .await
                    .map_err(|e| classify_redis_error(&e))?;

                if self.config.enable_client_side_caching && self.config.protocol == 3 {
                    let track_cmd = self.build_client_tracking_cmd();
                    let ok = matches!(
                        tokio::time::timeout(
                            Duration::from_secs(2),
                            track_cmd.query_async::<Value>(&mut cluster_conn),
                        )
                        .await,
                        Ok(Ok(_))
                    );
                    self.tracking_enabled.store(ok, Ordering::Release);
                } else {
                    self.tracking_enabled.store(false, Ordering::Release);
                }

                if self.is_closed() {
                    *guard = None;
                    return Err(RedisDriverError::interface("Redis client is closed", false));
                }
                *guard = Some(CoreConnection::Cluster(cluster_conn));
            }
        }

        Ok(())
    }

    /// Execute a command asynchronously on the active connection with an explicit optional timeout.
    pub async fn execute_cmd_with_timeout(
        &self,
        cmd: &Cmd,
        timeout: Option<Duration>,
    ) -> Result<Value, RedisDriverError> {
        self.ensure_connected().await?;
        let mut guard = self.conn.lock().await;
        if self.is_closed() {
            *guard = None;
            return Err(RedisDriverError::interface("Redis client is closed", false));
        }
        let conn = guard
            .as_mut()
            .ok_or_else(|| RedisDriverError::operational("Redis connection unavailable", true))?;

        match (conn, timeout) {
            (CoreConnection::Standalone(c), Some(dur)) => {
                tokio::time::timeout(dur, cmd.query_async(c))
                    .await
                    .map_err(|_| RedisDriverError::operational("command timed out", true))?
                    .map_err(|e| classify_redis_error(&e))
            }
            (CoreConnection::Standalone(c), None) => cmd
                .query_async(c)
                .await
                .map_err(|e| classify_redis_error(&e)),
            (CoreConnection::Cluster(c), Some(dur)) => {
                tokio::time::timeout(dur, cmd.query_async(c))
                    .await
                    .map_err(|_| RedisDriverError::operational("command timed out", true))?
                    .map_err(|e| classify_redis_error(&e))
            }
            (CoreConnection::Cluster(c), None) => cmd
                .query_async(c)
                .await
                .map_err(|e| classify_redis_error(&e)),
        }
    }

    /// Execute a command asynchronously on the active connection.
    pub async fn execute_cmd(&self, cmd: &Cmd) -> Result<Value, RedisDriverError> {
        self.execute_cmd_with_timeout(cmd, Some(Duration::from_secs(2)))
            .await
    }

    /// Execute a pipeline asynchronously on the active connection.
    pub async fn execute_pipe(&self, pipe: &Pipeline) -> Result<Vec<Value>, RedisDriverError> {
        self.ensure_connected().await?;
        let mut guard = self.conn.lock().await;
        if self.is_closed() {
            *guard = None;
            return Err(RedisDriverError::interface("Redis client is closed", false));
        }
        let conn = guard
            .as_mut()
            .ok_or_else(|| RedisDriverError::operational("Redis connection unavailable", true))?;

        match conn {
            CoreConnection::Standalone(c) => pipe
                .query_async(c)
                .await
                .map_err(|e| classify_redis_error(&e)),
            CoreConnection::Cluster(c) => pipe
                .query_async(c)
                .await
                .map_err(|e| classify_redis_error(&e)),
        }
    }

    // --- Key-Value Operations ---

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RedisDriverError> {
        let key_str = String::from_utf8_lossy(key);
        if self.is_caching_active()
            && let Some(cached) = self.client_cache.get(&key_str)
        {
            return Ok(Some(cached));
        }

        let mut cmd = redis::cmd("GET");
        cmd.arg(key);
        let val = self.execute_cmd(&cmd).await?;

        match val {
            Value::Nil => Ok(None),
            Value::BulkString(bytes) => {
                if self.is_caching_active() {
                    self.client_cache.put(
                        key_str.into_owned(),
                        bytes.clone(),
                        self.config.client_cache_ttl_seconds,
                    );
                }
                Ok(Some(bytes))
            }
            Value::SimpleString(s) => {
                let bytes = s.into_bytes();
                if self.is_caching_active() {
                    self.client_cache.put(
                        key_str.into_owned(),
                        bytes.clone(),
                        self.config.client_cache_ttl_seconds,
                    );
                }
                Ok(Some(bytes))
            }
            other => Err(RedisDriverError::data(
                format!("Unexpected response type for GET: {other:?}"),
                false,
            )),
        }
    }

    pub async fn set(
        &self,
        key: &[u8],
        value: &[u8],
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
    ) -> Result<bool, RedisDriverError> {
        if ex.is_some() && px.is_some() {
            return Err(RedisDriverError::config(
                "set() accepts ex or px, not both (cannot specify both 'ex' and 'px' expiration options in SET)",
            ));
        }
        if nx && xx {
            return Err(RedisDriverError::config(
                "Cannot specify both 'nx' and 'xx' conditional options in SET",
            ));
        }

        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(value);

        if let Some(seconds) = ex {
            cmd.arg("EX").arg(seconds);
        } else if let Some(ms) = px {
            cmd.arg("PX").arg(ms);
        }

        if nx {
            cmd.arg("NX");
        } else if xx {
            cmd.arg("XX");
        }

        let res = self.execute_cmd(&cmd).await?;
        let success = match res {
            Value::Okay => true,
            Value::SimpleString(ref s) if s == "OK" => true,
            Value::Nil => false,
            _ => false,
        };

        if success && self.is_caching_active() {
            let key_str = String::from_utf8_lossy(key).into_owned();
            self.client_cache.put(
                key_str,
                value.to_vec(),
                ex.map(|s| s as f64)
                    .or_else(|| px.map(|ms| ms as f64 / 1000.0)),
            );
        }

        Ok(success)
    }

    pub async fn delete(&self, keys: &[Vec<u8>]) -> Result<i64, RedisDriverError> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("DEL");
        for k in keys {
            cmd.arg(k.as_slice());
        }
        let val = self.execute_cmd(&cmd).await?;
        let count = match val {
            Value::Int(i) => i,
            _ => 0,
        };

        if self.config.enable_client_side_caching {
            let string_keys: Vec<String> = keys
                .iter()
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .collect();
            self.client_cache.evict_local(&string_keys);
        }

        Ok(count)
    }

    pub async fn unlink(&self, keys: &[Vec<u8>]) -> Result<i64, RedisDriverError> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("UNLINK");
        for k in keys {
            cmd.arg(k.as_slice());
        }
        let val = self.execute_cmd(&cmd).await?;
        let count = match val {
            Value::Int(i) => i,
            _ => 0,
        };

        if self.config.enable_client_side_caching {
            let string_keys: Vec<String> = keys
                .iter()
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .collect();
            self.client_cache.evict_local(&string_keys);
        }

        Ok(count)
    }

    pub async fn pttl(&self, key: &[u8]) -> Result<i64, RedisDriverError> {
        let mut cmd = redis::cmd("PTTL");
        cmd.arg(key);
        let val = self.execute_cmd(&cmd).await?;
        match val {
            Value::Int(i) => Ok(i),
            other => Err(RedisDriverError::data(
                format!("Unexpected response for PTTL: {other:?}"),
                false,
            )),
        }
    }

    pub async fn scan(
        &self,
        cursor: u64,
        match_pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<(u64, Vec<Vec<u8>>), RedisDriverError> {
        let mut cmd = redis::cmd("SCAN");
        cmd.arg(cursor);
        if let Some(pattern) = match_pattern {
            cmd.arg("MATCH").arg(pattern);
        }
        if let Some(cnt) = count {
            cmd.arg("COUNT").arg(cnt);
        }

        let val = self.execute_cmd(&cmd).await?;
        match val {
            Value::Array(items) if items.len() >= 2 => {
                let next_cursor = match &items[0] {
                    Value::BulkString(b) => {
                        let s = String::from_utf8_lossy(b);
                        s.parse::<u64>().unwrap_or(0)
                    }
                    Value::SimpleString(s) => s.parse::<u64>().unwrap_or(0),
                    Value::Int(i) => *i as u64,
                    _ => 0,
                };

                let mut keys = Vec::new();
                if let Value::Array(ref key_items) = items[1] {
                    for k in key_items {
                        match k {
                            Value::BulkString(b) => keys.push(b.clone()),
                            Value::SimpleString(s) => keys.push(s.as_bytes().to_vec()),
                            _ => {}
                        }
                    }
                }

                Ok((next_cursor, keys))
            }
            other => Err(RedisDriverError::data(
                format!("Unexpected response for SCAN: {other:?}"),
                false,
            )),
        }
    }

    pub async fn flushdb(&self) -> Result<(), RedisDriverError> {
        let cmd = redis::cmd("FLUSHDB");
        self.execute_cmd(&cmd).await?;
        if self.config.enable_client_side_caching {
            self.client_cache.invalidate_all();
        }
        Ok(())
    }

    pub async fn publish(&self, channel: &str, message: &[u8]) -> Result<i64, RedisDriverError> {
        let mut cmd = redis::cmd("PUBLISH");
        cmd.arg(channel).arg(message);
        let val = self.execute_cmd(&cmd).await?;
        match val {
            Value::Int(i) => Ok(i),
            _ => Ok(0),
        }
    }

    pub async fn spublish(
        &self,
        shard_channel: &str,
        message: &[u8],
    ) -> Result<i64, RedisDriverError> {
        let mut cmd = redis::cmd("SPUBLISH");
        cmd.arg(shard_channel).arg(message);
        let val = self.execute_cmd(&cmd).await?;
        match val {
            Value::Int(i) => Ok(i),
            _ => Ok(0),
        }
    }

    pub async fn execute_command(&self, args: &[Vec<u8>]) -> Result<Value, RedisDriverError> {
        if args.is_empty() {
            return Err(RedisDriverError::programming(
                "Empty command passed to execute_command",
                false,
            ));
        }
        let cmd_name = String::from_utf8_lossy(&args[0]);
        let mut cmd = redis::cmd(&cmd_name);
        for arg in &args[1..] {
            cmd.arg(arg.as_slice());
        }
        let timeout = infer_command_timeout(args);
        self.execute_cmd_with_timeout(&cmd, timeout).await
    }

    pub async fn execute_pipeline(
        &self,
        commands: &[Vec<Vec<u8>>],
        atomic: bool,
    ) -> Result<Vec<Value>, RedisDriverError> {
        let mut pipe = redis::Pipeline::new();
        if atomic {
            pipe.atomic();
        }
        for cmd_args in commands {
            if !cmd_args.is_empty() {
                let cmd_name = String::from_utf8_lossy(&cmd_args[0]);
                let mut cmd = redis::cmd(&cmd_name);
                for arg in &cmd_args[1..] {
                    cmd.arg(arg.as_slice());
                }
                pipe.add_command(cmd);
            }
        }
        self.execute_pipe(&pipe).await
    }

    pub async fn eval_script(
        &self,
        script: &str,
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
    ) -> Result<Value, RedisDriverError> {
        let script_obj = redis::Script::new(script);
        let sha = script_obj.get_hash();

        let mut cmd_sha = redis::cmd("EVALSHA");
        cmd_sha.arg(sha).arg(keys.len());
        for k in keys {
            cmd_sha.arg(k.as_slice());
        }
        for a in args {
            cmd_sha.arg(a.as_slice());
        }

        match self.execute_cmd(&cmd_sha).await {
            Ok(v) => Ok(v),
            Err(e) if e.message().to_ascii_uppercase().contains("NOSCRIPT") => {
                let mut cmd_eval = redis::cmd("EVAL");
                cmd_eval.arg(script).arg(keys.len());
                for k in keys {
                    cmd_eval.arg(k.as_slice());
                }
                for a in args {
                    cmd_eval.arg(a.as_slice());
                }
                self.execute_cmd(&cmd_eval).await
            }
            Err(e) => Err(e),
        }
    }

    pub fn cached_keys(&self) -> Vec<String> {
        self.client_cache.cached_keys()
    }

    pub fn drain_invalidations(&self) -> Vec<String> {
        self.client_cache.drain_invalidations()
    }
}
