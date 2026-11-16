//! Core Redis connection management, command dispatch, and pipeline execution.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::RwLock;
use redis::{Cmd, Pipeline, PushInfo, PushKind, Value};
use tokio::sync::Mutex as TokioMutex;

use crate::caching::ClientLruCache;
use crate::config::{RedisClientConfig, RedisTopologyMode};
use crate::errors::{RedisDriverError, classify_redis_error};
use crate::topology::ClusterSlotMap;

enum CoreConnection {
    Standalone(redis::aio::ConnectionManager),
    Cluster(redis::cluster_async::ClusterConnection),
}

/// Core thread-safe Redis client supporting Standalone, Sentinel, and Cluster topologies.
pub struct RedisCoreClient {
    pub config: RedisClientConfig,
    pub client_cache: Arc<ClientLruCache>,
    pub slot_map: Arc<ClusterSlotMap>,
    conn: TokioMutex<Option<CoreConnection>>,
    closed: AtomicBool,
    active_password: RwLock<Option<String>>,
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
            conn: TokioMutex::new(None),
            closed: AtomicBool::new(false),
            active_password,
        }))
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.client_cache.invalidate_all();
    }

    /// Update dynamic credentials (e.g. IAM token) and force reconnect.
    pub async fn reconnect(&self, new_password: Option<&str>) -> Result<(), RedisDriverError> {
        if let Some(pw) = new_password {
            *self.active_password.write() = Some(pw.to_string());
        }
        let mut guard = self.conn.lock().await;
        *guard = None;
        drop(guard);
        self.ensure_connected().await?;
        Ok(())
    }

    /// Ensure the client connection is open, establishing it lazily if needed.
    pub async fn ensure_connected(&self) -> Result<(), RedisDriverError> {
        if self.is_closed() {
            return Err(RedisDriverError::interface("Redis client is closed", false));
        }

        let mut guard = self.conn.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        if self.config.uses_tls() {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }

        let mut effective_password = self.active_password.read().clone();
        if self.config.enable_gcp_iam_auth {
            if let Some(ref handle) = self.config.iam_token_handle {
                let snap = handle
                    .get_or_refresh()
                    .await
                    .map_err(|e| RedisDriverError::operational(e, true))?;
                effective_password = Some(snap.access_token.clone());
                *self.active_password.write() = effective_password.clone();
            }
        }

        let effective_url = match &effective_password {
            Some(pw) => {
                let mut u = url::Url::parse(&self.config.normalized_url)
                    .map_err(|e| RedisDriverError::config(e.to_string()))?;
                let _ = u.set_password(Some(pw));
                if let Some(ref user) = self.config.username {
                    let _ = u.set_username(user);
                }
                u.to_string()
            }
            None => self.config.normalized_url.clone(),
        };

        match self.config.topology {
            RedisTopologyMode::Standalone | RedisTopologyMode::Sentinel => {
                let mut conn_info =
                    redis::IntoConnectionInfo::into_connection_info(effective_url.as_str())
                        .map_err(|e| classify_redis_error(&e))?;
                if self.config.protocol == 3 {
                    let redis_info = conn_info
                        .redis_settings()
                        .clone()
                        .set_protocol(redis::ProtocolVersion::RESP3);
                    conn_info = conn_info.set_redis_settings(redis_info);
                }
                let client =
                    redis::Client::open(conn_info).map_err(|e| classify_redis_error(&e))?;

                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
                let cache_ref = Arc::clone(&self.client_cache);
                tokio::spawn(async move {
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

                let mut mgr_config = redis::aio::ConnectionManagerConfig::new();
                mgr_config = mgr_config.set_number_of_retries(1);
                mgr_config =
                    mgr_config.set_connection_timeout(Some(std::time::Duration::from_millis(500)));
                mgr_config =
                    mgr_config.set_response_timeout(Some(std::time::Duration::from_millis(1500)));
                if self.config.protocol == 3 {
                    mgr_config = mgr_config.set_push_sender(tx);
                }
                let conn_mgr = tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    client.get_connection_manager_with_config(mgr_config),
                )
                .await
                .map_err(|_| RedisDriverError::operational("connection attempt timed out", true))?
                .map_err(|e| classify_redis_error(&e))?;

                *guard = Some(CoreConnection::Standalone(conn_mgr));
            }
            RedisTopologyMode::Cluster => {
                let nodes = if !self.config.cluster_nodes.is_empty() {
                    self.config
                        .cluster_nodes
                        .iter()
                        .map(|(h, p)| format!("redis://{h}:{p}"))
                        .collect()
                } else {
                    vec![effective_url]
                };

                let cluster_client = redis::cluster::ClusterClient::builder(nodes)
                    .use_protocol(redis::ProtocolVersion::RESP3)
                    .build()
                    .map_err(|e| classify_redis_error(&e))?;

                let cluster_conn = cluster_client
                    .get_async_connection()
                    .await
                    .map_err(|e| classify_redis_error(&e))?;

                *guard = Some(CoreConnection::Cluster(cluster_conn));
            }
        }

        Ok(())
    }

    /// Execute a command asynchronously on the active connection.
    pub async fn execute_cmd(&self, cmd: &Cmd) -> Result<Value, RedisDriverError> {
        self.ensure_connected().await?;
        let mut guard = self.conn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| RedisDriverError::operational("Redis connection unavailable", true))?;

        match conn {
            CoreConnection::Standalone(c) => {
                tokio::time::timeout(std::time::Duration::from_secs(2), cmd.query_async(c))
                    .await
                    .map_err(|_| RedisDriverError::operational("command timed out", true))?
                    .map_err(|e| classify_redis_error(&e))
            }
            CoreConnection::Cluster(c) => {
                tokio::time::timeout(std::time::Duration::from_secs(2), cmd.query_async(c))
                    .await
                    .map_err(|_| RedisDriverError::operational("command timed out", true))?
                    .map_err(|e| classify_redis_error(&e))
            }
        }
    }

    /// Execute a pipeline asynchronously on the active connection.
    pub async fn execute_pipe(&self, pipe: &Pipeline) -> Result<Vec<Value>, RedisDriverError> {
        self.ensure_connected().await?;
        let mut guard = self.conn.lock().await;
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
        if self.config.enable_client_side_caching {
            if let Some(cached) = self.client_cache.get(&key_str) {
                return Ok(Some(cached));
            }
        }

        let mut cmd = redis::cmd("GET");
        cmd.arg(key);
        let val = self.execute_cmd(&cmd).await?;

        match val {
            Value::Nil => Ok(None),
            Value::BulkString(bytes) => {
                if self.config.enable_client_side_caching {
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
                if self.config.enable_client_side_caching {
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

        if success && self.config.enable_client_side_caching {
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
        self.execute_cmd(&cmd).await
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
