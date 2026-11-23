//! Core RESP client engine, cluster/sentinel routing, L1 tracking, Lua fallback,
//! streams, sorted sets, pipelines, and optional PyO3 argument/value helpers.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
#[cfg(feature = "pyo3")]
use parking_lot::Mutex as SyncMutex;
use parking_lot::RwLock as SyncRwLock;
use tokio::sync::{Mutex, RwLock};

use crate::cluster::{
    ClusterNodeEndpoint, ClusterTopology, is_read_only_command, validate_cluster_keys_same_slot,
};
use crate::codec::{RespValue, sha1_hex};
use crate::config::{RespConfig, RespTarget};
use crate::error::{RespError, RespErrorKind};
use crate::sentinel::{
    build_get_master_addr_cmd, parse_get_master_addr_reply, parse_sentinel_addr,
    verify_role_is_primary,
};
use crate::streams::{
    build_xack, build_xadd, build_xautoclaim, build_xdel, build_xgroup_create, build_xrange,
    build_xreadgroup, build_zadd, build_zrangebyscore, build_zrem,
};
use crate::tracking::TrackingCache;
use crate::transport::RespConnection;
use crate::{hash_slot, parse_cluster_redirect};

#[derive(Debug, Clone)]
enum PoolRoute {
    Standalone,
    Endpoint {
        host: String,
        port: u16,
        is_replica: bool,
    },
}

impl PoolRoute {
    fn pool_key(&self) -> String {
        match self {
            Self::Standalone => "standalone".to_string(),
            Self::Endpoint {
                host,
                port,
                is_replica,
            } => {
                let kind = if *is_replica { "ro" } else { "rw" };
                format!("{kind}:{host}:{port}")
            }
        }
    }
}

/// Reply decoder tag for queued pipeline commands.
#[derive(Debug, Clone)]
pub enum PipelineDecoder {
    BoolOk,
    Int,
    BytesOrNone,
    HGetAllDict,
    IntList,
    StringOrBytes,
    GenericPy,
}

/// Queued command entry executed in a batch pipeline.
#[derive(Debug, Clone)]
pub struct QueuedCommand {
    pub args: Vec<Vec<u8>>,
    pub keys: Vec<Vec<u8>>,
    pub read_only: bool,
    pub decoder: PipelineDecoder,
}

/// Core thread-safe async RESP engine shared across Valkey and Redis Sync/Async clients and pipelines.
pub struct RespEngine {
    pub config: Arc<RespConfig>,
    password_override: SyncRwLock<Option<String>>,
    tracking_cache: Option<Arc<TrackingCache>>,
    tracking_active: AtomicBool,
    pools: Arc<Mutex<HashMap<String, Vec<RespConnection>>>>,
    topology: SyncRwLock<ClusterTopology>,
    sentinel_primary: RwLock<Option<(String, u16)>>,
    closed: AtomicBool,
}

/// Backward-compatible alias for Valkey callers.
pub type ValkeyEngine = RespEngine;

impl RespEngine {
    /// Construct a new [`RespEngine`] from immutable validated configuration.
    #[must_use]
    pub fn new(config: Arc<RespConfig>) -> Arc<Self> {
        let tracking_cache = if config.client_tracking_enabled {
            Some(Arc::new(TrackingCache::new(
                config.client_tracking_capacity,
                config.client_tracking_ttl,
            )))
        } else {
            None
        };
        Arc::new(Self {
            config,
            password_override: SyncRwLock::new(None),
            tracking_cache,
            tracking_active: AtomicBool::new(false),
            pools: Arc::new(Mutex::new(HashMap::new())),
            topology: SyncRwLock::new(ClusterTopology::empty()),
            sentinel_primary: RwLock::new(None),
            closed: AtomicBool::new(false),
        })
    }

    /// Return the active configuration incorporating any runtime `reconnect(password=...)` override.
    #[must_use]
    pub fn effective_config(&self) -> Arc<RespConfig> {
        let guard = self.password_override.read();
        if let Some(ref pw) = *guard {
            let mut cfg = (*self.config).clone();
            cfg.password = Some(pw.clone());
            Arc::new(cfg)
        } else {
            Arc::clone(&self.config)
        }
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Return whether at least one idle pooled connection is currently held open.
    #[must_use]
    pub fn has_active_connection(&self) -> bool {
        self.pools
            .try_lock()
            .is_ok_and(|g| g.values().any(|v| !v.is_empty()))
    }

    /// Return whether RESP3 `CLIENT TRACKING` was enabled and negotiated on an active connection.
    #[must_use]
    pub fn is_caching_active(&self) -> bool {
        self.config.client_tracking_enabled && self.tracking_active.load(Ordering::Acquire)
    }

    /// Close all pooled connections asynchronously.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.tracking_active.store(false, Ordering::Release);
        if let Some(ref cache) = self.tracking_cache {
            cache.invalidate_key(None);
        }
        let mut guard = self.pools.lock().await;
        guard.clear();
    }

    /// Close all pooled connections synchronously without blocking on Tokio locks.
    pub fn close_sync(&self) {
        self.closed.store(true, Ordering::Release);
        self.tracking_active.store(false, Ordering::Release);
        if let Some(ref cache) = self.tracking_cache {
            cache.invalidate_key(None);
        }
        if let Ok(mut guard) = self.pools.try_lock() {
            guard.clear();
        } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let pools = Arc::clone(&self.pools);
            handle.spawn(async move {
                pools.lock().await.clear();
            });
        } else {
            #[cfg(feature = "pyo3")]
            {
                let pools = Arc::clone(&self.pools);
                vertebra_db::py_async::db_runtime_handle().spawn(async move {
                    pools.lock().await.clear();
                });
            }
        }
    }

    /// Eagerly establish at least one connection (or cluster topology) and return it to the pool.
    pub async fn connect(&self) -> Result<(), RespError> {
        self.ensure_open()?;
        if self.config.cluster_enabled {
            let _ = self.refresh_cluster_topology().await?;
            return Ok(());
        }
        let conn = self.checkout_connection(&PoolRoute::Standalone).await?;
        self.return_connection(PoolRoute::Standalone, conn).await;
        Ok(())
    }

    /// Update dynamic credentials (e.g. rotated bearer/IAM token), drop pooled connections, and reconnect.
    pub async fn reconnect(&self, new_password: Option<String>) -> Result<(), RespError> {
        self.ensure_open()?;
        if let Some(pw) = new_password {
            *self.password_override.write() = Some(pw);
        }
        self.tracking_active.store(false, Ordering::Release);
        {
            let mut guard = self.pools.lock().await;
            guard.clear();
        }
        {
            let mut primary = self.sentinel_primary.write().await;
            *primary = None;
        }
        self.connect().await
    }

    #[must_use]
    pub fn tracking_stats(&self) -> (u64, u64, u64, usize) {
        match &self.tracking_cache {
            Some(cache) => (
                cache.hits(),
                cache.misses(),
                cache.invalidations(),
                cache.len(),
            ),
            None => (0, 0, 0, 0),
        }
    }

    #[must_use]
    pub fn cached_keys(&self) -> Vec<String> {
        match &self.tracking_cache {
            Some(cache) => cache.cached_keys(),
            None => Vec::new(),
        }
    }

    #[must_use]
    pub fn drain_invalidations(&self) -> Vec<String> {
        match &self.tracking_cache {
            Some(cache) => cache.drain_invalidations(),
            None => Vec::new(),
        }
    }

    pub fn invalidate_keys(&self, keys: Option<&[Vec<u8>]>) -> usize {
        match &self.tracking_cache {
            Some(cache) => cache.invalidate_keys(keys),
            None => 0,
        }
    }

    #[must_use]
    pub fn slot_map(&self) -> HashMap<u16, (String, u16)> {
        self.topology.read().slot_map()
    }

    fn ensure_open(&self) -> Result<(), RespError> {
        if self.is_closed() {
            Err(RespError::interface("RESP client is closed"))
        } else {
            Ok(())
        }
    }

    async fn resolve_standalone_target(&self) -> Result<RespTarget, RespError> {
        if self.config.sentinel_service_name.is_some() {
            if let Some((host, port)) = self.sentinel_primary.read().await.clone() {
                return Ok(RespTarget::Tcp { host, port });
            }
            let (host, port) = self.discover_sentinel_primary().await?;
            return Ok(RespTarget::Tcp { host, port });
        }
        Ok(self.config.target.clone())
    }

    async fn discover_sentinel_primary(&self) -> Result<(String, u16), RespError> {
        let Some(ref master_name) = self.config.sentinel_service_name else {
            return Err(RespError::config("Sentinel service name is not configured"));
        };
        let mut last_err = RespError::operational(
            format!("No healthy Sentinel node returned primary for '{master_name}'"),
            true,
        );
        let sentinel_cfg = Arc::new(self.sentinel_handshake_config());
        let effective_cfg = self.effective_config();
        let sentinel_specs: Vec<String> = if !self.config.sentinel_nodes.is_empty() {
            self.config.sentinel_nodes.clone()
        } else {
            vec![format!("{}:{}", self.config.host, self.config.port)]
        };

        for spec in &sentinel_specs {
            let (shost, sport) = match parse_sentinel_addr(spec, 26379) {
                Ok(addr) => addr,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };
            let starget = RespTarget::Tcp {
                host: shost,
                port: sport,
            };
            let use_auth = sentinel_cfg.username.is_some() || sentinel_cfg.password.is_some();
            let mut sconn =
                match RespConnection::establish(&starget, &sentinel_cfg, false, use_auth, None)
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        last_err = e;
                        continue;
                    }
                };
            let query = build_get_master_addr_cmd(master_name);
            let reply = match sconn.execute_single(&query).await {
                Ok(r) => r,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };
            let (mhost, mport) = match parse_get_master_addr_reply(&reply) {
                Ok(pair) => pair,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };

            let candidate = RespTarget::Tcp {
                host: mhost.clone(),
                port: mport,
            };
            let mut pconn = match RespConnection::establish(
                &candidate,
                &effective_cfg,
                false,
                true,
                self.tracking_cache.clone(),
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };
            if self.config.verify_sentinel_role {
                let role_cmd = vec![b"ROLE".to_vec()];
                match pconn.execute_single(&role_cmd).await {
                    Ok(role_resp) => {
                        if let Err(e) = verify_role_is_primary(&role_resp) {
                            last_err = e;
                            continue;
                        }
                    }
                    Err(e) => {
                        last_err = e;
                        continue;
                    }
                }
            }

            if pconn.tracking_negotiated {
                self.tracking_active.store(true, Ordering::Release);
            }
            {
                let mut guard = self.sentinel_primary.write().await;
                *guard = Some((mhost.clone(), mport));
            }
            self.return_connection(PoolRoute::Standalone, pconn).await;
            return Ok((mhost, mport));
        }

        Err(last_err)
    }

    fn sentinel_handshake_config(&self) -> RespConfig {
        let mut cfg = (*self.config).clone();
        cfg.db = 0;
        cfg.username = self.config.sentinel_username.clone();
        cfg.password = self.config.sentinel_password.clone();
        cfg.cluster_enabled = false;
        cfg.client_tracking_enabled = false;
        cfg.enable_gcp_memorystore_iam = false;
        cfg.gcp_auth = None;
        cfg.enable_capa_redirect = false;
        cfg
    }

    async fn checkout_connection(&self, route: &PoolRoute) -> Result<RespConnection, RespError> {
        self.ensure_open()?;
        let key = route.pool_key();
        let popped = {
            let mut pools = self.pools.lock().await;
            pools.get_mut(&key).and_then(Vec::pop)
        };
        if let Some(mut conn) = popped
            && conn.try_drain_pushes_nonblocking().await.is_ok()
        {
            return Ok(conn);
        }

        let effective_cfg = self.effective_config();
        let conn = match route {
            PoolRoute::Standalone => {
                let target = self.resolve_standalone_target().await?;
                if let Some(mut prewarmed) = {
                    let mut pools = self.pools.lock().await;
                    pools.get_mut(&key).and_then(Vec::pop)
                } && prewarmed.try_drain_pushes_nonblocking().await.is_ok()
                {
                    return Ok(prewarmed);
                }
                RespConnection::establish(
                    &target,
                    &effective_cfg,
                    false,
                    true,
                    self.tracking_cache.clone(),
                )
                .await?
            }
            PoolRoute::Endpoint {
                host,
                port,
                is_replica,
            } => {
                let target = RespTarget::Tcp {
                    host: host.clone(),
                    port: *port,
                };
                RespConnection::establish(
                    &target,
                    &effective_cfg,
                    *is_replica,
                    true,
                    self.tracking_cache.clone(),
                )
                .await?
            }
        };
        if conn.tracking_negotiated {
            self.tracking_active.store(true, Ordering::Release);
        }
        Ok(conn)
    }

    async fn return_connection(&self, route: PoolRoute, conn: RespConnection) {
        if self.is_closed() {
            return;
        }
        let max_size = self.config.pool_max_size.max(1);
        let key = route.pool_key();
        let mut pools = self.pools.lock().await;
        let bucket = pools.entry(key).or_default();
        if bucket.len() < max_size {
            bucket.push(conn);
        }
    }

    async fn drain_pooled_tracking_pushes(&self) {
        let mut taken: Vec<(String, RespConnection)> = Vec::new();
        {
            let mut pools = self.pools.lock().await;
            for (k, list) in pools.iter_mut() {
                while let Some(conn) = list.pop() {
                    taken.push((k.clone(), conn));
                }
            }
        }
        for (k, mut conn) in taken {
            if conn.try_drain_pushes_nonblocking().await.is_ok() && !self.is_closed() {
                let mut pools = self.pools.lock().await;
                let max_size = self.config.pool_max_size.max(1);
                let bucket = pools.entry(k).or_default();
                if bucket.len() < max_size {
                    bucket.push(conn);
                }
            }
        }
    }

    /// Refresh and store cluster topology (`CLUSTER SHARDS` with `CLUSTER SLOTS` fallback).
    pub async fn refresh_cluster_topology(&self) -> Result<ClusterTopology, RespError> {
        let mut candidates: Vec<ClusterNodeEndpoint> = Vec::new();
        {
            let existing = self.topology.read();
            candidates.extend(existing.primary_nodes());
        }
        for seed in &self.config.cluster_nodes {
            if let Ok((host, port)) = parse_sentinel_addr(seed, 6379) {
                candidates.push(ClusterNodeEndpoint { host, port });
            }
        }
        if candidates.is_empty() {
            candidates.push(ClusterNodeEndpoint {
                host: self.config.host.clone(),
                port: self.config.port,
            });
        }

        let mut seen = HashSet::new();
        let mut last_err = RespError::connection("Failed to discover Cluster topology");
        for ep in candidates {
            if !seen.insert(ep.addr_key()) {
                continue;
            }
            let route = PoolRoute::Endpoint {
                host: ep.host.clone(),
                port: ep.port,
                is_replica: false,
            };
            let mut conn = match self.checkout_connection(&route).await {
                Ok(c) => c,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };

            let shards_cmd = vec![b"CLUSTER".to_vec(), b"SHARDS".to_vec()];
            if let Ok(resp) = conn.execute_single(&shards_cmd).await
                && let Ok(topo) = ClusterTopology::from_cluster_shards(&resp, self.config.tls)
            {
                self.return_connection(route, conn).await;
                let mut guard = self.topology.write();
                *guard = topo.clone();
                return Ok(topo);
            }

            let slots_cmd = vec![b"CLUSTER".to_vec(), b"SLOTS".to_vec()];
            match conn.execute_single(&slots_cmd).await {
                Ok(resp) => match ClusterTopology::from_cluster_slots_with_fallback(
                    &resp,
                    Some(ep.host.as_str()),
                ) {
                    Ok(topo) => {
                        self.return_connection(route, conn).await;
                        let mut guard = self.topology.write();
                        *guard = topo.clone();
                        return Ok(topo);
                    }
                    Err(e) => {
                        last_err = e;
                    }
                },
                Err(e) => {
                    last_err = e;
                }
            }
        }
        Err(last_err)
    }

    async fn ensure_cluster_topology(&self) -> Result<ClusterTopology, RespError> {
        {
            let topo = self.topology.read();
            if topo.shard_count() > 0 {
                return Ok(topo.clone());
            }
        }
        match self.refresh_cluster_topology().await {
            Ok(t) => Ok(t),
            Err(e) => {
                let seed_opt = self.config.cluster_nodes.first().cloned().or_else(|| {
                    if !self.config.host.is_empty() {
                        Some(format!("{}:{}", self.config.host, self.config.port))
                    } else {
                        None
                    }
                });
                if let Some(seed) = seed_opt
                    && let Ok((host, port)) = parse_sentinel_addr(&seed, 6379)
                {
                    let fallback = ClusterTopology::initial_seed_fallback(host, port);
                    let mut guard = self.topology.write();
                    *guard = fallback.clone();
                    return Ok(fallback);
                }
                Err(e)
            }
        }
    }

    /// Execute a single command handling standalone, Sentinel failover, and Cluster routing/retries.
    pub async fn execute_command_routed(
        &self,
        args: Vec<Vec<u8>>,
        keys: &[&[u8]],
        read_only: bool,
    ) -> Result<RespValue, RespError> {
        self.ensure_open()?;
        if !self.config.cluster_enabled {
            return self.execute_standalone_or_sentinel(args).await;
        }
        self.execute_cluster_single(args, keys, read_only).await
    }

    async fn execute_standalone_or_sentinel(
        &self,
        args: Vec<Vec<u8>>,
    ) -> Result<RespValue, RespError> {
        let mut conn = self.checkout_connection(&PoolRoute::Standalone).await?;
        match conn.execute_single(&args).await {
            Ok(val) => {
                self.return_connection(PoolRoute::Standalone, conn).await;
                Ok(val)
            }
            Err(err) if self.config.sentinel_service_name.is_some() && should_failover(&err) => {
                {
                    let mut guard = self.sentinel_primary.write().await;
                    *guard = None;
                }
                {
                    let mut pools = self.pools.lock().await;
                    pools.remove("standalone");
                }
                self.discover_sentinel_primary().await?;
                let mut retry_conn = self.checkout_connection(&PoolRoute::Standalone).await?;
                let res = retry_conn.execute_single(&args).await;
                if res.is_ok() {
                    self.return_connection(PoolRoute::Standalone, retry_conn)
                        .await;
                }
                res
            }
            Err(err) => {
                if !is_connection_broken(&err) {
                    self.return_connection(PoolRoute::Standalone, conn).await;
                }
                Err(err)
            }
        }
    }

    async fn execute_cluster_single(
        &self,
        args: Vec<Vec<u8>>,
        keys: &[&[u8]],
        read_only: bool,
    ) -> Result<RespValue, RespError> {
        let slot = validate_cluster_keys_same_slot(keys)?;
        let _ = self.ensure_cluster_topology().await?;

        let mut override_route: Option<(ClusterNodeEndpoint, bool, bool)> = None;
        let max_redirs = self.config.max_redirections;

        for attempt in 0..=max_redirs {
            let (ep, is_ask, is_replica) = if let Some((ov_ep, ask, ro)) = override_route.take() {
                (ov_ep, ask, ro)
            } else {
                let picked = {
                    let topo = self.topology.read();
                    match slot {
                        Some(s) => topo
                            .route_for_slot(s, read_only, self.config.read_from_replicas)
                            .or_else(|| topo.any_primary().map(|p| (p, false))),
                        None => topo.any_primary().map(|p| (p, false)),
                    }
                };
                match picked {
                    Some((node, ro)) => (node, false, ro),
                    None => {
                        let refreshed = self.refresh_cluster_topology().await?;
                        match slot
                            .and_then(|s| {
                                refreshed.route_for_slot(
                                    s,
                                    read_only,
                                    self.config.read_from_replicas,
                                )
                            })
                            .or_else(|| refreshed.any_primary().map(|p| (p, false)))
                        {
                            Some((node, ro)) => (node, false, ro),
                            None => {
                                return Err(RespError::operational(
                                    "Cluster topology has no available shard nodes",
                                    true,
                                ));
                            }
                        }
                    }
                }
            };

            let route = PoolRoute::Endpoint {
                host: ep.host.clone(),
                port: ep.port,
                is_replica,
            };
            let mut conn = match self.checkout_connection(&route).await {
                Ok(c) => c,
                Err(_) if attempt < max_redirs => {
                    let _ = self.refresh_cluster_topology().await;
                    continue;
                }
                Err(e) => return Err(e),
            };

            let exec_res = if is_ask {
                let asking = vec![b"ASKING".to_vec()];
                let batch = vec![asking, args.clone()];
                match conn.execute_batch(&batch).await {
                    Ok(mut replies) => {
                        if replies.len() == 2 {
                            replies[0].clone().into_result()?;
                            replies.pop().expect("second reply exists").into_result()
                        } else {
                            Err(RespError::data("Unexpected ASK batch reply count"))
                        }
                    }
                    Err(e) => Err(e),
                }
            } else {
                conn.execute_single(&args).await
            };

            match exec_res {
                Ok(val) => {
                    self.return_connection(route, conn).await;
                    return Ok(val);
                }
                Err(err) => {
                    if !is_connection_broken(&err) {
                        self.return_connection(route, conn).await;
                    }
                    if let Some((kind, redir_slot, redir_host, redir_port)) =
                        parse_cluster_redirect(&err.message)
                    {
                        if attempt >= max_redirs {
                            return Err(RespError::operational(
                                format!(
                                    "Cluster redirection limit ({max_redirs}) exceeded: {}",
                                    err.message
                                ),
                                true,
                            ));
                        }
                        if kind == "MOVED" {
                            {
                                let mut topo = self.topology.write();
                                topo.update_moved_slot(redir_slot, &redir_host, redir_port);
                            }
                            override_route = Some((
                                ClusterNodeEndpoint {
                                    host: redir_host,
                                    port: redir_port,
                                },
                                false,
                                false,
                            ));
                            continue;
                        }
                        override_route = Some((
                            ClusterNodeEndpoint {
                                host: redir_host,
                                port: redir_port,
                            },
                            true,
                            false,
                        ));
                        continue;
                    }

                    if (matches!(
                        err.code.as_deref(),
                        Some("TRYAGAIN" | "CLUSTERDOWN" | "LOADING")
                    ) || is_connection_broken(&err))
                        && attempt < max_redirs
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        let _ = self.refresh_cluster_topology().await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        Err(RespError::operational(
            "Cluster command exceeded maximum redirections",
            true,
        ))
    }

    async fn execute_on_specific_endpoint(
        &self,
        ep: &ClusterNodeEndpoint,
        args: Vec<Vec<u8>>,
    ) -> Result<RespValue, RespError> {
        let route = PoolRoute::Endpoint {
            host: ep.host.clone(),
            port: ep.port,
            is_replica: false,
        };
        let mut conn = self.checkout_connection(&route).await?;
        let res = conn.execute_single(&args).await;
        if res.is_ok() || !res.as_ref().is_err_and(is_connection_broken) {
            self.return_connection(route, conn).await;
        }
        res
    }

    fn invalidate_local_cache_keys(&self, keys: &[Vec<u8>]) {
        if let Some(ref cache) = self.tracking_cache {
            for k in keys {
                cache.evict_local(k.as_slice());
            }
        }
    }

    /// Fetch key honoring L1 client tracking cache if enabled.
    pub async fn get_bytes(&self, key: Vec<u8>) -> Result<Option<Bytes>, RespError> {
        self.ensure_open()?;
        if let Some(ref cache) = self.tracking_cache {
            self.drain_pooled_tracking_pushes().await;
            if let Some(hit) = cache.get(&key) {
                return Ok(Some(hit));
            }
        }

        let cmd = vec![b"GET".to_vec(), key.clone()];
        let key_refs = [key.as_slice()];
        let resp = self.execute_command_routed(cmd, &key_refs, true).await?;
        let out = match resp.into_result()? {
            RespValue::Null => None,
            val => val.as_bytes().map(Bytes::copy_from_slice),
        };
        if let (Some(cache), Some(val_bytes)) = (&self.tracking_cache, &out) {
            cache.insert(Bytes::from(key), val_bytes.clone());
        }
        Ok(out)
    }

    pub async fn mget_bytes(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Bytes>>, RespError> {
        self.ensure_open()?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut cmd = Vec::with_capacity(1 + keys.len());
        cmd.push(b"MGET".to_vec());
        cmd.extend(keys.iter().cloned());
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let resp = self.execute_command_routed(cmd, &key_refs, true).await?;
        let items = match resp.into_result()? {
            RespValue::Array(arr) => arr,
            RespValue::Null => return Ok(vec![None; keys.len()]),
            other => {
                return Err(RespError::data(format!(
                    "Unexpected MGET response type: {other:?}"
                )));
            }
        };
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let checked = item.into_result()?;
            if matches!(checked, RespValue::Null) {
                out.push(None);
            } else {
                out.push(checked.as_bytes().map(Bytes::copy_from_slice));
            }
        }
        Ok(out)
    }

    pub async fn set_bytes(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        px: Option<i64>,
        ex: Option<i64>,
        nx: bool,
        xx: bool,
    ) -> Result<bool, RespError> {
        let cmd = build_set_cmd(&key, &value, px, ex, nx, xx)?;
        let key_refs = [key.as_slice()];
        let resp = self.execute_command_routed(cmd, &key_refs, false).await?;
        let success = resp.as_bool_ok().unwrap_or(false);
        if let Some(ref cache) = self.tracking_cache {
            if success {
                let ttl_override = px
                    .and_then(|ms| u64::try_from(ms).ok())
                    .map(Duration::from_millis)
                    .or_else(|| {
                        ex.and_then(|s| u64::try_from(s).ok())
                            .map(Duration::from_secs)
                    });
                cache.insert_with_ttl(Bytes::from(key), Bytes::from(value), ttl_override);
            } else {
                cache.evict_local(&key);
            }
        }
        Ok(success)
    }

    pub async fn multi_key_int_cmd(
        &self,
        op: &str,
        keys: Vec<Vec<u8>>,
        invalidate_cache: bool,
    ) -> Result<i64, RespError> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = Vec::with_capacity(1 + keys.len());
        cmd.push(op.as_bytes().to_vec());
        cmd.extend(keys.iter().cloned());
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let read_only = op.eq_ignore_ascii_case("EXISTS");
        let resp = self
            .execute_command_routed(cmd, &key_refs, read_only)
            .await?;
        if invalidate_cache {
            self.invalidate_local_cache_keys(&keys);
        }
        resp.as_i64()
            .ok_or_else(|| RespError::data(format!("Expected integer reply from {op}")))
    }

    pub async fn single_key_int_arg_cmd(
        &self,
        op: &str,
        key: Vec<u8>,
        arg: Option<i64>,
        read_only: bool,
    ) -> Result<i64, RespError> {
        let mut cmd = vec![op.as_bytes().to_vec(), key.clone()];
        if let Some(n) = arg {
            cmd.push(n.to_string().into_bytes());
        }
        let key_refs = [key.as_slice()];
        let resp = self
            .execute_command_routed(cmd, &key_refs, read_only)
            .await?;
        resp.as_i64()
            .ok_or_else(|| RespError::data(format!("Expected integer reply from {op}")))
    }

    pub async fn ping(&self) -> Result<bool, RespError> {
        let cmd = vec![b"PING".to_vec()];
        let resp = self.execute_command_routed(cmd, &[], true).await?;
        match resp.unattributed() {
            RespValue::SimpleString(b) | RespValue::BulkString(b) => {
                Ok(b.eq_ignore_ascii_case(b"PONG") || b.eq_ignore_ascii_case(b"OK"))
            }
            _ => Ok(true),
        }
    }

    pub async fn flushdb(&self, asynchronous: bool) -> Result<bool, RespError> {
        self.ensure_open()?;
        let mut cmd = vec![b"FLUSHDB".to_vec()];
        if asynchronous {
            cmd.push(b"ASYNC".to_vec());
        }
        if self.config.cluster_enabled {
            let topo = self.ensure_cluster_topology().await?;
            for ep in topo.primary_nodes() {
                let _ = self.execute_on_specific_endpoint(&ep, cmd.clone()).await?;
            }
        } else {
            let _ = self.execute_command_routed(cmd, &[], false).await?;
        }
        if let Some(ref cache) = self.tracking_cache {
            cache.invalidate_key(None);
        }
        Ok(true)
    }

    pub async fn publish(&self, channel: &str, message: Vec<u8>) -> Result<i64, RespError> {
        let cmd = vec![b"PUBLISH".to_vec(), channel.as_bytes().to_vec(), message];
        let resp = self.execute_command_routed(cmd, &[], false).await?;
        resp.as_i64()
            .ok_or_else(|| RespError::data("Expected integer from PUBLISH"))
    }

    pub async fn spublish(&self, shard_channel: &str, message: Vec<u8>) -> Result<i64, RespError> {
        let ch_bytes = shard_channel.as_bytes().to_vec();
        let cmd = vec![b"SPUBLISH".to_vec(), ch_bytes.clone(), message];
        let key_refs = [ch_bytes.as_slice()];
        let resp = self.execute_command_routed(cmd, &key_refs, false).await?;
        resp.as_i64()
            .ok_or_else(|| RespError::data("Expected integer from SPUBLISH"))
    }

    pub async fn scan(
        &self,
        cursor: u64,
        match_pattern: Option<String>,
        count: Option<u64>,
    ) -> Result<(u64, Vec<Bytes>), RespError> {
        let cmd = build_scan_cmd(cursor, match_pattern.as_deref(), count);
        let resp = self.execute_command_routed(cmd, &[], false).await?;
        parse_scan_reply(resp)
    }

    pub async fn scan_unlink_prefix(&self, prefix: Vec<u8>, count: u64) -> Result<i64, RespError> {
        self.ensure_open()?;
        let mut pattern = prefix;
        pattern.push(b'*');
        let pattern_str = String::from_utf8_lossy(&pattern).into_owned();
        let mut collected: Vec<Vec<u8>> = Vec::new();

        if self.config.cluster_enabled {
            let topo = self.ensure_cluster_topology().await?;
            for ep in topo.primary_nodes() {
                let mut cursor = 0u64;
                loop {
                    let cmd = build_scan_cmd(cursor, Some(&pattern_str), Some(count));
                    let resp = self.execute_on_specific_endpoint(&ep, cmd).await?;
                    let (next_cursor, batch) = parse_scan_reply(resp)?;
                    collected.extend(batch.into_iter().map(|b| b.to_vec()));
                    cursor = next_cursor;
                    if cursor == 0 {
                        break;
                    }
                }
            }
        } else {
            let mut cursor = 0u64;
            loop {
                let (next_cursor, batch) = self
                    .scan(cursor, Some(pattern_str.clone()), Some(count))
                    .await?;
                collected.extend(batch.into_iter().map(|b| b.to_vec()));
                cursor = next_cursor;
                if cursor == 0 {
                    break;
                }
            }
        }

        if collected.is_empty() {
            return Ok(0);
        }

        self.invalidate_local_cache_keys(&collected);
        let mut deleted = 0i64;
        if self.config.cluster_enabled {
            let mut by_slot: HashMap<u16, Vec<Vec<u8>>> = HashMap::new();
            for key in collected {
                let slot = hash_slot(&key);
                by_slot.entry(slot).or_default().push(key);
            }
            for (_slot, slot_keys) in by_slot {
                for chunk in slot_keys.chunks(128) {
                    deleted += self
                        .multi_key_int_cmd("UNLINK", chunk.to_vec(), false)
                        .await?;
                }
            }
        } else {
            for chunk in collected.chunks(128) {
                deleted += self
                    .multi_key_int_cmd("UNLINK", chunk.to_vec(), false)
                    .await?;
            }
        }
        Ok(deleted)
    }

    pub async fn script_load(&self, script: Vec<u8>) -> Result<String, RespError> {
        let cmd = vec![b"SCRIPT".to_vec(), b"LOAD".to_vec(), script];
        if self.config.cluster_enabled {
            let topo = self.ensure_cluster_topology().await?;
            let mut last_sha = String::new();
            for ep in topo.primary_nodes() {
                let resp = self.execute_on_specific_endpoint(&ep, cmd.clone()).await?;
                last_sha = resp
                    .as_str()
                    .map(std::borrow::Cow::into_owned)
                    .ok_or_else(|| RespError::data("SCRIPT LOAD returned non-string SHA1"))?;
            }
            if last_sha.is_empty() {
                let resp = self.execute_command_routed(cmd, &[], false).await?;
                last_sha = resp
                    .as_str()
                    .map(std::borrow::Cow::into_owned)
                    .ok_or_else(|| RespError::data("SCRIPT LOAD returned non-string SHA1"))?;
            }
            Ok(last_sha)
        } else {
            let resp = self.execute_command_routed(cmd, &[], false).await?;
            resp.as_str()
                .map(std::borrow::Cow::into_owned)
                .ok_or_else(|| RespError::data("SCRIPT LOAD returned non-string SHA1"))
        }
    }

    pub async fn eval_with_fallback(
        &self,
        script: Vec<u8>,
        keys: Vec<Vec<u8>>,
        args: Vec<Vec<u8>>,
    ) -> Result<RespValue, RespError> {
        let sha1 = sha1_hex(&script);
        self.evalsha_with_optional_fallback(sha1, keys, args, Some(script))
            .await
    }

    pub async fn evalsha_with_optional_fallback(
        &self,
        sha1: String,
        keys: Vec<Vec<u8>>,
        args: Vec<Vec<u8>>,
        fallback_script: Option<Vec<u8>>,
    ) -> Result<RespValue, RespError> {
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let sha_cmd = build_script_exec_cmd("EVALSHA", sha1.as_bytes(), &keys, &args);
        match self.execute_command_routed(sha_cmd, &key_refs, false).await {
            Ok(val) => Ok(val),
            Err(err) if err.is_noscript() => {
                if let Some(script_bytes) = fallback_script {
                    let eval_cmd = build_script_exec_cmd("EVAL", &script_bytes, &keys, &args);
                    self.execute_command_routed(eval_cmd, &key_refs, false)
                        .await
                } else {
                    Err(err)
                }
            }
            Err(err) => Err(err),
        }
    }

    /// Execute an arbitrary raw RESP command, classifying cluster key routing automatically.
    pub async fn execute_raw_command(&self, cmd: Vec<Vec<u8>>) -> Result<RespValue, RespError> {
        if cmd.is_empty() {
            return Err(RespError::programming(
                "execute_command requires a command name",
            ));
        }
        let (keys, read_only) = classify_generic_command(&cmd[0], &cmd);
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        self.execute_command_routed(cmd, &key_refs, read_only).await
    }

    /// Execute a list of raw commands as a pipeline (`atomic = true` wraps in `MULTI`/`EXEC`).
    pub async fn execute_raw_pipeline(
        &self,
        commands: Vec<Vec<Vec<u8>>>,
        atomic: bool,
    ) -> Result<Vec<RespValue>, RespError> {
        let queued: Vec<QueuedCommand> = commands
            .into_iter()
            .filter(|c| !c.is_empty())
            .map(|args| {
                let (keys, read_only) = classify_generic_command(&args[0], &args);
                QueuedCommand {
                    args,
                    keys,
                    read_only,
                    decoder: PipelineDecoder::GenericPy,
                }
            })
            .collect();
        let paired = self.execute_pipeline_commands(queued, atomic).await?;
        Ok(paired.into_iter().map(|(_, v)| v).collect())
    }

    // --- Stream & Sorted Set High-Level Helpers ---

    pub async fn xgroup_create(
        &self,
        key: Vec<u8>,
        group: &str,
        id: &str,
        mkstream: bool,
        ignore_busy: bool,
    ) -> Result<bool, RespError> {
        let cmd = build_xgroup_create(&key, group, id, mkstream);
        let key_refs = [key.as_slice()];
        match self.execute_command_routed(cmd, &key_refs, false).await {
            Ok(resp) => Ok(resp.as_bool_ok().unwrap_or(true)),
            Err(err) if ignore_busy && err.message.contains("BUSYGROUP") => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub async fn xadd(
        &self,
        key: Vec<u8>,
        fields: &[(Vec<u8>, Vec<u8>)],
        id: &str,
        maxlen: Option<u64>,
        approximate: bool,
        nomkstream: bool,
    ) -> Result<Bytes, RespError> {
        let cmd = build_xadd(&key, fields, id, maxlen, approximate, nomkstream)?;
        let key_refs = [key.as_slice()];
        let resp = self.execute_command_routed(cmd, &key_refs, false).await?;
        resp.as_bytes()
            .map(Bytes::copy_from_slice)
            .ok_or_else(|| RespError::data("XADD returned non-string entry id"))
    }

    pub async fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        streams: &[(Vec<u8>, String)],
        count: Option<u64>,
        block_ms: Option<u64>,
        noack: bool,
    ) -> Result<RespValue, RespError> {
        let cmd = build_xreadgroup(group, consumer, streams, count, block_ms, noack)?;
        let key_refs: Vec<&[u8]> = streams.iter().map(|(k, _)| k.as_slice()).collect();
        self.execute_command_routed(cmd, &key_refs, false).await
    }

    pub async fn xack(&self, key: Vec<u8>, group: &str, ids: &[String]) -> Result<i64, RespError> {
        let cmd = build_xack(&key, group, ids)?;
        let key_refs = [key.as_slice()];
        let resp = self.execute_command_routed(cmd, &key_refs, false).await?;
        resp.as_i64()
            .ok_or_else(|| RespError::data("Expected integer from XACK"))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn xautoclaim(
        &self,
        key: Vec<u8>,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start_id: &str,
        count: Option<u64>,
        justid: bool,
    ) -> Result<RespValue, RespError> {
        let cmd = build_xautoclaim(&key, group, consumer, min_idle_ms, start_id, count, justid);
        let key_refs = [key.as_slice()];
        self.execute_command_routed(cmd, &key_refs, false).await
    }

    pub async fn xrange(
        &self,
        key: Vec<u8>,
        min: &str,
        max: &str,
        count: Option<u64>,
    ) -> Result<RespValue, RespError> {
        let cmd = build_xrange(&key, min, max, count);
        let key_refs = [key.as_slice()];
        self.execute_command_routed(cmd, &key_refs, true).await
    }

    pub async fn xdel(&self, key: Vec<u8>, ids: &[String]) -> Result<i64, RespError> {
        let cmd = build_xdel(&key, ids)?;
        let key_refs = [key.as_slice()];
        let resp = self.execute_command_routed(cmd, &key_refs, false).await?;
        resp.as_i64()
            .ok_or_else(|| RespError::data("Expected integer from XDEL"))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn zadd(
        &self,
        key: Vec<u8>,
        members: &[(Vec<u8>, f64)],
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
    ) -> Result<i64, RespError> {
        let cmd = build_zadd(&key, members, nx, xx, gt, lt, ch)?;
        let key_refs = [key.as_slice()];
        let resp = self.execute_command_routed(cmd, &key_refs, false).await?;
        resp.as_i64()
            .ok_or_else(|| RespError::data("Expected integer from ZADD"))
    }

    pub async fn zrangebyscore(
        &self,
        key: Vec<u8>,
        min: &str,
        max: &str,
        withscores: bool,
        offset: Option<u64>,
        count: Option<u64>,
    ) -> Result<RespValue, RespError> {
        let cmd = build_zrangebyscore(&key, min, max, withscores, offset, count);
        let key_refs = [key.as_slice()];
        self.execute_command_routed(cmd, &key_refs, true).await
    }

    pub async fn zrem(&self, key: Vec<u8>, members: &[Vec<u8>]) -> Result<i64, RespError> {
        if members.is_empty() {
            return Ok(0);
        }
        let cmd = build_zrem(&key, members);
        let key_refs = [key.as_slice()];
        let resp = self.execute_command_routed(cmd, &key_refs, false).await?;
        resp.as_i64()
            .ok_or_else(|| RespError::data("Expected integer from ZREM"))
    }

    /// Execute a pipeline batch honoring transactions (`MULTI`/`EXEC`) or Cluster per-node batching.
    pub async fn execute_pipeline_commands(
        &self,
        commands: Vec<QueuedCommand>,
        transaction: bool,
    ) -> Result<Vec<(PipelineDecoder, RespValue)>, RespError> {
        self.ensure_open()?;
        if commands.is_empty() {
            return Ok(Vec::new());
        }

        if self.config.cluster_enabled {
            let all_keys: Vec<&[u8]> = commands
                .iter()
                .flat_map(|c| c.keys.iter().map(Vec::as_slice))
                .collect();
            if transaction {
                validate_cluster_keys_same_slot(&all_keys)?;
                return self.execute_cluster_transactional_pipeline(commands).await;
            }
            return self.execute_cluster_pipeline(commands).await;
        }

        self.execute_standalone_pipeline(commands, transaction)
            .await
    }

    async fn execute_standalone_pipeline(
        &self,
        commands: Vec<QueuedCommand>,
        transaction: bool,
    ) -> Result<Vec<(PipelineDecoder, RespValue)>, RespError> {
        let mut wire_cmds = Vec::with_capacity(commands.len() + if transaction { 2 } else { 0 });
        if transaction {
            wire_cmds.push(vec![b"MULTI".to_vec()]);
        }
        for c in &commands {
            wire_cmds.push(c.args.clone());
        }
        if transaction {
            wire_cmds.push(vec![b"EXEC".to_vec()]);
        }

        let raw_replies = match self.send_standalone_batch(&wire_cmds).await {
            Ok(r) => r,
            Err(err) if self.config.sentinel_service_name.is_some() && should_failover(&err) => {
                {
                    let mut guard = self.sentinel_primary.write().await;
                    *guard = None;
                }
                {
                    let mut pools = self.pools.lock().await;
                    pools.remove("standalone");
                }
                self.discover_sentinel_primary().await?;
                self.send_standalone_batch(&wire_cmds).await?
            }
            Err(err) => return Err(err),
        };

        for c in &commands {
            if !c.read_only {
                self.invalidate_local_cache_keys(&c.keys);
            }
        }

        extract_pipeline_results(commands, raw_replies, transaction)
    }

    async fn send_standalone_batch(
        &self,
        wire_cmds: &[Vec<Vec<u8>>],
    ) -> Result<Vec<RespValue>, RespError> {
        let mut conn = self.checkout_connection(&PoolRoute::Standalone).await?;
        let res = conn.execute_batch(wire_cmds).await;
        if res.is_ok() || !res.as_ref().is_err_and(is_connection_broken) {
            self.return_connection(PoolRoute::Standalone, conn).await;
        }
        res
    }

    async fn execute_cluster_transactional_pipeline(
        &self,
        commands: Vec<QueuedCommand>,
    ) -> Result<Vec<(PipelineDecoder, RespValue)>, RespError> {
        let topo = self.ensure_cluster_topology().await?;
        let first_slot = commands
            .iter()
            .find_map(|c| c.keys.first().map(|k| hash_slot(k)));
        let ep = first_slot
            .and_then(|s| topo.route_for_slot(s, false, false).map(|(e, _)| e))
            .or_else(|| topo.any_primary())
            .ok_or_else(|| RespError::operational("No cluster node available", true))?;

        let mut wire_cmds = Vec::with_capacity(commands.len() + 2);
        wire_cmds.push(vec![b"MULTI".to_vec()]);
        for c in &commands {
            wire_cmds.push(c.args.clone());
        }
        wire_cmds.push(vec![b"EXEC".to_vec()]);

        let route = PoolRoute::Endpoint {
            host: ep.host,
            port: ep.port,
            is_replica: false,
        };
        let mut conn = self.checkout_connection(&route).await?;
        let replies = conn.execute_batch(&wire_cmds).await;
        if replies.is_ok() || !replies.as_ref().is_err_and(is_connection_broken) {
            self.return_connection(route, conn).await;
        }
        let raw_replies = replies?;
        for c in &commands {
            if !c.read_only {
                self.invalidate_local_cache_keys(&c.keys);
            }
        }
        extract_pipeline_results(commands, raw_replies, true)
    }

    async fn execute_cluster_pipeline(
        &self,
        commands: Vec<QueuedCommand>,
    ) -> Result<Vec<(PipelineDecoder, RespValue)>, RespError> {
        let topo = self.ensure_cluster_topology().await?;
        let mut slot_checks = Vec::with_capacity(commands.len());
        for c in &commands {
            let key_refs: Vec<&[u8]> = c.keys.iter().map(Vec::as_slice).collect();
            let slot = validate_cluster_keys_same_slot(&key_refs)?;
            slot_checks.push(slot);
        }

        let mut groups: HashMap<String, (ClusterNodeEndpoint, bool, Vec<usize>)> = HashMap::new();
        for (idx, (cmd, slot)) in commands.iter().zip(slot_checks.iter()).enumerate() {
            let (ep, is_replica) = slot
                .and_then(|s| topo.route_for_slot(s, cmd.read_only, self.config.read_from_replicas))
                .or_else(|| topo.any_primary().map(|p| (p, false)))
                .ok_or_else(|| RespError::operational("No cluster shard found", true))?;
            let key = format!("{}:{}:{}", ep.host, ep.port, is_replica);
            groups
                .entry(key)
                .or_insert_with(|| (ep, is_replica, Vec::new()))
                .2
                .push(idx);
        }

        let mut results: Vec<Option<(PipelineDecoder, RespValue)>> = vec![None; commands.len()];
        for (_gkey, (ep, is_replica, indices)) in groups {
            let wire_cmds: Vec<Vec<Vec<u8>>> =
                indices.iter().map(|&i| commands[i].args.clone()).collect();
            let route = PoolRoute::Endpoint {
                host: ep.host,
                port: ep.port,
                is_replica,
            };
            let batch_res = {
                let mut conn = self.checkout_connection(&route).await?;
                let r = conn.execute_batch(&wire_cmds).await;
                if r.is_ok() || !r.as_ref().is_err_and(is_connection_broken) {
                    self.return_connection(route, conn).await;
                }
                r
            };

            match batch_res {
                Ok(replies) if replies.len() == indices.len() => {
                    for (&cmd_idx, reply) in indices.iter().zip(replies) {
                        if matches!(
                            reply.unattributed(),
                            RespValue::SimpleError(m) | RespValue::BulkError(m)
                                if parse_cluster_redirect(m).is_some()
                        ) {
                            let cmd = &commands[cmd_idx];
                            let key_refs: Vec<&[u8]> = cmd.keys.iter().map(Vec::as_slice).collect();
                            let redirected = self
                                .execute_cluster_single(cmd.args.clone(), &key_refs, cmd.read_only)
                                .await?;
                            results[cmd_idx] = Some((cmd.decoder.clone(), redirected));
                        } else {
                            results[cmd_idx] = Some((commands[cmd_idx].decoder.clone(), reply));
                        }
                    }
                }
                Ok(_) => {
                    return Err(RespError::data("Cluster pipeline reply count mismatch"));
                }
                Err(_) => {
                    for &cmd_idx in &indices {
                        let cmd = &commands[cmd_idx];
                        let key_refs: Vec<&[u8]> = cmd.keys.iter().map(Vec::as_slice).collect();
                        let reply = self
                            .execute_cluster_single(cmd.args.clone(), &key_refs, cmd.read_only)
                            .await?;
                        results[cmd_idx] = Some((cmd.decoder.clone(), reply));
                    }
                }
            }
        }

        for c in &commands {
            if !c.read_only {
                self.invalidate_local_cache_keys(&c.keys);
            }
        }

        let mut out = Vec::with_capacity(commands.len());
        for slot_res in results {
            out.push(slot_res.ok_or_else(|| RespError::data("Missing cluster pipeline reply"))?);
        }
        Ok(out)
    }
}

fn should_failover(err: &RespError) -> bool {
    is_connection_broken(err)
        || matches!(
            err.code.as_deref(),
            Some("READONLY" | "MASTERDOWN" | "CLUSTERDOWN" | "LOADING")
        )
}

fn is_connection_broken(err: &RespError) -> bool {
    err.kind == RespErrorKind::ConnectionFailure
}

fn build_scan_cmd(cursor: u64, match_pattern: Option<&str>, count: Option<u64>) -> Vec<Vec<u8>> {
    let mut cmd = vec![b"SCAN".to_vec(), cursor.to_string().into_bytes()];
    if let Some(pat) = match_pattern {
        cmd.push(b"MATCH".to_vec());
        cmd.push(pat.as_bytes().to_vec());
    }
    if let Some(c) = count {
        cmd.push(b"COUNT".to_vec());
        cmd.push(c.to_string().into_bytes());
    }
    cmd
}

fn parse_scan_reply(resp: RespValue) -> Result<(u64, Vec<Bytes>), RespError> {
    let val = resp.into_result()?;
    let Some(parts) = val.as_slice() else {
        return Err(RespError::data("Unexpected non-array SCAN response"));
    };
    if parts.len() < 2 {
        return Err(RespError::data("SCAN response truncated"));
    }
    let next_cursor = parts[0]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| parts[0].as_i64().and_then(|n| u64::try_from(n).ok()))
        .ok_or_else(|| RespError::data("Invalid cursor in SCAN reply"))?;
    let mut keys = Vec::new();
    if let Some(items) = parts[1].as_slice() {
        keys.reserve(items.len());
        for item in items {
            if let Some(raw) = item.as_bytes() {
                keys.push(Bytes::copy_from_slice(raw));
            }
        }
    }
    Ok((next_cursor, keys))
}

/// Build `EVAL` or `EVALSHA` command arguments.
#[must_use]
pub fn build_script_exec_cmd(
    op: &str,
    script_or_sha: &[u8],
    keys: &[Vec<u8>],
    args: &[Vec<u8>],
) -> Vec<Vec<u8>> {
    let mut cmd = Vec::with_capacity(3 + keys.len() + args.len());
    cmd.push(op.as_bytes().to_vec());
    cmd.push(script_or_sha.to_vec());
    cmd.push(keys.len().to_string().into_bytes());
    cmd.extend(keys.iter().cloned());
    cmd.extend(args.iter().cloned());
    cmd
}

/// Build `HSET <key> <field> <value> ...`.
#[must_use]
pub fn build_hset_cmd(key: &[u8], pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    let mut cmd = Vec::with_capacity(2 + pairs.len() * 2);
    cmd.push(b"HSET".to_vec());
    cmd.push(key.to_vec());
    for (f, v) in pairs {
        cmd.push(f.clone());
        cmd.push(v.clone());
    }
    cmd
}

/// Build `SET <key> <value> [PX ms | EX s] [NX | XX]`.
pub fn build_set_cmd(
    key: &[u8],
    value: &[u8],
    px: Option<i64>,
    ex: Option<i64>,
    nx: bool,
    xx: bool,
) -> Result<Vec<Vec<u8>>, RespError> {
    if nx && xx {
        return Err(RespError::programming(
            "SET NX and XX flags are mutually exclusive",
        ));
    }
    if px.is_some() && ex.is_some() {
        return Err(RespError::programming(
            "SET px and ex options are mutually exclusive",
        ));
    }
    let mut cmd = vec![b"SET".to_vec(), key.to_vec(), value.to_vec()];
    if let Some(ms) = px {
        cmd.push(b"PX".to_vec());
        cmd.push(ms.to_string().into_bytes());
    } else if let Some(secs) = ex {
        cmd.push(b"EX".to_vec());
        cmd.push(secs.to_string().into_bytes());
    }
    if nx {
        cmd.push(b"NX".to_vec());
    } else if xx {
        cmd.push(b"XX".to_vec());
    }
    Ok(cmd)
}

/// Classify a generic RESP command into `(cluster_routing_keys, read_only)`.
#[must_use]
pub fn classify_generic_command(cmd_name: &[u8], all_args: &[Vec<u8>]) -> (Vec<Vec<u8>>, bool) {
    let upper = String::from_utf8_lossy(cmd_name).to_ascii_uppercase();
    let keyless = matches!(
        upper.as_str(),
        "PING"
            | "INFO"
            | "CLIENT"
            | "CLUSTER"
            | "DBSIZE"
            | "FLUSHDB"
            | "FLUSHALL"
            | "SELECT"
            | "HELLO"
            | "ROLE"
            | "SCRIPT"
            | "SCAN"
            | "TIME"
            | "QUIT"
            | "ECHO"
            | "AUTH"
            | "SENTINEL"
            | "PUBLISH"
    );
    let keys = if keyless || all_args.len() < 2 {
        Vec::new()
    } else {
        vec![all_args[1].clone()]
    };
    (keys, is_read_only_command(cmd_name))
}

fn extract_pipeline_results(
    commands: Vec<QueuedCommand>,
    raw_replies: Vec<RespValue>,
    transaction: bool,
) -> Result<Vec<(PipelineDecoder, RespValue)>, RespError> {
    if !transaction {
        if raw_replies.len() != commands.len() {
            return Err(RespError::data(format!(
                "Pipeline reply count {} did not match sent commands {}",
                raw_replies.len(),
                commands.len()
            )));
        }
        return Ok(commands
            .into_iter()
            .zip(raw_replies)
            .map(|(c, r)| (c.decoder, r))
            .collect());
    }

    if raw_replies.len() != commands.len() + 2 {
        return Err(RespError::data("Transactional MULTI/EXEC reply truncated"));
    }
    for queued_ack in &raw_replies[..commands.len() + 1] {
        queued_ack.clone().into_result()?;
    }
    let exec_reply = raw_replies
        .into_iter()
        .last()
        .expect("MULTI/EXEC length checked")
        .into_result()?;
    if matches!(exec_reply, RespValue::Null) {
        return Err(RespError::operational(
            "Transactional EXEC aborted (WATCH conflict or EXECABORT)",
            true,
        ));
    }
    let RespValue::Array(exec_items) = exec_reply else {
        return Err(RespError::data("EXEC returned non-array reply"));
    };
    if exec_items.len() != commands.len() {
        return Err(RespError::data(format!(
            "EXEC result count {} did not match queued commands {}",
            exec_items.len(),
            commands.len()
        )));
    }
    Ok(commands
        .into_iter()
        .zip(exec_items)
        .map(|(c, r)| (c.decoder, r))
        .collect())
}

/// Shared PyO3 argument extraction, reply conversion, and pipeline queuing helpers.
#[cfg(feature = "pyo3")]
pub mod py_helpers {
    use std::collections::HashMap;
    use std::sync::Arc;

    use pyo3::exceptions::PyTypeError;
    use pyo3::prelude::*;
    use pyo3::types::{
        PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PySequence, PyString, PyTuple,
    };

    use super::{
        PipelineDecoder, QueuedCommand, RespEngine, SyncMutex, build_hset_cmd,
        build_script_exec_cmd, build_set_cmd, classify_generic_command,
    };
    use crate::codec::{
        RespValue, build_hash_expire_cmd, build_hash_field_query_cmd, decode_int_list, resp_to_py,
    };
    use crate::error::RespError;
    use crate::pubsub::extract_string_args;
    use crate::streams::{build_xack, build_xadd, build_xdel, build_zadd};

    pub fn slot_map_to_py_dict(
        py: Python<'_>,
        map: HashMap<u16, (String, u16)>,
    ) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for (slot, (host, port)) in map {
            let tuple = (host, port).into_pyobject(py)?;
            dict.set_item(slot, tuple)?;
        }
        Ok(dict.into_any().unbind())
    }

    pub fn decode_hgetall_py(py: Python<'_>, resp: RespValue) -> PyResult<Py<PyAny>> {
        let val = resp.into_result().map_err(|e| e.to_py_err(py))?;
        let dict = PyDict::new(py);
        match val {
            RespValue::Map(pairs) => {
                for (k, v) in pairs {
                    if let (Some(kb), Some(vb)) = (k.as_bytes(), v.as_bytes()) {
                        dict.set_item(PyBytes::new(py, kb), PyBytes::new(py, vb))?;
                    }
                }
            }
            RespValue::Array(items) => {
                for [k, v] in items.as_chunks::<2>().0 {
                    if let (Some(kb), Some(vb)) = (k.as_bytes(), v.as_bytes()) {
                        dict.set_item(PyBytes::new(py, kb), PyBytes::new(py, vb))?;
                    }
                }
            }
            RespValue::Null => {}
            other => {
                return Err(
                    RespError::data(format!("Unexpected HGETALL response: {other:?}"))
                        .to_py_err(py),
                );
            }
        }
        Ok(dict.into_any().unbind())
    }

    pub fn decode_pipeline_py(
        py: Python<'_>,
        items: Vec<(PipelineDecoder, RespValue)>,
    ) -> PyResult<Py<PyAny>> {
        let list = PyList::empty(py);
        for (decoder, resp) in items {
            let obj = match decoder {
                PipelineDecoder::BoolOk => {
                    let v = resp.into_result().map_err(|e| e.to_py_err(py))?;
                    PyBool::new(py, v.as_bool_ok().unwrap_or(false))
                        .to_owned()
                        .into_any()
                        .unbind()
                }
                PipelineDecoder::Int => {
                    let v = resp.into_result().map_err(|e| e.to_py_err(py))?;
                    let n = v.as_i64().ok_or_else(|| {
                        RespError::data("Expected integer reply inside pipeline").to_py_err(py)
                    })?;
                    n.into_pyobject(py)?.into_any().unbind()
                }
                PipelineDecoder::BytesOrNone => {
                    let v = resp.into_result().map_err(|e| e.to_py_err(py))?;
                    if matches!(v, RespValue::Null) {
                        py.None()
                    } else if let Some(b) = v.as_bytes() {
                        PyBytes::new(py, b).into_any().unbind()
                    } else {
                        py.None()
                    }
                }
                PipelineDecoder::HGetAllDict => decode_hgetall_py(py, resp)?,
                PipelineDecoder::IntList => {
                    let ints = decode_int_list(resp).map_err(|e| e.to_py_err(py))?;
                    ints.into_pyobject(py)?.into_any().unbind()
                }
                PipelineDecoder::StringOrBytes => {
                    let v = resp.into_result().map_err(|e| e.to_py_err(py))?;
                    if matches!(v, RespValue::Null) {
                        py.None()
                    } else if let Some(s) = v.as_str() {
                        s.as_ref().into_pyobject(py)?.into_any().unbind()
                    } else {
                        py.None()
                    }
                }
                PipelineDecoder::GenericPy => resp_to_py(py, resp)?,
            };
            list.append(obj)?;
        }
        Ok(list.into_any().unbind())
    }

    pub fn extract_bytes_arg(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
        if obj.is_instance_of::<PyString>() {
            return Ok(obj.cast::<PyString>()?.to_str()?.as_bytes().to_vec());
        }
        if obj.is_instance_of::<PyBytes>() {
            return Ok(obj.cast::<PyBytes>()?.as_bytes().to_vec());
        }
        obj.extract::<Vec<u8>>()
    }

    pub fn extract_command_arg(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
        if obj.is_instance_of::<PyString>() {
            return Ok(obj.cast::<PyString>()?.to_str()?.as_bytes().to_vec());
        }
        if obj.is_instance_of::<PyBytes>() {
            return Ok(obj.cast::<PyBytes>()?.as_bytes().to_vec());
        }
        if obj.is_instance_of::<PyInt>() && !obj.is_instance_of::<PyBool>() {
            return Ok(obj.extract::<i64>()?.to_string().into_bytes());
        }
        if obj.is_instance_of::<PyFloat>() {
            return Ok(obj.extract::<f64>()?.to_string().into_bytes());
        }
        obj.extract::<Vec<u8>>()
    }

    /// Permissive Python-to-RESP byte serializer preserving `_redis` coercion semantics.
    pub fn py_to_redis_arg(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
        if let Ok(bytes) = obj.cast::<PyBytes>() {
            Ok(bytes.as_bytes().to_vec())
        } else if let Ok(s) = obj.cast::<PyString>() {
            Ok(s.to_str()?.as_bytes().to_vec())
        } else if let Ok(b) = obj.cast::<PyBool>() {
            Ok(if b.is_true() {
                b"1".to_vec()
            } else {
                b"0".to_vec()
            })
        } else if let Ok(i) = obj.cast::<PyInt>() {
            let val: i64 = i.extract()?;
            Ok(val.to_string().into_bytes())
        } else if let Ok(f) = obj.cast::<PyFloat>() {
            let val: f64 = f.extract()?;
            Ok(val.to_string().into_bytes())
        } else if obj.is_none() {
            Ok(Vec::new())
        } else {
            let s = obj.str()?;
            Ok(s.to_str()?.as_bytes().to_vec())
        }
    }

    /// Convert a Python iterable of Redis arguments into byte vectors.
    pub fn py_seq_to_redis_args(seq: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
        let mut args = Vec::new();
        for item in seq.try_iter()? {
            let item = item?;
            args.push(py_to_redis_arg(&item)?);
        }
        Ok(args)
    }

    pub fn parse_redis_fields(fields: &Bound<'_, PyAny>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        if let Ok(dict) = fields.cast::<PyDict>() {
            for (k, v) in dict.iter() {
                out.push((py_to_redis_arg(&k)?, py_to_redis_arg(&v)?));
            }
        } else if let Ok(seq) = fields.cast::<PySequence>() {
            for item in seq.try_iter()? {
                let item = item?;
                if let Ok(tuple) = item.cast::<PyTuple>()
                    && tuple.len() >= 2
                {
                    let k = py_to_redis_arg(&tuple.get_item(0)?)?;
                    let v = py_to_redis_arg(&tuple.get_item(1)?)?;
                    out.push((k, v));
                }
            }
        }
        Ok(out)
    }

    pub fn parse_redis_zadd_mapping(mapping: &Bound<'_, PyAny>) -> PyResult<Vec<(Vec<u8>, f64)>> {
        let mut out = Vec::new();
        if let Ok(dict) = mapping.cast::<PyDict>() {
            for (k, v) in dict.iter() {
                let member_bytes = py_to_redis_arg(&k)?;
                let score: f64 = v.extract()?;
                out.push((member_bytes, score));
            }
        }
        Ok(out)
    }

    pub fn parse_redis_streams_arg(
        streams_obj: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<(Vec<u8>, String)>> {
        let mut out = Vec::new();
        if let Ok(dict) = streams_obj.cast::<PyDict>() {
            for (k, v) in dict.iter() {
                let stream_name = k.str()?.to_str()?.as_bytes().to_vec();
                let id = v.str()?.to_str()?.to_string();
                out.push((stream_name, id));
            }
        } else if let Ok(seq) = streams_obj.cast::<PySequence>() {
            for item in seq.try_iter()? {
                let item = item?;
                if let Ok(tuple) = item.cast::<PyTuple>()
                    && tuple.len() >= 2
                {
                    let s = tuple.get_item(0)?.str()?.to_str()?.as_bytes().to_vec();
                    let id = tuple.get_item(1)?.str()?.to_str()?.to_string();
                    out.push((s, id));
                }
            }
        }
        Ok(out)
    }

    pub fn extract_bytes_list(seq: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
        if seq.is_instance_of::<PyString>() || seq.is_instance_of::<PyBytes>() {
            return Err(PyTypeError::new_err(
                "Expected sequence of bytes or str keys",
            ));
        }
        let items = seq.extract::<Vec<Bound<'_, PyAny>>>()?;
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            out.push(extract_bytes_arg(&item)?);
        }
        Ok(out)
    }

    pub fn extract_varargs_bytes(args: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
        let items = args.extract::<Vec<Bound<'_, PyAny>>>()?;
        if items.len() == 1
            && !items[0].is_instance_of::<PyString>()
            && !items[0].is_instance_of::<PyBytes>()
            && items[0].cast::<PySequence>().is_ok()
        {
            return extract_bytes_list(&items[0]);
        }
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            out.push(extract_bytes_arg(&item)?);
        }
        Ok(out)
    }

    pub fn extract_pairs_bytes(
        py: Python<'_>,
        mapping: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        if let Ok(dict) = mapping.cast::<PyDict>() {
            for (k, v) in dict.iter() {
                out.push((extract_bytes_arg(&k)?, extract_bytes_arg(&v)?));
            }
        } else if let Ok(items_iter) = mapping.call_method0("items") {
            for entry in items_iter.try_iter()? {
                let pair = entry?;
                let (k, v): (Bound<'_, PyAny>, Bound<'_, PyAny>) = pair.extract()?;
                out.push((extract_bytes_arg(&k)?, extract_bytes_arg(&v)?));
            }
        } else {
            for entry in mapping.try_iter()? {
                let pair = entry?;
                let (k, v): (Bound<'_, PyAny>, Bound<'_, PyAny>) = pair.extract()?;
                out.push((extract_bytes_arg(&k)?, extract_bytes_arg(&v)?));
            }
        }
        if out.is_empty() {
            return Err(RespError::programming("Field mapping cannot be empty").to_py_err(py));
        }
        Ok(out)
    }

    pub fn extract_hset_pairs(
        py: Python<'_>,
        mapping_or_field: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if let Some(val_obj) = value {
            return Ok(vec![(
                extract_bytes_arg(mapping_or_field)?,
                extract_bytes_arg(val_obj)?,
            )]);
        }
        extract_pairs_bytes(py, mapping_or_field)
    }

    pub fn extract_streams_arg(streams: &Bound<'_, PyAny>) -> PyResult<Vec<(Vec<u8>, String)>> {
        let mut out = Vec::new();
        if let Ok(dict) = streams.cast::<PyDict>() {
            for (k, v) in dict.iter() {
                out.push((extract_bytes_arg(&k)?, v.extract::<String>()?));
            }
        } else if let Ok(items_iter) = streams.call_method0("items") {
            for entry in items_iter.try_iter()? {
                let pair = entry?;
                let (k, v): (Bound<'_, PyAny>, String) = pair.extract()?;
                out.push((extract_bytes_arg(&k)?, v));
            }
        } else {
            for entry in streams.try_iter()? {
                let pair = entry?;
                let (k, v): (Bound<'_, PyAny>, String) = pair.extract()?;
                out.push((extract_bytes_arg(&k)?, v));
            }
        }
        Ok(out)
    }

    pub fn extract_zadd_members(
        py: Python<'_>,
        members: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<(Vec<u8>, f64)>> {
        let mut out = Vec::new();
        if let Ok(dict) = members.cast::<PyDict>() {
            for (k, v) in dict.iter() {
                out.push((extract_bytes_arg(&k)?, v.extract::<f64>()?));
            }
        } else if let Ok(items_iter) = members.call_method0("items") {
            for entry in items_iter.try_iter()? {
                let pair = entry?;
                let (k, v): (Bound<'_, PyAny>, f64) = pair.extract()?;
                out.push((extract_bytes_arg(&k)?, v));
            }
        } else {
            for entry in members.try_iter()? {
                let pair = entry?;
                let (k, v): (Bound<'_, PyAny>, f64) = pair.extract()?;
                out.push((extract_bytes_arg(&k)?, v));
            }
        }
        if out.is_empty() {
            return Err(
                RespError::programming("ZADD requires at least one score/member pair")
                    .to_py_err(py),
            );
        }
        Ok(out)
    }

    pub fn parse_optional_keys(keys: Option<Bound<'_, PyAny>>) -> PyResult<Option<Vec<Vec<u8>>>> {
        match keys {
            None => Ok(None),
            Some(k) if k.is_none() => Ok(None),
            Some(k) => {
                let mut out = Vec::new();
                for item in k.try_iter()? {
                    let item = item?;
                    if let Ok(bytes) = item.extract::<&[u8]>() {
                        out.push(bytes.to_vec());
                    } else if let Ok(s) = item.extract::<&str>() {
                        out.push(s.as_bytes().to_vec());
                    } else {
                        return Err(PyTypeError::new_err("keys must contain str or bytes"));
                    }
                }
                Ok(Some(out))
            }
        }
    }

    /// Mutable interior command queue shared by sync and async pipeline classes.
    pub struct PipelineBuilder {
        pub engine: Arc<RespEngine>,
        pub transaction: bool,
        queue: SyncMutex<Vec<QueuedCommand>>,
    }

    #[allow(clippy::too_many_arguments)]
    impl PipelineBuilder {
        #[must_use]
        pub fn new(engine: Arc<RespEngine>, transaction: bool) -> Self {
            Self {
                engine,
                transaction,
                queue: SyncMutex::new(Vec::new()),
            }
        }

        pub fn push(&self, cmd: QueuedCommand) {
            self.queue.lock().push(cmd);
        }

        #[must_use]
        pub fn len(&self) -> usize {
            self.queue.lock().len()
        }

        #[must_use]
        pub fn is_empty(&self) -> bool {
            self.queue.lock().is_empty()
        }

        pub fn clear(&self) {
            self.queue.lock().clear();
        }

        pub fn drain_all(&self) -> Vec<QueuedCommand> {
            let mut guard = self.queue.lock();
            std::mem::take(&mut *guard)
        }

        pub fn queue_get(&self, key: &Bound<'_, PyAny>) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            self.push(QueuedCommand {
                args: vec![b"GET".to_vec(), kb.clone()],
                keys: vec![kb],
                read_only: true,
                decoder: PipelineDecoder::BytesOrNone,
            });
            Ok(())
        }

        pub fn queue_set(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            value: &Bound<'_, PyAny>,
            px: Option<i64>,
            ex: Option<i64>,
            nx: bool,
            xx: bool,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            let vb = extract_bytes_arg(value)?;
            let cmd = build_set_cmd(&kb, &vb, px, ex, nx, xx).map_err(|e| e.to_py_err(py))?;
            self.push(QueuedCommand {
                args: cmd,
                keys: vec![kb],
                read_only: false,
                decoder: PipelineDecoder::BoolOk,
            });
            Ok(())
        }

        pub fn queue_multi_key_int(
            &self,
            op: &str,
            keys: &Bound<'_, PyAny>,
            read_only: bool,
        ) -> PyResult<()> {
            let key_list = extract_varargs_bytes(keys)?;
            let mut args = Vec::with_capacity(1 + key_list.len());
            args.push(op.as_bytes().to_vec());
            args.extend(key_list.iter().cloned());
            self.push(QueuedCommand {
                args,
                keys: key_list,
                read_only,
                decoder: PipelineDecoder::Int,
            });
            Ok(())
        }

        pub fn queue_expire_like(
            &self,
            op: &str,
            key: &Bound<'_, PyAny>,
            val: i64,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            self.push(QueuedCommand {
                args: vec![
                    op.as_bytes().to_vec(),
                    kb.clone(),
                    val.to_string().into_bytes(),
                ],
                keys: vec![kb],
                read_only: false,
                decoder: PipelineDecoder::BoolOk,
            });
            Ok(())
        }

        pub fn queue_ttl_like(&self, op: &str, key: &Bound<'_, PyAny>) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            self.push(QueuedCommand {
                args: vec![op.as_bytes().to_vec(), kb.clone()],
                keys: vec![kb],
                read_only: true,
                decoder: PipelineDecoder::Int,
            });
            Ok(())
        }

        pub fn queue_incr_like(
            &self,
            op: &str,
            key: &Bound<'_, PyAny>,
            amount: i64,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            self.push(QueuedCommand {
                args: vec![
                    op.as_bytes().to_vec(),
                    kb.clone(),
                    amount.to_string().into_bytes(),
                ],
                keys: vec![kb],
                read_only: false,
                decoder: PipelineDecoder::Int,
            });
            Ok(())
        }

        pub fn queue_hset(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            mapping: &Bound<'_, PyAny>,
            value: Option<&Bound<'_, PyAny>>,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            let pairs = extract_hset_pairs(py, mapping, value)?;
            let cmd = build_hset_cmd(&kb, &pairs);
            self.push(QueuedCommand {
                args: cmd,
                keys: vec![kb],
                read_only: false,
                decoder: PipelineDecoder::Int,
            });
            Ok(())
        }

        pub fn queue_hget(&self, key: &Bound<'_, PyAny>, field: &Bound<'_, PyAny>) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            let fb = extract_bytes_arg(field)?;
            self.push(QueuedCommand {
                args: vec![b"HGET".to_vec(), kb.clone(), fb],
                keys: vec![kb],
                read_only: true,
                decoder: PipelineDecoder::BytesOrNone,
            });
            Ok(())
        }

        pub fn queue_hgetall(&self, key: &Bound<'_, PyAny>) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            self.push(QueuedCommand {
                args: vec![b"HGETALL".to_vec(), kb.clone()],
                keys: vec![kb],
                read_only: true,
                decoder: PipelineDecoder::HGetAllDict,
            });
            Ok(())
        }

        pub fn queue_hdel(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            fields: &Bound<'_, PyAny>,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            let fl = extract_varargs_bytes(fields)?;
            if fl.is_empty() {
                return Err(
                    RespError::programming("HDEL requires at least one field").to_py_err(py)
                );
            }
            let mut args = Vec::with_capacity(2 + fl.len());
            args.push(b"HDEL".to_vec());
            args.push(kb.clone());
            args.extend(fl);
            self.push(QueuedCommand {
                args,
                keys: vec![kb],
                read_only: false,
                decoder: PipelineDecoder::Int,
            });
            Ok(())
        }

        pub fn queue_hash_expire(
            &self,
            py: Python<'_>,
            cmd_name: &str,
            key: &Bound<'_, PyAny>,
            ttl_val: i64,
            fields: &Bound<'_, PyAny>,
            condition: Option<String>,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            let fl = extract_varargs_bytes(fields)?;
            let cmd = build_hash_expire_cmd(cmd_name, &kb, ttl_val, condition.as_deref(), &fl)
                .map_err(|e| e.to_py_err(py))?;
            self.push(QueuedCommand {
                args: cmd,
                keys: vec![kb],
                read_only: false,
                decoder: PipelineDecoder::IntList,
            });
            Ok(())
        }

        pub fn queue_hash_query(
            &self,
            py: Python<'_>,
            cmd_name: &str,
            key: &Bound<'_, PyAny>,
            fields: &Bound<'_, PyAny>,
            read_only: bool,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            let fl = extract_varargs_bytes(fields)?;
            let cmd =
                build_hash_field_query_cmd(cmd_name, &kb, &fl).map_err(|e| e.to_py_err(py))?;
            self.push(QueuedCommand {
                args: cmd,
                keys: vec![kb],
                read_only,
                decoder: PipelineDecoder::IntList,
            });
            Ok(())
        }

        pub fn queue_publish(&self, channel: String, message: &Bound<'_, PyAny>) -> PyResult<()> {
            let msg = extract_bytes_arg(message)?;
            self.push(QueuedCommand {
                args: vec![b"PUBLISH".to_vec(), channel.into_bytes(), msg],
                keys: Vec::new(),
                read_only: false,
                decoder: PipelineDecoder::Int,
            });
            Ok(())
        }

        pub fn queue_xadd(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            fields: &Bound<'_, PyAny>,
            id: &str,
            maxlen: Option<u64>,
            approximate: bool,
            nomkstream: bool,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            let pairs = extract_pairs_bytes(py, fields)?;
            let cmd = build_xadd(&kb, &pairs, id, maxlen, approximate, nomkstream)
                .map_err(|e| e.to_py_err(py))?;
            self.push(QueuedCommand {
                args: cmd,
                keys: vec![kb],
                read_only: false,
                decoder: PipelineDecoder::StringOrBytes,
            });
            Ok(())
        }

        pub fn queue_xack(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            group: String,
            ids: &Bound<'_, PyAny>,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            let id_list = extract_string_args(ids)?;
            let cmd = build_xack(&kb, &group, &id_list).map_err(|e| e.to_py_err(py))?;
            self.push(QueuedCommand {
                args: cmd,
                keys: vec![kb],
                read_only: false,
                decoder: PipelineDecoder::Int,
            });
            Ok(())
        }

        pub fn queue_xdel(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            ids: &Bound<'_, PyAny>,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            let id_list = extract_string_args(ids)?;
            let cmd = build_xdel(&kb, &id_list).map_err(|e| e.to_py_err(py))?;
            self.push(QueuedCommand {
                args: cmd,
                keys: vec![kb],
                read_only: false,
                decoder: PipelineDecoder::Int,
            });
            Ok(())
        }

        pub fn queue_zadd(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            members: &Bound<'_, PyAny>,
            nx: bool,
            xx: bool,
            gt: bool,
            lt: bool,
            ch: bool,
        ) -> PyResult<()> {
            let kb = extract_bytes_arg(key)?;
            let ms = extract_zadd_members(py, members)?;
            let cmd = build_zadd(&kb, &ms, nx, xx, gt, lt, ch).map_err(|e| e.to_py_err(py))?;
            self.push(QueuedCommand {
                args: cmd,
                keys: vec![kb],
                read_only: false,
                decoder: PipelineDecoder::Int,
            });
            Ok(())
        }

        pub fn queue_eval(
            &self,
            script: &Bound<'_, PyAny>,
            keys: Option<&Bound<'_, PyAny>>,
            args: Option<&Bound<'_, PyAny>>,
        ) -> PyResult<()> {
            let script_bytes = extract_bytes_arg(script)?;
            let key_list = match keys {
                Some(k) if !k.is_none() => extract_bytes_list(k)?,
                _ => Vec::new(),
            };
            let arg_list = match args {
                Some(a) if !a.is_none() => extract_bytes_list(a)?,
                _ => Vec::new(),
            };
            let cmd = build_script_exec_cmd("EVAL", &script_bytes, &key_list, &arg_list);
            self.push(QueuedCommand {
                args: cmd,
                keys: key_list,
                read_only: false,
                decoder: PipelineDecoder::GenericPy,
            });
            Ok(())
        }

        pub fn queue_evalsha(
            &self,
            sha1: String,
            keys: Option<&Bound<'_, PyAny>>,
            args: Option<&Bound<'_, PyAny>>,
        ) -> PyResult<()> {
            let key_list = match keys {
                Some(k) if !k.is_none() => extract_bytes_list(k)?,
                _ => Vec::new(),
            };
            let arg_list = match args {
                Some(a) if !a.is_none() => extract_bytes_list(a)?,
                _ => Vec::new(),
            };
            let cmd = build_script_exec_cmd("EVALSHA", sha1.as_bytes(), &key_list, &arg_list);
            self.push(QueuedCommand {
                args: cmd,
                keys: key_list,
                read_only: false,
                decoder: PipelineDecoder::GenericPy,
            });
            Ok(())
        }

        pub fn queue_execute_command(
            &self,
            py: Python<'_>,
            args: &Bound<'_, PyAny>,
        ) -> PyResult<()> {
            let items = args.extract::<Vec<Bound<'_, PyAny>>>()?;
            if items.is_empty() {
                return Err(
                    RespError::programming("execute_command requires a command name").to_py_err(py),
                );
            }
            let mut cmd = Vec::with_capacity(items.len());
            for item in items {
                cmd.push(extract_command_arg(&item)?);
            }
            let (keys, read_only) = classify_generic_command(&cmd[0], &cmd);
            self.push(QueuedCommand {
                args: cmd,
                keys,
                read_only,
                decoder: PipelineDecoder::GenericPy,
            });
            Ok(())
        }
    }
}
