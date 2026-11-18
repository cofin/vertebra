//! High-level Valkey client engine, cluster/sentinel routing, L1 tracking, Lua fallback,
//! pipelines, and PyO3 sync/async bindings (`SyncValkeyClient`, `AsyncValkeyClient`,
//! `SyncValkeyPipeline`, `AsyncValkeyPipeline`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::{Mutex as SyncMutex, RwLock as SyncRwLock};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PySequence, PyString, PyTuple};
use tokio::sync::{Mutex, Notify, RwLock};
use vertebra_db::py_async::{
    RustDbTaskAwaitable, block_on_db, spawn_rust_db_task_with_attr_and_cancel,
};

use crate::cluster::{
    ClusterNodeEndpoint, ClusterTopology, is_read_only_command, validate_cluster_keys_same_slot,
};
use crate::codec::{
    RespValue, build_hash_expire_cmd, build_hash_field_query_cmd, decode_int_list, resp_to_py,
    sha1_hex,
};
use crate::config::{
    ValkeyClientConfig, ValkeyConfigInner, ValkeyTarget, await_pending_python_token,
};
use crate::error::{ValkeyError, ValkeyErrorKind};
use crate::pubsub::{AsyncValkeyListener, SyncValkeyListener};
use crate::sentinel::{
    build_get_master_addr_cmd, parse_get_master_addr_reply, parse_sentinel_addr,
    verify_role_is_primary,
};
use crate::streams::{
    build_xack, build_xadd, build_xautoclaim, build_xdel, build_xgroup_create, build_xrange,
    build_xreadgroup, build_zadd, decode_xautoclaim_py, decode_xrange_py, decode_xreadgroup_py,
};
use crate::tracking::TrackingCache;
use crate::transport::RespConnection;
use crate::{VALKEY_COMPLETION_DRAINER_ATTR, parse_cluster_redirect, valkey_hash_slot};

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

fn valkey_slot_map_to_py_dict(
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

/// Core thread-safe async Valkey engine shared across Sync/Async clients and pipelines.
pub struct ValkeyEngine {
    pub config: Arc<ValkeyConfigInner>,
    tracking_cache: Option<Arc<TrackingCache>>,
    pools: Mutex<HashMap<String, Vec<RespConnection>>>,
    topology: SyncRwLock<ClusterTopology>,
    sentinel_primary: RwLock<Option<(String, u16)>>,
    closed: AtomicBool,
}

impl ValkeyEngine {
    /// Construct a new [`ValkeyEngine`] from immutable validated configuration.
    #[must_use]
    pub fn new(config: Arc<ValkeyConfigInner>) -> Arc<Self> {
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
            tracking_cache,
            pools: Mutex::new(HashMap::new()),
            topology: SyncRwLock::new(ClusterTopology::empty()),
            sentinel_primary: RwLock::new(None),
            closed: AtomicBool::new(false),
        })
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let mut guard = self.pools.lock().await;
        guard.clear();
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
    pub fn slot_map(&self) -> std::collections::HashMap<u16, (String, u16)> {
        self.topology.read().slot_map()
    }

    fn ensure_open(&self) -> Result<(), ValkeyError> {
        if self.is_closed() {
            Err(ValkeyError::interface("Valkey client is closed"))
        } else {
            Ok(())
        }
    }

    async fn resolve_standalone_target(&self) -> Result<ValkeyTarget, ValkeyError> {
        if self.config.sentinel_service_name.is_some() {
            if let Some((host, port)) = self.sentinel_primary.read().await.clone() {
                return Ok(ValkeyTarget::Tcp { host, port });
            }
            let (host, port) = self.discover_sentinel_primary().await?;
            return Ok(ValkeyTarget::Tcp { host, port });
        }
        Ok(self.config.target.clone())
    }

    async fn discover_sentinel_primary(&self) -> Result<(String, u16), ValkeyError> {
        let Some(ref master_name) = self.config.sentinel_service_name else {
            return Err(ValkeyError::config(
                "Sentinel service name is not configured",
            ));
        };
        let mut last_err = ValkeyError::operational(
            format!("No healthy Sentinel node returned primary for '{master_name}'"),
            true,
        );
        let sentinel_cfg = Arc::new(self.sentinel_handshake_config());

        for spec in &self.config.sentinel_nodes {
            let (shost, sport) = match parse_sentinel_addr(spec, 26379) {
                Ok(addr) => addr,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };
            let starget = ValkeyTarget::Tcp {
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

            let candidate = ValkeyTarget::Tcp {
                host: mhost.clone(),
                port: mport,
            };
            let mut pconn = match RespConnection::establish(
                &candidate,
                &self.config,
                false,
                true,
                Arc::clone_opt(&self.tracking_cache),
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    last_err = e;
                    continue;
                }
            };
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

            {
                let mut guard = self.sentinel_primary.write().await;
                *guard = Some((mhost.clone(), mport));
            }
            self.return_connection(PoolRoute::Standalone, pconn).await;
            return Ok((mhost, mport));
        }

        Err(last_err)
    }

    fn sentinel_handshake_config(&self) -> ValkeyConfigInner {
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

    async fn checkout_connection(&self, route: &PoolRoute) -> Result<RespConnection, ValkeyError> {
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

        match route {
            PoolRoute::Standalone => {
                let target = self.resolve_standalone_target().await?;
                RespConnection::establish(
                    &target,
                    &self.config,
                    false,
                    true,
                    self.tracking_cache.clone(),
                )
                .await
            }
            PoolRoute::Endpoint {
                host,
                port,
                is_replica,
            } => {
                let target = ValkeyTarget::Tcp {
                    host: host.clone(),
                    port: *port,
                };
                RespConnection::establish(
                    &target,
                    &self.config,
                    *is_replica,
                    true,
                    self.tracking_cache.clone(),
                )
                .await
            }
        }
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
    pub async fn refresh_cluster_topology(&self) -> Result<ClusterTopology, ValkeyError> {
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
        let mut last_err = ValkeyError::connection("Failed to discover Valkey Cluster topology");
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
                Ok(resp) => match ClusterTopology::from_cluster_slots(&resp) {
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

    async fn ensure_cluster_topology(&self) -> Result<ClusterTopology, ValkeyError> {
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
    ) -> Result<RespValue, ValkeyError> {
        self.ensure_open()?;
        if !self.config.cluster_enabled {
            return self.execute_standalone_or_sentinel(args).await;
        }
        self.execute_cluster_single(args, keys, read_only).await
    }

    async fn execute_standalone_or_sentinel(
        &self,
        args: Vec<Vec<u8>>,
    ) -> Result<RespValue, ValkeyError> {
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
    ) -> Result<RespValue, ValkeyError> {
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
                                return Err(ValkeyError::operational(
                                    "Valkey Cluster topology has no available shard nodes",
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
                Err(conn_err) if attempt < max_redirs => {
                    let _ = self.refresh_cluster_topology().await;
                    let _ = conn_err;
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
                            Err(ValkeyError::data("Unexpected ASK batch reply count"))
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
                            return Err(ValkeyError::operational(
                                format!(
                                    "Valkey Cluster redirection limit ({max_redirs}) exceeded: {}",
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

        Err(ValkeyError::operational(
            "Valkey Cluster command exceeded maximum redirections",
            true,
        ))
    }

    async fn execute_on_specific_endpoint(
        &self,
        ep: &ClusterNodeEndpoint,
        args: Vec<Vec<u8>>,
    ) -> Result<RespValue, ValkeyError> {
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
    pub async fn get_bytes(&self, key: Vec<u8>) -> Result<Option<Bytes>, ValkeyError> {
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

    pub async fn mget_bytes(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Bytes>>, ValkeyError> {
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
                return Err(ValkeyError::data(format!(
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
    ) -> Result<bool, ValkeyError> {
        if nx && xx {
            return Err(ValkeyError::programming(
                "SET NX and XX flags are mutually exclusive",
            ));
        }
        if px.is_some() && ex.is_some() {
            return Err(ValkeyError::programming(
                "SET px and ex options are mutually exclusive",
            ));
        }
        let mut cmd = vec![b"SET".to_vec(), key.clone(), value.clone()];
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
        let key_refs = [key.as_slice()];
        let resp = self.execute_command_routed(cmd, &key_refs, false).await?;
        let success = resp.as_bool_ok().unwrap_or(false);
        if let Some(ref cache) = self.tracking_cache {
            if success {
                cache.insert(Bytes::from(key), Bytes::from(value));
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
    ) -> Result<i64, ValkeyError> {
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
            .ok_or_else(|| ValkeyError::data(format!("Expected integer reply from {op}")))
    }

    pub async fn single_key_int_arg_cmd(
        &self,
        op: &str,
        key: Vec<u8>,
        arg: Option<i64>,
        read_only: bool,
    ) -> Result<i64, ValkeyError> {
        let mut cmd = vec![op.as_bytes().to_vec(), key.clone()];
        if let Some(n) = arg {
            cmd.push(n.to_string().into_bytes());
        }
        let key_refs = [key.as_slice()];
        let resp = self
            .execute_command_routed(cmd, &key_refs, read_only)
            .await?;
        resp.as_i64()
            .ok_or_else(|| ValkeyError::data(format!("Expected integer reply from {op}")))
    }

    pub async fn ping(&self) -> Result<bool, ValkeyError> {
        let cmd = vec![b"PING".to_vec()];
        let resp = self.execute_command_routed(cmd, &[], true).await?;
        match resp.unattributed() {
            RespValue::SimpleString(b) | RespValue::BulkString(b) => {
                Ok(b.eq_ignore_ascii_case(b"PONG") || b.eq_ignore_ascii_case(b"OK"))
            }
            _ => Ok(true),
        }
    }

    pub async fn flushdb(&self, asynchronous: bool) -> Result<bool, ValkeyError> {
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

    pub async fn scan(
        &self,
        cursor: u64,
        match_pattern: Option<String>,
        count: Option<u64>,
    ) -> Result<(u64, Vec<Bytes>), ValkeyError> {
        let cmd = build_scan_cmd(cursor, match_pattern.as_deref(), count);
        let resp = self.execute_command_routed(cmd, &[], false).await?;
        parse_scan_reply(resp)
    }

    pub async fn scan_unlink_prefix(
        &self,
        prefix: Vec<u8>,
        count: u64,
    ) -> Result<i64, ValkeyError> {
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
                let slot = valkey_hash_slot(&key);
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

    pub async fn script_load(&self, script: Vec<u8>) -> Result<String, ValkeyError> {
        let cmd = vec![b"SCRIPT".to_vec(), b"LOAD".to_vec(), script];
        if self.config.cluster_enabled {
            let topo = self.ensure_cluster_topology().await?;
            let mut last_sha = String::new();
            for ep in topo.primary_nodes() {
                let resp = self.execute_on_specific_endpoint(&ep, cmd.clone()).await?;
                last_sha = resp
                    .as_str()
                    .map(std::borrow::Cow::into_owned)
                    .ok_or_else(|| ValkeyError::data("SCRIPT LOAD returned non-string SHA1"))?;
            }
            if last_sha.is_empty() {
                let resp = self.execute_command_routed(cmd, &[], false).await?;
                last_sha = resp
                    .as_str()
                    .map(std::borrow::Cow::into_owned)
                    .ok_or_else(|| ValkeyError::data("SCRIPT LOAD returned non-string SHA1"))?;
            }
            Ok(last_sha)
        } else {
            let resp = self.execute_command_routed(cmd, &[], false).await?;
            resp.as_str()
                .map(std::borrow::Cow::into_owned)
                .ok_or_else(|| ValkeyError::data("SCRIPT LOAD returned non-string SHA1"))
        }
    }

    pub async fn eval_with_fallback(
        &self,
        script: Vec<u8>,
        keys: Vec<Vec<u8>>,
        args: Vec<Vec<u8>>,
    ) -> Result<RespValue, ValkeyError> {
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
    ) -> Result<RespValue, ValkeyError> {
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

    /// Execute a pipeline batch honoring transactions (`MULTI`/`EXEC`) or Cluster per-node batching.
    pub async fn execute_pipeline_commands(
        &self,
        commands: Vec<QueuedCommand>,
        transaction: bool,
    ) -> Result<Vec<(PipelineDecoder, RespValue)>, ValkeyError> {
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
    ) -> Result<Vec<(PipelineDecoder, RespValue)>, ValkeyError> {
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
    ) -> Result<Vec<RespValue>, ValkeyError> {
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
    ) -> Result<Vec<(PipelineDecoder, RespValue)>, ValkeyError> {
        let topo = self.ensure_cluster_topology().await?;
        let first_slot = commands
            .iter()
            .find_map(|c| c.keys.first().map(|k| valkey_hash_slot(k)));
        let ep = first_slot
            .and_then(|s| topo.route_for_slot(s, false, false).map(|(e, _)| e))
            .or_else(|| topo.any_primary())
            .ok_or_else(|| ValkeyError::operational("No cluster node available", true))?;

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
    ) -> Result<Vec<(PipelineDecoder, RespValue)>, ValkeyError> {
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
                .ok_or_else(|| ValkeyError::operational("No cluster shard found", true))?;
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
                    return Err(ValkeyError::data("Cluster pipeline reply count mismatch"));
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
            out.push(slot_res.ok_or_else(|| ValkeyError::data("Missing cluster pipeline reply"))?);
        }
        Ok(out)
    }
}

trait ArcOptionExt<T> {
    fn clone_opt(opt: &Option<Arc<T>>) -> Option<Arc<T>>;
}

impl<T> ArcOptionExt<T> for Arc<T> {
    fn clone_opt(opt: &Option<Arc<T>>) -> Option<Arc<T>> {
        opt.as_ref().map(Arc::clone)
    }
}

fn should_failover(err: &ValkeyError) -> bool {
    is_connection_broken(err)
        || matches!(
            err.code.as_deref(),
            Some("READONLY" | "MASTERDOWN" | "CLUSTERDOWN" | "LOADING")
        )
}

fn is_connection_broken(err: &ValkeyError) -> bool {
    err.kind == ValkeyErrorKind::ConnectionFailure
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

fn parse_scan_reply(resp: RespValue) -> Result<(u64, Vec<Bytes>), ValkeyError> {
    let val = resp.into_result()?;
    let Some(parts) = val.as_slice() else {
        return Err(ValkeyError::data("Unexpected non-array SCAN response"));
    };
    if parts.len() < 2 {
        return Err(ValkeyError::data("SCAN response truncated"));
    }
    let next_cursor = parts[0]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| parts[0].as_i64().and_then(|n| u64::try_from(n).ok()))
        .ok_or_else(|| ValkeyError::data("Invalid cursor in SCAN reply"))?;
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

fn build_script_exec_cmd(
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

fn extract_pipeline_results(
    commands: Vec<QueuedCommand>,
    raw_replies: Vec<RespValue>,
    transaction: bool,
) -> Result<Vec<(PipelineDecoder, RespValue)>, ValkeyError> {
    if !transaction {
        if raw_replies.len() != commands.len() {
            return Err(ValkeyError::data(format!(
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
        return Err(ValkeyError::data(
            "Transactional MULTI/EXEC reply truncated",
        ));
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
        return Err(ValkeyError::operational(
            "Transactional EXEC aborted (WATCH conflict or EXECABORT)",
            true,
        ));
    }
    let RespValue::Array(exec_items) = exec_reply else {
        return Err(ValkeyError::data("EXEC returned non-array reply"));
    };
    if exec_items.len() != commands.len() {
        return Err(ValkeyError::data(format!(
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

fn decode_hgetall_py(py: Python<'_>, resp: RespValue) -> PyResult<Py<PyAny>> {
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
                ValkeyError::data(format!("Unexpected HGETALL response: {other:?}")).to_py_err(py),
            );
        }
    }
    Ok(dict.into_any().unbind())
}

fn decode_pipeline_py(
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
                    ValkeyError::data("Expected integer reply inside pipeline").to_py_err(py)
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

fn exec_sync_valkey<F, T>(py: Python<'_>, engine: &Arc<ValkeyEngine>, fut: F) -> PyResult<T>
where
    F: std::future::Future<Output = Result<T, ValkeyError>> + Send,
    T: Send,
{
    engine
        .config
        .ensure_python_token_sync(py, false)
        .map_err(|e| e.to_py_err(py))?;
    py.detach(|| block_on_db(fut)).map_err(|e| e.to_py_err(py))
}

fn spawn_valkey_task<'py, F, T, MapOk>(
    py: Python<'py>,
    fut: F,
    map_ok: MapOk,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>>
where
    F: std::future::Future<Output = Result<T, ValkeyError>> + Send + 'static,
    T: Send + 'static,
    MapOk: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
{
    let cancel_notify = Arc::new(Notify::new());
    let cancel_wait = Arc::clone(&cancel_notify);
    let cancel_trigger = Arc::clone(&cancel_notify);
    spawn_rust_db_task_with_attr_and_cancel(
        py,
        async move {
            tokio::select! {
                res = fut => res,
                () = cancel_wait.notified() => Err(ValkeyError::cancelled("Valkey async operation cancelled")),
            }
        },
        map_ok,
        |py, err| err.to_py_err(py),
        move || cancel_trigger.notify_one(),
        VALKEY_COMPLETION_DRAINER_ATTR,
    )
}

fn spawn_valkey_cmd_task<'py, F, T, MapOk>(
    py: Python<'py>,
    config: &ValkeyConfigInner,
    fut: F,
    map_ok: MapOk,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>>
where
    F: std::future::Future<Output = Result<T, ValkeyError>> + Send + 'static,
    T: Send + 'static,
    MapOk: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
{
    let pending_tok = config
        .prepare_python_token_async(py, false)
        .map_err(|e| e.to_py_err(py))?;
    spawn_valkey_task(
        py,
        async move {
            if let Some(rx) = pending_tok {
                await_pending_python_token(rx).await?;
            }
            fut.await
        },
        map_ok,
    )
}

fn resolve_engine_config(
    py: Python<'_>,
    config: Option<&Bound<'_, PyAny>>,
) -> PyResult<Arc<ValkeyConfigInner>> {
    match config {
        None => ValkeyConfigInner::from_url_str("valkey://127.0.0.1:6379")
            .map(Arc::new)
            .map_err(|e| e.to_py_err(py)),
        Some(obj) if obj.is_none() => ValkeyConfigInner::from_url_str("valkey://127.0.0.1:6379")
            .map(Arc::new)
            .map_err(|e| e.to_py_err(py)),
        Some(obj) => {
            if let Ok(cfg) = obj.extract::<ValkeyClientConfig>() {
                Ok(cfg.inner)
            } else if let Ok(url) = obj.extract::<String>() {
                ValkeyConfigInner::from_url_str(&url)
                    .map(Arc::new)
                    .map_err(|e| e.to_py_err(py))
            } else {
                Err(PyTypeError::new_err(
                    "Expected ValkeyClientConfig or Valkey connection URL str",
                ))
            }
        }
    }
}

fn extract_bytes_arg(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if obj.is_instance_of::<PyString>() {
        return Ok(obj.cast::<PyString>()?.to_str()?.as_bytes().to_vec());
    }
    if obj.is_instance_of::<PyBytes>() {
        return Ok(obj.cast::<PyBytes>()?.as_bytes().to_vec());
    }
    obj.extract::<Vec<u8>>()
}

fn extract_command_arg(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
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

fn extract_bytes_list(seq: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
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

fn extract_varargs_bytes(args: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
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

fn extract_pairs_bytes(
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
        return Err(ValkeyError::programming("Field mapping cannot be empty").to_py_err(py));
    }
    Ok(out)
}

fn extract_hset_pairs(
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

fn extract_streams_arg(streams: &Bound<'_, PyAny>) -> PyResult<Vec<(Vec<u8>, String)>> {
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

fn extract_zadd_members(
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
            ValkeyError::programming("ZADD requires at least one score/member pair").to_py_err(py),
        );
    }
    Ok(out)
}

fn build_hset_cmd(key: &[u8], pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    let mut cmd = Vec::with_capacity(2 + pairs.len() * 2);
    cmd.push(b"HSET".to_vec());
    cmd.push(key.to_vec());
    for (f, v) in pairs {
        cmd.push(f.clone());
        cmd.push(v.clone());
    }
    cmd
}

fn build_set_cmd(
    key: &[u8],
    value: &[u8],
    px: Option<i64>,
    ex: Option<i64>,
    nx: bool,
    xx: bool,
) -> Result<Vec<Vec<u8>>, ValkeyError> {
    if nx && xx {
        return Err(ValkeyError::programming(
            "SET NX and XX flags are mutually exclusive",
        ));
    }
    if px.is_some() && ex.is_some() {
        return Err(ValkeyError::programming(
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

fn classify_generic_command(cmd_name: &[u8], all_args: &[Vec<u8>]) -> (Vec<Vec<u8>>, bool) {
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
            | "SPUBLISH"
    );
    let keys = if keyless || all_args.len() < 2 {
        Vec::new()
    } else {
        vec![all_args[1].clone()]
    };
    (keys, is_read_only_command(cmd_name))
}

fn parse_optional_keys(
    _py: Python<'_>,
    keys: Option<Bound<'_, PyAny>>,
) -> PyResult<Option<Vec<Vec<u8>>>> {
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
                    return Err(pyo3::exceptions::PyTypeError::new_err(
                        "keys must contain str or bytes",
                    ));
                }
            }
            Ok(Some(out))
        }
    }
}

/// Synchronous native Valkey client releasing the Python GIL across blocking network calls.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(frozen, immutable_type, freelist = 128, module = "vertebra._valkey")
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(frozen, immutable_type, module = "vertebra._valkey")
)]
pub struct SyncValkeyClient {
    engine: Arc<ValkeyEngine>,
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl SyncValkeyClient {
    #[new]
    #[pyo3(signature = (config = None))]
    pub fn new(py: Python<'_>, config: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let cfg = resolve_engine_config(py, config)?;
        Ok(Self {
            engine: ValkeyEngine::new(cfg),
        })
    }

    #[staticmethod]
    pub fn from_url(py: Python<'_>, url: String) -> PyResult<Self> {
        let cfg = ValkeyConfigInner::from_url_str(&url)
            .map(Arc::new)
            .map_err(|e| e.to_py_err(py))?;
        Ok(Self {
            engine: ValkeyEngine::new(cfg),
        })
    }

    #[getter]
    pub fn config(&self) -> ValkeyClientConfig {
        ValkeyClientConfig {
            inner: Arc::clone(&self.engine.config),
        }
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.engine.is_closed()
    }

    #[getter]
    pub fn resp3_enabled(&self) -> bool {
        self.engine.config.protocol_resp3
    }

    #[getter]
    pub fn tracking_enabled(&self) -> bool {
        self.engine.config.client_tracking_enabled
    }

    #[getter]
    pub fn cluster_enabled(&self) -> bool {
        self.engine.config.cluster_enabled
    }

    pub fn tracking_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (hits, misses, invalidations, entries) = self.engine.tracking_stats();
        let dict = PyDict::new(py);
        dict.set_item("hits", hits)?;
        dict.set_item("misses", misses)?;
        dict.set_item("invalidations", invalidations)?;
        dict.set_item("entries", entries)?;
        Ok(dict.into_any().unbind())
    }

    pub fn cached_keys(&self) -> Vec<String> {
        self.engine.cached_keys()
    }

    pub fn drain_invalidations(&self) -> Vec<String> {
        self.engine.drain_invalidations()
    }

    #[pyo3(signature = (keys = None))]
    pub fn invalidate_keys(
        &self,
        py: Python<'_>,
        keys: Option<Bound<'_, PyAny>>,
    ) -> PyResult<usize> {
        let parsed = parse_optional_keys(py, keys)?;
        Ok(self.engine.invalidate_keys(parsed.as_deref()))
    }

    pub fn slot_map(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        valkey_slot_map_to_py_dict(py, self.engine.slot_map())
    }

    pub fn close(&self, py: Python<'_>) {
        let engine = Arc::clone(&self.engine);
        py.detach(|| block_on_db(async move { engine.close().await }));
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type = None, _exc_val = None, _exc_tb = None))]
    pub fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        self.close(py);
        false
    }

    pub fn ping(&self, py: Python<'_>) -> PyResult<bool> {
        let engine = Arc::clone(&self.engine);
        exec_sync_valkey(py, &self.engine, async move { engine.ping().await })
    }

    pub fn get(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        let res = exec_sync_valkey(py, &self.engine, async move {
            engine.get_bytes(key_bytes).await
        })?;
        match res {
            Some(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
            None => Ok(py.None()),
        }
    }

    pub fn mget(&self, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let key_list = extract_bytes_list(keys)?;
        let engine = Arc::clone(&self.engine);
        let res = exec_sync_valkey(py, &self.engine, async move {
            engine.mget_bytes(key_list).await
        })?;
        let out = PyList::empty(py);
        for item in res {
            match item {
                Some(b) => out.append(PyBytes::new(py, &b))?,
                None => out.append(py.None())?,
            }
        }
        Ok(out.into_any().unbind())
    }

    #[pyo3(signature = (key, value, *, px = None, ex = None, nx = false, xx = false))]
    pub fn set(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        px: Option<i64>,
        ex: Option<i64>,
        nx: bool,
        xx: bool,
    ) -> PyResult<bool> {
        let key_bytes = extract_bytes_arg(key)?;
        let val_bytes = extract_bytes_arg(value)?;
        let engine = Arc::clone(&self.engine);
        exec_sync_valkey(py, &self.engine, async move {
            engine.set_bytes(key_bytes, val_bytes, px, ex, nx, xx).await
        })
    }

    #[pyo3(signature = (*keys))]
    pub fn delete(&self, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<i64> {
        let key_list = extract_varargs_bytes(keys)?;
        let engine = Arc::clone(&self.engine);
        exec_sync_valkey(py, &self.engine, async move {
            engine.multi_key_int_cmd("DEL", key_list, true).await
        })
    }

    #[pyo3(signature = (*keys))]
    pub fn unlink(&self, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<i64> {
        let key_list = extract_varargs_bytes(keys)?;
        let engine = Arc::clone(&self.engine);
        exec_sync_valkey(py, &self.engine, async move {
            engine.multi_key_int_cmd("UNLINK", key_list, true).await
        })
    }

    #[pyo3(signature = (*keys))]
    pub fn exists(&self, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<i64> {
        let key_list = extract_varargs_bytes(keys)?;
        let engine = Arc::clone(&self.engine);
        exec_sync_valkey(py, &self.engine, async move {
            engine.multi_key_int_cmd("EXISTS", key_list, false).await
        })
    }

    pub fn expire(&self, py: Python<'_>, key: &Bound<'_, PyAny>, seconds: i64) -> PyResult<bool> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        let n = exec_sync_valkey(py, &self.engine, async move {
            engine
                .single_key_int_arg_cmd("EXPIRE", key_bytes, Some(seconds), false)
                .await
        })?;
        Ok(n > 0)
    }

    pub fn pexpire(&self, py: Python<'_>, key: &Bound<'_, PyAny>, ms: i64) -> PyResult<bool> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        let n = exec_sync_valkey(py, &self.engine, async move {
            engine
                .single_key_int_arg_cmd("PEXPIRE", key_bytes, Some(ms), false)
                .await
        })?;
        Ok(n > 0)
    }

    pub fn ttl(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<i64> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        exec_sync_valkey(py, &self.engine, async move {
            engine
                .single_key_int_arg_cmd("TTL", key_bytes, None, true)
                .await
        })
    }

    pub fn pttl(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<i64> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        exec_sync_valkey(py, &self.engine, async move {
            engine
                .single_key_int_arg_cmd("PTTL", key_bytes, None, true)
                .await
        })
    }

    #[pyo3(signature = (key, amount = 1))]
    pub fn incr(&self, py: Python<'_>, key: &Bound<'_, PyAny>, amount: i64) -> PyResult<i64> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        let cmd = vec![
            b"INCRBY".to_vec(),
            key_bytes.clone(),
            amount.to_string().into_bytes(),
        ];
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        resp.as_i64()
            .ok_or_else(|| ValkeyError::data("Expected integer from INCRBY").to_py_err(py))
    }

    #[pyo3(signature = (key, amount = 1))]
    pub fn decr(&self, py: Python<'_>, key: &Bound<'_, PyAny>, amount: i64) -> PyResult<i64> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        let cmd = vec![
            b"DECRBY".to_vec(),
            key_bytes.clone(),
            amount.to_string().into_bytes(),
        ];
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        resp.as_i64()
            .ok_or_else(|| ValkeyError::data("Expected integer from DECRBY").to_py_err(py))
    }

    #[pyo3(signature = (cursor = 0, *, match_pattern = None, count = None))]
    pub fn scan(
        &self,
        py: Python<'_>,
        cursor: u64,
        match_pattern: Option<String>,
        count: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let engine = Arc::clone(&self.engine);
        let (next_cur, keys) = exec_sync_valkey(py, &self.engine, async move {
            engine.scan(cursor, match_pattern, count).await
        })?;
        let py_keys = PyList::empty(py);
        for k in keys {
            py_keys.append(PyBytes::new(py, &k))?;
        }
        Ok(PyTuple::new(
            py,
            [
                next_cur.into_pyobject(py)?.into_any().unbind(),
                py_keys.into_any().unbind(),
            ],
        )?
        .into_any()
        .unbind())
    }

    #[pyo3(signature = (prefix, *, count = 200))]
    pub fn scan_unlink_prefix(
        &self,
        py: Python<'_>,
        prefix: &Bound<'_, PyAny>,
        count: u64,
    ) -> PyResult<i64> {
        let prefix_bytes = extract_bytes_arg(prefix)?;
        let engine = Arc::clone(&self.engine);
        exec_sync_valkey(py, &self.engine, async move {
            engine.scan_unlink_prefix(prefix_bytes, count).await
        })
    }

    #[pyo3(signature = (*, asynchronous = false))]
    pub fn flushdb(&self, py: Python<'_>, asynchronous: bool) -> PyResult<bool> {
        let engine = Arc::clone(&self.engine);
        exec_sync_valkey(py, &self.engine, async move {
            engine.flushdb(asynchronous).await
        })
    }

    pub fn publish(
        &self,
        py: Python<'_>,
        channel: String,
        message: &Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let msg_bytes = extract_bytes_arg(message)?;
        let engine = Arc::clone(&self.engine);
        let cmd = vec![b"PUBLISH".to_vec(), channel.into_bytes(), msg_bytes];
        let resp = exec_sync_valkey(py, &self.engine, async move {
            engine.execute_command_routed(cmd, &[], false).await
        })?;
        resp.as_i64()
            .ok_or_else(|| ValkeyError::data("Expected integer from PUBLISH").to_py_err(py))
    }

    #[pyo3(signature = (key, mapping, value = None))]
    pub fn hset(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        mapping: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        let key_bytes = extract_bytes_arg(key)?;
        let pairs = extract_hset_pairs(py, mapping, value)?;
        let cmd = build_hset_cmd(&key_bytes, &pairs);
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        resp.as_i64()
            .ok_or_else(|| ValkeyError::data("Expected integer from HSET").to_py_err(py))
    }

    pub fn hget(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        field: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let key_bytes = extract_bytes_arg(key)?;
        let field_bytes = extract_bytes_arg(field)?;
        let cmd = vec![b"HGET".to_vec(), key_bytes.clone(), field_bytes];
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, true).await
        })?;
        match resp.into_result().map_err(|e| e.to_py_err(py))? {
            RespValue::Null => Ok(py.None()),
            val => match val.as_bytes() {
                Some(b) => Ok(PyBytes::new(py, b).into_any().unbind()),
                None => Ok(py.None()),
            },
        }
    }

    pub fn hgetall(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let key_bytes = extract_bytes_arg(key)?;
        let cmd = vec![b"HGETALL".to_vec(), key_bytes.clone()];
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, true).await
        })?;
        decode_hgetall_py(py, resp)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hdel(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let key_bytes = extract_bytes_arg(key)?;
        let field_list = extract_varargs_bytes(fields)?;
        if field_list.is_empty() {
            return Err(ValkeyError::programming("HDEL requires at least one field").to_py_err(py));
        }
        let mut cmd = Vec::with_capacity(2 + field_list.len());
        cmd.push(b"HDEL".to_vec());
        cmd.push(key_bytes.clone());
        cmd.extend(field_list);
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        resp.as_i64()
            .ok_or_else(|| ValkeyError::data("Expected integer from HDEL").to_py_err(py))
    }

    #[pyo3(signature = (key, ttl_seconds, *fields, condition = None))]
    pub fn hexpire(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ttl_seconds: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Vec<i64>> {
        self.exec_hash_expire_sync(py, "HEXPIRE", key, ttl_seconds, fields, condition)
    }

    #[pyo3(signature = (key, ttl_ms, *fields, condition = None))]
    pub fn hpexpire(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ttl_ms: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Vec<i64>> {
        self.exec_hash_expire_sync(py, "HPEXPIRE", key, ttl_ms, fields, condition)
    }

    #[pyo3(signature = (key, unix_time_seconds, *fields, condition = None))]
    pub fn hexpireat(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        unix_time_seconds: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Vec<i64>> {
        self.exec_hash_expire_sync(py, "HEXPIREAT", key, unix_time_seconds, fields, condition)
    }

    #[pyo3(signature = (key, unix_time_ms, *fields, condition = None))]
    pub fn hpexpireat(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        unix_time_ms: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Vec<i64>> {
        self.exec_hash_expire_sync(py, "HPEXPIREAT", key, unix_time_ms, fields, condition)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn httl(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<i64>> {
        self.exec_hash_query_sync(py, "HTTL", key, fields, true)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hpttl(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<i64>> {
        self.exec_hash_query_sync(py, "HPTTL", key, fields, true)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hpersist(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<i64>> {
        self.exec_hash_query_sync(py, "HPERSIST", key, fields, false)
    }

    pub fn script_load(&self, py: Python<'_>, script: &Bound<'_, PyAny>) -> PyResult<String> {
        let script_bytes = extract_bytes_arg(script)?;
        let engine = Arc::clone(&self.engine);
        exec_sync_valkey(py, &self.engine, async move {
            engine.script_load(script_bytes).await
        })
    }

    #[pyo3(signature = (script, keys = None, args = None))]
    pub fn eval(
        &self,
        py: Python<'_>,
        script: &Bound<'_, PyAny>,
        keys: Option<&Bound<'_, PyAny>>,
        args: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let script_bytes = extract_bytes_arg(script)?;
        let key_list = match keys {
            Some(k) if !k.is_none() => extract_bytes_list(k)?,
            _ => Vec::new(),
        };
        let arg_list = match args {
            Some(a) if !a.is_none() => extract_bytes_list(a)?,
            _ => Vec::new(),
        };
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            engine
                .eval_with_fallback(script_bytes, key_list, arg_list)
                .await
        })?;
        resp_to_py(py, resp)
    }

    #[pyo3(signature = (sha1, keys = None, args = None, *, script = None))]
    pub fn evalsha(
        &self,
        py: Python<'_>,
        sha1: String,
        keys: Option<&Bound<'_, PyAny>>,
        args: Option<&Bound<'_, PyAny>>,
        script: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let key_list = match keys {
            Some(k) if !k.is_none() => extract_bytes_list(k)?,
            _ => Vec::new(),
        };
        let arg_list = match args {
            Some(a) if !a.is_none() => extract_bytes_list(a)?,
            _ => Vec::new(),
        };
        let script_bytes = match script {
            Some(s) if !s.is_none() => Some(extract_bytes_arg(s)?),
            _ => None,
        };
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            engine
                .evalsha_with_optional_fallback(sha1, key_list, arg_list, script_bytes)
                .await
        })?;
        resp_to_py(py, resp)
    }

    #[pyo3(signature = (*args))]
    pub fn execute_command(&self, py: Python<'_>, args: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let items = args.extract::<Vec<Bound<'_, PyAny>>>()?;
        if items.is_empty() {
            return Err(
                ValkeyError::programming("execute_command requires a command name").to_py_err(py),
            );
        }
        let mut cmd = Vec::with_capacity(items.len());
        for item in items {
            cmd.push(extract_command_arg(&item)?);
        }
        let (keys, read_only) = classify_generic_command(&cmd[0], &cmd);
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
            engine
                .execute_command_routed(cmd, &key_refs, read_only)
                .await
        })?;
        resp_to_py(py, resp)
    }

    #[pyo3(signature = (key, group, id = "0", *, mkstream = false))]
    pub fn xgroup_create(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        group: String,
        id: &str,
        mkstream: bool,
    ) -> PyResult<bool> {
        let key_bytes = extract_bytes_arg(key)?;
        let cmd = build_xgroup_create(&key_bytes, &group, id, mkstream);
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        Ok(resp.as_bool_ok().unwrap_or(true))
    }

    #[pyo3(signature = (key, fields, *, id = "*", maxlen = None, approximate = true, nomkstream = false))]
    pub fn xadd(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
        id: &str,
        maxlen: Option<u64>,
        approximate: bool,
        nomkstream: bool,
    ) -> PyResult<String> {
        let key_bytes = extract_bytes_arg(key)?;
        let pairs = extract_pairs_bytes(py, fields)?;
        let cmd = build_xadd(&key_bytes, &pairs, id, maxlen, approximate, nomkstream)
            .map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        resp.as_str()
            .map(std::borrow::Cow::into_owned)
            .ok_or_else(|| ValkeyError::data("XADD returned non-string entry id").to_py_err(py))
    }

    #[pyo3(signature = (group, consumer, streams, *, count = None, block_ms = None, noack = false))]
    pub fn xreadgroup(
        &self,
        py: Python<'_>,
        group: String,
        consumer: String,
        streams: &Bound<'_, PyAny>,
        count: Option<u64>,
        block_ms: Option<u64>,
        noack: bool,
    ) -> PyResult<Py<PyAny>> {
        let stream_pairs = extract_streams_arg(streams)?;
        let cmd = build_xreadgroup(&group, &consumer, &stream_pairs, count, block_ms, noack)
            .map_err(|e| e.to_py_err(py))?;
        let keys: Vec<Vec<u8>> = stream_pairs.into_iter().map(|(k, _)| k).collect();
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        decode_xreadgroup_py(py, resp)
    }

    #[pyo3(signature = (key, group, *ids))]
    pub fn xack(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        group: String,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let key_bytes = extract_bytes_arg(key)?;
        let id_list = crate::pubsub::extract_string_args(ids)?;
        let cmd = build_xack(&key_bytes, &group, &id_list).map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        resp.as_i64()
            .ok_or_else(|| ValkeyError::data("Expected integer from XACK").to_py_err(py))
    }

    #[pyo3(signature = (key, group, consumer, min_idle_ms, start_id = "0-0", *, count = None, justid = false))]
    pub fn xautoclaim(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        group: String,
        consumer: String,
        min_idle_ms: u64,
        start_id: &str,
        count: Option<u64>,
        justid: bool,
    ) -> PyResult<Py<PyAny>> {
        let key_bytes = extract_bytes_arg(key)?;
        let cmd = build_xautoclaim(
            &key_bytes,
            &group,
            &consumer,
            min_idle_ms,
            start_id,
            count,
            justid,
        );
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        decode_xautoclaim_py(py, resp)
    }

    #[pyo3(signature = (key, min = "-", max = "+", *, count = None))]
    pub fn xrange(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        min: &str,
        max: &str,
        count: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let key_bytes = extract_bytes_arg(key)?;
        let cmd = build_xrange(&key_bytes, min, max, count);
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, true).await
        })?;
        decode_xrange_py(py, resp)
    }

    #[pyo3(signature = (key, *ids))]
    pub fn xdel(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let key_bytes = extract_bytes_arg(key)?;
        let id_list = crate::pubsub::extract_string_args(ids)?;
        let cmd = build_xdel(&key_bytes, &id_list).map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        resp.as_i64()
            .ok_or_else(|| ValkeyError::data("Expected integer from XDEL").to_py_err(py))
    }

    #[pyo3(signature = (key, members, *, nx = false, xx = false, gt = false, lt = false, ch = false))]
    pub fn zadd(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        members: &Bound<'_, PyAny>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
    ) -> PyResult<i64> {
        let key_bytes = extract_bytes_arg(key)?;
        let member_list = extract_zadd_members(py, members)?;
        let cmd = build_zadd(&key_bytes, &member_list, nx, xx, gt, lt, ch)
            .map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        resp.as_i64()
            .ok_or_else(|| ValkeyError::data("Expected integer from ZADD").to_py_err(py))
    }

    #[pyo3(signature = (*, transaction = false))]
    pub fn pipeline(&self, transaction: bool) -> SyncValkeyPipeline {
        SyncValkeyPipeline::from_engine(Arc::clone(&self.engine), transaction)
    }

    pub fn listener(&self) -> SyncValkeyListener {
        SyncValkeyListener::from_config(Arc::clone(&self.engine.config))
    }
}

impl SyncValkeyClient {
    fn exec_hash_expire_sync(
        &self,
        py: Python<'_>,
        cmd_name: &str,
        key: &Bound<'_, PyAny>,
        ttl_val: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Vec<i64>> {
        let key_bytes = extract_bytes_arg(key)?;
        let field_list = extract_varargs_bytes(fields)?;
        let cmd = build_hash_expire_cmd(
            cmd_name,
            &key_bytes,
            ttl_val,
            condition.as_deref(),
            &field_list,
        )
        .map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine.execute_command_routed(cmd, &key_refs, false).await
        })?;
        decode_int_list(resp).map_err(|e| e.to_py_err(py))
    }

    fn exec_hash_query_sync(
        &self,
        py: Python<'_>,
        cmd_name: &str,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
        read_only: bool,
    ) -> PyResult<Vec<i64>> {
        let key_bytes = extract_bytes_arg(key)?;
        let field_list = extract_varargs_bytes(fields)?;
        let cmd = build_hash_field_query_cmd(cmd_name, &key_bytes, &field_list)
            .map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        let resp = exec_sync_valkey(py, &self.engine, async move {
            let key_refs = [key_bytes.as_slice()];
            engine
                .execute_command_routed(cmd, &key_refs, read_only)
                .await
        })?;
        decode_int_list(resp).map_err(|e| e.to_py_err(py))
    }
}

/// Asynchronous native Valkey client resolving Python `asyncio.Future`s via Tokio completion queue.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(frozen, immutable_type, freelist = 128, module = "vertebra._valkey")
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(frozen, immutable_type, module = "vertebra._valkey")
)]
pub struct AsyncValkeyClient {
    engine: Arc<ValkeyEngine>,
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl AsyncValkeyClient {
    #[new]
    #[pyo3(signature = (config = None))]
    pub fn new(py: Python<'_>, config: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let cfg = resolve_engine_config(py, config)?;
        Ok(Self {
            engine: ValkeyEngine::new(cfg),
        })
    }

    #[staticmethod]
    pub fn from_url(py: Python<'_>, url: String) -> PyResult<Self> {
        let cfg = ValkeyConfigInner::from_url_str(&url)
            .map(Arc::new)
            .map_err(|e| e.to_py_err(py))?;
        Ok(Self {
            engine: ValkeyEngine::new(cfg),
        })
    }

    #[getter]
    pub fn config(&self) -> ValkeyClientConfig {
        ValkeyClientConfig {
            inner: Arc::clone(&self.engine.config),
        }
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.engine.is_closed()
    }

    #[getter]
    pub fn resp3_enabled(&self) -> bool {
        self.engine.config.protocol_resp3
    }

    #[getter]
    pub fn tracking_enabled(&self) -> bool {
        self.engine.config.client_tracking_enabled
    }

    #[getter]
    pub fn cluster_enabled(&self) -> bool {
        self.engine.config.cluster_enabled
    }

    pub fn tracking_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (hits, misses, invalidations, entries) = self.engine.tracking_stats();
        let dict = PyDict::new(py);
        dict.set_item("hits", hits)?;
        dict.set_item("misses", misses)?;
        dict.set_item("invalidations", invalidations)?;
        dict.set_item("entries", entries)?;
        Ok(dict.into_any().unbind())
    }

    pub fn cached_keys(&self) -> Vec<String> {
        self.engine.cached_keys()
    }

    pub fn drain_invalidations(&self) -> Vec<String> {
        self.engine.drain_invalidations()
    }

    #[pyo3(signature = (keys = None))]
    pub fn invalidate_keys(
        &self,
        py: Python<'_>,
        keys: Option<Bound<'_, PyAny>>,
    ) -> PyResult<usize> {
        let parsed = parse_optional_keys(py, keys)?;
        Ok(self.engine.invalidate_keys(parsed.as_deref()))
    }

    pub fn slot_map(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        valkey_slot_map_to_py_dict(py, self.engine.slot_map())
    }

    pub fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let engine = Arc::clone(&self.engine);
        spawn_valkey_task(
            py,
            async move {
                engine.close().await;
                Ok(())
            },
            |py, ()| Ok(py.None()),
        )
    }

    pub fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.close(py)
    }

    pub fn __aenter__<'py>(
        slf: Py<Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        spawn_valkey_task(py, async move { Ok(slf) }, |_py, obj| Ok(obj.into_any()))
    }

    #[pyo3(signature = (_exc_type = None, _exc_val = None, _exc_tb = None))]
    pub fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let engine = Arc::clone(&self.engine);
        spawn_valkey_task(
            py,
            async move {
                engine.close().await;
                Ok(false)
            },
            |py, suppressed| Ok(PyBool::new(py, suppressed).to_owned().into_any().unbind()),
        )
    }

    pub fn ping<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.ping().await },
            |py, ok| Ok(PyBool::new(py, ok).to_owned().into_any().unbind()),
        )
    }

    pub fn get<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.get_bytes(key_bytes).await },
            |py, opt| match opt {
                Some(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
                None => Ok(py.None()),
            },
        )
    }

    pub fn mget<'py>(
        &self,
        py: Python<'py>,
        keys: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_list = extract_bytes_list(keys)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.mget_bytes(key_list).await },
            |py, items| {
                let list = PyList::empty(py);
                for item in items {
                    match item {
                        Some(b) => list.append(PyBytes::new(py, &b))?,
                        None => list.append(py.None())?,
                    }
                }
                Ok(list.into_any().unbind())
            },
        )
    }

    #[pyo3(signature = (key, value, *, px = None, ex = None, nx = false, xx = false))]
    pub fn set<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        px: Option<i64>,
        ex: Option<i64>,
        nx: bool,
        xx: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let val_bytes = extract_bytes_arg(value)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.set_bytes(key_bytes, val_bytes, px, ex, nx, xx).await },
            |py, ok| Ok(PyBool::new(py, ok).to_owned().into_any().unbind()),
        )
    }

    #[pyo3(signature = (*keys))]
    pub fn delete<'py>(
        &self,
        py: Python<'py>,
        keys: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_list = extract_varargs_bytes(keys)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.multi_key_int_cmd("DEL", key_list, true).await },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (*keys))]
    pub fn unlink<'py>(
        &self,
        py: Python<'py>,
        keys: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_list = extract_varargs_bytes(keys)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.multi_key_int_cmd("UNLINK", key_list, true).await },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (*keys))]
    pub fn exists<'py>(
        &self,
        py: Python<'py>,
        keys: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_list = extract_varargs_bytes(keys)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.multi_key_int_cmd("EXISTS", key_list, false).await },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    pub fn expire<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        seconds: i64,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                engine
                    .single_key_int_arg_cmd("EXPIRE", key_bytes, Some(seconds), false)
                    .await
            },
            |py, n| Ok(PyBool::new(py, n > 0).to_owned().into_any().unbind()),
        )
    }

    pub fn pexpire<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        ms: i64,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                engine
                    .single_key_int_arg_cmd("PEXPIRE", key_bytes, Some(ms), false)
                    .await
            },
            |py, n| Ok(PyBool::new(py, n > 0).to_owned().into_any().unbind()),
        )
    }

    pub fn ttl<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                engine
                    .single_key_int_arg_cmd("TTL", key_bytes, None, true)
                    .await
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    pub fn pttl<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                engine
                    .single_key_int_arg_cmd("PTTL", key_bytes, None, true)
                    .await
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (key, amount = 1))]
    pub fn incr<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        amount: i64,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        let cmd = vec![
            b"INCRBY".to_vec(),
            key_bytes.clone(),
            amount.to_string().into_bytes(),
        ];
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, false).await?;
                resp.as_i64()
                    .ok_or_else(|| ValkeyError::data("Expected integer from INCRBY"))
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (key, amount = 1))]
    pub fn decr<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        amount: i64,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let engine = Arc::clone(&self.engine);
        let cmd = vec![
            b"DECRBY".to_vec(),
            key_bytes.clone(),
            amount.to_string().into_bytes(),
        ];
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, false).await?;
                resp.as_i64()
                    .ok_or_else(|| ValkeyError::data("Expected integer from DECRBY"))
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (cursor = 0, *, match_pattern = None, count = None))]
    pub fn scan<'py>(
        &self,
        py: Python<'py>,
        cursor: u64,
        match_pattern: Option<String>,
        count: Option<u64>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.scan(cursor, match_pattern, count).await },
            |py, (next_cur, keys)| {
                let py_keys = PyList::empty(py);
                for k in keys {
                    py_keys.append(PyBytes::new(py, &k))?;
                }
                Ok(PyTuple::new(
                    py,
                    [
                        next_cur.into_pyobject(py)?.into_any().unbind(),
                        py_keys.into_any().unbind(),
                    ],
                )?
                .into_any()
                .unbind())
            },
        )
    }

    #[pyo3(signature = (prefix, *, count = 200))]
    pub fn scan_unlink_prefix<'py>(
        &self,
        py: Python<'py>,
        prefix: &Bound<'_, PyAny>,
        count: u64,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let prefix_bytes = extract_bytes_arg(prefix)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.scan_unlink_prefix(prefix_bytes, count).await },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (*, asynchronous = false))]
    pub fn flushdb<'py>(
        &self,
        py: Python<'py>,
        asynchronous: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.flushdb(asynchronous).await },
            |py, ok| Ok(PyBool::new(py, ok).to_owned().into_any().unbind()),
        )
    }

    pub fn publish<'py>(
        &self,
        py: Python<'py>,
        channel: String,
        message: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let msg_bytes = extract_bytes_arg(message)?;
        let engine = Arc::clone(&self.engine);
        let cmd = vec![b"PUBLISH".to_vec(), channel.into_bytes(), msg_bytes];
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let resp = engine.execute_command_routed(cmd, &[], false).await?;
                resp.as_i64()
                    .ok_or_else(|| ValkeyError::data("Expected integer from PUBLISH"))
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (key, mapping, value = None))]
    pub fn hset<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        mapping: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let pairs = extract_hset_pairs(py, mapping, value)?;
        let cmd = build_hset_cmd(&key_bytes, &pairs);
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, false).await?;
                resp.as_i64()
                    .ok_or_else(|| ValkeyError::data("Expected integer from HSET"))
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    pub fn hget<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        field: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let field_bytes = extract_bytes_arg(field)?;
        let cmd = vec![b"HGET".to_vec(), key_bytes.clone(), field_bytes];
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, true).await?;
                match resp.into_result()? {
                    RespValue::Null => Ok(None),
                    val => Ok(val.as_bytes().map(Bytes::copy_from_slice)),
                }
            },
            |py, opt| match opt {
                Some(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
                None => Ok(py.None()),
            },
        )
    }

    pub fn hgetall<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let cmd = vec![b"HGETALL".to_vec(), key_bytes.clone()];
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                engine.execute_command_routed(cmd, &key_refs, true).await
            },
            decode_hgetall_py,
        )
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hdel<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let field_list = extract_varargs_bytes(fields)?;
        if field_list.is_empty() {
            return Err(ValkeyError::programming("HDEL requires at least one field").to_py_err(py));
        }
        let mut cmd = Vec::with_capacity(2 + field_list.len());
        cmd.push(b"HDEL".to_vec());
        cmd.push(key_bytes.clone());
        cmd.extend(field_list);
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, false).await?;
                resp.as_i64()
                    .ok_or_else(|| ValkeyError::data("Expected integer from HDEL"))
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (key, ttl_seconds, *fields, condition = None))]
    pub fn hexpire<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        ttl_seconds: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.exec_hash_expire_async(py, "HEXPIRE", key, ttl_seconds, fields, condition)
    }

    #[pyo3(signature = (key, ttl_ms, *fields, condition = None))]
    pub fn hpexpire<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        ttl_ms: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.exec_hash_expire_async(py, "HPEXPIRE", key, ttl_ms, fields, condition)
    }

    #[pyo3(signature = (key, unix_time_seconds, *fields, condition = None))]
    pub fn hexpireat<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        unix_time_seconds: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.exec_hash_expire_async(py, "HEXPIREAT", key, unix_time_seconds, fields, condition)
    }

    #[pyo3(signature = (key, unix_time_ms, *fields, condition = None))]
    pub fn hpexpireat<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        unix_time_ms: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.exec_hash_expire_async(py, "HPEXPIREAT", key, unix_time_ms, fields, condition)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn httl<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.exec_hash_query_async(py, "HTTL", key, fields, true)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hpttl<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.exec_hash_query_async(py, "HPTTL", key, fields, true)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hpersist<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.exec_hash_query_async(py, "HPERSIST", key, fields, false)
    }

    pub fn script_load<'py>(
        &self,
        py: Python<'py>,
        script: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let script_bytes = extract_bytes_arg(script)?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move { engine.script_load(script_bytes).await },
            |py, sha| Ok(sha.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (script, keys = None, args = None))]
    pub fn eval<'py>(
        &self,
        py: Python<'py>,
        script: &Bound<'_, PyAny>,
        keys: Option<&Bound<'_, PyAny>>,
        args: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let script_bytes = extract_bytes_arg(script)?;
        let key_list = match keys {
            Some(k) if !k.is_none() => extract_bytes_list(k)?,
            _ => Vec::new(),
        };
        let arg_list = match args {
            Some(a) if !a.is_none() => extract_bytes_list(a)?,
            _ => Vec::new(),
        };
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                engine
                    .eval_with_fallback(script_bytes, key_list, arg_list)
                    .await
            },
            resp_to_py,
        )
    }

    #[pyo3(signature = (sha1, keys = None, args = None, *, script = None))]
    pub fn evalsha<'py>(
        &self,
        py: Python<'py>,
        sha1: String,
        keys: Option<&Bound<'_, PyAny>>,
        args: Option<&Bound<'_, PyAny>>,
        script: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_list = match keys {
            Some(k) if !k.is_none() => extract_bytes_list(k)?,
            _ => Vec::new(),
        };
        let arg_list = match args {
            Some(a) if !a.is_none() => extract_bytes_list(a)?,
            _ => Vec::new(),
        };
        let script_bytes = match script {
            Some(s) if !s.is_none() => Some(extract_bytes_arg(s)?),
            _ => None,
        };
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                engine
                    .evalsha_with_optional_fallback(sha1, key_list, arg_list, script_bytes)
                    .await
            },
            resp_to_py,
        )
    }

    #[pyo3(signature = (*args))]
    pub fn execute_command<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let items = args.extract::<Vec<Bound<'_, PyAny>>>()?;
        if items.is_empty() {
            return Err(
                ValkeyError::programming("execute_command requires a command name").to_py_err(py),
            );
        }
        let mut cmd = Vec::with_capacity(items.len());
        for item in items {
            cmd.push(extract_command_arg(&item)?);
        }
        let (keys, read_only) = classify_generic_command(&cmd[0], &cmd);
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
                engine
                    .execute_command_routed(cmd, &key_refs, read_only)
                    .await
            },
            resp_to_py,
        )
    }

    #[pyo3(signature = (key, group, id = "0", *, mkstream = false))]
    pub fn xgroup_create<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        group: String,
        id: &str,
        mkstream: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let cmd = build_xgroup_create(&key_bytes, &group, id, mkstream);
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, false).await?;
                Ok(resp.as_bool_ok().unwrap_or(true))
            },
            |py, ok| Ok(PyBool::new(py, ok).to_owned().into_any().unbind()),
        )
    }

    #[pyo3(signature = (key, fields, *, id = "*", maxlen = None, approximate = true, nomkstream = false))]
    pub fn xadd<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
        id: &str,
        maxlen: Option<u64>,
        approximate: bool,
        nomkstream: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let pairs = extract_pairs_bytes(py, fields)?;
        let cmd = build_xadd(&key_bytes, &pairs, id, maxlen, approximate, nomkstream)
            .map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, false).await?;
                resp.as_str()
                    .map(std::borrow::Cow::into_owned)
                    .ok_or_else(|| ValkeyError::data("XADD returned non-string entry id"))
            },
            |py, s| Ok(s.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (group, consumer, streams, *, count = None, block_ms = None, noack = false))]
    pub fn xreadgroup<'py>(
        &self,
        py: Python<'py>,
        group: String,
        consumer: String,
        streams: &Bound<'_, PyAny>,
        count: Option<u64>,
        block_ms: Option<u64>,
        noack: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let stream_pairs = extract_streams_arg(streams)?;
        let cmd = build_xreadgroup(&group, &consumer, &stream_pairs, count, block_ms, noack)
            .map_err(|e| e.to_py_err(py))?;
        let keys: Vec<Vec<u8>> = stream_pairs.into_iter().map(|(k, _)| k).collect();
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
                engine.execute_command_routed(cmd, &key_refs, false).await
            },
            decode_xreadgroup_py,
        )
    }

    #[pyo3(signature = (key, group, *ids))]
    pub fn xack<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        group: String,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let id_list = crate::pubsub::extract_string_args(ids)?;
        let cmd = build_xack(&key_bytes, &group, &id_list).map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, false).await?;
                resp.as_i64()
                    .ok_or_else(|| ValkeyError::data("Expected integer from XACK"))
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (key, group, consumer, min_idle_ms, start_id = "0-0", *, count = None, justid = false))]
    pub fn xautoclaim<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        group: String,
        consumer: String,
        min_idle_ms: u64,
        start_id: &str,
        count: Option<u64>,
        justid: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let cmd = build_xautoclaim(
            &key_bytes,
            &group,
            &consumer,
            min_idle_ms,
            start_id,
            count,
            justid,
        );
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                engine.execute_command_routed(cmd, &key_refs, false).await
            },
            decode_xautoclaim_py,
        )
    }

    #[pyo3(signature = (key, min = "-", max = "+", *, count = None))]
    pub fn xrange<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        min: &str,
        max: &str,
        count: Option<u64>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let cmd = build_xrange(&key_bytes, min, max, count);
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                engine.execute_command_routed(cmd, &key_refs, true).await
            },
            decode_xrange_py,
        )
    }

    #[pyo3(signature = (key, *ids))]
    pub fn xdel<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let id_list = crate::pubsub::extract_string_args(ids)?;
        let cmd = build_xdel(&key_bytes, &id_list).map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, false).await?;
                resp.as_i64()
                    .ok_or_else(|| ValkeyError::data("Expected integer from XDEL"))
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (key, members, *, nx = false, xx = false, gt = false, lt = false, ch = false))]
    pub fn zadd<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        members: &Bound<'_, PyAny>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let member_list = extract_zadd_members(py, members)?;
        let cmd = build_zadd(&key_bytes, &member_list, nx, xx, gt, lt, ch)
            .map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, false).await?;
                resp.as_i64()
                    .ok_or_else(|| ValkeyError::data("Expected integer from ZADD"))
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (*, transaction = false))]
    pub fn pipeline(&self, transaction: bool) -> AsyncValkeyPipeline {
        AsyncValkeyPipeline::from_engine(Arc::clone(&self.engine), transaction)
    }

    pub fn listener(&self) -> AsyncValkeyListener {
        AsyncValkeyListener::from_config(Arc::clone(&self.engine.config))
    }
}

impl AsyncValkeyClient {
    fn exec_hash_expire_async<'py>(
        &self,
        py: Python<'py>,
        cmd_name: &'static str,
        key: &Bound<'_, PyAny>,
        ttl_val: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let field_list = extract_varargs_bytes(fields)?;
        let cmd = build_hash_expire_cmd(
            cmd_name,
            &key_bytes,
            ttl_val,
            condition.as_deref(),
            &field_list,
        )
        .map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine.execute_command_routed(cmd, &key_refs, false).await?;
                decode_int_list(resp)
            },
            |py, ints| Ok(ints.into_pyobject(py)?.into_any().unbind()),
        )
    }

    fn exec_hash_query_async<'py>(
        &self,
        py: Python<'py>,
        cmd_name: &'static str,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
        read_only: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let key_bytes = extract_bytes_arg(key)?;
        let field_list = extract_varargs_bytes(fields)?;
        let cmd = build_hash_field_query_cmd(cmd_name, &key_bytes, &field_list)
            .map_err(|e| e.to_py_err(py))?;
        let engine = Arc::clone(&self.engine);
        spawn_valkey_cmd_task(
            py,
            &self.engine.config,
            async move {
                let key_refs = [key_bytes.as_slice()];
                let resp = engine
                    .execute_command_routed(cmd, &key_refs, read_only)
                    .await?;
                decode_int_list(resp)
            },
            |py, ints| Ok(ints.into_pyobject(py)?.into_any().unbind()),
        )
    }
}

struct PipelineBuilder {
    engine: Arc<ValkeyEngine>,
    transaction: bool,
    queue: SyncMutex<Vec<QueuedCommand>>,
}

#[allow(clippy::too_many_arguments)]
impl PipelineBuilder {
    fn new(engine: Arc<ValkeyEngine>, transaction: bool) -> Self {
        Self {
            engine,
            transaction,
            queue: SyncMutex::new(Vec::new()),
        }
    }

    fn push(&self, cmd: QueuedCommand) {
        self.queue.lock().push(cmd);
    }

    fn len(&self) -> usize {
        self.queue.lock().len()
    }

    fn clear(&self) {
        self.queue.lock().clear();
    }

    fn drain_all(&self) -> Vec<QueuedCommand> {
        let mut guard = self.queue.lock();
        std::mem::take(&mut *guard)
    }

    fn queue_get(&self, key: &Bound<'_, PyAny>) -> PyResult<()> {
        let kb = extract_bytes_arg(key)?;
        self.push(QueuedCommand {
            args: vec![b"GET".to_vec(), kb.clone()],
            keys: vec![kb],
            read_only: true,
            decoder: PipelineDecoder::BytesOrNone,
        });
        Ok(())
    }

    fn queue_set(
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

    fn queue_multi_key_int(
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

    fn queue_expire_like(&self, op: &str, key: &Bound<'_, PyAny>, val: i64) -> PyResult<()> {
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

    fn queue_ttl_like(&self, op: &str, key: &Bound<'_, PyAny>) -> PyResult<()> {
        let kb = extract_bytes_arg(key)?;
        self.push(QueuedCommand {
            args: vec![op.as_bytes().to_vec(), kb.clone()],
            keys: vec![kb],
            read_only: true,
            decoder: PipelineDecoder::Int,
        });
        Ok(())
    }

    fn queue_incr_like(&self, op: &str, key: &Bound<'_, PyAny>, amount: i64) -> PyResult<()> {
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

    fn queue_hset(
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

    fn queue_hget(&self, key: &Bound<'_, PyAny>, field: &Bound<'_, PyAny>) -> PyResult<()> {
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

    fn queue_hgetall(&self, key: &Bound<'_, PyAny>) -> PyResult<()> {
        let kb = extract_bytes_arg(key)?;
        self.push(QueuedCommand {
            args: vec![b"HGETALL".to_vec(), kb.clone()],
            keys: vec![kb],
            read_only: true,
            decoder: PipelineDecoder::HGetAllDict,
        });
        Ok(())
    }

    fn queue_hdel(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let kb = extract_bytes_arg(key)?;
        let fl = extract_varargs_bytes(fields)?;
        if fl.is_empty() {
            return Err(ValkeyError::programming("HDEL requires at least one field").to_py_err(py));
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

    fn queue_hash_expire(
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

    fn queue_hash_query(
        &self,
        py: Python<'_>,
        cmd_name: &str,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
        read_only: bool,
    ) -> PyResult<()> {
        let kb = extract_bytes_arg(key)?;
        let fl = extract_varargs_bytes(fields)?;
        let cmd = build_hash_field_query_cmd(cmd_name, &kb, &fl).map_err(|e| e.to_py_err(py))?;
        self.push(QueuedCommand {
            args: cmd,
            keys: vec![kb],
            read_only,
            decoder: PipelineDecoder::IntList,
        });
        Ok(())
    }

    fn queue_publish(&self, channel: String, message: &Bound<'_, PyAny>) -> PyResult<()> {
        let msg = extract_bytes_arg(message)?;
        self.push(QueuedCommand {
            args: vec![b"PUBLISH".to_vec(), channel.into_bytes(), msg],
            keys: Vec::new(),
            read_only: false,
            decoder: PipelineDecoder::Int,
        });
        Ok(())
    }

    fn queue_xadd(
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

    fn queue_xack(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        group: String,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let kb = extract_bytes_arg(key)?;
        let id_list = crate::pubsub::extract_string_args(ids)?;
        let cmd = build_xack(&kb, &group, &id_list).map_err(|e| e.to_py_err(py))?;
        self.push(QueuedCommand {
            args: cmd,
            keys: vec![kb],
            read_only: false,
            decoder: PipelineDecoder::Int,
        });
        Ok(())
    }

    fn queue_xdel(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let kb = extract_bytes_arg(key)?;
        let id_list = crate::pubsub::extract_string_args(ids)?;
        let cmd = build_xdel(&kb, &id_list).map_err(|e| e.to_py_err(py))?;
        self.push(QueuedCommand {
            args: cmd,
            keys: vec![kb],
            read_only: false,
            decoder: PipelineDecoder::Int,
        });
        Ok(())
    }

    fn queue_zadd(
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

    fn queue_eval(
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

    fn queue_evalsha(
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

    fn queue_execute_command(&self, py: Python<'_>, args: &Bound<'_, PyAny>) -> PyResult<()> {
        let items = args.extract::<Vec<Bound<'_, PyAny>>>()?;
        if items.is_empty() {
            return Err(
                ValkeyError::programming("execute_command requires a command name").to_py_err(py),
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

/// Synchronous batched Valkey pipeline (`SyncValkeyClient.pipeline()`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(frozen, immutable_type, freelist = 128, module = "vertebra._valkey")
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(frozen, immutable_type, module = "vertebra._valkey")
)]
pub struct SyncValkeyPipeline {
    inner: PipelineBuilder,
}

impl SyncValkeyPipeline {
    fn from_engine(engine: Arc<ValkeyEngine>, transaction: bool) -> Self {
        Self {
            inner: PipelineBuilder::new(engine, transaction),
        }
    }
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl SyncValkeyPipeline {
    #[getter]
    pub fn transaction(&self) -> bool {
        self.inner.transaction
    }

    pub fn __len__(&self) -> usize {
        self.inner.len()
    }

    pub fn clear(slf: Py<Self>, py: Python<'_>) -> Py<Self> {
        slf.borrow(py).inner.clear();
        slf
    }

    pub fn get(slf: Py<Self>, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_get(key)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, value, *, px = None, ex = None, nx = false, xx = false))]
    pub fn set(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        px: Option<i64>,
        ex: Option<i64>,
        nx: bool,
        xx: bool,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_set(py, key, value, px, ex, nx, xx)?;
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    pub fn delete(slf: Py<Self>, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_multi_key_int("DEL", keys, false)?;
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    pub fn unlink(slf: Py<Self>, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_multi_key_int("UNLINK", keys, false)?;
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    pub fn exists(slf: Py<Self>, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_multi_key_int("EXISTS", keys, true)?;
        Ok(slf)
    }

    pub fn expire(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        seconds: i64,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_expire_like("EXPIRE", key, seconds)?;
        Ok(slf)
    }

    pub fn pexpire(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ms: i64,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_expire_like("PEXPIRE", key, ms)?;
        Ok(slf)
    }

    pub fn ttl(slf: Py<Self>, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_ttl_like("TTL", key)?;
        Ok(slf)
    }

    pub fn pttl(slf: Py<Self>, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_ttl_like("PTTL", key)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, amount = 1))]
    pub fn incr(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        amount: i64,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_incr_like("INCRBY", key, amount)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, amount = 1))]
    pub fn decr(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        amount: i64,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_incr_like("DECRBY", key, amount)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, mapping, value = None))]
    pub fn hset(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        mapping: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hset(py, key, mapping, value)?;
        Ok(slf)
    }

    pub fn hget(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        field: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hget(key, field)?;
        Ok(slf)
    }

    pub fn hgetall(slf: Py<Self>, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hgetall(key)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hdel(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hdel(py, key, fields)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, ttl_seconds, *fields, condition = None))]
    pub fn hexpire(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ttl_seconds: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hash_expire(
            py,
            "HEXPIRE",
            key,
            ttl_seconds,
            fields,
            condition,
        )?;
        Ok(slf)
    }

    #[pyo3(signature = (key, ttl_ms, *fields, condition = None))]
    pub fn hpexpire(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ttl_ms: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_hash_expire(py, "HPEXPIRE", key, ttl_ms, fields, condition)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, unix_time_seconds, *fields, condition = None))]
    pub fn hexpireat(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        unix_time_seconds: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hash_expire(
            py,
            "HEXPIREAT",
            key,
            unix_time_seconds,
            fields,
            condition,
        )?;
        Ok(slf)
    }

    #[pyo3(signature = (key, unix_time_ms, *fields, condition = None))]
    pub fn hpexpireat(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        unix_time_ms: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hash_expire(
            py,
            "HPEXPIREAT",
            key,
            unix_time_ms,
            fields,
            condition,
        )?;
        Ok(slf)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn httl(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_hash_query(py, "HTTL", key, fields, true)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hpttl(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_hash_query(py, "HPTTL", key, fields, true)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hpersist(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_hash_query(py, "HPERSIST", key, fields, false)?;
        Ok(slf)
    }

    pub fn publish(
        slf: Py<Self>,
        py: Python<'_>,
        channel: String,
        message: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_publish(channel, message)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, fields, *, id = "*", maxlen = None, approximate = true, nomkstream = false))]
    pub fn xadd(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
        id: &str,
        maxlen: Option<u64>,
        approximate: bool,
        nomkstream: bool,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_xadd(py, key, fields, id, maxlen, approximate, nomkstream)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, group, *ids))]
    pub fn xack(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        group: String,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_xack(py, key, group, ids)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, *ids))]
    pub fn xdel(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_xdel(py, key, ids)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, members, *, nx = false, xx = false, gt = false, lt = false, ch = false))]
    pub fn zadd(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        members: &Bound<'_, PyAny>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_zadd(py, key, members, nx, xx, gt, lt, ch)?;
        Ok(slf)
    }

    #[pyo3(signature = (script, keys = None, args = None))]
    pub fn eval(
        slf: Py<Self>,
        py: Python<'_>,
        script: &Bound<'_, PyAny>,
        keys: Option<&Bound<'_, PyAny>>,
        args: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_eval(script, keys, args)?;
        Ok(slf)
    }

    #[pyo3(signature = (sha1, keys = None, args = None))]
    pub fn evalsha(
        slf: Py<Self>,
        py: Python<'_>,
        sha1: String,
        keys: Option<&Bound<'_, PyAny>>,
        args: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_evalsha(sha1, keys, args)?;
        Ok(slf)
    }

    #[pyo3(signature = (*args))]
    pub fn execute_command(
        slf: Py<Self>,
        py: Python<'_>,
        args: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_execute_command(py, args)?;
        Ok(slf)
    }

    pub fn execute(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let queued = self.inner.drain_all();
        let engine = Arc::clone(&self.inner.engine);
        let transaction = self.inner.transaction;
        let items = exec_sync_valkey(py, &self.inner.engine, async move {
            engine.execute_pipeline_commands(queued, transaction).await
        })?;
        decode_pipeline_py(py, items)
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type = None, _exc_val = None, _exc_tb = None))]
    pub fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        self.inner.clear();
        false
    }
}

/// Asynchronous batched Valkey pipeline (`AsyncValkeyClient.pipeline()`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(frozen, immutable_type, freelist = 128, module = "vertebra._valkey")
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(frozen, immutable_type, module = "vertebra._valkey")
)]
pub struct AsyncValkeyPipeline {
    inner: PipelineBuilder,
}

impl AsyncValkeyPipeline {
    fn from_engine(engine: Arc<ValkeyEngine>, transaction: bool) -> Self {
        Self {
            inner: PipelineBuilder::new(engine, transaction),
        }
    }
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl AsyncValkeyPipeline {
    #[getter]
    pub fn transaction(&self) -> bool {
        self.inner.transaction
    }

    pub fn __len__(&self) -> usize {
        self.inner.len()
    }

    pub fn clear(slf: Py<Self>, py: Python<'_>) -> Py<Self> {
        slf.borrow(py).inner.clear();
        slf
    }

    pub fn get(slf: Py<Self>, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_get(key)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, value, *, px = None, ex = None, nx = false, xx = false))]
    pub fn set(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        px: Option<i64>,
        ex: Option<i64>,
        nx: bool,
        xx: bool,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_set(py, key, value, px, ex, nx, xx)?;
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    pub fn delete(slf: Py<Self>, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_multi_key_int("DEL", keys, false)?;
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    pub fn unlink(slf: Py<Self>, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_multi_key_int("UNLINK", keys, false)?;
        Ok(slf)
    }

    #[pyo3(signature = (*keys))]
    pub fn exists(slf: Py<Self>, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_multi_key_int("EXISTS", keys, true)?;
        Ok(slf)
    }

    pub fn expire(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        seconds: i64,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_expire_like("EXPIRE", key, seconds)?;
        Ok(slf)
    }

    pub fn pexpire(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ms: i64,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_expire_like("PEXPIRE", key, ms)?;
        Ok(slf)
    }

    pub fn ttl(slf: Py<Self>, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_ttl_like("TTL", key)?;
        Ok(slf)
    }

    pub fn pttl(slf: Py<Self>, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_ttl_like("PTTL", key)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, amount = 1))]
    pub fn incr(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        amount: i64,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_incr_like("INCRBY", key, amount)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, amount = 1))]
    pub fn decr(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        amount: i64,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_incr_like("DECRBY", key, amount)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, mapping, value = None))]
    pub fn hset(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        mapping: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hset(py, key, mapping, value)?;
        Ok(slf)
    }

    pub fn hget(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        field: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hget(key, field)?;
        Ok(slf)
    }

    pub fn hgetall(slf: Py<Self>, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hgetall(key)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hdel(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hdel(py, key, fields)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, ttl_seconds, *fields, condition = None))]
    pub fn hexpire(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ttl_seconds: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hash_expire(
            py,
            "HEXPIRE",
            key,
            ttl_seconds,
            fields,
            condition,
        )?;
        Ok(slf)
    }

    #[pyo3(signature = (key, ttl_ms, *fields, condition = None))]
    pub fn hpexpire(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ttl_ms: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_hash_expire(py, "HPEXPIRE", key, ttl_ms, fields, condition)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, unix_time_seconds, *fields, condition = None))]
    pub fn hexpireat(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        unix_time_seconds: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hash_expire(
            py,
            "HEXPIREAT",
            key,
            unix_time_seconds,
            fields,
            condition,
        )?;
        Ok(slf)
    }

    #[pyo3(signature = (key, unix_time_ms, *fields, condition = None))]
    pub fn hpexpireat(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        unix_time_ms: i64,
        fields: &Bound<'_, PyAny>,
        condition: Option<String>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_hash_expire(
            py,
            "HPEXPIREAT",
            key,
            unix_time_ms,
            fields,
            condition,
        )?;
        Ok(slf)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn httl(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_hash_query(py, "HTTL", key, fields, true)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hpttl(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_hash_query(py, "HPTTL", key, fields, true)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, *fields))]
    pub fn hpersist(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_hash_query(py, "HPERSIST", key, fields, false)?;
        Ok(slf)
    }

    pub fn publish(
        slf: Py<Self>,
        py: Python<'_>,
        channel: String,
        message: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_publish(channel, message)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, fields, *, id = "*", maxlen = None, approximate = true, nomkstream = false))]
    pub fn xadd(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        fields: &Bound<'_, PyAny>,
        id: &str,
        maxlen: Option<u64>,
        approximate: bool,
        nomkstream: bool,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_xadd(py, key, fields, id, maxlen, approximate, nomkstream)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, group, *ids))]
    pub fn xack(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        group: String,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_xack(py, key, group, ids)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, *ids))]
    pub fn xdel(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_xdel(py, key, ids)?;
        Ok(slf)
    }

    #[pyo3(signature = (key, members, *, nx = false, xx = false, gt = false, lt = false, ch = false))]
    pub fn zadd(
        slf: Py<Self>,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        members: &Bound<'_, PyAny>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py)
            .inner
            .queue_zadd(py, key, members, nx, xx, gt, lt, ch)?;
        Ok(slf)
    }

    #[pyo3(signature = (script, keys = None, args = None))]
    pub fn eval(
        slf: Py<Self>,
        py: Python<'_>,
        script: &Bound<'_, PyAny>,
        keys: Option<&Bound<'_, PyAny>>,
        args: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_eval(script, keys, args)?;
        Ok(slf)
    }

    #[pyo3(signature = (sha1, keys = None, args = None))]
    pub fn evalsha(
        slf: Py<Self>,
        py: Python<'_>,
        sha1: String,
        keys: Option<&Bound<'_, PyAny>>,
        args: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_evalsha(sha1, keys, args)?;
        Ok(slf)
    }

    #[pyo3(signature = (*args))]
    pub fn execute_command(
        slf: Py<Self>,
        py: Python<'_>,
        args: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        slf.borrow(py).inner.queue_execute_command(py, args)?;
        Ok(slf)
    }

    pub fn execute<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let queued = self.inner.drain_all();
        let engine = Arc::clone(&self.inner.engine);
        let transaction = self.inner.transaction;
        spawn_valkey_cmd_task(
            py,
            &self.inner.engine.config,
            async move { engine.execute_pipeline_commands(queued, transaction).await },
            decode_pipeline_py,
        )
    }

    pub fn __aenter__<'py>(
        slf: Py<Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        spawn_valkey_task(py, async move { Ok(slf) }, |_py, obj| Ok(obj.into_any()))
    }

    #[pyo3(signature = (_exc_type = None, _exc_val = None, _exc_tb = None))]
    pub fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.inner.clear();
        spawn_valkey_task(py, async move { Ok(false) }, |py, suppressed| {
            Ok(PyBool::new(py, suppressed).to_owned().into_any().unbind())
        })
    }
}
