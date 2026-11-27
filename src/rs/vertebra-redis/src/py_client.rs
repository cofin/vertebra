//! PyO3 client and pipeline bindings for Redis delegating to `vertebra_resp::client::RespEngine`.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use vertebra_db::py_async::{TaskAwaitable, block_on_db, spawn_rust_db_task_with_attr};
use vertebra_resp::client::RespEngine;
use vertebra_resp::client::py_helpers::{
    parse_redis_fields, parse_redis_streams_arg, parse_redis_zadd_mapping, py_seq_to_redis_args,
    py_to_redis_arg, slot_map_to_py_dict,
};
use vertebra_resp::codec::{RespValue, resp_to_py};
use vertebra_resp::config::{RedisTopologyMode, RespConfig, extract_gcp_auth_source};
use vertebra_resp::error::RespError;

use crate::pubsub::{AsyncRedisListener, SyncRedisListener};

pub const REDIS_COMPLETION_DRAINER_ATTR: &str = "_vertebra_redis_completion_drainer";

/// Reject `SET` option combinations Redis cannot honor before any command is queued.
fn validate_set_options(ex: Option<i64>, px: Option<i64>, nx: bool, xx: bool) -> PyResult<()> {
    if ex.is_some() && px.is_some() {
        return Err(PyValueError::new_err("set() accepts ex or px, not both"));
    }
    if nx && xx {
        return Err(PyValueError::new_err("set() accepts nx or xx, not both"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_engine_from_args(
    py: Python<'_>,
    url: &str,
    username: Option<String>,
    password: Option<String>,
    client_name: String,
    protocol: u8,
    topology: &str,
    sentinel_master_name: Option<String>,
    sentinel_nodes: Vec<(String, u16)>,
    cluster_nodes: Vec<(String, u16)>,
    max_redirections: usize,
    enable_client_side_caching: bool,
    client_tracking_prefixes: Vec<String>,
    client_cache_capacity: usize,
    client_cache_ttl_seconds: Option<f64>,
    enable_gcp_iam_auth: bool,
    token_cache_capsule: Option<&Bound<'_, PyAny>>,
    max_connections: usize,
) -> PyResult<Arc<RespEngine>> {
    let mode = RedisTopologyMode::parse(topology).map_err(|e| e.to_py_err(py))?;
    let gcp_auth = match token_cache_capsule {
        Some(obj) if !obj.is_none() => Some(extract_gcp_auth_source(py, obj)?),
        _ => None,
    };
    let cfg = RespConfig::build_redis(
        url,
        username,
        password,
        client_name,
        protocol,
        mode,
        sentinel_master_name,
        sentinel_nodes,
        cluster_nodes,
        max_redirections,
        enable_client_side_caching,
        client_tracking_prefixes,
        client_cache_capacity,
        client_cache_ttl_seconds,
        enable_gcp_iam_auth,
        gcp_auth,
        max_connections,
    )
    .map_err(|e| e.to_py_err(py))?;
    Ok(RespEngine::new(Arc::new(cfg)))
}

fn extract_id_strings(ids: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let raw = py_seq_to_redis_args(ids)?;
    Ok(raw
        .into_iter()
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect())
}

fn pipeline_values_to_py(py: Python<'_>, values: Vec<RespValue>) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for val in values {
        list.append(resp_to_py(py, val)?)?;
    }
    Ok(list.into_any().unbind())
}

fn scan_result_to_py(
    py: Python<'_>,
    (next_cursor, keys): (u64, Vec<bytes::Bytes>),
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for key in keys {
        list.append(PyBytes::new(py, &key))?;
    }
    let tuple = PyTuple::new(
        py,
        &[next_cursor.into_pyobject(py)?.into_any(), list.into_any()],
    )?;
    Ok(tuple.into_any().unbind())
}

// --- Pipelines ---

#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        name = "SyncRedisPipeline",
        module = "vertebra._redis",
        frozen,
        immutable_type,
        freelist = 128
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(
        name = "SyncRedisPipeline",
        module = "vertebra._redis",
        frozen,
        immutable_type
    )
)]
pub struct SyncRedisPipeline {
    engine: Arc<RespEngine>,
    commands: Mutex<Vec<Vec<Vec<u8>>>>,
    atomic: bool,
}

#[pymethods]
impl SyncRedisPipeline {
    pub fn execute_command(slf: PyRef<'_, Self>, args: Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        let cmd_args = py_seq_to_redis_args(&args)?;
        slf.commands.lock().push(cmd_args);
        Ok(slf.into())
    }

    pub fn results(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmds = std::mem::take(&mut *self.commands.lock());
        let engine = Arc::clone(&self.engine);
        let atomic = self.atomic;
        let results = py
            .detach(|| block_on_db(async move { engine.execute_raw_pipeline(cmds, atomic).await }))
            .map_err(|e| e.to_py_err(py))?;
        pipeline_values_to_py(py, results)
    }
}

#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        name = "AsyncRedisPipeline",
        module = "vertebra._redis",
        frozen,
        immutable_type,
        freelist = 128
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(
        name = "AsyncRedisPipeline",
        module = "vertebra._redis",
        frozen,
        immutable_type
    )
)]
pub struct AsyncRedisPipeline {
    engine: Arc<RespEngine>,
    commands: Mutex<Vec<Vec<Vec<u8>>>>,
    atomic: bool,
}

#[pymethods]
impl AsyncRedisPipeline {
    pub fn execute_command(slf: PyRef<'_, Self>, args: Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        let cmd_args = py_seq_to_redis_args(&args)?;
        slf.commands.lock().push(cmd_args);
        Ok(slf.into())
    }

    pub fn results(&self, py: Python<'_>) -> PyResult<TaskAwaitable> {
        let cmds = std::mem::take(&mut *self.commands.lock());
        let engine = Arc::clone(&self.engine);
        let atomic = self.atomic;
        spawn_rust_db_task_with_attr(
            py,
            async move { engine.execute_raw_pipeline(cmds, atomic).await },
            pipeline_values_to_py,
            |py, e| e.to_py_err(py),
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn submit_results(&self, py: Python<'_>) -> PyResult<TaskAwaitable> {
        self.results(py)
    }
}

// --- SyncRedisClient ---

#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        name = "SyncRedisClient",
        module = "vertebra._redis",
        frozen,
        immutable_type,
        freelist = 128
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(
        name = "SyncRedisClient",
        module = "vertebra._redis",
        frozen,
        immutable_type
    )
)]
pub struct SyncRedisClient {
    engine: Arc<RespEngine>,
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl SyncRedisClient {
    #[new]
    #[pyo3(signature = (
        url,
        username = None,
        password = None,
        client_name = "vertebra".to_string(),
        protocol = 3,
        topology = "standalone",
        sentinel_master_name = None,
        sentinel_nodes = Vec::new(),
        cluster_nodes = Vec::new(),
        max_redirections = 5,
        enable_client_side_caching = false,
        client_tracking_prefixes = Vec::new(),
        client_cache_capacity = 4096,
        client_cache_ttl_seconds = None,
        enable_gcp_iam_auth = false,
        token_cache_capsule = None,
        max_connections = 32
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        url: &str,
        username: Option<String>,
        password: Option<String>,
        client_name: String,
        protocol: u8,
        topology: &str,
        sentinel_master_name: Option<String>,
        sentinel_nodes: Vec<(String, u16)>,
        cluster_nodes: Vec<(String, u16)>,
        max_redirections: usize,
        enable_client_side_caching: bool,
        client_tracking_prefixes: Vec<String>,
        client_cache_capacity: usize,
        client_cache_ttl_seconds: Option<f64>,
        enable_gcp_iam_auth: bool,
        token_cache_capsule: Option<&Bound<'_, PyAny>>,
        max_connections: usize,
    ) -> PyResult<Self> {
        let engine = build_engine_from_args(
            py,
            url,
            username,
            password,
            client_name,
            protocol,
            topology,
            sentinel_master_name,
            sentinel_nodes,
            cluster_nodes,
            max_redirections,
            enable_client_side_caching,
            client_tracking_prefixes,
            client_cache_capacity,
            client_cache_ttl_seconds,
            enable_gcp_iam_auth,
            token_cache_capsule,
            max_connections,
        )?;
        Ok(Self { engine })
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.engine.is_closed()
    }

    pub fn connect(&self, py: Python<'_>) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        py.detach(|| block_on_db(async move { engine.connect().await }))
            .map_err(|e| e.to_py_err(py))
    }

    pub fn close(&self) {
        self.engine.close_sync();
    }

    #[pyo3(signature = (password = None))]
    pub fn reconnect(&self, py: Python<'_>, password: Option<&str>) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        let pw = password.map(str::to_string);
        py.detach(|| block_on_db(async move { engine.reconnect(pw).await }))
            .map_err(|e| e.to_py_err(py))
    }

    pub fn get(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
        let k = py_to_redis_arg(&key)?;
        let engine = Arc::clone(&self.engine);
        let res = py
            .detach(|| block_on_db(async move { engine.get_bytes(k).await }))
            .map_err(|e| e.to_py_err(py))?;
        Ok(res.map(|b| PyBytes::new(py, &b).into_any().unbind()))
    }

    #[pyo3(signature = (key, value, ex = None, px = None, nx = false, xx = false))]
    pub fn set(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        value: Bound<'_, PyAny>,
        ex: Option<i64>,
        px: Option<i64>,
        nx: bool,
        xx: bool,
    ) -> PyResult<bool> {
        validate_set_options(ex, px, nx, xx)?;
        let k = py_to_redis_arg(&key)?;
        let v = py_to_redis_arg(&value)?;
        let engine = Arc::clone(&self.engine);
        py.detach(|| block_on_db(async move { engine.set_bytes(k, v, px, ex, nx, xx).await }))
            .map_err(|e| e.to_py_err(py))
    }

    pub fn delete(&self, py: Python<'_>, keys: Bound<'_, PyAny>) -> PyResult<i64> {
        let ks = py_seq_to_redis_args(&keys)?;
        if ks.is_empty() {
            return Ok(0);
        }
        let engine = Arc::clone(&self.engine);
        py.detach(|| block_on_db(async move { engine.multi_key_int_cmd("DEL", ks, false).await }))
            .map_err(|e| e.to_py_err(py))
    }

    pub fn unlink(&self, py: Python<'_>, keys: Bound<'_, PyAny>) -> PyResult<i64> {
        let ks = py_seq_to_redis_args(&keys)?;
        if ks.is_empty() {
            return Ok(0);
        }
        let engine = Arc::clone(&self.engine);
        py.detach(|| {
            block_on_db(async move { engine.multi_key_int_cmd("UNLINK", ks, false).await })
        })
        .map_err(|e| e.to_py_err(py))
    }

    pub fn pttl(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<i64> {
        let k = py_to_redis_arg(&key)?;
        let engine = Arc::clone(&self.engine);
        let resp = py
            .detach(|| {
                block_on_db(async move {
                    let key_refs = [k.as_slice()];
                    let cmd = vec![b"PTTL".to_vec(), k.clone()];
                    engine.execute_command_routed(cmd, &key_refs, true).await
                })
            })
            .map_err(|e| e.to_py_err(py))?;
        Ok(resp.as_i64().unwrap_or(-2))
    }

    #[pyo3(signature = (cursor = 0, match_pattern = None, count = None))]
    pub fn scan(
        &self,
        py: Python<'_>,
        cursor: u64,
        match_pattern: Option<String>,
        count: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let engine = Arc::clone(&self.engine);
        let res = py
            .detach(|| block_on_db(async move { engine.scan(cursor, match_pattern, count).await }))
            .map_err(|e| e.to_py_err(py))?;
        scan_result_to_py(py, res)
    }

    pub fn flushdb(&self, py: Python<'_>) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        py.detach(|| block_on_db(async move { engine.flushdb(false).await }))
            .map_err(|e| e.to_py_err(py))?;
        Ok(())
    }

    pub fn publish(
        &self,
        py: Python<'_>,
        channel: &str,
        message: Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let msg = py_to_redis_arg(&message)?;
        let ch = channel.to_string();
        let engine = Arc::clone(&self.engine);
        py.detach(|| block_on_db(async move { engine.publish(&ch, msg).await }))
            .map_err(|e| e.to_py_err(py))
    }

    pub fn spublish(
        &self,
        py: Python<'_>,
        shard_channel: &str,
        message: Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let msg = py_to_redis_arg(&message)?;
        let ch = shard_channel.to_string();
        let engine = Arc::clone(&self.engine);
        py.detach(|| block_on_db(async move { engine.spublish(&ch, msg).await }))
            .map_err(|e| e.to_py_err(py))
    }

    pub fn execute_command(&self, py: Python<'_>, args: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let cmd = py_seq_to_redis_args(&args)?;
        let engine = Arc::clone(&self.engine);
        let val = py
            .detach(|| block_on_db(async move { engine.execute_raw_command(cmd).await }))
            .map_err(|e| e.to_py_err(py))?;
        resp_to_py(py, val)
    }

    #[pyo3(signature = (atomic = false))]
    pub fn pipeline(&self, atomic: bool) -> SyncRedisPipeline {
        SyncRedisPipeline {
            engine: Arc::clone(&self.engine),
            commands: Mutex::new(Vec::new()),
            atomic,
        }
    }

    pub fn eval_script(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Bound<'_, PyAny>,
        args: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let script_bytes = script.as_bytes().to_vec();
        let key_list = py_seq_to_redis_args(&keys)?;
        let arg_list = py_seq_to_redis_args(&args)?;
        let engine = Arc::clone(&self.engine);
        let val = py
            .detach(|| {
                block_on_db(async move {
                    engine
                        .eval_with_fallback(script_bytes, key_list, arg_list)
                        .await
                })
            })
            .map_err(|e| e.to_py_err(py))?;
        resp_to_py(py, val)
    }

    pub fn create_listener(&self) -> SyncRedisListener {
        SyncRedisListener::from_config(self.engine.effective_config())
    }

    pub fn listener(&self) -> SyncRedisListener {
        self.create_listener()
    }

    pub fn cached_keys(&self) -> Vec<String> {
        self.engine.cached_keys()
    }

    pub fn drain_invalidations(&self) -> Vec<String> {
        self.engine.drain_invalidations()
    }

    pub fn slot_map(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        slot_map_to_py_dict(py, self.engine.slot_map())
    }

    #[pyo3(signature = (stream, group, id = "$", mkstream = false))]
    pub fn xgroup_create(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> PyResult<bool> {
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        let g = group.to_string();
        let sid = id.to_string();
        py.detach(|| {
            block_on_db(async move { engine.xgroup_create(s, &g, &sid, mkstream, true).await })
        })
        .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (stream, fields, id = "*", maxlen = None, approximate = true))]
    pub fn xadd(
        &self,
        py: Python<'_>,
        stream: &str,
        fields: Bound<'_, PyAny>,
        id: &str,
        maxlen: Option<u64>,
        approximate: bool,
    ) -> PyResult<Py<PyAny>> {
        let parsed = parse_redis_fields(&fields)?;
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        let sid = id.to_string();
        let entry_id = py
            .detach(|| {
                block_on_db(async move {
                    engine
                        .xadd(s, &parsed, &sid, maxlen, approximate, false)
                        .await
                })
            })
            .map_err(|e| e.to_py_err(py))?;
        Ok(PyBytes::new(py, &entry_id).into_any().unbind())
    }

    #[pyo3(signature = (group, consumer, streams_obj, count = None, block_ms = None))]
    pub fn xreadgroup(
        &self,
        py: Python<'_>,
        group: &str,
        consumer: &str,
        streams_obj: Bound<'_, PyAny>,
        count: Option<u64>,
        block_ms: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let parsed = parse_redis_streams_arg(&streams_obj)?;
        let engine = Arc::clone(&self.engine);
        let g = group.to_string();
        let c = consumer.to_string();
        let val = py
            .detach(|| {
                block_on_db(async move {
                    engine
                        .xreadgroup(&g, &c, &parsed, count, block_ms, false)
                        .await
                })
            })
            .map_err(|e| e.to_py_err(py))?;
        resp_to_py(py, val)
    }

    pub fn xack(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        ids: Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let id_list = extract_id_strings(&ids)?;
        if id_list.is_empty() {
            return Ok(0);
        }
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        let g = group.to_string();
        py.detach(|| block_on_db(async move { engine.xack(s, &g, &id_list).await }))
            .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (stream, group, consumer, min_idle_ms, start_id = "0-0", count = None))]
    pub fn xautoclaim(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start_id: &str,
        count: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        let g = group.to_string();
        let c = consumer.to_string();
        let sid = start_id.to_string();
        let val = py
            .detach(|| {
                block_on_db(async move {
                    engine
                        .xautoclaim(s, &g, &c, min_idle_ms, &sid, count, false)
                        .await
                })
            })
            .map_err(|e| e.to_py_err(py))?;
        resp_to_py(py, val)
    }

    #[pyo3(signature = (stream, start = "-", end = "+", count = None))]
    pub fn xrange(
        &self,
        py: Python<'_>,
        stream: &str,
        start: &str,
        end: &str,
        count: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        let st = start.to_string();
        let ed = end.to_string();
        let val = py
            .detach(|| block_on_db(async move { engine.xrange(s, &st, &ed, count).await }))
            .map_err(|e| e.to_py_err(py))?;
        resp_to_py(py, val)
    }

    pub fn xdel(&self, py: Python<'_>, stream: &str, ids: Bound<'_, PyAny>) -> PyResult<i64> {
        let id_list = extract_id_strings(&ids)?;
        if id_list.is_empty() {
            return Ok(0);
        }
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        py.detach(|| block_on_db(async move { engine.xdel(s, &id_list).await }))
            .map_err(|e| e.to_py_err(py))
    }

    pub fn zadd(&self, py: Python<'_>, key: &str, mapping: Bound<'_, PyAny>) -> PyResult<i64> {
        let parsed = parse_redis_zadd_mapping(&mapping)?;
        if parsed.is_empty() {
            return Ok(0);
        }
        let engine = Arc::clone(&self.engine);
        let k = key.as_bytes().to_vec();
        py.detach(|| {
            block_on_db(async move {
                engine
                    .zadd(k, &parsed, false, false, false, false, false)
                    .await
            })
        })
        .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (key, min, max, withscores = false, offset = None, count = None))]
    pub fn zrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        withscores: bool,
        offset: Option<u64>,
        count: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let engine = Arc::clone(&self.engine);
        let k = key.as_bytes().to_vec();
        let mn = min.to_string();
        let mx = max.to_string();
        let val = py
            .detach(|| {
                block_on_db(async move {
                    engine
                        .zrangebyscore(k, &mn, &mx, withscores, offset, count)
                        .await
                })
            })
            .map_err(|e| e.to_py_err(py))?;
        resp_to_py(py, val)
    }

    pub fn zrem(&self, py: Python<'_>, key: &str, members: Bound<'_, PyAny>) -> PyResult<i64> {
        let mems = py_seq_to_redis_args(&members)?;
        let engine = Arc::clone(&self.engine);
        let k = key.as_bytes().to_vec();
        py.detach(|| block_on_db(async move { engine.zrem(k, &mems).await }))
            .map_err(|e| e.to_py_err(py))
    }
}

// --- AsyncRedisClient ---

#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        name = "AsyncRedisClient",
        module = "vertebra._redis",
        frozen,
        immutable_type,
        freelist = 128
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(
        name = "AsyncRedisClient",
        module = "vertebra._redis",
        frozen,
        immutable_type
    )
)]
pub struct AsyncRedisClient {
    engine: Arc<RespEngine>,
}

fn spawn_task<F, T, MapOk>(py: Python<'_>, fut: F, map_ok: MapOk) -> PyResult<TaskAwaitable>
where
    F: std::future::Future<Output = Result<T, RespError>> + Send + 'static,
    T: Send + 'static,
    MapOk: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
{
    spawn_rust_db_task_with_attr(
        py,
        fut,
        map_ok,
        |py, err| err.to_py_err(py),
        REDIS_COMPLETION_DRAINER_ATTR,
    )
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl AsyncRedisClient {
    #[new]
    #[pyo3(signature = (
        url,
        username = None,
        password = None,
        client_name = "vertebra".to_string(),
        protocol = 3,
        topology = "standalone",
        sentinel_master_name = None,
        sentinel_nodes = Vec::new(),
        cluster_nodes = Vec::new(),
        max_redirections = 5,
        enable_client_side_caching = false,
        client_tracking_prefixes = Vec::new(),
        client_cache_capacity = 4096,
        client_cache_ttl_seconds = None,
        enable_gcp_iam_auth = false,
        token_cache_capsule = None,
        max_connections = 32
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        url: &str,
        username: Option<String>,
        password: Option<String>,
        client_name: String,
        protocol: u8,
        topology: &str,
        sentinel_master_name: Option<String>,
        sentinel_nodes: Vec<(String, u16)>,
        cluster_nodes: Vec<(String, u16)>,
        max_redirections: usize,
        enable_client_side_caching: bool,
        client_tracking_prefixes: Vec<String>,
        client_cache_capacity: usize,
        client_cache_ttl_seconds: Option<f64>,
        enable_gcp_iam_auth: bool,
        token_cache_capsule: Option<&Bound<'_, PyAny>>,
        max_connections: usize,
    ) -> PyResult<Self> {
        let engine = build_engine_from_args(
            py,
            url,
            username,
            password,
            client_name,
            protocol,
            topology,
            sentinel_master_name,
            sentinel_nodes,
            cluster_nodes,
            max_redirections,
            enable_client_side_caching,
            client_tracking_prefixes,
            client_cache_capacity,
            client_cache_ttl_seconds,
            enable_gcp_iam_auth,
            token_cache_capsule,
            max_connections,
        )?;
        Ok(Self { engine })
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.engine.is_closed()
    }

    pub fn connect(&self, py: Python<'_>) -> PyResult<TaskAwaitable> {
        let engine = Arc::clone(&self.engine);
        spawn_task(py, async move { engine.connect().await }, |py, ()| {
            Ok(py.None())
        })
    }

    pub fn close(&self) {
        self.engine.close_sync();
    }

    #[pyo3(signature = (password = None))]
    pub fn reconnect(&self, py: Python<'_>, password: Option<&str>) -> PyResult<TaskAwaitable> {
        let engine = Arc::clone(&self.engine);
        let pw = password.map(str::to_string);
        spawn_task(py, async move { engine.reconnect(pw).await }, |py, ()| {
            Ok(py.None())
        })
    }

    pub fn get(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<TaskAwaitable> {
        let k = py_to_redis_arg(&key)?;
        let engine = Arc::clone(&self.engine);
        spawn_task(py, async move { engine.get_bytes(k).await }, |py, res| {
            Ok(match res {
                Some(b) => PyBytes::new(py, &b).into_any().unbind(),
                None => py.None(),
            })
        })
    }

    #[pyo3(signature = (key, value, ex = None, px = None, nx = false, xx = false))]
    pub fn set(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        value: Bound<'_, PyAny>,
        ex: Option<i64>,
        px: Option<i64>,
        nx: bool,
        xx: bool,
    ) -> PyResult<TaskAwaitable> {
        validate_set_options(ex, px, nx, xx)?;
        let k = py_to_redis_arg(&key)?;
        let v = py_to_redis_arg(&value)?;
        let engine = Arc::clone(&self.engine);
        spawn_task(
            py,
            async move { engine.set_bytes(k, v, px, ex, nx, xx).await },
            |py, ok| Ok(ok.into_pyobject(py)?.to_owned().into_any().unbind()),
        )
    }

    pub fn delete(&self, py: Python<'_>, keys: Bound<'_, PyAny>) -> PyResult<TaskAwaitable> {
        let ks = py_seq_to_redis_args(&keys)?;
        let engine = Arc::clone(&self.engine);
        spawn_task(
            py,
            async move {
                if ks.is_empty() {
                    return Ok(0i64);
                }
                engine.multi_key_int_cmd("DEL", ks, false).await
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    pub fn unlink(&self, py: Python<'_>, keys: Bound<'_, PyAny>) -> PyResult<TaskAwaitable> {
        let ks = py_seq_to_redis_args(&keys)?;
        let engine = Arc::clone(&self.engine);
        spawn_task(
            py,
            async move {
                if ks.is_empty() {
                    return Ok(0i64);
                }
                engine.multi_key_int_cmd("UNLINK", ks, false).await
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    pub fn pttl(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<TaskAwaitable> {
        let k = py_to_redis_arg(&key)?;
        let engine = Arc::clone(&self.engine);
        spawn_task(
            py,
            async move {
                let key_refs = [k.as_slice()];
                let cmd = vec![b"PTTL".to_vec(), k.clone()];
                let resp = engine.execute_command_routed(cmd, &key_refs, true).await?;
                Ok(resp.as_i64().unwrap_or(-2))
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (cursor = 0, match_pattern = None, count = None))]
    pub fn scan(
        &self,
        py: Python<'_>,
        cursor: u64,
        match_pattern: Option<String>,
        count: Option<u64>,
    ) -> PyResult<TaskAwaitable> {
        let engine = Arc::clone(&self.engine);
        spawn_task(
            py,
            async move { engine.scan(cursor, match_pattern, count).await },
            scan_result_to_py,
        )
    }

    pub fn flushdb(&self, py: Python<'_>) -> PyResult<TaskAwaitable> {
        let engine = Arc::clone(&self.engine);
        spawn_task(
            py,
            async move {
                engine.flushdb(false).await?;
                Ok(())
            },
            |py, ()| Ok(py.None()),
        )
    }

    pub fn publish(
        &self,
        py: Python<'_>,
        channel: &str,
        message: Bound<'_, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let msg = py_to_redis_arg(&message)?;
        let ch = channel.to_string();
        let engine = Arc::clone(&self.engine);
        spawn_task(
            py,
            async move { engine.publish(&ch, msg).await },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    pub fn spublish(
        &self,
        py: Python<'_>,
        shard_channel: &str,
        message: Bound<'_, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let msg = py_to_redis_arg(&message)?;
        let ch = shard_channel.to_string();
        let engine = Arc::clone(&self.engine);
        spawn_task(
            py,
            async move { engine.spublish(&ch, msg).await },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    pub fn execute_command(
        &self,
        py: Python<'_>,
        args: Bound<'_, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let cmd = py_seq_to_redis_args(&args)?;
        let engine = Arc::clone(&self.engine);
        spawn_task(
            py,
            async move { engine.execute_raw_command(cmd).await },
            resp_to_py,
        )
    }

    #[pyo3(signature = (atomic = false))]
    pub fn pipeline(&self, atomic: bool) -> AsyncRedisPipeline {
        AsyncRedisPipeline {
            engine: Arc::clone(&self.engine),
            commands: Mutex::new(Vec::new()),
            atomic,
        }
    }

    pub fn eval_script(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Bound<'_, PyAny>,
        args: Bound<'_, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let script_bytes = script.as_bytes().to_vec();
        let key_list = py_seq_to_redis_args(&keys)?;
        let arg_list = py_seq_to_redis_args(&args)?;
        let engine = Arc::clone(&self.engine);
        spawn_task(
            py,
            async move {
                engine
                    .eval_with_fallback(script_bytes, key_list, arg_list)
                    .await
            },
            resp_to_py,
        )
    }

    pub fn create_listener(&self) -> AsyncRedisListener {
        AsyncRedisListener::from_config(self.engine.effective_config())
    }

    pub fn listener(&self) -> AsyncRedisListener {
        self.create_listener()
    }

    pub fn cached_keys(&self) -> Vec<String> {
        self.engine.cached_keys()
    }

    pub fn drain_invalidations(&self) -> Vec<String> {
        self.engine.drain_invalidations()
    }

    pub fn slot_map(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        slot_map_to_py_dict(py, self.engine.slot_map())
    }

    #[pyo3(signature = (stream, group, id = "$", mkstream = false))]
    pub fn xgroup_create(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> PyResult<TaskAwaitable> {
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        let g = group.to_string();
        let sid = id.to_string();
        spawn_task(
            py,
            async move { engine.xgroup_create(s, &g, &sid, mkstream, true).await },
            |py, ok| Ok(ok.into_pyobject(py)?.to_owned().into_any().unbind()),
        )
    }

    #[pyo3(signature = (stream, fields, id = "*", maxlen = None, approximate = true))]
    pub fn xadd(
        &self,
        py: Python<'_>,
        stream: &str,
        fields: Bound<'_, PyAny>,
        id: &str,
        maxlen: Option<u64>,
        approximate: bool,
    ) -> PyResult<TaskAwaitable> {
        let parsed = parse_redis_fields(&fields)?;
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        let sid = id.to_string();
        spawn_task(
            py,
            async move {
                engine
                    .xadd(s, &parsed, &sid, maxlen, approximate, false)
                    .await
            },
            |py, entry_id| Ok(PyBytes::new(py, &entry_id).into_any().unbind()),
        )
    }

    #[pyo3(signature = (group, consumer, streams_obj, count = None, block_ms = None))]
    pub fn xreadgroup(
        &self,
        py: Python<'_>,
        group: &str,
        consumer: &str,
        streams_obj: Bound<'_, PyAny>,
        count: Option<u64>,
        block_ms: Option<u64>,
    ) -> PyResult<TaskAwaitable> {
        let parsed = parse_redis_streams_arg(&streams_obj)?;
        let engine = Arc::clone(&self.engine);
        let g = group.to_string();
        let c = consumer.to_string();
        spawn_task(
            py,
            async move {
                engine
                    .xreadgroup(&g, &c, &parsed, count, block_ms, false)
                    .await
            },
            resp_to_py,
        )
    }

    pub fn xack(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        ids: Bound<'_, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let id_list = extract_id_strings(&ids)?;
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        let g = group.to_string();
        spawn_task(
            py,
            async move {
                if id_list.is_empty() {
                    return Ok(0i64);
                }
                engine.xack(s, &g, &id_list).await
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (stream, group, consumer, min_idle_ms, start_id = "0-0", count = None))]
    pub fn xautoclaim(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start_id: &str,
        count: Option<u64>,
    ) -> PyResult<TaskAwaitable> {
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        let g = group.to_string();
        let c = consumer.to_string();
        let sid = start_id.to_string();
        spawn_task(
            py,
            async move {
                engine
                    .xautoclaim(s, &g, &c, min_idle_ms, &sid, count, false)
                    .await
            },
            resp_to_py,
        )
    }

    #[pyo3(signature = (stream, start = "-", end = "+", count = None))]
    pub fn xrange(
        &self,
        py: Python<'_>,
        stream: &str,
        start: &str,
        end: &str,
        count: Option<u64>,
    ) -> PyResult<TaskAwaitable> {
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        let st = start.to_string();
        let ed = end.to_string();
        spawn_task(
            py,
            async move { engine.xrange(s, &st, &ed, count).await },
            resp_to_py,
        )
    }

    pub fn xdel(
        &self,
        py: Python<'_>,
        stream: &str,
        ids: Bound<'_, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let id_list = extract_id_strings(&ids)?;
        let engine = Arc::clone(&self.engine);
        let s = stream.as_bytes().to_vec();
        spawn_task(
            py,
            async move {
                if id_list.is_empty() {
                    return Ok(0i64);
                }
                engine.xdel(s, &id_list).await
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    pub fn zadd(
        &self,
        py: Python<'_>,
        key: &str,
        mapping: Bound<'_, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let parsed = parse_redis_zadd_mapping(&mapping)?;
        let engine = Arc::clone(&self.engine);
        let k = key.as_bytes().to_vec();
        spawn_task(
            py,
            async move {
                if parsed.is_empty() {
                    return Ok(0i64);
                }
                engine
                    .zadd(k, &parsed, false, false, false, false, false)
                    .await
            },
            |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
        )
    }

    #[pyo3(signature = (key, min, max, withscores = false, offset = None, count = None))]
    pub fn zrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        withscores: bool,
        offset: Option<u64>,
        count: Option<u64>,
    ) -> PyResult<TaskAwaitable> {
        let engine = Arc::clone(&self.engine);
        let k = key.as_bytes().to_vec();
        let mn = min.to_string();
        let mx = max.to_string();
        spawn_task(
            py,
            async move {
                engine
                    .zrangebyscore(k, &mn, &mx, withscores, offset, count)
                    .await
            },
            resp_to_py,
        )
    }

    pub fn zrem(
        &self,
        py: Python<'_>,
        key: &str,
        members: Bound<'_, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let mems = py_seq_to_redis_args(&members)?;
        let engine = Arc::clone(&self.engine);
        let k = key.as_bytes().to_vec();
        spawn_task(py, async move { engine.zrem(k, &mems).await }, |py, n| {
            Ok(n.into_pyobject(py)?.into_any().unbind())
        })
    }
}
