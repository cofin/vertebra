//! PyO3 client bindings for synchronous and asynchronous Redis operations.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PySequence, PyTuple};
use vertebra_db::py_async::{RustDbTaskAwaitable, block_on_db, spawn_rust_db_task_with_attr};

use crate::client_core::RedisCoreClient;
use crate::config::{RedisClientConfig, RedisTopologyMode};
use crate::errors::redis_error_to_py;
use crate::pubsub::{AsyncRedisListener, SyncRedisListener};
use crate::streams;
use crate::value::{py_seq_to_redis_args, py_to_redis_arg, redis_value_to_py};

pub const REDIS_COMPLETION_DRAINER_ATTR: &str = "_vertebra_redis_completion_drainer";

fn parse_fields(fields: &Bound<'_, PyAny>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    if let Ok(dict) = fields.cast::<PyDict>() {
        for (k, v) in dict.iter() {
            let key_bytes = py_to_redis_arg(&k)?;
            let val_bytes = py_to_redis_arg(&v)?;
            out.push((key_bytes, val_bytes));
        }
    } else if let Ok(seq) = fields.cast::<PySequence>() {
        for item in seq.try_iter()? {
            let item = item?;
            if let Ok(tuple) = item.cast::<PyTuple>() {
                if tuple.len() >= 2 {
                    let k = py_to_redis_arg(&tuple.get_item(0)?)?;
                    let v = py_to_redis_arg(&tuple.get_item(1)?)?;
                    out.push((k, v));
                }
            }
        }
    }
    Ok(out)
}

fn parse_zadd_mapping(mapping: &Bound<'_, PyAny>) -> PyResult<Vec<(f64, Vec<u8>)>> {
    let mut out = Vec::new();
    if let Ok(dict) = mapping.cast::<PyDict>() {
        for (k, v) in dict.iter() {
            let member_bytes = py_to_redis_arg(&k)?;
            let score: f64 = v.extract()?;
            out.push((score, member_bytes));
        }
    }
    Ok(out)
}

fn parse_streams_arg(streams_obj: &Bound<'_, PyAny>) -> PyResult<Vec<(String, String)>> {
    let mut out = Vec::new();
    if let Ok(dict) = streams_obj.cast::<PyDict>() {
        for (k, v) in dict.iter() {
            let stream_name = k.str()?.to_str()?.to_string();
            let id = v.str()?.to_str()?.to_string();
            out.push((stream_name, id));
        }
    } else if let Ok(seq) = streams_obj.cast::<PySequence>() {
        for item in seq.try_iter()? {
            let item = item?;
            if let Ok(tuple) = item.cast::<PyTuple>() {
                if tuple.len() >= 2 {
                    let s = tuple.get_item(0)?.str()?.to_str()?.to_string();
                    let id = tuple.get_item(1)?.str()?.to_str()?.to_string();
                    out.push((s, id));
                }
            }
        }
    }
    Ok(out)
}

// --- Pipelines ---

#[pyclass(name = "SyncRedisPipeline", module = "vertebra._redis", immutable_type)]
pub struct SyncRedisPipeline {
    client: Arc<RedisCoreClient>,
    commands: Vec<Vec<Vec<u8>>>,
    atomic: bool,
}

impl SyncRedisPipeline {
    pub fn new(client: Arc<RedisCoreClient>, atomic: bool) -> Self {
        Self {
            client,
            commands: Vec::new(),
            atomic,
        }
    }
}

#[pymethods]
impl SyncRedisPipeline {
    pub fn execute_command(mut slf: PyRefMut<'_, Self>, args: Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        let cmd_args = py_seq_to_redis_args(&args)?;
        slf.commands.push(cmd_args);
        Ok(slf.into())
    }

    pub fn results(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let cmds = std::mem::take(&mut self.commands);
        let client = Arc::clone(&self.client);
        let atomic = self.atomic;

        let results = py.detach(|| {
            block_on_db(async move {
                client.execute_pipeline(&cmds, atomic).await
            })
        }).map_err(|e| e.into_pyerr(py))?;

        let py_list = PyList::empty(py);
        for val in results {
            py_list.append(redis_value_to_py(py, val)?)?;
        }
        Ok(py_list.into_pyobject(py)?.unbind().into_any())
    }
}

#[pyclass(name = "AsyncRedisPipeline", module = "vertebra._redis", immutable_type)]
pub struct AsyncRedisPipeline {
    client: Arc<RedisCoreClient>,
    commands: Vec<Vec<Vec<u8>>>,
    atomic: bool,
}

impl AsyncRedisPipeline {
    pub fn new(client: Arc<RedisCoreClient>, atomic: bool) -> Self {
        Self {
            client,
            commands: Vec::new(),
            atomic,
        }
    }
}

#[pymethods]
impl AsyncRedisPipeline {
    pub fn execute_command(mut slf: PyRefMut<'_, Self>, args: Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        let cmd_args = py_seq_to_redis_args(&args)?;
        slf.commands.push(cmd_args);
        Ok(slf.into())
    }

    pub fn results(&mut self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let cmds = std::mem::take(&mut self.commands);
        let client = Arc::clone(&self.client);
        let atomic = self.atomic;

        spawn_rust_db_task_with_attr(
            py,
            async move { client.execute_pipeline(&cmds, atomic).await },
            |py, vals| {
                let py_list = PyList::empty(py);
                for val in vals {
                    py_list.append(redis_value_to_py(py, val)?)?;
                }
                Ok(py_list.into_pyobject(py)?.unbind().into_any())
            },
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn submit_results(&mut self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.results(py)
    }
}

// --- Sync Client ---

#[pyclass(name = "SyncRedisClient", module = "vertebra._redis", immutable_type)]
pub struct SyncRedisClient {
    client: Arc<RedisCoreClient>,
}

#[pymethods]
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
        let mut cfg = RedisClientConfig::new(url).map_err(|e| e.into_pyerr(py))?;
        cfg.username = username;
        cfg.password = password;
        cfg.client_name = client_name;
        cfg.protocol = protocol;
        cfg.topology = RedisTopologyMode::parse(topology).map_err(|e| e.into_pyerr(py))?;
        cfg.sentinel_master_name = sentinel_master_name;
        cfg.sentinel_nodes = sentinel_nodes;
        cfg.cluster_nodes = cluster_nodes;
        cfg.max_redirections = max_redirections;
        cfg.enable_client_side_caching = enable_client_side_caching;
        cfg.client_tracking_prefixes = client_tracking_prefixes;
        cfg.client_cache_capacity = client_cache_capacity;
        cfg.client_cache_ttl_seconds = client_cache_ttl_seconds;
        cfg.enable_gcp_iam_auth = enable_gcp_iam_auth;
        cfg.max_connections = max_connections;

        if let Some(capsule) = token_cache_capsule {
            cfg.iam_token_handle = Some(RedisClientConfig::parse_token_cache_capsule(capsule).map_err(|e| e.into_pyerr(py))?);
        }

        let client = RedisCoreClient::new(cfg).map_err(|e| e.into_pyerr(py))?;
        Ok(Self { client })
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.client.is_closed()
    }

    pub fn connect(&self, py: Python<'_>) -> PyResult<()> {
        let client = Arc::clone(&self.client);
        py.detach(|| {
            block_on_db(async move {
                client.ensure_connected().await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    pub fn close(&self) {
        self.client.close();
    }

    #[pyo3(signature = (password = None))]
    pub fn reconnect(&self, py: Python<'_>, password: Option<&str>) -> PyResult<()> {
        let client = Arc::clone(&self.client);
        let pw = password.map(str::to_string);
        py.detach(|| {
            block_on_db(async move {
                client.reconnect(pw.as_deref()).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    pub fn get(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
        let key_bytes = py_to_redis_arg(&key)?;
        let client = Arc::clone(&self.client);
        let res = py.detach(|| {
            block_on_db(async move {
                client.get(&key_bytes).await
            })
        }).map_err(|e| e.into_pyerr(py))?;

        match res {
            Some(b) => Ok(Some(PyBytes::new(py, &b).into_pyobject(py)?.unbind().into_any())),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (key, value, ex = None, px = None, nx = false, xx = false))]
    pub fn set(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        value: Bound<'_, PyAny>,
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
    ) -> PyResult<bool> {
        let key_bytes = py_to_redis_arg(&key)?;
        let val_bytes = py_to_redis_arg(&value)?;
        let client = Arc::clone(&self.client);

        py.detach(|| {
            block_on_db(async move {
                client.set(&key_bytes, &val_bytes, ex, px, nx, xx).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    pub fn delete(&self, py: Python<'_>, keys: Bound<'_, PyAny>) -> PyResult<i64> {
        let key_list = py_seq_to_redis_args(&keys)?;
        let client = Arc::clone(&self.client);
        py.detach(|| {
            block_on_db(async move {
                client.delete(&key_list).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    pub fn unlink(&self, py: Python<'_>, keys: Bound<'_, PyAny>) -> PyResult<i64> {
        let key_list = py_seq_to_redis_args(&keys)?;
        let client = Arc::clone(&self.client);
        py.detach(|| {
            block_on_db(async move {
                client.unlink(&key_list).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    pub fn pttl(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<i64> {
        let key_bytes = py_to_redis_arg(&key)?;
        let client = Arc::clone(&self.client);
        py.detach(|| {
            block_on_db(async move {
                client.pttl(&key_bytes).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    #[pyo3(signature = (cursor = 0, match_pattern = None, count = None))]
    pub fn scan(
        &self,
        py: Python<'_>,
        cursor: u64,
        match_pattern: Option<&str>,
        count: Option<usize>,
    ) -> PyResult<(u64, Vec<Py<PyAny>>)> {
        let client = Arc::clone(&self.client);
        let pattern_string = match_pattern.map(str::to_string);
        let (next_cursor, keys) = py.detach(|| {
            block_on_db(async move {
                client.scan(cursor, pattern_string.as_deref(), count).await
            })
        }).map_err(|e| e.into_pyerr(py))?;

        let mut py_keys = Vec::with_capacity(keys.len());
        for k in keys {
            py_keys.push(PyBytes::new(py, &k).into_pyobject(py)?.unbind().into_any());
        }
        Ok((next_cursor, py_keys))
    }

    pub fn flushdb(&self, py: Python<'_>) -> PyResult<()> {
        let client = Arc::clone(&self.client);
        py.detach(|| {
            block_on_db(async move {
                client.flushdb().await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    pub fn publish(&self, py: Python<'_>, channel: &str, message: Bound<'_, PyAny>) -> PyResult<i64> {
        let msg_bytes = py_to_redis_arg(&message)?;
        let ch = channel.to_string();
        let client = Arc::clone(&self.client);
        py.detach(|| {
            block_on_db(async move {
                client.publish(&ch, &msg_bytes).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    pub fn spublish(&self, py: Python<'_>, shard_channel: &str, message: Bound<'_, PyAny>) -> PyResult<i64> {
        let msg_bytes = py_to_redis_arg(&message)?;
        let ch = shard_channel.to_string();
        let client = Arc::clone(&self.client);
        py.detach(|| {
            block_on_db(async move {
                client.spublish(&ch, &msg_bytes).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    pub fn execute_command(&self, py: Python<'_>, args: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let cmd_args = py_seq_to_redis_args(&args)?;
        let client = Arc::clone(&self.client);
        let val = py.detach(|| {
            block_on_db(async move {
                client.execute_command(&cmd_args).await
            })
        }).map_err(|e| e.into_pyerr(py))?;
        redis_value_to_py(py, val)
    }

    #[pyo3(signature = (atomic = false))]
    pub fn pipeline(&self, atomic: bool) -> SyncRedisPipeline {
        SyncRedisPipeline::new(Arc::clone(&self.client), atomic)
    }

    pub fn eval_script(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Bound<'_, PyAny>,
        args: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let key_list = py_seq_to_redis_args(&keys)?;
        let arg_list = py_seq_to_redis_args(&args)?;
        let client = Arc::clone(&self.client);
        let s = script.to_string();

        let val = py.detach(|| {
            block_on_db(async move {
                client.eval_script(&s, &key_list, &arg_list).await
            })
        }).map_err(|e| e.into_pyerr(py))?;
        redis_value_to_py(py, val)
    }

    pub fn create_listener(&self, py: Python<'_>) -> PyResult<SyncRedisListener> {
        SyncRedisListener::new(self.client.config.clone()).map_err(|e| e.into_pyerr(py))
    }

    pub fn listener(&self, py: Python<'_>) -> PyResult<SyncRedisListener> {
        self.create_listener(py)
    }

    pub fn cached_keys(&self) -> Vec<String> {
        self.client.cached_keys()
    }

    pub fn drain_invalidations(&self) -> Vec<String> {
        self.client.drain_invalidations()
    }

    // --- Stream Methods ---

    #[pyo3(signature = (stream, group, id = "$", mkstream = false))]
    pub fn xgroup_create(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> PyResult<bool> {
        let client = Arc::clone(&self.client);
        let s = stream.to_string();
        let g = group.to_string();
        let i = id.to_string();
        py.detach(|| {
            block_on_db(async move {
                streams::xgroup_create(&client, &s, &g, &i, mkstream).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    #[pyo3(signature = (stream, fields, id = "*", maxlen = None, approximate = true))]
    pub fn xadd(
        &self,
        py: Python<'_>,
        stream: &str,
        fields: Bound<'_, PyAny>,
        id: &str,
        maxlen: Option<usize>,
        approximate: bool,
    ) -> PyResult<Py<PyAny>> {
        let parsed_fields = parse_fields(&fields)?;
        let client = Arc::clone(&self.client);
        let s = stream.to_string();
        let i = id.to_string();

        let res = py.detach(|| {
            block_on_db(async move {
                streams::xadd(&client, &s, &parsed_fields, &i, maxlen, approximate).await
            })
        }).map_err(|e| e.into_pyerr(py))?;
        Ok(PyBytes::new(py, &res).into_pyobject(py)?.unbind().into_any())
    }

    #[pyo3(signature = (group, consumer, streams_obj, count = None, block_ms = None))]
    pub fn xreadgroup(
        &self,
        py: Python<'_>,
        group: &str,
        consumer: &str,
        streams_obj: Bound<'_, PyAny>,
        count: Option<usize>,
        block_ms: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        let parsed_streams = parse_streams_arg(&streams_obj)?;
        let client = Arc::clone(&self.client);
        let g = group.to_string();
        let c = consumer.to_string();

        let val = py.detach(|| {
            block_on_db(async move {
                streams::xreadgroup(&client, &g, &c, &parsed_streams, count, block_ms).await
            })
        }).map_err(|e| e.into_pyerr(py))?;
        redis_value_to_py(py, val)
    }

    pub fn xack(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        ids: Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let id_list = py_seq_to_redis_args(&ids)?;
        let client = Arc::clone(&self.client);
        let s = stream.to_string();
        let g = group.to_string();

        py.detach(|| {
            block_on_db(async move {
                streams::xack(&client, &s, &g, &id_list).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    #[pyo3(signature = (stream, group, consumer, min_idle_ms, start_id = "0-0", count = None))]
    pub fn xautoclaim(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: usize,
        start_id: &str,
        count: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        let client = Arc::clone(&self.client);
        let s = stream.to_string();
        let g = group.to_string();
        let c = consumer.to_string();
        let sid = start_id.to_string();

        let val = py.detach(|| {
            block_on_db(async move {
                streams::xautoclaim(&client, &s, &g, &c, min_idle_ms, &sid, count).await
            })
        }).map_err(|e| e.into_pyerr(py))?;
        redis_value_to_py(py, val)
    }

    #[pyo3(signature = (stream, start = "-", end = "+", count = None))]
    pub fn xrange(
        &self,
        py: Python<'_>,
        stream: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        let client = Arc::clone(&self.client);
        let s = stream.to_string();
        let st = start.to_string();
        let ed = end.to_string();

        let val = py.detach(|| {
            block_on_db(async move {
                streams::xrange(&client, &s, &st, &ed, count).await
            })
        }).map_err(|e| e.into_pyerr(py))?;
        redis_value_to_py(py, val)
    }

    pub fn xdel(&self, py: Python<'_>, stream: &str, ids: Bound<'_, PyAny>) -> PyResult<i64> {
        let id_list = py_seq_to_redis_args(&ids)?;
        let client = Arc::clone(&self.client);
        let s = stream.to_string();

        py.detach(|| {
            block_on_db(async move {
                streams::xdel(&client, &s, &id_list).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    pub fn zadd(&self, py: Python<'_>, key: &str, mapping: Bound<'_, PyAny>) -> PyResult<i64> {
        let parsed = parse_zadd_mapping(&mapping)?;
        let client = Arc::clone(&self.client);
        let k = key.to_string();

        py.detach(|| {
            block_on_db(async move {
                streams::zadd(&client, &k, &parsed).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }

    #[pyo3(signature = (key, min, max, withscores = false, offset = None, count = None))]
    pub fn zrangebyscore(
        &self,
        py: Python<'_>,
        key: &str,
        min: &str,
        max: &str,
        withscores: bool,
        offset: Option<usize>,
        count: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        let client = Arc::clone(&self.client);
        let k = key.to_string();
        let mn = min.to_string();
        let mx = max.to_string();

        let val = py.detach(|| {
            block_on_db(async move {
                streams::zrangebyscore(&client, &k, &mn, &mx, withscores, offset, count).await
            })
        }).map_err(|e| e.into_pyerr(py))?;
        redis_value_to_py(py, val)
    }

    pub fn zrem(&self, py: Python<'_>, key: &str, members: Bound<'_, PyAny>) -> PyResult<i64> {
        let mem_list = py_seq_to_redis_args(&members)?;
        let client = Arc::clone(&self.client);
        let k = key.to_string();

        py.detach(|| {
            block_on_db(async move {
                streams::zrem(&client, &k, &mem_list).await
            })
        }).map_err(|e| e.into_pyerr(py))
    }
}

// --- Async Client ---

#[pyclass(name = "AsyncRedisClient", module = "vertebra._redis", immutable_type)]
pub struct AsyncRedisClient {
    client: Arc<RedisCoreClient>,
}

#[pymethods]
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
        let mut cfg = RedisClientConfig::new(url).map_err(|e| e.into_pyerr(py))?;
        cfg.username = username;
        cfg.password = password;
        cfg.client_name = client_name;
        cfg.protocol = protocol;
        cfg.topology = RedisTopologyMode::parse(topology).map_err(|e| e.into_pyerr(py))?;
        cfg.sentinel_master_name = sentinel_master_name;
        cfg.sentinel_nodes = sentinel_nodes;
        cfg.cluster_nodes = cluster_nodes;
        cfg.max_redirections = max_redirections;
        cfg.enable_client_side_caching = enable_client_side_caching;
        cfg.client_tracking_prefixes = client_tracking_prefixes;
        cfg.client_cache_capacity = client_cache_capacity;
        cfg.client_cache_ttl_seconds = client_cache_ttl_seconds;
        cfg.enable_gcp_iam_auth = enable_gcp_iam_auth;
        cfg.max_connections = max_connections;

        if let Some(capsule) = token_cache_capsule {
            cfg.iam_token_handle = Some(RedisClientConfig::parse_token_cache_capsule(capsule).map_err(|e| e.into_pyerr(py))?);
        }

        let client = RedisCoreClient::new(cfg).map_err(|e| e.into_pyerr(py))?;
        Ok(Self { client })
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.client.is_closed()
    }

    pub fn connect(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.client);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.ensure_connected().await },
            |py, ()| Ok(py.None()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn close(&self) {
        self.client.close();
    }

    #[pyo3(signature = (password = None))]
    pub fn reconnect(
        &self,
        py: Python<'_>,
        password: Option<&str>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.client);
        let pw = password.map(str::to_string);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.reconnect(pw.as_deref()).await },
            |py, ()| Ok(py.None()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn get(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let key_bytes = py_to_redis_arg(&key)?;
        let client = Arc::clone(&self.client);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.get(&key_bytes).await },
            |py, res| match res {
                Some(b) => Ok(PyBytes::new(py, &b).into_pyobject(py)?.unbind().into_any()),
                None => Ok(py.None()),
            },
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (key, value, ex = None, px = None, nx = false, xx = false))]
    pub fn set(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        value: Bound<'_, PyAny>,
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let key_bytes = py_to_redis_arg(&key)?;
        let val_bytes = py_to_redis_arg(&value)?;
        let client = Arc::clone(&self.client);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.set(&key_bytes, &val_bytes, ex, px, nx, xx).await },
            |py, success| Ok(pyo3::types::PyBool::new(py, success).to_owned().unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn delete(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let key_list = py_seq_to_redis_args(&keys)?;
        let client = Arc::clone(&self.client);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.delete(&key_list).await },
            |py, count| Ok(count.into_pyobject(py)?.unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn unlink(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let key_list = py_seq_to_redis_args(&keys)?;
        let client = Arc::clone(&self.client);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.unlink(&key_list).await },
            |py, count| Ok(count.into_pyobject(py)?.unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn pttl(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let key_bytes = py_to_redis_arg(&key)?;
        let client = Arc::clone(&self.client);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.pttl(&key_bytes).await },
            |py, ttl| Ok(ttl.into_pyobject(py)?.unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (cursor = 0, match_pattern = None, count = None))]
    pub fn scan(
        &self,
        py: Python<'_>,
        cursor: u64,
        match_pattern: Option<&str>,
        count: Option<usize>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.client);
        let pattern_string = match_pattern.map(str::to_string);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.scan(cursor, pattern_string.as_deref(), count).await },
            |py, (next_cursor, keys)| {
                let py_keys = PyList::empty(py);
                for k in keys {
                    py_keys.append(PyBytes::new(py, &k))?;
                }
                let tuple = PyTuple::new(py, [next_cursor.into_pyobject(py)?.into_any(), py_keys.into_any()])?;
                Ok(tuple.into_pyobject(py)?.unbind().into_any())
            },
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn flushdb(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.client);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.flushdb().await },
            |py, ()| Ok(py.None()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn publish(
        &self,
        py: Python<'_>,
        channel: &str,
        message: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let msg_bytes = py_to_redis_arg(&message)?;
        let ch = channel.to_string();
        let client = Arc::clone(&self.client);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.publish(&ch, &msg_bytes).await },
            |py, count| Ok(count.into_pyobject(py)?.unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn spublish(
        &self,
        py: Python<'_>,
        shard_channel: &str,
        message: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let msg_bytes = py_to_redis_arg(&message)?;
        let ch = shard_channel.to_string();
        let client = Arc::clone(&self.client);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.spublish(&ch, &msg_bytes).await },
            |py, count| Ok(count.into_pyobject(py)?.unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn execute_command(
        &self,
        py: Python<'_>,
        args: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let cmd_args = py_seq_to_redis_args(&args)?;
        let client = Arc::clone(&self.client);
        spawn_rust_db_task_with_attr(
            py,
            async move { client.execute_command(&cmd_args).await },
            |py, val| redis_value_to_py(py, val),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (atomic = false))]
    pub fn pipeline(&self, atomic: bool) -> AsyncRedisPipeline {
        AsyncRedisPipeline::new(Arc::clone(&self.client), atomic)
    }

    pub fn eval_script(
        &self,
        py: Python<'_>,
        script: &str,
        keys: Bound<'_, PyAny>,
        args: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let key_list = py_seq_to_redis_args(&keys)?;
        let arg_list = py_seq_to_redis_args(&args)?;
        let client = Arc::clone(&self.client);
        let s = script.to_string();
        spawn_rust_db_task_with_attr(
            py,
            async move { client.eval_script(&s, &key_list, &arg_list).await },
            |py, val| redis_value_to_py(py, val),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn create_listener(&self, py: Python<'_>) -> PyResult<AsyncRedisListener> {
        AsyncRedisListener::new(self.client.config.clone()).map_err(|e| e.into_pyerr(py))
    }

    pub fn listener(&self, py: Python<'_>) -> PyResult<AsyncRedisListener> {
        self.create_listener(py)
    }

    pub fn cached_keys(&self) -> Vec<String> {
        self.client.cached_keys()
    }

    pub fn drain_invalidations(&self) -> Vec<String> {
        self.client.drain_invalidations()
    }

    // --- Async Stream Methods ---

    #[pyo3(signature = (stream, group, id = "$", mkstream = false))]
    pub fn xgroup_create(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.client);
        let s = stream.to_string();
        let g = group.to_string();
        let i = id.to_string();
        spawn_rust_db_task_with_attr(
            py,
            async move { streams::xgroup_create(&client, &s, &g, &i, mkstream).await },
            |py, created| Ok(pyo3::types::PyBool::new(py, created).to_owned().unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (stream, fields, id = "*", maxlen = None, approximate = true))]
    pub fn xadd(
        &self,
        py: Python<'_>,
        stream: &str,
        fields: Bound<'_, PyAny>,
        id: &str,
        maxlen: Option<usize>,
        approximate: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let parsed_fields = parse_fields(&fields)?;
        let client = Arc::clone(&self.client);
        let s = stream.to_string();
        let i = id.to_string();

        spawn_rust_db_task_with_attr(
            py,
            async move { streams::xadd(&client, &s, &parsed_fields, &i, maxlen, approximate).await },
            |py, res| Ok(PyBytes::new(py, &res).into_pyobject(py)?.unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (group, consumer, streams_obj, count = None, block_ms = None))]
    pub fn xreadgroup(
        &self,
        py: Python<'_>,
        group: &str,
        consumer: &str,
        streams_obj: Bound<'_, PyAny>,
        count: Option<usize>,
        block_ms: Option<usize>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let parsed_streams = parse_streams_arg(&streams_obj)?;
        let client = Arc::clone(&self.client);
        let g = group.to_string();
        let c = consumer.to_string();

        spawn_rust_db_task_with_attr(
            py,
            async move { streams::xreadgroup(&client, &g, &c, &parsed_streams, count, block_ms).await },
            |py, val| redis_value_to_py(py, val),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn xack(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        ids: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let id_list = py_seq_to_redis_args(&ids)?;
        let client = Arc::clone(&self.client);
        let s = stream.to_string();
        let g = group.to_string();

        spawn_rust_db_task_with_attr(
            py,
            async move { streams::xack(&client, &s, &g, &id_list).await },
            |py, count| Ok(count.into_pyobject(py)?.unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (stream, group, consumer, min_idle_ms, start_id = "0-0", count = None))]
    pub fn xautoclaim(
        &self,
        py: Python<'_>,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: usize,
        start_id: &str,
        count: Option<usize>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.client);
        let s = stream.to_string();
        let g = group.to_string();
        let c = consumer.to_string();
        let sid = start_id.to_string();

        spawn_rust_db_task_with_attr(
            py,
            async move { streams::xautoclaim(&client, &s, &g, &c, min_idle_ms, &sid, count).await },
            |py, val| redis_value_to_py(py, val),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (stream, start = "-", end = "+", count = None))]
    pub fn xrange(
        &self,
        py: Python<'_>,
        stream: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.client);
        let s = stream.to_string();
        let st = start.to_string();
        let ed = end.to_string();

        spawn_rust_db_task_with_attr(
            py,
            async move { streams::xrange(&client, &s, &st, &ed, count).await },
            |py, val| redis_value_to_py(py, val),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn xdel(
        &self,
        py: Python<'_>,
        stream: &str,
        ids: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let id_list = py_seq_to_redis_args(&ids)?;
        let client = Arc::clone(&self.client);
        let s = stream.to_string();

        spawn_rust_db_task_with_attr(
            py,
            async move { streams::xdel(&client, &s, &id_list).await },
            |py, count| Ok(count.into_pyobject(py)?.unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn zadd(
        &self,
        py: Python<'_>,
        key: &str,
        mapping: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let parsed = parse_zadd_mapping(&mapping)?;
        let client = Arc::clone(&self.client);
        let k = key.to_string();

        spawn_rust_db_task_with_attr(
            py,
            async move { streams::zadd(&client, &k, &parsed).await },
            |py, count| Ok(count.into_pyobject(py)?.unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
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
        offset: Option<usize>,
        count: Option<usize>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let client = Arc::clone(&self.client);
        let k = key.to_string();
        let mn = min.to_string();
        let mx = max.to_string();

        spawn_rust_db_task_with_attr(
            py,
            async move { streams::zrangebyscore(&client, &k, &mn, &mx, withscores, offset, count).await },
            |py, val| redis_value_to_py(py, val),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn zrem(
        &self,
        py: Python<'_>,
        key: &str,
        members: Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let mem_list = py_seq_to_redis_args(&members)?;
        let client = Arc::clone(&self.client);
        let k = key.to_string();

        spawn_rust_db_task_with_attr(
            py,
            async move { streams::zrem(&client, &k, &mem_list).await },
            |py, count| Ok(count.into_pyobject(py)?.unbind().into_any()),
            redis_error_to_py,
            REDIS_COMPLETION_DRAINER_ATTR,
        )
    }
}
