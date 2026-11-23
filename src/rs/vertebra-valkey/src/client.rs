//! PyO3 client and pipeline bindings delegating to `vertebra_resp::client`.

use std::sync::Arc;

use bytes::Bytes;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList, PyTuple};
use tokio::sync::Notify;
use vertebra_db::py_async::{
    RustDbTaskAwaitable, block_on_db, spawn_rust_db_task_with_attr_and_cancel,
};
use vertebra_resp::client::py_helpers::{
    PipelineBuilder, decode_hgetall_py, decode_pipeline_py, extract_bytes_arg, extract_bytes_list,
    extract_command_arg, extract_hset_pairs, extract_pairs_bytes, extract_streams_arg,
    extract_varargs_bytes, extract_zadd_members, parse_optional_keys, slot_map_to_py_dict,
};
use vertebra_resp::codec::{
    RespValue, build_hash_expire_cmd, build_hash_field_query_cmd, decode_int_list, resp_to_py,
};
use vertebra_resp::config::{ValkeyConfigInner, await_pending_python_token};
use vertebra_resp::error::ValkeyError;
use vertebra_resp::streams::{
    build_xack, build_xadd, build_xautoclaim, build_xdel, build_xgroup_create, build_xrange,
    build_xreadgroup, build_zadd, decode_xautoclaim_py, decode_xrange_py, decode_xreadgroup_py,
};

pub use vertebra_resp::client::{
    PipelineDecoder, QueuedCommand, RespEngine, ValkeyEngine, build_hset_cmd,
    build_script_exec_cmd, build_set_cmd, classify_generic_command,
};

use crate::VALKEY_COMPLETION_DRAINER_ATTR;
use crate::config::ValkeyClientConfig;
use crate::pubsub::{AsyncValkeyListener, SyncValkeyListener};

fn exec_sync_valkey<F, T>(py: Python<'_>, engine: &Arc<ValkeyEngine>, fut: F) -> PyResult<T>
where
    F: std::future::Future<Output = Result<T, ValkeyError>> + Send,
    T: Send,
{
    engine
        .effective_config()
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
        self.engine.effective_config().protocol_resp3
    }

    #[getter]
    pub fn tracking_enabled(&self) -> bool {
        self.engine.effective_config().client_tracking_enabled
    }

    #[getter]
    pub fn cluster_enabled(&self) -> bool {
        self.engine.effective_config().cluster_enabled
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
        _py: Python<'_>,
        keys: Option<Bound<'_, PyAny>>,
    ) -> PyResult<usize> {
        let parsed = parse_optional_keys(keys)?;
        Ok(self.engine.invalidate_keys(parsed.as_deref()))
    }

    pub fn slot_map(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        slot_map_to_py_dict(py, self.engine.slot_map())
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
        self.engine.effective_config().protocol_resp3
    }

    #[getter]
    pub fn tracking_enabled(&self) -> bool {
        self.engine.effective_config().client_tracking_enabled
    }

    #[getter]
    pub fn cluster_enabled(&self) -> bool {
        self.engine.effective_config().cluster_enabled
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
        _py: Python<'_>,
        keys: Option<Bound<'_, PyAny>>,
    ) -> PyResult<usize> {
        let parsed = parse_optional_keys(keys)?;
        Ok(self.engine.invalidate_keys(parsed.as_deref()))
    }

    pub fn slot_map(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        slot_map_to_py_dict(py, self.engine.slot_map())
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

macro_rules! define_valkey_pipeline {
    ($name:ident, { $($tail:tt)* }) => {
        #[cfg_attr(
            not(Py_GIL_DISABLED),
            pyclass(frozen, immutable_type, freelist = 128, module = "vertebra._valkey")
        )]
        #[cfg_attr(
            Py_GIL_DISABLED,
            pyclass(frozen, immutable_type, module = "vertebra._valkey")
        )]
        pub struct $name {
            inner: PipelineBuilder,
        }

        impl $name {
            fn from_engine(engine: Arc<ValkeyEngine>, transaction: bool) -> Self {
                Self {
                    inner: PipelineBuilder::new(engine, transaction),
                }
            }
        }

        #[pymethods]
        #[allow(clippy::too_many_arguments)]
        impl $name {
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


            $($tail)*
        }
    };
}

define_valkey_pipeline!(SyncValkeyPipeline, {
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
});

define_valkey_pipeline!(AsyncValkeyPipeline, {
    pub fn execute<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let queued = self.inner.drain_all();
        let engine = Arc::clone(&self.inner.engine);
        let transaction = self.inner.transaction;
        spawn_valkey_cmd_task(
            py,
            &self.inner.engine.effective_config(),
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
});
