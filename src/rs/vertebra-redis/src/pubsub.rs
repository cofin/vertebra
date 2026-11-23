//! PyO3 Redis Pub/Sub listener wrappers delegating to `vertebra_resp::pubsub::CommandDrivenListener`.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use vertebra_db::py_async::db_runtime_handle;
use vertebra_resp::config::RespConfig;
use vertebra_resp::pubsub::{CommandDrivenListener, PubSubMessageTuple, SUBSCRIBE_CONFIRM_TIMEOUT};

fn tuple_to_py<'py>(py: Python<'py>, msg: PubSubMessageTuple) -> PyResult<Bound<'py, PyTuple>> {
    let (kind, channel, payload, pattern) = msg;
    let payload_py = PyBytes::new(py, &payload);
    match pattern {
        Some(pat) => (kind, channel, payload_py, pat).into_pyobject(py),
        None => (kind, channel, payload_py, py.None()).into_pyobject(py),
    }
}

fn drain_to_py<'py>(
    py: Python<'py>,
    inner: &CommandDrivenListener,
) -> PyResult<Vec<Bound<'py, PyTuple>>> {
    let mut out = Vec::new();
    for msg in inner.drain_messages() {
        out.push(tuple_to_py(py, msg)?);
    }
    Ok(out)
}

fn wait_op(inner: &CommandDrivenListener, py: Python<'_>, op_id: usize) -> PyResult<()> {
    py.detach(|| inner.wait_completion(op_id, SUBSCRIBE_CONFIRM_TIMEOUT))
        .map_err(|e| e.to_py_err(py))
}

fn next_msg<'py>(
    py: Python<'py>,
    inner: &CommandDrivenListener,
    timeout_ms: Option<u64>,
) -> PyResult<Option<Bound<'py, PyTuple>>> {
    let msg = py.detach(|| inner.next_message_blocking(timeout_ms));
    msg.map(|m| tuple_to_py(py, m)).transpose()
}

/// Synchronous Redis Pub/Sub listener (`SyncRedisClient.listener()`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        name = "SyncRedisListener",
        module = "vertebra._redis",
        frozen,
        immutable_type,
        freelist = 128
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(
        name = "SyncRedisListener",
        module = "vertebra._redis",
        frozen,
        immutable_type
    )
)]
pub struct SyncRedisListener {
    inner: CommandDrivenListener,
}

impl SyncRedisListener {
    #[must_use]
    pub fn from_config(config: Arc<RespConfig>) -> Self {
        let handle = db_runtime_handle();
        Self {
            inner: CommandDrivenListener::new(config, false, handle),
        }
    }
}

#[pymethods]
impl SyncRedisListener {
    #[getter]
    pub fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn subscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_subscribe(channels);
        wait_op(&self.inner, py, op_id)
    }

    pub fn psubscribe(&self, py: Python<'_>, patterns: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_psubscribe(patterns);
        wait_op(&self.inner, py, op_id)
    }

    pub fn ssubscribe(&self, py: Python<'_>, shard_channels: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_ssubscribe(shard_channels);
        wait_op(&self.inner, py, op_id)
    }

    #[pyo3(signature = (channels = Vec::new()))]
    pub fn unsubscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_unsubscribe(channels);
        wait_op(&self.inner, py, op_id)
    }

    #[pyo3(signature = (patterns = Vec::new()))]
    pub fn punsubscribe(&self, py: Python<'_>, patterns: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_punsubscribe(patterns);
        wait_op(&self.inner, py, op_id)
    }

    #[pyo3(signature = (shard_channels = Vec::new()))]
    pub fn sunsubscribe(&self, py: Python<'_>, shard_channels: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_sunsubscribe(shard_channels);
        wait_op(&self.inner, py, op_id)
    }

    #[pyo3(signature = (timeout_ms = None))]
    pub fn next_message<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        next_msg(py, &self.inner, timeout_ms)
    }

    pub fn drain_messages<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        drain_to_py(py, &self.inner)
    }

    pub fn close(&self) {
        self.inner.close();
    }
}

/// Asynchronous command-driven Redis Pub/Sub listener (`AsyncRedisClient.listener()`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        name = "AsyncRedisListener",
        module = "vertebra._redis",
        frozen,
        immutable_type,
        freelist = 128
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(
        name = "AsyncRedisListener",
        module = "vertebra._redis",
        frozen,
        immutable_type
    )
)]
pub struct AsyncRedisListener {
    inner: CommandDrivenListener,
}

impl AsyncRedisListener {
    #[must_use]
    pub fn from_config(config: Arc<RespConfig>) -> Self {
        let handle = db_runtime_handle();
        Self {
            inner: CommandDrivenListener::new(config, true, handle),
        }
    }
}

#[pymethods]
impl AsyncRedisListener {
    #[getter]
    pub fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn submit_subscribe(&self, channels: Vec<String>) -> usize {
        self.inner.submit_subscribe(channels)
    }

    pub fn submit_psubscribe(&self, patterns: Vec<String>) -> usize {
        self.inner.submit_psubscribe(patterns)
    }

    pub fn submit_ssubscribe(&self, shard_channels: Vec<String>) -> usize {
        self.inner.submit_ssubscribe(shard_channels)
    }

    #[pyo3(signature = (channels = Vec::new()))]
    pub fn submit_unsubscribe(&self, channels: Vec<String>) -> usize {
        self.inner.submit_unsubscribe(channels)
    }

    #[pyo3(signature = (patterns = Vec::new()))]
    pub fn submit_punsubscribe(&self, patterns: Vec<String>) -> usize {
        self.inner.submit_punsubscribe(patterns)
    }

    #[pyo3(signature = (shard_channels = Vec::new()))]
    pub fn submit_sunsubscribe(&self, shard_channels: Vec<String>) -> usize {
        self.inner.submit_sunsubscribe(shard_channels)
    }

    pub fn subscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_subscribe(channels);
        wait_op(&self.inner, py, op_id)
    }

    pub fn psubscribe(&self, py: Python<'_>, patterns: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_psubscribe(patterns);
        wait_op(&self.inner, py, op_id)
    }

    pub fn ssubscribe(&self, py: Python<'_>, shard_channels: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_ssubscribe(shard_channels);
        wait_op(&self.inner, py, op_id)
    }

    #[pyo3(signature = (channels = Vec::new()))]
    pub fn unsubscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_unsubscribe(channels);
        wait_op(&self.inner, py, op_id)
    }

    #[pyo3(signature = (patterns = Vec::new()))]
    pub fn punsubscribe(&self, py: Python<'_>, patterns: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_punsubscribe(patterns);
        wait_op(&self.inner, py, op_id)
    }

    #[pyo3(signature = (shard_channels = Vec::new()))]
    pub fn sunsubscribe(&self, py: Python<'_>, shard_channels: Vec<String>) -> PyResult<()> {
        let op_id = self.inner.submit_sunsubscribe(shard_channels);
        wait_op(&self.inner, py, op_id)
    }

    #[pyo3(signature = (timeout_ms = None))]
    pub fn next_message<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        next_msg(py, &self.inner, timeout_ms)
    }

    pub fn drain_messages<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        drain_to_py(py, &self.inner)
    }

    pub fn drain_completions(&self) -> Vec<(usize, bool, Option<String>)> {
        self.inner.drain_completions()
    }

    pub fn cancel_operation(&self, op_id: usize) -> bool {
        self.inner.cancel_operation(op_id)
    }

    pub fn notifier_fd(&self) -> i32 {
        self.inner.notifier_fd()
    }

    pub fn close(&self) {
        self.inner.close();
    }
}
