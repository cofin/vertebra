//! Dedicated Pub/Sub connection, subscription management, and push message delivery.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString, PyTuple};
use vertebra_core::notify::Notifier;

use crate::config::RedisClientConfig;
use crate::errors::RedisDriverError;

pub type PubSubMessageTuple = (String, String, Vec<u8>, Option<String>);

enum PubSubCommand {
    Subscribe {
        channels: Vec<String>,
        op_id: Option<usize>,
    },
    Psubscribe {
        patterns: Vec<String>,
        op_id: Option<usize>,
    },
    Ssubscribe {
        shard_channels: Vec<String>,
        op_id: Option<usize>,
    },
    Unsubscribe {
        channels: Vec<String>,
        op_id: Option<usize>,
    },
    Punsubscribe {
        patterns: Vec<String>,
        op_id: Option<usize>,
    },
    Sunsubscribe {
        shard_channels: Vec<String>,
        op_id: Option<usize>,
    },
    Close,
}

struct ListenerState {
    messages: Mutex<VecDeque<PubSubMessageTuple>>,
    msg_condvar: Condvar,
    completions: Mutex<VecDeque<(usize, bool, Option<String>)>>,
    notifier: Option<Notifier>,
    next_op_id: AtomicUsize,
    closed: AtomicBool,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<PubSubCommand>,
}

impl ListenerState {
    fn push_message(&self, msg: PubSubMessageTuple) {
        let mut queue = self.messages.lock();
        queue.push_back(msg);
        self.msg_condvar.notify_one();
        if let Some(ref notif) = self.notifier {
            let _ = notif.notify();
        }
    }

    fn push_completion(&self, op_id: usize, success: bool, err_msg: Option<String>) {
        let mut queue = self.completions.lock();
        queue.push_back((op_id, success, err_msg));
        self.msg_condvar.notify_all();
        if let Some(ref notif) = self.notifier {
            let _ = notif.notify();
        }
    }
}

async fn run_pubsub_loop(
    config: RedisClientConfig,
    state: Arc<ListenerState>,
    mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<PubSubCommand>,
) {
    if config.uses_tls() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    let mut conn_info =
        match redis::IntoConnectionInfo::into_connection_info(config.normalized_url.as_str()) {
            Ok(info) => info,
            Err(e) => {
                state.push_completion(0, false, Some(e.to_string()));
                state.closed.store(true, Ordering::Release);
                return;
            }
        };
    let redis_info = conn_info
        .redis_settings()
        .clone()
        .set_protocol(redis::ProtocolVersion::RESP3);
    conn_info = conn_info.set_redis_settings(redis_info);

    let client = match redis::Client::open(conn_info) {
        Ok(c) => c,
        Err(e) => {
            state.push_completion(0, false, Some(e.to_string()));
            state.closed.store(true, Ordering::Release);
            return;
        }
    };

    let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel::<redis::PushInfo>();
    let conn_config = redis::AsyncConnectionConfig::new().set_push_sender(push_tx);

    let mut con = match client
        .get_multiplexed_async_connection_with_config(&conn_config)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            state.push_completion(0, false, Some(e.to_string()));
            state.closed.store(true, Ordering::Release);
            return;
        }
    };

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(PubSubCommand::Subscribe { channels, op_id }) => {
                        let res = con.subscribe(&channels).await;
                        if let Some(id) = op_id {
                            state.push_completion(id, res.is_ok(), res.err().map(|e| e.to_string()));
                        }
                    }
                    Some(PubSubCommand::Psubscribe { patterns, op_id }) => {
                        let res = con.psubscribe(&patterns).await;
                        if let Some(id) = op_id {
                            state.push_completion(id, res.is_ok(), res.err().map(|e| e.to_string()));
                        }
                    }
                    Some(PubSubCommand::Ssubscribe { shard_channels, op_id }) => {
                        let res = redis::cmd("SSUBSCRIBE").arg(&shard_channels).query_async::<redis::Value>(&mut con).await;
                        if let Some(id) = op_id {
                            state.push_completion(id, res.is_ok(), res.err().map(|e| e.to_string()));
                        }
                    }
                    Some(PubSubCommand::Unsubscribe { channels, op_id }) => {
                        let res = if channels.is_empty() {
                            con.unsubscribe(&[] as &[&str]).await
                        } else {
                            con.unsubscribe(&channels).await
                        };
                        if let Some(id) = op_id {
                            state.push_completion(id, res.is_ok(), res.err().map(|e| e.to_string()));
                        }
                    }
                    Some(PubSubCommand::Punsubscribe { patterns, op_id }) => {
                        let res = if patterns.is_empty() {
                            con.punsubscribe(&[] as &[&str]).await
                        } else {
                            con.punsubscribe(&patterns).await
                        };
                        if let Some(id) = op_id {
                            state.push_completion(id, res.is_ok(), res.err().map(|e| e.to_string()));
                        }
                    }
                    Some(PubSubCommand::Sunsubscribe { shard_channels, op_id }) => {
                        let mut c = redis::cmd("SUNSUBSCRIBE");
                        if !shard_channels.is_empty() {
                            c.arg(&shard_channels);
                        }
                        let res = c.query_async::<redis::Value>(&mut con).await;
                        if let Some(id) = op_id {
                            state.push_completion(id, res.is_ok(), res.err().map(|e| e.to_string()));
                        }
                    }
                    Some(PubSubCommand::Close) | None => {
                        break;
                    }
                }
            }
            push = push_rx.recv() => {
                match push {
                    Some(push_info) => {
                        let kind_str = match push_info.kind {
                            redis::PushKind::Message => "message",
                            redis::PushKind::PMessage => "pmessage",
                            redis::PushKind::SMessage => "smessage",
                            _ => continue,
                        };
                        let mut iter = push_info.data.into_iter();
                        if kind_str == "message" || kind_str == "smessage" {
                            if let (Some(ch_val), Some(pl_val)) = (iter.next(), iter.next()) {
                                let channel = match ch_val {
                                    redis::Value::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
                                    redis::Value::SimpleString(s) => s,
                                    _ => String::new(),
                                };
                                let payload = match pl_val {
                                    redis::Value::BulkString(b) => b,
                                    redis::Value::SimpleString(s) => s.into_bytes(),
                                    _ => Vec::new(),
                                };
                                state.push_message((kind_str.to_string(), channel, payload, None));
                            }
                        } else if kind_str == "pmessage" {
                            if let (Some(pat_val), Some(ch_val), Some(pl_val)) = (iter.next(), iter.next(), iter.next()) {
                                let pattern = match pat_val {
                                    redis::Value::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
                                    redis::Value::SimpleString(s) => s,
                                    _ => String::new(),
                                };
                                let channel = match ch_val {
                                    redis::Value::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
                                    redis::Value::SimpleString(s) => s,
                                    _ => String::new(),
                                };
                                let payload = match pl_val {
                                    redis::Value::BulkString(b) => b,
                                    redis::Value::SimpleString(s) => s.into_bytes(),
                                    _ => Vec::new(),
                                };
                                state.push_message((kind_str.to_string(), channel, payload, Some(pattern)));
                            }
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }

    state.closed.store(true, Ordering::Release);
    state.msg_condvar.notify_all();
}

/// Synchronous dedicated Pub/Sub listener for Redis subscriptions.
#[pyclass(
    name = "SyncRedisListener",
    module = "vertebra._redis",
    frozen,
    immutable_type
)]
pub struct SyncRedisListener {
    state: Arc<ListenerState>,
}

impl SyncRedisListener {
    pub fn new(config: RedisClientConfig) -> Result<Self, RedisDriverError> {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let state = Arc::new(ListenerState {
            messages: Mutex::new(VecDeque::new()),
            msg_condvar: Condvar::new(),
            completions: Mutex::new(VecDeque::new()),
            notifier: None,
            next_op_id: AtomicUsize::new(1),
            closed: AtomicBool::new(false),
            cmd_tx,
        });

        let st_clone = Arc::clone(&state);
        vertebra_db::py_async::db_runtime_handle().spawn(async move {
            run_pubsub_loop(config, st_clone, cmd_rx).await;
        });

        Ok(Self { state })
    }

    fn wait_completion(&self, py: Python<'_>, op_id: usize) -> PyResult<()> {
        py.detach(|| {
            let mut guard = self.state.completions.lock();
            loop {
                if let Some(idx) = guard.iter().position(|(id, _, _)| *id == op_id) {
                    let (_, ok, err) = guard.remove(idx).unwrap();
                    if ok {
                        return Ok(());
                    } else {
                        return Err(RedisDriverError::operational(
                            err.as_deref().unwrap_or("Subscription failed"),
                            false,
                        ));
                    }
                }
                if self.state.closed.load(Ordering::Acquire) {
                    return Err(RedisDriverError::interface("Listener closed", false));
                }
                let res = self
                    .state
                    .msg_condvar
                    .wait_for(&mut guard, Duration::from_secs(5));
                if res.timed_out() {
                    return Err(RedisDriverError::operational(
                        "Subscription confirmation timed out",
                        true,
                    ));
                }
            }
        })
        .map_err(|e| e.into_pyerr(py))
    }
}

#[pymethods]
impl SyncRedisListener {
    #[getter]
    pub fn closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    pub fn subscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.state
            .cmd_tx
            .send(PubSubCommand::Subscribe {
                channels,
                op_id: Some(op_id),
            })
            .map_err(|_| {
                RedisDriverError::interface("Listener connection closed", false).into_pyerr(py)
            })?;
        self.wait_completion(py, op_id)
    }

    pub fn psubscribe(&self, py: Python<'_>, patterns: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.state
            .cmd_tx
            .send(PubSubCommand::Psubscribe {
                patterns,
                op_id: Some(op_id),
            })
            .map_err(|_| {
                RedisDriverError::interface("Listener connection closed", false).into_pyerr(py)
            })?;
        self.wait_completion(py, op_id)
    }

    pub fn ssubscribe(&self, py: Python<'_>, shard_channels: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.state
            .cmd_tx
            .send(PubSubCommand::Ssubscribe {
                shard_channels,
                op_id: Some(op_id),
            })
            .map_err(|_| {
                RedisDriverError::interface("Listener connection closed", false).into_pyerr(py)
            })?;
        self.wait_completion(py, op_id)
    }

    #[pyo3(signature = (channels = Vec::new()))]
    pub fn unsubscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.state
            .cmd_tx
            .send(PubSubCommand::Unsubscribe {
                channels,
                op_id: Some(op_id),
            })
            .map_err(|_| {
                RedisDriverError::interface("Listener connection closed", false).into_pyerr(py)
            })?;
        self.wait_completion(py, op_id)
    }

    #[pyo3(signature = (patterns = Vec::new()))]
    pub fn punsubscribe(&self, py: Python<'_>, patterns: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.state
            .cmd_tx
            .send(PubSubCommand::Punsubscribe {
                patterns,
                op_id: Some(op_id),
            })
            .map_err(|_| {
                RedisDriverError::interface("Listener connection closed", false).into_pyerr(py)
            })?;
        self.wait_completion(py, op_id)
    }

    #[pyo3(signature = (shard_channels = Vec::new()))]
    pub fn sunsubscribe(&self, py: Python<'_>, shard_channels: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.state
            .cmd_tx
            .send(PubSubCommand::Sunsubscribe {
                shard_channels,
                op_id: Some(op_id),
            })
            .map_err(|_| {
                RedisDriverError::interface("Listener connection closed", false).into_pyerr(py)
            })?;
        self.wait_completion(py, op_id)
    }

    #[pyo3(signature = (timeout_ms = None))]
    pub fn next_message<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let msg = py.detach(|| {
            let mut guard = self.state.messages.lock();
            if let Some(m) = guard.pop_front() {
                return Some(m);
            }
            if self.state.closed.load(Ordering::Acquire) {
                return None;
            }

            match timeout_ms {
                Some(ms) => {
                    let dur = Duration::from_millis(ms);
                    let result = self.state.msg_condvar.wait_for(&mut guard, dur);
                    if result.timed_out() {
                        None
                    } else {
                        guard.pop_front()
                    }
                }
                None => {
                    self.state.msg_condvar.wait(&mut guard);
                    guard.pop_front()
                }
            }
        });

        match msg {
            Some((kind, channel, payload, pattern)) => {
                let py_kind = PyString::new(py, &kind);
                let py_channel = PyString::new(py, &channel);
                let py_payload = PyBytes::new(py, &payload);
                let py_pattern = match pattern {
                    Some(p) => PyString::new(py, &p).into_any(),
                    None => py.None().into_bound(py),
                };
                Ok(Some(PyTuple::new(
                    py,
                    [
                        py_kind.into_any(),
                        py_channel.into_any(),
                        py_payload.into_any(),
                        py_pattern,
                    ],
                )?))
            }
            None => Ok(None),
        }
    }

    pub fn drain_messages<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let mut raw = self.state.messages.lock();
        let drained: Vec<PubSubMessageTuple> = raw.drain(..).collect();
        drop(raw);

        let mut res = Vec::with_capacity(drained.len());
        for (kind, channel, payload, pattern) in drained {
            let py_kind = PyString::new(py, &kind);
            let py_channel = PyString::new(py, &channel);
            let py_payload = PyBytes::new(py, &payload);
            let py_pattern = match pattern {
                Some(p) => PyString::new(py, &p).into_any(),
                None => py.None().into_bound(py),
            };
            res.push(PyTuple::new(
                py,
                [
                    py_kind.into_any(),
                    py_channel.into_any(),
                    py_payload.into_any(),
                    py_pattern,
                ],
            )?);
        }
        Ok(res)
    }

    pub fn close(&self) {
        self.state.closed.store(true, Ordering::Release);
        let _ = self.state.cmd_tx.send(PubSubCommand::Close);
        self.state.msg_condvar.notify_all();
    }
}

/// Asynchronous dedicated Pub/Sub listener for Redis subscriptions.
#[pyclass(
    name = "AsyncRedisListener",
    module = "vertebra._redis",
    frozen,
    immutable_type
)]
pub struct AsyncRedisListener {
    state: Arc<ListenerState>,
}

impl AsyncRedisListener {
    pub fn new(config: RedisClientConfig) -> Result<Self, RedisDriverError> {
        let notifier = Notifier::new().ok();
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let state = Arc::new(ListenerState {
            messages: Mutex::new(VecDeque::new()),
            msg_condvar: Condvar::new(),
            completions: Mutex::new(VecDeque::new()),
            notifier,
            next_op_id: AtomicUsize::new(1),
            closed: AtomicBool::new(false),
            cmd_tx,
        });

        let st_clone = Arc::clone(&state);
        vertebra_db::py_async::db_runtime_handle().spawn(async move {
            run_pubsub_loop(config, st_clone, cmd_rx).await;
        });

        Ok(Self { state })
    }

    fn wait_completion(&self, py: Python<'_>, op_id: usize) -> PyResult<()> {
        py.detach(|| {
            let mut guard = self.state.completions.lock();
            let start = Instant::now();
            let timeout = Duration::from_millis(1000);
            while !self.state.closed.load(Ordering::Acquire) {
                if let Some(pos) = guard.iter().position(|(id, _, _)| *id == op_id) {
                    let (_, success, err_msg) = guard.remove(pos).unwrap();
                    if !success {
                        let msg =
                            err_msg.unwrap_or_else(|| "Redis subscription failed".to_string());
                        return Err(RedisDriverError::programming(msg, false));
                    }
                    return Ok(());
                }
                if start.elapsed() >= timeout {
                    return Ok(());
                }
                self.state
                    .msg_condvar
                    .wait_for(&mut guard, Duration::from_millis(50));
            }
            Ok(())
        })
        .map_err(|e: RedisDriverError| e.into_pyerr(py))
    }
}

#[pymethods]
impl AsyncRedisListener {
    #[getter]
    pub fn closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    pub fn submit_subscribe(&self, channels: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        let _ = self.state.cmd_tx.send(PubSubCommand::Subscribe {
            channels,
            op_id: Some(op_id),
        });
        op_id
    }

    pub fn submit_psubscribe(&self, patterns: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        let _ = self.state.cmd_tx.send(PubSubCommand::Psubscribe {
            patterns,
            op_id: Some(op_id),
        });
        op_id
    }

    pub fn submit_ssubscribe(&self, shard_channels: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        let _ = self.state.cmd_tx.send(PubSubCommand::Ssubscribe {
            shard_channels,
            op_id: Some(op_id),
        });
        op_id
    }

    #[pyo3(signature = (channels = Vec::new()))]
    pub fn submit_unsubscribe(&self, channels: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        let _ = self.state.cmd_tx.send(PubSubCommand::Unsubscribe {
            channels,
            op_id: Some(op_id),
        });
        op_id
    }

    #[pyo3(signature = (patterns = Vec::new()))]
    pub fn submit_punsubscribe(&self, patterns: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        let _ = self.state.cmd_tx.send(PubSubCommand::Punsubscribe {
            patterns,
            op_id: Some(op_id),
        });
        op_id
    }

    #[pyo3(signature = (shard_channels = Vec::new()))]
    pub fn submit_sunsubscribe(&self, shard_channels: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        let _ = self.state.cmd_tx.send(PubSubCommand::Sunsubscribe {
            shard_channels,
            op_id: Some(op_id),
        });
        op_id
    }

    pub fn subscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        let op_id = self.submit_subscribe(channels);
        self.wait_completion(py, op_id)
    }

    pub fn psubscribe(&self, py: Python<'_>, patterns: Vec<String>) -> PyResult<()> {
        let op_id = self.submit_psubscribe(patterns);
        self.wait_completion(py, op_id)
    }

    pub fn ssubscribe(&self, py: Python<'_>, shard_channels: Vec<String>) -> PyResult<()> {
        let op_id = self.submit_ssubscribe(shard_channels);
        self.wait_completion(py, op_id)
    }

    #[pyo3(signature = (channels = Vec::new()))]
    pub fn unsubscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        let op_id = self.submit_unsubscribe(channels);
        self.wait_completion(py, op_id)
    }

    #[pyo3(signature = (patterns = Vec::new()))]
    pub fn punsubscribe(&self, py: Python<'_>, patterns: Vec<String>) -> PyResult<()> {
        let op_id = self.submit_punsubscribe(patterns);
        self.wait_completion(py, op_id)
    }

    #[pyo3(signature = (shard_channels = Vec::new()))]
    pub fn sunsubscribe(&self, py: Python<'_>, shard_channels: Vec<String>) -> PyResult<()> {
        let op_id = self.submit_sunsubscribe(shard_channels);
        self.wait_completion(py, op_id)
    }

    #[pyo3(signature = (timeout_ms = None))]
    pub fn next_message<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let msg = py.detach(|| {
            let mut guard = self.state.messages.lock();
            if let Some(m) = guard.pop_front() {
                return Some(m);
            }
            if self.state.closed.load(Ordering::Acquire) {
                return None;
            }

            match timeout_ms {
                Some(ms) => {
                    let dur = Duration::from_millis(ms);
                    let result = self.state.msg_condvar.wait_for(&mut guard, dur);
                    if result.timed_out() {
                        None
                    } else {
                        guard.pop_front()
                    }
                }
                None => {
                    self.state.msg_condvar.wait(&mut guard);
                    guard.pop_front()
                }
            }
        });

        match msg {
            Some((kind, channel, payload, pattern)) => {
                let py_kind = PyString::new(py, &kind);
                let py_channel = PyString::new(py, &channel);
                let py_payload = PyBytes::new(py, &payload);
                let py_pattern = match pattern {
                    Some(p) => PyString::new(py, &p).into_any(),
                    None => py.None().into_bound(py),
                };
                Ok(Some(PyTuple::new(
                    py,
                    [
                        py_kind.into_any(),
                        py_channel.into_any(),
                        py_payload.into_any(),
                        py_pattern,
                    ],
                )?))
            }
            None => Ok(None),
        }
    }

    pub fn drain_messages<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let mut raw = self.state.messages.lock();
        let drained: Vec<PubSubMessageTuple> = raw.drain(..).collect();
        drop(raw);

        let mut res = Vec::with_capacity(drained.len());
        for (kind, channel, payload, pattern) in drained {
            let py_kind = PyString::new(py, &kind);
            let py_channel = PyString::new(py, &channel);
            let py_payload = PyBytes::new(py, &payload);
            let py_pattern = match pattern {
                Some(p) => PyString::new(py, &p).into_any(),
                None => py.None().into_bound(py),
            };
            res.push(PyTuple::new(
                py,
                [
                    py_kind.into_any(),
                    py_channel.into_any(),
                    py_payload.into_any(),
                    py_pattern,
                ],
            )?);
        }
        Ok(res)
    }

    pub fn drain_completions<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let mut raw = self.state.completions.lock();
        let drained: Vec<(usize, bool, Option<String>)> = raw.drain(..).collect();
        drop(raw);

        let mut res = Vec::with_capacity(drained.len());
        for (op_id, success, err) in drained {
            let py_id = op_id.into_pyobject(py)?;
            let py_success = pyo3::types::PyBool::new(py, success);
            let py_err = match err {
                Some(e) => PyString::new(py, &e).into_any(),
                None => py.None().into_bound(py),
            };
            res.push(PyTuple::new(
                py,
                [py_id.into_any(), py_success.to_owned().into_any(), py_err],
            )?);
        }
        Ok(res)
    }

    pub fn cancel_operation(&self, op_id: usize) -> bool {
        let _ = op_id;
        true
    }

    pub fn notifier_fd(&self) -> i32 {
        #[cfg(unix)]
        {
            self.state
                .notifier
                .as_ref()
                .map(|n| n.as_raw_fd())
                .unwrap_or(-1)
        }
        #[cfg(not(unix))]
        {
            -1
        }
    }

    pub fn close(&self) {
        self.state.closed.store(true, Ordering::Release);
        let _ = self.state.cmd_tx.send(PubSubCommand::Close);
        self.state.msg_condvar.notify_all();
    }
}
