//! Dedicated Pub/Sub connection, subscription management, and push message delivery.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString, PyTuple};
use vertebra_core::notify::Notifier;

use crate::client_core::{open_redis_client_for_config, resolve_effective_credentials};
use crate::config::RedisClientConfig;
use crate::errors::{RedisDriverError, classify_redis_error};

const MAX_PUBSUB_MESSAGES: usize = 4096;
const MAX_PUBSUB_COMPLETIONS: usize = 1024;
const SUBSCRIBE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);

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

impl PubSubCommand {
    fn op_id(&self) -> Option<usize> {
        match self {
            Self::Subscribe { op_id, .. }
            | Self::Psubscribe { op_id, .. }
            | Self::Ssubscribe { op_id, .. }
            | Self::Unsubscribe { op_id, .. }
            | Self::Punsubscribe { op_id, .. }
            | Self::Sunsubscribe { op_id, .. } => *op_id,
            Self::Close => None,
        }
    }
}

struct ListenerState {
    messages: Mutex<VecDeque<PubSubMessageTuple>>,
    msg_condvar: Condvar,
    completions: Mutex<VecDeque<(usize, bool, Option<String>)>>,
    comp_condvar: Condvar,
    dial_error: Mutex<Option<String>>,
    notifier: Option<Notifier>,
    next_op_id: AtomicUsize,
    closed: AtomicBool,
}

impl ListenerState {
    fn push_message(&self, msg: PubSubMessageTuple) {
        {
            let mut queue = self.messages.lock();
            if queue.len() >= MAX_PUBSUB_MESSAGES {
                queue.pop_front();
            }
            queue.push_back(msg);
        }
        self.msg_condvar.notify_one();
        if let Some(ref notif) = self.notifier {
            let _ = notif.notify();
        }
    }

    fn push_completion(&self, op_id: usize, success: bool, err_msg: Option<String>) {
        {
            let mut queue = self.completions.lock();
            if queue.len() >= MAX_PUBSUB_COMPLETIONS {
                queue.pop_front();
            }
            queue.push_back((op_id, success, err_msg));
        }
        self.comp_condvar.notify_all();
        if let Some(ref notif) = self.notifier {
            let _ = notif.notify();
        }
    }

    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
        self.msg_condvar.notify_all();
        self.comp_condvar.notify_all();
        if let Some(ref notif) = self.notifier {
            let _ = notif.notify();
        }
    }

    fn wait_completion(&self, op_id: usize, timeout: Duration) -> Result<(), RedisDriverError> {
        let deadline = Instant::now() + timeout;
        let mut guard = self.completions.lock();
        loop {
            if let Some(idx) = guard.iter().position(|(id, _, _)| *id == op_id) {
                let (_, ok, err) = guard.remove(idx).unwrap();
                if ok {
                    return Ok(());
                }
                return Err(RedisDriverError::operational(
                    err.as_deref().unwrap_or("Redis subscription failed"),
                    false,
                ));
            }
            if self.closed.load(Ordering::Acquire) {
                if let Some(dial_err) = self.dial_error.lock().clone() {
                    return Err(RedisDriverError::operational(dial_err, true));
                }
                return Err(RedisDriverError::interface("Listener closed", false));
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RedisDriverError::operational(
                    "Subscription confirmation timed out",
                    true,
                ));
            }
            let res = self.comp_condvar.wait_for(&mut guard, deadline - now);
            if res.timed_out() {
                if let Some(idx) = guard.iter().position(|(id, _, _)| *id == op_id) {
                    let (_, ok, err) = guard.remove(idx).unwrap();
                    if ok {
                        return Ok(());
                    }
                    return Err(RedisDriverError::operational(
                        err.as_deref().unwrap_or("Redis subscription failed"),
                        false,
                    ));
                }
                return Err(RedisDriverError::operational(
                    "Subscription confirmation timed out",
                    true,
                ));
            }
        }
    }

    fn next_message_blocking(&self, timeout_ms: Option<u64>) -> Option<PubSubMessageTuple> {
        let mut guard = self.messages.lock();
        match timeout_ms {
            Some(ms) => {
                let deadline = Instant::now() + Duration::from_millis(ms);
                loop {
                    if let Some(m) = guard.pop_front() {
                        return Some(m);
                    }
                    if self.closed.load(Ordering::Acquire) {
                        return None;
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        return None;
                    }
                    let result = self.msg_condvar.wait_for(&mut guard, deadline - now);
                    if result.timed_out() {
                        return guard.pop_front();
                    }
                }
            }
            None => loop {
                if let Some(m) = guard.pop_front() {
                    return Some(m);
                }
                if self.closed.load(Ordering::Acquire) {
                    return None;
                }
                self.msg_condvar.wait(&mut guard);
            },
        }
    }
}

fn fail_pubsub_dial(
    state: &ListenerState,
    cmd_rx: &mut tokio::sync::mpsc::UnboundedReceiver<PubSubCommand>,
    err_msg: String,
) {
    *state.dial_error.lock() = Some(err_msg.clone());
    cmd_rx.close();
    while let Ok(cmd) = cmd_rx.try_recv() {
        if let Some(id) = cmd.op_id() {
            state.push_completion(id, false, Some(err_msg.clone()));
        }
    }
    state.mark_closed();
}

async fn run_pubsub_loop(
    mut config: RedisClientConfig,
    state: Arc<ListenerState>,
    mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<PubSubCommand>,
) {
    if config.uses_tls() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    // Pub/Sub multiplexed push delivery requires RESP3 push frames.
    config.protocol = 3;

    let (effective_username, effective_password, db) =
        match resolve_effective_credentials(&config, config.password.clone()).await {
            Ok(creds) => creds,
            Err(e) => {
                fail_pubsub_dial(&state, &mut cmd_rx, e.message().to_string());
                return;
            }
        };

    let client = match open_redis_client_for_config(
        &config,
        effective_username.as_deref(),
        effective_password.as_deref(),
        db,
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            fail_pubsub_dial(&state, &mut cmd_rx, e.message().to_string());
            return;
        }
    };

    let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel::<redis::PushInfo>();
    let conn_config = redis::AsyncConnectionConfig::new().set_push_sender(push_tx);

    let mut con = match tokio::time::timeout(
        Duration::from_secs(2),
        client.get_multiplexed_async_connection_with_config(&conn_config),
    )
    .await
    {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            let err = classify_redis_error(&e);
            fail_pubsub_dial(&state, &mut cmd_rx, err.message().to_string());
            return;
        }
        Err(_) => {
            fail_pubsub_dial(
                &state,
                &mut cmd_rx,
                "Pub/Sub connection attempt timed out".to_string(),
            );
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
                        } else if kind_str == "pmessage"
                            && let (Some(pat_val), Some(ch_val), Some(pl_val)) =
                                (iter.next(), iter.next(), iter.next())
                        {
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
                    None => {
                        break;
                    }
                }
            }
        }
    }

    cmd_rx.close();
    while let Ok(cmd) = cmd_rx.try_recv() {
        if let Some(id) = cmd.op_id() {
            state.push_completion(id, false, Some("Listener closed".to_string()));
        }
    }
    state.mark_closed();
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
    cmd_tx: tokio::sync::mpsc::UnboundedSender<PubSubCommand>,
}

impl Drop for SyncRedisListener {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(PubSubCommand::Close);
        self.state.mark_closed();
    }
}

impl SyncRedisListener {
    pub fn new(config: RedisClientConfig) -> Result<Self, RedisDriverError> {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let state = Arc::new(ListenerState {
            messages: Mutex::new(VecDeque::new()),
            msg_condvar: Condvar::new(),
            completions: Mutex::new(VecDeque::new()),
            comp_condvar: Condvar::new(),
            dial_error: Mutex::new(None),
            notifier: None,
            next_op_id: AtomicUsize::new(1),
            closed: AtomicBool::new(false),
        });

        let st_clone = Arc::clone(&state);
        vertebra_db::py_async::db_runtime_handle().spawn(async move {
            run_pubsub_loop(config, st_clone, cmd_rx).await;
        });

        Ok(Self { state, cmd_tx })
    }

    fn send_and_wait(&self, py: Python<'_>, cmd: PubSubCommand, op_id: usize) -> PyResult<()> {
        if self.cmd_tx.send(cmd).is_err() {
            let err = if let Some(dial_err) = self.state.dial_error.lock().clone() {
                RedisDriverError::operational(dial_err, true)
            } else {
                RedisDriverError::interface("Listener connection closed", false)
            };
            return Err(err.into_pyerr(py));
        }
        py.detach(|| self.state.wait_completion(op_id, SUBSCRIBE_CONFIRM_TIMEOUT))
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
        self.send_and_wait(
            py,
            PubSubCommand::Subscribe {
                channels,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    pub fn psubscribe(&self, py: Python<'_>, patterns: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.send_and_wait(
            py,
            PubSubCommand::Psubscribe {
                patterns,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    pub fn ssubscribe(&self, py: Python<'_>, shard_channels: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.send_and_wait(
            py,
            PubSubCommand::Ssubscribe {
                shard_channels,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    #[pyo3(signature = (channels = Vec::new()))]
    pub fn unsubscribe(&self, py: Python<'_>, channels: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.send_and_wait(
            py,
            PubSubCommand::Unsubscribe {
                channels,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    #[pyo3(signature = (patterns = Vec::new()))]
    pub fn punsubscribe(&self, py: Python<'_>, patterns: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.send_and_wait(
            py,
            PubSubCommand::Punsubscribe {
                patterns,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    #[pyo3(signature = (shard_channels = Vec::new()))]
    pub fn sunsubscribe(&self, py: Python<'_>, shard_channels: Vec<String>) -> PyResult<()> {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.send_and_wait(
            py,
            PubSubCommand::Sunsubscribe {
                shard_channels,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    #[pyo3(signature = (timeout_ms = None))]
    pub fn next_message<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let msg = py.detach(|| self.state.next_message_blocking(timeout_ms));

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
        let _ = self.cmd_tx.send(PubSubCommand::Close);
        self.state.mark_closed();
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
    cmd_tx: tokio::sync::mpsc::UnboundedSender<PubSubCommand>,
}

impl Drop for AsyncRedisListener {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(PubSubCommand::Close);
        self.state.mark_closed();
    }
}

impl AsyncRedisListener {
    pub fn new(config: RedisClientConfig) -> Result<Self, RedisDriverError> {
        let notifier = Notifier::new().ok();
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let state = Arc::new(ListenerState {
            messages: Mutex::new(VecDeque::new()),
            msg_condvar: Condvar::new(),
            completions: Mutex::new(VecDeque::new()),
            comp_condvar: Condvar::new(),
            dial_error: Mutex::new(None),
            notifier,
            next_op_id: AtomicUsize::new(1),
            closed: AtomicBool::new(false),
        });

        let st_clone = Arc::clone(&state);
        vertebra_db::py_async::db_runtime_handle().spawn(async move {
            run_pubsub_loop(config, st_clone, cmd_rx).await;
        });

        Ok(Self { state, cmd_tx })
    }

    fn submit_command(&self, cmd: PubSubCommand, op_id: usize) -> usize {
        if self.cmd_tx.send(cmd).is_err() {
            let err_msg = self
                .state
                .dial_error
                .lock()
                .clone()
                .unwrap_or_else(|| "Listener connection closed".to_string());
            self.state.push_completion(op_id, false, Some(err_msg));
        }
        op_id
    }

    fn wait_completion(&self, py: Python<'_>, op_id: usize) -> PyResult<()> {
        py.detach(|| self.state.wait_completion(op_id, SUBSCRIBE_CONFIRM_TIMEOUT))
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
        self.submit_command(
            PubSubCommand::Subscribe {
                channels,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    pub fn submit_psubscribe(&self, patterns: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.submit_command(
            PubSubCommand::Psubscribe {
                patterns,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    pub fn submit_ssubscribe(&self, shard_channels: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.submit_command(
            PubSubCommand::Ssubscribe {
                shard_channels,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    #[pyo3(signature = (channels = Vec::new()))]
    pub fn submit_unsubscribe(&self, channels: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.submit_command(
            PubSubCommand::Unsubscribe {
                channels,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    #[pyo3(signature = (patterns = Vec::new()))]
    pub fn submit_punsubscribe(&self, patterns: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.submit_command(
            PubSubCommand::Punsubscribe {
                patterns,
                op_id: Some(op_id),
            },
            op_id,
        )
    }

    #[pyo3(signature = (shard_channels = Vec::new()))]
    pub fn submit_sunsubscribe(&self, shard_channels: Vec<String>) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        self.submit_command(
            PubSubCommand::Sunsubscribe {
                shard_channels,
                op_id: Some(op_id),
            },
            op_id,
        )
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
        let msg = py.detach(|| self.state.next_message_blocking(timeout_ms));

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
        let _ = self.cmd_tx.send(PubSubCommand::Close);
        self.state.mark_closed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completion_ack_does_not_wake_next_message_with_spurious_none() {
        let (_cmd_tx, _cmd_rx) = tokio::sync::mpsc::unbounded_channel::<PubSubCommand>();
        let state = Arc::new(ListenerState {
            messages: Mutex::new(VecDeque::new()),
            msg_condvar: Condvar::new(),
            completions: Mutex::new(VecDeque::new()),
            comp_condvar: Condvar::new(),
            dial_error: Mutex::new(None),
            notifier: None,
            next_op_id: AtomicUsize::new(1),
            closed: AtomicBool::new(false),
        });

        let state_reader = Arc::clone(&state);
        let reader = std::thread::spawn(move || state_reader.next_message_blocking(None));

        std::thread::sleep(Duration::from_millis(15));
        // Push a subscribe completion AND spuriously notify msg_condvar while listener is still open.
        state.push_completion(1, true, None);
        state.msg_condvar.notify_all();
        std::thread::sleep(Duration::from_millis(15));

        // Now deliver an actual message; next_message(None) must return this message, not None!
        state.push_message((
            "message".to_string(),
            "chan".to_string(),
            b"hello".to_vec(),
            None,
        ));
        let received = reader.join().unwrap();
        assert_eq!(
            received,
            Some((
                "message".to_string(),
                "chan".to_string(),
                b"hello".to_vec(),
                None
            ))
        );

        // Timeout on wait_completion must return Err, never Ok(()).
        let timeout_res = state.wait_completion(999, Duration::from_millis(20));
        assert!(timeout_res.is_err());
    }
}
