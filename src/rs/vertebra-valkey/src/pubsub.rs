//! Dedicated Valkey Pub/Sub (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`) listeners.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex as SyncMutex;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{Mutex, Notify};
use vertebra_db::py_async::{
    RustDbTaskAwaitable, block_on_db, spawn_rust_db_task_with_attr_and_cancel,
};

use crate::cluster::{ClusterTopology, is_read_only_command};
use crate::codec::{RespValue, encode_owned_command, try_decode_frame};
use crate::config::{ValkeyConfigInner, ValkeyTarget, await_pending_python_token};
use crate::error::ValkeyError;
use crate::sentinel::{
    build_get_master_addr_cmd, parse_get_master_addr_reply, parse_sentinel_addr,
    verify_role_is_primary,
};
use crate::transport::{RespConnection, ValkeyStream};
use crate::{VALKEY_COMPLETION_DRAINER_ATTR, valkey_hash_slot};

/// Maximum buffered Pub/Sub messages before oldest-drop backpressure engages.
pub const MAX_PUBSUB_MESSAGES: usize = 4096;

/// Decoded Pub/Sub message delivered by [`ValkeyListenerCore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValkeyPubSubMessage {
    pub msg_type: String,
    pub pattern: Option<String>,
    pub channel: String,
    pub data: Bytes,
}

impl ValkeyPubSubMessage {
    /// Convert message into a Python dictionary `{"type", "pattern", "channel", "data"}`.
    pub fn to_py_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        dict.set_item("type", self.msg_type.as_str())?;
        match &self.pattern {
            Some(pat) => dict.set_item("pattern", pat.as_str())?,
            None => dict.set_item("pattern", py.None())?,
        }
        dict.set_item("channel", self.channel.as_str())?;
        dict.set_item("data", PyBytes::new(py, &self.data))?;
        Ok(dict.into_any().unbind())
    }
}

/// Drop-restoring guard that pushes an undelivered [`ValkeyPubSubMessage`] back to the front of
/// the listener queue if the awaiting Python `Future` is cancelled before `map_ok` runs.
pub struct PendingDeliveryGuard {
    messages: Arc<SyncMutex<VecDeque<ValkeyPubSubMessage>>>,
    msg_notify: Arc<Notify>,
    msg: Option<ValkeyPubSubMessage>,
}

impl PendingDeliveryGuard {
    #[must_use]
    pub fn new(
        messages: Arc<SyncMutex<VecDeque<ValkeyPubSubMessage>>>,
        msg_notify: Arc<Notify>,
        msg: ValkeyPubSubMessage,
    ) -> Self {
        Self {
            messages,
            msg_notify,
            msg: Some(msg),
        }
    }

    /// Take ownership of the guarded message, disarming drop restoration.
    pub fn into_message(mut self) -> ValkeyPubSubMessage {
        self.msg
            .take()
            .expect("PendingDeliveryGuard consumed exactly once")
    }

    /// Convert into a Python dictionary and disarm drop restoration only after conversion succeeds.
    pub fn into_py_dict(mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = self
            .msg
            .as_ref()
            .expect("PendingDeliveryGuard holds a message")
            .to_py_dict(py)?;
        self.msg = None;
        Ok(dict)
    }
}

impl Drop for PendingDeliveryGuard {
    fn drop(&mut self) {
        if let Some(msg) = self.msg.take() {
            {
                let mut q = self.messages.lock();
                if q.len() >= MAX_PUBSUB_MESSAGES {
                    let _ = q.pop_back();
                }
                q.push_front(msg);
            }
            self.msg_notify.notify_one();
        }
    }
}

struct ListenerSession {
    writer: WriteHalf<ValkeyStream>,
    reader_task: tokio::task::JoinHandle<()>,
}

/// Core async Pub/Sub listener engine shared across sync and async PyO3 wrappers.
pub struct ValkeyListenerCore {
    config: Arc<ValkeyConfigInner>,
    session: Mutex<Option<ListenerSession>>,
    reader_abort: SyncMutex<Option<tokio::task::AbortHandle>>,
    messages: Arc<SyncMutex<VecDeque<ValkeyPubSubMessage>>>,
    msg_notify: Arc<Notify>,
    ack_notify: Arc<Notify>,
    close_notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

impl ValkeyListenerCore {
    /// Create a disconnected Pub/Sub listener handle.
    #[must_use]
    pub fn new(config: Arc<ValkeyConfigInner>) -> Arc<Self> {
        Arc::new(Self {
            config,
            session: Mutex::new(None),
            reader_abort: SyncMutex::new(None),
            messages: Arc::new(SyncMutex::new(VecDeque::new())),
            msg_notify: Arc::new(Notify::new()),
            ack_notify: Arc::new(Notify::new()),
            close_notify: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    #[must_use]
    pub fn config(&self) -> &Arc<ValkeyConfigInner> {
        &self.config
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn abort_sync(&self) {
        self.closed.store(true, Ordering::Release);
        if let Some(abort) = self.reader_abort.lock().take() {
            abort.abort();
        }
        self.ack_notify.notify_waiters();
        self.close_notify.notify_waiters();
        self.msg_notify.notify_waiters();
    }

    fn pop_message(&self) -> Option<ValkeyPubSubMessage> {
        self.messages.lock().pop_front()
    }

    async fn ensure_connected(&self, slot_hint: Option<u16>) -> Result<(), ValkeyError> {
        if self.is_closed() {
            return Err(ValkeyError::interface("Valkey listener is closed"));
        }
        let mut guard = self.session.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        let target = self.resolve_listener_target(slot_hint).await?;
        let conn = RespConnection::establish(&target, &self.config, false, true, None).await?;
        let (reader, writer, initial_buf) = conn.into_split();
        let messages = Arc::clone(&self.messages);
        let msg_notify = Arc::clone(&self.msg_notify);
        let ack_notify = Arc::clone(&self.ack_notify);
        let close_notify = Arc::clone(&self.close_notify);
        let closed = Arc::clone(&self.closed);

        let reader_task = tokio::spawn(async move {
            run_listener_reader(
                reader,
                initial_buf,
                messages,
                msg_notify,
                ack_notify,
                close_notify,
                closed,
            )
            .await;
        });
        *self.reader_abort.lock() = Some(reader_task.abort_handle());
        if self.is_closed() {
            if let Some(abort) = self.reader_abort.lock().take() {
                abort.abort();
            }
            return Err(ValkeyError::interface("Valkey listener is closed"));
        }

        *guard = Some(ListenerSession {
            writer,
            reader_task,
        });
        Ok(())
    }

    async fn resolve_listener_target(
        &self,
        slot_hint: Option<u16>,
    ) -> Result<ValkeyTarget, ValkeyError> {
        if !self.config.sentinel_nodes.is_empty()
            && let Some(ref service_name) = self.config.sentinel_service_name
        {
            for spec in &self.config.sentinel_nodes {
                let Ok((shost, sport)) = parse_sentinel_addr(spec, 26379) else {
                    continue;
                };
                let starget = ValkeyTarget::Tcp {
                    host: shost,
                    port: sport,
                };
                let Ok(mut sconn) =
                    RespConnection::establish(&starget, &self.config, false, false, None).await
                else {
                    continue;
                };
                if let Some(ref pass) = self.config.sentinel_password {
                    let mut cmd = vec![b"AUTH".to_vec()];
                    if let Some(ref u) = self.config.sentinel_username
                        && !u.is_empty()
                    {
                        cmd.push(u.as_bytes().to_vec());
                    }
                    cmd.push(pass.as_bytes().to_vec());
                    if sconn.execute_single(&cmd).await.is_err() {
                        continue;
                    }
                }
                let q = build_get_master_addr_cmd(service_name);
                let Ok(reply) = sconn.execute_single(&q).await else {
                    continue;
                };
                let Ok((mhost, mport)) = parse_get_master_addr_reply(&reply) else {
                    continue;
                };
                let mtarget = ValkeyTarget::Tcp {
                    host: mhost,
                    port: mport,
                };
                let Ok(mut check_conn) =
                    RespConnection::establish(&mtarget, &self.config, false, true, None).await
                else {
                    continue;
                };
                if let Ok(role_reply) = check_conn.execute_single(&[b"ROLE".to_vec()]).await
                    && verify_role_is_primary(&role_reply).is_ok()
                {
                    return Ok(mtarget);
                }
            }
            return Err(ValkeyError::connection(
                "No Sentinel reported an active Valkey primary for listener",
            ));
        }

        if self.config.cluster_enabled
            && let Some(slot) = slot_hint
        {
            let mut seeds = Vec::new();
            for spec in &self.config.cluster_nodes {
                if let Ok((h, p)) = parse_sentinel_addr(spec, 6379) {
                    seeds.push(ValkeyTarget::Tcp { host: h, port: p });
                }
            }
            if seeds.is_empty() {
                seeds.push(self.config.target.clone());
            }
            for seed in seeds {
                if let Ok(mut conn) =
                    RespConnection::establish(&seed, &self.config, false, true, None).await
                {
                    let topo_res = match conn
                        .execute_single(&[b"CLUSTER".to_vec(), b"SHARDS".to_vec()])
                        .await
                    {
                        Ok(r) => ClusterTopology::from_cluster_shards(&r, self.config.tls),
                        Err(_) => match conn
                            .execute_single(&[b"CLUSTER".to_vec(), b"SLOTS".to_vec()])
                            .await
                        {
                            Ok(r) => ClusterTopology::from_cluster_slots(&r),
                            Err(e) => Err(e),
                        },
                    };
                    if let Ok(topo) = topo_res
                        && let Some((ep, _)) =
                            topo.route_for_slot(slot, is_read_only_command(b"GET"), false)
                    {
                        return Ok(ValkeyTarget::Tcp {
                            host: ep.host,
                            port: ep.port,
                        });
                    }
                }
            }
        }

        Ok(self.config.target.clone())
    }

    async fn send_sub_command(&self, verb: &str, items: &[String]) -> Result<(), ValkeyError> {
        if items.is_empty() {
            return Ok(());
        }
        let slot_hint = if verb.eq_ignore_ascii_case("SSUBSCRIBE") {
            items.first().map(|ch| valkey_hash_slot(ch.as_bytes()))
        } else {
            None
        };
        self.ensure_connected(slot_hint).await?;

        let mut cmd = Vec::with_capacity(1 + items.len());
        cmd.push(verb.as_bytes().to_vec());
        for item in items {
            cmd.push(item.as_bytes().to_vec());
        }
        let mut buf = BytesMut::new();
        encode_owned_command(&mut buf, &cmd);
        let bytes = buf.freeze();

        let ack_notified = self.ack_notify.notified();
        tokio::pin!(ack_notified);
        ack_notified.as_mut().enable();
        {
            let mut guard = self.session.lock().await;
            let sess = guard
                .as_mut()
                .ok_or_else(|| ValkeyError::interface("Valkey listener is closed"))?;
            tokio::time::timeout(self.config.command_timeout, async {
                sess.writer.write_all(&bytes).await?;
                sess.writer.flush().await
            })
            .await
            .map_err(|_| ValkeyError::connection("Timed out sending subscription command"))?
            .map_err(ValkeyError::from)?;
        }

        let _ = tokio::time::timeout(self.config.command_timeout, ack_notified).await;
        Ok(())
    }

    /// Issue `SUBSCRIBE`.
    pub async fn subscribe(&self, channels: &[String]) -> Result<(), ValkeyError> {
        self.send_sub_command("SUBSCRIBE", channels).await
    }

    /// Issue `PSUBSCRIBE`.
    pub async fn psubscribe(&self, patterns: &[String]) -> Result<(), ValkeyError> {
        self.send_sub_command("PSUBSCRIBE", patterns).await
    }

    /// Issue `SSUBSCRIBE`.
    pub async fn ssubscribe(&self, channels: &[String]) -> Result<(), ValkeyError> {
        self.send_sub_command("SSUBSCRIBE", channels).await
    }

    /// Issue `UNSUBSCRIBE`.
    pub async fn unsubscribe(&self, channels: &[String]) -> Result<(), ValkeyError> {
        if !self.is_connected().await {
            return Ok(());
        }
        self.send_unsub_command("UNSUBSCRIBE", channels).await
    }

    /// Issue `PUNSUBSCRIBE`.
    pub async fn punsubscribe(&self, patterns: &[String]) -> Result<(), ValkeyError> {
        if !self.is_connected().await {
            return Ok(());
        }
        self.send_unsub_command("PUNSUBSCRIBE", patterns).await
    }

    /// Issue `SUNSUBSCRIBE`.
    pub async fn sunsubscribe(&self, channels: &[String]) -> Result<(), ValkeyError> {
        if !self.is_connected().await {
            return Ok(());
        }
        self.send_unsub_command("SUNSUBSCRIBE", channels).await
    }

    async fn is_connected(&self) -> bool {
        self.session.lock().await.is_some()
    }

    async fn send_unsub_command(&self, verb: &str, items: &[String]) -> Result<(), ValkeyError> {
        let mut cmd = Vec::with_capacity(1 + items.len());
        cmd.push(verb.as_bytes().to_vec());
        for item in items {
            cmd.push(item.as_bytes().to_vec());
        }
        let mut buf = BytesMut::new();
        encode_owned_command(&mut buf, &cmd);
        let bytes = buf.freeze();

        let ack_notified = self.ack_notify.notified();
        tokio::pin!(ack_notified);
        ack_notified.as_mut().enable();
        {
            let mut guard = self.session.lock().await;
            let Some(sess) = guard.as_mut() else {
                return Ok(());
            };
            tokio::time::timeout(self.config.command_timeout, async {
                sess.writer.write_all(&bytes).await?;
                sess.writer.flush().await
            })
            .await
            .map_err(|_| ValkeyError::connection("Timed out sending unsubscribe command"))?
            .map_err(ValkeyError::from)?;
        }
        let _ = tokio::time::timeout(self.config.command_timeout, ack_notified).await;
        Ok(())
    }

    /// Wait for the next delivered message or return `Ok(None)` on timeout/close.
    pub async fn next_message(
        &self,
        timeout_ms: Option<u64>,
    ) -> Result<Option<ValkeyPubSubMessage>, ValkeyError> {
        let deadline = timeout_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
        loop {
            let msg_notified = self.msg_notify.notified();
            let close_notified = self.close_notify.notified();
            tokio::pin!(msg_notified);
            tokio::pin!(close_notified);
            msg_notified.as_mut().enable();
            close_notified.as_mut().enable();

            if let Some(msg) = self.pop_message() {
                return Ok(Some(msg));
            }
            if self.is_closed() {
                return Ok(None);
            }

            match deadline {
                Some(dl) => {
                    let now = Instant::now();
                    if now >= dl {
                        return Ok(self.pop_message());
                    }
                    let sleep_fut = tokio::time::sleep(dl - now);
                    tokio::pin!(sleep_fut);
                    tokio::select! {
                        () = &mut msg_notified => {}
                        () = &mut close_notified => {
                            return Ok(self.pop_message());
                        }
                        () = &mut sleep_fut => {
                            return Ok(self.pop_message());
                        }
                    }
                }
                None => {
                    tokio::select! {
                        () = &mut msg_notified => {}
                        () = &mut close_notified => {
                            return Ok(self.pop_message());
                        }
                    }
                }
            }
        }
    }

    /// Wait for the next delivered message wrapped in a [`PendingDeliveryGuard`].
    pub async fn next_message_guarded(
        &self,
        timeout_ms: Option<u64>,
    ) -> Result<Option<PendingDeliveryGuard>, ValkeyError> {
        let maybe_msg = self.next_message(timeout_ms).await?;
        Ok(maybe_msg.map(|msg| {
            PendingDeliveryGuard::new(
                Arc::clone(&self.messages),
                Arc::clone(&self.msg_notify),
                msg,
            )
        }))
    }

    /// Drain currently buffered messages without blocking.
    pub async fn drain_messages(&self, max_messages: usize) -> Vec<ValkeyPubSubMessage> {
        let limit = max_messages.max(1);
        let mut q = self.messages.lock();
        let mut out = Vec::with_capacity(q.len().min(limit));
        while out.len() < limit {
            match q.pop_front() {
                Some(msg) => out.push(msg),
                None => break,
            }
        }
        out
    }

    /// Close the listener, terminate its socket/reader task, and unblock waiting callers.
    pub async fn close(&self) {
        self.abort_sync();
        let sess_opt = self.session.lock().await.take();
        if let Some(mut sess) = sess_opt {
            let _ = sess.writer.shutdown().await;
            sess.reader_task.abort();
        }
        self.ack_notify.notify_waiters();
        self.close_notify.notify_waiters();
        self.msg_notify.notify_waiters();
    }
}

impl Drop for ValkeyListenerCore {
    fn drop(&mut self) {
        self.abort_sync();
    }
}

async fn run_listener_reader(
    mut reader: ReadHalf<ValkeyStream>,
    mut read_buf: BytesMut,
    messages: Arc<SyncMutex<VecDeque<ValkeyPubSubMessage>>>,
    msg_notify: Arc<Notify>,
    ack_notify: Arc<Notify>,
    close_notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
) {
    let mut scratch = [0u8; 8192];
    loop {
        loop {
            match try_decode_frame(&mut read_buf) {
                Ok(Some(frame)) => {
                    if let Some(parsed) = parse_pubsub_frame(frame, &ack_notify) {
                        {
                            let mut q = messages.lock();
                            if q.len() >= MAX_PUBSUB_MESSAGES {
                                let _ = q.pop_front();
                            }
                            q.push_back(parsed);
                        }
                        msg_notify.notify_one();
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    closed.store(true, Ordering::Release);
                    ack_notify.notify_waiters();
                    msg_notify.notify_waiters();
                    close_notify.notify_waiters();
                    return;
                }
            }
        }

        if closed.load(Ordering::Acquire) {
            ack_notify.notify_waiters();
            msg_notify.notify_waiters();
            close_notify.notify_waiters();
            return;
        }

        match reader.read(&mut scratch).await {
            Ok(0) | Err(_) => {
                closed.store(true, Ordering::Release);
                ack_notify.notify_waiters();
                msg_notify.notify_waiters();
                close_notify.notify_waiters();
                return;
            }
            Ok(n) => read_buf.put_slice(&scratch[..n]),
        }
    }
}

fn parse_pubsub_frame(frame: RespValue, ack_notify: &Notify) -> Option<ValkeyPubSubMessage> {
    let unattr = frame.into_unattributed();
    let items = match unattr {
        RespValue::Push(items) | RespValue::Array(items) => items,
        _ => return None,
    };
    let kind = items.first()?.as_str()?.to_ascii_lowercase();
    match kind.as_str() {
        "subscribe" | "psubscribe" | "ssubscribe" | "unsubscribe" | "punsubscribe"
        | "sunsubscribe" => {
            ack_notify.notify_waiters();
            None
        }
        "message" | "smessage" => {
            if items.len() < 3 {
                return None;
            }
            let channel = items[1].as_str()?.into_owned();
            let data = extract_payload_bytes(&items[2]);
            Some(ValkeyPubSubMessage {
                msg_type: kind,
                pattern: None,
                channel,
                data,
            })
        }
        "pmessage" => {
            if items.len() < 4 {
                return None;
            }
            let pattern = items[1].as_str()?.into_owned();
            let channel = items[2].as_str()?.into_owned();
            let data = extract_payload_bytes(&items[3]);
            Some(ValkeyPubSubMessage {
                msg_type: kind,
                pattern: Some(pattern),
                channel,
                data,
            })
        }
        _ => None,
    }
}

fn extract_payload_bytes(val: &RespValue) -> Bytes {
    match val.unattributed() {
        RespValue::BulkString(b) | RespValue::SimpleString(b) => b.clone(),
        RespValue::VerbatimString { data, .. } => data.clone(),
        RespValue::Null => Bytes::new(),
        other => other
            .as_bytes()
            .map(Bytes::copy_from_slice)
            .unwrap_or_default(),
    }
}

pub(crate) fn extract_string_args(args: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let seq = args.extract::<Vec<Bound<'_, PyAny>>>()?;
    if seq.len() == 1
        && !seq[0].is_instance_of::<pyo3::types::PyString>()
        && let Ok(nested) = seq[0].extract::<Vec<String>>()
    {
        return Ok(nested);
    }
    let mut out = Vec::with_capacity(seq.len());
    for item in seq {
        out.push(item.extract::<String>()?);
    }
    Ok(out)
}

/// Synchronous Valkey Pub/Sub listener releasing the Python GIL across blocking reads.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(frozen, immutable_type, freelist = 128, module = "vertebra._valkey")
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(frozen, immutable_type, module = "vertebra._valkey")
)]
pub struct SyncValkeyListener {
    core: Arc<ValkeyListenerCore>,
}

impl SyncValkeyListener {
    pub(crate) fn from_config(config: Arc<ValkeyConfigInner>) -> Self {
        Self {
            core: ValkeyListenerCore::new(config),
        }
    }
}

#[pymethods]
impl SyncValkeyListener {
    #[getter]
    pub fn closed(&self) -> bool {
        self.core.is_closed()
    }

    #[pyo3(signature = (*channels))]
    pub fn subscribe(&self, py: Python<'_>, channels: &Bound<'_, PyAny>) -> PyResult<()> {
        let chs = extract_string_args(channels)?;
        self.core
            .config()
            .ensure_python_token_sync(py, false)
            .map_err(|e| e.to_py_err(py))?;
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.subscribe(&chs).await }))
            .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (*patterns))]
    pub fn psubscribe(&self, py: Python<'_>, patterns: &Bound<'_, PyAny>) -> PyResult<()> {
        let pats = extract_string_args(patterns)?;
        self.core
            .config()
            .ensure_python_token_sync(py, false)
            .map_err(|e| e.to_py_err(py))?;
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.psubscribe(&pats).await }))
            .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (*channels))]
    pub fn ssubscribe(&self, py: Python<'_>, channels: &Bound<'_, PyAny>) -> PyResult<()> {
        let chs = extract_string_args(channels)?;
        self.core
            .config()
            .ensure_python_token_sync(py, false)
            .map_err(|e| e.to_py_err(py))?;
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.ssubscribe(&chs).await }))
            .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (*channels))]
    pub fn unsubscribe(&self, py: Python<'_>, channels: &Bound<'_, PyAny>) -> PyResult<()> {
        let chs = extract_string_args(channels)?;
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.unsubscribe(&chs).await }))
            .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (*patterns))]
    pub fn punsubscribe(&self, py: Python<'_>, patterns: &Bound<'_, PyAny>) -> PyResult<()> {
        let pats = extract_string_args(patterns)?;
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.punsubscribe(&pats).await }))
            .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (*channels))]
    pub fn sunsubscribe(&self, py: Python<'_>, channels: &Bound<'_, PyAny>) -> PyResult<()> {
        let chs = extract_string_args(channels)?;
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.sunsubscribe(&chs).await }))
            .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (timeout_ms = None))]
    pub fn next_message(
        &self,
        py: Python<'_>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let core = Arc::clone(&self.core);
        let msg = py
            .detach(|| block_on_db(async move { core.next_message(timeout_ms).await }))
            .map_err(|e| e.to_py_err(py))?;
        msg.map(|m| m.to_py_dict(py)).transpose()
    }

    #[pyo3(signature = (max_messages = 1024))]
    pub fn drain_messages(&self, py: Python<'_>, max_messages: usize) -> PyResult<Py<PyAny>> {
        let core = Arc::clone(&self.core);
        let items =
            py.detach(|| block_on_db(async move { core.drain_messages(max_messages).await }));
        let list = PyList::empty(py);
        for item in items {
            list.append(item.to_py_dict(py)?)?;
        }
        Ok(list.into_any().unbind())
    }

    pub fn close(&self, py: Python<'_>) {
        let core = Arc::clone(&self.core);
        py.detach(|| block_on_db(async move { core.close().await }));
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
}

impl Drop for SyncValkeyListener {
    fn drop(&mut self) {
        self.core.abort_sync();
    }
}

/// Asynchronous Valkey Pub/Sub listener backed by `DbCompletionDrainer`.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(frozen, immutable_type, freelist = 128, module = "vertebra._valkey")
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(frozen, immutable_type, module = "vertebra._valkey")
)]
pub struct AsyncValkeyListener {
    core: Arc<ValkeyListenerCore>,
}

impl AsyncValkeyListener {
    pub(crate) fn from_config(config: Arc<ValkeyConfigInner>) -> Self {
        Self {
            core: ValkeyListenerCore::new(config),
        }
    }
}

#[pymethods]
impl AsyncValkeyListener {
    #[getter]
    pub fn closed(&self) -> bool {
        self.core.is_closed()
    }

    #[pyo3(signature = (*channels))]
    pub fn subscribe<'py>(
        &self,
        py: Python<'py>,
        channels: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let chs = extract_string_args(channels)?;
        let pending_tok = self
            .core
            .config()
            .prepare_python_token_async(py, false)
            .map_err(|e| e.to_py_err(py))?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                if let Some(rx) = pending_tok {
                    await_pending_python_token(rx).await?;
                }
                core.subscribe(&chs).await
            },
            |py, ()| Ok(py.None()),
            |py, e| e.to_py_err(py),
            || {},
            VALKEY_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (*patterns))]
    pub fn psubscribe<'py>(
        &self,
        py: Python<'py>,
        patterns: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let pats = extract_string_args(patterns)?;
        let pending_tok = self
            .core
            .config()
            .prepare_python_token_async(py, false)
            .map_err(|e| e.to_py_err(py))?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                if let Some(rx) = pending_tok {
                    await_pending_python_token(rx).await?;
                }
                core.psubscribe(&pats).await
            },
            |py, ()| Ok(py.None()),
            |py, e| e.to_py_err(py),
            || {},
            VALKEY_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (*channels))]
    pub fn ssubscribe<'py>(
        &self,
        py: Python<'py>,
        channels: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let chs = extract_string_args(channels)?;
        let pending_tok = self
            .core
            .config()
            .prepare_python_token_async(py, false)
            .map_err(|e| e.to_py_err(py))?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                if let Some(rx) = pending_tok {
                    await_pending_python_token(rx).await?;
                }
                core.ssubscribe(&chs).await
            },
            |py, ()| Ok(py.None()),
            |py, e| e.to_py_err(py),
            || {},
            VALKEY_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (*channels))]
    pub fn unsubscribe<'py>(
        &self,
        py: Python<'py>,
        channels: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let chs = extract_string_args(channels)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move { core.unsubscribe(&chs).await },
            |py, ()| Ok(py.None()),
            |py, e| e.to_py_err(py),
            || {},
            VALKEY_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (*patterns))]
    pub fn punsubscribe<'py>(
        &self,
        py: Python<'py>,
        patterns: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let pats = extract_string_args(patterns)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move { core.punsubscribe(&pats).await },
            |py, ()| Ok(py.None()),
            |py, e| e.to_py_err(py),
            || {},
            VALKEY_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (*channels))]
    pub fn sunsubscribe<'py>(
        &self,
        py: Python<'py>,
        channels: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let chs = extract_string_args(channels)?;
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move { core.sunsubscribe(&chs).await },
            |py, ()| Ok(py.None()),
            |py, e| e.to_py_err(py),
            || {},
            VALKEY_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (timeout_ms = None))]
    pub fn next_message<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let core = Arc::clone(&self.core);
        let cancel_notify = Arc::new(Notify::new());
        let cancel_wait = Arc::clone(&cancel_notify);
        let cancel_trigger = Arc::clone(&cancel_notify);
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::select! {
                    biased;
                    () = cancel_wait.notified() => {
                        Err(ValkeyError::cancelled("Valkey next_message cancelled"))
                    }
                    res = core.next_message_guarded(timeout_ms) => res,
                }
            },
            |py, guard_opt| match guard_opt {
                Some(guard) => guard.into_py_dict(py),
                None => Ok(py.None()),
            },
            |py, e| e.to_py_err(py),
            move || cancel_trigger.notify_one(),
            VALKEY_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (max_messages = 1024))]
    pub fn drain_messages(&self, py: Python<'_>, max_messages: usize) -> PyResult<Py<PyAny>> {
        let core = Arc::clone(&self.core);
        let items =
            py.detach(|| block_on_db(async move { core.drain_messages(max_messages).await }));
        let list = PyList::empty(py);
        for item in items {
            list.append(item.to_py_dict(py)?)?;
        }
        Ok(list.into_any().unbind())
    }

    pub fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                core.close().await;
                Ok::<(), ValkeyError>(())
            },
            |py, ()| Ok(py.None()),
            |py, e| e.to_py_err(py),
            || {},
            VALKEY_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.close(py)
    }

    pub fn __aenter__<'py>(
        slf: Py<Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move { Ok::<Py<Self>, ValkeyError>(slf) },
            |_py, obj| Ok(obj.into_any()),
            |py, e| e.to_py_err(py),
            || {},
            VALKEY_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (_exc_type = None, _exc_val = None, _exc_tb = None))]
    pub fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let core = Arc::clone(&self.core);
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                core.close().await;
                Ok::<bool, ValkeyError>(false)
            },
            |py, suppressed| Ok(suppressed.into_pyobject(py)?.to_owned().into_any().unbind()),
            |py, e| e.to_py_err(py),
            || {},
            VALKEY_COMPLETION_DRAINER_ATTR,
        )
    }
}

impl Drop for AsyncValkeyListener {
    fn drop(&mut self) {
        self.core.abort_sync();
    }
}
