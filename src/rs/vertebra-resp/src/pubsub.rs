//! Dedicated RESP Pub/Sub (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`) listeners.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::{Condvar, Mutex as SyncMutex};
#[cfg(feature = "pyo3")]
use pyo3::prelude::*;
#[cfg(feature = "pyo3")]
use pyo3::types::{PyBytes, PyDict};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{Mutex, Notify};
use vertebra_core::notify::Notifier;

use crate::cluster::{ClusterTopology, is_read_only_command};
use crate::codec::{RespValue, encode_owned_command, try_decode_frame};
use crate::config::{RespConfig, RespTarget};
use crate::error::RespError;
use crate::hash_slot;
use crate::sentinel::{
    build_get_master_addr_cmd, parse_get_master_addr_reply, parse_sentinel_addr,
    verify_role_is_primary,
};
use crate::transport::{RespConnection, RespStream};

/// Maximum buffered Pub/Sub messages before oldest-drop backpressure engages.
pub const MAX_PUBSUB_MESSAGES: usize = 4096;

/// Maximum buffered subscription control completions before dropping oldest entries.
pub const MAX_PUBSUB_COMPLETIONS: usize = 1024;

/// Default timeout waiting for subscription confirmation on synchronous listeners.
pub const SUBSCRIBE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);

/// Tuple representation `(kind, channel, payload, pattern)` used by Redis listeners.
pub type PubSubMessageTuple = (String, String, Vec<u8>, Option<String>);

/// Decoded Pub/Sub message delivered by [`RespListenerCore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespPubSubMessage {
    pub msg_type: String,
    pub pattern: Option<String>,
    pub channel: String,
    pub data: Bytes,
}

/// Backward-compatible alias for Valkey callers.
pub type ValkeyPubSubMessage = RespPubSubMessage;

#[cfg(feature = "pyo3")]
impl RespPubSubMessage {
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

/// Drop-restoring guard that pushes an undelivered [`RespPubSubMessage`] back to the front of
/// the listener queue if the awaiting Python `Future` is cancelled before `map_ok` runs.
pub struct PendingDeliveryGuard {
    messages: Arc<SyncMutex<VecDeque<RespPubSubMessage>>>,
    msg_notify: Arc<Notify>,
    msg: Option<RespPubSubMessage>,
}

impl PendingDeliveryGuard {
    #[must_use]
    pub fn new(
        messages: Arc<SyncMutex<VecDeque<RespPubSubMessage>>>,
        msg_notify: Arc<Notify>,
        msg: RespPubSubMessage,
    ) -> Self {
        Self {
            messages,
            msg_notify,
            msg: Some(msg),
        }
    }

    /// Take ownership of the guarded message, disarming drop restoration.
    pub fn into_message(mut self) -> RespPubSubMessage {
        self.msg
            .take()
            .expect("PendingDeliveryGuard consumed exactly once")
    }

    /// Convert into a Python dictionary and disarm drop restoration only after conversion succeeds.
    #[cfg(feature = "pyo3")]
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

/// Resolve the target endpoint for a Pub/Sub listener across Sentinel, Cluster, and Standalone topologies.
pub async fn resolve_listener_target(
    config: &RespConfig,
    slot_hint: Option<u16>,
) -> Result<RespTarget, RespError> {
    if let Some(ref service_name) = config.sentinel_service_name {
        let sentinel_specs: Vec<String> = if !config.sentinel_nodes.is_empty() {
            config.sentinel_nodes.clone()
        } else {
            vec![format!("{}:{}", config.host, config.port)]
        };
        for spec in &sentinel_specs {
            let Ok((shost, sport)) = parse_sentinel_addr(spec, 26379) else {
                continue;
            };
            let starget = RespTarget::Tcp {
                host: shost,
                port: sport,
            };
            let Ok(mut sconn) =
                RespConnection::establish(&starget, config, false, false, None).await
            else {
                continue;
            };
            if let Some(ref pass) = config.sentinel_password {
                let mut cmd = vec![b"AUTH".to_vec()];
                if let Some(ref u) = config.sentinel_username
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
            let mtarget = RespTarget::Tcp {
                host: mhost,
                port: mport,
            };
            if !config.verify_sentinel_role {
                return Ok(mtarget);
            }
            let Ok(mut check_conn) =
                RespConnection::establish(&mtarget, config, false, true, None).await
            else {
                continue;
            };
            if let Ok(role_reply) = check_conn.execute_single(&[b"ROLE".to_vec()]).await
                && verify_role_is_primary(&role_reply).is_ok()
            {
                return Ok(mtarget);
            }
        }
        return Err(RespError::connection(
            "No Sentinel reported an active primary for Pub/Sub listener",
        ));
    }

    if config.cluster_enabled
        && let Some(slot) = slot_hint
    {
        let mut seeds = Vec::new();
        for spec in &config.cluster_nodes {
            if let Ok((h, p)) = parse_sentinel_addr(spec, 6379) {
                seeds.push((h.clone(), RespTarget::Tcp { host: h, port: p }));
            }
        }
        if seeds.is_empty() {
            seeds.push((config.host.clone(), config.target.clone()));
        }
        for (seed_host, seed) in seeds {
            if let Ok(mut conn) = RespConnection::establish(&seed, config, false, true, None).await
            {
                let topo_res = match conn
                    .execute_single(&[b"CLUSTER".to_vec(), b"SHARDS".to_vec()])
                    .await
                {
                    Ok(r) => ClusterTopology::from_cluster_shards(&r, config.tls),
                    Err(_) => match conn
                        .execute_single(&[b"CLUSTER".to_vec(), b"SLOTS".to_vec()])
                        .await
                    {
                        Ok(r) => ClusterTopology::from_cluster_slots_with_fallback(
                            &r,
                            Some(seed_host.as_str()),
                        ),
                        Err(e) => Err(e),
                    },
                };
                if let Ok(topo) = topo_res
                    && let Some((ep, _)) =
                        topo.route_for_slot(slot, is_read_only_command(b"GET"), false)
                {
                    return Ok(RespTarget::Tcp {
                        host: ep.host,
                        port: ep.port,
                    });
                }
            }
        }
    }

    Ok(config.target.clone())
}

struct ListenerSession {
    writer: WriteHalf<RespStream>,
    reader_task: tokio::task::JoinHandle<()>,
}

/// Core async Pub/Sub listener engine shared across sync and async Valkey PyO3 wrappers.
pub struct RespListenerCore {
    config: Arc<RespConfig>,
    session: Mutex<Option<ListenerSession>>,
    reader_abort: SyncMutex<Option<tokio::task::AbortHandle>>,
    messages: Arc<SyncMutex<VecDeque<RespPubSubMessage>>>,
    msg_notify: Arc<Notify>,
    ack_notify: Arc<Notify>,
    close_notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
}

/// Backward-compatible alias for Valkey callers.
pub type ValkeyListenerCore = RespListenerCore;

impl RespListenerCore {
    /// Create a disconnected Pub/Sub listener handle.
    #[must_use]
    pub fn new(config: Arc<RespConfig>) -> Arc<Self> {
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
    pub fn config(&self) -> &Arc<RespConfig> {
        &self.config
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Synchronously mark closed, abort reader task, and wake waiters.
    pub fn abort_sync(&self) {
        self.closed.store(true, Ordering::Release);
        if let Some(abort) = self.reader_abort.lock().take() {
            abort.abort();
        }
        self.ack_notify.notify_waiters();
        self.close_notify.notify_waiters();
        self.msg_notify.notify_waiters();
    }

    fn pop_message(&self) -> Option<RespPubSubMessage> {
        self.messages.lock().pop_front()
    }

    async fn ensure_connected(&self, slot_hint: Option<u16>) -> Result<(), RespError> {
        if self.is_closed() {
            return Err(RespError::interface("RESP listener is closed"));
        }
        let mut guard = self.session.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        let target = resolve_listener_target(&self.config, slot_hint).await?;
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
            return Err(RespError::interface("RESP listener is closed"));
        }

        *guard = Some(ListenerSession {
            writer,
            reader_task,
        });
        Ok(())
    }

    async fn send_sub_command(&self, verb: &str, items: &[String]) -> Result<(), RespError> {
        if items.is_empty() {
            return Ok(());
        }
        let slot_hint = if verb.eq_ignore_ascii_case("SSUBSCRIBE") {
            items.first().map(|ch| hash_slot(ch.as_bytes()))
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
                .ok_or_else(|| RespError::interface("RESP listener is closed"))?;
            tokio::time::timeout(self.config.command_timeout, async {
                sess.writer.write_all(&bytes).await?;
                sess.writer.flush().await
            })
            .await
            .map_err(|_| RespError::connection("Timed out sending subscription command"))?
            .map_err(RespError::from)?;
        }

        let _ = tokio::time::timeout(self.config.command_timeout, ack_notified).await;
        Ok(())
    }

    /// Issue `SUBSCRIBE`.
    pub async fn subscribe(&self, channels: &[String]) -> Result<(), RespError> {
        self.send_sub_command("SUBSCRIBE", channels).await
    }

    /// Issue `PSUBSCRIBE`.
    pub async fn psubscribe(&self, patterns: &[String]) -> Result<(), RespError> {
        self.send_sub_command("PSUBSCRIBE", patterns).await
    }

    /// Issue `SSUBSCRIBE`.
    pub async fn ssubscribe(&self, channels: &[String]) -> Result<(), RespError> {
        self.send_sub_command("SSUBSCRIBE", channels).await
    }

    /// Issue `UNSUBSCRIBE`.
    pub async fn unsubscribe(&self, channels: &[String]) -> Result<(), RespError> {
        if !self.is_connected().await {
            return Ok(());
        }
        self.send_unsub_command("UNSUBSCRIBE", channels).await
    }

    /// Issue `PUNSUBSCRIBE`.
    pub async fn punsubscribe(&self, patterns: &[String]) -> Result<(), RespError> {
        if !self.is_connected().await {
            return Ok(());
        }
        self.send_unsub_command("PUNSUBSCRIBE", patterns).await
    }

    /// Issue `SUNSUBSCRIBE`.
    pub async fn sunsubscribe(&self, channels: &[String]) -> Result<(), RespError> {
        if !self.is_connected().await {
            return Ok(());
        }
        self.send_unsub_command("SUNSUBSCRIBE", channels).await
    }

    async fn is_connected(&self) -> bool {
        self.session.lock().await.is_some()
    }

    async fn send_unsub_command(&self, verb: &str, items: &[String]) -> Result<(), RespError> {
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
            .map_err(|_| RespError::connection("Timed out sending unsubscribe command"))?
            .map_err(RespError::from)?;
        }
        let _ = tokio::time::timeout(self.config.command_timeout, ack_notified).await;
        Ok(())
    }

    /// Wait for the next delivered message or return `Ok(None)` on timeout/close.
    pub async fn next_message(
        &self,
        timeout_ms: Option<u64>,
    ) -> Result<Option<RespPubSubMessage>, RespError> {
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
    ) -> Result<Option<PendingDeliveryGuard>, RespError> {
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
    pub async fn drain_messages(&self, max_messages: usize) -> Vec<RespPubSubMessage> {
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

impl Drop for RespListenerCore {
    fn drop(&mut self) {
        self.abort_sync();
    }
}

async fn run_listener_reader(
    mut reader: ReadHalf<RespStream>,
    mut read_buf: BytesMut,
    messages: Arc<SyncMutex<VecDeque<RespPubSubMessage>>>,
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

enum ParsedPubSubFrame {
    Ack,
    Message(RespPubSubMessage),
    Error(String),
    Other,
}

fn classify_pubsub_frame(frame: RespValue) -> ParsedPubSubFrame {
    let unattr = frame.into_unattributed();
    match unattr {
        RespValue::SimpleError(msg) | RespValue::BulkError(msg) => ParsedPubSubFrame::Error(msg),
        RespValue::Push(items) | RespValue::Array(items) => {
            let Some(kind) = items
                .first()
                .and_then(RespValue::as_str)
                .map(|s| s.to_ascii_lowercase())
            else {
                return ParsedPubSubFrame::Other;
            };
            match kind.as_str() {
                "subscribe" | "psubscribe" | "ssubscribe" | "unsubscribe" | "punsubscribe"
                | "sunsubscribe" => ParsedPubSubFrame::Ack,
                "message" | "smessage" => {
                    if items.len() < 3 {
                        return ParsedPubSubFrame::Other;
                    }
                    let Some(channel) = items[1].as_str().map(std::borrow::Cow::into_owned) else {
                        return ParsedPubSubFrame::Other;
                    };
                    let data = extract_payload_bytes(&items[2]);
                    ParsedPubSubFrame::Message(RespPubSubMessage {
                        msg_type: kind,
                        pattern: None,
                        channel,
                        data,
                    })
                }
                "pmessage" => {
                    if items.len() < 4 {
                        return ParsedPubSubFrame::Other;
                    }
                    let Some(pattern) = items[1].as_str().map(std::borrow::Cow::into_owned) else {
                        return ParsedPubSubFrame::Other;
                    };
                    let Some(channel) = items[2].as_str().map(std::borrow::Cow::into_owned) else {
                        return ParsedPubSubFrame::Other;
                    };
                    let data = extract_payload_bytes(&items[3]);
                    ParsedPubSubFrame::Message(RespPubSubMessage {
                        msg_type: kind,
                        pattern: Some(pattern),
                        channel,
                        data,
                    })
                }
                _ => ParsedPubSubFrame::Other,
            }
        }
        _ => ParsedPubSubFrame::Other,
    }
}

fn parse_pubsub_frame(frame: RespValue, ack_notify: &Notify) -> Option<RespPubSubMessage> {
    match classify_pubsub_frame(frame) {
        ParsedPubSubFrame::Ack => {
            ack_notify.notify_waiters();
            None
        }
        ParsedPubSubFrame::Message(msg) => Some(msg),
        ParsedPubSubFrame::Error(_) | ParsedPubSubFrame::Other => None,
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

/// Extract variadic string or single sequence-of-strings argument for PyO3 listener/stream methods.
#[cfg(feature = "pyo3")]
pub fn extract_string_args(args: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
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

// --- Command-Driven Pub/Sub Listener (for SyncRedisListener / AsyncRedisListener) ---

enum PubSubCommand {
    Control {
        verb: &'static str,
        items: Vec<String>,
        is_unsubscribe: bool,
        op_id: usize,
    },
    Close,
}

impl PubSubCommand {
    fn op_id(&self) -> Option<usize> {
        match self {
            Self::Control { op_id, .. } => Some(*op_id),
            Self::Close => None,
        }
    }
}

struct CommandListenerState {
    messages: SyncMutex<VecDeque<PubSubMessageTuple>>,
    msg_condvar: Condvar,
    completions: SyncMutex<VecDeque<(usize, bool, Option<String>)>>,
    comp_condvar: Condvar,
    dial_error: SyncMutex<Option<String>>,
    notifier: Option<Notifier>,
    next_op_id: AtomicUsize,
    closed: AtomicBool,
}

impl CommandListenerState {
    fn new(with_notifier: bool) -> Self {
        let notifier = if with_notifier {
            Notifier::new().ok()
        } else {
            None
        };
        Self {
            messages: SyncMutex::new(VecDeque::new()),
            msg_condvar: Condvar::new(),
            completions: SyncMutex::new(VecDeque::new()),
            comp_condvar: Condvar::new(),
            dial_error: SyncMutex::new(None),
            notifier,
            next_op_id: AtomicUsize::new(1),
            closed: AtomicBool::new(false),
        }
    }

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

    fn wait_completion(&self, op_id: usize, timeout: Duration) -> Result<(), RespError> {
        let deadline = Instant::now() + timeout;
        let mut guard = self.completions.lock();
        loop {
            if let Some(idx) = guard.iter().position(|(id, _, _)| *id == op_id) {
                let (_, ok, err) = guard.remove(idx).expect("index valid");
                if ok {
                    return Ok(());
                }
                return Err(RespError::operational(
                    err.as_deref().unwrap_or("RESP subscription failed"),
                    false,
                ));
            }
            if self.closed.load(Ordering::Acquire) {
                if let Some(dial_err) = self.dial_error.lock().clone() {
                    return Err(RespError::operational(dial_err, true));
                }
                return Err(RespError::interface("Listener closed"));
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RespError::operational(
                    "Subscription confirmation timed out",
                    true,
                ));
            }
            let res = self.comp_condvar.wait_for(&mut guard, deadline - now);
            if res.timed_out() {
                if let Some(idx) = guard.iter().position(|(id, _, _)| *id == op_id) {
                    let (_, ok, err) = guard.remove(idx).expect("index valid");
                    if ok {
                        return Ok(());
                    }
                    return Err(RespError::operational(
                        err.as_deref().unwrap_or("RESP subscription failed"),
                        false,
                    ));
                }
                return Err(RespError::operational(
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
    state: &CommandListenerState,
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

async fn write_control_cmd(
    writer: &mut WriteHalf<RespStream>,
    timeout: Duration,
    verb: &str,
    items: &[String],
) -> Result<(), RespError> {
    let mut cmd = Vec::with_capacity(1 + items.len());
    cmd.push(verb.as_bytes().to_vec());
    for item in items {
        cmd.push(item.as_bytes().to_vec());
    }
    let mut buf = BytesMut::new();
    encode_owned_command(&mut buf, &cmd);
    let bytes = buf.freeze();
    tokio::time::timeout(timeout, async {
        writer.write_all(&bytes).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| RespError::connection("Timed out sending Pub/Sub control command"))?
    .map_err(RespError::from)
}

fn drain_decoded_frames(
    read_buf: &mut BytesMut,
    state: &CommandListenerState,
    pending_acks: &mut VecDeque<(usize, usize)>,
) -> Result<(), RespError> {
    while let Some(frame) = try_decode_frame(read_buf)? {
        match classify_pubsub_frame(frame) {
            ParsedPubSubFrame::Ack => {
                let completed_id = if let Some(front) = pending_acks.front_mut() {
                    front.1 = front.1.saturating_sub(1);
                    if front.1 == 0 {
                        pending_acks.pop_front().map(|(id, _)| id)
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(id) = completed_id {
                    state.push_completion(id, true, None);
                }
            }
            ParsedPubSubFrame::Message(msg) => {
                state.push_message((msg.msg_type, msg.channel, msg.data.to_vec(), msg.pattern));
            }
            ParsedPubSubFrame::Error(err_msg) => {
                if let Some((id, _)) = pending_acks.pop_front() {
                    state.push_completion(id, false, Some(err_msg));
                }
            }
            ParsedPubSubFrame::Other => {}
        }
    }
    Ok(())
}

async fn run_command_listener_loop(
    config: Arc<RespConfig>,
    state: Arc<CommandListenerState>,
    mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<PubSubCommand>,
) {
    let first_cmd = loop {
        match cmd_rx.recv().await {
            Some(PubSubCommand::Close) | None => {
                state.mark_closed();
                return;
            }
            Some(PubSubCommand::Control {
                is_unsubscribe: true,
                op_id,
                ..
            }) => {
                state.push_completion(op_id, true, None);
            }
            Some(PubSubCommand::Control {
                items,
                is_unsubscribe: false,
                op_id,
                ..
            }) if items.is_empty() => {
                state.push_completion(op_id, true, None);
            }
            Some(cmd @ PubSubCommand::Control { .. }) => break cmd,
        }
    };

    let slot_hint = match &first_cmd {
        PubSubCommand::Control { verb, items, .. } if verb.eq_ignore_ascii_case("SSUBSCRIBE") => {
            items.first().map(|ch| hash_slot(ch.as_bytes()))
        }
        _ => None,
    };

    let target = match resolve_listener_target(&config, slot_hint).await {
        Ok(t) => t,
        Err(e) => {
            if let Some(id) = first_cmd.op_id() {
                state.push_completion(id, false, Some(e.message.clone()));
            }
            fail_pubsub_dial(&state, &mut cmd_rx, e.message);
            return;
        }
    };

    let conn = match RespConnection::establish(&target, &config, false, true, None).await {
        Ok(c) => c,
        Err(e) => {
            if let Some(id) = first_cmd.op_id() {
                state.push_completion(id, false, Some(e.message.clone()));
            }
            fail_pubsub_dial(&state, &mut cmd_rx, e.message);
            return;
        }
    };

    let (mut reader, mut writer, mut read_buf) = conn.into_split();
    let mut pending_acks: VecDeque<(usize, usize)> = VecDeque::new();
    let mut scratch = [0u8; 8192];

    if drain_decoded_frames(&mut read_buf, &state, &mut pending_acks).is_err() {
        if let Some(id) = first_cmd.op_id() {
            state.push_completion(id, false, Some("Protocol decode error".to_string()));
        }
        fail_pubsub_dial(&state, &mut cmd_rx, "Protocol decode error".to_string());
        return;
    }

    if let PubSubCommand::Control {
        verb, items, op_id, ..
    } = first_cmd
    {
        if let Err(e) = write_control_cmd(&mut writer, config.command_timeout, verb, &items).await {
            state.push_completion(op_id, false, Some(e.message.clone()));
            fail_pubsub_dial(&state, &mut cmd_rx, e.message);
            return;
        }
        pending_acks.push_back((op_id, items.len().max(1)));
    }

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(PubSubCommand::Control { verb, items, is_unsubscribe, op_id }) => {
                        if !is_unsubscribe && items.is_empty() {
                            state.push_completion(op_id, true, None);
                            continue;
                        }
                        if let Err(e) = write_control_cmd(&mut writer, config.command_timeout, verb, &items).await {
                            state.push_completion(op_id, false, Some(e.message));
                            break;
                        }
                        pending_acks.push_back((op_id, items.len().max(1)));
                    }
                    Some(PubSubCommand::Close) | None => {
                        let _ = writer.shutdown().await;
                        break;
                    }
                }
            }
            read_res = reader.read(&mut scratch) => {
                match read_res {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        read_buf.put_slice(&scratch[..n]);
                        if drain_decoded_frames(&mut read_buf, &state, &mut pending_acks).is_err() {
                            break;
                        }
                    }
                }
            }
        }
    }

    while let Some((id, _)) = pending_acks.pop_front() {
        state.push_completion(id, false, Some("Listener closed".to_string()));
    }
    cmd_rx.close();
    while let Ok(cmd) = cmd_rx.try_recv() {
        if let Some(id) = cmd.op_id() {
            state.push_completion(id, false, Some("Listener closed".to_string()));
        }
    }
    state.mark_closed();
}

/// Pure-Rust command-driven Pub/Sub listener with optional OS `Notifier` fd wakeup and completion queue.
///
/// Powers `SyncRedisListener` and `AsyncRedisListener` without any external `redis` crate dependency.
pub struct CommandDrivenListener {
    state: Arc<CommandListenerState>,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<PubSubCommand>,
    task_abort: SyncMutex<Option<tokio::task::AbortHandle>>,
}

impl CommandDrivenListener {
    /// Spawn a new command-driven Pub/Sub listener on `handle`.
    #[must_use]
    pub fn new(
        config: Arc<RespConfig>,
        with_notifier: bool,
        handle: &tokio::runtime::Handle,
    ) -> Self {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let state = Arc::new(CommandListenerState::new(with_notifier));
        let st_clone = Arc::clone(&state);
        let jh = handle.spawn(async move {
            run_command_listener_loop(config, st_clone, cmd_rx).await;
        });
        Self {
            state,
            cmd_tx,
            task_abort: SyncMutex::new(Some(jh.abort_handle())),
        }
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    fn submit_control(
        &self,
        verb: &'static str,
        items: Vec<String>,
        is_unsubscribe: bool,
    ) -> usize {
        let op_id = self.state.next_op_id.fetch_add(1, Ordering::Relaxed);
        let cmd = PubSubCommand::Control {
            verb,
            items,
            is_unsubscribe,
            op_id,
        };
        if self.is_closed() || self.cmd_tx.send(cmd).is_err() {
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

    pub fn submit_subscribe(&self, channels: Vec<String>) -> usize {
        self.submit_control("SUBSCRIBE", channels, false)
    }

    pub fn submit_psubscribe(&self, patterns: Vec<String>) -> usize {
        self.submit_control("PSUBSCRIBE", patterns, false)
    }

    pub fn submit_ssubscribe(&self, shard_channels: Vec<String>) -> usize {
        self.submit_control("SSUBSCRIBE", shard_channels, false)
    }

    pub fn submit_unsubscribe(&self, channels: Vec<String>) -> usize {
        self.submit_control("UNSUBSCRIBE", channels, true)
    }

    pub fn submit_punsubscribe(&self, patterns: Vec<String>) -> usize {
        self.submit_control("PUNSUBSCRIBE", patterns, true)
    }

    pub fn submit_sunsubscribe(&self, shard_channels: Vec<String>) -> usize {
        self.submit_control("SUNSUBSCRIBE", shard_channels, true)
    }

    pub fn wait_completion(&self, op_id: usize, timeout: Duration) -> Result<(), RespError> {
        self.state.wait_completion(op_id, timeout)
    }

    pub fn next_message_blocking(&self, timeout_ms: Option<u64>) -> Option<PubSubMessageTuple> {
        self.state.next_message_blocking(timeout_ms)
    }

    pub fn drain_messages(&self) -> Vec<PubSubMessageTuple> {
        self.state.messages.lock().drain(..).collect()
    }

    pub fn drain_completions(&self) -> Vec<(usize, bool, Option<String>)> {
        self.state.completions.lock().drain(..).collect()
    }

    pub fn cancel_operation(&self, _op_id: usize) -> bool {
        true
    }

    #[must_use]
    pub fn notifier_fd(&self) -> i32 {
        #[cfg(unix)]
        {
            self.state.notifier.as_ref().map_or(-1, Notifier::as_raw_fd)
        }
        #[cfg(not(unix))]
        {
            -1
        }
    }

    pub fn close(&self) {
        let _ = self.cmd_tx.send(PubSubCommand::Close);
        self.state.mark_closed();
        if let Some(abort) = self.task_abort.lock().take() {
            abort.abort();
        }
    }
}

impl Drop for CommandDrivenListener {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completion_ack_does_not_wake_next_message_with_spurious_none() {
        let state = Arc::new(CommandListenerState::new(false));

        let state_reader = Arc::clone(&state);
        let reader = std::thread::spawn(move || state_reader.next_message_blocking(None));

        std::thread::sleep(Duration::from_millis(15));
        state.push_completion(1, true, None);
        state.msg_condvar.notify_all();
        std::thread::sleep(Duration::from_millis(15));

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

        let timeout_res = state.wait_completion(999, Duration::from_millis(20));
        assert!(timeout_res.is_err());
    }
}
