//! Native Oracle AQ / TxEventQ notification listener and publisher (`vertebra._oracle`).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::oneshot;
use vertebra_db::py_async::{TaskAwaitable, spawn_rust_db_task_with_attr};

use crate::client::{
    BindArgs, ORACLE_COMPLETION_DRAINER_ATTR, OracleBreakHandle, OracleWorkerHandle, OutBindSpec,
    OutBindsPayload, WorkerCommand, WorkerReply,
};
use crate::config::NativeOracleConfig;
use crate::dialer::connect_oracledb;
use crate::errors::{OracleError, extract_ora_code};
use crate::types::OracleCellValue;

/// Validate an Oracle AQ queue or consumer identifier against `^[A-Za-z_][A-Za-z0-9_$#]{0,127}$`.
pub fn validate_aq_identifier(name: &str, kind: &str) -> Result<(), OracleError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 128 {
        return Err(OracleError::InvalidIdentifier(format!(
            "Invalid Oracle AQ {kind} identifier `{name}`; expected ^[A-Za-z_][A-Za-z0-9_$#]{{0,127}}$"
        )));
    }
    if !matches!(bytes[0], b'A'..=b'Z' | b'a'..=b'z' | b'_') {
        return Err(OracleError::InvalidIdentifier(format!(
            "Invalid Oracle AQ {kind} identifier `{name}`; expected ^[A-Za-z_][A-Za-z0-9_$#]{{0,127}}$"
        )));
    }
    if !bytes[1..]
        .iter()
        .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'$' | b'#'))
    {
        return Err(OracleError::InvalidIdentifier(format!(
            "Invalid Oracle AQ {kind} identifier `{name}`; expected ^[A-Za-z_][A-Za-z0-9_$#]{{0,127}}$"
        )));
    }
    Ok(())
}

/// Return `true` when `err` represents an idle `DBMS_AQ.DEQUEUE` wait timeout (`ORA-25228`).
#[must_use]
pub fn is_idle_dequeue_timeout(err: &OracleError) -> bool {
    match err {
        OracleError::Server { code: 25228, .. } => true,
        other => extract_ora_code(&other.to_string()) == Some(25228),
    }
}

/// Immutable AQ / TxEventQ notification frame delivered by [`AsyncOracleListener`].
#[pyclass(
    name = "OracleNotification",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleNotification {
    #[pyo3(get)]
    pub queue_name: String,
    #[pyo3(get)]
    pub consumer_name: Option<String>,
    pub msg_id_bytes: Vec<u8>,
    pub payload_bytes: Vec<u8>,
    #[pyo3(get)]
    pub enqueued_at_epoch_ms: Option<i64>,
}

#[pymethods]
impl OracleNotification {
    #[new]
    #[pyo3(signature = (queue_name, msg_id, payload, consumer_name=None, enqueued_at_epoch_ms=None))]
    pub fn new(
        queue_name: String,
        msg_id: Vec<u8>,
        payload: Vec<u8>,
        consumer_name: Option<String>,
        enqueued_at_epoch_ms: Option<i64>,
    ) -> Self {
        Self {
            queue_name,
            consumer_name,
            msg_id_bytes: msg_id,
            payload_bytes: payload,
            enqueued_at_epoch_ms,
        }
    }

    #[getter]
    pub fn msg_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.msg_id_bytes)
    }

    #[getter]
    pub fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.payload_bytes)
    }
}

/// Bounded FIFO ring buffer enforcing `oldest-drop` backpressure when capacity is reached.
#[derive(Debug)]
pub struct ListenerRingBuffer {
    max_buffered: usize,
    frames: VecDeque<OracleNotification>,
    dropped_count: usize,
}

impl ListenerRingBuffer {
    /// Construct a new bounded ring buffer with capacity `max_buffered.max(1)`.
    #[must_use]
    pub fn new(max_buffered: usize) -> Self {
        let cap = max_buffered.max(1);
        Self {
            max_buffered: cap,
            frames: VecDeque::with_capacity(cap.min(1024)),
            dropped_count: 0,
        }
    }

    /// Push a notification frame, evicting the oldest frame if at `max_buffered`.
    pub fn push(&mut self, notification: OracleNotification) {
        if self.frames.len() >= self.max_buffered {
            let _ = self.frames.pop_front();
            self.dropped_count = self.dropped_count.saturating_add(1);
        }
        self.frames.push_back(notification);
    }

    /// Pop the next oldest notification frame if available.
    pub fn pop(&mut self) -> Option<OracleNotification> {
        self.frames.pop_front()
    }

    /// Return the number of frames dropped due to backpressure.
    #[must_use]
    pub fn dropped_count(&self) -> usize {
        self.dropped_count
    }

    /// Return the current number of buffered frames.
    #[must_use]
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Return `true` if no frames are currently buffered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

type WaiterSender = oneshot::Sender<Result<Option<OracleNotification>, OracleError>>;
type WaiterReceiver = oneshot::Receiver<Result<Option<OracleNotification>, OracleError>>;

enum PollNotification {
    Ready(Option<OracleNotification>),
    Wait(WaiterReceiver),
    Fatal(OracleError),
}

struct ListenerLockedState {
    ring: ListenerRingBuffer,
    waiters: VecDeque<WaiterSender>,
    fatal_error: Option<OracleError>,
}

pub(crate) struct ListenerSharedState {
    closed: AtomicBool,
    dropped_count: AtomicUsize,
    inner: Mutex<ListenerLockedState>,
    wake_cvar: Condvar,
}

impl ListenerSharedState {
    fn new(max_buffered: usize) -> Self {
        Self {
            closed: AtomicBool::new(false),
            dropped_count: AtomicUsize::new(0),
            inner: Mutex::new(ListenerLockedState {
                ring: ListenerRingBuffer::new(max_buffered),
                waiters: VecDeque::new(),
                fatal_error: None,
            }),
            wake_cvar: Condvar::new(),
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn dropped_count(&self) -> usize {
        self.dropped_count.load(Ordering::Acquire)
    }

    pub(crate) fn deliver_frame(&self, frame: OracleNotification) {
        if self.is_closed() {
            return;
        }
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        while let Some(tx) = guard.waiters.pop_front() {
            if !tx.is_closed() {
                if let Err(Ok(Some(returned))) = tx.send(Ok(Some(frame))) {
                    guard.ring.push(returned);
                    self.dropped_count
                        .store(guard.ring.dropped_count(), Ordering::Release);
                }
                self.wake_cvar.notify_all();
                return;
            }
        }
        guard.ring.push(frame);
        self.dropped_count
            .store(guard.ring.dropped_count(), Ordering::Release);
        self.wake_cvar.notify_all();
    }

    pub(crate) fn record_fatal_error(&self, err: OracleError) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.fatal_error = Some(err.clone());
        while let Some(tx) = guard.waiters.pop_front() {
            let _ = tx.send(Err(err.clone()));
        }
        self.wake_cvar.notify_all();
    }

    fn mark_closed(&self) -> bool {
        let was_closed = self.closed.swap(true, Ordering::AcqRel);
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        while let Some(tx) = guard.waiters.pop_front() {
            let _ = tx.send(Ok(None));
        }
        self.wake_cvar.notify_all();
        !was_closed
    }

    fn poll_or_register_waiter(&self) -> PollNotification {
        if self.is_closed() {
            return PollNotification::Ready(None);
        }
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(err) = guard.fatal_error.take() {
            return PollNotification::Fatal(err);
        }
        if let Some(frame) = guard.ring.pop() {
            return PollNotification::Ready(Some(frame));
        }
        if self.is_closed() {
            return PollNotification::Ready(None);
        }
        let (tx, rx) = oneshot::channel();
        guard.waiters.push_back(tx);
        PollNotification::Wait(rx)
    }
}

fn dequeue_plsql_sql() -> &'static str {
    concat!(
        "DECLARE ",
        "  l_opt DBMS_AQ.DEQUEUE_OPTIONS_T; ",
        "  l_prop DBMS_AQ.MESSAGE_PROPERTIES_T; ",
        "  l_msgid RAW(16); ",
        "  l_payload RAW(32767); ",
        "BEGIN ",
        "  l_opt.wait := :wait_sec; ",
        "  l_opt.navigation := DBMS_AQ.NEXT_MESSAGE; ",
        "  IF :consumer IS NOT NULL THEN ",
        "    l_opt.consumer_name := :consumer; ",
        "  END IF; ",
        "  DBMS_AQ.DEQUEUE(",
        "    queue_name => :queue_name, ",
        "    dequeue_options => l_opt, ",
        "    message_properties => l_prop, ",
        "    payload => l_payload, ",
        "    msgid => l_msgid",
        "  ); ",
        "  COMMIT; ",
        "  :out_msgid := RAWTOHEX(l_msgid); ",
        "  :out_payload := l_payload; ",
        "END;"
    )
}

fn enqueue_plsql_sql() -> &'static str {
    concat!(
        "DECLARE ",
        "  l_opt DBMS_AQ.ENQUEUE_OPTIONS_T; ",
        "  l_prop DBMS_AQ.MESSAGE_PROPERTIES_T; ",
        "  l_msgid RAW(16); ",
        "BEGIN ",
        "  IF :correlation_id IS NOT NULL THEN ",
        "    l_prop.correlation := :correlation_id; ",
        "  END IF; ",
        "  DBMS_AQ.ENQUEUE(",
        "    queue_name => :queue_name, ",
        "    enqueue_options => l_opt, ",
        "    message_properties => l_prop, ",
        "    payload => :payload, ",
        "    msgid => l_msgid",
        "  ); ",
        "  COMMIT; ",
        "  :out_msgid := RAWTOHEX(l_msgid); ",
        "END;"
    )
}

trait LoopClock {
    fn now(&self) -> Instant;
}

struct SystemLoopClock;

impl LoopClock for SystemLoopClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

trait AqDequeueSession {
    fn dequeue_slice(
        &mut self,
        queue_name: &str,
        consumer_name: Option<&str>,
        wait_sec: i64,
    ) -> Result<OracleNotification, OracleError>;

    fn ping_session(&mut self) -> Result<(), OracleError>;

    fn close_session(self: Box<Self>);
}

struct LiveAqSession {
    conn: oracledb::Connection,
    break_handle: Arc<OracleBreakHandle>,
    sql: &'static str,
    out_payload_buf: Vec<u8>,
}

impl AqDequeueSession for LiveAqSession {
    fn dequeue_slice(
        &mut self,
        queue_name: &str,
        consumer_name: Option<&str>,
        wait_sec: i64,
    ) -> Result<OracleNotification, OracleError> {
        let consumer_owned = consumer_name.map(str::to_owned);
        let queue_owned = queue_name.to_owned();
        self.break_handle.begin_operation();
        let res = self.conn.execute_named(
            self.sql,
            &[
                ("wait_sec", &wait_sec as &dyn oracledb::ToDbValue),
                ("consumer", &consumer_owned as &dyn oracledb::ToDbValue),
                ("queue_name", &queue_owned as &dyn oracledb::ToDbValue),
                (
                    "out_msgid",
                    &oracledb::DB_TYPE_VARCHAR as &dyn oracledb::ToDbValue,
                ),
                (
                    "out_payload",
                    &self.out_payload_buf as &dyn oracledb::ToDbValue,
                ),
            ],
        );
        self.break_handle.end_operation();

        match res {
            Ok(mut exec_res) => {
                let out_row = exec_res.out_bind_data();
                let mut msg_id_bytes = Vec::new();
                let mut payload_bytes = Vec::new();
                for (idx, meta) in out_row.columns().to_vec().iter().enumerate() {
                    if meta.name().eq_ignore_ascii_case("out_msgid") {
                        if let Ok(Some(s)) = out_row.get::<Option<String>>(idx) {
                            msg_id_bytes = s.into_bytes();
                        }
                    } else if meta.name().eq_ignore_ascii_case("out_payload")
                        && let Ok(Some(raw)) = out_row.get::<Option<Vec<u8>>>(idx)
                    {
                        payload_bytes = raw;
                    }
                }
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .and_then(|d| i64::try_from(d.as_millis()).ok());
                Ok(OracleNotification {
                    queue_name: queue_owned,
                    consumer_name: consumer_owned,
                    msg_id_bytes,
                    payload_bytes,
                    enqueued_at_epoch_ms: now_ms,
                })
            }
            Err(raw_err) => Err(OracleError::from_oracledb(&raw_err)),
        }
    }

    fn ping_session(&mut self) -> Result<(), OracleError> {
        self.break_handle.begin_operation();
        let res = self.conn.query("SELECT 1 FROM DUAL", &[]);
        self.break_handle.end_operation();
        res.map(|_| ())
            .map_err(|raw_err| OracleError::from_oracledb(&raw_err))
    }

    fn close_session(mut self: Box<Self>) {
        self.break_handle.set_callback(None);
        let _ = self.conn.close();
    }
}

fn run_aq_loop(
    mut session: Box<dyn AqDequeueSession>,
    queue_name: &str,
    consumer_name: Option<&str>,
    wait_timeout_seconds: u32,
    state: &ListenerSharedState,
    clock: &dyn LoopClock,
) {
    // Slice server-blocking DBMS_AQ.DEQUEUE waits into 1-second kernel waits:
    // enqueued messages wake immediately (<1ms), ORA-25228 tracks cumulative
    // idle elapsed against `wait_timeout_seconds` to run `SELECT 1 FROM DUAL`,
    // and close()/Drop exits cleanly in <=1s without cancelling SYS.DBMS_AQ.DEQUEUE.
    let wait_budget = Duration::from_secs(u64::from(wait_timeout_seconds.max(1)));
    let wait_sec = 1_i64;
    let mut idle_since = clock.now();

    while !state.is_closed() {
        match session.dequeue_slice(queue_name, consumer_name, wait_sec) {
            Ok(frame) => {
                if state.is_closed() {
                    break;
                }
                idle_since = clock.now();
                state.deliver_frame(frame);
            }
            Err(ora_err) => {
                if state.is_closed() {
                    break;
                }
                if is_idle_dequeue_timeout(&ora_err) {
                    if clock.now().saturating_duration_since(idle_since) >= wait_budget {
                        match session.ping_session() {
                            Ok(()) => {
                                idle_since = clock.now();
                            }
                            Err(ping_err) => {
                                if !state.is_closed() {
                                    state.record_fatal_error(ping_err);
                                }
                                break;
                            }
                        }
                    }
                    continue;
                }
                state.record_fatal_error(ora_err);
                break;
            }
        }
    }

    session.close_session();
}

fn run_live_aq_worker(
    config: Arc<NativeOracleConfig>,
    queue_name: String,
    consumer_name: Option<String>,
    wait_timeout_seconds: u32,
    state: Arc<ListenerSharedState>,
    break_handle: Arc<OracleBreakHandle>,
) {
    let conn = match connect_oracledb(&config) {
        Ok(c) => c,
        Err(err) => {
            if !state.is_closed() {
                state.record_fatal_error(err);
            }
            return;
        }
    };

    let state_for_break = Arc::clone(&state);
    break_handle.set_callback(Some(Arc::new(move || {
        state_for_break.wake_cvar.notify_all();
    })));

    let session = Box::new(LiveAqSession {
        conn,
        break_handle,
        sql: dequeue_plsql_sql(),
        out_payload_buf: vec![0u8; 32767],
    });
    let clock = SystemLoopClock;
    run_aq_loop(
        session,
        &queue_name,
        consumer_name.as_deref(),
        wait_timeout_seconds,
        &state,
        &clock,
    );
}

/// Dedicated AQ / TxEventQ listener owning an isolated worker thread and bounded 1024-frame ring buffer.
#[pyclass(
    name = "AsyncOracleListener",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
pub struct AsyncOracleListener {
    queue_name: String,
    consumer_name: Option<String>,
    state: Arc<ListenerSharedState>,
    break_handle: Arc<OracleBreakHandle>,
    join_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl AsyncOracleListener {
    /// Spawn a new [`AsyncOracleListener`] on a dedicated `"vtb-ora-aq"` thread.
    pub fn spawn(
        config: Arc<NativeOracleConfig>,
        queue_name: String,
        consumer_name: Option<String>,
        wait_timeout_seconds: u32,
        max_buffered: usize,
    ) -> Result<Self, OracleError> {
        validate_aq_identifier(&queue_name, "queue_name")?;
        if let Some(ref consumer) = consumer_name {
            validate_aq_identifier(consumer, "consumer_name")?;
        }
        if max_buffered == 0 {
            return Err(OracleError::Config(
                "AsyncOracleListener max_buffered must be >= 1".to_owned(),
            ));
        }

        let normalized_queue = queue_name.to_ascii_uppercase();
        let state = Arc::new(ListenerSharedState::new(max_buffered));
        let break_handle = Arc::new(OracleBreakHandle::new());

        let state_clone = Arc::clone(&state);
        let break_clone = Arc::clone(&break_handle);
        let q_clone = normalized_queue.clone();
        let c_clone = consumer_name.clone();

        let join_handle = thread::Builder::new()
            .name("vtb-ora-aq".to_owned())
            .spawn(move || {
                run_live_aq_worker(
                    config,
                    q_clone,
                    c_clone,
                    wait_timeout_seconds,
                    state_clone,
                    break_clone,
                );
            })
            .map_err(|e| {
                OracleError::Interface(format!("failed to spawn Oracle AQ listener thread: {e}"))
            })?;

        Ok(Self {
            queue_name: normalized_queue,
            consumer_name,
            state,
            break_handle,
            join_handle: Arc::new(Mutex::new(Some(join_handle))),
        })
    }
}

#[pymethods]
impl AsyncOracleListener {
    #[getter]
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    #[getter]
    pub fn consumer_name(&self) -> Option<&str> {
        self.consumer_name.as_deref()
    }

    #[getter]
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    #[getter]
    pub fn dropped_count(&self) -> usize {
        self.state.dropped_count()
    }

    #[pyo3(signature = (timeout=None))]
    pub fn next_notification(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
    ) -> PyResult<TaskAwaitable> {
        if let Some(t) = timeout
            && (!t.is_finite() || t < 0.0)
        {
            return Err(OracleError::Config(
                "next_notification timeout must be a finite non-negative number".to_owned(),
            )
            .to_py_err(py));
        }

        let state = Arc::clone(&self.state);
        let timeout_dur = timeout.map(Duration::from_secs_f64);

        spawn_rust_db_task_with_attr(
            py,
            async move {
                match state.poll_or_register_waiter() {
                    PollNotification::Ready(frame_opt) => Ok(frame_opt),
                    PollNotification::Fatal(err) => Err(err),
                    PollNotification::Wait(rx) => {
                        if let Some(dur) = timeout_dur {
                            match tokio::time::timeout(dur, rx).await {
                                Ok(Ok(res)) => res,
                                Ok(Err(_)) => Ok(None),
                                Err(_) => Ok(None),
                            }
                        } else {
                            match rx.await {
                                Ok(res) => res,
                                Err(_) => Ok(None),
                            }
                        }
                    }
                }
            },
            |py, frame_opt| match frame_opt {
                Some(frame) => Ok(Py::new(py, frame)?.into_any()),
                None => Ok(py.None()),
            },
            |py, err| err.to_py_err(py),
            ORACLE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<TaskAwaitable> {
        let newly_closed = self.state.mark_closed();
        self.break_handle.break_execution();
        let join_arc = Arc::clone(&self.join_handle);

        spawn_rust_db_task_with_attr(
            py,
            async move {
                if newly_closed {
                    let handle_opt = {
                        let mut guard = join_arc.lock().unwrap_or_else(|e| e.into_inner());
                        guard.take()
                    };
                    if let Some(handle) = handle_opt {
                        let _ = tokio::task::spawn_blocking(move || {
                            let _ = handle.join();
                        })
                        .await;
                    }
                }
                Ok::<(), OracleError>(())
            },
            |py, ()| Ok(py.None()),
            |py, err| err.to_py_err(py),
            ORACLE_COMPLETION_DRAINER_ATTR,
        )
    }
}

impl Drop for AsyncOracleListener {
    fn drop(&mut self) {
        self.state.mark_closed();
        self.break_handle.break_execution();
        let handle_opt = {
            let mut guard = self.join_handle.lock().unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        if let Some(handle) = handle_opt {
            let _ = handle.join();
        }
    }
}

/// Dedicated AQ / TxEventQ synchronous listener owning an isolated worker thread and bounded ring buffer.
#[pyclass(
    name = "SyncOracleListener",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
pub struct SyncOracleListener {
    queue_name: String,
    consumer_name: Option<String>,
    state: Arc<ListenerSharedState>,
    break_handle: Arc<OracleBreakHandle>,
    join_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl SyncOracleListener {
    /// Spawn a new [`SyncOracleListener`] on a dedicated `"vtb-ora-aq-sync"` thread.
    pub fn spawn(
        config: Arc<NativeOracleConfig>,
        queue_name: String,
        consumer_name: Option<String>,
        wait_timeout_seconds: u32,
        max_buffered: usize,
    ) -> Result<Self, OracleError> {
        validate_aq_identifier(&queue_name, "queue_name")?;
        if let Some(ref consumer) = consumer_name {
            validate_aq_identifier(consumer, "consumer_name")?;
        }
        if max_buffered == 0 {
            return Err(OracleError::Config(
                "SyncOracleListener max_buffered must be >= 1".to_owned(),
            ));
        }

        let normalized_queue = queue_name.to_ascii_uppercase();
        let state = Arc::new(ListenerSharedState::new(max_buffered));
        let break_handle = Arc::new(OracleBreakHandle::new());

        let state_clone = Arc::clone(&state);
        let break_clone = Arc::clone(&break_handle);
        let q_clone = normalized_queue.clone();
        let c_clone = consumer_name.clone();

        let join_handle = thread::Builder::new()
            .name("vtb-ora-aq-sync".to_owned())
            .spawn(move || {
                run_live_aq_worker(
                    config,
                    q_clone,
                    c_clone,
                    wait_timeout_seconds,
                    state_clone,
                    break_clone,
                );
            })
            .map_err(|e| {
                OracleError::Interface(format!("failed to spawn Oracle AQ listener thread: {e}"))
            })?;

        Ok(Self {
            queue_name: normalized_queue,
            consumer_name,
            state,
            break_handle,
            join_handle: Arc::new(Mutex::new(Some(join_handle))),
        })
    }
}

#[pymethods]
impl SyncOracleListener {
    #[getter]
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    #[getter]
    pub fn consumer_name(&self) -> Option<&str> {
        self.consumer_name.as_deref()
    }

    #[getter]
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    #[getter]
    pub fn dropped_count(&self) -> usize {
        self.state.dropped_count()
    }

    #[pyo3(signature = (timeout=None))]
    pub fn next_notification(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
    ) -> PyResult<Option<OracleNotification>> {
        if let Some(t) = timeout
            && (!t.is_finite() || t < 0.0)
        {
            return Err(OracleError::Config(
                "next_notification timeout must be a finite non-negative number".to_owned(),
            )
            .to_py_err(py));
        }

        let state = Arc::clone(&self.state);
        let timeout_dur = timeout.map(Duration::from_secs_f64);

        let res = py.detach(|| {
            let start = Instant::now();
            let mut guard = state.inner.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if let Some(err) = guard.fatal_error.take() {
                    return Err(err);
                }
                if let Some(frame) = guard.ring.pop() {
                    return Ok(Some(frame));
                }
                if state.is_closed() {
                    return Ok(None);
                }
                match timeout_dur {
                    Some(dur) => {
                        let elapsed = start.elapsed();
                        if elapsed >= dur {
                            return Ok(None);
                        }
                        let remaining = dur - elapsed;
                        let (new_guard, timeout_res) = state
                            .wake_cvar
                            .wait_timeout(guard, remaining)
                            .unwrap_or_else(|e| e.into_inner());
                        guard = new_guard;
                        if timeout_res.timed_out()
                            && guard.ring.is_empty()
                            && guard.fatal_error.is_none()
                        {
                            return Ok(None);
                        }
                    }
                    None => {
                        guard = state
                            .wake_cvar
                            .wait(guard)
                            .unwrap_or_else(|e| e.into_inner());
                    }
                }
            }
        });

        res.map_err(|e| e.to_py_err(py))
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let newly_closed = self.state.mark_closed();
        self.break_handle.break_execution();
        if newly_closed {
            let handle_opt = {
                let mut guard = self.join_handle.lock().unwrap_or_else(|e| e.into_inner());
                guard.take()
            };
            if let Some(handle) = handle_opt {
                let _ = py.detach(|| handle.join());
            }
        }
        Ok(())
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_val=None, exc_tb=None))]
    pub fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_val: Option<&Bound<'_, PyAny>>,
        exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let _ = (exc_type, exc_val, exc_tb);
        self.close(py)?;
        Ok(false)
    }
}

impl Drop for SyncOracleListener {
    fn drop(&mut self) {
        self.state.mark_closed();
        self.break_handle.break_execution();
        let mut guard = self.join_handle.lock().unwrap_or_else(|e| e.into_inner());
        let _ = guard.take();
    }
}

/// Synchronously enqueue an AQ / TxEventQ notification on `worker`.
pub(crate) fn enqueue_notification_sync(
    worker: &OracleWorkerHandle,
    queue_name: &str,
    payload: &[u8],
    consumer_name: Option<&str>,
    correlation_id: Option<&str>,
) -> Result<Vec<u8>, OracleError> {
    validate_aq_identifier(queue_name, "queue_name")?;
    if let Some(consumer) = consumer_name {
        validate_aq_identifier(consumer, "consumer_name")?;
    }
    if worker.is_closed() {
        return Err(OracleError::Interface("Oracle client is closed".to_owned()));
    }

    let sql = enqueue_plsql_sql().to_owned();
    let params = BindArgs::Named(vec![
        (
            "queue_name".to_owned(),
            OracleCellValue::Str(queue_name.to_ascii_uppercase()),
        ),
        (
            "payload".to_owned(),
            OracleCellValue::Bytes(payload.to_vec()),
        ),
        (
            "correlation_id".to_owned(),
            match correlation_id {
                Some(c) => OracleCellValue::Str(c.to_owned()),
                None => OracleCellValue::Null,
            },
        ),
    ]);
    let out_binds = OutBindSpec::Named(vec![("out_msgid".to_owned(), "VARCHAR2".to_owned())]);
    let reply = worker.dispatch_sync(WorkerCommand::Execute {
        sql,
        params,
        out_binds,
        fetch_decimals: false,
        lowercase_column_names: true,
    })?;
    if let WorkerReply::QueryOutcome(outcome) = reply
        && let OutBindsPayload::Named(pairs) = outcome.out_binds
    {
        for (k, v) in pairs {
            if k.eq_ignore_ascii_case("out_msgid") {
                if let OracleCellValue::Str(s) = v {
                    return Ok(s.into_bytes());
                }
                if let OracleCellValue::Bytes(b) = v {
                    return Ok(b);
                }
            }
        }
    }
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::{
        AqDequeueSession, ListenerRingBuffer, ListenerSharedState, LoopClock, OracleNotification,
        is_idle_dequeue_timeout, run_aq_loop, validate_aq_identifier,
    };
    use crate::errors::{OracleError, OracleErrorKind};

    struct FakeLoopClock {
        base: Instant,
        elapsed_ms: Arc<AtomicU64>,
    }

    impl FakeLoopClock {
        fn new() -> Self {
            Self {
                base: Instant::now(),
                elapsed_ms: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl LoopClock for FakeLoopClock {
        fn now(&self) -> Instant {
            self.base + Duration::from_millis(self.elapsed_ms.load(Ordering::Acquire))
        }
    }

    enum ScriptedSlice {
        Timeout,
        Message(OracleNotification),
        CloseAfter,
    }

    struct ScriptedAqSession {
        slices: VecDeque<ScriptedSlice>,
        clock_ms: Arc<AtomicU64>,
        state: Arc<ListenerSharedState>,
        ping_calls: Arc<AtomicUsize>,
        fail_ping: bool,
        closed: Arc<AtomicBool>,
        observed_wait_secs: Vec<i64>,
    }

    impl AqDequeueSession for ScriptedAqSession {
        fn dequeue_slice(
            &mut self,
            _queue_name: &str,
            _consumer_name: Option<&str>,
            wait_sec: i64,
        ) -> Result<OracleNotification, OracleError> {
            self.observed_wait_secs.push(wait_sec);
            match self.slices.pop_front() {
                Some(ScriptedSlice::Timeout) => {
                    self.clock_ms.fetch_add(1000, Ordering::AcqRel);
                    Err(OracleError::Server {
                        code: 25228,
                        message: "ORA-25228: timeout or end-of-fetch".to_owned(),
                    })
                }
                Some(ScriptedSlice::Message(frame)) => Ok(frame),
                Some(ScriptedSlice::CloseAfter) | None => {
                    self.state.mark_closed();
                    Err(OracleError::Server {
                        code: 25228,
                        message: "ORA-25228: timeout or end-of-fetch".to_owned(),
                    })
                }
            }
        }

        fn ping_session(&mut self) -> Result<(), OracleError> {
            self.ping_calls.fetch_add(1, Ordering::AcqRel);
            if self.fail_ping {
                return Err(OracleError::Server {
                    code: 3113,
                    message: "ORA-03113: end-of-file on communication channel".to_owned(),
                });
            }
            Ok(())
        }

        fn close_session(self: Box<Self>) {
            self.closed.store(true, Ordering::Release);
            assert!(
                self.observed_wait_secs.iter().all(|&sec| sec == 1),
                "DBMS_AQ.DEQUEUE slice must remain 1s for responsive shutdown"
            );
        }
    }

    #[test]
    fn test_validate_aq_identifier_accepts_valid_and_rejects_injection() {
        assert!(validate_aq_identifier("VTB_AQ_ORDERS", "queue_name").is_ok());
        assert!(validate_aq_identifier("_vtb_consumer$1#", "consumer_name").is_ok());

        for invalid in [
            "",
            "1LEADING_DIGIT",
            "VTB_AQ_ORDERS; DROP TABLE users --",
            "VTB_AQ_ORDERS' OR '1'='1",
            "QUEUE WITH SPACES",
            "SCHEMA.QUEUE_DOT",
            &"A".repeat(129),
        ] {
            let err = validate_aq_identifier(invalid, "queue_name")
                .expect_err("invalid identifier must be rejected");
            let classified = err.classify();
            assert_eq!(classified.kind, OracleErrorKind::Programming);
            assert_eq!(classified.sqlstate, "42602");
            assert!(!classified.is_transient);
        }
    }

    #[test]
    fn test_listener_buffer_enforces_max_buffered_oldest_drop() {
        let mut ring = ListenerRingBuffer::new(1024);
        for i in 1..=1026_u32 {
            ring.push(OracleNotification {
                queue_name: "VTB_AQ_EVENTS".to_owned(),
                consumer_name: None,
                msg_id_bytes: format!("msg-{i}").into_bytes(),
                payload_bytes: format!("payload-{i}").into_bytes(),
                enqueued_at_epoch_ms: Some(i64::from(i)),
            });
        }

        assert_eq!(ring.len(), 1024);
        assert_eq!(ring.dropped_count(), 2);

        let first = ring.pop().expect("first retained frame");
        assert_eq!(first.msg_id_bytes, b"msg-3");
        assert_eq!(first.payload_bytes, b"payload-3");
        assert_eq!(first.enqueued_at_epoch_ms, Some(3));
    }

    #[test]
    fn test_ora_25228_is_classified_as_idle_dequeue_timeout() {
        let idle_timeout = OracleError::Server {
            code: 25228,
            message: "ORA-25228: timeout or end-of-fetch during message dequeue from VTB_AQ_EVENTS"
                .to_owned(),
        };
        assert!(is_idle_dequeue_timeout(&idle_timeout));

        let broken_pipe = OracleError::Server {
            code: 3113,
            message: "ORA-03113: end-of-file on communication channel".to_owned(),
        };
        assert!(!is_idle_dequeue_timeout(&broken_pipe));
        let classified = broken_pipe.classify();
        assert_eq!(classified.kind, OracleErrorKind::Operational);
        assert_eq!(classified.sqlstate, "08006");
        assert_eq!(classified.vendor_code, Some(3113));
        assert!(classified.is_transient);
    }

    #[test]
    fn test_wait_timeout_seconds_governs_idle_liveness_ping_frequency() {
        for (wait_timeout_seconds, expected_pings) in [(2_u32, 3_usize), (5_u32, 1_usize)] {
            let clock = FakeLoopClock::new();
            let state = Arc::new(ListenerSharedState::new(16));
            let ping_calls = Arc::new(AtomicUsize::new(0));
            let closed = Arc::new(AtomicBool::new(false));
            let slices = VecDeque::from([
                ScriptedSlice::Timeout,
                ScriptedSlice::Timeout,
                ScriptedSlice::Timeout,
                ScriptedSlice::Timeout,
                ScriptedSlice::Timeout,
                ScriptedSlice::Timeout,
                ScriptedSlice::CloseAfter,
            ]);
            let session = Box::new(ScriptedAqSession {
                slices,
                clock_ms: Arc::clone(&clock.elapsed_ms),
                state: Arc::clone(&state),
                ping_calls: Arc::clone(&ping_calls),
                fail_ping: false,
                closed: Arc::clone(&closed),
                observed_wait_secs: Vec::new(),
            });
            run_aq_loop(
                session,
                "VTB_AQ_EVENTS",
                None,
                wait_timeout_seconds,
                &state,
                &clock,
            );
            assert_eq!(ping_calls.load(Ordering::Acquire), expected_pings);
            assert!(closed.load(Ordering::Acquire));
        }
    }

    #[test]
    fn test_delivered_notification_resets_idle_timer_without_probing() {
        let clock = FakeLoopClock::new();
        let state = Arc::new(ListenerSharedState::new(16));
        let ping_calls = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let frame = OracleNotification {
            queue_name: "VTB_AQ_EVENTS".to_owned(),
            consumer_name: None,
            msg_id_bytes: b"m1".to_vec(),
            payload_bytes: b"p1".to_vec(),
            enqueued_at_epoch_ms: Some(1),
        };
        let slices = VecDeque::from([
            ScriptedSlice::Timeout,
            ScriptedSlice::Message(frame),
            ScriptedSlice::Timeout,
            ScriptedSlice::CloseAfter,
        ]);
        let session = Box::new(ScriptedAqSession {
            slices,
            clock_ms: Arc::clone(&clock.elapsed_ms),
            state: Arc::clone(&state),
            ping_calls: Arc::clone(&ping_calls),
            fail_ping: false,
            closed: Arc::clone(&closed),
            observed_wait_secs: Vec::new(),
        });
        run_aq_loop(session, "VTB_AQ_EVENTS", None, 2, &state, &clock);
        assert_eq!(ping_calls.load(Ordering::Acquire), 0);
        assert!(closed.load(Ordering::Acquire));
    }

    #[test]
    fn test_close_stops_aq_loop_within_single_slice() {
        let clock = FakeLoopClock::new();
        let state = Arc::new(ListenerSharedState::new(16));
        let ping_calls = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let slices = VecDeque::from([
            ScriptedSlice::CloseAfter,
            ScriptedSlice::Timeout,
            ScriptedSlice::Timeout,
        ]);
        let session = Box::new(ScriptedAqSession {
            slices,
            clock_ms: Arc::clone(&clock.elapsed_ms),
            state: Arc::clone(&state),
            ping_calls: Arc::clone(&ping_calls),
            fail_ping: false,
            closed: Arc::clone(&closed),
            observed_wait_secs: Vec::new(),
        });
        run_aq_loop(session, "VTB_AQ_EVENTS", None, 60, &state, &clock);
        assert_eq!(ping_calls.load(Ordering::Acquire), 0);
        assert!(closed.load(Ordering::Acquire));
    }

    #[test]
    fn test_failed_liveness_ping_records_fatal_error_and_closes() {
        let clock = FakeLoopClock::new();
        let state = Arc::new(ListenerSharedState::new(16));
        let ping_calls = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let slices = VecDeque::from([
            ScriptedSlice::Timeout,
            ScriptedSlice::Timeout,
            ScriptedSlice::Timeout,
        ]);
        let session = Box::new(ScriptedAqSession {
            slices,
            clock_ms: Arc::clone(&clock.elapsed_ms),
            state: Arc::clone(&state),
            ping_calls: Arc::clone(&ping_calls),
            fail_ping: true,
            closed: Arc::clone(&closed),
            observed_wait_secs: Vec::new(),
        });
        run_aq_loop(session, "VTB_AQ_EVENTS", None, 2, &state, &clock);
        assert_eq!(ping_calls.load(Ordering::Acquire), 1);
        assert!(closed.load(Ordering::Acquire));
        let fatal = {
            let guard = state.inner.lock().unwrap_or_else(|e| e.into_inner());
            guard.fatal_error.clone()
        };
        let err = fatal.expect("failed ping must surface fatal error for reconnect");
        assert_eq!(err.classify().kind, OracleErrorKind::Operational);
        assert!(err.classify().is_transient);
    }
}
