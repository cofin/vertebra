//! Per-connection Rust worker thread, completion bridge, and out-of-band `break_execution()` cancellation.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pyo3::prelude::*;
use tokio::sync::oneshot;
use vertebra_core::notify::SyncCompletionQueue;
use vertebra_db::py_async::{RustDbTaskAwaitable, spawn_rust_db_task_with_attr};

use crate::config::{NativeOracleConfig, RawOracleOptions};
use crate::dialer::connect_oracledb;
use crate::errors::OracleError;

/// Attribute name used to attach `vertebra._oracle`'s `AsyncCompletionQueue` drainer to an `asyncio` event loop.
pub const ORACLE_COMPLETION_DRAINER_ATTR: &str = "_vertebra_oracle_completion_drainer";

/// Validate an Oracle savepoint identifier against ASCII `[A-Za-z_][A-Za-z0-9_$#]*`.
pub fn validate_savepoint_identifier(name: &str) -> Result<(), OracleError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 128 {
        return Err(OracleError::Server {
            code: 900,
            message: format!("ORA-00900: invalid savepoint identifier `{name}`"),
        });
    }
    if !matches!(bytes[0], b'A'..=b'Z' | b'a'..=b'z' | b'_') {
        return Err(OracleError::Server {
            code: 900,
            message: format!("ORA-00900: invalid savepoint identifier `{name}`"),
        });
    }
    if !bytes[1..]
        .iter()
        .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'$' | b'#'))
    {
        return Err(OracleError::Server {
            code: 900,
            message: format!("ORA-00900: invalid savepoint identifier `{name}`"),
        });
    }
    Ok(())
}

type BreakCallback = Arc<dyn Fn() + Send + Sync>;

/// Out-of-band cancellation handle shared between the caller and the worker thread.
pub struct OracleBreakHandle {
    in_flight: AtomicBool,
    cancelled: AtomicBool,
    wait_lock: Mutex<()>,
    wait_cvar: Condvar,
    callback: Mutex<Option<BreakCallback>>,
}

impl Default for OracleBreakHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleBreakHandle {
    /// Construct a new idle [`OracleBreakHandle`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            in_flight: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            wait_lock: Mutex::new(()),
            wait_cvar: Condvar::new(),
            callback: Mutex::new(None),
        }
    }

    /// Register an optional out-of-band break callback invoked when [`Self::break_execution`] fires.
    pub fn set_callback(&self, cb: Option<BreakCallback>) {
        let mut guard = self.callback.lock().unwrap_or_else(|e| e.into_inner());
        *guard = cb;
    }

    /// Arm the break handle before dispatching an operation to the worker thread.
    pub fn begin_operation(&self) {
        self.cancelled.store(false, Ordering::Release);
        self.in_flight.store(true, Ordering::Release);
    }

    /// Disarm the break handle when an operation completes on the worker thread.
    pub fn end_operation(&self) {
        self.in_flight.store(false, Ordering::Release);
        self.cancelled.store(false, Ordering::Release);
    }

    /// Trigger an out-of-band `break_execution()` if an operation is currently in flight.
    pub fn break_execution(&self) {
        if !self.in_flight.load(Ordering::Acquire) {
            return;
        }
        self.cancelled.store(true, Ordering::SeqCst);
        {
            let _guard = self.wait_lock.lock().unwrap_or_else(|e| e.into_inner());
            self.wait_cvar.notify_all();
        }
        let cb = self
            .callback
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(f) = cb {
            f();
        }
    }

    /// Return `true` if the active operation has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Consume any pending cancellation flag and return `Err(ORA-01013)` if set.
    pub fn check_cancelled(&self) -> Result<(), OracleError> {
        if self.cancelled.swap(false, Ordering::AcqRel) {
            return Err(OracleError::Server {
                code: 1013,
                message: "ORA-01013: user requested cancel of current operation".to_owned(),
            });
        }
        Ok(())
    }

    /// Wait up to `duration` or abort immediately with `ORA-01013` if [`Self::break_execution`] is called.
    pub fn wait_or_cancelled(&self, duration: Duration) -> Result<(), OracleError> {
        let start = Instant::now();
        let mut guard = self.wait_lock.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if self.cancelled.swap(false, Ordering::AcqRel) {
                return Err(OracleError::Server {
                    code: 1013,
                    message: "ORA-01013: user requested cancel of current operation".to_owned(),
                });
            }
            let elapsed = start.elapsed();
            if elapsed >= duration {
                return Ok(());
            }
            let remaining = duration.saturating_sub(elapsed);
            let (next_guard, _) = self
                .wait_cvar
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|e| e.into_inner());
            guard = next_guard;
        }
    }
}

enum WorkerCommand {
    Connect,
    Ping,
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    RollbackToSavepoint(String),
    Execute { sql: String },
    Close,
}

enum WorkerReply {
    Unit,
    RowCount(usize),
}

enum ReplyChannel {
    Sync(Arc<SyncCompletionQueue<Result<WorkerReply, OracleError>>>),
    Async(oneshot::Sender<Result<WorkerReply, OracleError>>),
}

impl ReplyChannel {
    fn send(self, result: Result<WorkerReply, OracleError>) {
        match self {
            Self::Sync(queue) => queue.push(result),
            Self::Async(tx) => {
                let _ = tx.send(result);
            }
        }
    }
}

struct WorkerEnvelope {
    cmd: WorkerCommand,
    reply: ReplyChannel,
}

fn parse_synthetic_sleep_ms(sql: &str) -> Option<u64> {
    let upper = sql.to_ascii_uppercase();
    if let Some(idx) = upper.find("VTB_SLEEP_MS(") {
        let rest = &upper[idx + "VTB_SLEEP_MS(".len()..];
        let end = rest.find(')')?;
        return rest[..end].trim().parse::<u64>().ok();
    }
    for marker in ["DBMS_SESSION.SLEEP(", "DBMS_LOCK.SLEEP("] {
        if let Some(idx) = upper.find(marker) {
            let rest = &upper[idx + marker.len()..];
            let end = rest.find(')')?;
            let secs = rest[..end].trim().parse::<f64>().ok()?;
            if secs.is_finite() && secs >= 0.0 {
                return Some((secs * 1000.0) as u64);
            }
        }
    }
    None
}

struct WorkerSession {
    config: Arc<NativeOracleConfig>,
    conn: Option<oracledb::Connection>,
    savepoints: Vec<String>,
    in_transaction: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    break_handle: Arc<OracleBreakHandle>,
}

impl WorkerSession {
    fn ensure_connected(&mut self) -> Result<&oracledb::Connection, OracleError> {
        if self.conn.is_none() {
            let c = connect_oracledb(&self.config)?;
            self.conn = Some(c);
        }
        self.conn.as_ref().ok_or_else(|| {
            OracleError::Interface("Oracle worker connection unavailable".to_owned())
        })
    }

    fn handle_command(&mut self, cmd: WorkerCommand) -> Result<WorkerReply, OracleError> {
        self.break_handle.check_cancelled()?;
        match cmd {
            WorkerCommand::Connect => {
                if !self.config.host.eq_ignore_ascii_case("memory") {
                    let _ = self.ensure_connected()?;
                }
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Ping => {
                if let Some(ref conn) = self.conn {
                    conn.ping().map_err(|e| OracleError::from_oracledb(&e))?;
                } else if !self.config.host.eq_ignore_ascii_case("memory") {
                    let conn = self.ensure_connected()?;
                    conn.ping().map_err(|e| OracleError::from_oracledb(&e))?;
                }
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Begin => {
                self.in_transaction.store(true, Ordering::Release);
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Commit => {
                if let Some(ref conn) = self.conn {
                    conn.commit().map_err(|e| OracleError::from_oracledb(&e))?;
                }
                self.savepoints.clear();
                self.in_transaction.store(false, Ordering::Release);
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Rollback => {
                if let Some(ref conn) = self.conn {
                    conn.rollback().map_err(|e| OracleError::from_oracledb(&e))?;
                }
                self.savepoints.clear();
                self.in_transaction.store(false, Ordering::Release);
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Savepoint(name) => {
                validate_savepoint_identifier(&name)?;
                self.in_transaction.store(true, Ordering::Release);
                if let Some(ref conn) = self.conn {
                    let stmt = format!("SAVEPOINT {name}");
                    conn.execute(&stmt, &[])
                        .map_err(|e| OracleError::from_oracledb(&e))?;
                }
                if !self.savepoints.iter().any(|s| s.eq_ignore_ascii_case(&name)) {
                    self.savepoints.push(name);
                }
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::RollbackToSavepoint(name) => {
                validate_savepoint_identifier(&name)?;
                if let Some(ref conn) = self.conn {
                    let stmt = format!("ROLLBACK TO SAVEPOINT {name}");
                    conn.execute(&stmt, &[])
                        .map_err(|e| OracleError::from_oracledb(&e))?;
                } else if let Some(pos) = self
                    .savepoints
                    .iter()
                    .position(|s| s.eq_ignore_ascii_case(&name))
                {
                    self.savepoints.truncate(pos + 1);
                } else {
                    return Err(OracleError::Server {
                        code: 1086,
                        message: format!(
                            "ORA-01086: savepoint '{name}' never established in this session or is invalid"
                        ),
                    });
                }
                self.in_transaction.store(true, Ordering::Release);
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Execute { sql } => {
                if self.conn.is_none()
                    && let Some(sleep_ms) = parse_synthetic_sleep_ms(&sql)
                {
                    self.break_handle
                        .wait_or_cancelled(Duration::from_millis(sleep_ms))?;
                    return Ok(WorkerReply::RowCount(0));
                }
                if self.conn.is_none() && self.config.host.eq_ignore_ascii_case("memory") {
                    self.break_handle.check_cancelled()?;
                    return Ok(WorkerReply::RowCount(0));
                }
                let conn = self.ensure_connected()?;
                let exec_res = conn
                    .execute(&sql, &[])
                    .map_err(|e| OracleError::from_oracledb(&e))?;
                self.break_handle.check_cancelled()?;
                Ok(WorkerReply::RowCount(exec_res.rows_affected() as usize))
            }
            WorkerCommand::Close => {
                if let Some(mut conn) = self.conn.take() {
                    let _ = conn.close();
                }
                self.savepoints.clear();
                self.in_transaction.store(false, Ordering::Release);
                self.closed.store(true, Ordering::Release);
                Ok(WorkerReply::Unit)
            }
        }
    }
}

struct CancelOnDrop {
    break_handle: Arc<OracleBreakHandle>,
    armed: bool,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.break_handle.break_execution();
        }
    }
}

/// Handle to a dedicated `"vtb-ora-worker"` OS thread owning a single `oracledb::Connection`.
pub struct OracleWorkerHandle {
    config: Arc<NativeOracleConfig>,
    sender: Mutex<Option<mpsc::Sender<WorkerEnvelope>>>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    break_handle: Arc<OracleBreakHandle>,
    closed: Arc<AtomicBool>,
    in_transaction: Arc<AtomicBool>,
}

impl OracleWorkerHandle {
    /// Spawn a dedicated worker thread for `config` without opening a socket at construction time.
    pub fn spawn(config: NativeOracleConfig) -> Result<Self, OracleError> {
        let cfg = Arc::new(config);
        let break_handle = Arc::new(OracleBreakHandle::new());
        let closed = Arc::new(AtomicBool::new(false));
        let in_transaction = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel::<WorkerEnvelope>();

        let cfg_clone = Arc::clone(&cfg);
        let break_clone = Arc::clone(&break_handle);
        let closed_clone = Arc::clone(&closed);
        let in_tx_clone = Arc::clone(&in_transaction);

        let join_handle = thread::Builder::new()
            .name("vtb-ora-worker".to_owned())
            .spawn(move || {
                let mut session = WorkerSession {
                    config: cfg_clone,
                    conn: None,
                    savepoints: Vec::new(),
                    in_transaction: in_tx_clone,
                    closed: closed_clone,
                    break_handle: break_clone,
                };
                while let Ok(envelope) = rx.recv() {
                    let is_close = matches!(envelope.cmd, WorkerCommand::Close);
                    let outcome = session.handle_command(envelope.cmd);
                    session.break_handle.end_operation();
                    envelope.reply.send(outcome);
                    if is_close {
                        break;
                    }
                }
            })
            .map_err(|e| {
                OracleError::Interface(format!("failed to spawn Oracle worker thread: {e}"))
            })?;

        Ok(Self {
            config: cfg,
            sender: Mutex::new(Some(tx)),
            join_handle: Mutex::new(Some(join_handle)),
            break_handle,
            closed,
            in_transaction,
        })
    }

    /// Return a reference to the validated [`NativeOracleConfig`].
    #[must_use]
    pub fn config(&self) -> &NativeOracleConfig {
        &self.config
    }

    /// Return a shared reference to the out-of-band [`OracleBreakHandle`].
    #[must_use]
    pub fn break_handle(&self) -> &Arc<OracleBreakHandle> {
        &self.break_handle
    }

    /// Return `true` if this client has been closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Return `true` if a transaction is currently open on this client.
    #[must_use]
    pub fn is_in_transaction(&self) -> bool {
        self.in_transaction.load(Ordering::Acquire)
    }

    /// Trigger an out-of-band `break_execution()` on any in-flight command.
    pub fn cancel(&self) {
        if !self.is_closed() {
            self.break_handle.break_execution();
        }
    }

    fn ensure_open(&self) -> Result<(), OracleError> {
        if self.is_closed() {
            return Err(OracleError::Interface(
                "Oracle client is closed".to_owned(),
            ));
        }
        Ok(())
    }

    fn dispatch_sync(&self, cmd: WorkerCommand) -> Result<WorkerReply, OracleError> {
        if !matches!(cmd, WorkerCommand::Close) {
            self.ensure_open()?;
        }
        let tx = {
            let guard = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            guard.clone()
        };
        let Some(sender) = tx else {
            if matches!(cmd, WorkerCommand::Close) {
                self.closed.store(true, Ordering::Release);
                self.in_transaction.store(false, Ordering::Release);
                return Ok(WorkerReply::Unit);
            }
            return Err(OracleError::Interface(
                "Oracle client is closed".to_owned(),
            ));
        };

        let queue = Arc::new(SyncCompletionQueue::new());
        self.break_handle.begin_operation();
        if sender
            .send(WorkerEnvelope {
                cmd,
                reply: ReplyChannel::Sync(Arc::clone(&queue)),
            })
            .is_err()
        {
            self.break_handle.end_operation();
            return Err(OracleError::Interface(
                "Oracle worker thread terminated".to_owned(),
            ));
        }
        queue.pop_blocking()
    }

    fn dispatch_async<M>(
        &self,
        py: Python<'_>,
        cmd: WorkerCommand,
        map_ok: M,
    ) -> PyResult<RustDbTaskAwaitable>
    where
        M: FnOnce(Python<'_>, WorkerReply) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        let is_close = matches!(cmd, WorkerCommand::Close);
        if !is_close && let Err(err) = self.ensure_open() {
            return Err(err.to_py_err(py));
        }

        let tx_opt = {
            let mut guard = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            if is_close {
                self.closed.store(true, Ordering::Release);
                self.in_transaction.store(false, Ordering::Release);
                guard.take()
            } else {
                guard.clone()
            }
        };

        let Some(sender) = tx_opt else {
            if is_close {
                return spawn_rust_db_task_with_attr(
                    py,
                    async { Ok::<WorkerReply, OracleError>(WorkerReply::Unit) },
                    map_ok,
                    |py, err| err.to_py_err(py),
                    ORACLE_COMPLETION_DRAINER_ATTR,
                );
            }
            return Err(OracleError::Interface("Oracle client is closed".to_owned()).to_py_err(py));
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        self.break_handle.begin_operation();
        if sender
            .send(WorkerEnvelope {
                cmd,
                reply: ReplyChannel::Async(reply_tx),
            })
            .is_err()
        {
            self.break_handle.end_operation();
            return Err(
                OracleError::Interface("Oracle worker thread terminated".to_owned()).to_py_err(py),
            );
        }

        let break_for_drop = Arc::clone(&self.break_handle);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let mut cancel_guard = CancelOnDrop {
                    break_handle: break_for_drop,
                    armed: true,
                };
                let res = match reply_rx.await {
                    Ok(outcome) => outcome,
                    Err(_) => Err(OracleError::Interface(
                        "Oracle worker channel closed before reply".to_owned(),
                    )),
                };
                cancel_guard.armed = false;
                res
            },
            map_ok,
            |py, err| err.to_py_err(py),
            ORACLE_COMPLETION_DRAINER_ATTR,
        )
    }

    /// Synchronously close the worker session and join the worker thread.
    pub fn close_sync(&self) -> Result<(), OracleError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.in_transaction.store(false, Ordering::Release);
        self.break_handle.break_execution();
        let sender = {
            let mut guard = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        if let Some(tx) = sender {
            let queue = Arc::new(SyncCompletionQueue::new());
            if tx
                .send(WorkerEnvelope {
                    cmd: WorkerCommand::Close,
                    reply: ReplyChannel::Sync(Arc::clone(&queue)),
                })
                .is_ok()
            {
                let _ = queue.pop_timeout(Duration::from_secs(2));
            }
        }
        let join = {
            let mut guard = self.join_handle.lock().unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        if let Some(handle) = join {
            let _ = handle.join();
        }
        Ok(())
    }
}

impl Drop for OracleWorkerHandle {
    fn drop(&mut self) {
        let _ = self.close_sync();
    }
}

#[allow(clippy::too_many_arguments)]
fn build_raw_oracle_options(
    user: Option<String>,
    password: Option<String>,
    dsn: Option<String>,
    host: String,
    port: i32,
    service_name: String,
    sid: Option<String>,
    tns_admin: Option<String>,
    connect_descriptor: Option<String>,
    protocol: String,
    ssl_server_dn_match: bool,
    ssl_server_cert_dn: Option<String>,
    wallet_location: Option<String>,
    wallet_password: Option<String>,
    pem_cert_data: Option<Vec<u8>>,
    cwallet_sso_data: Option<Vec<u8>>,
    auth_mode: String,
    access_token: Option<String>,
    token_auth_options: Option<BTreeMap<String, String>>,
    cclass: Option<String>,
    purity: String,
    connection_tag: Option<String>,
    stmtcachesize: i32,
    prefetchrows: i64,
    arraysize: i64,
    call_timeout_ms: Option<i64>,
    connect_timeout: f64,
    enable_lowercase_column_names: bool,
    client_identifier: Option<String>,
    module: Option<String>,
    action: Option<String>,
    ecid: Option<String>,
) -> RawOracleOptions {
    RawOracleOptions {
        user,
        password,
        dsn,
        host,
        port,
        service_name,
        sid,
        tns_admin,
        connect_descriptor,
        protocol,
        ssl_server_dn_match,
        ssl_server_cert_dn,
        wallet_location,
        wallet_password,
        pem_cert_data,
        cwallet_sso_data,
        auth_mode,
        access_token,
        token_auth_options,
        cclass,
        purity,
        connection_tag,
        stmtcachesize,
        prefetchrows,
        arraysize,
        call_timeout_ms,
        connect_timeout,
        enable_lowercase_column_names,
        client_identifier,
        module,
        action,
        ecid,
    }
}

/// Synchronous physical Oracle session client (`vertebra._oracle.SyncOracleClient`).
#[pyclass(
    name = "SyncOracleClient",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
pub struct SyncOracleClient {
    worker: Arc<OracleWorkerHandle>,
}

impl SyncOracleClient {
    /// Return a reference to the underlying [`OracleWorkerHandle`].
    #[must_use]
    pub fn worker(&self) -> &Arc<OracleWorkerHandle> {
        &self.worker
    }
}

#[pymethods]
impl SyncOracleClient {
    #[new]
    #[pyo3(signature = (
        *,
        user=None,
        password=None,
        dsn=None,
        host="localhost",
        port=1521,
        service_name="FREEPDB1",
        sid=None,
        tns_admin=None,
        connect_descriptor=None,
        protocol="tcp",
        ssl_server_dn_match=true,
        ssl_server_cert_dn=None,
        wallet_location=None,
        wallet_password=None,
        pem_cert_data=None,
        cwallet_sso_data=None,
        auth_mode="password",
        access_token=None,
        token_auth_options=None,
        cclass=None,
        purity="SELF",
        connection_tag=None,
        stmtcachesize=64,
        prefetchrows=256,
        arraysize=256,
        call_timeout_ms=None,
        connect_timeout=10.0,
        enable_lowercase_column_names=true,
        client_identifier=None,
        module=None,
        action=None,
        ecid=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        user: Option<String>,
        password: Option<String>,
        dsn: Option<String>,
        host: &str,
        port: i32,
        service_name: &str,
        sid: Option<String>,
        tns_admin: Option<String>,
        connect_descriptor: Option<String>,
        protocol: &str,
        ssl_server_dn_match: bool,
        ssl_server_cert_dn: Option<String>,
        wallet_location: Option<String>,
        wallet_password: Option<String>,
        pem_cert_data: Option<Vec<u8>>,
        cwallet_sso_data: Option<Vec<u8>>,
        auth_mode: &str,
        access_token: Option<String>,
        token_auth_options: Option<BTreeMap<String, String>>,
        cclass: Option<String>,
        purity: &str,
        connection_tag: Option<String>,
        stmtcachesize: i32,
        prefetchrows: i64,
        arraysize: i64,
        call_timeout_ms: Option<i64>,
        connect_timeout: f64,
        enable_lowercase_column_names: bool,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) -> PyResult<Self> {
        let raw = build_raw_oracle_options(
            user,
            password,
            dsn,
            host.to_owned(),
            port,
            service_name.to_owned(),
            sid,
            tns_admin,
            connect_descriptor,
            protocol.to_owned(),
            ssl_server_dn_match,
            ssl_server_cert_dn,
            wallet_location,
            wallet_password,
            pem_cert_data,
            cwallet_sso_data,
            auth_mode.to_owned(),
            access_token,
            token_auth_options,
            cclass,
            purity.to_owned(),
            connection_tag,
            stmtcachesize,
            prefetchrows,
            arraysize,
            call_timeout_ms,
            connect_timeout,
            enable_lowercase_column_names,
            client_identifier,
            module,
            action,
            ecid,
        );
        let cfg = NativeOracleConfig::validate(raw).map_err(|e| e.to_py_err(py))?;
        let worker = OracleWorkerHandle::spawn(cfg).map_err(|e| e.to_py_err(py))?;
        Ok(Self {
            worker: Arc::new(worker),
        })
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.worker.is_closed()
    }

    #[getter]
    pub fn in_transaction(&self) -> bool {
        self.worker.is_in_transaction()
    }

    pub fn connect(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Connect))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.close_sync())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn ping(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Ping))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn cancel(&self) {
        self.worker.cancel();
    }

    pub fn begin(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Begin))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Commit))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Rollback))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn savepoint(&self, py: Python<'_>, name: String) -> PyResult<()> {
        validate_savepoint_identifier(&name).map_err(|e| e.to_py_err(py))?;
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Savepoint(name)))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn rollback_to_savepoint(&self, py: Python<'_>, name: String) -> PyResult<()> {
        validate_savepoint_identifier(&name).map_err(|e| e.to_py_err(py))?;
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::RollbackToSavepoint(name)))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn execute(&self, py: Python<'_>, sql: String) -> PyResult<usize> {
        let worker = Arc::clone(&self.worker);
        match py
            .detach(|| worker.dispatch_sync(WorkerCommand::Execute { sql }))
            .map_err(|e| e.to_py_err(py))?
        {
            WorkerReply::RowCount(n) => Ok(n),
            WorkerReply::Unit => Ok(0),
        }
    }
}

/// Asynchronous physical Oracle session client (`vertebra._oracle.AsyncOracleClient`).
#[pyclass(
    name = "AsyncOracleClient",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
pub struct AsyncOracleClient {
    worker: Arc<OracleWorkerHandle>,
}

impl AsyncOracleClient {
    /// Return a reference to the underlying [`OracleWorkerHandle`].
    #[must_use]
    pub fn worker(&self) -> &Arc<OracleWorkerHandle> {
        &self.worker
    }
}

#[pymethods]
impl AsyncOracleClient {
    #[new]
    #[pyo3(signature = (
        *,
        user=None,
        password=None,
        dsn=None,
        host="localhost",
        port=1521,
        service_name="FREEPDB1",
        sid=None,
        tns_admin=None,
        connect_descriptor=None,
        protocol="tcp",
        ssl_server_dn_match=true,
        ssl_server_cert_dn=None,
        wallet_location=None,
        wallet_password=None,
        pem_cert_data=None,
        cwallet_sso_data=None,
        auth_mode="password",
        access_token=None,
        token_auth_options=None,
        cclass=None,
        purity="SELF",
        connection_tag=None,
        stmtcachesize=64,
        prefetchrows=256,
        arraysize=256,
        call_timeout_ms=None,
        connect_timeout=10.0,
        enable_lowercase_column_names=true,
        client_identifier=None,
        module=None,
        action=None,
        ecid=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        user: Option<String>,
        password: Option<String>,
        dsn: Option<String>,
        host: &str,
        port: i32,
        service_name: &str,
        sid: Option<String>,
        tns_admin: Option<String>,
        connect_descriptor: Option<String>,
        protocol: &str,
        ssl_server_dn_match: bool,
        ssl_server_cert_dn: Option<String>,
        wallet_location: Option<String>,
        wallet_password: Option<String>,
        pem_cert_data: Option<Vec<u8>>,
        cwallet_sso_data: Option<Vec<u8>>,
        auth_mode: &str,
        access_token: Option<String>,
        token_auth_options: Option<BTreeMap<String, String>>,
        cclass: Option<String>,
        purity: &str,
        connection_tag: Option<String>,
        stmtcachesize: i32,
        prefetchrows: i64,
        arraysize: i64,
        call_timeout_ms: Option<i64>,
        connect_timeout: f64,
        enable_lowercase_column_names: bool,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) -> PyResult<Self> {
        let raw = build_raw_oracle_options(
            user,
            password,
            dsn,
            host.to_owned(),
            port,
            service_name.to_owned(),
            sid,
            tns_admin,
            connect_descriptor,
            protocol.to_owned(),
            ssl_server_dn_match,
            ssl_server_cert_dn,
            wallet_location,
            wallet_password,
            pem_cert_data,
            cwallet_sso_data,
            auth_mode.to_owned(),
            access_token,
            token_auth_options,
            cclass,
            purity.to_owned(),
            connection_tag,
            stmtcachesize,
            prefetchrows,
            arraysize,
            call_timeout_ms,
            connect_timeout,
            enable_lowercase_column_names,
            client_identifier,
            module,
            action,
            ecid,
        );
        let cfg = NativeOracleConfig::validate(raw).map_err(|e| e.to_py_err(py))?;
        let worker = OracleWorkerHandle::spawn(cfg).map_err(|e| e.to_py_err(py))?;
        Ok(Self {
            worker: Arc::new(worker),
        })
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.worker.is_closed()
    }

    #[getter]
    pub fn in_transaction(&self) -> bool {
        self.worker.is_in_transaction()
    }

    pub fn connect(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Connect, |py, _| Ok(py.None()))
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Close, |py, _| Ok(py.None()))
    }

    pub fn ping(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Ping, |py, _| Ok(py.None()))
    }

    pub fn cancel(&self) {
        self.worker.cancel();
    }

    pub fn begin(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Begin, |py, _| Ok(py.None()))
    }

    pub fn commit(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Commit, |py, _| Ok(py.None()))
    }

    pub fn rollback(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Rollback, |py, _| Ok(py.None()))
    }

    pub fn savepoint(&self, py: Python<'_>, name: String) -> PyResult<RustDbTaskAwaitable> {
        validate_savepoint_identifier(&name).map_err(|e| e.to_py_err(py))?;
        self.worker
            .dispatch_async(py, WorkerCommand::Savepoint(name), |py, _| Ok(py.None()))
    }

    pub fn rollback_to_savepoint(
        &self,
        py: Python<'_>,
        name: String,
    ) -> PyResult<RustDbTaskAwaitable> {
        validate_savepoint_identifier(&name).map_err(|e| e.to_py_err(py))?;
        self.worker
            .dispatch_async(py, WorkerCommand::RollbackToSavepoint(name), |py, _| {
                Ok(py.None())
            })
    }

    pub fn execute(&self, py: Python<'_>, sql: String) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Execute { sql }, |py, reply| match reply {
                WorkerReply::RowCount(n) => Ok(n.into_pyobject(py)?.into_any().unbind()),
                WorkerReply::Unit => Ok(0usize.into_pyobject(py)?.into_any().unbind()),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use super::{
        OracleWorkerHandle, WorkerCommand, validate_savepoint_identifier,
    };
    use crate::config::{NativeOracleConfig, RawOracleOptions};
    use crate::errors::OracleError;

    #[test]
    fn savepoint_identifier_validation_enforces_oracle_rules() {
        assert!(validate_savepoint_identifier("sp_1").is_ok());
        assert!(validate_savepoint_identifier("_vtb_pipe_1$#").is_ok());
        assert!(matches!(
            validate_savepoint_identifier(""),
            Err(OracleError::Server { code: 900, .. })
        ));
        assert!(matches!(
            validate_savepoint_identifier("1bad"),
            Err(OracleError::Server { code: 900, .. })
        ));
        assert!(matches!(
            validate_savepoint_identifier("sp;drop"),
            Err(OracleError::Server { code: 900, .. })
        ));
    }

    #[test]
    fn worker_out_of_band_cancel_aborts_sleep_and_keeps_worker_alive() {
        let cfg = NativeOracleConfig::validate(RawOracleOptions {
            host: "memory".to_owned(),
            ..Default::default()
        })
        .expect("valid memory config");
        let worker = Arc::new(OracleWorkerHandle::spawn(cfg).expect("spawn worker"));

        let worker_for_cancel = Arc::clone(&worker);
        let canceller = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            worker_for_cancel.cancel();
        });

        let res = worker.dispatch_sync(WorkerCommand::Execute {
            sql: "SELECT /*+ VTB_SLEEP_MS(2000) */ 1 FROM DUAL".to_owned(),
        });
        canceller.join().expect("join canceller");
        assert!(matches!(res, Err(OracleError::Server { code: 1013, .. })));

        worker
            .dispatch_sync(WorkerCommand::Begin)
            .expect("worker still alive after cancel");
        assert!(worker.is_in_transaction());
        worker
            .dispatch_sync(WorkerCommand::Commit)
            .expect("commit succeeds");
        assert!(!worker.is_in_transaction());
        worker.close_sync().expect("close worker");
        assert!(worker.is_closed());
    }
}
