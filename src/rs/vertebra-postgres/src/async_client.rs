//! Asynchronous physical PostgreSQL session client (`vertebra._postgres.AsyncPostgresClient`).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use vertebra_db::py_async::{
    RustDbTaskAwaitable, spawn_rust_db_task_with_attr, spawn_rust_db_task_with_attr_and_cancel,
};

use crate::arrow::arrow_output_into_py_result;
use crate::copy::{CopyFromStream, CopyOut};
use crate::errors::{PostgresError, postgres_error_to_py};
use crate::portal::{
    AsyncArrowCloseStart, AsyncArrowFetchStart, AsyncArrowPortalShared, Portal,
    UnopenedArrowPortalArgs, execute_select_to_arrow,
};
use crate::python_values::{extract_parameters, query_result_to_python};
use crate::session::cancel::OperationGeneration;
use crate::session::pipeline_scope::{
    PipelineScopeToken, extract_pipeline_batch, outcomes_to_python, pipeline_error_to_py,
};
use crate::session::{
    ClientBeginConnect, CloseAction, CommandBudget, CopyTaskCleanupGuard, SessionHandle,
};
use crate::sync_client::{
    OwnedBusyGuard, build_validated_config, copy_out_row_to_python, extract_arrow_batch_size,
    extract_batch_size, extract_cloud_auth_options, extract_copy_rows_and_oids,
    extract_iam_auth_options, extract_ipc_buffer_bytes, extract_max_result_bytes, extract_max_rows,
    extract_opt_copy_columns, extract_scope_id, extract_session_gucs, extract_strict_bool,
    extract_strict_f64, extract_strict_i64, extract_strict_opt_f64, extract_strict_opt_i64,
    extract_strict_u64, extract_threshold_rows,
};

/// Event-loop attribute name isolating the `vertebra._postgres` completion drainer from sibling extensions.
pub const POSTGRES_COMPLETION_DRAINER_ATTR: &str = "_vertebra_postgres_completion_drainer";

fn spawn_cancellable_op<'py, F, T, MapOk, MapErr>(
    py: Python<'py>,
    session: &Arc<SessionHandle>,
    op_gen: OperationGeneration,
    fut: F,
    map_ok: MapOk,
    map_err: MapErr,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>>
where
    F: Future<Output = Result<T, PostgresError>> + Send + 'static,
    T: Send + 'static,
    MapOk: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    MapErr: FnOnce(Python<'_>, PostgresError) -> PyErr + Send + 'static,
{
    let cancel_session = Arc::clone(session);
    match spawn_rust_db_task_with_attr_and_cancel(
        py,
        fut,
        map_ok,
        map_err,
        move || {
            cancel_session.trigger_async_cancel(op_gen);
        },
        POSTGRES_COMPLETION_DRAINER_ATTR,
    ) {
        Ok(awaitable) => Ok(awaitable),
        Err(err) => {
            session.unreserve_operation_generation(op_gen);
            Err(err)
        }
    }
}

/// Physical asynchronous native PostgreSQL client (`vertebra._postgres.AsyncPostgresClient`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        name = "AsyncPostgresClient",
        module = "vertebra._postgres",
        frozen,
        immutable_type,
        freelist = 128
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(
        name = "AsyncPostgresClient",
        module = "vertebra._postgres",
        frozen,
        immutable_type
    )
)]
pub struct PyAsyncPostgresClient {
    pub(crate) session: Arc<SessionHandle>,
}

impl PyAsyncPostgresClient {
    /// Wrap an existing [`SessionHandle`] (such as a checked-out pool lease).
    pub fn from_session(session: Arc<SessionHandle>) -> Self {
        Self { session }
    }

    /// Register asynchronous client, portal, Arrow stream, and COPY types on the `vertebra._postgres` module.
    pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<Self>()?;
        m.add_class::<PyAsyncPostgresPortal>()?;
        m.add_class::<PyAsyncPostgresArrowStream>()?;
        m.add_class::<PyAsyncPostgresCopyOut>()?;
        m.add_class::<PyAsyncPostgresCopyIn>()
    }
}

struct ActiveCopyStepState {
    running: bool,
    cancelled: bool,
    cleanup_guard: Option<CopyTaskCleanupGuard>,
}

struct ActiveCopyStepCancel {
    session: Arc<SessionHandle>,
    lease_op_gen: OperationGeneration,
    inner: std::sync::Mutex<ActiveCopyStepState>,
}

struct ActiveCopyStepGuard {
    shared: Arc<ActiveCopyStepCancel>,
    disarmed: bool,
}

impl ActiveCopyStepCancel {
    fn new_pair(
        session: &Arc<SessionHandle>,
        lease_op_gen: OperationGeneration,
    ) -> (Arc<Self>, ActiveCopyStepGuard) {
        let shared = Arc::new(Self {
            session: Arc::clone(session),
            lease_op_gen,
            inner: std::sync::Mutex::new(ActiveCopyStepState {
                running: true,
                cancelled: false,
                cleanup_guard: None,
            }),
        });
        let guard = ActiveCopyStepGuard {
            shared: Arc::clone(&shared),
            disarmed: false,
        };
        (shared, guard)
    }

    fn request_cancel(&self) {
        let should_trigger = {
            let mut st = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if !st.running || st.cancelled {
                false
            } else {
                st.cancelled = true;
                st.cleanup_guard = self.session.begin_copy_task_cleanup(self.lease_op_gen);
                true
            }
        };
        if should_trigger {
            self.session.trigger_async_cancel(self.lease_op_gen);
        }
    }

    fn was_cancelled(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cancelled
    }

    fn take_on_finish(&self) -> Option<CopyTaskCleanupGuard> {
        let mut st = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        st.running = false;
        st.cleanup_guard.take()
    }
}

impl ActiveCopyStepGuard {
    fn was_cancelled(&self) -> bool {
        self.shared.was_cancelled()
    }

    async fn finish(mut self) {
        self.disarmed = true;
        if let Some(cg) = self.shared.take_on_finish() {
            cg.finish().await;
        }
    }
}

impl Drop for ActiveCopyStepGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            let _ = self.shared.take_on_finish();
        }
    }
}

/// Exclusive asynchronous binary `COPY ... TO STDOUT` iterator (`vertebra._postgres.AsyncPostgresCopyOut`).
#[pyclass(
    name = "AsyncPostgresCopyOut",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PyAsyncPostgresCopyOut {
    stream: Arc<tokio::sync::Mutex<CopyOut>>,
    session: Arc<SessionHandle>,
    lease_op_gen: OperationGeneration,
    exhausted_flag: Arc<AtomicBool>,
    closed_flag: Arc<AtomicBool>,
    busy: Arc<AtomicBool>,
}

impl PyAsyncPostgresCopyOut {
    pub(crate) fn from_stream(
        stream: CopyOut,
        session: Arc<SessionHandle>,
        lease_op_gen: OperationGeneration,
    ) -> Self {
        let (exhausted_flag, closed_flag) = stream.status_flags();
        Self {
            stream: Arc::new(tokio::sync::Mutex::new(stream)),
            session,
            lease_op_gen,
            exhausted_flag,
            closed_flag,
            busy: Arc::new(AtomicBool::new(false)),
        }
    }

    fn spawn_close<'py>(
        &self,
        py: Python<'py>,
        return_false: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        if !self.closed_flag.load(Ordering::Acquire) && self.busy.load(Ordering::Acquire) {
            self.session.trigger_async_cancel(self.lease_op_gen);
        }
        let cleanup_guard = self.session.begin_copy_task_cleanup(self.lease_op_gen);
        let stream = Arc::clone(&self.stream);
        let cancel_session = Arc::clone(&self.session);
        let lease_op_gen = self.lease_op_gen;
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                let res = {
                    let mut guard = stream.lock().await;
                    guard.close().await
                };
                if let Some(cg) = cleanup_guard {
                    cg.finish().await;
                }
                res
            },
            move |py, ()| {
                if return_false {
                    Ok(false.into_pyobject(py)?.to_owned().into_any().unbind())
                } else {
                    Ok(py.None())
                }
            },
            postgres_error_to_py,
            move || {
                cancel_session.trigger_async_cancel(lease_op_gen);
            },
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[pymethods]
impl PyAsyncPostgresCopyOut {
    /// Return the stream itself as an asynchronous row iterator.
    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Resolve to the next decoded binary row as `tuple[bytes | None, ...]`, or raise `StopAsyncIteration`.
    pub fn __anext__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, RustDbTaskAwaitable>>> {
        let busy_guard = OwnedBusyGuard::try_enter(py, &self.busy, "COPY TO")?;
        if self.closed_flag.load(Ordering::Acquire) && !self.exhausted_flag.load(Ordering::Acquire)
        {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Interface("cannot read from a closed COPY TO stream".to_string()),
            ));
        }
        let (cancel_ctl, step_guard) =
            ActiveCopyStepCancel::new_pair(&self.session, self.lease_op_gen);
        let stream = Arc::clone(&self.stream);
        let session = Arc::clone(&self.session);
        let lease_op_gen = self.lease_op_gen;
        let awaitable = spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                let _keep_busy = busy_guard;
                let res = {
                    let mut guard = stream.lock().await;
                    if (step_guard.was_cancelled() || session.did_op_claim_cancel(lease_op_gen))
                        && !guard.is_closed()
                    {
                        let _ = guard.close().await;
                        Err(PostgresError::Operational(
                            "COPY TO stream was cancelled".to_string(),
                        ))
                    } else {
                        let r = guard.next_row().await;
                        if (step_guard.was_cancelled() || session.did_op_claim_cancel(lease_op_gen))
                            && !guard.is_closed()
                        {
                            let _ = guard.close().await;
                            Err(PostgresError::Operational(
                                "COPY TO stream was cancelled".to_string(),
                            ))
                        } else {
                            r
                        }
                    }
                };
                step_guard.finish().await;
                res
            },
            |py, opt_row| match opt_row {
                Some(fields) => copy_out_row_to_python(py, fields),
                None => Err(PyStopAsyncIteration::new_err(())),
            },
            postgres_error_to_py,
            move || {
                cancel_ctl.request_cancel();
            },
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )?;
        Ok(Some(awaitable))
    }

    /// Asynchronously close an unfinished `COPY TO` stream via `CancelRequest` drain/retire, or no-op if closed.
    pub fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_close(py, false)
    }

    /// Enter an `async with` scope resolving to this `AsyncPostgresCopyOut`.
    pub fn __aenter__<'py>(slf: PyRef<'py, Self>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let py = slf.py();
        let py_self: Py<Self> = slf.into_pyobject(py)?.unbind();
        let awaitable = spawn_rust_db_task_with_attr(
            py,
            async { Ok::<(), PostgresError>(()) },
            move |_py, ()| Ok(py_self.into_any()),
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )?;
        Bound::new(py, awaitable)
    }

    /// Asynchronously close the stream when leaving `async with` without suppressing exceptions.
    #[pyo3(signature = (exc_type = None, exc_val = None, exc_tb = None))]
    pub fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<&Bound<'py, PyAny>>,
        exc_val: Option<&Bound<'py, PyAny>>,
        exc_tb: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let _ = (exc_type, exc_val, exc_tb);
        self.spawn_close(py, true)
    }

    fn __repr__(&self) -> String {
        format!(
            "AsyncPostgresCopyOut(exhausted={}, closed={})",
            self.exhausted_flag.load(Ordering::Acquire),
            self.closed_flag.load(Ordering::Acquire),
        )
    }
}

/// Exclusive asynchronous binary `COPY ... FROM STDIN` stream (`vertebra._postgres.AsyncPostgresCopyIn`).
#[pyclass(
    name = "AsyncPostgresCopyIn",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PyAsyncPostgresCopyIn {
    stream: Arc<tokio::sync::Mutex<CopyFromStream>>,
    session: Arc<SessionHandle>,
    lease_op_gen: OperationGeneration,
    closed_flag: Arc<AtomicBool>,
    busy: Arc<AtomicBool>,
}

impl PyAsyncPostgresCopyIn {
    pub(crate) fn from_stream(
        stream: CopyFromStream,
        session: Arc<SessionHandle>,
        lease_op_gen: OperationGeneration,
    ) -> Self {
        let closed_flag = stream.closed_flag();
        Self {
            stream: Arc::new(tokio::sync::Mutex::new(stream)),
            session,
            lease_op_gen,
            closed_flag,
            busy: Arc::new(AtomicBool::new(false)),
        }
    }

    fn spawn_abort_with_py_err<'py>(
        &self,
        py: Python<'py>,
        busy_guard: OwnedBusyGuard,
        reason: &'static str,
        err: PyErr,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let cleanup_guard = self.session.begin_copy_task_cleanup(self.lease_op_gen);
        let stream = Arc::clone(&self.stream);
        let cancel_session = Arc::clone(&self.session);
        let lease_op_gen = self.lease_op_gen;
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                let _keep_busy = busy_guard;
                {
                    let mut guard = stream.lock().await;
                    let _ = guard.abort(reason).await;
                }
                if let Some(cg) = cleanup_guard {
                    cg.finish().await;
                }
                Ok::<(), PostgresError>(())
            },
            move |_py, ()| Err(err),
            postgres_error_to_py,
            move || {
                cancel_session.trigger_async_cancel(lease_op_gen);
            },
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }

    fn spawn_close<'py>(
        &self,
        py: Python<'py>,
        return_false: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        if !self.closed_flag.load(Ordering::Acquire) && self.busy.load(Ordering::Acquire) {
            self.session.trigger_async_cancel(self.lease_op_gen);
        }
        let cleanup_guard = self.session.begin_copy_task_cleanup(self.lease_op_gen);
        let stream = Arc::clone(&self.stream);
        let cancel_session = Arc::clone(&self.session);
        let lease_op_gen = self.lease_op_gen;
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                let res = {
                    let mut guard = stream.lock().await;
                    guard.abort("COPY FROM closed before finish").await
                };
                if let Some(cg) = cleanup_guard {
                    cg.finish().await;
                }
                res
            },
            move |py, ()| {
                if return_false {
                    Ok(false.into_pyobject(py)?.to_owned().into_any().unbind())
                } else {
                    Ok(py.None())
                }
            },
            postgres_error_to_py,
            move || {
                cancel_session.trigger_async_cancel(lease_op_gen);
            },
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[pymethods]
impl PyAsyncPostgresCopyIn {
    /// Asynchronously encode and flush binary `COPY` rows against explicit PostgreSQL target `column_oids`.
    pub fn write<'py>(
        &self,
        py: Python<'py>,
        rows: &Bound<'py, PyAny>,
        column_oids: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let busy_guard = OwnedBusyGuard::try_enter(py, &self.busy, "COPY FROM")?;
        if self.closed_flag.load(Ordering::Acquire) {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Interface(
                    "cannot call write() on a closed COPY FROM stream".to_string(),
                ),
            ));
        }
        let (oids, pg_rows) = match extract_copy_rows_and_oids(py, rows, column_oids) {
            Ok(extracted) => extracted,
            Err(err) => {
                return self.spawn_abort_with_py_err(
                    py,
                    busy_guard,
                    "COPY FROM aborted due to invalid input rows",
                    err,
                );
            }
        };
        let (cancel_ctl, step_guard) =
            ActiveCopyStepCancel::new_pair(&self.session, self.lease_op_gen);
        let stream = Arc::clone(&self.stream);
        let session = Arc::clone(&self.session);
        let lease_op_gen = self.lease_op_gen;
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                let _keep_busy = busy_guard;
                let res = {
                    let mut guard = stream.lock().await;
                    if (step_guard.was_cancelled() || session.did_op_claim_cancel(lease_op_gen))
                        && !guard.is_closed()
                    {
                        let _ = guard.abort("COPY FROM cancelled").await;
                        Err(PostgresError::Operational(
                            "COPY FROM write was cancelled".to_string(),
                        ))
                    } else {
                        let wres = guard.write_rows(&oids, &pg_rows).await;
                        if wres.is_ok()
                            && (step_guard.was_cancelled()
                                || session.did_op_claim_cancel(lease_op_gen))
                            && !guard.is_closed()
                        {
                            let _ = guard.abort("COPY FROM cancelled").await;
                            Err(PostgresError::Operational(
                                "COPY FROM write was cancelled".to_string(),
                            ))
                        } else {
                            wres
                        }
                    }
                };
                step_guard.finish().await;
                res
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
            move || {
                cancel_ctl.request_cancel();
            },
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }

    /// Asynchronously encode and flush a bounded (`<= 16 MiB`) Arrow IPC stream chunk without materializing rows.
    pub fn write_arrow_ipc<'py>(
        &self,
        py: Python<'py>,
        batch_ipc: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let busy_guard = OwnedBusyGuard::try_enter(py, &self.busy, "COPY FROM")?;
        if self.closed_flag.load(Ordering::Acquire) {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Interface(
                    "cannot call write_arrow_ipc() on a closed COPY FROM stream".to_string(),
                ),
            ));
        }
        let ipc_bytes = match extract_ipc_buffer_bytes(py, batch_ipc, "batch_ipc") {
            Ok(buf) => buf,
            Err(err) => {
                return self.spawn_abort_with_py_err(
                    py,
                    busy_guard,
                    "COPY FROM Arrow aborted due to invalid IPC buffer",
                    err,
                );
            }
        };
        let (cancel_ctl, step_guard) =
            ActiveCopyStepCancel::new_pair(&self.session, self.lease_op_gen);
        let stream = Arc::clone(&self.stream);
        let session = Arc::clone(&self.session);
        let lease_op_gen = self.lease_op_gen;
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                let _keep_busy = busy_guard;
                let res = {
                    let mut guard = stream.lock().await;
                    if (step_guard.was_cancelled() || session.did_op_claim_cancel(lease_op_gen))
                        && !guard.is_closed()
                    {
                        let _ = guard.abort("COPY FROM Arrow cancelled").await;
                        Err(PostgresError::Operational(
                            "COPY FROM Arrow write was cancelled".to_string(),
                        ))
                    } else {
                        let wres = guard.write_arrow_ipc(&ipc_bytes).await;
                        if wres.is_ok()
                            && (step_guard.was_cancelled()
                                || session.did_op_claim_cancel(lease_op_gen))
                            && !guard.is_closed()
                        {
                            let _ = guard.abort("COPY FROM Arrow cancelled").await;
                            Err(PostgresError::Operational(
                                "COPY FROM Arrow write was cancelled".to_string(),
                            ))
                        } else {
                            wres
                        }
                    }
                };
                step_guard.finish().await;
                res
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
            move || {
                cancel_ctl.request_cancel();
            },
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }

    /// Asynchronously emit the binary `-1i16` trailer and `CopyDone`, commit COPY-owned scope, and resolve row count.
    pub fn finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let busy_guard = OwnedBusyGuard::try_enter(py, &self.busy, "COPY FROM")?;
        if self.closed_flag.load(Ordering::Acquire) {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Interface(
                    "cannot call finish() on a closed COPY FROM stream".to_string(),
                ),
            ));
        }
        let (cancel_ctl, step_guard) =
            ActiveCopyStepCancel::new_pair(&self.session, self.lease_op_gen);
        let stream = Arc::clone(&self.stream);
        let session = Arc::clone(&self.session);
        let lease_op_gen = self.lease_op_gen;
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                let _keep_busy = busy_guard;
                let res = {
                    let mut guard = stream.lock().await;
                    if (step_guard.was_cancelled() || session.did_op_claim_cancel(lease_op_gen))
                        && !guard.is_closed()
                    {
                        let _ = guard.abort("COPY FROM finish cancelled").await;
                        Err(PostgresError::Operational(
                            "COPY FROM finish was cancelled".to_string(),
                        ))
                    } else {
                        guard.finish().await
                    }
                };
                step_guard.finish().await;
                res
            },
            |py, rows| Ok(rows.into_pyobject(py)?.into_any().unbind()),
            postgres_error_to_py,
            move || {
                cancel_ctl.request_cancel();
            },
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }

    /// Asynchronously abort an unfinished `COPY FROM` stream via `CopyFail` + rollback, or no-op once closed.
    pub fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_close(py, false)
    }

    /// Enter an `async with` scope resolving to this `AsyncPostgresCopyIn`.
    pub fn __aenter__<'py>(slf: PyRef<'py, Self>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let py = slf.py();
        let py_self: Py<Self> = slf.into_pyobject(py)?.unbind();
        let awaitable = spawn_rust_db_task_with_attr(
            py,
            async { Ok::<(), PostgresError>(()) },
            move |_py, ()| Ok(py_self.into_any()),
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )?;
        Bound::new(py, awaitable)
    }

    /// Abort via `aclose()` when leaving `async with` unfinished or on exception without suppressing exceptions.
    #[pyo3(signature = (exc_type = None, exc_val = None, exc_tb = None))]
    pub fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<&Bound<'py, PyAny>>,
        exc_val: Option<&Bound<'py, PyAny>>,
        exc_tb: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let _ = (exc_type, exc_val, exc_tb);
        self.spawn_close(py, true)
    }

    fn __repr__(&self) -> String {
        format!(
            "AsyncPostgresCopyIn(closed={})",
            self.closed_flag.load(Ordering::Acquire),
        )
    }
}

/// Session-pinned asynchronous PostgreSQL Protocol 3.0 portal (`vertebra._postgres.AsyncPostgresPortal`).
#[pyclass(
    name = "AsyncPostgresPortal",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PyAsyncPostgresPortal {
    portal: Arc<tokio::sync::Mutex<Portal>>,
    session: Arc<SessionHandle>,
    exhausted_flag: Arc<AtomicBool>,
    closed_flag: Arc<AtomicBool>,
}

impl PyAsyncPostgresPortal {
    pub(crate) fn from_portal(portal: Portal) -> Self {
        let (exhausted_flag, closed_flag) = portal.status_flags();
        let session = Arc::clone(portal.session());
        Self {
            portal: Arc::new(tokio::sync::Mutex::new(portal)),
            session,
            exhausted_flag,
            closed_flag,
        }
    }
}

#[pymethods]
impl PyAsyncPostgresPortal {
    /// Fetch up to `max_rows` rows (`>= 1`) asynchronously from the suspended server portal as a `PostgresQueryResult`.
    pub fn fetch<'py>(
        &self,
        py: Python<'py>,
        #[pyo3(from_py_with = extract_max_rows)] max_rows: i32,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let portal = Arc::clone(&self.portal);
        let op_gen = self.session.reserve_operation_generation();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                let mut guard = portal.lock().await;
                guard.fetch_with_generation(op_gen, max_rows).await
            },
            |py, res| query_result_to_python(py, &res),
            postgres_error_to_py,
        )
    }

    /// Close the portal idempotently and complete/roll back its pinned `TransactionScope` asynchronously.
    pub fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let portal = Arc::clone(&self.portal);
        let awaitable = spawn_rust_db_task_with_attr(
            py,
            async move {
                let mut guard = portal.lock().await;
                guard.close().await
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )?;
        Bound::new(py, awaitable)
    }

    /// Return `True` once the server portal has reached `CommandComplete` or `EmptyQueryResponse`.
    #[getter]
    pub fn exhausted(&self) -> bool {
        self.exhausted_flag.load(Ordering::Acquire)
    }

    /// Return `True` once the portal has been closed or exhausted.
    #[getter]
    pub fn closed(&self) -> bool {
        self.closed_flag.load(Ordering::Acquire)
    }

    fn __repr__(&self) -> String {
        format!(
            "AsyncPostgresPortal(exhausted={}, closed={})",
            self.exhausted(),
            self.closed()
        )
    }
}

/// Cancellation-safe asynchronous `VTBARROW` shared-memory Arrow IPC portal batch iterator
/// (`vertebra._postgres.AsyncPostgresArrowStream`).
#[pyclass(
    name = "AsyncPostgresArrowStream",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PyAsyncPostgresArrowStream {
    shared: Arc<AsyncArrowPortalShared>,
}

impl PyAsyncPostgresArrowStream {
    pub(crate) fn new_unopened(session: Arc<SessionHandle>, args: UnopenedArrowPortalArgs) -> Self {
        Self {
            shared: Arc::new(AsyncArrowPortalShared::new_unopened(session, args)),
        }
    }

    fn spawn_aclose_inner<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let _ = py.import("asyncio")?.call_method0("get_running_loop")?;
        match self.shared.begin_aclose() {
            AsyncArrowCloseStart::ImmediateNoOp => {
                let awaitable = spawn_rust_db_task_with_attr(
                    py,
                    async { Ok::<(), PostgresError>(()) },
                    |py, ()| Ok(py.None()),
                    postgres_error_to_py,
                    POSTGRES_COMPLETION_DRAINER_ATTR,
                )?;
                Bound::new(py, awaitable)
            }
            AsyncArrowCloseStart::RunClose {
                stream,
                close_gen,
                cleanup_reg,
            } => {
                let run_shared = Arc::clone(&self.shared);
                let cancel_shared = Arc::clone(&self.shared);
                spawn_rust_db_task_with_attr_and_cancel(
                    py,
                    async move {
                        run_shared
                            .run_explicit_close(*stream, close_gen, cleanup_reg)
                            .await
                    },
                    |py, ()| Ok(py.None()),
                    postgres_error_to_py,
                    move || {
                        cancel_shared.cancel_active_close();
                    },
                    POSTGRES_COMPLETION_DRAINER_ATTR,
                )
            }
            AsyncArrowCloseStart::WaitUntilClosed(waiter) => {
                let wait_shared = Arc::clone(&self.shared);
                let cancel_shared = Arc::clone(&self.shared);
                spawn_rust_db_task_with_attr_and_cancel(
                    py,
                    async move { wait_shared.wait_until_closed(waiter).await },
                    |py, ()| Ok(py.None()),
                    postgres_error_to_py,
                    move || {
                        cancel_shared.cancel_active_close();
                    },
                    POSTGRES_COMPLETION_DRAINER_ATTR,
                )
            }
        }
    }
}

#[pymethods]
impl PyAsyncPostgresArrowStream {
    /// Return the stream itself as an asynchronous `ArrowCursor` batch iterator.
    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Reserve an `OperationGeneration`, advance the portal off the event loop, and resolve to the next `vertebra._vertebra.ArrowCursor` batch.
    pub fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let _ = py.import("asyncio")?.call_method0("get_running_loop")?;
        let start = self
            .shared
            .begin_fetch_step()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let reserved_op = match &start {
            AsyncArrowFetchStart::OpenAndFetch { op_gen, .. }
            | AsyncArrowFetchStart::FetchNext { op_gen, .. } => Some(*op_gen),
            AsyncArrowFetchStart::Exhausted => None,
        };
        let drive_shared = Arc::clone(&self.shared);
        let cancel_shared = Arc::clone(&self.shared);
        match spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move { drive_shared.drive_fetch_step(start).await },
            |py, opt_guard| match opt_guard {
                Some(batch_guard) => batch_guard.into_py_cursor(py),
                None => Err(PyStopAsyncIteration::new_err(())),
            },
            postgres_error_to_py,
            move || {
                cancel_shared.cancel_fetch(false);
            },
            POSTGRES_COMPLETION_DRAINER_ATTR,
        ) {
            Ok(awaitable) => Ok(awaitable),
            Err(err) => {
                if let Some(op_gen) = reserved_op {
                    self.shared.abort_unspawned_fetch(op_gen);
                }
                Err(err)
            }
        }
    }

    /// Idempotently close the Arrow portal stream across `Unopened`, `Fetching`, `Open`, `Closing`, and `Closed` states.
    pub fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_aclose_inner(py)
    }

    /// Enter an `async with` scope resolving to this `AsyncPostgresArrowStream`.
    pub fn __aenter__<'py>(slf: PyRef<'py, Self>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let py = slf.py();
        let py_self: Py<Self> = slf.into_pyobject(py)?.unbind();
        let awaitable = spawn_rust_db_task_with_attr(
            py,
            async { Ok::<(), PostgresError>(()) },
            move |_py, ()| Ok(py_self.into_any()),
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )?;
        Bound::new(py, awaitable)
    }

    /// Idempotently close the stream on `async with` exit without suppressing exceptions.
    #[pyo3(signature = (exc_type = None, exc_val = None, exc_tb = None))]
    pub fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<&Bound<'py, PyAny>>,
        exc_val: Option<&Bound<'py, PyAny>>,
        exc_tb: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let _ = (exc_type, exc_val, exc_tb);
        self.spawn_aclose_inner(py)
    }

    fn __repr__(&self) -> String {
        format!(
            "AsyncPostgresArrowStream(state={:?})",
            self.shared.state_label()
        )
    }
}

#[pymethods]
impl PyAsyncPostgresClient {
    #[new]
    #[pyo3(signature = (
        *,
        user,
        database,
        host = "localhost",
        port = 5432,
        password = None,
        unix_socket = None,
        sslmode = "verify-full",
        ca_pem = None,
        client_cert_pem = None,
        client_key_pem = None,
        server_name = None,
        channel_binding = "prefer",
        connect_timeout = 10.0,
        application_name = "vertebra",
        session_gucs = None,
        keepalives = true,
        keepalives_idle = None,
        keepalives_interval = None,
        keepalives_count = None,
        cloud_sql_instance = None,
        cloud_sql_ip_type = "PRIVATE",
        alloydb_instance_uri = None,
        alloydb_ip_type = "PRIVATE",
        enable_iam_auth = false,
        cloud_auth_options = None,
        iam_auth_options = None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        user: String,
        database: String,
        host: &str,
        #[pyo3(from_py_with = extract_strict_i64)] port: i64,
        password: Option<String>,
        unix_socket: Option<String>,
        sslmode: &str,
        ca_pem: Option<Vec<u8>>,
        client_cert_pem: Option<Vec<u8>>,
        client_key_pem: Option<Vec<u8>>,
        server_name: Option<String>,
        channel_binding: &str,
        #[pyo3(from_py_with = extract_strict_f64)] connect_timeout: f64,
        application_name: &str,
        #[pyo3(from_py_with = extract_session_gucs)] session_gucs: Option<HashMap<String, String>>,
        keepalives: bool,
        #[pyo3(from_py_with = extract_strict_opt_i64)] keepalives_idle: Option<i64>,
        #[pyo3(from_py_with = extract_strict_opt_i64)] keepalives_interval: Option<i64>,
        #[pyo3(from_py_with = extract_strict_opt_i64)] keepalives_count: Option<i64>,
        cloud_sql_instance: Option<String>,
        cloud_sql_ip_type: &str,
        alloydb_instance_uri: Option<String>,
        alloydb_ip_type: &str,
        enable_iam_auth: bool,
        #[pyo3(from_py_with = extract_cloud_auth_options)] cloud_auth_options: Option<
            HashMap<String, String>,
        >,
        #[pyo3(from_py_with = extract_iam_auth_options)] iam_auth_options: Option<
            HashMap<String, String>,
        >,
    ) -> PyResult<Self> {
        let cfg = build_validated_config(
            py,
            user,
            database,
            host.to_string(),
            port,
            password,
            unix_socket,
            sslmode.to_string(),
            ca_pem,
            client_cert_pem,
            client_key_pem,
            server_name,
            channel_binding.to_string(),
            connect_timeout,
            application_name.to_string(),
            session_gucs,
            keepalives,
            keepalives_idle,
            keepalives_interval,
            keepalives_count,
            cloud_sql_instance,
            cloud_sql_ip_type.to_string(),
            alloydb_instance_uri,
            alloydb_ip_type.to_string(),
            enable_iam_auth,
            cloud_auth_options,
            iam_auth_options,
        )?;
        Ok(Self {
            session: Arc::new(SessionHandle::from_config(cfg)),
        })
    }

    /// Establish a physical PostgreSQL session asynchronously and publish `Ready` exclusively inside `map_ok`.
    pub fn connect(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        match self
            .session
            .begin_client_connect()
            .map_err(|e| postgres_error_to_py(py, e))?
        {
            ClientBeginConnect::AlreadyReady => spawn_rust_db_task_with_attr(
                py,
                async { Ok::<(), PostgresError>(()) },
                |py, ()| Ok(py.None()),
                postgres_error_to_py,
                POSTGRES_COMPLETION_DRAINER_ATTR,
            ),
            ClientBeginConnect::Attempt(attempt) => spawn_rust_db_task_with_attr(
                py,
                async move { attempt.run().await },
                |py, pending| {
                    pending.commit().map_err(|e| postgres_error_to_py(py, e))?;
                    Ok(py.None())
                },
                postgres_error_to_py,
                POSTGRES_COMPLETION_DRAINER_ATTR,
            ),
        }
    }

    /// Synchronously mark the session `Closed` and schedule asynchronous `Terminate` (`'X'`) or pool return shutdown.
    pub fn close(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let action = self
            .session
            .begin_close_action()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                match action {
                    CloseAction::Immediate(Some(backing)) => {
                        backing.close().await?;
                    }
                    CloseAction::Immediate(None) => {}
                    CloseAction::WaitInFlight(notified) => {
                        session.wait_close_inflight(notified).await;
                    }
                }
                Ok::<(), PostgresError>(())
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }

    /// Trigger an out-of-band generation-fenced protocol `CancelRequest` asynchronously without waiting on the session execution lock.
    pub fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let awaitable = spawn_rust_db_task_with_attr(
            py,
            async move {
                let _ = session.cancel().await;
                Ok::<(), PostgresError>(())
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )?;
        Bound::new(py, awaitable)
    }

    /// Execute a single-statement PostgreSQL Protocol 3.0 extended query asynchronously, resolving to `PostgresQueryResult`.
    #[pyo3(signature = (sql, parameters = None, *, parameter_oids = None, timeout = None))]
    pub fn query<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        parameters: Option<&Bound<'py, PyAny>>,
        parameter_oids: Option<&Bound<'py, PyAny>>,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let timeout_secs = extract_strict_opt_f64(timeout)?;
        let timeout_dur = CommandBudget::validate_optional_seconds(timeout_secs)
            .map_err(|e| postgres_error_to_py(py, e))?;
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let budget = match CommandBudget::start_from_duration(timeout_dur) {
            Ok(b) => b,
            Err(err) => {
                session.unreserve_operation_generation(op_gen);
                return Err(postgres_error_to_py(py, err));
            }
        };
        let sql_owned = sql.to_owned();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .query_as_with_generation_and_budget(
                        op_gen,
                        None,
                        &sql_owned,
                        &params,
                        oids.as_deref(),
                        budget,
                    )
                    .await
            },
            |py, res| query_result_to_python(py, &res),
            postgres_error_to_py,
        )
    }

    /// Execute a multi-statement PostgreSQL Protocol 3.0 simple query (`'Q'`) asynchronously, resolving to `tuple[PostgresQueryResult, ...]`.
    #[pyo3(signature = (sql, *, timeout = None))]
    pub fn query_multi<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let timeout_secs = extract_strict_opt_f64(timeout)?;
        let timeout_dur = CommandBudget::validate_optional_seconds(timeout_secs)
            .map_err(|e| postgres_error_to_py(py, e))?;
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let budget = match CommandBudget::start_from_duration(timeout_dur) {
            Ok(b) => b,
            Err(err) => {
                session.unreserve_operation_generation(op_gen);
                return Err(postgres_error_to_py(py, err));
            }
        };
        let sql_owned = sql.to_owned();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .simple_query_with_generation_and_budget(op_gen, &sql_owned, budget)
                    .await
            },
            |py, results| {
                let mut py_results = Vec::with_capacity(results.len());
                for res in &results {
                    py_results.push(query_result_to_python(py, res)?);
                }
                Ok(PyTuple::new(py, py_results)?.into_any().unbind())
            },
            postgres_error_to_py,
        )
    }

    /// Open a session-pinned PostgreSQL portal inside an explicit transaction or nested savepoint scope asynchronously.
    #[pyo3(signature = (sql, parameters = None, *, parameter_oids = None, batch_size = 65536))]
    pub fn open_portal<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        parameters: Option<&Bound<'py, PyAny>>,
        parameter_oids: Option<&Bound<'py, PyAny>>,
        #[pyo3(from_py_with = extract_batch_size)] batch_size: i32,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let sql_owned = sql.to_owned();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .open_portal_with_generation(
                        op_gen,
                        &sql_owned,
                        &params,
                        oids.as_deref(),
                        batch_size,
                    )
                    .await
            },
            |py, portal| Ok(Py::new(py, PyAsyncPostgresPortal::from_portal(portal))?.into_any()),
            postgres_error_to_py,
        )
    }

    /// Execute a single-statement query directly into a frozen `PostgresArrowResult` asynchronously without building Python rows.
    #[pyo3(signature = (
        sql,
        parameters = None,
        *,
        parameter_oids = None,
        timeout = None,
        threshold_rows = 256,
        max_result_bytes = 268435456
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn select_to_arrow<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        parameters: Option<&Bound<'py, PyAny>>,
        parameter_oids: Option<&Bound<'py, PyAny>>,
        timeout: Option<&Bound<'py, PyAny>>,
        #[pyo3(from_py_with = extract_threshold_rows)] threshold_rows: usize,
        #[pyo3(from_py_with = extract_max_result_bytes)] max_result_bytes: usize,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let timeout_secs = extract_strict_opt_f64(timeout)?;
        let timeout_dur = CommandBudget::validate_optional_seconds(timeout_secs)
            .map_err(|e| postgres_error_to_py(py, e))?;
        if sql.as_bytes().contains(&0) {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Data(
                    "SQL query string cannot contain embedded NUL byte".to_string(),
                ),
            ));
        }
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let budget = match CommandBudget::start_from_duration(timeout_dur) {
            Ok(b) => b,
            Err(err) => {
                session.unreserve_operation_generation(op_gen);
                return Err(postgres_error_to_py(py, err));
            }
        };
        let sql_owned = sql.to_owned();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                execute_select_to_arrow(
                    &session,
                    op_gen,
                    &sql_owned,
                    &params,
                    oids.as_deref(),
                    None,
                    budget,
                    threshold_rows,
                    max_result_bytes,
                )
                .await
            },
            arrow_output_into_py_result,
            postgres_error_to_py,
        )
    }

    /// Synchronously validate options and construct a cancellation-safe `AsyncPostgresArrowStream` that lazily opens a portal on first `__anext__`.
    #[pyo3(signature = (
        sql,
        parameters = None,
        *,
        parameter_oids = None,
        batch_size = 65536,
        columns = None
    ))]
    pub fn stream_arrow_batches(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        parameter_oids: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = extract_arrow_batch_size)] batch_size: i32,
        columns: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyAsyncPostgresArrowStream> {
        let cols_owned = extract_opt_copy_columns(columns)?;
        if sql.as_bytes().contains(&0) {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Data(
                    "SQL query string cannot contain embedded NUL byte".to_string(),
                ),
            ));
        }
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        Ok(PyAsyncPostgresArrowStream::new_unopened(
            Arc::clone(&self.session),
            UnopenedArrowPortalArgs {
                sql: sql.to_owned(),
                parameters: params,
                parameter_oids: oids,
                batch_size,
                columns: cols_owned,
            },
        ))
    }

    /// Asynchronously open an exclusive binary `COPY ... TO STDOUT (FORMAT binary)` streaming iterator.
    pub fn copy_to<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let wrapper_session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let sql_owned = sql.to_owned();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .copy_to_with_generation_as(op_gen, &sql_owned, None)
                    .await
            },
            move |py, stream| {
                Ok(Py::new(
                    py,
                    PyAsyncPostgresCopyOut::from_stream(stream, wrapper_session, op_gen),
                )?
                .into_any())
            },
            postgres_error_to_py,
        )
    }

    /// Asynchronously open an exclusive binary `COPY ... FROM STDIN (FORMAT binary)` write stream.
    pub fn copy_from<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let wrapper_session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let sql_owned = sql.to_owned();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .copy_from_with_generation_as(op_gen, &sql_owned, None)
                    .await
            },
            move |py, stream| {
                Ok(Py::new(
                    py,
                    PyAsyncPostgresCopyIn::from_stream(stream, wrapper_session, op_gen),
                )?
                .into_any())
            },
            postgres_error_to_py,
        )
    }

    /// Validate schema against `pg_catalog`, open an async binary `COPY ... FROM STDIN` stream, and flush `first_batch_ipc`.
    #[pyo3(signature = (table_name, first_batch_ipc, *, columns = None, schema_name = None))]
    pub fn copy_from_arrow<'py>(
        &self,
        py: Python<'py>,
        table_name: &str,
        first_batch_ipc: &Bound<'py, PyAny>,
        columns: Option<&Bound<'py, PyAny>>,
        schema_name: Option<&str>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let cols_owned = extract_opt_copy_columns(columns)?;
        let ipc_owned = extract_ipc_buffer_bytes(py, first_batch_ipc, "first_batch_ipc")?;
        let session = Arc::clone(&self.session);
        let wrapper_session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let table_owned = table_name.to_owned();
        let schema_owned = schema_name.map(str::to_owned);
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .copy_from_arrow_with_generation_as(
                        op_gen,
                        &table_owned,
                        schema_owned.as_deref(),
                        &ipc_owned,
                        cols_owned.as_deref(),
                        None,
                    )
                    .await
            },
            move |py, stream| {
                Ok(Py::new(
                    py,
                    PyAsyncPostgresCopyIn::from_stream(stream, wrapper_session, op_gen),
                )?
                .into_any())
            },
            postgres_error_to_py,
        )
    }

    /// Open a retained pipeline transaction (`BEGIN` at `'I'`) or savepoint (`SAVEPOINT` at `'T'`) asynchronously and resolve to its integer scope identifier.
    pub fn pipeline_begin<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move { session.pipeline_begin_with_generation(op_gen).await },
            |py, token| Ok(token.0.into_pyobject(py)?.into_any().unbind()),
            postgres_error_to_py,
        )
    }

    /// Submit `operations` inside `scope_id` asynchronously as one single-`Sync` extended pipeline batch while retaining the open transaction/savepoint scope.
    pub fn pipeline_results<'py>(
        &self,
        py: Python<'py>,
        #[pyo3(from_py_with = extract_scope_id)] scope_id: PipelineScopeToken,
        operations: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let batch = match extract_pipeline_batch(py, operations) {
            Ok(b) => b,
            Err(err) => {
                let _ = self
                    .session
                    .mark_pipeline_scope_failed(scope_id, err.clone());
                return Err(pipeline_error_to_py(py, err));
            }
        };
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .pipeline_results_with_generation(op_gen, scope_id, batch)
                    .await
            },
            |py, outcomes| outcomes_to_python(py, &outcomes),
            pipeline_error_to_py,
        )
    }

    /// Commit/release (`success=True` with no prior failure) or roll back (`success=False` or persistent failure) the retained pipeline scope `scope_id` asynchronously.
    pub fn pipeline_finish<'py>(
        &self,
        py: Python<'py>,
        #[pyo3(from_py_with = extract_scope_id)] scope_id: PipelineScopeToken,
        #[pyo3(from_py_with = extract_strict_bool)] success: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .pipeline_finish_with_generation(op_gen, scope_id, success)
                    .await
            },
            |py, ()| Ok(py.None()),
            pipeline_error_to_py,
        )
    }

    /// Start an explicit transaction (`BEGIN`) asynchronously.
    pub fn begin<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session.begin_as_with_generation(op_gen, None).await?;
                Ok::<(), PostgresError>(())
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
        )
    }

    /// Commit the current transaction (`COMMIT`) asynchronously.
    pub fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session.commit_as_with_generation(op_gen, None).await?;
                Ok::<(), PostgresError>(())
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
        )
    }

    /// Roll back the current transaction (`ROLLBACK`) asynchronously.
    pub fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session.rollback_as_with_generation(op_gen, None).await?;
                Ok::<(), PostgresError>(())
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
        )
    }

    /// Create a numeric savepoint (`SAVEPOINT vtb_sp_<token>`) asynchronously.
    pub fn savepoint<'py>(
        &self,
        py: Python<'py>,
        #[pyo3(from_py_with = extract_strict_u64)] token: u64,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .savepoint_as_with_generation(op_gen, None, token)
                    .await?;
                Ok::<(), PostgresError>(())
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
        )
    }

    /// Release an active numeric savepoint (`RELEASE SAVEPOINT vtb_sp_<token>`) asynchronously.
    pub fn release_savepoint<'py>(
        &self,
        py: Python<'py>,
        #[pyo3(from_py_with = extract_strict_u64)] token: u64,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .release_savepoint_as_with_generation(op_gen, None, token)
                    .await?;
                Ok::<(), PostgresError>(())
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
        )
    }

    /// Roll back to an active numeric savepoint (`ROLLBACK TO SAVEPOINT vtb_sp_<token>`) asynchronously.
    pub fn rollback_to_savepoint<'py>(
        &self,
        py: Python<'py>,
        #[pyo3(from_py_with = extract_strict_u64)] token: u64,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        spawn_cancellable_op(
            py,
            &self.session,
            op_gen,
            async move {
                session
                    .rollback_to_savepoint_as_with_generation(op_gen, None, token)
                    .await?;
                Ok::<(), PostgresError>(())
            },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
        )
    }

    /// Return `True` unless an authenticated session has reached `ReadyForQuery(Idle)` and has not been closed.
    #[getter]
    pub fn closed(&self) -> bool {
        self.session.is_client_closed()
    }

    /// Return a newly allocated dictionary copy of the session's `ParameterStatus` values.
    #[getter]
    pub fn parameters<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in self.session.parameters() {
            dict.set_item(k, v)?;
        }
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "AsyncPostgresClient(host={:?}, port={}, database={:?}, user={:?}, closed={})",
            self.session.config().host,
            self.session.config().port,
            self.session.config().database,
            self.session.config().user,
            self.session.is_client_closed(),
        )
    }
}
