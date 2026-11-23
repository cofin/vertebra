//! PyO3 Valkey Pub/Sub listener wrappers (`SyncValkeyListener`, `AsyncValkeyListener`).

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;
use tokio::sync::Notify;
use vertebra_db::py_async::{
    RustDbTaskAwaitable, block_on_db, spawn_rust_db_task_with_attr_and_cancel,
};
use vertebra_resp::config::{ValkeyConfigInner, await_pending_python_token};
use vertebra_resp::error::ValkeyError;
pub use vertebra_resp::pubsub::{
    PendingDeliveryGuard, RespPubSubMessage, ValkeyListenerCore, ValkeyPubSubMessage,
    extract_string_args,
};

use crate::VALKEY_COMPLETION_DRAINER_ATTR;

fn drain_listener_messages(
    core: &Arc<ValkeyListenerCore>,
    py: Python<'_>,
    max_messages: usize,
) -> PyResult<Py<PyAny>> {
    let core = Arc::clone(core);
    let items = py.detach(|| block_on_db(async move { core.drain_messages(max_messages).await }));
    let list = PyList::empty(py);
    for item in items {
        list.append(item.to_py_dict(py)?)?;
    }
    Ok(list.into_any().unbind())
}

fn spawn_valkey_listener_unit<'py, F>(
    py: Python<'py>,
    fut: F,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>>
where
    F: std::future::Future<Output = Result<(), ValkeyError>> + Send + 'static,
{
    spawn_rust_db_task_with_attr_and_cancel(
        py,
        fut,
        |py, ()| Ok(py.None()),
        |py, e| e.to_py_err(py),
        || {},
        VALKEY_COMPLETION_DRAINER_ATTR,
    )
}

/// Synchronous Valkey Pub/Sub (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`) listener.
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

    fn run_sub(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyAny>,
        need_token: bool,
        kind: u8,
    ) -> PyResult<()> {
        let items = extract_string_args(args)?;
        if need_token {
            self.core
                .config()
                .ensure_python_token_sync(py, false)
                .map_err(|e| e.to_py_err(py))?;
        }
        let core = Arc::clone(&self.core);
        py.detach(|| {
            block_on_db(async move {
                match kind {
                    0 => core.subscribe(&items).await,
                    1 => core.psubscribe(&items).await,
                    2 => core.ssubscribe(&items).await,
                    3 => core.unsubscribe(&items).await,
                    4 => core.punsubscribe(&items).await,
                    _ => core.sunsubscribe(&items).await,
                }
            })
        })
        .map_err(|e| e.to_py_err(py))
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
        self.run_sub(py, channels, true, 0)
    }

    #[pyo3(signature = (*patterns))]
    pub fn psubscribe(&self, py: Python<'_>, patterns: &Bound<'_, PyAny>) -> PyResult<()> {
        self.run_sub(py, patterns, true, 1)
    }

    #[pyo3(signature = (*channels))]
    pub fn ssubscribe(&self, py: Python<'_>, channels: &Bound<'_, PyAny>) -> PyResult<()> {
        self.run_sub(py, channels, true, 2)
    }

    #[pyo3(signature = (*channels))]
    pub fn unsubscribe(&self, py: Python<'_>, channels: &Bound<'_, PyAny>) -> PyResult<()> {
        self.run_sub(py, channels, false, 3)
    }

    #[pyo3(signature = (*patterns))]
    pub fn punsubscribe(&self, py: Python<'_>, patterns: &Bound<'_, PyAny>) -> PyResult<()> {
        self.run_sub(py, patterns, false, 4)
    }

    #[pyo3(signature = (*channels))]
    pub fn sunsubscribe(&self, py: Python<'_>, channels: &Bound<'_, PyAny>) -> PyResult<()> {
        self.run_sub(py, channels, false, 5)
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
        drain_listener_messages(&self.core, py, max_messages)
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

    fn spawn_sub<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'_, PyAny>,
        need_token: bool,
        kind: u8,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let items = extract_string_args(args)?;
        let pending_tok = if need_token {
            self.core
                .config()
                .prepare_python_token_async(py, false)
                .map_err(|e| e.to_py_err(py))?
        } else {
            None
        };
        let core = Arc::clone(&self.core);
        spawn_valkey_listener_unit(py, async move {
            if let Some(rx) = pending_tok {
                await_pending_python_token(rx).await?;
            }
            match kind {
                0 => core.subscribe(&items).await,
                1 => core.psubscribe(&items).await,
                2 => core.ssubscribe(&items).await,
                3 => core.unsubscribe(&items).await,
                4 => core.punsubscribe(&items).await,
                _ => core.sunsubscribe(&items).await,
            }
        })
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
        self.spawn_sub(py, channels, true, 0)
    }

    #[pyo3(signature = (*patterns))]
    pub fn psubscribe<'py>(
        &self,
        py: Python<'py>,
        patterns: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_sub(py, patterns, true, 1)
    }

    #[pyo3(signature = (*channels))]
    pub fn ssubscribe<'py>(
        &self,
        py: Python<'py>,
        channels: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_sub(py, channels, true, 2)
    }

    #[pyo3(signature = (*channels))]
    pub fn unsubscribe<'py>(
        &self,
        py: Python<'py>,
        channels: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_sub(py, channels, false, 3)
    }

    #[pyo3(signature = (*patterns))]
    pub fn punsubscribe<'py>(
        &self,
        py: Python<'py>,
        patterns: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_sub(py, patterns, false, 4)
    }

    #[pyo3(signature = (*channels))]
    pub fn sunsubscribe<'py>(
        &self,
        py: Python<'py>,
        channels: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_sub(py, channels, false, 5)
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
        drain_listener_messages(&self.core, py, max_messages)
    }

    pub fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let core = Arc::clone(&self.core);
        spawn_valkey_listener_unit(py, async move {
            core.close().await;
            Ok(())
        })
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
