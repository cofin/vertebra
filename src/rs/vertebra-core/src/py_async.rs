//! Shared PyO3 Tokio completion bridge for Vertebra native extensions.
//!
//! Each native extension crate (`vertebra._vertebra`, `vertebra._storage`,
//! `vertebra._bigquery`, `vertebra._spanner`, and future wheels) compiles its own copy
//! of this module with an extension-specific `attr_name` (such as
//! `"_vertebra_core_completion_drainer"` or `"_vertebra_storage_drainer"`) so multiple
//! `.so` extensions attach independent [`AsyncCompletionQueue`] drainers to the same
//! Python `asyncio` event loop without `PyTypeObject` cast collisions.

use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};

use pyo3::exceptions::PyRuntimeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyType, PyWeakref, PyWeakrefMethods, PyWeakrefReference};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::AbortHandle;

use crate::notify::{AsyncCompletionQueue, Notifier};

static GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn get_running_loop<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let getter = GET_RUNNING_LOOP.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import("asyncio")?.getattr("get_running_loop")?.unbind())
    })?;
    getter.bind(py).call0()
}

struct CatchUnwindFuture<F>(F);

impl<F: Future> Future for CatchUnwindFuture<F> {
    type Output = Result<F::Output, Box<dyn Any + Send>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `CatchUnwindFuture` only projects a structural pin to its single field `0`.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        match catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Ready(val)) => Poll::Ready(Ok(val)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        format!("Rust task panicked: {text}")
    } else if let Some(text) = payload.downcast_ref::<String>() {
        format!("Rust task panicked: {text}")
    } else {
        "Rust task panicked".to_string()
    }
}

fn make_task_panic_err(py: Python<'_>, message: String) -> PyErr {
    if let Ok(core_mod) = py.import("vertebra._vertebra")
        && let Ok(exc_attr) = core_mod.getattr("TaskPanicError")
        && let Ok(exc_ty) = exc_attr.cast_into::<PyType>()
    {
        return PyErr::from_type(exc_ty, (message,));
    }
    PyRuntimeError::new_err(message)
}

fn is_future_cancelled(py: Python<'_>, fut: &Bound<'_, PyAny>) -> bool {
    fut.call_method0(intern!(py, "cancelled"))
        .and_then(|v| v.extract())
        .unwrap_or(false)
}

#[pyclass(
    name = "_RustTaskCanceller",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
struct TaskCanceller {
    abort: Mutex<Option<AbortHandle>>,
    cancelled_early: AtomicBool,
}

#[pymethods]
impl TaskCanceller {
    fn __call__(&self, py: Python<'_>, future: &Bound<'_, PyAny>) -> PyResult<()> {
        if future
            .call_method0(intern!(py, "cancelled"))?
            .extract::<bool>()?
        {
            let handle = {
                let guard = self.abort.lock().unwrap_or_else(|e| e.into_inner());
                self.cancelled_early.store(true, Ordering::Release);
                guard.clone()
            };
            if let Some(abort) = handle {
                abort.abort();
            }
        }
        Ok(())
    }
}

type BoxedCancelCallback = Box<dyn FnOnce() + Send + 'static>;

#[pyclass(
    name = "_RustCustomTaskCanceller",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
struct CustomTaskCanceller {
    on_cancel: Arc<Mutex<Option<BoxedCancelCallback>>>,
    cancelled_early: Arc<AtomicBool>,
}

#[pymethods]
impl CustomTaskCanceller {
    fn __call__(&self, py: Python<'_>, future: &Bound<'_, PyAny>) -> PyResult<()> {
        if future
            .call_method0(intern!(py, "cancelled"))?
            .extract::<bool>()?
        {
            let callback = {
                let mut guard = self.on_cancel.lock().unwrap_or_else(|e| e.into_inner());
                self.cancelled_early.store(true, Ordering::Release);
                guard.take()
            };
            if let Some(cb) = callback {
                cb();
            }
        }
        Ok(())
    }
}

/// Boxed Python result converter invoked on the Python event-loop thread when a task finishes.
pub type OutcomeProducer =
    Box<dyn for<'py> FnOnce(Python<'py>, Bound<'py, PyAny>) -> PyResult<Py<PyAny>> + Send>;

/// Completed background task queued into an [`AsyncCompletionQueue`].
pub struct CompletedTask {
    pub future: Py<PyAny>,
    pub produce: OutcomeProducer,
}

static SHARED_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Return a handle to the shared multi-threaded Tokio runtime (`VERTEBRA_IO_THREADS` overrides the worker count).
pub fn runtime_handle() -> &'static Handle {
    SHARED_RUNTIME
        .get_or_init(|| {
            let default_threads = std::thread::available_parallelism()
                .map(|n| n.get().clamp(2, 16))
                .unwrap_or(4);
            let worker_threads = std::env::var("VERTEBRA_IO_THREADS")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .map(|n| n.clamp(1, 64))
                .unwrap_or(default_threads);
            Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .enable_all()
                .thread_name("vtb-core-rt")
                .build()
                .expect("failed to build vertebra-core tokio runtime")
        })
        .handle()
}

/// Drive a Rust future synchronously on the shared Tokio runtime.
pub fn block_on_runtime<F>(fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    if Handle::try_current().is_ok() {
        std::thread::scope(|s| {
            s.spawn(|| runtime_handle().block_on(fut))
                .join()
                .expect("shared runtime thread panicked")
        })
    } else {
        runtime_handle().block_on(fut)
    }
}

struct CompletionRing {
    completion: AsyncCompletionQueue<CompletedTask>,
    fallback_wakeup: Mutex<Option<(Py<PyWeakref>, &'static str)>>,
}

impl CompletionRing {
    fn signal_fallback(&self) {
        if self.completion.notifier().is_some() {
            return;
        }
        Python::attach(|py| {
            let target = {
                let mut guard = self
                    .fallback_wakeup
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                match guard.as_ref() {
                    Some((weak_loop, attr_name)) => match weak_loop.bind(py).upgrade() {
                        Some(loop_bound) => Some((loop_bound, *attr_name)),
                        None => {
                            *guard = None;
                            None
                        }
                    },
                    None => None,
                }
            };
            if let Some((loop_bound, attr_name)) = target
                && let Ok(drainer) = loop_bound.getattr(attr_name)
            {
                let _ = loop_bound.call_method1("call_soon_threadsafe", (drainer,));
            }
        });
    }
}

#[pyclass(
    name = "_RustCompletionDrainer",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
pub struct CompletionDrainer {
    ring: Arc<CompletionRing>,
    batch: Mutex<Vec<CompletedTask>>,
}

#[pymethods]
impl CompletionDrainer {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        self.ring.completion.clear_notifier();
        let mut batch = {
            let mut guard = self.batch.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *guard)
        };
        let set_result_name = intern!(py, "set_result");
        let set_exception_name = intern!(py, "set_exception");
        while self.ring.completion.drain_into(&mut batch) {
            for item in batch.drain(..) {
                let fut = item.future.bind(py);
                if is_future_cancelled(py, fut) {
                    continue;
                }
                let res = catch_unwind(AssertUnwindSafe(|| (item.produce)(py, fut.clone())));
                if is_future_cancelled(py, fut) {
                    continue;
                }
                match res {
                    Ok(Ok(val)) => {
                        let _ = fut.call_method1(set_result_name, (val,));
                    }
                    Ok(Err(err)) => {
                        let _ = fut.call_method1(set_exception_name, (err.into_value(py),));
                    }
                    Err(payload) => {
                        let err = make_task_panic_err(py, panic_message(payload));
                        let _ = fut.call_method1(set_exception_name, (err.into_value(py),));
                    }
                }
            }
        }
        batch.clear();
        let mut guard = self.batch.lock().unwrap_or_else(|e| e.into_inner());
        if guard.capacity() < batch.capacity() {
            *guard = batch;
        }
        Ok(())
    }
}

fn ensure_completion_ring(
    py: Python<'_>,
    loop_obj: &Bound<'_, PyAny>,
    attr_name: &'static str,
) -> PyResult<Arc<CompletionRing>> {
    if let Ok(existing) = loop_obj.getattr(attr_name)
        && let Ok(drainer) = existing.cast::<CompletionDrainer>()
    {
        return Ok(Arc::clone(&drainer.get().ring));
    }

    #[cfg(unix)]
    if let Ok(notifier) = Notifier::new() {
        let fd = notifier.as_raw_fd();
        let temp_ring = Arc::new(CompletionRing {
            completion: AsyncCompletionQueue::new(Some(notifier)),
            fallback_wakeup: Mutex::new(None),
        });
        let drainer_py = Py::new(
            py,
            CompletionDrainer {
                ring: Arc::clone(&temp_ring),
                batch: Mutex::new(Vec::with_capacity(16)),
            },
        )?;
        if loop_obj
            .call_method1("add_reader", (fd, drainer_py.bind(py)))
            .is_ok()
        {
            if let Err(err) = loop_obj.setattr(attr_name, drainer_py.bind(py)) {
                let _ = loop_obj.call_method1("remove_reader", (fd,));
                return Err(err);
            }
            return Ok(temp_ring);
        }
    }

    let weak_loop = PyWeakrefReference::new(loop_obj)?
        .into_any()
        .cast_into::<PyWeakref>()?
        .unbind();
    let ring = Arc::new(CompletionRing {
        completion: AsyncCompletionQueue::new(None),
        fallback_wakeup: Mutex::new(Some((weak_loop, attr_name))),
    });
    let drainer_py = Py::new(
        py,
        CompletionDrainer {
            ring: Arc::clone(&ring),
            batch: Mutex::new(Vec::with_capacity(16)),
        },
    )?;
    loop_obj.setattr(attr_name, drainer_py.bind(py))?;
    Ok(ring)
}

/// Native Rust awaitable backed by the shared Tokio runtime and [`AsyncCompletionQueue`].
#[pyclass(
    name = "RustTaskAwaitable",
    module = "vertebra._vertebra",
    frozen,
    immutable_type
)]
pub struct RustTaskAwaitable {
    future: Py<PyAny>,
    awaited: AtomicBool,
    on_unawaited_drop: Arc<Mutex<Option<BoxedCancelCallback>>>,
}

impl RustTaskAwaitable {
    /// Return a cloned Python reference to the underlying `asyncio.Future`.
    pub fn future_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.future.clone_ref(py)
    }
}

#[pymethods]
impl RustTaskAwaitable {
    fn __await__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.awaited.store(true, Ordering::Release);
        let iter = self
            .future
            .bind(py)
            .call_method0(intern!(py, "__await__"))?;
        Ok(iter.unbind())
    }
}

impl Drop for RustTaskAwaitable {
    fn drop(&mut self) {
        if !self.awaited.load(Ordering::Acquire) {
            let cb = self
                .on_unawaited_drop
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            if let Some(cancel_fn) = cb {
                cancel_fn();
            }
            let _ = Python::try_attach(|py| {
                let fut = self.future.bind(py);
                if let Ok(false) = fut
                    .call_method0(intern!(py, "done"))
                    .and_then(|v| v.extract::<bool>())
                {
                    let _ = fut.call_method0(intern!(py, "cancel"));
                }
            });
        }
    }
}

/// Spawn a background Tokio task and resolve its Python `asyncio.Future` via an `attr_name`-keyed
/// [`AsyncCompletionQueue`] drainer attached to the running event loop, passing the original Python
/// `Future` to `map_ok` so mappers can gate side effects on owning-loop cancellation.
pub fn spawn_rust_task_with_attr_and_future<F, T, E, M, EMap>(
    py: Python<'_>,
    fut: F,
    map_ok: M,
    map_err: EMap,
    attr_name: &'static str,
) -> PyResult<RustTaskAwaitable>
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
    M: for<'py> FnOnce(Python<'py>, Bound<'py, PyAny>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    EMap: FnOnce(Python<'_>, E) -> PyErr + Send + 'static,
{
    let loop_obj = get_running_loop(py)?;
    let py_fut = loop_obj.call_method0(intern!(py, "create_future"))?;
    let ring = ensure_completion_ring(py, &loop_obj, attr_name)?;
    let canceller = Py::new(
        py,
        TaskCanceller {
            abort: Mutex::new(None),
            cancelled_early: AtomicBool::new(false),
        },
    )?;
    py_fut.call_method1(intern!(py, "add_done_callback"), (canceller.bind(py),))?;
    let fut_for_cb = py_fut.clone().unbind();
    let fut_for_await = py_fut.unbind();

    let worker = runtime_handle().spawn(async move {
        let produce: OutcomeProducer = match CatchUnwindFuture(fut).await {
            Ok(Ok(value)) => Box::new(move |py, fut| map_ok(py, fut, value)),
            Ok(Err(err)) => Box::new(move |py, _fut| Err(map_err(py, err))),
            Err(payload) => {
                let message = panic_message(payload);
                Box::new(move |py, _fut| Err(make_task_panic_err(py, message)))
            }
        };
        let should_signal = ring.completion.push(CompletedTask {
            future: fut_for_cb,
            produce,
        });
        if should_signal {
            ring.signal_fallback();
        }
    });
    let abort = worker.abort_handle();
    let abort_for_drop = abort.clone();
    let abort_immediately = {
        let canceller_ref = canceller.get();
        let mut guard = canceller_ref
            .abort
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if canceller_ref.cancelled_early.load(Ordering::Acquire) {
            true
        } else {
            *guard = Some(abort);
            false
        }
    };
    if abort_immediately {
        abort_for_drop.abort();
    }

    Ok(RustTaskAwaitable {
        future: fut_for_await,
        awaited: AtomicBool::new(false),
        on_unawaited_drop: Arc::new(Mutex::new(Some(Box::new(move || abort_for_drop.abort())))),
    })
}

/// Spawn a background Tokio task and resolve its Python `asyncio.Future` via an `attr_name`-keyed
/// [`AsyncCompletionQueue`] drainer attached to the running event loop.
pub fn spawn_rust_task_with_attr<F, T, E, M, EMap>(
    py: Python<'_>,
    fut: F,
    map_ok: M,
    map_err: EMap,
    attr_name: &'static str,
) -> PyResult<RustTaskAwaitable>
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
    M: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    EMap: FnOnce(Python<'_>, E) -> PyErr + Send + 'static,
{
    spawn_rust_task_with_attr_and_future(
        py,
        fut,
        move |py, _py_fut, value| map_ok(py, value),
        map_err,
        attr_name,
    )
}

/// Spawn a background Tokio task with a custom Python-`Future` cancellation callback that triggers
/// protocol-level cancellation without aborting the cleanup-owning Tokio worker task.
pub fn spawn_rust_task_with_attr_and_cancel<'py, F, T, E, MapOk, MapErr, OnCancel>(
    py: Python<'py>,
    fut: F,
    map_ok: MapOk,
    map_err: MapErr,
    on_cancel: OnCancel,
    attr_name: &'static str,
) -> PyResult<Bound<'py, RustTaskAwaitable>>
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
    MapOk: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    MapErr: FnOnce(Python<'_>, E) -> PyErr + Send + 'static,
    OnCancel: FnOnce() + Send + 'static,
{
    let loop_obj = get_running_loop(py)?;
    let py_fut = loop_obj.call_method0(intern!(py, "create_future"))?;
    let ring = ensure_completion_ring(py, &loop_obj, attr_name)?;
    let cancel_slot: Arc<Mutex<Option<BoxedCancelCallback>>> = Arc::new(Mutex::new(None));
    let cancelled_early = Arc::new(AtomicBool::new(false));
    let canceller = Py::new(
        py,
        CustomTaskCanceller {
            on_cancel: Arc::clone(&cancel_slot),
            cancelled_early: Arc::clone(&cancelled_early),
        },
    )?;
    py_fut.call_method1(intern!(py, "add_done_callback"), (canceller,))?;
    let fut_for_cb = py_fut.clone().unbind();
    let awaitable = Bound::new(
        py,
        RustTaskAwaitable {
            future: py_fut.unbind(),
            awaited: AtomicBool::new(false),
            on_unawaited_drop: Arc::clone(&cancel_slot),
        },
    )?;

    let immediate_cancel = {
        let mut guard = cancel_slot.lock().unwrap_or_else(|e| e.into_inner());
        if cancelled_early.load(Ordering::Acquire) {
            Some(on_cancel)
        } else {
            *guard = Some(Box::new(on_cancel));
            None
        }
    };
    if let Some(cancel_fn) = immediate_cancel {
        cancel_fn();
    }

    runtime_handle().spawn(async move {
        let produce: OutcomeProducer = match CatchUnwindFuture(fut).await {
            Ok(Ok(value)) => Box::new(move |py, _fut| map_ok(py, value)),
            Ok(Err(err)) => Box::new(move |py, _fut| Err(map_err(py, err))),
            Err(payload) => {
                let message = panic_message(payload);
                Box::new(move |py, _fut| Err(make_task_panic_err(py, message)))
            }
        };
        let should_signal = ring.completion.push(CompletedTask {
            future: fut_for_cb,
            produce,
        });
        if should_signal {
            ring.signal_fallback();
        }
    });

    Ok(awaitable)
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use pyo3::prelude::*;
    use pyo3::types::{PyCFunction, PyDict, PyModule, PyTuple};

    use super::{
        block_on_runtime, ensure_completion_ring, spawn_rust_task_with_attr,
        spawn_rust_task_with_attr_and_cancel, spawn_rust_task_with_attr_and_future,
    };

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    pyo3::create_exception!(
        vertebra._vertebra,
        TestTaskPanicError,
        pyo3::exceptions::PyRuntimeError,
        "Test exception matching vertebra._vertebra.TaskPanicError."
    );

    #[test]
    fn ok_and_err_resolution_work_across_event_loop() {
        Python::initialize();
        Python::attach(|py| {
            let spawn_ok =
                PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                    spawn_rust_task_with_attr(
                        args.py(),
                        async { Ok::<i64, String>(123) },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyValueError::new_err(e),
                        "_core_test_ok_err",
                    )
                })
                .expect("spawn_ok");
            let spawn_err =
                PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                    spawn_rust_task_with_attr(
                        args.py(),
                        async { Err::<i64, String>("boom".to_string()) },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyValueError::new_err(e),
                        "_core_test_ok_err",
                    )
                })
                .expect("spawn_err");

            let globals = PyDict::new(py);
            globals.set_item("spawn_ok", spawn_ok).unwrap();
            globals.set_item("spawn_err", spawn_err).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "async def main():\n",
                "    val = await spawn_ok()\n",
                "    err_msg = None\n",
                "    try:\n",
                "        await spawn_err()\n",
                "    except ValueError as exc:\n",
                "        err_msg = str(exc)\n",
                "    return (val, err_msg)\n",
                "result = asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();
            let res: (i64, String) = globals
                .get_item("result")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(res, (123, "boom".to_string()));
        });
    }

    #[test]
    fn distinct_attr_names_coexist_on_single_event_loop() {
        Python::initialize();
        Python::attach(|py| {
            let spawn_a =
                PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                    spawn_rust_task_with_attr(
                        args.py(),
                        async { Ok::<i64, String>(10) },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_core_test_drainer_a",
                    )
                })
                .expect("spawn_a");
            let spawn_b =
                PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                    spawn_rust_task_with_attr(
                        args.py(),
                        async { Ok::<i64, String>(32) },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_core_test_drainer_b",
                    )
                })
                .expect("spawn_b");

            let globals = PyDict::new(py);
            globals.set_item("spawn_a", spawn_a).unwrap();
            globals.set_item("spawn_b", spawn_b).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "async def main():\n",
                "    loop = asyncio.get_running_loop()\n",
                "    setattr(loop, '_foreign_drainer', object())\n",
                "    a, b = await asyncio.gather(spawn_a(), spawn_b())\n",
                "    same = getattr(loop, '_core_test_drainer_a') is getattr(loop, '_core_test_drainer_b')\n",
                "    return (a + b, hasattr(loop, '_core_test_drainer_a'), hasattr(loop, '_core_test_drainer_b'), same)\n",
                "result = asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();
            let res: (i64, bool, bool, bool) = globals
                .get_item("result")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(res, (42, true, true, false));
        });
    }

    #[test]
    fn unawaited_drop_aborts_spawned_task_and_cancels_future() {
        Python::initialize();
        let dropped = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicBool::new(false));
        let dropped_clone = Arc::clone(&dropped);
        let completed_clone = Arc::clone(&completed);

        Python::attach(|py| {
            let captured_future: Arc<Mutex<Option<Py<PyAny>>>> = Arc::new(Mutex::new(None));
            let captured_for_spawn = Arc::clone(&captured_future);
            let spawn =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let guard = DropFlag(Arc::clone(&dropped_clone));
                    let done = Arc::clone(&completed_clone);
                    let aw = spawn_rust_task_with_attr(
                        args.py(),
                        async move {
                            let _guard = guard;
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            done.store(true, Ordering::SeqCst);
                            Ok::<i64, String>(1)
                        },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_core_test_unawaited_drop",
                    )?;
                    *captured_for_spawn.lock().unwrap() = Some(aw.future_handle(args.py()));
                    Bound::new(args.py(), aw).map(Bound::unbind)
                })
                .expect("spawn");

            let globals = PyDict::new(py);
            globals.set_item("spawn", spawn).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "async def main():\n",
                "    aw = spawn()\n",
                "    await asyncio.sleep(0.02)\n",
                "    del aw\n",
                "    await asyncio.sleep(0.02)\n",
                "asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();

            let fut_cancelled = captured_future
                .lock()
                .unwrap()
                .as_ref()
                .expect("future captured")
                .bind(py)
                .call_method0("cancelled")
                .unwrap()
                .extract::<bool>()
                .unwrap();
            assert!(fut_cancelled);

            py.detach(|| {
                let deadline = Instant::now() + Duration::from_secs(1);
                while !dropped.load(Ordering::SeqCst) && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
        });

        assert!(dropped.load(Ordering::SeqCst));
        assert!(!completed.load(Ordering::SeqCst));
    }

    #[test]
    fn in_flight_cancellation_aborts_spawned_tokio_task() {
        Python::initialize();
        let dropped = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicBool::new(false));
        let dropped_clone = Arc::clone(&dropped);
        let completed_clone = Arc::clone(&completed);

        Python::attach(|py| {
            let spawn =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let guard = DropFlag(Arc::clone(&dropped_clone));
                    let done = Arc::clone(&completed_clone);
                    spawn_rust_task_with_attr(
                        args.py(),
                        async move {
                            let _guard = guard;
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            done.store(true, Ordering::SeqCst);
                            Ok::<i64, String>(1)
                        },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_core_test_in_flight_cancel",
                    )
                })
                .expect("spawn");

            let globals = PyDict::new(py);
            globals.set_item("spawn", spawn).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "async def main():\n",
                "    t = asyncio.ensure_future(spawn())\n",
                "    await asyncio.sleep(0.03)\n",
                "    t.cancel()\n",
                "    cancelled = False\n",
                "    try:\n",
                "        await t\n",
                "    except asyncio.CancelledError:\n",
                "        cancelled = True\n",
                "    return cancelled\n",
                "result = asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();
            let cancelled: bool = globals
                .get_item("result")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert!(cancelled);

            py.detach(|| {
                let deadline = Instant::now() + Duration::from_secs(1);
                while !dropped.load(Ordering::SeqCst) && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
        });

        assert!(dropped.load(Ordering::SeqCst));
        assert!(!completed.load(Ordering::SeqCst));
    }

    #[test]
    fn cancel_callback_runs_without_aborting_cleanup() {
        Python::initialize();
        let callback_count = Arc::new(AtomicUsize::new(0));
        let cleanup_completed = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(tokio::sync::Notify::new());

        let cb_count_spawn = Arc::clone(&callback_count);
        let cleanup_spawn = Arc::clone(&cleanup_completed);
        let notify_spawn = Arc::clone(&notify);

        Python::attach(|py| {
            let spawn =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let cb_count = Arc::clone(&cb_count_spawn);
                    let cleanup_flag = Arc::clone(&cleanup_spawn);
                    let notify_worker = Arc::clone(&notify_spawn);
                    let notify_cancel = Arc::clone(&notify_spawn);
                    spawn_rust_task_with_attr_and_cancel(
                        args.py(),
                        async move {
                            notify_worker.notified().await;
                            tokio::time::sleep(Duration::from_millis(30)).await;
                            cleanup_flag.store(true, Ordering::SeqCst);
                            Ok::<i64, String>(99)
                        },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        move || {
                            cb_count.fetch_add(1, Ordering::SeqCst);
                            notify_cancel.notify_waiters();
                        },
                        "_core_test_custom_cancel",
                    )
                    .map(Bound::unbind)
                })
                .expect("spawn");

            let globals = PyDict::new(py);
            globals.set_item("spawn", spawn).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "async def main():\n",
                "    t = asyncio.ensure_future(spawn())\n",
                "    await asyncio.sleep(0.02)\n",
                "    t.cancel()\n",
                "    t.cancel()\n",
                "    cancelled = False\n",
                "    try:\n",
                "        await t\n",
                "    except asyncio.CancelledError:\n",
                "        cancelled = True\n",
                "    return cancelled\n",
                "result = asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();
            let was_cancelled: bool = globals
                .get_item("result")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert!(was_cancelled);
            py.detach(|| {
                let deadline = Instant::now() + Duration::from_secs(2);
                while !cleanup_completed.load(Ordering::SeqCst) && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
        });

        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
        assert!(cleanup_completed.load(Ordering::SeqCst));
    }

    #[test]
    fn panicking_task_converts_to_task_panic_error_and_fallback_runtime_error() {
        Python::initialize();
        Python::attach(|py| {
            let spawn = PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                spawn_rust_task_with_attr(
                    args.py(),
                    async {
                        if std::hint::black_box(true) {
                            panic!("ring panic");
                        }
                        Ok::<i64, String>(1)
                    },
                    |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                    |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                    "_core_test_drainer_panic",
                )
            })
            .expect("spawn");

            let sys_modules = py.import("sys").unwrap().getattr("modules").unwrap();
            let had_pkg = sys_modules.contains("vertebra").unwrap_or(false);
            let had_core = sys_modules.contains("vertebra._vertebra").unwrap_or(false);
            let prev_pkg = sys_modules.get_item("vertebra").ok();
            let prev_core = sys_modules.get_item("vertebra._vertebra").ok();
            sys_modules
                .set_item("vertebra._vertebra", py.None())
                .unwrap();

            let globals = PyDict::new(py);
            globals.set_item("spawn", spawn).unwrap();
            let fallback_script = CString::new(concat!(
                "import asyncio\n",
                "async def main():\n",
                "    try:\n",
                "        await spawn()\n",
                "    except RuntimeError as exc:\n",
                "        return (type(exc).__name__, str(exc))\n",
                "result = asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&fallback_script, Some(&globals), None).unwrap();
            let (ty_name, msg): (String, String) = globals
                .get_item("result")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(ty_name, "RuntimeError");
            assert_eq!(msg, "Rust task panicked: ring panic");

            let pkg = PyModule::new(py, "vertebra").unwrap();
            let core = PyModule::new(py, "vertebra._vertebra").unwrap();
            core.add("TaskPanicError", py.get_type::<TestTaskPanicError>())
                .unwrap();
            pkg.add("_vertebra", &core).unwrap();
            sys_modules.set_item("vertebra", &pkg).unwrap();
            sys_modules.set_item("vertebra._vertebra", &core).unwrap();

            py.run(&fallback_script, Some(&globals), None).unwrap();
            let (panic_ty_name, panic_msg): (String, String) = globals
                .get_item("result")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();

            if had_pkg {
                if let Some(p) = prev_pkg {
                    let _ = sys_modules.set_item("vertebra", p);
                }
            } else {
                let _ = sys_modules.del_item("vertebra");
            }
            if had_core {
                if let Some(c) = prev_core {
                    let _ = sys_modules.set_item("vertebra._vertebra", c);
                }
            } else {
                let _ = sys_modules.del_item("vertebra._vertebra");
            }

            assert_eq!(panic_ty_name, "TestTaskPanicError");
            assert_eq!(panic_msg, "Rust task panicked: ring panic");
        });
    }

    #[test]
    fn block_on_runtime_succeeds_inside_current_and_multi_thread_tokio_runtimes() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = rt.block_on(async {
            block_on_runtime(async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                42_u64
            })
        });
        assert_eq!(out, 42);

        let mt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let mt_out = mt.block_on(async {
            block_on_runtime(async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                99_u64
            })
        });
        assert_eq!(mt_out, 99);

        let _guard = mt.enter();
        assert_eq!(block_on_runtime(async { 7_u64 }), 7);
    }

    #[test]
    fn fallback_wakeup_uses_weakref_and_clears_when_loop_collected() {
        Python::initialize();
        Python::attach(|py| {
            let globals = PyDict::new(py);
            let script = CString::new(concat!(
                "import weakref\n",
                "class StubLoop:\n",
                "    def __init__(self):\n",
                "        self.scheduled = 0\n",
                "    def add_reader(self, fd, cb):\n",
                "        raise NotImplementedError('no selector')\n",
                "    def call_soon_threadsafe(self, cb):\n",
                "        self.scheduled += 1\n",
                "stub_loop = StubLoop()\n",
                "stub_weak = weakref.ref(stub_loop)\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();

            let ring = {
                let loop_obj = globals.get_item("stub_loop").unwrap().unwrap();
                let ring =
                    ensure_completion_ring(py, &loop_obj, "_core_test_fallback_drainer").unwrap();
                ring.signal_fallback();
                let scheduled: usize = loop_obj.getattr("scheduled").unwrap().extract().unwrap();
                assert_eq!(scheduled, 1);
                ring
            };

            globals.del_item("stub_loop").unwrap();
            let weak_collected: bool = globals
                .get_item("stub_weak")
                .unwrap()
                .unwrap()
                .call0()
                .unwrap()
                .is_none();
            assert!(weak_collected);

            ring.signal_fallback();
            assert!(ring.fallback_wakeup.lock().unwrap().is_none());
        });
    }

    #[test]
    fn registration_failure_drops_worker_without_starting() {
        Python::initialize();
        let dropped = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let callback_called = Arc::new(AtomicBool::new(false));
        let dropped_clone = Arc::clone(&dropped);
        let started_clone = Arc::clone(&started);
        let cb_clone = Arc::clone(&callback_called);

        Python::attach(|py| {
            let spawn =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let guard = DropFlag(Arc::clone(&dropped_clone));
                    let started_flag = Arc::clone(&started_clone);
                    let cb_flag = Arc::clone(&cb_clone);
                    spawn_rust_task_with_attr_and_cancel(
                        args.py(),
                        async move {
                            let _guard = guard;
                            started_flag.store(true, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            Ok::<i64, String>(1)
                        },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        move || {
                            cb_flag.store(true, Ordering::SeqCst);
                        },
                        "_core_test_reg_fail",
                    )
                    .map(Bound::unbind)
                })
                .expect("spawn");

            let globals = PyDict::new(py);
            globals.set_item("spawn", spawn).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "class BrokenFuture:\n",
                "    def add_done_callback(self, cb):\n",
                "        raise RuntimeError('custom callback registration failed')\n",
                "async def main():\n",
                "    loop = asyncio.get_running_loop()\n",
                "    orig = loop.create_future\n",
                "    loop.create_future = lambda: BrokenFuture()\n",
                "    try:\n",
                "        spawn()\n",
                "    except RuntimeError as exc:\n",
                "        return str(exc)\n",
                "    finally:\n",
                "        loop.create_future = orig\n",
                "result = asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();
            let msg: String = globals
                .get_item("result")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(msg, "custom callback registration failed");
            assert!(dropped.load(Ordering::SeqCst));
            assert!(!started.load(Ordering::SeqCst));
        });

        assert!(dropped.load(Ordering::SeqCst));
        assert!(!started.load(Ordering::SeqCst));
        assert!(!callback_called.load(Ordering::SeqCst));
    }

    #[test]
    fn spawn_with_future_passes_original_future_and_observes_cancellation() {
        Python::initialize();
        let observed_future_match = Arc::new(AtomicBool::new(false));
        let observed_cancelled_in_mapper = Arc::new(AtomicBool::new(false));
        let dropped_after_cancel = Arc::new(AtomicBool::new(false));

        let flag_match = Arc::clone(&observed_future_match);
        let flag_cancelled = Arc::clone(&observed_cancelled_in_mapper);
        let flag_drop = Arc::clone(&dropped_after_cancel);

        Python::attach(|py| {
            let captured_future: Arc<Mutex<Option<Py<PyAny>>>> = Arc::new(Mutex::new(None));
            let captured_for_spawn = Arc::clone(&captured_future);
            let spawn_ok =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let flag_m = Arc::clone(&flag_match);
                    let cap = Arc::clone(&captured_for_spawn);
                    let aw = spawn_rust_task_with_attr_and_future(
                        args.py(),
                        async { Ok::<i64, String>(42) },
                        move |py, fut, val| {
                            let is_same = cap
                                .lock()
                                .unwrap()
                                .as_ref()
                                .is_some_and(|saved| saved.bind(py).is(&fut));
                            flag_m.store(is_same, Ordering::SeqCst);
                            Ok(val.into_pyobject(py)?.into_any().unbind())
                        },
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_core_test_with_future",
                    )?;
                    *captured_for_spawn.lock().unwrap() = Some(aw.future_handle(args.py()));
                    Bound::new(args.py(), aw).map(Bound::unbind)
                })
                .expect("spawn_ok");

            let spawn_cancel =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let guard = DropFlag(Arc::clone(&flag_drop));
                    let flag_c = Arc::clone(&flag_cancelled);
                    let aw = spawn_rust_task_with_attr_and_future(
                        args.py(),
                        async move { Ok::<DropFlag, String>(guard) },
                        move |py, fut, guard| {
                            let cancelled = fut.call_method0("cancelled")?.extract::<bool>()?;
                            flag_c.store(cancelled, Ordering::SeqCst);
                            drop(guard);
                            Ok(42_i64.into_pyobject(py)?.into_any().unbind())
                        },
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_core_test_with_future",
                    )?;
                    Bound::new(args.py(), aw).map(Bound::unbind)
                })
                .expect("spawn_cancel");

            let globals = PyDict::new(py);
            globals.set_item("spawn_ok", spawn_ok).unwrap();
            globals.set_item("spawn_cancel", spawn_cancel).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "async def main():\n",
                "    val = await spawn_ok()\n",
                "    assert val == 42\n",
                "    aw = spawn_cancel()\n",
                "    await asyncio.sleep(0.05)\n",
                "    del aw\n",
                "    await asyncio.sleep(0.05)\n",
                "asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();
        });

        assert!(observed_future_match.load(Ordering::SeqCst));
        assert!(dropped_after_cancel.load(Ordering::SeqCst));
        assert!(!observed_cancelled_in_mapper.load(Ordering::SeqCst));
    }
}
