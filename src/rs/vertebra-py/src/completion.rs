//! Shared Tokio completion ring and awaitables for `vertebra._vertebra`.
//!
//! Bridges pure-Rust background Tokio tasks to Python `asyncio.Future` instances
//! via [`vertebra_core::notify::CompletionQueue`] and `loop.add_reader()` without
//! calling `Python::attach` on Tokio worker threads when platform `Notifier` is active.
//!
//! # Panic and Cancellation Semantics
//!
//! - **Panic Isolation (`TaskPanicError`)**: In unwinding builds (`make dev` and test
//!   profiles), a panic inside the spawned Rust future or inside the `map_ok` result
//!   converter is caught and resolves the Python `asyncio.Future` with [`TaskPanicError`].
//!   The workspace `release` profile sets `panic = "abort"` (`src/rs/Cargo.toml`), so
//!   unrecoverable panics in release binaries abort the process.
//! - **Task Cancellation**: Attaches a `_DbTaskCanceller` done-callback to the Python
//!   `asyncio.Future` that calls [`tokio::task::AbortHandle::abort`] when the future is
//!   cancelled, dropping the Rust task at its next `.await` point.

use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, OnceLock};

use pyo3::prelude::*;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::AbortHandle;
use vertebra_core::notify::{CompletionQueue, Notifier};

pyo3::create_exception!(
    vertebra._vertebra,
    TaskPanicError,
    pyo3::exceptions::PyRuntimeError,
    "Raised when a Rust background task panics before producing a result."
);

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        format!("Rust task panicked: {text}")
    } else if let Some(text) = payload.downcast_ref::<String>() {
        format!("Rust task panicked: {text}")
    } else {
        "Rust task panicked".to_string()
    }
}

#[pyclass(name = "_DbTaskCanceller", frozen, immutable_type)]
struct DbTaskCanceller {
    abort: AbortHandle,
}

#[pymethods]
impl DbTaskCanceller {
    fn __call__(&self, future: &Bound<'_, PyAny>) -> PyResult<()> {
        if future.call_method0("cancelled")?.extract::<bool>()? {
            self.abort.abort();
        }
        Ok(())
    }
}

pub type DbOutcomeProducer = Box<dyn FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send>;

pub struct CompletedDbTask {
    pub future: Py<PyAny>,
    pub produce: DbOutcomeProducer,
}

static DB_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Return a handle to the shared multi-threaded Tokio runtime for `_vertebra` tasks.
pub fn db_runtime_handle() -> &'static Handle {
    DB_RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("vtb-db-rt")
                .build()
                .expect("failed to build vertebra-py tokio runtime")
        })
        .handle()
}

/// Drive a Rust future synchronously on the shared `_vertebra` Tokio runtime.
pub fn block_on_db<F: Future>(fut: F) -> F::Output {
    if let Ok(handle) = Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(fut))
    } else {
        db_runtime_handle().block_on(fut)
    }
}

struct DbCompletionRing {
    completion: CompletionQueue<CompletedDbTask>,
    fallback_wakeup: Mutex<Option<(Py<PyAny>, Py<PyAny>)>>,
}

impl DbCompletionRing {
    fn signal_fallback(&self) {
        if self.completion.notifier().is_some() {
            return;
        }
        let pair = self
            .fallback_wakeup
            .lock()
            .expect("lock db fallback wakeup")
            .as_ref()
            .map(|(loop_ref, drainer_ref)| {
                Python::attach(|py| (loop_ref.clone_ref(py), drainer_ref.clone_ref(py)))
            });
        if let Some((loop_ref, drainer_ref)) = pair {
            Python::attach(|py| {
                let _ = loop_ref
                    .bind(py)
                    .call_method1("call_soon_threadsafe", (&drainer_ref,));
            });
        }
    }
}

#[pyclass(name = "_DbCompletionDrainer", frozen, immutable_type)]
pub struct DbCompletionDrainer {
    ring: Arc<DbCompletionRing>,
}

#[pymethods]
impl DbCompletionDrainer {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        self.ring.completion.clear_notifier();
        let mut batch = Vec::new();
        while self.ring.completion.drain_into(&mut batch) {
            for item in batch.drain(..) {
                let fut = item.future.bind(py);
                let cancelled: bool = fut
                    .call_method0("cancelled")
                    .and_then(|v| v.extract())
                    .unwrap_or(false);
                if cancelled {
                    continue;
                }
                match catch_unwind(AssertUnwindSafe(|| (item.produce)(py))) {
                    Ok(Ok(val)) => {
                        let _ = fut.call_method1("set_result", (val,));
                    }
                    Ok(Err(err)) => {
                        let _ = fut.call_method1("set_exception", (err.into_value(py),));
                    }
                    Err(payload) => {
                        let err = TaskPanicError::new_err(panic_message(payload));
                        let _ = fut.call_method1("set_exception", (err.into_value(py),));
                    }
                }
            }
        }
        Ok(())
    }
}

fn ensure_db_completion_ring(
    py: Python<'_>,
    loop_obj: &Bound<'_, PyAny>,
) -> PyResult<Arc<DbCompletionRing>> {
    const ATTR_NAME: &str = "_vertebra_db_completion_drainer";
    if let Ok(existing) = loop_obj.getattr(ATTR_NAME)
        && let Ok(drainer) = existing.cast::<DbCompletionDrainer>()
    {
        return Ok(Arc::clone(&drainer.borrow().ring));
    }

    #[cfg(unix)]
    if let Ok(notifier) = Notifier::new() {
        let fd = notifier.as_raw_fd();
        let temp_ring = Arc::new(DbCompletionRing {
            completion: CompletionQueue::new(Some(notifier)),
            fallback_wakeup: Mutex::new(None),
        });
        let drainer_py = Py::new(
            py,
            DbCompletionDrainer {
                ring: Arc::clone(&temp_ring),
            },
        )?;
        if loop_obj
            .call_method1("add_reader", (fd, drainer_py.bind(py)))
            .is_ok()
        {
            let _ = loop_obj.setattr(ATTR_NAME, drainer_py.bind(py));
            return Ok(temp_ring);
        }
    }

    let ring = Arc::new(DbCompletionRing {
        completion: CompletionQueue::new(None),
        fallback_wakeup: Mutex::new(None),
    });
    let drainer_py = Py::new(
        py,
        DbCompletionDrainer {
            ring: Arc::clone(&ring),
        },
    )?;
    *ring
        .fallback_wakeup
        .lock()
        .expect("lock db fallback wakeup") = Some((
        loop_obj.clone().unbind(),
        drainer_py.clone_ref(py).into_any(),
    ));
    let _ = loop_obj.setattr(ATTR_NAME, drainer_py.bind(py));
    Ok(ring)
}

/// Native Rust awaitable backed by the shared `_vertebra` Tokio runtime without `asyncio.to_thread`.
#[pyclass(name = "RustDbTaskAwaitable", frozen, immutable_type)]
pub struct RustDbTaskAwaitable {
    future: Py<PyAny>,
}

#[pymethods]
impl RustDbTaskAwaitable {
    fn __await__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let iter = self.future.bind(py).call_method0("__await__")?;
        Ok(iter.unbind())
    }
}

/// Spawn a background Tokio task and resolve its Python `asyncio.Future` via `CompletionQueue`.
pub fn spawn_rust_db_task<F, T, E, M, EMap>(
    py: Python<'_>,
    fut: F,
    map_ok: M,
    map_err: EMap,
) -> PyResult<RustDbTaskAwaitable>
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
    M: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    EMap: FnOnce(E) -> PyErr + Send + 'static,
{
    let asyncio = py.import("asyncio")?;
    let loop_obj = asyncio.call_method0("get_running_loop")?;
    let py_fut = loop_obj.call_method0("create_future")?;
    let ring = ensure_db_completion_ring(py, &loop_obj)?;
    let fut_for_cb = py_fut.clone().unbind();
    let worker = db_runtime_handle().spawn(fut);
    py_fut.call_method1(
        "add_done_callback",
        (Py::new(
            py,
            DbTaskCanceller {
                abort: worker.abort_handle(),
            },
        )?,),
    )?;
    let fut_for_await = py_fut.unbind();

    db_runtime_handle().spawn(async move {
        let produce: DbOutcomeProducer = match worker.await {
            Ok(Ok(value)) => Box::new(move |py| map_ok(py, value)),
            Ok(Err(err)) => Box::new(move |_| Err(map_err(err))),
            Err(join) if join.is_panic() => {
                let message = panic_message(join.into_panic());
                Box::new(move |_| Err(TaskPanicError::new_err(message)))
            }
            Err(_) => Box::new(|_| {
                Err(pyo3::exceptions::asyncio::CancelledError::new_err(
                    "Rust task was cancelled",
                ))
            }),
        };
        let should_signal = ring.completion.push(CompletedDbTask {
            future: fut_for_cb,
            produce,
        });
        if should_signal {
            ring.signal_fallback();
        }
    });

    Ok(RustDbTaskAwaitable {
        future: fut_for_await,
    })
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustDbTaskAwaitable>()?;
    m.add("TaskPanicError", m.py().get_type::<TaskPanicError>())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use pyo3::prelude::*;
    use pyo3::types::{PyCFunction, PyDict, PyTuple};

    use super::spawn_rust_db_task;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn run_coroutine<'py>(
        py: Python<'py>,
        spawn: Bound<'py, PyCFunction>,
        body: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let globals = PyDict::new(py);
        globals.set_item("spawn", spawn)?;
        let script = CString::new(format!("import asyncio\n{body}")).expect("valid script cstring");
        py.run(&script, Some(&globals), None)?;
        globals
            .get_item("result")?
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("result"))
    }

    #[test]
    fn panicking_task_raises_task_panic_error() {
        Python::attach(|py| {
            let spawn = PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                spawn_rust_db_task(
                    args.py(),
                    async {
                        if true {
                            panic!("boom");
                        }
                        Ok::<i64, String>(1)
                    },
                    |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                    pyo3::exceptions::PyRuntimeError::new_err,
                )
            })
            .expect("closure");
            let result = run_coroutine(
                py,
                spawn,
                concat!(
                    "async def main():\n",
                    "    try:\n",
                    "        await spawn()\n",
                    "    except Exception as exc:\n",
                    "        return (type(exc).__name__, str(exc))\n",
                    "result = asyncio.run(main())\n",
                ),
            )
            .expect("run coroutine");
            let (name, message): (String, String) = result.extract().expect("extract");
            assert_eq!(name, "TaskPanicError");
            assert_eq!(message, "Rust task panicked: boom");
        });
    }

    #[test]
    fn panicking_map_ok_raises_task_panic_error() {
        Python::attach(|py| {
            let spawn = PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                spawn_rust_db_task(
                    args.py(),
                    async { Ok::<i64, String>(1) },
                    |py, v| {
                        if true {
                            panic!("map boom");
                        }
                        Ok(v.into_pyobject(py)?.into_any().unbind())
                    },
                    pyo3::exceptions::PyRuntimeError::new_err,
                )
            })
            .expect("closure");
            let result = run_coroutine(
                py,
                spawn,
                concat!(
                    "async def main():\n",
                    "    try:\n",
                    "        await spawn()\n",
                    "    except Exception as exc:\n",
                    "        return (type(exc).__name__, str(exc))\n",
                    "result = asyncio.run(main())\n",
                ),
            )
            .expect("run coroutine");
            let (name, message): (String, String) = result.extract().expect("extract");
            assert_eq!(name, "TaskPanicError");
            assert_eq!(message, "Rust task panicked: map boom");
        });
    }

    #[test]
    fn cancelled_await_aborts_rust_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicBool::new(false));
        let dropped_clone = Arc::clone(&dropped);
        let completed_clone = Arc::clone(&completed);

        Python::attach(|py| {
            let spawn =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let dropped_inner = Arc::clone(&dropped_clone);
                    let completed_inner = Arc::clone(&completed_clone);
                    spawn_rust_db_task(
                        args.py(),
                        async move {
                            let _guard = DropFlag(dropped_inner);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            completed_inner.store(true, Ordering::SeqCst);
                            Ok::<i64, String>(1)
                        },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        pyo3::exceptions::PyRuntimeError::new_err,
                    )
                })
                .expect("closure");
            let _ = run_coroutine(
                py,
                spawn,
                concat!(
                    "async def main():\n",
                    "    task = asyncio.ensure_future(spawn())\n",
                    "    await asyncio.sleep(0.05)\n",
                    "    task.cancel()\n",
                    "    try:\n",
                    "        await task\n",
                    "    except asyncio.CancelledError:\n",
                    "        pass\n",
                    "    return True\n",
                    "result = asyncio.run(main())\n",
                ),
            )
            .expect("run coroutine");
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
}
