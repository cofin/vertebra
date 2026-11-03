//! Shared Tokio completion ring and awaitables for `vertebra._vertebra`.
//!
//! Bridges pure-Rust background Tokio tasks to Python `asyncio.Future` instances
//! via [`vertebra_core::notify::CompletionQueue`] and `loop.add_reader()` without
//! calling `Python::attach` on Tokio worker threads when platform `Notifier` is active.

use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};

use pyo3::prelude::*;
use tokio::runtime::{Builder, Handle, Runtime};
use vertebra_core::notify::{CompletionQueue, Notifier};

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
                match (item.produce)(py) {
                    Ok(val) => {
                        let _ = fut.call_method1("set_result", (val,));
                    }
                    Err(err) => {
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
    let fut_for_await = py_fut.unbind();

    db_runtime_handle().spawn(async move {
        let output = fut.await;
        let should_signal = ring.completion.push(CompletedDbTask {
            future: fut_for_cb,
            produce: Box::new(move |py| match output {
                Ok(val) => map_ok(py, val),
                Err(err) => Err(map_err(err)),
            }),
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
    Ok(())
}
