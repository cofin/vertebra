//! Shared Tokio completion ring and awaitables for `vertebra._vertebra`.
//!
//! Delegates task execution and per-loop `AsyncCompletionQueue` draining to
//! [`vertebra_db::py_async`] using the `"_vertebra_core_completion_drainer"` loop attribute so
//! `vertebra._vertebra` coexists cleanly alongside standalone driver wheels (`vertebra._bigquery`,
//! `vertebra._spanner`) on the same Python `asyncio` event loop.

use std::future::Future;

use pyo3::prelude::*;

pub use vertebra_db::py_async::{
    CompletedDbTask, DbCompletionDrainer, DbOutcomeProducer, RustDbTaskAwaitable, block_on_db,
    db_runtime_handle, spawn_rust_db_task_with_attr,
};

/// Event-loop attribute name storing the `_vertebra` core completion drainer.
pub const CORE_COMPLETION_DRAINER_ATTR: &str = "_vertebra_core_completion_drainer";

pyo3::create_exception!(
    vertebra._vertebra,
    TaskPanicError,
    pyo3::exceptions::PyRuntimeError,
    "Raised when a Rust background task panics before producing a result."
);

/// Spawn a background Tokio task and resolve its Python `asyncio.Future` via `AsyncCompletionQueue`.
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
    EMap: FnOnce(Python<'_>, E) -> PyErr + Send + 'static,
{
    spawn_rust_db_task_with_attr(py, fut, map_ok, map_err, CORE_COMPLETION_DRAINER_ATTR)
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
    use pyo3::types::{PyCFunction, PyDict, PyModule, PyTuple};

    use super::{TaskPanicError, spawn_rust_db_task};

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn ensure_task_panic_registered(py: Python<'_>) {
        if py.import("vertebra._vertebra").is_ok() {
            return;
        }
        let sys_modules = py
            .import("sys")
            .and_then(|s| s.getattr("modules"))
            .expect("sys.modules");
        let pkg = PyModule::new(py, "vertebra").expect("vertebra pkg");
        let core = PyModule::new(py, "vertebra._vertebra").expect("core mod");
        core.add("TaskPanicError", py.get_type::<TaskPanicError>())
            .expect("add TaskPanicError");
        pkg.add("_vertebra", &core).expect("attach _vertebra");
        sys_modules
            .set_item("vertebra", &pkg)
            .expect("register vertebra");
        sys_modules
            .set_item("vertebra._vertebra", &core)
            .expect("register _vertebra");
    }

    fn run_coroutine<'py>(
        py: Python<'py>,
        spawn: Bound<'py, PyCFunction>,
        body: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        ensure_task_panic_registered(py);
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
                        if std::hint::black_box(true) {
                            panic!("boom");
                        }
                        Ok::<i64, String>(1)
                    },
                    |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                    |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
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
                        if std::hint::black_box(true) {
                            panic!("map boom");
                        }
                        Ok(v.into_pyobject(py)?.into_any().unbind())
                    },
                    |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
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
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
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
