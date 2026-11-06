//! Shared PyO3 Tokio completion bridge and cross-extension [`ShmCapsule`] `ArrowCursor` handoff.
//!
//! Each native extension crate (`vertebra._vertebra`, `vertebra._bigquery`, `vertebra._spanner`,
//! and future database wheels) compiles its own copy of this module with an extension-specific
//! `attr_name` (such as `"_vertebra_core_completion_drainer"` or
//! `"_vertebra_bigquery_completion_drainer"`) so multiple `.so` extensions attach independent
//! `AsyncCompletionQueue` drainers to the same Python `asyncio` event loop without `PyTypeObject`
//! cast collisions.

use std::any::Any;
use std::ffi::{CStr, c_void};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyType, PyWeakref, PyWeakrefMethods, PyWeakrefReference};
use tokio::runtime::{Builder, Handle, Runtime, RuntimeFlavor};
use tokio::task::AbortHandle;
use vertebra_core::notify::{AsyncCompletionQueue, Notifier};
use vertebra_core::shm::{ShmCapsule, ShmRegion};

/// Canonical `PyCapsule` name used for cross-extension [`ShmCapsule`] transfers.
pub const SHM_CAPSULE_NAME: &CStr = c"vertebra.ShmCapsule";

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

/// Boxed Python result converter invoked on the Python event-loop thread when a task finishes.
pub type DbOutcomeProducer = Box<dyn FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send>;

/// Completed background task queued into an [`AsyncCompletionQueue`].
pub struct CompletedDbTask {
    pub future: Py<PyAny>,
    pub produce: DbOutcomeProducer,
}

static DB_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Return a handle to this extension's multi-threaded Tokio database runtime.
pub fn db_runtime_handle() -> &'static Handle {
    DB_RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("vtb-db-rt")
                .build()
                .expect("failed to build vertebra-db tokio runtime")
        })
        .handle()
}

/// Drive a Rust future synchronously on the Tokio database runtime.
pub fn block_on_db<F>(fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    if let Ok(handle) = Handle::try_current() {
        if handle.runtime_flavor() == RuntimeFlavor::MultiThread {
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else {
            std::thread::scope(|s| {
                s.spawn(|| db_runtime_handle().block_on(fut))
                    .join()
                    .expect("db runtime thread panicked")
            })
        }
    } else {
        db_runtime_handle().block_on(fut)
    }
}

struct DbCompletionRing {
    completion: AsyncCompletionQueue<CompletedDbTask>,
    fallback_wakeup: Mutex<Option<(Py<PyWeakref>, &'static str)>>,
}

impl DbCompletionRing {
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
                        let err = make_task_panic_err(py, panic_message(payload));
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
    attr_name: &'static str,
) -> PyResult<Arc<DbCompletionRing>> {
    if let Ok(existing) = loop_obj.getattr(attr_name)
        && let Ok(drainer) = existing.cast::<DbCompletionDrainer>()
    {
        return Ok(Arc::clone(&drainer.get().ring));
    }

    #[cfg(unix)]
    if let Ok(notifier) = Notifier::new() {
        let fd = notifier.as_raw_fd();
        let temp_ring = Arc::new(DbCompletionRing {
            completion: AsyncCompletionQueue::new(Some(notifier)),
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
            let _ = loop_obj.setattr(attr_name, drainer_py.bind(py));
            return Ok(temp_ring);
        }
    }

    let weak_loop = PyWeakrefReference::new(loop_obj)?
        .into_any()
        .cast_into::<PyWeakref>()?
        .unbind();
    let ring = Arc::new(DbCompletionRing {
        completion: AsyncCompletionQueue::new(None),
        fallback_wakeup: Mutex::new(Some((weak_loop, attr_name))),
    });
    let drainer_py = Py::new(
        py,
        DbCompletionDrainer {
            ring: Arc::clone(&ring),
        },
    )?;
    let _ = loop_obj.setattr(attr_name, drainer_py.bind(py));
    Ok(ring)
}

/// Native Rust awaitable backed by a Tokio runtime and `AsyncCompletionQueue`.
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

/// Spawn a background Tokio task and resolve its Python `asyncio.Future` via an `attr_name`-keyed
/// `AsyncCompletionQueue` drainer attached to the running event loop.
pub fn spawn_rust_db_task_with_attr<F, T, E, M, EMap>(
    py: Python<'_>,
    fut: F,
    map_ok: M,
    map_err: EMap,
    attr_name: &'static str,
) -> PyResult<RustDbTaskAwaitable>
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
    M: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    EMap: FnOnce(Python<'_>, E) -> PyErr + Send + 'static,
{
    let asyncio = py.import("asyncio")?;
    let loop_obj = asyncio.call_method0("get_running_loop")?;
    let py_fut = loop_obj.call_method0("create_future")?;
    let ring = ensure_db_completion_ring(py, &loop_obj, attr_name)?;
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
            Ok(Err(err)) => Box::new(move |py| Err(map_err(py, err))),
            Err(join) if join.is_panic() => {
                let message = panic_message(join.into_panic());
                Box::new(move |py| Err(make_task_panic_err(py, message)))
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

unsafe extern "C" fn drop_shm_region_arc(ctx: *mut c_void) {
    if !ctx.is_null() {
        // SAFETY: `ctx` was produced via `Arc::into_raw` on an `Arc<ShmRegion>` in
        // `wrap_shm_region_capsule` and ownership is claimed at most once via atomic swap.
        unsafe {
            drop(Arc::from_raw(ctx.cast::<ShmRegion>()));
        }
    }
}

unsafe extern "C" fn unlink_shm_region_arc(ctx: *mut c_void) -> bool {
    if ctx.is_null() {
        return false;
    }
    #[cfg(unix)]
    {
        // SAFETY: `ctx` points to a live `ShmRegion` inside an `Arc<ShmRegion>` whose strong
        // count remains non-zero while the caller holds ownership of `ctx`.
        let region = unsafe { &*(ctx as *const ShmRegion) };
        region.unlink_now()
    }
    #[cfg(not(unix))]
    {
        false
    }
}

unsafe extern "C" fn shm_pycapsule_destructor(capsule_obj: *mut ffi::PyObject) {
    // SAFETY: `capsule_obj` is a valid `PyCapsule` being finalized by CPython.
    let raw = unsafe { ffi::PyCapsule_GetPointer(capsule_obj, SHM_CAPSULE_NAME.as_ptr()) }
        .cast::<ShmCapsule>();
    if raw.is_null() {
        // SAFETY: Clears any CPython error set by `PyCapsule_GetPointer` during finalization.
        unsafe {
            ffi::PyErr_Clear();
        }
        return;
    }
    // SAFETY: `raw` was allocated via `Box::into_raw` in `wrap_shm_region_capsule` and is
    // dereferenced immutably to atomically claim `ctx` before reclaiming the `Box`.
    let (ctx, drop_fn) = unsafe {
        let cap_ref = &*(raw as *const ShmCapsule);
        let ctx = cap_ref.ctx.swap(core::ptr::null_mut(), Ordering::AcqRel);
        (ctx, cap_ref.drop_fn)
    };
    if !ctx.is_null() {
        // SAFETY: `ctx` was atomically claimed once via `AcqRel` swap and was not consumed by
        // `ArrowCursor::_from_shm_capsule`.
        unsafe {
            (drop_fn)(ctx);
        }
    }
    // SAFETY: CPython invokes the `PyCapsule` destructor once at zero refcount; reclaim the
    // `Box<ShmCapsule>` allocation in this crate's allocator.
    unsafe {
        drop(Box::from_raw(raw));
    }
}

/// Wrap an [`Arc<ShmRegion>`] in a CPython `PyCapsule` (`c"vertebra.ShmCapsule"`) holding a
/// `#[repr(C)]` [`ShmCapsule`].
pub fn wrap_shm_region_capsule(
    py: Python<'_>,
    region: Arc<ShmRegion>,
) -> PyResult<Bound<'_, PyAny>> {
    let ptr = region.as_ptr();
    let len = region.len();
    let boxed = Box::new(ShmCapsule {
        ptr,
        len,
        ctx: AtomicPtr::new(Arc::into_raw(region) as *mut c_void),
        drop_fn: drop_shm_region_arc,
        unlink_fn: Some(unlink_shm_region_arc),
    });
    let raw = Box::into_raw(boxed).cast::<c_void>();
    // SAFETY: `raw` is non-null, `SHM_CAPSULE_NAME` is a static NUL-terminated C string, and
    // `shm_pycapsule_destructor` reclaims `raw` (and `ctx` if unconsumed) in this crate's allocator.
    let cap_ptr = unsafe {
        ffi::PyCapsule_New(
            raw,
            SHM_CAPSULE_NAME.as_ptr(),
            Some(shm_pycapsule_destructor),
        )
    };
    if cap_ptr.is_null() {
        // SAFETY: `PyCapsule_New` failed before taking ownership of `raw`, so reclaim the `Box` and `Arc`.
        let cap = unsafe { Box::from_raw(raw.cast::<ShmCapsule>()) };
        let ctx = cap.ctx.swap(core::ptr::null_mut(), Ordering::AcqRel);
        if !ctx.is_null() {
            // SAFETY: `ctx` came from `Arc::into_raw(region)` above and was not transferred.
            unsafe {
                (cap.drop_fn)(ctx);
            }
        }
        return Err(PyErr::fetch(py));
    }
    // SAFETY: `cap_ptr` is a fresh owned reference returned by `PyCapsule_New`.
    Ok(unsafe { Bound::from_owned_ptr(py, cap_ptr) })
}

/// Construct a `vertebra._vertebra.ArrowCursor` from an unregistered [`Arc<ShmRegion>`] via
/// `PyCapsule` (`c"vertebra.ShmCapsule"`) without cross-linking `cdylib` crates.
pub fn construct_py_arrow_cursor(
    py: Python<'_>,
    shm_name: &str,
    byte_len: usize,
    row_count: usize,
    region: Arc<ShmRegion>,
) -> PyResult<Py<PyAny>> {
    let capsule = wrap_shm_region_capsule(py, region)?;
    let core_mod = py.import("vertebra._vertebra")?;
    let arrow_cls = core_mod.getattr("ArrowCursor")?;
    let cursor = arrow_cls.call_method1(
        "_from_shm_capsule",
        (shm_name, byte_len, row_count, capsule),
    )?;
    Ok(cursor.unbind())
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use pyo3::prelude::*;
    use pyo3::types::{PyCFunction, PyDict, PyTuple};
    use vertebra_core::shm::ShmRegion;

    use super::{
        block_on_db, ensure_db_completion_ring, spawn_rust_db_task_with_attr,
        wrap_shm_region_capsule,
    };

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn distinct_attr_names_coexist_on_single_event_loop() {
        Python::initialize();
        Python::attach(|py| {
            let spawn_a =
                PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                    spawn_rust_db_task_with_attr(
                        args.py(),
                        async { Ok::<i64, String>(10) },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_test_drainer_a",
                    )
                })
                .expect("spawn_a");
            let spawn_b =
                PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                    spawn_rust_db_task_with_attr(
                        args.py(),
                        async { Ok::<i64, String>(32) },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_test_drainer_b",
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
                "    same = getattr(loop, '_test_drainer_a') is getattr(loop, '_test_drainer_b')\n",
                "    return (a + b, hasattr(loop, '_test_drainer_a'), hasattr(loop, '_test_drainer_b'), same)\n",
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
    fn panicking_task_raises_task_panic_or_runtime_error() {
        Python::initialize();
        Python::attach(|py| {
            let spawn = PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| {
                spawn_rust_db_task_with_attr(
                    args.py(),
                    async {
                        if std::hint::black_box(true) {
                            panic!("ring panic");
                        }
                        Ok::<i64, String>(1)
                    },
                    |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                    |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                    "_test_drainer_panic",
                )
            })
            .expect("spawn");
            let globals = PyDict::new(py);
            globals.set_item("spawn", spawn).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "async def main():\n",
                "    try:\n",
                "        await spawn()\n",
                "    except RuntimeError as exc:\n",
                "        return (isinstance(exc, RuntimeError), str(exc))\n",
                "result = asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();
            let (is_rt, msg): (bool, String) = globals
                .get_item("result")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert!(is_rt);
            assert_eq!(msg, "Rust task panicked: ring panic");
        });
    }

    #[test]
    fn cancelled_future_aborts_spawned_tokio_task() {
        Python::initialize();
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_clone = Arc::clone(&dropped);

        Python::attach(|py| {
            let spawn =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let flag = Arc::clone(&dropped_clone);
                    spawn_rust_db_task_with_attr(
                        args.py(),
                        async move {
                            let _guard = DropFlag(flag);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            Ok::<i64, String>(1)
                        },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_test_drainer_cancel",
                    )
                })
                .expect("spawn");
            let globals = PyDict::new(py);
            globals.set_item("spawn", spawn).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "async def main():\n",
                "    t = asyncio.ensure_future(spawn())\n",
                "    await asyncio.sleep(0.05)\n",
                "    t.cancel()\n",
                "    try:\n",
                "        await t\n",
                "    except asyncio.CancelledError:\n",
                "        pass\n",
                "asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();
            py.detach(|| {
                let deadline = Instant::now() + Duration::from_secs(1);
                while !dropped.load(Ordering::SeqCst) && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
        });

        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn unconsumed_shm_capsule_releases_arc_on_drop() {
        Python::initialize();
        let region = Arc::new(ShmRegion::create(4096).unwrap());
        let weak = Arc::downgrade(&region);
        Python::attach(|py| {
            let cap = wrap_shm_region_capsule(py, region).unwrap();
            assert!(weak.upgrade().is_some());
            drop(cap);
        });
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn block_on_db_succeeds_inside_current_thread_tokio_runtime() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = rt.block_on(async {
            block_on_db(async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                42_u64
            })
        });
        assert_eq!(out, 42);
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
                    ensure_db_completion_ring(py, &loop_obj, "_test_fallback_drainer").unwrap();
                ring.signal_fallback();
                let scheduled: usize = loop_obj.getattr("scheduled").unwrap().extract().unwrap();
                assert_eq!(scheduled, 1);
                ring
            };

            globals.del_item("stub_loop").unwrap();
            let weak_alive: bool = globals
                .get_item("stub_weak")
                .unwrap()
                .unwrap()
                .call0()
                .unwrap()
                .is_none();
            assert!(weak_alive);

            ring.signal_fallback();
            assert!(ring.fallback_wakeup.lock().unwrap().is_none());
        });
    }
}
