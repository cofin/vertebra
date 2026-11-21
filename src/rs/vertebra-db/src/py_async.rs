//! Shared PyO3 Tokio completion bridge and cross-extension [`ShmCapsule`] `ArrowCursor` handoff.
//!
//! Re-exports the canonical [`vertebra_core::py_async`] completion bridge primitives so each
//! database extension wheel (`vertebra._vertebra`, `vertebra._bigquery`, `vertebra._spanner`, and
//! peer drivers) attaches its own extension-specific `attr_name` drainer to the Python `asyncio`
//! event loop without `PyTypeObject` cast collisions. Extension crates import the completed
//! awaitable from here.

use std::ffi::{CStr, c_void};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use pyo3::ffi;
use pyo3::prelude::*;
pub use vertebra_core::py_async::{
    CompletedTask as CompletedDbTask, CompletionDrainer as DbCompletionDrainer,
    OutcomeProducer as DbOutcomeProducer, ReadyAwaitable, RustTaskAwaitable as RustDbTaskAwaitable,
    block_on_runtime as block_on_db, ready_err, ready_ok, runtime_handle as db_runtime_handle,
    spawn_rust_task_with_attr as spawn_rust_db_task_with_attr,
    spawn_rust_task_with_attr_and_cancel as spawn_rust_db_task_with_attr_and_cancel,
    spawn_rust_task_with_attr_and_future as spawn_rust_db_task_with_attr_and_future,
};
use vertebra_core::shm::{ShmCapsule, ShmRegion};

/// Canonical `PyCapsule` name used for cross-extension [`ShmCapsule`] transfers.
pub const SHM_CAPSULE_NAME: &CStr = c"vertebra.ShmCapsule";

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
    let boxed = Box::new(ShmCapsule::from_arc(region));
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use pyo3::prelude::*;
    use pyo3::types::{PyCFunction, PyDict, PyTuple};
    use vertebra_core::shm::ShmRegion;

    use super::{
        block_on_db, spawn_rust_db_task_with_attr, spawn_rust_db_task_with_attr_and_cancel,
        spawn_rust_db_task_with_attr_and_future, wrap_shm_region_capsule,
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
                    let guard = DropFlag(Arc::clone(&dropped_clone));
                    spawn_rust_db_task_with_attr(
                        args.py(),
                        async move {
                            let _guard = guard;
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
    fn block_on_db_succeeds_inside_current_and_multi_thread_tokio_runtimes() {
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

        let mt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let mt_out = mt.block_on(async {
            block_on_db(async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                99_u64
            })
        });
        assert_eq!(mt_out, 99);

        let _guard = mt.enter();
        assert_eq!(block_on_db(async { 7_u64 }), 7);
    }

    #[test]
    fn callback_registration_failure_aborts_worker() {
        Python::initialize();
        let dropped = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let dropped_clone = Arc::clone(&dropped);
        let started_clone = Arc::clone(&started);

        Python::attach(|py| {
            let spawn =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let guard = DropFlag(Arc::clone(&dropped_clone));
                    let started_flag = Arc::clone(&started_clone);
                    spawn_rust_db_task_with_attr(
                        args.py(),
                        async move {
                            let _guard = guard;
                            started_flag.store(true, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            Ok::<i64, String>(1)
                        },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_test_drainer_cb_fail",
                    )
                })
                .expect("spawn");
            let globals = PyDict::new(py);
            globals.set_item("spawn", spawn).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "class BrokenFuture:\n",
                "    def add_done_callback(self, cb):\n",
                "        raise RuntimeError('callback registration failed')\n",
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
            assert_eq!(msg, "callback registration failed");
            assert!(dropped.load(Ordering::SeqCst));
            assert!(!started.load(Ordering::SeqCst));
        });

        assert!(dropped.load(Ordering::SeqCst));
        assert!(!started.load(Ordering::SeqCst));
    }

    #[test]
    fn cancel_callback_runs_without_aborting_cleanup() {
        Python::initialize();
        let callback_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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
                    spawn_rust_db_task_with_attr_and_cancel(
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
                        "_test_drainer_custom_cancel",
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
    fn legacy_abort_cancels_worker() {
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
                    spawn_rust_db_task_with_attr(
                        args.py(),
                        async move {
                            let _guard = guard;
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            done.store(true, Ordering::SeqCst);
                            Ok::<i64, String>(1)
                        },
                        |py, v| Ok(v.into_pyobject(py)?.into_any().unbind()),
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_test_drainer_legacy_abort",
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
        assert!(!completed.load(Ordering::SeqCst));
    }

    #[test]
    fn registration_failure_drops_worker() {
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
                    spawn_rust_db_task_with_attr_and_cancel(
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
                        "_test_drainer_custom_reg_fail",
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
                    let aw = spawn_rust_db_task_with_attr_and_future(
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
                        "_test_drainer_with_future",
                    )?;
                    *captured_for_spawn.lock().unwrap() = Some(aw.future_handle(args.py()));
                    Bound::new(args.py(), aw).map(Bound::unbind)
                })
                .expect("spawn_ok");

            let spawn_cancel =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let guard = DropFlag(Arc::clone(&flag_drop));
                    let flag_c = Arc::clone(&flag_cancelled);
                    let aw = spawn_rust_db_task_with_attr_and_future(
                        args.py(),
                        async move { Ok::<DropFlag, String>(guard) },
                        move |_py, fut, guard| {
                            let cancelled = fut.call_method0("cancelled")?.extract::<bool>()?;
                            flag_c.store(cancelled, Ordering::SeqCst);
                            drop(guard);
                            Ok(42_i64.into_pyobject(_py)?.into_any().unbind())
                        },
                        |_, e| pyo3::exceptions::PyRuntimeError::new_err(e),
                        "_test_drainer_with_future",
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
