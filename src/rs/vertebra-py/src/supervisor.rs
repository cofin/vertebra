use pyo3::prelude::*;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use signal_hook::consts::signal::*;
#[cfg(unix)]
use signal_hook::iterator::Signals;

use vertebra_core::notify::InheritableNotifier;

/// FD information for cross-process IPC.
#[derive(Default)]
struct InheritedFds {
    /// Notify FD for event-driven wakeups (eventfd on Linux, pipe read end on others).
    notify_fd: Option<i32>,
    /// Write FD for notifications (same as notify_fd on Linux, pipe write end on others).
    #[cfg(not(target_os = "linux"))]
    notify_write_fd: Option<i32>,
}

#[pyclass(name = "NativeSupervisor")]
pub struct NativeSupervisor {
    bun_process: Arc<Mutex<Option<Child>>>,
    workers: Arc<Mutex<Vec<Child>>>,
    /// Inheritable notifier for event-driven IPC wakeups.
    /// Created BEFORE spawning children so FD is inherited.
    notifier: Arc<Mutex<Option<InheritableNotifier>>>,
    /// FD numbers for passing to children.
    fds: Arc<Mutex<InheritedFds>>,
}

#[pymethods]
impl NativeSupervisor {
    #[new]
    fn new() -> Self {
        Self {
            bun_process: Arc::new(Mutex::new(None)),
            workers: Arc::new(Mutex::new(Vec::new())),
            notifier: Arc::new(Mutex::new(None)),
            fds: Arc::new(Mutex::new(InheritedFds::default())),
        }
    }

    /// Get the notify FD for Python to use with asyncio.add_reader().
    fn notify_fd(&self) -> Option<i32> {
        self.fds.lock().unwrap().notify_fd
    }

    fn start(
        &mut self,
        py: Python,
        engine_path: &str,
        engine_args: Vec<String>,
        python_executable: &str,
        _config: Py<PyAny>, // Placeholder for now, passed via env/args later
        workers: usize,
    ) -> PyResult<()> {
        let bun_process = self.bun_process.clone();
        let workers_vec = self.workers.clone();
        let notifier_arc = self.notifier.clone();
        let fds_arc = self.fds.clone();
        let engine_path = engine_path.to_string();
        let engine_args = engine_args.clone();
        let python_executable = python_executable.to_string();

        // Create InheritableNotifier BEFORE spawning children.
        // This allows the FD to be inherited by forked processes.
        let notifier = InheritableNotifier::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create notifier: {}",
                e
            ))
        })?;
        let notify_fd = notifier.as_raw_fd();
        #[cfg(not(target_os = "linux"))]
        let notify_write_fd = notifier.write_fd();

        // Store notifier and FD numbers for later access.
        {
            let mut guard = notifier_arc.lock().unwrap();
            *guard = Some(notifier);
        }
        {
            let mut fds = fds_arc.lock().unwrap();
            fds.notify_fd = Some(notify_fd);
            #[cfg(not(target_os = "linux"))]
            {
                fds.notify_write_fd = Some(notify_write_fd);
            }
        }

        py.detach(move || {
            // Start Bun engine with inherited FDs via environment variables.
            let mut cmd = Command::new(&engine_path);
            cmd.args(&engine_args)
                .env("VERTEBRA_NOTIFY_FD", notify_fd.to_string());
            #[cfg(not(target_os = "linux"))]
            {
                cmd.env("VERTEBRA_NOTIFY_WRITE_FD", notify_write_fd.to_string());
            }

            let bun = cmd.spawn().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to start engine: {}",
                    e
                ))
            })?;

            {
                let mut guard = bun_process.lock().unwrap();
                *guard = Some(bun);
            }

            // Start Python workers with inherited FDs.
            let mut new_workers = Vec::new();
            for _ in 0..workers {
                let mut cmd = Command::new(&python_executable);
                cmd.arg("-m")
                    .arg("vertebra.server.worker")
                    .env("VERTEBRA_NOTIFY_FD", notify_fd.to_string());
                #[cfg(not(target_os = "linux"))]
                {
                    cmd.env("VERTEBRA_NOTIFY_WRITE_FD", notify_write_fd.to_string());
                }

                let worker = cmd.spawn().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to start worker: {}",
                        e
                    ))
                })?;
                new_workers.push(worker);
            }

            {
                let mut guard = workers_vec.lock().unwrap();
                *guard = new_workers;
            }

            Ok(())
        })
    }

    /// Send a notification to wake up waiting processes.
    fn notify(&self) -> PyResult<()> {
        let guard = self.notifier.lock().unwrap();
        if let Some(ref notifier) = *guard {
            notifier.notify().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to notify: {}",
                    e
                ))
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Notifier not initialized",
            ))
        }
    }

    /// Clear pending notifications.
    fn clear_notifications(&self) -> PyResult<()> {
        let guard = self.notifier.lock().unwrap();
        if let Some(ref notifier) = *guard {
            notifier.clear().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to clear notifications: {}",
                    e
                ))
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Notifier not initialized",
            ))
        }
    }

    fn wait_for_shutdown(&mut self, py: Python) -> PyResult<i32> {
        #[cfg(unix)]
        {
            py.detach(|| {
                let mut signals = Signals::new(&[SIGTERM, SIGINT]).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to register signals: {}",
                        e
                    ))
                })?;

                // Block until a signal is received
                if let Some(signal) = signals.forever().next() {
                    // Signal received, initiate shutdown
                    self.stop_internal()?;
                    Ok(signal + 128) // Standard exit code convention for signals
                } else {
                    Ok(0)
                }
            })
        }

        #[cfg(not(unix))]
        {
            // Windows implementation placeholder
            // In reality, we'd wait for Ctrl-C or similar via a crate like `ctrlc`
            // For now, just wait on the bun process
            py.detach(|| {
                let mut guard = self.bun_process.lock().unwrap();
                if let Some(ref mut child) = *guard {
                    let status = child.wait().map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to wait for engine: {}",
                            e
                        ))
                    })?;
                    Ok(status.code().unwrap_or(0))
                } else {
                    Ok(0)
                }
            })
        }
    }

    fn stop(&mut self) -> PyResult<()> {
        self.stop_internal()
    }
}

impl NativeSupervisor {
    fn stop_internal(&self) -> PyResult<()> {
        let mut bun_guard = self.bun_process.lock().unwrap();
        if let Some(ref mut bun) = *bun_guard {
            let _ = bun.kill(); // Ignore errors if already dead
            let _ = bun.wait();
        }

        let mut workers_guard = self.workers.lock().unwrap();
        for worker in workers_guard.iter_mut() {
            let _ = worker.kill();
            let _ = worker.wait();
        }
        Ok(())
    }
}
