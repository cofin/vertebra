use pyo3::prelude::*;
use std::path::Path;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use signal_hook::consts::signal::*;
#[cfg(unix)]
use signal_hook::iterator::Signals;
#[cfg(unix)]
use std::os::unix::process::CommandExt;

use vertebra_core::notify::Notifier;

/// Clean up stale shared memory files from previous runs.
///
/// Removes any files in /dev/shm/ that match the given prefix.
/// This is a defensive measure to handle crashed processes that
/// didn't clean up their shared memory regions.
#[cfg(unix)]
fn cleanup_stale_shm(prefix: &str) {
    let shm_dir = Path::new("/dev/shm");
    if !shm_dir.exists() {
        return;
    }
    if let Ok(entries) = std::fs::read_dir(shm_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with(prefix) {
                // Best-effort removal - ignore errors (may be in use)
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

#[cfg(not(unix))]
fn cleanup_stale_shm(_prefix: &str) {
    // No-op on non-Unix platforms (Windows uses different shm mechanism)
}

/// FD information for cross-process IPC.
#[derive(Default)]
struct InheritedFds {
    /// Request notify FD (Rust→Python direction): signals when request is available.
    req_notify_fd: Option<i32>,
    /// Response notify FD (Python→Rust direction): signals when response is ready.
    /// On Linux this is the same as req_notify_fd (eventfd is bidirectional).
    /// On other platforms this is the pipe write end.
    resp_notify_fd: Option<i32>,
}

#[pyclass(name = "NativeSupervisor")]
pub struct NativeSupervisor {
    bun_process: Arc<Mutex<Option<Child>>>,
    workers: Arc<Mutex<Vec<Child>>>,
    /// Notifier for event-driven IPC wakeups.
    /// Created BEFORE spawning children so FD is inherited (no CLOEXEC).
    notifier: Arc<Mutex<Option<Notifier>>>,
    /// FD numbers for passing to children.
    fds: Arc<Mutex<InheritedFds>>,
    /// Optional dev server process (e.g., npm run dev).
    dev_server: Arc<Mutex<Option<Child>>>,
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
            dev_server: Arc::new(Mutex::new(None)),
        }
    }

    /// Get the request notify FD for Python to use with asyncio.add_reader().
    fn req_notify_fd(&self) -> Option<i32> {
        self.fds.lock().unwrap().req_notify_fd
    }

    /// Get the response notify FD for Python to write to after sending responses.
    fn resp_notify_fd(&self) -> Option<i32> {
        self.fds.lock().unwrap().resp_notify_fd
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

        // Create Notifier BEFORE spawning children.
        // FDs are inheritable (no CLOEXEC), allowing forked processes to receive them.
        let notifier = Notifier::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create notifier: {}",
                e
            ))
        })?;
        let req_notify_fd = notifier.as_raw_fd();
        // On Linux, eventfd is bidirectional (same FD for read/write).
        // On other platforms, Notifier uses pipe (separate read/write FDs).
        #[cfg(target_os = "linux")]
        let resp_notify_fd = req_notify_fd;
        #[cfg(not(target_os = "linux"))]
        let resp_notify_fd = notifier.write_fd();

        // Store notifier and FD numbers for later access.
        {
            let mut guard = notifier_arc.lock().unwrap();
            *guard = Some(notifier);
        }
        {
            let mut fds = fds_arc.lock().unwrap();
            fds.req_notify_fd = Some(req_notify_fd);
            fds.resp_notify_fd = Some(resp_notify_fd);
        }

        py.detach(move || {
            // Clean up any stale shared memory files from previous runs
            cleanup_stale_shm("vertebra-");

            let app_path = std::env::var("VERTEBRA_PYTHON_APP").map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "VERTEBRA_PYTHON_APP must be set to start Python workers",
                )
            })?;
            let host = std::env::var("VERTEBRA_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
            let port = std::env::var("VERTEBRA_PORT")
                .ok()
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap_or(8000);

            // Start Bun engine with inherited FDs via environment variables.
            let mut cmd = Command::new(&engine_path);
            // Read spin config from environment or use default.
            // Bun/Rust use time-based spin (VERTEBRA_SPIN_US in microseconds).
            // Python uses iteration-based spin (VERTEBRA_SPIN_ITERS, ~50 iters ≈ 5µs).
            let spin_us = std::env::var("VERTEBRA_SPIN_US")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(5);
            let spin_iters = std::env::var("VERTEBRA_SPIN_ITERS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(50); // Default: ~50 iterations ≈ 5µs

            cmd.args(&engine_args)
                .env("VERTEBRA_REQ_NOTIFY_FD", req_notify_fd.to_string())
                .env("VERTEBRA_RESP_NOTIFY_FD", resp_notify_fd.to_string())
                .env("VERTEBRA_SKIP_PROCESS_MANAGER", "1")
                .env("VERTEBRA_SPIN_US", spin_us.to_string());

            // Set up parent death signal so child dies when supervisor dies.
            // This prevents orphaned Bun processes if the parent is killed unexpectedly.
            #[cfg(target_os = "linux")]
            unsafe {
                cmd.pre_exec(|| {
                    // PR_SET_PDEATHSIG tells the kernel to send SIGTERM when parent dies
                    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                    Ok(())
                });
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
            for worker_id in 0..workers {
                let mut cmd = Command::new(&python_executable);
                cmd.arg("-m")
                    .arg("vertebra.server.worker")
                    .arg("--app")
                    .arg(&app_path)
                    .arg("--host")
                    .arg(&host)
                    .arg("--port")
                    .arg(port.to_string())
                    .arg("--worker-id")
                    .arg(worker_id.to_string())
                    .env("VERTEBRA_REQ_NOTIFY_FD", req_notify_fd.to_string())
                    .env("VERTEBRA_RESP_NOTIFY_FD", resp_notify_fd.to_string())
                    .env("VERTEBRA_SPIN_ITERS", spin_iters.to_string());

                // Set up parent death signal so worker dies when supervisor dies.
                #[cfg(target_os = "linux")]
                unsafe {
                    cmd.pre_exec(|| {
                        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                        Ok(())
                    });
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

    /// Start a frontend development server (e.g. npm run dev)
    fn start_dev_server(
        &mut self,
        py: Python,
        command: Vec<String>,
        cwd: Option<String>,
    ) -> PyResult<()> {
        let dev_server_arc = self.dev_server.clone();

        py.detach(move || {
            if command.is_empty() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Command cannot be empty",
                ));
            }

            let mut cmd = Command::new(&command[0]);
            cmd.args(&command[1..]);

            if let Some(dir) = cwd {
                cmd.current_dir(dir);
            }

            let child = cmd.spawn().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to start dev server: {}",
                    e
                ))
            })?;

            // Store the child handle for cleanup in stop_internal
            {
                let mut guard = dev_server_arc.lock().unwrap();
                *guard = Some(child);
            }

            Ok(())
        })
    }
}

impl NativeSupervisor {
    fn stop_internal(&self) -> PyResult<()> {
        // Stop dev server if running
        let mut dev_server_guard = self.dev_server.lock().unwrap();
        if let Some(ref mut dev) = *dev_server_guard {
            let _ = dev.kill(); // Ignore errors if already dead
            let _ = dev.wait();
        }
        *dev_server_guard = None;

        // Stop bun process
        let mut bun_guard = self.bun_process.lock().unwrap();
        if let Some(ref mut bun) = *bun_guard {
            let _ = bun.kill(); // Ignore errors if already dead
            let _ = bun.wait();
        }

        // Stop workers
        let mut workers_guard = self.workers.lock().unwrap();
        for worker in workers_guard.iter_mut() {
            let _ = worker.kill();
            let _ = worker.wait();
        }
        Ok(())
    }
}
