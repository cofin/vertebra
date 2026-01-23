use pyo3::prelude::*;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use signal_hook::consts::signal::*;
#[cfg(unix)]
use signal_hook::iterator::Signals;

#[pyclass(name = "NativeSupervisor")]
pub struct NativeSupervisor {
    bun_process: Arc<Mutex<Option<Child>>>,
    workers: Arc<Mutex<Vec<Child>>>,
}

#[pymethods]
impl NativeSupervisor {
    #[new]
    fn new() -> Self {
        Self {
            bun_process: Arc::new(Mutex::new(None)),
            workers: Arc::new(Mutex::new(Vec::new())),
        }
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
        let engine_path = engine_path.to_string();
        let engine_args = engine_args.clone();
        let python_executable = python_executable.to_string();

        // Serialize config if needed, or pass as env vars. For now, we assume basic startup.
        // In a real implementation, we'd extract values from `config`.

        py.detach(move || {
            // Start Bun engine
            let bun = Command::new(&engine_path)
                .args(&engine_args)
                // .env("VERTEBRA_CONFIG", ...)
                .spawn()
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to start engine: {}",
                        e
                    ))
                })?;

            {
                let mut guard = bun_process.lock().unwrap();
                *guard = Some(bun);
            }

            // Start Python workers
            let mut new_workers = Vec::new();
            for _ in 0..workers {
                let worker = Command::new(&python_executable)
                    .arg("-m")
                    .arg("vertebra.server.worker")
                    // .env(...)
                    .spawn()
                    .map_err(|e| {
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
