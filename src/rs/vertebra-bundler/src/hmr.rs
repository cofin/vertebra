//! HMR (Hot Module Replacement) file watcher implementation

use crate::errors::{BundlerError, Result};
use crossbeam_channel::{Receiver, Sender, bounded};
use notify::{RecommendedWatcher, WatchMode};
use notify_debouncer_mini::{DebouncedEvent, Debouncer, new_debouncer};
use pyo3::prelude::*;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

/// HMR event types sent to connected clients
#[derive(Clone, Debug)]
pub enum HmrEvent {
    /// File was updated
    Update { path: String, timestamp: u64 },
    /// Type definitions changed
    Types { version: String },
    /// Full page reload required
    Reload,
}

impl HmrEvent {
    pub fn update(path: impl Into<String>) -> Self {
        Self::Update {
            path: path.into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    pub fn types(version: impl Into<String>) -> Self {
        Self::Types {
            version: version.into(),
        }
    }

    pub fn reload() -> Self {
        Self::Reload
    }
}

/// File watcher for HMR
pub struct SourceWatcher {
    debouncer: Debouncer<RecommendedWatcher>,
    event_tx: Sender<HmrEvent>,
    event_rx: Receiver<HmrEvent>,
    watch_dirs: Vec<PathBuf>,
}

impl SourceWatcher {
    /// Create a new HMR watcher
    pub fn new(
        watch_dirs: Vec<PathBuf>,
        extensions: Vec<String>,
        debounce_ms: u64,
    ) -> Result<(Self, Receiver<HmrEvent>)> {
        let (event_tx, event_rx) = bounded(256);
        let event_tx_clone = event_tx.clone();

        let debouncer = new_debouncer(
            Duration::from_millis(debounce_ms),
            move |result: std::result::Result<Vec<DebouncedEvent>, _>| {
                if let Ok(events) = result {
                    for event in events {
                        let path = &event.path;

                        // Check extension filter
                        if let Some(ext) = path.extension() {
                            let ext_str = ext.to_string_lossy();
                            if extensions.iter().any(|e| e == ext_str.as_ref()) {
                                let path_str = path.to_string_lossy().to_string();
                                let _ = event_tx_clone.try_send(HmrEvent::update(path_str));
                            }
                        }
                    }
                }
            },
        )
        .map_err(|e| BundlerError::Watcher(e.to_string()))?;

        let rx_for_caller = event_rx.clone();
        Ok((
            Self {
                debouncer,
                event_tx,
                event_rx,
                watch_dirs,
            },
            rx_for_caller,
        ))
    }

    /// Start watching directories
    pub fn start(&mut self) -> Result<Receiver<HmrEvent>> {
        for dir in &self.watch_dirs {
            self.debouncer
                .watcher()
                .watch(dir, WatchMode::recursive())
                .map_err(|e| BundlerError::Watcher(e.to_string()))?;
        }
        Ok(self.event_rx.clone())
    }

    /// Stop watching
    pub fn stop(&mut self) -> Result<()> {
        for dir in &self.watch_dirs {
            let _ = self.debouncer.watcher().unwatch(dir);
        }
        Ok(())
    }

    /// Send a type change notification
    pub fn notify_type_change(&self, version: &str) {
        let _ = self.event_tx.try_send(HmrEvent::types(version));
    }

    /// Send a reload signal
    pub fn notify_reload(&self) {
        let _ = self.event_tx.try_send(HmrEvent::reload());
    }
}

struct WatcherState {
    watcher: Option<SourceWatcher>,
    rx: Option<Receiver<HmrEvent>>,
    watch_dirs: Vec<PathBuf>,
    extensions: Vec<String>,
    debounce_ms: u64,
}

/// Python-visible wrapper around `SourceWatcher` with non-blocking event polling.
#[pyclass(name = "SourceWatcher", immutable_type)]
pub struct PySourceWatcher {
    state: Mutex<WatcherState>,
}

#[pymethods]
impl PySourceWatcher {
    #[new]
    #[pyo3(signature = (watch_dirs, extensions=None, debounce_ms=100))]
    pub fn py_new(
        watch_dirs: Vec<String>,
        extensions: Option<Vec<String>>,
        debounce_ms: u64,
    ) -> PyResult<Self> {
        let paths: Vec<PathBuf> = watch_dirs.into_iter().map(PathBuf::from).collect();
        let exts = extensions.unwrap_or_else(|| vec!["py".to_string(), "ts".to_string()]);
        let (watcher, rx) = SourceWatcher::new(paths.clone(), exts.clone(), debounce_ms)?;
        Ok(Self {
            state: Mutex::new(WatcherState {
                watcher: Some(watcher),
                rx: Some(rx),
                watch_dirs: paths,
                extensions: exts,
                debounce_ms,
            }),
        })
    }

    pub fn start(&self) -> PyResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BundlerError::Poisoned("watcher"))?;
        if state.watcher.is_none() {
            let (watcher, rx) = SourceWatcher::new(
                state.watch_dirs.clone(),
                state.extensions.clone(),
                state.debounce_ms,
            )?;
            state.watcher = Some(watcher);
            state.rx = Some(rx);
        }
        if let Some(ref mut w) = state.watcher {
            let rx = w.start()?;
            state.rx = Some(rx);
        }
        Ok(())
    }

    pub fn stop(&self) -> PyResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BundlerError::Poisoned("watcher"))?;
        if let Some(ref mut w) = state.watcher {
            w.stop()?;
        }
        state.watcher = None;
        state.rx = None;
        Ok(())
    }

    /// Drain updated file paths from the receiver, optionally blocking up to `timeout_ms`.
    #[pyo3(signature = (timeout_ms=None))]
    pub fn poll_events(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<Vec<String>> {
        let rx = {
            let state = self
                .state
                .lock()
                .map_err(|_| BundlerError::Poisoned("watcher"))?;
            match &state.rx {
                Some(rx) => rx.clone(),
                None => return Ok(Vec::new()),
            }
        };

        py.detach(|| {
            let mut paths = Vec::new();
            if let Some(ms) = timeout_ms
                && ms > 0
                && let Ok(HmrEvent::Update { path, .. }) =
                    rx.recv_timeout(Duration::from_millis(ms))
            {
                paths.push(path);
            }
            while let Ok(evt) = rx.try_recv() {
                if let HmrEvent::Update { path, .. } = evt {
                    paths.push(path);
                }
            }
            Ok(paths)
        })
    }

    pub fn notify_type_change(&self, version: &str) -> PyResult<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| BundlerError::Poisoned("watcher"))?;
        if let Some(ref w) = state.watcher {
            w.notify_type_change(version);
        }
        Ok(())
    }

    pub fn notify_reload(&self) -> PyResult<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| BundlerError::Poisoned("watcher"))?;
        if let Some(ref w) = state.watcher {
            w.notify_reload();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmr_events() {
        let event = HmrEvent::update("/src/App.tsx");
        if let HmrEvent::Update { path, .. } = event {
            assert_eq!(path, "/src/App.tsx");
        } else {
            panic!("Expected Update event");
        }

        let types_event = HmrEvent::types("abc123");
        if let HmrEvent::Types { version } = types_event {
            assert_eq!(version, "abc123");
        } else {
            panic!("Expected Types event");
        }

        let reload_event = HmrEvent::reload();
        assert!(matches!(reload_event, HmrEvent::Reload));
    }

    #[test]
    fn test_hmr_watcher_creation() {
        let watch_dirs = vec![PathBuf::from(".")];
        let extensions = vec!["ts".to_string(), "tsx".to_string()];

        let result = SourceWatcher::new(watch_dirs, extensions, 100);
        assert!(result.is_ok());
    }
}
