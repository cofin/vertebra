//! HMR (Hot Module Replacement) file watcher implementation

use crate::errors::{BundlerError, Result};
use notify::{RecommendedWatcher, WatchMode};
use notify_debouncer_mini::{DebouncedEvent, Debouncer, new_debouncer};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::broadcast;

/// HMR event types sent to connected clients
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum HmrEvent {
    /// File was updated
    Update { path: String, timestamp: u64 },
    /// Type definitions changed
    Types { version: String },
    /// Full page reload required
    Reload,
    /// Connection established
    Connected { hmr_url: String },
}

impl HmrEvent {
    pub fn update(path: impl Into<String>) -> Self {
        Self::Update {
            path: path.into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
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

    pub fn connected(hmr_url: impl Into<String>) -> Self {
        Self::Connected {
            hmr_url: hmr_url.into(),
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// File watcher for HMR
pub struct HmrWatcher {
    debouncer: Debouncer<RecommendedWatcher>,
    event_tx: broadcast::Sender<HmrEvent>,
    watch_dirs: Vec<PathBuf>,
}

impl HmrWatcher {
    /// Create a new HMR watcher
    pub fn new(
        watch_dirs: Vec<PathBuf>,
        extensions: Vec<String>,
        debounce_ms: u64,
    ) -> Result<(Self, broadcast::Receiver<HmrEvent>)> {
        let (event_tx, event_rx) = broadcast::channel(100);
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
                                let _ = event_tx_clone.send(HmrEvent::update(path_str));
                            }
                        }
                    }
                }
            },
        )
        .map_err(|e| BundlerError::Watcher(e.to_string()))?;

        Ok((
            Self {
                debouncer,
                event_tx,
                watch_dirs,
            },
            event_rx,
        ))
    }

    /// Start watching directories
    pub fn start(&mut self) -> Result<()> {
        for dir in &self.watch_dirs {
            self.debouncer
                .watcher()
                .watch(dir, WatchMode::recursive())
                .map_err(|e| BundlerError::Watcher(e.to_string()))?;
        }
        Ok(())
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
        let _ = self.event_tx.send(HmrEvent::types(version));
    }

    /// Send a reload signal
    pub fn notify_reload(&self) {
        let _ = self.event_tx.send(HmrEvent::reload());
    }
}

/// Domain-oriented alias for [`HmrWatcher`].
pub type SourceWatcher = HmrWatcher;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmr_event_serialization() {
        let event = HmrEvent::update("/src/App.tsx");
        let json = event.to_json();
        assert!(json.contains("\"type\":\"update\""));
        assert!(json.contains("/src/App.tsx"));

        let types_event = HmrEvent::types("abc123");
        let json = types_event.to_json();
        assert!(json.contains("\"type\":\"types\""));
        assert!(json.contains("abc123"));

        let reload_event = HmrEvent::reload();
        let json = reload_event.to_json();
        assert!(json.contains("\"type\":\"reload\""));

        let connected_event = HmrEvent::connected("ws://localhost:5173/hmr");
        let json = connected_event.to_json();
        assert!(json.contains("\"type\":\"connected\""));
        assert!(json.contains("ws://localhost:5173/hmr"));
    }

    #[test]
    fn test_hmr_watcher_creation() {
        let watch_dirs = vec![PathBuf::from(".")];
        let extensions = vec!["ts".to_string(), "tsx".to_string()];

        let result = HmrWatcher::new(watch_dirs, extensions, 100);
        assert!(result.is_ok());
    }
}
