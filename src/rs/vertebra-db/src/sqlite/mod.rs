//! SQLite 3.45+ WAL, `mmap_size`, and `busy_timeout` initialization pragmas.

use serde::{Deserialize, Serialize};

/// Configuration for SQLite 3.45+ WAL mode, memory-mapped I/O, and reader pool sizing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteConfig {
    pub path: String,
    pub wal_mode: bool,
    pub synchronous: String,
    pub mmap_size: u64,
    pub busy_timeout_ms: u32,
    pub max_readers: u16,
    pub foreign_keys: bool,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: ":memory:".to_string(),
            wal_mode: true,
            synchronous: "NORMAL".to_string(),
            mmap_size: 268_435_456,
            busy_timeout_ms: 5000,
            max_readers: 16,
            foreign_keys: true,
        }
    }
}

impl SqliteConfig {
    /// Generate the ordered list of initialization `PRAGMA` statements for this configuration.
    pub fn initialization_pragmas(&self) -> Vec<String> {
        let mut pragmas = Vec::with_capacity(6);
        if self.wal_mode {
            pragmas.push("PRAGMA journal_mode = WAL".to_string());
        } else {
            pragmas.push("PRAGMA journal_mode = MEMORY".to_string());
        }
        let sync_mode = self.synchronous.trim().to_ascii_uppercase();
        pragmas.push(format!("PRAGMA synchronous = {sync_mode}"));
        pragmas.push(format!("PRAGMA mmap_size = {}", self.mmap_size));
        pragmas.push(format!("PRAGMA busy_timeout = {}", self.busy_timeout_ms));
        pragmas.push("PRAGMA temp_store = MEMORY".to_string());
        let fk = if self.foreign_keys { "ON" } else { "OFF" };
        pragmas.push(format!("PRAGMA foreign_keys = {fk}"));
        pragmas
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_initialization_pragmas() {
        let cfg = SqliteConfig::default();
        let pragmas = cfg.initialization_pragmas();
        assert_eq!(
            pragmas,
            vec![
                "PRAGMA journal_mode = WAL",
                "PRAGMA synchronous = NORMAL",
                "PRAGMA mmap_size = 268435456",
                "PRAGMA busy_timeout = 5000",
                "PRAGMA temp_store = MEMORY",
                "PRAGMA foreign_keys = ON",
            ]
        );
    }
}
