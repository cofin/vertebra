//! SQLite 3.45+ configuration, URI handling, and initialization pragmas.

use std::fmt;
use std::str::FromStr;

use crate::errors::SqliteClientError;

/// Synchronous pragma mode for SQLite.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SqliteSynchronous {
    /// Synchronous OFF (fastest, lowest durability).
    Off,
    /// Synchronous NORMAL (recommended for WAL mode).
    #[default]
    Normal,
    /// Synchronous FULL (traditional durability).
    Full,
    /// Synchronous EXTRA (extra sync on directory operations).
    Extra,
}

impl SqliteSynchronous {
    /// Return the canonical uppercase pragma string value.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
            Self::Extra => "EXTRA",
        }
    }
}

impl fmt::Display for SqliteSynchronous {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SqliteSynchronous {
    type Err = SqliteClientError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed.eq_ignore_ascii_case("OFF") || trimmed == "0" {
            Ok(Self::Off)
        } else if trimmed.eq_ignore_ascii_case("NORMAL") || trimmed == "1" {
            Ok(Self::Normal)
        } else if trimmed.eq_ignore_ascii_case("FULL") || trimmed == "2" {
            Ok(Self::Full)
        } else if trimmed.eq_ignore_ascii_case("EXTRA") || trimmed == "3" {
            Ok(Self::Extra)
        } else {
            Err(SqliteClientError::InvalidInput(format!(
                "invalid SQLite synchronous mode `{s}`; must be one of OFF, NORMAL, FULL, EXTRA"
            )))
        }
    }
}

/// Configuration for SQLite 3.45+ client worker pool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqliteClientConfig {
    /// Database path or URI (e.g. `":memory:"`, `"file:memdb1?mode=memory&cache=shared"`, or filesystem path).
    pub path: String,
    /// Whether to enable Write-Ahead Logging (`PRAGMA journal_mode = WAL`).
    pub wal_mode: bool,
    /// SQLite synchronous pragma mode.
    pub synchronous: SqliteSynchronous,
    /// Memory-mapped I/O byte size (`PRAGMA mmap_size`).
    pub mmap_size: u64,
    /// Busy timeout in milliseconds (`PRAGMA busy_timeout`).
    pub busy_timeout_ms: u32,
    /// Maximum concurrent read-only worker threads for WAL mode.
    pub max_readers: usize,
    /// Whether to enforce foreign keys (`PRAGMA foreign_keys = ON`).
    pub foreign_keys: bool,
    /// Whether to enable SQLite 3.45+ JSONB support.
    pub enable_jsonb: bool,
}

impl Default for SqliteClientConfig {
    fn default() -> Self {
        Self {
            path: ":memory:".to_string(),
            wal_mode: true,
            synchronous: SqliteSynchronous::Normal,
            mmap_size: 268_435_456,
            busy_timeout_ms: 5000,
            max_readers: 8,
            foreign_keys: true,
            enable_jsonb: true,
        }
    }
}

impl SqliteClientConfig {
    /// Return whether this database targets plain unshared `:memory:`.
    #[must_use]
    pub fn is_plain_in_memory(&self) -> bool {
        let trimmed = self.path.trim();
        trimmed.is_empty() || trimmed == ":memory:"
    }

    /// Return whether this database uses a URI shared in-memory format.
    #[must_use]
    pub fn is_shared_in_memory(&self) -> bool {
        let trimmed = self.path.trim();
        trimmed.starts_with("file:") && trimmed.contains("mode=memory")
    }

    /// Return whether this database is in-memory (plain or shared URI).
    #[must_use]
    pub fn is_in_memory(&self) -> bool {
        self.is_plain_in_memory() || self.is_shared_in_memory()
    }

    /// Return whether this database path is formatted as a SQLite URI (`file:...`).
    #[must_use]
    pub fn is_uri(&self) -> bool {
        self.path.trim().starts_with("file:")
    }

    /// Validate the configuration parameters.
    pub fn validate(&self) -> Result<(), SqliteClientError> {
        if self.path.contains('\0') {
            return Err(SqliteClientError::InvalidInput(
                "database path must not contain NUL bytes".to_string(),
            ));
        }
        Ok(())
    }

    /// Generate the ordered initialization PRAGMA statements.
    #[must_use]
    pub fn initialization_pragmas(&self) -> Vec<String> {
        let mut pragmas = Vec::with_capacity(6);

        // WAL mode is omitted for plain :memory: as in-memory databases cannot use WAL
        if !self.is_plain_in_memory() {
            if self.wal_mode {
                pragmas.push("PRAGMA journal_mode = WAL".to_string());
            } else {
                pragmas.push("PRAGMA journal_mode = DELETE".to_string());
            }
        }

        pragmas.push(format!("PRAGMA synchronous = {}", self.synchronous));
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
    fn test_default_config_pragmas() {
        let cfg = SqliteClientConfig::default();
        assert!(cfg.is_plain_in_memory());
        assert!(cfg.is_in_memory());
        assert!(!cfg.is_uri());

        let pragmas = cfg.initialization_pragmas();
        assert_eq!(
            pragmas,
            vec![
                "PRAGMA synchronous = NORMAL",
                "PRAGMA mmap_size = 268435456",
                "PRAGMA busy_timeout = 5000",
                "PRAGMA temp_store = MEMORY",
                "PRAGMA foreign_keys = ON",
            ]
        );
    }

    #[test]
    fn test_file_config_pragmas() {
        let cfg = SqliteClientConfig {
            path: "/tmp/test.db".to_string(),
            wal_mode: true,
            synchronous: SqliteSynchronous::Extra,
            mmap_size: 67_108_864,
            busy_timeout_ms: 10_000,
            max_readers: 4,
            foreign_keys: false,
            enable_jsonb: true,
        };
        assert!(!cfg.is_plain_in_memory());
        assert!(!cfg.is_in_memory());

        let pragmas = cfg.initialization_pragmas();
        assert_eq!(
            pragmas,
            vec![
                "PRAGMA journal_mode = WAL",
                "PRAGMA synchronous = EXTRA",
                "PRAGMA mmap_size = 67108864",
                "PRAGMA busy_timeout = 10000",
                "PRAGMA temp_store = MEMORY",
                "PRAGMA foreign_keys = OFF",
            ]
        );
    }

    #[test]
    fn test_uri_shared_memory_pragmas() {
        let cfg = SqliteClientConfig {
            path: "file:memdb1?mode=memory&cache=shared".to_string(),
            wal_mode: true,
            ..Default::default()
        };
        assert!(!cfg.is_plain_in_memory());
        assert!(cfg.is_shared_in_memory());
        assert!(cfg.is_in_memory());
        assert!(cfg.is_uri());

        let pragmas = cfg.initialization_pragmas();
        assert!(pragmas.contains(&"PRAGMA journal_mode = WAL".to_string()));
    }

    #[test]
    fn test_synchronous_parsing() {
        assert_eq!(
            "off".parse::<SqliteSynchronous>().unwrap(),
            SqliteSynchronous::Off
        );
        assert_eq!(
            "Normal".parse::<SqliteSynchronous>().unwrap(),
            SqliteSynchronous::Normal
        );
        assert_eq!(
            "FULL".parse::<SqliteSynchronous>().unwrap(),
            SqliteSynchronous::Full
        );
        assert_eq!(
            "extra".parse::<SqliteSynchronous>().unwrap(),
            SqliteSynchronous::Extra
        );
        assert!("BOGUS".parse::<SqliteSynchronous>().is_err());
    }

    #[test]
    fn test_validation() {
        let valid = SqliteClientConfig::default();
        assert!(valid.validate().is_ok());

        let zero_readers = SqliteClientConfig {
            max_readers: 0,
            ..Default::default()
        };
        assert!(zero_readers.validate().is_ok());

        let invalid_path = SqliteClientConfig {
            path: "bad\0path".to_string(),
            ..Default::default()
        };
        assert!(invalid_path.validate().is_err());
    }
}
