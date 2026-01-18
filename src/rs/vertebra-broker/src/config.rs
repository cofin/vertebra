use std::env;
use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::error::BrokerError;

#[derive(Debug, Deserialize, Clone)]
pub struct BrokerConfig {
    pub ipc: IpcConfig,
    pub pools: Vec<PoolConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct IpcConfig {
    pub transport: String,
    pub path: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub codec: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PoolConfig {
    pub name: String,
    pub driver: String,
    pub min_size: usize,
    pub max_size: usize,
}

impl BrokerConfig {
    pub fn from_toml_str(value: &str) -> Result<Self, BrokerError> {
        let expanded = substitute_env(value);
        toml::from_str(&expanded).map_err(|err| BrokerError::Config(err.to_string()))
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, BrokerError> {
        let content = fs::read_to_string(path.as_ref())
            .map_err(|err| BrokerError::Config(err.to_string()))?;
        Self::from_toml_str(&content)
    }
}

fn substitute_env(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '$' && matches!(chars.peek(), Some('{')) {
            let _ = chars.next();
            let mut key = String::new();
            while let Some(next) = chars.next() {
                if next == '}' {
                    break;
                }
                key.push(next);
            }
            if key.is_empty() {
                continue;
            }
            let replacement = env::var(&key).unwrap_or_default();
            output.push_str(&replacement);
        } else {
            output.push(ch);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::BrokerConfig;

    #[test]
    fn parse_config_from_str() {
        let toml = r#"
            [ipc]
            transport = "tcp"
            host = "127.0.0.1"
            port = 7002
            codec = "msgpack"

            [[pools]]
            name = "default"
            driver = "postgres"
            min_size = 1
            max_size = 4
        "#;
        let config = BrokerConfig::from_toml_str(toml).expect("config parsed");
        assert_eq!(config.ipc.transport, "tcp");
        assert_eq!(config.ipc.codec.as_deref(), Some("msgpack"));
        assert_eq!(config.pools.len(), 1);
    }
}
