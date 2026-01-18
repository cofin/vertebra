use thiserror::Error;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("ipc error: {0}")]
    Ipc(String),
    #[error("pool error: {0}")]
    Pool(String),
}
