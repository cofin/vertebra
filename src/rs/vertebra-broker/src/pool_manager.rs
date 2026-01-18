use crate::config::PoolConfig;
use crate::error::BrokerError;

pub trait PoolManager: Send + Sync {
    fn name(&self) -> &str;
    fn config(&self) -> &PoolConfig;
    fn health_check(&self) -> Result<(), BrokerError>;
}
