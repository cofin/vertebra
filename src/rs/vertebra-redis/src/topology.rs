//! Redis Cluster slot routing, redirect handling, and Sentinel master discovery.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::errors::RedisDriverError;

/// Cluster slot map mapping hash slot (0..16383) to node `(host, port)`.
#[derive(Clone, Default)]
pub struct ClusterSlotMap {
    slots: Arc<RwLock<HashMap<u16, (String, u16)>>>,
}

impl ClusterSlotMap {
    pub fn new() -> Self {
        Self {
            slots: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_slot_node(&self, slot: u16) -> Option<(String, u16)> {
        self.slots.read().get(&slot).cloned()
    }

    pub fn update_slot(&self, slot: u16, host: String, port: u16) {
        self.slots.write().insert(slot, (host, port));
    }

    pub fn snapshot(&self) -> HashMap<u16, (String, u16)> {
        self.slots.read().clone()
    }

    pub fn clear(&self) {
        self.slots.write().clear();
    }
}

/// Action to take on a cluster command error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRedirectAction {
    /// Update slot map and retry against the target node.
    Moved { slot: u16, host: String, port: u16 },
    /// Send ASKING to the target node without mutating slot map.
    Ask { slot: u16, host: String, port: u16 },
}

/// Parse a Redis error message to detect MOVED or ASK redirections.
pub fn check_cluster_redirect(err_msg: &str) -> Option<ClusterRedirectAction> {
    if let Some(r) = vertebra_db::resp3::parse_cluster_redirect(err_msg) {
        match r.kind {
            vertebra_db::resp3::RedirectKind::Moved => {
                return Some(ClusterRedirectAction::Moved {
                    slot: r.slot,
                    host: r.host,
                    port: r.port,
                });
            }
            vertebra_db::resp3::RedirectKind::Ask => {
                return Some(ClusterRedirectAction::Ask {
                    slot: r.slot,
                    host: r.host,
                    port: r.port,
                });
            }
        }
    }
    None
}

/// Parse a Sentinel `SENTINEL get-master-addr-by-name` response.
pub fn parse_sentinel_master_addr(
    response: &redis::Value,
) -> Result<(String, u16), RedisDriverError> {
    match response {
        redis::Value::Array(items) if items.len() >= 2 => {
            let host = match &items[0] {
                redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                redis::Value::SimpleString(s) => s.clone(),
                other => {
                    return Err(RedisDriverError::operational(
                        format!("Unexpected host type in Sentinel response: {other:?}"),
                        true,
                    ));
                }
            };
            let port = match &items[1] {
                redis::Value::Int(i) => *i as u16,
                redis::Value::BulkString(b) => {
                    let s = String::from_utf8_lossy(b);
                    s.parse::<u16>().map_err(|e| {
                        RedisDriverError::operational(
                            format!("Failed to parse Sentinel port '{s}': {e}"),
                            true,
                        )
                    })?
                }
                redis::Value::SimpleString(s) => s.parse::<u16>().map_err(|e| {
                    RedisDriverError::operational(
                        format!("Failed to parse Sentinel port '{s}': {e}"),
                        true,
                    )
                })?,
                other => {
                    return Err(RedisDriverError::operational(
                        format!("Unexpected port type in Sentinel response: {other:?}"),
                        true,
                    ));
                }
            };
            Ok((host, port))
        }
        redis::Value::Nil => Err(RedisDriverError::operational(
            "Master name not found by Sentinel",
            true,
        )),
        other => Err(RedisDriverError::operational(
            format!("Unexpected response from Sentinel get-master-addr-by-name: {other:?}"),
            true,
        )),
    }
}
