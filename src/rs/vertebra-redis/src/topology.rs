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

    /// Populate the slot map from a `CLUSTER SLOTS` response.
    pub fn populate_from_cluster_slots(
        &self,
        response: &redis::Value,
        fallback_host: Option<&str>,
    ) -> Result<usize, RedisDriverError> {
        let ranges = match response {
            redis::Value::Array(items) => items,
            other => {
                return Err(RedisDriverError::data(
                    format!("CLUSTER SLOTS response was not an array: {other:?}"),
                    false,
                ));
            }
        };

        let mut next_slots = HashMap::new();
        for range in ranges {
            let redis::Value::Array(cols) = range else {
                continue;
            };
            if cols.len() < 3 {
                continue;
            }
            let Some(start) = extract_slot_index(&cols[0]) else {
                continue;
            };
            let Some(end) = extract_slot_index(&cols[1]) else {
                continue;
            };
            if start > end {
                continue;
            }
            let Some((host, port)) = extract_slots_primary(&cols[2], fallback_host) else {
                continue;
            };
            for slot in start..=end {
                next_slots.insert(slot, (host.clone(), port));
            }
        }

        let count = next_slots.len();
        *self.slots.write() = next_slots;
        Ok(count)
    }
}

fn extract_slot_index(value: &redis::Value) -> Option<u16> {
    match value {
        redis::Value::Int(i) => u16::try_from(*i).ok().filter(|&slot| slot < 16_384),
        _ => None,
    }
}

fn extract_slots_primary(
    value: &redis::Value,
    fallback_host: Option<&str>,
) -> Option<(String, u16)> {
    let redis::Value::Array(node) = value else {
        return None;
    };
    if node.len() < 2 {
        return None;
    }
    let raw_host = match &node[0] {
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Nil => String::new(),
        _ => return None,
    };
    let host = if raw_host.is_empty() {
        fallback_host.unwrap_or_default().to_string()
    } else {
        raw_host
    };
    if host.is_empty() {
        return None;
    }
    let port = match &node[1] {
        redis::Value::Int(p) => u16::try_from(*p).ok()?,
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).parse::<u16>().ok()?,
        redis::Value::SimpleString(s) => s.parse::<u16>().ok()?,
        _ => return None,
    };
    Some((host, port))
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
    let trimmed = err_msg.trim();
    let mut parts = trimmed.split_whitespace();
    let kind_raw = parts.next()?.trim_end_matches(':');
    let is_moved = match kind_raw {
        "MOVED" | "moved" | "Moved" => true,
        "ASK" | "ask" | "Ask" => false,
        _ => return None,
    };
    let slot: u16 = parts.next()?.parse().ok()?;
    if slot >= 16384 {
        return None;
    }
    let addr = parts.next()?;
    let (host, port_str) = addr.rsplit_once(':')?;
    let host_clean = host.trim_start_matches('[').trim_end_matches(']');
    if host_clean.is_empty() {
        return None;
    }
    let port: u16 = port_str.parse().ok()?;
    if is_moved {
        Some(ClusterRedirectAction::Moved {
            slot,
            host: host_clean.to_string(),
            port,
        })
    } else {
        Some(ClusterRedirectAction::Ask {
            slot,
            host: host_clean.to_string(),
            port,
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_populate_slot_map_from_cluster_slots_and_redirect_update() {
        let slot_map = ClusterSlotMap::new();
        let resp = redis::Value::Array(vec![
            redis::Value::Array(vec![
                redis::Value::Int(0),
                redis::Value::Int(5460),
                redis::Value::Array(vec![
                    redis::Value::BulkString(b"".to_vec()),
                    redis::Value::Int(7001),
                ]),
            ]),
            redis::Value::Array(vec![
                redis::Value::Int(5461),
                redis::Value::Int(16383),
                redis::Value::Array(vec![
                    redis::Value::BulkString(b"127.0.0.2".to_vec()),
                    redis::Value::Int(7002),
                ]),
            ]),
        ]);

        let count = slot_map
            .populate_from_cluster_slots(&resp, Some("127.0.0.1"))
            .expect("cluster slots parsed");
        assert_eq!(count, 16_384);
        assert_eq!(
            slot_map.get_slot_node(0),
            Some(("127.0.0.1".to_string(), 7001))
        );
        assert_eq!(
            slot_map.get_slot_node(5461),
            Some(("127.0.0.2".to_string(), 7002))
        );

        let moved = check_cluster_redirect("Moved: 100 127.0.0.3:7003").expect("moved");
        assert_eq!(
            moved,
            ClusterRedirectAction::Moved {
                slot: 100,
                host: "127.0.0.3".to_string(),
                port: 7003,
            }
        );
        if let ClusterRedirectAction::Moved { slot, host, port } = moved {
            slot_map.update_slot(slot, host, port);
        }
        assert_eq!(
            slot_map.get_slot_node(100),
            Some(("127.0.0.3".to_string(), 7003))
        );
    }
}
