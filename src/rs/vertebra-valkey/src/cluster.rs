//! Valkey Cluster topology (`CLUSTER SHARDS` / `CLUSTER SLOTS`), slot routing, `MOVED`/`ASK`, and `READONLY` replicas.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::codec::RespValue;
use crate::error::ValkeyError;
use crate::valkey_hash_slot;

pub const CLUSTER_SLOTS_COUNT: usize = 16_384;

/// A cluster shard node endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterNodeEndpoint {
    pub host: String,
    pub port: u16,
}

impl ClusterNodeEndpoint {
    #[must_use]
    pub fn addr_key(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Primary + healthy online replicas for a Valkey Cluster hash-slot shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRoute {
    pub primary: ClusterNodeEndpoint,
    pub replicas: Vec<ClusterNodeEndpoint>,
}

/// Cached 16384-slot Valkey Cluster topology table.
#[derive(Debug)]
pub struct ClusterTopology {
    slots: Vec<Option<usize>>,
    shards: Vec<ShardRoute>,
    replica_rr: AtomicUsize,
}

impl Clone for ClusterTopology {
    fn clone(&self) -> Self {
        Self {
            slots: self.slots.clone(),
            shards: self.shards.clone(),
            replica_rr: AtomicUsize::new(self.replica_rr.load(Ordering::Relaxed)),
        }
    }
}

impl Default for ClusterTopology {
    fn default() -> Self {
        Self::empty()
    }
}

impl ClusterTopology {
    /// Construct an empty unpopulated 16384-slot table.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            slots: vec![None; CLUSTER_SLOTS_COUNT],
            shards: Vec::new(),
            replica_rr: AtomicUsize::new(0),
        }
    }

    /// Construct a 1-shard initial fallback topology pointing to a single seed endpoint.
    #[must_use]
    pub fn initial_seed_fallback(host: String, port: u16) -> Self {
        let mut topo = Self::empty();
        topo.shards.push(ShardRoute {
            primary: ClusterNodeEndpoint { host, port },
            replicas: Vec::new(),
        });
        topo
    }

    /// Parse Valkey `CLUSTER SHARDS` response (RESP3 `Array[Map]` or RESP2 flat `Array`).
    pub fn from_cluster_shards(resp: &RespValue, use_tls: bool) -> Result<Self, ValkeyError> {
        let val = resp.unattributed();
        if let RespValue::SimpleError(msg) | RespValue::BulkError(msg) = val {
            return Err(ValkeyError::from_server_error(msg));
        }
        let Some(shard_entries) = val.as_slice() else {
            return Err(ValkeyError::data(
                "CLUSTER SHARDS response was not an array",
            ));
        };
        let mut topo = Self::empty();
        for entry in shard_entries {
            let Some((slot_pairs, nodes)) = parse_shard_entry(entry, use_tls)? else {
                continue;
            };
            let mut primary: Option<ClusterNodeEndpoint> = None;
            let mut replicas = Vec::new();
            for node in nodes {
                match node.role.as_str() {
                    "master" | "primary" => {
                        if primary.is_none() || node.health == "online" {
                            primary = Some(node.endpoint);
                        }
                    }
                    "replica" | "slave" if node.health == "online" => {
                        replicas.push(node.endpoint);
                    }
                    _ => {}
                }
            }
            let Some(primary_ep) = primary else {
                continue;
            };
            let shard_idx = topo.shards.len();
            topo.shards.push(ShardRoute {
                primary: primary_ep,
                replicas,
            });
            for (start, end) in slot_pairs {
                topo.assign_slot_range(start, end, shard_idx);
            }
        }
        if topo.shards.is_empty() {
            return Err(ValkeyError::operational(
                "CLUSTER SHARDS returned zero healthy primary shards",
                true,
            ));
        }
        Ok(topo)
    }

    /// Parse legacy `CLUSTER SLOTS` response into a [`ClusterTopology`].
    pub fn from_cluster_slots(resp: &RespValue) -> Result<Self, ValkeyError> {
        let val = resp.unattributed();
        if let RespValue::SimpleError(msg) | RespValue::BulkError(msg) = val {
            return Err(ValkeyError::from_server_error(msg));
        }
        let Some(rows) = val.as_slice() else {
            return Err(ValkeyError::data("CLUSTER SLOTS response was not an array"));
        };
        let mut topo = Self::empty();
        let mut primary_index: Vec<(ClusterNodeEndpoint, usize)> = Vec::new();

        for row in rows {
            let Some(cols) = row.as_slice() else {
                continue;
            };
            if cols.len() < 3 {
                continue;
            }
            let start = cols[0]
                .as_i64()
                .and_then(|v| u16::try_from(v).ok())
                .ok_or_else(|| ValkeyError::data("Invalid CLUSTER SLOTS start slot"))?;
            let end = cols[1]
                .as_i64()
                .and_then(|v| u16::try_from(v).ok())
                .ok_or_else(|| ValkeyError::data("Invalid CLUSTER SLOTS end slot"))?;
            let Some(primary_ep) = parse_slots_node(&cols[2]) else {
                continue;
            };
            let mut replicas = Vec::new();
            for rep_node in &cols[3..] {
                if let Some(ep) = parse_slots_node(rep_node) {
                    replicas.push(ep);
                }
            }
            let shard_idx = if let Some((_, existing_idx)) =
                primary_index.iter().find(|(ep, _)| *ep == primary_ep)
            {
                let idx = *existing_idx;
                if topo.shards[idx].replicas.is_empty() && !replicas.is_empty() {
                    topo.shards[idx].replicas = replicas;
                }
                idx
            } else {
                let idx = topo.shards.len();
                primary_index.push((primary_ep.clone(), idx));
                topo.shards.push(ShardRoute {
                    primary: primary_ep,
                    replicas,
                });
                idx
            };
            topo.assign_slot_range(start, end, shard_idx);
        }

        if topo.shards.is_empty() {
            return Err(ValkeyError::operational(
                "CLUSTER SLOTS returned zero shards",
                true,
            ));
        }
        Ok(topo)
    }

    fn assign_slot_range(&mut self, start: u16, end: u16, shard_idx: usize) {
        let s = usize::from(start).min(CLUSTER_SLOTS_COUNT.saturating_sub(1));
        let e = usize::from(end).min(CLUSTER_SLOTS_COUNT.saturating_sub(1));
        if s <= e {
            for slot in &mut self.slots[s..=e] {
                *slot = Some(shard_idx);
            }
        }
    }

    /// Update a single slot owner immediately upon receiving a `-MOVED` redirect.
    pub fn update_moved_slot(&mut self, slot: u16, host: &str, port: u16) {
        if usize::from(slot) >= CLUSTER_SLOTS_COUNT {
            return;
        }
        let target = ClusterNodeEndpoint {
            host: host.to_string(),
            port,
        };
        let shard_idx = if let Some(pos) = self.shards.iter().position(|s| s.primary == target) {
            pos
        } else {
            let pos = self.shards.len();
            self.shards.push(ShardRoute {
                primary: target,
                replicas: Vec::new(),
            });
            pos
        };
        self.slots[usize::from(slot)] = Some(shard_idx);
    }

    /// Choose `(endpoint, is_replica)` for `slot`.
    #[must_use]
    pub fn route_for_slot(
        &self,
        slot: u16,
        read_only: bool,
        read_from_replicas: bool,
    ) -> Option<(ClusterNodeEndpoint, bool)> {
        let shard_idx = self.slots.get(usize::from(slot)).copied().flatten()?;
        let shard = self.shards.get(shard_idx)?;
        if read_only && read_from_replicas && !shard.replicas.is_empty() {
            let idx = self.replica_rr.fetch_add(1, Ordering::Relaxed) % shard.replicas.len();
            return Some((shard.replicas[idx].clone(), true));
        }
        Some((shard.primary.clone(), false))
    }

    /// Pick any primary endpoint (for keyless commands or topology refresh).
    #[must_use]
    pub fn any_primary(&self) -> Option<ClusterNodeEndpoint> {
        self.shards.first().map(|s| s.primary.clone())
    }

    /// Return unique primary endpoints across all known shards.
    #[must_use]
    pub fn primary_nodes(&self) -> Vec<ClusterNodeEndpoint> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for shard in &self.shards {
            if seen.insert(shard.primary.clone()) {
                out.push(shard.primary.clone());
            }
        }
        out
    }

    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Return a mapping from slot index to (primary host, primary port).
    #[must_use]
    pub fn slot_map(&self) -> std::collections::HashMap<u16, (String, u16)> {
        let mut map = std::collections::HashMap::new();
        for (slot, shard_opt) in self.slots.iter().enumerate() {
            if let Some(shard_idx) = shard_opt
                && let Some(shard) = self.shards.get(*shard_idx)
            {
                map.insert(
                    slot as u16,
                    (shard.primary.host.clone(), shard.primary.port),
                );
            }
        }
        map
    }
}

struct ParsedShardNode {
    endpoint: ClusterNodeEndpoint,
    role: String,
    health: String,
}

type ParsedShardEntry = (Vec<(u16, u16)>, Vec<ParsedShardNode>);

fn parse_shard_entry(
    entry: &RespValue,
    use_tls: bool,
) -> Result<Option<ParsedShardEntry>, ValkeyError> {
    let mut slots_val: Option<&RespValue> = None;
    let mut nodes_val: Option<&RespValue> = None;

    match entry.unattributed() {
        RespValue::Map(pairs) => {
            for (k, v) in pairs {
                if let Some(name) = k.as_str() {
                    if name.eq_ignore_ascii_case("slots") {
                        slots_val = Some(v);
                    } else if name.eq_ignore_ascii_case("nodes") {
                        nodes_val = Some(v);
                    }
                }
            }
        }
        RespValue::Array(flat) => {
            for [k, v] in flat.as_chunks::<2>().0 {
                if let Some(name) = k.as_str() {
                    if name.eq_ignore_ascii_case("slots") {
                        slots_val = Some(v);
                    } else if name.eq_ignore_ascii_case("nodes") {
                        nodes_val = Some(v);
                    }
                }
            }
        }
        _ => return Ok(None),
    }

    let Some(slots_arr) = slots_val.and_then(RespValue::as_slice) else {
        return Ok(None);
    };
    let mut slot_pairs = Vec::new();
    for [s_val, e_val] in slots_arr.as_chunks::<2>().0 {
        if let (Some(s), Some(e)) = (s_val.as_i64(), e_val.as_i64())
            && let (Ok(s16), Ok(e16)) = (u16::try_from(s), u16::try_from(e))
        {
            slot_pairs.push((s16, e16));
        }
    }

    let Some(nodes_arr) = nodes_val.and_then(RespValue::as_slice) else {
        return Ok(None);
    };
    let mut parsed_nodes = Vec::with_capacity(nodes_arr.len());
    for node_val in nodes_arr {
        if let Some(n) = parse_shard_node(node_val, use_tls) {
            parsed_nodes.push(n);
        }
    }
    Ok(Some((slot_pairs, parsed_nodes)))
}

fn parse_shard_node(val: &RespValue, use_tls: bool) -> Option<ParsedShardNode> {
    let mut endpoint_str: Option<String> = None;
    let mut ip_str: Option<String> = None;
    let mut port_val: Option<u16> = None;
    let mut tls_port_val: Option<u16> = None;
    let mut role = String::new();
    let mut health = "online".to_string();

    let mut visit = |k: &RespValue, v: &RespValue| {
        let Some(key_name) = k.as_str() else {
            return;
        };
        match key_name.to_ascii_lowercase().as_str() {
            "endpoint" => {
                if let Some(s) = v.as_str()
                    && !s.is_empty()
                {
                    endpoint_str = Some(s.into_owned());
                }
            }
            "ip" => {
                if let Some(s) = v.as_str()
                    && !s.is_empty()
                {
                    ip_str = Some(s.into_owned());
                }
            }
            "port" => {
                if let Some(p) = v.as_i64().and_then(|n| u16::try_from(n).ok())
                    && p > 0
                {
                    port_val = Some(p);
                }
            }
            "tls-port" => {
                if let Some(p) = v.as_i64().and_then(|n| u16::try_from(n).ok())
                    && p > 0
                {
                    tls_port_val = Some(p);
                }
            }
            "role" => {
                if let Some(s) = v.as_str() {
                    role = s.to_ascii_lowercase();
                }
            }
            "health" => {
                if let Some(s) = v.as_str() {
                    health = s.to_ascii_lowercase();
                }
            }
            _ => {}
        }
    };

    match val.unattributed() {
        RespValue::Map(pairs) => {
            for (k, v) in pairs {
                visit(k, v);
            }
        }
        RespValue::Array(flat) => {
            for [k, v] in flat.as_chunks::<2>().0 {
                visit(k, v);
            }
        }
        _ => return None,
    }

    let host = endpoint_str.or(ip_str)?;
    let port = if use_tls {
        tls_port_val.or(port_val)?
    } else {
        port_val.or(tls_port_val)?
    };
    Some(ParsedShardNode {
        endpoint: ClusterNodeEndpoint { host, port },
        role,
        health,
    })
}

fn parse_slots_node(val: &RespValue) -> Option<ClusterNodeEndpoint> {
    let items = val.as_slice()?;
    if items.len() < 2 {
        return None;
    }
    let host = items[0].as_str()?.into_owned();
    if host.is_empty() {
        return None;
    }
    let port = items[1].as_i64().and_then(|n| u16::try_from(n).ok())?;
    if port == 0 {
        return None;
    }
    Some(ClusterNodeEndpoint { host, port })
}

/// Validate that all keys in a multi-key command resolve to the exact same Valkey hash slot.
pub fn validate_cluster_keys_same_slot(keys: &[&[u8]]) -> Result<Option<u16>, ValkeyError> {
    let Some(first) = keys.first() else {
        return Ok(None);
    };
    let slot0 = valkey_hash_slot(first);
    for key in &keys[1..] {
        let slot = valkey_hash_slot(key);
        if slot != slot0 {
            return Err(ValkeyError::programming(format!(
                "CROSSSLOT Keys in request don't hash to the same slot ({slot0} != {slot})"
            )));
        }
    }
    Ok(Some(slot0))
}

/// Determine whether a Valkey command can safely be dispatched to a `READONLY` replica.
#[must_use]
pub fn is_read_only_command(cmd: &[u8]) -> bool {
    let upper = String::from_utf8_lossy(cmd).to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "GET"
            | "MGET"
            | "EXISTS"
            | "TTL"
            | "PTTL"
            | "STRLEN"
            | "HGET"
            | "HGETALL"
            | "HMGET"
            | "HTTL"
            | "HPTTL"
            | "HEXISTS"
            | "HLEN"
            | "ZRANGE"
            | "ZRANGEBYSCORE"
            | "ZSCORE"
            | "XRANGE"
            | "XREVRANGE"
            | "XLEN"
            | "LRANGE"
            | "LLEN"
            | "SMEMBERS"
            | "SISMEMBER"
            | "SCARD"
    )
}
