use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;

/// Opcode identifiers for state deltas.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateOpcode {
    FieldSet = 0x01,
    FieldDelete = 0x02,
    ObjectReplace = 0x03,
    SyncRequest = 0x04,
    SyncResponse = 0x05,
    ActionCall = 0x06,
    ActionResult = 0x07,
    BatchPatch = 0x08,
}

impl TryFrom<u8> for StateOpcode {
    type Error = StateDeltaError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::FieldSet),
            0x02 => Ok(Self::FieldDelete),
            0x03 => Ok(Self::ObjectReplace),
            0x04 => Ok(Self::SyncRequest),
            0x05 => Ok(Self::SyncResponse),
            0x06 => Ok(Self::ActionCall),
            0x07 => Ok(Self::ActionResult),
            0x08 => Ok(Self::BatchPatch),
            _ => Err(StateDeltaError::InvalidOpcode(value)),
        }
    }
}

impl From<StateOpcode> for u8 {
    fn from(value: StateOpcode) -> Self {
        value as u8
    }
}

/// 64-bit Hybrid Logical Clock (HLC) combining a 48-bit physical millisecond clock and 16-bit logical counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HybridLogicalClock {
    pub wall_ms: u64,
    pub logical: u16,
}

impl HybridLogicalClock {
    pub fn new(wall_ms: u64, logical: u16) -> Self {
        Self {
            wall_ms: wall_ms & 0x0000_FFFF_FFFF_FFFF,
            logical,
        }
    }

    pub fn pack(self) -> u64 {
        ((self.wall_ms & 0x0000_FFFF_FFFF_FFFF) << 16) | u64::from(self.logical)
    }

    pub fn unpack(packed: u64) -> Self {
        Self {
            wall_ms: packed >> 16,
            logical: (packed & 0xFFFF) as u16,
        }
    }

    pub fn tick(&mut self, now_ms: u64) -> u64 {
        let clamped_now = now_ms & 0x0000_FFFF_FFFF_FFFF;
        if clamped_now > self.wall_ms {
            self.wall_ms = clamped_now;
            self.logical = 0;
        } else {
            self.logical = self.logical.saturating_add(1);
        }
        self.pack()
    }

    pub fn receive(&mut self, remote_packed: u64, now_ms: u64) -> u64 {
        let remote = Self::unpack(remote_packed);
        let clamped_now = now_ms & 0x0000_FFFF_FFFF_FFFF;
        if clamped_now > self.wall_ms && clamped_now > remote.wall_ms {
            self.wall_ms = clamped_now;
            self.logical = 0;
        } else if self.wall_ms == remote.wall_ms {
            self.logical = self.logical.max(remote.logical).saturating_add(1);
        } else if remote.wall_ms > self.wall_ms {
            self.wall_ms = remote.wall_ms;
            self.logical = remote.logical.saturating_add(1);
        } else {
            self.logical = self.logical.saturating_add(1);
        }
        self.pack()
    }
}

/// Last-Write-Wins (LWW) state register keyed by 32-bit FNV-1a field ID.
#[derive(Debug, Clone, Default)]
pub struct LwwStateRegister {
    pub state_id: u64,
    fields: BTreeMap<u32, (u64, Value)>,
    tombstones: BTreeMap<u32, u64>,
}

impl LwwStateRegister {
    pub fn new(state_id: u64) -> Self {
        Self {
            state_id,
            fields: BTreeMap::new(),
            tombstones: BTreeMap::new(),
        }
    }

    pub fn apply(&mut self, delta: &StateDelta) -> bool {
        match delta {
            StateDelta::FieldSet {
                state_id,
                field_id,
                timestamp_ns,
                value,
            }
            | StateDelta::ObjectReplace {
                state_id,
                field_id,
                timestamp_ns,
                value,
            } => {
                if *state_id != self.state_id {
                    return false;
                }
                let latest_ts = self
                    .fields
                    .get(field_id)
                    .map(|(ts, _)| *ts)
                    .max(self.tombstones.get(field_id).copied())
                    .unwrap_or(0);
                if *timestamp_ns >= latest_ts {
                    self.tombstones.remove(field_id);
                    self.fields
                        .insert(*field_id, (*timestamp_ns, value.clone()));
                    true
                } else {
                    false
                }
            }
            StateDelta::FieldDelete {
                state_id,
                field_id,
                timestamp_ns,
            } => {
                if *state_id != self.state_id {
                    return false;
                }
                let latest_ts = self
                    .fields
                    .get(field_id)
                    .map(|(ts, _)| *ts)
                    .max(self.tombstones.get(field_id).copied())
                    .unwrap_or(0);
                if *timestamp_ns >= latest_ts {
                    self.fields.remove(field_id);
                    self.tombstones.insert(*field_id, *timestamp_ns);
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    pub fn get(&self, field_id: u32) -> Option<&Value> {
        self.fields.get(&field_id).map(|(_, v)| v)
    }
}

/// Decoded state delta message.
#[derive(Debug, Clone, PartialEq)]
pub enum StateDelta {
    FieldSet {
        state_id: u64,
        field_id: u32,
        timestamp_ns: u64,
        value: Value,
    },
    FieldDelete {
        state_id: u64,
        field_id: u32,
        timestamp_ns: u64,
    },
    ObjectReplace {
        state_id: u64,
        field_id: u32,
        timestamp_ns: u64,
        value: Value,
    },
    SyncRequest {
        state_id: u64,
    },
    SyncResponse {
        state_id: u64,
        state: Value,
    },
    ActionCall {
        state_id: u64,
        action_id: u32,
        call_id: u64,
        args: Value,
    },
    ActionResult {
        state_id: u64,
        call_id: u64,
        result: Value,
    },
    BatchPatch {
        state_id: u64,
        timestamp_ns: u64,
        patches: Value,
    },
}

#[derive(Debug, Error)]
pub enum StateDeltaError {
    #[error("failed to decode state delta: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("invalid state delta payload: {0}")]
    InvalidWire(&'static str),
    #[error("invalid opcode: {0}")]
    InvalidOpcode(u8),
    #[error("invalid {0} value")]
    InvalidValue(&'static str),
}

/// Return the deterministic 64-bit state ID for a state class name.
pub fn state_id(name: &str) -> u64 {
    const OFFSET: u64 = 0xCBF29CE484222325;
    const PRIME: u64 = 0x100000001B3;
    let mut hash = OFFSET;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Return the deterministic 32-bit field ID for a state field name.
pub fn field_id(name: &str) -> u32 {
    const OFFSET: u32 = 0x811C9DC5;
    const PRIME: u32 = 0x01000193;
    let mut hash = OFFSET;
    for byte in name.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn json_u64(value: u64) -> Value {
    Value::Number(serde_json::Number::from(value))
}

fn value_to_u64(value: &Value) -> Result<u64, StateDeltaError> {
    value.as_u64().ok_or(StateDeltaError::InvalidValue("u64"))
}

fn value_to_u32(value: &Value) -> Result<u32, StateDeltaError> {
    let number = value.as_u64().ok_or(StateDeltaError::InvalidValue("u32"))?;
    u32::try_from(number).map_err(|_| StateDeltaError::InvalidValue("u32"))
}

/// Encode a state delta into MessagePack bytes.
pub fn encode_delta(delta: &StateDelta) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let payload = match delta {
        StateDelta::FieldSet {
            state_id,
            field_id,
            timestamp_ns,
            value,
        } => vec![
            json_u64(u64::from(StateOpcode::FieldSet as u8)),
            json_u64(*state_id),
            json_u64(u64::from(*field_id)),
            json_u64(*timestamp_ns),
            value.clone(),
        ],
        StateDelta::FieldDelete {
            state_id,
            field_id,
            timestamp_ns,
        } => vec![
            json_u64(u64::from(StateOpcode::FieldDelete as u8)),
            json_u64(*state_id),
            json_u64(u64::from(*field_id)),
            json_u64(*timestamp_ns),
        ],
        StateDelta::ObjectReplace {
            state_id,
            field_id,
            timestamp_ns,
            value,
        } => vec![
            json_u64(u64::from(StateOpcode::ObjectReplace as u8)),
            json_u64(*state_id),
            json_u64(u64::from(*field_id)),
            json_u64(*timestamp_ns),
            value.clone(),
        ],
        StateDelta::SyncRequest { state_id } => vec![
            json_u64(u64::from(StateOpcode::SyncRequest as u8)),
            json_u64(*state_id),
        ],
        StateDelta::SyncResponse { state_id, state } => vec![
            json_u64(u64::from(StateOpcode::SyncResponse as u8)),
            json_u64(*state_id),
            state.clone(),
        ],
        StateDelta::ActionCall {
            state_id,
            action_id,
            call_id,
            args,
        } => vec![
            json_u64(u64::from(StateOpcode::ActionCall as u8)),
            json_u64(*state_id),
            json_u64(u64::from(*action_id)),
            json_u64(*call_id),
            args.clone(),
        ],
        StateDelta::ActionResult {
            state_id,
            call_id,
            result,
        } => vec![
            json_u64(u64::from(StateOpcode::ActionResult as u8)),
            json_u64(*state_id),
            json_u64(*call_id),
            result.clone(),
        ],
        StateDelta::BatchPatch {
            state_id,
            timestamp_ns,
            patches,
        } => vec![
            json_u64(u64::from(StateOpcode::BatchPatch as u8)),
            json_u64(*state_id),
            json_u64(*timestamp_ns),
            patches.clone(),
        ],
    };
    rmp_serde::to_vec(&payload)
}

/// Decode MessagePack bytes into a state delta.
pub fn decode_delta(data: &[u8]) -> Result<StateDelta, StateDeltaError> {
    let raw: Vec<Value> = rmp_serde::from_slice(data)?;
    if raw.len() < 2 {
        return Err(StateDeltaError::InvalidWire(
            "delta must have at least 2 elements",
        ));
    }

    let opcode_value = value_to_u64(&raw[0])?;
    let opcode = StateOpcode::try_from(opcode_value as u8)?;
    let state_id = value_to_u64(&raw[1])?;

    match opcode {
        StateOpcode::FieldSet | StateOpcode::ObjectReplace => {
            if raw.len() != 5 {
                return Err(StateDeltaError::InvalidWire(
                    "field update delta must have 5 elements",
                ));
            }
            let field_id = value_to_u32(&raw[2])?;
            let timestamp_ns = value_to_u64(&raw[3])?;
            let value = raw[4].clone();
            if opcode == StateOpcode::FieldSet {
                Ok(StateDelta::FieldSet {
                    state_id,
                    field_id,
                    timestamp_ns,
                    value,
                })
            } else {
                Ok(StateDelta::ObjectReplace {
                    state_id,
                    field_id,
                    timestamp_ns,
                    value,
                })
            }
        }
        StateOpcode::FieldDelete => {
            if raw.len() != 4 {
                return Err(StateDeltaError::InvalidWire(
                    "field delete delta must have 4 elements",
                ));
            }
            Ok(StateDelta::FieldDelete {
                state_id,
                field_id: value_to_u32(&raw[2])?,
                timestamp_ns: value_to_u64(&raw[3])?,
            })
        }
        StateOpcode::SyncRequest => {
            if raw.len() != 2 {
                return Err(StateDeltaError::InvalidWire(
                    "sync request delta must have 2 elements",
                ));
            }
            Ok(StateDelta::SyncRequest { state_id })
        }
        StateOpcode::SyncResponse => {
            if raw.len() != 3 {
                return Err(StateDeltaError::InvalidWire(
                    "sync response delta must have 3 elements",
                ));
            }
            Ok(StateDelta::SyncResponse {
                state_id,
                state: raw[2].clone(),
            })
        }
        StateOpcode::ActionCall => {
            if raw.len() != 5 {
                return Err(StateDeltaError::InvalidWire(
                    "action call delta must have 5 elements",
                ));
            }
            Ok(StateDelta::ActionCall {
                state_id,
                action_id: value_to_u32(&raw[2])?,
                call_id: value_to_u64(&raw[3])?,
                args: raw[4].clone(),
            })
        }
        StateOpcode::ActionResult => {
            if raw.len() != 4 {
                return Err(StateDeltaError::InvalidWire(
                    "action result delta must have 4 elements",
                ));
            }
            Ok(StateDelta::ActionResult {
                state_id,
                call_id: value_to_u64(&raw[2])?,
                result: raw[3].clone(),
            })
        }
        StateOpcode::BatchPatch => {
            if raw.len() != 4 {
                return Err(StateDeltaError::InvalidWire(
                    "batch patch delta must have 4 elements",
                ));
            }
            Ok(StateDelta::BatchPatch {
                state_id,
                timestamp_ns: value_to_u64(&raw[2])?,
                patches: raw[3].clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn state_id_hashing() {
        assert_eq!(state_id("GameState"), 0x374157F8DDD4C9A8);
        assert_eq!(field_id("score"), 0xD22F9095);
    }

    #[test]
    fn encode_decode_field_set() {
        let delta = StateDelta::FieldSet {
            state_id: state_id("GameState"),
            field_id: field_id("score"),
            timestamp_ns: 1234,
            value: json!({ "count": 1 }),
        };

        let encoded = encode_delta(&delta).expect("encode");
        let decoded = decode_delta(&encoded).expect("decode");
        assert_eq!(decoded, delta);
    }

    #[test]
    fn encode_decode_field_delete() {
        let delta = StateDelta::FieldDelete {
            state_id: state_id("GameState"),
            field_id: field_id("score"),
            timestamp_ns: 999,
        };

        let encoded = encode_delta(&delta).expect("encode");
        let decoded = decode_delta(&encoded).expect("decode");
        assert_eq!(decoded, delta);
    }

    #[test]
    fn hlc_and_lww_conflict_resolution() {
        let mut hlc = HybridLogicalClock::new(1000, 0);
        let ts1 = hlc.tick(1000);
        let ts2 = hlc.tick(1000);
        assert!(ts2 > ts1);

        let sid = state_id("CartState");
        let fid = field_id("quantity");
        let mut reg = LwwStateRegister::new(sid);

        let newer = StateDelta::FieldSet {
            state_id: sid,
            field_id: fid,
            timestamp_ns: ts2,
            value: json!(5),
        };
        let older = StateDelta::FieldSet {
            state_id: sid,
            field_id: fid,
            timestamp_ns: ts1,
            value: json!(2),
        };
        assert!(reg.apply(&newer));
        assert!(!reg.apply(&older));
        assert_eq!(reg.get(fid), Some(&json!(5)));
    }
}
