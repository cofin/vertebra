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
            _ => Err(StateDeltaError::InvalidOpcode(value)),
        }
    }
}

impl From<StateOpcode> for u8 {
    fn from(value: StateOpcode) -> Self {
        value as u8
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
}
