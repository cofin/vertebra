use serde::{Deserialize, Serialize};

use crate::error::BrokerError;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerRequest {
    Acquire { pool: String },
    Release { pool: String, token: String },
    Health { pool: String },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerResponse {
    Acquired { token: String },
    Released { ok: bool },
    Health { ok: bool },
    Error { message: String },
}

pub fn encode_response(codec: &str, response: &BrokerResponse) -> Result<Vec<u8>, BrokerError> {
    match codec {
        "bincode" => bincode::serialize(response).map_err(|err| BrokerError::Ipc(err.to_string())),
        "msgpack" => rmp_serde::to_vec(response).map_err(|err| BrokerError::Ipc(err.to_string())),
        other => Err(BrokerError::Ipc(format!("unsupported codec: {other}"))),
    }
}

pub fn decode_request(codec: &str, payload: &[u8]) -> Result<BrokerRequest, BrokerError> {
    match codec {
        "bincode" => bincode::deserialize(payload).map_err(|err| BrokerError::Ipc(err.to_string())),
        "msgpack" => rmp_serde::from_slice(payload).map_err(|err| BrokerError::Ipc(err.to_string())),
        other => Err(BrokerError::Ipc(format!("unsupported codec: {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_request, encode_response, BrokerRequest, BrokerResponse};

    #[test]
    fn roundtrip_response() {
        let response = BrokerResponse::Health { ok: true };
        let encoded = encode_response("bincode", &response).expect("encode");
        let decoded: BrokerResponse = bincode::deserialize(&encoded).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn decode_request_payload() {
        let request = BrokerRequest::Acquire {
            pool: "default".into(),
        };
        let payload = bincode::serialize(&request).expect("serialize");
        let decoded = decode_request("bincode", &payload).expect("decode");
        assert_eq!(decoded, request);
    }
}
