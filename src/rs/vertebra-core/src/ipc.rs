use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum Opcode {
    /// Standard ASGI HTTP request
    AsgiHttp = 1,
    /// Standard ASGI WebSocket connection
    AsgiWebsocket = 2,
    /// Direct optimized HTTP request (bypasses ASGI)
    DirectHttp = 3,
    /// Direct optimized WebSocket open
    WsOpen = 4,
    /// Direct optimized WebSocket message
    WsMessage = 5,
    /// Direct optimized WebSocket close
    WsClose = 6,
    /// Server-Sent Events data
    SseData = 7,
}

impl From<u32> for Opcode {
    fn from(value: u32) -> Self {
        match value {
            1 => Opcode::AsgiHttp,
            2 => Opcode::AsgiWebsocket,
            3 => Opcode::DirectHttp,
            4 => Opcode::WsOpen,
            5 => Opcode::WsMessage,
            6 => Opcode::WsClose,
            7 => Opcode::SseData,
            _ => Opcode::AsgiHttp, // Default/fallback
        }
    }
}
