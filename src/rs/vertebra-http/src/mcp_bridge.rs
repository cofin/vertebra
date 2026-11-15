//! MCP JSON-RPC 2.0 observability header extractor.

use serde_json::Value;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct McpObservabilityHeaders {
    pub mcp_method: Option<String>,
    pub mcp_name: Option<String>,
}

pub fn extract_mcp_observability_headers(payload: &[u8]) -> Option<McpObservabilityHeaders> {
    let parsed: Value = serde_json::from_slice(payload).ok()?;
    let mcp_method = parsed
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mcp_name = parsed
        .get("params")
        .and_then(|p| p.get("name"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    Some(McpObservabilityHeaders {
        mcp_method,
        mcp_name,
    })
}
