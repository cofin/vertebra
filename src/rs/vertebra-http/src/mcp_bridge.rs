//! Native Rust stdio-to-HTTP MCP bridge (`vtb mcp bridge`) and JSON-RPC 2.0 header extractor.

use std::io::{BufRead, BufReader, Read, Write};

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

#[derive(Debug, Clone)]
pub struct McpBridgeConfig {
    pub endpoint_url: String,
    pub bearer_token: Option<String>,
}

pub struct McpStdioBridge {
    config: McpBridgeConfig,
}

impl McpStdioBridge {
    pub fn new(config: McpBridgeConfig) -> Self {
        Self { config }
    }

    pub async fn run_with_responder<R, W, F>(
        &self,
        reader: R,
        mut writer: W,
        mut responder: F,
    ) -> Result<(), std::io::Error>
    where
        R: Read,
        W: Write,
        F: FnMut(&str, &McpObservabilityHeaders, Option<String>) -> Result<String, std::io::Error>,
    {
        let buf_reader = BufReader::new(reader);
        let auth_header = self
            .config
            .bearer_token
            .as_ref()
            .map(|tok| format!("Bearer {tok}"));

        for line_res in buf_reader.lines() {
            let line = line_res?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let obs = extract_mcp_observability_headers(trimmed.as_bytes()).unwrap_or_default();
            let response_line = responder(trimmed, &obs, auth_header.clone())?;
            if !response_line.is_empty() {
                writeln!(writer, "{response_line}")?;
                writer.flush()?;
            }
        }
        Ok(())
    }
}
