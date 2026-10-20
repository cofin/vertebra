use bytes::Bytes;
use http::StatusCode;
use vertebra_http::discovery::{WellKnownDiscoveryCache, is_excluded_from_spa_fallback};
use vertebra_http::mcp_bridge::{
    McpBridgeConfig, McpStdioBridge, extract_mcp_observability_headers,
};

#[test]
fn test_well_known_discovery_cache_etag_and_304() {
    let cache = WellKnownDiscoveryCache::new();
    cache.register_agent_card(Bytes::from_static(
        br#"{"name":"vertebra-agent","version":"1.0"}"#,
    ));
    cache.register_mcp_manifest(Bytes::from_static(br#"{"protocolVersion":"2026-07-28"}"#));
    cache.register_oauth_protected_resource(Bytes::from_static(
        br#"{"resource":"https://api.example.com/mcp"}"#,
    ));

    let resp_200 = cache
        .serve("/.well-known/agent-card.json", None)
        .expect("agent card should be cached");
    assert_eq!(resp_200.status, StatusCode::OK);
    assert_eq!(resp_200.headers.get("a2a-version").unwrap(), "1.0");
    let etag = resp_200
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let resp_304 = cache
        .serve("/.well-known/agent-card.json", Some(&etag))
        .expect("agent card 304");
    assert_eq!(resp_304.status, StatusCode::NOT_MODIFIED);
    assert!(resp_304.body.is_empty());

    let mcp_200 = cache
        .serve("/.well-known/mcp.json", None)
        .expect("mcp manifest should be cached");
    assert_eq!(
        mcp_200.headers.get("mcp-protocol-version").unwrap(),
        "2026-07-28"
    );
}

#[test]
fn test_spa_fallback_exclusions_for_agent_routes() {
    assert!(is_excluded_from_spa_fallback(
        "/.well-known/agent-card.json"
    ));
    assert!(is_excluded_from_spa_fallback("/.well-known/mcp.json"));
    assert!(is_excluded_from_spa_fallback("/mcp"));
    assert!(is_excluded_from_spa_fallback("/mcp/messages"));
    assert!(is_excluded_from_spa_fallback("/a2a"));
    assert!(is_excluded_from_spa_fallback("/a2a/tasks"));
    assert!(!is_excluded_from_spa_fallback("/dashboard/settings"));
}

#[test]
fn test_extract_mcp_observability_headers() {
    let payload = br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query_schema","arguments":{"table":"orders"}}}"#;
    let obs = extract_mcp_observability_headers(payload).expect("valid jsonrpc");
    assert_eq!(obs.mcp_method.as_deref(), Some("tools/call"));
    assert_eq!(obs.mcp_name.as_deref(), Some("query_schema"));
}

#[tokio::test]
async fn test_mcp_stdio_bridge_roundtrip_over_in_memory_streams() {
    let input =
        b"{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"tools/call\",\"params\":{\"name\":\"echo\"}}\n";
    let config = McpBridgeConfig {
        endpoint_url: "http://127.0.0.1:0/mcp".to_string(),
        bearer_token: Some("tok_test".to_string()),
    };
    let bridge = McpStdioBridge::new(config);
    let mut output = Vec::new();

    bridge
        .run_with_responder(&input[..], &mut output, |req_line, obs, auth| {
            assert!(req_line.contains("\"tools/call\""));
            assert_eq!(obs.mcp_method.as_deref(), Some("tools/call"));
            assert_eq!(obs.mcp_name.as_deref(), Some("echo"));
            assert_eq!(auth.as_deref(), Some("Bearer tok_test"));
            Ok(r#"{"jsonrpc":"2.0","id":7,"result":{"ok":true}}"#.to_string())
        })
        .await
        .expect("bridge roundtrip should succeed");

    let out_str = String::from_utf8(output).unwrap();
    assert_eq!(
        out_str.trim(),
        r#"{"jsonrpc":"2.0","id":7,"result":{"ok":true}}"#
    );
}
