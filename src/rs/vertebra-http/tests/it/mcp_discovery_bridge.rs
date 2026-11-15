use bytes::Bytes;
use http::StatusCode;
use vertebra_http::discovery::{WellKnownDiscoveryCache, is_excluded_from_spa_fallback};
use vertebra_http::mcp_bridge::extract_mcp_observability_headers;

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

    let notification = br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    let obs_notif = extract_mcp_observability_headers(notification).expect("valid json");
    assert_eq!(
        obs_notif.mcp_method.as_deref(),
        Some("notifications/initialized")
    );
    assert_eq!(obs_notif.mcp_name, None);

    assert_eq!(extract_mcp_observability_headers(b"{invalid"), None);
}
