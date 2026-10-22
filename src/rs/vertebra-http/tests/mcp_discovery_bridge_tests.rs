use bytes::Bytes;
use http::StatusCode;
use vertebra_http::discovery::{
    WellKnownDiscoveryCache, is_excluded_from_spa_fallback, is_excluded_from_spa_fallback_custom,
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
fn test_well_known_discovery_cache_custom_version() {
    let cache = WellKnownDiscoveryCache::new();
    cache.register_mcp_manifest_with_version("2026-10-01", Bytes::from_static(br#"{"v":"next"}"#));
    let resp = cache
        .serve("/.well-known/mcp.json", None)
        .expect("mcp manifest with custom version");
    assert_eq!(resp.status, StatusCode::OK);
    assert_eq!(
        resp.headers.get("mcp-protocol-version").unwrap(),
        "2026-10-01"
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

    let custom = ["/custom-api", "/internal"];
    assert!(is_excluded_from_spa_fallback_custom(
        "/custom-api/v1",
        &custom
    ));
    assert!(is_excluded_from_spa_fallback_custom("/internal", &custom));
    assert!(!is_excluded_from_spa_fallback_custom("/public", &custom));
}
