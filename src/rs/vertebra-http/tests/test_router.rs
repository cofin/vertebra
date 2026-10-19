//! Unit tests for the native router.

use vertebra_http::{
    route_table::{RouteTable, handler_id},
    router::Router,
};

fn build_router(routes: &[(&str, &str)]) -> Router {
    let mut table = RouteTable::new();
    for (method, path) in routes {
        table.insert(method, path);
    }
    Router::from_entries(table.into_entries()).unwrap()
}

#[test]
fn test_static_route() {
    let router = build_router(&[("GET", "/hello")]);
    let m = router.lookup("GET", "/hello").unwrap();
    assert_eq!(m.handler_id, handler_id("GET", "/hello"));
}

#[test]
fn test_parametric_route() {
    let router = build_router(&[("GET", "/users/:id")]);
    let m = router.lookup("GET", "/users/42").unwrap();
    assert_eq!(m.params.get("id"), Some("42"));
}

#[test]
fn test_method_not_matched() {
    let router = build_router(&[("GET", "/hello")]);
    assert!(router.lookup("POST", "/hello").is_err());
}

#[test]
fn test_unknown_path() {
    let router = build_router(&[("GET", "/hello")]);
    assert!(router.lookup("GET", "/world").is_err());
}

#[test]
fn test_handler_id_stability() {
    let id1 = handler_id("GET", "/api/v1/users");
    let id2 = handler_id("GET", "/api/v1/users");
    assert_eq!(id1, id2);
}

#[test]
fn test_handler_id_method_sensitivity() {
    let get_id = handler_id("GET", "/resource");
    let post_id = handler_id("POST", "/resource");
    assert_ne!(get_id, post_id);
}

#[test]
fn test_case_insensitive_method_lookup() {
    let router = build_router(&[("GET", "/hello")]);
    let m = router.lookup("get", "/hello").unwrap();
    assert_eq!(m.handler_id, handler_id("GET", "/hello"));
}

#[test]
fn test_multi_segment_param() {
    let router = build_router(&[("DELETE", "/orgs/:org/repos/:repo")]);
    let m = router.lookup("DELETE", "/orgs/acme/repos/widget").unwrap();
    assert_eq!(m.params.get("org"), Some("acme"));
    assert_eq!(m.params.get("repo"), Some("widget"));
}
