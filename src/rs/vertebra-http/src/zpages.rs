//! Opt-in z-pages diagnostic endpoints (`zpages.rs`).

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method, Response, StatusCode, Uri};
use http_body_util::Full;
use serde_json::{Map, Value, json};
use vertebra_core::util::constant_time_eq;

/// Maximum time to wait for a worker z-page response before timing out.
pub const ZPAGES_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Exact HTTP paths intercepted by the opt-in z-pages endpoint.
pub const ZPAGES_PATHS: [&str; 7] = [
    "/taskz",
    "/debug/zpages/tasks",
    "/statusz",
    "/varz",
    "/rpcz",
    "/brokerz",
    "/debug/zpages/broker",
];

/// Configuration for opt-in z-pages diagnostic endpoints.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZPagesConfig {
    /// Whether z-pages endpoints are active.
    pub enabled: bool,
    /// Optional `Authorization: Bearer <token>` secret. When `None`, only loopback binds are authorized.
    pub bearer_token: Option<String>,
}

/// Validated z-page request returned by [`ZPagesEndpoint::preflight`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZPagesRequest {
    pub path: &'static str,
    pub is_head: bool,
    pub target_worker: Option<usize>,
}

/// Per-worker response outcome collected during SHM z-pages fan-out.
#[derive(Debug, Clone)]
pub enum WorkerZPageOutcome {
    Ok { worker_id: usize, payload: Value },
    Timeout { worker_id: usize },
    Error { worker_id: usize, message: String },
}

/// Authorization, method, and query preflight handler for z-pages endpoints.
pub struct ZPagesEndpoint {
    config: ZPagesConfig,
    bind_is_loopback: bool,
}

/// Return `true` only for the seven exact z-page paths in [`ZPAGES_PATHS`].
#[inline]
pub fn is_zpages_path(path: &str) -> bool {
    canonical_zpages_path(path).is_some()
}

/// Map an exact z-page path to its `'static` canonical slice.
#[inline]
pub fn canonical_zpages_path(path: &str) -> Option<&'static str> {
    match path {
        "/taskz" => Some("/taskz"),
        "/debug/zpages/tasks" => Some("/debug/zpages/tasks"),
        "/statusz" => Some("/statusz"),
        "/varz" => Some("/varz"),
        "/rpcz" => Some("/rpcz"),
        "/brokerz" => Some("/brokerz"),
        "/debug/zpages/broker" => Some("/debug/zpages/broker"),
        _ => None,
    }
}

/// Parse an optional `worker=<usize>` query parameter from a raw query string.
pub fn parse_worker_query(query: Option<&str>) -> Result<Option<usize>, &'static str> {
    let Some(query) = query else {
        return Ok(None);
    };
    let mut selected = None;
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (pair, None),
        };
        if key != "worker" {
            continue;
        }
        let Some(value) = value else {
            return Err("invalid_worker_id");
        };
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err("invalid_worker_id");
        }
        let worker_id = value.parse::<usize>().map_err(|_| "invalid_worker_id")?;
        selected = Some(worker_id);
    }
    Ok(selected)
}

impl ZPagesEndpoint {
    /// Create a new z-pages endpoint gate for the given configuration and bind locality.
    pub fn new(config: ZPagesConfig, bind_is_loopback: bool) -> Self {
        Self {
            config,
            bind_is_loopback,
        }
    }

    /// Return whether z-pages interception is enabled.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Verify whether the request is authorized via constant-time Bearer token or loopback bind.
    pub fn is_authorized(&self, headers: &HeaderMap) -> bool {
        if let Some(ref expected) = self.config.bearer_token {
            if expected.is_empty() {
                return false;
            }
            let Some(raw_auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) else {
                return false;
            };
            return raw_auth.strip_prefix("Bearer ").is_some_and(|token| {
                constant_time_eq(token.trim().as_bytes(), expected.as_bytes())
            });
        }
        self.bind_is_loopback
    }

    /// Match, authorize, and validate a z-page HTTP request prior to worker dispatch.
    pub fn preflight(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
    ) -> Option<Result<ZPagesRequest, Response<Full<Bytes>>>> {
        if !self.config.enabled {
            return None;
        }
        let path = canonical_zpages_path(uri.path())?;
        if !self.is_authorized(headers) {
            return Some(Err(json_error_response(
                StatusCode::UNAUTHORIZED,
                b"{\"error\":\"unauthorized\"}",
                Some("Bearer realm=\"vertebra-zpages\""),
                None,
                *method == Method::HEAD,
            )));
        }
        let is_head = match *method {
            Method::GET => false,
            Method::HEAD => true,
            _ => {
                return Some(Err(json_error_response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    b"{\"error\":\"method_not_allowed\"}",
                    None,
                    Some("GET, HEAD"),
                    false,
                )));
            }
        };
        let target_worker = match parse_worker_query(uri.query()) {
            Ok(worker) => worker,
            Err(_) => {
                return Some(Err(json_error_response(
                    StatusCode::BAD_REQUEST,
                    b"{\"error\":\"invalid_worker_id\"}",
                    None,
                    None,
                    is_head,
                )));
            }
        };
        Some(Ok(ZPagesRequest {
            path,
            is_head,
            target_worker,
        }))
    }
}

/// Build a JSON z-pages HTTP response with `no-store` caching and empty body on `HEAD`.
pub fn json_response(status: StatusCode, body: Bytes, is_head: bool) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(Full::new(if is_head { Bytes::new() } else { body }))
        .expect("build zpages json response")
}

/// Build a JSON z-pages error HTTP response with optional `WWW-Authenticate` and `Allow` headers.
pub fn json_error_response(
    status: StatusCode,
    body: &'static [u8],
    www_authenticate: Option<&'static str>,
    allow: Option<&'static str>,
    is_head: bool,
) -> Response<Full<Bytes>> {
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store");
    if let Some(auth_header) = www_authenticate {
        builder = builder.header("www-authenticate", auth_header);
    }
    if let Some(allow_header) = allow {
        builder = builder.header("allow", allow_header);
    }
    let payload = if is_head {
        Bytes::new()
    } else {
        Bytes::from_static(body)
    };
    builder
        .body(Full::new(payload))
        .expect("build zpages error response")
}

/// Merge per-worker SHM z-pages responses into a single JSON response body and status code.
pub fn merge_shm_zpages_responses(
    path: &str,
    outcomes: Vec<WorkerZPageOutcome>,
) -> (StatusCode, Bytes) {
    let is_taskz = matches!(path, "/taskz" | "/debug/zpages/tasks");
    let total_outcomes = outcomes.len();
    let mut succeeded = 0usize;
    let mut timed_out = 0usize;
    let mut workers = Vec::with_capacity(total_outcomes);
    let mut merged_tasks = Vec::new();
    let mut merged_loops = Map::new();
    let mut total_tasks = 0usize;

    for outcome in outcomes {
        match outcome {
            WorkerZPageOutcome::Ok { worker_id, payload } => {
                succeeded += 1;
                if is_taskz {
                    let mut worker_tasks_len = 0usize;
                    if let Some(tasks) = payload.get("tasks").and_then(Value::as_array) {
                        worker_tasks_len = tasks.len();
                        for task in tasks {
                            let mut task_val = task.clone();
                            if let Value::Object(ref mut obj) = task_val {
                                obj.insert("worker_id".to_string(), Value::from(worker_id));
                            }
                            merged_tasks.push(task_val);
                        }
                    }
                    let reported_total = payload
                        .get("total_tasks")
                        .and_then(Value::as_u64)
                        .and_then(|n| usize::try_from(n).ok())
                        .unwrap_or(worker_tasks_len);
                    total_tasks = total_tasks.saturating_add(reported_total);
                    if let Some(loops) = payload.get("loops").and_then(Value::as_object) {
                        for (loop_key, loop_val) in loops {
                            merged_loops
                                .insert(format!("worker.{worker_id}.{loop_key}"), loop_val.clone());
                        }
                    }
                }
                workers.push(json!({
                    "worker_id": worker_id,
                    "status": "ok",
                    "payload": payload,
                }));
            }
            WorkerZPageOutcome::Timeout { worker_id } => {
                timed_out += 1;
                workers.push(json!({
                    "worker_id": worker_id,
                    "status": "timeout",
                    "error": "zpages_timeout",
                }));
            }
            WorkerZPageOutcome::Error { worker_id, message } => {
                workers.push(json!({
                    "worker_id": worker_id,
                    "status": "error",
                    "error": message,
                }));
            }
        }
    }

    if succeeded == 0 {
        let (status, error_code) = if total_outcomes > 0 && timed_out == total_outcomes {
            (StatusCode::GATEWAY_TIMEOUT, "zpages_timeout")
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, "zpages_unavailable")
        };
        let body = serde_json::to_vec(&json!({
            "error": error_code,
            "workers": workers,
        }))
        .unwrap_or_else(|_| b"{\"error\":\"zpages_unavailable\",\"workers\":[]}".to_vec());
        return (status, Bytes::from(body));
    }

    let mut root = Map::new();
    root.insert("status".to_string(), Value::from("ok"));
    root.insert("topology".to_string(), Value::from("shm"));
    root.insert("workers".to_string(), Value::Array(workers));
    if is_taskz {
        root.insert("total_tasks".to_string(), Value::from(total_tasks));
        root.insert("tasks".to_string(), Value::Array(merged_tasks));
        root.insert("loops".to_string(), Value::Object(merged_loops));
    }

    let body = serde_json::to_vec(&Value::Object(root))
        .unwrap_or_else(|_| b"{\"status\":\"ok\",\"topology\":\"shm\",\"workers\":[]}".to_vec());
    (StatusCode::OK, Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
    use http_body_util::BodyExt;

    async fn read_body_bytes(
        resp: http::Response<http_body_util::Full<bytes::Bytes>>,
    ) -> bytes::Bytes {
        resp.into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes()
    }

    #[test]
    fn disabled_endpoint_passes_through() {
        let ep = ZPagesEndpoint::new(ZPagesConfig::default(), true);
        assert!(!ep.is_enabled());
        let uri: Uri = "/taskz".parse().unwrap();
        assert!(
            ep.preflight(&Method::GET, &uri, &HeaderMap::new())
                .is_none()
        );
    }

    #[test]
    fn health_and_unmatched_paths_are_excluded() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let ep = ZPagesEndpoint::new(cfg, true);
        for path in [
            "/healthz",
            "/readyz",
            "/livez",
            "/__vertebra/health",
            "/api/taskz",
            "/taskz/",
        ] {
            assert!(!is_zpages_path(path));
            let uri: Uri = path.parse().unwrap();
            assert!(
                ep.preflight(&Method::GET, &uri, &HeaderMap::new())
                    .is_none()
            );
        }
        for path in ZPAGES_PATHS {
            assert!(is_zpages_path(path));
            assert_eq!(canonical_zpages_path(path), Some(path));
        }
    }

    #[test]
    fn loopback_without_token_is_allowed() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let ep = ZPagesEndpoint::new(cfg, true);
        let uri: Uri = "/taskz".parse().unwrap();
        let res = ep
            .preflight(&Method::GET, &uri, &HeaderMap::new())
            .expect("matched")
            .expect("authorized");
        assert_eq!(
            res,
            ZPagesRequest {
                path: "/taskz",
                is_head: false,
                target_worker: None,
            }
        );
    }

    #[tokio::test]
    async fn non_loopback_without_token_is_denied_even_on_post() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let ep = ZPagesEndpoint::new(cfg, false);
        for method in [Method::GET, Method::POST, Method::HEAD] {
            let uri: Uri = "/taskz".parse().unwrap();
            let resp = ep
                .preflight(&method, &uri, &HeaderMap::new())
                .expect("matched")
                .expect_err("unauthorized");
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(
                resp.headers()
                    .get("www-authenticate")
                    .and_then(|v| v.to_str().ok()),
                Some("Bearer realm=\"vertebra-zpages\"")
            );
            assert_eq!(
                resp.headers()
                    .get("cache-control")
                    .and_then(|v| v.to_str().ok()),
                Some("no-store")
            );
            let body = read_body_bytes(resp).await;
            if method == Method::HEAD {
                assert!(body.is_empty());
            } else {
                assert_eq!(body.as_ref(), b"{\"error\":\"unauthorized\"}");
            }
        }
    }

    #[tokio::test]
    async fn bearer_token_required_even_on_loopback_and_empty_token_fails_closed() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: Some("tok".to_string()),
        };
        let ep = ZPagesEndpoint::new(cfg, true);
        let uri: Uri = "/taskz".parse().unwrap();

        assert!(
            ep.preflight(&Method::GET, &uri, &HeaderMap::new())
                .unwrap()
                .is_err()
        );

        let mut wrong = HeaderMap::new();
        wrong.insert("authorization", HeaderValue::from_static("Bearer wrong"));
        assert!(ep.preflight(&Method::GET, &uri, &wrong).unwrap().is_err());

        let mut basic = HeaderMap::new();
        basic.insert("authorization", HeaderValue::from_static("Basic tok"));
        assert!(ep.preflight(&Method::GET, &uri, &basic).unwrap().is_err());

        let mut valid = HeaderMap::new();
        valid.insert("authorization", HeaderValue::from_static("Bearer tok"));
        assert!(ep.preflight(&Method::GET, &uri, &valid).unwrap().is_ok());

        let empty_cfg = ZPagesConfig {
            enabled: true,
            bearer_token: Some(String::new()),
        };
        let empty_ep = ZPagesEndpoint::new(empty_cfg, true);
        let mut empty_hdr = HeaderMap::new();
        empty_hdr.insert("authorization", HeaderValue::from_static("Bearer "));
        assert!(!empty_ep.is_authorized(&empty_hdr));
        assert!(!empty_ep.is_authorized(&HeaderMap::new()));
    }

    #[tokio::test]
    async fn method_enforcement_allows_get_and_head_only() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let ep = ZPagesEndpoint::new(cfg, true);

        let post_uri: Uri = "/taskz".parse().unwrap();
        let resp = ep
            .preflight(&Method::POST, &post_uri, &HeaderMap::new())
            .expect("matched")
            .expect_err("method not allowed");
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            resp.headers().get("allow").and_then(|v| v.to_str().ok()),
            Some("GET, HEAD")
        );
        let body = read_body_bytes(resp).await;
        assert_eq!(body.as_ref(), b"{\"error\":\"method_not_allowed\"}");

        let head_uri: Uri = "/varz".parse().unwrap();
        let req = ep
            .preflight(&Method::HEAD, &head_uri, &HeaderMap::new())
            .expect("matched")
            .expect("head allowed");
        assert_eq!(
            req,
            ZPagesRequest {
                path: "/varz",
                is_head: true,
                target_worker: None,
            }
        );
    }

    #[tokio::test]
    async fn worker_query_parsing_accepts_digits_and_rejects_invalid() {
        assert_eq!(parse_worker_query(None), Ok(None));
        assert_eq!(parse_worker_query(Some("worker=2")), Ok(Some(2)));
        assert_eq!(parse_worker_query(Some("foo=1&worker=0")), Ok(Some(0)));
        assert_eq!(parse_worker_query(Some("foo=bar")), Ok(None));

        for invalid in ["worker=+1", "worker=abc", "worker=-1", "worker=", "worker"] {
            assert_eq!(
                parse_worker_query(Some(invalid)),
                Err("invalid_worker_id"),
                "expected error for {invalid}"
            );
        }

        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let ep = ZPagesEndpoint::new(cfg, true);
        let uri: Uri = "/taskz?worker=-1".parse().unwrap();
        let resp = ep
            .preflight(&Method::GET, &uri, &HeaderMap::new())
            .expect("matched")
            .expect_err("bad request");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = read_body_bytes(resp).await;
        assert_eq!(body.as_ref(), b"{\"error\":\"invalid_worker_id\"}");
    }

    #[tokio::test]
    async fn json_response_clears_body_on_head() {
        let get_resp = json_response(StatusCode::OK, Bytes::from_static(b"{\"ok\":true}"), false);
        assert_eq!(get_resp.status(), StatusCode::OK);
        assert_eq!(
            get_resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(
            get_resp
                .headers()
                .get("cache-control")
                .and_then(|v| v.to_str().ok()),
            Some("no-store")
        );
        assert_eq!(read_body_bytes(get_resp).await.as_ref(), b"{\"ok\":true}");

        let head_resp = json_response(StatusCode::OK, Bytes::from_static(b"{\"ok\":true}"), true);
        assert_eq!(head_resp.status(), StatusCode::OK);
        assert!(read_body_bytes(head_resp).await.is_empty());
    }

    #[test]
    fn merge_shm_taskz_partial_timeout() {
        let outcomes = vec![
            WorkerZPageOutcome::Ok {
                worker_id: 0,
                payload: serde_json::json!({
                    "total_tasks": 2,
                    "tasks": [
                        {"id": 1, "name": "t1"},
                        {"id": 2, "name": "t2"}
                    ],
                    "loops": {
                        "worker.shard.0": {"running": true}
                    }
                }),
            },
            WorkerZPageOutcome::Timeout { worker_id: 1 },
        ];
        let (status, body) = merge_shm_zpages_responses("/taskz", outcomes);
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        assert_eq!(json["status"], "ok");
        assert_eq!(json["topology"], "shm");
        assert_eq!(json["total_tasks"], 2);
        assert_eq!(json["tasks"].as_array().unwrap().len(), 2);
        assert_eq!(json["tasks"][0]["worker_id"], 0);
        assert_eq!(json["loops"]["worker.0.worker.shard.0"]["running"], true);
        let workers = json["workers"].as_array().unwrap();
        assert_eq!(workers.len(), 2);
        assert_eq!(workers[0]["worker_id"], 0);
        assert_eq!(workers[0]["status"], "ok");
        assert_eq!(workers[1]["worker_id"], 1);
        assert_eq!(workers[1]["status"], "timeout");
        assert_eq!(workers[1]["error"], "zpages_timeout");
    }

    #[test]
    fn merge_shm_all_timed_out_and_empty_outcomes() {
        let (status, body) = merge_shm_zpages_responses(
            "/statusz",
            vec![
                WorkerZPageOutcome::Timeout { worker_id: 0 },
                WorkerZPageOutcome::Timeout { worker_id: 1 },
            ],
        );
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        assert_eq!(json["error"], "zpages_timeout");
        assert_eq!(json["workers"].as_array().unwrap().len(), 2);

        let (status, body) = merge_shm_zpages_responses("/statusz", vec![]);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        assert_eq!(json["error"], "zpages_unavailable");
        assert_eq!(json["workers"].as_array().unwrap().len(), 0);

        let (status, body) = merge_shm_zpages_responses(
            "/statusz",
            vec![WorkerZPageOutcome::Error {
                worker_id: 0,
                message: "boom".to_string(),
            }],
        );
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        assert_eq!(json["error"], "zpages_unavailable");
        assert_eq!(json["workers"][0]["status"], "error");
        assert_eq!(json["workers"][0]["error"], "boom");
    }
}
