//! Zero-GIL Rust discovery cache for `/.well-known/{agent-card.json,mcp.json,oauth-protected-resource}`.

use std::hash::Hasher;

use bytes::Bytes;
use dashmap::DashMap;
use fnv::FnvHasher;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};

pub const DEFAULT_SPA_EXCLUDED_PREFIXES: [&str; 3] = ["/.well-known", "/mcp", "/a2a"];

#[derive(Debug, Clone)]
pub struct CachedDiscoveryDoc {
    pub content_type: HeaderValue,
    pub etag: String,
    pub etag_header: HeaderValue,
    pub protocol_header: Option<(HeaderName, HeaderValue)>,
    pub body: Bytes,
}

#[derive(Debug, Clone)]
pub struct DiscoveryResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

pub struct WellKnownDiscoveryCache {
    entries: DashMap<String, CachedDiscoveryDoc>,
}

impl Default for WellKnownDiscoveryCache {
    fn default() -> Self {
        Self::new()
    }
}

impl WellKnownDiscoveryCache {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    pub fn register_document(
        &self,
        path: &str,
        content_type: &'static str,
        protocol_header: Option<(&'static str, &'static str)>,
        body: Bytes,
    ) {
        let mut hasher = FnvHasher::default();
        hasher.write(&body);
        let etag = format!("\"{:016x}\"", hasher.finish());
        let etag_header =
            HeaderValue::from_str(&etag).unwrap_or_else(|_| HeaderValue::from_static("\"0\""));
        let proto =
            protocol_header.map(|(k, v)| (HeaderName::from_static(k), HeaderValue::from_static(v)));
        self.entries.insert(
            path.to_string(),
            CachedDiscoveryDoc {
                content_type: HeaderValue::from_static(content_type),
                etag,
                etag_header,
                protocol_header: proto,
                body,
            },
        );
    }

    pub fn register_agent_card(&self, body: Bytes) {
        self.register_document(
            "/.well-known/agent-card.json",
            "application/json",
            Some(("a2a-version", "1.0")),
            body,
        );
    }

    pub fn register_mcp_manifest(&self, body: Bytes) {
        self.register_document(
            "/.well-known/mcp.json",
            "application/json",
            Some(("mcp-protocol-version", "2026-07-28")),
            body,
        );
    }

    pub fn register_oauth_protected_resource(&self, body: Bytes) {
        self.register_document(
            "/.well-known/oauth-protected-resource",
            "application/json",
            None,
            body,
        );
    }

    pub fn serve(&self, path: &str, if_none_match: Option<&str>) -> Option<DiscoveryResponse> {
        let doc = self.entries.get(path)?;
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, doc.content_type.clone());
        headers.insert(header::ETAG, doc.etag_header.clone());
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=60, must-revalidate"),
        );
        if let Some((ref name, ref val)) = doc.protocol_header {
            headers.insert(name.clone(), val.clone());
        }
        if let Some(candidate) = if_none_match
            && candidate.trim() == doc.etag
        {
            return Some(DiscoveryResponse {
                status: StatusCode::NOT_MODIFIED,
                headers,
                body: Bytes::new(),
            });
        }
        Some(DiscoveryResponse {
            status: StatusCode::OK,
            headers,
            body: doc.body.clone(),
        })
    }
}

pub fn is_excluded_from_spa_fallback(path: &str) -> bool {
    DEFAULT_SPA_EXCLUDED_PREFIXES.iter().any(|prefix| {
        path == *prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/'))
    })
}
