//! Shared synthetic batch builder for standalone request proxies and protocol scopes.

use std::sync::Arc;
use vertebra_core::transport::{BatchSnapshot, TransportNode};

pub struct SyntheticRequest;

impl SyntheticRequest {
    #[inline]
    pub fn method_tag(method: &str) -> u8 {
        match method {
            "GET" => 0,
            "POST" => 1,
            "PUT" => 2,
            "DELETE" => 3,
            "PATCH" => 4,
            "HEAD" => 5,
            "OPTIONS" => 6,
            "TRACE" => 7,
            "CONNECT" => 8,
            _ => 255,
        }
    }

    #[inline]
    pub fn http_version_tag(http_version: &str) -> u8 {
        match http_version {
            "1.0" => 0,
            "1.1" => 1,
            "2" | "2.0" => 2,
            "3" | "3.0" => 3,
            _ => 1,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build(
        method: &str,
        path: &str,
        query: Option<&str>,
        headers_raw: &[u8],
        body: &[u8],
        correlation_id: u64,
        handler_id: u64,
        http_version: &str,
    ) -> Arc<BatchSnapshot> {
        let (clean_path, clean_query) = match query {
            Some(q) => (path, q),
            None => path.split_once('?').unwrap_or((path, "")),
        };
        let path_bytes = clean_path.as_bytes();
        let query_bytes = clean_query.as_bytes();
        let mut slab = Vec::with_capacity(
            path_bytes.len() + query_bytes.len() + headers_raw.len() + body.len(),
        );

        let path_offset = 0u32;
        let path_len = path_bytes.len() as u32;
        slab.extend_from_slice(path_bytes);

        let query_offset = slab.len() as u32;
        let query_len = query_bytes.len() as u32;
        slab.extend_from_slice(query_bytes);

        let headers_offset = slab.len() as u32;
        let headers_len = headers_raw.len() as u32;
        slab.extend_from_slice(headers_raw);

        let body_offset = slab.len() as u32;
        let body_len = body.len() as u32;
        slab.extend_from_slice(body);

        let node = TransportNode {
            method_tag: Self::method_tag(method),
            http_version_tag: Self::http_version_tag(http_version),
            _pad: [0; 2],
            handler_id,
            correlation_id,
            path_offset,
            path_len,
            query_offset,
            query_len,
            headers_offset,
            headers_len,
            body_offset,
            body_len,
        };

        Arc::new(BatchSnapshot::new(slab, vec![node]))
    }
}
