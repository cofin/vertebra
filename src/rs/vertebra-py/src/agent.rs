//! Fastpath agentic JSON-RPC classification and schema digest caching.

use std::collections::BTreeMap;
use std::sync::RwLock;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

pub const FRAME_KIND_NOTIFICATION: u8 = 0;
pub const FRAME_KIND_SINGLE_REQUEST: u8 = 1;
pub const FRAME_KIND_BATCH_REQUEST: u8 = 2;
pub const FRAME_KIND_RESPONSE: u8 = 3;

/// 64-bit FNV-1a hash offset basis and prime.
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Classify a raw JSON-RPC payload into (frame_kind, Option<method>, Option<id>).
#[pyfunction]
pub fn classify_jsonrpc_frame(
    _py: Python<'_>,
    payload: &[u8],
) -> PyResult<(u8, Option<String>, Option<String>)> {
    let trimmed = payload.trim_ascii();
    if trimmed.starts_with(b"[") {
        return Ok((FRAME_KIND_BATCH_REQUEST, None, None));
    }

    if !trimmed.starts_with(b"{") {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Invalid JSON-RPC frame: must start with { or [",
        ));
    }

    let val: serde_json::Value = match serde_json::from_slice(trimmed) {
        Ok(v) => v,
        Err(e) => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "JSON-RPC parse error: {e}"
            )));
        }
    };

    let obj = match val.as_object() {
        Some(o) => o,
        None => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Invalid JSON-RPC frame: expected object",
            ));
        }
    };

    let method = obj
        .get("method")
        .and_then(|m| m.as_str())
        .map(|s| s.to_string());
    let id = obj.get("id").map(|i| match i {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => i.to_string(),
    });

    if let Some(m) = method {
        if id.is_some() {
            Ok((FRAME_KIND_SINGLE_REQUEST, Some(m), id))
        } else {
            Ok((FRAME_KIND_NOTIFICATION, Some(m), None))
        }
    } else if obj.contains_key("result") || obj.contains_key("error") {
        Ok((FRAME_KIND_RESPONSE, None, id))
    } else {
        Ok((FRAME_KIND_SINGLE_REQUEST, None, id))
    }
}

type CachedSchemaEntry = (u64, String, Vec<u8>);

/// Thread-safe digest and byte cache for tool and skill JSON Schema payloads.
#[pyclass(name = "SchemaDigestCache", frozen)]
pub struct SchemaDigestCache {
    inner: RwLock<BTreeMap<String, CachedSchemaEntry>>,
}

impl Default for SchemaDigestCache {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl SchemaDigestCache {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }

    #[staticmethod]
    pub fn compute_schema_digest(bytes: &[u8]) -> u64 {
        fnv1a_64(bytes)
    }

    pub fn put(&self, key: &str, json_bytes: &[u8]) -> (u64, String) {
        let digest = fnv1a_64(json_bytes);
        let etag = format!("\"{:016x}\"", digest);
        let mut map = self.inner.write().unwrap();
        map.insert(key.to_string(), (digest, etag.clone(), json_bytes.to_vec()));
        (digest, etag)
    }

    pub fn get_etag(&self, key: &str) -> Option<String> {
        let map = self.inner.read().unwrap();
        map.get(key).map(|(_, etag, _)| etag.clone())
    }

    pub fn get_bytes<'py>(&self, py: Python<'py>, key: &str) -> Option<Bound<'py, PyBytes>> {
        let map = self.inner.read().unwrap();
        map.get(key).map(|(_, _, bytes)| PyBytes::new(py, bytes))
    }

    pub fn combined_etag(&self) -> String {
        let map = self.inner.read().unwrap();
        let mut combined = FNV_OFFSET_BASIS;
        for (key, (digest, _, _)) in map.iter() {
            for &b in key.as_bytes() {
                combined ^= b as u64;
                combined = combined.wrapping_mul(FNV_PRIME);
            }
            combined ^= *digest;
            combined = combined.wrapping_mul(FNV_PRIME);
        }
        format!("\"W/{:016x}\"", combined)
    }
}
