//! Matchit-based native HTTP routing with method-tag indexed dispatch.
//!
//! Provides `Router` indexed by standard HTTP method tags `0..=8` plus an
//! extension vector for custom methods, and `RouteTable` for route collection.

use fnv::FnvHasher;
use matchit::Router as MatchitRouter;
use std::hash::{Hash, Hasher};
use tracing::debug;

/// Stable identifier for a route handler derived via FNV-1a.
pub type HandlerId = u64;

/// High-bit tag identifying pre-indexed mounted sub-application routes in O(1).
pub const MOUNT_HANDLER_BASE: HandlerId = 0xFFFF_0000_0000_0000;

/// Reserved internal handler ID for HTTP diagnostic Z-Pages (`/taskz`, `/statusz`, `/varz`, `/rpcz`, `/brokerz`).
pub const ZPAGES_HANDLER_ID: HandlerId = 0xFFFE_0000_0000_0001;

/// HTTP methods registered on mounted ASGI/RSGI/WSGI prefix catch-alls.
pub const MOUNT_METHODS: [&str; 7] = ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"];

/// Encode a 0-based mount index into a reserved `HandlerId`.
#[inline]
pub const fn mount_handler_id(mount_index: usize) -> HandlerId {
    MOUNT_HANDLER_BASE | ((mount_index as u64) & 0x0000_FFFF_FFFF_FFFF)
}

/// Standard HTTP method names in tag order (0..=8).
pub const STANDARD_METHODS: [&str; 9] = [
    "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE", "CONNECT",
];

/// Map a standard HTTP method name to its tag (0..=8), or 255 for custom.
#[inline]
pub fn standard_method_tag(method: &str) -> u8 {
    match method {
        "GET" | "get" => 0,
        "POST" | "post" => 1,
        "PUT" | "put" => 2,
        "DELETE" | "delete" => 3,
        "PATCH" | "patch" => 4,
        "HEAD" | "head" => 5,
        "OPTIONS" | "options" => 6,
        "TRACE" | "trace" => 7,
        "CONNECT" | "connect" => 8,
        _ => 255,
    }
}

/// Compute a stable `HandlerId` for a (method, path_pattern) pair matching Python `_make_handler_id`.
#[inline]
pub fn handler_id(method: &str, path: &str) -> HandlerId {
    let mut h = FnvHasher::default();
    method.to_ascii_uppercase().hash(&mut h);
    b' '.hash(&mut h);
    path.hash(&mut h);
    h.finish()
}

/// A single route entry ready for insertion into `matchit::Router`.
#[derive(Debug, Clone)]
pub struct RouteEntry {
    pub method: String,
    pub path: String,
    pub handler_id: HandlerId,
}

/// Builder for collecting routes prior to compiling into `Router`.
#[derive(Debug, Default)]
pub struct RouteTable {
    entries: Vec<RouteEntry>,
    next_mount_index: usize,
}

impl RouteTable {
    /// Create an empty route table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a route, returning its stable `HandlerId`.
    pub fn insert(&mut self, method: &str, path: &str) -> HandlerId {
        let id = handler_id(method, path);
        self.entries.push(RouteEntry {
            method: method.to_ascii_uppercase(),
            path: path.to_owned(),
            handler_id: id,
        });
        id
    }

    /// Register a route with an explicit `HandlerId`.
    pub fn insert_with_id(&mut self, method: &str, path: &str, id: HandlerId) {
        let upper = method.to_ascii_uppercase();
        if self
            .entries
            .iter()
            .any(|e| e.method == upper && e.path == path)
        {
            return;
        }
        self.entries.push(RouteEntry {
            method: upper,
            path: path.to_owned(),
            handler_id: id,
        });
    }

    /// Register a mounted sub-application prefix across [`MOUNT_METHODS`] using the next mount index.
    pub fn insert_mount_prefix(&mut self, prefix: &str) -> HandlerId {
        let idx = self.next_mount_index;
        self.next_mount_index += 1;
        self.insert_mount_prefix_indexed(prefix, idx)
    }

    /// Register a mounted sub-application prefix across [`MOUNT_METHODS`] with an explicit 0-based `mount_index`.
    pub fn insert_mount_prefix_indexed(&mut self, prefix: &str, mount_index: usize) -> HandlerId {
        let id = mount_handler_id(mount_index);
        let trimmed = prefix.trim_end_matches('/');
        let (exact, catch_all) = if trimmed.is_empty() {
            ("/".to_string(), "/*rest".to_string())
        } else {
            (trimmed.to_string(), format!("{trimmed}/*rest"))
        };
        for method in &MOUNT_METHODS {
            self.insert_with_id(method, &exact, id);
            self.insert_with_id(method, &catch_all, id);
        }
        id
    }

    /// Consume the table and return collected route entries.
    pub fn into_entries(self) -> Vec<RouteEntry> {
        self.entries
    }
}

/// Error returned by `Router::lookup` or `Router::lookup_method`.
#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("no route matched: method={method} path={path}")]
    NotFound { method: String, path: String },
    #[error("method not allowed for path {path}")]
    MethodNotAllowed { path: String, allowed: Vec<String> },
}

/// Result of a successful route match.
#[derive(Debug)]
pub struct RouteMatch<'a> {
    pub handler_id: HandlerId,
    pub params: matchit::Params<'a, 'a>,
}

/// Method-tag-indexed immutable router.
pub struct Router {
    standard: [Option<MatchitRouter<HandlerId>>; 9],
    custom: Vec<(Box<str>, MatchitRouter<HandlerId>)>,
}

impl Router {
    /// Compile a `Router` from collected route entries.
    pub fn from_entries(
        entries: Vec<RouteEntry>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut standard: [Option<MatchitRouter<HandlerId>>; 9] = Default::default();
        let mut custom: Vec<(Box<str>, MatchitRouter<HandlerId>)> = Vec::new();

        for entry in entries {
            let normalized = normalize_typed_path(entry.path.as_str())?;
            let tag = standard_method_tag(&entry.method);
            if (tag as usize) < 9 {
                let mr = standard[tag as usize].get_or_insert_with(MatchitRouter::new);
                mr.insert(normalized.as_str(), entry.handler_id)?;
            } else {
                let existing = custom
                    .iter_mut()
                    .find(|(m, _)| m.eq_ignore_ascii_case(&entry.method));
                if let Some((_, mr)) = existing {
                    mr.insert(normalized.as_str(), entry.handler_id)?;
                } else {
                    let mut mr = MatchitRouter::new();
                    mr.insert(normalized.as_str(), entry.handler_id)?;
                    custom.push((entry.method.to_ascii_uppercase().into_boxed_str(), mr));
                }
            }
            debug!(
                method = %entry.method,
                path = %entry.path,
                id = entry.handler_id,
                "registered route"
            );
        }

        Ok(Self { standard, custom })
    }

    /// Return sorted HTTP methods that match `path`.
    pub fn allowed_methods(&self, path: &str) -> Vec<String> {
        let mut methods: Vec<String> = Vec::new();
        for (idx, slot) in self.standard.iter().enumerate() {
            if let Some(router) = slot
                && router.at(path).is_ok()
            {
                methods.push(STANDARD_METHODS[idx].to_owned());
            }
        }
        for (custom_m, router) in &self.custom {
            if router.at(path).is_ok() {
                methods.push(custom_m.to_string());
            }
        }
        methods.sort();
        methods
    }

    /// Look up a route using an `http::Method` directly.
    #[inline]
    pub fn lookup_method<'r>(
        &'r self,
        method: &http::Method,
        path: &'r str,
    ) -> Result<RouteMatch<'r>, RouterError> {
        let tag = standard_method_tag(method.as_str());
        if (tag as usize) < 9 {
            if let Some(router) = &self.standard[tag as usize]
                && let Ok(matched) = router.at(path)
            {
                return Ok(RouteMatch {
                    handler_id: *matched.value,
                    params: matched.params,
                });
            }
        } else {
            let m_str = method.as_str();
            for (custom_m, router) in &self.custom {
                if custom_m.eq_ignore_ascii_case(m_str)
                    && let Ok(matched) = router.at(path)
                {
                    return Ok(RouteMatch {
                        handler_id: *matched.value,
                        params: matched.params,
                    });
                }
            }
        }

        let allowed = self.allowed_methods(path);
        if !allowed.is_empty() {
            return Err(RouterError::MethodNotAllowed {
                path: path.to_owned(),
                allowed,
            });
        }

        Err(RouterError::NotFound {
            method: method.as_str().to_owned(),
            path: path.to_owned(),
        })
    }

    /// Look up a route by method string and path.
    #[inline]
    pub fn lookup<'r>(
        &'r self,
        method: &str,
        path: &'r str,
    ) -> Result<RouteMatch<'r>, RouterError> {
        let tag = standard_method_tag(method);
        if (tag as usize) < 9 {
            if let Some(router) = &self.standard[tag as usize]
                && let Ok(matched) = router.at(path)
            {
                return Ok(RouteMatch {
                    handler_id: *matched.value,
                    params: matched.params,
                });
            }
        } else {
            for (custom_m, router) in &self.custom {
                if custom_m.eq_ignore_ascii_case(method)
                    && let Ok(matched) = router.at(path)
                {
                    return Ok(RouteMatch {
                        handler_id: *matched.value,
                        params: matched.params,
                    });
                }
            }
        }

        let allowed = self.allowed_methods(path);
        if !allowed.is_empty() {
            return Err(RouterError::MethodNotAllowed {
                path: path.to_owned(),
                allowed,
            });
        }

        Err(RouterError::NotFound {
            method: method.to_ascii_uppercase(),
            path: path.to_owned(),
        })
    }
}

/// Normalize path parameters into matchit syntax.
pub fn normalize_typed_path(
    path: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    vertebra_core::router::normalize_typed_path(path)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_id_reference_values() {
        assert_eq!(handler_id("GET", "/users/{id}"), 12798828550840791120);
        assert_eq!(handler_id("POST", "/api/v1/items"), 1576413124061346619);
        assert_eq!(handler_id("DELETE", "/healthz"), 17830759806517673951);
    }

    #[test]
    fn zpages_handler_id_is_distinct_from_mount_range() {
        assert_ne!(ZPAGES_HANDLER_ID, MOUNT_HANDLER_BASE);
        assert_ne!(ZPAGES_HANDLER_ID & MOUNT_HANDLER_BASE, MOUNT_HANDLER_BASE);
    }
}
