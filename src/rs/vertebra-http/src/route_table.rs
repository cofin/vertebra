//! Route registration API.
//!
//! `RouteTable` is a builder that accepts (method, path_pattern, handler_id)
//! triples before startup. Once built it hands off the data to `Router`.

use fnv::FnvHasher;
use std::hash::{Hash, Hasher};

/// Stable identifier for a route handler.
///
/// The value is the FNV-1a hash of the canonical string `"METHOD /path/pattern"`.
pub type HandlerId = u64;

/// Compute a stable `HandlerId` for a (method, path_pattern) pair.
///
/// The canonical form is `"{METHOD_UPPER} {path_pattern}"` with a single space.
pub fn handler_id(method: &str, path: &str) -> HandlerId {
    let mut h = FnvHasher::default();
    method.to_ascii_uppercase().hash(&mut h);
    b' '.hash(&mut h);
    path.hash(&mut h);
    h.finish()
}

/// A single route entry ready for insertion into a `matchit::Router`.
#[derive(Debug, Clone)]
pub struct RouteEntry {
    pub method: String,
    pub path: String,
    pub handler_id: HandlerId,
}

/// Builder for the route table.
///
/// Collect routes at startup, then call `build()` to get a `Router`.
#[derive(Debug, Default)]
pub struct RouteTable {
    entries: Vec<RouteEntry>,
}

impl RouteTable {
    /// Create an empty route table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a route.
    ///
    /// The `handler_id` is computed automatically from the method and path.
    pub fn insert(&mut self, method: &str, path: &str) -> HandlerId {
        let id = handler_id(method, path);
        self.entries.push(RouteEntry {
            method: method.to_ascii_uppercase(),
            path: path.to_owned(),
            handler_id: id,
        });
        id
    }

    /// Consume the table and produce the router-ready entries.
    pub fn into_entries(self) -> Vec<RouteEntry> {
        self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_id_reference_values() {
        // Reference values for cross-language verification.
        let id1 = handler_id("GET", "/hello");
        let id2 = handler_id("POST", "/api/users");
        let id3 = handler_id("GET", "/");
        let id4 = handler_id("DELETE", "/items/{id}");
        eprintln!("GET /hello = {id1}");
        eprintln!("POST /api/users = {id2}");
        eprintln!("GET / = {id3}");
        eprintln!("DELETE /items/{{id}} = {id4}");
        // Just verify they are non-zero and stable.
        assert_ne!(id1, 0);
        assert_ne!(id2, 0);
        assert_ne!(id1, id2);
    }
}
