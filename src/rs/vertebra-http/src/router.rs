//! matchit-based native router.
//!
//! Each HTTP method gets its own `matchit::Router<HandlerId>` so that two routes
//! on different methods with the same path pattern do not conflict inside matchit.

use std::collections::HashMap;

use matchit::Router as MatchitRouter;
use tracing::debug;

use crate::route_table::{HandlerId, RouteEntry};

/// Error returned by `Router::lookup`.
#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("no route matched: method={method} path={path}")]
    NotFound { method: String, path: String },
    #[error("method not allowed for path {path}")]
    MethodNotAllowed { path: String },
}

/// The result of a successful route match.
#[derive(Debug)]
pub struct RouteMatch<'a> {
    /// Stable handler ID; this is what the batch collector embeds in `RequestInfo`.
    pub handler_id: HandlerId,
    /// Path parameters extracted by matchit (e.g., `id` from `/:id`).
    pub params: matchit::Params<'a, 'a>,
}

/// A compiled, immutable router built from a `RouteTable`.
///
/// Thread-safe: wrap in `Arc<Router>` and share across Tokio tasks.
pub struct Router {
    /// One matchit router per HTTP method.
    per_method: HashMap<String, MatchitRouter<HandlerId>>,
}

impl Router {
    /// Build a `Router` from the collected route entries.
    pub fn from_entries(
        entries: Vec<RouteEntry>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut per_method: HashMap<String, MatchitRouter<HandlerId>> = HashMap::new();

        for entry in entries {
            let mr = per_method.entry(entry.method.clone()).or_default();
            let normalized = vertebra_core::router::normalize_typed_path(entry.path.as_str())?;
            mr.insert(normalized.as_str(), entry.handler_id)?;
            debug!(
                method = %entry.method,
                path  = %entry.path,
                id    = entry.handler_id,
                "registered route"
            );
        }

        Ok(Self { per_method })
    }

    /// Look up a request by method and path.
    #[inline]
    pub fn lookup<'r>(
        &'r self,
        method: &str,
        path: &'r str,
    ) -> Result<RouteMatch<'r>, RouterError> {
        let method_upper: std::borrow::Cow<'_, str> =
            if method.bytes().all(|b| !b.is_ascii_lowercase()) {
                std::borrow::Cow::Borrowed(method)
            } else {
                std::borrow::Cow::Owned(method.to_ascii_uppercase())
            };
        let mr =
            self.per_method
                .get(method_upper.as_ref())
                .ok_or_else(|| RouterError::NotFound {
                    method: method_upper.clone().into_owned(),
                    path: path.to_owned(),
                })?;

        let matched = mr.at(path).map_err(|_| RouterError::NotFound {
            method: method_upper.into_owned(),
            path: path.to_owned(),
        })?;

        Ok(RouteMatch {
            handler_id: *matched.value,
            params: matched.params,
        })
    }
}
