use matchit::Router as MatchRouter;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RouteId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterError {
    Insert(matchit::InsertError),
    InvalidParam { segment: String },
    InvalidParamType { segment: String, param_type: String },
}

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RouterError::Insert(err) => write!(f, "{err}"),
            RouterError::InvalidParam { segment } => {
                write!(f, "invalid typed param segment: {segment}")
            }
            RouterError::InvalidParamType {
                segment,
                param_type,
            } => write!(
                f,
                "invalid typed param type '{param_type}' in segment: {segment}"
            ),
        }
    }
}

impl std::error::Error for RouterError {}

impl From<matchit::InsertError> for RouterError {
    fn from(err: matchit::InsertError) -> Self {
        RouterError::Insert(err)
    }
}

pub struct Router {
    inner: MatchRouter<RouteId>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            inner: MatchRouter::new(),
        }
    }

    pub fn insert(&mut self, path: &str, id: RouteId) -> Result<(), RouterError> {
        let normalized = normalize_typed_path(path)?;
        self.inner.insert(&normalized, id)?;
        Ok(())
    }

    pub fn match_path<'k, 'v>(
        &'k self,
        path: &'v str,
    ) -> Option<(RouteId, Vec<(&'k str, &'v str)>)> {
        match self.inner.at(path) {
            Ok(m) => {
                let params = m.params.iter().collect();
                Some((*m.value, params))
            }
            Err(_) => None,
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

fn normalize_typed_path(path: &str) -> Result<String, RouterError> {
    if !path.contains('{') {
        return Ok(path.to_string());
    }

    let mut segments = Vec::new();
    for segment in path.split('/') {
        if let Some(normalized) = normalize_typed_segment(segment)? {
            segments.push(normalized);
        } else {
            segments.push(segment.to_string());
        }
    }

    Ok(segments.join("/"))
}

fn normalize_typed_segment(segment: &str) -> Result<Option<String>, RouterError> {
    if !(segment.starts_with('{') && segment.ends_with('}')) {
        return Ok(None);
    }

    let inner = &segment[1..segment.len() - 1];
    let mut parts = inner.splitn(2, ':');
    let name = parts.next().unwrap_or("").trim();
    if name.is_empty() {
        return Err(RouterError::InvalidParam {
            segment: segment.to_string(),
        });
    }

    if let Some(param_type) = parts.next() {
        let param_type = param_type.trim();
        if !param_type.is_empty() && !is_supported_type(param_type) {
            return Err(RouterError::InvalidParamType {
                segment: segment.to_string(),
                param_type: param_type.to_string(),
            });
        }
    }

    Ok(Some(format!(":{name}")))
}

fn is_supported_type(param_type: &str) -> bool {
    matches!(
        param_type,
        "str" | "string" | "int" | "i64" | "u64" | "float" | "f64" | "bool" | "uuid"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_matching() {
        let mut router = Router::new();
        router.insert("/hello", RouteId(1)).unwrap();
        router.insert("/echo", RouteId(2)).unwrap();
        router.insert("/user/{id:int}", RouteId(3)).unwrap();

        assert_eq!(
            router.match_path("/hello").map(|(id, _)| id),
            Some(RouteId(1))
        );
        assert_eq!(
            router.match_path("/echo").map(|(id, _)| id),
            Some(RouteId(2))
        );

        let (id, params) = router.match_path("/user/123").unwrap();
        assert_eq!(id, RouteId(3));
        assert_eq!(params, vec![("id", "123")]);

        assert_eq!(router.match_path("/notfound"), None);
    }
}
