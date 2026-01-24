use matchit::Router as MatchRouter;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RouteId(pub u64);

pub struct Router {
    inner: MatchRouter<RouteId>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            inner: MatchRouter::new(),
        }
    }

    pub fn insert(&mut self, path: &str, id: RouteId) -> Result<(), matchit::InsertError> {
        self.inner.insert(path, id)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_matching() {
        let mut router = Router::new();
        router.insert("/hello", RouteId(1)).unwrap();
        router.insert("/echo", RouteId(2)).unwrap();
        router.insert("/user/:id", RouteId(3)).unwrap();

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
