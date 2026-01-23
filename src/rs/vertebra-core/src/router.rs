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

    pub fn match_path(&self, path: &str) -> Option<RouteId> {
        self.inner.at(path).ok().map(|m| *m.value)
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

        assert_eq!(router.match_path("/hello"), Some(RouteId(1)));
        assert_eq!(router.match_path("/echo"), Some(RouteId(2)));
        assert_eq!(router.match_path("/user/123"), Some(RouteId(3)));
        assert_eq!(router.match_path("/notfound"), None);
    }
}
