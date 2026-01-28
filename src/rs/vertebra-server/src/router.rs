use matchit::Router as MatchitRouter;

pub struct Router {
    inner: MatchitRouter<String>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            inner: MatchitRouter::new(),
        }
    }

    pub fn insert(&mut self, path: &str, handler: String) -> Result<(), matchit::InsertError> {
        self.inner.insert(path, handler)
    }

    pub fn lookup<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<matchit::Match<'a, 'a, &'a String>, matchit::MatchError> {
        self.inner.at(path)
    }
}
