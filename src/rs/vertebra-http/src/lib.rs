//! vertebra-http — HTTP/2 and HTTP/3 server primitives.
//!
//! This crate is a stub. Implementation arrives in the `http-server-h2` chapter.

pub mod placeholder {
    /// Confirm the crate links against vertebra-core.
    pub fn core_version() -> &'static str {
        vertebra_core::placeholder::version()
    }
}

#[cfg(test)]
mod tests {
    use super::placeholder;

    #[test]
    fn links_to_core() {
        assert_eq!(placeholder::core_version(), "0.1.0");
    }
}
