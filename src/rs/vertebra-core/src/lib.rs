//! vertebra-core — IPC primitives, ring buffers, and arena allocators.
//!
//! This crate is a stub. Implementation arrives in the `ipc-substrate` chapter.

pub mod placeholder {
    /// Placeholder to confirm the crate compiles.
    pub fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}

#[cfg(test)]
mod tests {
    use super::placeholder;

    #[test]
    fn version_is_set() {
        assert_eq!(placeholder::version(), "0.1.0");
    }
}
