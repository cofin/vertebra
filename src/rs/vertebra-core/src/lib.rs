//! Core IPC primitives and shared types for Vertebra.
//!
//! This crate provides foundational types and protocols used across the
//! Vertebra ecosystem for zero-copy inter-process communication.

/// Version constant synchronized with Python package.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Placeholder for IPC protocol types.
///
/// Future PRDs will define:
/// - Message formats
/// - Serialization protocols
/// - Shared memory primitives
pub mod ipc {
    // TODO: Implement in PRD-1.
}

/// Placeholder for type bridge utilities.
///
/// Future PRDs will define:
/// - Python type → Rust type mappings
/// - TypeScript type generation
pub mod types {
    // TODO: Implement in PRD-2.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }
}
