//! Core IPC primitives and shared types for Vertebra.
//!
//! This crate provides foundational types and protocols used across the
//! Vertebra ecosystem for zero-copy inter-process communication.

/// Version constant synchronized with Python package.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod async_ring;
pub mod buffer;
pub mod channel;
pub mod ipc;
pub mod notify;
pub(crate) mod platform;
pub mod router;
pub mod shm;
pub mod sync;

/// Placeholder for type bridge utilities (PRD-2+).
pub mod types {}
