//! Core IPC primitives and shared types for Vertebra.
//!
//! This crate provides foundational types and protocols used across the
//! Vertebra ecosystem for zero-copy inter-process communication.

/// Version constant synchronized with Python package.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod async_notifier;
pub mod async_notify;
pub use async_notify::AsyncNotifier;
pub mod async_ring;
pub mod bridge;
pub mod buffer;
pub mod channel;
pub mod cleanup;
pub mod guards;
pub mod ipc;
pub mod notify;
pub mod platform;
pub mod process;
pub mod protocol;
pub mod router;
pub mod shm;
pub mod sync;
pub mod tiered_ring;
pub mod transport;

// Re-exports for ergonomics
pub use bridge::{BatchBridge, BridgeResponse, LocalExchange, WriteRequest};
pub use transport::ring_pair::{ResponseChannel, RingPair, TieredRingSet, WorkerChannel};
pub use transport::{BatchSnapshot, RequestBatch, RequestRecord, TransportError, TransportNode};
