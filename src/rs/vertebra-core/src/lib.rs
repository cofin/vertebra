//! Core IPC primitives and shared types for Vertebra.
//!
//! This crate provides foundational types and protocols used across the
//! Vertebra ecosystem for zero-copy inter-process communication.

pub mod bridge;
pub mod broker;
pub mod buffer;
pub mod channel;
pub mod cleanup;
pub mod guards;
pub mod ipc;
pub mod layout;
pub mod logger;
pub mod notify;
pub mod platform;
pub mod process;
pub mod protocol;
pub mod router;
pub mod shm;
pub mod sync;
pub mod telemetry;
pub mod tiered_ring;
pub mod transport;
pub mod util;

// Re-exports for ergonomics
pub use bridge::{BatchBridge, BridgeResponse, ResponseSink, WriteRequest};
pub use broker::{
    BrokerError, LeaseGrant, LeaseSlot, LeaseTableStats, ShmLeaseTable, ShmStatementCache,
    StatementCacheStats, StatementPlanEntry, StatementSlot,
};
pub use layout::CacheLine;
pub use transport::ring_pair::{RingPair, TieredRingSet};
pub use transport::{BatchSnapshot, InProcessRequest, TransportError, TransportNode};
