//! Core IPC primitives and shared types for Vertebra.
//!
//! This crate provides foundational types and protocols used across the
//! Vertebra ecosystem for zero-copy inter-process communication.

pub mod bridge;
pub mod broker;
pub mod buffer;
pub mod channel;
pub mod cleanup;
pub mod dbrpc;
#[cfg(feature = "gcp-auth")]
pub mod gcp_auth;
#[cfg(feature = "gcp-auth")]
pub mod gcp_identity;
pub mod guards;
pub mod id;
pub mod ipc;
pub mod layout;
pub mod logger;
pub mod notify;
pub mod platform;
pub mod process;
pub mod proto;
pub mod protocol;
pub mod router;
pub mod shm;
pub mod sync;
pub mod telemetry;
pub mod text;
pub mod tiered_ring;
pub mod transport;
pub mod util;
pub mod vector;

// Re-exports for ergonomics
pub use bridge::{
    BatchBridge, BridgeResponse, ResponseSink, ResponseStreamFrame, StreamDelivery, StreamSink,
    WriteRequest,
};
pub use broker::{
    BrokerError, LeaseGrant, LeaseSlot, LeaseTableStats, MAX_POOL_PARTITIONS, PoolPartitionSpec,
    ShmLeaseTable, ShmStatementCache, StatementCacheStats, StatementPlanEntry, StatementSlot,
    stable_hash64,
};
pub use channel::{MpmcSlotView, SlotView};
pub use layout::CacheLine;
pub use notify::{AsyncCompletionQueue, Notifier, SyncCompletionQueue};
pub use transport::ring_pair::{RingPair, TieredRingSet};
pub use transport::{BatchSnapshot, InProcessRequest, TransportError, TransportNode};
