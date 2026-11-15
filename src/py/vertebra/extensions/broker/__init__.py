"""Database connection broker and cross-process statement cache extension."""

from __future__ import annotations

from vertebra.extensions.broker.dbrpc import (
    DBRPC_HEADER_SIZE,
    DBRPC_VERSION,
    DbRpcHeader,
    DbRpcOp,
    DbRpcRequest,
    DbRpcResponse,
)
from vertebra.extensions.broker.engines import BrokerEngineDiscovery
from vertebra.extensions.broker.errors import (
    BrokerAttachError,
    BrokerAuthenticationError,
    BrokerSegmentInUseError,
    LeaseQuarantinedError,
)
from vertebra.extensions.broker.host import BrokerHost, ShutdownReport
from vertebra.extensions.broker.plugin import BrokerPlugin
from vertebra.extensions.broker.transport import (
    AsyncConnectionLease,
    AsyncSHMBrokerTransport,
    BrokerConfig,
    BrokerMetrics,
    BrokerStream,
    ChannelMessage,
    PoolMetrics,
    StatementPlan,
    SyncConnectionLease,
    SyncSHMBrokerTransport,
)

__all__ = (
    "DBRPC_HEADER_SIZE",
    "DBRPC_VERSION",
    "AsyncConnectionLease",
    "AsyncSHMBrokerTransport",
    "BrokerAttachError",
    "BrokerAuthenticationError",
    "BrokerConfig",
    "BrokerEngineDiscovery",
    "BrokerHost",
    "BrokerMetrics",
    "BrokerPlugin",
    "BrokerSegmentInUseError",
    "BrokerStream",
    "ChannelMessage",
    "DbRpcHeader",
    "DbRpcOp",
    "DbRpcRequest",
    "DbRpcResponse",
    "LeaseQuarantinedError",
    "PoolMetrics",
    "ShutdownReport",
    "StatementPlan",
    "SyncConnectionLease",
    "SyncSHMBrokerTransport",
)
