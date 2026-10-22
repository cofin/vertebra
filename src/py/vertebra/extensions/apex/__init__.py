"""Oracle APEX, ORDS, and 26ai JSON Relational Duality View integration."""

from __future__ import annotations

from vertebra.extensions.apex.bridge import (
    ApexFederatedQuery,
    ApexFederatedSource,
    ApexIslandRegistry,
    ApexIslandSpec,
    ApexOfflineMutation,
    ApexOfflineSyncEngine,
    ApexOfflineSyncResult,
)
from vertebra.extensions.apex.gateway import (
    ApexAccessPolicy,
    ApexGateway,
    ApexPageToolSpec,
    ApexPolicyDecision,
    ApexPolicyEvaluator,
    ConfirmationGate,
    OrdsRestTransport,
)
from vertebra.extensions.apex.openapi import (
    ApexOpenAPIGenerator,
    DualityViewMcpHandler,
    DualityViewSpec,
    OrdsEndpointSpec,
    OrdsMcpSynthesizer,
    OrdsTransportProtocol,
)
from vertebra.extensions.apex.session import (
    ApexHandoffClaims,
    ApexSessionAuth,
    ApexSessionContext,
    OracleConnectionLike,
    OraclePoolLike,
    OracleSessionManager,
)

__all__ = (
    "ApexAccessPolicy",
    "ApexFederatedQuery",
    "ApexFederatedSource",
    "ApexGateway",
    "ApexHandoffClaims",
    "ApexIslandRegistry",
    "ApexIslandSpec",
    "ApexOfflineMutation",
    "ApexOfflineSyncEngine",
    "ApexOfflineSyncResult",
    "ApexOpenAPIGenerator",
    "ApexPageToolSpec",
    "ApexPolicyDecision",
    "ApexPolicyEvaluator",
    "ApexSessionAuth",
    "ApexSessionContext",
    "ConfirmationGate",
    "DualityViewMcpHandler",
    "DualityViewSpec",
    "OracleConnectionLike",
    "OraclePoolLike",
    "OracleSessionManager",
    "OrdsEndpointSpec",
    "OrdsMcpSynthesizer",
    "OrdsRestTransport",
    "OrdsTransportProtocol",
)
