"""Oracle APEX, ORDS, and 26ai Duality View integration for Vertebra."""

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
    ApexAgentCard,
    ApexGateway,
    ApexPageToolSpec,
    ApexPolicyDecision,
    ApexPolicyEvaluator,
    ConfirmationGate,
    OrdsRestTransport,
)
from vertebra.extensions.apex.openapi import (
    ApexOpenAPIGenerator,
    ApexSkillSpec,
    ApexToolSpec,
    DualityViewSpec,
    OrdsEndpointSpec,
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
    "ApexAgentCard",
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
    "ApexSkillSpec",
    "ApexToolSpec",
    "ConfirmationGate",
    "DualityViewSpec",
    "OracleConnectionLike",
    "OraclePoolLike",
    "OracleSessionManager",
    "OrdsEndpointSpec",
    "OrdsRestTransport",
)
