"""Google-first zero-weight and shared-memory local vector embeddings for Vertebra."""

from __future__ import annotations

from vertebra.embeddings._base import (
    Embedder,
    EmbeddingTask,
    MultimodalPart,
    SimilarityMatch,
    VectorBatchView,
    VectorCache,
    VectorView,
)
from vertebra.embeddings._google import (
    AlloyDbEmbedder,
    GeminiEmbedder,
    GrpcTransportCallable,
    OracleEmbedder,
    VertexEmbedder,
)
from vertebra.embeddings._remote import (
    RemoteEmbedder,
    RemoteEmbedRequest,
    RemoteEmbedResponse,
    create_embedding_grpc_service,
)

__all__ = (
    "AlloyDbEmbedder",
    "Embedder",
    "EmbeddingTask",
    "GeminiEmbedder",
    "GrpcTransportCallable",
    "MultimodalPart",
    "OracleEmbedder",
    "RemoteEmbedRequest",
    "RemoteEmbedResponse",
    "RemoteEmbedder",
    "SimilarityMatch",
    "VectorBatchView",
    "VectorCache",
    "VectorView",
    "VertexEmbedder",
    "create_embedding_grpc_service",
)
