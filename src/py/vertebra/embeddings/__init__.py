"""Google-first zero-weight and shared-memory local vector embeddings for Vertebra."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.embeddings._base import (
        Embedder,
        EmbeddingError,
        EmbeddingTask,
        MultimodalPart,
        SimilarityMatch,
        VectorBatchView,
        VectorCache,
        VectorView,
    )
    from vertebra.embeddings._broker import (
        BackendFactory,
        BackendInferenceCallable,
        EmbeddingBroker,
        EmbeddingBrokerConfig,
    )
    from vertebra.embeddings._google import (
        GeminiEmbedder,
        GrpcTransportCallable,
        VertexEmbedder,
    )
    from vertebra.embeddings._local import (
        FastEmbedder,
        GemmaEmbedder,
        StaticEmbedder,
    )
    from vertebra.embeddings._models import (
        MODEL_CATALOG,
        ModelSpec,
        get_model_spec,
        list_model_specs,
    )
    from vertebra.embeddings._pushdown import PushdownEmbedder
    from vertebra.embeddings._remote import (
        RemoteEmbedder,
        RemoteEmbedRequest,
        RemoteEmbedResponse,
        create_embedding_grpc_service,
    )

__all__ = (
    "MODEL_CATALOG",
    "BackendFactory",
    "BackendInferenceCallable",
    "Embedder",
    "EmbeddingBroker",
    "EmbeddingBrokerConfig",
    "EmbeddingError",
    "EmbeddingTask",
    "FastEmbedder",
    "GeminiEmbedder",
    "GemmaEmbedder",
    "GrpcTransportCallable",
    "ModelSpec",
    "MultimodalPart",
    "PushdownEmbedder",
    "RemoteEmbedRequest",
    "RemoteEmbedResponse",
    "RemoteEmbedder",
    "SimilarityMatch",
    "StaticEmbedder",
    "VectorBatchView",
    "VectorCache",
    "VectorView",
    "VertexEmbedder",
    "create_embedding_grpc_service",
    "get_model_spec",
    "list_model_specs",
)

__lazy_modules__ = [
    "vertebra.embeddings._base",
    "vertebra.embeddings._broker",
    "vertebra.embeddings._google",
    "vertebra.embeddings._local",
    "vertebra.embeddings._models",
    "vertebra.embeddings._pushdown",
    "vertebra.embeddings._remote",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "BackendFactory": ("vertebra.embeddings._broker", "BackendFactory"),
    "BackendInferenceCallable": ("vertebra.embeddings._broker", "BackendInferenceCallable"),
    "Embedder": ("vertebra.embeddings._base", "Embedder"),
    "EmbeddingBroker": ("vertebra.embeddings._broker", "EmbeddingBroker"),
    "EmbeddingBrokerConfig": ("vertebra.embeddings._broker", "EmbeddingBrokerConfig"),
    "EmbeddingError": ("vertebra.embeddings._base", "EmbeddingError"),
    "EmbeddingTask": ("vertebra.embeddings._base", "EmbeddingTask"),
    "FastEmbedder": ("vertebra.embeddings._local", "FastEmbedder"),
    "GeminiEmbedder": ("vertebra.embeddings._google", "GeminiEmbedder"),
    "GemmaEmbedder": ("vertebra.embeddings._local", "GemmaEmbedder"),
    "GrpcTransportCallable": ("vertebra.embeddings._google", "GrpcTransportCallable"),
    "MODEL_CATALOG": ("vertebra.embeddings._models", "MODEL_CATALOG"),
    "ModelSpec": ("vertebra.embeddings._models", "ModelSpec"),
    "MultimodalPart": ("vertebra.embeddings._base", "MultimodalPart"),
    "PushdownEmbedder": ("vertebra.embeddings._pushdown", "PushdownEmbedder"),
    "RemoteEmbedRequest": ("vertebra.embeddings._remote", "RemoteEmbedRequest"),
    "RemoteEmbedResponse": ("vertebra.embeddings._remote", "RemoteEmbedResponse"),
    "RemoteEmbedder": ("vertebra.embeddings._remote", "RemoteEmbedder"),
    "SimilarityMatch": ("vertebra.embeddings._base", "SimilarityMatch"),
    "StaticEmbedder": ("vertebra.embeddings._local", "StaticEmbedder"),
    "VectorBatchView": ("vertebra.embeddings._base", "VectorBatchView"),
    "VectorCache": ("vertebra.embeddings._base", "VectorCache"),
    "VectorView": ("vertebra.embeddings._base", "VectorView"),
    "VertexEmbedder": ("vertebra.embeddings._google", "VertexEmbedder"),
    "create_embedding_grpc_service": ("vertebra.embeddings._remote", "create_embedding_grpc_service"),
    "get_model_spec": ("vertebra.embeddings._models", "get_model_spec"),
    "list_model_specs": ("vertebra.embeddings._models", "list_model_specs"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
