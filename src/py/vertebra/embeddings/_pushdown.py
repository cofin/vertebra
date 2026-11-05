"""Embedding facade for engine-owned SQL pushdown capabilities."""

from __future__ import annotations

from typing import Any

from vertebra.db.driver import DataError, EmbeddingPushdown
from vertebra.embeddings._base import Embedder, EmbeddingError, VectorBatchView, VectorCache, VectorView

__all__ = ("PushdownEmbedder",)


class PushdownEmbedder(Embedder):
    """Adapt database embedding inference to the shared Embedder interface."""

    def __new__(cls, *args: Any, **kwargs: Any) -> PushdownEmbedder:
        """Allocate without invoking the default Vertex provider factory."""
        return object.__new__(cls)

    def __init__(
        self,
        pushdown: EmbeddingPushdown,
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        normalize: bool = True,
        cache: VectorCache | None = None,
    ) -> None:
        super().__init__(model, dimensions=dimensions, cache=cache)
        self._pushdown = pushdown
        self._normalize = normalize

    @property
    def pushdown(self) -> EmbeddingPushdown:
        """Return the engine embedding capability."""
        return self._pushdown

    @property
    def normalize(self) -> bool:
        """Return whether generated vectors are normalized."""
        return self._normalize

    async def vector_search(
        self,
        query: str | VectorView,
        *,
        table: str,
        embedding_column: str = "embedding",
        top_k: int = 10,
        metric: str = "cosine",
    ) -> list[dict[str, Any]]:
        """Execute vector similarity search in the database engine."""
        vector = await self.embed_query(query) if isinstance(query, str) else query
        return await self._pushdown.similarity_search(
            vector, table=table, embedding_column=embedding_column, top_k=top_k, metric=metric
        )

    async def _embed_single_impl(self, text: str, *, task: str = "query", title: str | None = None) -> VectorView:
        if task != "query" or title is not None:
            return await super()._embed_single_impl(text, task=task, title=title)
        self._record_usage((text,))
        try:
            return await self._pushdown.embed_query(text, dimensions=self._dimensions, normalize=self._normalize)
        except DataError as exc:
            raise EmbeddingError(str(exc)) from exc

    async def _embed_batch_impl(
        self, texts: list[str], *, task: str = "document", titles: list[str | None] | None = None
    ) -> VectorBatchView:
        try:
            return await self._pushdown.embed_documents(
                texts, task=task, dimensions=self._dimensions, normalize=self._normalize
            )
        except DataError as exc:
            raise EmbeddingError(str(exc)) from exc
