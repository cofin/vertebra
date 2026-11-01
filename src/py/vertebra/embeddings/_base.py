"""Base Embedder interface, default Google Vertex factory, VectorCache integration, and GenAI telemetry."""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Awaitable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, cast

import msgspec

from vertebra._vertebra import (
    ArrowCursor,
    SimilarityMatch,
    VectorBatchView,
    VectorCache,
    VectorView,
)
from vertebra.telemetry.genai import GenAiUsage
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.embeddings._google import VertexEmbedder

__all__ = (
    "Embedder",
    "EmbeddingTask",
    "MultimodalPart",
    "SimilarityMatch",
    "VectorBatchView",
    "VectorCache",
    "VectorView",
)

EmbeddingTask = Literal[
    "query",
    "document",
    "search_query",
    "search_document",
    "question_answering",
    "fact_verification",
    "classification",
    "clustering",
    "semantic_similarity",
    "code_retrieval",
]


class MultimodalPart(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Multimodal content part for Gemini Embedding 2 (image, audio, video, PDF, or text)."""

    mime_type: str = "text/plain"
    data: bytes | None = None
    uri: str | None = None
    text: str | None = None


class Embedder:
    """Base embedding provider with zero-copy PEP 3118 `VectorView` / `VectorBatchView` outputs.

    Instantiating `Embedder()` directly returns a `VertexEmbedder("gemini-embedding-001", dimensions=768)`
    by default. Subclasses inherit `VectorCache` deduplication, `embed_arrow`, `similarity_search`,
    and OpenTelemetry `GenAiUsage` accounting.
    """

    def __new__(
        cls,
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        **kwargs: Any,
    ) -> Embedder:
        """Return a default `VertexEmbedder` when `Embedder(...)` is called directly."""
        if cls is Embedder:
            google_mod = ensure_module("vertebra.embeddings._google")
            vertex_cls = google_mod.VertexEmbedder
            instance: VertexEmbedder = object.__new__(vertex_cls)
            vertex_cls.__init__(instance, model, dimensions=dimensions, **kwargs)
            return instance
        return super().__new__(cls)

    def __init__(
        self,
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        cache: VectorCache | None = None,
    ) -> None:
        """Initialize the base embedder model name, output dimensions, and optional SHM VectorCache."""
        if getattr(self, "_initialized_base", False):
            return
        if dimensions <= 0:
            raise ValueError(f"dimensions must be > 0, got {dimensions}")
        self._model = model
        self._dimensions = dimensions
        self._cache = cache
        self._last_usage: GenAiUsage | None = None
        self._initialized_base = True

    @property
    def model(self) -> str:
        """Return the configured embedding model identifier."""
        return self._model

    @property
    def dimensions(self) -> int:
        """Return the output vector dimensionality."""
        return self._dimensions

    @property
    def cache(self) -> VectorCache | None:
        """Return the optional shared-memory seqlock `VectorCache`."""
        return self._cache

    @property
    def last_usage(self) -> GenAiUsage | None:
        """Return the most recent OpenTelemetry `GenAiUsage` snapshot."""
        return self._last_usage

    async def embed_query(self, text: str) -> VectorView:
        """Embed a singular search query and return a zero-copy PEP 3118 `VectorView`."""
        if self._cache is not None:
            cached = self._cache.lookup(self._model, "query", text)
            if cached is not None:
                return cached
        vec = await self._embed_single_impl(text, task="query", title=None)
        if self._cache is not None and vec.dimensions == self._cache.dimensions:
            self._cache.insert(self._model, "query", text, vec)
        return vec

    def embed_query_sync(self, text: str) -> VectorView:
        """Synchronously embed a singular search query."""
        return self._run_sync(self.embed_query(text))

    async def embed_document(self, text: str, *, title: str | None = None) -> VectorView:
        """Embed a singular document (with optional title) and return a `VectorView`."""
        lookup_text = f"{title or ''}|{text}"
        if self._cache is not None:
            cached = self._cache.lookup(self._model, "document", lookup_text)
            if cached is not None:
                return cached
        vec = await self._embed_single_impl(text, task="document", title=title)
        if self._cache is not None and vec.dimensions == self._cache.dimensions:
            self._cache.insert(self._model, "document", lookup_text, vec)
        return vec

    def embed_document_sync(self, text: str, *, title: str | None = None) -> VectorView:
        """Synchronously embed a singular document."""
        return self._run_sync(self.embed_document(text, title=title))

    async def embed_many(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        titles: Sequence[str | None] | None = None,
    ) -> VectorBatchView:
        """Embed a batch of texts and return a contiguous 2D PEP 3118 `VectorBatchView`."""
        text_list = list(texts)
        if not text_list:
            raise ValueError("texts must not be empty")
        title_list: list[str | None] = list(titles) if titles is not None else [None] * len(text_list)
        if len(title_list) != len(text_list):
            raise ValueError("titles length must match texts length")
        self._record_usage(text_list)
        batch = await self._embed_batch_impl(text_list, task=task, titles=title_list)
        if self._cache is not None and batch.dimensions == self._cache.dimensions:
            for idx, item_text in enumerate(text_list):
                lookup_text = f"{title_list[idx]}|{item_text}" if title_list[idx] else item_text
                self._cache.insert(self._model, task, lookup_text, batch[idx])
        return batch

    def embed_many_sync(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        titles: Sequence[str | None] | None = None,
    ) -> VectorBatchView:
        """Synchronously embed a batch of texts into a `VectorBatchView`."""
        return self._run_sync(self.embed_many(texts, task=task, titles=titles))

    async def embed_arrow(
        self,
        cursor_or_table: Any,
        *,
        text_column: str = "text",
        output_column: str = "embedding",
    ) -> ArrowCursor:
        """Extract `text_column` from an ArrowCursor, RecordBatch, or row sequence and return a SHM `ArrowCursor`."""
        texts = self._extract_texts_from_tabular(cursor_or_table, text_column)
        batch = await self.embed_many(texts, task="document")
        return batch.to_arrow_shm(column_name=output_column)

    def similarity_search(
        self,
        query: str | VectorView,
        candidates: VectorBatchView,
        *,
        top_k: int = 10,
        metric: str = "cosine",
    ) -> list[SimilarityMatch]:
        """Execute off-GIL SIMD top-k similarity search over a `VectorBatchView`."""
        query_vec = self.embed_query_sync(query) if isinstance(query, str) else query
        return candidates.top_k(query_vec, k=top_k, metric=metric)

    async def _embed_single_impl(
        self,
        text: str,
        *,
        task: str = "query",
        title: str | None = None,
    ) -> VectorView:
        """Embed a single text item, defaulting to a 1-row `_embed_batch_impl` call."""
        self._record_usage((text,))
        batch = await self._embed_batch_impl([text], task=task, titles=[title])
        return batch[0]

    async def _embed_batch_impl(
        self,
        texts: list[str],
        *,
        task: str = "document",
        titles: list[str | None] | None = None,
    ) -> VectorBatchView:
        """Concrete subclass implementation for batch embedding."""
        raise NotImplementedError(f"{type(self).__name__} must implement _embed_batch_impl")

    def _record_usage(self, texts: Sequence[str]) -> GenAiUsage:
        """Estimate input token count and record a `GenAiUsage` telemetry snapshot."""
        estimated_tokens = sum(max(1, len(t) // 4) for t in texts)
        usage = GenAiUsage(
            input_tokens=estimated_tokens,
            output_tokens=0,
            thought_tokens=0,
            cached_input_tokens=0,
        )
        self._last_usage = usage
        return usage

    @staticmethod
    def _extract_texts_from_tabular(cursor_or_table: Any, text_column: str) -> list[str]:
        """Extract a list of strings from a column in an ArrowCursor, RecordBatch, Table, or sequence of dicts."""
        if hasattr(cursor_or_table, "to_pylist"):
            rows = cursor_or_table.to_pylist()
            return [str(r[text_column]) for r in rows]
        if isinstance(cursor_or_table, Sequence) and not isinstance(cursor_or_table, (str, bytes)):
            extracted: list[str] = []
            for item in cursor_or_table:
                if isinstance(item, Mapping):
                    extracted.append(str(item[text_column]))
                else:
                    extracted.append(str(item))
            return extracted
        raise TypeError(f"Unsupported tabular input type for embed_arrow: {type(cursor_or_table).__name__}")

    @staticmethod
    def _run_sync[T](coro: Awaitable[T]) -> T:
        """Execute an awaitable synchronously, bridging safely if an event loop is already running."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None:
            return cast("T", asyncio.run(cast("Any", coro)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return cast("T", pool.submit(asyncio.run, cast("Any", coro)).result())
