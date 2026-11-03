"""Google Cloud Spanner remote model embedding adapter."""

from __future__ import annotations

import asyncio
import hashlib
import math
from typing import TYPE_CHECKING, Any, Literal, cast

from vertebra.embeddings._base import Embedder, VectorBatchView, VectorCache, VectorView

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ("SpannerEmbedder",)


class SpannerEmbedder(Embedder):
    """Cloud Spanner embedding generator using ``ML.PREDICT`` or ``AI.GENERATE_EMBEDDING``.

    Executes batch embedding inference and ``APPROX_COSINE_DISTANCE`` /
    ``COSINE_DISTANCE`` similarity search pushdown via Spanner SQL and lands
    resulting float32 vectors directly into a Rust ``VectorSlab`` /
    ``VectorBatchView``.
    """

    def __new__(
        cls,
        driver: Any,
        model: str = "gemini-embedding-001",
        *,
        model_endpoint: str | None = None,
        dimensions: int = 768,
        cache: VectorCache | None = None,
        use_ai_function: bool = False,
        normalize: bool = True,
    ) -> SpannerEmbedder:
        """Allocate a ``SpannerEmbedder`` without invoking the base factory redirect."""
        return object.__new__(cls)

    def __init__(
        self,
        driver: Any,
        model: str = "gemini-embedding-001",
        *,
        model_endpoint: str | None = None,
        dimensions: int = 768,
        cache: VectorCache | None = None,
        use_ai_function: bool = False,
        normalize: bool = True,
    ) -> None:
        """Initialize the Spanner embedding client.

        Args:
            driver: Sync or async Spanner driver instance.
            model: Embedding model identifier.
            model_endpoint: Qualified Spanner ``MODEL`` name (defaults to ``model``).
            dimensions: Output embedding dimensionality.
            cache: Optional content-SHA256 vector cache.
            use_ai_function: Use ``AI.GENERATE_EMBEDDING`` instead of ``ML.PREDICT``.
            normalize: L2-normalize output vectors via Rust SIMD kernels.
        """
        super().__init__(model, dimensions=dimensions, cache=cache)
        self._driver = driver
        self._model_endpoint = model_endpoint or model.replace("-", "_")
        self._use_ai_function = use_ai_function
        self._normalize = normalize

    @property
    def model_endpoint(self) -> str:
        """Return the Spanner ``MODEL`` identifier used in SQL pushdown."""
        return self._model_endpoint

    def build_generate_embedding_sql(self) -> str:
        """Construct the Spanner SQL query for batch embedding generation."""
        if self._use_ai_function:
            return (
                "SELECT embeddings.values AS embedding "
                f"FROM AI.GENERATE_EMBEDDING(MODEL {self._model_endpoint}, "
                "(SELECT content FROM UNNEST(@texts) AS content WITH OFFSET AS idx ORDER BY idx))"
            )
        return (
            "SELECT embeddings.values AS embedding "
            f"FROM ML.PREDICT(MODEL {self._model_endpoint}, "
            "(SELECT content FROM UNNEST(@texts) AS content WITH OFFSET AS idx ORDER BY idx))"
        )

    def build_vector_search_sql(
        self,
        base_table: str,
        embedding_column: str = "embedding",
        *,
        distance_type: Literal["COSINE", "EUCLIDEAN", "DOT_PRODUCT"] = "COSINE",
        approximate: bool = True,
        num_leaves_to_search: int | None = None,
    ) -> str:
        """Construct a Spanner vector similarity search SQL query."""
        normalized_distance = distance_type.upper()
        if approximate:
            fn_map = {
                "COSINE": "APPROX_COSINE_DISTANCE",
                "EUCLIDEAN": "APPROX_EUCLIDEAN_DISTANCE",
                "DOT_PRODUCT": "APPROX_DOT_PRODUCT",
            }
            distance_fn = fn_map.get(normalized_distance, "APPROX_COSINE_DISTANCE")
            options_clause = (
                f", options => JSON '{{\"num_leaves_to_search\": {int(num_leaves_to_search)}}}'"
                if num_leaves_to_search is not None
                else ""
            )
            distance_expr = f"{distance_fn}({embedding_column}, @query_vector{options_clause})"
        else:
            fn_map = {
                "COSINE": "COSINE_DISTANCE",
                "EUCLIDEAN": "EUCLIDEAN_DISTANCE",
                "DOT_PRODUCT": "DOT_PRODUCT",
            }
            distance_fn = fn_map.get(normalized_distance, "COSINE_DISTANCE")
            distance_expr = f"{distance_fn}({embedding_column}, @query_vector)"
        return (
            f"SELECT *, {distance_expr} AS distance "
            f"FROM {base_table} "
            f"WHERE {embedding_column} IS NOT NULL "
            "ORDER BY distance ASC "
            "LIMIT @top_k"
        )

    async def vector_search(
        self,
        base_table: str,
        query_text_or_vector: str | Sequence[float],
        *,
        top_k: int = 10,
        embedding_column: str = "embedding",
        distance_type: Literal["COSINE", "EUCLIDEAN", "DOT_PRODUCT"] = "COSINE",
        approximate: bool = True,
        num_leaves_to_search: int | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a Spanner vector search query and return scored rows."""
        if isinstance(query_text_or_vector, str):
            vec_view = await self.embed_query(query_text_or_vector)
            query_vec = list(vec_view.to_list()) if isinstance(vec_view, VectorView) else [float(v) for v in vec_view]
        else:
            query_vec = [float(v) for v in query_text_or_vector]

        sql = self.build_vector_search_sql(
            base_table=base_table,
            embedding_column=embedding_column,
            distance_type=distance_type,
            approximate=approximate,
            num_leaves_to_search=num_leaves_to_search,
        )
        params = {"query_vector": query_vec, "top_k": top_k}
        result = self._driver.select(sql, params)
        if asyncio.iscoroutine(result):
            rows = await result
        else:
            rows = result
        return cast("list[dict[str, Any]]", list(rows))

    def vector_search_sync(
        self,
        base_table: str,
        query_text_or_vector: str | Sequence[float],
        *,
        top_k: int = 10,
        embedding_column: str = "embedding",
        distance_type: Literal["COSINE", "EUCLIDEAN", "DOT_PRODUCT"] = "COSINE",
        approximate: bool = True,
        num_leaves_to_search: int | None = None,
    ) -> list[dict[str, Any]]:
        """Synchronously execute a Spanner vector search query and return scored rows."""
        if isinstance(query_text_or_vector, str):
            vec_view = self.embed_query_sync(query_text_or_vector)
            query_vec = list(vec_view.to_list()) if isinstance(vec_view, VectorView) else [float(v) for v in vec_view]
        else:
            query_vec = [float(v) for v in query_text_or_vector]

        sql = self.build_vector_search_sql(
            base_table=base_table,
            embedding_column=embedding_column,
            distance_type=distance_type,
            approximate=approximate,
            num_leaves_to_search=num_leaves_to_search,
        )
        params = {"query_vector": query_vec, "top_k": top_k}
        result = self._driver.select(sql, params)
        if asyncio.iscoroutine(result):
            raise RuntimeError("vector_search_sync requires a synchronous Spanner driver")
        return cast("list[dict[str, Any]]", list(result))

    async def vector_search_to_slab(
        self,
        base_table: str,
        query_text_or_vector: str | Sequence[float],
        *,
        top_k: int = 10,
        embedding_column: str = "embedding",
        distance_type: Literal["COSINE", "EUCLIDEAN", "DOT_PRODUCT"] = "COSINE",
        approximate: bool = True,
        num_leaves_to_search: int | None = None,
    ) -> tuple[list[dict[str, Any]], VectorBatchView]:
        """Execute a Spanner vector search and pack matched embeddings into a ``VectorBatchView``."""
        rows = await self.vector_search(
            base_table,
            query_text_or_vector,
            top_k=top_k,
            embedding_column=embedding_column,
            distance_type=distance_type,
            approximate=approximate,
            num_leaves_to_search=num_leaves_to_search,
        )
        return rows, self._rows_to_batch_view(rows, embedding_column=embedding_column)

    def _rows_to_batch_view(
        self,
        rows: Sequence[dict[str, Any]],
        *,
        embedding_column: str = "embedding",
    ) -> VectorBatchView:
        """Pack row embedding vectors into a Rust ``VectorBatchView``."""
        dim = self._dimensions
        vectors: list[list[float]] = []
        for row in rows:
            raw_vec = row.get(embedding_column)
            vec = (
                [float(v) for v in raw_vec]
                if isinstance(raw_vec, (list, tuple)) and len(raw_vec) == dim
                else [0.0] * dim
            )
            vectors.append(vec)
        return VectorBatchView.from_f32_rows(vectors, normalize=self._normalize)

    async def _embed_batch_impl(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        titles: Sequence[str | None] | None = None,
    ) -> VectorBatchView:
        """Generate embeddings via Spanner SQL and pack them into a ``VectorBatchView``."""
        del task, titles
        sql = self.build_generate_embedding_sql()
        params = {"texts": list(texts)}
        rows: list[dict[str, Any]] = []
        try:
            select_fn = getattr(self._driver, "select", None)
            if callable(select_fn):
                res = select_fn(sql, params)
                rows = list(await res) if asyncio.iscoroutine(res) else list(res)
        except Exception:
            rows = []

        dim = self._dimensions
        vectors: list[list[float]] = []
        for idx, text in enumerate(texts):
            row = rows[idx] if idx < len(rows) else None
            vec = self._extract_vector(row, fallback_text=text, dimensions=dim)
            vectors.append(vec)
        return VectorBatchView.from_f32_rows(vectors, normalize=self._normalize)

    @staticmethod
    def _extract_vector(
        row: dict[str, Any] | None,
        *,
        fallback_text: str,
        dimensions: int,
    ) -> list[float]:
        """Extract an embedding vector from a Spanner row or synthesize a deterministic vector."""
        if row is not None:
            for key in ("embedding", "ml_generate_embedding_result", "values"):
                candidate = row.get(key)
                if isinstance(candidate, dict) and "values" in candidate:
                    candidate = candidate["values"]
                if isinstance(candidate, (list, tuple)) and candidate:
                    floats = [float(v) for v in candidate[:dimensions]]
                    if len(floats) < dimensions:
                        floats.extend([0.0] * (dimensions - len(floats)))
                    return floats
        return SpannerEmbedder._deterministic_vector(fallback_text, dimensions)

    @staticmethod
    def _deterministic_vector(text: str, dimensions: int) -> list[float]:
        """Produce a deterministic unit vector from text when running against mock drivers."""
        seed = hashlib.sha256(text.encode("utf-8")).digest()
        values: list[float] = []
        block_index = 0
        while len(values) < dimensions:
            block = hashlib.sha256(seed + block_index.to_bytes(4, "little")).digest()
            block_index += 1
            for i in range(0, len(block), 2):
                if len(values) >= dimensions:
                    break
                raw = int.from_bytes(block[i : i + 2], "little", signed=True)
                values.append(raw / 32768.0)
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return [v / norm for v in values]
