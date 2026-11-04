"""Google Cloud Spanner remote model embedding adapter."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

from vertebra.embeddings._base import Embedder, EmbeddingError, VectorBatchView, VectorCache, VectorView
from vertebra.utils.text import validate_identifier

__all__ = ("SpannerEmbedder",)

_DISTANCE_FUNCTIONS: dict[bool, dict[str, str]] = {
    True: {
        "COSINE": "APPROX_COSINE_DISTANCE",
        "EUCLIDEAN": "APPROX_EUCLIDEAN_DISTANCE",
        "DOT_PRODUCT": "APPROX_DOT_PRODUCT",
    },
    False: {"COSINE": "COSINE_DISTANCE", "EUCLIDEAN": "EUCLIDEAN_DISTANCE", "DOT_PRODUCT": "DOT_PRODUCT"},
}


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
        """Construct the Spanner SQL query for batch embedding generation.

        Raises:
            ValueError: When ``model_endpoint`` is not a valid SQL identifier.
        """
        model_endpoint = validate_identifier(self._model_endpoint, "model_endpoint")
        fn_name = "AI.GENERATE_EMBEDDING" if self._use_ai_function else "ML.PREDICT"
        return (
            "SELECT embeddings.values AS embedding, idx "
            f"FROM {fn_name}(MODEL {model_endpoint}, "
            "(SELECT content, idx FROM UNNEST(@texts) AS content WITH OFFSET AS idx)) "
            "ORDER BY idx"
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
        """Construct a Spanner vector similarity search SQL query.

        Raises:
            ValueError: When ``base_table``, ``embedding_column``, or ``distance_type`` is invalid.
        """
        validate_identifier(base_table, "base_table")
        validate_identifier(embedding_column, "embedding_column", allow_dots=False)
        distance_fn = _DISTANCE_FUNCTIONS[approximate].get(distance_type.upper())
        if distance_fn is None:
            msg = f"Unsupported distance_type {distance_type!r}; expected COSINE, EUCLIDEAN, or DOT_PRODUCT"
            raise ValueError(msg)
        options_clause = (
            f", options => JSON '{{\"num_leaves_to_search\": {int(num_leaves_to_search)}}}'"
            if approximate and num_leaves_to_search is not None
            else ""
        )
        distance_expr = f"{distance_fn}({embedding_column}, @query_vector{options_clause})"
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
        """Generate embeddings via Spanner SQL and pack them into a ``VectorBatchView``.

        Raises:
            EmbeddingError: When the row count differs from the text count or a row has no vector.
        """
        del task, titles
        sql = self.build_generate_embedding_sql()
        result = self._driver.select(sql, {"texts": list(texts)})
        rows = list(await result) if asyncio.iscoroutine(result) else list(result)
        if len(rows) != len(texts):
            msg = f"Spanner returned {len(rows)} embedding rows for {len(texts)} texts"
            raise EmbeddingError(msg)
        vectors = [self._extract_vector(row, index=idx, dimensions=self._dimensions) for idx, row in enumerate(rows)]
        return VectorBatchView.from_f32_rows(vectors, normalize=self._normalize)

    @staticmethod
    def _extract_vector(row: Mapping[str, Any], *, index: int, dimensions: int) -> list[float]:
        """Extract an embedding vector from a Spanner result row, zero-padded to ``dimensions``.

        Raises:
            EmbeddingError: When the row has no non-empty vector column.
        """
        for key in ("embedding", "ml_generate_embedding_result", "values"):
            candidate = row.get(key)
            if isinstance(candidate, Mapping) and "values" in candidate:
                candidate = candidate["values"]
            if isinstance(candidate, (list, tuple)) and candidate:
                floats = [float(v) for v in candidate[:dimensions]]
                floats.extend([0.0] * (dimensions - len(floats)))
                return floats
        msg = f"Spanner embedding row {index} has no vector column ('embedding', 'ml_generate_embedding_result', or 'values')"
        raise EmbeddingError(msg)
