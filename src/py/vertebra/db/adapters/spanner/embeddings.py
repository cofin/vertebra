"""Spanner SQL embedding generation and vector similarity pushdown."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, Protocol

from vertebra._vertebra import VectorBatchView, VectorView
from vertebra.db.driver import AsyncDatabaseDriver, DataError
from vertebra.utils.serializers import decode_json
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.db.adapters.spanner.driver import AsyncSpannerDriver, SyncSpannerDriver

__all__ = ("SpannerEmbeddingPushdown", "SpannerSelectPort")


class SpannerSelectPort(Protocol):
    """Structural synchronous SQL selection port accepted by ``SpannerEmbeddingPushdown``."""

    def select(self, sql: str, parameters: Any = None, /) -> Sequence[Any]:
        """Execute ``sql`` and return row records."""
        ...


_DISTANCE_FUNCTIONS: dict[bool, dict[str, str]] = {
    True: {
        "COSINE": "APPROX_COSINE_DISTANCE",
        "EUCLIDEAN": "APPROX_EUCLIDEAN_DISTANCE",
        "DOT_PRODUCT": "APPROX_DOT_PRODUCT",
    },
    False: {"COSINE": "COSINE_DISTANCE", "EUCLIDEAN": "EUCLIDEAN_DISTANCE", "DOT_PRODUCT": "DOT_PRODUCT"},
}

_TASK_TO_VERTEX_TASK_TYPE: dict[str, str] = {
    "query": "RETRIEVAL_QUERY",
    "search_query": "RETRIEVAL_QUERY",
    "retrieval_query": "RETRIEVAL_QUERY",
    "document": "RETRIEVAL_DOCUMENT",
    "search_document": "RETRIEVAL_DOCUMENT",
    "retrieval_document": "RETRIEVAL_DOCUMENT",
    "qa": "QUESTION_ANSWERING",
    "question_answering": "QUESTION_ANSWERING",
    "fact": "FACT_VERIFICATION",
    "fact_verification": "FACT_VERIFICATION",
    "classification": "CLASSIFICATION",
    "clustering": "CLUSTERING",
    "semantic_similarity": "SEMANTIC_SIMILARITY",
    "code_retrieval": "CODE_RETRIEVAL_QUERY",
}


class SpannerEmbeddingPushdown:
    """Execute Spanner embedding SQL through a sync or async database driver."""

    def __init__(
        self,
        driver: AsyncSpannerDriver | SyncSpannerDriver | SpannerSelectPort,
        model: str = "gemini-embedding-001",
        *,
        model_endpoint: str | None = None,
        dimensions: int = 768,
        use_ai_function: bool = False,
        normalize: bool = True,
        approximate: bool = True,
        num_leaves_to_search: int | None = None,
    ) -> None:
        self._driver = driver
        self._model_endpoint = model_endpoint or model.replace("-", "_")
        if dimensions <= 0:
            raise ValueError("dimensions must be positive")
        self._dimensions = dimensions
        self._use_ai_function = use_ai_function
        self._normalize = normalize
        self._approximate = approximate
        self._num_leaves_to_search = num_leaves_to_search

    @property
    def driver(self) -> AsyncSpannerDriver | SyncSpannerDriver | SpannerSelectPort:
        """Return the database driver."""
        return self._driver

    @property
    def model_endpoint(self) -> str:
        """Return the SQL model identifier."""
        return self._model_endpoint

    def build_generate_embedding_sql(self) -> str:
        """Construct the Spanner SQL query for batch embedding generation.

        Raises:
            ValueError: When ``model_endpoint`` is not a valid SQL identifier.
        """
        model_endpoint = validate_identifier(self._model_endpoint, "model_endpoint")
        if self._use_ai_function:
            return (
                "SELECT embeddings.values AS embedding, idx "
                f"FROM AI.GENERATE_EMBEDDING(MODEL {model_endpoint}, "
                "(SELECT content, idx FROM UNNEST(@texts) AS content WITH OFFSET AS idx), "
                "STRUCT(TRUE AS flatten_json_output, @task_type AS task_type, @output_dimensionality AS output_dimensionality)"
                ") ORDER BY idx"
            )
        return (
            "SELECT embeddings.values AS embedding, idx "
            f"FROM ML.PREDICT(MODEL {model_endpoint}, "
            "(SELECT content, idx FROM UNNEST(@texts) AS content WITH OFFSET AS idx)) "
            "ORDER BY idx"
        )

    def build_vector_search_sql(
        self,
        base_table: str,
        embedding_column: str = "embedding",
        *,
        distance_type: Literal["COSINE", "EUCLIDEAN", "DOT_PRODUCT"] = "COSINE",
        approximate: bool | None = None,
        num_leaves_to_search: int | None = None,
    ) -> str:
        """Construct a Spanner vector similarity search SQL query.

        Raises:
            ValueError: When ``base_table``, ``embedding_column``, or ``distance_type`` is invalid.
        """
        approximate = self._approximate if approximate is None else approximate
        num_leaves_to_search = self._num_leaves_to_search if num_leaves_to_search is None else num_leaves_to_search
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

    async def embed_documents(
        self, texts: Sequence[str], *, task: str = "document", dimensions: int | None = None, normalize: bool = True
    ) -> VectorBatchView:
        """Generate input-ordered vectors, rejecting incomplete database responses."""
        dims = self._dimensions if dimensions is None else dimensions
        if dims <= 0:
            raise ValueError("dimensions must be positive")
        if not texts:
            raise ValueError("texts must not be empty")
        task_type = _TASK_TO_VERTEX_TASK_TYPE.get(task.strip().lower())
        if task_type is None:
            raise ValueError(f"Unsupported embedding task {task!r}")
        params: dict[str, Any] = {"texts": list(texts)}
        if self._use_ai_function:
            params["task_type"] = task_type
            params["output_dimensionality"] = dims
        rows = await self._select(self.build_generate_embedding_sql(), params)
        if len(rows) != len(texts):
            raise DataError(f"Spanner returned {len(rows)} embedding rows for {len(texts)} texts")
        vectors = [self._extract_vector(row, dims) for row in rows]
        return VectorBatchView.from_f32_rows(vectors, normalize=normalize and self._normalize)

    async def embed_batch(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorBatchView:
        """Embed a batch of texts in a single Spanner SQL query."""
        return await self.embed_documents(texts, task=task, dimensions=dimensions, normalize=normalize)

    async def embed_query(self, text: str, *, dimensions: int | None = None, normalize: bool = True) -> VectorView:
        """Generate a query vector with the retrieval-query task."""
        return (await self.embed_documents([text], task="query", dimensions=dimensions, normalize=normalize))[0]

    async def similarity_search(
        self,
        query_vector: VectorView | Sequence[float],
        *,
        table: str,
        embedding_column: str = "embedding",
        top_k: int = 10,
        metric: str = "cosine",
    ) -> list[dict[str, Any]]:
        """Execute vector search with bound query vectors and limits."""
        if top_k <= 0:
            raise ValueError("top_k must be positive")
        distance_types: dict[str, Literal["COSINE", "EUCLIDEAN", "DOT_PRODUCT"]] = {
            "cosine": "COSINE",
            "euclidean": "EUCLIDEAN",
            "dot": "DOT_PRODUCT",
        }
        if metric not in distance_types:
            raise ValueError(f"Unsupported metric {metric!r}")
        sql = self.build_vector_search_sql(table, embedding_column, distance_type=distance_types[metric])
        values = query_vector.to_list() if isinstance(query_vector, VectorView) else list(query_vector)
        return await self._select(sql, {"query_vector": values, "top_k": top_k})

    async def _select(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if isinstance(self._driver, AsyncDatabaseDriver):
            return list(await self._driver.select(sql, params))
        return list(await asyncio.to_thread(self._driver.select, sql, params))

    @staticmethod
    def _extract_vector(row: Any, dimensions: int) -> list[float]:
        """Decode a vector cell and reject missing or malformed values."""
        candidate: Any = None
        if isinstance(row, Mapping):
            for key in ("embedding", "ml_generate_embedding_result", "values"):
                if key in row:
                    candidate = row[key]
                    break
        elif isinstance(row, (tuple, list)) and row:
            candidate = row[0]
        try:
            if isinstance(candidate, Mapping):
                candidate = candidate.get("values")
            if isinstance(candidate, str):
                candidate = decode_json(candidate)
            if not isinstance(candidate, (list, tuple)) or len(candidate) != dimensions:
                raise ValueError("missing vector")
            result: list[float] = []
            for value in candidate:
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError("invalid vector element")
                result.append(float(value))
            return result
        except (TypeError, ValueError) as exc:
            raise DataError("Spanner embedding row has no valid vector") from exc
