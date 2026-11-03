"""BigQuery SQL pushdown embedder (`ML.GENERATE_EMBEDDING` / `AI.GENERATE_EMBEDDING` and `VECTOR_SEARCH`)."""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from vertebra.embeddings._base import (
    Embedder,
    VectorBatchView,
    VectorCache,
    VectorView,
)
from vertebra.utils.serializers import decode_json

__all__ = ("BigQueryEmbedder",)

_TASK_TO_VERTEX_TASK_TYPE: dict[str, str] = {
    "query": "RETRIEVAL_QUERY",
    "search_query": "RETRIEVAL_QUERY",
    "document": "RETRIEVAL_DOCUMENT",
    "search_document": "RETRIEVAL_DOCUMENT",
    "question_answering": "QUESTION_ANSWERING",
    "fact_verification": "FACT_VERIFICATION",
    "classification": "CLASSIFICATION",
    "clustering": "CLUSTERING",
    "semantic_similarity": "SEMANTIC_SIMILARITY",
    "code_retrieval": "CODE_RETRIEVAL_QUERY",
}


class BigQueryEmbedder(Embedder):
    """In-database Google Cloud BigQuery embedding provider via ``ML.GENERATE_EMBEDDING`` and ``VECTOR_SEARCH``."""

    def __new__(cls, *args: Any, **kwargs: Any) -> BigQueryEmbedder:
        """Allocate a ``BigQueryEmbedder`` instance without triggering the base ``VertexEmbedder`` factory."""
        _ = (args, kwargs)
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
        super().__init__(model, dimensions=dimensions, cache=cache)
        self._driver = driver
        self._model_endpoint = model_endpoint or model
        self._use_ai_function = use_ai_function
        self._normalize = normalize

    @property
    def driver(self) -> Any:
        """Return the underlying BigQuery sync or async driver."""
        return self._driver

    @property
    def model_endpoint(self) -> str:
        """Return the BigQuery remote model identifier passed to ``MODEL `...` ``."""
        return self._model_endpoint

    def build_generate_embedding_sql(self) -> str:
        """Build the parameterized GoogleSQL ``ML.GENERATE_EMBEDDING`` or ``AI.GENERATE_EMBEDDING`` query."""
        fn_name = "AI.GENERATE_EMBEDDING" if self._use_ai_function else "ML.GENERATE_EMBEDDING"
        result_col = "embedding" if self._use_ai_function else "ml_generate_embedding_result"
        return (
            f"SELECT {result_col} AS embedding "
            f"FROM {fn_name}("
            f"MODEL `{self._model_endpoint}`, "
            "(SELECT content FROM UNNEST(@texts) AS content WITH OFFSET AS idx ORDER BY idx), "
            "STRUCT(TRUE AS flatten_json_output, @task_type AS task_type, @output_dimensionality AS output_dimensionality)"
            ")"
        )

    def build_vector_search_sql(
        self,
        base_table: str,
        embedding_column: str = "embedding",
        *,
        distance_type: Literal["COSINE", "EUCLIDEAN", "DOT_PRODUCT"] = "COSINE",
    ) -> str:
        """Build a GoogleSQL ``VECTOR_SEARCH`` query against ``base_table``."""
        dist = distance_type.upper()
        return (
            "SELECT base.*, distance "
            f"FROM VECTOR_SEARCH("
            f"TABLE {base_table}, "
            f"'{embedding_column}', "
            "(SELECT @query_vector AS embedding), "
            "top_k => @top_k, "
            f"distance_type => '{dist}'"
            ")"
        )

    async def vector_search(
        self,
        base_table: str,
        query: str | Sequence[float] | VectorView,
        *,
        embedding_column: str = "embedding",
        top_k: int = 10,
        distance_type: Literal["COSINE", "EUCLIDEAN", "DOT_PRODUCT"] = "COSINE",
    ) -> list[dict[str, Any]]:
        """Execute a BigQuery ``VECTOR_SEARCH`` query using a text or vector query."""
        if isinstance(query, str):
            vec_view = await self.embed_query(query)
            query_floats = list(vec_view.to_list())
        elif isinstance(query, VectorView):
            query_floats = list(query.to_list())
        else:
            query_floats = [float(x) for x in query]

        sql = self.build_vector_search_sql(
            base_table,
            embedding_column=embedding_column,
            distance_type=distance_type,
        )
        params = {
            "query_vector": query_floats,
            "top_k": int(top_k),
        }
        return await self._execute_driver_rows(sql, params)

    def vector_search_sync(
        self,
        base_table: str,
        query: str | Sequence[float] | VectorView,
        *,
        embedding_column: str = "embedding",
        top_k: int = 10,
        distance_type: Literal["COSINE", "EUCLIDEAN", "DOT_PRODUCT"] = "COSINE",
    ) -> list[dict[str, Any]]:
        """Synchronously execute a BigQuery ``VECTOR_SEARCH`` query."""
        return self._run_sync(
            self.vector_search(
                base_table,
                query,
                embedding_column=embedding_column,
                top_k=top_k,
                distance_type=distance_type,
            )
        )

    async def _embed_batch_impl(
        self,
        texts: list[str],
        *,
        task: str = "document",
        titles: list[str | None] | None = None,
    ) -> VectorBatchView:
        sql = self.build_generate_embedding_sql()
        vertex_task = _TASK_TO_VERTEX_TASK_TYPE.get(task.lower(), "RETRIEVAL_DOCUMENT")
        params: dict[str, Any] = {
            "texts": list(texts),
            "task_type": vertex_task,
            "output_dimensionality": int(self._dimensions),
        }
        rows = await self._execute_driver_rows(sql, params)
        vectors: list[list[float]] = []
        for row in rows:
            raw_vec = _extract_embedding_from_row(row)
            vectors.append(raw_vec[: self._dimensions])
        return VectorBatchView.from_f32_rows(vectors, normalize=self._normalize)

    async def _execute_driver_rows(
        self,
        sql: str,
        params: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        if hasattr(self._driver, "select"):
            res = self._driver.select(sql, params)
            if inspect.isawaitable(res):
                res = await res
            return list(res)
        if hasattr(self._driver, "fetch_all"):
            res = self._driver.fetch_all(sql, params)
            if inspect.isawaitable(res):
                res = await res
            return [
                dict(r) if isinstance(r, Mapping) else {"embedding": r[0] if isinstance(r, (list, tuple)) else r}
                for r in res
            ]
        raise TypeError(
            f"Unsupported BigQuery driver {type(self._driver).__name__!r}; expected select() or fetch_all()"
        )


def _extract_embedding_from_row(row: Any) -> list[float]:
    if isinstance(row, Mapping):
        candidate = row.get("embedding") or row.get("ml_generate_embedding_result") or next(iter(row.values()), ())
    elif isinstance(row, (list, tuple)):
        candidate = row[0] if row else ()
    else:
        candidate = row

    if isinstance(candidate, str):
        parsed = decode_json(candidate)
        return [float(x) for x in parsed]
    if isinstance(candidate, Sequence):
        return [float(x) for x in candidate]
    return []
