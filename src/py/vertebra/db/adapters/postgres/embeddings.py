"""PostgreSQL and AlloyDB SQL embedding generation and pgvector search."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol

from vertebra._vertebra import VectorBatchView, VectorView, decode_pgvector_f32
from vertebra.db.driver import AsyncDatabaseDriver, DataError
from vertebra.utils.serializers import decode_json
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.db.adapters.postgres.driver import AsyncPostgreSQLDriver, SyncPostgreSQLDriver

__all__ = ("PostgresEmbeddingPushdown", "PostgresSelectPort")


class PostgresSelectPort(Protocol):
    """Structural synchronous SQL selection port accepted by ``PostgresEmbeddingPushdown``."""

    def select(self, sql: str, parameters: Any = None, /) -> Sequence[Any]:
        """Execute ``sql`` and return row records."""
        ...


class PostgresEmbeddingPushdown:
    """Generate vectors with AlloyDB embedding() and search with pgvector."""

    def __init__(
        self,
        driver: AsyncPostgreSQLDriver | SyncPostgreSQLDriver | PostgresSelectPort,
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        normalize: bool = True,
    ) -> None:
        if not model.strip():
            raise ValueError("model must not be empty")
        self._driver = driver
        self._model = model
        if dimensions <= 0:
            raise ValueError("dimensions must be positive")
        self._dimensions = dimensions
        self._normalize = normalize

    @property
    def driver(self) -> AsyncPostgreSQLDriver | SyncPostgreSQLDriver | PostgresSelectPort:
        """Return the database driver."""
        return self._driver

    @property
    def model(self) -> str:
        """Return the model value bound during inference."""
        return self._model

    def sql_expr(self, column: str) -> str:
        """Build an embedding expression for a validated column identifier."""
        validate_identifier(column, "column")
        model = self._model.replace("'", "''")
        return f"embedding('{model}', {column})::vector"

    async def embed_documents(
        self, texts: Sequence[str], *, task: str = "document", dimensions: int | None = None, normalize: bool = True
    ) -> VectorBatchView:
        """Generate input-ordered vectors and decode pgvector binary or text cells."""
        if not texts:
            raise ValueError("texts must not be empty")
        dims = self._dimensions if dimensions is None else dimensions
        if dims <= 0:
            raise ValueError("dimensions must be positive")
        sql = (
            "SELECT embedding($1, t.txt)::vector FROM unnest($2::text[]) WITH ORDINALITY AS t(txt, idx) ORDER BY t.idx"
        )
        rows = await self._select(sql, (self._model, list(texts)))
        if len(rows) != len(texts):
            raise DataError(f"PostgreSQL returned {len(rows)} embedding rows for {len(texts)} texts")
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
        """Embed a batch of texts in a single PostgreSQL SQL query."""
        return await self.embed_documents(texts, task=task, dimensions=dimensions, normalize=normalize)

    async def embed_query(self, text: str, *, dimensions: int | None = None, normalize: bool = True) -> VectorView:
        """Generate one query vector."""
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
        """Search a pgvector column using a bound vector and row limit."""
        if top_k <= 0:
            raise ValueError("top_k must be positive")
        validate_identifier(table, "table")
        validate_identifier(embedding_column, "embedding_column", allow_dots=False)
        operators = {"cosine": "<=>", "euclidean": "<->", "dot": "<#>"}
        if metric not in operators:
            raise ValueError(f"Unsupported metric {metric!r}")
        values = query_vector.to_list() if isinstance(query_vector, VectorView) else list(query_vector)
        sql = (
            f"SELECT *, {embedding_column} {operators[metric]} $1::vector AS distance "
            f"FROM {table} ORDER BY distance ASC LIMIT $2"
        )
        return await self._select(sql, (values, top_k))

    async def _select(self, sql: str, params: Any) -> list[dict[str, Any]]:
        if isinstance(self._driver, AsyncDatabaseDriver):
            return list(await self._driver.select(sql, params))
        return list(await asyncio.to_thread(self._driver.select, sql, params))

    @staticmethod
    def _extract_vector(row: Any, dimensions: int) -> list[float]:
        cell = (
            next(iter(row.values()), None)
            if isinstance(row, Mapping)
            else row[0]
            if isinstance(row, (list, tuple)) and row
            else None
        )
        try:
            if isinstance(cell, (bytes, bytearray, memoryview)):
                cell = decode_pgvector_f32(bytes(cell))
            elif isinstance(cell, str):
                cell = decode_json(cell)
            if not isinstance(cell, (tuple, list)) or len(cell) != dimensions:
                raise ValueError("missing vector")
            result: list[float] = []
            for value in cell:
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError("invalid vector element")
                result.append(float(value))
            return result
        except (TypeError, ValueError) as exc:
            raise DataError("PostgreSQL embedding row has no valid vector") from exc
