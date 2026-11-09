"""Oracle 26ai embedding inference, credential rotation, and vector search."""

from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from vertebra._vertebra import (
    AsyncGcpTokenCache,
    SyncGcpTokenCache,
    VectorBatchView,
    VectorView,
    decode_oracle_vector_f32,
)
from vertebra.db.driver import AsyncDatabaseDriver, DataError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.db.adapters.oracle.driver import AsyncOracleDriver, SyncOracleDriver

__all__ = ("OracleEmbeddingPushdown",)


class OracleEmbeddingPushdown:
    """In-database SQL pushdown embedder for Oracle 26ai `DBMS_VECTOR.UTL_TO_EMBEDDING(S)`."""

    ORACLE_URL_MAX_LEN = 128

    def __init__(
        self,
        driver: AsyncOracleDriver | SyncOracleDriver,
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        provider: str = "vertexai",
        project: str | None = None,
        project_number: str | None = None,
        location: str = "us-central1",
        credential_name: str = "VERTEX_CRED",
        endpoint_url: str | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
        normalize: bool = True,
    ) -> None:
        """Initialize the Oracle 26ai SQL pushdown embedder with `:predict` and 128-byte URL guards."""
        if dimensions <= 0:
            raise ValueError("dimensions must be positive")
        self._dimensions = dimensions
        self._normalize = normalize
        validate_identifier(credential_name, "credential_name", allow_dots=False)
        self._driver = driver
        self._provider = provider.lower()
        self._credential_name = credential_name
        self._token_cache = token_cache
        self._applied_token_generation = 0

        if self._provider == "vertexai":
            resolved_model = model if model.endswith(":predict") else f"{model}:predict"
            proj_id = project_number or project or os.environ.get("GOOGLE_CLOUD_PROJECT") or "default"
            built_url = (
                endpoint_url
                or f"https://{location}-aiplatform.googleapis.com/v1/projects/{proj_id}/locations/{location}/publishers/google/models/"
            )
            if len(built_url.encode("utf-8")) > self.ORACLE_URL_MAX_LEN:
                raise ValueError(
                    f"Oracle 26ai DBMS_VECTOR vertexai endpoint URL length ({len(built_url.encode('utf-8'))}) exceeds the "
                    f"128-byte PL/SQL buffer ceiling (ORA-06502): {built_url!r}. "
                    "Pass a numeric project_number (e.g. project_number='123456789012') instead of a long project slug."
                )
            self._resolved_model_parameter = resolved_model
            self._endpoint_url = built_url
        else:
            self._resolved_model_parameter = model
            self._endpoint_url = endpoint_url or ""

    @property
    def driver(self) -> AsyncOracleDriver | SyncOracleDriver:
        """Return the database driver."""
        return self._driver

    @property
    def resolved_model_parameter(self) -> str:
        """Return the model string passed inside the `DBMS_VECTOR` JSON parameter."""
        return self._resolved_model_parameter

    @property
    def endpoint_url(self) -> str:
        """Return the validated provider endpoint URL (`<= 128` UTF-8 bytes for `vertexai`)."""
        return self._endpoint_url

    @property
    def applied_token_generation(self) -> int:
        """Return the last token cache generation rotated into `DBMS_VECTOR.CREATE_CREDENTIAL`."""
        return self._applied_token_generation

    async def embed_documents(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorBatchView:
        """Execute `DBMS_VECTOR.UTL_TO_EMBEDDINGS` and decode Oracle 26ai `0xDB` binary vectors.

        Raises:
            DataError: When Oracle returns fewer or invalid vectors for the requested texts.
        """
        _ = task
        text_list = list(texts)
        if not text_list:
            raise ValueError("texts must not be empty")
        dims = self._dimensions if dimensions is None else dimensions
        if dims <= 0:
            raise ValueError("dimensions must be positive")
        await self._ensure_oracle_credential_fresh()
        params_dict: dict[str, Any] = {
            "provider": self._provider,
            "credential_name": self._credential_name,
            "model": self._resolved_model_parameter,
            "dimensions": dims,
        }
        if self._endpoint_url:
            params_dict["url"] = self._endpoint_url
        params_json = encode_json(params_dict)

        sql = "SELECT DBMS_VECTOR.UTL_TO_EMBEDDINGS(:texts, JSON(:params)) FROM DUAL"
        rows = await self._select(
            sql,
            {"texts": encode_json(text_list), "params": params_json},
        )
        decoded_rows = self._extract_batch_vectors(rows, len(text_list), dims)
        return VectorBatchView.from_f32_rows(decoded_rows, normalize=normalize and self._normalize)

    async def embed_batch(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorBatchView:
        """Embed a batch of texts in a single `DBMS_VECTOR.UTL_TO_EMBEDDINGS` round trip."""
        return await self.embed_documents(
            texts,
            task=task,
            dimensions=dimensions,
            normalize=normalize,
        )

    async def embed_query(self, text: str, *, dimensions: int | None = None, normalize: bool = True) -> VectorView:
        """Generate a query vector."""
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
        """Search an Oracle VECTOR column using bound vectors and limits."""
        validate_identifier(table, "table")
        validate_identifier(embedding_column, "embedding_column", allow_dots=False)
        metrics = {"cosine": "COSINE", "euclidean": "EUCLIDEAN", "dot": "DOT"}
        if metric not in metrics:
            raise ValueError(f"Unsupported metric {metric!r}")
        values = query_vector.to_list() if isinstance(query_vector, VectorView) else list(query_vector)
        sql = f"SELECT t.*, VECTOR_DISTANCE({embedding_column}, TO_VECTOR(:query_vector), {metrics[metric]}) AS distance FROM {table} t ORDER BY distance ASC FETCH FIRST :top_k ROWS ONLY"
        return await self._select(sql, {"query_vector": encode_json(values), "top_k": top_k})

    async def _ensure_oracle_credential_fresh(self) -> None:
        """Rotate `DBMS_VECTOR.CREATE_CREDENTIAL` when the token cache advances to a new generation."""
        if self._token_cache is None and self._provider == "vertexai":
            opts = {"use_metadata_server": "true"}
            try:
                self._token_cache = AsyncGcpTokenCache(opts)
            except TypeError:
                fallback_mod = import_optional("vertebra.storage._store")
                if fallback_mod is None:
                    return
                self._token_cache = fallback_mod.AsyncGcpTokenCache(opts)
            except ValueError:
                return
        if self._token_cache is None:
            return
        raw_snap = self._token_cache.get_token()
        snap = await raw_snap if inspect.isawaitable(raw_snap) else raw_snap
        gen = int(snap.generation)
        if gen == self._applied_token_generation:
            return
        cred_json = encode_json({"access_token": snap.access_token})
        plsql = (
            "BEGIN DBMS_VECTOR.DROP_CREDENTIAL(credential_name => :cred_name); EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
        create_plsql = (
            "BEGIN DBMS_VECTOR.CREATE_CREDENTIAL(credential_name => :cred_name, params => JSON(:cred_params)); END;"
        )
        await self._execute(plsql, {"cred_name": self._credential_name})
        await self._execute(create_plsql, {"cred_name": self._credential_name, "cred_params": cred_json})
        self._applied_token_generation = int(gen)

    async def _select(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if isinstance(self._driver, AsyncDatabaseDriver):
            return list(await self._driver.select(sql, params))
        return list(await asyncio.to_thread(self._driver.select, sql, params))

    async def _execute(self, sql: str, params: dict[str, Any]) -> None:
        if isinstance(self._driver, AsyncDatabaseDriver):
            await self._driver.execute(sql, params)
        else:
            await asyncio.to_thread(self._driver.execute, sql, params)

    @classmethod
    def _extract_batch_vectors(
        cls,
        rows: Sequence[Any],
        expected_count: int,
        dimensions: int,
    ) -> list[list[float]]:
        if not rows:
            msg = "Oracle DBMS_VECTOR.UTL_TO_EMBEDDINGS returned no embedding rows"
            raise DataError(msg)
        if len(rows) == 1:
            first_row = rows[0]
            cell = (
                cls._extract_mapping_value(first_row)
                if isinstance(first_row, Mapping)
                else first_row[0]
                if isinstance(first_row, (list, tuple)) and first_row
                else first_row
            )
            if isinstance(cell, str):
                try:
                    cell = decode_json(cell)
                except (TypeError, ValueError) as exc:
                    raise DataError("Oracle embedding row has no valid vector") from exc
            if (
                isinstance(cell, (list, tuple))
                and len(cell) == expected_count
                and all(
                    isinstance(
                        item,
                        (list, tuple, bytes, bytearray, memoryview, Mapping, str, VectorView),
                    )
                    for item in cell
                )
            ):
                return [
                    cls._extract_vector(
                        item if isinstance(item, (Mapping, VectorView)) else (item,),
                        dimensions,
                    )
                    for item in cell
                ]
        if len(rows) == expected_count:
            return [cls._extract_vector(row, dimensions) for row in rows]
        msg = f"Oracle DBMS_VECTOR.UTL_TO_EMBEDDINGS returned {len(rows)} rows for {expected_count} requested texts"
        raise DataError(msg)

    @staticmethod
    def _extract_mapping_value(mapping: Mapping[str, Any]) -> Any:
        for key in ("embed_vector", "EMBED_VECTOR", "embed_data", "EMBED_DATA"):
            if key in mapping:
                return mapping[key]
        return next(iter(mapping.values()), None)

    @staticmethod
    def _extract_vector(row: Any, dimensions: int) -> list[float]:
        if isinstance(row, VectorView):
            cell: Any = row.to_list()
        elif isinstance(row, Mapping):
            cell = OracleEmbeddingPushdown._extract_mapping_value(row)
        elif isinstance(row, (list, tuple)) and row:
            cell = row if isinstance(row[0], (int, float)) and not isinstance(row[0], bool) else row[0]
        else:
            cell = row
        try:
            for _ in range(4):
                if isinstance(cell, VectorView):
                    cell = cell.to_list()
                elif isinstance(cell, (bytes, bytearray, memoryview)):
                    cell = decode_oracle_vector_f32(bytes(cell))
                elif isinstance(cell, str):
                    cell = decode_json(cell)
                elif isinstance(cell, Mapping):
                    cell = OracleEmbeddingPushdown._extract_mapping_value(cell)
                else:
                    break
            if not isinstance(cell, (tuple, list)) or len(cell) < dimensions:
                raise ValueError("missing vector")
            result: list[float] = []
            for value in cell[:dimensions]:
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError("invalid vector element")
                result.append(float(value))
            return result
        except (TypeError, ValueError) as exc:
            raise DataError("Oracle embedding row has no valid vector") from exc
