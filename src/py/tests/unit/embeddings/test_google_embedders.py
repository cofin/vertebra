"""Unit tests for Google-first zero-weight embedders, gRPC packed-float framing, and SQL pushdown."""

from __future__ import annotations

import asyncio
import base64
import json
import math
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

import vertebra
from vertebra._vertebra import (
    ArrowCursor,
    AsyncGcpIdTokenCache,
    AsyncGcpTokenCache,
    GcpIdTokenSnapshot,
    GcpTokenSnapshot,
    GoogleCloudAuthError,
    SyncGcpIdTokenCache,
    SyncGcpTokenCache,
    VectorBatchView,
    VectorCache,
    VectorView,
    encode_gemini_embed_grpc_response_frame,
    encode_oracle_vector_f32,
    encode_pgvector_f32,
)
from vertebra.db.adapters.bigquery import BigQueryEmbeddingPushdown
from vertebra.db.adapters.oracle import OracleEmbeddingPushdown
from vertebra.db.adapters.postgres import PostgresEmbeddingPushdown
from vertebra.db.adapters.spanner import SpannerEmbeddingPushdown
from vertebra.db.driver import AsyncDatabaseDriver, DataError, EmbeddingPushdown
from vertebra.embeddings import (
    Embedder,
    EmbeddingError,
    GeminiEmbedder,
    MultimodalPart,
    PushdownEmbedder,
    RemoteEmbedder,
    VertexEmbedder,
    create_embedding_grpc_service,
)
from vertebra.embeddings._models import get_model_spec
from vertebra.embeddings._remote import RemoteEmbedResponse
from vertebra.extensions.grpc import encode_lpm_frame
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import encode_msgpack_bytes


class _FakeTokenCache:
    """In-memory fake AsyncGcpTokenCache returning GcpTokenSnapshot with generation rotation."""

    def __init__(self, prefix: str = "mock-vertex-token", quota_project: str | None = "quota-proj-123") -> None:
        self._prefix = prefix
        self._quota_project = quota_project
        self._generation = 0
        self._invalidated_up_to = 0

    def invalidate(self, failed_generation: int) -> None:
        if failed_generation >= self._invalidated_up_to:
            self._invalidated_up_to = failed_generation

    async def get_token(self, *, force_refresh: bool = False) -> GcpTokenSnapshot:
        if force_refresh or self._generation == 0 or self._generation <= self._invalidated_up_to:
            self._generation += 1
        return GcpTokenSnapshot(
            access_token=f"{self._prefix}-{self._generation}",
            expires_at_unix_ms=4_102_444_800_000,
            generation=self._generation,
            quota_project_id=self._quota_project,
        )


def test_clean_domain_naming_and_default_factory() -> None:
    """Verify Embedder() returns VertexEmbedder('gemini-embedding-001', dimensions=768) and exports clean names."""
    emb = Embedder()
    assert isinstance(emb, VertexEmbedder)
    assert isinstance(emb, Embedder)
    assert emb.model == "gemini-embedding-001"
    assert emb.dimensions == 768

    assert vertebra.Embedder is Embedder
    assert vertebra.VectorView is VectorView
    assert vertebra.VectorBatchView is VectorBatchView
    assert "vertebra.embeddings" in vertebra.__lazy_modules__
    assert all(
        not exported_name.endswith("Bridge") and "GrpcBridge" not in exported_name
        for exported_name in vertebra.embeddings.__all__
    )


@pytest.mark.anyio
async def test_vertex_embedder_grpc_microbatching_adc_headers_and_401_retry() -> None:
    """Verify VertexEmbedder coalesces concurrent embed_query calls onto Vertex REST predict, attaches ADC + quota project, and retries 401."""
    token_cache: Any = _FakeTokenCache(prefix="mock-vertex-token", quota_project="quota-proj-123")

    transport_calls: list[tuple[str, dict[str, str], bytes]] = []

    async def fake_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        transport_calls.append((url, dict(headers), body))
        if len(transport_calls) == 1:
            return (401, {"content-type": "application/json"}, b"")
        req_payload = json.loads(body.decode("utf-8"))
        assert len(req_payload["instances"]) == 1
        text = req_payload["instances"][0]["content"]
        vec = [3.0, 4.0, 0.0, 0.0] if text == "first query" else [0.0, 3.0, 4.0, 0.0]
        resp_body = json.dumps({"predictions": [{"embeddings": {"values": vec}}]}).encode("utf-8")
        return (200, {"content-type": "application/json"}, resp_body)

    vec_cache = VectorCache(capacity=64, dimensions=4)
    embedder = VertexEmbedder(
        "gemini-embedding-001",
        dimensions=4,
        project="test-gcp-project",
        location="us-central1",
        token_cache=token_cache,
        cache=vec_cache,
        transport=fake_transport,
        coalesce_window_ms=5.0,
    )

    v1, v2 = await asyncio.gather(
        embedder.embed_query("first query"),
        embedder.embed_query("second query"),
    )
    assert len(transport_calls) == 3
    assert all(
        url
        == "https://us-central1-aiplatform.googleapis.com/v1/projects/test-gcp-project/locations/us-central1/publishers/google/models/gemini-embedding-001:predict"
        for url, _, _ in transport_calls
    )
    assert transport_calls[0][1]["authorization"] == "Bearer mock-vertex-token-1"
    assert transport_calls[0][1]["x-goog-user-project"] == "quota-proj-123"
    assert transport_calls[0][1]["content-type"] == "application/json"
    assert transport_calls[-1][1]["authorization"] == "Bearer mock-vertex-token-2"
    assert v1.dimensions == 4
    assert v2.dimensions == 4
    norm1 = math.sqrt(sum(x * x for x in v1.to_list()))
    norm2 = math.sqrt(sum(x * x for x in v2.to_list()))
    assert norm1 == pytest.approx(1.0, rel=1e-5)
    assert norm2 == pytest.approx(1.0, rel=1e-5)

    cached_v1 = await embedder.embed_query("first query")
    assert len(transport_calls) == 3
    assert cached_v1.to_list() == pytest.approx(v1.to_list(), rel=1e-6)


@pytest.mark.anyio
async def test_gemini_embedder_api_key_multimodal_and_embed_arrow() -> None:
    """Verify GeminiEmbedder supports api_key, MultimodalPart inputs, and embed_arrow SHM output."""
    recorded_headers: list[dict[str, str]] = []

    async def fake_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        recorded_headers.append(dict(headers))
        src_batch = VectorBatchView.from_f32_rows(
            [[1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0]],
            normalize=True,
        )
        return (
            200,
            {"content-type": "application/grpc+proto", "grpc-status": "0"},
            encode_gemini_embed_grpc_response_frame(src_batch),
        )

    embedder = GeminiEmbedder(
        "gemini-embedding-2",
        dimensions=4,
        api_key="mock-gemini-api-key",
        transport=fake_transport,
    )
    part = MultimodalPart(mime_type="image/png", data=b"\x89PNG\r\n", text="diagram")
    batch = await embedder.embed_multimodal([part, "plain text"])
    assert isinstance(batch, VectorBatchView)
    assert batch.count == 2
    assert batch.dimensions == 4
    assert recorded_headers[0]["x-goog-api-key"] == "mock-gemini-api-key"

    arrow_cursor = await embedder.embed_arrow(
        [{"text": "alpha"}, {"text": "beta"}],
        text_column="text",
        output_column="embedding",
    )
    assert isinstance(arrow_cursor, ArrowCursor)
    arrow_cursor.release()


@pytest.mark.anyio
async def test_remote_embedder_and_grpc_service_roundtrip() -> None:
    """Verify RemoteEmbedder roundtrips embed_query and embed_many through create_embedding_grpc_service."""

    class LocalTestEmbedder(Embedder):
        def __init__(self) -> None:
            super().__init__(model="test-local", dimensions=4)

        async def _embed_batch_impl(
            self,
            texts: list[str],
            *,
            task: str = "document",
            titles: list[str | None] | None = None,
        ) -> VectorBatchView:
            rows = [[float(idx + 1), 1.0, 2.0, 3.0] for idx in range(len(texts))]
            return VectorBatchView.from_f32_rows(rows, normalize=True)

    backing = LocalTestEmbedder()
    registry = create_embedding_grpc_service(backing)

    async def asgi_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        path = "/" + url.split("://", 1)[-1].split("/", 1)[-1]
        sent_chunks: list[bytes] = []
        resp_headers: dict[str, str] = {}
        resp_status = 200
        req_consumed = False

        async def receive() -> dict[str, Any]:
            nonlocal req_consumed
            if not req_consumed:
                req_consumed = True
                return {"type": "http.request", "body": body, "more_body": False}
            return {"type": "http.disconnect"}

        async def send(event: dict[str, Any]) -> None:
            nonlocal resp_status
            if event["type"] == "http.response.start":
                resp_status = event.get("status", 200)
                for k, v in event.get("headers", []):
                    resp_headers[k.decode("latin1").lower()] = v.decode("latin1")
            elif event["type"] == "http.response.body":
                chunk = event.get("body", b"")
                if chunk:
                    sent_chunks.append(chunk)
            elif event["type"] == "http.response.trailers":
                for k, v in event.get("headers", []):
                    resp_headers[k.decode("latin1").lower()] = v.decode("latin1")

        scope = {
            "type": "http",
            "path": path,
            "headers": [(k.encode("latin1"), v.encode("latin1")) for k, v in headers.items()],
        }
        await registry.handle_asgi(scope, receive, send)
        return (resp_status, resp_headers, b"".join(sent_chunks))

    remote = RemoteEmbedder(
        endpoint="http://127.0.0.1:8000",
        model="test-local",
        dimensions=4,
        transport=asgi_transport,
    )
    q_vec = await remote.embed_query("hello cluster")
    assert q_vec.dimensions == 4
    batch = await remote.embed_many(["one", "two", "three"])
    assert batch.count == 3
    assert batch.dimensions == 4
    matches = remote.similarity_search(q_vec, batch, top_k=2)
    assert len(matches) == 2


class _SelectPort:
    """External database port with observable calls and configured row responses."""

    def __init__(self, rows: Any) -> None:
        self.rows = rows
        self.calls: list[tuple[str, Any, int]] = []

    def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> Any:
        _ = kwargs
        self.calls.append((sql, parameters, threading.get_ident()))
        return self.rows

    def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> Any:
        _ = kwargs
        self.calls.append((sql, parameters, threading.get_ident()))


class _AsyncSelectPort(AsyncDatabaseDriver[Any]):
    """Async database port implementing the real driver dispatch boundary."""

    def __init__(self, rows: Any) -> None:
        self.rows = rows
        self.calls: list[tuple[str, Any, int]] = []

    async def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> Any:
        _ = kwargs
        self.calls.append((sql, parameters, threading.get_ident()))
        return self.rows

    async def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> Any:
        _ = kwargs
        self.calls.append((sql, parameters, threading.get_ident()))


_PROVIDERS = (BigQueryEmbeddingPushdown, SpannerEmbeddingPushdown, PostgresEmbeddingPushdown, OracleEmbeddingPushdown)


@pytest.mark.anyio
@pytest.mark.parametrize("provider", _PROVIDERS)
@pytest.mark.parametrize("port_type", [_SelectPort, _AsyncSelectPort])
async def test_pushdown_capability_dispatch_and_order(provider: Any, port_type: Any) -> None:
    """Every provider returns typed vectors through select with sync offloading."""
    port = port_type([{"embedding": [3.0, 4.0, 0.0, 0.0]}])
    options = {"provider": "local"} if provider is OracleEmbeddingPushdown else {}
    pushdown = provider(port, dimensions=4, **options)
    assert isinstance(pushdown, EmbeddingPushdown)
    pushdown_any: Any = pushdown
    assert pushdown_any.driver is port
    vector = await pushdown.embed_query("query", normalize=False)
    assert vector.to_list() == [3.0, 4.0, 0.0, 0.0]
    port.rows = [{"embedding": [3.0, 4.0]}]
    batch = await pushdown.embed_documents(["document"], dimensions=2, normalize=True)
    assert batch.to_list() == [pytest.approx([0.6, 0.8])]
    assert all(
        (thread_id == threading.get_ident()) == (port_type is _AsyncSelectPort) for _, _, thread_id in port.calls
    )
    if provider is not OracleEmbeddingPushdown:
        assert "ORDER BY" in port.calls[0][0]


@pytest.mark.anyio
@pytest.mark.parametrize("provider", _PROVIDERS)
@pytest.mark.parametrize(
    "rows",
    [[], [{"embedding": []}], [{"embedding": ["bad"]}], [{"other": 1}], [{"embedding": None}], [{"embedding": [1.0]}]],
)
async def test_pushdown_rejects_invalid_vectors(provider: Any, rows: Any) -> None:
    """Database data failures remain DataError at the capability boundary."""
    options = {"provider": "local"} if provider is OracleEmbeddingPushdown else {}
    pushdown = provider(_SelectPort(rows), dimensions=4, **options)
    with pytest.raises(DataError):
        await pushdown.embed_query("bad")
    with pytest.raises(EmbeddingError):
        await PushdownEmbedder(pushdown, dimensions=4).embed_query("bad")
    with pytest.raises(EmbeddingError):
        await PushdownEmbedder(pushdown, dimensions=4).embed_document("bad")


@pytest.mark.anyio
@pytest.mark.parametrize("provider", _PROVIDERS)
@pytest.mark.parametrize("metric", ["cosine", "euclidean", "dot"])
async def test_pushdown_search_parameters_and_validation(provider: Any, metric: str) -> None:
    """Search binds vectors and limits and rejects unsafe identifiers and metrics."""
    rows = [{"id": 42, "distance": 0.25}]
    port = _SelectPort(rows)
    options = {"provider": "local"} if provider is OracleEmbeddingPushdown else {}
    pushdown = provider(port, **options)
    assert await pushdown.similarity_search([1.0, 2.0], table="docs", top_k=3, metric=metric) == rows
    sql, params, _ = port.calls[-1]
    assert "docs" in sql and 3 in params.values() if isinstance(params, dict) else 3 in params
    if provider is PostgresEmbeddingPushdown:
        assert {"cosine": "<=>", "euclidean": "<->", "dot": "<#>"}[metric] in sql


@pytest.mark.anyio
@pytest.mark.parametrize("provider", _PROVIDERS)
@pytest.mark.parametrize(
    "invalid_kwargs",
    [
        {"table": "docs; DROP TABLE docs"},
        {"table": "docs", "embedding_column": "x.y"},
        {"table": "docs", "metric": "invalid"},
    ],
    ids=["bad-table", "dotted-column", "bad-metric"],
)
async def test_pushdown_search_rejects_invalid_arguments(
    provider: Any,
    invalid_kwargs: dict[str, Any],
) -> None:
    """Similarity search rejects unsafe table/column identifiers and unsupported metrics."""
    options = {"provider": "local"} if provider is OracleEmbeddingPushdown else {}
    pushdown = provider(_SelectPort([]), **options)
    with pytest.raises(ValueError):
        await pushdown.similarity_search([1.0], **invalid_kwargs)


@pytest.mark.anyio
async def test_pushdown_embedder_inherited_surfaces_and_cache() -> None:
    """The facade preserves query/document dispatch, cache, Arrow, and SIMD search."""
    port = _SelectPort([{"embedding": [3.0, 4.0, 0.0, 0.0]}])
    pushdown = BigQueryEmbeddingPushdown(port, dimensions=4)
    cache = VectorCache(dimensions=4, capacity=32)
    embedder = PushdownEmbedder(pushdown, model="gemini-embedding-001", dimensions=4, cache=cache)
    assert embedder.pushdown is pushdown and embedder.normalize is True
    assert isinstance(embedder, Embedder)
    vector = await embedder.embed_query("query")
    assert port.calls[-1][1]["task_type"] == "RETRIEVAL_QUERY"
    assert embedder.embed_query_sync("query").to_list() == vector.to_list()
    assert len(port.calls) == 1 and embedder.last_usage is not None
    assert (await embedder.embed_document("doc", title="Title")).dimensions == 4
    assert port.calls[-1][1]["task_type"] == "RETRIEVAL_DOCUMENT"
    assert embedder.embed_document_sync("sync").dimensions == 4
    assert (await embedder.embed_many(["many"])).count == 1
    assert embedder.embed_many_sync(["sync-many"]).count == 1
    assert isinstance(await embedder.embed_arrow([{"text": "arrow"}]), ArrowCursor)
    candidates = VectorBatchView.from_f32_rows([[0.6, 0.8, 0, 0]], normalize=False)
    assert len(embedder.similarity_search(vector, candidates, top_k=1)) == 1
    port.rows = [{"id": 7}]
    assert await embedder.vector_search(vector, table="docs") == [{"id": 7}]
    assert await embedder.vector_search("query", table="docs") == [{"id": 7}]


def test_postgres_sql_expression_model_binding_and_clean_break() -> None:
    """Model strings are SQL values; only column identifiers are interpolated."""
    pushdown = PostgresEmbeddingPushdown(_SelectPort([]), model="gemini's-model")
    assert pushdown.sql_expr("content") == "embedding('gemini''s-model', content)::vector"
    with pytest.raises(ValueError):
        pushdown.sql_expr("content); DROP TABLE docs")
    with pytest.raises(ValueError):
        PostgresEmbeddingPushdown(_SelectPort([]), model="")
    assert all(
        not hasattr(vertebra.embeddings, name)
        for name in ("BigQueryEmbedder", "SpannerEmbedder", "AlloyDbEmbedder", "OracleEmbedder")
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import vertebra.embeddings as e; assert not any(k.startswith('vertebra.embeddings._') for k in sys.modules); assert e.PushdownEmbedder.__name__ == 'PushdownEmbedder'",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.anyio
@pytest.mark.parametrize("port_type", [_SelectPort, _AsyncSelectPort])
@pytest.mark.parametrize("async_cache", [False, True])
async def test_oracle_url_bytes_and_token_rotation(port_type: Any, async_cache: bool) -> None:
    """Oracle credentials rotate across both driver and token cache dispatch modes."""

    class SyncTokenPort:
        def __init__(self) -> None:
            self.generation = 1

        def get_token(self) -> GcpTokenSnapshot:
            return GcpTokenSnapshot(
                access_token=f"token-{self.generation}",
                expires_at_unix_ms=4102444800000,
                generation=self.generation,
                quota_project_id=None,
            )

    class AsyncTokenPort:
        def __init__(self) -> None:
            self.generation = 1

        async def get_token(self) -> GcpTokenSnapshot:
            return GcpTokenSnapshot(
                access_token=f"token-{self.generation}",
                expires_at_unix_ms=4102444800000,
                generation=self.generation,
                quota_project_id=None,
            )

    token_cache: Any = AsyncTokenPort() if async_cache else SyncTokenPort()
    port = port_type([{"embedding": encode_oracle_vector_f32([3, 4, 0, 0])}])
    pushdown = OracleEmbeddingPushdown(port, project_number="123", dimensions=4, token_cache=token_cache)
    assert pushdown.resolved_model_parameter == "gemini-embedding-001:predict"
    await pushdown.embed_query("first")
    await pushdown.embed_query("second")
    assert sum("CREATE_CREDENTIAL" in sql for sql, _, _ in port.calls) == 1
    token_cache.generation = 2
    await pushdown.embed_query("third")
    assert pushdown.applied_token_generation == 2
    assert sum("CREATE_CREDENTIAL" in sql for sql, _, _ in port.calls) == 2
    assert sum("DROP_CREDENTIAL" in sql for sql, _, _ in port.calls) == 2
    with pytest.raises(ValueError, match="128"):
        OracleEmbeddingPushdown(port, endpoint_url="https://" + "é" * 61)
    with pytest.raises(ValueError):
        OracleEmbeddingPushdown(port, credential_name="bad.name")


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("provider", "cell"),
    [
        (BigQueryEmbeddingPushdown, "[3, 4, 0, 0]"),
        (BigQueryEmbeddingPushdown, [3, 4, 0, 0]),
        (SpannerEmbeddingPushdown, {"values": [3, 4, 0, 0]}),
        (SpannerEmbeddingPushdown, [3, 4, 0, 0]),
        (PostgresEmbeddingPushdown, "[3, 4, 0, 0]"),
        (PostgresEmbeddingPushdown, encode_pgvector_f32([3, 4, 0, 0])),
        (OracleEmbeddingPushdown, "[3, 4, 0, 0]"),
        (OracleEmbeddingPushdown, encode_oracle_vector_f32([3, 4, 0, 0])),
    ],
)
@pytest.mark.parametrize("row_shape", ["mapping", "tuple"])
@pytest.mark.parametrize("port_type", [_SelectPort, _AsyncSelectPort])
async def test_pushdown_decodes_supported_vector_cells(
    provider: Any, cell: Any, row_shape: str, port_type: Any
) -> None:
    """Each engine decodes its public row and vector formats into native views."""
    options = {"provider": "local"} if provider is OracleEmbeddingPushdown else {}
    port = port_type([{"embedding": cell} if row_shape == "mapping" else (cell,)])
    adapter = provider(port, dimensions=4, **options)
    assert (await adapter.embed_query("decode", normalize=False)).to_list() == [3, 4, 0, 0]
    if provider is PostgresEmbeddingPushdown:
        assert port.calls[0][1] == ("gemini-embedding-001", ["decode"])
        assert "embedding($1, t.txt)::vector" in port.calls[0][0]
        assert port.calls[0][0].endswith("ORDER BY t.idx")


class _CapabilityPort:
    """External capability port recording the facade's query and document calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any, dict[str, Any]]] = []

    async def embed_query(self, text: str, **kwargs: Any) -> VectorView:
        self.calls.append(("query", text, kwargs))
        return VectorView.from_f32([1, 0, 0, 0])

    async def embed_documents(self, texts: Any, **kwargs: Any) -> VectorBatchView:
        self.calls.append(("documents", texts, kwargs))
        return VectorBatchView.from_f32_rows([[1, 0, 0, 0] for _ in texts], normalize=False)

    async def similarity_search(self, query_vector: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(("search", query_vector, kwargs))
        return [{"id": 9}]


@pytest.mark.anyio
async def test_facade_dispatches_query_and_document_capability_methods() -> None:
    """Query and document APIs use the declared capability with caller options."""
    capability = _CapabilityPort()
    embedder = PushdownEmbedder(capability, dimensions=4, normalize=False)
    await embedder.embed_query("q")
    embedder.embed_query_sync("qs")
    await embedder.embed_document("d", title="T")
    embedder.embed_document_sync("ds")
    await embedder.embed_many(["m"])
    embedder.embed_many_sync(["ms"])
    await embedder.embed_arrow([{"text": "a"}])
    assert [kind for kind, _, _ in capability.calls] == ["query", "query"] + ["documents"] * 5
    assert all(kwargs["dimensions"] == 4 and kwargs["normalize"] is False for _, _, kwargs in capability.calls)
    assert all(kwargs["task"] == "document" for kind, _, kwargs in capability.calls if kind == "documents")
    assert await embedder.vector_search("s", table="docs", top_k=2, metric="dot") == [{"id": 9}]
    assert capability.calls[-1][2] == {"table": "docs", "embedding_column": "embedding", "top_k": 2, "metric": "dot"}


@pytest.mark.anyio
async def test_embed_document_and_embed_many_vector_cache_parity_and_partial_misses() -> None:
    """embed_document and embed_many share title-scoped VectorCache keys and record cached_input_tokens."""
    batch_calls: list[tuple[list[str], str, list[str | None] | None]] = []

    class CountingEmbedder(Embedder):
        def __init__(self, cache: VectorCache) -> None:
            super().__init__(model="test-cache-model", dimensions=4, cache=cache)

        async def _embed_batch_impl(
            self,
            texts: list[str],
            *,
            task: str = "document",
            titles: list[str | None] | None = None,
        ) -> VectorBatchView:
            batch_calls.append((list(texts), task, list(titles) if titles is not None else None))
            rows = [
                [
                    float(len(txt)),
                    float(len(title_val) if titles and (title_val := titles[idx]) else 0),
                    1.0,
                    0.0,
                ]
                for idx, txt in enumerate(texts)
            ]
            return VectorBatchView.from_f32_rows(rows, normalize=False)

    cache = VectorCache(capacity=64, dimensions=4)
    emb = CountingEmbedder(cache)

    v_doc_a = await emb.embed_document("body-text", title="DocA")
    v_doc_b = await emb.embed_document("body-text", title="DocB")
    v_doc_none = await emb.embed_document("body-text")
    assert v_doc_a.to_list() == [9.0, 4.0, 1.0, 0.0]
    assert v_doc_b.to_list() == [9.0, 4.0, 1.0, 0.0]
    assert v_doc_none.to_list() == [9.0, 0.0, 1.0, 0.0]
    assert len(batch_calls) == 3

    full_hit = await emb.embed_many(["body-text", "body-text"], titles=["DocA", None])
    assert len(batch_calls) == 3
    assert full_hit.to_list() == [[9.0, 4.0, 1.0, 0.0], [9.0, 0.0, 1.0, 0.0]]
    assert emb.last_usage is not None
    assert emb.last_usage.input_tokens == 0
    assert emb.last_usage.cached_input_tokens > 0

    partial_hit = await emb.embed_many(
        ["body-text", "fresh-body", "body-text"],
        titles=["DocA", "FreshTitle", None],
    )
    assert len(batch_calls) == 4
    assert batch_calls[-1] == (["fresh-body"], "document", ["FreshTitle"])
    assert partial_hit.to_list() == [
        [9.0, 4.0, 1.0, 0.0],
        [10.0, 10.0, 1.0, 0.0],
        [9.0, 0.0, 1.0, 0.0],
    ]
    assert emb.last_usage is not None
    assert emb.last_usage.input_tokens > 0
    assert emb.last_usage.cached_input_tokens > 0


@pytest.mark.anyio
async def test_pushdown_embedder_preserves_document_titles() -> None:
    """PushdownEmbedder prefixes non-empty document titles before delegating to embed_documents."""
    capability = _CapabilityPort()
    embedder = PushdownEmbedder(capability, dimensions=4, normalize=False)
    await embedder.embed_document("b1", title="T1")
    await embedder.embed_many(["b1", "b2"], titles=["T1", None])
    assert capability.calls[0] == (
        "documents",
        ["T1: b1"],
        {"task": "document", "dimensions": 4, "normalize": False},
    )
    assert capability.calls[1] == (
        "documents",
        ["T1: b1", "b2"],
        {"task": "document", "dimensions": 4, "normalize": False},
    )


@pytest.mark.anyio
@pytest.mark.parametrize("driver_kind", ["sync", "async"])
async def test_postgres_embedding_pushdown_placeholder_selection(driver_kind: str) -> None:
    """PostgresEmbeddingPushdown unconditionally emits native $1/$2 positional parameters."""
    rows = [{"embedding": [1.0, 0.0, 0.0, 0.0]}]
    port: Any = _SelectPort(rows) if driver_kind == "sync" else _AsyncSelectPort(rows)
    pushdown = PostgresEmbeddingPushdown(port, dimensions=4, normalize=False)
    await pushdown.embed_query("alpha")
    assert "embedding($1, t.txt)::vector FROM unnest($2::text[])" in port.calls[-1][0]


@pytest.mark.anyio
async def test_embedder_default_factory_initializes_once_and_run_sync_reuses_portal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Embedder() invokes VertexEmbedder.__init__ once and _run_sync reuses one background loop thread."""
    orig_init = VertexEmbedder.__init__
    init_calls = 0

    def counting_init(self: VertexEmbedder, *args: Any, **kwargs: Any) -> None:
        nonlocal init_calls
        init_calls += 1
        orig_init(self, *args, **kwargs)

    monkeypatch.setattr(VertexEmbedder, "__init__", counting_init)
    emb = Embedder("gemini-embedding-001", dimensions=256)
    assert isinstance(emb, VertexEmbedder)
    assert init_calls == 1

    caller_tid = threading.get_ident()

    async def probe() -> tuple[int, int]:
        return (threading.get_ident(), id(asyncio.get_running_loop()))

    tid1, loop_id1 = Embedder._run_sync(probe())
    tid2, loop_id2 = Embedder._run_sync(probe())
    assert tid1 != caller_tid
    assert tid1 == tid2
    assert loop_id1 == loop_id2


@pytest.mark.anyio
async def test_gemini_embedder_multimodal_preserves_full_inline_data_and_rust_http2_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GeminiEmbedder.embed_multimodal encodes full inline media payloads without 64-char truncation."""
    recorded_bodies: list[bytes] = []

    async def fake_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        recorded_bodies.append(body)
        src_batch = VectorBatchView.from_f32_rows([[1.0, 0.0, 0.0, 0.0]], normalize=True)
        return (
            200,
            {"content-type": "application/grpc+proto", "grpc-status": "0"},
            encode_gemini_embed_grpc_response_frame(src_batch),
        )

    embedder = GeminiEmbedder(
        "gemini-embedding-2",
        dimensions=4,
        api_key="mock-gemini-api-key",
        transport=fake_transport,
    )
    raw_media = b"\x89PNG\r\n\x1a\n" + (b"VERTEBRA_INLINE_MEDIA_CHUNK_" * 6)
    expected_b64 = base64.b64encode(raw_media).decode("ascii").encode("ascii")
    assert len(expected_b64) > 128

    part = MultimodalPart(mime_type="image/png", data=raw_media, text="architecture-diagram")
    batch = await embedder.embed_multimodal([part])
    assert batch.count == 1
    assert raw_media in recorded_bodies[0]
    assert b"architecture-diagram" in recorded_bodies[0]
    assert expected_b64 not in recorded_bodies[0]

    assert hasattr(vertebra._vertebra, "gcp_http2_post_async")
    assert hasattr(vertebra._vertebra, "gcp_http2_post_sync")

    called_native: list[tuple[str, dict[str, str], bytes]] = []

    async def fake_native_http2(
        url: str,
        headers: dict[str, str],
        body: bytes,
        timeout_ms: int = 30_000,
    ) -> tuple[int, dict[str, str], bytes]:
        _ = timeout_ms
        called_native.append((url, dict(headers), body))
        resp_body = json.dumps({"predictions": [{"embeddings": {"values": [0.0, 1.0, 0.0, 0.0]}}]}).encode("utf-8")
        return (200, {"content-type": "application/json"}, resp_body)

    monkeypatch.setattr("vertebra.embeddings._google.gcp_http2_post_async", fake_native_http2, raising=False)
    default_vertex = VertexEmbedder(
        "gemini-embedding-001",
        dimensions=4,
        project="test-gcp-project",
        token_cache=_FakeTokenCache(),
        coalesce_window_ms=0.0,
    )
    vec = await default_vertex.embed_query("native http2 transport")
    assert vec.dimensions == 4
    assert len(called_native) == 1


class _FakeIdTokenCache:
    """In-memory fake AsyncGcpIdTokenCache returning GcpIdTokenSnapshot with generation rotation."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []
        self.invalidated: list[tuple[str, int]] = []
        self._generation = 0
        self._invalidated_up_to = 0

    def invalidate(self, audience: str, failed_generation: int) -> None:
        self.invalidated.append((audience, failed_generation))
        if failed_generation >= self._invalidated_up_to:
            self._invalidated_up_to = failed_generation

    async def get_token(self, audience: str, *, force_refresh: bool = False) -> GcpIdTokenSnapshot:
        self.calls.append((audience, force_refresh))
        if force_refresh or self._generation == 0 or self._generation <= self._invalidated_up_to:
            self._generation += 1
        return GcpIdTokenSnapshot(
            id_token=f"mock-id-token-{self._generation}",
            audience=audience,
            expires_at_unix_ms=4_102_444_800_000,
            generation=self._generation,
        )


def test_google_embedder_sync_token_cache_normalization_and_model_catalog() -> None:
    """Verify SyncGcpTokenCache normalizes to AsyncGcpTokenCache at init and ModelSpec encodes Vertex batch metadata."""
    sync_tok = SyncGcpTokenCache(
        {
            "client_id": "cid",
            "client_secret": "csec",
            "refresh_token": "rtok",
            "token_uri": "http://127.0.0.1:9/token",
        },
        allow_ambient_adc=False,
    )
    vertex = VertexEmbedder("gemini-embedding-001", project="p1", token_cache=sync_tok)
    gemini = GeminiEmbedder("gemini-embedding-2", token_cache=sync_tok)
    assert isinstance(vertex._token_cache, AsyncGcpTokenCache)
    assert isinstance(gemini._token_cache, AsyncGcpTokenCache)

    spec_001 = get_model_spec("gemini-embedding-001")
    spec_005 = get_model_spec("text-embedding-005")
    spec_multi = get_model_spec("text-multilingual-embedding-002")
    spec_gem2 = get_model_spec("gemini-embedding-2")
    assert spec_001 is not None and spec_001.vertex_method == "predict" and spec_001.max_instances_per_request == 1
    assert spec_005 is not None and spec_005.vertex_method == "predict" and spec_005.max_instances_per_request == 250
    assert (
        spec_multi is not None and spec_multi.vertex_method == "predict" and spec_multi.max_instances_per_request == 250
    )
    assert (
        spec_gem2 is not None
        and spec_gem2.vertex_method == "embed_content"
        and spec_gem2.max_instances_per_request == 1
    )


@pytest.mark.anyio
async def test_google_and_gemini_error_mapping_and_missing_adc_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify HTTP 401 double-failure, non-zero grpc-status, row drift, and missing ambient ADC raise EmbeddingError or ImproperConfigurationError."""
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("CLOUDSDK_CONFIG", raising=False)
    monkeypatch.delenv("GOOGLE_OAUTH_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("CLOUDSDK_AUTH_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("HOME", "/nonexistent-home-for-vertebra-test")
    monkeypatch.setenv("GCE_METADATA_HOST", "127.0.0.1:9")

    unconfigured = GeminiEmbedder("gemini-embedding-001", dimensions=4)
    with pytest.raises(ImproperConfigurationError, match="ADC"):
        await unconfigured.embed_many(["hello"])

    async def double_401_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        _ = (url, headers, body)
        return (401, {"content-type": "application/json"}, b"unauthorized")

    failing_401 = GeminiEmbedder(
        "gemini-embedding-001",
        dimensions=4,
        token_cache=_FakeTokenCache(),
        transport=double_401_transport,
    )
    with pytest.raises(EmbeddingError, match="HTTP 401"):
        await failing_401.embed_many(["hello"])

    async def grpc_err_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        _ = (url, headers, body)
        return (200, {"grpc-status": "3", "grpc-message": "invalid content"}, b"")

    failing_grpc = GeminiEmbedder("gemini-embedding-001", dimensions=4, api_key="k", transport=grpc_err_transport)
    with pytest.raises(EmbeddingError, match="gRPC status 3"):
        await failing_grpc.embed_many(["hello"])

    async def row_drift_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        _ = (url, headers, body)
        one_row = VectorBatchView.from_f32_rows([[1.0, 0.0, 0.0, 0.0]], normalize=True)
        return (200, {"grpc-status": "0"}, encode_gemini_embed_grpc_response_frame(one_row))

    drifting = GeminiEmbedder("gemini-embedding-001", dimensions=4, api_key="k", transport=row_drift_transport)
    with pytest.raises(EmbeddingError, match="expected 2"):
        await drifting.embed_many(["one", "two"])


@pytest.mark.anyio
async def test_google_micro_batch_per_loop_isolation_and_gemini_multimodal_validation() -> None:
    """Verify concurrent embed_query_sync (portal loop) and embed_query (main loop) isolate queues and Gemini validates multimodal parts."""
    call_threads: list[int] = []

    async def multi_loop_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        _ = (url, headers, body)
        call_threads.append(threading.get_ident())
        batch = VectorBatchView.from_f32_rows([[1.0, 0.0, 0.0, 0.0]], normalize=True)
        return (200, {"grpc-status": "0"}, encode_gemini_embed_grpc_response_frame(batch))

    embedder = GeminiEmbedder(
        "gemini-embedding-2",
        dimensions=4,
        api_key="k",
        coalesce_window_ms=2.0,
        transport=multi_loop_transport,
    )
    async_task = embedder.embed_query("from-main-loop")
    sync_task = asyncio.to_thread(embedder.embed_query_sync, "from-portal-loop")
    v_async, v_sync = await asyncio.gather(async_task, sync_task)
    assert v_async.dimensions == 4
    assert v_sync.dimensions == 4
    assert len(embedder._queues) == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    "bad_part",
    [
        MultimodalPart(mime_type="image/png", data=b"x", uri="https://example.com/a.png"),
        MultimodalPart(mime_type="text/plain"),
        MultimodalPart(mime_type="image/png", uri="gs://bucket/obj.png"),
    ],
)
async def test_gemini_embedder_rejects_invalid_multimodal_parts(bad_part: MultimodalPart) -> None:
    """GeminiEmbedder.embed_multimodal rejects conflicting, empty, or gs:// MultimodalPart payloads."""
    embedder = GeminiEmbedder("gemini-embedding-2", dimensions=4, api_key="k")
    with pytest.raises(ValueError):
        await embedder.embed_multimodal([bad_part])


@pytest.mark.anyio
async def test_vertex_rest_predict_chunking_embed_content_multimodal_and_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Vertex REST :predict chunking (1 vs 250 instances & 20k tokens), :embedContent multimodal, global endpoint, titles, drift, and TaskGroup cancellation."""
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    monkeypatch.delenv("GCP_PROJECT", raising=False)

    no_project = VertexEmbedder("gemini-embedding-001", token_cache=_FakeTokenCache(), coalesce_window_ms=0.0)
    with pytest.raises(ImproperConfigurationError, match="project"):
        await no_project.embed_many(["needs project"])

    seen_calls: list[tuple[str, dict[str, Any]]] = []

    async def predict_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        _ = headers
        req = json.loads(body.decode("utf-8"))
        seen_calls.append((url, req))
        preds = [{"embeddings": {"values": [float(idx + 1), 1.0, 0.0, 0.0]}} for idx, _ in enumerate(req["instances"])]
        return (200, {"content-type": "application/json"}, json.dumps({"predictions": preds}).encode("utf-8"))

    emb_001 = VertexEmbedder(
        "gemini-embedding-001",
        dimensions=4,
        project="p1",
        location="global",
        token_cache=_FakeTokenCache(),
        max_concurrent_requests=2,
        transport=predict_transport,
    )
    with pytest.raises(ValueError, match="document"):
        await emb_001.embed_many(["q1"], task="query", titles=["Disallowed Title"])
    assert len(seen_calls) == 0

    batch_3 = await emb_001.embed_many(["a", "b", "c"], task="document", titles=["T1", None, "T3"])
    assert batch_3.count == 3
    assert len(seen_calls) == 3
    assert all(
        url.startswith("https://aiplatform.googleapis.com/v1/projects/p1/locations/global/") for url, _ in seen_calls
    )
    assert all(len(req["instances"]) == 1 for _, req in seen_calls)

    seen_calls.clear()
    emb_005 = VertexEmbedder(
        "text-embedding-005",
        dimensions=4,
        project="p1",
        token_cache=_FakeTokenCache(),
        transport=predict_transport,
    )
    batch_300 = await emb_005.embed_many([f"doc-{i}" for i in range(300)])
    assert batch_300.count == 300
    assert sorted(len(req["instances"]) for _, req in seen_calls) == [50, 250]

    seen_calls.clear()
    big_text = "x" * (12_000 * 4)
    batch_tok = await emb_005.embed_many([big_text, big_text])
    assert batch_tok.count == 2
    assert [len(req["instances"]) for _, req in seen_calls] == [1, 1]

    with pytest.raises(ValueError, match="gemini-embedding-2"):
        await emb_001.embed_multimodal([MultimodalPart(mime_type="image/png", data=b"\x89PNG")])

    embed_content_calls: list[tuple[str, dict[str, Any]]] = []

    async def embed_content_transport(
        url: str, headers: dict[str, str], body: bytes
    ) -> tuple[int, dict[str, str], bytes]:
        _ = headers
        req = json.loads(body.decode("utf-8"))
        embed_content_calls.append((url, req))
        return (
            200,
            {"content-type": "application/json"},
            json.dumps({"embedding": {"values": [1.0, 2.0, 3.0, 4.0]}}).encode("utf-8"),
        )

    emb_gem2 = VertexEmbedder(
        "gemini-embedding-2",
        dimensions=4,
        project="p1",
        location="us-east1",
        auto_truncate=False,
        token_cache=_FakeTokenCache(),
        transport=embed_content_transport,
    )
    mm_batch = await emb_gem2.embed_multimodal(
        [
            MultimodalPart(mime_type="image/png", data=b"\x89PNG", text="logo"),
            MultimodalPart(mime_type="video/mp4", uri="gs://my-bucket/clip.mp4"),
        ]
    )
    assert mm_batch.count == 2
    assert all(url.endswith(":embedContent") for url, _ in embed_content_calls)
    first_req = embed_content_calls[0][1]
    assert first_req["content"]["role"] == "user"
    assert first_req["autoTruncate"] is False
    assert first_req["outputDimensionality"] == 4
    assert first_req["content"]["parts"][0]["inlineData"]["mimeType"] == "image/png"
    assert embed_content_calls[1][1]["content"]["parts"][0]["fileData"]["fileUri"] == "gs://my-bucket/clip.mp4"

    async def drift_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        _ = (url, headers, body)
        return (
            200,
            {"content-type": "application/json"},
            json.dumps({"predictions": [{"embeddings": {"values": [1.0, 2.0]}}]}).encode("utf-8"),
        )

    drifting = VertexEmbedder(
        "gemini-embedding-001",
        dimensions=4,
        project="p1",
        token_cache=_FakeTokenCache(),
        transport=drift_transport,
    )
    with pytest.raises(EmbeddingError, match="expected 4"):
        await drifting.embed_many(["x"])

    async def fail_500_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        _ = (url, headers, body)
        return (500, {"content-type": "application/json"}, b"backend error")

    failing = VertexEmbedder(
        "gemini-embedding-001",
        dimensions=4,
        project="p1",
        token_cache=_FakeTokenCache(),
        transport=fail_500_transport,
    )
    with pytest.raises(EmbeddingError, match="HTTP 500"):
        await failing.embed_many(["one", "two"])


@pytest.mark.anyio
async def test_remote_embedder_id_token_auth_audience_and_401_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify RemoteEmbedder supports Sync/Async ID token caches, endpoint-origin default audience, mutual exclusion, and single 401 retry."""
    fake_id: Any = _FakeIdTokenCache()

    with pytest.raises(ValueError, match="mutually exclusive"):
        RemoteEmbedder(
            endpoint="https://emb-svc.a.run.app/v1/",
            bearer_token="static-tok",
            id_token_cache=fake_id,
        )

    with pytest.raises(ValueError, match="audience requires id_token_cache"):
        RemoteEmbedder(
            endpoint="https://emb-svc.a.run.app/v1/",
            audience="https://emb-svc.a.run.app",
        )

    with pytest.raises(ValueError, match="scheme or host"):
        RemoteEmbedder(endpoint="/relative-only", id_token_cache=fake_id)

    monkeypatch.setenv("GCE_METADATA_HOST", "127.0.0.1:9")
    sync_id_cache = SyncGcpIdTokenCache()
    normalized_remote = RemoteEmbedder(
        endpoint="https://emb-svc.a.run.app/v1/",
        id_token_cache=sync_id_cache,
    )
    assert isinstance(normalized_remote._id_token_cache, AsyncGcpIdTokenCache)
    assert normalized_remote._audience == "https://emb-svc.a.run.app"

    sent_headers: list[dict[str, str]] = []
    fail_first_401 = True

    async def remote_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        nonlocal fail_first_401
        _ = (url, body)
        sent_headers.append(dict(headers))
        if fail_first_401:
            fail_first_401 = False
            return (401, {"content-type": "application/grpc+msgpack"}, b"")
        batch = VectorBatchView.from_f32_rows([[1.0, 0.0, 0.0, 0.0]], normalize=True)
        resp_msg = RemoteEmbedResponse(
            packed_frame=encode_gemini_embed_grpc_response_frame(batch),
            count=1,
            dimensions=4,
        )
        return (200, {"grpc-status": "0"}, encode_lpm_frame(encode_msgpack_bytes(resp_msg)))

    remote = RemoteEmbedder(
        endpoint="https://emb-svc.a.run.app/v1/",
        dimensions=4,
        id_token_cache=fake_id,
        transport=remote_transport,
    )
    batch = await remote.embed_many(["hello"])
    assert batch.count == 1
    assert fake_id.calls == [("https://emb-svc.a.run.app", False), ("https://emb-svc.a.run.app", False)]
    assert fake_id.invalidated == [("https://emb-svc.a.run.app", 1)]
    assert sent_headers[0]["authorization"] == "Bearer mock-id-token-1"
    assert sent_headers[1]["authorization"] == "Bearer mock-id-token-2"

    explicit_remote = RemoteEmbedder(
        endpoint="https://emb-svc.a.run.app/v1/",
        dimensions=4,
        id_token_cache=fake_id,
        audience="custom-iap-audience",
        transport=remote_transport,
    )
    await explicit_remote.embed_many(["world"])
    assert fake_id.calls[-1] == ("custom-iap-audience", False)

    with pytest.raises(EmbeddingError, match="expected 2"):
        await remote.embed_many(["one", "two"])

    async def double_401_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        _ = (url, headers, body)
        return (401, {"content-type": "application/grpc+msgpack"}, b"")

    double_401_remote = RemoteEmbedder(
        endpoint="https://emb-svc.a.run.app/v1/",
        dimensions=4,
        id_token_cache=fake_id,
        transport=double_401_transport,
    )
    with pytest.raises(EmbeddingError, match="401"):
        await double_401_remote.embed_many(["fail"])


@pytest.mark.anyio
async def test_remote_embedder_concurrent_401_coalesces_single_id_token_mint() -> None:
    """Concurrent 401 retries pass force_refresh=False so generation-fenced single-flight mints once."""

    class _SingleFlightIdCache:
        def __init__(self) -> None:
            self._lock = asyncio.Lock()
            self.mint_count = 0
            self._cached: GcpIdTokenSnapshot | None = None

        def invalidate(self, audience: str, failed_generation: int) -> None:
            if (
                self._cached is not None
                and self._cached.audience == audience
                and self._cached.generation == failed_generation
            ):
                self._cached = None

        async def get_token(self, audience: str, *, force_refresh: bool = False) -> GcpIdTokenSnapshot:
            if not force_refresh and self._cached is not None:
                return self._cached
            async with self._lock:
                if not force_refresh and self._cached is not None:
                    return self._cached
                await asyncio.sleep(0.01)
                self.mint_count += 1
                self._cached = GcpIdTokenSnapshot(
                    id_token=f"sf-id-token-{self.mint_count}",
                    audience=audience,
                    expires_at_unix_ms=4_102_444_800_000,
                    generation=self.mint_count,
                )
                return self._cached

    sf_cache: Any = _SingleFlightIdCache()
    await sf_cache.get_token("https://emb-svc.a.run.app")
    assert sf_cache.mint_count == 1

    async def gen1_401_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        _ = (url, body)
        if headers.get("authorization") == "Bearer sf-id-token-1":
            return (401, {"content-type": "application/grpc+msgpack"}, b"")
        batch = VectorBatchView.from_f32_rows([[1.0, 0.0, 0.0, 0.0]], normalize=True)
        resp_msg = RemoteEmbedResponse(
            packed_frame=encode_gemini_embed_grpc_response_frame(batch),
            count=1,
            dimensions=4,
        )
        return (200, {"grpc-status": "0"}, encode_lpm_frame(encode_msgpack_bytes(resp_msg)))

    remote = RemoteEmbedder(
        endpoint="https://emb-svc.a.run.app/v1/",
        dimensions=4,
        id_token_cache=sf_cache,
        transport=gen1_401_transport,
    )
    batches = await asyncio.gather(*(remote.embed_many(["concurrent-item"]) for _ in range(8)))
    assert all(b.count == 1 for b in batches)
    assert sf_cache.mint_count == 2


@pytest.mark.anyio
async def test_oracle_pushdown_explicit_cache_and_oversized_vector_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OracleEmbeddingPushdown avoids implicit token caches, normalizes sync caches, and rejects oversized vectors."""
    monkeypatch.setenv("GOOGLE_OAUTH_ACCESS_TOKEN", "mock-oauth-token")
    monkeypatch.setenv("GCE_METADATA_HOST", "127.0.0.1:9")

    port_no_cache = _AsyncSelectPort([{"embedding": encode_oracle_vector_f32([3.0, 4.0, 0.0, 0.0])}])
    implicit_none = OracleEmbeddingPushdown(port_no_cache, project_number="123", dimensions=4)
    vec = await implicit_none.embed_query("no-rotation", normalize=False)
    assert vec.to_list() == [3.0, 4.0, 0.0, 0.0]
    assert len(port_no_cache.calls) == 1
    assert "UTL_TO_EMBEDDINGS" in port_no_cache.calls[0][0]
    assert not any("CREATE_CREDENTIAL" in sql or "DROP_CREDENTIAL" in sql for sql, _, _ in port_no_cache.calls)

    monkeypatch.setenv("HOME", "/nonexistent-home-for-vertebra-test")
    monkeypatch.setenv("GCE_METADATA_HOST", "127.0.0.1:9")
    sync_cache = SyncGcpTokenCache({"access_token": "mock-oauth-token"}, allow_ambient_adc=False)
    port_sync_cache = _AsyncSelectPort([{"embedding": encode_oracle_vector_f32([1.0, 0.0, 0.0, 0.0])}])
    with_sync_cache = OracleEmbeddingPushdown(
        port_sync_cache,
        project_number="123",
        dimensions=4,
        token_cache=sync_cache,
    )
    assert isinstance(with_sync_cache._token_cache, AsyncGcpTokenCache)
    await with_sync_cache.embed_query("rotate-once")
    assert sum("CREATE_CREDENTIAL" in sql for sql, _, _ in port_sync_cache.calls) == 1

    oversized_port = _AsyncSelectPort([{"embedding": encode_oracle_vector_f32([1.0, 2.0, 3.0, 4.0, 5.0])}])
    oversized_pushdown = OracleEmbeddingPushdown(oversized_port, project_number="123", dimensions=4)
    with pytest.raises(DataError, match="Oracle embedding row"):
        await oversized_pushdown.embed_query("oversized")


@pytest.mark.anyio
@pytest.mark.parametrize(
    "exc",
    [RuntimeError("oauth daemon offline"), GoogleCloudAuthError("expired grant")],
)
async def test_oracle_pushdown_propagates_token_cache_errors(exc: Exception) -> None:
    """OracleEmbeddingPushdown propagates explicit token_cache errors without swallowing them."""

    class BrokenTokenCache:
        async def get_token(self) -> GcpTokenSnapshot:
            raise exc

    broken: Any = BrokenTokenCache()
    failing_pushdown = OracleEmbeddingPushdown(
        _AsyncSelectPort([]),
        project_number="123",
        dimensions=4,
        token_cache=broken,
    )
    with pytest.raises(type(exc)):
        await failing_pushdown.embed_query("fail-loud")


@pytest.mark.anyio
@pytest.mark.parametrize("bad_top_k", [0, -3])
async def test_oracle_pushdown_rejects_non_positive_top_k(bad_top_k: int) -> None:
    """OracleEmbeddingPushdown.similarity_search rejects non-positive top_k."""
    pushdown = OracleEmbeddingPushdown(_AsyncSelectPort([]), project_number="123", dimensions=4)
    with pytest.raises(ValueError, match="top_k must be positive"):
        await pushdown.similarity_search([1.0, 0.0, 0.0, 0.0], table="docs", top_k=bad_top_k)


@pytest.mark.anyio
async def test_remote_embedder_default_http_transport_handles_http_401_and_retries() -> None:
    """RemoteEmbedder._default_http_transport returns HTTPError status/headers/body so 401 retry runs."""
    seen_auths: list[str | None] = []
    batch = VectorBatchView.from_f32_rows([[1.0, 0.0, 0.0, 0.0]], normalize=True)
    ok_payload = encode_lpm_frame(
        encode_msgpack_bytes(
            RemoteEmbedResponse(
                packed_frame=encode_gemini_embed_grpc_response_frame(batch),
                count=1,
                dimensions=4,
            )
        )
    )

    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("content-length", "0"))
            _ = self.rfile.read(length)
            seen_auths.append(self.headers.get("authorization"))
            if len(seen_auths) == 1:
                self.send_response(401)
                self.send_header("content-type", "application/grpc+msgpack")
                self.end_headers()
                self.wfile.write(b"expired")
                return
            self.send_response(200)
            self.send_header("content-type", "application/grpc+msgpack")
            self.send_header("grpc-status", "0")
            self.end_headers()
            self.wfile.write(ok_payload)

        def log_message(self, format: str, *args: Any) -> None:
            _ = (format, args)

    class _LoopbackIdCache:
        def __init__(self) -> None:
            self.generation = 0
            self.invalidated: list[tuple[str, int]] = []

        def invalidate(self, audience: str, failed_generation: int) -> None:
            self.invalidated.append((audience, failed_generation))

        async def get_token(self, audience: str, *, force_refresh: bool = False) -> GcpIdTokenSnapshot:
            _ = force_refresh
            self.generation += 1
            return GcpIdTokenSnapshot(
                id_token=f"http-id-tok-{self.generation}",
                audience=audience,
                expires_at_unix_ms=4_102_444_800_000,
                generation=self.generation,
            )

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        cache: Any = _LoopbackIdCache()
        remote = RemoteEmbedder(
            endpoint=f"http://127.0.0.1:{server.server_port}",
            dimensions=4,
            id_token_cache=cache,
        )
        out = await remote.embed_many(["loopback-401"])
        assert out.count == 1
        assert seen_auths == ["Bearer http-id-tok-1", "Bearer http-id-tok-2"]
        assert cache.invalidated == [(f"http://127.0.0.1:{server.server_port}", 1)]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
