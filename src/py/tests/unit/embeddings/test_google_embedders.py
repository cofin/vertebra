"""Unit tests for Google-first zero-weight embedders, gRPC packed-float framing, and SQL pushdown."""

from __future__ import annotations

import asyncio
import base64
import math
import subprocess
import sys
import threading
from typing import Any

import pytest

import vertebra
from vertebra._vertebra import (
    ArrowCursor,
    GcpTokenSnapshot,
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
    """Verify VertexEmbedder coalesces concurrent embed_query calls, attaches ADC + quota project, and retries 401."""
    token_cache: Any = _FakeTokenCache(prefix="mock-vertex-token", quota_project="quota-proj-123")

    transport_calls: list[tuple[str, dict[str, str], bytes]] = []

    async def fake_transport(url: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], bytes]:
        transport_calls.append((url, dict(headers), body))
        if len(transport_calls) == 1:
            return (401, {"content-type": "application/grpc+proto"}, b"")
        src_batch = VectorBatchView.from_f32_rows(
            [
                [3.0, 4.0, 12.0, 0.0, 1.0, 2.0, 3.0, 4.0],
                [0.0, 3.0, 4.0, 0.0, 5.0, 6.0, 7.0, 8.0],
            ],
            normalize=False,
        )
        grpc_resp = encode_gemini_embed_grpc_response_frame(src_batch)
        return (200, {"content-type": "application/grpc+proto", "grpc-status": "0"}, grpc_resp)

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
    assert len(transport_calls) == 2
    assert transport_calls[0][1]["authorization"] == "Bearer mock-vertex-token-1"
    assert transport_calls[0][1]["x-goog-user-project"] == "quota-proj-123"
    assert transport_calls[1][1]["authorization"] == "Bearer mock-vertex-token-2"
    assert v1.dimensions == 4
    assert v2.dimensions == 4
    norm1 = math.sqrt(sum(x * x for x in v1.to_list()))
    norm2 = math.sqrt(sum(x * x for x in v2.to_list()))
    assert norm1 == pytest.approx(1.0, rel=1e-5)
    assert norm2 == pytest.approx(1.0, rel=1e-5)

    cached_v1 = await embedder.embed_query("first query")
    assert len(transport_calls) == 2
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

    def select(self, sql: str, parameters: Any = None) -> Any:
        self.calls.append((sql, parameters, threading.get_ident()))
        return self.rows

    def execute(self, sql: str, parameters: Any = None) -> None:
        self.calls.append((sql, parameters, threading.get_ident()))


class _AsyncSelectPort(AsyncDatabaseDriver[Any], _SelectPort):
    """Async database port implementing the real driver dispatch boundary."""

    def __init__(self, rows: Any) -> None:
        _SelectPort.__init__(self, rows)

    async def select(self, sql: str, parameters: Any = None, **kwargs: Any) -> Any:
        return _SelectPort.select(self, sql, parameters)

    async def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> Any:
        return _SelectPort.execute(self, sql, parameters)


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
    assert pushdown.driver is port
    vector = await pushdown.embed_query("query", normalize=False)
    assert vector.to_list() == [3.0, 4.0, 0.0, 0.0]
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

    class AsyncTokenPort(SyncTokenPort):
        async def get_token(self) -> GcpTokenSnapshot:
            return super().get_token()

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
        expected_model_parameter = "$1" if port_type is _AsyncSelectPort else "%s"
        assert f"embedding({expected_model_parameter}, t.txt)::vector" in port.calls[0][0]
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
                    float(len(titles[idx]) if titles and titles[idx] else 0),
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
@pytest.mark.parametrize(
    ("driver_kind", "expected_binds"),
    [
        ("sync", ("%s", "%s")),
        ("asyncpg_pool", ("$1", "$2")),
        ("psycopg_async_pool", ("%s", "%s")),
        ("tx_asyncpg", ("$1", "$2")),
        ("tx_psycopg", ("%s", "%s")),
    ],
)
async def test_postgres_embedding_pushdown_placeholder_selection(
    driver_kind: str,
    expected_binds: tuple[str, str],
) -> None:
    """PostgresEmbeddingPushdown selects $1/$2 for asyncpg shapes and %s/%s for sync or psycopg pools."""

    class AcquirePool:
        def acquire(self) -> None:
            return None

    class ConnectionPool:
        def connection(self) -> None:
            return None

    class AsyncpgTxConn:
        def fetch(self) -> None:
            return None

    class PsycopgTxConn:
        def cursor(self) -> None:
            return None

    rows = [{"embedding": [1.0, 0.0, 0.0, 0.0]}]
    if driver_kind == "sync":
        port: Any = _SelectPort(rows)
    else:
        port = _AsyncSelectPort(rows)
        if driver_kind == "asyncpg_pool":
            port.pool = AcquirePool()
        elif driver_kind == "psycopg_async_pool":
            port.pool = ConnectionPool()
        elif driver_kind == "tx_asyncpg":
            port._tx_conn = AsyncpgTxConn()
        elif driver_kind == "tx_psycopg":
            port._tx_conn = PsycopgTxConn()

    pushdown = PostgresEmbeddingPushdown(port, dimensions=4, normalize=False)
    await pushdown.embed_query("alpha")
    b1, b2 = expected_binds
    assert f"embedding({b1}, t.txt)::vector FROM unnest({b2}::text[])" in port.calls[-1][0]


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
    assert expected_b64 in recorded_bodies[0]

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
        src_batch = VectorBatchView.from_f32_rows([[0.0, 1.0, 0.0, 0.0]], normalize=True)
        return (
            200,
            {"content-type": "application/grpc+proto", "grpc-status": "0"},
            encode_gemini_embed_grpc_response_frame(src_batch),
        )

    monkeypatch.setattr("vertebra.embeddings._google.gcp_http2_post_async", fake_native_http2, raising=False)
    default_vertex = VertexEmbedder(
        "gemini-embedding-001",
        dimensions=4,
        token_cache=_FakeTokenCache(),
        coalesce_window_ms=0.0,
    )
    vec = await default_vertex.embed_query("native http2 transport")
    assert vec.dimensions == 4
    assert len(called_native) == 1
