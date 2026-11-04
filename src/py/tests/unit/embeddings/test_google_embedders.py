"""Unit tests for Google-first zero-weight embedders, gRPC packed-float framing, and SQL pushdown."""

from __future__ import annotations

import asyncio
import math
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
from vertebra.embeddings import (
    AlloyDbEmbedder,
    Embedder,
    EmbeddingError,
    GeminiEmbedder,
    MultimodalPart,
    OracleEmbedder,
    RemoteEmbedder,
    VertexEmbedder,
    create_embedding_grpc_service,
)


class _FakeTokenCache:
    """In-memory fake GcpTokenCache returning GcpTokenSnapshot with generation rotation."""

    def __init__(self, prefix: str = "mock-vertex-token", quota_project: str | None = "quota-proj-123") -> None:
        self._prefix = prefix
        self._quota_project = quota_project
        self._generation = 0
        self._invalidated_up_to = 0

    def invalidate(self, failed_generation: int) -> None:
        if failed_generation >= self._invalidated_up_to:
            self._invalidated_up_to = failed_generation

    async def get_token_async(self, *, force_refresh: bool = False) -> GcpTokenSnapshot:
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

    for exported_name in vertebra.embeddings.__all__:
        assert not exported_name.endswith("Bridge")
        assert "GrpcBridge" not in exported_name


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
async def test_alloydb_embedder_sql_pushdown_and_binary_pgvector_decode() -> None:
    """Verify AlloyDbEmbedder generates unnest WITH ORDINALITY SQL and decodes binary pgvector frames."""
    executed_queries: list[tuple[str, tuple[Any, ...]]] = []

    class FakeAlloyDbDriver:
        async def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[bytes]]:
            executed_queries.append((sql, params))
            texts = params[1] if len(params) > 1 and isinstance(params[1], list) else [params[1]]
            return [(encode_pgvector_f32([float(i + 1), 2.0, 3.0, 4.0]),) for i in range(len(texts))]

    driver = FakeAlloyDbDriver()
    embedder = AlloyDbEmbedder(
        driver=driver,
        model="gemini-embedding-001",
        dimensions=4,
    )

    expr = embedder.sql_expr("content_col")
    assert expr == "embedding('gemini-embedding-001', content_col)::vector"

    batch = await embedder.embed_many(["doc a", "doc b"])
    assert len(executed_queries) == 1
    sql, params = executed_queries[0]
    assert "unnest($2::text[]) WITH ORDINALITY" in sql
    assert "ORDER BY t.ord" in sql
    assert params == ("gemini-embedding-001", ["doc a", "doc b"])
    assert batch.count == 2
    assert batch.dimensions == 4


@pytest.mark.anyio
async def test_oracle_embedder_predict_suffix_128_char_url_guard_and_credential_rotation() -> None:
    """Verify OracleEmbedder suffixes :predict, enforces <= 128-char URL, rotates credentials, and decodes 0xDB."""
    token_cache: Any = _FakeTokenCache(prefix="mock-ora-vertex-token", quota_project=None)
    executed_sql: list[tuple[str, Any]] = []

    class FakeOracleDriver:
        async def execute(self, sql: str, params: Any = None) -> None:
            executed_sql.append((sql, params))

        async def fetch_all(self, sql: str, params: Any = None) -> list[tuple[bytes]]:
            executed_sql.append((sql, params))
            return [(encode_oracle_vector_f32([0.6, 0.8, 0.0, 0.0]),)]

    with pytest.raises(ValueError, match="128"):
        OracleEmbedder(
            driver=FakeOracleDriver(),
            model="gemini-embedding-001",
            dimensions=4,
            provider="vertexai",
            project="extremely-long-enterprise-google-cloud-project-identifier-exceeding-limit",
            location="australia-southeast2",
        )

    driver = FakeOracleDriver()
    embedder = OracleEmbedder(
        driver=driver,
        model="gemini-embedding-001",
        dimensions=4,
        provider="vertexai",
        project_number="123456789012",
        location="us-central1",
        credential_name="VERTEX_CRED",
        token_cache=token_cache,
    )
    assert embedder.resolved_model_parameter == "gemini-embedding-001:predict"
    assert len(embedder.endpoint_url) <= 128

    vec1 = await embedder.embed_query("oracle vector test")
    assert vec1.dimensions == 4
    assert vec1.to_list() == pytest.approx([0.6, 0.8, 0.0, 0.0], rel=1e-6)
    assert embedder.applied_token_generation == 1
    assert any("DBMS_VECTOR.CREATE_CREDENTIAL" in s for s, _ in executed_sql)

    vec2 = await embedder.embed_query("second query same token")
    assert vec2.dimensions == 4
    cred_calls_before = sum(1 for s, _ in executed_sql if "DBMS_VECTOR.CREATE_CREDENTIAL" in s)
    assert cred_calls_before == 1

    token_cache.invalidate(1)
    await embedder.embed_query("third query after token rotation")
    cred_calls_after = sum(1 for s, _ in executed_sql if "DBMS_VECTOR.CREATE_CREDENTIAL" in s)
    assert cred_calls_after == 2
    assert embedder.applied_token_generation == 2


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


@pytest.mark.anyio
async def test_oracle_embedder_raises_embedding_error_on_empty_result() -> None:
    """An empty DBMS_VECTOR result raises EmbeddingError instead of IndexError."""

    class EmptyOracleDriver:
        async def execute(self, sql: str, params: Any = None) -> None:
            return None

        async def fetch_all(self, sql: str, params: Any = None) -> list[tuple[bytes]]:
            return []

    token_cache: Any = _FakeTokenCache(prefix="mock-ora-vertex-token", quota_project=None)
    embedder = OracleEmbedder(
        driver=EmptyOracleDriver(),
        model="gemini-embedding-001",
        dimensions=4,
        provider="vertexai",
        project_number="123456789012",
        token_cache=token_cache,
    )
    with pytest.raises(EmbeddingError, match="no embedding row"):
        await embedder.embed_query("empty result")
