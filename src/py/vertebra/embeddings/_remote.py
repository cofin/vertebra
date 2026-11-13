"""Remote cluster gRPC embedder and GrpcServiceRegistry factory."""

from __future__ import annotations

import asyncio
import inspect
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra._vertebra import (
    AsyncGcpIdTokenCache,
    SyncGcpIdTokenCache,
    VectorBatchView,
    VectorCache,
    decode_gemini_embed_grpc_frame_to_batch,
    encode_gemini_embed_grpc_response_frame,
)
from vertebra.embeddings._base import Embedder, EmbeddingError
from vertebra.extensions.grpc import (
    GrpcServiceRegistry,
    decode_lpm_frames,
    encode_lpm_frame,
    grpc_method,
)
from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes

if TYPE_CHECKING:
    from vertebra.embeddings._google import GrpcTransportCallable

__all__ = (
    "RemoteEmbedRequest",
    "RemoteEmbedResponse",
    "RemoteEmbedder",
    "create_embedding_grpc_service",
)


class RemoteEmbedRequest(msgspec.Struct, frozen=True, gc=False):
    """Wire request struct for `/vertebra.EmbeddingService/BatchEmbed`."""

    model: str
    texts: list[str]
    task: str = "document"
    titles: list[str | None] | None = None
    dimensions: int = 768


class RemoteEmbedResponse(msgspec.Struct, frozen=True, gc=False):
    """Wire response struct carrying a packed-float Protobuf v3 `VectorSlab` frame."""

    packed_frame: bytes
    count: int
    dimensions: int


class RemoteEmbedder(Embedder):
    """Cluster-wide remote gRPC embedder connecting to a `create_embedding_grpc_service` endpoint."""

    def __init__(
        self,
        endpoint: str = "http://127.0.0.1:8000",
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        bearer_token: str | None = None,
        id_token_cache: SyncGcpIdTokenCache | AsyncGcpIdTokenCache | None = None,
        audience: str | None = None,
        cache: VectorCache | None = None,
        transport: GrpcTransportCallable | None = None,
    ) -> None:
        """Initialize the remote cluster gRPC embedder."""
        if bearer_token is not None and id_token_cache is not None:
            raise ValueError("bearer_token and id_token_cache are mutually exclusive")
        super().__init__(model=model, dimensions=dimensions, cache=cache)
        self._endpoint = endpoint.rstrip("/")
        self._bearer_token = bearer_token
        self._id_token_cache: Any = (
            id_token_cache.to_async() if isinstance(id_token_cache, SyncGcpIdTokenCache) else id_token_cache
        )
        self._audience: str | None = None
        if self._id_token_cache is not None:
            if audience is not None:
                cleaned = audience.strip()
                if not cleaned:
                    raise ValueError("audience must not be empty")
                self._audience = cleaned
            else:
                parsed = urllib.parse.urlsplit(endpoint)
                if not parsed.scheme or not parsed.netloc:
                    raise ValueError(
                        f"cannot derive ID token audience from endpoint {endpoint!r}: missing scheme or host"
                    )
                self._audience = f"{parsed.scheme}://{parsed.netloc}"
        self._transport = transport or self._default_http_transport

    async def _fetch_id_token(self, audience: str, *, force_refresh: bool = False) -> Any:
        """Fetch an OIDC ID token snapshot from the normalized async token cache."""
        raw = self._id_token_cache.get_token(audience, force_refresh=force_refresh)
        return await raw if inspect.isawaitable(raw) else raw

    async def _embed_batch_impl(
        self,
        texts: list[str],
        *,
        task: str = "document",
        titles: list[str | None] | None = None,
    ) -> VectorBatchView:
        """Send a `/vertebra.EmbeddingService/BatchEmbed` RPC and decode into a `VectorBatchView`."""
        req_msg = RemoteEmbedRequest(
            model=self._model,
            texts=texts,
            task=task,
            titles=titles,
            dimensions=self._dimensions,
        )
        wire_body = encode_lpm_frame(encode_msgpack_bytes(req_msg))
        headers: dict[str, str] = {
            "content-type": "application/grpc+msgpack",
            "te": "trailers",
        }
        url = f"{self._endpoint}/vertebra.EmbeddingService/BatchEmbed"

        if self._id_token_cache is not None:
            assert self._audience is not None
            snap = await self._fetch_id_token(self._audience, force_refresh=False)
            req_headers = {**headers, "authorization": f"Bearer {snap.id_token}"}
            status, resp_headers, resp_body = await self._transport(url, req_headers, wire_body)
            if status == 401:
                self._id_token_cache.invalidate(self._audience, snap.generation)
                snap2 = await self._fetch_id_token(self._audience, force_refresh=True)
                req_headers2 = {**headers, "authorization": f"Bearer {snap2.id_token}"}
                status, resp_headers, resp_body = await self._transport(url, req_headers2, wire_body)
        else:
            if self._bearer_token:
                headers["authorization"] = f"Bearer {self._bearer_token}"
            status, resp_headers, resp_body = await self._transport(url, headers, wire_body)

        normalized_headers = {k.lower(): v for k, v in resp_headers.items()}
        if status != 200:
            raise EmbeddingError(f"RemoteEmbedder HTTP status {status}")
        grpc_status = normalized_headers.get("grpc-status", "0")
        if grpc_status != "0":
            msg = normalized_headers.get("grpc-message", "unknown remote gRPC error")
            raise EmbeddingError(f"RemoteEmbedder gRPC status {grpc_status}: {msg}")

        frames = decode_lpm_frames(resp_body)
        if not frames:
            raise EmbeddingError("RemoteEmbedder received empty gRPC response frame list")
        resp_msg = decode_msgpack(frames[0], type=RemoteEmbedResponse)
        return decode_gemini_embed_grpc_frame_to_batch(
            resp_msg.packed_frame,
            dimensions=self._dimensions,
            normalize=True,
        )

    @staticmethod
    async def _default_http_transport(
        url: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[int, dict[str, str], bytes]:
        """Execute an HTTP POST in a worker thread when no custom transport is supplied."""

        def _sync_send() -> tuple[int, dict[str, str], bytes]:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=30.0) as resp:
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                return (resp.status, resp_headers, resp.read())

        return await asyncio.to_thread(_sync_send)


def create_embedding_grpc_service(embedder: Embedder) -> GrpcServiceRegistry:
    """Create a `GrpcServiceRegistry` exposing `/vertebra.EmbeddingService/BatchEmbed`."""
    registry = GrpcServiceRegistry()

    @grpc_method(service="EmbeddingService", package="vertebra", name="BatchEmbed")
    async def batch_embed(req: RemoteEmbedRequest) -> RemoteEmbedResponse:
        batch = await embedder.embed_many(req.texts, task=req.task, titles=req.titles)
        if req.dimensions > 0 and req.dimensions < batch.dimensions:
            batch = batch.truncate(req.dimensions, normalize=True)
        packed_frame = encode_gemini_embed_grpc_response_frame(batch)
        return RemoteEmbedResponse(
            packed_frame=packed_frame,
            count=batch.count,
            dimensions=batch.dimensions,
        )

    registry.register(batch_embed)
    return registry
