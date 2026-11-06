"""Google Cloud gRPC embedders for Vertex AI and Gemini."""

from __future__ import annotations

import asyncio
import base64
import inspect
import os
import urllib.request
from collections.abc import Awaitable, Callable, Sequence

import msgspec

from vertebra._vertebra import (
    AsyncGcpTokenCache,
    SyncGcpTokenCache,
    VectorBatchView,
    VectorCache,
    VectorView,
    decode_gemini_embed_grpc_frame_to_batch,
    encode_gemini_embed_grpc_frame,
)
from vertebra.embeddings._base import Embedder, MultimodalPart
from vertebra.utils.module_loader import import_optional

__all__ = (
    "GeminiEmbedder",
    "GrpcTransportCallable",
    "VertexEmbedder",
)

GrpcTransportCallable = Callable[
    [str, dict[str, str], bytes],
    Awaitable[tuple[int, dict[str, str], bytes]],
]


class _PendingMicroBatchItem(msgspec.Struct, gc=False):
    """Queued singular embedding request waiting in the async micro-batcher."""

    text: str
    task: str
    title: str | None
    estimated_tokens: int
    future: asyncio.Future[VectorView]


class VertexEmbedder(Embedder):
    """Google Cloud Vertex AI / Gemini packed-float gRPC embedder with active ADC refresh and micro-batching."""

    def __init__(
        self,
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        project: str | None = None,
        location: str = "us-central1",
        endpoint: str | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
        target_service_account: str | None = None,
        quota_project_id: str | None = None,
        cache: VectorCache | None = None,
        max_batch_size: int = 64,
        max_batch_tokens: int = 16_384,
        coalesce_window_ms: float = 1.0,
        transport: GrpcTransportCallable | None = None,
    ) -> None:
        """Initialize the Vertex AI gRPC embedder."""
        super().__init__(model=model, dimensions=dimensions, cache=cache)
        self._project = project or os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT") or ""
        self._location = location
        self._endpoint = endpoint or f"https://{location}-aiplatform.googleapis.com"
        self._token_cache = token_cache
        self._target_service_account = target_service_account
        self._quota_project_id = quota_project_id
        self._max_batch_size = max(1, max_batch_size)
        self._max_batch_tokens = max(1, max_batch_tokens)
        self._coalesce_window_ms = max(0.0, coalesce_window_ms)
        self._transport = transport or self._default_http_transport
        self._pending: list[_PendingMicroBatchItem] = []
        self._flush_task: asyncio.Task[None] | None = None

    @property
    def project(self) -> str:
        """Return the configured Google Cloud project ID."""
        return self._project

    @property
    def location(self) -> str:
        """Return the configured Google Cloud region."""
        return self._location

    @property
    def resource_model_name(self) -> str:
        """Return the canonical resource path for the model."""
        if self._model.startswith(("models/", "projects/")):
            return self._model
        return f"models/{self._model}"

    async def _embed_single_impl(
        self,
        text: str,
        *,
        task: str = "query",
        title: str | None = None,
    ) -> VectorView:
        """Coalesce concurrent singular embedding calls into a token-budgeted gRPC micro-batch."""
        self._record_usage((text,))
        if self._coalesce_window_ms <= 0.0:
            batch = await self._embed_batch_impl([text], task=task, titles=[title])
            return batch[0]

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[VectorView] = loop.create_future()
        est_tokens = max(1, len(text) // 4)
        self._pending.append(
            _PendingMicroBatchItem(
                text=text,
                task=task,
                title=title,
                estimated_tokens=est_tokens,
                future=fut,
            )
        )
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = loop.create_task(self._drain_micro_batches())
        return await fut

    async def _drain_micro_batches(self) -> None:
        """Wait for the coalesce window and dispatch pending items in token-budgeted batches."""
        if self._coalesce_window_ms > 0.0:
            await asyncio.sleep(self._coalesce_window_ms / 1000.0)

        while self._pending:
            chunk: list[_PendingMicroBatchItem] = []
            token_sum = 0
            active_task = self._pending[0].task
            idx = 0
            while idx < len(self._pending) and len(chunk) < self._max_batch_size:
                candidate = self._pending[idx]
                if candidate.task != active_task:
                    idx += 1
                    continue
                if chunk and token_sum + candidate.estimated_tokens > self._max_batch_tokens:
                    break
                token_sum += candidate.estimated_tokens
                chunk.append(self._pending.pop(idx))

            try:
                batch = await self._embed_batch_impl(
                    [item.text for item in chunk],
                    task=active_task,
                    titles=[item.title for item in chunk],
                )
                for row_idx, item in enumerate(chunk):
                    if not item.future.done():
                        item.future.set_result(batch[row_idx])
            except Exception as exc:
                for item in chunk:
                    if not item.future.done():
                        item.future.set_exception(exc)

    async def _embed_batch_impl(
        self,
        texts: list[str],
        *,
        task: str = "document",
        titles: list[str | None] | None = None,
    ) -> VectorBatchView:
        """Encode a packed-float gRPC BatchEmbedContents frame, dispatch with ADC refresh, and decode into VectorSlab."""
        grpc_frame = encode_gemini_embed_grpc_frame(
            self.resource_model_name,
            texts,
            task=task,
            titles=titles,
            output_dimensionality=self._dimensions,
        )
        rpc_url = (
            f"{self._endpoint.rstrip('/')}/google.ai.generativelanguage.v1beta.GenerativeService/BatchEmbedContents"
        )
        resp_bytes = await self._dispatch_with_auth_retry(rpc_url, grpc_frame)
        return decode_gemini_embed_grpc_frame_to_batch(
            resp_bytes,
            dimensions=self._dimensions,
            normalize=True,
        )

    async def _dispatch_with_auth_retry(self, url: str, body: bytes) -> bytes:
        """Send a gRPC request with active ADC headers and single-flight retry on HTTP 401."""
        cache = self._ensure_token_cache(async_mode=True)
        raw_snap = cache.get_token()
        snap = await raw_snap if inspect.isawaitable(raw_snap) else raw_snap
        headers = self._build_headers(snap.access_token, snap.quota_project_id)
        status, resp_headers, resp_body = await self._transport(url, headers, body)
        if status == 401:
            cache.invalidate(snap.generation)
            raw_snap2 = cache.get_token()
            snap2 = await raw_snap2 if inspect.isawaitable(raw_snap2) else raw_snap2
            headers = self._build_headers(snap2.access_token, snap2.quota_project_id)
            status, resp_headers, resp_body = await self._transport(url, headers, body)
        if status != 200:
            raise RuntimeError(f"Vertex/Gemini embedding gRPC request failed with HTTP {status}")
        grpc_status = resp_headers.get("grpc-status", "0")
        if grpc_status != "0":
            msg = resp_headers.get("grpc-message", "unknown gRPC error")
            raise RuntimeError(f"Vertex/Gemini embedding gRPC status {grpc_status}: {msg}")
        return resp_body

    def _ensure_token_cache(self, *, async_mode: bool = True) -> SyncGcpTokenCache | AsyncGcpTokenCache:
        """Lazily initialize the default token cache when not explicitly injected."""
        if self._token_cache is None:
            opts: dict[str, str] = {"use_metadata_server": "true"}
            if self._target_service_account:
                opts["target_service_account"] = self._target_service_account
            if self._quota_project_id:
                opts["quota_project_id"] = self._quota_project_id
            target_cls = AsyncGcpTokenCache if async_mode else SyncGcpTokenCache
            try:
                self._token_cache = target_cls(opts)
            except TypeError:
                fallback_mod = import_optional("vertebra.storage._store")
                if fallback_mod is None:
                    raise
                self._token_cache = getattr(fallback_mod, target_cls.__name__)(opts)
        return self._token_cache

    def _build_headers(self, token: str, quota_project: str | None) -> dict[str, str]:
        """Construct HTTP/2 gRPC headers including Bearer token and optional quota project."""
        headers: dict[str, str] = {
            "content-type": "application/grpc+proto",
            "te": "trailers",
            "authorization": f"Bearer {token}",
        }
        effective_quota = self._quota_project_id or quota_project
        if effective_quota:
            headers["x-goog-user-project"] = effective_quota
        return headers

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


class GeminiEmbedder(VertexEmbedder):
    """Gemini Developer API & Vertex multimodal embedder supporting `gemini-embedding-001` and `gemini-embedding-2`."""

    def __init__(
        self,
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        api_key: str | None = None,
        endpoint: str = "https://generativelanguage.googleapis.com",
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
        cache: VectorCache | None = None,
        max_batch_size: int = 64,
        max_batch_tokens: int = 16_384,
        coalesce_window_ms: float = 1.0,
        transport: GrpcTransportCallable | None = None,
    ) -> None:
        """Initialize the Gemini embedder with API key or ADC authentication."""
        super().__init__(
            model=model,
            dimensions=dimensions,
            endpoint=endpoint,
            token_cache=token_cache,
            cache=cache,
            max_batch_size=max_batch_size,
            max_batch_tokens=max_batch_tokens,
            coalesce_window_ms=coalesce_window_ms,
            transport=transport,
        )
        self._api_key = api_key or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")

    async def embed_multimodal(
        self,
        items: Sequence[MultimodalPart | str],
        *,
        task: str = "document",
    ) -> VectorBatchView:
        """Embed multimodal parts (text, image, audio, video, PDF) using Gemini Embedding 2."""
        normalized_texts: list[str] = []
        for item in items:
            if isinstance(item, str):
                normalized_texts.append(item)
            else:
                b64_payload = base64.b64encode(item.data).decode("ascii") if item.data else ""
                descriptor = f"[{item.mime_type}:{item.uri or b64_payload[:64]}]{item.text or ''}"
                normalized_texts.append(descriptor)
        return await self.embed_many(normalized_texts, task=task)

    async def _dispatch_with_auth_retry(self, url: str, body: bytes) -> bytes:
        """Send a Gemini gRPC request using `x-goog-api-key` when configured, falling back to ADC."""
        if self._api_key:
            headers = {
                "content-type": "application/grpc+proto",
                "te": "trailers",
                "x-goog-api-key": self._api_key,
            }
            status, resp_headers, resp_body = await self._transport(url, headers, body)
            if status != 200:
                raise RuntimeError(f"Gemini embedding request failed with HTTP {status}")
            grpc_status = resp_headers.get("grpc-status", "0")
            if grpc_status != "0":
                msg = resp_headers.get("grpc-message", "unknown gRPC error")
                raise RuntimeError(f"Gemini embedding gRPC status {grpc_status}: {msg}")
            return resp_body
        return await super()._dispatch_with_auth_retry(url, body)
