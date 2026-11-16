"""Google Cloud embedders for Vertex AI REST and Gemini gRPC."""

from __future__ import annotations

import asyncio
import base64
import inspect
import os
import threading
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, ClassVar, Literal, Protocol

import msgspec

from vertebra._vertebra import (
    AsyncGcpTokenCache,
    GcpTokenSnapshot,
    GoogleCloudAuthError,
    SyncGcpTokenCache,
    VectorBatchView,
    VectorCache,
    VectorView,
    decode_gemini_embed_grpc_frame_to_batch,
    encode_gemini_embed_contents_grpc_frame,
    encode_gemini_embed_grpc_frame,
    gcp_http2_post_async,
)
from vertebra.embeddings._base import Embedder, EmbeddingError, MultimodalPart
from vertebra.embeddings._models import get_model_spec
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "GcpTokenCachePort",
    "GeminiEmbedder",
    "GrpcTransportCallable",
    "VertexEmbedder",
    "_GoogleEmbedder",
)


class GcpTokenCachePort(Protocol):
    """Structural OAuth access-token cache port accepted by Google embedders."""

    def get_token(self) -> GcpTokenSnapshot | Awaitable[GcpTokenSnapshot]:
        """Return an OAuth access-token snapshot."""
        ...

    def invalidate(self, failed_generation: int, /) -> Any:
        """Invalidate a rejected token generation."""
        ...


GrpcTransportCallable = Callable[
    [str, dict[str, str], bytes],
    Awaitable[tuple[int, dict[str, str], bytes]],
]

_VERTEX_TASK_MAP: dict[str, str] = {
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

_PREDICT_MAX_ESTIMATED_TOKENS = 20_000


class _GoogleEmbedder(Embedder):
    """Shared base for Google Cloud Vertex AI and Gemini embedders."""

    DEFAULT_SCOPES: ClassVar[tuple[str, ...]] = ("https://www.googleapis.com/auth/cloud-platform",)

    def __init__(
        self,
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        endpoint: str,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | GcpTokenCachePort | None = None,
        target_service_account: str | None = None,
        quota_project_id: str | None = None,
        cache: VectorCache | None = None,
        max_batch_size: int = 64,
        max_batch_tokens: int = 16_384,
        coalesce_window_ms: float = 1.0,
        transport: GrpcTransportCallable | None = None,
        provider_label: str = "Google",
    ) -> None:
        """Initialize shared token cache normalization, transport, and per-loop micro-batching."""
        super().__init__(model=model, dimensions=dimensions, cache=cache)
        self._endpoint = endpoint.rstrip("/")
        if isinstance(token_cache, SyncGcpTokenCache):
            self._token_cache: Any = token_cache.to_async()
        else:
            self._token_cache = token_cache
        self._target_service_account = target_service_account
        self._quota_project_id = quota_project_id
        self._max_batch_size = max(1, max_batch_size)
        self._max_batch_tokens = max(1, max_batch_tokens)
        self._coalesce_window_ms = max(0.0, coalesce_window_ms)
        self._transport = transport or self._default_http_transport
        self._provider_label = provider_label
        self._queues_lock = threading.Lock()
        self._queues: dict[asyncio.AbstractEventLoop, _LoopMicroBatchQueue] = {}

    @property
    def resource_model_name(self) -> str:
        """Return the canonical resource path for the model."""
        if self._model.startswith(("models/", "projects/")):
            return self._model
        return f"models/{self._model}"

    @staticmethod
    def _decode_error_snippet(body: bytes, *, limit: int = 256) -> str:
        """Decode an HTTP error response snippet safely for diagnostics."""
        if not body:
            return ""
        snippet = body[:limit].decode("utf-8", errors="replace").strip()
        return f": {snippet}" if snippet else ""

    @staticmethod
    def _validate_multimodal_part(part: MultimodalPart, *, allow_gcs: bool, provider: str) -> None:
        """Validate MultimodalPart payload fields before serialization."""
        if part.data is not None and part.uri is not None:
            raise ValueError("MultimodalPart cannot specify both data and uri")
        if part.uri is not None:
            if not part.uri.strip():
                raise ValueError("MultimodalPart uri must not be empty")
            if not allow_gcs and part.uri.startswith("gs://"):
                raise ValueError(f"{provider} does not support gs:// URIs; use VertexEmbedder for GCS media URIs")
            if not part.mime_type.strip():
                raise ValueError("MultimodalPart mime_type must not be empty")
            return
        if part.data is not None:
            if len(part.data) == 0:
                raise ValueError("MultimodalPart data must not be empty")
            if not part.mime_type.strip():
                raise ValueError("MultimodalPart mime_type must not be empty")
            return
        if part.text is None or not part.text:
            raise ValueError("MultimodalPart must specify data, uri, or text")

    def _validate_single_request(self, text: str, *, task: str, title: str | None) -> None:
        """Hook for subclasses to validate per-request task/title constraints before queuing."""
        _ = (text, task, title)

    async def _embed_single_impl(
        self,
        text: str,
        *,
        task: str = "query",
        title: str | None = None,
    ) -> VectorView:
        """Coalesce concurrent singular embedding calls per running event loop."""
        self._validate_single_request(text, task=task, title=title)
        self._record_usage((text,))
        if self._coalesce_window_ms <= 0.0:
            batch = await self._embed_batch_impl([text], task=task, titles=[title])
            return batch[0]

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[VectorView] = loop.create_future()
        est_tokens = max(1, len(text) // 4)
        pending_item = _PendingMicroBatchItem(
            text=text,
            task=task,
            title=title,
            estimated_tokens=est_tokens,
            future=fut,
        )
        with self._queues_lock:
            closed_loops = [existing for existing in self._queues if existing.is_closed() and existing is not loop]
            for closed_loop in closed_loops:
                self._queues.pop(closed_loop, None)
            queue = self._queues.get(loop)
            if queue is None:
                queue = _LoopMicroBatchQueue()
                self._queues[loop] = queue
            queue.pending.append(pending_item)
            if queue.flush_task is None or queue.flush_task.done():
                queue.flush_task = loop.create_task(self._drain_micro_batches(loop, queue))
        return await fut

    def _pop_pending_chunk(self, queue: _LoopMicroBatchQueue) -> tuple[str, list[_PendingMicroBatchItem]] | None:
        """Pop one task-consistent, token-budgeted chunk from `queue.pending` under lock."""
        with self._queues_lock:
            if not queue.pending:
                return None
            chunk: list[_PendingMicroBatchItem] = []
            token_sum = 0
            active_task = queue.pending[0].task
            idx = 0
            while idx < len(queue.pending) and len(chunk) < self._max_batch_size:
                candidate = queue.pending[idx]
                if candidate.task != active_task:
                    idx += 1
                    continue
                if chunk and token_sum + candidate.estimated_tokens > self._max_batch_tokens:
                    break
                token_sum += candidate.estimated_tokens
                chunk.append(queue.pending.pop(idx))
            return (active_task, chunk)

    async def _drain_micro_batches(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: _LoopMicroBatchQueue,
    ) -> None:
        """Wait for the coalesce window and dispatch pending items for a single event loop."""
        current_chunk: list[_PendingMicroBatchItem] = []
        try:
            if self._coalesce_window_ms > 0.0:
                await asyncio.sleep(self._coalesce_window_ms / 1000.0)

            while True:
                popped = self._pop_pending_chunk(queue)
                if popped is None:
                    break
                active_task, current_chunk = popped
                try:
                    batch = await self._embed_batch_impl(
                        [item.text for item in current_chunk],
                        task=active_task,
                        titles=[item.title for item in current_chunk],
                    )
                    for row_idx, item in enumerate(current_chunk):
                        if not item.future.done():
                            item.future.set_result(batch[row_idx])
                except asyncio.CancelledError:
                    for item in current_chunk:
                        if not item.future.done():
                            item.future.cancel()
                    raise
                except Exception as exc:
                    for item in current_chunk:
                        if not item.future.done():
                            item.future.set_exception(exc)
                finally:
                    current_chunk = []
        except asyncio.CancelledError:
            with self._queues_lock:
                remaining = list(queue.pending)
                queue.pending.clear()
            for item in remaining:
                if not item.future.done():
                    item.future.cancel()
            raise
        finally:
            with self._queues_lock:
                if queue.pending and not loop.is_closed():
                    queue.flush_task = loop.create_task(self._drain_micro_batches(loop, queue))
                else:
                    queue.flush_task = None
                    if self._queues.get(loop) is queue:
                        self._queues.pop(loop, None)

    def _ensure_token_cache(self) -> Any:
        """Lazily initialize the default AsyncGcpTokenCache using canonical ADC precedence."""
        if self._token_cache is None:
            opts: dict[str, str] = {}
            if self._target_service_account:
                opts["target_service_account"] = self._target_service_account
            if self._quota_project_id:
                opts["quota_project_id"] = self._quota_project_id
            try:
                self._token_cache = AsyncGcpTokenCache(opts, self.DEFAULT_SCOPES)
            except ValueError as exc:
                raise ImproperConfigurationError(
                    f"{self._provider_label} failed to resolve GCP credentials from ADC: {exc}"
                ) from exc
        return self._token_cache

    def _build_auth_headers(self, token: str, quota_project: str | None) -> dict[str, str]:
        """Construct Bearer authorization and optional quota-project headers."""
        headers: dict[str, str] = {"authorization": f"Bearer {token}"}
        effective_quota = self._quota_project_id or quota_project
        if effective_quota:
            headers["x-goog-user-project"] = effective_quota
        return headers

    def _check_response_status(
        self,
        status: int,
        resp_headers: dict[str, str],
        resp_body: bytes,
    ) -> dict[str, str]:
        """Validate HTTP and optional gRPC status headers, raising `EmbeddingError` on failure."""
        normalized_headers = {k.lower(): v for k, v in resp_headers.items()}
        if status != 200:
            raise EmbeddingError(
                f"{self._provider_label} embedding request failed with HTTP {status}{self._decode_error_snippet(resp_body)}"
            )
        grpc_status = normalized_headers.get("grpc-status")
        if grpc_status is not None and grpc_status != "0":
            msg = normalized_headers.get("grpc-message", "unknown gRPC error")
            raise EmbeddingError(f"{self._provider_label} embedding gRPC status {grpc_status}: {msg}")
        return normalized_headers

    async def _acquire_token_snapshot(self, cache: Any) -> Any:
        """Fetch an OAuth token snapshot, translating credential resolution errors to `ImproperConfigurationError`."""
        try:
            raw_snap = cache.get_token()
            return await raw_snap if inspect.isawaitable(raw_snap) else raw_snap
        except (ValueError, GoogleCloudAuthError) as exc:
            raise ImproperConfigurationError(
                f"{self._provider_label} failed to resolve GCP credentials from ADC: {exc}"
            ) from exc

    async def _post_authorized(
        self,
        url: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[dict[str, str], bytes]:
        """Send an authorized POST request with single-flight token refresh retry on HTTP 401."""
        cache = self._ensure_token_cache()
        snap = await self._acquire_token_snapshot(cache)
        req_headers = {**headers, **self._build_auth_headers(snap.access_token, snap.quota_project_id)}
        status, resp_headers, resp_body = await self._transport(url, req_headers, body)
        if status == 401:
            cache.invalidate(snap.generation)
            snap2 = await self._acquire_token_snapshot(cache)
            req_headers2 = {**headers, **self._build_auth_headers(snap2.access_token, snap2.quota_project_id)}
            status, resp_headers, resp_body = await self._transport(url, req_headers2, body)
        normalized_headers = self._check_response_status(status, resp_headers, resp_body)
        return (normalized_headers, resp_body)

    @staticmethod
    def _require_rows(batch: VectorBatchView, expected: int, provider: str) -> VectorBatchView:
        """Verify returned row count matches input item count, raising `EmbeddingError` on mismatch."""
        if batch.count != expected:
            raise EmbeddingError(f"{provider} returned {batch.count} embedding vectors, expected {expected}")
        return batch

    @staticmethod
    async def _default_http_transport(
        url: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[int, dict[str, str], bytes]:
        """Execute a connection-pooled HTTP/2 POST via the native Rust transport."""
        return await gcp_http2_post_async(url, headers, body)


class VertexEmbedder(_GoogleEmbedder):
    """Google Cloud Vertex AI v1 REST embedder for `:predict` and `:embedContent` endpoints."""

    def __init__(
        self,
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        project: str | None = None,
        location: str = "us-central1",
        endpoint: str | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | GcpTokenCachePort | None = None,
        target_service_account: str | None = None,
        quota_project_id: str | None = None,
        cache: VectorCache | None = None,
        max_batch_size: int = 64,
        max_batch_tokens: int = 16_384,
        coalesce_window_ms: float = 1.0,
        max_concurrent_requests: int = 8,
        auto_truncate: bool = False,
        transport: GrpcTransportCallable | None = None,
    ) -> None:
        """Initialize the Vertex AI REST embedder without eager network I/O."""
        resolved_endpoint = (
            endpoint
            if endpoint is not None
            else (
                "https://aiplatform.googleapis.com"
                if location == "global"
                else f"https://{location}-aiplatform.googleapis.com"
            )
        )
        super().__init__(
            model=model,
            dimensions=dimensions,
            endpoint=resolved_endpoint,
            token_cache=token_cache,
            target_service_account=target_service_account,
            quota_project_id=quota_project_id,
            cache=cache,
            max_batch_size=max_batch_size,
            max_batch_tokens=max_batch_tokens,
            coalesce_window_ms=coalesce_window_ms,
            transport=transport,
            provider_label="VertexEmbedder",
        )
        self._project = project or os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT") or ""
        self._location = location
        self._max_concurrent_requests = max(1, max_concurrent_requests)
        self._auto_truncate = bool(auto_truncate)

    @property
    def project(self) -> str:
        """Return the configured Google Cloud project ID."""
        return self._project

    @property
    def location(self) -> str:
        """Return the configured Google Cloud region."""
        return self._location

    async def embed_multimodal(
        self,
        items: Sequence[MultimodalPart | str],
        *,
        task: str = "document",
    ) -> VectorBatchView:
        """Embed multimodal parts (`inlineData`, `fileData` including `gs://`, `text`) via Vertex `:embedContent`."""
        method, _ = self._vertex_model_spec()
        if method != "embed_content":
            raise ValueError(
                f"VertexEmbedder.embed_multimodal requires an embedContent model such as gemini-embedding-2, "
                f"got {self._model!r}"
            )
        if not items:
            raise ValueError("items must not be empty")
        api_task = self._resolve_task_type(task)
        _ = self._require_project()

        item_parts: list[list[dict[str, Any]]] = []
        usage_texts: list[str] = []
        for item in items:
            if isinstance(item, str):
                item_parts.append([{"text": item}])
                usage_texts.append(item)
                continue
            self._validate_multimodal_part(item, allow_gcs=True, provider="VertexEmbedder")
            parts: list[dict[str, Any]] = []
            if item.uri is not None:
                parts.append({"fileData": {"mimeType": item.mime_type, "fileUri": item.uri}})
            elif item.data is not None:
                b64_data = base64.b64encode(item.data).decode("ascii")
                parts.append({"inlineData": {"mimeType": item.mime_type, "data": b64_data}})
            if item.text:
                parts.append({"text": item.text})
                usage_texts.append(item.text)
            item_parts.append(parts)

        self._record_usage(usage_texts)
        rows = await self._run_embed_content_batch(item_parts, api_task=api_task)
        batch = VectorBatchView.from_f32_rows(rows, normalize=True)
        return self._require_rows(batch, len(items), "VertexEmbedder")

    def _require_project(self) -> str:
        """Return configured GCP project ID or raise `ImproperConfigurationError`."""
        if not self._project:
            raise ImproperConfigurationError(
                "VertexEmbedder requires a Google Cloud project (pass project=... or set GOOGLE_CLOUD_PROJECT)"
            )
        return self._project

    def _short_model_name(self) -> str:
        """Return the bare model identifier stripped of resource prefixes."""
        return self._model.rsplit("/", 1)[-1]

    def _vertex_model_spec(self) -> tuple[Literal["predict", "embed_content"], int]:
        """Resolve Vertex AI method and max instances per request from `MODEL_CATALOG`."""
        spec = get_model_spec(self._short_model_name())
        if spec is None:
            return ("predict", 1)
        return (spec.vertex_method, max(1, spec.max_instances_per_request))

    def _vertex_url(self, rpc_method: str) -> str:
        """Build the Vertex AI v1 REST publisher model endpoint URL."""
        project = self._require_project()
        if self._model.startswith("projects/"):
            return f"{self._endpoint}/v1/{self._model}:{rpc_method}"
        return (
            f"{self._endpoint}/v1/projects/{project}/locations/{self._location}"
            f"/publishers/google/models/{self._short_model_name()}:{rpc_method}"
        )

    @staticmethod
    def _resolve_task_type(task: str) -> str:
        """Map canonical `EmbeddingTask` names to Vertex AI task_type enum strings."""
        mapped = _VERTEX_TASK_MAP.get(task.strip().lower())
        if mapped is None:
            raise ValueError(f"unsupported embedding task {task!r}")
        return mapped

    @staticmethod
    def _validate_titles(task: str, titles: Sequence[str | None] | None) -> None:
        """Enforce Vertex rule that titles are only valid with document embedding tasks."""
        if titles is None:
            return
        if any(title is not None for title in titles) and task not in (
            "document",
            "search_document",
            "retrieval_document",
        ):
            raise ValueError("titles are only supported for 'document' or 'search_document' tasks")

    def _validate_single_request(self, text: str, *, task: str, title: str | None) -> None:
        """Validate task and title before entering micro-batch queueing."""
        _ = text
        self._resolve_task_type(task)
        if title is not None:
            self._validate_titles(task, (title,))

    def _validate_vector_row(self, values: Any) -> list[float]:
        """Verify returned vector payload is a list of length `self._dimensions`."""
        if not isinstance(values, list):
            raise EmbeddingError("VertexEmbedder response missing embedding values list")
        if len(values) != self._dimensions:
            raise EmbeddingError(f"VertexEmbedder returned {len(values)} dimensions, expected {self._dimensions}")
        return [float(v) for v in values]

    async def _post_vertex_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Serialize a Vertex JSON request body, dispatch with auth/401 retry, and decode JSON."""
        body = encode_json_bytes(payload)
        _, resp_body = await self._post_authorized(url, {"content-type": "application/json"}, body)
        try:
            decoded = decode_json(resp_body)
        except Exception as exc:
            raise EmbeddingError(f"VertexEmbedder received invalid JSON response: {exc}") from exc
        if not isinstance(decoded, dict):
            raise EmbeddingError("VertexEmbedder response JSON must be an object")
        return decoded

    async def _run_predict_batch(
        self,
        texts: list[str],
        *,
        api_task: str,
        titles: list[str | None],
        max_instances: int,
    ) -> list[list[float]]:
        """Chunk texts by instance limit and 20k token budget and dispatch `:predict` concurrently."""
        url = self._vertex_url("predict")
        chunks: list[list[dict[str, Any]]] = []
        current_chunk: list[dict[str, Any]] = []
        current_tokens = 0

        for idx, text in enumerate(texts):
            est_tokens = max(1, len(text) // 4)
            if current_chunk and (
                len(current_chunk) >= max_instances or current_tokens + est_tokens > _PREDICT_MAX_ESTIMATED_TOKENS
            ):
                chunks.append(current_chunk)
                current_chunk = []
                current_tokens = 0
            instance: dict[str, Any] = {"content": text, "task_type": api_task}
            title = titles[idx]
            if title:
                instance["title"] = title
            current_chunk.append(instance)
            current_tokens += est_tokens
        if current_chunk:
            chunks.append(current_chunk)

        parameters: dict[str, Any] = {
            "outputDimensionality": self._dimensions,
            "autoTruncate": self._auto_truncate,
        }
        chunk_rows: list[list[list[float]]] = [[] for _ in chunks]
        semaphore = asyncio.Semaphore(self._max_concurrent_requests)

        async def _send_chunk(chunk_idx: int, instances: list[dict[str, Any]]) -> None:
            async with semaphore:
                payload = {"instances": instances, "parameters": parameters}
                resp = await self._post_vertex_json(url, payload)
                predictions = resp.get("predictions")
                if not isinstance(predictions, list) or len(predictions) != len(instances):
                    got = len(predictions) if isinstance(predictions, list) else "missing"
                    raise EmbeddingError(
                        f"VertexEmbedder :predict returned {got} predictions, expected {len(instances)}"
                    )
                parsed_rows: list[list[float]] = []
                for pred in predictions:
                    embeddings_obj = pred.get("embeddings") if isinstance(pred, dict) else None
                    values = embeddings_obj.get("values") if isinstance(embeddings_obj, dict) else None
                    parsed_rows.append(self._validate_vector_row(values))
                chunk_rows[chunk_idx] = parsed_rows

        try:
            async with asyncio.TaskGroup() as tg:
                for chunk_idx, instances in enumerate(chunks):
                    tg.create_task(_send_chunk(chunk_idx, instances))
        except* Exception as eg:
            raise eg.exceptions[0] from eg

        ordered_rows: list[list[float]] = []
        for rows in chunk_rows:
            ordered_rows.extend(rows)
        return ordered_rows

    async def _run_embed_content_batch(
        self,
        item_parts: list[list[dict[str, Any]]],
        *,
        api_task: str,
        titles: Sequence[str | None] | None = None,
    ) -> list[list[float]]:
        """Dispatch per-item Vertex AI `:embedContent` JSON requests with bounded concurrency."""
        url = self._vertex_url("embedContent")
        ordered_rows: list[list[float]] = [[] for _ in item_parts]
        semaphore = asyncio.Semaphore(self._max_concurrent_requests)

        async def _send_item(item_idx: int, parts: list[dict[str, Any]]) -> None:
            async with semaphore:
                payload: dict[str, Any] = {
                    "content": {"role": "user", "parts": parts},
                    "taskType": api_task,
                    "outputDimensionality": self._dimensions,
                    "autoTruncate": self._auto_truncate,
                }
                if titles is not None and titles[item_idx]:
                    payload["title"] = titles[item_idx]
                resp = await self._post_vertex_json(url, payload)
                emb_obj = resp.get("embedding")
                values = emb_obj.get("values") if isinstance(emb_obj, dict) else None
                ordered_rows[item_idx] = self._validate_vector_row(values)

        try:
            async with asyncio.TaskGroup() as tg:
                for item_idx, parts in enumerate(item_parts):
                    tg.create_task(_send_item(item_idx, parts))
        except* Exception as eg:
            raise eg.exceptions[0] from eg

        return ordered_rows

    async def _embed_batch_impl(
        self,
        texts: list[str],
        *,
        task: str = "document",
        titles: list[str | None] | None = None,
    ) -> VectorBatchView:
        """Dispatch text embeddings over Vertex AI v1 REST `:predict` or `:embedContent`."""
        api_task = self._resolve_task_type(task)
        title_list: list[str | None] = list(titles) if titles is not None else [None] * len(texts)
        if len(title_list) != len(texts):
            raise ValueError("titles length must match texts length")
        self._validate_titles(task, title_list)
        _ = self._require_project()

        method, max_instances = self._vertex_model_spec()
        if method == "embed_content":
            parts_per_item = [[{"text": text}] for text in texts]
            rows = await self._run_embed_content_batch(
                parts_per_item,
                api_task=api_task,
                titles=title_list,
            )
        else:
            rows = await self._run_predict_batch(
                texts,
                api_task=api_task,
                titles=title_list,
                max_instances=max_instances,
            )

        batch = VectorBatchView.from_f32_rows(rows, normalize=True)
        return self._require_rows(batch, len(texts), "VertexEmbedder")


class GeminiEmbedder(_GoogleEmbedder):
    """Gemini Developer API gRPC embedder supporting text and native multimodal protobuf parts."""

    def __init__(
        self,
        model: str = "gemini-embedding-001",
        *,
        dimensions: int = 768,
        api_key: str | None = None,
        endpoint: str = "https://generativelanguage.googleapis.com",
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | GcpTokenCachePort | None = None,
        target_service_account: str | None = None,
        quota_project_id: str | None = None,
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
            target_service_account=target_service_account,
            quota_project_id=quota_project_id,
            cache=cache,
            max_batch_size=max_batch_size,
            max_batch_tokens=max_batch_tokens,
            coalesce_window_ms=coalesce_window_ms,
            transport=transport,
            provider_label="GeminiEmbedder",
        )
        self._api_key = api_key or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")

    async def embed_multimodal(
        self,
        items: Sequence[MultimodalPart | str],
        *,
        task: str = "document",
    ) -> VectorBatchView:
        """Embed multimodal parts (`inline_data` raw bytes, `file_data` HTTPS URI, text) via Gemini protobuf."""
        if not items:
            raise ValueError("items must not be empty")
        contents: list[list[tuple[Literal["text", "inline_data", "file_data"], str, str | bytes]]] = []
        usage_texts: list[str] = []

        for item in items:
            if isinstance(item, str):
                contents.append([("text", "", item)])
                usage_texts.append(item)
                continue
            self._validate_multimodal_part(item, allow_gcs=False, provider="GeminiEmbedder")
            parts: list[tuple[Literal["text", "inline_data", "file_data"], str, str | bytes]] = []
            if item.uri is not None:
                parts.append(("file_data", item.mime_type, item.uri))
            elif item.data is not None:
                parts.append(("inline_data", item.mime_type, item.data))
            if item.text:
                parts.append(("text", "", item.text))
                usage_texts.append(item.text)
            contents.append(parts)

        self._record_usage(usage_texts)
        grpc_frame = encode_gemini_embed_contents_grpc_frame(
            self.resource_model_name,
            contents,
            task=task,
            output_dimensionality=self._dimensions,
        )
        batch = await self._dispatch_gemini(grpc_frame)
        return self._require_rows(batch, len(items), "GeminiEmbedder")

    def _rpc_url(self) -> str:
        """Return the Gemini `GenerativeService/BatchEmbedContents` RPC URL."""
        return f"{self._endpoint}/google.ai.generativelanguage.v1beta.GenerativeService/BatchEmbedContents"

    async def _dispatch_gemini(self, frame: bytes) -> VectorBatchView:
        """Send a gRPC `BatchEmbedContents` frame via API key or ADC retry and decode results."""
        url = self._rpc_url()
        headers: dict[str, str] = {
            "content-type": "application/grpc+proto",
            "te": "trailers",
        }
        if self._api_key:
            api_headers = {**headers, "x-goog-api-key": self._api_key}
            status, resp_headers, resp_body = await self._transport(url, api_headers, frame)
            self._check_response_status(status, resp_headers, resp_body)
        else:
            _, resp_body = await self._post_authorized(url, headers, frame)
        try:
            return decode_gemini_embed_grpc_frame_to_batch(
                resp_body,
                dimensions=self._dimensions,
                normalize=True,
            )
        except ValueError as exc:
            raise EmbeddingError(f"GeminiEmbedder failed to decode gRPC response: {exc}") from exc

    async def _embed_batch_impl(
        self,
        texts: list[str],
        *,
        task: str = "document",
        titles: list[str | None] | None = None,
    ) -> VectorBatchView:
        """Encode texts into a `BatchEmbedContents` gRPC frame, dispatch, and validate row count."""
        grpc_frame = encode_gemini_embed_grpc_frame(
            self.resource_model_name,
            texts,
            task=task,
            titles=titles,
            output_dimensionality=self._dimensions,
        )
        batch = await self._dispatch_gemini(grpc_frame)
        return self._require_rows(batch, len(texts), "GeminiEmbedder")


class _PendingMicroBatchItem(msgspec.Struct):
    """Queued singular embedding request waiting in an event-loop micro-batch queue."""

    text: str
    task: str
    title: str | None
    estimated_tokens: int
    future: asyncio.Future[VectorView]


class _LoopMicroBatchQueue(msgspec.Struct):
    """Per-event-loop pending micro-batch buffer and active drain task."""

    pending: list[_PendingMicroBatchItem] = msgspec.field(default_factory=list)
    flush_task: asyncio.Task[None] | None = None
