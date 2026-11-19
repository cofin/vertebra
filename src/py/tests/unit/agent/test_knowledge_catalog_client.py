"""Unit tests for native GCP HTTP/2 transport and Knowledge Catalog clients."""

from __future__ import annotations

import asyncio
import socket
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from vertebra._vertebra import (
    AsyncGcpTokenCache,
    GcpTokenSnapshot,
    GoogleCloudAuthError,
    GoogleCloudError,
    SyncGcpTokenCache,
    gcp_http2_post_async,
    gcp_http2_post_sync,
    gcp_http2_request_async,
    gcp_http2_request_sync,
)
from vertebra.agent.knowledge_catalog import (
    MAX_BATCH_SYNC_ENTRIES,
    AsyncKnowledgeCatalogClient,
    CatalogEntryRecord,
    CatalogSyncError,
    CatalogSyncSummary,
    KnowledgeCatalogConfig,
    SyncKnowledgeCatalogClient,
)
from vertebra.agent.okf import OkfBundle, OkfDocument
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json_bytes


@contextmanager
def _ephemeral_http_server() -> Iterator[tuple[str, list[tuple[str, str, dict[str, str], bytes]]]]:
    """Run an ephemeral local HTTP/1.1 server recording method, path, headers, and body."""
    recorded: list[tuple[str, str, dict[str, str], bytes]] = []

    class _Handler(BaseHTTPRequestHandler):
        def _handle(self) -> None:
            length_hdr = self.headers.get("Content-Length")
            req_body = self.rfile.read(int(length_hdr)) if length_hdr is not None else b""
            hdrs = {k.lower(): v for k, v in self.headers.items()}
            recorded.append((self.command, self.path, hdrs, req_body))
            payload = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Echo-Method", self.command)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            self._handle()

        def do_POST(self) -> None:
            self._handle()

        def do_PATCH(self) -> None:
            self._handle()

        def do_PUT(self) -> None:
            self._handle()

        def do_DELETE(self) -> None:
            self._handle()

        def log_message(self, format: str, *args: object) -> None:
            _ = (format, args)

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield (f"http://127.0.0.1:{port}", recorded)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def _unused_loopback_url() -> str:
    """Reserve and immediately release an ephemeral TCP port to construct an unreachable URL."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"http://127.0.0.1:{port}/v1/unreachable"


@pytest.mark.parametrize(
    ("method", "expected_method", "body"),
    [
        ("GET", "GET", None),
        ("get", "GET", None),
        ("DELETE", "DELETE", None),
        ("delete", "DELETE", None),
        ("POST", "POST", b'{"entryGroupId":"okf-context"}'),
        ("patch", "PATCH", b'{"displayName":"Updated"}'),
        ("PUT", "PUT", b'{"state":"ACTIVE"}'),
    ],
)
def test_gcp_http2_request_sync_roundtrip(
    method: str,
    expected_method: str,
    body: bytes | None,
) -> None:
    """Verify gcp_http2_request_sync sends expected method, headers, and optional body."""
    with _ephemeral_http_server() as (base_url, recorded):
        status, resp_headers, resp_body = gcp_http2_request_sync(
            method,
            f"{base_url}/v1/projects/demo/locations/global/entryGroups",
            {"x-goog-user-project": "demo-project", "authorization": "Bearer test-token-placeholder"},
            body,
            5_000,
        )

    assert status == 200
    assert resp_headers["x-echo-method"] == expected_method
    assert resp_body == b'{"status":"ok"}'
    assert len(recorded) == 1
    req_method, req_path, req_headers, req_body = recorded[0]
    assert req_method == expected_method
    assert req_path == "/v1/projects/demo/locations/global/entryGroups"
    assert req_headers["x-goog-user-project"] == "demo-project"
    assert req_headers["authorization"] == "Bearer test-token-placeholder"
    if body is None:
        assert "content-length" not in req_headers
        assert req_body == b""
    else:
        assert req_body == body


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method", "expected_method", "body"),
    [
        ("GET", "GET", None),
        ("DELETE", "DELETE", None),
        ("POST", "POST", b'{"name":"entry-1"}'),
        ("PATCH", "PATCH", b'{"aspects":{}}'),
        ("put", "PUT", b'{"mode":"replace"}'),
    ],
)
async def test_gcp_http2_request_async_roundtrip(
    method: str,
    expected_method: str,
    body: bytes | None,
) -> None:
    """Verify gcp_http2_request_async sends expected method, headers, and optional body."""
    with _ephemeral_http_server() as (base_url, recorded):
        status, resp_headers, resp_body = await gcp_http2_request_async(
            method,
            f"{base_url}/v1/projects/demo/locations/global/entryGroups/okf-context",
            {"x-test-trace": "trace-123"},
            body,
            5_000,
        )

    assert status == 200
    assert resp_headers["x-echo-method"] == expected_method
    assert resp_body == b'{"status":"ok"}'
    assert len(recorded) == 1
    req_method, _, req_headers, req_body = recorded[0]
    assert req_method == expected_method
    assert req_headers["x-test-trace"] == "trace-123"
    if body is None:
        assert "content-length" not in req_headers
        assert req_body == b""
    else:
        assert req_body == body


@pytest.mark.parametrize("bad_method", ["", "TRACE", "CONNECT", "HEAD", "OPTIONS"])
def test_gcp_http2_request_rejects_invalid_method(bad_method: str) -> None:
    """Verify unsupported HTTP methods raise ValueError before network execution."""
    with pytest.raises(ValueError, match="unsupported HTTP method"):
        gcp_http2_request_sync(bad_method, "http://127.0.0.1:9/unused", {})

    with pytest.raises(ValueError, match="unsupported HTTP method"):
        gcp_http2_request_async(bad_method, "http://127.0.0.1:9/unused", {})


@pytest.mark.parametrize("bad_timeout", [0, -1, -500])
def test_gcp_http2_request_rejects_non_positive_timeout(bad_timeout: int) -> None:
    """Verify non-positive timeout_ms raises ValueError before detaching the GIL."""
    with pytest.raises(ValueError, match="timeout_ms"):
        gcp_http2_request_sync("GET", "http://127.0.0.1:9/unused", {}, None, bad_timeout)

    with pytest.raises(ValueError, match="timeout_ms"):
        gcp_http2_request_async("GET", "http://127.0.0.1:9/unused", {}, None, bad_timeout)


def test_gcp_http2_request_sync_unreachable_raises_google_cloud_error() -> None:
    """Verify sync transport errors map to GoogleCloudError and not GoogleCloudAuthError."""
    url = _unused_loopback_url()
    with pytest.raises(GoogleCloudError) as exc_info:
        gcp_http2_request_sync("GET", url, {}, None, 2_000)
    assert not isinstance(exc_info.value, GoogleCloudAuthError)


@pytest.mark.anyio
async def test_gcp_http2_request_async_unreachable_raises_google_cloud_error() -> None:
    """Verify async transport errors map to GoogleCloudError and not GoogleCloudAuthError."""
    url = _unused_loopback_url()
    with pytest.raises(GoogleCloudError) as exc_info:
        await gcp_http2_request_async("DELETE", url, {}, None, 2_000)
    assert not isinstance(exc_info.value, GoogleCloudAuthError)


@pytest.mark.anyio
async def test_gcp_http2_post_bindings_remain_intact() -> None:
    """Verify existing gcp_http2_post_sync and gcp_http2_post_async remain functional."""
    with _ephemeral_http_server() as (base_url, recorded):
        sync_status, _, sync_body = gcp_http2_post_sync(
            f"{base_url}/v1/post-sync",
            {"content-type": "application/json"},
            b'{"sync":true}',
        )
        async_status, _, async_body = await gcp_http2_post_async(
            f"{base_url}/v1/post-async",
            {"content-type": "application/json"},
            b'{"async":true}',
        )

    assert sync_status == 200
    assert sync_body == b'{"status":"ok"}'
    assert async_status == 200
    assert async_body == b'{"status":"ok"}'
    assert [entry[0] for entry in recorded] == ["POST", "POST"]
    assert [entry[3] for entry in recorded] == [b'{"sync":true}', b'{"async":true}']


class _SyncFakeTokenCache:
    """In-memory synchronous OAuth2 token cache supporting generation invalidation."""

    def __init__(self, *, quota_project_id: str | None = None) -> None:
        self._generation = 0
        self._invalidated_up_to = 0
        self._quota_project_id = quota_project_id
        self.invalidated: list[int] = []

    def invalidate(self, failed_generation: int, /) -> None:
        self.invalidated.append(failed_generation)
        if failed_generation >= self._invalidated_up_to:
            self._invalidated_up_to = failed_generation

    def get_token(self) -> GcpTokenSnapshot:
        if self._generation == 0 or self._generation <= self._invalidated_up_to:
            self._generation += 1
        return GcpTokenSnapshot(
            access_token=f"test-token-placeholder-{self._generation}",
            expires_at_unix_ms=4_102_444_800_000,
            generation=self._generation,
            quota_project_id=self._quota_project_id,
        )


class _AsyncFakeTokenCache:
    """In-memory asynchronous OAuth2 token cache supporting generation invalidation."""

    def __init__(self, *, quota_project_id: str | None = None) -> None:
        self._sync = _SyncFakeTokenCache(quota_project_id=quota_project_id)

    @property
    def invalidated(self) -> list[int]:
        return self._sync.invalidated

    def invalidate(self, failed_generation: int, /) -> None:
        self._sync.invalidate(failed_generation)

    async def get_token(self) -> GcpTokenSnapshot:
        return self._sync.get_token()


def _make_okf_doc(rel_path: str, title: str, body: str = "Primary verified knowledge.") -> OkfDocument:
    """Build a valid `OkfDocument` for Knowledge Catalog sync tests."""
    raw = (
        "---\n"
        "type: Guide\n"
        f'title: "{title}"\n'
        'description: "Catalog sync doc."\n'
        "status: stable\n"
        "sources:\n"
        "  - id: s1\n"
        '    resource: "src/app.py"\n'
        '    title: "App"\n'
        "---\n\n"
        f"# {title}\n\n"
        "<!-- truth: start -->\n"
        f"- {body} [^s1].\n"
        "<!-- truth: end -->\n\n"
        "[^s1]: `src/app.py` — App.\n"
    )
    return OkfDocument.parse(rel_path, raw)


@pytest.mark.anyio
async def test_config_validation_and_token_cache_normalization() -> None:
    """Verify KnowledgeCatalogConfig constraints, default transports, and token cache normalization."""
    assert MAX_BATCH_SYNC_ENTRIES == 250
    with pytest.raises(ImproperConfigurationError, match="project_id"):
        KnowledgeCatalogConfig(project_id="  ")
    with pytest.raises(ImproperConfigurationError, match="location"):
        KnowledgeCatalogConfig(project_id="proj-1", location="")
    with pytest.raises(ImproperConfigurationError, match="endpoint"):
        KnowledgeCatalogConfig(project_id="proj-1", endpoint="  ")
    with pytest.raises(ImproperConfigurationError, match="timeout_ms"):
        KnowledgeCatalogConfig(project_id="proj-1", timeout_ms=0)

    default_cfg = KnowledgeCatalogConfig(project_id="proj-1")
    assert default_cfg.entry_group_id == "okf-context"

    with _ephemeral_http_server() as (base_url, recorded):
        local_cfg = KnowledgeCatalogConfig(project_id="proj-1", endpoint=base_url)

        async_cache = AsyncGcpTokenCache({"access_token": "tok-sync-normalized"}, allow_ambient_adc=False)
        sync_client = SyncKnowledgeCatalogClient(local_cfg, token_cache=async_cache)
        assert sync_client.list_entry_groups() == []

        sync_cache = SyncGcpTokenCache({"access_token": "tok-async-normalized"}, allow_ambient_adc=False)
        async_client = AsyncKnowledgeCatalogClient(local_cfg, token_cache=sync_cache)
        assert await async_client.list_entry_groups() == []

    assert len(recorded) == 2
    assert recorded[0][0] == "GET"
    assert recorded[0][2]["authorization"] == "Bearer tok-sync-normalized"
    assert recorded[1][0] == "GET"
    assert recorded[1][2]["authorization"] == "Bearer tok-async-normalized"


@pytest.mark.anyio
@pytest.mark.parametrize("use_async", [False, True])
async def test_catalog_401_invalidation_retry_and_quota_header(use_async: bool) -> None:
    """Verify 401 invalidates failed token generation, retries once, and sends x-goog-user-project."""
    calls: list[tuple[str, str, dict[str, str], bytes | None, int]] = []

    def handler(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        calls.append((method, url, headers, body, timeout_ms))
        if len(calls) == 1:
            err = {"error": {"code": 401, "status": "UNAUTHENTICATED", "message": "expired"}}
            return (401, {}, encode_json_bytes(err))
        return (200, {}, encode_json_bytes({"name": "projects/p/locations/us-central1/entryGroups/okf-context"}))

    cfg = KnowledgeCatalogConfig(project_id="p", quota_project_id="quota-p")
    if use_async:
        cache_a = _AsyncFakeTokenCache()

        async def async_transport(
            method: str,
            url: str,
            headers: dict[str, str],
            body: bytes | None,
            timeout_ms: int,
        ) -> tuple[int, dict[str, str], bytes]:
            return handler(method, url, headers, body, timeout_ms)

        client_a = AsyncKnowledgeCatalogClient(cfg, token_cache=cache_a, transport=async_transport)
        group = await client_a.get_entry_group()
        invalidated = cache_a.invalidated
    else:
        cache_s = _SyncFakeTokenCache()
        client_s = SyncKnowledgeCatalogClient(cfg, token_cache=cache_s, transport=handler)
        group = client_s.get_entry_group()
        invalidated = cache_s.invalidated

    assert group["name"].endswith("/entryGroups/okf-context")
    assert invalidated == [1]
    assert len(calls) == 2
    assert calls[0][2]["authorization"] == "Bearer test-token-placeholder-1"
    assert calls[1][2]["authorization"] == "Bearer test-token-placeholder-2"
    assert calls[0][2]["x-goog-user-project"] == "quota-p"
    assert calls[1][2]["x-goog-user-project"] == "quota-p"


@pytest.mark.anyio
async def test_catalog_non_2xx_parses_google_cloud_error() -> None:
    """Verify non-2xx Dataplex responses populate GoogleCloudError code, status, reason, and retry_delay_ms."""
    err_payload = {
        "error": {
            "code": 429,
            "message": "Quota exceeded for Dataplex requests.",
            "status": "RESOURCE_EXHAUSTED",
            "details": [
                {"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "RATE_LIMIT_EXCEEDED"},
                {"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "1.5s"},
            ],
        }
    }

    def sync_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        _ = (method, url, headers, body, timeout_ms)
        return (429, {}, encode_json_bytes(err_payload))

    async def async_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        return sync_transport(method, url, headers, body, timeout_ms)

    cfg = KnowledgeCatalogConfig(project_id="p")
    sync_client = SyncKnowledgeCatalogClient(cfg, token_cache=_SyncFakeTokenCache(), transport=sync_transport)
    async_client = AsyncKnowledgeCatalogClient(cfg, token_cache=_AsyncFakeTokenCache(), transport=async_transport)

    with pytest.raises(GoogleCloudError) as exc_info:
        sync_client.get_entry_group()
    assert exc_info.value.code == 8
    assert exc_info.value.code_name == "RESOURCE_EXHAUSTED"
    assert exc_info.value.http_status == 429
    assert exc_info.value.reason == "RATE_LIMIT_EXCEEDED"
    assert exc_info.value.retry_delay_ms == 1500
    assert "Quota exceeded" in exc_info.value.message

    with pytest.raises(GoogleCloudError) as async_exc:
        await async_client.get_entry_group()
    assert async_exc.value.code == 8
    assert async_exc.value.code_name == "RESOURCE_EXHAUSTED"


@pytest.mark.parametrize(
    "bad_id",
    ["@dataplex", "@bigquery", "Bad_Group", "-leading", "trailing-", "a" * 64, "has spaces", ""],
)
def test_entry_group_creation_rejects_non_rfc1035_ids(bad_id: str) -> None:
    """Verify create_entry_group rejects @system and invalid IDs with ImproperConfigurationError."""
    cfg = KnowledgeCatalogConfig(project_id="p")

    def unused_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        raise AssertionError(f"Transport should not be called for invalid ID {bad_id!r}")

    client = SyncKnowledgeCatalogClient(cfg, token_cache=_SyncFakeTokenCache(), transport=unused_transport)
    with pytest.raises(ImproperConfigurationError):
        client.create_entry_group(bad_id)


@pytest.mark.anyio
@pytest.mark.parametrize("system_id", ["@dataplex", "@bigquery", "okf-context", "a"])
async def test_entry_group_lookup_allows_system_and_rfc1035_ids(system_id: str) -> None:
    """Verify read/lookup endpoints permit @* system entryGroups as well as RFC-1035 IDs."""
    seen_urls: list[str] = []

    def sync_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        _ = (method, headers, body, timeout_ms)
        seen_urls.append(url)
        return (200, {}, encode_json_bytes({"name": f"projects/p/locations/us-central1/entryGroups/{system_id}"}))

    async def async_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        return sync_transport(method, url, headers, body, timeout_ms)

    cfg = KnowledgeCatalogConfig(project_id="p")
    sync_client = SyncKnowledgeCatalogClient(cfg, token_cache=_SyncFakeTokenCache(), transport=sync_transport)
    async_client = AsyncKnowledgeCatalogClient(cfg, token_cache=_AsyncFakeTokenCache(), transport=async_transport)

    assert sync_client.get_entry_group(system_id)["name"].endswith(f"/entryGroups/{system_id}")
    assert (await async_client.get_entry_group(system_id))["name"].endswith(f"/entryGroups/{system_id}")
    assert len(seen_urls) == 2


@pytest.mark.anyio
@pytest.mark.parametrize("use_async", [False, True])
async def test_create_entry_group_lro_polling_and_operation_error(use_async: bool) -> None:
    """Verify create_entry_group polls LRO until done and raises GoogleCloudError on operation.error."""
    requests: list[tuple[str, str, dict[str, Any] | None]] = []
    poll_count = 0
    fail_mode = False

    def handle(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        nonlocal poll_count
        _ = (headers, timeout_ms)
        decoded_body = decode_json(body) if body else None
        requests.append((method, url, decoded_body))
        if method == "POST":
            return (
                200,
                {},
                encode_json_bytes({"name": "projects/p/locations/us-central1/operations/op-1", "done": False}),
            )
        poll_count += 1
        if fail_mode:
            return (
                200,
                {},
                encode_json_bytes(
                    {
                        "name": "projects/p/locations/us-central1/operations/op-1",
                        "done": True,
                        "error": {"code": 7, "message": "Permission denied on entryGroup"},
                    }
                ),
            )
        if poll_count == 1:
            return (
                200,
                {},
                encode_json_bytes({"name": "projects/p/locations/us-central1/operations/op-1", "done": False}),
            )
        return (
            200,
            {},
            encode_json_bytes(
                {
                    "name": "projects/p/locations/us-central1/operations/op-1",
                    "done": True,
                    "response": {
                        "name": "projects/p/locations/us-central1/entryGroups/team-okf",
                        "labels": {"type": "context-set"},
                    },
                }
            ),
        )

    cfg = KnowledgeCatalogConfig(project_id="p")

    async def async_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        return handle(method, url, headers, body, timeout_ms)

    client_a = AsyncKnowledgeCatalogClient(
        cfg,
        token_cache=_AsyncFakeTokenCache(),
        transport=async_transport,
        poll_interval_ms=0,
    )
    client_s = SyncKnowledgeCatalogClient(
        cfg,
        token_cache=_SyncFakeTokenCache(),
        transport=handle,
        poll_interval_ms=0,
    )
    if use_async:
        created = await client_a.create_entry_group("team-okf", labels={"type": "context-set"})
    else:
        created = client_s.create_entry_group("team-okf", labels={"type": "context-set"})

    assert created["name"] == "projects/p/locations/us-central1/entryGroups/team-okf"
    assert requests[0][0] == "POST"
    assert "entryGroupId=team-okf" in requests[0][1]
    assert requests[1] == (
        "GET",
        "https://dataplex.googleapis.com/v1/projects/p/locations/us-central1/operations/op-1",
        None,
    )
    assert requests[2] == (
        "GET",
        "https://dataplex.googleapis.com/v1/projects/p/locations/us-central1/operations/op-1",
        None,
    )

    fail_mode = True
    if use_async:
        with pytest.raises(GoogleCloudError) as err_a:
            await client_a.create_entry_group("team-okf")
        assert err_a.value.code == 7
        assert err_a.value.code_name == "PERMISSION_DENIED"
        assert "Permission denied" in err_a.value.message
    else:
        with pytest.raises(GoogleCloudError) as err_s:
            client_s.create_entry_group("team-okf")
        assert err_s.value.code == 7
        assert err_s.value.code_name == "PERMISSION_DENIED"
        assert "Permission denied" in err_s.value.message


@pytest.mark.anyio
@pytest.mark.parametrize("use_async", [False, True])
async def test_list_entry_groups_client_label_filter_and_resolve_context_set(use_async: bool) -> None:
    """Verify list_entry_groups filters labels client-side and resolve_context_set avoids duplicates."""
    seen_queries: list[str] = []
    groups_store: list[dict[str, Any]] = [
        {"name": "projects/p/locations/us-central1/entryGroups/unlabeled-set", "labels": {}},
        {"name": "projects/p/locations/us-central1/entryGroups/other", "labels": {"env": "prod"}},
    ]
    patches: list[tuple[str, dict[str, Any]]] = []
    creates: list[str] = []

    def handle(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        _ = (headers, timeout_ms)
        parsed = urlparse(url)
        seen_queries.append(parsed.query)
        qs = parse_qs(parsed.query)
        payload = decode_json(body) if body else {}
        if method == "GET" and parsed.path.endswith("/entryGroups"):
            if "pageToken" not in qs:
                return (200, {}, encode_json_bytes({"entryGroups": [groups_store[0]], "nextPageToken": "p2"}))
            return (200, {}, encode_json_bytes({"entryGroups": groups_store[1:]}))
        if method == "PATCH":
            patches.append((url, payload))
            for item in groups_store:
                if parsed.path.endswith(item["name"]):
                    item["labels"] = payload["labels"]
                    return (200, {}, encode_json_bytes({"done": True, "response": item}))
        if method == "POST" and parsed.path.endswith("/entryGroups"):
            eg_id = qs["entryGroupId"][0]
            creates.append(eg_id)
            created = {
                "name": f"projects/p/locations/us-central1/entryGroups/{eg_id}",
                "labels": payload.get("labels", {}),
            }
            groups_store.append(created)
            return (200, {}, encode_json_bytes({"done": True, "response": created}))
        raise AssertionError(f"Unexpected request: {method} {url}")

    cfg = KnowledgeCatalogConfig(project_id="p")

    async def async_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        return handle(method, url, headers, body, timeout_ms)

    if use_async:
        client_a = AsyncKnowledgeCatalogClient(cfg, token_cache=_AsyncFakeTokenCache(), transport=async_transport)
        filtered = await client_a.list_entry_groups(labels={"env": "prod"})
        adopted = await client_a.resolve_context_set("unlabeled-set")
        existing = await client_a.resolve_context_set()
        created = await client_a.resolve_context_set("fresh-ctx")
    else:
        client_s = SyncKnowledgeCatalogClient(cfg, token_cache=_SyncFakeTokenCache(), transport=handle)
        filtered = client_s.list_entry_groups(labels={"env": "prod"})
        adopted = client_s.resolve_context_set("unlabeled-set")
        existing = client_s.resolve_context_set()
        created = client_s.resolve_context_set("fresh-ctx")

    assert all("filter=" not in q for q in seen_queries)
    assert [g["name"].rsplit("/", 1)[-1] for g in filtered] == ["other"]
    assert adopted["labels"]["type"] == "context-set"
    assert len(patches) == 1
    assert existing["name"] == adopted["name"]
    assert creates == ["fresh-ctx"]
    assert created["labels"] == {"type": "context-set"}


@pytest.mark.anyio
@pytest.mark.parametrize("use_async", [False, True])
async def test_entry_crud_view_all_and_aspect_keys_patch_encoding(use_async: bool) -> None:
    """Verify get_entry view=ALL, list_entries omitting view, and update_entry repeated aspectKeys."""
    recorded: list[tuple[str, str, dict[str, list[str]], dict[str, Any] | None]] = []

    def handle(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        _ = (headers, timeout_ms)
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        payload = decode_json(body) if body else None
        recorded.append((method, parsed.path, qs, payload))
        if method == "GET" and parsed.path.endswith("/entries/doc-1"):
            return (
                200,
                {},
                encode_json_bytes(
                    {
                        "name": "projects/p/locations/us-central1/entryGroups/okf-context/entries/doc-1",
                        "entryType": "projects/dataplex-types/locations/global/entryTypes/generic",
                        "entrySource": {"displayName": "Doc 1", "labels": {"okf-sha256": "a" * 64}},
                        "aspects": {"dataplex-types.global.overview": {"data": {"content": "hello"}}},
                    }
                ),
            )
        if method == "GET" and parsed.path.endswith("/entries"):
            if "pageToken" not in qs:
                return (
                    200,
                    {},
                    encode_json_bytes(
                        {
                            "entries": [
                                {"name": "projects/p/locations/us-central1/entryGroups/okf-context/entries/e1"}
                            ],
                            "nextPageToken": "page-2",
                        }
                    ),
                )
            return (
                200,
                {},
                encode_json_bytes(
                    {
                        "entries": [{"name": "projects/p/locations/us-central1/entryGroups/okf-context/entries/e2"}],
                    }
                ),
            )
        if method in {"POST", "PATCH"}:
            return (
                200,
                {},
                encode_json_bytes(
                    {
                        "name": "projects/p/locations/us-central1/entryGroups/okf-context/entries/doc-1",
                        "entrySource": (payload or {}).get("entrySource", {}),
                        "aspects": (payload or {}).get("aspects", {}),
                    }
                ),
            )
        if method == "DELETE":
            return (200, {}, b"{}")
        raise AssertionError(f"Unexpected request: {method} {url}")

    cfg = KnowledgeCatalogConfig(project_id="p")

    async def async_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        return handle(method, url, headers, body, timeout_ms)

    aspects_payload = {
        "dataplex-types.global.overview": {"data": {"content": "summary"}},
        "p.global.okf-document": {"data": {"sha256": "b" * 64}},
    }

    if use_async:
        client_a = AsyncKnowledgeCatalogClient(cfg, token_cache=_AsyncFakeTokenCache(), transport=async_transport)
        got = await client_a.get_entry("doc-1")
        listed = await client_a.list_entries(page_size=1)
        created = await client_a.create_entry("doc-1", display_name="Doc 1", labels={"okf-sha256": "a" * 64})
        updated = await client_a.update_entry("doc-1", aspects=aspects_payload, labels={"okf-sha256": "b" * 64})
        await client_a.delete_entry("doc-1")
    else:
        client_s = SyncKnowledgeCatalogClient(cfg, token_cache=_SyncFakeTokenCache(), transport=handle)
        got = client_s.get_entry("doc-1")
        listed = client_s.list_entries(page_size=1)
        created = client_s.create_entry("doc-1", display_name="Doc 1", labels={"okf-sha256": "a" * 64})
        updated = client_s.update_entry("doc-1", aspects=aspects_payload, labels={"okf-sha256": "b" * 64})
        client_s.delete_entry("doc-1")

    assert isinstance(got, CatalogEntryRecord)
    assert got.sha256 == "a" * 64
    assert "dataplex-types.global.overview" in got.aspects
    assert recorded[0][2]["view"] == ["ALL"]

    assert [item.entry_id for item in listed] == ["e1", "e2"]
    assert "view" not in recorded[1][2]
    assert "view" not in recorded[2][2]

    assert created.entry_id == "doc-1"
    assert recorded[3][0] == "POST"
    assert recorded[3][2]["entryId"] == ["doc-1"]

    patch_rec = recorded[4]
    assert patch_rec[0] == "PATCH"
    assert patch_rec[2]["updateMask"] == ["aspects,entrySource"]
    assert patch_rec[2]["aspectKeys"] == ["dataplex-types.global.overview", "p.global.okf-document"]
    assert updated.sha256 == "b" * 64

    assert recorded[5][0] == "DELETE"


@pytest.mark.anyio
@pytest.mark.parametrize("use_async", [False, True])
async def test_sync_bundle_progressive_digest_chunking_and_delete_stale(use_async: bool) -> None:
    """Verify sync_bundle skips unchanged digests, chunks >250 entries (260 -> 250 + 10), and deletes stale."""
    unchanged_doc = _make_okf_doc("knowledge/unchanged.md", "Unchanged")
    changed_doc = _make_okf_doc("knowledge/changed.md", "Changed v2", "Updated truth.")
    new_doc = _make_okf_doc("knowledge/new.md", "New Doc")

    unchanged_id = "knowledge-unchanged-md-ff13e3e1"
    changed_id = "knowledge-changed-md-fa295c26"
    stale_id = "stale-removed-00001111"

    remote_entries: dict[str, dict[str, Any]] = {
        unchanged_id: {
            "name": f"projects/p/locations/us-central1/entryGroups/okf-context/entries/{unchanged_id}",
            "entrySource": {"labels": {"okf-sha256": unchanged_doc.sha256}},
        },
        changed_id: {
            "name": f"projects/p/locations/us-central1/entryGroups/okf-context/entries/{changed_id}",
            "entrySource": {"labels": {"okf-sha256": "0" * 64}},
        },
        stale_id: {
            "name": f"projects/p/locations/us-central1/entryGroups/okf-context/entries/{stale_id}",
            "entrySource": {"system": "okf", "labels": {"okf-sha256": "f" * 64, "okf-format": "v0-2"}},
        },
    }
    mutations: list[tuple[str, str, dict[str, Any] | None]] = []

    def build_transport() -> Callable[[str, str, dict[str, str], bytes | None, int], tuple[int, dict[str, str], bytes]]:
        def handler(
            method: str,
            url: str,
            headers: dict[str, str],
            body: bytes | None,
            timeout_ms: int,
        ) -> tuple[int, dict[str, str], bytes]:
            _ = (headers, timeout_ms)
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            payload = decode_json(body) if body else None
            if method == "GET" and parsed.path.endswith("/entries"):
                return (200, {}, encode_json_bytes({"entries": list(remote_entries.values())}))
            if method == "POST" and parsed.path.endswith("/entries"):
                eid = qs["entryId"][0]
                mutations.append(("POST", eid, payload))
                created = {
                    "name": f"projects/p/locations/us-central1/entryGroups/okf-context/entries/{eid}",
                    **(payload or {}),
                }
                remote_entries[eid] = created
                return (200, {}, encode_json_bytes(created))
            if method == "PATCH":
                eid = parsed.path.rsplit("/", 1)[-1]
                mutations.append(("PATCH", eid, payload))
                updated = {
                    "name": f"projects/p/locations/us-central1/entryGroups/okf-context/entries/{eid}",
                    **(payload or {}),
                }
                remote_entries[eid] = updated
                return (200, {}, encode_json_bytes(updated))
            if method == "DELETE":
                eid = parsed.path.rsplit("/", 1)[-1]
                mutations.append(("DELETE", eid, None))
                remote_entries.pop(eid, None)
                return (200, {}, b"{}")
            raise AssertionError(f"Unexpected request: {method} {url}")

        return handler

    handler = build_transport()
    cfg = KnowledgeCatalogConfig(project_id="p")

    async def async_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        return handler(method, url, headers, body, timeout_ms)

    bundle_small = OkfBundle(documents=(unchanged_doc, changed_doc, new_doc))
    client_a = AsyncKnowledgeCatalogClient(cfg, token_cache=_AsyncFakeTokenCache(), transport=async_transport)
    client_s = SyncKnowledgeCatalogClient(cfg, token_cache=_SyncFakeTokenCache(), transport=handler)
    if use_async:
        summary_small = await client_a.sync_bundle(bundle_small, delete_stale=True)
    else:
        summary_small = client_s.sync_bundle(bundle_small, delete_stale=True)

    assert isinstance(summary_small, CatalogSyncSummary)
    assert summary_small.skipped == 1
    assert summary_small.skipped_entry_ids == (unchanged_id,)
    assert summary_small.created == 1
    assert summary_small.updated == 1
    assert summary_small.deleted == 1
    assert summary_small.deleted_entry_ids == (stale_id,)
    assert summary_small.batch_count == 1
    assert summary_small.batch_sizes == (3,)
    assert stale_id not in remote_entries

    patch_calls = [item for item in mutations if item[0] == "PATCH"]
    assert len(patch_calls) == 1
    assert patch_calls[0][1] == changed_id
    patch_payload = patch_calls[0][2] or {}
    assert patch_payload["entrySource"]["labels"]["okf-sha256"] == changed_doc.sha256
    assert patch_payload["aspects"]["p.global.okf-document"]["data"]["sha256"] == changed_doc.sha256

    remote_entries.clear()
    mutations.clear()
    bulk_docs = tuple(_make_okf_doc(f"knowledge/doc-{idx:03d}.md", f"Doc {idx}") for idx in range(260))
    bulk_bundle = OkfBundle(documents=bulk_docs)
    if use_async:
        summary_bulk = await client_a.sync_bundle(bulk_bundle)
    else:
        summary_bulk = client_s.sync_bundle(bulk_bundle)

    assert summary_bulk.total_documents == 260
    assert summary_bulk.created == 260
    assert summary_bulk.batch_count == 2
    assert summary_bulk.batch_sizes == (250, 10)
    assert len(mutations) == 260


@pytest.mark.anyio
@pytest.mark.parametrize("use_async", [False, True])
async def test_search_entries_routes_to_global_endpoint_with_context_set_scope(use_async: bool) -> None:
    """Verify search_entries posts to locations/global:searchEntries even with regional config."""
    requests: list[tuple[str, str, dict[str, Any]]] = []

    def handle(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        _ = (headers, timeout_ms)
        payload = decode_json(body) if body else {}
        requests.append((method, url, payload))
        return (
            200,
            {},
            encode_json_bytes(
                {
                    "results": [
                        {
                            "dataplexEntry": {
                                "name": "projects/p/locations/us-central1/entryGroups/okf-context/entries/doc-1",
                                "entrySource": {"displayName": "Doc 1", "labels": {"okf-sha256": "c" * 64}},
                            }
                        }
                    ]
                }
            ),
        )

    cfg = KnowledgeCatalogConfig(project_id="p", location="europe-west1")

    async def async_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        return handle(method, url, headers, body, timeout_ms)

    if use_async:
        client_a = AsyncKnowledgeCatalogClient(cfg, token_cache=_AsyncFakeTokenCache(), transport=async_transport)
        unscoped = await client_a.search_entries("postgres pooling", page_size=15, semantic_search=False)
        scoped = await client_a.search_entries(
            "postgres pooling",
            entry_group_id="@dataplex",
            page_token="tok-next",
        )
    else:
        client_s = SyncKnowledgeCatalogClient(cfg, token_cache=_SyncFakeTokenCache(), transport=handle)
        unscoped = client_s.search_entries("postgres pooling", page_size=15, semantic_search=False)
        scoped = client_s.search_entries(
            "postgres pooling",
            entry_group_id="@dataplex",
            page_token="tok-next",
        )

    assert len(unscoped) == 1
    assert unscoped[0].sha256 == "c" * 64
    assert len(scoped) == 1

    assert requests[0][0] == "POST"
    assert requests[0][1] == "https://dataplex.googleapis.com/v1/projects/p/locations/global:searchEntries"
    assert requests[0][2] == {
        "query": "postgres pooling",
        "scope": "projects/p",
        "pageSize": 15,
        "semanticSearch": False,
    }

    assert requests[1][1] == "https://dataplex.googleapis.com/v1/projects/p/locations/global:searchEntries"
    assert requests[1][2] == {
        "query": "(postgres pooling) parent=projects/p/locations/europe-west1/entryGroups/@dataplex",
        "scope": "projects/p",
        "pageSize": 20,
        "semanticSearch": True,
        "pageToken": "tok-next",
    }


_ENTRY_NAME_PREFIX = "projects/p/locations/us-central1/entryGroups/okf-context/entries"
_OKF_LABELS = {"okf-sha256": "f" * 64, "okf-format": "v0-2"}


@pytest.mark.anyio
@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("entry_source", "expect_deleted"),
    [
        ({"system": "okf", "labels": _OKF_LABELS}, True),
        (None, False),
        ({"system": "bigquery", "labels": _OKF_LABELS}, False),
        ({"system": "okf"}, False),
        ({"system": "okf", "labels": {"okf-sha256": "f" * 64}}, False),
        ({"system": "okf", "labels": {"okf-format": "v0-2"}}, False),
        ({"labels": _OKF_LABELS}, False),
    ],
    ids=["okf-managed", "no-source", "other-system", "no-labels", "no-format", "no-digest", "no-system"],
)
async def test_sync_bundle_delete_stale_only_deletes_entries_it_wrote(
    use_async: bool,
    entry_source: dict[str, Any] | None,
    expect_deleted: bool,
) -> None:
    """Verify delete_stale removes only OKF-managed entries and never counts foreign entries as skipped."""
    remote_id = "third-party-table-0001"
    remote: dict[str, Any] = {"name": f"{_ENTRY_NAME_PREFIX}/{remote_id}"}
    if entry_source is not None:
        remote["entrySource"] = entry_source
    deleted: list[str] = []

    def handler(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        _ = (headers, body, timeout_ms)
        path = urlparse(url).path
        if method == "GET":
            return (200, {}, encode_json_bytes({"entries": [remote]}))
        if method == "DELETE":
            deleted.append(path.rsplit("/", 1)[-1])
            return (200, {}, b"{}")
        return (200, {}, encode_json_bytes({"name": f"{_ENTRY_NAME_PREFIX}/created"}))

    async def async_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        return handler(method, url, headers, body, timeout_ms)

    cfg = KnowledgeCatalogConfig(project_id="p")
    bundle = OkfBundle(documents=(_make_okf_doc("knowledge/kept.md", "Kept"),))
    if use_async:
        client_a = AsyncKnowledgeCatalogClient(cfg, token_cache=_AsyncFakeTokenCache(), transport=async_transport)
        summary = await client_a.sync_bundle(bundle, delete_stale=True)
    else:
        client_s = SyncKnowledgeCatalogClient(cfg, token_cache=_SyncFakeTokenCache(), transport=handler)
        summary = client_s.sync_bundle(bundle, delete_stale=True)

    assert CatalogEntryRecord.from_dataplex_dict(remote).is_okf_managed is expect_deleted
    assert deleted == ([remote_id] if expect_deleted else [])
    assert summary.deleted_entry_ids == ((remote_id,) if expect_deleted else ())
    assert summary.skipped_entry_ids == ()
    assert summary.created == 1


@pytest.mark.anyio
@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
async def test_search_entries_page_returns_next_page_token_until_last_page(use_async: bool) -> None:
    """Verify search_entries_page exposes nextPageToken so results after the first page are reachable."""
    seen_tokens: list[str | None] = []
    pages: dict[str | None, dict[str, Any]] = {
        None: {"results": [{"dataplexEntry": {"name": f"{_ENTRY_NAME_PREFIX}/doc-1"}}], "nextPageToken": "tok-2"},
        "tok-2": {"results": [{"dataplexEntry": {"name": f"{_ENTRY_NAME_PREFIX}/doc-2"}}], "nextPageToken": ""},
    }

    def handler(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        _ = (method, url, headers, timeout_ms)
        token = (decode_json(body) if body else {}).get("pageToken")
        seen_tokens.append(token)
        return (200, {}, encode_json_bytes(pages[token]))

    async def async_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        return handler(method, url, headers, body, timeout_ms)

    cfg = KnowledgeCatalogConfig(project_id="p")
    if use_async:
        client_a = AsyncKnowledgeCatalogClient(cfg, token_cache=_AsyncFakeTokenCache(), transport=async_transport)
        first, first_token = await client_a.search_entries_page("pooling")
        second, second_token = await client_a.search_entries_page("pooling", page_token=first_token)
        single = await client_a.search_entries("pooling")
    else:
        client_s = SyncKnowledgeCatalogClient(cfg, token_cache=_SyncFakeTokenCache(), transport=handler)
        first, first_token = client_s.search_entries_page("pooling")
        second, second_token = client_s.search_entries_page("pooling", page_token=first_token)
        single = client_s.search_entries("pooling")

    assert [record.entry_id for record in first] == ["doc-1"]
    assert first_token == "tok-2"
    assert [record.entry_id for record in second] == ["doc-2"]
    assert second_token is None
    assert [record.entry_id for record in single] == ["doc-1"]
    assert seen_tokens == [None, "tok-2", None]


def test_sync_bundle_sync_failure_raises_catalog_sync_error_with_completed_summary() -> None:
    """Verify the sync client chains the original error and reports the entries written before it."""
    attempted: list[str] = []

    def handler(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        _ = (headers, body, timeout_ms)
        parsed = urlparse(url)
        if method == "GET":
            return (200, {}, encode_json_bytes({"entries": []}))
        entry_id = parse_qs(parsed.query)["entryId"][0]
        attempted.append(entry_id)
        if "fail" in entry_id:
            return (500, {}, encode_json_bytes({"error": {"status": "INTERNAL", "message": "backend exploded"}}))
        return (200, {}, encode_json_bytes({"name": f"{_ENTRY_NAME_PREFIX}/{entry_id}"}))

    bundle = OkfBundle(
        documents=(
            _make_okf_doc("knowledge/ok.md", "Ok"),
            _make_okf_doc("knowledge/fail.md", "Fail"),
            _make_okf_doc("knowledge/never.md", "Never"),
        )
    )
    client = SyncKnowledgeCatalogClient(
        KnowledgeCatalogConfig(project_id="p"), token_cache=_SyncFakeTokenCache(), transport=handler
    )

    with pytest.raises(CatalogSyncError) as excinfo:
        client.sync_bundle(bundle)

    err = excinfo.value
    assert isinstance(err.__cause__, GoogleCloudError)
    assert err.errors == (err.__cause__,)
    assert "backend exploded" in str(err)
    assert len(attempted) == 2
    assert err.summary.created_entry_ids == (attempted[0],)
    assert err.summary.created == 1
    assert err.summary.total_documents == 3
    assert err.summary.batch_sizes == (3,)


@pytest.mark.anyio
async def test_sync_bundle_async_failure_cancels_in_flight_siblings_and_keeps_summary() -> None:
    """Verify the async client cancels and awaits in-flight batch mutations and reports completed entries."""
    never_released = asyncio.Event()
    hang_started = asyncio.Event()
    outcomes: dict[str, str] = {}

    async def transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout_ms: int,
    ) -> tuple[int, dict[str, str], bytes]:
        _ = (headers, body, timeout_ms)
        parsed = urlparse(url)
        if method == "GET":
            return (200, {}, encode_json_bytes({"entries": []}))
        entry_id = parse_qs(parsed.query)["entryId"][0]
        if "hang" in entry_id:
            hang_started.set()
            try:
                await never_released.wait()
            except asyncio.CancelledError:
                outcomes[entry_id] = "cancelled"
                raise
            outcomes[entry_id] = "completed"
        if "fail" in entry_id:
            await hang_started.wait()
            outcomes[entry_id] = "failed"
            return (500, {}, encode_json_bytes({"error": {"status": "INTERNAL", "message": "backend exploded"}}))
        outcomes[entry_id] = "completed"
        return (200, {}, encode_json_bytes({"name": f"{_ENTRY_NAME_PREFIX}/{entry_id}"}))

    bundle = OkfBundle(
        documents=(
            _make_okf_doc("knowledge/ok.md", "Ok"),
            _make_okf_doc("knowledge/hang.md", "Hang"),
            _make_okf_doc("knowledge/fail.md", "Fail"),
        )
    )
    client = AsyncKnowledgeCatalogClient(
        KnowledgeCatalogConfig(project_id="p"), token_cache=_AsyncFakeTokenCache(), transport=transport
    )

    with pytest.raises(CatalogSyncError) as excinfo:
        await asyncio.wait_for(client.sync_bundle(bundle), timeout=5.0)

    err = excinfo.value
    by_outcome = {outcome: entry_id for entry_id, outcome in outcomes.items()}
    assert sorted(outcomes.values()) == ["cancelled", "completed", "failed"]
    assert isinstance(err.__cause__, GoogleCloudError)
    assert err.errors == (err.__cause__,)
    assert err.summary.created_entry_ids == (by_outcome["completed"],)
    assert err.summary.updated_entry_ids == ()
    assert err.summary.batch_sizes == (3,)
