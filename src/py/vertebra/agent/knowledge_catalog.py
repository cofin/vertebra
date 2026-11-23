"""Google Cloud Dataplex v1 Knowledge Catalog clients with progressive OKF sync.

Provides synchronous and asynchronous Dataplex Universal Catalog clients over
Vertebra's native `GcpTokenCache` and HTTP/2 transport (`gcp_http2_request_*`),
supporting `entryGroups` management with LRO polling, label-matched context-set
resolution, `view="ALL"` entry CRUD with `aspectKeys` updates, digest-aware
chunked `OkfBundle` synchronization, and `locations/global:searchEntries`.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Final, cast
from urllib.parse import quote, urlencode

import msgspec

from vertebra._vertebra import (
    AsyncGcpTokenCache,
    GcpTokenSnapshot,
    GoogleCloudError,
    SyncGcpTokenCache,
    gcp_http2_request_async,
    gcp_http2_request_sync,
)
from vertebra.agent.okf import OkfBundle, OkfDocument
from vertebra.embeddings._google import GcpTokenCachePort as GcpTokenCachePort
from vertebra.typing import ImproperConfigurationError, VertebraError
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "DEFAULT_DATAPLEX_ENDPOINT",
    "DEFAULT_DATAPLEX_SCOPES",
    "DEFAULT_ENTRY_GROUP_ID",
    "DEFAULT_ENTRY_TYPE",
    "MAX_BATCH_SYNC_ENTRIES",
    "AsyncHttpTransport",
    "AsyncKnowledgeCatalogClient",
    "CatalogEntryRecord",
    "CatalogSyncError",
    "CatalogSyncSummary",
    "GcpTokenCachePort",
    "KnowledgeCatalogConfig",
    "SyncHttpTransport",
    "SyncKnowledgeCatalogClient",
)

MAX_BATCH_SYNC_ENTRIES: Final[int] = 250
DEFAULT_DATAPLEX_ENDPOINT: Final[str] = "https://dataplex.googleapis.com"
DEFAULT_ENTRY_GROUP_ID: Final[str] = "okf-context"
DEFAULT_ENTRY_TYPE: Final[str] = "projects/dataplex-types/locations/global/entryTypes/generic"
DEFAULT_OVERVIEW_ASPECT_KEY: Final[str] = "dataplex-types.global.overview"
DEFAULT_OVERVIEW_ASPECT_TYPE: Final[str] = "projects/dataplex-types/locations/global/aspectTypes/overview"
DEFAULT_DATAPLEX_SCOPES: Final[tuple[str, ...]] = ("https://www.googleapis.com/auth/cloud-platform",)
OKF_ENTRY_SOURCE_SYSTEM: Final[str] = "okf"
OKF_FORMAT_LABEL: Final[str] = "okf-format"
OKF_SHA256_LABEL: Final[str] = "okf-sha256"

_RFC1035_ENTRY_GROUP_ID_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")
_SYSTEM_ENTRY_GROUP_ID_RE: Final[re.Pattern[str]] = re.compile(r"^@[a-z0-9_-]+$")
_NON_SLUG_RE: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9]+")

_GRPC_CODE_BY_NAME: Final[dict[str, int]] = {
    "OK": 0,
    "CANCELLED": 1,
    "UNKNOWN": 2,
    "INVALID_ARGUMENT": 3,
    "DEADLINE_EXCEEDED": 4,
    "NOT_FOUND": 5,
    "ALREADY_EXISTS": 6,
    "PERMISSION_DENIED": 7,
    "RESOURCE_EXHAUSTED": 8,
    "FAILED_PRECONDITION": 9,
    "ABORTED": 10,
    "OUT_OF_RANGE": 11,
    "UNIMPLEMENTED": 12,
    "INTERNAL": 13,
    "UNAVAILABLE": 14,
    "DATA_LOSS": 15,
    "UNAUTHENTICATED": 16,
}
_GRPC_NAME_BY_CODE: Final[dict[int, str]] = {code: name for name, code in _GRPC_CODE_BY_NAME.items()}
_GRPC_BY_HTTP_STATUS: Final[dict[int, tuple[int, str]]] = {
    400: (3, "INVALID_ARGUMENT"),
    401: (16, "UNAUTHENTICATED"),
    403: (7, "PERMISSION_DENIED"),
    404: (5, "NOT_FOUND"),
    409: (6, "ALREADY_EXISTS"),
    429: (8, "RESOURCE_EXHAUSTED"),
    499: (1, "CANCELLED"),
    500: (13, "INTERNAL"),
    501: (12, "UNIMPLEMENTED"),
    503: (14, "UNAVAILABLE"),
    504: (4, "DEADLINE_EXCEEDED"),
}


SyncHttpTransport = Callable[
    [str, str, dict[str, str], bytes | None, int],
    tuple[int, dict[str, str], bytes],
]
AsyncHttpTransport = Callable[
    [str, str, dict[str, str], bytes | None, int],
    Awaitable[tuple[int, dict[str, str], bytes]],
]


class KnowledgeCatalogConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for synchronous and asynchronous Dataplex Knowledge Catalog clients."""

    project_id: str
    location: str = "us-central1"
    entry_group_id: str = DEFAULT_ENTRY_GROUP_ID
    endpoint: str = DEFAULT_DATAPLEX_ENDPOINT
    quota_project_id: str | None = None
    timeout_ms: int = 30_000

    def __post_init__(self) -> None:
        if not self.project_id.strip():
            raise ImproperConfigurationError("KnowledgeCatalogConfig project_id must not be empty")
        if not self.location.strip():
            raise ImproperConfigurationError("KnowledgeCatalogConfig location must not be empty")
        if not self.endpoint.strip():
            raise ImproperConfigurationError("KnowledgeCatalogConfig endpoint must not be empty")
        if self.timeout_ms <= 0:
            raise ImproperConfigurationError("KnowledgeCatalogConfig timeout_ms must be > 0")


class CatalogEntryRecord(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Normalized Dataplex v1 catalog entry record with optional OKF digest metadata."""

    name: str
    entry_id: str = ""
    entry_type: str = ""
    fully_qualified_name: str = ""
    parent_entry: str = ""
    display_name: str = ""
    description: str = ""
    source_system: str = ""
    sha256: str = ""
    labels: dict[str, str] = msgspec.field(default_factory=dict)
    aspects: dict[str, Any] = msgspec.field(default_factory=dict)
    entry_source: dict[str, Any] = msgspec.field(default_factory=dict)
    create_time: str | None = None
    update_time: str | None = None
    raw: dict[str, Any] = msgspec.field(default_factory=dict)

    @property
    def is_okf_managed(self) -> bool:
        """Return whether `sync_bundle` wrote this entry.

        An entry is OKF-managed when `entrySource.system` is `okf` and its `entrySource.labels`
        carry both `okf-format` and `okf-sha256`. Only OKF-managed entries are eligible for
        `sync_bundle(delete_stale=True)` deletion.
        """
        return (
            self.source_system == OKF_ENTRY_SOURCE_SYSTEM
            and OKF_FORMAT_LABEL in self.labels
            and OKF_SHA256_LABEL in self.labels
        )

    @classmethod
    def from_dataplex_dict(cls, payload: Mapping[str, Any]) -> CatalogEntryRecord:
        """Construct a normalized `CatalogEntryRecord` from a Dataplex v1 entry dictionary."""
        name = str(payload.get("name", ""))
        entry_id = name.rsplit("/", 1)[-1] if name else ""
        raw_source = payload.get("entrySource")
        entry_source = dict(raw_source) if isinstance(raw_source, Mapping) else {}
        raw_labels = entry_source.get("labels")
        labels = {str(k): str(v) for k, v in raw_labels.items()} if isinstance(raw_labels, Mapping) else {}
        raw_aspects = payload.get("aspects")
        aspects = dict(raw_aspects) if isinstance(raw_aspects, Mapping) else {}

        sha256 = labels.get(OKF_SHA256_LABEL, "")
        if not sha256:
            for aspect_val in aspects.values():
                if not isinstance(aspect_val, Mapping):
                    continue
                aspect_data = aspect_val.get("data")
                if not isinstance(aspect_data, Mapping):
                    continue
                candidate = aspect_data.get("sha256") or aspect_data.get("okf-sha256")
                if isinstance(candidate, str) and candidate:
                    sha256 = candidate
                    break

        create_time_raw = payload.get("createTime")
        update_time_raw = payload.get("updateTime")
        return cls(
            name=name,
            entry_id=entry_id,
            entry_type=str(payload.get("entryType", "") or ""),
            fully_qualified_name=str(payload.get("fullyQualifiedName", "") or ""),
            parent_entry=str(payload.get("parentEntry", "") or ""),
            display_name=str(entry_source.get("displayName", "") or ""),
            description=str(entry_source.get("description", "") or ""),
            source_system=str(entry_source.get("system", "") or ""),
            sha256=sha256,
            labels=labels,
            aspects=aspects,
            entry_source=entry_source,
            create_time=str(create_time_raw) if isinstance(create_time_raw, str) else None,
            update_time=str(update_time_raw) if isinstance(update_time_raw, str) else None,
            raw=dict(payload),
        )


class CatalogSyncSummary(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Summary of a progressive digest-aware OKF bundle synchronization run."""

    entry_group_id: str
    bundle_digest: str = ""
    total_documents: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    deleted: int = 0
    batch_count: int = 0
    batch_sizes: tuple[int, ...] = ()
    created_entry_ids: tuple[str, ...] = ()
    updated_entry_ids: tuple[str, ...] = ()
    skipped_entry_ids: tuple[str, ...] = ()
    deleted_entry_ids: tuple[str, ...] = ()


class CatalogSyncError(VertebraError):
    """Raised when a `sync_bundle` mutation fails after zero or more mutations completed.

    The first failure is chained as `__cause__`. `summary` describes the mutations that completed
    before the run stopped, so callers can tell which entry IDs were already written or deleted.

    Attributes:
        summary: `CatalogSyncSummary` covering only the mutations that completed.
        errors: Every mutation failure observed in the failing batch, in batch order.
    """

    def __init__(self, summary: CatalogSyncSummary, errors: Sequence[BaseException]) -> None:
        self.summary = summary
        self.errors: tuple[BaseException, ...] = tuple(errors)
        completed = summary.created + summary.updated + summary.deleted
        detail = f": {self.errors[0]}" if self.errors else ""
        super().__init__(
            f"Knowledge Catalog sync of entry group {summary.entry_group_id!r} failed after "
            f"{completed} completed mutation(s){detail}"
        )


def _validate_create_entry_group_id(entry_group_id: str) -> str:
    """Validate an entry group ID for creation against Dataplex RFC-1035 constraints."""
    if not _RFC1035_ENTRY_GROUP_ID_RE.fullmatch(entry_group_id):
        raise ImproperConfigurationError(
            f"Invalid entry_group_id {entry_group_id!r}; expected RFC-1035 identifier "
            "matching ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$"
        )
    return entry_group_id


def _validate_lookup_entry_group_id(entry_group_id: str) -> str:
    """Validate an entry group ID for read or lookup, permitting `@*` system entryGroups."""
    if _RFC1035_ENTRY_GROUP_ID_RE.fullmatch(entry_group_id) or _SYSTEM_ENTRY_GROUP_ID_RE.fullmatch(entry_group_id):
        return entry_group_id
    raise ImproperConfigurationError(
        f"Invalid entry_group_id {entry_group_id!r}; expected RFC-1035 identifier or @system entryGroup"
    )


def _parse_retry_delay_ms(raw_delay: object) -> int | None:
    """Parse a protobuf duration string such as `2s` or `500ms` into milliseconds."""
    if not isinstance(raw_delay, str):
        return None
    text = raw_delay.strip().lower()
    if text.endswith("ms"):
        try:
            return max(0, int(float(text[:-2])))
        except ValueError:
            return None
    if text.endswith("s"):
        try:
            return max(0, int(float(text[:-1]) * 1000))
        except ValueError:
            return None
    return None


def _build_google_cloud_error(
    *,
    message: str,
    code: int,
    code_name: str,
    http_status: int | None,
    retry_delay_ms: int | None = None,
    reason: str | None = None,
) -> GoogleCloudError:
    """Instantiate `GoogleCloudError` populated with structured gRPC and HTTP metadata."""
    err = GoogleCloudError(message)
    err.code = code
    err.code_name = code_name
    err.message = message
    err.http_status = http_status
    err.retry_delay_ms = retry_delay_ms
    err.reason = reason
    return err


def _error_from_rpc_status(
    error_obj: Mapping[str, Any],
    *,
    http_status: int | None,
    fallback_message: str,
) -> GoogleCloudError:
    """Convert a Google RPC `Status` or REST `error` dictionary into `GoogleCloudError`."""
    raw_status = error_obj.get("status")
    raw_code = error_obj.get("code")
    if isinstance(raw_status, str) and raw_status in _GRPC_CODE_BY_NAME:
        code_name = raw_status
        code = _GRPC_CODE_BY_NAME[raw_status]
    elif http_status is None and isinstance(raw_code, int) and raw_code in _GRPC_NAME_BY_CODE:
        code = raw_code
        code_name = _GRPC_NAME_BY_CODE[raw_code]
    elif http_status is not None and http_status in _GRPC_BY_HTTP_STATUS:
        code, code_name = _GRPC_BY_HTTP_STATUS[http_status]
    else:
        code = 2
        code_name = "UNKNOWN"

    raw_message = error_obj.get("message")
    message = str(raw_message) if isinstance(raw_message, str) and raw_message.strip() else fallback_message

    reason: str | None = None
    retry_delay_ms: int | None = None
    details = error_obj.get("details")
    if isinstance(details, list):
        for detail in details:
            if not isinstance(detail, Mapping):
                continue
            detail_type = str(detail.get("@type", ""))
            if detail_type.endswith("google.rpc.ErrorInfo") and reason is None:
                raw_reason = detail.get("reason")
                if isinstance(raw_reason, str) and raw_reason:
                    reason = raw_reason
            elif detail_type.endswith("google.rpc.RetryInfo") and retry_delay_ms is None:
                retry_delay_ms = _parse_retry_delay_ms(detail.get("retryDelay"))

    return _build_google_cloud_error(
        message=message,
        code=code,
        code_name=code_name,
        http_status=http_status,
        retry_delay_ms=retry_delay_ms,
        reason=reason,
    )


def _error_from_http_response(status: int, body: bytes) -> GoogleCloudError:
    """Parse a non-2xx Dataplex HTTP response body into a structured `GoogleCloudError`."""
    snippet = body[:512].decode("utf-8", errors="replace").strip() if body else ""
    fallback = (
        f"Dataplex API request failed with HTTP {status}: {snippet}"
        if snippet
        else f"Dataplex API request failed with HTTP {status}"
    )
    if body:
        try:
            decoded = decode_json(body)
        except Exception:
            decoded = None
        if isinstance(decoded, Mapping):
            err_obj = decoded.get("error")
            if isinstance(err_obj, Mapping):
                return _error_from_rpc_status(err_obj, http_status=status, fallback_message=fallback)
            if "message" in decoded or "status" in decoded:
                return _error_from_rpc_status(decoded, http_status=status, fallback_message=fallback)
    code, code_name = _GRPC_BY_HTTP_STATUS.get(status, (2, "UNKNOWN"))
    return _build_google_cloud_error(
        message=fallback,
        code=code,
        code_name=code_name,
        http_status=status,
    )


def _decode_json_dict(body: bytes) -> dict[str, Any]:
    """Decode a JSON response body into a dictionary, returning `{}` for empty bodies."""
    if not body:
        return {}
    decoded = decode_json(body)
    if not isinstance(decoded, dict):
        raise ValueError(f"Expected JSON object from Dataplex API, got {type(decoded).__name__}")
    return decoded


def _parse_search_results(data: Mapping[str, Any]) -> list[CatalogEntryRecord]:
    """Extract normalized `CatalogEntryRecord` entries from a Dataplex `searchEntries` payload."""
    raw_results = data.get("results")
    if not isinstance(raw_results, list):
        return []
    records: list[CatalogEntryRecord] = []
    for item in raw_results:
        if not isinstance(item, Mapping):
            continue
        entry_payload = item.get("dataplexEntry")
        if isinstance(entry_payload, Mapping):
            records.append(CatalogEntryRecord.from_dataplex_dict(entry_payload))
    return records


def _parse_next_page_token(data: Mapping[str, Any]) -> str | None:
    """Return the non-empty `nextPageToken` of a paged Dataplex response, or `None` on the last page."""
    token = data.get("nextPageToken")
    return token if isinstance(token, str) and token else None


def _matches_labels(group: Mapping[str, Any], labels: Mapping[str, str] | None) -> bool:
    """Check client-side whether an entryGroup dictionary matches every required label."""
    if not labels:
        return True
    group_labels = group.get("labels")
    if not isinstance(group_labels, Mapping):
        return False
    return all(group_labels.get(k) == v for k, v in labels.items())


class _BaseKnowledgeCatalogClient:
    """Shared URL, header, payload, and OKF sync-planning logic for Knowledge Catalog clients."""

    def __init__(
        self,
        config: KnowledgeCatalogConfig,
        *,
        target_service_account: str | None = None,
        poll_interval_ms: int = 250,
    ) -> None:
        self._config = config
        base = config.endpoint.rstrip("/")
        self._api_base = base if base.endswith("/v1") else f"{base}/v1"
        self._target_service_account = target_service_account
        self._poll_interval_ms = max(0, poll_interval_ms)

    @property
    def config(self) -> KnowledgeCatalogConfig:
        """Return the immutable `KnowledgeCatalogConfig`."""
        return self._config

    def _token_cache_options(self) -> dict[str, str]:
        """Build native `GcpTokenCache` constructor options from configuration."""
        opts: dict[str, str] = {}
        if self._target_service_account:
            opts["target_service_account"] = self._target_service_account
        if self._config.quota_project_id:
            opts["quota_project_id"] = self._config.quota_project_id
        return opts

    def _build_headers(
        self,
        snap: GcpTokenSnapshot,
        *,
        has_body: bool,
        extra_headers: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        """Build authorized Dataplex HTTP/2 headers with optional `x-goog-user-project`."""
        headers: dict[str, str] = {
            "authorization": f"Bearer {snap.access_token}",
            "accept": "application/json",
        }
        if has_body:
            headers["content-type"] = "application/json"
        effective_quota = self._config.quota_project_id or snap.quota_project_id
        if effective_quota:
            headers["x-goog-user-project"] = effective_quota
        if extra_headers:
            headers.update(extra_headers)
        return headers

    def _parent_path(self, location: str | None = None) -> str:
        """Build the `projects/{project}/locations/{location}` parent resource path."""
        loc = location or self._config.location
        return f"projects/{self._config.project_id}/locations/{loc}"

    def _entry_group_path(
        self,
        entry_group_id: str | None = None,
        *,
        location: str | None = None,
        for_create: bool = False,
    ) -> str:
        """Build and validate a Dataplex `entryGroups/{id}` resource path."""
        eg_id = entry_group_id if entry_group_id is not None else self._config.entry_group_id
        validated = _validate_create_entry_group_id(eg_id) if for_create else _validate_lookup_entry_group_id(eg_id)
        return f"{self._parent_path(location)}/entryGroups/{validated}"

    def _entry_path(
        self,
        entry_id: str,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
    ) -> str:
        """Build a Dataplex `entries/{entry_id}` resource path."""
        quoted_id = quote(entry_id, safe="-_.~")
        return f"{self._entry_group_path(entry_group_id, location=location)}/entries/{quoted_id}"

    def _url(self, resource_path: str, query: Sequence[tuple[str, str]] | None = None) -> str:
        """Build an absolute Dataplex v1 REST URL with optional multi-value query parameters."""
        normalized = resource_path.lstrip("/")
        if normalized.startswith("v1/"):
            normalized = normalized[3:]
        url = f"{self._api_base}/{normalized}"
        if query:
            url = f"{url}?{urlencode(list(query), doseq=True)}"
        return url

    def _operation_url(self, operation_name: str) -> str:
        """Build the LRO polling URL for a Dataplex operation resource name."""
        return self._url(operation_name)

    def _build_entry_group_payload(
        self,
        *,
        display_name: str | None = None,
        description: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Build a Dataplex `EntryGroup` JSON request payload."""
        payload: dict[str, Any] = {}
        if display_name is not None:
            payload["displayName"] = display_name
        if description is not None:
            payload["description"] = description
        if labels is not None:
            payload["labels"] = {str(k): str(v) for k, v in labels.items()}
        return payload

    def _build_entry_payload(
        self,
        *,
        entry_type: str | None = None,
        fully_qualified_name: str | None = None,
        parent_entry: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
        labels: Mapping[str, str] | None = None,
        aspects: Mapping[str, Any] | None = None,
        entry_source: Mapping[str, Any] | None = None,
        include_default_entry_type: bool = False,
    ) -> dict[str, Any]:
        """Build a Dataplex `Entry` JSON request payload."""
        payload: dict[str, Any] = {}
        if entry_type is not None:
            payload["entryType"] = entry_type
        elif include_default_entry_type:
            payload["entryType"] = DEFAULT_ENTRY_TYPE
        if fully_qualified_name is not None:
            payload["fullyQualifiedName"] = fully_qualified_name
        if parent_entry is not None:
            payload["parentEntry"] = parent_entry

        merged_source: dict[str, Any] = dict(entry_source) if entry_source is not None else {}
        if display_name is not None:
            merged_source["displayName"] = display_name
        if description is not None:
            merged_source["description"] = description
        if labels is not None:
            existing_labels = merged_source.get("labels")
            combined_labels = dict(existing_labels) if isinstance(existing_labels, Mapping) else {}
            combined_labels.update({str(k): str(v) for k, v in labels.items()})
            merged_source["labels"] = combined_labels
        if merged_source:
            payload["entrySource"] = merged_source
        if aspects is not None:
            payload["aspects"] = dict(aspects)
        return payload

    def _build_update_entry_query(
        self,
        *,
        update_mask: str | Sequence[str] = ("aspects", "entrySource"),
        aspect_keys: Sequence[str] | None = None,
        aspects: Mapping[str, Any] | None = None,
        allow_missing: bool = False,
        delete_missing_aspects: bool = False,
    ) -> list[tuple[str, str]]:
        """Build `PATCH` query parameters including `updateMask` and repeated `aspectKeys`."""
        mask_value = update_mask if isinstance(update_mask, str) else ",".join(update_mask)
        query: list[tuple[str, str]] = [("updateMask", mask_value)]
        resolved_keys = tuple(aspect_keys) if aspect_keys is not None else (tuple(aspects.keys()) if aspects else ())
        for key in resolved_keys:
            query.append(("aspectKeys", key))
        if allow_missing:
            query.append(("allowMissing", "true"))
        if delete_missing_aspects:
            query.append(("deleteMissingAspects", "true"))
        return query

    def _build_search_payload(
        self,
        query: str,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        page_size: int = 20,
        page_token: str | None = None,
        semantic_search: bool = True,
    ) -> tuple[str, dict[str, Any]]:
        """Build the `locations/global:searchEntries` URL and JSON request body."""
        if entry_group_id is not None:
            eg_path = self._entry_group_path(entry_group_id, location=location)
            stripped = query.strip()
            effective_query = f"({stripped}) parent={eg_path}" if stripped else f"parent={eg_path}"
        else:
            effective_query = query

        payload: dict[str, Any] = {
            "query": effective_query,
            "scope": f"projects/{self._config.project_id}",
            "pageSize": page_size,
            "semanticSearch": semantic_search,
        }
        if page_token is not None:
            payload["pageToken"] = page_token
        url = self._url(f"projects/{self._config.project_id}/locations/global:searchEntries")
        return (url, payload)

    def _build_okf_entry_fields(
        self,
        doc: OkfDocument,
        *,
        entry_type: str | None = None,
        aspect_type: str | None = None,
    ) -> dict[str, Any]:
        """Build Dataplex entry fields and aspects carrying an OKF document and its SHA-256 digest."""
        title = str(doc.frontmatter.get("title") or doc.rel_path)
        description = str(doc.frontmatter.get("description") or "")
        overview_content = "\n".join(doc.truth_lines) if doc.truth_lines else doc.body[:4096]
        okf_aspect_key = f"{self._config.project_id}.global.okf-document"
        resolved_aspect_type = (
            aspect_type or f"projects/{self._config.project_id}/locations/global/aspectTypes/okf-document"
        )
        aspects: dict[str, Any] = {
            DEFAULT_OVERVIEW_ASPECT_KEY: {
                "aspectType": DEFAULT_OVERVIEW_ASPECT_TYPE,
                "data": {"content": overview_content},
            },
            okf_aspect_key: {
                "aspectType": resolved_aspect_type,
                "data": {
                    "rel_path": doc.rel_path,
                    "sha256": doc.sha256,
                    "status": str(doc.frontmatter.get("status", "")),
                    "type": str(doc.frontmatter.get("type", "")),
                    "title": title,
                    "truth_lines": list(doc.truth_lines),
                    "markdown": doc.to_markdown(),
                },
            },
        }
        return {
            "entry_type": entry_type or DEFAULT_ENTRY_TYPE,
            "fully_qualified_name": f"okf:{doc.rel_path}",
            "display_name": title,
            "description": description,
            "labels": {
                OKF_SHA256_LABEL: doc.sha256,
                OKF_FORMAT_LABEL: "v0-2",
            },
            "entry_source": {
                "resource": doc.rel_path,
                "system": OKF_ENTRY_SOURCE_SYSTEM,
            },
            "aspects": aspects,
        }

    def _plan_sync_operations(
        self,
        bundle: OkfBundle,
        existing_entries: Sequence[CatalogEntryRecord],
        *,
        delete_stale: bool,
    ) -> tuple[
        list[tuple[str, str, OkfDocument | None]],
        list[str],
    ]:
        """Diff an `OkfBundle` against remote catalog entries by SHA-256 digest.

        With `delete_stale`, only remote entries whose `CatalogEntryRecord.is_okf_managed` is true and
        whose ID is absent from the bundle are planned for deletion. Entries written by other tools
        are neither deleted nor reported as skipped.
        """
        remote_by_id = {entry.entry_id: entry for entry in existing_entries if entry.entry_id}
        mutations: list[tuple[str, str, OkfDocument | None]] = []
        skipped_ids: list[str] = []
        desired_ids: set[str] = set()

        for doc in bundle.documents:
            entry_id = self._okf_rel_path_to_entry_id(doc.rel_path)
            desired_ids.add(entry_id)
            remote = remote_by_id.get(entry_id)
            if remote is None:
                mutations.append(("create", entry_id, doc))
            elif remote.sha256 == doc.sha256:
                skipped_ids.append(entry_id)
            else:
                mutations.append(("update", entry_id, doc))

        if delete_stale:
            for remote_id in sorted(remote_by_id):
                if remote_id not in desired_ids and remote_by_id[remote_id].is_okf_managed:
                    mutations.append(("delete", remote_id, None))

        return (mutations, skipped_ids)

    @staticmethod
    def _build_sync_summary(
        bundle: OkfBundle,
        *,
        entry_group_id: str,
        batches: Sequence[Sequence[tuple[str, str, OkfDocument | None]]],
        created_ids: Sequence[str],
        updated_ids: Sequence[str],
        skipped_ids: Sequence[str],
        deleted_ids: Sequence[str],
    ) -> CatalogSyncSummary:
        """Build a `CatalogSyncSummary` from the planned batches and the mutations that completed."""
        return CatalogSyncSummary(
            entry_group_id=entry_group_id,
            bundle_digest=bundle.digest(),
            total_documents=len(bundle.documents),
            created=len(created_ids),
            updated=len(updated_ids),
            skipped=len(skipped_ids),
            deleted=len(deleted_ids),
            batch_count=len(batches),
            batch_sizes=tuple(len(batch) for batch in batches),
            created_entry_ids=tuple(created_ids),
            updated_entry_ids=tuple(updated_ids),
            skipped_entry_ids=tuple(skipped_ids),
            deleted_entry_ids=tuple(deleted_ids),
        )

    @staticmethod
    def _okf_rel_path_to_entry_id(rel_path: str) -> str:
        """Convert an OKF relative path into a RFC-1035-safe Dataplex entry ID."""
        digest_suffix = hashlib.sha256(rel_path.encode("utf-8")).hexdigest()[:8]
        slug = _NON_SLUG_RE.sub("-", rel_path.lower()).strip("-")
        if not slug or not slug[0].isalpha():
            slug = f"okf-{slug}" if slug else "okf"
        max_prefix = 63 - 1 - len(digest_suffix)
        trimmed = slug[:max_prefix].rstrip("-")
        if not trimmed:
            trimmed = "okf"
        return f"{trimmed}-{digest_suffix}"

    @staticmethod
    def _extract_group_id(group: Mapping[str, Any]) -> str:
        """Extract the short `entryGroupId` from a Dataplex entryGroup resource dictionary."""
        name = str(group.get("name", ""))
        return name.rsplit("/", 1)[-1] if name else ""

    @staticmethod
    def _partition_mutation_batches(
        mutations: Sequence[tuple[str, str, OkfDocument | None]],
    ) -> list[tuple[tuple[str, str, OkfDocument | None], ...]]:
        """Chunk planned catalog mutations into batches of at most `MAX_BATCH_SYNC_ENTRIES`."""
        return [
            tuple(mutations[idx : idx + MAX_BATCH_SYNC_ENTRIES])
            for idx in range(0, len(mutations), MAX_BATCH_SYNC_ENTRIES)
        ]

    @classmethod
    def _plan_context_set_action(
        cls,
        groups: Sequence[dict[str, Any]],
        *,
        requested_id: str | None,
        default_id: str,
        required_labels: Mapping[str, str],
    ) -> tuple[str, dict[str, Any] | None, str]:
        """Decide whether to return, adopt (PATCH), or create a `type=context-set` entry group."""
        target_id = requested_id if requested_id is not None else default_id
        for group in groups:
            if cls._extract_group_id(group) == target_id:
                if _matches_labels(group, required_labels):
                    return ("return", group, target_id)
                return ("adopt", group, target_id)

        if requested_id is None:
            for group in groups:
                if _matches_labels(group, required_labels):
                    return ("return", group, cls._extract_group_id(group))

        return ("create", None, target_id)

    def _build_entry_groups_url(self, location: str | None, *, page_size: int | None, page_token: str | None) -> str:
        parent = self._parent_path(location)
        query: list[tuple[str, str]] = []
        if page_size is not None:
            query.append(("pageSize", str(page_size)))
        if page_token is not None:
            query.append(("pageToken", page_token))
        return self._url(f"{parent}/entryGroups", query)

    @staticmethod
    def _collect_entry_groups_page(
        data: Mapping[str, Any], labels: Mapping[str, str] | None, out: list[dict[str, Any]]
    ) -> str | None:
        raw_groups = data.get("entryGroups")
        if isinstance(raw_groups, list):
            out.extend(item for item in raw_groups if isinstance(item, dict) and _matches_labels(item, labels))
        next_token = data.get("nextPageToken")
        return next_token if isinstance(next_token, str) and next_token else None

    def _prepare_create_entry_group(
        self,
        entry_group_id: str | None,
        *,
        location: str | None,
        display_name: str | None,
        description: str | None,
        labels: Mapping[str, str] | None,
        validate_only: bool,
        timeout_ms: int | None,
    ) -> tuple[str, dict[str, Any], int]:
        eg_id = entry_group_id if entry_group_id is not None else self._config.entry_group_id
        validated = _validate_create_entry_group_id(eg_id)
        parent = self._parent_path(location)
        query: list[tuple[str, str]] = [("entryGroupId", validated)]
        if validate_only:
            query.append(("validateOnly", "true"))
        body = self._build_entry_group_payload(display_name=display_name, description=description, labels=labels)
        return (
            self._url(f"{parent}/entryGroups", query),
            body,
            (timeout_ms if timeout_ms is not None else self._config.timeout_ms),
        )

    def _prepare_list_entries_url(
        self,
        entry_group_id: str | None,
        *,
        location: str | None,
        filter_expr: str | None,
        page_size: int | None,
        page_token: str | None,
    ) -> str:
        eg_path = self._entry_group_path(entry_group_id, location=location, for_create=False)
        query: list[tuple[str, str]] = []
        if filter_expr:
            query.append(("filter", filter_expr))
        if page_size is not None:
            query.append(("pageSize", str(page_size)))
        if page_token is not None:
            query.append(("pageToken", page_token))
        return self._url(f"{eg_path}/entries", query)

    @staticmethod
    def _parse_entries_page(data: Mapping[str, Any]) -> tuple[list[CatalogEntryRecord], str | None]:
        raw_entries = data.get("entries")
        records = [
            CatalogEntryRecord.from_dataplex_dict(item)
            for item in (raw_entries if isinstance(raw_entries, list) else ())
            if isinstance(item, Mapping)
        ]
        return records, _parse_next_page_token(data)

    @staticmethod
    def _required_context_labels(entry_group_id: str | None, labels: Mapping[str, str] | None) -> dict[str, str]:
        if entry_group_id is not None:
            _validate_create_entry_group_id(entry_group_id)
        required_labels: dict[str, str] = {"type": "context-set"}
        if labels:
            required_labels.update({str(k): str(v) for k, v in labels.items()})
            required_labels["type"] = "context-set"
        return required_labels

    @staticmethod
    def _merge_adopt_labels(existing: Mapping[str, Any], required_labels: Mapping[str, str]) -> dict[str, str]:
        existing_labels = existing.get("labels")
        merged = dict(existing_labels) if isinstance(existing_labels, Mapping) else {}
        merged.update(required_labels)
        return merged

    @staticmethod
    def _finalize_operation(current: dict[str, Any]) -> dict[str, Any]:
        op_error = current.get("error")
        if isinstance(op_error, Mapping):
            raise _error_from_rpc_status(op_error, http_status=None, fallback_message="Dataplex operation failed")
        response = current.get("response")
        return dict(response) if isinstance(response, Mapping) else current


class SyncKnowledgeCatalogClient(_BaseKnowledgeCatalogClient):
    """Synchronous Google Cloud Dataplex v1 Knowledge Catalog client."""

    def __init__(
        self,
        config: KnowledgeCatalogConfig,
        *,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | GcpTokenCachePort | None = None,
        transport: SyncHttpTransport | None = None,
        target_service_account: str | None = None,
        poll_interval_ms: int = 250,
    ) -> None:
        super().__init__(
            config,
            target_service_account=target_service_account,
            poll_interval_ms=poll_interval_ms,
        )
        if isinstance(token_cache, AsyncGcpTokenCache):
            self._token_cache: Any = token_cache.to_sync()
        else:
            self._token_cache = token_cache
        self._transport: SyncHttpTransport = transport or gcp_http2_request_sync

    def _ensure_token_cache(self) -> Any:
        if self._token_cache is None:
            try:
                self._token_cache = SyncGcpTokenCache(self._token_cache_options(), list(DEFAULT_DATAPLEX_SCOPES))
            except ValueError as exc:
                raise ImproperConfigurationError(
                    f"KnowledgeCatalog failed to resolve GCP credentials from ADC: {exc}"
                ) from exc
        return self._token_cache

    def _acquire_token_snapshot(self, cache: Any) -> GcpTokenSnapshot:
        try:
            snap = cache.get_token()
        except ValueError as exc:
            raise ImproperConfigurationError(
                f"KnowledgeCatalog failed to resolve GCP credentials from ADC: {exc}"
            ) from exc
        if inspect.isawaitable(snap):
            raise ImproperConfigurationError("SyncKnowledgeCatalogClient requires a synchronous token_cache")
        return cast("GcpTokenSnapshot", snap)

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        body: Mapping[str, Any] | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        payload_bytes = encode_json_bytes(body) if body is not None else None
        effective_timeout = timeout_ms if timeout_ms is not None else self._config.timeout_ms
        cache = self._ensure_token_cache()
        snap = self._acquire_token_snapshot(cache)
        headers = self._build_headers(snap, has_body=payload_bytes is not None)
        status, _, resp_body = self._transport(method, url, headers, payload_bytes, effective_timeout)
        if status == 401:
            cache.invalidate(snap.generation)
            snap_retry = self._acquire_token_snapshot(cache)
            retry_headers = self._build_headers(snap_retry, has_body=payload_bytes is not None)
            status, _, resp_body = self._transport(method, url, retry_headers, payload_bytes, effective_timeout)
        if not (200 <= status < 300):
            raise _error_from_http_response(status, resp_body)
        return _decode_json_dict(resp_body)

    def get_entry_group(
        self,
        entry_group_id: str | None = None,
        *,
        location: str | None = None,
    ) -> dict[str, Any]:
        """Fetch a Dataplex `EntryGroup` resource, permitting `@*` system entry groups."""
        path = self._entry_group_path(entry_group_id, location=location, for_create=False)
        return self._request_json("GET", self._url(path))

    def list_entry_groups(
        self,
        *,
        location: str | None = None,
        labels: Mapping[str, str] | None = None,
        page_size: int | None = None,
    ) -> list[dict[str, Any]]:
        """List entry groups in `location` and apply optional `labels` filtering client-side."""
        results: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            url = self._build_entry_groups_url(location, page_size=page_size, page_token=page_token)
            page_token = self._collect_entry_groups_page(self._request_json("GET", url), labels, results)
            if page_token is None:
                break
        return results

    def create_entry_group(
        self,
        entry_group_id: str | None = None,
        *,
        location: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
        labels: Mapping[str, str] | None = None,
        validate_only: bool = False,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """Create an RFC-1035 `EntryGroup` and poll the Dataplex LRO until completion."""
        url, body, effective_timeout = self._prepare_create_entry_group(
            entry_group_id,
            location=location,
            display_name=display_name,
            description=description,
            labels=labels,
            validate_only=validate_only,
            timeout_ms=timeout_ms,
        )
        operation = self._request_json("POST", url, body=body, timeout_ms=effective_timeout)
        return self._await_operation(operation, timeout_ms=effective_timeout)

    def _await_operation(self, operation: dict[str, Any], *, timeout_ms: int) -> dict[str, Any]:
        deadline = time.monotonic() + (timeout_ms / 1000.0)
        current = operation
        while not current.get("done"):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                op_name = current.get("name", "<unknown>")
                raise _build_google_cloud_error(
                    message=f"Timed out waiting for Dataplex operation {op_name}",
                    code=4,
                    code_name="DEADLINE_EXCEEDED",
                    http_status=504,
                )
            if self._poll_interval_ms > 0:
                time.sleep(min(self._poll_interval_ms / 1000.0, remaining))
            op_name = current.get("name")
            if not isinstance(op_name, str) or not op_name:
                raise _build_google_cloud_error(
                    message="Dataplex operation response missing operation 'name'",
                    code=13,
                    code_name="INTERNAL",
                    http_status=None,
                )
            current = self._request_json("GET", self._operation_url(op_name), timeout_ms=timeout_ms)

        return self._finalize_operation(current)

    def _patch_entry_group_labels(
        self,
        resource_name: str,
        *,
        labels: Mapping[str, str],
    ) -> dict[str, Any]:
        operation = self._request_json(
            "PATCH",
            self._url(resource_name, [("updateMask", "labels")]),
            body={"labels": {str(k): str(v) for k, v in labels.items()}},
        )
        if "done" in operation:
            return self._await_operation(operation, timeout_ms=self._config.timeout_ms)
        return operation

    def resolve_context_set(
        self,
        entry_group_id: str | None = None,
        *,
        location: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Return an existing `type=context-set` entry group, adopt an unlabeled one, or create it."""
        required_labels = self._required_context_labels(entry_group_id, labels)

        groups = self.list_entry_groups(location=location)
        action, existing, target_id = self._plan_context_set_action(
            groups,
            requested_id=entry_group_id,
            default_id=self._config.entry_group_id,
            required_labels=required_labels,
        )
        if action == "return" and existing is not None:
            return existing
        if action == "adopt" and existing is not None:
            merged = self._merge_adopt_labels(existing, required_labels)
            return self._patch_entry_group_labels(str(existing["name"]), labels=merged)

        _validate_create_entry_group_id(target_id)
        return self.create_entry_group(
            target_id,
            location=location,
            display_name=display_name,
            description=description,
            labels=required_labels,
        )

    def get_entry(
        self,
        entry_id: str,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        view: str = "ALL",
    ) -> CatalogEntryRecord:
        """Fetch a single Dataplex `Entry`, defaulting to `view='ALL'` so aspects hydrate."""
        path = self._entry_path(entry_id, entry_group_id=entry_group_id, location=location)
        query = [("view", view)] if view else []
        data = self._request_json("GET", self._url(path, query))
        return CatalogEntryRecord.from_dataplex_dict(data)

    def list_entries_page(
        self,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        filter_expr: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
    ) -> tuple[list[CatalogEntryRecord], str | None]:
        """Fetch one page of Dataplex entries, omitting `view` as required on list endpoints."""
        url = self._prepare_list_entries_url(
            entry_group_id, location=location, filter_expr=filter_expr, page_size=page_size, page_token=page_token
        )
        return self._parse_entries_page(self._request_json("GET", url))

    def list_entries(
        self,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        filter_expr: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
    ) -> list[CatalogEntryRecord]:
        """List all entries in an entry group across `nextPageToken` pages."""
        all_records: list[CatalogEntryRecord] = []
        cursor = page_token
        while True:
            page_records, next_token = self.list_entries_page(
                entry_group_id=entry_group_id,
                location=location,
                filter_expr=filter_expr,
                page_size=page_size,
                page_token=cursor,
            )
            all_records.extend(page_records)
            if not next_token:
                break
            cursor = next_token
        return all_records

    def create_entry(
        self,
        entry_id: str,
        *,
        entry_type: str | None = None,
        entry_group_id: str | None = None,
        location: str | None = None,
        fully_qualified_name: str | None = None,
        parent_entry: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
        labels: Mapping[str, str] | None = None,
        aspects: Mapping[str, Any] | None = None,
        entry_source: Mapping[str, Any] | None = None,
    ) -> CatalogEntryRecord:
        """Create a Dataplex `Entry` inside an entry group."""
        eg_path = self._entry_group_path(entry_group_id, location=location, for_create=False)
        body = self._build_entry_payload(
            entry_type=entry_type,
            fully_qualified_name=fully_qualified_name,
            parent_entry=parent_entry,
            display_name=display_name,
            description=description,
            labels=labels,
            aspects=aspects,
            entry_source=entry_source,
            include_default_entry_type=True,
        )
        data = self._request_json(
            "POST",
            self._url(f"{eg_path}/entries", [("entryId", entry_id)]),
            body=body,
        )
        return CatalogEntryRecord.from_dataplex_dict(data)

    def update_entry(
        self,
        entry_id: str,
        *,
        entry_type: str | None = None,
        entry_group_id: str | None = None,
        location: str | None = None,
        fully_qualified_name: str | None = None,
        parent_entry: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
        labels: Mapping[str, str] | None = None,
        aspects: Mapping[str, Any] | None = None,
        entry_source: Mapping[str, Any] | None = None,
        update_mask: str | Sequence[str] = ("aspects", "entrySource"),
        aspect_keys: Sequence[str] | None = None,
        allow_missing: bool = False,
        delete_missing_aspects: bool = False,
    ) -> CatalogEntryRecord:
        """Patch a Dataplex `Entry` with `updateMask` and repeated `aspectKeys` query parameters."""
        path = self._entry_path(entry_id, entry_group_id=entry_group_id, location=location)
        query = self._build_update_entry_query(
            update_mask=update_mask,
            aspect_keys=aspect_keys,
            aspects=aspects,
            allow_missing=allow_missing,
            delete_missing_aspects=delete_missing_aspects,
        )
        body = self._build_entry_payload(
            entry_type=entry_type,
            fully_qualified_name=fully_qualified_name,
            parent_entry=parent_entry,
            display_name=display_name,
            description=description,
            labels=labels,
            aspects=aspects,
            entry_source=entry_source,
            include_default_entry_type=False,
        )
        data = self._request_json("PATCH", self._url(path, query), body=body)
        return CatalogEntryRecord.from_dataplex_dict(data)

    def delete_entry(
        self,
        entry_id: str,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
    ) -> None:
        """Delete a Dataplex `Entry` from an entry group."""
        path = self._entry_path(entry_id, entry_group_id=entry_group_id, location=location)
        self._request_json("DELETE", self._url(path))

    def sync_bundle(
        self,
        bundle: OkfBundle,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        delete_stale: bool = False,
        entry_type: str | None = None,
        aspect_type: str | None = None,
    ) -> CatalogSyncSummary:
        """Progressively sync an `OkfBundle` by SHA-256 digest in `<= 250`-entry batches.

        With `delete_stale=True`, only stale entries that `sync_bundle` itself wrote
        (`CatalogEntryRecord.is_okf_managed`) are deleted; entries created by other tools in the
        same entry group are left untouched.

        Raises:
            CatalogSyncError: When a mutation fails. The original error is chained as `__cause__`
                and `CatalogSyncError.summary` lists the entry IDs that completed before the failure.
        """
        resolved_eg = entry_group_id if entry_group_id is not None else self._config.entry_group_id
        _validate_lookup_entry_group_id(resolved_eg)
        existing = self.list_entries(entry_group_id=resolved_eg, location=location)
        mutations, skipped_ids = self._plan_sync_operations(
            bundle,
            existing,
            delete_stale=delete_stale,
        )
        batches = self._partition_mutation_batches(mutations)
        created_ids: list[str] = []
        updated_ids: list[str] = []
        deleted_ids: list[str] = []

        try:
            for batch in batches:
                for op, entry_id, doc in batch:
                    if op == "delete" or doc is None:
                        self.delete_entry(entry_id, entry_group_id=resolved_eg, location=location)
                        deleted_ids.append(entry_id)
                        continue
                    fields = self._build_okf_entry_fields(doc, entry_type=entry_type, aspect_type=aspect_type)
                    if op == "create":
                        self.create_entry(entry_id, entry_group_id=resolved_eg, location=location, **fields)
                        created_ids.append(entry_id)
                    else:
                        self.update_entry(entry_id, entry_group_id=resolved_eg, location=location, **fields)
                        updated_ids.append(entry_id)
        except Exception as exc:
            partial = self._build_sync_summary(
                bundle,
                entry_group_id=resolved_eg,
                batches=batches,
                created_ids=created_ids,
                updated_ids=updated_ids,
                skipped_ids=skipped_ids,
                deleted_ids=deleted_ids,
            )
            raise CatalogSyncError(partial, (exc,)) from exc

        return self._build_sync_summary(
            bundle,
            entry_group_id=resolved_eg,
            batches=batches,
            created_ids=created_ids,
            updated_ids=updated_ids,
            skipped_ids=skipped_ids,
            deleted_ids=deleted_ids,
        )

    def search_entries_page(
        self,
        query: str,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        page_size: int = 20,
        page_token: str | None = None,
        semantic_search: bool = True,
    ) -> tuple[list[CatalogEntryRecord], str | None]:
        """Fetch one `locations/global:searchEntries` page and its `nextPageToken`.

        Pass the returned token back as `page_token` to fetch the following page; the token is
        `None` on the last page. `search_entries` returns only the records of a single page.
        """
        url, payload = self._build_search_payload(
            query,
            entry_group_id=entry_group_id,
            location=location,
            page_size=page_size,
            page_token=page_token,
            semantic_search=semantic_search,
        )
        data = self._request_json("POST", url, body=payload)
        return (_parse_search_results(data), _parse_next_page_token(data))

    def search_entries(
        self,
        query: str,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        page_size: int = 20,
        page_token: str | None = None,
        semantic_search: bool = True,
    ) -> list[CatalogEntryRecord]:
        """Search Dataplex Universal Catalog and return the records of one result page.

        The response's `nextPageToken` is not returned; use `search_entries_page` to walk
        results beyond the page selected by `page_token`.
        """
        records, _ = self.search_entries_page(
            query,
            entry_group_id=entry_group_id,
            location=location,
            page_size=page_size,
            page_token=page_token,
            semantic_search=semantic_search,
        )
        return records


class AsyncKnowledgeCatalogClient(_BaseKnowledgeCatalogClient):
    """Asynchronous Google Cloud Dataplex v1 Knowledge Catalog client."""

    def __init__(
        self,
        config: KnowledgeCatalogConfig,
        *,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | GcpTokenCachePort | None = None,
        transport: AsyncHttpTransport | None = None,
        target_service_account: str | None = None,
        poll_interval_ms: int = 250,
    ) -> None:
        super().__init__(
            config,
            target_service_account=target_service_account,
            poll_interval_ms=poll_interval_ms,
        )
        if isinstance(token_cache, SyncGcpTokenCache):
            self._token_cache: Any = token_cache.to_async()
        else:
            self._token_cache = token_cache
        self._transport: AsyncHttpTransport = transport or gcp_http2_request_async

    def _ensure_token_cache(self) -> Any:
        if self._token_cache is None:
            try:
                self._token_cache = AsyncGcpTokenCache(self._token_cache_options(), list(DEFAULT_DATAPLEX_SCOPES))
            except ValueError as exc:
                raise ImproperConfigurationError(
                    f"KnowledgeCatalog failed to resolve GCP credentials from ADC: {exc}"
                ) from exc
        return self._token_cache

    async def _acquire_token_snapshot(self, cache: Any) -> GcpTokenSnapshot:
        try:
            raw = cache.get_token()
            snap = await raw if inspect.isawaitable(raw) else raw
            return cast("GcpTokenSnapshot", snap)
        except ValueError as exc:
            raise ImproperConfigurationError(
                f"KnowledgeCatalog failed to resolve GCP credentials from ADC: {exc}"
            ) from exc

    async def _invalidate_token(self, cache: Any, generation: int) -> None:
        result = cache.invalidate(generation)
        if inspect.isawaitable(result):
            await result

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        body: Mapping[str, Any] | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        payload_bytes = encode_json_bytes(body) if body is not None else None
        effective_timeout = timeout_ms if timeout_ms is not None else self._config.timeout_ms
        cache = self._ensure_token_cache()
        snap = await self._acquire_token_snapshot(cache)
        headers = self._build_headers(snap, has_body=payload_bytes is not None)
        status, _, resp_body = await self._transport(method, url, headers, payload_bytes, effective_timeout)
        if status == 401:
            await self._invalidate_token(cache, snap.generation)
            snap_retry = await self._acquire_token_snapshot(cache)
            retry_headers = self._build_headers(snap_retry, has_body=payload_bytes is not None)
            status, _, resp_body = await self._transport(method, url, retry_headers, payload_bytes, effective_timeout)
        if not (200 <= status < 300):
            raise _error_from_http_response(status, resp_body)
        return _decode_json_dict(resp_body)

    async def get_entry_group(
        self,
        entry_group_id: str | None = None,
        *,
        location: str | None = None,
    ) -> dict[str, Any]:
        """Fetch a Dataplex `EntryGroup` resource, permitting `@*` system entry groups."""
        path = self._entry_group_path(entry_group_id, location=location, for_create=False)
        return await self._request_json("GET", self._url(path))

    async def list_entry_groups(
        self,
        *,
        location: str | None = None,
        labels: Mapping[str, str] | None = None,
        page_size: int | None = None,
    ) -> list[dict[str, Any]]:
        """List entry groups in `location` and apply optional `labels` filtering client-side."""
        results: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            url = self._build_entry_groups_url(location, page_size=page_size, page_token=page_token)
            page_token = self._collect_entry_groups_page(await self._request_json("GET", url), labels, results)
            if page_token is None:
                break
        return results

    async def create_entry_group(
        self,
        entry_group_id: str | None = None,
        *,
        location: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
        labels: Mapping[str, str] | None = None,
        validate_only: bool = False,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """Create an RFC-1035 `EntryGroup` and poll the Dataplex LRO until completion."""
        url, body, effective_timeout = self._prepare_create_entry_group(
            entry_group_id,
            location=location,
            display_name=display_name,
            description=description,
            labels=labels,
            validate_only=validate_only,
            timeout_ms=timeout_ms,
        )
        operation = await self._request_json("POST", url, body=body, timeout_ms=effective_timeout)
        return await self._await_operation(operation, timeout_ms=effective_timeout)

    async def _await_operation(self, operation: dict[str, Any], *, timeout_ms: int) -> dict[str, Any]:
        deadline = time.monotonic() + (timeout_ms / 1000.0)
        current = operation
        while not current.get("done"):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                op_name = current.get("name", "<unknown>")
                raise _build_google_cloud_error(
                    message=f"Timed out waiting for Dataplex operation {op_name}",
                    code=4,
                    code_name="DEADLINE_EXCEEDED",
                    http_status=504,
                )
            if self._poll_interval_ms > 0:
                await asyncio.sleep(min(self._poll_interval_ms / 1000.0, remaining))
            op_name = current.get("name")
            if not isinstance(op_name, str) or not op_name:
                raise _build_google_cloud_error(
                    message="Dataplex operation response missing operation 'name'",
                    code=13,
                    code_name="INTERNAL",
                    http_status=None,
                )
            current = await self._request_json("GET", self._operation_url(op_name), timeout_ms=timeout_ms)

        return self._finalize_operation(current)

    async def _patch_entry_group_labels(
        self,
        resource_name: str,
        *,
        labels: Mapping[str, str],
    ) -> dict[str, Any]:
        operation = await self._request_json(
            "PATCH",
            self._url(resource_name, [("updateMask", "labels")]),
            body={"labels": {str(k): str(v) for k, v in labels.items()}},
        )
        if "done" in operation:
            return await self._await_operation(operation, timeout_ms=self._config.timeout_ms)
        return operation

    async def resolve_context_set(
        self,
        entry_group_id: str | None = None,
        *,
        location: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Return an existing `type=context-set` entry group, adopt an unlabeled one, or create it."""
        required_labels = self._required_context_labels(entry_group_id, labels)

        groups = await self.list_entry_groups(location=location)
        action, existing, target_id = self._plan_context_set_action(
            groups,
            requested_id=entry_group_id,
            default_id=self._config.entry_group_id,
            required_labels=required_labels,
        )
        if action == "return" and existing is not None:
            return existing
        if action == "adopt" and existing is not None:
            merged = self._merge_adopt_labels(existing, required_labels)
            return await self._patch_entry_group_labels(str(existing["name"]), labels=merged)

        _validate_create_entry_group_id(target_id)
        return await self.create_entry_group(
            target_id,
            location=location,
            display_name=display_name,
            description=description,
            labels=required_labels,
        )

    async def get_entry(
        self,
        entry_id: str,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        view: str = "ALL",
    ) -> CatalogEntryRecord:
        """Fetch a single Dataplex `Entry`, defaulting to `view='ALL'` so aspects hydrate."""
        path = self._entry_path(entry_id, entry_group_id=entry_group_id, location=location)
        query = [("view", view)] if view else []
        data = await self._request_json("GET", self._url(path, query))
        return CatalogEntryRecord.from_dataplex_dict(data)

    async def list_entries_page(
        self,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        filter_expr: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
    ) -> tuple[list[CatalogEntryRecord], str | None]:
        """Fetch one page of Dataplex entries, omitting `view` as required on list endpoints."""
        url = self._prepare_list_entries_url(
            entry_group_id, location=location, filter_expr=filter_expr, page_size=page_size, page_token=page_token
        )
        return self._parse_entries_page(await self._request_json("GET", url))

    async def list_entries(
        self,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        filter_expr: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
    ) -> list[CatalogEntryRecord]:
        """List all entries in an entry group across `nextPageToken` pages."""
        all_records: list[CatalogEntryRecord] = []
        cursor = page_token
        while True:
            page_records, next_token = await self.list_entries_page(
                entry_group_id=entry_group_id,
                location=location,
                filter_expr=filter_expr,
                page_size=page_size,
                page_token=cursor,
            )
            all_records.extend(page_records)
            if not next_token:
                break
            cursor = next_token
        return all_records

    async def create_entry(
        self,
        entry_id: str,
        *,
        entry_type: str | None = None,
        entry_group_id: str | None = None,
        location: str | None = None,
        fully_qualified_name: str | None = None,
        parent_entry: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
        labels: Mapping[str, str] | None = None,
        aspects: Mapping[str, Any] | None = None,
        entry_source: Mapping[str, Any] | None = None,
    ) -> CatalogEntryRecord:
        """Create a Dataplex `Entry` inside an entry group."""
        eg_path = self._entry_group_path(entry_group_id, location=location, for_create=False)
        body = self._build_entry_payload(
            entry_type=entry_type,
            fully_qualified_name=fully_qualified_name,
            parent_entry=parent_entry,
            display_name=display_name,
            description=description,
            labels=labels,
            aspects=aspects,
            entry_source=entry_source,
            include_default_entry_type=True,
        )
        data = await self._request_json(
            "POST",
            self._url(f"{eg_path}/entries", [("entryId", entry_id)]),
            body=body,
        )
        return CatalogEntryRecord.from_dataplex_dict(data)

    async def update_entry(
        self,
        entry_id: str,
        *,
        entry_type: str | None = None,
        entry_group_id: str | None = None,
        location: str | None = None,
        fully_qualified_name: str | None = None,
        parent_entry: str | None = None,
        display_name: str | None = None,
        description: str | None = None,
        labels: Mapping[str, str] | None = None,
        aspects: Mapping[str, Any] | None = None,
        entry_source: Mapping[str, Any] | None = None,
        update_mask: str | Sequence[str] = ("aspects", "entrySource"),
        aspect_keys: Sequence[str] | None = None,
        allow_missing: bool = False,
        delete_missing_aspects: bool = False,
    ) -> CatalogEntryRecord:
        """Patch a Dataplex `Entry` with `updateMask` and repeated `aspectKeys` query parameters."""
        path = self._entry_path(entry_id, entry_group_id=entry_group_id, location=location)
        query = self._build_update_entry_query(
            update_mask=update_mask,
            aspect_keys=aspect_keys,
            aspects=aspects,
            allow_missing=allow_missing,
            delete_missing_aspects=delete_missing_aspects,
        )
        body = self._build_entry_payload(
            entry_type=entry_type,
            fully_qualified_name=fully_qualified_name,
            parent_entry=parent_entry,
            display_name=display_name,
            description=description,
            labels=labels,
            aspects=aspects,
            entry_source=entry_source,
            include_default_entry_type=False,
        )
        data = await self._request_json("PATCH", self._url(path, query), body=body)
        return CatalogEntryRecord.from_dataplex_dict(data)

    async def delete_entry(
        self,
        entry_id: str,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
    ) -> None:
        """Delete a Dataplex `Entry` from an entry group."""
        path = self._entry_path(entry_id, entry_group_id=entry_group_id, location=location)
        await self._request_json("DELETE", self._url(path))

    async def _execute_mutation(
        self,
        op: str,
        entry_id: str,
        doc: OkfDocument | None,
        *,
        entry_group_id: str,
        location: str | None,
        entry_type: str | None,
        aspect_type: str | None,
    ) -> tuple[str, str]:
        if op == "delete" or doc is None:
            await self.delete_entry(entry_id, entry_group_id=entry_group_id, location=location)
            return ("delete", entry_id)
        fields = self._build_okf_entry_fields(doc, entry_type=entry_type, aspect_type=aspect_type)
        if op == "create":
            await self.create_entry(
                entry_id,
                entry_group_id=entry_group_id,
                location=location,
                **fields,
            )
            return ("create", entry_id)
        await self.update_entry(
            entry_id,
            entry_group_id=entry_group_id,
            location=location,
            **fields,
        )
        return ("update", entry_id)

    async def sync_bundle(
        self,
        bundle: OkfBundle,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        delete_stale: bool = False,
        entry_type: str | None = None,
        aspect_type: str | None = None,
    ) -> CatalogSyncSummary:
        """Progressively sync an `OkfBundle` by SHA-256 digest in `<= 250`-entry batches.

        With `delete_stale=True`, only stale entries that `sync_bundle` itself wrote
        (`CatalogEntryRecord.is_okf_managed`) are deleted; entries created by other tools in the
        same entry group are left untouched.

        Mutations of one batch run concurrently. On the first failure the remaining in-flight
        mutations of that batch are cancelled and awaited before the error is raised.

        Raises:
            CatalogSyncError: When a mutation fails. The original error is chained as `__cause__`
                and `CatalogSyncError.summary` lists the entry IDs that completed before the failure.
        """
        resolved_eg = entry_group_id if entry_group_id is not None else self._config.entry_group_id
        _validate_lookup_entry_group_id(resolved_eg)
        existing = await self.list_entries(entry_group_id=resolved_eg, location=location)
        mutations, skipped_ids = self._plan_sync_operations(
            bundle,
            existing,
            delete_stale=delete_stale,
        )
        batches = self._partition_mutation_batches(mutations)
        created_ids: list[str] = []
        updated_ids: list[str] = []
        deleted_ids: list[str] = []

        for batch in batches:
            tasks: list[asyncio.Task[tuple[str, str]]] = []
            try:
                async with asyncio.TaskGroup() as group:
                    tasks.extend(
                        group.create_task(
                            self._execute_mutation(
                                op,
                                entry_id,
                                doc,
                                entry_group_id=resolved_eg,
                                location=location,
                                entry_type=entry_type,
                                aspect_type=aspect_type,
                            )
                        )
                        for op, entry_id, doc in batch
                    )
            except ExceptionGroup as failures:
                self._collect_mutation_outcomes(tasks, created_ids, updated_ids, deleted_ids)
                partial = self._build_sync_summary(
                    bundle,
                    entry_group_id=resolved_eg,
                    batches=batches,
                    created_ids=created_ids,
                    updated_ids=updated_ids,
                    skipped_ids=skipped_ids,
                    deleted_ids=deleted_ids,
                )
                errors = self._flatten_exception_group(failures)
                raise CatalogSyncError(partial, errors) from errors[0]
            self._collect_mutation_outcomes(tasks, created_ids, updated_ids, deleted_ids)

        return self._build_sync_summary(
            bundle,
            entry_group_id=resolved_eg,
            batches=batches,
            created_ids=created_ids,
            updated_ids=updated_ids,
            skipped_ids=skipped_ids,
            deleted_ids=deleted_ids,
        )

    async def search_entries_page(
        self,
        query: str,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        page_size: int = 20,
        page_token: str | None = None,
        semantic_search: bool = True,
    ) -> tuple[list[CatalogEntryRecord], str | None]:
        """Fetch one `locations/global:searchEntries` page and its `nextPageToken`.

        Pass the returned token back as `page_token` to fetch the following page; the token is
        `None` on the last page. `search_entries` returns only the records of a single page.
        """
        url, payload = self._build_search_payload(
            query,
            entry_group_id=entry_group_id,
            location=location,
            page_size=page_size,
            page_token=page_token,
            semantic_search=semantic_search,
        )
        data = await self._request_json("POST", url, body=payload)
        return (_parse_search_results(data), _parse_next_page_token(data))

    async def search_entries(
        self,
        query: str,
        *,
        entry_group_id: str | None = None,
        location: str | None = None,
        page_size: int = 20,
        page_token: str | None = None,
        semantic_search: bool = True,
    ) -> list[CatalogEntryRecord]:
        """Search Dataplex Universal Catalog and return the records of one result page.

        The response's `nextPageToken` is not returned; use `search_entries_page` to walk
        results beyond the page selected by `page_token`.
        """
        records, _ = await self.search_entries_page(
            query,
            entry_group_id=entry_group_id,
            location=location,
            page_size=page_size,
            page_token=page_token,
            semantic_search=semantic_search,
        )
        return records

    @staticmethod
    def _collect_mutation_outcomes(
        tasks: Sequence[asyncio.Task[tuple[str, str]]],
        created_ids: list[str],
        updated_ids: list[str],
        deleted_ids: list[str],
    ) -> None:
        """Append the entry IDs of successfully finished mutation tasks to their per-operation lists."""
        for task in tasks:
            if not task.done() or task.cancelled() or task.exception() is not None:
                continue
            op, entry_id = task.result()
            if op == "create":
                created_ids.append(entry_id)
            elif op == "update":
                updated_ids.append(entry_id)
            else:
                deleted_ids.append(entry_id)

    @classmethod
    def _flatten_exception_group(cls, group: ExceptionGroup[Exception]) -> list[Exception]:
        """Flatten nested exception groups into their leaf exceptions in order."""
        leaves: list[Exception] = []
        for exc in group.exceptions:
            if isinstance(exc, ExceptionGroup):
                leaves.extend(cls._flatten_exception_group(exc))
            else:
                leaves.append(exc)
        return leaves
