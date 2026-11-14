"""External Object Table configuration, storage catalog listing, and presigned reference hydration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

import msgspec

from vertebra.db.adapters.bigquery.core import BigQueryTableIdentifier
from vertebra.typing import (
    FrozenBaseStruct,
    ImproperConfigurationError,
    VertebraError,
)
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.storage import AsyncObjectStore, SyncObjectStore

__all__ = (
    "ObjectRef",
    "ObjectTableConfig",
    "hydrate_object_refs_async",
    "hydrate_object_refs_sync",
)


class ObjectRef(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Metadata and optional presigned HTTP URL for an object row in a BigQuery Object Table."""

    uri: str
    """Canonical `gs://bucket/key` or multicloud object URI."""

    version: str | None = None
    """Storage object generation or version identifier."""

    content_type: str | None = None
    """MIME content type reported by the object store."""

    size: int = 0
    """Object payload size in bytes."""

    md5_hex: str | None = None
    """Hex or base64 MD5 digest reported by BigQuery Object Table metadata."""

    updated: str | None = None
    """Last-modified timestamp string or microsecond epoch representation."""

    signed_url: str | None = None
    """Zero-RTT presigned `GET` URL generated via `vertebra.storage.SyncObjectStore`."""


class ObjectTableConfig(FrozenBaseStruct, frozen=True, kw_only=True):
    """Configuration for creating and querying a BigQuery External Object Table over GCS."""

    table_name: str
    """Qualified or unqualified BigQuery external table name (such as `dataset.invoices_obj`)."""

    connection_id: str
    """BigQuery Cloud Resource connection identifier (such as `us.gcs-conn`)."""

    uris: tuple[str, ...]
    """One or more `gs://bucket/prefix/*` URI patterns backing the Object Table."""

    object_metadata: Literal["SIMPLE", "DIRECTORY"] = "SIMPLE"
    """BigQuery `object_metadata` mode (`SIMPLE` or `DIRECTORY`)."""

    max_staleness: str | None = None
    """Optional metadata cache staleness interval literal (such as `INTERVAL 1 HOUR`)."""

    metadata_cache_mode: Literal["AUTOMATIC", "MANUAL"] | None = None
    """Optional BigQuery metadata cache refresh mode (`AUTOMATIC` or `MANUAL`)."""

    def create_object_table_sql(self, *, if_not_exists: bool = True) -> str:
        """Render the GoogleSQL `CREATE EXTERNAL TABLE ... WITH CONNECTION ... OPTIONS(...)` DDL."""
        if not self.uris:
            raise ImproperConfigurationError("ObjectTableConfig.uris must contain at least one URI")
        quoted_table = BigQueryTableIdentifier.validate_and_quote(self.table_name, field_name="table_name")
        validate_identifier(self.connection_id, "connection_id", allow_hyphens=True)
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        staleness_clause = f"\nWITH MAX STALENESS {self.max_staleness}" if self.max_staleness else ""
        escaped_uris = ", ".join("'" + u.replace("'", "\\'") + "'" for u in self.uris)
        options_parts = [
            f"object_metadata = '{self.object_metadata}'",
            f"uris = [{escaped_uris}]",
        ]
        if self.metadata_cache_mode is not None:
            options_parts.append(f"metadata_cache_mode = '{self.metadata_cache_mode}'")
        options_sql = ",\n  ".join(options_parts)
        return (
            f"CREATE EXTERNAL TABLE {exists_clause}{quoted_table}\n"
            f"WITH CONNECTION `{self.connection_id}`{staleness_clause}\n"
            f"OPTIONS (\n  {options_sql}\n)"
        )

    def build_select_sql(
        self,
        *,
        where: str | None = None,
        limit: int | None = None,
    ) -> str:
        """Build a `SELECT` query against this BigQuery Object Table."""
        quoted_table = BigQueryTableIdentifier.validate_and_quote(self.table_name)
        sql = f"SELECT uri, generation, content_type, size, md5_hash, updated FROM {quoted_table}"
        if where:
            sql += f" WHERE {where}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return sql

    def list_from_store_sync(
        self,
        store: Any,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List objects directly from `store` when the Object Table catalog query is unavailable."""
        if limit is not None and limit <= 0:
            return []
        sync_store = store.to_sync() if hasattr(store, "to_sync") else store
        rows: list[dict[str, Any]] = []
        for pattern in self.uris:
            scheme = urlparse(pattern).scheme or "gs"
            bucket, prefix = self._extract_bucket_prefix_from_uri(pattern)
            try:
                items = sync_store.list(prefix or "")
            except (VertebraError, OSError, ValueError):
                items = []
            for item in items:
                rows.append(self._normalize_store_item_to_row(item, scheme=scheme, bucket=bucket))
                if limit is not None and len(rows) >= limit:
                    return rows
        return rows

    async def list_from_store_async(
        self,
        store: Any,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Asynchronously list objects directly from `store` when the Object Table catalog is unavailable."""
        if limit is not None and limit <= 0:
            return []
        async_store = store.to_async() if hasattr(store, "to_async") else store
        rows: list[dict[str, Any]] = []
        for pattern in self.uris:
            scheme = urlparse(pattern).scheme or "gs"
            bucket, prefix = self._extract_bucket_prefix_from_uri(pattern)
            try:
                items = await async_store.list(prefix or "")
            except (VertebraError, OSError, ValueError):
                items = []
            for item in items:
                rows.append(self._normalize_store_item_to_row(item, scheme=scheme, bucket=bucket))
                if limit is not None and len(rows) >= limit:
                    return rows
        return rows

    def hydrate_object_refs_sync(
        self,
        rows: Sequence[Mapping[str, Any]] | Sequence[ObjectRef],
        store: Any | None = None,
        *,
        expires_in_secs: int = 3600,
        fallback_to_authenticated_url: bool = True,
    ) -> list[ObjectRef]:
        """Convert Object Table rows into `ObjectRef` structs with presigned URLs synchronously."""
        if store is not None:
            sync_store = store.to_sync() if hasattr(store, "to_sync") else store
            return hydrate_object_refs_sync(
                rows,
                sync_store,
                expires_in_secs=expires_in_secs,
                fallback_to_authenticated_url=fallback_to_authenticated_url,
            )
        return _hydrate_refs_without_store(
            rows,
            fallback_to_authenticated_url=fallback_to_authenticated_url,
        )

    async def hydrate_object_refs_async(
        self,
        rows: Sequence[Mapping[str, Any]] | Sequence[ObjectRef],
        store: Any | None = None,
        *,
        expires_in_secs: int = 3600,
        fallback_to_authenticated_url: bool = True,
    ) -> list[ObjectRef]:
        """Asynchronously convert Object Table rows into `ObjectRef` structs with presigned URLs."""
        if store is not None:
            async_store = store.to_async() if hasattr(store, "to_async") else store
            return await hydrate_object_refs_async(
                rows,
                async_store,
                expires_in_secs=expires_in_secs,
                fallback_to_authenticated_url=fallback_to_authenticated_url,
            )
        return _hydrate_refs_without_store(
            rows,
            fallback_to_authenticated_url=fallback_to_authenticated_url,
        )

    @staticmethod
    def _extract_bucket_prefix_from_uri(uri: str) -> tuple[str, str]:
        if "://" not in uri:
            return "", uri.lstrip("/").rstrip("*")
        parsed = urlparse(uri)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/").rstrip("*")
        return bucket, prefix

    @staticmethod
    def _normalize_store_item_to_row(
        item: Any,
        *,
        scheme: str,
        bucket: str,
    ) -> dict[str, Any]:
        if isinstance(item, str):
            loc = item.lstrip("/")
            full_uri = f"{scheme}://{bucket}/{loc}" if bucket else f"{scheme}://{loc}"
            return {
                "uri": full_uri,
                "generation": None,
                "content_type": None,
                "size": 0,
                "md5_hash": None,
                "updated": None,
            }
        if isinstance(item, Mapping):
            loc = str(item.get("location") or item.get("path") or "").lstrip("/")
            full_uri = f"{scheme}://{bucket}/{loc}" if bucket else f"{scheme}://{loc}"
            return {
                "uri": full_uri,
                "generation": item.get("version") or item.get("e_tag") or item.get("etag"),
                "content_type": item.get("content_type"),
                "size": int(item.get("size", 0) or 0),
                "md5_hash": item.get("md5") or item.get("e_tag") or item.get("etag"),
                "updated": item.get("last_modified"),
            }
        loc = str(getattr(item, "path", None) or getattr(item, "location", "") or "").lstrip("/")
        full_uri = f"{scheme}://{bucket}/{loc}" if bucket else f"{scheme}://{loc}"
        etag = getattr(item, "etag", None) or getattr(item, "e_tag", None)
        version = getattr(item, "version", None) or etag
        md5_val = getattr(item, "md5", None) or etag
        updated_val = getattr(item, "last_modified", None)
        return {
            "uri": full_uri,
            "generation": version,
            "content_type": getattr(item, "content_type", None),
            "size": int(getattr(item, "size", 0) or 0),
            "md5_hash": md5_val,
            "updated": str(updated_val) if updated_val is not None else None,
        }


def hydrate_object_refs_sync(
    refs: Sequence[ObjectRef] | Sequence[Mapping[str, Any]],
    store: SyncObjectStore,
    *,
    expires_in_secs: int = 3600,
    fallback_to_authenticated_url: bool = True,
) -> list[ObjectRef]:
    """Hydrate zero-RTT presigned GET URLs onto ObjectRef structs synchronously."""
    hydrated: list[ObjectRef] = []
    for item in refs:
        uri = _extract_item_uri(item)
        bucket, object_key = _split_uri_bucket_and_key(uri)
        signed_url: str | None = None
        if object_key:
            try:
                signed_url = store.presign_get(object_key, expires_in_secs=expires_in_secs)
            except (VertebraError, OSError, ValueError):
                signed_url = None
        hydrated.append(
            _finalize_hydrated_ref(
                item,
                uri,
                bucket,
                object_key,
                signed_url,
                fallback_to_authenticated_url=fallback_to_authenticated_url,
            )
        )
    return hydrated


async def hydrate_object_refs_async(
    refs: Sequence[ObjectRef] | Sequence[Mapping[str, Any]],
    store: AsyncObjectStore,
    *,
    expires_in_secs: int = 3600,
    fallback_to_authenticated_url: bool = True,
) -> list[ObjectRef]:
    """Asynchronously hydrate zero-RTT presigned GET URLs onto ObjectRef structs."""
    hydrated: list[ObjectRef] = []
    for item in refs:
        uri = _extract_item_uri(item)
        bucket, object_key = _split_uri_bucket_and_key(uri)
        signed_url: str | None = None
        if object_key:
            try:
                signed_url = await store.presign_get(object_key, expires_in_secs=expires_in_secs)
            except (VertebraError, OSError, ValueError):
                signed_url = None
        hydrated.append(
            _finalize_hydrated_ref(
                item,
                uri,
                bucket,
                object_key,
                signed_url,
                fallback_to_authenticated_url=fallback_to_authenticated_url,
            )
        )
    return hydrated


def _hydrate_refs_without_store(
    rows: Sequence[Mapping[str, Any]] | Sequence[ObjectRef],
    *,
    fallback_to_authenticated_url: bool,
) -> list[ObjectRef]:
    refs: list[ObjectRef] = []
    for item in rows:
        uri = _extract_item_uri(item)
        bucket, object_key = (
            _split_uri_bucket_and_key(uri) if (fallback_to_authenticated_url and uri.startswith("gs://")) else ("", "")
        )
        signed_url = item.signed_url if isinstance(item, ObjectRef) else None
        refs.append(
            _finalize_hydrated_ref(
                item,
                uri,
                bucket,
                object_key,
                signed_url,
                fallback_to_authenticated_url=fallback_to_authenticated_url,
            )
        )
    return refs


def _extract_item_uri(item: ObjectRef | Mapping[str, Any]) -> str:
    return item.uri if isinstance(item, ObjectRef) else str(item.get("uri") or item.get("object_uri") or "")


def _split_uri_bucket_and_key(uri: str) -> tuple[str, str]:
    if "://" not in uri:
        return "", uri.lstrip("/")
    parsed = urlparse(uri)
    return parsed.netloc.strip("/"), parsed.path.lstrip("/")


def _finalize_hydrated_ref(
    item: ObjectRef | Mapping[str, Any],
    uri: str,
    bucket: str,
    object_key: str,
    signed_url: str | None,
    *,
    fallback_to_authenticated_url: bool,
) -> ObjectRef:
    if signed_url is None and fallback_to_authenticated_url and uri.startswith("gs://") and bucket and object_key:
        signed_url = f"https://storage.cloud.google.com/{bucket}/{object_key}"
    if isinstance(item, ObjectRef):
        return msgspec.structs.replace(item, signed_url=signed_url)
    return _row_to_object_ref(item, signed_url=signed_url)


def _row_to_object_ref(
    row: Mapping[str, Any],
    *,
    signed_url: str | None = None,
) -> ObjectRef:
    uri = str(row.get("uri") or row.get("object_uri") or "")
    version_raw = row.get("generation") or row.get("version")
    version = str(version_raw) if version_raw is not None else None
    content_type_raw = row.get("content_type")
    content_type = str(content_type_raw) if content_type_raw is not None else None
    size_raw = row.get("size", 0)
    size = int(size_raw) if size_raw is not None else 0
    md5_raw = row.get("md5_hash") or row.get("md5") or row.get("md5_hex") or row.get("e_tag")
    md5_hex = str(md5_raw) if md5_raw is not None else None
    updated_raw = row.get("updated") or row.get("last_modified")
    updated = str(updated_raw) if updated_raw is not None else None
    return ObjectRef(
        uri=uri,
        version=version,
        content_type=content_type,
        size=size,
        md5_hex=md5_hex,
        updated=updated,
        signed_url=signed_url,
    )
