"""Zero-copy Python object storage facade over vertebra._storage."""

from __future__ import annotations

import asyncio
import base64
import builtins
import calendar
import hashlib
import hmac
import importlib
import json
import shutil
import time
import urllib.request
from collections.abc import Buffer, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Literal
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

__all__ = (
    "ObjectMeta",
    "ObjectStore",
    "PutResult",
    "SigningConfig",
    "VertebraStore",
)


@dataclass(frozen=True, slots=True)
class ObjectMeta:
    """Metadata describing an object stored in VertebraStore."""

    path: str
    size: int
    last_modified: str
    etag: str | None = None
    version: str | None = None


@dataclass(frozen=True, slots=True)
class PutResult:
    """Result metadata returned from a write operation."""

    path: str
    size: int
    etag: str | None = None
    version: str | None = None


@dataclass(frozen=True, slots=True)
class SigningConfig:
    """Configuration for HMAC-SHA256, S3 SigV4, and GCS V4 presigned URL generation."""

    provider: Literal["hmac", "s3", "gcs"] = "hmac"
    bucket: str = ""
    region: str = "us-east-1"
    access_key_id: str = ""
    secret_access_key: str = ""
    hmac_secret: str = ""
    service_account_email: str = ""
    iam_endpoint: str = "https://iamcredentials.googleapis.com"
    access_token: str | None = None
    endpoint: str | None = None


class VertebraStore:
    """Unified object store supporting zero-copy memoryview buffers, sync/async I/O, and V4 presigning."""

    _native_module: ClassVar[Any] = None
    _native_loaded: ClassVar[bool] = False
    __slots__ = ("_local_root", "_memory", "_native", "_options", "_scheme", "_signing", "_url")

    def __init__(
        self,
        url: str = "memory://",
        *,
        options: dict[str, str] | None = None,
        signing: SigningConfig | None = None,
    ) -> None:
        self._url = url
        self._options = dict(options or {})
        self._signing = signing
        parsed = urlparse(url)
        self._scheme = parsed.scheme or "memory"
        self._memory: dict[str, tuple[bytes, ObjectMeta]] = {}
        self._local_root: Path | None = None
        if self._scheme == "file":
            root_path = Path(parsed.netloc + parsed.path if parsed.netloc else parsed.path)
            root_path.mkdir(parents=True, exist_ok=True)
            self._local_root = root_path

        native_mod = self._get_native_module()
        self._native: Any = None
        if native_mod is not None and self._scheme in {
            "memory",
            "file",
            "s3",
            "gs",
            "gcs",
            "az",
            "azure",
            "abfs",
            "http",
            "https",
            "r2",
            "b2",
        }:
            try:
                self._native = native_mod.NativeObjectStore(url, self._options or None)
            except Exception:
                self._native = None

    @property
    def url(self) -> str:
        """Return the configured store base URL."""
        return self._url

    @property
    def scheme(self) -> str:
        """Return the URL scheme for this store."""
        return self._scheme

    def put(self, path: str, data: Buffer) -> PutResult:
        """Write any Buffer (bytes, bytearray, memoryview) to the given object key."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        view = memoryview(data).cast("B")
        if self._native is not None:
            native_meta = self._native.put(key, view)
            etag = native_meta.etag or hashlib.sha256(view).hexdigest()[:16]
            return PutResult(
                path=str(native_meta.path),
                size=int(native_meta.size),
                etag=str(etag),
                version=native_meta.version,
            )
        digest = hashlib.sha256(view).hexdigest()[:16]
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        if self._local_root is not None:
            target = self._resolve_local(key)
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("wb") as handle:
                handle.write(view)
            return PutResult(path=key, size=view.nbytes, etag=digest)
        raw = view.tobytes()
        meta = ObjectMeta(path=key, size=len(raw), last_modified=timestamp, etag=digest)
        self._memory[key] = (raw, meta)
        return PutResult(path=key, size=len(raw), etag=digest)

    def put_multipart(self, path: str, parts: Sequence[Buffer]) -> PutResult:
        """Concatenate and upload multiple buffer parts atomically."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None:
            views = [memoryview(part).cast("B") for part in parts]
            native_meta = self._native.put_multipart(key, views)
            etag = native_meta.etag or f"{int(native_meta.size):016x}"
            return PutResult(
                path=str(native_meta.path),
                size=int(native_meta.size),
                etag=str(etag),
                version=native_meta.version,
            )
        combined = bytearray()
        for part in parts:
            combined.extend(memoryview(part).cast("B"))
        return self.put(path, memoryview(combined))

    def get(self, path: str) -> bytes:
        """Read full object bytes for the given key."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None:
            return bytes(self._native.get(key))
        if self._local_root is not None:
            target = self._resolve_local(key)
            if not target.exists():
                msg = f"Object not found: {key}"
                raise FileNotFoundError(msg)
            return target.read_bytes()
        entry = self._memory.get(key)
        if entry is None:
            msg = f"Object not found: {key}"
            raise FileNotFoundError(msg)
        return entry[0]

    def get_view(self, path: str) -> memoryview:
        """Return a zero-copy memoryview over the object payload."""
        return memoryview(self.get(path))

    def get_range(self, path: str, offset: int, length: int) -> bytes:
        """Read a byte slice of length bytes starting at offset."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None:
            return bytes(self._native.get_range(key, offset, length))
        view = self.get_view(path)
        return bytes(view[offset : offset + length])

    def head(self, path: str) -> ObjectMeta:
        """Return object metadata without transferring the full payload."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None:
            native_meta = self._native.head(key)
            return ObjectMeta(
                path=str(native_meta.path),
                size=int(native_meta.size),
                last_modified=str(native_meta.last_modified),
                etag=native_meta.etag,
                version=native_meta.version,
            )
        if self._local_root is not None:
            target = self._resolve_local(key)
            if not target.exists():
                msg = f"Object not found: {key}"
                raise FileNotFoundError(msg)
            stat = target.stat()
            timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stat.st_mtime))
            return ObjectMeta(path=key, size=stat.st_size, last_modified=timestamp)
        entry = self._memory.get(key)
        if entry is None:
            msg = f"Object not found: {key}"
            raise FileNotFoundError(msg)
        return entry[1]

    def exists(self, path: str) -> bool:
        """Return True if the object key exists."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None:
            return bool(self._native.exists(key))
        if self._local_root is not None:
            return self._resolve_local(key).exists()
        return key in self._memory

    def delete(self, path: str) -> None:
        """Delete the object at the given key."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None:
            try:
                self._native.delete(key)
            except FileNotFoundError:
                return
            return
        if self._local_root is not None:
            target = self._resolve_local(key)
            if target.exists():
                target.unlink()
            return
        self._memory.pop(key, None)

    def copy(self, src: str, dst: str) -> None:
        """Copy an object within the store from src to dst."""
        src_key = self._normalize_key(src)
        dst_key = self._normalize_key(dst)
        if self._local_root is not None:
            self._resolve_local(src_key)
            self._resolve_local(dst_key)
        if self._native is not None:
            self._native.copy(src_key, dst_key)
            return
        if self._local_root is not None:
            src_path = self._resolve_local(src_key)
            dst_path = self._resolve_local(dst_key)
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dst_path)
            return
        payload = self.get(src_key)
        self.put(dst_key, payload)

    def list(self, prefix: str = "") -> builtins.list[ObjectMeta]:
        """List all objects under the given key prefix sorted by path."""
        norm_prefix = self._normalize_key(prefix) if prefix else ""
        if self._native is not None:
            items = self._native.list(norm_prefix or None)
            return [
                ObjectMeta(
                    path=str(item.path),
                    size=int(item.size),
                    last_modified=str(item.last_modified),
                    etag=item.etag,
                    version=item.version,
                )
                for item in items
                if not norm_prefix or str(item.path).startswith(norm_prefix)
            ]
        if self._local_root is not None:
            results: builtins.list[ObjectMeta] = []
            for file_path in sorted(self._local_root.rglob("*")):
                if not file_path.is_file():
                    continue
                rel = file_path.relative_to(self._local_root).as_posix()
                if not norm_prefix or rel.startswith(norm_prefix):
                    results.append(self.head(rel))
            return results
        return [
            meta for key, (_, meta) in sorted(self._memory.items()) if not norm_prefix or key.startswith(norm_prefix)
        ]

    async def put_async(self, path: str, data: Buffer) -> PutResult:
        """Asynchronously write a buffer to the store."""
        return await asyncio.to_thread(self.put, path, data)

    async def get_async(self, path: str) -> bytes:
        """Asynchronously read an object from the store."""
        return await asyncio.to_thread(self.get, path)

    async def get_range_async(self, path: str, offset: int, length: int) -> bytes:
        """Asynchronously read a byte range from the store."""
        return await asyncio.to_thread(self.get_range, path, offset, length)

    async def head_async(self, path: str) -> ObjectMeta:
        """Asynchronously read object metadata."""
        return await asyncio.to_thread(self.head, path)

    async def exists_async(self, path: str) -> bool:
        """Asynchronously check whether an object exists."""
        return await asyncio.to_thread(self.exists, path)

    async def delete_async(self, path: str) -> None:
        """Asynchronously delete an object."""
        await asyncio.to_thread(self.delete, path)

    async def list_async(self, prefix: str = "") -> builtins.list[ObjectMeta]:
        """Asynchronously list objects matching prefix."""
        return await asyncio.to_thread(self.list, prefix)

    def presign_get(
        self,
        path: str,
        *,
        expires_in_secs: int = 3600,
        timestamp: int | None = None,
    ) -> str:
        """Generate a presigned GET URL for the given object key."""
        return self._presign(path, method="GET", expires_in_secs=expires_in_secs, timestamp=timestamp)

    def presign_put(
        self,
        path: str,
        *,
        expires_in_secs: int = 3600,
        timestamp: int | None = None,
    ) -> str:
        """Generate a presigned PUT URL for the given object key."""
        return self._presign(path, method="PUT", expires_in_secs=expires_in_secs, timestamp=timestamp)

    def verify_presigned_url(
        self,
        signed_url: str,
        *,
        method: str = "GET",
        timestamp: int | None = None,
    ) -> bool:
        """Verify an HMAC-SHA256 or S3 SigV4 presigned URL signature and expiration."""
        if self._signing is None:
            msg = "SigningConfig is required to verify presigned URLs"
            raise RuntimeError(msg)
        now_ts = int(time.time()) if timestamp is None else timestamp
        if self._signing.provider == "s3":
            return self._verify_s3_v4(signed_url, method=method, now_ts=now_ts)
        return self._verify_hmac(signed_url, method=method, now_ts=now_ts)

    def _presign(
        self,
        path: str,
        *,
        method: str,
        expires_in_secs: int,
        timestamp: int | None,
    ) -> str:
        if self._signing is None:
            msg = "SigningConfig is required for presigned URL generation"
            raise RuntimeError(msg)
        now_ts = int(time.time()) if timestamp is None else timestamp
        key = self._normalize_key(path)
        if self._signing.provider == "gcs":
            return self._sign_gcs_v4_iam(key, method=method, expires_in_secs=expires_in_secs, now_ts=now_ts)
        if self._signing.provider == "s3":
            return self._sign_s3_v4(key, method=method, expires_in_secs=expires_in_secs, now_ts=now_ts)
        return self._sign_hmac(key, method=method, expires_in_secs=expires_in_secs, now_ts=now_ts)

    def _sign_hmac(
        self,
        key: str,
        *,
        method: str,
        expires_in_secs: int,
        now_ts: int,
    ) -> str:
        assert self._signing is not None
        base = (self._signing.endpoint or "https://storage.vertebra.local").rstrip("/")
        native_mod = self._get_native_module()
        if native_mod is not None:
            return str(
                native_mod.py_sign_hmac_url(
                    f"{base}/{key}",
                    self._signing.hmac_secret,
                    method,
                    expires_in_secs,
                    now_ts,
                )
            )
        expires_at = now_ts + expires_in_secs
        url_path = f"/{key}"
        canonical = f"{method.upper()}\n{url_path}\n{expires_at}".encode()
        sig = hmac.new(self._signing.hmac_secret.encode(), canonical, hashlib.sha256).hexdigest()
        query = urlencode(
            {
                "X-Vtb-Expires": str(expires_at),
                "X-Vtb-Method": method.upper(),
                "X-Vtb-Signature": sig,
            }
        )
        return f"{base}{url_path}?{query}"

    def _verify_hmac(self, signed_url: str, *, method: str, now_ts: int) -> bool:
        assert self._signing is not None
        native_mod = self._get_native_module()
        if native_mod is not None:
            return bool(
                native_mod.py_verify_hmac_url(
                    signed_url,
                    self._signing.hmac_secret,
                    method,
                    now_ts,
                )
            )
        parsed = urlparse(signed_url)
        params = parse_qs(parsed.query)
        try:
            expires_at = int(params["X-Vtb-Expires"][0])
            signed_method = params["X-Vtb-Method"][0]
            signature = params["X-Vtb-Signature"][0]
        except (KeyError, IndexError, ValueError):
            return False
        if now_ts > expires_at or signed_method.upper() != method.upper():
            return False
        canonical = f"{signed_method.upper()}\n{parsed.path}\n{expires_at}".encode()
        expected = hmac.new(self._signing.hmac_secret.encode(), canonical, hashlib.sha256).hexdigest()
        return hmac.compare_digest(signature, expected)

    def _sign_s3_v4(
        self,
        key: str,
        *,
        method: str,
        expires_in_secs: int,
        now_ts: int,
    ) -> str:
        assert self._signing is not None
        native_mod = self._get_native_module()
        if native_mod is not None:
            return str(
                native_mod.py_sign_s3_v4_url(
                    self._signing.bucket,
                    self._signing.region,
                    self._signing.access_key_id,
                    self._signing.secret_access_key,
                    method,
                    key,
                    expires_in_secs,
                    now_ts,
                    self._signing.endpoint,
                )
            )
        amz_date = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now_ts))
        datestamp = time.strftime("%Y%m%d", time.gmtime(now_ts))
        endpoint = (self._signing.endpoint or f"https://s3.{self._signing.region}.amazonaws.com").rstrip("/")
        full_url = f"{endpoint}/{self._signing.bucket}/{key}"
        parsed = urlparse(full_url)
        host = parsed.netloc
        scope = f"{datestamp}/{self._signing.region}/s3/aws4_request"
        query_items = [
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            ("X-Amz-Credential", f"{self._signing.access_key_id}/{scope}"),
            ("X-Amz-Date", amz_date),
            ("X-Amz-Expires", str(expires_in_secs)),
            ("X-Amz-SignedHeaders", "host"),
        ]
        canonical_qs = urlencode(query_items)
        canonical_req = f"{method.upper()}\n{parsed.path}\n{canonical_qs}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD"
        string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hashlib.sha256(canonical_req.encode()).hexdigest()}"
        k_date = hmac.new(
            f"AWS4{self._signing.secret_access_key}".encode(),
            datestamp.encode(),
            hashlib.sha256,
        ).digest()
        k_region = hmac.new(k_date, self._signing.region.encode(), hashlib.sha256).digest()
        k_service = hmac.new(k_region, b"s3", hashlib.sha256).digest()
        k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
        signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()
        return urlunparse(parsed._replace(query=f"{canonical_qs}&X-Amz-Signature={signature}"))

    def _verify_s3_v4(self, signed_url: str, *, method: str, now_ts: int) -> bool:
        assert self._signing is not None
        native_mod = self._get_native_module()
        if native_mod is not None:
            return bool(
                native_mod.py_verify_s3_v4_url(
                    self._signing.bucket,
                    self._signing.region,
                    self._signing.access_key_id,
                    self._signing.secret_access_key,
                    method,
                    signed_url,
                    now_ts,
                    self._signing.endpoint,
                )
            )
        parsed = urlparse(signed_url)
        params = parse_qs(parsed.query)
        try:
            amz_date = params["X-Amz-Date"][0]
            expires_secs = int(params["X-Amz-Expires"][0])
            signature = params["X-Amz-Signature"][0]
        except (KeyError, IndexError, ValueError):
            return False

        signed_ts = int(calendar.timegm(time.strptime(amz_date, "%Y%m%dT%H%M%SZ")))
        if now_ts > signed_ts + expires_secs:
            return False
        prefix = f"/{self._signing.bucket}/"
        key = parsed.path[len(prefix) :] if parsed.path.startswith(prefix) else parsed.path.lstrip("/")
        expected_url = self._sign_s3_v4(
            key,
            method=method,
            expires_in_secs=expires_secs,
            now_ts=signed_ts,
        )
        expected_sig = parse_qs(urlparse(expected_url).query)["X-Amz-Signature"][0]
        return hmac.compare_digest(signature, expected_sig)

    def _sign_gcs_v4_iam(
        self,
        key: str,
        *,
        method: str,
        expires_in_secs: int,
        now_ts: int,
    ) -> str:
        assert self._signing is not None
        native_mod = self._get_native_module()
        if native_mod is not None:
            return str(
                native_mod.py_sign_gcs_v4_iam_url(
                    self._signing.bucket,
                    self._signing.service_account_email,
                    method,
                    key,
                    expires_in_secs,
                    now_ts,
                    self._signing.iam_endpoint,
                    self._signing.access_token,
                )
            )
        goog_date = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now_ts))
        datestamp = time.strftime("%Y%m%d", time.gmtime(now_ts))
        canonical_uri = f"/{self._signing.bucket}/{key}"
        scope = f"{datestamp}/auto/storage/goog4_request"
        query_items = [
            ("X-Goog-Algorithm", "GOOG4-RSA-SHA256"),
            ("X-Goog-Credential", f"{self._signing.service_account_email}/{scope}"),
            ("X-Goog-Date", goog_date),
            ("X-Goog-Expires", str(expires_in_secs)),
            ("X-Goog-SignedHeaders", "host"),
        ]
        canonical_qs = urlencode(query_items)
        canonical_req = (
            f"{method.upper()}\n{canonical_uri}\n{canonical_qs}\nhost:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD"
        )
        string_to_sign = f"GOOG4-RSA-SHA256\n{goog_date}\n{scope}\n{hashlib.sha256(canonical_req.encode()).hexdigest()}"
        iam_url = (
            f"{self._signing.iam_endpoint.rstrip('/')}/v1/projects/-/serviceAccounts/"
            f"{self._signing.service_account_email}:signBlob"
        )
        body_bytes = json.dumps({"payload": base64.b64encode(string_to_sign.encode()).decode("ascii")}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self._signing.access_token:
            headers["Authorization"] = f"Bearer {self._signing.access_token}"
        req = urllib.request.Request(iam_url, data=body_bytes, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp_data = json.loads(resp.read().decode("utf-8"))
        signed_blob_str = str(resp_data["signedBlob"])
        try:
            raw_sig = base64.b64decode(signed_blob_str, validate=True)
        except Exception:
            raw_sig = signed_blob_str.encode("utf-8")
        hex_sig = raw_sig.hex()
        return f"https://storage.googleapis.com{canonical_uri}?{canonical_qs}&X-Goog-Signature={hex_sig}"

    def _resolve_local(self, key: str) -> Path:
        assert self._local_root is not None
        resolved = (self._local_root / key).resolve()
        root_resolved = self._local_root.resolve()
        if root_resolved not in resolved.parents and resolved != root_resolved:
            msg = f"Path traversal outside storage root rejected: {key}"
            raise ValueError(msg)
        return resolved

    @classmethod
    def _get_native_module(cls) -> Any:
        if not cls._native_loaded:
            for mod_name in ("vertebra._storage", "_storage"):
                try:
                    cls._native_module = importlib.import_module(mod_name)
                    break
                except ImportError:
                    continue
            cls._native_loaded = True
        return cls._native_module

    @staticmethod
    def _normalize_key(path: str) -> str:
        cleaned = path.strip().lstrip("/")
        if ".." in Path(cleaned).parts:
            msg = f"Invalid relative parent segment in storage key: {path}"
            raise ValueError(msg)
        return cleaned


ObjectStore = VertebraStore
