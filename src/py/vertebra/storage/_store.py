"""Zero-copy Python object storage facade over vertebra._storage."""

from __future__ import annotations

import base64
import binascii
import builtins
import calendar
import hashlib
import hmac
import io
import shutil
import time
import urllib.request
from collections.abc import Buffer, Sequence
from pathlib import Path
from typing import Any, ClassVar, Literal, Self
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import msgspec

from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "AzureConfig",
    "GcsConfig",
    "HttpConfig",
    "LocalConfig",
    "MemoryConfig",
    "ObjectMeta",
    "ObjectStore",
    "OciConfig",
    "PutResult",
    "R2Config",
    "ReadableFile",
    "S3Config",
    "SigningConfig",
    "StorageConfig",
)


class LocalConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for local filesystem object storage."""

    root: str | Path
    create_dirs: bool = True

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        resolved = Path(self.root).resolve()
        if self.create_dirs:
            resolved.mkdir(parents=True, exist_ok=True)
        return f"file://{resolved.as_posix()}", {}


class S3Config(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for AWS S3 or S3-compatible object storage."""

    bucket: str
    region: str = "us-east-1"
    endpoint: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    virtual_hosted_style: bool = False
    allow_http: bool = False

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        opts: dict[str, str] = {"region": self.region}
        if self.endpoint:
            opts["endpoint"] = self.endpoint
        if self.access_key_id:
            opts["access_key_id"] = self.access_key_id
        if self.secret_access_key:
            opts["secret_access_key"] = self.secret_access_key
        if self.session_token:
            opts["session_token"] = self.session_token
        if self.allow_http:
            opts["allow_http"] = "true"
        return f"s3://{self.bucket}", opts


class GcsConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Google Cloud Storage."""

    bucket: str
    service_account_path: str | None = None
    service_account_key: str | None = None
    application_credentials: str | None = None
    service_account_email: str | None = None
    iam_endpoint: str = "https://iamcredentials.googleapis.com"
    access_token: str | None = None

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        opts: dict[str, str] = {}
        if self.service_account_path:
            opts["service_account_path"] = self.service_account_path
        elif self.application_credentials:
            opts["service_account_path"] = self.application_credentials
        if self.service_account_key:
            opts["service_account_key"] = self.service_account_key
        return f"gs://{self.bucket}", opts


class AzureConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Azure Blob Storage and ADLS Gen2."""

    container: str
    account: str
    access_key: str | None = None
    sas_token: str | None = None
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    endpoint: str | None = None
    use_emulator: bool = False

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        opts: dict[str, str] = {"account": self.account}
        if self.access_key:
            opts["access_key"] = self.access_key
        if self.sas_token:
            opts["sas_token"] = self.sas_token
        if self.tenant_id:
            opts["tenant_id"] = self.tenant_id
        if self.client_id:
            opts["client_id"] = self.client_id
        if self.client_secret:
            opts["client_secret"] = self.client_secret
        if self.endpoint:
            opts["endpoint"] = self.endpoint
        elif self.use_emulator:
            opts["endpoint"] = f"http://127.0.0.1:4577/{self.account}"
        return f"az://{self.container}", opts


class OciConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for native Oracle Cloud Infrastructure (OCI) Object Storage REST v2."""

    bucket: str
    namespace: str
    region: str = "us-ashburn-1"
    tenancy_ocid: str | None = None
    user_ocid: str | None = None
    fingerprint: str | None = None
    private_key_pem: str | None = None
    private_key_path: str | Path | None = None
    endpoint: str | None = None

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        opts: dict[str, str] = {
            "namespace": self.namespace,
            "region": self.region,
        }
        if self.tenancy_ocid:
            opts["tenancy_ocid"] = self.tenancy_ocid
        if self.user_ocid:
            opts["user_ocid"] = self.user_ocid
        if self.fingerprint:
            opts["fingerprint"] = self.fingerprint
        pem = self.private_key_pem
        if pem is None and self.private_key_path is not None:
            pem = Path(self.private_key_path).read_text(encoding="utf-8")
        if pem:
            opts["private_key_pem"] = pem
        if self.endpoint:
            opts["endpoint"] = self.endpoint
        return f"oci://{self.bucket}@{self.namespace}", opts


class R2Config(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Cloudflare R2 object storage."""

    bucket: str
    account_id: str
    access_key_id: str | None = None
    secret_access_key: str | None = None
    jurisdiction: Literal["eu", "fedramp"] | str | None = None
    custom_domain: str | None = None
    endpoint: str | None = None
    allow_http: bool = False

    @staticmethod
    def chunk_uniform_parts(data: Buffer, part_size: int = 8 * 1024 * 1024) -> list[bytes]:
        """Split a payload into uniform-sized parts for Cloudflare R2 multipart uploads."""
        raw = memoryview(data).cast("B").tobytes()
        native_mod = ObjectStore._get_native_module()
        if native_mod is not None and hasattr(native_mod, "py_chunk_r2_uniform_parts"):
            return [bytes(part) for part in native_mod.py_chunk_r2_uniform_parts(raw, part_size)]
        size = max(part_size, 5 * 1024 * 1024)
        if not raw:
            return [b""]
        return [raw[i : i + size] for i in range(0, len(raw), size)]

    def resolved_endpoint(self) -> str:
        """Resolve the Cloudflare R2 jurisdiction or custom endpoint URL."""
        if self.endpoint:
            return self.endpoint.rstrip("/")
        if self.jurisdiction:
            return f"https://{self.account_id}.{self.jurisdiction}.r2.cloudflarestorage.com"
        return f"https://{self.account_id}.r2.cloudflarestorage.com"

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        opts: dict[str, str] = {
            "account_id": self.account_id,
            "endpoint": self.resolved_endpoint(),
        }
        if self.access_key_id:
            opts["access_key_id"] = self.access_key_id
        if self.secret_access_key:
            opts["secret_access_key"] = self.secret_access_key
        if self.jurisdiction:
            opts["jurisdiction"] = str(self.jurisdiction)
        if self.allow_http:
            opts["allow_http"] = "true"
        return f"r2://{self.bucket}", opts


class HttpConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for HTTP/WebDAV object storage."""

    url: str
    headers: dict[str, str] | None = None

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        return self.url, dict(self.headers or {})


class MemoryConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for in-memory object storage."""

    namespace: str = "default"

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        return "memory://", {"namespace": self.namespace}


type StorageConfig = LocalConfig | S3Config | GcsConfig | AzureConfig | OciConfig | R2Config | HttpConfig | MemoryConfig


class ObjectMeta(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Metadata describing an object stored in ObjectStore."""

    path: str
    size: int
    last_modified: str
    etag: str | None = None
    version: str | None = None


class PutResult(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Result metadata returned from a write operation."""

    path: str
    size: int
    etag: str | None = None
    version: str | None = None


class SigningConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
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

    @staticmethod
    def sign_oci_headers(
        tenancy_ocid: str,
        user_ocid: str,
        fingerprint: str,
        private_key_pem: str,
        method: str,
        url: str,
        *,
        body: bytes | None = None,
        content_type: str | None = None,
        timestamp: int = 1_700_000_000,
    ) -> dict[str, str]:
        """Compute OCI draft-cavage-http-signatures-08 RSA-SHA256 signed headers."""
        native_mod = ObjectStore._get_native_module()
        if native_mod is not None and hasattr(native_mod, "py_sign_oci_cavage_headers"):
            return dict(
                native_mod.py_sign_oci_cavage_headers(
                    tenancy_ocid,
                    user_ocid,
                    fingerprint,
                    private_key_pem,
                    method,
                    url,
                    body,
                    content_type,
                    timestamp,
                )
            )
        msg = "vertebra._storage native module is required for RSA-SHA256 OCI request signing"
        raise RuntimeError(msg)

    @staticmethod
    def sign_azure_sas(
        account: str,
        access_key: str,
        container: str,
        blob_name: str,
        *,
        method: str = "GET",
        expires_in_secs: int = 3600,
        now_ts: int | None = None,
        endpoint: str | None = None,
    ) -> str:
        """Compute an Azure Blob Service SAS URL (HMAC-SHA256)."""
        resolved_ts = int(time.time()) if now_ts is None else now_ts
        native_mod = ObjectStore._get_native_module()
        if native_mod is not None and hasattr(native_mod, "py_sign_azure_sas_url"):
            return str(
                native_mod.py_sign_azure_sas_url(
                    account,
                    container,
                    access_key,
                    method,
                    blob_name,
                    expires_in_secs,
                    resolved_ts,
                    endpoint,
                )
            )
        expiry_ts = resolved_ts + expires_in_secs
        se = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expiry_ts))
        sv = "2020-12-06"
        sp = "cw" if method.upper() in {"PUT", "POST"} else "r"
        canonicalized_resource = f"/blob/{account}/{container}/{blob_name.lstrip('/')}"
        string_to_sign = "\n".join(
            [sp, "", se, canonicalized_resource, "", "", "https", sv, "b", "", "", "", "", "", ""]
        )
        decoded_key = base64.b64decode(access_key)
        sig = base64.b64encode(hmac.new(decoded_key, string_to_sign.encode("utf-8"), hashlib.sha256).digest()).decode(
            "ascii"
        )
        qs = urlencode({"sv": sv, "sr": "b", "sp": sp, "se": se, "spr": "https", "sig": sig})
        base_url = (endpoint or f"https://{account}.blob.core.windows.net").rstrip("/")
        return f"{base_url}/{container}/{blob_name.lstrip('/')}?{qs}"


class ObjectStore:
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
            "oci",
            "http",
            "https",
            "r2",
            "b2",
        }:
            try:
                self._native = native_mod.NativeObjectStore(url, self._options or None)
            except (RuntimeError, OSError, ValueError):
                self._native = None

    @property
    def url(self) -> str:
        """Return the configured store base URL."""
        return self._url

    @property
    def scheme(self) -> str:
        """Return the URL scheme for this store."""
        return self._scheme

    @property
    def options(self) -> dict[str, str]:
        """Return a copy of the configured options dictionary."""
        return dict(self._options)

    @classmethod
    def from_config(
        cls,
        config: StorageConfig,
        *,
        signing: SigningConfig | None = None,
    ) -> ObjectStore:
        """Construct an ObjectStore from any provider StorageConfig."""
        url, options = config.to_url_and_options()
        return cls(url, options=options, signing=signing)

    @classmethod
    def from_local(cls, root: str | Path, *, create_dirs: bool = True) -> ObjectStore:
        """Construct an ObjectStore backed by a local filesystem directory."""
        return cls.from_config(LocalConfig(root=root, create_dirs=create_dirs))

    @classmethod
    def from_s3(
        cls,
        bucket: str,
        *,
        region: str = "us-east-1",
        endpoint: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        virtual_hosted_style: bool = False,
        allow_http: bool = False,
        signing: SigningConfig | None = None,
    ) -> ObjectStore:
        """Construct an ObjectStore backed by AWS S3 or an S3-compatible endpoint."""
        cfg = S3Config(
            bucket=bucket,
            region=region,
            endpoint=endpoint,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            virtual_hosted_style=virtual_hosted_style,
            allow_http=allow_http,
        )
        default_signing = signing or (
            SigningConfig(
                provider="s3",
                bucket=bucket,
                region=region,
                access_key_id=access_key_id or "",
                secret_access_key=secret_access_key or "",
                endpoint=endpoint,
            )
            if access_key_id and secret_access_key
            else None
        )
        return cls.from_config(cfg, signing=default_signing)

    @classmethod
    def from_gcs(
        cls,
        bucket: str,
        *,
        service_account_path: str | None = None,
        service_account_key: str | None = None,
        application_credentials: str | None = None,
        service_account_email: str | None = None,
        iam_endpoint: str = "https://iamcredentials.googleapis.com",
        access_token: str | None = None,
    ) -> ObjectStore:
        """Construct an ObjectStore backed by Google Cloud Storage."""
        cfg = GcsConfig(
            bucket=bucket,
            service_account_path=service_account_path,
            service_account_key=service_account_key,
            application_credentials=application_credentials,
            service_account_email=service_account_email,
            iam_endpoint=iam_endpoint,
            access_token=access_token,
        )
        signing = (
            SigningConfig(
                provider="gcs",
                bucket=bucket,
                service_account_email=service_account_email,
                iam_endpoint=iam_endpoint,
                access_token=access_token,
            )
            if service_account_email
            else None
        )
        return cls.from_config(cfg, signing=signing)

    @classmethod
    def from_azure(
        cls,
        container: str,
        account: str,
        *,
        access_key: str | None = None,
        sas_token: str | None = None,
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        endpoint: str | None = None,
        use_emulator: bool = False,
    ) -> ObjectStore:
        """Construct an ObjectStore backed by Azure Blob Storage or ADLS Gen2."""
        return cls.from_config(
            AzureConfig(
                container=container,
                account=account,
                access_key=access_key,
                sas_token=sas_token,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                endpoint=endpoint,
                use_emulator=use_emulator,
            )
        )

    @classmethod
    def from_oci(
        cls,
        bucket: str,
        namespace: str,
        *,
        region: str = "us-ashburn-1",
        tenancy_ocid: str | None = None,
        user_ocid: str | None = None,
        fingerprint: str | None = None,
        private_key_pem: str | None = None,
        private_key_path: str | Path | None = None,
        endpoint: str | None = None,
    ) -> ObjectStore:
        """Construct an ObjectStore backed by native OCI Object Storage REST v2."""
        return cls.from_config(
            OciConfig(
                bucket=bucket,
                namespace=namespace,
                region=region,
                tenancy_ocid=tenancy_ocid,
                user_ocid=user_ocid,
                fingerprint=fingerprint,
                private_key_pem=private_key_pem,
                private_key_path=private_key_path,
                endpoint=endpoint,
            )
        )

    @classmethod
    def from_r2(
        cls,
        bucket: str,
        account_id: str,
        *,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        jurisdiction: Literal["eu", "fedramp"] | str | None = None,
        custom_domain: str | None = None,
        endpoint: str | None = None,
        allow_http: bool = False,
    ) -> ObjectStore:
        """Construct an ObjectStore backed by Cloudflare R2."""
        cfg = R2Config(
            bucket=bucket,
            account_id=account_id,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            jurisdiction=jurisdiction,
            custom_domain=custom_domain,
            endpoint=endpoint,
            allow_http=allow_http,
        )
        signing = (
            SigningConfig(
                provider="s3",
                bucket=bucket,
                region="auto",
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                endpoint=cfg.resolved_endpoint(),
            )
            if access_key_id and secret_access_key
            else None
        )
        return cls.from_config(cfg, signing=signing)

    @classmethod
    def from_http(cls, url: str, *, headers: dict[str, str] | None = None) -> ObjectStore:
        """Construct an ObjectStore backed by HTTP/WebDAV."""
        return cls.from_config(HttpConfig(url=url, headers=headers))

    @classmethod
    def from_memory(cls, namespace: str = "default") -> ObjectStore:
        """Construct an in-memory ObjectStore."""
        return cls.from_config(MemoryConfig(namespace=namespace))

    @staticmethod
    def _native_to_put_result(native_meta: Any, fallback_etag: str = "") -> PutResult:
        """Convert a native NativeObjectMeta instance into a PutResult."""
        etag = getattr(native_meta, "etag", None) or fallback_etag
        return PutResult(
            path=str(native_meta.path),
            size=int(native_meta.size),
            etag=str(etag) if etag else None,
            version=getattr(native_meta, "version", None),
        )

    @staticmethod
    def _native_to_object_meta(native_meta: Any) -> ObjectMeta:
        """Convert a native NativeObjectMeta instance into an ObjectMeta."""
        return ObjectMeta(
            path=str(native_meta.path),
            size=int(native_meta.size),
            last_modified=str(getattr(native_meta, "last_modified", "")),
            etag=getattr(native_meta, "etag", None),
            version=getattr(native_meta, "version", None),
        )

    def put(self, path: str, data: Buffer) -> PutResult:
        """Write any Buffer (bytes, bytearray, memoryview) to the given object key."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        view = memoryview(data).cast("B")
        if self._native is not None:
            native_meta = self._native.put(key, view)
            return self._native_to_put_result(native_meta, hashlib.sha256(view).hexdigest()[:16])
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

    def put_file(self, path: str, file_path: str | Path) -> PutResult:
        """Read a local file and upload it to the given object key."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        resolved_src = Path(file_path)
        if not resolved_src.exists():
            msg = f"Source file not found: {resolved_src}"
            raise FileNotFoundError(msg)
        if self._native is not None and hasattr(self._native, "put_file"):
            native_meta = self._native.put_file(key, str(resolved_src))
            return self._native_to_put_result(native_meta, f"{int(native_meta.size):016x}")
        return self.put(path, resolved_src.read_bytes())

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
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None and hasattr(self._native, "get_buffer"):
            return memoryview(self._native.get_buffer(key))
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
            return self._native_to_object_meta(native_meta)
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
                self._native_to_object_meta(item)
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
        """Asynchronously write a buffer to the store via Rust Tokio awaitable."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        view = memoryview(data).cast("B")
        if self._native is not None and hasattr(self._native, "put_async"):
            native_meta = await self._native.put_async(key, view)
            return self._native_to_put_result(native_meta, hashlib.sha256(view).hexdigest()[:16])
        return self.put(path, data)

    async def put_file_async(self, path: str, file_path: str | Path) -> PutResult:
        """Asynchronously stream a local file into the store via Rust Tokio awaitable."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        resolved_src = Path(file_path)
        if not resolved_src.exists():
            msg = f"Source file not found: {resolved_src}"
            raise FileNotFoundError(msg)
        if self._native is not None and hasattr(self._native, "put_file_async"):
            native_meta = await self._native.put_file_async(key, str(resolved_src))
            return self._native_to_put_result(native_meta, f"{int(native_meta.size):016x}")
        return self.put_file(path, resolved_src)

    async def get_async(self, path: str) -> bytes:
        """Asynchronously read an object from the store via Rust Tokio awaitable."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None and hasattr(self._native, "get_async"):
            return bytes(await self._native.get_async(key))
        return self.get(path)

    async def get_view_async(self, path: str) -> memoryview:
        """Asynchronously read a zero-copy memoryview over an object payload."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None and hasattr(self._native, "get_buffer_async"):
            return memoryview(await self._native.get_buffer_async(key))
        return memoryview(await self.get_async(path))

    async def get_range_async(self, path: str, offset: int, length: int) -> bytes:
        """Asynchronously read a byte range from the store via Rust Tokio awaitable."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None and hasattr(self._native, "get_range_async"):
            return bytes(await self._native.get_range_async(key, offset, length))
        return self.get_range(path, offset, length)

    async def head_async(self, path: str) -> ObjectMeta:
        """Asynchronously read object metadata via Rust Tokio awaitable."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None and hasattr(self._native, "head_async"):
            native_meta = await self._native.head_async(key)
            return self._native_to_object_meta(native_meta)
        return self.head(path)

    async def exists_async(self, path: str) -> bool:
        """Asynchronously check whether an object exists via Rust Tokio awaitable."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None and hasattr(self._native, "exists_async"):
            return bool(await self._native.exists_async(key))
        return self.exists(path)

    async def delete_async(self, path: str) -> None:
        """Asynchronously delete an object via Rust Tokio awaitable."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None and hasattr(self._native, "delete_async"):
            try:
                await self._native.delete_async(key)
            except FileNotFoundError:
                return
            return
        self.delete(path)

    async def copy_async(self, src: str, dst: str) -> None:
        """Asynchronously copy an object within the store via Rust Tokio awaitable."""
        src_key = self._normalize_key(src)
        dst_key = self._normalize_key(dst)
        if self._local_root is not None:
            self._resolve_local(src_key)
            self._resolve_local(dst_key)
        if self._native is not None and hasattr(self._native, "copy_async"):
            await self._native.copy_async(src_key, dst_key)
            return
        self.copy(src, dst)

    async def list_async(self, prefix: str = "") -> builtins.list[ObjectMeta]:
        """Asynchronously list objects matching prefix via Rust Tokio awaitable."""
        norm_prefix = self._normalize_key(prefix) if prefix else ""
        if self._native is not None and hasattr(self._native, "list_async"):
            items = await self._native.list_async(norm_prefix or None)
            return [
                self._native_to_object_meta(item)
                for item in items
                if not norm_prefix or str(item.path).startswith(norm_prefix)
            ]
        return self.list(prefix)

    def open_readable_file(self, path: str, *, size: int | None = None) -> ReadableFile:
        """Open a seekable read-only ReadableFile stream over the given object key."""
        return ReadableFile(self, path, size=size)

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
        if self._signing.provider == "gcs":
            msg = "GCS V4 RSA-SHA256 presigned URL verification is not supported offline"
            raise NotImplementedError(msg)
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
        if not iam_url.startswith(("http:", "https:")):
            msg = f"Unsupported IAM endpoint scheme: {iam_url!r}"
            raise ValueError(msg)
        parsed_iam = urlparse(iam_url)
        body_bytes = bytes(encode_json_bytes({"payload": base64.b64encode(string_to_sign.encode()).decode("ascii")}))
        headers = {"Content-Type": "application/json"}
        if self._signing.access_token:
            headers["Authorization"] = f"Bearer {self._signing.access_token}"
        req = urllib.request.Request(iam_url, data=body_bytes, headers=headers, method="POST")
        opener = (
            urllib.request.build_opener(urllib.request.ProxyHandler({}))
            if parsed_iam.hostname in {"127.0.0.1", "localhost", "::1"}
            else urllib.request.build_opener()
        )
        with opener.open(req, timeout=10) as resp:
            resp_data = decode_json(resp.read())
        signed_blob_str = str(resp_data["signedBlob"])
        try:
            raw_sig = base64.b64decode(signed_blob_str, validate=True)
        except (ValueError, binascii.Error):
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
            cls._native_module = import_optional("vertebra._storage")
            cls._native_loaded = True
        return cls._native_module

    @staticmethod
    def _normalize_key(path: str) -> str:
        cleaned = path.strip().lstrip("/")
        if ".." in Path(cleaned).parts:
            msg = f"Invalid relative parent segment in storage key: {path}"
            raise ValueError(msg)
        return cleaned


class ReadableFile(io.RawIOBase):
    """Seekable read-only file-like object backed by ObjectStore range reads (`get_range`).

    Enables PyArrow, Polars, and DataFusion to inspect Parquet footer metadata and read
    targeted row-group byte slices without downloading full multi-gigabyte blobs.
    """

    __slots__ = ("_closed", "_key", "_pos", "_size", "_store")

    def __init__(self, store: ObjectStore, key: str, *, size: int | None = None) -> None:
        super().__init__()
        self._store = store
        self._key = key
        self._size = store.head(key).size if size is None else size
        self._pos = 0
        self._closed = False

    @property
    def size(self) -> int:
        """Return total object byte size."""
        return self._size

    @property
    def closed(self) -> bool:
        """Return True if the file handle has been closed."""
        return self._closed

    def __enter__(self) -> Self:
        """Enter the context manager and return self."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the handle when exiting the context manager."""
        _ = (exc_type, exc_val, exc_tb)
        self.close()

    def readable(self) -> bool:
        """Return True indicating this stream supports reading."""
        return True

    def seekable(self) -> bool:
        """Return True indicating this stream supports random access seeking."""
        return True

    def writable(self) -> bool:
        """Return False indicating this stream is read-only."""
        return False

    def tell(self) -> int:
        """Return the current cursor byte offset."""
        self._ensure_open()
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move the read cursor to a new byte offset and return the updated position."""
        self._ensure_open()
        if whence == io.SEEK_SET:
            target = offset
        elif whence == io.SEEK_CUR:
            target = self._pos + offset
        elif whence == io.SEEK_END:
            target = self._size + offset
        else:
            msg = f"Invalid whence value: {whence}"
            raise ValueError(msg)
        if target < 0:
            msg = f"Negative seek position: {target}"
            raise ValueError(msg)
        self._pos = min(target, self._size)
        return self._pos

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes starting at the current cursor position."""
        self._ensure_open()
        if self._pos >= self._size:
            return b""
        remaining = self._size - self._pos
        length = remaining if size < 0 else min(size, remaining)
        if length <= 0:
            return b""
        data = self._store.get_range(self._key, self._pos, length)
        self._pos += len(data)
        return data

    def read_at(self, offset: int, length: int) -> bytes:
        """Read length bytes at offset without mutating the stream cursor."""
        self._ensure_open()
        if offset < 0 or length < 0:
            msg = "offset and length must be non-negative"
            raise ValueError(msg)
        if offset >= self._size or length == 0:
            return b""
        bounded_len = min(length, self._size - offset)
        return self._store.get_range(self._key, offset, bounded_len)

    async def read_at_async(self, offset: int, length: int) -> bytes:
        """Asynchronously read length bytes at offset without mutating the stream cursor."""
        self._ensure_open()
        if offset < 0 or length < 0:
            msg = "offset and length must be non-negative"
            raise ValueError(msg)
        if offset >= self._size or length == 0:
            return b""
        bounded_len = min(length, self._size - offset)
        return await self._store.get_range_async(self._key, offset, bounded_len)

    def close(self) -> None:
        """Mark the file handle as closed."""
        self._closed = True
        super().close()

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "I/O operation on closed ReadableFile"
            raise ValueError(msg)
