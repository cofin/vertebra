"""Zero-copy Python object storage facade over vertebra._storage."""

from __future__ import annotations

import contextlib
import fnmatch
import io
import os
import re
import shutil
import tempfile
import time
import warnings
from collections.abc import AsyncIterable, AsyncIterator, Buffer, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Literal, Self
from urllib.parse import unquote, urlparse

import msgspec

from vertebra._vertebra import AsyncGcpTokenCache, GcpTokenSnapshot, GoogleCloudAuthError, SyncGcpTokenCache
from vertebra.storage import _arrow
from vertebra.typing import ImproperConfigurationError, MissingDependencyError, PreconditionError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json

if TYPE_CHECKING:
    from vertebra._storage import StorageBuffer

__all__ = (
    "AsyncGcpTokenCache",
    "AsyncObjectStore",
    "AsyncReadableFile",
    "AsyncWritableFile",
    "AzureConfig",
    "GCSConfig",
    "GcpTokenSnapshot",
    "HTTPConfig",
    "LocalConfig",
    "MemoryConfig",
    "OCIConfig",
    "ObjectMeta",
    "PreconditionError",
    "PutResult",
    "R2Config",
    "S3Config",
    "SigningConfig",
    "StorageConfig",
    "SyncGcpTokenCache",
    "SyncObjectStore",
    "SyncReadableFile",
    "SyncWritableFile",
)

GCS_DEFAULT_SCOPES: Final[tuple[str, str]] = (
    "https://www.googleapis.com/auth/devstorage.full_control",
    "https://www.googleapis.com/auth/cloud-platform",
)

_SUPPORTED_SCHEMES: Final[frozenset[str]] = frozenset(
    {
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
    }
)

_OCI_PAR_PREFIX_RE: Final[re.Pattern[str]] = re.compile(r"^(https?://[^/]+)/p/([^/]+)/n/([^/]+)/b/([^/]+)/o/?$")
_GLOB_WILDCARD_RE: Final[re.Pattern[str]] = re.compile(r"[*?\[]")
_SHM_STAGING_DIR: Final[Path] = Path("/dev/shm")


class LocalConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for local filesystem object storage."""

    root: str | Path
    """Root directory on the local filesystem under which object keys are resolved."""
    create_dirs: bool = True
    """When True, automatically create `root` and parent directories on initialization and writes."""

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        resolved = Path(self.root).resolve()
        if self.create_dirs:
            resolved.mkdir(parents=True, exist_ok=True)
        return f"file://{resolved.as_posix()}", {}


class S3Config(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for AWS S3 or S3-compatible object storage."""

    bucket: str
    """Target S3 bucket name."""
    region: str = "us-east-1"
    """AWS region used for SigV4 request signing and endpoint resolution."""
    endpoint: str | None = None
    """Optional custom S3-compatible endpoint URL (e.g. MinIO, LocalStack, or Ceph RGW)."""
    access_key_id: str | None = None
    """Optional AWS IAM access key ID."""
    secret_access_key: str | None = None
    """Optional AWS IAM secret access key."""
    session_token: str | None = None
    """Optional AWS STS temporary session token."""
    virtual_hosted_style: bool = False
    """When True, address buckets using virtual-hosted-style URLs (`<bucket>.s3.<region>.amazonaws.com`)."""
    allow_http: bool = False
    """When True, permit unencrypted `http://` endpoints for local emulators."""

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
        if self.virtual_hosted_style:
            opts["virtual_hosted_style"] = "true"
        if self.allow_http:
            opts["allow_http"] = "true"
        return f"s3://{self.bucket}", opts


class GCSConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Google Cloud Storage with Application Default Credentials (ADC) support."""

    bucket: str
    """Target Google Cloud Storage bucket name."""
    service_account_path: str | None = None
    """Optional filesystem path to a Google Cloud service account JSON key file."""
    service_account_key: str | None = None
    """Optional inline JSON string containing a Google Cloud service account key."""
    application_credentials: str | None = None
    """Optional path to an Application Default Credentials JSON file (`authorized_user`, `external_account`, or `service_account`)."""
    service_account_email: str | None = None
    """Optional service account email used for IAM `signBlob` V4 URL signing."""
    target_service_account: str | None = None
    """Optional target service account email for keyless IAM impersonation (`generateAccessToken` / `signBlob`)."""
    iam_endpoint: str = "https://iamcredentials.googleapis.com"
    """Base URL for the Google Cloud IAM Credentials REST API."""
    access_token: str | None = None
    """Optional OAuth2 bearer access token used for GCS REST and IAM `signBlob` calls."""
    endpoint: str | None = None
    """Optional custom GCS endpoint URL (or automatically resolved from `STORAGE_EMULATOR_HOST`)."""
    allow_http: bool = False
    """When True, permit unencrypted `http://` endpoints for local emulators."""
    skip_signature: bool = False
    """When True, send unsigned requests against anonymous buckets or local emulators."""
    refresh_skew_seconds: int = 300
    """Proactive OAuth2 token refresh skew window in seconds before expiration."""
    quota_project_id: str | None = None
    """Optional Google Cloud quota project ID forwarded as `x-goog-user-project`."""

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        opts: dict[str, str] = {
            "refresh_skew_ms": str(max(0, int(self.refresh_skew_seconds * 1000))),
            "iam_endpoint": self.iam_endpoint,
        }
        if self.service_account_path:
            opts["service_account_path"] = self.service_account_path
        if self.application_credentials:
            opts["application_credentials_path"] = self.application_credentials
        if self.service_account_key:
            opts["service_account_key"] = self.service_account_key
        if self.service_account_email:
            opts["service_account_email"] = self.service_account_email
        if self.target_service_account:
            opts["target_service_account"] = self.target_service_account
        if self.quota_project_id:
            opts["quota_project_id"] = self.quota_project_id
        if self.access_token:
            opts["access_token"] = self.access_token

        emulator_host = os.environ.get("STORAGE_EMULATOR_HOST", "").strip()
        resolved_endpoint = self.endpoint
        if resolved_endpoint is None and emulator_host:
            resolved_endpoint = (
                emulator_host if emulator_host.startswith(("http://", "https://")) else f"http://{emulator_host}"
            )
        if resolved_endpoint:
            opts["endpoint"] = resolved_endpoint

        if self.allow_http or (resolved_endpoint is not None and resolved_endpoint.startswith("http://")):
            opts["allow_http"] = "true"

        has_explicit_auth = bool(
            self.service_account_path
            or self.service_account_key
            or self.application_credentials
            or self.access_token
            or self.target_service_account
        )
        if self.skip_signature or (bool(emulator_host) and self.endpoint is None and not has_explicit_auth):
            opts["skip_signature"] = "true"

        return f"gs://{self.bucket}", opts

    def extract_service_account_identity(self, *, allow_ambient: bool = True) -> tuple[str | None, str | None]:
        """Extract `(client_email, private_key_pem)` when service account JSON material is configured."""
        raw_json: str | bytes | None = None
        if self.service_account_key:
            raw_json = self.service_account_key
        elif self.service_account_path:
            sa_path = Path(self.service_account_path)
            if sa_path.is_file():
                raw_json = sa_path.read_bytes()
        else:
            adc_path = self._resolve_adc_file(allow_ambient=allow_ambient)
            if adc_path is not None:
                try:
                    raw_json = adc_path.read_bytes()
                except OSError:
                    raw_json = None

        if raw_json is not None:
            try:
                payload = decode_json(raw_json)
            except (ValueError, TypeError, msgspec.DecodeError):
                payload = None
            if isinstance(payload, dict) and payload.get("type") in {None, "service_account"}:
                email = payload.get("client_email")
                pem = payload.get("private_key")
                resolved_email = (
                    self.target_service_account or self.service_account_email or (str(email) if email else None)
                )
                resolved_pem = None if self.target_service_account else (str(pem) if pem else None)
                return resolved_email, resolved_pem

        return self.target_service_account or self.service_account_email, None

    def to_token_cache_options(
        self,
        *,
        allow_ambient_adc: bool = True,
        scopes: Sequence[str] | None = None,
    ) -> dict[str, str]:
        """Build native GcpTokenCache option dictionary for vertebra._vertebra token caches."""
        opts: dict[str, str] = {
            "refresh_skew_ms": str(max(0, int(self.refresh_skew_seconds * 1000))),
            "iam_endpoint": self.iam_endpoint,
            "allow_ambient_adc": "true" if allow_ambient_adc else "false",
        }
        if self.service_account_path:
            opts["service_account_path"] = self.service_account_path
        if self.service_account_key:
            opts["service_account_key"] = self.service_account_key
        if self.application_credentials:
            opts["application_credentials_path"] = self.application_credentials
        if self.target_service_account:
            opts["target_service_account"] = self.target_service_account
        if self.access_token:
            opts["access_token"] = self.access_token
        if self.quota_project_id:
            opts["quota_project_id"] = self.quota_project_id
        if scopes:
            opts["scopes"] = ",".join(scopes)
        has_explicit_creds = bool(
            self.application_credentials or self.service_account_path or self.service_account_key or self.access_token
        )
        has_ambient_env_token = bool(
            os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN", "").strip()
            or os.environ.get("CLOUDSDK_AUTH_ACCESS_TOKEN", "").strip()
        )
        if (
            allow_ambient_adc
            and not has_explicit_creds
            and not self.target_service_account
            and self._resolve_adc_file(allow_ambient=True) is None
            and not has_ambient_env_token
        ):
            opts["use_metadata_server"] = "true"
        return opts

    def sync_token_cache(
        self,
        *,
        allow_ambient_adc: bool = True,
        scopes: Sequence[str] = GCS_DEFAULT_SCOPES,
    ) -> SyncGcpTokenCache:
        """Construct a unified vertebra._vertebra.SyncGcpTokenCache for this GCSConfig."""
        opts = self.to_token_cache_options(allow_ambient_adc=allow_ambient_adc, scopes=scopes)
        return SyncGcpTokenCache(opts, list(scopes))

    def async_token_cache(
        self,
        *,
        allow_ambient_adc: bool = True,
        scopes: Sequence[str] = GCS_DEFAULT_SCOPES,
    ) -> AsyncGcpTokenCache:
        """Construct a unified vertebra._vertebra.AsyncGcpTokenCache for this GCSConfig."""
        opts = self.to_token_cache_options(allow_ambient_adc=allow_ambient_adc, scopes=scopes)
        return AsyncGcpTokenCache(opts, list(scopes))

    def resolve_adc_token(
        self,
        *,
        scopes: Sequence[str] = GCS_DEFAULT_SCOPES,
        allow_ambient_adc: bool = True,
    ) -> str | None:
        """Resolve an OAuth2 bearer token via vertebra._vertebra.SyncGcpTokenCache."""
        try:
            cache = self.sync_token_cache(allow_ambient_adc=allow_ambient_adc, scopes=scopes)
            return cache.get_token().access_token
        except (GoogleCloudAuthError, RuntimeError, ValueError, OSError):
            return None

    async def resolve_adc_token_async(
        self,
        *,
        scopes: Sequence[str] = GCS_DEFAULT_SCOPES,
        allow_ambient_adc: bool = True,
    ) -> str | None:
        """Asynchronously resolve an OAuth2 bearer token via vertebra._vertebra.AsyncGcpTokenCache."""
        try:
            cache = self.async_token_cache(allow_ambient_adc=allow_ambient_adc, scopes=scopes)
            snap = await cache.get_token()
            return snap.access_token
        except (GoogleCloudAuthError, RuntimeError, ValueError, OSError):
            return None

    def _resolve_adc_file(self, *, allow_ambient: bool = True) -> Path | None:
        """Locate an explicit or ambient Application Default Credentials JSON file."""
        candidates: list[str] = []
        if self.application_credentials:
            candidates.append(self.application_credentials)
        if allow_ambient:
            env_adc = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
            if env_adc:
                candidates.append(env_adc)
            xdg_cfg = os.environ.get("CLOUDSDK_CONFIG", "").strip()
            if xdg_cfg:
                candidates.append(str(Path(xdg_cfg) / "application_default_credentials.json"))
            else:
                appdata_cfg = os.environ.get("APPDATA", "").strip()
                if appdata_cfg:
                    candidates.append(str(Path(appdata_cfg) / "gcloud" / "application_default_credentials.json"))
                candidates.append(str(Path.home() / ".config" / "gcloud" / "application_default_credentials.json"))

        for raw in candidates:
            candidate = Path(raw)
            if candidate.is_file():
                return candidate
        return None


class AzureConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Azure Blob Storage and ADLS Gen2."""

    container: str
    """Target Azure Blob Storage container or ADLS Gen2 filesystem name."""
    account: str
    """Azure Storage account name."""
    access_key: str | None = None
    """Optional Base64-encoded Azure Storage account shared key."""
    sas_token: str | None = None
    """Optional pre-generated Shared Access Signature (SAS) query string."""
    tenant_id: str | None = None
    """Optional Microsoft Entra ID (Azure AD) tenant ID for service principal auth."""
    client_id: str | None = None
    """Optional Microsoft Entra ID application (client) ID."""
    client_secret: str | None = None
    """Optional Microsoft Entra ID client secret."""
    endpoint: str | None = None
    """Optional custom Azure Blob endpoint URL."""
    use_emulator: bool = False
    """When True, connect to the local Azurite emulator endpoint."""

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


class OCIConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for native Oracle Cloud Infrastructure (OCI) Object Storage REST v2."""

    bucket: str
    """Target OCI Object Storage bucket name."""
    namespace: str
    """OCI Object Storage tenancy namespace string."""
    region: str = "us-ashburn-1"
    """OCI region identifier (e.g. `"us-ashburn-1"` or `"us-phoenix-1"`)."""
    tenancy_ocid: str | None = None
    """OCID of the Oracle Cloud tenancy (`ocid1.tenancy.oc1..`)."""
    user_ocid: str | None = None
    """OCID of the IAM user signing API requests (`ocid1.user.oc1..`)."""
    fingerprint: str | None = None
    """MD5 fingerprint of the RSA API signing public key."""
    private_key_pem: str | None = None
    """Inline PEM-encoded RSA private key used for `draft-cavage-http-signatures-08` signing."""
    private_key_path: str | Path | None = None
    """Filesystem path to a PEM-encoded RSA private key file."""
    par_token: str | None = None
    """Optional OCI Pre-Authenticated Request (PAR) token for bucket-scoped URL signing."""
    par_url_prefix: str | None = None
    """Optional full OCI Pre-Authenticated Request base URL prefix ending in `/o/`."""
    endpoint: str | None = None
    """Optional custom OCI Object Storage REST v2 endpoint URL."""

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
        if self.par_token:
            opts["par_token"] = self.par_token
        if self.par_url_prefix:
            opts["par_url_prefix"] = self.par_url_prefix
        if self.endpoint:
            opts["endpoint"] = self.endpoint
        return f"oci://{self.bucket}@{self.namespace}", opts


class R2Config(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Cloudflare R2 object storage."""

    bucket: str
    """Target Cloudflare R2 bucket name."""
    account_id: str
    """Cloudflare 32-character hexadecimal account identifier."""
    access_key_id: str | None = None
    """Optional R2 S3-compatible API token access key ID."""
    secret_access_key: str | None = None
    """Optional R2 S3-compatible API token secret access key."""
    jurisdiction: Literal["eu", "fedramp"] | str | None = None
    """Optional R2 data-locality jurisdiction subdomain (`"eu"` or `"fedramp"`)."""
    custom_domain: str | None = None
    """Optional public custom domain bound to the R2 bucket.

    The domain is forwarded only as the `custom_domain` store option and has no runtime effect on
    presigned URLs: `sign`, `presign_get`, and `presign_put` always target the R2 S3 API endpoint
    (`resolved_endpoint()`), because Cloudflare honors presigned URLs only on that domain and rejects
    them on custom domains.
    """
    endpoint: str | None = None
    """Optional explicit R2 endpoint URL override."""
    allow_http: bool = False
    """When True, permit unencrypted `http://` connections for local testing."""

    @staticmethod
    def chunk_uniform_parts(data: Buffer, part_size: int = 8 * 1024 * 1024) -> list[bytes]:
        """Split a payload into uniform-sized parts for Cloudflare R2 multipart uploads."""
        raw = memoryview(data).cast("B").tobytes()
        native_mod = _BaseObjectStore._ensure_native_module()
        return [bytes(part) for part in native_mod.py_chunk_r2_uniform_parts(raw, part_size)]

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
        if self.custom_domain:
            opts["custom_domain"] = self.custom_domain
        if self.allow_http:
            opts["allow_http"] = "true"
        return f"r2://{self.bucket}", opts


class HTTPConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for HTTP/WebDAV object storage."""

    url: str
    """Base `http://` or `https://` origin URL for the HTTP/WebDAV store."""
    headers: dict[str, str] | None = None
    """Optional static HTTP headers included on every outbound storage request."""

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary.

        Custom headers travel as one JSON object under the `headers` option.
        """
        if not self.headers:
            return self.url, {}
        return self.url, {"headers": encode_json(self.headers)}


class MemoryConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for in-memory object storage."""

    namespace: str = "default"
    """Descriptive label for the in-memory store; it does not select or share storage.

    Every store built from a `MemoryConfig` or `memory://` URI owns a private in-memory backend, so two
    stores with the same namespace never see each other's objects. Only handles derived through
    `to_async()` or `to_sync()` share data.
    """

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        return "memory://", {"namespace": self.namespace}


type StorageConfig = LocalConfig | S3Config | GCSConfig | AzureConfig | OCIConfig | R2Config | HTTPConfig | MemoryConfig


class ObjectMeta(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Metadata describing an object stored in ObjectStore."""

    path: str
    """Normalized `/`-delimited object key within the bucket or root."""
    size: int
    """Object payload size in bytes."""
    last_modified: str = ""
    """RFC 3339 / ISO-8601 UTC timestamp of the object's last modification."""
    etag: str | None = None
    """Optional content entity tag (`ETag`) returned by the storage backend."""
    version: str | None = None
    """Optional backend object version identifier."""
    content_type: str | None = None
    """Optional MIME content type (`Content-Type`) associated with the object."""
    attributes: dict[str, str] = msgspec.field(default_factory=dict)
    """User-defined string metadata attributes persisted with the object."""


class PutResult(ObjectMeta, frozen=True, gc=False, kw_only=True):
    """Result metadata returned from a write operation."""


class SigningConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for HMAC-SHA256, S3 SigV4, Azure SAS, OCI PAR, and GCS V4 presigned URL generation."""

    provider: Literal["hmac", "s3", "gcs", "azure", "oci"] = "hmac"
    """URL signing scheme (`"hmac"`, `"s3"`, `"gcs"`, `"azure"`, or `"oci"`)."""
    bucket: str = ""
    """Target bucket name embedded in S3 SigV4, GCS V4, or OCI PAR presigned URLs."""
    namespace: str = ""
    """OCI Object Storage tenancy namespace used when `provider="oci"`."""
    region: str = "us-east-1"
    """Region identifier used in S3 SigV4 credential scopes or OCI PAR origins."""
    access_key_id: str = ""
    """Access key ID or GCS HMAC key ID used to sign presigned requests."""
    secret_access_key: str = ""
    """Secret access key used to derive the S3 SigV4 or GCS signing key."""
    hmac_secret: str = ""
    """Shared secret key used when `provider="hmac"`."""
    service_account_email: str = ""
    """Google Cloud service account email used for GCS V4 signing."""
    target_service_account: str | None = None
    """Optional target service account email for keyless IAM `signBlob` impersonation."""
    private_key_pem: str | None = None
    """Optional PEM-encoded RSA private key for local zero-network GCS V4 signing."""
    iam_endpoint: str = "https://iamcredentials.googleapis.com"
    """Google Cloud IAM Credentials API base URL used for keyless GCS V4 signing."""
    access_token: str | None = None
    """Optional OAuth2 bearer token authenticating keyless IAM `signBlob` calls."""
    account: str = ""
    """Azure Storage account name used when `provider="azure"`."""
    container: str = ""
    """Azure Blob container name used when `provider="azure"`."""
    azure_access_key: str = ""
    """Base64-encoded Azure Storage shared key used when `provider="azure"`."""
    par_token: str | None = None
    """Optional OCI Pre-Authenticated Request token used when `provider="oci"`."""
    par_url_prefix: str | None = None
    """Optional full OCI Pre-Authenticated Request URL prefix ending in `/o/`."""
    endpoint: str | None = None
    """Optional custom origin URL overriding the default cloud storage hostname."""
    virtual_hosted_style: bool = False
    """When True, S3 SigV4 presigned URLs address the bucket as a host subdomain (`<bucket>.<endpoint host>`)."""

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
        native_mod = _BaseObjectStore._ensure_native_module()
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
        native_mod = _BaseObjectStore._ensure_native_module()
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


class _BaseObjectStore:
    """Base class providing fail-closed target validation, native backend binding, and URL signing helpers."""

    __slots__ = (
        "_gcp_token_cache",
        "_local_root",
        "_native",
        "_options",
        "_scheme",
        "_signing",
        "_source_config_or_uri",
        "_source_options",
        "_url",
    )

    def __init__(
        self,
        target: str | StorageConfig,
        *,
        options: dict[str, str] | None = None,
        signing: SigningConfig | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
    ) -> None:
        if target is None:
            msg = "Storage target configuration or URI is required"
            raise ImproperConfigurationError(msg)
        self._source_config_or_uri = target
        self._source_options = dict(options) if options is not None else None
        create_local_dirs = True
        if isinstance(target, str):
            resolved_url = target
            merged_opts = dict(options or {})
        elif isinstance(
            target,
            (LocalConfig, S3Config, GCSConfig, AzureConfig, OCIConfig, R2Config, HTTPConfig, MemoryConfig),
        ):
            if isinstance(target, LocalConfig):
                create_local_dirs = target.create_dirs
            resolved_url, cfg_opts = target.to_url_and_options()
            merged_opts = dict(cfg_opts)
            if options:
                merged_opts.update(options)
            signing = signing if signing is not None else self._derive_signing_from_config(target)
        else:
            msg = f"Unsupported storage target configuration: {type(target).__name__}"
            raise ImproperConfigurationError(msg)

        cleaned_url, scheme, parsed = self._validate_url(resolved_url)
        self._url = cleaned_url
        self._scheme = scheme
        self._options = merged_opts
        self._signing = signing
        self._gcp_token_cache = token_cache
        self._backfill_options_from_signing()
        self._local_root: Path | None = None
        if self._scheme == "file":
            root_path = Path(parsed.netloc + parsed.path if parsed.netloc else parsed.path)
            if create_local_dirs:
                root_path.mkdir(parents=True, exist_ok=True)
            self._local_root = root_path
        self._native: Any = None

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

    @property
    def token_cache(self) -> SyncGcpTokenCache | AsyncGcpTokenCache | None:
        """Return the active Google Cloud OAuth2 token cache bound to this store, if any."""
        return self._gcp_token_cache

    @property
    def supports_signing(self) -> bool:
        """Return True if a SigningConfig is attached to this ObjectStore."""
        return self._signing is not None

    def close(self) -> None:
        """Close the object store handle and release resources."""
        self._native = None

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
        if self._signing.provider in {"gcs", "azure", "oci"}:
            msg = f"{self._signing.provider.upper()} presigned URL verification is not supported offline"
            raise NotImplementedError(msg)
        if self._signing.provider == "s3":
            return self._verify_s3_v4(signed_url, method=method, now_ts=now_ts)
        return self._verify_hmac(signed_url, method=method, now_ts=now_ts)

    @classmethod
    def from_config(
        cls,
        config: StorageConfig,
        *,
        signing: SigningConfig | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
    ) -> Self:
        """Construct an ObjectStore from any provider StorageConfig."""
        return cls(config, signing=signing, token_cache=token_cache)

    @classmethod
    def from_local(cls, root: str | Path, *, create_dirs: bool = True) -> Self:
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
    ) -> Self:
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
        return cls.from_config(cfg, signing=signing)

    @classmethod
    def from_gcs(
        cls,
        bucket: str,
        *,
        service_account_path: str | None = None,
        service_account_key: str | None = None,
        application_credentials: str | None = None,
        service_account_email: str | None = None,
        target_service_account: str | None = None,
        iam_endpoint: str = "https://iamcredentials.googleapis.com",
        access_token: str | None = None,
        endpoint: str | None = None,
        allow_http: bool = False,
        skip_signature: bool = False,
        signing: SigningConfig | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
    ) -> Self:
        """Construct an ObjectStore backed by Google Cloud Storage."""
        cfg = GCSConfig(
            bucket=bucket,
            service_account_path=service_account_path,
            service_account_key=service_account_key,
            application_credentials=application_credentials,
            service_account_email=service_account_email,
            target_service_account=target_service_account,
            iam_endpoint=iam_endpoint,
            access_token=access_token,
            endpoint=endpoint,
            allow_http=allow_http,
            skip_signature=skip_signature,
        )
        return cls.from_config(cfg, signing=signing, token_cache=token_cache)

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
        signing: SigningConfig | None = None,
    ) -> Self:
        """Construct an ObjectStore backed by Azure Blob Storage or ADLS Gen2."""
        cfg = AzureConfig(
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
        return cls.from_config(cfg, signing=signing)

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
        par_token: str | None = None,
        par_url_prefix: str | None = None,
        endpoint: str | None = None,
        signing: SigningConfig | None = None,
    ) -> Self:
        """Construct an ObjectStore backed by native OCI Object Storage REST v2."""
        return cls.from_config(
            OCIConfig(
                bucket=bucket,
                namespace=namespace,
                region=region,
                tenancy_ocid=tenancy_ocid,
                user_ocid=user_ocid,
                fingerprint=fingerprint,
                private_key_pem=private_key_pem,
                private_key_path=private_key_path,
                par_token=par_token,
                par_url_prefix=par_url_prefix,
                endpoint=endpoint,
            ),
            signing=signing,
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
        signing: SigningConfig | None = None,
    ) -> Self:
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
        return cls.from_config(cfg, signing=signing)

    @classmethod
    def from_http(cls, url: str, *, headers: dict[str, str] | None = None) -> Self:
        """Construct an ObjectStore backed by HTTP/WebDAV."""
        return cls.from_config(HTTPConfig(url=url, headers=headers))

    @classmethod
    def from_memory(cls, namespace: str = "default") -> Self:
        """Construct an in-memory ObjectStore."""
        return cls.from_config(MemoryConfig(namespace=namespace))

    @staticmethod
    def _validate_url(url: str) -> tuple[str, str, Any]:
        """Validate a storage target URL and return `(cleaned_url, scheme, parsed)`."""
        cleaned = url.strip()
        if not cleaned or "://" not in cleaned:
            msg = f"Storage target URI must include a valid scheme: {url!r}"
            raise ImproperConfigurationError(msg)
        parsed = urlparse(cleaned)
        scheme = parsed.scheme.lower()
        if scheme not in _SUPPORTED_SCHEMES:
            msg = f"Unsupported storage URI scheme {scheme!r} in {url!r}"
            raise ImproperConfigurationError(msg)
        if scheme == "file":
            if not parsed.netloc and not parsed.path:
                msg = f"Local filesystem storage URI is missing root path: {url!r}"
                raise ImproperConfigurationError(msg)
        elif scheme != "memory" and not parsed.netloc:
            msg = f"Storage target URI is missing bucket or host: {url!r}"
            raise ImproperConfigurationError(msg)
        return cleaned, scheme, parsed

    def _backfill_options_from_signing(self) -> None:
        """Populate missing native builder options from an attached SigningConfig when provided."""
        if self._signing is None:
            return
        if self._scheme in {"s3", "r2", "b2"} and self._signing.provider == "s3":
            if self._signing.region:
                self._options.setdefault("region", self._signing.region)
            if self._signing.access_key_id:
                self._options.setdefault("access_key_id", self._signing.access_key_id)
            if self._signing.secret_access_key:
                self._options.setdefault("secret_access_key", self._signing.secret_access_key)
            if self._signing.endpoint:
                self._options.setdefault("endpoint", self._signing.endpoint)
                if self._signing.endpoint.startswith("http://"):
                    self._options.setdefault("allow_http", "true")
            if self._signing.virtual_hosted_style:
                self._options.setdefault("virtual_hosted_style", "true")
        elif self._scheme in {"az", "azure", "abfs"} and self._signing.provider == "azure":
            if self._signing.account:
                self._options.setdefault("account", self._signing.account)
            if self._signing.azure_access_key:
                self._options.setdefault("access_key", self._signing.azure_access_key)
            if self._signing.endpoint:
                self._options.setdefault("endpoint", self._signing.endpoint)
                if self._signing.endpoint.startswith("http://"):
                    self._options.setdefault("allow_http", "true")
        elif self._scheme in {"gs", "gcs"} and self._signing.provider == "gcs":
            if self._signing.access_token:
                self._options.setdefault("access_token", self._signing.access_token)
            if self._signing.service_account_email:
                self._options.setdefault("service_account_email", self._signing.service_account_email)
            if self._signing.target_service_account:
                self._options.setdefault("target_service_account", self._signing.target_service_account)
            if self._signing.iam_endpoint:
                self._options.setdefault("iam_endpoint", self._signing.iam_endpoint)
            if self._signing.endpoint:
                self._options.setdefault("endpoint", self._signing.endpoint)
                if self._signing.endpoint.startswith("http://"):
                    self._options.setdefault("allow_http", "true")
        elif self._scheme == "oci" and self._signing.provider == "oci":
            if self._signing.namespace:
                self._options.setdefault("namespace", self._signing.namespace)
            if self._signing.region:
                self._options.setdefault("region", self._signing.region)
            if self._signing.par_token:
                self._options.setdefault("par_token", self._signing.par_token)
            if self._signing.par_url_prefix:
                self._options.setdefault("par_url_prefix", self._signing.par_url_prefix)
            if self._signing.endpoint:
                self._options.setdefault("endpoint", self._signing.endpoint)

    def _require_native(self) -> Any:
        """Return the bound native store instance or raise RuntimeError if closed."""
        if self._native is None:
            msg = "ObjectStore handle is closed"
            raise RuntimeError(msg)
        return self._native

    @staticmethod
    def _derive_signing_from_config(config: StorageConfig) -> SigningConfig | None:
        """Derive a default SigningConfig from provider credentials when available."""
        if isinstance(config, S3Config) and config.access_key_id and config.secret_access_key:
            return SigningConfig(
                provider="s3",
                bucket=config.bucket,
                region=config.region,
                access_key_id=config.access_key_id,
                secret_access_key=config.secret_access_key,
                endpoint=config.endpoint,
                virtual_hosted_style=config.virtual_hosted_style,
            )
        if isinstance(config, R2Config) and config.access_key_id and config.secret_access_key:
            return SigningConfig(
                provider="s3",
                bucket=config.bucket,
                region="auto",
                access_key_id=config.access_key_id,
                secret_access_key=config.secret_access_key,
                endpoint=config.resolved_endpoint(),
            )
        if isinstance(config, GCSConfig):
            email, pem = config.extract_service_account_identity()
            if email:
                return SigningConfig(
                    provider="gcs",
                    bucket=config.bucket,
                    service_account_email=email,
                    target_service_account=config.target_service_account,
                    private_key_pem=pem,
                    iam_endpoint=config.iam_endpoint,
                    access_token=config.access_token,
                    endpoint=config.endpoint,
                )
        if isinstance(config, AzureConfig) and config.access_key:
            resolved_ep = config.endpoint or (
                f"http://127.0.0.1:4577/{config.account}" if config.use_emulator else None
            )
            return SigningConfig(
                provider="azure",
                account=config.account,
                container=config.container,
                azure_access_key=config.access_key,
                endpoint=resolved_ep,
            )
        if isinstance(config, OCIConfig) and (config.par_token or config.par_url_prefix):
            return SigningConfig(
                provider="oci",
                bucket=config.bucket,
                namespace=config.namespace,
                region=config.region,
                par_token=config.par_token,
                par_url_prefix=config.par_url_prefix,
                endpoint=config.endpoint,
            )
        return None

    @staticmethod
    def _native_payload(data: Buffer) -> Buffer:
        """Return a buffer the native layer accepts as unsigned bytes without wrapping plain byte buffers.

        ``bytes``, ``bytearray``, and ``"B"``-format buffers (``memoryview``, ``StorageBuffer``) pass through
        unchanged so ``bytes`` and ``StorageBuffer`` stay zero-copy in native extraction. Typed buffers such as
        ``pyarrow.Buffer`` (``"b"``) or NumPy arrays need a flat ``"B"`` view because native extraction only
        accepts ``u8`` element formats.
        """
        if isinstance(data, bytes | bytearray):
            return data
        view = memoryview(data)
        if view.format == "B":
            view.release()
            return data
        return view.cast("B")

    @staticmethod
    def _native_to_put_result(native_meta: Any, fallback_etag: str = "") -> PutResult:
        """Convert a native ObjectMeta instance into a PutResult."""
        etag = getattr(native_meta, "etag", None) or fallback_etag
        raw_attrs = getattr(native_meta, "attributes", None)
        return PutResult(
            path=str(native_meta.path),
            size=int(native_meta.size),
            last_modified=str(getattr(native_meta, "last_modified", "")),
            etag=str(etag) if etag else None,
            version=getattr(native_meta, "version", None),
            content_type=getattr(native_meta, "content_type", None),
            attributes={str(k): str(v) for k, v in raw_attrs.items()} if isinstance(raw_attrs, Mapping) else {},
        )

    @staticmethod
    def _native_to_object_meta(native_meta: Any) -> ObjectMeta:
        """Convert a native ObjectMeta instance into an ObjectMeta."""
        raw_attrs = getattr(native_meta, "attributes", None)
        return ObjectMeta(
            path=str(native_meta.path),
            size=int(native_meta.size),
            last_modified=str(getattr(native_meta, "last_modified", "")),
            etag=getattr(native_meta, "etag", None),
            version=getattr(native_meta, "version", None),
            content_type=getattr(native_meta, "content_type", None),
            attributes={str(k): str(v) for k, v in raw_attrs.items()} if isinstance(raw_attrs, Mapping) else {},
        )

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
        key = self._key(path)
        if self._signing.provider == "gcs":
            return self._sign_gcs_v4_iam(key, method=method, expires_in_secs=expires_in_secs, now_ts=now_ts)
        if self._signing.provider == "s3":
            return self._sign_s3_v4(key, method=method, expires_in_secs=expires_in_secs, now_ts=now_ts)
        if self._signing.provider == "azure":
            return SigningConfig.sign_azure_sas(
                self._signing.account,
                self._signing.azure_access_key,
                self._signing.container,
                key,
                method=method,
                expires_in_secs=expires_in_secs,
                now_ts=now_ts,
                endpoint=self._signing.endpoint,
            )
        if self._signing.provider == "oci":
            return self._sign_oci_par(key, expires_in_secs=expires_in_secs)
        return self._sign_hmac(key, method=method, expires_in_secs=expires_in_secs, now_ts=now_ts)

    async def _presign_async(
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
        key = self._key(path)
        if self._signing.provider == "gcs":
            return await self._sign_gcs_v4_iam_async(
                key,
                method=method,
                expires_in_secs=expires_in_secs,
                now_ts=now_ts,
            )
        return self._presign(path, method=method, expires_in_secs=expires_in_secs, timestamp=now_ts)

    def _sign_oci_par(self, key: str, *, expires_in_secs: int) -> str:
        assert self._signing is not None
        native_mod = self._ensure_native_module()
        bucket = self._signing.bucket
        namespace = self._signing.namespace
        region = self._signing.region
        par_token = self._signing.par_token or ""
        endpoint = self._signing.endpoint
        if not par_token and self._signing.par_url_prefix:
            matched = _OCI_PAR_PREFIX_RE.match(self._signing.par_url_prefix.strip())
            if matched is None:
                msg = f"Invalid OCI PAR URL prefix: {self._signing.par_url_prefix!r}"
                raise ValueError(msg)
            origin, raw_token, raw_namespace, raw_bucket = matched.groups()
            par_token = unquote(raw_token)
            namespace = namespace or unquote(raw_namespace)
            bucket = bucket or unquote(raw_bucket)
            endpoint = endpoint or origin
        return str(
            native_mod.sign_oci_par_url(
                bucket,
                namespace,
                region,
                par_token,
                key,
                expires_in_secs,
                endpoint,
            )
        )

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
        native_mod = self._ensure_native_module()
        return str(
            native_mod.py_sign_hmac_url(
                f"{base}/{key}",
                self._signing.hmac_secret,
                method,
                expires_in_secs,
                now_ts,
            )
        )

    def _verify_hmac(self, signed_url: str, *, method: str, now_ts: int) -> bool:
        assert self._signing is not None
        native_mod = self._ensure_native_module()
        return bool(
            native_mod.py_verify_hmac_url(
                signed_url,
                self._signing.hmac_secret,
                method,
                now_ts,
            )
        )

    def _sign_s3_v4(
        self,
        key: str,
        *,
        method: str,
        expires_in_secs: int,
        now_ts: int,
    ) -> str:
        assert self._signing is not None
        native_mod = self._ensure_native_module()
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
                virtual_hosted_style=self._signing.virtual_hosted_style,
            )
        )

    def _verify_s3_v4(self, signed_url: str, *, method: str, now_ts: int) -> bool:
        assert self._signing is not None
        native_mod = self._ensure_native_module()
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
                virtual_hosted_style=self._signing.virtual_hosted_style,
            )
        )

    def _allow_ambient_adc(self) -> bool:
        """Return True unless `allow_ambient_adc` is explicitly disabled in store options."""
        return self._options.get("allow_ambient_adc", "true").strip().lower() not in {"false", "0", "no"}

    def _build_signing_adc_helper(self, effective_email: str) -> GCSConfig:
        """Build a source-credential GCSConfig for authenticating IAM signBlob calls."""
        assert self._signing is not None
        refresh_skew_seconds = 300
        raw_skew_ms = self._options.get("refresh_skew_ms")
        if raw_skew_ms is not None:
            with contextlib.suppress(ValueError):
                refresh_skew_seconds = max(0, int(raw_skew_ms) // 1000)
        return GCSConfig(
            bucket=self._signing.bucket,
            service_account_path=self._options.get("service_account_path"),
            service_account_key=self._options.get("service_account_key"),
            application_credentials=self._options.get("application_credentials_path"),
            service_account_email=effective_email,
            iam_endpoint=self._signing.iam_endpoint,
            refresh_skew_seconds=refresh_skew_seconds,
            quota_project_id=self._options.get("quota_project_id"),
        )

    def _auto_build_gcs_token_cache_options(self) -> dict[str, str] | None:
        """Build native GcpTokenCache options when dynamic GCS OAuth2 credentials can be shared."""
        if self._scheme not in {"gs", "gcs"}:
            return None
        if self._options.get("skip_signature", "").strip().lower() in {"true", "1", "yes"}:
            return None
        if self._options.get("access_token") or self._options.get("target_service_account"):
            return None
        opts = dict(self._options)
        opts.setdefault("scopes", ",".join(GCS_DEFAULT_SCOPES))
        return opts

    def _ensure_sync_gcs_token_cache(self, effective_email: str) -> SyncGcpTokenCache:
        """Lazily initialize and return the synchronous token cache for keyless GCS IAM signing."""
        if not isinstance(self._gcp_token_cache, SyncGcpTokenCache):
            self._gcp_token_cache = self._build_signing_adc_helper(effective_email).sync_token_cache(
                allow_ambient_adc=self._allow_ambient_adc()
            )
        return self._gcp_token_cache

    def _ensure_async_gcs_token_cache(self, effective_email: str) -> AsyncGcpTokenCache:
        """Lazily initialize and return the asynchronous token cache for keyless GCS IAM signing."""
        if not isinstance(self._gcp_token_cache, AsyncGcpTokenCache):
            self._gcp_token_cache = self._build_signing_adc_helper(effective_email).async_token_cache(
                allow_ambient_adc=self._allow_ambient_adc()
            )
        return self._gcp_token_cache

    def _sign_gcs_v4_iam(
        self,
        key: str,
        *,
        method: str,
        expires_in_secs: int,
        now_ts: int,
    ) -> str:
        assert self._signing is not None
        effective_email = self._signing.target_service_account or self._signing.service_account_email
        if not effective_email:
            msg = "service_account_email or target_service_account is required for GCS V4 URL signing"
            raise ValueError(msg)
        private_key_pem = None if self._signing.target_service_account else self._signing.private_key_pem
        access_token = self._signing.access_token
        capsule: object | None = None
        if access_token is None and private_key_pem is None:
            cache = self._ensure_sync_gcs_token_cache(effective_email)
            capsule = cache._capsule()

        native_mod = self._ensure_native_module()
        return str(
            native_mod.py_sign_gcs_v4_iam_url(
                self._signing.bucket,
                effective_email,
                method,
                key,
                expires_in_secs,
                now_ts,
                self._signing.iam_endpoint,
                access_token,
                private_key_pem,
                self._signing.endpoint,
                capsule,
            )
        )

    async def _sign_gcs_v4_iam_async(
        self,
        key: str,
        *,
        method: str,
        expires_in_secs: int,
        now_ts: int,
    ) -> str:
        assert self._signing is not None
        effective_email = self._signing.target_service_account or self._signing.service_account_email
        if not effective_email:
            msg = "service_account_email or target_service_account is required for GCS V4 URL signing"
            raise ValueError(msg)
        private_key_pem = None if self._signing.target_service_account else self._signing.private_key_pem
        access_token = self._signing.access_token
        capsule: object | None = None
        if access_token is None and private_key_pem is None:
            cache = self._ensure_async_gcs_token_cache(effective_email)
            capsule = cache._capsule()

        native_mod = self._ensure_native_module()
        return str(
            await native_mod.py_sign_gcs_v4_iam_url_async(
                self._signing.bucket,
                effective_email,
                method,
                key,
                expires_in_secs,
                now_ts,
                self._signing.iam_endpoint,
                access_token,
                private_key_pem,
                self._signing.endpoint,
                capsule,
            )
        )

    def _resolve_local(self, key: str) -> Path:
        assert self._local_root is not None
        resolved = (self._local_root / key).resolve()
        root_resolved = self._local_root.resolve()
        if root_resolved not in resolved.parents and resolved != root_resolved:
            msg = f"Path traversal outside storage root rejected: {key}"
            raise ValueError(msg)
        return resolved

    @classmethod
    def _ensure_native_module(cls) -> Any:
        native_mod = import_optional("vertebra._storage")
        if native_mod is None:
            raise MissingDependencyError(
                "vertebra._storage",
                install_package="vertebra-core[storage]",
                extra="storage",
            )
        return native_mod

    @staticmethod
    def _normalize_key(path: str) -> str:
        """Strip surrounding whitespace and leading `/`, collapse empty segments, and drop `.` segments.

        A trailing `/` is kept so directory-placeholder keys and list prefixes keep their meaning; `~`
        and `%` pass through unchanged for native validation.

        Raises:
            ValueError: If the key contains a `..` segment.
        """
        cleaned = path.strip()
        segments = cleaned.split("/")
        if ".." in segments:
            msg = f"Invalid relative parent segment in storage key: {path}"
            raise ValueError(msg)
        normalized = "/".join(segment for segment in segments if segment and segment != ".")
        if normalized and cleaned.endswith("/"):
            return f"{normalized}/"
        return normalized

    @staticmethod
    def _staging_base_dir() -> Path:
        """Return `/dev/shm` when it is a writable directory, otherwise the system temporary directory."""
        if _SHM_STAGING_DIR.is_dir() and os.access(_SHM_STAGING_DIR, os.W_OK):
            return _SHM_STAGING_DIR
        return Path(tempfile.gettempdir())

    @staticmethod
    def _discard_staged(created: Sequence[Path], staging_dir: Path | None) -> None:
        """Remove a temporary staging directory, or only the files the call created in a caller-owned directory."""
        if staging_dir is not None:
            shutil.rmtree(staging_dir, ignore_errors=True)
            return
        for created_path in created:
            created_path.unlink(missing_ok=True)

    @staticmethod
    def _glob_matches(items: Iterable[ObjectMeta], key_pattern: str) -> list[ObjectMeta]:
        """Filter listed objects with case-sensitive `fnmatch`, skipping trailing-`/` directory placeholders."""
        return [meta for meta in items if not meta.path.endswith("/") and fnmatch.fnmatchcase(meta.path, key_pattern)]

    @staticmethod
    def _parquet_writer_options(
        compression: str,
        use_dictionary: bool | Sequence[str],
        use_byte_stream_split: bool | Sequence[str],
        options: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Merge explicit Parquet encoding keywords with caller `ParquetWriter` options, letting `options` win.

        A single column name passed as `str` selects that one column rather than its characters.
        """

        encodings = {"use_dictionary": use_dictionary, "use_byte_stream_split": use_byte_stream_split}
        return {
            "compression": compression,
            **{
                name: value if isinstance(value, bool) else [value] if isinstance(value, str) else list(value)
                for name, value in encodings.items()
            },
            **(options or {}),
        }

    def _key(self, path: str) -> str:
        """Return the normalized object key, rejecting keys that escape a local storage root.

        Raises:
            ValueError: If the key contains a `..` segment or resolves outside the local root.
        """
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        return key

    def _download_targets(self, keys: Sequence[str]) -> list[tuple[str, str]]:
        """Return `(key, file_name)` pairs, rejecting keys without a basename or with duplicate basenames.

        Raises:
            ValueError: If a key has no file name or two keys share a basename.
        """
        targets: dict[str, str] = {}
        for raw_key in keys:
            key = self._key(raw_key)
            name = key.rsplit("/", 1)[-1]
            if not name:
                msg = f"Storage key has no file name to download into: {raw_key!r}"
                raise ValueError(msg)
            if name in targets:
                msg = f"Duplicate destination file name {name!r} for keys {targets[name]!r} and {key!r}"
                raise ValueError(msg)
            targets[name] = key
        return [(key, name) for name, key in targets.items()]

    def _upload_sources(self, paths: Sequence[str | Path], destination_prefix: str) -> list[tuple[str, Path]]:
        """Return `(key, local_path)` pairs keyed by `destination_prefix` joined with each file name.

        Raises:
            ValueError: If two local files map to the same object key.
            FileNotFoundError: If a local source file does not exist.
        """
        prefix = destination_prefix.strip().strip("/")
        sources: dict[str, Path] = {}
        for raw_path in paths:
            local = Path(raw_path)
            key = self._key(f"{prefix}/{local.name}" if prefix else local.name)
            if key in sources:
                msg = f"Duplicate object key {key!r} for files {str(sources[key])!r} and {str(local)!r}"
                raise ValueError(msg)
            if not local.is_file():
                msg = f"Source file not found: {local}"
                raise FileNotFoundError(msg)
            sources[key] = local
        return list(sources.items())

    def _glob_query(self, pattern: str) -> tuple[str, str | None]:
        """Return the normalized key pattern and its segment-aligned list prefix, or `None` for literal keys.

        A leading store URL is stripped. The list prefix ends at the last `/` before the first wildcard
        because native listings match whole path segments.

        Raises:
            ValueError: If the pattern is empty after normalization.
        """
        base = self._url.rstrip("/")
        relative = pattern[len(base) + 1 :] if pattern.startswith(f"{base}/") else pattern
        key_pattern = self._normalize_key(relative)
        if not key_pattern:
            msg = "glob pattern must not be empty"
            raise ValueError(msg)
        wildcard = _GLOB_WILDCARD_RE.search(key_pattern)
        if wildcard is None:
            return key_pattern, None
        literal = key_pattern[: wildcard.start()]
        return key_pattern, literal[: literal.rfind("/") + 1]

    def _compose_keys(self, sources: Sequence[str], dst: str) -> tuple[list[str], str]:
        """Normalize compose source keys and destination key.

        Raises:
            ValueError: If `sources` is empty.
        """
        if not sources:
            msg = "compose requires at least one source object"
            raise ValueError(msg)
        return [self._key(source) for source in sources], self._key(dst)


class SyncObjectStore(_BaseObjectStore):
    """Synchronous object store supporting zero-copy buffers, blocking I/O, and multi-cloud URL signing."""

    __slots__ = ()

    def __init__(
        self,
        target: str | StorageConfig,
        *,
        options: dict[str, str] | None = None,
        signing: SigningConfig | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
        _native_instance: Any = None,
    ) -> None:
        if token_cache is not None and not isinstance(token_cache, (SyncGcpTokenCache, AsyncGcpTokenCache)):
            msg = f"token_cache must be a SyncGcpTokenCache or AsyncGcpTokenCache, got {type(token_cache).__name__}"
            raise ImproperConfigurationError(msg)
        resolved_cache = (
            token_cache
            if isinstance(token_cache, SyncGcpTokenCache)
            else (token_cache.to_sync() if isinstance(token_cache, AsyncGcpTokenCache) else None)
        )
        super().__init__(target, options=options, signing=signing, token_cache=resolved_cache)
        native_mod = self._ensure_native_module()
        if _native_instance is not None:
            self._native = _native_instance
            return
        if self._gcp_token_cache is None:
            cache_opts = self._auto_build_gcs_token_cache_options()
            if cache_opts is not None:
                allow_ambient = self._allow_ambient_adc()
                with contextlib.suppress(ValueError, GoogleCloudAuthError, OSError):
                    self._gcp_token_cache = SyncGcpTokenCache(
                        cache_opts,
                        list(GCS_DEFAULT_SCOPES),
                        allow_ambient_adc=allow_ambient,
                    )
        capsule = self._gcp_token_cache._capsule() if self._gcp_token_cache is not None else None
        try:
            self._native = native_mod.SyncObjectStore(self._url, self._options or None, capsule)
        except ValueError as exc:
            raise ImproperConfigurationError(str(exc)) from exc

    def __enter__(self) -> Self:
        """Enter the context manager and return self."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the object store when exiting the context manager."""
        _ = (exc_type, exc_val, exc_tb)
        self.close()

    def to_async(self) -> AsyncObjectStore:
        """Return an equivalent AsyncObjectStore instance sharing the underlying storage engine."""
        native = self._require_native()
        async_token_cache = (
            self._gcp_token_cache.to_async()
            if isinstance(self._gcp_token_cache, SyncGcpTokenCache)
            else (self._gcp_token_cache if isinstance(self._gcp_token_cache, AsyncGcpTokenCache) else None)
        )
        return AsyncObjectStore(
            self._source_config_or_uri,
            options=self._source_options,
            signing=self._signing,
            token_cache=async_token_cache,
            _native_instance=native.to_async(),
        )

    def put(
        self,
        path: str,
        data: Buffer,
        *,
        content_type: str | None = None,
        attributes: Mapping[str, str] | None = None,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> PutResult:
        """Write any Buffer (bytes, bytearray, memoryview) to the given object key."""
        key = self._key(path)
        norm_attrs = {str(k): str(v) for k, v in attributes.items()} if attributes is not None else None
        native = self._require_native()
        native_meta = native.put(
            key,
            self._native_payload(data),
            content_type=content_type,
            attributes=norm_attrs,
            if_not_exists=if_not_exists,
            if_match=if_match,
            if_none_match=if_none_match,
        )
        fallback_etag = "" if getattr(native_meta, "etag", None) else f"{int(native_meta.size):016x}"
        return self._native_to_put_result(native_meta, fallback_etag)

    def put_file(self, path: str, file_path: str | Path) -> PutResult:
        """Read a local file and upload it to the given object key."""
        key = self._key(path)
        resolved_src = Path(file_path)
        if not resolved_src.exists():
            msg = f"Source file not found: {resolved_src}"
            raise FileNotFoundError(msg)
        native = self._require_native()
        native_meta = native.put_file(key, str(resolved_src))
        return self._native_to_put_result(native_meta, f"{int(native_meta.size):016x}")

    def upload_many(
        self,
        paths: Sequence[str | Path],
        destination_prefix: str = "",
        *,
        concurrency: int = 16,
    ) -> list[PutResult]:
        """Upload local files concurrently to keys formed from `destination_prefix` and each file name.

        Raises:
            ValueError: If two files map to the same key; raised before any transfer starts.
            FileNotFoundError: If a local source file does not exist.
        """
        sources = self._upload_sources(paths, destination_prefix)
        if not sources:
            return []
        native_metas = self._require_native().put_files(
            [(key, str(local)) for key, local in sources], concurrency=concurrency
        )
        return [self._native_to_put_result(meta, f"{int(meta.size):016x}") for meta in native_metas]

    def put_multipart(self, path: str, parts: Iterable[Buffer]) -> PutResult:
        """Upload `parts` in order as one object through the incremental multipart path."""
        return self.put_stream(path, parts)

    def compose(self, sources: Sequence[str], dst: str, *, content_type: str | None = None) -> PutResult:
        """Concatenate `sources` in order into `dst` through the backend's native compose path.

        Raises:
            ValueError: If `sources` is empty.
        """
        source_keys, dst_key = self._compose_keys(sources, dst)
        native_meta = self._require_native().compose(source_keys, dst_key, content_type=content_type)
        return self._native_to_put_result(native_meta, f"{int(native_meta.size):016x}")

    def put_stream(
        self,
        path: str,
        chunks: Iterable[Buffer],
        *,
        content_type: str | None = None,
        attributes: Mapping[str, str] | None = None,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> PutResult:
        """Stream an iterable of buffer chunks into an incremental multipart upload.

        Each chunk passes through the same buffer normalization as `put`, so typed buffers such as
        `array("I")`, NumPy arrays, and `pyarrow.Buffer` upload as their raw bytes.
        """
        key = self._key(path)
        norm_attrs = {str(k): str(v) for k, v in attributes.items()} if attributes is not None else None
        native = self._require_native()
        upload = native.create_multipart(
            key,
            content_type=content_type,
            attributes=norm_attrs,
            if_not_exists=if_not_exists,
            if_match=if_match,
            if_none_match=if_none_match,
        )
        try:
            for chunk in chunks:
                upload.write_part(self._native_payload(chunk))
            native_meta = upload.complete()
        except BaseException:
            with contextlib.suppress(Exception):
                upload.abort()
            raise
        fallback_etag = "" if getattr(native_meta, "etag", None) else f"{int(native_meta.size):016x}"
        return self._native_to_put_result(native_meta, fallback_etag)

    def get(self, path: str) -> bytes:
        """Read full object bytes for the given key."""
        key = self._key(path)
        return bytes(self._require_native().get(key))

    def get_file(self, path: str, file_path: str | Path) -> ObjectMeta:
        """Stream the object at the given key into a local file and return its metadata."""
        key = self._key(path)
        return self._native_to_object_meta(self._require_native().get_file(key, str(Path(file_path))))

    def download_many(self, keys: Sequence[str], destination: str | Path, *, concurrency: int = 16) -> list[Path]:
        """Download objects concurrently into `destination`, naming each file after its key basename.

        Raises:
            ValueError: If two keys share a basename; raised before any transfer starts.
        """
        targets = self._download_targets(keys)
        if not targets:
            return []
        root = Path(destination)
        local_paths = [root / name for _, name in targets]
        root.mkdir(parents=True, exist_ok=True)
        self._require_native().get_files(
            [(key, str(local)) for (key, _), local in zip(targets, local_paths, strict=True)],
            concurrency=concurrency,
        )
        return local_paths

    @contextlib.contextmanager
    def staged_download(
        self,
        keys: Sequence[str],
        destination: str | Path | None = None,
        *,
        concurrency: int = 16,
    ) -> Iterator[list[Path]]:
        """Download objects into a staging directory and remove the staged files when the context exits.

        Without `destination`, files stage in a unique directory under `/dev/shm` (when writable) or the
        system temporary directory, and that directory is removed on exit. With `destination`, the
        directory is kept and only staged paths that did not exist before the call are unlinked on exit,
        so pre-existing files with the same names survive.

        Raises:
            ValueError: If two keys share a basename; raised before any transfer starts.
        """
        targets = self._download_targets(keys)
        owned_dir = (
            Path(tempfile.mkdtemp(prefix="vertebra-staged-", dir=self._staging_base_dir()))
            if destination is None
            else None
        )
        staging_dir = owned_dir if owned_dir is not None else Path(str(destination))
        staged = [staging_dir / name for _, name in targets]
        created = [path for path in staged if not path.exists(follow_symlinks=False)]
        try:
            if targets:
                self.download_many([key for key, _ in targets], staging_dir, concurrency=concurrency)
            yield staged
        finally:
            self._discard_staged(created, owned_dir)

    def get_view(self, path: str) -> memoryview:
        """Return a zero-copy memoryview over the object payload."""
        key = self._key(path)
        return memoryview(self._require_native().get_buffer(key))

    def get_stream(self, path: str, *, chunk_size: int | None = None) -> Iterator[StorageBuffer]:
        """Stream object payload chunks as zero-copy StorageBuffer instances."""
        key = self._key(path)
        if chunk_size is not None and chunk_size < 1:
            msg = "chunk_size must be >= 1"
            raise ValueError(msg)
        reader = self._require_native().get_stream(key, chunk_size=chunk_size)
        try:
            yield from reader
        finally:
            reader.close()

    def get_range(self, path: str, offset: int, length: int) -> bytes:
        """Read a byte slice of length bytes starting at offset."""
        key = self._key(path)
        return bytes(self._require_native().get_range(key, offset, length))

    def head(self, path: str) -> ObjectMeta:
        """Return object metadata without transferring the full payload."""
        key = self._key(path)
        return self._native_to_object_meta(self._require_native().head(key))

    def exists(self, path: str) -> bool:
        """Return True if the object key exists."""
        key = self._key(path)
        return bool(self._require_native().exists(key))

    def delete(self, path: str) -> None:
        """Delete the object at the given key."""
        key = self._key(path)
        with contextlib.suppress(FileNotFoundError):
            self._require_native().delete(key)

    def copy(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Copy an object within the store from src to dst.

        Raises:
            FileExistsError: If `overwrite` is False and `dst` already exists.
        """
        src_key = self._key(src)
        dst_key = self._key(dst)
        self._require_native().copy(src_key, dst_key, if_not_exists=not overwrite)

    def rename(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Atomically rename or move an object from src to dst within the store."""
        src_key = self._key(src)
        dst_key = self._key(dst)
        self._require_native().rename(src_key, dst_key, if_not_exists=not overwrite)

    def list_page(
        self,
        prefix: str = "",
        *,
        cursor: str | None = None,
        limit: int = 1000,
    ) -> tuple[list[ObjectMeta], str | None]:
        """Return up to limit objects under prefix starting after cursor, with the next cursor."""
        if limit < 1:
            msg = "limit must be >= 1"
            raise ValueError(msg)
        norm_prefix = self._normalize_key(prefix) if prefix else None
        items, next_cursor = self._require_native().list_page(norm_prefix, cursor, limit)
        return [self._native_to_object_meta(item) for item in items], next_cursor

    def list_with_delimiter(self, prefix: str = "") -> tuple[list[ObjectMeta], list[str]]:
        """List direct child objects and `/` common sub-prefixes immediately beneath prefix."""
        norm_prefix = self._normalize_key(prefix) if prefix else ""
        items, prefixes = self._require_native().list_with_delimiter(norm_prefix or None)
        return [self._native_to_object_meta(item) for item in items], [str(p) for p in prefixes]

    def glob(self, pattern: str) -> list[ObjectMeta]:
        """Return objects whose keys match a shell-style `*`, `?`, or `[...]` pattern.

        Literal patterns resolve with a single `head` call; wildcard patterns list the segment-aligned
        literal prefix and filter full keys with `fnmatch.fnmatchcase`. Matching is not segment-aware:
        `*` and `?` also match `/`, so `runs/*` matches nested keys such as `runs/a/b.json`. Narrow a
        match with explicit literal segments (`runs/2024/part-*.json` lists only `runs/2024/` and
        requires the `part-` and `.json` anchors), and use `list_with_delimiter` when exactly one level
        is required. Keys ending in `/` are never returned.
        """
        key_pattern, list_prefix = self._glob_query(pattern)
        if list_prefix is not None:
            return self._glob_matches(self.list(list_prefix), key_pattern)
        if key_pattern.endswith("/"):
            return []
        try:
            return [self.head(key_pattern)]
        except FileNotFoundError:
            return []

    def list(self, prefix: str = "") -> list[ObjectMeta]:
        """List objects matching prefix."""
        norm_prefix = self._normalize_key(prefix) if prefix else ""
        items = self._require_native().list(norm_prefix or None)
        return [
            self._native_to_object_meta(item)
            for item in items
            if not norm_prefix or str(item.path).startswith(norm_prefix)
        ]

    def write_arrow(
        self, key: str, data: Any, *, batch_size: int = 65536, options: Mapping[str, Any] | None = None
    ) -> PutResult:
        """Write batch-bounded Arrow IPC through native multipart upload.

        ``options`` keys such as ``compression`` build ``pyarrow.ipc.IpcWriteOptions``;
        a prebuilt ``options`` entry passes through unchanged.
        """
        return _arrow.write_batches_sync(
            self, key, data, file_format="arrow-ipc", batch_size=batch_size, options=options
        )

    def write_parquet(
        self,
        key: str,
        batches: Any,
        *,
        schema: Any = None,
        compression: str = "zstd",
        row_group_size: int = 65536,
        use_dictionary: bool | Sequence[str] = True,
        use_byte_stream_split: bool | Sequence[str] = False,
        options: Mapping[str, Any] | None = None,
    ) -> PutResult:
        """Write batch-bounded Parquet rowgroups through native multipart upload.

        ``options`` is forwarded to ``pyarrow.parquet.ParquetWriter`` and overrides
        ``compression``, ``row_group_size``, ``use_dictionary``, and ``use_byte_stream_split``.
        """
        return _arrow.write_batches_sync(
            self,
            key,
            batches,
            file_format="parquet",
            batch_size=row_group_size,
            schema=schema,
            options=self._parquet_writer_options(compression, use_dictionary, use_byte_stream_split, options),
        )

    def iter_arrow_batches(
        self, key: str, *, batch_size: int = 65536, options: Mapping[str, Any] | None = None
    ) -> Iterator[Any]:
        """Yield Arrow IPC RecordBatch objects through native streaming reads.

        ``options`` keys such as ``use_threads`` build ``pyarrow.ipc.IpcReadOptions``.
        """
        return _arrow.iter_batches_sync(self, key, file_format="arrow-ipc", batch_size=batch_size, options=options)

    def iter_parquet_batches(
        self,
        key: str,
        *,
        batch_size: int = 65536,
        columns: Sequence[str] | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> Iterator[Any]:
        """Yield projected Parquet batches through bounded native range reads.

        ``options`` is forwarded to ``pyarrow.parquet.ParquetFile``, except ``row_groups``
        and ``use_threads``, which select and decode rowgroups in ``iter_batches``.
        """
        return _arrow.iter_batches_sync(
            self, key, file_format="parquet", batch_size=batch_size, columns=columns, options=options
        )

    def open_reader(
        self,
        path: str,
        *,
        size: int | None = None,
        chunk_size: int = 65536,
        read_ahead: int = 4_194_304,
    ) -> SyncReadableFile:
        """Open a seekable read-only SyncReadableFile stream over the given object key.

        Reads shorter than ``read_ahead`` are served from one cached ``read_ahead``-byte range window;
        ``read_ahead=0`` issues one range request per read.
        """
        return SyncReadableFile(self, path, size=size, chunk_size=chunk_size, read_ahead=read_ahead)

    def open_writer(
        self,
        path: str,
        *,
        content_type: str | None = None,
        attributes: Mapping[str, str] | None = None,
        part_size: int = 8_388_608,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> SyncWritableFile:
        """Open a write-only SyncWritableFile that streams buffered ``part_size`` slices into a multipart upload.

        The upload is created lazily on the first full part or on close, completed by closing the
        writer, and aborted by ``abort()`` or an exception escaping the context manager.
        """
        return SyncWritableFile(
            self,
            path,
            content_type=content_type,
            attributes=attributes,
            part_size=part_size,
            if_not_exists=if_not_exists,
            if_match=if_match,
            if_none_match=if_none_match,
        )

    def sign(
        self,
        key: str,
        *,
        expires_in: int = 3600,
        method: str = "GET",
        timestamp: int | None = None,
    ) -> str:
        """Generate a presigned URL for `key` using the store's configured provider credentials."""
        return self._presign(key, method=method, expires_in_secs=expires_in, timestamp=timestamp)

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


class AsyncObjectStore(_BaseObjectStore):
    """Asynchronous object store supporting zero-copy buffers, Tokio awaitables, and multi-cloud URL signing."""

    __slots__ = ()

    def __init__(
        self,
        target: str | StorageConfig,
        *,
        options: dict[str, str] | None = None,
        signing: SigningConfig | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
        _native_instance: Any = None,
    ) -> None:
        if token_cache is not None and not isinstance(token_cache, (SyncGcpTokenCache, AsyncGcpTokenCache)):
            msg = f"token_cache must be a SyncGcpTokenCache or AsyncGcpTokenCache, got {type(token_cache).__name__}"
            raise ImproperConfigurationError(msg)
        resolved_cache = (
            token_cache
            if isinstance(token_cache, AsyncGcpTokenCache)
            else (token_cache.to_async() if isinstance(token_cache, SyncGcpTokenCache) else None)
        )
        super().__init__(target, options=options, signing=signing, token_cache=resolved_cache)
        native_mod = self._ensure_native_module()
        if _native_instance is not None:
            self._native = _native_instance
            return
        if self._gcp_token_cache is None:
            cache_opts = self._auto_build_gcs_token_cache_options()
            if cache_opts is not None:
                allow_ambient = self._allow_ambient_adc()
                with contextlib.suppress(ValueError, GoogleCloudAuthError, OSError):
                    self._gcp_token_cache = AsyncGcpTokenCache(
                        cache_opts,
                        list(GCS_DEFAULT_SCOPES),
                        allow_ambient_adc=allow_ambient,
                    )
        capsule = self._gcp_token_cache._capsule() if self._gcp_token_cache is not None else None
        try:
            self._native = native_mod.AsyncObjectStore(self._url, self._options or None, capsule)
        except ValueError as exc:
            raise ImproperConfigurationError(str(exc)) from exc

    async def __aenter__(self) -> Self:
        """Enter the async context manager and return self."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the object store when exiting the async context manager."""
        _ = (exc_type, exc_val, exc_tb)
        self.close()

    def to_sync(self) -> SyncObjectStore:
        """Return an equivalent SyncObjectStore instance sharing the underlying storage engine."""
        native = self._require_native()
        sync_token_cache = (
            self._gcp_token_cache.to_sync()
            if isinstance(self._gcp_token_cache, AsyncGcpTokenCache)
            else (self._gcp_token_cache if isinstance(self._gcp_token_cache, SyncGcpTokenCache) else None)
        )
        return SyncObjectStore(
            self._source_config_or_uri,
            options=self._source_options,
            signing=self._signing,
            token_cache=sync_token_cache,
            _native_instance=native.to_sync(),
        )

    async def put(
        self,
        path: str,
        data: Buffer,
        *,
        content_type: str | None = None,
        attributes: Mapping[str, str] | None = None,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> PutResult:
        """Asynchronously write any Buffer to the given object key."""
        key = self._key(path)
        norm_attrs = {str(k): str(v) for k, v in attributes.items()} if attributes is not None else None
        native = self._require_native()
        native_meta = await native.put(
            key,
            self._native_payload(data),
            content_type=content_type,
            attributes=norm_attrs,
            if_not_exists=if_not_exists,
            if_match=if_match,
            if_none_match=if_none_match,
        )
        fallback_etag = "" if getattr(native_meta, "etag", None) else f"{int(native_meta.size):016x}"
        return self._native_to_put_result(native_meta, fallback_etag)

    async def put_file(self, path: str, file_path: str | Path) -> PutResult:
        """Asynchronously read a local file and upload it to the given object key."""
        key = self._key(path)
        resolved_src = Path(file_path)
        if not resolved_src.exists():
            msg = f"Source file not found: {resolved_src}"
            raise FileNotFoundError(msg)
        native = self._require_native()
        native_meta = await native.put_file(key, str(resolved_src))
        return self._native_to_put_result(native_meta, f"{int(native_meta.size):016x}")

    async def upload_many(
        self,
        paths: Sequence[str | Path],
        destination_prefix: str = "",
        *,
        concurrency: int = 16,
    ) -> list[PutResult]:
        """Asynchronously upload local files to keys formed from `destination_prefix` and each file name.

        Raises:
            ValueError: If two files map to the same key; raised before any transfer starts.
            FileNotFoundError: If a local source file does not exist.
        """
        sources = self._upload_sources(paths, destination_prefix)
        if not sources:
            return []
        native_metas = await self._require_native().put_files(
            [(key, str(local)) for key, local in sources], concurrency=concurrency
        )
        return [self._native_to_put_result(meta, f"{int(meta.size):016x}") for meta in native_metas]

    async def put_multipart(self, path: str, parts: Iterable[Buffer]) -> PutResult:
        """Upload `parts` in order as one object through the incremental multipart path."""
        return await self.put_stream(path, parts)

    async def compose(self, sources: Sequence[str], dst: str, *, content_type: str | None = None) -> PutResult:
        """Asynchronously concatenate `sources` in order into `dst` through the backend's native compose path.

        Raises:
            ValueError: If `sources` is empty.
        """
        source_keys, dst_key = self._compose_keys(sources, dst)
        native_meta = await self._require_native().compose(source_keys, dst_key, content_type=content_type)
        return self._native_to_put_result(native_meta, f"{int(native_meta.size):016x}")

    async def put_stream(
        self,
        path: str,
        chunks: AsyncIterable[Buffer] | Iterable[Buffer],
        *,
        content_type: str | None = None,
        attributes: Mapping[str, str] | None = None,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> PutResult:
        """Asynchronously stream sync or async buffer chunks into an incremental multipart upload.

        Each chunk passes through the same buffer normalization as `put`, so typed buffers such as
        `array("I")`, NumPy arrays, and `pyarrow.Buffer` upload as their raw bytes.
        """
        key = self._key(path)
        norm_attrs = {str(k): str(v) for k, v in attributes.items()} if attributes is not None else None
        native = self._require_native()
        upload = await native.create_multipart(
            key,
            content_type=content_type,
            attributes=norm_attrs,
            if_not_exists=if_not_exists,
            if_match=if_match,
            if_none_match=if_none_match,
        )
        try:
            if isinstance(chunks, AsyncIterable):
                async for chunk in chunks:
                    await upload.write_part(self._native_payload(chunk))
            else:
                for chunk in chunks:
                    await upload.write_part(self._native_payload(chunk))
            native_meta = await upload.complete()
        except BaseException:
            with contextlib.suppress(Exception):
                await upload.abort()
            raise
        fallback_etag = "" if getattr(native_meta, "etag", None) else f"{int(native_meta.size):016x}"
        return self._native_to_put_result(native_meta, fallback_etag)

    async def get(self, path: str) -> bytes:
        """Asynchronously read an object from the store via Rust Tokio awaitable."""
        key = self._key(path)
        return bytes(await self._require_native().get(key))

    async def get_file(self, path: str, file_path: str | Path) -> ObjectMeta:
        """Asynchronously stream the object at the given key into a local file and return its metadata."""
        key = self._key(path)
        return self._native_to_object_meta(await self._require_native().get_file(key, str(Path(file_path))))

    async def download_many(self, keys: Sequence[str], destination: str | Path, *, concurrency: int = 16) -> list[Path]:
        """Asynchronously download objects into `destination`, naming each file after its key basename.

        Raises:
            ValueError: If two keys share a basename; raised before any transfer starts.
        """
        targets = self._download_targets(keys)
        if not targets:
            return []
        root = Path(destination)
        local_paths = [root / name for _, name in targets]
        root.mkdir(parents=True, exist_ok=True)
        await self._require_native().get_files(
            [(key, str(local)) for (key, _), local in zip(targets, local_paths, strict=True)],
            concurrency=concurrency,
        )
        return local_paths

    @contextlib.asynccontextmanager
    async def staged_download(
        self,
        keys: Sequence[str],
        destination: str | Path | None = None,
        *,
        concurrency: int = 16,
    ) -> AsyncIterator[list[Path]]:
        """Asynchronously download objects into a staging directory and remove the staged files on exit.

        Without `destination`, files stage in a unique directory under `/dev/shm` (when writable) or the
        system temporary directory, and that directory is removed on exit. With `destination`, the
        directory is kept and only staged paths that did not exist before the call are unlinked on exit,
        so pre-existing files with the same names survive. Cleanup runs inline on the local or tmpfs
        filesystem.

        Raises:
            ValueError: If two keys share a basename; raised before any transfer starts.
        """
        targets = self._download_targets(keys)
        owned_dir = (
            Path(tempfile.mkdtemp(prefix="vertebra-staged-", dir=self._staging_base_dir()))
            if destination is None
            else None
        )
        staging_dir = owned_dir if owned_dir is not None else Path(str(destination))
        staged = [staging_dir / name for _, name in targets]
        created = [path for path in staged if not path.exists(follow_symlinks=False)]
        try:
            if targets:
                await self.download_many([key for key, _ in targets], staging_dir, concurrency=concurrency)
            yield staged
        finally:
            self._discard_staged(created, owned_dir)

    async def get_view(self, path: str) -> memoryview:
        """Asynchronously read a zero-copy memoryview over an object payload."""
        key = self._key(path)
        return memoryview(await self._require_native().get_buffer(key))

    async def get_stream(self, path: str, *, chunk_size: int | None = None) -> AsyncIterator[StorageBuffer]:
        """Asynchronously stream object payload chunks as zero-copy StorageBuffer instances."""
        key = self._key(path)
        if chunk_size is not None and chunk_size < 1:
            msg = "chunk_size must be >= 1"
            raise ValueError(msg)
        reader = self._require_native().get_stream(key, chunk_size=chunk_size)
        try:
            async for chunk in reader:
                yield chunk
        finally:
            with contextlib.suppress(RuntimeError):
                await reader.aclose()

    async def get_range(self, path: str, offset: int, length: int) -> bytes:
        """Asynchronously read a byte range from the store via Rust Tokio awaitable."""
        key = self._key(path)
        return bytes(await self._require_native().get_range(key, offset, length))

    async def head(self, path: str) -> ObjectMeta:
        """Asynchronously read object metadata via Rust Tokio awaitable."""
        key = self._key(path)
        return self._native_to_object_meta(await self._require_native().head(key))

    async def exists(self, path: str) -> bool:
        """Asynchronously check whether an object exists via Rust Tokio awaitable."""
        key = self._key(path)
        return bool(await self._require_native().exists(key))

    async def delete(self, path: str) -> None:
        """Asynchronously delete an object via Rust Tokio awaitable."""
        key = self._key(path)
        with contextlib.suppress(FileNotFoundError):
            await self._require_native().delete(key)

    async def copy(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Asynchronously copy an object within the store via Rust Tokio awaitable.

        Raises:
            FileExistsError: If `overwrite` is False and `dst` already exists.
        """
        src_key = self._key(src)
        dst_key = self._key(dst)
        await self._require_native().copy(src_key, dst_key, if_not_exists=not overwrite)

    async def rename(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        """Asynchronously rename or move an object from src to dst within the store."""
        src_key = self._key(src)
        dst_key = self._key(dst)
        await self._require_native().rename(src_key, dst_key, if_not_exists=not overwrite)

    async def list_page(
        self,
        prefix: str = "",
        *,
        cursor: str | None = None,
        limit: int = 1000,
    ) -> tuple[list[ObjectMeta], str | None]:
        """Asynchronously return up to limit objects under prefix starting after cursor."""
        if limit < 1:
            msg = "limit must be >= 1"
            raise ValueError(msg)
        norm_prefix = self._normalize_key(prefix) if prefix else None
        items, next_cursor = await self._require_native().list_page(norm_prefix, cursor, limit)
        return [self._native_to_object_meta(item) for item in items], next_cursor

    async def list_with_delimiter(self, prefix: str = "") -> tuple[list[ObjectMeta], list[str]]:
        """Asynchronously list direct child objects and `/` common sub-prefixes beneath prefix."""
        norm_prefix = self._normalize_key(prefix) if prefix else ""
        items, prefixes = await self._require_native().list_with_delimiter(norm_prefix or None)
        return [self._native_to_object_meta(item) for item in items], [str(p) for p in prefixes]

    async def glob(self, pattern: str) -> list[ObjectMeta]:
        """Asynchronously return objects whose keys match a shell-style `*`, `?`, or `[...]` pattern.

        Literal patterns resolve with a single `head` call; wildcard patterns list the segment-aligned
        literal prefix and filter full keys with `fnmatch.fnmatchcase`. Matching is not segment-aware:
        `*` and `?` also match `/`, so `runs/*` matches nested keys such as `runs/a/b.json`. Narrow a
        match with explicit literal segments (`runs/2024/part-*.json` lists only `runs/2024/` and
        requires the `part-` and `.json` anchors), and use `list_with_delimiter` when exactly one level
        is required. Keys ending in `/` are never returned.
        """
        key_pattern, list_prefix = self._glob_query(pattern)
        if list_prefix is not None:
            return self._glob_matches(await self.list(list_prefix), key_pattern)
        if key_pattern.endswith("/"):
            return []
        try:
            return [await self.head(key_pattern)]
        except FileNotFoundError:
            return []

    async def list(self, prefix: str = "") -> list[ObjectMeta]:
        """Asynchronously list objects matching prefix via Rust Tokio awaitable."""
        norm_prefix = self._normalize_key(prefix) if prefix else ""
        items = await self._require_native().list(norm_prefix or None)
        return [
            self._native_to_object_meta(item)
            for item in items
            if not norm_prefix or str(item.path).startswith(norm_prefix)
        ]

    async def write_arrow(
        self, key: str, data: Any, *, batch_size: int = 65536, options: Mapping[str, Any] | None = None
    ) -> PutResult:
        """Write batch-bounded Arrow IPC through native multipart upload.

        ``options`` keys such as ``compression`` build ``pyarrow.ipc.IpcWriteOptions``;
        a prebuilt ``options`` entry passes through unchanged.
        """
        return await _arrow.write_batches_async(
            self, key, data, file_format="arrow-ipc", batch_size=batch_size, options=options
        )

    async def write_parquet(
        self,
        key: str,
        batches: Any,
        *,
        schema: Any = None,
        compression: str = "zstd",
        row_group_size: int = 65536,
        use_dictionary: bool | Sequence[str] = True,
        use_byte_stream_split: bool | Sequence[str] = False,
        options: Mapping[str, Any] | None = None,
    ) -> PutResult:
        """Write batch-bounded Parquet rowgroups through native multipart upload.

        ``options`` is forwarded to ``pyarrow.parquet.ParquetWriter`` and overrides
        ``compression``, ``row_group_size``, ``use_dictionary``, and ``use_byte_stream_split``.
        """
        return await _arrow.write_batches_async(
            self,
            key,
            batches,
            file_format="parquet",
            batch_size=row_group_size,
            schema=schema,
            options=self._parquet_writer_options(compression, use_dictionary, use_byte_stream_split, options),
        )

    def iter_arrow_batches(
        self, key: str, *, batch_size: int = 65536, options: Mapping[str, Any] | None = None
    ) -> AsyncIterator[Any]:
        """Yield Arrow IPC RecordBatch objects through native asynchronous streaming reads.

        ``options`` keys such as ``use_threads`` build ``pyarrow.ipc.IpcReadOptions``.
        """
        return _arrow.iter_batches_async(self, key, file_format="arrow-ipc", batch_size=batch_size, options=options)

    def iter_parquet_batches(
        self,
        key: str,
        *,
        batch_size: int = 65536,
        columns: Sequence[str] | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[Any]:
        """Yield projected Parquet batches through bounded native range reads.

        ``options`` is forwarded to ``pyarrow.parquet.ParquetFile``, except ``row_groups``
        and ``use_threads``, which select and decode rowgroups in ``iter_batches``.
        """
        return _arrow.iter_batches_async(
            self, key, file_format="parquet", batch_size=batch_size, columns=columns, options=options
        )

    def open_reader(
        self,
        path: str,
        *,
        size: int | None = None,
        chunk_size: int = 65536,
        read_ahead: int = 4_194_304,
    ) -> AsyncReadableFile:
        """Open a seekable read-only AsyncReadableFile stream over the given object key.

        Reads shorter than ``read_ahead`` are served from one cached ``read_ahead``-byte range window;
        ``read_ahead=0`` issues one range request per read.
        """
        return AsyncReadableFile(self, path, size=size, chunk_size=chunk_size, read_ahead=read_ahead)

    def open_writer(
        self,
        path: str,
        *,
        content_type: str | None = None,
        attributes: Mapping[str, str] | None = None,
        part_size: int = 8_388_608,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> AsyncWritableFile:
        """Open a write-only AsyncWritableFile that streams buffered ``part_size`` slices into a multipart upload.

        The upload is created lazily on the first full part or on close, completed by closing the
        writer, and aborted by ``abort()`` or an exception escaping the context manager.
        """
        return AsyncWritableFile(
            self,
            path,
            content_type=content_type,
            attributes=attributes,
            part_size=part_size,
            if_not_exists=if_not_exists,
            if_match=if_match,
            if_none_match=if_none_match,
        )

    async def sign(
        self,
        key: str,
        *,
        expires_in: int = 3600,
        method: str = "GET",
        timestamp: int | None = None,
    ) -> str:
        """Asynchronously generate a presigned URL for `key` without blocking the event loop."""
        return await self._presign_async(key, method=method, expires_in_secs=expires_in, timestamp=timestamp)

    async def presign_get(
        self,
        path: str,
        *,
        expires_in_secs: int = 3600,
        timestamp: int | None = None,
    ) -> str:
        """Asynchronously generate a presigned GET URL via Rust Tokio awaitable without blocking the GIL."""
        return await self._presign_async(path, method="GET", expires_in_secs=expires_in_secs, timestamp=timestamp)

    async def presign_put(
        self,
        path: str,
        *,
        expires_in_secs: int = 3600,
        timestamp: int | None = None,
    ) -> str:
        """Asynchronously generate a presigned PUT URL via Rust Tokio awaitable without blocking the GIL."""
        return await self._presign_async(path, method="PUT", expires_in_secs=expires_in_secs, timestamp=timestamp)


class SyncReadableFile(io.RawIOBase):
    """Seekable synchronous read-only file-like object backed by SyncObjectStore range and stream reads.

    Enables PyArrow, Polars, and DataFusion to inspect Parquet footer metadata and read
    targeted row-group byte slices without downloading full multi-gigabyte blobs. Reads shorter
    than ``read_ahead`` are served from one cached range window so small sequential and backward
    reads within the window cost a single ``get_range`` call.
    """

    __slots__ = ("_buf", "_buf_start", "_chunk_size", "_closed", "_key", "_pos", "_read_ahead", "_size", "_store")

    def __init__(
        self,
        store: SyncObjectStore,
        key: str,
        *,
        size: int | None = None,
        chunk_size: int = 65536,
        read_ahead: int = 4_194_304,
    ) -> None:
        super().__init__()
        if chunk_size < 1:
            msg = "chunk_size must be >= 1"
            raise ValueError(msg)
        if read_ahead < 0:
            msg = "read_ahead must be >= 0"
            raise ValueError(msg)
        self._store = store
        self._key = key
        self._size = store.head(key).size if size is None else size
        self._chunk_size = chunk_size
        self._read_ahead = read_ahead
        self._buf = b""
        self._buf_start = 0
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

    def __iter__(self) -> Iterator[bytes]:
        """Yield byte chunks from the current cursor position to EOF without buffering the full object."""
        self._ensure_open()
        if self._pos == 0:
            for buf in self._store.get_stream(self._key, chunk_size=self._chunk_size):
                self._ensure_open()
                chunk = bytes(buf)
                if chunk:
                    self._pos += len(chunk)
                    yield chunk
            return
        while True:
            chunk = self.read(self._chunk_size)
            if not chunk:
                break
            yield chunk

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
        """Move the read cursor to a new byte offset and return the updated position.

        Seeking only moves the cursor, so a cached read-ahead window stays valid for later reads.
        """
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

    def read(self, size: int | None = -1) -> bytes:
        """Read up to size bytes starting at the current cursor position."""
        self._ensure_open()
        if size == 0 or self._pos >= self._size:
            return b""
        remaining = self._size - self._pos
        length = remaining if (size is None or size < 0) else min(size, remaining)
        if length <= 0:
            return b""
        offset = self._pos - self._buf_start
        if self._read_ahead == 0 or length >= self._read_ahead:
            data = self._store.get_range(self._key, self._pos, length)
        elif offset >= 0 and offset + length <= len(self._buf):
            data = self._buf[offset : offset + length]
        else:
            self._buf = self._store.get_range(self._key, self._pos, min(self._read_ahead, remaining))
            self._buf_start = self._pos
            data = self._buf[:length]
        self._pos += len(data)
        return data

    def readinto(self, b: Buffer) -> int:
        """Read bytes directly into a writable buffer and return the byte count."""
        self._ensure_open()
        view = memoryview(b).cast("B")
        if len(view) == 0:
            return 0
        data = self.read(len(view))
        count = len(data)
        view[:count] = data
        return count

    def read_at(self, offset: int, length: int) -> bytes:
        """Read length bytes at offset without mutating the stream cursor or read-ahead window."""
        self._ensure_open()
        if offset < 0 or length < 0:
            msg = "offset and length must be non-negative"
            raise ValueError(msg)
        if offset >= self._size or length == 0:
            return b""
        bounded_len = min(length, self._size - offset)
        return self._store.get_range(self._key, offset, bounded_len)

    def close(self) -> None:
        """Mark the file handle as closed and release the read-ahead window."""
        self._closed = True
        self._buf = b""
        super().close()

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "I/O operation on closed SyncReadableFile"
            raise ValueError(msg)


class AsyncReadableFile:
    """Seekable asynchronous read-only stream backed by AsyncObjectStore range and stream reads.

    Reads shorter than ``read_ahead`` are served from one cached range window so small sequential
    and backward reads within the window cost a single ``get_range`` call.
    """

    __slots__ = ("_buf", "_buf_start", "_chunk_size", "_closed", "_key", "_pos", "_read_ahead", "_size", "_store")

    def __init__(
        self,
        store: AsyncObjectStore,
        key: str,
        *,
        size: int | None = None,
        chunk_size: int = 65536,
        read_ahead: int = 4_194_304,
    ) -> None:
        if chunk_size < 1:
            msg = "chunk_size must be >= 1"
            raise ValueError(msg)
        if read_ahead < 0:
            msg = "read_ahead must be >= 0"
            raise ValueError(msg)
        self._store = store
        self._key = key
        self._size = size
        self._chunk_size = chunk_size
        self._read_ahead = read_ahead
        self._buf = b""
        self._buf_start = 0
        self._pos = 0
        self._closed = False

    @property
    def size(self) -> int | None:
        """Return total object byte size when resolved."""
        return self._size

    @property
    def closed(self) -> bool:
        """Return True if the file handle has been closed."""
        return self._closed

    async def __aenter__(self) -> Self:
        """Enter the async context manager and return self."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the handle when exiting the async context manager."""
        _ = (exc_type, exc_val, exc_tb)
        self.close()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Asynchronously yield byte chunks from the current cursor position to EOF."""
        self._ensure_open()
        if self._pos == 0:
            async for buf in self._store.get_stream(self._key, chunk_size=self._chunk_size):
                self._ensure_open()
                chunk = bytes(buf)
                if chunk:
                    self._pos += len(chunk)
                    yield chunk
            if self._size is None:
                self._size = self._pos
            return
        while True:
            chunk = await self.read(self._chunk_size)
            if not chunk:
                break
            yield chunk

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
        """Move the read cursor to a new byte offset and return the updated position.

        Seeking only moves the cursor, so a cached read-ahead window stays valid for later reads.
        """
        self._ensure_open()
        if whence == io.SEEK_SET:
            target = offset
        elif whence == io.SEEK_CUR:
            target = self._pos + offset
        elif whence == io.SEEK_END:
            if self._size is None:
                msg = "Seeking relative to SEEK_END on AsyncReadableFile requires known size"
                raise ValueError(msg)
            target = self._size + offset
        else:
            msg = f"Invalid whence value: {whence}"
            raise ValueError(msg)
        if target < 0:
            msg = f"Negative seek position: {target}"
            raise ValueError(msg)
        if self._size is not None:
            self._pos = min(target, self._size)
        else:
            self._pos = target
        return self._pos

    async def read(self, size: int | None = -1) -> bytes:
        """Asynchronously read up to size bytes starting at the current cursor position."""
        self._ensure_open()
        if size == 0:
            return b""
        total_size = await self._ensure_size()
        if self._pos >= total_size:
            return b""
        remaining = total_size - self._pos
        length = remaining if (size is None or size < 0) else min(size, remaining)
        if length <= 0:
            return b""
        offset = self._pos - self._buf_start
        if self._read_ahead == 0 or length >= self._read_ahead:
            data = await self._store.get_range(self._key, self._pos, length)
        elif offset >= 0 and offset + length <= len(self._buf):
            data = self._buf[offset : offset + length]
        else:
            self._buf = await self._store.get_range(self._key, self._pos, min(self._read_ahead, remaining))
            self._buf_start = self._pos
            data = self._buf[:length]
        self._pos += len(data)
        return data

    async def readinto(self, b: Buffer) -> int:
        """Asynchronously read bytes into a writable buffer and return the byte count."""
        self._ensure_open()
        view = memoryview(b).cast("B")
        if len(view) == 0:
            return 0
        data = await self.read(len(view))
        count = len(data)
        view[:count] = data
        return count

    async def read_at(self, offset: int, length: int) -> bytes:
        """Asynchronously read length bytes at offset without mutating the stream cursor or read-ahead window."""
        self._ensure_open()
        if offset < 0 or length < 0:
            msg = "offset and length must be non-negative"
            raise ValueError(msg)
        if length == 0:
            return b""
        total_size = await self._ensure_size()
        if offset >= total_size:
            return b""
        bounded_len = min(length, total_size - offset)
        return await self._store.get_range(self._key, offset, bounded_len)

    def close(self) -> None:
        """Mark the file handle as closed and release the read-ahead window."""
        self._closed = True
        self._buf = b""

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "I/O operation on closed AsyncReadableFile"
            raise ValueError(msg)

    async def _ensure_size(self) -> int:
        if self._size is None:
            meta = await self._store.head(self._key)
            self._size = meta.size
        return self._size


class SyncWritableFile(io.RawIOBase):
    """Write-only synchronous stream that uploads buffered ``part_size`` slices through a native multipart upload.

    Written bytes are copied into a rolling buffer, so callers may reuse or clear their own buffers
    immediately after ``write`` returns. The multipart upload is created on the first full part or on
    close; ``close()`` uploads the remaining tail and completes the object (a writer that received no
    bytes completes a valid zero-byte object), while ``abort()`` or an exception escaping the context
    manager discards it. A writer collected without being closed aborts instead of committing a
    partial object. Inheriting ``io.RawIOBase`` lets ``gzip.GzipFile`` and ``io.BufferedWriter``
    wrap it directly.
    """

    __slots__ = (
        "_attributes",
        "_buffer",
        "_closed",
        "_content_type",
        "_if_match",
        "_if_none_match",
        "_if_not_exists",
        "_key",
        "_part_size",
        "_result",
        "_store",
        "_upload",
        "_written",
    )

    def __init__(
        self,
        store: SyncObjectStore,
        key: str,
        *,
        content_type: str | None = None,
        attributes: Mapping[str, str] | None = None,
        part_size: int = 8_388_608,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> None:
        super().__init__()
        self._closed = True
        self._upload: Any = None
        if part_size <= 0:
            msg = "part_size must be > 0"
            raise ValueError(msg)
        self._store = store
        self._key = store._key(key)
        self._content_type = content_type
        self._attributes = {str(k): str(v) for k, v in attributes.items()} if attributes is not None else None
        self._part_size = part_size
        self._if_not_exists = if_not_exists
        self._if_match = if_match
        self._if_none_match = if_none_match
        self._buffer = bytearray()
        self._written = 0
        self._result: PutResult | None = None
        self._closed = False

    @property
    def closed(self) -> bool:
        """Return True once the writer has been completed or aborted."""
        return self._closed

    @property
    def result(self) -> PutResult | None:
        """Return the completed upload metadata, or None until a successful close."""
        return self._result

    def __enter__(self) -> Self:
        """Enter the context manager and return self."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Complete the upload on success or abort it when an exception escapes, re-raising the exception."""
        _ = (exc_val, exc_tb)
        if exc_type is None:
            self.close()
        else:
            self.abort()

    def __del__(self) -> None:
        """Abort an unclosed upload on collection so a partial object is never committed."""
        if not self._closed:
            with contextlib.suppress(Exception):
                self.abort()

    def readable(self) -> bool:
        """Return False indicating this stream is write-only."""
        return False

    def seekable(self) -> bool:
        """Return False indicating this stream only appends."""
        return False

    def writable(self) -> bool:
        """Return True indicating this stream supports writing."""
        return True

    def tell(self) -> int:
        """Return the total number of bytes accepted by ``write``."""
        self._ensure_open()
        return self._written

    def write(self, b: Buffer, /) -> int:
        """Copy a buffer into the rolling part buffer, upload every full part, and return the byte count.

        A failure while creating the upload or writing a part aborts the multipart upload and closes the
        writer before re-raising, so a later ``close()`` cannot commit an object with a missing part.
        """
        self._ensure_open()
        with memoryview(b) as view:
            count = view.nbytes
            self._buffer += view
        self._written += count
        try:
            while len(self._buffer) >= self._part_size:
                self._ensure_upload().write_part(_take_part(self._buffer, self._part_size))
        except BaseException:
            self.abort()
            raise
        return count

    def close(self) -> None:
        """Upload the buffered tail and complete the multipart upload, aborting it if completion fails."""
        if self._closed:
            return
        try:
            upload = self._ensure_upload()
            if self._buffer:
                upload.write_part(_take_part(self._buffer, self._part_size))
            native_meta = upload.complete()
        except BaseException:
            self.abort()
            raise
        self._upload = None
        self._closed = True
        fallback_etag = "" if getattr(native_meta, "etag", None) else f"{int(native_meta.size):016x}"
        self._result = self._store._native_to_put_result(native_meta, fallback_etag)
        super().close()

    def abort(self) -> None:
        """Discard buffered bytes and abort the multipart upload without committing an object."""
        if self._closed:
            return
        self._closed = True
        self._buffer = bytearray()
        upload, self._upload = self._upload, None
        if upload is not None:
            with contextlib.suppress(Exception):
                upload.abort()
        super().close()

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "I/O operation on closed SyncWritableFile"
            raise ValueError(msg)

    def _ensure_upload(self) -> Any:
        if self._upload is None:
            self._upload = self._store._require_native().create_multipart(
                self._key,
                content_type=self._content_type,
                attributes=self._attributes,
                if_not_exists=self._if_not_exists,
                if_match=self._if_match,
                if_none_match=self._if_none_match,
            )
        return self._upload


class AsyncWritableFile:
    """Write-only asynchronous stream that uploads buffered ``part_size`` slices through a native multipart upload.

    Written bytes are copied into a rolling buffer, so callers may reuse or clear their own buffers
    immediately after ``await write(...)`` returns. The multipart upload is created on the first full
    part or on close; ``aclose()`` uploads the remaining tail and completes the object (a writer that
    received no bytes completes a valid zero-byte object), while ``abort()`` or an exception escaping
    ``async with`` discards it. A writer collected without being closed or aborted emits a
    ``ResourceWarning`` because a finalizer cannot await the native abort.
    """

    __slots__ = (
        "_attributes",
        "_buffer",
        "_closed",
        "_content_type",
        "_if_match",
        "_if_none_match",
        "_if_not_exists",
        "_key",
        "_part_size",
        "_result",
        "_store",
        "_upload",
        "_written",
    )

    def __init__(
        self,
        store: AsyncObjectStore,
        key: str,
        *,
        content_type: str | None = None,
        attributes: Mapping[str, str] | None = None,
        part_size: int = 8_388_608,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> None:
        self._closed = True
        self._upload: Any = None
        if part_size <= 0:
            msg = "part_size must be > 0"
            raise ValueError(msg)
        self._store = store
        self._key = store._key(key)
        self._content_type = content_type
        self._attributes = {str(k): str(v) for k, v in attributes.items()} if attributes is not None else None
        self._part_size = part_size
        self._if_not_exists = if_not_exists
        self._if_match = if_match
        self._if_none_match = if_none_match
        self._buffer = bytearray()
        self._written = 0
        self._result: PutResult | None = None
        self._closed = False

    @property
    def closed(self) -> bool:
        """Return True once the writer has been completed or aborted."""
        return self._closed

    @property
    def result(self) -> PutResult | None:
        """Return the completed upload metadata, or None until a successful close."""
        return self._result

    async def __aenter__(self) -> Self:
        """Enter the async context manager and return self."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Complete the upload on success or abort it when an exception escapes, re-raising the exception."""
        _ = (exc_val, exc_tb)
        if exc_type is None:
            await self.aclose()
        else:
            await self.abort()

    def __del__(self) -> None:
        """Warn when an open writer is collected, since a finalizer cannot await the native abort.

        The buffered bytes are never committed, and a multipart upload that was already created stays
        incomplete on the backend until its lifecycle policy expires it.
        """
        if not self._closed:
            warnings.warn(
                f"AsyncWritableFile for {self._key!r} was garbage collected without aclose() or abort(); "
                "the multipart upload was never completed or aborted",
                ResourceWarning,
                stacklevel=2,
                source=self,
            )

    def readable(self) -> bool:
        """Return False indicating this stream is write-only."""
        return False

    def seekable(self) -> bool:
        """Return False indicating this stream only appends."""
        return False

    def writable(self) -> bool:
        """Return True indicating this stream supports writing."""
        return True

    def tell(self) -> int:
        """Return the total number of bytes accepted by ``write``."""
        self._ensure_open()
        return self._written

    async def write(self, b: Buffer, /) -> int:
        """Copy a buffer into the rolling part buffer, upload every full part, and return the byte count.

        A failure or cancellation while creating the upload or writing a part aborts the multipart upload
        and closes the writer before re-raising, so a later ``aclose()`` cannot commit an object with a
        missing part.
        """
        self._ensure_open()
        with memoryview(b) as view:
            count = view.nbytes
            self._buffer += view
        self._written += count
        try:
            while len(self._buffer) >= self._part_size:
                upload = await self._ensure_upload()
                await upload.write_part(_take_part(self._buffer, self._part_size))
        except BaseException:
            await self.abort()
            raise
        return count

    async def flush(self) -> None:
        """Validate that the writer is open; buffered bytes stay pending until a full part or ``aclose()``.

        ``write`` already uploads every full part, and multipart backends reject undersized non-final
        parts, so the remaining tail is held back exactly as ``SyncWritableFile.flush`` does.
        """
        self._ensure_open()

    async def aclose(self) -> PutResult | None:
        """Upload the buffered tail, complete the multipart upload, and return its metadata.

        Completion failures abort the upload and re-raise. A second call returns the stored ``result``;
        a writer that was aborted returns None because no object was committed.
        """
        if self._closed:
            return self._result
        try:
            upload = await self._ensure_upload()
            if self._buffer:
                await upload.write_part(_take_part(self._buffer, self._part_size))
            native_meta = await upload.complete()
        except BaseException:
            await self.abort()
            raise
        self._upload = None
        self._closed = True
        fallback_etag = "" if getattr(native_meta, "etag", None) else f"{int(native_meta.size):016x}"
        self._result = self._store._native_to_put_result(native_meta, fallback_etag)
        return self._result

    async def abort(self) -> None:
        """Discard buffered bytes and abort the multipart upload without committing an object."""
        if self._closed:
            return
        self._closed = True
        self._buffer = bytearray()
        upload, self._upload = self._upload, None
        if upload is not None:
            with contextlib.suppress(Exception):
                await upload.abort()

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "I/O operation on closed AsyncWritableFile"
            raise ValueError(msg)

    async def _ensure_upload(self) -> Any:
        if self._upload is None:
            self._upload = await self._store._require_native().create_multipart(
                self._key,
                content_type=self._content_type,
                attributes=self._attributes,
                if_not_exists=self._if_not_exists,
                if_match=self._if_match,
                if_none_match=self._if_none_match,
            )
        return self._upload


def _take_part(buffer: bytearray, part_size: int) -> bytes:
    """Copy the next part out of a writer's rolling buffer as immutable bytes and drop it from the buffer.

    Shared by `SyncWritableFile` and `AsyncWritableFile`. Returning ``bytes`` keeps the native upload
    zero-copy over an object the writer never mutates.
    """
    size = min(part_size, len(buffer))
    with memoryview(buffer) as view:
        part = bytes(view[:size])
    del buffer[:size]
    return part
