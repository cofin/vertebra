"""Object storage backend configuration and metadata value structs."""

from __future__ import annotations

import os
from collections.abc import Buffer, Sequence
from datetime import datetime
from pathlib import Path
from typing import Final, Literal

import msgspec

from vertebra._vertebra import AsyncGcpTokenCache, GoogleCloudAuthError, SyncGcpTokenCache
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json

__all__ = (
    "ArtifactReference",
    "AzureConfig",
    "GCSConfig",
    "GCS_DEFAULT_SCOPES",
    "HTTPConfig",
    "LocalConfig",
    "MemoryConfig",
    "OCIConfig",
    "ObjectMeta",
    "PutResult",
    "R2Config",
    "S3Config",
    "StorageConfig",
)

GCS_DEFAULT_SCOPES: Final[tuple[str, str]] = (
    "https://www.googleapis.com/auth/devstorage.full_control",
    "https://www.googleapis.com/auth/cloud-platform",
)

_EXPLICIT_GCS_CREDENTIAL_KEYS: Final[tuple[str, ...]] = (
    "service_account_path",
    "service_account_key",
    "application_credentials_path",
    "application_credentials",
)


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
        if self.sas_token is not None:
            opts["sas_token"] = self.sas_token
        if self.tenant_id is not None:
            opts["tenant_id"] = self.tenant_id
        if self.client_id is not None:
            opts["client_id"] = self.client_id
        if self.client_secret is not None:
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
        if self.tenancy_ocid is not None:
            opts["tenancy_ocid"] = self.tenancy_ocid
        if self.user_ocid is not None:
            opts["user_ocid"] = self.user_ocid
        if self.fingerprint is not None:
            opts["fingerprint"] = self.fingerprint
        pem = self.private_key_pem
        if pem is None and self.private_key_path is not None:
            pem = Path(self.private_key_path).read_text(encoding="utf-8")
        if pem is not None:
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
        native_mod = import_optional("vertebra._storage")
        if native_mod is None:
            raise MissingDependencyError("vertebra._storage", install_package="vertebra-storage", extra="storage")
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


class ObjectMeta(msgspec.Struct, frozen=True, kw_only=True):
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
    sha256: str | None = None
    """Optional lowercase hexadecimal SHA-256 content digest receipt."""
    crc32c: str | None = None
    """Optional lowercase hexadecimal Castagnoli CRC32C content digest receipt."""
    md5: str | None = None
    """Optional lowercase hexadecimal MD5 content digest receipt."""


class PutResult(ObjectMeta, frozen=True, kw_only=True):
    """Result metadata returned from a write operation, carrying sha256/crc32c/md5 receipts when requested."""


class ArtifactReference(msgspec.Struct, frozen=True):
    """Immutable reference describing a persisted storage artifact and its version metadata."""

    uri: str
    """Normalized URI identifying the stored artifact."""
    version: int
    """Monotonic version number of the stored artifact."""
    content_type: str | None = None
    """Optional MIME content type of the stored artifact."""
    size: int | None = None
    """Optional payload size in bytes."""
    updated: datetime | None = None
    """Optional UTC timestamp recording when this version was stored."""
    signed_url: str | None = None
    """Optional time-bounded presigned URL for reading the artifact."""
