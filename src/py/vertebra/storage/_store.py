"""Zero-copy Python object storage facade over vertebra._storage."""

from __future__ import annotations

import base64
import binascii
import builtins
import calendar
import datetime
import hashlib
import hmac
import io
import os
import shutil
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Buffer, Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar, Literal, Self
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import msgspec

from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "AzureConfig",
    "GCSConfig",
    "GcpTokenCache",
    "GcpTokenSnapshot",
    "HTTPConfig",
    "LocalConfig",
    "MemoryConfig",
    "OCIConfig",
    "ObjectMeta",
    "ObjectStore",
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

    def to_url_and_options(self, *, eager_impersonation: bool = True) -> tuple[str, dict[str, str]]:
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

        resolved_token = self.access_token
        if resolved_token is None and self.target_service_account and eager_impersonation:
            resolved_token = self.resolve_adc_token()
        if resolved_token:
            opts["access_token"] = resolved_token

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
            for env_key in ("NO_PROXY", "no_proxy"):
                existing_no_proxy = os.environ.get(env_key, "")
                if "127.0.0.1" not in existing_no_proxy:
                    os.environ[env_key] = (
                        f"{existing_no_proxy},127.0.0.1,localhost,::1,metadata.google.internal"
                        if existing_no_proxy
                        else "127.0.0.1,localhost,::1,metadata.google.internal"
                    )

        has_explicit_auth = bool(
            self.service_account_path
            or self.service_account_key
            or self.application_credentials
            or resolved_token
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

    def resolve_adc_token(
        self,
        *,
        scopes: Sequence[str] = (
            "https://www.googleapis.com/auth/devstorage.full_control",
            "https://www.googleapis.com/auth/cloud-platform",
        ),
    ) -> str | None:
        """Resolve an OAuth2 bearer token via Application Default Credentials or service-account impersonation."""
        token, _, _ = self._fetch_adc_token_with_expiry(scopes=scopes)
        return token

    def _fetch_adc_token_with_expiry(
        self,
        *,
        scopes: Sequence[str] = (
            "https://www.googleapis.com/auth/devstorage.full_control",
            "https://www.googleapis.com/auth/cloud-platform",
        ),
        allow_ambient_adc: bool = True,
    ) -> tuple[str | None, int, str | None]:
        """Resolve `(access_token, expires_at_unix_ms, quota_project_id)` across ADC sources."""
        now_ms = int(time.time() * 1000)
        if self.access_token:
            return self.access_token, now_ms + 3_600_000, None

        source_token, expires_at_ms, quota_project = self._exchange_authorized_user_token_with_expiry(
            allow_ambient=allow_ambient_adc
        )
        if source_token is not None:
            if self.target_service_account:
                return self._impersonate_service_account_token_with_expiry(
                    source_token,
                    scopes=scopes,
                    quota_project_id=quota_project,
                )
            return source_token, expires_at_ms, quota_project

        google_auth = import_optional("google.auth")
        google_requests = import_optional("google.auth.transport.requests")
        if google_auth is not None and google_requests is not None:
            try:
                creds: Any = None
                if self.service_account_key:
                    sa_mod = import_optional("google.oauth2.service_account")
                    if sa_mod is not None:
                        info = decode_json(self.service_account_key)
                        creds = sa_mod.Credentials.from_service_account_info(info, scopes=list(scopes))
                elif self.service_account_path and Path(self.service_account_path).is_file():
                    creds, _ = google_auth.load_credentials_from_file(
                        self.service_account_path,
                        scopes=list(scopes),
                    )
                elif self.application_credentials and Path(self.application_credentials).is_file():
                    creds, _ = google_auth.load_credentials_from_file(
                        self.application_credentials,
                        scopes=list(scopes),
                    )
                elif allow_ambient_adc:
                    creds, _ = google_auth.default(scopes=list(scopes))

                if creds is not None and self.target_service_account:
                    impersonated_mod = import_optional("google.auth.impersonated_credentials")
                    if impersonated_mod is not None:
                        creds = impersonated_mod.Credentials(
                            source_credentials=creds,
                            target_principal=self.target_service_account,
                            target_scopes=list(scopes),
                        )

                if creds is not None:
                    if not getattr(creds, "valid", False) or not getattr(creds, "token", None):
                        creds.refresh(google_requests.Request())
                    token_val = getattr(creds, "token", None)
                    if token_val:
                        expiry_dt = getattr(creds, "expiry", None)
                        exp_ms = (
                            int(expiry_dt.timestamp() * 1000)
                            if isinstance(expiry_dt, datetime.datetime)
                            else int(time.time() * 1000) + 3_600_000
                        )
                        qp_val = getattr(creds, "quota_project_id", None)
                        return str(token_val), exp_ms, str(qp_val) if qp_val else None
            except Exception:
                pass

        meta_token, meta_exp_ms = self._fetch_metadata_server_token_with_expiry()
        if meta_token and self.target_service_account:
            return self._impersonate_service_account_token_with_expiry(
                meta_token,
                scopes=scopes,
                quota_project_id=None,
            )
        return meta_token, meta_exp_ms, None

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

    def _exchange_authorized_user_token(self) -> str | None:
        """Exchange an ADC `authorized_user` refresh token for a short-lived access token."""
        token, _, _ = self._exchange_authorized_user_token_with_expiry()
        return token

    def _exchange_authorized_user_token_with_expiry(
        self,
        *,
        allow_ambient: bool = True,
    ) -> tuple[str | None, int, str | None]:
        """Exchange an ADC `authorized_user` refresh token and return `(token, expires_at_ms, quota_project_id)`."""
        adc_file = self._resolve_adc_file(allow_ambient=allow_ambient)
        if adc_file is None:
            return None, 0, None
        try:
            payload = decode_json(adc_file.read_bytes())
        except (OSError, ValueError, TypeError, msgspec.DecodeError):
            return None, 0, None
        if not isinstance(payload, dict) or payload.get("type") != "authorized_user":
            return None, 0, None
        client_id = payload.get("client_id")
        client_secret = payload.get("client_secret")
        refresh_token = payload.get("refresh_token")
        if not (client_id and client_secret and refresh_token):
            return None, 0, None
        token_uri = str(payload.get("token_uri") or "https://oauth2.googleapis.com/token")
        form_bytes = urlencode(
            {
                "client_id": str(client_id),
                "client_secret": str(client_secret),
                "refresh_token": str(refresh_token),
                "grant_type": "refresh_token",
            }
        ).encode("utf-8")
        headers: dict[str, str] = {"Content-Type": "application/x-www-form-urlencoded"}
        quota_project_raw = payload.get("quota_project_id")
        quota_project = str(quota_project_raw) if quota_project_raw else None
        if quota_project:
            headers["x-goog-user-project"] = quota_project
        now_ms = int(time.time() * 1000)
        resp_data = self._open_http_json(token_uri, method="POST", body=form_bytes, headers=headers, timeout=5.0)
        if isinstance(resp_data, dict) and resp_data.get("access_token"):
            ttl_secs = int(resp_data.get("expires_in") or 3600)
            return str(resp_data["access_token"]), now_ms + max(0, ttl_secs) * 1000, quota_project
        return None, 0, None

    @classmethod
    def _fetch_metadata_server_token(cls) -> str | None:
        """Query the GCE / Cloud Run / GKE metadata server for an ambient service account token."""
        token, _ = cls._fetch_metadata_server_token_with_expiry()
        return token

    @classmethod
    def _fetch_metadata_server_token_with_expiry(cls) -> tuple[str | None, int]:
        """Query the metadata server and return `(access_token, expires_at_unix_ms)`."""
        metadata_host = os.environ.get("GCE_METADATA_HOST", "metadata.google.internal").strip()
        base = metadata_host if metadata_host.startswith(("http://", "https://")) else f"http://{metadata_host}"
        url = f"{base.rstrip('/')}/computeMetadata/v1/instance/service-accounts/default/token"
        now_ms = int(time.time() * 1000)
        resp_data = cls._open_http_json(
            url,
            method="GET",
            body=None,
            headers={"Metadata-Flavor": "Google"},
            timeout=1.5,
        )
        if isinstance(resp_data, dict) and resp_data.get("access_token"):
            ttl_secs = int(resp_data.get("expires_in") or 3600)
            return str(resp_data["access_token"]), now_ms + max(0, ttl_secs) * 1000
        return None, 0

    def _impersonate_service_account_token(self, source_token: str, *, scopes: Sequence[str]) -> str | None:
        """Exchange a source bearer token for an impersonated service account access token via IAM Credentials."""
        token, _, _ = self._impersonate_service_account_token_with_expiry(
            source_token,
            scopes=scopes,
            quota_project_id=None,
        )
        return token

    def _impersonate_service_account_token_with_expiry(
        self,
        source_token: str,
        *,
        scopes: Sequence[str],
        quota_project_id: str | None,
    ) -> tuple[str | None, int, str | None]:
        """Exchange a source token for an impersonated service account token with expiration timestamp."""
        now_ms = int(time.time() * 1000)
        if not self.target_service_account:
            return source_token, now_ms + 3_600_000, quota_project_id
        iam_url = (
            f"{self.iam_endpoint.rstrip('/')}/v1/projects/-/serviceAccounts/"
            f"{self.target_service_account}:generateAccessToken"
        )
        body = bytes(encode_json_bytes({"scope": list(scopes)}))
        resp_data = self._open_http_json(
            iam_url,
            method="POST",
            body=body,
            headers={
                "Authorization": f"Bearer {source_token}",
                "Content-Type": "application/json",
            },
            timeout=5.0,
        )
        if isinstance(resp_data, dict) and resp_data.get("accessToken"):
            exp_ms = now_ms + 3_600_000
            expire_str = resp_data.get("expireTime")
            if isinstance(expire_str, str) and expire_str:
                try:
                    dt = datetime.datetime.fromisoformat(expire_str)
                    exp_ms = int(dt.timestamp() * 1000)
                except ValueError:
                    pass
            return str(resp_data["accessToken"]), exp_ms, quota_project_id
        return None, 0, quota_project_id

    @staticmethod
    def _open_http_json(
        url: str,
        *,
        method: str,
        body: bytes | None,
        headers: dict[str, str],
        timeout: float,
    ) -> Any:
        """Execute an HTTP request and decode a JSON response payload, returning None on transport failure."""
        if not url.startswith(("http://", "https://")):
            return None
        parsed = urlparse(url)
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        opener = (
            urllib.request.build_opener(urllib.request.ProxyHandler({}))
            if parsed.hostname in {"127.0.0.1", "localhost", "::1", "metadata.google.internal"}
            else urllib.request.build_opener()
        )
        try:
            with opener.open(req, timeout=timeout) as resp:
                return decode_json(resp.read())
        except (OSError, urllib.error.URLError, ValueError, TypeError, msgspec.DecodeError):
            return None


class GcpTokenSnapshot(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Immutable snapshot of a cached Google Cloud OAuth2 access token and its generation."""

    access_token: str
    """Short-lived OAuth2 bearer access token."""
    expires_at_unix_ms: int
    """Token expiration timestamp in Unix epoch milliseconds."""
    generation: int
    """Monotonically increasing refresh generation counter."""
    quota_project_id: str | None = None
    """Optional Google Cloud quota project ID associated with the credential."""


class GcpTokenCache:
    """Thread-safe and async-aware Google Cloud ADC token cache backed by Rust Tokio off-GIL refresh."""

    __slots__ = (
        "_allow_ambient_adc",
        "_config",
        "_generation",
        "_lock",
        "_native",
        "_refresh_lock",
        "_scopes",
        "_snapshot",
    )

    def __init__(
        self,
        config: GCSConfig | Mapping[str, str] | None = None,
        *,
        allow_ambient_adc: bool = True,
        scopes: Sequence[str] | None = None,
    ) -> None:
        raw_opts: dict[str, str] | None = None
        if isinstance(config, Mapping):
            raw_opts = {str(k): str(v) for k, v in config.items()}
            resolved_config = GCSConfig(
                bucket=raw_opts.get("bucket", ""),
                service_account_path=raw_opts.get("service_account_path"),
                service_account_key=raw_opts.get("service_account_key"),
                application_credentials=(
                    raw_opts.get("application_credentials_path") or raw_opts.get("application_credentials")
                ),
                service_account_email=raw_opts.get("service_account_email"),
                target_service_account=raw_opts.get("target_service_account"),
                iam_endpoint=raw_opts.get("iam_endpoint", "https://iamcredentials.googleapis.com"),
                access_token=raw_opts.get("access_token"),
                quota_project_id=raw_opts.get("quota_project_id"),
            )
        elif config is None:
            resolved_config = GCSConfig(bucket="")
        else:
            resolved_config = config
        self._config = resolved_config
        self._allow_ambient_adc = allow_ambient_adc
        self._scopes: tuple[str, ...] | None = tuple(scopes) if scopes is not None else None
        self._generation = 0
        self._snapshot: GcpTokenSnapshot | None = None
        self._lock = threading.RLock()
        self._refresh_lock = threading.Lock()
        self._native: Any = None
        if raw_opts is not None:
            opts = dict(raw_opts)
            if self._scopes:
                opts["scopes"] = ",".join(self._scopes)
        else:
            opts = {
                "refresh_skew_ms": str(max(0, int(resolved_config.refresh_skew_seconds * 1000))),
                "iam_endpoint": resolved_config.iam_endpoint,
                "allow_ambient_adc": "true" if allow_ambient_adc else "false",
            }
            if resolved_config.service_account_path:
                opts["service_account_path"] = resolved_config.service_account_path
            if resolved_config.service_account_key:
                opts["service_account_key"] = resolved_config.service_account_key
            if resolved_config.application_credentials:
                opts["application_credentials_path"] = resolved_config.application_credentials
            if resolved_config.target_service_account:
                opts["target_service_account"] = resolved_config.target_service_account
            if resolved_config.access_token:
                opts["access_token"] = resolved_config.access_token
            if resolved_config.quota_project_id:
                opts["quota_project_id"] = resolved_config.quota_project_id
            if self._scopes:
                opts["scopes"] = ",".join(self._scopes)
            if not (
                resolved_config.application_credentials
                or resolved_config.service_account_path
                or resolved_config.service_account_key
                or resolved_config.access_token
            ):
                opts["use_metadata_server"] = "true"
        for candidate_mod in (import_optional("vertebra._vertebra"), ObjectStore._get_native_module()):
            native_cls = getattr(candidate_mod, "GcpTokenCache", None) if candidate_mod is not None else None
            if native_cls is None:
                continue
            try:
                self._native = native_cls(opts)
                break
            except (TypeError, RuntimeError, ValueError, OSError):
                self._native = None

    @property
    def generation(self) -> int:
        """Return the current token generation counter (`0` prior to initial fetch)."""
        if self._native is not None:
            return int(self._native.generation)
        with self._lock:
            return self._generation

    @property
    def refresh_skew_ms(self) -> int:
        """Return the proactive refresh skew window in milliseconds."""
        if self._native is not None:
            return int(self._native.refresh_skew_ms)
        return max(0, int(self._config.refresh_skew_seconds * 1000))

    @property
    def has_service_account_key(self) -> bool:
        """Return True when backed by a Google Cloud Service Account RSA private key."""
        if self._native is not None and hasattr(self._native, "has_service_account_key"):
            return bool(self._native.has_service_account_key)
        _, pem = self._config.extract_service_account_identity(allow_ambient=self._allow_ambient_adc)
        return pem is not None

    @property
    def service_account_email(self) -> str | None:
        """Return the Service Account client_email when backed by a Service Account key."""
        if self._native is not None and hasattr(self._native, "service_account_email"):
            val = self._native.service_account_email
            return str(val) if val else None
        email, _ = self._config.extract_service_account_identity(allow_ambient=self._allow_ambient_adc)
        return email

    @classmethod
    def from_gcs_config(
        cls,
        config: GCSConfig,
        *,
        allow_ambient_adc: bool = True,
        scopes: Sequence[str] | None = None,
    ) -> GcpTokenCache:
        """Construct a `GcpTokenCache` bound to a `GCSConfig`."""
        return cls(config, allow_ambient_adc=allow_ambient_adc, scopes=scopes)

    def needs_refresh(self, *, now_unix_ms: int | None = None) -> bool:
        """Return True when no token is cached or the cached token is within the proactive skew window."""
        if self._native is not None:
            return bool(self._native.needs_refresh(now_unix_ms=now_unix_ms))
        resolved_now = int(time.time() * 1000) if now_unix_ms is None else int(now_unix_ms)
        with self._lock:
            if self._snapshot is None:
                return True
            return resolved_now + self.refresh_skew_ms >= self._snapshot.expires_at_unix_ms

    def invalidate(self, failed_generation: int) -> None:
        """Invalidate the cached token if its generation matches `failed_generation`."""
        if self._native is not None:
            self._native.invalidate(int(failed_generation))
            return
        with self._lock:
            if self._snapshot is not None and self._snapshot.generation == failed_generation:
                self._snapshot = None

    def sign_jwt_for_audience(self, audience: str) -> str:
        """Sign a self-issued RS256 JWT assertion for the given target audience URI."""
        if self._native is not None and hasattr(self._native, "sign_jwt_for_audience"):
            return str(self._native.sign_jwt_for_audience(audience))
        msg = "GcpTokenCache requires native RSA support and a service_account key for sign_jwt_for_audience"
        raise ValueError(msg)

    def get_token(self, *, force_refresh: bool = False) -> GcpTokenSnapshot:
        """Return a valid `GcpTokenSnapshot`, refreshing off-GIL under a single-flight lock when needed."""
        if self._native is not None:
            raw_snap = self._native.get_token(force_refresh=force_refresh)
            return GcpTokenSnapshot(
                access_token=str(raw_snap.access_token),
                expires_at_unix_ms=int(raw_snap.expires_at_unix_ms),
                generation=int(raw_snap.generation),
                quota_project_id=raw_snap.quota_project_id,
            )

        if not force_refresh and not self.needs_refresh():
            with self._lock:
                if self._snapshot is not None:
                    return self._snapshot

        with self._refresh_lock:
            if not force_refresh and not self.needs_refresh():
                with self._lock:
                    if self._snapshot is not None:
                        return self._snapshot

            fetch_kwargs: dict[str, Any] = {"allow_ambient_adc": self._allow_ambient_adc}
            if self._scopes is not None:
                fetch_kwargs["scopes"] = self._scopes
            token, expires_at_ms, quota_project = self._config._fetch_adc_token_with_expiry(**fetch_kwargs)
            if not token:
                msg = f"Failed to resolve Google Cloud ADC access token for bucket '{self._config.bucket}'"
                raise RuntimeError(msg)

            with self._lock:
                self._generation += 1
                snapshot = GcpTokenSnapshot(
                    access_token=token,
                    expires_at_unix_ms=expires_at_ms,
                    generation=self._generation,
                    quota_project_id=quota_project,
                )
                self._snapshot = snapshot
                return snapshot

    async def get_token_async(self, *, force_refresh: bool = False) -> GcpTokenSnapshot:
        """Asynchronously return a valid `GcpTokenSnapshot` via Rust Tokio awaitable without blocking the GIL."""
        if self._native is not None:
            raw_snap = await self._native.get_token_async(force_refresh=force_refresh)
            return GcpTokenSnapshot(
                access_token=str(raw_snap.access_token),
                expires_at_unix_ms=int(raw_snap.expires_at_unix_ms),
                generation=int(raw_snap.generation),
                quota_project_id=raw_snap.quota_project_id,
            )
        if not force_refresh and not self.needs_refresh():
            with self._lock:
                if self._snapshot is not None:
                    return self._snapshot
        return self.get_token(force_refresh=force_refresh)


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
    """Optional public custom domain bound to the R2 bucket."""
    endpoint: str | None = None
    """Optional explicit R2 endpoint URL override."""
    allow_http: bool = False
    """When True, permit unencrypted `http://` connections for local testing."""

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


class HTTPConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for HTTP/WebDAV object storage."""

    url: str
    """Base `http://` or `https://` origin URL for the HTTP/WebDAV store."""
    headers: dict[str, str] | None = None
    """Optional static HTTP headers included on every outbound storage request."""

    def to_url_and_options(self) -> tuple[str, dict[str, str]]:
        """Convert configuration into a canonical URL and options dictionary."""
        return self.url, dict(self.headers or {})


class MemoryConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for in-memory object storage."""

    namespace: str = "default"
    """Logical namespace isolating in-memory object buckets inside the process."""

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
    last_modified: str
    """RFC 3339 / ISO-8601 UTC timestamp of the object's last modification."""
    etag: str | None = None
    """Optional content entity tag (`ETag`) returned by the storage backend."""
    version: str | None = None
    """Optional backend object version identifier."""


class PutResult(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Result metadata returned from a write operation."""

    path: str
    """Normalized `/`-delimited object key written to the store."""
    size: int
    """Number of payload bytes persisted."""
    etag: str | None = None
    """Content entity tag (`ETag`) assigned to the written object."""
    version: str | None = None
    """Optional object version identifier created by the write operation."""


class SigningConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for HMAC-SHA256, S3 SigV4, Azure SAS, and GCS V4 presigned URL generation."""

    provider: Literal["hmac", "s3", "gcs", "azure"] = "hmac"
    """URL signing scheme (`"hmac"`, `"s3"`, `"gcs"`, or `"azure"`)."""
    bucket: str = ""
    """Target bucket name embedded in S3 SigV4 or GCS V4 presigned URLs."""
    region: str = "us-east-1"
    """Region identifier used in the S3 SigV4 credential scope."""
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
    endpoint: str | None = None
    """Optional custom origin URL overriding the default cloud storage hostname."""

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
    __slots__ = (
        "_gcp_token_cache",
        "_local_root",
        "_memory",
        "_native",
        "_options",
        "_scheme",
        "_signing",
        "_url",
    )

    def __init__(
        self,
        url: str | StorageConfig = "memory://",
        *,
        options: dict[str, str] | None = None,
        signing: SigningConfig | None = None,
        token_cache: GcpTokenCache | None = None,
    ) -> None:
        if not isinstance(url, str):
            if isinstance(url, GCSConfig):
                resolved_url, cfg_opts = url.to_url_and_options(eager_impersonation=False)
            else:
                resolved_url, cfg_opts = url.to_url_and_options()
            merged_opts = dict(cfg_opts)
            if options:
                merged_opts.update(options)
            signing = signing if signing is not None else self._derive_signing_from_config(url)
            url = resolved_url
            options = merged_opts
        self._url = url
        self._options = dict(options or {})
        self._signing = signing
        self._gcp_token_cache = token_cache
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
                self._native = native_mod.ObjectStore(url, self._options or None)
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

    @property
    def token_cache(self) -> GcpTokenCache | None:
        """Return the active Google Cloud OAuth2 token cache bound to this store, if any."""
        return self._gcp_token_cache

    @classmethod
    def from_config(
        cls,
        config: StorageConfig,
        *,
        signing: SigningConfig | None = None,
        token_cache: GcpTokenCache | None = None,
    ) -> ObjectStore:
        """Construct an ObjectStore from any provider StorageConfig."""
        if isinstance(config, GCSConfig):
            url, options = config.to_url_and_options(eager_impersonation=False)
        else:
            url, options = config.to_url_and_options()
        resolved_signing = signing if signing is not None else cls._derive_signing_from_config(config)
        return cls(url, options=options, signing=resolved_signing, token_cache=token_cache)

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
        token_cache: GcpTokenCache | None = None,
    ) -> ObjectStore:
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
            ),
            signing=signing,
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
            OCIConfig(
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
        signing: SigningConfig | None = None,
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
        return cls.from_config(cfg, signing=signing)

    @classmethod
    def from_http(cls, url: str, *, headers: dict[str, str] | None = None) -> ObjectStore:
        """Construct an ObjectStore backed by HTTP/WebDAV."""
        return cls.from_config(HTTPConfig(url=url, headers=headers))

    @classmethod
    def from_memory(cls, namespace: str = "default") -> ObjectStore:
        """Construct an in-memory ObjectStore."""
        return cls.from_config(MemoryConfig(namespace=namespace))

    def put(self, path: str, data: Buffer) -> PutResult:
        """Write any Buffer (bytes, bytearray, memoryview) to the given object key."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        view = memoryview(data).cast("B")
        if self._native is not None:
            native_meta = self._native.put(key, view)
            fallback_etag = "" if getattr(native_meta, "etag", None) else f"{view.nbytes:016x}"
            return self._native_to_put_result(native_meta, fallback_etag)
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
            fallback_etag = "" if getattr(native_meta, "etag", None) else f"{view.nbytes:016x}"
            return self._native_to_put_result(native_meta, fallback_etag)
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

    async def put_multipart_async(self, path: str, parts: Sequence[Buffer]) -> PutResult:
        """Asynchronously upload multiple buffer parts atomically."""
        key = self._normalize_key(path)
        if self._local_root is not None:
            self._resolve_local(key)
        if self._native is not None and hasattr(self._native, "put_multipart_async"):
            views = [memoryview(part).cast("B") for part in parts]
            native_meta = await self._native.put_multipart_async(key, views)
            etag = native_meta.etag or f"{int(native_meta.size):016x}"
            return PutResult(
                path=str(native_meta.path),
                size=int(native_meta.size),
                etag=str(etag),
                version=native_meta.version,
            )
        return self.put_multipart(path, parts)

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

    async def presign_get_async(
        self,
        path: str,
        *,
        expires_in_secs: int = 3600,
        timestamp: int | None = None,
    ) -> str:
        """Asynchronously generate a presigned GET URL via Rust Tokio awaitable without blocking the GIL."""
        return await self._presign_async(path, method="GET", expires_in_secs=expires_in_secs, timestamp=timestamp)

    def presign_put(
        self,
        path: str,
        *,
        expires_in_secs: int = 3600,
        timestamp: int | None = None,
    ) -> str:
        """Generate a presigned PUT URL for the given object key."""
        return self._presign(path, method="PUT", expires_in_secs=expires_in_secs, timestamp=timestamp)

    async def presign_put_async(
        self,
        path: str,
        *,
        expires_in_secs: int = 3600,
        timestamp: int | None = None,
    ) -> str:
        """Asynchronously generate a presigned PUT URL via Rust Tokio awaitable without blocking the GIL."""
        return await self._presign_async(path, method="PUT", expires_in_secs=expires_in_secs, timestamp=timestamp)

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
        if self._signing.provider in {"gcs", "azure"}:
            msg = f"{self._signing.provider.upper()} presigned URL verification is not supported offline"
            raise NotImplementedError(msg)
        if self._signing.provider == "s3":
            return self._verify_s3_v4(signed_url, method=method, now_ts=now_ts)
        return self._verify_hmac(signed_url, method=method, now_ts=now_ts)

    @property
    def supports_signing(self) -> bool:
        """Return True if a SigningConfig is attached to this ObjectStore."""
        return self._signing is not None

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
        return None

    @staticmethod
    def _native_to_put_result(native_meta: Any, fallback_etag: str = "") -> PutResult:
        """Convert a native ObjectMeta instance into a PutResult."""
        etag = getattr(native_meta, "etag", None) or fallback_etag
        return PutResult(
            path=str(native_meta.path),
            size=int(native_meta.size),
            etag=str(etag) if etag else None,
            version=getattr(native_meta, "version", None),
        )

    @staticmethod
    def _native_to_object_meta(native_meta: Any) -> ObjectMeta:
        """Convert a native ObjectMeta instance into an ObjectMeta."""
        return ObjectMeta(
            path=str(native_meta.path),
            size=int(native_meta.size),
            last_modified=str(getattr(native_meta, "last_modified", "")),
            etag=getattr(native_meta, "etag", None),
            version=getattr(native_meta, "version", None),
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
        key = self._normalize_key(path)
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
        key = self._normalize_key(path)
        if self._signing.provider == "gcs":
            return await self._sign_gcs_v4_iam_async(
                key,
                method=method,
                expires_in_secs=expires_in_secs,
                now_ts=now_ts,
            )
        return self._presign(path, method=method, expires_in_secs=expires_in_secs, timestamp=now_ts)

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

    def _ensure_gcs_token_cache(self, effective_email: str) -> GcpTokenCache:
        """Lazily initialize and return the store-scoped `GcpTokenCache` for keyless GCS IAM signing."""
        if self._gcp_token_cache is None:
            assert self._signing is not None
            adc_helper = GCSConfig(
                bucket=self._signing.bucket,
                service_account_path=self._options.get("service_account_path"),
                service_account_key=self._options.get("service_account_key"),
                application_credentials=self._options.get("application_credentials_path"),
                service_account_email=effective_email,
                iam_endpoint=self._signing.iam_endpoint,
            )
            self._gcp_token_cache = GcpTokenCache.from_gcs_config(adc_helper)
        return self._gcp_token_cache

    @staticmethod
    def _is_iam_unauthorized_error(exc: BaseException) -> bool:
        """Return True when an IAM signBlob error indicates an HTTP 401 Unauthorized response."""
        if isinstance(exc, urllib.error.HTTPError) and exc.code == 401:
            return True
        return isinstance(exc, RuntimeError) and "HTTP 401" in str(exc)

    def _execute_gcs_v4_iam_sign(
        self,
        key: str,
        *,
        effective_email: str,
        method: str,
        expires_in_secs: int,
        now_ts: int,
        access_token: str | None,
        private_key_pem: str | None,
    ) -> str:
        assert self._signing is not None
        native_mod = self._get_native_module()
        if native_mod is not None:
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
                )
            )
        goog_date = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now_ts))
        datestamp = time.strftime("%Y%m%d", time.gmtime(now_ts))
        canonical_uri = f"/{self._signing.bucket}/{key}"
        scope = f"{datestamp}/auto/storage/goog4_request"
        query_items = [
            ("X-Goog-Algorithm", "GOOG4-RSA-SHA256"),
            ("X-Goog-Credential", f"{effective_email}/{scope}"),
            ("X-Goog-Date", goog_date),
            ("X-Goog-Expires", str(expires_in_secs)),
            ("X-Goog-SignedHeaders", "host"),
        ]
        canonical_qs = urlencode(query_items)
        canonical_req = (
            f"{method.upper()}\n{canonical_uri}\n{canonical_qs}\nhost:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD"
        )
        string_to_sign = f"GOOG4-RSA-SHA256\n{goog_date}\n{scope}\n{hashlib.sha256(canonical_req.encode()).hexdigest()}"
        iam_url = f"{self._signing.iam_endpoint.rstrip('/')}/v1/projects/-/serviceAccounts/{effective_email}:signBlob"
        if not iam_url.startswith(("http:", "https:")):
            msg = f"Unsupported IAM endpoint scheme: {iam_url!r}"
            raise ValueError(msg)
        parsed_iam = urlparse(iam_url)
        body_bytes = bytes(encode_json_bytes({"payload": base64.b64encode(string_to_sign.encode()).decode("ascii")}))
        headers = {"Content-Type": "application/json"}
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
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
        token_generation: int | None = None
        if access_token is None and private_key_pem is None:
            cache = self._ensure_gcs_token_cache(effective_email)
            snap = cache.get_token()
            access_token = snap.access_token
            token_generation = snap.generation

        try:
            return self._execute_gcs_v4_iam_sign(
                key,
                effective_email=effective_email,
                method=method,
                expires_in_secs=expires_in_secs,
                now_ts=now_ts,
                access_token=access_token,
                private_key_pem=private_key_pem,
            )
        except (RuntimeError, urllib.error.HTTPError) as exc:
            if token_generation is None or not self._is_iam_unauthorized_error(exc):
                raise
            cache = self._ensure_gcs_token_cache(effective_email)
            cache.invalidate(token_generation)
            refreshed = cache.get_token()
            return self._execute_gcs_v4_iam_sign(
                key,
                effective_email=effective_email,
                method=method,
                expires_in_secs=expires_in_secs,
                now_ts=now_ts,
                access_token=refreshed.access_token,
                private_key_pem=private_key_pem,
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
        token_generation: int | None = None
        if access_token is None and private_key_pem is None:
            cache = self._ensure_gcs_token_cache(effective_email)
            snap = await cache.get_token_async()
            access_token = snap.access_token
            token_generation = snap.generation

        native_mod = self._get_native_module()
        if native_mod is not None and hasattr(native_mod, "py_sign_gcs_v4_iam_url_async"):
            try:
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
                    )
                )
            except RuntimeError as exc:
                if token_generation is None or not self._is_iam_unauthorized_error(exc):
                    raise
                cache = self._ensure_gcs_token_cache(effective_email)
                cache.invalidate(token_generation)
                refreshed = await cache.get_token_async()
                return str(
                    await native_mod.py_sign_gcs_v4_iam_url_async(
                        self._signing.bucket,
                        effective_email,
                        method,
                        key,
                        expires_in_secs,
                        now_ts,
                        self._signing.iam_endpoint,
                        refreshed.access_token,
                        private_key_pem,
                    )
                )
        return self._sign_gcs_v4_iam(
            key,
            method=method,
            expires_in_secs=expires_in_secs,
            now_ts=now_ts,
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

    def readinto(self, b: Buffer) -> int:
        """Read bytes directly into a writable buffer and return the byte count."""
        self._ensure_open()
        view = memoryview(b).cast("B")
        data = self.read(len(view))
        count = len(data)
        view[:count] = data
        return count

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
