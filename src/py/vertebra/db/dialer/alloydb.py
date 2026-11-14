"""Zero-sidecar Google Cloud AlloyDB (:5433) mTLS and MetadataExchange dialer."""

from __future__ import annotations

import asyncio
import socket
import ssl
import struct
import time
from collections.abc import Callable
from enum import IntEnum
from typing import Any, ClassVar, Self
from urllib.parse import urlparse

import anyio

from vertebra.db.dialer._base import (
    VALID_GCP_IP_TYPES,
    EphemeralCertBundle,
    _BaseGCPDialer,
)
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

__all__ = (
    "AlloyDBAuthType",
    "AlloyDBClusterURI",
    "AlloyDBConfig",
    "AsyncAlloyDBDialer",
    "SyncAlloyDBDialer",
)


class AlloyDBAuthType(IntEnum):
    """Authentication mode for AlloyDB MetadataExchangeRequest on port 5433."""

    BUILT_IN = 1
    IAM = 2


class AlloyDBClusterURI(FrozenBaseStruct, frozen=True, gc=False, kw_only=True, cache_hash=True):
    """Parsed AlloyDB instance URI (`projects/<p>/locations/<l>/clusters/<c>/instances/<i>`)."""

    project: str
    """Google Cloud project ID owning the AlloyDB cluster."""
    location: str
    """Google Cloud region hosting the AlloyDB cluster (e.g. `"us-central1"`)."""
    cluster: str
    """AlloyDB cluster resource ID."""
    instance: str
    """AlloyDB primary or read-pool instance resource ID."""

    @classmethod
    def parse(cls, raw: str) -> AlloyDBClusterURI:
        """Parse `projects/<p>/locations/<l>/clusters/<c>/instances/<i>`."""
        trimmed = raw.strip().strip("/")
        parts = trimmed.split("/")
        if (
            len(parts) == 8
            and parts[0] == "projects"
            and parts[2] == "locations"
            and parts[4] == "clusters"
            and parts[6] == "instances"
            and parts[1]
            and parts[3]
            and parts[5]
            and parts[7]
        ):
            return cls(project=parts[1], location=parts[3], cluster=parts[5], instance=parts[7])
        msg = f"Invalid AlloyDB instance URI {raw!r}; expected 'projects/<p>/locations/<l>/clusters/<c>/instances/<i>'"
        raise ValueError(msg)

    def generate_client_cert_url(self, base_endpoint: str = "https://alloydb.googleapis.com") -> str:
        """Return the AlloyDB v1beta generateClientCertificate endpoint URL."""
        base = base_endpoint.rstrip("/")
        return f"{base}/v1beta/projects/{self.project}/locations/{self.location}/clusters/{self.cluster}:generateClientCertificate"


class AlloyDBConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Configuration for zero-sidecar AlloyDB connections on port 5433."""

    url_schemes: ClassVar[tuple[str, ...]] = ("alloydb",)

    instance_uri: str
    """Full AlloyDB instance URI (`projects/<p>/locations/<l>/clusters/<c>/instances/<i>`)."""
    enable_iam_auth: bool = True
    """When True, authenticate using `MetadataExchangeRequest` `IAM` mode with an OAuth2 access token."""
    ip_type: str = "PRIVATE"
    """Preferred AlloyDB instance IP connectivity type (`"PRIVATE"`, `"PUBLIC"`, or `"PSC"`)."""
    enable_columnar_engine: bool = True
    """When True, enable the AlloyDB columnar engine GUC (`google_columnar_engine.enabled = on`)."""
    port: int = 5433
    """AlloyDB Auth Proxy protocol TLS port (default `5433`)."""
    user_agent: str = "vertebra-db/0.1"
    """User-agent string sent during the `MetadataExchangeRequest` protobuf handshake."""

    def __post_init__(self) -> None:
        """Validate AlloyDB IP connectivity type."""
        if self.ip_type not in VALID_GCP_IP_TYPES:
            msg = f"Invalid AlloyDB ip_type {self.ip_type!r}; expected one of 'PUBLIC', 'PRIVATE', 'PSC'"
            raise ValueError(msg)

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct an `AlloyDBConfig` from an `alloydb://` instance URI."""
        parsed = urlparse(url)
        instance_uri = parsed.path if parsed.path.startswith("/projects/") else f"/{parsed.netloc}{parsed.path}"
        return cls(instance_uri=instance_uri, **overrides)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-dialer fingerprint tuple."""
        return ("alloydb", self.instance_uri)


class _BaseAlloyDBDialer(_BaseGCPDialer):
    """Base zero-sidecar AlloyDB mTLS dialer foundation."""

    _tls_temp_prefix: ClassVar[str] = "vtb-adb-tls-"

    __slots__ = ()

    def __init__(
        self,
        config: AlloyDBConfig,
        *,
        token_cache: Any = None,
        default_cache_cls_name: str | None = None,
    ) -> None:
        super().__init__(
            config,
            AlloyDBClusterURI.parse(config.instance_uri),
            token_cache=token_cache,
            default_cache_cls_name=default_cache_cls_name,
        )

    @staticmethod
    def encode_metadata_exchange(
        user_agent: str,
        auth_type: AlloyDBAuthType | int = AlloyDBAuthType.IAM,
        oauth2_token: str = "",
    ) -> bytes:
        """Encode a 4-byte big-endian length-prefixed AlloyDB MetadataExchangeRequest protobuf frame."""
        mode = int(auth_type)
        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None and mode in (AlloyDBAuthType.BUILT_IN, AlloyDBAuthType.IAM):
            native_fn = getattr(native_mod, "encode_alloydb_metadata_exchange", None)
            if callable(native_fn):
                return bytes(native_fn(user_agent, oauth2_token, mode == AlloyDBAuthType.IAM))

        body = bytearray()
        if user_agent:
            ua_bytes = user_agent.encode("utf-8")
            body.append(0x0A)
            body.extend(_BaseAlloyDBDialer._encode_varint(len(ua_bytes)))
            body.extend(ua_bytes)
        body.append(0x10)
        body.extend(_BaseAlloyDBDialer._encode_varint(mode))
        if oauth2_token:
            tok_bytes = oauth2_token.encode("utf-8")
            body.append(0x1A)
            body.extend(_BaseAlloyDBDialer._encode_varint(len(tok_bytes)))
            body.extend(tok_bytes)
        return struct.pack(">I", len(body)) + bytes(body)

    @staticmethod
    def decode_metadata_exchange(frame: bytes) -> dict[str, Any]:
        """Decode a 4-byte big-endian length-prefixed AlloyDB MetadataExchangeRequest protobuf frame."""
        if len(frame) < 4:
            msg = "Frame too short for length prefix"
            raise ValueError(msg)
        (body_len,) = struct.unpack(">I", frame[:4])
        body = frame[4 : 4 + body_len]
        if len(body) < body_len:
            msg = "Truncated MetadataExchangeRequest body"
            raise ValueError(msg)

        user_agent = ""
        auth_type = AlloyDBAuthType.IAM
        oauth2_token = ""
        idx = 0
        while idx < len(body):
            tag_byte = body[idx]
            idx += 1
            field_num = tag_byte >> 3
            wire_type = tag_byte & 0x07
            if wire_type == 0:
                val, idx = _BaseAlloyDBDialer._decode_varint(body, idx)
                if field_num == 2:
                    auth_type = AlloyDBAuthType(val)
            elif wire_type == 2:
                length, idx = _BaseAlloyDBDialer._decode_varint(body, idx)
                str_bytes = body[idx : idx + length]
                idx += length
                if field_num == 1:
                    user_agent = str_bytes.decode("utf-8", errors="replace")
                elif field_num == 3:
                    oauth2_token = str_bytes.decode("utf-8", errors="replace")
            else:
                break
        return {
            "user_agent": user_agent,
            "auth_type": auth_type,
            "oauth2_token": oauth2_token,
        }

    @staticmethod
    def encode_metadata_exchange_response(code: int = 1, error_message: str = "") -> bytes:
        """Encode a 4-byte length-prefixed AlloyDB MetadataExchangeResponse protobuf frame."""
        body = bytearray()
        body.append(0x08)
        body.extend(_BaseAlloyDBDialer._encode_varint(code))
        if error_message:
            msg_bytes = error_message.encode("utf-8")
            body.append(0x12)
            body.extend(_BaseAlloyDBDialer._encode_varint(len(msg_bytes)))
            body.extend(msg_bytes)
        return struct.pack(">I", len(body)) + bytes(body)

    @staticmethod
    def decode_metadata_exchange_response(frame: bytes) -> tuple[int, str]:
        """Decode a 4-byte length-prefixed AlloyDB MetadataExchangeResponse protobuf frame."""
        if len(frame) < 4:
            msg = "Frame too short for length prefix"
            raise ValueError(msg)
        (body_len,) = struct.unpack(">I", frame[:4])
        body = frame[4 : 4 + body_len]
        if len(body) < body_len:
            msg = "Truncated MetadataExchangeResponse body"
            raise ValueError(msg)

        code = 1
        error_msg = ""
        idx = 0
        while idx < len(body):
            tag_byte = body[idx]
            idx += 1
            field_num = tag_byte >> 3
            wire_type = tag_byte & 0x07
            if wire_type == 0:
                val, idx = _BaseAlloyDBDialer._decode_varint(body, idx)
                if field_num == 1:
                    code = val
            elif wire_type == 2:
                length, idx = _BaseAlloyDBDialer._decode_varint(body, idx)
                str_bytes = body[idx : idx + length]
                idx += length
                if field_num == 2:
                    error_msg = str_bytes.decode("utf-8", errors="replace")
            else:
                break

        if code != 1:
            msg = f"AlloyDB MetadataExchange failed (code={code}): {error_msg}"
            raise PermissionError(msg)
        return code, error_msg

    def build_handshake_frame(self, oauth2_token: str = "") -> bytes:
        """Build the initial MetadataExchangeRequest wire frame for port 5433."""
        mode = AlloyDBAuthType.IAM if self._config.enable_iam_auth else AlloyDBAuthType.BUILT_IN
        return self.encode_metadata_exchange(self._config.user_agent, mode, oauth2_token)

    def _build_client_cert_request(
        self,
        public_key_pem: str,
        access_token: str,
    ) -> tuple[str, bytes, dict[str, str]]:
        url = self._uri.generate_client_cert_url()
        payload = encode_json_bytes({"pem_csr": public_key_pem, "access_token": access_token})
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        return url, payload, headers

    @staticmethod
    def _parse_client_cert_response(resp_bytes: bytes, private_key_pem: str) -> EphemeralCertBundle:
        data = decode_json(resp_bytes)
        ca_cert = ""
        client_cert = ""
        if isinstance(data, dict):
            ca_cert = data.get("caCert", "")
            client_cert = data.get("clientCert", "")
        return EphemeralCertBundle(
            ca_cert_pem=ca_cert,
            client_cert_pem=client_cert,
            private_key_pem=private_key_pem,
            server_ip="127.0.0.1",
            expiration_ts=time.time() + 3600,
        )

    @staticmethod
    def _encode_varint(value: int) -> bytes:
        out = bytearray()
        val = value
        while val >= 0x80:
            out.append((val & 0x7F) | 0x80)
            val >>= 7
        out.append(val)
        return bytes(out)

    @staticmethod
    def _decode_varint(body: bytes, start: int) -> tuple[int, int]:
        val = 0
        shift = 0
        idx = start
        while idx < len(body):
            byte_val = body[idx]
            idx += 1
            val |= (byte_val & 0x7F) << shift
            if not (byte_val & 0x80):
                break
            shift += 7
        return val, idx


class SyncAlloyDBDialer(_BaseAlloyDBDialer):
    """Synchronous zero-sidecar AlloyDB mTLS dialer with MetadataExchange handshake on port 5433."""

    __slots__ = ()

    def __init__(self, config: AlloyDBConfig, *, token_cache: Any = None) -> None:
        super().__init__(config, token_cache=token_cache, default_cache_cls_name="SyncGcpTokenCache")

    def refresh_client_cert(
        self,
        *,
        public_key_pem: str | None = None,
        private_key_pem: str | None = None,
        access_token: str | None = None,
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
    ) -> EphemeralCertBundle:
        """Exchange an ephemeral client RSA/ECDSA public key for an AlloyDB client certificate."""
        return self._refresh_bundle_sync(
            self._build_client_cert_request,
            self._parse_client_cert_response,
            public_key_pem=public_key_pem,
            private_key_pem=private_key_pem,
            access_token=access_token,
            http_transport=http_transport,
        )

    def connect(
        self,
        host: str,
        *,
        oauth2_token: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Establish a synchronous TLS 1.3 mTLS connection to AlloyDB and perform MetadataExchange."""
        resolved_token = self._resolve_sync_token(
            oauth2_token,
            use_cache=self._config.enable_iam_auth,
        )
        ctx = self._resolve_ssl_context(ssl_context)
        raw_sock = socket.create_connection((host, self.port), timeout=timeout)
        try:
            tls_sock = ctx.wrap_socket(raw_sock)
            req_frame = self.build_handshake_frame(resolved_token)
            tls_sock.sendall(req_frame)
            header = tls_sock.recv(4)
            if len(header) < 4:
                msg = "AlloyDB socket closed before metadata exchange response header"
                raise ConnectionResetError(msg)
            (body_len,) = struct.unpack(">I", header)
            body = bytearray()
            while len(body) < body_len:
                chunk = tls_sock.recv(body_len - len(body))
                if not chunk:
                    break
                body.extend(chunk)
            self.decode_metadata_exchange_response(header + bytes(body))
            return tls_sock
        except Exception:
            raw_sock.close()
            raise

    def close(self) -> None:
        """Clear cached tokens and certificates."""
        self._clear_cached_credentials()

    def __call__(
        self,
        host: str = "127.0.0.1",
        *,
        oauth2_token: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Callable dialer entrypoint returning a synchronous TLS 1.3 mTLS socket after MetadataExchange."""
        return self.connect(host, oauth2_token=oauth2_token, ssl_context=ssl_context, timeout=timeout)


class AsyncAlloyDBDialer(_BaseAlloyDBDialer):
    """Asynchronous zero-sidecar AlloyDB mTLS dialer with MetadataExchange handshake on port 5433."""

    __slots__ = ("_refresh_lock",)

    def __init__(self, config: AlloyDBConfig, *, token_cache: Any = None) -> None:
        super().__init__(config, token_cache=token_cache, default_cache_cls_name="AsyncGcpTokenCache")
        self._refresh_lock = anyio.Lock()

    async def refresh_client_cert(
        self,
        *,
        public_key_pem: str | None = None,
        private_key_pem: str | None = None,
        access_token: str | None = None,
        http_transport: Callable[[str, bytes, dict[str, str]], Any] | None = None,
    ) -> EphemeralCertBundle:
        """Asynchronously exchange an ephemeral client public key for an AlloyDB client certificate."""
        async with self._refresh_lock:
            return await self._refresh_bundle_async(
                self._build_client_cert_request,
                self._parse_client_cert_response,
                public_key_pem=public_key_pem,
                private_key_pem=private_key_pem,
                access_token=access_token,
                http_transport=http_transport,
            )

    async def connect(
        self,
        host: str,
        *,
        oauth2_token: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish an asynchronous TLS 1.3 mTLS connection to AlloyDB and perform MetadataExchange."""
        resolved_token = await self._resolve_async_token(
            oauth2_token,
            use_cache=self._config.enable_iam_auth,
        )
        ctx = self._resolve_ssl_context(ssl_context)
        reader, writer = await asyncio.open_connection(host, self.port, ssl=ctx)
        try:
            req_frame = self.build_handshake_frame(resolved_token)
            writer.write(req_frame)
            await writer.drain()
            header = await reader.readexactly(4)
            (body_len,) = struct.unpack(">I", header)
            body = await reader.readexactly(body_len)
            self.decode_metadata_exchange_response(header + body)
            return reader, writer
        except Exception:
            writer.close()
            await writer.wait_closed()
            raise

    async def close(self) -> None:
        """Clear cached tokens and certificates."""
        self._clear_cached_credentials()
