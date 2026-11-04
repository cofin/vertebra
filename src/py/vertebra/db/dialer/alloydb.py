"""Zero-sidecar Google Cloud AlloyDB (:5433) mTLS and MetadataExchange dialer."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import socket
import ssl
import struct
import tempfile
import time
import urllib.request
from collections.abc import Callable
from enum import IntEnum
from pathlib import Path
from typing import Any

from vertebra.db.dialer.cloudsql import (
    EphemeralCertBundle,
    generate_ephemeral_keypair,
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


class _BaseAlloyDBDialer:
    """Base zero-sidecar AlloyDB mTLS dialer foundation."""

    __slots__ = (
        "_cached_cert_bundle",
        "_cached_expiry",
        "_cached_token",
        "_config",
        "_native_dialer",
        "_uri",
    )

    def __init__(self, config: AlloyDBConfig) -> None:
        self._config = config
        self._uri = AlloyDBClusterURI.parse(config.instance_uri)
        self._cached_token: str | None = None
        self._cached_expiry: float = 0.0
        self._cached_cert_bundle: EphemeralCertBundle | None = None
        self._native_dialer: Any = None
        native_mod = import_optional("vertebra._vertebra")
        native_cls = getattr(native_mod, "AlloyDBDialer", None)
        if native_cls is not None:
            with contextlib.suppress(TypeError, ValueError, RuntimeError):
                self._native_dialer = native_cls(config.instance_uri)

    @property
    def config(self) -> AlloyDBConfig:
        """Return the configuration for this AlloyDB dialer."""
        return self._config

    @property
    def uri(self) -> AlloyDBClusterURI:
        """Return the parsed AlloyDBClusterURI."""
        return self._uri

    @property
    def port(self) -> int:
        """Return the AlloyDB mTLS server proxy port (default 5433)."""
        return self._config.port

    @staticmethod
    def generate_ephemeral_keypair(bits: int = 2048) -> tuple[str, str]:
        """Generate an ephemeral client public/private keypair in PEM format."""
        return generate_ephemeral_keypair(bits)

    def build_handshake_frame(self, oauth2_token: str = "") -> bytes:
        """Build the initial MetadataExchangeRequest wire frame for port 5433."""
        mode = AlloyDBAuthType.IAM if self._config.enable_iam_auth else AlloyDBAuthType.BUILT_IN
        return self.encode_metadata_exchange(self._config.user_agent, mode, oauth2_token)

    @staticmethod
    def encode_metadata_exchange(
        user_agent: str,
        auth_type: AlloyDBAuthType | int = AlloyDBAuthType.IAM,
        oauth2_token: str = "",
    ) -> bytes:
        """Encode a 4-byte big-endian length-prefixed AlloyDB MetadataExchangeRequest protobuf frame."""
        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None:
            native_cls = getattr(native_mod, "AlloyDBDialer", None)
            if native_cls is not None and hasattr(native_cls, "encode_metadata_exchange"):
                with contextlib.suppress(TypeError, ValueError, RuntimeError):
                    res = native_cls.encode_metadata_exchange(user_agent, int(auth_type), oauth2_token)
                    if isinstance(res, (bytes, bytearray)):
                        return bytes(res)
            for fn_name in ("encode_alloydb_metadata_exchange", "encode_alloydb_metadata_exchange_native"):
                native_fn = getattr(native_mod, fn_name, None)
                if callable(native_fn):
                    with contextlib.suppress(TypeError, ValueError, RuntimeError):
                        res = native_fn(user_agent, int(auth_type), oauth2_token)
                        if isinstance(res, (bytes, bytearray)):
                            return bytes(res)

        body = bytearray()
        if user_agent:
            ua_bytes = user_agent.encode("utf-8")
            body.append(0x0A)
            body.extend(_BaseAlloyDBDialer._encode_varint(len(ua_bytes)))
            body.extend(ua_bytes)
        body.append(0x10)
        body.extend(_BaseAlloyDBDialer._encode_varint(int(auth_type)))
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
                val = 0
                shift = 0
                while idx < len(body):
                    b = body[idx]
                    idx += 1
                    val |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
                if field_num == 2:
                    auth_type = AlloyDBAuthType(val)
            elif wire_type == 2:
                length = 0
                shift = 0
                while idx < len(body):
                    b = body[idx]
                    idx += 1
                    length |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
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
                val = 0
                shift = 0
                while idx < len(body):
                    b = body[idx]
                    idx += 1
                    val |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
                if field_num == 1:
                    code = val
            elif wire_type == 2:
                length = 0
                shift = 0
                while idx < len(body):
                    b = body[idx]
                    idx += 1
                    length |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
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

    def build_tls_context(
        self,
        *,
        ca_pem: str | None = None,
        client_cert_pem: str | None = None,
        client_key_pem: str | None = None,
    ) -> ssl.SSLContext:
        """Build a TLS 1.3 SSLContext for AlloyDB port 5433."""
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        ctx.check_hostname = False
        if ca_pem:
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.load_verify_locations(cadata=ca_pem)
        else:
            ctx.verify_mode = ssl.CERT_NONE
        if client_cert_pem and client_key_pem:
            with tempfile.TemporaryDirectory(prefix="vtb-adb-tls-") as tmpdir:
                os.chmod(tmpdir, 0o700)
                cert_file = Path(tmpdir) / "cert.pem"
                key_file = Path(tmpdir) / "key.pem"
                cert_file.write_text(client_cert_pem, encoding="utf-8")
                key_file.write_text(client_key_pem, encoding="utf-8")
                os.chmod(cert_file, 0o600)
                os.chmod(key_file, 0o600)
                ctx.load_cert_chain(certfile=str(cert_file), keyfile=str(key_file))
        return ctx

    def _execute_client_cert_http(
        self,
        public_key_pem: str,
        access_token: str,
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
        private_key_pem: str = "",
    ) -> EphemeralCertBundle:
        url = self._uri.generate_client_cert_url()
        payload = encode_json_bytes({"pem_csr": public_key_pem, "access_token": access_token})
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        if http_transport is not None:
            resp_bytes = http_transport(url, payload, headers)
        else:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req) as resp:
                resp_bytes = resp.read()
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
    def _unwrap_access_token(raw: Any) -> str:
        if raw is None:
            return ""
        if hasattr(raw, "access_token"):
            return str(raw.access_token or "")
        return str(raw or "")


class SyncAlloyDBDialer(_BaseAlloyDBDialer):
    """Synchronous zero-sidecar AlloyDB mTLS dialer with MetadataExchange handshake on port 5433."""

    __slots__ = ("_token_cache",)

    def __init__(self, config: AlloyDBConfig, *, token_cache: Any = None) -> None:
        super().__init__(config)
        self._token_cache = token_cache
        if self._token_cache is None:
            native_mod = import_optional("vertebra._vertebra")
            cache_cls = getattr(native_mod, "SyncGcpTokenCache", None) if native_mod is not None else None
            if cache_cls is not None:
                with contextlib.suppress(Exception):
                    self._token_cache = cache_cls()

    def refresh_client_cert(
        self,
        *,
        public_key_pem: str | None = None,
        private_key_pem: str | None = None,
        access_token: str | None = None,
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
    ) -> EphemeralCertBundle:
        """Exchange an ephemeral client RSA/ECDSA public key for an AlloyDB client certificate."""
        resolved_token = self._unwrap_access_token(access_token) if access_token is not None else ""
        if access_token is None:
            if self._token_cache is not None and hasattr(self._token_cache, "get_token"):
                with contextlib.suppress(Exception):
                    resolved_token = self._unwrap_access_token(self._token_cache.get_token())
            if not resolved_token and self._cached_token:
                resolved_token = self._cached_token
        if resolved_token:
            self._cached_token = resolved_token
        resolved_pub = public_key_pem
        resolved_priv = private_key_pem
        if resolved_pub is None or resolved_priv is None:
            gen_pub, gen_priv = self.generate_ephemeral_keypair()
            resolved_pub = resolved_pub or gen_pub
            resolved_priv = resolved_priv or gen_priv
        bundle = self._execute_client_cert_http(
            resolved_pub,
            resolved_token,
            http_transport=http_transport,
            private_key_pem=resolved_priv,
        )
        self._cached_cert_bundle = bundle
        return bundle

    def connect(
        self,
        host: str,
        *,
        oauth2_token: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Establish a synchronous TLS 1.3 mTLS connection to AlloyDB and perform MetadataExchange."""
        resolved_token = self._unwrap_access_token(oauth2_token) if oauth2_token is not None else ""
        if oauth2_token is None:
            if self._config.enable_iam_auth:
                if self._token_cache is not None and hasattr(self._token_cache, "get_token"):
                    with contextlib.suppress(Exception):
                        resolved_token = self._unwrap_access_token(self._token_cache.get_token())
                if not resolved_token and self._cached_token:
                    resolved_token = self._cached_token
                if resolved_token:
                    self._cached_token = resolved_token
        elif resolved_token:
            self._cached_token = resolved_token
        ctx = ssl_context or self.build_tls_context()
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
        self._cached_token = None
        self._cached_cert_bundle = None
        self._cached_expiry = 0.0

    def __call__(
        self,
        host: str = "127.0.0.1",
        *,
        oauth2_token: str = "",
        ssl_context: ssl.SSLContext | None = None,
        timeout: float = 10.0,
    ) -> ssl.SSLSocket:
        """Callable dialer entrypoint returning a synchronous TLS 1.3 mTLS socket after MetadataExchange."""
        return self.connect(host, oauth2_token=oauth2_token, ssl_context=ssl_context, timeout=timeout)

    def create_psycopg_connect_func(
        self,
        host: str = "127.0.0.1",
        *,
        oauth2_token: str = "",
        ssl_context: ssl.SSLContext | None = None,
    ) -> Callable[..., Any]:
        """Return a psycopg-compatible `connect` function dialing AlloyDB port 5433."""
        ctx = ssl_context or self.build_tls_context()

        def _connect(*args: Any, **kwargs: Any) -> Any:
            psycopg_mod = import_optional("psycopg")
            if psycopg_mod is None:
                return self.connect(host, oauth2_token=oauth2_token, ssl_context=ctx)
            kw = dict(kwargs)
            kw.setdefault("host", host)
            kw.setdefault("port", self.port)
            if self._config.enable_iam_auth and oauth2_token:
                kw["password"] = oauth2_token
            return psycopg_mod.connect(*args, **kw)

        return _connect


class AsyncAlloyDBDialer(_BaseAlloyDBDialer):
    """Asynchronous zero-sidecar AlloyDB mTLS dialer with MetadataExchange handshake on port 5433."""

    __slots__ = ("_token_cache",)

    def __init__(self, config: AlloyDBConfig, *, token_cache: Any = None) -> None:
        super().__init__(config)
        self._token_cache = token_cache
        if self._token_cache is None:
            native_mod = import_optional("vertebra._vertebra")
            cache_cls = getattr(native_mod, "AsyncGcpTokenCache", None) if native_mod is not None else None
            if cache_cls is not None:
                with contextlib.suppress(Exception):
                    self._token_cache = cache_cls()

    async def refresh_client_cert(
        self,
        *,
        public_key_pem: str | None = None,
        private_key_pem: str | None = None,
        access_token: str | None = None,
        http_transport: Callable[[str, bytes, dict[str, str]], bytes] | None = None,
    ) -> EphemeralCertBundle:
        """Asynchronously exchange an ephemeral client public key for an AlloyDB client certificate."""
        resolved_token = self._unwrap_access_token(access_token) if access_token is not None else ""
        if access_token is None:
            if self._token_cache is not None and hasattr(self._token_cache, "get_token"):
                with contextlib.suppress(Exception):
                    res = self._token_cache.get_token()
                    raw_token = await res if inspect.isawaitable(res) else res
                    resolved_token = self._unwrap_access_token(raw_token)
            if not resolved_token and self._cached_token:
                resolved_token = self._cached_token
        if resolved_token:
            self._cached_token = resolved_token
        resolved_pub = public_key_pem
        resolved_priv = private_key_pem
        if resolved_pub is None or resolved_priv is None:
            gen_pub, gen_priv = self.generate_ephemeral_keypair()
            resolved_pub = resolved_pub or gen_pub
            resolved_priv = resolved_priv or gen_priv
        bundle = await asyncio.to_thread(
            self._execute_client_cert_http,
            resolved_pub,
            resolved_token,
            http_transport,
            resolved_priv,
        )
        self._cached_cert_bundle = bundle
        return bundle

    async def connect(
        self,
        host: str,
        *,
        oauth2_token: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish an asynchronous TLS 1.3 mTLS connection to AlloyDB and perform MetadataExchange."""
        resolved_token = self._unwrap_access_token(oauth2_token) if oauth2_token is not None else ""
        if oauth2_token is None:
            if self._config.enable_iam_auth:
                if self._token_cache is not None and hasattr(self._token_cache, "get_token"):
                    with contextlib.suppress(Exception):
                        res = self._token_cache.get_token()
                        raw_token = await res if inspect.isawaitable(res) else res
                        resolved_token = self._unwrap_access_token(raw_token)
                if not resolved_token and self._cached_token:
                    resolved_token = self._cached_token
                if resolved_token:
                    self._cached_token = resolved_token
        elif resolved_token:
            self._cached_token = resolved_token
        ctx = ssl_context or self.build_tls_context()
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
        self._cached_token = None
        self._cached_cert_bundle = None
        self._cached_expiry = 0.0

    def create_asyncpg_connect_func(
        self,
        host: str = "127.0.0.1",
        *,
        oauth2_token: str = "",
        ssl_context: ssl.SSLContext | None = None,
    ) -> Callable[..., Any]:
        """Return an asyncpg-compatible `connect` coroutine function dialing AlloyDB port 5433."""
        ctx = ssl_context or self.build_tls_context()

        async def _connect(*args: Any, **kwargs: Any) -> Any:
            asyncpg_mod = import_optional("asyncpg")
            if asyncpg_mod is None:
                return await self.connect(host, oauth2_token=oauth2_token, ssl_context=ctx)
            kw = dict(kwargs)
            kw.setdefault("host", host)
            kw.setdefault("port", self.port)
            kw["ssl"] = ctx
            if self._config.enable_iam_auth and oauth2_token:
                kw["password"] = oauth2_token
            return await asyncpg_mod.connect(*args, **kw)

        return _connect
