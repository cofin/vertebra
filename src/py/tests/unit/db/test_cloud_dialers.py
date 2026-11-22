"""Unit tests for Cloud SQL and AlloyDB zero-sidecar mTLS dialers."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import io
import logging
import math
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
from http.client import HTTPMessage
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast
from urllib.parse import parse_qs, quote, urlsplit

import anyio
import pytest

import vertebra.db as db_pkg
import vertebra.db.dialer as dialer_pkg
from vertebra._vertebra import (
    AwsAuroraDsqlDialer,
    AwsRdsDialer,
    AzureEntraDialer,
    GoogleCloudAuthError,
    aws_rds_root_ca_pem,
)
from vertebra.db.adapters.mssql.config import MSSQLConfig
from vertebra.db.adapters.mysql.config import MySQLConfig
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.dialer import (
    DEFAULT_CONNECTOR_RETRY_COUNT,
    DEFAULT_CONNECTOR_RETRY_DELAY,
    DEFAULT_CONNECTOR_RETRY_MAX_DELAY,
    AsyncAwsAuroraDsqlDialer,
    AsyncAwsRdsDialer,
    AsyncAzureEntraDialer,
    SyncAwsAuroraDsqlDialer,
    SyncAwsRdsDialer,
    SyncAzureEntraDialer,
    calculate_connector_backoff_seconds,
    is_transient_connector_error,
    validate_connector_retry_config,
)
from vertebra.db.dialer.alloydb import (
    AlloyDBAuthType,
    AlloyDBClusterURI,
    AlloyDBConfig,
    AsyncAlloyDBDialer,
    SyncAlloyDBDialer,
)
from vertebra.db.dialer.cloudsql import (
    AsyncCloudSQLDialer,
    CloudSQLConfig,
    CloudSQLInstanceURI,
    EphemeralCertBundle,
    SyncCloudSQLDialer,
)
from vertebra.db.driver import (
    AZURE_MSSQL_SCOPE,
    AZURE_MYSQL_SCOPE,
    AZURE_POSTGRES_SCOPE,
    AZURE_REDIS_SCOPE,
    AwsAuroraDsqlIamTokenProvider,
    AwsRdsIamTokenProvider,
    AzureEntraTokenProvider,
    DatabaseTokenProvider,
)
from vertebra.db.driver._cloud_auth import _sign_sigv4_presigned_token
from vertebra.db.registry import DatabaseRegistry
from vertebra.typing import ImproperConfigurationError, MissingDependencyError, reset_dependency_cache
from vertebra.utils.serializers import decode_json, encode_json_bytes


def _generate_test_tls_credentials() -> tuple[str, str]:
    """Generate self-signed test X.509 certificate and private key using OpenSSL."""
    with tempfile.TemporaryDirectory(prefix="vtb-test-tls-") as tmpdir:
        key_path = Path(tmpdir) / "key.pem"
        cert_path = Path(tmpdir) / "cert.pem"
        cmd = [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key_path),
            "-out",
            str(cert_path),
            "-days",
            "1",
            "-subj",
            "/CN=my-proj:my-pg",
        ]
        subprocess.run(cmd, check=True, capture_output=True, timeout=30)
        return cert_path.read_text(encoding="utf-8"), key_path.read_text(encoding="utf-8")


def test_cloudsql_config_and_uri_parsing() -> None:
    """Verify Cloud SQL instance connection name parsing, URI properties, and lead window."""
    uri = CloudSQLInstanceURI.parse("my-proj:us-central1:my-pg")
    assert uri.project == "my-proj"
    assert uri.region == "us-central1"
    assert uri.instance == "my-pg"
    assert uri.expected_server_cn == "my-proj:my-pg"
    assert "generateEphemeralCert" in uri.generate_ephemeral_cert_url()

    cfg = CloudSQLConfig(
        instance_connection_name="my-proj:us-central1:my-pg",
        refresh_lead_seconds=300,
    )
    assert cfg.refresh_lead_seconds == 300

    dialer = SyncCloudSQLDialer(cfg)
    assert dialer.needs_refresh(now_ts=1000.0) is True
    dialer.cache_iam_token("mock-token", expires_in_seconds=500, now_ts=1000.0)
    assert dialer.needs_refresh(now_ts=1100.0) is False
    assert dialer.needs_refresh(now_ts=1300.0) is True


def test_cloudsql_dialer_build_tls_context_with_client_pem() -> None:
    """Verify TLS 1.3 client PEM loading and context configuration."""
    cert_pem, key_pem = _generate_test_tls_credentials()
    cfg = CloudSQLConfig(instance_connection_name="my-proj:us-central1:my-pg")
    dialer = SyncCloudSQLDialer(cfg)

    ctx = dialer.build_tls_context(
        ca_pem=cert_pem,
        client_cert_pem=cert_pem,
        client_key_pem=key_pem,
    )
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_3
    assert ctx.check_hostname is False
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_cloudsql_refresh_ephemeral_cert() -> None:
    """Verify exchanging client public key for signed ephemeral cert bundle."""
    cfg = CloudSQLConfig(instance_connection_name="my-proj:us-central1:my-pg")
    dialer = SyncCloudSQLDialer(cfg)

    recorded_url: list[str] = []

    def mock_http(url: str, payload: bytes, headers: dict[str, str]) -> bytes:
        recorded_url.append(url)
        assert headers["Authorization"] == "Bearer mock-oauth-token"
        req_data = decode_json(payload)
        assert req_data["public_key"] == "PUBLIC_KEY_PEM"
        resp = {
            "ephemeralCert": {"cert": "EPHEMERAL_CERT_PEM"},
            "serverCaCert": {"cert": "SERVER_CA_PEM"},
            "ipAddress": "10.0.0.5",
        }
        return encode_json_bytes(resp)

    bundle = dialer.refresh_certificate(
        public_key_pem="PUBLIC_KEY_PEM",
        private_key_pem="PRIVATE_KEY_PEM",
        access_token="mock-oauth-token",
        http_transport=mock_http,
    )
    assert isinstance(bundle, EphemeralCertBundle)
    assert bundle.client_cert_pem == "EPHEMERAL_CERT_PEM"
    assert bundle.ca_cert_pem == "SERVER_CA_PEM"
    assert bundle.server_ip == "10.0.0.5"
    assert "generateEphemeralCert" in recorded_url[0]


def test_alloydb_metadata_exchange_codec() -> None:
    """Verify AlloyDB MetadataExchange request encoding and response decoding."""
    uri = AlloyDBClusterURI.parse("projects/p1/locations/us-central1/clusters/c1/instances/i1")
    assert uri.project == "p1"
    assert uri.instance == "i1"

    req_frame = SyncAlloyDBDialer.encode_metadata_exchange(
        "vertebra/0.1",
        AlloyDBAuthType.IAM,
        "mock-oauth-token",
    )
    assert len(req_frame) > 4

    ok_resp = SyncAlloyDBDialer.encode_metadata_exchange_response(code=1, error_message="")
    code, msg = SyncAlloyDBDialer.decode_metadata_exchange_response(ok_resp)
    assert code == 1
    assert msg == ""

    err_resp = SyncAlloyDBDialer.encode_metadata_exchange_response(
        code=2,
        error_message="IAM token expired",
    )
    with pytest.raises(PermissionError, match="AlloyDB MetadataExchange failed \\(code=2\\): IAM token expired"):
        SyncAlloyDBDialer.decode_metadata_exchange_response(err_resp)

    with pytest.raises(ValueError, match="Frame too short"):
        SyncAlloyDBDialer.decode_metadata_exchange_response(b"\x00\x00")


def test_alloydb_refresh_client_cert() -> None:
    """Verify AlloyDB client certificate exchange with Admin API mock transport."""
    cfg = AlloyDBConfig(instance_uri="projects/p1/locations/us-central1/clusters/c1/instances/i1")
    dialer = SyncAlloyDBDialer(cfg)

    def mock_http(url: str, payload: bytes, headers: dict[str, str]) -> bytes:
        assert "generateClientCertificate" in url
        assert headers["Authorization"] == "Bearer mock-oauth-token"
        req_data = decode_json(payload)
        assert req_data["pem_csr"] == "PUBLIC_KEY_PEM"
        resp = {
            "clientCert": "ALLOYDB_CLIENT_CERT_PEM",
            "caCert": "ALLOYDB_CA_CERT_PEM",
        }
        return encode_json_bytes(resp)

    bundle = dialer.refresh_client_cert(
        public_key_pem="PUBLIC_KEY_PEM",
        private_key_pem="PRIVATE_KEY_PEM",
        access_token="mock-oauth-token",
        http_transport=mock_http,
    )
    assert bundle.client_cert_pem == "ALLOYDB_CLIENT_CERT_PEM"
    assert bundle.ca_cert_pem == "ALLOYDB_CA_CERT_PEM"


def test_end_to_end_mtls_loopback_handshake_for_cloudsql_and_alloydb() -> None:
    """Verify loopback TLS 1.3 mTLS connection and AlloyDB MetadataExchange handshake."""
    cert_pem, key_pem = _generate_test_tls_credentials()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(5)
    port = server_sock.getsockname()[1]

    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.minimum_version = ssl.TLSVersion.TLSv1_3

    with tempfile.TemporaryDirectory(prefix="vtb-srv-tls-") as tmpdir:
        cfile = Path(tmpdir) / "cert.pem"
        kfile = Path(tmpdir) / "key.pem"
        cfile.write_text(cert_pem, encoding="utf-8")
        kfile.write_text(key_pem, encoding="utf-8")
        server_ctx.load_cert_chain(str(cfile), str(kfile))

    def run_server(is_alloydb: bool = False, reject: bool = False) -> None:
        raw_conn, _ = server_sock.accept()
        tls_conn = server_ctx.wrap_socket(raw_conn, server_side=True)
        try:
            if is_alloydb:
                req_header = tls_conn.recv(4)
                req_len = int.from_bytes(req_header, "big")
                _ = tls_conn.recv(req_len)
                if reject:
                    resp_frame = SyncAlloyDBDialer.encode_metadata_exchange_response(
                        code=2,
                        error_message="unauthorized token",
                    )
                else:
                    resp_frame = SyncAlloyDBDialer.encode_metadata_exchange_response(
                        code=1,
                        error_message="",
                    )
                tls_conn.sendall(resp_frame)
            else:
                tls_conn.sendall(b"OK_CSQL")
        finally:
            tls_conn.close()

    srv_thread = threading.Thread(target=run_server, args=(False, False), daemon=True)
    srv_thread.start()

    csql_cfg = CloudSQLConfig(
        instance_connection_name="my-proj:us-central1:my-pg",
        port=port,
    )
    csql_dialer = SyncCloudSQLDialer(csql_cfg)
    client_ctx = csql_dialer.build_tls_context(
        ca_pem=cert_pem,
        client_cert_pem=cert_pem,
        client_key_pem=key_pem,
    )
    sock = csql_dialer.connect("127.0.0.1", ssl_context=client_ctx)
    data = sock.recv(7)
    assert data == b"OK_CSQL"
    sock.close()
    srv_thread.join(timeout=2.0)

    srv_thread2 = threading.Thread(target=run_server, args=(True, False), daemon=True)
    srv_thread2.start()

    alloy_cfg = AlloyDBConfig(
        instance_uri="projects/p1/locations/us-central1/clusters/c1/instances/i1",
        port=port,
    )
    alloy_dialer = SyncAlloyDBDialer(alloy_cfg)
    alloy_ctx = alloy_dialer.build_tls_context(
        ca_pem=cert_pem,
        client_cert_pem=cert_pem,
        client_key_pem=key_pem,
    )
    alloy_sock = alloy_dialer.connect(
        "127.0.0.1",
        oauth2_token="mock-oauth-token",
        ssl_context=alloy_ctx,
    )
    assert alloy_sock is not None
    alloy_sock.close()
    srv_thread2.join(timeout=2.0)

    srv_thread3 = threading.Thread(target=run_server, args=(True, True), daemon=True)
    srv_thread3.start()

    with pytest.raises(PermissionError, match="AlloyDB MetadataExchange failed \\(code=2\\): unauthorized token"):
        alloy_dialer.connect(
            "127.0.0.1",
            oauth2_token="invalid-token",
            ssl_context=alloy_ctx,
        )
    srv_thread3.join(timeout=2.0)
    server_sock.close()


def test_cloud_dialers_remove_legacy_driver_helpers_and_validate_configs() -> None:
    """Cloud SQL and AlloyDB dialers remove legacy driver connect helpers and validate config inputs."""
    assert not hasattr(SyncCloudSQLDialer, "create_psycopg_connect_func")
    assert not hasattr(AsyncCloudSQLDialer, "create_asyncpg_connect_func")
    assert not hasattr(AsyncCloudSQLDialer, "create_asyncmy_creator")
    assert not hasattr(SyncAlloyDBDialer, "create_psycopg_connect_func")
    assert not hasattr(AsyncAlloyDBDialer, "create_asyncpg_connect_func")

    with pytest.raises(ValueError, match="ip_type"):
        CloudSQLConfig(instance_connection_name="p:r:i", ip_type="ANYCAST")
    with pytest.raises(ValueError, match="refresh_lead_seconds"):
        CloudSQLConfig(instance_connection_name="p:r:i", refresh_lead_seconds=-1)
    with pytest.raises(ValueError, match="ip_type"):
        AlloyDBConfig(instance_uri="projects/p/locations/r/clusters/c/instances/i", ip_type="INTERNAL")

    assert CloudSQLConfig(instance_connection_name="p:r:i", refresh_lead_seconds=0).refresh_lead_seconds == 0


@pytest.mark.parametrize("dialer_kind", ["cloudsql", "alloydb"])
def test_cloud_dialers_default_tls_cert_required_and_reject_fake_rsa_fallback(
    dialer_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default TLS contexts require certificate verification and keygen raises instead of returning static fake keys."""
    dialer: SyncCloudSQLDialer | SyncAlloyDBDialer = (
        SyncCloudSQLDialer(CloudSQLConfig(instance_connection_name="p:r:i"))
        if dialer_kind == "cloudsql"
        else SyncAlloyDBDialer(AlloyDBConfig(instance_uri="projects/p/locations/r/clusters/c/instances/i"))
    )
    ctx = dialer.build_tls_context(ca_pem=None)
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_3
    assert ctx.check_hostname is False
    assert ctx.verify_mode == ssl.CERT_REQUIRED

    monkeypatch.setitem(sys.modules, "vertebra._vertebra", None)
    monkeypatch.setitem(sys.modules, "cryptography.hazmat.primitives.asymmetric.rsa", None)
    reset_dependency_cache("vertebra._vertebra")
    reset_dependency_cache("cryptography.hazmat.primitives.asymmetric.rsa")
    try:
        with pytest.raises(MissingDependencyError):
            dialer.generate_ephemeral_keypair()
    finally:
        reset_dependency_cache("vertebra._vertebra")
        reset_dependency_cache("cryptography.hazmat.primitives.asymmetric.rsa")


@pytest.mark.anyio
@pytest.mark.parametrize("dialer_kind", ["cloudsql", "alloydb"])
async def test_cloud_dialers_propagate_token_cache_errors_and_serialize_async_refresh(
    dialer_kind: str,
) -> None:
    """Token cache exceptions propagate and async certificate refresh serializes concurrent callers."""

    class _FailingSyncCache:
        def get_token(self) -> str:
            raise RuntimeError("adc-sync-boom")

    class _FailingAsyncCache:
        async def get_token(self) -> str:
            raise RuntimeError("adc-async-boom")

    if dialer_kind == "cloudsql":
        cfg = CloudSQLConfig(instance_connection_name="my-proj:us-central1:my-pg")
        sync_dialer = SyncCloudSQLDialer(cfg, token_cache=_FailingSyncCache())
        with pytest.raises(RuntimeError, match="adc-sync-boom"):
            sync_dialer.refresh_certificate(public_key_pem="PUB", private_key_pem="PRIV")

        async_failing = AsyncCloudSQLDialer(cfg, token_cache=_FailingAsyncCache())
        with pytest.raises(RuntimeError, match="adc-async-boom"):
            await async_failing.refresh_certificate(public_key_pem="PUB", private_key_pem="PRIV")

        async_dialer: AsyncCloudSQLDialer | AsyncAlloyDBDialer = AsyncCloudSQLDialer(cfg)
    else:
        cfg_alloy = AlloyDBConfig(instance_uri="projects/p1/locations/us-central1/clusters/c1/instances/i1")
        sync_alloy = SyncAlloyDBDialer(cfg_alloy, token_cache=_FailingSyncCache())
        with pytest.raises(RuntimeError, match="adc-sync-boom"):
            sync_alloy.refresh_client_cert(public_key_pem="PUB", private_key_pem="PRIV")

        async_alloy_fail = AsyncAlloyDBDialer(cfg_alloy, token_cache=_FailingAsyncCache())
        with pytest.raises(RuntimeError, match="adc-async-boom"):
            await async_alloy_fail.refresh_client_cert(public_key_pem="PUB", private_key_pem="PRIV")

        async_dialer = AsyncAlloyDBDialer(cfg_alloy)

    in_flight = 0
    max_in_flight = 0
    calls = 0
    first_entered = anyio.Event()
    release_first = anyio.Event()
    second_started = anyio.Event()

    async def _async_http(_url: str, _payload: bytes, _headers: dict[str, str]) -> bytes:
        nonlocal in_flight, max_in_flight, calls
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        calls += 1
        call_num = calls
        if call_num == 1:
            first_entered.set()
            await release_first.wait()
        in_flight -= 1
        if dialer_kind == "cloudsql":
            return b'{"ephemeralCert":{"cert":"CERT"},"serverCaCert":{"cert":"CA"},"ipAddress":"10.0.0.1"}'
        return b'{"clientCert":"CERT","caCert":"CA"}'

    async def _invoke(*, is_second: bool = False) -> None:
        if is_second:
            second_started.set()
        if isinstance(async_dialer, AsyncCloudSQLDialer):
            await async_dialer.refresh_certificate(
                public_key_pem="PUB",
                private_key_pem="PRIV",
                access_token="tok",
                http_transport=_async_http,
            )
        else:
            await async_dialer.refresh_client_cert(
                public_key_pem="PUB",
                private_key_pem="PRIV",
                access_token="tok",
                http_transport=_async_http,
            )

    async with anyio.create_task_group() as tg:
        tg.start_soon(lambda: _invoke(is_second=False))
        await first_entered.wait()
        tg.start_soon(lambda: _invoke(is_second=True))
        await second_started.wait()
        await anyio.lowlevel.checkpoint()
        assert calls == 1
        assert in_flight == 1
        release_first.set()

    assert calls == 2
    assert max_in_flight == 1


@pytest.mark.parametrize("auth_type", [AlloyDBAuthType.IAM, AlloyDBAuthType.BUILT_IN])
def test_alloydb_encode_metadata_exchange_uses_native_extension(auth_type: AlloyDBAuthType) -> None:
    """Native encode_alloydb_metadata_exchange produces frames matching pure-Python decoding."""
    frame = SyncAlloyDBDialer.encode_metadata_exchange("vertebra/0.9", auth_type, "tok-xyz")
    decoded = SyncAlloyDBDialer.decode_metadata_exchange(frame)
    assert decoded == {
        "user_agent": "vertebra/0.9",
        "auth_type": auth_type,
        "oauth2_token": "tok-xyz",
    }


def test_cloud_dialers_sync_connect_uses_cached_cert_bundle_and_callable_token_cache() -> None:
    """Sync CloudSQL/AlloyDB connect uses cached mTLS cert bundles and SyncAlloyDBDialer.__call__ resolves cached tokens."""
    cert_pem, key_pem = _generate_test_tls_credentials()
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    server_ctx.verify_mode = ssl.CERT_REQUIRED
    server_ctx.load_verify_locations(cadata=cert_pem)

    with tempfile.TemporaryDirectory(prefix="vtb-srv-mtls-") as tmpdir:
        cfile = Path(tmpdir) / "cert.pem"
        kfile = Path(tmpdir) / "key.pem"
        cfile.write_text(cert_pem, encoding="utf-8")
        kfile.write_text(key_pem, encoding="utf-8")
        server_ctx.load_cert_chain(str(cfile), str(kfile))

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(4)
    port = server_sock.getsockname()[1]
    seen_tokens: list[str] = []

    def _serve_mtls(is_alloydb: bool) -> None:
        raw_conn, _ = server_sock.accept()
        tls_conn = server_ctx.wrap_socket(raw_conn, server_side=True)
        try:
            if is_alloydb:
                header = tls_conn.recv(4)
                body_len = int.from_bytes(header, "big")
                frame = header + tls_conn.recv(body_len)
                seen_tokens.append(SyncAlloyDBDialer.decode_metadata_exchange(frame)["oauth2_token"])
                tls_conn.sendall(SyncAlloyDBDialer.encode_metadata_exchange_response(1, ""))
            else:
                tls_conn.sendall(b"MTLS_OK")
        finally:
            tls_conn.close()

    try:
        csql_dialer = SyncCloudSQLDialer(CloudSQLConfig(instance_connection_name="p:r:i", port=port))
        csql_dialer.refresh_certificate(
            public_key_pem="PUB",
            private_key_pem=key_pem,
            access_token="tok",
            http_transport=lambda _u, _p, _h: encode_json_bytes(
                {"ephemeralCert": {"cert": cert_pem}, "serverCaCert": {"cert": cert_pem}, "ipAddress": "127.0.0.1"}
            ),
        )
        t1 = threading.Thread(target=_serve_mtls, args=(False,), daemon=True)
        t1.start()
        sock = csql_dialer.connect("127.0.0.1")
        assert sock.recv(7) == b"MTLS_OK"
        sock.close()
        t1.join(timeout=2.0)

        class _TokenCache:
            def get_token(self) -> str:
                return "resolved-callable-token"

        adb_dialer = SyncAlloyDBDialer(
            AlloyDBConfig(instance_uri="projects/p/locations/r/clusters/c/instances/i", port=port),
            token_cache=_TokenCache(),
        )
        adb_dialer.refresh_client_cert(
            public_key_pem="PUB",
            private_key_pem=key_pem,
            http_transport=lambda _u, _p, _h: encode_json_bytes({"clientCert": cert_pem, "caCert": cert_pem}),
        )
        t2 = threading.Thread(target=_serve_mtls, args=(True,), daemon=True)
        t2.start()
        adb_sock = adb_dialer("127.0.0.1")
        adb_sock.close()
        t2.join(timeout=2.0)
        assert seen_tokens == ["resolved-callable-token"]
    finally:
        server_sock.close()


@pytest.mark.anyio
async def test_cloud_dialers_async_connect_uses_cached_cert_bundle() -> None:
    """Async CloudSQL/AlloyDB connect uses cached mTLS cert bundles when ssl_context is omitted."""
    cert_pem, key_pem = _generate_test_tls_credentials()
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    server_ctx.verify_mode = ssl.CERT_REQUIRED
    server_ctx.load_verify_locations(cadata=cert_pem)

    with tempfile.TemporaryDirectory(prefix="vtb-asrv-mtls-") as tmpdir:
        cfile = Path(tmpdir) / "cert.pem"
        kfile = Path(tmpdir) / "key.pem"
        cfile.write_text(cert_pem, encoding="utf-8")
        kfile.write_text(key_pem, encoding="utf-8")
        server_ctx.load_cert_chain(str(cfile), str(kfile))

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(4)
    port = server_sock.getsockname()[1]

    def _serve_once(is_alloydb: bool) -> None:
        raw_conn, _ = server_sock.accept()
        tls_conn = server_ctx.wrap_socket(raw_conn, server_side=True)
        try:
            if is_alloydb:
                header = tls_conn.recv(4)
                body_len = int.from_bytes(header, "big")
                _ = tls_conn.recv(body_len)
                tls_conn.sendall(SyncAlloyDBDialer.encode_metadata_exchange_response(1, ""))
            else:
                tls_conn.sendall(b"ASYNC_MTLS")
        finally:
            tls_conn.close()

    try:
        csql_async = AsyncCloudSQLDialer(CloudSQLConfig(instance_connection_name="p:r:i", port=port))
        await csql_async.refresh_certificate(
            public_key_pem="PUB",
            private_key_pem=key_pem,
            access_token="tok",
            http_transport=lambda _u, _p, _h: encode_json_bytes(
                {"ephemeralCert": {"cert": cert_pem}, "serverCaCert": {"cert": cert_pem}, "ipAddress": "127.0.0.1"}
            ),
        )
        t1 = threading.Thread(target=_serve_once, args=(False,), daemon=True)
        t1.start()
        reader, writer = await csql_async.connect("127.0.0.1")
        assert await reader.readexactly(10) == b"ASYNC_MTLS"
        writer.close()
        await writer.wait_closed()
        t1.join(timeout=2.0)

        adb_async = AsyncAlloyDBDialer(
            AlloyDBConfig(instance_uri="projects/p/locations/r/clusters/c/instances/i", port=port),
        )
        await adb_async.refresh_client_cert(
            public_key_pem="PUB",
            private_key_pem=key_pem,
            access_token="tok",
            http_transport=lambda _u, _p, _h: encode_json_bytes({"clientCert": cert_pem, "caCert": cert_pem}),
        )
        t2 = threading.Thread(target=_serve_once, args=(True,), daemon=True)
        t2.start()
        adb_reader, adb_writer = await adb_async.connect("127.0.0.1", oauth2_token="tok")
        assert adb_reader is not None
        adb_writer.close()
        await adb_writer.wait_closed()
        t2.join(timeout=2.0)
    finally:
        server_sock.close()


def _expected_sigv4_signature(
    *,
    host_header: str,
    region: str,
    service: str,
    secret_access_key: str,
    amz_date: str,
    query_pairs: dict[str, list[str]],
) -> str:
    """Independently compute the expected AWS SigV4 presigned query signature."""
    date_stamp = amz_date[:8]
    canonical_items = sorted(
        (quote(k, safe="-._~"), quote(v[0], safe="-._~")) for k, v in query_pairs.items() if k != "X-Amz-Signature"
    )
    canonical_query = "&".join(f"{k}={v}" for k, v in canonical_items)
    payload_hash = hashlib.sha256(b"").hexdigest()
    canonical_request = f"GET\n/\n{canonical_query}\nhost:{host_header}\n\nhost\n{payload_hash}"
    hashed_request = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hashed_request}"
    k_date = hmac.new(f"AWS4{secret_access_key}".encode(), date_stamp.encode("utf-8"), hashlib.sha256).digest()
    k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode("utf-8"), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    return hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()


@pytest.mark.parametrize(
    ("endpoint_host", "session_token", "expected_authority"),
    [
        ("db.example.us-east-1.rds.amazonaws.com", None, "db.example.us-east-1.rds.amazonaws.com:5432"),
        (
            "https://db.example.us-east-1.rds.amazonaws.com/",
            "test-token-placeholder",
            "db.example.us-east-1.rds.amazonaws.com:5432",
        ),
    ],
)
def test_aws_rds_iam_token_provider_signing_and_scheme_stripping(
    endpoint_host: str,
    session_token: str | None,
    expected_authority: str,
) -> None:
    """Verify RDS IAM SigV4 presigned query token format, optional STS token, and repr redaction."""
    fixed_ts = 1_794_873_600.0
    access_key = "test-access-key-placeholder"
    secret_key = "test-secret-access-key-placeholder"
    provider = AwsRdsIamTokenProvider(
        endpoint_host=endpoint_host,
        port=5432,
        region="us-east-1",
        username="iam_user",
        access_key_id=access_key,
        secret_access_key=secret_key,
        session_token=session_token,
        clock=lambda: fixed_ts,
    )
    assert isinstance(provider, DatabaseTokenProvider)
    rendered = repr(provider)
    assert access_key not in rendered
    assert secret_key not in rendered
    if session_token is not None:
        assert session_token not in rendered

    token = provider.resolve_sync()
    assert not token.startswith("https://")
    assert not token.startswith("http://")
    parsed = urlsplit(f"https://{token}")
    assert parsed.netloc == expected_authority
    assert parsed.path == "/"
    qs = parse_qs(parsed.query, keep_blank_values=True)
    assert qs["Action"] == ["connect"]
    assert qs["DBUser"] == ["iam_user"]
    assert qs["X-Amz-Algorithm"] == ["AWS4-HMAC-SHA256"]
    assert qs["X-Amz-Expires"] == ["900"]
    assert qs["X-Amz-SignedHeaders"] == ["host"]
    amz_date = qs["X-Amz-Date"][0]
    assert qs["X-Amz-Credential"] == [f"{access_key}/{amz_date[:8]}/us-east-1/rds-db/aws4_request"]
    if session_token is not None:
        assert qs["X-Amz-Security-Token"] == [session_token]
    else:
        assert "X-Amz-Security-Token" not in qs

    expected_sig = _expected_sigv4_signature(
        host_header=expected_authority,
        region="us-east-1",
        service="rds-db",
        secret_access_key=secret_key,
        amz_date=amz_date,
        query_pairs=qs,
    )
    assert qs["X-Amz-Signature"] == [expected_sig]


@pytest.mark.anyio
async def test_aws_iam_token_provider_caching_refresh_and_env_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify fake-clock pre-expiry refresh, force_refresh, invalidate, and env credentials."""
    now = [1000.0]
    minted_calls = 0

    def _custom_signer(*, now_ts: float, endpoint_host: str, port: int) -> str:
        nonlocal minted_calls
        minted_calls += 1
        return f"https://{endpoint_host}:{port}/?Action=connect&call={minted_calls}&ts={int(now_ts)}"

    provider = AwsRdsIamTokenProvider(
        endpoint_host="db.internal",
        port=5432,
        region="us-west-2",
        username="app_user",
        refresh_margin_seconds=60.0,
        token_ttl_seconds=900,
        clock=lambda: now[0],
        signer=_custom_signer,
    )

    first = provider.resolve_sync()
    assert first == "db.internal:5432/?Action=connect&call=1&ts=1000"
    assert minted_calls == 1

    now[0] = 1839.0
    assert provider.resolve_sync() == first
    assert minted_calls == 1

    now[0] = 1840.0
    refreshed = provider.resolve_sync()
    assert refreshed == "db.internal:5432/?Action=connect&call=2&ts=1840"
    assert minted_calls == 2

    forced = provider.resolve_sync(force_refresh=True)
    assert forced == "db.internal:5432/?Action=connect&call=3&ts=1840"
    assert minted_calls == 3

    provider.invalidate()
    async_token = await provider.resolve_async()
    assert async_token == "db.internal:5432/?Action=connect&call=4&ts=1840"
    assert minted_calls == 4

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIATESTENV12345")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-env-secret-placeholder")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "test-env-session-placeholder")
    env_provider = AwsRdsIamTokenProvider(
        endpoint_host="db.env.internal",
        region="eu-west-1",
        username="env_user",
        clock=lambda: 1_794_873_600.0,
    )
    env_token = env_provider.resolve_sync()
    env_qs = parse_qs(urlsplit(f"https://{env_token}").query)
    assert env_qs["X-Amz-Security-Token"] == ["test-env-session-placeholder"]
    assert env_qs["DBUser"] == ["env_user"]


@pytest.mark.parametrize(
    ("admin", "expected_action"),
    [
        (False, "DbConnect"),
        (True, "DbConnectAdmin"),
    ],
)
def test_aws_aurora_dsql_iam_token_provider_actions_and_signing(
    admin: bool,
    expected_action: str,
) -> None:
    """Verify Aurora DSQL SigV4 token signing for DbConnect and DbConnectAdmin actions."""
    fixed_ts = 1_794_873_600.0
    access_key = "AKIADSQLKEY12345"
    secret_key = "test-dsql-secret-placeholder"
    provider = AwsAuroraDsqlIamTokenProvider(
        hostname="https://example.dsql.us-east-1.on.aws/",
        region="us-east-1",
        admin=admin,
        access_key_id=access_key,
        secret_access_key=secret_key,
        session_token="test-token-placeholder",
        clock=lambda: fixed_ts,
    )
    rendered = repr(provider)
    assert access_key not in rendered
    assert secret_key not in rendered
    assert "test-token-placeholder" not in rendered

    token = provider.resolve_sync()
    assert not token.startswith("https://")
    parsed = urlsplit(f"https://{token}")
    assert parsed.netloc == "example.dsql.us-east-1.on.aws"
    qs = parse_qs(parsed.query)
    assert qs["Action"] == [expected_action]
    assert "DBUser" not in qs
    assert qs["X-Amz-Expires"] == ["900"]
    assert qs["X-Amz-Security-Token"] == ["test-token-placeholder"]
    amz_date = qs["X-Amz-Date"][0]
    assert qs["X-Amz-Credential"] == [f"{access_key}/{amz_date[:8]}/us-east-1/dsql/aws4_request"]
    expected_sig = _expected_sigv4_signature(
        host_header="example.dsql.us-east-1.on.aws",
        region="us-east-1",
        service="dsql",
        secret_access_key=secret_key,
        amz_date=amz_date,
        query_pairs=qs,
    )
    assert qs["X-Amz-Signature"] == [expected_sig]


@pytest.mark.parametrize(
    ("factory", "kwargs"),
    [
        (AwsRdsIamTokenProvider, {"endpoint_host": "", "region": "us-east-1", "username": "u"}),
        (AwsRdsIamTokenProvider, {"endpoint_host": "db.local", "port": 0, "region": "us-east-1", "username": "u"}),
        (AwsRdsIamTokenProvider, {"endpoint_host": "db.local", "region": "", "username": "u"}),
        (AwsRdsIamTokenProvider, {"endpoint_host": "db.local", "region": "us-east-1", "username": ""}),
        (
            AwsRdsIamTokenProvider,
            {"endpoint_host": "db.local", "region": "us-east-1", "username": "u", "token_ttl_seconds": 0},
        ),
        (
            AwsRdsIamTokenProvider,
            {"endpoint_host": "db.local", "region": "us-east-1", "username": "u", "refresh_margin_seconds": -1.0},
        ),
        (
            AwsRdsIamTokenProvider,
            {"endpoint_host": "db.local", "region": "us-east-1", "username": "u", "refresh_margin_seconds": 900.0},
        ),
        (AwsAuroraDsqlIamTokenProvider, {"hostname": "", "region": "us-east-1"}),
        (AwsAuroraDsqlIamTokenProvider, {"hostname": "c.dsql.on.aws", "region": ""}),
        (AwsAuroraDsqlIamTokenProvider, {"hostname": "c.dsql.on.aws", "region": "us-east-1", "token_ttl_seconds": -5}),
    ],
)
def test_aws_iam_token_provider_validation_errors(
    factory: type[AwsRdsIamTokenProvider] | type[AwsAuroraDsqlIamTokenProvider],
    kwargs: dict[str, Any],
) -> None:
    """Verify invalid host, region, username, port, TTL, and refresh margin raise ImproperConfigurationError."""
    with pytest.raises(ImproperConfigurationError):
        factory(**kwargs)


@pytest.mark.anyio
async def test_aws_iam_token_provider_missing_credentials_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify missing AWS static/env keys and custom signer raise ImproperConfigurationError without unsigned tokens."""
    for env_key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(env_key, raising=False)

    rds_provider = AwsRdsIamTokenProvider(
        endpoint_host="db.example.internal",
        region="us-east-1",
        username="iam_user",
    )
    with pytest.raises(ImproperConfigurationError):
        rds_provider.resolve_sync()
    with pytest.raises(ImproperConfigurationError):
        await rds_provider.resolve_async()

    dsql_provider = AwsAuroraDsqlIamTokenProvider(
        hostname="cluster.dsql.us-east-1.on.aws",
        region="us-east-1",
    )
    with pytest.raises(ImproperConfigurationError):
        dsql_provider.resolve_sync()


@pytest.mark.parametrize(
    "engine_scope",
    [
        AZURE_POSTGRES_SCOPE,
        AZURE_MYSQL_SCOPE,
        AZURE_MSSQL_SCOPE,
        AZURE_REDIS_SCOPE,
    ],
)
def test_azure_entra_token_provider_default_scope_injection(engine_scope: str) -> None:
    """Verify AzureEntraTokenProvider.with_default_scope injects unset engine scopes and preserves explicit ones."""
    seen_scopes: list[str] = []

    def _fetch(scope: str) -> tuple[str, float]:
        seen_scopes.append(scope)
        return ("test-token-placeholder", 5000.0)

    unscoped = AzureEntraTokenProvider(fetcher=_fetch, clock=lambda: 1000.0)
    scoped = unscoped.with_default_scope(engine_scope)
    assert scoped.scope == engine_scope
    assert scoped.resolve_sync() == "test-token-placeholder"
    assert seen_scopes == [engine_scope]

    explicit_scope = "https://custom.database.windows.net/.default"
    explicit = AzureEntraTokenProvider(scope=explicit_scope, fetcher=_fetch, clock=lambda: 1000.0)
    preserved = explicit.with_default_scope(engine_scope)
    assert preserved is explicit
    assert preserved.scope == explicit_scope
    assert preserved.resolve_sync() == "test-token-placeholder"
    assert seen_scopes == [engine_scope, explicit_scope]


@pytest.mark.anyio
@pytest.mark.parametrize("auth_mode", ["client_credentials", "imds", "custom_fetcher"])
@pytest.mark.parametrize("exec_mode", ["sync", "async"])
async def test_azure_entra_token_provider_modes_and_pre_expiry_refresh(
    auth_mode: str,
    exec_mode: str,
) -> None:
    """Verify AzureEntraTokenProvider across client-credentials, IMDS, custom fetcher, and pre-expiry refresh."""
    now = [1000.0]
    recorded_requests: list[dict[str, Any]] = []
    issued_tokens: list[str] = []

    class _EntraHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length).decode("utf-8")
            form = parse_qs(raw_body, keep_blank_values=True)
            recorded_requests.append({"method": "POST", "path": self.path, "form": form})
            tok = f"test-token-placeholder-cc-{len(issued_tokens) + 1}"
            issued_tokens.append(tok)
            payload = encode_json_bytes({"token_type": "Bearer", "expires_in": 600, "access_token": tok})
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            qs = parse_qs(parsed.query, keep_blank_values=True)
            recorded_requests.append(
                {
                    "method": "GET",
                    "path": parsed.path,
                    "query": qs,
                    "metadata_header": self.headers.get("Metadata"),
                }
            )
            tok = f"test-token-placeholder-imds-{len(issued_tokens) + 1}"
            issued_tokens.append(tok)
            payload = encode_json_bytes({"access_token": tok, "expires_in": "600", "token_type": "Bearer"})
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server: ThreadingHTTPServer | None = None
    srv_thread: threading.Thread | None = None
    if auth_mode in {"client_credentials", "imds"}:
        server = ThreadingHTTPServer(("127.0.0.1", 0), _EntraHandler)
        port = server.server_address[1]
        srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
        srv_thread.start()
    else:
        port = 0

    try:
        if auth_mode == "client_credentials":
            provider = AzureEntraTokenProvider(
                tenant_id="11111111-2222-3333-4444-555555555555",
                client_id="app-client-id",
                client_secret="test-client-secret-placeholder",
                scope=AZURE_POSTGRES_SCOPE,
                token_endpoint=f"http://127.0.0.1:{port}/oauth2/v2.0/token",
                refresh_margin_seconds=120.0,
                clock=lambda: now[0],
            )
        elif auth_mode == "imds":
            provider = AzureEntraTokenProvider(
                client_id="uami-client-id",
                scope=AZURE_POSTGRES_SCOPE,
                imds_endpoint=f"http://127.0.0.1:{port}/metadata/identity/oauth2/token",
                api_version="2019-08-01",
                refresh_margin_seconds=120.0,
                clock=lambda: now[0],
            )
        else:

            def _sync_fetcher(scope: str) -> dict[str, Any]:
                recorded_requests.append({"method": "FETCHER", "scope": scope})
                tok = f"test-token-placeholder-fetch-{len(issued_tokens) + 1}"
                issued_tokens.append(tok)
                return {"access_token": tok, "expires_in": 600}

            async def _async_fetcher(scope: str) -> dict[str, Any]:
                await anyio.sleep(0)
                return _sync_fetcher(scope)

            provider = AzureEntraTokenProvider(
                scope=AZURE_POSTGRES_SCOPE,
                refresh_margin_seconds=120.0,
                clock=lambda: now[0],
                fetcher=_async_fetcher if exec_mode == "async" else _sync_fetcher,
            )

        assert isinstance(provider, DatabaseTokenProvider)

        async def _resolve(*, force_refresh: bool = False) -> str:
            if exec_mode == "sync":
                return provider.resolve_sync(force_refresh=force_refresh)
            return await provider.resolve_async(force_refresh=force_refresh)

        first = await _resolve()
        assert first == issued_tokens[0]
        assert len(issued_tokens) == 1

        rendered = repr(provider)
        assert "test-client-secret-placeholder" not in rendered
        assert first not in rendered

        now[0] = 1479.0
        assert await _resolve() == first
        assert len(issued_tokens) == 1

        now[0] = 1480.0
        refreshed = await _resolve()
        assert refreshed == issued_tokens[1]
        assert refreshed != first
        assert len(issued_tokens) == 2

        forced = await _resolve(force_refresh=True)
        assert forced == issued_tokens[2]
        assert len(issued_tokens) == 3

        provider.invalidate()
        after_invalidate = await _resolve()
        assert after_invalidate == issued_tokens[3]
        assert len(issued_tokens) == 4

        if auth_mode == "client_credentials":
            req = recorded_requests[0]
            assert req["method"] == "POST"
            assert req["form"]["grant_type"] == ["client_credentials"]
            assert req["form"]["client_id"] == ["app-client-id"]
            assert req["form"]["client_secret"] == ["test-client-secret-placeholder"]
            assert req["form"]["scope"] == [AZURE_POSTGRES_SCOPE]
        elif auth_mode == "imds":
            req = recorded_requests[0]
            assert req["method"] == "GET"
            assert req["metadata_header"] == "true"
            assert req["query"]["api-version"] == ["2019-08-01"]
            assert req["query"]["resource"] == ["https://ossrdbms-aad.database.windows.net"]
            assert req["query"]["client_id"] == ["uami-client-id"]
        else:
            assert recorded_requests[0]["scope"] == AZURE_POSTGRES_SCOPE
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        if srv_thread is not None:
            srv_thread.join(timeout=2.0)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"refresh_margin_seconds": -0.5, "scope": AZURE_POSTGRES_SCOPE},
        {"refresh_margin_seconds": float("nan"), "scope": AZURE_POSTGRES_SCOPE},
        {"tenant_id": "", "client_id": "cid", "client_secret": "test-token-placeholder", "scope": AZURE_POSTGRES_SCOPE},
        {
            "tenant_id": "../evil",
            "client_id": "cid",
            "client_secret": "test-token-placeholder",
            "scope": AZURE_POSTGRES_SCOPE,
        },
        {
            "tenant_id": "tenant/bad",
            "client_id": "cid",
            "client_secret": "test-token-placeholder",
            "scope": AZURE_POSTGRES_SCOPE,
        },
        {"client_id": "cid", "client_secret": "test-token-placeholder", "scope": AZURE_POSTGRES_SCOPE},
        {
            "tenant_id": "contoso.onmicrosoft.com",
            "client_secret": "test-token-placeholder",
            "scope": AZURE_POSTGRES_SCOPE,
        },
        {"scope": ""},
        {"imds_endpoint": "ftp://169.254.169.254/token"},
    ],
)
def test_azure_entra_token_provider_validation_errors(kwargs: dict[str, Any]) -> None:
    """Verify invalid refresh margin, tenant_id, client credentials, scope, and URL raise ImproperConfigurationError."""
    with pytest.raises(ImproperConfigurationError):
        AzureEntraTokenProvider(**kwargs)


def test_azure_entra_token_provider_missing_scope() -> None:
    """Verify unscoped Entra provider raises ImproperConfigurationError for sync resolution."""
    unscoped = AzureEntraTokenProvider(fetcher=lambda scope: ("test-token-placeholder", 5000.0))
    with pytest.raises(ImproperConfigurationError, match="scope"):
        unscoped.resolve_sync()


@pytest.mark.anyio
async def test_azure_entra_token_provider_missing_scope_async() -> None:
    """Verify unscoped Entra provider raises ImproperConfigurationError for async resolution."""
    unscoped = AzureEntraTokenProvider(fetcher=lambda scope: ("test-token-placeholder", 5000.0))
    with pytest.raises(ImproperConfigurationError, match="scope"):
        await unscoped.resolve_async()


@pytest.mark.parametrize(
    ("status_code", "response_body"),
    [
        (200, b"not-valid-json"),
        (200, encode_json_bytes(["unexpected-list"])),
        (200, encode_json_bytes({"expires_in": 300})),
        (200, encode_json_bytes({"access_token": "test-token-placeholder-secret", "expires_in": "bogus"})),
        (200, encode_json_bytes({"access_token": "test-token-placeholder-secret"})),
        (401, b'{"error":"invalid_client","secret":"test-client-secret-placeholder"}'),
    ],
)
def test_azure_entra_token_provider_malformed_json_errors_redact_secrets(
    status_code: int,
    response_body: bytes,
) -> None:
    """Verify malformed and error token JSON payloads fail loudly without leaking client secrets."""

    class _MalformedHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            self.rfile.read(length)
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _MalformedHandler)
    port = server.server_address[1]
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()
    try:
        provider = AzureEntraTokenProvider(
            tenant_id="contoso.onmicrosoft.com",
            client_id="cid",
            client_secret="test-client-secret-placeholder",
            scope=AZURE_POSTGRES_SCOPE,
            token_endpoint=f"http://127.0.0.1:{port}/token",
        )
        with pytest.raises(ImproperConfigurationError) as exc_info:
            provider.resolve_sync()
        err_text = str(exc_info.value)
        assert "test-client-secret-placeholder" not in err_text
        assert "test-token-placeholder-secret" not in err_text
    finally:
        server.shutdown()
        server.server_close()
        srv_thread.join(timeout=2.0)


def test_aws_rds_root_ca_pem_loads_into_ssl_context() -> None:
    """Verify bundled AWS RDS Global Root CA PEM loads cleanly into ssl.SSLContext."""
    pem = aws_rds_root_ca_pem()
    assert pem.startswith("-----BEGIN CERTIFICATE-----")
    assert pem.count("-----BEGIN CERTIFICATE-----") >= 3

    ctx = ssl.create_default_context(cadata=pem)
    certs = ctx.get_ca_certs()
    assert len(certs) >= 3
    common_names: set[str] = set()
    for cert in certs:
        subject = cert.get("subject", ())
        if isinstance(subject, tuple):
            for rdn in subject:
                if isinstance(rdn, tuple):
                    for pair in rdn:
                        if isinstance(pair, tuple) and len(pair) == 2 and pair[0] == "commonName":
                            common_names.add(str(pair[1]))
    assert any("2019" in cn or "Root CA" in cn for cn in common_names)
    assert any("RSA2048 G1" in cn for cn in common_names)
    assert any("ECC384 G1" in cn for cn in common_names)


@pytest.mark.parametrize(
    (
        "endpoint_host",
        "port",
        "region",
        "username",
        "expected_host",
        "expected_port",
        "expected_region",
        "expected_id",
        "expected_cluster",
        "expected_proxy",
    ),
    [
        (
            "mydb.c9akciq32.us-east-1.rds.amazonaws.com",
            None,
            None,
            "iam_user",
            "mydb.c9akciq32.us-east-1.rds.amazonaws.com",
            5432,
            "us-east-1",
            "mydb",
            False,
            False,
        ),
        (
            "orders.cluster-abc123xyz.eu-west-1.rds.amazonaws.com",
            3306,
            None,
            "aurora_writer",
            "orders.cluster-abc123xyz.eu-west-1.rds.amazonaws.com",
            3306,
            "eu-west-1",
            "orders",
            True,
            False,
        ),
        (
            "orders.cluster-ro-abc123xyz.cn-north-1.rds.amazonaws.com.cn",
            None,
            None,
            "aurora_reader",
            "orders.cluster-ro-abc123xyz.cn-north-1.rds.amazonaws.com.cn",
            5432,
            "cn-north-1",
            "orders",
            True,
            False,
        ),
        (
            "app-proxy.proxy-xyz789.ap-northeast-1.rds.amazonaws.com:5432",
            None,
            None,
            "proxy_user",
            "app-proxy.proxy-xyz789.ap-northeast-1.rds.amazonaws.com",
            5432,
            "ap-northeast-1",
            "app-proxy",
            False,
            True,
        ),
        (
            "ro-ep.endpoint.proxy-xyz789.us-gov-west-1.rds.amazonaws.com",
            None,
            None,
            "gov_user",
            "ro-ep.endpoint.proxy-xyz789.us-gov-west-1.rds.amazonaws.com",
            5432,
            "us-gov-west-1",
            "ro-ep",
            False,
            True,
        ),
        (
            "db.internal.corp",
            15432,
            "us-west-2",
            "explicit_user",
            "db.internal.corp",
            15432,
            "us-west-2",
            None,
            False,
            False,
        ),
    ],
)
def test_native_aws_rds_dialer_endpoint_parsing(
    endpoint_host: str,
    port: int | None,
    region: str | None,
    username: str,
    expected_host: str,
    expected_port: int,
    expected_region: str,
    expected_id: str | None,
    expected_cluster: bool,
    expected_proxy: bool,
) -> None:
    """Verify AwsRdsDialer parses RDS, Aurora, RDS Proxy, China, GovCloud, and explicit endpoints."""
    dialer = AwsRdsDialer(endpoint_host, port=port, region=region, username=username)
    assert dialer.endpoint_host == expected_host
    assert dialer.port == expected_port
    assert dialer.server_port == expected_port
    assert dialer.region == expected_region
    assert dialer.username == username
    assert dialer.identifier == expected_id
    assert dialer.is_cluster_endpoint is expected_cluster
    assert dialer.is_proxy_endpoint is expected_proxy
    assert dialer.ca_pem == aws_rds_root_ca_pem()


@pytest.mark.parametrize(
    (
        "endpoint_host",
        "port",
        "region",
        "admin",
        "expected_host",
        "expected_port",
        "expected_region",
        "expected_cluster",
        "expected_action",
    ),
    [
        (
            "abc123def456.dsql.us-east-1.on.aws",
            None,
            None,
            False,
            "abc123def456.dsql.us-east-1.on.aws",
            5432,
            "us-east-1",
            "abc123def456",
            "DbConnect",
        ),
        (
            "abc123def456.dsql-fnh4.us-west-2.on.aws",
            None,
            None,
            True,
            "abc123def456.dsql-fnh4.us-west-2.on.aws",
            5432,
            "us-west-2",
            "abc123def456",
            "DbConnectAdmin",
        ),
        (
            "127.0.0.1",
            15432,
            "eu-central-1",
            True,
            "127.0.0.1",
            15432,
            "eu-central-1",
            None,
            "DbConnectAdmin",
        ),
    ],
)
def test_native_aws_aurora_dsql_dialer_endpoint_parsing(
    endpoint_host: str,
    port: int | None,
    region: str | None,
    admin: bool,
    expected_host: str,
    expected_port: int,
    expected_region: str,
    expected_cluster: str | None,
    expected_action: str,
) -> None:
    """Verify AwsAuroraDsqlDialer parses DSQL cluster endpoints, default port 5432, and admin actions."""
    dialer = AwsAuroraDsqlDialer(endpoint_host, port=port, region=region, admin=admin)
    assert dialer.endpoint_host == expected_host
    assert dialer.hostname == expected_host
    assert dialer.port == expected_port
    assert dialer.server_port == expected_port
    assert dialer.region == expected_region
    assert dialer.cluster_id == expected_cluster
    assert dialer.admin is admin
    assert dialer.action == expected_action


@pytest.mark.parametrize(
    (
        "endpoint_host",
        "port",
        "engine",
        "expected_host",
        "expected_port",
        "expected_engine",
        "expected_scope",
        "expected_server",
    ),
    [
        (
            "contoso-pg.postgres.database.azure.com",
            None,
            None,
            "contoso-pg.postgres.database.azure.com",
            5432,
            "postgres",
            AZURE_POSTGRES_SCOPE,
            "contoso-pg",
        ),
        (
            "contoso-mysql.mysql.database.azure.com",
            None,
            None,
            "contoso-mysql.mysql.database.azure.com",
            3306,
            "mysql",
            AZURE_MYSQL_SCOPE,
            "contoso-mysql",
        ),
        (
            "contoso-sql.database.windows.net",
            None,
            None,
            "contoso-sql.database.windows.net",
            1433,
            "mssql",
            AZURE_MSSQL_SCOPE,
            "contoso-sql",
        ),
        (
            "127.0.0.1",
            15432,
            "postgresql",
            "127.0.0.1",
            15432,
            "postgres",
            AZURE_POSTGRES_SCOPE,
            None,
        ),
        (
            "sql-privatelink.internal.corp",
            None,
            "sqlserver",
            "sql-privatelink.internal.corp",
            1433,
            "mssql",
            AZURE_MSSQL_SCOPE,
            None,
        ),
    ],
)
def test_native_azure_entra_dialer_endpoint_parsing(
    endpoint_host: str,
    port: int | None,
    engine: str | None,
    expected_host: str,
    expected_port: int,
    expected_engine: str,
    expected_scope: str,
    expected_server: str | None,
) -> None:
    """Verify AzureEntraDialer resolves canonical OAuth2 scopes and default ports across engines."""
    dialer = AzureEntraDialer(endpoint_host, port=port, engine=engine)
    assert dialer.endpoint_host == expected_host
    assert dialer.port == expected_port
    assert dialer.server_port == expected_port
    assert dialer.engine == expected_engine
    assert dialer.scope == expected_scope
    assert dialer.server_name == expected_server


@pytest.mark.parametrize(
    ("factory", "kwargs"),
    [
        (AwsRdsDialer, {"endpoint_host": "https://mydb.c1.us-east-1.rds.amazonaws.com"}),
        (AwsRdsDialer, {"endpoint_host": "postgres://mydb.c1.us-east-1.rds.amazonaws.com"}),
        (AwsRdsDialer, {"endpoint_host": " mydb.c1.us-east-1.rds.amazonaws.com"}),
        (AwsRdsDialer, {"endpoint_host": "my db.c1.us-east-1.rds.amazonaws.com"}),
        (AwsRdsDialer, {"endpoint_host": "mydb.c1.us-east-1.rds.amazonaws.com", "port": 0}),
        (AwsRdsDialer, {"endpoint_host": "mydb.c1.us-east-1.rds.amazonaws.com", "port": -1}),
        (AwsRdsDialer, {"endpoint_host": "mydb.c1.us-east-1.rds.amazonaws.com", "port": 70000}),
        (AwsRdsDialer, {"endpoint_host": "mydb.c1.us-east-1.rds.amazonaws.com:0"}),
        (AwsRdsDialer, {"endpoint_host": "mydb.us-east-1.rds.amazonaws.com"}),
        (AwsRdsDialer, {"endpoint_host": "custom.internal.corp"}),
        (AwsRdsDialer, {"endpoint_host": "mydb.c1.us-east-1.rds.amazonaws.com", "username": "   "}),
        (AwsAuroraDsqlDialer, {"endpoint_host": "https://abc123.dsql.us-east-1.on.aws"}),
        (AwsAuroraDsqlDialer, {"endpoint_host": " abc123.dsql.us-east-1.on.aws"}),
        (AwsAuroraDsqlDialer, {"endpoint_host": "abc123.dsql.us-east-1.on.aws", "port": 0}),
        (AwsAuroraDsqlDialer, {"endpoint_host": "abc123.s3.us-east-1.on.aws"}),
        (AwsAuroraDsqlDialer, {"endpoint_host": "dsql.us-east-1.on.aws"}),
        (AwsAuroraDsqlDialer, {"endpoint_host": "custom.internal.corp"}),
        (AzureEntraDialer, {"endpoint_host": "https://pg.postgres.database.azure.com"}),
        (AzureEntraDialer, {"endpoint_host": "pg.postgres.database.azure.com "}),
        (AzureEntraDialer, {"endpoint_host": "pg.postgres.database.azure.com", "port": 0}),
        (AzureEntraDialer, {"endpoint_host": "postgres.database.azure.com"}),
        (AzureEntraDialer, {"endpoint_host": "database.windows.net"}),
        (AzureEntraDialer, {"endpoint_host": "pg.postgres.database.azure.com", "engine": "mssql"}),
        (AzureEntraDialer, {"endpoint_host": "127.0.0.1", "engine": "oracle"}),
        (AzureEntraDialer, {"endpoint_host": "custom.internal.corp"}),
    ],
)
def test_native_aws_and_azure_dialer_invalid_endpoints_raise_value_error(
    factory: Any,
    kwargs: dict[str, Any],
) -> None:
    """Verify schemes, whitespace, port 0, and malformed hostnames raise ValueError."""
    with pytest.raises(ValueError):
        factory(**kwargs)


@pytest.mark.parametrize(
    ("endpoint_host", "port", "region", "username", "session_token", "ttl_seconds", "now_ts"),
    [
        (
            "mydb.c9akciq32.us-east-1.rds.amazonaws.com",
            5432,
            None,
            "iam_app_user",
            None,
            900,
            1_700_000_000.0,
        ),
        (
            "orders.cluster-abc123.cn-north-1.rds.amazonaws.com.cn",
            3306,
            None,
            "iam.reader+role",
            "test-token-placeholder/with+special=chars==",
            600,
            1_715_123_456.0,
        ),
        (
            "127.0.0.1",
            15432,
            "us-gov-west-1",
            "gov_db_user",
            "test-token-placeholder",
            300,
            1_725_000_001.0,
        ),
    ],
)
def test_native_aws_rds_sigv4_vectors_match_python_identically(
    endpoint_host: str,
    port: int,
    region: str | None,
    username: str,
    session_token: str | None,
    ttl_seconds: int,
    now_ts: float,
) -> None:
    """Verify Rust AwsRdsDialer.sign_iam_token matches Python SigV4 vectors byte-for-byte."""
    dialer = AwsRdsDialer(endpoint_host, port=port, region=region, username=username)
    rust_token = dialer.sign_iam_token(
        access_key_id="test-access-key-placeholder",
        secret_access_key="test-secret-key-placeholder",
        session_token=session_token,
        ttl_seconds=ttl_seconds,
        now_ts=now_ts,
    )
    py_token = _sign_sigv4_presigned_token(
        host_header=f"{dialer.endpoint_host}:{dialer.port}",
        region=dialer.region,
        service="rds-db",
        action="connect",
        ttl_seconds=ttl_seconds,
        now_ts=now_ts,
        access_key_id="test-access-key-placeholder",
        secret_access_key="test-secret-key-placeholder",
        session_token=session_token,
        db_user=username,
    )
    provider = AwsRdsIamTokenProvider(
        endpoint_host=dialer.endpoint_host,
        port=dialer.port,
        region=dialer.region,
        username=username,
        access_key_id="test-access-key-placeholder",
        secret_access_key="test-secret-key-placeholder",
        session_token=session_token,
        token_ttl_seconds=ttl_seconds,
        refresh_margin_seconds=10.0,
        clock=lambda: now_ts,
    )
    assert rust_token == py_token
    assert rust_token == provider.resolve_sync()


@pytest.mark.parametrize(
    ("endpoint_host", "admin", "session_token", "ttl_seconds", "now_ts"),
    [
        (
            "abc123def456.dsql.us-east-1.on.aws",
            False,
            None,
            900,
            1_700_000_000.0,
        ),
        (
            "abc123def456.dsql-fnh4.us-west-2.on.aws",
            True,
            "test-token-placeholder/sts+session==",
            3600,
            1_715_123_456.0,
        ),
    ],
)
def test_native_aws_aurora_dsql_sigv4_vectors_match_python_identically(
    endpoint_host: str,
    admin: bool,
    session_token: str | None,
    ttl_seconds: int,
    now_ts: float,
) -> None:
    """Verify Rust AwsAuroraDsqlDialer.sign_iam_token omits DBUser and matches Python SigV4 vectors."""
    dialer = AwsAuroraDsqlDialer(endpoint_host, admin=admin)
    rust_token = dialer.sign_iam_token(
        access_key_id="test-access-key-placeholder",
        secret_access_key="test-secret-key-placeholder",
        session_token=session_token,
        ttl_seconds=ttl_seconds,
        now_ts=now_ts,
    )
    py_token = _sign_sigv4_presigned_token(
        host_header=dialer.hostname,
        region=dialer.region,
        service="dsql",
        action="DbConnectAdmin" if admin else "DbConnect",
        ttl_seconds=ttl_seconds,
        now_ts=now_ts,
        access_key_id="test-access-key-placeholder",
        secret_access_key="test-secret-key-placeholder",
        session_token=session_token,
    )
    assert "DBUser=" not in rust_token
    assert rust_token == py_token
    if ttl_seconds <= 900:
        provider = AwsAuroraDsqlIamTokenProvider(
            hostname=dialer.hostname,
            region=dialer.region,
            admin=admin,
            access_key_id="test-access-key-placeholder",
            secret_access_key="test-secret-key-placeholder",
            session_token=session_token,
            token_ttl_seconds=ttl_seconds,
            refresh_margin_seconds=10.0,
            clock=lambda: now_ts,
        )
        assert rust_token == provider.resolve_sync()


def _generate_san_tls_credentials(dns_name: str) -> tuple[str, str]:
    """Generate a self-signed X.509 certificate and private key with a DNS SubjectAltName."""
    with tempfile.TemporaryDirectory(prefix="vtb-san-tls-") as tmpdir:
        key_path = Path(tmpdir) / "key.pem"
        cert_path = Path(tmpdir) / "cert.pem"
        cmd = [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key_path),
            "-out",
            str(cert_path),
            "-days",
            "1",
            "-subj",
            f"/CN={dns_name}",
            "-addext",
            f"subjectAltName=DNS:{dns_name}",
        ]
        subprocess.run(cmd, check=True, capture_output=True, timeout=30)
        return cert_path.read_text(encoding="utf-8"), key_path.read_text(encoding="utf-8")


def _build_tls13_server_context(cert_pem: str, key_pem: str) -> ssl.SSLContext:
    """Create a server SSLContext restricted to TLS 1.3 for loopback handshake tests."""
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    with tempfile.TemporaryDirectory(prefix="vtb-srv-san-") as tmpdir:
        cert_file = Path(tmpdir) / "cert.pem"
        key_file = Path(tmpdir) / "key.pem"
        cert_file.write_text(cert_pem, encoding="utf-8")
        key_file.write_text(key_pem, encoding="utf-8")
        server_ctx.load_cert_chain(str(cert_file), str(key_file))
    return server_ctx


def _serve_single_tls_greeting(server_sock: socket.socket, server_ctx: ssl.SSLContext, greeting: bytes) -> None:
    """Accept one TCP connection, complete or absorb a TLS 1.3 handshake, and write a greeting."""
    raw_conn, _ = server_sock.accept()
    try:
        with server_ctx.wrap_socket(raw_conn, server_side=True) as tls_conn:
            tls_conn.sendall(greeting)
    except OSError:
        raw_conn.close()


def _make_cloud_dialer_pair(
    kind: str,
    *,
    endpoint_host: str,
    port: int,
    ca_pem: str | None,
    clock: Any = None,
) -> tuple[Any, Any]:
    """Build matching sync and async AWS or Azure dialers for parameterized handshake and token tests."""
    now_fn = clock or (lambda: 1_794_873_600.0)
    if kind == "aws_rds":
        common_rds: dict[str, Any] = {
            "port": port,
            "username": "iam_app_user",
            "ca_pem": ca_pem,
            "access_key_id": "test-access-key-placeholder",
            "secret_access_key": "test-secret-key-placeholder",
            "session_token": "test-token-placeholder",
            "token_ttl_seconds": 900,
            "refresh_margin_seconds": 60.0,
            "clock": now_fn,
        }
        return SyncAwsRdsDialer(endpoint_host, **common_rds), AsyncAwsRdsDialer(endpoint_host, **common_rds)
    if kind == "aws_dsql":
        common_dsql: dict[str, Any] = {
            "port": port,
            "admin": True,
            "ca_pem": ca_pem,
            "access_key_id": "test-access-key-placeholder",
            "secret_access_key": "test-secret-key-placeholder",
            "session_token": "test-token-placeholder",
            "token_ttl_seconds": 900,
            "refresh_margin_seconds": 60.0,
            "clock": now_fn,
        }
        return (
            SyncAwsAuroraDsqlDialer(endpoint_host, **common_dsql),
            AsyncAwsAuroraDsqlDialer(endpoint_host, **common_dsql),
        )
    seq = [0]

    def _entra_fetcher(scope: str) -> dict[str, Any]:
        seq[0] += 1
        return {
            "access_token": f"test-token-placeholder-{seq[0]}:{scope}",
            "expires_in": 900,
        }

    common_azure: dict[str, Any] = {
        "port": port,
        "username": "entra_user",
        "ca_pem": ca_pem,
        "refresh_margin_seconds": 120.0,
        "clock": now_fn,
        "fetcher": _entra_fetcher,
    }
    return SyncAzureEntraDialer(endpoint_host, **common_azure), AsyncAzureEntraDialer(endpoint_host, **common_azure)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("dialer_kind", "endpoint_host", "mismatch_host"),
    [
        (
            "aws_rds",
            "orders.cluster-c9akciq32.us-east-1.rds.amazonaws.com",
            "other.cluster-c9akciq32.us-east-1.rds.amazonaws.com",
        ),
        (
            "aws_dsql",
            "abc123def456.dsql.us-east-1.on.aws",
            "xyz987uvw654.dsql.us-east-1.on.aws",
        ),
        (
            "azure_entra",
            "contoso-pg.postgres.database.azure.com",
            "other-pg.postgres.database.azure.com",
        ),
    ],
)
async def test_aws_and_azure_dialers_tls13_verify_full_untrusted_and_hostname_mismatch(
    dialer_kind: str,
    endpoint_host: str,
    mismatch_host: str,
) -> None:
    """Verify TLS 1.3 handshake, untrusted CA rejection, and hostname mismatch failure across all six dialers."""
    cert_pem, key_pem = _generate_san_tls_credentials(endpoint_host)
    untrusted_ca_pem, _ = _generate_san_tls_credentials("untrusted-root.internal")
    server_ctx = _build_tls13_server_context(cert_pem, key_pem)

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(12)
    port = server_sock.getsockname()[1]

    try:
        sync_dialer, async_dialer = _make_cloud_dialer_pair(
            dialer_kind,
            endpoint_host=endpoint_host,
            port=port,
            ca_pem=cert_pem,
        )
        assert sync_dialer.ssl_context.verify_mode == ssl.CERT_REQUIRED
        assert sync_dialer.ssl_context.check_hostname is True
        assert sync_dialer.ssl_context.minimum_version == ssl.TLSVersion.TLSv1_2
        assert "test-token-placeholder" not in repr(sync_dialer)
        assert "test-secret-key-placeholder" not in repr(sync_dialer)

        t_sync_ok = threading.Thread(
            target=_serve_single_tls_greeting,
            args=(server_sock, server_ctx, b"SYNC_TLS13_OK"),
            daemon=True,
        )
        t_sync_ok.start()
        with sync_dialer.connect("127.0.0.1") as tls_sock:
            assert tls_sock.version() == "TLSv1.3"
            assert tls_sock.recv(13) == b"SYNC_TLS13_OK"
        t_sync_ok.join(timeout=2.0)

        t_async_ok = threading.Thread(
            target=_serve_single_tls_greeting,
            args=(server_sock, server_ctx, b"ASYNC_TLS13_OK"),
            daemon=True,
        )
        t_async_ok.start()
        reader, writer = await async_dialer.connect("127.0.0.1")
        ssl_obj = writer.get_extra_info("ssl_object")
        assert ssl_obj is not None
        assert ssl_obj.version() == "TLSv1.3"
        assert await reader.readexactly(14) == b"ASYNC_TLS13_OK"
        writer.close()
        await writer.wait_closed()
        t_async_ok.join(timeout=2.0)

        default_untrusted_sync, default_untrusted_async = _make_cloud_dialer_pair(
            dialer_kind,
            endpoint_host=endpoint_host,
            port=port,
            ca_pem=None if dialer_kind == "aws_rds" else untrusted_ca_pem,
        )
        if dialer_kind == "aws_rds":
            assert default_untrusted_sync.ca_pem == aws_rds_root_ca_pem()

        t_sync_untrusted = threading.Thread(
            target=_serve_single_tls_greeting,
            args=(server_sock, server_ctx, b"NEVER"),
            daemon=True,
        )
        t_sync_untrusted.start()
        with pytest.raises(ssl.SSLCertVerificationError):
            default_untrusted_sync.connect("127.0.0.1")
        t_sync_untrusted.join(timeout=2.0)

        t_async_untrusted = threading.Thread(
            target=_serve_single_tls_greeting,
            args=(server_sock, server_ctx, b"NEVER"),
            daemon=True,
        )
        t_async_untrusted.start()
        with pytest.raises(ssl.SSLCertVerificationError):
            await default_untrusted_async.connect("127.0.0.1")
        t_async_untrusted.join(timeout=2.0)

        mismatch_sync, mismatch_async = _make_cloud_dialer_pair(
            dialer_kind,
            endpoint_host=mismatch_host,
            port=port,
            ca_pem=cert_pem,
        )

        t_sync_mismatch = threading.Thread(
            target=_serve_single_tls_greeting,
            args=(server_sock, server_ctx, b"NEVER"),
            daemon=True,
        )
        t_sync_mismatch.start()
        with pytest.raises(ssl.SSLCertVerificationError):
            mismatch_sync.connect("127.0.0.1")
        t_sync_mismatch.join(timeout=2.0)

        t_async_mismatch = threading.Thread(
            target=_serve_single_tls_greeting,
            args=(server_sock, server_ctx, b"NEVER"),
            daemon=True,
        )
        t_async_mismatch.start()
        with pytest.raises(ssl.SSLCertVerificationError):
            await mismatch_async.connect("127.0.0.1")
        t_async_mismatch.join(timeout=2.0)
    finally:
        server_sock.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("dialer_kind", "endpoint_host", "pre_margin_ts", "rotate_ts"),
    [
        ("aws_rds", "orders.cluster-c9akciq32.us-east-1.rds.amazonaws.com", 1_500.0, 1_845.0),
        ("aws_dsql", "abc123def456.dsql.us-east-1.on.aws", 1_500.0, 1_845.0),
        ("azure_entra", "contoso-pg.postgres.database.azure.com", 1_500.0, 1_785.0),
    ],
)
async def test_aws_and_azure_dialers_pre_expiry_token_rotation(
    dialer_kind: str,
    endpoint_host: str,
    pre_margin_ts: float,
    rotate_ts: float,
) -> None:
    """Verify sync and async resolve_token() cache valid tokens and rotate before expiry."""
    sync_now = [1_000.0]
    async_now = [1_000.0]
    sync_dialer, _ = _make_cloud_dialer_pair(
        dialer_kind,
        endpoint_host=endpoint_host,
        port=5432,
        ca_pem=None,
        clock=lambda: sync_now[0],
    )
    _, async_dialer = _make_cloud_dialer_pair(
        dialer_kind,
        endpoint_host=endpoint_host,
        port=5432,
        ca_pem=None,
        clock=lambda: async_now[0],
    )

    first_sync = sync_dialer.resolve_token()
    sync_now[0] = pre_margin_ts
    assert sync_dialer.resolve_token() == first_sync

    sync_now[0] = rotate_ts
    rotated_sync = sync_dialer.resolve_token()
    assert rotated_sync != first_sync

    sync_now[0] = rotate_ts + 5.0
    assert sync_dialer.resolve_token() == rotated_sync
    sync_dialer.close()
    sync_now[0] = rotate_ts + 10.0
    after_close_sync = sync_dialer.resolve_token()
    assert after_close_sync != rotated_sync

    first_async = await async_dialer.resolve_token()
    async_now[0] = pre_margin_ts
    assert await async_dialer.resolve_token() == first_async

    async_now[0] = rotate_ts
    rotated_async = await async_dialer.resolve_token()
    assert rotated_async != first_async

    await async_dialer.close()
    async_now[0] = rotate_ts + 10.0
    after_close_async = await async_dialer.resolve_token()
    assert after_close_async != rotated_async


@pytest.mark.parametrize(
    ("endpoint_host", "explicit_authority", "env_authority", "expected_authority"),
    [
        ("contoso-pg.postgres.database.azure.com", None, None, None),
        ("gov-pg.postgres.database.usgovcloudapi.net", None, None, "https://login.microsoftonline.us"),
        ("cn-sql.database.chinacloudapi.cn", None, None, "https://login.chinacloudapi.cn"),
        (
            "gov-pg.postgres.database.usgovcloudapi.net",
            None,
            "https://login.example.test",
            "https://login.example.test",
        ),
        (
            "gov-pg.postgres.database.usgovcloudapi.net",
            "https://login.override.test",
            "https://login.example.test",
            "https://login.override.test",
        ),
    ],
)
def test_azure_entra_dialer_selects_authority_host_for_endpoint_cloud(
    monkeypatch: pytest.MonkeyPatch,
    endpoint_host: str,
    explicit_authority: str | None,
    env_authority: str | None,
    expected_authority: str | None,
) -> None:
    """Verify Azure dialers pick the Entra authority for sovereign clouds unless one is set explicitly."""
    if env_authority is None:
        monkeypatch.delenv("AZURE_AUTHORITY_HOST", raising=False)
    else:
        monkeypatch.setenv("AZURE_AUTHORITY_HOST", env_authority)

    dialer = SyncAzureEntraDialer(
        endpoint_host,
        tenant_id="contoso.onmicrosoft.com",
        client_id="cid",
        client_secret="test-client-secret-placeholder",
        authority_host=explicit_authority,
    )
    provider = dialer.token_provider
    assert isinstance(provider, AzureEntraTokenProvider)
    assert provider.authority_host == expected_authority


@pytest.mark.parametrize(
    ("endpoint_host", "engine", "expected_engine", "expected_port", "expected_scope"),
    [
        ("contoso-pg.postgres.database.azure.com", None, "postgres", 5432, AZURE_POSTGRES_SCOPE),
        ("contoso-mysql.mysql.database.azure.com", None, "mysql", 3306, AZURE_MYSQL_SCOPE),
        ("contoso-sql.database.windows.net", None, "mssql", 1433, AZURE_MSSQL_SCOPE),
        ("privatelink-sql.internal.corp", "sqlserver", "mssql", 1433, AZURE_MSSQL_SCOPE),
        (
            "gov-pg.postgres.database.usgovcloudapi.net",
            None,
            "postgres",
            5432,
            "https://ossrdbms-aad.database.usgovcloudapi.net/.default",
        ),
        (
            "gov-sql.database.usgovcloudapi.net",
            None,
            "mssql",
            1433,
            "https://database.usgovcloudapi.net/.default",
        ),
        (
            "cn-mysql.mysql.database.chinacloudapi.cn",
            None,
            "mysql",
            3306,
            "https://ossrdbms-aad.database.chinacloudapi.cn/.default",
        ),
    ],
)
def test_azure_entra_dialer_scope_selection_and_connection_kwargs(
    endpoint_host: str,
    engine: str | None,
    expected_engine: str,
    expected_port: int,
    expected_scope: str,
) -> None:
    """Verify Azure dialers auto-select engine scope, inject unscoped providers, and build connection_kwargs."""
    seen_scopes: list[str] = []

    def _capture_scope(scope: str) -> dict[str, Any]:
        seen_scopes.append(scope)
        return {"access_token": "test-token-placeholder", "expires_in": 600}

    unscoped_provider = AzureEntraTokenProvider(fetcher=_capture_scope)
    dialer = SyncAzureEntraDialer(
        endpoint_host,
        engine=engine,
        username="entra_app",
        database="orders",
        token_provider=unscoped_provider,
    )
    assert dialer.engine == expected_engine
    assert dialer.port == expected_port
    assert dialer.scope == expected_scope
    assert dialer.resolve_token() == "test-token-placeholder"
    assert seen_scopes == [expected_scope]

    kwargs = dialer.connection_kwargs()
    assert kwargs["token_provider"] is dialer.token_provider
    if expected_engine == "postgres":
        assert kwargs["dsn"] == f"postgresql://entra_app@{endpoint_host}:5432/orders?sslmode=verify-full"
    elif expected_engine == "mysql":
        assert kwargs["ssl_mode"] == "verify-full"
        assert kwargs["port"] == 3306
        assert kwargs["database"] == "orders"
    else:
        assert kwargs["encryption"] == "required"
        assert kwargs["trust_server_certificate"] is False
        assert kwargs["port"] == 1433
        assert kwargs["database"] == "orders"


def test_aws_dialers_connection_kwargs_and_lazy_package_exports() -> None:
    """Verify AWS RDS Postgres/MySQL and DSQL connection_kwargs plus vertebra.db.dialer lazy exports."""
    rds_pg = SyncAwsRdsDialer(
        "orders.c9akciq32.us-east-1.rds.amazonaws.com",
        username="iam_pg",
        database="appdb",
        access_key_id="test-access-key-placeholder",
        secret_access_key="test-secret-key-placeholder",
    )
    pg_kwargs = rds_pg.connection_kwargs()
    assert (
        pg_kwargs["dsn"]
        == "postgresql://iam_pg@orders.c9akciq32.us-east-1.rds.amazonaws.com:5432/appdb?sslmode=verify-full"
    )
    assert pg_kwargs["ca_pem"] == aws_rds_root_ca_pem()
    assert pg_kwargs["token_provider"] is rds_pg.token_provider

    rds_mysql = SyncAwsRdsDialer(
        "orders.c9akciq32.us-east-1.rds.amazonaws.com",
        engine="mysql",
        username="iam_mysql",
        database="shop",
        access_key_id="test-access-key-placeholder",
        secret_access_key="test-secret-key-placeholder",
    )
    mysql_kwargs = rds_mysql.connection_kwargs()
    assert rds_mysql.port == 3306
    assert mysql_kwargs["ssl_mode"] == "verify-full"
    assert mysql_kwargs["ssl_ca"] == aws_rds_root_ca_pem()
    assert mysql_kwargs["user"] == "iam_mysql"

    dsql = SyncAwsAuroraDsqlDialer(
        "abc123def456.dsql.us-east-1.on.aws",
        admin=False,
        access_key_id="test-access-key-placeholder",
        secret_access_key="test-secret-key-placeholder",
    )
    dsql_kwargs = dsql.connection_kwargs(database="analytics")
    assert dsql.action == "DbConnect"
    assert (
        dsql_kwargs["dsn"]
        == "postgresql://postgres@abc123def456.dsql.us-east-1.on.aws:5432/analytics?sslmode=verify-full"
    )
    assert "ca_pem" not in dsql_kwargs

    expected_exports = {
        "SyncAwsRdsDialer": SyncAwsRdsDialer,
        "AsyncAwsRdsDialer": AsyncAwsRdsDialer,
        "SyncAwsAuroraDsqlDialer": SyncAwsAuroraDsqlDialer,
        "AsyncAwsAuroraDsqlDialer": AsyncAwsAuroraDsqlDialer,
        "SyncAzureEntraDialer": SyncAzureEntraDialer,
        "AsyncAzureEntraDialer": AsyncAzureEntraDialer,
        "aws_rds_root_ca_pem": aws_rds_root_ca_pem,
    }
    for symbol_name, symbol_obj in expected_exports.items():
        assert symbol_name in dialer_pkg.__all__
        assert symbol_name in dir(dialer_pkg)
        assert getattr(dialer_pkg, symbol_name) is symbol_obj

    with pytest.raises(ImproperConfigurationError):
        SyncAwsRdsDialer("orders.c9akciq32.us-east-1.rds.amazonaws.com", ca_pem="   ")
    with pytest.raises(ImproperConfigurationError):
        SyncAzureEntraDialer("contoso-pg.postgres.database.azure.com", port=True)


@pytest.mark.parametrize(
    ("symbol_name", "expected_symbol"),
    [
        ("SyncAwsRdsDialer", SyncAwsRdsDialer),
        ("AsyncAwsRdsDialer", AsyncAwsRdsDialer),
        ("SyncAwsAuroraDsqlDialer", SyncAwsAuroraDsqlDialer),
        ("AsyncAwsAuroraDsqlDialer", AsyncAwsAuroraDsqlDialer),
        ("SyncAzureEntraDialer", SyncAzureEntraDialer),
        ("AsyncAzureEntraDialer", AsyncAzureEntraDialer),
        ("aws_rds_root_ca_pem", aws_rds_root_ca_pem),
    ],
)
def test_vertebra_db_top_level_cloud_dialer_exports(symbol_name: str, expected_symbol: Any) -> None:
    """Verify vertebra.db re-exports AWS and Azure dialers and aws_rds_root_ca_pem in __all__ and __dir__."""
    assert symbol_name in db_pkg.__all__
    assert symbol_name in dir(db_pkg)
    assert getattr(db_pkg, symbol_name) is expected_symbol


def test_vertebra_db_lazy_cloud_dialer_resolution_in_fresh_interpreter() -> None:
    """Verify importing vertebra.db keeps dialer submodules lazy until AWS and Azure symbols are imported."""
    script = (
        "import sys\n"
        "import vertebra.db\n"
        "for mod in ('vertebra.db.dialer', 'vertebra.db.dialer.aws', 'vertebra.db.dialer.azure'):\n"
        "    assert mod not in sys.modules, f'{mod} loaded eagerly'\n"
        "from vertebra.db import (\n"
        "    SyncAwsRdsDialer,\n"
        "    AsyncAwsRdsDialer,\n"
        "    SyncAwsAuroraDsqlDialer,\n"
        "    AsyncAwsAuroraDsqlDialer,\n"
        "    SyncAzureEntraDialer,\n"
        "    AsyncAzureEntraDialer,\n"
        "    aws_rds_root_ca_pem,\n"
        ")\n"
        "expected = {\n"
        "    'SyncAwsRdsDialer': SyncAwsRdsDialer,\n"
        "    'AsyncAwsRdsDialer': AsyncAwsRdsDialer,\n"
        "    'SyncAwsAuroraDsqlDialer': SyncAwsAuroraDsqlDialer,\n"
        "    'AsyncAwsAuroraDsqlDialer': AsyncAwsAuroraDsqlDialer,\n"
        "    'SyncAzureEntraDialer': SyncAzureEntraDialer,\n"
        "    'AsyncAzureEntraDialer': AsyncAzureEntraDialer,\n"
        "    'aws_rds_root_ca_pem': aws_rds_root_ca_pem,\n"
        "}\n"
        "for name, obj in expected.items():\n"
        "    assert name in vertebra.db.__all__\n"
        "    assert getattr(vertebra.db, name) is obj\n"
        "assert 'vertebra.db.dialer.aws' in sys.modules\n"
        "assert 'vertebra.db.dialer.azure' in sys.modules\n"
        "assert aws_rds_root_ca_pem().startswith('-----BEGIN CERTIFICATE-----')\n"
    )
    proc = subprocess.run([sys.executable, "-c", script], check=False, capture_output=True, text=True, timeout=30)
    assert proc.returncode == 0, proc.stderr


_DSQL_TEST_ENDPOINT = "abc123def456.dsql.us-east-1.on.aws"
_AWS_TEST_KEYS: dict[str, Any] = {
    "access_key_id": "test-access-key-placeholder",
    "secret_access_key": "test-secret-key-placeholder",
}


@pytest.mark.parametrize("dialer_cls", [SyncAwsAuroraDsqlDialer, AsyncAwsAuroraDsqlDialer])
@pytest.mark.parametrize("native_admin", [True, False])
@pytest.mark.parametrize("rebuild_port", [None, 5433], ids=["reuse-native", "rebuild-native"])
def test_aws_aurora_dsql_dialer_wrapping_inherits_native_admin(
    dialer_cls: Any,
    native_admin: bool,
    rebuild_port: int | None,
) -> None:
    """Verify wrapping a native `AwsAuroraDsqlDialer` keeps its `admin` flag on the dialer and its token provider."""
    native = AwsAuroraDsqlDialer(_DSQL_TEST_ENDPOINT, admin=native_admin)
    dialer = dialer_cls(native, port=rebuild_port, **_AWS_TEST_KEYS)

    assert dialer.admin is native_admin
    assert dialer.token_provider.admin is native_admin
    assert dialer.action == ("DbConnectAdmin" if native_admin else "DbConnect")
    assert dialer.username == ("admin" if native_admin else "postgres")
    assert dialer_cls(native, admin=native_admin, port=rebuild_port, **_AWS_TEST_KEYS).admin is native_admin


@pytest.mark.parametrize("dialer_cls", [SyncAwsAuroraDsqlDialer, AsyncAwsAuroraDsqlDialer])
@pytest.mark.parametrize(
    ("native_admin", "provider_admin", "explicit_admin"),
    [
        (True, None, False),
        (False, None, True),
        (True, False, None),
        (None, True, False),
        (None, False, True),
    ],
)
def test_aws_aurora_dsql_dialer_rejects_conflicting_admin_sources(
    dialer_cls: Any,
    native_admin: bool | None,
    provider_admin: bool | None,
    explicit_admin: bool | None,
) -> None:
    """Verify an `admin` argument, wrapped dialer, and DSQL token provider that disagree fail instead of one winning."""
    endpoint: Any = (
        _DSQL_TEST_ENDPOINT if native_admin is None else AwsAuroraDsqlDialer(_DSQL_TEST_ENDPOINT, admin=native_admin)
    )
    kwargs: dict[str, Any] = dict(_AWS_TEST_KEYS)
    if provider_admin is not None:
        kwargs = {
            "token_provider": AwsAuroraDsqlIamTokenProvider(
                hostname=_DSQL_TEST_ENDPOINT,
                region="us-east-1",
                admin=provider_admin,
                **_AWS_TEST_KEYS,
            )
        }
    if explicit_admin is not None:
        kwargs["admin"] = explicit_admin
    with pytest.raises(ImproperConfigurationError, match="admin"):
        dialer_cls(endpoint, **kwargs)


@pytest.mark.parametrize("dialer_cls", [SyncAwsAuroraDsqlDialer, AsyncAwsAuroraDsqlDialer])
@pytest.mark.parametrize("provider_admin", [True, False])
def test_aws_aurora_dsql_dialer_admin_defaults_to_token_provider(dialer_cls: Any, provider_admin: bool) -> None:
    """Verify an omitted `admin` argument follows a supplied `AwsAuroraDsqlIamTokenProvider`."""
    provider = AwsAuroraDsqlIamTokenProvider(
        hostname=_DSQL_TEST_ENDPOINT,
        region="us-east-1",
        admin=provider_admin,
        **_AWS_TEST_KEYS,
    )
    dialer = dialer_cls(_DSQL_TEST_ENDPOINT, token_provider=provider)
    assert dialer.admin is provider_admin
    assert dialer.token_provider is provider


@pytest.mark.parametrize("dialer_cls", [SyncAwsRdsDialer, AsyncAwsRdsDialer])
@pytest.mark.parametrize(
    ("endpoint_host", "port", "engine", "expected_engine", "expected_port", "expected_user", "expected_db"),
    [
        ("orders.c9akciq32.us-east-1.rds.amazonaws.com:3306", None, None, "mysql", 3306, "admin", "mysql"),
        ("orders.c9akciq32.us-east-1.rds.amazonaws.com", 3306, None, "mysql", 3306, "admin", "mysql"),
        ("orders.c9akciq32.us-east-1.rds.amazonaws.com", None, "mysql", "mysql", 3306, "admin", "mysql"),
        (
            "orders.c9akciq32.us-east-1.rds.amazonaws.com:3306",
            None,
            "postgres",
            "postgres",
            3306,
            "postgres",
            "postgres",
        ),
        ("orders.c9akciq32.us-east-1.rds.amazonaws.com:5432", None, None, "postgres", 5432, "postgres", "postgres"),
        ("orders.c9akciq32.us-east-1.rds.amazonaws.com", None, None, "postgres", 5432, "postgres", "postgres"),
        ("orders.c9akciq32.us-east-1.rds.amazonaws.com:3307", None, "mariadb", "mysql", 3307, "admin", "mysql"),
    ],
)
def test_aws_rds_dialer_resolves_engine_before_default_username(
    dialer_cls: Any,
    endpoint_host: str,
    port: int | None,
    engine: str | None,
    expected_engine: str,
    expected_port: int,
    expected_user: str,
    expected_db: str,
) -> None:
    """Verify the default IAM username follows the engine resolved from `engine`, `port`, or an embedded port."""
    dialer = dialer_cls(endpoint_host, port=port, engine=engine, clock=lambda: 1_794_873_600.0, **_AWS_TEST_KEYS)

    assert dialer.engine == expected_engine
    assert dialer.port == expected_port
    assert dialer.username == expected_user
    assert dialer.database == expected_db
    assert dialer.token_provider.username == expected_user
    signed = dialer.token_provider.resolve_sync()
    assert parse_qs(urlsplit(f"https://{signed}").query)["DBUser"] == [expected_user]


def _make_http_error(status_code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url="https://sqladmin.googleapis.com/sql/v1beta4/projects/p/instances/i:generateEphemeralCert",
        code=status_code,
        msg=f"HTTP {status_code}",
        hdrs=HTTPMessage(),
        fp=io.BytesIO(b""),
    )


def test_connector_retry_default_exports_and_no_eager_optional_imports() -> None:
    """Verify retry default constants and ensure error classification never eagerly imports optional libraries."""
    assert DEFAULT_CONNECTOR_RETRY_COUNT == 3
    assert DEFAULT_CONNECTOR_RETRY_DELAY == 0.5
    assert DEFAULT_CONNECTOR_RETRY_MAX_DELAY == 5.0
    assert dialer_pkg.DEFAULT_CONNECTOR_RETRY_COUNT == 3
    assert dialer_pkg.DEFAULT_CONNECTOR_RETRY_DELAY == 0.5
    assert dialer_pkg.DEFAULT_CONNECTOR_RETRY_MAX_DELAY == 5.0

    optional_modules = (
        "google.cloud.sql.connector.exceptions",
        "google.cloud.alloydb.connector.exceptions",
        "aiohttp",
    )
    previously_loaded = {name: name in sys.modules for name in optional_modules}
    assert is_transient_connector_error(TimeoutError("probe")) is True
    assert is_transient_connector_error(ValueError("bad")) is False
    assert {name: name in sys.modules for name in optional_modules} == previously_loaded


@pytest.mark.parametrize(
    ("attempt", "base_delay", "max_delay", "expected_min", "expected_max"),
    [
        pytest.param(0, 0.0, 5.0, 0.0, 0.0, id="zero-base"),
        pytest.param(0, 0.5, 0.0, 0.0, 0.0, id="zero-max"),
        pytest.param(-2, 0.5, 5.0, 0.25, 0.5, id="negative-attempt-clamped"),
        pytest.param(0, 0.5, 5.0, 0.25, 0.5, id="attempt-0"),
        pytest.param(1, 0.5, 5.0, 0.5, 1.0, id="attempt-1"),
        pytest.param(2, 0.5, 5.0, 1.0, 2.0, id="attempt-2"),
        pytest.param(5, 0.5, 5.0, 2.5, 5.0, id="capped-at-max"),
        pytest.param(100, 0.5, 5.0, 2.5, 5.0, id="overflow-attempt-clamped"),
    ],
)
def test_calculate_connector_backoff_seconds_bounds(
    attempt: int,
    base_delay: float,
    max_delay: float,
    expected_min: float,
    expected_max: float,
) -> None:
    """Verify jittered exponential backoff lies within [capped / 2, capped] and returns 0.0 when disabled."""
    delay = calculate_connector_backoff_seconds(attempt, base_delay, max_delay)
    assert math.isfinite(delay)
    assert expected_min <= delay <= expected_max


@pytest.mark.parametrize(
    ("retry_count", "retry_delay", "retry_max_delay", "match"),
    [
        pytest.param(True, 0.5, 5.0, "non-negative integer", id="bool-count-true"),
        pytest.param(False, 0.5, 5.0, "non-negative integer", id="bool-count-false"),
        pytest.param(-1, 0.5, 5.0, "non-negative integer", id="negative-count"),
        pytest.param(1.5, 0.5, 5.0, "non-negative integer", id="float-count"),
        pytest.param("3", 0.5, 5.0, "non-negative integer", id="str-count"),
        pytest.param(3, True, 5.0, "connector_retry_delay", id="bool-delay"),
        pytest.param(3, -0.1, 5.0, "connector_retry_delay", id="negative-delay"),
        pytest.param(3, float("nan"), 5.0, "connector_retry_delay", id="nan-delay"),
        pytest.param(3, float("inf"), 5.0, "connector_retry_delay", id="inf-delay"),
        pytest.param(3, "0.5", 5.0, "connector_retry_delay", id="str-delay"),
        pytest.param(3, 0.5, False, "connector_retry_max_delay", id="bool-max-delay"),
        pytest.param(3, 0.5, -1.0, "connector_retry_max_delay", id="negative-max-delay"),
        pytest.param(3, 0.5, float("nan"), "connector_retry_max_delay", id="nan-max-delay"),
        pytest.param(3, 0.5, float("inf"), "connector_retry_max_delay", id="inf-max-delay"),
        pytest.param(3, 2.0, 1.0, "greater than or equal", id="max-less-than-delay"),
    ],
)
def test_validate_connector_retry_config_rejects_invalid_values(
    retry_count: Any,
    retry_delay: Any,
    retry_max_delay: Any,
    match: str,
) -> None:
    """Verify eager validation rejects bool, negative, non-finite, non-numeric, and inverted retry bounds."""
    with pytest.raises(ImproperConfigurationError, match=match):
        validate_connector_retry_config(retry_count, retry_delay, retry_max_delay)

    with pytest.raises(ImproperConfigurationError, match=match):
        CloudSQLConfig(
            instance_connection_name="my-proj:us-central1:my-pg",
            connector_retry_count=retry_count,
            connector_retry_delay=retry_delay,
            connector_retry_max_delay=retry_max_delay,
        )

    with pytest.raises(ImproperConfigurationError, match=match):
        AlloyDBConfig(
            instance_uri="projects/my-proj/locations/us-central1/clusters/prod/instances/primary",
            connector_retry_count=retry_count,
            connector_retry_delay=retry_delay,
            connector_retry_max_delay=retry_max_delay,
        )

    with pytest.raises(ImproperConfigurationError, match=match):
        PostgreSQLConfig(
            connector_retry_count=retry_count,
            connector_retry_delay=retry_delay,
            connector_retry_max_delay=retry_max_delay,
        )

    with pytest.raises(ImproperConfigurationError, match=match):
        MySQLConfig(
            connector_retry_count=retry_count,
            connector_retry_delay=retry_delay,
            connector_retry_max_delay=retry_max_delay,
        )

    with pytest.raises(ImproperConfigurationError, match=match):
        MSSQLConfig(
            connector_retry_count=retry_count,
            connector_retry_delay=retry_delay,
            connector_retry_max_delay=retry_max_delay,
        )


@pytest.mark.parametrize(
    ("error_factory", "expected"),
    [
        pytest.param(lambda: TimeoutError("timed out"), True, id="timeout"),
        pytest.param(lambda: ConnectionResetError("reset"), True, id="conn-reset"),
        pytest.param(lambda: ConnectionRefusedError("refused"), True, id="conn-refused"),
        pytest.param(lambda: BrokenPipeError("broken pipe"), True, id="broken-pipe"),
        pytest.param(lambda: asyncio.IncompleteReadError(b"", 4), True, id="incomplete-read"),
        pytest.param(lambda: socket.gaierror(socket.EAI_AGAIN, "try again"), True, id="gai-again"),
        pytest.param(lambda: socket.gaierror(socket.EAI_NONAME, "no host"), False, id="gai-noname"),
        pytest.param(lambda: _make_http_error(429), True, id="http-429"),
        pytest.param(lambda: _make_http_error(500), True, id="http-500"),
        pytest.param(lambda: _make_http_error(502), True, id="http-502"),
        pytest.param(lambda: _make_http_error(503), True, id="http-503"),
        pytest.param(lambda: _make_http_error(504), True, id="http-504"),
        pytest.param(lambda: _make_http_error(400), False, id="http-400"),
        pytest.param(lambda: _make_http_error(401), False, id="http-401"),
        pytest.param(lambda: _make_http_error(403), False, id="http-403"),
        pytest.param(lambda: _make_http_error(404), False, id="http-404"),
        pytest.param(lambda: _make_http_error(409), False, id="http-409"),
        pytest.param(lambda: urllib.error.URLError(TimeoutError("connect timeout")), True, id="urlerror-timeout"),
        pytest.param(
            lambda: urllib.error.URLError(ConnectionResetError("reset")),
            True,
            id="urlerror-conn-reset",
        ),
        pytest.param(
            lambda: urllib.error.URLError(ssl.SSLCertVerificationError(1, "cert mismatch")),
            False,
            id="urlerror-ssl-cert",
        ),
        pytest.param(lambda: urllib.error.URLError("unknown host"), False, id="urlerror-str"),
        pytest.param(lambda: ssl.SSLError("tls failure"), False, id="ssl-error"),
        pytest.param(lambda: ssl.SSLCertVerificationError(1, "cert invalid"), False, id="ssl-cert-error"),
        pytest.param(lambda: PermissionError("denied"), False, id="permission-error"),
        pytest.param(lambda: ValueError("bad input"), False, id="value-error"),
        pytest.param(lambda: TypeError("wrong type"), False, id="type-error"),
        pytest.param(lambda: KeyError("missing"), False, id="key-error"),
        pytest.param(lambda: ImproperConfigurationError("bad cfg"), False, id="improper-config"),
        pytest.param(lambda: MissingDependencyError("pkg"), False, id="missing-dep"),
        pytest.param(lambda: GoogleCloudAuthError("invalid token"), False, id="native-gcp-auth"),
        pytest.param(lambda: asyncio.CancelledError(), False, id="cancelled-error"),
        pytest.param(lambda: KeyboardInterrupt(), False, id="keyboard-interrupt"),
        pytest.param(lambda: RuntimeError("unknown"), False, id="plain-runtime-error"),
    ],
)
def test_is_transient_connector_error_builtin_matrix(
    error_factory: Any,
    expected: bool,
) -> None:
    """Classify stdlib, urllib, TLS, native, and BaseException failures accurately."""
    assert is_transient_connector_error(error_factory()) is expected


@pytest.mark.parametrize(
    ("status_name", "expected"),
    [
        ("UNAVAILABLE", True),
        ("ABORTED", True),
        ("DEADLINE_EXCEEDED", True),
        ("RESOURCE_EXHAUSTED", True),
        ("StatusCode.UNAVAILABLE", True),
        ("PERMISSION_DENIED", False),
        ("UNAUTHENTICATED", False),
        ("NOT_FOUND", False),
        ("INVALID_ARGUMENT", False),
        ("CANCELLED", False),
        ("FAILED_PRECONDITION", False),
    ],
)
def test_is_transient_connector_error_grpc_status_codes(status_name: str, expected: bool) -> None:
    """Classify gRPC status names extracted from grpc_status_code and callable code()."""

    class _FakeGrpcEnumError(Exception):
        def __init__(self, name: str) -> None:
            self.grpc_status_code = SimpleNamespace(name=name.rsplit(".", maxsplit=1)[-1])

    class _FakeGrpcMethodError(Exception):
        def code(self) -> str:
            return status_name

    assert is_transient_connector_error(_FakeGrpcEnumError(status_name)) is expected
    assert is_transient_connector_error(_FakeGrpcMethodError()) is expected


def test_is_transient_connector_error_loaded_optional_modules_and_causal_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inspect loaded optional modules via sys.modules and bound cyclic or deep causal chains."""
    fake_api_core: Any = ModuleType("google.api_core.exceptions")
    fake_api_core_retry = type("RetryError", (Exception,), {})
    fake_api_core_unavail = type("ServiceUnavailable", (Exception,), {})
    fake_api_core_denied = type("PermissionDenied", (Exception,), {})
    fake_api_core.RetryError = fake_api_core_retry
    fake_api_core.ServiceUnavailable = fake_api_core_unavail
    fake_api_core.PermissionDenied = fake_api_core_denied
    monkeypatch.setitem(sys.modules, "google.api_core.exceptions", fake_api_core)

    fake_auth: Any = ModuleType("google.auth.exceptions")
    fake_default_creds = type("DefaultCredentialsError", (Exception,), {})
    fake_transport = type("TransportError", (Exception,), {})
    fake_refresh = type("RefreshError", (Exception,), {})
    fake_auth.DefaultCredentialsError = fake_default_creds
    fake_auth.TransportError = fake_transport
    fake_auth.RefreshError = fake_refresh
    monkeypatch.setitem(sys.modules, "google.auth.exceptions", fake_auth)

    fake_csql: Any = ModuleType("google.cloud.sql.connector.exceptions")
    fake_closed = type("ClosedConnectorError", (Exception,), {})
    fake_res_exhausted = type("ResourceExhaustedError", (Exception,), {})
    fake_csql.ClosedConnectorError = fake_closed
    fake_csql.ResourceExhaustedError = fake_res_exhausted
    monkeypatch.setitem(sys.modules, "google.cloud.sql.connector.exceptions", fake_csql)

    fake_adb: Any = ModuleType("google.cloud.alloydb.connector.exceptions")
    fake_ip_missing = type("IPTypeNotFoundError", (Exception,), {})
    fake_adb.IPTypeNotFoundError = fake_ip_missing
    monkeypatch.setitem(sys.modules, "google.cloud.alloydb.connector.exceptions", fake_adb)

    fake_aiohttp: Any = ModuleType("aiohttp")
    fake_client_ssl = type("ClientSSLError", (Exception,), {})
    fake_client_resp = type("ClientResponseError", (Exception,), {})
    fake_server_timeout = type("ServerTimeoutError", (Exception,), {})
    fake_aiohttp.ClientSSLError = fake_client_ssl
    fake_aiohttp.ClientResponseError = fake_client_resp
    fake_aiohttp.ServerTimeoutError = fake_server_timeout
    monkeypatch.setitem(sys.modules, "aiohttp", fake_aiohttp)

    assert is_transient_connector_error(fake_api_core_unavail("503")) is True
    assert is_transient_connector_error(fake_api_core_denied("403")) is False
    assert is_transient_connector_error(fake_api_core_retry("bare retry")) is True

    wrapped_denied: Any = fake_api_core_retry("wrapped denied")
    wrapped_denied.cause = fake_api_core_denied("403")
    assert is_transient_connector_error(wrapped_denied) is False

    assert is_transient_connector_error(fake_default_creds("no adc")) is False
    assert is_transient_connector_error(fake_transport("socket err")) is True
    retryable_refresh: Any = fake_refresh("retryable")
    retryable_refresh.retryable = True
    assert is_transient_connector_error(retryable_refresh) is True
    perm_refresh: Any = fake_refresh("permanent")
    perm_refresh.retryable = False
    assert is_transient_connector_error(perm_refresh) is False

    assert is_transient_connector_error(fake_closed("closed")) is False
    assert is_transient_connector_error(fake_res_exhausted("quota")) is True
    assert is_transient_connector_error(fake_ip_missing("no psc")) is False
    assert is_transient_connector_error(fake_client_ssl("ssl")) is False
    assert is_transient_connector_error(fake_server_timeout("timeout")) is True

    resp_503: Any = fake_client_resp("503")
    resp_503.status = 503
    resp_401: Any = fake_client_resp("401")
    resp_401.status = 401
    assert is_transient_connector_error(resp_503) is True
    assert is_transient_connector_error(resp_401) is False

    cyc_a = RuntimeError("a")
    cyc_b = RuntimeError("b")
    cyc_a.__cause__ = cyc_b
    cyc_b.__cause__ = cyc_a
    assert is_transient_connector_error(cyc_a) is False

    tail: BaseException = TimeoutError("deep transient")
    for idx in range(10):
        outer = RuntimeError(f"wrap-{idx}")
        outer.__cause__ = tail
        tail = outer
    assert is_transient_connector_error(tail) is False


@pytest.mark.parametrize("dialer_kind", ["cloudsql", "alloydb"])
def test_sync_dialer_admin_http_retry_recovery_and_log_redaction(
    dialer_kind: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify sync dialers retry transient Admin API errors with backoff and never log access tokens."""
    sleep_calls: list[float] = []
    monkeypatch.setattr(time, "sleep", sleep_calls.append)
    secret_token = "opaque-bearer-token-redaction-check"
    attempts = 0

    def flaky_http(url: str, payload: bytes, headers: dict[str, str]) -> bytes:
        nonlocal attempts
        _ = (url, payload, headers)
        attempts += 1
        if attempts == 1:
            raise _make_http_error(503)
        if attempts == 2:
            raise urllib.error.URLError(TimeoutError("read timed out"))
        if dialer_kind == "cloudsql":
            return encode_json_bytes({"ephemeralCert": {"cert": "CERT"}, "serverCaCert": {"cert": "CA"}})
        return encode_json_bytes({"caCert": "CA", "clientCert": "CERT"})

    with caplog.at_level(logging.WARNING, logger="vertebra.db.dialer"):
        if dialer_kind == "cloudsql":
            csql_cfg = CloudSQLConfig(
                instance_connection_name="my-proj:us-central1:my-pg",
                connector_retry_count=3,
                connector_retry_delay=0.25,
                connector_retry_max_delay=1.0,
            )
            bundle = SyncCloudSQLDialer(csql_cfg).refresh_certificate(
                public_key_pem="PUB",
                private_key_pem="PRIV",
                access_token=secret_token,
                http_transport=flaky_http,
            )
        else:
            adb_cfg = AlloyDBConfig(
                instance_uri="projects/my-proj/locations/us-central1/clusters/prod/instances/primary",
                connector_retry_count=3,
                connector_retry_delay=0.25,
                connector_retry_max_delay=1.0,
            )
            bundle = SyncAlloyDBDialer(adb_cfg).refresh_client_cert(
                public_key_pem="PUB",
                private_key_pem="PRIV",
                access_token=secret_token,
                http_transport=flaky_http,
            )

    assert bundle.client_cert_pem == "CERT"
    assert attempts == 3
    assert len(sleep_calls) == 2
    assert 0.125 <= sleep_calls[0] <= 0.25
    assert 0.25 <= sleep_calls[1] <= 0.5
    assert len(caplog.records) == 2
    assert secret_token not in caplog.text


@pytest.mark.parametrize(
    ("retry_count", "retry_delay", "exc_factory", "expected_exc", "expected_attempts", "expected_sleeps"),
    [
        pytest.param(
            2, 0.2, lambda: _make_http_error(429), urllib.error.HTTPError, 3, 2, id="exhaust-transient-http-429"
        ),
        pytest.param(0, 0.2, lambda: TimeoutError("t/o"), TimeoutError, 1, 0, id="disabled-retries"),
        pytest.param(2, 0.0, lambda: TimeoutError("t/o"), TimeoutError, 3, 0, id="zero-delay-skips-sleep"),
        pytest.param(3, 0.2, lambda: _make_http_error(403), urllib.error.HTTPError, 1, 0, id="fail-fast-http-403"),
        pytest.param(
            3,
            0.2,
            lambda: urllib.error.URLError(ssl.SSLCertVerificationError(1, "bad cert")),
            urllib.error.URLError,
            1,
            0,
            id="fail-fast-ssl-verify",
        ),
    ],
)
def test_sync_cloudsql_admin_http_retry_exhaustion_and_fail_fast(
    monkeypatch: pytest.MonkeyPatch,
    retry_count: int,
    retry_delay: float,
    exc_factory: Any,
    expected_exc: type[BaseException],
    expected_attempts: int,
    expected_sleeps: int,
) -> None:
    """Verify retry exhaustion, zero-retry disabling, zero-delay skip, and fail-fast on permanent errors."""
    sleep_calls: list[float] = []
    monkeypatch.setattr(time, "sleep", sleep_calls.append)
    calls = 0

    def failing_http(_url: str, _payload: bytes, _headers: dict[str, str]) -> bytes:
        nonlocal calls
        calls += 1
        raise exc_factory()

    cfg = CloudSQLConfig(
        instance_connection_name="my-proj:us-central1:my-pg",
        connector_retry_count=retry_count,
        connector_retry_delay=retry_delay,
        connector_retry_max_delay=max(retry_delay, 1.0),
    )
    with pytest.raises(expected_exc):
        SyncCloudSQLDialer(cfg).refresh_certificate(
            public_key_pem="PUB",
            private_key_pem="PRIV",
            access_token="tok",
            http_transport=failing_http,
        )

    assert calls == expected_attempts
    assert len(sleep_calls) == expected_sleeps


@pytest.mark.anyio
@pytest.mark.parametrize("dialer_kind", ["cloudsql", "alloydb"])
async def test_async_dialer_admin_http_retry_recovery_and_cancellation(
    dialer_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify async dialers retry with anyio.sleep and propagate asyncio.CancelledError without retrying."""
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(anyio, "sleep", fake_sleep)
    attempts = 0

    async def flaky_http(_url: str, _payload: bytes, _headers: dict[str, str]) -> bytes:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _make_http_error(502)
        if dialer_kind == "cloudsql":
            return encode_json_bytes({"ephemeralCert": {"cert": "ASYNC_CERT"}, "serverCaCert": {"cert": "CA"}})
        return encode_json_bytes({"caCert": "CA", "clientCert": "ASYNC_CERT"})

    if dialer_kind == "cloudsql":
        csql_cfg = CloudSQLConfig(
            instance_connection_name="my-proj:us-central1:my-pg",
            connector_retry_count=2,
            connector_retry_delay=0.2,
            connector_retry_max_delay=1.0,
        )
        dialer: Any = AsyncCloudSQLDialer(csql_cfg)
        bundle = await dialer.refresh_certificate(
            public_key_pem="PUB",
            private_key_pem="PRIV",
            access_token="tok",
            http_transport=flaky_http,
        )
    else:
        adb_cfg = AlloyDBConfig(
            instance_uri="projects/my-proj/locations/us-central1/clusters/prod/instances/primary",
            connector_retry_count=2,
            connector_retry_delay=0.2,
            connector_retry_max_delay=1.0,
        )
        dialer = AsyncAlloyDBDialer(adb_cfg)
        bundle = await dialer.refresh_client_cert(
            public_key_pem="PUB",
            private_key_pem="PRIV",
            access_token="tok",
            http_transport=flaky_http,
        )

    assert bundle.client_cert_pem == "ASYNC_CERT"
    assert attempts == 2
    assert len(sleep_calls) == 1
    assert 0.1 <= sleep_calls[0] <= 0.2

    cancel_attempts = 0

    async def cancelling_http(_url: str, _payload: bytes, _headers: dict[str, str]) -> bytes:
        nonlocal cancel_attempts
        cancel_attempts += 1
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        if dialer_kind == "cloudsql":
            await dialer.refresh_certificate(
                public_key_pem="PUB",
                private_key_pem="PRIV",
                access_token="tok",
                http_transport=cancelling_http,
            )
        else:
            await dialer.refresh_client_cert(
                public_key_pem="PUB",
                private_key_pem="PRIV",
                access_token="tok",
                http_transport=cancelling_http,
            )

    assert cancel_attempts == 1


@pytest.mark.anyio
async def test_dialer_connect_retries_and_closes_leaked_sockets(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify sync/async Cloud SQL and AlloyDB connect() retry transient disconnects and close sockets cleanly."""
    monkeypatch.setattr(time, "sleep", lambda _d: None)

    async def fake_async_sleep(_d: float) -> None:
        return None

    monkeypatch.setattr(anyio, "sleep", fake_async_sleep)
    closed_sockets: list[int] = []
    attempt_num = 0

    class _MockSock:
        def __init__(self, idx: int) -> None:
            self.idx = idx

        def close(self) -> None:
            closed_sockets.append(self.idx)

    class _MockTlsCtx:
        def wrap_socket(self, raw_sock: _MockSock, *, server_hostname: str | None = None) -> _MockSock:
            _ = server_hostname
            if raw_sock.idx == 1:
                raise ConnectionResetError("transient reset during tls")
            return raw_sock

    def fake_create_conn(_addr: tuple[str, int], timeout: float = 10.0) -> _MockSock:
        nonlocal attempt_num
        _ = timeout
        attempt_num += 1
        return _MockSock(attempt_num)

    monkeypatch.setattr(socket, "create_connection", fake_create_conn)
    csql_sync = SyncCloudSQLDialer(
        CloudSQLConfig(
            instance_connection_name="my-proj:us-central1:my-pg",
            connector_retry_count=2,
            connector_retry_delay=0.0,
            connector_retry_max_delay=0.0,
        )
    )
    sock = csql_sync.connect("127.0.0.1", ssl_context=cast("Any", _MockTlsCtx()))
    assert getattr(sock, "idx", None) == 2
    assert closed_sockets == [1]

    class _FailSslCtx:
        def wrap_socket(self, _raw_sock: _MockSock, *, server_hostname: str | None = None) -> _MockSock:
            _ = server_hostname
            raise ssl.SSLCertVerificationError(1, "verify failed")

    attempt_num = 0
    closed_sockets.clear()
    with pytest.raises(ssl.SSLCertVerificationError):
        csql_sync.connect("127.0.0.1", ssl_context=cast("Any", _FailSslCtx()))
    assert attempt_num == 1
    assert closed_sockets == [1]

    writer_closes = 0
    open_calls = 0

    class _FakeWriter:
        def write(self, _data: bytes) -> None:
            pass

        async def drain(self) -> None:
            pass

        def close(self) -> None:
            nonlocal writer_closes
            writer_closes += 1

        async def wait_closed(self) -> None:
            pass

    class _FakeReader:
        def __init__(self, fail: bool) -> None:
            self._fail = fail

        async def readexactly(self, n: int) -> bytes:
            if self._fail:
                raise asyncio.IncompleteReadError(b"", 4)
            resp = AsyncAlloyDBDialer.encode_metadata_exchange_response(1)
            return resp[:4] if n == 4 else resp[4:]

    async def fake_open_connection(_host: str, _port: int, *, ssl: Any = None) -> tuple[_FakeReader, _FakeWriter]:
        nonlocal open_calls
        _ = ssl
        open_calls += 1
        return _FakeReader(fail=(open_calls == 1)), _FakeWriter()

    monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)
    adb_async = AsyncAlloyDBDialer(
        AlloyDBConfig(
            instance_uri="projects/my-proj/locations/us-central1/clusters/prod/instances/primary",
            connector_retry_count=2,
            connector_retry_delay=0.0,
            connector_retry_max_delay=0.0,
        )
    )
    _reader, _writer = await adb_async.connect(
        "127.0.0.1",
        oauth2_token="tok",
        ssl_context=ssl.create_default_context(),
    )
    assert open_calls == 2
    assert writer_closes == 1


def test_config_from_url_to_cloud_dialer_and_registry_cache_isolation() -> None:
    """Verify connector_retry_* and ip_type forward from DB configs, URLs, and isolate registry dialer cache."""
    csql_url = CloudSQLConfig.from_url(
        "cloudsql://p:us-central1:inst?connector_retry_count=5&connector_retry_delay=0.2&connector_retry_max_delay=2.5"
    )
    assert csql_url.connector_retry_count == 5
    assert csql_url.connector_retry_delay == 0.2
    assert csql_url.connector_retry_max_delay == 2.5

    adb_url = AlloyDBConfig.from_url(
        "alloydb:///projects/p/locations/l/clusters/c/instances/i"
        "?connector_retry_count=4&connector_retry_delay=0.1&connector_retry_max_delay=1.5"
    )
    assert adb_url.connector_retry_count == 4
    assert adb_url.connector_retry_delay == 0.1
    assert adb_url.connector_retry_max_delay == 1.5

    pg_csql = PostgreSQLConfig.from_url(
        "postgresql://user@localhost/db?cloud_sql_instance=p:r:i"
        "&connector_retry_count=6&connector_retry_delay=0.25&connector_retry_max_delay=3.0",
        cloud_sql_ip_type="PSC",
    )
    pg_dialer = pg_csql.to_cloud_dialer_config()
    assert isinstance(pg_dialer, CloudSQLConfig)
    assert pg_dialer.ip_type == "PSC"
    assert pg_dialer.connector_retry_count == 6
    assert pg_dialer.connector_retry_delay == 0.25
    assert pg_dialer.connector_retry_max_delay == 3.0

    pg_adb = PostgreSQLConfig(
        alloydb_instance_uri="projects/p/locations/l/clusters/c/instances/i",
        alloydb_ip_type="PUBLIC",
        connector_retry_count=2,
        connector_retry_delay=0.1,
        connector_retry_max_delay=0.8,
    )
    adb_dialer = pg_adb.to_cloud_dialer_config()
    assert isinstance(adb_dialer, AlloyDBConfig)
    assert adb_dialer.ip_type == "PUBLIC"
    assert adb_dialer.connector_retry_count == 2
    assert adb_dialer.connector_retry_delay == 0.1
    assert adb_dialer.connector_retry_max_delay == 0.8

    mysql_cfg = MySQLConfig.from_url(
        "mysql://root@localhost:3306/app?cloud_sql_instance=p:r:i"
        "&connector_retry_count=4&connector_retry_delay=0.3&connector_retry_max_delay=2.0",
        cloud_sql_ip_type="PRIVATE",
    )
    mysql_dialer = mysql_cfg.to_cloud_dialer_config()
    assert isinstance(mysql_dialer, CloudSQLConfig)
    assert mysql_dialer.ip_type == "PRIVATE"
    assert mysql_dialer.connector_retry_count == 4
    assert mysql_dialer.connector_retry_delay == 0.3
    assert mysql_dialer.connector_retry_max_delay == 2.0

    mssql_cfg = MSSQLConfig.from_url(
        "mssql://sa@127.0.0.1:1433/master?cloud_sql_instance=p:r:i"
        "&connector_retry_count=1&connector_retry_delay=0.0&connector_retry_max_delay=0.5"
    )
    mssql_dialer = mssql_cfg.to_cloud_dialer_config()
    assert isinstance(mssql_dialer, CloudSQLConfig)
    assert mssql_dialer.connector_retry_count == 1
    assert mssql_dialer.connector_retry_delay == 0.0
    assert mssql_dialer.connector_retry_max_delay == 0.5

    registry = DatabaseRegistry()
    d_default = registry.get_dialer(CloudSQLConfig(instance_connection_name="p:r:i"), sync=True)
    d_same = registry.get_dialer(CloudSQLConfig(instance_connection_name="p:r:i"), sync=True)
    d_zero = registry.get_dialer(
        CloudSQLConfig(instance_connection_name="p:r:i", connector_retry_count=0),
        sync=True,
    )
    assert d_default is d_same
    assert d_default is not d_zero
    assert d_zero.config.connector_retry_count == 0

    with pytest.raises(ImproperConfigurationError, match="non-negative integer"):
        CloudSQLConfig.from_url("cloudsql://p:r:i?connector_retry_count=abc")
    with pytest.raises(ImproperConfigurationError, match="connector_retry_delay"):
        PostgreSQLConfig.from_url("postgresql://localhost/db?connector_retry_delay=nan")
