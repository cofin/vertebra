"""Unit tests for Cloud SQL and AlloyDB zero-sidecar mTLS dialers."""

from __future__ import annotations

import hashlib
import hmac
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import anyio
import pytest

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
        subprocess.run(cmd, check=True, capture_output=True)
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


@pytest.mark.anyio
async def test_azure_entra_token_provider_missing_scope_and_malformed_json() -> None:
    """Verify missing standalone scope and malformed token JSON fail loudly without leaking secrets."""
    unscoped = AzureEntraTokenProvider(fetcher=lambda scope: ("test-token-placeholder", 5000.0))
    with pytest.raises(ImproperConfigurationError, match="scope"):
        unscoped.resolve_sync()
    with pytest.raises(ImproperConfigurationError, match="scope"):
        await unscoped.resolve_async()

    malformed_bodies: list[tuple[int, bytes]] = [
        (200, b"not-valid-json"),
        (200, encode_json_bytes(["unexpected-list"])),
        (200, encode_json_bytes({"expires_in": 300})),
        (200, encode_json_bytes({"access_token": "test-token-placeholder-secret", "expires_in": "bogus"})),
        (200, encode_json_bytes({"access_token": "test-token-placeholder-secret"})),
        (401, b'{"error":"invalid_client","secret":"test-client-secret-placeholder"}'),
    ]
    idx = [0]

    class _MalformedHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            self.rfile.read(length)
            status, body = malformed_bodies[idx[0]]
            idx[0] += 1
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

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
        for _ in malformed_bodies:
            with pytest.raises(ImproperConfigurationError) as exc_info:
                provider.resolve_sync()
            err_text = str(exc_info.value)
            assert "test-client-secret-placeholder" not in err_text
            assert "test-token-placeholder-secret" not in err_text
        assert idx[0] == len(malformed_bodies)
    finally:
        server.shutdown()
        server.server_close()
        srv_thread.join(timeout=2.0)
