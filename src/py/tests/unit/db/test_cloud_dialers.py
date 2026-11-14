"""Unit tests for Cloud SQL and AlloyDB zero-sidecar mTLS dialers."""

from __future__ import annotations

import socket
import ssl
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

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
from vertebra.typing import MissingDependencyError, reset_dependency_cache
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
