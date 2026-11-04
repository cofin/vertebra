"""Unit tests for Cloud SQL and AlloyDB zero-sidecar mTLS dialers."""

from __future__ import annotations

import socket
import ssl
import subprocess
import tempfile
import threading
from pathlib import Path

import pytest

from vertebra.db.dialer.alloydb import (
    AlloyDBAuthType,
    AlloyDBClusterURI,
    AlloyDBConfig,
    SyncAlloyDBDialer,
)
from vertebra.db.dialer.cloudsql import (
    CloudSQLConfig,
    CloudSQLInstanceURI,
    EphemeralCertBundle,
    SyncCloudSQLDialer,
)
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
