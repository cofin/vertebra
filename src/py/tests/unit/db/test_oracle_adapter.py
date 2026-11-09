"""Unit tests for the native Oracle database adapter, dialer, worker bridge, codecs, pools, and tenancy."""

from __future__ import annotations

import io
import zipfile
from typing import TYPE_CHECKING, Any

import pytest

from vertebra.db.adapters.oracle.config import (
    DEFAULT_ORACLE_DSN,
    OracleConfig,
    OracleConnectDescriptor,
    OracleDrcpConfig,
    OracleTopologyConfig,
    OracleWallet,
)

if TYPE_CHECKING:
    from pathlib import Path


def _build_wallet_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "tnsnames.ora",
            (
                "mydb_high = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(PORT=1522)"
                "(HOST=adb.us-ashburn-1.oraclecloud.com))"
                "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=g1_mydb_high.adb.oraclecloud.com))"
                '(SECURITY=(SSL_SERVER_CERT_DN="CN=adb.us-ashburn-1.oraclecloud.com")))\n'
            ),
        )
        zf.writestr("sqlnet.ora", "WALLET_LOCATION = (SOURCE = (METHOD = file))")
        zf.writestr(
            "ewallet.pem",
            b"-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n",
        )
    return buf.getvalue()


@pytest.mark.parametrize(
    ("kwargs", "expected_host", "expected_port", "expected_service", "expected_sid", "expected_protocol"),
    [
        (
            {},
            "localhost",
            1521,
            "FREEPDB1",
            None,
            "TCP",
        ),
        (
            {"host": "db.internal", "port": 1530, "service_name": "ORCLPDB1"},
            "db.internal",
            1530,
            "ORCLPDB1",
            None,
            "TCP",
        ),
        (
            {"host": "legacy.internal", "port": 1521, "sid": "ORCLSID"},
            "legacy.internal",
            1521,
            None,
            "ORCLSID",
            "TCP",
        ),
        (
            {"dsn": "oracle://rac.internal:1525/PROD"},
            "rac.internal",
            1525,
            "PROD",
            None,
            "TCP",
        ),
        (
            {
                "connect_descriptor_str": (
                    "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=explicit.internal)(PORT=1522))"
                    "(CONNECT_DATA=(SERVICE_NAME=EXPLICIT_SVC)))"
                ),
                "dsn": "oracle://rac.internal:1525/PROD",
                "host": "db.internal",
            },
            "explicit.internal",
            1522,
            "EXPLICIT_SVC",
            None,
            "TCPS",
        ),
    ],
)
def test_oracle_config_connect_descriptor_precedence(
    kwargs: dict[str, Any],
    expected_host: str,
    expected_port: int,
    expected_service: str | None,
    expected_sid: str | None,
    expected_protocol: str,
) -> None:
    """Verify 4-tier priority order in `OracleConfig.connect_descriptor()`."""
    cfg = OracleConfig(**kwargs)
    assert DEFAULT_ORACLE_DSN == "oracle://localhost:1521/FREEPDB1"
    desc = cfg.connect_descriptor()
    assert isinstance(desc, OracleConnectDescriptor)
    assert desc.host == expected_host
    assert desc.port == expected_port
    assert desc.service_name == expected_service
    assert desc.sid == expected_sid
    assert desc.protocol == expected_protocol


def test_oracle_config_topology_and_wallet_resolution(tmp_path: Path) -> None:
    """Verify `OracleTopologyConfig.to_tns_descriptor()` and `OracleWallet` alias resolution."""
    topo_cfg = OracleConfig(
        topology=OracleTopologyConfig(
            hosts=("rac1.internal", "rac2.internal"),
            port=1522,
            service_name="RAC_SVC",
            protocol="TCPS",
        ),
        host="ignored.internal",
    )
    topo_desc = topo_cfg.connect_descriptor()
    assert topo_desc.host == "rac1.internal"
    assert topo_desc.port == 1522
    assert topo_desc.service_name == "RAC_SVC"
    assert topo_desc.protocol == "TCPS"

    wallet_bytes = _build_wallet_zip_bytes()
    wallet_cfg = OracleConfig(wallet_zip=wallet_bytes, tns_alias="mydb_high")
    wallet_desc = wallet_cfg.connect_descriptor()
    assert wallet_desc.host == "adb.us-ashburn-1.oraclecloud.com"
    assert wallet_desc.port == 1522
    assert wallet_desc.service_name == "g1_mydb_high.adb.oraclecloud.com"
    assert wallet_desc.protocol == "TCPS"

    zip_file = tmp_path / "Wallet_MYDB.zip"
    zip_file.write_bytes(wallet_bytes)
    path_cfg = OracleConfig(
        wallet_path=str(zip_file),
        tns_alias="mydb_high",
        drcp=OracleDrcpConfig(cclass="APP_POOL", purity="SELF", connection_tag="ROLE=ADMIN"),
        auth_mode="token",
        access_token="ya29-sample",
    )
    path_desc = path_cfg.connect_descriptor()
    assert path_desc.host == "adb.us-ashburn-1.oraclecloud.com"
    assert path_cfg.drcp.cclass == "APP_POOL"
    assert path_cfg.drcp.purity == "SELF"
    assert path_cfg.drcp.connection_tag == "ROLE=ADMIN"
    assert path_cfg.auth_mode == "token"
    assert path_cfg.access_token == "ya29-sample"
    wallet = OracleWallet.from_path(zip_file)
    assert "MYDB_HIGH" in wallet.aliases
