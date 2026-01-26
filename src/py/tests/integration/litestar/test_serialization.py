"""Tests for Litestar serialization integration."""

import datetime
import uuid
from typing import Any

import pytest

try:
    from litestar import Litestar, get
    from litestar.status_codes import HTTP_200_OK
    from litestar.testing import TestClient

    from vertebra.extensions.litestar.serializers import VERTEBRA_TYPE_ENCODERS

    HAS_LITESTAR = True
except ImportError:
    HAS_LITESTAR = False


@pytest.mark.skipif(not HAS_LITESTAR, reason="Litestar not installed")
def test_litestar_serialization() -> None:
    @get("/", sync_to_thread=False)
    def handler() -> dict[str, Any]:
        return {
            "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
            "dt": datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC),
        }

    app = Litestar(route_handlers=[handler], type_encoders=VERTEBRA_TYPE_ENCODERS)

    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["uuid"] == "12345678-1234-5678-1234-567812345678"
        # Check date string format (Z or +00:00)
        assert "2023-01-01T12:00:00" in data["dt"]


@pytest.mark.skipif(not HAS_LITESTAR, reason="Litestar not installed")
def test_litestar_parsing() -> None:
    import ipaddress
    import pathlib

    from litestar import get

    from vertebra.extensions.litestar.serializers import VERTEBRA_TYPE_DECODERS

    @get("/query", sync_to_thread=False)
    def query_handler(path: pathlib.Path, ip: ipaddress.IPv4Address) -> dict[str, Any]:
        return {"path": str(path), "ip": str(ip)}

    app = Litestar(
        route_handlers=[query_handler], type_encoders=VERTEBRA_TYPE_ENCODERS, type_decoders=VERTEBRA_TYPE_DECODERS
    )

    with TestClient(app) as client:
        # Test query param decoding (uses type_decoders)
        response = client.get("/query", params={"path": "/tmp/test", "ip": "192.168.1.1"})
        assert response.status_code == HTTP_200_OK, response.text
        assert response.json() == {"path": "/tmp/test", "ip": "192.168.1.1"}
