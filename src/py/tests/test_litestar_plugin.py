"""Tests for Vertebra Litestar Plugin."""

from typing import Any

import pytest

try:
    from litestar import Litestar, get
    from litestar.status_codes import HTTP_200_OK
    from litestar.testing import TestClient

    from vertebra.extensions.litestar import VertebraPlugin

    HAS_LITESTAR = True
except ImportError:
    HAS_LITESTAR = False


@pytest.mark.skipif(not HAS_LITESTAR, reason="Litestar not installed")
def test_plugin_registration() -> None:
    # Functional test to ensure encoders are registered via plugin
    import uuid

    @get("/")
    def handler() -> dict[str, Any]:
        return {"id": uuid.uuid4()}

    app = Litestar(route_handlers=[handler], plugins=[VertebraPlugin()])

    with TestClient(app) as client:
        res = client.get("/")
        assert res.status_code == 200
        # If serialization failed, it would be 500 or error
        assert "id" in res.json()
