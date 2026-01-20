from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from vertebra.procedures.registry import ProcedureMetadata, ProcedureRegistry
from vertebra.types.bridge import TypeBridge
from vertebra.types.ir import PrimitiveType

pytestmark = pytest.mark.unit


def _register_dummy() -> ProcedureRegistry:
    registry = ProcedureRegistry.instance()
    registry.clear()
    meta = ProcedureMetadata(
        procedure_id="ping",
        handler_fn=lambda value: value,
        source_file=Path(__file__),
        params={"value": PrimitiveType("number")},
        return_type=PrimitiveType("number"),
        serializer="msgspec",
        metadata={},
    )
    registry.register(meta)
    return registry


def test_type_bridge_etag_and_cache() -> None:
    registry = _register_dummy()
    bridge = TypeBridge(registry)
    status, headers, body = bridge.build_response()
    assert status == 200
    assert body
    assert "ETag" in headers

    status2, headers2, body2 = bridge.build_response(if_none_match=headers["ETag"])
    assert status2 == 304
    assert body2 == b""
    assert headers2["ETag"] == headers["ETag"]


def test_type_bridge_gzip() -> None:
    registry = _register_dummy()
    bridge = TypeBridge(registry, compress_threshold=1)
    status, headers, body = bridge.build_response(accept_encoding="gzip")
    assert status == 200
    assert headers.get("Content-Encoding") == "gzip"
    decompressed = gzip.decompress(body)
    assert decompressed
