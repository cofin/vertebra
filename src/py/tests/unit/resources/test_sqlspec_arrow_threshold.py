import pytest

from vertebra.resources.arrow import decode_arrow, encode_arrow, should_use_arrow


def _rows(decoded: object) -> list[dict[str, object]]:
    if hasattr(decoded, "to_pylist"):
        return decoded.to_pylist()
    return decoded  # type: ignore[return-value]


@pytest.mark.unit
def test_should_use_arrow() -> None:
    # Small result
    result = {"columns": ["id", "name"], "rows": [[1, "Alice"], [2, "Bob"]]}
    assert should_use_arrow(result, threshold_rows=10, threshold_bytes=1000) is False

    # Large row count
    assert should_use_arrow(result, threshold_rows=2, threshold_bytes=1000) is True

    # Large byte size (estimated)
    # 2 rows * 2 columns * 8 bytes = 32 bytes
    assert should_use_arrow(result, threshold_rows=10, threshold_bytes=30) is True
    assert should_use_arrow(result, threshold_rows=10, threshold_bytes=40) is False


@pytest.mark.unit
def test_arrow_roundtrip() -> None:
    data = {"columns": ["id", "name", "active"], "rows": [[1, "Alice", True], [2, "Bob", False], [3, "Charlie", True]]}

    encoded = encode_arrow(data)
    assert isinstance(encoded, bytes)
    assert len(encoded) > 0

    decoded = decode_arrow(encoded)
    rows = _rows(decoded)
    assert len(rows) == 3
    assert rows[0] == {"id": 1, "name": "Alice", "active": True}
    assert rows[1] == {"id": 2, "name": "Bob", "active": False}
    assert rows[2] == {"id": 3, "name": "Charlie", "active": True}


@pytest.mark.unit
def test_arrow_empty_result() -> None:
    data = {"columns": ["id"], "rows": []}
    encoded = encode_arrow(data)
    decoded = decode_arrow(encoded)
    rows = _rows(decoded)
    assert rows == []
