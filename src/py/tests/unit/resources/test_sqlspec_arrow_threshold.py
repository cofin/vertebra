import pytest

import vertebra._vertebra as native
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


@pytest.mark.unit
def test_rust_arrow_roundtrip() -> None:
    rows = [[1, "Alice", True], [2, "Bob", False]]
    columns = ["id", "name", "active"]
    encoded = native.encode_arrow_rows(rows, columns)
    assert isinstance(encoded, (bytes, bytearray))
    decoded = decode_arrow(encoded)
    assert _rows(decoded) == [{"id": 1, "name": "Alice", "active": True}, {"id": 2, "name": "Bob", "active": False}]


@pytest.mark.unit
def test_rust_arrow_promotes_int_to_float() -> None:
    rows = [[1], [2.5]]
    columns = ["value"]
    encoded = native.encode_arrow_rows(rows, columns)
    decoded = decode_arrow(encoded)
    output = _rows(decoded)
    assert output[0]["value"] == 1.0
    assert output[1]["value"] == 2.5


@pytest.mark.unit
def test_rust_arrow_types_hint() -> None:
    rows = [[1, "2"], [3, "4"]]
    columns = ["id", "value"]
    types = ["int64", "string"]
    encoded = native.encode_arrow_rows(rows, columns, types=types)
    decoded = decode_arrow(encoded)
    output = _rows(decoded)
    assert output[0] == {"id": 1, "value": "2"}
    assert output[1] == {"id": 3, "value": "4"}


@pytest.mark.unit
def test_rust_arrow_batching() -> None:
    rows = [[1, "a"], [2, "b"], [3, "c"]]
    columns = ["id", "label"]
    encoded = native.encode_arrow_rows(rows, columns, batch_size=2)
    decoded = decode_arrow(encoded)
    assert _rows(decoded) == [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}, {"id": 3, "label": "c"}]
