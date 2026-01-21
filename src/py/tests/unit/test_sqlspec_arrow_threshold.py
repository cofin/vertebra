import pytest
from unittest.mock import MagicMock
from vertebra.resources.arrow import should_use_arrow, encode_arrow_nano, decode_arrow_nano

@pytest.mark.unit
def test_should_use_arrow():
    # Small result
    result = {
        "columns": ["id", "name"],
        "rows": [[1, "Alice"], [2, "Bob"]]
    }
    assert should_use_arrow(result, threshold_rows=10, threshold_bytes=1000) is False
    
    # Large row count
    assert should_use_arrow(result, threshold_rows=2, threshold_bytes=1000) is True
    
    # Large byte size (estimated)
    # 2 rows * 2 columns * 8 bytes = 32 bytes
    assert should_use_arrow(result, threshold_rows=10, threshold_bytes=30) is True
    assert should_use_arrow(result, threshold_rows=10, threshold_bytes=40) is False

@pytest.mark.unit
def test_arrow_roundtrip_nano():
    data = {
        "columns": ["id", "name", "active"],
        "rows": [
            [1, "Alice", True],
            [2, "Bob", False],
            [3, "Charlie", True]
        ]
    }
    
    encoded = encode_arrow_nano(data)
    assert isinstance(encoded, bytes)
    assert len(encoded) > 0
    
    decoded = decode_arrow_nano(encoded)
    assert len(decoded) == 3
    assert decoded[0] == {"id": 1, "name": "Alice", "active": True}
    assert decoded[1] == {"id": 2, "name": "Bob", "active": False}
    assert decoded[2] == {"id": 3, "name": "Charlie", "active": True}

@pytest.mark.unit
def test_arrow_empty_result():
    data = {
        "columns": ["id"],
        "rows": []
    }
    encoded = encode_arrow_nano(data)
    decoded = decode_arrow_nano(encoded)
    assert decoded == []
