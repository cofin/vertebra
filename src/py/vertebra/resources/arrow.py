"""Arrow shared memory region pool for providers."""

import io
import uuid
from dataclasses import dataclass
from typing import Any

from vertebra._vertebra import ShmRegion


@dataclass(slots=True)
class ArrowRegion:
    """A managed shared memory region for an Arrow batch."""

    name: str
    size: int
    region: ShmRegion


class ArrowRegionPool:
    """Manages shared memory regions for Arrow data transfer."""

    def __init__(self, prefix: str = "vertebra-arrow") -> None:
        self._prefix = prefix
        self._regions: dict[str, ArrowRegion] = {}

    def create_region(self, size: int) -> ArrowRegion:
        """Create a new shared memory region for Arrow data."""
        name = f"/{self._prefix}.{uuid.uuid4().hex}"
        region = ShmRegion.create_named(name, size)
        managed = ArrowRegion(name=name, size=size, region=region)
        self._regions[name] = managed
        return managed

    def release_region(self, name: str) -> bool:
        """Release a shared memory region."""
        managed = self._regions.pop(name, None)
        if managed:
            managed.region.close()
            return True
        return False

    def close(self) -> None:
        """Close all managed regions."""
        for managed in self._regions.values():
            managed.region.close()
        self._regions.clear()


def should_use_arrow(result: dict[str, Any], threshold_rows: int, threshold_bytes: int) -> bool:
    """Determine if a result should be encoded using Arrow.

    Args:
        result: The query result dictionary.
        threshold_rows: Minimum number of rows to trigger Arrow.
        threshold_bytes: Minimum estimated byte size to trigger Arrow.

    Returns:
        True if Arrow should be used.
    """
    if "arrow" in result or "arrow_bytes" in result:
        return False

    rows = result.get("rows")
    if not isinstance(rows, list) or not rows:
        return False

    if len(rows) >= threshold_rows:
        return True

    # Simple estimation: rows * columns * 8 bytes (roughly)
    columns = result.get("columns", [])
    estimated_size = len(rows) * (len(columns) if columns else 1) * 8
    return estimated_size >= threshold_bytes


def _infer_schema(data: dict[str, list[Any]]) -> Any:
    """Infer nanoarrow schema from dictionary data."""
    import nanoarrow as na

    fields = {}
    for col, values in data.items():
        if not values:
            fields[col] = na.string()
            continue

        val = next((v for v in values if v is not None), None)
        if isinstance(val, bool):
            fields[col] = na.bool_()
        elif isinstance(val, int):
            fields[col] = na.int64()
        elif isinstance(val, float):
            fields[col] = na.float64()
        else:
            fields[col] = na.string()

    return na.struct(fields)


def encode_arrow(result: dict[str, Any]) -> bytes:
    """Encode a result to Arrow IPC using nanoarrow.

    Args:
        result: The result dictionary containing 'columns' and 'rows'.

    Returns:
        Arrow IPC stream bytes.
    """
    import nanoarrow as na
    from nanoarrow.ipc import StreamWriter

    columns = result.get("columns", [])
    rows = result.get("rows", [])

    if not columns or not rows:
        # Return empty stream with minimal schema
        schema = na.c_schema(na.Type.STRUCT)
        out = io.BytesIO()
        with StreamWriter.from_writable(out) as writer:
            pass
        return out.getvalue()

    # Convert rows to dict of columns for nanoarrow
    data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}

    # Infer schema
    schema = _infer_schema(data)

    # Create child arrays
    child_arrays = []
    for i, (_col, values) in enumerate(data.items()):
        child_arrays.append(na.c_array(values, schema.field(i)))

    # Create struct array
    array = na.c_array_from_buffers(schema, length=len(rows), buffers=[None], children=child_arrays)

    out = io.BytesIO()
    with StreamWriter.from_writable(out) as writer:
        writer.write_array(array)

    return out.getvalue()


def decode_arrow(data: bytes | memoryview) -> list[dict[str, Any]]:
    """Decode Arrow IPC stream to list of dicts using nanoarrow.

    Args:
        data: Arrow IPC stream bytes or memoryview.

    Returns:
        List of dictionaries (rows).
    """
    import nanoarrow as na
    import nanoarrow.ipc as na_ipc
    from nanoarrow.iterator import iter_py

    # Create input stream
    input_stream = io.BytesIO(data) if isinstance(data, (bytes, bytearray)) else io.BytesIO(bytes(data))

    rows: list[dict[str, Any]] = []
    try:
        with na_ipc.InputStream.from_readable(input_stream) as ipc_stream, na.c_array_stream(ipc_stream) as stream:
            for array in stream:
                rows.extend(list(iter_py(array)))
    except Exception:  # noqa: BLE001, S110
        pass
    return rows
