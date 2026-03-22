"""Pure Python reader for Vertebra's TransportNode and DataSlab layout.

This module parses the C-ABI memory layout defined in
``vertebra-core/src/transport.rs``. It is designed to be imported in
sub-interpreters that do NOT have access to the ``_vertebra`` native
extension — only ``struct`` and standard library are needed.

The layout is verified by Rust-side ``static_assert`` tests in
``transport.rs`` to prevent drift between Rust and Python.
"""

import struct
from typing import NamedTuple

# ---------------------------------------------------------------------------
# TransportNode layout (repr(C), matches transport.rs:39-65)
# ---------------------------------------------------------------------------
#
# Offset  Size  Field
# ------  ----  -----
#   0       1   method_tag: u8
#   1       1   http_version_tag: u8
#   2       2   _pad: [u8; 2]
#   4       4   (implicit alignment padding for u64)
#   8       8   handler_id: u64
#  16       8   correlation_id: u64
#  24       4   path_offset: u32
#  28       4   path_len: u32
#  32       4   query_offset: u32
#  36       4   query_len: u32
#  40       4   headers_offset: u32
#  44       4   headers_len: u32
#  48       4   body_offset: u32
#  52       4   body_len: u32
# ------
#  56 bytes total

_NODE_FMT = "<BB2x4xQQIIIIIIII"
_NODE_SIZE = struct.calcsize(_NODE_FMT)
assert _NODE_SIZE == 56, f"TransportNode size mismatch: expected 56, got {_NODE_SIZE}"

_METHOD_NAMES: dict[int, str] = {
    0: "GET",
    1: "POST",
    2: "PUT",
    3: "DELETE",
    4: "PATCH",
    5: "HEAD",
    6: "OPTIONS",
    7: "TRACE",
    8: "CONNECT",
}

_HTTP_VERSIONS: dict[int, str] = {
    0: "1.0",
    1: "1.1",
    2: "2",
    3: "3",
}


class RawRequest(NamedTuple):
    """Parsed request from shared memory."""

    method: str
    http_version: str
    handler_id: int
    correlation_id: int
    path: str
    query: str | None
    headers_raw: bytes
    body: bytes


def parse_node(node_buf: bytes | memoryview, slab_buf: bytes | memoryview) -> RawRequest:
    """Parse a single TransportNode from a buffer pair.

    Args:
        node_buf: At least ``NODE_SIZE`` bytes starting at the node.
        slab_buf: The full DataSlab content (variable-length data).

    Returns:
        RawRequest with all fields materialised from the buffers.
    """
    (
        method_tag,
        http_version_tag,
        handler_id,
        correlation_id,
        path_off,
        path_len,
        query_off,
        query_len,
        headers_off,
        headers_len,
        body_off,
        body_len,
    ) = struct.unpack_from(_NODE_FMT, node_buf)

    path = bytes(slab_buf[path_off : path_off + path_len]).decode("utf-8")
    query = bytes(slab_buf[query_off : query_off + query_len]).decode("utf-8") if query_len else None
    headers_raw = bytes(slab_buf[headers_off : headers_off + headers_len])
    body = bytes(slab_buf[body_off : body_off + body_len])

    return RawRequest(
        method=_METHOD_NAMES.get(method_tag, "OTHER"),
        http_version=_HTTP_VERSIONS.get(http_version_tag, "1.1"),
        handler_id=handler_id,
        correlation_id=correlation_id,
        path=path,
        query=query,
        headers_raw=headers_raw,
        body=body,
    )


def parse_batch(
    node_buf: bytes | memoryview,
    node_count: int,
    slab_buf: bytes | memoryview,
) -> list[RawRequest]:
    """Parse a full batch of TransportNodes.

    Args:
        node_buf: Contiguous buffer of ``node_count * NODE_SIZE`` bytes.
        node_count: Number of nodes in the batch.
        slab_buf: The full DataSlab content.

    Returns:
        List of parsed requests in batch order.
    """
    requests = []
    for i in range(node_count):
        off = i * _NODE_SIZE
        requests.append(parse_node(node_buf[off : off + _NODE_SIZE], slab_buf))
    return requests


def decode_headers(raw: bytes) -> list[tuple[str, str]]:
    """Decode Vertebra's compact header format.

    Format per header: ``[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]``

    Matches ``vertebra-core/src/transport.rs::decode_headers_raw``.
    """
    headers: list[tuple[str, str]] = []
    pos = 0
    while pos < len(raw):
        name_len = raw[pos]
        pos += 1
        if pos + name_len > len(raw):
            break
        name = raw[pos : pos + name_len].decode("latin-1")
        pos += name_len

        if pos + 2 > len(raw):
            break
        value_len = int.from_bytes(raw[pos : pos + 2], "little")
        pos += 2
        if pos + value_len > len(raw):
            break
        value = raw[pos : pos + value_len].decode("latin-1")
        pos += value_len

        headers.append((name, value))
    return headers


def parse_batch_from_slot(slot_data: "bytes | memoryview") -> list[RawRequest]:
    """Parse a BatchSnapshot from the ring slot wire format.

    Wire format (defined in vertebra-core/src/transport/mod.rs):
        [batch_flags: u32]          — protocol flags
        [node_count:  u32]          — number of TransportNodes
        [slab_len:    u32]          — length of DataSlab bytes
        [_pad:        u32]          — alignment padding
        [nodes: TransportNode × N]  — fixed-size, offset-based
        [slab_data: u8 × slab_len] — variable-length request data
    """
    if len(slot_data) < 16:
        return []

    _batch_flags, node_count, slab_len, _pad = struct.unpack_from("<IIII", slot_data)
    header_size = 16
    nodes_size = node_count * _NODE_SIZE

    if len(slot_data) < header_size + nodes_size + slab_len:
        return []

    node_buf = slot_data[header_size : header_size + nodes_size]
    slab_buf = slot_data[header_size + nodes_size : header_size + nodes_size + slab_len]
    return parse_batch(node_buf, node_count, slab_buf)


# Expose NODE_SIZE for external consumers
NODE_SIZE = _NODE_SIZE

__all__ = (
    "NODE_SIZE",
    "RawRequest",
    "decode_headers",
    "parse_batch",
    "parse_batch_from_slot",
    "parse_node",
)
