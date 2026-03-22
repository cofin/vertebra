"""Tests for vertebra.server._ipc._ring_reader — pure Python TransportNode parser."""

import struct

from vertebra.server._ipc._ring_reader import (
    NODE_SIZE,
    decode_headers,
    parse_batch,
    parse_batch_from_slot,
    parse_node,
)

# The struct format must match Rust's repr(C) TransportNode exactly.
_NODE_FMT = "<BB2x4xQQIIIIIIII"


def _make_node_and_slab(
    *,
    method_tag: int = 0,
    http_version_tag: int = 1,
    handler_id: int = 42,
    correlation_id: int = 1,
    path: bytes = b"/hello",
    query: bytes = b"",
    headers_raw: bytes = b"",
    body: bytes = b"",
) -> tuple[bytes, bytes]:
    """Build a (node_bytes, slab_bytes) pair for testing."""
    slab = bytearray()

    path_off = len(slab)
    slab.extend(path)
    path_len = len(path)

    query_off = len(slab)
    slab.extend(query)
    query_len = len(query)

    hdr_off = len(slab)
    slab.extend(headers_raw)
    hdr_len = len(headers_raw)

    body_off = len(slab)
    slab.extend(body)
    body_len = len(body)

    node = struct.pack(
        _NODE_FMT,
        method_tag,
        http_version_tag,
        handler_id,
        correlation_id,
        path_off,
        path_len,
        query_off,
        query_len,
        hdr_off,
        hdr_len,
        body_off,
        body_len,
    )
    return node, bytes(slab)


# --- TestNodeSize ---


def test_node_size_is_56():
    assert NODE_SIZE == 56


def test_node_size_struct_format_matches():
    assert struct.calcsize(_NODE_FMT) == 56


# --- TestParseNode ---


def test_parse_node_basic_request():
    node, slab = _make_node_and_slab(path=b"/hello", handler_id=42, correlation_id=7)
    req = parse_node(node, slab)
    assert req.method == "GET"
    assert req.http_version == "1.1"
    assert req.handler_id == 42
    assert req.correlation_id == 7
    assert req.path == "/hello"
    assert req.query is None
    assert req.body == b""


def test_parse_node_with_query():
    node, slab = _make_node_and_slab(path=b"/search", query=b"q=test&page=1")
    req = parse_node(node, slab)
    assert req.path == "/search"
    assert req.query == "q=test&page=1"


def test_parse_node_with_body():
    node, slab = _make_node_and_slab(method_tag=1, path=b"/api", body=b'{"key":"value"}')
    req = parse_node(node, slab)
    assert req.method == "POST"
    assert req.body == b'{"key":"value"}'


def test_parse_node_all_methods():
    for tag, name in [
        (0, "GET"),
        (1, "POST"),
        (2, "PUT"),
        (3, "DELETE"),
        (4, "PATCH"),
        (5, "HEAD"),
        (6, "OPTIONS"),
        (7, "TRACE"),
        (8, "CONNECT"),
        (255, "OTHER"),
    ]:
        node, slab = _make_node_and_slab(method_tag=tag, path=b"/")
        req = parse_node(node, slab)
        assert req.method == name, f"tag {tag} → {req.method}, expected {name}"


def test_parse_node_http_versions():
    for tag, version in [(0, "1.0"), (1, "1.1"), (2, "2"), (3, "3")]:
        node, slab = _make_node_and_slab(http_version_tag=tag, path=b"/")
        req = parse_node(node, slab)
        assert req.http_version == version


def test_parse_node_memoryview_input():
    node, slab = _make_node_and_slab(path=b"/memview")
    req = parse_node(memoryview(node), memoryview(slab))
    assert req.path == "/memview"


# --- TestParseBatch ---


def test_parse_batch_empty_batch():
    assert parse_batch(b"", 0, b"") == []


def test_parse_batch_single_node():
    node, slab = _make_node_and_slab(path=b"/one")
    batch = parse_batch(node, 1, slab)
    assert len(batch) == 1
    assert batch[0].path == "/one"


def test_parse_batch_multiple_nodes():
    slab = bytearray()
    nodes = bytearray()

    for i in range(3):
        path = f"/path{i}".encode()
        path_off = len(slab)
        slab.extend(path)
        node = struct.pack(
            _NODE_FMT,
            0,  # GET
            1,  # HTTP/1.1
            i + 100,  # handler_id
            i,  # correlation_id
            path_off,
            len(path),
            0,
            0,  # no query
            0,
            0,  # no headers
            0,
            0,  # no body
        )
        nodes.extend(node)

    batch = parse_batch(bytes(nodes), 3, bytes(slab))
    assert len(batch) == 3
    assert [r.path for r in batch] == ["/path0", "/path1", "/path2"]
    assert [r.handler_id for r in batch] == [100, 101, 102]


# --- TestDecodeHeaders ---


def test_decode_headers_empty():
    assert decode_headers(b"") == []


def test_decode_headers_single_header():
    buf = bytearray()
    buf.append(4)  # name_len
    buf.extend(b"host")
    buf.extend((9).to_bytes(2, "little"))  # value_len
    buf.extend(b"localhost")
    assert decode_headers(bytes(buf)) == [("host", "localhost")]


def test_decode_headers_multiple_headers():
    buf = bytearray()
    for name, value in [("host", "example.com"), ("content-type", "text/plain")]:
        buf.append(len(name))
        buf.extend(name.encode())
        buf.extend(len(value).to_bytes(2, "little"))
        buf.extend(value.encode())
    headers = decode_headers(bytes(buf))
    assert len(headers) == 2
    assert headers[0] == ("host", "example.com")
    assert headers[1] == ("content-type", "text/plain")


def test_decode_headers_truncated_is_safe():
    buf = bytes([10, ord("a"), ord("b")])  # name_len=10 but only 2 bytes
    assert decode_headers(buf) == []


# --- TestParseBatchFromSlot ---


def test_parse_batch_from_slot_empty_slot_too_short():
    assert parse_batch_from_slot(b"") == []
    assert parse_batch_from_slot(b"\x00" * 10) == []


def test_parse_batch_from_slot_single_request_round_trip():
    """Build a slot matching the Rust wire format and parse it."""
    # Wire format: [flags:u32][node_count:u32][slab_len:u32][pad:u32][nodes][slab]
    path = b"/api/users"
    slab = bytearray()
    path_off = len(slab)
    slab.extend(path)

    node = struct.pack(
        _NODE_FMT,
        0,  # GET
        1,  # HTTP/1.1
        42,  # handler_id
        100,  # correlation_id
        path_off,
        len(path),
        0, 0,  # no query
        0, 0,  # no headers
        0, 0,  # no body
    )

    # Build slot
    header = struct.pack("<IIII", 0, 1, len(slab), 0)  # flags=0, 1 node
    slot = header + node + bytes(slab)

    requests = parse_batch_from_slot(slot)
    assert len(requests) == 1
    assert requests[0].path == "/api/users"
    assert requests[0].handler_id == 42
    assert requests[0].correlation_id == 100
    assert requests[0].method == "GET"


def test_parse_batch_from_slot_multi_request_batch():
    """Multiple requests in a single slot."""
    slab = bytearray()
    nodes = bytearray()

    for i in range(5):
        path = f"/item/{i}".encode()
        path_off = len(slab)
        slab.extend(path)
        nodes.extend(struct.pack(
            _NODE_FMT,
            1,  # POST
            1,  # HTTP/1.1
            i + 10,
            i + 200,
            path_off, len(path),
            0, 0, 0, 0, 0, 0,
        ))

    header = struct.pack("<IIII", 0, 5, len(slab), 0)
    slot = header + bytes(nodes) + bytes(slab)

    requests = parse_batch_from_slot(slot)
    assert len(requests) == 5
    assert requests[2].path == "/item/2"
    assert requests[2].handler_id == 12
    assert all(r.method == "POST" for r in requests)


def test_parse_batch_from_slot_with_body_and_headers():
    """Slot with headers and body data."""
    path = b"/upload"
    headers_raw = b"\x0ccontent-type\x10\x00application/json"
    body = b'{"data":"test"}'

    slab = bytearray()
    path_off = len(slab)
    slab.extend(path)
    hdr_off = len(slab)
    slab.extend(headers_raw)
    body_off = len(slab)
    slab.extend(body)

    node = struct.pack(
        _NODE_FMT,
        1, 1, 99, 500,
        path_off, len(path),
        0, 0,
        hdr_off, len(headers_raw),
        body_off, len(body),
    )

    header = struct.pack("<IIII", 0, 1, len(slab), 0)
    slot = header + node + bytes(slab)

    requests = parse_batch_from_slot(slot)
    assert len(requests) == 1
    assert requests[0].path == "/upload"
    assert requests[0].body == body
    assert requests[0].headers_raw == headers_raw


def test_parse_batch_from_slot_truncated_slot_is_safe():
    """Slot claims more data than available — should return empty."""
    # Header claims 100 nodes but slot is too short
    header = struct.pack("<IIII", 0, 100, 1000, 0)
    assert parse_batch_from_slot(header) == []


def test_parse_batch_from_slot_memoryview_input():
    """Should accept memoryview for zero-copy from ring slots."""
    path = b"/mv"
    slab = bytearray(path)
    node = struct.pack(_NODE_FMT, 0, 1, 1, 1, 0, len(path), 0, 0, 0, 0, 0, 0)
    header = struct.pack("<IIII", 0, 1, len(slab), 0)
    slot = header + node + bytes(slab)

    requests = parse_batch_from_slot(memoryview(slot))
    assert len(requests) == 1
    assert requests[0].path == "/mv"
