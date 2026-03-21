"""Tests for vertebra.server._ipc._ring_reader — pure Python TransportNode parser."""

import struct

from vertebra.server._ipc._ring_reader import (
    NODE_SIZE,
    decode_headers,
    parse_batch,
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


class TestNodeSize:
    def test_node_size_is_56(self):
        assert NODE_SIZE == 56

    def test_struct_format_matches(self):
        assert struct.calcsize(_NODE_FMT) == 56


class TestParseNode:
    def test_basic_request(self):
        node, slab = _make_node_and_slab(path=b"/hello", handler_id=42, correlation_id=7)
        req = parse_node(node, slab)
        assert req.method == "GET"
        assert req.http_version == "1.1"
        assert req.handler_id == 42
        assert req.correlation_id == 7
        assert req.path == "/hello"
        assert req.query is None
        assert req.body == b""

    def test_with_query(self):
        node, slab = _make_node_and_slab(path=b"/search", query=b"q=test&page=1")
        req = parse_node(node, slab)
        assert req.path == "/search"
        assert req.query == "q=test&page=1"

    def test_with_body(self):
        node, slab = _make_node_and_slab(method_tag=1, path=b"/api", body=b'{"key":"value"}')
        req = parse_node(node, slab)
        assert req.method == "POST"
        assert req.body == b'{"key":"value"}'

    def test_all_methods(self):
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

    def test_http_versions(self):
        for tag, version in [(0, "1.0"), (1, "1.1"), (2, "2"), (3, "3")]:
            node, slab = _make_node_and_slab(http_version_tag=tag, path=b"/")
            req = parse_node(node, slab)
            assert req.http_version == version

    def test_memoryview_input(self):
        node, slab = _make_node_and_slab(path=b"/memview")
        req = parse_node(memoryview(node), memoryview(slab))
        assert req.path == "/memview"


class TestParseBatch:
    def test_empty_batch(self):
        assert parse_batch(b"", 0, b"") == []

    def test_single_node(self):
        node, slab = _make_node_and_slab(path=b"/one")
        batch = parse_batch(node, 1, slab)
        assert len(batch) == 1
        assert batch[0].path == "/one"

    def test_multiple_nodes(self):
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


class TestDecodeHeaders:
    def test_empty(self):
        assert decode_headers(b"") == []

    def test_single_header(self):
        buf = bytearray()
        buf.append(4)  # name_len
        buf.extend(b"host")
        buf.extend((9).to_bytes(2, "little"))  # value_len
        buf.extend(b"localhost")
        assert decode_headers(bytes(buf)) == [("host", "localhost")]

    def test_multiple_headers(self):
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

    def test_truncated_is_safe(self):
        buf = bytes([10, ord("a"), ord("b")])  # name_len=10 but only 2 bytes
        assert decode_headers(buf) == []
