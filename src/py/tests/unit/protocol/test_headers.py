"""Tests for protocol headers."""

import pytest

from vertebra.messaging.protocol import (
    HEADER_SIZE,
    ControlMessageKind,
    Flags,
    RequestHeader,
    ResponseFlags,
    ResponseHeader,
)


class TestRequestHeader:
    """Tests for RequestHeader."""

    def test_size(self) -> None:
        """Header should be exactly 16 bytes."""
        assert HEADER_SIZE == 16

    def test_roundtrip(self) -> None:
        """Header should serialize and deserialize correctly."""
        header = RequestHeader(id=42, flags=Flags.PRIORITY | Flags.COMPRESSED, payload_len=1024)
        data = header.to_bytes()
        assert len(data) == 16

        restored = RequestHeader.from_bytes(data)
        assert restored.id == 42
        assert restored.flags == Flags.PRIORITY | Flags.COMPRESSED
        assert restored.payload_len == 1024

    def test_from_memoryview(self) -> None:
        """Header should parse from memoryview (zero-copy)."""
        header = RequestHeader(id=123, flags=0, payload_len=512)
        data = bytearray(header.to_bytes())
        view = memoryview(data)

        restored = RequestHeader.from_bytes(view)
        assert restored.id == 123
        assert restored.payload_len == 512

    def test_write_to(self) -> None:
        """Header should write to buffer at offset."""
        header = RequestHeader(id=99, flags=Flags.BATCH, payload_len=256)
        buffer = bytearray(32)  # Extra space

        header.write_to(buffer, offset=8)

        # First 8 bytes should be unchanged
        assert buffer[:8] == b"\x00" * 8
        # Header should be at offset 8
        restored = RequestHeader.from_bytes(buffer[8:])
        assert restored.id == 99
        assert restored.flags == Flags.BATCH
        assert restored.payload_len == 256

    def test_too_short_raises(self) -> None:
        """Parsing too-short data should raise ValueError."""
        with pytest.raises(ValueError, match="requires 16 bytes"):
            RequestHeader.from_bytes(b"\x00" * 15)


class TestResponseHeader:
    """Tests for ResponseHeader."""

    def test_size(self) -> None:
        """Header should be exactly 16 bytes."""
        assert HEADER_SIZE == 16

    def test_roundtrip(self) -> None:
        """Header should serialize and deserialize correctly."""
        header = ResponseHeader(id=99, flags=ResponseFlags.OK, payload_len=512)
        data = header.to_bytes()
        restored = ResponseHeader.from_bytes(data)

        assert restored.id == 99
        assert restored.flags == ResponseFlags.OK
        assert restored.payload_len == 512

    def test_is_ok(self) -> None:
        """is_ok should check OK flag."""
        ok_header = ResponseHeader(id=1, flags=ResponseFlags.OK, payload_len=0)
        err_header = ResponseHeader(id=2, flags=ResponseFlags.ERROR, payload_len=0)
        none_header = ResponseHeader(id=3, flags=ResponseFlags.NONE, payload_len=0)

        assert ok_header.is_ok is True
        assert ok_header.is_error is False

        assert err_header.is_ok is False
        assert err_header.is_error is True

        assert none_header.is_ok is False
        assert none_header.is_error is False


class TestFlags:
    """Tests for flag constants."""

    def test_flag_values(self) -> None:
        """Flags should have correct bit values."""
        assert Flags.NONE == 0
        assert Flags.PRIORITY == 1
        assert Flags.COMPRESSED == 2
        assert Flags.FIRE_AND_FORGET == 4
        assert Flags.BATCH == 8
        assert Flags.OK == 16
        assert Flags.ERROR == 32

    def test_flag_combination(self) -> None:
        """Flags should combine with bitwise OR."""
        combined = Flags.PRIORITY | Flags.COMPRESSED
        assert combined & Flags.PRIORITY
        assert combined & Flags.COMPRESSED
        assert not (combined & Flags.BATCH)


class TestControlMessageKind:
    """Tests for control message kinds."""

    def test_valid_kinds(self) -> None:
        """Valid kinds should be recognized."""
        assert ControlMessageKind.is_valid("connect")
        assert ControlMessageKind.is_valid("disconnect")
        assert ControlMessageKind.is_valid("heartbeat")
        assert ControlMessageKind.is_valid("shutdown")

    def test_invalid_kinds(self) -> None:
        """Invalid kinds should be rejected."""
        assert not ControlMessageKind.is_valid("invalid")
        assert not ControlMessageKind.is_valid("")
        assert not ControlMessageKind.is_valid("CONNECT")  # Case sensitive
