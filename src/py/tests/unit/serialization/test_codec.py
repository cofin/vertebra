"""Tests for Vertebra serialization codecs."""

import datetime
import decimal
import uuid

import msgspec

from vertebra.common.serializers import JSONCodec, MessagePackCodec


class TestJSONCodec:
    """Test JSON serialization."""

    def test_encode_decode_primitives(self) -> None:
        codec = JSONCodec()
        data = {"a": 1, "b": "test", "c": True}
        encoded = codec.encode(data)
        decoded = codec.decode(encoded)
        assert decoded == data

    def test_encode_decode_datetime(self) -> None:
        codec = JSONCodec()
        now = datetime.datetime.now(datetime.UTC)
        encoded = codec.encode(now)
        # JSON decodes to string by default
        decoded = codec.decode(encoded)
        # Parse back to compare time value, ignoring Z vs +00:00 difference
        assert datetime.datetime.fromisoformat(decoded) == now

        # Typed decoding
        decoded_typed = codec.decode(encoded, type_hint=datetime.datetime)
        assert decoded_typed == now

    def test_encode_decode_uuid(self) -> None:
        codec = JSONCodec()
        u = uuid.uuid4()
        encoded = codec.encode(u)
        decoded = codec.decode(encoded)
        assert decoded == str(u)

        # Typed decoding
        decoded_typed = codec.decode(encoded, type_hint=uuid.UUID)
        assert decoded_typed == u

    def test_encode_decode_decimal(self) -> None:
        codec = JSONCodec()
        d = decimal.Decimal("1.23")
        encoded = codec.encode(d)
        decoded = codec.decode(encoded)
        assert decoded == "1.23"

        # Typed decoding
        decoded_typed = codec.decode(encoded, type_hint=decimal.Decimal)
        assert decoded_typed == d

    def test_encode_decode_path(self) -> None:
        import pathlib

        codec = JSONCodec()
        p = pathlib.Path("/tmp/foo")
        encoded = codec.encode(p)
        decoded = codec.decode(encoded)
        assert decoded == "/tmp/foo"

        decoded_typed = codec.decode(encoded, type_hint=pathlib.Path)
        assert decoded_typed == p

    def test_encode_decode_ip(self) -> None:
        import ipaddress

        codec = JSONCodec()
        ip = ipaddress.IPv4Address("127.0.0.1")
        encoded = codec.encode(ip)
        decoded = codec.decode(encoded)
        assert decoded == "127.0.0.1"

        decoded_typed = codec.decode(encoded, type_hint=ipaddress.IPv4Address)
        assert decoded_typed == ip

    def test_encode_decode_set(self) -> None:
        codec = JSONCodec()
        s = {1, 2, 3}
        encoded = codec.encode(s)
        decoded = codec.decode(encoded)
        assert set(decoded) == s

        # msgspec JSON doesn't support typed decoding to set directly from list without type_hint
        decoded_typed = codec.decode(encoded, type_hint=set[int])
        assert decoded_typed == s

    def test_encode_decode_dataclass(self) -> None:
        import dataclasses

        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        codec = JSONCodec()
        p = Point(1, 2)
        encoded = codec.encode(p)
        decoded = codec.decode(encoded)
        assert decoded == {"x": 1, "y": 2}

        # Dataclass decoding is not automatic unless we register a hook or use type_hint with struct?
        # msgspec supports dataclasses natively if we use generic functions but here we use Codec.
        # VertebraCodec doesn't auto-decode to dataclass unless we tell it.
        # But we can test that it encodes correctly at least.


class TestMessagePackCodec:
    """Test MessagePack serialization."""

    def test_encode_decode_primitives(self) -> None:
        codec = MessagePackCodec()
        data = {"a": 1, "b": "test", "c": True}
        encoded = codec.encode(data)
        decoded = codec.decode(encoded)
        assert decoded == data

    def test_encode_decode_path(self) -> None:
        import pathlib

        codec = MessagePackCodec()
        p = pathlib.Path("/tmp/foo")
        encoded = codec.encode(p)
        # msgpack might encode as string via default_encoder
        decoded = codec.decode(encoded)
        assert decoded == "/tmp/foo"

        decoded_typed = codec.decode(encoded, type_hint=pathlib.Path)
        assert decoded_typed == p

    def test_encode_decode_ip(self) -> None:
        import ipaddress

        codec = MessagePackCodec()
        ip = ipaddress.IPv4Address("127.0.0.1")
        encoded = codec.encode(ip)
        decoded = codec.decode(encoded)
        assert decoded == "127.0.0.1"

        decoded_typed = codec.decode(encoded, type_hint=ipaddress.IPv4Address)
        assert decoded_typed == ip

    def test_encode_decode_bytes(self) -> None:
        codec = MessagePackCodec()
        b = b"hello bytes"
        encoded = codec.encode(b)
        decoded = codec.decode(encoded)
        assert decoded == b

    def test_encode_decode_datetime(self) -> None:
        codec = MessagePackCodec()
        now = datetime.datetime.now(datetime.UTC)
        encoded = codec.encode(now)
        # Msgpack preserves datetime natively (extension type)
        # But msgspec might encode it as string if default_encoder interferes?
        # msgspec handles datetime natively.
        codec.decode(encoded)
        # msgspec decodes datetime strings if they are strings, but if it's native ext type it returns datetime.
        # Let's see what default_encoder does. It passes datetime through usually or encodes to str.
        # Check if default_encoder handles datetime.
        # If default_encoder returns isoformat string, then decode returns string.
        # If default_encoder returns None (pass through), msgspec handles it.

        # Typed decoding
        decoded_typed = codec.decode(encoded, type_hint=datetime.datetime)
        assert decoded_typed == now

    def test_encode_decode_uuid(self) -> None:
        codec = MessagePackCodec()
        u = uuid.uuid4()
        encoded = codec.encode(u)
        codec.decode(encoded)
        # Msgpack might decode as string or bytes depending on encoding

        # Typed decoding
        decoded_typed = codec.decode(encoded, type_hint=uuid.UUID)
        assert decoded_typed == u


class CustomStruct(msgspec.Struct):
    x: int
    y: str


class TestTypedDecoding:
    """Test decoding into typed structures."""

    def test_decode_struct(self) -> None:
        codec = JSONCodec()
        data = CustomStruct(x=1, y="foo")
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=CustomStruct)
        assert decoded == data
        assert isinstance(decoded, CustomStruct)

    def test_decode_struct_msgpack(self) -> None:
        codec = MessagePackCodec()
        data = CustomStruct(x=1, y="foo")
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=CustomStruct)
        assert decoded == data
        assert isinstance(decoded, CustomStruct)
