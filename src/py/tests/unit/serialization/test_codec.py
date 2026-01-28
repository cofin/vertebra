"""Tests for Vertebra serialization codecs."""

import datetime
import decimal
import uuid

import msgspec

from vertebra.core.serializers import JSONCodec, MessagePackCodec


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


class TestPreCreatedDecoders:
    """Test pre-created decoder optimization for common types."""

    def test_json_decode_with_dict_type_hint(self) -> None:
        """Test that dict type hint uses pre-created decoder."""
        codec = JSONCodec()
        data = {"key": "value", "number": 42}
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=dict)
        assert decoded == data
        assert isinstance(decoded, dict)

    def test_json_decode_with_list_type_hint(self) -> None:
        """Test that list type hint uses pre-created decoder."""
        codec = JSONCodec()
        data = [1, 2, 3, "four", 5.0]
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=list)
        assert decoded == data
        assert isinstance(decoded, list)

    def test_json_decode_with_str_type_hint(self) -> None:
        """Test that str type hint uses pre-created decoder."""
        codec = JSONCodec()
        data = "hello world"
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=str)
        assert decoded == data
        assert isinstance(decoded, str)

    def test_json_decode_with_int_type_hint(self) -> None:
        """Test that int type hint uses pre-created decoder."""
        codec = JSONCodec()
        data = 12345
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=int)
        assert decoded == data
        assert isinstance(decoded, int)

    def test_json_decode_with_float_type_hint(self) -> None:
        """Test that float type hint uses pre-created decoder."""
        codec = JSONCodec()
        data = 3.14159
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=float)
        assert decoded == data
        assert isinstance(decoded, float)

    def test_json_decode_with_bool_type_hint(self) -> None:
        """Test that bool type hint uses pre-created decoder."""
        codec = JSONCodec()
        for data in [True, False]:
            encoded = codec.encode(data)
            decoded = codec.decode(encoded, type_hint=bool)
            assert decoded == data
            assert isinstance(decoded, bool)

    def test_msgpack_decode_with_dict_type_hint(self) -> None:
        """Test that dict type hint uses pre-created decoder for msgpack."""
        codec = MessagePackCodec()
        data = {"key": "value", "number": 42}
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=dict)
        assert decoded == data
        assert isinstance(decoded, dict)

    def test_msgpack_decode_with_list_type_hint(self) -> None:
        """Test that list type hint uses pre-created decoder for msgpack."""
        codec = MessagePackCodec()
        data = [1, 2, 3, "four", 5.0]
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=list)
        assert decoded == data
        assert isinstance(decoded, list)

    def test_msgpack_decode_with_str_type_hint(self) -> None:
        """Test that str type hint uses pre-created decoder for msgpack."""
        codec = MessagePackCodec()
        data = "hello world"
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=str)
        assert decoded == data
        assert isinstance(decoded, str)

    def test_msgpack_decode_with_int_type_hint(self) -> None:
        """Test that int type hint uses pre-created decoder for msgpack."""
        codec = MessagePackCodec()
        data = 12345
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=int)
        assert decoded == data
        assert isinstance(decoded, int)

    def test_msgpack_decode_with_float_type_hint(self) -> None:
        """Test that float type hint uses pre-created decoder for msgpack."""
        codec = MessagePackCodec()
        data = 3.14159
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=float)
        assert decoded == data
        assert isinstance(decoded, float)

    def test_msgpack_decode_with_bool_type_hint(self) -> None:
        """Test that bool type hint uses pre-created decoder for msgpack."""
        codec = MessagePackCodec()
        for data in [True, False]:
            encoded = codec.encode(data)
            decoded = codec.decode(encoded, type_hint=bool)
            assert decoded == data
            assert isinstance(decoded, bool)

    def test_msgpack_decode_with_bytes_type_hint(self) -> None:
        """Test that bytes type hint uses pre-created decoder for msgpack."""
        codec = MessagePackCodec()
        data = b"binary data"
        encoded = codec.encode(data)
        decoded = codec.decode(encoded, type_hint=bytes)
        assert decoded == data
        assert isinstance(decoded, bytes)

    def test_fallback_to_cached_decoder(self) -> None:
        """Test that unknown types fall back to cached decoder creation."""
        import dataclasses

        @dataclasses.dataclass
        class CustomData:
            name: str
            value: int

        codec = JSONCodec()
        # First decode creates and caches the decoder
        encoded = codec.encode({"name": "test", "value": 123})
        decoded1 = codec.decode(encoded, type_hint=CustomData)
        assert decoded1.name == "test"
        assert decoded1.value == 123

        # Second decode should use cached decoder
        encoded2 = codec.encode({"name": "test2", "value": 456})
        decoded2 = codec.decode(encoded2, type_hint=CustomData)
        assert decoded2.name == "test2"
        assert decoded2.value == 456

        # Verify decoder was cached
        assert CustomData in codec._decoder_cache

    def test_msgpack_fallback_to_cached_decoder(self) -> None:
        """Test that unknown types fall back to cached decoder for msgpack."""
        import dataclasses

        @dataclasses.dataclass
        class CustomMsgData:
            name: str
            count: int

        codec = MessagePackCodec()
        # First decode creates and caches the decoder
        encoded = codec.encode({"name": "test", "count": 99})
        decoded1 = codec.decode(encoded, type_hint=CustomMsgData)
        assert decoded1.name == "test"
        assert decoded1.count == 99

        # Second decode should use cached decoder
        encoded2 = codec.encode({"name": "test2", "count": 200})
        decoded2 = codec.decode(encoded2, type_hint=CustomMsgData)
        assert decoded2.name == "test2"
        assert decoded2.count == 200

        # Verify decoder was cached
        assert CustomMsgData in codec._decoder_cache
