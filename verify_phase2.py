
import datetime
import decimal
import ipaddress
import pathlib
import uuid
from vertebra.serializers.codecs import VertebraCodec

def verify_codec():
    print("Verifying VertebraCodec...")
    codec = VertebraCodec()
    data = {
        "path": pathlib.Path("/tmp/foo"),
        "ip": ipaddress.IPv4Address("127.0.0.1"),
        "uuid": uuid.uuid4(),
        "decimal": decimal.Decimal("1.23"),
        "dt": datetime.datetime.now()
    }
    encoded = codec.encode(data)
    decoded = codec.decode(encoded, type_hint=dict) # Untyped decoding
    # Note: Untyped decoding might return strings for some types if default decoder is used.
    # But let's check typed decoding.
    
    # Typed decoding requires a struct or type hint.
    # But our decoder hooks handles specific types if we ask for them.
    # For `dict`, it decodes recursively but doesn't know what types expected inside.
    # So expected behavior: Path -> str, IP -> str, etc.
    
    print(f"Encoded: {encoded!r}")
    print(f"Decoded (untyped): {decoded}")
    
    # Typed check
    import msgspec
    class Data(msgspec.Struct):
        path: pathlib.Path
        ip: ipaddress.IPv4Address
    
    d2 = Data(path=pathlib.Path("/tmp/bar"), ip=ipaddress.IPv4Address("10.0.0.1"))
    enc2 = codec.encode(d2)
    dec2 = codec.decode(enc2, type_hint=Data)
    print(f"Decoded (typed): {dec2}")
    assert isinstance(dec2.path, pathlib.Path)
    assert isinstance(dec2.ip, ipaddress.IPv4Address)
    print("VertebraCodec verification passed!")

if __name__ == "__main__":
    verify_codec()
