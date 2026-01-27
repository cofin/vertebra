import cbor2

try:
    import _cbor2

    print(f"Content of _cbor2: {dir(_cbor2)}")
except ImportError:
    print("_cbor2 not importable")

import cbor2.decoder
import cbor2.encoder

print(f"cbor2.encoder: {cbor2.encoder}")
# Check if C types are present in the encoder module
print(f"Has CEncoder: {hasattr(cbor2.encoder, 'CEncoder')}")
