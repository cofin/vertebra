import sys
import cbor2
print(f"CBOR2 file: {cbor2.__file__}")
try:
    from _cbor2 import encoder
    print("CBOR2: C Extension Detected")
except ImportError as e:
    print(f"CBOR2: Pure Python Detected. Error importing _cbor2: {e}")
    # Try to find where it might be
    import importlib.util
    spec = importlib.util.find_spec("_cbor2")
    print(f"_cbor2 spec: {spec}")