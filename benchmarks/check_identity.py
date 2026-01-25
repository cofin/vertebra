import cbor2
try:
    import _cbor2
except ImportError:
    print("_cbor2 missing")
    exit(1)

print(f"cbor2.dumps is _cbor2.dumps: {cbor2.dumps is _cbor2.dumps}")
print(f"cbor2.dumps: {cbor2.dumps}")
print(f"_cbor2.dumps: {_cbor2.dumps}")
