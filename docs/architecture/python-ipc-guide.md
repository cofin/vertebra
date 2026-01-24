# Python IPC Guide

## High-Level Channel API

The recommended way to use Vertebra IPC is through the high-level `Channel` class which provides protocol headers, correlation IDs, and automatic serialization.

```python
from vertebra.ipc.channel import Channel

# Create a channel pair (client + server)
client, server = Channel.create_pair(slot_size=4096, slots=64)

# Server: receive and respond
procedure_id, params = server.receive_sync(timeout_ms=5000)
server.respond_sync(procedure_id, {"result": "ok"})

# Client: send and receive
result = client.send_sync("my.procedure", {"key": "value"}, timeout_ms=5000)
```

## Protocol Headers

All IPC messages use a fixed 16-byte header for zero-parse-cost multiplexing:

```python
from vertebra.protocol import RequestHeader, ResponseHeader, Flags, HEADER_SIZE

# Create a request header
header = RequestHeader(id=42, flags=Flags.NONE, payload_len=128)
header_bytes = header.to_bytes()  # 16 bytes, little-endian

# Parse from bytes
parsed = RequestHeader.from_bytes(data)
print(f"Correlation ID: {parsed.id}, Payload: {parsed.payload_len} bytes")
```

Wire format:

```
┌─────────────────────┬─────────────────────┐
│  Header (16 bytes)  │  MessagePack Payload │
└─────────────────────┴─────────────────────┘

Header layout (little-endian):
- id:          u64 (8 bytes) - Correlation ID for multiplexing
- flags:       u32 (4 bytes) - PRIORITY, COMPRESSED, ERROR, etc.
- payload_len: u32 (4 bytes) - Length of payload
```

## Import the Extension

```python
from vertebra import _vertebra as v
```

## Optional Dependencies & Typing

Vertebra uses a strict typing pattern for optional dependencies like `pyarrow` and `numpy`. See [Optional Dependencies](optional-dependencies.md) for details on how to use `ArrowTable` and other types safely without requiring the dependencies to be installed.

## Shared Memory + Named SPSC (Unix)

```python
region = v.ShmRegion.create_named("/vb-demo-shm", 4096)
region.write(0, b"hello")

ring = v.SpscRing.create_named("/vb-demo-ring", 64, 256)
ring.send(b"ping", 0)
assert ring.recv(64) == b"ping"
```

## Buffer Pool + Buffer Channel

```python
channel = v.BufferChannel(128, 4, 64)
buf = channel.acquire()
buf.write(b"payload")
channel.send(buf, 7, 0)
buffer, length, flags = channel.recv()
assert buffer.read()[:length] == b"payload"
```

## MPMC

```python
ring = v.MpmcRing(64, 128)
ring.send(b"hello", 0)
assert ring.recv(64) == b"hello"
```
