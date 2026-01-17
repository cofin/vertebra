# Python IPC Guide

## Import the Extension

```python
from vertebra import _vertebra as v
```

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
