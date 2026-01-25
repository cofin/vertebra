# State Sync Protocol

This document defines the opcode-based delta protocol used for native state
synchronization across Python workers and the Bun broker.

## ID Schemes

State and field identifiers are deterministic hashes of names:

- **state_id**: FNV-1a 64-bit over the state class name (UTF-8 bytes)
- **field_id**: FNV-1a 32-bit over the field name (UTF-8 bytes)

These IDs are language-agnostic and must match across Rust, Python, and
TypeScript implementations.

## Opcode Definitions

| Opcode | Name           | Payload                                    |
|--------|----------------|--------------------------------------------|
| 0x01   | FIELD_SET      | state_id, field_id, timestamp_ns, value    |
| 0x02   | FIELD_DELETE   | state_id, field_id, timestamp_ns           |
| 0x03   | OBJECT_REPLACE | state_id, field_id, timestamp_ns, value    |
| 0x04   | SYNC_REQUEST   | state_id                                   |
| 0x05   | SYNC_RESPONSE  | state_id, full_state                       |

## MessagePack Delta Format

State deltas are encoded as MessagePack arrays (not maps) for minimal overhead:

```text
[opcode, state_id, field_id, timestamp_ns, value]
```

The array length depends on the opcode:

```text
FIELD_SET / OBJECT_REPLACE: [op, state_id, field_id, timestamp_ns, value]
FIELD_DELETE:               [op, state_id, field_id, timestamp_ns]
SYNC_REQUEST:               [op, state_id]
SYNC_RESPONSE:              [op, state_id, full_state]
```

## Timestamp Semantics

`timestamp_ns` is a monotonic, nanosecond-resolution `u64` used for
Last-Writer-Wins (LWW) conflict resolution in the broker.

## Example

```text
FIELD_SET:
  [0x01, 0x374157f8ddd4c9a8, 0xd22f9095, 1700000000000000000, {"count": 1}]
```
