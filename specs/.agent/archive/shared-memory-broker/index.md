# Shared Memory Broker (Bun + C ABI)

Status: In Progress (supersedes socket-based driver protocol)

## Summary

This spec defines the native shared-memory broker that powers all SQL **and storage**
access in Vertebra. The broker is a **central Bun process** that owns connection pools
and storage adapters. Python and other runtimes talk to it through a **C ABI shared-memory
transport** (no sockets, no TCP). Results use MessagePack for small payloads and Arrow
IPC for large result sets (shared-memory Arrow regions).

## Scope

- Shared-memory transport (ring buffers + semaphores) for broker requests/responses.
- C ABI library used by **Bun (bun:ffi)** and **Python (CFFI)**.
- Bun broker process that owns pools and manages fairness scheduling.
- Provider registration for non-Bun SQLSpec adapters.
- Provider registration for storage backends (fsspec/object store) over the same C ABI.

## Links

- [PRD](./prd.md)
- [Tasks](./tasks.md)
- [Transport Design](./design/transport.md)
