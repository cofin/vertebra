---
type: Decision
title: 'ADR-005: Persistent Object-Store TTL Envelopes'
description: "Persist value bytes and wall-clock expiration together across object-storage backends and process reconstruction."
status: stable
tags:
- decisions
- adr
- storage
- litestar
- ttl
generated:
  by: flow/0.30.1
  at: '2026-11-08T18:27:30Z'
sources:
- id: litestar-store
  resource: src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore
  title: Versioned envelope encoding and expiration semantics
- id: native-storage
  resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
  title: Local backend fallback without unsupported attributes
- id: store-consumers
  resource: src/py/tests/unit/extensions/test_litestar_plugin.py
  title: Native Local reconstruction, namespace, and corruption consumers
verified:
- by: agent:flow-setup
  at: '2026-11-27T22:10:00Z'
  commit: f3b624a2
- by: agent:codex
  at: '2026-11-08T18:27:30.456016Z'
  commit: 067fa901
---
# ADR-005: Persistent Object-Store TTL Envelopes

<!-- truth: start -->
- `ObjectStoreLitestarStore` writes every value as `b"VTBSTORE\x01"` followed by MessagePack `(expires_at_ms, payload)` [^litestar-store].
- The envelope is authoritative; expiration attributes supplement it when the backend preserves them [^litestar-store] [^native-storage].
- Absolute wall-clock deadlines survive store reconstruction; expired reads delete the object lazily [^litestar-store] [^store-consumers].
<!-- truth: end -->

## Context

Litestar values need consistent expiration across workers and reconstructed store instances. Object-store attributes cannot carry the only deadline: the native Local backend retries writes without unsupported attributes. An in-process TTL map also loses expiration when a worker restarts. Native Local reconstruction tests exercise that boundary [^native-storage] [^store-consumers].

## Decision

Store the optional deadline and payload in one versioned object body. UTF-8 string inputs become bytes. The deadline uses absolute wall-clock milliseconds so separate processes compare the same value. Expiring writes also attach `vertebra-expires-at-ms`; reads use the body's deadline independently of attribute availability [^litestar-store].

Always write the envelope, including immortal values with a `None` deadline. Validate the magic/version and typed MessagePack entry before returning bytes. Renew only live entries already carrying an expiration; immortal values keep their lifetime. Expired values are deleted on read, and `expires_in` returns nonnegative whole seconds or `None` for missing, expired, and immortal entries [^litestar-store].

## Alternatives Rejected

| Alternative | Trade-off |
| --- | --- |
| Store expiration only in object attributes | Preserves raw payload bytes and avoids decoding overhead, but loses deadlines through the Local attribute fallback. |
| Keep a worker-local deadline map | Makes expiry lookup cheap, but loses deadlines on reconstruction and separates workers' expiration state. |
| Persist a separate expiration index | Supports durable deadlines, but adds lookup/write operations and requires coordination between index and payload updates. |

## Consequences

Each stored value carries envelope and decoding overhead. One object write publishes payload and deadline together; no separate TTL index needs synchronization. Wall-clock deadlines inherit clock adjustment/skew behavior. The private persistent format requires an explicit migration or version change if its representation changes; unknown or malformed envelopes fail explicitly [^litestar-store].

Native consumer tests cover cross-instance Local expiration and renewal, immortal-value behavior, corrupt-envelope rejection, and confined namespace deletion. Preserve these consumer contracts when changing the encoding or metadata path [^store-consumers].

[^litestar-store]: `src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore` — Authoritative persistent value envelope and Store behavior.
[^native-storage]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Native backend attributes and Local fallback.
[^store-consumers]: `src/py/tests/unit/extensions/test_litestar_plugin.py` — Real native storage and Litestar consumers.
