---
type: Task
id: "broker-pool-sharing:ch05:t02"
chapter_id: ch05-lease-safety-and-host
title: Authenticated worker attach
description: Add BrokerAttachServer and BrokerAttachClient: a permission-checked private Unix socket handshake with constant-time secret compare that passes the broker SHM fd by SCM_RIGHTS, with typed errors and redacted secrets.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch05:t01"
read_set:
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/errors.py
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
files:
  - src/py/vertebra/extensions/broker/attach.py
  - src/py/vertebra/extensions/broker/errors.py
  - src/py/vertebra/extensions/broker/transport.py
tests:
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
estimated_loc: 240
plan_revision: 3
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: from-fd
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker::from_fd
    title: fd attach
  - id: shm-transport
    resource: src/py/vertebra/extensions/broker/transport.py::BrokerConfig
    title: Broker configuration
tags:
  - broker
---

# Task t02: Authenticated Worker Attach

## Objective & Scope Boundary

Implement [#97](https://github.com/cofin/vertebra/issues/97):

1. Create `src/py/vertebra/extensions/broker/attach.py` with `BrokerAttachServer` and `BrokerAttachClient` (chapter §2.2). Server: creates the socket directory with mode `0700`, binds `AF_UNIX` `SOCK_STREAM` with mode `0600`, accepts on a daemon thread, reads a 4-byte length plus secret within `timeout`, compares with `hmac.compare_digest`, then sends `b"OK"` plus the SHM fd via `socket.send_fds` (`[broker.shm_fd()]`); on mismatch sends `b"NO"` and closes. Each connection is handled on its own short-lived thread so a stalled client never blocks others.
2. Client: `os.lstat` the socket and its parent directory; refuse (raise `BrokerAuthenticationError`) when `st_uid != os.getuid()`, when the parent mode is not `0700`, or when the path is a symlink, all before sending the secret; then send the secret, receive the fd via `socket.recv_fds`, and return `LeaseBroker.from_fd(fd)` [^from-fd].
3. Add `BrokerAuthenticationError(BrokerAttachError)` and `LeaseQuarantinedError(VertebraError)` (attributes `slot_index: int`, `generation: int`) to `errors.py`.
4. `BrokerConfig` [^shm-transport] gains `attach_socket: str | None = None` and `secret_file: str | None = None`; `_BaseSHMBrokerTransport.__init__` uses `BrokerAttachClient.attach` when `attach_socket` is set (secret read from `secret_file`, which must be mode `0600`).
5. `__repr__` of server, client config, and `BrokerConfig` render `secret_file` path only, never secret bytes.

## Bounded Read Manifest

- `src/rs/vertebra-py/src/broker.rs` — `LeaseBroker::{from_fd,shm_fd}`.
- `src/py/vertebra/extensions/broker/transport.py` — `BrokerConfig`, `_BaseSHMBrokerTransport.__init__`.
- `src/py/vertebra/extensions/broker/errors.py`, `src/py/tests/unit/extensions/test_shm_lease_broker.py`.

## Symbol & Signature Contracts

- `MAX_SECRET_BYTES: Final = 256`; secrets shorter than 16 bytes raise `ValueError` at server construction.
- Wire: client → `struct.pack("!I", len(secret)) + secret`; server → `b"OK"` with one fd or `b"NO"`.

## Edge Cases & Error Semantics

- Wrong secret: `BrokerAuthenticationError("broker rejected attach credentials")`; no fd received.
- Squatted socket (other UID, wrong dir mode, symlink): `BrokerAuthenticationError` naming the check; zero bytes written to the socket.
- Stalled client: server closes it after `timeout`; a concurrent valid client attaches successfully.
- Server ABI/generation mismatch after attach: `LeaseBroker.from_fd` raises; the client closes the fd (no leaked resources).
- `secret_file` mode not `0600`: `BrokerAttachError`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_shm_lease_broker.py`. Dependency category: local Unix socket (`tmp_path`).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Valid attach | Server with secret `b"s" * 32`; client same secret | Returned broker `stats().total_capacity` equals server's |
| Wrong secret | Client secret `b"x" * 32` | `BrokerAuthenticationError`; server `stats().active_leases == 0` |
| Squatted dir mode | `chmod(tmp_path / "sock-dir", 0o755)` before attach | `BrokerAuthenticationError` mentioning `0700`; server recorded zero received bytes |
| Symlink | Socket path is a symlink to the real socket | `BrokerAuthenticationError` |
| Stalled client | Raw socket connects and sleeps; second client attaches | Second attach succeeds within 1 s |
| Redaction | `caplog` at DEBUG over a full attach; `repr(server)` | Secret bytes absent from logs and repr |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py -q -k attach`.
- `make lint`.

## Acceptance Criteria

- [ ] Only processes holding the secret obtain the broker segment; squatted sockets are refused before any secret is written (R12).

## Notes & Discoveries

- [2026-11-12 01:40] Supervisor-spawned workers keep attaching through the inherited `VERTEBRA_BROKER_SHM_FD`; inheritance is the trust boundary there.

[^from-fd]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker::from_fd` — fd attach.
[^shm-transport]: `src/py/vertebra/extensions/broker/transport.py::BrokerConfig` — Broker configuration.
