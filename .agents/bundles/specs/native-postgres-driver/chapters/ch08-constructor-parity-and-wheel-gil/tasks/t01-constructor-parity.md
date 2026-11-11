---
type: Task
id: "native-postgres-driver:ch08:t01"
chapter_id: ch08-constructor-parity-and-wheel-gil
title: Characterize and lock complete sync and async constructor parity
description: Enforce exact runtime inspect.signature, bool-free integer extraction, mapping snapshot isolation, and _postgres.pyi docstring parity across SyncPostgresClient and AsyncPostgresClient, verifying all 25 constructor parameters from Python.
status: stable
state: completed
priority: P1
verification_strategy: characterization
depends_on:
  - "native-postgres-driver:ch07:t03"
read_set:
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/tests/unit/db/test_postgres_driver.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/vertebra/_postgres.pyi
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
estimated_loc: 235
plan_revision: 5
plan_commit: 0147d80c
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: adf34226
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Exact 25-parameter SyncPostgresClient and AsyncPostgresClient constructor specification
  - id: pg-driver-tests
    resource: src/py/tests/unit/db/test_postgres_driver.py
    title: Canonical native PostgreSQL client unit test module
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: Existing PostgreSQL option precedence and session_gucs semantics
tags:
  - postgres
  - pyo3
  - constructor
  - stubs
---

# Task t01: Characterize and Lock Complete Sync and Async Constructor Parity

## Objective & Scope Boundary

Verify and lock complete 25-parameter constructor parity across `src/rs/vertebra-postgres/src/sync_client.rs`, `src/rs/vertebra-postgres/src/async_client.rs`, and `src/py/vertebra/_postgres.pyi`, characterized end to end in `src/py/tests/unit/db/test_postgres_driver.py` (`native_constructor`) [^pg-contracts] [^pg-driver-tests] [^pg-config]. Ensure runtime `inspect.signature` parameter names, `KEYWORD_ONLY` kinds, and defaults match `contracts.md` identically on both `SyncPostgresClient` and `AsyncPostgresClient`, enforce strict `TypeError` (for positional/unknown kwargs, non-`str` mapping entries, and `bool` passed to integer `port` / `keepalives_*` fields) vs `ValueError` (for semantic conflicts), prove zero I/O at construction time and post-construction `session_gucs` mutation isolation, and verify Python-level reachability of `channel_binding="require"` (`SCRAM-SHA-256-PLUS`) and Unix `sslmode="disable"` startup.

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/sync_client.rs` — `PySyncPostgresClient::__new__` and `extract_string_map` from `ch07:t02`.
- `src/rs/vertebra-postgres/src/async_client.rs` — `PyAsyncPostgresClient::__new__` from `ch07:t03`.
- `src/rs/vertebra-postgres/src/config.rs` — Normalized 25-field `PostgresConfig` semantic rules from `ch06:t03`.
- `src/py/vertebra/_postgres.pyi` — Public type stub and docstrings for `vertebra._postgres`.
- `src/py/tests/unit/db/test_postgres_driver.py` — Loopback socket/TLS server fixtures and native client tests [^pg-driver-tests].
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) `Connection constructors` [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/sync_client.rs` & `src/rs/vertebra-postgres/src/async_client.rs`:
  - Share one canonical constructor-extraction helper across `PySyncPostgresClient` and `PyAsyncPostgresClient` so the two classes can never drift in parameter handling:
    - Exact 25 `KEYWORD_ONLY` parameters in order: `user`, `database`, `host = "localhost"`, `port = 5432`, `password = None`, `unix_socket = None`, `sslmode = "verify-full"`, `ca_pem = None`, `client_cert_pem = None`, `client_key_pem = None`, `server_name = None`, `channel_binding = "prefer"`, `connect_timeout = 10.0`, `application_name = "vertebra"`, `session_gucs = None`, `keepalives = true`, `keepalives_idle = None`, `keepalives_interval = None`, `keepalives_count = None`, `cloud_sql_instance = None`, `cloud_sql_ip_type = "PRIVATE"`, `alloydb_instance_uri = None`, `alloydb_ip_type = "PRIVATE"`, `enable_iam_auth = false`, `cloud_auth_options = None`, `iam_auth_options = None`.
    - Reject `PyBool` passed to `port`, `keepalives_idle`, `keepalives_interval`, or `keepalives_count` (`is_instance_of::<PyBool>()` -> `PyTypeError`) and reject `PyInt`/`PyBool` where `bool` is required (`keepalives`, `enable_iam_auth`).
    - Enforce `Mapping[str, str]` on `session_gucs`, `cloud_auth_options`, and `iam_auth_options` (`PyTypeError` on non-mapping or non-`str` keys/values; snapshot copied immediately while attached).
    - Redact `password`, `client_key_pem`, `access_token`, and `bearer_token` from every raised `ValueError` and `__repr__`.
- `src/py/vertebra/_postgres.pyi`:
  - Preserve module-level `__version__: str`, `FREE_THREADED: bool`, and `GIL_USED: bool` (`__all__ = ["FREE_THREADED", "GIL_USED", "AsyncPostgresClient", "SyncPostgresClient", "__version__"]`), and author complete Google-style class, `def __new__(cls, *, ...) -> SyncPostgresClient` / `def __new__(cls, *, ...) -> AsyncPostgresClient` (never `def __init__`, matching `@final` `#[pyclass(frozen, immutable_type)]` `mypy.stubtest` rules), method, and property docstrings with exact PEP 604/585 annotations (`Mapping[str, str] | None`, `Awaitable[None]`, `dict[str, str]`) verified by `mypy.stubtest vertebra._postgres`.

## Edge Cases & Error Semantics

- Constructing `SyncPostgresClient` or `AsyncPostgresClient` with `cloud_sql_instance="proj:us-central1:inst"` or `alloydb_instance_uri="projects/p/locations/r/clusters/c/instances/i"` or `unix_socket="/nonexistent/.s.PGSQL.5432", sslmode="disable"` performs `0` DNS lookups (`socket.getaddrinfo` spy), `0` socket accepts, and `0` credential refreshes.
- Mutating caller `gucs["application_name"] = "after"` after `client = SyncPostgresClient(..., session_gucs=gucs)` leaves the `StartupMessage` sent during `client.connect()` with the original `"before"` value.
- All semantic conflicts from `ch06:t03` (Unix + TLS/cloud/keepalive tuning/`channel_binding="require"`, Cloud SQL + AlloyDB, invalid `sslmode`/`ip_type`/`channel_binding`, non-`UTF8` `client_encoding`, reserved startup keys, opaque static Admin token without `iam_auth_options`, invalid port/timeout) raise `ValueError` identically on both `SyncPostgresClient` and `AsyncPostgresClient`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_driver.py` (`test_native_constructor_*`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Exact `inspect.signature` & default inventory (`test_native_constructor_signature_parity`) | `inspect.signature(SyncPostgresClient)` and `inspect.signature(AsyncPostgresClient)` | Both expose the exact 25 `KEYWORD_ONLY` parameter names and default values listed in `contracts.md`; positional args `("u", "d")` and unknown kwarg `extra=1` raise `TypeError`. |
| `TypeError` vs `ValueError` & secret redaction | `session_gucs={"k": 1}`, `iam_auth_options={1: "v"}`, `port=True` vs `port=70000`, `sslmode="bogus"`, `password="s3cr3t\x00bad"`, `client_key_pem=b"PRIVATE-KEY-SECRET"` with `client_cert_pem=None` | Type violations raise `TypeError`; semantic violations raise `ValueError` whose message omits `"s3cr3t"` and `"PRIVATE-KEY-SECRET"`. |
| Pure zero-I/O construction & `session_gucs` snapshot isolation | Construct both clients with Cloud SQL, AlloyDB, and Unix options under `socket.getaddrinfo` spy and local server accept counter; mutate `gucs["app.tag"] = "mutated"` after construction and connect | `0` DNS/accept/OAuth calls during construction; connected loopback server receives original `gucs["app.tag"] == "initial"`. |
| Python-level `channel_binding="require"` (`SCRAM-SHA-256-PLUS`) reachability | Both `SyncPostgresClient` and `AsyncPostgresClient` constructed with `sslmode="verify-full"`, `ca_pem=server_ca_pem`, `server_name="localhost"`, `channel_binding="require"`, `password="scram-pass"` against local TLS `SCRAM-SHA-256-PLUS` scripted server | Both complete `tls-server-end-point` channel-bound SCRAM authentication and reach `closed is False` (and fail before credentials when server advertises only plain `SCRAM-SHA-256`). |
| Python-level Unix `sslmode="disable"` + `session_gucs` reachability (`sys.platform != "win32"`) | Both clients constructed with `unix_socket=str(sock_path)`, `sslmode="disable"`, `session_gucs={"application_name": "unix-override", "app.env": "ci"}` | Both connect over Unix domain socket without `SSLRequest` and report `StartupMessage` `"application_name" == "unix-override"` and `"app.env" == "ci"`. |

## Verification

- Characterization & Red/Green baseline:
  - `make dev`
  - `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_constructor -q`
  - `uv run --no-sync python -m mypy.stubtest vertebra._postgres`
  - `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [x] `SyncPostgresClient`, `AsyncPostgresClient`, and `src/py/vertebra/_postgres.pyi` expose identical 25-parameter keyword-only signatures, defaults, and `TypeError`/`ValueError` boundaries matching `contracts.md` and `mypy.stubtest` (R2, R6, R7).
- [x] Pure zero-I/O construction, `Mapping[str, str]` snapshot isolation, secret redaction, `channel_binding="require"` (`SCRAM-SHA-256-PLUS`), and Unix `sslmode="disable"` + `session_gucs` startup are proven for both sync and async clients (R2, R6, R7).
- [x] Commit `test(postgres): preserve complete constructor parity` passes `pytest -k native_constructor`, `stubtest`, and `clippy` (R2, R6, R7).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen 25-parameter `SyncPostgresClient` and `AsyncPostgresClient` constructor specification.
[^pg-driver-tests]: `src/py/tests/unit/db/test_postgres_driver.py` — Canonical PostgreSQL driver unit and characterization tests.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL configuration and `session_gucs` precedence rules.
