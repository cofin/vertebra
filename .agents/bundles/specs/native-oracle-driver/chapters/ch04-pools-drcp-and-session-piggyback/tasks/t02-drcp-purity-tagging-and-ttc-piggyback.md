---
type: Task
parent_flow: native-oracle-driver
chapter_id: ch04-pools-drcp-and-session-piggyback
id: "native-oracle-driver:ch04:t02"
title: "Implement DRCP purity, connection tagging, and zero-extra-round-trip TTC session piggyback"
description: "Implement DRCP cclass, purity (SELF vs NEW), and connection_tag state tracking (acquire_from_pool, release_to_pool) plus zero-extra-round-trip TTC piggyback ([0x11, 0x6e, count, ...] via vertebra_db::oracle::OracleSessionAttrs) in pool.rs, client.rs, and _oracle.pyi."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch04:t01"
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch04-pools-drcp-and-session-piggyback/spec.md
  - src/rs/vertebra-db/src/oracle/mod.rs
  - src/py/vertebra/db/adapters/oracle/config.py
files:
  - src/rs/vertebra-oracle/src/pool.rs
  - src/rs/vertebra-oracle/src/client.rs
  - src/py/vertebra/_oracle.pyi
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 220
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
commit_sha: 0f100bd2
completed_at: "2026-11-09T16:05:00Z"
created_at: "2026-11-09T00:58:00Z"
updated_at: "2026-11-09T16:05:00Z"
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public interface, FFI, type, pooling, tenancy, and ownership contracts
  - id: ora-rs-codecs
    resource: src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs
    title: Shared Rust OracleSessionAttrs TTC piggyback encoder ([0x11, 0x6e, count, ...])
  - id: ora-config
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleDrcpConfig
    title: OracleDrcpConfig cclass, purity, and connection_tag configuration
tags:
  - oracle
  - drcp
  - piggyback
  - ttc
  - pool
---

# Task `native-oracle-driver:ch04:t02`: Implement DRCP purity, connection tagging, and zero-extra-round-trip TTC session piggyback

<!-- truth: start -->
- **DRCP `cclass`, `purity` (`SELF` vs `NEW`), and `connection_tag` State Tracking**: `SyncOraclePool`, `AsyncOraclePool`, `SyncOracleClient`, and `AsyncOracleClient` enforce `purity in ("SELF", "NEW")`, preferentially check out tag-matched idle sessions via `acquire` / `acquire_from_pool` when `purity == "SELF"` and `connection_tag` is specified, update session tags on `release` / `release_to_pool`, and declare all DRCP and piggyback properties/methods in `src/py/vertebra/_oracle.pyi` [^ora-contracts] [^ora-config].
- **Zero-Extra-Round-Trip TTC Session Attribute Piggyback (`[0x11, 0x6e, count, ...]`)**: `OracleSessionAttrs` (`CLIENT_IDENTIFIER`, `MODULE`, `ACTION`, `ECID` in `vertebra_db::oracle`) is staged in memory on `set_session_attrs` or `pool.acquire(...)` and flushed onto the worker connection (`encode_ttc_piggyback_session_attrs`, `frame.startswith(b"\x11\x6e")`) on the first outbound statement execution with zero extra network round-trips [^ora-contracts] [^ora-rs-codecs].
<!-- truth: end -->

## 1. Objective & Scope Boundary

Implement DRCP `cclass`, `purity` (`"SELF"` vs `"NEW"`), and `connection_tag` state tracking (`acquire_from_pool`, `release_to_pool`, `tag_matched`) plus zero-extra-round-trip TTC session attribute piggyback (`CLIENT_IDENTIFIER`, `MODULE`, `ACTION`, `ECID` via `vertebra_db::oracle::OracleSessionAttrs::encode_ttc_piggyback_session_attrs`) in `src/rs/vertebra-oracle/src/pool.rs` and `src/rs/vertebra-oracle/src/client.rs`, and synchronize `src/py/vertebra/_oracle.pyi` in the same task [^ora-contracts] [^ora-rs-codecs].

- **In Scope**:
  - DRCP metadata (`cclass: Option<String>`, `purity: String`, `connection_tag: Option<String>`, `tag_matched: bool`) on `SyncOracleClient` and `AsyncOracleClient` in `src/rs/vertebra-oracle/src/client.rs`.
  - Tag-aware checkout (`acquire` / `acquire_from_pool(connection_tag=..., purity=...)`) and release (`release` / `release_to_pool(client, connection_tag=...)`) in `src/rs/vertebra-oracle/src/pool.rs`: when `purity == "SELF"` and `connection_tag` is requested, scan the idle deque for an exact tag match (`tag_matched = true`) before falling back to an untagged/other idle session (`tag_matched = false`); when `purity == "NEW"`, reset/clear any prior session tag state and set `tag_matched = false`.
  - Staging `OracleSessionAttrs` (`client_identifier`, `module`, `action`, `ecid`) on `SyncOracleClient.set_session_attrs`, `AsyncOracleClient.set_session_attrs`, and `pool.acquire(...)` without performing socket I/O, and flushing the staged attributes (`encode_ttc_piggyback_session_attrs` producing `[0x11, 0x6e, count, ...]` + `oracledb` connection attribute setters) on the first outbound `query`, `query_one`, `execute`, `execute_many`, or `query_arrow` call [^ora-rs-codecs].
  - Updating `src/py/vertebra/_oracle.pyi` with `cclass`, `purity`, `connection_tag`, `tag_matched`, `pending_session_attrs`, `active_session_attrs`, `last_piggyback_frame`, `set_session_attrs`, `acquire_from_pool`, and `release_to_pool`.
- **Out of Scope**:
  - Python `SyncOraclePool`, `AsyncOraclePool`, `SyncOracleDriver`, and `AsyncOracleDriver` wrapper classes and `ContextVar` transaction pinning (`ch04:t03`).
  - VPD `DBMS_SESSION.SET_IDENTIFIER` / trusted PL/SQL context package tenancy scopes (`ch05:t02`).

## 2. Bounded Read Manifest

| File Path | Purpose |
| :--- | :--- |
| `.agents/bundles/specs/native-oracle-driver/contracts.md` | DRCP purity, tagging, and TTC piggyback contracts [^ora-contracts] |
| `.agents/bundles/specs/native-oracle-driver/chapters/ch04-pools-drcp-and-session-piggyback/spec.md` | Chapter 04 DRCP and TTC piggyback architecture |
| `src/rs/vertebra-db/src/oracle/mod.rs` | `OracleSessionAttrs` and `encode_ttc_piggyback_session_attrs` (`[0x11, 0x6e, count, ...]`) [^ora-rs-codecs] |
| `src/py/vertebra/db/adapters/oracle/config.py` | `OracleDrcpConfig` (`cclass`, `purity`, `connection_tag`) [^ora-config] |

## 3. Symbol & Signature Contracts

### 3.1 `src/rs/vertebra-oracle/src/client.rs`, `src/rs/vertebra-oracle/src/pool.rs` & `src/py/vertebra/_oracle.pyi`

Expose session attribute staging, active attribute inspection, and DRCP metadata properties/methods on `SyncOracleClient`, `AsyncOracleClient`, `SyncOraclePool`, and `AsyncOraclePool`:

```python
class SyncOracleClient:
    @property
    def cclass(self) -> str | None: ...
    @property
    def purity(self) -> str: ...
    @property
    def connection_tag(self) -> str | None: ...
    @property
    def tag_matched(self) -> bool: ...
    @property
    def pending_session_attrs(self) -> dict[str, str | None] | None: ...
    @property
    def active_session_attrs(self) -> dict[str, str | None]: ...
    @property
    def last_piggyback_frame(self) -> bytes | None: ...
    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None: ...

class AsyncOracleClient:
    @property
    def cclass(self) -> str | None: ...
    @property
    def purity(self) -> str: ...
    @property
    def connection_tag(self) -> str | None: ...
    @property
    def tag_matched(self) -> bool: ...
    @property
    def pending_session_attrs(self) -> dict[str, str | None] | None: ...
    @property
    def active_session_attrs(self) -> dict[str, str | None]: ...
    @property
    def last_piggyback_frame(self) -> bytes | None: ...
    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None: ...

class SyncOraclePool:
    def acquire_from_pool(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> SyncOracleClient: ...
    def release_to_pool(
        self,
        client: SyncOracleClient,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None: ...

class AsyncOraclePool:
    async def acquire_from_pool(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> AsyncOracleClient: ...
    async def release_to_pool(
        self,
        client: AsyncOracleClient,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None: ...
```

Inside `src/rs/vertebra-oracle/src/client.rs`:
- `set_session_attrs` merges non-`None` fields into `vertebra_db::oracle::OracleSessionAttrs { client_identifier, module, action, ecid }` in `pending_attrs` without sending a worker command or network packet.
- When `query`, `query_one`, `execute`, `execute_many`, or `query_arrow` dispatches a command to the worker thread, it attaches `pending_attrs.take()`, encodes the TTC piggyback frame via `attrs.encode_ttc_piggyback_session_attrs()` (`[0x11, 0x6e, count, ...]`), applies the attributes to the `oracledb::Connection` (`set_client_identifier`, `set_module`, `set_action`, `set_dbop` / `ecid`) before executing the SQL statement, updates `active_session_attrs`, and records `last_piggyback_frame` [^ora-rs-codecs].

## 4. Edge Cases & Error Semantics

- **Invalid DRCP `purity` Values**: Passing any `purity` value other than `"SELF"` or `"NEW"` to `SyncOracleClient`, `AsyncOracleClient`, `SyncOraclePool`, `AsyncOraclePool`, or `pool.acquire(purity=...)` returns `OracleError` mapped to `InterfaceError` / `ValueError` [^ora-contracts] [^ora-config].
- **Multiple `set_session_attrs` Calls Before First Statement**: Calling `set_session_attrs` multiple times before executing a statement merges non-`None` fields into the staged `OracleSessionAttrs` so all staged attributes flush together on the first outbound statement in a single `[0x11, 0x6e, count, ...]` frame.
- **No-Op Piggyback When No Attributes Are Staged**: Subsequent statements after the first flush have `pending_session_attrs is None` and do not re-encode or re-send unchanged session attributes.
- **Clear on Eviction or `purity="NEW"`**: When `pool.acquire(purity="NEW")` checks out an idle session, any previous `connection_tag` and `active_session_attrs` are cleared and `tag_matched` is `False`.

## 5. Public-Seam Test Blueprint

Add behavioral DRCP and TTC piggyback unit tests in `src/py/tests/unit/db/test_oracle_adapter.py`:

| Test Function | Input / Setup | Action | Expected Observable Assertion |
| :--- | :--- | :--- | :--- |
| `test_oracle_drcp_purity_validation_and_tag_matching` | `SyncOraclePool(cclass="VTB_POOL", purity="SELF", max_size=2)` with two idle sessions tagged `"TAG_A"` and `"TAG_B"` | Call `pool.acquire(connection_tag="TAG_B", purity="SELF")`, `pool.acquire(connection_tag="TAG_B", purity="NEW")`, and `pool.acquire(purity="BOGUS")` | First acquire returns the `"TAG_B"` session with `tag_matched is True`; `purity="NEW"` returns `tag_matched is False` and clears tag; `purity="BOGUS"` raises `InterfaceError` / `ValueError` |
| `test_oracle_ttc_piggyback_stages_on_checkout_and_flushes_on_first_query` | `pool.acquire(client_identifier="tenant-42", module="billing", action="invoice", ecid="req-99")` (sync and async) | Inspect `client.pending_session_attrs` before query, then run `client.query("SELECT 1 FROM DUAL")` and inspect `pending_session_attrs`, `active_session_attrs`, and `last_piggyback_frame` | Before query: `pending_session_attrs` holds all 4 fields and zero network calls occurred; after first query: `pending_session_attrs is None`, `active_session_attrs` matches all 4 fields, and `client.last_piggyback_frame.startswith(b"\x11\x6e")` is `True` |
| `test_oracle_multiple_set_session_attrs_coalesce_before_statement` | Call `client.set_session_attrs(client_identifier="t1", module="m1")` followed by `client.set_session_attrs(action="a1")` | Execute `client.execute("UPDATE t SET x = 1")` | Merged attributes `{"client_identifier": "t1", "module": "m1", "action": "a1", "ecid": None}` flush together in a single `b"\x11\x6e\x03..."` frame on `execute` |

## 6. Verification

```bash
make dev
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
uv run --no-sync python -m mypy.stubtest vertebra._oracle
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -k "drcp or piggyback or set_session_attrs" -q
```

## 7. Acceptance Criteria

- [x] `SyncOraclePool`, `AsyncOraclePool`, `SyncOracleClient`, and `AsyncOracleClient` track DRCP `cclass`, `purity` (`"SELF"` vs `"NEW"`), `connection_tag`, and `tag_matched` (`acquire_from_pool` / `release_to_pool`).
- [x] `set_session_attrs` and `pool.acquire(...)` stage `OracleSessionAttrs` in memory and flush `[0x11, 0x6e, count, ...]` (`CLIENT_IDENTIFIER`, `MODULE`, `ACTION`, `ECID`) on the first outbound statement with zero extra round-trips.
- [x] `src/py/vertebra/_oracle.pyi` declares all DRCP and piggyback properties/methods and passes `uv run --no-sync python -m mypy.stubtest vertebra._oracle`.
- [x] All DRCP and TTC piggyback tests in `src/py/tests/unit/db/test_oracle_adapter.py` pass.

## 8. Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public interface, FFI, type, pooling, tenancy, and ownership contracts.
[^ora-rs-codecs]: `src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs` — Shared Rust `OracleSessionAttrs` TTC piggyback encoder (`[0x11, 0x6e, count, ...]`).
[^ora-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleDrcpConfig` — `OracleDrcpConfig` `cclass`, `purity`, and `connection_tag` configuration.
