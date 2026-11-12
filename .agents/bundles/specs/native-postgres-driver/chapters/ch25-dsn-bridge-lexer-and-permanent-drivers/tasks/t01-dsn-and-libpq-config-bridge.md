---
type: Task
id: "native-postgres-driver:ch25:t01"
chapter_id: ch25-dsn-bridge-lexer-and-permanent-drivers
title: Bridge PostgreSQL URL and libpq configuration into native options
description: Implement pure-Python URI and libpq keyword DSN parsing in options.py and wire PostgreSQLConfig.from_url, pool_key, resolve_sync_options, resolve_async_options, and derive_direct_config in config.py.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch24:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/py/vertebra/db/adapters/postgres/config.py
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
  - .agents/skills/python-framework/SKILL.md
files:
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/postgres/options.py
tests:
  - src/py/tests/unit/db/test_postgres_options.py
estimated_loc: 245
plan_revision: 8
plan_commit: 5eb75b2a
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
updated_at: "2026-11-12T01:30:00Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native connection constructor kwargs, PG environment precedence, and sslmode validation
  - id: pg-config-py
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQLConfig fields, from_url, pool_key, derive_direct_config, and cloud dialer builders
  - id: adr-004
    resource: .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
    title: Cross-extension primitive option boundary without PyRef/Arc sharing
tags:
  - postgres
  - configuration
  - dsn
  - libpq
---

# Task t01: Bridge PostgreSQL URL and `libpq` Configuration into Native Options

## Objective & Scope Boundary

Create `src/py/vertebra/db/adapters/postgres/options.py` with class-organized `libpq` keyword and `urllib.parse` URI parsing (`ParsedPostgresDsn`, `PostgresConnectOptions`) and extend `src/py/vertebra/db/adapters/postgres/config.py` (`PostgreSQLConfig`) with `+`-preserving `from_url`, secret-safe resolved `pool_key()`, nullable TLS/timeout/keepalive overrides, `cloud_auth_options`, synchronous `memory://` and `sslmode="allow"` rejection, fixed `to_cloud_dialer_config` IP-type forwarding, strict `derive_direct_config()`, and `resolve_sync_options` / `resolve_async_options` emitting exact primitive `vertebra._postgres` kwargs [^npd-contracts] [^pg-config-py] [^adr-004]. Lexical SQL bind compilation belongs to `ch25:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Exact `SyncPostgresClient` / `SyncPostgresPool` constructor kwargs, `PG*` environment precedence, `sslmode="prefer"` adapter default (`sslmode="allow"` rejected), and static `iam_auth_options` rules [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/config.py` — Existing `PostgreSQLConfig` fields, `from_url`, `pool_key`, `derive_direct_config`, `build_session_gucs`, and `to_cloud_dialer_config` [^pg-config-py].
- [database-drivers-and-dialers.md](../../../../../knowledge/domains/data/database-drivers-and-dialers.md) — `DatabaseRegistry` pool caching, Cloud SQL (`:3307`), AlloyDB (`:5433`), and direct IAM dialer contracts.
- [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) — Prohibition on passing Rust `Arc` / `GcpTokenCache` objects across cdylib wheels [^adr-004].
- [SKILL.md](../../../../../../skills/python-framework/SKILL.md) — `msgspec.Struct` `gc=False` safety rules, domain-class organization, PEP 604 typing, and zero inline `#` comments.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/options.py` (structured domain classes with `@staticmethod`/`@classmethod`/private methods; zero standalone single-caller module-level `_...` helper functions):
  - `class PostgresConnectOptions(msgspec.Struct, frozen=True, kw_only=True):` (omit `gc=False` completely; store `session_gucs: tuple[tuple[str, str], ...] = ()`, `cloud_auth_options: tuple[tuple[str, str], ...] = ()`, and `iam_auth_options: tuple[tuple[str, str], ...] = ()`, converting those tuples to `dict[str, str]` only inside `to_native_client_kwargs` and `to_native_pool_kwargs` — never combine `gc=False` with `dict`/`Mapping` fields):
    - Holds all primitive native connection and pool fields (`user`, `database`, `host`, `port`, `password`, `unix_socket`, `sslmode`, `ca_pem`, `client_cert_pem`, `client_key_pem`, `server_name`, `channel_binding`, `connect_timeout`, `application_name`, `session_gucs`, `keepalives`, `keepalives_idle`, `keepalives_interval`, `keepalives_count`, `cloud_sql_instance`, `cloud_sql_ip_type`, `alloydb_instance_uri`, `alloydb_ip_type`, `enable_iam_auth`, `cloud_auth_options`, `iam_auth_options`, `pooler_mode`, `protocol_level_pooler_cache`, `statement_cache_size`, `min_size`, `max_size`, `acquire_timeout`, `reset_timeout`, `close_timeout`).
    - Custom `__repr__` redacts `password`, `client_key_pem`, `cloud_auth_options`, and `iam_auth_options` token/key secrets (`"***"`).
    - `def to_native_client_kwargs(self) -> dict[str, Any]`, `def to_native_pool_kwargs(self) -> dict[str, Any]`, and `def cache_identity(self) -> tuple[Any, ...]` (emitting SHA-256 digests for `password`, PEM bytes, and secret auth option values so tuples never carry plaintext secrets).
  - `def parse_postgres_dsn(dsn: str) -> ParsedPostgresDsn`:
    - Case-insensitively rejects `memory://` prefixes synchronously with `ImproperConfigurationError` matching `"memory://"`; rejects any other non-PostgreSQL URI scheme (outside `postgresql://` and `postgres://`) with `ImproperConfigurationError` / `ValueError`.
    - Parses `postgresql://` / `postgres://` URIs via `urllib.parse.urlsplit` with strict UTF-8 percent-decoding preserving literal `+` across authority, path, and query (never calling `parse_qsl`, which turns `+` into space; retains explicit blanks `key=`; last duplicate query option wins before typed overrides), and parses `libpq` keyword `key = 'val\'ue'` DSNs via a character state machine on `ParsedPostgresDsn` (`\'` and `\\` escapes; zero regex splitting or third-party parsers).
    - Recognizes `host`, `hostaddr`, `port`, `user`, `password`, `dbname`, `sslmode`, `sslrootcert`, `sslcert`, `sslkey`, `channel_binding`, `connect_timeout`, `application_name`, `options`, `keepalives`, `keepalives_idle`, `keepalives_interval`, `keepalives_count`; rejects unknown keys or multi-host comma lists with `NotSupportedError`.
    - Parses libpq `options` tokens supporting `-c key=val`, `--key=val`, `-ckey=val`, `\ `-escaped spaces inside values (`-c search_path=a,\ b`), and `\\` escapes; rejects unsupported flags and rejects reserved startup/wire GUCs (`user`, `database`, `options`, `replication`, and any `client_encoding` whose casefolded normalized name is not `utf8` / `utf-8`) with `ValueError` / `ImproperConfigurationError`.
- `src/py/vertebra/db/adapters/postgres/config.py` (`PostgreSQLConfig`):
  - Add nullable override fields (`None` defers to DSN): `sslmode: str | None = None`, `ca_pem: bytes | None = None`, `sslrootcert: str | None = None`, `client_cert_pem: bytes | None = None`, `sslcert: str | None = None`, `client_key_pem: bytes | None = None`, `sslkey: str | None = None`, `server_name: str | None = None`, `channel_binding: Literal["disable", "prefer", "require"] | None = None`, `connect_timeout: float | None = None`, `keepalives: bool | None = None`, `keepalives_idle: int | None = None`, `keepalives_interval: int | None = None`, `keepalives_count: int | None = None`, plus `acquire_timeout: float = 30.0`, `reset_timeout: float = 10.0`, `close_timeout: float = 30.0`, `share_pool: bool = True`, and `cloud_auth_options: Mapping[str, str] = msgspec.field(default_factory=dict)`, with secret-redacting `__repr__`.
  - Update `from_url(cls, url: str, **overrides: Any) -> Self` to delegate through `parse_postgres_dsn(url)` (`+`-preserving percent decoding, preserving DSN TLS/socket/keepalive/timeout/GUC settings alongside explicit `overrides`, and rejecting `memory://` / unsupported schemes / `sslmode="allow"`).
  - Update `pool_key(self) -> tuple[Any, ...]` so `DatabaseRegistry` cache keys match complete resolved connection, Unix socket, TLS (`sslmode`, `server_name`, `channel_binding`, SHA-256 digests of resolved `ca_pem`/`client_cert_pem`/`client_key_pem`), startup GUC, Cloud SQL / AlloyDB target + IP type, IAM (`enable_iam_auth`, hashed `cloud_auth_options` and `iam_auth_options`), credential (SHA-256 presence+digest of resolved `password`, never plaintext), keepalive/timeout, and preparation (`pooler_mode`, `protocol_level_pooler_cache`, `statement_cache_size`, pool bounds) identity.
  - Precedence resolved dynamically per bridge call (never at module import): typed override > DSN query/keyword > URI authority/path > `PG*` env / documented defaults (`user`: DSN > `PGUSER` > `getpass.getuser()`; `database`: DSN > `PGDATABASE` > resolved `user`; `password`: DSN > `PGPASSWORD` including `""` > `None`; `host`: DSN > `PGHOST` > `"localhost"`; `port`: DSN > `PGPORT` > `5432`; `sslmode`: typed > DSN > `PGSSLMODE` > `"disable"` on implicit Unix socket else `"prefer"`). Validate `sslmode` strictly against `{"disable", "prefer", "require", "verify-ca", "verify-full"}` — `sslmode="allow"` from DSN, typed field, or `PGSSLMODE` fails closed with `ValueError` matching `vertebra-postgres`.
  - Update `to_cloud_dialer_config(self)` to forward `ip_type=self.cloud_sql_ip_type` / `ip_type=self.alloydb_ip_type`; update `derive_direct_config(self) -> PostgreSQLConfig` to reject `pooler_mode in ("transaction", "statement")` without `direct_dsn` (`ImproperConfigurationError`) and preserve TLS/GUC/cloud/IAM/`cloud_auth_options` with `broker=None, share_pool=False`; add `resolve_sync_options(self, *, include_pool_options: bool = True) -> dict[str, Any]` and `resolve_async_options(...) -> dict[str, Any]` (lazy PEM reads, cert/key pair validation, reserved-GUC checks on `session_gucs`, zero network I/O, zero `resolve_iam_password_*` calls).

## Edge Cases & Error Semantics

- Reject NUL bytes anywhere in DSN/options, malformed `%` hex escapes (`%2G`), invalid UTF-8 percent sequences (`%FF`), unclosed single quotes or trailing `\` in keyword DSNs, invalid ports (`<= 0` or `> 65535`), non-positive timeouts, `sslcert` without `sslkey` (or vice versa), `sslmode="allow"`, `memory://` (any casing), unsupported URI schemes, reserved GUCs (`user`, `database`, `options`, `replication`, non-`UTF8` `client_encoding`), and Unix socket combined with Cloud SQL / AlloyDB or explicit TLS (`"require"` / `"verify-ca"` / `"verify-full"`).
- `host=/var/run/postgresql` with `port=5433` normalizes to `unix_socket="/var/run/postgresql/.s.PGSQL.5433"` and `sslmode="disable"` when `sslmode` is unset; `hostaddr=10.0.0.5` with `host=db.internal` dials `10.0.0.5` with `server_name="db.internal"`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_options.py` (using `@pytest.mark.parametrize` across DSN/escape/rejection matrices, zero `for` loops inside test functions).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| URI/`from_url` literal `+`, `%25`, IPv6, & `-ckey=val` / `\ `-escaped `options` | `PostgreSQLConfig.from_url("postgresql://u+1:p%25ss@[2001:db8::1]:5433/app+db?sslmode=require&sslmode=verify-ca&options=-cTimeZone%3DUTC%20-c%20search_path%3Da,\\ b")` | `user="u+1"`, `password="p%ss"`, `host="2001:db8::1"`, `port=5433`, `database="app+db"`, `sslmode="verify-ca"`, `session_gucs == {"TimeZone": "UTC", "search_path": "a, b"}` (plus base GUCs), `repr()` hides `"p%ss"`. |
| `memory://`, bad scheme, `sslmode="allow"`, & reserved GUCs | `"MEMORY://local"`, `"mysql://localhost/db"`, `sslmode="allow"` (DSN, field, and `PGSSLMODE=allow`), `options="-c replication=database"`, `session_gucs={"client_encoding": "SQL_ASCII"}` | `MEMORY://local` raises `ImproperConfigurationError(match="memory://")`; `mysql://` raises `ImproperConfigurationError`/`ValueError`; all `sslmode="allow"` paths raise `ValueError`; reserved GUCs rejected while `client_encoding="UTF8"` succeeds. |
| `libpq` keyword grammar, dynamic `PG*` env, & Unix/`hostaddr` split | `"host=db.local user='a\\'b' password='x\\\\y' dbname=prod"`; live `PGUSER`/`PGPASSWORD=""`/`PGDATABASE` mutations + `getuser` `KeyError`; `host=/var/run/postgresql&port=5434`; `host=pg.example.com&hostaddr=10.1.2.3&sslmode=verify-full` | Escaped keyword values preserved; `target_session_attrs=any` raises `NotSupportedError`; `getuser` failure raises `ImproperConfigurationError`; Unix socket sets `sslmode="disable"`; `hostaddr` preserves `server_name="pg.example.com"`. |
| `pool_key()` identity, Cloud/IAM forwarding, & `derive_direct_config` | Compare `pool_key()` across configs differing by password, `ca_pem`, `session_gucs`, `pooler_mode`, `cloud_sql_ip_type="PSC"`, and `iam_auth_options`; `derive_direct_config()` on `pooler_mode="transaction"` without `direct_dsn` | Every differing dimension produces a distinct `pool_key()` with zero plaintext password/key bytes inside `repr(cfg.pool_key())`; `to_cloud_dialer_config().ip_type == "PSC"`; missing `direct_dsn` raises `ImproperConfigurationError`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_options.py -q`
- Final Green + Lint + Typecheck gate:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_options.py src/py/tests/unit/db/test_postgres_driver.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/config.py src/py/vertebra/db/adapters/postgres/options.py src/py/tests/unit/db/test_postgres_options.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
  ```

## Acceptance Criteria

- [ ] `options.py` defines `PostgresConnectOptions(msgspec.Struct, frozen=True, kw_only=True)` (without `gc=False`, storing pair collections as `tuple[tuple[str, str], ...]`) and class-organized URI/`libpq` parsing supporting `+`-preserving UTF-8 percent decoding, `-c key=val`/`-ckey=val`/`\ ` `options`, reserved-GUC rejection, `memory://` `ImproperConfigurationError`, and `sslmode="allow"` `ValueError` (R2).
- [ ] `PostgreSQLConfig.{from_url, pool_key, resolve_sync_options, resolve_async_options, derive_direct_config, to_cloud_dialer_config}` preserve resolved connection/TLS/socket/GUC/IAM/credential/preparation identity without leaking plaintext secrets (R2).
- [ ] Commit message follows `feat(postgres): bridge native connection configuration` once verified.

## Notes & Discoveries

- Constructors (`PostgreSQLConfig(...)`, `PostgresConnectOptions(...)`) perform zero network I/O; PEM file reads execute lazily inside `resolve_sync_options` / `resolve_async_options` (and `pool_key()` hashes resolved PEM inputs without returning raw private-key material) [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native connection constructor kwargs, PG environment precedence, and sslmode validation.
[^pg-config-py]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQLConfig fields, from_url, pool_key, derive_direct_config, and cloud dialer builders.
[^adr-004]: `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — Cross-extension primitive option boundary without PyRef/Arc sharing.
