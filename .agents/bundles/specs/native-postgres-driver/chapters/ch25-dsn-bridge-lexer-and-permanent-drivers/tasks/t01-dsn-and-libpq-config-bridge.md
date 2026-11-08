---
type: Task
id: "native-postgres-driver:ch25:t01"
chapter_id: ch25-dsn-bridge-lexer-and-permanent-drivers
title: Bridge PostgreSQL URL and libpq configuration into native options
description: Implement pure-Python URI and libpq keyword DSN parsing in options.py and wire PostgreSQLConfig.resolve_sync_options, resolve_async_options, and derive_direct_config in config.py.
status: stable
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
plan_revision: 7
plan_commit: 1c75af65
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
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native connection constructor kwargs, PG environment precedence, and sslmode defaults
  - id: pg-config-py
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQLConfig fields, derive_direct_config, and cloud dialer builders
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

Create `src/py/vertebra/db/adapters/postgres/options.py` with a bounded state-machine `libpq` keyword and `urllib.parse` URI normalizer (`PostgresConnectOptions`) and extend `src/py/vertebra/db/adapters/postgres/config.py` (`PostgreSQLConfig`) with nullable TLS/timeout/keepalive overrides, `cloud_auth_options`, fixed `to_cloud_dialer_config` IP-type forwarding, strict `derive_direct_config()` pooler validation, and `resolve_sync_options` / `resolve_async_options` emitting exact primitive `vertebra._postgres` kwargs [^npd-contracts] [^pg-config-py] [^adr-004]. Lexical SQL bind compilation belongs to `ch25:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Exact `SyncPostgresClient` / `SyncPostgresPool` constructor kwargs, `PG*` environment precedence, `sslmode="prefer"` adapter default, and static `iam_auth_options` rules [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/config.py` — Existing `PostgreSQLConfig` fields, `derive_direct_config`, `build_session_gucs`, and `to_cloud_dialer_config` [^pg-config-py].
- [database-drivers-and-dialers.md](../../../../../knowledge/domains/data/database-drivers-and-dialers.md) — Cloud SQL (`:3307`), AlloyDB (`:5433`), and direct IAM dialer contracts.
- [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) — Prohibition on passing Rust `Arc` / `GcpTokenCache` objects across cdylib wheels [^adr-004].
- [SKILL.md](../../../../../../skills/python-framework/SKILL.md) — `msgspec.Struct` conventions, PEP 604 typing, and zero inline `#` comments.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/options.py`:
  - `class PostgresConnectOptions(msgspec.Struct, frozen=True, gc=False, kw_only=True):`
    - Holds all primitive native connection + pool fields (`user`, `database`, `host`, `port`, `password`, `unix_socket`, `sslmode`, `ca_pem`, `client_cert_pem`, `client_key_pem`, `server_name`, `channel_binding`, `connect_timeout`, `application_name`, `session_gucs`, `keepalives`, `keepalives_idle`, `keepalives_interval`, `keepalives_count`, `cloud_sql_instance`, `cloud_sql_ip_type`, `alloydb_instance_uri`, `alloydb_ip_type`, `enable_iam_auth`, `cloud_auth_options`, `iam_auth_options`, `pooler_mode`, `protocol_level_pooler_cache`, `statement_cache_size`, `min_size`, `max_size`, `acquire_timeout`, `reset_timeout`, `close_timeout`).
    - Custom `__repr__` redacts `password`, `client_key_pem`, `cloud_auth_options`, and `iam_auth_options` token/key secrets (`"***"`).
    - `def to_native_client_kwargs(self) -> dict[str, Any]` and `def to_native_pool_kwargs(self) -> dict[str, Any]`.
  - `def parse_postgres_dsn(dsn: str) -> ParsedPostgresDsn`:
    - Supports `postgresql://` / `postgres://` URIs via `urllib.parse` with strict UTF-8 percent-decoding (preserving literal `+` in authority, path, and query; retaining explicit blanks `key=`; last duplicate query option wins before typed overrides) and `libpq` keyword `key = 'val\'ue'` DSNs via an explicit character state machine (whitespace, single-quoted strings, `\'` / `\\` escapes; zero regex splitting or third-party DSN parsers).
    - Recognizes `host`, `hostaddr`, `port`, `user`, `password`, `dbname`, `sslmode`, `sslrootcert`, `sslcert`, `sslkey`, `channel_binding`, `connect_timeout`, `application_name`, `options` (`-c key=value` grammar), `keepalives`, `keepalives_idle`, `keepalives_interval`, `keepalives_count`; rejects any unknown key or multi-host comma list with `NotSupportedError`.
- `src/py/vertebra/db/adapters/postgres/config.py` (`PostgreSQLConfig`):
  - Add nullable override fields (`None` defers to DSN): `sslmode: str | None = None`, `ca_pem: bytes | None = None`, `sslrootcert: str | None = None`, `client_cert_pem: bytes | None = None`, `sslcert: str | None = None`, `client_key_pem: bytes | None = None`, `sslkey: str | None = None`, `server_name: str | None = None`, `channel_binding: Literal["disable", "prefer", "require"] | None = None`, `connect_timeout: float | None = None`, `keepalives: bool | None = None`, `keepalives_idle: int | None = None`, `keepalives_interval: int | None = None`, `keepalives_count: int | None = None`, plus `acquire_timeout: float = 30.0`, `reset_timeout: float = 10.0`, `close_timeout: float = 30.0`, `share_pool: bool = True`, and `cloud_auth_options: Mapping[str, str] = msgspec.field(default_factory=dict)`.
  - Precedence resolved dynamically at each bridge call (never at module import):
    - Explicit non-`None` typed field > DSN query/keyword > URI authority/path > `PG*` env / documented defaults (`application_name` preserves historical `PostgreSQLConfig.application_name` authority over DSN `application_name`).
    - `user`: explicit DSN > nonempty `PGUSER` > `getpass.getuser()` (`OSError`/`KeyError` -> `ImproperConfigurationError`; explicit `""` -> `ValueError`).
    - `database`: explicit DSN (`/postgres` supplies `"postgres"`; empty `/` path is absent; explicit `dbname=""` -> `ValueError`) > nonempty `PGDATABASE` > resolved `user`.
    - `password`: explicit DSN > `PGPASSWORD` when present in `os.environ` (including `""`) > `None` (superseded when direct `enable_iam_auth=True`).
    - `host`: explicit DSN > nonempty `PGHOST` > `"localhost"`; `port`: explicit DSN > nonempty `PGPORT` > `5432`.
    - `sslmode`: typed `sslmode` > DSN `sslmode` > nonempty `PGSSLMODE` > `"disable"` (for implicit Unix socket) or `"prefer"` (established `libpq` adapter default).
  - Update `to_cloud_dialer_config(self)` to forward `ip_type=self.cloud_sql_ip_type` / `ip_type=self.alloydb_ip_type`.
  - Update `derive_direct_config(self) -> PostgreSQLConfig`: reject `pooler_mode in ("transaction", "statement")` (or `pgbouncer`/`pgbouncer_mode`) when `direct_dsn is None` (`ImproperConfigurationError`); return a direct/session `PostgreSQLConfig` with `broker=None`, `share_pool=False`, and all TLS/GUC/cloud/IAM/`cloud_auth_options` fields preserved.
  - Add `resolve_sync_options(self, *, include_pool_options: bool = True) -> dict[str, Any]` and `resolve_async_options(self, *, include_pool_options: bool = True) -> dict[str, Any]` performing pure merge + lazy PEM file reads (validating cert/key pairing and redacting secrets on file error; zero network I/O, zero calls to `resolve_iam_password_sync`/`async`).

## Edge Cases & Error Semantics

- Reject NUL bytes anywhere in DSN/options, malformed `%` hex escapes (`%2G`), invalid UTF-8 percent sequences (`%FF`), unclosed single quotes or trailing `\` in keyword DSNs, invalid ports (`<= 0` or `> 65535`), non-positive `connect_timeout`, `sslcert` without `sslkey` (or vice versa), and Unix socket combined with Cloud SQL / AlloyDB or explicit TLS (`sslmode="require"` / `"verify-ca"` / `"verify-full"`).
- `host=/var/run/postgresql` with `port=5433` normalizes to `unix_socket="/var/run/postgresql/.s.PGSQL.5433"`, `sslmode="disable"` when `sslmode` was not explicitly requested; `hostaddr=10.0.0.5` with `host=db.internal` dials `10.0.0.5` while setting `server_name="db.internal"`.
- Merge `-c key=value` entries from DSN `options` with `session_gucs` without dropping caller `DateStyle`, `IntervalStyle`, or `TimeZone`; reject unsupported unescaped `options` flags clearly.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_options.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| URI literal `+`, `%25`, IPv6, & duplicate query keys | `"postgresql://u+1:p%25ss@[2001:db8::1]:5433/app+db?sslmode=require&sslmode=verify-ca&options=-c%20TimeZone%3DUTC"` | `user="u+1"`, `password="p%25ss"` -> `"p%ss"`, `host="2001:db8::1"`, `port=5433`, `database="app+db"`, `sslmode="verify-ca"`, `session_gucs["TimeZone"] == "UTC"`, `repr()` masks `"p%ss"`. |
| `libpq` keyword grammar & negative cases | `"host=db.local user='a\\'b' password='x\\\\y' dbname=prod"` vs unclosed `"user='abc"`, `%FF`, or unknown `"target_session_attrs=any"` | Valid keyword DSN yields `user="a'b"`, `password="x\\y"`; unclosed quote and `%FF` raise `ValueError`; unknown key raises `NotSupportedError`. |
| Dynamic `PG*` env precedence & OS user failure | Mutate `PGUSER`, `PGDATABASE`, `PGPASSWORD=""`, `PGHOST`, `PGPORT`, `PGSSLMODE` across calls with `dsn="postgresql:///?"`; mock `getpass.getuser` raising `KeyError` | Resolves live env per call (`password=""` when `PGPASSWORD=""`, `database == resolved_user` when `PGDATABASE` unset, `sslmode="prefer"` when `PGSSLMODE` unset); `getuser` failure raises `ImproperConfigurationError`; explicit `user=""` raises `ValueError`. |
| Unix socket normalization & `hostaddr` TLS split | `dsn="postgresql:///app?host=/var/run/postgresql&port=5434"` and `dsn="postgresql://pg.example.com/app?hostaddr=10.1.2.3&sslmode=verify-full"` | First yields `unix_socket="/var/run/postgresql/.s.PGSQL.5434", sslmode="disable"`; explicit `&sslmode=require` on Unix raises `ImproperConfigurationError`/`ValueError`; second dials `host="10.1.2.3"` with `server_name="pg.example.com"`. |
| Cloud/IAM primitive forwarding & `derive_direct_config` | `PostgreSQLConfig(cloud_sql_instance="p:r:i", cloud_sql_ip_type="PSC", enable_iam_auth=True, cloud_auth_options={"scopes": "..."}, iam_auth_options={"access_token": "tok", "expires_at_unix_ms": "1999999999000", "scopes": "..."})`; plus `PostgreSQLConfig(pooler_mode="transaction")` without `direct_dsn` | `to_cloud_dialer_config().ip_type == "PSC"`; `resolve_async_options()` passes primitive dicts without calling `resolve_iam_password_*`; `derive_direct_config()` on transaction pooler without `direct_dsn` raises `ImproperConfigurationError`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_options.py -q`
- Final Green + Lint + Typecheck gate:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_options.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/config.py src/py/vertebra/db/adapters/postgres/options.py src/py/tests/unit/db/test_postgres_options.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
  ```

## Acceptance Criteria

- [ ] `options.py` parses `postgresql://` URIs (preserving literal `+` and strict UTF-8 percent-decoding) and `libpq` keyword DSNs without regex splitting or third-party PostgreSQL client dependencies, rejecting unknown keys with `NotSupportedError` and redacting secrets (R2).
- [ ] `PostgreSQLConfig.{resolve_sync_options, resolve_async_options, derive_direct_config, to_cloud_dialer_config}` enforce documented precedence, lazy PEM loading, pooler direct-DSN validation, and primitive cloud/IAM kwargs forwarding (R2).
- [ ] Commit message follows `feat(postgres): bridge native connection configuration` once verified.

## Notes & Discoveries

- Constructors (`PostgreSQLConfig(...)`, `PostgresConnectOptions(...)`) perform zero filesystem or network I/O; PEM file reads execute lazily inside `resolve_sync_options` / `resolve_async_options` [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native connection constructor kwargs, PG environment precedence, and sslmode defaults.
[^pg-config-py]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQLConfig fields, derive_direct_config, and cloud dialer builders.
[^adr-004]: `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — Cross-extension primitive option boundary without PyRef/Arc sharing.
