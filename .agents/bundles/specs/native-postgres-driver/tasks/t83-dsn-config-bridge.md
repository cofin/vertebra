---
type: Task
id: "native-postgres-driver:t83"
title: "Bridge PostgreSQL URL and libpq configuration into native options"
description: "Bridge PostgreSQL URL and libpq configuration into native options for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t82"
files:
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/postgres/options.py
  - src/py/tests/unit/db/test_postgres_options.py
tests:
  - src/py/tests/unit/db/test_postgres_options.py
plan_revision: 2
plan_commit: 9af810f5
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
updated_at: "2026-11-07T04:15:00Z"
commit: null
tags:
  - postgres
  - configuration
  - native
---

# Bridge PostgreSQL URL and libpq configuration into native options

## Objective

Preserve supported PostgreSQL connection configuration at the native kwargs boundary without a Python database client dependency.

## Context

Read `../spec.md` and the frozen complete public contracts integrated there, foundation connection/cloud material worksheets and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/python-framework/SKILL.md`. The current PostgreSQLConfig.from_url parses a few application overrides; there is no full shared DSN parser to call. This parser and its tests precede sync cutover as their own bounded invocation. One invocation makes one signed conventional local commit on the complete driver's feature branch, one PR only after all acceptance. No production work starts while plan_commit is null. Own only listed files and preserve unrelated changes.

## Steps

1. RED table-driven tests cover percent-encoded URL credentials/database, IPv4/IPv6, Unix socket URL query host, empty/default user/database policy, keyword `key=value` DSNs with whitespace, single-quoted values, backslash escapes, escaped quotes and malformed/unclosed/dangling input. Include secret redaction and unsupported key/error type. Do not use regex splitting or third-party psycopg/asyncpg DSN parser. Use urllib.parse for URLs and a bounded explicit state machine for libpq keyword grammar. Percent-decode UTF8 strictly, reject malformed percent escapes/invalid UTF8, and preserve literal + in URI authority/path/query (libpq URI percent decoding is not form-urlencoding). Query splitting retains explicit blanks and last duplicate option wins before typed overrides. Test literal+/encoded+/%25/%FF and duplicate options without logging secrets.
2. Add adapter-owned frozen `PostgresConnectOptions` msgspec value object and one parser/normalizer in options.py. Recognize exactly supported host/hostaddr/port/user/password/dbname, sslmode/sslrootcert/sslcert/sslkey, connect_timeout, application_name, options, keepalives/keepalives_idle/keepalives_interval/keepalives_count; preserve valid cloud configuration. Reject unknown or unsupported keys with NotSupportedError, never drop them. Reject NUL, invalid numeric/time/port bounds and conflicting impossible endpoint choices. Convert DSN Unix host directory into exact .s.PGSQL.<port> socket path, excluding cloud; choose disable for implicit Unix TLS default and reject explicitly requested incompatible TLS. Test unsupported-platform errors and IPv6 zone/percent encoding. For hostaddr, retain host for TLS verification/server_name while dialing explicit address. Do not add multi-host failover silently: if not covered by native endpoint contract, fail clearly.
3. Add typed nullable PostgreSQLConfig overrides for TLS material/path, channel_binding, timeout and keepalive options with None defaults plus cloud_auth_options mapping for Cloud Admin credentials; existing iam_auth_options is designated login credentials. Precedence: explicit non-None typed config field > URL query or keyword setting > URI authority/path > native documented defaults. Preserve existing application_name default `vertebra` and its historical config override of DSN application_name; document this exception to newly nullable override fields and test it. Only new TLS/timeouts/keepalive fields use None to defer to DSN. Resolve defaults at each bridge call with exact precedence: explicit DSN user (URI/query/keyword) > nonempty PGUSER > getpass.getuser(); explicit dbname > nonempty PGDATABASE > resolved user. Existing default URI path /postgres already supplies explicit database; an empty URI path is absent, but explicitly empty user/dbname raises ValueError. Password explicit > PGPASSWORD when set (including empty) > None; host explicit > nonempty PGHOST > localhost; port explicit > nonempty PGPORT >5432. Non-None supported typed config fields override applicable DSN settings. OS username lookup failure raises ImproperConfigurationError. Preserve the established high-level adapter/libpq sslmode default prefer when neither typed config, DSN nor nonempty PGSSLMODE supplies one; raw native client default verify-full is unchanged. Unix implicit TLS default becomes disable. Test these defaults and explicit override precedence. Source: https://www.postgresql.org/docs/18/libpq-connect.html#LIBPQ-CONNECT-SSLMODE. Read environment only at bridge invocation, never import time; do not read unrelated credential files. Add table tests for each precedence edge, environment mutation across calls and empty/failed fallback. File options load configured CA/cert/key bytes during lazy connect preparation, validate key/cert pairing and redact file/secret errors; constructors remain no-I/O.
4. Merge startup options/GUCs without silently dropping caller settings. Parse supported `options` `-c key=value` grammar and merge session_gucs plus typed timeout/application fields under documented precedence. Preserve DateStyle/IntervalStyle/TimeZone startup values and pass ParameterStatus to decoder contracts rather than force defaults that contradict caller settings. Unsupported libpq option syntax fails clearly. Route Cloud SQL/AlloyDB/IAM through foundation's actual automatic material/token path, never prepared-only stubs. Options contain owned secrets and do not print them in repr. Fix to_cloud_dialer_config dropping cloud_sql_ip_type/alloydb_ip_type by forwarding each exact IP choice to its corresponding config. Forward primitive auth_options to inside-postgres scope-keyed token caches (Admin cloud-platform, login sqlservice.login/alloydb.login), preserving NativeCloudConnectorConfig URI/IpType/authmode/API endpoint/refreshlead240s from completed automatic cloud-material and scoped-authentication worksheets. Never extract a core PyGcpTokenCache Arc across cdylib. Cloud Admin uses new PostgreSQLConfig.cloud_auth_options; login uses existing iam_auth_options. Missing login options derive the same credential source with required login scopes. Static login tokens are designated only via iam_auth_options; opaque Admin tokens never imply login grants. Direct enable_iam_auth without cloud resource stays supported: fresh token password with default sqlservice.login scope, explicit iam_auth_options scopes authoritative (including alloydb.login), DSN password superseded and configured TLS unchanged. Test expiry/refresh, rejected grants and failed auth without same-stream retry.
5. Add PostgreSQLConfig.resolve_sync_options/resolve_async_options sharing pure parse/merge and explicit lazy PEM file resolution; both pass primitive native IAM/cloud maps rather than resolving Python-cache tokens. Native connection tasks own async HTTP/material/token refresh and its deadline; constructors/config parsing perform no cloud network I/O. Return exact native primitive kwargs from frozen public contracts: user/database/host/port/password/unix_socket/sslmode/PEM/server_name/channel_binding/connect_timeout/application_name/session_gucs/keepalive options/cloud resource+IP/enable_iam_auth/cloud_auth_options/iam_auth_options; no raw DSN passed to client/pool. Test exact direct/cloud option maps, IAM scope precedence and no calls to old resolve_iam_password_sync/async or another extension token cache. Native connection fixtures test actual refresh; config tests make no live cloud calls. Commit feat(postgres): bridge native connection configuration.

## Verification

Write named observable RED tables before behavior. Estimated changed lines including tests350–400. If grammar/material integration exceeds this budget, `/flow:revise native-postgres-driver` splits this configuration boundary before implementation, retaining full one-PR scope. Planned commands are not executed evidence:

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_options.py -q
uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/config.py src/py/vertebra/db/adapters/postgres/options.py src/py/tests/unit/db/test_postgres_options.py
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
```

## Acceptance Criteria

- [ ] R2: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] URL/keyword grammar and precedence preserve supported settings, reject unknown keys explicitly and keep secrets private.
- [ ] Sync/async native pool kwargs use one real configuration bridge without Python PostgreSQL clients, live cloud writes or duplicate parsing.
- [ ] Tests pass; one signed local commit and task-first checklist/state are synchronized.

## Notes & Discoveries

None.
