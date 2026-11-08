---
type: Task
id: "native-postgres-driver:ch25:t02"
chapter_id: ch25-dsn-bridge-lexer-and-permanent-drivers
title: Lower PostgreSQL legacy binds with a lexer-aware adapter compiler
description: Implement a single-pass PostgreSQL lexical bind compiler in binds.py and expose PostgreSQLCore.prepare_native_query in core.py.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch25:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/py/vertebra/db/adapters/postgres/core.py
  - src/py/vertebra/db/adapters/postgres/types.py
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/skills/python-framework/SKILL.md
files:
  - src/py/vertebra/db/adapters/postgres/binds.py
  - src/py/vertebra/db/adapters/postgres/core.py
tests:
  - src/py/tests/unit/db/test_postgres_binds.py
estimated_loc: 235
plan_revision: 4
plan_commit: e8901de4
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
commit: null
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Lexical pyformat lowering, numeric passthrough, backslash-quote rejection, and PgTemporal wire_text refusal
  - id: pg-core-py
    resource: src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore
    title: Shared PostgreSQLCore adapter seam
  - id: cdc-domain
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: Raw PostgreSQL operator and parameter style boundaries
tags:
  - postgres
  - lexer
  - parameters
  - binds
---

# Task t02: Lower PostgreSQL Legacy Binds with a Lexer-Aware Adapter Compiler

## Objective & Scope Boundary

Create `src/py/vertebra/db/adapters/postgres/binds.py` implementing a bounded single-pass SQL lexical compiler that lowers raw `%s` and `%(name)s` binds to `$1..$N` while passing native `$N` SQL through byte-for-byte unchanged, and expose `PostgreSQLCore.prepare_native_query(sql, parameters)` in `src/py/vertebra/db/adapters/postgres/core.py` as the shared query and pipeline bind seam [^npd-contracts] [^pg-core-py] [^cdc-domain]. SQLSpec `?` / `:name` and SQLAlchemy `:name` prepared-statement compilation boundaries belong to `ch26:t01`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Numeric passthrough vs pyformat emulation rules, ordinary-string backslash-quote rejection, `foo$1` identifier boundaries, and unresolved `PgTemporal(special="wire_text")` preflight `DataError` [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/core.py` — `PostgreSQLCore` static helper module [^pg-core-py].
- `src/py/vertebra/db/adapters/postgres/types.py` — `PostgresValue`, `PgParameter`, `PgTemporal`, and `PgUnchangedToast` validation contracts.
- [query-pipeline-cdc-and-tenancy.md](../../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) — Raw adapter vs SQLSpec compiler boundaries [^cdc-domain].
- [SKILL.md](../../../../../../skills/python-framework/SKILL.md) — Python typing, `msgspec`, and zero inline `#` comment rules.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/binds.py`:
  - `DEFAULT_BIND_CACHE_SIZE: Final[int] = 512` (bounded LRU cache storing compiled `(rewritten_sql, style, ordered_names_or_arity)` keyed strictly by `(sql, is_mapping)` — never caching parameter values or secrets).
  - `def compile_postgres_binds(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> tuple[str, tuple[Any, ...]]`:
    - Single-pass linear `O(len(sql))` lexical scanner tracking:
      1. Normal SQL
      2. Ordinary single-quoted strings (`'...'` with `''` doubled escapes)
      3. Explicit escape strings (`E'...'` / `e'...'` with `\'` and `\\` escapes)
      4. Double-quoted identifiers (`"..."` with `""` doubled escapes)
      5. Dollar-quoted strings (`$$...$$` and `$tag$...$tag$` where `tag` is a valid PostgreSQL identifier/Unicode tag)
      6. Line comments (`--` to `\n`)
      7. Nested block comments (`/* ... /* ... */ ... */`)
    - **Numeric-only passthrough** (when normal SQL contains zero `%s`, `%(...)s`, or `%%` pyformat escapes and `parameters` is `None` or an ordered non-`str`/`bytes`/`bytearray` `Sequence`):
      - Returns `sql` byte-for-byte unchanged (never rewriting `$0`, `$2, $1`, ordinary backslash-quote literals, `::` casts, or `?`/`?|`/`?&` JSONB operators — PostgreSQL owns SQL syntax and `$N` index coverage) and returns `tuple(parameters)` (`()` when `None`).
      - Passing a `Mapping` when zero `%(name)s` placeholders exist in normal SQL raises `ProgrammingError` before I/O (when empty `{}` and zero placeholders, returns `(sql, ())`; non-empty `Mapping` without `%(name)s` raises `ProgrammingError`, never binding `mapping.values()`).
    - **Pyformat emulation mode** (when normal SQL contains `%s`, `%(name)s`, or `%%`):
      - Rewrites `%s` -> `$1, $2, ...` consuming an ordered `Sequence` of exact matching length; rewrites `%(name)s` -> `$k` assigning `k` by first SQL appearance and reusing `$k` for repeated `%(name)s` occurrences, requiring exact key set match against `Mapping` (missing or extra keys raise `ProgrammingError` before I/O).
      - Rewrites `%%` -> `%` in normal SQL while preserving binary/spacing modulo `%` (`total % 10`) when not followed by `s` or `(`.
      - Preserves literal `%s`, `%(name)s`, `$1`, `:name`, `::` casts, and `?` / `?|` / `?&` JSONB operators inside all quoted, dollar-quoted, and comment states (and raw `?`/`::` in normal SQL) byte-for-byte.
      - Rejects mixed `%s` + `%(name)s`, mixed pyformat + native `$N` at an identifier boundary (whereas `foo$1` and Unicode `café$1` are valid identifiers, NOT native `$N` binds!), malformed `%` specifiers (`%d`, `%(unclosed`), unterminated quotes/dollar-quotes/block comments, and ambiguous ordinary single-quoted backslash-quote forms (`'a\'b'`) with `ProgrammingError` before any wire I/O (requiring explicit `E'a\'b'` regardless of `standard_conforming_strings` or pipeline operation index).
- `src/py/vertebra/db/adapters/postgres/core.py`:
  - `PostgreSQLCore.prepare_native_query(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> tuple[str, tuple[PostgresValue | PgParameter, ...]]` — runs `compile_postgres_binds`, validates/copies mutable JSON/buffer inputs, rejects `PgUnchangedToast` (`TypeError`/`DataError`) and unresolved `PgTemporal(special="wire_text")` (or `PgParameter` wrapping it) with canonical `DataError` before submission, and returns `(native_sql, adapted_tuple)`.

## Edge Cases & Error Semantics

- Never use `re.sub`, `sql.split`, or string interpolation of parameter values into SQL text.
- `parameters = "abc"` or `b"abc"` passed as a sequence is rejected as `TypeError` / `ProgrammingError` rather than binding `'a'`, `'b'`, `'c'` character-by-character.
- Adversarial inputs (`100_000`-byte nested comments `/* /* ... */ */`, near-miss dollar tags `$tag1$ ... $tag2$ ... $tag1$`, and `1_000` unique queries) execute in strictly linear single-pass time and evict cleanly at `DEFAULT_BIND_CACHE_SIZE = 512`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_binds.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Positional `%s` & repeated named `%(name)s` | `prepare_native_query("SELECT %(b)s, %(a)s, %(b)s WHERE doc ? 'k' AND x::int = %(a)s", {"a": 10, "b": Decimal("1.50")})` | Returns `("SELECT $1, $2, $1 WHERE doc ? 'k' AND x::int = $2", (Decimal("1.50"), 10))` preserving `?` and `::int`. |
| Quotes, `E'...'`, dollar-quotes, nested comments, & `foo$1` | `"SELECT '%s', E'it\\'s %(x)s', $fn$ BEGIN PERFORM %s; END $fn$, /* outer /* %s */ */ foo$1, café$1, %s"`, `(99,)` | Returns exact SQL with only the trailing normal-SQL `%s` rewritten to `$1` and `(99,)`; `foo$1` and `café$1` are not flagged as mixed `$1` binds. |
| Numeric-only passthrough invariance | `prepare_native_query("SELECT $2, $1, $0, 'raw\\'quote', payload ?| array['a']", ("x", "y"))` | SQL returned byte-for-byte unchanged with `("x", "y")`; zero client-side `$0` or backslash-quote errors invented. |
| Emulation backslash-quote & mixed-style rejections | `"SELECT 'ambiguous\\'quote', %s"` with `(1,)`; `"SELECT $1, %s"` with `(1, 2)`; `"SELECT %(a)s"` with `{"a": 1, "extra": 2}` | Each raises `ProgrammingError` before any network I/O (in both standalone and pipeline batch positions). |
| Unresolved `PgTemporal(special="wire_text")` & `PgUnchangedToast` refusal | Pass `PgTemporal(kind="timestamptz", value=None, special="wire_text", wire_text="...", date_style="ISO", time_zone="EST")` or `PG_UNCHANGED_TOAST` | Raises `DataError` (or `TypeError`/`DataError` for TOAST) before wire submission. |
| Linear adversarial scan & bounded LRU cache | `64 KiB` nested comment / mismatched `$tag$` SQL + `600` distinct compiled templates | Completes in single pass; cache length stays `<= 512` and stores zero parameter objects. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_binds.py -q`
- Final Green + Lint + Typecheck gate:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_binds.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/binds.py src/py/vertebra/db/adapters/postgres/core.py src/py/tests/unit/db/test_postgres_binds.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
  ```

## Acceptance Criteria

- [ ] `binds.py` implements a single-pass lexical state machine covering single quotes, `E'...'` strings, `""` identifiers, `$$`/`$tag$` dollar quotes, and `--`/nested `/*...*/` comments with a bounded `512`-entry metadata cache (R2, R24).
- [ ] `PostgreSQLCore.prepare_native_query` preserves numeric-only SQL unchanged, lowers `%s` and `%(name)s` to `$N`, preserves `?`/`?|`/`?&`, `::`, and `foo$1`, and rejects ambiguous ordinary backslash-quotes, extra/missing binds, and `wire_text` temporals before I/O (R2, R24).
- [ ] Commit message follows `feat(postgres): lower legacy binds with lexical metadata` once verified.

## Notes & Discoveries

- Raw `?` in `PostgreSQLCore.prepare_native_query` is always preserved as a PostgreSQL JSONB existence operator (`?`, `?|`, `?&`); explicit question-mark SQLSpec statements compile through `ch26:t01` into numeric `$N` SQL before entering this seam [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Lexical pyformat lowering, numeric passthrough, backslash-quote rejection, and PgTemporal wire_text refusal.
[^pg-core-py]: `src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore` — Shared PostgreSQLCore adapter seam.
[^cdc-domain]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Raw PostgreSQL operator and parameter style boundaries.
