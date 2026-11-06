---
type: Task
id: native-postgres-driver:t84
title: Lower PostgreSQL legacy binds with a lexer-aware adapter compiler
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t83
files:
- src/py/vertebra/db/adapters/postgres/binds.py
- src/py/vertebra/db/adapters/postgres/core.py
- src/py/tests/unit/db/test_postgres_binds.py
tests:
- src/py/tests/unit/db/test_postgres_binds.py
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-06T06:30:00Z'
commit: null
tags:
- postgres
- parameters
- native
requirements:
- R2
- R24
---

# Lower PostgreSQL legacy binds with a lexer-aware adapter compiler

## Objective

Preserve raw PostgreSQL positional `%s` and named `%(name)s` calls while passing only `$n` SQL and ordered values into the native wire client.

## Context

Read `../spec.md` and the frozen complete public contracts integrated there, types/query/pipeline predecessor public contracts and `.agents/skills/python-framework/SKILL.md`. Preserve raw named Mapping and positional format behavior; SQLSpec remains optional for raw driver execution. This bounded compiler is adapter-owned, not a regex replacement or a second SQL statement engine. All work remains on the complete native driver's one feature branch; one invocation/one signed conventional local commit. No implementation while plan_commit is null.

## Steps

1. Write RED table-driven tests before implementation. Cover native `$1/$2` sequences including repeated/out-of-order indexes, positional `%s`, repeated named `%(name)s` using stable first-occurrence index, mapping insertion order differing from SQL order, NULL/Decimal/bytes/custom typed values and empty/no parameters. Assert exact SQL plus ordered values. In pyformat emulation, missing/extra binds, mixed native/format/named styles, sequence-vs-mapping mismatch and malformed placeholders raise typed argument errors before any I/O. Numeric-only SQL stays byte-for-byte unchanged, including invalid $0, out-of-range/missing/extra references and ordinary backslash-quote forms under either standard_conforming_strings setting; its SQL syntax and parameter coverage are server-owned. Unit tests assert passthrough rather than inventing pre-I/O numeric errors. Ordered-sequence shape and protocol resource limits remain client-validated.
2. Implement one bounded single-pass lexical compiler in binds.py with explicit states for normal SQL, single quotes with doubled escapes, E/e escape strings, doubled double-quoted identifiers, dollar-quoted strings (empty/tagged delimiter), line comments and nested block comments. Detect whether raw input actually needs pyformat emulation; no `%s`/`%(name)s` means pass native SQL through without quote/backslash emulation. In emulation mode recognize placeholders only in normal SQL and reject ordinary-string backslash-quote forms whose interpretation depends on standard_conforming_strings before any write, requiring explicit E strings. Valid E strings have explicit escape semantics. Apply this rejection to first/later pipeline operations identically; ParameterStatus is observed but arbitrary earlier server functions can change GUCs and are never guessed by scanning SQL. Add both standard_conforming_strings settings and first/later batch negative tests. Preserve literal `%s`, `%(name)s`, `$1` and `?` inside all quoted/comment states byte-for-byte. Preserve modulo `%`, format literal `%%` semantics consistent with prior pyformat contract, casts and dollar text that is not a valid delimiter/bind. Test escaped backslashes/quotes, nested comments and adjacent punctuation. Never use regex global substitution, sql.split or dictionary values order.
3. During pyformat emulation, recognize native `$n` for mixed-style rejection only at a real identifier boundary: `foo$1`, including Unicode identifier bytes, is an identifier and not a bind; add exact no-rewrite regressions. Numeric-only SQL requires an ordered sequence and passes it unchanged without scanning, renumbering indexes or validating SQL-dependent parameter coverage; the server owns those errors. Emulation never treats an identifier suffix as a native bind. `%s` consumes sequence in occurrence order; named Mapping assigns an index by SQL first appearance and reuses it for repeated name, preserving each value object without stringification. Reject extra mapping keys rather than silently dropping them. No parameter values interpolate into SQL. Unknown SQL syntax without emulation is server-owned passthrough. Emulated unterminated quotes/comments/dollar quote fail before wire submission; ambiguous ordinary backslash-quote is rejected rather than guessed from current GUC. Compile metadata may use a bounded cache keyed only by SQL/style, never retaining bind values/secrets.
4. Expose PostgreSQLCore.prepare_native_query(sql, parameters)->tuple[str,tuple[PostgresValue|PgParameter,...]] as one shared driver/pipeline seam using this compiler and exact predecessor value adaptation. `None` becomes empty tuple. Raw `?` is always preserved as PostgreSQL JSON operator; this compiler cannot infer question-mark placeholders. SQLSpec explicit question-mark statements lower from existing prepared SQL metadata through its own compiler task, then enter this seam as numeric. No SQLSpec runtime import/dependency for ordinary raw numeric/format operations.
5. Add algorithmic long-input tests proving linear progress on adversarial quote/comment/dollar-tag patterns and bounded cache capacity without timing flakiness; assert output bytes/parameter metadata and cache eviction, not private helper call counts. Commit feat(postgres): lower legacy binds with lexical metadata.

## Verification

Estimated changed handwritten lines including tests320–400. If the complete lexical state and negative cases do not fit, revise worksheet boundaries before implementation; never omit a quoted SQL form. Record actual RED then final command exits. Commands are planned:

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_binds.py -q
uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/binds.py src/py/vertebra/db/adapters/postgres/core.py src/py/tests/unit/db/test_postgres_binds.py
uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
```

## Acceptance Criteria

- [ ] R2, R24: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Emulated bind forms validate arguments before I/O; numeric-only SQL and ordered values pass unchanged to server-owned syntax/coverage validation, preserving quote/comment/operator semantics without regex substitution.
- [ ] Ordinary queries and queued pipeline operations use the same typed adapter compiler; optional SQLSpec preparation remains a separate owning boundary.
- [ ] One local signed conventional commit and synchronized task/spec state follow passing verification.

## Notes & Discoveries

None.
