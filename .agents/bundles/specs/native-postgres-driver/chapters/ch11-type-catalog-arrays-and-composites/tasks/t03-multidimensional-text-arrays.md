---
type: Task
id: "native-postgres-driver:ch11:t03"
chapter_id: ch11-type-catalog-arrays-and-composites
title: Implement escaped multidimensional text array codecs and bounded container scanning
description: "Decode and encode PostgreSQL multidimensional text arrays ([lower:upper]= prefixes) with custom typdelim, exact NULL vs quoted NULL vs empty-string rules, and a bounded quoted-field scanner shared with composite codecs."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch11:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch11-type-catalog-arrays-and-composites/spec.md
  - src/rs/vertebra-postgres/src/types.rs
  - src/rs/vertebra-postgres/src/codecs/array.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-postgres/src/codecs/array_text.rs
  - src/rs/vertebra-postgres/src/codecs/array.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/array_text.rs
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
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: PgArray multidimensional bounds, NULL vs empty string distinction, and unknown custom array text fallback
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL array_in/array_out text grammar, typdelim (; for box), and composite vs array NULL rules
tags:
  - postgres
  - rust
  - codecs
  - arrays
---

# Task t03: Implement Escaped Multidimensional Text Array Codecs and Bounded Container Scanning

## Objective & Scope Boundary

Implement PostgreSQL `array_out` / `array_in` text decoding and encoding in `src/rs/vertebra-postgres/src/codecs/array_text.rs`, including the internal bounded quoted/escaped container field scanner (`scan_delimited_container`) reused by composite text codecs in `ch11:t04` without exposing any public Python text-parsing API [^pg-contracts] [^pg-research]. Named and anonymous composite binary/text codecs (`composite.rs`) belong to `ch11:t04`.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — `PgArray` bounds/rank (`0..=6`), `NULL` vs `""` vs `"NULL"`, and unknown custom OID text preservation [^pg-contracts].
- [spec.md](../spec.md) — Chapter 11 `array_text.rs` contracts and container NULL rules.
- `src/rs/vertebra-postgres/src/types.rs` — `TypeCatalog` (`TypeKind::Array { element_oid, delimiter }`, `_box` `1020` with `delimiter = b';'`).
- `src/rs/vertebra-postgres/src/codecs/array.rs` — Shared `PgArray` shape/bound validation helpers from `ch11:t02`.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `CodecContext`, `decode_value_with_catalog`, and `encode_value_with_catalog`.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/codecs/array_text.rs`**:
  - Internal bounded container scanner shared with `ch11:t04` (`composite.rs`):
    - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub(crate) enum ContainerNullRule { ArrayUnquotedNullKeyword, CompositeEmptyUnquoted }`
    - `#[derive(Clone, Debug, PartialEq, Eq)] pub(crate) enum ScannedToken { Null, Value(String), SubArray(Vec<ScannedToken>) }`
    - `pub(crate) fn scan_delimited_container(input: &str, open: u8, close: u8, delimiter: u8, null_rule: ContainerNullRule, expected_arity: Option<usize>, max_depth: usize) -> Result<Vec<ScannedToken>, PostgresError>`:
      1. Enforces `max_depth` and rejects unclosed quotes, unclosed open/close delimiters, or trailing characters after the outer `close`.
      2. Keeps array vs composite `NULL` and empty-slot rules strictly separate:
         - Under `ContainerNullRule::ArrayUnquotedNullKeyword`: adjacent, leading, or trailing delimiters (`"{,}"`, `"{a,}"`, `"{,b}"`, `"{1,,2}"`) fail with `PostgresError::Data`; `"{"` + `"}"` (`"{}"`) yields `0` tokens; unquoted outer ASCII whitespace is trimmed, unquoted `\"`/`\\` escapes unescape per `array_in`, and only unquoted ASCII case-insensitive `NULL` produces `ScannedToken::Null`.
         - Under `ContainerNullRule::CompositeEmptyUnquoted`: empty unquoted slots before, between, or after `delimiter` (`"(,)"` -> `[Null, Null]`, `"(a,)"` -> `[Value("a"), Null]`, `"(,b)"` -> `[Null, Value("b")]`, `"(1,,3)"` -> `[Value("1"), Null, Value("3")]`) emit `ScannedToken::Null` (adjacent/dangling delimiter rejection NEVER applies to `CompositeEmptyUnquoted`); unquoted `NULL` stays `ScannedToken::Value("NULL".into())`, and unquoted literal characters (such as `a\b`) are preserved verbatim without stripping backslashes.
      3. Inside double-quoted `"..."` tokens (under both rules), unescapes BOTH backslash escapes (`\"` -> `"`, `\\` -> `\`) AND PostgreSQL `record_out` doubled-quote escapes (`""` -> `"`).
      4. Disambiguates empty `"()"` under `CompositeEmptyUnquoted` via `expected_arity`: `"()"` returns `Ok(vec![])` (`0` tokens) when `expected_arity == Some(0)`, `Ok(vec![ScannedToken::Null])` (`1` `Null` token) when `expected_arity == Some(1)`, and `Err(PostgresError::Data(_))` when `expected_arity` is `None` or `Some(n)` with `n >= 2` (and whenever `expected_arity == Some(k)` does not match the scanned token count).
  - `pub fn decode_array_text(array_oid: u32, element_oid: u32, delimiter: u8, text: &str, ctx: &CodecContext, catalog: &TypeCatalog, depth: usize) -> Result<PgValue, PostgresError>`:
    - Parses optional leading `[lower1:upper1][lower2:upper2]...=` dimension prefix (validating `1 <= rank <= 6`, `i32` bounds, `dim = upper - lower + 1 >= 1`, and exact match against braced `{...}` extents).
    - `{}` (with no prefix) decodes to canonical rank-`0` `PgValue::Array { values: vec![], dimensions: vec![], lower_bounds: vec![], element_oid }`.
    - Validates rectangular `{...}` nesting across all dimensions (`1..=6`) via `scan_delimited_container(..., ContainerNullRule::ArrayUnquotedNullKeyword, None, ...)` and dispatches each leaf token through `decode_value_with_catalog(element_oid, Format::Text, ...)` at `depth + 1`.
  - `pub fn encode_array_text(value: &PgValue, element_oid: u32, delimiter: u8, out: &mut BytesMut, ctx: &CodecContext, catalog: &TypeCatalog, depth: usize) -> Result<(), PostgresError>`:
    - Validates `PgValue::Array` shape via `array.rs` helpers; writes `"{}"` for rank `0`; emits `[lower:upper]...=` prefix whenever any `lower_bound != 1`; writes `NULL` for `PgValue::Null` elements, `""` for empty strings, and double-quotes any element string that equals case-insensitive `"null"`, is empty, starts/ends with ASCII whitespace, or contains `{`, `}`, `"`, `\`, `delimiter`, or ASCII whitespace (escaping `\` as `\\` and `"` as `\"`).
- **`src/rs/vertebra-postgres/src/codecs/array.rs` & `src/rs/vertebra-postgres/src/codecs/mod.rs`**:
  - Wire `Format::Text` array dispatch in `codecs/mod.rs` to `array_text::{decode_array_text, encode_array_text}` using `(element_oid, delimiter)` from `TypeCatalog`; genuinely undiscovered custom array OIDs absent from `TypeCatalog` stay `PgValue::Text` at their outer `Column.type_oid` until discovered.

## Edge Cases & Error Semantics

- **`NULL` vs `"NULL"` vs `""`**: In array `{NULL,"NULL","","null",null}`, elements `0` and `4` decode to `PgValue::Null`, element `1` to `PgValue::Text("NULL")`, element `2` to `PgValue::Text("")`, and element `3` to `PgValue::Text("null")`; re-encoding produces `{NULL,"NULL","","null",NULL}`.
- **Composite scanner parity (`CompositeEmptyUnquoted`)**: `"(,\"\",NULL,\"NULL\",a\\b,\"x\"\"y\")"` scans into `[Null, Value(""), Value("NULL"), Value("NULL"), Value("a\\b"), Value("x\"y")]`, `"(a,)"` -> `[Value("a"), Null]`, `"(,b)"` -> `[Null, Value("b")]`, and `"()"` yields `[]` for `expected_arity = Some(0)` vs `[Null]` for `expected_arity = Some(1)`.
- **Custom `typdelim` (`_box`, OID `1020`) & escaped nested containers**: `_box` uses `;` (`{(1,2),(0,0);(3,4),(1,1)}`) because `box` literals contain internal commas; escaped composite/JSON elements such as `{"(1,\"a,b\")","{\"k\":[1,2]}"}` unescape cleanly into leaf strings ready for `ch11:t04` / `json.rs` dispatch.
- **Malformed text arrays**: Ragged rows `{{1,2},{3}}`, empty unquoted array slots `{1,,2}` / `{,}`, rank `> 6`, mismatched `[0:2]={1,2}` prefix vs body count (`3 != 2`), inverted `[5:2]={1}`, unclosed quotes `{"abc}`, trailing junk `{1,2}extra`, or `depth > ctx.max_nesting_depth` return `Err(PostgresError::Data(_))`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/array_text.rs` (server-output fixtures + text-vs-binary `PgValue` equality assertions + `SimpleQueryCollector` cold/simple-query integration + composite container scanner parity cases).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `NULL` vs `"NULL"` vs `""` & special chars | `{NULL,"NULL","","a,b","c\"d\\e","{brace}"," lead "}` on `_text` (`1009`) | Decodes `[Null, Text("NULL"), Text(""), Text("a,b"), Text("c\"d\\e"), Text("{brace}"), Text(" lead ")]`; round-trips losslessly and equals binary `_text` `PgValue`. |
| Nonstandard `[lower:upper]=` & multidimensional bounds | `[0:1][-2:0]={{10,NULL,30},{40,50,60}}` on `_int4` (`1007`) and `{}` on `_uuid` (`2951`) | Yields `dimensions=[2, 3], lower_bounds=[0, -2]` (re-encodes with `[0:1][-2:0]=` prefix) and rank-`0` empty `_uuid` array (`{}`). |
| Custom `;` delimiter (`_box` `1020`) & nested JSON/composite text | `{(1,2),(0,0);(3,4),(1,1)}` (`1020`), `{"{\"a\":[1,2]}",NULL}` (`_jsonb` `3807`), and `{"(1,\"x\"\"y\")"}` | `_box` splits on `;` into 2 elements; `_jsonb` yields `[Json(b"{\"a\":[1,2]}"), Null]`; composite string unescapes cleanly. |
| Composite scanner empty slots & doubled quotes (`CompositeEmptyUnquoted`) | `"(,\"\",NULL,\"NULL\",a\\b,\"x\"\"y\")"`, `"(a,)"`, `"(,b)"` with `CompositeEmptyUnquoted` | Yields `[Null, Value(""), Value("NULL"), Value("NULL"), Value("a\\b"), Value("x\"y")]`, `[Value("a"), Null]`, and `[Null, Value("b")]`. |
| Composite `"()"` `0`-attribute vs `1`-attribute disambiguation | `"()"` with `expected_arity = Some(0)`, `Some(1)`, and `Some(2)` under `CompositeEmptyUnquoted` | `Some(0)` -> `Ok([])`; `Some(1)` -> `Ok([ScannedToken::Null])`; `Some(2)` -> `Err(PostgresError::Data(_))`. |
| Unknown custom array OID before discovery | Simple-query column with unknown `type_oid = 55001` (`Format::Text`, `"{1,2}"`) | Remains `PgValue::Text("{1,2}")` with `Column.type_oid == 55001` without speculative array splitting. |
| Ragged, mismatched-bound, & malformed array inputs | `{{1,2},{3}}`, `[1:3]={1,2}`, `{1,,2}`, `{a,}`, `{"unterminated}`, `{{{{{{{1}}}}}}}` (7D) | All six return `Err(PostgresError::Data(_))`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test array_text`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test array_text && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Multidimensional text arrays preserve rank (`0..=6`), `[lower:upper]=` bounds, and exact distinctions among SQL `NULL`, `"NULL"`, and `""`, producing `PgValue::Array` values identical to binary array decoding (R11).
- [ ] `pg_type.typdelim` (`;` for `_box`, `,` otherwise) and PostgreSQL quote/backslash escaping match server `array_in`/`array_out` output (R11).
- [ ] `scan_delimited_container` enforces bounded nesting, handles `CompositeEmptyUnquoted` leading/inner/trailing empty slots, `""` + `\"`/`\\` quote unescaping, and `0`-vs-`1`-attribute `"()"` via `expected_arity`, and rejects ragged or malformed syntax with `PostgresError::Data` (R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `PgArray` multidimensional bounds, `NULL` vs empty string distinction, and unknown custom array text fallback.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `array_in`/`array_out` text grammar, `typdelim` (`;` for `box`), and composite vs array `NULL` rules.
