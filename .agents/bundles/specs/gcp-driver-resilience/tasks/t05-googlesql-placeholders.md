---
type: Task
id: gcp-driver-resilience:t05
title: Char-aware GoogleSQL placeholder lexer
state: pending
priority: P2
verification_strategy: regression_tdd
depends_on: [gcp-driver-resilience:t02]
files:
  - src/rs/vertebra-db/src/spanner.rs
tests:
  - src/rs/vertebra-db/tests/spanner_wire_tests.rs
plan_revision: 2
plan_commit: 24e2931
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T16:10:00Z
commit: null
tags: [rust, spanner, sql, bugfix]
---

# t05 — Char-aware GoogleSQL placeholder lexer

## Objective

`rewrite_spangres_placeholders` rewrites positional `$N` placeholders to `@pN` only outside GoogleSQL literals, identifiers, and comments, and preserves every non-ASCII character.

## Context

- Spec R10 (`../spec.md`).
- `regression_tdd`: the current implementation pushes `bytes[i] as char` inside quotes and comments (UTF-8 bytes become Latin-1 characters), treats `"..."` as an identifier without escapes, ignores backslash escapes, backtick identifiers, triple-quoted strings, and `#` comments, and handles PostgreSQL `$$` dollar quotes that GoogleSQL does not have.
- The only caller is `SpannerClient::statement` (GoogleSQL dialect when the SQL contains `$`). The name stays (domain naming memory: Spangres-style `$N` input). The public signature stays `pub fn rewrite_spangres_placeholders(sql: &str) -> (String, Vec<String>)`.
- GoogleSQL lexical rules applied: string and bytes literals use `'`, `"`, `'''`, or `"""` with optional `r`/`b`/`rb`/`br` prefixes (copied as ordinary characters); inside any literal a backslash consumes the next character for termination purposes (raw literals included, since they cannot end with an odd number of backslashes); triple-quoted literals may span lines; backtick identifiers use backslash escapes; comments are `--` and `#` to end of line and non-nesting `/* */`. Unterminated literals or comments copy the rest verbatim.

## Steps

1. **Write failing tests (RED)** in `src/rs/vertebra-db/tests/spanner_wire_tests.rs`: keep `test_spangres_placeholder_rewriter` and add:

   ```rust
   #[test]
   fn spangres_placeholder_rewriter_lexes_googlesql_literals_and_comments() {
       let cases: [(&str, &str, &str, usize); 8] = [
           ("non-ascii literal", "SELECT 'é$1ü', $1", "SELECT 'é$1ü', @p1", 1),
           ("backslash escape", r"SELECT 'it\'s $1', $2", r"SELECT 'it\'s $1', @p2", 2),
           ("double-quoted string", r#"SELECT "a\"$1", $3"#, r#"SELECT "a\"$1", @p3"#, 3),
           ("backtick identifier", "SELECT `col$1` FROM t WHERE x = $1", "SELECT `col$1` FROM t WHERE x = @p1", 1),
           ("triple quoted", "SELECT '''line $1\n'still' $2''', $4", "SELECT '''line $1\n'still' $2''', @p4", 4),
           ("raw and bytes", r"SELECT r'\$1', b'$2', rb'$3', $5", r"SELECT r'\$1', b'$2', rb'$3', @p5", 5),
           ("hash and dash comments", "SELECT 1 # $1 ☃\n, 2 -- $2\nWHERE a = $6", "SELECT 1 # $1 ☃\n, 2 -- $2\nWHERE a = @p6", 6),
           ("dollar zero and unterminated", "SELECT $0, '$1", "SELECT $0, '$1", 0),
       ];
       for (label, sql, expected_sql, max_ordinal) in cases {
           let (rewritten, names) = rewrite_spangres_placeholders(sql);
           assert_eq!(rewritten, expected_sql, "{label}");
           assert_eq!(names, (1..=max_ordinal).map(|n| format!("p{n}")).collect::<Vec<_>>(), "{label}");
       }
   }
   ```

   - RED: `cd src/rs && cargo test -p vertebra-db --test spanner_wire_tests spangres_placeholder` → failures on the non-ASCII, escape, backtick, triple-quote, and `#` cases.

2. **Implement (GREEN)** — rewrite the body of `rewrite_spangres_placeholders` in `src/rs/vertebra-db/src/spanner.rs`:
   - Walk `sql` with a byte index `i` and only ever copy whole slices `&sql[start..end]` whose bounds sit on ASCII delimiters, so multi-byte characters are copied intact; ordinary characters advance by `ch.len_utf8()` from `sql[i..].chars().next()`.
   - Private helpers below the function (module-level `fn`s): `fn skip_quoted(sql: &str, start: usize, quote: u8) -> usize` (handles single-character quotes `'`, `"`, `` ` ``; backslash skips the next character; returns the index after the closing quote or `sql.len()`), `fn skip_triple_quoted(sql: &str, start: usize, quote: u8) -> usize` (start points at the first of three quotes; backslash skips the next character; ends after the next three consecutive quotes), `fn skip_line_comment(sql: &str, start: usize) -> usize` (to and excluding `\n`), `fn skip_block_comment(sql: &str, start: usize) -> usize` (after `*/` or end).
   - Dispatch order at each position: `'''`/`"""` → triple; `'`, `"`, `` ` `` → quoted; `--` or `#` → line comment; `/*` → block comment; `$` followed by an ASCII digit `1..=9` → read the digit run, parse as `usize` (overflow → copy verbatim), emit `@p{n}`, track the max ordinal; anything else → copy one character.
   - Remove the `$$` dollar-quote branch. Update the doc comment: "Rewrite Spangres-style `$1..$N` positional placeholders into GoogleSQL `@p1..@pN` outside GoogleSQL string/bytes literals (including triple-quoted and raw forms), backtick identifiers, and `--`, `#`, and `/* */` comments, returning `(rewritten_sql, ["p1", .., "pN"])`."

3. **Refactor & verify.**

## Verification

- RED: `cd src/rs && cargo test -p vertebra-db --test spanner_wire_tests spangres_placeholder` → failures.
- GREEN:
  - `cd src/rs && cargo test -p vertebra-db --test spanner_wire_tests` → pass.
  - `cd src/rs && cargo clippy -p vertebra-db --all-targets -- -D warnings && cargo fmt --check` → exit 0.
  - `grep -n "as char" src/rs/vertebra-db/src/spanner.rs` → no matches inside `rewrite_spangres_placeholders`.
- Phase gate (last task): `make lint-all && make rs-test && make py-test`.
- Commit: `git commit -S -m "fix(rs-db): lex GoogleSQL literals and comments when rewriting positional placeholders"`.

## Acceptance Criteria

- [ ] R10: placeholders inside every GoogleSQL literal, identifier, and comment form stay untouched; non-ASCII text is preserved; `$N` elsewhere becomes `@pN`.

## Notes & Discoveries
