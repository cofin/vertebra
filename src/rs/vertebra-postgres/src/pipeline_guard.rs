//! Bounded linear UTF-8 SQL-head scanner rejecting transaction-control aliases and `COPY`
//! statements at the native pipeline boundary before any operation frame is emitted.

use crate::errors::PostgresError;

#[inline]
const fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b >= 0x80
}

#[inline]
const fn is_ident_continue(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

/// Skip whitespace, `--` line comments, nested `/* ... */` block comments, and (only when
/// `allow_semicolons` is `true` before the very first token) leading `;` empty statements.
fn skip_trivia(bytes: &[u8], pos: &mut usize, allow_semicolons: bool) {
    let len = bytes.len();
    while *pos < len {
        let b = bytes[*pos];
        if b.is_ascii_whitespace() || (allow_semicolons && b == b';') {
            *pos += 1;
            continue;
        }
        if b == b'-' && *pos + 1 < len && bytes[*pos + 1] == b'-' {
            *pos += 2;
            while *pos < len && bytes[*pos] != b'\n' && bytes[*pos] != b'\r' {
                *pos += 1;
            }
            continue;
        }
        if b == b'/' && *pos + 1 < len && bytes[*pos + 1] == b'*' {
            *pos += 2;
            let mut depth = 1usize;
            while *pos < len && depth > 0 {
                if *pos + 1 < len && bytes[*pos] == b'/' && bytes[*pos + 1] == b'*' {
                    depth += 1;
                    *pos += 2;
                } else if *pos + 1 < len && bytes[*pos] == b'*' && bytes[*pos + 1] == b'/' {
                    depth -= 1;
                    *pos += 2;
                } else {
                    *pos += 1;
                }
            }
            continue;
        }
        break;
    }
}

/// Read an unquoted PostgreSQL identifier/keyword starting at `*pos`.
///
/// Quoted identifiers (`"..."`), string literals (`'...'`, `E'...'`), dollar-quoted strings
/// (`$tag$...$tag$`), and punctuation return `None`. Non-ASCII UTF-8 continuation bytes (`>= 0x80`),
/// ASCII digits, `_`, and `$` remain attached to the identifier so `COMMIT_suffix` and `COMMITé`
/// never match `COMMIT`.
fn read_unquoted_keyword<'a>(bytes: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    if *pos >= bytes.len() {
        return None;
    }
    let first = bytes[*pos];
    if !is_ident_start(first) {
        return None;
    }
    if matches!(
        first,
        b'e' | b'E' | b'b' | b'B' | b'x' | b'X' | b'n' | b'N' | b'u' | b'U'
    ) && bytes.get(*pos + 1) == Some(&b'\'')
    {
        return None;
    }
    let start = *pos;
    *pos += 1;
    while *pos < bytes.len() && is_ident_continue(bytes[*pos]) {
        *pos += 1;
    }
    Some(&bytes[start..*pos])
}

#[inline]
fn kw_eq(tok: &[u8], expected_upper_ascii: &[u8]) -> bool {
    tok.eq_ignore_ascii_case(expected_upper_ascii)
}

fn tx_control_err() -> PostgresError {
    PostgresError::Interface(
        "transaction control statement is forbidden inside a PostgreSQL pipeline batch; the pipeline context owns transaction and savepoint scope".to_string(),
    )
}

fn copy_err() -> PostgresError {
    PostgresError::UnsupportedFeature(
        "COPY statement is not supported inside a PostgreSQL pipeline batch; use dedicated COPY streaming APIs".to_string(),
    )
}

/// Validate a single SQL command's unquoted leading head (`O(len)` time, `O(1)` space, zero splitting).
pub(crate) fn validate_pipeline_command(sql: &str) -> Result<(), PostgresError> {
    let bytes = sql.as_bytes();
    let mut pos = 0usize;
    skip_trivia(bytes, &mut pos, true);

    let Some(tok1) = read_unquoted_keyword(bytes, &mut pos) else {
        return Ok(());
    };

    if kw_eq(tok1, b"BEGIN")
        || kw_eq(tok1, b"COMMIT")
        || kw_eq(tok1, b"END")
        || kw_eq(tok1, b"ROLLBACK")
        || kw_eq(tok1, b"ABORT")
        || kw_eq(tok1, b"SAVEPOINT")
        || kw_eq(tok1, b"RELEASE")
    {
        return Err(tx_control_err());
    }

    if kw_eq(tok1, b"COPY") {
        return Err(copy_err());
    }

    if kw_eq(tok1, b"START") || kw_eq(tok1, b"PREPARE") {
        skip_trivia(bytes, &mut pos, false);
        if let Some(tok2) = read_unquoted_keyword(bytes, &mut pos)
            && kw_eq(tok2, b"TRANSACTION")
        {
            return Err(tx_control_err());
        }
        return Ok(());
    }

    if kw_eq(tok1, b"SET") {
        skip_trivia(bytes, &mut pos, false);
        let Some(tok2) = read_unquoted_keyword(bytes, &mut pos) else {
            return Ok(());
        };
        if kw_eq(tok2, b"TRANSACTION") {
            return Err(tx_control_err());
        }
        if kw_eq(tok2, b"LOCAL") {
            skip_trivia(bytes, &mut pos, false);
            if let Some(tok3) = read_unquoted_keyword(bytes, &mut pos)
                && kw_eq(tok3, b"TRANSACTION")
            {
                return Err(tx_control_err());
            }
            return Ok(());
        }
        if kw_eq(tok2, b"SESSION") {
            skip_trivia(bytes, &mut pos, false);
            if let Some(tok3) = read_unquoted_keyword(bytes, &mut pos)
                && (kw_eq(tok3, b"TRANSACTION") || kw_eq(tok3, b"CHARACTERISTICS"))
            {
                return Err(tx_control_err());
            }
            return Ok(());
        }
    }

    Ok(())
}

/// Validate all SQL command heads in index order before encoding or submitting any pipeline frame.
pub(crate) fn validate_pipeline_commands<'a>(
    sqls: impl IntoIterator<Item = &'a str>,
) -> Result<(), PostgresError> {
    for (failing_index, sql) in sqls.into_iter().enumerate() {
        if let Err(err) = validate_pipeline_command(sql) {
            return Err(PostgresError::PipelineValidation {
                source: Box::new(err),
                failing_index,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use super::*;
    use crate::errors::postgres_error_to_py;

    #[test]
    fn rejects_single_keyword_transaction_aliases_across_comments_and_semicolons() {
        let cases = [
            ";/*outer/*nested*/x*/;--line\nEnD WORK",
            " \t\r\n BEGIN ISOLATION LEVEL SERIALIZABLE",
            ";;;commit AND CHAIN",
            "-- header\r\nROLLBACK PREPARED 'gx'",
            "/* a /* b */ c */ ABORT;",
            "SAVEPOINT s1",
            "RELEASE SAVEPOINT s1",
            "RELEASE s1",
            "end",
            "commit prepared 'tx1'",
        ];
        for sql in cases {
            let err = validate_pipeline_command(sql)
                .expect_err(&format!("expected InterfaceError for {sql:?}"));
            assert!(
                matches!(err, PostgresError::Interface(_)),
                "unexpected error category for {sql:?}: {err:?}"
            );
            assert_eq!(err.sqlstate(), None);
        }
    }

    #[test]
    fn rejects_multi_keyword_transaction_heads_with_interleaved_comments() {
        let cases = [
            ";;PREPARE/*x*/TRANSACTION 'gid'",
            "START\n--comment\nTRANSACTION READ ONLY",
            ";SET/**/SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
            "SET LOCAL /*nested/*c*/*/ TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "set session transaction read write",
            "set transaction snapshot '00000003-0000001A-1'",
        ];
        for sql in cases {
            let err = validate_pipeline_command(sql)
                .expect_err(&format!("expected InterfaceError for {sql:?}"));
            assert!(
                matches!(err, PostgresError::Interface(_)),
                "unexpected error category for {sql:?}: {err:?}"
            );
            assert_eq!(err.sqlstate(), None);
        }
    }

    #[test]
    fn rejects_copy_statement_heads_as_unsupported_feature() {
        let cases = [
            "; /*bulk*/ COPY items FROM STDIN BINARY",
            "copy (SELECT 1) TO STDOUT",
            "-- load\nCoPy public.events FROM STDIN",
        ];
        for sql in cases {
            let err = validate_pipeline_command(sql)
                .expect_err(&format!("expected UnsupportedFeature for {sql:?}"));
            assert!(
                matches!(err, PostgresError::UnsupportedFeature(_)),
                "unexpected error category for {sql:?}: {err:?}"
            );
            assert_eq!(err.sqlstate(), None);
        }
    }

    #[test]
    fn allows_prepare_statements_quotes_literals_plpgsql_and_identifier_continuations() {
        let allowed = [
            "PREPARE my_stmt AS SELECT 1",
            "PREPARE \"TRANSACTION\" AS SELECT 1",
            "PREPARE transaction_plan (int) AS INSERT INTO t VALUES ($1)",
            "SELECT 'COMMIT', $$ROLLBACK$$, $tag$BEGIN; COPY t FROM STDIN;$tag$",
            "DO $$ BEGIN PERFORM 1; END $$",
            "WITH cte AS (SELECT 1 AS commit) SELECT * FROM cte",
            "CALL process_batch($1)",
            "SELECT COMMIT_suffix, COMMIT\u{00e9}, BEGIN$1 FROM t",
            "\"BEGIN\" INTO t",
            "SET LOCAL TimeZone = 'UTC'",
            "SET SESSION application_name = 'app'",
            "SET search_path = public",
            "START; TRANSACTION",
            "SELECT 1; COMMIT",
            "/* unterminated comment BEGIN COMMIT COPY",
            "-- comment only COMMIT",
            ";;;",
            "",
        ];
        for sql in allowed {
            validate_pipeline_command(sql).unwrap_or_else(|e| {
                panic!("expected {sql:?} to pass pipeline SQL-head guard, got {e:?}")
            });
        }
    }

    #[test]
    fn batch_guard_reports_first_failing_index_and_empty_python_outcomes() {
        let err = validate_pipeline_commands([
            "INSERT INTO t VALUES ($1)",
            ";;/*x*/COMMIT",
            "COPY t TO STDOUT",
        ])
        .expect_err("expected pipeline batch validation error");

        match &err {
            PostgresError::PipelineValidation {
                source,
                failing_index,
            } => {
                assert_eq!(*failing_index, 1);
                assert!(matches!(source.as_ref(), PostgresError::Interface(_)));
            }
            other => panic!("expected PipelineValidation, got {other:?}"),
        }
        assert_eq!(err.sqlstate(), None);

        let copy_batch_err =
            validate_pipeline_commands(["SELECT 1", "SELECT 2", "; --x\nCOPY t TO STDOUT"])
                .expect_err("expected COPY rejection");
        assert!(matches!(
            copy_batch_err,
            PostgresError::PipelineValidation {
                failing_index: 2,
                ..
            }
        ));

        Python::initialize();
        Python::attach(|py| {
            let py_err = postgres_error_to_py(py, err);
            let val = py_err.value(py);
            if let Ok(core_mod) = py.import("vertebra._vertebra") {
                let iface_ty = core_mod.getattr("InterfaceError").unwrap();
                assert!(val.is_instance(&iface_ty).unwrap());
            } else {
                assert_eq!(val.get_type().name().unwrap(), "InterfaceError");
            }
            assert_eq!(
                val.getattr("failing_index")
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                1
            );
            let outcomes = val.getattr("outcomes").unwrap();
            assert_eq!(outcomes.len().unwrap(), 0);
            assert!(val.getattr("sqlstate").unwrap().is_none());

            let py_copy = postgres_error_to_py(py, copy_batch_err);
            let copy_val = py_copy.value(py);
            if let Ok(core_mod) = py.import("vertebra._vertebra") {
                let not_sup_ty = core_mod.getattr("NotSupportedError").unwrap();
                assert!(copy_val.is_instance(&not_sup_ty).unwrap());
            } else {
                assert_eq!(copy_val.get_type().name().unwrap(), "NotSupportedError");
            }
            assert_eq!(
                copy_val
                    .getattr("failing_index")
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                2
            );
            assert_eq!(copy_val.getattr("outcomes").unwrap().len().unwrap(), 0);
            assert!(copy_val.getattr("sqlstate").unwrap().is_none());
        });
    }
}
