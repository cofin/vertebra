//! SQL Server single-pass `@P1` placeholder rewriter.

/// Single-pass state machine rewriting `?`, `$1`/`$2`, and existing `@P1`/`@p1` placeholders
/// to T-SQL `@P1, @P2, ...` while preserving placeholders inside single-quoted literals (`'...'`),
/// double-quoted identifiers (`"..."`), bracketed identifiers (`[...]`), `--` line comments, and
/// `/* ... */` block comments.
pub fn rewrite_mssql_placeholders(sql: &str) -> (String, usize) {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum LexState {
        Normal,
        SingleQuote,
        DoubleQuote,
        BracketIdent,
        LineComment,
        BlockComment,
    }

    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 16);
    let mut state = LexState::Normal;
    let mut i = 0usize;
    let mut seq_index = 0usize;
    let mut max_param = 0usize;

    while i < bytes.len() {
        let b = bytes[i];
        match state {
            LexState::Normal => {
                if b == b'\'' {
                    out.push('\'');
                    state = LexState::SingleQuote;
                    i += 1;
                } else if b == b'"' {
                    out.push('"');
                    state = LexState::DoubleQuote;
                    i += 1;
                } else if b == b'[' {
                    out.push('[');
                    state = LexState::BracketIdent;
                    i += 1;
                } else if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    out.push_str("--");
                    state = LexState::LineComment;
                    i += 2;
                } else if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    out.push_str("/*");
                    state = LexState::BlockComment;
                    i += 2;
                } else if b == b'?' {
                    seq_index += 1;
                    max_param = max_param.max(seq_index);
                    out.push_str(&format!("@P{seq_index}"));
                    i += 1;
                } else if b == b'$'
                    && i + 1 < bytes.len()
                    && bytes[i + 1].is_ascii_digit()
                    && bytes[i + 1] != b'0'
                {
                    let mut j = i + 1;
                    let mut num = 0usize;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        num = num
                            .saturating_mul(10)
                            .saturating_add((bytes[j] - b'0') as usize);
                        j += 1;
                    }
                    seq_index = seq_index.max(num);
                    max_param = max_param.max(num);
                    out.push_str(&format!("@P{num}"));
                    i = j;
                } else if b == b'@'
                    && (i == 0
                        || !(bytes[i - 1].is_ascii_alphanumeric()
                            || bytes[i - 1] == b'_'
                            || bytes[i - 1] == b'@'))
                    && i + 2 < bytes.len()
                    && matches!(bytes[i + 1], b'P' | b'p')
                    && bytes[i + 2].is_ascii_digit()
                    && bytes[i + 2] != b'0'
                {
                    let mut j = i + 2;
                    let mut num = 0usize;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        num = num
                            .saturating_mul(10)
                            .saturating_add((bytes[j] - b'0') as usize);
                        j += 1;
                    }
                    let at_boundary = j == bytes.len()
                        || !(bytes[j].is_ascii_alphanumeric()
                            || bytes[j] == b'_'
                            || bytes[j] == b'$'
                            || bytes[j] == b'#'
                            || bytes[j] == b'@');
                    if at_boundary {
                        seq_index = seq_index.max(num);
                        max_param = max_param.max(num);
                        out.push_str(&format!("@P{num}"));
                        i = j;
                    } else {
                        out.push('@');
                        i += 1;
                    }
                } else {
                    let ch = sql[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
            LexState::SingleQuote => {
                if b == b'\'' {
                    out.push('\'');
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        out.push('\'');
                        i += 2;
                    } else {
                        state = LexState::Normal;
                        i += 1;
                    }
                } else {
                    let ch = sql[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
            LexState::DoubleQuote => {
                if b == b'"' {
                    out.push('"');
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        out.push('"');
                        i += 2;
                    } else {
                        state = LexState::Normal;
                        i += 1;
                    }
                } else {
                    let ch = sql[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
            LexState::BracketIdent => {
                if b == b']' {
                    out.push(']');
                    if i + 1 < bytes.len() && bytes[i + 1] == b']' {
                        out.push(']');
                        i += 2;
                    } else {
                        state = LexState::Normal;
                        i += 1;
                    }
                } else {
                    let ch = sql[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
            LexState::LineComment => {
                let ch = sql[i..].chars().next().unwrap();
                out.push(ch);
                i += ch.len_utf8();
                if ch == '\n' {
                    state = LexState::Normal;
                }
            }
            LexState::BlockComment => {
                if b == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    out.push_str("*/");
                    state = LexState::Normal;
                    i += 2;
                } else {
                    let ch = sql[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
        }
    }

    (out, max_param)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_mssql_placeholders_preserves_quotes_and_comments() {
        let sql = "SELECT 'a?b''$1', \"col?name\", [col$2?], * FROM t WHERE a = ? AND b = $2 -- ? $3 comment\n AND /* ? $4 */ c = ?";
        let (rewritten, count) = rewrite_mssql_placeholders(sql);
        assert_eq!(
            rewritten,
            "SELECT 'a?b''$1', \"col?name\", [col$2?], * FROM t WHERE a = @P1 AND b = @P2 -- ? $3 comment\n AND /* ? $4 */ c = @P3"
        );
        assert_eq!(count, 3);
    }

    #[test]
    fn test_rewrite_mssql_placeholders_counts_existing_at_p_and_respects_boundaries() {
        let sql = "INSERT INTO t (a, b, c, d) VALUES (@P1, @p2, @P1, ?)";
        let (rewritten, count) = rewrite_mssql_placeholders(sql);
        assert_eq!(
            rewritten,
            "INSERT INTO t (a, b, c, d) VALUES (@P1, @P2, @P1, @P3)"
        );
        assert_eq!(count, 3);

        let non_placeholders = "SELECT '@P9', \"@P8\", [@P7], @@P6, 'user@p5.com', @P1a, @P0, @P1_col, @P1@x -- @P4\n/* @P3 */ FROM t WHERE x = @P2 AND y = @P1";
        let (rewritten_np, count_np) = rewrite_mssql_placeholders(non_placeholders);
        assert_eq!(rewritten_np, non_placeholders);
        assert_eq!(count_np, 2);

        let email = "SELECT a@P1 FROM t";
        assert_eq!(rewrite_mssql_placeholders(email), (email.to_owned(), 0));
    }
}
