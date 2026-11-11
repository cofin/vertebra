//! SQL Server single-pass `@P1` placeholder rewriter.

/// Single-pass state machine rewriting `?` and `$1`/`$2` placeholders to T-SQL `@P1, @P2, ...`
/// while preserving placeholders inside single-quoted literals (`'...'`), double-quoted identifiers (`"..."`),
/// bracketed identifiers (`[...]`), `--` line comments, and `/* ... */` block comments.
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
                        num = num * 10 + ((bytes[j] - b'0') as usize);
                        j += 1;
                    }
                    seq_index = seq_index.max(num);
                    max_param = max_param.max(num);
                    out.push_str(&format!("@P{num}"));
                    i = j;
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
}
