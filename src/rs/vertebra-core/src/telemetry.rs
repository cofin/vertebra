//! Zero-overhead OpenTelemetry W3C `traceparent` parser, span record, and SQL sanitizer (`telemetry.rs`).

/// Parsed W3C Trace Context (`00-<32-hex-trace-id>-<16-hex-span-id>-<2-hex-flags>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct W3cTraceContext {
    pub version: u8,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub trace_flags: u8,
}

impl W3cTraceContext {
    pub fn parse_traceparent(header: &str) -> Option<Self> {
        let trimmed = header.trim();
        let bytes = trimmed.as_bytes();
        if bytes.len() < 55 || bytes[2] != b'-' || bytes[35] != b'-' || bytes[52] != b'-' {
            return None;
        }
        let version = decode_hex_byte(bytes[0], bytes[1])?;
        if version == 0xff {
            return None;
        }
        let mut trace_id = [0u8; 16];
        for i in 0..16 {
            trace_id[i] = decode_hex_byte(bytes[3 + i * 2], bytes[4 + i * 2])?;
        }
        if trace_id == [0u8; 16] {
            return None;
        }
        let mut span_id = [0u8; 8];
        for i in 0..8 {
            span_id[i] = decode_hex_byte(bytes[36 + i * 2], bytes[37 + i * 2])?;
        }
        if span_id == [0u8; 8] {
            return None;
        }
        let trace_flags = decode_hex_byte(bytes[53], bytes[54])?;
        Some(Self {
            version,
            trace_id,
            span_id,
            trace_flags,
        })
    }

    pub fn format_traceparent(&self) -> String {
        let mut out = String::with_capacity(55);
        push_hex_byte(&mut out, self.version);
        out.push('-');
        for &b in &self.trace_id {
            push_hex_byte(&mut out, b);
        }
        out.push('-');
        for &b in &self.span_id {
            push_hex_byte(&mut out, b);
        }
        out.push('-');
        push_hex_byte(&mut out, self.trace_flags);
        out
    }

    pub fn is_sampled(&self) -> bool {
        (self.trace_flags & 0x01) != 0
    }
}

fn decode_hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn decode_hex_byte(hi: u8, lo: u8) -> Option<u8> {
    Some((decode_hex_nibble(hi)? << 4) | decode_hex_nibble(lo)?)
}

fn push_hex_byte(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(HEX[(byte >> 4) as usize] as char);
    out.push(HEX[(byte & 0x0f) as usize] as char);
}

/// Fixed-layout telemetry span record for zero-allocation SPSC ring export.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: [u8; 8],
    pub start_unix_ns: u64,
    pub duration_ns: u64,
    pub status_code: u16,
    pub span_kind: u8,
    pub flags: u8,
}

fn is_sql_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn match_sql_keyword(bytes: &[u8], pos: usize, kw: &[u8]) -> Option<usize> {
    if pos > 0 && is_sql_word_byte(bytes[pos - 1]) {
        return None;
    }
    let end = pos + kw.len();
    if end > bytes.len() || !bytes[pos..end].eq_ignore_ascii_case(kw) {
        return None;
    }
    if end < bytes.len() && is_sql_word_byte(bytes[end]) {
        return None;
    }
    Some(end)
}

/// Sanitize high-cardinality SQL `IN (...)` and `VALUES (...)` lists for `db.query.text`.
pub fn sanitize_sql_statement(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut last_copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let matched = match_sql_keyword(bytes, i, b"IN")
            .map(|end| ("IN (?)", end))
            .or_else(|| match_sql_keyword(bytes, i, b"VALUES").map(|end| ("VALUES (?)", end)));

        if let Some((replacement, kw_end)) = matched {
            let mut j = kw_end;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j > kw_end && j < bytes.len() && bytes[j] == b'(' {
                let mut depth = 1usize;
                let mut k = j + 1;
                while k < bytes.len() && depth > 0 {
                    if bytes[k] == b'(' {
                        depth += 1;
                    } else if bytes[k] == b')' {
                        depth -= 1;
                    }
                    k += 1;
                }
                if depth == 0 {
                    out.push_str(&sql[last_copied..i]);
                    out.push_str(replacement);
                    i = k;
                    last_copied = i;
                    continue;
                }
            }
        }
        i += 1;
    }
    out.push_str(&sql[last_copied..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_format_w3c_traceparent() {
        let raw = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = W3cTraceContext::parse_traceparent(raw).expect("valid traceparent");
        assert!(ctx.is_sampled());
        assert_eq!(ctx.format_traceparent(), raw);
    }

    #[test]
    fn sanitize_high_cardinality_sql_in_clause() {
        let sql = "SELECT * FROM orders WHERE id IN (1, 2, 3, 4, 5) AND status = 'OPEN'";
        let sanitized = sanitize_sql_statement(sql);
        assert_eq!(
            sanitized,
            "SELECT * FROM orders WHERE id IN (?) AND status = 'OPEN'"
        );
    }

    #[test]
    fn sanitize_sql_preserves_utf8_and_join_subquery_and_collapses_values() {
        let sql = "SELECT * FROM orders JOIN (SELECT id FROM cafés) sub ON sub.id = orders.id WHERE tag IN ('α', 'β') AND note = '🚀'";
        let sanitized = sanitize_sql_statement(sql);
        assert_eq!(
            sanitized,
            "SELECT * FROM orders JOIN (SELECT id FROM cafés) sub ON sub.id = orders.id WHERE tag IN (?) AND note = '🚀'"
        );

        let insert_sql = "INSERT INTO items (name, qty) VALUES ('café', 1)";
        assert_eq!(
            sanitize_sql_statement(insert_sql),
            "INSERT INTO items (name, qty) VALUES (?)"
        );
    }
}
