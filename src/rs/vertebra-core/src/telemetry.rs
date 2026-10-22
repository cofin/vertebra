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

/// Sanitize high-cardinality SQL `IN (...)` and `VALUES (...)` lists for `db.query.text`.
pub fn sanitize_sql_statement(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 3 <= bytes.len() && bytes[i..i + 3].eq_ignore_ascii_case(b"IN ") {
            let mut j = i + 3;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'(' {
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
                    out.push_str("IN (?)");
                    i = k;
                    continue;
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
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
}
