//! Zero-GIL compiled HTTP access logger (`access_log.rs`).

use std::sync::Mutex;

/// Built-in and custom access log output formats.
#[derive(Debug, Clone)]
pub enum AccessLogFormat {
    /// Apache Common Log Format (`%h - - "%m %U %H" %s %b`).
    Common,
    /// Apache Combined Log Format (`%h - - "%m %U %H" %s %b "%R" "%A" %D`).
    Combined,
    /// Structured single-line JSON log record.
    Json,
    /// Custom format pattern using `%h`, `%m`, `%U`, `%H`, `%s`, `%b`, `%D`, `%R`, `%A`.
    Custom(String),
}

#[derive(Debug, Clone)]
enum CompiledLogToken {
    Literal(String),
    RemoteAddr,
    Method,
    Path,
    Protocol,
    Status,
    ResponseBytes,
    DurationMicros,
    Referer,
    UserAgent,
}

/// Per-request telemetry fields formatted by `AccessLogger`.
#[derive(Debug, Clone)]
pub struct AccessLogEntry<'a> {
    pub remote_addr: &'a str,
    pub method: &'a str,
    pub path: &'a str,
    pub protocol: &'a str,
    pub status: u16,
    pub response_bytes: usize,
    pub duration_micros: u64,
    pub referer: &'a str,
    pub user_agent: &'a str,
}

/// Pre-compiled access logger with non-blocking record buffering.
pub struct AccessLogger {
    is_json: bool,
    tokens: Vec<CompiledLogToken>,
    buffer: Mutex<Vec<String>>,
}

impl AccessLogger {
    /// Compile an `AccessLogFormat` into a flat token slice at startup.
    pub fn new(format: AccessLogFormat) -> Self {
        match format {
            AccessLogFormat::Json => Self {
                is_json: true,
                tokens: Vec::new(),
                buffer: Mutex::new(Vec::new()),
            },
            AccessLogFormat::Common => Self::compile_pattern("%h - - \"%m %U %H\" %s %b"),
            AccessLogFormat::Combined => {
                Self::compile_pattern("%h - - \"%m %U %H\" %s %b \"%R\" \"%A\" %D")
            }
            AccessLogFormat::Custom(pattern) => Self::compile_pattern(&pattern),
        }
    }

    fn compile_pattern(pattern: &str) -> Self {
        let mut tokens = Vec::new();
        let mut literal = String::new();
        let mut chars = pattern.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '%'
                && let Some(&spec) = chars.peek()
            {
                chars.next();
                if !literal.is_empty() {
                    tokens.push(CompiledLogToken::Literal(std::mem::take(&mut literal)));
                }
                match spec {
                    'h' => tokens.push(CompiledLogToken::RemoteAddr),
                    'm' => tokens.push(CompiledLogToken::Method),
                    'U' => tokens.push(CompiledLogToken::Path),
                    'H' => tokens.push(CompiledLogToken::Protocol),
                    's' => tokens.push(CompiledLogToken::Status),
                    'b' => tokens.push(CompiledLogToken::ResponseBytes),
                    'D' => tokens.push(CompiledLogToken::DurationMicros),
                    'R' => tokens.push(CompiledLogToken::Referer),
                    'A' => tokens.push(CompiledLogToken::UserAgent),
                    other => {
                        literal.push('%');
                        literal.push(other);
                    }
                }
                continue;
            }
            literal.push(ch);
        }
        if !literal.is_empty() {
            tokens.push(CompiledLogToken::Literal(literal));
        }
        Self {
            is_json: false,
            tokens,
            buffer: Mutex::new(Vec::new()),
        }
    }

    /// Format an `AccessLogEntry` without runtime format-string parsing.
    pub fn format_entry(&self, entry: &AccessLogEntry<'_>) -> String {
        if self.is_json {
            return format!(
                "{{\"remote_addr\":{:?},\"method\":{:?},\"path\":{:?},\"protocol\":{:?},\"status\":{},\"bytes\":{},\"duration_us\":{},\"referer\":{:?},\"user_agent\":{:?}}}",
                entry.remote_addr,
                entry.method,
                entry.path,
                entry.protocol,
                entry.status,
                entry.response_bytes,
                entry.duration_micros,
                entry.referer,
                entry.user_agent,
            );
        }
        let mut out = String::with_capacity(128);
        for token in &self.tokens {
            match token {
                CompiledLogToken::Literal(lit) => out.push_str(lit),
                CompiledLogToken::RemoteAddr => out.push_str(entry.remote_addr),
                CompiledLogToken::Method => out.push_str(entry.method),
                CompiledLogToken::Path => out.push_str(entry.path),
                CompiledLogToken::Protocol => out.push_str(entry.protocol),
                CompiledLogToken::Status => out.push_str(&entry.status.to_string()),
                CompiledLogToken::ResponseBytes => out.push_str(&entry.response_bytes.to_string()),
                CompiledLogToken::DurationMicros => {
                    out.push_str(&entry.duration_micros.to_string())
                }
                CompiledLogToken::Referer => out.push_str(entry.referer),
                CompiledLogToken::UserAgent => out.push_str(entry.user_agent),
            }
        }
        out
    }

    /// Enqueue a formatted access log line into the non-blocking sink.
    pub fn record(&self, entry: &AccessLogEntry<'_>) {
        let formatted = self.format_entry(entry);
        if let Ok(mut guard) = self.buffer.lock() {
            guard.push(formatted);
        }
    }

    /// Drain all buffered access log lines.
    pub fn drain_buffered(&self) -> Vec<String> {
        self.buffer
            .lock()
            .map(|mut guard| std::mem::take(&mut *guard))
            .unwrap_or_default()
    }
}

/// Format a Unix epoch second timestamp into Apache Common Log Format UTC (`[dd/Mon/yyyy:HH:MM:SS +0000]`)
/// using Howard Hinnant's civil-from-days algorithm without external date crates.
pub fn format_utc_clf_timestamp(epoch_secs: i64) -> String {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = epoch_secs.div_euclid(86_400);
    let secs_of_day = epoch_secs.rem_euclid(86_400);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    let month_str = MONTHS[(m as usize).saturating_sub(1).min(11)];

    format!("[{d:02}/{month_str}/{year:04}:{hour:02}:{minute:02}:{second:02} +0000]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_clf_timestamp_formatting() {
        let formatted = format_utc_clf_timestamp(1_700_000_000);
        assert_eq!(formatted, "[14/Nov/2023:22:13:20 +0000]");
    }
}
