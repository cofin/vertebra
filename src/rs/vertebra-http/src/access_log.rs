//! Zero-GIL compiled HTTP access logger (`access_log.rs`).

use std::fmt::Write as _;
use std::time::Duration;
use vertebra_core::logger::{AsyncLogCore, OverflowPolicy};

const ACCESS_LOG_CAPACITY: usize = 4096;
const ACCESS_LOG_FLUSH_INTERVAL: Duration = Duration::from_millis(10);

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

/// Pre-compiled access logger backed by a bounded non-blocking `AsyncLogCore` stream.
pub struct AccessLogger {
    is_json: bool,
    tokens: Vec<CompiledLogToken>,
    core: AsyncLogCore,
}

impl AccessLogger {
    /// Compile an `AccessLogFormat` and stream formatted records to standard output.
    pub fn new(format: AccessLogFormat) -> Self {
        Self::with_writer(format, std::io::stdout())
    }

    /// Compile an `AccessLogFormat` and stream formatted records to a custom `Write` sink.
    pub fn with_writer<W: std::io::Write + Send + 'static>(
        format: AccessLogFormat,
        writer: W,
    ) -> Self {
        let (is_json, tokens) = match format {
            AccessLogFormat::Json => (true, Vec::new()),
            AccessLogFormat::Common => (false, Self::compile_pattern("%h - - \"%m %U %H\" %s %b")),
            AccessLogFormat::Combined => (
                false,
                Self::compile_pattern("%h - - \"%m %U %H\" %s %b \"%R\" \"%A\" %D"),
            ),
            AccessLogFormat::Custom(pattern) => (false, Self::compile_pattern(&pattern)),
        };
        let core = AsyncLogCore::spawn(
            ACCESS_LOG_CAPACITY,
            OverflowPolicy::DropOldest,
            ACCESS_LOG_FLUSH_INTERVAL,
            writer,
        );
        Self {
            is_json,
            tokens,
            core,
        }
    }

    fn compile_pattern(pattern: &str) -> Vec<CompiledLogToken> {
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
        tokens
    }

    /// Format an `AccessLogEntry` without runtime format-string parsing.
    pub fn format_entry(&self, entry: &AccessLogEntry<'_>) -> String {
        if self.is_json {
            let mut out = String::with_capacity(256);
            out.push_str("{\"remote_addr\":");
            push_json_escaped(&mut out, entry.remote_addr);
            out.push_str(",\"method\":");
            push_json_escaped(&mut out, entry.method);
            out.push_str(",\"path\":");
            push_json_escaped(&mut out, entry.path);
            out.push_str(",\"protocol\":");
            push_json_escaped(&mut out, entry.protocol);
            let _ = write!(
                out,
                ",\"status\":{},\"bytes\":{},\"duration_us\":{},\"referer\":",
                entry.status, entry.response_bytes, entry.duration_micros
            );
            push_json_escaped(&mut out, entry.referer);
            out.push_str(",\"user_agent\":");
            push_json_escaped(&mut out, entry.user_agent);
            out.push('}');
            return out;
        }
        let mut out = String::with_capacity(128);
        for token in &self.tokens {
            match token {
                CompiledLogToken::Literal(lit) => out.push_str(lit),
                CompiledLogToken::RemoteAddr => out.push_str(entry.remote_addr),
                CompiledLogToken::Method => out.push_str(entry.method),
                CompiledLogToken::Path => out.push_str(entry.path),
                CompiledLogToken::Protocol => out.push_str(entry.protocol),
                CompiledLogToken::Status => {
                    let _ = write!(out, "{}", entry.status);
                }
                CompiledLogToken::ResponseBytes => {
                    let _ = write!(out, "{}", entry.response_bytes);
                }
                CompiledLogToken::DurationMicros => {
                    let _ = write!(out, "{}", entry.duration_micros);
                }
                CompiledLogToken::Referer => out.push_str(entry.referer),
                CompiledLogToken::UserAgent => out.push_str(entry.user_agent),
            }
        }
        out
    }

    /// Enqueue a formatted access log line into the non-blocking `AsyncLogCore` sink.
    pub fn log(&self, entry: &AccessLogEntry<'_>) {
        let formatted = self.format_entry(entry);
        let _ = self.core.write_bytes(formatted.as_bytes());
    }

    /// Forward an access log entry to [`AccessLogger::log`] for callers migrating off `record`.
    pub fn record(&self, entry: &AccessLogEntry<'_>) {
        self.log(entry);
    }

    /// Block until all enqueued access log lines up to this call are flushed or evicted.
    pub fn flush(&self, timeout: Duration) -> bool {
        self.core.flush(timeout)
    }
}

fn push_json_escaped(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
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
    use std::io::{self, Write};
    use std::sync::{Arc, Condvar, Mutex};

    struct SharedBufferWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBufferWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct GateState {
        entered: bool,
        open: bool,
    }

    struct GatedWriter {
        gate: Arc<(Mutex<GateState>, Condvar)>,
        out: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for GatedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let (lock, cv) = &*self.gate;
            let mut state = lock.lock().unwrap();
            if !state.entered {
                state.entered = true;
                cv.notify_all();
            }
            while !state.open {
                state = cv.wait(state).unwrap();
            }
            drop(state);
            self.out.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn utc_clf_timestamp_formatting() {
        let formatted = format_utc_clf_timestamp(1_700_000_000);
        assert_eq!(formatted, "[14/Nov/2023:22:13:20 +0000]");
    }

    #[test]
    fn with_writer_streams_10k_entries_with_remote_addr_on_flush() {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let logger = AccessLogger::with_writer(
            AccessLogFormat::Combined,
            SharedBufferWriter(Arc::clone(&sink)),
        );

        for idx in 0..10_000 {
            let path = format!("/items/{idx}");
            let entry = AccessLogEntry {
                remote_addr: "203.0.113.42",
                method: "GET",
                path: &path,
                protocol: "HTTP/1.1",
                status: 200,
                response_bytes: 512,
                duration_micros: 42,
                referer: "https://example.test/",
                user_agent: "vertebra-bench/1.0",
            };
            logger.log(&entry);
        }

        assert!(logger.flush(Duration::from_secs(5)));
        let text = String::from_utf8(sink.lock().unwrap().clone()).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert!(!lines.is_empty());
        assert!(lines.iter().all(|line| line.starts_with("203.0.113.42 ")));
        assert!(lines.last().unwrap().contains("/items/9999"));
    }

    #[test]
    fn saturated_sink_drops_oldest_and_stays_bounded() {
        let gate = Arc::new((
            Mutex::new(GateState {
                entered: false,
                open: false,
            }),
            Condvar::new(),
        ));
        let sink = Arc::new(Mutex::new(Vec::new()));
        let logger = AccessLogger::with_writer(
            AccessLogFormat::Json,
            GatedWriter {
                gate: Arc::clone(&gate),
                out: Arc::clone(&sink),
            },
        );

        logger.log(&AccessLogEntry {
            remote_addr: "198.51.100.7",
            method: "GET",
            path: "/warmup",
            protocol: "HTTP/2.0",
            status: 200,
            response_bytes: 0,
            duration_micros: 1,
            referer: "-",
            user_agent: "sat-client",
        });

        {
            let (lock, cv) = &*gate;
            let mut state = lock.lock().unwrap();
            while !state.entered {
                state = cv.wait(state).unwrap();
            }
        }

        for idx in 0..10_000 {
            let path = format!("/sat/{idx}");
            let entry = AccessLogEntry {
                remote_addr: "198.51.100.7",
                method: "POST",
                path: &path,
                protocol: "HTTP/2.0",
                status: 201,
                response_bytes: 64,
                duration_micros: 15,
                referer: "-",
                user_agent: "sat-client",
            };
            logger.log(&entry);
        }

        {
            let (lock, cv) = &*gate;
            let mut state = lock.lock().unwrap();
            state.open = true;
            cv.notify_all();
        }

        assert!(logger.flush(Duration::from_secs(5)));
        let text = String::from_utf8(sink.lock().unwrap().clone()).unwrap();
        let line_count = text.lines().count();
        assert!(
            line_count <= ACCESS_LOG_CAPACITY + 1,
            "expected bounded output under saturation, got {line_count} lines"
        );
        assert!(text.contains("\"/sat/9999\""));
        assert!(!text.contains("\"/sat/0\""));
    }

    #[test]
    fn json_escaping_handles_quotes_backslashes_and_control_chars() {
        let logger = AccessLogger::new(AccessLogFormat::Json);
        let entry = AccessLogEntry {
            remote_addr: "192.0.2.10",
            method: "GET",
            path: "/search?q=\"a\\b\"\n\t\x00\x1b",
            protocol: "HTTP/1.1",
            status: 400,
            response_bytes: 0,
            duration_micros: 9,
            referer: "https://ref.test/x'y\"z\\",
            user_agent: "agent\x08\x0c\r\n/1.0",
        };
        let line = logger.format_entry(&entry);
        let parsed: serde_json::Value =
            serde_json::from_str(&line).expect("formatted access log must be valid JSON");
        assert_eq!(parsed["remote_addr"], "192.0.2.10");
        assert_eq!(parsed["path"], "/search?q=\"a\\b\"\n\t\x00\x1b");
        assert_eq!(parsed["referer"], "https://ref.test/x'y\"z\\");
        assert_eq!(parsed["user_agent"], "agent\x08\x0c\r\n/1.0");
    }
}
