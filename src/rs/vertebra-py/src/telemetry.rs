//! Native telemetry, logging, and guard boosters backed by Rust primitives.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use vertebra_core::logger::{AsyncLogCore, OverflowPolicy};
use vertebra_core::telemetry::W3cTraceContext;

#[derive(Clone)]
struct MemoryLogSink(Arc<Mutex<Vec<Vec<u8>>>>);

impl std::io::Write for MemoryLogSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self
            .0
            .lock()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        guard.push(buf.to_vec());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Dedicated native log sink draining byte records off-GIL.
#[pyclass(name = "NativeLogSink")]
pub struct NativeLogSink {
    core: Option<AsyncLogCore>,
    memory_buf: Option<Arc<Mutex<Vec<Vec<u8>>>>>,
    #[allow(dead_code)]
    target: String,
    #[allow(dead_code)]
    capacity: usize,
    overflow_policy: String,
    flush_interval_ms: u64,
}

#[pymethods]
impl NativeLogSink {
    #[new]
    #[pyo3(signature = (target="stdout", capacity=65536, overflow_policy="block", flush_interval_ms=100))]
    pub fn py_new(
        target: &str,
        capacity: usize,
        overflow_policy: &str,
        flush_interval_ms: u64,
    ) -> PyResult<Self> {
        let policy = match overflow_policy {
            "drop_oldest" | "drop-oldest" => OverflowPolicy::DropOldest,
            "drop_newest" | "drop-newest" => OverflowPolicy::DropNewest,
            "block" | "block_timeout" => OverflowPolicy::BlockTimeoutMs(5000),
            _ => OverflowPolicy::BlockTimeoutMs(5000),
        };
        let interval = Duration::from_millis(flush_interval_ms.max(1));

        let (core, memory_buf) = match target {
            "stdout" => (
                AsyncLogCore::spawn(capacity, policy, interval, std::io::stdout()),
                None,
            ),
            "stderr" => (
                AsyncLogCore::spawn(capacity, policy, interval, std::io::stderr()),
                None,
            ),
            "memory" => {
                let mem = Arc::new(Mutex::new(Vec::new()));
                let writer = MemoryLogSink(Arc::clone(&mem));
                (
                    AsyncLogCore::spawn(capacity, policy, interval, writer),
                    Some(mem),
                )
            }
            path => {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .map_err(|e| {
                        PyIOError::new_err(format!("failed to open log file {path:?}: {e}"))
                    })?;
                (AsyncLogCore::spawn(capacity, policy, interval, file), None)
            }
        };

        Ok(Self {
            core: Some(core),
            memory_buf,
            target: target.to_string(),
            capacity,
            overflow_policy: overflow_policy.to_string(),
            flush_interval_ms,
        })
    }

    pub fn emit(&self, py: Python<'_>, record: &[u8]) -> PyResult<()> {
        let Some(ref core) = self.core else {
            return Err(PyRuntimeError::new_err("logger is shut down"));
        };
        let slice = record;
        let success = py.detach(|| core.write_bytes(slice));
        if !success && self.overflow_policy == "block" {
            return Err(PyRuntimeError::new_err(
                "log queue full and timeout expired",
            ));
        }
        Ok(())
    }

    pub fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let Some(ref core) = self.core else {
            return Ok(());
        };
        let q = core.queue();
        let interval = self.flush_interval_ms.max(50);
        py.detach(|| {
            let start = Instant::now();
            let max_wait = Duration::from_millis(interval * 4);
            while start.elapsed() < max_wait {
                let enq = q.metrics.enqueued.load(Ordering::Relaxed);
                let flu = q.metrics.flushed.load(Ordering::Relaxed);
                let drop = q.metrics.dropped.load(Ordering::Relaxed);
                if flu + drop >= enq {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        Ok(())
    }

    pub fn metrics(&self) -> (u64, u64, u64) {
        if let Some(ref core) = self.core {
            let q = core.queue();
            (
                q.metrics.enqueued.load(Ordering::Relaxed),
                q.metrics.flushed.load(Ordering::Relaxed),
                q.metrics.dropped.load(Ordering::Relaxed),
            )
        } else {
            (0, 0, 0)
        }
    }

    pub fn len(&self) -> usize {
        if let Some(ref core) = self.core {
            let q = core.queue();
            let enq = q.metrics.enqueued.load(Ordering::Relaxed);
            let flu = q.metrics.flushed.load(Ordering::Relaxed);
            let drop = q.metrics.dropped.load(Ordering::Relaxed);
            enq.saturating_sub(flu + drop) as usize
        } else {
            0
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dropped_count(&self) -> u64 {
        if let Some(ref core) = self.core {
            core.queue().metrics.dropped.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    pub fn drain<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyBytes>>> {
        let _ = self.flush(py);
        if let Some(ref mem) = self.memory_buf {
            let mut lock = mem
                .lock()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let items: Vec<Bound<'py, PyBytes>> =
                lock.drain(..).map(|b| PyBytes::new(py, &b)).collect();
            Ok(items)
        } else {
            Ok(Vec::new())
        }
    }
}

/// Zero-overhead W3C traceparent parser.
#[pyfunction]
pub fn parse_traceparent(header: &str) -> Option<(String, String, u8)> {
    let ctx = W3cTraceContext::parse_traceparent(header)?;
    let mut trace_id = String::with_capacity(32);
    for b in ctx.trace_id {
        let _ = write!(trace_id, "{:02x}", b);
    }
    let mut span_id = String::with_capacity(16);
    for b in ctx.span_id {
        let _ = write!(span_id, "{:02x}", b);
    }
    Some((trace_id, span_id, ctx.trace_flags))
}

/// Zero-overhead W3C traceparent formatter.
#[pyfunction]
pub fn format_traceparent(trace_id: &str, span_id: &str, trace_flags: u8) -> String {
    format!("00-{trace_id}-{span_id}-{trace_flags:02x}")
}

/// High-performance UTF-8 safe SQL statement sanitizer.
///
/// Preserves UTF-8 multi-byte characters and collapses `IN (...)` and `VALUES (...)` lists.
#[pyfunction]
pub fn sanitize_sql(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut span_start = 0;
    let mut i = 0;

    while i < bytes.len() {
        let is_word_boundary =
            i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');

        if is_word_boundary {
            // Check for IN (
            if i + 2 <= bytes.len() && bytes[i..i + 2].eq_ignore_ascii_case(b"IN") {
                let mut j = i + 2;
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
                        out.push_str(&sql[span_start..i]);
                        out.push_str("IN (?)");
                        i = k;
                        span_start = i;
                        continue;
                    }
                }
            }

            // Check for VALUES (
            if i + 6 <= bytes.len() && bytes[i..i + 6].eq_ignore_ascii_case(b"VALUES") {
                let mut j = i + 6;
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
                        // Collapse consecutive value tuples if present: VALUES (1, 2), (3, 4)
                        loop {
                            let mut next_j = k;
                            while next_j < bytes.len() && bytes[next_j].is_ascii_whitespace() {
                                next_j += 1;
                            }
                            if next_j < bytes.len() && bytes[next_j] == b',' {
                                let mut tuple_j = next_j + 1;
                                while tuple_j < bytes.len() && bytes[tuple_j].is_ascii_whitespace()
                                {
                                    tuple_j += 1;
                                }
                                if tuple_j < bytes.len() && bytes[tuple_j] == b'(' {
                                    let mut next_depth = 1usize;
                                    let mut next_k = tuple_j + 1;
                                    while next_k < bytes.len() && next_depth > 0 {
                                        if bytes[next_k] == b'(' {
                                            next_depth += 1;
                                        } else if bytes[next_k] == b')' {
                                            next_depth -= 1;
                                        }
                                        next_k += 1;
                                    }
                                    if next_depth == 0 {
                                        k = next_k;
                                        continue;
                                    }
                                }
                            }
                            break;
                        }
                        out.push_str(&sql[span_start..i]);
                        out.push_str("VALUES (?)");
                        i = k;
                        span_start = i;
                        continue;
                    }
                }
            }
        }

        i += 1;
    }

    if span_start < sql.len() {
        out.push_str(&sql[span_start..]);
    }

    out
}

/// HS256 JWT validator executing in Rust.
#[pyclass(name = "NativeJwtGuard")]
pub struct NativeJwtGuard {
    secret: Vec<u8>,
    leeway_secs: u64,
}

#[pymethods]
impl NativeJwtGuard {
    #[new]
    #[pyo3(signature = (secret, leeway_secs=60))]
    pub fn py_new(secret: &[u8], leeway_secs: u64) -> Self {
        Self {
            secret: secret.to_vec(),
            leeway_secs,
        }
    }

    pub fn validate(&self, token: &str) -> PyResult<String> {
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.leeway = self.leeway_secs;
        validation.validate_exp = true;
        let token_data = jsonwebtoken::decode::<serde_json::Value>(
            token,
            &jsonwebtoken::DecodingKey::from_secret(&self.secret),
            &validation,
        )
        .map_err(|e| PyValueError::new_err(format!("invalid JWT: {e}")))?;
        serde_json::to_string(&token_data.claims)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to serialize claims: {e}")))
    }
}

/// Token bucket rate limiter executing in Rust.
#[pyclass(name = "NativeRateLimiter")]
pub struct NativeRateLimiter {
    capacity: f64,
    refill_rate_per_sec: f64,
    buckets: Mutex<HashMap<String, (f64, Instant)>>,
}

#[pymethods]
impl NativeRateLimiter {
    #[new]
    #[pyo3(signature = (capacity, refill_rate_per_sec))]
    pub fn py_new(capacity: u64, refill_rate_per_sec: f64) -> Self {
        Self {
            capacity: capacity as f64,
            refill_rate_per_sec,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    #[pyo3(signature = (key, tokens=1))]
    pub fn acquire(&self, key: &str, tokens: u64) -> bool {
        let needed = tokens as f64;
        if self.capacity < needed {
            return false;
        }
        let now = Instant::now();
        let mut buckets = match self.buckets.lock() {
            Ok(b) => b,
            Err(p) => p.into_inner(),
        };
        let (current_tokens, last_refill) = buckets
            .entry(key.to_string())
            .or_insert((self.capacity, now));
        let elapsed = now.duration_since(*last_refill).as_secs_f64();
        if elapsed > 0.0 {
            let refilled = elapsed * self.refill_rate_per_sec;
            *current_tokens = (*current_tokens + refilled).min(self.capacity);
            *last_refill = now;
        }
        if *current_tokens >= needed {
            *current_tokens -= needed;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_sql_utf8_join_values() {
        let sql = "SELECT /* café */ * FROM t JOIN (SELECT 1) j ON t.id = j.id WHERE x IN (1, 2) AND y VALUES ('a', 42)";
        let sanitized = sanitize_sql(sql);
        assert_eq!(
            sanitized,
            "SELECT /* café */ * FROM t JOIN (SELECT 1) j ON t.id = j.id WHERE x IN (?) AND y VALUES (?)"
        );
    }

    #[test]
    fn test_sanitize_sql_multiple_values_tuples() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')";
        let sanitized = sanitize_sql(sql);
        assert_eq!(sanitized, "INSERT INTO users (id, name) VALUES (?)");
    }

    #[test]
    fn test_rate_limiter() {
        let limiter = NativeRateLimiter::py_new(2, 0.0);
        assert!(limiter.acquire("ip", 1));
        assert!(limiter.acquire("ip", 1));
        assert!(!limiter.acquire("ip", 1));
    }
}
