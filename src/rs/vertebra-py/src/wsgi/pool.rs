//! Adaptive inline-plus-OS-thread-pool executor for PEP 3333 WSGI batches.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pyo3::prelude::*;

use vertebra_core::transport::BatchSnapshot;

use crate::asgi::log_app_exception;
use crate::transport::ResponseSubmitter;
use crate::wsgi::environ::WsgiServerConfig;
use crate::wsgi::runner::execute_single_wsgi_item;

const DEFAULT_STALL_THRESHOLD_US: u64 = 2_000;
const DEFAULT_HYSTERESIS_US: u64 = 2_000_000;
const WATCHDOG_IDLE_GRACE_NS: u64 = 2_000_000;
const MAX_WSGI_THREADS: usize = 512;

static MONOTONIC_EPOCH: OnceLock<Instant> = OnceLock::new();
static GLOBAL_WSGI_EXECUTOR: RwLock<Option<Arc<WsgiExecutor>>> = RwLock::new(None);

#[inline]
fn now_ns() -> u64 {
    let epoch = MONOTONIC_EPOCH.get_or_init(Instant::now);
    (epoch.elapsed().as_nanos() as u64).saturating_add(1)
}

/// Execution mode for `drive_wsgi_batch` controlled by `VERTEBRA_WSGI_MODE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WsgiExecutionMode {
    Auto,
    Concurrent,
    Inline,
}

/// Runtime configuration for `WsgiExecutor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WsgiPoolConfig {
    pub mode: WsgiExecutionMode,
    pub threads: usize,
    pub stall_threshold_us: u64,
    pub hysteresis_us: u64,
}

impl WsgiPoolConfig {
    pub fn from_env() -> Self {
        let cpus = thread::available_parallelism().map_or(4, |n| n.get());
        Self::from_lookup(cfg!(Py_GIL_DISABLED), cpus, |key| std::env::var(key).ok())
    }

    pub(crate) fn from_lookup<F>(free_threaded: bool, cpus: usize, get_env: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        let default_mode = if free_threaded {
            WsgiExecutionMode::Concurrent
        } else {
            WsgiExecutionMode::Auto
        };
        let mode = get_env("VERTEBRA_WSGI_MODE")
            .and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
                "auto" => Some(WsgiExecutionMode::Auto),
                "concurrent" => Some(WsgiExecutionMode::Concurrent),
                "inline" => Some(WsgiExecutionMode::Inline),
                _ => None,
            })
            .unwrap_or(default_mode);

        let default_threads = cpus.saturating_mul(12).clamp(64, 128);
        let threads = get_env("VERTEBRA_WSGI_THREADS")
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .filter(|&v| v > 0)
            .map_or(default_threads, |v| v.clamp(1, MAX_WSGI_THREADS));

        let stall_threshold_us = get_env("VERTEBRA_WSGI_STALL_US")
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_STALL_THRESHOLD_US);

        Self {
            mode,
            threads,
            stall_threshold_us,
            hysteresis_us: DEFAULT_HYSTERESIS_US,
        }
    }
}

/// Owned representation of a batch of WSGI request targets.
pub(crate) enum WsgiBatchItems {
    Contiguous {
        snapshot: Arc<BatchSnapshot>,
        len: usize,
    },
    Indexed(Vec<(Arc<BatchSnapshot>, usize)>),
}

impl WsgiBatchItems {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Contiguous { len, .. } => *len,
            Self::Indexed(items) => items.len(),
        }
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<(Arc<BatchSnapshot>, usize)> {
        match self {
            Self::Contiguous { snapshot, len } => {
                if idx < *len {
                    Some((Arc::clone(snapshot), idx))
                } else {
                    None
                }
            }
            Self::Indexed(items) => items
                .get(idx)
                .map(|(snap, snap_idx)| (Arc::clone(snap), *snap_idx)),
        }
    }
}

/// Wrapper guaranteeing `Py<PyAny>` is never decref'd without an attached Python thread state.
struct GilSafePyObject(Option<Py<PyAny>>);

impl GilSafePyObject {
    #[inline]
    fn new(obj: Py<PyAny>) -> Self {
        Self(Some(obj))
    }

    #[inline]
    fn bind<'py>(&self, py: Python<'py>) -> &Bound<'py, PyAny> {
        self.0
            .as_ref()
            .expect("GilSafePyObject accessed after drop")
            .bind(py)
    }
}

impl Drop for GilSafePyObject {
    fn drop(&mut self) {
        if let Some(obj) = self.0.take() {
            Python::attach(|_| drop(obj));
        }
    }
}

struct InlineHandoffState {
    total: usize,
    next_index: AtomicUsize,
    handed_off: AtomicBool,
    item_start_ns: AtomicU64,
}

impl InlineHandoffState {
    fn new(total: usize) -> Self {
        Self {
            total,
            next_index: AtomicUsize::new(0),
            handed_off: AtomicBool::new(false),
            item_start_ns: AtomicU64::new(0),
        }
    }

    #[inline]
    fn claim_next_inline(&self) -> Option<usize> {
        if self.handed_off.load(Ordering::Acquire) {
            return None;
        }
        let idx = self.next_index.fetch_add(1, Ordering::AcqRel);
        if idx < self.total { Some(idx) } else { None }
    }

    #[inline]
    fn mark_item_start(&self, start_ns: u64) {
        self.item_start_ns.store(start_ns.max(1), Ordering::Release);
    }

    #[inline]
    fn clear_item_start(&self) {
        self.item_start_ns.store(0, Ordering::Release);
    }

    #[inline]
    fn is_stalled(&self, now: u64, threshold_ns: u64) -> bool {
        let start = self.item_start_ns.load(Ordering::Acquire);
        start != 0 && now.saturating_sub(start) >= threshold_ns
    }

    #[inline]
    fn try_handoff(&self) -> Option<Range<usize>> {
        if self.handed_off.swap(true, Ordering::AcqRel) {
            return None;
        }
        let start = self.next_index.swap(self.total, Ordering::AcqRel);
        if start < self.total {
            Some(start..self.total)
        } else {
            None
        }
    }
}

struct InlineBatchHandoff {
    state: InlineHandoffState,
    payload: HandoffPayload,
}

enum HandoffPayload {
    Wsgi {
        app: Arc<GilSafePyObject>,
        items: WsgiBatchItems,
        server_config: Arc<WsgiServerConfig>,
    },
    #[cfg(test)]
    Test(Arc<dyn Fn(usize) + Send + Sync>),
}

enum PoolTask {
    Wsgi {
        app: Arc<GilSafePyObject>,
        snapshot: Arc<BatchSnapshot>,
        index: usize,
        server_config: Arc<WsgiServerConfig>,
        request_id: u64,
    },
    #[cfg(test)]
    Test(Box<dyn FnOnce() + Send>),
}

struct QueueState {
    tasks: VecDeque<PoolTask>,
    shutdown: bool,
}

struct WatchdogState {
    active: Vec<Arc<InlineBatchHandoff>>,
    awake: bool,
    shutdown: bool,
}

/// Adaptive native Rust WSGI executor with inline fastpath, stall watchdog, and lazy OS thread pool.
pub(crate) struct WsgiExecutor {
    config: WsgiPoolConfig,
    stall_threshold_ns: u64,
    hysteresis_ns: u64,
    hysteresis_until_ns: AtomicU64,
    queue_mu: Mutex<QueueState>,
    queue_cv: Condvar,
    workers_spawned: AtomicBool,
    workers: Mutex<Vec<JoinHandle<()>>>,
    watchdog_started: AtomicBool,
    watchdog_mu: Mutex<WatchdogState>,
    watchdog_cv: Condvar,
    watchdog_handle: Mutex<Option<JoinHandle<()>>>,
}

impl WsgiExecutor {
    pub fn new(config: WsgiPoolConfig) -> Self {
        Self {
            config,
            stall_threshold_ns: config.stall_threshold_us.saturating_mul(1_000),
            hysteresis_ns: config.hysteresis_us.saturating_mul(1_000),
            hysteresis_until_ns: AtomicU64::new(0),
            queue_mu: Mutex::new(QueueState {
                tasks: VecDeque::new(),
                shutdown: false,
            }),
            queue_cv: Condvar::new(),
            workers_spawned: AtomicBool::new(false),
            workers: Mutex::new(Vec::new()),
            watchdog_started: AtomicBool::new(false),
            watchdog_mu: Mutex::new(WatchdogState {
                active: Vec::new(),
                awake: false,
                shutdown: false,
            }),
            watchdog_cv: Condvar::new(),
            watchdog_handle: Mutex::new(None),
        }
    }

    #[inline]
    fn is_in_concurrent_window(&self, now: u64) -> bool {
        let until = self.hysteresis_until_ns.load(Ordering::Acquire);
        until != 0 && now < until
    }

    #[inline]
    fn mark_concurrent_window(&self, now: u64) {
        let deadline = now.saturating_add(self.hysteresis_ns);
        self.hysteresis_until_ns
            .fetch_max(deadline, Ordering::AcqRel);
    }

    fn ensure_workers_spawned(self: &Arc<Self>) {
        if self.workers_spawned.load(Ordering::Acquire) {
            return;
        }
        let mut guard = self
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.workers_spawned.load(Ordering::Acquire) {
            return;
        }
        let is_shutdown = self
            .queue_mu
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .shutdown;
        if is_shutdown {
            return;
        }
        let count = self.config.threads.max(1);
        guard.reserve(count);
        for idx in 0..count {
            let exec = Arc::clone(self);
            let handle = thread::Builder::new()
                .name(format!("vertebra-wsgi-{idx}"))
                .spawn(move || exec.worker_loop());
            if let Ok(h) = handle {
                guard.push(h);
            }
        }
        self.workers_spawned.store(true, Ordering::Release);
    }

    fn ensure_watchdog_spawned(self: &Arc<Self>) {
        if self.watchdog_started.load(Ordering::Acquire) {
            return;
        }
        let mut handle_guard = self
            .watchdog_handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.watchdog_started.load(Ordering::Acquire) {
            return;
        }
        let is_shutdown = self
            .watchdog_mu
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .shutdown;
        if is_shutdown {
            return;
        }
        let exec = Arc::clone(self);
        if let Ok(h) = thread::Builder::new()
            .name("vertebra-wsgi-watchdog".to_string())
            .spawn(move || exec.watchdog_loop())
        {
            *handle_guard = Some(h);
            self.watchdog_started.store(true, Ordering::Release);
        }
    }

    fn worker_loop(self: Arc<Self>) {
        loop {
            let task = {
                let mut guard = self
                    .queue_mu
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                while guard.tasks.is_empty() && !guard.shutdown {
                    guard = self
                        .queue_cv
                        .wait(guard)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
                match guard.tasks.pop_front() {
                    Some(t) => t,
                    None => return,
                }
            };
            match task {
                PoolTask::Wsgi {
                    app,
                    snapshot,
                    index,
                    server_config,
                    request_id,
                } => {
                    Python::attach(|py| {
                        let flushed = execute_single_wsgi_item(
                            py,
                            app.bind(py),
                            snapshot,
                            index,
                            server_config,
                            request_id,
                        );
                        if let Err(err) = ResponseSubmitter::submit_raw_inner(
                            py,
                            flushed.request_id,
                            flushed.status,
                            flushed.headers_raw.as_slice(),
                            flushed.body.as_slice(py),
                        ) {
                            log_app_exception(py, "WSGI", &err);
                        }
                        drop(flushed);
                        drop(app);
                    });
                }
                #[cfg(test)]
                PoolTask::Test(func) => func(),
            }
        }
    }

    fn enqueue_handoff_range(
        self: &Arc<Self>,
        handoff: &Arc<InlineBatchHandoff>,
        range: Range<usize>,
    ) {
        if range.is_empty() {
            return;
        }
        let count = range.len();
        let mut batch_tasks = Vec::with_capacity(count);
        match &handoff.payload {
            HandoffPayload::Wsgi {
                app,
                items,
                server_config,
            } => {
                for idx in range {
                    if let Some((snapshot, snap_idx)) = items.get(idx) {
                        let request_id = snapshot.node(snap_idx).correlation_id;
                        batch_tasks.push(PoolTask::Wsgi {
                            app: Arc::clone(app),
                            snapshot,
                            index: snap_idx,
                            server_config: Arc::clone(server_config),
                            request_id,
                        });
                    }
                }
            }
            #[cfg(test)]
            HandoffPayload::Test(func) => {
                for idx in range {
                    let f = Arc::clone(func);
                    batch_tasks.push(PoolTask::Test(Box::new(move || f(idx))));
                }
            }
        }
        let _ = self.enqueue_tasks(batch_tasks);
    }

    fn enqueue_tasks(self: &Arc<Self>, tasks: Vec<PoolTask>) -> bool {
        if tasks.is_empty() {
            return true;
        }
        self.ensure_workers_spawned();
        let count = tasks.len();
        {
            let mut guard = self
                .queue_mu
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if guard.shutdown {
                drop(guard);
                drop(tasks);
                return false;
            }
            guard.tasks.extend(tasks);
        }
        if count == 1 {
            self.queue_cv.notify_one();
        } else {
            self.queue_cv.notify_all();
        }
        true
    }

    fn watchdog_loop(self: Arc<Self>) {
        let poll_interval =
            Duration::from_nanos((self.stall_threshold_ns / 2).clamp(25_000, 100_000));
        let mut idle_since: Option<u64> = None;
        {
            let mut guard = self
                .watchdog_mu
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.awake = true;
        }

        loop {
            let (snapshot, shutdown) = {
                let guard = self
                    .watchdog_mu
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                (guard.active.clone(), guard.shutdown)
            };

            if shutdown {
                drop(snapshot);
                let mut guard = self
                    .watchdog_mu
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                guard.awake = false;
                return;
            }

            if !snapshot.is_empty() {
                idle_since = None;
                let now = now_ns();
                let mut handed_off_refs: Vec<Arc<InlineBatchHandoff>> = Vec::new();
                for handoff in &snapshot {
                    if handoff.state.is_stalled(now, self.stall_threshold_ns) {
                        self.mark_concurrent_window(now);
                        if let Some(range) = handoff.state.try_handoff() {
                            handed_off_refs.push(Arc::clone(handoff));
                            self.enqueue_handoff_range(handoff, range);
                        }
                    }
                }
                if !handed_off_refs.is_empty() {
                    let removed = {
                        let mut guard = self
                            .watchdog_mu
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        let mut extracted = Vec::with_capacity(handed_off_refs.len());
                        guard.active.retain(|cur| {
                            if handed_off_refs.iter().any(|h| Arc::ptr_eq(cur, h)) {
                                extracted.push(Arc::clone(cur));
                                false
                            } else {
                                true
                            }
                        });
                        extracted
                    };
                    drop(removed);
                }
                drop(handed_off_refs);
                drop(snapshot);

                let guard = self
                    .watchdog_mu
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if !guard.shutdown {
                    let _ = self.watchdog_cv.wait_timeout(guard, poll_interval);
                }
            } else {
                drop(snapshot);
                let now = now_ns();
                let start_idle = *idle_since.get_or_insert(now);
                if now.saturating_sub(start_idle) < WATCHDOG_IDLE_GRACE_NS {
                    let guard = self
                        .watchdog_mu
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if guard.active.is_empty() && !guard.shutdown {
                        let _ = self
                            .watchdog_cv
                            .wait_timeout(guard, Duration::from_micros(100));
                    }
                    continue;
                }

                let mut guard = self
                    .watchdog_mu
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if guard.shutdown {
                    guard.awake = false;
                    return;
                }
                if !guard.active.is_empty() {
                    idle_since = None;
                    continue;
                }
                guard.awake = false;
                while guard.active.is_empty() && !guard.shutdown {
                    guard = self
                        .watchdog_cv
                        .wait(guard)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
                if guard.shutdown {
                    return;
                }
                guard.awake = true;
                idle_since = None;
            }
        }
    }

    fn publish_active_handoff(self: &Arc<Self>, handoff: Arc<InlineBatchHandoff>) {
        self.ensure_watchdog_spawned();
        let mut guard = self
            .watchdog_mu
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.active.push(handoff);
        if !guard.awake {
            self.watchdog_cv.notify_one();
        }
    }

    fn unpublish_active_handoff(&self, handoff: &Arc<InlineBatchHandoff>) {
        let removed = {
            let mut guard = self
                .watchdog_mu
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard
                .active
                .iter()
                .position(|cur| Arc::ptr_eq(cur, handoff))
                .map(|pos| guard.active.swap_remove(pos))
        };
        drop(removed);
    }

    fn dispatch_concurrent(
        self: &Arc<Self>,
        app: &Bound<'_, PyAny>,
        items: WsgiBatchItems,
        server_config: Arc<WsgiServerConfig>,
    ) -> usize {
        let total = items.len();
        let shared_app = Arc::new(GilSafePyObject::new(app.clone().unbind()));
        let mut tasks = Vec::with_capacity(total);
        for idx in 0..total {
            if let Some((snapshot, snap_idx)) = items.get(idx) {
                let request_id = snapshot.node(snap_idx).correlation_id;
                tasks.push(PoolTask::Wsgi {
                    app: Arc::clone(&shared_app),
                    snapshot,
                    index: snap_idx,
                    server_config: Arc::clone(&server_config),
                    request_id,
                });
            }
        }
        let _ = self.enqueue_tasks(tasks);
        total
    }

    fn dispatch_inline_all(
        py: Python<'_>,
        app: &Bound<'_, PyAny>,
        items: &WsgiBatchItems,
        server_config: &Arc<WsgiServerConfig>,
    ) -> PyResult<usize> {
        let total = items.len();
        let mut completed = Vec::with_capacity(total);
        for idx in 0..total {
            if let Some((snapshot, snap_idx)) = items.get(idx) {
                let request_id = snapshot.node(snap_idx).correlation_id;
                completed.push(execute_single_wsgi_item(
                    py,
                    app,
                    snapshot,
                    snap_idx,
                    Arc::clone(server_config),
                    request_id,
                ));
            }
        }
        let len = completed.len();
        ResponseSubmitter::submit_batch_flushed(py, completed)?;
        Ok(len)
    }

    pub(crate) fn dispatch_batch(
        self: &Arc<Self>,
        py: Python<'_>,
        app: &Bound<'_, PyAny>,
        items: WsgiBatchItems,
        server_config: Arc<WsgiServerConfig>,
    ) -> PyResult<usize> {
        let total = items.len();
        if total == 0 {
            return Ok(0);
        }

        match self.config.mode {
            WsgiExecutionMode::Inline => {
                return Self::dispatch_inline_all(py, app, &items, &server_config);
            }
            WsgiExecutionMode::Concurrent => {
                return Ok(self.dispatch_concurrent(app, items, server_config));
            }
            WsgiExecutionMode::Auto => {}
        }

        let now = now_ns();
        if self.is_in_concurrent_window(now) {
            return Ok(self.dispatch_concurrent(app, items, server_config));
        }

        if total == 1 {
            let Some((snapshot, snap_idx)) = items.get(0) else {
                return Ok(0);
            };
            let request_id = snapshot.node(snap_idx).correlation_id;
            let start = now;
            let flushed =
                execute_single_wsgi_item(py, app, snapshot, snap_idx, server_config, request_id);
            let end = now_ns();
            if end.saturating_sub(start) >= self.stall_threshold_ns {
                self.mark_concurrent_window(end);
            }
            ResponseSubmitter::submit_batch_flushed(py, vec![flushed])?;
            return Ok(1);
        }

        let shared_app = Arc::new(GilSafePyObject::new(app.clone().unbind()));
        let handoff = Arc::new(InlineBatchHandoff {
            state: InlineHandoffState::new(total),
            payload: HandoffPayload::Wsgi {
                app: shared_app,
                items,
                server_config,
            },
        });
        self.publish_active_handoff(Arc::clone(&handoff));

        let mut completed = Vec::with_capacity(total);
        let mut item_start = now;
        while let Some(idx) = handoff.state.claim_next_inline() {
            let (snapshot, snap_idx, cfg) = match &handoff.payload {
                HandoffPayload::Wsgi {
                    items,
                    server_config,
                    ..
                } => {
                    let Some((s, si)) = items.get(idx) else {
                        continue;
                    };
                    (s, si, Arc::clone(server_config))
                }
                #[cfg(test)]
                HandoffPayload::Test(_) => unreachable!(),
            };
            let request_id = snapshot.node(snap_idx).correlation_id;
            handoff.state.mark_item_start(item_start);
            let flushed = execute_single_wsgi_item(py, app, snapshot, snap_idx, cfg, request_id);
            let item_end = now_ns();
            handoff.state.clear_item_start();
            completed.push(flushed);

            let elapsed = item_end.saturating_sub(item_start);
            item_start = item_end;
            if handoff.state.handed_off.load(Ordering::Acquire)
                || elapsed >= self.stall_threshold_ns
            {
                if elapsed >= self.stall_threshold_ns {
                    self.mark_concurrent_window(item_end);
                }
                if let Some(range) = handoff.state.try_handoff() {
                    self.unpublish_active_handoff(&handoff);
                    self.enqueue_handoff_range(&handoff, range);
                }
                break;
            }
        }

        self.unpublish_active_handoff(&handoff);
        drop(handoff);
        ResponseSubmitter::submit_batch_flushed(py, completed)?;
        Ok(total)
    }

    pub fn shutdown(&self) {
        {
            let mut guard = self
                .watchdog_mu
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.shutdown = true;
            let drained = std::mem::take(&mut guard.active);
            self.watchdog_cv.notify_all();
            drop(guard);
            drop(drained);
        }
        if let Some(handle) = self
            .watchdog_handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = handle.join();
        }
        {
            let mut guard = self
                .queue_mu
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.shutdown = true;
            self.queue_cv.notify_all();
        }
        let handles = {
            let mut guard = self
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            let _ = handle.join();
        }
    }
}

/// Return the process-global `WsgiExecutor`, initializing it lazily from environment variables.
pub(crate) fn global_wsgi_executor() -> Arc<WsgiExecutor> {
    {
        let guard = GLOBAL_WSGI_EXECUTOR
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(ref exec) = *guard {
            return Arc::clone(exec);
        }
    }
    let mut guard = GLOBAL_WSGI_EXECUTOR
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(ref exec) = *guard {
        return Arc::clone(exec);
    }
    let exec = Arc::new(WsgiExecutor::new(WsgiPoolConfig::from_env()));
    *guard = Some(Arc::clone(&exec));
    exec
}

/// Drain and shut down the global WSGI executor without holding the GIL across thread joins.
#[pyfunction]
#[pyo3(name = "shutdown_wsgi_executor")]
pub fn shutdown_wsgi_executor(py: Python<'_>) -> PyResult<()> {
    let existing = {
        let mut guard = GLOBAL_WSGI_EXECUTOR
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.take()
    };
    if let Some(exec) = existing {
        py.detach(|| exec.shutdown());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Barrier;

    fn lookup_map<'a>(entries: &[(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        let map: HashMap<&'a str, &'a str> = entries.iter().copied().collect();
        move |k| map.get(k).map(|v| (*v).to_string())
    }

    #[test]
    fn config_parsing_defaults_and_overrides() {
        let gil_default = WsgiPoolConfig::from_lookup(false, 12, lookup_map(&[]));
        assert_eq!(gil_default.mode, WsgiExecutionMode::Auto);
        assert_eq!(gil_default.threads, 128);
        assert_eq!(gil_default.stall_threshold_us, 2_000);
        assert_eq!(gil_default.hysteresis_us, 2_000_000);

        let ft_default = WsgiPoolConfig::from_lookup(true, 4, lookup_map(&[]));
        assert_eq!(ft_default.mode, WsgiExecutionMode::Concurrent);
        assert_eq!(ft_default.threads, 64);

        let many_cpus = WsgiPoolConfig::from_lookup(false, 64, lookup_map(&[]));
        assert_eq!(many_cpus.threads, 128);

        let overrides = WsgiPoolConfig::from_lookup(
            false,
            12,
            lookup_map(&[
                ("VERTEBRA_WSGI_MODE", "  CoNcUrReNt "),
                ("VERTEBRA_WSGI_THREADS", "999"),
                ("VERTEBRA_WSGI_STALL_US", "500"),
            ]),
        );
        assert_eq!(overrides.mode, WsgiExecutionMode::Concurrent);
        assert_eq!(overrides.threads, 512);
        assert_eq!(overrides.stall_threshold_us, 500);

        let inline_mode =
            WsgiPoolConfig::from_lookup(true, 12, lookup_map(&[("VERTEBRA_WSGI_MODE", "INLINE")]));
        assert_eq!(inline_mode.mode, WsgiExecutionMode::Inline);

        let invalid = WsgiPoolConfig::from_lookup(
            false,
            8,
            lookup_map(&[
                ("VERTEBRA_WSGI_MODE", "bogus"),
                ("VERTEBRA_WSGI_THREADS", "0"),
                ("VERTEBRA_WSGI_STALL_US", "not-a-number"),
            ]),
        );
        assert_eq!(invalid.mode, WsgiExecutionMode::Auto);
        assert_eq!(invalid.threads, 96);
        assert_eq!(invalid.stall_threshold_us, 2_000);
    }

    #[test]
    fn atomic_handoff_executes_every_index_strictly_once() {
        const N: usize = 32;
        for _ in 0..1_000 {
            let state = Arc::new(InlineHandoffState::new(N));
            let counts: Arc<Vec<AtomicUsize>> =
                Arc::new((0..N).map(|_| AtomicUsize::new(0)).collect());
            let barrier = Arc::new(Barrier::new(2));

            let s2 = Arc::clone(&state);
            let c2 = Arc::clone(&counts);
            let b2 = Arc::clone(&barrier);
            let watchdog = thread::spawn(move || {
                b2.wait();
                if let Some(range) = s2.try_handoff() {
                    for idx in range {
                        c2[idx].fetch_add(1, Ordering::Relaxed);
                    }
                }
            });

            barrier.wait();
            while let Some(idx) = state.claim_next_inline() {
                counts[idx].fetch_add(1, Ordering::Relaxed);
            }
            watchdog.join().unwrap();

            for (idx, count) in counts.iter().enumerate() {
                assert_eq!(
                    count.load(Ordering::Relaxed),
                    1,
                    "index {idx} executed wrong number of times"
                );
            }
        }
    }

    #[test]
    fn watchdog_stall_hands_off_remaining_items_in_parallel_and_sets_hysteresis() {
        let exec = Arc::new(WsgiExecutor::new(WsgiPoolConfig {
            mode: WsgiExecutionMode::Auto,
            threads: 16,
            stall_threshold_us: 250,
            hysteresis_us: 2_000_000,
        }));
        const TOTAL: usize = 16;
        let done_count = Arc::new(AtomicUsize::new(0));
        let done_pair = Arc::new((Mutex::new(false), Condvar::new()));

        let dc_pool = Arc::clone(&done_count);
        let dp_pool = Arc::clone(&done_pair);
        let handoff = Arc::new(InlineBatchHandoff {
            state: InlineHandoffState::new(TOTAL),
            payload: HandoffPayload::Test(Arc::new(move |_idx| {
                thread::sleep(Duration::from_millis(15));
                if dc_pool.fetch_add(1, Ordering::AcqRel) + 1 == TOTAL {
                    let mut g = dp_pool.0.lock().unwrap();
                    *g = true;
                    dp_pool.1.notify_one();
                }
            })),
        });

        let start = Instant::now();
        exec.publish_active_handoff(Arc::clone(&handoff));

        let idx0 = handoff.state.claim_next_inline().unwrap();
        assert_eq!(idx0, 0);
        handoff.state.mark_item_start(now_ns());
        thread::sleep(Duration::from_millis(15));
        handoff.state.clear_item_start();
        if done_count.fetch_add(1, Ordering::AcqRel) + 1 == TOTAL {
            let mut g = done_pair.0.lock().unwrap();
            *g = true;
            done_pair.1.notify_one();
        }
        exec.unpublish_active_handoff(&handoff);

        let mut g = done_pair.0.lock().unwrap();
        while !*g {
            let (ng, res) = done_pair.1.wait_timeout(g, Duration::from_secs(2)).unwrap();
            g = ng;
            assert!(!res.timed_out(), "timed out waiting for handed-off items");
        }
        let elapsed = start.elapsed();
        assert_eq!(done_count.load(Ordering::Acquire), TOTAL);
        assert!(
            elapsed < Duration::from_millis(120),
            "expected parallel completion < 120ms, got {elapsed:?}"
        );
        assert!(exec.is_in_concurrent_window(now_ns()));
        exec.shutdown();
    }

    #[test]
    fn shutdown_drains_queued_tasks_and_rejects_post_shutdown_enqueue() {
        let unused = WsgiExecutor::new(WsgiPoolConfig {
            mode: WsgiExecutionMode::Auto,
            threads: 4,
            stall_threshold_us: 250,
            hysteresis_us: 2_000_000,
        });
        unused.shutdown();
        assert!(!unused.workers_spawned.load(Ordering::Acquire));

        let exec = Arc::new(WsgiExecutor::new(WsgiPoolConfig {
            mode: WsgiExecutionMode::Concurrent,
            threads: 4,
            stall_threshold_us: 250,
            hysteresis_us: 2_000_000,
        }));
        let completed = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<PoolTask> = (0..24)
            .map(|_| {
                let c = Arc::clone(&completed);
                PoolTask::Test(Box::new(move || {
                    thread::sleep(Duration::from_millis(2));
                    c.fetch_add(1, Ordering::Relaxed);
                }))
            })
            .collect();
        assert!(exec.enqueue_tasks(tasks));
        exec.shutdown();
        assert_eq!(completed.load(Ordering::Acquire), 24);
        assert!(!exec.enqueue_tasks(vec![PoolTask::Test(Box::new(|| {}))]));
    }
}
