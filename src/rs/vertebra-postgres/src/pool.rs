//! Concrete Rust physical PostgreSQL connection pool (`PostgresPool`, `PostgresPoolOptions`, `PooledConnection`, `PoolStats`).

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::cloud::material::{Clock, CloudConnector, SystemClock};
use crate::config::PostgresConfig;
use crate::credentials::ScopedTokenProvider;
use crate::errors::PostgresError;
use crate::session::statements::PoolerMode;
use crate::session::{ExchangeState, PostgresConnection, establish_postgres_connection};

/// Validated configuration for [`PostgresPool`] sizing, deadlines, and preparation policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresPoolOptions {
    /// Connection pooler topology mode (`Direct` by default).
    pub pooler_mode: PoolerMode,
    /// Whether protocol-level prepared statement caching across a transaction pooler is explicitly enabled.
    pub protocol_level_pooler_cache: bool,
    /// Per-session prepared statement LRU cache capacity (`256` by default; `0` disables caching).
    pub statement_cache_size: usize,
    /// Minimum warm connections established lazily on first acquisition (`1` by default).
    pub min_size: usize,
    /// Maximum concurrent physical connections (`10` by default; must be `>= 1` and `>= min_size`).
    pub max_size: usize,
    /// Maximum time to wait for a pooled connection lease including dial (`30s` by default).
    pub acquire_timeout: Duration,
    /// Maximum time to reset a returned lease (`ROLLBACK` + `DISCARD ALL` + GUC restore; `10s` by default).
    pub reset_timeout: Duration,
    /// Maximum time for [`PostgresPool::close`] to drain active leases (`30s` by default).
    pub close_timeout: Duration,
    /// Optional maximum idle age before an idle socket is evicted on acquisition.
    pub max_idle_lifetime: Option<Duration>,
    /// Optional maximum total connection age before retirement.
    pub max_lifetime: Option<Duration>,
}

impl Default for PostgresPoolOptions {
    fn default() -> Self {
        Self {
            pooler_mode: PoolerMode::Direct,
            protocol_level_pooler_cache: false,
            statement_cache_size: 256,
            min_size: 1,
            max_size: 10,
            acquire_timeout: Duration::from_secs(30),
            reset_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(30),
            max_idle_lifetime: None,
            max_lifetime: None,
        }
    }
}

impl PostgresPoolOptions {
    /// Validate pool sizing and timeout invariants in memory.
    pub fn validate(&self) -> Result<(), PostgresError> {
        if self.max_size == 0 {
            return Err(PostgresError::Config(
                "PostgresPoolOptions.max_size must be >= 1".to_string(),
            ));
        }
        if self.min_size > self.max_size {
            return Err(PostgresError::Config(format!(
                "PostgresPoolOptions.min_size ({}) cannot exceed max_size ({})",
                self.min_size, self.max_size
            )));
        }
        if self.acquire_timeout.is_zero() {
            return Err(PostgresError::Config(
                "PostgresPoolOptions.acquire_timeout must be > 0".to_string(),
            ));
        }
        if self.reset_timeout.is_zero() {
            return Err(PostgresError::Config(
                "PostgresPoolOptions.reset_timeout must be > 0".to_string(),
            ));
        }
        if self.close_timeout.is_zero() {
            return Err(PostgresError::Config(
                "PostgresPoolOptions.close_timeout must be > 0".to_string(),
            ));
        }
        if let Some(idle) = self.max_idle_lifetime
            && idle.is_zero()
        {
            return Err(PostgresError::Config(
                "PostgresPoolOptions.max_idle_lifetime must be > 0 when set".to_string(),
            ));
        }
        if let Some(lt) = self.max_lifetime
            && lt.is_zero()
        {
            return Err(PostgresError::Config(
                "PostgresPoolOptions.max_lifetime must be > 0 when set".to_string(),
            ));
        }
        Ok(())
    }
}

/// Instantaneous capacity and lifecycle counters for a [`PostgresPool`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Physical sockets currently dialing or authenticating.
    pub connecting: usize,
    /// Authenticated sockets sitting idle in the pool queue.
    pub idle: usize,
    /// Leases currently checked out to callers.
    pub borrowed: usize,
    /// Returned leases currently executing bounded reset (`ROLLBACK` / `DISCARD ALL`).
    pub resetting: usize,
    /// Cumulative count of sockets permanently closed and retired.
    pub retired: u64,
}

/// Opaque handle to an event-loop socket reader registered for a pooled connection.
///
/// Callers must detach via [`LoopReaderSlot::detach_async`] outside any pool lock before
/// closing or reusing the underlying socket on Tokio.
pub(crate) trait LoopReaderSlot: std::any::Any + Send + Sync + std::fmt::Debug {
    #[cfg(unix)]
    fn as_any(&self) -> &dyn std::any::Any;

    /// Schedule detaching this reader from its Python event loop and return a oneshot receiver
    /// that completes once `remove_reader(fd)` has run (or `None` if already detached or closed).
    fn detach_async(&self) -> Option<tokio::sync::oneshot::Receiver<()>>;
}

async fn detach_reader_slot(slot: Option<Box<dyn LoopReaderSlot>>) {
    if let Some(reader) = slot {
        if let Some(rx) = reader.detach_async() {
            let _ = rx.await;
        }
        drop(reader);
    }
}

#[derive(Debug)]
struct IdleEntry {
    conn: PostgresConnection,
    created_at: Instant,
    idle_since: Instant,
    loop_reader: Option<Box<dyn LoopReaderSlot>>,
}

#[derive(Debug)]
struct PoolInner {
    connecting: usize,
    idle: VecDeque<IdleEntry>,
    borrowed: usize,
    resetting: usize,
    retired: u64,
    next_generation: u64,
    closed: bool,
    retire_all_borrowed: bool,
}

impl PoolInner {
    fn active_slots(&self) -> usize {
        self.connecting + self.idle.len() + self.borrowed + self.resetting
    }

    fn stats(&self) -> PoolStats {
        PoolStats {
            connecting: self.connecting,
            idle: self.idle.len(),
            borrowed: self.borrowed,
            resetting: self.resetting,
            retired: self.retired,
        }
    }
}

/// Concrete physical PostgreSQL connection pool owning authenticated [`PostgresConnection`] sockets.
pub struct PostgresPool {
    config: PostgresConfig,
    options: PostgresPoolOptions,
    cloud_connector: Arc<CloudConnector>,
    token_provider: ScopedTokenProvider,
    clock: Arc<dyn Clock>,
    inner: Mutex<PoolInner>,
    slot_notify: Notify,
    drain_notify: Notify,
    warmup_started: AtomicBool,
}

impl std::fmt::Debug for PostgresPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresPool")
            .field("config", &self.config)
            .field("options", &self.options)
            .field("stats", &self.stats())
            .finish()
    }
}

impl PostgresPool {
    /// Validate configuration and construct a [`PostgresPool`] with zero network or credential I/O.
    pub fn new(
        config: PostgresConfig,
        options: PostgresPoolOptions,
    ) -> Result<Arc<Self>, PostgresError> {
        Self::with_clock(config, options, Arc::new(SystemClock))
    }

    /// Validate configuration and construct a [`PostgresPool`] with an injected [`Clock`].
    pub fn with_clock(
        mut config: PostgresConfig,
        options: PostgresPoolOptions,
        clock: Arc<dyn Clock>,
    ) -> Result<Arc<Self>, PostgresError> {
        options.validate()?;
        config.pooler_mode = options.pooler_mode;
        config.protocol_level_pooler_cache = options.protocol_level_pooler_cache;
        config.statement_cache_size = options.statement_cache_size;

        let cloud_connector = Arc::new(CloudConnector::with_clock(Arc::clone(&clock)));
        let token_provider = ScopedTokenProvider::new(
            Some(config.cloud_auth_options.clone()),
            (!config.iam_auth_options.is_empty()).then(|| config.iam_auth_options.clone()),
            Arc::clone(&clock),
        );

        Ok(Arc::new(Self {
            config,
            options,
            cloud_connector,
            token_provider,
            clock,
            inner: Mutex::new(PoolInner {
                connecting: 0,
                idle: VecDeque::new(),
                borrowed: 0,
                resetting: 0,
                retired: 0,
                next_generation: 0,
                closed: false,
                retire_all_borrowed: false,
            }),
            slot_notify: Notify::new(),
            drain_notify: Notify::new(),
            warmup_started: AtomicBool::new(false),
        }))
    }

    /// Borrow the pool's canonical [`PostgresConfig`].
    pub const fn config(&self) -> &PostgresConfig {
        &self.config
    }

    pub(crate) const fn cloud_connector(&self) -> &Arc<CloudConnector> {
        &self.cloud_connector
    }

    /// Borrow the pool's validated [`PostgresPoolOptions`].
    pub const fn options(&self) -> &PostgresPoolOptions {
        &self.options
    }

    /// Return a snapshot of current [`PoolStats`].
    pub fn stats(&self) -> PoolStats {
        let guard = self.inner.lock().expect("PostgresPool lock poisoned");
        guard.stats()
    }

    /// Return `true` if the pool has been closed or is draining.
    pub fn is_closed(&self) -> bool {
        let guard = self.inner.lock().expect("PostgresPool lock poisoned");
        guard.closed
    }

    /// Compare candidate `config` and preparation policy against this pool's canonical configuration without I/O.
    pub fn validate_configuration(
        &self,
        config: &PostgresConfig,
        options: &PostgresPoolOptions,
    ) -> Result<(), PostgresError> {
        let mut candidate = config.clone();
        candidate.pooler_mode = options.pooler_mode;
        candidate.protocol_level_pooler_cache = options.protocol_level_pooler_cache;
        candidate.statement_cache_size = options.statement_cache_size;

        if self.config != candidate {
            return Err(PostgresError::Interface(
                "candidate PostgreSQL connection or preparation configuration does not match existing PostgresPool"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Synchronously pop a recently-used idle [`PooledConnection`] without entering Tokio.
    ///
    /// Returns `Ok(None)` when no fresh idle connection is available so the caller can fall back
    /// to [`PostgresPool::acquire`].
    #[cfg(unix)]
    pub(crate) fn try_acquire_idle(
        self: &Arc<Self>,
    ) -> Result<Option<PooledConnection>, PostgresError> {
        let mut expired_to_close: Vec<IdleEntry> = Vec::new();
        let mut acquired_idle: Option<IdleEntry> = None;
        {
            let mut guard = self.inner.lock().expect("PostgresPool lock poisoned");
            if guard.closed {
                return Err(PostgresError::Interface(
                    "cannot acquire connection from a closed PostgresPool".to_string(),
                ));
            }
            let now = Instant::now();
            while let Some(entry) = guard.idle.pop_back() {
                let lifetime_expired = self
                    .options
                    .max_lifetime
                    .is_some_and(|max_lt| now.duration_since(entry.created_at) >= max_lt);
                let idle_expired = self
                    .options
                    .max_idle_lifetime
                    .is_some_and(|max_idle| now.duration_since(entry.idle_since) >= max_idle);
                if lifetime_expired || idle_expired || entry.conn.is_closed() {
                    guard.retired += 1;
                    expired_to_close.push(entry);
                    continue;
                }
                let recently_used = now.duration_since(entry.idle_since) < Duration::from_secs(1);
                if !recently_used {
                    guard.idle.push_back(entry);
                    break;
                }
                guard.borrowed += 1;
                acquired_idle = Some(entry);
                break;
            }
        }
        for mut stale in expired_to_close {
            vertebra_db::py_async::db_runtime_handle().spawn(async move {
                detach_reader_slot(stale.loop_reader.take()).await;
                let _ = stale.conn.close().await;
            });
        }
        Ok(acquired_idle.map(|entry| PooledConnection {
            pool: Arc::clone(self),
            conn: Some(entry.conn),
            created_at: entry.created_at,
            retire: false,
            loop_reader: entry.loop_reader,
        }))
    }

    /// Acquire a physical [`PooledConnection`] lease within `options.acquire_timeout`.
    pub async fn acquire(self: &Arc<Self>) -> Result<PooledConnection, PostgresError> {
        let acquire_deadline = Instant::now() + self.options.acquire_timeout;
        let is_first_acquire = self
            .warmup_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();

        let lease = self.acquire_single(acquire_deadline).await?;

        if is_first_acquire && self.options.min_size > 1 {
            self.warm_to_min_size(acquire_deadline).await;
        }

        Ok(lease)
    }

    /// Execute a single extended query directly on a pooled connection, returning it straight to
    /// the idle queue without `DISCARD ALL` when it completes cleanly in `Idle` state with a
    /// non-session-mutating command tag.
    pub async fn query_oneshot(
        self: &Arc<Self>,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
        budget: crate::session::CommandBudget,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        let mut lease = self.acquire().await?;
        let (res, timed_out) = {
            let mut dl_guard = lease.enter_session_command_deadline(budget.deadline());
            let res = dl_guard.query(sql, parameters, parameter_oids).await;
            (res, dl_guard.timed_out())
        };
        match res {
            Ok(query_result) => {
                let is_clean = !timed_out
                    && !lease.is_closed()
                    && lease.exchange_state == ExchangeState::Idle
                    && lease.transaction_status == crate::session::TransactionStatus::Idle
                    && lease.open_portals.is_empty()
                    && is_clean_oneshot_query(sql, &query_result.command_tag);
                if is_clean {
                    lease.return_clean();
                } else {
                    let _ = lease.release().await;
                }
                Ok(query_result)
            }
            Err(err) => {
                if timed_out || lease.is_closed() {
                    lease.mark_retire();
                }
                let _ = lease.release().await;
                Err(err)
            }
        }
    }

    async fn warm_to_min_size(self: &Arc<Self>, deadline: Instant) {
        let mut handles = Vec::new();
        loop {
            let gen_opt = {
                let mut guard = self.inner.lock().expect("PostgresPool lock poisoned");
                if guard.closed
                    || guard.active_slots() >= self.options.min_size
                    || guard.active_slots() >= self.options.max_size
                {
                    None
                } else {
                    guard.connecting += 1;
                    guard.next_generation = guard.next_generation.saturating_add(1);
                    Some(guard.next_generation)
                }
            };
            let Some(generation) = gen_opt else {
                break;
            };
            let pool = Arc::clone(self);
            handles.push(tokio::spawn(async move {
                let mut slot_guard = ConnectingSlotGuard::new(Arc::clone(&pool));
                let dial_deadline = deadline.min(Instant::now() + pool.config.connect_timeout);
                if let Ok(conn) = establish_postgres_connection(
                    &pool.config,
                    &pool.cloud_connector,
                    &pool.token_provider,
                    pool.clock.as_ref(),
                    generation,
                    dial_deadline,
                )
                .await
                {
                    let now = Instant::now();
                    let mut to_close: Option<PostgresConnection> = None;
                    {
                        let mut guard = pool.inner.lock().expect("PostgresPool lock poisoned");
                        slot_guard.disarm();
                        guard.connecting = guard.connecting.saturating_sub(1);
                        if guard.closed || guard.retire_all_borrowed {
                            guard.retired += 1;
                            to_close = Some(conn);
                        } else {
                            guard.idle.push_back(IdleEntry {
                                conn,
                                created_at: now,
                                idle_since: now,
                                loop_reader: None,
                            });
                        }
                    }
                    if let Some(mut c) = to_close {
                        let _ = c.close().await;
                        pool.drain_notify.notify_waiters();
                    } else {
                        pool.slot_notify.notify_one();
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }

    async fn acquire_single(
        self: &Arc<Self>,
        acquire_deadline: Instant,
    ) -> Result<PooledConnection, PostgresError> {
        let mut first_attempt = true;
        loop {
            let mut expired_to_close: Vec<IdleEntry> = Vec::new();
            let mut dial_generation: Option<u64> = None;
            let mut acquired_idle: Option<IdleEntry> = None;

            if first_attempt {
                first_attempt = false;
                {
                    let mut guard = self.inner.lock().expect("PostgresPool lock poisoned");
                    if guard.closed {
                        return Err(PostgresError::Interface(
                            "cannot acquire connection from a closed PostgresPool".to_string(),
                        ));
                    }
                    let now = Instant::now();
                    while let Some(mut entry) = guard.idle.pop_back() {
                        let lifetime_expired = self
                            .options
                            .max_lifetime
                            .is_some_and(|max_lt| now.duration_since(entry.created_at) >= max_lt);
                        let idle_expired = self.options.max_idle_lifetime.is_some_and(|max_idle| {
                            now.duration_since(entry.idle_since) >= max_idle
                        });
                        let recently_used =
                            now.duration_since(entry.idle_since) < Duration::from_secs(1);
                        let broken = entry.conn.is_closed()
                            || (!recently_used && !check_connection_idle_alive(&mut entry.conn));

                        if lifetime_expired || idle_expired || broken {
                            guard.retired += 1;
                            expired_to_close.push(entry);
                            continue;
                        }

                        guard.borrowed += 1;
                        acquired_idle = Some(entry);
                        break;
                    }
                }
                for mut stale in expired_to_close.drain(..) {
                    tokio::spawn(async move {
                        detach_reader_slot(stale.loop_reader.take()).await;
                        let _ = stale.conn.close().await;
                    });
                }
                if let Some(mut entry) = acquired_idle {
                    detach_reader_slot(entry.loop_reader.take()).await;
                    return Ok(PooledConnection {
                        pool: Arc::clone(self),
                        conn: Some(entry.conn),
                        created_at: entry.created_at,
                        retire: false,
                        loop_reader: None,
                    });
                }
            }

            let notified = self.slot_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut guard = self.inner.lock().expect("PostgresPool lock poisoned");
                if guard.closed {
                    return Err(PostgresError::Interface(
                        "cannot acquire connection from a closed PostgresPool".to_string(),
                    ));
                }

                let now = Instant::now();
                while let Some(mut entry) = guard.idle.pop_back() {
                    let lifetime_expired = self
                        .options
                        .max_lifetime
                        .is_some_and(|max_lt| now.duration_since(entry.created_at) >= max_lt);
                    let idle_expired = self
                        .options
                        .max_idle_lifetime
                        .is_some_and(|max_idle| now.duration_since(entry.idle_since) >= max_idle);
                    let recently_used =
                        now.duration_since(entry.idle_since) < Duration::from_secs(1);
                    let broken = entry.conn.is_closed()
                        || (!recently_used && !check_connection_idle_alive(&mut entry.conn));

                    if lifetime_expired || idle_expired || broken {
                        guard.retired += 1;
                        expired_to_close.push(entry);
                        continue;
                    }

                    guard.borrowed += 1;
                    acquired_idle = Some(entry);
                    break;
                }

                if acquired_idle.is_none() && guard.active_slots() < self.options.max_size {
                    guard.connecting += 1;
                    guard.next_generation = guard.next_generation.saturating_add(1);
                    dial_generation = Some(guard.next_generation);
                }
            }

            for mut stale in expired_to_close {
                tokio::spawn(async move {
                    detach_reader_slot(stale.loop_reader.take()).await;
                    let _ = stale.conn.close().await;
                });
            }

            if let Some(mut entry) = acquired_idle {
                detach_reader_slot(entry.loop_reader.take()).await;
                return Ok(PooledConnection {
                    pool: Arc::clone(self),
                    conn: Some(entry.conn),
                    created_at: entry.created_at,
                    retire: false,
                    loop_reader: None,
                });
            }

            if let Some(generation) = dial_generation {
                let mut slot_guard = ConnectingSlotGuard::new(Arc::clone(self));
                let now = Instant::now();
                if now >= acquire_deadline {
                    return Err(PostgresError::PoolExhausted(format!(
                        "PostgresPool acquire_timeout ({:?}) exceeded before connection setup",
                        self.options.acquire_timeout
                    )));
                }
                let remaining_acquire = acquire_deadline.duration_since(now);
                let timed_out_by_acquire = remaining_acquire < self.config.connect_timeout;
                let dial_deadline = acquire_deadline.min(now + self.config.connect_timeout);

                match establish_postgres_connection(
                    &self.config,
                    &self.cloud_connector,
                    &self.token_provider,
                    self.clock.as_ref(),
                    generation,
                    dial_deadline,
                )
                .await
                {
                    Ok(mut conn) => {
                        let created_at = Instant::now();
                        let mut closed_while_dialing = false;
                        {
                            let mut guard = self.inner.lock().expect("PostgresPool lock poisoned");
                            slot_guard.disarm();
                            guard.connecting = guard.connecting.saturating_sub(1);
                            if guard.closed {
                                guard.retired += 1;
                                closed_while_dialing = true;
                            } else {
                                guard.borrowed += 1;
                            }
                        }
                        if closed_while_dialing {
                            let _ = conn.close().await;
                            self.drain_notify.notify_waiters();
                            return Err(PostgresError::Interface(
                                "PostgresPool was closed while establishing connection".to_string(),
                            ));
                        }
                        return Ok(PooledConnection {
                            pool: Arc::clone(self),
                            conn: Some(conn),
                            created_at,
                            retire: false,
                            loop_reader: None,
                        });
                    }
                    Err(err) => {
                        if timed_out_by_acquire && Instant::now() >= acquire_deadline {
                            return Err(PostgresError::PoolExhausted(format!(
                                "PostgresPool acquire_timeout ({:?}) exceeded during connection setup: {err}",
                                self.options.acquire_timeout
                            )));
                        }
                        return Err(err);
                    }
                }
            }

            if Instant::now() >= acquire_deadline {
                return Err(PostgresError::PoolExhausted(format!(
                    "timed out after {:?} waiting for an available connection from PostgresPool (max_size={})",
                    self.options.acquire_timeout, self.options.max_size
                )));
            }

            if tokio::time::timeout_at(acquire_deadline, notified)
                .await
                .is_err()
            {
                return Err(PostgresError::PoolExhausted(format!(
                    "timed out after {:?} waiting for an available connection from PostgresPool (max_size={})",
                    self.options.acquire_timeout, self.options.max_size
                )));
            }
        }
    }

    /// Close the pool, rejecting new acquisitions, closing idle sockets, and awaiting active leases up to `deadline`.
    pub async fn close(&self, deadline: Duration) -> Result<(), PostgresError> {
        let close_deadline = Instant::now() + deadline;
        let drained_idle: Vec<IdleEntry> = {
            let mut guard = self.inner.lock().expect("PostgresPool lock poisoned");
            guard.closed = true;
            let count = guard.idle.len() as u64;
            guard.retired += count;
            guard.idle.drain(..).collect()
        };
        self.slot_notify.notify_waiters();

        for mut entry in drained_idle {
            let _ = tokio::time::timeout_at(
                close_deadline,
                detach_reader_slot(entry.loop_reader.take()),
            )
            .await;
            let _ = tokio::time::timeout_at(close_deadline, entry.conn.close()).await;
        }

        loop {
            let notified = self.drain_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let extra_idle: Vec<IdleEntry> = {
                let mut guard = self.inner.lock().expect("PostgresPool lock poisoned");
                let count = guard.idle.len() as u64;
                guard.retired += count;
                let extra: Vec<IdleEntry> = guard.idle.drain(..).collect();
                if extra.is_empty()
                    && guard.connecting == 0
                    && guard.borrowed == 0
                    && guard.resetting == 0
                {
                    return Ok(());
                }
                extra
            };

            for mut entry in extra_idle {
                let _ = tokio::time::timeout_at(
                    close_deadline,
                    detach_reader_slot(entry.loop_reader.take()),
                )
                .await;
                let _ = tokio::time::timeout_at(close_deadline, entry.conn.close()).await;
            }

            if Instant::now() >= close_deadline
                || tokio::time::timeout_at(close_deadline, notified)
                    .await
                    .is_err()
            {
                let mut guard = self.inner.lock().expect("PostgresPool lock poisoned");
                if guard.connecting == 0 && guard.borrowed == 0 && guard.resetting == 0 {
                    return Ok(());
                }
                guard.retire_all_borrowed = true;
                return Err(PostgresError::Operational(format!(
                    "PostgresPool::close timed out after {deadline:?} waiting for active leases to drain"
                )));
            }
        }
    }

    pub(crate) async fn reset_and_return_slot(
        self: &Arc<Self>,
        mut conn: PostgresConnection,
        created_at: Instant,
        force_retire: bool,
    ) -> Result<(), PostgresError> {
        let mut reset_guard = ResettingSlotGuard::new(Arc::clone(self));

        let lifetime_expired = self
            .options
            .max_lifetime
            .is_some_and(|max_lt| Instant::now().duration_since(created_at) >= max_lt);
        let pool_closing = {
            let guard = self.inner.lock().expect("PostgresPool lock poisoned");
            guard.closed || guard.retire_all_borrowed
        };

        if force_retire
            || lifetime_expired
            || pool_closing
            || conn.is_closed()
            || conn.exchange_state == ExchangeState::CopyActive
        {
            let _ = tokio::time::timeout(self.options.reset_timeout, conn.close()).await;
            return Ok(());
        }

        let reset_deadline = Instant::now() + self.options.reset_timeout;
        let reset_outcome =
            tokio::time::timeout_at(reset_deadline, async { conn.reset_for_pool_return().await })
                .await;

        match reset_outcome {
            Ok(Ok(())) if !conn.is_closed() && check_connection_idle_alive(&mut conn) => {
                let mut to_close: Option<PostgresConnection> = None;
                {
                    let mut guard = self.inner.lock().expect("PostgresPool lock poisoned");
                    reset_guard.disarm();
                    guard.resetting = guard.resetting.saturating_sub(1);
                    if guard.closed || guard.retire_all_borrowed {
                        guard.retired += 1;
                        to_close = Some(conn);
                    } else {
                        guard.idle.push_back(IdleEntry {
                            conn,
                            created_at,
                            idle_since: Instant::now(),
                            loop_reader: None,
                        });
                    }
                }
                if let Some(mut c) = to_close {
                    let _ = tokio::time::timeout(self.options.reset_timeout, c.close()).await;
                    self.drain_notify.notify_waiters();
                } else {
                    self.slot_notify.notify_one();
                    self.drain_notify.notify_waiters();
                }
                Ok(())
            }
            Ok(Err(err)) => {
                let _ = tokio::time::timeout(Duration::from_millis(200), conn.close()).await;
                Err(err)
            }
            Ok(Ok(())) => {
                let _ = tokio::time::timeout(Duration::from_millis(200), conn.close()).await;
                Err(PostgresError::Transport(
                    "connection closed by peer during pool reset".to_string(),
                ))
            }
            Err(_) => {
                let _ = tokio::time::timeout(Duration::from_millis(100), conn.close()).await;
                Err(PostgresError::Operational(format!(
                    "pooled connection reset timed out after {:?}",
                    self.options.reset_timeout
                )))
            }
        }
    }
}

struct ConnectingSlotGuard {
    pool: Option<Arc<PostgresPool>>,
}

impl ConnectingSlotGuard {
    fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool: Some(pool) }
    }

    fn disarm(&mut self) {
        self.pool = None;
    }
}

impl Drop for ConnectingSlotGuard {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            {
                let mut guard = pool.inner.lock().expect("PostgresPool lock poisoned");
                guard.connecting = guard.connecting.saturating_sub(1);
            }
            pool.slot_notify.notify_one();
            pool.drain_notify.notify_waiters();
        }
    }
}

struct ResettingSlotGuard {
    pool: Option<Arc<PostgresPool>>,
}

impl ResettingSlotGuard {
    fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool: Some(pool) }
    }

    fn disarm(&mut self) {
        self.pool = None;
    }
}

impl Drop for ResettingSlotGuard {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            {
                let mut guard = pool.inner.lock().expect("PostgresPool lock poisoned");
                guard.resetting = guard.resetting.saturating_sub(1);
                guard.retired += 1;
            }
            pool.slot_notify.notify_one();
            pool.drain_notify.notify_waiters();
        }
    }
}

/// Checked-out physical connection lease from a [`PostgresPool`].
#[derive(Debug)]
pub struct PooledConnection {
    pool: Arc<PostgresPool>,
    conn: Option<PostgresConnection>,
    created_at: Instant,
    retire: bool,
    loop_reader: Option<Box<dyn LoopReaderSlot>>,
}

impl PooledConnection {
    /// Borrow the optional event-loop reader slot attached to this pooled connection.
    #[cfg(unix)]
    pub(crate) fn loop_reader(&self) -> Option<&dyn LoopReaderSlot> {
        self.loop_reader.as_deref()
    }

    /// Take the optional event-loop reader slot off this pooled connection.
    #[cfg(unix)]
    pub(crate) fn take_loop_reader(&mut self) -> Option<Box<dyn LoopReaderSlot>> {
        self.loop_reader.take()
    }

    /// Attach an event-loop reader slot to this pooled connection (detaching any previous one).
    #[cfg(unix)]
    pub(crate) fn set_loop_reader(&mut self, slot: Box<dyn LoopReaderSlot>) {
        self.loop_reader = Some(slot);
    }

    /// Mark this lease to close and retire on return rather than re-entering the idle queue.
    pub fn mark_retire(&mut self) {
        self.retire = true;
    }

    /// Return `true` if this lease has been marked for retirement.
    pub const fn will_retire(&self) -> bool {
        self.retire
    }

    /// Borrow the owning [`PostgresPool`].
    pub const fn pool(&self) -> &Arc<PostgresPool> {
        &self.pool
    }

    /// Explicitly reset and return this lease to the pool under `options.reset_timeout`.
    pub async fn release(mut self) -> Result<(), PostgresError> {
        let loop_reader = self.loop_reader.take();
        let Some(conn) = self.conn.take() else {
            detach_reader_slot(loop_reader).await;
            return Ok(());
        };
        let pool = Arc::clone(&self.pool);
        let created_at = self.created_at;
        let retire = self.retire;
        {
            let mut guard = pool.inner.lock().expect("PostgresPool lock poisoned");
            guard.borrowed = guard.borrowed.saturating_sub(1);
            guard.resetting += 1;
        }
        detach_reader_slot(loop_reader).await;
        pool.reset_and_return_slot(conn, created_at, retire).await
    }

    /// Return a verified-clean `Idle` connection straight to the idle queue without `DISCARD ALL`.
    pub(crate) fn return_clean(mut self) {
        let Some(conn) = self.conn.take() else {
            drop(self.loop_reader.take());
            return;
        };
        let loop_reader = self.loop_reader.take();
        let pool = Arc::clone(&self.pool);
        let created_at = self.created_at;
        let now = Instant::now();
        let lifetime_expired = pool
            .options
            .max_lifetime
            .is_some_and(|max_lt| now.duration_since(created_at) >= max_lt);
        let mut to_close: Option<(PostgresConnection, Option<Box<dyn LoopReaderSlot>>)> = None;
        {
            let mut guard = pool.inner.lock().expect("PostgresPool lock poisoned");
            guard.borrowed = guard.borrowed.saturating_sub(1);
            if guard.closed || guard.retire_all_borrowed || lifetime_expired || self.retire {
                guard.retired += 1;
                to_close = Some((conn, loop_reader));
            } else {
                guard.idle.push_back(IdleEntry {
                    conn,
                    created_at,
                    idle_since: now,
                    loop_reader,
                });
            }
        }
        if let Some((mut c, reader)) = to_close {
            let close_timeout = pool.options.reset_timeout;
            let pool_for_close = Arc::clone(&pool);
            let task = async move {
                let _ = tokio::time::timeout(close_timeout, detach_reader_slot(reader)).await;
                let _ = tokio::time::timeout(close_timeout, c.close()).await;
                pool_for_close.drain_notify.notify_waiters();
            };
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(task);
            } else {
                vertebra_db::py_async::db_runtime_handle().spawn(task);
            }
        } else {
            pool.slot_notify.notify_one();
            pool.drain_notify.notify_waiters();
        }
    }
}

impl Deref for PooledConnection {
    type Target = PostgresConnection;

    fn deref(&self) -> &Self::Target {
        self.conn
            .as_ref()
            .expect("PooledConnection dereferenced after release")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
            .as_mut()
            .expect("PooledConnection dereferenced after release")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        let loop_reader = self.loop_reader.take();
        let Some(conn) = self.conn.take() else {
            drop(loop_reader);
            return;
        };
        let pool = Arc::clone(&self.pool);
        let created_at = self.created_at;
        let retire = self.retire;
        {
            let mut guard = pool.inner.lock().expect("PostgresPool lock poisoned");
            guard.borrowed = guard.borrowed.saturating_sub(1);
            guard.resetting += 1;
        }
        let task = async move {
            detach_reader_slot(loop_reader).await;
            let _ = pool.reset_and_return_slot(conn, created_at, retire).await;
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(task);
        } else {
            vertebra_db::py_async::db_runtime_handle().spawn(task);
        }
    }
}

fn contains_ascii_icase(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

pub(crate) fn is_clean_oneshot_query(sql: &str, command_tag: &str) -> bool {
    let verb = command_tag.split_whitespace().next().unwrap_or("");
    let safe_tag = verb.eq_ignore_ascii_case("SELECT")
        || verb.eq_ignore_ascii_case("INSERT")
        || verb.eq_ignore_ascii_case("UPDATE")
        || verb.eq_ignore_ascii_case("DELETE")
        || verb.eq_ignore_ascii_case("MERGE")
        || verb.eq_ignore_ascii_case("VALUES")
        || verb.eq_ignore_ascii_case("SHOW")
        || verb.eq_ignore_ascii_case("EXPLAIN");
    if !safe_tag {
        return false;
    }
    let bytes = sql.as_bytes();
    !contains_ascii_icase(bytes, b"set_config")
        && !contains_ascii_icase(bytes, b"advisory")
        && !contains_ascii_icase(bytes, b"temp")
        && !contains_ascii_icase(bytes, b"unlogged")
}

fn check_connection_idle_alive(conn: &mut PostgresConnection) -> bool {
    if conn.is_closed() || conn.exchange_state != ExchangeState::Idle {
        return false;
    }
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut probe = [0u8; 1];
    let mut read_buf = ReadBuf::new(&mut probe);
    match Pin::new(&mut conn.stream).poll_read(&mut cx, &mut read_buf) {
        Poll::Pending => true,
        Poll::Ready(Ok(())) => {
            if read_buf.filled().is_empty() {
                conn.mark_closed();
                false
            } else {
                conn.read_buf.extend_from_slice(read_buf.filled());
                true
            }
        }
        Poll::Ready(Err(_)) => {
            conn.mark_closed();
            false
        }
    }
}
