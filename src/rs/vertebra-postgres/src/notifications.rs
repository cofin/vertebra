//! Dedicated non-pooled PostgreSQL `LISTEN`/`NOTIFY` session pump with bounded queue overflow
//! termination and optional recoverable-disconnect reconnect gap tracking.

use std::collections::{HashSet, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use postgres_protocol::message::backend::{Message, NotificationResponseBody};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::config::PostgresConfig;
use crate::errors::PostgresError;
use crate::protocol::parse_error_fields;
use crate::result::TransactionStatus;
use crate::session::statements::PoolerMode;
use crate::session::{PostgresConnection, establish_standalone_connection};

/// Default maximum number of buffered [`NotificationFrame`] items before fail-closed overflow (`1024`).
pub const DEFAULT_NOTIFICATION_QUEUE_CAPACITY: usize = 1024;

/// Default maximum cumulative buffered payload bytes before fail-closed overflow (`8 MiB`).
pub const DEFAULT_NOTIFICATION_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Maximum UTF-8 byte length of a PostgreSQL notification channel identifier (`NAMEDATALEN - 1`).
pub const MAX_CHANNEL_BYTES: usize = 63;

/// Maximum consecutive reconnect attempts on recoverable transport drops when `reconnect = true`.
pub const MAX_RECONNECT_ATTEMPTS: u32 = 8;

const CONTROL_CHANNEL_CAPACITY: usize = 32;
const RECONNECT_BASE_DELAY_MS: u64 = 100;
const RECONNECT_MAX_DELAY_MS: u64 = 5_000;

/// Single asynchronous PostgreSQL `NotificationResponse` (`'A'`) frame received from a backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationFrame {
    /// Backend process ID (`be_pid`) that emitted `NOTIFY` / `pg_notify`.
    pub pid: i32,
    /// Exact channel identifier reported by PostgreSQL.
    pub channel: String,
    /// Raw UTF-8 notification payload bytes (`b""` when `NOTIFY` had no payload).
    pub payload: Bytes,
    /// `true` on the first delivered frame following one or more automatic reconnects.
    pub reconnected_gap: bool,
}

/// Queue bounds and reconnect configuration for [`PostgresListener::open`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListenerOptions {
    /// Maximum buffered notifications before terminating with `PostgresError::Operational`.
    pub queue_capacity: usize,
    /// Maximum cumulative buffered payload bytes before terminating with `PostgresError::Operational`.
    pub max_queue_bytes: usize,
    /// Whether recoverable transport drops retry up to [`MAX_RECONNECT_ATTEMPTS`] times with resubscribe.
    pub reconnect: bool,
}

impl Default for ListenerOptions {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_NOTIFICATION_QUEUE_CAPACITY,
            max_queue_bytes: DEFAULT_NOTIFICATION_MAX_BYTES,
            reconnect: false,
        }
    }
}

/// Validate a PostgreSQL `LISTEN`/`UNLISTEN` channel name against SQL identifier syntax and the
/// `1..=63` UTF-8 byte (`NAMEDATALEN - 1`) bound, returning a double-quoted identifier.
pub fn validate_and_quote_channel(channel: &str) -> Result<String, PostgresError> {
    let byte_len = channel.len();
    if byte_len == 0 {
        return Err(PostgresError::Data(
            "PostgreSQL notification channel name cannot be empty".to_string(),
        ));
    }
    if byte_len > MAX_CHANNEL_BYTES {
        return Err(PostgresError::Data(format!(
            "PostgreSQL notification channel {channel:?} is {byte_len} UTF-8 bytes (exceeds maximum {MAX_CHANNEL_BYTES})"
        )));
    }
    if channel.as_bytes().contains(&0) {
        return Err(PostgresError::Data(
            "PostgreSQL notification channel name cannot contain embedded NUL byte".to_string(),
        ));
    }

    let mut chars = channel.chars();
    let Some(first) = chars.next() else {
        return Err(PostgresError::Data(
            "PostgreSQL notification channel name cannot be empty".to_string(),
        ));
    };
    if !is_valid_channel_start(first) || !chars.all(is_valid_channel_continue) {
        return Err(PostgresError::Data(format!(
            "invalid PostgreSQL notification channel identifier {channel:?}"
        )));
    }

    let mut quoted = String::with_capacity(byte_len + 2);
    quoted.push('"');
    for ch in channel.chars() {
        if ch == '"' {
            quoted.push_str("\"\"");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('"');
    Ok(quoted)
}

/// Dedicated PostgreSQL `LISTEN`/`NOTIFY` client backed by a standalone `Direct`/`Session` socket
/// and a single Tokio receive/control pump task.
pub struct PostgresListener {
    shared: Arc<ListenerShared>,
    ctrl_tx: mpsc::Sender<ControlCommand>,
}

impl std::fmt::Debug for PostgresListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresListener")
            .field("closed", &self.is_closed())
            .field("options", &self.shared.options)
            .finish()
    }
}

impl PostgresListener {
    /// Open a dedicated non-pooled PostgreSQL listener connection, subscribe `initial_channels`
    /// through `ReadyForQuery` when non-empty, and spawn the single receive/control pump task.
    pub async fn open(
        config: PostgresConfig,
        initial_channels: Vec<String>,
        options: ListenerOptions,
    ) -> Result<Self, PostgresError> {
        if options.queue_capacity == 0 {
            return Err(PostgresError::Interface(
                "notification listener queue_capacity must be greater than 0".to_string(),
            ));
        }
        if options.max_queue_bytes == 0 {
            return Err(PostgresError::Interface(
                "notification listener max_queue_bytes must be greater than 0".to_string(),
            ));
        }
        if !matches!(config.pooler_mode, PoolerMode::Direct | PoolerMode::Session) {
            return Err(PostgresError::Interface(format!(
                "PostgreSQL LISTEN/NOTIFY requires explicit pooler_mode `direct` or `session`, got `{}`",
                config.pooler_mode.as_str()
            )));
        }

        let validated_initial = validate_and_dedup_channels(&initial_channels)?;
        let mut conn = establish_standalone_connection(&config, false).await?;
        let shared = Arc::new(ListenerShared::new(config.connect_timeout, options));
        let mut active = ActiveChannels::new();
        let mut pending_gap = false;

        for (raw, quoted) in validated_initial {
            let sql = format!("LISTEN {quoted}");
            match execute_control_sql(
                &mut conn,
                &sql,
                config.connect_timeout,
                &shared,
                false,
                &mut pending_gap,
            )
            .await
            {
                ControlStepOutcome::Completed => {
                    active.insert(raw, quoted);
                }
                ControlStepOutcome::Fatal(err) | ControlStepOutcome::Recoverable(err) => {
                    let _ = tokio::time::timeout(config.connect_timeout, conn.close()).await;
                    return Err(err);
                }
                ControlStepOutcome::UserClosed => {
                    let _ = tokio::time::timeout(config.connect_timeout, conn.close()).await;
                    return Err(PostgresError::Interface(
                        "PostgreSQL notification listener closed during startup".to_string(),
                    ));
                }
            }
        }

        let (ctrl_tx, ctrl_rx) = mpsc::channel(CONTROL_CHANNEL_CAPACITY);
        let pump_shared = Arc::clone(&shared);
        let join_handle = runtime_handle().spawn(async move {
            ListenerPump::new(config, options, active, ctrl_rx, pump_shared)
                .run(conn)
                .await;
        });
        {
            let mut guard = shared.pump_handle.lock().unwrap_or_else(|e| e.into_inner());
            *guard = Some(join_handle);
        }

        Ok(Self { shared, ctrl_tx })
    }

    /// Return a non-blocking snapshot of whether this listener has permanently terminated or closed.
    ///
    /// Remains `false` while actively receiving frames or retrying a recoverable disconnect with
    /// `reconnect = true`.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// Await the next delivered [`NotificationFrame`], returning `Ok(None)` after [`Self::close`].
    ///
    /// Enforces a strict single-waiter invariant: concurrent calls fail immediately with
    /// [`PostgresError::Interface`].
    pub async fn next(&self) -> Result<Option<NotificationFrame>, PostgresError> {
        let _waiter = NextWaiterGuard::acquire(&self.shared)?;
        loop {
            let notified = self.shared.data_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut guard = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
                if guard.closed_by_user {
                    return Ok(None);
                }
                if let Some(frame) = guard.front_unconsumed.take() {
                    return Ok(Some(frame));
                }
                if let Some(frame) = guard.queue.pop_front() {
                    guard.queued_payload_bytes = guard
                        .queued_payload_bytes
                        .saturating_sub(frame.payload.len());
                    return Ok(Some(frame));
                }
                if let Some(ref err) = guard.terminal_error {
                    return Err(err.clone());
                }
            }

            notified.await;
        }
    }

    /// Restore an unconsumed [`NotificationFrame`] (preserving `reconnected_gap`) to the dedicated
    /// 1-slot front buffer when a caller task cancels delivery while the listener remains open.
    pub fn push_front_unconsumed(&self, frame: NotificationFrame) {
        let wake = {
            let mut guard = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
            if guard.closed_by_user || guard.terminal_error.is_some() {
                false
            } else {
                if let Some(displaced) = guard.front_unconsumed.replace(frame) {
                    guard.queued_payload_bytes = guard
                        .queued_payload_bytes
                        .saturating_add(displaced.payload.len());
                    guard.queue.push_front(displaced);
                }
                true
            }
        };
        if wake {
            self.shared.data_notify.notify_waiters();
        }
    }

    /// Atomically validate `channels`, issue `LISTEN "<channel>"` for newly added channels, and
    /// resolve once the server responds with `ReadyForQuery`.
    pub async fn subscribe(&self, channels: Vec<String>) -> Result<(), PostgresError> {
        let validated = validate_and_dedup_channels(&channels)?;
        if self.is_closed() {
            return Err(PostgresError::Interface(
                "PostgreSQL notification listener is closed".to_string(),
            ));
        }
        if validated.is_empty() {
            return Ok(());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.ctrl_tx
            .send(ControlCommand::Subscribe {
                channels: validated,
                reply: reply_tx,
            })
            .await
            .map_err(|_| {
                PostgresError::Interface("PostgreSQL notification listener is closed".to_string())
            })?;
        reply_rx.await.unwrap_or_else(|_| {
            Err(PostgresError::Interface(
                "PostgreSQL notification listener closed before LISTEN completed".to_string(),
            ))
        })
    }

    /// Atomically validate `channels`, issue `UNLISTEN "<channel>"` for currently active channels,
    /// and resolve once the server responds with `ReadyForQuery`.
    pub async fn unsubscribe(&self, channels: Vec<String>) -> Result<(), PostgresError> {
        let validated = validate_and_dedup_channels(&channels)?;
        if self.is_closed() {
            return Err(PostgresError::Interface(
                "PostgreSQL notification listener is closed".to_string(),
            ));
        }
        if validated.is_empty() {
            return Ok(());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.ctrl_tx
            .send(ControlCommand::Unsubscribe {
                channels: validated,
                reply: reply_tx,
            })
            .await
            .map_err(|_| {
                PostgresError::Interface("PostgreSQL notification listener is closed".to_string())
            })?;
        reply_rx.await.unwrap_or_else(|_| {
            Err(PostgresError::Interface(
                "PostgreSQL notification listener closed before UNLISTEN completed".to_string(),
            ))
        })
    }

    /// Idempotently shut down the notification pump, aborting active receive or reconnect waits,
    /// clearing buffered notifications, waking any pending [`Self::next`] waiter with `Ok(None)`,
    /// and joining the pump within `connect_timeout`.
    pub async fn close(&self) -> Result<(), PostgresError> {
        self.shared.initiate_user_close();

        let handle_opt = {
            let mut guard = self
                .shared
                .pump_handle
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.take()
        };

        if let Some(mut handle) = handle_opt {
            if tokio::time::timeout(self.shared.connect_timeout, &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
            self.shared.mark_pump_finished();
        } else {
            self.shared.wait_for_pump_finished().await;
        }
        Ok(())
    }
}

impl Drop for PostgresListener {
    fn drop(&mut self) {
        self.shared.initiate_user_close();
        let handle_opt = {
            let mut guard = self
                .shared
                .pump_handle
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        if let Some(mut handle) = handle_opt {
            if handle.is_finished() {
                self.shared.mark_pump_finished();
                return;
            }
            let shared = Arc::clone(&self.shared);
            let timeout = shared.connect_timeout;
            runtime_handle().spawn(async move {
                if tokio::time::timeout(timeout, &mut handle).await.is_err() {
                    handle.abort();
                    let _ = handle.await;
                }
                shared.mark_pump_finished();
            });
        }
    }
}

enum ControlCommand {
    Subscribe {
        channels: Vec<(String, String)>,
        reply: oneshot::Sender<Result<(), PostgresError>>,
    },
    Unsubscribe {
        channels: Vec<(String, String)>,
        reply: oneshot::Sender<Result<(), PostgresError>>,
    },
}

struct ListenerState {
    front_unconsumed: Option<NotificationFrame>,
    queue: VecDeque<NotificationFrame>,
    queued_payload_bytes: usize,
    closed_by_user: bool,
    terminal_error: Option<PostgresError>,
    waiter_active: bool,
}

struct ListenerShared {
    connect_timeout: Duration,
    options: ListenerOptions,
    closed: AtomicBool,
    user_close_requested: AtomicBool,
    pump_finished: AtomicBool,
    state: Mutex<ListenerState>,
    data_notify: Notify,
    close_notify: Notify,
    done_notify: Notify,
    pump_handle: Mutex<Option<JoinHandle<()>>>,
}

impl ListenerShared {
    fn new(connect_timeout: Duration, options: ListenerOptions) -> Self {
        let initial_cap = options.queue_capacity.min(64);
        Self {
            connect_timeout,
            options,
            closed: AtomicBool::new(false),
            user_close_requested: AtomicBool::new(false),
            pump_finished: AtomicBool::new(false),
            state: Mutex::new(ListenerState {
                front_unconsumed: None,
                queue: VecDeque::with_capacity(initial_cap),
                queued_payload_bytes: 0,
                closed_by_user: false,
                terminal_error: None,
                waiter_active: false,
            }),
            data_notify: Notify::new(),
            close_notify: Notify::new(),
            done_notify: Notify::new(),
            pump_handle: Mutex::new(None),
        }
    }

    fn initiate_user_close(&self) {
        self.user_close_requested.store(true, Ordering::Release);
        self.closed.store(true, Ordering::Release);
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            guard.closed_by_user = true;
            guard.terminal_error = None;
            guard.front_unconsumed = None;
            guard.queue.clear();
            guard.queued_payload_bytes = 0;
        }
        self.close_notify.notify_waiters();
        self.data_notify.notify_waiters();
    }

    async fn wait_for_user_close(&self) {
        loop {
            let notified = self.close_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.user_close_requested.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn mark_pump_finished(&self) {
        self.pump_finished.store(true, Ordering::Release);
        self.done_notify.notify_waiters();
    }

    async fn wait_for_pump_finished(&self) {
        let wait = async {
            loop {
                let notified = self.done_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.pump_finished.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(self.connect_timeout, wait).await;
    }

    fn try_enqueue(&self, frame: NotificationFrame) -> Result<(), PostgresError> {
        let mut overflow_err: Option<PostgresError> = None;
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if guard.closed_by_user || guard.terminal_error.is_some() {
                return Ok(());
            }
            let frame_overflow = guard.queue.len() >= self.options.queue_capacity;
            let byte_overflow = guard
                .queued_payload_bytes
                .checked_add(frame.payload.len())
                .is_none_or(|total| total > self.options.max_queue_bytes);
            if frame_overflow || byte_overflow {
                let err = PostgresError::Operational(format!(
                    "notification queue overflow (capacity={} frames, max_bytes={} bytes)",
                    self.options.queue_capacity, self.options.max_queue_bytes
                ));
                guard.front_unconsumed = None;
                guard.queue.clear();
                guard.queued_payload_bytes = 0;
                guard.terminal_error = Some(err.clone());
                self.closed.store(true, Ordering::Release);
                overflow_err = Some(err);
            } else {
                guard.queued_payload_bytes += frame.payload.len();
                guard.queue.push_back(frame);
            }
        }
        self.data_notify.notify_waiters();
        match overflow_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn fail_terminal(&self, err: PostgresError) {
        let should_wake = {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if guard.closed_by_user || guard.terminal_error.is_some() {
                self.closed.store(true, Ordering::Release);
                false
            } else {
                guard.terminal_error = Some(err);
                self.closed.store(true, Ordering::Release);
                true
            }
        };
        if should_wake {
            self.data_notify.notify_waiters();
        }
    }
}

struct NextWaiterGuard<'a> {
    shared: &'a ListenerShared,
}

impl<'a> NextWaiterGuard<'a> {
    fn acquire(shared: &'a ListenerShared) -> Result<Self, PostgresError> {
        let mut guard = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if guard.waiter_active {
            return Err(PostgresError::Interface(
                "concurrent PostgresListener::next() calls are not permitted".to_string(),
            ));
        }
        guard.waiter_active = true;
        Ok(Self { shared })
    }
}

impl Drop for NextWaiterGuard<'_> {
    fn drop(&mut self) {
        let mut guard = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.waiter_active = false;
    }
}

struct ActiveChannels {
    order: Vec<(String, String)>,
    seen: HashSet<String>,
}

impl ActiveChannels {
    fn new() -> Self {
        Self {
            order: Vec::new(),
            seen: HashSet::new(),
        }
    }

    fn contains(&self, raw: &str) -> bool {
        self.seen.contains(raw)
    }

    fn insert(&mut self, raw: String, quoted: String) {
        if self.seen.insert(raw.clone()) {
            self.order.push((raw, quoted));
        }
    }

    fn remove(&mut self, raw: &str) {
        if self.seen.remove(raw) {
            self.order.retain(|(r, _)| r != raw);
        }
    }

    fn snapshot(&self) -> Vec<(String, String)> {
        self.order.clone()
    }
}

enum ControlStepOutcome {
    Completed,
    Recoverable(PostgresError),
    Fatal(PostgresError),
    UserClosed,
}

enum ReconnectOutcome {
    Reconnected(Box<PostgresConnection>),
    Failed(PostgresError),
    UserClosed,
}

enum ControlApplyError {
    Recoverable(PostgresError),
    Fatal(PostgresError),
    UserClosed,
}

struct ListenerPump {
    config: PostgresConfig,
    options: ListenerOptions,
    active: ActiveChannels,
    ctrl_rx: mpsc::Receiver<ControlCommand>,
    ctrl_closed: bool,
    shared: Arc<ListenerShared>,
    pending_gap: bool,
}

impl ListenerPump {
    fn new(
        config: PostgresConfig,
        options: ListenerOptions,
        active: ActiveChannels,
        ctrl_rx: mpsc::Receiver<ControlCommand>,
        shared: Arc<ListenerShared>,
    ) -> Self {
        Self {
            config,
            options,
            active,
            ctrl_rx,
            ctrl_closed: false,
            shared,
            pending_gap: false,
        }
    }

    async fn run(mut self, mut conn: PostgresConnection) {
        loop {
            if self.shared.user_close_requested.load(Ordering::Acquire) {
                close_conn_bounded(&mut conn, self.config.connect_timeout).await;
                self.shared.mark_pump_finished();
                return;
            }

            tokio::select! {
                biased;

                () = self.shared.wait_for_user_close() => {
                    close_conn_bounded(&mut conn, self.config.connect_timeout).await;
                    self.shared.mark_pump_finished();
                    return;
                }

                maybe_cmd = self.ctrl_rx.recv(), if !self.ctrl_closed => {
                    match maybe_cmd {
                        None => self.ctrl_closed = true,
                        Some(cmd) => {
                            let mut pending_replies = Vec::new();
                            match self.apply_control_command(&mut conn, cmd, &mut pending_replies).await {
                                Ok(()) => {
                                    for tx in pending_replies {
                                        let _ = tx.send(Ok(()));
                                    }
                                }
                                Err(ControlApplyError::UserClosed) => {
                                    fail_pending_replies(
                                        pending_replies,
                                        &PostgresError::Interface(
                                            "PostgreSQL notification listener was closed".to_string(),
                                        ),
                                    );
                                    close_conn_bounded(&mut conn, self.config.connect_timeout).await;
                                    self.shared.mark_pump_finished();
                                    return;
                                }
                                Err(ControlApplyError::Fatal(err)) => {
                                    self.shared.fail_terminal(err.clone());
                                    fail_pending_replies(pending_replies, &err);
                                    close_conn_bounded(&mut conn, self.config.connect_timeout).await;
                                    self.shared.mark_pump_finished();
                                    return;
                                }
                                Err(ControlApplyError::Recoverable(trigger_err)) => {
                                    close_conn_bounded(&mut conn, self.config.connect_timeout).await;
                                    let Some(new_conn) = self.recover_or_finish(pending_replies, trigger_err).await else {
                                        return;
                                    };
                                    conn = new_conn;
                                }
                            }
                        }
                    }
                }

                read_res = conn.read_raw_message(None) => {
                    let err = match read_res {
                        Ok(msg) => match handle_idle_backend_message(
                            &mut conn,
                            msg,
                            &self.shared,
                            &mut self.pending_gap,
                        ) {
                            Ok(()) => continue,
                            Err(e) => e,
                        },
                        Err(e) => e,
                    };
                    close_conn_bounded(&mut conn, self.config.connect_timeout).await;
                    if self.options.reconnect && is_recoverable_disconnect_error(&err) {
                        let Some(new_conn) = self.recover_or_finish(Vec::new(), err).await else {
                            return;
                        };
                        conn = new_conn;
                    } else {
                        self.shared.fail_terminal(err);
                        self.shared.mark_pump_finished();
                        return;
                    }
                }
            }
        }
    }

    async fn recover_or_finish(
        &mut self,
        mut pending_replies: Vec<oneshot::Sender<Result<(), PostgresError>>>,
        trigger_err: PostgresError,
    ) -> Option<PostgresConnection> {
        match self
            .reconnect_session(&mut pending_replies, trigger_err)
            .await
        {
            ReconnectOutcome::Reconnected(new_conn) => {
                for tx in pending_replies {
                    let _ = tx.send(Ok(()));
                }
                Some(*new_conn)
            }
            ReconnectOutcome::UserClosed => {
                fail_pending_replies(
                    pending_replies,
                    &PostgresError::Interface(
                        "PostgreSQL notification listener was closed".to_string(),
                    ),
                );
                self.shared.mark_pump_finished();
                None
            }
            ReconnectOutcome::Failed(err) => {
                self.shared.fail_terminal(err.clone());
                fail_pending_replies(pending_replies, &err);
                self.shared.mark_pump_finished();
                None
            }
        }
    }

    async fn apply_control_command(
        &mut self,
        conn: &mut PostgresConnection,
        cmd: ControlCommand,
        pending_replies: &mut Vec<oneshot::Sender<Result<(), PostgresError>>>,
    ) -> Result<(), ControlApplyError> {
        match cmd {
            ControlCommand::Subscribe { channels, reply } => {
                pending_replies.push(reply);
                let to_add: Vec<(String, String)> = channels
                    .into_iter()
                    .filter(|(raw, _)| !self.active.contains(raw))
                    .collect();
                for (idx, (raw, quoted)) in to_add.iter().enumerate() {
                    let sql = format!("LISTEN {quoted}");
                    match execute_control_sql(
                        conn,
                        &sql,
                        self.config.connect_timeout,
                        &self.shared,
                        self.options.reconnect,
                        &mut self.pending_gap,
                    )
                    .await
                    {
                        ControlStepOutcome::Completed => {
                            self.active.insert(raw.clone(), quoted.clone());
                        }
                        ControlStepOutcome::Recoverable(err) => {
                            for (rem_raw, rem_quoted) in &to_add[idx..] {
                                self.active.insert(rem_raw.clone(), rem_quoted.clone());
                            }
                            return Err(ControlApplyError::Recoverable(err));
                        }
                        ControlStepOutcome::Fatal(err) => {
                            return Err(ControlApplyError::Fatal(err));
                        }
                        ControlStepOutcome::UserClosed => {
                            return Err(ControlApplyError::UserClosed);
                        }
                    }
                }
                Ok(())
            }
            ControlCommand::Unsubscribe { channels, reply } => {
                pending_replies.push(reply);
                let to_remove: Vec<(String, String)> = channels
                    .into_iter()
                    .filter(|(raw, _)| self.active.contains(raw))
                    .collect();
                for (idx, (raw, quoted)) in to_remove.iter().enumerate() {
                    let sql = format!("UNLISTEN {quoted}");
                    match execute_control_sql(
                        conn,
                        &sql,
                        self.config.connect_timeout,
                        &self.shared,
                        self.options.reconnect,
                        &mut self.pending_gap,
                    )
                    .await
                    {
                        ControlStepOutcome::Completed => {
                            self.active.remove(raw);
                        }
                        ControlStepOutcome::Recoverable(err) => {
                            for (rem_raw, _) in &to_remove[idx..] {
                                self.active.remove(rem_raw);
                            }
                            return Err(ControlApplyError::Recoverable(err));
                        }
                        ControlStepOutcome::Fatal(err) => {
                            return Err(ControlApplyError::Fatal(err));
                        }
                        ControlStepOutcome::UserClosed => {
                            return Err(ControlApplyError::UserClosed);
                        }
                    }
                }
                Ok(())
            }
        }
    }

    async fn reconnect_session(
        &mut self,
        pending_replies: &mut Vec<oneshot::Sender<Result<(), PostgresError>>>,
        initial_err: PostgresError,
    ) -> ReconnectOutcome {
        let mut last_err = initial_err;

        'attempts: for attempt in 0..MAX_RECONNECT_ATTEMPTS {
            let sleep_deadline = Instant::now() + compute_reconnect_backoff(attempt);
            loop {
                tokio::select! {
                    biased;

                    () = self.shared.wait_for_user_close() => return ReconnectOutcome::UserClosed,

                    maybe_cmd = self.ctrl_rx.recv(), if !self.ctrl_closed => {
                        match maybe_cmd {
                            None => self.ctrl_closed = true,
                            Some(ControlCommand::Subscribe { channels, reply }) => {
                                for (raw, quoted) in channels {
                                    self.active.insert(raw, quoted);
                                }
                                pending_replies.push(reply);
                            }
                            Some(ControlCommand::Unsubscribe { channels, reply }) => {
                                for (raw, _) in channels {
                                    self.active.remove(&raw);
                                }
                                pending_replies.push(reply);
                            }
                        }
                    }

                    () = tokio::time::sleep_until(sleep_deadline) => break,
                }
            }

            let dial_res = tokio::select! {
                biased;
                () = self.shared.wait_for_user_close() => return ReconnectOutcome::UserClosed,
                res = establish_standalone_connection(&self.config, false) => res,
            };

            let mut new_conn = match dial_res {
                Ok(c) => c,
                Err(err) => {
                    if is_recoverable_disconnect_error(&err) {
                        last_err = err;
                        continue 'attempts;
                    }
                    return ReconnectOutcome::Failed(err);
                }
            };

            let mut attempt_gap = true;
            let snapshot = self.active.snapshot();
            for (_raw, quoted) in snapshot {
                let sql = format!("LISTEN {quoted}");
                match execute_control_sql(
                    &mut new_conn,
                    &sql,
                    self.config.connect_timeout,
                    &self.shared,
                    true,
                    &mut attempt_gap,
                )
                .await
                {
                    ControlStepOutcome::Completed => {}
                    ControlStepOutcome::UserClosed => {
                        close_conn_bounded(&mut new_conn, self.config.connect_timeout).await;
                        return ReconnectOutcome::UserClosed;
                    }
                    ControlStepOutcome::Fatal(err) => {
                        close_conn_bounded(&mut new_conn, self.config.connect_timeout).await;
                        return ReconnectOutcome::Failed(err);
                    }
                    ControlStepOutcome::Recoverable(err) => {
                        close_conn_bounded(&mut new_conn, self.config.connect_timeout).await;
                        last_err = err;
                        continue 'attempts;
                    }
                }
            }

            self.pending_gap = attempt_gap;
            return ReconnectOutcome::Reconnected(Box::new(new_conn));
        }

        ReconnectOutcome::Failed(last_err)
    }
}

async fn execute_control_sql(
    conn: &mut PostgresConnection,
    sql: &str,
    timeout: Duration,
    shared: &ListenerShared,
    reconnect: bool,
    pending_gap: &mut bool,
) -> ControlStepOutcome {
    let deadline = Instant::now() + timeout;

    let send_res = tokio::select! {
        biased;
        () = shared.wait_for_user_close() => return ControlStepOutcome::UserClosed,
        res = conn.send_simple_command(sql, Some(deadline)) => res,
    };
    if let Err(err) = send_res {
        return classify_control_error(err, reconnect);
    }

    let mut first_sql_err: Option<PostgresError> = None;
    loop {
        let msg_res = tokio::select! {
            biased;
            () = shared.wait_for_user_close() => return ControlStepOutcome::UserClosed,
            res = conn.read_raw_message(Some(deadline)) => res,
        };
        let msg = match msg_res {
            Ok(m) => m,
            Err(err) => {
                if let Some(prior) = first_sql_err {
                    return classify_control_error(prior, reconnect);
                }
                return classify_control_error(err, reconnect);
            }
        };

        match msg {
            Message::CommandComplete(_)
            | Message::EmptyQueryResponse
            | Message::NoticeResponse(_) => {}
            Message::ParameterStatus(body) => {
                if let Err(err) = conn.handle_parameter_status_frame(&body) {
                    return ControlStepOutcome::Fatal(err);
                }
            }
            Message::NotificationResponse(body) => {
                match decode_notification_frame(&body, pending_gap) {
                    Ok(frame) => {
                        if let Err(overflow_err) = shared.try_enqueue(frame) {
                            return ControlStepOutcome::Fatal(overflow_err);
                        }
                    }
                    Err(proto_err) => return ControlStepOutcome::Fatal(proto_err),
                }
            }
            Message::ErrorResponse(body) => match parse_error_fields(body.fields()) {
                Ok(err) => {
                    if first_sql_err.is_none() {
                        first_sql_err = Some(err);
                    }
                }
                Err(proto_err) => return ControlStepOutcome::Fatal(proto_err),
            },
            Message::ReadyForQuery(body) => {
                match TransactionStatus::from_wire_tag(body.status()) {
                    Ok(status) => conn.set_transaction_status_from_wire(status),
                    Err(proto_err) => return ControlStepOutcome::Fatal(proto_err),
                }
                if let Some(sql_err) = first_sql_err {
                    return classify_control_error(sql_err, reconnect);
                }
                return ControlStepOutcome::Completed;
            }
            _ => {
                return ControlStepOutcome::Fatal(PostgresError::Protocol(
                    "unexpected backend frame during LISTEN/UNLISTEN exchange".to_string(),
                ));
            }
        }
    }
}

fn handle_idle_backend_message(
    conn: &mut PostgresConnection,
    msg: Message,
    shared: &ListenerShared,
    pending_gap: &mut bool,
) -> Result<(), PostgresError> {
    match msg {
        Message::NotificationResponse(body) => {
            let frame = decode_notification_frame(&body, pending_gap)?;
            shared.try_enqueue(frame)
        }
        Message::NoticeResponse(_) => Ok(()),
        Message::ParameterStatus(body) => conn.handle_parameter_status_frame(&body),
        Message::ErrorResponse(body) => {
            let err = parse_error_fields(body.fields())?;
            Err(err)
        }
        _ => Err(PostgresError::Protocol(
            "unexpected unsolicited backend frame on notification listener".to_string(),
        )),
    }
}

fn decode_notification_frame(
    body: &NotificationResponseBody,
    pending_gap: &mut bool,
) -> Result<NotificationFrame, PostgresError> {
    let channel = body
        .channel()
        .map_err(|e| {
            PostgresError::Protocol(format!("invalid NotificationResponse channel UTF-8: {e}"))
        })?
        .to_owned();
    let payload_str = body.message().map_err(|e| {
        PostgresError::Protocol(format!("invalid NotificationResponse payload UTF-8: {e}"))
    })?;
    let reconnected_gap = std::mem::replace(pending_gap, false);
    Ok(NotificationFrame {
        pid: body.process_id(),
        channel,
        payload: Bytes::copy_from_slice(payload_str.as_bytes()),
        reconnected_gap,
    })
}

fn classify_control_error(err: PostgresError, reconnect: bool) -> ControlStepOutcome {
    if reconnect && is_recoverable_disconnect_error(&err) {
        ControlStepOutcome::Recoverable(err)
    } else {
        ControlStepOutcome::Fatal(err)
    }
}

fn is_recoverable_disconnect_error(err: &PostgresError) -> bool {
    match err {
        PostgresError::Transport(_) => true,
        PostgresError::Server(srv) => matches!(
            srv.sqlstate.as_str(),
            "57P01" | "57P02" | "57P03" | "08000" | "08001" | "08003" | "08006"
        ),
        _ => false,
    }
}

fn compute_reconnect_backoff(attempt: u32) -> Duration {
    let shift = attempt.min(6);
    let base_ms = RECONNECT_BASE_DELAY_MS
        .saturating_mul(1_u64 << shift)
        .min(RECONNECT_MAX_DELAY_MS);
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u32(attempt);
    let rand_u64 = hasher.finish();
    let delay_ms = if base_ms >= RECONNECT_MAX_DELAY_MS {
        let span = 1_000_u64;
        (RECONNECT_MAX_DELAY_MS - span) + (rand_u64 % (span + 1))
    } else {
        let max_jitter = (base_ms / 4).min(RECONNECT_MAX_DELAY_MS - base_ms);
        base_ms + (rand_u64 % (max_jitter + 1))
    };
    Duration::from_millis(delay_ms)
}

fn validate_and_dedup_channels(
    raw_channels: &[String],
) -> Result<Vec<(String, String)>, PostgresError> {
    let mut validated = Vec::with_capacity(raw_channels.len());
    let mut seen = HashSet::with_capacity(raw_channels.len());
    for ch in raw_channels {
        let quoted = validate_and_quote_channel(ch)?;
        if seen.insert(ch.clone()) {
            validated.push((ch.clone(), quoted));
        }
    }
    Ok(validated)
}

fn is_valid_channel_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_' || (!ch.is_ascii() && ch.is_alphabetic())
}

fn is_valid_channel_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '$' || (!ch.is_ascii() && ch.is_alphanumeric())
}

fn fail_pending_replies(
    replies: Vec<oneshot::Sender<Result<(), PostgresError>>>,
    err: &PostgresError,
) {
    for tx in replies {
        let _ = tx.send(Err(err.clone()));
    }
}

async fn close_conn_bounded(conn: &mut PostgresConnection, timeout: Duration) {
    if !conn.is_closed() {
        let budget = timeout.min(Duration::from_millis(250));
        let _ = tokio::time::timeout(budget, conn.close()).await;
    }
}

fn runtime_handle() -> tokio::runtime::Handle {
    tokio::runtime::Handle::try_current()
        .unwrap_or_else(|_| vertebra_db::py_async::db_runtime_handle().clone())
}
