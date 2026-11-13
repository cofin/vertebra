//! Multi-generation logical replication supervision, generation-fenced event delivery leases,
//! and bounded jittered reconnect recovery over `PostgresReplicationConnection`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tokio::task::AbortHandle;

use super::feedback::{MAX_UNACKED_COMMITS, ReplicationFeedback, compute_status_interval};
use super::pgoutput::ReplicationRecord;
use super::stream::ReplicationSession;
use super::{
    PostgresReplicationConnection, VerifiedConnectFailure, VerifiedReplicationIdentity,
    ensure_session_capable_pooler, validate_replication_identifier,
};
use crate::config::PostgresConfig;
use crate::errors::PostgresError;

/// Maximum consecutive raw reconnect attempts before surfacing a terminal replication error.
pub const MAX_RAW_RECONNECT_ATTEMPTS: u32 = 8;

const MIN_RECONNECT_DELAY_MS: u64 = 100;
const MAX_RECONNECT_DELAY_MS: u64 = 5_000;
const CAP_JITTER_BASE_MS: u64 = 4_000;

static NEXT_SUPERVISOR_SEED: AtomicU64 = AtomicU64::new(1);

/// Snapshot of the supervisor's current or most recent verified identity, monotonic acknowledged
/// LSN, and generation counter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresReplicationResume {
    pub identity: VerifiedReplicationIdentity,
    pub acknowledged_lsn: u64,
    pub generation: u64,
}

/// Generation-tagged event emitted by [`ReplicationSupervisor`].
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationStreamEvent {
    Control {
        identity: VerifiedReplicationIdentity,
        acknowledged_lsn: u64,
        generation: u64,
    },
    Record {
        record: ReplicationRecord,
        generation: u64,
    },
}

/// Compute a deterministic jittered reconnect delay in `100ms..=5s` for `attempt`.
pub fn raw_reconnect_delay(attempt: u32, jitter_seed: u64) -> Duration {
    let shift = attempt.min(6);
    let raw_base_ms = MIN_RECONNECT_DELAY_MS
        .saturating_mul(1_u64 << shift)
        .min(MAX_RECONNECT_DELAY_MS);
    let mixed = mix_u64(jitter_seed ^ u64::from(attempt).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let delay_ms = if raw_base_ms < MAX_RECONNECT_DELAY_MS {
        let jitter_span_ms = raw_base_ms / 4;
        raw_base_ms + (mixed % (jitter_span_ms + 1))
    } else {
        let cap_span_ms = MAX_RECONNECT_DELAY_MS - CAP_JITTER_BASE_MS;
        CAP_JITTER_BASE_MS + (mixed % (cap_span_ms + 1))
    };
    Duration::from_millis(delay_ms.clamp(MIN_RECONNECT_DELAY_MS, MAX_RECONNECT_DELAY_MS))
}

struct ActiveGeneration {
    generation: u64,
    session: Arc<ReplicationSession>,
    feedback: ReplicationFeedback,
    pending_control: Option<ReplicationStreamEvent>,
    front: Option<ReplicationStreamEvent>,
}

struct SupervisorState {
    active: Option<ActiveGeneration>,
    closing_session: Option<Arc<ReplicationSession>>,
    last_identity: VerifiedReplicationIdentity,
    acknowledged_lsn: u64,
    last_generation: u64,
    reconnecting: bool,
    closing: bool,
    terminal: Option<Result<(), PostgresError>>,
    error_delivered: bool,
    receiver_reserved: bool,
}

struct SupervisorShared {
    state: Mutex<SupervisorState>,
    events: watch::Sender<u64>,
}

impl SupervisorShared {
    fn notify(&self) {
        self.events.send_modify(|rev| *rev = rev.wrapping_add(1));
    }
}

/// Multi-generation logical replication supervisor with generation-fenced delivery leases and
/// optional identity-verified reconnect recovery.
pub struct ReplicationSupervisor {
    shared: Arc<SupervisorShared>,
    worker: AbortHandle,
}

impl std::fmt::Debug for ReplicationSupervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let resume = self.resume();
        f.debug_struct("ReplicationSupervisor")
            .field("generation", &resume.generation)
            .field("acknowledged_lsn", &resume.acknowledged_lsn)
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl ReplicationSupervisor {
    /// Open an initial verified logical replication generation and spawn its background supervisor.
    pub async fn open(
        config: PostgresConfig,
        slot: String,
        publication: String,
        start_lsn: Option<u64>,
        queue_capacity: usize,
        reconnect: bool,
    ) -> Result<Self, PostgresError> {
        ensure_session_capable_pooler(&config)?;
        if !(1..=1024).contains(&queue_capacity) {
            return Err(PostgresError::Interface(
                "replication supervisor requires queue_capacity in 1..=1024".into(),
            ));
        }
        validate_replication_identifier(&slot, "slot")?;
        validate_replication_identifier(&publication, "publication")?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            PostgresError::Interface(
                "replication supervisor requires an active Tokio runtime".into(),
            )
        })?;

        let conn =
            PostgresReplicationConnection::connect_logical(&config, &slot, &publication, start_lsn)
                .await?;

        let initial_lsn = conn.effective_start_lsn();
        let initial_identity = conn.identity().clone();
        let status_interval = compute_status_interval(parse_wal_sender_timeout(
            conn.parameter("wal_sender_timeout"),
        ));
        let shutdown_timeout = config.connect_timeout.max(Duration::from_millis(1));
        let session =
            Arc::new(conn.into_stream(queue_capacity, status_interval, shutdown_timeout)?);

        let (events, _) = watch::channel(0_u64);
        let shared = Arc::new(SupervisorShared {
            state: Mutex::new(SupervisorState {
                active: Some(ActiveGeneration {
                    generation: 1,
                    session,
                    feedback: ReplicationFeedback::new(initial_lsn),
                    pending_control: Some(ReplicationStreamEvent::Control {
                        identity: initial_identity.clone(),
                        acknowledged_lsn: initial_lsn,
                        generation: 1,
                    }),
                    front: None,
                }),
                closing_session: None,
                last_identity: initial_identity,
                acknowledged_lsn: initial_lsn,
                last_generation: 1,
                reconnecting: false,
                closing: false,
                terminal: None,
                error_delivered: false,
                receiver_reserved: false,
            }),
            events,
        });

        let worker_shared = Arc::clone(&shared);
        let seed_base = NEXT_SUPERVISOR_SEED
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_mul(0xD1B5_4A32_D192_ED03);
        let task = runtime.spawn(run_supervisor_worker(
            worker_shared,
            config,
            slot,
            publication,
            queue_capacity,
            reconnect,
            seed_base,
        ));

        Ok(Self {
            shared,
            worker: task.abort_handle(),
        })
    }

    /// Lease the next generation-fenced control or record event.
    pub async fn next_event(&self) -> Result<Option<ReplicationEventDelivery>, PostgresError> {
        let mut events_rx = self.shared.events.subscribe();
        {
            let mut state = self.shared.state.lock().unwrap();
            if state.receiver_reserved {
                return Err(PostgresError::Interface(
                    "replication already has a pending receiver or delivery".into(),
                ));
            }
            state.receiver_reserved = true;
        }
        let reservation = SupervisorReservation {
            shared: Arc::clone(&self.shared),
        };

        loop {
            let active_session = {
                let mut state = self.shared.state.lock().unwrap();
                let _ = events_rx.borrow_and_update();

                if let Some(Err(error)) = state.terminal.clone() {
                    if !state.error_delivered {
                        state.error_delivered = true;
                        return Err(error);
                    }
                    return Ok(None);
                }
                if state.closing {
                    return Ok(None);
                }

                if let Some(active) = state.active.as_mut() {
                    if let Some(event) = active
                        .front
                        .take()
                        .or_else(|| active.pending_control.take())
                    {
                        let blocked_by_unacked = match &event {
                            ReplicationStreamEvent::Record { record, .. }
                                if record.commit_end_lsn().is_some() =>
                            {
                                active.feedback.unacked_count() >= MAX_UNACKED_COMMITS
                            }
                            _ => false,
                        };
                        if !blocked_by_unacked {
                            let generation = active.generation;
                            drop(state);
                            return Ok(Some(ReplicationEventDelivery {
                                event: Some(event),
                                generation,
                                reservation,
                            }));
                        }
                        active.front = Some(event);
                        None
                    } else {
                        Some((active.generation, Arc::clone(&active.session)))
                    }
                } else if state.terminal.is_some() && !state.reconnecting {
                    return Ok(None);
                } else {
                    None
                }
            };

            let Some((gen_num, session)) = active_session else {
                if events_rx.changed().await.is_err() {
                    return Ok(None);
                }
                continue;
            };

            let next_res = tokio::select! {
                res = session.next() => Some(res),
                changed = events_rx.changed() => {
                    if changed.is_err() {
                        return Ok(None);
                    }
                    None
                }
            };

            let Some(session_outcome) = next_res else {
                continue;
            };

            match session_outcome {
                Ok(Some(session_delivery)) => {
                    let Ok(record) = session_delivery.publish() else {
                        continue;
                    };
                    let commit_lsn = record.commit_end_lsn();
                    let mut state = self.shared.state.lock().unwrap();
                    if state.closing || matches!(state.terminal, Some(Err(_))) {
                        continue;
                    }
                    if let Some(active) = state.active.as_mut()
                        && active.generation == gen_num
                    {
                        if let Some(lsn) = commit_lsn {
                            active.feedback.record_received_commit(lsn)?;
                        }
                        drop(state);
                        return Ok(Some(ReplicationEventDelivery {
                            event: Some(ReplicationStreamEvent::Record {
                                record,
                                generation: gen_num,
                            }),
                            generation: gen_num,
                            reservation,
                        }));
                    }
                }
                Ok(None) => {
                    let clean_eof = match session.wait_closed().await {
                        Ok(()) => true,
                        Err(_) => false,
                    };
                    let mut state = self.shared.state.lock().unwrap();
                    if state.closing || state.reconnecting || matches!(state.terminal, Some(Err(_)))
                    {
                        continue;
                    }
                    if clean_eof
                        && let Some(active) = state.active.as_ref()
                        && active.generation == gen_num
                        && active.front.is_none()
                        && active.pending_control.is_none()
                    {
                        state.active = None;
                        state.terminal.get_or_insert(Ok(()));
                        drop(state);
                        self.shared.notify();
                        return Ok(None);
                    }
                }
                Err(_) => {
                    let should_wait = {
                        let state = self.shared.state.lock().unwrap();
                        !state.closing
                            && state.terminal.is_none()
                            && !state.reconnecting
                            && state.active.as_ref().map(|a| a.generation) == Some(gen_num)
                    };
                    if should_wait && events_rx.changed().await.is_err() {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Return the latest verified identity, monotonic acknowledged LSN, and generation counter.
    pub fn resume(&self) -> PostgresReplicationResume {
        let state = self.shared.state.lock().unwrap();
        PostgresReplicationResume {
            identity: state.last_identity.clone(),
            acknowledged_lsn: state.acknowledged_lsn,
            generation: state.last_generation,
        }
    }

    /// Synchronously fence leases, mark closed, and abort owned worker and session tasks without
    /// entering or requiring an ambient Tokio runtime.
    pub fn cancel(&self) {
        let (active, closing_session) = {
            let mut state = self.shared.state.lock().unwrap();
            state.closing = true;
            state.reconnecting = false;
            if state.terminal.is_none() {
                state.terminal = Some(Ok(()));
            }
            (state.active.take(), state.closing_session.take())
        };
        if let Some(active) = active {
            active.session.cancel();
        }
        if let Some(session) = closing_session {
            session.cancel();
        }
        self.shared.notify();
        self.worker.abort();
    }

    /// Acknowledge a published Commit boundary on the matching live `generation`.
    pub fn acknowledge(&self, generation: u64, commit_end_lsn: u64) -> Result<u64, PostgresError> {
        let acked = {
            let mut state = self.shared.state.lock().unwrap();
            if state.closing || matches!(state.terminal, Some(Err(_))) {
                return Err(PostgresError::Interface(
                    "replication supervisor is closed".into(),
                ));
            }
            let Some(active) = state.active.as_mut() else {
                return Err(PostgresError::Interface(format!(
                    "replication generation {generation} is not active"
                )));
            };
            if active.generation != generation {
                return Err(PostgresError::Interface(format!(
                    "replication acknowledgement generation {generation} does not match active generation {}",
                    active.generation
                )));
            }
            active.feedback.acknowledge(commit_end_lsn)?;
            match active.session.acknowledge(commit_end_lsn) {
                Ok(lsn) => {
                    state.acknowledged_lsn = state.acknowledged_lsn.max(lsn);
                    lsn
                }
                Err(err) => {
                    state.active = None;
                    return Err(err);
                }
            }
        };
        self.shared.notify();
        Ok(acked)
    }

    /// Gracefully drain the active generation under its configured shutdown budget without
    /// acknowledging undelivered records, or cancel pending reconnect backoff immediately.
    pub async fn shutdown(&self) -> Result<(), PostgresError> {
        let session_opt = {
            let mut state = self.shared.state.lock().unwrap();
            if let Some(result) = state.terminal.clone() {
                return result;
            }
            state.closing = true;
            state.reconnecting = false;
            if let Some(active) = state.active.take() {
                state.closing_session = Some(active.session);
            }
            state.closing_session.clone()
        };
        self.shared.notify();

        let result = if let Some(session) = session_opt {
            session.shutdown().await
        } else {
            Ok(())
        };

        let final_result = {
            let mut state = self.shared.state.lock().unwrap();
            state.closing_session = None;
            state.terminal.get_or_insert(result).clone()
        };
        self.shared.notify();
        self.worker.abort();
        final_result
    }

    /// Report whether graceful shutdown, synchronous cancellation, or terminal completion has occurred.
    pub fn is_closed(&self) -> bool {
        let state = self.shared.state.lock().unwrap();
        state.closing || state.terminal.is_some()
    }
}

impl Drop for ReplicationSupervisor {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// Generation-fenced lease over one [`ReplicationStreamEvent`] and the supervisor's single-receiver
/// reservation.
pub struct ReplicationEventDelivery {
    event: Option<ReplicationStreamEvent>,
    generation: u64,
    reservation: SupervisorReservation,
}

impl std::fmt::Debug for ReplicationEventDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationEventDelivery")
            .field("generation", &self.generation)
            .field("event", &self.event)
            .finish()
    }
}

struct SupervisorReservation {
    shared: Arc<SupervisorShared>,
}

impl Drop for SupervisorReservation {
    fn drop(&mut self) {
        self.shared.state.lock().unwrap().receiver_reserved = false;
        self.shared.notify();
    }
}

impl Drop for ReplicationEventDelivery {
    fn drop(&mut self) {
        if let Some(event) = self.event.take() {
            let mut state = self.reservation.shared.state.lock().unwrap();
            if !state.closing
                && !matches!(state.terminal, Some(Err(_)))
                && let Some(active) = state.active.as_mut()
                && active.generation == self.generation
            {
                active.front = Some(event);
            }
        }
    }
}

impl ReplicationEventDelivery {
    /// Borrow the leased stream event without publishing it or advancing commit feedback.
    pub fn event(&self) -> &ReplicationStreamEvent {
        self.event.as_ref().expect("live delivery owns its event")
    }

    /// Publish this event on its matching live generation, registering Commit boundaries for
    /// subsequent [`ReplicationSupervisor::acknowledge`] calls.
    pub fn publish(mut self) -> Result<ReplicationStreamEvent, PostgresError> {
        let mut state = self.reservation.shared.state.lock().unwrap();
        if state.closing || matches!(state.terminal, Some(Err(_))) {
            let _ = self.event.take();
            return Err(PostgresError::Interface(
                "replication supervisor is closed".into(),
            ));
        }
        let Some(active) = state.active.as_mut() else {
            let _ = self.event.take();
            return Err(PostgresError::Interface(format!(
                "replication generation {} is stale",
                self.generation
            )));
        };
        if active.generation != self.generation {
            let _ = self.event.take();
            return Err(PostgresError::Interface(format!(
                "replication generation {} is stale (active generation {})",
                self.generation, active.generation
            )));
        }
        if let Some(ReplicationStreamEvent::Record { record, .. }) = self.event.as_ref()
            && let Some(lsn) = record.commit_end_lsn()
        {
            active.feedback.record_received_commit(lsn)?;
            active.feedback.record_published_commit(lsn)?;
        }
        drop(state);
        Ok(self.event.take().expect("live delivery owns its event"))
    }
}

async fn run_supervisor_worker(
    shared: Arc<SupervisorShared>,
    config: PostgresConfig,
    slot: String,
    publication: String,
    queue_capacity: usize,
    reconnect: bool,
    seed_base: u64,
) {
    let mut control_rx = shared.events.subscribe();

    loop {
        let current = {
            let state = shared.state.lock().unwrap();
            let _ = control_rx.borrow_and_update();
            if state.closing || state.terminal.is_some() {
                return;
            }
            state
                .active
                .as_ref()
                .map(|active| (active.generation, Arc::clone(&active.session)))
        };

        let Some((gen_num, session)) = current else {
            return;
        };

        let closed_outcome = tokio::select! {
            res = session.wait_closed() => Some(res),
            changed = control_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                None
            }
        };

        let Some(outcome) = closed_outcome else {
            continue;
        };

        let session_err = match outcome {
            Ok(()) => {
                let mut state = shared.state.lock().unwrap();
                if state.closing {
                    state.terminal.get_or_insert(Ok(()));
                    drop(state);
                    shared.notify();
                }
                return;
            }
            Err(err) => err,
        };

        {
            let mut state = shared.state.lock().unwrap();
            let _ = control_rx.borrow_and_update();
            if state.closing {
                state.active = None;
                state.reconnecting = false;
                state.terminal.get_or_insert(Err(session_err));
                drop(state);
                shared.notify();
                return;
            }
            if state.active.as_ref().map(|a| a.generation) == Some(gen_num) {
                state.active = None;
            }
            if !reconnect || !is_retryable_replication_error(&session_err, false) {
                state.reconnecting = false;
                state.terminal.get_or_insert(Err(session_err));
                drop(state);
                shared.notify();
                return;
            }
            state.reconnecting = true;
        }
        shared.notify();

        for attempt in 0..MAX_RAW_RECONNECT_ATTEMPTS {
            let jitter_seed = derive_attempt_seed(seed_base, gen_num, attempt);
            let delay = raw_reconnect_delay(attempt, jitter_seed);

            let interrupted = tokio::select! {
                () = tokio::time::sleep(delay) => false,
                () = wait_until_closing(&mut control_rx, &shared) => true,
            };
            if interrupted {
                finish_cancelled_reconnect(&shared);
                return;
            }

            let (resume_lsn, expected_identity) = {
                let mut state = shared.state.lock().unwrap();
                let _ = control_rx.borrow_and_update();
                if state.closing {
                    state.reconnecting = false;
                    state.terminal.get_or_insert(Ok(()));
                    drop(state);
                    shared.notify();
                    return;
                }
                (state.acknowledged_lsn, state.last_identity.clone())
            };

            let connect_res = tokio::select! {
                res = PostgresReplicationConnection::connect_logical_verified_outcome(
                    &config,
                    &slot,
                    &publication,
                    resume_lsn,
                    &expected_identity,
                ) => Some(res),
                () = wait_until_closing(&mut control_rx, &shared) => None,
            };

            let Some(connect_res) = connect_res else {
                finish_cancelled_reconnect(&shared);
                return;
            };

            match connect_res {
                Ok(new_conn) => {
                    let new_effective_lsn = new_conn.effective_start_lsn();
                    let new_identity = new_conn.identity().clone();
                    let status_interval = compute_status_interval(parse_wal_sender_timeout(
                        new_conn.parameter("wal_sender_timeout"),
                    ));
                    let shutdown_timeout = config.connect_timeout.max(Duration::from_millis(1));
                    let new_session = match new_conn.into_stream(
                        queue_capacity,
                        status_interval,
                        shutdown_timeout,
                    ) {
                        Ok(session) => Arc::new(session),
                        Err(err) => {
                            let mut state = shared.state.lock().unwrap();
                            state.reconnecting = false;
                            state.terminal.get_or_insert(Err(err));
                            drop(state);
                            shared.notify();
                            return;
                        }
                    };

                    let mut state = shared.state.lock().unwrap();
                    if state.closing {
                        new_session.cancel();
                        state.reconnecting = false;
                        state.terminal.get_or_insert(Ok(()));
                        drop(state);
                        shared.notify();
                        return;
                    }
                    let next_gen = state.last_generation + 1;
                    let effective_ack = state.acknowledged_lsn.max(new_effective_lsn);
                    state.acknowledged_lsn = effective_ack;
                    state.last_identity = new_identity.clone();
                    state.last_generation = next_gen;
                    state.reconnecting = false;
                    state.active = Some(ActiveGeneration {
                        generation: next_gen,
                        session: new_session,
                        feedback: ReplicationFeedback::new(effective_ack),
                        pending_control: Some(ReplicationStreamEvent::Control {
                            identity: new_identity,
                            acknowledged_lsn: effective_ack,
                            generation: next_gen,
                        }),
                        front: None,
                    });
                    drop(state);
                    shared.notify();
                    break;
                }
                Err(failure) => {
                    let (slot_active, err) = match failure {
                        VerifiedConnectFailure::SlotActive(err) => (true, err),
                        VerifiedConnectFailure::Other(err) => (false, err),
                    };
                    if !is_retryable_replication_error(&err, slot_active)
                        || attempt + 1 >= MAX_RAW_RECONNECT_ATTEMPTS
                    {
                        let mut state = shared.state.lock().unwrap();
                        state.reconnecting = false;
                        state.terminal.get_or_insert(Err(err));
                        drop(state);
                        shared.notify();
                        return;
                    }
                }
            }
        }
    }
}

async fn wait_until_closing(control_rx: &mut watch::Receiver<u64>, shared: &SupervisorShared) {
    loop {
        if shared.state.lock().unwrap().closing {
            return;
        }
        if control_rx.changed().await.is_err() {
            return;
        }
    }
}

fn finish_cancelled_reconnect(shared: &SupervisorShared) {
    let mut state = shared.state.lock().unwrap();
    state.reconnecting = false;
    state.terminal.get_or_insert(Ok(()));
    drop(state);
    shared.notify();
}

fn is_retryable_replication_error(err: &PostgresError, slot_active_preflight: bool) -> bool {
    if slot_active_preflight {
        return true;
    }
    match err {
        PostgresError::Transport(_) => true,
        PostgresError::Server(srv) => {
            let code = srv.sqlstate.as_str();
            (code.starts_with("08") && code != "08P01")
                || code.starts_with("57P")
                || code == "53300"
                || code == "55006"
        }
        _ => false,
    }
}

fn parse_wal_sender_timeout(raw: Option<&str>) -> Option<Duration> {
    let trimmed = raw?.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    let (num_str, unit_scale_us) = if let Some(v) = lower.strip_suffix("min") {
        (v.trim(), 60_000_000_u64)
    } else if let Some(v) = lower.strip_suffix("ms") {
        (v.trim(), 1_000_u64)
    } else if let Some(v) = lower.strip_suffix("us") {
        (v.trim(), 1_u64)
    } else if let Some(v) = lower.strip_suffix('s') {
        (v.trim(), 1_000_000_u64)
    } else if let Some(v) = lower.strip_suffix('h') {
        (v.trim(), 3_600_000_000_u64)
    } else if let Some(v) = lower.strip_suffix('d') {
        (v.trim(), 86_400_000_000_u64)
    } else {
        (lower.as_str(), 1_000_u64)
    };
    let count = num_str.parse::<u64>().ok()?;
    if count == 0 {
        return None;
    }
    Some(Duration::from_micros(count.saturating_mul(unit_scale_us)))
}

fn derive_attempt_seed(seed_base: u64, generation: u64, attempt: u32) -> u64 {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    mix_u64(
        seed_base
            ^ generation.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ u64::from(attempt).wrapping_mul(0xBF58_476D_1CE4_E5B9)
            ^ now_nanos,
    )
}

const fn mix_u64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
