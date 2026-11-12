//! Split-owned logical replication transport and independent standby status writer.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use postgres_protocol::message::backend::Message;
use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio::time::{Instant, MissedTickBehavior};

use super::feedback::{
    MAX_UNACKED_COMMITS, ReplicationCopyData, ReplicationFeedback, decode_replication_copy_data,
    encode_standby_status_update,
};
use super::pgoutput::{
    BoundedReplicationQueue, DEFAULT_MAX_QUEUE_BYTES, DEFAULT_MAX_RECORD_BYTES,
    DEFAULT_MAX_RELATIONS, PgOutputDecoder, ReplicationRecord,
};
use super::{PostgresReplicationConnection, VerifiedReplicationIdentity};
use crate::codecs::CodecContext;
use crate::errors::PostgresError;
use crate::protocol::{
    MAX_BACKEND_FRAME_LENGTH, ReplicationBackendMessage, parse_error_fields,
    read_replication_message,
};
use crate::session::ConnectionStream;
use crate::types::TypeCatalog;

struct SessionState {
    feedback: ReplicationFeedback,
    closing: bool,
    terminal: Option<Result<(), PostgresError>>,
    queue: BoundedReplicationQueue,
    reader_eof: bool,
    error_delivered: bool,
    front: Option<ReplicationRecord>,
    receiver_reserved: bool,
    deadline: Option<Instant>,
}

struct SessionShared {
    state: Mutex<SessionState>,
    revision: watch::Sender<u64>,
    status: watch::Sender<u64>,
    shutdown_timeout: Duration,
}

impl SessionShared {
    fn signal_records(&self) {
        self.revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    fn signal(&self) {
        self.signal_records();
        self.status
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    fn finish(&self, result: Result<(), PostgresError>) {
        let mut state = self.state.lock().unwrap();
        if result.is_err() || state.closing {
            state.closing = true;
            while state.queue.pop().is_some() {}
            state.front = None;
        }
        if state.terminal.is_none() {
            state.terminal = Some(result);
        }
        drop(state);
        self.signal();
    }
}

/// Single-generation replication transport, cancelled when its owner is dropped.
pub struct ReplicationSession {
    identity: VerifiedReplicationIdentity,
    shared: Arc<SessionShared>,
    worker: AbortHandle,
}

impl PostgresReplicationConnection {
    /// Consume the authenticated CopyBoth connection and start its independent status writer.
    pub fn into_stream(
        self,
        queue_capacity: usize,
        status_interval: Duration,
        shutdown_timeout: Duration,
    ) -> Result<ReplicationSession, PostgresError> {
        if !(1..=1024).contains(&queue_capacity)
            || status_interval.is_zero()
            || shutdown_timeout.is_zero()
        {
            return Err(PostgresError::Interface(
                "replication requires queue capacity 1..=1024 and positive intervals/timeouts"
                    .into(),
            ));
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            PostgresError::Interface("replication stream requires an active Tokio runtime".into())
        })?;
        let Self {
            conn,
            identity,
            type_catalog,
            type_identities,
            codec_ctx,
            effective_start_lsn,
        } = self;
        let (reader, writer) = tokio::io::split(conn.stream);
        let (revision, _) = watch::channel(0);
        let (status, changes) = watch::channel(0);
        let shared = Arc::new(SessionShared {
            state: Mutex::new(SessionState {
                feedback: ReplicationFeedback::new(effective_start_lsn),
                closing: false,
                terminal: None,
                queue: BoundedReplicationQueue::new(
                    queue_capacity,
                    DEFAULT_MAX_QUEUE_BYTES,
                    DEFAULT_MAX_RECORD_BYTES,
                ),
                reader_eof: false,
                error_delivered: false,
                front: None,
                receiver_reserved: false,
                deadline: None,
            }),
            revision,
            status,
            shutdown_timeout,
        });
        let owner = Arc::clone(&shared);
        let reader_identity = identity.clone();
        let task = runtime.spawn(async move {
            let reader = run_reader(
                reader,
                conn.read_buf,
                type_catalog,
                codec_ctx,
                PgOutputDecoder::with_type_identities(
                    DEFAULT_MAX_RELATIONS,
                    DEFAULT_MAX_RECORD_BYTES,
                    type_identities,
                ),
                &reader_identity,
                &owner,
            );
            let writer = run_writer(writer, &owner, changes, status_interval);
            let mut revisions = owner.revision.subscribe();
            let result = {
                let transport = async {
                    let (_reader, mut writer) = tokio::try_join!(reader, writer)?;
                    writer
                        .shutdown()
                        .await
                        .map_err(|e| PostgresError::Transport(e.to_string()))
                };
                tokio::pin!(transport);
                loop {
                    let deadline = owner.state.lock().unwrap().deadline;
                    if let Some(deadline) = deadline {
                        break tokio::time::timeout_at(deadline, &mut transport)
                            .await
                            .unwrap_or_else(|_| {
                                Err(PostgresError::Operational(
                                    "replication shutdown timed out".into(),
                                ))
                            });
                    }
                    tokio::select! {
                        result = &mut transport => break result,
                        _ = revisions.changed() => {}
                    }
                }
            };
            owner.finish(result);
        });
        Ok(ReplicationSession {
            identity,
            shared,
            worker: task.abort_handle(),
        })
    }
}

impl ReplicationSession {
    /// Lease the next decoded record; Commit publication remains explicit.
    pub async fn next(&self) -> Result<Option<ReplicationDelivery>, PostgresError> {
        let mut changes = self.shared.revision.subscribe();
        {
            let mut state = self.shared.state.lock().unwrap();
            if state.receiver_reserved {
                return Err(PostgresError::Interface(
                    "replication already has a pending receiver or delivery".into(),
                ));
            }
            state.receiver_reserved = true;
        }
        let reservation = ReceiverReservation {
            shared: Arc::clone(&self.shared),
        };
        loop {
            {
                let mut state = self.shared.state.lock().unwrap();
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
                if let Some(record) = state.front.take().or_else(|| state.queue.pop()) {
                    if record.commit_end_lsn().is_some()
                        && state.feedback.unacked_count() >= MAX_UNACKED_COMMITS
                    {
                        state.front = Some(record);
                    } else {
                        drop(state);
                        self.shared.signal_records();
                        return Ok(Some(ReplicationDelivery {
                            record: Some(record),
                            reservation,
                        }));
                    }
                }
                if state.reader_eof && state.front.is_none() {
                    return Ok(None);
                }
            }
            changes
                .changed()
                .await
                .map_err(|_| PostgresError::Transport("replication reader disappeared".into()))?;
        }
    }

    /// Borrow the verified identity retained across ownership transfer.
    pub fn identity(&self) -> &VerifiedReplicationIdentity {
        &self.identity
    }

    /// Return received write progress and commit-fenced flush/apply positions.
    pub fn status_positions(&self) -> (u64, u64, u64) {
        self.shared
            .state
            .lock()
            .unwrap()
            .feedback
            .status_positions()
    }

    /// Acknowledge an exact previously published Commit boundary.
    pub fn acknowledge(&self, commit_end_lsn: u64) -> Result<u64, PostgresError> {
        let mut state = self.shared.state.lock().unwrap();
        if state.closing {
            return Err(PostgresError::Interface(
                "replication session is closed".into(),
            ));
        }
        let result = state.feedback.acknowledge(commit_end_lsn)?;
        drop(state);
        self.shared.signal();
        Ok(result)
    }

    /// Report whether graceful shutdown or terminal cancellation has begun.
    pub fn is_closed(&self) -> bool {
        let state = self.shared.state.lock().unwrap();
        state.closing || state.terminal.is_some()
    }

    /// Fence the session and abort its owned tasks without entering a runtime.
    pub fn cancel(&self) {
        self.shared.state.lock().unwrap().closing = true;
        self.shared.finish(Ok(()));
        self.worker.abort();
    }

    /// Send final feedback and CopyDone under the configured shutdown deadline.
    pub async fn shutdown(&self) -> Result<(), PostgresError> {
        {
            let mut state = self.shared.state.lock().unwrap();
            state.closing = true;
            state
                .deadline
                .get_or_insert_with(|| Instant::now() + self.shared.shutdown_timeout);
            while state.queue.pop().is_some() {}
            state.front = None;
        }
        self.shared.signal();
        self.wait_closed().await
    }

    /// Await stable terminal status; cancelling this waiter leaves transport ownership intact.
    pub async fn wait_closed(&self) -> Result<(), PostgresError> {
        let mut changes = self.shared.revision.subscribe();
        loop {
            if let Some(result) = self.shared.state.lock().unwrap().terminal.clone() {
                return result;
            }
            changes
                .changed()
                .await
                .map_err(|_| PostgresError::Transport("replication writer disappeared".into()))?;
        }
    }
}

/// A decoded record whose Commit boundary is published only on successful consumption.
pub struct ReplicationDelivery {
    record: Option<ReplicationRecord>,
    reservation: ReceiverReservation,
}

struct ReceiverReservation {
    shared: Arc<SessionShared>,
}

impl Drop for ReceiverReservation {
    fn drop(&mut self) {
        self.shared.state.lock().unwrap().receiver_reserved = false;
        self.shared.signal_records();
    }
}

impl Drop for ReplicationDelivery {
    fn drop(&mut self) {
        if let Some(record) = self.record.take() {
            let mut state = self.reservation.shared.state.lock().unwrap();
            if !state.closing {
                state.front = Some(record);
            }
        }
    }
}

impl ReplicationDelivery {
    /// Borrow the decoded record without advancing publication or acknowledgement.
    pub fn record(&self) -> &ReplicationRecord {
        self.record.as_ref().expect("live delivery owns its record")
    }

    /// Publish one record, making an exact Commit boundary acknowledgeable.
    pub fn publish(mut self) -> Result<ReplicationRecord, PostgresError> {
        let mut state = self.reservation.shared.state.lock().unwrap();
        if state.closing {
            return Err(PostgresError::Interface(
                "replication session is closed".into(),
            ));
        }
        if let Some(lsn) = self.record().commit_end_lsn() {
            state.feedback.record_published_commit(lsn)?;
        }
        drop(state);
        Ok(self.record.take().expect("live delivery owns its record"))
    }
}

async fn run_reader(
    mut reader: ReadHalf<ConnectionStream>,
    mut read_buf: BytesMut,
    mut catalog: TypeCatalog,
    codec_ctx: CodecContext,
    mut decoder: PgOutputDecoder,
    identity: &VerifiedReplicationIdentity,
    shared: &SessionShared,
) -> Result<ReadHalf<ConnectionStream>, PostgresError> {
    let mut changes = shared.revision.subscribe();
    let mut pending = None;
    loop {
        if shared.state.lock().unwrap().closing {
            drop(pending);
            drop(decoder);
            drain_backend(&mut reader, &mut read_buf, false).await?;
            return Ok(reader);
        }
        if shared.state.lock().unwrap().feedback.unacked_count() >= MAX_UNACKED_COMMITS {
            changes
                .changed()
                .await
                .map_err(|_| PostgresError::Transport("replication session disappeared".into()))?;
            continue;
        }
        if let Some(record) = pending.take() {
            let bytes = ReplicationRecord::approx_bytes(&record);
            if bytes > DEFAULT_MAX_RECORD_BYTES {
                return Err(PostgresError::Operational(
                    "replication record size exceeded".into(),
                ));
            }
            {
                let mut state = shared.state.lock().unwrap();
                if state.queue.can_accept(bytes) {
                    state.queue.push(record)?;
                } else {
                    pending = Some(record);
                }
            }
            if pending.is_some() {
                changes.changed().await.map_err(|_| {
                    PostgresError::Transport("replication session disappeared".into())
                })?;
            } else {
                shared.signal_records();
            }
            continue;
        }
        if let Some(record) = decoder.next_record(identity, &mut catalog, &codec_ctx)? {
            if let Some(lsn) = record.commit_end_lsn() {
                shared
                    .state
                    .lock()
                    .unwrap()
                    .feedback
                    .record_received_commit(lsn)?;
            }
            pending = Some(record);
            continue;
        }
        let message = tokio::select! {
            result = read_session_message(&mut reader, &mut read_buf) => result?,
            _ = changes.changed() => continue,
        };
        match message {
            ReplicationBackendMessage::Standard(Message::CopyData(body)) => {
                match decode_replication_copy_data(body.data())? {
                    ReplicationCopyData::XLogData { payload, .. } => decoder.push(payload)?,
                    ReplicationCopyData::PrimaryKeepalive {
                        reply_requested: true,
                        ..
                    } => shared.signal(),
                    ReplicationCopyData::PrimaryKeepalive { .. } => {}
                }
            }
            ReplicationBackendMessage::Standard(Message::CopyDone) => {
                decoder.finish_clean_eof()?;
                {
                    let mut state = shared.state.lock().unwrap();
                    state.reader_eof = true;
                    state
                        .deadline
                        .get_or_insert_with(|| Instant::now() + shared.shutdown_timeout);
                }
                shared.signal();
                drain_backend(&mut reader, &mut read_buf, true).await?;
                return Ok(reader);
            }
            ReplicationBackendMessage::Standard(Message::ErrorResponse(body)) => {
                return Err(parse_error_fields(body.fields())?);
            }
            ReplicationBackendMessage::Standard(
                Message::NoticeResponse(_) | Message::ParameterStatus(_),
            ) => {}
            _ => {
                return Err(PostgresError::Protocol(
                    "unexpected replication backend message".into(),
                ));
            }
        }
    }
}

impl Drop for ReplicationSession {
    fn drop(&mut self) {
        self.cancel();
    }
}

async fn write_status(
    writer: &mut WriteHalf<ConnectionStream>,
    shared: &SessionShared,
) -> Result<(), PostgresError> {
    let (write, flush, apply) = shared.state.lock().unwrap().feedback.status_positions();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
        - 946_684_800_000_000;
    let payload = encode_standby_status_update(write, flush, apply, timestamp, false);
    let mut frame = [0; 39];
    frame[0] = b'd';
    frame[1..5].copy_from_slice(&38_u32.to_be_bytes());
    frame[5..].copy_from_slice(&payload);
    writer
        .write_all(&frame)
        .await
        .map_err(|e| PostgresError::Transport(e.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|e| PostgresError::Transport(e.to_string()))?;
    Ok(())
}

async fn run_writer(
    mut writer: WriteHalf<ConnectionStream>,
    shared: &SessionShared,
    mut changes: watch::Receiver<u64>,
    interval: Duration,
) -> Result<WriteHalf<ConnectionStream>, PostgresError> {
    let mut ticks = tokio::time::interval_at(Instant::now() + interval, interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let stopping = {
            let state = shared.state.lock().unwrap();
            state.closing || state.reader_eof
        };
        if stopping {
            write_status(&mut writer, shared).await?;
            writer
                .write_all(b"c\0\0\0\x04")
                .await
                .map_err(|e| PostgresError::Transport(e.to_string()))?;
            writer
                .flush()
                .await
                .map_err(|e| PostgresError::Transport(e.to_string()))?;
            return Ok(writer);
        }
        tokio::select! {
            _ = ticks.tick() => write_status(&mut writer, shared).await?,
            result = changes.changed() => {
                if result.is_err() { return Ok(writer); }
                let stopping = {
                    let state = shared.state.lock().unwrap();
                    state.closing || state.reader_eof
                };
                if !stopping { write_status(&mut writer, shared).await?; }
            }
        }
    }
}

async fn drain_backend(
    reader: &mut ReadHalf<ConnectionStream>,
    read_buf: &mut BytesMut,
    mut saw_copy_done: bool,
) -> Result<(), PostgresError> {
    let mut saw_command = false;
    loop {
        match read_session_message(reader, read_buf).await? {
            ReplicationBackendMessage::Standard(Message::CopyDone) if !saw_copy_done => {
                saw_copy_done = true
            }
            ReplicationBackendMessage::Standard(Message::CommandComplete(_))
                if saw_copy_done && !saw_command =>
            {
                saw_command = true
            }
            ReplicationBackendMessage::Standard(Message::ReadyForQuery(_)) if saw_command => {
                return Ok(());
            }
            ReplicationBackendMessage::Standard(Message::ErrorResponse(body)) => {
                return Err(parse_error_fields(body.fields())?);
            }
            ReplicationBackendMessage::Standard(
                Message::NoticeResponse(_) | Message::ParameterStatus(_),
            ) => {}
            ReplicationBackendMessage::Standard(Message::CopyData(_)) if !saw_copy_done => {}
            _ => {
                return Err(PostgresError::Protocol(
                    "unexpected replication shutdown backend message".into(),
                ));
            }
        }
    }
}

async fn read_session_message(
    reader: &mut ReadHalf<ConnectionStream>,
    read_buf: &mut BytesMut,
) -> Result<ReplicationBackendMessage, PostgresError> {
    read_replication_message(reader, read_buf, MAX_BACKEND_FRAME_LENGTH, None)
        .await
        .map_err(|error| match error {
            PostgresError::Transport(message) if !read_buf.is_empty() => {
                PostgresError::Protocol(format!("truncated replication backend frame: {message}"))
            }
            other => other,
        })
}
