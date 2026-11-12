//! Split-owned logical replication transport and independent standby status writer.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio::time::{Instant, MissedTickBehavior};

use super::feedback::{ReplicationFeedback, encode_standby_status_update};
use super::{PostgresReplicationConnection, VerifiedReplicationIdentity};
use crate::codecs::CodecContext;
use crate::errors::PostgresError;
use crate::session::ConnectionStream;
use crate::types::TypeCatalog;

struct SessionState {
    feedback: ReplicationFeedback,
    closing: bool,
    terminal: Option<Result<(), PostgresError>>,
}

struct SessionShared {
    state: Mutex<SessionState>,
    revision: watch::Sender<u64>,
}

impl SessionShared {
    fn signal(&self) {
        self.revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    fn finish(&self, result: Result<(), PostgresError>) {
        let mut state = self.state.lock().unwrap();
        state.closing = true;
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
    workers: Vec<AbortHandle>,
    _reader: ReadHalf<ConnectionStream>,
    _read_buf: BytesMut,
    _type_catalog: TypeCatalog,
    _type_identities: std::collections::HashMap<u32, (String, String)>,
    _codec_ctx: CodecContext,
    _queue_capacity: usize,
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
        let (revision, changes) = watch::channel(0);
        let shared = Arc::new(SessionShared {
            state: Mutex::new(SessionState {
                feedback: ReplicationFeedback::new(effective_start_lsn),
                closing: false,
                terminal: None,
            }),
            revision,
        });
        let owner = Arc::clone(&shared);
        let task = runtime.spawn(async move {
            let result =
                run_writer(writer, &owner, changes, status_interval, shutdown_timeout).await;
            owner.finish(result);
        });
        Ok(ReplicationSession {
            identity,
            shared,
            workers: vec![task.abort_handle()],
            _reader: reader,
            _read_buf: conn.read_buf,
            _type_catalog: type_catalog,
            _type_identities: type_identities,
            _codec_ctx: codec_ctx,
            _queue_capacity: queue_capacity,
        })
    }
}

impl ReplicationSession {
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
        self.shared.state.lock().unwrap().closing
    }

    /// Fence the session and abort its owned tasks without entering a runtime.
    pub fn cancel(&self) {
        self.shared.finish(Ok(()));
        for worker in &self.workers {
            worker.abort();
        }
    }

    /// Send final feedback and CopyDone under the configured shutdown deadline.
    pub async fn shutdown(&self) -> Result<(), PostgresError> {
        self.shared.state.lock().unwrap().closing = true;
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
    shutdown_timeout: Duration,
) -> Result<(), PostgresError> {
    let mut ticks = tokio::time::interval_at(Instant::now() + interval, interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        if shared.state.lock().unwrap().closing {
            return tokio::time::timeout(shutdown_timeout, async {
                write_status(&mut writer, shared).await?;
                writer
                    .write_all(b"c\0\0\0\x04")
                    .await
                    .map_err(|e| PostgresError::Transport(e.to_string()))?;
                writer
                    .flush()
                    .await
                    .map_err(|e| PostgresError::Transport(e.to_string()))?;
                writer
                    .shutdown()
                    .await
                    .map_err(|e| PostgresError::Transport(e.to_string()))?;
                Ok(())
            })
            .await
            .map_err(|_| PostgresError::Operational("replication shutdown timed out".into()))?;
        }
        tokio::select! {
            _ = ticks.tick() => {
                tokio::select! {
                    result = write_status(&mut writer, shared) => result?,
                    _ = changes.changed() => continue,
                }
            }
            result = changes.changed() => {
                if result.is_err() { return Ok(()); }
            }
        }
    }
}
