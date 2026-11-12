//! Logical replication payload codecs and commit-fenced status primitives.

use std::collections::VecDeque;
use std::time::Duration;

use crate::errors::PostgresError;

/// Maximum number of published Commit boundaries awaiting acknowledgement.
pub const MAX_UNACKED_COMMITS: usize = 1024;
/// Default interval between standby status updates when no timeout constrains it.
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);

/// Borrowed PostgreSQL replication `CopyData` payload.
#[derive(Debug, PartialEq, Eq)]
pub enum ReplicationCopyData<'a> {
    /// WAL position metadata and an uninterpreted logical replication payload.
    XLogData {
        wal_start: u64,
        wal_end: u64,
        server_time_pg_us: i64,
        payload: &'a [u8],
    },
    /// Server liveness notification and optional immediate-response request.
    PrimaryKeepalive {
        wal_end: u64,
        server_time_pg_us: i64,
        reply_requested: bool,
    },
}

/// Decode a replication payload, retaining the logical WAL data as a borrowed slice.
pub fn decode_replication_copy_data(
    frame: &[u8],
) -> Result<ReplicationCopyData<'_>, PostgresError> {
    match frame.first() {
        Some(b'w') if frame.len() >= 25 => Ok(ReplicationCopyData::XLogData {
            wal_start: u64::from_be_bytes(frame[1..9].try_into().unwrap()),
            wal_end: u64::from_be_bytes(frame[9..17].try_into().unwrap()),
            server_time_pg_us: i64::from_be_bytes(frame[17..25].try_into().unwrap()),
            payload: &frame[25..],
        }),
        Some(b'k') if frame.len() == 18 && frame[17] <= 1 => {
            Ok(ReplicationCopyData::PrimaryKeepalive {
                wal_end: u64::from_be_bytes(frame[1..9].try_into().unwrap()),
                server_time_pg_us: i64::from_be_bytes(frame[9..17].try_into().unwrap()),
                reply_requested: frame[17] != 0,
            })
        }
        _ => Err(PostgresError::Protocol(
            "invalid replication CopyData tag, length, or keepalive reply flag".to_owned(),
        )),
    }
}

/// Encode a standby status payload; the caller supplies frontend `CopyData` framing.
pub fn encode_standby_status_update(
    write_lsn: u64,
    flush_lsn: u64,
    apply_lsn: u64,
    client_time_pg_us: i64,
    reply_requested: bool,
) -> [u8; 34] {
    let mut payload = [0; 34];
    payload[0] = b'r';
    payload[1..9].copy_from_slice(&write_lsn.to_be_bytes());
    payload[9..17].copy_from_slice(&flush_lsn.to_be_bytes());
    payload[17..25].copy_from_slice(&apply_lsn.to_be_bytes());
    payload[25..33].copy_from_slice(&client_time_pg_us.to_be_bytes());
    payload[33] = u8::from(reply_requested);
    payload
}

/// Bound the heartbeat interval by one third of the configured sender timeout.
pub fn compute_status_interval(wal_sender_timeout: Option<Duration>) -> Duration {
    match wal_sender_timeout {
        Some(timeout) if !timeout.is_zero() => {
            (timeout / 3).clamp(Duration::from_millis(100), DEFAULT_STATUS_INTERVAL)
        }
        _ => DEFAULT_STATUS_INTERVAL,
    }
}

/// Complete received and published Commit boundaries, fenced by caller acknowledgement.
#[derive(Debug)]
pub struct ReplicationFeedback {
    received_complete_lsn: u64,
    published_commits: VecDeque<u64>,
    acknowledged_commit_lsn: u64,
}

impl ReplicationFeedback {
    /// Initialize all status positions at the validated replication start position.
    pub fn new(initial_lsn: u64) -> Self {
        Self {
            received_complete_lsn: initial_lsn,
            published_commits: VecDeque::new(),
            acknowledged_commit_lsn: initial_lsn,
        }
    }

    /// Advance received progress only after decoding a complete Commit.
    pub fn record_received_commit(&mut self, commit_end_lsn: u64) -> Result<(), PostgresError> {
        if commit_end_lsn < self.received_complete_lsn {
            return Err(PostgresError::Interface(
                "received replication Commit position regressed".to_owned(),
            ));
        }
        self.received_complete_lsn = commit_end_lsn;
        Ok(())
    }

    /// Register a Commit after actual consumer publication, retaining bounded pending state.
    pub fn record_published_commit(&mut self, commit_end_lsn: u64) -> Result<(), PostgresError> {
        let last_published = self
            .published_commits
            .back()
            .copied()
            .unwrap_or(self.acknowledged_commit_lsn);
        if commit_end_lsn <= last_published || commit_end_lsn > self.received_complete_lsn {
            return Err(PostgresError::Interface(
                "published replication Commit must advance within received progress".to_owned(),
            ));
        }
        if self.published_commits.len() >= MAX_UNACKED_COMMITS {
            return Err(PostgresError::Interface(
                "replication has reached its unacknowledged Commit limit".to_owned(),
            ));
        }
        self.published_commits.push_back(commit_end_lsn);
        Ok(())
    }

    /// Cumulatively acknowledge an exact published Commit boundary without accepting regressions.
    pub fn acknowledge(&mut self, commit_end_lsn: u64) -> Result<u64, PostgresError> {
        if commit_end_lsn == self.acknowledged_commit_lsn {
            return Ok(commit_end_lsn);
        }
        let Some(index) = self
            .published_commits
            .iter()
            .position(|lsn| *lsn == commit_end_lsn)
        else {
            return Err(PostgresError::Interface(
                "acknowledgement requires a current published replication Commit boundary"
                    .to_owned(),
            ));
        };
        self.published_commits.drain(..=index);
        self.acknowledged_commit_lsn = commit_end_lsn;
        Ok(commit_end_lsn)
    }

    /// Return write, flush, and apply positions without treating receipt as durability.
    pub fn status_positions(&self) -> (u64, u64, u64) {
        (
            self.received_complete_lsn.max(self.acknowledged_commit_lsn),
            self.acknowledged_commit_lsn,
            self.acknowledged_commit_lsn,
        )
    }

    /// Count published Commit boundaries not yet covered by an acknowledgement.
    pub fn unacked_count(&self) -> usize {
        self.published_commits.len()
    }
}
