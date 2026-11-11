//! Explicit transaction ownership, savepoint stack tracking, and nested `TransactionScope` state.

use crate::errors::PostgresError;
use crate::result::TransactionStatus;

/// Logical owner token pinning a session across explicit transactions, portals, or pipelines.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TransactionOwner {
    /// Logical caller or scope identifier (`>= 1`).
    pub id: u64,
    /// Monotonic session lease generation when ownership was acquired.
    pub generation: u64,
}

impl TransactionOwner {
    /// Construct a [`TransactionOwner`] for `(id, generation)`.
    pub const fn new(id: u64, generation: u64) -> Self {
        Self { id, generation }
    }
}

/// Confirmed server disposition after a transaction or savepoint control operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionDisposition {
    /// Server entered `ReadyForQuery('T')` via `BEGIN`.
    Begun,
    /// Server confirmed `CommandComplete("COMMIT")` and `ReadyForQuery('I')`.
    Committed,
    /// Server confirmed `ROLLBACK` and `ReadyForQuery('I')`.
    RolledBack,
    /// Server created savepoint `vtb_sp_<token>` in `'T'`.
    SavepointCreated(u64),
    /// Server released savepoint `vtb_sp_<token>` in `'T'`.
    SavepointReleased(u64),
    /// Server rolled back to savepoint `vtb_sp_<token>` and restored `'T'`.
    RolledBackToSavepoint(u64),
}

/// Discriminant distinguishing an outer `BEGIN` scope from a nested savepoint inside ambient `'T'`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionScopeKind {
    /// Scope started an explicit `BEGIN` from `Idle` (`'I'`).
    ExplicitTransaction,
    /// Scope created a nested `SAVEPOINT vtb_sp_<token>` inside an already-active `'T'` transaction.
    NestedSavepoint {
        /// Monotonic numeric savepoint token.
        token: u64,
    },
}

/// Pinned transaction or savepoint scope used by portals and pipelines.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionScope {
    /// Logical owner pinning the session.
    pub owner: TransactionOwner,
    /// Whether this scope owns the outer transaction or a nested savepoint.
    pub kind: TransactionScopeKind,
    /// Whether the scope encountered an error requiring rollback on exit.
    pub failed: bool,
}

impl TransactionScope {
    /// Construct a new [`TransactionScope`].
    pub const fn new(owner: TransactionOwner, kind: TransactionScopeKind) -> Self {
        Self {
            owner,
            kind,
            failed: false,
        }
    }
}

/// Format a deterministic internal savepoint identifier from a numeric `token`.
pub fn format_savepoint_ident(token: u64) -> String {
    format!("vtb_sp_{token}")
}

/// Format a `SAVEPOINT vtb_sp_<token>` command string.
pub fn build_savepoint_sql(token: u64) -> String {
    format!("SAVEPOINT vtb_sp_{token}")
}

/// Format a `RELEASE SAVEPOINT vtb_sp_<token>` command string.
pub fn build_release_savepoint_sql(token: u64) -> String {
    format!("RELEASE SAVEPOINT vtb_sp_{token}")
}

/// Format a `ROLLBACK TO SAVEPOINT vtb_sp_<token>` command string.
pub fn build_rollback_to_savepoint_sql(token: u64) -> String {
    format!("ROLLBACK TO SAVEPOINT vtb_sp_{token}")
}

/// Per-connection transaction and numeric savepoint stack tracker synchronized with `ReadyForQuery`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransactionState {
    status: TransactionStatus,
    savepoints: Vec<u64>,
}

impl TransactionState {
    /// Construct a new `Idle` [`TransactionState`].
    pub const fn new() -> Self {
        Self {
            status: TransactionStatus::Idle,
            savepoints: Vec::new(),
        }
    }

    /// Return the last synchronized [`TransactionStatus`].
    pub const fn status(&self) -> TransactionStatus {
        self.status
    }

    /// Return the active numeric savepoint stack in creation order.
    pub fn savepoints(&self) -> &[u64] {
        &self.savepoints
    }

    /// Synchronize state from a wire `ReadyForQuery` status indicator.
    pub fn sync_from_wire(&mut self, status: TransactionStatus) {
        self.status = status;
        if status == TransactionStatus::Idle {
            self.savepoints.clear();
        }
    }

    /// Validate that `BEGIN` is legal in the current state.
    pub fn validate_begin(&self) -> Result<&'static str, PostgresError> {
        match self.status {
            TransactionStatus::Idle => Ok("BEGIN"),
            TransactionStatus::InTransaction => Err(PostgresError::Interface(
                "cannot BEGIN: transaction is already in progress ('T')".to_string(),
            )),
            TransactionStatus::Failed => Err(PostgresError::Interface(
                "cannot BEGIN: current transaction is aborted ('E'); call rollback() first"
                    .to_string(),
            )),
        }
    }

    /// Validate that `COMMIT` can be sent in the current state.
    pub fn validate_commit(&self) -> Result<&'static str, PostgresError> {
        match self.status {
            TransactionStatus::Idle => Err(PostgresError::Interface(
                "cannot COMMIT: no transaction is in progress ('I')".to_string(),
            )),
            TransactionStatus::InTransaction | TransactionStatus::Failed => Ok("COMMIT"),
        }
    }

    /// Validate that `ROLLBACK` can be sent in the current state.
    pub fn validate_rollback(&self) -> Result<&'static str, PostgresError> {
        match self.status {
            TransactionStatus::Idle => Err(PostgresError::Interface(
                "cannot ROLLBACK: no transaction is in progress ('I')".to_string(),
            )),
            TransactionStatus::InTransaction | TransactionStatus::Failed => Ok("ROLLBACK"),
        }
    }

    /// Validate that `SAVEPOINT vtb_sp_<token>` can be created in the current state.
    pub fn validate_savepoint(&self, token: u64) -> Result<String, PostgresError> {
        match self.status {
            TransactionStatus::InTransaction => Ok(build_savepoint_sql(token)),
            TransactionStatus::Idle => Err(PostgresError::Interface(
                "cannot create SAVEPOINT outside an active transaction ('I')".to_string(),
            )),
            TransactionStatus::Failed => Err(PostgresError::Interface(
                "cannot create SAVEPOINT while transaction is aborted ('E')".to_string(),
            )),
        }
    }

    /// Record a newly created savepoint `token` after server confirmation.
    pub fn record_savepoint(&mut self, token: u64, status: TransactionStatus) {
        self.sync_from_wire(status);
        if status == TransactionStatus::InTransaction {
            self.savepoints.push(token);
        }
    }

    /// Validate that `ROLLBACK TO SAVEPOINT vtb_sp_<token>` targets an active savepoint.
    pub fn validate_rollback_to_savepoint(
        &self,
        token: u64,
    ) -> Result<(usize, String), PostgresError> {
        if self.status == TransactionStatus::Idle {
            return Err(PostgresError::Interface(
                "cannot ROLLBACK TO SAVEPOINT outside an active transaction ('I')".to_string(),
            ));
        }
        let Some(idx) = self.savepoints.iter().rposition(|&t| t == token) else {
            return Err(PostgresError::Interface(format!(
                "savepoint vtb_sp_{token} is not active on this transaction"
            )));
        };
        Ok((idx, build_rollback_to_savepoint_sql(token)))
    }

    /// Apply savepoint stack unwinding after `ROLLBACK TO SAVEPOINT vtb_sp_<token>` completes.
    pub fn apply_rollback_to_savepoint(&mut self, idx: usize, status: TransactionStatus) {
        self.savepoints.truncate(idx + 1);
        self.sync_from_wire(status);
    }

    /// Validate that `RELEASE SAVEPOINT vtb_sp_<token>` targets an active savepoint in `'T'`.
    pub fn validate_release_savepoint(&self, token: u64) -> Result<(usize, String), PostgresError> {
        match self.status {
            TransactionStatus::Idle => {
                return Err(PostgresError::Interface(
                    "cannot RELEASE SAVEPOINT outside an active transaction ('I')".to_string(),
                ));
            }
            TransactionStatus::Failed => {
                return Err(PostgresError::Interface(
                    "cannot RELEASE SAVEPOINT while transaction is aborted ('E')".to_string(),
                ));
            }
            TransactionStatus::InTransaction => {}
        }
        let Some(idx) = self.savepoints.iter().rposition(|&t| t == token) else {
            return Err(PostgresError::Interface(format!(
                "savepoint vtb_sp_{token} is not active on this transaction"
            )));
        };
        Ok((idx, build_release_savepoint_sql(token)))
    }

    /// Apply savepoint stack removal after `RELEASE SAVEPOINT vtb_sp_<token>` completes.
    pub fn apply_release_savepoint(&mut self, idx: usize, status: TransactionStatus) {
        self.savepoints.truncate(idx);
        self.sync_from_wire(status);
    }
}
