//! Retained transaction/savepoint scope across caller-side pipeline result conversion
//! (`SessionAccess::Pipeline`, `PipelineScopeToken`, and PyO3 conversion helpers).

use pyo3::prelude::*;

use crate::errors::{PostgresError, postgres_error_to_py};
use crate::pipeline::{PipelineBatch, PipelineOutcome};
use crate::python_values::{extract_pipeline_operations, pipeline_outcomes_to_python};
use crate::session::statements::PoolerMode;
use crate::session::{SessionAccess, SessionHandle, TransactionStatus};
use crate::transaction::{TransactionOwner, TransactionScope, TransactionScopeKind};

/// Opaque monotonic token identifying an active retained pipeline transaction/savepoint scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PipelineScopeToken(pub u64);

/// Internal state of an active pipeline scope held inside `SessionInner` across result hydration.
#[derive(Clone, Debug)]
pub(crate) struct ActivePipelineScope {
    pub(crate) token: PipelineScopeToken,
    pub(crate) scope: TransactionScope,
    pub(crate) had_ambient_tx: bool,
    pub(crate) pre_pinned_owner: Option<TransactionOwner>,
    pub(crate) submitted: bool,
    pub(crate) failed: bool,
    pub(crate) primary_error: Option<PostgresError>,
}

impl SessionHandle {
    /// Open a retained pipeline transaction (`BEGIN` at `'I'`) or savepoint (`SAVEPOINT vtb_sp_<n>` at `'T'`)
    /// outside the measured single-`Sync` data batch and pin exclusive [`SessionAccess::Pipeline`] ownership.
    pub async fn pipeline_begin(&self) -> Result<PipelineScopeToken, PostgresError> {
        self.pipeline_begin_as(None).await
    }

    /// Open a retained pipeline transaction or nested savepoint scope on behalf of `owner`.
    pub async fn pipeline_begin_as(
        &self,
        owner: Option<TransactionOwner>,
    ) -> Result<PipelineScopeToken, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.pipeline_begin_as_with_generation(op_gen, owner).await
    }

    pub(crate) async fn pipeline_begin_with_generation(
        &self,
        op_gen: crate::session::cancel::OperationGeneration,
    ) -> Result<PipelineScopeToken, PostgresError> {
        self.pipeline_begin_as_with_generation(op_gen, None).await
    }

    pub(crate) async fn pipeline_begin_as_with_generation(
        &self,
        op_gen: crate::session::cancel::OperationGeneration,
        owner: Option<TransactionOwner>,
    ) -> Result<PipelineScopeToken, PostgresError> {
        self.run_cancellable_operation(op_gen, SessionAccess::Ordinary, owner, |conn, meta| {
            Box::pin(async move {
                if !conn.pooler_capabilities().supports_scoped_state()
                    || conn.pooler_capabilities().effective_mode == PoolerMode::Statement
                {
                    return Err(PostgresError::Interface(
                        "cannot start a pipeline scope in PoolerMode::Statement".to_string(),
                    ));
                }
                if conn.transaction_status() == TransactionStatus::Failed {
                    return Err(PostgresError::Interface(
                        "cannot start a pipeline scope while transaction is aborted ('E')"
                            .to_string(),
                    ));
                }

                let raw_token = meta.next_pipeline_token;
                let pipe_token = PipelineScopeToken(raw_token);
                let owner =
                    TransactionOwner::new(0x4000_0000_0000_0000 | raw_token, conn.generation());

                let (scope, had_ambient_tx) = match conn.transaction_status() {
                    TransactionStatus::Idle => {
                        conn.begin().await?;
                        (
                            TransactionScope::new(owner, TransactionScopeKind::ExplicitTransaction),
                            false,
                        )
                    }
                    TransactionStatus::InTransaction => {
                        let sp = conn.next_savepoint_token();
                        conn.savepoint(sp).await?;
                        (
                            TransactionScope::new(
                                owner,
                                TransactionScopeKind::NestedSavepoint { token: sp },
                            ),
                            true,
                        )
                    }
                    TransactionStatus::Failed => {
                        return Err(PostgresError::Interface(
                            "cannot start a pipeline scope while transaction is aborted ('E')"
                                .to_string(),
                        ));
                    }
                };

                meta.install_pipeline_scope = Some(ActivePipelineScope {
                    token: pipe_token,
                    scope,
                    had_ambient_tx,
                    pre_pinned_owner: meta.pre_pinned_owner,
                    submitted: false,
                    failed: false,
                    primary_error: None,
                });
                Ok(pipe_token)
            })
        })
        .await
    }

    /// Execute `batch` inside the active pipeline scope `token` as one single-`Sync` extended batch,
    /// leaving the transaction/savepoint open for caller-side Python row/cardinality conversion.
    pub async fn pipeline_results(
        &self,
        token: PipelineScopeToken,
        batch: PipelineBatch,
    ) -> Result<Vec<PipelineOutcome>, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.pipeline_results_with_generation(op_gen, token, batch)
            .await
    }

    pub(crate) async fn pipeline_results_with_generation(
        &self,
        op_gen: crate::session::cancel::OperationGeneration,
        token: PipelineScopeToken,
        batch: PipelineBatch,
    ) -> Result<Vec<PipelineOutcome>, PostgresError> {
        if token.0 == 0 {
            return Err(PostgresError::Interface(
                "invalid pipeline scope token 0".to_string(),
            ));
        }
        self.run_cancellable_operation(
            op_gen,
            SessionAccess::Pipeline(token),
            None,
            move |conn, meta| {
                Box::pin(async move {
                    if conn.is_closed() {
                        return Err(PostgresError::Interface(
                            "session connection is closed".to_string(),
                        ));
                    }
                    if meta.pipeline_already_submitted {
                        return Err(PostgresError::Interface(
                            "pipeline_results has already been called for this pipeline scope"
                                .to_string(),
                        ));
                    }
                    if meta.pipeline_already_failed {
                        return Err(PostgresError::Interface(
                            "pipeline scope has already failed and must be finished with rollback"
                                .to_string(),
                        ));
                    }
                    meta.mark_pipeline_submitted = true;

                    let outcome = match conn.submit_pipeline(&batch).await {
                        Ok(submitted) => conn
                            .collect_submitted_pipeline(submitted)
                            .await
                            .map_err(|failure| PostgresError::PipelineAborted(Box::new(failure))),
                        Err(err) => Err(err),
                    };

                    if let Err(ref err) = outcome {
                        meta.mark_pipeline_failed = Some(err.clone());
                    }
                    outcome
                })
            },
        )
        .await
    }

    /// Record a caller-side preparation/snapshot failure against `token` so `pipeline_finish(token, true)`
    /// still rolls back even if a caller catches the error and attempts to commit.
    pub fn mark_pipeline_scope_failed(
        &self,
        token: PipelineScopeToken,
        error: PostgresError,
    ) -> Result<(), PostgresError> {
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        let Some(ref mut active) = guard.active_pipeline else {
            return Err(PostgresError::Interface(
                "no active pipeline scope on this session".to_string(),
            ));
        };
        if active.token != token {
            return Err(PostgresError::Interface(format!(
                "pipeline scope token {} does not match active scope {}",
                token.0, active.token.0
            )));
        }
        active.failed = true;
        if active.primary_error.is_none() {
            active.primary_error = Some(error);
        }
        Ok(())
    }

    /// Finish the retained pipeline scope `token`, committing/releasing only when `success == true`
    /// and no preflight, server, or cancellation failure occurred during the scope.
    pub async fn pipeline_finish(
        &self,
        token: PipelineScopeToken,
        success: bool,
    ) -> Result<(), PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.pipeline_finish_with_generation(op_gen, token, success)
            .await
    }

    pub(crate) async fn pipeline_finish_with_generation(
        &self,
        op_gen: crate::session::cancel::OperationGeneration,
        token: PipelineScopeToken,
        success: bool,
    ) -> Result<(), PostgresError> {
        if token.0 == 0 {
            return Err(PostgresError::Interface(
                "invalid pipeline scope token 0".to_string(),
            ));
        }
        self.run_cancellable_operation(
            op_gen,
            SessionAccess::Pipeline(token),
            None,
            move |conn, meta| {
                Box::pin(async move {
                    let finishing = meta
                        .finish_pipeline_scope
                        .take()
                        .expect("SessionAccess::Pipeline verified active scope");
                    meta.clear_pipeline_scope =
                        Some((finishing.had_ambient_tx, finishing.pre_pinned_owner));

                    let should_commit = success
                        && !finishing.failed
                        && !meta.retire_pending
                        && !meta.ambient_cancel_failed
                        && !conn.is_closed()
                        && conn.transaction_status() == TransactionStatus::InTransaction;

                    if should_commit {
                        match finishing.scope.kind {
                            TransactionScopeKind::ExplicitTransaction => {
                                conn.commit().await?;
                            }
                            TransactionScopeKind::NestedSavepoint { token: sp } => {
                                conn.release_savepoint(sp).await?;
                            }
                        }
                        return Ok(());
                    }

                    let rollback_res = rollback_pipeline_scope(conn, &finishing.scope).await;

                    if meta.retire_pending && !finishing.had_ambient_tx {
                        meta.force_retire_now = true;
                    }

                    if let Err(rb_err) = rollback_res
                        && !meta.retire_pending
                    {
                        conn.mark_closed();
                        meta.force_retire_now = true;
                        if success && let Some(primary) = finishing.primary_error {
                            return Err(primary);
                        }
                        return Err(rb_err);
                    }

                    if success && finishing.failed {
                        return Err(finishing.primary_error.unwrap_or_else(|| {
                            PostgresError::Interface(
                                "pipeline scope failed earlier and was rolled back".to_string(),
                            )
                        }));
                    }
                    if success && (meta.retire_pending || meta.ambient_cancel_failed) {
                        return Err(PostgresError::Interface(
                            "cancelled pipeline scope was rolled back and cannot commit"
                                .to_string(),
                        ));
                    }

                    Ok(())
                })
            },
        )
        .await
    }
}

pub(crate) async fn rollback_pipeline_scope(
    conn: &mut crate::session::PostgresConnection,
    scope: &TransactionScope,
) -> Result<(), PostgresError> {
    if conn.is_closed() {
        return Ok(());
    }
    match scope.kind {
        TransactionScopeKind::ExplicitTransaction => {
            if conn.transaction_status() != TransactionStatus::Idle {
                conn.rollback().await?;
            }
            Ok(())
        }
        TransactionScopeKind::NestedSavepoint { token: sp } => {
            if conn.transaction_status() != TransactionStatus::Idle {
                conn.rollback_to_savepoint(sp).await?;
                conn.release_savepoint(sp).await?;
            }
            Ok(())
        }
    }
}

/// Extract a Python `Sequence[PostgresPipelineOperation]` into an owned [`PipelineBatch`] while holding the GIL.
pub fn extract_pipeline_batch(
    py: Python<'_>,
    operations: &Bound<'_, PyAny>,
) -> Result<PipelineBatch, PostgresError> {
    let ops = extract_pipeline_operations(py, operations)?;
    Ok(PipelineBatch::new(ops))
}

/// Project ordered Rust [`PipelineOutcome`] items into `tuple[PostgresPipelineOutcome, ...]`.
pub fn outcomes_to_python(py: Python<'_>, outcomes: &[PipelineOutcome]) -> PyResult<Py<PyAny>> {
    pipeline_outcomes_to_python(py, outcomes)
}

/// Convert a pipeline [`PostgresError`] into its canonical `vertebra._vertebra` exception with `.outcomes` and `.failing_index`.
pub fn pipeline_error_to_py(py: Python<'_>, err: PostgresError) -> PyErr {
    postgres_error_to_py(py, err)
}
