//! Concrete PostgreSQL Protocol 3.0 bounded `Portal` streaming (`Execute(max_rows)`, `PortalSuspended`, `TransactionScope` pinning).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;

use crate::arrow::{
    ArrowOutput, ArrowStorageMode, DEFAULT_ARROW_BATCH_BYTES, DEFAULT_ARROW_BATCH_ROWS,
    PostgresArrowBatchBuilder, PostgresArrowIpcWriter, PostgresArrowSchema,
};
use crate::codecs::{
    CodecContext, TypeCatalog, decode_value_with_catalog, format_for_oid_with_catalog,
};
use crate::errors::PostgresError;
use crate::extended::{Parameter, PreparedDescription, parse_parameter_description};
use crate::protocol::read_message;
use crate::query::{
    COMMAND_IO_DEADLINE_EXCEEDED_MSG, parse_command_tag_affected_rows, parse_error_response,
    parse_row_description,
};
use crate::result::{Column, Format, PgValue, QueryResult, TransactionStatus};
use crate::session::cancel::OperationGeneration;
use crate::session::statements::PoolerMode;
use crate::session::{
    CommandBudget, ExchangeState, PostgresConnection, SessionAccess, SessionHandle,
};
use crate::transaction::{TransactionOwner, TransactionScope, TransactionScopeKind};

/// Streaming event emitted by [`Portal::next_event`].
#[derive(Clone, Debug, PartialEq)]
pub enum PortalEvent {
    /// Decoded row from an in-flight `Execute(max_rows)` chunk.
    Row(Vec<PgValue>),
    /// Backend sent `PortalSuspended` (`'s'`) and `ReadyForQuery('T')`; more rows remain.
    Suspended,
    /// Backend sent `CommandComplete` (`'C'`) or `EmptyQueryResponse` (`'I'`); portal is exhausted and closed.
    Complete {
        /// Exact server command completion tag.
        command_tag: String,
        /// Parsed affected row count from `command_tag`.
        affected_rows: u64,
    },
}

/// Session-pinned PostgreSQL named portal with row-limited `Execute(max_rows)` streaming.
pub struct Portal {
    session: Arc<SessionHandle>,
    portal_name: String,
    generation: u64,
    scope: TransactionScope,
    columns: Vec<Column>,
    codec_context: Arc<CodecContext>,
    type_catalog: TypeCatalog,
    exhausted: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    in_flight_execute: bool,
    max_result_bytes: usize,
}

impl std::fmt::Debug for Portal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Portal")
            .field("portal_name", &self.portal_name)
            .field("generation", &self.generation)
            .field("scope", &self.scope)
            .field("columns", &self.columns)
            .field("exhausted", &self.is_exhausted())
            .field("closed", &self.is_closed())
            .field("in_flight_execute", &self.in_flight_execute)
            .finish()
    }
}

impl Portal {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        session: Arc<SessionHandle>,
        portal_name: String,
        generation: u64,
        scope: TransactionScope,
        columns: Vec<Column>,
        codec_context: Arc<CodecContext>,
        type_catalog: TypeCatalog,
        max_result_bytes: usize,
    ) -> Self {
        Self {
            session,
            portal_name,
            generation,
            scope,
            columns,
            codec_context,
            type_catalog,
            exhausted: Arc::new(AtomicBool::new(false)),
            closed: Arc::new(AtomicBool::new(false)),
            in_flight_execute: false,
            max_result_bytes,
        }
    }

    /// Borrow the negotiated portal [`Column`] metadata.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Borrow the session [`TypeCatalog`] snapshot captured when the portal opened.
    pub fn type_catalog(&self) -> &TypeCatalog {
        &self.type_catalog
    }

    /// Return the unique server portal name (`vtb_p_<id>`).
    pub fn portal_name(&self) -> &str {
        &self.portal_name
    }

    /// Return `true` when the portal has reached `CommandComplete` / `EmptyQueryResponse`.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Acquire)
    }

    /// Return `true` when the portal has been closed or exhausted.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Return shared atomic handles for lock-free Python status inspection.
    pub fn status_flags(&self) -> (Arc<AtomicBool>, Arc<AtomicBool>) {
        (Arc::clone(&self.exhausted), Arc::clone(&self.closed))
    }

    /// Borrow the underlying [`SessionHandle`].
    pub fn session(&self) -> &Arc<SessionHandle> {
        &self.session
    }

    /// Validate `max_rows >= 1` and issue `Execute(portal_name, max_rows)` + `Sync` on the pinned session.
    pub async fn start_fetch(&mut self, max_rows: i32) -> Result<(), PostgresError> {
        if max_rows <= 0 {
            return Err(PostgresError::Interface(
                "Portal::start_fetch requires max_rows >= 1 (max_rows <= 0 is forbidden)"
                    .to_string(),
            ));
        }
        if self.is_closed() {
            return Err(PostgresError::Interface(
                "cannot fetch from a closed Portal".to_string(),
            ));
        }
        if self.is_exhausted() {
            return Err(PostgresError::Interface(
                "cannot fetch from an exhausted Portal".to_string(),
            ));
        }
        if self.in_flight_execute {
            return Err(PostgresError::Interface(
                "Portal already has an in-flight Execute; call next_event() to drain it first"
                    .to_string(),
            ));
        }

        let portal_name = self.portal_name.clone();
        let owner = self.scope.owner;
        let mut guard = self
            .session
            .acquire_portal_operation(owner, &portal_name)
            .await?;
        let conn = guard.connection_mut();

        conn.write_buf.clear();
        frontend::execute(&portal_name, max_rows, &mut conn.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Execute frame: {e}")))?;
        frontend::sync(&mut conn.write_buf);

        conn.set_exchange_state(ExchangeState::InExtendedExchange);
        if let Err(err) = conn.stream.write_all(&conn.write_buf).await {
            conn.write_buf.clear();
            conn.mark_closed();
            drop(guard);
            self.mark_closed_and_unpin();
            return Err(PostgresError::Transport(format!(
                "Portal Execute write failed: {err}"
            )));
        }
        conn.write_buf.clear();
        if let Err(err) = conn.stream.flush().await {
            conn.mark_closed();
            drop(guard);
            self.mark_closed_and_unpin();
            return Err(PostgresError::Transport(format!(
                "Portal Execute flush failed: {err}"
            )));
        }

        self.in_flight_execute = true;
        Ok(())
    }

    /// Read the next [`PortalEvent`] (`Row`, `Suspended`, or `Complete`) from an active `Execute(max_rows)` stream.
    pub async fn next_event(&mut self) -> Result<PortalEvent, PostgresError> {
        if self.is_closed() {
            return Err(PostgresError::Interface(
                "cannot read next_event from a closed Portal".to_string(),
            ));
        }
        if !self.in_flight_execute {
            return Err(PostgresError::Interface(
                "no in-flight Execute on Portal; call start_fetch(max_rows) first".to_string(),
            ));
        }

        let portal_name = self.portal_name.clone();
        let owner = self.scope.owner;
        let mut guard = self
            .session
            .acquire_portal_operation(owner, &portal_name)
            .await?;
        let conn = guard.connection_mut();

        loop {
            let msg = match read_message(&mut conn.stream, &mut conn.read_buf, None).await {
                Ok(m) => m,
                Err(err) => {
                    conn.exchange_state = ExchangeState::Idle;
                    conn.mark_closed();
                    drop(guard);
                    self.in_flight_execute = false;
                    self.mark_closed_and_unpin();
                    return Err(err);
                }
            };

            match msg {
                Message::DataRow(body) => {
                    let buffer = body.buffer();
                    let mut ranges = body.ranges();
                    let mut row = Vec::with_capacity(self.columns.len());
                    let mut idx = 0usize;
                    let mut decode_failure: Option<PostgresError> = None;

                    loop {
                        let range_opt = match ranges.next() {
                            Ok(Some(r)) => r,
                            Ok(None) => break,
                            Err(e) => {
                                conn.exchange_state = ExchangeState::Idle;
                                conn.mark_closed();
                                drop(guard);
                                self.in_flight_execute = false;
                                self.mark_closed_and_unpin();
                                return Err(PostgresError::Protocol(format!(
                                    "malformed Portal DataRow range: {e}"
                                )));
                            }
                        };
                        let Some(col) = self.columns.get(idx) else {
                            conn.exchange_state = ExchangeState::Idle;
                            conn.mark_closed();
                            drop(guard);
                            self.in_flight_execute = false;
                            self.mark_closed_and_unpin();
                            return Err(PostgresError::Protocol(format!(
                                "Portal DataRow field count exceeds column count {}",
                                self.columns.len()
                            )));
                        };
                        let raw_slice = range_opt.map(|r| &buffer[r]);
                        match decode_value_with_catalog(
                            col.type_oid,
                            col.format,
                            raw_slice,
                            &self.codec_context,
                            &self.type_catalog,
                        ) {
                            Ok(val) => row.push(val),
                            Err(err) => {
                                decode_failure = Some(err);
                                break;
                            }
                        }
                        idx += 1;
                    }

                    if let Some(err) = decode_failure {
                        let _ = drain_until_ready_for_query(conn).await;
                        self.in_flight_execute = false;
                        let _ = close_and_rollback_portal_on_conn(
                            conn,
                            &portal_name,
                            &self.scope,
                            true,
                        )
                        .await;
                        drop(guard);
                        self.mark_closed_and_unpin();
                        return Err(err);
                    }

                    if idx != self.columns.len() {
                        conn.exchange_state = ExchangeState::Idle;
                        conn.mark_closed();
                        drop(guard);
                        self.in_flight_execute = false;
                        self.mark_closed_and_unpin();
                        return Err(PostgresError::Protocol(format!(
                            "Portal DataRow field count {idx} does not match column count {}",
                            self.columns.len()
                        )));
                    }

                    return Ok(PortalEvent::Row(row));
                }
                Message::PortalSuspended => {
                    if let Err(err) = drain_until_ready_for_query(conn).await {
                        drop(guard);
                        self.in_flight_execute = false;
                        self.mark_closed_and_unpin();
                        return Err(err);
                    }
                    self.in_flight_execute = false;
                    return Ok(PortalEvent::Suspended);
                }
                Message::CommandComplete(body) => {
                    let tag = match body.tag() {
                        Ok(t) => t.to_owned(),
                        Err(e) => {
                            conn.exchange_state = ExchangeState::Idle;
                            conn.mark_closed();
                            drop(guard);
                            self.in_flight_execute = false;
                            self.mark_closed_and_unpin();
                            return Err(PostgresError::Protocol(format!(
                                "invalid Portal CommandComplete tag UTF-8: {e}"
                            )));
                        }
                    };
                    let affected_rows = match parse_command_tag_affected_rows(&tag) {
                        Ok(n) => n,
                        Err(err) => {
                            conn.exchange_state = ExchangeState::Idle;
                            conn.mark_closed();
                            drop(guard);
                            self.in_flight_execute = false;
                            self.mark_closed_and_unpin();
                            return Err(err);
                        }
                    };
                    if let Err(err) = drain_until_ready_for_query(conn).await {
                        drop(guard);
                        self.in_flight_execute = false;
                        self.mark_closed_and_unpin();
                        return Err(err);
                    }
                    self.in_flight_execute = false;
                    self.exhausted.store(true, Ordering::Release);
                    let finish_res =
                        finish_completed_portal_on_conn(conn, &portal_name, &self.scope).await;
                    drop(guard);
                    self.mark_closed_and_unpin();
                    finish_res?;
                    return Ok(PortalEvent::Complete {
                        command_tag: tag,
                        affected_rows,
                    });
                }
                Message::EmptyQueryResponse => {
                    if let Err(err) = drain_until_ready_for_query(conn).await {
                        drop(guard);
                        self.in_flight_execute = false;
                        self.mark_closed_and_unpin();
                        return Err(err);
                    }
                    self.in_flight_execute = false;
                    self.exhausted.store(true, Ordering::Release);
                    let finish_res =
                        finish_completed_portal_on_conn(conn, &portal_name, &self.scope).await;
                    drop(guard);
                    self.mark_closed_and_unpin();
                    finish_res?;
                    return Ok(PortalEvent::Complete {
                        command_tag: String::new(),
                        affected_rows: 0,
                    });
                }
                Message::ErrorResponse(body) => {
                    let server_err = match parse_error_response(&body) {
                        Ok(s) => PostgresError::Server(s),
                        Err(err) => {
                            conn.exchange_state = ExchangeState::Idle;
                            conn.mark_closed();
                            drop(guard);
                            self.in_flight_execute = false;
                            self.mark_closed_and_unpin();
                            return Err(err);
                        }
                    };
                    let _ = drain_until_ready_for_query(conn).await;
                    self.in_flight_execute = false;
                    let _ =
                        close_and_rollback_portal_on_conn(conn, &portal_name, &self.scope, false)
                            .await;
                    drop(guard);
                    self.mark_closed_and_unpin();
                    return Err(server_err);
                }
                Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    conn.handle_parameter_status_frame(&body)?;
                }
                Message::NotificationResponse(body) => {
                    conn.handle_notification_frame(&body)?;
                }
                other => {
                    conn.exchange_state = ExchangeState::Idle;
                    conn.mark_closed();
                    drop(guard);
                    self.in_flight_execute = false;
                    self.mark_closed_and_unpin();
                    return Err(PostgresError::Protocol(format!(
                        "unexpected backend frame during Portal Execute: {}",
                        message_debug_name(&other)
                    )));
                }
            }
        }
    }

    /// Fetch up to `max_rows` rows into a materialized [`QueryResult`].
    pub async fn fetch(&mut self, max_rows: i32) -> Result<QueryResult, PostgresError> {
        self.start_fetch(max_rows).await?;
        let mut rows = Vec::new();
        let mut approx_bytes = 0usize;
        let (command_tag, affected_rows) = loop {
            match self.next_event().await? {
                PortalEvent::Row(row) => {
                    approx_bytes = approx_bytes.saturating_add(estimate_row_bytes(&row));
                    if approx_bytes > self.max_result_bytes {
                        let _ = self.close().await;
                        return Err(PostgresError::Data(format!(
                            "Portal::fetch batch size {approx_bytes} bytes exceeds max_result_bytes ({})",
                            self.max_result_bytes
                        )));
                    }
                    rows.push(row);
                }
                PortalEvent::Suspended => {
                    break (String::new(), 0);
                }
                PortalEvent::Complete {
                    command_tag,
                    affected_rows,
                } => {
                    break (command_tag, affected_rows);
                }
            }
        };
        let status = self.session.transaction_status();
        QueryResult::try_new(
            self.columns.clone(),
            rows,
            affected_rows,
            command_tag,
            status,
        )
    }

    /// Fetch up to `max_rows` rows under a fenced [`OperationGeneration`] so Python `asyncio`
    /// cancellation emits `CancelRequest`, drains/retires the physical socket boundedly under
    /// `connect_timeout`, and marks the portal closed without prematurely releasing any pool slot.
    pub(crate) async fn fetch_with_generation(
        &mut self,
        op_gen: OperationGeneration,
        max_rows: i32,
    ) -> Result<QueryResult, PostgresError> {
        if max_rows <= 0 {
            self.session.unreserve_operation_generation(op_gen);
            return Err(PostgresError::Interface(
                "Portal::fetch requires max_rows >= 1 (max_rows <= 0 is forbidden)".to_string(),
            ));
        }
        if self.is_closed() {
            self.session.unreserve_operation_generation(op_gen);
            return Err(PostgresError::Interface(
                "cannot fetch from a closed Portal".to_string(),
            ));
        }
        if self.is_exhausted() {
            self.session.unreserve_operation_generation(op_gen);
            return Err(PostgresError::Interface(
                "cannot fetch from an exhausted Portal".to_string(),
            ));
        }
        if self.in_flight_execute {
            self.session.unreserve_operation_generation(op_gen);
            return Err(PostgresError::Interface(
                "Portal already has an in-flight Execute".to_string(),
            ));
        }

        let portal_name = self.portal_name.clone();
        let owner = self.scope.owner;
        let had_caller_ambient = matches!(
            self.scope.kind,
            TransactionScopeKind::NestedSavepoint { .. }
        );
        let (mut guard, cancel_notify) = self
            .session
            .acquire_portal_operation_with_generation(
                Some(op_gen),
                owner,
                &portal_name,
                had_caller_ambient,
            )
            .await?;

        let connect_timeout = self.session.config().connect_timeout;
        let columns = self.columns.clone();
        let codec_context = Arc::clone(&self.codec_context);
        let type_catalog = self.type_catalog.clone();
        let max_result_bytes = self.max_result_bytes;
        let scope = self.scope.clone();

        let timed_res = {
            let conn = guard.connection_mut();
            let batch_fut = execute_portal_batch_on_conn(
                conn,
                &portal_name,
                max_rows,
                &columns,
                &codec_context,
                &type_catalog,
                max_result_bytes,
                &scope,
            );
            tokio::pin!(batch_fut);
            let cancel_drain_timeout = async {
                cancel_notify.notified().await;
                tokio::time::sleep(connect_timeout).await;
            };
            tokio::pin!(cancel_drain_timeout);
            tokio::select! {
                biased;
                res = &mut batch_fut => Ok(res),
                () = &mut cancel_drain_timeout => Err(()),
            }
        };

        let cancelled = self.session.did_op_claim_cancel(op_gen);
        match timed_res {
            Err(()) => {
                guard.connection_mut().mark_closed();
                guard.retire_socket_bounded().await;
                drop(guard);
                self.mark_closed_and_unpin();
                Err(PostgresError::Operational(
                    "cancelled Portal::fetch timed out while draining wire response".to_string(),
                ))
            }
            Ok(Ok((rows, completed, command_tag, affected_rows))) => {
                if cancelled {
                    guard.connection_mut().mark_closed();
                    guard.retire_socket_bounded().await;
                    if completed {
                        self.exhausted.store(true, Ordering::Release);
                    }
                    drop(guard);
                    self.mark_closed_and_unpin();
                } else if completed {
                    self.exhausted.store(true, Ordering::Release);
                    drop(guard);
                    self.mark_closed_and_unpin();
                } else {
                    drop(guard);
                }
                let status = self.session.transaction_status();
                QueryResult::try_new(columns, rows, affected_rows, command_tag, status)
            }
            Ok(Err(err)) => {
                if cancelled {
                    guard.connection_mut().mark_closed();
                    guard.retire_socket_bounded().await;
                }
                drop(guard);
                self.mark_closed_and_unpin();
                Err(err)
            }
        }
    }

    /// Close the portal idempotently, draining any in-flight `Execute`, sending `Close(b'P', portal_name)`, and rolling back early-abandoned scopes.
    pub async fn close(&mut self) -> Result<(), PostgresError> {
        if self.is_closed() {
            return Ok(());
        }
        let portal_name = self.portal_name.clone();
        let owner = self.scope.owner;
        let mut guard = match self
            .session
            .acquire_portal_operation(owner, &portal_name)
            .await
        {
            Ok(g) => g,
            Err(_) => {
                self.mark_closed_and_unpin();
                return Ok(());
            }
        };
        let conn = guard.connection_mut();
        if self.in_flight_execute {
            self.in_flight_execute = false;
            if let Err(err) = drain_until_ready_for_query(conn).await {
                conn.mark_closed();
                drop(guard);
                self.mark_closed_and_unpin();
                return Err(err);
            }
        }
        let res = close_and_rollback_portal_on_conn(conn, &portal_name, &self.scope, true).await;
        drop(guard);
        self.mark_closed_and_unpin();
        res
    }

    fn mark_closed_and_unpin(&self) {
        self.closed.store(true, Ordering::Release);
        self.session
            .unpin_portal(&self.portal_name, self.scope.owner);
    }
}

impl Drop for Portal {
    fn drop(&mut self) {
        if self.is_closed() {
            return;
        }
        self.closed.store(true, Ordering::Release);
        let session = Arc::clone(&self.session);
        let portal_name = self.portal_name.clone();
        let scope = self.scope.clone();
        let in_flight = self.in_flight_execute;
        session.begin_portal_drop_cleanup(&portal_name, scope.owner);
        let cleanup = async move {
            let connect_timeout = session.config().connect_timeout;
            if let Ok(mut guard) = session
                .acquire_portal_operation(scope.owner, &portal_name)
                .await
            {
                let cleanup_dl = tokio::time::Instant::now() + connect_timeout;
                let cleanup_res = tokio::time::timeout_at(cleanup_dl, async {
                    let conn = guard.connection_mut();
                    if in_flight {
                        drain_until_ready_for_query(conn).await?;
                    }
                    close_and_rollback_portal_on_conn(conn, &portal_name, &scope, true).await
                })
                .await;
                if !matches!(cleanup_res, Ok(Ok(()))) {
                    guard.connection_mut().mark_closed();
                    guard.retire_socket_bounded().await;
                }
            }
            session
                .finish_portal_drop_cleanup(&portal_name, scope.owner)
                .await;
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(cleanup);
        } else {
            vertebra_db::py_async::db_runtime_handle().spawn(cleanup);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_portal_batch_on_conn(
    conn: &mut PostgresConnection,
    portal_name: &str,
    max_rows: i32,
    columns: &[Column],
    codec_context: &Arc<CodecContext>,
    type_catalog: &TypeCatalog,
    max_result_bytes: usize,
    scope: &TransactionScope,
) -> Result<(Vec<Vec<PgValue>>, bool, String, u64), PostgresError> {
    conn.write_buf.clear();
    frontend::execute(portal_name, max_rows, &mut conn.write_buf)
        .map_err(|e| PostgresError::Protocol(format!("failed to encode Execute frame: {e}")))?;
    frontend::sync(&mut conn.write_buf);

    conn.set_exchange_state(ExchangeState::InExtendedExchange);
    if let Err(err) = conn.stream.write_all(&conn.write_buf).await {
        conn.write_buf.clear();
        conn.mark_closed();
        return Err(PostgresError::Transport(format!(
            "Portal Execute write failed: {err}"
        )));
    }
    conn.write_buf.clear();
    if let Err(err) = conn.stream.flush().await {
        conn.mark_closed();
        return Err(PostgresError::Transport(format!(
            "Portal Execute flush failed: {err}"
        )));
    }

    let mut rows = Vec::new();
    let mut approx_bytes = 0usize;

    loop {
        let msg = match read_message(&mut conn.stream, &mut conn.read_buf, None).await {
            Ok(m) => m,
            Err(err) => {
                conn.exchange_state = ExchangeState::Idle;
                conn.mark_closed();
                return Err(err);
            }
        };

        match msg {
            Message::DataRow(body) => {
                let buffer = body.buffer();
                let mut ranges = body.ranges();
                let mut row = Vec::with_capacity(columns.len());
                let mut idx = 0usize;
                let mut decode_failure: Option<PostgresError> = None;

                loop {
                    let range_opt = match ranges.next() {
                        Ok(Some(r)) => r,
                        Ok(None) => break,
                        Err(e) => {
                            conn.exchange_state = ExchangeState::Idle;
                            conn.mark_closed();
                            return Err(PostgresError::Protocol(format!(
                                "malformed Portal DataRow range: {e}"
                            )));
                        }
                    };
                    let Some(col) = columns.get(idx) else {
                        conn.exchange_state = ExchangeState::Idle;
                        conn.mark_closed();
                        return Err(PostgresError::Protocol(format!(
                            "Portal DataRow field count exceeds column count {}",
                            columns.len()
                        )));
                    };
                    let raw_slice = range_opt.map(|r| &buffer[r]);
                    match decode_value_with_catalog(
                        col.type_oid,
                        col.format,
                        raw_slice,
                        codec_context,
                        type_catalog,
                    ) {
                        Ok(val) => row.push(val),
                        Err(err) => {
                            decode_failure = Some(err);
                            break;
                        }
                    }
                    idx += 1;
                }

                if let Some(err) = decode_failure {
                    let _ = drain_until_ready_for_query(conn).await;
                    let _ = close_and_rollback_portal_on_conn(conn, portal_name, scope, true).await;
                    return Err(err);
                }

                if idx != columns.len() {
                    conn.exchange_state = ExchangeState::Idle;
                    conn.mark_closed();
                    return Err(PostgresError::Protocol(format!(
                        "Portal DataRow field count {idx} does not match column count {}",
                        columns.len()
                    )));
                }

                approx_bytes = approx_bytes.saturating_add(estimate_row_bytes(&row));
                if approx_bytes > max_result_bytes {
                    let _ = drain_until_ready_for_query(conn).await;
                    let _ = close_and_rollback_portal_on_conn(conn, portal_name, scope, true).await;
                    return Err(PostgresError::Data(format!(
                        "Portal::fetch batch size {approx_bytes} bytes exceeds max_result_bytes ({max_result_bytes})"
                    )));
                }
                rows.push(row);
            }
            Message::PortalSuspended => {
                drain_until_ready_for_query(conn).await?;
                return Ok((rows, false, String::new(), 0));
            }
            Message::CommandComplete(body) => {
                let tag = match body.tag() {
                    Ok(t) => t.to_owned(),
                    Err(e) => {
                        conn.exchange_state = ExchangeState::Idle;
                        conn.mark_closed();
                        return Err(PostgresError::Protocol(format!(
                            "invalid Portal CommandComplete tag UTF-8: {e}"
                        )));
                    }
                };
                let affected_rows = match parse_command_tag_affected_rows(&tag) {
                    Ok(n) => n,
                    Err(err) => {
                        conn.exchange_state = ExchangeState::Idle;
                        conn.mark_closed();
                        return Err(err);
                    }
                };
                drain_until_ready_for_query(conn).await?;
                finish_completed_portal_on_conn(conn, portal_name, scope).await?;
                return Ok((rows, true, tag, affected_rows));
            }
            Message::EmptyQueryResponse => {
                drain_until_ready_for_query(conn).await?;
                finish_completed_portal_on_conn(conn, portal_name, scope).await?;
                return Ok((rows, true, String::new(), 0));
            }
            Message::ErrorResponse(body) => {
                let server_err = match parse_error_response(&body) {
                    Ok(s) => PostgresError::Server(s),
                    Err(err) => {
                        conn.exchange_state = ExchangeState::Idle;
                        conn.mark_closed();
                        return Err(err);
                    }
                };
                let _ = drain_until_ready_for_query(conn).await;
                let _ = close_and_rollback_portal_on_conn(conn, portal_name, scope, false).await;
                return Err(server_err);
            }
            Message::NoticeResponse(_) => {}
            Message::ParameterStatus(body) => {
                conn.handle_parameter_status_frame(&body)?;
            }
            Message::NotificationResponse(body) => {
                conn.handle_notification_frame(&body)?;
            }
            other => {
                conn.exchange_state = ExchangeState::Idle;
                conn.mark_closed();
                return Err(PostgresError::Protocol(format!(
                    "unexpected backend frame during Portal Execute: {}",
                    message_debug_name(&other)
                )));
            }
        }
    }
}

async fn drain_until_ready_for_query(conn: &mut PostgresConnection) -> Result<(), PostgresError> {
    loop {
        let msg = read_message(&mut conn.stream, &mut conn.read_buf, None).await?;
        match msg {
            Message::DataRow(_)
            | Message::PortalSuspended
            | Message::CommandComplete(_)
            | Message::EmptyQueryResponse
            | Message::CloseComplete
            | Message::ErrorResponse(_)
            | Message::NoticeResponse(_) => {}
            Message::ParameterStatus(body) => {
                conn.handle_parameter_status_frame(&body)?;
            }
            Message::NotificationResponse(body) => {
                conn.handle_notification_frame(&body)?;
            }
            Message::ReadyForQuery(body) => {
                conn.set_exchange_state(ExchangeState::Idle);
                let status = TransactionStatus::from_wire_tag(body.status())?;
                conn.set_transaction_status_from_wire(status);
                return Ok(());
            }
            other => {
                conn.mark_closed();
                return Err(PostgresError::Protocol(format!(
                    "unexpected frame while draining portal to ReadyForQuery: {}",
                    message_debug_name(&other)
                )));
            }
        }
    }
}

async fn send_close_portal(
    conn: &mut PostgresConnection,
    portal_name: &str,
) -> Result<(), PostgresError> {
    if conn.is_closed() {
        return Ok(());
    }
    conn.write_buf.clear();
    frontend::close(b'P', portal_name, &mut conn.write_buf)
        .map_err(|e| PostgresError::Protocol(format!("failed to encode Close(P): {e}")))?;
    frontend::sync(&mut conn.write_buf);
    conn.set_exchange_state(ExchangeState::InExtendedExchange);
    if let Err(err) = conn.stream.write_all(&conn.write_buf).await {
        conn.write_buf.clear();
        conn.mark_closed();
        return Err(PostgresError::Transport(format!(
            "Close(P) write failed: {err}"
        )));
    }
    conn.write_buf.clear();
    if let Err(err) = conn.stream.flush().await {
        conn.mark_closed();
        return Err(PostgresError::Transport(format!(
            "Close(P) flush failed: {err}"
        )));
    }
    drain_until_ready_for_query(conn).await
}

async fn finish_completed_portal_on_conn(
    conn: &mut PostgresConnection,
    portal_name: &str,
    scope: &TransactionScope,
) -> Result<(), PostgresError> {
    send_close_portal(conn, portal_name).await?;
    match scope.kind {
        TransactionScopeKind::ExplicitTransaction => {
            conn.commit().await?;
        }
        TransactionScopeKind::NestedSavepoint { token } => {
            conn.release_savepoint(token).await?;
        }
    }
    Ok(())
}

pub(crate) async fn close_and_rollback_portal_on_conn(
    conn: &mut PostgresConnection,
    portal_name: &str,
    scope: &TransactionScope,
    send_portal_close: bool,
) -> Result<(), PostgresError> {
    if send_portal_close && conn.transaction_status() == TransactionStatus::InTransaction {
        let _ = send_close_portal(conn, portal_name).await;
    }
    if conn.is_closed() {
        return Ok(());
    }
    match scope.kind {
        TransactionScopeKind::ExplicitTransaction => {
            if conn.transaction_status() != TransactionStatus::Idle {
                conn.rollback().await?;
            }
        }
        TransactionScopeKind::NestedSavepoint { token } => {
            if conn.transaction_status() != TransactionStatus::Idle {
                conn.rollback_to_savepoint(token).await?;
                conn.release_savepoint(token).await?;
            }
        }
    }
    Ok(())
}

fn estimate_row_bytes(row: &[PgValue]) -> usize {
    let mut sum = std::mem::size_of_val(row);
    for cell in row {
        sum = sum.saturating_add(match cell {
            PgValue::Text(s) => s.len(),
            PgValue::Bytes(b) | PgValue::Json(b) => b.len(),
            PgValue::BitString { data, .. } => data.len(),
            PgValue::Vector { values, .. } => values.len() * 4,
            PgValue::Array { values, .. } | PgValue::Composite { values, .. } => {
                estimate_row_bytes(values)
            }
            _ => 16,
        });
    }
    sum
}

fn message_debug_name(msg: &Message) -> &'static str {
    match msg {
        Message::AuthenticationOk => "AuthenticationOk",
        Message::BindComplete => "BindComplete",
        Message::CloseComplete => "CloseComplete",
        Message::CommandComplete(_) => "CommandComplete",
        Message::DataRow(_) => "DataRow",
        Message::EmptyQueryResponse => "EmptyQueryResponse",
        Message::ErrorResponse(_) => "ErrorResponse",
        Message::NoData => "NoData",
        Message::NoticeResponse(_) => "NoticeResponse",
        Message::NotificationResponse(_) => "NotificationResponse",
        Message::ParameterDescription(_) => "ParameterDescription",
        Message::ParameterStatus(_) => "ParameterStatus",
        Message::ParseComplete => "ParseComplete",
        Message::PortalSuspended => "PortalSuspended",
        Message::ReadyForQuery(_) => "ReadyForQuery",
        Message::RowDescription(_) => "RowDescription",
        _ => "OtherMessage",
    }
}

fn is_ddl_or_cache_clear_tag(tag: &str) -> bool {
    tag.starts_with("ALTER ")
        || tag.starts_with("DROP ")
        || tag.starts_with("CREATE ")
        || tag.eq_ignore_ascii_case("DISCARD ALL")
        || tag.starts_with("DEALLOCATE")
}

/// Execute `sql` directly into an inline or `VTBARROW` shared-memory Arrow IPC stream without
/// materializing `QueryResult.rows`, guarding cold custom result OIDs behind a non-executing
/// `Parse("", sql, param_oids) + Describe(b'S', "") + Sync` preflight before `Bind + Execute`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_select_to_arrow(
    session: &Arc<SessionHandle>,
    op_gen: OperationGeneration,
    sql: &str,
    parameters: &[Parameter],
    parameter_oids: Option<&[u32]>,
    owner: Option<TransactionOwner>,
    budget: CommandBudget,
    threshold_rows: usize,
    max_result_bytes: usize,
) -> Result<ArrowOutput, PostgresError> {
    if max_result_bytes == 0 {
        session.unreserve_operation_generation(op_gen);
        return Err(PostgresError::Data(
            "max_result_bytes must be >= 1".to_string(),
        ));
    }
    if sql.as_bytes().contains(&0) {
        session.unreserve_operation_generation(op_gen);
        return Err(PostgresError::Data(
            "SQL query string cannot contain embedded NUL byte".to_string(),
        ));
    }
    if let Some(explicit) = parameter_oids
        && explicit.len() != parameters.len()
    {
        session.unreserve_operation_generation(op_gen);
        return Err(PostgresError::Data(format!(
            "parameter_oids length {} does not match parameters length {}",
            explicit.len(),
            parameters.len()
        )));
    }

    let sql_owned = sql.to_owned();
    let params_owned = parameters.to_vec();
    let oids_owned = parameter_oids.map(ToOwned::to_owned);
    let shared = Arc::clone(&session.shared);

    session
        .run_cancellable_operation(op_gen, SessionAccess::Ordinary, owner, move |conn, meta| {
            Box::pin(async move {
                let (res, timed_out, wire_started, write_uncertain) = {
                    let mut dl_guard = conn.enter_session_command_deadline(budget.deadline());
                    let res = execute_select_to_arrow_on_conn(
                        &mut dl_guard,
                        &sql_owned,
                        &params_owned,
                        oids_owned.as_deref(),
                        threshold_rows,
                        max_result_bytes,
                    )
                    .await;
                    (
                        res,
                        dl_guard.timed_out(),
                        dl_guard.wire_started(),
                        dl_guard.write_uncertain(),
                    )
                };
                if timed_out {
                    SessionHandle::cleanup_deadline_expired_command(
                        &shared,
                        op_gen,
                        wire_started,
                        write_uncertain,
                        conn,
                        meta,
                    )
                    .await;
                }
                res
            })
        })
        .await
}

async fn execute_select_to_arrow_on_conn(
    conn: &mut PostgresConnection,
    sql: &str,
    parameters: &[Parameter],
    parameter_oids: Option<&[u32]>,
    threshold_rows: usize,
    max_result_bytes: usize,
) -> Result<ArrowOutput, PostgresError> {
    if conn.is_closed() {
        return Err(PostgresError::Interface(
            "cannot execute select_to_arrow on a closed PostgresConnection".to_string(),
        ));
    }
    if conn.exchange_state != ExchangeState::Idle {
        return Err(PostgresError::Interface(format!(
            "cannot execute select_to_arrow while exchange state is {:?}",
            conn.exchange_state
        )));
    }
    if conn
        .active_deadline()
        .is_some_and(|dl| Instant::now() >= dl)
    {
        conn.deadline_timed_out = true;
        return Err(PostgresError::Operational(
            COMMAND_IO_DEADLINE_EXCEEDED_MSG.to_string(),
        ));
    }

    let resolved =
        crate::metadata::resolve_parameter_metadata(conn, parameters, parameter_oids).await?;

    let cached_desc = conn.statement_descriptions.get(sql).cloned();
    let (description, preflight_unnamed_ready) = match cached_desc {
        Some(desc)
            if desc.parameter_oids.len() == resolved.parameter_oids.len()
                && desc.result_columns.iter().all(|c| {
                    c.type_oid == 0 || conn.type_catalog.type_info(c.type_oid).is_some()
                })
                && desc
                    .parameter_oids
                    .iter()
                    .all(|&o| o == 0 || conn.type_catalog.type_info(o).is_some()) =>
        {
            (desc, false)
        }
        _ => {
            let desc = describe_statement_preflight(conn, sql, &resolved.parameter_oids).await?;
            let mut missing_oids: Vec<u32> = Vec::new();
            for col in &desc.result_columns {
                if col.type_oid != 0
                    && conn.type_catalog.type_info(col.type_oid).is_none()
                    && !missing_oids.contains(&col.type_oid)
                {
                    missing_oids.push(col.type_oid);
                }
            }
            for &poid in &desc.parameter_oids {
                if poid != 0
                    && conn.type_catalog.type_info(poid).is_none()
                    && !missing_oids.contains(&poid)
                {
                    missing_oids.push(poid);
                }
            }

            let mut unnamed_survived = conn.unnamed_statement_valid
                && (conn.pooler_capabilities.effective_mode == PoolerMode::Session
                    || conn.transaction_status() == TransactionStatus::InTransaction);

            if !missing_oids.is_empty() {
                discover_arrow_oids_on_conn(conn, &missing_oids).await?;
                unnamed_survived = false;
            }
            conn.statement_descriptions
                .insert(sql.to_owned(), desc.clone());
            (desc, unnamed_survived)
        }
    };

    let effective_param_oids: Vec<u32> = resolved
        .parameter_oids
        .iter()
        .enumerate()
        .map(|(i, &oid)| {
            if oid != 0 {
                oid
            } else {
                description.parameter_oids.get(i).copied().unwrap_or(0)
            }
        })
        .collect();
    let param_formats: Vec<Format> = effective_param_oids
        .iter()
        .map(|&oid| {
            if oid != 0 {
                format_for_oid_with_catalog(oid, &conn.type_catalog)
            } else {
                Format::Text
            }
        })
        .collect();
    let result_formats: Vec<Format> = description
        .result_columns
        .iter()
        .map(|col| format_for_oid_with_catalog(col.type_oid, &conn.type_catalog))
        .collect();

    let need_parse = !preflight_unnamed_ready;
    let ctx = CodecContext::from_parameters(&conn.parameters);
    conn.write_buf.clear();
    if need_parse {
        frontend::parse(
            "",
            sql,
            effective_param_oids.iter().copied(),
            &mut conn.write_buf,
        )
        .map_err(|e| {
            PostgresError::Protocol(format!("failed to encode select_to_arrow Parse: {e}"))
        })?;
        conn.unnamed_statement_valid = true;
    }
    crate::extended::encode_bind(
        "",
        "",
        &resolved.parameters,
        &effective_param_oids,
        &param_formats,
        &result_formats,
        &ctx,
        &conn.type_catalog,
        &mut conn.write_buf,
    )?;
    frontend::describe(b'P', "", &mut conn.write_buf).map_err(|e| {
        PostgresError::Protocol(format!("failed to encode select_to_arrow Describe(P): {e}"))
    })?;
    frontend::execute("", 0, &mut conn.write_buf).map_err(|e| {
        PostgresError::Protocol(format!("failed to encode select_to_arrow Execute: {e}"))
    })?;
    frontend::close(b'P', "", &mut conn.write_buf).map_err(|e| {
        PostgresError::Protocol(format!("failed to encode select_to_arrow Close(P): {e}"))
    })?;
    frontend::sync(&mut conn.write_buf);

    conn.set_exchange_state(ExchangeState::InExtendedExchange);
    conn.write_command_buf("select_to_arrow execute", need_parse)
        .await?;

    let mut saw_parse_complete = !need_parse;
    let mut saw_bind_complete = false;
    let mut saw_portal_describe = false;
    let mut saw_execution_end = false;
    let mut first_server_err: Option<PostgresError> = None;
    let mut stream_err: Option<PostgresError> = None;
    let mut builder_opt: Option<PostgresArrowBatchBuilder> = None;
    let mut writer_opt: Option<PostgresArrowIpcWriter> = None;
    let mut expected_col_count = 0usize;
    let mut row_ctx = ctx;

    loop {
        let msg = conn.read_command_frame().await?;
        match msg {
            Message::ParseComplete => {
                if saw_parse_complete {
                    conn.mark_closed();
                    return Err(PostgresError::Protocol(
                        "unexpected duplicate ParseComplete in select_to_arrow".to_string(),
                    ));
                }
                saw_parse_complete = true;
            }
            Message::BindComplete => {
                if saw_bind_complete {
                    conn.mark_closed();
                    return Err(PostgresError::Protocol(
                        "unexpected duplicate BindComplete in select_to_arrow".to_string(),
                    ));
                }
                saw_bind_complete = true;
            }
            Message::RowDescription(body) => {
                if first_server_err.is_none() {
                    if saw_portal_describe {
                        conn.mark_closed();
                        return Err(PostgresError::Protocol(
                            "unexpected duplicate RowDescription in select_to_arrow".to_string(),
                        ));
                    }
                    saw_portal_describe = true;
                    row_ctx = CodecContext::from_parameters(&conn.parameters);
                    let portal_cols = match parse_row_description(&body) {
                        Ok(cols) => cols,
                        Err(err) => {
                            conn.mark_closed();
                            return Err(err);
                        }
                    };
                    if let Err(err) =
                        validate_negotiated_result_formats(&portal_cols, &result_formats)
                    {
                        conn.mark_closed();
                        return Err(err);
                    }
                    expected_col_count = portal_cols.len();
                    let col_names: Vec<String> =
                        portal_cols.iter().map(|c| c.name.clone()).collect();
                    match PostgresArrowBatchBuilder::try_new(
                        &portal_cols,
                        &conn.type_catalog,
                        DEFAULT_ARROW_BATCH_ROWS,
                        DEFAULT_ARROW_BATCH_BYTES,
                    ) {
                        Ok(b) => {
                            let schema = Arc::clone(b.schema());
                            match PostgresArrowIpcWriter::try_new(
                                schema,
                                col_names,
                                ArrowStorageMode::Adaptive { threshold_rows },
                                max_result_bytes,
                            ) {
                                Ok(w) => {
                                    builder_opt = Some(b);
                                    writer_opt = Some(w);
                                }
                                Err(err) => stream_err = Some(err),
                            }
                        }
                        Err(err) => stream_err = Some(err),
                    }
                }
            }
            Message::NoData => {
                if first_server_err.is_none() {
                    if saw_portal_describe {
                        conn.mark_closed();
                        return Err(PostgresError::Protocol(
                            "unexpected duplicate NoData in select_to_arrow".to_string(),
                        ));
                    }
                    saw_portal_describe = true;
                    expected_col_count = 0;
                    match PostgresArrowSchema::from_columns(&[], &conn.type_catalog) {
                        Ok(schema) => match PostgresArrowIpcWriter::try_new(
                            schema,
                            Vec::new(),
                            ArrowStorageMode::Adaptive { threshold_rows },
                            max_result_bytes,
                        ) {
                            Ok(w) => writer_opt = Some(w),
                            Err(err) => stream_err = Some(err),
                        },
                        Err(err) => stream_err = Some(err),
                    }
                }
            }
            Message::DataRow(body) => {
                if !saw_bind_complete || !saw_portal_describe {
                    conn.mark_closed();
                    return Err(PostgresError::Protocol(
                        "received DataRow before BindComplete/RowDescription in select_to_arrow"
                            .to_string(),
                    ));
                }
                if first_server_err.is_none() && stream_err.is_none() {
                    let buffer = body.buffer();
                    let mut ranges = body.ranges();
                    let mut fields: Vec<Option<&[u8]>> = Vec::with_capacity(expected_col_count);
                    while let Some(range_opt) = ranges.next().map_err(|e| {
                        conn.mark_closed();
                        PostgresError::Protocol(format!(
                            "malformed DataRow field range in select_to_arrow: {e}"
                        ))
                    })? {
                        if fields.len() >= expected_col_count {
                            conn.mark_closed();
                            return Err(PostgresError::Protocol(format!(
                                "select_to_arrow DataRow contains more fields than RowDescription ({expected_col_count})"
                            )));
                        }
                        fields.push(range_opt.map(|r| &buffer[r]));
                    }
                    if fields.len() != expected_col_count {
                        conn.mark_closed();
                        return Err(PostgresError::Protocol(format!(
                            "select_to_arrow DataRow field count {} does not match RowDescription count {expected_col_count}",
                            fields.len()
                        )));
                    }

                    let Some(builder) = builder_opt.as_mut() else {
                        conn.mark_closed();
                        return Err(PostgresError::Protocol(
                            "received DataRow after NoData in select_to_arrow".to_string(),
                        ));
                    };
                    match builder.append_data_row(&fields, &row_ctx, &conn.type_catalog) {
                        Ok(Some(batch)) => {
                            if let Some(writer) = writer_opt.as_mut()
                                && let Err(err) = writer.write_batch(&batch)
                            {
                                writer_opt = None;
                                builder_opt = None;
                                stream_err = Some(err);
                            }
                        }
                        Ok(None) => {}
                        Err(err) => {
                            writer_opt = None;
                            builder_opt = None;
                            stream_err = Some(err);
                        }
                    }
                }
            }
            Message::CommandComplete(body) => {
                let tag = body.tag().map_err(|e| {
                    conn.mark_closed();
                    PostgresError::Protocol(format!(
                        "invalid select_to_arrow CommandComplete UTF-8: {e}"
                    ))
                })?;
                let _ = parse_command_tag_affected_rows(tag).inspect_err(|_| conn.mark_closed())?;
                if is_ddl_or_cache_clear_tag(tag) {
                    conn.statement_descriptions.clear();
                    conn.statement_cache.invalidate_all();
                }
                saw_execution_end = true;
            }
            Message::EmptyQueryResponse => {
                saw_execution_end = true;
            }
            Message::CloseComplete => {}
            Message::NoticeResponse(_) => {}
            Message::ParameterStatus(body) => {
                if let Err(err) = conn.handle_parameter_status_frame(&body) {
                    conn.mark_closed();
                    return Err(err);
                }
            }
            Message::NotificationResponse(body) => {
                if let Err(err) = conn.handle_notification_frame(&body) {
                    conn.mark_closed();
                    return Err(err);
                }
            }
            Message::ErrorResponse(body) => match parse_error_response(&body) {
                Ok(srv) => {
                    if first_server_err.is_none() {
                        first_server_err = Some(PostgresError::Server(srv));
                    }
                    writer_opt = None;
                    builder_opt = None;
                    conn.unnamed_statement_valid = false;
                    conn.statement_descriptions.remove(sql);
                }
                Err(err) => {
                    conn.mark_closed();
                    return Err(err);
                }
            },
            Message::ReadyForQuery(body) => {
                conn.set_exchange_state(ExchangeState::Idle);
                let status = TransactionStatus::from_wire_tag(body.status()).inspect_err(|_| {
                    conn.mark_closed();
                })?;
                conn.set_transaction_status_from_wire(status);
                conn.deadline_wire_started = false;

                if let Some(err) = first_server_err {
                    return Err(err);
                }
                if !saw_parse_complete || !saw_bind_complete || !saw_execution_end {
                    conn.mark_closed();
                    return Err(PostgresError::Protocol(
                        "select_to_arrow ReadyForQuery arrived before extended exchange completed"
                            .to_string(),
                    ));
                }
                if let Some(err) = stream_err {
                    return Err(err);
                }
                let Some(mut writer) = writer_opt else {
                    conn.mark_closed();
                    return Err(PostgresError::Protocol(
                        "select_to_arrow completed without RowDescription or NoData".to_string(),
                    ));
                };
                if let Some(mut builder) = builder_opt
                    && let Some(tail_batch) = builder.flush_batch()?
                {
                    writer.write_batch(&tail_batch)?;
                }
                return writer.finish();
            }
            other => {
                conn.mark_closed();
                return Err(PostgresError::Protocol(format!(
                    "unexpected backend frame during select_to_arrow: {}",
                    message_debug_name(&other)
                )));
            }
        }
    }
}

async fn describe_statement_preflight(
    conn: &mut PostgresConnection,
    sql: &str,
    param_oids: &[u32],
) -> Result<PreparedDescription, PostgresError> {
    conn.write_buf.clear();
    frontend::parse("", sql, param_oids.iter().copied(), &mut conn.write_buf).map_err(|e| {
        PostgresError::Protocol(format!("failed to encode preflight Parse frame: {e}"))
    })?;
    frontend::describe(b'S', "", &mut conn.write_buf).map_err(|e| {
        PostgresError::Protocol(format!("failed to encode preflight Describe(S) frame: {e}"))
    })?;
    frontend::sync(&mut conn.write_buf);

    conn.unnamed_statement_valid = true;
    conn.set_exchange_state(ExchangeState::InExtendedExchange);
    conn.write_command_buf("select_to_arrow Describe preflight", true)
        .await?;

    let mut saw_parse_complete = false;
    let mut saw_param_desc = false;
    let mut saw_row_desc_or_nodata = false;
    let mut desc_param_oids: Vec<u32> = Vec::new();
    let mut desc_columns: Vec<Column> = Vec::new();
    let mut first_server_err: Option<PostgresError> = None;

    loop {
        let msg = conn.read_command_frame().await?;
        match msg {
            Message::ParseComplete => {
                if saw_parse_complete {
                    conn.mark_closed();
                    return Err(PostgresError::Protocol(
                        "duplicate ParseComplete during Describe preflight".to_string(),
                    ));
                }
                saw_parse_complete = true;
            }
            Message::ParameterDescription(body) => {
                if first_server_err.is_none() {
                    let oids = parse_parameter_description(&body).inspect_err(|_| {
                        conn.mark_closed();
                    })?;
                    if oids.len() != param_oids.len() {
                        conn.mark_closed();
                        return Err(PostgresError::Protocol(format!(
                            "preflight ParameterDescription count {} does not match bound parameter count {}",
                            oids.len(),
                            param_oids.len()
                        )));
                    }
                    desc_param_oids = oids;
                    saw_param_desc = true;
                }
            }
            Message::RowDescription(body) => {
                if first_server_err.is_none() {
                    let cols = parse_row_description(&body).inspect_err(|_| {
                        conn.mark_closed();
                    })?;
                    desc_columns = cols;
                    saw_row_desc_or_nodata = true;
                }
            }
            Message::NoData => {
                if first_server_err.is_none() {
                    desc_columns.clear();
                    saw_row_desc_or_nodata = true;
                }
            }
            Message::ErrorResponse(body) => match parse_error_response(&body) {
                Ok(srv) => {
                    if first_server_err.is_none() {
                        first_server_err = Some(PostgresError::Server(srv));
                    }
                    conn.unnamed_statement_valid = false;
                }
                Err(err) => {
                    conn.mark_closed();
                    return Err(err);
                }
            },
            Message::NoticeResponse(_) => {}
            Message::ParameterStatus(body) => {
                if let Err(err) = conn.handle_parameter_status_frame(&body) {
                    conn.mark_closed();
                    return Err(err);
                }
            }
            Message::NotificationResponse(body) => {
                if let Err(err) = conn.handle_notification_frame(&body) {
                    conn.mark_closed();
                    return Err(err);
                }
            }
            Message::ReadyForQuery(body) => {
                conn.set_exchange_state(ExchangeState::Idle);
                let status = TransactionStatus::from_wire_tag(body.status()).inspect_err(|_| {
                    conn.mark_closed();
                })?;
                conn.set_transaction_status_from_wire(status);
                if let Some(err) = first_server_err {
                    conn.deadline_wire_started = false;
                    return Err(err);
                }
                if !saw_parse_complete || !saw_param_desc || !saw_row_desc_or_nodata {
                    conn.mark_closed();
                    return Err(PostgresError::Protocol(
                        "incomplete Parse+Describe(S) preflight before ReadyForQuery".to_string(),
                    ));
                }
                return Ok(PreparedDescription {
                    parameter_oids: desc_param_oids,
                    result_columns: desc_columns,
                });
            }
            other => {
                conn.mark_closed();
                return Err(PostgresError::Protocol(format!(
                    "unexpected backend frame during Describe preflight: {}",
                    message_debug_name(&other)
                )));
            }
        }
    }
}

async fn discover_arrow_oids_on_conn(
    conn: &mut PostgresConnection,
    missing_oids: &[u32],
) -> Result<(), PostgresError> {
    match conn.transaction_status() {
        TransactionStatus::Idle => {
            let mut cat = conn.type_catalog.clone();
            cat.discover(conn, missing_oids, &[]).await
        }
        TransactionStatus::InTransaction => {
            conn.simple_query("SAVEPOINT _vtb_arrow_meta").await?;
            let mut cat = conn.type_catalog.clone();
            match cat.discover(conn, missing_oids, &[]).await {
                Ok(()) => {
                    conn.simple_query("RELEASE SAVEPOINT _vtb_arrow_meta")
                        .await?;
                    Ok(())
                }
                Err(err) => {
                    if conn
                        .simple_query(
                            "ROLLBACK TO SAVEPOINT _vtb_arrow_meta; RELEASE SAVEPOINT _vtb_arrow_meta",
                        )
                        .await
                        .is_err()
                    {
                        conn.closed = true;
                    }
                    Err(err)
                }
            }
        }
        TransactionStatus::Failed => Err(PostgresError::Interface(
            "cannot run Arrow catalog discovery inside an aborted transaction".to_string(),
        )),
    }
}

fn validate_negotiated_result_formats(
    portal_cols: &[Column],
    requested: &[Format],
) -> Result<(), PostgresError> {
    if requested.is_empty() {
        return Ok(());
    }
    if requested.len() == 1 {
        let expected = requested[0];
        for col in portal_cols {
            if col.format != expected {
                return Err(PostgresError::Protocol(format!(
                    "portal RowDescription column `{}` format {:?} does not match requested format {:?}",
                    col.name, col.format, expected
                )));
            }
        }
        return Ok(());
    }
    if requested.len() != portal_cols.len() {
        return Err(PostgresError::Protocol(format!(
            "portal RowDescription column count {} does not match requested result_formats count {}",
            portal_cols.len(),
            requested.len()
        )));
    }
    for (col, &expected) in portal_cols.iter().zip(requested.iter()) {
        if col.format != expected {
            return Err(PostgresError::Protocol(format!(
                "portal RowDescription column `{}` format {:?} does not match requested format {:?}",
                col.name, col.format, expected
            )));
        }
    }
    Ok(())
}

/// Pinned streaming cursor yielding bounded `VTBARROW` shared-memory Arrow IPC batches over a
/// single [`Portal`] and [`TransactionScope`] (`<= batch_size` rows and `<= 8 MiB` per batch).
pub struct ArrowPortalStream {
    portal: Option<Portal>,
    builder: Option<PostgresArrowBatchBuilder>,
    schema: Arc<Schema>,
    column_names: Vec<String>,
    batch_size: i32,
    exhausted: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
}

impl std::fmt::Debug for ArrowPortalStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowPortalStream")
            .field("column_names", &self.column_names)
            .field("batch_size", &self.batch_size)
            .field("exhausted", &self.is_exhausted())
            .field("closed", &self.is_closed())
            .finish()
    }
}

enum WirePortalItem {
    Row(Vec<PgValue>),
    DecodeError(PostgresError),
    Suspended,
    Complete,
    ServerError(PostgresError),
}

enum ArrowStepOutcome {
    SuspendedOrMidExecuteBatch(ArrowOutput),
    CompletedWithBatch(ArrowOutput),
    CompletedEmpty,
    RowBuildError(PostgresError),
    ServerError(PostgresError),
}

impl ArrowPortalStream {
    /// Open a session-pinned [`ArrowPortalStream`] inside a single `BEGIN` or `SAVEPOINT` scope.
    pub async fn open(
        session: &Arc<SessionHandle>,
        sql: &str,
        parameters: &[Parameter],
        parameter_oids: Option<&[u32]>,
        batch_size: i32,
        columns: Option<Vec<String>>,
    ) -> Result<Self, PostgresError> {
        let op_gen = session.reserve_operation_generation();
        Self::open_with_generation(
            session,
            op_gen,
            sql,
            parameters,
            parameter_oids,
            batch_size,
            columns,
        )
        .await
    }

    /// Open a session-pinned [`ArrowPortalStream`] under a fenced [`OperationGeneration`] inside
    /// [`Portal`]'s single `BEGIN` or nested `SAVEPOINT` scope.
    pub(crate) async fn open_with_generation(
        session: &Arc<SessionHandle>,
        op_gen: OperationGeneration,
        sql: &str,
        parameters: &[Parameter],
        parameter_oids: Option<&[u32]>,
        batch_size: i32,
        columns: Option<Vec<String>>,
    ) -> Result<Self, PostgresError> {
        Self::open_as_with_generation(
            session,
            op_gen,
            None,
            sql,
            parameters,
            parameter_oids,
            batch_size,
            columns,
        )
        .await
    }

    /// Open a session-pinned [`ArrowPortalStream`] under a fenced [`OperationGeneration`] and optional caller [`TransactionOwner`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn open_as_with_generation(
        session: &Arc<SessionHandle>,
        op_gen: OperationGeneration,
        caller_owner: Option<TransactionOwner>,
        sql: &str,
        parameters: &[Parameter],
        parameter_oids: Option<&[u32]>,
        batch_size: i32,
        columns: Option<Vec<String>>,
    ) -> Result<Self, PostgresError> {
        if batch_size <= 0 {
            session.unreserve_operation_generation(op_gen);
            return Err(PostgresError::Data(
                "stream_arrow_batches requires batch_size >= 1".to_string(),
            ));
        }
        let mut portal = session
            .open_portal_as_with_generation(
                op_gen,
                caller_owner,
                sql,
                parameters,
                parameter_oids,
                batch_size,
            )
            .await?;

        let effective_columns: Vec<Column> = match columns {
            Some(names) => {
                if names.len() != portal.columns().len() {
                    let expected = portal.columns().len();
                    let actual = names.len();
                    let _ = portal.close().await;
                    return Err(PostgresError::Data(format!(
                        "columns length {actual} does not match query column count {expected}"
                    )));
                }
                portal
                    .columns()
                    .iter()
                    .zip(names)
                    .map(|(col, name)| {
                        let mut renamed = col.clone();
                        renamed.name = name;
                        renamed
                    })
                    .collect()
            }
            None => portal.columns().to_vec(),
        };

        let builder = match PostgresArrowBatchBuilder::try_new(
            &effective_columns,
            portal.type_catalog(),
            batch_size as usize,
            DEFAULT_ARROW_BATCH_BYTES,
        ) {
            Ok(b) => b,
            Err(err) => {
                let _ = portal.close().await;
                return Err(err);
            }
        };

        let schema = Arc::clone(builder.schema());
        let column_names = effective_columns.iter().map(|c| c.name.clone()).collect();
        let (exhausted, closed) = portal.status_flags();

        Ok(Self {
            portal: Some(portal),
            builder: Some(builder),
            schema,
            column_names,
            batch_size,
            exhausted,
            closed,
        })
    }

    /// Borrow the negotiated Arrow [`Schema`] (`postgres.arrow.version = "1"`).
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Borrow the ordered Arrow output column names (preserving duplicates verbatim).
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Return `true` when the underlying portal reached `CommandComplete` / `EmptyQueryResponse`.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Acquire)
    }

    /// Return `true` when the stream has finished, errored, or been explicitly closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Shared lock-free `(exhausted, closed)` flags for Python stream wrappers.
    pub fn status_flags(&self) -> (Arc<AtomicBool>, Arc<AtomicBool>) {
        (Arc::clone(&self.exhausted), Arc::clone(&self.closed))
    }

    /// Borrow the pinned [`SessionHandle`] while the stream remains active.
    pub fn session(&self) -> Option<&Arc<SessionHandle>> {
        self.portal.as_ref().map(Portal::session)
    }

    pub(crate) fn portal_identity(&self) -> Option<(String, TransactionOwner)> {
        self.portal
            .as_ref()
            .filter(|p| !p.is_closed())
            .map(|p| (p.portal_name.clone(), p.scope.owner))
    }

    /// Advance the portal incrementally and emit the next `VTBARROW` shared-memory Arrow IPC batch,
    /// returning `Ok(None)` when the portal completes cleanly.
    pub async fn next_batch_shm(&mut self) -> Result<Option<ArrowOutput>, PostgresError> {
        if self.is_exhausted() {
            return Ok(None);
        }
        let Some(session) = self.session().cloned() else {
            return Err(PostgresError::Interface(
                "cannot fetch from a closed Arrow portal stream".to_string(),
            ));
        };
        let op_gen = session.reserve_operation_generation();
        self.next_batch_shm_with_generation(op_gen).await
    }

    /// Generation-fenced implementation of [`ArrowPortalStream::next_batch_shm`].
    pub(crate) async fn next_batch_shm_with_generation(
        &mut self,
        op_gen: OperationGeneration,
    ) -> Result<Option<ArrowOutput>, PostgresError> {
        if self.is_exhausted() {
            if let Some(s) = self.session() {
                s.unreserve_operation_generation(op_gen);
            }
            return Ok(None);
        }
        if self.is_closed() || self.portal.is_none() || self.builder.is_none() {
            if let Some(s) = self.session() {
                s.unreserve_operation_generation(op_gen);
            }
            return Err(PostgresError::Interface(
                "cannot fetch from a closed Arrow portal stream".to_string(),
            ));
        }

        let (session, portal_name, scope, had_caller_ambient, connect_timeout) = {
            let p = self.portal.as_ref().expect("checked portal present");
            let s = Arc::clone(&p.session);
            let ct = s.config().connect_timeout;
            let ambient = matches!(p.scope.kind, TransactionScopeKind::NestedSavepoint { .. });
            (s, p.portal_name.clone(), p.scope.clone(), ambient, ct)
        };

        let (mut guard, cancel_notify) = match session
            .acquire_portal_operation_with_generation(
                Some(op_gen),
                scope.owner,
                &portal_name,
                had_caller_ambient,
            )
            .await
        {
            Ok(pair) => pair,
            Err(err) => return Err(err),
        };

        let batch_size = self.batch_size;
        let col_names = self.column_names.clone();

        let timed_res = {
            let conn = guard.connection_mut();
            let portal = self.portal.as_mut().expect("checked portal present");
            let builder = self.builder.as_mut().expect("checked builder present");
            let step_fut =
                drive_arrow_portal_step_on_conn(conn, portal, builder, batch_size, &col_names);
            tokio::pin!(step_fut);
            let cancel_drain_timeout = async {
                cancel_notify.notified().await;
                tokio::time::sleep(connect_timeout).await;
            };
            tokio::pin!(cancel_drain_timeout);
            tokio::select! {
                biased;
                res = &mut step_fut => Ok(res),
                () = &mut cancel_drain_timeout => Err(()),
            }
        };

        let cancelled = session.did_op_claim_cancel(op_gen);
        match timed_res {
            Err(()) => {
                guard.connection_mut().mark_closed();
                guard.retire_socket_bounded().await;
                drop(guard);
                self.force_mark_closed_and_clear(false);
                Err(PostgresError::Operational(
                    "cancelled Arrow portal stream timed out while draining wire response"
                        .to_string(),
                ))
            }
            Ok(Ok(ArrowStepOutcome::SuspendedOrMidExecuteBatch(out))) => {
                if cancelled {
                    guard.connection_mut().mark_closed();
                    guard.retire_socket_bounded().await;
                    drop(guard);
                    self.force_mark_closed_and_clear(false);
                    return Err(PostgresError::Operational(
                        "Arrow portal stream was cancelled".to_string(),
                    ));
                }
                drop(guard);
                Ok(Some(out))
            }
            Ok(Ok(ArrowStepOutcome::CompletedWithBatch(out))) => {
                if cancelled {
                    guard.connection_mut().mark_closed();
                    guard.retire_socket_bounded().await;
                }
                drop(guard);
                self.force_mark_closed_and_clear(true);
                Ok(Some(out))
            }
            Ok(Ok(ArrowStepOutcome::CompletedEmpty)) => {
                if cancelled {
                    guard.connection_mut().mark_closed();
                    guard.retire_socket_bounded().await;
                }
                drop(guard);
                self.force_mark_closed_and_clear(true);
                Ok(None)
            }
            Ok(Ok(ArrowStepOutcome::RowBuildError(data_err))) => {
                let dl = Instant::now() + connect_timeout;
                let drained_clean = !cancelled
                    && matches!(
                        tokio::time::timeout_at(dl, async {
                            let conn = guard.connection_mut();
                            conn.drain_to_ready_for_query_until(Some(dl)).await?;
                            close_and_rollback_portal_on_conn(conn, &portal_name, &scope, true)
                                .await
                        })
                        .await,
                        Ok(Ok(()))
                    )
                    && !session.did_op_claim_cancel(op_gen);
                if !drained_clean {
                    let _ = session.fenced_cancel_handle(op_gen).cancel_until(dl).await;
                    if !guard.connection_mut().is_closed() && Instant::now() < dl {
                        let _ = guard
                            .connection_mut()
                            .drain_to_ready_for_query_until(Some(dl))
                            .await;
                    }
                    guard.connection_mut().mark_closed();
                    guard.retire_socket_bounded().await;
                }
                drop(guard);
                self.force_mark_closed_and_clear(false);
                Err(data_err)
            }
            Ok(Ok(ArrowStepOutcome::ServerError(srv_err))) => {
                if cancelled {
                    guard.connection_mut().mark_closed();
                    guard.retire_socket_bounded().await;
                }
                drop(guard);
                self.force_mark_closed_and_clear(false);
                Err(srv_err)
            }
            Ok(Err(wire_err)) => {
                guard.connection_mut().mark_closed();
                if cancelled {
                    guard.retire_socket_bounded().await;
                }
                drop(guard);
                self.force_mark_closed_and_clear(false);
                Err(wire_err)
            }
        }
    }

    /// Idempotently close the Arrow portal stream. When paused mid-`Execute` at the `8 MiB` byte cap,
    /// issues a generation-fenced `CancelRequest` + `connect_timeout` bounded drain and socket
    /// retirement so live portals never leak back into pools.
    pub async fn close(&mut self) -> Result<(), PostgresError> {
        if self.is_closed() {
            self.builder = None;
            self.portal = None;
            return Ok(());
        }
        let Some(session) = self.session().cloned() else {
            self.closed.store(true, Ordering::Release);
            self.builder = None;
            self.portal = None;
            return Ok(());
        };
        let op_gen = session.reserve_operation_generation();
        self.close_with_generation(op_gen).await
    }

    /// Generation-fenced implementation of [`ArrowPortalStream::close`].
    pub(crate) async fn close_with_generation(
        &mut self,
        op_gen: OperationGeneration,
    ) -> Result<(), PostgresError> {
        self.builder = None;
        let Some(mut portal) = self.portal.take() else {
            self.closed.store(true, Ordering::Release);
            return Ok(());
        };
        if portal.is_closed() {
            portal.session.unreserve_operation_generation(op_gen);
            self.closed.store(true, Ordering::Release);
            return Ok(());
        }

        let session = Arc::clone(&portal.session);
        let portal_name = portal.portal_name.clone();
        let scope = portal.scope.clone();
        let in_flight = portal.in_flight_execute;
        let had_caller_ambient = matches!(scope.kind, TransactionScopeKind::NestedSavepoint { .. });
        let connect_timeout = session.config().connect_timeout;

        let (mut guard, _cancel_notify) = match session
            .acquire_portal_operation_with_generation(
                Some(op_gen),
                scope.owner,
                &portal_name,
                had_caller_ambient,
            )
            .await
        {
            Ok(pair) => pair,
            Err(_) => {
                portal.in_flight_execute = false;
                portal.mark_closed_and_unpin();
                return Ok(());
            }
        };

        if in_flight {
            portal.in_flight_execute = false;
            let dl = Instant::now() + connect_timeout;
            let _ = session.fenced_cancel_handle(op_gen).cancel_until(dl).await;
            if !guard.connection_mut().is_closed() && Instant::now() < dl {
                let _ = guard
                    .connection_mut()
                    .drain_to_ready_for_query_until(Some(dl))
                    .await;
            }
            guard.connection_mut().mark_closed();
            guard.retire_socket_bounded().await;
            drop(guard);
            portal.mark_closed_and_unpin();
            return Ok(());
        }

        let close_res = {
            let conn = guard.connection_mut();
            let close_fut = close_and_rollback_portal_on_conn(conn, &portal_name, &scope, true);
            tokio::pin!(close_fut);
            let close_deadline = tokio::time::sleep(connect_timeout);
            tokio::pin!(close_deadline);
            tokio::select! {
                biased;
                res = &mut close_fut => Ok(res),
                () = &mut close_deadline => Err(()),
            }
        };

        let cancelled = session.did_op_claim_cancel(op_gen);
        match close_res {
            Err(()) => {
                guard.connection_mut().mark_closed();
                guard.retire_socket_bounded().await;
                drop(guard);
                portal.mark_closed_and_unpin();
                Err(PostgresError::Operational(
                    "cancelled ArrowPortalStream::close timed out while rolling back portal"
                        .to_string(),
                ))
            }
            Ok(res) => {
                if cancelled || res.is_err() {
                    guard.connection_mut().mark_closed();
                    guard.retire_socket_bounded().await;
                }
                drop(guard);
                portal.mark_closed_and_unpin();
                res
            }
        }
    }

    fn force_mark_closed_and_clear(&mut self, mark_exhausted: bool) {
        self.builder = None;
        if mark_exhausted {
            self.exhausted.store(true, Ordering::Release);
        }
        self.closed.store(true, Ordering::Release);
        if let Some(mut portal) = self.portal.take() {
            portal.in_flight_execute = false;
            if mark_exhausted {
                portal.exhausted.store(true, Ordering::Release);
            }
            portal.mark_closed_and_unpin();
        }
    }
}

impl Drop for ArrowPortalStream {
    fn drop(&mut self) {
        self.builder = None;
        let Some(mut portal) = self.portal.take() else {
            return;
        };
        if portal.is_closed() {
            return;
        }
        if !portal.in_flight_execute {
            drop(portal);
            return;
        }

        portal.in_flight_execute = false;
        portal.closed.store(true, Ordering::Release);
        self.closed.store(true, Ordering::Release);

        let session = Arc::clone(&portal.session);
        let portal_name = portal.portal_name.clone();
        let scope = portal.scope.clone();
        let had_caller_ambient = matches!(scope.kind, TransactionScopeKind::NestedSavepoint { .. });
        let op_gen = session.reserve_operation_generation();
        session.begin_portal_drop_cleanup(&portal_name, scope.owner);

        let cleanup = async move {
            let connect_timeout = session.config().connect_timeout;
            match session
                .acquire_portal_operation_with_generation(
                    Some(op_gen),
                    scope.owner,
                    &portal_name,
                    had_caller_ambient,
                )
                .await
            {
                Ok((mut guard, _)) => {
                    let dl = Instant::now() + connect_timeout;
                    let _ = session.fenced_cancel_handle(op_gen).cancel_until(dl).await;
                    if !guard.connection_mut().is_closed() && Instant::now() < dl {
                        let _ = guard
                            .connection_mut()
                            .drain_to_ready_for_query_until(Some(dl))
                            .await;
                    }
                    guard.connection_mut().mark_closed();
                    guard.retire_socket_bounded().await;
                }
                Err(_) => {
                    session.unreserve_operation_generation(op_gen);
                }
            }
            session
                .finish_portal_drop_cleanup(&portal_name, scope.owner)
                .await;
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(cleanup);
        } else {
            vertebra_db::py_async::db_runtime_handle().spawn(cleanup);
        }
    }
}

async fn drive_arrow_portal_step_on_conn(
    conn: &mut PostgresConnection,
    portal: &mut Portal,
    builder: &mut PostgresArrowBatchBuilder,
    batch_size: i32,
    col_names: &[String],
) -> Result<ArrowStepOutcome, PostgresError> {
    let portal_name = portal.portal_name.clone();
    let portal_scope = portal.scope.clone();

    loop {
        if !portal.in_flight_execute {
            send_portal_execute_on_conn(conn, &portal_name, batch_size).await?;
            portal.in_flight_execute = true;
        }

        let item = read_next_portal_wire_item_on_conn(
            conn,
            &portal.columns,
            &portal.codec_context,
            &portal.type_catalog,
        )
        .await?;

        match item {
            WirePortalItem::Row(pg_row) => match builder.append_pg_row(&pg_row) {
                Ok(Some(batch)) => {
                    if builder.buffered_rows() > 0 {
                        let out = serialize_record_batch_to_shm(&batch, col_names)?;
                        return Ok(ArrowStepOutcome::SuspendedOrMidExecuteBatch(out));
                    }
                    match finalize_full_batch_terminal_on_conn(conn, &portal_name, &portal_scope)
                        .await?
                    {
                        FullBatchTerminal::Suspended => {
                            portal.in_flight_execute = false;
                            let out = serialize_record_batch_to_shm(&batch, col_names)?;
                            return Ok(ArrowStepOutcome::SuspendedOrMidExecuteBatch(out));
                        }
                        FullBatchTerminal::Completed => {
                            portal.in_flight_execute = false;
                            let out = match serialize_record_batch_to_shm(&batch, col_names) {
                                Ok(o) => o,
                                Err(err) => {
                                    let _ = close_and_rollback_portal_on_conn(
                                        conn,
                                        &portal_name,
                                        &portal_scope,
                                        true,
                                    )
                                    .await;
                                    return Ok(ArrowStepOutcome::ServerError(err));
                                }
                            };
                            finish_completed_portal_on_conn(conn, &portal_name, &portal_scope)
                                .await?;
                            return Ok(ArrowStepOutcome::CompletedWithBatch(out));
                        }
                        FullBatchTerminal::ServerError(srv_err) => {
                            portal.in_flight_execute = false;
                            let _ = close_and_rollback_portal_on_conn(
                                conn,
                                &portal_name,
                                &portal_scope,
                                false,
                            )
                            .await;
                            return Ok(ArrowStepOutcome::ServerError(srv_err));
                        }
                    }
                }
                Ok(None) => {}
                Err(data_err) => {
                    return Ok(ArrowStepOutcome::RowBuildError(data_err));
                }
            },
            WirePortalItem::DecodeError(decode_err) => {
                return Ok(ArrowStepOutcome::RowBuildError(decode_err));
            }
            WirePortalItem::Suspended => {
                drain_until_ready_for_query(conn).await?;
                portal.in_flight_execute = false;
                if let Some(batch) = builder.flush_batch()? {
                    let out = serialize_record_batch_to_shm(&batch, col_names)?;
                    return Ok(ArrowStepOutcome::SuspendedOrMidExecuteBatch(out));
                }
            }
            WirePortalItem::Complete => {
                drain_until_ready_for_query(conn).await?;
                portal.in_flight_execute = false;
                let maybe_out = match builder.flush_batch()? {
                    Some(batch) => match serialize_record_batch_to_shm(&batch, col_names) {
                        Ok(o) => Some(o),
                        Err(err) => {
                            let _ = close_and_rollback_portal_on_conn(
                                conn,
                                &portal_name,
                                &portal_scope,
                                true,
                            )
                            .await;
                            return Ok(ArrowStepOutcome::ServerError(err));
                        }
                    },
                    None => None,
                };
                finish_completed_portal_on_conn(conn, &portal_name, &portal_scope).await?;
                return match maybe_out {
                    Some(out) => Ok(ArrowStepOutcome::CompletedWithBatch(out)),
                    None => Ok(ArrowStepOutcome::CompletedEmpty),
                };
            }
            WirePortalItem::ServerError(srv_err) => {
                let _ = drain_until_ready_for_query(conn).await;
                portal.in_flight_execute = false;
                let _ = close_and_rollback_portal_on_conn(conn, &portal_name, &portal_scope, false)
                    .await;
                return Ok(ArrowStepOutcome::ServerError(srv_err));
            }
        }
    }
}

enum FullBatchTerminal {
    Suspended,
    Completed,
    ServerError(PostgresError),
}

async fn finalize_full_batch_terminal_on_conn(
    conn: &mut PostgresConnection,
    _portal_name: &str,
    _portal_scope: &TransactionScope,
) -> Result<FullBatchTerminal, PostgresError> {
    loop {
        let msg = read_message(&mut conn.stream, &mut conn.read_buf, None).await?;
        match msg {
            Message::PortalSuspended => {
                drain_until_ready_for_query(conn).await?;
                return Ok(FullBatchTerminal::Suspended);
            }
            Message::CommandComplete(body) => {
                let tag = body.tag().map_err(|e| {
                    PostgresError::Protocol(format!("invalid Portal CommandComplete UTF-8: {e}"))
                })?;
                let _ = parse_command_tag_affected_rows(tag)?;
                drain_until_ready_for_query(conn).await?;
                return Ok(FullBatchTerminal::Completed);
            }
            Message::EmptyQueryResponse => {
                drain_until_ready_for_query(conn).await?;
                return Ok(FullBatchTerminal::Completed);
            }
            Message::ErrorResponse(body) => {
                let srv = parse_error_response(&body)?;
                let _ = drain_until_ready_for_query(conn).await;
                return Ok(FullBatchTerminal::ServerError(PostgresError::Server(srv)));
            }
            Message::NoticeResponse(_) => {}
            Message::ParameterStatus(body) => {
                conn.handle_parameter_status_frame(&body)?;
            }
            Message::NotificationResponse(body) => {
                conn.handle_notification_frame(&body)?;
            }
            other => {
                return Err(PostgresError::Protocol(format!(
                    "unexpected backend frame after max_rows portal batch: {}",
                    message_debug_name(&other)
                )));
            }
        }
    }
}

async fn send_portal_execute_on_conn(
    conn: &mut PostgresConnection,
    portal_name: &str,
    max_rows: i32,
) -> Result<(), PostgresError> {
    conn.write_buf.clear();
    frontend::execute(portal_name, max_rows, &mut conn.write_buf)
        .map_err(|e| PostgresError::Protocol(format!("failed to encode Execute frame: {e}")))?;
    frontend::sync(&mut conn.write_buf);
    conn.set_exchange_state(ExchangeState::InExtendedExchange);
    if let Err(err) = conn.stream.write_all(&conn.write_buf).await {
        conn.write_buf.clear();
        conn.mark_closed();
        return Err(PostgresError::Transport(format!(
            "ArrowPortalStream Execute write failed: {err}"
        )));
    }
    conn.write_buf.clear();
    if let Err(err) = conn.stream.flush().await {
        conn.mark_closed();
        return Err(PostgresError::Transport(format!(
            "ArrowPortalStream Execute flush failed: {err}"
        )));
    }
    Ok(())
}

async fn read_next_portal_wire_item_on_conn(
    conn: &mut PostgresConnection,
    columns: &[Column],
    codec_context: &CodecContext,
    type_catalog: &TypeCatalog,
) -> Result<WirePortalItem, PostgresError> {
    loop {
        let msg = read_message(&mut conn.stream, &mut conn.read_buf, None).await?;
        match msg {
            Message::DataRow(body) => {
                let buffer = body.buffer();
                let mut ranges = body.ranges();
                let mut row = Vec::with_capacity(columns.len());
                let mut idx = 0usize;
                let mut decode_err: Option<PostgresError> = None;

                while let Some(range_opt) = ranges.next().map_err(|e| {
                    PostgresError::Protocol(format!("malformed Portal DataRow range: {e}"))
                })? {
                    let Some(col) = columns.get(idx) else {
                        return Err(PostgresError::Protocol(format!(
                            "Portal DataRow field count exceeds column count {}",
                            columns.len()
                        )));
                    };
                    let raw_slice = range_opt.map(|r| &buffer[r]);
                    match decode_value_with_catalog(
                        col.type_oid,
                        col.format,
                        raw_slice,
                        codec_context,
                        type_catalog,
                    ) {
                        Ok(val) => row.push(val),
                        Err(err) => {
                            decode_err = Some(err);
                            break;
                        }
                    }
                    idx += 1;
                }
                if let Some(err) = decode_err {
                    return Ok(WirePortalItem::DecodeError(err));
                }
                if idx != columns.len() {
                    return Err(PostgresError::Protocol(format!(
                        "Portal DataRow field count {idx} does not match column count {}",
                        columns.len()
                    )));
                }
                return Ok(WirePortalItem::Row(row));
            }
            Message::PortalSuspended => return Ok(WirePortalItem::Suspended),
            Message::CommandComplete(body) => {
                let tag = body.tag().map_err(|e| {
                    PostgresError::Protocol(format!("invalid Portal CommandComplete UTF-8: {e}"))
                })?;
                let _ = parse_command_tag_affected_rows(tag)?;
                return Ok(WirePortalItem::Complete);
            }
            Message::EmptyQueryResponse => return Ok(WirePortalItem::Complete),
            Message::ErrorResponse(body) => {
                let srv = parse_error_response(&body)?;
                return Ok(WirePortalItem::ServerError(PostgresError::Server(srv)));
            }
            Message::NoticeResponse(_) => {}
            Message::ParameterStatus(body) => {
                conn.handle_parameter_status_frame(&body)?;
            }
            Message::NotificationResponse(body) => {
                conn.handle_notification_frame(&body)?;
            }
            other => {
                return Err(PostgresError::Protocol(format!(
                    "unexpected backend frame during ArrowPortalStream Execute: {}",
                    message_debug_name(&other)
                )));
            }
        }
    }
}

fn serialize_record_batch_to_shm(
    batch: &RecordBatch,
    col_names: &[String],
) -> Result<ArrowOutput, PostgresError> {
    let mut writer = PostgresArrowIpcWriter::try_new(
        batch.schema(),
        col_names.to_vec(),
        ArrowStorageMode::AlwaysShm,
        usize::MAX,
    )?;
    writer.write_batch(batch)?;
    writer.finish()
}

/// Deferred configuration held by an `AsyncPostgresArrowStream` before its first `__anext__` opens a server portal.
pub(crate) struct UnopenedArrowPortalArgs {
    pub(crate) sql: String,
    pub(crate) parameters: Vec<Parameter>,
    pub(crate) parameter_oids: Option<Vec<u32>>,
    pub(crate) batch_size: i32,
    pub(crate) columns: Option<Vec<String>>,
}

type SharedArrowOutputSlot = Arc<std::sync::Mutex<Option<ArrowOutput>>>;
pub(crate) type PortalCleanupRegistration = (String, TransactionOwner);

struct DetachedPortalCleanup {
    stream: ArrowPortalStream,
    close_gen: OperationGeneration,
    reg: Option<PortalCleanupRegistration>,
}

/// Explicit non-blocking state machine (`Unopened -> Fetching -> Open -> Closing -> Closed`)
/// governing `AsyncPostgresArrowStream` across event-loop and Tokio worker threads.
pub(crate) enum AsyncArrowPortalState {
    Unopened(Option<UnopenedArrowPortalArgs>),
    Fetching {
        active_op_gen: OperationGeneration,
        cancel_requested: bool,
        close_requested: bool,
        completed_waiting_drainer: bool,
        portal_identity: Option<PortalCleanupRegistration>,
        cleanup_registered: Option<PortalCleanupRegistration>,
        stream: Option<ArrowPortalStream>,
        pending_output: Option<SharedArrowOutputSlot>,
        pending_exhausted: bool,
    },
    Open(ArrowPortalStream),
    Closing {
        active_op_gen: Option<OperationGeneration>,
    },
    Closed {
        exhausted: bool,
    },
}

pub(crate) enum AsyncArrowFetchStart {
    OpenAndFetch {
        args: UnopenedArrowPortalArgs,
        op_gen: OperationGeneration,
    },
    FetchNext {
        stream: Box<ArrowPortalStream>,
        op_gen: OperationGeneration,
    },
    Exhausted,
}

pub(crate) enum AsyncArrowCloseStart {
    ImmediateNoOp,
    RunClose {
        stream: Box<ArrowPortalStream>,
        close_gen: OperationGeneration,
        cleanup_reg: Option<PortalCleanupRegistration>,
    },
    WaitUntilClosed(std::pin::Pin<Box<tokio::sync::futures::OwnedNotified>>),
}

/// Shared synchronization block coordinating `AsyncPostgresArrowStream` and [`PendingArrowBatchGuard`].
pub(crate) struct AsyncArrowPortalShared {
    session: Arc<SessionHandle>,
    state: std::sync::Mutex<AsyncArrowPortalState>,
    step_done_notify: Arc<tokio::sync::Notify>,
}

impl AsyncArrowPortalShared {
    pub(crate) fn new_unopened(session: Arc<SessionHandle>, args: UnopenedArrowPortalArgs) -> Self {
        Self {
            session,
            state: std::sync::Mutex::new(AsyncArrowPortalState::Unopened(Some(args))),
            step_done_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub(crate) fn state_label(&self) -> &'static str {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match &*guard {
            AsyncArrowPortalState::Unopened(_) => "unopened",
            AsyncArrowPortalState::Fetching { .. } => "fetching",
            AsyncArrowPortalState::Open(_) => "open",
            AsyncArrowPortalState::Closing { .. } => "closing",
            AsyncArrowPortalState::Closed { exhausted: true } => "exhausted",
            AsyncArrowPortalState::Closed { exhausted: false } => "closed",
        }
    }

    pub(crate) fn begin_fetch_step(&self) -> Result<AsyncArrowFetchStart, PostgresError> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let next = match &mut *guard {
            AsyncArrowPortalState::Unopened(args_opt) => {
                let args = args_opt.take().ok_or_else(|| {
                    PostgresError::Interface("Arrow portal stream already initialized".to_string())
                })?;
                let op_gen = self.session.reserve_operation_generation();
                *guard = AsyncArrowPortalState::Fetching {
                    active_op_gen: op_gen,
                    cancel_requested: false,
                    close_requested: false,
                    completed_waiting_drainer: false,
                    portal_identity: None,
                    cleanup_registered: None,
                    stream: None,
                    pending_output: None,
                    pending_exhausted: false,
                };
                AsyncArrowFetchStart::OpenAndFetch { args, op_gen }
            }
            AsyncArrowPortalState::Open(_) => {
                let op_gen = self.session.reserve_operation_generation();
                let prev = std::mem::replace(
                    &mut *guard,
                    AsyncArrowPortalState::Closed { exhausted: false },
                );
                let AsyncArrowPortalState::Open(stream) = prev else {
                    unreachable!("checked Open state");
                };
                let portal_identity = stream.portal_identity();
                *guard = AsyncArrowPortalState::Fetching {
                    active_op_gen: op_gen,
                    cancel_requested: false,
                    close_requested: false,
                    completed_waiting_drainer: false,
                    portal_identity,
                    cleanup_registered: None,
                    stream: None,
                    pending_output: None,
                    pending_exhausted: false,
                };
                AsyncArrowFetchStart::FetchNext {
                    stream: Box::new(stream),
                    op_gen,
                }
            }
            AsyncArrowPortalState::Fetching { .. } => {
                return Err(PostgresError::Interface(
                    "another __anext__ fetch is already in flight on AsyncPostgresArrowStream"
                        .to_string(),
                ));
            }
            AsyncArrowPortalState::Closing { .. } => {
                return Err(PostgresError::Interface(
                    "cannot call __anext__ while AsyncPostgresArrowStream is closing".to_string(),
                ));
            }
            AsyncArrowPortalState::Closed { exhausted: true } => AsyncArrowFetchStart::Exhausted,
            AsyncArrowPortalState::Closed { exhausted: false } => {
                return Err(PostgresError::Interface(
                    "cannot fetch from a closed AsyncPostgresArrowStream".to_string(),
                ));
            }
        };
        Ok(next)
    }

    pub(crate) fn abort_unspawned_fetch(&self, op_gen: OperationGeneration) {
        self.session.unreserve_operation_generation(op_gen);
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            *guard = AsyncArrowPortalState::Closed { exhausted: false };
        }
        self.step_done_notify.notify_waiters();
    }

    fn arm_step_waiter(&self) -> std::pin::Pin<Box<tokio::sync::futures::OwnedNotified>> {
        let notified = Arc::clone(&self.step_done_notify).notified_owned();
        let mut pinned = Box::pin(notified);
        pinned.as_mut().enable();
        pinned
    }

    fn spawn_stream_cleanup_detached(self: &Arc<Self>, mut cleanup: DetachedPortalCleanup) {
        let session = Arc::clone(&self.session);
        let shared = Arc::clone(self);
        vertebra_db::py_async::db_runtime_handle().spawn(async move {
            let _ = cleanup
                .stream
                .close_with_generation(cleanup.close_gen)
                .await;
            if let Some((pname, powner)) = cleanup.reg {
                session.finish_portal_drop_cleanup(&pname, powner).await;
            }
            {
                let mut guard = shared.state.lock().unwrap_or_else(|e| e.into_inner());
                *guard = AsyncArrowPortalState::Closed { exhausted: false };
            }
            shared.step_done_notify.notify_waiters();
        });
    }

    /// Cancel an in-flight fetch or reclaim an undelivered SHM batch queued on the completion drainer.
    pub(crate) fn cancel_fetch(self: &Arc<Self>, close_requested_flag: bool) {
        let mut dropped_output: Option<ArrowOutput> = None;
        let mut detached_cleanup: Option<DetachedPortalCleanup> = None;
        let mut op_to_cancel: Option<OperationGeneration> = None;
        let mut notify_closed = false;

        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let AsyncArrowPortalState::Fetching {
                active_op_gen,
                cancel_requested,
                close_requested,
                completed_waiting_drainer,
                portal_identity,
                cleanup_registered,
                stream,
                pending_output,
                ..
            } = &mut *guard
            {
                *cancel_requested = true;
                if close_requested_flag {
                    *close_requested = true;
                }
                if *completed_waiting_drainer {
                    if let Some(slot) = pending_output.take() {
                        dropped_output = slot.lock().unwrap_or_else(|e| e.into_inner()).take();
                    }
                    let live_stream = stream.take();
                    let prior_reg = cleanup_registered.take();
                    match live_stream {
                        Some(s) if !s.is_closed() => {
                            let reg = prior_reg.or_else(|| {
                                let id = s.portal_identity()?;
                                self.session.begin_portal_drop_cleanup(&id.0, id.1);
                                Some(id)
                            });
                            let close_gen = self.session.reserve_operation_generation();
                            *guard = AsyncArrowPortalState::Closing {
                                active_op_gen: Some(close_gen),
                            };
                            detached_cleanup = Some(DetachedPortalCleanup {
                                stream: s,
                                close_gen,
                                reg,
                            });
                        }
                        _ => {
                            if let Some((pname, powner)) = prior_reg {
                                let session = Arc::clone(&self.session);
                                let shared = Arc::clone(self);
                                *guard = AsyncArrowPortalState::Closing {
                                    active_op_gen: None,
                                };
                                vertebra_db::py_async::db_runtime_handle().spawn(async move {
                                    session.finish_portal_drop_cleanup(&pname, powner).await;
                                    {
                                        let mut g =
                                            shared.state.lock().unwrap_or_else(|e| e.into_inner());
                                        *g = AsyncArrowPortalState::Closed { exhausted: false };
                                    }
                                    shared.step_done_notify.notify_waiters();
                                });
                            } else {
                                *guard = AsyncArrowPortalState::Closed { exhausted: false };
                                notify_closed = true;
                            }
                        }
                    }
                } else {
                    if cleanup_registered.is_none()
                        && let Some((pname, powner)) = portal_identity.clone()
                    {
                        self.session.begin_portal_drop_cleanup(&pname, powner);
                        *cleanup_registered = Some((pname, powner));
                    }
                    op_to_cancel = Some(*active_op_gen);
                }
            }
        }

        drop(dropped_output);
        if let Some(op_gen) = op_to_cancel {
            self.session.trigger_async_cancel(op_gen);
        }
        if let Some(cleanup) = detached_cleanup {
            self.spawn_stream_cleanup_detached(cleanup);
        }
        if notify_closed {
            self.step_done_notify.notify_waiters();
        }
    }

    pub(crate) fn cancel_active_close(&self) {
        let op_opt = {
            let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            match &*guard {
                AsyncArrowPortalState::Closing { active_op_gen } => *active_op_gen,
                _ => None,
            }
        };
        if let Some(op_gen) = op_opt {
            self.session.trigger_async_cancel(op_gen);
        }
    }

    pub(crate) fn begin_aclose(self: &Arc<Self>) -> AsyncArrowCloseStart {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match &*guard {
            AsyncArrowPortalState::Unopened(_) => {
                *guard = AsyncArrowPortalState::Closed { exhausted: false };
                drop(guard);
                self.step_done_notify.notify_waiters();
                AsyncArrowCloseStart::ImmediateNoOp
            }
            AsyncArrowPortalState::Closed { .. } => AsyncArrowCloseStart::ImmediateNoOp,
            AsyncArrowPortalState::Open(_) => {
                let prev = std::mem::replace(
                    &mut *guard,
                    AsyncArrowPortalState::Closing {
                        active_op_gen: None,
                    },
                );
                let AsyncArrowPortalState::Open(stream) = prev else {
                    unreachable!("checked Open state");
                };
                if stream.is_closed() {
                    *guard = AsyncArrowPortalState::Closed { exhausted: false };
                    drop(guard);
                    self.step_done_notify.notify_waiters();
                    return AsyncArrowCloseStart::ImmediateNoOp;
                }
                let cleanup_reg = stream.portal_identity();
                if let Some((ref pname, powner)) = cleanup_reg {
                    self.session.begin_portal_drop_cleanup(pname, powner);
                }
                let close_gen = self.session.reserve_operation_generation();
                *guard = AsyncArrowPortalState::Closing {
                    active_op_gen: Some(close_gen),
                };
                AsyncArrowCloseStart::RunClose {
                    stream: Box::new(stream),
                    close_gen,
                    cleanup_reg,
                }
            }
            AsyncArrowPortalState::Closing { .. } => {
                let waiter = self.arm_step_waiter();
                AsyncArrowCloseStart::WaitUntilClosed(waiter)
            }
            AsyncArrowPortalState::Fetching { .. } => {
                let waiter = self.arm_step_waiter();
                drop(guard);
                self.cancel_fetch(true);
                let guard_after = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if matches!(&*guard_after, AsyncArrowPortalState::Closed { .. }) {
                    AsyncArrowCloseStart::ImmediateNoOp
                } else {
                    AsyncArrowCloseStart::WaitUntilClosed(waiter)
                }
            }
        }
    }

    pub(crate) async fn run_explicit_close(
        self: Arc<Self>,
        mut stream: ArrowPortalStream,
        close_gen: OperationGeneration,
        cleanup_reg: Option<(String, TransactionOwner)>,
    ) -> Result<(), PostgresError> {
        let res = stream.close_with_generation(close_gen).await;
        if let Some((pname, powner)) = cleanup_reg {
            self.session
                .finish_portal_drop_cleanup(&pname, powner)
                .await;
        }
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            *guard = AsyncArrowPortalState::Closed { exhausted: false };
        }
        self.step_done_notify.notify_waiters();
        res
    }

    pub(crate) async fn wait_until_closed(
        self: Arc<Self>,
        mut first_waiter: std::pin::Pin<Box<tokio::sync::futures::OwnedNotified>>,
    ) -> Result<(), PostgresError> {
        loop {
            first_waiter.await;
            let next_waiter = {
                let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if matches!(&*guard, AsyncArrowPortalState::Closed { .. }) {
                    return Ok(());
                }
                self.arm_step_waiter()
            };
            first_waiter = next_waiter;
        }
    }

    async fn finish_cancelled_or_failed_stream(&self, mut stream_opt: Option<ArrowPortalStream>) {
        let action = {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let prior_reg = match &mut *guard {
                AsyncArrowPortalState::Fetching {
                    cleanup_registered, ..
                } => cleanup_registered.take(),
                _ => None,
            };
            match stream_opt.as_ref().filter(|s| !s.is_closed()) {
                Some(live) => {
                    let reg = prior_reg.or_else(|| {
                        let id = live.portal_identity()?;
                        self.session.begin_portal_drop_cleanup(&id.0, id.1);
                        Some(id)
                    });
                    let close_gen = self.session.reserve_operation_generation();
                    *guard = AsyncArrowPortalState::Closing {
                        active_op_gen: Some(close_gen),
                    };
                    Some((Some(close_gen), reg))
                }
                None => {
                    if let Some(reg) = prior_reg {
                        *guard = AsyncArrowPortalState::Closing {
                            active_op_gen: None,
                        };
                        Some((None, Some(reg)))
                    } else {
                        *guard = AsyncArrowPortalState::Closed { exhausted: false };
                        None
                    }
                }
            }
        };

        let Some((close_gen_opt, reg_opt)) = action else {
            self.step_done_notify.notify_waiters();
            return;
        };

        if let (Some(close_gen), Some(mut s)) = (close_gen_opt, stream_opt.take()) {
            let _ = s.close_with_generation(close_gen).await;
        }
        if let Some((pname, powner)) = reg_opt {
            self.session
                .finish_portal_drop_cleanup(&pname, powner)
                .await;
        }
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            *guard = AsyncArrowPortalState::Closed { exhausted: false };
        }
        self.step_done_notify.notify_waiters();
    }

    pub(crate) async fn drive_fetch_step(
        self: Arc<Self>,
        start: AsyncArrowFetchStart,
    ) -> Result<Option<PendingArrowBatchGuard>, PostgresError> {
        let (mut stream, fetch_gen) = match start {
            AsyncArrowFetchStart::Exhausted => return Ok(None),
            AsyncArrowFetchStart::FetchNext { stream, op_gen } => (*stream, op_gen),
            AsyncArrowFetchStart::OpenAndFetch { args, op_gen } => {
                let opened = match ArrowPortalStream::open_with_generation(
                    &self.session,
                    op_gen,
                    &args.sql,
                    &args.parameters,
                    args.parameter_oids.as_deref(),
                    args.batch_size,
                    args.columns,
                )
                .await
                {
                    Ok(s) => s,
                    Err(err) => {
                        self.finish_cancelled_or_failed_stream(None).await;
                        return Err(err);
                    }
                };

                let post_open = {
                    let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    match &mut *guard {
                        AsyncArrowPortalState::Fetching {
                            active_op_gen,
                            cancel_requested,
                            close_requested,
                            portal_identity,
                            ..
                        } => {
                            if *cancel_requested || *close_requested || self.session.will_retire() {
                                None
                            } else {
                                let next_gen = self.session.reserve_operation_generation();
                                *active_op_gen = next_gen;
                                *portal_identity = opened.portal_identity();
                                Some(next_gen)
                            }
                        }
                        _ => None,
                    }
                };

                match post_open {
                    Some(next_gen) => (opened, next_gen),
                    None => {
                        self.finish_cancelled_or_failed_stream(Some(opened)).await;
                        return Err(PostgresError::Operational(
                            "Arrow portal stream was cancelled during portal open".to_string(),
                        ));
                    }
                }
            }
        };

        match stream.next_batch_shm_with_generation(fetch_gen).await {
            Err(err) => {
                self.finish_cancelled_or_failed_stream(Some(stream)).await;
                Err(err)
            }
            Ok(None) => {
                let (was_cancelled, prior_reg) = {
                    let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    match &mut *guard {
                        AsyncArrowPortalState::Fetching {
                            cancel_requested,
                            close_requested,
                            cleanup_registered,
                            ..
                        } => {
                            let c = *cancel_requested || *close_requested;
                            let r = cleanup_registered.take();
                            if r.is_some() {
                                *guard = AsyncArrowPortalState::Closing {
                                    active_op_gen: None,
                                };
                            } else {
                                *guard = AsyncArrowPortalState::Closed { exhausted: !c };
                            }
                            (c, r)
                        }
                        _ => (true, None),
                    }
                };
                if let Some((pname, powner)) = prior_reg {
                    self.session
                        .finish_portal_drop_cleanup(&pname, powner)
                        .await;
                    let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    *guard = AsyncArrowPortalState::Closed {
                        exhausted: !was_cancelled,
                    };
                }
                self.step_done_notify.notify_waiters();
                if was_cancelled {
                    Err(PostgresError::Operational(
                        "Arrow portal stream was cancelled".to_string(),
                    ))
                } else {
                    Ok(None)
                }
            }
            Ok(Some(arrow_out)) => {
                let stream_exhausted = stream.is_exhausted() && stream.is_closed();
                let output_slot: SharedArrowOutputSlot =
                    Arc::new(std::sync::Mutex::new(Some(arrow_out)));

                let parked = {
                    let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    match &mut *guard {
                        AsyncArrowPortalState::Fetching {
                            cancel_requested,
                            close_requested,
                            completed_waiting_drainer,
                            stream: slot_stream,
                            pending_output,
                            pending_exhausted,
                            ..
                        } if !*cancel_requested && !*close_requested => {
                            *completed_waiting_drainer = true;
                            *pending_output = Some(Arc::clone(&output_slot));
                            *pending_exhausted = stream_exhausted;
                            if !stream_exhausted {
                                *slot_stream = Some(stream);
                            }
                            Ok(())
                        }
                        _ => Err(stream),
                    }
                };

                match parked {
                    Ok(()) => Ok(Some(PendingArrowBatchGuard::new(output_slot, self))),
                    Err(unparked_stream) => {
                        let reclaimed =
                            output_slot.lock().unwrap_or_else(|e| e.into_inner()).take();
                        drop(reclaimed);
                        self.finish_cancelled_or_failed_stream(Some(unparked_stream))
                            .await;
                        Err(PostgresError::Operational(
                            "Arrow portal stream was cancelled".to_string(),
                        ))
                    }
                }
            }
        }
    }

    fn abort_after_cursor_error(self: &Arc<Self>) {
        let mut to_close = None;
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if matches!(&*guard, AsyncArrowPortalState::Open(_)) {
                let prev = std::mem::replace(
                    &mut *guard,
                    AsyncArrowPortalState::Closed { exhausted: false },
                );
                if let AsyncArrowPortalState::Open(s) = prev
                    && !s.is_closed()
                {
                    let reg = s.portal_identity();
                    if let Some((ref pname, powner)) = reg {
                        self.session.begin_portal_drop_cleanup(pname, powner);
                    }
                    let close_gen = self.session.reserve_operation_generation();
                    *guard = AsyncArrowPortalState::Closing {
                        active_op_gen: Some(close_gen),
                    };
                    to_close = Some(DetachedPortalCleanup {
                        stream: s,
                        close_gen,
                        reg,
                    });
                }
            }
        }
        if let Some(cleanup) = to_close {
            self.spawn_stream_cleanup_detached(cleanup);
        } else {
            self.step_done_notify.notify_waiters();
        }
    }
}

/// Drop-safe completion guard wrapping a produced `ArrowOutput::Shm` and shared
/// [`AsyncArrowPortalShared`] state inside `DbCompletionDrainer`'s `map_ok` closure.
///
/// When the awaiting `asyncio.Future` is not cancelled, [`PendingArrowBatchGuard::into_py_cursor`]
/// disarms the guard, transitions `Fetching -> Open` (or `Closed` on final completion), and hands
/// only the batch `Arc<ShmRegion>` into `vertebra._vertebra.ArrowCursor`. When the future was
/// cancelled before `DbCompletionDrainer` runs `map_ok`, `Drop` immediately drops the undelivered
/// `ArrowOutput::Shm` and spawns bounded generation-fenced portal close/cancel/retirement on
/// `db_runtime_handle()`.
pub(crate) struct PendingArrowBatchGuard {
    output_slot: SharedArrowOutputSlot,
    shared: Arc<AsyncArrowPortalShared>,
    disarmed: bool,
}

impl PendingArrowBatchGuard {
    fn new(output_slot: SharedArrowOutputSlot, shared: Arc<AsyncArrowPortalShared>) -> Self {
        Self {
            output_slot,
            shared,
            disarmed: false,
        }
    }

    pub(crate) fn into_py_cursor(
        mut self,
        py: pyo3::Python<'_>,
    ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        self.disarmed = true;
        let output_opt = self
            .output_slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();

        let transitioned = {
            let mut guard = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
            match &mut *guard {
                AsyncArrowPortalState::Fetching {
                    cancel_requested,
                    close_requested,
                    stream,
                    pending_output,
                    pending_exhausted,
                    ..
                } if !*cancel_requested && !*close_requested => {
                    let _ = pending_output.take();
                    let exhausted = *pending_exhausted;
                    if exhausted {
                        *guard = AsyncArrowPortalState::Closed { exhausted: true };
                        true
                    } else if let Some(s) = stream.take() {
                        *guard = AsyncArrowPortalState::Open(s);
                        true
                    } else {
                        *guard = AsyncArrowPortalState::Closed { exhausted: false };
                        false
                    }
                }
                _ => false,
            }
        };

        if !transitioned {
            drop(output_opt);
            self.shared.cancel_fetch(false);
            return Err(crate::errors::postgres_error_to_py(
                py,
                PostgresError::Operational("Arrow portal stream was cancelled".to_string()),
            ));
        }

        self.shared.step_done_notify.notify_waiters();
        let Some(output) = output_opt else {
            self.shared.abort_after_cursor_error();
            return Err(crate::errors::postgres_error_to_py(
                py,
                PostgresError::Operational(
                    "Arrow portal batch was reclaimed on cancellation".to_string(),
                ),
            ));
        };

        match crate::arrow::shm_output_into_py_cursor(py, output) {
            Ok(cursor) => Ok(cursor),
            Err(py_err) => {
                self.shared.abort_after_cursor_error();
                Err(py_err)
            }
        }
    }
}

impl Drop for PendingArrowBatchGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        let undelivered = self
            .output_slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        drop(undelivered);
        self.shared.cancel_fetch(false);
    }
}
