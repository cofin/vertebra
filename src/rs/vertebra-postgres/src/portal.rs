//! Concrete PostgreSQL Protocol 3.0 bounded `Portal` streaming (`Execute(max_rows)`, `PortalSuspended`, `TransactionScope` pinning).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use tokio::io::AsyncWriteExt;

use crate::codecs::{CodecContext, TypeCatalog, decode_value_with_catalog};
use crate::errors::PostgresError;
use crate::protocol::read_message;
use crate::query::{parse_command_tag_affected_rows, parse_error_response};
use crate::result::{Column, PgValue, QueryResult, TransactionStatus};
use crate::session::cancel::OperationGeneration;
use crate::session::{ExchangeState, PostgresConnection, SessionHandle};
use crate::transaction::{TransactionScope, TransactionScopeKind};

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
