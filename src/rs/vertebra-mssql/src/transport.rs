//! TDS 7.3+ transport session, `PRELOGIN` / TLS negotiation, `ENVCHANGE` routing redirects,
//! and out-of-band `ATTENTION` (`0x06`) cancellation for `vertebra-mssql`.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow_array::RecordBatch;
use futures_util::TryStreamExt;
use tiberius::error::Error as TiberiusError;
use tiberius::{Client as TiberiusClient, QueryItem, ToSql};
use tokio::sync::Notify;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use vertebra_db::{
    BoxFuture, CancelHandle, Connection, TransportError, TransportStream,
    install_rustls_ring_provider,
};

use crate::bulk_load::record_batch_to_token_rows;
use crate::codecs::MssqlQueryResult;
use crate::config::{MssqlClientConfig, MssqlIsolationLevel};
use crate::errors::{MssqlError, MssqlErrorKind};
use crate::params::MssqlParam;

/// Maximum number of TDS `ENVCHANGE` routing redirects followed during connection establishment.
pub const MAX_ROUTING_REDIRECTS: usize = 3;

/// Shared out-of-band cancellation state for an [`MssqlSession`].
#[derive(Debug, Default)]
pub struct MssqlCancelState {
    armed: AtomicBool,
    notify: Notify,
}

impl MssqlCancelState {
    /// Create a new cancellation state.
    pub fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// Signal cancellation to any in-flight operation on the parent session.
    pub fn trigger(&self) {
        self.armed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Clear any armed cancellation flag and return the previous state.
    pub fn take_armed(&self) -> bool {
        self.armed.swap(false, Ordering::SeqCst)
    }
}

/// [`vertebra_db::CancelHandle`] implementation for [`MssqlSession`].
#[derive(Clone, Debug)]
pub struct MssqlCancelHandle {
    state: Arc<MssqlCancelState>,
}

impl MssqlCancelHandle {
    /// Construct a handle sharing `state`.
    pub fn new(state: Arc<MssqlCancelState>) -> Self {
        Self { state }
    }

    /// Trigger cancellation synchronously without awaiting a future.
    pub fn trigger_now(&self) {
        self.state.trigger();
    }
}

impl CancelHandle for MssqlCancelHandle {
    fn cancel(&self) -> BoxFuture<'_, Result<(), TransportError>> {
        self.state.trigger();
        Box::pin(async { Ok(()) })
    }
}

/// Active TDS 7.3+ wire session wrapping `tiberius::Client<Compat<TransportStream>>`.
pub struct MssqlSession {
    client: TiberiusClient<Compat<TransportStream>>,
    cancel_state: Arc<MssqlCancelState>,
    broken: bool,
    in_transaction: bool,
    routed_host: String,
    routed_port: u16,
    query_timeout: Duration,
}

impl MssqlSession {
    /// Connect a new TDS 7.3+ session, negotiating `PRELOGIN` encryption (or Cloud SQL `:3307` mTLS),
    /// authenticating via `LOGIN7`, and following up to [`MAX_ROUTING_REDIRECTS`] `ENVCHANGE` routing tokens.
    pub async fn connect(config: &MssqlClientConfig) -> Result<Self, MssqlError> {
        install_rustls_ring_provider();
        let mut target_host = config.host.clone();
        let mut target_port = config.port;

        for hop in 0..=MAX_ROUTING_REDIRECTS {
            let conn_opts = config.to_connect_options(&target_host, target_port)?;
            let stream = TransportStream::connect(&conn_opts).await?;
            let pre_encrypted = stream.is_tls();
            let tib_cfg = config.to_tiberius_config(&target_host, target_port, pre_encrypted);

            let connect_res = tokio::time::timeout(
                config.connect_timeout,
                TiberiusClient::connect(tib_cfg, stream.compat_write()),
            )
            .await;

            match connect_res {
                Ok(Ok(client)) => {
                    return Ok(Self {
                        client,
                        cancel_state: Arc::new(MssqlCancelState::new()),
                        broken: false,
                        in_transaction: false,
                        routed_host: target_host,
                        routed_port: target_port,
                        query_timeout: config.query_timeout,
                    });
                }
                Ok(Err(TiberiusError::Routing { host, port })) => {
                    if hop == MAX_ROUTING_REDIRECTS {
                        return Err(MssqlError::new(
                            MssqlErrorKind::ConnectionFailure,
                            format!(
                                "exceeded maximum MSSQL ENVCHANGE routing redirects ({MAX_ROUTING_REDIRECTS}) at {host}:{port}"
                            ),
                            Some("08001"),
                            None,
                            false,
                        ));
                    }
                    target_host = host;
                    target_port = port;
                }
                Ok(Err(err)) => return Err(MssqlError::from(err)),
                Err(_) => {
                    return Err(MssqlError::new(
                        MssqlErrorKind::ConnectionFailure,
                        format!(
                            "MSSQL TDS PRELOGIN/LOGIN7 handshake timed out after {:?}",
                            config.connect_timeout
                        ),
                        Some("08001"),
                        None,
                        true,
                    ));
                }
            }
        }

        Err(MssqlError::new(
            MssqlErrorKind::ConnectionFailure,
            "failed to establish MSSQL session",
            Some("08001"),
            None,
            false,
        ))
    }

    /// Hostname after any `ENVCHANGE` routing redirects.
    pub fn routed_host(&self) -> &str {
        &self.routed_host
    }

    /// TCP port after any `ENVCHANGE` routing redirects.
    pub const fn routed_port(&self) -> u16 {
        self.routed_port
    }

    /// Return `true` when this session has an open transaction.
    pub const fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Return a cloneable [`MssqlCancelHandle`] for out-of-band `ATTENTION` cancellation.
    pub fn mssql_cancel_handle(&self) -> MssqlCancelHandle {
        MssqlCancelHandle::new(Arc::clone(&self.cancel_state))
    }

    /// Transmit a TDS `0x06` `ATTENTION` packet and drain the server's `DONE_ATTN` response token.
    pub async fn send_attention(&mut self) -> Result<(), MssqlError> {
        self.cancel_state.take_armed();
        if let Err(err) = self.client.cancel_query().await {
            self.broken = true;
            return Err(MssqlError::from(err));
        }
        Ok(())
    }

    async fn run_cancellable<T, F>(
        &mut self,
        op: F,
    ) -> Result<T, MssqlError>
    where
        F: FnOnce(&mut TiberiusClient<Compat<TransportStream>>) -> BoxFuture<'_, Result<T, MssqlError>>,
    {
        if self.cancel_state.take_armed() {
            let _ = self.client.cancel_query().await;
            return Err(MssqlError::cancelled(
                "MSSQL query cancelled via TDS ATTENTION before execution",
            ));
        }

        let timeout = self.query_timeout;
        let cancel_state = Arc::clone(&self.cancel_state);
        let outcome = {
            let fut = op(&mut self.client);
            tokio::select! {
                biased;
                () = cancel_state.notify.notified() => {
                    Err(CancelReason::Attention)
                }
                res = tokio::time::timeout(timeout, fut) => {
                    match res {
                        Ok(inner) => Ok(inner),
                        Err(_) => Err(CancelReason::Timeout(timeout)),
                    }
                }
            }
        };

        match outcome {
            Ok(Ok(val)) => Ok(val),
            Ok(Err(err)) => {
                if matches!(err.kind, MssqlErrorKind::ConnectionFailure) {
                    self.broken = true;
                }
                Err(err)
            }
            Err(reason) => {
                cancel_state.take_armed();
                if self.client.cancel_query().await.is_err() {
                    self.broken = true;
                }
                match reason {
                    CancelReason::Attention => Err(MssqlError::cancelled(
                        "MSSQL query cancelled via out-of-band TDS ATTENTION packet",
                    )),
                    CancelReason::Timeout(dur) => Err(MssqlError::cancelled(format!(
                        "MSSQL query timed out after {dur:?}; sent TDS ATTENTION packet"
                    ))),
                }
            }
        }
    }

    /// Execute a DML/DDL statement (`INSERT`, `UPDATE`, `DELETE`, `CREATE`, etc.) and return affected row count.
    pub async fn execute_dml(
        &mut self,
        sql: &str,
        params: &[MssqlParam],
    ) -> Result<u64, MssqlError> {
        let sql_owned = sql.to_string();
        let params_owned = params.to_vec();
        self.run_cancellable(|client| {
            Box::pin(async move {
                let refs: Vec<&dyn ToSql> = params_owned.iter().map(MssqlParam::as_to_sql).collect();
                let res = client.execute(sql_owned, &refs).await?;
                Ok(res.rows_affected().iter().sum())
            })
        })
        .await
    }

    /// Execute a query (`SELECT` or `OUTPUT`) and decode all rows and column metadata.
    pub async fn execute_query(
        &mut self,
        sql: &str,
        params: &[MssqlParam],
    ) -> Result<MssqlQueryResult, MssqlError> {
        let sql_owned = sql.to_string();
        let params_owned = params.to_vec();
        self.run_cancellable(|client| {
            Box::pin(async move {
                let refs: Vec<&dyn ToSql> = params_owned.iter().map(MssqlParam::as_to_sql).collect();
                let mut stream = client.query(sql_owned, &refs).await?;
                let mut columns = Vec::new();
                let mut rows = Vec::new();

                while let Some(item) = stream.try_next().await? {
                    match item {
                        QueryItem::Metadata(meta) => {
                            if columns.is_empty() {
                                columns = MssqlQueryResult::extract_columns(meta.columns());
                            }
                        }
                        QueryItem::Row(row) => {
                            if columns.is_empty() {
                                columns = MssqlQueryResult::extract_columns(row.columns());
                            }
                            rows.push(MssqlQueryResult::decode_row(&row)?);
                        }
                    }
                }

                let rows_affected = rows.len() as u64;
                Ok(MssqlQueryResult {
                    columns,
                    rows,
                    rows_affected,
                })
            })
        })
        .await
    }

    /// Begin a TDS transaction with an optional [`MssqlIsolationLevel`].
    pub async fn begin_transaction(
        &mut self,
        isolation: Option<MssqlIsolationLevel>,
    ) -> Result<(), MssqlError> {
        self.run_cancellable(|client| {
            Box::pin(async move {
                match isolation {
                    Some(iso) => {
                        client
                            .begin_transaction_with_isolation(iso.to_tiberius())
                            .await?;
                    }
                    None => {
                        client.begin_transaction().await?;
                    }
                }
                Ok(())
            })
        })
        .await?;
        self.in_transaction = true;
        Ok(())
    }

    /// Commit the active TDS transaction.
    pub async fn commit_transaction(&mut self) -> Result<(), MssqlError> {
        self.run_cancellable(|client| {
            Box::pin(async move {
                client.commit_transaction().await?;
                Ok(())
            })
        })
        .await?;
        self.in_transaction = false;
        Ok(())
    }

    /// Roll back the active TDS transaction.
    pub async fn rollback_transaction(&mut self) -> Result<(), MssqlError> {
        self.run_cancellable(|client| {
            Box::pin(async move {
                client.rollback_transaction().await?;
                Ok(())
            })
        })
        .await?;
        self.in_transaction = false;
        Ok(())
    }

    /// Create a named transaction savepoint (`SAVE TRANSACTION`).
    pub async fn save_transaction(&mut self, name: &str) -> Result<(), MssqlError> {
        validate_savepoint_identifier(name)?;
        let name_owned = name.to_string();
        self.run_cancellable(|client| {
            Box::pin(async move {
                client.save_transaction(&name_owned).await?;
                Ok(())
            })
        })
        .await
    }

    /// Roll back to a named transaction savepoint (`ROLLBACK TRANSACTION <name>`).
    pub async fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), MssqlError> {
        validate_savepoint_identifier(name)?;
        let stmt = format!("ROLLBACK TRANSACTION [{name}]");
        self.run_cancellable(|client| {
            Box::pin(async move {
                let mut stream = client.simple_query(stmt).await?;
                while stream.try_next().await?.is_some() {}
                Ok(())
            })
        })
        .await
    }

    /// Validate a named transaction savepoint for release (T-SQL releases savepoints on transaction completion).
    pub async fn release_savepoint(&mut self, name: &str) -> Result<(), MssqlError> {
        validate_savepoint_identifier(name)?;
        Ok(())
    }

    /// Stream Arrow [`RecordBatch`]es into `table` via TDS `0x07` `BulkLoadRequest`.
    pub async fn bulk_load_batches(
        &mut self,
        table: &str,
        batches: &[RecordBatch],
        columns: Option<&[String]>,
    ) -> Result<u64, MssqlError> {
        if table.trim().is_empty() {
            return Err(MssqlError::programming("bulk load target table name must not be empty"));
        }
        let mut token_rows = Vec::new();
        for batch in batches {
            let mut batch_rows = record_batch_to_token_rows(batch)?;
            token_rows.append(&mut batch_rows);
        }
        if token_rows.is_empty() {
            return Ok(0);
        }

        let table_owned = table.to_string();
        let cols_owned: Option<Vec<String>> = columns.map(<[String]>::to_vec);
        self.run_cancellable(|client| {
            Box::pin(async move {
                let col_refs: Vec<&str> = cols_owned
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(String::as_str)
                    .collect();
                let mut req = if cols_owned.is_some() {
                    client.bulk_insert_columns(&table_owned, &col_refs).await?
                } else {
                    client.bulk_insert(&table_owned).await?
                };
                for row in token_rows {
                    req.send(row).await?;
                }
                let res = req.finalize().await?;
                Ok(res.rows_affected().iter().sum())
            })
        })
        .await
    }
}

enum CancelReason {
    Attention,
    Timeout(Duration),
}

fn validate_savepoint_identifier(name: &str) -> Result<(), MssqlError> {
    let trimmed = name.trim();
    if trimmed.is_empty()
        || trimmed.len() > 128
        || !trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(MssqlError::programming(format!(
            "invalid SQL Server savepoint identifier `{name}`"
        )));
    }
    Ok(())
}

impl Connection for MssqlSession {
    fn ping(&mut self) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async move {
            let mut stream = self.client.simple_query("SELECT 1").await.map_err(|e| {
                self.broken = true;
                TransportError::Io(std::io::Error::other(e.to_string()))
            })?;
            while stream.try_next().await.map_err(|e| {
                self.broken = true;
                TransportError::Io(std::io::Error::other(e.to_string()))
            })?.is_some() {}
            Ok(())
        })
    }

    fn is_broken(&self) -> bool {
        self.broken
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(self.mssql_cancel_handle())
    }

    fn close(self) -> BoxFuture<'static, Result<(), TransportError>> {
        Box::pin(async move {
            self.client
                .close()
                .await
                .map_err(|e| TransportError::Io(std::io::Error::other(e.to_string())))
        })
    }
}

#[allow(dead_code)]
fn _assert_session_future_send<F: Future + Send>(_: F) {}
