//! High-level pooled [`MssqlClient`] coordinating [`MssqlSession`] reuse, [`MssqlBrokerBackend`]
//! leases and `@P1` statement caching, transactions, out-of-band `ATTENTION` cancellation,
//! `VTBARROW` shared-memory Arrow export, and `BulkLoadRequest` ingestion.

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use tokio::sync::Mutex as AsyncMutex;
use vertebra_db::Connection;

use crate::arrow_export::{
    ShmArrowBatchPart, query_result_to_shm_arrow, query_result_to_shm_arrow_batches,
};
use crate::broker::MssqlBrokerBackend;
use crate::bulk_load::{decode_arrow_ipc_batches, decode_shm_arrow_batches};
use crate::codecs::{MssqlCellValue, MssqlQueryResult};
use crate::config::{MssqlClientConfig, MssqlIsolationLevel};
use crate::errors::{MssqlError, MssqlErrorKind};
use crate::params::MssqlParam;
use crate::transport::{MssqlCancelHandle, MssqlSession};

/// Thread-safe native SQL Server client managing TDS 7.3+ wire sessions and SHM broker leases.
pub struct MssqlClient {
    config: MssqlClientConfig,
    broker: MssqlBrokerBackend,
    session: AsyncMutex<Option<MssqlSession>>,
    active_cancel: StdMutex<Option<MssqlCancelHandle>>,
    routing_destination: StdMutex<Option<(String, u16)>>,
    in_transaction: AtomicBool,
    closed: AtomicBool,
}

impl MssqlClient {
    /// Construct a new [`MssqlClient`] with an initialized [`MssqlBrokerBackend`].
    pub fn new(config: MssqlClientConfig) -> Result<Self, MssqlError> {
        let broker = MssqlBrokerBackend::new(
            config.pool_size,
            config.max_overflow,
            config.statement_cache_capacity,
        )?;
        Ok(Self {
            config,
            broker,
            session: AsyncMutex::new(None),
            active_cancel: StdMutex::new(None),
            routing_destination: StdMutex::new(None),
            in_transaction: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        })
    }

    /// Borrow the client configuration.
    pub fn config(&self) -> &MssqlClientConfig {
        &self.config
    }

    /// Borrow the shared-memory broker backend.
    pub fn broker(&self) -> &MssqlBrokerBackend {
        &self.broker
    }

    /// Return `true` if `close()` has been called on this client.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Return `true` if an explicit transaction is currently open on this client's session.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction.load(Ordering::Acquire)
    }

    /// Return the latest `(host, port)` received via TDS `ENVCHANGE` routing, if any.
    pub fn routing_destination(&self) -> Option<(String, u16)> {
        self.routing_destination
            .lock()
            .ok()
            .and_then(|g| g.clone())
    }

    fn ensure_open(&self) -> Result<(), MssqlError> {
        if self.is_closed() {
            return Err(MssqlError::config("MSSQL client is closed"));
        }
        Ok(())
    }

    async fn with_session<T, F>(&self, f: F) -> Result<T, MssqlError>
    where
        F: for<'a> FnOnce(
            &'a mut MssqlSession,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, MssqlError>> + Send + 'a>,
        >,
    {
        self.ensure_open()?;
        let grant = self.broker.acquire_lease(
            std::process::id(),
            self.config.query_timeout.as_nanos() as u64,
            self.config.connect_timeout,
        )?;

        let result = async {
            let mut guard = self.session.lock().await;
            if guard.as_ref().is_none_or(MssqlSession::is_broken) {
                let fresh = MssqlSession::connect(&self.config).await?;
                if let Ok(mut cancel_guard) = self.active_cancel.lock() {
                    *cancel_guard = Some(fresh.mssql_cancel_handle());
                }
                *guard = Some(fresh);
            }
            let sess = guard.as_mut().expect("session initialized");
            if let Ok(mut cancel_guard) = self.active_cancel.lock() {
                *cancel_guard = Some(sess.mssql_cancel_handle());
            }
            let res = f(sess).await;
            if let Ok(mut route_guard) = self.routing_destination.lock() {
                *route_guard = sess.routing_destination();
            }
            if sess.is_broken() {
                self.in_transaction.store(false, Ordering::Release);
                *guard = None;
            } else {
                self.in_transaction
                    .store(sess.in_transaction(), Ordering::Release);
            }
            res
        }
        .await;

        let _ = self.broker.release_lease(grant);
        result
    }

    /// Establish a wire session if not already connected.
    pub async fn connect(&self) -> Result<(), MssqlError> {
        self.with_session(|_| Box::pin(async { Ok(()) })).await
    }

    /// Trigger out-of-band TDS `ATTENTION` cancellation on the active session.
    pub async fn cancel(&self) -> Result<(), MssqlError> {
        let handle_opt = self
            .active_cancel
            .lock()
            .ok()
            .and_then(|g| g.as_ref().cloned());
        if let Some(handle) = handle_opt {
            handle.trigger_now();
        }
        if let Ok(mut guard) = self.session.try_lock()
            && let Some(sess) = guard.as_mut()
        {
            sess.send_attention().await?;
        }
        Ok(())
    }

    /// Close the active TDS session and mark the client closed.
    pub async fn close(&self) -> Result<(), MssqlError> {
        self.closed.store(true, Ordering::Release);
        self.in_transaction.store(false, Ordering::Release);
        let mut guard = self.session.lock().await;
        if let Some(sess) = guard.take() {
            let _ = sess.close().await;
        }
        Ok(())
    }

    /// Execute a wire-level `SELECT 1` liveness check.
    pub async fn ping(&self) -> Result<bool, MssqlError> {
        self.with_session(|sess| {
            Box::pin(async move {
                use vertebra_db::Connection;
                sess.ping().await.map_err(MssqlError::from)?;
                Ok(true)
            })
        })
        .await
    }

    /// Execute a DML/DDL statement and return affected row count.
    pub async fn execute(&self, sql: String, params: Vec<MssqlParam>) -> Result<u64, MssqlError> {
        let (compiled_sql, _) = self.broker.resolve_or_rewrite_sql(&sql);
        self.with_session(|sess| {
            Box::pin(async move { sess.execute_dml(&compiled_sql, &params).await })
        })
        .await
    }

    /// Execute a sequence of parameterized DML statements and return total affected rows.
    pub async fn execute_many(
        &self,
        batches: Vec<(String, Vec<MssqlParam>)>,
    ) -> Result<u64, MssqlError> {
        if batches.is_empty() {
            return Ok(0);
        }
        let compiled_batches: Vec<(String, Vec<MssqlParam>)> = batches
            .into_iter()
            .map(|(s, p)| {
                let (compiled, _) = self.broker.resolve_or_rewrite_sql(&s);
                (compiled, p)
            })
            .collect();

        self.with_session(|sess| {
            Box::pin(async move {
                let mut total = 0_u64;
                for (sql, params) in &compiled_batches {
                    total += sess.execute_dml(sql, params).await?;
                }
                Ok(total)
            })
        })
        .await
    }

    /// Execute a `SELECT` query and return the full [`MssqlQueryResult`].
    pub async fn query(
        &self,
        sql: String,
        params: Vec<MssqlParam>,
    ) -> Result<MssqlQueryResult, MssqlError> {
        let (compiled_sql, _) = self.broker.resolve_or_rewrite_sql(&sql);
        self.with_session(|sess| {
            Box::pin(async move { sess.execute_query(&compiled_sql, &params).await })
        })
        .await
    }

    /// Execute a query and return the single first row, raising `NotFoundError` (`02000`) when empty.
    pub async fn fetch_one(
        &self,
        sql: String,
        params: Vec<MssqlParam>,
    ) -> Result<MssqlQueryResult, MssqlError> {
        let mut res = self.query(sql, params).await?;
        if res.rows.is_empty() {
            return Err(MssqlError::not_found(
                "fetch_one query returned zero rows",
            ));
        }
        res.rows.truncate(1);
        Ok(res)
    }

    /// Execute a query and return at most one row (`None` if empty).
    pub async fn fetch_optional(
        &self,
        sql: String,
        params: Vec<MssqlParam>,
    ) -> Result<MssqlQueryResult, MssqlError> {
        let mut res = self.query(sql, params).await?;
        if res.rows.len() > 1 {
            res.rows.truncate(1);
        }
        Ok(res)
    }

    /// Execute a scalar query and return the first column of the first row.
    pub async fn select_value(
        &self,
        sql: String,
        params: Vec<MssqlParam>,
    ) -> Result<MssqlCellValue, MssqlError> {
        let res = self.query(sql, params).await?;
        Ok(res
            .rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next())
            .unwrap_or(MssqlCellValue::Null))
    }

    /// Begin a transaction with an optional isolation level name.
    pub async fn begin_transaction(
        &self,
        isolation_level: Option<String>,
    ) -> Result<(), MssqlError> {
        let iso = match isolation_level.as_deref() {
            Some(s) if !s.trim().is_empty() => Some(MssqlIsolationLevel::parse(s)?),
            _ => None,
        };
        self.with_session(|sess| Box::pin(async move { sess.begin_transaction(iso).await }))
            .await
    }

    /// Commit the active transaction on this client's session.
    pub async fn commit_transaction(&self) -> Result<(), MssqlError> {
        self.with_session(|sess| Box::pin(async move { sess.commit_transaction().await }))
            .await
    }

    /// Roll back the active transaction on this client's session.
    pub async fn rollback_transaction(&self) -> Result<(), MssqlError> {
        self.with_session(|sess| Box::pin(async move { sess.rollback_transaction().await }))
            .await
    }

    /// Create a named savepoint (`SAVE TRANSACTION <name>`).
    pub async fn savepoint(&self, name: String) -> Result<(), MssqlError> {
        self.with_session(|sess| Box::pin(async move { sess.save_transaction(&name).await }))
            .await
    }

    /// Roll back to a named savepoint (`ROLLBACK TRANSACTION <name>`).
    pub async fn rollback_to_savepoint(&self, name: String) -> Result<(), MssqlError> {
        self.with_session(|sess| Box::pin(async move { sess.rollback_to_savepoint(&name).await }))
            .await
    }

    /// Release a named savepoint (validates savepoint identifier; T-SQL releases savepoints on commit/rollback).
    pub async fn release_savepoint(&self, name: String) -> Result<(), MssqlError> {
        self.with_session(|sess| Box::pin(async move { sess.release_savepoint(&name).await }))
            .await
    }

    /// Execute a query and materialize the result into a `VTBARROW` POSIX shared-memory [`ShmRegion`].
    pub async fn select_to_arrow(
        &self,
        sql: String,
        params: Vec<MssqlParam>,
    ) -> Result<ShmArrowBatchPart, MssqlError> {
        let res = self.query(sql, params).await?;
        query_result_to_shm_arrow(&res)
    }

    /// Execute a query and materialize the result into chunked `VTBARROW` shared-memory [`ShmRegion`]s.
    pub async fn stream_arrow_batches(
        &self,
        sql: String,
        params: Vec<MssqlParam>,
        batch_size: usize,
    ) -> Result<Vec<ShmArrowBatchPart>, MssqlError> {
        let res = self.query(sql, params).await?;
        query_result_to_shm_arrow_batches(&res, batch_size)
    }

    /// Stream Arrow [`RecordBatch`]es into `table` via TDS `0x07` `BulkLoadRequest`.
    pub async fn load_from_arrow_batches(
        &self,
        table: String,
        batches: Vec<RecordBatch>,
        columns: Option<Vec<String>>,
    ) -> Result<u64, MssqlError> {
        self.with_session(|sess| {
            Box::pin(async move {
                sess.bulk_load_batches(&table, &batches, columns.as_deref())
                    .await
            })
        })
        .await
    }

    /// Decode an Arrow IPC stream byte vector and bulk-load all batches into `table`.
    pub async fn load_from_arrow_ipc(
        &self,
        table: String,
        ipc_bytes: Vec<u8>,
        columns: Option<Vec<String>>,
    ) -> Result<u64, MssqlError> {
        let batches = decode_arrow_ipc_batches(&ipc_bytes)?;
        self.load_from_arrow_batches(table, batches, columns).await
    }

    /// Read a `VTBARROW` shared-memory segment (`shm_name`) and bulk-load all batches into `table`.
    pub async fn load_from_arrow_shm(
        &self,
        table: String,
        shm_name: String,
        columns: Option<Vec<String>>,
    ) -> Result<u64, MssqlError> {
        let batches = decode_shm_arrow_batches(&shm_name)?;
        self.load_from_arrow_batches(table, batches, columns).await
    }
}

#[allow(dead_code)]
const fn _assert_client_send_sync() {
    const fn check<T: Send + Sync>() {}
    check::<MssqlClient>();
    check::<MssqlErrorKind>();
}
