//! Dedicated OS worker thread pool for SQLite with 1 writer and N readers.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::Mutex;
use rusqlite::backup::Backup;
use rusqlite::{Connection, InterruptHandle, OpenFlags};

use crate::arrow_bridge::{ArrowResultPayload, execute_load_from_arrow, execute_select_to_arrow};
use crate::config::SqliteClientConfig;
use crate::errors::SqliteClientError;
use crate::value::{BoundParams, PipelineResultItem, QueryResult, SqliteValue};

/// Operation request dispatched to a dedicated SQLite worker thread.
enum WorkerTask {
    Execute {
        sql: String,
        params: BoundParams,
        op_id: u64,
        resp: Sender<Result<usize, SqliteClientError>>,
    },
    ExecuteMany {
        sql: String,
        params_seq: Vec<BoundParams>,
        op_id: u64,
        resp: Sender<Result<usize, SqliteClientError>>,
    },
    ExecuteScript {
        sql_script: String,
        op_id: u64,
        resp: Sender<Result<(), SqliteClientError>>,
    },
    FetchAll {
        sql: String,
        params: BoundParams,
        op_id: u64,
        resp: Sender<Result<QueryResult, SqliteClientError>>,
    },
    ExecutePipeline {
        operations: Vec<(bool, String, BoundParams)>,
        use_savepoint: bool,
        op_id: u64,
        resp: Sender<Result<Vec<PipelineResultItem>, SqliteClientError>>,
    },
    SelectToArrow {
        sql: String,
        params: BoundParams,
        batch_size: usize,
        op_id: u64,
        resp: Sender<Result<ArrowResultPayload, SqliteClientError>>,
    },
    LoadFromArrow {
        table_name: String,
        arrow_ipc_bytes: Vec<u8>,
        op_id: u64,
        resp: Sender<Result<usize, SqliteClientError>>,
    },
    BackupTo {
        target_path: String,
        pages_per_step: i32,
        sleep_ms: u64,
        op_id: u64,
        resp: Sender<Result<(), SqliteClientError>>,
    },
    RestoreFrom {
        source_path: String,
        pages_per_step: i32,
        sleep_ms: u64,
        op_id: u64,
        resp: Sender<Result<(), SqliteClientError>>,
    },
    Begin {
        mode: String,
        op_id: u64,
        resp: Sender<Result<(), SqliteClientError>>,
    },
    Commit {
        op_id: u64,
        resp: Sender<Result<(), SqliteClientError>>,
    },
    Rollback {
        op_id: u64,
        resp: Sender<Result<(), SqliteClientError>>,
    },
    Close,
}

/// Handle to an active worker thread.
struct WorkerSlot {
    sender: Sender<WorkerTask>,
    #[allow(dead_code)]
    join_handle: Option<JoinHandle<()>>,
}

impl WorkerSlot {
    fn spawn(
        config: SqliteClientConfig,
        is_writer: bool,
        active_interrupts: Arc<Mutex<HashMap<u64, Arc<InterruptHandle>>>>,
        last_rowid: Option<Arc<AtomicI64>>,
    ) -> Result<Self, SqliteClientError> {
        let (sender, receiver) = mpsc::channel();
        let (init_tx, init_rx) = mpsc::channel();

        let join_handle = thread::Builder::new()
            .name(if is_writer {
                "sqlite-writer".to_string()
            } else {
                "sqlite-reader".to_string()
            })
            .spawn(move || {
                let conn_res = open_worker_connection(&config, is_writer);
                let mut conn = match conn_res {
                    Ok(c) => {
                        let _ = init_tx.send(Ok(()));
                        c
                    }
                    Err(e) => {
                        let _ = init_tx.send(Err(e));
                        return;
                    }
                };

                let interrupt_handle = Arc::new(conn.get_interrupt_handle());
                run_worker_loop(
                    &mut conn,
                    interrupt_handle,
                    receiver,
                    active_interrupts,
                    last_rowid,
                );
            })
            .map_err(|e| SqliteClientError::Operational {
                message: format!("failed to spawn SQLite worker thread: {e}"),
                code: None,
                sqlstate: Some("08000"),
                is_transient: false,
            })?;

        match init_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                sender,
                join_handle: Some(join_handle),
            }),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(SqliteClientError::Operational {
                message: "SQLite worker thread exited during initialization".to_string(),
                code: None,
                sqlstate: Some("08000"),
                is_transient: false,
            }),
        }
    }
}

fn open_worker_connection(
    config: &SqliteClientConfig,
    is_writer: bool,
) -> Result<Connection, SqliteClientError> {
    let mut flags = if is_writer {
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
    } else {
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
    };

    if config.is_uri() {
        flags |= OpenFlags::SQLITE_OPEN_URI;
    }

    let conn = Connection::open_with_flags(&config.path, flags)
        .map_err(SqliteClientError::from_rusqlite)?;

    // Apply initialization pragmas
    for pragma in config.initialization_pragmas() {
        conn.execute_batch(&pragma)
            .map_err(SqliteClientError::from_rusqlite)?;
    }

    Ok(conn)
}

fn run_worker_loop(
    conn: &mut Connection,
    interrupt_handle: Arc<InterruptHandle>,
    receiver: Receiver<WorkerTask>,
    active_interrupts: Arc<Mutex<HashMap<u64, Arc<InterruptHandle>>>>,
    last_rowid: Option<Arc<AtomicI64>>,
) {
    while let Ok(task) = receiver.recv() {
        match task {
            WorkerTask::Execute {
                sql,
                params,
                op_id,
                resp,
            } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = execute_statement(conn, &sql, &params);
                if result.is_ok()
                    && let Some(ref lr) = last_rowid
                {
                    lr.store(conn.last_insert_rowid(), Ordering::Relaxed);
                }
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::ExecuteMany {
                sql,
                params_seq,
                op_id,
                resp,
            } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = execute_many_statement(conn, &sql, params_seq);
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::ExecuteScript {
                sql_script,
                op_id,
                resp,
            } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = conn
                    .execute_batch(&sql_script)
                    .map_err(SqliteClientError::from_rusqlite);
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::FetchAll {
                sql,
                params,
                op_id,
                resp,
            } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = fetch_all_query(conn, &sql, &params);
                if result.is_ok()
                    && let Some(ref lr) = last_rowid
                {
                    let id = conn.last_insert_rowid();
                    if id > 0 {
                        lr.store(id, Ordering::Relaxed);
                    }
                }
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::ExecutePipeline {
                operations,
                use_savepoint,
                op_id,
                resp,
            } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = execute_pipeline_internal(conn, operations, use_savepoint, op_id);
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::SelectToArrow {
                sql,
                params,
                batch_size,
                op_id,
                resp,
            } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = execute_select_to_arrow(conn, &sql, &params, batch_size);
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::LoadFromArrow {
                table_name,
                arrow_ipc_bytes,
                op_id,
                resp,
            } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = execute_load_from_arrow(conn, &table_name, &arrow_ipc_bytes);
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::BackupTo {
                target_path,
                pages_per_step,
                sleep_ms,
                op_id,
                resp,
            } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = execute_backup_to(conn, &target_path, pages_per_step, sleep_ms);
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::RestoreFrom {
                source_path,
                pages_per_step,
                sleep_ms,
                op_id,
                resp,
            } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = execute_restore_from(conn, &source_path, pages_per_step, sleep_ms);
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::Begin { mode, op_id, resp } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = conn
                    .execute_batch(&format!("BEGIN {mode}"))
                    .map_err(SqliteClientError::from_rusqlite);
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::Commit { op_id, resp } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = conn
                    .execute_batch("COMMIT")
                    .map_err(SqliteClientError::from_rusqlite);
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::Rollback { op_id, resp } => {
                active_interrupts
                    .lock()
                    .insert(op_id, Arc::clone(&interrupt_handle));
                let result = conn
                    .execute_batch("ROLLBACK")
                    .map_err(SqliteClientError::from_rusqlite);
                active_interrupts.lock().remove(&op_id);
                let _ = resp.send(result);
            }
            WorkerTask::Close => break,
        }
    }
}

fn execute_statement(
    conn: &mut Connection,
    sql: &str,
    params: &BoundParams,
) -> Result<usize, SqliteClientError> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(SqliteClientError::from_rusqlite)?;
    params.bind_to_stmt(&mut stmt)?;

    // Step to completion (especially critical for statements with RETURNING)
    let mut rows = stmt.raw_query();
    while rows
        .next()
        .map_err(SqliteClientError::from_rusqlite)?
        .is_some()
    {}
    drop(rows);

    Ok(conn.changes() as usize)
}

fn execute_many_statement(
    conn: &mut Connection,
    sql: &str,
    params_seq: Vec<BoundParams>,
) -> Result<usize, SqliteClientError> {
    let is_autocommit = conn.is_autocommit();
    if is_autocommit {
        conn.execute_batch("BEGIN IMMEDIATE")
            .map_err(SqliteClientError::from_rusqlite)?;
    }

    let mut total_changes = 0;
    let mut exec_result = Ok(());

    {
        let mut stmt = match conn.prepare(sql).map_err(SqliteClientError::from_rusqlite) {
            Ok(s) => s,
            Err(e) => {
                if is_autocommit {
                    let _ = conn.execute_batch("ROLLBACK");
                }
                return Err(e);
            }
        };

        for params in params_seq {
            if let Err(e) = params.bind_to_stmt(&mut stmt) {
                exec_result = Err(e);
                break;
            }
            let mut rows = stmt.raw_query();
            let mut step_err = None;
            while let Ok(Some(_)) = rows.next() {}
            if let Err(e) = rows.next() {
                step_err = Some(SqliteClientError::from_rusqlite(e));
            }
            drop(rows);
            if let Some(e) = step_err {
                exec_result = Err(e);
                break;
            }
            total_changes += conn.changes() as usize;
        }
    }

    match exec_result {
        Ok(()) => {
            if is_autocommit {
                conn.execute_batch("COMMIT")
                    .map_err(SqliteClientError::from_rusqlite)?;
            }
            Ok(total_changes)
        }
        Err(e) => {
            if is_autocommit {
                let _ = conn.execute_batch("ROLLBACK");
            }
            Err(e)
        }
    }
}

fn fetch_all_query(
    conn: &mut Connection,
    sql: &str,
    params: &BoundParams,
) -> Result<QueryResult, SqliteClientError> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(SqliteClientError::from_rusqlite)?;
    params.bind_to_stmt(&mut stmt)?;

    let columns: Vec<String> = stmt.column_names().map(ToString::to_string).collect();
    let col_count = columns.len();
    let mut rows_out = Vec::new();

    let mut rows = stmt.raw_query();
    while let Some(row) = rows.next().map_err(SqliteClientError::from_rusqlite)? {
        let mut row_values = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val_ref = row.get_ref(i).map_err(SqliteClientError::from_rusqlite)?;
            row_values.push(SqliteValue::from(val_ref));
        }
        rows_out.push(row_values);
    }

    Ok(QueryResult {
        columns,
        rows: rows_out,
    })
}

fn execute_pipeline_internal(
    conn: &mut Connection,
    operations: Vec<(bool, String, BoundParams)>,
    use_savepoint: bool,
    op_id: u64,
) -> Result<Vec<PipelineResultItem>, SqliteClientError> {
    let savepoint_name = format!("vtb_pipe_{op_id}");
    let entered_tx = if use_savepoint {
        conn.execute_batch(&format!("SAVEPOINT {savepoint_name}"))
            .map_err(SqliteClientError::from_rusqlite)?;
        true
    } else if conn.is_autocommit() {
        conn.execute_batch("BEGIN IMMEDIATE")
            .map_err(SqliteClientError::from_rusqlite)?;
        true
    } else {
        false
    };

    let mut results = Vec::with_capacity(operations.len());
    let mut pipeline_err = None;

    for (is_query, sql, params) in operations {
        if is_query {
            match fetch_all_query(conn, &sql, &params) {
                Ok(qr) => results.push(PipelineResultItem::Query(qr)),
                Err(e) => {
                    pipeline_err = Some(e);
                    break;
                }
            }
        } else {
            match execute_statement(conn, &sql, &params) {
                Ok(n) => results.push(PipelineResultItem::RowCount(n)),
                Err(e) => {
                    pipeline_err = Some(e);
                    break;
                }
            }
        }
    }

    match pipeline_err {
        None => {
            if use_savepoint {
                conn.execute_batch(&format!("RELEASE {savepoint_name}"))
                    .map_err(SqliteClientError::from_rusqlite)?;
            } else if entered_tx {
                conn.execute_batch("COMMIT")
                    .map_err(SqliteClientError::from_rusqlite)?;
            }
            Ok(results)
        }
        Some(err) => {
            if use_savepoint {
                let _ = conn.execute_batch(&format!(
                    "ROLLBACK TO {savepoint_name}; RELEASE {savepoint_name};"
                ));
            } else if entered_tx {
                let _ = conn.execute_batch("ROLLBACK");
            }
            Err(err)
        }
    }
}

fn execute_backup_to(
    conn: &mut Connection,
    target_path: &str,
    pages_per_step: i32,
    sleep_ms: u64,
) -> Result<(), SqliteClientError> {
    let mut target_conn = Connection::open_with_flags(
        target_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(SqliteClientError::from_rusqlite)?;

    let backup = Backup::new(conn, &mut target_conn).map_err(SqliteClientError::from_rusqlite)?;
    let sleep_dur = Duration::from_millis(sleep_ms);
    backup
        .run_to_completion(pages_per_step, sleep_dur, None)
        .map_err(SqliteClientError::from_rusqlite)
}

fn execute_restore_from(
    conn: &mut Connection,
    source_path: &str,
    pages_per_step: i32,
    sleep_ms: u64,
) -> Result<(), SqliteClientError> {
    let source_conn = Connection::open_with_flags(
        source_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(SqliteClientError::from_rusqlite)?;

    let backup = Backup::new(&source_conn, conn).map_err(SqliteClientError::from_rusqlite)?;
    let sleep_dur = Duration::from_millis(sleep_ms);
    backup
        .run_to_completion(pages_per_step, sleep_dur, None)
        .map_err(SqliteClientError::from_rusqlite)
}

fn is_read_only_sql(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    (upper.starts_with("SELECT") || upper.starts_with("EXPLAIN") || upper.starts_with("WITH"))
        && !upper.contains("INSERT")
        && !upper.contains("UPDATE")
        && !upper.contains("DELETE")
        && !upper.contains("DROP")
        && !upper.contains("ALTER")
        && !upper.contains("CREATE")
}

/// SQLite thread pool managing a serialized writer and optional WAL readers.
pub struct SqlitePool {
    config: SqliteClientConfig,
    writer: WorkerSlot,
    readers: Vec<WorkerSlot>,
    in_transaction: AtomicBool,
    closed: AtomicBool,
    reader_cursor: AtomicUsize,
    active_interrupts: Arc<Mutex<HashMap<u64, Arc<InterruptHandle>>>>,
    next_op_id: AtomicU64,
    last_rowid: Arc<AtomicI64>,
}

impl SqlitePool {
    /// Initialize a new SQLite worker pool.
    pub fn open(config: SqliteClientConfig) -> Result<Arc<Self>, SqliteClientError> {
        config.validate()?;
        let active_interrupts = Arc::new(Mutex::new(HashMap::new()));
        let last_rowid = Arc::new(AtomicI64::new(0));

        let writer = WorkerSlot::spawn(
            config.clone(),
            true,
            Arc::clone(&active_interrupts),
            Some(Arc::clone(&last_rowid)),
        )?;

        let mut readers = Vec::new();
        // If not plain in-memory, spawn readers
        if !config.is_plain_in_memory() && config.max_readers > 0 {
            for _ in 0..config.max_readers {
                let slot =
                    WorkerSlot::spawn(config.clone(), false, Arc::clone(&active_interrupts), None)?;
                readers.push(slot);
            }
        }

        Ok(Arc::new(Self {
            config,
            writer,
            readers,
            in_transaction: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            reader_cursor: AtomicUsize::new(0),
            active_interrupts,
            next_op_id: AtomicU64::new(1),
            last_rowid,
        }))
    }

    /// Return the most recent inserted rowid on the writer connection.
    #[must_use]
    pub fn last_insert_rowid(&self) -> i64 {
        self.last_rowid.load(Ordering::Relaxed)
    }

    /// Return the resolved database path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.config.path
    }

    /// Return whether an explicit transaction is active.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.in_transaction.load(Ordering::Acquire)
    }

    /// Return whether the pool has been closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn ensure_open(&self) -> Result<(), SqliteClientError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteClientError::Operational {
                message: "SQLite connection pool is closed".to_string(),
                code: None,
                sqlstate: Some("08000"),
                is_transient: false,
            });
        }
        Ok(())
    }

    /// Allocate the next unique operation ID.
    pub fn next_op_id(&self) -> u64 {
        self.next_op_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Route to a reader worker or fallback to writer.
    fn dispatch_read(
        &self,
        task_maker: impl FnOnce(Sender<WorkerTask>) -> Result<(), SqliteClientError>,
    ) -> Result<(), SqliteClientError> {
        self.ensure_open()?;
        if !self.in_transaction.load(Ordering::Acquire) && !self.readers.is_empty() {
            let idx = self.reader_cursor.fetch_add(1, Ordering::Relaxed) % self.readers.len();
            let sender = self.readers[idx].sender.clone();
            task_maker(sender)
        } else {
            let sender = self.writer.sender.clone();
            task_maker(sender)
        }
    }

    /// Route unconditionally to the single writer worker.
    fn dispatch_write(
        &self,
        task_maker: impl FnOnce(Sender<WorkerTask>) -> Result<(), SqliteClientError>,
    ) -> Result<(), SqliteClientError> {
        self.ensure_open()?;
        let sender = self.writer.sender.clone();
        task_maker(sender)
    }

    /// Execute a DDL or DML statement.
    pub fn execute(&self, sql: String, params: BoundParams) -> Result<usize, SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        self.dispatch_write(|sender| {
            sender
                .send(WorkerTask::Execute {
                    sql,
                    params,
                    op_id,
                    resp: resp_tx,
                })
                .map_err(|_| SqliteClientError::Operational {
                    message: "worker thread disconnected".to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })
        })?;
        resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?
    }

    /// Execute multiple parameter sets.
    pub fn execute_many(
        &self,
        sql: String,
        params_seq: Vec<BoundParams>,
    ) -> Result<usize, SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        self.dispatch_write(|sender| {
            sender
                .send(WorkerTask::ExecuteMany {
                    sql,
                    params_seq,
                    op_id,
                    resp: resp_tx,
                })
                .map_err(|_| SqliteClientError::Operational {
                    message: "worker thread disconnected".to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })
        })?;
        resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?
    }

    /// Execute a multi-statement SQL script.
    pub fn execute_script(&self, sql_script: String) -> Result<(), SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        self.dispatch_write(|sender| {
            sender
                .send(WorkerTask::ExecuteScript {
                    sql_script,
                    op_id,
                    resp: resp_tx,
                })
                .map_err(|_| SqliteClientError::Operational {
                    message: "worker thread disconnected".to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })
        })?;
        resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?
    }

    /// Fetch all rows of a query.
    pub fn fetch_all(
        &self,
        sql: String,
        params: BoundParams,
    ) -> Result<QueryResult, SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        let is_ro = is_read_only_sql(&sql);
        let task = WorkerTask::FetchAll {
            sql,
            params,
            op_id,
            resp: resp_tx,
        };

        if is_ro {
            self.dispatch_read(|sender| {
                sender
                    .send(task)
                    .map_err(|_| SqliteClientError::Operational {
                        message: "worker thread disconnected".to_string(),
                        code: None,
                        sqlstate: Some("08000"),
                        is_transient: false,
                    })
            })?;
        } else {
            self.dispatch_write(|sender| {
                sender
                    .send(task)
                    .map_err(|_| SqliteClientError::Operational {
                        message: "worker thread disconnected".to_string(),
                        code: None,
                        sqlstate: Some("08000"),
                        is_transient: false,
                    })
            })?;
        }

        resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?
    }

    /// Execute atomic batch pipeline.
    pub fn execute_pipeline(
        &self,
        operations: Vec<(bool, String, BoundParams)>,
        use_savepoint: bool,
    ) -> Result<Vec<PipelineResultItem>, SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        self.dispatch_write(|sender| {
            sender
                .send(WorkerTask::ExecutePipeline {
                    operations,
                    use_savepoint,
                    op_id,
                    resp: resp_tx,
                })
                .map_err(|_| SqliteClientError::Operational {
                    message: "worker thread disconnected".to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })
        })?;
        resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?
    }

    /// Select to Arrow shared memory.
    pub fn select_to_arrow(
        &self,
        sql: String,
        params: BoundParams,
        batch_size: usize,
    ) -> Result<ArrowResultPayload, SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        let is_ro = is_read_only_sql(&sql);
        let task = WorkerTask::SelectToArrow {
            sql,
            params,
            batch_size,
            op_id,
            resp: resp_tx,
        };

        if is_ro {
            self.dispatch_read(|sender| {
                sender
                    .send(task)
                    .map_err(|_| SqliteClientError::Operational {
                        message: "worker thread disconnected".to_string(),
                        code: None,
                        sqlstate: Some("08000"),
                        is_transient: false,
                    })
            })?;
        } else {
            self.dispatch_write(|sender| {
                sender
                    .send(task)
                    .map_err(|_| SqliteClientError::Operational {
                        message: "worker thread disconnected".to_string(),
                        code: None,
                        sqlstate: Some("08000"),
                        is_transient: false,
                    })
            })?;
        }

        resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?
    }

    /// Bulk ingest Arrow IPC stream bytes.
    pub fn load_from_arrow(
        &self,
        table_name: String,
        arrow_ipc_bytes: Vec<u8>,
    ) -> Result<usize, SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        self.dispatch_write(|sender| {
            sender
                .send(WorkerTask::LoadFromArrow {
                    table_name,
                    arrow_ipc_bytes,
                    op_id,
                    resp: resp_tx,
                })
                .map_err(|_| SqliteClientError::Operational {
                    message: "worker thread disconnected".to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })
        })?;
        resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?
    }

    /// Online backup to destination path.
    pub fn backup_to(
        &self,
        target_path: String,
        pages_per_step: i32,
        sleep_ms: u64,
    ) -> Result<(), SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        self.dispatch_write(|sender| {
            sender
                .send(WorkerTask::BackupTo {
                    target_path,
                    pages_per_step,
                    sleep_ms,
                    op_id,
                    resp: resp_tx,
                })
                .map_err(|_| SqliteClientError::Operational {
                    message: "worker thread disconnected".to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })
        })?;
        resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?
    }

    /// Online restore from source path.
    pub fn restore_from(
        &self,
        source_path: String,
        pages_per_step: i32,
        sleep_ms: u64,
    ) -> Result<(), SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        self.dispatch_write(|sender| {
            sender
                .send(WorkerTask::RestoreFrom {
                    source_path,
                    pages_per_step,
                    sleep_ms,
                    op_id,
                    resp: resp_tx,
                })
                .map_err(|_| SqliteClientError::Operational {
                    message: "worker thread disconnected".to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })
        })?;
        resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?
    }

    /// Begin an explicit transaction.
    pub fn begin(&self, mode: String) -> Result<(), SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        self.dispatch_write(|sender| {
            sender
                .send(WorkerTask::Begin {
                    mode,
                    op_id,
                    resp: resp_tx,
                })
                .map_err(|_| SqliteClientError::Operational {
                    message: "worker thread disconnected".to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })
        })?;
        let res = resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?;
        if res.is_ok() {
            self.in_transaction.store(true, Ordering::Release);
        }
        res
    }

    /// Commit the active transaction.
    pub fn commit(&self) -> Result<(), SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        self.dispatch_write(|sender| {
            sender
                .send(WorkerTask::Commit {
                    op_id,
                    resp: resp_tx,
                })
                .map_err(|_| SqliteClientError::Operational {
                    message: "worker thread disconnected".to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })
        })?;
        let res = resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?;
        self.in_transaction.store(false, Ordering::Release);
        res
    }

    /// Roll back the active transaction.
    pub fn rollback(&self) -> Result<(), SqliteClientError> {
        let op_id = self.next_op_id();
        let (resp_tx, resp_rx) = mpsc::channel();
        self.dispatch_write(|sender| {
            sender
                .send(WorkerTask::Rollback {
                    op_id,
                    resp: resp_tx,
                })
                .map_err(|_| SqliteClientError::Operational {
                    message: "worker thread disconnected".to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })
        })?;
        let res = resp_rx.recv().map_err(|_| SqliteClientError::Operational {
            message: "worker response dropped".to_string(),
            code: None,
            sqlstate: Some("08000"),
            is_transient: false,
        })?;
        self.in_transaction.store(false, Ordering::Release);
        res
    }

    /// Cancel all in-flight operations by triggering `InterruptHandle::interrupt()`.
    pub fn cancel(&self) {
        let map = self.active_interrupts.lock();
        for handle in map.values() {
            handle.interrupt();
        }
    }

    /// Cancel a specific operation by its `op_id`.
    pub fn cancel_operation(&self, op_id: u64) -> bool {
        let map = self.active_interrupts.lock();
        if let Some(handle) = map.get(&op_id) {
            handle.interrupt();
            true
        } else {
            false
        }
    }

    /// Shut down the worker pool.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.cancel();
        let _ = self.writer.sender.send(WorkerTask::Close);
        for reader in &self.readers {
            let _ = reader.sender.send(WorkerTask::Close);
        }
    }
}

impl Drop for SqlitePool {
    fn drop(&mut self) {
        self.close();
    }
}
