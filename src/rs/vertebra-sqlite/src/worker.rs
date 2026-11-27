//! SQLite connection pool with 1 writer and N readers, executed on the calling thread.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use rusqlite::backup::Backup;
use rusqlite::{Connection, InterruptHandle, OpenFlags};

use crate::arrow_bridge::{ArrowResultPayload, execute_load_from_arrow, execute_select_to_arrow};
use crate::config::SqliteClientConfig;
use crate::errors::SqliteClientError;
use crate::value::{BoundParams, PipelineResultItem, QueryResult, SqliteValue, StatementOutcome};

const MAX_CANCELLED_OP_IDS: usize = 4096;
const STATEMENT_CACHE_CAPACITY: usize = 256;

struct SlotInterrupt {
    handle: Arc<InterruptHandle>,
    running_op: Arc<AtomicU64>,
}

#[derive(Default)]
struct InterruptState {
    slots: Vec<SlotInterrupt>,
    cancelled: HashSet<u64>,
    cancelled_order: VecDeque<u64>,
}

impl InterruptState {
    fn register_slot(&mut self, handle: &Arc<InterruptHandle>) -> Arc<AtomicU64> {
        let running_op = Arc::new(AtomicU64::new(0));
        self.slots.push(SlotInterrupt {
            handle: Arc::clone(handle),
            running_op: Arc::clone(&running_op),
        });
        running_op
    }

    fn take_if_cancelled(&mut self, op_id: u64) -> bool {
        self.cancelled.remove(&op_id)
    }

    fn begin_op(&mut self, op_id: u64, running_op: &AtomicU64) -> bool {
        if self.cancelled.remove(&op_id) {
            return false;
        }
        running_op.store(op_id, Ordering::Release);
        true
    }

    fn cancel_op(&mut self, op_id: u64) -> bool {
        if op_id != 0 {
            for slot in &self.slots {
                if slot.running_op.load(Ordering::Acquire) == op_id {
                    slot.handle.interrupt();
                    return true;
                }
            }
        }
        if self.cancelled.insert(op_id) {
            if self.cancelled_order.len() >= MAX_CANCELLED_OP_IDS
                && let Some(oldest) = self.cancelled_order.pop_front()
            {
                self.cancelled.remove(&oldest);
            }
            self.cancelled_order.push_back(op_id);
        }
        true
    }

    fn cancel_all(&self) {
        for slot in &self.slots {
            if slot.running_op.load(Ordering::Acquire) != 0 {
                slot.handle.interrupt();
            }
        }
    }
}

fn normalize_begin_sql(mode: &str) -> Result<&'static str, SqliteClientError> {
    let trimmed = mode.trim();
    if trimmed.is_empty() {
        return Ok("BEGIN");
    }
    match trimmed.to_ascii_uppercase().as_str() {
        "DEFERRED" => Ok("BEGIN DEFERRED"),
        "IMMEDIATE" => Ok("BEGIN IMMEDIATE"),
        "EXCLUSIVE" => Ok("BEGIN EXCLUSIVE"),
        _ => Err(SqliteClientError::Programming {
            message: format!(
                "invalid BEGIN transaction mode `{mode}`; expected '', 'DEFERRED', 'IMMEDIATE', or 'EXCLUSIVE'"
            ),
            code: None,
            sqlstate: Some("42000"),
        }),
    }
}

/// One pooled SQLite connection.
struct WorkerSlot {
    conn: Mutex<Option<Connection>>,
    running_op: Arc<AtomicU64>,
    interrupts: Arc<Mutex<InterruptState>>,
    last_rowid: Option<Arc<AtomicI64>>,
    in_transaction: Option<Arc<AtomicBool>>,
}

impl WorkerSlot {
    fn spawn(
        config: SqliteClientConfig,
        is_writer: bool,
        interrupts: Arc<Mutex<InterruptState>>,
        last_rowid: Option<Arc<AtomicI64>>,
        in_transaction: Option<Arc<AtomicBool>>,
    ) -> Result<Self, SqliteClientError> {
        let conn = open_worker_connection(&config, is_writer)?;
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        let interrupt_handle = Arc::new(conn.get_interrupt_handle());
        let running_op = interrupts.lock().register_slot(&interrupt_handle);
        Ok(Self {
            conn: Mutex::new(Some(conn)),
            running_op,
            interrupts,
            last_rowid,
            in_transaction,
        })
    }

    /// Execute `f` directly on the calling thread while holding this slot's connection lock.
    fn with_conn<T>(
        &self,
        op_id: u64,
        f: impl FnOnce(&mut Connection, Option<&Arc<AtomicI64>>) -> Result<T, SqliteClientError>,
    ) -> Result<T, SqliteClientError> {
        let mut guard = self.conn.lock();
        let Some(conn) = guard.as_mut() else {
            return Err(SqliteClientError::Operational {
                message: "SQLite connection is closed".to_string(),
                code: None,
                sqlstate: Some("08000"),
                is_transient: false,
            });
        };
        let res = run_flagged_op(&self.interrupts, op_id, &self.running_op, || {
            f(conn, self.last_rowid.as_ref())
        });
        if let Some(in_tx) = self.in_transaction.as_ref() {
            in_tx.store(!conn.is_autocommit(), Ordering::Release);
        }
        res
    }

    fn run_statement(
        &self,
        op_id: u64,
        sql: &str,
        params: &BoundParams,
        is_insert: impl FnOnce() -> bool,
    ) -> Result<StatementOutcome, SqliteClientError> {
        self.with_conn(op_id, |conn, last_rowid| {
            let res = run_statement(conn, sql, params, is_insert);
            if res.is_ok()
                && let Some(lr) = last_rowid
            {
                lr.store(conn.last_insert_rowid(), Ordering::Relaxed);
            }
            res
        })
    }

    fn signal_close(&self) {
        drop(self.conn.lock().take());
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

    conn.busy_timeout(Duration::from_millis(u64::from(config.busy_timeout_ms)))
        .map_err(SqliteClientError::from_rusqlite)?;

    let desired_mode = if config.is_plain_in_memory() {
        None
    } else if config.wal_mode {
        Some("wal")
    } else {
        Some("delete")
    };

    // Apply initialization pragmas, skipping redundant journal_mode writes when already set.
    for pragma in config.initialization_pragmas() {
        if let Some(target_mode) = desired_mode
            && pragma.starts_with("PRAGMA journal_mode = ")
        {
            let current: Result<String, _> =
                conn.query_row("PRAGMA journal_mode", [], |row| row.get(0));
            if current.is_ok_and(|mode| mode.eq_ignore_ascii_case(target_mode)) {
                continue;
            }
        }
        conn.execute_batch(&pragma)
            .map_err(SqliteClientError::from_rusqlite)?;
    }

    Ok(conn)
}

fn run_flagged_op<T>(
    interrupts: &Mutex<InterruptState>,
    op_id: u64,
    running_op: &AtomicU64,
    f: impl FnOnce() -> Result<T, SqliteClientError>,
) -> Result<T, SqliteClientError> {
    if !interrupts.lock().begin_op(op_id, running_op) {
        return Err(SqliteClientError::Cancelled(
            "operation cancelled".to_string(),
        ));
    }
    let res = f();
    running_op.store(0, Ordering::Release);
    res
}

fn execute_statement(
    conn: &mut Connection,
    sql: &str,
    params: &BoundParams,
) -> Result<usize, SqliteClientError> {
    let mut stmt = conn
        .prepare_cached(sql)
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
            loop {
                match rows.next() {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(e) => {
                        step_err = Some(SqliteClientError::from_rusqlite(e));
                        break;
                    }
                }
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
        .prepare_cached(sql)
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

fn run_statement(
    conn: &mut Connection,
    sql: &str,
    params: &BoundParams,
    is_insert: impl FnOnce() -> bool,
) -> Result<StatementOutcome, SqliteClientError> {
    let mut stmt = conn
        .prepare_cached(sql)
        .map_err(SqliteClientError::from_rusqlite)?;
    params.bind_to_stmt(&mut stmt)?;

    let col_count = stmt.column_count();
    let read_only = stmt.readonly();
    let before = conn.total_changes();
    let columns = (col_count > 0).then(|| {
        stmt.column_names()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    });
    let mut rows_out = Vec::new();

    let mut rows = stmt.raw_query();
    while let Some(row) = rows.next().map_err(SqliteClientError::from_rusqlite)? {
        if col_count == 0 {
            continue;
        }
        let mut row_values = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val_ref = row.get_ref(i).map_err(SqliteClientError::from_rusqlite)?;
            row_values.push(SqliteValue::from(val_ref));
        }
        rows_out.push(row_values);
    }
    drop(rows);

    let changed = conn.total_changes() != before;
    let rowcount = if read_only {
        -1
    } else if changed {
        conn.changes() as i64
    } else {
        0
    };
    let lastrowid = if changed && is_insert() {
        Some(conn.last_insert_rowid()).filter(|id| *id > 0)
    } else {
        None
    };

    Ok(StatementOutcome {
        columns,
        rows: rows_out,
        rowcount,
        lastrowid,
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

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_continue(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn is_write_keyword(token: &str) -> bool {
    token.eq_ignore_ascii_case("INSERT")
        || token.eq_ignore_ascii_case("UPDATE")
        || token.eq_ignore_ascii_case("DELETE")
        || token.eq_ignore_ascii_case("REPLACE")
        || token.eq_ignore_ascii_case("DROP")
        || token.eq_ignore_ascii_case("ALTER")
        || token.eq_ignore_ascii_case("CREATE")
        || token.eq_ignore_ascii_case("ATTACH")
        || token.eq_ignore_ascii_case("DETACH")
        || token.eq_ignore_ascii_case("VACUUM")
        || token.eq_ignore_ascii_case("REINDEX")
        || token.eq_ignore_ascii_case("PRAGMA")
        || token.eq_ignore_ascii_case("BEGIN")
        || token.eq_ignore_ascii_case("COMMIT")
        || token.eq_ignore_ascii_case("ROLLBACK")
        || token.eq_ignore_ascii_case("SAVEPOINT")
        || token.eq_ignore_ascii_case("RELEASE")
}

fn skips_lazy_begin_first_token(token: &str) -> bool {
    token.eq_ignore_ascii_case("PRAGMA")
        || token.eq_ignore_ascii_case("BEGIN")
        || token.eq_ignore_ascii_case("COMMIT")
        || token.eq_ignore_ascii_case("END")
        || token.eq_ignore_ascii_case("ROLLBACK")
        || token.eq_ignore_ascii_case("RELEASE")
        || token.eq_ignore_ascii_case("ATTACH")
        || token.eq_ignore_ascii_case("DETACH")
        || token.eq_ignore_ascii_case("VACUUM")
}

fn analyze_statement_facts(sql: &str) -> StatementFacts {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut first_token: Option<&str> = None;
    let mut starts_read = false;
    let mut saw_write = false;

    while i < len {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if b == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(len);
            continue;
        }
        if b == b'\'' || b == b'"' || b == b'`' {
            let quote = b;
            i += 1;
            while i < len {
                if bytes[i] == quote {
                    if i + 1 < len && bytes[i + 1] == quote {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if b == b'[' {
            i += 1;
            while i < len && bytes[i] != b']' {
                i += 1;
            }
            if i < len {
                i += 1;
            }
            continue;
        }
        if (b == b':' || b == b'@' || b == b'$') && i + 1 < len && is_ident_continue(bytes[i + 1]) {
            i += 2;
            while i < len && is_ident_continue(bytes[i]) {
                i += 1;
            }
            continue;
        }
        if is_ident_start(b) {
            let start = i;
            i += 1;
            while i < len && is_ident_continue(bytes[i]) {
                i += 1;
            }
            let token = &sql[start..i];
            if first_token.is_none() {
                first_token = Some(token);
                starts_read = token.eq_ignore_ascii_case("SELECT")
                    || token.eq_ignore_ascii_case("EXPLAIN")
                    || token.eq_ignore_ascii_case("VALUES")
                    || token.eq_ignore_ascii_case("WITH");
                if !starts_read {
                    saw_write = true;
                    break;
                }
            } else if is_write_keyword(token) {
                saw_write = true;
                break;
            }
            continue;
        }
        i += 1;
    }

    let Some(first) = first_token else {
        return StatementFacts {
            read_only: false,
            needs_lazy_begin: false,
            is_insert: false,
        };
    };
    let read_only = starts_read && !saw_write;
    let needs_lazy_begin = !read_only && !skips_lazy_begin_first_token(first);
    let is_insert = first.eq_ignore_ascii_case("INSERT");
    StatementFacts {
        read_only,
        needs_lazy_begin,
        is_insert,
    }
}

/// Facts about one SQL text that decide routing, lazy_begin, and rowid reporting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatementFacts {
    /// Whether the statement can run on a read-only reader connection.
    pub read_only: bool,
    /// Whether a `lazy_begin=True` cursor should open `BEGIN IMMEDIATE` before this statement.
    pub needs_lazy_begin: bool,
    /// Whether the statement is an `INSERT`.
    pub is_insert: bool,
}

/// SQLite thread pool managing a serialized writer and optional WAL readers.
pub struct SqlitePool {
    config: SqliteClientConfig,
    writer: WorkerSlot,
    readers: Vec<WorkerSlot>,
    in_transaction: Arc<AtomicBool>,
    closed: AtomicBool,
    reader_cursor: AtomicUsize,
    interrupts: Arc<Mutex<InterruptState>>,
    next_op_id: AtomicU64,
    last_rowid: Arc<AtomicI64>,
    statement_facts: RwLock<HashMap<Box<str>, StatementFacts>>,
}

impl SqlitePool {
    /// Initialize a new SQLite worker pool.
    pub fn open(config: SqliteClientConfig) -> Result<Arc<Self>, SqliteClientError> {
        config.validate()?;
        let interrupts = Arc::new(Mutex::new(InterruptState::default()));
        let last_rowid = Arc::new(AtomicI64::new(0));
        let in_transaction = Arc::new(AtomicBool::new(false));

        let writer = WorkerSlot::spawn(
            config.clone(),
            true,
            Arc::clone(&interrupts),
            Some(Arc::clone(&last_rowid)),
            Some(Arc::clone(&in_transaction)),
        )?;

        let mut readers: Vec<WorkerSlot> = Vec::new();
        // If not plain in-memory, spawn readers
        if !config.is_plain_in_memory() && config.max_readers > 0 {
            for _ in 0..config.max_readers {
                match WorkerSlot::spawn(config.clone(), false, Arc::clone(&interrupts), None, None)
                {
                    Ok(slot) => readers.push(slot),
                    Err(err) => {
                        writer.signal_close();
                        for r in &readers {
                            r.signal_close();
                        }
                        return Err(err);
                    }
                }
            }
        }

        Ok(Arc::new(Self {
            config,
            writer,
            readers,
            in_transaction,
            closed: AtomicBool::new(false),
            reader_cursor: AtomicUsize::new(0),
            interrupts,
            next_op_id: AtomicU64::new(1),
            last_rowid,
            statement_facts: RwLock::new(HashMap::new()),
        }))
    }

    /// Return the cached facts for `sql`, computing and caching them on a miss.
    pub fn statement_facts(&self, sql: &str) -> StatementFacts {
        if let Some(facts) = self.statement_facts.read().get(sql) {
            return *facts;
        }
        let facts = analyze_statement_facts(sql);
        let mut cache = self.statement_facts.write();
        if cache.len() == STATEMENT_CACHE_CAPACITY {
            cache.clear();
        }
        cache.insert(sql.into(), facts);
        facts
    }

    /// Return the number of SQL texts held in the statement facts cache.
    #[must_use]
    pub fn statement_facts_len(&self) -> usize {
        self.statement_facts.read().len()
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

    fn ensure_open(&self, op_id: u64) -> Result<(), SqliteClientError> {
        if self.closed.load(Ordering::Acquire) {
            self.interrupts.lock().take_if_cancelled(op_id);
            return Err(SqliteClientError::InvalidInput(
                "SQLite connection pool is closed".to_string(),
            ));
        }
        Ok(())
    }

    /// Allocate the next unique operation ID.
    pub fn next_op_id(&self) -> u64 {
        self.next_op_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Route to a reader worker or fallback to writer.
    fn dispatch_read<T>(
        &self,
        op_id: u64,
        task_maker: impl FnOnce(&WorkerSlot) -> Result<T, SqliteClientError>,
    ) -> Result<T, SqliteClientError> {
        self.ensure_open(op_id)?;
        if !self.in_transaction.load(Ordering::Acquire) && !self.readers.is_empty() {
            let idx = self.reader_cursor.fetch_add(1, Ordering::Relaxed) % self.readers.len();
            task_maker(&self.readers[idx])
        } else {
            task_maker(&self.writer)
        }
    }

    /// Route unconditionally to the single writer worker.
    fn dispatch_write<T>(
        &self,
        op_id: u64,
        task_maker: impl FnOnce(&WorkerSlot) -> Result<T, SqliteClientError>,
    ) -> Result<T, SqliteClientError> {
        self.ensure_open(op_id)?;
        task_maker(&self.writer)
    }

    /// Execute a DDL or DML statement.
    pub fn execute(&self, sql: String, params: BoundParams) -> Result<usize, SqliteClientError> {
        self.execute_with_op_id(self.next_op_id(), sql, params)
    }

    /// Execute a DDL or DML statement using a caller-allocated `op_id`.
    pub fn execute_with_op_id(
        &self,
        op_id: u64,
        sql: String,
        params: BoundParams,
    ) -> Result<usize, SqliteClientError> {
        self.dispatch_write(op_id, |slot| {
            slot.with_conn(op_id, |conn, last_rowid| {
                let res = execute_statement(conn, &sql, &params);
                if res.is_ok()
                    && let Some(lr) = last_rowid
                {
                    lr.store(conn.last_insert_rowid(), Ordering::Relaxed);
                }
                res
            })
        })
    }

    /// Execute multiple parameter sets.
    pub fn execute_many(
        &self,
        sql: String,
        params_seq: Vec<BoundParams>,
    ) -> Result<usize, SqliteClientError> {
        self.execute_many_with_op_id(self.next_op_id(), sql, params_seq)
    }

    /// Execute multiple parameter sets using a caller-allocated `op_id`.
    pub fn execute_many_with_op_id(
        &self,
        op_id: u64,
        sql: String,
        params_seq: Vec<BoundParams>,
    ) -> Result<usize, SqliteClientError> {
        self.dispatch_write(op_id, |slot| {
            slot.with_conn(op_id, |conn, _| {
                execute_many_statement(conn, &sql, params_seq)
            })
        })
    }

    /// Execute a multi-statement SQL script.
    pub fn execute_script(&self, sql_script: String) -> Result<(), SqliteClientError> {
        self.execute_script_with_op_id(self.next_op_id(), sql_script)
    }

    /// Execute a multi-statement SQL script using a caller-allocated `op_id`.
    pub fn execute_script_with_op_id(
        &self,
        op_id: u64,
        sql_script: String,
    ) -> Result<(), SqliteClientError> {
        self.dispatch_write(op_id, |slot| {
            slot.with_conn(op_id, |conn, _| {
                conn.execute_batch(&sql_script)
                    .map_err(SqliteClientError::from_rusqlite)
            })
        })
    }

    /// Fetch all rows of a query.
    pub fn fetch_all(
        &self,
        sql: String,
        params: BoundParams,
    ) -> Result<QueryResult, SqliteClientError> {
        self.fetch_all_with_op_id(self.next_op_id(), sql, params)
    }

    /// Fetch all rows of a query using a caller-allocated `op_id`.
    pub fn fetch_all_with_op_id(
        &self,
        op_id: u64,
        sql: String,
        params: BoundParams,
    ) -> Result<QueryResult, SqliteClientError> {
        let is_ro = self.statement_facts(&sql).read_only;
        let run = |slot: &WorkerSlot| {
            slot.with_conn(op_id, |conn, last_rowid| {
                let res = fetch_all_query(conn, &sql, &params);
                if res.is_ok()
                    && let Some(lr) = last_rowid
                {
                    let id = conn.last_insert_rowid();
                    if id > 0 {
                        lr.store(id, Ordering::Relaxed);
                    }
                }
                res
            })
        };
        if is_ro {
            self.dispatch_read(op_id, run)
        } else {
            self.dispatch_write(op_id, run)
        }
    }

    /// Run any statement and report its result set, changed-row count, and inserted rowid.
    pub fn run(
        &self,
        sql: &str,
        params: BoundParams,
    ) -> Result<StatementOutcome, SqliteClientError> {
        self.run_with_op_id(self.next_op_id(), sql, params)
    }

    /// Run any statement using a caller-allocated `op_id`.
    pub fn run_with_op_id(
        &self,
        op_id: u64,
        sql: &str,
        params: BoundParams,
    ) -> Result<StatementOutcome, SqliteClientError> {
        self.ensure_open(op_id)?;
        let reader_eligible =
            !self.readers.is_empty() && !self.in_transaction.load(Ordering::Acquire);
        let known = reader_eligible.then(|| self.statement_facts(sql));
        let slot = if known.is_some_and(|facts| facts.read_only) {
            let idx = self.reader_cursor.fetch_add(1, Ordering::Relaxed) % self.readers.len();
            &self.readers[idx]
        } else {
            &self.writer
        };
        slot.run_statement(op_id, sql, &params, || {
            known.map_or_else(
                || self.statement_facts(sql).is_insert,
                |facts| facts.is_insert,
            )
        })
    }

    /// Run one statement once per parameter set and report the total changed-row count.
    pub fn run_many(
        &self,
        sql: String,
        params_seq: Vec<BoundParams>,
    ) -> Result<StatementOutcome, SqliteClientError> {
        self.run_many_with_op_id(self.next_op_id(), sql, params_seq)
    }

    /// Run one statement once per parameter set using a caller-allocated `op_id`.
    pub fn run_many_with_op_id(
        &self,
        op_id: u64,
        sql: String,
        params_seq: Vec<BoundParams>,
    ) -> Result<StatementOutcome, SqliteClientError> {
        let count = if params_seq.is_empty() {
            0
        } else {
            self.execute_many_with_op_id(op_id, sql, params_seq)?
        };
        Ok(StatementOutcome {
            columns: None,
            rows: Vec::new(),
            rowcount: count as i64,
            lastrowid: None,
        })
    }

    /// Execute atomic batch pipeline.
    pub fn execute_pipeline(
        &self,
        operations: Vec<(bool, String, BoundParams)>,
        use_savepoint: bool,
    ) -> Result<Vec<PipelineResultItem>, SqliteClientError> {
        self.execute_pipeline_with_op_id(self.next_op_id(), operations, use_savepoint)
    }

    /// Execute atomic batch pipeline using a caller-allocated `op_id`.
    pub fn execute_pipeline_with_op_id(
        &self,
        op_id: u64,
        operations: Vec<(bool, String, BoundParams)>,
        use_savepoint: bool,
    ) -> Result<Vec<PipelineResultItem>, SqliteClientError> {
        self.dispatch_write(op_id, |slot| {
            slot.with_conn(op_id, |conn, _| {
                execute_pipeline_internal(conn, operations, use_savepoint, op_id)
            })
        })
    }

    /// Select to Arrow shared memory.
    pub fn select_to_arrow(
        &self,
        sql: String,
        params: BoundParams,
        batch_size: usize,
    ) -> Result<ArrowResultPayload, SqliteClientError> {
        self.select_to_arrow_with_op_id(self.next_op_id(), sql, params, batch_size)
    }

    /// Select to Arrow shared memory using a caller-allocated `op_id`.
    pub fn select_to_arrow_with_op_id(
        &self,
        op_id: u64,
        sql: String,
        params: BoundParams,
        batch_size: usize,
    ) -> Result<ArrowResultPayload, SqliteClientError> {
        let is_ro = self.statement_facts(&sql).read_only;
        let run = |slot: &WorkerSlot| {
            slot.with_conn(op_id, |conn, _| {
                execute_select_to_arrow(conn, &sql, &params, batch_size)
            })
        };
        if is_ro {
            self.dispatch_read(op_id, run)
        } else {
            self.dispatch_write(op_id, run)
        }
    }

    /// Bulk ingest Arrow IPC stream bytes.
    pub fn load_from_arrow(
        &self,
        table_name: String,
        arrow_ipc_bytes: Vec<u8>,
    ) -> Result<usize, SqliteClientError> {
        self.load_from_arrow_with_op_id(self.next_op_id(), table_name, arrow_ipc_bytes)
    }

    /// Bulk ingest Arrow IPC stream bytes using a caller-allocated `op_id`.
    pub fn load_from_arrow_with_op_id(
        &self,
        op_id: u64,
        table_name: String,
        arrow_ipc_bytes: Vec<u8>,
    ) -> Result<usize, SqliteClientError> {
        self.dispatch_write(op_id, |slot| {
            slot.with_conn(op_id, |conn, _| {
                execute_load_from_arrow(conn, &table_name, &arrow_ipc_bytes)
            })
        })
    }

    /// Online backup to destination path.
    pub fn backup_to(
        &self,
        target_path: String,
        pages_per_step: i32,
        sleep_ms: u64,
    ) -> Result<(), SqliteClientError> {
        self.backup_to_with_op_id(self.next_op_id(), target_path, pages_per_step, sleep_ms)
    }

    /// Online backup to destination path using a caller-allocated `op_id`.
    pub fn backup_to_with_op_id(
        &self,
        op_id: u64,
        target_path: String,
        pages_per_step: i32,
        sleep_ms: u64,
    ) -> Result<(), SqliteClientError> {
        self.dispatch_write(op_id, |slot| {
            slot.with_conn(op_id, |conn, _| {
                execute_backup_to(conn, &target_path, pages_per_step, sleep_ms)
            })
        })
    }

    /// Online restore from source path.
    pub fn restore_from(
        &self,
        source_path: String,
        pages_per_step: i32,
        sleep_ms: u64,
    ) -> Result<(), SqliteClientError> {
        self.restore_from_with_op_id(self.next_op_id(), source_path, pages_per_step, sleep_ms)
    }

    /// Online restore from source path using a caller-allocated `op_id`.
    pub fn restore_from_with_op_id(
        &self,
        op_id: u64,
        source_path: String,
        pages_per_step: i32,
        sleep_ms: u64,
    ) -> Result<(), SqliteClientError> {
        self.dispatch_write(op_id, |slot| {
            slot.with_conn(op_id, |conn, _| {
                execute_restore_from(conn, &source_path, pages_per_step, sleep_ms)
            })
        })
    }

    /// Begin an explicit transaction.
    pub fn begin(&self, mode: String) -> Result<(), SqliteClientError> {
        self.begin_with_op_id(self.next_op_id(), mode)
    }

    /// Begin an explicit transaction using a caller-allocated `op_id`.
    pub fn begin_with_op_id(&self, op_id: u64, mode: String) -> Result<(), SqliteClientError> {
        let begin_sql = match normalize_begin_sql(&mode) {
            Ok(sql) => sql,
            Err(err) => {
                self.interrupts.lock().take_if_cancelled(op_id);
                return Err(err);
            }
        };
        let res = self.dispatch_write(op_id, |slot| {
            slot.with_conn(op_id, |conn, _| {
                conn.execute_batch(begin_sql)
                    .map_err(SqliteClientError::from_rusqlite)
            })
        });
        if res.is_ok() {
            self.in_transaction.store(true, Ordering::Release);
        }
        res
    }

    /// Commit the active transaction.
    pub fn commit(&self) -> Result<(), SqliteClientError> {
        self.commit_with_op_id(self.next_op_id())
    }

    /// Commit the active transaction using a caller-allocated `op_id`.
    pub fn commit_with_op_id(&self, op_id: u64) -> Result<(), SqliteClientError> {
        let res = self.dispatch_write(op_id, |slot| {
            slot.with_conn(op_id, |conn, _| {
                conn.execute_batch("COMMIT")
                    .map_err(SqliteClientError::from_rusqlite)
            })
        });
        self.in_transaction.store(false, Ordering::Release);
        res
    }

    /// Roll back the active transaction.
    pub fn rollback(&self) -> Result<(), SqliteClientError> {
        self.rollback_with_op_id(self.next_op_id())
    }

    /// Roll back the active transaction using a caller-allocated `op_id`.
    pub fn rollback_with_op_id(&self, op_id: u64) -> Result<(), SqliteClientError> {
        let res = self.dispatch_write(op_id, |slot| {
            slot.with_conn(op_id, |conn, _| {
                conn.execute_batch("ROLLBACK")
                    .map_err(SqliteClientError::from_rusqlite)
            })
        });
        self.in_transaction.store(false, Ordering::Release);
        res
    }

    /// Cancel all in-flight operations by triggering `InterruptHandle::interrupt()`.
    pub fn cancel(&self) {
        self.interrupts.lock().cancel_all();
    }

    /// Cancel a specific operation by its `op_id`.
    pub fn cancel_operation(&self, op_id: u64) -> bool {
        self.interrupts.lock().cancel_op(op_id)
    }

    /// Close every pooled connection after interrupting in-flight operations.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.cancel();
        self.writer.signal_close();
        for reader in &self.readers {
            reader.signal_close();
        }
    }
}

impl Drop for SqlitePool {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_read_only_sql_lexical_classification() {
        assert!(analyze_statement_facts("SELECT 1").read_only);
        assert!(
            analyze_statement_facts(
                "SELECT id, created_at, updated_at, deleted_at FROM items WHERE id = ?"
            )
            .read_only
        );
        assert!(
            analyze_statement_facts("SELECT * FROM items WHERE status = :update AND tag = @delete")
                .read_only
        );
        assert!(analyze_statement_facts("/* trace */ SELECT 1").read_only);
        assert!(
            analyze_statement_facts(
                "-- leading note\nSELECT 'UPDATE', \"delete\", [insert] FROM items"
            )
            .read_only
        );
        assert!(analyze_statement_facts("WITH cte AS (SELECT 1) SELECT * FROM cte").read_only);
        assert!(analyze_statement_facts("EXPLAIN QUERY PLAN SELECT 1").read_only);
        assert!(analyze_statement_facts("VALUES (1, 2)").read_only);

        let ins = analyze_statement_facts("INSERT INTO items VALUES (1)");
        assert!(!ins.read_only && ins.needs_lazy_begin && ins.is_insert);
        let semi_ins = analyze_statement_facts("; INSERT INTO items VALUES (1)");
        assert!(!semi_ins.read_only && semi_ins.needs_lazy_begin && semi_ins.is_insert);
        assert!(!analyze_statement_facts("UPDATE items SET x = 1").read_only);
        assert!(!analyze_statement_facts("DELETE FROM items").read_only);
        assert!(!analyze_statement_facts("REPLACE INTO items VALUES (1)").read_only);
        let with_replace =
            analyze_statement_facts("WITH cte AS (SELECT 1) REPLACE INTO items VALUES (1)");
        assert!(!with_replace.read_only && with_replace.needs_lazy_begin);
        assert!(
            !analyze_statement_facts("WITH cte AS (SELECT 1) INSERT INTO items SELECT * FROM cte")
                .read_only
        );
        let pragma = analyze_statement_facts("PRAGMA read_uncommitted");
        assert!(!pragma.read_only && !pragma.needs_lazy_begin);
        let begin = analyze_statement_facts("BEGIN IMMEDIATE");
        assert!(!begin.read_only && !begin.needs_lazy_begin);
        let sp = analyze_statement_facts("SAVEPOINT sa_savepoint_1");
        assert!(!sp.read_only && sp.needs_lazy_begin);
        let empty = analyze_statement_facts("   -- only a comment");
        assert!(!empty.read_only && !empty.needs_lazy_begin);
        assert!(analyze_statement_facts("CREATE TABLE items (id INT)").needs_lazy_begin);
    }

    #[test]
    fn direct_with_conn_handles_cancellation_and_last_rowid() {
        let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open memory pool");
        pool.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT)".to_string(),
            BoundParams::default(),
        )
        .expect("create table");
        pool.execute(
            "INSERT INTO items (label) VALUES ('alpha')".to_string(),
            BoundParams::default(),
        )
        .expect("insert row");
        assert_eq!(pool.last_insert_rowid(), 1);

        let cancelled_op = pool.next_op_id();
        assert!(pool.cancel_operation(cancelled_op));
        let err = pool
            .fetch_all_with_op_id(
                cancelled_op,
                "SELECT id FROM items".to_string(),
                BoundParams::default(),
            )
            .expect_err("pre-cancelled op must return Cancelled");
        assert!(matches!(err, SqliteClientError::Cancelled(_)));
        pool.close();
    }
}
