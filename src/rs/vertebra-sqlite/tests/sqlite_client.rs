//! Integration tests for vertebra-sqlite pool, transactions, WAL concurrency,
//! Arrow streaming, backup/restore, and cancellation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use _sqlite::config::{SqliteClientConfig, SqliteSynchronous};
use _sqlite::errors::SqliteClientError;
use _sqlite::value::{BoundParams, PipelineResultItem, SqliteValue, StatementOutcome};
use _sqlite::worker::SqlitePool;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use pyo3::prelude::*;
use tempfile::NamedTempFile;

#[test]
fn test_in_memory_pool_crud_and_pinning() {
    let config = SqliteClientConfig::default();
    let pool = SqlitePool::open(config).expect("open in-memory pool");
    assert!(pool.path() == ":memory:");
    assert!(!pool.in_transaction());

    // Create table
    pool.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL)".to_string(),
        BoundParams::Empty,
    )
    .expect("create table");

    // Insert rows
    let inserted = pool
        .execute(
            "INSERT INTO users (name, score) VALUES (?, ?)".to_string(),
            BoundParams::Positional(vec![
                SqliteValue::Text("Alice".to_string()),
                SqliteValue::Real(99.5),
            ]),
        )
        .expect("insert alice");
    assert_eq!(inserted, 1);

    // Fetch rows
    let res = pool
        .fetch_all(
            "SELECT id, name, score FROM users ORDER BY id".to_string(),
            BoundParams::Empty,
        )
        .expect("fetch users");
    assert_eq!(res.columns, vec!["id", "name", "score"]);
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], SqliteValue::Integer(1));
    assert_eq!(res.rows[0][1], SqliteValue::Text("Alice".to_string()));
    assert_eq!(res.rows[0][2], SqliteValue::Real(99.5));
}

#[test]
fn test_wal_file_concurrent_readers() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let config = SqliteClientConfig {
        path: path.clone(),
        wal_mode: true,
        synchronous: SqliteSynchronous::Normal,
        max_readers: 4,
        ..Default::default()
    };

    let pool = SqlitePool::open(config).expect("open WAL pool");

    pool.execute(
        "CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)".to_string(),
        BoundParams::Empty,
    )
    .expect("create table");

    for i in 0..10 {
        pool.execute(
            "INSERT INTO counter (val) VALUES (?)".to_string(),
            BoundParams::Positional(vec![SqliteValue::Integer(i)]),
        )
        .expect("insert");
    }

    let running = Arc::new(AtomicBool::new(true));
    let start_barrier = Arc::new(Barrier::new(5));
    let mut reader_handles = Vec::new();

    // Spawn 4 concurrent readers
    for _ in 0..4 {
        let pool_clone = Arc::clone(&pool);
        let running_clone = Arc::clone(&running);
        let barrier_clone = Arc::clone(&start_barrier);
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let mut read_count = 0;
            while running_clone.load(Ordering::Relaxed) || read_count == 0 {
                let res = pool_clone
                    .fetch_all(
                        "SELECT count(*) FROM counter".to_string(),
                        BoundParams::Empty,
                    )
                    .expect("read");
                assert!(!res.rows.is_empty());
                read_count += 1;
            }
            read_count
        });
        reader_handles.push(handle);
    }

    start_barrier.wait();

    // Writer writes more rows concurrently
    for i in 10..20 {
        pool.execute(
            "INSERT INTO counter (val) VALUES (?)".to_string(),
            BoundParams::Positional(vec![SqliteValue::Integer(i)]),
        )
        .expect("concurrent insert");
    }

    running.store(false, Ordering::Relaxed);
    for h in reader_handles {
        let count = h.join().expect("join reader");
        assert!(count > 0);
    }

    let final_res = pool
        .fetch_all(
            "SELECT count(*) FROM counter".to_string(),
            BoundParams::Empty,
        )
        .expect("final count");
    assert_eq!(final_res.rows[0][0], SqliteValue::Integer(20));
}

#[test]
fn test_explicit_transaction_commit_and_rollback() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");

    pool.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, title TEXT)".to_string(),
        BoundParams::Empty,
    )
    .expect("create table");

    // Test rollback
    pool.begin("IMMEDIATE".to_string()).expect("begin");
    assert!(pool.in_transaction());
    pool.execute(
        "INSERT INTO items (title) VALUES ('Should Rollback')".to_string(),
        BoundParams::Empty,
    )
    .expect("insert in tx");
    pool.rollback().expect("rollback");
    assert!(!pool.in_transaction());

    let res = pool
        .fetch_all("SELECT count(*) FROM items".to_string(), BoundParams::Empty)
        .expect("fetch items");
    assert_eq!(res.rows[0][0], SqliteValue::Integer(0));

    // Test commit
    pool.begin("IMMEDIATE".to_string()).expect("begin");
    assert!(pool.in_transaction());
    pool.execute(
        "INSERT INTO items (title) VALUES ('Committed')".to_string(),
        BoundParams::Empty,
    )
    .expect("insert in tx");
    pool.commit().expect("commit");
    assert!(!pool.in_transaction());

    let res2 = pool
        .fetch_all("SELECT count(*) FROM items".to_string(), BoundParams::Empty)
        .expect("fetch items");
    assert_eq!(res2.rows[0][0], SqliteValue::Integer(1));
}

#[test]
fn test_execute_many_and_execute_script() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");

    pool.execute_script("CREATE TABLE t1 (x INT); CREATE TABLE t2 (y TEXT);".to_string())
        .expect("script");

    let params_seq = vec![
        BoundParams::Positional(vec![SqliteValue::Integer(10)]),
        BoundParams::Positional(vec![SqliteValue::Integer(20)]),
        BoundParams::Positional(vec![SqliteValue::Integer(30)]),
    ];
    let count = pool
        .execute_many("INSERT INTO t1 (x) VALUES (?)".to_string(), params_seq)
        .expect("execute_many");
    assert_eq!(count, 3);

    let res = pool
        .fetch_all("SELECT sum(x) FROM t1".to_string(), BoundParams::Empty)
        .expect("fetch");
    assert_eq!(res.rows[0][0], SqliteValue::Integer(60));
}

#[test]
fn test_execute_pipeline_atomicity() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");

    pool.execute(
        "CREATE TABLE accounts (name TEXT PRIMARY KEY, balance INT)".to_string(),
        BoundParams::Empty,
    )
    .expect("create accounts");

    pool.execute(
        "INSERT INTO accounts (name, balance) VALUES ('Alice', 100), ('Bob', 50)".to_string(),
        BoundParams::Empty,
    )
    .expect("init accounts");

    // Successful pipeline with savepoint
    let ops = vec![
        (
            false,
            "UPDATE accounts SET balance = balance - 20 WHERE name = 'Alice'".to_string(),
            BoundParams::Empty,
        ),
        (
            false,
            "UPDATE accounts SET balance = balance + 20 WHERE name = 'Bob'".to_string(),
            BoundParams::Empty,
        ),
        (
            true,
            "SELECT name, balance FROM accounts ORDER BY name".to_string(),
            BoundParams::Empty,
        ),
    ];
    let results = pool.execute_pipeline(ops, true).expect("pipeline");
    assert_eq!(results.len(), 3);
    match &results[0] {
        PipelineResultItem::RowCount(n) => assert_eq!(*n, 1),
        _ => panic!("expected row count"),
    }
    match &results[2] {
        PipelineResultItem::Query(qr) => {
            assert_eq!(qr.rows.len(), 2);
            assert_eq!(qr.rows[0][1], SqliteValue::Integer(80)); // Alice
            assert_eq!(qr.rows[1][1], SqliteValue::Integer(70)); // Bob
        }
        _ => panic!("expected query result"),
    }

    // Failing pipeline should rollback atomically
    let failing_ops = vec![
        (
            false,
            "UPDATE accounts SET balance = balance - 10 WHERE name = 'Alice'".to_string(),
            BoundParams::Empty,
        ),
        (
            false,
            "INSERT INTO accounts (name, balance) VALUES ('Alice', 999)".to_string(),
            BoundParams::Empty,
        ), // PK violation!
    ];
    let fail_res = pool.execute_pipeline(failing_ops, true);
    assert!(fail_res.is_err());

    // Balance should remain unchanged (80, not 70)
    let check = pool
        .fetch_all(
            "SELECT balance FROM accounts WHERE name = 'Alice'".to_string(),
            BoundParams::Empty,
        )
        .expect("check");
    assert_eq!(check.rows[0][0], SqliteValue::Integer(80));
}

#[test]
fn test_online_backup_and_restore() {
    let source_tmp = NamedTempFile::new().expect("temp file");
    let target_tmp = NamedTempFile::new().expect("temp file");
    let source_path = source_tmp.path().to_str().unwrap().to_string();
    let target_path = target_tmp.path().to_str().unwrap().to_string();

    let source_pool = SqlitePool::open(SqliteClientConfig {
        path: source_path.clone(),
        wal_mode: true,
        ..Default::default()
    })
    .expect("open source pool");

    source_pool
        .execute(
            "CREATE TABLE data (k TEXT PRIMARY KEY, v TEXT)".to_string(),
            BoundParams::Empty,
        )
        .expect("create");
    source_pool
        .execute(
            "INSERT INTO data (k, v) VALUES ('hello', 'world'), ('foo', 'bar')".to_string(),
            BoundParams::Empty,
        )
        .expect("insert");

    // Backup to target file
    source_pool
        .backup_to(target_path.clone(), 64, 0)
        .expect("backup_to");

    // Open target file and verify contents
    let target_pool = SqlitePool::open(SqliteClientConfig {
        path: target_path.clone(),
        ..Default::default()
    })
    .expect("open target pool");

    let res = target_pool
        .fetch_all(
            "SELECT k, v FROM data ORDER BY k".to_string(),
            BoundParams::Empty,
        )
        .expect("fetch backup");
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][0], SqliteValue::Text("foo".to_string()));
    assert_eq!(res.rows[1][0], SqliteValue::Text("hello".to_string()));
}

#[test]
fn test_sqlite_jsonb_functions() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");

    let res = pool
        .fetch_all(
            "SELECT json_extract(jsonb('{\"key\": \"value\", \"num\": 42}'), '$.num') AS n"
                .to_string(),
            BoundParams::Empty,
        )
        .expect("jsonb query");

    assert_eq!(res.columns, vec!["n"]);
    assert_eq!(res.rows[0][0], SqliteValue::Integer(42));
}

#[test]
fn test_recursive_cte_cancellation() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");

    let pool_clone = Arc::clone(&pool);
    let _start = Instant::now();

    // Spawn a long-running recursive query
    let handle = thread::spawn(move || {
        pool_clone.fetch_all(
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt) SELECT count(*) FROM cnt".to_string(),
            BoundParams::Empty,
        )
    });

    // Let query start, then cancel within 20ms
    thread::sleep(Duration::from_millis(20));
    let cancel_start = Instant::now();
    pool.cancel();

    let res = handle.join().expect("join");
    let elapsed = cancel_start.elapsed();
    assert!(res.is_err(), "query should have been cancelled");
    assert!(
        elapsed < Duration::from_millis(200),
        "cancellation took too long: {:?}",
        elapsed
    );

    // Verify subsequent queries on pool work cleanly
    let verify = pool
        .fetch_all("SELECT 1 + 1".to_string(), BoundParams::Empty)
        .expect("subsequent query");
    assert_eq!(verify.rows[0][0], SqliteValue::Integer(2));
}

#[test]
fn test_arrow_load_and_select_roundtrip() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");

    // Generate Arrow RecordBatch with 1000 multi-type rows
    let row_count = 1000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_int", DataType::Int64, true),
        Field::new("c_real", DataType::Float64, true),
        Field::new("c_bool", DataType::Boolean, true),
        Field::new("c_text", DataType::Utf8, true),
        Field::new("c_blob", DataType::Binary, true),
    ]));

    let mut b_int = Int64Builder::with_capacity(row_count);
    let mut b_real = Float64Builder::with_capacity(row_count);
    let mut b_bool = BooleanBuilder::with_capacity(row_count);
    let mut b_text = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut b_blob = BinaryBuilder::with_capacity(row_count, row_count * 16);

    for i in 0..row_count {
        if i % 10 == 0 {
            b_int.append_null();
            b_real.append_null();
            b_bool.append_null();
            b_text.append_null();
            b_blob.append_null();
        } else {
            b_int.append_value(i as i64);
            b_real.append_value(i as f64 * 1.5);
            b_bool.append_value(i % 2 == 0);
            b_text.append_value(format!("row_{i}"));
            b_blob.append_value(format!("blob_{i}").as_bytes());
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(b_int.finish()),
        Arc::new(b_real.finish()),
        Arc::new(b_bool.finish()),
        Arc::new(b_text.finish()),
        Arc::new(b_blob.finish()),
    ];

    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).expect("create batch");
    let mut ipc_bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_bytes, &schema).expect("writer");
        writer.write(&batch).expect("write");
        writer.finish().expect("finish");
    }

    // Load from Arrow
    let loaded = pool
        .load_from_arrow("multi_test".to_string(), ipc_bytes)
        .expect("load_from_arrow");
    assert_eq!(loaded, row_count);

    // Select to Arrow
    let payload = pool
        .select_to_arrow(
            "SELECT * FROM multi_test ORDER BY rowid".to_string(),
            BoundParams::Empty,
            8192,
        )
        .expect("select_to_arrow");

    match payload {
        _sqlite::arrow_bridge::ArrowResultPayload::Shm {
            shm_name,
            byte_len,
            row_count: sel_rows,
            region,
        } => {
            assert!(shm_name.starts_with("/vtb_sqlite_"));
            assert_eq!(sel_rows, row_count);
            assert!(byte_len > 0);
            // Verify magic VTBARROW header in SHM region
            let slice = unsafe { region.as_slice() };
            assert_eq!(&slice[0..8], b"VTBARROW");
        }
        _ => panic!("expected SHM payload"),
    }
}

#[test]
fn test_execute_many_constraint_violation_rolls_back_and_returns_err() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");
    pool.execute(
        "CREATE TABLE uniq_items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)".to_string(),
        BoundParams::Empty,
    )
    .expect("create table");

    let params_seq = vec![
        BoundParams::Positional(vec![
            SqliteValue::Integer(1),
            SqliteValue::Text("one".to_string()),
        ]),
        BoundParams::Positional(vec![
            SqliteValue::Integer(1), // duplicate PK must error and roll back row 1
            SqliteValue::Text("dup".to_string()),
        ]),
    ];

    let err = pool
        .execute_many(
            "INSERT INTO uniq_items (id, label) VALUES (?, ?)".to_string(),
            params_seq,
        )
        .expect_err("duplicate PK in execute_many must return Err");
    assert_eq!(err.sqlstate(), Some("23505"));
    assert!(!pool.in_transaction());

    let res = pool
        .fetch_all(
            "SELECT count(*) FROM uniq_items".to_string(),
            BoundParams::Empty,
        )
        .expect("count after rollback");
    assert_eq!(res.rows[0][0], SqliteValue::Integer(0));
}

#[test]
fn test_begin_mode_whitelist_and_op_id_cancellation() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");

    // Whitelisted BEGIN modes succeed.
    for mode in ["", "deferred", "IMMEDIATE", " EXCLUSIVE "] {
        pool.begin(mode.to_string()).expect("valid begin mode");
        assert!(pool.in_transaction());
        pool.rollback().expect("rollback");
        assert!(!pool.in_transaction());
    }

    // Non-whitelisted / injection attempt is rejected before touching SQLite.
    let bad = pool.begin("IMMEDIATE; CREATE TABLE pwned(x INT)".to_string());
    assert!(bad.is_err());
    assert!(!pool.in_transaction());

    // Pre-cancelled op_id aborts before execution.
    let pre_op = pool.next_op_id();
    assert!(pool.cancel_operation(pre_op));
    let pre_res = pool.fetch_all_with_op_id(pre_op, "SELECT 42".to_string(), BoundParams::Empty);
    assert!(pre_res.is_err());

    // In-flight op_id cancellation interrupts that exact op_id.
    let live_op = pool.next_op_id();
    let pool_clone = Arc::clone(&pool);
    let handle = thread::spawn(move || {
        pool_clone.fetch_all_with_op_id(
            live_op,
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt) SELECT count(*) FROM cnt"
                .to_string(),
            BoundParams::Empty,
        )
    });
    thread::sleep(Duration::from_millis(20));
    assert!(pool.cancel_operation(live_op));
    assert!(handle.join().expect("join cancelled query").is_err());

    // Pool close joins worker threads cleanly and rejects subsequent queries.
    pool.close();
    assert!(pool.is_closed());
    assert!(
        pool.fetch_all("SELECT 1".to_string(), BoundParams::Empty)
            .is_err()
    );
}

fn open_outcome_pool() -> Arc<SqlitePool> {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open in-memory pool");
    pool.run(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
        BoundParams::Empty,
    )
    .expect("create items");
    pool
}

fn insert_item(pool: &SqlitePool, name: &str) -> StatementOutcome {
    pool.run(
        "INSERT INTO items (name) VALUES (?)",
        BoundParams::Positional(vec![SqliteValue::Text(name.to_string())]),
    )
    .expect("insert item")
}

#[test]
fn test_run_reports_query_outcome() {
    let pool = open_outcome_pool();

    let query = pool
        .run("SELECT 1 AS a", BoundParams::Empty)
        .expect("select literal");
    assert_eq!(query.columns, Some(vec!["a".to_string()]));
    assert_eq!(query.rows, vec![vec![SqliteValue::Integer(1)]]);
    assert_eq!(query.rowcount, -1);
    assert_eq!(query.lastrowid, None);

    let empty = pool
        .run("SELECT id, name FROM items WHERE 1 = 0", BoundParams::Empty)
        .expect("select with false predicate");
    assert_eq!(
        empty.columns,
        Some(vec!["id".to_string(), "name".to_string()])
    );
    assert!(empty.rows.is_empty());
    assert_eq!(empty.rowcount, -1);
    assert_eq!(empty.lastrowid, None);
}

#[test]
fn test_run_reports_dml_and_ddl_outcome() {
    let pool = open_outcome_pool();

    let inserted = insert_item(&pool, "a");
    assert_eq!(inserted.columns, None);
    assert!(inserted.rows.is_empty());
    assert_eq!(inserted.rowcount, 1);
    assert_eq!(inserted.lastrowid, Some(1));
    assert_eq!(pool.last_insert_rowid(), 1);

    assert_eq!(insert_item(&pool, "b").lastrowid, Some(2));

    let updated = pool
        .run("UPDATE items SET name = 'z'", BoundParams::Empty)
        .expect("update two rows");
    assert_eq!(updated.columns, None);
    assert_eq!(updated.rowcount, 2);
    assert_eq!(updated.lastrowid, None);

    let ddl = pool
        .run(
            "CREATE TABLE other (id INTEGER PRIMARY KEY)",
            BoundParams::Empty,
        )
        .expect("create table after dml");
    assert_eq!(ddl.columns, None);
    assert!(ddl.rows.is_empty());
    assert_eq!(ddl.rowcount, 0);
    assert_eq!(ddl.lastrowid, None);

    let returning = pool
        .run(
            "INSERT INTO items (name) VALUES (?) RETURNING id",
            BoundParams::Positional(vec![SqliteValue::Text("c".to_string())]),
        )
        .expect("insert returning");
    assert_eq!(returning.columns, Some(vec!["id".to_string()]));
    assert_eq!(returning.rows, vec![vec![SqliteValue::Integer(3)]]);
    assert_eq!(returning.rowcount, 1);
    assert_eq!(returning.lastrowid, Some(3));
}

#[test]
fn test_run_many_reports_total_rowcount() {
    let pool = open_outcome_pool();
    let sql = "INSERT INTO items (name) VALUES (?)";

    let many = pool
        .run_many(
            sql.to_string(),
            ["a", "b", "c"]
                .into_iter()
                .map(|name| BoundParams::Positional(vec![SqliteValue::Text(name.to_string())]))
                .collect(),
        )
        .expect("run many");
    assert_eq!(many.columns, None);
    assert!(many.rows.is_empty());
    assert_eq!(many.rowcount, 3);
    assert_eq!(many.lastrowid, None);

    let none = pool
        .run_many(sql.to_string(), Vec::new())
        .expect("run many without parameter sets");
    assert_eq!(none.columns, None);
    assert_eq!(none.rowcount, 0);
    assert_eq!(none.lastrowid, None);

    let count = pool
        .run("SELECT count(*) FROM items", BoundParams::Empty)
        .expect("count items");
    assert_eq!(count.rows, vec![vec![SqliteValue::Integer(3)]]);
}

#[test]
fn test_run_reports_integrity_error_on_duplicate_key() {
    let pool = open_outcome_pool();
    let sql = "INSERT INTO items (id, name) VALUES (1, 'a')";
    pool.run(sql, BoundParams::Empty).expect("first insert");

    let err = pool
        .run(sql, BoundParams::Empty)
        .expect_err("duplicate primary key");
    assert!(
        matches!(err, SqliteClientError::Integrity { .. }),
        "expected Integrity, got {err:?}"
    );
}

#[test]
fn test_statement_facts_cache_reuses_and_clears() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open in-memory pool");
    assert_eq!(pool.statement_facts_len(), 0);

    pool.run(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
        BoundParams::Empty,
    )
    .expect("create items");
    let before = pool.statement_facts_len();
    assert_eq!(before, 0);
    for _ in 0..2 {
        pool.run("INSERT INTO items (name) VALUES ('a')", BoundParams::Empty)
            .expect("insert literal");
    }
    assert_eq!(pool.statement_facts_len(), before + 1);

    let after_inserts = pool.statement_facts_len();
    for _ in 0..2 {
        pool.run("SELECT 1 AS a", BoundParams::Empty)
            .expect("select literal");
    }
    assert_eq!(pool.statement_facts_len(), after_inserts);

    let select = pool.statement_facts("SELECT 1 AS a");
    assert!(select.read_only);
    assert!(!select.is_insert);
    let insert = pool.statement_facts("INSERT INTO items (name) VALUES (?)");
    assert!(!insert.read_only);
    assert!(insert.is_insert);

    let fresh = SqlitePool::open(SqliteClientConfig::default()).expect("open in-memory pool");
    for i in 0..257 {
        fresh.statement_facts(&format!("SELECT {i}"));
    }
    assert_eq!(fresh.statement_facts_len(), 1);
}

#[test]
fn test_run_pre_cancelled_op_does_not_execute() {
    let pool = open_outcome_pool();
    let id = pool.next_op_id();
    assert!(pool.cancel_operation(id));

    let err = pool
        .run_with_op_id(
            id,
            "INSERT INTO items (name) VALUES ('cancelled')",
            BoundParams::Empty,
        )
        .expect_err("pre-cancelled op should fail");
    assert!(
        matches!(err, SqliteClientError::Cancelled(_)),
        "expected Cancelled, got {err:?}"
    );

    let count = pool
        .run("SELECT count(*) FROM items", BoundParams::Empty)
        .expect("count items");
    assert_eq!(count.rows, vec![vec![SqliteValue::Integer(0)]]);
}

#[test]
fn test_run_live_op_cancellation_interrupts_statement() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");
    let live = pool.next_op_id();
    let pool_clone = Arc::clone(&pool);
    let started = Arc::new(std::sync::Barrier::new(2));
    let started_worker = Arc::clone(&started);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let handle = thread::spawn(move || {
        started_worker.wait();
        let res = pool_clone.run_with_op_id(
            live,
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt) SELECT count(*) FROM cnt",
            BoundParams::Empty,
        );
        let _ = done_tx.send(());
        res
    });

    started.wait();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut cancelled = false;
    while Instant::now() < deadline {
        if pool.cancel_operation(live) {
            cancelled = true;
        }
        if done_rx.recv_timeout(Duration::from_millis(5)).is_ok() {
            break;
        }
    }
    assert!(cancelled, "cancel_operation must succeed for live op");
    assert!(handle.join().expect("join cancelled run").is_err());

    let verify = pool
        .run("SELECT 1", BoundParams::Empty)
        .expect("subsequent run after op cancel");
    assert_eq!(verify.rows, vec![vec![SqliteValue::Integer(1)]]);
}

#[test]
fn test_run_cancel_all_interrupts_running_statement() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");
    let pool_clone = Arc::clone(&pool);
    let started = Arc::new(std::sync::Barrier::new(2));
    let started_worker = Arc::clone(&started);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let handle = thread::spawn(move || {
        started_worker.wait();
        let res = pool_clone.run(
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt) SELECT count(*) FROM cnt",
            BoundParams::Empty,
        );
        let _ = done_tx.send(());
        res
    });

    started.wait();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut finished = false;
    while Instant::now() < deadline {
        pool.cancel();
        if done_rx.recv_timeout(Duration::from_millis(5)).is_ok() {
            finished = true;
            break;
        }
    }
    assert!(
        finished,
        "pool.cancel() did not interrupt running query before deadline"
    );

    let res = handle.join().expect("join cancelled run");
    assert!(res.is_err(), "run query should have been cancelled");

    let verify = pool
        .run("SELECT 1 + 1", BoundParams::Empty)
        .expect("subsequent run after cancel");
    assert_eq!(verify.rows, vec![vec![SqliteValue::Integer(2)]]);
}

#[test]
fn test_cancel_with_nothing_running_leaves_next_statement_alone() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");
    pool.cancel();

    let res = pool
        .run("SELECT 1 AS a", BoundParams::Empty)
        .expect("run after idle cancel");
    assert_eq!(res.rows, vec![vec![SqliteValue::Integer(1)]]);
}

#[test]
fn test_cancel_of_finished_op_leaves_later_ops_alone() {
    let pool = SqlitePool::open(SqliteClientConfig::default()).expect("open pool");
    let op_a = pool.next_op_id();
    let first = pool
        .run_with_op_id(op_a, "SELECT 1", BoundParams::Empty)
        .expect("first run");
    assert_eq!(first.rows, vec![vec![SqliteValue::Integer(1)]]);

    assert!(pool.cancel_operation(op_a));

    let second = pool
        .run("SELECT 2", BoundParams::Empty)
        .expect("later run after finished op cancel");
    assert_eq!(second.rows, vec![vec![SqliteValue::Integer(2)]]);
}

#[test]
fn test_run_routes_reads_to_readers_and_reports_insert_rowid() {
    let temp_file = NamedTempFile::new().expect("temp file");
    let path = temp_file.path().to_str().unwrap().to_string();
    let config = SqliteClientConfig {
        path,
        wal_mode: true,
        synchronous: SqliteSynchronous::Normal,
        max_readers: 2,
        ..Default::default()
    };
    let pool = SqlitePool::open(config).expect("open WAL pool");

    pool.run(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
        BoundParams::Empty,
    )
    .expect("create items");
    let inserted = pool
        .run(
            "INSERT INTO items (name) VALUES (?)",
            BoundParams::Positional(vec![SqliteValue::Text("alpha".to_string())]),
        )
        .expect("insert item");
    assert_eq!(inserted.lastrowid, Some(1));
    assert_eq!(inserted.rowcount, 1);

    let selected = pool
        .run("SELECT id, name FROM items", BoundParams::Empty)
        .expect("select items");
    assert_eq!(
        selected.rows,
        vec![vec![
            SqliteValue::Integer(1),
            SqliteValue::Text("alpha".to_string()),
        ]]
    );
    assert_eq!(pool.statement_facts_len(), 3);
}

#[test]
fn test_run_in_memory_looks_facts_up_only_for_changes() {
    let pool = open_outcome_pool();
    assert_eq!(pool.statement_facts_len(), 0);

    let selected = pool
        .run("SELECT 1 AS a", BoundParams::Empty)
        .expect("select literal");
    assert_eq!(selected.rows, vec![vec![SqliteValue::Integer(1)]]);
    assert_eq!(pool.statement_facts_len(), 0);

    let inserted = pool
        .run("INSERT INTO items (name) VALUES ('a')", BoundParams::Empty)
        .expect("insert item");
    assert_eq!(inserted.lastrowid, Some(1));
    assert_eq!(pool.statement_facts_len(), 1);
}

#[test]
fn test_bound_params_exact_containers() {
    Python::attach(|py| {
        for expr in [c"(1, 'a')", c"[1, 'a']"] {
            let obj = py.eval(expr, None, None).expect("eval positional");
            let params = BoundParams::from_py(Some(&obj)).expect("extract positional");
            match params {
                BoundParams::Positional(vals) => {
                    assert_eq!(
                        vals,
                        vec![SqliteValue::Integer(1), SqliteValue::Text("a".to_string())]
                    );
                }
                other => panic!("expected Positional for {expr:?}, got {other:?}"),
            }
        }

        let dict_obj = py.eval(c"{'k': 1}", None, None).expect("eval dict");
        let params = BoundParams::from_py(Some(&dict_obj)).expect("extract dict");
        match params {
            BoundParams::Named(map) => {
                assert_eq!(map.len(), 1);
                assert_eq!(map.get("k"), Some(&SqliteValue::Integer(1)));
            }
            other => panic!("expected Named for dict, got {other:?}"),
        }
    });
}

#[test]
fn test_bound_params_empty_containers_are_empty() {
    Python::attach(|py| {
        let absent = BoundParams::from_py(None).expect("extract absent");
        assert!(
            matches!(absent, BoundParams::Empty),
            "expected Empty for absent, got {absent:?}"
        );

        for expr in [c"()", c"[]", c"{}", c"None"] {
            let obj = py.eval(expr, None, None).expect("eval empty");
            let params = BoundParams::from_py(Some(&obj)).expect("extract empty");
            assert!(
                matches!(params, BoundParams::Empty),
                "expected Empty for {expr:?}, got {params:?}"
            );
        }
    });
}

#[test]
fn test_bound_params_subclasses_and_mappings_use_fallback() {
    Python::attach(|py| {
        for expr in [
            c"__import__('collections').namedtuple('Pair', ('a', 'b'))(1, 2)",
            c"type('SubList', (list,), {})([1, 2])",
        ] {
            let obj = py.eval(expr, None, None).expect("eval sequence subclass");
            let params = BoundParams::from_py(Some(&obj)).expect("extract sequence subclass");
            match params {
                BoundParams::Positional(vals) => {
                    assert_eq!(vals, vec![SqliteValue::Integer(1), SqliteValue::Integer(2)]);
                }
                other => panic!("expected Positional for {expr:?}, got {other:?}"),
            }
        }

        for expr in [
            c"type('SubDict', (dict,), {})({'k': 1})",
            c"__import__('types').MappingProxyType({'k': 1})",
        ] {
            let obj = py.eval(expr, None, None).expect("eval mapping");
            let params = BoundParams::from_py(Some(&obj)).expect("extract mapping");
            match params {
                BoundParams::Named(map) => {
                    assert_eq!(map.len(), 1);
                    assert_eq!(map.get("k"), Some(&SqliteValue::Integer(1)));
                }
                other => panic!("expected Named for {expr:?}, got {other:?}"),
            }
        }

        let str_obj = py.eval(c"'ab'", None, None).expect("eval str");
        let str_params = BoundParams::from_py(Some(&str_obj)).expect("extract str sequence");
        match str_params {
            BoundParams::Positional(vals) => {
                assert_eq!(
                    vals,
                    vec![
                        SqliteValue::Text("a".to_string()),
                        SqliteValue::Text("b".to_string()),
                    ]
                );
            }
            other => panic!("expected Positional for str, got {other:?}"),
        }
    });
}

#[test]
fn test_bound_params_rejects_generator_and_non_str_key() {
    Python::attach(|py| {
        let generator = py.eval(c"(x for x in [1])", None, None).expect("eval gen");
        let gen_err = BoundParams::from_py(Some(&generator)).expect_err("generator must fail");
        assert!(
            matches!(&gen_err, SqliteClientError::InvalidInput(msg) if msg.contains("parameters must be a sequence or mapping")),
            "unexpected generator error: {gen_err:?}"
        );

        let bad_dict = py.eval(c"{1: 'a'}", None, None).expect("eval bad dict");
        let key_err = BoundParams::from_py(Some(&bad_dict)).expect_err("non-str key must fail");
        assert!(
            matches!(&key_err, SqliteClientError::InvalidInput(msg) if msg.contains("parameter key must be str")),
            "unexpected non-str key error: {key_err:?}"
        );

        for expr in [c"(2**70,)", c"[2**70]", c"{'k': 2**70}"] {
            let bad_val = py.eval(expr, None, None).expect("eval bad value");
            let val_err =
                BoundParams::from_py(Some(&bad_val)).expect_err("overflow value must fail");
            assert!(
                matches!(&val_err, SqliteClientError::InvalidInput(msg) if msg.contains("unsupported Python parameter type")),
                "unexpected value error for {expr:?}: {val_err:?}"
            );
        }
    });
}

#[test]
fn test_sqlite_value_conversion_order() {
    Python::attach(|py| {
        let cases: [(&std::ffi::CStr, SqliteValue); 12] = [
            (c"True", SqliteValue::Integer(1)),
            (c"False", SqliteValue::Integer(0)),
            (c"1", SqliteValue::Integer(1)),
            (c"1.5", SqliteValue::Real(1.5)),
            (c"'s'", SqliteValue::Text("s".to_string())),
            (c"b'b'", SqliteValue::Blob(b"b".to_vec())),
            (c"bytearray(b'b')", SqliteValue::Blob(b"b".to_vec())),
            (c"type('SubInt', (int,), {})(7)", SqliteValue::Integer(7)),
            (
                c"type('SubStr', (str,), {})('sub')",
                SqliteValue::Text("sub".to_string()),
            ),
            (
                c"__import__('datetime').date(2026, 1, 2)",
                SqliteValue::Text("2026-01-02".to_string()),
            ),
            (
                c"__import__('decimal').Decimal('1.5')",
                SqliteValue::Text("1.5".to_string()),
            ),
            (c"{'a': 1}", SqliteValue::Text("{\"a\": 1}".to_string())),
        ];

        for (expr, expected) in cases {
            let obj = py.eval(expr, None, None).expect("eval value");
            let actual = SqliteValue::from_py(&obj).expect("convert value");
            assert_eq!(actual, expected, "mismatch for {expr:?}");
        }

        let big_int = py.eval(c"2**70", None, None).expect("eval big int");
        let err = SqliteValue::from_py(&big_int).expect_err("2**70 must fail");
        assert!(
            matches!(&err, SqliteClientError::InvalidInput(msg) if msg.contains("unsupported Python parameter type")),
            "unexpected overflow error: {err:?}"
        );
    });
}
