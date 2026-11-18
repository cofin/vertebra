//! Integration tests for vertebra-sqlite pool, transactions, WAL concurrency,
//! Arrow streaming, backup/restore, and cancellation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use _sqlite::config::{SqliteClientConfig, SqliteSynchronous};
use _sqlite::value::{BoundParams, PipelineResultItem, SqliteValue};
use _sqlite::worker::SqlitePool;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
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
