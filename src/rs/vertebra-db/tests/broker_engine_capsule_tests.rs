use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use vertebra_core::dbrpc::{DbRpcHeader, DbRpcOp};
use vertebra_db::{
    BROKER_ENGINE_ABI_VERSION, BrokerEngineBackend, BrokerPoolBackend, DbRpcEncodedResponse,
    DbRpcScalar, ForeignEngineCapsule, PlaceholderStyle, decode_dbrpc_request_frame,
    encode_dbrpc_response_frame, new_broker_engine_capsule, normalize_placeholders,
};

struct CountingBackend {
    tag: u16,
    name: &'static str,
    drops: Arc<AtomicUsize>,
    panic_on_op: AtomicBool,
}

impl CountingBackend {
    fn new(tag: u16, name: &'static str, drops: Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(Self {
            tag,
            name,
            drops,
            panic_on_op: AtomicBool::new(false),
        })
    }
}

impl Drop for CountingBackend {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}

impl BrokerEngineBackend for CountingBackend {
    fn dialect_tag(&self) -> u16 {
        self.tag
    }

    fn dialect_name(&self) -> &'static str {
        self.name
    }

    fn normalize_statement(&self, sql: &str) -> (String, usize) {
        assert!(!self.panic_on_op.load(Ordering::Acquire), "boom normalize");
        normalize_placeholders(sql, PlaceholderStyle::DollarNumbered)
    }
}

impl BrokerPoolBackend for CountingBackend {
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String> {
        assert!(!self.panic_on_op.load(Ordering::Acquire), "boom open_pool");
        if dsn.starts_with("invalid://") || max_size == 0 {
            return Err(format!("invalid test DSN `{dsn}`"));
        }
        Ok(77)
    }

    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String> {
        assert!(!self.panic_on_op.load(Ordering::Acquire), "boom execute");
        if pool_id != 77 {
            return Err(format!("unknown pool id {pool_id}"));
        }
        let req = decode_dbrpc_request_frame(frame)?;
        let resp = DbRpcEncodedResponse::ok_rows(
            req.request_id,
            1,
            vec!["val".to_string()],
            vec![vec![DbRpcScalar::Int(42)]],
        );
        Ok(encode_dbrpc_response_frame(&resp))
    }

    fn close_pool(&self, pool_id: u64) {
        assert!(!self.panic_on_op.load(Ordering::Acquire), "boom close_pool");
        let _ = pool_id;
    }
}

#[test]
fn capsule_refcount_clone_drop_balance_and_swap() {
    let drops = Arc::new(AtomicUsize::new(0));
    {
        let backend = CountingBackend::new(91, "counting", Arc::clone(&drops));
        let capsule = new_broker_engine_capsule(backend);
        assert_eq!(capsule.abi_version, BROKER_ENGINE_ABI_VERSION);
        // SAFETY: `capsule` is live and owns an `Arc<CapsuleState>`.
        let imported1 = unsafe { ForeignEngineCapsule::from_capsule_ptr(&capsule) }
            .expect("import valid capsule");
        let imported2 = imported1.clone();
        assert_eq!(imported1.dialect_tag(), 91);
        assert_eq!(imported2.dialect_name(), "counting");
        drop(capsule);
        assert_eq!(drops.load(Ordering::Acquire), 0);
        let (norm, n) = imported1.normalize("SELECT :id").expect("normalize");
        assert_eq!(norm, "SELECT $1");
        assert_eq!(n, 1);
        drop(imported1);
        assert_eq!(drops.load(Ordering::Acquire), 0);
        drop(imported2);
    }
    assert_eq!(drops.load(Ordering::Acquire), 1);
}

#[test]
fn capsule_rejects_abi_version_mismatch_without_cloning_context() {
    let drops = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend::new(92, "mismatch_engine", Arc::clone(&drops));
    let mut capsule = new_broker_engine_capsule(backend);
    capsule.abi_version = 99;
    // SAFETY: `capsule` is a valid initialized struct with modified `abi_version`.
    let err = match unsafe { ForeignEngineCapsule::from_capsule_ptr(&capsule) } {
        Ok(_) => panic!("expected ABI mismatch error"),
        Err(err) => err,
    };
    assert!(err.contains("mismatch_engine"), "got: {err}");
    assert!(err.contains("expected 1, got 99"), "got: {err}");
    drop(capsule);
    assert_eq!(drops.load(Ordering::Acquire), 1);
}

#[test]
fn capsule_traps_panics_across_every_extern_c_entry_point() {
    let drops = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend::new(93, "panic_engine", Arc::clone(&drops));
    let capsule = new_broker_engine_capsule(Arc::clone(&backend) as Arc<dyn BrokerPoolBackend>);
    // SAFETY: `capsule` is initialized above.
    let imported =
        unsafe { ForeignEngineCapsule::from_capsule_ptr(&capsule) }.expect("import valid capsule");
    backend.panic_on_op.store(true, Ordering::Release);

    let norm_err = imported.normalize("SELECT 1").expect_err("panic contained");
    assert!(norm_err.contains("boom normalize"), "got: {norm_err}");

    let open_err = imported
        .open_pool("postgres://localhost/db", 4)
        .expect_err("panic contained");
    assert!(open_err.contains("boom open_pool"), "got: {open_err}");

    let exec_err = imported
        .execute(77, &[1, 0, 1])
        .expect_err("panic contained");
    assert!(exec_err.contains("boom execute"), "got: {exec_err}");

    imported.close_pool(77);

    backend.panic_on_op.store(false, Ordering::Release);
    assert_eq!(
        imported
            .open_pool("invalid://dsn", 4)
            .expect_err("invalid dsn rejected"),
        "invalid test DSN `invalid://dsn`"
    );
    assert_eq!(
        imported
            .execute(999, &[])
            .expect_err("unknown pool rejected"),
        "unknown pool id 999"
    );
}

#[test]
fn dbrpc_frame_decode_and_response_encode_roundtrip() {
    let hdr = DbRpcHeader::new(DbRpcOp::Execute, 42, 0, 1).encode();
    let mut frame = hdr.to_vec();
    frame.extend_from_slice(&[
        0x99, 0x01, 0x01, 0x2a, 0x00, 0x01, 0xa8, b'S', b'E', b'L', b'E', b'C', b'T', b' ', b'?',
        0x95, 0x07, 0xa1, b'a', 0xc3, 0xc0, 0xcb, 0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xc0, 0x00,
    ]);
    let req = decode_dbrpc_request_frame(&frame).expect("decode dbrpc frame");
    assert_eq!(req.op, DbRpcOp::Execute);
    assert_eq!(req.request_id, 42);
    assert_eq!(req.sql.as_deref(), Some("SELECT ?"));
    assert_eq!(
        req.params,
        vec![
            DbRpcScalar::Int(7),
            DbRpcScalar::Str("a".to_string()),
            DbRpcScalar::Bool(true),
            DbRpcScalar::Null,
            DbRpcScalar::Float(1.5),
        ]
    );

    let encoded = encode_dbrpc_response_frame(&DbRpcEncodedResponse::ok_rows(
        42,
        1,
        vec!["x".to_string()],
        vec![vec![DbRpcScalar::Int(7)]],
    ));
    assert_eq!(
        encoded,
        vec![
            0x99, 0x2a, 0x00, 0x01, 0x91, 0xa1, b'x', 0x91, 0x91, 0x07, 0xc0, 0xc0, 0xc0, 0xc0
        ]
    );
}
