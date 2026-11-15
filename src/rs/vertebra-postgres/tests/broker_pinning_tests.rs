mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use _postgres::PostgresBrokerBackend;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use vertebra_core::dbrpc::{DBRPC_VERSION, DbRpcHeader, DbRpcOp};
use vertebra_db::BrokerPoolBackend;

use support::{
    frame_auth_ok, frame_backend_key_data, frame_parameter_status, frame_ready_for_query,
    read_startup_packet, read_tagged_frontend_frame, write_frames,
};

fn frame_command_complete(tag: &str) -> Vec<u8> {
    let mut payload = tag.as_bytes().to_vec();
    payload.push(0);
    let len = (4 + payload.len() as u32).to_be_bytes();
    let mut out = vec![b'C'];
    out.extend_from_slice(&len);
    out.extend_from_slice(&payload);
    out
}

fn frame_parse_complete() -> Vec<u8> {
    vec![b'1', 0, 0, 0, 4]
}

fn frame_bind_complete() -> Vec<u8> {
    vec![b'2', 0, 0, 0, 4]
}

fn frame_close_complete() -> Vec<u8> {
    vec![b'3', 0, 0, 0, 4]
}

fn frame_param_desc_empty() -> Vec<u8> {
    vec![b't', 0, 0, 0, 6, 0, 0]
}

fn frame_no_data() -> Vec<u8> {
    vec![b'n', 0, 0, 0, 4]
}

fn mp_write_u64(buf: &mut Vec<u8>, v: u64) {
    if v <= 0x7f {
        buf.push(v as u8);
    } else if v <= u64::from(u8::MAX) {
        buf.push(0xcc);
        buf.push(v as u8);
    } else if v <= u64::from(u16::MAX) {
        buf.push(0xcd);
        buf.extend_from_slice(&(v as u16).to_be_bytes());
    } else if v <= u64::from(u32::MAX) {
        buf.push(0xce);
        buf.extend_from_slice(&(v as u32).to_be_bytes());
    } else {
        buf.push(0xcf);
        buf.extend_from_slice(&v.to_be_bytes());
    }
}

fn mp_write_opt_str(buf: &mut Vec<u8>, val: Option<&str>) {
    match val {
        None => buf.push(0xc0),
        Some(s) => {
            let b = s.as_bytes();
            if b.len() <= 31 {
                buf.push(0xa0 | (b.len() as u8));
            } else if b.len() <= usize::from(u8::MAX) {
                buf.push(0xd9);
                buf.push(b.len() as u8);
            } else {
                buf.push(0xda);
                buf.extend_from_slice(&(b.len() as u16).to_be_bytes());
            }
            buf.extend_from_slice(b);
        }
    }
}

fn encode_request_frame(
    op: DbRpcOp,
    request_id: u64,
    lease_slot: u32,
    lease_generation: u64,
    sql: Option<&str>,
    savepoint: Option<&str>,
) -> Vec<u8> {
    let mut out = DbRpcHeader::new(op, request_id, lease_slot, lease_generation)
        .encode()
        .to_vec();
    out.push(0x99);
    mp_write_u64(&mut out, u64::from(DBRPC_VERSION));
    mp_write_u64(&mut out, u64::from(op as u8));
    mp_write_u64(&mut out, request_id);
    mp_write_u64(&mut out, u64::from(lease_slot));
    mp_write_u64(&mut out, lease_generation);
    mp_write_opt_str(&mut out, sql);
    out.push(0x90);
    mp_write_opt_str(&mut out, savepoint);
    mp_write_u64(&mut out, 0);
    out
}

fn decode_mp_u64(src: &[u8], pos: &mut usize) -> u64 {
    let tag = src[*pos];
    *pos += 1;
    match tag {
        0x00..=0x7f => u64::from(tag),
        0xcc => {
            let v = src[*pos];
            *pos += 1;
            u64::from(v)
        }
        0xcd => {
            let v = u16::from_be_bytes([src[*pos], src[*pos + 1]]);
            *pos += 2;
            u64::from(v)
        }
        0xce => {
            let v = u32::from_be_bytes([src[*pos], src[*pos + 1], src[*pos + 2], src[*pos + 3]]);
            *pos += 4;
            u64::from(v)
        }
        0xcf => {
            let v = u64::from_be_bytes(src[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            v
        }
        other => panic!("unexpected u64 msgpack tag 0x{other:02x}"),
    }
}

fn skip_mp_val(src: &[u8], pos: &mut usize) {
    let tag = src[*pos];
    *pos += 1;
    match tag {
        0x00..=0x7f | 0xc0 | 0xc2 | 0xc3 | 0xe0..=0xff => {}
        0x80..=0x8f => {
            for _ in 0..(tag & 0x0f) {
                skip_mp_val(src, pos);
                skip_mp_val(src, pos);
            }
        }
        0x90..=0x9f => {
            for _ in 0..(tag & 0x0f) {
                skip_mp_val(src, pos);
            }
        }
        0xa0..=0xbf => *pos += usize::from(tag & 0x1f),
        0xcc | 0xd0 => *pos += 1,
        0xcd | 0xd1 => *pos += 2,
        0xce | 0xd2 => *pos += 4,
        0xcf | 0xd3 => *pos += 8,
        0xd9 => {
            let len = usize::from(src[*pos]);
            *pos += 1 + len;
        }
        0xda => {
            let len = usize::from(u16::from_be_bytes([src[*pos], src[*pos + 1]]));
            *pos += 2 + len;
        }
        other => panic!("unexpected msgpack tag 0x{other:02x}"),
    }
}

fn decode_mp_opt_str(src: &[u8], pos: &mut usize) -> Option<String> {
    let tag = src[*pos];
    *pos += 1;
    match tag {
        0xc0 => None,
        0xa0..=0xbf => {
            let len = usize::from(tag & 0x1f);
            let s = String::from_utf8(src[*pos..*pos + len].to_vec()).unwrap();
            *pos += len;
            Some(s)
        }
        0xd9 => {
            let len = usize::from(src[*pos]);
            *pos += 1;
            let s = String::from_utf8(src[*pos..*pos + len].to_vec()).unwrap();
            *pos += len;
            Some(s)
        }
        0xda => {
            let len = usize::from(u16::from_be_bytes([src[*pos], src[*pos + 1]]));
            *pos += 2;
            let s = String::from_utf8(src[*pos..*pos + len].to_vec()).unwrap();
            *pos += len;
            Some(s)
        }
        other => panic!("unexpected str msgpack tag 0x{other:02x}"),
    }
}

fn decode_response_status(frame: &[u8]) -> (u64, Option<String>) {
    assert_eq!(frame[0], 0x99);
    let mut pos = 1;
    let _req_id = decode_mp_u64(frame, &mut pos);
    let status = decode_mp_u64(frame, &mut pos);
    skip_mp_val(frame, &mut pos);
    skip_mp_val(frame, &mut pos);
    skip_mp_val(frame, &mut pos);
    skip_mp_val(frame, &mut pos);
    skip_mp_val(frame, &mut pos);
    let _err_cls = decode_mp_opt_str(frame, &mut pos);
    let err_msg = decode_mp_opt_str(frame, &mut pos);
    (status, err_msg)
}

struct MockPgServer {
    addr: SocketAddr,
    events: Arc<Mutex<Vec<(usize, String)>>>,
    stop: Arc<AtomicBool>,
    notify: Arc<Notify>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl MockPgServer {
    fn start() -> Self {
        let events = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());
        let (tx, rx) = std::sync::mpsc::channel();

        let events_srv = Arc::clone(&events);
        let stop_srv = Arc::clone(&stop);
        let notify_srv = Arc::clone(&notify);

        let thread_handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
                let addr = listener.local_addr().expect("addr");
                tx.send(addr).expect("send addr");
                let next_conn_id = Arc::new(AtomicUsize::new(1));
                let mut tasks = Vec::new();
                while !stop_srv.load(Ordering::Acquire) {
                    tokio::select! {
                        accept_res = listener.accept() => {
                            let Ok((mut stream, _)) = accept_res else { break; };
                            let _ = stream.set_nodelay(true);
                            let conn_id = next_conn_id.fetch_add(1, Ordering::Relaxed);
                            let log = Arc::clone(&events_srv);
                            tasks.push(tokio::spawn(async move {
                                let _ = read_startup_packet(&mut stream).await;
                                if write_frames(
                                    &mut stream,
                                    &[
                                        frame_auth_ok(),
                                        frame_parameter_status("server_version", "16.0"),
                                        frame_backend_key_data(conn_id as i32, 99),
                                        frame_ready_for_query(b'I'),
                                    ],
                                ).await.is_err() {
                                    return;
                                }
                                let mut tx_status = b'I';
                                let mut stmts: BTreeMap<String, String> = BTreeMap::new();
                                let mut portals: BTreeMap<String, String> = BTreeMap::new();
                                let mut queued: Vec<Vec<u8>> = Vec::new();
                                while let Ok((tag, payload)) = read_tagged_frontend_frame(&mut stream).await {
                                    match tag {
                                        b'Q' => {
                                            let sql = String::from_utf8_lossy(payload.split(|b| *b == 0).next().unwrap_or(&[])).into_owned();
                                            let upper = sql.trim().to_ascii_uppercase();
                                            log.lock().unwrap().push((conn_id, format!("Q:{sql}")));
                                            let cmd = if upper == "BEGIN" {
                                                tx_status = b'T';
                                                "BEGIN"
                                            } else if upper == "COMMIT" {
                                                tx_status = b'I';
                                                "COMMIT"
                                            } else if upper.starts_with("ROLLBACK TO") {
                                                "ROLLBACK"
                                            } else if upper == "ROLLBACK" {
                                                tx_status = b'I';
                                                "ROLLBACK"
                                            } else if upper.starts_with("SAVEPOINT") {
                                                "SAVEPOINT"
                                            } else {
                                                "SELECT 1"
                                            };
                                            if write_frames(&mut stream, &[frame_command_complete(cmd), frame_ready_for_query(tx_status)]).await.is_err() {
                                                break;
                                            }
                                        }
                                        b'P' => {
                                            let mut parts = payload.split(|b| *b == 0);
                                            let name = String::from_utf8_lossy(parts.next().unwrap_or(&[])).into_owned();
                                            let sql = String::from_utf8_lossy(parts.next().unwrap_or(&[])).into_owned();
                                            stmts.insert(name, sql);
                                            queued.push(frame_parse_complete());
                                        }
                                        b'D' => {
                                            if payload.first().copied() == Some(b'S') {
                                                queued.push(frame_param_desc_empty());
                                            }
                                            queued.push(frame_no_data());
                                        }
                                        b'B' => {
                                            let mut parts = payload.split(|b| *b == 0);
                                            let portal = String::from_utf8_lossy(parts.next().unwrap_or(&[])).into_owned();
                                            let stmt_name = String::from_utf8_lossy(parts.next().unwrap_or(&[])).into_owned();
                                            let sql = stmts.get(&stmt_name).cloned().unwrap_or_default();
                                            portals.insert(portal, sql);
                                            queued.push(frame_bind_complete());
                                        }
                                        b'E' => {
                                            let portal = String::from_utf8_lossy(payload.split(|b| *b == 0).next().unwrap_or(&[])).into_owned();
                                            let sql = portals
                                                .get(&portal)
                                                .cloned()
                                                .or_else(|| stmts.get(&portal).cloned())
                                                .unwrap_or_else(|| "<unnamed>".to_string());
                                            log.lock().unwrap().push((conn_id, format!("E:{sql}")));
                                            queued.push(frame_command_complete("INSERT 0 1"));
                                        }
                                        b'C' => {
                                            queued.push(frame_close_complete());
                                        }
                                        b'S' => {
                                            queued.push(frame_ready_for_query(tx_status));
                                            let to_send = std::mem::take(&mut queued);
                                            if write_frames(&mut stream, &to_send).await.is_err() {
                                                break;
                                            }
                                        }
                                        b'X' => break,
                                        _ => {}
                                    }
                                }
                            }));
                        }
                        _ = notify_srv.notified() => break,
                    }
                }
                for t in tasks {
                    t.abort();
                }
            });
        });
        let addr = rx.recv().expect("server addr");
        Self {
            addr,
            events,
            stop,
            notify,
            thread_handle: Some(thread_handle),
        }
    }
}

impl Drop for MockPgServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.notify.notify_waiters();
        if let Some(h) = self.thread_handle.take() {
            let _ = h.join();
        }
    }
}

#[test]
fn postgres_broker_backend_pins_generations_handles_idle_rollback_and_drains_on_close() {
    let srv = MockPgServer::start();
    let backend = PostgresBrokerBackend::new();
    let dsn = format!(
        "postgres://postgres@{}:{}/prod?sslmode=disable",
        srv.addr.ip(),
        srv.addr.port()
    );
    let pool_id = backend.open_pool(&dsn, 2).expect("open pool");

    // 1. Idle Rollback(None) without an active pinned tx succeeds cleanly without pool rollback failure.
    let idle_rb = backend
        .execute(
            pool_id,
            &encode_request_frame(DbRpcOp::Rollback, 1, 0, 1, None, None),
        )
        .expect("idle rollback ok");
    assert_eq!(decode_response_status(&idle_rb).0, 0);

    // 2. Full pinned lifecycle on (slot=0, gen=1): Begin -> Execute -> Savepoint -> Rollback(sp) -> Commit.
    for (req_id, op, sql, sp) in [
        (2, DbRpcOp::Begin, None, None),
        (
            3,
            DbRpcOp::Execute,
            Some("INSERT INTO items VALUES (1)"),
            None,
        ),
        (4, DbRpcOp::Savepoint, None, Some("sp1")),
        (5, DbRpcOp::Rollback, None, Some("sp1")),
        (6, DbRpcOp::Commit, None, None),
    ] {
        let resp = backend
            .execute(pool_id, &encode_request_frame(op, req_id, 0, 1, sql, sp))
            .expect("pinned op");
        assert_eq!(decode_response_status(&resp), (0, None));
    }

    let pin_conn_id = {
        let ev = srv.events.lock().unwrap();
        let begin_ev = ev
            .iter()
            .find(|(_, s)| s == "Q:BEGIN")
            .expect("BEGIN logged");
        let exec_ev = ev
            .iter()
            .find(|(_, s)| s == "E:INSERT INTO items VALUES (1)")
            .expect("Execute logged");
        let commit_ev = ev
            .iter()
            .find(|(_, s)| s == "Q:COMMIT")
            .expect("COMMIT logged");
        assert_eq!(exec_ev.0, begin_ev.0);
        assert_eq!(commit_ev.0, begin_ev.0);
        begin_ev.0
    };
    assert!(pin_conn_id >= 1);

    // 3. Generation fencing on (slot=0, gen=2):
    // Unleased (0, 0) Execute must NOT run inside slot 0's active tx;
    // Stale gen=1 Execute/Savepoint/Commit/Rollback must be rejected without mutating gen=2 tx;
    // Double Begin cleans up cleanly; BrokerHost bumped gen=3 Rollback rolls back gen=2 tx.
    let b2 = backend
        .execute(
            pool_id,
            &encode_request_frame(DbRpcOp::Begin, 10, 0, 2, None, None),
        )
        .unwrap();
    assert_eq!(decode_response_status(&b2).0, 0);

    let unleased = backend
        .execute(
            pool_id,
            &encode_request_frame(DbRpcOp::Execute, 11, 0, 0, Some("SELECT 42"), None),
        )
        .unwrap();
    assert_eq!(decode_response_status(&unleased).0, 0);

    for (req_id, op, sql, sp) in [
        (
            12,
            DbRpcOp::Execute,
            Some("INSERT INTO bad VALUES (1)"),
            None,
        ),
        (13, DbRpcOp::Savepoint, None, Some("sp_stale")),
        (14, DbRpcOp::Commit, None, None),
        (15, DbRpcOp::Rollback, None, None),
    ] {
        let stale = backend
            .execute(pool_id, &encode_request_frame(op, req_id, 0, 1, sql, sp))
            .unwrap();
        let (status, msg) = decode_response_status(&stale);
        assert_ne!(status, 0);
        assert!(msg.unwrap_or_default().contains("stale lease generation"));
    }

    // Re-begin on same slot/gen rolls back prior pinned tx cleanly.
    let double_b2 = backend
        .execute(
            pool_id,
            &encode_request_frame(DbRpcOp::Begin, 16, 0, 2, None, None),
        )
        .unwrap();
    assert_eq!(decode_response_status(&double_b2).0, 0);

    // BrokerHost crash recovery uses bumped generation (gen=3 >= pinned_gen=2).
    let reclaim_rb = backend
        .execute(
            pool_id,
            &encode_request_frame(DbRpcOp::Rollback, 17, 0, 3, None, None),
        )
        .unwrap();
    assert_eq!(decode_response_status(&reclaim_rb), (0, None));

    // Verify unleased (0, 0) ran on a separate pooled connection while gen=2 held its pinned transaction.
    {
        let ev = srv.events.lock().unwrap();
        let begin_positions: Vec<(usize, usize)> = ev
            .iter()
            .enumerate()
            .filter(|(_, (_, s))| s == "Q:BEGIN")
            .map(|(idx, (cid, _))| (idx, *cid))
            .collect();
        let (unleased_idx, unleased_cid) = ev
            .iter()
            .enumerate()
            .find(|(_, (_, s))| s == "E:SELECT 42")
            .map(|(idx, (cid, _))| (idx, *cid))
            .expect("unleased SELECT 42 logged");
        assert!(begin_positions.len() >= 3);
        let (gen2_begin_idx, gen2_begin_cid) = begin_positions[1];
        let (double_begin_idx, _) = begin_positions[2];
        assert!(gen2_begin_idx < unleased_idx && unleased_idx < double_begin_idx);
        assert_ne!(unleased_cid, gen2_begin_cid);
        assert!(!ev.iter().any(|(_, s)| s == "E:INSERT INTO bad VALUES (1)"));
    }

    // 4. Higher generation Execute evicts stale leftover pinned connection before running autocommit.
    let b_stale = backend
        .execute(
            pool_id,
            &encode_request_frame(DbRpcOp::Begin, 18, 0, 4, None, None),
        )
        .unwrap();
    assert_eq!(decode_response_status(&b_stale).0, 0);
    let evicted = backend
        .execute(
            pool_id,
            &encode_request_frame(DbRpcOp::Execute, 19, 0, 5, Some("SELECT 99"), None),
        )
        .unwrap();
    assert_eq!(decode_response_status(&evicted).0, 0);

    // 5. close_pool drains active pinned transaction without stalling 5s.
    let b_close = backend
        .execute(
            pool_id,
            &encode_request_frame(DbRpcOp::Begin, 20, 0, 6, None, None),
        )
        .unwrap();
    assert_eq!(decode_response_status(&b_close).0, 0);
    let close_start = std::time::Instant::now();
    backend.close_pool(pool_id);
    assert!(
        close_start.elapsed() < std::time::Duration::from_secs(2),
        "close_pool must drain active pinned connections immediately"
    );
}
