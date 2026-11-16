//! Integration tests for Valkey Cluster (`CLUSTER SHARDS` / `CLUSTER SLOTS` fallback,
//! CRC16-XMODEM `{tag}`, `CROSSSLOT`, `MOVED`/`ASK` + loop limit, `READONLY` replica routing)
//! and Sentinel primary discovery / promotion failover.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use _valkey::client::{PipelineDecoder, QueuedCommand, ValkeyEngine};
use _valkey::cluster::validate_cluster_keys_same_slot;
use _valkey::codec::{RespValue, try_decode_frame};
use _valkey::config::ValkeyConfigInner;
use _valkey::error::ValkeyErrorKind;
use _valkey::{crc16_xmodem, valkey_hash_slot};
use bytes::{BufMut, BytesMut};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn encode_bulk(s: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", s.len()).into_bytes();
    out.extend_from_slice(s);
    out.extend_from_slice(b"\r\n");
    out
}

fn encode_array(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        out.extend_from_slice(item);
    }
    out
}

fn encode_int(n: i64) -> Vec<u8> {
    format!(":{n}\r\n").into_bytes()
}

fn cmd_parts(frame: RespValue) -> Vec<String> {
    frame
        .into_unattributed()
        .as_slice()
        .unwrap_or(&[])
        .iter()
        .map(|v| v.as_str().map_or_else(String::new, |s| s.into_owned()))
        .collect()
}

async fn read_next_cmd<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> Option<Vec<String>> {
    let mut tmp = [0u8; 4096];
    loop {
        if let Ok(Some(frame)) = try_decode_frame(buf) {
            return Some(cmd_parts(frame));
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.put_slice(&tmp[..n]);
    }
}

fn build_cluster_shards_reply(p1_port: u16, r1_port: u16, p2_port: u16) -> Vec<u8> {
    let shard1 = encode_array(&[
        encode_bulk(b"slots"),
        encode_array(&[encode_int(0), encode_int(8191)]),
        encode_bulk(b"nodes"),
        encode_array(&[
            encode_array(&[
                encode_bulk(b"endpoint"),
                encode_bulk(b"127.0.0.1"),
                encode_bulk(b"port"),
                encode_int(i64::from(p1_port)),
                encode_bulk(b"role"),
                encode_bulk(b"master"),
                encode_bulk(b"health"),
                encode_bulk(b"online"),
            ]),
            encode_array(&[
                encode_bulk(b"endpoint"),
                encode_bulk(b"127.0.0.1"),
                encode_bulk(b"port"),
                encode_int(i64::from(r1_port)),
                encode_bulk(b"role"),
                encode_bulk(b"replica"),
                encode_bulk(b"health"),
                encode_bulk(b"online"),
            ]),
        ]),
    ]);
    let shard2 = encode_array(&[
        encode_bulk(b"slots"),
        encode_array(&[encode_int(8192), encode_int(16383)]),
        encode_bulk(b"nodes"),
        encode_array(&[encode_array(&[
            encode_bulk(b"endpoint"),
            encode_bulk(b"127.0.0.1"),
            encode_bulk(b"port"),
            encode_int(i64::from(p2_port)),
            encode_bulk(b"role"),
            encode_bulk(b"master"),
            encode_bulk(b"health"),
            encode_bulk(b"online"),
        ])]),
    ]);
    encode_array(&[shard1, shard2])
}

fn find_key_in_slot_range(lo: u16, hi: u16, prefix: &str) -> Vec<u8> {
    for i in 0..50_000 {
        let cand = format!("{prefix}:{i}").into_bytes();
        let slot = valkey_hash_slot(&cand);
        if slot >= lo && slot <= hi {
            return cand;
        }
    }
    panic!("failed to find key in slot range {lo}..={hi}");
}

#[test]
fn crc16_xmodem_and_hash_tags_match_cluster_spec() {
    assert_eq!(crc16_xmodem(b"123456789"), 0x31C3);
    assert_eq!(valkey_hash_slot(b"{tag}"), valkey_hash_slot(b"a:{tag}:b"));
    assert_ne!(valkey_hash_slot(b"foo{}bar"), valkey_hash_slot(b""));
    assert_eq!(
        valkey_hash_slot(b"a{first}{second}"),
        valkey_hash_slot(b"first")
    );

    assert!(validate_cluster_keys_same_slot(&[b"{same}:k1", b"{same}:k2"]).is_ok());
    let err = validate_cluster_keys_same_slot(&[b"alpha_1", b"beta_2"]).unwrap_err();
    assert_eq!(err.kind, ValkeyErrorKind::Programming);
    assert!(err.message.contains("CROSSSLOT"));
}

#[tokio::test]
async fn cluster_shards_replica_reads_moved_ask_and_loop_protection() {
    let l_p1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l_r1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l_p2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p1_port = l_p1.local_addr().unwrap().port();
    let r1_port = l_r1.local_addr().unwrap().port();
    let p2_port = l_p2.local_addr().unwrap().port();

    let key_s1 = find_key_in_slot_range(0, 8191, "s1");
    let key_s2 = find_key_in_slot_range(8192, 16383, "s2");
    let slot_s1 = valkey_hash_slot(&key_s1);
    let slot_s2 = valkey_hash_slot(&key_s2);
    let key_moved_from_s1 = find_key_in_slot_range(0, 8191, "migrated");
    let moved_slot = valkey_hash_slot(&key_moved_from_s1);
    let key_ask = find_key_in_slot_range(0, 8191, "asking");
    let ask_slot = valkey_hash_slot(&key_ask);
    let key_loop = find_key_in_slot_range(0, 8191, "loop");
    let loop_slot = valkey_hash_slot(&key_loop);

    let r1_readonly_seen = Arc::new(AtomicBool::new(false));
    let r1_gets = Arc::new(AtomicUsize::new(0));
    let p2_asking_seen = Arc::new(AtomicBool::new(false));

    let s1_data = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));
    let s2_data = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

    // Primary 1 task
    let s1_data_srv = Arc::clone(&s1_data);
    let moved_key_clone = key_moved_from_s1.clone();
    let ask_key_clone = key_ask.clone();
    let loop_key_clone = key_loop.clone();
    let p1_task = tokio::spawn(async move {
        let shards_payload = build_cluster_shards_reply(p1_port, r1_port, p2_port);
        loop {
            let Ok((mut sock, _)) = l_p1.accept().await else {
                break;
            };
            let store = Arc::clone(&s1_data_srv);
            let moved_k = moved_key_clone.clone();
            let ask_k = ask_key_clone.clone();
            let loop_k = loop_key_clone.clone();
            let sp = shards_payload.clone();
            tokio::spawn(async move {
                let mut buf = BytesMut::new();
                while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
                    let op = parts[0].to_ascii_uppercase();
                    let reply = match op.as_str() {
                        "HELLO" => b"%1\r\n$5\r\nproto\r\n:3\r\n".to_vec(),
                        "CLIENT" => b"+OK\r\n".to_vec(),
                        "CLUSTER" => sp.clone(),
                        "SCAN" => {
                            let k_bytes = encode_bulk(&s1_key_sample(&store));
                            encode_array(&[encode_bulk(b"0"), encode_array(&[k_bytes])])
                        }
                        "UNLINK" => b":1\r\n".to_vec(),
                        "SET" => {
                            let kb = parts[1].as_bytes().to_vec();
                            if kb == moved_k {
                                format!("-MOVED {moved_slot} 127.0.0.1:{p2_port}\r\n").into_bytes()
                            } else if kb == ask_k {
                                format!("-ASK {ask_slot} 127.0.0.1:{p2_port}\r\n").into_bytes()
                            } else if kb == loop_k {
                                format!("-MOVED {loop_slot} 127.0.0.1:{p2_port}\r\n").into_bytes()
                            } else {
                                store.lock().insert(kb, parts[2].as_bytes().to_vec());
                                b"+OK\r\n".to_vec()
                            }
                        }
                        "GET" => {
                            let kb = parts[1].as_bytes();
                            match store.lock().get(kb) {
                                Some(v) => encode_bulk(v),
                                None => b"_\r\n".to_vec(),
                            }
                        }
                        _ => b"+OK\r\n".to_vec(),
                    };
                    if sock.write_all(&reply).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    // Replica 1 task
    let r1_ro_flag = Arc::clone(&r1_readonly_seen);
    let r1_gets_cnt = Arc::clone(&r1_gets);
    let s1_mirror = Arc::clone(&s1_data);
    let r1_task = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = l_r1.accept().await else {
                break;
            };
            let ro_flag = Arc::clone(&r1_ro_flag);
            let gets = Arc::clone(&r1_gets_cnt);
            let mirror = Arc::clone(&s1_mirror);
            tokio::spawn(async move {
                let mut buf = BytesMut::new();
                while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
                    let op = parts[0].to_ascii_uppercase();
                    let reply = match op.as_str() {
                        "HELLO" => b"%1\r\n$5\r\nproto\r\n:3\r\n".to_vec(),
                        "CLIENT" => b"+OK\r\n".to_vec(),
                        "READONLY" => {
                            ro_flag.store(true, Ordering::Release);
                            b"+OK\r\n".to_vec()
                        }
                        "GET" => {
                            gets.fetch_add(1, Ordering::SeqCst);
                            match mirror.lock().get(parts[1].as_bytes()) {
                                Some(v) => encode_bulk(v),
                                None => b"_\r\n".to_vec(),
                            }
                        }
                        _ => b"+OK\r\n".to_vec(),
                    };
                    if sock.write_all(&reply).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    // Primary 2 task
    let p2_ask_flag = Arc::clone(&p2_asking_seen);
    let s2_data_srv = Arc::clone(&s2_data);
    let loop_key_clone2 = key_loop.clone();
    let p2_task = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = l_p2.accept().await else {
                break;
            };
            let ask_flag = Arc::clone(&p2_ask_flag);
            let store = Arc::clone(&s2_data_srv);
            let loop_k = loop_key_clone2.clone();
            tokio::spawn(async move {
                let mut buf = BytesMut::new();
                while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
                    let op = parts[0].to_ascii_uppercase();
                    let reply = match op.as_str() {
                        "HELLO" => b"%1\r\n$5\r\nproto\r\n:3\r\n".to_vec(),
                        "CLIENT" => b"+OK\r\n".to_vec(),
                        "ASKING" => {
                            ask_flag.store(true, Ordering::Release);
                            b"+OK\r\n".to_vec()
                        }
                        "SCAN" => encode_array(&[encode_bulk(b"0"), encode_array(&[])]),
                        "UNLINK" => b":1\r\n".to_vec(),
                        "SET" => {
                            let kb = parts[1].as_bytes().to_vec();
                            if kb == loop_k {
                                format!("-MOVED {loop_slot} 127.0.0.1:{p1_port}\r\n").into_bytes()
                            } else {
                                store.lock().insert(kb, parts[2].as_bytes().to_vec());
                                b"+OK\r\n".to_vec()
                            }
                        }
                        "GET" => match store.lock().get(parts[1].as_bytes()) {
                            Some(v) => encode_bulk(v),
                            None => b"_\r\n".to_vec(),
                        },
                        _ => b"+OK\r\n".to_vec(),
                    };
                    if sock.write_all(&reply).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let cfg = ValkeyConfigInner::build(
        None,
        "127.0.0.1".to_string(),
        p1_port,
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
        None,
        1,
        8,
        5000,
        5000,
        3,
        true,
        vec![format!("127.0.0.1:{p1_port}")],
        true,
        None,
        Vec::new(),
        None,
        None,
        false,
        Vec::new(),
        128,
        None,
        None,
        false,
        None,
        true,
        true,
    )
    .unwrap();
    let engine = ValkeyEngine::new(Arc::new(cfg));

    // Write to Shard 1 primary & Shard 2 primary, then read from Replica 1 via READONLY.
    assert!(
        engine
            .set_bytes(key_s1.clone(), b"val1".to_vec(), None, None, false, false)
            .await
            .unwrap()
    );
    assert!(
        engine
            .set_bytes(key_s2.clone(), b"val2".to_vec(), None, None, false, false)
            .await
            .unwrap()
    );
    assert_eq!(
        engine.get_bytes(key_s1.clone()).await.unwrap().as_deref(),
        Some(b"val1".as_slice())
    );
    assert!(r1_readonly_seen.load(Ordering::Acquire));
    assert_eq!(r1_gets.load(Ordering::SeqCst), 1);

    // MOVED updates topology cleanly and writes to Primary 2.
    assert!(
        engine
            .set_bytes(
                key_moved_from_s1.clone(),
                b"migrated_ok".to_vec(),
                None,
                None,
                false,
                false
            )
            .await
            .unwrap()
    );
    assert_eq!(
        s2_data.lock().get(&key_moved_from_s1).map(Vec::as_slice),
        Some(b"migrated_ok".as_slice())
    );

    // ASK sends ASKING + SET to Primary 2.
    assert!(
        engine
            .set_bytes(
                key_ask.clone(),
                b"ask_ok".to_vec(),
                None,
                None,
                false,
                false
            )
            .await
            .unwrap()
    );
    assert!(p2_asking_seen.load(Ordering::Acquire));

    // Cross-slot DEL raises ProgrammingError (`CROSSSLOT`).
    let cross_err = engine
        .multi_key_int_cmd("DEL", vec![key_s1.clone(), key_s2.clone()], true)
        .await
        .unwrap_err();
    assert_eq!(cross_err.kind, ValkeyErrorKind::Programming);
    assert!(cross_err.message.contains("CROSSSLOT"));

    // Multi-shard cluster pipeline routes across Primary 1 & Primary 2 in one call.
    let multi_pipe = vec![
        QueuedCommand {
            args: vec![b"GET".to_vec(), key_s1.clone()],
            keys: vec![key_s1.clone()],
            read_only: true,
            decoder: PipelineDecoder::BytesOrNone,
        },
        QueuedCommand {
            args: vec![b"GET".to_vec(), key_s2.clone()],
            keys: vec![key_s2.clone()],
            read_only: true,
            decoder: PipelineDecoder::BytesOrNone,
        },
    ];
    let pipe_res = engine
        .execute_pipeline_commands(multi_pipe, false)
        .await
        .unwrap();
    assert_eq!(pipe_res[0].1.as_bytes(), Some(b"val1".as_slice()));
    assert_eq!(pipe_res[1].1.as_bytes(), Some(b"val2".as_slice()));

    // Ping-pong MOVED loop exceeds `max_redirections = 3` and raises transient OperationalError.
    assert_ne!(slot_s1, slot_s2);
    let loop_err = engine
        .set_bytes(key_loop, b"boom".to_vec(), None, None, false, false)
        .await
        .unwrap_err();
    assert_eq!(
        loop_err.kind,
        ValkeyErrorKind::Operational { is_transient: true }
    );
    assert!(loop_err.message.contains("redirection limit"));

    engine.close().await;
    p1_task.abort();
    r1_task.abort();
    p2_task.abort();
}

fn s1_key_sample(store: &Mutex<HashMap<Vec<u8>, Vec<u8>>>) -> Vec<u8> {
    store
        .lock()
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| b"s1:0".to_vec())
}

#[tokio::test]
async fn cluster_slots_fallback_when_cluster_shards_unsupported() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = BytesMut::new();
        while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
            let reply = if parts[0].eq_ignore_ascii_case("HELLO") {
                b"%1\r\n$5\r\nproto\r\n:3\r\n".to_vec()
            } else if parts[0].eq_ignore_ascii_case("CLIENT") {
                b"+OK\r\n".to_vec()
            } else if parts[0].eq_ignore_ascii_case("CLUSTER")
                && parts.get(1).map(|s| s.eq_ignore_ascii_case("SHARDS")) == Some(true)
            {
                b"-ERR unknown subcommand 'SHARDS'\r\n".to_vec()
            } else if parts[0].eq_ignore_ascii_case("CLUSTER") {
                encode_array(&[encode_array(&[
                    encode_int(0),
                    encode_int(16383),
                    encode_array(&[encode_bulk(b"127.0.0.1"), encode_int(i64::from(port))]),
                ])])
            } else {
                b"+OK\r\n".to_vec()
            };
            sock.write_all(&reply).await.unwrap();
        }
    });

    let cfg = ValkeyConfigInner::build(
        None,
        "127.0.0.1".to_string(),
        port,
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
        None,
        1,
        4,
        5000,
        5000,
        4,
        true,
        vec![format!("127.0.0.1:{port}")],
        false,
        None,
        Vec::new(),
        None,
        None,
        false,
        Vec::new(),
        128,
        None,
        None,
        false,
        None,
        true,
        true,
    )
    .unwrap();
    let engine = ValkeyEngine::new(Arc::new(cfg));
    let topo = engine.refresh_cluster_topology().await.unwrap();
    assert_eq!(topo.shard_count(), 1);
    assert!(
        engine
            .set_bytes(b"k".to_vec(), b"v".to_vec(), None, None, false, false)
            .await
            .unwrap()
    );
    engine.close().await;
    srv.abort();
}

#[tokio::test]
async fn sentinel_discovery_and_failover_promotion_on_readonly() {
    let l_sent = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l_old = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l_new = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sent_port = l_sent.local_addr().unwrap().port();
    let old_port = l_old.local_addr().unwrap().port();
    let new_port = l_new.local_addr().unwrap().port();

    let promoted = Arc::new(AtomicBool::new(false));
    let promoted_sent = Arc::clone(&promoted);
    let sentinel_task = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = l_sent.accept().await else {
                break;
            };
            let flag = Arc::clone(&promoted_sent);
            tokio::spawn(async move {
                let mut buf = BytesMut::new();
                while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
                    let reply = match parts[0].to_ascii_uppercase().as_str() {
                        "HELLO" => b"%1\r\n$5\r\nproto\r\n:3\r\n".to_vec(),
                        "SENTINEL" => {
                            let target_port = if flag.load(Ordering::Acquire) {
                                new_port
                            } else {
                                old_port
                            };
                            encode_array(&[
                                encode_bulk(b"127.0.0.1"),
                                encode_bulk(target_port.to_string().as_bytes()),
                            ])
                        }
                        _ => b"+OK\r\n".to_vec(),
                    };
                    if sock.write_all(&reply).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let promoted_old = Arc::clone(&promoted);
    let old_task = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = l_old.accept().await else {
                break;
            };
            let flag = Arc::clone(&promoted_old);
            tokio::spawn(async move {
                let mut buf = BytesMut::new();
                while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
                    let reply = match parts[0].to_ascii_uppercase().as_str() {
                        "HELLO" => b"%1\r\n$5\r\nproto\r\n:3\r\n".to_vec(),
                        "CLIENT" => b"+OK\r\n".to_vec(),
                        "ROLE" => encode_array(&[encode_bulk(b"master")]),
                        "PING" => b"+PONG\r\n".to_vec(),
                        "SET" => {
                            if flag.load(Ordering::Acquire) {
                                b"-READONLY You can't write against a read only replica.\r\n"
                                    .to_vec()
                            } else {
                                b"+OK\r\n".to_vec()
                            }
                        }
                        _ => b"+OK\r\n".to_vec(),
                    };
                    if sock.write_all(&reply).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let new_writes = Arc::new(AtomicUsize::new(0));
    let new_writes_srv = Arc::clone(&new_writes);
    let new_task = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = l_new.accept().await else {
                break;
            };
            let cnt = Arc::clone(&new_writes_srv);
            tokio::spawn(async move {
                let mut buf = BytesMut::new();
                while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
                    let reply = match parts[0].to_ascii_uppercase().as_str() {
                        "HELLO" => b"%1\r\n$5\r\nproto\r\n:3\r\n".to_vec(),
                        "CLIENT" => b"+OK\r\n".to_vec(),
                        "ROLE" => encode_array(&[encode_bulk(b"master")]),
                        "SET" => {
                            cnt.fetch_add(1, Ordering::SeqCst);
                            b"+OK\r\n".to_vec()
                        }
                        _ => b"+OK\r\n".to_vec(),
                    };
                    if sock.write_all(&reply).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let cfg = ValkeyConfigInner::build(
        None,
        "127.0.0.1".to_string(),
        old_port,
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
        None,
        1,
        4,
        5000,
        5000,
        4,
        false,
        Vec::new(),
        false,
        Some("mymaster".to_string()),
        vec![format!("127.0.0.1:{sent_port}")],
        None,
        None,
        false,
        Vec::new(),
        128,
        None,
        None,
        false,
        None,
        true,
        true,
    )
    .unwrap();
    let engine = ValkeyEngine::new(Arc::new(cfg));

    // Initial write goes to old primary.
    assert!(
        engine
            .set_bytes(b"s:k".to_vec(), b"v1".to_vec(), None, None, false, false)
            .await
            .unwrap()
    );
    assert_eq!(new_writes.load(Ordering::SeqCst), 0);

    // Promote new primary; old primary returns -READONLY -> engine invalidates primary cache,
    // re-queries Sentinel, verifies ROLE=master on new primary, and transparently succeeds.
    promoted.store(true, Ordering::Release);
    assert!(
        engine
            .set_bytes(b"s:k".to_vec(), b"v2".to_vec(), None, None, false, false)
            .await
            .unwrap()
    );
    assert_eq!(new_writes.load(Ordering::SeqCst), 1);

    engine.close().await;
    sentinel_task.abort();
    old_task.abort();
    new_task.abort();
}
