//! Integration and loopback RESP3 tests for `_valkey`: config URLs, Rustls TLS/mTLS,
//! Unix domain sockets, Memorystore GCP IAM token refresh, RESP3/RESP2 handshake,
//! Valkey 9.0 hash-field expirations, Lua `EVALSHA -> NOSCRIPT -> EVAL`, `CLIENT TRACKING`
//! push invalidation, Streams/ZSET, pipelines, and Pub/Sub listeners.

use std::collections::HashMap;
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use _valkey::client::{PipelineDecoder, QueuedCommand, ValkeyEngine};
use _valkey::codec::{
    RespValue, build_hash_expire_cmd, build_hash_field_query_cmd, decode_int_list, sha1_hex,
    try_decode_frame,
};
use _valkey::config::{GcpAuthSource, ValkeyConfigInner, ValkeyTarget};
use _valkey::error::{ValkeyError, ValkeyErrorKind, classify_server_error};
use _valkey::pubsub::ValkeyListenerCore;
use _valkey::streams::{build_xack, build_xadd, build_xautoclaim, build_xgroup_create, build_zadd};
use _valkey::{ensure_rustls_provider, parse_cluster_redirect, valkey_hash_slot};
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio_rustls::TlsAcceptor;
use vertebra_core::gcp_auth::{
    DEFAULT_REFRESH_SKEW_MS, GcpAuthSource as CoreGcpAuthSource, GcpTokenCache,
    SharedGcpTokenCacheHandle,
};

const TEST_CA_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIBljCCAT2gAwIBAgIUHuQgf5iQ9FM/lxO0LVML2VcqtiAwCgYIKoZIzj0EAwIw
GTEXMBUGA1UEAwwOVmVydGVicmFUZXN0Q0EwHhcNMjYxMTE2MDAwNjMzWhcNMzYx
MTEzMDAwNjMzWjAZMRcwFQYDVQQDDA5WZXJ0ZWJyYVRlc3RDQTBZMBMGByqGSM49
AgEGCCqGSM49AwEHA0IABDoEZGaRvSmFMxQvGDPCXeN82EObXPOPaOOcDRyZijt6
vBkCO+PQKxLisfpktuBPK4NHoO+rLeDNwC1tU1ix9HCjYzBhMB0GA1UdDgQWBBRP
+Txm917jAeduXwEKYDwuecyDUDAfBgNVHSMEGDAWgBRP+Txm917jAeduXwEKYDwu
ecyDUDAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBBjAKBggqhkjOPQQD
AgNHADBEAiAVDKAt13UwTdLzjiqNRgBgKtQ6yQey+hNEEiCkL0FYDQIgElW6wp83
JQgm7HqjIgWy+hsKCUBsSOezb2gsx5yBurs=
-----END CERTIFICATE-----";

const TEST_SRV_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIBojCCAUigAwIBAgIUOZSID7EJ4YYxP+sl7pMytvEKtwcwCgYIKoZIzj0EAwIw
GTEXMBUGA1UEAwwOVmVydGVicmFUZXN0Q0EwHhcNMjYxMTE2MDAwNjMzWhcNMzYx
MTEzMDAwNjMzWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggq
hkjOPQMBBwNCAAR+3xpOplNI8S73HBXJVGr9zTwS7hoAa2Veq8y6hMl9NeaIIxIw
KQHI57+dHmeOxcNcawG+vlTgdxXw1FmYqqOHo3MwcTAaBgNVHREEEzARgglsb2Nh
bGhvc3SHBH8AAAEwEwYDVR0lBAwwCgYIKwYBBQUHAwEwHQYDVR0OBBYEFF8HSnkB
szAmH8o9n5Jo9AgVDT6GMB8GA1UdIwQYMBaAFE/5PGb3XuMB525fAQpgPC55zINQ
MAoGCCqGSM49BAMCA0gAMEUCIBUm/3LrhunB7dp9sFbGCavROX7EmCAalqIbAUh6
JGCDAiEAsRPbV5+OM4RJdqfmDW8GBBJGuGpv9xWhm3OLviOPcqI=
-----END CERTIFICATE-----";

const TEST_SRV_KEY_PEM: &str = "\
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIEfN2P+Etrmde197DBCW3+jN7aoq1Zhj0OQ5zjlqzpZOoAoGCCqGSM49
AwEHoUQDQgAEft8aTqZTSPEu9xwVyVRq/c08Eu4aAGtlXqvMuoTJfTXmiCMSMCkB
yOe/nR5njsXDXGsBvr5U4HcV8NRZmKqjhw==
-----END EC PRIVATE KEY-----";

const TEST_CLI_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIBijCCATCgAwIBAgIUOZSID7EJ4YYxP+sl7pMytvEKtwgwCgYIKoZIzj0EAwIw
GTEXMBUGA1UEAwwOVmVydGVicmFUZXN0Q0EwHhcNMjYxMTE2MDAwNjMzWhcNMzYx
MTEzMDAwNjMzWjAYMRYwFAYDVQQDDA12YWxrZXktY2xpZW50MFkwEwYHKoZIzj0C
AQYIKoZIzj0DAQcDQgAEEWAyo9G7sDShUCQCJNfPTbuL/OR5GK6nzKuUSnjxHo0U
iixK62vyZS/UUuHUIma9r1Enp0/0Qou55w/ywyzyjKNXMFUwEwYDVR0lBAwwCgYI
KwYBBQUHAwIwHQYDVR0OBBYEFKfZNLKoj2etsfKEyGgZrLgJ5Sb6MB8GA1UdIwQY
MBaAFE/5PGb3XuMB525fAQpgPC55zINQMAoGCCqGSM49BAMCA0gAMEUCIQDUAxW2
CSFh4v6M3n0IcAR56j6xLLucG0l402VcT7knowIgca1JzunLHdThxgR66IS7VAzF
bBOwQDAiOlWHme1U1Mg=
-----END CERTIFICATE-----";

const TEST_CLI_KEY_PEM: &str = "\
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEID8GDPODELUEH3eKIepeFdWmnHV4OqDuzPw+b2laafB2oAoGCCqGSM49
AwEHoUQDQgAEEWAyo9G7sDShUCQCJNfPTbuL/OR5GK6nzKuUSnjxHo0UiixK62vy
ZS/UUuHUIma9r1Enp0/0Qou55w/ywyzyjA==
-----END EC PRIVATE KEY-----";

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

fn encode_push(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!(">{}\r\n", items.len()).into_bytes();
    for item in items {
        out.extend_from_slice(item);
    }
    out
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

fn make_base_config(host: &str, port: u16) -> ValkeyConfigInner {
    ValkeyConfigInner::from_url_str(&format!("valkey://{host}:{port}/0"))
        .expect("valid base config")
}

#[test]
fn url_validation_and_scheme_enforcement() {
    let c1 =
        ValkeyConfigInner::from_url_str("valkey://alice:s3cr3t@cache.internal:6381/3").unwrap();
    assert!(!c1.tls);
    assert_eq!(c1.host, "cache.internal");
    assert_eq!(c1.port, 6381);
    assert_eq!(c1.db, 3);
    assert_eq!(c1.username.as_deref(), Some("alice"));
    assert_eq!(c1.password.as_deref(), Some("s3cr3t"));
    assert!(!c1.url_redacted.contains("s3cr3t"));
    assert!(c1.url_redacted.contains("***"));

    let c2 = ValkeyConfigInner::from_url_str("valkeys://secure.internal/1").unwrap();
    assert!(c2.tls);
    assert_eq!(c2.port, 6379);
    assert_eq!(c2.db, 1);

    let c3 = ValkeyConfigInner::from_url_str("unix:///var/run/valkey.sock?db=4").unwrap();
    assert_eq!(
        c3.target,
        ValkeyTarget::Unix {
            path: PathBuf::from("/var/run/valkey.sock")
        }
    );
    assert_eq!(c3.db, 4);

    for bad_url in [
        "memory://localhost/0",
        "redis://127.0.0.1:6379/0",
        "rediss://127.0.0.1:6380/0",
        "postgresql://user:pass@127.0.0.1:5432/db",
        "valkey:///0",
        "unix://",
        "not a url",
    ] {
        let err = ValkeyConfigInner::from_url_str(bad_url)
            .expect_err(&format!("expected failure for {bad_url}"));
        assert_eq!(err.kind, ValkeyErrorKind::ImproperConfiguration);
        assert!(!err.kind.is_transient());
    }
}

#[test]
fn error_classification_maps_transient_and_programming_errors() {
    assert_eq!(
        classify_server_error("WRONGPASS invalid username-password pair"),
        ValkeyErrorKind::Operational {
            is_transient: false
        }
    );
    assert_eq!(
        classify_server_error("NOAUTH Authentication required."),
        ValkeyErrorKind::Operational {
            is_transient: false
        }
    );
    for code in [
        "READONLY",
        "MASTERDOWN",
        "CLUSTERDOWN",
        "LOADING",
        "TRYAGAIN",
    ] {
        let kind = classify_server_error(&format!("{code} node unavailable"));
        assert_eq!(kind, ValkeyErrorKind::Operational { is_transient: true });
        assert!(kind.is_transient());
    }
    for msg in [
        "WRONGTYPE Operation against a key holding the wrong kind of value",
        "BUSYKEY Target key name already exists.",
        "BUSYGROUP Consumer Group name already exists",
        "CROSSSLOT Keys in request don't hash to the same slot",
        "NOSCRIPT No matching script. Please use EVAL.",
        "ERR syntax error",
        "ERR wrong number of arguments for 'hexpire' command",
    ] {
        let kind = classify_server_error(msg);
        assert_eq!(kind, ValkeyErrorKind::Programming);
        assert!(!kind.is_transient());
    }
    assert_eq!(
        classify_server_error("ERR value is not an integer or out of range"),
        ValkeyErrorKind::Data
    );
    assert!(ValkeyError::connection("eof").kind.is_transient());
}

#[test]
fn resp3_decode_all_types_and_sha1_rfc3174() {
    assert_eq!(sha1_hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
    assert_eq!(
        sha1_hex(b"return 1"),
        "e0e1f9fabfc9d4800c877a703b823ac0578ff8db"
    );

    let mut raw = BytesMut::from(
        "+OK\r\n-WRONGPASS bad\r\n:42\r\n,3.5\r\n#t\r\n_\r\n(99999999999999999999999\r\n\
         $5\r\nhello\r\n!7\r\nERR bad\r\n=9\r\ntxt:world\r\n\
         %1\r\n$1\r\na\r\n:7\r\n~2\r\n$1\r\nx\r\n$1\r\ny\r\n\
         >2\r\n$10\r\ninvalidate\r\n*1\r\n$2\r\nk1\r\n\
         |1\r\n$3\r\nttl\r\n:5\r\n$3\r\nval\r\n",
    );

    assert_eq!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::SimpleString(Bytes::from_static(b"OK")))
    );
    assert_eq!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::SimpleError("WRONGPASS bad".to_string()))
    );
    assert_eq!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::Integer(42))
    );
    assert_eq!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::Double(3.5))
    );
    assert_eq!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::Boolean(true))
    );
    assert_eq!(try_decode_frame(&mut raw).unwrap(), Some(RespValue::Null));
    assert_eq!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::BigNumber("99999999999999999999999".to_string()))
    );
    assert_eq!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::BulkString(Bytes::from_static(b"hello")))
    );
    assert_eq!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::BulkError("ERR bad".to_string()))
    );
    assert_eq!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::VerbatimString {
            format: *b"txt",
            data: Bytes::from_static(b"world"),
        })
    );
    assert!(matches!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::Map(_))
    ));
    assert!(matches!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::Set(_))
    ));
    assert!(matches!(
        try_decode_frame(&mut raw).unwrap(),
        Some(RespValue::Push(_))
    ));
    let attr = try_decode_frame(&mut raw).unwrap().unwrap();
    assert_eq!(attr.as_str().as_deref(), Some("val"));
    assert!(raw.is_empty());

    assert_eq!(valkey_hash_slot(b"{user:1}:a"), valkey_hash_slot(b"user:1"));
    assert_eq!(
        parse_cluster_redirect("MOVED 3999 10.0.0.2:6380"),
        Some(("MOVED".to_string(), 3999, "10.0.0.2".to_string(), 6380))
    );
    assert_eq!(
        parse_cluster_redirect("ASK 12 [::1]:6381"),
        Some(("ASK".to_string(), 12, "::1".to_string(), 6381))
    );
}

#[test]
fn valkey_9_hash_field_ttl_and_stream_command_builders() {
    let exp = build_hash_expire_cmd(
        "HEXPIRE",
        b"hkey",
        120,
        Some("nx"),
        &[b"f1".to_vec(), b"f2".to_vec()],
    )
    .unwrap();
    assert_eq!(
        exp,
        vec![
            b"HEXPIRE".to_vec(),
            b"hkey".to_vec(),
            b"120".to_vec(),
            b"NX".to_vec(),
            b"FIELDS".to_vec(),
            b"2".to_vec(),
            b"f1".to_vec(),
            b"f2".to_vec(),
        ]
    );
    let q = build_hash_field_query_cmd("HPTTL", b"hkey", &[b"f1".to_vec()]).unwrap();
    assert_eq!(
        q,
        vec![
            b"HPTTL".to_vec(),
            b"hkey".to_vec(),
            b"FIELDS".to_vec(),
            b"1".to_vec(),
            b"f1".to_vec(),
        ]
    );
    let bad =
        build_hash_expire_cmd("HEXPIRE", b"k", 10, Some("INVALID"), &[b"f".to_vec()]).unwrap_err();
    assert_eq!(bad.kind, ValkeyErrorKind::Programming);

    let decoded = decode_int_list(RespValue::Array(vec![
        RespValue::Integer(1),
        RespValue::Integer(-2),
    ]))
    .unwrap();
    assert_eq!(decoded, vec![1, -2]);

    assert_eq!(
        build_xgroup_create(b"s", "g1", "$", true),
        vec![
            b"XGROUP".to_vec(),
            b"CREATE".to_vec(),
            b"s".to_vec(),
            b"g1".to_vec(),
            b"$".to_vec(),
            b"MKSTREAM".to_vec(),
        ]
    );
    assert!(
        build_xadd(
            b"s",
            &[(b"a".to_vec(), b"1".to_vec())],
            "*",
            Some(50),
            true,
            false
        )
        .is_ok()
    );
    assert!(build_xack(b"s", "g", &[]).is_err());
    assert!(!build_xautoclaim(b"s", "g", "c", 1000, "0-0", Some(10), true).is_empty());
    assert!(
        build_zadd(
            b"z",
            &[(b"m".to_vec(), 1.5)],
            true,
            true,
            false,
            false,
            false
        )
        .is_err()
    );
}

#[tokio::test]
async fn live_resp3_handshake_tracking_lua_hash_ttl_and_pipelines() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen_cmds: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let push_invalidate_on_second_conn = Arc::new(AtomicBool::new(false));
    let get_calls = Arc::new(AtomicUsize::new(0));

    let seen_srv = Arc::clone(&seen_cmds);
    let get_calls_srv = Arc::clone(&get_calls);
    let push_flag = Arc::clone(&push_invalidate_on_second_conn);

    let srv_task = tokio::spawn(async move {
        let mut store: HashMap<String, Vec<u8>> = HashMap::new();
        let mut loaded_scripts: HashMap<String, String> = HashMap::new();
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let mut buf = BytesMut::new();
            while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
                seen_srv.lock().push(parts.clone());
                let op = parts
                    .first()
                    .map(|s| s.to_ascii_uppercase())
                    .unwrap_or_default();
                let reply: Vec<u8> = match op.as_str() {
                    "HELLO" => b"%1\r\n$5\r\nproto\r\n:3\r\n".to_vec(),
                    "CLIENT" => b"+OK\r\n".to_vec(),
                    "SELECT" | "PING" => b"+PONG\r\n".to_vec(),
                    "SET" => {
                        let k = parts.get(1).cloned().unwrap_or_default();
                        let v = parts.get(2).cloned().unwrap_or_default().into_bytes();
                        store.insert(k, v);
                        b"+OK\r\n".to_vec()
                    }
                    "GET" => {
                        let c = get_calls_srv.fetch_add(1, Ordering::SeqCst);
                        let k = parts.get(1).cloned().unwrap_or_default();
                        let val = if push_flag.load(Ordering::Acquire) && c >= 1 {
                            b"fresh_v2".to_vec()
                        } else {
                            store.get(&k).cloned().unwrap_or_default()
                        };
                        if val.is_empty() && !store.contains_key(&k) {
                            b"_\r\n".to_vec()
                        } else {
                            let mut out = encode_bulk(&val);
                            if c == 0 {
                                // Emit an immediate RESP3 tracking push frame right after replying or
                                // wait until explicit push in test_tracking_push below.
                                let _ = c;
                            }
                            let _ = &mut out;
                            out
                        }
                    }
                    "HEXPIRE" => b"*2\r\n:1\r\n:1\r\n".to_vec(),
                    "HTTL" => b"*2\r\n:59\r\n:59\r\n".to_vec(),
                    "HPERSIST" => b"*1\r\n:1\r\n".to_vec(),
                    "EVALSHA" => {
                        let sha = parts.get(1).cloned().unwrap_or_default();
                        if let Some(script) = loaded_scripts.get(&sha) {
                            encode_bulk(script.as_bytes())
                        } else {
                            b"-NOSCRIPT No matching script. Please use EVAL.\r\n".to_vec()
                        }
                    }
                    "EVAL" => {
                        let script = parts.get(1).cloned().unwrap_or_default();
                        loaded_scripts.insert(sha1_hex(script.as_bytes()), script.clone());
                        b":123\r\n".to_vec()
                    }
                    "MULTI" => b"+OK\r\n".to_vec(),
                    "INCRBY" => b":2\r\n".to_vec(),
                    "EXEC" => b"*2\r\n+OK\r\n$3\r\nbar\r\n".to_vec(),
                    _ => b"+OK\r\n".to_vec(),
                };
                if sock.write_all(&reply).await.is_err() {
                    break;
                }
            }
        }
    });

    let mut cfg = make_base_config("127.0.0.1", addr.port());
    cfg.client_tracking_enabled = true;
    cfg.client_tracking_prefixes = vec!["app:".to_string()];
    let engine = ValkeyEngine::new(Arc::new(cfg));

    assert!(engine.ping().await.unwrap());
    assert!(
        engine
            .set_bytes(
                b"app:k1".to_vec(),
                b"v1".to_vec(),
                Some(5000),
                None,
                false,
                false
            )
            .await
            .unwrap()
    );

    // Write-through SET populates L1 tracking cache directly (`get_calls` stays 0).
    let v1 = engine.get_bytes(b"app:k1".to_vec()).await.unwrap();
    assert_eq!(v1.as_deref(), Some(b"v1".as_slice()));
    assert_eq!(get_calls.load(Ordering::SeqCst), 0);
    assert_eq!(engine.tracking_stats().0, 1);

    // Cold server key warms L1 tracking cache on first GET (`get_calls` -> 1) and serves second GET from L1.
    engine
        .execute_command_routed(
            vec![b"SET".to_vec(), b"app:cold".to_vec(), b"vcold".to_vec()],
            &[b"app:cold"],
            false,
        )
        .await
        .unwrap();
    let v_cold = engine.get_bytes(b"app:cold".to_vec()).await.unwrap();
    assert_eq!(v_cold.as_deref(), Some(b"vcold".as_slice()));
    assert_eq!(get_calls.load(Ordering::SeqCst), 1);

    let v_cold_cached = engine.get_bytes(b"app:cold".to_vec()).await.unwrap();
    assert_eq!(v_cold_cached.as_deref(), Some(b"vcold".as_slice()));
    assert_eq!(get_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.tracking_stats().0, 2);

    // Lua EVALSHA -> NOSCRIPT -> EVAL fallback.
    let lua_res = engine
        .eval_with_fallback(b"return 123".to_vec(), vec![b"app:k1".to_vec()], vec![])
        .await
        .unwrap();
    assert_eq!(lua_res.as_i64(), Some(123));

    // Valkey 9.0 HEXPIRE / HTTL / HPERSIST over live RESP3 loopback.
    let hexp_cmd = build_hash_expire_cmd(
        "HEXPIRE",
        b"app:h",
        60,
        Some("NX"),
        &[b"f1".to_vec(), b"f2".to_vec()],
    )
    .unwrap();
    let hexp_rep = engine
        .execute_command_routed(hexp_cmd, &[b"app:h"], false)
        .await
        .unwrap();
    assert_eq!(decode_int_list(hexp_rep).unwrap(), vec![1, 1]);

    // Pipeline batching (non-transactional & MULTI/EXEC).
    let pipe_cmds = vec![
        QueuedCommand {
            args: vec![b"SET".to_vec(), b"p:1".to_vec(), b"bar".to_vec()],
            keys: vec![b"p:1".to_vec()],
            read_only: false,
            decoder: PipelineDecoder::BoolOk,
        },
        QueuedCommand {
            args: vec![b"GET".to_vec(), b"p:1".to_vec()],
            keys: vec![b"p:1".to_vec()],
            read_only: true,
            decoder: PipelineDecoder::BytesOrNone,
        },
    ];
    let batch_res = engine
        .execute_pipeline_commands(pipe_cmds.clone(), false)
        .await
        .unwrap();
    assert_eq!(batch_res.len(), 2);
    assert_eq!(batch_res[0].1.as_bool_ok(), Some(true));
    assert_eq!(batch_res[1].1.as_bytes(), Some(b"bar".as_slice()));

    let txn_res = engine
        .execute_pipeline_commands(pipe_cmds, true)
        .await
        .unwrap();
    assert_eq!(txn_res[1].1.as_bytes(), Some(b"bar".as_slice()));

    // Verify HELLO 3 + CLIENT CAPA redirect + CLIENT TRACKING ON PREFIX app: BCAST were sent.
    let recorded = seen_cmds.lock().clone();
    assert!(
        recorded
            .iter()
            .any(|c| c.first().map(String::as_str) == Some("HELLO")
                && c.get(1).map(String::as_str) == Some("3"))
    );
    assert!(
        recorded
            .iter()
            .any(|c| c == &["CLIENT", "CAPA", "redirect"])
    );
    assert!(
        recorded
            .iter()
            .any(|c| c == &["CLIENT", "TRACKING", "ON", "PREFIX", "app:", "BCAST"])
    );
    assert!(
        recorded
            .iter()
            .any(|c| c.first().map(String::as_str) == Some("EVALSHA"))
    );
    assert!(
        recorded
            .iter()
            .any(|c| c.first().map(String::as_str) == Some("EVAL"))
    );

    engine.close().await;
    srv_task.abort();
}

#[tokio::test]
async fn client_tracking_resp3_push_invalidate_evicts_l1_cache() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let gets = Arc::new(AtomicUsize::new(0));
    let gets_srv = Arc::clone(&gets);
    let (inv_tx, inv_rx) = tokio::sync::oneshot::channel::<()>();
    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = sock.set_nodelay(true);
        let mut buf = BytesMut::new();
        let mut inv_tx_opt = Some(inv_tx);
        while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
            let op = parts[0].to_ascii_uppercase();
            if op == "HELLO" {
                sock.write_all(b"%1\r\n$5\r\nproto\r\n:3\r\n")
                    .await
                    .unwrap();
            } else if op == "CLIENT" {
                sock.write_all(b"+OK\r\n").await.unwrap();
            } else if op == "GET" {
                let n = gets_srv.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    sock.write_all(&encode_bulk(b"old")).await.unwrap();
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    let push = encode_push(&[
                        encode_bulk(b"invalidate"),
                        encode_array(&[encode_bulk(b"app:x")]),
                    ]);
                    sock.write_all(&push).await.unwrap();
                    sock.flush().await.unwrap();
                    if let Some(tx) = inv_tx_opt.take() {
                        let _ = tx.send(());
                    }
                } else {
                    sock.write_all(&encode_bulk(b"new")).await.unwrap();
                }
            }
        }
    });

    let mut cfg = make_base_config("127.0.0.1", addr.port());
    cfg.client_tracking_enabled = true;
    cfg.pool_max_size = 1;
    let engine = ValkeyEngine::new(Arc::new(cfg));
    assert_eq!(
        engine
            .get_bytes(b"app:x".to_vec())
            .await
            .unwrap()
            .as_deref(),
        Some(b"old".as_slice())
    );
    inv_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(
        engine
            .get_bytes(b"app:x".to_vec())
            .await
            .unwrap()
            .as_deref(),
        Some(b"new".as_slice())
    );
    assert!(engine.tracking_stats().2 >= 1);
    assert_eq!(gets.load(Ordering::SeqCst), 2);
    engine.close().await;
    srv.abort();
}

#[tokio::test]
async fn resp2_fallback_when_hello_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cmds = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
    let cmds_srv = Arc::clone(&cmds);

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = sock.set_nodelay(true);
        let mut buf = BytesMut::new();
        while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
            cmds_srv.lock().push(parts.clone());
            let op = parts[0].to_ascii_uppercase();
            let reply = match op.as_str() {
                "HELLO" => b"-ERR unknown command 'HELLO'\r\n".to_vec(),
                "AUTH" => b"+OK\r\n".to_vec(),
                "CLIENT" if parts.get(1).map(|s| s.eq_ignore_ascii_case("CAPA")) == Some(true) => {
                    b"-ERR syntax error\r\n".to_vec()
                }
                "CLIENT" => b"+OK\r\n".to_vec(),
                "SELECT" => b"+OK\r\n".to_vec(),
                "PING" => b"+PONG\r\n".to_vec(),
                _ => b"+OK\r\n".to_vec(),
            };
            sock.write_all(&reply).await.unwrap();
        }
    });

    let cfg =
        ValkeyConfigInner::from_url_str(&format!("valkey://u1:pw1@127.0.0.1:{}/2", addr.port()))
            .unwrap();
    let engine = ValkeyEngine::new(Arc::new(cfg));
    assert!(engine.ping().await.unwrap());

    let seen = cmds.lock().clone();
    assert!(seen.iter().any(|c| c == &["AUTH", "u1", "pw1"]));
    assert!(seen.iter().any(|c| c == &["SELECT", "2"]));
    engine.close().await;
    srv.abort();
}

#[tokio::test]
async fn rustls_tls_and_mtls_loopback_handshake() {
    ensure_rustls_provider();

    let mut ca_reader = Cursor::new(TEST_CA_PEM.as_bytes());
    let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_reader)
        .collect::<Result<_, _>>()
        .unwrap();
    let mut roots = RootCertStore::empty();
    let _ = roots.add_parsable_certificates(ca_certs);
    let client_verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();

    let srv_certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut Cursor::new(TEST_SRV_CERT_PEM.as_bytes()))
            .collect::<Result<_, _>>()
            .unwrap();
    let srv_key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut Cursor::new(TEST_SRV_KEY_PEM.as_bytes()))
            .unwrap()
            .unwrap();

    let server_cfg = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(srv_certs, srv_key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut tls_sock = acceptor.accept(tcp).await.unwrap();
        let mut buf = BytesMut::new();
        while let Some(parts) = read_next_cmd(&mut tls_sock, &mut buf).await {
            let reply = match parts[0].to_ascii_uppercase().as_str() {
                "HELLO" => b"%1\r\n$5\r\nproto\r\n:3\r\n".to_vec(),
                "CLIENT" => b"+OK\r\n".to_vec(),
                "PING" => b"+PONG\r\n".to_vec(),
                "GET" => encode_bulk(b"mtls_secret"),
                _ => b"+OK\r\n".to_vec(),
            };
            tls_sock.write_all(&reply).await.unwrap();
        }
    });

    let cfg = ValkeyConfigInner::build(
        None,
        "127.0.0.1".to_string(),
        addr.port(),
        0,
        None,
        None,
        Some(true),
        Some(TEST_CA_PEM.to_string()),
        Some(TEST_CLI_CERT_PEM.to_string()),
        Some(TEST_CLI_KEY_PEM.to_string()),
        Some("localhost".to_string()),
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
    assert!(engine.ping().await.unwrap());
    assert_eq!(
        engine.get_bytes(b"k".to_vec()).await.unwrap().as_deref(),
        Some(b"mtls_secret".as_slice())
    );
    engine.close().await;
    srv.abort();
}

#[tokio::test]
async fn gcp_memorystore_iam_token_refresh_and_wrongpass_invalidation() {
    let oauth_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let oauth_addr = oauth_listener.local_addr().unwrap();
    let oauth_calls = Arc::new(AtomicUsize::new(0));
    let oauth_calls_srv = Arc::clone(&oauth_calls);

    let oauth_srv = tokio::spawn(async move {
        for idx in 1..=2usize {
            let (mut sock, _) = oauth_listener.accept().await.unwrap();
            let mut buf = [0u8; 2048];
            let _ = sock.read(&mut buf).await;
            oauth_calls_srv.fetch_add(1, Ordering::SeqCst);
            let body = format!(
                "{{\"access_token\":\"iam-tok-{idx}\",\"expires_in\":3600,\"token_type\":\"Bearer\"}}"
            );
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
        }
    });

    let valkey_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let valkey_addr: SocketAddr = valkey_listener.local_addr().unwrap();
    let auth_attempts = Arc::new(AtomicUsize::new(0));
    let auth_srv = Arc::clone(&auth_attempts);

    let valkey_srv = tokio::spawn(async move {
        let (mut sock, _) = valkey_listener.accept().await.unwrap();
        let mut buf = BytesMut::new();
        while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
            let op = parts[0].to_ascii_uppercase();
            if op == "HELLO" {
                let n = auth_srv.fetch_add(1, Ordering::SeqCst);
                let token = parts.last().map(String::as_str).unwrap_or_default();
                if n == 0 && token == "iam-tok-1" {
                    sock.write_all(b"-WRONGPASS stale iam token\r\n")
                        .await
                        .unwrap();
                } else if token == "iam-tok-2" {
                    sock.write_all(b"%1\r\n$5\r\nproto\r\n:3\r\n")
                        .await
                        .unwrap();
                } else {
                    sock.write_all(b"-WRONGPASS unexpected token\r\n")
                        .await
                        .unwrap();
                }
            } else if op == "CLIENT" {
                sock.write_all(b"+OK\r\n").await.unwrap();
            } else if op == "PING" {
                sock.write_all(b"+PONG\r\n").await.unwrap();
            }
        }
    });

    let token_cache = Arc::new(GcpTokenCache::new(
        CoreGcpAuthSource::AuthorizedUser {
            client_id: "cid".to_string(),
            client_secret: "csec".to_string(),
            refresh_token: "rtok".to_string(),
            quota_project_id: None,
            token_uri: format!("http://{oauth_addr}/token"),
        },
        DEFAULT_REFRESH_SKEW_MS,
    ));
    let handle = SharedGcpTokenCacheHandle::from_arc(token_cache);

    let cfg = ValkeyConfigInner::build(
        None,
        "127.0.0.1".to_string(),
        valkey_addr.port(),
        0,
        Some("iam-sa".to_string()),
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
        None,
        Vec::new(),
        None,
        None,
        false,
        Vec::new(),
        128,
        None,
        None,
        true,
        Some(GcpAuthSource::Handle(handle)),
        true,
        true,
    )
    .unwrap();

    let engine = ValkeyEngine::new(Arc::new(cfg));
    assert!(engine.ping().await.unwrap());
    assert_eq!(oauth_calls.load(Ordering::SeqCst), 2);
    assert_eq!(auth_attempts.load(Ordering::SeqCst), 2);
    engine.close().await;
    oauth_srv.await.unwrap();
    valkey_srv.abort();
}

#[tokio::test]
async fn pubsub_listener_subscribe_psubscribe_ssubscribe_and_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = sock.set_nodelay(true);
        let mut buf = BytesMut::new();
        while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
            let op = parts[0].to_ascii_uppercase();
            match op.as_str() {
                "HELLO" => {
                    sock.write_all(b"%1\r\n$5\r\nproto\r\n:3\r\n")
                        .await
                        .unwrap();
                }
                "CLIENT" => {
                    sock.write_all(b"+OK\r\n").await.unwrap();
                }
                "SUBSCRIBE" => {
                    let ack = encode_push(&[
                        encode_bulk(b"subscribe"),
                        encode_bulk(parts[1].as_bytes()),
                        b":1\r\n".to_vec(),
                    ]);
                    sock.write_all(&ack).await.unwrap();
                    let msg = encode_push(&[
                        encode_bulk(b"message"),
                        encode_bulk(b"events"),
                        encode_bulk(b"payload-1"),
                    ]);
                    sock.write_all(&msg).await.unwrap();
                }
                "PSUBSCRIBE" => {
                    let ack = encode_push(&[
                        encode_bulk(b"psubscribe"),
                        encode_bulk(parts[1].as_bytes()),
                        b":2\r\n".to_vec(),
                    ]);
                    sock.write_all(&ack).await.unwrap();
                    let msg = encode_push(&[
                        encode_bulk(b"pmessage"),
                        encode_bulk(b"jobs.*"),
                        encode_bulk(b"jobs.run"),
                        encode_bulk(b"payload-2"),
                    ]);
                    sock.write_all(&msg).await.unwrap();
                }
                "SSUBSCRIBE" => {
                    let ack = encode_push(&[
                        encode_bulk(b"ssubscribe"),
                        encode_bulk(parts[1].as_bytes()),
                        b":3\r\n".to_vec(),
                    ]);
                    sock.write_all(&ack).await.unwrap();
                    let msg = encode_push(&[
                        encode_bulk(b"smessage"),
                        encode_bulk(b"shard:{1}"),
                        encode_bulk(b"payload-3"),
                    ]);
                    sock.write_all(&msg).await.unwrap();
                }
                _ => {
                    sock.write_all(b"+OK\r\n").await.unwrap();
                }
            }
        }
    });

    let core = ValkeyListenerCore::new(Arc::new(make_base_config("127.0.0.1", addr.port())));
    core.subscribe(&["events".to_string()]).await.unwrap();
    let m1 = core.next_message(Some(1000)).await.unwrap().unwrap();
    assert_eq!(m1.msg_type, "message");
    assert_eq!(m1.channel, "events");
    assert_eq!(m1.data.as_ref(), b"payload-1");

    core.psubscribe(&["jobs.*".to_string()]).await.unwrap();
    let m2 = core.next_message(Some(1000)).await.unwrap().unwrap();
    assert_eq!(m2.msg_type, "pmessage");
    assert_eq!(m2.pattern.as_deref(), Some("jobs.*"));
    assert_eq!(m2.channel, "jobs.run");
    assert_eq!(m2.data.as_ref(), b"payload-2");

    core.ssubscribe(&["shard:{1}".to_string()]).await.unwrap();
    let mut drained = Vec::new();
    for _ in 0..100 {
        drained = core.drain_messages(10).await;
        if !drained.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].msg_type, "smessage");
    assert_eq!(drained[0].data.as_ref(), b"payload-3");

    core.close().await;
    assert!(core.is_closed());
    assert!(core.next_message(Some(50)).await.unwrap().is_none());
    srv.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_domain_socket_roundtrip() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sock_path = std::env::temp_dir().join(format!("vtb-valkey-uds-{nonce}.sock"));
    let _ = std::fs::remove_file(&sock_path);
    let listener = UnixListener::bind(&sock_path).unwrap();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = BytesMut::new();
        while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
            let reply = match parts[0].to_ascii_uppercase().as_str() {
                "HELLO" => b"%1\r\n$5\r\nproto\r\n:3\r\n".to_vec(),
                "CLIENT" => b"+OK\r\n".to_vec(),
                "PING" => b"+PONG\r\n".to_vec(),
                "GET" => encode_bulk(b"uds-ok"),
                _ => b"+OK\r\n".to_vec(),
            };
            sock.write_all(&reply).await.unwrap();
        }
    });

    let cfg = ValkeyConfigInner::from_url_str(&format!("unix://{}", sock_path.display())).unwrap();
    let engine = ValkeyEngine::new(Arc::new(cfg));
    assert!(engine.ping().await.unwrap());
    assert_eq!(
        engine.get_bytes(b"k".to_vec()).await.unwrap().as_deref(),
        Some(b"uds-ok".as_slice())
    );
    engine.close().await;
    srv.abort();
    let _ = std::fs::remove_file(&sock_path);
}

#[tokio::test]
async fn pubsub_drop_abort_eof_closed_and_pending_delivery_guard_restore() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (eof_done_tx, eof_done_rx) = tokio::sync::oneshot::channel::<()>();

    let srv = tokio::spawn(async move {
        // Connection 1: send 2 messages, wait for signal, then close socket (EOF).
        let (mut sock1, _) = listener.accept().await.unwrap();
        let _ = sock1.set_nodelay(true);
        let mut buf1 = BytesMut::new();
        while let Some(parts) = read_next_cmd(&mut sock1, &mut buf1).await {
            match parts[0].to_ascii_uppercase().as_str() {
                "HELLO" => {
                    sock1
                        .write_all(b"%1\r\n$5\r\nproto\r\n:3\r\n")
                        .await
                        .unwrap();
                }
                "CLIENT" => {
                    sock1.write_all(b"+OK\r\n").await.unwrap();
                }
                "SUBSCRIBE" => {
                    let ack = encode_push(&[
                        encode_bulk(b"subscribe"),
                        encode_bulk(parts[1].as_bytes()),
                        b":1\r\n".to_vec(),
                    ]);
                    let m1 = encode_push(&[
                        encode_bulk(b"message"),
                        encode_bulk(b"ch"),
                        encode_bulk(b"first"),
                    ]);
                    let m2 = encode_push(&[
                        encode_bulk(b"message"),
                        encode_bulk(b"ch"),
                        encode_bulk(b"second"),
                    ]);
                    sock1.write_all(&ack).await.unwrap();
                    sock1.write_all(&m1).await.unwrap();
                    sock1.write_all(&m2).await.unwrap();
                    break;
                }
                _ => {
                    sock1.write_all(b"+OK\r\n").await.unwrap();
                }
            }
        }
        let _ = eof_done_rx.await;
        drop(sock1);

        // Connection 2: verify dropping ValkeyListenerCore aborts reader and closes client socket.
        let (mut sock2, _) = listener.accept().await.unwrap();
        let _ = sock2.set_nodelay(true);
        let mut buf2 = BytesMut::new();
        while let Some(parts) = read_next_cmd(&mut sock2, &mut buf2).await {
            match parts[0].to_ascii_uppercase().as_str() {
                "HELLO" => {
                    sock2
                        .write_all(b"%1\r\n$5\r\nproto\r\n:3\r\n")
                        .await
                        .unwrap();
                }
                "CLIENT" => {
                    sock2.write_all(b"+OK\r\n").await.unwrap();
                }
                "SUBSCRIBE" => {
                    let ack = encode_push(&[
                        encode_bulk(b"subscribe"),
                        encode_bulk(parts[1].as_bytes()),
                        b":1\r\n".to_vec(),
                    ]);
                    sock2.write_all(&ack).await.unwrap();
                    break;
                }
                _ => {
                    sock2.write_all(b"+OK\r\n").await.unwrap();
                }
            }
        }
        let mut probe = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(2), sock2.read(&mut probe))
            .await
            .expect("reader task abort should close client socket promptly")
            .unwrap_or(0);
        assert_eq!(n, 0);
    });

    // Part 1: PendingDeliveryGuard drop restores message in FIFO order; EOF marks closed before waking.
    let core1 = ValkeyListenerCore::new(Arc::new(make_base_config("127.0.0.1", addr.port())));
    core1.subscribe(&["ch".to_string()]).await.unwrap();
    {
        let unconsumed_guard = core1
            .next_message_guarded(Some(1000))
            .await
            .unwrap()
            .expect("expected first message");
        // Drop `unconsumed_guard` without consuming to simulate cancelled Python future!
        drop(unconsumed_guard);
    }
    let restored = core1.next_message(Some(1000)).await.unwrap().unwrap();
    assert_eq!(restored.data.as_ref(), b"first");
    let second = core1.next_message(Some(1000)).await.unwrap().unwrap();
    assert_eq!(second.data.as_ref(), b"second");

    let _ = eof_done_tx.send(());
    let eof_res = core1.next_message(Some(1000)).await.unwrap();
    assert!(eof_res.is_none());
    assert!(
        core1.is_closed(),
        "EOF must set closed=true before notifying waiters"
    );

    // Part 2: Dropping ValkeyListenerCore without close() aborts reader task and closes TCP socket.
    let core2 = ValkeyListenerCore::new(Arc::new(make_base_config("127.0.0.1", addr.port())));
    core2.subscribe(&["ch2".to_string()]).await.unwrap();
    drop(core2);

    srv.await.unwrap();
}

#[tokio::test]
async fn xreadgroup_block_extends_deadline_and_zero_is_untimed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = sock.set_nodelay(true);
        let mut buf = BytesMut::new();
        while let Some(parts) = read_next_cmd(&mut sock, &mut buf).await {
            match parts[0].to_ascii_uppercase().as_str() {
                "HELLO" => {
                    sock.write_all(b"%1\r\n$5\r\nproto\r\n:3\r\n")
                        .await
                        .unwrap();
                }
                "CLIENT" => {
                    sock.write_all(b"+OK\r\n").await.unwrap();
                }
                "XREADGROUP" => {
                    // Sleep longer than command_timeout (60ms) to verify BLOCK extension.
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    sock.write_all(b"_\r\n").await.unwrap();
                }
                _ => {
                    sock.write_all(b"+OK\r\n").await.unwrap();
                }
            }
        }
    });

    let mut cfg = make_base_config("127.0.0.1", addr.port());
    cfg.command_timeout = Duration::from_millis(60);
    let engine = ValkeyEngine::new(Arc::new(cfg));

    // BLOCK 250 -> effective read timeout = 60ms + 250ms = 310ms > 120ms sleep.
    let cmd_bounded = _valkey::streams::build_xreadgroup(
        "grp",
        "c1",
        &[(b"mystream".to_vec(), ">".to_string())],
        Some(1),
        Some(250),
        false,
    )
    .unwrap();
    let r1 = engine
        .execute_command_routed(cmd_bounded, &[b"mystream"], false)
        .await
        .expect("XREADGROUP BLOCK 250 should extend read deadline past command_timeout");
    assert_eq!(r1, RespValue::Null);

    // BLOCK 0 -> un-timed read > 120ms sleep.
    let cmd_infinite = _valkey::streams::build_xreadgroup(
        "grp",
        "c1",
        &[(b"mystream".to_vec(), ">".to_string())],
        Some(1),
        Some(0),
        false,
    )
    .unwrap();
    let r2 = engine
        .execute_command_routed(cmd_infinite, &[b"mystream"], false)
        .await
        .expect("XREADGROUP BLOCK 0 should use un-timed read");
    assert_eq!(r2, RespValue::Null);

    engine.close().await;
    srv.abort();
}

#[test]
fn resp_to_py_set_unhashable_fallback_preserves_all_elements() {
    use pyo3::prelude::*;

    Python::initialize();
    Python::attach(|py| {
        let resp_set = RespValue::Set(vec![
            RespValue::BulkString(Bytes::from_static(b"alpha")),
            RespValue::BulkString(Bytes::from_static(b"beta")),
            RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(2)]),
            RespValue::BulkString(Bytes::from_static(b"gamma")),
        ]);
        let py_obj = _valkey::codec::resp_to_py(py, resp_set).unwrap();
        let bound = py_obj.bind(py);
        let list = bound.cast::<pyo3::types::PyList>().unwrap();
        assert_eq!(list.len(), 4);

        let mut saw_alpha = false;
        let mut saw_beta = false;
        let mut saw_list = false;
        let mut saw_gamma = false;
        for item in list.iter() {
            if let Ok(py_bytes) = item.cast::<pyo3::types::PyBytes>() {
                let b = py_bytes.as_bytes();
                if b == b"alpha" {
                    saw_alpha = true;
                } else if b == b"beta" {
                    saw_beta = true;
                } else if b == b"gamma" {
                    saw_gamma = true;
                }
            } else if let Ok(v) = item.extract::<Vec<i64>>()
                && v == vec![1, 2]
            {
                saw_list = true;
            }
        }
        assert!(
            saw_alpha && saw_beta && saw_list && saw_gamma,
            "All prior set elements, the unhashable element, and remaining elements must be preserved"
        );

        // Verify GcpAuthSource::PythonCallable primes on Python thread and clones/resolves detached!
        let builtins = py.import("builtins").unwrap();
        let py_callable = builtins
            .getattr("str")
            .unwrap()
            .call1(("tok-from-py",))
            .unwrap();
        let ns = pyo3::types::PyDict::new(py);
        ns.set_item("tok", py_callable).unwrap();
        let provider_fn = py.eval(c"lambda: tok", Some(&ns), None).unwrap().unbind();
        let provider = _valkey::config::PythonTokenProvider::new(provider_fn);
        provider.ensure_token_sync(py, false).unwrap();
        let src = GcpAuthSource::PythonCallable(provider);
        py.detach(move || {
            let cloned = src.clone();
            if let GcpAuthSource::PythonCallable(ref p) = cloned {
                let (tok, gen_id) = p.resolve_cached(None).unwrap();
                assert_eq!(tok, "tok-from-py");
                assert!(gen_id >= 1);
            } else {
                panic!("expected PythonCallable");
            }
        });
    });
}
