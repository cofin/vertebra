//! Integration and wire-level tests for `vertebra_db::spanner` (Cloud Spanner v1 gRPC multiplexed
//! sessions, `x-goog-spanner-route-to-leader` routing, `PartialResultSet` chunk merging +
//! `resume_token` replay, `ReadWrite` transaction state machine with `MultiplexedSessionPrecommitToken`
//! and wound-wait `Aborted` retry, and parallel `PartitionQuery` + Data Boost into `VTBARROW` SHM).

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{HeaderMap, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use vertebra_db::arrow_shm::{ARROW_IPC_EOS_MARKER, read_shm_arrow_frame, release_shm_arrow_frame};
use vertebra_db::grpc::{
    DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE, ProtoReader, ProtoWriter, WireType, encode_lpm_frame,
};
use vertebra_db::spanner::{
    MultiplexedSessionPrecommitToken, PartialResultSetAssembler, PartialResultSetFrame,
    ProtobufValue, SpannerBatchDmlStatement, SpannerClient, SpannerClientConfig, SpannerMutation,
    SpannerQueryParameter, SpannerReadWriteTransaction, SpannerStaleness, SpannerStructField,
    SpannerType, SpannerTypeCode, SpannerValue, rewrite_spangres_placeholders,
};

#[derive(Clone, Debug)]
struct RecordedRpc {
    path: String,
    route_to_leader: Option<String>,
    request_id: Option<String>,
    resource_prefix: Option<String>,
    routing_params: Option<String>,
    body: Vec<u8>,
}

fn strip_lpm_prefix(raw: &[u8]) -> &[u8] {
    if raw.len() >= 5 { &raw[5..] } else { raw }
}

fn grpc_ok_response(frames: Vec<Bytes>) -> Response<Full<Bytes>> {
    let mut payload = Vec::new();
    for frame in frames {
        let lpm = encode_lpm_frame(&frame, false, DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE)
            .expect("encode lpm frame");
        payload.extend_from_slice(&lpm);
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .header("grpc-status", "0")
        .body(Full::new(Bytes::from(payload)))
        .expect("build grpc ok response")
}

fn grpc_error_response(
    code: i32,
    message: &str,
    status_details_b64: Option<&str>,
) -> Response<Full<Bytes>> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .header("grpc-status", code.to_string())
        .header("grpc-message", message);
    if let Some(b64) = status_details_b64 {
        builder = builder.header("grpc-status-details-bin", b64);
    }
    builder
        .body(Full::new(Bytes::new()))
        .expect("build grpc error response")
}

fn encode_retry_info_status_bin(code: i32, message: &str, delay_ms: u64) -> String {
    let mut retry_info = ProtoWriter::new();
    retry_info.write_nested(1, |dw| {
        let secs = (delay_ms / 1_000) as i64;
        let nanos = ((delay_ms % 1_000) * 1_000_000) as i32;
        if secs > 0 {
            dw.write_i64(1, secs);
        }
        if nanos > 0 {
            dw.write_i32(2, nanos);
        }
    });
    let retry_bytes = retry_info.finish();

    let mut status = ProtoWriter::new();
    status.write_i32(1, code);
    status.write_str(2, message);
    status.write_nested(3, |any_w| {
        any_w.write_str(1, "type.googleapis.com/google.rpc.RetryInfo");
        any_w.write_bytes(2, &retry_bytes);
    });
    BASE64_STANDARD.encode(status.finish())
}

async fn start_mock_spanner_h2c_server<F>(handler: F) -> SocketAddr
where
    F: Fn(String, HeaderMap, Vec<u8>) -> Response<Full<Bytes>> + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind h2c listener");
    let addr = listener.local_addr().expect("local_addr");
    let handler = Arc::new(handler);

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let h = Arc::clone(&handler);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: Request<Incoming>| {
                    let h = Arc::clone(&h);
                    async move {
                        let path = req.uri().path().to_string();
                        let headers = req.headers().clone();
                        let body = req
                            .into_body()
                            .collect()
                            .await
                            .map(|c| c.to_bytes().to_vec())
                            .unwrap_or_default();
                        Ok::<_, Infallible>(h(path, headers, body))
                    }
                });
                let _ = http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });

    addr
}

#[test]
fn test_partial_result_set_chunk_merger_split_bytes_json_and_nested_list() {
    let raw_bytes = b"Vertebra-Spanner-Zero-Copy-Binary-Payload-123456";
    let b64_full = BASE64_STANDARD.encode(raw_bytes);
    let split_at = 11;
    let b64_part1 = &b64_full[..split_at];
    let b64_part2 = &b64_full[split_at..];

    let json_part1 = r#"{"service":"vertebra","#;
    let json_part2 = r#""multiplexed":true}"#;

    let schema = vec![
        SpannerStructField {
            name: "payload".to_string(),
            field_type: SpannerType::scalar(SpannerTypeCode::Bytes),
        },
        SpannerStructField {
            name: "meta".to_string(),
            field_type: SpannerType::scalar(SpannerTypeCode::Json),
        },
        SpannerStructField {
            name: "tags".to_string(),
            field_type: SpannerType::array(SpannerType::scalar(SpannerTypeCode::String)),
        },
    ];

    let mut assembler = PartialResultSetAssembler::new();

    // Frame 1: `payload` split mid-base64 string (`chunked_value = true`).
    assembler
        .push_frame(PartialResultSetFrame {
            row_type: Some(schema),
            transaction_id: Some(b"tx-chunk-1".to_vec()),
            read_timestamp_micros: None,
            values: vec![ProtobufValue::String(b64_part1.to_string())],
            chunked_value: true,
            resume_token: Vec::new(),
            row_count_exact: None,
            row_count_lower_bound: None,
            precommit_token: Some(MultiplexedSessionPrecommitToken {
                precommit_token: b"tok-1".to_vec(),
                seq_num: 1,
            }),
            last: false,
        })
        .expect("push frame 1");

    // Frame 2: finishes `payload`, starts `meta` JSON and splits mid-JSON (`chunked_value = true`).
    assembler
        .push_frame(PartialResultSetFrame {
            row_type: None,
            transaction_id: None,
            read_timestamp_micros: None,
            values: vec![
                ProtobufValue::String(b64_part2.to_string()),
                ProtobufValue::String(json_part1.to_string()),
            ],
            chunked_value: true,
            resume_token: Vec::new(),
            row_count_exact: None,
            row_count_lower_bound: None,
            precommit_token: Some(MultiplexedSessionPrecommitToken {
                precommit_token: b"tok-2".to_vec(),
                seq_num: 2,
            }),
            last: false,
        })
        .expect("push frame 2");

    // Frame 3: finishes `meta` JSON, starts `tags` ARRAY<STRING> where `"alp"` is split into `"ha"`.
    assembler
        .push_frame(PartialResultSetFrame {
            row_type: None,
            transaction_id: None,
            read_timestamp_micros: None,
            values: vec![
                ProtobufValue::String(json_part2.to_string()),
                ProtobufValue::List(vec![
                    ProtobufValue::String("first".to_string()),
                    ProtobufValue::String("alp".to_string()),
                ]),
            ],
            chunked_value: true,
            resume_token: Vec::new(),
            row_count_exact: None,
            row_count_lower_bound: None,
            precommit_token: None,
            last: false,
        })
        .expect("push frame 3");

    // Frame 4: finishes `"alpha"` and adds `"omega"`, with checkpoint `resume_token`.
    assembler
        .push_frame(PartialResultSetFrame {
            row_type: None,
            transaction_id: None,
            read_timestamp_micros: None,
            values: vec![ProtobufValue::List(vec![
                ProtobufValue::String("ha".to_string()),
                ProtobufValue::String("omega".to_string()),
            ])],
            chunked_value: false,
            resume_token: b"resume-row-1".to_vec(),
            row_count_exact: None,
            row_count_lower_bound: None,
            precommit_token: None,
            last: true,
        })
        .expect("push frame 4");

    let res = assembler.finish().expect("finish assembler");
    assert_eq!(res.rows.len(), 1);
    assert_eq!(
        res.rows[0][0],
        SpannerValue::Bytes(raw_bytes.to_vec()),
        "base64 decoding must occur only after chunked StringValue fragments are concatenated"
    );
    assert_eq!(
        res.rows[0][1],
        SpannerValue::Json(r#"{"service":"vertebra","multiplexed":true}"#.to_string())
    );
    assert_eq!(
        res.rows[0][2],
        SpannerValue::Array(vec![
            SpannerValue::String("first".to_string()),
            SpannerValue::String("alpha".to_string()),
            SpannerValue::String("omega".to_string()),
        ])
    );
    assert_eq!(
        res.precommit_token,
        Some(MultiplexedSessionPrecommitToken {
            precommit_token: b"tok-2".to_vec(),
            seq_num: 2,
        })
    );
}

#[test]
fn test_spangres_placeholder_rewriter() {
    let sql = "SELECT $1, '$2', \"$3\", /* $4 */ $2 FROM items WHERE id = $1";
    let (rewritten, names) = rewrite_spangres_placeholders(sql);
    assert_eq!(
        rewritten,
        "SELECT @p1, '$2', \"$3\", /* $4 */ @p2 FROM items WHERE id = @p1"
    );
    assert_eq!(names, vec!["p1".to_string(), "p2".to_string()]);
}

#[tokio::test]
async fn test_multiplexed_session_caching_routing_headers_and_resume_token_replay() {
    let rpcs: Arc<Mutex<Vec<RecordedRpc>>> = Arc::new(Mutex::new(Vec::new()));
    let stream_calls = Arc::new(AtomicUsize::new(0));

    let rpcs_clone = Arc::clone(&rpcs);
    let stream_calls_clone = Arc::clone(&stream_calls);

    let addr = start_mock_spanner_h2c_server(move |path, headers, raw_body| {
        let body = strip_lpm_prefix(&raw_body).to_vec();
        rpcs_clone.lock().unwrap().push(RecordedRpc {
            path: path.clone(),
            route_to_leader: headers
                .get("x-goog-spanner-route-to-leader")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
            request_id: headers
                .get("x-goog-spanner-request-id")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
            resource_prefix: headers
                .get("google-cloud-resource-prefix")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
            routing_params: headers
                .get("x-goog-request-params")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
            body: body.clone(),
        });

        match path.as_str() {
            "/google.spanner.v1.Spanner/CreateSession" => {
                let mut w = ProtoWriter::new();
                w.write_str(
                    1,
                    "projects/p1/instances/i1/databases/d1/sessions/mux-sess-1",
                );
                w.write_bool(6, true);
                grpc_ok_response(vec![w.finish()])
            }
            "/google.spanner.v1.Spanner/ExecuteStreamingSql" => {
                let call_idx = stream_calls_clone.fetch_add(1, Ordering::SeqCst);
                if call_idx == 0 {
                    let mut prs1 = ProtoWriter::new();
                    prs1.write_nested(1, |mw| {
                        mw.write_nested(1, |stw| {
                            stw.write_nested(1, |fw| {
                                fw.write_str(1, "id");
                                fw.write_nested(2, |tw| tw.write_i32(1, 2)); // INT64
                            });
                            stw.write_nested(1, |fw| {
                                fw.write_str(1, "name");
                                fw.write_nested(2, |tw| tw.write_i32(1, 6)); // STRING
                            });
                        });
                    });
                    prs1.write_nested(2, |vw| vw.write_str(3, "101"));
                    prs1.write_nested(2, |vw| vw.write_str(3, "Ada "));
                    prs1.write_bool(3, true); // chunked_value = true

                    let mut prs2 = ProtoWriter::new();
                    prs2.write_nested(2, |vw| vw.write_str(3, "Lovelace"));
                    prs2.write_bool(3, false);
                    prs2.write_bytes(4, b"tok-1");
                    prs2.write_bool(9, true);

                    grpc_ok_response(vec![prs1.finish(), prs2.finish()])
                } else {
                    let mut prs = ProtoWriter::new();
                    prs.write_nested(1, |mw| {
                        mw.write_nested(1, |stw| {
                            stw.write_nested(1, |fw| {
                                fw.write_str(1, "id");
                                fw.write_nested(2, |tw| tw.write_i32(1, 2));
                            });
                        });
                    });
                    prs.write_nested(2, |vw| vw.write_str(3, "202"));
                    prs.write_bytes(4, b"tok-2");
                    grpc_ok_response(vec![prs.finish()])
                }
            }
            _ => grpc_error_response(12, "unimplemented", None),
        }
    })
    .await;

    let mut cfg = SpannerClientConfig::new("projects/p1/instances/i1/databases/d1");
    cfg.endpoint = format!("http://{}:{}", addr.ip(), addr.port());
    cfg.database_role = Some("analytics_reader".to_string());
    let client = SpannerClient::new(cfg).expect("create SpannerClient");

    let res1 = client
        .query_read_only(
            "SELECT id, name FROM users WHERE id = @id",
            &[SpannerQueryParameter::from_value(
                "id",
                &SpannerValue::Int64(101),
            )],
            SpannerStaleness::ExactStalenessMs(5_000),
        )
        .await
        .expect("query_read_only 1");
    assert_eq!(res1.columns, vec!["id", "name"]);
    assert_eq!(res1.rows.len(), 1);
    assert_eq!(res1.rows[0][0], SpannerValue::Int64(101));
    assert_eq!(
        res1.rows[0][1],
        SpannerValue::String("Ada Lovelace".to_string())
    );

    let res2 = client
        .query_read_only("SELECT id FROM users", &[], SpannerStaleness::Strong)
        .await
        .expect("query_read_only 2");
    assert_eq!(res2.rows.len(), 1);
    assert_eq!(res2.rows[0][0], SpannerValue::Int64(202));

    let log = rpcs.lock().unwrap();
    let create_sessions: Vec<&RecordedRpc> = log
        .iter()
        .filter(|r| r.path == "/google.spanner.v1.Spanner/CreateSession")
        .collect();
    assert_eq!(
        create_sessions.len(),
        1,
        "multiplexed session must be cached and reused across queries"
    );
    assert_eq!(
        create_sessions[0].route_to_leader.as_deref(),
        Some("true"),
        "CreateSession must set x-goog-spanner-route-to-leader: true"
    );
    assert_eq!(
        create_sessions[0].resource_prefix.as_deref(),
        Some("projects/p1/instances/i1/databases/d1")
    );
    assert_eq!(
        create_sessions[0].routing_params.as_deref(),
        Some("database=projects/p1/instances/i1/databases/d1")
    );
    assert!(
        create_sessions[0]
            .body
            .windows(b"analytics_reader".len())
            .any(|w| w == b"analytics_reader"),
        "CreateSession request body must encode Session.creator_role"
    );
    let req_id = create_sessions[0]
        .request_id
        .as_deref()
        .expect("x-goog-spanner-request-id present");
    assert!(
        req_id.starts_with("1.") && req_id.split('.').count() == 6,
        "unexpected x-goog-spanner-request-id format: {req_id}"
    );

    let streaming_calls: Vec<&RecordedRpc> = log
        .iter()
        .filter(|r| r.path == "/google.spanner.v1.Spanner/ExecuteStreamingSql")
        .collect();
    assert_eq!(streaming_calls.len(), 2);
    assert!(
        streaming_calls[0].route_to_leader.is_none(),
        "ReadOnly ExecuteStreamingSql must omit x-goog-spanner-route-to-leader"
    );
}

#[tokio::test]
async fn test_read_write_transaction_inline_begin_precommit_retry_and_aborted_wound_wait() {
    let commit_calls = Arc::new(AtomicUsize::new(0));
    let stmt_calls = Arc::new(AtomicUsize::new(0));
    let observed_commit_bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let observed_stmt_bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));

    let commit_calls_c = Arc::clone(&commit_calls);
    let stmt_calls_c = Arc::clone(&stmt_calls);
    let commit_bodies_c = Arc::clone(&observed_commit_bodies);
    let stmt_bodies_c = Arc::clone(&observed_stmt_bodies);

    let addr = start_mock_spanner_h2c_server(move |path, _headers, raw_body| {
        let body = strip_lpm_prefix(&raw_body).to_vec();
        match path.as_str() {
            "/google.spanner.v1.Spanner/CreateSession" => {
                let mut w = ProtoWriter::new();
                w.write_str(1, "projects/p1/instances/i1/databases/d1/sessions/mux-rw-1");
                w.write_bool(6, true);
                grpc_ok_response(vec![w.finish()])
            }
            "/google.spanner.v1.Spanner/ExecuteStreamingSql" => {
                stmt_bodies_c.lock().unwrap().push(body);
                let idx = stmt_calls_c.fetch_add(1, Ordering::SeqCst);
                let tx_id = if idx == 0 {
                    b"tx-attempt-1".to_vec()
                } else {
                    b"tx-attempt-2".to_vec()
                };
                let mut prs = ProtoWriter::new();
                prs.write_nested(1, |mw| {
                    mw.write_nested(2, |tw| {
                        tw.write_bytes(1, &tx_id);
                        tw.write_nested(8, |pw| {
                            pw.write_bytes(1, b"precommit-seq-1");
                            pw.write_i32(2, 1);
                        });
                    });
                });
                prs.write_nested(5, |sw| {
                    sw.write_i64(3, 1);
                });
                prs.write_nested(8, |pw| {
                    pw.write_bytes(1, b"precommit-seq-2");
                    pw.write_i32(2, 2);
                });
                grpc_ok_response(vec![prs.finish()])
            }
            "/google.spanner.v1.Spanner/ExecuteBatchDml" => {
                let mut resp = ProtoWriter::new();
                resp.write_nested(1, |rsw| {
                    rsw.write_nested(3, |sw| sw.write_i64(3, 2));
                });
                resp.write_nested(2, |stw| {
                    stw.write_i32(1, 0);
                });
                resp.write_nested(3, |pw| {
                    pw.write_bytes(1, b"precommit-seq-5");
                    pw.write_i32(2, 5);
                });
                grpc_ok_response(vec![resp.finish()])
            }
            "/google.spanner.v1.Spanner/Commit" => {
                commit_bodies_c.lock().unwrap().push(body);
                let c_idx = commit_calls_c.fetch_add(1, Ordering::SeqCst);
                if c_idx == 0 {
                    let details = encode_retry_info_status_bin(10, "Transaction aborted", 5);
                    grpc_error_response(10, "Transaction aborted", Some(&details))
                } else if c_idx == 1 {
                    let mut resp = ProtoWriter::new();
                    resp.write_nested(4, |pw| {
                        pw.write_bytes(1, b"precommit-retry-99");
                        pw.write_i32(2, 99);
                    });
                    grpc_ok_response(vec![resp.finish()])
                } else {
                    let mut resp = ProtoWriter::new();
                    resp.write_nested(1, |tw| {
                        tw.write_i64(1, 1_700_000_000);
                    });
                    resp.write_nested(2, |sw| {
                        sw.write_i64(1, 3);
                    });
                    grpc_ok_response(vec![resp.finish()])
                }
            }
            _ => grpc_error_response(12, "unimplemented", None),
        }
    })
    .await;

    let mut cfg = SpannerClientConfig::new("projects/p1/instances/i1/databases/d1");
    cfg.endpoint = format!("http://{}:{}", addr.ip(), addr.port());
    let client = Arc::new(SpannerClient::new(cfg).expect("create SpannerClient"));

    let client_ref = Arc::clone(&client);
    let (batch_counts, commit_res) = client
        .run_in_read_write_transaction(3, move |mut tx: SpannerReadWriteTransaction| {
            let c = Arc::clone(&client_ref);
            async move {
                let dml1 = c
                    .execute_in_read_write_tx(
                        &mut tx,
                        "UPDATE accounts SET balance = balance - 10 WHERE id = 1",
                        &[],
                        Some("debit"),
                    )
                    .await
                    .map_err(|e| (e, tx.transaction_id.clone()))?;
                assert_eq!(dml1.row_count_exact, Some(1));

                let batch_res = c
                    .execute_batch_dml(
                        &mut tx,
                        &[SpannerBatchDmlStatement {
                            sql: "UPDATE accounts SET balance = balance + 10 WHERE id = 2"
                                .to_string(),
                            params: Vec::new(),
                        }],
                        Some("credit-batch"),
                    )
                    .await
                    .map_err(|e| (e, tx.transaction_id.clone()))?;

                tx.add_mutation(SpannerMutation::insert_or_update(
                    "audit_log",
                    vec!["id".to_string(), "event".to_string()],
                    vec![vec![
                        ProtobufValue::String("1".to_string()),
                        ProtobufValue::String("transfer".to_string()),
                    ]],
                ));
                Ok((batch_res.row_counts, tx))
            }
        })
        .await
        .expect("run_in_read_write_transaction");

    assert_eq!(batch_counts, vec![2]);
    assert_eq!(commit_res.retry_count, 1);
    assert_eq!(commit_res.mutation_count, Some(3));

    let stmt_bodies = observed_stmt_bodies.lock().unwrap();
    assert_eq!(stmt_bodies.len(), 2);
    assert!(
        stmt_bodies[1]
            .windows(b"tx-attempt-1".len())
            .any(|w| w == b"tx-attempt-1"),
        "retry attempt after Aborted must include aborted transaction_id in multiplexed_session_previous_transaction_id"
    );

    let commits = observed_commit_bodies.lock().unwrap();
    assert_eq!(commits.len(), 3);
    assert!(
        commits[1]
            .windows(b"precommit-seq-5".len())
            .any(|w| w == b"precommit-seq-5")
    );
    assert!(
        commits[1]
            .windows(b"audit_log".len())
            .any(|w| w == b"audit_log")
    );
    assert!(
        commits[2]
            .windows(b"precommit-retry-99".len())
            .any(|w| w == b"precommit-retry-99")
    );
    assert!(
        !commits[2]
            .windows(b"audit_log".len())
            .any(|w| w == b"audit_log"),
        "MultiplexedSessionRetry replay CommitRequest must omit already-buffered mutations"
    );
}

#[tokio::test]
async fn test_partition_query_and_data_boost_to_vtbarrow_shm() {
    let data_boost_flags: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(Vec::new()));
    let boost_clone = Arc::clone(&data_boost_flags);

    let addr = start_mock_spanner_h2c_server(move |path, _headers, raw_body| {
        let body = strip_lpm_prefix(&raw_body).to_vec();
        match path.as_str() {
            "/google.spanner.v1.Spanner/CreateSession" => {
                let mut w = ProtoWriter::new();
                w.write_str(
                    1,
                    "projects/p1/instances/i1/databases/d1/sessions/mux-part-1",
                );
                w.write_bool(6, true);
                grpc_ok_response(vec![w.finish()])
            }
            "/google.spanner.v1.Spanner/BeginTransaction" => {
                let mut w = ProtoWriter::new();
                w.write_bytes(1, b"ro-snap-1");
                grpc_ok_response(vec![w.finish()])
            }
            "/google.spanner.v1.Spanner/PartitionQuery" => {
                let mut w = ProtoWriter::new();
                w.write_nested(1, |pw| pw.write_bytes(1, b"part-token-A"));
                w.write_nested(1, |pw| pw.write_bytes(1, b"part-token-B"));
                grpc_ok_response(vec![w.finish()])
            }
            "/google.spanner.v1.Spanner/ExecuteStreamingSql" => {
                let mut r = ProtoReader::new(&body);
                let mut boost = false;
                let mut part_tok = Vec::new();
                while let Some((f, wt)) = r.next_tag().unwrap_or(None) {
                    match (f, wt) {
                        (8, WireType::LengthDelimited) => {
                            part_tok = r.read_bytes().unwrap_or(&[]).to_vec();
                        }
                        (16, WireType::Varint) => {
                            boost = r.read_bool().unwrap_or(false);
                        }
                        _ => {
                            let _ = r.skip_field(wt);
                        }
                    }
                }
                boost_clone.lock().unwrap().push(boost);

                let row_val = if part_tok == b"part-token-A" {
                    "10"
                } else {
                    "20"
                };
                let mut prs = ProtoWriter::new();
                prs.write_nested(1, |mw| {
                    mw.write_nested(1, |stw| {
                        stw.write_nested(1, |fw| {
                            fw.write_str(1, "metric");
                            fw.write_nested(2, |tw| tw.write_i32(1, 2)); // INT64
                        });
                    });
                });
                prs.write_nested(2, |vw| vw.write_str(3, row_val));
                prs.write_bytes(4, b"part-done");
                grpc_ok_response(vec![prs.finish()])
            }
            _ => grpc_error_response(12, "unimplemented", None),
        }
    })
    .await;

    let mut cfg = SpannerClientConfig::new("projects/p1/instances/i1/databases/d1");
    cfg.endpoint = format!("http://{}:{}", addr.ip(), addr.port());
    let client = Arc::new(SpannerClient::new(cfg).expect("create SpannerClient"));

    let (shm_name, byte_len, row_count, _region) = client
        .execute_partitioned_arrow(
            "SELECT metric FROM telemetry",
            &[],
            2,
            true,
            SpannerStaleness::Strong,
        )
        .await
        .expect("execute_partitioned_arrow");

    assert_eq!(row_count, 2);
    assert!(byte_len > 32);

    let flags = data_boost_flags.lock().unwrap().clone();
    assert_eq!(flags, vec![true, true]);

    let shm_payload = read_shm_arrow_frame(&shm_name).expect("read VTBARROW payload");
    assert_eq!(shm_payload.len(), byte_len);
    assert_eq!(&shm_payload[0..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
    assert_eq!(&shm_payload[shm_payload.len() - 8..], &ARROW_IPC_EOS_MARKER);
    assert!(release_shm_arrow_frame(&shm_name));
}
