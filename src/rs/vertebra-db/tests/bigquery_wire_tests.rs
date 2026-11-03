use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};

use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::response::Response as GaxResponse;
use google_cloud_gax::streaming::ResponseStream;
use vertebra_db::arrow_shm::{ARROW_IPC_EOS_MARKER, ShmArrowFrameHeader};
use vertebra_db::bigquery::{
    BigQueryArrowCompression, BigQueryCellValue, BigQueryClient, BigQueryClientConfig,
    BigQueryError, BigQueryParamValue, BigQueryQueryOptions, BigQueryQueryParameter,
    DEFAULT_BIGQUERY_STORAGE_ENDPOINT, decode_append_rows_request,
};
use vertebra_db::grpc::{
    GrpcCode, LpmFrameDecoder, ProtoReader, ProtoWriter, WireType, encode_lpm_frame,
};

struct MockGrpcBody {
    rx: mpsc::Receiver<Frame<Bytes>>,
}

impl Body for MockGrpcBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(frame)) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn make_grpc_response(
    frames: Vec<Bytes>,
    grpc_status: GrpcCode,
    grpc_message: &str,
) -> Response<MockGrpcBody> {
    let (tx, rx) = mpsc::channel::<Frame<Bytes>>(16);
    let status_val = (grpc_status as i32).to_string();
    let msg_val = grpc_message.to_string();
    tokio::spawn(async move {
        for payload in frames {
            let lpm = encode_lpm_frame(&payload, false, 64 * 1024 * 1024).expect("lpm frame");
            let _ = tx.send(Frame::data(lpm)).await;
        }
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", HeaderValue::from_str(&status_val).unwrap());
        if !msg_val.is_empty() {
            trailers.insert("grpc-message", HeaderValue::from_str(&msg_val).unwrap());
        }
        let _ = tx.send(Frame::trailers(trailers)).await;
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc+proto")
        .body(MockGrpcBody { rx })
        .unwrap()
}

fn build_mock_arrow_ipc_stream(schema_payload: &[u8], batch_payloads: &[(&[u8], i64)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&build_encapsulated_arrow_frame(1, schema_payload, 0));
    for (body, rows) in batch_payloads {
        out.extend_from_slice(&build_encapsulated_arrow_frame(3, body, *rows));
    }
    out.extend_from_slice(&ARROW_IPC_EOS_MARKER);
    out
}

fn build_encapsulated_arrow_frame(header_type: u8, body: &[u8], row_count: i64) -> Vec<u8> {
    let padded_body_len = (body.len() + 7) & !7;
    let mut fb = vec![0u8; 72];
    let root_offset: u32 = 20;
    fb[0..4].copy_from_slice(&root_offset.to_le_bytes());

    let vtable_pos: usize = 4;
    let vtable_len: u16 = 12;
    let table_len: u16 = 24;
    fb[vtable_pos..vtable_pos + 2].copy_from_slice(&vtable_len.to_le_bytes());
    fb[vtable_pos + 2..vtable_pos + 4].copy_from_slice(&table_len.to_le_bytes());
    fb[vtable_pos + 4..vtable_pos + 6].copy_from_slice(&4u16.to_le_bytes());
    fb[vtable_pos + 6..vtable_pos + 8].copy_from_slice(&6u16.to_le_bytes());
    fb[vtable_pos + 8..vtable_pos + 10].copy_from_slice(&8u16.to_le_bytes());
    fb[vtable_pos + 10..vtable_pos + 12].copy_from_slice(&16u16.to_le_bytes());

    let root = root_offset as usize;
    let vtable_rel: i32 = (root - vtable_pos) as i32;
    fb[root..root + 4].copy_from_slice(&vtable_rel.to_le_bytes());
    fb[root + 6] = header_type;
    fb[root + 16..root + 24].copy_from_slice(&(padded_body_len as i64).to_le_bytes());

    if header_type == 3 {
        let union_loc = root + 8;
        let rb_table = 56usize;
        let rel = (rb_table - union_loc) as u32;
        fb[union_loc..union_loc + 4].copy_from_slice(&rel.to_le_bytes());

        let rb_vtable = 48usize;
        fb[rb_vtable..rb_vtable + 2].copy_from_slice(&6u16.to_le_bytes());
        fb[rb_vtable + 2..rb_vtable + 4].copy_from_slice(&12u16.to_le_bytes());
        fb[rb_vtable + 4..rb_vtable + 6].copy_from_slice(&4u16.to_le_bytes());

        let rb_vtable_rel = (rb_table - rb_vtable) as i32;
        fb[rb_table..rb_table + 4].copy_from_slice(&rb_vtable_rel.to_le_bytes());
        fb[rb_table + 4..rb_table + 12].copy_from_slice(&row_count.to_le_bytes());
    }

    let meta_size = fb.len() as u32;
    let mut frame = Vec::with_capacity(8 + fb.len() + padded_body_len);
    frame.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
    frame.extend_from_slice(&meta_size.to_le_bytes());
    frame.extend_from_slice(&fb);
    frame.extend_from_slice(body);
    frame.resize(8 + fb.len() + padded_body_len, 0);
    frame
}

#[tokio::test]
async fn test_bigquery_rest_fastpath_parameters_and_nested_row_decoding() {
    let captured_requests: Arc<Mutex<Vec<(String, String, Value)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let captured_clone = Arc::clone(&captured_requests);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let cap = Arc::clone(&captured_clone);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: Request<Incoming>| {
                    let cap = Arc::clone(&cap);
                    async move {
                        let method = req.method().to_string();
                        let uri = req.uri().to_string();
                        let body_bytes = req.into_body().collect().await.unwrap().to_bytes();
                        let payload: Value = if body_bytes.is_empty() {
                            json!({})
                        } else {
                            serde_json::from_slice(&body_bytes).unwrap_or_else(|_| json!({}))
                        };
                        cap.lock().await.push((method, uri, payload));

                        let response_json = json!({
                            "jobComplete": true,
                            "jobReference": {
                                "projectId": "analytics-prod",
                                "jobId": "job-fast-1",
                                "location": "US"
                            },
                            "schema": {
                                "fields": [
                                    {"name": "id", "type": "INT64", "mode": "REQUIRED"},
                                    {"name": "tags", "type": "STRING", "mode": "REPEATED"},
                                    {
                                        "name": "meta",
                                        "type": "STRUCT",
                                        "mode": "NULLABLE",
                                        "fields": [
                                            {"name": "score", "type": "FLOAT64", "mode": "NULLABLE"},
                                            {"name": "active", "type": "BOOL", "mode": "NULLABLE"}
                                        ]
                                    },
                                    {"name": "raw_bytes", "type": "BYTES", "mode": "NULLABLE"},
                                    {"name": "payload_json", "type": "JSON", "mode": "NULLABLE"},
                                    {"name": "created_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
                                    {"name": "amount", "type": "NUMERIC", "mode": "NULLABLE"},
                                    {"name": "opt_null", "type": "STRING", "mode": "NULLABLE"}
                                ]
                            },
                            "rows": [
                                {
                                    "f": [
                                        {"v": "42"},
                                        {"v": [{"v": "alpha"}, {"v": "beta"}]},
                                        {"v": {"f": [{"v": "98.5"}, {"v": "true"}]}},
                                        {"v": "aGVsbG8="},
                                        {"v": "{\"k\":1}"},
                                        {"v": "1700000000123456"},
                                        {"v": "123.45"},
                                        {"v": null}
                                    ]
                                }
                            ],
                            "totalRows": "1",
                            "totalBytesProcessed": "8192",
                            "totalBytesBilled": "10485760",
                            "totalSlotMs": "35",
                            "cacheHit": false
                        });
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Full::new(Bytes::from(response_json.to_string())))
                                .unwrap(),
                        )
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });

    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "test".to_string());
    let cfg = BigQueryClientConfig {
        project_id: "analytics-prod".to_string(),
        dataset_id: Some("warehouse".to_string()),
        location: Some("US".to_string()),
        rest_endpoint: format!("http://{addr}"),
        storage_endpoint: DEFAULT_BIGQUERY_STORAGE_ENDPOINT.to_string(),
        maximum_bytes_billed: Some(50_000_000),
        job_timeout_ms: Some(15_000),
        priority: Some("INTERACTIVE".to_string()),
        labels,
        auth_options: HashMap::new(),
        pool_size: 2,
        ca_pem: None,
    };

    let client = BigQueryClient::new(cfg).expect("client");
    let params = vec![
        BigQueryQueryParameter::named("id", BigQueryParamValue::Int64(42)),
        BigQueryQueryParameter::named("opt_null", BigQueryParamValue::Null("STRING".to_string())),
        BigQueryQueryParameter::named(
            "tags",
            BigQueryParamValue::Array {
                elem_type: "STRING".to_string(),
                values: vec![
                    BigQueryParamValue::String("alpha".to_string()),
                    BigQueryParamValue::String("beta".to_string()),
                ],
            },
        ),
        BigQueryQueryParameter::named(
            "meta",
            BigQueryParamValue::Struct {
                fields: vec![
                    ("score".to_string(), BigQueryParamValue::Float64(98.5)),
                    ("active".to_string(), BigQueryParamValue::Bool(true)),
                ],
            },
        ),
    ];

    let res = client
        .query("SELECT * FROM events WHERE id = @id", &params, None)
        .await
        .expect("query succeeds");

    assert_eq!(res.columns.len(), 8);
    assert_eq!(res.total_rows, 1);
    assert_eq!(res.total_bytes_processed, 8192);
    assert_eq!(res.total_bytes_billed, 10_485_760);
    assert_eq!(res.slot_ms, 35);
    assert!(!res.cache_hit);
    assert_eq!(res.job_id.as_deref(), Some("job-fast-1"));

    let row = &res.rows[0];
    assert_eq!(row[0], BigQueryCellValue::Int64(42));
    assert_eq!(
        row[1],
        BigQueryCellValue::Array(vec![
            BigQueryCellValue::String("alpha".to_string()),
            BigQueryCellValue::String("beta".to_string()),
        ])
    );
    assert_eq!(
        row[2],
        BigQueryCellValue::Struct(vec![
            ("score".to_string(), BigQueryCellValue::Float64(98.5)),
            ("active".to_string(), BigQueryCellValue::Bool(true)),
        ])
    );
    assert_eq!(row[3], BigQueryCellValue::Bytes(b"hello".to_vec()));
    assert_eq!(row[4], BigQueryCellValue::Json("{\"k\":1}".to_string()));
    assert_eq!(row[5], BigQueryCellValue::Int64(1_700_000_000_123_456));
    assert_eq!(row[6], BigQueryCellValue::String("123.45".to_string()));
    assert_eq!(row[7], BigQueryCellValue::Null);

    let reqs = captured_requests.lock().await;
    assert_eq!(reqs.len(), 1);
    let (method, path, body) = &reqs[0];
    assert_eq!(method, "POST");
    assert_eq!(path, "/bigquery/v2/projects/analytics-prod/queries");
    assert_eq!(body["useLegacySql"], json!(false));
    assert_eq!(body["jobCreationMode"], json!("JOB_CREATION_OPTIONAL"));
    assert_eq!(body["formatOptions"]["useInt64Timestamp"], json!(true));
    assert_eq!(body["maximumBytesBilled"], json!("50000000"));
    assert_eq!(body["timeoutMs"], json!("15000"));
    assert_eq!(
        body["defaultDataset"],
        json!({"projectId": "analytics-prod", "datasetId": "warehouse"})
    );
    let req_id = body["requestId"].as_str().expect("requestId UUID");
    assert_eq!(req_id.len(), 36);

    let qp = body["queryParameters"].as_array().expect("queryParameters");
    assert_eq!(qp[1]["name"], json!("opt_null"));
    assert_eq!(qp[1]["parameterType"]["type"], json!("STRING"));
    assert_eq!(qp[1]["parameterValue"], json!({}));
}

#[tokio::test]
async fn test_bigquery_poll_pagination_dry_run_and_sessions() {
    let poll_counter = Arc::new(AtomicUsize::new(0));
    let poll_counter_clone = Arc::clone(&poll_counter);
    let captured_bodies: Arc<Mutex<Vec<(String, String, Value)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let cap_clone = Arc::clone(&captured_bodies);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let poll_counter = Arc::clone(&poll_counter_clone);
            let cap = Arc::clone(&cap_clone);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: Request<Incoming>| {
                    let poll_counter = Arc::clone(&poll_counter);
                    let cap = Arc::clone(&cap);
                    async move {
                        let method = req.method().to_string();
                        let uri = req.uri().to_string();
                        let body_bytes = req.into_body().collect().await.unwrap().to_bytes();
                        let payload: Value = if body_bytes.is_empty() {
                            json!({})
                        } else {
                            serde_json::from_slice(&body_bytes).unwrap_or_else(|_| json!({}))
                        };
                        cap.lock()
                            .await
                            .push((method.clone(), uri.clone(), payload.clone()));

                        let resp_json = if method == "POST"
                            && uri.ends_with("/jobs")
                            && payload["configuration"]["dryRun"] == json!(true)
                        {
                            json!({
                                "jobReference": {"projectId": "p1", "jobId": "dry-1", "location": "US"},
                                "statistics": {
                                    "totalBytesProcessed": "65536",
                                    "query": {
                                        "totalBytesProcessed": "65536",
                                        "totalBytesBilled": "10485760",
                                        "cacheHit": false,
                                        "referencedTables": [
                                            {"projectId": "p1", "datasetId": "d1", "tableId": "t1"}
                                        ],
                                        "schema": {
                                            "fields": [
                                                {"name": "col_a", "type": "STRING", "mode": "NULLABLE"}
                                            ]
                                        }
                                    }
                                }
                            })
                        } else if method == "POST" && uri.ends_with("/queries") {
                            if payload["createSession"] == json!(true) {
                                json!({
                                    "jobComplete": true,
                                    "jobReference": {"projectId": "p1", "jobId": "sess-job", "location": "US"},
                                    "statistics": {
                                        "sessionInfo": {"sessionId": "bq-session-abc-123"}
                                    },
                                    "schema": {"fields": []},
                                    "rows": []
                                })
                            } else {
                                json!({
                                    "jobComplete": false,
                                    "jobReference": {"projectId": "p1", "jobId": "poll-job-99", "location": "US"}
                                })
                            }
                        } else if method == "GET" && uri.contains("poll-job-99") {
                            let step = poll_counter.fetch_add(1, Ordering::SeqCst);
                            if step == 0 {
                                json!({
                                    "jobComplete": true,
                                    "jobReference": {"projectId": "p1", "jobId": "poll-job-99", "location": "US"},
                                    "schema": {
                                        "fields": [{"name": "n", "type": "INT64", "mode": "NULLABLE"}]
                                    },
                                    "rows": [{"f": [{"v": "10"}]}],
                                    "pageToken": "next-page-2",
                                    "totalRows": "2"
                                })
                            } else {
                                json!({
                                    "jobComplete": true,
                                    "jobReference": {"projectId": "p1", "jobId": "poll-job-99", "location": "US"},
                                    "schema": {
                                        "fields": [{"name": "n", "type": "INT64", "mode": "NULLABLE"}]
                                    },
                                    "rows": [{"f": [{"v": "20"}]}],
                                    "totalRows": "2"
                                })
                            }
                        } else {
                            json!({"jobComplete": true, "schema": {"fields": []}, "rows": []})
                        };

                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(Full::new(Bytes::from(resp_json.to_string())))
                                .unwrap(),
                        )
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });

    let mut cfg = BigQueryClientConfig::new("p1");
    cfg.dataset_id = Some("d1".to_string());
    cfg.location = Some("US".to_string());
    cfg.rest_endpoint = format!("http://{addr}");
    let client = BigQueryClient::new(cfg).expect("client");

    let paged_res = client
        .query("SELECT n FROM t1", &[], None)
        .await
        .expect("poll + page drain");
    assert_eq!(paged_res.rows.len(), 2);
    assert_eq!(paged_res.rows[0][0], BigQueryCellValue::Int64(10));
    assert_eq!(paged_res.rows[1][0], BigQueryCellValue::Int64(20));

    let dry = client
        .dry_run("SELECT col_a FROM d1.t1", &[])
        .await
        .expect("dry_run");
    assert_eq!(dry.total_bytes_processed, 65536);
    assert_eq!(dry.total_bytes_billed, 10_485_760);
    assert_eq!(dry.referenced_tables, vec!["p1.d1.t1".to_string()]);
    assert_eq!(dry.schema.len(), 1);
    assert_eq!(dry.schema[0].name, "col_a");

    let begin_opts = BigQueryQueryOptions {
        create_session: true,
        ..BigQueryQueryOptions::default()
    };
    let begin_res = client
        .query_with_options("BEGIN TRANSACTION", &[], &begin_opts)
        .await
        .expect("begin session");
    assert_eq!(begin_res.session_id.as_deref(), Some("bq-session-abc-123"));
}

#[derive(Clone, Debug, Default)]
struct RecordingRead {
    sessions: Arc<std::sync::Mutex<Vec<model::CreateReadSessionRequest>>>,
}

impl google_cloud_bigquery::stub::Read for RecordingRead {
    async fn create_read_session(
        &self,
        req: model::CreateReadSessionRequest,
        _options: RequestOptions,
    ) -> google_cloud_bigquery::Result<GaxResponse<model::ReadSession>> {
        self.sessions.lock().unwrap().push(req);
        let session = model::ReadSession::new()
            .set_name("projects/p1/locations/US/sessions/s1")
            .set_arrow_schema(
                model::ArrowSchema::new().set_serialized_schema(Bytes::from_static(b"SCHEMA00")),
            )
            .set_streams(vec![
                model::ReadStream::new().set_name("projects/p1/locations/US/sessions/s1/streams/0"),
                model::ReadStream::new().set_name("projects/p1/locations/US/sessions/s1/streams/1"),
            ]);
        Ok(GaxResponse::from(session))
    }

    async fn read_rows(
        &self,
        req: model::ReadRowsRequest,
        _options: RequestOptions,
    ) -> google_cloud_bigquery::Result<ResponseStream<model::ReadRowsResponse>> {
        let (tx, rx) = mpsc::channel(4);
        if req.read_stream.ends_with("/streams/0") {
            let r0 = model::ReadRowsResponse::new()
                .set_row_count(3)
                .set_arrow_record_batch(
                    model::ArrowRecordBatch::new()
                        .set_serialized_record_batch(Bytes::from_static(b"BATCH-A0")),
                );
            let r1 = model::ReadRowsResponse::new()
                .set_row_count(2)
                .set_arrow_record_batch(
                    model::ArrowRecordBatch::new()
                        .set_serialized_record_batch(Bytes::from_static(b"BATCH-A1")),
                );
            let _ = tx.try_send(Ok(r0));
            let _ = tx.try_send(Ok(r1));
        } else {
            let r0 = model::ReadRowsResponse::new()
                .set_row_count(4)
                .set_arrow_record_batch(
                    model::ArrowRecordBatch::new()
                        .set_serialized_record_batch(Bytes::from_static(b"BATCH-B0")),
                );
            let _ = tx.try_send(Ok(r0));
        }
        drop(tx);
        Ok(ResponseStream::from(rx))
    }
}

#[tokio::test]
async fn read_table_writes_schema_then_stream_batches_in_order() {
    let stub = RecordingRead::default();
    let mut cfg = BigQueryClientConfig::new("p1");
    cfg.dataset_id = Some("d1".into());
    let client =
        BigQueryClient::with_read_client(cfg, Read::from_stub(stub.clone())).expect("client");
    let (shm_name, byte_len, rows, region) = client
        .read_table_to_shm_arrow_with_compression(
            "p1",
            "d1",
            "t1",
            &["id".into(), "name".into()],
            Some("id > 10"),
            2,
            BigQueryArrowCompression::Zstd,
        )
        .await
        .expect("read");
    assert_eq!(rows, 9);
    let req = stub.sessions.lock().unwrap()[0].clone();
    assert_eq!(req.parent, "projects/p1");
    assert_eq!(req.max_stream_count, 2);
    let session = req.read_session.expect("read_session");
    assert_eq!(session.table, "projects/p1/datasets/d1/tables/t1");
    assert_eq!(session.data_format, model::DataFormat::Arrow);
    let opts = session.read_options.expect("read_options");
    assert_eq!(
        opts.selected_fields,
        vec!["id".to_string(), "name".to_string()]
    );
    assert_eq!(opts.row_restriction, "id > 10");
    assert_eq!(
        opts.arrow_serialization_options()
            .map(|o| o.buffer_compression.clone()),
        Some(model::arrow_serialization_options::CompressionCodec::Zstd)
    );
    // SAFETY: `region` is the live mapping returned by the reader and nothing writes to it concurrently.
    let slice = unsafe { region.as_slice() };
    let header = ShmArrowFrameHeader::from_slice(slice).expect("header");
    assert_eq!(header.byte_len as usize, byte_len);
    let payload = &slice[ShmArrowFrameHeader::SIZE..ShmArrowFrameHeader::SIZE + byte_len];
    let expected: Vec<u8> = [
        b"SCHEMA00".as_slice(),
        b"BATCH-A0",
        b"BATCH-A1",
        b"BATCH-B0",
        &ARROW_IPC_EOS_MARKER,
    ]
    .concat();
    assert_eq!(payload, expected.as_slice());
    assert!(shm_name.starts_with('/'));
}

#[tokio::test]
async fn test_bigquery_storage_write_default_pending_and_row_errors() {
    let committed_streams: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let committed_clone = Arc::clone(&committed_streams);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let committed = Arc::clone(&committed_clone);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: Request<Incoming>| {
                    let committed = Arc::clone(&committed);
                    async move {
                        let path = req.uri().path().to_string();
                        let raw_body = req.into_body().collect().await.unwrap().to_bytes();
                        let mut dec = LpmFrameDecoder::new(64 * 1024 * 1024);
                        dec.push_chunk(&raw_body);

                        if path.ends_with("/CreateWriteStream") {
                            let mut w = ProtoWriter::new();
                            w.write_str(
                                1,
                                "projects/p1/datasets/d1/tables/t1/streams/pending-stream-1",
                            );
                            Ok::<_, Infallible>(make_grpc_response(
                                vec![w.finish()],
                                GrpcCode::Ok,
                                "",
                            ))
                        } else if path.ends_with("/AppendRows") {
                            let mut resp_frames = Vec::new();
                            let mut msg_idx = 0usize;
                            let mut current_stream = String::new();
                            while let Some(frame) = dec.next_frame().unwrap() {
                                let parsed = decode_append_rows_request(&frame).unwrap();
                                if msg_idx == 0 {
                                    assert!(!parsed.write_stream.is_empty());
                                    assert!(parsed.writer_schema.is_some());
                                    current_stream = parsed.write_stream.clone();
                                }
                                msg_idx += 1;

                                if current_stream.contains("reject_table") {
                                    let mut w = ProtoWriter::new();
                                    w.write_nested(5, |re| {
                                        re.write_i64(1, 0);
                                        re.write_i32(2, 3);
                                        re.write_str(3, "schema mismatch on column x");
                                    });
                                    resp_frames.push(w.finish());
                                } else {
                                    let mut w = ProtoWriter::new();
                                    w.write_nested(1, |res| {
                                        res.write_nested(1, |off| {
                                            off.write_i64(1, parsed.offset.unwrap_or(0));
                                        });
                                    });
                                    resp_frames.push(w.finish());
                                }
                            }
                            Ok::<_, Infallible>(make_grpc_response(resp_frames, GrpcCode::Ok, ""))
                        } else if path.ends_with("/FinalizeWriteStream") {
                            let mut w = ProtoWriter::new();
                            w.write_i64(1, 5);
                            Ok::<_, Infallible>(make_grpc_response(
                                vec![w.finish()],
                                GrpcCode::Ok,
                                "",
                            ))
                        } else if path.ends_with("/BatchCommitWriteStreams") {
                            let frame = dec.next_frame().unwrap().unwrap_or_default();
                            let mut r = ProtoReader::new(&frame);
                            while let Some((f, wt)) = r.next_tag().unwrap() {
                                if f == 2 && wt == WireType::LengthDelimited {
                                    committed
                                        .lock()
                                        .await
                                        .push(r.read_str().unwrap().to_string());
                                } else {
                                    r.skip_field(wt).unwrap();
                                }
                            }
                            let mut w = ProtoWriter::new();
                            w.write_nested(1, |ts| {
                                ts.write_i64(1, 1_700_000_000);
                            });
                            Ok::<_, Infallible>(make_grpc_response(
                                vec![w.finish()],
                                GrpcCode::Ok,
                                "",
                            ))
                        } else {
                            Ok::<_, Infallible>(make_grpc_response(
                                vec![],
                                GrpcCode::Unimplemented,
                                "unimplemented",
                            ))
                        }
                    }
                });
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });

    let mut cfg = BigQueryClientConfig::new("p1");
    cfg.dataset_id = Some("d1".to_string());
    cfg.location = Some("US".to_string());
    cfg.storage_endpoint = format!("http://{addr}");
    let client = BigQueryClient::new(cfg).expect("client");

    let ipc_bytes = build_mock_arrow_ipc_stream(
        b"SCHEMA_PAYLOAD_BYTES",
        &[(b"BATCH_1_DATA", 3), (b"BATCH_2_DATA", 2)],
    );

    let default_res = client
        .append_arrow_stream("projects/p1/datasets/d1/tables/t1", &ipc_bytes, "_default")
        .await
        .expect("default append");
    assert_eq!(default_res.appended_rows, 5);
    assert!(default_res.write_stream.ends_with("/streams/_default"));

    let pending_res = client
        .append_arrow_stream("projects/p1/datasets/d1/tables/t1", &ipc_bytes, "PENDING")
        .await
        .expect("pending append");
    assert_eq!(pending_res.appended_rows, 5);
    assert_eq!(
        pending_res.write_stream,
        "projects/p1/datasets/d1/tables/t1/streams/pending-stream-1"
    );
    assert_eq!(
        *committed_streams.lock().await,
        vec!["projects/p1/datasets/d1/tables/t1/streams/pending-stream-1".to_string()]
    );

    let err = client
        .append_arrow_stream(
            "projects/p1/datasets/d1/tables/reject_table",
            &ipc_bytes,
            "_default",
        )
        .await
        .expect_err("row_errors should fail");
    match err {
        BigQueryError::RowErrors(errors) => {
            assert!(!errors.is_empty());
            assert_eq!(errors[0].index, 0);
            assert!(errors[0].message.contains("schema mismatch"));
        }
        other => panic!("expected RowErrors, got {other:?}"),
    }
}
