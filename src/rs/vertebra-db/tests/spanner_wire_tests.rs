//! Integration and wire-level tests for `vertebra_db::spanner` (Cloud Spanner v1 gRPC multiplexed
//! sessions, `x-goog-spanner-route-to-leader` routing, `PartialResultSet` chunk merging +
//! `resume_token` replay, `ReadWrite` transaction state machine with `MultiplexedSessionPrecommitToken`
//! and wound-wait `Aborted` retry, and parallel `PartitionQuery` + Data Boost into `VTBARROW` SHM).

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code as RpcCode, Status as RpcStatus};
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::response::Response as GaxResponse;
use google_cloud_gax::streaming::ResponseStream;
use google_cloud_spanner::channel_pool::StaticChannelPoolConfig;
use google_cloud_spanner::client::{Spanner, SpannerBuilderExt};
use google_cloud_spanner::model;
use google_cloud_spanner::stub::Spanner as SpannerStub;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{HeaderMap, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message as _;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use vertebra_db::grpc::{DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE, ProtoWriter, encode_lpm_frame};
use vertebra_db::spanner::{
    MultiplexedSessionPrecommitToken, PartialResultSetAssembler, PartialResultSetFrame,
    ProtobufValue, SpannerBatchDmlStatement, SpannerClient, SpannerClientConfig, SpannerError,
    SpannerMutation, SpannerQueryParameter, SpannerReadWriteTransaction, SpannerStaleness,
    SpannerStructField, SpannerType, SpannerTypeCode, SpannerValue, rewrite_spangres_placeholders,
};

#[derive(Clone, PartialEq, prost::Message)]
struct ProstSession {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(bool, tag = "6")]
    multiplexed: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstTransaction {
    #[prost(bytes = "vec", tag = "1")]
    id: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstPartitionOptions {
    #[prost(int64, tag = "1")]
    partition_size_bytes: i64,
    #[prost(int64, tag = "2")]
    max_partitions: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstPartitionQueryRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(string, tag = "3")]
    sql: String,
    #[prost(message, optional, tag = "6")]
    partition_options: Option<ProstPartitionOptions>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstPartition {
    #[prost(bytes = "vec", tag = "1")]
    partition_token: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstPartitionResponse {
    #[prost(message, repeated, tag = "1")]
    partitions: Vec<ProstPartition>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstType {
    #[prost(int32, tag = "1")]
    code: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstStructField {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(message, optional, tag = "2")]
    r#type: Option<ProstType>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstStructType {
    #[prost(message, repeated, tag = "1")]
    fields: Vec<ProstStructField>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstResultSetMetadata {
    #[prost(message, optional, tag = "1")]
    row_type: Option<ProstStructType>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstResultSetStats {
    #[prost(int64, optional, tag = "3")]
    row_count_exact: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstExecuteSqlRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(string, tag = "3")]
    sql: String,
    #[prost(message, optional, tag = "4")]
    params: Option<prost_types::Struct>,
    #[prost(map = "string, message", tag = "5")]
    param_types: HashMap<String, ProstType>,
    #[prost(bytes = "vec", tag = "8")]
    partition_token: Vec<u8>,
    #[prost(bool, tag = "16")]
    data_boost_enabled: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstPartialResultSet {
    #[prost(message, optional, tag = "1")]
    metadata: Option<ProstResultSetMetadata>,
    #[prost(message, repeated, tag = "2")]
    values: Vec<prost_types::Value>,
    #[prost(message, optional, tag = "5")]
    stats: Option<ProstResultSetStats>,
}

fn prost_lpm_frame<M: prost::Message>(msg: &M) -> Vec<u8> {
    let encoded = msg.encode_to_vec();
    let len = encoded.len() as u32;
    let mut out = Vec::with_capacity(5 + encoded.len());
    out.push(0);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&encoded);
    out
}

fn prost_grpc_ok(frames: Vec<Vec<u8>>) -> Response<Full<Bytes>> {
    let payload = frames.into_iter().flatten().collect::<Vec<u8>>();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .header("grpc-status", "0")
        .body(Full::new(Bytes::from(payload)))
        .expect("grpc ok")
}

fn prost_grpc_err(code: i32, message: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .header("grpc-status", code.to_string())
        .header("grpc-message", message)
        .body(Full::new(Bytes::new()))
        .expect("grpc err")
}

fn prost_value_to_json(val: &prost_types::Value) -> serde_json::Value {
    match &val.kind {
        Some(prost_types::value::Kind::NullValue(_)) | None => serde_json::Value::Null,
        Some(prost_types::value::Kind::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(prost_types::value::Kind::NumberValue(n)) => serde_json::json!(*n),
        Some(prost_types::value::Kind::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(prost_types::value::Kind::ListValue(list)) => {
            serde_json::Value::Array(list.values.iter().map(prost_value_to_json).collect())
        }
        Some(prost_types::value::Kind::StructValue(st)) => serde_json::Value::Object(
            st.fields
                .iter()
                .map(|(k, v)| (k.clone(), prost_value_to_json(v)))
                .collect(),
        ),
    }
}

fn json_to_prost_value(val: &serde_json::Value) -> prost_types::Value {
    let kind = match val {
        serde_json::Value::Null => prost_types::value::Kind::NullValue(0),
        serde_json::Value::Bool(b) => prost_types::value::Kind::BoolValue(*b),
        serde_json::Value::Number(n) => {
            prost_types::value::Kind::NumberValue(n.as_f64().unwrap_or(0.0))
        }
        serde_json::Value::String(s) => prost_types::value::Kind::StringValue(s.clone()),
        serde_json::Value::Array(items) => {
            prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: items.iter().map(json_to_prost_value).collect(),
            })
        }
        serde_json::Value::Object(map) => {
            prost_types::value::Kind::StructValue(prost_types::Struct {
                fields: map
                    .iter()
                    .map(|(k, v)| (k.clone(), json_to_prost_value(v)))
                    .collect(),
            })
        }
    };
    prost_types::Value { kind: Some(kind) }
}

#[derive(Clone, Debug, Default)]
struct RecordingSpanner {
    sql_requests: Arc<Mutex<Vec<model::ExecuteSqlRequest>>>,
    partition_requests: Arc<Mutex<Vec<model::PartitionQueryRequest>>>,
    partitions: Vec<Vec<u8>>,
    partition_error: bool,
}

impl google_cloud_spanner::stub::Spanner for RecordingSpanner {
    async fn create_session(
        &self,
        _req: model::CreateSessionRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<GaxResponse<model::Session>> {
        Ok(GaxResponse::from(
            model::Session::new()
                .set_name("projects/p/instances/i/databases/d/sessions/mux1")
                .set_multiplexed(true),
        ))
    }

    async fn begin_transaction(
        &self,
        _req: model::BeginTransactionRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<GaxResponse<model::Transaction>> {
        Ok(GaxResponse::from(
            model::Transaction::new().set_id(b"ro-tx".to_vec()),
        ))
    }

    async fn partition_query(
        &self,
        req: model::PartitionQueryRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<GaxResponse<model::PartitionResponse>> {
        self.partition_requests.lock().unwrap().push(req);
        if self.partition_error {
            return Err(GaxError::service(
                RpcStatus::default()
                    .set_code(RpcCode::InvalidArgument)
                    .set_message("query is not root partitionable"),
            ));
        }
        let partitions = self
            .partitions
            .iter()
            .cloned()
            .map(|token| model::Partition::new().set_partition_token(token))
            .collect::<Vec<_>>();
        Ok(GaxResponse::from(
            model::PartitionResponse::new().set_partitions(partitions),
        ))
    }

    async fn execute_streaming_sql(
        &self,
        req: model::ExecuteSqlRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<ResponseStream<model::PartialResultSet>> {
        let token = req.partition_token.to_vec();
        self.sql_requests.lock().unwrap().push(req);
        let values = match token.as_slice() {
            b"p0" => vec![
                serde_json::Value::String("1".into()),
                serde_json::Value::String("a".into()),
            ],
            b"p1" => vec![
                serde_json::Value::String("2".into()),
                serde_json::Value::String("b".into()),
            ],
            _ => vec![
                serde_json::Value::String("1".into()),
                serde_json::Value::String("a".into()),
                serde_json::Value::String("2".into()),
                serde_json::Value::String("b".into()),
            ],
        };
        let row_type = model::StructType::new().set_fields(vec![
            model::struct_type::Field::new()
                .set_name("id")
                .set_type(model::Type::new().set_code(model::TypeCode::Int64)),
            model::struct_type::Field::new()
                .set_name("name")
                .set_type(model::Type::new().set_code(model::TypeCode::String)),
        ]);
        let prs = model::PartialResultSet::new()
            .set_metadata(model::ResultSetMetadata::new().set_row_type(row_type))
            .set_stats(model::ResultSetStats::new().set_row_count_exact(2))
            .set_values(values);
        let (tx, rx) = mpsc::channel(2);
        tx.try_send(Ok(prs)).expect("send partial result set");
        drop(tx);
        Ok(ResponseStream::from(rx))
    }
}

async fn stub_client(stub: RecordingSpanner) -> SpannerClient {
    if stub.partition_error {
        return SpannerClient::with_clients(
            Spanner::from_stub(stub),
            None,
            SpannerClientConfig::new("projects/p/instances/i/databases/d"),
        )
        .expect("client");
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let stub = stub.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: Request<Incoming>| {
                    let stub = stub.clone();
                    async move {
                        let path = req.uri().path().to_string();
                        let raw = req
                            .into_body()
                            .collect()
                            .await
                            .map(|c| c.to_bytes().to_vec())
                            .unwrap_or_default();
                        let payload = if raw.len() >= 5 { &raw[5..] } else { &raw[..] };
                        let resp = match path.as_str() {
                            "/google.spanner.v1.Spanner/CreateSession" => {
                                let s = SpannerStub::create_session(
                                    &stub,
                                    model::CreateSessionRequest::new(),
                                    RequestOptions::default(),
                                )
                                .await
                                .expect("create_session")
                                .into_body();
                                prost_grpc_ok(vec![prost_lpm_frame(&ProstSession {
                                    name: s.name,
                                    multiplexed: s.multiplexed,
                                })])
                            }
                            "/google.spanner.v1.Spanner/BeginTransaction" => {
                                let tx = SpannerStub::begin_transaction(
                                    &stub,
                                    model::BeginTransactionRequest::new(),
                                    RequestOptions::default(),
                                )
                                .await
                                .expect("begin_transaction")
                                .into_body();
                                prost_grpc_ok(vec![prost_lpm_frame(&ProstTransaction {
                                    id: tx.id.to_vec(),
                                })])
                            }
                            "/google.spanner.v1.Spanner/PartitionQuery" => {
                                let decoded = ProstPartitionQueryRequest::decode(payload)
                                    .expect("decode PartitionQueryRequest");
                                let mut mreq = model::PartitionQueryRequest::new()
                                    .set_session(decoded.session)
                                    .set_sql(decoded.sql);
                                if let Some(opts) = decoded.partition_options {
                                    mreq = mreq.set_partition_options(
                                        model::PartitionOptions::new()
                                            .set_partition_size_bytes(opts.partition_size_bytes)
                                            .set_max_partitions(opts.max_partitions),
                                    );
                                }
                                match SpannerStub::partition_query(
                                    &stub,
                                    mreq,
                                    RequestOptions::default(),
                                )
                                .await
                                {
                                    Ok(resp) => {
                                        let body = resp.into_body();
                                        let partitions = body
                                            .partitions
                                            .into_iter()
                                            .map(|p| ProstPartition {
                                                partition_token: p.partition_token.to_vec(),
                                            })
                                            .collect();
                                        prost_grpc_ok(vec![prost_lpm_frame(
                                            &ProstPartitionResponse { partitions },
                                        )])
                                    }
                                    Err(err) => prost_grpc_err(3, &err.to_string()),
                                }
                            }
                            "/google.spanner.v1.Spanner/ExecuteStreamingSql" => {
                                let decoded = ProstExecuteSqlRequest::decode(payload)
                                    .expect("decode ExecuteSqlRequest");
                                let mut mreq = model::ExecuteSqlRequest::new()
                                    .set_session(decoded.session)
                                    .set_sql(decoded.sql)
                                    .set_partition_token(decoded.partition_token)
                                    .set_data_boost_enabled(decoded.data_boost_enabled);
                                if let Some(params) = decoded.params {
                                    let map = params
                                        .fields
                                        .iter()
                                        .map(|(k, v)| (k.clone(), prost_value_to_json(v)))
                                        .collect::<serde_json::Map<String, serde_json::Value>>();
                                    mreq = mreq.set_params(map);
                                }
                                if !decoded.param_types.is_empty() {
                                    let pt = decoded
                                        .param_types
                                        .into_iter()
                                        .map(|(k, v)| {
                                            (
                                                k,
                                                model::Type::new()
                                                    .set_code(model::TypeCode::from(v.code)),
                                            )
                                        })
                                        .collect::<HashMap<String, model::Type>>();
                                    mreq = mreq.set_param_types(pt);
                                }
                                let mut stream = SpannerStub::execute_streaming_sql(
                                    &stub,
                                    mreq,
                                    RequestOptions::default(),
                                )
                                .await
                                .expect("execute_streaming_sql");
                                let mut frames = Vec::new();
                                while let Some(item) = stream.next().await {
                                    let prs = item.expect("prs item");
                                    let metadata = prs.metadata.map(|m| ProstResultSetMetadata {
                                        row_type: m.row_type.map(|rt| ProstStructType {
                                            fields: rt
                                                .fields
                                                .into_iter()
                                                .map(|f| ProstStructField {
                                                    name: f.name,
                                                    r#type: f.r#type.map(|t| ProstType {
                                                        code: t.code.value().unwrap_or(0),
                                                    }),
                                                })
                                                .collect(),
                                        }),
                                    });
                                    let stats = prs.stats.map(|s| ProstResultSetStats {
                                        row_count_exact: s.row_count_exact().copied(),
                                    });
                                    let values =
                                        prs.values.iter().map(json_to_prost_value).collect();
                                    frames.push(prost_lpm_frame(&ProstPartialResultSet {
                                        metadata,
                                        values,
                                        stats,
                                    }));
                                }
                                prost_grpc_ok(frames)
                            }
                            _ => prost_grpc_err(12, "unimplemented"),
                        };
                        Ok::<_, Infallible>(resp)
                    }
                });
                let _ = http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    let spanner = Spanner::builder()
        .with_endpoint(format!("http://{addr}"))
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(StaticChannelPoolConfig::new(1))
        .build()
        .await
        .expect("build spanner");
    SpannerClient::with_clients(
        spanner,
        None,
        SpannerClientConfig::new("projects/p/instances/i/databases/d"),
    )
    .expect("client")
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
async fn query_maps_staleness_params_and_decodes_rows() {
    let stub = RecordingSpanner::default();
    let client = stub_client(stub.clone()).await;
    let params = vec![SpannerQueryParameter::from_value(
        "p1",
        &SpannerValue::Int64(7),
    )];
    let result = client
        .query(
            "SELECT id, name FROM t WHERE id > @p1",
            &params,
            SpannerStaleness::MaxStalenessMs(15_000),
        )
        .await
        .expect("query");
    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        result.rows,
        vec![
            vec![SpannerValue::Int64(1), SpannerValue::String("a".into())],
            vec![SpannerValue::Int64(2), SpannerValue::String("b".into())],
        ]
    );
    assert_eq!(result.row_count_exact, Some(2));
    let sent = stub.sql_requests.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].sql, "SELECT id, name FROM t WHERE id > @p1");
    assert!(
        sent[0]
            .params
            .as_ref()
            .is_some_and(|p| p.contains_key("p1"))
    );
    assert_eq!(sent[0].param_types["p1"].code, model::TypeCode::Int64);
}

#[tokio::test]
async fn query_rewrites_positional_placeholders_for_googlesql() {
    let stub = RecordingSpanner::default();
    let client = stub_client(stub.clone()).await;
    let params = vec![SpannerQueryParameter::from_value(
        "p1",
        &SpannerValue::Int64(7),
    )];
    client
        .query(
            "SELECT id, name FROM t WHERE id > $1",
            &params,
            SpannerStaleness::Strong,
        )
        .await
        .expect("query");
    assert_eq!(
        stub.sql_requests.lock().unwrap()[0].sql,
        "SELECT id, name FROM t WHERE id > @p1"
    );
}

#[tokio::test]
async fn partitioned_query_sets_data_boost_and_concatenates_partitions() {
    let stub = RecordingSpanner {
        partitions: vec![b"p0".to_vec(), b"p1".to_vec()],
        ..Default::default()
    };
    let client = stub_client(stub.clone()).await;
    let result = client
        .partitioned_query(
            "SELECT id, name FROM t",
            &[],
            4,
            true,
            SpannerStaleness::Strong,
        )
        .await
        .expect("partitioned query");
    assert_eq!(
        result.rows,
        vec![
            vec![SpannerValue::Int64(1), SpannerValue::String("a".into())],
            vec![SpannerValue::Int64(2), SpannerValue::String("b".into())],
        ]
    );
    assert_eq!(
        stub.partition_requests.lock().unwrap()[0]
            .partition_options
            .as_ref()
            .map(|o| o.max_partitions),
        Some(4)
    );
    let sent = stub.sql_requests.lock().unwrap();
    assert_eq!(
        sent.iter()
            .map(|r| r.partition_token.to_vec())
            .collect::<Vec<_>>(),
        vec![b"p0".to_vec(), b"p1".to_vec()]
    );
    assert!(sent.iter().all(|r| r.data_boost_enabled));
}

#[tokio::test]
async fn partitioned_query_propagates_partition_errors() {
    let stub = RecordingSpanner {
        partition_error: true,
        ..Default::default()
    };
    let client = stub_client(stub.clone()).await;
    let outcome = client
        .partitioned_query("SELECT 1", &[], 4, true, SpannerStaleness::Strong)
        .await;
    assert!(matches!(outcome, Err(SpannerError::Client(_))));
    assert!(stub.sql_requests.lock().unwrap().is_empty());
}

#[test]
fn ca_pem_policy_follows_endpoint_host() {
    let pem = b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n".to_vec();
    let mut cloud = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    cloud.ca_pem = Some(pem.clone());
    assert!(matches!(
        SpannerClient::new(cloud),
        Err(SpannerError::InvalidArgument(_))
    ));
    let mut omni = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    omni.endpoint = "https://omni.internal:15000".into();
    omni.ca_pem = Some(pem.clone());
    assert!(SpannerClient::new(omni).is_ok());
    let mut plaintext = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    plaintext.endpoint = "http://127.0.0.1:9010".into();
    plaintext.ca_pem = Some(pem);
    match SpannerClient::new(plaintext) {
        Err(SpannerError::InvalidArgument(message)) => {
            assert_eq!(message, "ca_pem requires an https:// endpoint")
        }
        other => panic!("expected InvalidArgument, got {:?}", other.err()),
    }
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
