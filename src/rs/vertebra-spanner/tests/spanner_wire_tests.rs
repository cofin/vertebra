//! Integration tests for `_spanner::client` backed by the official
//! `google-cloud-spanner` client and stub seams.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use _spanner::client::{
    ProtobufValue, SpannerBatchDmlStatement, SpannerClient, SpannerClientConfig, SpannerError,
    SpannerMutation, SpannerMutationGroup, SpannerQueryParameter, SpannerStaleness, SpannerValue,
    rewrite_spangres_placeholders,
};
use bytes::Bytes;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code as RpcCode, Status as RpcStatus};
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::response::Response as GaxResponse;
use google_cloud_gax::streaming::ResponseStream;
use google_cloud_longrunning::model::Operation;
use google_cloud_longrunning::model::operation::Result as OperationResult;
use google_cloud_spanner::channel_pool::StaticChannelPoolConfig;
use google_cloud_spanner::client::{DatabaseAdmin, Spanner, SpannerBuilderExt};
use google_cloud_spanner::model;
use google_cloud_spanner::stub::Spanner as SpannerStub;
use google_cloud_spanner::transaction::Timestamp;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message as _;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

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

#[derive(Clone, PartialEq, prost::Message)]
struct ProstRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProstBatchWriteResponse {
    #[prost(int32, repeated, tag = "1")]
    indexes: Vec<i32>,
    #[prost(message, optional, tag = "2")]
    status: Option<ProstRpcStatus>,
    #[prost(message, optional, tag = "3")]
    commit_timestamp: Option<prost_types::Timestamp>,
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
    batch_dml_requests: Arc<Mutex<Vec<model::ExecuteBatchDmlRequest>>>,
    commit_requests: Arc<Mutex<Vec<model::CommitRequest>>>,
    partition_requests: Arc<Mutex<Vec<model::PartitionQueryRequest>>>,
    partitions: Vec<Vec<u8>>,
    partition_error: bool,
    omit_commit_timestamp: bool,
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
            model::Transaction::new().set_id(b"rw-tx".to_vec()),
        ))
    }

    async fn execute_sql(
        &self,
        req: model::ExecuteSqlRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<GaxResponse<model::ResultSet>> {
        self.sql_requests.lock().unwrap().push(req);
        let rs = model::ResultSet::new()
            .set_metadata(
                model::ResultSetMetadata::new()
                    .set_transaction(model::Transaction::new().set_id(b"rw-tx".to_vec())),
            )
            .set_stats(model::ResultSetStats::new().set_row_count_exact(3));
        Ok(GaxResponse::from(rs))
    }

    async fn execute_batch_dml(
        &self,
        req: model::ExecuteBatchDmlRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<GaxResponse<model::ExecuteBatchDmlResponse>> {
        self.batch_dml_requests.lock().unwrap().push(req);
        let rs1 = model::ResultSet::new()
            .set_metadata(
                model::ResultSetMetadata::new()
                    .set_transaction(model::Transaction::new().set_id(b"rw-tx".to_vec())),
            )
            .set_stats(model::ResultSetStats::new().set_row_count_exact(2));
        let rs2 =
            model::ResultSet::new().set_stats(model::ResultSetStats::new().set_row_count_exact(5));
        let resp = model::ExecuteBatchDmlResponse::new()
            .set_result_sets(vec![rs1, rs2])
            .set_status(google_cloud_rpc::model::Status::new().set_code(0));
        Ok(GaxResponse::from(resp))
    }

    async fn commit(
        &self,
        req: model::CommitRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<GaxResponse<model::CommitResponse>> {
        self.commit_requests.lock().unwrap().push(req);
        let mut resp = model::CommitResponse::new();
        if !self.omit_commit_timestamp {
            resp = resp.set_commit_timestamp(Timestamp::new(1_700_000_000, 0).expect("ts"));
        }
        Ok(GaxResponse::from(resp))
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

    async fn batch_write(
        &self,
        _req: model::BatchWriteRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<ResponseStream<model::BatchWriteResponse>> {
        let r0 = model::BatchWriteResponse::new()
            .set_indexes(vec![0])
            .set_status(google_cloud_rpc::model::Status::new().set_code(0))
            .set_commit_timestamp(Timestamp::new(1_700_000_000, 0).expect("ts"));
        let r1 = model::BatchWriteResponse::new()
            .set_indexes(vec![1])
            .set_status(
                google_cloud_rpc::model::Status::new()
                    .set_code(6)
                    .set_message("already exists"),
            );
        let (tx, rx) = mpsc::channel(4);
        tx.try_send(Ok(r0)).expect("send batch_write r0");
        tx.try_send(Ok(r1)).expect("send batch_write r1");
        drop(tx);
        Ok(ResponseStream::from(rx))
    }
}

#[derive(Clone, Copy, Debug, Default)]
enum AdminMode {
    #[default]
    Succeed,
    Fail,
    NeverDone,
}

#[derive(Clone, Debug, Default)]
struct RecordingAdmin {
    mode: AdminMode,
    statements: Arc<Mutex<Vec<String>>>,
}

impl google_cloud_spanner_admin_database_v1::stub::DatabaseAdmin for RecordingAdmin {
    async fn update_database_ddl(
        &self,
        req: google_cloud_spanner_admin_database_v1::model::UpdateDatabaseDdlRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<GaxResponse<Operation>> {
        *self.statements.lock().unwrap() = req.statements;
        Ok(GaxResponse::from(
            Operation::new()
                .set_name("projects/p/instances/i/databases/d/operations/op1")
                .set_done(false),
        ))
    }

    async fn get_operation(
        &self,
        req: google_cloud_longrunning::model::GetOperationRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<GaxResponse<Operation>> {
        let op = match self.mode {
            AdminMode::NeverDone => Operation::new().set_name(req.name).set_done(false),
            AdminMode::Succeed => Operation::new().set_name(req.name).set_done(true),
            AdminMode::Fail => Operation::new()
                .set_name(req.name)
                .set_done(true)
                .set_result(OperationResult::Error(Box::new(
                    google_cloud_rpc::model::Status::new()
                        .set_code(3)
                        .set_message("Duplicate name in schema: t"),
                ))),
        };
        Ok(GaxResponse::from(op))
    }
}

fn stub_client_with_admin(
    stub: RecordingSpanner,
    admin: RecordingAdmin,
    ddl_timeout: Duration,
) -> SpannerClient {
    let mut cfg = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    cfg.ddl_timeout = ddl_timeout;
    SpannerClient::with_clients(
        Spanner::from_stub(stub),
        Some(DatabaseAdmin::from_stub(admin)),
        cfg,
    )
    .expect("client")
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
                            "/google.spanner.v1.Spanner/BatchWrite" => {
                                let mut stream = SpannerStub::batch_write(
                                    &stub,
                                    model::BatchWriteRequest::new(),
                                    RequestOptions::default(),
                                )
                                .await
                                .expect("batch_write");
                                let mut frames = Vec::new();
                                while let Some(item) = stream.next().await {
                                    let bwr = item.expect("bwr item");
                                    let status = bwr.status.map(|st| ProstRpcStatus {
                                        code: st.code,
                                        message: st.message,
                                    });
                                    let commit_timestamp =
                                        bwr.commit_timestamp.map(|ts| prost_types::Timestamp {
                                            seconds: ts.seconds(),
                                            nanos: ts.nanos(),
                                        });
                                    frames.push(prost_lpm_frame(&ProstBatchWriteResponse {
                                        indexes: bwr.indexes,
                                        status,
                                        commit_timestamp,
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
async fn execute_dml_runs_in_read_write_transaction_with_tags() {
    let stub = RecordingSpanner::default();
    let client = stub_client_with_admin(
        stub.clone(),
        RecordingAdmin::default(),
        Duration::from_secs(5),
    );
    let params = vec![SpannerQueryParameter::from_value(
        "p1",
        &SpannerValue::Int64(1),
    )];
    let count = client
        .execute_dml(
            "UPDATE t SET v = 1 WHERE id = @p1",
            &params,
            Some("req-a"),
            Some("tx-a"),
        )
        .await
        .expect("execute_dml");
    assert_eq!(count, 3);
    let sql_reqs = stub.sql_requests.lock().unwrap();
    assert_eq!(sql_reqs.len(), 1);
    assert_eq!(
        sql_reqs[0]
            .request_options
            .as_ref()
            .map(|o| o.request_tag.as_str()),
        Some("req-a")
    );
    let commit_reqs = stub.commit_requests.lock().unwrap();
    assert_eq!(commit_reqs.len(), 1);
    assert_eq!(
        commit_reqs[0]
            .request_options
            .as_ref()
            .map(|o| o.transaction_tag.as_str()),
        Some("tx-a")
    );
}

#[tokio::test]
async fn execute_batch_dml_returns_row_counts() {
    let stub = RecordingSpanner::default();
    let client = stub_client_with_admin(
        stub.clone(),
        RecordingAdmin::default(),
        Duration::from_secs(5),
    );
    let stmts = vec![
        SpannerBatchDmlStatement {
            sql: "UPDATE t SET v = 1 WHERE id = 1".into(),
            params: vec![],
        },
        SpannerBatchDmlStatement {
            sql: "UPDATE t SET v = 2 WHERE id = 2".into(),
            params: vec![],
        },
    ];
    let counts = client
        .execute_batch_dml(&stmts, Some("batch-req"), Some("batch-tx"))
        .await
        .expect("execute_batch_dml");
    assert_eq!(counts, vec![2, 5]);
}

#[tokio::test]
async fn apply_mutations_commits_insert_rows() {
    let stub = RecordingSpanner::default();
    let client = stub_client_with_admin(
        stub.clone(),
        RecordingAdmin::default(),
        Duration::from_secs(5),
    );
    let mutation = SpannerMutation::insert(
        "t",
        vec!["id".into(), "v".into()],
        vec![vec![
            ProtobufValue::String("1".into()),
            ProtobufValue::String("x".into()),
        ]],
    );
    let res = client
        .apply_mutations(&[mutation], Some("mut-tx"), false)
        .await
        .expect("apply_mutations");
    assert_eq!(res.commit_timestamp_micros, Some(1_700_000_000_000_000));
    let commits = stub.commit_requests.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].mutations.len(), 1);
    let insert = commits[0].mutations[0].insert().expect("insert mutation");
    assert_eq!(insert.table, "t");
    assert_eq!(insert.columns, vec!["id".to_string(), "v".to_string()]);
}

#[tokio::test]
async fn apply_mutations_rejects_commit_without_timestamp() {
    let stub = RecordingSpanner {
        omit_commit_timestamp: true,
        ..Default::default()
    };
    let client = stub_client_with_admin(stub, RecordingAdmin::default(), Duration::from_secs(5));
    let mutation = SpannerMutation::insert(
        "t",
        vec!["id".into()],
        vec![vec![ProtobufValue::String("1".into())]],
    );
    match client.apply_mutations(&[mutation], None, false).await {
        Err(SpannerError::Protocol(msg)) => assert!(msg.contains("commit_timestamp")),
        other => panic!("expected Protocol error, got {other:?}"),
    }
}

#[tokio::test]
async fn batch_write_reports_group_status() {
    let stub = RecordingSpanner::default();
    let client = stub_client(stub).await;
    let g0 = SpannerMutationGroup {
        mutations: vec![SpannerMutation::insert(
            "t",
            vec!["id".into()],
            vec![vec![ProtobufValue::String("1".into())]],
        )],
    };
    let g1 = SpannerMutationGroup {
        mutations: vec![SpannerMutation::insert(
            "t",
            vec!["id".into()],
            vec![vec![ProtobufValue::String("2".into())]],
        )],
    };
    let results = client
        .batch_write(&[g0, g1], false)
        .await
        .expect("batch_write");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].indexes, vec![0]);
    assert_eq!(results[0].status_code, 0);
    assert_eq!(results[1].indexes, vec![1]);
    assert_eq!(results[1].status_code, 6);
    assert_eq!(results[1].status_message, "already exists");
}

#[tokio::test]
async fn update_database_ddl_returns_operation_name() {
    let admin = RecordingAdmin {
        mode: AdminMode::Succeed,
        ..Default::default()
    };
    let client = stub_client_with_admin(
        RecordingSpanner::default(),
        admin.clone(),
        Duration::from_secs(5),
    );
    let op_name = client
        .update_database_ddl(&["CREATE TABLE t (id INT64) PRIMARY KEY (id)".into()])
        .await
        .expect("update_database_ddl");
    assert_eq!(op_name, "projects/p/instances/i/databases/d/operations/op1");
    assert_eq!(
        *admin.statements.lock().unwrap(),
        vec!["CREATE TABLE t (id INT64) PRIMARY KEY (id)".to_string()]
    );
}

#[tokio::test]
async fn update_database_ddl_surfaces_operation_error() {
    let admin = RecordingAdmin {
        mode: AdminMode::Fail,
        ..Default::default()
    };
    let client = stub_client_with_admin(RecordingSpanner::default(), admin, Duration::from_secs(5));
    match client
        .update_database_ddl(&["CREATE TABLE t (id INT64) PRIMARY KEY (id)".into()])
        .await
    {
        Err(SpannerError::Ddl {
            operation,
            code,
            message,
        }) => {
            assert!(operation.ends_with("/operations/op1"));
            assert_eq!(code, 3);
            assert!(message.contains("Duplicate name in schema"));
        }
        other => panic!("expected SpannerError::Ddl, got {other:?}"),
    }
}

#[tokio::test]
async fn update_database_ddl_times_out_while_operation_pending() {
    let admin = RecordingAdmin {
        mode: AdminMode::NeverDone,
        ..Default::default()
    };
    let client = stub_client_with_admin(
        RecordingSpanner::default(),
        admin,
        Duration::from_millis(200),
    );
    let started = std::time::Instant::now();
    match client
        .update_database_ddl(&["CREATE TABLE t (id INT64) PRIMARY KEY (id)".into()])
        .await
    {
        Err(SpannerError::DdlTimeout {
            operation,
            timeout_ms,
        }) => {
            assert!(operation.ends_with("/operations/op1"));
            assert_eq!(timeout_ms, 200);
            assert!(started.elapsed() < Duration::from_secs(5));
        }
        other => panic!("expected SpannerError::DdlTimeout, got {other:?}"),
    }
}

#[test]
fn admin_endpoint_rule() {
    let mut explicit = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    explicit.endpoint = "http://localhost:7000".into();
    explicit.admin_endpoint = Some("http://admin.local:8080".into());
    assert_eq!(
        SpannerClient::new(explicit)
            .expect("client")
            .effective_admin_endpoint()
            .expect("admin endpoint"),
        "http://admin.local:8080"
    );

    let default_cfg = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    assert_eq!(
        SpannerClient::new(default_cfg)
            .expect("client")
            .effective_admin_endpoint()
            .expect("admin endpoint"),
        "https://spanner.googleapis.com"
    );

    let mut emulator = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    emulator.endpoint = "http://localhost:9010".into();
    assert_eq!(
        SpannerClient::new(emulator)
            .expect("client")
            .effective_admin_endpoint()
            .expect("admin endpoint"),
        "http://localhost:9020"
    );

    let mut unknown = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    unknown.endpoint = "http://localhost:7000".into();
    assert!(matches!(
        SpannerClient::new(unknown)
            .expect("client")
            .effective_admin_endpoint(),
        Err(SpannerError::InvalidArgument(_))
    ));
}

#[test]
fn spangres_placeholder_rewriter_lexes_googlesql_literals_and_comments() {
    let cases: [(&str, &str, &str, usize); 8] = [
        (
            "non-ascii literal",
            "SELECT 'é$1ü', $1",
            "SELECT 'é$1ü', @p1",
            1,
        ),
        (
            "backslash escape",
            r"SELECT 'it\'s $1', $2",
            r"SELECT 'it\'s $1', @p2",
            2,
        ),
        (
            "double-quoted string",
            r#"SELECT "a\"$1", $3"#,
            r#"SELECT "a\"$1", @p3"#,
            3,
        ),
        (
            "backtick identifier",
            "SELECT `col$1` FROM t WHERE x = $1",
            "SELECT `col$1` FROM t WHERE x = @p1",
            1,
        ),
        (
            "triple quoted",
            "SELECT '''line $1\n'still' $2''', $4",
            "SELECT '''line $1\n'still' $2''', @p4",
            4,
        ),
        (
            "raw and bytes",
            r"SELECT r'\$1', b'$2', rb'$3', $5",
            r"SELECT r'\$1', b'$2', rb'$3', @p5",
            5,
        ),
        (
            "hash and dash comments",
            "SELECT 1 # $1 ☃\n, 2 -- $2\nWHERE a = $6",
            "SELECT 1 # $1 ☃\n, 2 -- $2\nWHERE a = @p6",
            6,
        ),
        (
            "dollar zero and unterminated",
            "SELECT $0, '$1",
            "SELECT $0, '$1",
            0,
        ),
    ];
    for (label, sql, expected_sql, max_ordinal) in cases {
        let (rewritten, names) = rewrite_spangres_placeholders(sql);
        assert_eq!(rewritten, expected_sql, "{label}");
        assert_eq!(
            names,
            (1..=max_ordinal)
                .map(|n| format!("p{n}"))
                .collect::<Vec<_>>(),
            "{label}"
        );
    }
}

#[tokio::test]
#[ignore = "requires Spanner emulator: set SPANNER_EMULATOR_HOST and VERTEBRA_SPANNER_EMULATOR_DATABASE"]
async fn update_database_ddl_against_emulator() {
    let host = std::env::var("SPANNER_EMULATOR_HOST").expect("SPANNER_EMULATOR_HOST");
    let database = std::env::var("VERTEBRA_SPANNER_EMULATOR_DATABASE")
        .expect("VERTEBRA_SPANNER_EMULATOR_DATABASE");
    let mut cfg = SpannerClientConfig::new(database);
    cfg.endpoint = format!("http://{host}");
    let client = SpannerClient::new(cfg).expect("client");
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("unix time")
        .as_millis();
    let ddl = format!("CREATE TABLE vtb_ddl_probe_{unix_ms} (id INT64) PRIMARY KEY (id)");
    let op_name = client.update_database_ddl(&[ddl]).await.expect("ddl");
    assert!(!op_name.is_empty());
}
