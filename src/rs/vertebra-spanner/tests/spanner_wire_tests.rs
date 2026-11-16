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
    let mut cfg = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    cfg.endpoint = format!("http://{addr}");
    SpannerClient::with_clients(spanner, None, cfg).expect("client")
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
        .apply_mutations(&[mutation], Some("mut-tx"), false, None)
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
    match client.apply_mutations(&[mutation], None, false, None).await {
        Err(SpannerError::Protocol(msg)) => assert!(msg.contains("commit_timestamp")),
        other => panic!("expected Protocol error, got {other:?}"),
    }
}

#[tokio::test]
async fn commit_requests_propagate_max_commit_delay_across_dml_batch_dml_and_mutations() {
    let stub = RecordingSpanner::default();
    let mut cfg = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    cfg.max_commit_delay_ms = Some(25);
    let client = SpannerClient::with_clients(
        Spanner::from_stub(stub.clone()),
        Some(DatabaseAdmin::from_stub(RecordingAdmin::default())),
        cfg,
    )
    .expect("client");

    let dml_count = client
        .execute_dml("UPDATE t SET v = 1 WHERE id = 1", &[], None, Some("tx-dml"))
        .await
        .expect("execute_dml");
    assert_eq!(dml_count, 3);

    let stmts = vec![SpannerBatchDmlStatement {
        sql: "UPDATE t SET v = 2 WHERE id = 2".into(),
        params: vec![],
    }];
    let batch_counts = client
        .execute_batch_dml(&stmts, None, Some("tx-batch"))
        .await
        .expect("execute_batch_dml");
    assert_eq!(batch_counts, vec![2, 5]);

    let mutation = SpannerMutation::insert(
        "t",
        vec!["id".into()],
        vec![vec![ProtobufValue::String("1".into())]],
    );
    client
        .apply_mutations(std::slice::from_ref(&mutation), Some("tx-mut"), false, None)
        .await
        .expect("apply_mutations default delay");
    client
        .apply_mutations(&[mutation], Some("tx-mut-override"), false, Some(40))
        .await
        .expect("apply_mutations override delay");

    let commits = stub.commit_requests.lock().unwrap();
    assert_eq!(commits.len(), 4);
    for idx in 0..3 {
        let delay = commits[idx]
            .max_commit_delay
            .as_ref()
            .unwrap_or_else(|| panic!("commit[{idx}] missing max_commit_delay"));
        assert_eq!(delay.seconds(), 0);
        assert_eq!(delay.nanos(), 25_000_000);
    }
    let override_delay = commits[3]
        .max_commit_delay
        .as_ref()
        .expect("commit[3] missing override max_commit_delay");
    assert_eq!(override_delay.seconds(), 0);
    assert_eq!(override_delay.nanos(), 40_000_000);
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

#[test]
fn spanner_client_precomputes_routing_metadata_and_decodes_owned_nested_values() {
    use _spanner::client::{SpannerStructField, SpannerType, SpannerTypeCode};

    let mut cfg =
        SpannerClientConfig::new("projects/google.com:prod/instances/inst-1/databases/db-1");
    cfg.endpoint = "http://127.0.0.1:9010/".to_string();
    let client = SpannerClient::new(cfg).expect("client");

    assert_eq!(client.effective_endpoint(), "http://127.0.0.1:9010");
    assert_eq!(
        client.routing_header_value(),
        "database=projects%2Fgoogle.com%3Aprod%2Finstances%2Finst-1%2Fdatabases%2Fdb-1"
    );

    let nested_ty = SpannerType::array(SpannerType::struct_type(vec![
        SpannerStructField {
            name: "label".to_string(),
            field_type: SpannerType::scalar(SpannerTypeCode::String),
        },
        SpannerStructField {
            name: "payload".to_string(),
            field_type: SpannerType::scalar(SpannerTypeCode::Json),
        },
    ]));
    let raw = ProtobufValue::List(vec![ProtobufValue::List(vec![
        ProtobufValue::String("item-1".to_string()),
        ProtobufValue::String("{\"ok\":true}".to_string()),
    ])]);
    let decoded = SpannerValue::from_protobuf_value_owned(raw, &nested_ty).expect("decode owned");
    assert_eq!(
        decoded,
        SpannerValue::Array(vec![SpannerValue::Struct(vec![
            (
                "label".to_string(),
                SpannerValue::String("item-1".to_string())
            ),
            (
                "payload".to_string(),
                SpannerValue::Json("{\"ok\":true}".to_string()),
            ),
        ])])
    );
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireRequestOptions {
    #[prost(int32, tag = "1")]
    priority: i32,
    #[prost(string, tag = "2")]
    request_tag: String,
    #[prost(string, tag = "3")]
    transaction_tag: String,
}

#[derive(Clone, PartialEq, Eq, Hash, prost::Message)]
struct RwWirePrecommitToken {
    #[prost(bytes = "vec", tag = "1")]
    precommit_token: Vec<u8>,
    #[prost(int32, tag = "2")]
    seq_num: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireTransaction {
    #[prost(bytes = "vec", tag = "1")]
    id: Vec<u8>,
    #[prost(message, optional, tag = "3")]
    precommit_token: Option<RwWirePrecommitToken>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireReadWrite {
    #[prost(int32, tag = "1")]
    read_lock_mode: i32,
    #[prost(bytes = "vec", tag = "2")]
    multiplexed_session_previous_transaction_id: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireTransactionOptions {
    #[prost(bool, tag = "5")]
    exclude_txn_from_change_streams: bool,
    #[prost(message, optional, tag = "1")]
    read_write: Option<RwWireReadWrite>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireBeginTransactionRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(message, optional, tag = "2")]
    options: Option<RwWireTransactionOptions>,
    #[prost(message, optional, tag = "3")]
    request_options: Option<RwWireRequestOptions>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireTransactionSelector {
    #[prost(bytes = "vec", tag = "2")]
    id: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireExecuteSqlRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(message, optional, tag = "2")]
    transaction: Option<RwWireTransactionSelector>,
    #[prost(string, tag = "3")]
    sql: String,
    #[prost(int64, tag = "9")]
    seqno: i64,
    #[prost(message, optional, tag = "11")]
    request_options: Option<RwWireRequestOptions>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireExecuteBatchDmlStatement {
    #[prost(string, tag = "1")]
    sql: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireExecuteBatchDmlRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(message, optional, tag = "2")]
    transaction: Option<RwWireTransactionSelector>,
    #[prost(message, repeated, tag = "3")]
    statements: Vec<RwWireExecuteBatchDmlStatement>,
    #[prost(int64, tag = "4")]
    seqno: i64,
    #[prost(message, optional, tag = "5")]
    request_options: Option<RwWireRequestOptions>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireResultSetMetadata {
    #[prost(message, optional, tag = "1")]
    row_type: Option<ProstStructType>,
    #[prost(message, optional, tag = "2")]
    transaction: Option<RwWireTransaction>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireResultSet {
    #[prost(message, optional, tag = "1")]
    metadata: Option<RwWireResultSetMetadata>,
    #[prost(message, optional, tag = "3")]
    stats: Option<ProstResultSetStats>,
    #[prost(message, optional, tag = "5")]
    precommit_token: Option<RwWirePrecommitToken>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWirePartialResultSet {
    #[prost(message, optional, tag = "1")]
    metadata: Option<RwWireResultSetMetadata>,
    #[prost(message, repeated, tag = "2")]
    values: Vec<prost_types::Value>,
    #[prost(message, optional, tag = "5")]
    stats: Option<ProstResultSetStats>,
    #[prost(message, optional, tag = "8")]
    precommit_token: Option<RwWirePrecommitToken>,
    #[prost(bool, tag = "9")]
    last: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireRetryInfo {
    #[prost(message, optional, tag = "1")]
    retry_delay: Option<prost_types::Duration>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireExecuteBatchDmlResponse {
    #[prost(message, repeated, tag = "1")]
    result_sets: Vec<RwWireResultSet>,
    #[prost(message, optional, tag = "2")]
    status: Option<RwWireRpcStatus>,
    #[prost(message, optional, tag = "3")]
    precommit_token: Option<RwWirePrecommitToken>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireMutation {
    #[prost(bytes = "vec", tag = "1")]
    insert: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    update: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    insert_or_update: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    replace: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    delete: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireCommitRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(bytes = "vec", tag = "2")]
    transaction_id: Vec<u8>,
    #[prost(message, repeated, tag = "4")]
    mutations: Vec<RwWireMutation>,
    #[prost(message, optional, tag = "6")]
    request_options: Option<RwWireRequestOptions>,
    #[prost(message, optional, tag = "8")]
    max_commit_delay: Option<prost_types::Duration>,
    #[prost(message, optional, tag = "9")]
    precommit_token: Option<RwWirePrecommitToken>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, prost::Message)]
struct RwWireCommitStats {
    #[prost(int64, tag = "1")]
    mutation_count: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireCommitResponse {
    #[prost(message, optional, tag = "1")]
    commit_timestamp: Option<prost_types::Timestamp>,
    #[prost(message, optional, tag = "2")]
    commit_stats: Option<RwWireCommitStats>,
    #[prost(message, optional, tag = "4")]
    precommit_token: Option<RwWirePrecommitToken>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RwWireRollbackRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(bytes = "vec", tag = "2")]
    transaction_id: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
struct RwWireState {
    create_session_calls: usize,
    leader_headers: Vec<(String, String)>,
    begin_requests: Vec<RwWireBeginTransactionRequest>,
    sql_requests: Vec<RwWireExecuteSqlRequest>,
    batch_dml_requests: Vec<RwWireExecuteBatchDmlRequest>,
    commit_requests: Vec<RwWireCommitRequest>,
    rollback_requests: Vec<RwWireRollbackRequest>,
    retry_first_commit: bool,
    abort_commit_delay_ms: Option<i64>,
    batch_dml_abort_delay_ms: Option<i64>,
}

fn encode_base64_standard(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = if chunk.len() > 1 {
            u32::from(chunk[1])
        } else {
            0
        };
        let b2 = if chunk.len() > 2 {
            u32::from(chunk[2])
        } else {
            0
        };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((triple >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(triple & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

fn build_aborted_rpc_status(delay_ms: i64, msg: &str) -> RwWireRpcStatus {
    let retry_info = RwWireRetryInfo {
        retry_delay: Some(prost_types::Duration {
            seconds: delay_ms / 1_000,
            nanos: ((delay_ms % 1_000) * 1_000_000) as i32,
        }),
    };
    RwWireRpcStatus {
        code: 10,
        message: msg.to_string(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.RetryInfo".to_string(),
            value: retry_info.encode_to_vec(),
        }],
    }
}

fn prost_grpc_aborted_with_retry_info(delay_ms: i64, msg: &str) -> Response<Full<Bytes>> {
    let rpc_status = build_aborted_rpc_status(delay_ms, msg);
    let details_b64 = encode_base64_standard(&rpc_status.encode_to_vec());
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc")
        .header("grpc-status", "10")
        .header("grpc-message", msg)
        .header("grpc-status-details-bin", details_b64)
        .body(Full::new(Bytes::new()))
        .expect("grpc aborted")
}

async fn interactive_rw_client(state: Arc<Mutex<RwWireState>>) -> SpannerClient {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: Request<Incoming>| {
                    let state = Arc::clone(&state);
                    async move {
                        let path = req.uri().path().to_string();
                        let leader_val = req
                            .headers()
                            .get("x-goog-spanner-route-to-leader")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        let raw = req
                            .into_body()
                            .collect()
                            .await
                            .map(|c| c.to_bytes().to_vec())
                            .unwrap_or_default();
                        let payload = if raw.len() >= 5 { &raw[5..] } else { &raw[..] };
                        let mut guard = state.lock().unwrap();
                        if path != "/google.spanner.v1.Spanner/CreateSession" {
                            guard.leader_headers.push((path.clone(), leader_val));
                        }
                        let resp = match path.as_str() {
                            "/google.spanner.v1.Spanner/CreateSession" => {
                                guard.create_session_calls += 1;
                                prost_grpc_ok(vec![prost_lpm_frame(&ProstSession {
                                    name: "projects/p/instances/i/databases/d/sessions/mux-rw"
                                        .to_string(),
                                    multiplexed: true,
                                })])
                            }
                            "/google.spanner.v1.Spanner/BeginTransaction" => {
                                let decoded = RwWireBeginTransactionRequest::decode(payload)
                                    .expect("decode BeginTransactionRequest");
                                let idx = guard.begin_requests.len() + 1;
                                guard.begin_requests.push(decoded);
                                prost_grpc_ok(vec![prost_lpm_frame(&RwWireTransaction {
                                    id: format!("tx-{idx}").into_bytes(),
                                    precommit_token: Some(RwWirePrecommitToken {
                                        precommit_token: b"tok-1".to_vec(),
                                        seq_num: 1,
                                    }),
                                })])
                            }
                            "/google.spanner.v1.Spanner/ExecuteStreamingSql" => {
                                let decoded = RwWireExecuteSqlRequest::decode(payload)
                                    .expect("decode ExecuteStreamingSqlRequest");
                                guard.sql_requests.push(decoded);
                                let f1 = RwWirePartialResultSet {
                                    metadata: Some(RwWireResultSetMetadata {
                                        row_type: Some(ProstStructType {
                                            fields: vec![ProstStructField {
                                                name: "balance".to_string(),
                                                r#type: Some(ProstType { code: 2 }),
                                            }],
                                        }),
                                        transaction: None,
                                    }),
                                    values: vec![],
                                    stats: None,
                                    precommit_token: Some(RwWirePrecommitToken {
                                        precommit_token: b"tok-5".to_vec(),
                                        seq_num: 5,
                                    }),
                                    last: false,
                                };
                                let f2 = RwWirePartialResultSet {
                                    metadata: None,
                                    values: vec![prost_types::Value {
                                        kind: Some(prost_types::value::Kind::StringValue(
                                            "42".to_string(),
                                        )),
                                    }],
                                    stats: Some(ProstResultSetStats {
                                        row_count_exact: None,
                                    }),
                                    precommit_token: Some(RwWirePrecommitToken {
                                        precommit_token: b"tok-stale-3".to_vec(),
                                        seq_num: 3,
                                    }),
                                    last: true,
                                };
                                prost_grpc_ok(vec![prost_lpm_frame(&f1), prost_lpm_frame(&f2)])
                            }
                            "/google.spanner.v1.Spanner/ExecuteSql" => {
                                let decoded = RwWireExecuteSqlRequest::decode(payload)
                                    .expect("decode ExecuteSqlRequest");
                                guard.sql_requests.push(decoded);
                                prost_grpc_ok(vec![prost_lpm_frame(&RwWireResultSet {
                                    metadata: None,
                                    stats: Some(ProstResultSetStats {
                                        row_count_exact: Some(2),
                                    }),
                                    precommit_token: None,
                                })])
                            }
                            "/google.spanner.v1.Spanner/ExecuteBatchDml" => {
                                let decoded = RwWireExecuteBatchDmlRequest::decode(payload)
                                    .expect("decode ExecuteBatchDmlRequest");
                                let stmt_count = decoded.statements.len();
                                guard.batch_dml_requests.push(decoded);
                                if let Some(delay_ms) = guard.batch_dml_abort_delay_ms {
                                    prost_grpc_ok(vec![prost_lpm_frame(
                                        &RwWireExecuteBatchDmlResponse {
                                            result_sets: vec![RwWireResultSet {
                                                metadata: None,
                                                stats: Some(ProstResultSetStats {
                                                    row_count_exact: Some(1),
                                                }),
                                                precommit_token: None,
                                            }],
                                            status: Some(build_aborted_rpc_status(
                                                delay_ms,
                                                "batch dml aborted",
                                            )),
                                            precommit_token: None,
                                        },
                                    )])
                                } else {
                                    let result_sets = (0..stmt_count)
                                        .map(|i| RwWireResultSet {
                                            metadata: None,
                                            stats: Some(ProstResultSetStats {
                                                row_count_exact: Some(i as i64 + 1),
                                            }),
                                            precommit_token: None,
                                        })
                                        .collect();
                                    prost_grpc_ok(vec![prost_lpm_frame(
                                        &RwWireExecuteBatchDmlResponse {
                                            result_sets,
                                            status: Some(RwWireRpcStatus {
                                                code: 0,
                                                message: String::new(),
                                                details: vec![],
                                            }),
                                            precommit_token: None,
                                        },
                                    )])
                                }
                            }
                            "/google.spanner.v1.Spanner/Commit" => {
                                let decoded = RwWireCommitRequest::decode(payload)
                                    .expect("decode CommitRequest");
                                guard.commit_requests.push(decoded);
                                if let Some(delay_ms) = guard.abort_commit_delay_ms {
                                    prost_grpc_aborted_with_retry_info(
                                        delay_ms,
                                        "transaction aborted at commit",
                                    )
                                } else if guard.retry_first_commit
                                    && guard.commit_requests.len() == 1
                                {
                                    prost_grpc_ok(vec![prost_lpm_frame(&RwWireCommitResponse {
                                        commit_timestamp: None,
                                        commit_stats: None,
                                        precommit_token: Some(RwWirePrecommitToken {
                                            precommit_token: b"tok-retry-9".to_vec(),
                                            seq_num: 9,
                                        }),
                                    })])
                                } else {
                                    prost_grpc_ok(vec![prost_lpm_frame(&RwWireCommitResponse {
                                        commit_timestamp: Some(prost_types::Timestamp {
                                            seconds: 1_700_000_000,
                                            nanos: 123_000,
                                        }),
                                        commit_stats: Some(RwWireCommitStats { mutation_count: 1 }),
                                        precommit_token: None,
                                    })])
                                }
                            }
                            "/google.spanner.v1.Spanner/Rollback" => {
                                let decoded = RwWireRollbackRequest::decode(payload)
                                    .expect("decode RollbackRequest");
                                guard.rollback_requests.push(decoded);
                                prost_grpc_ok(vec![prost_lpm_frame(&())])
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
    let mut cfg = SpannerClientConfig::new("projects/p/instances/i/databases/d");
    cfg.endpoint = format!("http://{addr}");
    SpannerClient::new(cfg).expect("client")
}

#[tokio::test]
async fn interactive_rw_multi_statement_commit_tracks_seqno_tags_leader_and_mutations() {
    use _spanner::client::SpannerBeginTransactionOptions;

    let state = Arc::new(Mutex::new(RwWireState::default()));
    let client = interactive_rw_client(Arc::clone(&state)).await;

    let tx1 = client
        .begin_read_write_transaction(SpannerBeginTransactionOptions {
            transaction_tag: Some("tx-order".to_string()),
            previous_transaction_id: Some(b"prev-tx-0".to_vec()),
            exclude_txn_from_change_streams: true,
            max_commit_delay_ms: Some(15),
        })
        .await
        .expect("begin tx1");
    assert_eq!(tx1.transaction_id(), b"tx-1");

    let qres = tx1
        .query(
            "SELECT balance FROM accounts WHERE id = @id",
            &[SpannerQueryParameter::from_value(
                "id",
                &SpannerValue::Int64(1),
            )],
            Some("req-read"),
        )
        .await
        .expect("query");
    assert_eq!(qres.rows, vec![vec![SpannerValue::Int64(42)]]);

    let dml_count = tx1
        .execute_dml(
            "UPDATE accounts SET balance = 50 WHERE id = 1",
            &[],
            Some("req-dml"),
        )
        .await
        .expect("execute_dml");
    assert_eq!(dml_count, 2);

    let batch_counts = tx1
        .execute_batch_dml(
            &[
                SpannerBatchDmlStatement {
                    sql: "UPDATE accounts SET balance = 51 WHERE id = 2".into(),
                    params: vec![],
                },
                SpannerBatchDmlStatement {
                    sql: "UPDATE accounts SET balance = 52 WHERE id = 3".into(),
                    params: vec![],
                },
            ],
            Some("req-batch"),
        )
        .await
        .expect("execute_batch_dml");
    assert_eq!(batch_counts, vec![1, 2]);

    let mutation = SpannerMutation::insert(
        "audit",
        vec!["id".into()],
        vec![vec![ProtobufValue::String("1".into())]],
    );
    let commit_res = tx1.commit(&[mutation]).await.expect("commit");
    assert_eq!(
        commit_res.commit_timestamp_micros,
        Some(1_700_000_000_000_123)
    );
    assert_eq!(commit_res.mutation_count, Some(1));
    assert_eq!(commit_res.retry_count, 0);

    match tx1
        .execute_dml("UPDATE accounts SET balance = 99", &[], None)
        .await
    {
        Err(SpannerError::Gcp(info)) => assert_eq!(info.code, 9),
        other => panic!("expected post-commit FAILED_PRECONDITION, got {other:?}"),
    }

    let tx2 = client
        .begin_read_write_transaction(SpannerBeginTransactionOptions::default())
        .await
        .expect("begin tx2");
    assert_eq!(tx2.transaction_id(), b"tx-2");

    let guard = state.lock().unwrap();
    assert_eq!(guard.create_session_calls, 1);
    assert!(
        guard
            .leader_headers
            .iter()
            .all(|(path, val)| !path.is_empty() && val == "true")
    );

    assert_eq!(guard.begin_requests.len(), 2);
    let b0 = &guard.begin_requests[0];
    assert_eq!(
        b0.request_options.as_ref().unwrap().transaction_tag,
        "tx-order"
    );
    assert!(b0.options.as_ref().unwrap().exclude_txn_from_change_streams);
    assert_eq!(
        b0.options
            .as_ref()
            .unwrap()
            .read_write
            .as_ref()
            .unwrap()
            .multiplexed_session_previous_transaction_id,
        b"prev-tx-0"
    );

    assert_eq!(guard.sql_requests.len(), 2);
    assert_eq!(guard.sql_requests[0].seqno, 1);
    assert_eq!(guard.sql_requests[1].seqno, 2);
    assert_eq!(
        guard.sql_requests[0].transaction.as_ref().unwrap().id,
        b"tx-1"
    );
    assert_eq!(
        guard.sql_requests[1].transaction.as_ref().unwrap().id,
        b"tx-1"
    );
    assert_eq!(
        guard.sql_requests[0]
            .request_options
            .as_ref()
            .unwrap()
            .request_tag,
        "req-read"
    );
    assert_eq!(
        guard.sql_requests[0]
            .request_options
            .as_ref()
            .unwrap()
            .transaction_tag,
        "tx-order"
    );
    assert_eq!(guard.batch_dml_requests.len(), 1);
    assert_eq!(guard.batch_dml_requests[0].seqno, 3);
    assert_eq!(
        guard.batch_dml_requests[0].transaction.as_ref().unwrap().id,
        b"tx-1"
    );

    assert_eq!(guard.commit_requests.len(), 1);
    let creq = &guard.commit_requests[0];
    assert_eq!(creq.transaction_id, b"tx-1");
    assert_eq!(creq.mutations.len(), 1);
    assert_eq!(
        creq.precommit_token,
        Some(RwWirePrecommitToken {
            precommit_token: b"tok-5".to_vec(),
            seq_num: 5,
        })
    );
    assert_eq!(
        creq.max_commit_delay,
        Some(prost_types::Duration {
            seconds: 0,
            nanos: 15_000_000,
        })
    );
}

#[tokio::test]
async fn interactive_rw_commit_retries_multiplexed_session_retry_with_empty_mutations() {
    use _spanner::client::SpannerBeginTransactionOptions;

    let state = Arc::new(Mutex::new(RwWireState {
        retry_first_commit: true,
        ..Default::default()
    }));
    let client = interactive_rw_client(Arc::clone(&state)).await;
    let tx = client
        .begin_read_write_transaction(SpannerBeginTransactionOptions::default())
        .await
        .expect("begin tx");

    let mutation = SpannerMutation::insert(
        "audit",
        vec!["id".into()],
        vec![vec![ProtobufValue::String("1".into())]],
    );
    let commit_res = tx.commit(&[mutation]).await.expect("commit after retry");
    assert_eq!(commit_res.retry_count, 1);
    assert_eq!(commit_res.mutation_count, Some(1));

    let guard = state.lock().unwrap();
    assert_eq!(guard.commit_requests.len(), 2);
    assert_eq!(guard.commit_requests[0].mutations.len(), 1);
    assert_eq!(guard.commit_requests[1].mutations.len(), 0);
    assert_eq!(
        guard.commit_requests[1].precommit_token,
        Some(RwWirePrecommitToken {
            precommit_token: b"tok-retry-9".to_vec(),
            seq_num: 9,
        })
    );
}

#[tokio::test]
async fn interactive_rw_commit_abort_extracts_retry_delay_ms_and_prevents_double_rollback() {
    use _spanner::client::SpannerBeginTransactionOptions;

    let state = Arc::new(Mutex::new(RwWireState {
        abort_commit_delay_ms: Some(250),
        ..Default::default()
    }));
    let client = interactive_rw_client(Arc::clone(&state)).await;
    let tx = client
        .begin_read_write_transaction(SpannerBeginTransactionOptions::default())
        .await
        .expect("begin tx");

    match tx.commit(&[]).await {
        Err(SpannerError::Gcp(info)) => {
            assert_eq!(info.code, 10);
            assert_eq!(info.retry_delay_ms, Some(250));
        }
        other => panic!("expected SpannerError::Gcp Aborted, got {other:?}"),
    }

    tx.rollback().await.expect("post-abort rollback is no-op");
    let guard = state.lock().unwrap();
    assert!(guard.rollback_requests.is_empty());
}

#[tokio::test]
async fn interactive_rw_batch_dml_status_failure_surfaces_abort_retry_delay() {
    use _spanner::client::SpannerBeginTransactionOptions;

    let state = Arc::new(Mutex::new(RwWireState {
        batch_dml_abort_delay_ms: Some(125),
        ..Default::default()
    }));
    let client = interactive_rw_client(Arc::clone(&state)).await;
    let tx = client
        .begin_read_write_transaction(SpannerBeginTransactionOptions::default())
        .await
        .expect("begin tx");

    match tx
        .execute_batch_dml(
            &[SpannerBatchDmlStatement {
                sql: "UPDATE t SET v = 1".into(),
                params: vec![],
            }],
            None,
        )
        .await
    {
        Err(SpannerError::Gcp(info)) => {
            assert_eq!(info.code, 10);
            assert_eq!(info.retry_delay_ms, Some(125));
        }
        other => panic!("expected batch DML Aborted error, got {other:?}"),
    }
}

#[tokio::test]
async fn interactive_rw_rollback_sends_tx_id_and_blocks_subsequent_calls() {
    use _spanner::client::SpannerBeginTransactionOptions;

    let state = Arc::new(Mutex::new(RwWireState::default()));
    let client = interactive_rw_client(Arc::clone(&state)).await;
    let tx = client
        .begin_read_write_transaction(SpannerBeginTransactionOptions::default())
        .await
        .expect("begin tx");

    tx.execute_dml("UPDATE t SET v = 1", &[], None)
        .await
        .expect("dml");
    tx.rollback().await.expect("first rollback");
    tx.rollback().await.expect("idempotent rollback");

    match tx.query("SELECT 1", &[], None).await {
        Err(SpannerError::Gcp(info)) => assert_eq!(info.code, 9),
        other => panic!("expected FAILED_PRECONDITION after rollback, got {other:?}"),
    }

    let guard = state.lock().unwrap();
    assert_eq!(guard.rollback_requests.len(), 1);
    assert_eq!(guard.rollback_requests[0].transaction_id, b"tx-1");
}
