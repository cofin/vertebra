//! Interactive Cloud Spanner `ReadWrite` transactions over `google.spanner.v1` gRPC.
//!
//! Uses a multiplexed session (`CreateSession`), explicit `BeginTransaction` with
//! optional `multiplexed_session_previous_transaction_id` wound-wait priority,
//! monotonic 1-based `seqno` statement execution (`ExecuteStreamingSql`,
//! `ExecuteSql`, `ExecuteBatchDml`), highest-`seq_num`
//! `MultiplexedSessionPrecommitToken` tracking, `Commit` with
//! `MultiplexedSessionRetry` replay, and explicit `Rollback`.

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use http::HeaderValue;
use prost::Message;
use tokio::sync::Mutex;
use tonic::Status;
use tonic::client::Grpc;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::{MetadataKey, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use vertebra_db::gcp::GcpErrorInfo;

use crate::client::{SpannerClient, SpannerError};
use crate::codec::{
    ProtobufValue, SpannerBatchDmlStatement, SpannerCommitResult, SpannerDialect, SpannerMutation,
    SpannerQueryParameter, SpannerQueryResult, SpannerStructField, SpannerType,
    SpannerTypeAnnotation, SpannerTypeCode, SpannerValue, rewrite_spangres_placeholders,
};

const CREATE_SESSION_PATH: &str = "/google.spanner.v1.Spanner/CreateSession";
const BEGIN_TRANSACTION_PATH: &str = "/google.spanner.v1.Spanner/BeginTransaction";
const EXECUTE_SQL_PATH: &str = "/google.spanner.v1.Spanner/ExecuteSql";
const EXECUTE_STREAMING_SQL_PATH: &str = "/google.spanner.v1.Spanner/ExecuteStreamingSql";
const EXECUTE_BATCH_DML_PATH: &str = "/google.spanner.v1.Spanner/ExecuteBatchDml";
const COMMIT_PATH: &str = "/google.spanner.v1.Spanner/Commit";
const ROLLBACK_PATH: &str = "/google.spanner.v1.Spanner/Rollback";

const ROUTE_TO_LEADER_HEADER: &str = "x-goog-spanner-route-to-leader";
const REQUEST_PARAMS_HEADER: &str = "x-goog-request-params";
const USER_PROJECT_HEADER: &str = "x-goog-user-project";

const MAX_PRECOMMIT_RETRY_ATTEMPTS: usize = 3;

/// Options for opening an interactive [`SpannerReadWriteTx`].
#[derive(Clone, Debug, Default)]
pub struct SpannerBeginTransactionOptions {
    /// Optional Spanner transaction tag propagated on `BeginTransaction`, statements, and `Commit`.
    pub transaction_tag: Option<String>,
    /// Previous aborted transaction ID passed in `multiplexed_session_previous_transaction_id`.
    pub previous_transaction_id: Option<Vec<u8>>,
    /// Whether mutations and DML in this transaction are excluded from change streams.
    pub exclude_txn_from_change_streams: bool,
    /// Optional `CommitRequest.max_commit_delay` override in milliseconds.
    pub max_commit_delay_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxLifecycleState {
    Active,
    Committed,
    RolledBack,
    Aborted,
}

#[derive(Debug)]
struct TxMutableState {
    lifecycle: TxLifecycleState,
    precommit_token: Option<ProtoMultiplexedSessionPrecommitToken>,
    recorded_abort: Option<GcpErrorInfo>,
}

/// Stateful interactive Cloud Spanner `ReadWrite` transaction session handle.
#[derive(Clone, Debug)]
pub struct SpannerReadWriteTx {
    client: SpannerClient,
    session_name: String,
    transaction_id: Vec<u8>,
    transaction_tag: Option<String>,
    max_commit_delay_ms: Option<u64>,
    seqno: Arc<AtomicI64>,
    state: Arc<Mutex<TxMutableState>>,
}

impl SpannerReadWriteTx {
    /// Open a new interactive `ReadWrite` transaction via `BeginTransaction` on `client`.
    pub async fn begin(
        client: &SpannerClient,
        options: SpannerBeginTransactionOptions,
    ) -> Result<Self, SpannerError> {
        let channel = client.grpc_channel().await?;
        let session_name = client.ensure_mux_session(&channel).await?;
        let tx_tag = options.transaction_tag.filter(|tag| !tag.is_empty());
        let prev_id = options
            .previous_transaction_id
            .filter(|id| !id.is_empty())
            .map(Bytes::from)
            .unwrap_or_default();

        let request_options = tx_tag.as_ref().map(|tag| ProtoRequestOptions {
            priority: 0,
            request_tag: String::new(),
            transaction_tag: tag.clone(),
        });

        let req = ProtoBeginTransactionRequest {
            session: session_name.clone(),
            options: Some(ProtoTransactionOptions {
                exclude_txn_from_change_streams: options.exclude_txn_from_change_streams,
                isolation_level: 0,
                mode: Some(proto_transaction_options::Mode::ReadWrite(
                    proto_transaction_options::ReadWrite {
                        read_lock_mode: 0,
                        multiplexed_session_previous_transaction_id: prev_id,
                    },
                )),
            }),
            request_options,
        };

        let tx_resp: ProtoTransaction = client
            .unary_rpc(&channel, BEGIN_TRANSACTION_PATH, req, true)
            .await?;

        if tx_resp.id.is_empty() {
            return Err(SpannerError::Protocol(
                "BeginTransaction returned an empty transaction id".to_string(),
            ));
        }

        let effective_delay = options
            .max_commit_delay_ms
            .or(client.config().max_commit_delay_ms);

        Ok(Self {
            client: client.clone(),
            session_name,
            transaction_id: tx_resp.id.to_vec(),
            transaction_tag: tx_tag,
            max_commit_delay_ms: effective_delay,
            seqno: Arc::new(AtomicI64::new(1)),
            state: Arc::new(Mutex::new(TxMutableState {
                lifecycle: TxLifecycleState::Active,
                precommit_token: tx_resp.precommit_token,
                recorded_abort: None,
            })),
        })
    }

    /// Return the server-assigned Cloud Spanner transaction identifier bytes.
    pub fn transaction_id(&self) -> &[u8] {
        &self.transaction_id
    }

    /// Return the resolved multiplexed session resource name backing this transaction.
    pub fn session_name(&self) -> &str {
        &self.session_name
    }

    /// Execute a SQL query inside this interactive transaction via `ExecuteStreamingSql`.
    pub async fn query(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        request_tag: Option<&str>,
    ) -> Result<SpannerQueryResult, SpannerError> {
        self.ensure_active().await?;
        let seqno = self.next_seqno();
        let (encoded_sql, proto_params, proto_param_types) = self.encode_statement(sql, params);
        let request = ProtoExecuteSqlRequest {
            session: self.session_name.clone(),
            transaction: Some(self.tx_selector()),
            sql: encoded_sql,
            params: proto_params,
            param_types: proto_param_types,
            resume_token: Bytes::new(),
            query_mode: 0,
            partition_token: Bytes::new(),
            seqno,
            request_options: self.build_request_options(request_tag),
            data_boost_enabled: false,
            last_statement: false,
        };

        let channel = self.client.grpc_channel().await?;
        let mut grpc = Grpc::new(channel);
        grpc.ready().await.map_err(|err| {
            SpannerError::Gcp(GcpErrorInfo::new(
                14,
                format!("gRPC channel not ready: {err}"),
            ))
        })?;
        let tonic_req = self.client.build_grpc_request(request, true).await?;
        let mut stream = grpc
            .server_streaming(
                tonic_req,
                EXECUTE_STREAMING_SQL_PATH
                    .parse()
                    .map_err(|e| SpannerError::Protocol(format!("invalid gRPC path: {e}")))?,
                ProstCodec::<ProtoExecuteSqlRequest, ProtoPartialResultSet>::default(),
            )
            .await
            .map_err(|status| status_to_spanner_error(&status))?
            .into_inner();

        let mut decoder = PartialResultDecoder::default();
        while let Some(item) = stream.next().await {
            let chunk = match item {
                Ok(chunk) => chunk,
                Err(status) => {
                    let err = status_to_spanner_error(&status);
                    return Err(self.record_error_if_aborted(err).await);
                }
            };
            if let Some(token) = chunk.precommit_token.as_ref() {
                self.update_precommit_token(token).await;
            }
            decoder.push(chunk)?;
        }
        decoder.finish()
    }

    /// Execute a DML statement inside this interactive transaction via `ExecuteSql`.
    pub async fn execute_dml(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        request_tag: Option<&str>,
    ) -> Result<i64, SpannerError> {
        self.ensure_active().await?;
        let seqno = self.next_seqno();
        let (encoded_sql, proto_params, proto_param_types) = self.encode_statement(sql, params);
        let request = ProtoExecuteSqlRequest {
            session: self.session_name.clone(),
            transaction: Some(self.tx_selector()),
            sql: encoded_sql,
            params: proto_params,
            param_types: proto_param_types,
            resume_token: Bytes::new(),
            query_mode: 0,
            partition_token: Bytes::new(),
            seqno,
            request_options: self.build_request_options(request_tag),
            data_boost_enabled: false,
            last_statement: false,
        };

        let channel = self.client.grpc_channel().await?;
        let resp: ProtoResultSet = match self
            .client
            .unary_rpc(&channel, EXECUTE_SQL_PATH, request, true)
            .await
        {
            Ok(resp) => resp,
            Err(err) => return Err(self.record_error_if_aborted(err).await),
        };
        if let Some(token) = resp.precommit_token.as_ref() {
            self.update_precommit_token(token).await;
        }

        let Some(stats) = resp.stats else {
            return Err(SpannerError::Protocol(
                "ExecuteSql returned no ResultSetStats".to_string(),
            ));
        };
        match stats.row_count {
            Some(proto_result_set_stats::RowCount::RowCountExact(count)) => Ok(count),
            _ => Err(SpannerError::Protocol(
                "ExecuteSql returned missing or non-exact row count for ReadWrite transaction"
                    .to_string(),
            )),
        }
    }

    /// Execute a batch of DML statements inside this interactive transaction via `ExecuteBatchDml`.
    pub async fn execute_batch_dml(
        &self,
        statements: &[SpannerBatchDmlStatement],
        request_tag: Option<&str>,
    ) -> Result<Vec<i64>, SpannerError> {
        self.ensure_active().await?;
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        let seqno = self.next_seqno();
        let encoded_statements = statements
            .iter()
            .map(|stmt| {
                let (encoded_sql, proto_params, proto_param_types) =
                    self.encode_statement(&stmt.sql, &stmt.params);
                proto_execute_batch_dml_request::Statement {
                    sql: encoded_sql,
                    params: proto_params,
                    param_types: proto_param_types,
                }
            })
            .collect();

        let request = ProtoExecuteBatchDmlRequest {
            session: self.session_name.clone(),
            transaction: Some(self.tx_selector()),
            statements: encoded_statements,
            seqno,
            request_options: self.build_request_options(request_tag),
            last_statements: false,
        };

        let channel = self.client.grpc_channel().await?;
        let resp: ProtoExecuteBatchDmlResponse = match self
            .client
            .unary_rpc(&channel, EXECUTE_BATCH_DML_PATH, request, true)
            .await
        {
            Ok(resp) => resp,
            Err(err) => return Err(self.record_error_if_aborted(err).await),
        };

        if let Some(token) = resp.precommit_token.as_ref() {
            self.update_precommit_token(token).await;
        }
        for rs in &resp.result_sets {
            if let Some(token) = rs.precommit_token.as_ref() {
                self.update_precommit_token(token).await;
            }
        }

        if let Some(status) = resp.status
            && status.code != 0
        {
            let err = rpc_status_to_spanner_error(&status);
            return Err(self.record_error_if_aborted(err).await);
        }

        let mut counts = Vec::with_capacity(resp.result_sets.len());
        for rs in resp.result_sets {
            match rs.stats.and_then(|s| s.row_count) {
                Some(proto_result_set_stats::RowCount::RowCountExact(count)) => counts.push(count),
                _ => {
                    return Err(SpannerError::Protocol(
                        "ExecuteBatchDml result set missing row_count_exact".to_string(),
                    ));
                }
            }
        }
        Ok(counts)
    }

    /// Commit the transaction and any buffered mutations, handling `MultiplexedSessionRetry`.
    pub async fn commit(
        &self,
        mutations: &[SpannerMutation],
    ) -> Result<SpannerCommitResult, SpannerError> {
        let initial_token = {
            let guard = self.state.lock().await;
            match guard.lifecycle {
                TxLifecycleState::Active => {}
                TxLifecycleState::Committed => {
                    return Err(SpannerError::Gcp(GcpErrorInfo::new(
                        9,
                        "Transaction has already been committed",
                    )));
                }
                TxLifecycleState::RolledBack => {
                    return Err(SpannerError::Gcp(GcpErrorInfo::new(
                        9,
                        "Transaction has already been rolled back",
                    )));
                }
                TxLifecycleState::Aborted => {
                    return Err(abort_error_from_state(guard.recorded_abort.as_ref()));
                }
            }
            guard.precommit_token.clone()
        };

        let proto_mutations: Vec<ProtoMutation> =
            mutations.iter().map(encode_proto_mutation).collect();
        let channel = self.client.grpc_channel().await?;
        let mut current_token = initial_token;
        let mut retry_count: u32 = 0;
        let mut pending_mutations = proto_mutations;

        loop {
            let req = ProtoCommitRequest {
                session: self.session_name.clone(),
                mutations: std::mem::take(&mut pending_mutations),
                return_commit_stats: true,
                max_commit_delay: self.max_commit_delay_ms.map(ms_to_proto_duration),
                request_options: self.build_request_options(None),
                precommit_token: current_token.clone(),
                transaction: Some(proto_commit_request::Transaction::TransactionId(
                    Bytes::from(self.transaction_id.clone()),
                )),
            };

            let resp: ProtoCommitResponse = match self
                .client
                .unary_rpc(&channel, COMMIT_PATH, req, true)
                .await
            {
                Ok(resp) => resp,
                Err(err) => {
                    if let SpannerError::Gcp(ref info) = err
                        && info.code == 10
                    {
                        let mut guard = self.state.lock().await;
                        guard.lifecycle = TxLifecycleState::Aborted;
                        guard.recorded_abort = Some(info.clone());
                    }
                    return Err(err);
                }
            };

            if let Some(proto_commit_response::MultiplexedSessionRetry::PrecommitToken(
                retry_token,
            )) = resp.multiplexed_session_retry
            {
                if retry_count as usize >= MAX_PRECOMMIT_RETRY_ATTEMPTS {
                    return Err(SpannerError::Protocol(
                        "Commit exceeded maximum MultiplexedSessionRetry replays".to_string(),
                    ));
                }
                {
                    let mut guard = self.state.lock().await;
                    let should_replace = guard
                        .precommit_token
                        .as_ref()
                        .is_none_or(|existing| retry_token.seq_num >= existing.seq_num);
                    if should_replace {
                        guard.precommit_token = Some(retry_token.clone());
                    }
                    current_token = guard.precommit_token.clone().or(Some(retry_token));
                }
                retry_count += 1;
                pending_mutations.clear();
                continue;
            }

            let Some(ts) = resp.commit_timestamp else {
                return Err(SpannerError::Protocol(
                    "Commit returned no commit_timestamp; the transaction outcome is unknown"
                        .to_string(),
                ));
            };

            {
                let mut guard = self.state.lock().await;
                guard.lifecycle = TxLifecycleState::Committed;
            }

            let micros = ts
                .seconds
                .saturating_mul(1_000_000)
                .saturating_add(i64::from(ts.nanos) / 1_000);
            let mutation_count = resp.commit_stats.as_ref().map(|s| s.mutation_count);
            return Ok(SpannerCommitResult {
                commit_timestamp_micros: Some(micros),
                mutation_count,
                retry_count,
            });
        }
    }

    /// Roll back the transaction on Spanner if it is still active.
    pub async fn rollback(&self) -> Result<(), SpannerError> {
        {
            let mut guard = self.state.lock().await;
            match guard.lifecycle {
                TxLifecycleState::Active => {
                    guard.lifecycle = TxLifecycleState::RolledBack;
                }
                TxLifecycleState::Committed
                | TxLifecycleState::RolledBack
                | TxLifecycleState::Aborted => {
                    return Ok(());
                }
            }
        }

        let channel = self.client.grpc_channel().await?;
        let req = ProtoRollbackRequest {
            session: self.session_name.clone(),
            transaction_id: Bytes::from(self.transaction_id.clone()),
        };
        let _: () = self
            .client
            .unary_rpc(&channel, ROLLBACK_PATH, req, true)
            .await?;
        Ok(())
    }

    fn next_seqno(&self) -> i64 {
        self.seqno.fetch_add(1, Ordering::SeqCst)
    }

    fn tx_selector(&self) -> ProtoTransactionSelector {
        ProtoTransactionSelector {
            selector: Some(proto_transaction_selector::Selector::Id(Bytes::from(
                self.transaction_id.clone(),
            ))),
        }
    }

    fn build_request_options(&self, request_tag: Option<&str>) -> Option<ProtoRequestOptions> {
        let req_tag = request_tag.unwrap_or("");
        let tx_tag = self.transaction_tag.as_deref().unwrap_or("");
        if req_tag.is_empty() && tx_tag.is_empty() {
            return None;
        }
        Some(ProtoRequestOptions {
            priority: 0,
            request_tag: req_tag.to_string(),
            transaction_tag: tx_tag.to_string(),
        })
    }

    fn encode_statement(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
    ) -> (
        String,
        Option<prost_types::Struct>,
        HashMap<String, ProtoType>,
    ) {
        let encoded_sql = match self.client.config().dialect {
            SpannerDialect::GoogleSql => rewrite_spangres_placeholders(sql).0,
            SpannerDialect::Spangres => sql.to_string(),
        };
        if params.is_empty() {
            return (encoded_sql, None, HashMap::new());
        }
        let mut fields = std::collections::BTreeMap::new();
        let mut param_types = HashMap::with_capacity(params.len());
        for param in params {
            fields.insert(param.name.clone(), encode_prost_value(&param.value));
            param_types.insert(param.name.clone(), encode_proto_type(&param.param_type));
        }
        (
            encoded_sql,
            Some(prost_types::Struct { fields }),
            param_types,
        )
    }

    async fn ensure_active(&self) -> Result<(), SpannerError> {
        let guard = self.state.lock().await;
        match guard.lifecycle {
            TxLifecycleState::Active => Ok(()),
            TxLifecycleState::Committed => Err(SpannerError::Gcp(GcpErrorInfo::new(
                9,
                "Transaction has already been committed",
            ))),
            TxLifecycleState::RolledBack => Err(SpannerError::Gcp(GcpErrorInfo::new(
                9,
                "Transaction has already been rolled back",
            ))),
            TxLifecycleState::Aborted => Err(abort_error_from_state(guard.recorded_abort.as_ref())),
        }
    }

    async fn update_precommit_token(&self, candidate: &ProtoMultiplexedSessionPrecommitToken) {
        let mut guard = self.state.lock().await;
        let should_update = guard
            .precommit_token
            .as_ref()
            .is_none_or(|existing| candidate.seq_num > existing.seq_num);
        if should_update {
            guard.precommit_token = Some(candidate.clone());
        }
    }

    async fn record_error_if_aborted(&self, err: SpannerError) -> SpannerError {
        if let SpannerError::Gcp(ref info) = err
            && info.code == 10
        {
            let mut guard = self.state.lock().await;
            guard.lifecycle = TxLifecycleState::Aborted;
            guard.recorded_abort = Some(info.clone());
        }
        err
    }
}

impl SpannerClient {
    /// Lazily connect and cache the `tonic::transport::Channel` targeting `effective_endpoint`.
    pub(crate) async fn grpc_channel(&self) -> Result<Channel, SpannerError> {
        self.grpc_channel
            .get_or_try_init(|| async {
                let endpoint_str = self.effective_endpoint();
                let mut endpoint =
                    Endpoint::from_shared(endpoint_str.to_string()).map_err(|err| {
                        SpannerError::InvalidArgument(format!(
                            "invalid Spanner endpoint URI: {err}"
                        ))
                    })?;
                if endpoint_str.starts_with("https://") {
                    let tls = match self.config().ca_pem.as_deref() {
                        Some(ca_pem) => {
                            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_pem))
                        }
                        None => ClientTlsConfig::new().with_webpki_roots(),
                    };
                    endpoint = endpoint.tls_config(tls).map_err(|err| {
                        SpannerError::InvalidArgument(format!("invalid Spanner TLS config: {err}"))
                    })?;
                }
                endpoint.connect().await.map_err(|err| {
                    SpannerError::Gcp(GcpErrorInfo::new(
                        14,
                        format!("failed to connect Spanner gRPC endpoint {endpoint_str}: {err}"),
                    ))
                })
            })
            .await
            .cloned()
    }

    /// Lazily create and cache one multiplexed `google.spanner.v1.Session`.
    pub(crate) async fn ensure_mux_session(
        &self,
        channel: &Channel,
    ) -> Result<String, SpannerError> {
        self.mux_session
            .get_or_try_init(|| async {
                let req = ProtoCreateSessionRequest {
                    database: self.config().database.clone(),
                    session: Some(ProtoSession {
                        name: String::new(),
                        creator_role: self.config().database_role.clone().unwrap_or_default(),
                        multiplexed: true,
                    }),
                };
                let session: ProtoSession = self
                    .unary_rpc(channel, CREATE_SESSION_PATH, req, false)
                    .await?;
                if session.name.is_empty() {
                    return Err(SpannerError::Protocol(
                        "CreateSession returned empty session name".to_string(),
                    ));
                }
                Ok(session.name)
            })
            .await
            .cloned()
    }

    async fn build_grpc_request<T>(
        &self,
        message: T,
        route_to_leader: bool,
    ) -> Result<tonic::Request<T>, SpannerError> {
        let mut req = tonic::Request::new(message);
        let md = req.metadata_mut();

        if let Ok(key) = MetadataKey::from_bytes(REQUEST_PARAMS_HEADER.as_bytes())
            && let Ok(val) = MetadataValue::try_from(self.routing_header_value())
        {
            md.insert(key, val);
        }

        if route_to_leader
            && self.config().route_to_leader_enabled
            && let Ok(key) = MetadataKey::from_bytes(ROUTE_TO_LEADER_HEADER.as_bytes())
        {
            md.insert(key, MetadataValue::from_static("true"));
        }

        if let Some(cache) = self.token_cache() {
            let snapshot = cache.get_or_refresh().await.map_err(SpannerError::Auth)?;
            let bearer_raw = format!("Bearer {}", snapshot.access_token);
            let bearer = MetadataValue::try_from(bearer_raw.as_str()).map_err(|err| {
                SpannerError::Auth(format!("invalid bearer token metadata: {err}"))
            })?;
            md.insert("authorization", bearer);
            if let Some(project) = snapshot
                .quota_project_id
                .as_deref()
                .filter(|p| !p.is_empty())
                && let Ok(key) = MetadataKey::from_bytes(USER_PROJECT_HEADER.as_bytes())
                && let Ok(val) = HeaderValue::from_str(project)
                && let Ok(meta_val) = MetadataValue::try_from(val.to_str().unwrap_or_default())
            {
                md.insert(key, meta_val);
            }
        }

        Ok(req)
    }

    async fn unary_rpc<Req, Resp>(
        &self,
        channel: &Channel,
        path: &'static str,
        request: Req,
        route_to_leader: bool,
    ) -> Result<Resp, SpannerError>
    where
        Req: Message + Send + 'static,
        Resp: Message + Default + Send + 'static,
    {
        let mut grpc = Grpc::new(channel.clone());
        grpc.ready().await.map_err(|err| {
            SpannerError::Gcp(GcpErrorInfo::new(
                14,
                format!("gRPC channel not ready: {err}"),
            ))
        })?;
        let tonic_req = self.build_grpc_request(request, route_to_leader).await?;
        let path_and_query = path
            .parse()
            .map_err(|err| SpannerError::Protocol(format!("invalid gRPC path {path}: {err}")))?;
        let resp = grpc
            .unary(
                tonic_req,
                path_and_query,
                ProstCodec::<Req, Resp>::default(),
            )
            .await
            .map_err(|status| status_to_spanner_error(&status))?;
        Ok(resp.into_inner())
    }

    /// Open a stateful interactive `ReadWrite` transaction session on this client.
    pub async fn begin_read_write_transaction(
        &self,
        options: SpannerBeginTransactionOptions,
    ) -> Result<SpannerReadWriteTx, SpannerError> {
        SpannerReadWriteTx::begin(self, options).await
    }
}

fn abort_error_from_state(recorded: Option<&GcpErrorInfo>) -> SpannerError {
    if let Some(info) = recorded {
        SpannerError::Gcp(info.clone())
    } else {
        SpannerError::Gcp(GcpErrorInfo::new(10, "Transaction has already aborted"))
    }
}

fn ms_to_proto_duration(ms: u64) -> prost_types::Duration {
    let clamped = ms.min(i64::MAX as u64);
    prost_types::Duration {
        seconds: (clamped / 1_000) as i64,
        nanos: ((clamped % 1_000) * 1_000_000) as i32,
    }
}

fn status_to_spanner_error(status: &Status) -> SpannerError {
    let mut info = GcpErrorInfo::new(
        status.code() as i32,
        if status.message().is_empty() {
            status.to_string()
        } else {
            status.message().to_string()
        },
    );
    if !status.details().is_empty()
        && let Ok(rpc_status) = ProtoRpcStatus::decode(status.details())
    {
        populate_error_details(&mut info, &rpc_status);
    }
    SpannerError::Gcp(info)
}

fn rpc_status_to_spanner_error(status: &ProtoRpcStatus) -> SpannerError {
    let mut info = GcpErrorInfo::new(status.code, status.message.clone());
    populate_error_details(&mut info, status);
    SpannerError::Gcp(info)
}

fn populate_error_details(info: &mut GcpErrorInfo, status: &ProtoRpcStatus) {
    for detail in &status.details {
        match detail.type_url.as_str() {
            "type.googleapis.com/google.rpc.RetryInfo" if info.retry_delay_ms.is_none() => {
                if let Ok(retry) = ProtoRetryInfo::decode(detail.value.as_slice())
                    && let Some(delay) = retry.retry_delay
                {
                    let secs = delay.seconds.max(0) as u64;
                    let nanos = delay.nanos.max(0) as u64;
                    info.retry_delay_ms = Some(secs * 1_000 + nanos / 1_000_000);
                }
            }
            "type.googleapis.com/google.rpc.ErrorInfo" if info.reason.is_none() => {
                if let Ok(err_info) = ProtoErrorInfo::decode(detail.value.as_slice())
                    && !err_info.reason.is_empty()
                {
                    info.reason = Some(err_info.reason);
                }
            }
            _ => {}
        }
    }
}

#[derive(Default)]
struct PartialResultDecoder {
    columns: Vec<String>,
    schema: Vec<SpannerStructField>,
    metadata_seen: bool,
    buffered_values: VecDeque<prost_types::Value>,
    chunked: bool,
    rows: Vec<Vec<SpannerValue>>,
    row_count_exact: Option<i64>,
    row_count_lower_bound: Option<i64>,
}

impl PartialResultDecoder {
    fn push(&mut self, prs: ProtoPartialResultSet) -> Result<(), SpannerError> {
        if let Some(meta) = prs.metadata
            && !self.metadata_seen
        {
            self.metadata_seen = true;
            if let Some(row_type) = meta.row_type {
                self.columns = row_type.fields.iter().map(|f| f.name.clone()).collect();
                self.schema =
                    row_type
                        .fields
                        .iter()
                        .map(|f| SpannerStructField {
                            name: f.name.clone(),
                            field_type: f.r#type.as_ref().map(decode_proto_type).unwrap_or_else(
                                || SpannerType::scalar(SpannerTypeCode::Unspecified),
                            ),
                        })
                        .collect();
            }
        }

        if let Some(stats) = prs.stats {
            match stats.row_count {
                Some(proto_result_set_stats::RowCount::RowCountExact(count)) => {
                    self.row_count_exact = Some(count);
                }
                Some(proto_result_set_stats::RowCount::RowCountLowerBound(count)) => {
                    self.row_count_lower_bound = Some(count);
                }
                None => {}
            }
        }

        if !prs.values.is_empty() {
            if self.schema.is_empty() {
                return Err(SpannerError::Protocol(
                    "PartialResultSet contained values before row_type metadata".to_string(),
                ));
            }
            let mut iter = prs.values.into_iter();
            if self.chunked
                && let Some(prev) = self.buffered_values.back_mut()
                && let Some(next_val) = iter.next()
            {
                merge_prost_values(prev, next_val)?;
            }
            self.buffered_values.extend(iter);
            self.chunked = prs.chunked_value;
            self.drain_ready_rows()?;
        } else {
            self.chunked = prs.chunked_value;
        }
        Ok(())
    }

    fn drain_ready_rows(&mut self) -> Result<(), SpannerError> {
        let width = self.schema.len();
        if width == 0 {
            return Ok(());
        }
        while self.buffered_values.len() >= width {
            if self.buffered_values.len() == width && self.chunked {
                break;
            }
            let mut decoded_row = Vec::with_capacity(width);
            for field in &self.schema {
                let raw = self
                    .buffered_values
                    .pop_front()
                    .expect("width checked above");
                let pb = decode_prost_value(raw)?;
                decoded_row.push(SpannerValue::from_protobuf_value_owned(
                    pb,
                    &field.field_type,
                )?);
            }
            self.rows.push(decoded_row);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<SpannerQueryResult, SpannerError> {
        if self.chunked {
            return Err(SpannerError::Protocol(
                "ExecuteStreamingSql stream ended with chunked_value=true".to_string(),
            ));
        }
        self.drain_ready_rows()?;
        if !self.buffered_values.is_empty() {
            return Err(SpannerError::Protocol(
                "ExecuteStreamingSql stream ended with partial row values".to_string(),
            ));
        }
        Ok(SpannerQueryResult {
            columns: self.columns,
            schema: self.schema,
            rows: self.rows,
            row_count_exact: self.row_count_exact,
            row_count_lower_bound: self.row_count_lower_bound,
        })
    }
}

fn merge_prost_values(
    target: &mut prost_types::Value,
    source: prost_types::Value,
) -> Result<(), SpannerError> {
    use prost_types::value::Kind;
    match (&mut target.kind, source.kind) {
        (Some(Kind::StringValue(dst)), Some(Kind::StringValue(src))) => {
            dst.push_str(&src);
            Ok(())
        }
        (Some(Kind::ListValue(dst)), Some(Kind::ListValue(src))) => {
            if src.values.is_empty() {
                return Ok(());
            }
            if dst.values.is_empty() {
                dst.values = src.values;
                return Ok(());
            }
            let mut iter = src.values.into_iter();
            if let Some(first) = iter.next() {
                let last = dst.values.last_mut().expect("non-empty list");
                match (&last.kind, &first.kind) {
                    (Some(Kind::StringValue(_)), Some(Kind::StringValue(_)))
                    | (Some(Kind::ListValue(_)), Some(Kind::ListValue(_))) => {
                        merge_prost_values(last, first)?;
                    }
                    _ => dst.values.push(first),
                }
            }
            dst.values.extend(iter);
            Ok(())
        }
        _ => Err(SpannerError::Protocol(
            "Incompatible types while merging chunked PartialResultSet values".to_string(),
        )),
    }
}

fn decode_proto_type(ty: &ProtoType) -> SpannerType {
    SpannerType {
        code: SpannerTypeCode::from_i32(ty.code),
        array_element_type: ty
            .array_element_type
            .as_deref()
            .map(decode_proto_type)
            .map(Box::new),
        struct_fields: ty
            .struct_type
            .as_ref()
            .map(|st| {
                st.fields
                    .iter()
                    .map(|f| SpannerStructField {
                        name: f.name.clone(),
                        field_type: f
                            .r#type
                            .as_ref()
                            .map(decode_proto_type)
                            .unwrap_or_else(|| SpannerType::scalar(SpannerTypeCode::Unspecified)),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        type_annotation: SpannerTypeAnnotation::from_i32(ty.type_annotation),
        proto_type_fqn: (!ty.proto_type_fqn.is_empty()).then(|| ty.proto_type_fqn.clone()),
    }
}

fn encode_proto_type(ty: &SpannerType) -> ProtoType {
    let struct_type = if ty.struct_fields.is_empty() {
        None
    } else {
        Some(ProtoStructType {
            fields: ty
                .struct_fields
                .iter()
                .map(|f| proto_struct_type::Field {
                    name: f.name.clone(),
                    r#type: Some(encode_proto_type(&f.field_type)),
                })
                .collect(),
        })
    };
    ProtoType {
        code: ty.code as i32,
        array_element_type: ty
            .array_element_type
            .as_deref()
            .map(encode_proto_type)
            .map(Box::new),
        struct_type,
        type_annotation: ty.type_annotation as i32,
        proto_type_fqn: ty.proto_type_fqn.clone().unwrap_or_default(),
    }
}

fn encode_prost_value(value: &ProtobufValue) -> prost_types::Value {
    use prost_types::value::Kind;
    let kind = match value {
        ProtobufValue::Null => Kind::NullValue(0),
        ProtobufValue::Bool(b) => Kind::BoolValue(*b),
        ProtobufValue::Number(n) => Kind::NumberValue(*n),
        ProtobufValue::String(s) => Kind::StringValue(s.clone()),
        ProtobufValue::Struct(entries) => Kind::StructValue(prost_types::Struct {
            fields: entries
                .iter()
                .map(|(k, v)| (k.clone(), encode_prost_value(v)))
                .collect(),
        }),
        ProtobufValue::List(items) => Kind::ListValue(prost_types::ListValue {
            values: items.iter().map(encode_prost_value).collect(),
        }),
    };
    prost_types::Value { kind: Some(kind) }
}

fn decode_prost_value(value: prost_types::Value) -> Result<ProtobufValue, SpannerError> {
    use prost_types::value::Kind;
    match value.kind {
        None | Some(Kind::NullValue(_)) => Ok(ProtobufValue::Null),
        Some(Kind::BoolValue(b)) => Ok(ProtobufValue::Bool(b)),
        Some(Kind::NumberValue(n)) => Ok(ProtobufValue::Number(n)),
        Some(Kind::StringValue(s)) => Ok(ProtobufValue::String(s)),
        Some(Kind::StructValue(st)) => {
            let mut out = Vec::with_capacity(st.fields.len());
            for (k, v) in st.fields {
                out.push((k, decode_prost_value(v)?));
            }
            Ok(ProtobufValue::Struct(out))
        }
        Some(Kind::ListValue(list)) => {
            let mut out = Vec::with_capacity(list.values.len());
            for item in list.values {
                out.push(decode_prost_value(item)?);
            }
            Ok(ProtobufValue::List(out))
        }
    }
}

fn encode_proto_mutation(mutation: &SpannerMutation) -> ProtoMutation {
    let encode_write =
        |table: &str, columns: &[String], rows: &[Vec<ProtobufValue>]| -> proto_mutation::Write {
            proto_mutation::Write {
                table: table.to_string(),
                columns: columns.to_vec(),
                values: rows
                    .iter()
                    .map(|row| prost_types::ListValue {
                        values: row.iter().map(encode_prost_value).collect(),
                    })
                    .collect(),
            }
        };
    let op = match mutation {
        SpannerMutation::Insert {
            table,
            columns,
            values,
        } => proto_mutation::Operation::Insert(encode_write(table, columns, values)),
        SpannerMutation::Update {
            table,
            columns,
            values,
        } => proto_mutation::Operation::Update(encode_write(table, columns, values)),
        SpannerMutation::InsertOrUpdate {
            table,
            columns,
            values,
        } => proto_mutation::Operation::InsertOrUpdate(encode_write(table, columns, values)),
        SpannerMutation::Replace {
            table,
            columns,
            values,
        } => proto_mutation::Operation::Replace(encode_write(table, columns, values)),
        SpannerMutation::Delete { table, keys, all } => {
            let encoded_keys = keys
                .iter()
                .map(|row| prost_types::ListValue {
                    values: row.iter().map(encode_prost_value).collect(),
                })
                .collect();
            proto_mutation::Operation::Delete(proto_mutation::Delete {
                table: table.clone(),
                key_set: Some(ProtoKeySet {
                    keys: encoded_keys,
                    all: *all,
                }),
            })
        }
    };
    ProtoMutation {
        operation: Some(op),
    }
}

#[derive(Debug, Clone)]
struct ProstCodec<T, U> {
    _pd: PhantomData<(T, U)>,
}

impl<T, U> Default for ProstCodec<T, U> {
    fn default() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<T, U> Codec for ProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = ProstEncoder<T>;
    type Decoder = ProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        ProstEncoder {
            _pd: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstDecoder {
            _pd: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct ProstEncoder<T> {
    _pd: PhantomData<T>,
    buffer_settings: BufferSettings,
}

impl<T: Message> Encoder for ProstEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        item.encode(buf)
            .map_err(|err| Status::internal(format!("protobuf encode failure: {err}")))
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

#[derive(Debug, Clone)]
struct ProstDecoder<U> {
    _pd: PhantomData<U>,
    buffer_settings: BufferSettings,
}

impl<U: Message + Default> Decoder for ProstDecoder<U> {
    type Item = U;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Message::decode(buf)
            .map(Some)
            .map_err(|err| Status::internal(format!("protobuf decode failure: {err}")))
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoRequestOptions {
    #[prost(int32, tag = "1")]
    priority: i32,
    #[prost(string, tag = "2")]
    request_tag: String,
    #[prost(string, tag = "3")]
    transaction_tag: String,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoCreateSessionRequest {
    #[prost(string, tag = "1")]
    database: String,
    #[prost(message, optional, tag = "2")]
    session: Option<ProtoSession>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoSession {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "5")]
    creator_role: String,
    #[prost(bool, tag = "6")]
    multiplexed: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Message)]
struct ProtoMultiplexedSessionPrecommitToken {
    #[prost(bytes = "bytes", tag = "1")]
    precommit_token: Bytes,
    #[prost(int32, tag = "2")]
    seq_num: i32,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoTransaction {
    #[prost(bytes = "bytes", tag = "1")]
    id: Bytes,
    #[prost(message, optional, tag = "2")]
    read_timestamp: Option<prost_types::Timestamp>,
    #[prost(message, optional, tag = "3")]
    precommit_token: Option<ProtoMultiplexedSessionPrecommitToken>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoTransactionOptions {
    #[prost(bool, tag = "5")]
    exclude_txn_from_change_streams: bool,
    #[prost(int32, tag = "6")]
    isolation_level: i32,
    #[prost(oneof = "proto_transaction_options::Mode", tags = "1")]
    mode: Option<proto_transaction_options::Mode>,
}

mod proto_transaction_options {
    use bytes::Bytes;
    use prost::Message;

    #[derive(Clone, PartialEq, Eq, Hash, Message)]
    pub struct ReadWrite {
        #[prost(int32, tag = "1")]
        pub read_lock_mode: i32,
        #[prost(bytes = "bytes", tag = "2")]
        pub multiplexed_session_previous_transaction_id: Bytes,
    }

    #[derive(Clone, PartialEq, Eq, Hash, prost::Oneof)]
    pub enum Mode {
        #[prost(message, tag = "1")]
        ReadWrite(ReadWrite),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoBeginTransactionRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(message, optional, tag = "2")]
    options: Option<ProtoTransactionOptions>,
    #[prost(message, optional, tag = "3")]
    request_options: Option<ProtoRequestOptions>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoTransactionSelector {
    #[prost(oneof = "proto_transaction_selector::Selector", tags = "2")]
    selector: Option<proto_transaction_selector::Selector>,
}

mod proto_transaction_selector {
    use bytes::Bytes;

    #[derive(Clone, PartialEq, Eq, Hash, prost::Oneof)]
    pub enum Selector {
        #[prost(bytes, tag = "2")]
        Id(Bytes),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoType {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(message, optional, boxed, tag = "2")]
    array_element_type: Option<Box<ProtoType>>,
    #[prost(message, optional, tag = "3")]
    struct_type: Option<ProtoStructType>,
    #[prost(int32, tag = "4")]
    type_annotation: i32,
    #[prost(string, tag = "5")]
    proto_type_fqn: String,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoStructType {
    #[prost(message, repeated, tag = "1")]
    fields: Vec<proto_struct_type::Field>,
}

mod proto_struct_type {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct Field {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(message, optional, tag = "2")]
        pub r#type: Option<super::ProtoType>,
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoExecuteSqlRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(message, optional, tag = "2")]
    transaction: Option<ProtoTransactionSelector>,
    #[prost(string, tag = "3")]
    sql: String,
    #[prost(message, optional, tag = "4")]
    params: Option<prost_types::Struct>,
    #[prost(map = "string, message", tag = "5")]
    param_types: HashMap<String, ProtoType>,
    #[prost(bytes = "bytes", tag = "6")]
    resume_token: Bytes,
    #[prost(int32, tag = "7")]
    query_mode: i32,
    #[prost(bytes = "bytes", tag = "8")]
    partition_token: Bytes,
    #[prost(int64, tag = "9")]
    seqno: i64,
    #[prost(message, optional, tag = "11")]
    request_options: Option<ProtoRequestOptions>,
    #[prost(bool, tag = "16")]
    data_boost_enabled: bool,
    #[prost(bool, tag = "17")]
    last_statement: bool,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoExecuteBatchDmlRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(message, optional, tag = "2")]
    transaction: Option<ProtoTransactionSelector>,
    #[prost(message, repeated, tag = "3")]
    statements: Vec<proto_execute_batch_dml_request::Statement>,
    #[prost(int64, tag = "4")]
    seqno: i64,
    #[prost(message, optional, tag = "5")]
    request_options: Option<ProtoRequestOptions>,
    #[prost(bool, tag = "6")]
    last_statements: bool,
}

mod proto_execute_batch_dml_request {
    use prost::Message;
    use std::collections::HashMap;

    #[derive(Clone, PartialEq, Message)]
    pub struct Statement {
        #[prost(string, tag = "1")]
        pub sql: String,
        #[prost(message, optional, tag = "2")]
        pub params: Option<prost_types::Struct>,
        #[prost(map = "string, message", tag = "3")]
        pub param_types: HashMap<String, super::ProtoType>,
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoResultSetMetadata {
    #[prost(message, optional, tag = "1")]
    row_type: Option<ProtoStructType>,
    #[prost(message, optional, tag = "2")]
    transaction: Option<ProtoTransaction>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoResultSetStats {
    #[prost(oneof = "proto_result_set_stats::RowCount", tags = "3, 4")]
    row_count: Option<proto_result_set_stats::RowCount>,
}

mod proto_result_set_stats {
    #[derive(Clone, Copy, PartialEq, Eq, Hash, prost::Oneof)]
    pub enum RowCount {
        #[prost(int64, tag = "3")]
        RowCountExact(i64),
        #[prost(int64, tag = "4")]
        RowCountLowerBound(i64),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoResultSet {
    #[prost(message, optional, tag = "1")]
    metadata: Option<ProtoResultSetMetadata>,
    #[prost(message, repeated, tag = "2")]
    rows: Vec<prost_types::ListValue>,
    #[prost(message, optional, tag = "3")]
    stats: Option<ProtoResultSetStats>,
    #[prost(message, optional, tag = "5")]
    precommit_token: Option<ProtoMultiplexedSessionPrecommitToken>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoPartialResultSet {
    #[prost(message, optional, tag = "1")]
    metadata: Option<ProtoResultSetMetadata>,
    #[prost(message, repeated, tag = "2")]
    values: Vec<prost_types::Value>,
    #[prost(bool, tag = "3")]
    chunked_value: bool,
    #[prost(bytes = "bytes", tag = "4")]
    resume_token: Bytes,
    #[prost(message, optional, tag = "5")]
    stats: Option<ProtoResultSetStats>,
    #[prost(message, optional, tag = "8")]
    precommit_token: Option<ProtoMultiplexedSessionPrecommitToken>,
    #[prost(bool, tag = "9")]
    last: bool,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoRetryInfo {
    #[prost(message, optional, tag = "1")]
    retry_delay: Option<prost_types::Duration>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoExecuteBatchDmlResponse {
    #[prost(message, repeated, tag = "1")]
    result_sets: Vec<ProtoResultSet>,
    #[prost(message, optional, tag = "2")]
    status: Option<ProtoRpcStatus>,
    #[prost(message, optional, tag = "3")]
    precommit_token: Option<ProtoMultiplexedSessionPrecommitToken>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoKeySet {
    #[prost(message, repeated, tag = "1")]
    keys: Vec<prost_types::ListValue>,
    #[prost(bool, tag = "3")]
    all: bool,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoMutation {
    #[prost(oneof = "proto_mutation::Operation", tags = "1, 2, 3, 4, 5")]
    operation: Option<proto_mutation::Operation>,
}

mod proto_mutation {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct Write {
        #[prost(string, tag = "1")]
        pub table: String,
        #[prost(string, repeated, tag = "2")]
        pub columns: Vec<String>,
        #[prost(message, repeated, tag = "3")]
        pub values: Vec<prost_types::ListValue>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct Delete {
        #[prost(string, tag = "1")]
        pub table: String,
        #[prost(message, optional, tag = "2")]
        pub key_set: Option<super::ProtoKeySet>,
    }

    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Operation {
        #[prost(message, tag = "1")]
        Insert(Write),
        #[prost(message, tag = "2")]
        Update(Write),
        #[prost(message, tag = "3")]
        InsertOrUpdate(Write),
        #[prost(message, tag = "4")]
        Replace(Write),
        #[prost(message, tag = "5")]
        Delete(Delete),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoCommitRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(message, repeated, tag = "4")]
    mutations: Vec<ProtoMutation>,
    #[prost(bool, tag = "5")]
    return_commit_stats: bool,
    #[prost(message, optional, tag = "8")]
    max_commit_delay: Option<prost_types::Duration>,
    #[prost(message, optional, tag = "6")]
    request_options: Option<ProtoRequestOptions>,
    #[prost(message, optional, tag = "9")]
    precommit_token: Option<ProtoMultiplexedSessionPrecommitToken>,
    #[prost(oneof = "proto_commit_request::Transaction", tags = "2")]
    transaction: Option<proto_commit_request::Transaction>,
}

mod proto_commit_request {
    use bytes::Bytes;

    #[derive(Clone, PartialEq, Eq, Hash, prost::Oneof)]
    pub enum Transaction {
        #[prost(bytes, tag = "2")]
        TransactionId(Bytes),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Message)]
struct ProtoCommitStats {
    #[prost(int64, tag = "1")]
    mutation_count: i64,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoCommitResponse {
    #[prost(message, optional, tag = "1")]
    commit_timestamp: Option<prost_types::Timestamp>,
    #[prost(message, optional, tag = "2")]
    commit_stats: Option<ProtoCommitStats>,
    #[prost(oneof = "proto_commit_response::MultiplexedSessionRetry", tags = "4")]
    multiplexed_session_retry: Option<proto_commit_response::MultiplexedSessionRetry>,
}

mod proto_commit_response {
    #[derive(Clone, PartialEq, Eq, Hash, prost::Oneof)]
    pub enum MultiplexedSessionRetry {
        #[prost(message, tag = "4")]
        PrecommitToken(super::ProtoMultiplexedSessionPrecommitToken),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoRollbackRequest {
    #[prost(string, tag = "1")]
    session: String,
    #[prost(bytes = "bytes", tag = "2")]
    transaction_id: Bytes,
}
