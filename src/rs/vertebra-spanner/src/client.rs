//! Google Cloud Spanner v1 engine (`google.spanner.v1.Spanner` &
//! `google.spanner.admin.database.v1.DatabaseAdmin`) backed by the official
//! `google-cloud-spanner` Rust client.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use google_cloud_longrunning::model::operation::Result as OperationResult;
use google_cloud_spanner::batch::BatchDml;
use google_cloud_spanner::channel_pool::StaticChannelPoolConfig;
use google_cloud_spanner::client::{DatabaseAdmin, DatabaseClient, Spanner, SpannerBuilderExt};
use google_cloud_spanner::model as spanner_model;
use google_cloud_spanner::mutation::MutationGroup;
use google_cloud_spanner::omni::TlsConfig;
use google_cloud_spanner::result::ResultSet;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction::ReadWriteTransaction;
use tokio::sync::OnceCell;
use tonic::transport::Channel;
use url::Url;
use vertebra_core::gcp_auth::GcpTokenCache;
use vertebra_db::gcp::{GcpErrorInfo, install_rustls_ring_provider, official_credentials};

pub use crate::arrow_ipc::*;
pub use crate::codec::{
    ProtobufValue, SpannerBatchDmlStatement, SpannerBatchWriteResult, SpannerCommitResult,
    SpannerDialect, SpannerMutation, SpannerMutationGroup, SpannerQueryParameter,
    SpannerQueryResult, SpannerStaleness, SpannerStructField, SpannerType, SpannerTypeAnnotation,
    SpannerTypeCode, SpannerValue, parse_batch_dml_statements_json, parse_mutation_groups_json,
    parse_mutation_value, parse_mutations_json, parse_spanner_parameters_json,
    parse_spanner_parameters_value, rewrite_spangres_placeholders,
};
use crate::codec::{
    commit_result, official_mutation_rows, official_mutation_rows_owned, official_type,
    official_value, official_value_owned, protobuf_value, spanner_type, timestamp_bound,
};
pub use crate::rw_tx::{SpannerBeginTransactionOptions, SpannerReadWriteTx};

/// Default Cloud Spanner gRPC endpoint.
pub const DEFAULT_SPANNER_ENDPOINT: &str = "https://spanner.googleapis.com";

/// Default Cloud Spanner DatabaseAdmin REST endpoint.
pub const DEFAULT_SPANNER_ADMIN_ENDPOINT: &str = "https://spanner.googleapis.com";

/// Default OAuth2 scopes for Cloud Spanner data-plane and platform authentication.
pub const SPANNER_OAUTH_SCOPES: &[&str] = &[
    "https://www.googleapis.com/auth/spanner.data",
    "https://www.googleapis.com/auth/cloud-platform",
];

/// Default deadline (`600 s`) for `UpdateDatabaseDdl` long-running operation polling.
pub const DEFAULT_DDL_TIMEOUT: Duration = Duration::from_secs(600);

/// Maximum mutated cells per `BatchWrite` or `Commit` batch (`80,000`, below Spanner's `100,000` limit).
pub const MAX_MUTATION_CELLS_PER_BATCH: usize = 80_000;

/// Configuration for [`SpannerClient`].
#[derive(Clone, Debug)]
pub struct SpannerClientConfig {
    /// Fully-qualified database resource (`projects/{p}/instances/{i}/databases/{d}`).
    pub database: String,
    /// Optional fine-grained access control database role (`Session.creator_role`).
    pub database_role: Option<String>,
    /// Cloud Spanner gRPC endpoint (`https://spanner.googleapis.com` by default).
    pub endpoint: String,
    /// REST endpoint for DDL (`UpdateDatabaseDdl`); derived when absent.
    pub admin_endpoint: Option<String>,
    /// Maximum duration to poll an `UpdateDatabaseDdl` operation before returning [`SpannerError::DdlTimeout`].
    pub ddl_timeout: Duration,
    /// SQL dialect (`GoogleSql` or `Spangres`).
    pub dialect: SpannerDialect,
    /// Number of HTTP/2 gRPC channels in the pool (default `4`).
    pub pool_size: usize,
    /// Whether leader-routing metadata (`x-goog-spanner-route-to-leader: true`) is enabled.
    pub route_to_leader_enabled: bool,
    /// Optional `CommitRequest.max_commit_delay` in milliseconds for write batching throughput.
    pub max_commit_delay_ms: Option<u64>,
    /// Options forwarded to `GcpTokenCache::from_options`.
    pub auth_options: HashMap<String, String>,
    /// Optional custom PEM root certificate bundle for non-googleapis `https://` endpoints.
    pub ca_pem: Option<Vec<u8>>,
}

impl SpannerClientConfig {
    /// Create a new [`SpannerClientConfig`] for `database` (`projects/{p}/instances/{i}/databases/{d}`).
    pub fn new(database: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            database_role: None,
            endpoint: DEFAULT_SPANNER_ENDPOINT.to_string(),
            admin_endpoint: None,
            ddl_timeout: DEFAULT_DDL_TIMEOUT,
            dialect: SpannerDialect::GoogleSql,
            pool_size: 4,
            route_to_leader_enabled: true,
            max_commit_delay_ms: None,
            auth_options: HashMap::new(),
            ca_pem: None,
        }
    }

    /// Format a canonical database resource URI from `(project_id, instance_id, database_id)`.
    pub fn format_database_uri(project_id: &str, instance_id: &str, database_id: &str) -> String {
        format!(
            "projects/{}/instances/{}/databases/{}",
            project_id.trim(),
            instance_id.trim(),
            database_id.trim()
        )
    }

    /// Effective gRPC endpoint URL.
    pub fn effective_endpoint(&self) -> String {
        let trimmed = self.endpoint.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return DEFAULT_SPANNER_ENDPOINT.to_string();
        }
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            return trimmed.to_string();
        }
        format!("http://{trimmed}")
    }

    /// Return `true` when targeting a cleartext `http://` endpoint.
    pub fn is_cleartext(&self) -> bool {
        self.effective_endpoint().starts_with("http://")
    }
}

/// Errors produced by [`SpannerClient`].
#[derive(Debug, thiserror::Error)]
pub enum SpannerError {
    #[error("Spanner invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Spanner auth error: {0}")]
    Auth(String),
    #[error("Spanner client error: {0}")]
    Client(#[from] google_cloud_spanner::Error),
    #[error("{}", .0.message)]
    Gcp(GcpErrorInfo),
    #[error("Spanner protocol error: {0}")]
    Protocol(String),
    #[error("UpdateDatabaseDdl operation {operation} failed with code {code}: {message}")]
    Ddl {
        operation: String,
        code: i32,
        message: String,
    },
    #[error(
        "UpdateDatabaseDdl operation {operation} did not finish within {timeout_ms} ms; it may still be running"
    )]
    DdlTimeout { operation: String, timeout_ms: u64 },
}

fn endpoint_is_googleapis(endpoint: &str) -> bool {
    Url::parse(endpoint)
        .ok()
        .and_then(|u| {
            u.host_str()
                .map(|h| h == "googleapis.com" || h.ends_with(".googleapis.com"))
        })
        .unwrap_or(false)
}

fn validate_spanner_config(config: &SpannerClientConfig) -> Result<(), SpannerError> {
    if config.database.trim().is_empty() {
        return Err(SpannerError::InvalidArgument(
            "SpannerClientConfig.database must not be empty".to_string(),
        ));
    }
    let endpoint = config.effective_endpoint();
    if config.ca_pem.is_some() {
        if endpoint_is_googleapis(&endpoint) {
            return Err(SpannerError::InvalidArgument(
                "SpannerClientConfig.ca_pem applies only to non-googleapis endpoints (Spanner Omni or private test servers); Cloud Spanner uses the system trust store".to_string(),
            ));
        }
        if Url::parse(&endpoint)
            .ok()
            .is_some_and(|u| u.scheme() == "http")
        {
            return Err(SpannerError::InvalidArgument(
                "ca_pem requires an https:// endpoint".to_string(),
            ));
        }
    }
    Ok(())
}

async fn collect_query_result(mut rs: ResultSet) -> Result<SpannerQueryResult, SpannerError> {
    let mut raw_rows = Vec::new();
    while let Some(row) = rs.next().await {
        raw_rows.push(row?.into_values());
    }
    let meta = rs.metadata();
    let columns = meta.column_names().to_vec();
    let schema = columns
        .iter()
        .zip(meta.column_types().iter())
        .map(|(name, ty)| SpannerStructField {
            name: name.clone(),
            field_type: spanner_type(ty),
        })
        .collect::<Vec<_>>();
    let default_field_ty = SpannerType::scalar(SpannerTypeCode::String);
    let mut rows = Vec::with_capacity(raw_rows.len());
    for raw_row in raw_rows {
        let mut decoded_row = Vec::with_capacity(raw_row.len());
        for (idx, cell) in raw_row.into_iter().enumerate() {
            let pb = protobuf_value(&cell)?;
            let field_ty = schema
                .get(idx)
                .map(|f| &f.field_type)
                .unwrap_or(&default_field_ty);
            decoded_row.push(SpannerValue::from_protobuf_value_owned(pb, field_ty)?);
        }
        rows.push(decoded_row);
    }
    let row_count_exact = rs.stats().and_then(|s| s.row_count_exact().copied());
    let row_count_lower_bound = rs.stats().and_then(|s| s.row_count_lower_bound().copied());
    Ok(SpannerQueryResult {
        columns,
        schema,
        rows,
        row_count_exact,
        row_count_lower_bound,
    })
}

fn build_routing_header_value(database: &str) -> String {
    let encoded: String =
        url::form_urlencoded::byte_serialize(database.trim().as_bytes()).collect();
    format!("database={encoded}")
}

/// Google Cloud Spanner v1 client.
#[derive(Clone)]
pub struct SpannerClient {
    config: SpannerClientConfig,
    effective_endpoint: String,
    routing_header_value: String,
    token_cache: Option<Arc<GcpTokenCache>>,
    injected_spanner: Option<Arc<Spanner>>,
    injected_admin: Option<Arc<DatabaseAdmin>>,
    official: Arc<OnceCell<Spanner>>,
    database_client: Arc<OnceCell<DatabaseClient>>,
    admin_client: Arc<OnceCell<DatabaseAdmin>>,
    pub(crate) grpc_channel: Arc<OnceCell<Channel>>,
    pub(crate) mux_session: Arc<OnceCell<String>>,
}

impl std::fmt::Debug for SpannerClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpannerClient")
            .field("database", &self.config.database)
            .field("effective_endpoint", &self.effective_endpoint)
            .field("pool_size", &self.config.pool_size)
            .finish()
    }
}

impl SpannerClient {
    /// Construct a new [`SpannerClient`].
    pub fn new(config: SpannerClientConfig) -> Result<Self, SpannerError> {
        validate_spanner_config(&config)?;
        let effective_endpoint = config.effective_endpoint();
        let routing_header_value = build_routing_header_value(&config.database);
        let token_cache =
            if effective_endpoint.starts_with("http://") && config.auth_options.is_empty() {
                None
            } else {
                GcpTokenCache::from_options(&config.auth_options, SPANNER_OAUTH_SCOPES)
            };
        Ok(Self {
            config,
            effective_endpoint,
            routing_header_value,
            token_cache,
            injected_spanner: None,
            injected_admin: None,
            official: Arc::new(OnceCell::new()),
            database_client: Arc::new(OnceCell::new()),
            admin_client: Arc::new(OnceCell::new()),
            grpc_channel: Arc::new(OnceCell::new()),
            mux_session: Arc::new(OnceCell::new()),
        })
    }

    /// Construct a [`SpannerClient`] with pre-built [`Spanner`] and optional [`DatabaseAdmin`] handles.
    pub fn with_clients(
        spanner: Spanner,
        admin: Option<DatabaseAdmin>,
        config: SpannerClientConfig,
    ) -> Result<Self, SpannerError> {
        validate_spanner_config(&config)?;
        let effective_endpoint = config.effective_endpoint();
        let routing_header_value = build_routing_header_value(&config.database);
        Ok(Self {
            config,
            effective_endpoint,
            routing_header_value,
            token_cache: None,
            injected_spanner: Some(Arc::new(spanner)),
            injected_admin: admin.map(Arc::new),
            official: Arc::new(OnceCell::new()),
            database_client: Arc::new(OnceCell::new()),
            admin_client: Arc::new(OnceCell::new()),
            grpc_channel: Arc::new(OnceCell::new()),
            mux_session: Arc::new(OnceCell::new()),
        })
    }

    /// Client configuration reference.
    #[inline]
    pub fn config(&self) -> &SpannerClientConfig {
        &self.config
    }

    /// Optional shared OAuth2 token cache for GCP RPC metadata.
    #[inline]
    pub(crate) fn token_cache(&self) -> Option<&Arc<GcpTokenCache>> {
        self.token_cache.as_ref()
    }

    /// Precomputed effective gRPC endpoint URL.
    #[inline]
    pub fn effective_endpoint(&self) -> &str {
        &self.effective_endpoint
    }

    /// Precomputed `x-goog-request-params` database routing header value.
    #[inline]
    pub fn routing_header_value(&self) -> &str {
        &self.routing_header_value
    }

    /// Resolve the effective DatabaseAdmin REST endpoint for `UpdateDatabaseDdl`.
    pub fn effective_admin_endpoint(&self) -> Result<String, SpannerError> {
        if let Some(ref explicit) = self.config.admin_endpoint {
            let trimmed = explicit.trim().trim_end_matches('/');
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
        let endpoint = &self.effective_endpoint;
        if endpoint_is_googleapis(endpoint) {
            return Ok(DEFAULT_SPANNER_ADMIN_ENDPOINT.to_string());
        }
        if let Ok(mut parsed) = Url::parse(endpoint)
            && parsed.scheme() == "http"
            && parsed.port() == Some(9010)
        {
            let _ = parsed.set_port(Some(9020));
            return Ok(parsed.as_str().trim_end_matches('/').to_string());
        }
        Err(SpannerError::InvalidArgument(format!(
            "set SpannerClientConfig.admin_endpoint: the DDL REST endpoint cannot be derived from {endpoint}"
        )))
    }

    async fn official_spanner(&self) -> Result<&Spanner, SpannerError> {
        if let Some(ref spanner) = self.injected_spanner {
            return Ok(spanner.as_ref());
        }
        self.official
            .get_or_try_init(|| async {
                install_rustls_ring_provider();
                let mut builder = Spanner::builder()
                    .with_endpoint(self.effective_endpoint.clone())
                    .with_credentials(official_credentials(self.token_cache.clone()))
                    .with_channel_pool(StaticChannelPoolConfig::new(self.config.pool_size.max(1)));
                if let Some(ref pem) = self.config.ca_pem {
                    builder = builder
                        .with_omni_tls(TlsConfig::new().with_root_certificate_pem(pem.clone()));
                }
                builder.build().await.map_err(|e| {
                    SpannerError::Protocol(format!("failed to build Spanner client: {e}"))
                })
            })
            .await
    }

    async fn database_client(&self) -> Result<&DatabaseClient, SpannerError> {
        self.database_client
            .get_or_try_init(|| async {
                let spanner = self.official_spanner().await?;
                let mut builder = spanner
                    .database_client(&self.config.database)
                    .with_leader_aware_routing(self.config.route_to_leader_enabled);
                if let Some(ref role) = self.config.database_role
                    && !role.is_empty()
                {
                    builder = builder.with_database_role(role.clone());
                }
                Ok(builder.build().await?)
            })
            .await
    }

    async fn database_admin(&self) -> Result<&DatabaseAdmin, SpannerError> {
        if let Some(ref admin) = self.injected_admin {
            return Ok(admin.as_ref());
        }
        self.admin_client
            .get_or_try_init(|| async {
                install_rustls_ring_provider();
                let admin_ep = self.effective_admin_endpoint()?;
                DatabaseAdmin::builder()
                    .with_endpoint(admin_ep)
                    .with_credentials(official_credentials(self.token_cache.clone()))
                    .build()
                    .await
                    .map_err(|e| {
                        SpannerError::Protocol(format!("failed to build DatabaseAdmin: {e}"))
                    })
            })
            .await
    }

    fn statement(&self, sql: &str, params: &[SpannerQueryParameter]) -> Statement {
        self.statement_with_tag(sql, params, None)
    }

    fn statement_with_tag(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        request_tag: Option<&str>,
    ) -> Statement {
        let effective_sql = if self.config.dialect == SpannerDialect::GoogleSql && sql.contains('$')
        {
            rewrite_spangres_placeholders(sql).0
        } else {
            sql.to_string()
        };
        let mut builder = Statement::builder(effective_sql);
        for param in params {
            builder = builder.add_typed_param(
                &param.name,
                official_value(&param.value),
                official_type(&param.param_type),
            );
        }
        if let Some(tag) = request_tag.filter(|t| !t.is_empty()) {
            builder = builder.set_request_tag(tag);
        }
        builder.build()
    }

    fn statement_owned(&self, sql: &str, params: Vec<SpannerQueryParameter>) -> Statement {
        self.statement_owned_with_tag(sql, params, None)
    }

    fn statement_owned_with_tag(
        &self,
        sql: &str,
        params: Vec<SpannerQueryParameter>,
        request_tag: Option<&str>,
    ) -> Statement {
        let mut builder = if self.config.dialect == SpannerDialect::GoogleSql && sql.contains('$') {
            Statement::builder(rewrite_spangres_placeholders(sql).0)
        } else {
            Statement::builder(sql)
        };
        for param in params {
            builder = builder.add_typed_param(
                param.name,
                official_value_owned(param.value),
                official_type(&param.param_type),
            );
        }
        if let Some(tag) = request_tag.filter(|t| !t.is_empty()) {
            builder = builder.set_request_tag(tag);
        }
        builder.build()
    }

    /// Execute a single-use read-only query with the specified [`SpannerStaleness`] bound.
    pub async fn query(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        staleness: SpannerStaleness,
    ) -> Result<SpannerQueryResult, SpannerError> {
        self.execute_statement_query(self.statement(sql, params), staleness)
            .await
    }

    /// Execute a single-use read-only query consuming owned [`SpannerQueryParameter`] items.
    pub async fn query_owned(
        &self,
        sql: &str,
        params: Vec<SpannerQueryParameter>,
        staleness: SpannerStaleness,
    ) -> Result<SpannerQueryResult, SpannerError> {
        self.execute_statement_query(self.statement_owned(sql, params), staleness)
            .await
    }

    async fn execute_statement_query(
        &self,
        stmt: Statement,
        staleness: SpannerStaleness,
    ) -> Result<SpannerQueryResult, SpannerError> {
        let db = self.database_client().await?;
        let bound = timestamp_bound(staleness)?;
        let rs = db
            .single_use()
            .set_timestamp_bound(bound)
            .build()
            .execute_query(stmt)
            .await?;
        collect_query_result(rs).await
    }

    /// Execute a batch read-only `PartitionQuery` and run all partitions concurrently.
    pub async fn partitioned_query(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        max_partitions: i64,
        data_boost_enabled: bool,
        staleness: SpannerStaleness,
    ) -> Result<SpannerQueryResult, SpannerError> {
        self.execute_statement_partitioned_query(
            self.statement(sql, params),
            max_partitions,
            data_boost_enabled,
            staleness,
        )
        .await
    }

    /// Execute a batch read-only `PartitionQuery` consuming owned [`SpannerQueryParameter`] items.
    pub async fn partitioned_query_owned(
        &self,
        sql: &str,
        params: Vec<SpannerQueryParameter>,
        max_partitions: i64,
        data_boost_enabled: bool,
        staleness: SpannerStaleness,
    ) -> Result<SpannerQueryResult, SpannerError> {
        self.execute_statement_partitioned_query(
            self.statement_owned(sql, params),
            max_partitions,
            data_boost_enabled,
            staleness,
        )
        .await
    }

    async fn execute_statement_partitioned_query(
        &self,
        stmt: Statement,
        max_partitions: i64,
        data_boost_enabled: bool,
        staleness: SpannerStaleness,
    ) -> Result<SpannerQueryResult, SpannerError> {
        let db = self.database_client().await?;
        let bound = timestamp_bound(staleness)?;
        let batch_tx = db
            .batch_read_only_transaction()
            .set_timestamp_bound(bound)
            .build()
            .await?;
        let options =
            spanner_model::PartitionOptions::new().set_max_partitions(max_partitions.max(1));
        let partitions = batch_tx.partition_query(stmt, options).await?;
        let part_results = try_join_all(partitions.into_iter().map(|part| async move {
            let rs = part.set_data_boost(data_boost_enabled).execute(db).await?;
            collect_query_result(rs).await
        }))
        .await?;

        let mut columns = Vec::new();
        let mut schema = Vec::new();
        let mut rows = Vec::new();
        let mut row_count_exact: Option<i64> = None;
        let mut row_count_lower_bound: Option<i64> = None;
        for part in part_results {
            if schema.is_empty() && !part.schema.is_empty() {
                columns = part.columns;
                schema = part.schema;
            }
            rows.extend(part.rows);
            if let Some(exact) = part.row_count_exact {
                row_count_exact = Some(row_count_exact.unwrap_or(0) + exact);
            }
            if let Some(lower) = part.row_count_lower_bound {
                row_count_lower_bound = Some(row_count_lower_bound.unwrap_or(0) + lower);
            }
        }
        Ok(SpannerQueryResult {
            columns,
            schema,
            rows,
            row_count_exact,
            row_count_lower_bound,
        })
    }

    /// Execute a single DML statement inside a read-write transaction and return the modified row count.
    pub async fn execute_dml(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        request_tag: Option<&str>,
        transaction_tag: Option<&str>,
    ) -> Result<i64, SpannerError> {
        self.execute_statement_dml(
            self.statement_with_tag(sql, params, request_tag),
            transaction_tag,
        )
        .await
    }

    /// Execute a single DML statement consuming owned [`SpannerQueryParameter`] items.
    pub async fn execute_dml_owned(
        &self,
        sql: &str,
        params: Vec<SpannerQueryParameter>,
        request_tag: Option<&str>,
        transaction_tag: Option<&str>,
    ) -> Result<i64, SpannerError> {
        self.execute_statement_dml(
            self.statement_owned_with_tag(sql, params, request_tag),
            transaction_tag,
        )
        .await
    }

    async fn execute_statement_dml(
        &self,
        stmt: Statement,
        transaction_tag: Option<&str>,
    ) -> Result<i64, SpannerError> {
        let db = self.database_client().await?;
        let mut runner_builder = db.read_write_transaction();
        if let Some(tag) = transaction_tag.filter(|t| !t.is_empty()) {
            runner_builder = runner_builder.set_transaction_tag(tag);
        }
        if let Some(delay_ms) = self.config.max_commit_delay_ms
            && delay_ms > 0
        {
            runner_builder = runner_builder.set_max_commit_delay(Duration::from_millis(delay_ms));
        }
        let outcome = runner_builder
            .build()
            .run(move |tx: ReadWriteTransaction| {
                let stmt = stmt.clone();
                async move { tx.execute_update(stmt).await }
            })
            .await?;
        let _ = commit_result(&outcome.commit_response)?;
        Ok(outcome.result)
    }

    /// Execute a batch of DML statements inside a read-write transaction and return per-statement row counts.
    pub async fn execute_batch_dml(
        &self,
        statements: &[SpannerBatchDmlStatement],
        request_tag: Option<&str>,
        transaction_tag: Option<&str>,
    ) -> Result<Vec<i64>, SpannerError> {
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        let mut batch_builder = BatchDml::builder();
        for item in statements {
            batch_builder = batch_builder.add_statement(self.statement(&item.sql, &item.params));
        }
        if let Some(tag) = request_tag.filter(|t| !t.is_empty()) {
            batch_builder = batch_builder.set_request_tag(tag);
        }
        self.run_batch_dml(batch_builder.build(), transaction_tag)
            .await
    }

    /// Execute a batch of DML statements consuming owned [`SpannerBatchDmlStatement`] items.
    pub async fn execute_batch_dml_owned(
        &self,
        statements: Vec<SpannerBatchDmlStatement>,
        request_tag: Option<&str>,
        transaction_tag: Option<&str>,
    ) -> Result<Vec<i64>, SpannerError> {
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        let mut batch_builder = BatchDml::builder();
        for item in statements {
            batch_builder =
                batch_builder.add_statement(self.statement_owned(&item.sql, item.params));
        }
        if let Some(tag) = request_tag.filter(|t| !t.is_empty()) {
            batch_builder = batch_builder.set_request_tag(tag);
        }
        self.run_batch_dml(batch_builder.build(), transaction_tag)
            .await
    }

    async fn run_batch_dml(
        &self,
        batch: BatchDml,
        transaction_tag: Option<&str>,
    ) -> Result<Vec<i64>, SpannerError> {
        let db = self.database_client().await?;

        let mut runner_builder = db.read_write_transaction();
        if let Some(tag) = transaction_tag.filter(|t| !t.is_empty()) {
            runner_builder = runner_builder.set_transaction_tag(tag);
        }
        if let Some(delay_ms) = self.config.max_commit_delay_ms
            && delay_ms > 0
        {
            runner_builder = runner_builder.set_max_commit_delay(Duration::from_millis(delay_ms));
        }
        let outcome = runner_builder
            .build()
            .run(move |tx: ReadWriteTransaction| {
                let batch = batch.clone();
                async move { tx.execute_batch_update(batch).await }
            })
            .await?;
        let _ = commit_result(&outcome.commit_response)?;
        Ok(outcome.result)
    }

    /// Execute a Partitioned DML (`PDML`) statement and return the lower-bound modified row count.
    pub async fn execute_partitioned_dml(
        &self,
        sql: &str,
        params: &[SpannerQueryParameter],
        exclude_txn_from_change_streams: bool,
    ) -> Result<i64, SpannerError> {
        self.execute_statement_partitioned_dml(
            self.statement(sql, params),
            exclude_txn_from_change_streams,
        )
        .await
    }

    /// Execute a Partitioned DML (`PDML`) statement consuming owned [`SpannerQueryParameter`] items.
    pub async fn execute_partitioned_dml_owned(
        &self,
        sql: &str,
        params: Vec<SpannerQueryParameter>,
        exclude_txn_from_change_streams: bool,
    ) -> Result<i64, SpannerError> {
        self.execute_statement_partitioned_dml(
            self.statement_owned(sql, params),
            exclude_txn_from_change_streams,
        )
        .await
    }

    async fn execute_statement_partitioned_dml(
        &self,
        stmt: Statement,
        exclude_txn_from_change_streams: bool,
    ) -> Result<i64, SpannerError> {
        let db = self.database_client().await?;
        let count = db
            .partitioned_dml_transaction()
            .set_exclude_txn_from_change_streams(exclude_txn_from_change_streams)
            .build()
            .execute_update(stmt)
            .await?;
        Ok(count)
    }

    /// Commit a list of [`SpannerMutation`] items in a write-only transaction.
    pub async fn apply_mutations(
        &self,
        mutations: &[SpannerMutation],
        transaction_tag: Option<&str>,
        exclude_txn_from_change_streams: bool,
        max_commit_delay_ms: Option<u64>,
    ) -> Result<SpannerCommitResult, SpannerError> {
        if mutations.is_empty() {
            return Ok(SpannerCommitResult {
                commit_timestamp_micros: None,
                mutation_count: Some(0),
                retry_count: 0,
            });
        }
        let official_mutations = mutations
            .iter()
            .flat_map(official_mutation_rows)
            .collect::<Vec<_>>();
        self.commit_official_mutations(
            official_mutations,
            transaction_tag,
            exclude_txn_from_change_streams,
            max_commit_delay_ms,
        )
        .await
    }

    /// Commit owned [`SpannerMutation`] items in a write-only transaction without cloning cell values.
    pub async fn apply_mutations_owned(
        &self,
        mutations: Vec<SpannerMutation>,
        transaction_tag: Option<&str>,
        exclude_txn_from_change_streams: bool,
        max_commit_delay_ms: Option<u64>,
    ) -> Result<SpannerCommitResult, SpannerError> {
        if mutations.is_empty() {
            return Ok(SpannerCommitResult {
                commit_timestamp_micros: None,
                mutation_count: Some(0),
                retry_count: 0,
            });
        }
        let official_mutations = mutations
            .into_iter()
            .flat_map(official_mutation_rows_owned)
            .collect::<Vec<_>>();
        self.commit_official_mutations(
            official_mutations,
            transaction_tag,
            exclude_txn_from_change_streams,
            max_commit_delay_ms,
        )
        .await
    }

    async fn commit_official_mutations(
        &self,
        official_mutations: Vec<google_cloud_spanner::mutation::Mutation>,
        transaction_tag: Option<&str>,
        exclude_txn_from_change_streams: bool,
        max_commit_delay_ms: Option<u64>,
    ) -> Result<SpannerCommitResult, SpannerError> {
        let db = self.database_client().await?;
        let mut tx_builder = db
            .write_only_transaction()
            .set_exclude_txn_from_change_streams(exclude_txn_from_change_streams);
        if let Some(tag) = transaction_tag.filter(|t| !t.is_empty()) {
            tx_builder = tx_builder.set_transaction_tag(tag);
        }
        if let Some(delay_ms) = max_commit_delay_ms.or(self.config.max_commit_delay_ms)
            && delay_ms > 0
        {
            tx_builder = tx_builder.set_max_commit_delay(Duration::from_millis(delay_ms));
        }
        let resp = tx_builder.build().write(official_mutations).await?;
        commit_result(&resp)
    }

    /// Execute `BatchWrite` across `mutation_groups`, chunking batches so each RPC stays at or below
    /// [`MAX_MUTATION_CELLS_PER_BATCH`] (`80,000` cells).
    pub async fn batch_write(
        &self,
        mutation_groups: &[SpannerMutationGroup],
        exclude_txn_from_change_streams: bool,
    ) -> Result<Vec<SpannerBatchWriteResult>, SpannerError> {
        if mutation_groups.is_empty() {
            return Ok(Vec::new());
        }
        let db = self.database_client().await?;
        let mut results = Vec::new();
        let mut chunk: Vec<MutationGroup> = Vec::new();
        let mut chunk_cells: usize = 0;
        let mut chunk_start: i32 = 0;

        for group in mutation_groups {
            let group_cells = group.cell_count();
            if !chunk.is_empty() && chunk_cells + group_cells > MAX_MUTATION_CELLS_PER_BATCH {
                let chunk_len = chunk.len() as i32;
                let mut part = self
                    .batch_write_official_chunk(
                        db,
                        std::mem::take(&mut chunk),
                        chunk_start,
                        exclude_txn_from_change_streams,
                    )
                    .await?;
                results.append(&mut part);
                chunk_start += chunk_len;
                chunk_cells = 0;
            }
            chunk.push(MutationGroup::new(
                group
                    .mutations
                    .iter()
                    .flat_map(official_mutation_rows)
                    .collect(),
            ));
            chunk_cells += group_cells;
        }
        if !chunk.is_empty() {
            let mut part = self
                .batch_write_official_chunk(db, chunk, chunk_start, exclude_txn_from_change_streams)
                .await?;
            results.append(&mut part);
        }
        Ok(results)
    }

    /// Execute `BatchWrite` consuming owned [`SpannerMutationGroup`] items without cloning cells.
    pub async fn batch_write_owned(
        &self,
        mutation_groups: Vec<SpannerMutationGroup>,
        exclude_txn_from_change_streams: bool,
    ) -> Result<Vec<SpannerBatchWriteResult>, SpannerError> {
        if mutation_groups.is_empty() {
            return Ok(Vec::new());
        }
        let db = self.database_client().await?;
        let mut results = Vec::new();
        let mut chunk: Vec<MutationGroup> = Vec::new();
        let mut chunk_cells: usize = 0;
        let mut chunk_start: i32 = 0;

        for group in mutation_groups {
            let group_cells = group.cell_count();
            if !chunk.is_empty() && chunk_cells + group_cells > MAX_MUTATION_CELLS_PER_BATCH {
                let chunk_len = chunk.len() as i32;
                let mut part = self
                    .batch_write_official_chunk(
                        db,
                        std::mem::take(&mut chunk),
                        chunk_start,
                        exclude_txn_from_change_streams,
                    )
                    .await?;
                results.append(&mut part);
                chunk_start += chunk_len;
                chunk_cells = 0;
            }
            chunk.push(MutationGroup::new(
                group
                    .mutations
                    .into_iter()
                    .flat_map(official_mutation_rows_owned)
                    .collect(),
            ));
            chunk_cells += group_cells;
        }
        if !chunk.is_empty() {
            let mut part = self
                .batch_write_official_chunk(db, chunk, chunk_start, exclude_txn_from_change_streams)
                .await?;
            results.append(&mut part);
        }
        Ok(results)
    }

    async fn batch_write_official_chunk(
        &self,
        db: &DatabaseClient,
        official_groups: Vec<MutationGroup>,
        chunk_start: i32,
        exclude_txn_from_change_streams: bool,
    ) -> Result<Vec<SpannerBatchWriteResult>, SpannerError> {
        let mut stream = db
            .batch_write_transaction()
            .set_exclude_txn_from_change_streams(exclude_txn_from_change_streams)
            .build()
            .execute_streaming(official_groups)
            .await?;
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let resp = item?;
            let (status_code, status_message) = resp
                .status
                .as_ref()
                .map(|st| (st.code, st.message.clone()))
                .unwrap_or((0, String::new()));
            let commit_timestamp_micros = resp.commit_timestamp.as_ref().map(|ts| {
                ts.seconds()
                    .saturating_mul(1_000_000)
                    .saturating_add(i64::from(ts.nanos()) / 1_000)
            });
            out.push(SpannerBatchWriteResult {
                indexes: resp.indexes.into_iter().map(|i| i + chunk_start).collect(),
                status_code,
                status_message,
                commit_timestamp_micros,
            });
        }
        Ok(out)
    }

    /// Execute batched DDL statements via `DatabaseAdmin::update_database_ddl` and poll `get_operation`
    /// until `done == true` or `config.ddl_timeout` elapses.
    pub async fn update_database_ddl(&self, statements: &[String]) -> Result<String, SpannerError> {
        if statements.is_empty() {
            return Ok(String::new());
        }
        let admin = self.database_admin().await?;
        let deadline = Instant::now() + self.config.ddl_timeout;
        let timeout_ms = self.config.ddl_timeout.as_millis() as u64;
        let mut op = admin
            .update_database_ddl()
            .set_database(&self.config.database)
            .set_statements(statements.to_vec())
            .send()
            .await
            .map_err(|e| SpannerError::Protocol(format!("UpdateDatabaseDdl failed: {e}")))?;

        let mut backoff = Duration::from_millis(100);
        loop {
            if op.done {
                if let Some(OperationResult::Error(status)) = op.result {
                    return Err(SpannerError::Ddl {
                        operation: op.name,
                        code: status.code,
                        message: status.message,
                    });
                }
                return Ok(op.name);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(SpannerError::DdlTimeout {
                    operation: op.name,
                    timeout_ms,
                });
            }
            let sleep_for = backoff.min(deadline.saturating_duration_since(now));
            tokio::time::sleep(sleep_for).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
            let op_name = op.name.clone();
            op = admin
                .get_operation()
                .set_name(&op_name)
                .send()
                .await
                .map_err(|e| {
                    SpannerError::Protocol(format!("GetOperation({op_name}) failed: {e}"))
                })?;
        }
    }
}
