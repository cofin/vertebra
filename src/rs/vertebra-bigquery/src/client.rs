//! Pure-Rust Google Cloud BigQuery REST v2 (`jobs.query` + `jobs.insert`) and Storage Read/Write API v1
//! (`BigQueryRead` -> `VTBARROW` `ShmRegion` & `BigQueryWrite.AppendRows`) client engine.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use google_cloud_bigquery::client::{Read, Write};
use google_cloud_bigquery::error::{AppendError, CommitError, WriterBuilderError};
use google_cloud_bigquery::model as bq_model;
use google_cloud_bigquery::write::retry_policy::RetryableErrors as WriteRetryableErrors;
use google_cloud_bigquery::write::stream_type::{BufferedStream, CommittedStream, PendingStream};
use google_cloud_gax::retry_policy::RetryPolicyExt as _;
use serde_json::{Map, Value, json};
use thiserror::Error;
use vertebra_core::gcp_auth::GcpTokenCache;
use vertebra_core::id::uuid4_str;
use vertebra_core::shm::ShmRegion;
use vertebra_db::gcp::{install_rustls_ring_provider, official_credentials};

use crate::arrow_ipc::{build_arrow_record_batch, write_read_rows_batches_to_shm};
use crate::codec::{
    check_rest_error, decode_single_query_response, decode_table_row, parse_json_i64,
    rest_error_details,
};
pub use crate::{arrow_ipc::*, codec::*};

/// Default BigQuery REST v2 endpoint.
pub const DEFAULT_BIGQUERY_REST_ENDPOINT: &str = "https://bigquery.googleapis.com";

/// Default BigQuery Storage API v1 gRPC endpoint.
pub const DEFAULT_BIGQUERY_STORAGE_ENDPOINT: &str = "https://bigquerystorage.googleapis.com:443";

/// Default OAuth2 scopes for BigQuery REST v2 and Storage API v1 authentication.
pub const BIGQUERY_OAUTH_SCOPES: &[&str] = &[
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
];

/// Errors produced by [`BigQueryClient`].
#[derive(Debug, Error)]
pub enum BigQueryError {
    #[error("BigQuery authentication error: {0}")]
    Auth(String),
    #[error("BigQuery HTTP transport error: {0}")]
    Http(String),
    #[error("BigQuery REST API error (HTTP {status}): {message}")]
    Api {
        status: u16,
        message: String,
        reason: Option<String>,
        canonical_status: Option<String>,
    },
    #[error("BigQuery client error: {0}")]
    Client(#[from] google_cloud_bigquery::Error),
    #[error("BigQuery shared-memory Arrow error: {0}")]
    Shm(String),
    #[error("BigQuery Storage Write row errors: {0:?}")]
    RowErrors(Vec<BigQueryRowError>),
    #[error("BigQuery Storage Write verification failed: {0}")]
    AppendVerification(String),
    #[error("BigQuery invalid argument: {0}")]
    InvalidArgument(String),
}

/// Configuration for [`BigQueryClient`].
#[derive(Debug, Clone)]
pub struct BigQueryClientConfig {
    pub project_id: String,
    pub dataset_id: Option<String>,
    pub location: Option<String>,
    pub rest_endpoint: String,
    pub storage_endpoint: String,
    pub maximum_bytes_billed: Option<i64>,
    pub job_timeout_ms: Option<u64>,
    pub priority: Option<String>,
    pub labels: HashMap<String, String>,
    pub auth_options: HashMap<String, String>,
    pub pool_size: usize,
    /// PEM roots added to the REST (`jobs.query`/`jobs.insert`) client. The Storage Read/Write gRPC clients use the system trust store plus bundled roots and ignore this value.
    pub ca_pem: Option<Vec<u8>>,
}

impl BigQueryClientConfig {
    /// Create a new configuration for `project_id` with production defaults.
    pub fn new(project_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            dataset_id: None,
            location: None,
            rest_endpoint: DEFAULT_BIGQUERY_REST_ENDPOINT.to_string(),
            storage_endpoint: DEFAULT_BIGQUERY_STORAGE_ENDPOINT.to_string(),
            maximum_bytes_billed: None,
            job_timeout_ms: None,
            priority: None,
            labels: HashMap::new(),
            auth_options: HashMap::new(),
            pool_size: 4,
            ca_pem: None,
        }
    }

    /// Resolve the effective REST base URL.
    pub fn effective_rest_endpoint(&self) -> String {
        let trimmed = self.rest_endpoint.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return DEFAULT_BIGQUERY_REST_ENDPOINT.to_string();
        }
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            return trimmed.to_string();
        }
        format!("http://{trimmed}")
    }

    /// Resolve the effective Storage gRPC endpoint URL.
    pub fn effective_storage_endpoint(&self) -> String {
        let trimmed = self.storage_endpoint.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return DEFAULT_BIGQUERY_STORAGE_ENDPOINT.to_string();
        }
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            return trimmed.to_string();
        }
        format!("http://{trimmed}")
    }
}

/// Pure-Rust Google Cloud BigQuery client over REST v2 and Storage Read/Write API v1.
pub struct BigQueryClient {
    config: BigQueryClientConfig,
    rest_endpoint: String,
    storage_endpoint: String,
    queries_url: String,
    jobs_url: String,
    queries_base_url: Option<url::Url>,
    project_parent: String,
    default_dataset_json: Option<Value>,
    config_labels_json: Option<Value>,
    max_bytes_billed_str: Option<String>,
    job_timeout_ms_str: Option<String>,
    routing_header: Option<reqwest::header::HeaderValue>,
    quota_project_header: Option<reqwest::header::HeaderValue>,
    http_client: reqwest::Client,
    token_cache: Option<Arc<GcpTokenCache>>,
    injected_read: Option<Read>,
    read_client: tokio::sync::OnceCell<Read>,
    write_client: tokio::sync::OnceCell<Write>,
}

impl BigQueryClient {
    /// Construct a new [`BigQueryClient`] from `config`.
    pub fn new(config: BigQueryClientConfig) -> Result<Self, BigQueryError> {
        let rest_endpoint = config.effective_rest_endpoint();
        let storage_endpoint = config.effective_storage_endpoint();
        let is_cleartext =
            rest_endpoint.starts_with("http://") || storage_endpoint.starts_with("http://");

        let mut builder = reqwest::Client::builder()
            .pool_max_idle_per_host(config.pool_size.max(1))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120));

        if is_cleartext {
            builder = builder.no_proxy();
        }

        if let Some(ref pem) = config.ca_pem {
            let certs = reqwest::Certificate::from_pem_bundle(pem).map_err(|e| {
                BigQueryError::InvalidArgument(format!("invalid BigQuery ca_pem certificate: {e}"))
            })?;
            if certs.is_empty() {
                return Err(BigQueryError::InvalidArgument(
                    "invalid BigQuery ca_pem certificate: no PEM certificate blocks found".into(),
                ));
            }
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }

        let http_client = builder
            .build()
            .map_err(|e| BigQueryError::Http(e.to_string()))?;

        let mut auth_opts = config.auth_options.clone();
        if is_cleartext {
            auth_opts
                .entry("allow_ambient_adc".to_string())
                .or_insert_with(|| "false".to_string());
        }
        let token_cache = GcpTokenCache::from_options(&auth_opts, BIGQUERY_OAUTH_SCOPES);

        let queries_url = format!(
            "{rest_endpoint}/bigquery/v2/projects/{}/queries",
            config.project_id
        );
        let jobs_url = format!(
            "{rest_endpoint}/bigquery/v2/projects/{}/jobs",
            config.project_id
        );
        let queries_base_url = url::Url::parse(&rest_endpoint).ok().and_then(|mut u| {
            u.path_segments_mut().ok()?.extend([
                "bigquery",
                "v2",
                "projects",
                config.project_id.as_str(),
                "queries",
            ]);
            Some(u)
        });
        let project_parent = format!("projects/{}", config.project_id);
        let default_dataset_json = config
            .dataset_id
            .as_deref()
            .filter(|ds| !ds.is_empty())
            .map(|ds| {
                json!({
                    "projectId": config.project_id,
                    "datasetId": ds,
                })
            });
        let config_labels_json = if config.labels.is_empty() {
            None
        } else {
            Some(json!(config.labels))
        };
        let max_bytes_billed_str = config.maximum_bytes_billed.map(|n| n.to_string());
        let job_timeout_ms_str = config.job_timeout_ms.map(|n| n.to_string());
        let routing_header = if config.project_id.trim().is_empty() {
            None
        } else {
            let encoded_proj: String =
                url::form_urlencoded::byte_serialize(config.project_id.trim().as_bytes()).collect();
            reqwest::header::HeaderValue::from_str(&format!("project_id={encoded_proj}")).ok()
        };
        let quota_project_header = config
            .auth_options
            .get("quota_project_id")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .and_then(|qp| reqwest::header::HeaderValue::from_str(qp).ok());

        Ok(Self {
            config,
            rest_endpoint,
            storage_endpoint,
            queries_url,
            jobs_url,
            queries_base_url,
            project_parent,
            default_dataset_json,
            config_labels_json,
            max_bytes_billed_str,
            job_timeout_ms_str,
            routing_header,
            quota_project_header,
            http_client,
            token_cache,
            injected_read: None,
            read_client: tokio::sync::OnceCell::new(),
            write_client: tokio::sync::OnceCell::new(),
        })
    }

    /// Construct a [`BigQueryClient`] with an injected Storage [`Read`] client (for stub testing).
    pub fn with_read_client(
        config: BigQueryClientConfig,
        read: Read,
    ) -> Result<Self, BigQueryError> {
        let mut client = Self::new(config)?;
        client.injected_read = Some(read);
        Ok(client)
    }

    async fn storage_read(&self) -> Result<&Read, BigQueryError> {
        if let Some(ref read) = self.injected_read {
            return Ok(read);
        }
        self.read_client
            .get_or_try_init(|| async {
                install_rustls_ring_provider();
                Read::builder()
                    .with_endpoint(self.storage_endpoint.clone())
                    .with_credentials(official_credentials(self.token_cache.clone()))
                    .build()
                    .await
                    .map_err(|e| {
                        BigQueryError::Http(format!("failed to build BigQuery Read client: {e}"))
                    })
            })
            .await
    }

    async fn storage_write(&self) -> Result<&Write, BigQueryError> {
        self.write_client
            .get_or_try_init(|| async {
                install_rustls_ring_provider();
                Write::builder()
                    .with_endpoint(self.storage_endpoint.clone())
                    .with_credentials(official_credentials(self.token_cache.clone()))
                    .build()
                    .await
                    .map_err(|e| {
                        BigQueryError::Http(format!("failed to build BigQuery Write client: {e}"))
                    })
            })
            .await
    }

    /// Return a reference to the client configuration.
    pub fn config(&self) -> &BigQueryClientConfig {
        &self.config
    }

    /// Precomputed effective BigQuery REST v2 base URL.
    #[inline]
    pub fn rest_endpoint(&self) -> &str {
        &self.rest_endpoint
    }

    /// Precomputed effective BigQuery Storage API gRPC base URL.
    #[inline]
    pub fn storage_endpoint(&self) -> &str {
        &self.storage_endpoint
    }

    /// Precomputed `x-goog-request-params` header value for project routing.
    #[inline]
    pub fn routing_header(&self) -> Option<&reqwest::header::HeaderValue> {
        self.routing_header.as_ref()
    }

    /// Execute a GoogleSQL statement or query over BigQuery REST v2 (`jobs.query` fast-path with
    /// automatic promotion to `jobs.insert`, poll continuation, and `pageToken` drain).
    pub async fn query(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        session_id: Option<&str>,
    ) -> Result<BigQueryQueryResult, BigQueryError> {
        let opts = BigQueryQueryOptions {
            session_id: session_id.map(str::to_string),
            ..Default::default()
        };
        self.query_with_options(sql, params, &opts).await
    }

    /// Execute a GoogleSQL query with explicit [`BigQueryQueryOptions`].
    pub async fn query_with_options(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        options: &BigQueryQueryOptions,
    ) -> Result<BigQueryQueryResult, BigQueryError> {
        if options.dry_run
            || options.destination_table.is_some()
            || options.priority.as_deref() == Some("BATCH")
            || requires_jobs_insert_sql(sql)
        {
            return self.execute_jobs_insert_query(sql, params, options).await;
        }
        self.execute_fastpath_query(sql, params, options).await
    }

    /// Execute a `dryRun: true` query via `jobs.insert` to estimate bytes processed and extract output schema.
    pub async fn dry_run(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
    ) -> Result<BigQueryDryRunResult, BigQueryError> {
        let opts = BigQueryQueryOptions {
            dry_run: true,
            ..Default::default()
        };
        let payload = self.build_jobs_insert_payload(sql, params, &opts);
        let resp = self
            .send_rest_json(reqwest::Method::POST, &self.jobs_url, Some(&payload))
            .await?;
        check_rest_error(&resp)?;

        let stats = resp.get("statistics");
        let query_stats = stats.and_then(|s| s.get("query"));
        let total_bytes_processed = query_stats
            .and_then(|q| q.get("totalBytesProcessed"))
            .or_else(|| stats.and_then(|s| s.get("totalBytesProcessed")))
            .and_then(parse_json_i64)
            .unwrap_or(0);
        let total_bytes_billed = query_stats
            .and_then(|q| q.get("totalBytesBilled"))
            .or_else(|| stats.and_then(|s| s.get("totalBytesBilled")))
            .and_then(parse_json_i64)
            .unwrap_or(total_bytes_processed);
        let cache_hit = query_stats
            .and_then(|q| q.get("cacheHit"))
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let referenced_tables = query_stats
            .and_then(|q| q.get("referencedTables"))
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let p = t.get("projectId")?.as_str()?;
                        let d = t.get("datasetId")?.as_str()?;
                        let tbl = t.get("tableId")?.as_str()?;
                        Some(format!("{p}.{d}.{tbl}"))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let schema = query_stats
            .and_then(|q| q.get("schema"))
            .or_else(|| resp.get("schema"))
            .and_then(|s| s.get("fields"))
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(BigQueryFieldSchema::from_rest_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Ok(BigQueryDryRunResult {
            total_bytes_processed,
            total_bytes_billed,
            cache_hit,
            referenced_tables,
            schema,
        })
    }

    /// Read a BigQuery table via Storage Read API v1 (`CreateReadSession` + parallel `ReadRows` streams)
    /// directly into a zero-copy `VTBARROW` shared-memory `ShmRegion`.
    pub async fn read_table_to_shm_arrow(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        selected_fields: &[String],
        row_restriction: Option<&str>,
        max_streams: i32,
    ) -> Result<(String, usize, usize, Arc<ShmRegion>), BigQueryError> {
        self.read_table_to_shm_arrow_with_compression(
            project_id,
            dataset_id,
            table_id,
            selected_fields,
            row_restriction,
            max_streams,
            BigQueryArrowCompression::Lz4Frame,
        )
        .await
    }

    /// Read a BigQuery table via Storage Read API v1 with an explicit Arrow compression codec.
    #[allow(clippy::too_many_arguments)]
    pub async fn read_table_to_shm_arrow_with_compression(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
        selected_fields: &[String],
        row_restriction: Option<&str>,
        max_streams: i32,
        compression: BigQueryArrowCompression,
    ) -> Result<(String, usize, usize, Arc<ShmRegion>), BigQueryError> {
        let read = self.storage_read().await?;
        let eff_project = if project_id.trim().is_empty() {
            &self.config.project_id
        } else {
            project_id.trim()
        };
        let table_resource =
            format!("projects/{eff_project}/datasets/{dataset_id}/tables/{table_id}");
        let parent = if eff_project == self.config.project_id {
            self.project_parent.clone()
        } else {
            format!("projects/{eff_project}")
        };

        let mut read_opts = bq_model::read_session::TableReadOptions::new();
        let mut has_read_opts = false;
        if !selected_fields.is_empty() {
            read_opts = read_opts.set_selected_fields(selected_fields.to_vec());
            has_read_opts = true;
        }
        if let Some(filter) = row_restriction.filter(|s| !s.is_empty()) {
            read_opts = read_opts.set_row_restriction(filter);
            has_read_opts = true;
        }
        let codec = match compression {
            BigQueryArrowCompression::Unspecified => None,
            BigQueryArrowCompression::Lz4Frame => {
                Some(bq_model::arrow_serialization_options::CompressionCodec::Lz4Frame)
            }
            BigQueryArrowCompression::Zstd => {
                Some(bq_model::arrow_serialization_options::CompressionCodec::Zstd)
            }
        };
        if let Some(c) = codec {
            read_opts = read_opts.set_arrow_serialization_options(
                bq_model::ArrowSerializationOptions::new().set_buffer_compression(c),
            );
            has_read_opts = true;
        }

        let mut req_session = bq_model::ReadSession::new()
            .set_table(table_resource)
            .set_data_format(bq_model::DataFormat::Arrow);
        if has_read_opts {
            req_session = req_session.set_read_options(read_opts);
        }

        let session = read
            .create_read_session()
            .set_parent(parent)
            .set_read_session(req_session)
            .set_max_stream_count(max_streams.max(1))
            .send()
            .await?;

        let schema_bytes = session
            .arrow_schema()
            .map(|s| s.serialized_schema.clone())
            .filter(|b| !b.is_empty())
            .ok_or_else(|| {
                BigQueryError::InvalidArgument(
                    "CreateReadSession returned no Arrow schema".to_string(),
                )
            })?;

        let mut join_set = tokio::task::JoinSet::new();
        for (stream_idx, stream) in session.streams.into_iter().enumerate() {
            let read_clone = read.clone();
            let stream_name = stream.name;
            join_set.spawn(async move {
                let mut reader = read_clone
                    .read_rows()
                    .set_read_stream(stream_name)
                    .set_offset(0)
                    .into_reader();
                let mut batches = Vec::new();
                while let Some(resp) = reader.next().await {
                    let resp = resp?;
                    if let Some(batch) = resp.arrow_record_batch()
                        && !batch.serialized_record_batch.is_empty()
                    {
                        let rows = resp.row_count.max(0) as usize;
                        batches.push((batch.serialized_record_batch.clone(), rows));
                    }
                }
                Ok::<(usize, Vec<(Bytes, usize)>), BigQueryError>((stream_idx, batches))
            });
        }

        let mut results_by_stream = Vec::with_capacity(join_set.len());
        while let Some(joined) = join_set.join_next().await {
            let pair =
                joined.map_err(|e| BigQueryError::Http(format!("ReadRows task failed: {e}")))??;
            results_by_stream.push(pair);
        }

        write_read_rows_batches_to_shm(&schema_bytes, results_by_stream)
    }

    /// Append an Arrow IPC stream slice to `parent_table` (`projects/{p}/datasets/{d}/tables/{t}`) over
    /// BigQuery Storage Write API v1 (`_default`, `COMMITTED`, `PENDING`, or `BUFFERED`).
    pub async fn append_arrow_stream(
        &self,
        parent_table: &str,
        arrow_ipc_bytes: &[u8],
        mode: &str,
    ) -> Result<BigQueryAppendResult, BigQueryError> {
        let _ = BigQueryWriteMode::from_str_mode(mode)?;
        let table = self.normalize_table_resource(parent_table)?;
        if arrow_ipc_bytes.is_empty() {
            return Ok(BigQueryAppendResult {
                appended_rows: 0,
                write_stream: format!("{table}/streams/_default"),
            });
        }
        self.append_arrow_stream_bytes(&table, Bytes::copy_from_slice(arrow_ipc_bytes), mode)
            .await
    }

    /// Append an owned [`Bytes`] Arrow IPC stream to `parent_table` with zero-copy frame slicing.
    pub async fn append_arrow_stream_bytes(
        &self,
        parent_table: &str,
        arrow_ipc_bytes: Bytes,
        mode: &str,
    ) -> Result<BigQueryAppendResult, BigQueryError> {
        let write_mode = BigQueryWriteMode::from_str_mode(mode)?;
        let table = self.normalize_table_resource(parent_table)?;
        let plan = ArrowAppendPlan::build_bytes(arrow_ipc_bytes, write_mode)?;
        if plan.requests.is_empty() {
            return Ok(BigQueryAppendResult {
                appended_rows: 0,
                write_stream: format!("{table}/streams/_default"),
            });
        }

        let write = self.storage_write().await?;
        let schema = bq_model::ArrowSchema::new().set_serialized_schema(plan.schema.clone());

        match write_mode {
            BigQueryWriteMode::Default => {
                let writer = write
                    .open_default_stream(&table)
                    .with_retry_policy(WriteRetryableErrors.with_attempt_limit(3))
                    .build_arrow(schema)
                    .await
                    .map_err(map_writer_builder_error)?;
                let mut pending = Vec::with_capacity(plan.requests.len());
                for req in &plan.requests {
                    pending.push(writer.append(build_arrow_record_batch(req)).send());
                }
                let mut offsets = Vec::with_capacity(pending.len());
                for fut in pending {
                    let resp = fut.await.map_err(map_append_error)?;
                    offsets.push(resp.offset);
                }
                let appended_rows = plan.verify_responses(&offsets)?;
                Ok(BigQueryAppendResult {
                    appended_rows,
                    write_stream: format!("{table}/streams/_default"),
                })
            }
            BigQueryWriteMode::Committed => {
                let writer = write
                    .create_stream::<CommittedStream>(&table)
                    .build_arrow(schema)
                    .await
                    .map_err(map_writer_builder_error)?;
                let write_stream = writer.write_stream().to_string();
                let mut pending = Vec::with_capacity(plan.requests.len());
                for req in &plan.requests {
                    pending.push(
                        writer
                            .append(build_arrow_record_batch(req))
                            .set_offset(req.offset.unwrap_or(0))
                            .send(),
                    );
                }
                let mut offsets = Vec::with_capacity(pending.len());
                for fut in pending {
                    let resp = fut.await.map_err(map_append_error)?;
                    offsets.push(resp.offset);
                }
                let appended_rows = plan.verify_responses(&offsets)?;
                Ok(BigQueryAppendResult {
                    appended_rows,
                    write_stream,
                })
            }
            BigQueryWriteMode::Pending => {
                let writer = write
                    .create_stream::<PendingStream>(&table)
                    .build_arrow(schema)
                    .await
                    .map_err(map_writer_builder_error)?;
                let write_stream = writer.write_stream().to_string();
                let mut pending = Vec::with_capacity(plan.requests.len());
                for req in &plan.requests {
                    pending.push(
                        writer
                            .append(build_arrow_record_batch(req))
                            .set_offset(req.offset.unwrap_or(0))
                            .send(),
                    );
                }
                let mut offsets = Vec::with_capacity(pending.len());
                for fut in pending {
                    let resp = fut.await.map_err(map_append_error)?;
                    offsets.push(resp.offset);
                }
                let appended_rows = plan.verify_responses(&offsets)?;
                writer.finalize().await?;
                writer.commit().await.map_err(map_commit_error)?;
                Ok(BigQueryAppendResult {
                    appended_rows,
                    write_stream,
                })
            }
            BigQueryWriteMode::Buffered => {
                let writer = write
                    .create_stream::<BufferedStream>(&table)
                    .build_arrow(schema)
                    .await
                    .map_err(map_writer_builder_error)?;
                let write_stream = writer.write_stream().to_string();
                let mut pending = Vec::with_capacity(plan.requests.len());
                for req in &plan.requests {
                    pending.push(
                        writer
                            .append(build_arrow_record_batch(req))
                            .set_offset(req.offset.unwrap_or(0))
                            .send(),
                    );
                }
                let mut offsets = Vec::with_capacity(pending.len());
                for fut in pending {
                    let resp = fut.await.map_err(map_append_error)?;
                    offsets.push(resp.offset);
                }
                let appended_rows = plan.verify_responses(&offsets)?;
                if plan.total_rows() > 0 {
                    writer.flush(plan.total_rows() - 1).await?;
                }
                Ok(BigQueryAppendResult {
                    appended_rows,
                    write_stream,
                })
            }
        }
    }

    fn normalize_table_resource(&self, table: &str) -> Result<String, BigQueryError> {
        let trimmed = table.trim();
        if trimmed.starts_with("projects/") {
            return Ok(trimmed.to_string());
        }
        let parts = split_bigquery_table_parts(trimmed);
        match parts.as_slice() {
            [p, d, t] => Ok(format!("projects/{p}/datasets/{d}/tables/{t}")),
            [d, t] => Ok(format!(
                "projects/{}/datasets/{d}/tables/{t}",
                self.config.project_id
            )),
            [t] if !t.is_empty() => {
                let ds = self.config.dataset_id.as_deref().ok_or_else(|| {
                    BigQueryError::InvalidArgument(format!(
                        "unqualified table `{t}` requires dataset_id in BigQueryClientConfig"
                    ))
                })?;
                Ok(format!(
                    "projects/{}/datasets/{ds}/tables/{t}",
                    self.config.project_id
                ))
            }
            _ => Err(BigQueryError::InvalidArgument(format!(
                "invalid BigQuery table identifier: `{table}`"
            ))),
        }
    }

    async fn execute_fastpath_query(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        options: &BigQueryQueryOptions,
    ) -> Result<BigQueryQueryResult, BigQueryError> {
        let payload = self.build_fastpath_query_payload(sql, params, options);
        let resp = self
            .send_rest_json(reqwest::Method::POST, &self.queries_url, Some(&payload))
            .await?;
        self.collect_query_response_pages(resp).await
    }

    async fn execute_jobs_insert_query(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        options: &BigQueryQueryOptions,
    ) -> Result<BigQueryQueryResult, BigQueryError> {
        let payload = self.build_jobs_insert_payload(sql, params, options);
        let resp = self
            .send_rest_json(reqwest::Method::POST, &self.jobs_url, Some(&payload))
            .await?;
        check_rest_error(&resp)?;

        let job_ref = resp.get("jobReference");
        let job_id = job_ref
            .and_then(|jr| jr.get("jobId"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let location = job_ref
            .and_then(|jr| jr.get("location"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| self.config.location.clone());

        let Some(ref jid) = job_id else {
            return decode_single_query_response(&resp);
        };

        let query_results_url = self.build_get_query_results_url(jid, location.as_deref(), None)?;
        match self
            .send_rest_json(reqwest::Method::GET, &query_results_url, None)
            .await
        {
            Ok(qresp) => self.collect_query_response_pages(qresp).await,
            Err(BigQueryError::Api { status: 404, .. }) => decode_single_query_response(&resp),
            Err(e) => Err(e),
        }
    }

    async fn collect_query_response_pages(
        &self,
        mut current: Value,
    ) -> Result<BigQueryQueryResult, BigQueryError> {
        check_rest_error(&current)?;

        let mut job_id = current
            .get("jobReference")
            .and_then(|jr| jr.get("jobId"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let mut location = current
            .get("jobReference")
            .and_then(|jr| jr.get("location"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| self.config.location.clone());

        while !current
            .get("jobComplete")
            .and_then(Value::as_bool)
            .unwrap_or(true)
        {
            let jid = job_id.as_deref().ok_or_else(|| {
                BigQueryError::InvalidArgument(
                    "BigQuery response has jobComplete=false without jobReference.jobId"
                        .to_string(),
                )
            })?;
            let poll_url = self.build_get_query_results_url(jid, location.as_deref(), None)?;
            current = self
                .send_rest_json(reqwest::Method::GET, &poll_url, None)
                .await?;
            check_rest_error(&current)?;
            if job_id.is_none() {
                job_id = current
                    .get("jobReference")
                    .and_then(|jr| jr.get("jobId"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
            if location.is_none() {
                location = current
                    .get("jobReference")
                    .and_then(|jr| jr.get("location"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
        }

        let mut result = decode_single_query_response(&current)?;
        if result.job_id.is_none() {
            result.job_id = job_id.clone();
        }

        let mut page_token = current
            .get("pageToken")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        while let Some(ref token) = page_token {
            let jid = result
                .job_id
                .as_deref()
                .or(job_id.as_deref())
                .ok_or_else(|| {
                    BigQueryError::InvalidArgument(
                        "BigQuery response returned pageToken without jobReference.jobId"
                            .to_string(),
                    )
                })?;
            let page_url =
                self.build_get_query_results_url(jid, location.as_deref(), Some(token.as_str()))?;
            let page_resp = self
                .send_rest_json(reqwest::Method::GET, &page_url, None)
                .await?;
            check_rest_error(&page_resp)?;
            if result.schema.is_empty()
                && let Some(fields) = page_resp
                    .get("schema")
                    .and_then(|s| s.get("fields"))
                    .and_then(Value::as_array)
            {
                result.schema = fields
                    .iter()
                    .filter_map(BigQueryFieldSchema::from_rest_json)
                    .collect();
                result.columns = result.schema.iter().map(|f| f.name.clone()).collect();
            }
            if let Some(raw_rows) = page_resp.get("rows").and_then(Value::as_array) {
                for raw_row in raw_rows {
                    result.rows.push(decode_table_row(raw_row, &result.schema));
                }
            }
            page_token = page_resp
                .get("pageToken")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
        }

        if result.total_rows == 0 && !result.rows.is_empty() {
            result.total_rows = result.rows.len() as u64;
        }
        Ok(result)
    }

    fn build_get_query_results_url(
        &self,
        job_id: &str,
        location: Option<&str>,
        page_token: Option<&str>,
    ) -> Result<String, BigQueryError> {
        let mut url = match self.queries_base_url {
            Some(ref base) => base.clone(),
            None => {
                let mut u = url::Url::parse(&self.rest_endpoint).map_err(|e| {
                    BigQueryError::InvalidArgument(format!("invalid BigQuery REST endpoint: {e}"))
                })?;
                u.path_segments_mut()
                    .map_err(|()| {
                        BigQueryError::InvalidArgument(
                            "REST endpoint cannot be a base URL".to_string(),
                        )
                    })?
                    .extend([
                        "bigquery",
                        "v2",
                        "projects",
                        self.config.project_id.as_str(),
                        "queries",
                    ]);
                u
            }
        };
        url.path_segments_mut()
            .map_err(|()| {
                BigQueryError::InvalidArgument("REST endpoint cannot be a base URL".to_string())
            })?
            .push(job_id);
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("formatOptions.useInt64Timestamp", "true");
            if let Some(loc) = location.filter(|s| !s.is_empty()) {
                pairs.append_pair("location", loc);
            }
            if let Some(tok) = page_token.filter(|s| !s.is_empty()) {
                pairs.append_pair("pageToken", tok);
            }
        }
        Ok(url.into())
    }

    fn build_fastpath_query_payload(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        options: &BigQueryQueryOptions,
    ) -> Value {
        let mut body = Map::new();
        body.insert("query".to_string(), Value::String(sql.to_string()));
        body.insert("useLegacySql".to_string(), Value::Bool(false));
        body.insert(
            "jobCreationMode".to_string(),
            Value::String("JOB_CREATION_OPTIONAL".to_string()),
        );
        body.insert(
            "formatOptions".to_string(),
            json!({ "useInt64Timestamp": true }),
        );
        body.insert("requestId".to_string(), Value::String(uuid4_str()));

        if let Some(ref ds_json) = self.default_dataset_json {
            body.insert("defaultDataset".to_string(), ds_json.clone());
        }
        if let Some(ref loc) = self.config.location
            && !loc.is_empty()
        {
            body.insert("location".to_string(), Value::String(loc.clone()));
        }
        if let Some(max_bytes) = options.maximum_bytes_billed {
            body.insert(
                "maximumBytesBilled".to_string(),
                Value::String(max_bytes.to_string()),
            );
        } else if let Some(ref max_bytes_str) = self.max_bytes_billed_str {
            body.insert(
                "maximumBytesBilled".to_string(),
                Value::String(max_bytes_str.clone()),
            );
        }
        if let Some(timeout_ms) = options.job_timeout_ms {
            body.insert(
                "timeoutMs".to_string(),
                Value::String(timeout_ms.to_string()),
            );
        } else if let Some(ref timeout_str) = self.job_timeout_ms_str {
            body.insert("timeoutMs".to_string(), Value::String(timeout_str.clone()));
        }
        if let Some(max_res) = options.max_results {
            body.insert("maxResults".to_string(), json!(max_res));
        }

        if options.labels.is_empty() {
            if let Some(ref lbls) = self.config_labels_json {
                body.insert("labels".to_string(), lbls.clone());
            }
        } else {
            let mut merged_labels = self.config.labels.clone();
            for (k, v) in &options.labels {
                merged_labels.insert(k.clone(), v.clone());
            }
            body.insert("labels".to_string(), json!(merged_labels));
        }

        if options.create_session {
            body.insert("createSession".to_string(), Value::Bool(true));
        }
        if let Some(ref sid) = options.session_id
            && !sid.is_empty()
        {
            body.insert(
                "connectionProperties".to_string(),
                json!([{ "key": "session_id", "value": sid }]),
            );
        }

        if !params.is_empty() {
            let mode = if params.iter().any(|p| p.name.is_some()) {
                "NAMED"
            } else {
                "POSITIONAL"
            };
            body.insert("parameterMode".to_string(), Value::String(mode.to_string()));
            body.insert(
                "queryParameters".to_string(),
                Value::Array(
                    params
                        .iter()
                        .map(BigQueryQueryParameter::to_rest_json)
                        .collect(),
                ),
            );
        }

        Value::Object(body)
    }

    fn build_jobs_insert_payload(
        &self,
        sql: &str,
        params: &[BigQueryQueryParameter],
        options: &BigQueryQueryOptions,
    ) -> Value {
        let mut query_cfg = Map::new();
        query_cfg.insert("query".to_string(), Value::String(sql.to_string()));
        query_cfg.insert("useLegacySql".to_string(), Value::Bool(false));

        if let Some(ref ds_json) = self.default_dataset_json {
            query_cfg.insert("defaultDataset".to_string(), ds_json.clone());
        }
        if let Some(max_bytes) = options.maximum_bytes_billed {
            query_cfg.insert(
                "maximumBytesBilled".to_string(),
                Value::String(max_bytes.to_string()),
            );
        } else if let Some(ref max_bytes_str) = self.max_bytes_billed_str {
            query_cfg.insert(
                "maximumBytesBilled".to_string(),
                Value::String(max_bytes_str.clone()),
            );
        }
        if let Some(prio) = options.priority.as_ref().or(self.config.priority.as_ref()) {
            query_cfg.insert("priority".to_string(), Value::String(prio.clone()));
        }
        if options.create_session {
            query_cfg.insert("createSession".to_string(), Value::Bool(true));
        }
        if let Some(ref sid) = options.session_id
            && !sid.is_empty()
        {
            query_cfg.insert(
                "connectionProperties".to_string(),
                json!([{ "key": "session_id", "value": sid }]),
            );
        }
        if let Some(ref dest) = options.destination_table
            && !dest.is_empty()
        {
            let parts = split_bigquery_table_parts(dest);
            let dest_json = match parts.as_slice() {
                [p, d, t] => json!({ "projectId": p, "datasetId": d, "tableId": t }),
                [d, t] => {
                    json!({ "projectId": self.config.project_id, "datasetId": d, "tableId": t })
                }
                _ => json!({
                    "projectId": self.config.project_id,
                    "datasetId": self.config.dataset_id.as_deref().unwrap_or_default(),
                    "tableId": dest.trim().replace('`', ""),
                }),
            };
            query_cfg.insert("destinationTable".to_string(), dest_json);
            if let Some(ref wd) = options.write_disposition
                && !wd.is_empty()
            {
                query_cfg.insert("writeDisposition".to_string(), Value::String(wd.clone()));
            }
        }
        if !params.is_empty() {
            let mode = if params.iter().any(|p| p.name.is_some()) {
                "NAMED"
            } else {
                "POSITIONAL"
            };
            query_cfg.insert("parameterMode".to_string(), Value::String(mode.to_string()));
            query_cfg.insert(
                "queryParameters".to_string(),
                Value::Array(
                    params
                        .iter()
                        .map(BigQueryQueryParameter::to_rest_json)
                        .collect(),
                ),
            );
        }

        let mut configuration = Map::new();
        configuration.insert("query".to_string(), Value::Object(query_cfg));
        if options.dry_run {
            configuration.insert("dryRun".to_string(), Value::Bool(true));
        }
        if let Some(timeout_ms) = options.job_timeout_ms {
            configuration.insert(
                "jobTimeoutMs".to_string(),
                Value::String(timeout_ms.to_string()),
            );
        } else if let Some(ref timeout_str) = self.job_timeout_ms_str {
            configuration.insert(
                "jobTimeoutMs".to_string(),
                Value::String(timeout_str.clone()),
            );
        }
        if options.labels.is_empty() {
            if let Some(ref lbls) = self.config_labels_json {
                configuration.insert("labels".to_string(), lbls.clone());
            }
        } else {
            let mut merged_labels = self.config.labels.clone();
            for (k, v) in &options.labels {
                merged_labels.insert(k.clone(), v.clone());
            }
            configuration.insert("labels".to_string(), json!(merged_labels));
        }

        let mut job_ref = Map::new();
        job_ref.insert(
            "projectId".to_string(),
            Value::String(self.config.project_id.clone()),
        );
        job_ref.insert("jobId".to_string(), Value::String(uuid4_str()));
        if let Some(ref loc) = self.config.location
            && !loc.is_empty()
        {
            job_ref.insert("location".to_string(), Value::String(loc.clone()));
        }

        json!({
            "jobReference": Value::Object(job_ref),
            "configuration": Value::Object(configuration),
        })
    }

    async fn send_rest_json(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<&Value>,
    ) -> Result<Value, BigQueryError> {
        let mut snapshot = None;
        if let Some(ref cache) = self.token_cache {
            snapshot = Some(cache.get_or_refresh().await.map_err(BigQueryError::Auth)?);
        }

        let mut resp = self
            .dispatch_http_once(method.clone(), url, body, snapshot.as_deref())
            .await?;

        if resp.status() == reqwest::StatusCode::UNAUTHORIZED
            && let (Some(cache), Some(snap)) = (&self.token_cache, &snapshot)
        {
            cache.invalidate(snap.generation);
            let refreshed = cache.get_or_refresh().await.map_err(BigQueryError::Auth)?;
            resp = self
                .dispatch_http_once(method, url, body, Some(&refreshed))
                .await?;
        }

        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| BigQueryError::Http(e.to_string()))?;

        if !status.is_success() {
            let parsed = serde_json::from_str::<Value>(&text).ok();
            let err_obj = parsed.as_ref().and_then(|v| v.get("error"));
            let message = err_obj
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| text.clone());
            let (canonical_status, reason) =
                err_obj.map(rest_error_details).unwrap_or((None, None));
            return Err(BigQueryError::Api {
                status: status.as_u16(),
                message,
                reason,
                canonical_status,
            });
        }

        if text.trim().is_empty() {
            return Ok(json!({}));
        }
        serde_json::from_str::<Value>(&text)
            .map_err(|e| BigQueryError::Http(format!("invalid JSON from BigQuery REST API: {e}")))
    }

    async fn dispatch_http_once(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<&Value>,
        snapshot: Option<&vertebra_core::gcp_auth::GcpTokenSnapshot>,
    ) -> Result<reqwest::Response, BigQueryError> {
        let mut req = self.http_client.request(method, url);
        if let Some(ref routing_hdr) = self.routing_header {
            req = req.header("x-goog-request-params", routing_hdr.clone());
        }
        if let Some(snap) = snapshot {
            req = req.header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", snap.access_token),
            );
            if let Some(ref qp) = snap.quota_project_id {
                req = req.header("x-goog-user-project", qp.as_str());
            } else if let Some(ref qp_hdr) = self.quota_project_header {
                req = req.header("x-goog-user-project", qp_hdr.clone());
            }
        } else if let Some(ref qp_hdr) = self.quota_project_header {
            req = req.header("x-goog-user-project", qp_hdr.clone());
        }
        if let Some(json_body) = body {
            req = req.json(json_body);
        }
        req.send()
            .await
            .map_err(|e| BigQueryError::Http(e.to_string()))
    }
}

fn map_writer_builder_error(err: WriterBuilderError) -> BigQueryError {
    BigQueryError::AppendVerification(err.to_string())
}

fn map_append_error(err: AppendError) -> BigQueryError {
    match err {
        AppendError::RowErrors { row_errors, .. } => BigQueryError::RowErrors(
            row_errors
                .into_iter()
                .map(|e| BigQueryRowError {
                    index: e.index,
                    code: e.code.value().unwrap_or(0),
                    message: e.message,
                })
                .collect(),
        ),
        AppendError::Rpc { source, .. } => BigQueryError::Client(source),
        other => BigQueryError::AppendVerification(other.to_string()),
    }
}

fn map_commit_error(err: CommitError) -> BigQueryError {
    match err {
        CommitError::Rpc { source, .. } => BigQueryError::Client(source),
        CommitError::FailedTransaction { stream_errors, .. } => {
            if let Some(first) = stream_errors.first() {
                BigQueryError::AppendVerification(format!(
                    "BatchCommitWriteStreams rejected stream {}: {}",
                    first.entity, first.error_message
                ))
            } else {
                BigQueryError::AppendVerification(
                    "BatchCommitWriteStreams rejected transaction".to_string(),
                )
            }
        }
        other => BigQueryError::AppendVerification(other.to_string()),
    }
}
