//! Single-`Sync` PostgreSQL Protocol 3.0 extended pipeline batching, duplex read-while-writing
//! submission, and ordered `Result` / `Error` / `Skipped` outcome demultiplexing.

use std::collections::{HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::codecs::{
    CodecContext, TypeCatalog, decode_value_with_catalog, format_for_oid_with_catalog,
};
use crate::errors::{PostgresError, ServerError};
use crate::extended::{Parameter, PreparedDescription, encode_bind, parse_parameter_description};
use crate::metadata::resolve_parameter_metadata_from_catalog;
use crate::pipeline_guard::validate_pipeline_commands;
use crate::protocol::MAX_BACKEND_FRAME_LENGTH;
use crate::query::{parse_command_tag_affected_rows, parse_error_response, parse_row_description};
use crate::result::{Column, Format, PgValue, QueryResult, TransactionStatus};
use crate::session::statements::{self, StatementKey};
use crate::session::{ExchangeState, PostgresConnection};

/// Default maximum operations permitted in one single-`Sync` [`PipelineBatch`] (`4,096`).
pub const DEFAULT_MAX_PIPELINE_OPERATIONS: usize = 4096;
/// Default maximum cumulative SQL byte size across a [`PipelineBatch`] (`16 MiB`).
pub const DEFAULT_MAX_PIPELINE_SQL_BYTES: usize = 16 * 1024 * 1024;
/// Default maximum cumulative serialized parameter payload across a [`PipelineBatch`] (`64 MiB`).
pub const DEFAULT_MAX_PIPELINE_PARAMETER_BYTES: usize = 64 * 1024 * 1024;
/// Default maximum buffered inbound result bytes across a [`PipelineBatch`] (`256 MiB`).
pub const DEFAULT_MAX_PIPELINE_RESULT_BYTES: usize = 268_435_456;

const OUTBOUND_WRITE_CHUNK_BYTES: usize = 64 * 1024;

/// Resource limits enforced before and during a single-`Sync` [`PipelineBatch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipelineLimits {
    /// Maximum number of extended operations allowed in a single batch.
    pub max_operations: usize,
    /// Maximum cumulative UTF-8 byte length across all SQL strings in the batch.
    pub max_sql_bytes: usize,
    /// Maximum cumulative encoded parameter wire bytes across the batch.
    pub max_parameter_bytes: usize,
    /// Maximum cumulative inbound result frame bytes buffered for the batch.
    pub max_result_bytes: usize,
}

impl Default for PipelineLimits {
    fn default() -> Self {
        Self {
            max_operations: DEFAULT_MAX_PIPELINE_OPERATIONS,
            max_sql_bytes: DEFAULT_MAX_PIPELINE_SQL_BYTES,
            max_parameter_bytes: DEFAULT_MAX_PIPELINE_PARAMETER_BYTES,
            max_result_bytes: DEFAULT_MAX_PIPELINE_RESULT_BYTES,
        }
    }
}

/// Single extended-query operation queued inside a [`PipelineBatch`].
#[derive(Clone, Debug, PartialEq)]
pub struct PipelineOperation {
    /// Single-statement PostgreSQL SQL text.
    pub sql: String,
    /// Ordered bound parameter values.
    pub parameters: Vec<Parameter>,
    /// Optional parameter OID hint vector (`0` requests inference).
    pub parameter_oids: Option<Vec<u32>>,
}

impl PipelineOperation {
    /// Construct a parameterless [`PipelineOperation`].
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            parameters: Vec::new(),
            parameter_oids: None,
        }
    }

    /// Construct a [`PipelineOperation`] with ordered [`Parameter`] values.
    pub fn with_parameters(sql: impl Into<String>, parameters: Vec<Parameter>) -> Self {
        Self {
            sql: sql.into(),
            parameters,
            parameter_oids: None,
        }
    }

    /// Construct a [`PipelineOperation`] with ordered [`Parameter`] values and explicit `parameter_oids`.
    pub fn with_parameter_oids(
        sql: impl Into<String>,
        parameters: Vec<Parameter>,
        parameter_oids: Vec<u32>,
    ) -> Self {
        Self {
            sql: sql.into(),
            parameters,
            parameter_oids: Some(parameter_oids),
        }
    }
}

/// Single-`Sync` batch of extended-query operations and bounded resource limits.
#[derive(Clone, Debug, PartialEq)]
pub struct PipelineBatch {
    /// Ordered operations submitted before a single trailing `Sync`.
    pub operations: Vec<PipelineOperation>,
    /// Resource limits governing outbound encoding and inbound buffering.
    pub limits: PipelineLimits,
}

impl PipelineBatch {
    /// Construct a [`PipelineBatch`] with [`PipelineLimits::default`].
    pub fn new(operations: Vec<PipelineOperation>) -> Self {
        Self {
            operations,
            limits: PipelineLimits::default(),
        }
    }

    /// Construct a [`PipelineBatch`] with explicit [`PipelineLimits`].
    pub fn with_limits(operations: Vec<PipelineOperation>, limits: PipelineLimits) -> Self {
        Self { operations, limits }
    }
}

/// Ordered per-operation outcome from a single-`Sync` pipeline exchange.
#[derive(Clone, Debug, PartialEq)]
pub enum PipelineOutcome {
    /// Operation completed with `CommandComplete` or `EmptyQueryResponse`.
    Result(QueryResult),
    /// Server aborted the batch at this operation with `ErrorResponse`.
    Error(ServerError),
    /// Server discarded this operation while skipping to `Sync` after `failed_index` failed.
    Skipped {
        /// Zero-based index of the earlier operation that emitted `ErrorResponse`.
        failed_index: usize,
    },
}

/// Structured failure returned when a pipeline fails preflight or encounters an error during execution.
#[derive(Clone, Debug, PartialEq)]
pub struct PipelineFailure {
    /// Canonical primary failure (`Server`, `Data`, `Interface`, `UnsupportedFeature`, `Transport`, or `Protocol`).
    pub first_error: PostgresError,
    /// Zero-based index of the first failing operation.
    pub failing_index: usize,
    /// Ordered outcomes recorded across the batch (`empty` when rejected before submission).
    pub outcomes: Vec<PipelineOutcome>,
    /// Final session [`TransactionStatus`] after draining through `ReadyForQuery`.
    pub transaction_status: TransactionStatus,
}

#[derive(Clone, Debug)]
pub(crate) struct PlannedPipelineOp {
    pub sql: String,
    pub expected_param_count: usize,
    pub requested_result_formats: Vec<Format>,
    pub expect_parse: bool,
    pub expect_stmt_describe: bool,
    pub expect_portal_describe: bool,
    pub cache_candidate: Option<(String, StatementKey)>,
}

/// Outbound plan and prefetched inbound backend frames captured during [`PostgresConnection::submit_pipeline`].
#[derive(Debug)]
pub struct SubmittedPipeline {
    pub(crate) ops: Vec<PlannedPipelineOp>,
    pub(crate) evicted_close_count: usize,
    pub(crate) limits: PipelineLimits,
    pub(crate) catalog_snapshot: Arc<TypeCatalog>,
    pub(crate) prefetched_frames: VecDeque<BytesMut>,
    pub(crate) prefetched_bytes: usize,
    pub(crate) used_unnamed_stmt: bool,
}

impl SubmittedPipeline {
    /// Return the number of submitted extended operations in this batch.
    pub fn operation_count(&self) -> usize {
        self.ops.len()
    }

    /// Return the number of complete backend frames already buffered under duplex backpressure during write.
    pub fn prefetched_frame_count(&self) -> usize {
        self.prefetched_frames.len()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpDemuxPhase {
    ParseComplete,
    ParamDesc,
    StmtDesc,
    BindComplete,
    PortalDesc,
    RowsOrComplete,
}

type PendingCacheInsert = (Option<(String, StatementKey)>, String, PreparedDescription);

impl OpDemuxPhase {
    const fn initial(plan: &PlannedPipelineOp) -> Self {
        if plan.expect_parse {
            Self::ParseComplete
        } else if plan.expect_stmt_describe {
            Self::ParamDesc
        } else {
            Self::BindComplete
        }
    }
}

fn extract_complete_frames(
    read_buf: &mut BytesMut,
    queue: &mut VecDeque<BytesMut>,
    buffered_bytes: &mut usize,
    max_result_bytes: usize,
) -> Result<(), PostgresError> {
    while read_buf.len() >= 5 {
        let raw_len =
            u32::from_be_bytes([read_buf[1], read_buf[2], read_buf[3], read_buf[4]]) as usize;
        if !(4..=MAX_BACKEND_FRAME_LENGTH).contains(&raw_len) {
            return Err(PostgresError::Protocol(format!(
                "backend frame length {raw_len} is outside valid bounds 4..={MAX_BACKEND_FRAME_LENGTH}"
            )));
        }
        let frame_total = 1 + raw_len;
        if buffered_bytes.saturating_add(frame_total) > max_result_bytes {
            return Err(PostgresError::Data(format!(
                "pipeline inbound response exceeded max_result_bytes ({max_result_bytes})"
            )));
        }
        if read_buf.len() < frame_total {
            break;
        }
        let frame = read_buf.split_to(frame_total);
        *buffered_bytes += frame_total;
        queue.push_back(frame);
    }
    Ok(())
}

async fn drive_duplex_write_and_single_flush<S>(
    stream: &mut S,
    outbound: &[u8],
    read_buf: &mut BytesMut,
    prefetched: &mut VecDeque<BytesMut>,
    prefetched_bytes: &mut usize,
    max_result_bytes: usize,
) -> Result<(), PostgresError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    extract_complete_frames(read_buf, prefetched, prefetched_bytes, max_result_bytes)?;

    let mut written = 0usize;
    let mut flushed = false;
    let mut scratch = vec![0u8; 16 * 1024];
    let mut inbound_eof = false;

    std::future::poll_fn(|cx: &mut Context<'_>| -> Poll<Result<(), PostgresError>> {
        loop {
            let mut made_progress = false;

            while written < outbound.len() {
                let end = (written + OUTBOUND_WRITE_CHUNK_BYTES).min(outbound.len());
                match Pin::new(&mut *stream).poll_write(cx, &outbound[written..end]) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(PostgresError::Transport(
                            "connection closed by peer during pipeline batch write".to_string(),
                        )));
                    }
                    Poll::Ready(Ok(n)) => {
                        written += n;
                        made_progress = true;
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Err(PostgresError::Transport(format!(
                            "pipeline socket write failed: {err}"
                        ))));
                    }
                    Poll::Pending => break,
                }
            }

            if written == outbound.len() && !flushed {
                match Pin::new(&mut *stream).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {
                        flushed = true;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Err(PostgresError::Transport(format!(
                            "pipeline socket flush failed: {err}"
                        ))));
                    }
                    Poll::Pending => {}
                }
            }

            if !inbound_eof {
                loop {
                    let mut rbuf = ReadBuf::new(&mut scratch);
                    match Pin::new(&mut *stream).poll_read(cx, &mut rbuf) {
                        Poll::Ready(Ok(())) => {
                            let filled = rbuf.filled();
                            if filled.is_empty() {
                                inbound_eof = true;
                                break;
                            }
                            if read_buf
                                .len()
                                .saturating_add(filled.len())
                                .saturating_add(*prefetched_bytes)
                                > max_result_bytes.saturating_add(MAX_BACKEND_FRAME_LENGTH)
                            {
                                return Poll::Ready(Err(PostgresError::Data(format!(
                                    "pipeline inbound buffer exceeded max_result_bytes ({max_result_bytes})"
                                ))));
                            }
                            read_buf.extend_from_slice(filled);
                            if let Err(err) = extract_complete_frames(
                                read_buf,
                                prefetched,
                                prefetched_bytes,
                                max_result_bytes,
                            ) {
                                return Poll::Ready(Err(err));
                            }
                            made_progress = true;
                        }
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(Err(PostgresError::Transport(format!(
                                "pipeline concurrent socket read failed: {err}"
                            ))));
                        }
                        Poll::Pending => break,
                    }
                }
            }

            if !made_progress {
                if inbound_eof && written < outbound.len() {
                    return Poll::Pending;
                }
                return Poll::Pending;
            }
        }
    })
    .await
}

fn validate_portal_columns(cols: &[Column], requested: &[Format]) -> Result<(), PostgresError> {
    if requested.is_empty() {
        return Ok(());
    }
    if requested.len() == 1 {
        let expected = requested[0];
        for col in cols {
            if col.format != expected {
                return Err(PostgresError::Protocol(format!(
                    "pipeline portal column `{}` format {:?} does not match requested uniform format {:?}",
                    col.name, col.format, expected
                )));
            }
        }
        return Ok(());
    }
    if requested.len() != cols.len() {
        return Err(PostgresError::Protocol(format!(
            "pipeline portal column count {} does not match requested result_formats count {}",
            cols.len(),
            requested.len()
        )));
    }
    for (col, &expected) in cols.iter().zip(requested.iter()) {
        if col.format != expected {
            return Err(PostgresError::Protocol(format!(
                "pipeline portal column `{}` format {:?} does not match requested format {:?}",
                col.name, col.format, expected
            )));
        }
    }
    Ok(())
}

fn is_ddl_invalidation_tag(tag: &str) -> bool {
    tag.starts_with("ALTER ")
        || tag.starts_with("DROP ")
        || tag.starts_with("CREATE ")
        || tag.eq_ignore_ascii_case("DISCARD ALL")
        || tag.starts_with("DEALLOCATE")
}

impl PostgresConnection {
    /// Validate the entire [`PipelineBatch`] before emitting a single byte, encode all `N` cold or
    /// warm operations followed by **one** `Sync`, and drive duplex writes + concurrent inbound reads
    /// finishing with **one** logical `AsyncWrite::poll_flush` completion.
    pub async fn submit_pipeline(
        &mut self,
        batch: &PipelineBatch,
    ) -> Result<SubmittedPipeline, PostgresError> {
        if self.closed {
            return Err(PostgresError::Interface(
                "cannot submit pipeline on a closed PostgresConnection".to_string(),
            ));
        }
        if self.exchange_state != ExchangeState::Idle {
            return Err(PostgresError::Interface(format!(
                "cannot submit pipeline while exchange_state is {:?}",
                self.exchange_state
            )));
        }

        if batch.operations.is_empty() {
            return Ok(SubmittedPipeline {
                ops: Vec::new(),
                evicted_close_count: 0,
                limits: batch.limits,
                catalog_snapshot: self.type_catalog.snapshot_for_batch(),
                prefetched_frames: VecDeque::new(),
                prefetched_bytes: 0,
                used_unnamed_stmt: false,
            });
        }

        validate_pipeline_commands(batch.operations.iter().map(|op| op.sql.as_str()))?;

        if batch.operations.len() > batch.limits.max_operations {
            return Err(PostgresError::PipelineValidation {
                source: Box::new(PostgresError::Interface(format!(
                    "pipeline operation count {} exceeds max_operations ({})",
                    batch.operations.len(),
                    batch.limits.max_operations
                ))),
                failing_index: batch.limits.max_operations,
            });
        }

        let catalog_snapshot = self.type_catalog.snapshot_for_batch();
        let codec_ctx = CodecContext::from_parameters(&self.parameters);
        let cache_enabled = self.statement_cache.capacity() > 0;
        let schema_gen = self.statement_cache.schema_generation();

        let mut total_sql_bytes = 0usize;
        let mut total_param_bytes = 0usize;
        let mut resolved_ops = Vec::with_capacity(batch.operations.len());

        for (idx, op) in batch.operations.iter().enumerate() {
            if op.sql.as_bytes().contains(&0) {
                return Err(PostgresError::PipelineValidation {
                    source: Box::new(PostgresError::Data(
                        "pipeline SQL string cannot contain embedded NUL byte".to_string(),
                    )),
                    failing_index: idx,
                });
            }
            total_sql_bytes = total_sql_bytes.saturating_add(op.sql.len());
            if total_sql_bytes > batch.limits.max_sql_bytes {
                return Err(PostgresError::PipelineValidation {
                    source: Box::new(PostgresError::Data(format!(
                        "pipeline cumulative SQL byte length {total_sql_bytes} exceeds max_sql_bytes ({})",
                        batch.limits.max_sql_bytes
                    ))),
                    failing_index: idx,
                });
            }
            let resolved = resolve_parameter_metadata_from_catalog(
                &catalog_snapshot,
                &op.parameters,
                op.parameter_oids.as_deref(),
            )
            .map_err(|err| PostgresError::PipelineValidation {
                source: Box::new(err),
                failing_index: idx,
            })?;
            resolved_ops.push(resolved);
        }

        let evicted = self.statement_cache.evicted_names().to_vec();
        let mut outbound = BytesMut::with_capacity(batch.operations.len().saturating_mul(128) + 16);
        for ev_name in &evicted {
            frontend::close(b'S', ev_name, &mut outbound).map_err(|e| {
                PostgresError::Protocol(format!("failed to encode pipeline Close(S): {e}"))
            })?;
        }

        let mut planned_ops = Vec::with_capacity(batch.operations.len());
        let mut batch_cold_keys: HashSet<StatementKey> = HashSet::new();
        let mut temp_stmt_seq = 0usize;
        let mut used_unnamed_stmt = false;

        for (idx, (op, resolved)) in batch.operations.iter().zip(resolved_ops.iter()).enumerate() {
            let key =
                StatementKey::new(op.sql.clone(), resolved.parameter_oids.clone(), schema_gen);

            let warm_named = if cache_enabled {
                self.statement_cache.get_by_key(&key)
            } else {
                None
            };
            let warm_unnamed_desc = if !cache_enabled {
                self.statement_descriptions
                    .get(&op.sql)
                    .filter(|d| d.parameter_oids.len() == resolved.parameters.len())
                    .cloned()
            } else {
                None
            };

            if let Some(cached) = warm_named {
                let effective_param_oids: Vec<u32> = resolved
                    .parameter_oids
                    .iter()
                    .enumerate()
                    .map(|(i, &oid)| {
                        if oid != 0 {
                            oid
                        } else {
                            cached
                                .description
                                .parameter_oids
                                .get(i)
                                .copied()
                                .unwrap_or(0)
                        }
                    })
                    .collect();
                let param_formats: Vec<Format> = effective_param_oids
                    .iter()
                    .map(|&oid| {
                        if oid != 0 {
                            format_for_oid_with_catalog(oid, &catalog_snapshot)
                        } else {
                            Format::Text
                        }
                    })
                    .collect();
                let result_formats: Vec<Format> = cached
                    .description
                    .result_columns
                    .iter()
                    .map(|col| format_for_oid_with_catalog(col.type_oid, &catalog_snapshot))
                    .collect();

                let before_bind = outbound.len();
                encode_bind(
                    "",
                    &cached.name,
                    &resolved.parameters,
                    &effective_param_oids,
                    &param_formats,
                    &result_formats,
                    &codec_ctx,
                    &catalog_snapshot,
                    &mut outbound,
                )
                .map_err(|err| PostgresError::PipelineValidation {
                    source: Box::new(err),
                    failing_index: idx,
                })?;
                let bind_bytes = outbound.len().saturating_sub(before_bind);
                total_param_bytes = total_param_bytes.saturating_add(bind_bytes);
                if total_param_bytes > batch.limits.max_parameter_bytes {
                    return Err(PostgresError::PipelineValidation {
                        source: Box::new(PostgresError::Data(format!(
                            "pipeline serialized parameter payload exceeded max_parameter_bytes ({})",
                            batch.limits.max_parameter_bytes
                        ))),
                        failing_index: idx,
                    });
                }
                frontend::describe(b'P', "", &mut outbound).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode pipeline Describe(P): {e}"))
                })?;
                frontend::execute("", 0, &mut outbound).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode pipeline Execute: {e}"))
                })?;

                planned_ops.push(PlannedPipelineOp {
                    sql: op.sql.clone(),
                    expected_param_count: resolved.parameters.len(),
                    requested_result_formats: result_formats,
                    expect_parse: false,
                    expect_stmt_describe: false,
                    expect_portal_describe: true,
                    cache_candidate: None,
                });
            } else if let Some(desc) = warm_unnamed_desc {
                let effective_param_oids: Vec<u32> = resolved
                    .parameter_oids
                    .iter()
                    .enumerate()
                    .map(|(i, &oid)| {
                        if oid != 0 {
                            oid
                        } else {
                            desc.parameter_oids.get(i).copied().unwrap_or(0)
                        }
                    })
                    .collect();
                let param_formats: Vec<Format> = effective_param_oids
                    .iter()
                    .map(|&oid| {
                        if oid != 0 {
                            format_for_oid_with_catalog(oid, &catalog_snapshot)
                        } else {
                            Format::Text
                        }
                    })
                    .collect();
                let result_formats: Vec<Format> = desc
                    .result_columns
                    .iter()
                    .map(|col| format_for_oid_with_catalog(col.type_oid, &catalog_snapshot))
                    .collect();

                used_unnamed_stmt = true;
                frontend::parse(
                    "",
                    &op.sql,
                    effective_param_oids.iter().copied(),
                    &mut outbound,
                )
                .map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode pipeline Parse: {e}"))
                })?;
                let before_bind = outbound.len();
                encode_bind(
                    "",
                    "",
                    &resolved.parameters,
                    &effective_param_oids,
                    &param_formats,
                    &result_formats,
                    &codec_ctx,
                    &catalog_snapshot,
                    &mut outbound,
                )
                .map_err(|err| PostgresError::PipelineValidation {
                    source: Box::new(err),
                    failing_index: idx,
                })?;
                let bind_bytes = outbound.len().saturating_sub(before_bind);
                total_param_bytes = total_param_bytes.saturating_add(bind_bytes);
                if total_param_bytes > batch.limits.max_parameter_bytes {
                    return Err(PostgresError::PipelineValidation {
                        source: Box::new(PostgresError::Data(format!(
                            "pipeline serialized parameter payload exceeded max_parameter_bytes ({})",
                            batch.limits.max_parameter_bytes
                        ))),
                        failing_index: idx,
                    });
                }
                frontend::describe(b'P', "", &mut outbound).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode pipeline Describe(P): {e}"))
                })?;
                frontend::execute("", 0, &mut outbound).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode pipeline Execute: {e}"))
                })?;

                planned_ops.push(PlannedPipelineOp {
                    sql: op.sql.clone(),
                    expected_param_count: resolved.parameters.len(),
                    requested_result_formats: result_formats,
                    expect_parse: true,
                    expect_stmt_describe: false,
                    expect_portal_describe: true,
                    cache_candidate: None,
                });
            } else {
                let (stmt_name, cache_candidate) =
                    if cache_enabled && batch_cold_keys.insert(key.clone()) {
                        temp_stmt_seq += 1;
                        let candidate_name = format!(
                            "vtb_ps_{}_{}",
                            self.statement_cache.peek_next_id_seed(),
                            temp_stmt_seq
                        );
                        (candidate_name.clone(), Some((candidate_name, key)))
                    } else {
                        used_unnamed_stmt = true;
                        (String::new(), None)
                    };

                let param_formats = vec![Format::Text; resolved.parameters.len()];
                let result_formats = vec![Format::Text];

                frontend::parse(
                    &stmt_name,
                    &op.sql,
                    resolved.parameter_oids.iter().copied(),
                    &mut outbound,
                )
                .map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode pipeline Parse: {e}"))
                })?;
                frontend::describe(b'S', &stmt_name, &mut outbound).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode pipeline Describe(S): {e}"))
                })?;

                let before_bind = outbound.len();
                encode_bind(
                    "",
                    &stmt_name,
                    &resolved.parameters,
                    &resolved.parameter_oids,
                    &param_formats,
                    &result_formats,
                    &codec_ctx,
                    &catalog_snapshot,
                    &mut outbound,
                )
                .map_err(|err| PostgresError::PipelineValidation {
                    source: Box::new(err),
                    failing_index: idx,
                })?;
                let bind_bytes = outbound.len().saturating_sub(before_bind);
                total_param_bytes = total_param_bytes.saturating_add(bind_bytes);
                if total_param_bytes > batch.limits.max_parameter_bytes {
                    return Err(PostgresError::PipelineValidation {
                        source: Box::new(PostgresError::Data(format!(
                            "pipeline serialized parameter payload exceeded max_parameter_bytes ({})",
                            batch.limits.max_parameter_bytes
                        ))),
                        failing_index: idx,
                    });
                }

                frontend::describe(b'P', "", &mut outbound).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode pipeline Describe(P): {e}"))
                })?;
                frontend::execute("", 0, &mut outbound).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode pipeline Execute: {e}"))
                })?;

                planned_ops.push(PlannedPipelineOp {
                    sql: op.sql.clone(),
                    expected_param_count: resolved.parameters.len(),
                    requested_result_formats: result_formats,
                    expect_parse: true,
                    expect_stmt_describe: true,
                    expect_portal_describe: true,
                    cache_candidate,
                });
            }
        }

        frontend::sync(&mut outbound);

        let _ = self.statement_cache.take_evicted_names();
        if temp_stmt_seq > 0 {
            self.statement_cache.advance_id_seed(temp_stmt_seq as u64);
        }
        if used_unnamed_stmt {
            self.unnamed_statement_valid = true;
        }
        self.exchange_state = ExchangeState::PipelineActive;

        let mut prefetched_frames = VecDeque::new();
        let mut prefetched_bytes = 0usize;

        if let Err(err) = drive_duplex_write_and_single_flush(
            &mut self.stream,
            &outbound,
            &mut self.read_buf,
            &mut prefetched_frames,
            &mut prefetched_bytes,
            batch.limits.max_result_bytes,
        )
        .await
        {
            self.mark_closed();
            return Err(err);
        }

        Ok(SubmittedPipeline {
            ops: planned_ops,
            evicted_close_count: evicted.len(),
            limits: batch.limits,
            catalog_snapshot,
            prefetched_frames,
            prefetched_bytes,
            used_unnamed_stmt,
        })
    }

    /// Execute a single-`Sync` [`PipelineBatch`] and demultiplex ordered `Result`, `Error`, and
    /// `Skipped` outcomes drained boundedly through trailing `ReadyForQuery` (`'I'` / `'T'` / `'E'`).
    #[allow(clippy::result_large_err)]
    pub async fn pipeline_results(
        &mut self,
        batch: &PipelineBatch,
    ) -> Result<Vec<PipelineOutcome>, PipelineFailure> {
        let submitted = match self.submit_pipeline(batch).await {
            Ok(sub) => sub,
            Err(PostgresError::PipelineValidation {
                source,
                failing_index,
            }) => {
                return Err(PipelineFailure {
                    first_error: *source,
                    failing_index,
                    outcomes: Vec::new(),
                    transaction_status: self.transaction_status,
                });
            }
            Err(err) => {
                return Err(PipelineFailure {
                    first_error: err,
                    failing_index: 0,
                    outcomes: Vec::new(),
                    transaction_status: self.transaction_status,
                });
            }
        };
        self.collect_submitted_pipeline(submitted).await
    }

    /// Demultiplex an already-flushed [`SubmittedPipeline`] through trailing `ReadyForQuery`.
    #[allow(clippy::result_large_err)]
    pub async fn collect_submitted_pipeline(
        &mut self,
        mut submitted: SubmittedPipeline,
    ) -> Result<Vec<PipelineOutcome>, PipelineFailure> {
        if submitted.ops.is_empty() {
            return Ok(Vec::new());
        }

        let total_ops = submitted.ops.len();
        let mut outcomes: Vec<PipelineOutcome> = Vec::with_capacity(total_ops);
        let mut first_failure: Option<(usize, PostgresError)> = None;
        let mut server_aborted = false;
        let mut remaining_evicted_closes = submitted.evicted_close_count;
        let mut cumulative_bytes = submitted.prefetched_bytes;
        let max_result_bytes = submitted.limits.max_result_bytes;

        let mut op_idx = 0usize;
        let mut phase = OpDemuxPhase::initial(&submitted.ops[0]);
        let mut stmt_param_oids: Vec<u32> = Vec::new();
        let mut stmt_columns: Vec<Column> = Vec::new();
        let mut portal_columns: Vec<Column> = Vec::new();
        let mut op_rows: Vec<Vec<PgValue>> = Vec::new();
        let mut op_ctx = Arc::new(CodecContext::from_parameters(&self.parameters));
        let mut op_had_decode_err = false;
        let mut saw_ddl = false;
        let mut completed_cache_inserts: Vec<PendingCacheInsert> = Vec::new();

        loop {
            let msg = match self
                .next_pipeline_message(
                    &mut submitted.prefetched_frames,
                    &mut cumulative_bytes,
                    max_result_bytes,
                )
                .await
            {
                Ok(m) => m,
                Err(err) => {
                    let is_limit = matches!(err, PostgresError::Data(_));
                    self.mark_closed();
                    let fail_idx = first_failure
                        .as_ref()
                        .map(|(i, _)| *i)
                        .unwrap_or_else(|| op_idx.min(total_ops.saturating_sub(1)));
                    let primary = first_failure.map(|(_, e)| e).unwrap_or(err);
                    let _ = is_limit;
                    return Err(PipelineFailure {
                        first_error: primary,
                        failing_index: fail_idx,
                        outcomes,
                        transaction_status: self.transaction_status,
                    });
                }
            };

            match msg {
                Message::NoticeResponse(_) => continue,
                Message::ParameterStatus(body) => {
                    if let Err(err) = self.handle_parameter_status_frame(&body) {
                        let (fi, pe) =
                            first_failure.unwrap_or((op_idx.min(total_ops.saturating_sub(1)), err));
                        return Err(PipelineFailure {
                            first_error: pe,
                            failing_index: fi,
                            outcomes,
                            transaction_status: self.transaction_status,
                        });
                    }
                    op_ctx = Arc::new(CodecContext::from_parameters(&self.parameters));
                    continue;
                }
                Message::NotificationResponse(body) => {
                    if let Err(err) = self.handle_notification_frame(&body) {
                        let (fi, pe) =
                            first_failure.unwrap_or((op_idx.min(total_ops.saturating_sub(1)), err));
                        return Err(PipelineFailure {
                            first_error: pe,
                            failing_index: fi,
                            outcomes,
                            transaction_status: self.transaction_status,
                        });
                    }
                    continue;
                }
                Message::CloseComplete if remaining_evicted_closes > 0 && !server_aborted => {
                    remaining_evicted_closes -= 1;
                    continue;
                }
                Message::ReadyForQuery(body) => {
                    self.exchange_state = ExchangeState::Idle;
                    let status = match TransactionStatus::from_wire_tag(body.status()) {
                        Ok(s) => s,
                        Err(err) => {
                            self.mark_closed();
                            let (fi, pe) = first_failure
                                .unwrap_or((op_idx.min(total_ops.saturating_sub(1)), err));
                            return Err(PipelineFailure {
                                first_error: pe,
                                failing_index: fi,
                                outcomes,
                                transaction_status: self.transaction_status,
                            });
                        }
                    };
                    self.set_transaction_status_from_wire(status);

                    if !server_aborted && op_idx < total_ops {
                        self.mark_closed();
                        let proto_err = PostgresError::Protocol(format!(
                            "received premature ReadyForQuery after {op_idx} of {total_ops} pipeline operations"
                        ));
                        let (fi, pe) = first_failure.unwrap_or((op_idx, proto_err));
                        return Err(PipelineFailure {
                            first_error: pe,
                            failing_index: fi,
                            outcomes,
                            transaction_status: status,
                        });
                    }

                    for out in &mut outcomes {
                        if let PipelineOutcome::Result(res) = out {
                            res.transaction_status = status;
                        }
                    }

                    if !saw_ddl && status != TransactionStatus::Failed && first_failure.is_none() {
                        for (candidate, sql, desc) in completed_cache_inserts {
                            self.statement_descriptions.insert(sql, desc.clone());
                            if let Some((stmt_name, key)) = candidate {
                                self.statement_cache.insert_named(
                                    stmt_name,
                                    key.sql,
                                    key.param_oids,
                                    desc,
                                );
                            }
                        }
                    } else if status == TransactionStatus::Failed && submitted.used_unnamed_stmt {
                        self.unnamed_statement_valid = false;
                    }

                    if let Some((failing_index, first_error)) = first_failure {
                        return Err(PipelineFailure {
                            first_error,
                            failing_index,
                            outcomes,
                            transaction_status: status,
                        });
                    }
                    return Ok(outcomes);
                }
                Message::ErrorResponse(body) => {
                    let server_err = match parse_error_response(&body) {
                        Ok(e) => e,
                        Err(err) => {
                            self.mark_closed();
                            let (fi, pe) = first_failure
                                .unwrap_or((op_idx.min(total_ops.saturating_sub(1)), err));
                            return Err(PipelineFailure {
                                first_error: pe,
                                failing_index: fi,
                                outcomes,
                                transaction_status: self.transaction_status,
                            });
                        }
                    };

                    if statements::is_statement_invalidation_sqlstate(&server_err.sqlstate) {
                        self.statement_cache.invalidate_all();
                        self.statement_descriptions.clear();
                    }

                    if !server_aborted {
                        server_aborted = true;
                        if op_idx < total_ops {
                            let failed_at = op_idx;
                            if first_failure.is_none() {
                                first_failure =
                                    Some((failed_at, PostgresError::Server(server_err.clone())));
                            }
                            outcomes.push(PipelineOutcome::Error(server_err));
                            for _ in (failed_at + 1)..total_ops {
                                outcomes.push(PipelineOutcome::Skipped {
                                    failed_index: failed_at,
                                });
                            }
                            op_idx = total_ops;
                        } else if first_failure.is_none() {
                            first_failure = Some((
                                total_ops.saturating_sub(1),
                                PostgresError::Server(server_err),
                            ));
                        }
                    }
                    continue;
                }
                other if server_aborted => {
                    self.mark_closed();
                    let proto_err = PostgresError::Protocol(format!(
                        "unexpected backend frame after pipeline ErrorResponse before ReadyForQuery: {}",
                        frame_label(&other)
                    ));
                    let (fi, pe) = first_failure.unwrap_or((0, proto_err));
                    return Err(PipelineFailure {
                        first_error: pe,
                        failing_index: fi,
                        outcomes,
                        transaction_status: self.transaction_status,
                    });
                }
                other if op_idx >= total_ops => {
                    if matches!(other, Message::CloseComplete) {
                        continue;
                    }
                    self.mark_closed();
                    let proto_err = PostgresError::Protocol(format!(
                        "unexpected extra backend frame after all {total_ops} pipeline operations completed: {}",
                        frame_label(&other)
                    ));
                    let (fi, pe) = first_failure.unwrap_or((total_ops - 1, proto_err));
                    return Err(PipelineFailure {
                        first_error: pe,
                        failing_index: fi,
                        outcomes,
                        transaction_status: self.transaction_status,
                    });
                }
                other => {
                    let plan = &submitted.ops[op_idx];
                    let step_res: Result<Option<(String, u64)>, PostgresError> = match (phase, other)
                    {
                        (OpDemuxPhase::ParseComplete, Message::ParseComplete) => {
                            phase = if plan.expect_stmt_describe {
                                OpDemuxPhase::ParamDesc
                            } else {
                                OpDemuxPhase::BindComplete
                            };
                            Ok(None)
                        }
                        (OpDemuxPhase::ParamDesc, Message::ParameterDescription(body)) => {
                            parse_parameter_description(&body).and_then(|oids| {
                                if oids.len() != plan.expected_param_count {
                                    return Err(PostgresError::Protocol(format!(
                                        "pipeline op {op_idx} ParameterDescription count {} != bound count {}",
                                        oids.len(),
                                        plan.expected_param_count
                                    )));
                                }
                                stmt_param_oids = oids;
                                phase = OpDemuxPhase::StmtDesc;
                                Ok(None)
                            })
                        }
                        (OpDemuxPhase::StmtDesc, Message::RowDescription(body)) => {
                            parse_row_description(&body).and_then(|cols| {
                                stmt_columns = cols.clone();
                                if !plan.expect_portal_describe {
                                    validate_portal_columns(&cols, &plan.requested_result_formats)?;
                                    portal_columns = cols;
                                }
                                phase = OpDemuxPhase::BindComplete;
                                Ok(None)
                            })
                        }
                        (OpDemuxPhase::StmtDesc, Message::NoData) => {
                            stmt_columns.clear();
                            phase = OpDemuxPhase::BindComplete;
                            Ok(None)
                        }
                        (OpDemuxPhase::BindComplete, Message::BindComplete) => {
                            op_ctx = Arc::new(CodecContext::from_parameters(&self.parameters));
                            phase = if plan.expect_portal_describe {
                                OpDemuxPhase::PortalDesc
                            } else {
                                OpDemuxPhase::RowsOrComplete
                            };
                            Ok(None)
                        }
                        (OpDemuxPhase::PortalDesc, Message::RowDescription(body)) => {
                            op_ctx = Arc::new(CodecContext::from_parameters(&self.parameters));
                            parse_row_description(&body).and_then(|cols| {
                                validate_portal_columns(&cols, &plan.requested_result_formats)?;
                                portal_columns = cols;
                                phase = OpDemuxPhase::RowsOrComplete;
                                Ok(None)
                            })
                        }
                        (OpDemuxPhase::PortalDesc, Message::NoData) => {
                            portal_columns.clear();
                            phase = OpDemuxPhase::RowsOrComplete;
                            Ok(None)
                        }
                        (OpDemuxPhase::RowsOrComplete, Message::DataRow(body)) => {
                            if portal_columns.is_empty() {
                                Err(PostgresError::Protocol(format!(
                                    "pipeline op {op_idx} received DataRow after NoData / empty RowDescription"
                                )))
                            } else {
                                decode_pipeline_row(
                                    &body,
                                    &portal_columns,
                                    &op_ctx,
                                    &submitted.catalog_snapshot,
                                    &mut op_rows,
                                    &mut op_had_decode_err,
                                    op_idx,
                                    &mut first_failure,
                                )
                                .map(|()| None)
                            }
                        }
                        (OpDemuxPhase::RowsOrComplete, Message::CommandComplete(body)) => {
                            body.tag()
                                .map_err(|e| {
                                    PostgresError::Protocol(format!(
                                        "invalid pipeline CommandComplete UTF-8: {e}"
                                    ))
                                })
                                .and_then(|tag| {
                                    let affected = parse_command_tag_affected_rows(tag)?;
                                    Ok(Some((tag.to_owned(), affected)))
                                })
                        }
                        (OpDemuxPhase::RowsOrComplete, Message::EmptyQueryResponse) => {
                            if !op_rows.is_empty() {
                                Err(PostgresError::Protocol(format!(
                                    "pipeline op {op_idx} received EmptyQueryResponse after DataRow"
                                )))
                            } else {
                                Ok(Some((String::new(), 0)))
                            }
                        }
                        (_, Message::CloseComplete) => Ok(None),
                        (cur_phase, unexpected) => Err(PostgresError::Protocol(format!(
                            "unexpected frame {} during pipeline op {op_idx} phase {cur_phase:?}",
                            frame_label(&unexpected)
                        ))),
                    };

                    match step_res {
                        Ok(None) => {}
                        Ok(Some((tag, affected))) => {
                            if is_ddl_invalidation_tag(&tag) {
                                saw_ddl = true;
                                self.statement_descriptions.clear();
                                self.statement_cache.invalidate_all();
                            } else if plan.expect_stmt_describe && !op_had_decode_err {
                                let desc = PreparedDescription {
                                    parameter_oids: std::mem::take(&mut stmt_param_oids),
                                    result_columns: std::mem::take(&mut stmt_columns),
                                };
                                completed_cache_inserts.push((
                                    plan.cache_candidate.clone(),
                                    plan.sql.clone(),
                                    desc,
                                ));
                            } else {
                                stmt_param_oids.clear();
                                stmt_columns.clear();
                            }

                            let cols = std::mem::take(&mut portal_columns);
                            let rows = std::mem::take(&mut op_rows);
                            if !op_had_decode_err && first_failure.is_none() {
                                match QueryResult::try_new(
                                    cols,
                                    rows,
                                    affected,
                                    tag,
                                    self.transaction_status,
                                ) {
                                    Ok(qr) => outcomes.push(PipelineOutcome::Result(qr)),
                                    Err(err) => {
                                        first_failure = Some((op_idx, err));
                                    }
                                }
                            }

                            op_idx += 1;
                            op_had_decode_err = false;
                            if op_idx < total_ops {
                                phase = OpDemuxPhase::initial(&submitted.ops[op_idx]);
                            }
                        }
                        Err(err) => {
                            self.mark_closed();
                            let (fi, pe) = first_failure.unwrap_or((op_idx, err));
                            return Err(PipelineFailure {
                                first_error: pe,
                                failing_index: fi,
                                outcomes,
                                transaction_status: self.transaction_status,
                            });
                        }
                    }
                }
            }
        }
    }

    async fn next_pipeline_message(
        &mut self,
        prefetched: &mut VecDeque<BytesMut>,
        cumulative_bytes: &mut usize,
        max_result_bytes: usize,
    ) -> Result<Message, PostgresError> {
        if let Some(mut raw_frame) = prefetched.pop_front() {
            return Message::parse(&mut raw_frame)
                .map_err(|e| {
                    PostgresError::Protocol(format!("malformed prefetched pipeline frame: {e}"))
                })?
                .ok_or_else(|| {
                    PostgresError::Protocol(
                        "prefetched complete pipeline frame failed Message::parse".to_string(),
                    )
                });
        }
        let before = self.read_buf.len();
        let msg = crate::protocol::read_message(&mut self.stream, &mut self.read_buf, None).await?;
        let after = self.read_buf.len();
        let approx_frame = before.saturating_sub(after).max(5);
        *cumulative_bytes = cumulative_bytes.saturating_add(approx_frame);
        if *cumulative_bytes > max_result_bytes {
            return Err(PostgresError::Data(format!(
                "pipeline inbound response exceeded max_result_bytes ({max_result_bytes})"
            )));
        }
        Ok(msg)
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_pipeline_row(
    body: &postgres_protocol::message::backend::DataRowBody,
    cols: &[Column],
    ctx: &CodecContext,
    catalog: &TypeCatalog,
    op_rows: &mut Vec<Vec<PgValue>>,
    op_had_decode_err: &mut bool,
    op_idx: usize,
    first_failure: &mut Option<(usize, PostgresError)>,
) -> Result<(), PostgresError> {
    let buffer = body.buffer();
    let mut ranges = body.ranges();
    let mut row = Vec::with_capacity(cols.len());
    let mut col_idx = 0usize;

    while let Some(range_opt) = ranges
        .next()
        .map_err(|e| PostgresError::Protocol(format!("malformed pipeline DataRow range: {e}")))?
    {
        let Some(col) = cols.get(col_idx) else {
            return Err(PostgresError::Protocol(format!(
                "pipeline op {op_idx} DataRow contains more fields than RowDescription ({})",
                cols.len()
            )));
        };
        if !*op_had_decode_err {
            let raw_slice = range_opt.map(|r| &buffer[r]);
            match decode_value_with_catalog(col.type_oid, col.format, raw_slice, ctx, catalog) {
                Ok(val) => row.push(val),
                Err(err) => {
                    *op_had_decode_err = true;
                    op_rows.clear();
                    if first_failure.is_none() {
                        *first_failure = Some((op_idx, err));
                    }
                }
            }
        }
        col_idx += 1;
    }

    if col_idx != cols.len() {
        return Err(PostgresError::Protocol(format!(
            "pipeline op {op_idx} DataRow field count {col_idx} != RowDescription column count {}",
            cols.len()
        )));
    }

    if !*op_had_decode_err {
        op_rows.push(row);
    }
    Ok(())
}

const fn frame_label(msg: &Message) -> &'static str {
    match msg {
        Message::ParseComplete => "ParseComplete",
        Message::BindComplete => "BindComplete",
        Message::CloseComplete => "CloseComplete",
        Message::ParameterDescription(_) => "ParameterDescription",
        Message::RowDescription(_) => "RowDescription",
        Message::NoData => "NoData",
        Message::DataRow(_) => "DataRow",
        Message::PortalSuspended => "PortalSuspended",
        Message::CommandComplete(_) => "CommandComplete",
        Message::EmptyQueryResponse => "EmptyQueryResponse",
        Message::ErrorResponse(_) => "ErrorResponse",
        Message::ReadyForQuery(_) => "ReadyForQuery",
        Message::CopyInResponse(_) => "CopyInResponse",
        Message::CopyOutResponse(_) => "CopyOutResponse",
        Message::CopyData(_) => "CopyData",
        Message::CopyDone => "CopyDone",
        _ => "Other",
    }
}
