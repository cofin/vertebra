//! Cold-query pre-`Parse` parameter metadata resolution and post-`ReadyForQuery` owned text result normalization.

use std::sync::Arc;

use crate::codecs::{
    BOOL_OID, BYTEA_OID, CodecContext, DATE_OID, FLOAT4_OID, FLOAT8_OID, INT2_OID, INT4_OID,
    INT8_OID, INTERVAL_OID, JSONB_OID, NUMERIC_OID, RECORD_OID, TEXT_OID, TIME_OID, TIMESTAMP_OID,
    TIMESTAMPTZ_OID, TypeCatalog, TypeInfo, TypeKind, UUID_OID, VARBIT_OID,
    decode_value_with_catalog, format_for_oid_with_catalog,
};
use crate::errors::PostgresError;
use crate::extended::Parameter;
use crate::result::{Column, Format, PgValue, QueryResult, TransactionStatus, VectorKind};
use crate::session::{ExchangeState, PostgresConnection};

/// Input parameter descriptor before catalog OID resolution.
#[derive(Clone, Debug, PartialEq)]
pub struct ParameterCandidate {
    /// Parameter value.
    pub value: PgValue,
    /// Optional explicit positive PostgreSQL type OID.
    pub explicit_oid: Option<u32>,
}

/// Trait abstracting parameter inputs (`Parameter`, `ParameterCandidate`, `(PgValue, Option<u32>)`).
pub trait AsParameterInput {
    /// Borrow the underlying [`PgValue`].
    fn value_ref(&self) -> &PgValue;
    /// Optional explicit parameter type OID (`None` when unspecified).
    fn type_oid_opt(&self) -> Option<u32>;
}

impl AsParameterInput for Parameter {
    fn value_ref(&self) -> &PgValue {
        &self.value
    }

    fn type_oid_opt(&self) -> Option<u32> {
        self.type_oid
    }
}

impl AsParameterInput for ParameterCandidate {
    fn value_ref(&self) -> &PgValue {
        &self.value
    }

    fn type_oid_opt(&self) -> Option<u32> {
        self.explicit_oid
    }
}

impl AsParameterInput for (PgValue, Option<u32>) {
    fn value_ref(&self) -> &PgValue {
        &self.0
    }

    fn type_oid_opt(&self) -> Option<u32> {
        self.1
    }
}

/// Resolved parameter list and OIDs ready for extended `Parse` / `Bind` emission.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedParameters {
    /// Validated parameter descriptors with resolved OIDs where determinable.
    pub parameters: Vec<Parameter>,
    /// Parameter OID vector passed to `Parse` (`0` for server-inferred `NULL` or untyped binds).
    pub parameter_oids: Vec<u32>,
    /// `true` when every non-null parameter has a concrete OID with a known codec format.
    pub all_formats_known: bool,
}

/// Owned raw bytes of a single text-format cell captured during a cold extended query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedTextField {
    /// Raw field bytes (`None` for SQL `NULL`).
    pub bytes: Option<Vec<u8>>,
}

/// Owned cold-query text result awaiting post-`ReadyForQuery` custom OID enrichment.
#[derive(Clone, Debug, PartialEq)]
pub struct OwnedTextResult {
    /// Ordered column metadata captured from `RowDescription`.
    pub columns: Vec<Column>,
    /// Owned text-format cells per row.
    pub rows: Vec<Vec<OwnedTextField>>,
    /// Affected row count parsed from `CommandComplete`.
    pub affected_rows: u64,
    /// Server `CommandComplete` tag.
    pub command_tag: String,
    /// Final `ReadyForQuery` transaction state.
    pub transaction_status: TransactionStatus,
    /// Snapshot of session `CodecContext` captured when the rows arrived (immune to later catalog GUC drift).
    pub codec_context: Arc<CodecContext>,
    /// Gate confirming the original wire exchange finished `ReadyForQuery` before normalization runs.
    pub exchange_completed: bool,
}

impl OwnedTextResult {
    /// Construct a new uncompleted [`OwnedTextResult`].
    pub fn new(
        columns: Vec<Column>,
        rows: Vec<Vec<OwnedTextField>>,
        affected_rows: u64,
        command_tag: impl Into<String>,
        codec_context: Arc<CodecContext>,
    ) -> Self {
        Self {
            columns,
            rows,
            affected_rows,
            command_tag: command_tag.into(),
            transaction_status: TransactionStatus::Idle,
            codec_context,
            exchange_completed: false,
        }
    }

    /// Mark the wire exchange completed at `ReadyForQuery(status)`.
    pub fn mark_completed(&mut self, status: TransactionStatus) {
        self.transaction_status = status;
        self.exchange_completed = true;
    }
}

pub(crate) fn ensure_no_temporal_text(value: &PgValue) -> Result<(), PostgresError> {
    match value {
        PgValue::TemporalText { .. } => Err(PostgresError::Data(
            "unresolved TemporalText (wire_text) values are output-only and cannot be bound"
                .to_string(),
        )),
        PgValue::Array { values, .. } | PgValue::Composite { values, .. } => {
            for elem in values {
                ensure_no_temporal_text(elem)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_missing_value_metadata(
    value: &PgValue,
    explicit_oid: Option<u32>,
    catalog: &TypeCatalog,
    missing_oids: &mut Vec<u32>,
    missing_exts: &mut Vec<&'static str>,
) -> Result<(), PostgresError> {
    if let Some(oid) = explicit_oid
        && oid != 0
        && catalog.type_info(oid).is_none()
        && !missing_oids.contains(&oid)
    {
        missing_oids.push(oid);
    }

    match value {
        PgValue::Vector { kind, .. } => {
            let ext_name = match kind {
                VectorKind::Vector => "vector",
                VectorKind::HalfVec => "halfvec",
            };
            if catalog.lookup_extension_type(ext_name).is_none()
                && !missing_exts.contains(&ext_name)
            {
                missing_exts.push(ext_name);
            }
        }
        PgValue::Composite {
            type_oid,
            field_oids,
            values,
            ..
        } => {
            let eff_oid = explicit_oid.filter(|&o| o != 0).unwrap_or(*type_oid);
            if eff_oid == 0 || eff_oid == RECORD_OID {
                return Err(PostgresError::Data(
                    "cannot bind anonymous record (OID 2249) without a concrete composite OID"
                        .to_string(),
                ));
            }
            if catalog.type_info(eff_oid).is_none() && !missing_oids.contains(&eff_oid) {
                missing_oids.push(eff_oid);
            }
            for (&f_oid, f_val) in field_oids.iter().zip(values.iter()) {
                collect_missing_value_metadata(
                    f_val,
                    (f_oid != 0).then_some(f_oid),
                    catalog,
                    missing_oids,
                    missing_exts,
                )?;
            }
        }
        PgValue::Array {
            values,
            element_oid,
            ..
        } => {
            if *element_oid != 0
                && catalog.type_info(*element_oid).is_none()
                && !missing_oids.contains(element_oid)
            {
                missing_oids.push(*element_oid);
            }
            for elem in values {
                collect_missing_value_metadata(
                    elem,
                    (*element_oid != 0).then_some(*element_oid),
                    catalog,
                    missing_oids,
                    missing_exts,
                )?;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn run_pre_parse_discovery(
    conn: &mut PostgresConnection,
    missing_oids: &[u32],
    missing_exts: &[&str],
) -> Result<(), PostgresError> {
    match conn.transaction_status() {
        TransactionStatus::Idle => {
            let mut cat = conn.type_catalog().clone();
            cat.discover(conn, missing_oids, missing_exts).await
        }
        TransactionStatus::InTransaction => {
            conn.simple_query("SAVEPOINT _vtb_meta").await?;
            let mut cat = conn.type_catalog().clone();
            match cat.discover(conn, missing_oids, missing_exts).await {
                Ok(()) => {
                    conn.simple_query("RELEASE SAVEPOINT _vtb_meta").await?;
                    Ok(())
                }
                Err(err) => {
                    if conn
                        .simple_query(
                            "ROLLBACK TO SAVEPOINT _vtb_meta; RELEASE SAVEPOINT _vtb_meta",
                        )
                        .await
                        .is_err()
                    {
                        conn.closed = true;
                    }
                    Err(err)
                }
            }
        }
        TransactionStatus::Failed => Err(PostgresError::Interface(
            "cannot run catalog parameter discovery inside a failed transaction block".to_string(),
        )),
    }
}

fn infer_parameter_oid(
    value: &PgValue,
    explicit_oid: Option<u32>,
    catalog: &TypeCatalog,
) -> Result<u32, PostgresError> {
    if let Some(oid) = explicit_oid
        && oid != 0
    {
        match value {
            PgValue::Vector { kind, .. } => {
                let expected_ext = match kind {
                    VectorKind::Vector => "vector",
                    VectorKind::HalfVec => "halfvec",
                };
                let base_oid = catalog.resolve_domain_base(oid)?;
                let matches = matches!(
                    catalog.type_info(base_oid),
                    Some(TypeInfo {
                        kind: TypeKind::Extension { name, .. },
                        ..
                    }) if name == expected_ext
                );
                if !matches {
                    return Err(PostgresError::Data(format!(
                        "parameter {kind:?} does not match explicit target OID {oid}"
                    )));
                }
            }
            PgValue::Composite { .. } => {
                let base_oid = catalog.resolve_domain_base(oid)?;
                let is_comp = matches!(
                    catalog.type_info(base_oid),
                    Some(TypeInfo {
                        kind: TypeKind::Composite { .. },
                        ..
                    }) if base_oid != RECORD_OID
                );
                if !is_comp {
                    return Err(PostgresError::Data(format!(
                        "parameter Composite target OID {oid} is not a registered named composite in TypeCatalog"
                    )));
                }
            }
            _ => {}
        }
        return Ok(oid);
    }

    match value {
        PgValue::Null => Ok(0),
        PgValue::Bool(_) => Ok(BOOL_OID),
        PgValue::Int2(_) => Ok(INT2_OID),
        PgValue::Int4(_) => Ok(INT4_OID),
        PgValue::Int8(_) => Ok(INT8_OID),
        PgValue::Float4(_) => Ok(FLOAT4_OID),
        PgValue::Float8(_) => Ok(FLOAT8_OID),
        PgValue::Text(_) => Ok(TEXT_OID),
        PgValue::Bytes(_) => Ok(BYTEA_OID),
        PgValue::Uuid(_) => Ok(UUID_OID),
        PgValue::Numeric(_) => Ok(NUMERIC_OID),
        PgValue::Date(_) => Ok(DATE_OID),
        PgValue::Time(_) => Ok(TIME_OID),
        PgValue::Timestamp(_) => Ok(TIMESTAMP_OID),
        PgValue::TimestampTz(_) => Ok(TIMESTAMPTZ_OID),
        PgValue::TemporalText { .. } => Err(PostgresError::Data(
            "unresolved TemporalText (wire_text) values cannot be bound".to_string(),
        )),
        PgValue::Interval { .. } => Ok(INTERVAL_OID),
        PgValue::Json(_) => Ok(JSONB_OID),
        PgValue::BitString { .. } => Ok(VARBIT_OID),
        PgValue::Vector { kind, .. } => {
            let ext_name = match kind {
                VectorKind::Vector => "vector",
                VectorKind::HalfVec => "halfvec",
            };
            let Some(info) = catalog.lookup_extension_type(ext_name) else {
                return Err(PostgresError::Data(format!(
                    "pgvector extension type {ext_name:?} was not found in pg_catalog"
                )));
            };
            Ok(info.oid)
        }
        PgValue::Composite { type_oid, .. } => {
            if *type_oid == 0 || *type_oid == RECORD_OID {
                return Err(PostgresError::Data(
                    "cannot bind anonymous record (OID 2249) without a concrete composite OID"
                        .to_string(),
                ));
            }
            let base_oid = catalog.resolve_domain_base(*type_oid)?;
            let is_comp = matches!(
                catalog.type_info(base_oid),
                Some(TypeInfo {
                    kind: TypeKind::Composite { .. },
                    ..
                }) if base_oid != RECORD_OID
            );
            if !is_comp {
                return Err(PostgresError::Data(format!(
                    "composite type OID {type_oid} is not registered in TypeCatalog"
                )));
            }
            Ok(*type_oid)
        }
        PgValue::Array { element_oid, .. } => {
            if *element_oid == 0 {
                return Err(PostgresError::Data(
                    "PgArray parameter requires a positive element_oid".to_string(),
                ));
            }
            Ok(0)
        }
    }
}

/// Resolve parameter OIDs strictly from an in-memory [`TypeCatalog`] snapshot with zero wire I/O
/// (used by single-`Sync` cold and warm pipeline batches).
pub(crate) fn resolve_parameter_metadata_from_catalog<P: AsParameterInput>(
    catalog: &TypeCatalog,
    params: &[P],
    parameter_oids: Option<&[u32]>,
) -> Result<ResolvedParameters, PostgresError> {
    if params.len() > i16::MAX as usize {
        return Err(PostgresError::Data(format!(
            "parameter count {} exceeds PostgreSQL Protocol 3.0 limit ({})",
            params.len(),
            i16::MAX
        )));
    }
    if let Some(explicit) = parameter_oids
        && explicit.len() != params.len()
    {
        return Err(PostgresError::Data(format!(
            "parameter_oids length {} does not match parameters length {}",
            explicit.len(),
            params.len()
        )));
    }

    let mut resolved_params = Vec::with_capacity(params.len());
    let mut resolved_oids = Vec::with_capacity(params.len());
    let mut all_formats_known = true;

    for (idx, param) in params.iter().enumerate() {
        ensure_no_temporal_text(param.value_ref())?;
        let param_oid = param.type_oid_opt();
        if param_oid == Some(0) {
            return Err(PostgresError::Data(format!(
                "parameter {idx} has invalid explicit type_oid 0 (use None for server inference)"
            )));
        }
        let slice_oid = parameter_oids.map(|s| s[idx]).filter(|&o| o != 0);
        if let (Some(a), Some(b)) = (param_oid, slice_oid)
            && a != b
        {
            return Err(PostgresError::Data(format!(
                "parameter {idx} explicit type_oid {a} conflicts with parameter_oids[{idx}] == {b}"
            )));
        }
        let val = param.value_ref().clone();
        let merged = param_oid.or(slice_oid);
        let oid = infer_parameter_oid(&val, merged, catalog)?;
        if !matches!(val, PgValue::Null)
            && (oid == 0 || format_for_oid_with_catalog(oid, catalog) == Format::Text)
        {
            all_formats_known = false;
        }
        resolved_params.push(Parameter {
            value: val,
            type_oid: (oid != 0).then_some(oid),
        });
        resolved_oids.push(oid);
    }

    Ok(ResolvedParameters {
        parameters: resolved_params,
        parameter_oids: resolved_oids,
        all_formats_known,
    })
}

/// Synchronously resolve parameter OIDs when every parameter type is already known in `catalog`.
///
/// Returns `Ok(None)` if any parameter requires a pre-`Parse` catalog discovery round trip.
#[cfg(unix)]
pub(crate) fn try_resolve_parameter_metadata_sync(
    catalog: &TypeCatalog,
    params: &[Parameter],
) -> Result<Option<ResolvedParameters>, PostgresError> {
    if params.is_empty() {
        return Ok(Some(ResolvedParameters {
            parameters: Vec::new(),
            parameter_oids: Vec::new(),
            all_formats_known: true,
        }));
    }
    let mut missing_oids: Vec<u32> = Vec::new();
    let mut missing_exts: Vec<&'static str> = Vec::new();
    for param in params {
        collect_missing_value_metadata(
            &param.value,
            param.type_oid,
            catalog,
            &mut missing_oids,
            &mut missing_exts,
        )?;
    }
    if !missing_oids.is_empty() || !missing_exts.is_empty() {
        return Ok(None);
    }
    resolve_parameter_metadata_from_catalog(catalog, params, None).map(Some)
}

/// Resolve parameter OIDs and discover any required custom parameter metadata before `Parse` is emitted.
pub async fn resolve_parameter_metadata<P: AsParameterInput>(
    conn: &mut PostgresConnection,
    params: &[P],
    parameter_oids: Option<&[u32]>,
) -> Result<ResolvedParameters, PostgresError> {
    if conn.is_closed() {
        return Err(PostgresError::Interface(
            "cannot resolve parameter metadata on a closed PostgresConnection".to_string(),
        ));
    }
    if params.len() > i16::MAX as usize {
        return Err(PostgresError::Data(format!(
            "parameter count {} exceeds PostgreSQL Protocol 3.0 limit ({})",
            params.len(),
            i16::MAX
        )));
    }
    if let Some(explicit) = parameter_oids
        && explicit.len() != params.len()
    {
        return Err(PostgresError::Data(format!(
            "parameter_oids length {} does not match parameters length {}",
            explicit.len(),
            params.len()
        )));
    }

    let mut combined_oids: Vec<Option<u32>> = Vec::with_capacity(params.len());
    let mut missing_oids: Vec<u32> = Vec::new();
    let mut missing_exts: Vec<&'static str> = Vec::new();

    for (idx, param) in params.iter().enumerate() {
        ensure_no_temporal_text(param.value_ref())?;
        let param_oid = param.type_oid_opt();
        if param_oid == Some(0) {
            return Err(PostgresError::Data(format!(
                "parameter {idx} has invalid explicit type_oid 0 (use None for server inference)"
            )));
        }
        let slice_oid = parameter_oids.map(|s| s[idx]).filter(|&o| o != 0);
        if let (Some(a), Some(b)) = (param_oid, slice_oid)
            && a != b
        {
            return Err(PostgresError::Data(format!(
                "parameter {idx} explicit type_oid {a} conflicts with parameter_oids[{idx}] == {b}"
            )));
        }
        let merged = param_oid.or(slice_oid);
        collect_missing_value_metadata(
            param.value_ref(),
            merged,
            conn.type_catalog(),
            &mut missing_oids,
            &mut missing_exts,
        )?;
        combined_oids.push(merged);
    }

    if !missing_oids.is_empty() || !missing_exts.is_empty() {
        run_pre_parse_discovery(conn, &missing_oids, &missing_exts).await?;
    }

    let mut resolved_params = Vec::with_capacity(params.len());
    let mut resolved_oids = Vec::with_capacity(params.len());
    let mut all_formats_known = true;

    for (idx, param) in params.iter().enumerate() {
        let val = param.value_ref().clone();
        let explicit = combined_oids[idx];
        let oid = infer_parameter_oid(&val, explicit, conn.type_catalog())?;
        if !matches!(val, PgValue::Null)
            && (oid == 0 || format_for_oid_with_catalog(oid, conn.type_catalog()) == Format::Text)
        {
            all_formats_known = false;
        }
        resolved_params.push(Parameter {
            value: val,
            type_oid: (oid != 0).then_some(oid),
        });
        resolved_oids.push(oid);
    }

    Ok(ResolvedParameters {
        parameters: resolved_params,
        parameter_oids: resolved_oids,
        all_formats_known,
    })
}

fn build_fallback_query_result(
    columns: Vec<Column>,
    rows: Vec<Vec<OwnedTextField>>,
    affected_rows: u64,
    command_tag: String,
    transaction_status: TransactionStatus,
    ctx: &CodecContext,
    catalog: &TypeCatalog,
) -> QueryResult {
    let mut decoded_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut out_row = Vec::with_capacity(columns.len());
        for (col, cell) in columns.iter().zip(row) {
            match cell.bytes {
                None => out_row.push(PgValue::Null),
                Some(bytes) => {
                    match decode_value_with_catalog(
                        col.type_oid,
                        Format::Text,
                        Some(&bytes),
                        ctx,
                        catalog,
                    ) {
                        Ok(v) => out_row.push(v),
                        Err(_) => {
                            let s = String::from_utf8_lossy(&bytes).into_owned();
                            out_row.push(PgValue::Text(s));
                        }
                    }
                }
            }
        }
        decoded_rows.push(out_row);
    }
    QueryResult {
        columns,
        rows: decoded_rows,
        affected_rows,
        command_tag,
        transaction_status,
    }
}

/// Normalize an already-completed [`OwnedTextResult`] after `ReadyForQuery`, discovering custom result OIDs safely.
pub async fn normalize_materialized_result(
    conn: &mut PostgresConnection,
    raw: OwnedTextResult,
) -> Result<QueryResult, PostgresError> {
    if !raw.exchange_completed {
        return Err(PostgresError::Interface(
            "cannot normalize OwnedTextResult before exchange_completed is true".to_string(),
        ));
    }
    if conn.is_closed() || conn.exchange_state() != ExchangeState::Idle {
        return Err(PostgresError::Interface(
            "cannot normalize OwnedTextResult on a closed or non-idle PostgresConnection"
                .to_string(),
        ));
    }

    let OwnedTextResult {
        columns,
        rows,
        affected_rows,
        command_tag,
        transaction_status: original_status,
        codec_context,
        exchange_completed: _,
    } = raw;

    let mut missing_oids: Vec<u32> = Vec::new();
    for col in &columns {
        if col.type_oid != 0
            && conn.type_catalog().type_info(col.type_oid).is_none()
            && !missing_oids.contains(&col.type_oid)
        {
            missing_oids.push(col.type_oid);
        }
    }

    if !missing_oids.is_empty() && original_status != TransactionStatus::Failed {
        match original_status {
            TransactionStatus::Idle => {
                let mut cat = conn.type_catalog().clone();
                match cat.discover(conn, &missing_oids, &[]).await {
                    Ok(()) => {}
                    Err(PostgresError::Server(_)) | Err(PostgresError::Data(_)) => {
                        if conn.is_closed() {
                            let completed_result = build_fallback_query_result(
                                columns,
                                rows,
                                affected_rows,
                                command_tag,
                                original_status,
                                &codec_context,
                                conn.type_catalog(),
                            );
                            return Err(PostgresError::AfterCompletion {
                                source: Box::new(PostgresError::Transport(
                                    "connection closed during post-completion catalog lookup"
                                        .to_string(),
                                )),
                                completed_result,
                            });
                        }
                    }
                    Err(unrecoverable) => {
                        conn.closed = true;
                        let completed_result = build_fallback_query_result(
                            columns,
                            rows,
                            affected_rows,
                            command_tag,
                            original_status,
                            &codec_context,
                            conn.type_catalog(),
                        );
                        return Err(PostgresError::AfterCompletion {
                            source: Box::new(unrecoverable),
                            completed_result,
                        });
                    }
                }
            }
            TransactionStatus::InTransaction => {
                match conn.simple_query("SAVEPOINT _vtb_meta").await {
                    Ok(_) => {
                        let mut cat = conn.type_catalog().clone();
                        match cat.discover(conn, &missing_oids, &[]).await {
                            Ok(()) => {
                                if let Err(rel_err) =
                                    conn.simple_query("RELEASE SAVEPOINT _vtb_meta").await
                                {
                                    conn.closed = true;
                                    let completed_result = build_fallback_query_result(
                                        columns,
                                        rows,
                                        affected_rows,
                                        command_tag,
                                        original_status,
                                        &codec_context,
                                        conn.type_catalog(),
                                    );
                                    return Err(PostgresError::AfterCompletion {
                                        source: Box::new(rel_err),
                                        completed_result,
                                    });
                                }
                            }
                            Err(PostgresError::Server(_)) | Err(PostgresError::Data(_)) => {
                                match conn
                                    .simple_query(
                                        "ROLLBACK TO SAVEPOINT _vtb_meta; RELEASE SAVEPOINT _vtb_meta",
                                    )
                                    .await
                                {
                                    Ok(_)
                                        if conn.transaction_status()
                                            == TransactionStatus::InTransaction => {}
                                    Ok(_) => {
                                        conn.closed = true;
                                        let completed_result = build_fallback_query_result(
                                            columns,
                                            rows,
                                            affected_rows,
                                            command_tag,
                                            original_status,
                                            &codec_context,
                                            conn.type_catalog(),
                                        );
                                        return Err(PostgresError::AfterCompletion {
                                            source: Box::new(PostgresError::Protocol(
                                                "metadata savepoint rollback did not restore InTransaction state".to_string(),
                                            )),
                                            completed_result,
                                        });
                                    }
                                    Err(cleanup_err) => {
                                        conn.closed = true;
                                        let completed_result = build_fallback_query_result(
                                            columns,
                                            rows,
                                            affected_rows,
                                            command_tag,
                                            original_status,
                                            &codec_context,
                                            conn.type_catalog(),
                                        );
                                        return Err(PostgresError::AfterCompletion {
                                            source: Box::new(cleanup_err),
                                            completed_result,
                                        });
                                    }
                                }
                            }
                            Err(unrecoverable) => {
                                conn.closed = true;
                                let completed_result = build_fallback_query_result(
                                    columns,
                                    rows,
                                    affected_rows,
                                    command_tag,
                                    original_status,
                                    &codec_context,
                                    conn.type_catalog(),
                                );
                                return Err(PostgresError::AfterCompletion {
                                    source: Box::new(unrecoverable),
                                    completed_result,
                                });
                            }
                        }
                    }
                    Err(sp_err) => {
                        conn.closed = true;
                        let completed_result = build_fallback_query_result(
                            columns,
                            rows,
                            affected_rows,
                            command_tag,
                            original_status,
                            &codec_context,
                            conn.type_catalog(),
                        );
                        return Err(PostgresError::AfterCompletion {
                            source: Box::new(sp_err),
                            completed_result,
                        });
                    }
                }
            }
            TransactionStatus::Failed => {}
        }
    }

    let catalog = conn.type_catalog();
    let mut decoded_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut out_row = Vec::with_capacity(columns.len());
        for (col, cell) in columns.iter().zip(row) {
            let val = decode_value_with_catalog(
                col.type_oid,
                Format::Text,
                cell.bytes.as_deref(),
                &codec_context,
                catalog,
            )?;
            out_row.push(val);
        }
        decoded_rows.push(out_row);
    }

    QueryResult::try_new(
        columns,
        decoded_rows,
        affected_rows,
        command_tag,
        original_status,
    )
}
