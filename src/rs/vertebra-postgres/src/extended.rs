//! PostgreSQL Protocol 3.0 extended-query framing (`Parameter`, `PreparedDescription`, `encode_bind`, `ExtendedQueryCollector`).

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::IsNull;
use postgres_protocol::message::backend::{
    DataRowBody, ParameterDescriptionBody, RowDescriptionBody,
};
use postgres_protocol::message::frontend;

use crate::codecs::{
    CodecContext, TypeCatalog, decode_value_with_catalog, encode_value_with_catalog,
};
use crate::errors::{PostgresError, ServerError};
use crate::metadata::ensure_no_temporal_text;
use crate::query::{parse_command_tag_affected_rows, parse_row_description};
use crate::result::{Column, Format, PgValue, QueryResult, TransactionStatus};

/// Backend `DataRow` frame body alias used by zero-materialization streaming row sinks.
pub type RowBody = DataRowBody;

/// Bound parameter value and optional explicit PostgreSQL type OID.
#[derive(Clone, Debug, PartialEq)]
pub struct Parameter {
    /// Parameter value (`PgValue::Null` for SQL `NULL`).
    pub value: PgValue,
    /// Optional explicit positive PostgreSQL type OID (`None` for server inference).
    pub type_oid: Option<u32>,
}

impl Parameter {
    /// Construct a [`Parameter`] without an explicit OID override.
    pub const fn new(value: PgValue) -> Self {
        Self {
            value,
            type_oid: None,
        }
    }

    /// Construct a [`Parameter`] with a validated positive PostgreSQL `oid` (`>= 1`).
    pub fn with_oid(value: PgValue, oid: u32) -> Result<Self, PostgresError> {
        if oid == 0 {
            return Err(PostgresError::Data(
                "Parameter::with_oid requires a positive PostgreSQL type OID (>= 1)".to_string(),
            ));
        }
        Ok(Self {
            value,
            type_oid: Some(oid),
        })
    }
}

/// Statement parameter and result-column metadata captured from `Describe('S')`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedDescription {
    /// Parameter type OIDs reported by `ParameterDescription`.
    pub parameter_oids: Vec<u32>,
    /// Result column descriptors reported by statement `RowDescription` (`empty` on `NoData`).
    pub result_columns: Vec<Column>,
}

/// Encode a PostgreSQL Protocol 3.0 `Bind` (`'B'`) frontend message into `out`.
#[allow(clippy::too_many_arguments)]
pub fn encode_bind(
    portal: &str,
    statement: &str,
    parameters: &[Parameter],
    param_oids: &[u32],
    param_formats: &[Format],
    result_formats: &[Format],
    ctx: &CodecContext,
    catalog: &TypeCatalog,
    out: &mut BytesMut,
) -> Result<(), PostgresError> {
    if parameters.len() > i16::MAX as usize {
        return Err(PostgresError::Data(format!(
            "Bind parameter count {} exceeds i16::MAX ({})",
            parameters.len(),
            i16::MAX
        )));
    }
    if result_formats.len() > i16::MAX as usize {
        return Err(PostgresError::Data(format!(
            "Bind result_formats count {} exceeds i16::MAX ({})",
            result_formats.len(),
            i16::MAX
        )));
    }
    if param_oids.len() != parameters.len() {
        return Err(PostgresError::Data(format!(
            "Bind param_oids length {} does not match parameters length {}",
            param_oids.len(),
            parameters.len()
        )));
    }
    if param_formats.len() != parameters.len() {
        return Err(PostgresError::Data(format!(
            "Bind param_formats length {} does not match parameters length {}",
            param_formats.len(),
            parameters.len()
        )));
    }

    for param in parameters {
        ensure_no_temporal_text(&param.value)?;
    }

    let fmt_codes = param_formats.iter().map(|f| f.code());
    let res_codes = result_formats.iter().map(|f| f.code());

    let bind_res = frontend::bind(
        portal,
        statement,
        fmt_codes,
        parameters
            .iter()
            .zip(param_oids.iter())
            .zip(param_formats.iter()),
        |((param, &oid), &fmt), buf| {
            let start = buf.len();
            let wrote = encode_value_with_catalog(&param.value, oid, fmt, buf, ctx, catalog)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;
            if !wrote {
                return Ok(IsNull::Yes);
            }
            let written = buf.len() - start;
            if written > i32::MAX as usize {
                return Err(Box::new(PostgresError::Data(format!(
                    "serialized parameter length {written} exceeds i32::MAX"
                )))
                    as Box<dyn std::error::Error + Sync + Send>);
            }
            Ok(IsNull::No)
        },
        res_codes,
        out,
    );

    match bind_res {
        Ok(()) => Ok(()),
        Err(frontend::BindError::Conversion(err)) => match err.downcast::<PostgresError>() {
            Ok(pg_err) => Err(*pg_err),
            Err(other) => Err(PostgresError::Data(other.to_string())),
        },
        Err(frontend::BindError::Serialization(err)) => Err(PostgresError::Protocol(format!(
            "failed to serialize Bind frame: {err}"
        ))),
    }
}

/// Decode a `ParameterDescription` frame body into validated positive parameter OIDs.
pub fn parse_parameter_description(
    body: &ParameterDescriptionBody,
) -> Result<Vec<u32>, PostgresError> {
    let mut iter = body.parameters();
    let mut oids = Vec::new();
    while let Some(oid) = iter.next().map_err(|e| {
        PostgresError::Protocol(format!("malformed ParameterDescription field: {e}"))
    })? {
        if oid == 0 {
            return Err(PostgresError::Protocol(
                "ParameterDescription reported invalid parameter type OID 0".to_string(),
            ));
        }
        oids.push(oid);
    }
    Ok(oids)
}

/// Stateful collector correlating extended-query frames (`ParseComplete`, `ParameterDescription`,
/// statement/portal `RowDescription`/`NoData`, `BindComplete`, `DataRow`, `CommandComplete`, `CloseComplete`, `ReadyForQuery`).
#[derive(Debug)]
pub struct ExtendedQueryCollector {
    expected_param_count: usize,
    requested_result_formats: Vec<Format>,
    expect_portal_describe: bool,
    saw_parse_complete: bool,
    saw_bind_complete: bool,
    statement_describe_done: bool,
    portal_describe_done: bool,
    statement_param_oids: Vec<u32>,
    statement_columns: Vec<Column>,
    portal_columns: Vec<Column>,
    rows: Vec<Vec<PgValue>>,
    command_tag: Option<String>,
    affected_rows: u64,
    first_error: Option<PostgresError>,
}

impl ExtendedQueryCollector {
    /// Construct a new [`ExtendedQueryCollector`].
    pub fn new(
        expected_param_count: usize,
        requested_result_formats: Vec<Format>,
        expect_statement_describe: bool,
        expect_portal_describe: bool,
    ) -> Self {
        Self {
            expected_param_count,
            requested_result_formats,
            expect_portal_describe,
            saw_parse_complete: false,
            saw_bind_complete: false,
            statement_describe_done: !expect_statement_describe,
            portal_describe_done: !expect_portal_describe,
            statement_param_oids: Vec::new(),
            statement_columns: Vec::new(),
            portal_columns: Vec::new(),
            rows: Vec::new(),
            command_tag: None,
            affected_rows: 0,
            first_error: None,
        }
    }

    /// Return `true` if a recoverable server or data error has been recorded.
    pub const fn has_error(&self) -> bool {
        self.first_error.is_some()
    }

    /// Borrow the negotiated portal [`Column`] descriptors.
    pub fn portal_columns(&self) -> &[Column] {
        &self.portal_columns
    }

    /// Extract the captured [`PreparedDescription`] from statement `Describe('S')`.
    pub fn prepared_description(&self) -> PreparedDescription {
        PreparedDescription {
            parameter_oids: self.statement_param_oids.clone(),
            result_columns: self.statement_columns.clone(),
        }
    }

    /// Record a `ParseComplete` frame.
    pub fn handle_parse_complete(&mut self) -> Result<(), PostgresError> {
        if self.saw_parse_complete {
            return Err(PostgresError::Protocol(
                "received duplicate ParseComplete in single extended exchange".to_string(),
            ));
        }
        self.saw_parse_complete = true;
        Ok(())
    }

    /// Record and validate a `ParameterDescription` frame against `expected_param_count`.
    pub fn handle_parameter_description(
        &mut self,
        body: &ParameterDescriptionBody,
    ) -> Result<(), PostgresError> {
        let oids = parse_parameter_description(body)?;
        if oids.len() != self.expected_param_count {
            return Err(PostgresError::Protocol(format!(
                "ParameterDescription count {} does not match bound parameter count {}",
                oids.len(),
                self.expected_param_count
            )));
        }
        self.statement_param_oids = oids;
        Ok(())
    }

    /// Record a `BindComplete` frame.
    pub fn handle_bind_complete(&mut self) -> Result<(), PostgresError> {
        if self.saw_bind_complete {
            return Err(PostgresError::Protocol(
                "received duplicate BindComplete in single extended exchange".to_string(),
            ));
        }
        self.saw_bind_complete = true;
        Ok(())
    }

    /// Record a `RowDescription` frame (for either statement `Describe('S')` or portal `Describe('P')`).
    pub fn handle_row_description(
        &mut self,
        body: &RowDescriptionBody,
    ) -> Result<(), PostgresError> {
        let cols = parse_row_description(body)?;
        if !self.statement_describe_done && !self.saw_bind_complete {
            self.statement_columns = cols.clone();
            self.statement_describe_done = true;
            if !self.expect_portal_describe {
                self.portal_columns = cols;
                self.validate_portal_formats()?;
            }
            return Ok(());
        }
        if !self.portal_describe_done {
            self.portal_columns = cols;
            self.portal_describe_done = true;
            self.validate_portal_formats()?;
            return Ok(());
        }
        Err(PostgresError::Protocol(
            "received unexpected extra RowDescription in extended exchange".to_string(),
        ))
    }

    /// Record a `NoData` frame (for either statement `Describe('S')` or portal `Describe('P')`).
    pub fn handle_no_data(&mut self) -> Result<(), PostgresError> {
        if !self.statement_describe_done && !self.saw_bind_complete {
            self.statement_columns.clear();
            self.statement_describe_done = true;
            return Ok(());
        }
        if !self.portal_describe_done {
            self.portal_columns.clear();
            self.portal_describe_done = true;
            return Ok(());
        }
        Err(PostgresError::Protocol(
            "received unexpected extra NoData in extended exchange".to_string(),
        ))
    }

    fn validate_portal_formats(&self) -> Result<(), PostgresError> {
        if self.requested_result_formats.is_empty() {
            return Ok(());
        }
        if self.requested_result_formats.len() == 1 {
            let expected = self.requested_result_formats[0];
            for col in &self.portal_columns {
                if col.format != expected {
                    return Err(PostgresError::Protocol(format!(
                        "portal RowDescription column `{}` format {:?} does not match requested uniform format {:?}",
                        col.name, col.format, expected
                    )));
                }
            }
            return Ok(());
        }
        if self.requested_result_formats.len() != self.portal_columns.len() {
            return Err(PostgresError::Protocol(format!(
                "portal RowDescription column count {} does not match requested result_formats count {}",
                self.portal_columns.len(),
                self.requested_result_formats.len()
            )));
        }
        for (col, &expected) in self
            .portal_columns
            .iter()
            .zip(self.requested_result_formats.iter())
        {
            if col.format != expected {
                return Err(PostgresError::Protocol(format!(
                    "portal RowDescription column `{}` format {:?} does not match requested format {:?}",
                    col.name, col.format, expected
                )));
            }
        }
        Ok(())
    }

    /// Decode a `DataRow` frame into `self.rows` using `ctx` and `catalog`.
    pub fn handle_data_row(
        &mut self,
        body: &DataRowBody,
        ctx: &CodecContext,
        catalog: &TypeCatalog,
    ) -> Result<(), PostgresError> {
        if !self.saw_bind_complete {
            return Err(PostgresError::Protocol(
                "received DataRow before BindComplete in extended exchange".to_string(),
            ));
        }
        if self.portal_columns.is_empty() && self.statement_columns.is_empty() {
            return Err(PostgresError::Protocol(
                "received DataRow without preceding RowDescription".to_string(),
            ));
        }
        let cols = if !self.portal_columns.is_empty() {
            &self.portal_columns
        } else {
            &self.statement_columns
        };

        let buffer = body.buffer();
        let mut ranges = body.ranges();
        let mut row = Vec::with_capacity(cols.len());
        let mut idx = 0usize;

        while let Some(range_opt) = ranges
            .next()
            .map_err(|e| PostgresError::Protocol(format!("malformed DataRow field range: {e}")))?
        {
            let Some(col) = cols.get(idx) else {
                return Err(PostgresError::Protocol(format!(
                    "DataRow contains more fields than RowDescription ({})",
                    cols.len()
                )));
            };
            let raw_slice = range_opt.map(|r| &buffer[r]);
            match decode_value_with_catalog(col.type_oid, col.format, raw_slice, ctx, catalog) {
                Ok(val) => row.push(val),
                Err(err) => {
                    if self.first_error.is_none() {
                        self.first_error = Some(err);
                    }
                    row.push(PgValue::Null);
                }
            }
            idx += 1;
        }

        if idx != cols.len() {
            return Err(PostgresError::Protocol(format!(
                "DataRow field count {idx} does not match RowDescription column count {}",
                cols.len()
            )));
        }

        if self.first_error.is_none() {
            self.rows.push(row);
        }
        Ok(())
    }

    /// Record a `CommandComplete` tag.
    pub fn handle_command_complete(&mut self, tag: &str) -> Result<(), PostgresError> {
        let affected = parse_command_tag_affected_rows(tag)?;
        self.affected_rows = affected;
        self.command_tag = Some(tag.to_owned());
        Ok(())
    }

    /// Record an `EmptyQueryResponse`.
    pub fn handle_empty_query(&mut self) -> Result<(), PostgresError> {
        if !self.rows.is_empty() {
            return Err(PostgresError::Protocol(
                "received EmptyQueryResponse after DataRow".to_string(),
            ));
        }
        self.affected_rows = 0;
        self.command_tag = Some(String::new());
        Ok(())
    }

    /// Record a recoverable `ErrorResponse` or application error.
    pub fn handle_error(&mut self, err: PostgresError) {
        if self.first_error.is_none() {
            self.first_error = Some(err);
        }
    }

    /// Record a backend `ErrorResponse`.
    pub fn handle_error_response(&mut self, err: ServerError) {
        self.handle_error(PostgresError::Server(err));
    }

    /// Finalize the extended exchange at `ReadyForQuery(final_status)`.
    pub fn finish(self, final_status: TransactionStatus) -> Result<QueryResult, PostgresError> {
        if let Some(err) = self.first_error {
            return Err(err);
        }
        let Some(tag) = self.command_tag else {
            return Err(PostgresError::Protocol(
                "received ReadyForQuery before CommandComplete or EmptyQueryResponse".to_string(),
            ));
        };
        let cols = if !self.portal_columns.is_empty() {
            self.portal_columns
        } else {
            self.statement_columns
        };
        QueryResult::try_new(cols, self.rows, self.affected_rows, tag, final_status)
    }
}

/// Materialize an already-completed [`crate::metadata::OwnedTextResult`] without issuing optional
/// post-`ReadyForQuery` catalog discovery I/O when the caller's command deadline is exhausted.
///
/// Known built-in and cached custom OIDs decode via `catalog` + captured `codec_context`; uncached
/// custom OIDs preserve exact owned UTF-8 text and column `type_oid` metadata.
pub(crate) fn decode_completed_text_result_without_enrichment(
    raw: crate::metadata::OwnedTextResult,
    catalog: &TypeCatalog,
) -> Result<QueryResult, PostgresError> {
    if !raw.exchange_completed {
        return Err(PostgresError::Interface(
            "cannot decode OwnedTextResult before exchange_completed is true".to_string(),
        ));
    }
    let crate::metadata::OwnedTextResult {
        columns,
        rows,
        affected_rows,
        command_tag,
        transaction_status,
        codec_context,
        ..
    } = raw;
    let mut decoded_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut out_row = Vec::with_capacity(columns.len());
        for (col, cell) in columns.iter().zip(row) {
            match cell.bytes {
                None => out_row.push(PgValue::Null),
                Some(bytes) => {
                    if catalog.type_info(col.type_oid).is_some() {
                        let val = decode_value_with_catalog(
                            col.type_oid,
                            Format::Text,
                            Some(&bytes),
                            &codec_context,
                            catalog,
                        )?;
                        out_row.push(val);
                    } else {
                        match decode_value_with_catalog(
                            col.type_oid,
                            Format::Text,
                            Some(&bytes),
                            &codec_context,
                            catalog,
                        ) {
                            Ok(v) => out_row.push(v),
                            Err(_) => {
                                let text = std::str::from_utf8(&bytes).map_err(|e| {
                                    PostgresError::Data(format!(
                                        "invalid UTF-8 in fallback text column `{}` (OID {}): {e}",
                                        col.name, col.type_oid
                                    ))
                                })?;
                                out_row.push(PgValue::Text(text.to_owned()));
                            }
                        }
                    }
                }
            }
        }
        decoded_rows.push(out_row);
    }
    QueryResult::try_new(
        columns,
        decoded_rows,
        affected_rows,
        command_tag,
        transaction_status,
    )
}
