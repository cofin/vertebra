//! PostgreSQL Protocol 3.0 simple-query collector, command-tag parser, and `ErrorResponse` decoder.

use std::collections::BTreeMap;

use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::{DataRowBody, ErrorResponseBody, RowDescriptionBody};

use crate::codecs::{CodecContext, ParameterMap, decode_value};
use crate::errors::{PostgresError, ServerError};
use crate::protocol::parse_error_fields;
use crate::result::{Column, Format, PgValue, QueryResult, TransactionStatus};

/// Parse a `CommandComplete` tag and extract its checked `u64` affected row count.
///
/// Only documented row-bearing PostgreSQL command tags (`INSERT <oid> <rows>`, `DELETE <rows>`,
/// `UPDATE <rows>`, `SELECT <rows>`, `MOVE <rows>`, `FETCH <rows>`, `COPY <rows>`, `MERGE <rows>`)
/// return a non-zero count; DDL, transaction, and utility tags return `Ok(0)`.
pub fn parse_command_tag_affected_rows(tag: &str) -> Result<u64, PostgresError> {
    let trimmed = tag.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    match parts.as_slice() {
        ["INSERT", _oid, rows] => rows.parse::<u64>().map_err(|_| {
            PostgresError::Protocol(format!("invalid INSERT row count in command tag {tag:?}"))
        }),
        [
            "DELETE" | "UPDATE" | "SELECT" | "MOVE" | "FETCH" | "COPY" | "MERGE",
            rows,
        ] => rows.parse::<u64>().map_err(|_| {
            PostgresError::Protocol(format!("invalid row count in command tag {tag:?}"))
        }),
        _ => Ok(0),
    }
}

/// Decode a `RowDescription` frame body into validated [`Column`] descriptors.
pub fn parse_row_description(body: &RowDescriptionBody) -> Result<Vec<Column>, PostgresError> {
    let mut fields = body.fields();
    let mut columns = Vec::new();
    while let Some(field) = fields
        .next()
        .map_err(|e| PostgresError::Protocol(format!("malformed RowDescription field: {e}")))?
    {
        let format = Format::from_code(field.format())?;
        let col = Column::try_new(
            field.name().to_owned(),
            field.type_oid(),
            format,
            field.type_modifier(),
            field.table_oid(),
            field.column_id(),
        )?;
        columns.push(col);
    }
    Ok(columns)
}

/// Decode a backend `ErrorResponse` frame body into a structured [`ServerError`].
pub fn parse_error_response(body: &ErrorResponseBody) -> Result<ServerError, PostgresError> {
    match parse_error_fields(body.fields())? {
        PostgresError::Server(srv) => Ok(srv),
        other => Err(other),
    }
}

/// Stateful collector correlating `RowDescription`, `DataRow`, `CommandComplete`,
/// `EmptyQueryResponse`, and `ErrorResponse` frames within a simple `Query` exchange.
#[derive(Debug, Default)]
pub struct SimpleQueryCollector {
    results: Vec<QueryResult>,
    current_columns: Option<Vec<Column>>,
    current_rows: Vec<Vec<PgValue>>,
    current_ctx: CodecContext,
    first_error: Option<PostgresError>,
}

impl SimpleQueryCollector {
    /// Create a new empty [`SimpleQueryCollector`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Return `true` if a recoverable error has already been recorded during this exchange.
    pub const fn has_error(&self) -> bool {
        self.first_error.is_some()
    }

    /// Record a `RowDescription` frame using the session's current `ParameterStatus` map.
    pub fn handle_row_description<M: ParameterMap + ?Sized>(
        &mut self,
        body: &RowDescriptionBody,
        params: &M,
    ) -> Result<(), PostgresError> {
        if self.current_columns.is_some() {
            return Err(PostgresError::Protocol(
                "received second RowDescription before CommandComplete".to_string(),
            ));
        }
        let cols = parse_row_description(body)?;
        self.current_columns = Some(cols);
        self.current_rows.clear();
        self.current_ctx = CodecContext::from_parameters(params);
        Ok(())
    }

    /// Record and decode a `DataRow` frame against the active `RowDescription`.
    pub fn handle_data_row(&mut self, body: &DataRowBody) -> Result<(), PostgresError> {
        let Some(columns) = self.current_columns.as_ref() else {
            return Err(PostgresError::Protocol(
                "received DataRow without preceding RowDescription".to_string(),
            ));
        };
        let buffer = body.buffer();
        let mut ranges = body.ranges();
        let mut row = Vec::with_capacity(columns.len());
        let mut col_idx = 0usize;

        while let Some(range_opt) = ranges
            .next()
            .map_err(|e| PostgresError::Protocol(format!("malformed DataRow field range: {e}")))?
        {
            let Some(col) = columns.get(col_idx) else {
                return Err(PostgresError::Protocol(format!(
                    "DataRow contains more fields than RowDescription ({})",
                    columns.len()
                )));
            };
            let raw_slice = range_opt.map(|r| &buffer[r]);
            match decode_value(col.type_oid, col.format, raw_slice, &self.current_ctx) {
                Ok(val) => row.push(val),
                Err(err) => {
                    if self.first_error.is_none() {
                        self.first_error = Some(err);
                    }
                    row.push(PgValue::Null);
                }
            }
            col_idx += 1;
        }

        if col_idx != columns.len() {
            return Err(PostgresError::Protocol(format!(
                "DataRow field count {col_idx} does not match RowDescription column count {}",
                columns.len()
            )));
        }

        if self.first_error.is_none() {
            self.current_rows.push(row);
        }
        Ok(())
    }

    /// Complete the current statement with a `CommandComplete` tag.
    pub fn handle_command_complete(&mut self, tag: &str) -> Result<(), PostgresError> {
        let affected_rows = parse_command_tag_affected_rows(tag)?;
        let columns = self.current_columns.take().unwrap_or_default();
        let rows = std::mem::take(&mut self.current_rows);
        if self.first_error.is_none() {
            let result = QueryResult::try_new(
                columns,
                rows,
                affected_rows,
                tag.to_owned(),
                TransactionStatus::Idle,
            )?;
            self.results.push(result);
        }
        Ok(())
    }

    /// Complete an empty statement (`EmptyQueryResponse`).
    pub fn handle_empty_query(&mut self) -> Result<(), PostgresError> {
        if self.current_columns.is_some() || !self.current_rows.is_empty() {
            return Err(PostgresError::Protocol(
                "received EmptyQueryResponse after RowDescription/DataRow".to_string(),
            ));
        }
        if self.first_error.is_none() {
            self.results.push(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                affected_rows: 0,
                command_tag: String::new(),
                transaction_status: TransactionStatus::Idle,
            });
        }
        Ok(())
    }

    /// Record a backend `ErrorResponse` and discard any partial in-progress result set.
    pub fn handle_error_response(&mut self, err: ServerError) {
        self.current_columns = None;
        self.current_rows.clear();
        if self.first_error.is_none() {
            self.first_error = Some(PostgresError::Server(err));
        }
    }

    /// Finalize the exchange at `ReadyForQuery(final_status)`.
    pub fn finish(
        mut self,
        final_status: TransactionStatus,
    ) -> Result<Vec<QueryResult>, PostgresError> {
        if let Some(err) = self.first_error {
            return Err(err);
        }
        if self.current_columns.is_some() || !self.current_rows.is_empty() {
            return Err(PostgresError::Protocol(
                "received ReadyForQuery while a result set was still open without CommandComplete"
                    .to_string(),
            ));
        }
        for res in &mut self.results {
            res.transaction_status = final_status;
        }
        Ok(self.results)
    }
}

/// Convenience helper to build a [`CodecContext`] from a `BTreeMap` parameter snapshot.
pub fn context_from_btree(params: &BTreeMap<String, String>) -> CodecContext {
    CodecContext::from_parameters(params)
}
