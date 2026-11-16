//! Zero-copy VTBARROW shared-memory Arrow IPC streaming and bulk Arrow table ingestion.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rusqlite::types::ValueRef;
use vertebra_core::shm::ShmRegion;
use vertebra_db::arrow_shm::write_shm_arrow_region_unregistered;

use crate::errors::SqliteClientError;
use crate::value::{BoundParams, SqliteValue};

static ARROW_SHM_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Payload representation of an Arrow query result set.
#[derive(Clone, Debug)]
pub enum ArrowResultPayload {
    /// Zero-copy `VTBARROW` shared-memory region.
    Shm {
        shm_name: String,
        byte_len: usize,
        row_count: usize,
        region: Arc<ShmRegion>,
    },
    /// Inline IPC bytes fallback for small or empty payloads.
    Inline {
        ipc_bytes: Vec<u8>,
        row_count: usize,
        columns: Vec<String>,
    },
}

enum ColumnBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
}

impl ColumnBuilder {
    fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, capacity * 32)),
            _ => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 32)),
        }
    }

    fn append_value_ref(&mut self, val: ValueRef<'_>) -> Result<(), SqliteClientError> {
        match self {
            Self::Int64(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(i) => b.append_value(i),
                ValueRef::Real(r) => b.append_value(r as i64),
                ValueRef::Text(t) => {
                    let s = std::str::from_utf8(t).map_err(|e| SqliteClientError::Data {
                        message: format!("invalid UTF-8 for integer: {e}"),
                        code: None,
                        sqlstate: Some("22000"),
                    })?;
                    let i: i64 = s.trim().parse().map_err(|e| SqliteClientError::Data {
                        message: format!("failed to parse integer `{s}`: {e}"),
                        code: None,
                        sqlstate: Some("22000"),
                    })?;
                    b.append_value(i);
                }
                ValueRef::Blob(_) => b.append_null(),
            },
            Self::Float64(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(i) => b.append_value(i as f64),
                ValueRef::Real(r) => b.append_value(r),
                ValueRef::Text(t) => {
                    let s = std::str::from_utf8(t).map_err(|e| SqliteClientError::Data {
                        message: format!("invalid UTF-8 for float: {e}"),
                        code: None,
                        sqlstate: Some("22000"),
                    })?;
                    let f: f64 = s.trim().parse().map_err(|e| SqliteClientError::Data {
                        message: format!("failed to parse float `{s}`: {e}"),
                        code: None,
                        sqlstate: Some("22000"),
                    })?;
                    b.append_value(f);
                }
                ValueRef::Blob(_) => b.append_null(),
            },
            Self::Boolean(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(i) => b.append_value(i != 0),
                ValueRef::Real(r) => b.append_value(r != 0.0),
                ValueRef::Text(t) => {
                    let s = std::str::from_utf8(t).map_err(|e| SqliteClientError::Data {
                        message: format!("invalid UTF-8 for boolean: {e}"),
                        code: None,
                        sqlstate: Some("22000"),
                    })?;
                    b.append_value(s.eq_ignore_ascii_case("true") || s == "1");
                }
                ValueRef::Blob(_) => b.append_null(),
            },
            Self::Utf8(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Integer(i) => b.append_value(i.to_string()),
                ValueRef::Real(r) => b.append_value(r.to_string()),
                ValueRef::Text(t) => {
                    let s = std::str::from_utf8(t).map_err(|e| SqliteClientError::Data {
                        message: format!("invalid UTF-8 for text: {e}"),
                        code: None,
                        sqlstate: Some("22000"),
                    })?;
                    b.append_value(s);
                }
                ValueRef::Blob(bytes) => {
                    b.append_value(String::from_utf8_lossy(bytes));
                }
            },
            Self::Binary(b) => match val {
                ValueRef::Null => b.append_null(),
                ValueRef::Blob(bytes) => b.append_value(bytes),
                ValueRef::Text(t) => b.append_value(t),
                ValueRef::Integer(i) => b.append_value(i.to_le_bytes()),
                ValueRef::Real(r) => b.append_value(r.to_le_bytes()),
            },
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
        }
    }
}

fn infer_data_type(decl_type: Option<&str>) -> DataType {
    if let Some(decl) = decl_type {
        let upper = decl.to_ascii_uppercase();
        if upper.contains("BOOL") {
            return DataType::Boolean;
        }
        if upper.contains("INT") {
            return DataType::Int64;
        }
        if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            return DataType::Float64;
        }
        if upper.contains("BLOB") || upper.contains("BINARY") || upper.contains("JSONB") {
            return DataType::Binary;
        }
        if upper.contains("CHAR")
            || upper.contains("CLOB")
            || upper.contains("TEXT")
            || upper.contains("STR")
        {
            return DataType::Utf8;
        }
    }
    DataType::Utf8
}

/// Execute a query and stream its results into a `VTBARROW` shared-memory region.
pub fn execute_select_to_arrow(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &BoundParams,
    batch_size: usize,
) -> Result<ArrowResultPayload, SqliteClientError> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(SqliteClientError::from_rusqlite)?;
    params.bind_to_stmt(&mut stmt)?;

    let col_names: Vec<String> = stmt.column_names().map(ToString::to_string).collect();
    let col_count = col_names.len();

    let mut fields = Vec::with_capacity(col_count);
    for col in stmt.columns() {
        let name = col.name().to_string();
        let decl = col.decl_type();
        let dt = infer_data_type(decl);
        fields.push(Field::new(name, dt, true));
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    let target_batch_size = if batch_size == 0 { 8192 } else { batch_size };
    let mut rows = stmt.raw_query();

    let mut ipc_buffer = Vec::new();
    let mut stream_writer = StreamWriter::try_new(&mut ipc_buffer, &schema).map_err(|e| {
        SqliteClientError::ShmArrow(format!("failed to initialize Arrow IPC stream writer: {e}"))
    })?;

    let mut total_rows = 0;
    let mut builders: Vec<ColumnBuilder> = schema
        .fields()
        .iter()
        .map(|f| ColumnBuilder::new(f.data_type(), target_batch_size))
        .collect();
    let mut current_batch_len = 0;

    while let Some(row) = rows.next().map_err(SqliteClientError::from_rusqlite)? {
        for (idx, builder) in builders.iter_mut().enumerate().take(col_count) {
            let val_ref = row.get_ref(idx).map_err(SqliteClientError::from_rusqlite)?;
            builder.append_value_ref(val_ref)?;
        }
        total_rows += 1;
        current_batch_len += 1;

        if current_batch_len >= target_batch_size {
            let arrays: Vec<ArrayRef> = builders.iter_mut().map(ColumnBuilder::finish).collect();
            let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(|e| {
                SqliteClientError::ShmArrow(format!("failed to construct RecordBatch: {e}"))
            })?;
            stream_writer.write(&batch).map_err(|e| {
                SqliteClientError::ShmArrow(format!("failed to write Arrow RecordBatch: {e}"))
            })?;
            current_batch_len = 0;
            builders = schema
                .fields()
                .iter()
                .map(|f| ColumnBuilder::new(f.data_type(), target_batch_size))
                .collect();
        }
    }

    if current_batch_len > 0 {
        let arrays: Vec<ArrayRef> = builders.iter_mut().map(ColumnBuilder::finish).collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(|e| {
            SqliteClientError::ShmArrow(format!("failed to construct RecordBatch: {e}"))
        })?;
        stream_writer.write(&batch).map_err(|e| {
            SqliteClientError::ShmArrow(format!("failed to write Arrow RecordBatch: {e}"))
        })?;
    } else if total_rows == 0 {
        let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
        stream_writer.write(&empty_batch).map_err(|e| {
            SqliteClientError::ShmArrow(format!("failed to write empty Arrow RecordBatch: {e}"))
        })?;
    }

    stream_writer.finish().map_err(|e| {
        SqliteClientError::ShmArrow(format!("failed to finalize Arrow IPC stream: {e}"))
    })?;

    let shm_seq = ARROW_SHM_COUNTER.fetch_add(1, Ordering::Relaxed);
    let shm_name = format!("/vtb_sqlite_{}_{}", std::process::id(), shm_seq);
    let (norm_name, byte_len, region) = write_shm_arrow_region_unregistered(&shm_name, &ipc_buffer)
        .map_err(SqliteClientError::ShmArrow)?;

    Ok(ArrowResultPayload::Shm {
        shm_name: norm_name,
        byte_len,
        row_count: total_rows,
        region,
    })
}

/// Ingest Arrow IPC stream bytes into a SQLite database table inside a transaction.
pub fn execute_load_from_arrow(
    conn: &mut rusqlite::Connection,
    table_name: &str,
    arrow_ipc_bytes: &[u8],
) -> Result<usize, SqliteClientError> {
    if arrow_ipc_bytes.is_empty() {
        return Ok(0);
    }
    let cursor = std::io::Cursor::new(arrow_ipc_bytes);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| SqliteClientError::Data {
        message: format!("invalid Arrow IPC stream: {e}"),
        code: None,
        sqlstate: Some("22000"),
    })?;

    let schema = reader.schema();
    let fields = schema.fields();
    if fields.is_empty() {
        return Ok(0);
    }

    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(SqliteClientError::from_rusqlite)?;

    // Check if table exists; create if not present
    let table_exists: bool = tx
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?1",
            [table_name],
            |_| Ok(true),
        )
        .unwrap_or(false);

    if !table_exists {
        let mut ddl_cols = Vec::with_capacity(fields.len());
        for f in fields {
            let col_name = f.name().replace('"', "\"\"");
            let sql_type = match f.data_type() {
                DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => "INTEGER",
                DataType::Float16 | DataType::Float32 | DataType::Float64 => "REAL",
                DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
                DataType::Binary | DataType::LargeBinary => "BLOB",
                _ => "BLOB",
            };
            ddl_cols.push(format!("\"{col_name}\" {sql_type}"));
        }
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" ({})",
            table_name.replace('"', "\"\""),
            ddl_cols.join(", ")
        );
        tx.execute_batch(&create_sql)
            .map_err(SqliteClientError::from_rusqlite)?;
    }

    let col_names_quoted: Vec<String> = fields
        .iter()
        .map(|f| format!("\"{}\"", f.name().replace('"', "\"\"")))
        .collect();
    let placeholders: Vec<String> = (1..=fields.len()).map(|i| format!("?{i}")).collect();
    let insert_sql = format!(
        "INSERT INTO \"{}\" ({}) VALUES ({})",
        table_name.replace('"', "\"\""),
        col_names_quoted.join(", "),
        placeholders.join(", ")
    );

    let mut stmt = tx
        .prepare(&insert_sql)
        .map_err(SqliteClientError::from_rusqlite)?;
    let mut total_inserted = 0;

    for batch_res in reader {
        let batch = batch_res.map_err(|e| SqliteClientError::Data {
            message: format!("failed to decode Arrow RecordBatch: {e}"),
            code: None,
            sqlstate: Some("22000"),
        })?;

        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            for col_idx in 0..num_cols {
                let array = batch.column(col_idx);
                let param_idx = col_idx + 1;
                if array.is_null(row_idx) {
                    stmt.raw_bind_parameter(param_idx, &SqliteValue::Null)
                        .map_err(SqliteClientError::from_rusqlite)?;
                    continue;
                }
                match array.data_type() {
                    DataType::Boolean => {
                        let bool_arr = array
                            .as_any()
                            .downcast_ref::<arrow_array::BooleanArray>()
                            .unwrap();
                        stmt.raw_bind_parameter(
                            param_idx,
                            if bool_arr.value(row_idx) { 1i64 } else { 0i64 },
                        )
                        .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::Int8 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::Int8Array>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, i64::from(a.value(row_idx)))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::Int16 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::Int16Array>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, i64::from(a.value(row_idx)))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::Int32 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::Int32Array>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, i64::from(a.value(row_idx)))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::Int64 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::Int64Array>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, a.value(row_idx))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::UInt8 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::UInt8Array>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, i64::from(a.value(row_idx)))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::UInt16 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::UInt16Array>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, i64::from(a.value(row_idx)))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::UInt32 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::UInt32Array>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, i64::from(a.value(row_idx)))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::UInt64 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::UInt64Array>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, a.value(row_idx) as i64)
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::Float32 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::Float32Array>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, f64::from(a.value(row_idx)))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::Float64 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::Float64Array>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, a.value(row_idx))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::Utf8 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, a.value(row_idx))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::LargeUtf8 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::LargeStringArray>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, a.value(row_idx))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::Binary => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::BinaryArray>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, a.value(row_idx))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    DataType::LargeBinary => {
                        let a = array
                            .as_any()
                            .downcast_ref::<arrow_array::LargeBinaryArray>()
                            .unwrap();
                        stmt.raw_bind_parameter(param_idx, a.value(row_idx))
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                    _ => {
                        stmt.raw_bind_parameter(param_idx, &SqliteValue::Null)
                            .map_err(SqliteClientError::from_rusqlite)?;
                    }
                }
            }
            stmt.raw_execute()
                .map_err(SqliteClientError::from_rusqlite)?;
            total_inserted += 1;
        }
    }
    drop(stmt);
    tx.commit().map_err(SqliteClientError::from_rusqlite)?;

    Ok(total_inserted)
}
