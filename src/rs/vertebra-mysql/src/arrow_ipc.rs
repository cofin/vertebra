//! Zero-copy `VTBARROW` POSIX shared-memory Arrow IPC serialization and bulk-load decoding for `vertebra-mysql`.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, Time64MicrosecondType, TimestampMicrosecondType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use mysql_async::Value;
use mysql_async::consts::ColumnType;
use vertebra_core::shm::ShmRegion;
use vertebra_db::arrow_shm::{
    SHM_ARROW_MAGIC, ShmArrowFrameHeader, ShmArrowStreamWriter, write_shm_arrow_region_unregistered,
};

use crate::client::DecodedCell;
use crate::errors::MySQLError;

/// Default initial byte capacity (`64 KiB`) when opening a MySQL `VTBARROW` stream writer.
pub const DEFAULT_MYSQL_ARROW_SHM_CAPACITY: usize = 64 * 1024;

/// Default scale (`9`) used when encoding MySQL `DECIMAL` columns into `Decimal128(38, 9)`.
pub const MYSQL_ARROW_DECIMAL_SCALE: i8 = 9;

static SHM_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Tuple `(shm_name, byte_len, row_count, Arc<ShmRegion>)` representing a materialized `VTBARROW` segment.
pub type ShmArrowBatchHandle = (String, usize, usize, Arc<ShmRegion>);

/// Allocate the next unique process-scoped POSIX shared-memory name (`/vtb-my-<pid>-<seq>`, `<= 31` bytes)
/// for a MySQL `VTBARROW` result cursor.
#[inline]
#[pyo3::pyfunction]
pub fn next_mysql_shm_name() -> String {
    let seq = SHM_COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFFF_FFFF;
    let pid = std::process::id();
    format!("/vtb-my-{pid:x}-{seq:x}")
}

/// Open a [`ShmArrowStreamWriter`] backed by a fresh `/vtb-my-*` POSIX shared-memory segment
/// initialized with `schema_ipc_frame`.
pub fn open_mysql_shm_writer(schema_ipc_frame: &[u8]) -> Result<ShmArrowStreamWriter, MySQLError> {
    let shm_name = next_mysql_shm_name();
    ShmArrowStreamWriter::create(
        &shm_name,
        DEFAULT_MYSQL_ARROW_SHM_CAPACITY,
        schema_ipc_frame,
    )
    .map_err(MySQLError::protocol)
}

/// Write a complete Arrow IPC byte stream into an unregistered `VTBARROW` shared-memory region,
/// returning `(shm_name, payload_len, Arc<ShmRegion>)`.
pub fn write_mysql_shm_frame(
    ipc_bytes: &[u8],
) -> Result<(String, usize, Arc<ShmRegion>), MySQLError> {
    let shm_name = next_mysql_shm_name();
    write_shm_arrow_region_unregistered(&shm_name, ipc_bytes).map_err(MySQLError::protocol)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArrowColKind {
    Boolean,
    Int64,
    UInt64,
    Float64,
    Decimal128,
    Date32,
    TimestampMicros,
    Time64Micros,
    Binary,
    Utf8,
}

impl ArrowColKind {
    fn data_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int64 => DataType::Int64,
            Self::UInt64 => DataType::UInt64,
            Self::Float64 => DataType::Float64,
            Self::Decimal128 => DataType::Decimal128(38, MYSQL_ARROW_DECIMAL_SCALE),
            Self::Date32 => DataType::Date32,
            Self::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Time64Micros => DataType::Time64(TimeUnit::Microsecond),
            Self::Binary => DataType::Binary,
            Self::Utf8 => DataType::Utf8,
        }
    }
}

fn infer_column_kind(col_type: u8, rows: &[Vec<DecodedCell>], col_idx: usize) -> ArrowColKind {
    for row in rows {
        if let Some(cell) = row.get(col_idx) {
            match cell {
                DecodedCell::Null => continue,
                DecodedCell::Bool(_) => return ArrowColKind::Boolean,
                DecodedCell::Int(_) => return ArrowColKind::Int64,
                DecodedCell::UInt(u) => {
                    if *u > i64::MAX as u64 {
                        return ArrowColKind::UInt64;
                    }
                    return ArrowColKind::Int64;
                }
                DecodedCell::Float(_) => return ArrowColKind::Float64,
                DecodedCell::Decimal(s) => {
                    if parse_decimal128_scaled(s, MYSQL_ARROW_DECIMAL_SCALE as u32).is_some() {
                        return ArrowColKind::Decimal128;
                    }
                    return ArrowColKind::Utf8;
                }
                DecodedCell::Date(..) => return ArrowColKind::Date32,
                DecodedCell::DateTime(..) => return ArrowColKind::TimestampMicros,
                DecodedCell::Time(..) => return ArrowColKind::Time64Micros,
                DecodedCell::Bytes(_) => return ArrowColKind::Binary,
                DecodedCell::Json(_) | DecodedCell::Text(_) => return ArrowColKind::Utf8,
            }
        }
    }

    if col_type == ColumnType::MYSQL_TYPE_TINY as u8
        || col_type == ColumnType::MYSQL_TYPE_SHORT as u8
        || col_type == ColumnType::MYSQL_TYPE_INT24 as u8
        || col_type == ColumnType::MYSQL_TYPE_LONG as u8
        || col_type == ColumnType::MYSQL_TYPE_LONGLONG as u8
        || col_type == ColumnType::MYSQL_TYPE_YEAR as u8
    {
        return ArrowColKind::Int64;
    }
    if col_type == ColumnType::MYSQL_TYPE_FLOAT as u8
        || col_type == ColumnType::MYSQL_TYPE_DOUBLE as u8
    {
        return ArrowColKind::Float64;
    }
    if col_type == ColumnType::MYSQL_TYPE_DECIMAL as u8
        || col_type == ColumnType::MYSQL_TYPE_NEWDECIMAL as u8
    {
        return ArrowColKind::Decimal128;
    }
    if col_type == ColumnType::MYSQL_TYPE_DATE as u8
        || col_type == ColumnType::MYSQL_TYPE_NEWDATE as u8
    {
        return ArrowColKind::Date32;
    }
    if col_type == ColumnType::MYSQL_TYPE_DATETIME as u8
        || col_type == ColumnType::MYSQL_TYPE_TIMESTAMP as u8
        || col_type == ColumnType::MYSQL_TYPE_DATETIME2 as u8
        || col_type == ColumnType::MYSQL_TYPE_TIMESTAMP2 as u8
    {
        return ArrowColKind::TimestampMicros;
    }
    if col_type == ColumnType::MYSQL_TYPE_TIME as u8
        || col_type == ColumnType::MYSQL_TYPE_TIME2 as u8
    {
        return ArrowColKind::Time64Micros;
    }
    if col_type == ColumnType::MYSQL_TYPE_BIT as u8
        || col_type == ColumnType::MYSQL_TYPE_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_TINY_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_MEDIUM_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_LONG_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_VECTOR as u8
    {
        return ArrowColKind::Binary;
    }
    ArrowColKind::Utf8
}

/// Convert a Gregorian `(year, month, day)` triple into days since `1970-01-01`.
#[inline]
pub fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d.saturating_sub(1);
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + (doe as i32) - 719468
}

/// Convert days since `1970-01-01` back into a Gregorian `(year, month, day)` triple.
#[inline]
pub fn civil_from_days(z: i32) -> (i32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i32) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year, m, d)
}

fn parse_decimal128_scaled(text: &str, scale: u32) -> Option<i128> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (is_neg, body) = if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = trimmed.strip_prefix('+') {
        (false, rest)
    } else {
        (false, trimmed)
    };
    let (int_str, frac_str) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    let int_val: i128 = if int_str.is_empty() {
        0
    } else {
        int_str.parse::<i128>().ok()?
    };
    let factor = 10_i128.checked_pow(scale)?;
    let mut padded_frac = String::with_capacity(scale as usize);
    for ch in frac_str.chars().take(scale as usize) {
        if !ch.is_ascii_digit() {
            return None;
        }
        padded_frac.push(ch);
    }
    while padded_frac.len() < scale as usize {
        padded_frac.push('0');
    }
    let frac_val: i128 = if padded_frac.is_empty() {
        0
    } else {
        padded_frac.parse::<i128>().ok()?
    };
    let combined = int_val.checked_mul(factor)?.checked_add(frac_val)?;
    Some(if is_neg { -combined } else { combined })
}

fn format_decimal128_scaled(val: i128, scale: i8) -> String {
    if scale <= 0 {
        return val.to_string();
    }
    let scale_u = scale as u32;
    let factor = 10_i128.pow(scale_u);
    let sign = if val < 0 { "-" } else { "" };
    let abs = val.unsigned_abs();
    let int_part = abs / (factor as u128);
    let frac_part = abs % (factor as u128);
    if frac_part == 0 {
        format!("{sign}{int_part}")
    } else {
        let frac_formatted = format!("{frac_part:0width$}", width = scale_u as usize);
        let trimmed_frac = frac_formatted.trim_end_matches('0');
        format!("{sign}{int_part}.{trimmed_frac}")
    }
}

fn build_record_batch(
    schema: &Arc<Schema>,
    kinds: &[ArrowColKind],
    rows: &[Vec<DecodedCell>],
) -> Result<RecordBatch, MySQLError> {
    if schema.fields().is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    let num_rows = rows.len();
    let mut columns_out: Vec<ArrayRef> = Vec::with_capacity(kinds.len());

    for (col_idx, kind) in kinds.iter().enumerate() {
        match kind {
            ArrowColKind::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx).unwrap_or(&DecodedCell::Null) {
                        DecodedCell::Null => builder.append_null(),
                        DecodedCell::Bool(b) => builder.append_value(*b),
                        DecodedCell::Int(i) => builder.append_value(*i != 0),
                        DecodedCell::UInt(u) => builder.append_value(*u != 0),
                        _ => builder.append_null(),
                    }
                }
                columns_out.push(Arc::new(builder.finish()));
            }
            ArrowColKind::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx).unwrap_or(&DecodedCell::Null) {
                        DecodedCell::Null => builder.append_null(),
                        DecodedCell::Int(i) => builder.append_value(*i),
                        DecodedCell::UInt(u) => builder.append_value(*u as i64),
                        DecodedCell::Bool(b) => builder.append_value(if *b { 1 } else { 0 }),
                        DecodedCell::Text(s) => match s.trim().parse::<i64>() {
                            Ok(v) => builder.append_value(v),
                            Err(_) => builder.append_null(),
                        },
                        _ => builder.append_null(),
                    }
                }
                columns_out.push(Arc::new(builder.finish()));
            }
            ArrowColKind::UInt64 => {
                let mut builder = UInt64Builder::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx).unwrap_or(&DecodedCell::Null) {
                        DecodedCell::Null => builder.append_null(),
                        DecodedCell::UInt(u) => builder.append_value(*u),
                        DecodedCell::Int(i) if *i >= 0 => builder.append_value(*i as u64),
                        DecodedCell::Bool(b) => builder.append_value(if *b { 1 } else { 0 }),
                        _ => builder.append_null(),
                    }
                }
                columns_out.push(Arc::new(builder.finish()));
            }
            ArrowColKind::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx).unwrap_or(&DecodedCell::Null) {
                        DecodedCell::Null => builder.append_null(),
                        DecodedCell::Float(f) => builder.append_value(*f),
                        DecodedCell::Int(i) => builder.append_value(*i as f64),
                        DecodedCell::UInt(u) => builder.append_value(*u as f64),
                        DecodedCell::Decimal(s) | DecodedCell::Text(s) => {
                            match s.trim().parse::<f64>() {
                                Ok(v) => builder.append_value(v),
                                Err(_) => builder.append_null(),
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                columns_out.push(Arc::new(builder.finish()));
            }
            ArrowColKind::Decimal128 => {
                let mut builder = Decimal128Builder::with_capacity(num_rows)
                    .with_data_type(DataType::Decimal128(38, MYSQL_ARROW_DECIMAL_SCALE));
                for row in rows {
                    match row.get(col_idx).unwrap_or(&DecodedCell::Null) {
                        DecodedCell::Null => builder.append_null(),
                        DecodedCell::Decimal(s) | DecodedCell::Text(s) => {
                            if let Some(scaled) =
                                parse_decimal128_scaled(s, MYSQL_ARROW_DECIMAL_SCALE as u32)
                            {
                                builder.append_value(scaled);
                            } else {
                                builder.append_null();
                            }
                        }
                        DecodedCell::Int(i) => {
                            let factor = 10_i128.pow(MYSQL_ARROW_DECIMAL_SCALE as u32);
                            builder.append_value((*i as i128) * factor);
                        }
                        DecodedCell::UInt(u) => {
                            let factor = 10_i128.pow(MYSQL_ARROW_DECIMAL_SCALE as u32);
                            builder.append_value((*u as i128) * factor);
                        }
                        _ => builder.append_null(),
                    }
                }
                columns_out.push(Arc::new(builder.finish()));
            }
            ArrowColKind::Date32 => {
                let mut builder = Date32Builder::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx).unwrap_or(&DecodedCell::Null) {
                        DecodedCell::Null => builder.append_null(),
                        DecodedCell::Date(y, m, d) => {
                            builder.append_value(days_from_civil(*y, *m, *d));
                        }
                        DecodedCell::DateTime(y, m, d, ..) => {
                            builder.append_value(days_from_civil(*y, *m, *d));
                        }
                        _ => builder.append_null(),
                    }
                }
                columns_out.push(Arc::new(builder.finish()));
            }
            ArrowColKind::TimestampMicros => {
                let mut builder =
                    TimestampMicrosecondBuilder::with_capacity(num_rows).with_timezone("UTC");
                for row in rows {
                    match row.get(col_idx).unwrap_or(&DecodedCell::Null) {
                        DecodedCell::Null => builder.append_null(),
                        DecodedCell::DateTime(y, m, d, hh, mm, ss, us) => {
                            let days = days_from_civil(*y, *m, *d) as i64;
                            let secs = days * 86_400
                                + (*hh as i64) * 3_600
                                + (*mm as i64) * 60
                                + (*ss as i64);
                            builder.append_value(secs * 1_000_000 + (*us as i64));
                        }
                        DecodedCell::Date(y, m, d) => {
                            let days = days_from_civil(*y, *m, *d) as i64;
                            builder.append_value(days * 86_400 * 1_000_000);
                        }
                        _ => builder.append_null(),
                    }
                }
                columns_out.push(Arc::new(builder.finish()));
            }
            ArrowColKind::Time64Micros => {
                let mut builder = Time64MicrosecondBuilder::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx).unwrap_or(&DecodedCell::Null) {
                        DecodedCell::Null => builder.append_null(),
                        DecodedCell::Time(is_neg, days, hh, mm, ss, us) => {
                            let secs = (*days as i64) * 86_400
                                + (*hh as i64) * 3_600
                                + (*mm as i64) * 60
                                + (*ss as i64);
                            let abs_us = secs * 1_000_000 + (*us as i64);
                            builder.append_value(if *is_neg { -abs_us } else { abs_us });
                        }
                        _ => builder.append_null(),
                    }
                }
                columns_out.push(Arc::new(builder.finish()));
            }
            ArrowColKind::Binary => {
                let mut builder = BinaryBuilder::with_capacity(num_rows, num_rows * 16);
                for row in rows {
                    match row.get(col_idx).unwrap_or(&DecodedCell::Null) {
                        DecodedCell::Null => builder.append_null(),
                        DecodedCell::Bytes(b) => builder.append_value(b.as_slice()),
                        DecodedCell::Text(s) | DecodedCell::Json(s) | DecodedCell::Decimal(s) => {
                            builder.append_value(s.as_bytes());
                        }
                        _ => builder.append_null(),
                    }
                }
                columns_out.push(Arc::new(builder.finish()));
            }
            ArrowColKind::Utf8 => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                for row in rows {
                    match row.get(col_idx).unwrap_or(&DecodedCell::Null) {
                        DecodedCell::Null => builder.append_null(),
                        DecodedCell::Text(s) | DecodedCell::Json(s) | DecodedCell::Decimal(s) => {
                            builder.append_value(s.as_str());
                        }
                        DecodedCell::Int(i) => builder.append_value(i.to_string()),
                        DecodedCell::UInt(u) => builder.append_value(u.to_string()),
                        DecodedCell::Float(f) => builder.append_value(f.to_string()),
                        DecodedCell::Bool(b) => builder.append_value(if *b { "true" } else { "false" }),
                        DecodedCell::Bytes(b) => {
                            builder.append_value(String::from_utf8_lossy(b));
                        }
                        DecodedCell::Date(y, m, d) => {
                            builder.append_value(format!("{y:04}-{m:02}-{d:02}"));
                        }
                        DecodedCell::DateTime(y, m, d, hh, mm, ss, us) => {
                            if *us > 0 {
                                builder.append_value(format!(
                                    "{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{us:06}"
                                ));
                            } else {
                                builder.append_value(format!(
                                    "{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}"
                                ));
                            }
                        }
                        DecodedCell::Time(is_neg, days, hh, mm, ss, us) => {
                            let sign = if *is_neg { "-" } else { "" };
                            let total_hh = days * 24 + hh;
                            if *us > 0 {
                                builder.append_value(format!(
                                    "{sign}{total_hh:02}:{mm:02}:{ss:02}.{us:06}"
                                ));
                            } else {
                                builder.append_value(format!("{sign}{total_hh:02}:{mm:02}:{ss:02}"));
                            }
                        }
                    }
                }
                columns_out.push(Arc::new(builder.finish()));
            }
        }
    }

    RecordBatch::try_new(Arc::clone(schema), columns_out)
        .map_err(|e| MySQLError::protocol(format!("Failed to build Arrow RecordBatch: {e}")))
}

/// Serialize decoded MySQL result-set columns and rows into an Arrow IPC stream (`Vec<u8>`).
pub fn encode_result_set_to_arrow_ipc(
    columns: &[(String, u8)],
    rows: &[Vec<DecodedCell>],
    batch_size: usize,
) -> Result<Vec<u8>, MySQLError> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut kinds = Vec::with_capacity(columns.len());
    for (col_idx, (col_name, col_type)) in columns.iter().enumerate() {
        let kind = infer_column_kind(*col_type, rows, col_idx);
        fields.push(Field::new(col_name.as_str(), kind.data_type(), true));
        kinds.push(kind);
    }
    let schema = Arc::new(Schema::new(fields));
    let mut ipc_bytes = Vec::with_capacity(1024 + rows.len() * 32);
    {
        let mut writer = StreamWriter::try_new(&mut ipc_bytes, &schema).map_err(|e| {
            MySQLError::protocol(format!("Failed to initialize Arrow IPC StreamWriter: {e}"))
        })?;
        let chunk_len = batch_size.max(1);
        if rows.is_empty() {
            let empty_batch = build_record_batch(&schema, &kinds, &[])?;
            writer.write(&empty_batch).map_err(|e| {
                MySQLError::protocol(format!("Failed to write empty Arrow RecordBatch: {e}"))
            })?;
        } else {
            for chunk in rows.chunks(chunk_len) {
                let batch = build_record_batch(&schema, &kinds, chunk)?;
                writer.write(&batch).map_err(|e| {
                    MySQLError::protocol(format!("Failed to write Arrow RecordBatch: {e}"))
                })?;
            }
        }
        writer.finish().map_err(|e| {
            MySQLError::protocol(format!("Failed to finish Arrow IPC stream: {e}"))
        })?;
    }
    Ok(ipc_bytes)
}

/// Encode a decoded MySQL result set into a single `VTBARROW` POSIX shared-memory segment.
pub fn encode_result_set_to_shm(
    columns: &[(String, u8)],
    rows: &[Vec<DecodedCell>],
    batch_size: usize,
) -> Result<ShmArrowBatchHandle, MySQLError> {
    let ipc_bytes = encode_result_set_to_arrow_ipc(columns, rows, batch_size)?;
    let (shm_name, byte_len, region) = write_mysql_shm_frame(&ipc_bytes)?;
    Ok((shm_name, byte_len, rows.len(), region))
}

/// Encode a decoded MySQL result set into chunked `VTBARROW` POSIX shared-memory batch segments.
pub fn encode_result_set_to_shm_batches(
    columns: &[(String, u8)],
    rows: &[Vec<DecodedCell>],
    batch_size: usize,
) -> Result<Vec<ShmArrowBatchHandle>, MySQLError> {
    let chunk_len = batch_size.max(1);
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut batches = Vec::with_capacity(rows.len().div_ceil(chunk_len));
    for chunk in rows.chunks(chunk_len) {
        let ipc_bytes = encode_result_set_to_arrow_ipc(columns, chunk, chunk_len)?;
        let (shm_name, byte_len, region) = write_mysql_shm_frame(&ipc_bytes)?;
        batches.push((shm_name, byte_len, chunk.len(), region));
    }
    Ok(batches)
}

fn extract_cell_from_arrow_array(col: &dyn Array, row_idx: usize) -> Value {
    if col.is_null(row_idx) {
        return Value::NULL;
    }
    match col.data_type() {
        DataType::Boolean => {
            let b = col.as_boolean().value(row_idx);
            Value::Int(if b { 1 } else { 0 })
        }
        DataType::Int8 => Value::Int(col.as_primitive::<Int8Type>().value(row_idx) as i64),
        DataType::Int16 => Value::Int(col.as_primitive::<Int16Type>().value(row_idx) as i64),
        DataType::Int32 => Value::Int(col.as_primitive::<Int32Type>().value(row_idx) as i64),
        DataType::Int64 => Value::Int(col.as_primitive::<Int64Type>().value(row_idx)),
        DataType::UInt8 => Value::UInt(col.as_primitive::<UInt8Type>().value(row_idx) as u64),
        DataType::UInt16 => Value::UInt(col.as_primitive::<UInt16Type>().value(row_idx) as u64),
        DataType::UInt32 => Value::UInt(col.as_primitive::<UInt32Type>().value(row_idx) as u64),
        DataType::UInt64 => Value::UInt(col.as_primitive::<UInt64Type>().value(row_idx)),
        DataType::Float32 => {
            Value::Double(col.as_primitive::<Float32Type>().value(row_idx) as f64)
        }
        DataType::Float64 => Value::Double(col.as_primitive::<Float64Type>().value(row_idx)),
        DataType::Decimal128(_, scale) => {
            let raw = col.as_primitive::<Decimal128Type>().value(row_idx);
            let s = format_decimal128_scaled(raw, *scale);
            Value::Bytes(s.into_bytes())
        }
        DataType::Date32 => {
            let days = col.as_primitive::<Date32Type>().value(row_idx);
            let (y, m, d) = civil_from_days(days);
            Value::Date(y.max(0) as u16, m as u8, d as u8, 0, 0, 0, 0)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let total_us = col.as_primitive::<TimestampMicrosecondType>().value(row_idx);
            let secs = total_us.div_euclid(1_000_000);
            let us = total_us.rem_euclid(1_000_000) as u32;
            let days = secs.div_euclid(86_400) as i32;
            let rem_secs = secs.rem_euclid(86_400) as u32;
            let hh = (rem_secs / 3_600) as u8;
            let mm = ((rem_secs % 3_600) / 60) as u8;
            let ss = (rem_secs % 60) as u8;
            let (y, m, d) = civil_from_days(days);
            Value::Date(y.max(0) as u16, m as u8, d as u8, hh, mm, ss, us)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let signed_us = col.as_primitive::<Time64MicrosecondType>().value(row_idx);
            let is_neg = signed_us < 0;
            let abs_us = signed_us.unsigned_abs();
            let us = (abs_us % 1_000_000) as u32;
            let total_secs = abs_us / 1_000_000;
            let days = (total_secs / 86_400) as u32;
            let rem = total_secs % 86_400;
            let hh = (rem / 3_600) as u8;
            let mm = ((rem % 3_600) / 60) as u8;
            let ss = (rem % 60) as u8;
            Value::Time(is_neg, days, hh, mm, ss, micros_clamp(us))
        }
        DataType::Binary => Value::Bytes(col.as_binary::<i32>().value(row_idx).to_vec()),
        DataType::LargeBinary => Value::Bytes(col.as_binary::<i64>().value(row_idx).to_vec()),
        DataType::Utf8 => Value::Bytes(col.as_string::<i32>().value(row_idx).as_bytes().to_vec()),
        DataType::LargeUtf8 => {
            Value::Bytes(col.as_string::<i64>().value(row_idx).as_bytes().to_vec())
        }
        _ => Value::NULL,
    }
}

#[inline]
fn micros_clamp(us: u32) -> u32 {
    us.min(999_999)
}

/// Decode an Arrow IPC stream (or `VTBARROW`-prefixed shared-memory frame) into column names and
/// rows of `mysql_async::Value`.
pub fn decode_arrow_ipc_to_mysql_rows(
    ipc_or_vtbarrow: &[u8],
    requested_columns: Option<&[String]>,
) -> Result<(Vec<String>, Vec<Vec<Value>>), MySQLError> {
    if ipc_or_vtbarrow.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let payload = if ipc_or_vtbarrow.starts_with(SHM_ARROW_MAGIC) {
        let header =
            ShmArrowFrameHeader::from_slice(ipc_or_vtbarrow).map_err(MySQLError::protocol)?;
        let len = usize::try_from(header.byte_len)
            .map_err(|_| MySQLError::protocol("VTBARROW byte_len overflow"))?;
        let end = ShmArrowFrameHeader::SIZE
            .checked_add(len)
            .filter(|&e| e <= ipc_or_vtbarrow.len())
            .ok_or_else(|| MySQLError::protocol("VTBARROW frame exceeds buffer length"))?;
        &ipc_or_vtbarrow[ShmArrowFrameHeader::SIZE..end]
    } else {
        ipc_or_vtbarrow
    };

    let reader = StreamReader::try_new(Cursor::new(payload), None)
        .map_err(|e| MySQLError::protocol(format!("Invalid Arrow IPC stream: {e}")))?;
    let schema = reader.schema();
    let schema_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let (target_cols, col_positions): (Vec<String>, Vec<Option<usize>>) =
        if let Some(req) = requested_columns {
            let positions = req
                .iter()
                .map(|c| schema_names.iter().position(|s| s == c))
                .collect();
            (req.to_vec(), positions)
        } else {
            let positions = (0..schema_names.len()).map(Some).collect();
            (schema_names, positions)
        };

    let mut out_rows = Vec::new();
    for batch_res in reader {
        let batch = batch_res.map_err(|e| {
            MySQLError::protocol(format!("Failed to decode Arrow RecordBatch: {e}"))
        })?;
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            let mut row = Vec::with_capacity(col_positions.len());
            for pos_opt in &col_positions {
                if let Some(col_idx) = pos_opt {
                    let array = batch.column(*col_idx);
                    row.push(extract_cell_from_arrow_array(array.as_ref(), row_idx));
                } else {
                    row.push(Value::NULL);
                }
            }
            out_rows.push(row);
        }
    }

    Ok((target_cols, out_rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_name_length_and_prefix() {
        let name = next_mysql_shm_name();
        assert!(name.starts_with("/vtb-my-"));
        assert!(name.len() <= 31, "SHM name too long: {} ({})", name, name.len());
    }

    #[test]
    fn test_encode_and_decode_arrow_ipc_roundtrip_all_types() {
        let columns = vec![
            ("id".to_string(), ColumnType::MYSQL_TYPE_LONGLONG as u8),
            ("flag".to_string(), ColumnType::MYSQL_TYPE_TINY as u8),
            ("big_u".to_string(), ColumnType::MYSQL_TYPE_LONGLONG as u8),
            ("score".to_string(), ColumnType::MYSQL_TYPE_DOUBLE as u8),
            (
                "price".to_string(),
                ColumnType::MYSQL_TYPE_NEWDECIMAL as u8,
            ),
            ("d_val".to_string(), ColumnType::MYSQL_TYPE_DATE as u8),
            (
                "dt_val".to_string(),
                ColumnType::MYSQL_TYPE_DATETIME as u8,
            ),
            ("t_val".to_string(), ColumnType::MYSQL_TYPE_TIME as u8),
            ("raw_bin".to_string(), ColumnType::MYSQL_TYPE_BLOB as u8),
            (
                "label".to_string(),
                ColumnType::MYSQL_TYPE_VAR_STRING as u8,
            ),
        ];
        let rows = vec![
            vec![
                DecodedCell::Int(1),
                DecodedCell::Bool(true),
                DecodedCell::UInt(10_000_000_000_000_000_000),
                DecodedCell::Float(12.5),
                DecodedCell::Decimal("99.125".to_string()),
                DecodedCell::Date(2026, 11, 9),
                DecodedCell::DateTime(2026, 11, 9, 15, 30, 0, 500_000),
                DecodedCell::Time(false, 0, 8, 15, 30, 0),
                DecodedCell::Bytes(vec![1, 2, 3]),
                DecodedCell::Text("alpha".to_string()),
            ],
            vec![
                DecodedCell::Int(2),
                DecodedCell::Null,
                DecodedCell::Null,
                DecodedCell::Null,
                DecodedCell::Null,
                DecodedCell::Null,
                DecodedCell::Null,
                DecodedCell::Null,
                DecodedCell::Null,
                DecodedCell::Text("beta".to_string()),
            ],
        ];

        let (shm_name, byte_len, row_count, region) =
            encode_result_set_to_shm(&columns, &rows, 1024).expect("encode to shm");
        assert!(shm_name.starts_with("/vtb-my-"));
        assert!(byte_len > 0);
        assert_eq!(row_count, 2);

        // SAFETY: `region` was initialized by `encode_result_set_to_shm` and is immutable.
        let slice = unsafe { region.as_slice() };
        let (decoded_cols, decoded_rows) =
            decode_arrow_ipc_to_mysql_rows(&slice[..ShmArrowFrameHeader::SIZE + byte_len], None)
                .expect("decode from vtbarrow frame");
        assert_eq!(decoded_cols.len(), 10);
        assert_eq!(decoded_rows.len(), 2);
        assert_eq!(decoded_rows[0][0], Value::Int(1));
        assert_eq!(decoded_rows[0][4], Value::Bytes(b"99.125".to_vec()));
        assert_eq!(decoded_rows[0][5], Value::Date(2026, 11, 9, 0, 0, 0, 0));
        assert_eq!(
            decoded_rows[0][6],
            Value::Date(2026, 11, 9, 15, 30, 0, 500_000)
        );
        assert_eq!(decoded_rows[0][9], Value::Bytes(b"alpha".to_vec()));
        assert_eq!(decoded_rows[1][1], Value::NULL);
        assert_eq!(decoded_rows[1][9], Value::Bytes(b"beta".to_vec()));
    }
}
