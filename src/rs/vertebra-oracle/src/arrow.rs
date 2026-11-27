//! Crate-local Arrow 60 (`arrow-array`, `arrow-schema`, `arrow-ipc`) serialization to `VTBARROW` IPC bytes.
//!
//! `oracledb = "=26.0.0-beta.6"` aligns with workspace Arrow 60 (`=60.1.0`). This module serializes
//! `RecordBatch` streams into Arrow IPC stream bytes (`(Vec<u8>, usize)`), which are then framed
//! into 16-byte `VTBARROW` shared-memory segments via `vertebra-db`.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int8Array, Int16Array, Int32Array, RecordBatch,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

use crate::errors::OracleError;
use crate::types::{OracleCellValue, OracleColumn};

/// Normalize an Arrow 59 [`Schema`] by lowercasing field names when `lowercase_column_names` is `true`.
#[must_use]
pub fn normalize_arrow59_schema(schema: &Arc<Schema>, lowercase_column_names: bool) -> Arc<Schema> {
    if !lowercase_column_names {
        return Arc::clone(schema);
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            Field::new(
                f.name().to_ascii_lowercase(),
                f.data_type().clone(),
                f.is_nullable(),
            )
            .with_metadata(f.metadata().clone())
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Serialize a slice of Arrow 59 [`RecordBatch`] values into an Arrow IPC stream `(ipc_bytes, row_count)`.
pub fn batches_to_arrow_ipc(
    batches: &[RecordBatch],
    lowercase_column_names: bool,
) -> Result<(Vec<u8>, usize), OracleError> {
    if batches.is_empty() {
        let empty_schema = Arc::new(Schema::empty());
        let mut writer = StreamWriter::try_new(Vec::new(), &empty_schema).map_err(|e| {
            OracleError::data_error(format!("Arrow 59 StreamWriter init error: {e}"))
        })?;
        writer.finish().map_err(|e| {
            OracleError::data_error(format!("Arrow 59 StreamWriter finish error: {e}"))
        })?;
        let bytes = writer.into_inner().map_err(|e| {
            OracleError::data_error(format!("Arrow 59 StreamWriter buffer error: {e}"))
        })?;
        return Ok((bytes, 0));
    }

    let norm_schema = normalize_arrow59_schema(&batches[0].schema(), lowercase_column_names);
    let mut writer = StreamWriter::try_new(Vec::new(), &norm_schema)
        .map_err(|e| OracleError::data_error(format!("Arrow 59 StreamWriter init error: {e}")))?;
    let mut total_rows = 0usize;

    for batch in batches {
        let rebou_batch = if lowercase_column_names {
            RecordBatch::try_new(Arc::clone(&norm_schema), batch.columns().to_vec()).map_err(
                |e| OracleError::data_error(format!("Arrow 59 RecordBatch schema error: {e}")),
            )?
        } else {
            batch.clone()
        };
        total_rows = total_rows.saturating_add(rebou_batch.num_rows());
        writer.write(&rebou_batch).map_err(|e| {
            OracleError::data_error(format!("Arrow 59 StreamWriter write error: {e}"))
        })?;
    }

    writer
        .finish()
        .map_err(|e| OracleError::data_error(format!("Arrow 59 StreamWriter finish error: {e}")))?;
    let ipc_bytes = writer
        .into_inner()
        .map_err(|e| OracleError::data_error(format!("Arrow 59 StreamWriter buffer error: {e}")))?;
    Ok((ipc_bytes, total_rows))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnArrowKind {
    Int64,
    Float64,
    Boolean,
    Binary,
    Utf8,
}

fn infer_column_kind(
    col: &OracleColumn,
    rows: &[Vec<OracleCellValue>],
    col_idx: usize,
) -> ColumnArrowKind {
    for row in rows {
        if let Some(cell) = row.get(col_idx) {
            match cell {
                OracleCellValue::Null => continue,
                OracleCellValue::Int(_) => return ColumnArrowKind::Int64,
                OracleCellValue::Float(_) => return ColumnArrowKind::Float64,
                OracleCellValue::Bool(_) => return ColumnArrowKind::Boolean,
                OracleCellValue::Bytes(_) => return ColumnArrowKind::Binary,
                OracleCellValue::Str(_)
                | OracleCellValue::DecimalStr(_)
                | OracleCellValue::Date { .. }
                | OracleCellValue::Timestamp { .. }
                | OracleCellValue::Oson(_)
                | OracleCellValue::Vector(_) => return ColumnArrowKind::Utf8,
            }
        }
    }
    let upper = col.type_name.to_ascii_uppercase();
    if upper == "NUMBER" && col.scale == Some(0) {
        ColumnArrowKind::Int64
    } else if upper == "BINARY_FLOAT" || upper == "BINARY_DOUBLE" || upper == "FLOAT" {
        ColumnArrowKind::Float64
    } else if upper == "BOOLEAN" {
        ColumnArrowKind::Boolean
    } else if upper.contains("RAW") || upper == "BLOB" {
        ColumnArrowKind::Binary
    } else {
        ColumnArrowKind::Utf8
    }
}

fn build_record_batch_chunk(
    schema: &Arc<Schema>,
    kinds: &[ColumnArrowKind],
    chunk: &[Vec<OracleCellValue>],
) -> Result<RecordBatch, OracleError> {
    let num_rows = chunk.len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(kinds.len());

    for (col_idx, kind) in kinds.iter().copied().enumerate() {
        let array: ArrayRef = match kind {
            ColumnArrowKind::Int64 => {
                let mut b = Int64Builder::with_capacity(num_rows);
                for row in chunk {
                    match row.get(col_idx).unwrap_or(&OracleCellValue::Null) {
                        OracleCellValue::Null => b.append_null(),
                        OracleCellValue::Int(v) => b.append_value(*v),
                        OracleCellValue::Float(v) => b.append_value(*v as i64),
                        OracleCellValue::Bool(v) => b.append_value(i64::from(*v)),
                        OracleCellValue::DecimalStr(s) | OracleCellValue::Str(s) => {
                            if let Ok(parsed) = s.parse::<i64>() {
                                b.append_value(parsed);
                            } else {
                                b.append_null();
                            }
                        }
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ColumnArrowKind::Float64 => {
                let mut b = Float64Builder::with_capacity(num_rows);
                for row in chunk {
                    match row.get(col_idx).unwrap_or(&OracleCellValue::Null) {
                        OracleCellValue::Null => b.append_null(),
                        OracleCellValue::Float(v) => b.append_value(*v),
                        OracleCellValue::Int(v) => b.append_value(*v as f64),
                        OracleCellValue::DecimalStr(s) | OracleCellValue::Str(s) => {
                            if let Ok(parsed) = s.parse::<f64>() {
                                b.append_value(parsed);
                            } else {
                                b.append_null();
                            }
                        }
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ColumnArrowKind::Boolean => {
                let mut b = BooleanBuilder::with_capacity(num_rows);
                for row in chunk {
                    match row.get(col_idx).unwrap_or(&OracleCellValue::Null) {
                        OracleCellValue::Null => b.append_null(),
                        OracleCellValue::Bool(v) => b.append_value(*v),
                        OracleCellValue::Int(v) => b.append_value(*v != 0),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ColumnArrowKind::Binary => {
                let mut b = BinaryBuilder::with_capacity(num_rows, num_rows * 16);
                for row in chunk {
                    match row.get(col_idx).unwrap_or(&OracleCellValue::Null) {
                        OracleCellValue::Null => b.append_null(),
                        OracleCellValue::Bytes(bytes) => b.append_value(bytes),
                        OracleCellValue::Str(s) => b.append_value(s.as_bytes()),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ColumnArrowKind::Utf8 => {
                let mut b = StringBuilder::with_capacity(num_rows, num_rows * 16);
                for row in chunk {
                    match row.get(col_idx).unwrap_or(&OracleCellValue::Null) {
                        OracleCellValue::Null => b.append_null(),
                        OracleCellValue::Str(s) | OracleCellValue::DecimalStr(s) => {
                            b.append_value(s)
                        }
                        OracleCellValue::Int(v) => b.append_value(v.to_string()),
                        OracleCellValue::Float(v) => b.append_value(v.to_string()),
                        OracleCellValue::Bool(v) => {
                            b.append_value(if *v { "true" } else { "false" })
                        }
                        OracleCellValue::Date { year, month, day } => {
                            b.append_value(format!("{year:04}-{month:02}-{day:02}"));
                        }
                        OracleCellValue::Timestamp {
                            year,
                            month,
                            day,
                            hour,
                            minute,
                            second,
                            nanosecond,
                            ..
                        } => {
                            let micro = nanosecond / 1_000;
                            b.append_value(format!(
                                "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micro:06}"
                            ));
                        }
                        OracleCellValue::Oson(bytes) | OracleCellValue::Bytes(bytes) => {
                            b.append_value(String::from_utf8_lossy(bytes));
                        }
                        OracleCellValue::Vector(vec_val) => {
                            b.append_value(format!("{vec_val:?}"));
                        }
                    }
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(array);
    }

    RecordBatch::try_new(Arc::clone(schema), arrays)
        .map_err(|e| OracleError::data_error(format!("Arrow 59 RecordBatch build error: {e}")))
}

/// Convert decoded [`OracleColumn`] metadata and [`OracleCellValue`] rows into an Arrow 59 IPC stream.
pub fn rows_to_arrow_ipc(
    columns: &[OracleColumn],
    rows: &[Vec<OracleCellValue>],
    lowercase_column_names: bool,
) -> Result<(Vec<u8>, usize), OracleError> {
    rows_to_arrow_ipc_chunked(columns, rows, lowercase_column_names, 8192)
}

/// Convert decoded [`OracleColumn`] metadata and [`OracleCellValue`] rows into a chunked Arrow 59 IPC stream.
pub fn rows_to_arrow_ipc_chunked(
    columns: &[OracleColumn],
    rows: &[Vec<OracleCellValue>],
    lowercase_column_names: bool,
    batch_size: usize,
) -> Result<(Vec<u8>, usize), OracleError> {
    if columns.is_empty() && rows.is_empty() {
        return batches_to_arrow_ipc(&[], lowercase_column_names);
    }

    let effective_cols: Vec<OracleColumn> = if !columns.is_empty() {
        columns.to_vec()
    } else if let Some(first) = rows.first() {
        (0..first.len())
            .map(|i| {
                OracleColumn::new(
                    format!("col_{i}"),
                    "VARCHAR2".to_owned(),
                    1,
                    None,
                    None,
                    None,
                    true,
                )
            })
            .collect()
    } else {
        Vec::new()
    };

    let kinds: Vec<ColumnArrowKind> = effective_cols
        .iter()
        .enumerate()
        .map(|(idx, col)| infer_column_kind(col, rows, idx))
        .collect();

    let fields: Vec<Field> = effective_cols
        .iter()
        .zip(kinds.iter())
        .map(|(col, kind)| {
            let dt = match kind {
                ColumnArrowKind::Int64 => DataType::Int64,
                ColumnArrowKind::Float64 => DataType::Float64,
                ColumnArrowKind::Boolean => DataType::Boolean,
                ColumnArrowKind::Binary => DataType::Binary,
                ColumnArrowKind::Utf8 => DataType::Utf8,
            };
            let name = if lowercase_column_names {
                col.name.to_ascii_lowercase()
            } else {
                col.name.clone()
            };
            Field::new(name, dt, col.nullable)
        })
        .collect();

    let schema = Arc::new(Schema::new(fields));
    let chunk_len = batch_size.max(1);
    let mut batches = Vec::new();
    if rows.is_empty() {
        batches.push(build_record_batch_chunk(&schema, &kinds, &[])?);
    } else {
        for chunk in rows.chunks(chunk_len) {
            batches.push(build_record_batch_chunk(&schema, &kinds, chunk)?);
        }
    }

    batches_to_arrow_ipc(&batches, lowercase_column_names)
}

fn try_promote_column_to_int64(field: &Field, col: &ArrayRef) -> Option<(Field, ArrayRef)> {
    let len = col.len();
    match field.data_type() {
        DataType::Int8 => {
            let arr = col.as_any().downcast_ref::<Int8Array>()?;
            let mut b = Int64Builder::with_capacity(len);
            for i in 0..len {
                if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(i64::from(arr.value(i)));
                }
            }
            let new_field = Field::new(field.name(), DataType::Int64, field.is_nullable())
                .with_metadata(field.metadata().clone());
            Some((new_field, Arc::new(b.finish())))
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>()?;
            let mut b = Int64Builder::with_capacity(len);
            for i in 0..len {
                if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(i64::from(arr.value(i)));
                }
            }
            let new_field = Field::new(field.name(), DataType::Int64, field.is_nullable())
                .with_metadata(field.metadata().clone());
            Some((new_field, Arc::new(b.finish())))
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>()?;
            let mut b = Int64Builder::with_capacity(len);
            for i in 0..len {
                if arr.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(i64::from(arr.value(i)));
                }
            }
            let new_field = Field::new(field.name(), DataType::Int64, field.is_nullable())
                .with_metadata(field.metadata().clone());
            Some((new_field, Arc::new(b.finish())))
        }
        DataType::Decimal128(_, 0) => {
            let arr = col.as_any().downcast_ref::<Decimal128Array>()?;
            let mut b = Int64Builder::with_capacity(len);
            for i in 0..len {
                if arr.is_null(i) {
                    b.append_null();
                } else {
                    let v = i64::try_from(arr.value(i)).ok()?;
                    b.append_value(v);
                }
            }
            let new_field = Field::new(field.name(), DataType::Int64, field.is_nullable())
                .with_metadata(field.metadata().clone());
            Some((new_field, Arc::new(b.finish())))
        }
        DataType::Float64
            if field
                .metadata()
                .get("oracle.type-name")
                .is_some_and(|t| t.eq_ignore_ascii_case("NUMBER")) =>
        {
            const MAX_SAFE_F64_INT: f64 = 9_007_199_254_740_992.0;
            let arr = col.as_any().downcast_ref::<Float64Array>()?;
            if len == 0 || arr.null_count() == len {
                return None;
            }
            let mut b = Int64Builder::with_capacity(len);
            for i in 0..len {
                if arr.is_null(i) {
                    b.append_null();
                } else {
                    let v = arr.value(i);
                    if !v.is_finite() || v.fract() != 0.0 || v.abs() > MAX_SAFE_F64_INT {
                        return None;
                    }
                    b.append_value(v as i64);
                }
            }
            let new_field = Field::new(field.name(), DataType::Int64, field.is_nullable())
                .with_metadata(field.metadata().clone());
            Some((new_field, Arc::new(b.finish())))
        }
        _ => None,
    }
}

/// Normalize a native `oracledb` Arrow 59 [`RecordBatch`] and serialize into chunked Arrow IPC bytes.
pub fn oracledb_batch_to_arrow_ipc_chunked(
    batch: &RecordBatch,
    lowercase_column_names: bool,
    batch_size: usize,
) -> Result<(Vec<u8>, usize), OracleError> {
    let orig_schema = batch.schema();
    if orig_schema.fields().is_empty() && batch.num_rows() == 0 {
        return batches_to_arrow_ipc(&[], lowercase_column_names);
    }

    let mut fields = Vec::with_capacity(orig_schema.fields().len());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (idx, field_arc) in orig_schema.fields().iter().enumerate() {
        let col = batch.column(idx);
        if let Some((new_field, new_col)) = try_promote_column_to_int64(field_arc.as_ref(), col) {
            fields.push(new_field);
            columns.push(new_col);
        } else {
            fields.push(field_arc.as_ref().clone());
            columns.push(Arc::clone(col));
        }
    }

    let norm_schema = Arc::new(Schema::new_with_metadata(
        fields,
        orig_schema.metadata().clone(),
    ));
    let norm_batch = RecordBatch::try_new(norm_schema, columns).map_err(|e| {
        OracleError::data_error(format!("Arrow 59 RecordBatch normalize error: {e}"))
    })?;

    let chunk_len = batch_size.max(1);
    let total_rows = norm_batch.num_rows();
    let mut slices = Vec::new();
    if total_rows == 0 {
        slices.push(norm_batch);
    } else {
        let mut offset = 0usize;
        while offset < total_rows {
            let len = (total_rows - offset).min(chunk_len);
            slices.push(norm_batch.slice(offset, len));
            offset += len;
        }
    }

    batches_to_arrow_ipc(&slices, lowercase_column_names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rows_to_arrow_ipc_and_schema_lowercase_normalization() {
        let cols = vec![
            OracleColumn::new(
                "ID".to_owned(),
                "NUMBER".to_owned(),
                2,
                Some(22),
                Some(10),
                Some(0),
                false,
            ),
            OracleColumn::new(
                "NAME".to_owned(),
                "VARCHAR2".to_owned(),
                1,
                Some(64),
                None,
                None,
                true,
            ),
        ];
        let rows = vec![
            vec![
                OracleCellValue::Int(1),
                OracleCellValue::Str("Ada".to_owned()),
            ],
            vec![
                OracleCellValue::Int(2),
                OracleCellValue::Str("Grace".to_owned()),
            ],
        ];
        let (ipc_bytes, row_count) = rows_to_arrow_ipc(&cols, &rows, true).expect("arrow ipc");
        assert_eq!(row_count, 2);
        assert!(!ipc_bytes.is_empty());
    }
}
