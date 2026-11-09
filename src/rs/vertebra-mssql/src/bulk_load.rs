//! Arrow IPC to TDS `BulkLoadRequest` (`0x07` `BULK_LOAD`) ingestion for `vertebra-mssql`.

use std::borrow::Cow;
use std::io::Cursor;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, NaiveDate};
use tiberius::{ColumnData, TokenRow};
use vertebra_db::arrow_shm::read_shm_arrow_frame_with;

use crate::errors::MssqlError;
use crate::params::MssqlParam;

const UNIX_EPOCH_CE_DAYS: i32 = 719_163;

/// Decode an Arrow IPC stream byte slice into a vector of [`RecordBatch`]es.
pub fn decode_arrow_ipc_batches(ipc_bytes: &[u8]) -> Result<Vec<RecordBatch>, MssqlError> {
    let reader = StreamReader::try_new(Cursor::new(ipc_bytes), None).map_err(|e| {
        MssqlError::data(format!("failed to decode Arrow IPC stream for bulk load: {e}"))
    })?;
    let mut batches = Vec::new();
    for item in reader {
        let batch = item.map_err(|e| {
            MssqlError::data(format!("corrupted RecordBatch in Arrow IPC stream: {e}"))
        })?;
        batches.push(batch);
    }
    Ok(batches)
}

/// Read an Arrow IPC stream from a `VTBARROW` shared-memory segment (`shm_name`) and decode its [`RecordBatch`]es.
pub fn decode_shm_arrow_batches(shm_name: &str) -> Result<Vec<RecordBatch>, MssqlError> {
    read_shm_arrow_frame_with(shm_name, decode_arrow_ipc_batches)
        .map_err(MssqlError::data)?
}

/// Convert an Arrow [`RecordBatch`] into owned [`TokenRow<'static>`] rows suitable for
/// `tiberius::BulkLoadRequest::send`.
pub fn record_batch_to_token_rows(batch: &RecordBatch) -> Result<Vec<TokenRow<'static>>, MssqlError> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows: Vec<TokenRow<'static>> = (0..num_rows)
        .map(|_| TokenRow::with_capacity(num_cols))
        .collect();

    for col_idx in 0..num_cols {
        let col = batch.column(col_idx);
        for (row_idx, row_out) in rows.iter_mut().enumerate() {
            let cell = arrow_cell_to_column_data(col.as_ref(), row_idx)?;
            row_out.push(cell);
        }
    }
    Ok(rows)
}

/// Convert an Arrow [`RecordBatch`] into parameter rows (`Vec<Vec<MssqlParam>>`) and column names.
pub fn record_batch_to_param_rows(
    batch: &RecordBatch,
) -> Result<(Vec<String>, Vec<Vec<MssqlParam>>), MssqlError> {
    let schema = batch.schema();
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows: Vec<Vec<MssqlParam>> = (0..num_rows)
        .map(|_| Vec::with_capacity(num_cols))
        .collect();

    for col_idx in 0..num_cols {
        let col = batch.column(col_idx);
        for (row_idx, row_out) in rows.iter_mut().enumerate() {
            row_out.push(arrow_cell_to_param(col.as_ref(), row_idx)?);
        }
    }
    Ok((col_names, rows))
}

fn arrow_cell_to_column_data(
    col: &dyn Array,
    row_idx: usize,
) -> Result<ColumnData<'static>, MssqlError> {
    if col.is_null(row_idx) {
        return Ok(match col.data_type() {
            DataType::Boolean => ColumnData::Bit(None),
            DataType::Int8 | DataType::UInt8 => ColumnData::U8(None),
            DataType::Int16 | DataType::UInt16 => ColumnData::I16(None),
            DataType::Int32 | DataType::UInt32 => ColumnData::I32(None),
            DataType::Int64 | DataType::UInt64 => ColumnData::I64(None),
            DataType::Float32 => ColumnData::F32(None),
            DataType::Float64 => ColumnData::F64(None),
            DataType::Binary | DataType::LargeBinary => ColumnData::Binary(None),
            _ => ColumnData::String(None),
        });
    }

    match col.data_type() {
        DataType::Boolean => Ok(ColumnData::Bit(Some(col.as_boolean().value(row_idx)))),
        DataType::UInt8 => Ok(ColumnData::U8(Some(
            col.as_primitive::<UInt8Type>().value(row_idx),
        ))),
        DataType::Int8 => Ok(ColumnData::I16(Some(i16::from(
            col.as_primitive::<Int8Type>().value(row_idx),
        )))),
        DataType::Int16 => Ok(ColumnData::I16(Some(
            col.as_primitive::<Int16Type>().value(row_idx),
        ))),
        DataType::UInt16 => Ok(ColumnData::I32(Some(i32::from(
            col.as_primitive::<UInt16Type>().value(row_idx),
        )))),
        DataType::Int32 => Ok(ColumnData::I32(Some(
            col.as_primitive::<Int32Type>().value(row_idx),
        ))),
        DataType::UInt32 => Ok(ColumnData::I64(Some(i64::from(
            col.as_primitive::<UInt32Type>().value(row_idx),
        )))),
        DataType::Int64 => Ok(ColumnData::I64(Some(
            col.as_primitive::<Int64Type>().value(row_idx),
        ))),
        DataType::UInt64 => {
            let v = col.as_primitive::<UInt64Type>().value(row_idx);
            let signed = i64::try_from(v).map_err(|_| {
                MssqlError::data(format!("UInt64 value {v} overflows SQL Server BIGINT"))
            })?;
            Ok(ColumnData::I64(Some(signed)))
        }
        DataType::Float32 => Ok(ColumnData::F32(Some(
            col.as_primitive::<Float32Type>().value(row_idx),
        ))),
        DataType::Float64 => Ok(ColumnData::F64(Some(
            col.as_primitive::<Float64Type>().value(row_idx),
        ))),
        DataType::Utf8 => Ok(ColumnData::String(Some(Cow::Owned(
            col.as_string::<i32>().value(row_idx).to_string(),
        )))),
        DataType::LargeUtf8 => Ok(ColumnData::String(Some(Cow::Owned(
            col.as_string::<i64>().value(row_idx).to_string(),
        )))),
        DataType::Binary => Ok(ColumnData::Binary(Some(Cow::Owned(
            col.as_binary::<i32>().value(row_idx).to_vec(),
        )))),
        DataType::LargeBinary => Ok(ColumnData::Binary(Some(Cow::Owned(
            col.as_binary::<i64>().value(row_idx).to_vec(),
        )))),
        DataType::Date32 => {
            let days = col.as_primitive::<Date32Type>().value(row_idx);
            let d = NaiveDate::from_num_days_from_ce_opt(days + UNIX_EPOCH_CE_DAYS)
                .ok_or_else(|| MssqlError::data("Date32 out of range"))?;
            Ok(ColumnData::String(Some(Cow::Owned(d.to_string()))))
        }
        DataType::Timestamp(unit, _) => {
            let micros = match unit {
                TimeUnit::Second => col.as_primitive::<TimestampSecondType>().value(row_idx) * 1_000_000,
                TimeUnit::Millisecond => col.as_primitive::<TimestampMillisecondType>().value(row_idx) * 1_000,
                TimeUnit::Microsecond => col.as_primitive::<TimestampMicrosecondType>().value(row_idx),
                TimeUnit::Nanosecond => col.as_primitive::<TimestampNanosecondType>().value(row_idx) / 1_000,
            };
            let dt = DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| MssqlError::data("Timestamp out of range"))?;
            Ok(ColumnData::String(Some(Cow::Owned(
                dt.naive_utc().format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
            ))))
        }
        other => Err(MssqlError::data(format!(
            "unsupported Arrow column data type for MSSQL bulk load: {other:?}"
        ))),
    }
}

fn arrow_cell_to_param(col: &dyn Array, row_idx: usize) -> Result<MssqlParam, MssqlError> {
    if col.is_null(row_idx) {
        return Ok(MssqlParam::null());
    }
    match col.data_type() {
        DataType::Boolean => Ok(MssqlParam::Bool(col.as_boolean().value(row_idx))),
        DataType::UInt8 => Ok(MssqlParam::I64(i64::from(
            col.as_primitive::<UInt8Type>().value(row_idx),
        ))),
        DataType::Int8 => Ok(MssqlParam::I64(i64::from(
            col.as_primitive::<Int8Type>().value(row_idx),
        ))),
        DataType::Int16 => Ok(MssqlParam::I64(i64::from(
            col.as_primitive::<Int16Type>().value(row_idx),
        ))),
        DataType::UInt16 => Ok(MssqlParam::I64(i64::from(
            col.as_primitive::<UInt16Type>().value(row_idx),
        ))),
        DataType::Int32 => Ok(MssqlParam::I64(i64::from(
            col.as_primitive::<Int32Type>().value(row_idx),
        ))),
        DataType::UInt32 => Ok(MssqlParam::I64(i64::from(
            col.as_primitive::<UInt32Type>().value(row_idx),
        ))),
        DataType::Int64 => Ok(MssqlParam::I64(
            col.as_primitive::<Int64Type>().value(row_idx),
        )),
        DataType::UInt64 => {
            let v = col.as_primitive::<UInt64Type>().value(row_idx);
            let signed = i64::try_from(v).map_err(|_| {
                MssqlError::data(format!("UInt64 value {v} overflows SQL Server BIGINT"))
            })?;
            Ok(MssqlParam::I64(signed))
        }
        DataType::Float32 => Ok(MssqlParam::F64(f64::from(
            col.as_primitive::<Float32Type>().value(row_idx),
        ))),
        DataType::Float64 => Ok(MssqlParam::F64(
            col.as_primitive::<Float64Type>().value(row_idx),
        )),
        DataType::Utf8 => Ok(MssqlParam::String(
            col.as_string::<i32>().value(row_idx).to_string(),
        )),
        DataType::LargeUtf8 => Ok(MssqlParam::String(
            col.as_string::<i64>().value(row_idx).to_string(),
        )),
        DataType::Binary => Ok(MssqlParam::Bytes(
            col.as_binary::<i32>().value(row_idx).to_vec(),
        )),
        DataType::LargeBinary => Ok(MssqlParam::Bytes(
            col.as_binary::<i64>().value(row_idx).to_vec(),
        )),
        DataType::Date32 => {
            let days = col.as_primitive::<Date32Type>().value(row_idx);
            let d = NaiveDate::from_num_days_from_ce_opt(days + UNIX_EPOCH_CE_DAYS)
                .ok_or_else(|| MssqlError::data("Date32 out of range"))?;
            Ok(MssqlParam::Date(d))
        }
        DataType::Timestamp(unit, _) => {
            let micros = match unit {
                TimeUnit::Second => col.as_primitive::<TimestampSecondType>().value(row_idx) * 1_000_000,
                TimeUnit::Millisecond => col.as_primitive::<TimestampMillisecondType>().value(row_idx) * 1_000,
                TimeUnit::Microsecond => col.as_primitive::<TimestampMicrosecondType>().value(row_idx),
                TimeUnit::Nanosecond => col.as_primitive::<TimestampNanosecondType>().value(row_idx) / 1_000,
            };
            let dt = DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| MssqlError::data("Timestamp out of range"))?;
            Ok(MssqlParam::DateTime(dt.naive_utc()))
        }
        other => Err(MssqlError::data(format!(
            "unsupported Arrow column data type for MSSQL bulk load: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    use crate::arrow_export::encode_batch_to_ipc_stream;

    use super::*;

    #[test]
    fn arrow_batch_roundtrips_to_tds_token_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec![Some("alpha"), None])),
            ],
        )
        .unwrap();
        let ipc = encode_batch_to_ipc_stream(&batch).unwrap();
        let decoded = decode_arrow_ipc_batches(&ipc).unwrap();
        assert_eq!(decoded.len(), 1);
        let token_rows = record_batch_to_token_rows(&decoded[0]).unwrap();
        assert_eq!(token_rows.len(), 2);
    }
}
