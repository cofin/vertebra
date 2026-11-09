//! Zero-copy `VTBARROW` POSIX shared-memory Arrow IPC materialization and `ShmCapsule` handoff
//! for `vertebra-mssql`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::Datelike;
use tiberius::ColumnType;
use vertebra_core::shm::ShmRegion;
use vertebra_db::arrow_shm::{ShmArrowStreamWriter, write_shm_arrow_region_unregistered};

use crate::codecs::{MssqlCellValue, MssqlColumnMeta, MssqlQueryResult};
use crate::errors::MssqlError;

/// Default initial byte capacity (`64 KiB`) when opening an MSSQL `VTBARROW` stream writer.
pub const DEFAULT_MSSQL_ARROW_SHM_CAPACITY: usize = 64 * 1024;

/// Tuple `(shm_name, payload_len, row_count, Arc<ShmRegion>)` returned by `VTBARROW` materializers.
pub type ShmArrowBatchPart = (String, usize, usize, Arc<ShmRegion>);

static MSSQL_SHM_COUNTER: AtomicU64 = AtomicU64::new(1);

const UNIX_EPOCH_CE_DAYS: i32 = 719_163;

/// Allocate the next unique process-scoped POSIX shared-memory name (`/vtb-ms-<pid>-<seq>`)
/// for an MSSQL `VTBARROW` result cursor.
#[inline]
pub fn next_mssql_shm_name() -> String {
    let seq = MSSQL_SHM_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("/vtb-ms-{pid:x}-{seq:x}")
}

/// Map a [`ColumnType`] and sample column values onto an Arrow [`DataType`].
pub fn column_to_arrow_data_type(
    col_type: ColumnType,
    rows: &[Vec<MssqlCellValue>],
    col_idx: usize,
) -> DataType {
    match col_type {
        ColumnType::Bit | ColumnType::Bitn => DataType::Boolean,
        ColumnType::Int1
        | ColumnType::Int2
        | ColumnType::Int4
        | ColumnType::Int8
        | ColumnType::Intn => DataType::Int64,
        ColumnType::Float4
        | ColumnType::Float8
        | ColumnType::Floatn
        | ColumnType::Money
        | ColumnType::Money4 => DataType::Float64,
        ColumnType::Daten => DataType::Date32,
        ColumnType::Datetime
        | ColumnType::Datetime4
        | ColumnType::Datetimen
        | ColumnType::Datetime2 => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::DatetimeOffsetn => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => DataType::Binary,
        ColumnType::Decimaln
        | ColumnType::Numericn
        | ColumnType::Guid
        | ColumnType::Timen
        | ColumnType::Xml
        | ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::Text
        | ColumnType::NText => DataType::Utf8,
        ColumnType::Null | ColumnType::Udt | ColumnType::SSVariant => {
            for row in rows {
                if let Some(cell) = row.get(col_idx) {
                    match cell {
                        MssqlCellValue::Null => continue,
                        MssqlCellValue::Bool(_) => return DataType::Boolean,
                        MssqlCellValue::Int64(_) => return DataType::Int64,
                        MssqlCellValue::Float64(_) => return DataType::Float64,
                        MssqlCellValue::Bytes(_) => return DataType::Binary,
                        MssqlCellValue::Date(_) => return DataType::Date32,
                        MssqlCellValue::DateTime(_) => {
                            return DataType::Timestamp(TimeUnit::Microsecond, None);
                        }
                        MssqlCellValue::DateTimeOffset(_) => {
                            return DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
                        }
                        MssqlCellValue::String(_)
                        | MssqlCellValue::Decimal(_)
                        | MssqlCellValue::Uuid(_)
                        | MssqlCellValue::Time(_)
                        | MssqlCellValue::Xml(_) => return DataType::Utf8,
                    }
                }
            }
            DataType::Utf8
        }
    }
}

/// Build an Arrow [`Schema`] for the given [`MssqlColumnMeta`] slice and sample rows.
pub fn build_arrow_schema(
    columns: &[MssqlColumnMeta],
    rows: &[Vec<MssqlCellValue>],
) -> Arc<Schema> {
    let fields: Vec<Field> = columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let dt = column_to_arrow_data_type(col.column_type, rows, idx);
            Field::new(&col.name, dt, true)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Convert a slice of decoded rows into an Arrow [`RecordBatch`].
pub fn rows_to_record_batch(
    schema: Arc<Schema>,
    rows: &[Vec<MssqlCellValue>],
) -> Result<RecordBatch, MssqlError> {
    if schema.fields().is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let num_rows = rows.len();
    let mut columns_out: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array: ArrayRef = match field.data_type() {
            DataType::Boolean => {
                let mut vals = Vec::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx) {
                        Some(MssqlCellValue::Bool(b)) => vals.push(Some(*b)),
                        Some(MssqlCellValue::Int64(n)) => vals.push(Some(*n != 0)),
                        _ => vals.push(None),
                    }
                }
                Arc::new(BooleanArray::from(vals))
            }
            DataType::Int64 => {
                let mut vals = Vec::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx) {
                        Some(MssqlCellValue::Int64(n)) => vals.push(Some(*n)),
                        Some(MssqlCellValue::Bool(b)) => vals.push(Some(i64::from(*b))),
                        _ => vals.push(None),
                    }
                }
                Arc::new(Int64Array::from(vals))
            }
            DataType::Float64 => {
                let mut vals = Vec::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx) {
                        Some(MssqlCellValue::Float64(f)) => vals.push(Some(*f)),
                        Some(MssqlCellValue::Int64(n)) => vals.push(Some(*n as f64)),
                        _ => vals.push(None),
                    }
                }
                Arc::new(Float64Array::from(vals))
            }
            DataType::Date32 => {
                let mut vals = Vec::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx) {
                        Some(MssqlCellValue::Date(d)) => {
                            vals.push(Some(d.num_days_from_ce() - UNIX_EPOCH_CE_DAYS));
                        }
                        _ => vals.push(None),
                    }
                }
                Arc::new(Date32Array::from(vals))
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
                let mut vals = Vec::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx) {
                        Some(MssqlCellValue::DateTime(dt)) => {
                            vals.push(Some(dt.and_utc().timestamp_micros()));
                        }
                        Some(MssqlCellValue::DateTimeOffset(dto)) => {
                            vals.push(Some(dto.timestamp_micros()));
                        }
                        _ => vals.push(None),
                    }
                }
                let arr = TimestampMicrosecondArray::from(vals).with_timezone_opt(tz_opt.clone());
                Arc::new(arr)
            }
            DataType::Binary => {
                let mut vals: Vec<Option<&[u8]>> = Vec::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx) {
                        Some(MssqlCellValue::Bytes(b)) => vals.push(Some(b.as_slice())),
                        Some(MssqlCellValue::String(s)) => vals.push(Some(s.as_bytes())),
                        _ => vals.push(None),
                    }
                }
                Arc::new(BinaryArray::from_opt_vec(vals))
            }
            _ => {
                let mut vals: Vec<Option<String>> = Vec::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx) {
                        None | Some(MssqlCellValue::Null) => vals.push(None),
                        Some(MssqlCellValue::String(s) | MssqlCellValue::Xml(s)) => {
                            vals.push(Some(s.clone()))
                        }
                        Some(MssqlCellValue::Decimal(d)) => vals.push(Some(d.to_string())),
                        Some(MssqlCellValue::Uuid(u)) => vals.push(Some(u.to_string())),
                        Some(MssqlCellValue::Time(t)) => vals.push(Some(t.to_string())),
                        Some(MssqlCellValue::Date(d)) => vals.push(Some(d.to_string())),
                        Some(MssqlCellValue::DateTime(dt)) => {
                            vals.push(Some(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string()))
                        }
                        Some(MssqlCellValue::DateTimeOffset(dto)) => {
                            vals.push(Some(dto.to_rfc3339()))
                        }
                        Some(MssqlCellValue::Bool(b)) => vals.push(Some(b.to_string())),
                        Some(MssqlCellValue::Int64(n)) => vals.push(Some(n.to_string())),
                        Some(MssqlCellValue::Float64(f)) => vals.push(Some(f.to_string())),
                        Some(MssqlCellValue::Bytes(b)) => {
                            vals.push(Some(String::from_utf8_lossy(b).into_owned()))
                        }
                    }
                }
                Arc::new(StringArray::from(vals))
            }
        };
        columns_out.push(array);
    }

    RecordBatch::try_new(schema, columns_out)
        .map_err(|e| MssqlError::data(format!("failed to construct Arrow RecordBatch: {e}")))
}

/// Serialize a [`RecordBatch`] into an Arrow IPC stream byte vector.
pub fn encode_batch_to_ipc_stream(batch: &RecordBatch) -> Result<Vec<u8>, MssqlError> {
    let mut buf = Vec::with_capacity(4096);
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref()).map_err(|e| {
            MssqlError::data(format!("failed to initialize Arrow StreamWriter: {e}"))
        })?;
        writer
            .write(batch)
            .map_err(|e| MssqlError::data(format!("failed to write Arrow RecordBatch: {e}")))?;
        writer
            .finish()
            .map_err(|e| MssqlError::data(format!("failed to finalize Arrow StreamWriter: {e}")))?;
    }
    Ok(buf)
}

/// Materialize an [`MssqlQueryResult`] into an unregistered `VTBARROW` POSIX shared-memory
/// [`ShmRegion`], returning `(shm_name, payload_len, row_count, Arc<ShmRegion>)`.
pub fn query_result_to_shm_arrow(res: &MssqlQueryResult) -> Result<ShmArrowBatchPart, MssqlError> {
    let schema = build_arrow_schema(&res.columns, &res.rows);
    let batch = rows_to_record_batch(schema, &res.rows)?;
    let ipc_bytes = encode_batch_to_ipc_stream(&batch)?;
    let shm_name = next_mssql_shm_name();
    let (norm_name, byte_len, region) =
        write_shm_arrow_region_unregistered(&shm_name, &ipc_bytes).map_err(MssqlError::data)?;
    Ok((norm_name, byte_len, res.rows.len(), region))
}

/// Materialize an [`MssqlQueryResult`] into chunked `VTBARROW` shared-memory [`ShmRegion`]s
/// of at most `batch_size` rows each.
pub fn query_result_to_shm_arrow_batches(
    res: &MssqlQueryResult,
    batch_size: usize,
) -> Result<Vec<ShmArrowBatchPart>, MssqlError> {
    let step = batch_size.max(1);
    let schema = build_arrow_schema(&res.columns, &res.rows);
    if res.rows.is_empty() {
        let empty_batch = rows_to_record_batch(schema, &[])?;
        let ipc_bytes = encode_batch_to_ipc_stream(&empty_batch)?;
        let shm_name = next_mssql_shm_name();
        let (norm_name, byte_len, region) =
            write_shm_arrow_region_unregistered(&shm_name, &ipc_bytes).map_err(MssqlError::data)?;
        return Ok(vec![(norm_name, byte_len, 0, region)]);
    }

    let mut out = Vec::new();
    for chunk in res.rows.chunks(step) {
        let batch = rows_to_record_batch(Arc::clone(&schema), chunk)?;
        let ipc_bytes = encode_batch_to_ipc_stream(&batch)?;
        let shm_name = next_mssql_shm_name();
        let (norm_name, byte_len, region) =
            write_shm_arrow_region_unregistered(&shm_name, &ipc_bytes).map_err(MssqlError::data)?;
        out.push((norm_name, byte_len, chunk.len(), region));
    }
    Ok(out)
}

/// Open an incremental [`ShmArrowStreamWriter`] backed by a fresh `/vtb-ms-*` segment.
pub fn open_mssql_shm_stream_writer(
    schema_ipc_frame: &[u8],
) -> Result<ShmArrowStreamWriter, MssqlError> {
    let shm_name = next_mssql_shm_name();
    ShmArrowStreamWriter::create(
        &shm_name,
        DEFAULT_MSSQL_ARROW_SHM_CAPACITY,
        schema_ipc_frame,
    )
    .map_err(MssqlError::data)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow_ipc::reader::StreamReader;
    use rust_decimal::Decimal;
    use vertebra_db::arrow_shm::{SHM_ARROW_MAGIC, ShmArrowFrameHeader};

    use super::*;

    #[test]
    fn mssql_query_result_materializes_into_vtbarrow_shm_region() {
        let res = MssqlQueryResult {
            columns: vec![
                MssqlColumnMeta {
                    name: "id".to_string(),
                    column_type: ColumnType::Int8,
                },
                MssqlColumnMeta {
                    name: "amount".to_string(),
                    column_type: ColumnType::Decimaln,
                },
                MssqlColumnMeta {
                    name: "active".to_string(),
                    column_type: ColumnType::Bit,
                },
            ],
            rows: vec![
                vec![
                    MssqlCellValue::Int64(1),
                    MssqlCellValue::Decimal(Decimal::new(1999, 2)),
                    MssqlCellValue::Bool(true),
                ],
                vec![
                    MssqlCellValue::Int64(2),
                    MssqlCellValue::Null,
                    MssqlCellValue::Bool(false),
                ],
            ],
            rows_affected: 2,
        };

        let (shm_name, byte_len, row_count, region) =
            query_result_to_shm_arrow(&res).expect("materialize VTBARROW");
        assert!(shm_name.starts_with("/vtb-ms-"));
        assert_eq!(row_count, 2);
        assert!(byte_len > 0);

        let raw = unsafe { region.as_slice() };
        let header = ShmArrowFrameHeader::from_slice(raw).expect("decode header");
        assert_eq!(&header.magic, SHM_ARROW_MAGIC);
        let ipc_slice = &raw[ShmArrowFrameHeader::SIZE..ShmArrowFrameHeader::SIZE + byte_len];

        let mut reader =
            StreamReader::try_new(Cursor::new(ipc_slice), None).expect("open Arrow StreamReader");
        let batch = reader.next().unwrap().expect("read first batch");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }
}
