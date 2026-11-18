//! TDS 7.3+ column value decoding and Python object materialization for `vertebra-mssql`.
//!
//! Decodes `tiberius::Row` cells across all SQL Server wire types:
//! - Exact numerics (`Numeric` / `DECIMAL` / `NUMERIC` / `MONEY` / `SMALLMONEY` via `tiberius::numeric::Numeric` up to 38 digits)
//! - Integers (`TINYINT` `u8`, `SMALLINT` `i16`, `INT` `i32`, `BIGINT` `i64`) and `BIT` (`bool`)
//! - Floating point (`REAL` `f32`, `FLOAT` `f64`)
//! - Temporal types (`DATE`, `TIME`, `DATETIME`, `SMALLDATETIME`, `DATETIME2`, `DATETIMEOFFSET`)
//! - `UNIQUEIDENTIFIER` (`uuid::Uuid`)
//! - Variable-length and PLP streaming strings/bytes (`NVARCHAR(MAX)`, `VARCHAR(MAX)`, `VARBINARY(MAX)`, `XML`, `JSON`)

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use tiberius::numeric::Numeric;
use tiberius::xml::XmlData;
use tiberius::{ColumnType, Row};
use uuid::Uuid;

use crate::errors::MssqlError;

/// Format a 128-bit TDS [`Numeric`] preserving exact scale (`0..=38`) without a spurious `.0` when `scale == 0`.
pub fn format_numeric(num: Numeric) -> String {
    if num.scale() == 0 {
        return num.value().to_string();
    }
    let sign = if num.value() < 0 { "-" } else { "" };
    format!(
        "{sign}{}.{:0pad$}",
        num.int_part().unsigned_abs(),
        num.dec_part().unsigned_abs(),
        pad = usize::from(num.scale()),
    )
}

/// Metadata describing a single SQL Server result column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MssqlColumnMeta {
    pub name: String,
    pub column_type: ColumnType,
}

/// Owned decoded TDS column value independent of the wire stream lifetime.
#[derive(Clone, Debug, PartialEq)]
pub enum MssqlCellValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Decimal(Numeric),
    Uuid(Uuid),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    DateTimeOffset(DateTime<FixedOffset>),
    Xml(String),
}

/// Complete decoded result set returned by a SQL Server query.
#[derive(Clone, Debug, Default)]
pub struct MssqlQueryResult {
    pub columns: Vec<MssqlColumnMeta>,
    pub rows: Vec<Vec<MssqlCellValue>>,
    pub rows_affected: u64,
}

impl MssqlQueryResult {
    /// Convert a [`tiberius::Row`] into a vector of [`MssqlCellValue`] items.
    pub fn decode_row(row: &Row) -> Result<Vec<MssqlCellValue>, MssqlError> {
        let mut cells = Vec::with_capacity(row.len());
        for (idx, col) in row.columns().iter().enumerate() {
            let val = decode_column_cell(row, idx, col.column_type())?;
            cells.push(val);
        }
        Ok(cells)
    }

    /// Extract column metadata from a slice of [`tiberius::Column`].
    pub fn extract_columns(cols: &[tiberius::Column]) -> Vec<MssqlColumnMeta> {
        cols.iter()
            .map(|c| MssqlColumnMeta {
                name: c.name().to_string(),
                column_type: c.column_type(),
            })
            .collect()
    }
}

fn decode_column_cell(
    row: &Row,
    idx: usize,
    col_type: ColumnType,
) -> Result<MssqlCellValue, MssqlError> {
    match col_type {
        ColumnType::Null => Ok(MssqlCellValue::Null),
        ColumnType::Bit | ColumnType::Bitn => match row.try_get::<bool, usize>(idx)? {
            Some(b) => Ok(MssqlCellValue::Bool(b)),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Int1 => match row.try_get::<u8, usize>(idx)? {
            Some(v) => Ok(MssqlCellValue::Int64(i64::from(v))),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Int2 => match row.try_get::<i16, usize>(idx)? {
            Some(v) => Ok(MssqlCellValue::Int64(i64::from(v))),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Int4 => match row.try_get::<i32, usize>(idx)? {
            Some(v) => Ok(MssqlCellValue::Int64(i64::from(v))),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Int8 => match row.try_get::<i64, usize>(idx)? {
            Some(v) => Ok(MssqlCellValue::Int64(v)),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Intn => {
            if let Ok(opt) = row.try_get::<i64, usize>(idx) {
                return Ok(opt.map_or(MssqlCellValue::Null, MssqlCellValue::Int64));
            }
            if let Ok(opt) = row.try_get::<i32, usize>(idx) {
                return Ok(opt.map_or(MssqlCellValue::Null, |v| {
                    MssqlCellValue::Int64(i64::from(v))
                }));
            }
            if let Ok(opt) = row.try_get::<i16, usize>(idx) {
                return Ok(opt.map_or(MssqlCellValue::Null, |v| {
                    MssqlCellValue::Int64(i64::from(v))
                }));
            }
            match row.try_get::<u8, usize>(idx)? {
                Some(v) => Ok(MssqlCellValue::Int64(i64::from(v))),
                None => Ok(MssqlCellValue::Null),
            }
        }
        ColumnType::Float4 => match row.try_get::<f32, usize>(idx)? {
            Some(v) => Ok(MssqlCellValue::Float64(f64::from(v))),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Float8 => match row.try_get::<f64, usize>(idx)? {
            Some(v) => Ok(MssqlCellValue::Float64(v)),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Floatn | ColumnType::Money | ColumnType::Money4 => {
            if let Ok(opt) = row.try_get::<f64, usize>(idx) {
                return Ok(opt.map_or(MssqlCellValue::Null, MssqlCellValue::Float64));
            }
            match row.try_get::<f32, usize>(idx)? {
                Some(v) => Ok(MssqlCellValue::Float64(f64::from(v))),
                None => Ok(MssqlCellValue::Null),
            }
        }
        ColumnType::Decimaln | ColumnType::Numericn => match row.try_get::<Numeric, usize>(idx)? {
            Some(d) => Ok(MssqlCellValue::Decimal(d)),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Guid => match row.try_get::<Uuid, usize>(idx)? {
            Some(u) => Ok(MssqlCellValue::Uuid(u)),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Daten => match row.try_get::<NaiveDate, usize>(idx)? {
            Some(d) => Ok(MssqlCellValue::Date(d)),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Timen => match row.try_get::<NaiveTime, usize>(idx)? {
            Some(t) => Ok(MssqlCellValue::Time(t)),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Datetime
        | ColumnType::Datetime4
        | ColumnType::Datetimen
        | ColumnType::Datetime2 => match row.try_get::<NaiveDateTime, usize>(idx)? {
            Some(dt) => Ok(MssqlCellValue::DateTime(dt)),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::DatetimeOffsetn => match row.try_get::<DateTime<FixedOffset>, usize>(idx)? {
            Some(dto) => Ok(MssqlCellValue::DateTimeOffset(dto)),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Xml => match row.try_get::<&XmlData, usize>(idx)? {
            Some(xml) => Ok(MssqlCellValue::Xml(xml.as_ref().to_string())),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => {
            match row.try_get::<&[u8], usize>(idx)? {
                Some(bytes) => Ok(MssqlCellValue::Bytes(bytes.to_vec())),
                None => Ok(MssqlCellValue::Null),
            }
        }
        ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::Text
        | ColumnType::NText => match row.try_get::<&str, usize>(idx)? {
            Some(s) => Ok(MssqlCellValue::String(s.to_string())),
            None => Ok(MssqlCellValue::Null),
        },
        ColumnType::Udt | ColumnType::SSVariant => {
            if let Ok(Some(s)) = row.try_get::<&str, usize>(idx) {
                return Ok(MssqlCellValue::String(s.to_string()));
            }
            if let Ok(Some(b)) = row.try_get::<&[u8], usize>(idx) {
                return Ok(MssqlCellValue::Bytes(b.to_vec()));
            }
            Ok(MssqlCellValue::Null)
        }
    }
}

struct PyCodecImporters {
    decimal_cls: Py<PyAny>,
    uuid_cls: Py<PyAny>,
    date_cls: Py<PyAny>,
    time_cls: Py<PyAny>,
    datetime_cls: Py<PyAny>,
    timezone_cls: Py<PyAny>,
    timedelta_cls: Py<PyAny>,
}

static PY_CODEC_IMPORTERS: PyOnceLock<PyCodecImporters> = PyOnceLock::new();

fn codec_importers(py: Python<'_>) -> PyResult<&'static PyCodecImporters> {
    PY_CODEC_IMPORTERS.get_or_try_init(py, || {
        let decimal_mod = py.import("decimal")?;
        let uuid_mod = py.import("uuid")?;
        let dt_mod = py.import("datetime")?;
        Ok(PyCodecImporters {
            decimal_cls: decimal_mod.getattr("Decimal")?.unbind(),
            uuid_cls: uuid_mod.getattr("UUID")?.unbind(),
            date_cls: dt_mod.getattr("date")?.unbind(),
            time_cls: dt_mod.getattr("time")?.unbind(),
            datetime_cls: dt_mod.getattr("datetime")?.unbind(),
            timezone_cls: dt_mod.getattr("timezone")?.unbind(),
            timedelta_cls: dt_mod.getattr("timedelta")?.unbind(),
        })
    })
}

/// Convert an [`MssqlCellValue`] into its canonical Python object representation.
pub fn cell_to_py(py: Python<'_>, cell: &MssqlCellValue) -> PyResult<Py<PyAny>> {
    match cell {
        MssqlCellValue::Null => Ok(py.None()),
        MssqlCellValue::Bool(b) => Ok((*b).into_pyobject(py)?.to_owned().into_any().unbind()),
        MssqlCellValue::Int64(n) => Ok(n.into_pyobject(py)?.into_any().unbind()),
        MssqlCellValue::Float64(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        MssqlCellValue::String(s) | MssqlCellValue::Xml(s) => {
            Ok(s.into_pyobject(py)?.into_any().unbind())
        }
        MssqlCellValue::Bytes(b) => Ok(PyBytes::new(py, b).into_any().unbind()),
        MssqlCellValue::Decimal(d) => {
            let imp = codec_importers(py)?;
            let obj = imp.decimal_cls.bind(py).call1((format_numeric(*d),))?;
            Ok(obj.unbind())
        }
        MssqlCellValue::Uuid(u) => {
            let imp = codec_importers(py)?;
            let obj = imp.uuid_cls.bind(py).call1((u.to_string(),))?;
            Ok(obj.unbind())
        }
        MssqlCellValue::Date(d) => {
            let imp = codec_importers(py)?;
            let obj = imp
                .date_cls
                .bind(py)
                .call1((d.year(), d.month(), d.day()))?;
            Ok(obj.unbind())
        }
        MssqlCellValue::Time(t) => {
            let imp = codec_importers(py)?;
            let micro = t.nanosecond() / 1_000;
            let obj = imp
                .time_cls
                .bind(py)
                .call1((t.hour(), t.minute(), t.second(), micro))?;
            Ok(obj.unbind())
        }
        MssqlCellValue::DateTime(dt) => {
            let imp = codec_importers(py)?;
            let d = dt.date();
            let t = dt.time();
            let micro = t.nanosecond() / 1_000;
            let obj = imp.datetime_cls.bind(py).call1((
                d.year(),
                d.month(),
                d.day(),
                t.hour(),
                t.minute(),
                t.second(),
                micro,
            ))?;
            Ok(obj.unbind())
        }
        MssqlCellValue::DateTimeOffset(dto) => {
            let imp = codec_importers(py)?;
            let d = dto.date_naive();
            let t = dto.time();
            let micro = t.nanosecond() / 1_000;
            let offset_secs = dto.offset().local_minus_utc();
            let td = imp.timedelta_cls.bind(py).call1((0, offset_secs))?;
            let tz = imp.timezone_cls.bind(py).call1((td,))?;
            let obj = imp.datetime_cls.bind(py).call1((
                d.year(),
                d.month(),
                d.day(),
                t.hour(),
                t.minute(),
                t.second(),
                micro,
                tz,
            ))?;
            Ok(obj.unbind())
        }
    }
}

/// Convert an [`MssqlQueryResult`] into a Python `list[dict[str, Any]]`.
pub fn rows_to_py_dicts(py: Python<'_>, res: &MssqlQueryResult) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for row in &res.rows {
        let dict = PyDict::new(py);
        for (col, cell) in res.columns.iter().zip(row.iter()) {
            dict.set_item(&col.name, cell_to_py(py, cell)?)?;
        }
        list.append(dict)?;
    }
    Ok(list.into_any().unbind())
}

/// Convert the first row of [`MssqlQueryResult`] into a Python `dict[str, Any]` or `None`.
pub fn first_row_to_py_dict(py: Python<'_>, res: &MssqlQueryResult) -> PyResult<Py<PyAny>> {
    let Some(first) = res.rows.first() else {
        return Ok(py.None());
    };
    let dict = PyDict::new(py);
    for (col, cell) in res.columns.iter().zip(first.iter()) {
        dict.set_item(&col.name, cell_to_py(py, cell)?)?;
    }
    Ok(dict.into_any().unbind())
}

/// Convert a full [`MssqlQueryResult`] into a structured dictionary (`columns`, `rows`, `rows_affected`).
pub fn query_result_to_py_envelope(py: Python<'_>, res: MssqlQueryResult) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    let col_names: Vec<&str> = res.columns.iter().map(|c| c.name.as_str()).collect();
    dict.set_item("columns", col_names)?;
    let rows_list = PyList::empty(py);
    for row in &res.rows {
        let cells_py: Vec<Py<PyAny>> = row
            .iter()
            .map(|c| cell_to_py(py, c))
            .collect::<PyResult<Vec<_>>>()?;
        rows_list.append(PyTuple::new(py, cells_py)?)?;
    }
    dict.set_item("rows", rows_list)?;
    dict.set_item("rows_affected", res.rows_affected)?;
    Ok(dict.into_any().unbind())
}

/// Convert a full [`MssqlQueryResult`] into `(rows, column_names)` where `rows` is `list[tuple[Any, ...]]`
/// and `column_names` is `tuple[str, ...]`, preserving `COLMETADATA` column names even when `0` rows match.
pub fn query_result_to_py_rows_and_cols(
    py: Python<'_>,
    res: MssqlQueryResult,
) -> PyResult<Py<PyAny>> {
    let col_names: Vec<&str> = res.columns.iter().map(|c| c.name.as_str()).collect();
    let cols_tuple = PyTuple::new(py, col_names)?;
    let rows_list = PyList::empty(py);
    for row in &res.rows {
        let cells_py: Vec<Py<PyAny>> = row
            .iter()
            .map(|c| cell_to_py(py, c))
            .collect::<PyResult<Vec<_>>>()?;
        rows_list.append(PyTuple::new(py, cells_py)?)?;
    }
    let pair = PyTuple::new(py, [rows_list.into_any(), cols_tuple.into_any()])?;
    Ok(pair.into_any().unbind())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::parse_decimal_numeric;

    #[test]
    fn python_cell_materialization_preserves_decimal_uuid_and_temporal_types() {
        Python::initialize();
        Python::attach(|py| {
            let dec = MssqlCellValue::Decimal(parse_decimal_numeric("12345.6789").unwrap());
            let py_dec = cell_to_py(py, &dec).unwrap();
            assert_eq!(
                py_dec.bind(py).str().unwrap().to_str().unwrap(),
                "12345.6789"
            );

            let dec38 = MssqlCellValue::Decimal(
                parse_decimal_numeric("1234567890123456789012345678.0123456789").unwrap(),
            );
            let py_dec38 = cell_to_py(py, &dec38).unwrap();
            assert_eq!(
                py_dec38.bind(py).str().unwrap().to_str().unwrap(),
                "1234567890123456789012345678.0123456789"
            );

            let uid = MssqlCellValue::Uuid(Uuid::nil());
            let py_uid = cell_to_py(py, &uid).unwrap();
            assert_eq!(
                py_uid.bind(py).str().unwrap().to_str().unwrap(),
                "00000000-0000-0000-0000-000000000000"
            );

            let dt = MssqlCellValue::DateTime(
                NaiveDate::from_ymd_opt(2026, 11, 9)
                    .unwrap()
                    .and_hms_micro_opt(14, 30, 0, 123456)
                    .unwrap(),
            );
            let py_dt = cell_to_py(py, &dt).unwrap();
            assert_eq!(
                py_dt
                    .bind(py)
                    .call_method0("isoformat")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "2026-11-09T14:30:00.123456"
            );
        });
    }
}
