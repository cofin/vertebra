//! Oracle 26ai scalar, temporal, inline LOB, OSON, and VECTOR codecs and PyO3 result types.

use std::collections::HashMap;
use std::io::Read;
use std::str::FromStr;

use chrono::{Datelike, Timelike};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyAny, PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};
use vertebra_db::oracle::{
    ORACLE_VECTOR_MAGIC, ORACLE_VECTOR_VERSION, OSON_MAGIC, decode_oracle_vector_binary,
    decode_oracle_vector_f32, decode_oracle_vector_f64, decode_oracle_vector_i8,
    decode_oson_payload, encode_oracle_vector_binary, encode_oracle_vector_f32,
    encode_oracle_vector_f64, encode_oracle_vector_i8, encode_oson_payload,
};

use crate::errors::OracleError;

impl OracleError {
    pub(crate) fn data_error(message: impl Into<String>) -> Self {
        let msg = message.into();
        Self::Server {
            code: 1722,
            message: if msg.starts_with("ORA-") {
                msg
            } else {
                format!("ORA-01722: {msg}")
            },
        }
    }

    pub(crate) fn operational(message: impl Into<String>) -> Self {
        Self::Transport {
            code: None,
            message: message.into(),
        }
    }
}

/// Metadata describing a single column in an [`OracleQueryResult`].
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(module = "vertebra._oracle", frozen, immutable_type, freelist = 128)
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(module = "vertebra._oracle", frozen, immutable_type)
)]
#[derive(Clone, Debug)]
pub struct OracleColumn {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub type_code: u16,
    #[pyo3(get)]
    pub type_name: String,
    #[pyo3(get)]
    pub db_size: Option<usize>,
    #[pyo3(get)]
    pub precision: Option<i64>,
    #[pyo3(get)]
    pub scale: Option<i64>,
    #[pyo3(get)]
    pub nullable: bool,
}

#[pymethods]
impl OracleColumn {
    #[new]
    #[pyo3(signature = (name, type_name, type_code=0, db_size=None, precision=None, scale=None, nullable=true))]
    pub fn new(
        name: String,
        type_name: String,
        type_code: u16,
        db_size: Option<usize>,
        precision: Option<i64>,
        scale: Option<i64>,
        nullable: bool,
    ) -> Self {
        Self {
            name,
            type_code,
            type_name,
            db_size,
            precision,
            scale,
            nullable,
        }
    }

    #[getter]
    pub fn oracle_type(&self) -> &str {
        &self.type_name
    }
}

fn canonical_oracle_type_name(raw_name: &str) -> String {
    let stripped = raw_name.strip_prefix("DB_TYPE_").unwrap_or(raw_name);
    match stripped {
        "VARCHAR" => "VARCHAR2".to_owned(),
        "NVARCHAR" => "NVARCHAR2".to_owned(),
        "TIMESTAMP_TZ" => "TIMESTAMP WITH TIME ZONE".to_owned(),
        "TIMESTAMP_LTZ" => "TIMESTAMP WITH LOCAL TIME ZONE".to_owned(),
        "INTERVAL_DS" => "INTERVAL DAY TO SECOND".to_owned(),
        "INTERVAL_YM" => "INTERVAL YEAR TO MONTH".to_owned(),
        other => other.to_owned(),
    }
}

impl OracleColumn {
    /// Construct an [`OracleColumn`] from an `oracledb::Metadata` descriptor.
    pub fn from_metadata(meta: &oracledb::Metadata, lowercase_column_names: bool) -> Self {
        let name = if lowercase_column_names {
            meta.name().to_ascii_lowercase()
        } else {
            meta.name().to_owned()
        };
        let db_type = meta.db_type();
        let type_code = db_type.num();
        let type_name = canonical_oracle_type_name(db_type.name());
        let db_size = if meta.max_size() > 0 {
            Some(meta.max_size() as usize)
        } else {
            None
        };
        let precision = if meta.precision() > 0 {
            Some(i64::from(meta.precision()))
        } else {
            None
        };
        let scale = if meta.precision() > 0 || meta.scale() != 0 {
            Some(i64::from(meta.scale()))
        } else {
            None
        };
        Self {
            name,
            type_code,
            type_name,
            db_size,
            precision,
            scale,
            nullable: meta.nullable(),
        }
    }
}

/// Result container returned by `SyncOracleClient` and `AsyncOracleClient` query and execute calls.
#[pyclass(module = "vertebra._oracle", frozen, immutable_type)]
#[derive(Debug)]
pub struct OracleQueryResult {
    pub columns: Vec<Py<OracleColumn>>,
    pub rows: Vec<Py<PyTuple>>,
    #[pyo3(get)]
    pub rowcount: usize,
    #[pyo3(get)]
    pub lastrowid: Option<String>,
    pub out_binds: Option<Py<PyAny>>,
}

#[pymethods]
impl OracleQueryResult {
    #[new]
    #[pyo3(signature = (columns, rows, rowcount=0, out_binds=None, lastrowid=None))]
    pub fn new(
        py: Python<'_>,
        columns: Vec<Py<OracleColumn>>,
        rows: Vec<Py<PyTuple>>,
        rowcount: usize,
        out_binds: Option<Py<PyAny>>,
        lastrowid: Option<String>,
    ) -> Self {
        let _ = py;
        Self {
            columns,
            rows,
            rowcount,
            lastrowid,
            out_binds,
        }
    }

    #[getter]
    pub fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(py, self.columns.iter().map(|c| c.bind(py)))
    }

    #[getter]
    pub fn rows<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, self.rows.iter().map(|r| r.bind(py)))
    }

    #[getter]
    pub fn out_binds<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyAny>> {
        self.out_binds.as_ref().map(|b| b.bind(py).clone())
    }

    pub fn to_dicts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let col_names: Vec<Bound<'py, PyString>> = self
            .columns
            .iter()
            .map(|c| PyString::new(py, &c.borrow(py).name))
            .collect();
        let out = PyList::empty(py);
        for row_tuple in &self.rows {
            let tuple_bound = row_tuple.bind(py);
            let dict = PyDict::new(py);
            for (idx, col_key) in col_names.iter().enumerate() {
                if let Ok(val) = tuple_bound.get_item(idx) {
                    dict.set_item(col_key, val)?;
                }
            }
            out.append(dict)?;
        }
        Ok(out)
    }

    pub fn __len__(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Owned Oracle 26ai `VECTOR` payload across supported storage formats.
#[derive(Clone, Debug, PartialEq)]
pub enum OracleVectorData {
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Int8(Vec<i8>),
    Binary(Vec<u8>),
}

impl OracleVectorData {
    /// Encode this vector into a `0xDB 0x01` binary wire frame.
    pub fn encode_wire_bytes(&self) -> Vec<u8> {
        match self {
            Self::Float32(v) => encode_oracle_vector_f32(v).to_vec(),
            Self::Float64(v) => encode_oracle_vector_f64(v).to_vec(),
            Self::Int8(v) => encode_oracle_vector_i8(v).to_vec(),
            Self::Binary(v) => encode_oracle_vector_binary(v).to_vec(),
        }
    }

    /// Decode a `0xDB 0x01` binary wire frame into [`OracleVectorData`].
    pub fn decode_wire_bytes(buf: &[u8]) -> Result<Self, OracleError> {
        if buf.len() < 8 {
            return Err(OracleError::data_error(
                "Buffer too short for 8-byte Oracle VECTOR header",
            ));
        }
        if buf[0] != ORACLE_VECTOR_MAGIC || buf[1] != ORACLE_VECTOR_VERSION {
            return Err(OracleError::data_error(format!(
                "Invalid Oracle VECTOR magic/version header: [0x{:02x}, 0x{:02x}]",
                buf[0], buf[1]
            )));
        }
        match buf[2] {
            2 => decode_oracle_vector_f32(buf)
                .map(Self::Float32)
                .map_err(OracleError::data_error),
            3 => decode_oracle_vector_f64(buf)
                .map(Self::Float64)
                .map_err(OracleError::data_error),
            4 => decode_oracle_vector_i8(buf)
                .map(Self::Int8)
                .map_err(OracleError::data_error),
            5 => decode_oracle_vector_binary(buf)
                .map(Self::Binary)
                .map_err(OracleError::data_error),
            other => Err(OracleError::data_error(format!(
                "Unsupported Oracle VECTOR format code: {other}"
            ))),
        }
    }

    /// Convert into an `oracledb::Vector` value for wire binding.
    pub fn to_oracledb_vector(&self) -> oracledb::Vector {
        let data = match self {
            Self::Float32(v) => oracledb::VectorData::Float32(v.clone()),
            Self::Float64(v) => oracledb::VectorData::Float64(v.clone()),
            Self::Int8(v) => oracledb::VectorData::Int8(v.clone()),
            Self::Binary(v) => oracledb::VectorData::Binary(v.clone()),
        };
        oracledb::Vector::Dense(data)
    }

    /// Convert from an `oracledb::Vector` value returned by the server.
    pub fn from_oracledb_vector(vec: &oracledb::Vector) -> Result<Self, OracleError> {
        match vec {
            oracledb::Vector::Dense(data) => match data {
                oracledb::VectorData::Float32(v) => Ok(Self::Float32(v.clone())),
                oracledb::VectorData::Float64(v) => Ok(Self::Float64(v.clone())),
                oracledb::VectorData::Int8(v) => Ok(Self::Int8(v.clone())),
                oracledb::VectorData::Binary(v) => Ok(Self::Binary(v.clone())),
            },
            oracledb::Vector::Sparse(sparse) => {
                let dims = sparse.num_dimensions();
                let indices = sparse.indices();
                match sparse.values() {
                    oracledb::VectorData::Float32(vals) => {
                        let mut dense = vec![0.0f32; dims];
                        for (&idx, &val) in indices.iter().zip(vals.iter()) {
                            if idx < dims {
                                dense[idx] = val;
                            }
                        }
                        Ok(Self::Float32(dense))
                    }
                    oracledb::VectorData::Float64(vals) => {
                        let mut dense = vec![0.0f64; dims];
                        for (&idx, &val) in indices.iter().zip(vals.iter()) {
                            if idx < dims {
                                dense[idx] = val;
                            }
                        }
                        Ok(Self::Float64(dense))
                    }
                    oracledb::VectorData::Int8(vals) => {
                        let mut dense = vec![0i8; dims];
                        for (&idx, &val) in indices.iter().zip(vals.iter()) {
                            if idx < dims {
                                dense[idx] = val;
                            }
                        }
                        Ok(Self::Int8(dense))
                    }
                    oracledb::VectorData::Binary(_) => Err(OracleError::data_error(
                        "Sparse BINARY Oracle VECTOR is not supported",
                    )),
                }
            }
        }
    }

    /// Construct a `vertebra._vertebra.VectorView` (or Python fallback list/bytes) on the GIL thread.
    pub fn into_py_vector_view<'py>(
        &self,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyAny>, OracleError> {
        if let Ok(vtb_mod) = py.import("vertebra._vertebra")
            && let Ok(vv_cls) = vtb_mod.getattr("VectorView")
        {
            match self {
                Self::Float32(vals) => {
                    return vv_cls
                        .call_method1("from_f32", (vals.clone(), false))
                        .map_err(|e| OracleError::data_error(e.to_string()));
                }
                Self::Float64(vals) => {
                    let mut raw = Vec::with_capacity(vals.len() * 8);
                    for &v in vals {
                        raw.extend_from_slice(&v.to_ne_bytes());
                    }
                    return vv_cls
                        .call_method1(
                            "from_bytes",
                            (PyBytes::new(py, &raw), vals.len(), "float64", 1.0f32),
                        )
                        .map_err(|e| OracleError::data_error(e.to_string()));
                }
                Self::Int8(vals) => {
                    let raw: Vec<u8> = vals.iter().map(|&v| v as u8).collect();
                    return vv_cls
                        .call_method1(
                            "from_bytes",
                            (PyBytes::new(py, &raw), vals.len(), "int8", 1.0f32),
                        )
                        .map_err(|e| OracleError::data_error(e.to_string()));
                }
                Self::Binary(packed) => {
                    return vv_cls
                        .call_method1(
                            "from_bytes",
                            (PyBytes::new(py, packed), packed.len() * 8, "binary", 1.0f32),
                        )
                        .map_err(|e| OracleError::data_error(e.to_string()));
                }
            }
        }
        match self {
            Self::Float32(vals) => vals
                .clone()
                .into_pyobject(py)
                .map(Bound::into_any)
                .map_err(|e| OracleError::data_error(e.to_string())),
            Self::Float64(vals) => vals
                .clone()
                .into_pyobject(py)
                .map(Bound::into_any)
                .map_err(|e| OracleError::data_error(e.to_string())),
            Self::Int8(vals) => vals
                .clone()
                .into_pyobject(py)
                .map(Bound::into_any)
                .map_err(|e| OracleError::data_error(e.to_string())),
            Self::Binary(bytes) => Ok(PyBytes::new(py, bytes).into_any()),
        }
    }
}

/// Owned intermediate cell/bind representation transferable across worker threads without the GIL.
#[derive(Clone, Debug, PartialEq)]
pub enum OracleCellValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    DecimalStr(String),
    Str(String),
    Bytes(Vec<u8>),
    Date {
        year: i32,
        month: u8,
        day: u8,
    },
    Timestamp {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
        tz_offset_seconds: Option<i32>,
    },
    Oson(Vec<u8>),
    Vector(OracleVectorData),
}

impl OracleCellValue {
    /// Materialize this owned cell into a Python object on the calling GIL thread.
    pub fn into_py_object<'py>(
        self,
        py: Python<'py>,
        fetch_decimals: bool,
    ) -> Result<Bound<'py, PyAny>, OracleError> {
        match self {
            Self::Null => Ok(py.None().into_bound(py)),
            Self::Bool(b) => Ok(PyBool::new(py, b).to_owned().into_any()),
            Self::Int(v) => {
                if fetch_decimals {
                    let dec_cls = py
                        .import("decimal")
                        .and_then(|m| m.getattr("Decimal"))
                        .map_err(|e| OracleError::data_error(e.to_string()))?;
                    dec_cls
                        .call1((v.to_string(),))
                        .map_err(|e| OracleError::data_error(e.to_string()))
                } else {
                    Ok(PyInt::new(py, v).into_any())
                }
            }
            Self::Float(f) => {
                if fetch_decimals {
                    let dec_cls = py
                        .import("decimal")
                        .and_then(|m| m.getattr("Decimal"))
                        .map_err(|e| OracleError::data_error(e.to_string()))?;
                    dec_cls
                        .call1((f.to_string(),))
                        .map_err(|e| OracleError::data_error(e.to_string()))
                } else {
                    Ok(PyFloat::new(py, f).into_any())
                }
            }
            Self::DecimalStr(s) => {
                if !fetch_decimals
                    && !s.contains('.')
                    && !s.contains('e')
                    && !s.contains('E')
                    && let Ok(iv) = s.parse::<i64>()
                {
                    return Ok(PyInt::new(py, iv).into_any());
                }
                let dec_cls = py
                    .import("decimal")
                    .and_then(|m| m.getattr("Decimal"))
                    .map_err(|e| OracleError::data_error(e.to_string()))?;
                dec_cls
                    .call1((s,))
                    .map_err(|e| OracleError::data_error(e.to_string()))
            }
            Self::Str(s) => Ok(PyString::new(py, &s).into_any()),
            Self::Bytes(b) => Ok(PyBytes::new(py, &b).into_any()),
            Self::Date { year, month, day } => {
                let dt_mod = py
                    .import("datetime")
                    .map_err(|e| OracleError::data_error(e.to_string()))?;
                dt_mod
                    .getattr("date")
                    .and_then(|cls| cls.call1((year, month, day)))
                    .map_err(|e| OracleError::data_error(e.to_string()))
            }
            Self::Timestamp {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanosecond,
                tz_offset_seconds,
            } => {
                let dt_mod = py
                    .import("datetime")
                    .map_err(|e| OracleError::data_error(e.to_string()))?;
                let microsecond = nanosecond / 1000;
                if let Some(total_seconds) = tz_offset_seconds {
                    let td = dt_mod
                        .getattr("timedelta")
                        .and_then(|cls| cls.call1((0, total_seconds)))
                        .map_err(|e| OracleError::data_error(e.to_string()))?;
                    let tz = dt_mod
                        .getattr("timezone")
                        .and_then(|cls| cls.call1((td,)))
                        .map_err(|e| OracleError::data_error(e.to_string()))?;
                    dt_mod
                        .getattr("datetime")
                        .and_then(|cls| {
                            cls.call1((year, month, day, hour, minute, second, microsecond, tz))
                        })
                        .map_err(|e| OracleError::data_error(e.to_string()))
                } else {
                    dt_mod
                        .getattr("datetime")
                        .and_then(|cls| {
                            cls.call1((year, month, day, hour, minute, second, microsecond))
                        })
                        .map_err(|e| OracleError::data_error(e.to_string()))
                }
            }
            Self::Oson(oson_bytes) => {
                if oson_bytes.len() < 8 || oson_bytes[3] != 0x01 {
                    return Err(OracleError::data_error(
                        "Invalid or unsupported Oracle OSON header version",
                    ));
                }
                let json_slice =
                    decode_oson_payload(&oson_bytes).map_err(OracleError::data_error)?;
                let ser_mod = py
                    .import("vertebra.utils.serializers")
                    .map_err(|e| OracleError::data_error(e.to_string()))?;
                ser_mod
                    .getattr("decode_json")
                    .and_then(|fn_dec| fn_dec.call1((PyBytes::new(py, json_slice),)))
                    .map_err(|e| OracleError::data_error(e.to_string()))
            }
            Self::Vector(vec_data) => vec_data.into_py_vector_view(py),
        }
    }
}

/// Extract a Python bind parameter into an owned [`OracleCellValue`].
pub fn extract_bind_value(obj: &Bound<'_, PyAny>) -> Result<OracleCellValue, OracleError> {
    if obj.is_none() {
        return Ok(OracleCellValue::Null);
    }
    if obj.is_instance_of::<PyBool>() {
        let b = obj
            .extract::<bool>()
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Bool(b));
    }

    let type_name = obj
        .get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_default();

    if type_name == "VectorView" {
        let dtype = obj
            .getattr("dtype")
            .and_then(|d| d.extract::<String>())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let dims = obj
            .getattr("dimensions")
            .and_then(|d| d.extract::<usize>())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let raw_bytes = obj
            .call_method0("to_bytes")
            .and_then(|b| b.extract::<Vec<u8>>())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let vec_data = match dtype.as_str() {
            "float32" => {
                if raw_bytes.len() != dims * 4 {
                    return Err(OracleError::data_error(
                        "VectorView float32 byte length mismatch",
                    ));
                }
                let mut vals = Vec::with_capacity(dims);
                for &chunk in raw_bytes.as_chunks::<4>().0 {
                    vals.push(f32::from_ne_bytes(chunk));
                }
                OracleVectorData::Float32(vals)
            }
            "float64" => {
                if raw_bytes.len() != dims * 8 {
                    return Err(OracleError::data_error(
                        "VectorView float64 byte length mismatch",
                    ));
                }
                let mut vals = Vec::with_capacity(dims);
                for &chunk in raw_bytes.as_chunks::<8>().0 {
                    vals.push(f64::from_ne_bytes(chunk));
                }
                OracleVectorData::Float64(vals)
            }
            "int8" => OracleVectorData::Int8(raw_bytes.into_iter().map(|b| b as i8).collect()),
            "binary" => OracleVectorData::Binary(raw_bytes),
            other => {
                return Err(OracleError::data_error(format!(
                    "Unsupported VectorView dtype for Oracle bind: {other}"
                )));
            }
        };
        return Ok(OracleCellValue::Vector(vec_data));
    }

    if type_name == "datetime" {
        let year: i32 = obj
            .getattr("year")
            .and_then(|v| v.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let month: u8 = obj
            .getattr("month")
            .and_then(|v| v.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let day: u8 = obj
            .getattr("day")
            .and_then(|v| v.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let hour: u8 = obj
            .getattr("hour")
            .and_then(|v| v.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let minute: u8 = obj
            .getattr("minute")
            .and_then(|v| v.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let second: u8 = obj
            .getattr("second")
            .and_then(|v| v.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let microsecond: u32 = obj
            .getattr("microsecond")
            .and_then(|v| v.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;

        let mut tz_offset_seconds = None;
        if let Ok(offset_obj) = obj.call_method0("utcoffset")
            && !offset_obj.is_none()
            && let Ok(total_secs_f) = offset_obj
                .call_method0("total_seconds")
                .and_then(|s| s.extract::<f64>())
        {
            tz_offset_seconds = Some(total_secs_f.round() as i32);
        }
        return Ok(OracleCellValue::Timestamp {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond: microsecond * 1000,
            tz_offset_seconds,
        });
    }

    if type_name == "date" {
        let year: i32 = obj
            .getattr("year")
            .and_then(|v| v.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let month: u8 = obj
            .getattr("month")
            .and_then(|v| v.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let day: u8 = obj
            .getattr("day")
            .and_then(|v| v.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Date { year, month, day });
    }

    if type_name == "Decimal" {
        let text = obj
            .str()
            .and_then(|s| Ok(s.to_str()?.to_owned()))
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::DecimalStr(text));
    }

    if type_name == "UUID" {
        let raw_bytes = obj
            .getattr("bytes")
            .and_then(|b| b.extract::<Vec<u8>>())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Bytes(raw_bytes));
    }

    if obj.is_instance_of::<PyInt>() {
        if let Ok(iv) = obj.extract::<i64>() {
            return Ok(OracleCellValue::Int(iv));
        }
        let text = obj
            .str()
            .and_then(|s| Ok(s.to_str()?.to_owned()))
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::DecimalStr(text));
    }

    if obj.is_instance_of::<PyFloat>() {
        let fv = obj
            .extract::<f64>()
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Float(fv));
    }

    if obj.is_instance_of::<PyString>() {
        let sv = obj
            .extract::<String>()
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Str(sv));
    }

    if obj.is_instance_of::<PyBytes>() || obj.is_instance_of::<PyByteArray>() {
        let bv = obj
            .extract::<Vec<u8>>()
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        if bv.len() >= 4 && &bv[0..3] == OSON_MAGIC {
            if bv[3] != 0x01 {
                return Err(OracleError::data_error(
                    "Invalid Oracle OSON version byte (expected 0x01)",
                ));
            }
            decode_oson_payload(&bv).map_err(OracleError::data_error)?;
            return Ok(OracleCellValue::Oson(bv));
        }
        if bv.len() >= 2 && bv[0] == ORACLE_VECTOR_MAGIC {
            let vec_data = OracleVectorData::decode_wire_bytes(&bv)?;
            return Ok(OracleCellValue::Vector(vec_data));
        }
        return Ok(OracleCellValue::Bytes(bv));
    }

    if obj.is_instance_of::<PyDict>() {
        let py = obj.py();
        let ser_mod = py
            .import("vertebra.utils.serializers")
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let json_bytes: Vec<u8> = ser_mod
            .getattr("encode_json_bytes")
            .and_then(|f| f.call1((obj,)))
            .and_then(|b| b.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Oson(
            encode_oson_payload(&json_bytes).to_vec(),
        ));
    }

    if obj.is_instance_of::<PyList>() {
        let list = obj
            .cast::<PyList>()
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let is_numeric_vector = !list.is_empty()
            && list.iter().all(|item| {
                !item.is_instance_of::<PyBool>()
                    && (item.is_instance_of::<PyFloat>() || item.is_instance_of::<PyInt>())
            });
        if is_numeric_vector && let Ok(vals) = obj.extract::<Vec<f32>>() {
            return Ok(OracleCellValue::Vector(OracleVectorData::Float32(vals)));
        }
        let py = obj.py();
        let ser_mod = py
            .import("vertebra.utils.serializers")
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let json_bytes: Vec<u8> = ser_mod
            .getattr("encode_json_bytes")
            .and_then(|f| f.call1((obj,)))
            .and_then(|b| b.extract())
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Oson(
            encode_oson_payload(&json_bytes).to_vec(),
        ));
    }

    Err(OracleError::data_error(format!(
        "Unsupported Python bind parameter type: {type_name}"
    )))
}

/// Convert an `oracledb::JsonValue` tree into a `serde_json::Value` and wrap in an `OSON` frame.
pub fn json_value_to_oson_bytes(val: &oracledb::JsonValue) -> Result<Vec<u8>, OracleError> {
    let json_str = render_oracledb_json_value(val)?;
    Ok(encode_oson_payload(json_str.as_bytes()).to_vec())
}

fn render_oracledb_json_value(val: &oracledb::JsonValue) -> Result<String, OracleError> {
    match val {
        oracledb::JsonValue::Null => Ok("null".to_owned()),
        oracledb::JsonValue::Boolean(b) => Ok(if *b { "true" } else { "false" }.to_owned()),
        oracledb::JsonValue::BinaryFloat(f) => Ok(f.to_string()),
        oracledb::JsonValue::BinaryDouble(d) => Ok(d.to_string()),
        oracledb::JsonValue::Number(n) => Ok(n.to_string()),
        oracledb::JsonValue::String(s) => Ok(escape_json_string(s)),
        oracledb::JsonValue::Timestamp(ts) => Ok(escape_json_string(&ts.to_string())),
        oracledb::JsonValue::JsonArray(arr) => {
            let mut parts = Vec::with_capacity(arr.len());
            for item in arr {
                parts.push(render_oracledb_json_value(item)?);
            }
            Ok(format!("[{}]", parts.join(",")))
        }
        oracledb::JsonValue::JsonObject(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut parts = Vec::with_capacity(keys.len());
            for k in keys {
                let v = &map[k];
                parts.push(format!(
                    "{}:{}",
                    escape_json_string(k),
                    render_oracledb_json_value(v)?
                ));
            }
            Ok(format!("{{{}}}", parts.join(",")))
        }
        other => Ok(escape_json_string(&format!("{other:?}"))),
    }
}

fn escape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Decode a single `oracledb::Row` into a vector of owned [`OracleCellValue`]s on the worker thread.
pub fn decode_oracledb_row(
    row: &mut oracledb::Row,
    columns: &[OracleColumn],
) -> Result<Vec<OracleCellValue>, OracleError> {
    let mut out = Vec::with_capacity(columns.len());
    for (idx, col) in columns.iter().enumerate() {
        let type_upper = col.type_name.to_ascii_uppercase();
        let cell = if type_upper.contains("BOOLEAN") {
            match row
                .get::<Option<bool>>(idx)
                .map_err(|e| OracleError::from_oracledb(&e))?
            {
                Some(b) => OracleCellValue::Bool(b),
                None => OracleCellValue::Null,
            }
        } else if type_upper.contains("BINARY_FLOAT") || type_upper.contains("BINARY_DOUBLE") {
            match row
                .get::<Option<f64>>(idx)
                .map_err(|e| OracleError::from_oracledb(&e))?
            {
                Some(f) => OracleCellValue::Float(f),
                None => OracleCellValue::Null,
            }
        } else if type_upper.contains("NUMBER") || type_upper.contains("INTEGER") {
            match row
                .get::<Option<oracledb::OracleNumber>>(idx)
                .map_err(|e| OracleError::from_oracledb(&e))?
            {
                Some(num) => {
                    let s = num.to_string();
                    if col.scale.unwrap_or(0) <= 0 && !s.contains('.') {
                        if let Ok(iv) = s.parse::<i64>() {
                            OracleCellValue::Int(iv)
                        } else {
                            OracleCellValue::DecimalStr(s)
                        }
                    } else {
                        OracleCellValue::DecimalStr(s)
                    }
                }
                None => OracleCellValue::Null,
            }
        } else if type_upper == "DATE" {
            match row
                .get::<Option<oracledb::OracleTimestamp>>(idx)
                .map_err(|e| OracleError::from_oracledb(&e))?
            {
                Some(ts) => {
                    if ts.hour() == 0
                        && ts.minute() == 0
                        && ts.second() == 0
                        && ts.nanoseconds() == 0
                    {
                        OracleCellValue::Date {
                            year: i32::from(ts.year()),
                            month: ts.month(),
                            day: ts.day(),
                        }
                    } else {
                        OracleCellValue::Timestamp {
                            year: i32::from(ts.year()),
                            month: ts.month(),
                            day: ts.day(),
                            hour: ts.hour(),
                            minute: ts.minute(),
                            second: ts.second(),
                            nanosecond: ts.nanoseconds(),
                            tz_offset_seconds: None,
                        }
                    }
                }
                None => OracleCellValue::Null,
            }
        } else if type_upper.contains("TIMESTAMP") {
            match row
                .get::<Option<oracledb::OracleTimestamp>>(idx)
                .map_err(|e| OracleError::from_oracledb(&e))?
            {
                Some(ts) => {
                    let tz_offset_seconds = if ts.is_utc() || type_upper.contains("TIME ZONE") {
                        Some(0)
                    } else {
                        None
                    };
                    OracleCellValue::Timestamp {
                        year: i32::from(ts.year()),
                        month: ts.month(),
                        day: ts.day(),
                        hour: ts.hour(),
                        minute: ts.minute(),
                        second: ts.second(),
                        nanosecond: ts.nanoseconds(),
                        tz_offset_seconds,
                    }
                }
                None => OracleCellValue::Null,
            }
        } else if type_upper.contains("JSON") {
            match row
                .get::<Option<oracledb::JsonValue>>(idx)
                .map_err(|e| OracleError::from_oracledb(&e))?
            {
                Some(jv) => OracleCellValue::Oson(json_value_to_oson_bytes(&jv)?),
                None => OracleCellValue::Null,
            }
        } else if type_upper.contains("VECTOR") {
            match row
                .get::<Option<oracledb::Vector>>(idx)
                .map_err(|e| OracleError::from_oracledb(&e))?
            {
                Some(vec) => OracleCellValue::Vector(OracleVectorData::from_oracledb_vector(&vec)?),
                None => OracleCellValue::Null,
            }
        } else if type_upper.contains("RAW") {
            match row
                .get::<Option<Vec<u8>>>(idx)
                .map_err(|e| OracleError::from_oracledb(&e))?
            {
                Some(b) => OracleCellValue::Bytes(b),
                None => OracleCellValue::Null,
            }
        } else if type_upper.contains("BLOB") || type_upper.contains("BFILE") {
            if let Ok(opt_bytes) = row.get::<Option<Vec<u8>>>(idx) {
                match opt_bytes {
                    Some(b) => OracleCellValue::Bytes(b),
                    None => OracleCellValue::Null,
                }
            } else {
                match row
                    .take::<Option<oracledb::Lob>>(idx)
                    .map_err(|e| OracleError::from_oracledb(&e))?
                {
                    Some(mut lob) => {
                        let mut buf = Vec::new();
                        lob.read_to_end(&mut buf)
                            .map_err(|e| OracleError::operational(e.to_string()))?;
                        OracleCellValue::Bytes(buf)
                    }
                    None => OracleCellValue::Null,
                }
            }
        } else if type_upper.contains("CLOB") {
            if let Ok(opt_str) = row.get::<Option<String>>(idx) {
                match opt_str {
                    Some(s) => OracleCellValue::Str(s),
                    None => OracleCellValue::Null,
                }
            } else {
                match row
                    .take::<Option<oracledb::Lob>>(idx)
                    .map_err(|e| OracleError::from_oracledb(&e))?
                {
                    Some(mut lob) => {
                        let mut buf = Vec::new();
                        lob.read_to_end(&mut buf)
                            .map_err(|e| OracleError::operational(e.to_string()))?;
                        let text = String::from_utf8(buf)
                            .map_err(|e| OracleError::data_error(e.to_string()))?;
                        OracleCellValue::Str(text)
                    }
                    None => OracleCellValue::Null,
                }
            }
        } else {
            match row
                .get::<Option<String>>(idx)
                .map_err(|e| OracleError::from_oracledb(&e))?
            {
                Some(s) => OracleCellValue::Str(s),
                None => OracleCellValue::Null,
            }
        };
        out.push(cell);
    }
    Ok(out)
}

fn serde_json_to_oracledb_json(val: serde_json::Value) -> Result<oracledb::JsonValue, OracleError> {
    match val {
        serde_json::Value::Null => Ok(oracledb::JsonValue::Null),
        serde_json::Value::Bool(b) => Ok(oracledb::JsonValue::Boolean(b)),
        serde_json::Value::Number(n) => {
            let text = n.to_string();
            if let Ok(ora_num) = oracledb::OracleNumber::from_str(&text) {
                Ok(oracledb::JsonValue::Number(ora_num))
            } else if let Some(fv) = n.as_f64() {
                Ok(oracledb::JsonValue::BinaryDouble(fv))
            } else {
                Err(OracleError::data_error(format!(
                    "Unsupported JSON numeric literal: {text}"
                )))
            }
        }
        serde_json::Value::String(s) => Ok(oracledb::JsonValue::String(s)),
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(serde_json_to_oracledb_json(item)?);
            }
            Ok(oracledb::JsonValue::JsonArray(out))
        }
        serde_json::Value::Object(map) => {
            let mut out = HashMap::with_capacity(map.len());
            for (k, v) in map {
                out.insert(k, serde_json_to_oracledb_json(v)?);
            }
            Ok(oracledb::JsonValue::JsonObject(out))
        }
    }
}

/// Convert an [`OracleCellValue`] into a boxed `oracledb::ToDbValue` for query/execute binding.
pub fn cell_to_oracledb_bind(
    cell: &OracleCellValue,
) -> Result<Box<dyn oracledb::ToDbValue + Send + Sync>, OracleError> {
    match cell {
        OracleCellValue::Null => Ok(Box::new(None::<String>)),
        OracleCellValue::Bool(b) => Ok(Box::new(*b)),
        OracleCellValue::Int(i) => Ok(Box::new(*i)),
        OracleCellValue::Float(f) => Ok(Box::new(*f)),
        OracleCellValue::DecimalStr(s) => {
            let num =
                oracledb::OracleNumber::from_str(s).map_err(|e| OracleError::from_oracledb(&e))?;
            Ok(Box::new(num))
        }
        OracleCellValue::Str(s) => Ok(Box::new(s.clone())),
        OracleCellValue::Bytes(b) => Ok(Box::new(b.clone())),
        OracleCellValue::Date { year, month, day } => Ok(Box::new(
            oracledb::OracleTimestamp::new_date(*year as i16, *month, *day),
        )),
        OracleCellValue::Timestamp {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            tz_offset_seconds,
        } => {
            let ts = if let Some(offset_secs) = tz_offset_seconds {
                let date =
                    chrono::NaiveDate::from_ymd_opt(*year, u32::from(*month), u32::from(*day))
                        .ok_or_else(|| {
                            OracleError::data_error("Invalid timestamp date components")
                        })?;
                let dt = date
                    .and_hms_nano_opt(
                        u32::from(*hour),
                        u32::from(*minute),
                        u32::from(*second),
                        *nanosecond,
                    )
                    .ok_or_else(|| OracleError::data_error("Invalid timestamp time components"))?;
                let utc_dt = dt - chrono::Duration::seconds(i64::from(*offset_secs));
                oracledb::OracleTimestamp::new_utc_timestamp(
                    utc_dt.year() as i16,
                    utc_dt.month() as u8,
                    utc_dt.day() as u8,
                    utc_dt.hour() as u8,
                    utc_dt.minute() as u8,
                    utc_dt.second() as u8,
                    utc_dt.nanosecond(),
                )
            } else {
                oracledb::OracleTimestamp::new_timestamp(
                    *year as i16,
                    *month,
                    *day,
                    *hour,
                    *minute,
                    *second,
                    *nanosecond,
                )
            };
            Ok(Box::new(ts))
        }
        OracleCellValue::Oson(oson_bytes) => {
            let json_slice = decode_oson_payload(oson_bytes).map_err(OracleError::data_error)?;
            let parsed: serde_json::Value = serde_json::from_slice(json_slice)
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            let ora_json = serde_json_to_oracledb_json(parsed)?;
            Ok(Box::new(ora_json))
        }
        OracleCellValue::Vector(vec_data) => Ok(Box::new(vec_data.to_oracledb_vector())),
    }
}

/// Build an output bind placeholder value for `DML RETURNING INTO` or PL/SQL `OUT` binds.
pub fn out_bind_type_placeholder(type_hint: &str) -> Box<dyn oracledb::ToDbValue + Send + Sync> {
    let upper = type_hint.trim().to_ascii_uppercase();
    if upper.contains("NUMBER")
        || upper.contains("INT")
        || upper.contains("FLOAT")
        || upper.contains("DECIMAL")
    {
        Box::new(oracledb::DB_TYPE_NUMBER)
    } else if upper == "RAW_MAX" || upper.contains("BLOB") {
        Box::new(vec![0u8; 32767])
    } else if upper.contains("RAW") || upper.contains("BYTES") {
        Box::new(oracledb::DB_TYPE_RAW)
    } else if upper.contains("DATE") {
        Box::new(oracledb::DB_TYPE_DATE)
    } else if upper.contains("TIMESTAMP") {
        Box::new(oracledb::DB_TYPE_TIMESTAMP)
    } else if upper.contains("BOOL") {
        Box::new(oracledb::DB_TYPE_BOOLEAN)
    } else if upper.contains("JSON") || upper.contains("OSON") {
        Box::new(oracledb::DB_TYPE_JSON)
    } else if upper.contains("VECTOR") {
        Box::new(oracledb::DB_TYPE_VECTOR)
    } else if upper == "VARCHAR2_MAX" || upper.contains("CLOB") {
        Box::new(" ".repeat(32767))
    } else {
        Box::new(oracledb::DB_TYPE_VARCHAR)
    }
}

/// Helper to normalize named parameter dictionary keys (stripping any leading `:`).
pub fn normalize_named_bind_map(
    raw: HashMap<String, OracleCellValue>,
) -> Vec<(String, OracleCellValue)> {
    let mut pairs: Vec<(String, OracleCellValue)> = raw
        .into_iter()
        .map(|(k, v)| {
            let clean = k.strip_prefix(':').unwrap_or(&k).to_owned();
            (clean, v)
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
}

/// Return format for decoded Oracle 26ai `VECTOR` cells.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum VectorReturnFormat {
    #[default]
    List,
    VectorView,
    Bytes,
}

impl VectorReturnFormat {
    /// Parse a string option (`"list"`, `"vector_view"`, or `"bytes"`).
    #[allow(dead_code)]
    pub fn parse(raw: &str) -> Result<Self, OracleError> {
        match raw {
            "list" => Ok(Self::List),
            "vector_view" => Ok(Self::VectorView),
            "bytes" => Ok(Self::Bytes),
            other => Err(OracleError::Interface(format!(
                "Unsupported vector_return_format: {other:?}; expected 'list', 'vector_view', or 'bytes'"
            ))),
        }
    }

    /// Return the canonical string name for Python codec interop.
    #[allow(dead_code)]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::VectorView => "vector_view",
            Self::Bytes => "bytes",
        }
    }
}

/// Per-cursor/driver options governing Oracle value coercion on the worker boundary.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OracleValueOptions {
    pub enable_uuid_binary: bool,
    pub fetch_decimals: bool,
    pub vector_return_format: VectorReturnFormat,
}

impl Default for OracleValueOptions {
    fn default() -> Self {
        Self {
            enable_uuid_binary: true,
            fetch_decimals: false,
            vector_return_format: VectorReturnFormat::List,
        }
    }
}

#[allow(dead_code)]
static DECODE_OSON_FN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
#[allow(dead_code)]
static ENCODE_OSON_FN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
#[allow(dead_code)]
static DECODE_VECTOR_FN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
#[allow(dead_code)]
static ENCODE_VECTOR_FN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
#[allow(dead_code)]
static UUID_CLS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
#[allow(dead_code)]
static DECIMAL_CLS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
#[allow(dead_code)]
static VECTOR_VIEW_CLS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
#[allow(dead_code)]
static MSGSPEC_STRUCT_CLS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

#[allow(dead_code)]
fn cached_oracle_core_callable<'py>(
    py: Python<'py>,
    slot: &'static PyOnceLock<Py<PyAny>>,
    attr: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let obj = slot.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        let core_mod = py.import("vertebra.db.adapters.oracle.core")?;
        let core_cls = core_mod.getattr("OracleCore")?;
        Ok(core_cls.getattr(attr)?.unbind())
    })?;
    Ok(obj.bind(py).clone())
}

#[allow(dead_code)]
fn cached_decode_oson_fn<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    cached_oracle_core_callable(py, &DECODE_OSON_FN, "decode_oson")
}

#[allow(dead_code)]
fn cached_encode_oson_fn<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    cached_oracle_core_callable(py, &ENCODE_OSON_FN, "encode_oson")
}

#[allow(dead_code)]
fn cached_decode_vector_fn<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    cached_oracle_core_callable(py, &DECODE_VECTOR_FN, "decode_vector")
}

#[allow(dead_code)]
fn cached_encode_vector_fn<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    cached_oracle_core_callable(py, &ENCODE_VECTOR_FN, "encode_vector")
}

#[allow(dead_code)]
fn cached_uuid_cls<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let obj = UUID_CLS.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import("uuid")?.getattr("UUID")?.unbind())
    })?;
    Ok(obj.bind(py).clone())
}

#[allow(dead_code)]
fn cached_decimal_cls<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let obj = DECIMAL_CLS.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import("decimal")?.getattr("Decimal")?.unbind())
    })?;
    Ok(obj.bind(py).clone())
}

#[allow(dead_code)]
fn cached_vector_view_cls<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let obj = VECTOR_VIEW_CLS.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py
            .import("vertebra._vertebra")?
            .getattr("VectorView")?
            .unbind())
    })?;
    Ok(obj.bind(py).clone())
}

#[allow(dead_code)]
fn cached_msgspec_struct_cls<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let obj = MSGSPEC_STRUCT_CLS.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import("msgspec")?.getattr("Struct")?.unbind())
    })?;
    Ok(obj.bind(py).clone())
}

#[allow(dead_code)]
fn column_is_raw_or_unknown(column_type: Option<&str>) -> bool {
    match column_type {
        None => true,
        Some(ct) => ct.is_empty() || ct.to_ascii_uppercase().contains("RAW"),
    }
}

/// Coerce an [`OracleCellValue`] into a Python object matching `OracleCore.coerce_row_cell`.
#[allow(dead_code)]
pub fn coerce_cell_to_py(
    py: Python<'_>,
    cell: OracleCellValue,
    column_type: Option<&str>,
    opts: OracleValueOptions,
) -> PyResult<Py<PyAny>> {
    match cell {
        OracleCellValue::Null => Ok(py.None()),
        OracleCellValue::Bool(b) => Ok(PyBool::new(py, b).to_owned().into_any().unbind()),
        OracleCellValue::Int(v) => {
            if opts.fetch_decimals {
                let dec_cls = cached_decimal_cls(py)?;
                Ok(dec_cls.call1((v.to_string(),))?.unbind())
            } else {
                Ok(PyInt::new(py, v).into_any().unbind())
            }
        }
        OracleCellValue::Float(f) => {
            if opts.fetch_decimals {
                let dec_cls = cached_decimal_cls(py)?;
                let py_str = PyFloat::new(py, f).str()?;
                Ok(dec_cls.call1((py_str,))?.unbind())
            } else {
                Ok(PyFloat::new(py, f).into_any().unbind())
            }
        }
        OracleCellValue::DecimalStr(s) => {
            if !opts.fetch_decimals
                && !s.contains('.')
                && !s.contains('e')
                && !s.contains('E')
                && let Ok(iv) = s.parse::<i64>()
            {
                return Ok(PyInt::new(py, iv).into_any().unbind());
            }
            let dec_cls = cached_decimal_cls(py)?;
            let dec = dec_cls.call1((&s,))?;
            if !opts.fetch_decimals && dec.call_method0("is_finite")?.is_truthy()? {
                let integral = dec.call_method0("to_integral_value")?;
                if dec.eq(&integral)? {
                    return Ok(py.get_type::<PyInt>().call1((dec,))?.unbind());
                }
            }
            Ok(dec.unbind())
        }
        OracleCellValue::Str(s) => Ok(PyString::new(py, &s).into_any().unbind()),
        OracleCellValue::Bytes(raw) => {
            if raw.len() >= 8 && &raw[..3] == OSON_MAGIC && raw[3] == 0x01 {
                let decode_oson = cached_decode_oson_fn(py)?;
                return Ok(decode_oson.call1((PyBytes::new(py, &raw),))?.unbind());
            }
            if raw.len() >= 8 && raw[0] == ORACLE_VECTOR_MAGIC && raw[1] == ORACLE_VECTOR_VERSION {
                let decode_vec = cached_decode_vector_fn(py)?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("return_format", opts.vector_return_format.as_str())?;
                return Ok(decode_vec
                    .call((PyBytes::new(py, &raw),), Some(&kwargs))?
                    .unbind());
            }
            if opts.enable_uuid_binary && raw.len() == 16 && column_is_raw_or_unknown(column_type) {
                let uuid_cls = cached_uuid_cls(py)?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("bytes", PyBytes::new(py, &raw))?;
                return Ok(uuid_cls.call((), Some(&kwargs))?.unbind());
            }
            Ok(PyBytes::new(py, &raw).into_any().unbind())
        }
        OracleCellValue::Vector(vec_data) => {
            let vv = vec_data
                .into_py_vector_view(py)
                .map_err(|e| e.to_py_err(py))?;
            match opts.vector_return_format {
                VectorReturnFormat::VectorView => Ok(vv.unbind()),
                VectorReturnFormat::List => {
                    let decode_vec = cached_decode_vector_fn(py)?;
                    let kwargs = PyDict::new(py);
                    kwargs.set_item("return_format", "list")?;
                    Ok(decode_vec.call((vv,), Some(&kwargs))?.unbind())
                }
                VectorReturnFormat::Bytes => Ok(vv.call_method0("to_bytes")?.unbind()),
            }
        }
        other @ (OracleCellValue::Date { .. }
        | OracleCellValue::Timestamp { .. }
        | OracleCellValue::Oson(_)) => other
            .into_py_object(py, opts.fetch_decimals)
            .map(Bound::unbind)
            .map_err(|e| e.to_py_err(py)),
    }
}

/// Coerce and extract a Python bind parameter matching `OracleCore.coerce_bind_parameter` followed by [`extract_bind_value`].
#[allow(dead_code)]
pub fn coerce_bind_from_py(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    opts: OracleValueOptions,
) -> Result<OracleCellValue, OracleError> {
    if value.is_none() {
        return Ok(OracleCellValue::Null);
    }
    if value.is_exact_instance_of::<PyBool>() {
        let b = value
            .extract::<bool>()
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Bool(b));
    }
    if value.is_exact_instance_of::<PyInt>() {
        if let Ok(iv) = value.extract::<i64>() {
            return Ok(OracleCellValue::Int(iv));
        }
        let text = value
            .str()
            .and_then(|s| Ok(s.to_str()?.to_owned()))
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::DecimalStr(text));
    }
    if value.is_exact_instance_of::<PyFloat>() {
        let fv = value
            .extract::<f64>()
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Float(fv));
    }
    if value.is_exact_instance_of::<PyString>() {
        let sv = value
            .extract::<String>()
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Str(sv));
    }
    if value.is_exact_instance_of::<PyBytes>() {
        return extract_bind_value(value);
    }

    let uuid_cls = cached_uuid_cls(py).map_err(|e| OracleError::data_error(e.to_string()))?;
    if value
        .is_instance(&uuid_cls)
        .map_err(|e| OracleError::data_error(e.to_string()))?
    {
        if opts.enable_uuid_binary {
            let raw_bytes = value
                .getattr("bytes")
                .and_then(|b| b.extract::<Vec<u8>>())
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            return Ok(OracleCellValue::Bytes(raw_bytes));
        }
        let text = value
            .str()
            .and_then(|s| Ok(s.to_str()?.to_owned()))
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return Ok(OracleCellValue::Str(text));
    }

    if let Ok(vv_cls) = cached_vector_view_cls(py)
        && value
            .is_instance(&vv_cls)
            .map_err(|e| OracleError::data_error(e.to_string()))?
    {
        let encode_vec =
            cached_encode_vector_fn(py).map_err(|e| OracleError::data_error(e.to_string()))?;
        let encoded = encode_vec
            .call1((value,))
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return extract_bind_value(&encoded);
    }

    let is_dict_or_struct = value.is_instance_of::<PyDict>()
        || cached_msgspec_struct_cls(py)
            .is_ok_and(|struct_cls| value.is_instance(&struct_cls).unwrap_or(false));
    if is_dict_or_struct {
        let encode_oson =
            cached_encode_oson_fn(py).map_err(|e| OracleError::data_error(e.to_string()))?;
        let encoded = encode_oson
            .call1((value,))
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        return extract_bind_value(&encoded);
    }

    extract_bind_value(value)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn ensure_test_python_path(py: Python<'_>) {
        let sys = py.import("sys").unwrap();
        let sys_path = sys.getattr("path").unwrap();
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let mut candidates = vec![repo_root.join("src/py")];
        if let Ok(entries) = std::fs::read_dir(repo_root.join(".venv/lib")) {
            for entry in entries.flatten() {
                let site_packages = entry.path().join("site-packages");
                if site_packages.is_dir() {
                    candidates.push(site_packages);
                }
            }
        }
        for path in candidates {
            if let Ok(canonical) = path.canonicalize()
                && let Some(text) = canonical.to_str()
            {
                let contains: bool = sys_path
                    .call_method1("__contains__", (text,))
                    .unwrap()
                    .extract()
                    .unwrap();
                if !contains {
                    sys_path.call_method1("insert", (0, text)).unwrap();
                }
            }
        }
    }

    #[test]
    fn test_vector_wire_roundtrip_and_invalid_magic() {
        let f32_vec = OracleVectorData::Float32(vec![1.5, -2.25, 3.0]);
        let f32_bytes = f32_vec.encode_wire_bytes();
        assert_eq!(&f32_bytes[0..2], &[0xDB, 0x01]);
        assert_eq!(
            OracleVectorData::decode_wire_bytes(&f32_bytes).unwrap(),
            f32_vec
        );

        let f64_vec = OracleVectorData::Float64(vec![1.125, -0.5]);
        let f64_bytes = f64_vec.encode_wire_bytes();
        assert_eq!(
            OracleVectorData::decode_wire_bytes(&f64_bytes).unwrap(),
            f64_vec
        );

        let i8_vec = OracleVectorData::Int8(vec![-128, 0, 127]);
        let i8_bytes = i8_vec.encode_wire_bytes();
        assert_eq!(
            OracleVectorData::decode_wire_bytes(&i8_bytes).unwrap(),
            i8_vec
        );

        let bin_vec = OracleVectorData::Binary(vec![0xAA, 0x55]);
        let bin_bytes = bin_vec.encode_wire_bytes();
        assert_eq!(
            OracleVectorData::decode_wire_bytes(&bin_bytes).unwrap(),
            bin_vec
        );

        let bad_err =
            OracleVectorData::decode_wire_bytes(b"\x00\x01\x02\x00\x00\x00\x00\x01").unwrap_err();
        assert_eq!(bad_err.classify().sqlstate, "22000");
    }

    #[test]
    fn test_vector_return_format_parse_and_options_default() {
        assert_eq!(
            VectorReturnFormat::parse("list").unwrap(),
            VectorReturnFormat::List
        );
        assert_eq!(
            VectorReturnFormat::parse("vector_view").unwrap(),
            VectorReturnFormat::VectorView
        );
        assert_eq!(
            VectorReturnFormat::parse("bytes").unwrap(),
            VectorReturnFormat::Bytes
        );
        assert!(matches!(
            VectorReturnFormat::parse("numpy"),
            Err(OracleError::Interface(_))
        ));

        let opts = OracleValueOptions::default();
        assert!(opts.enable_uuid_binary);
        assert!(!opts.fetch_decimals);
        assert_eq!(opts.vector_return_format, VectorReturnFormat::List);
    }

    #[test]
    fn test_coerce_cell_to_py_uuid_and_binary_rules() {
        Python::attach(|py| {
            let uuid_bytes: Vec<u8> = (0u8..16).collect();
            let uuid_cls = py.import("uuid").unwrap().getattr("UUID").unwrap();

            let out_raw = coerce_cell_to_py(
                py,
                OracleCellValue::Bytes(uuid_bytes.clone()),
                Some("RAW"),
                OracleValueOptions::default(),
            )
            .unwrap();
            assert!(out_raw.bind(py).is_instance(&uuid_cls).unwrap());
            assert_eq!(
                out_raw
                    .bind(py)
                    .getattr("bytes")
                    .unwrap()
                    .extract::<Vec<u8>>()
                    .unwrap(),
                uuid_bytes
            );

            let out_unknown = coerce_cell_to_py(
                py,
                OracleCellValue::Bytes(uuid_bytes.clone()),
                None,
                OracleValueOptions::default(),
            )
            .unwrap();
            assert!(out_unknown.bind(py).is_instance(&uuid_cls).unwrap());

            let out_disabled = coerce_cell_to_py(
                py,
                OracleCellValue::Bytes(uuid_bytes.clone()),
                Some("RAW"),
                OracleValueOptions {
                    enable_uuid_binary: false,
                    ..Default::default()
                },
            )
            .unwrap();
            assert!(out_disabled.bind(py).is_exact_instance_of::<PyBytes>());
            assert_eq!(out_disabled.extract::<Vec<u8>>(py).unwrap(), uuid_bytes);

            let out_blob = coerce_cell_to_py(
                py,
                OracleCellValue::Bytes(uuid_bytes.clone()),
                Some("BLOB"),
                OracleValueOptions::default(),
            )
            .unwrap();
            assert!(out_blob.bind(py).is_exact_instance_of::<PyBytes>());
            assert_eq!(out_blob.extract::<Vec<u8>>(py).unwrap(), uuid_bytes);
        });
    }

    #[test]
    fn test_coerce_cell_to_py_numbers_bool_and_null() {
        Python::attach(|py| {
            let dec_cls = py.import("decimal").unwrap().getattr("Decimal").unwrap();
            let default_opts = OracleValueOptions::default();
            let decimal_opts = OracleValueOptions {
                fetch_decimals: true,
                ..Default::default()
            };

            let int_dec = coerce_cell_to_py(
                py,
                OracleCellValue::DecimalStr("5".to_owned()),
                Some("NUMBER"),
                default_opts,
            )
            .unwrap();
            assert!(int_dec.bind(py).is_exact_instance_of::<PyInt>());
            assert_eq!(int_dec.extract::<i64>(py).unwrap(), 5);

            let int_dec_trailing = coerce_cell_to_py(
                py,
                OracleCellValue::DecimalStr("5.0".to_owned()),
                Some("NUMBER"),
                default_opts,
            )
            .unwrap();
            assert!(int_dec_trailing.bind(py).is_exact_instance_of::<PyInt>());
            assert_eq!(int_dec_trailing.extract::<i64>(py).unwrap(), 5);

            let frac_dec = coerce_cell_to_py(
                py,
                OracleCellValue::DecimalStr("5.5".to_owned()),
                Some("NUMBER"),
                default_opts,
            )
            .unwrap();
            assert!(frac_dec.bind(py).is_instance(&dec_cls).unwrap());
            assert_eq!(frac_dec.bind(py).str().unwrap().to_str().unwrap(), "5.5");

            let float_to_dec = coerce_cell_to_py(
                py,
                OracleCellValue::Float(2.5),
                Some("BINARY_DOUBLE"),
                decimal_opts,
            )
            .unwrap();
            assert!(float_to_dec.bind(py).is_instance(&dec_cls).unwrap());
            assert_eq!(
                float_to_dec.bind(py).str().unwrap().to_str().unwrap(),
                "2.5"
            );

            let bool_untouched = coerce_cell_to_py(
                py,
                OracleCellValue::Bool(true),
                Some("BOOLEAN"),
                decimal_opts,
            )
            .unwrap();
            assert!(bool_untouched.bind(py).is_exact_instance_of::<PyBool>());
            assert!(bool_untouched.extract::<bool>(py).unwrap());

            let null_val =
                coerce_cell_to_py(py, OracleCellValue::Null, None, default_opts).unwrap();
            assert!(null_val.is_none(py));
        });
    }

    #[test]
    fn test_coerce_bind_from_py_uuid_and_dict() {
        Python::attach(|py| {
            ensure_test_python_path(py);
            let uuid_cls = py.import("uuid").unwrap().getattr("UUID").unwrap();
            let uuid_str = "00010203-0405-0607-0809-0a0b0c0d0e0f";
            let uuid_obj = uuid_cls.call1((uuid_str,)).unwrap();

            let bound_uuid =
                coerce_bind_from_py(py, &uuid_obj, OracleValueOptions::default()).unwrap();
            assert_eq!(
                bound_uuid,
                OracleCellValue::Bytes((0u8..16).collect::<Vec<u8>>())
            );

            let bound_uuid_str = coerce_bind_from_py(
                py,
                &uuid_obj,
                OracleValueOptions {
                    enable_uuid_binary: false,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(bound_uuid_str, OracleCellValue::Str(uuid_str.to_owned()));

            let dict = PyDict::new(py);
            dict.set_item("a", 1i64).unwrap();
            let bound_dict =
                coerce_bind_from_py(py, dict.as_any(), OracleValueOptions::default()).unwrap();
            match bound_dict {
                OracleCellValue::Oson(bytes) => {
                    assert!(bytes.len() >= 8);
                    assert_eq!(&bytes[..3], OSON_MAGIC);
                    assert_eq!(bytes[3], 0x01);

                    let decoded = coerce_cell_to_py(
                        py,
                        OracleCellValue::Bytes(bytes),
                        Some("JSON"),
                        OracleValueOptions::default(),
                    )
                    .unwrap();
                    let decoded_dict = decoded.bind(py).cast::<PyDict>().unwrap();
                    assert_eq!(
                        decoded_dict
                            .get_item("a")
                            .unwrap()
                            .unwrap()
                            .extract::<i64>()
                            .unwrap(),
                        1
                    );
                }
                other => panic!("expected Oson bind value, got {other:?}"),
            }

            let vec_bytes = OracleVectorData::Float32(vec![1.5, -2.25]).encode_wire_bytes();
            let decoded_vec = coerce_cell_to_py(
                py,
                OracleCellValue::Bytes(vec_bytes),
                Some("VECTOR"),
                OracleValueOptions::default(),
            )
            .unwrap();
            assert_eq!(
                decoded_vec.extract::<Vec<f32>>(py).unwrap(),
                vec![1.5f32, -2.25f32]
            );
        });
    }
}
