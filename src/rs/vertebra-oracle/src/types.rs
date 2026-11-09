//! Oracle 26ai scalar, temporal, inline LOB, OSON, and VECTOR codecs and PyO3 result types.

use std::collections::HashMap;
use std::io::Read;
use std::str::FromStr;

use pyo3::prelude::*;
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
        let type_name = db_type.name().to_owned();
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
        tz_hour_offset: Option<i8>,
        tz_minute_offset: Option<i8>,
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
                    v.into_pyobject(py)
                        .map(Bound::into_any)
                        .map_err(|e| OracleError::data_error(e.to_string()))
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
                    f.into_pyobject(py)
                        .map(Bound::into_any)
                        .map_err(|e| OracleError::data_error(e.to_string()))
                }
            }
            Self::DecimalStr(s) => {
                if !fetch_decimals
                    && !s.contains('.')
                    && !s.contains('e')
                    && !s.contains('E')
                    && let Ok(iv) = s.parse::<i64>()
                {
                    return iv
                        .into_pyobject(py)
                        .map(Bound::into_any)
                        .map_err(|e| OracleError::data_error(e.to_string()));
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
                tz_hour_offset,
                tz_minute_offset,
            } => {
                let dt_mod = py
                    .import("datetime")
                    .map_err(|e| OracleError::data_error(e.to_string()))?;
                let microsecond = nanosecond / 1000;
                if let (Some(tz_h), Some(tz_m)) = (tz_hour_offset, tz_minute_offset) {
                    let total_seconds = (i32::from(tz_h) * 3600) + (i32::from(tz_m) * 60);
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

        let mut tz_hour_offset = None;
        let mut tz_minute_offset = None;
        if let Ok(offset_obj) = obj.call_method0("utcoffset")
            && !offset_obj.is_none()
            && let Ok(total_secs_f) = offset_obj
                .call_method0("total_seconds")
                .and_then(|s| s.extract::<f64>())
        {
            let total_secs = total_secs_f.round() as i32;
            tz_hour_offset = Some((total_secs / 3600) as i8);
            tz_minute_offset = Some(((total_secs % 3600) / 60) as i8);
        }
        return Ok(OracleCellValue::Timestamp {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond: microsecond * 1000,
            tz_hour_offset,
            tz_minute_offset,
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
                            tz_hour_offset: None,
                            tz_minute_offset: None,
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
                    let (tz_h, tz_m) = if ts.is_utc() || type_upper.contains("TIME ZONE") {
                        (Some(0), Some(0))
                    } else {
                        (None, None)
                    };
                    OracleCellValue::Timestamp {
                        year: i32::from(ts.year()),
                        month: ts.month(),
                        day: ts.day(),
                        hour: ts.hour(),
                        minute: ts.minute(),
                        second: ts.second(),
                        nanosecond: ts.nanoseconds(),
                        tz_hour_offset: tz_h,
                        tz_minute_offset: tz_m,
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
            tz_hour_offset,
            ..
        } => {
            let ts = if tz_hour_offset.is_some() {
                oracledb::OracleTimestamp::new_utc_timestamp(
                    *year as i16,
                    *month,
                    *day,
                    *hour,
                    *minute,
                    *second,
                    *nanosecond,
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
            let text = std::str::from_utf8(json_slice)
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            Ok(Box::new(oracledb::JsonValue::String(text.to_owned())))
        }
        OracleCellValue::Vector(vec_data) => Ok(Box::new(vec_data.to_oracledb_vector())),
    }
}

/// Build an output bind placeholder value for `DML RETURNING INTO` or PL/SQL `OUT` binds.
pub fn out_bind_type_placeholder(type_hint: &str) -> Box<dyn oracledb::ToDbValue + Send + Sync> {
    let upper = type_hint.trim().to_ascii_uppercase();
    if upper.contains("NUMBER") || upper.contains("INT") {
        Box::new(oracledb::DB_TYPE_NUMBER)
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
    } else {
        Box::new(" ".repeat(256))
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
