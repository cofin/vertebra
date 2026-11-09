//! Shared PyO3 extraction and projection bridge between `vertebra.db.adapters.postgres.types` and `PgValue` / `QueryResult`.

use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple, PyType,
};

use crate::codecs::array::MAX_ARRAY_DIMS;
use crate::codecs::temporal::{
    PG_DATE_MAX_FINITE, PG_DATE_MIN_FINITE, PG_TS_MAX_FINITE, PG_TS_MIN_FINITE, pg_days_to_ymd,
    validate_date_days, validate_time_micros, ymd_to_pg_days,
};
use crate::codecs::{
    BIT_OID, BOOL_OID, BYTEA_OID, DATE_OID, FLOAT4_OID, FLOAT8_OID, INT2_OID, INT4_OID, INT8_OID,
    INTERVAL_OID, JSON_OID, JSONB_OID, NUMERIC_OID, OID_OID, TEXT_OID, TIME_OID, TIMESTAMP_OID,
    TIMESTAMPTZ_OID, TypeCatalog, TypeInfo, TypeKind, UUID_OID, VARBIT_OID,
};
use crate::errors::PostgresError;
use crate::extended::Parameter;
use crate::result::{
    PG_DATE_NEG_INF, PG_DATE_POS_INF, PG_TIME_MAX_MICROS, PG_TS_NEG_INF, PG_TS_POS_INF, PgNumeric,
    PgValue, QueryResult, TemporalKind, TransactionStatus, VectorKind,
};

const PY_DATE_MIN_PG_DAYS: i32 = -730_119;
const PY_DATE_MAX_PG_DAYS: i32 = 2_921_939;
const PY_TS_MIN_PG_MICROS: i64 = -63_113_904_000_000_000;
const PY_TS_MAX_PG_MICROS: i64 = 252_455_616_000_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;

struct CachedPythonTypes {
    postgres_column: Py<PyType>,
    postgres_query_result: Py<PyType>,
    pg_interval: Py<PyType>,
    pg_array: Py<PyType>,
    pg_composite: Py<PyType>,
    pg_temporal: Py<PyType>,
    pg_json: Py<PyType>,
    pg_bit_string: Py<PyType>,
    pg_vector: Py<PyType>,
    pg_parameter: Py<PyType>,
    decimal: Py<PyType>,
    uuid: Py<PyType>,
    date: Py<PyType>,
    time: Py<PyType>,
    datetime: Py<PyType>,
    timezone_utc: Py<PyAny>,
    msgspec_json_encode: Py<PyAny>,
    msgspec_json_decode: Py<PyAny>,
}

static PYTHON_TYPES: PyOnceLock<CachedPythonTypes> = PyOnceLock::new();

fn resolve_python_types(py: Python<'_>) -> Result<&'static CachedPythonTypes, PostgresError> {
    PYTHON_TYPES
        .get_or_try_init(py, || -> PyResult<CachedPythonTypes> {
            let pg_types = py.import("vertebra.db.adapters.postgres.types")?;
            let get_ty = |m: &Bound<'_, PyModule>, name: &str| -> PyResult<Py<PyType>> {
                Ok(m.getattr(name)?.cast_into::<PyType>()?.unbind())
            };
            let decimal_mod = py.import("decimal")?;
            let uuid_mod = py.import("uuid")?;
            let dt_mod = py.import("datetime")?;
            let timezone_utc = dt_mod.getattr("timezone")?.getattr("utc")?.unbind();
            let msgspec_json = py.import("msgspec.json")?;

            Ok(CachedPythonTypes {
                postgres_column: get_ty(&pg_types, "PostgresColumn")?,
                postgres_query_result: get_ty(&pg_types, "PostgresQueryResult")?,
                pg_interval: get_ty(&pg_types, "PgInterval")?,
                pg_array: get_ty(&pg_types, "PgArray")?,
                pg_composite: get_ty(&pg_types, "PgComposite")?,
                pg_temporal: get_ty(&pg_types, "PgTemporal")?,
                pg_json: get_ty(&pg_types, "PgJson")?,
                pg_bit_string: get_ty(&pg_types, "PgBitString")?,
                pg_vector: get_ty(&pg_types, "PgVector")?,
                pg_parameter: get_ty(&pg_types, "PgParameter")?,
                decimal: get_ty(&decimal_mod, "Decimal")?,
                uuid: get_ty(&uuid_mod, "UUID")?,
                date: get_ty(&dt_mod, "date")?,
                time: get_ty(&dt_mod, "time")?,
                datetime: get_ty(&dt_mod, "datetime")?,
                timezone_utc,
                msgspec_json_encode: msgspec_json.getattr("encode")?.unbind(),
                msgspec_json_decode: msgspec_json.getattr("decode")?.unbind(),
            })
        })
        .map_err(|e| PostgresError::Interface(format!("failed to load Python types: {e}")))
}

/// Extract ordered query parameters and optional `parameter_oids` into owned Rust [`Parameter`] records before releasing the GIL.
pub fn extract_parameters(
    py: Python<'_>,
    parameters: &Bound<'_, PyAny>,
    parameter_oids: Option<&Bound<'_, PyAny>>,
) -> Result<(Vec<Parameter>, Option<Vec<u32>>), PostgresError> {
    let types = resolve_python_types(py)?;
    if parameters.is_none() {
        if let Some(oids_obj) = parameter_oids.filter(|o| !o.is_none()) {
            let oids = extract_oid_sequence(oids_obj)?;
            if !oids.is_empty() {
                return Err(PostgresError::Data(format!(
                    "parameter_oids length {} does not match parameters length 0",
                    oids.len()
                )));
            }
            return Ok((Vec::new(), Some(oids)));
        }
        return Ok((Vec::new(), None));
    }

    if parameters.is_instance_of::<PyString>()
        || parameters.is_instance_of::<PyBytes>()
        || parameters.is_instance_of::<PyByteArray>()
        || parameters.is_instance_of::<PyDict>()
    {
        return Err(PostgresError::Interface(
            "parameters must be an ordered sequence (tuple or list), not str/bytes/dict"
                .to_string(),
        ));
    }

    let items: Vec<Bound<'_, PyAny>> = parameters
        .try_iter()
        .map_err(|_| {
            PostgresError::Interface("parameters must be an iterable sequence".to_string())
        })?
        .collect::<PyResult<Vec<_>>>()
        .map_err(|e| PostgresError::Interface(format!("failed to iterate parameters: {e}")))?;

    let explicit_oids = match parameter_oids.filter(|o| !o.is_none()) {
        Some(oids_obj) => {
            let oids = extract_oid_sequence(oids_obj)?;
            if oids.len() != items.len() {
                return Err(PostgresError::Data(format!(
                    "parameter_oids length {} does not match parameters length {}",
                    oids.len(),
                    items.len()
                )));
            }
            Some(oids)
        }
        None => None,
    };

    let mut out_params = Vec::with_capacity(items.len());
    for (idx, item) in items.iter().enumerate() {
        let vec_oid = explicit_oids.as_ref().and_then(|v| v.get(idx).copied());
        if item
            .is_instance(types.pg_parameter.bind(py))
            .map_err(|e| PostgresError::Data(e.to_string()))?
        {
            let wrapper_oid = extract_positive_u32_attr(item, "type_oid")?;
            if let Some(v_oid) = vec_oid
                && v_oid != 0
                && v_oid != wrapper_oid
            {
                return Err(PostgresError::Data(format!(
                    "parameter_oids[{idx}] = {v_oid} conflicts with PgParameter.type_oid = {wrapper_oid}"
                )));
            }
            let inner = item
                .getattr("value")
                .map_err(|e| PostgresError::Data(e.to_string()))?;
            let val = extract_pg_value(py, &inner, Some(wrapper_oid))?;
            out_params.push(Parameter {
                value: val,
                type_oid: Some(wrapper_oid),
            });
        } else {
            let target = vec_oid.filter(|&o| o != 0);
            let val = extract_pg_value(py, item, target)?;
            out_params.push(Parameter {
                value: val,
                type_oid: target,
            });
        }
    }

    Ok((out_params, explicit_oids))
}

fn extract_oid_sequence(obj: &Bound<'_, PyAny>) -> Result<Vec<u32>, PostgresError> {
    if obj.is_instance_of::<PyString>()
        || obj.is_instance_of::<PyBytes>()
        || obj.is_instance_of::<PyByteArray>()
        || obj.is_instance_of::<PyDict>()
    {
        return Err(PostgresError::Interface(
            "parameter_oids must be a sequence of unsigned 32-bit integers".to_string(),
        ));
    }
    let iter = obj.try_iter().map_err(|_| {
        PostgresError::Interface(
            "parameter_oids must be a sequence of unsigned 32-bit integers".to_string(),
        )
    })?;
    let mut oids = Vec::new();
    for entry in iter {
        let item = entry.map_err(|e| PostgresError::Interface(e.to_string()))?;
        let oid = extract_u32_no_bool(&item, "parameter_oid")?;
        oids.push(oid);
    }
    Ok(oids)
}

fn extract_u32_no_bool(obj: &Bound<'_, PyAny>, field: &str) -> Result<u32, PostgresError> {
    if obj.is_instance_of::<PyBool>() || !obj.is_instance_of::<PyInt>() {
        return Err(PostgresError::Data(format!(
            "{field} must be an integer (not bool)"
        )));
    }
    obj.extract::<u32>()
        .map_err(|_| PostgresError::Data(format!("{field} is outside 0..=4294967295")))
}

fn extract_positive_u32_attr(obj: &Bound<'_, PyAny>, attr: &str) -> Result<u32, PostgresError> {
    let val = obj
        .getattr(attr)
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let oid = extract_u32_no_bool(&val, attr)?;
    if oid == 0 {
        return Err(PostgresError::Data(format!(
            "{attr} must be a positive unsigned 32-bit OID (>= 1)"
        )));
    }
    Ok(oid)
}

/// Extract a single Python object into an owned [`PgValue`] according to optional `target_oid`.
pub fn extract_pg_value(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    target_oid: Option<u32>,
) -> Result<PgValue, PostgresError> {
    let types = resolve_python_types(py)?;
    let target = target_oid.filter(|&o| o != 0);

    if obj.is_none() {
        return Ok(PgValue::Null);
    }

    if obj
        .is_instance(types.pg_parameter.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        return Err(PostgresError::Data(
            "PgParameter is only valid as a top-level query parameter, not nested inside a SQL value"
                .to_string(),
        ));
    }

    if obj.is_instance_of::<PyBool>() {
        if let Some(oid) = target
            && oid != BOOL_OID
        {
            return Err(PostgresError::Data(format!(
                "cannot bind bool value to non-BOOL target OID {oid}"
            )));
        }
        let b = obj
            .extract::<bool>()
            .map_err(|e| PostgresError::Data(e.to_string()))?;
        return Ok(PgValue::Bool(b));
    }

    if obj.is_instance_of::<PyInt>() {
        return extract_py_int(obj, target);
    }

    if obj.is_instance_of::<PyFloat>() {
        let v = obj
            .extract::<f64>()
            .map_err(|e| PostgresError::Data(e.to_string()))?;
        return match target {
            Some(FLOAT4_OID) => {
                if v.is_finite() && v.abs() > (f32::MAX as f64) {
                    return Err(PostgresError::Data(format!(
                        "float value {v} overflows PostgreSQL FLOAT4 range"
                    )));
                }
                Ok(PgValue::Float4(v as f32))
            }
            Some(FLOAT8_OID) | None => Ok(PgValue::Float8(v)),
            Some(other) => Err(PostgresError::Data(format!(
                "cannot bind float value to target OID {other}"
            ))),
        };
    }

    if obj.is_instance_of::<PyString>() {
        let s = obj
            .extract::<String>()
            .map_err(|e| PostgresError::Data(e.to_string()))?;
        if s.as_bytes().contains(&0) {
            return Err(PostgresError::Data(
                "PostgreSQL text value cannot contain embedded NUL byte".to_string(),
            ));
        }
        return Ok(PgValue::Text(s));
    }

    if let Some(bytes_vec) = try_extract_owned_bytes(py, obj)? {
        return Ok(PgValue::Bytes(bytes_vec));
    }

    if obj
        .is_instance(types.decimal.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        let num = extract_py_decimal(obj)?;
        return Ok(PgValue::Numeric(num));
    }

    if obj
        .is_instance(types.uuid.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        let int_val: u128 = obj
            .getattr("int")
            .and_then(|v| v.extract::<u128>())
            .map_err(|e| PostgresError::Data(format!("failed to extract UUID.int: {e}")))?;
        return Ok(PgValue::Uuid(int_val.to_be_bytes()));
    }

    if obj
        .is_instance(types.datetime.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        return extract_py_datetime(py, obj, types, target);
    }

    if obj
        .is_instance(types.date.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        if let Some(oid) = target
            && oid != DATE_OID
        {
            return Err(PostgresError::Data(format!(
                "cannot bind datetime.date to non-DATE target OID {oid}"
            )));
        }
        let year: i32 = obj.getattr("year").unwrap().extract().unwrap();
        let month: u8 = obj.getattr("month").unwrap().extract().unwrap();
        let day: u8 = obj.getattr("day").unwrap().extract().unwrap();
        let days = ymd_to_pg_days(year, month, day)?;
        return Ok(PgValue::Date(days));
    }

    if obj
        .is_instance(types.time.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        if let Some(oid) = target
            && oid != TIME_OID
        {
            return Err(PostgresError::Data(format!(
                "cannot bind datetime.time to non-TIME target OID {oid}"
            )));
        }
        let tzinfo = obj
            .getattr("tzinfo")
            .map_err(|e| PostgresError::Data(e.to_string()))?;
        if !tzinfo.is_none() {
            return Err(PostgresError::Data(
                "timezone-aware datetime.time (TIMETZ) is not supported".to_string(),
            ));
        }
        let hour: i64 = obj.getattr("hour").unwrap().extract().unwrap();
        let minute: i64 = obj.getattr("minute").unwrap().extract().unwrap();
        let second: i64 = obj.getattr("second").unwrap().extract().unwrap();
        let micro: i64 = obj.getattr("microsecond").unwrap().extract().unwrap();
        let us = ((hour * 3600 + minute * 60 + second) * 1_000_000) + micro;
        return Ok(PgValue::Time(validate_time_micros(us)?));
    }

    if obj
        .is_instance(types.pg_temporal.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        return extract_pg_temporal(obj, target);
    }

    if obj
        .is_instance(types.pg_interval.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        if let Some(oid) = target
            && oid != INTERVAL_OID
        {
            return Err(PostgresError::Data(format!(
                "cannot bind PgInterval to non-INTERVAL target OID {oid}"
            )));
        }
        let months: i32 = obj.getattr("months").unwrap().extract().unwrap();
        let days: i32 = obj.getattr("days").unwrap().extract().unwrap();
        let microseconds: i64 = obj.getattr("microseconds").unwrap().extract().unwrap();
        return Ok(PgValue::Interval {
            months,
            days,
            microseconds,
        });
    }

    if obj
        .is_instance(types.pg_json.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        if let Some(oid) = target
            && oid != JSON_OID
            && oid != JSONB_OID
        {
            return Err(PostgresError::Data(format!(
                "cannot bind PgJson to non-JSON target OID {oid}"
            )));
        }
        let val = obj
            .getattr("value")
            .map_err(|e| PostgresError::Data(e.to_string()))?;
        let json_bytes = encode_python_json(py, &val, types)?;
        return Ok(PgValue::Json(json_bytes));
    }

    if obj
        .is_instance(types.pg_bit_string.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        if let Some(oid) = target
            && oid != BIT_OID
            && oid != VARBIT_OID
        {
            return Err(PostgresError::Data(format!(
                "cannot bind PgBitString to non-BIT/VARBIT target OID {oid}"
            )));
        }
        let data: Vec<u8> = obj
            .getattr("data")
            .and_then(|v| v.extract::<Vec<u8>>())
            .map_err(|e| PostgresError::Data(e.to_string()))?;
        let bit_length: i32 = obj
            .getattr("bit_length")
            .and_then(|v| v.extract::<i32>())
            .map_err(|e| PostgresError::Data(e.to_string()))?;
        return Ok(PgValue::BitString { data, bit_length });
    }

    if obj
        .is_instance(types.pg_vector.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        let kind_str: String = obj
            .getattr("kind")
            .and_then(|v| v.extract::<String>())
            .map_err(|e| PostgresError::Data(e.to_string()))?;
        let vals_f64: Vec<f64> = obj
            .getattr("values")
            .and_then(|v| v.extract::<Vec<f64>>())
            .map_err(|e| PostgresError::Data(e.to_string()))?;
        if vals_f64.is_empty() || vals_f64.len() > 16_000 {
            return Err(PostgresError::Data(format!(
                "PgVector dimension {} must lie in 1..=16000",
                vals_f64.len()
            )));
        }
        let kind = match kind_str.as_str() {
            "vector" => VectorKind::Vector,
            "halfvec" => VectorKind::HalfVec,
            other => {
                return Err(PostgresError::Data(format!(
                    "unsupported PgVector.kind `{other}`"
                )));
            }
        };
        let mut values = Vec::with_capacity(vals_f64.len());
        for v in vals_f64 {
            if !v.is_finite() {
                return Err(PostgresError::Data(
                    "PgVector elements must be finite floats".to_string(),
                ));
            }
            let f32_val = v as f32;
            if !f32_val.is_finite() {
                return Err(PostgresError::Data(format!(
                    "PgVector element {v} overflows f32"
                )));
            }
            if kind == VectorKind::HalfVec {
                let h = half::f16::from_f32(f32_val);
                if !h.is_finite() {
                    return Err(PostgresError::Data(format!(
                        "PgVector halfvec element {v} overflows f16"
                    )));
                }
                values.push(h.to_f32());
            } else {
                values.push(f32_val);
            }
        }
        return Ok(PgValue::Vector { values, kind });
    }

    if obj
        .is_instance(types.pg_array.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        return extract_pg_array(py, obj);
    }

    if obj
        .is_instance(types.pg_composite.bind(py))
        .map_err(|e| PostgresError::Data(e.to_string()))?
    {
        return extract_pg_composite(py, obj);
    }

    if obj.is_instance_of::<PyDict>() {
        if let Some(oid) = target
            && oid != JSON_OID
            && oid != JSONB_OID
        {
            return Err(PostgresError::Data(format!(
                "cannot bind dict to non-JSON target OID {oid}"
            )));
        }
        let json_bytes = encode_python_json(py, obj, types)?;
        return Ok(PgValue::Json(json_bytes));
    }

    if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
        if target == Some(JSON_OID) || target == Some(JSONB_OID) {
            let json_bytes = encode_python_json(py, obj, types)?;
            return Ok(PgValue::Json(json_bytes));
        }
        return extract_convenience_sequence(py, obj, target);
    }

    Err(PostgresError::Data(format!(
        "unsupported Python parameter value type `{}`",
        obj.get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "<unknown>".to_string())
    )))
}

fn extract_py_int(obj: &Bound<'_, PyAny>, target: Option<u32>) -> Result<PgValue, PostgresError> {
    match target {
        Some(INT2_OID) => {
            let v = obj.extract::<i16>().map_err(|_| {
                PostgresError::Data("integer parameter overflows INT2 (-32768..=32767)".to_string())
            })?;
            Ok(PgValue::Int2(v))
        }
        Some(INT4_OID) => {
            let v = obj.extract::<i32>().map_err(|_| {
                PostgresError::Data(
                    "integer parameter overflows INT4 (-2147483648..=2147483647)".to_string(),
                )
            })?;
            Ok(PgValue::Int4(v))
        }
        Some(INT8_OID) => {
            let v = obj.extract::<i64>().map_err(|_| {
                PostgresError::Data("integer parameter overflows INT8 (64-bit signed)".to_string())
            })?;
            Ok(PgValue::Int8(v))
        }
        Some(OID_OID) => {
            let u = obj.extract::<u32>().map_err(|_| {
                PostgresError::Data(
                    "OID parameter must lie in 0..=4294967295 (unsigned 32-bit)".to_string(),
                )
            })?;
            Ok(PgValue::Int8(i64::from(u)))
        }
        Some(FLOAT4_OID) => {
            let v = obj
                .extract::<f64>()
                .map_err(|_| PostgresError::Data("integer overflows FLOAT4".to_string()))?;
            if !v.is_finite() || v.abs() > (f32::MAX as f64) {
                return Err(PostgresError::Data(
                    "integer parameter overflows FLOAT4".to_string(),
                ));
            }
            Ok(PgValue::Float4(v as f32))
        }
        Some(FLOAT8_OID) => {
            let v = obj
                .extract::<f64>()
                .map_err(|_| PostgresError::Data("integer overflows FLOAT8".to_string()))?;
            if !v.is_finite() {
                return Err(PostgresError::Data(
                    "integer parameter overflows FLOAT8".to_string(),
                ));
            }
            Ok(PgValue::Float8(v))
        }
        Some(NUMERIC_OID) => {
            let s = obj
                .str()
                .and_then(|p| p.extract::<String>())
                .map_err(|e| PostgresError::Data(e.to_string()))?;
            let num = crate::codecs::numeric::decode_numeric_text(&s)?;
            Ok(PgValue::Numeric(num))
        }
        None => {
            if let Ok(v32) = obj.extract::<i32>() {
                Ok(PgValue::Int4(v32))
            } else if let Ok(v64) = obj.extract::<i64>() {
                Ok(PgValue::Int8(v64))
            } else {
                Err(PostgresError::Data(
                    "integer parameter overflows 64-bit signed integer; wrap in Decimal for NUMERIC"
                        .to_string(),
                ))
            }
        }
        Some(other) => {
            if let Ok(v32) = obj.extract::<i32>() {
                Ok(PgValue::Int4(v32))
            } else if let Ok(v64) = obj.extract::<i64>() {
                Ok(PgValue::Int8(v64))
            } else {
                Err(PostgresError::Data(format!(
                    "cannot bind integer parameter to target OID {other}"
                )))
            }
        }
    }
}

fn try_extract_owned_bytes(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
) -> Result<Option<Vec<u8>>, PostgresError> {
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok(Some(b.as_bytes().to_vec()));
    }
    if let Ok(ba) = obj.cast::<PyByteArray>() {
        return Ok(Some(ba.to_vec()));
    }
    if let Ok(buf) = PyBuffer::<u8>::get(obj) {
        let vec = buf
            .to_vec(py)
            .map_err(|e| PostgresError::Data(format!("failed to copy buffer bytes: {e}")))?;
        return Ok(Some(vec));
    }
    Ok(None)
}

fn extract_py_decimal(obj: &Bound<'_, PyAny>) -> Result<PgNumeric, PostgresError> {
    let tup = obj
        .call_method0("as_tuple")
        .map_err(|e| PostgresError::Data(format!("Decimal.as_tuple() failed: {e}")))?;
    let sign_bit: u8 = tup
        .getattr("sign")
        .and_then(|v| v.extract::<u8>())
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let negative = sign_bit != 0;
    let digits_obj = tup
        .getattr("digits")
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let digits: Vec<u8> = digits_obj
        .extract::<Vec<u8>>()
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let exp_obj = tup
        .getattr("exponent")
        .map_err(|e| PostgresError::Data(e.to_string()))?;

    if let Ok(exp_str) = exp_obj.extract::<String>() {
        return match exp_str.as_str() {
            "n" => Ok(PgNumeric::NaN),
            "N" => PgNumeric::from_decimal_parts(negative, &digits, 0, true),
            "F" => Ok(if negative {
                PgNumeric::NegInf
            } else {
                PgNumeric::PosInf
            }),
            other => Err(PostgresError::Data(format!(
                "unsupported Decimal special exponent `{other}`"
            ))),
        };
    }

    let exponent: i32 = exp_obj.extract::<i32>().map_err(|_| {
        PostgresError::Data("Decimal exponent overflows 32-bit signed integer".to_string())
    })?;
    PgNumeric::from_decimal_parts(negative, &digits, exponent, false)
}

fn extract_py_datetime(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    types: &CachedPythonTypes,
    target: Option<u32>,
) -> Result<PgValue, PostgresError> {
    let tzinfo = obj
        .getattr("tzinfo")
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let is_aware = !tzinfo.is_none()
        && !tzinfo
            .call_method1("utcoffset", (obj,))
            .map_err(|e| PostgresError::Data(e.to_string()))?
            .is_none();

    if is_aware {
        if let Some(oid) = target
            && oid != TIMESTAMPTZ_OID
        {
            return Err(PostgresError::Data(format!(
                "cannot bind timezone-aware datetime to non-TIMESTAMPTZ target OID {oid}"
            )));
        }
        let utc_dt = obj
            .call_method1("astimezone", (types.timezone_utc.bind(py),))
            .map_err(|e| PostgresError::Data(format!("datetime.astimezone(UTC) failed: {e}")))?;
        let us = py_datetime_fields_to_pg_micros(&utc_dt)?;
        Ok(PgValue::TimestampTz(us))
    } else {
        if let Some(oid) = target
            && oid != TIMESTAMP_OID
        {
            return Err(PostgresError::Data(format!(
                "cannot bind naive datetime to non-TIMESTAMP target OID {oid}"
            )));
        }
        let us = py_datetime_fields_to_pg_micros(obj)?;
        Ok(PgValue::Timestamp(us))
    }
}

fn py_datetime_fields_to_pg_micros(dt: &Bound<'_, PyAny>) -> Result<i64, PostgresError> {
    let year: i32 = dt.getattr("year").unwrap().extract().unwrap();
    let month: u8 = dt.getattr("month").unwrap().extract().unwrap();
    let day: u8 = dt.getattr("day").unwrap().extract().unwrap();
    let hour: i64 = dt.getattr("hour").unwrap().extract().unwrap();
    let minute: i64 = dt.getattr("minute").unwrap().extract().unwrap();
    let second: i64 = dt.getattr("second").unwrap().extract().unwrap();
    let micro: i64 = dt.getattr("microsecond").unwrap().extract().unwrap();
    let days = i64::from(ymd_to_pg_days(year, month, day)?);
    let time_us = ((hour * 3600 + minute * 60 + second) * 1_000_000) + micro;
    Ok(days * MICROS_PER_DAY + time_us)
}

fn extract_pg_temporal(
    obj: &Bound<'_, PyAny>,
    target: Option<u32>,
) -> Result<PgValue, PostgresError> {
    let kind: String = obj
        .getattr("kind")
        .and_then(|v| v.extract::<String>())
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let special: Option<String> = obj
        .getattr("special")
        .and_then(|v| v.extract::<Option<String>>())
        .map_err(|e| PostgresError::Data(e.to_string()))?;

    if special.as_deref() == Some("wire_text") {
        return Err(PostgresError::Data(
            "cannot bind unresolved PgTemporal(special='wire_text') parameter; resolve to an unambiguous instant or bind explicit raw text"
                .to_string(),
        ));
    }

    let raw_val: i64 = obj
        .getattr("value")
        .and_then(|v| v.extract::<i64>())
        .map_err(|_| {
            PostgresError::Data("non-wire_text PgTemporal requires an integer value".to_string())
        })?;

    match kind.as_str() {
        "date" => {
            if let Some(oid) = target
                && oid != DATE_OID
            {
                return Err(PostgresError::Data(format!(
                    "PgTemporal(kind='date') conflicts with target OID {oid}"
                )));
            }
            let days = i32::try_from(raw_val).map_err(|_| {
                PostgresError::Data("PgTemporal date value overflows i32".to_string())
            })?;
            Ok(PgValue::Date(validate_date_days(days)?))
        }
        "time" => {
            if let Some(oid) = target
                && oid != TIME_OID
            {
                return Err(PostgresError::Data(format!(
                    "PgTemporal(kind='time') conflicts with target OID {oid}"
                )));
            }
            Ok(PgValue::Time(validate_time_micros(raw_val)?))
        }
        "timestamp" => {
            if let Some(oid) = target
                && oid != TIMESTAMP_OID
            {
                return Err(PostgresError::Data(format!(
                    "PgTemporal(kind='timestamp') conflicts with target OID {oid}"
                )));
            }
            Ok(PgValue::Timestamp(raw_val))
        }
        "timestamptz" => {
            if let Some(oid) = target
                && oid != TIMESTAMPTZ_OID
            {
                return Err(PostgresError::Data(format!(
                    "PgTemporal(kind='timestamptz') conflicts with target OID {oid}"
                )));
            }
            Ok(PgValue::TimestampTz(raw_val))
        }
        other => Err(PostgresError::Data(format!(
            "unsupported PgTemporal.kind `{other}`"
        ))),
    }
}

fn encode_python_json(
    py: Python<'_>,
    val: &Bound<'_, PyAny>,
    types: &CachedPythonTypes,
) -> Result<Vec<u8>, PostgresError> {
    validate_json_bound(py, val, types, 0)?;
    let encoded = types
        .msgspec_json_encode
        .bind(py)
        .call1((val,))
        .map_err(|e| PostgresError::Data(format!("JSON serialization failed: {e}")))?;
    let bytes = encoded
        .cast::<PyBytes>()
        .map_err(|e| PostgresError::Data(format!("msgspec.json.encode did not return bytes: {e}")))?
        .as_bytes()
        .to_vec();
    Ok(bytes)
}

fn validate_json_bound(
    py: Python<'_>,
    node: &Bound<'_, PyAny>,
    types: &CachedPythonTypes,
    depth: usize,
) -> Result<(), PostgresError> {
    if depth > 64 {
        return Err(PostgresError::Data(
            "JSON value exceeds maximum nesting depth (64) or contains a reference cycle"
                .to_string(),
        ));
    }
    if node.is_none()
        || node.is_instance_of::<PyBool>()
        || node.is_instance_of::<PyInt>()
        || node.is_instance_of::<PyString>()
    {
        return Ok(());
    }
    if node.is_instance_of::<PyFloat>() {
        let f = node
            .extract::<f64>()
            .map_err(|e| PostgresError::Data(e.to_string()))?;
        if !f.is_finite() {
            return Err(PostgresError::Data(
                "JSON float values must be finite (NaN/Infinity are not valid RFC 8259 JSON)"
                    .to_string(),
            ));
        }
        return Ok(());
    }
    if let Ok(list) = node.cast::<PyList>() {
        for item in list.iter() {
            validate_json_bound(py, &item, types, depth + 1)?;
        }
        return Ok(());
    }
    if let Ok(dict) = node.cast::<PyDict>() {
        for (k, v) in dict.iter() {
            if !k.is_instance_of::<PyString>() {
                return Err(PostgresError::Data(
                    "JSON object keys must be strings".to_string(),
                ));
            }
            validate_json_bound(py, &v, types, depth + 1)?;
        }
        return Ok(());
    }
    if node
        .is_instance(types.pg_parameter.bind(py))
        .unwrap_or(false)
    {
        return Err(PostgresError::Data(
            "PgParameter cannot be nested inside a JSON document".to_string(),
        ));
    }
    Err(PostgresError::Data(format!(
        "unsupported value type `{}` inside JSON document",
        node.get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "<unknown>".to_string())
    )))
}

fn extract_pg_array(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<PgValue, PostgresError> {
    let element_oid: u32 = obj
        .getattr("element_oid")
        .and_then(|v| v.extract::<u32>())
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let dimensions: Vec<i32> = obj
        .getattr("dimensions")
        .and_then(|v| v.extract::<Vec<i32>>())
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let lower_bounds: Vec<i32> = obj
        .getattr("lower_bounds")
        .and_then(|v| v.extract::<Vec<i32>>())
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let raw_vals = obj
        .getattr("values")
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let iter = raw_vals
        .try_iter()
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let mut values = Vec::new();
    for entry in iter {
        let item = entry.map_err(|e| PostgresError::Data(e.to_string()))?;
        values.push(extract_pg_value(py, &item, Some(element_oid))?);
    }
    Ok(PgValue::Array {
        values,
        dimensions,
        lower_bounds,
        element_oid,
    })
}

fn extract_pg_composite(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<PgValue, PostgresError> {
    let type_oid: u32 = obj
        .getattr("type_oid")
        .and_then(|v| v.extract::<u32>())
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let field_names: Vec<String> = obj
        .getattr("field_names")
        .and_then(|v| v.extract::<Vec<String>>())
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let field_oids: Vec<u32> = obj
        .getattr("field_oids")
        .and_then(|v| v.extract::<Vec<u32>>())
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let raw_vals = obj
        .getattr("values")
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let iter = raw_vals
        .try_iter()
        .map_err(|e| PostgresError::Data(e.to_string()))?;
    let mut values = Vec::with_capacity(field_oids.len());
    for (idx, entry) in iter.enumerate() {
        let item = entry.map_err(|e| PostgresError::Data(e.to_string()))?;
        let f_oid = field_oids.get(idx).copied();
        values.push(extract_pg_value(py, &item, f_oid)?);
    }
    Ok(PgValue::Composite {
        type_oid,
        field_names,
        field_oids,
        values,
    })
}

fn extract_convenience_sequence(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    target_array_oid: Option<u32>,
) -> Result<PgValue, PostgresError> {
    let catalog = TypeCatalog::new_with_builtins();
    let target_elem_oid = match target_array_oid {
        Some(arr_oid) => match catalog.type_info(arr_oid) {
            Some(TypeInfo {
                kind: TypeKind::Array { element_oid, .. },
                ..
            }) => Some(*element_oid),
            _ => None,
        },
        None => None,
    };

    let mut dimensions = Vec::new();
    let mut flat_values = Vec::new();
    let mut inferred_elem_oid: Option<u32> = target_elem_oid;

    flatten_py_sequence_level(
        py,
        obj,
        1,
        &mut dimensions,
        &mut flat_values,
        &mut inferred_elem_oid,
    )?;

    if flat_values.is_empty() {
        let Some(elem_oid) = inferred_elem_oid else {
            return Err(PostgresError::Data(
                "cannot bind untyped empty sequence as PostgreSQL array without an explicit PgArray or array target OID"
                    .to_string(),
            ));
        };
        return Ok(PgValue::Array {
            values: Vec::new(),
            dimensions: Vec::new(),
            lower_bounds: Vec::new(),
            element_oid: elem_oid,
        });
    }

    let Some(elem_oid) = inferred_elem_oid else {
        return Err(PostgresError::Data(
            "cannot bind all-NULL sequence as PostgreSQL array without an explicit PgArray or array target OID"
                .to_string(),
        ));
    };

    let lower_bounds = vec![1; dimensions.len()];
    Ok(PgValue::Array {
        values: flat_values,
        dimensions,
        lower_bounds,
        element_oid: elem_oid,
    })
}

fn flatten_py_sequence_level(
    py: Python<'_>,
    seq: &Bound<'_, PyAny>,
    depth: usize,
    dimensions: &mut Vec<i32>,
    flat_values: &mut Vec<PgValue>,
    inferred_elem_oid: &mut Option<u32>,
) -> Result<(), PostgresError> {
    if depth > MAX_ARRAY_DIMS {
        return Err(PostgresError::Data(format!(
            "convenience array rank {depth} exceeds MAX_ARRAY_DIMS ({MAX_ARRAY_DIMS})"
        )));
    }
    let items: Vec<Bound<'_, PyAny>> = seq
        .try_iter()
        .map_err(|e| PostgresError::Data(e.to_string()))?
        .collect::<PyResult<Vec<_>>>()
        .map_err(|e| PostgresError::Data(e.to_string()))?;

    let len = i32::try_from(items.len()).map_err(|_| {
        PostgresError::Data("convenience array dimension overflows i32".to_string())
    })?;

    if items.is_empty() {
        if depth > 1 {
            return Err(PostgresError::Data(
                "nested convenience array cannot contain empty inner sequences".to_string(),
            ));
        }
        return Ok(());
    }

    if dimensions.len() < depth {
        dimensions.push(len);
    } else if dimensions[depth - 1] != len {
        return Err(PostgresError::Data(format!(
            "ragged convenience sequence at depth {depth}: expected length {}, got {len}",
            dimensions[depth - 1]
        )));
    }

    let first_is_seq = items[0].is_instance_of::<PyList>() || items[0].is_instance_of::<PyTuple>();
    for item in &items {
        let is_seq = item.is_instance_of::<PyList>() || item.is_instance_of::<PyTuple>();
        if is_seq != first_is_seq {
            return Err(PostgresError::Data(
                "ragged convenience sequence mixes nested sequences and scalar values at the same depth"
                    .to_string(),
            ));
        }
        if is_seq {
            flatten_py_sequence_level(
                py,
                item,
                depth + 1,
                dimensions,
                flat_values,
                inferred_elem_oid,
            )?;
        } else {
            let val = extract_pg_value(py, item, *inferred_elem_oid)?;
            if let Some(val_oid) = default_scalar_oid(&val) {
                match *inferred_elem_oid {
                    None => *inferred_elem_oid = Some(val_oid),
                    Some(existing) if existing != val_oid => {
                        return Err(PostgresError::Data(format!(
                            "heterogeneous convenience sequence mixes element OID {existing} and {val_oid}"
                        )));
                    }
                    Some(_) => {}
                }
            }
            flat_values.push(val);
        }
    }
    Ok(())
}

fn default_scalar_oid(val: &PgValue) -> Option<u32> {
    match val {
        PgValue::Null => None,
        PgValue::Bool(_) => Some(BOOL_OID),
        PgValue::Int2(_) => Some(INT2_OID),
        PgValue::Int4(_) => Some(INT4_OID),
        PgValue::Int8(_) => Some(INT8_OID),
        PgValue::Float4(_) => Some(FLOAT4_OID),
        PgValue::Float8(_) => Some(FLOAT8_OID),
        PgValue::Text(_) => Some(TEXT_OID),
        PgValue::Bytes(_) => Some(BYTEA_OID),
        PgValue::Uuid(_) => Some(UUID_OID),
        PgValue::Numeric(_) => Some(NUMERIC_OID),
        PgValue::Date(_) => Some(DATE_OID),
        PgValue::Time(_) => Some(TIME_OID),
        PgValue::Timestamp(_) => Some(TIMESTAMP_OID),
        PgValue::TimestampTz(_) => Some(TIMESTAMPTZ_OID),
        PgValue::Interval { .. } => Some(INTERVAL_OID),
        PgValue::Json(_) => Some(JSONB_OID),
        PgValue::BitString { .. } => Some(VARBIT_OID),
        PgValue::Composite { type_oid, .. } => Some(*type_oid),
        PgValue::TemporalText { .. } | PgValue::Array { .. } | PgValue::Vector { .. } => None,
    }
}

/// Project an owned [`PgValue`] into its canonical Python representation (`PostgresValue`).
pub fn pg_value_to_python(py: Python<'_>, value: &PgValue) -> PyResult<Py<PyAny>> {
    let types = resolve_python_types(py).map_err(|e| crate::errors::postgres_error_to_py(py, e))?;
    pg_value_to_python_with_types(py, value, types)
}

fn pg_value_to_python_with_types(
    py: Python<'_>,
    value: &PgValue,
    types: &CachedPythonTypes,
) -> PyResult<Py<PyAny>> {
    match value {
        PgValue::Null => Ok(py.None()),
        PgValue::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        PgValue::Int2(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        PgValue::Int4(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        PgValue::Int8(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        PgValue::Float4(v) => Ok((*v as f64).into_pyobject(py)?.into_any().unbind()),
        PgValue::Float8(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        PgValue::Text(s) => Ok(s.as_str().into_pyobject(py)?.into_any().unbind()),
        PgValue::Bytes(b) => Ok(PyBytes::new(py, b).into_any().unbind()),
        PgValue::Uuid(b) => {
            let int_val = u128::from_be_bytes(*b);
            let kwargs = PyDict::new(py);
            kwargs.set_item("int", int_val)?;
            Ok(types.uuid.bind(py).call((), Some(&kwargs))?.unbind())
        }
        PgValue::Numeric(num) => {
            let s = num.to_canonical_string();
            Ok(types.decimal.bind(py).call1((s.as_str(),))?.unbind())
        }
        PgValue::Date(days) => {
            if *days == PG_DATE_POS_INF {
                Ok(types
                    .pg_temporal
                    .bind(py)
                    .call1(("date", *days, "positive_infinity"))?
                    .unbind())
            } else if *days == PG_DATE_NEG_INF {
                Ok(types
                    .pg_temporal
                    .bind(py)
                    .call1(("date", *days, "negative_infinity"))?
                    .unbind())
            } else if (PY_DATE_MIN_PG_DAYS..=PY_DATE_MAX_PG_DAYS).contains(days) {
                let (y, m, d) = pg_days_to_ymd(*days);
                Ok(types.date.bind(py).call1((y, m, d))?.unbind())
            } else if (PG_DATE_MIN_FINITE..=PG_DATE_MAX_FINITE).contains(days) {
                Ok(types
                    .pg_temporal
                    .bind(py)
                    .call1(("date", *days, "out_of_range"))?
                    .unbind())
            } else {
                Err(crate::errors::postgres_error_to_py(
                    py,
                    PostgresError::Data(format!("invalid PostgreSQL DATE days {days}")),
                ))
            }
        }
        PgValue::Time(us) => {
            if *us == PG_TIME_MAX_MICROS {
                Ok(types
                    .pg_temporal
                    .bind(py)
                    .call1(("time", *us, "end_of_day"))?
                    .unbind())
            } else if (0..PG_TIME_MAX_MICROS).contains(us) {
                let total_secs = *us / 1_000_000;
                let micro = (*us % 1_000_000) as u32;
                let hour = (total_secs / 3600) as u8;
                let minute = ((total_secs % 3600) / 60) as u8;
                let second = (total_secs % 60) as u8;
                Ok(types
                    .time
                    .bind(py)
                    .call1((hour, minute, second, micro))?
                    .unbind())
            } else {
                Err(crate::errors::postgres_error_to_py(
                    py,
                    PostgresError::Data(format!("invalid PostgreSQL TIME microseconds {us}")),
                ))
            }
        }
        PgValue::Timestamp(us) => project_timestamp(py, *us, false, types),
        PgValue::TimestampTz(us) => project_timestamp(py, *us, true, types),
        PgValue::TemporalText {
            kind,
            wire_text,
            date_style,
            time_zone,
        } => {
            let kind_str = match kind {
                TemporalKind::Date => "date",
                TemporalKind::Time => "time",
                TemporalKind::Timestamp => "timestamp",
                TemporalKind::TimestampTz => "timestamptz",
            };
            Ok(types
                .pg_temporal
                .bind(py)
                .call1((
                    kind_str,
                    py.None(),
                    "wire_text",
                    wire_text.as_str(),
                    date_style.as_str(),
                    time_zone.as_str(),
                ))?
                .unbind())
        }
        PgValue::Interval {
            months,
            days,
            microseconds,
        } => Ok(types
            .pg_interval
            .bind(py)
            .call1((*months, *days, *microseconds))?
            .unbind()),
        PgValue::Json(raw_bytes) => {
            let decoded = types
                .msgspec_json_decode
                .bind(py)
                .call1((PyBytes::new(py, raw_bytes),))?;
            if decoded.is_none() {
                Ok(types.pg_json.bind(py).call1((py.None(),))?.unbind())
            } else {
                Ok(decoded.unbind())
            }
        }
        PgValue::BitString { data, bit_length } => Ok(types
            .pg_bit_string
            .bind(py)
            .call1((PyBytes::new(py, data), *bit_length))?
            .unbind()),
        PgValue::Vector { values, kind } => {
            let kind_str = match kind {
                VectorKind::Vector => "vector",
                VectorKind::HalfVec => "halfvec",
            };
            let floats: Vec<f64> = values
                .iter()
                .map(|&v| {
                    if *kind == VectorKind::HalfVec {
                        half::f16::from_f32(v).to_f64()
                    } else {
                        f64::from(v)
                    }
                })
                .collect();
            let tup = PyTuple::new(py, floats)?;
            Ok(types.pg_vector.bind(py).call1((tup, kind_str))?.unbind())
        }
        PgValue::Array {
            values,
            dimensions,
            lower_bounds,
            element_oid,
        } => {
            let mut py_vals = Vec::with_capacity(values.len());
            for elem in values {
                py_vals.push(pg_value_to_python_with_types(py, elem, types)?);
            }
            let vals_tup = PyTuple::new(py, py_vals)?;
            let dims_tup = PyTuple::new(py, dimensions.iter().copied())?;
            let lbs_tup = PyTuple::new(py, lower_bounds.iter().copied())?;
            Ok(types
                .pg_array
                .bind(py)
                .call1((vals_tup, dims_tup, lbs_tup, *element_oid))?
                .unbind())
        }
        PgValue::Composite {
            type_oid,
            field_names,
            field_oids,
            values,
        } => {
            let names_tup = PyTuple::new(py, field_names.iter().map(String::as_str))?;
            let oids_tup = PyTuple::new(py, field_oids.iter().copied())?;
            let mut py_vals = Vec::with_capacity(values.len());
            for v in values {
                py_vals.push(pg_value_to_python_with_types(py, v, types)?);
            }
            let vals_tup = PyTuple::new(py, py_vals)?;
            Ok(types
                .pg_composite
                .bind(py)
                .call1((*type_oid, names_tup, oids_tup, vals_tup))?
                .unbind())
        }
    }
}

fn project_timestamp(
    py: Python<'_>,
    us: i64,
    with_tz: bool,
    types: &CachedPythonTypes,
) -> PyResult<Py<PyAny>> {
    let kind_str = if with_tz { "timestamptz" } else { "timestamp" };
    if us == PG_TS_POS_INF {
        return Ok(types
            .pg_temporal
            .bind(py)
            .call1((kind_str, us, "positive_infinity"))?
            .unbind());
    }
    if us == PG_TS_NEG_INF {
        return Ok(types
            .pg_temporal
            .bind(py)
            .call1((kind_str, us, "negative_infinity"))?
            .unbind());
    }
    if (PY_TS_MIN_PG_MICROS..PY_TS_MAX_PG_MICROS).contains(&us) {
        let days = us.div_euclid(MICROS_PER_DAY) as i32;
        let rem_us = us.rem_euclid(MICROS_PER_DAY);
        let (y, m, d) = pg_days_to_ymd(days);
        let total_secs = rem_us / 1_000_000;
        let micro = (rem_us % 1_000_000) as u32;
        let hour = (total_secs / 3600) as u8;
        let minute = ((total_secs % 3600) / 60) as u8;
        let second = (total_secs % 60) as u8;
        if with_tz {
            return Ok(types
                .datetime
                .bind(py)
                .call1((
                    y,
                    m,
                    d,
                    hour,
                    minute,
                    second,
                    micro,
                    types.timezone_utc.bind(py),
                ))?
                .unbind());
        }
        return Ok(types
            .datetime
            .bind(py)
            .call1((y, m, d, hour, minute, second, micro))?
            .unbind());
    }
    if (PG_TS_MIN_FINITE..=PG_TS_MAX_FINITE).contains(&us) {
        return Ok(types
            .pg_temporal
            .bind(py)
            .call1((kind_str, us, "out_of_range"))?
            .unbind());
    }
    Err(crate::errors::postgres_error_to_py(
        py,
        PostgresError::Data(format!("invalid PostgreSQL {kind_str} microseconds {us}")),
    ))
}

/// Project an owned [`QueryResult`] into a frozen `vertebra.db.adapters.postgres.types.PostgresQueryResult`.
pub fn query_result_to_python(py: Python<'_>, result: &QueryResult) -> PyResult<Py<PyAny>> {
    let types = resolve_python_types(py).map_err(|e| crate::errors::postgres_error_to_py(py, e))?;

    let mut py_cols = Vec::with_capacity(result.columns.len());
    let col_cls = types.postgres_column.bind(py);
    for col in &result.columns {
        let py_col = col_cls.call1((
            col.name.as_str(),
            col.type_oid,
            col.format.code(),
            col.type_modifier,
            col.table_oid,
            col.attribute_number,
        ))?;
        py_cols.push(py_col);
    }
    let cols_tuple = PyTuple::new(py, py_cols)?;

    let mut py_rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let mut py_cells = Vec::with_capacity(row.len());
        for cell in row {
            py_cells.push(pg_value_to_python_with_types(py, cell, types)?);
        }
        py_rows.push(PyTuple::new(py, py_cells)?);
    }
    let rows_tuple = PyTuple::new(py, py_rows)?;

    let status_str = match result.transaction_status {
        TransactionStatus::Idle => "I",
        TransactionStatus::InTransaction => "T",
        TransactionStatus::FailedTransaction => "E",
    };

    let py_result = types.postgres_query_result.bind(py).call1((
        cols_tuple,
        rows_tuple,
        result.affected_rows,
        result.command_tag.as_str(),
        status_str,
    ))?;
    Ok(py_result.unbind())
}
