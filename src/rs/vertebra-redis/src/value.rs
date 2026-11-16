//! RESP2/RESP3 value conversion between `redis::Value` and Python objects.

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PySet, PyString};

use crate::errors::classify_redis_error;

/// Convert a Python object into a byte vector for Redis command arguments.
pub fn py_to_redis_arg(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = obj.cast::<PyBytes>() {
        Ok(bytes.as_bytes().to_vec())
    } else if let Ok(s) = obj.cast::<PyString>() {
        Ok(s.to_str()?.as_bytes().to_vec())
    } else if let Ok(b) = obj.cast::<PyBool>() {
        Ok(if b.is_true() {
            b"1".to_vec()
        } else {
            b"0".to_vec()
        })
    } else if let Ok(i) = obj.cast::<PyInt>() {
        let val: i64 = i.extract()?;
        Ok(val.to_string().into_bytes())
    } else if let Ok(f) = obj.cast::<PyFloat>() {
        let val: f64 = f.extract()?;
        Ok(val.to_string().into_bytes())
    } else if obj.is_none() {
        Ok(Vec::new())
    } else {
        let s = obj.str()?;
        Ok(s.to_str()?.as_bytes().to_vec())
    }
}

/// Convert a sequence of Python arguments into a list of byte arguments for Redis commands.
pub fn py_seq_to_redis_args(seq: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
    let mut args = Vec::new();
    for item in seq.try_iter()? {
        let item = item?;
        args.push(py_to_redis_arg(&item)?);
    }
    Ok(args)
}

/// Recursively convert a `redis::Value` into an owned Python object (`Py<PyAny>`).
pub fn redis_value_to_py(py: Python<'_>, val: redis::Value) -> PyResult<Py<PyAny>> {
    match val {
        redis::Value::Nil => Ok(py.None()),
        redis::Value::Int(i) => Ok(i.into_pyobject(py)?.unbind().into_any()),
        redis::Value::BulkString(bytes) => {
            Ok(PyBytes::new(py, &bytes).into_pyobject(py)?.unbind().into_any())
        }
        redis::Value::Array(items) => {
            let py_list = PyList::empty(py);
            for item in items {
                py_list.append(redis_value_to_py(py, item)?)?;
            }
            Ok(py_list.into_pyobject(py)?.unbind().into_any())
        }
        redis::Value::SimpleString(s) => Ok(s.into_pyobject(py)?.unbind().into_any()),
        redis::Value::Okay => Ok(PyString::new(py, "OK").into_pyobject(py)?.unbind().into_any()),
        redis::Value::Map(kvs) => {
            let dict = PyDict::new(py);
            for (k, v) in kvs {
                let py_k = redis_value_to_py(py, k)?;
                let py_v = redis_value_to_py(py, v)?;
                dict.set_item(py_k, py_v)?;
            }
            Ok(dict.into_pyobject(py)?.unbind().into_any())
        }
        redis::Value::Attribute { data, .. } => redis_value_to_py(py, *data),
        redis::Value::Set(items) => {
            let set = PySet::empty(py)?;
            for item in items {
                set.add(redis_value_to_py(py, item)?)?;
            }
            Ok(set.into_pyobject(py)?.unbind().into_any())
        }
        redis::Value::Double(d) => Ok(d.into_pyobject(py)?.unbind().into_any()),
        redis::Value::Boolean(b) => Ok(PyBool::new(py, b).to_owned().unbind().into_any()),
        redis::Value::VerbatimString { text, .. } => Ok(text.into_pyobject(py)?.unbind().into_any()),
        redis::Value::BigNumber(bytes) => {
            if let Ok(s) = std::str::from_utf8(&bytes) {
                if let Ok(int_val) = s.parse::<i128>() {
                    return Ok(int_val.into_pyobject(py)?.unbind().into_any());
                }
            }
            Ok(PyBytes::new(py, &bytes).into_pyobject(py)?.unbind().into_any())
        }
        redis::Value::Push { kind: _, data } => {
            let py_list = PyList::empty(py);
            for item in data {
                py_list.append(redis_value_to_py(py, item)?)?;
            }
            Ok(py_list.into_pyobject(py)?.unbind().into_any())
        }
        redis::Value::ServerError(err) => {
            let redis_err = redis::RedisError::from(err);
            Err(classify_redis_error(&redis_err).into_pyerr(py))
        }
        _ => Ok(py.None()),
    }
}
