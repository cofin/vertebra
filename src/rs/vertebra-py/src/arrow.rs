use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyAnyMethods, PyBool, PyBytes, PySequence, PySequenceMethods};

#[derive(Clone, Copy)]
enum ColumnType {
    Bool,
    Int64,
    Float64,
    String,
}

fn infer_column_type(value: &Bound<'_, PyAny>) -> ColumnType {
    if value.is_instance_of::<PyBool>() {
        return ColumnType::Bool;
    }
    if value.extract::<i64>().is_ok() {
        return ColumnType::Int64;
    }
    if value.extract::<f64>().is_ok() {
        return ColumnType::Float64;
    }
    ColumnType::String
}

fn empty_arrow_stream(py: Python<'_>) -> PyResult<Py<PyBytes>> {
    let schema = Arc::new(Schema::new(Vec::<Field>::new()));
    let buffer = py
        .detach(|| -> Result<Vec<u8>, arrow::error::ArrowError> {
            let mut out = Vec::new();
            let mut writer = StreamWriter::try_new(&mut out, schema.as_ref())?;
            writer.finish()?;
            Ok(out)
        })
        .map_err(crate::map_err)?;
    Ok(PyBytes::new(py, &buffer).into())
}

fn row_sequence<'py>(
    row: &'py Bound<'py, PyAny>,
    row_index: usize,
) -> PyResult<&'py Bound<'py, PySequence>> {
    row.cast::<PySequence>()
        .map_err(|_| PyErr::new::<PyValueError, _>(format!("row {row_index} is not a sequence")))
}

#[pyfunction]
pub fn encode_arrow_rows(
    py: Python<'_>,
    rows: Bound<'_, PyAny>,
    columns: Vec<String>,
) -> PyResult<Py<PyBytes>> {
    let rows_seq = rows
        .cast::<PySequence>()
        .map_err(|_| PyErr::new::<PyValueError, _>("rows must be a sequence"))?;
    let row_count = rows_seq.len()?;
    let col_count = columns.len();

    if row_count == 0 || col_count == 0 {
        return empty_arrow_stream(py);
    }

    for row_index in 0..row_count {
        let row = rows_seq.get_item(row_index)?;
        let row_seq = row_sequence(&row, row_index)?;
        let row_len = row_seq.len()?;
        if row_len != col_count {
            return Err(PyErr::new::<PyValueError, _>(format!(
                "row {row_index} has {row_len} values, expected {col_count}"
            )));
        }
    }

    let mut column_types = Vec::with_capacity(col_count);
    for col_index in 0..col_count {
        let mut inferred = None;
        for row_index in 0..row_count {
            let row = rows_seq.get_item(row_index)?;
            let row_seq = row_sequence(&row, row_index)?;
            let value = row_seq.get_item(col_index)?;
            if value.is_none() {
                continue;
            }
            inferred = Some(infer_column_type(&value));
            break;
        }
        column_types.push(inferred.unwrap_or(ColumnType::String));
    }

    let mut fields = Vec::with_capacity(col_count);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_count);

    for (col_index, column) in columns.iter().enumerate() {
        match column_types[col_index] {
            ColumnType::Bool => {
                let mut builder = BooleanBuilder::new();
                for row_index in 0..row_count {
                    let row = rows_seq.get_item(row_index)?;
                    let row_seq = row_sequence(&row, row_index)?;
                    let value = row_seq.get_item(col_index)?;
                    if value.is_none() {
                        builder.append_null();
                    } else {
                        builder.append_value(value.extract::<bool>()?);
                    }
                }
                fields.push(Field::new(column, DataType::Boolean, true));
                arrays.push(Arc::new(builder.finish()));
            }
            ColumnType::Int64 => {
                let mut builder = Int64Builder::new();
                for row_index in 0..row_count {
                    let row = rows_seq.get_item(row_index)?;
                    let row_seq = row_sequence(&row, row_index)?;
                    let value = row_seq.get_item(col_index)?;
                    if value.is_none() {
                        builder.append_null();
                    } else {
                        builder.append_value(value.extract::<i64>()?);
                    }
                }
                fields.push(Field::new(column, DataType::Int64, true));
                arrays.push(Arc::new(builder.finish()));
            }
            ColumnType::Float64 => {
                let mut builder = Float64Builder::new();
                for row_index in 0..row_count {
                    let row = rows_seq.get_item(row_index)?;
                    let row_seq = row_sequence(&row, row_index)?;
                    let value = row_seq.get_item(col_index)?;
                    if value.is_none() {
                        builder.append_null();
                    } else {
                        builder.append_value(value.extract::<f64>()?);
                    }
                }
                fields.push(Field::new(column, DataType::Float64, true));
                arrays.push(Arc::new(builder.finish()));
            }
            ColumnType::String => {
                let mut builder = StringBuilder::new();
                for row_index in 0..row_count {
                    let row = rows_seq.get_item(row_index)?;
                    let row_seq = row_sequence(&row, row_index)?;
                    let value = row_seq.get_item(col_index)?;
                    if value.is_none() {
                        builder.append_null();
                    } else if let Ok(text) = value.extract::<String>() {
                        builder.append_value(text);
                    } else {
                        let text = value.str()?.to_str()?.to_string();
                        builder.append_value(text);
                    }
                }
                fields.push(Field::new(column, DataType::Utf8, true));
                arrays.push(Arc::new(builder.finish()));
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(crate::map_err)?;

    let buffer = py
        .detach(|| -> Result<Vec<u8>, arrow::error::ArrowError> {
            let mut out = Vec::new();
            let mut writer = StreamWriter::try_new(&mut out, schema.as_ref())?;
            writer.write(&batch)?;
            writer.finish()?;
            Ok(out)
        })
        .map_err(crate::map_err)?;

    Ok(PyBytes::new(py, &buffer).into())
}
