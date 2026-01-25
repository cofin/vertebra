use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyAnyMethods, PyBool, PyBytes, PyFloat, PyInt, PySequence, PySequenceMethods,
    PyString,
};

const DEFAULT_BATCH_SIZE: usize = 8192;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnType {
    Bool,
    Int64,
    Float64,
    String,
}

enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    String(StringBuilder),
}

impl ColumnBuilder {
    fn with_capacity(kind: ColumnType, capacity: usize) -> Self {
        match kind {
            ColumnType::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            ColumnType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            ColumnType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            ColumnType::String => Self::String(StringBuilder::with_capacity(capacity, capacity * 8)),
        }
    }

    fn append(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            match self {
                ColumnBuilder::Bool(builder) => builder.append_null(),
                ColumnBuilder::Int64(builder) => builder.append_null(),
                ColumnBuilder::Float64(builder) => builder.append_null(),
                ColumnBuilder::String(builder) => builder.append_null(),
            }
            return Ok(());
        }

        match self {
            ColumnBuilder::Bool(builder) => {
                if !value.is_instance_of::<PyBool>() {
                    return Err(PyErr::new::<PyValueError, _>("expected bool value"));
                }
                builder.append_value(value.extract::<bool>()?);
            }
            ColumnBuilder::Int64(builder) => {
                builder.append_value(value.extract::<i64>()?);
            }
            ColumnBuilder::Float64(builder) => {
                builder.append_value(value.extract::<f64>()?);
            }
            ColumnBuilder::String(builder) => {
                if let Ok(py_str) = value.cast::<PyString>() {
                    builder.append_value(py_str.to_str()?);
                } else {
                    let py_str = value.str()?;
                    let text = py_str.to_str()?;
                    builder.append_value(text);
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColumnBuilder::Bool(mut builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int64(mut builder) => Arc::new(builder.finish()),
            ColumnBuilder::Float64(mut builder) => Arc::new(builder.finish()),
            ColumnBuilder::String(mut builder) => Arc::new(builder.finish()),
        }
    }
}

fn data_type_for(kind: ColumnType) -> DataType {
    match kind {
        ColumnType::Bool => DataType::Boolean,
        ColumnType::Int64 => DataType::Int64,
        ColumnType::Float64 => DataType::Float64,
        ColumnType::String => DataType::Utf8,
    }
}

fn parse_column_type(raw: &str) -> Option<ColumnType> {
    match raw.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Some(ColumnType::Bool),
        "int" | "int64" | "integer" => Some(ColumnType::Int64),
        "float" | "float64" | "double" => Some(ColumnType::Float64),
        "str" | "string" | "utf8" => Some(ColumnType::String),
        _ => None,
    }
}

fn infer_value_type(value: &Bound<'_, PyAny>) -> ColumnType {
    if value.is_instance_of::<PyBool>() {
        return ColumnType::Bool;
    }
    if value.is_instance_of::<PyFloat>() {
        return ColumnType::Float64;
    }
    if value.is_instance_of::<PyInt>() {
        return ColumnType::Int64;
    }
    if value.extract::<f64>().is_ok() {
        return ColumnType::Float64;
    }
    if value.extract::<i64>().is_ok() {
        return ColumnType::Int64;
    }
    ColumnType::String
}

fn merge_types(current: ColumnType, incoming: ColumnType) -> ColumnType {
    use ColumnType::{Bool, Float64, Int64, String};
    match (current, incoming) {
        (String, _) | (_, String) => String,
        (Bool, Bool) => Bool,
        (Bool, Int64) | (Int64, Bool) => Int64,
        (Bool, Float64) | (Float64, Bool) => Float64,
        (Int64, Int64) => Int64,
        (Float64, Float64) => Float64,
        (Int64, Float64) | (Float64, Int64) => Float64,
    }
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
#[pyo3(signature = (rows, columns, types=None, batch_size=None))]
pub fn encode_arrow_rows(
    py: Python<'_>,
    rows: Bound<'_, PyAny>,
    columns: Vec<String>,
    types: Option<Vec<String>>,
    batch_size: Option<usize>,
) -> PyResult<Py<PyBytes>> {
    let has_types = types.is_some();
    let rows_seq = rows
        .cast::<PySequence>()
        .map_err(|_| PyErr::new::<PyValueError, _>("rows must be a sequence"))?;
    let row_count = rows_seq.len()?;
    let col_count = columns.len();

    if row_count == 0 || col_count == 0 {
        return empty_arrow_stream(py);
    }

    let mut column_types: Vec<Option<ColumnType>> = if let Some(types) = types {
        if types.len() != col_count {
            return Err(PyErr::new::<PyValueError, _>(
                "types length must match columns length",
            ));
        }
        let mut parsed = Vec::with_capacity(col_count);
        for (idx, raw) in types.iter().enumerate() {
            match parse_column_type(raw) {
                Some(kind) => parsed.push(Some(kind)),
                None => {
                    return Err(PyErr::new::<PyValueError, _>(format!(
                        "unsupported column type at index {idx}: {raw}"
                    )));
                }
            }
        }
        parsed
    } else {
        vec![None; col_count]
    };

    if !has_types {
        for row_index in 0..row_count {
            let row = rows_seq.get_item(row_index)?;
            let row_seq = row_sequence(&row, row_index)?;
            let row_len = row_seq.len()?;
            if row_len != col_count {
                return Err(PyErr::new::<PyValueError, _>(format!(
                    "row {row_index} has {row_len} values, expected {col_count}"
                )));
            }

            for col_index in 0..col_count {
                let value = row_seq.get_item(col_index)?;
                if value.is_none() {
                    continue;
                }
                let incoming = infer_value_type(&value);
                column_types[col_index] = Some(match column_types[col_index] {
                    Some(existing) => merge_types(existing, incoming),
                    None => incoming,
                });
            }
        }
    }

    let final_types: Vec<ColumnType> = column_types
        .into_iter()
        .map(|kind| kind.unwrap_or(ColumnType::String))
        .collect();

    let fields: Vec<Field> = columns
        .iter()
        .zip(final_types.iter())
        .map(|(column, kind)| Field::new(column, data_type_for(*kind), true))
        .collect();

    let schema = Arc::new(Schema::new(fields));
    let mut out = Vec::new();
    let mut writer = StreamWriter::try_new(&mut out, schema.as_ref()).map_err(crate::map_err)?;

    let mut start = 0;
    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
    while start < row_count {
        let end = (start + batch_size).min(row_count);
        let batch_len = end - start;

        let mut builders: Vec<ColumnBuilder> = final_types
            .iter()
            .map(|kind| ColumnBuilder::with_capacity(*kind, batch_len))
            .collect();

        for row_index in start..end {
            let row = rows_seq.get_item(row_index)?;
            let row_seq = row_sequence(&row, row_index)?;
            let row_len = row_seq.len()?;
            if row_len != col_count {
                return Err(PyErr::new::<PyValueError, _>(format!(
                    "row {row_index} has {row_len} values, expected {col_count}"
                )));
            }

            for col_index in 0..col_count {
                let value = row_seq.get_item(col_index)?;
                builders[col_index].append(&value)?;
            }
        }

        let arrays: Vec<ArrayRef> = builders.into_iter().map(|builder| builder.finish()).collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(crate::map_err)?;
        writer.write(&batch).map_err(crate::map_err)?;

        start = end;
    }

    writer.finish().map_err(crate::map_err)?;

    Ok(PyBytes::new(py, &out).into())
}
