//! PyO3 bindings for `vertebra-core::text` and `vertebra-core::id`.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use vertebra_core::{id, text};

#[pyfunction]
pub fn snake_case(value: &str) -> String {
    text::snake_case(value)
}

#[pyfunction]
pub fn camelize(value: &str) -> String {
    text::camelize(value)
}

#[pyfunction]
pub fn pascalize(value: &str) -> String {
    text::pascalize(value)
}

#[pyfunction]
pub fn kebabize(value: &str) -> String {
    text::kebabize(value)
}

#[pyfunction]
#[pyo3(signature = (value, separator = "-"))]
pub fn slugify(value: &str, separator: &str) -> String {
    text::slugify(value, separator)
}

#[pyfunction]
pub fn quote_identifier(identifier: &str) -> String {
    text::quote_identifier(identifier)
}

#[pyfunction]
pub fn quote_backtick_identifier(identifier: &str) -> String {
    text::quote_backtick_identifier(identifier)
}

#[pyfunction]
pub fn quote_bracket_identifier(identifier: &str) -> String {
    text::quote_bracket_identifier(identifier)
}

#[pyfunction]
#[pyo3(signature = (identifier, dialect = "ansi"))]
pub fn quote_dialect_identifier(identifier: &str, dialect: &str) -> String {
    text::quote_dialect_identifier(identifier, dialect)
}

#[pyfunction]
#[pyo3(signature = (identifier, *, quote_chars = "\"`", allow_bracket_quotes = true))]
pub fn split_qualified_identifier(
    identifier: &str,
    quote_chars: &str,
    allow_bracket_quotes: bool,
) -> Vec<String> {
    text::split_qualified_identifier(identifier, quote_chars, allow_bracket_quotes)
}

#[pyfunction]
#[pyo3(signature = (identifier, dialect = None))]
pub fn normalize_identifier(identifier: &str, dialect: Option<&str>) -> PyResult<String> {
    text::normalize_identifier(identifier, dialect).map_err(PyValueError::new_err)
}

#[pyfunction]
#[pyo3(signature = (identifier, field_name = "identifier", *, allow_dots = true, allow_hyphens = false))]
pub fn validate_identifier(
    identifier: &str,
    field_name: &str,
    allow_dots: bool,
    allow_hyphens: bool,
) -> PyResult<String> {
    text::validate_identifier(identifier, field_name, allow_dots, allow_hyphens)
        .map_err(PyValueError::new_err)
}

#[pyfunction]
pub fn truncate_utf8_bytes(value: &str, max_bytes: usize) -> String {
    text::truncate_utf8_bytes(value, max_bytes).to_string()
}

#[pyfunction]
#[pyo3(signature = (value, max_chars, *, suffix = "..."))]
pub fn truncate(value: &str, max_chars: usize, suffix: &str) -> String {
    text::truncate(value, max_chars, suffix)
}

#[pyfunction]
#[pyo3(signature = (value, *, capitalize = true))]
pub fn humanize(value: &str, capitalize: bool) -> String {
    text::humanize(value, capitalize)
}

#[pyfunction]
pub fn titleize(value: &str) -> String {
    text::titleize(value)
}

#[pyfunction]
#[pyo3(signature = (word, count = None, *, include_count = false))]
pub fn pluralize(word: &str, count: Option<i64>, include_count: bool) -> String {
    text::pluralize(word, count, include_count)
}

#[pyfunction]
pub fn singularize(word: &str) -> String {
    text::singularize(word)
}

#[pyfunction]
pub fn ordinalize(value: i64) -> String {
    text::ordinalize(value)
}

#[pyfunction]
#[pyo3(signature = (num_bytes, *, binary = true, precision = 1))]
pub fn humanize_bytes(num_bytes: u64, binary: bool, precision: usize) -> String {
    text::humanize_bytes(num_bytes, binary, precision)
}

#[pyfunction]
pub fn parse_bytes(value: &str) -> PyResult<u64> {
    text::parse_bytes(value).map_err(PyValueError::new_err)
}

#[pyfunction]
#[pyo3(signature = (seconds, *, precision = 1))]
pub fn humanize_duration(seconds: f64, precision: usize) -> String {
    text::humanize_duration(seconds, precision)
}

#[pyfunction]
pub fn parse_duration(value: &str) -> PyResult<f64> {
    text::parse_duration(value).map_err(PyValueError::new_err)
}

#[pyfunction]
#[pyo3(signature = (value, *, precision = 1))]
pub fn humanize_number(value: f64, precision: usize) -> String {
    text::humanize_number(value, precision)
}

#[pyfunction]
#[pyo3(signature = (count, seconds = 1.0, *, unit = "req", precision = 1))]
pub fn humanize_rate(count: f64, seconds: f64, unit: &str, precision: usize) -> String {
    text::humanize_rate(count, seconds, unit, precision)
}

#[pyfunction]
pub fn uuid4_u128() -> u128 {
    id::uuid4_u128()
}

#[pyfunction]
pub fn uuid4_str() -> String {
    id::uuid4_str()
}

#[pyfunction]
pub fn uuid4_hex() -> String {
    id::uuid4_hex()
}

#[pyfunction]
pub fn uuid7_u128() -> u128 {
    id::uuid7_u128()
}

#[pyfunction]
pub fn uuid7_str() -> String {
    id::uuid7_str()
}

#[pyfunction]
#[pyo3(signature = (size = 21))]
pub fn nanoid_generate(size: usize) -> PyResult<String> {
    id::nanoid(size).map_err(PyValueError::new_err)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(snake_case, m)?)?;
    m.add_function(wrap_pyfunction!(camelize, m)?)?;
    m.add_function(wrap_pyfunction!(pascalize, m)?)?;
    m.add_function(wrap_pyfunction!(kebabize, m)?)?;
    m.add_function(wrap_pyfunction!(slugify, m)?)?;
    m.add_function(wrap_pyfunction!(quote_identifier, m)?)?;
    m.add_function(wrap_pyfunction!(quote_backtick_identifier, m)?)?;
    m.add_function(wrap_pyfunction!(quote_bracket_identifier, m)?)?;
    m.add_function(wrap_pyfunction!(quote_dialect_identifier, m)?)?;
    m.add_function(wrap_pyfunction!(split_qualified_identifier, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_identifier, m)?)?;
    m.add_function(wrap_pyfunction!(validate_identifier, m)?)?;
    m.add_function(wrap_pyfunction!(truncate_utf8_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(truncate, m)?)?;
    m.add_function(wrap_pyfunction!(humanize, m)?)?;
    m.add_function(wrap_pyfunction!(titleize, m)?)?;
    m.add_function(wrap_pyfunction!(pluralize, m)?)?;
    m.add_function(wrap_pyfunction!(singularize, m)?)?;
    m.add_function(wrap_pyfunction!(ordinalize, m)?)?;
    m.add_function(wrap_pyfunction!(humanize_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(parse_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(humanize_duration, m)?)?;
    m.add_function(wrap_pyfunction!(parse_duration, m)?)?;
    m.add_function(wrap_pyfunction!(humanize_number, m)?)?;
    m.add_function(wrap_pyfunction!(humanize_rate, m)?)?;
    m.add_function(wrap_pyfunction!(uuid4_u128, m)?)?;
    m.add_function(wrap_pyfunction!(uuid4_str, m)?)?;
    m.add_function(wrap_pyfunction!(uuid4_hex, m)?)?;
    m.add_function(wrap_pyfunction!(uuid7_u128, m)?)?;
    m.add_function(wrap_pyfunction!(uuid7_str, m)?)?;
    m.add_function(wrap_pyfunction!(nanoid_generate, m)?)?;
    Ok(())
}
