//! Parameter extraction, `@P1` placeholder rewriting, and `tiberius::ToSql` binding for `vertebra-mssql`.

use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PySequence, PyString};
use rust_decimal::Decimal;
use tiberius::ToSql;
use tiberius::xml::XmlData;
use uuid::Uuid;
use vertebra_db::rewrite_mssql_placeholders;

use crate::errors::MssqlError;

/// Owned parameter value bound to a TDS `sp_executesql` RPC call (`@P1`, `@P2`, ...).
#[derive(Clone, Debug)]
pub enum MssqlParam {
    Null(Option<String>),
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Decimal(Decimal),
    Uuid(Uuid),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    DateTimeOffset(DateTime<FixedOffset>),
    Xml(XmlData),
}

impl MssqlParam {
    /// Construct a `NULL` parameter.
    pub const fn null() -> Self {
        Self::Null(None)
    }

    /// Borrow this parameter as a [`tiberius::ToSql`] trait object for `Client::query` / `Client::execute`.
    pub fn as_to_sql(&self) -> &dyn ToSql {
        match self {
            Self::Null(opt) => opt,
            Self::Bool(b) => b,
            Self::I64(n) => n,
            Self::F64(f) => f,
            Self::String(s) => s,
            Self::Bytes(b) => b,
            Self::Decimal(d) => d,
            Self::Uuid(u) => u,
            Self::Date(d) => d,
            Self::Time(t) => t,
            Self::DateTime(dt) => dt,
            Self::DateTimeOffset(dto) => dto,
            Self::Xml(x) => x,
        }
    }
}

/// Extract a single [`MssqlParam`] from an arbitrary Python object.
pub fn py_any_to_mssql_param(obj: &Bound<'_, PyAny>) -> Result<MssqlParam, MssqlError> {
    if obj.is_none() {
        return Ok(MssqlParam::null());
    }
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(MssqlParam::Bool(b.is_true()));
    }
    if let Ok(i) = obj.cast::<PyInt>() {
        let val = i.extract::<i64>().map_err(|_| {
            MssqlError::data("integer parameter exceeds 64-bit signed BIGINT range")
        })?;
        return Ok(MssqlParam::I64(val));
    }
    if let Ok(f) = obj.cast::<PyFloat>() {
        let val = f
            .extract::<f64>()
            .map_err(|e| MssqlError::data(e.to_string()))?;
        return Ok(MssqlParam::F64(val));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        let text = s.to_str().map_err(|e| MssqlError::data(e.to_string()))?;
        return Ok(MssqlParam::String(text.to_string()));
    }
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok(MssqlParam::Bytes(b.as_bytes().to_vec()));
    }
    if let Ok(ba) = obj.cast::<PyByteArray>() {
        return Ok(MssqlParam::Bytes(unsafe { ba.as_bytes() }.to_vec()));
    }

    let type_name = obj
        .get_type()
        .qualname()
        .map(|s| s.to_string())
        .unwrap_or_default();
    let module_name = obj
        .get_type()
        .module()
        .map(|s| s.to_string())
        .unwrap_or_default();

    if type_name == "Decimal" || module_name == "decimal" {
        let text = obj
            .str()
            .and_then(|s| Ok(s.to_str()?.to_string()))
            .map_err(|e| MssqlError::data(e.to_string()))?;
        let dec = Decimal::from_str(&text)
            .map_err(|e| MssqlError::data(format!("invalid Decimal parameter `{text}`: {e}")))?;
        return Ok(MssqlParam::Decimal(dec));
    }

    if type_name == "UUID" || module_name == "uuid" {
        let text = obj
            .str()
            .and_then(|s| Ok(s.to_str()?.to_string()))
            .map_err(|e| MssqlError::data(e.to_string()))?;
        let uuid = Uuid::parse_str(&text)
            .map_err(|e| MssqlError::data(format!("invalid UUID parameter `{text}`: {e}")))?;
        return Ok(MssqlParam::Uuid(uuid));
    }

    if type_name == "datetime"
        || (module_name == "datetime"
            && obj.hasattr("hour").unwrap_or(false)
            && obj.hasattr("year").unwrap_or(false))
    {
        return extract_py_datetime(obj);
    }

    if type_name == "date" || (module_name == "datetime" && obj.hasattr("year").unwrap_or(false)) {
        let year: i32 = obj
            .getattr("year")
            .and_then(|v| v.extract())
            .map_err(|e| MssqlError::data(e.to_string()))?;
        let month: u32 = obj
            .getattr("month")
            .and_then(|v| v.extract())
            .map_err(|e| MssqlError::data(e.to_string()))?;
        let day: u32 = obj
            .getattr("day")
            .and_then(|v| v.extract())
            .map_err(|e| MssqlError::data(e.to_string()))?;
        let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
            MssqlError::data(format!("invalid date {year:04}-{month:02}-{day:02}"))
        })?;
        return Ok(MssqlParam::Date(date));
    }

    if type_name == "time" || (module_name == "datetime" && obj.hasattr("hour").unwrap_or(false)) {
        let hour: u32 = obj
            .getattr("hour")
            .and_then(|v| v.extract())
            .map_err(|e| MssqlError::data(e.to_string()))?;
        let minute: u32 = obj
            .getattr("minute")
            .and_then(|v| v.extract())
            .map_err(|e| MssqlError::data(e.to_string()))?;
        let second: u32 = obj
            .getattr("second")
            .and_then(|v| v.extract())
            .map_err(|e| MssqlError::data(e.to_string()))?;
        let micro: u32 = obj
            .getattr("microsecond")
            .and_then(|v| v.extract())
            .map_err(|e| MssqlError::data(e.to_string()))?;
        let time = NaiveTime::from_hms_micro_opt(hour, minute, second, micro)
            .ok_or_else(|| MssqlError::data("invalid time value"))?;
        return Ok(MssqlParam::Time(time));
    }

    if obj.cast::<PyDict>().is_ok() || obj.cast::<PySequence>().is_ok() {
        let json_mod = obj
            .py()
            .import("json")
            .map_err(|e| MssqlError::data(e.to_string()))?;
        let serialized: String = json_mod
            .call_method1("dumps", (obj,))
            .and_then(|v| v.extract())
            .map_err(|e| MssqlError::data(format!("failed to serialize JSON parameter: {e}")))?;
        return Ok(MssqlParam::String(serialized));
    }

    let fallback = obj
        .str()
        .and_then(|s| Ok(s.to_str()?.to_string()))
        .map_err(|e| MssqlError::data(format!("unsupported MSSQL parameter type: {e}")))?;
    Ok(MssqlParam::String(fallback))
}

fn extract_py_datetime(obj: &Bound<'_, PyAny>) -> Result<MssqlParam, MssqlError> {
    let year: i32 = obj
        .getattr("year")
        .and_then(|v| v.extract())
        .map_err(|e| MssqlError::data(e.to_string()))?;
    let month: u32 = obj
        .getattr("month")
        .and_then(|v| v.extract())
        .map_err(|e| MssqlError::data(e.to_string()))?;
    let day: u32 = obj
        .getattr("day")
        .and_then(|v| v.extract())
        .map_err(|e| MssqlError::data(e.to_string()))?;
    let hour: u32 = obj
        .getattr("hour")
        .and_then(|v| v.extract())
        .map_err(|e| MssqlError::data(e.to_string()))?;
    let minute: u32 = obj
        .getattr("minute")
        .and_then(|v| v.extract())
        .map_err(|e| MssqlError::data(e.to_string()))?;
    let second: u32 = obj
        .getattr("second")
        .and_then(|v| v.extract())
        .map_err(|e| MssqlError::data(e.to_string()))?;
    let micro: u32 = obj
        .getattr("microsecond")
        .and_then(|v| v.extract())
        .map_err(|e| MssqlError::data(e.to_string()))?;

    let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
        MssqlError::data(format!(
            "invalid datetime date {year:04}-{month:02}-{day:02}"
        ))
    })?;
    let time = NaiveTime::from_hms_micro_opt(hour, minute, second, micro)
        .ok_or_else(|| MssqlError::data("invalid datetime time component"))?;
    let naive = NaiveDateTime::new(date, time);

    let tzinfo = obj.getattr("tzinfo").ok();
    if let Some(tz) = tzinfo
        && !tz.is_none()
        && let Ok(offset_obj) = obj.call_method0("utcoffset")
        && !offset_obj.is_none()
        && let Ok(total_seconds) = offset_obj
            .call_method0("total_seconds")
            .and_then(|v| v.extract::<f64>())
    {
        let secs = total_seconds.round() as i32;
        let offset = FixedOffset::east_opt(secs)
            .ok_or_else(|| MssqlError::data(format!("invalid UTC offset {secs}s")))?;
        let dto = DateTime::<FixedOffset>::from_naive_utc_and_offset(
            naive - chrono::Duration::seconds(i64::from(secs)),
            offset,
        );
        return Ok(MssqlParam::DateTimeOffset(dto));
    }

    let _ = naive.nanosecond();
    Ok(MssqlParam::DateTime(naive))
}

/// Rewrite SQL placeholders (`?`, `$1`, `:name`, `@name`) into TDS `@P1, @P2, ...` and align
/// Python positional or named parameters accordingly.
pub fn prepare_sql_and_params(
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
) -> Result<(String, Vec<MssqlParam>), MssqlError> {
    let Some(params_obj) = params else {
        let (rewritten, _) = rewrite_mssql_placeholders(sql);
        return Ok((rewritten, Vec::new()));
    };
    if params_obj.is_none() {
        let (rewritten, _) = rewrite_mssql_placeholders(sql);
        return Ok((rewritten, Vec::new()));
    }

    if let Ok(dict) = params_obj.cast::<PyDict>() {
        let mut named_map = HashMap::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key_str = k
                .extract::<String>()
                .map_err(|_| MssqlError::programming("named SQL parameter keys must be strings"))?;
            let clean_key = key_str
                .trim_start_matches('@')
                .trim_start_matches(':')
                .to_string();
            named_map.insert(clean_key, py_any_to_mssql_param(&v)?);
        }
        let (rewritten_named, ordered_params) = rewrite_named_mssql_placeholders(sql, &named_map)?;
        return Ok((rewritten_named, ordered_params));
    }

    if let Ok(seq) = params_obj.cast::<PySequence>() {
        let len = seq
            .len()
            .map_err(|e| MssqlError::programming(e.to_string()))?;
        let mut out = Vec::with_capacity(len);
        for idx in 0..len {
            let item = seq
                .get_item(idx)
                .map_err(|e| MssqlError::programming(e.to_string()))?;
            out.push(py_any_to_mssql_param(&item)?);
        }
        let (rewritten, _) = rewrite_mssql_placeholders(sql);
        return Ok((rewritten, out));
    }

    Err(MssqlError::programming(
        "MSSQL query parameters must be a sequence (list/tuple) or mapping (dict)",
    ))
}

/// Extract a batch of parameter sets for `execute_many`.
pub fn prepare_many_sql_and_params(
    sql: &str,
    param_sets: &Bound<'_, PyAny>,
) -> Result<Vec<(String, Vec<MssqlParam>)>, MssqlError> {
    let seq = param_sets.cast::<PySequence>().map_err(|_| {
        MssqlError::programming("execute_many parameters must be a sequence of parameter sets")
    })?;
    let len = seq
        .len()
        .map_err(|e| MssqlError::programming(e.to_string()))?;
    let mut batches = Vec::with_capacity(len);
    for idx in 0..len {
        let item = seq
            .get_item(idx)
            .map_err(|e| MssqlError::programming(e.to_string()))?;
        batches.push(prepare_sql_and_params(sql, Some(&item))?);
    }
    Ok(batches)
}

/// Rewrite `@name` or `:name` placeholders matching keys in `named_map` into `@P1, @P2, ...`.
pub fn rewrite_named_mssql_placeholders(
    sql: &str,
    named_map: &HashMap<String, MssqlParam>,
) -> Result<(String, Vec<MssqlParam>), MssqlError> {
    if named_map.is_empty() {
        let (rewritten, _) = rewrite_mssql_placeholders(sql);
        return Ok((rewritten, Vec::new()));
    }

    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len + 16);
    let mut ordered_params: Vec<MssqlParam> = Vec::new();
    let mut assigned_indices: HashMap<String, usize> = HashMap::new();
    let mut i = 0;

    while i < len {
        let b = bytes[i];
        if b == b'\'' {
            out.push('\'');
            i += 1;
            while i < len {
                let c = sql[i..].chars().next().unwrap();
                out.push(c);
                i += c.len_utf8();
                if c == '\'' {
                    if i < len && bytes[i] == b'\'' {
                        out.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if b == b'"' {
            out.push('"');
            i += 1;
            while i < len {
                let c = sql[i..].chars().next().unwrap();
                out.push(c);
                i += c.len_utf8();
                if c == '"' {
                    break;
                }
            }
            continue;
        }
        if b == b'[' {
            out.push('[');
            i += 1;
            while i < len {
                let c = sql[i..].chars().next().unwrap();
                out.push(c);
                i += c.len_utf8();
                if c == ']' {
                    break;
                }
            }
            continue;
        }
        if b == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            while i < len && bytes[i] != b'\n' {
                let c = sql[i..].chars().next().unwrap();
                out.push(c);
                i += c.len_utf8();
            }
            continue;
        }
        if (b == b'@' && (i + 1 >= len || bytes[i + 1] != b'@'))
            || (b == b':' && (i + 1 >= len || bytes[i + 1] != b':'))
        {
            let start = i + 1;
            let mut end = start;
            while end < len && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                end += 1;
            }
            if end > start {
                let ident = &sql[start..end];
                if let Some(param_val) = named_map.get(ident) {
                    let p_idx = if let Some(&existing) = assigned_indices.get(ident) {
                        existing
                    } else {
                        ordered_params.push(param_val.clone());
                        let next_idx = ordered_params.len();
                        assigned_indices.insert(ident.to_string(), next_idx);
                        next_idx
                    };
                    out.push_str(&format!("@P{p_idx}"));
                    i = end;
                    continue;
                }
            }
        }

        let c = sql[i..].chars().next().unwrap();
        out.push(c);
        i += c.len_utf8();
    }

    let (final_sql, _) = rewrite_mssql_placeholders(&out);
    let _ = Cow::Borrowed(sql);
    Ok((final_sql, ordered_params))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_placeholder_rewriting_reuses_parameter_index() {
        let mut map = HashMap::new();
        map.insert("tenant_id".to_string(), MssqlParam::I64(42));
        map.insert(
            "status".to_string(),
            MssqlParam::String("active".to_string()),
        );

        let (sql, params) = rewrite_named_mssql_placeholders(
            "SELECT * FROM orders WHERE tenant_id = :tenant_id AND status = @status AND parent_tenant = :tenant_id",
            &map,
        )
        .expect("rewrite named");
        assert_eq!(
            sql,
            "SELECT * FROM orders WHERE tenant_id = @P1 AND status = @P2 AND parent_tenant = @P1"
        );
        assert_eq!(params.len(), 2);
    }
}
