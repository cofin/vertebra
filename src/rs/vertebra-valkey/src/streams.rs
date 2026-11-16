//! Valkey Streams (`XGROUP`, `XADD`, `XREADGROUP`, `XACK`, `XAUTOCLAIM`, `XRANGE`, `XDEL`) and `ZADD`.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use crate::codec::RespValue;
use crate::error::ValkeyError;

/// Build `XGROUP CREATE <key> <group> <id> [MKSTREAM]`.
#[must_use]
pub fn build_xgroup_create(key: &[u8], group: &str, id: &str, mkstream: bool) -> Vec<Vec<u8>> {
    let mut cmd = vec![
        b"XGROUP".to_vec(),
        b"CREATE".to_vec(),
        key.to_vec(),
        group.as_bytes().to_vec(),
        id.as_bytes().to_vec(),
    ];
    if mkstream {
        cmd.push(b"MKSTREAM".to_vec());
    }
    cmd
}

/// Build `XADD <key> [NOMKSTREAM] [MAXLEN [~] <maxlen>] <id> <field> <value> ...`.
pub fn build_xadd(
    key: &[u8],
    fields: &[(Vec<u8>, Vec<u8>)],
    id: &str,
    maxlen: Option<u64>,
    approximate: bool,
    nomkstream: bool,
) -> Result<Vec<Vec<u8>>, ValkeyError> {
    if fields.is_empty() {
        return Err(ValkeyError::programming(
            "XADD requires at least one field/value pair",
        ));
    }
    let mut cmd = Vec::with_capacity(8 + fields.len() * 2);
    cmd.push(b"XADD".to_vec());
    cmd.push(key.to_vec());
    if nomkstream {
        cmd.push(b"NOMKSTREAM".to_vec());
    }
    if let Some(ml) = maxlen {
        cmd.push(b"MAXLEN".to_vec());
        if approximate {
            cmd.push(b"~".to_vec());
        }
        cmd.push(ml.to_string().into_bytes());
    }
    cmd.push(id.as_bytes().to_vec());
    for (f, v) in fields {
        cmd.push(f.clone());
        cmd.push(v.clone());
    }
    Ok(cmd)
}

/// Build `XREADGROUP GROUP <group> <consumer> [COUNT <n>] [BLOCK <ms>] [NOACK] STREAMS k... id...`.
pub fn build_xreadgroup(
    group: &str,
    consumer: &str,
    streams: &[(Vec<u8>, String)],
    count: Option<u64>,
    block_ms: Option<u64>,
    noack: bool,
) -> Result<Vec<Vec<u8>>, ValkeyError> {
    if streams.is_empty() {
        return Err(ValkeyError::programming(
            "XREADGROUP requires at least one stream",
        ));
    }
    let mut cmd = vec![
        b"XREADGROUP".to_vec(),
        b"GROUP".to_vec(),
        group.as_bytes().to_vec(),
        consumer.as_bytes().to_vec(),
    ];
    if let Some(c) = count {
        cmd.push(b"COUNT".to_vec());
        cmd.push(c.to_string().into_bytes());
    }
    if let Some(b) = block_ms {
        cmd.push(b"BLOCK".to_vec());
        cmd.push(b.to_string().into_bytes());
    }
    if noack {
        cmd.push(b"NOACK".to_vec());
    }
    cmd.push(b"STREAMS".to_vec());
    for (k, _) in streams {
        cmd.push(k.clone());
    }
    for (_, id) in streams {
        cmd.push(id.as_bytes().to_vec());
    }
    Ok(cmd)
}

/// Build `XACK <key> <group> <id> ...`.
pub fn build_xack(key: &[u8], group: &str, ids: &[String]) -> Result<Vec<Vec<u8>>, ValkeyError> {
    if ids.is_empty() {
        return Err(ValkeyError::programming(
            "XACK requires at least one entry id",
        ));
    }
    let mut cmd = Vec::with_capacity(3 + ids.len());
    cmd.push(b"XACK".to_vec());
    cmd.push(key.to_vec());
    cmd.push(group.as_bytes().to_vec());
    for id in ids {
        cmd.push(id.as_bytes().to_vec());
    }
    Ok(cmd)
}

/// Build `XAUTOCLAIM <key> <group> <consumer> <min-idle-ms> <start> [COUNT <n>] [JUSTID]`.
#[must_use]
pub fn build_xautoclaim(
    key: &[u8],
    group: &str,
    consumer: &str,
    min_idle_ms: u64,
    start_id: &str,
    count: Option<u64>,
    justid: bool,
) -> Vec<Vec<u8>> {
    let mut cmd = vec![
        b"XAUTOCLAIM".to_vec(),
        key.to_vec(),
        group.as_bytes().to_vec(),
        consumer.as_bytes().to_vec(),
        min_idle_ms.to_string().into_bytes(),
        start_id.as_bytes().to_vec(),
    ];
    if let Some(c) = count {
        cmd.push(b"COUNT".to_vec());
        cmd.push(c.to_string().into_bytes());
    }
    if justid {
        cmd.push(b"JUSTID".to_vec());
    }
    cmd
}

/// Build `XRANGE <key> <min> <max> [COUNT <n>]`.
#[must_use]
pub fn build_xrange(key: &[u8], min: &str, max: &str, count: Option<u64>) -> Vec<Vec<u8>> {
    let mut cmd = vec![
        b"XRANGE".to_vec(),
        key.to_vec(),
        min.as_bytes().to_vec(),
        max.as_bytes().to_vec(),
    ];
    if let Some(c) = count {
        cmd.push(b"COUNT".to_vec());
        cmd.push(c.to_string().into_bytes());
    }
    cmd
}

/// Build `XDEL <key> <id> ...`.
pub fn build_xdel(key: &[u8], ids: &[String]) -> Result<Vec<Vec<u8>>, ValkeyError> {
    if ids.is_empty() {
        return Err(ValkeyError::programming(
            "XDEL requires at least one entry id",
        ));
    }
    let mut cmd = Vec::with_capacity(2 + ids.len());
    cmd.push(b"XDEL".to_vec());
    cmd.push(key.to_vec());
    for id in ids {
        cmd.push(id.as_bytes().to_vec());
    }
    Ok(cmd)
}

/// Build `ZADD <key> [NX|XX] [GT|LT] [CH] <score> <member> ...`.
pub fn build_zadd(
    key: &[u8],
    members: &[(Vec<u8>, f64)],
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
) -> Result<Vec<Vec<u8>>, ValkeyError> {
    if nx && xx {
        return Err(ValkeyError::programming(
            "ZADD NX and XX options are mutually exclusive",
        ));
    }
    if gt && lt {
        return Err(ValkeyError::programming(
            "ZADD GT and LT options are mutually exclusive",
        ));
    }
    let mut cmd = Vec::with_capacity(6 + members.len() * 2);
    cmd.push(b"ZADD".to_vec());
    cmd.push(key.to_vec());
    if nx {
        cmd.push(b"NX".to_vec());
    }
    if xx {
        cmd.push(b"XX".to_vec());
    }
    if gt {
        cmd.push(b"GT".to_vec());
    }
    if lt {
        cmd.push(b"LT".to_vec());
    }
    if ch {
        cmd.push(b"CH".to_vec());
    }
    for (member, score) in members {
        if !score.is_finite() {
            return Err(ValkeyError::data("ZADD score must be finite"));
        }
        cmd.push(score.to_string().into_bytes());
        cmd.push(member.clone());
    }
    Ok(cmd)
}

/// Decode `XREADGROUP` response into Python `list[tuple[str, list[tuple[str, dict[str, str | bytes]]]]]`.
pub fn decode_xreadgroup_py(py: Python<'_>, resp: RespValue) -> PyResult<Py<PyAny>> {
    let val = resp.into_result().map_err(|e| e.to_py_err(py))?;
    let out = PyList::empty(py);
    match val {
        RespValue::Null => Ok(out.into_any().unbind()),
        RespValue::Map(streams) => {
            for (k, v) in streams {
                let s_name = k
                    .as_str()
                    .map(std::borrow::Cow::into_owned)
                    .unwrap_or_default();
                let entries = decode_stream_entries_py(py, &v)?;
                out.append(PyTuple::new(
                    py,
                    [
                        s_name.into_pyobject(py)?.into_any().unbind(),
                        entries.into_any().unbind(),
                    ],
                )?)?;
            }
            Ok(out.into_any().unbind())
        }
        RespValue::Array(streams) => {
            for item in streams {
                if let Some(pair) = item.as_slice()
                    && pair.len() >= 2
                {
                    let s_name = pair[0]
                        .as_str()
                        .map(std::borrow::Cow::into_owned)
                        .unwrap_or_default();
                    let entries = decode_stream_entries_py(py, &pair[1])?;
                    out.append(PyTuple::new(
                        py,
                        [
                            s_name.into_pyobject(py)?.into_any().unbind(),
                            entries.into_any().unbind(),
                        ],
                    )?)?;
                }
            }
            Ok(out.into_any().unbind())
        }
        other => Err(
            ValkeyError::data(format!("Unexpected XREADGROUP response shape: {other:?}"))
                .to_py_err(py),
        ),
    }
}

/// Decode `XRANGE` response into `list[tuple[str, dict[str, str | bytes]]]`.
pub fn decode_xrange_py(py: Python<'_>, resp: RespValue) -> PyResult<Py<PyAny>> {
    let val = resp.into_result().map_err(|e| e.to_py_err(py))?;
    Ok(decode_stream_entries_py(py, &val)?.into_any().unbind())
}

/// Decode `XAUTOCLAIM` response into `(next_start_id, entries, deleted_ids)`.
pub fn decode_xautoclaim_py(py: Python<'_>, resp: RespValue) -> PyResult<Py<PyAny>> {
    let val = resp.into_result().map_err(|e| e.to_py_err(py))?;
    let Some(parts) = val.as_slice() else {
        return Err(ValkeyError::data("Unexpected non-array XAUTOCLAIM response").to_py_err(py));
    };
    if parts.len() < 2 {
        return Err(ValkeyError::data("XAUTOCLAIM response truncated").to_py_err(py));
    }
    let next_id = parts[0]
        .as_str()
        .map(std::borrow::Cow::into_owned)
        .unwrap_or_else(|| "0-0".to_string());
    let entries = decode_stream_entries_py(py, &parts[1])?;
    let deleted = PyList::empty(py);
    if let Some(del_items) = parts.get(2).and_then(RespValue::as_slice) {
        for item in del_items {
            if let Some(id) = item.as_str() {
                deleted.append(id.as_ref())?;
            }
        }
    }
    Ok(PyTuple::new(
        py,
        [
            next_id.into_pyobject(py)?.into_any().unbind(),
            entries.into_any().unbind(),
            deleted.into_any().unbind(),
        ],
    )?
    .into_any()
    .unbind())
}

fn decode_stream_entries_py<'py>(py: Python<'py>, val: &RespValue) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    let Some(entries) = val.as_slice() else {
        return Ok(list);
    };
    for entry in entries {
        let Some(parts) = entry.as_slice() else {
            continue;
        };
        if parts.is_empty() {
            continue;
        }
        let id = parts[0]
            .as_str()
            .map(std::borrow::Cow::into_owned)
            .unwrap_or_default();
        let fields = PyDict::new(py);
        if let Some(fv_val) = parts.get(1) {
            match fv_val.unattributed() {
                RespValue::Array(kv) => {
                    for [k_val, v_val] in kv.as_chunks::<2>().0 {
                        if let Some(k) = k_val.as_str() {
                            let py_v = stream_field_value_to_py(py, v_val)?;
                            fields.set_item(k.as_ref(), py_v)?;
                        }
                    }
                }
                RespValue::Map(pairs) => {
                    for (k_val, v_val) in pairs {
                        if let Some(k) = k_val.as_str() {
                            let py_v = stream_field_value_to_py(py, v_val)?;
                            fields.set_item(k.as_ref(), py_v)?;
                        }
                    }
                }
                _ => {}
            }
        }
        list.append(PyTuple::new(
            py,
            [
                id.into_pyobject(py)?.into_any().unbind(),
                fields.into_any().unbind(),
            ],
        )?)?;
    }
    Ok(list)
}

fn stream_field_value_to_py(py: Python<'_>, val: &RespValue) -> PyResult<Py<PyAny>> {
    match val.as_bytes() {
        Some(raw) => match std::str::from_utf8(raw) {
            Ok(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
            Err(_) => Ok(PyBytes::new(py, raw).into_any().unbind()),
        },
        None => Ok(py.None()),
    }
}
