//! Zero-copy C-API `msgspec`, `pydantic-core`, gRPC LPM, and `PyObject_Vectorcall` bridge.
//!
//! Eliminates intermediate `PyBytes` and `PyDict` allocations when decoding
//! HTTP or gRPC 5-byte Length-Prefixed Message (LPM) request bodies or hydrating
//! database/ORM rows into `msgspec.Struct`, `pydantic.BaseModel`, `@dataclass`,
//! `attrs`, and Protobuf message types.

use std::collections::HashMap;
use std::ptr;
use std::sync::{OnceLock, RwLock};

use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

/// Length of the gRPC 5-byte Length-Prefixed Message (LPM) header.
pub const GRPC_LPM_HEADER_LEN: usize = 5;

static MSGSPEC_JSON_DECODE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static MSGSPEC_MSGPACK_DECODE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static MSGSPEC_JSON_DECODER: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static MSGSPEC_MSGPACK_DECODER: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

type DecoderCacheMap = HashMap<(usize, bool), Py<PyAny>>;
static MSGSPEC_DECODER_CACHE: OnceLock<RwLock<DecoderCacheMap>> = OnceLock::new();
static PYDANTIC_VALIDATOR_CACHE: OnceLock<RwLock<HashMap<usize, Py<PyAny>>>> = OnceLock::new();
static PROTOBUF_DECODER_CACHE: OnceLock<RwLock<HashMap<usize, Py<PyAny>>>> = OnceLock::new();
static STRUCT_FIELDS_CACHE: OnceLock<RwLock<HashMap<usize, Py<PyTuple>>>> = OnceLock::new();

#[inline]
fn decoder_cache() -> &'static RwLock<DecoderCacheMap> {
    MSGSPEC_DECODER_CACHE.get_or_init(|| RwLock::new(HashMap::with_capacity(64)))
}

#[inline]
fn pydantic_cache() -> &'static RwLock<HashMap<usize, Py<PyAny>>> {
    PYDANTIC_VALIDATOR_CACHE.get_or_init(|| RwLock::new(HashMap::with_capacity(64)))
}

#[inline]
fn protobuf_cache() -> &'static RwLock<HashMap<usize, Py<PyAny>>> {
    PROTOBUF_DECODER_CACHE.get_or_init(|| RwLock::new(HashMap::with_capacity(64)))
}

#[inline]
fn struct_fields_cache() -> &'static RwLock<HashMap<usize, Py<PyTuple>>> {
    STRUCT_FIELDS_CACHE.get_or_init(|| RwLock::new(HashMap::with_capacity(64)))
}

#[inline]
fn is_msgpack_format(format: &str) -> bool {
    format.eq_ignore_ascii_case("msgpack") || format.contains("msgpack")
}

#[inline]
fn is_proto_format(format: &str) -> bool {
    format.eq_ignore_ascii_case("proto")
        || format.eq_ignore_ascii_case("protobuf")
        || format.contains("+proto")
}

#[inline]
fn get_msgspec_decode_fn<'py>(
    py: Python<'py>,
    is_msgpack: bool,
) -> PyResult<&'py Bound<'py, PyAny>> {
    if is_msgpack {
        MSGSPEC_MSGPACK_DECODE
            .get_or_try_init(py, || {
                let m = py.import("msgspec.msgpack")?;
                Ok(m.getattr("decode")?.unbind())
            })
            .map(|f| f.bind(py))
    } else {
        MSGSPEC_JSON_DECODE
            .get_or_try_init(py, || {
                let m = py.import("msgspec.json")?;
                Ok(m.getattr("decode")?.unbind())
            })
            .map(|f| f.bind(py))
    }
}

#[inline]
fn get_bound_msgspec_decoder(
    py: Python<'_>,
    target_type: &Bound<'_, PyAny>,
    is_msgpack: bool,
) -> PyResult<Py<PyAny>> {
    let key = (target_type.as_ptr() as usize, is_msgpack);
    if let Ok(guard) = decoder_cache().read()
        && let Some(cached) = guard.get(&key)
    {
        return Ok(cached.clone_ref(py));
    }

    let decoder_cls = if is_msgpack {
        MSGSPEC_MSGPACK_DECODER
            .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
                let m = py.import("msgspec.msgpack")?;
                Ok(m.getattr("Decoder")?.unbind())
            })?
            .bind(py)
    } else {
        MSGSPEC_JSON_DECODER
            .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
                let m = py.import("msgspec.json")?;
                Ok(m.getattr("Decoder")?.unbind())
            })?
            .bind(py)
    };

    let decoder_instance = decoder_cls.call1((target_type,))?;
    let bound_decode = decoder_instance.getattr("decode")?.unbind();
    if let Ok(mut guard) = decoder_cache().write() {
        guard.insert(key, bound_decode.clone_ref(py));
    }
    Ok(bound_decode)
}

#[inline]
unsafe fn vectorcall_with_memoryview<'py>(
    py: Python<'py>,
    callable_ptr: *mut ffi::PyObject,
    slice: &[u8],
    fallback_on_type_error: bool,
) -> PyResult<Bound<'py, PyAny>> {
    unsafe {
        let ptr = if slice.is_empty() {
            std::ptr::NonNull::<u8>::dangling().as_ptr()
        } else {
            slice.as_ptr()
        };
        let mv = ffi::PyMemoryView_FromMemory(
            ptr as *mut std::os::raw::c_char,
            slice.len() as ffi::Py_ssize_t,
            ffi::PyBUF_READ,
        );
        if mv.is_null() {
            return Err(PyErr::fetch(py));
        }
        let args = [ptr::null_mut(), mv];
        let res = ffi::PyObject_Vectorcall(
            callable_ptr,
            args.as_ptr().add(1),
            1 | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
            ptr::null_mut(),
        );
        ffi::Py_DECREF(mv);
        if !res.is_null() {
            return Ok(Bound::from_owned_ptr(py, res));
        }

        let err = PyErr::fetch(py);
        if fallback_on_type_error && err.is_instance_of::<pyo3::exceptions::PyTypeError>(py) {
            let py_bytes = ffi::PyBytes_FromStringAndSize(
                ptr as *const std::os::raw::c_char,
                slice.len() as ffi::Py_ssize_t,
            );
            if py_bytes.is_null() {
                return Err(PyErr::fetch(py));
            }
            let retry_args = [ptr::null_mut(), py_bytes];
            let retry_res = ffi::PyObject_Vectorcall(
                callable_ptr,
                retry_args.as_ptr().add(1),
                1 | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                ptr::null_mut(),
            );
            ffi::Py_DECREF(py_bytes);
            if retry_res.is_null() {
                return Err(PyErr::fetch(py));
            }
            return Ok(Bound::from_owned_ptr(py, retry_res));
        }
        Err(err)
    }
}

/// Decode a raw byte slice directly via `msgspec` using an ephemeral read-only `PyMemoryView_FromMemory` + `PyObject_Vectorcall`.
pub fn decode_msgspec_from_slice<'py>(
    py: Python<'py>,
    slice: &[u8],
    target_type: Option<&Bound<'py, PyAny>>,
    format: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let is_msgpack = is_msgpack_format(format);
    let callable: Py<PyAny> = match target_type {
        Some(ty) if !ty.is_none() => get_bound_msgspec_decoder(py, ty, is_msgpack)?,
        _ => get_msgspec_decode_fn(py, is_msgpack)?.clone().unbind(),
    };
    unsafe { vectorcall_with_memoryview(py, callable.as_ptr(), slice, false) }
}

/// Decode a PEP 3118 buffer owner (`Request` or `WsgiInput`) directly via `msgspec` using `PyObject_Vectorcall`.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn decode_msgspec_from_buffer<'py>(
    py: Python<'py>,
    buffer_owner_ptr: *mut ffi::PyObject,
    target_type: Option<&Bound<'py, PyAny>>,
    format: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let is_msgpack = is_msgpack_format(format);

    let callable: Py<PyAny> = match target_type {
        Some(ty) if !ty.is_none() => get_bound_msgspec_decoder(py, ty, is_msgpack)?,
        _ => get_msgspec_decode_fn(py, is_msgpack)?.clone().unbind(),
    };

    unsafe {
        let mv = ffi::PyMemoryView_FromObject(buffer_owner_ptr);
        if mv.is_null() {
            return Err(PyErr::fetch(py));
        }
        let args = [ptr::null_mut(), mv];
        let res = ffi::PyObject_Vectorcall(
            callable.as_ptr(),
            args.as_ptr().add(1),
            1 | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
            ptr::null_mut(),
        );
        ffi::Py_DECREF(mv);
        if res.is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(Bound::from_owned_ptr(py, res))
    }
}

/// Resolve and cache the `pydantic-core` `validate_json` callable for a model or TypeAdapter.
fn get_pydantic_validate_json(
    py: Python<'_>,
    validator_or_type: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let key = validator_or_type.as_ptr() as usize;
    if let Ok(guard) = pydantic_cache().read()
        && let Some(cached) = guard.get(&key)
    {
        return Ok(cached.clone_ref(py));
    }
    let resolved = if let Ok(v) = validator_or_type.getattr("__pydantic_validator__") {
        v.getattr("validate_json")?.unbind()
    } else if let Ok(m) = validator_or_type.getattr("model_validate_json") {
        m.unbind()
    } else if let Ok(m) = validator_or_type.getattr("validate_json") {
        m.unbind()
    } else {
        validator_or_type.clone().unbind()
    };
    if let Ok(mut guard) = pydantic_cache().write() {
        guard.insert(key, resolved.clone_ref(py));
    }
    Ok(resolved)
}

/// Resolve and cache a Protobuf deserialization callable (`FromString`, `from_bytes`, or `parse`) for `target_type`.
fn get_protobuf_decoder(py: Python<'_>, target_type: &Bound<'_, PyAny>) -> Option<Py<PyAny>> {
    let key = target_type.as_ptr() as usize;
    if let Ok(guard) = protobuf_cache().read()
        && let Some(cached) = guard.get(&key)
    {
        return Some(cached.clone_ref(py));
    }
    let resolved = if let Ok(m) = target_type.getattr("FromString") {
        Some(m.unbind())
    } else if let Ok(m) = target_type.getattr("from_bytes") {
        Some(m.unbind())
    } else {
        None
    }?;
    if let Ok(mut guard) = protobuf_cache().write() {
        guard.insert(key, resolved.clone_ref(py));
    }
    Some(resolved)
}

/// Decode a PEP 3118 buffer owner (`Request` or `WsgiInput`) via `pydantic-core` `validate_json`.
pub fn decode_pydantic_from_buffer<'py>(
    py: Python<'py>,
    _buffer_owner_ptr: *mut ffi::PyObject,
    validator_or_type: &Bound<'py, PyAny>,
    raw_fallback: &[u8],
) -> PyResult<Bound<'py, PyAny>> {
    let validate_fn = get_pydantic_validate_json(py, validator_or_type)?;

    unsafe {
        let py_bytes = ffi::PyBytes_FromStringAndSize(
            raw_fallback.as_ptr() as *const std::os::raw::c_char,
            raw_fallback.len() as ffi::Py_ssize_t,
        );
        if py_bytes.is_null() {
            return Err(PyErr::fetch(py));
        }
        let args = [ptr::null_mut(), py_bytes];
        let res = ffi::PyObject_Vectorcall(
            validate_fn.as_ptr(),
            args.as_ptr().add(1),
            1 | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
            ptr::null_mut(),
        );
        ffi::Py_DECREF(py_bytes);
        if !res.is_null() {
            return Ok(Bound::from_owned_ptr(py, res));
        }
        Err(PyErr::fetch(py))
    }
}

/// Strip a 5-byte gRPC Length-Prefixed Message (LPM) header from `raw` without copying.
///
/// If `raw` begins with a valid 5-byte LPM header (`flag in 0..=1` and `5 + len <= raw.len()`),
/// returns `(compressed, &raw[5..5 + len], 5 + len)`. Otherwise returns `(false, raw, raw.len())`
/// so callers can transparently accept both LPM-framed gRPC bodies and unframed Connect/HTTP bodies.
#[inline]
pub fn strip_grpc_lpm_header(raw: &[u8]) -> PyResult<(bool, &[u8], usize)> {
    if raw.len() >= GRPC_LPM_HEADER_LEN {
        let flag = raw[0];
        let msg_len = u32::from_be_bytes([raw[1], raw[2], raw[3], raw[4]]) as usize;
        if flag <= 1 && GRPC_LPM_HEADER_LEN + msg_len <= raw.len() {
            return Ok((
                flag == 1,
                &raw[GRPC_LPM_HEADER_LEN..GRPC_LPM_HEADER_LEN + msg_len],
                GRPC_LPM_HEADER_LEN + msg_len,
            ));
        }
    }
    Ok((false, raw, raw.len()))
}

/// Decode a single gRPC payload slice into `target_type` using `msgspec` (MsgPack/JSON), `pydantic-core`, or Protobuf `FromString`.
pub fn decode_grpc_payload_slice<'py>(
    py: Python<'py>,
    payload: &[u8],
    target_type: Option<&Bound<'py, PyAny>>,
    format: &str,
) -> PyResult<Bound<'py, PyAny>> {
    if let Some(ty) = target_type
        && !ty.is_none()
    {
        if is_proto_format(format)
            && let Some(proto_fn) = get_protobuf_decoder(py, ty)
        {
            return unsafe { vectorcall_with_memoryview(py, proto_fn.as_ptr(), payload, true) };
        }
        if ty.hasattr("__pydantic_validator__")? && !is_msgpack_format(format) {
            let val_fn = get_pydantic_validate_json(py, ty)?;
            return unsafe { vectorcall_with_memoryview(py, val_fn.as_ptr(), payload, true) };
        }
        if let Some(proto_fn) = get_protobuf_decoder(py, ty) {
            return unsafe { vectorcall_with_memoryview(py, proto_fn.as_ptr(), payload, true) };
        }
    }
    decode_msgspec_from_slice(py, payload, target_type, format)
}

/// Decode a unary gRPC LPM frame from `raw` in zero copy via `PyMemoryView_FromMemory` + `PyObject_Vectorcall`.
pub fn decode_grpc_unary_from_slice<'py>(
    py: Python<'py>,
    raw: &[u8],
    target_type: Option<&Bound<'py, PyAny>>,
    format: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let (_compressed, payload, _consumed) = strip_grpc_lpm_header(raw)?;
    decode_grpc_payload_slice(py, payload, target_type, format)
}

/// Decode all concatenated 5-byte LPM gRPC frames in `raw` into a Python `list` in zero copy.
pub fn decode_grpc_frames_from_slice<'py>(
    py: Python<'py>,
    raw: &[u8],
    target_type: Option<&Bound<'py, PyAny>>,
    format: &str,
) -> PyResult<Bound<'py, PyList>> {
    if raw.is_empty() {
        return Ok(PyList::empty(py));
    }

    let mut slices: Vec<&[u8]> = Vec::with_capacity(4);
    let mut offset = 0usize;
    while offset + GRPC_LPM_HEADER_LEN <= raw.len() {
        let flag = raw[offset];
        if flag > 1 {
            if offset == 0 {
                slices.push(raw);
                break;
            }
            return Err(PyValueError::new_err(format!(
                "invalid gRPC compression flag byte: {flag}"
            )));
        }
        let msg_len = u32::from_be_bytes([
            raw[offset + 1],
            raw[offset + 2],
            raw[offset + 3],
            raw[offset + 4],
        ]) as usize;
        if offset + GRPC_LPM_HEADER_LEN + msg_len > raw.len() {
            if offset == 0 {
                slices.push(raw);
            }
            break;
        }
        slices.push(&raw[offset + GRPC_LPM_HEADER_LEN..offset + GRPC_LPM_HEADER_LEN + msg_len]);
        offset += GRPC_LPM_HEADER_LEN + msg_len;
    }

    unsafe {
        let out_list = ffi::PyList_New(slices.len() as ffi::Py_ssize_t);
        if out_list.is_null() {
            return Err(PyErr::fetch(py));
        }
        for (i, payload) in slices.into_iter().enumerate() {
            match decode_grpc_payload_slice(py, payload, target_type, format) {
                Ok(item) => {
                    ffi::PyList_SET_ITEM(out_list, i as ffi::Py_ssize_t, item.into_ptr());
                }
                Err(err) => {
                    ffi::Py_DECREF(out_list);
                    return Err(err);
                }
            }
        }
        Ok(Bound::from_owned_ptr(py, out_list).cast_into_unchecked())
    }
}

/// Frame a payload buffer with a 5-byte gRPC Length-Prefixed Message (LPM) header in a single allocation.
#[pyfunction]
#[pyo3(signature = (payload, compressed=false))]
pub fn pack_grpc_lpm_frame<'py>(
    py: Python<'py>,
    payload: &Bound<'py, PyAny>,
    compressed: bool,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = crate::response::extract_buffer_bytes(py, payload)?;
    let total = GRPC_LPM_HEADER_LEN + bytes.len();
    PyBytes::new_with(py, total, |dst| {
        dst[0] = u8::from(compressed);
        let len_be = (bytes.len() as u32).to_be_bytes();
        dst[1..5].copy_from_slice(&len_be);
        dst[5..].copy_from_slice(&bytes);
        Ok(())
    })
}

/// Resolve and cache the field names `PyTuple` for a struct, dataclass, attrs, or Pydantic model.
fn resolve_struct_fields<'py>(
    py: Python<'py>,
    schema_type: &Bound<'py, PyAny>,
    explicit_fields: Option<&Bound<'py, PyTuple>>,
) -> PyResult<Bound<'py, PyTuple>> {
    if let Some(f) = explicit_fields {
        return Ok(f.clone());
    }
    let key = schema_type.as_ptr() as usize;
    if let Ok(guard) = struct_fields_cache().read()
        && let Some(cached) = guard.get(&key)
    {
        return Ok(cached.bind(py).clone());
    }

    let tuple: Bound<'py, PyTuple> = if let Ok(sf) = schema_type.getattr("__struct_fields__")
        && let Ok(t) = sf.cast::<PyTuple>()
    {
        t.clone()
    } else if let Ok(df) = schema_type.getattr("__dataclass_fields__")
        && let Ok(d) = df.cast::<PyDict>()
    {
        let keys: Vec<Bound<'py, PyAny>> = d.keys().iter().collect();
        PyTuple::new(py, keys)?
    } else if let Ok(af) = schema_type.getattr("__attrs_attrs__")
        && let Ok(t) = af.cast::<PyTuple>()
    {
        let mut names = Vec::with_capacity(t.len());
        for item in t.iter() {
            names.push(item.getattr("name")?);
        }
        PyTuple::new(py, names)?
    } else if let Ok(mf) = schema_type.getattr("model_fields")
        && let Ok(d) = mf.cast::<PyDict>()
    {
        let keys: Vec<Bound<'py, PyAny>> = d.keys().iter().collect();
        PyTuple::new(py, keys)?
    } else {
        PyTuple::empty(py)
    };

    if !tuple.is_empty()
        && let Ok(mut guard) = struct_fields_cache().write()
    {
        guard.insert(key, tuple.clone().unbind());
    }
    Ok(tuple)
}

/// Instantiate a `msgspec.Struct`, `@dataclass`, `attrs`, or Python type via `PyObject_Vectorcall`.
#[pyfunction]
#[pyo3(signature = (cls, args=None, kwargs=None))]
pub fn instantiate_struct_vectorcall<'py>(
    py: Python<'py>,
    cls: &Bound<'py, PyAny>,
    args: Option<&Bound<'py, PyTuple>>,
    kwargs: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    call_vectorcall(py, cls, args, kwargs)
}

/// Invoke a Python callable via `PyObject_Vectorcall` / `PyObject_VectorcallDict` using a stack buffer.
#[pyfunction]
#[pyo3(signature = (callable, args=None, kwargs=None))]
pub fn call_vectorcall<'py>(
    py: Python<'py>,
    callable: &Bound<'py, PyAny>,
    args: Option<&Bound<'py, PyTuple>>,
    kwargs: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    unsafe {
        let nargs = args.map_or(0, |t| t.len());
        let kwargs_ptr = match kwargs {
            Some(d) if !d.is_empty() => d.as_ptr(),
            _ => ptr::null_mut(),
        };

        if nargs == 0 && kwargs_ptr.is_null() {
            let mut stack_buf: [*mut ffi::PyObject; 1] = [ptr::null_mut()];
            let res = ffi::PyObject_Vectorcall(
                callable.as_ptr(),
                stack_buf.as_mut_ptr().add(1),
                ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                ptr::null_mut(),
            );
            if res.is_null() {
                return Err(PyErr::fetch(py));
            }
            return Ok(Bound::from_owned_ptr(py, res));
        }

        if nargs <= 16 {
            let mut stack_buf: [*mut ffi::PyObject; 17] = [ptr::null_mut(); 17];
            if let Some(t) = args {
                let tuple_ptr = t.as_ptr();
                for i in 0..nargs {
                    stack_buf[i + 1] = ffi::PyTuple_GET_ITEM(tuple_ptr, i as ffi::Py_ssize_t);
                }
            }
            let res = if kwargs_ptr.is_null() {
                ffi::PyObject_Vectorcall(
                    callable.as_ptr(),
                    stack_buf.as_mut_ptr().add(1),
                    nargs | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                    ptr::null_mut(),
                )
            } else {
                ffi::PyObject_VectorcallDict(
                    callable.as_ptr(),
                    stack_buf.as_mut_ptr().add(1),
                    nargs | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                    kwargs_ptr,
                )
            };
            if res.is_null() {
                return Err(PyErr::fetch(py));
            }
            return Ok(Bound::from_owned_ptr(py, res));
        }

        let mut heap_buf: Vec<*mut ffi::PyObject> = Vec::with_capacity(nargs + 1);
        heap_buf.push(ptr::null_mut());
        if let Some(t) = args {
            let tuple_ptr = t.as_ptr();
            for i in 0..nargs {
                heap_buf.push(ffi::PyTuple_GET_ITEM(tuple_ptr, i as ffi::Py_ssize_t));
            }
        }
        let res = if kwargs_ptr.is_null() {
            ffi::PyObject_Vectorcall(
                callable.as_ptr(),
                heap_buf.as_mut_ptr().add(1),
                nargs | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                ptr::null_mut(),
            )
        } else {
            ffi::PyObject_VectorcallDict(
                callable.as_ptr(),
                heap_buf.as_mut_ptr().add(1),
                nargs | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                kwargs_ptr,
            )
        };
        if res.is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(Bound::from_owned_ptr(py, res))
    }
}

#[inline]
unsafe fn hydrate_single_item<'py>(
    py: Python<'py>,
    schema_type: &Bound<'py, PyAny>,
    pydantic_validate_python: Option<&Bound<'py, PyAny>>,
    item_ptr: *mut ffi::PyObject,
    kwnames: &Bound<'py, PyTuple>,
) -> PyResult<*mut ffi::PyObject> {
    unsafe {
        if ffi::PyObject_IsInstance(item_ptr, schema_type.as_ptr()) == 1 {
            ffi::Py_INCREF(item_ptr);
            return Ok(item_ptr);
        }

        if ffi::PyDict_Check(item_ptr) != 0 {
            if let Some(val_fn) = pydantic_validate_python {
                let args = [ptr::null_mut(), item_ptr];
                let res = ffi::PyObject_Vectorcall(
                    val_fn.as_ptr(),
                    args.as_ptr().add(1),
                    1 | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                    ptr::null_mut(),
                );
                if res.is_null() {
                    return Err(PyErr::fetch(py));
                }
                return Ok(res);
            }
            let res = ffi::PyObject_VectorcallDict(schema_type.as_ptr(), ptr::null(), 0, item_ptr);
            if res.is_null() {
                return Err(PyErr::fetch(py));
            }
            return Ok(res);
        }

        let is_tuple = ffi::PyTuple_Check(item_ptr) != 0;
        let is_list = !is_tuple && ffi::PyList_Check(item_ptr) != 0;
        if is_tuple || is_list {
            let seq_len = if is_tuple {
                ffi::PyTuple_GET_SIZE(item_ptr) as usize
            } else {
                ffi::PyList_GET_SIZE(item_ptr) as usize
            };
            let kw_len = kwnames.len();
            let use_kw = kw_len > 0 && kw_len == seq_len;

            if seq_len <= 32 {
                let mut buf: [*mut ffi::PyObject; 33] = [ptr::null_mut(); 33];
                for i in 0..seq_len {
                    buf[i + 1] = if is_tuple {
                        ffi::PyTuple_GET_ITEM(item_ptr, i as ffi::Py_ssize_t)
                    } else {
                        ffi::PyList_GET_ITEM(item_ptr, i as ffi::Py_ssize_t)
                    };
                }
                let res = if use_kw {
                    ffi::PyObject_Vectorcall(
                        schema_type.as_ptr(),
                        buf.as_mut_ptr().add(1),
                        ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                        kwnames.as_ptr(),
                    )
                } else {
                    ffi::PyObject_Vectorcall(
                        schema_type.as_ptr(),
                        buf.as_mut_ptr().add(1),
                        seq_len | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                        ptr::null_mut(),
                    )
                };
                if res.is_null() {
                    return Err(PyErr::fetch(py));
                }
                return Ok(res);
            }

            let mut heap_buf: Vec<*mut ffi::PyObject> = Vec::with_capacity(seq_len + 1);
            heap_buf.push(ptr::null_mut());
            for i in 0..seq_len {
                let elem = if is_tuple {
                    ffi::PyTuple_GET_ITEM(item_ptr, i as ffi::Py_ssize_t)
                } else {
                    ffi::PyList_GET_ITEM(item_ptr, i as ffi::Py_ssize_t)
                };
                heap_buf.push(elem);
            }
            let res = if use_kw {
                ffi::PyObject_Vectorcall(
                    schema_type.as_ptr(),
                    heap_buf.as_mut_ptr().add(1),
                    ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                    kwnames.as_ptr(),
                )
            } else {
                ffi::PyObject_Vectorcall(
                    schema_type.as_ptr(),
                    heap_buf.as_mut_ptr().add(1),
                    seq_len | ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                    ptr::null_mut(),
                )
            };
            if res.is_null() {
                return Err(PyErr::fetch(py));
            }
            return Ok(res);
        }

        if ffi::PyMapping_Check(item_ptr) != 0 && !kwnames.is_empty() {
            let kw_len = kwnames.len();
            if kw_len <= 32 {
                let mut buf: [*mut ffi::PyObject; 33] = [ptr::null_mut(); 33];
                let mut fetched = 0usize;
                let kwnames_ptr = kwnames.as_ptr();
                for i in 0..kw_len {
                    let key_ptr = ffi::PyTuple_GET_ITEM(kwnames_ptr, i as ffi::Py_ssize_t);
                    let val_ptr = ffi::PyObject_GetItem(item_ptr, key_ptr);
                    if val_ptr.is_null() {
                        ffi::PyErr_Clear();
                        let attr_ptr = ffi::PyObject_GetAttr(item_ptr, key_ptr);
                        if attr_ptr.is_null() {
                            for &ptr_val in buf.iter().take(fetched + 1).skip(1) {
                                ffi::Py_DECREF(ptr_val);
                            }
                            return Err(PyErr::fetch(py));
                        }
                        buf[i + 1] = attr_ptr;
                    } else {
                        buf[i + 1] = val_ptr;
                    }
                    fetched += 1;
                }
                let res = ffi::PyObject_Vectorcall(
                    schema_type.as_ptr(),
                    buf.as_mut_ptr().add(1),
                    ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                    kwnames_ptr,
                );
                for &ptr_val in buf.iter().take(fetched + 1).skip(1) {
                    ffi::Py_DECREF(ptr_val);
                }
                if res.is_null() {
                    return Err(PyErr::fetch(py));
                }
                return Ok(res);
            }
        }

        hydrate_orm_ptr(py, schema_type, item_ptr, kwnames)
    }
}

#[inline]
unsafe fn hydrate_orm_ptr<'py>(
    py: Python<'py>,
    schema_type: &Bound<'py, PyAny>,
    orm_ptr: *mut ffi::PyObject,
    kwnames: &Bound<'py, PyTuple>,
) -> PyResult<*mut ffi::PyObject> {
    unsafe {
        let n = kwnames.len();
        let kwnames_ptr = kwnames.as_ptr();
        if n <= 32 {
            let mut buf: [*mut ffi::PyObject; 33] = [ptr::null_mut(); 33];
            let mut fetched = 0usize;
            for i in 0..n {
                let attr_name = ffi::PyTuple_GET_ITEM(kwnames_ptr, i as ffi::Py_ssize_t);
                let val = ffi::PyObject_GetAttr(orm_ptr, attr_name);
                if val.is_null() {
                    for &ptr_val in buf.iter().take(fetched + 1).skip(1) {
                        ffi::Py_DECREF(ptr_val);
                    }
                    return Err(PyErr::fetch(py));
                }
                buf[i + 1] = val;
                fetched += 1;
            }
            let res = ffi::PyObject_Vectorcall(
                schema_type.as_ptr(),
                buf.as_mut_ptr().add(1),
                ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
                kwnames_ptr,
            );
            for &ptr_val in buf.iter().take(fetched + 1).skip(1) {
                ffi::Py_DECREF(ptr_val);
            }
            if res.is_null() {
                return Err(PyErr::fetch(py));
            }
            return Ok(res);
        }

        let mut heap_buf: Vec<*mut ffi::PyObject> = Vec::with_capacity(n + 1);
        heap_buf.push(ptr::null_mut());
        for i in 0..n {
            let attr_name = ffi::PyTuple_GET_ITEM(kwnames_ptr, i as ffi::Py_ssize_t);
            let val = ffi::PyObject_GetAttr(orm_ptr, attr_name);
            if val.is_null() {
                for &ptr_val in &heap_buf[1..] {
                    ffi::Py_DECREF(ptr_val);
                }
                return Err(PyErr::fetch(py));
            }
            heap_buf.push(val);
        }
        let res = ffi::PyObject_Vectorcall(
            schema_type.as_ptr(),
            heap_buf.as_mut_ptr().add(1),
            ffi::PY_VECTORCALL_ARGUMENTS_OFFSET,
            kwnames_ptr,
        );
        for &ptr_val in &heap_buf[1..] {
            ffi::Py_DECREF(ptr_val);
        }
        if res.is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(res)
    }
}

fn normalize_columns_tuple<'py>(
    py: Python<'py>,
    columns: Option<&Bound<'py, PyAny>>,
    schema_type: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyTuple>> {
    if let Some(cols) = columns
        && !cols.is_none()
    {
        if let Ok(t) = cols.cast::<PyTuple>() {
            return Ok(t.clone());
        }
        if let Ok(l) = cols.cast::<PyList>() {
            let items: Vec<Bound<'py, PyAny>> = l.iter().collect();
            return PyTuple::new(py, items);
        }
        let mut items = Vec::new();
        for item in cols.try_iter()? {
            let s = item?;
            let interned = PyString::intern(py, s.extract::<&str>()?);
            items.push(interned.into_any());
        }
        return PyTuple::new(py, items);
    }
    resolve_struct_fields(py, schema_type, None)
}

/// Hydrate a single database row (`dict`, `tuple`, `list`, `Record`, or ORM object) into `schema_type`.
#[pyfunction]
#[pyo3(signature = (schema_type, row, columns=None))]
pub fn hydrate_one_vectorcall<'py>(
    py: Python<'py>,
    schema_type: &Bound<'py, PyAny>,
    row: &Bound<'py, PyAny>,
    columns: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let kwnames = normalize_columns_tuple(py, columns, schema_type)?;
    let pydantic_val = schema_type
        .getattr("__pydantic_validator__")
        .ok()
        .and_then(|v| v.getattr("validate_python").ok());
    unsafe {
        let ptr = hydrate_single_item(
            py,
            schema_type,
            pydantic_val.as_ref(),
            row.as_ptr(),
            &kwnames,
        )?;
        Ok(Bound::from_owned_ptr(py, ptr))
    }
}

/// Hydrate a sequence of database rows (`list`/`tuple` of `dict`, `tuple`, `Record`, or ORM objects) into `list[schema_type]`.
#[pyfunction]
#[pyo3(signature = (schema_type, rows, columns=None))]
pub fn hydrate_rows_vectorcall<'py>(
    py: Python<'py>,
    schema_type: &Bound<'py, PyAny>,
    rows: &Bound<'py, PyAny>,
    columns: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyList>> {
    let kwnames = normalize_columns_tuple(py, columns, schema_type)?;
    let pydantic_val = schema_type
        .getattr("__pydantic_validator__")
        .ok()
        .and_then(|v| v.getattr("validate_python").ok());

    unsafe {
        let rows_ptr = rows.as_ptr();
        let is_list = ffi::PyList_Check(rows_ptr) != 0;
        let is_tuple = !is_list && ffi::PyTuple_Check(rows_ptr) != 0;

        if is_list || is_tuple {
            let len = if is_list {
                ffi::PyList_GET_SIZE(rows_ptr)
            } else {
                ffi::PyTuple_GET_SIZE(rows_ptr)
            };
            let out_list = ffi::PyList_New(len);
            if out_list.is_null() {
                return Err(PyErr::fetch(py));
            }
            for i in 0..len {
                let item_ptr = if is_list {
                    ffi::PyList_GET_ITEM(rows_ptr, i)
                } else {
                    ffi::PyTuple_GET_ITEM(rows_ptr, i)
                };
                match hydrate_single_item(
                    py,
                    schema_type,
                    pydantic_val.as_ref(),
                    item_ptr,
                    &kwnames,
                ) {
                    Ok(hydrated) => {
                        ffi::PyList_SET_ITEM(out_list, i, hydrated);
                    }
                    Err(err) => {
                        ffi::Py_DECREF(out_list);
                        return Err(err);
                    }
                }
            }
            return Ok(Bound::from_owned_ptr(py, out_list).cast_into_unchecked());
        }
    }

    let out = PyList::empty(py);
    for item_res in rows.try_iter()? {
        let item = item_res?;
        let hydrated_ptr = unsafe {
            hydrate_single_item(
                py,
                schema_type,
                pydantic_val.as_ref(),
                item.as_ptr(),
                &kwnames,
            )?
        };
        let hydrated = unsafe { Bound::from_owned_ptr(py, hydrated_ptr) };
        out.append(hydrated)?;
    }
    Ok(out)
}

/// Project an ORM instance (or `list`/`tuple` of ORM instances) into `schema_type` via `PyObject_GetAttr` + `PyObject_Vectorcall`.
#[pyfunction]
#[pyo3(signature = (schema_type, orm_obj, fields=None))]
pub fn orm_to_struct_vectorcall<'py>(
    py: Python<'py>,
    schema_type: &Bound<'py, PyAny>,
    orm_obj: &Bound<'py, PyAny>,
    fields: Option<&Bound<'py, PyTuple>>,
) -> PyResult<Bound<'py, PyAny>> {
    let kwnames = resolve_struct_fields(py, schema_type, fields)?;
    unsafe {
        let obj_ptr = orm_obj.as_ptr();
        let is_list = ffi::PyList_Check(obj_ptr) != 0;
        let is_tuple = !is_list && ffi::PyTuple_Check(obj_ptr) != 0;
        if is_list || is_tuple {
            let len = if is_list {
                ffi::PyList_GET_SIZE(obj_ptr)
            } else {
                ffi::PyTuple_GET_SIZE(obj_ptr)
            };
            let out_list = ffi::PyList_New(len);
            if out_list.is_null() {
                return Err(PyErr::fetch(py));
            }
            for i in 0..len {
                let elem_ptr = if is_list {
                    ffi::PyList_GET_ITEM(obj_ptr, i)
                } else {
                    ffi::PyTuple_GET_ITEM(obj_ptr, i)
                };
                match hydrate_orm_ptr(py, schema_type, elem_ptr, &kwnames) {
                    Ok(hydrated) => {
                        ffi::PyList_SET_ITEM(out_list, i, hydrated);
                    }
                    Err(err) => {
                        ffi::Py_DECREF(out_list);
                        return Err(err);
                    }
                }
            }
            return Ok(Bound::from_owned_ptr(py, out_list));
        }

        let res_ptr = hydrate_orm_ptr(py, schema_type, obj_ptr, &kwnames)?;
        Ok(Bound::from_owned_ptr(py, res_ptr))
    }
}
