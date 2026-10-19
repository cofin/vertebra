//! Response builder for the Vertebra batch runner.

use bytes::Bytes;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PyString, PyTuple};

#[inline]
fn extract_header_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = obj.cast::<PyBytes>() {
        Ok(b.as_bytes().to_vec())
    } else if let Ok(s) = obj.cast::<PyString>() {
        Ok(s.to_str()?.as_bytes().to_vec())
    } else {
        obj.extract::<Vec<u8>>()
    }
}

#[inline]
fn extract_header_entry(item: &Bound<'_, PyAny>) -> PyResult<(Vec<u8>, Vec<u8>)> {
    let (k_obj, v_obj) = if let Ok(t) = item.cast::<PyTuple>() {
        (t.get_item(0)?, t.get_item(1)?)
    } else if let Ok(l) = item.cast::<PyList>() {
        (l.get_item(0)?, l.get_item(1)?)
    } else {
        return Err(PyTypeError::new_err(format!(
            "header entry must be a tuple or list, got {}",
            item.get_type().name()?
        )));
    };
    Ok((extract_header_bytes(&k_obj)?, extract_header_bytes(&v_obj)?))
}

#[pyclass(name = "VertebraResponse")]
#[derive(Clone)]
pub struct VertebraResponse {
    pub status: u16,
    pub body: Bytes,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub request_id: u64,
}

#[pymethods]
impl VertebraResponse {
    #[new]
    #[pyo3(signature = (status, body=None, headers=None, request_id=0))]
    fn new(
        status: u16,
        body: Option<Bound<'_, PyAny>>,
        headers: Option<Bound<'_, PyAny>>,
        request_id: u64,
    ) -> PyResult<Self> {
        let body_bytes = match body {
            Some(b) => {
                if let Ok(py_bytes) = b.cast::<PyBytes>() {
                    Bytes::copy_from_slice(py_bytes.as_bytes())
                } else if let Ok(py_str) = b.cast::<PyString>() {
                    Bytes::copy_from_slice(py_str.to_str()?.as_bytes())
                } else {
                    Bytes::from(b.extract::<Vec<u8>>()?)
                }
            }
            None => Bytes::new(),
        };

        let mut header_vec: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        if let Some(h) = headers {
            if let Ok(dict) = h.cast::<PyDict>() {
                header_vec.reserve(dict.len());
                for (k, v) in dict.iter() {
                    header_vec.push((extract_header_bytes(&k)?, extract_header_bytes(&v)?));
                }
            } else if let Ok(list) = h.cast::<PyList>() {
                header_vec.reserve(list.len());
                for item in list.iter() {
                    header_vec.push(extract_header_entry(&item)?);
                }
            } else if let Ok(tuple) = h.cast::<PyTuple>() {
                header_vec.reserve(tuple.len());
                for item in tuple.iter() {
                    header_vec.push(extract_header_entry(&item)?);
                }
            }
        }

        Ok(Self {
            status,
            body: body_bytes,
            headers: header_vec,
            request_id,
        })
    }

    #[getter]
    pub fn status(&self) -> u16 {
        self.status
    }

    #[getter]
    pub fn body<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.body)
    }

    #[getter]
    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    #[setter]
    pub fn set_request_id(&mut self, request_id: u64) {
        self.request_id = request_id;
    }

    #[getter]
    pub fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::with_capacity(self.headers.len());
        for (name, value) in &self.headers {
            let t = PyTuple::new(
                py,
                [
                    PyBytes::new(py, name).into_any(),
                    PyBytes::new(py, value).into_any(),
                ],
            )?;
            tuples.push(t.into_any());
        }
        PyList::new(py, &tuples)
    }

    fn __repr__(&self) -> String {
        format!(
            "VertebraResponse(status={}, body_len={}, request_id={})",
            self.status,
            self.body.len(),
            self.request_id,
        )
    }
}

impl VertebraResponse {
    pub fn get_headers(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.headers
    }

    pub fn get_body(&self) -> &Bytes {
        &self.body
    }
}
