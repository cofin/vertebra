//! Response builder for the Vertebra batch runner.

use bytes::Bytes;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PyTuple};

#[pyclass(name = "VertebraResponse", skip_from_py_object)]
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
                } else {
                    Bytes::copy_from_slice(&b.extract::<Vec<u8>>()?)
                }
            }
            None => Bytes::new(),
        };

        let mut header_vec: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        if let Some(h) = headers {
            if let Ok(dict) = h.cast::<PyDict>() {
                for (k, v) in dict.iter() {
                    header_vec.push((
                        k.extract::<Vec<u8>>().or_else(|_| k.extract::<String>().map(|s| s.into_bytes()))?,
                        v.extract::<Vec<u8>>().or_else(|_| v.extract::<String>().map(|s| s.into_bytes()))?,
                    ));
                }
            } else if let Ok(list) = h.cast::<PyList>() {
                for item in list.iter() {
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

                    header_vec.push((
                        k_obj.extract::<Vec<u8>>().or_else(|_| k_obj.extract::<String>().map(|s| s.into_bytes()))?,
                        v_obj.extract::<Vec<u8>>().or_else(|_| v_obj.extract::<String>().map(|s| s.into_bytes()))?,
                    ));
                }
            } else if let Ok(tuple) = h.cast::<PyTuple>() {
                for item in tuple.iter() {
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

                    header_vec.push((
                        k_obj.extract::<Vec<u8>>().or_else(|_| k_obj.extract::<String>().map(|s| s.into_bytes()))?,
                        v_obj.extract::<Vec<u8>>().or_else(|_| v_obj.extract::<String>().map(|s| s.into_bytes()))?,
                    ));
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
