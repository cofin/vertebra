//! Response builder for the Vertebra batch runner.

use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyString, PyTuple};

#[pyclass(name = "VertebraResponse", from_py_object)]
#[derive(Clone)]
pub struct VertebraResponse {
    pub status: u16,
    pub body: Bytes,
    pub headers: Vec<(String, String)>,
    pub request_id: u64,
}

#[pymethods]
impl VertebraResponse {
    #[new]
    #[pyo3(signature = (status, body=None, headers=None, request_id=0))]
    fn new(
        status: u16,
        body: Option<&[u8]>,
        headers: Option<&Bound<'_, PyAny>>,
        request_id: u64,
    ) -> PyResult<Self> {
        let body = Bytes::copy_from_slice(body.unwrap_or(&[]));
        let mut header_vec: Vec<(String, String)> = Vec::new();

        if let Some(h) = headers {
            if let Ok(dict) = h.cast::<pyo3::types::PyDict>() {
                for (k, v) in dict.iter() {
                    header_vec.push((k.extract::<String>()?, v.extract::<String>()?));
                }
            } else if let Ok(list) = h.cast::<PyList>() {
                for item in list.iter() {
                    let tuple = item.cast::<PyTuple>()?;
                    let k_obj = tuple.get_item(0)?;
                    let v_obj = tuple.get_item(1)?;

                    let k = if let Ok(b) = k_obj.cast::<PyBytes>() {
                        String::from_utf8_lossy(b.as_bytes()).to_string()
                    } else {
                        k_obj.extract::<String>()?
                    };

                    let v = if let Ok(b) = v_obj.cast::<PyBytes>() {
                        String::from_utf8_lossy(b.as_bytes()).to_string()
                    } else {
                        v_obj.extract::<String>()?
                    };

                    header_vec.push((k, v));
                }
            }
        }

        Ok(Self {
            status,
            body,
            headers: header_vec,
            request_id,
        })
    }

    #[getter]
    fn status(&self) -> u16 {
        self.status
    }

    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.body)
    }

    #[getter]
    fn request_id(&self) -> u64 {
        self.request_id
    }

    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut tuples: Vec<Bound<'py, PyAny>> = Vec::with_capacity(self.headers.len());
        for (name, value) in &self.headers {
            let t = PyTuple::new(
                py,
                [
                    PyString::new(py, name).into_any(),
                    PyString::new(py, value).into_any(),
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
