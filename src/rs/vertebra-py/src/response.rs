//! Response builder for the Vertebra batch runner.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyString, PyTuple};

#[pyclass(name = "VertebraResponse")]
pub struct VertebraResponse {
    pub status: u16,
    pub body: Vec<u8>,
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
        let body = body.unwrap_or(&[]).to_vec();
        let mut header_vec: Vec<(String, String)> = Vec::new();

        if let Some(h) = headers {
            if let Ok(dict) = h.cast::<pyo3::types::PyDict>() {
                for (k, v) in dict.iter() {
                    header_vec.push((k.extract::<String>()?, v.extract::<String>()?));
                }
            } else if let Ok(list) = h.cast::<PyList>() {
                for item in list.iter() {
                    let tuple = item.cast::<PyTuple>()?;
                    header_vec.push((
                        tuple.get_item(0)?.extract::<String>()?,
                        tuple.get_item(1)?.extract::<String>()?,
                    ));
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
