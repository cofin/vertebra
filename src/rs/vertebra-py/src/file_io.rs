//! File I/O helpers for streaming file chunks and range responses.

use std::io::{Read, Seek, SeekFrom};

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

pub enum FileRangeError {
    InvalidRange(&'static str),
    Io(std::io::Error),
}

impl From<std::io::Error> for FileRangeError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

pub fn stat_file_range_sync(
    path: &str,
    start: u64,
    end: u64,
) -> Result<(u64, u64), FileRangeError> {
    let metadata = std::fs::metadata(path)?;
    let total_len = metadata.len();
    let clamped_end = end.min(total_len);
    if start >= clamped_end {
        return Err(FileRangeError::InvalidRange(
            "Invalid file range: start exceeds file length",
        ));
    }
    Ok((clamped_end, total_len))
}

pub fn read_file_range_sync(
    path: &str,
    start: Option<u64>,
    end: Option<u64>,
) -> std::io::Result<bytes::Bytes> {
    let mut file = std::fs::File::open(path)?;
    let total_len = file.metadata()?.len();
    let s = start.unwrap_or(0);
    let e = end.unwrap_or(total_len).min(total_len);
    if s > 0 {
        file.seek(SeekFrom::Start(s))?;
    }
    let len = e.saturating_sub(s) as usize;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(bytes::Bytes::from(buf))
}

/// PyO3 helper that reads a file or byte range in `chunk_size` chunks while releasing the GIL.
#[pyfunction]
#[pyo3(name = "stream_file_chunks", signature = (path, start=None, end=None, chunk_size=65536))]
pub fn stream_file_chunks<'py>(
    py: Python<'py>,
    path: &str,
    start: Option<u64>,
    end: Option<u64>,
    chunk_size: usize,
) -> PyResult<Bound<'py, PyList>> {
    if let (Some(s), Some(e)) = (start, end)
        && s >= e
    {
        return Err(PyValueError::new_err(
            "Invalid file range: start must be less than end",
        ));
    }
    if chunk_size == 0 {
        return Err(PyValueError::new_err("chunk_size must be > 0"));
    }
    let path_owned = path.to_string();
    let chunks = py
        .detach(|| -> std::io::Result<Vec<Vec<u8>>> {
            let mut file = std::fs::File::open(&path_owned)?;
            let total_len = file.metadata()?.len();
            let s = start.unwrap_or(0);
            let e = end.unwrap_or(total_len).min(total_len);
            if s > 0 {
                file.seek(SeekFrom::Start(s))?;
            }
            let mut remaining = e.saturating_sub(s) as usize;
            let mut out = Vec::new();
            let mut buf = vec![0u8; chunk_size];
            while remaining > 0 {
                let to_read = remaining.min(chunk_size);
                let n = file.read(&mut buf[..to_read])?;
                if n == 0 {
                    break;
                }
                out.push(buf[..n].to_vec());
                remaining -= n;
            }
            Ok(out)
        })
        .map_err(|e| PyIOError::new_err(format!("failed to stream file {path}: {e}")))?;

    let py_chunks: Vec<Bound<'py, PyAny>> = chunks
        .into_iter()
        .map(|c| PyBytes::new(py, &c).into_any())
        .collect();
    PyList::new(py, &py_chunks)
}
