//! Rust ASGI 3.0 wire extension execution helpers (`pathsend` chunked file reader, `zerocopysend`, `trailers`).

use std::io::{Read, Seek, SeekFrom};

use futures_util::StreamExt;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

/// Stream a file or `[start, end)` byte range in bounded chunks using `tokio::fs::File` and `ReaderStream`.
pub async fn read_file_range_chunks(
    path: &str,
    start: Option<u64>,
    end: Option<u64>,
    chunk_size: usize,
) -> std::io::Result<Vec<bytes::Bytes>> {
    let mut file = tokio::fs::File::open(path).await?;
    let meta = file.metadata().await?;
    let total_len = meta.len();
    let range_start = start.unwrap_or(0);
    let range_end = end.unwrap_or(total_len).min(total_len);
    if range_start > range_end {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "range start exceeds range end",
        ));
    }
    if range_start > 0 {
        file.seek(SeekFrom::Start(range_start)).await?;
    }
    let bounded = file.take(range_end - range_start);
    let cap = chunk_size.max(4096);
    let mut stream = ReaderStream::with_capacity(bounded, cap);
    let mut out = Vec::new();
    while let Some(chunk_res) = stream.next().await {
        out.push(chunk_res?);
    }
    Ok(out)
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
            let meta = file.metadata()?;
            let total_len = meta.len();
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
        .iter()
        .map(|c| PyBytes::new(py, c).into_any())
        .collect();
    PyList::new(py, &py_chunks)
}
