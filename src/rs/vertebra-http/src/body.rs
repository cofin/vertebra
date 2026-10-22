//! HTTP body types and frame stream adapters.
//!
//! Provides `FrameReceiver` for progressive chunk and trailer streaming,
//! and `ResponseBody` as the unified dual-path response body enum.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::{Either, Full};
use hyper::Response;
use hyper::body::{Body, Frame};
use tokio::sync::mpsc;

/// Progressive multi-chunk and trailer body stream wrapping an `mpsc::Receiver`.
pub struct FrameReceiver {
    rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
}

impl FrameReceiver {
    /// Wrap a bounded Tokio `mpsc::Receiver` as an HTTP body.
    #[inline]
    pub fn new(rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>) -> Self {
        Self { rx }
    }

    /// Consume the wrapper and return the underlying channel receiver.
    #[inline]
    pub fn into_inner(self) -> mpsc::Receiver<Result<Frame<Bytes>, Infallible>> {
        self.rx
    }
}

impl std::fmt::Debug for FrameReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameReceiver").finish_non_exhaustive()
    }
}

impl Body for FrameReceiver {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.rx.poll_recv(cx)
    }
}

/// Dual-path HTTP response body: `Left(Full<Bytes>)` for zero-channel unary responses,
/// `Right(FrameReceiver)` for chunked, SSE, and trailer-bearing responses.
pub type ResponseBody = Either<Full<Bytes>, FrameReceiver>;

/// Canonical HTTP response type used across `vertebra-http`.
pub type HttpResponse = Response<ResponseBody>;

/// Wrap a contiguous byte buffer in the zero-allocation unary `Either::Left(Full<Bytes>)` branch.
#[inline]
pub fn full_body(bytes: impl Into<Bytes>) -> ResponseBody {
    Either::Left(Full::new(bytes.into()))
}

/// Wrap a frame receiver in the progressive streaming `Either::Right(FrameReceiver)` branch.
#[inline]
pub fn stream_body(rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>) -> ResponseBody {
    Either::Right(FrameReceiver::new(rx))
}

/// A completed or streaming response returned from a worker.
#[derive(Debug)]
pub struct PendingResponse {
    pub status: u16,
    pub headers_mp: bytes::Bytes,
    pub body: bytes::Bytes,
    pub stream_rx: Option<FrameReceiver>,
}

impl PendingResponse {
    /// Construct a unary in-memory response.
    pub fn unary(status: u16, headers_mp: bytes::Bytes, body: bytes::Bytes) -> Self {
        Self {
            status,
            headers_mp,
            body,
            stream_rx: None,
        }
    }

    /// Construct a progressive streaming response.
    pub fn streaming(
        status: u16,
        headers_mp: bytes::Bytes,
        rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
    ) -> Self {
        Self {
            status,
            headers_mp,
            body: Bytes::new(),
            stream_rx: Some(FrameReceiver::new(rx)),
        }
    }
}
