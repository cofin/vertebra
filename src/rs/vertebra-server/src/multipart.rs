use crate::ServerState;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Request, Response};
use multer::Multipart;
use std::sync::Arc;

pub async fn handle_multipart(
    req: Request<hyper::body::Incoming>,
    _state: Arc<ServerState>,
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    let boundary = match req
        .headers()
        .get("content-type")
        .and_then(|ct| ct.to_str().ok())
        .and_then(|ct| multer::parse_boundary(ct).ok())
    {
        Some(b) => b,
        None => {
            return Ok(Response::builder()
                .status(400)
                .body(Full::new(Bytes::from("Missing boundary")))?)
        }
    };

    // Adapt Incoming to Stream
    let stream = req.into_body().into_data_stream();

    let mut multipart = Multipart::new(stream, boundary);

    let mut uploaded_files = Vec::new();

    while let Some(mut field) = multipart.next_field().await? {
        if let Some(filename) = field.file_name() {
            let filename = filename.to_owned();
            uploaded_files.push(filename.clone());

            // Mock streaming to disk
            while let Some(_chunk) = field.chunk().await? {
                // In production: write to temp file
            }
        }
    }

    Ok(Response::new(Full::new(Bytes::from(format!(
        "Uploaded: {:?}",
        uploaded_files
    )))))
}
