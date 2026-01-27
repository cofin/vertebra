use std::path::PathBuf;
use tower_http::services::ServeDir;
use reqwest::Client;
use hyper::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Bytes;

pub struct AssetManager {
    static_dir: PathBuf,
    dev_proxy_url: Option<String>,
    client: Client,
}

impl AssetManager {
    pub fn new(static_dir: &str, dev_proxy_url: Option<String>) -> Self {
        Self {
            static_dir: PathBuf::from(static_dir),
            dev_proxy_url,
            client: Client::new(),
        }
    }

    pub async fn serve(&self, req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(proxy_url) = &self.dev_proxy_url {
            // Proxy to dev server
            let path = req.uri().path();
            let url = format!("{}{}", proxy_url, path);
            
            // Simple proxy logic (simplified for prototype)
            // In real impl, we'd stream the body and copy headers carefully
            match self.client.get(&url).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    let status_code = StatusCode::from_u16(status.as_u16())?;
                    let bytes = resp.bytes().await?;
                    let mut builder = Response::builder().status(status_code);
                    // Copy basic headers? For now just body
                    Ok(builder.body(Full::new(bytes))?)
                }
                Err(_) => {
                    Ok(Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .body(Full::new(Bytes::from("Dev server unavailable")))?)
                }
            }
        } else {
            // Serve static files
            // Tower ServeDir expects a Request, but returns a specialized Response
            // We need to adapt it to our Response type or use it directly
            // For now, placeholder static serving logic
            
            let path = self.static_dir.join(req.uri().path().trim_start_matches('/'));
            if path.exists() && path.is_file() {
                let content = tokio::fs::read(path).await?;
                Ok(Response::new(Full::new(Bytes::from(content))))
            } else {
                Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::from("Not Found")))?)
            }
        }
    }
}
