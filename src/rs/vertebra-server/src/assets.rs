use std::path::PathBuf;
use tower_http::services::ServeDir;
use reqwest::Client;
use hyper::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Bytes;
use std::collections::HashMap;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct ManifestEntry {
    pub file: String,
    pub src: Option<String>,
    #[serde(rename = "isEntry")]
    pub is_entry: Option<bool>,
    pub imports: Option<Vec<String>>,
    pub css: Option<Vec<String>>,
}

pub type Manifest = HashMap<String, ManifestEntry>;

pub struct AssetManager {
    static_dir: PathBuf,
    dev_proxy_url: Option<String>,
    client: Client,
    manifest: Option<Manifest>,
}

impl AssetManager {
    pub fn new(static_dir: &str, dev_proxy_url: Option<String>) -> Self {
        let static_path = PathBuf::from(static_dir);
        let manifest = if dev_proxy_url.is_none() {
            Self::load_manifest(&static_path)
        } else {
            None
        };

        Self {
            static_dir: static_path,
            dev_proxy_url,
            client: Client::new(),
            manifest,
        }
    }

    fn load_manifest(static_dir: &PathBuf) -> Option<Manifest> {
        let p1 = static_dir.join(".vite/manifest.json"); // Vite 5+ location
        let p2 = static_dir.join("manifest.json"); // Vite 4- location
        
        let path = if p1.exists() { p1 } else { p2 };

        if let Ok(content) = std::fs::read_to_string(path) {
             return serde_json::from_str(&content).ok();
        }
        None
    }

    pub fn resolve_entry(&self, entry: &str) -> Option<String> {
        self.manifest.as_ref()
            .and_then(|m| m.get(entry))
            .map(|e| e.file.clone())
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
            let path_str = req.uri().path().trim_start_matches('/');
            let path = self.static_dir.join(path_str);
            
            // Handle root as index.html
            let (is_index, effective_path) = if path_str == "" || path_str == "index.html" {
                (true, self.static_dir.join("index.html"))
            } else {
                (false, path)
            };

            if effective_path.exists() && effective_path.is_file() {
                let content = tokio::fs::read(&effective_path).await?;
                let mut builder = Response::builder().status(StatusCode::OK);

                // Early Hints / Link Headers for index.html
                if is_index {
                    if let Some(manifest) = &self.manifest {
                        // Try "index.html" as key
                        if let Some(entry) = manifest.get("index.html") {
                            // Preload Main JS
                            let link = format!("</{}>; rel=preload; as=script", entry.file);
                            builder = builder.header("Link", link);

                            // Preload CSS
                            if let Some(css_files) = &entry.css {
                                for css in css_files {
                                    let link = format!("</{}>; rel=preload; as=style", css);
                                    builder = builder.header("Link", link);
                                }
                            }
                            
                            // Preload Imports (Chunks)
                            if let Some(imports) = &entry.imports {
                                for import_key in imports {
                                    // Resolve chunk file from key
                                    if let Some(chunk) = manifest.get(import_key) {
                                        let link = format!("</{}>; rel=preload; as=script", chunk.file);
                                        builder = builder.header("Link", link);
                                    }
                                }
                            }
                        }
                    }
                }

                Ok(builder.body(Full::new(Bytes::from(content)))?)
            } else {
                Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::from("Not Found")))?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_manifest_loading() {
        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("manifest.json");
        let mut file = File::create(manifest_path).unwrap();
        let json = r#"{
            "src/main.ts": {
                "file": "assets/main.123456.js",
                "isEntry": true
            }
        }"#;
        file.write_all(json.as_bytes()).unwrap();

        let manager = AssetManager::new(dir.path().to_str().unwrap(), None);
        assert!(manager.manifest.is_some());
        assert_eq!(manager.resolve_entry("src/main.ts"), Some("assets/main.123456.js".to_string()));
    }
}

