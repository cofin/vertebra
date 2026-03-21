//! Asset serving and Vite manifest resolution for Vertebra.
//!
//! In production, serves static files from the build output directory with
//! `Link` preload headers derived from the Vite manifest. In development,
//! tracks the dev-proxy URL so the Python layer can redirect asset requests.

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A single entry from the Vite manifest (`manifest.json`).
#[derive(Deserialize, Debug, Clone)]
pub struct ManifestEntry {
    pub file: String,
    pub src: Option<String>,
    #[serde(rename = "isEntry")]
    pub is_entry: Option<bool>,
    pub imports: Option<Vec<String>>,
    pub css: Option<Vec<String>>,
}

/// Parsed Vite manifest — maps source paths to build outputs.
pub type Manifest = HashMap<String, ManifestEntry>;

/// Manages static-file serving and Vite manifest lookup.
///
/// Two modes:
/// - **Dev proxy**: `dev_proxy_url` is `Some(url)` — asset requests should be
///   forwarded to the Vite dev server by the caller.
/// - **Static**: reads files from `static_dir` and resolves entries through
///   the Vite manifest for `Link` preload headers.
pub struct AssetManager {
    static_dir: PathBuf,
    dev_proxy_url: Option<String>,
    manifest: Option<Manifest>,
}

impl AssetManager {
    /// Create a new `AssetManager`.
    ///
    /// When `dev_proxy_url` is `None` and `static_dir` exists, the Vite
    /// manifest is loaded eagerly.
    pub fn new(static_dir: Option<&str>, dev_proxy_url: Option<String>) -> Self {
        let static_path = static_dir.map(PathBuf::from).unwrap_or_default();
        let manifest = if dev_proxy_url.is_none() && static_path.exists() {
            Self::load_manifest(&static_path)
        } else {
            None
        };
        Self {
            static_dir: static_path,
            dev_proxy_url,
            manifest,
        }
    }

    /// Try to load the Vite manifest from the static directory.
    ///
    /// Checks Vite 5+ (`.vite/manifest.json`) first, then Vite 4
    /// (`manifest.json`).
    fn load_manifest(static_dir: &Path) -> Option<Manifest> {
        let v5_path = static_dir.join(".vite/manifest.json");
        let v4_path = static_dir.join("manifest.json");
        let path = if v5_path.exists() { v5_path } else { v4_path };
        std::fs::read_to_string(path)
            .ok()
            .and_then(|content| serde_json::from_str(&content).ok())
    }

    /// Resolve a source entry to its hashed output file path.
    pub fn resolve_entry(&self, entry: &str) -> Option<String> {
        self.manifest.as_ref()?.get(entry).map(|e| e.file.clone())
    }

    /// Whether the manager is configured for dev-proxy mode.
    pub fn is_dev_proxy(&self) -> bool {
        self.dev_proxy_url.is_some()
    }

    /// The dev-proxy base URL, if configured.
    pub fn dev_proxy_url(&self) -> Option<&str> {
        self.dev_proxy_url.as_deref()
    }

    /// Whether the static directory exists on disk.
    pub fn has_static_dir(&self) -> bool {
        self.static_dir.exists()
    }

    /// Serve a static file, returning `None` if the path cannot be resolved.
    ///
    /// For the root path or `index.html`, `Link` preload headers are injected
    /// from the Vite manifest when available. Non-existent paths fall back to
    /// `index.html` for SPA routing.
    pub async fn serve_static(&self, path: &str) -> Option<Response<Full<Bytes>>> {
        let clean = path.trim_start_matches('/');
        let is_index = clean.is_empty() || clean == "index.html";

        let file_path = if clean.is_empty() {
            self.static_dir.join("index.html")
        } else {
            self.static_dir.join(clean)
        };

        // Try the exact path first, then SPA fallback to index.html.
        let effective = if file_path.exists() && file_path.is_file() {
            file_path
        } else {
            let index = self.static_dir.join("index.html");
            if index.exists() {
                index
            } else {
                return None;
            }
        };

        let content = tokio::fs::read(&effective).await.ok()?;
        let mime = mime_for_path(&effective);

        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", mime);

        // Inject Link preload headers for the index page.
        if is_index || effective.ends_with("index.html") {
            if let Some(manifest) = &self.manifest {
                if let Some(entry) = manifest.get("index.html") {
                    builder = builder
                        .header("Link", format!("</{}>; rel=preload; as=script", entry.file));
                    if let Some(css) = &entry.css {
                        for c in css {
                            builder =
                                builder.header("Link", format!("</{}>; rel=preload; as=style", c));
                        }
                    }
                    if let Some(imports) = &entry.imports {
                        for key in imports {
                            if let Some(chunk) = manifest.get(key) {
                                builder = builder.header(
                                    "Link",
                                    format!("</{}>; rel=preload; as=script", chunk.file),
                                );
                            }
                        }
                    }
                }
            }
        }

        Some(builder.body(Full::new(Bytes::from(content))).unwrap())
    }
}

/// Map file extension to MIME type.
fn mime_for_path(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html",
        Some("js" | "mjs") => "application/javascript",
        Some("css") => "text/css",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("svg") => "image/svg+xml",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("ico") => "image/x-icon",
        Some("map") => "application/json",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_manifest_loading_v4() {
        let dir = tempdir().unwrap();
        let json = r#"{
            "src/main.ts": {
                "file": "assets/main.abc123.js",
                "isEntry": true,
                "css": ["assets/main.def456.css"]
            }
        }"#;
        fs::write(dir.path().join("manifest.json"), json).unwrap();

        let mgr = AssetManager::new(Some(dir.path().to_str().unwrap()), None);
        assert!(mgr.manifest.is_some());
        assert_eq!(
            mgr.resolve_entry("src/main.ts"),
            Some("assets/main.abc123.js".to_string())
        );
    }

    #[test]
    fn test_manifest_loading_v5() {
        let dir = tempdir().unwrap();
        let vite_dir = dir.path().join(".vite");
        fs::create_dir(&vite_dir).unwrap();
        let json = r#"{
            "index.html": {
                "file": "assets/index.hash.js",
                "isEntry": true
            }
        }"#;
        fs::write(vite_dir.join("manifest.json"), json).unwrap();

        let mgr = AssetManager::new(Some(dir.path().to_str().unwrap()), None);
        assert!(mgr.manifest.is_some());
        assert_eq!(
            mgr.resolve_entry("index.html"),
            Some("assets/index.hash.js".to_string())
        );
    }

    #[test]
    fn test_dev_proxy_mode() {
        let mgr = AssetManager::new(None, Some("http://localhost:5173".into()));
        assert!(mgr.is_dev_proxy());
        assert_eq!(mgr.dev_proxy_url(), Some("http://localhost:5173"));
        assert!(mgr.manifest.is_none());
    }

    #[test]
    fn test_no_manifest_when_dir_missing() {
        let mgr = AssetManager::new(Some("/tmp/nonexistent_vertebra_test"), None);
        assert!(mgr.manifest.is_none());
        assert!(!mgr.has_static_dir());
    }

    #[test]
    fn test_mime_types() {
        assert_eq!(mime_for_path(Path::new("app.js")), "application/javascript");
        assert_eq!(mime_for_path(Path::new("style.css")), "text/css");
        assert_eq!(mime_for_path(Path::new("index.html")), "text/html");
        assert_eq!(mime_for_path(Path::new("image.png")), "image/png");
        assert_eq!(mime_for_path(Path::new("font.woff2")), "font/woff2");
        assert_eq!(
            mime_for_path(Path::new("data.bin")),
            "application/octet-stream"
        );
    }

    #[tokio::test]
    async fn test_serve_static_file() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("hello.txt"), "hello world").unwrap();

        let mgr = AssetManager::new(Some(dir.path().to_str().unwrap()), None);
        let resp = mgr.serve_static("/hello.txt").await;
        assert!(resp.is_some());
        let resp = resp.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_serve_index_fallback() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("index.html"), "<html></html>").unwrap();

        let mgr = AssetManager::new(Some(dir.path().to_str().unwrap()), None);

        // Root path should serve index.html
        let resp = mgr.serve_static("/").await;
        assert!(resp.is_some());

        // Non-existent path should fall back to index.html (SPA)
        let resp = mgr.serve_static("/app/dashboard").await;
        assert!(resp.is_some());
    }

    #[tokio::test]
    async fn test_serve_missing_returns_none() {
        let dir = tempdir().unwrap();
        // No index.html, no matching file
        let mgr = AssetManager::new(Some(dir.path().to_str().unwrap()), None);
        let resp = mgr.serve_static("/missing.txt").await;
        assert!(resp.is_none());
    }
}
