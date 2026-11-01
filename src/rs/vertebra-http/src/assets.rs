//! Asset serving, Vite manifest resolution, pre-compressed sidecars (`.zst`/`.br`/`.gz`), `ETag`/`304`, and HTTP `Range` (`206`).

use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use http::{HeaderMap, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Bytes;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Configuration for static asset serving, pre-compression, caching, and range requests.
#[derive(Debug, Clone)]
pub struct StaticAssetServeConfig {
    /// Serve pre-compressed `.zst`, `.br`, and `.gz` sidecar files when present.
    pub precompressed: bool,
    /// Emit `ETag` headers and honor `If-None-Match` with `304 Not Modified`.
    pub etag: bool,
    /// Support HTTP `Range: bytes=start-end` (`206 Partial Content`).
    pub range_requests: bool,
    /// Max-age in seconds for immutable fingerprinted assets (default: 31536000).
    pub immutable_max_age_secs: u64,
    /// Max-age in seconds for non-fingerprinted assets (default: 0 / must-revalidate).
    pub default_max_age_secs: u64,
    /// Fall back to `index.html` when a requested sub-path does not exist (default: true).
    pub spa_fallback: bool,
}

impl Default for StaticAssetServeConfig {
    fn default() -> Self {
        Self {
            precompressed: true,
            etag: true,
            range_requests: true,
            immutable_max_age_secs: 31_536_000,
            default_max_age_secs: 0,
            spa_fallback: true,
        }
    }
}

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

/// Parsed Vite manifest mapping source paths to build outputs.
pub type Manifest = HashMap<String, ManifestEntry>;

/// Manages static-file serving and Vite manifest lookup.
pub struct AssetManager {
    static_dir: PathBuf,
    dev_proxy_url: Option<String>,
    manifest: Option<Manifest>,
    config: StaticAssetServeConfig,
}

impl AssetManager {
    /// Create a new `AssetManager` with default static asset serving configuration.
    pub fn new(static_dir: Option<&str>, dev_proxy_url: Option<String>) -> Self {
        Self::with_config(static_dir, dev_proxy_url, StaticAssetServeConfig::default())
    }

    /// Create a new `AssetManager` with explicit `StaticAssetServeConfig`.
    pub fn with_config(
        static_dir: Option<&str>,
        dev_proxy_url: Option<String>,
        config: StaticAssetServeConfig,
    ) -> Self {
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
            config,
        }
    }

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

    /// Serve a static file without request headers (convenience wrapper around `serve_static_request`).
    pub async fn serve_static(&self, path: &str) -> Option<Response<Full<Bytes>>> {
        let empty = HeaderMap::new();
        self.serve_static_request(path, &empty).await
    }

    /// Serve a static file honoring `Accept-Encoding` (`.zst`/`.br`/`.gz`), `If-None-Match` (`304`), and `Range` (`206`).
    pub async fn serve_static_request(
        &self,
        path: &str,
        req_headers: &HeaderMap,
    ) -> Option<Response<Full<Bytes>>> {
        let clean = path.trim_start_matches('/');
        if clean.contains("..") {
            return None;
        }
        if is_protected_build_metadata(clean) {
            return Some(
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            );
        }
        let is_index = clean.is_empty() || clean == "index.html";

        let file_path = if clean.is_empty() {
            self.static_dir.join("index.html")
        } else {
            self.static_dir.join(clean)
        };

        let effective = if file_path.exists() && file_path.is_file() {
            file_path
        } else if self.config.spa_fallback {
            let index = self.static_dir.join("index.html");
            if index.exists() {
                index
            } else {
                return None;
            }
        } else {
            return None;
        };

        let meta = tokio::fs::metadata(&effective).await.ok()?;
        let total_len = meta.len();
        let mtime_secs = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let range_header = req_headers.get("range").and_then(|v| v.to_str().ok());
        let accept_enc = req_headers
            .get("accept-encoding")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let (read_target, content_encoding) = if self.config.precompressed && range_header.is_none()
        {
            resolve_precompressed_sidecar(&effective, accept_enc)
        } else {
            (effective.clone(), None)
        };

        let etag_value = match content_encoding {
            Some(enc) => format!("W/\"{:x}-{:x}-{enc}\"", total_len, mtime_secs),
            None => format!("W/\"{:x}-{:x}\"", total_len, mtime_secs),
        };

        let cache_control = if is_fingerprinted_filename(&effective) {
            format!(
                "public, max-age={}, immutable",
                self.config.immutable_max_age_secs
            )
        } else if self.config.default_max_age_secs > 0 {
            format!("public, max-age={}", self.config.default_max_age_secs)
        } else {
            "public, max-age=0, must-revalidate".to_string()
        };

        if self.config.etag
            && let Some(inm) = req_headers
                .get("if-none-match")
                .and_then(|v| v.to_str().ok())
            && (inm.trim() == "*" || inm.split(',').any(|tag| tag.trim() == etag_value))
        {
            let mut builder = Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("etag", &etag_value)
                .header("cache-control", &cache_control);
            if self.config.precompressed {
                builder = builder.header("vary", "Accept-Encoding");
            }
            return Some(builder.body(Full::new(Bytes::new())).unwrap());
        }

        let mime = mime_for_path(&effective);

        if self.config.range_requests
            && let Some(raw_range) = range_header
        {
            match parse_byte_range(raw_range, total_len) {
                Some((start, end_inclusive)) => {
                    let take_len = (end_inclusive - start + 1) as usize;
                    let mut fh = tokio::fs::File::open(&effective).await.ok()?;
                    fh.seek(SeekFrom::Start(start)).await.ok()?;
                    let mut buf = vec![0u8; take_len];
                    fh.read_exact(&mut buf).await.ok()?;
                    let mut builder = Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header("content-type", mime)
                        .header("accept-ranges", "bytes")
                        .header(
                            "content-range",
                            format!("bytes {start}-{end_inclusive}/{total_len}"),
                        )
                        .header("content-length", take_len.to_string())
                        .header("cache-control", &cache_control);
                    if self.config.etag {
                        builder = builder.header("etag", &etag_value);
                    }
                    return Some(builder.body(Full::new(Bytes::from(buf))).unwrap());
                }
                None => {
                    return Some(
                        Response::builder()
                            .status(StatusCode::RANGE_NOT_SATISFIABLE)
                            .header("content-range", format!("bytes */{total_len}"))
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    );
                }
            }
        }

        let content = tokio::fs::read(&read_target).await.ok()?;

        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", mime)
            .header("accept-ranges", "bytes")
            .header("cache-control", &cache_control)
            .header("vary", "Accept-Encoding");

        if self.config.etag {
            builder = builder.header("etag", &etag_value);
        }
        if let Some(enc) = content_encoding {
            builder = builder.header("content-encoding", enc);
        }

        if (is_index || effective.ends_with("index.html"))
            && let Some(manifest) = &self.manifest
            && let Some(entry) = manifest.get("index.html")
        {
            builder = builder.header("Link", format!("</{}>; rel=preload; as=script", entry.file));
            let mut visited: HashSet<&str> = HashSet::new();
            visited.insert("index.html");
            let mut css_seen: HashSet<&str> = HashSet::new();
            let mut preload_seen: HashSet<&str> = HashSet::new();
            if let Some(css) = &entry.css {
                for c in css {
                    if css_seen.insert(c.as_str()) {
                        builder =
                            builder.header("Link", format!("</{}>; rel=preload; as=style", c));
                    }
                }
            }
            if let Some(imports) = &entry.imports {
                let mut stack: Vec<&str> = imports.iter().map(String::as_str).collect();
                while let Some(key) = stack.pop() {
                    if !visited.insert(key) {
                        continue;
                    }
                    if let Some(chunk) = manifest.get(key) {
                        if preload_seen.insert(chunk.file.as_str()) {
                            builder = builder.header(
                                "Link",
                                format!("</{}>; rel=preload; as=script", chunk.file),
                            );
                        }
                        if let Some(css) = &chunk.css {
                            for c in css {
                                if css_seen.insert(c.as_str()) {
                                    builder = builder
                                        .header("Link", format!("</{}>; rel=preload; as=style", c));
                                }
                            }
                        }
                        if let Some(sub_imports) = &chunk.imports {
                            for sub in sub_imports {
                                stack.push(sub.as_str());
                            }
                        }
                    }
                }
            }
        }

        Some(builder.body(Full::new(Bytes::from(content))).unwrap())
    }
}

fn is_protected_build_metadata(clean_path: &str) -> bool {
    const PROTECTED: &[&str] = &[
        "manifest.json",
        ".vite/manifest.json",
        "ssr-manifest.json",
        ".vite/ssr-manifest.json",
        "hot",
        ".vertebra/importmap.json",
        ".litestar.json",
    ];
    for p in PROTECTED {
        if clean_path == *p || clean_path.ends_with(&format!("/{p}")) {
            return true;
        }
    }
    false
}

fn accepts_encoding(accept_encoding: &str, target: &str) -> bool {
    for raw_item in accept_encoding.split(',') {
        let mut segments = raw_item.trim().split(';');
        let Some(token) = segments.next() else {
            continue;
        };
        if !token.trim().eq_ignore_ascii_case(target) {
            continue;
        }
        let mut allowed = true;
        for param in segments {
            let trimmed = param.trim();
            if let Some(q_str) = trimmed
                .strip_prefix("q=")
                .or_else(|| trimmed.strip_prefix("Q="))
                && let Ok(q) = q_str.trim().parse::<f32>()
                && q <= 0.0
            {
                allowed = false;
            }
        }
        if allowed {
            return true;
        }
    }
    false
}

fn resolve_precompressed_sidecar(
    base_path: &Path,
    accept_encoding: &str,
) -> (PathBuf, Option<&'static str>) {
    let base_os = base_path.as_os_str();
    for (token, ext, encoding) in [
        ("zstd", ".zst", "zstd"),
        ("br", ".br", "br"),
        ("gzip", ".gz", "gzip"),
    ] {
        if accepts_encoding(accept_encoding, token) {
            let mut candidate_os = base_os.to_os_string();
            candidate_os.push(ext);
            let candidate = PathBuf::from(candidate_os);
            if candidate.is_file() {
                return (candidate, Some(encoding));
            }
        }
    }
    (base_path.to_path_buf(), None)
}

fn is_fingerprinted_filename(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    let parts: Vec<&str> = name.split(['.', '-']).collect();
    if parts.len() < 3 {
        return false;
    }
    for segment in &parts[1..parts.len() - 1] {
        if segment.len() >= 6
            && segment.len() <= 16
            && segment.chars().all(|c| c.is_ascii_alphanumeric())
            && segment.chars().any(|c| c.is_ascii_digit())
        {
            return true;
        }
    }
    false
}

fn parse_byte_range(header_val: &str, total_len: u64) -> Option<(u64, u64)> {
    if total_len == 0 {
        return None;
    }
    let spec = header_val.trim().strip_prefix("bytes=")?;
    let first_range = spec.split(',').next()?.trim();
    let (start_str, end_str) = first_range.split_once('-')?;
    if start_str.is_empty() {
        let suffix_len = end_str.parse::<u64>().ok()?;
        if suffix_len == 0 {
            return None;
        }
        let start = total_len.saturating_sub(suffix_len);
        return Some((start, total_len - 1));
    }
    let start = start_str.parse::<u64>().ok()?;
    if start >= total_len {
        return None;
    }
    let end = if end_str.is_empty() {
        total_len - 1
    } else {
        end_str.parse::<u64>().ok()?.min(total_len - 1)
    };
    if start > end {
        return None;
    }
    Some((start, end))
}

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
        Some("mp4") => "video/mp4",
        Some("wasm") => "application/wasm",
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

        let resp = mgr.serve_static("/").await;
        assert!(resp.is_some());

        let resp = mgr.serve_static("/app/dashboard").await;
        assert!(resp.is_some());
    }

    #[tokio::test]
    async fn test_serve_missing_returns_none() {
        let dir = tempdir().unwrap();
        let mgr = AssetManager::new(Some(dir.path().to_str().unwrap()), None);
        let resp = mgr.serve_static("/missing.txt").await;
        assert!(resp.is_none());
    }
}
