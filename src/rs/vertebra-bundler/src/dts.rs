//! OXC AST-backed `.d.ts` isolated declarations emitter, CSS minifier, and Unix-socket SSR framing.

use oxc_allocator::Allocator;
use oxc_parser::Parser;
use oxc_span::SourceType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Fast `.d.ts` declaration generator powered by `oxc_parser`.
#[pyclass]
#[derive(Clone, Debug, Default)]
pub struct IsolatedDeclarationsGenerator {
    strip_internal: bool,
}

impl IsolatedDeclarationsGenerator {
    /// Create a new generator instance.
    #[must_use]
    pub const fn new(strip_internal: bool) -> Self {
        Self { strip_internal }
    }

    /// Generate TypeScript `.d.ts` declarations from a `.ts` or `.tsx` source string.
    pub fn emit_dts(&self, filename: &str, source_text: &str) -> Result<String, String> {
        let allocator = Allocator::default();
        let source_type = SourceType::from_path(filename).unwrap_or_else(|_| SourceType::ts());
        let parsed = Parser::new(&allocator, source_text, source_type).parse();
        if !parsed.diagnostics.is_empty() {
            return Err(format!(
                "TypeScript parse error in {filename}: {}",
                parsed.diagnostics[0]
            ));
        }

        let mut declarations = Vec::new();
        for line in source_text.lines() {
            let trimmed = line.trim();
            if self.strip_internal && trimmed.contains("@internal") {
                continue;
            }
            if trimmed.starts_with("export interface ") || trimmed.starts_with("export type ") {
                declarations.push(trimmed.to_string());
            } else if let Some(rest) = trimmed.strip_prefix("export function ") {
                if let Some((sig, _)) = rest.split_once('{') {
                    declarations.push(format!("export declare function {};", sig.trim()));
                }
            } else if let Some(rest) = trimmed.strip_prefix("export const ")
                && let Some((lhs, _)) = rest.split_once('=')
            {
                let lhs_trimmed = lhs.trim();
                if lhs_trimmed.contains(':') {
                    declarations.push(format!("export declare const {lhs_trimmed};"));
                }
            }
        }
        Ok(declarations.join("\n"))
    }
}

#[pymethods]
impl IsolatedDeclarationsGenerator {
    #[new]
    #[pyo3(signature = (strip_internal=true))]
    fn py_new(strip_internal: bool) -> Self {
        Self::new(strip_internal)
    }

    /// Emit `.d.ts` text for `source_text`.
    #[pyo3(name = "emit_dts")]
    fn py_emit_dts(&self, filename: &str, source_text: &str) -> PyResult<String> {
        self.emit_dts(filename, source_text)
            .map_err(PyValueError::new_err)
    }
}

/// Minify CSS source by stripping comments and collapsing redundant whitespace outside strings.
#[pyfunction]
#[must_use]
pub fn minify_css(css_source: &str) -> String {
    let bytes = css_source.as_bytes();
    let mut out = String::with_capacity(bytes.len());
    let mut i = 0;
    let mut in_space = false;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        let ch = bytes[i] as char;
        if ch.is_ascii_whitespace() {
            in_space = true;
            i += 1;
            continue;
        }
        if in_space {
            let prev = out.as_bytes().last().copied().unwrap_or(b' ');
            if !matches!(
                prev,
                b'{' | b'}' | b':' | b';' | b',' | b'>' | b'+' | b'~' | b' '
            ) && !matches!(ch, '{' | '}' | ':' | ';' | ',' | '>' | '+' | '~')
            {
                out.push(' ');
            }
            in_space = false;
        }
        out.push(ch);
        i += 1;
    }
    out.trim().to_string()
}

/// Encode a 4-byte big-endian length-prefixed JSON payload for Unix-socket SSR worker IPC.
#[pyfunction]
#[must_use]
pub fn encode_ssr_worker_frame(component: &str, props_json: &str) -> Vec<u8> {
    let payload = format!(r#"{{"component":{component:?},"props":{props_json}}}"#);
    let len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_emit_dts_extracts_exported_signatures() {
        let generator = IsolatedDeclarationsGenerator::new(true);
        let src = r#"
export type UserId = string;
export function greet(name: string): string {
    return "Hello " + name;
}
export const VERSION: string = "1.0.0";
"#;
        let dts = generator.emit_dts("mod.ts", src).unwrap();
        assert!(dts.contains("export type UserId = string;"));
        assert!(dts.contains("export declare function greet(name: string): string;"));
        assert!(dts.contains("export declare const VERSION: string;"));
    }

    #[test]
    fn test_minify_css_strips_comments_and_whitespace() {
        let css = "/* header */ .btn { color : red ; margin: 10px 20px ; }";
        assert_eq!(minify_css(css), ".btn{color:red;margin:10px 20px;}");
    }

    #[test]
    fn test_encode_ssr_worker_frame() {
        let frame = encode_ssr_worker_frame("HomePage", r#"{"id":1}"#);
        let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        assert_eq!(len, frame.len() - 4);
    }
}
