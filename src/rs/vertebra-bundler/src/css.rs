//! CSS minification for vertebra-bundler

use pyo3::prelude::*;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minify_css_strips_comments_and_whitespace() {
        let css = r#"
            /* header styling */
            .header {
                color: red;
                margin: 0px 10px;
            }
        "#;
        let min = minify_css(css);
        assert_eq!(min, ".header{color:red;margin:0px 10px;}");
    }
}
