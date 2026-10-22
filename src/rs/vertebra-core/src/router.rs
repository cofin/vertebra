use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterError {
    InvalidParam { segment: String },
    InvalidParamType { segment: String, param_type: String },
}

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RouterError::InvalidParam { segment } => {
                write!(f, "invalid typed param segment: {segment}")
            }
            RouterError::InvalidParamType {
                segment,
                param_type,
            } => write!(
                f,
                "invalid typed param type '{param_type}' in segment: {segment}"
            ),
        }
    }
}

impl std::error::Error for RouterError {}

pub fn normalize_typed_path(path: &str) -> Result<String, RouterError> {
    if !path.contains('{') && !path.contains(':') && !path.contains('*') {
        return Ok(path.to_string());
    }

    let mut segments = Vec::new();
    for segment in path.split('/') {
        if let Some(normalized) = normalize_typed_segment(segment)? {
            segments.push(normalized);
        } else {
            segments.push(segment.to_string());
        }
    }

    Ok(segments.join("/"))
}

fn normalize_typed_segment(segment: &str) -> Result<Option<String>, RouterError> {
    if let Some(name) = segment.strip_prefix(':') {
        let name = name.trim();
        if name.is_empty() {
            return Err(RouterError::InvalidParam {
                segment: segment.to_string(),
            });
        }
        return Ok(Some(format!("{{{name}}}")));
    }
    if let Some(name) = segment.strip_prefix('*') {
        let name = name.trim();
        if name.is_empty() {
            return Err(RouterError::InvalidParam {
                segment: segment.to_string(),
            });
        }
        return Ok(Some(format!("{{*{name}}}")));
    }
    if !(segment.starts_with('{') && segment.ends_with('}')) {
        return Ok(None);
    }

    let inner = &segment[1..segment.len() - 1];
    let mut parts = inner.splitn(2, ':');
    let name = parts.next().unwrap_or("").trim();
    if name.is_empty() {
        return Err(RouterError::InvalidParam {
            segment: segment.to_string(),
        });
    }

    if let Some(param_type) = parts.next() {
        let param_type = param_type.trim();
        if param_type == "path" {
            return Ok(Some(format!("{{*{name}}}")));
        }
        if !param_type.is_empty() && !is_supported_type(param_type) {
            return Err(RouterError::InvalidParamType {
                segment: segment.to_string(),
                param_type: param_type.to_string(),
            });
        }
    }

    Ok(Some(format!("{{{name}}}")))
}

fn is_supported_type(param_type: &str) -> bool {
    matches!(
        param_type,
        "str"
            | "string"
            | "int"
            | "i64"
            | "u64"
            | "float"
            | "f64"
            | "bool"
            | "uuid"
            | "decimal"
            | "date"
            | "datetime"
            | "time"
            | "timedelta"
            | "path"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_typed_path() {
        assert_eq!(
            normalize_typed_path("/user/{id:int}").unwrap(),
            "/user/{id}"
        );
        assert_eq!(
            normalize_typed_path("/static/{path:path}").unwrap(),
            "/static/{*path}"
        );
        assert_eq!(
            normalize_typed_path("/simple/path").unwrap(),
            "/simple/path"
        );
    }
}
