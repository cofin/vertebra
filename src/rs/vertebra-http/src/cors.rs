//! Zero-GIL CORS preflight and response header engine offloaded from Litestar `CORSConfig`.

use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CorsOffloadConfig {
    pub allow_origins: Vec<String>,
    pub allow_methods: Vec<String>,
    pub allow_headers: Vec<String>,
    pub allow_credentials: bool,
    pub expose_headers: Vec<String>,
    pub max_age: u32,
    pub allow_origin_regex: Option<String>,
}

/// Startup-compiled CORS offload engine avoiding per-request regex compilation.
#[derive(Debug, Clone)]
pub struct CompiledCorsOffload {
    config: CorsOffloadConfig,
    origin_regex: Option<Regex>,
}

impl CompiledCorsOffload {
    pub fn new(config: CorsOffloadConfig) -> Self {
        let origin_regex = config
            .allow_origin_regex
            .as_deref()
            .and_then(|pat| Regex::new(&format!("^(?:{pat})$")).ok());
        Self {
            config,
            origin_regex,
        }
    }

    pub fn is_origin_allowed(&self, origin: &str) -> bool {
        if self
            .config
            .allow_origins
            .iter()
            .any(|o| o == "*" || o == origin)
        {
            return true;
        }
        self.origin_regex
            .as_ref()
            .is_some_and(|re| re.is_match(origin))
    }

    pub fn evaluate_preflight(
        &self,
        origin: &str,
        request_method: Option<&str>,
        request_headers: Option<&str>,
    ) -> (StatusCode, HeaderMap) {
        self.config.evaluate_preflight_with_check(
            origin,
            self.is_origin_allowed(origin),
            request_method,
            request_headers,
        )
    }

    pub fn apply_response_headers(&self, origin: &str, headers: &mut HeaderMap) {
        self.config.apply_response_headers_with_check(
            origin,
            self.is_origin_allowed(origin),
            headers,
        );
    }
}

impl CorsOffloadConfig {
    pub fn compile(self) -> CompiledCorsOffload {
        CompiledCorsOffload::new(self)
    }

    pub fn is_origin_allowed(&self, origin: &str) -> bool {
        if self.allow_origins.iter().any(|o| o == "*" || o == origin) {
            return true;
        }
        self.allow_origin_regex
            .as_deref()
            .and_then(|pat| Regex::new(&format!("^(?:{pat})$")).ok())
            .is_some_and(|re| re.is_match(origin))
    }

    pub fn resolve_allow_origin(&self, origin: &str) -> Option<String> {
        self.resolve_allow_origin_checked(origin, self.is_origin_allowed(origin))
    }

    fn resolve_allow_origin_checked(&self, origin: &str, allowed: bool) -> Option<String> {
        if !allowed {
            return None;
        }
        if !self.allow_credentials && self.allow_origins.iter().any(|o| o == "*") {
            Some("*".to_string())
        } else {
            Some(origin.to_string())
        }
    }

    pub fn evaluate_preflight(
        &self,
        origin: &str,
        request_method: Option<&str>,
        request_headers: Option<&str>,
    ) -> (StatusCode, HeaderMap) {
        self.evaluate_preflight_with_check(
            origin,
            self.is_origin_allowed(origin),
            request_method,
            request_headers,
        )
    }

    fn evaluate_preflight_with_check(
        &self,
        origin: &str,
        origin_allowed: bool,
        request_method: Option<&str>,
        request_headers: Option<&str>,
    ) -> (StatusCode, HeaderMap) {
        let mut headers = HeaderMap::new();
        let Some(allowed_origin) = self.resolve_allow_origin_checked(origin, origin_allowed) else {
            return (StatusCode::BAD_REQUEST, headers);
        };
        if let Some(req_method) = request_method
            && !self
                .allow_methods
                .iter()
                .any(|m| m == "*" || m.eq_ignore_ascii_case(req_method))
        {
            return (StatusCode::BAD_REQUEST, headers);
        }
        if let Ok(val) = HeaderValue::from_str(&allowed_origin) {
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, val);
        }
        if self.allow_credentials {
            headers.insert(
                header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
        let methods_str = if self.allow_methods.iter().any(|m| m == "*") {
            request_method
                .unwrap_or("GET, POST, PUT, PATCH, DELETE, OPTIONS")
                .to_string()
        } else {
            self.allow_methods.join(", ")
        };
        if let Ok(val) = HeaderValue::from_str(&methods_str) {
            headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, val);
        }
        let headers_str = if self.allow_headers.iter().any(|h| h == "*") {
            request_headers.unwrap_or("*").to_string()
        } else {
            self.allow_headers.join(", ")
        };
        if !headers_str.is_empty()
            && let Ok(val) = HeaderValue::from_str(&headers_str)
        {
            headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, val);
        }
        if let Ok(val) = HeaderValue::from_str(&self.max_age.to_string()) {
            headers.insert(header::ACCESS_CONTROL_MAX_AGE, val);
        }
        headers.insert(header::VARY, HeaderValue::from_static("Origin"));
        (StatusCode::NO_CONTENT, headers)
    }

    pub fn apply_response_headers(&self, origin: &str, headers: &mut HeaderMap) {
        self.apply_response_headers_with_check(origin, self.is_origin_allowed(origin), headers);
    }

    fn apply_response_headers_with_check(
        &self,
        origin: &str,
        origin_allowed: bool,
        headers: &mut HeaderMap,
    ) {
        let Some(allowed_origin) = self.resolve_allow_origin_checked(origin, origin_allowed) else {
            return;
        };
        if let Ok(val) = HeaderValue::from_str(&allowed_origin) {
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, val);
        }
        if self.allow_credentials {
            headers.insert(
                header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
        if !self.expose_headers.is_empty()
            && let Ok(val) = HeaderValue::from_str(&self.expose_headers.join(", "))
        {
            let expose_name = HeaderName::from_static("access-control-expose-headers");
            headers.insert(expose_name, val);
        }
        if allowed_origin != "*" {
            headers.insert(header::VARY, HeaderValue::from_static("Origin"));
        }
    }
}
