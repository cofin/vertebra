use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GuardError {
    #[error("invalid token: {0}")]
    InvalidToken(String),
}

pub fn validate_jwt(
    token: &str,
    secret: &str,
    issuer: Option<&str>,
    audience: Option<&str>,
) -> Result<(), GuardError> {
    let mut validation = Validation::new(Algorithm::HS256);
    if let Some(issuer) = issuer {
        validation.set_issuer(&[issuer]);
    }
    if let Some(audience) = audience {
        validation.set_audience(&[audience]);
    }

    decode::<Value>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map(|_| ())
    .map_err(|err| GuardError::InvalidToken(err.to_string()))
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

#[derive(Debug)]
pub struct RateLimiter {
    max_tokens: f64,
    refill_per_second: f64,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl RateLimiter {
    pub fn new(max_tokens: f64, refill_per_second: f64) -> Self {
        Self {
            max_tokens: max_tokens.max(0.0),
            refill_per_second: refill_per_second.max(0.0),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    pub fn allow(&self, key: &str) -> bool {
        if self.max_tokens <= 0.0 {
            return false;
        }
        let now = Instant::now();
        let mut buckets = self
            .buckets
            .lock()
            .expect("rate limiter bucket lock poisoned");
        let bucket = buckets.entry(key.to_string()).or_insert(Bucket {
            tokens: self.max_tokens,
            last_refill: now,
        });

        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            let refill = elapsed * self.refill_per_second;
            bucket.tokens = (bucket.tokens + refill).min(self.max_tokens);
            bucket.last_refill = now;
        }

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Debug, Serialize)]
    struct Claims {
        sub: String,
        exp: usize,
        iss: String,
        aud: String,
    }

    #[test]
    fn jwt_validates_with_secret() {
        let secret = "secret";
        let claims = Claims {
            sub: "user".to_string(),
            exp: (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock set")
                .as_secs()
                + 300) as usize,
            iss: "issuer".to_string(),
            aud: "audience".to_string(),
        };
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("token encoded");

        let result = validate_jwt(&token, secret, Some("issuer"), Some("audience"));
        assert!(result.is_ok());
    }

    #[test]
    fn jwt_rejects_invalid_secret() {
        let secret = "secret";
        let claims = Claims {
            sub: "user".to_string(),
            exp: (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock set")
                .as_secs()
                + 300) as usize,
            iss: "issuer".to_string(),
            aud: "audience".to_string(),
        };
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("token encoded");

        let result = validate_jwt(&token, "wrong", Some("issuer"), Some("audience"));
        assert!(result.is_err());
    }

    #[test]
    fn rate_limiter_blocks_after_limit() {
        let limiter = RateLimiter::new(2.0, 0.0);
        assert!(limiter.allow("user"));
        assert!(limiter.allow("user"));
        assert!(!limiter.allow("user"));
    }

    #[test]
    fn rate_limiter_refills_tokens() {
        let limiter = RateLimiter::new(1.0, 1000.0);
        assert!(limiter.allow("user"));
        assert!(!limiter.allow("user"));
        std::thread::sleep(Duration::from_millis(5));
        assert!(limiter.allow("user"));
    }
}
