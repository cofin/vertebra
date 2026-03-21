use criterion::{black_box, criterion_group, criterion_main, Criterion};
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

use vertebra_core::guards::{validate_jwt, RateLimiter};

#[derive(Debug, Serialize)]
struct Claims {
    sub: String,
    exp: usize,
    iss: String,
    aud: String,
}

fn bench_rate_limiter(c: &mut Criterion) {
    let limiter = RateLimiter::new(10.0, 10.0);
    c.bench_function("rate_limiter_allow", |b| {
        b.iter(|| {
            let allowed = limiter.allow(black_box("user"));
            black_box(allowed);
        })
    });
}

fn bench_validate_jwt(c: &mut Criterion) {
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

    c.bench_function("jwt_validate_hs256", |b| {
        b.iter(|| {
            let result = validate_jwt(
                black_box(&token),
                black_box(secret),
                Some("issuer"),
                Some("audience"),
            );
            let _ = black_box(result);
        })
    });
}

criterion_group!(guards, bench_rate_limiter, bench_validate_jwt);
criterion_main!(guards);
