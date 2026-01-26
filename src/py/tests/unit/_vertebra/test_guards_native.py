import time

import jwt

from vertebra._vertebra import RateLimiter, validate_jwt


def test_rate_limiter_allows_and_blocks() -> None:
    limiter = RateLimiter(2.0, 0.0)
    assert limiter.allow("user")
    assert limiter.allow("user")
    assert not limiter.allow("user")


def test_validate_jwt_hs256() -> None:
    secret = "secret"
    token = jwt.encode(
        {"sub": "user", "iss": "issuer", "aud": "audience", "exp": int(time.time()) + 300}, secret, algorithm="HS256"
    )

    assert validate_jwt(token, secret, "issuer", "audience")
    assert not validate_jwt(token, "bad", "issuer", "audience")
