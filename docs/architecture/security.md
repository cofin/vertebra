# Security & User Framework

Vertebra implements a comprehensive security and user management framework designed for high performance and hybrid runtime support (standard ASGI and Vertebra Native).

## Architecture

The security architecture follows a **Broker-Centric** model:

1.  **Ingress Validation**: The Vertebra Broker (Bun/Rust) intercepts all incoming requests and validates authentication credentials (JWT, OAuth2, IAP) *before* the request reaches the Python worker.
2.  **Claim Propagation**: Validated claims (User ID, scopes, etc.) are passed to the Python worker via high-performance IPC (Shared Memory).
3.  **Lazy Hydration**: The Python worker hydrates a `User` object lazily. Basic claims are available immediately; database lookup happens only when accessing other attributes.

## Authentication

### Configuration

Authentication is configured in `vertebra.config.ts`:

```typescript
export default {
  auth: {
    cookieName: "access_token",
    refreshTokenCookieName: "refresh_token", // Enable rotation
    providers: [
      {
        type: "jwt",
        secret: "your-secret-key",
        refreshSecret: "refresh-secret", // For rotation
        issuer: "your-issuer",
        audience: "your-audience",
      },
      // ... other providers (IAP, OAuth2)
    ]
  }
}
```

### Refresh Token Rotation

Vertebra supports automatic Refresh Token rotation at the Broker layer for Symmetric JWTs.

- If an Access Token is expired but a valid Refresh Token is present, the Broker automatically issues new Access and Refresh tokens.
- New tokens are injected into the response via `Set-Cookie` headers.
- The Python application receives the validated user claims transparently, as if the access token were valid.

## User Model

The `VertebraUserMixin` provides standard authentication properties and permission checks. It works with `msgspec.Struct`, `pydantic`, or SQLAlchemy models.

```python
from vertebra.security.auth.models import VertebraUserMixin
import msgspec

class User(VertebraUserMixin, msgspec.Struct):
    id: str
    username: str
    scopes: list[str] = []
    permissions: list[str] = []

# Usage
user.is_authenticated  # True
user.has_scope("admin") # Checks 'scopes' list
user.has_perm("read")   # Checks 'permissions' list
```

### Anonymous User

If no authentication is present, an `AnonymousUser` instance is attached to the request.
- `is_authenticated` returns `False`.
- `has_scope` / `has_perm` return `False`.

## Native Integration

Vertebra Native handlers (running in the optimized path) support authentication via Dependency Injection and Request context.

### Dependency Injection

Use the `provide_user` dependency to access the current user in Native handlers:

```python
from litestar import get
from litestar.di import Provide
from vertebra.security.auth.dependencies import provide_user
from vertebra.security.auth.models import VertebraUserMixin

@get("/", dependencies={"current_user": Provide(provide_user)})
async def handler(current_user: VertebraUserMixin) -> dict:
    return {"user": current_user.id}
```

### Request Context

The `VertebraRequest` object (used in Native mode) exposes the user directly:

```python
async def handler(request: Request) -> dict:
    # request is VertebraRequest in native mode
    user = request.user
    if user.is_authenticated:
        return {"user": user.id}
    return {"error": "Unauthorized"}
```

## IPC Security Utilities

The `vertebra.security.SecurityClient` provides access to high-performance cryptographic operations implemented in the Broker (Rust/Bun), avoiding Python overhead for compute-intensive tasks.

- `hash_password(password: str) -> str`: Bcrypt hashing.
- `verify_password(password: str, hash: str) -> bool`: Bcrypt verification.
- `generate_random(length: int) -> bytes`: CSPRNG.
- `csrf_sign(data: str, secret: str) -> str`: HMAC-SHA256 signing.
