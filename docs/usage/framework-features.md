# Dependency Injection, Security & OpenAPI

Vertebra includes scoped dependency injection, declarative route guards, CSRF protection, rate limiting, and OpenAPI and AsyncAPI schema builders. See [Queues, LiveState, Email & Telemetry](queues-and-realtime.md) for background tasks, real-time state, stores, and observability.

## Dependency Injection

Register dependencies on an app, router, controller, route, or procedure using `Provide`. Providers can be plain functions, classes, or generators. Inject them by parameter name, type annotation, or `Inject`:

```python
from collections.abc import Iterator
from typing import Annotated

from vertebra import App, Inject, Provide


class DatabaseSession:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn


def provide_session() -> Iterator[DatabaseSession]:
    session = DatabaseSession("sqlite:///:memory:")
    try:
        yield session
    finally:
        pass


app = App(
    dependencies={
        "db": Provide(provide_session, scope="request"),
    }
)


@app.get("/db-status")
def db_status(session: Annotated[DatabaseSession, Inject("db")]) -> dict[str, str]:
    return {"dsn": session.dsn}
```

Dependencies support `singleton`, `request`, and `transient` scopes. Generator providers clean up automatically after each request, and inner layers override outer ones from app down to individual routes.

## Security, Guards & Rate Limiting

Attach route guards, CSRF protection, and rate limits at the app, router, controller, or route level:

```python
from vertebra import App
from vertebra.framework.security import (
    CsrfConfig,
    Principal,
    RateLimitConfig,
    SecurityConfig,
    requires_role,
    requires_scope,
)

security = SecurityConfig(
    csrf=CsrfConfig(secret="signing-secret-key"),
    rate_limit=RateLimitConfig(requests_per_window=100, window_seconds=60),
)

app = App()


@app.get(
    "/admin/audit",
    guards=[requires_role("admin"), requires_scope("audit:read")],
)
def read_audit_log() -> dict[str, str]:
    return {"status": "authorized"}
```

If a guard raises `PermissionDeniedError` or returns `False`, the request fails with `403 Forbidden` before the handler runs.

## OpenAPI, Scalar UI & AsyncAPI

Call `app.enable_docs()` to serve `/openapi.json` and an interactive Scalar UI, or build OpenAPI 3.1 and AsyncAPI 3.0 schemas directly:

```python
from vertebra import App
from vertebra.framework.asyncapi import AsyncAPIConfig, AsyncAPISpecBuilder
from vertebra.framework.openapi import OpenAPIConfig, ScalarDocsConfig

app = App()
app.enable_docs(title="Orders API", version="1.0.0", docs_path="/docs")

openapi_config = OpenAPIConfig(
    title="Orders API",
    version="1.0.0",
    scalar=ScalarDocsConfig(path="/docs"),
)
asyncapi_builder = AsyncAPISpecBuilder(
    AsyncAPIConfig(title="Orders Events", version="1.0.0")
)
```
