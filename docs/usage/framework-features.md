# Dependency Injection, Security & OpenAPI

`vertebra.framework` provides scoped dependency injection (`Provide`, `Inject`, `Container`), declarative route guards and rate limiting (`SecurityConfig`, `Guard`), and automatic OpenAPI 3.1 / Scalar UI and AsyncAPI 3.0 schema builders. See [Queues, LiveState, Email & Telemetry](queues-and-realtime.md) for background tasks, real-time state, stores, and observability.

## Dependency Injection (`Provide`, `Inject` & `Container`)

Register dependencies at any layer (`App`, `Router`, `Controller`, `Route`, or `Procedure`) using `Provide`, plain functions, classes, or generator functions (`yield`), and inject them by parameter name, type annotation, or `Inject`:

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

Dependencies support `singleton`, `request`, and `transient` scopes, automatic generator cleanup after each request (`RequestScope`), and hierarchical overrides from `App` -> `Router` -> `Controller` -> `Route`.

## Security, Guards & Rate Limiting

Protect routes at the `App`, `Router`, `Controller`, or `@route` level using `Guard` callables, `Principal` claims (`requires_role`, `requires_scope`, `requires_tenant`, `requires_capability`), `CsrfConfig` (`CsrfMiddleware`), and `RateLimitConfig` (`RateLimiter`):

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

When a guard raises `PermissionDeniedError` or returns `False`, the dispatcher short-circuits the request with `403 Forbidden` before running the handler.

## OpenAPI 3.1, Scalar UI & AsyncAPI 3.0

Enable automatic `/openapi.json` and interactive Scalar documentation on any `App` with `app.enable_docs()`, or build OpenAPI 3.1 and AsyncAPI 3.0 specs directly via `OpenAPIConfig`, `ScalarDocsConfig`, and `AsyncAPISpecBuilder`:

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
