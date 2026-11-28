# Dependency Injection, Security & OpenAPI

Vertebra includes scoped dependency injection, declarative route guards, CSRF protection, rate limiting, and OpenAPI and AsyncAPI schema builders. See [Queues, LiveState, Email & Telemetry](queues-and-realtime.md) for background tasks, real-time state, stores, and observability.

## Dependency Injection

Register dependencies on an app, router, controller, route, or procedure using `Provide`. Providers can be plain functions, classes, or generators. Inject them by parameter name, type annotation, or `Inject`:

```python
from typing import Annotated

from vertebra import App, Inject, Provide, get


class ServiceSettings:
    def __init__(self, region: str) -> None:
        self.region = region


def provide_settings() -> ServiceSettings:
    return ServiceSettings(region="us-east")


@get("/status")
async def status(
    settings: Annotated[ServiceSettings, Inject("settings")],
) -> dict[str, str]:
    return {"region": settings.region}


app = App(
    route_handlers=[status],
    dependencies={"settings": Provide(provide_settings, scope="singleton")},
)
```

Dependencies support `singleton`, `request`, and `transient` scopes.
Request-scoped generators can yield a resource and release it when the request
ends. A provider declared on an inner layer overrides a provider with the same
key declared on an outer layer.

## Security, guards, and rate limiting

Use role and scope guards with a verified principal. The inventory application
shows a request-scoped provider and the behavior of authenticated and
unauthenticated requests. [Guarded inventory app](../examples/index.md#controllers-routers-guards)

Apply CSRF checks to state-changing requests that use browser cookies.
The example below checks a request using the parsed cookies supplied by its
authentication layer. A rate limiter uses the authenticated account ID as the
bucket key.

```python
from collections.abc import Mapping
from os import environ

from vertebra.framework.security import CsrfConfig, CsrfMiddleware, RateLimitGuard

csrf = CsrfMiddleware(CsrfConfig(secret=environ["APP_CSRF_SECRET"]))
rate_limit = RateLimitGuard(capacity=20, refill_rate_per_sec=2)


def check_write_request(
    method: str,
    headers: Mapping[str, str],
    cookies: Mapping[str, str],
    account_id: str,
) -> int:
    if not csrf.verify_request(method, headers, cookies):
        return 403
    if not rate_limit.allow(account_id):
        return 429
    return 200
```

Call this check from a write handler or its middleware before applying the
change. Issue a token with `csrf.generate_token()` when opening the browser
session, then send it in the configured cookie and request header.

## OpenAPI, Scalar UI & AsyncAPI

Call `app.enable_docs()` to serve `/openapi.json` and an interactive Scalar UI, or build OpenAPI 3.1 and AsyncAPI 3.0 schemas directly:

```python
from vertebra import App, get
from vertebra.framework.asyncapi import AsyncAPIConfig, AsyncAPISpecBuilder
from vertebra.framework.openapi import OpenAPIConfig, ScalarDocsConfig


@get("/orders")
async def list_orders() -> list[dict[str, int]]:
    return [{"id": 101}]


app = App(route_handlers=[list_orders])
openapi_config = OpenAPIConfig(
    title="Orders API",
    version="1.0.0",
    scalar=ScalarDocsConfig(path="/docs"),
)
app.enable_docs(openapi_config)

asyncapi_builder = AsyncAPISpecBuilder(
    AsyncAPIConfig(title="Orders Events", version="1.0.0")
)
```
