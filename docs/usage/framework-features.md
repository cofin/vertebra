# Framework Features

Beyond routing, `vertebra.framework` provides scoped dependency injection, declarative security guards, OpenAPI 3.1 schema generation, background task queues, and real-time `LiveState` synchronization.

## Dependency Injection (`Container` & `Provide`)

Register application-scoped singletons or request-scoped factories with `Container`, `DependencyGraph`, and `Provide`:

```python
from vertebra.framework.di import DependencyGraph, Provide


def provide_settings() -> dict[str, str]:
    return {"env": "production"}


graph = DependencyGraph(
    {"settings": Provide(dependency=provide_settings, scope="singleton")}
)
```

Use `Provide(dependency=...)` in route or router configurations to resolve dependencies automatically per singleton, request, or transient scope.

## Security, Guards & Rate Limiting

Protect routes with `Guard` callables, `Principal` claims (`requires_role`, `requires_scope`, `requires_capability`), `CsrfConfig`, and `RateLimitConfig` grouped under `SecurityConfig`:

```python
from vertebra.framework.security import (
    CsrfConfig,
    Principal,
    RateLimitConfig,
    SecurityConfig,
    requires_role,
)

security = SecurityConfig(
    csrf=CsrfConfig(secret="signing-secret-key"),
    rate_limit=RateLimitConfig(requests_per_window=100, window_seconds=60),
)
```

## OpenAPI 3.1 & Scalar Documentation UI

Configure automatic OpenAPI 3.1 schema generation and interactive Scalar API documentation via `OpenAPIConfig` and `ScalarDocsConfig`:

```python
from vertebra.framework.openapi import OpenAPIConfig, ScalarDocsConfig

openapi_config = OpenAPIConfig(
    title="Orders API",
    version="1.0.0",
    scalar=ScalarDocsConfig(path="/docs"),
)
```

## Background Task Queues & Cron Scheduling

`vertebra.framework.queues` provides an in-memory and SQL/Redis-backed task queue (`QueueService`), retry policies (`TaskRetryPolicy` with exponential backoff and dead-letter queues), and cron scheduling (`CronScheduler`, `CronSchedule`):

```python
from vertebra.framework.queues import (
    QueueConfig,
    QueueService,
    TaskRetryPolicy,
    task,
)

queue = QueueService(QueueConfig(backend_url="memory://"))


@task(
    name="emails.welcome",
    lane="io",
    retry_policy=TaskRetryPolicy(max_retries=3),
)
async def send_welcome_email(user_id: int) -> str:
    return f"sent:{user_id}"


queue.register(send_welcome_email)
```

## Real-Time `LiveState`

Synchronize reactive server state with browser clients over WebSocket binary delta frames (`StateDeltaFrame`, `HybridLogicalClock`, and FNV-1a field hashing) using `vertebra.framework.live.LiveState` and `@vertebra/client/live`.
