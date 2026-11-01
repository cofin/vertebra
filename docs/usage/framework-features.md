# Framework Features

Beyond routing, `vertebra.framework` provides scoped dependency injection, security guards, OpenAPI and AsyncAPI schema generation, background task queues, real-time `LiveState` sync, pub/sub channels, response caching, email delivery, and OpenTelemetry instrumentation.

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

## Background Task Queues, Backends & Cron

`vertebra.framework.queues` provides background task execution (`QueueService`), retry policies (`TaskRetryPolicy` with exponential backoff and dead-letter queues), delayed scheduling (`enqueue_delayed`), and cron jobs (`CronScheduler` with distributed leader leases):

| Backend | Scheme / Class | Backing Mechanism |
| --- | --- | --- |
| **Memory** | `memory://` (`MemoryQueueBackend`) | In-process queue for local dev and unit tests |
| **PostgreSQL** | `postgresql://` (`PostgresQueueBackend`) | `FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY` wakeups |
| **Redis** | `redis://` (`RedisQueueBackend`) | Redis Streams consumer groups (`XREADGROUP` / `XACK`) |
| **Valkey** | `valkey://` (`ValkeyQueueBackend`) | Valkey Streams + sorted-set delayed scheduling |
| **Oracle** | `oracle://` (`OracleQueueBackend`) | `FOR UPDATE SKIP LOCKED` transactional queue tables |

```python
from vertebra.framework.queues import (
    CronSchedule,
    CronScheduler,
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
scheduler = CronScheduler(
    queue,
    schedules=[
        CronSchedule(
            name="hourly-sync",
            cron="0 * * * *",
            task_name="emails.welcome",
        )
    ],
)
```

Failed tasks that exhaust `max_retries` move to the dead-letter queue and can be inspected with `await queue.list_dlq()` or re-queued with `await queue.replay_dlq(task_id)`.

## Real-Time `LiveState` & Pub/Sub Channels

Synchronize server state with browser clients over WebSocket binary delta frames using `LiveState` and `@live_action` (`vertebra.framework.live`), or broadcast topic events across workers with `ChannelBus` (`vertebra.framework.channels`):

```python
from vertebra.framework.channels import ChannelBus
from vertebra.framework.live import LiveState, LiveStateRoom, live_action


class CounterState(LiveState):
    count: int = 0

    @live_action
    def increment(self, step: int = 1) -> None:
        self.count += step


room = LiveStateRoom("room:lobby", CounterState())
bus = ChannelBus()
```

## Key-Value Stores & Transactional Email

Cache serialized responses or key-value state in `KeyValueStore` (`NamespacedStore`), and send transactional emails through a unified `EmailService` supporting 6 backends (`MemoryEmailBackend`, `SmtpBackend`, `ResendBackend`, `SendGridBackend`, `MailgunBackend`, `SesBackend`):

```python
from vertebra.framework.email import EmailConfig, EmailMessage, EmailService
from vertebra.framework.stores import KeyValueStore

cache = KeyValueStore()
email_service = EmailService.from_config(
    EmailConfig(backend="memory", default_from="noreply@example.com")
)
await email_service.send(
    EmailMessage(
        to=["user@example.com"],
        subject="Welcome",
        text_body="Hello from Vertebra",
    )
)
```

## Telemetry & Structured Logging (`vertebra.telemetry`)

Configure structured JSON logging (`LoggingConfig`, Rust `LogSink` / `structlog` integration), W3C trace propagation (`TraceContext`, `SpanExporter`), GenAI token/cost metrics (`GenAiRecorder`), and database pool metrics (`DbPoolRecorder`, `SqlTracePropagator`):

```python
from vertebra.telemetry import (
    GenAiRecorder,
    GenAiUsage,
    LoggingConfig,
    TraceContext,
)

log_config = LoggingConfig(level="INFO", json_format=True)
trace = TraceContext.generate()
genai_metrics = GenAiRecorder()
genai_metrics.record(
    GenAiUsage(model="gemini-2.5-flash", input_tokens=120, output_tokens=45)
)
```
