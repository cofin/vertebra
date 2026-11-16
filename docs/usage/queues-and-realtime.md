# Queues, LiveState, Email & Telemetry

`vertebra.framework` includes distributed background task queues (`QueueService`, `CronScheduler`), WebSocket delta state synchronization (`LiveState`, `ChannelBus`), key-value stores (`KeyValueStore`), transactional email (`EmailService`), and OpenTelemetry instrumentation (`vertebra.telemetry`).

## Background Task Queues, Backends & Cron (`vertebra.framework.queues`)

`QueueService` provides background task execution, retry policies (`TaskRetryPolicy` with exponential backoff and dead-letter queues), delayed scheduling (`enqueue_delayed`), and cron jobs (`CronScheduler` with distributed leader leases):

| Backend | Scheme / Class | Backing Mechanism | Requires |
| --- | --- | --- | --- |
| **Memory** | `memory://` (`MemoryQueueBackend`) | In-process queue for local dev and unit tests | Nothing |
| **PostgreSQL** | `postgresql://`, `postgres://` (`PostgreSQLQueueBackend`) | `FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY` wakeups | `executor=` (`PostgreSQLQueueExecutor`); `vertebra-core[postgres]` |
| **Redis** | `redis://`, `rediss://`, `unix://` (`RedisQueueBackend`) | Redis Streams consumer groups (`XREADGROUP` / `XACK`) + sorted-set delayed scheduling | `vertebra-core[redis]` native driver, or `client=` (`RedisStreamClient`) |
| **Valkey** | `valkey://`, `valkeys://`, `unix://` (`ValkeyQueueBackend`) | Valkey Streams consumer groups (`XREADGROUP` / `XACK`) + sorted-set delayed scheduling | `vertebra-core[valkey]` native driver, or `client=` (`ValkeyStreamClient`) |
| **Oracle** | `oracle://` (`OracleQueueBackend`) | `FOR UPDATE SKIP LOCKED` transactional queue tables | `executor=` (`OracleQueueExecutor`); `oracledb` |
| **Cloud Spanner** | `spanner://`, `spangres://` (`SpannerQueueBackend`) | Hash-sharded keys + `FOR UPDATE` claims + dead-letter table | `executor=` (`SpannerQueueExecutor`); `vertebra-core[spanner]` |

Backends that need an executor raise `MissingDependencyError` when their package is missing and `ImproperConfigurationError` when it is installed but no `executor=` is passed. Vertebra does not ship these executors, so build the backend and pass it in: `QueueService(config, backend=PostgreSQLQueueBackend(config, executor=executor))`.

`RedisQueueBackend` raises `MissingDependencyError` without `vertebra-core[redis]` and `ImproperConfigurationError` when `backend_url` has no host or unsupported scheme, unless `client=` is passed. `ValkeyQueueBackend` raises `MissingDependencyError` without `vertebra-core[valkey]` and `ImproperConfigurationError` when `backend_url` has no host or unsupported scheme, unless `client=` is passed. Install the optional client package for your selected queue backend (`vertebra-core[postgres]`, `vertebra-core[redis]`, `vertebra-core[valkey]`, `oracledb`, or `vertebra-core[spanner]`):

=== "uv"

    ```bash
    uv add "vertebra-core[postgres]"
    uv add "vertebra-core[redis]"
    uv add "vertebra-core[valkey]"
    uv add oracledb
    uv add "vertebra-core[spanner]"
    ```

=== "pip"

    ```bash
    pip install "vertebra-core[postgres]"
    pip install "vertebra-core[redis]"
    pip install "vertebra-core[valkey]"
    pip install oracledb
    pip install "vertebra-core[spanner]"
    ```

=== "pdm"

    ```bash
    pdm add "vertebra-core[postgres]"
    pdm add "vertebra-core[redis]"
    pdm add "vertebra-core[valkey]"
    pdm add oracledb
    pdm add "vertebra-core[spanner]"
    ```

=== "poetry"

    ```bash
    poetry add "vertebra-core[postgres]"
    poetry add "vertebra-core[redis]"
    poetry add "vertebra-core[valkey]"
    poetry add oracledb
    poetry add "vertebra-core[spanner]"
    ```

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
    cron=CronSchedule(expression="0 * * * *"),
)
async def send_welcome_email(user_id: int = 0) -> str:
    return f"sent:{user_id}"


queue.register(send_welcome_email)
scheduler = CronScheduler(queue)
```

Run background workers standalone (`vtb workers run app:app`) or co-located with HTTP workers (`vtb run app:app --with-workers --queues default,io`).

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

Cache serialized responses or key-value state in `KeyValueStore` (`NamespacedStore`), and send transactional emails through `EmailService` across 6 transports (`MemoryEmailTransport`, `SmtpEmailTransport`, `ResendEmailTransport`, `SendGridEmailTransport`, `MailgunEmailTransport`, `SesEmailTransport`):

Stores backed by Redis or Valkey prefix every key with `key_prefix` (default `"vertebra"`), and `delete_all()` removes only keys under that prefix. Expiries are sent in milliseconds, and `expires_in()` returns `None` for keys without an expiry.

```python
from vertebra.framework.email import EmailConfig, EmailMessage, EmailService
from vertebra.framework.stores import KeyValueStore

cache = KeyValueStore(key_prefix="app-cache")
email_service = EmailService(
    EmailConfig(backend="memory", default_from="noreply@example.com")
)
await email_service.send(
    EmailMessage(
        to=("user@example.com",),
        subject="Welcome",
        text_body="Hello from Vertebra",
    )
)
```

## Telemetry & Structured Logging (`vertebra.telemetry`)

Configure structured JSON logging (`LoggingConfig`), W3C trace propagation (`TraceContext`, `SpanExporter`), GenAI token/cost metrics (`GenAiRecorder`), and database pool metrics (`DbPoolRecorder`, `SqlTracePropagator`):

```python
from vertebra.telemetry import (
    GenAiRecorder,
    LoggingConfig,
    TraceContext,
)

log_config = LoggingConfig(level="INFO", json_format=True)
trace = TraceContext.generate()
genai_metrics = GenAiRecorder()
genai_metrics.record_generation(
    model="gemini-2.5-flash",
    input_tokens=120,
    output_tokens=45,
    duration_seconds=0.12,
)
```
