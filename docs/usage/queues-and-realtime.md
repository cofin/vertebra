# Queues, LiveState, Email & Telemetry

Vertebra includes distributed task queues, cron scheduling, WebSocket delta state sync, pub/sub channels, key-value stores, transactional email, and OpenTelemetry instrumentation.

## Background Task Queues & Cron

`QueueService` runs background tasks with per-attempt dependency injection, lease heartbeats, exponential-backoff retries, dead-letter queues, delayed jobs, and sync or async producers. Pair it with `CronScheduler` for distributed cron schedules backed by leader leases.

| Backend | URL Scheme | Backing Mechanism | Optional Extra |
| --- | --- | --- | --- |
| **Memory** | `memory://` | In-process queue for local dev and unit tests | None |
| **PostgreSQL** | `postgresql://`, `postgres://` | Row locking (`SKIP LOCKED`) with `LISTEN/NOTIFY` wakeups | `vertebra-core[postgres]` |
| **Oracle** | `oracle://` | Row locking (`SKIP LOCKED`) with TxEventQ / AQ wakeups | `vertebra-core[oracle]` |
| **Redis** | `redis://`, `rediss://`, `unix://` | Redis Streams consumer groups, sorted-set delays, Pub/Sub wakeups | `vertebra-core[redis]` |
| **Valkey** | `valkey://`, `valkeys://`, `unix://` | Valkey Streams consumer groups, sorted-set delays, Pub/Sub wakeups | `vertebra-core[valkey]` |
| **Spanner** | `spanner://`, `spangres://` | Hash-sharded tables, read-write claims, and dead-letter table | `vertebra-core[spanner]` |

Database backends build their queue store automatically from the configured driver or URL, raising `MissingDependencyError` when the matching extra is not installed. Redis and Valkey backends support both async and sync producers.

!!! note "Spanner wakeups and TTL cleanup"
    Spanner lacks a push notification channel, so workers wake via adaptive polling (`poll_interval_seconds`), which shortens automatically when delayed jobs are due soon. Expired rows are reclaimed asynchronously by Spanner's background TTL compaction; set `cleanup_on_ack=True` to delete acknowledged rows right away.

Install the extra for your chosen backend:

```bash
uv add "vertebra-core[postgres]"
uv add "vertebra-core[oracle]"
uv add "vertebra-core[redis]"
uv add "vertebra-core[valkey]"
uv add "vertebra-core[spanner]"
```

```python
from vertebra.framework.queues import (
    CronSchedule,
    CronScheduler,
    QueueBackendSettings,
    QueueService,
    TaskRetryPolicy,
    task,
)

queue = QueueService(QueueBackendSettings(kind="memory"))


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

Run workers standalone with `vtb workers run app:app`, or alongside HTTP workers with `vtb run app:app --with-workers --queues default,io`.

## LiveState & Pub/Sub Channels

Run the [live counter example](../examples/index.md#live-state) from the repository root.

```bash
vtb run docs.examples.live_state_and_ssr:app
```

Open [http://127.0.0.1:8000/live](http://127.0.0.1:8000/live). The page uses an
`EventSource` to receive the initial counter value and three updates. Each
subscription creates its own `LiveState` and `LiveStateRoom`. The handler calls
an `@live_action` method to change the counter and uses `broadcast_pending()`
to drain the room. The HTTP stream sends a JSON snapshot for each change and a
`complete` event that closes the browser subscription.

For applications that send binary deltas, the room encodes changes as MessagePack.
Pass an async `publish_fn(channel, payload)` to `LiveStateRoom` to forward each
encoded frame to your transport. The transport owns connections and delivery.
You can adapt the callback to `bus.publish(payload, [channel])` on a
`create_channels_backend()` bus. The [TypeScript SDK guide](typescript-sdk.md#live-state)
shows how to deliver decoded frames to the UI.

Inertia supplies page props on navigation. Live updates arrive through the
application's event stream or another transport.

## Key-Value Stores & Email

Store cached responses or application state in memory, Redis, or Valkey, and send transactional email through `EmailService` over memory, SMTP, Resend, SendGrid, Mailgun, or SES transports.

`ValkeyStore` and `RedisStore` enable client-side L1 cache invalidation by default and prefix keys with `key_prefix` (default `"vertebra"`). Calling `delete_all()` scans and unlinks only keys under that prefix. Expiries use millisecond precision, and `expires_in()` returns `None` for keys without an expiry.

```python
from vertebra.framework.email import EmailConfig, EmailMessage, EmailService
from vertebra.framework.stores import KeyValueStore, ValkeyStore

cache = KeyValueStore(key_prefix="app-cache")
valkey_cache = ValkeyStore.from_url(
    "valkey://localhost:6379/0",
    key_prefix="app-cache",
)
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

## Telemetry & Structured Logging

Configure JSON logging, W3C trace propagation, GenAI token and cost metrics, and database pool telemetry:

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
