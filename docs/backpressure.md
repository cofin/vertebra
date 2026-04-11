# Backpressure & Admission Control

Vertebra ships a `BackpressureController` in `vertebra-http::backpressure`
that the HTTP ingress layer can use to shed load with explicit 503 / 429
responses instead of letting request queues grow unboundedly. The
controller is opt-in: attach it to a `Dispatcher` with
`Dispatcher::with_backpressure(config)`. When attached, the HTTP ingress
must call `controller.try_acquire().await` before `dispatcher.dispatch()`
and hold the returned permit for the lifetime of the request.

The controller is not wired into Vertebra's existing server binary by
default — it is available as a library for applications that want to
bolt it on, and the default server path will adopt it incrementally in a
follow-up. Keeping it library-only ships the logic and tests without
perturbing the current hot path.

## Rejection reasons

| Reason | HTTP status | When |
|---|---|---|
| `Saturated` | 503 | Semaphore out of permits **and** waiter queue is at `max_queue_depth`. Fail fast. |
| `QueueTimeout` | 503 | Waiter blocked longer than `max_queue_time` waiting for a permit. |
| `CircuitOpen` | 429 | Circuit breaker tripped after `min_samples` recent responses showed a failure rate ≥ `failure_rate_threshold`. |

## Circuit breaker lifecycle

```
Closed ──(failure rate ≥ threshold)──► Open
   ▲                                     │
   │                                     │ (window elapsed)
   │                                     ▼
   └──(N consecutive probes succeed)── HalfOpen
                                         │
                                         ├──(success)──► Closed
                                         └──(failure)──► Open
```

- `Closed`: normal operation. Failures accumulate in a sliding window
  (`window`) and contribute to the failure-rate calculation once the
  sample count reaches `min_samples`.
- `Open`: every `try_acquire` returns `CircuitOpen` → 429 with no
  dispatch. Time spent open = `window`.
- `HalfOpen`: the next `half_open_probe_count` successful responses
  close the circuit; a single failure while half-open reopens it.

## Configuration

All knobs are read from environment variables in `ServerConfig::from_env`:

| Env var | Default | Description |
|---|---:|---|
| `VERTEBRA_BP_MAX_IN_FLIGHT` | 1024 | Maximum concurrent in-flight permits. |
| `VERTEBRA_BP_MAX_QUEUE_DEPTH` | 1024 | Waiters allowed before new requests are shed with 503. |
| `VERTEBRA_BP_MAX_QUEUE_TIME_MS` | 100 | Maximum time a waiter can block before 503. |
| `VERTEBRA_BP_CB_FAILURE_RATE` | 0.5 | Failure fraction (0.0–1.0) that trips the circuit. |
| `VERTEBRA_BP_CB_MIN_SAMPLES` | 20 | Minimum samples in the window before the circuit can trip. |
| `VERTEBRA_BP_CB_WINDOW_MS` | 10000 | Sliding-window size for sample decay and time-to-half-open. |
| `VERTEBRA_BP_CB_HALF_OPEN_PROBES` | 5 | Consecutive successful probes required to fully close the circuit. |

Defaults are set so no rejections occur under normal load. Tune
`VERTEBRA_BP_MAX_IN_FLIGHT` to `worker_count × 64` for a reasonable
starting point on production workloads.

## Metrics snapshot

`BackpressureMetricsSnapshot` is a plain struct with five counters plus
live values:

```rust
pub struct BackpressureMetricsSnapshot {
    pub accepted: u64,
    pub saturated_rejected: u64,
    pub queue_rejected: u64,
    pub circuit_rejected: u64,
    pub timeout_rejected: u64,
    pub in_flight: usize,
    pub queue_depth: usize,
    pub circuit_state: CircuitState,
}
```

The intended health-endpoint JSON is:

```json
{
  "backpressure": {
    "in_flight": 17,
    "queue_depth": 3,
    "circuit_state": "closed",
    "counters": {
      "accepted": 142000,
      "saturated_rejected": 4,
      "queue_rejected": 0,
      "circuit_rejected": 0,
      "timeout_rejected": 1
    }
  }
}
```

The health-endpoint wiring is a follow-up task.

## Recording results

The HTTP ingress layer must call `record_success()` or `record_failure()`
after each response so the circuit breaker sees real data:

```rust
let permit = controller.try_acquire().await?;
let response = handle(req).await;
if response.status().is_server_error() {
    controller.record_failure();
} else {
    controller.record_success();
}
drop(permit);
```

Dropping the permit early (e.g. before response body streaming completes)
is a trade-off: shorter-held permits improve throughput but risk
over-admitting if downstream is genuinely slow.

## Interaction with other signals

- **SHM ring capacity** in `ShmDispatcher::dispatch_shm` is an independent
  signal. The controller sits in front of dispatch; the ring is the last
  line of defense inside dispatch. Both can return 503 today.
- **Locality-aware routing** (`feedback_shm_locality_routing.md`): the
  controller does not distinguish in-process from cross-process. If that
  matters for a deployment, run separate controllers per routing pool.
- **Tiered response rings** (`feedback_tiered_response_rings.md`): the
  controller is upstream of response routing and does not influence the
  4-tier sizing.
