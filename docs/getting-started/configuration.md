# Configuration

Configure your Python app and the Rust HTTP server through CLI flags, config objects, or environment variables.

## Worker Modes

By default (`--topology auto`), Vertebra picks a worker mode from `--workers` and your Python build. Override it with `--topology` or `VERTEBRA_TOPOLOGY`:

| Mode | Auto-selected when | Override | How it runs |
| --- | --- | --- | --- |
| **Embedded** | Single worker or free-threaded Python | `--topology embedded` | Runs the HTTP server and Python in one process. |
| **Shared memory** | Multiple workers on standard Python (Linux/macOS) | `--topology shm` | Spawns separate Python worker processes connected over shared memory. |

## Server Environment Variables

`vtb run` and `vtb dev` pass these variables to the server and workers:

| Variable | Default | Description |
| --- | --- | --- |
| `VERTEBRA_APP` | - | Application import path (`module:attribute`). |
| `VERTEBRA_HOST` | `"127.0.0.1"` | Bind address for the HTTP listener. |
| `VERTEBRA_PORT` | `"8000"` | Port for HTTP/1.1, HTTP/2, and optional HTTP/3. |
| `VERTEBRA_TOPOLOGY` | `"auto"` | Worker mode: `auto`, `embedded`, or `shm`. |
| `VERTEBRA_WORKERS` | `"0"` (`embedded`) / `"1"` (`shm`) | Worker count (`0` runs in-process). |
| `VERTEBRA_THREADS` | CPU quota | Rust HTTP worker threads (respects container CPU limits). |
| `VERTEBRA_LOG_LEVEL` | `"info"` | Log level (`debug`, `info`, `warning`, `error`). |
| `PYTHON_JIT` | - | Set to `"1"` or `"0"` when `--jit` or `--no-jit` is passed to `vtb run`. |
| `VERTEBRA_STRICT_FREE_THREADING` | `"0"` | Set to `"1"` to fail startup if an imported C extension re-enables the GIL. |
| `VERTEBRA_TLS_CERT` | - | Path to the PEM TLS certificate chain. |
| `VERTEBRA_TLS_KEY` | - | Path to the PEM private key matching `VERTEBRA_TLS_CERT`. |
| `VERTEBRA_HTTP3` | `"false"` | Set to `"1"` or `"true"` to enable HTTP/3 over QUIC (requires TLS). |
| `VERTEBRA_HTTP3_ALT_SVC_MAX_AGE` | `"86400"` | `Alt-Svc` max-age in seconds when HTTP/3 is enabled. |
| `VERTEBRA_HTTP3_MAX_STREAMS` | `"256"` | Max concurrent bidirectional QUIC streams per connection. |
| `VERTEBRA_HTTP3_KEEP_ALIVE_MS` | `"15000"` | QUIC keep-alive interval in milliseconds (`0` disables it). |

## Backpressure & Circuit Breaker

Control request queueing and overload shedding:

| Variable | Default | Description |
| --- | --- | --- |
| `VERTEBRA_BP_MAX_IN_FLIGHT` | `1024` | Max active requests before new ones queue. |
| `VERTEBRA_BP_MAX_QUEUE_DEPTH` | `1024` | Max queued requests before returning `503 Service Unavailable`. |
| `VERTEBRA_BP_MAX_QUEUE_TIME_MS` | `100` | Max queue wait time in milliseconds before returning `503`. |
| `VERTEBRA_BP_CB_FAILURE_RATE` | `0.5` | Error rate (`0.0`–`1.0`) that trips the circuit breaker (`429`). |
| `VERTEBRA_BP_CB_MIN_SAMPLES` | `20` | Minimum completed requests before evaluating the error rate. |
| `VERTEBRA_BP_CB_WINDOW_MS` | `10000` | Window duration in milliseconds before testing recovery. |
| `VERTEBRA_BP_CB_HALF_OPEN_PROBES` | `5` | Consecutive successful probe requests needed to close the breaker. |

## Container CPU & Memory Limits

On Linux, Vertebra reads cgroup v2 and cgroup v1 limits at startup:

- **CPU sizing**: Defaults `VERTEBRA_THREADS` to the container's CPU quota rather than the host core count.
- **Shared memory**: Checks available `/dev/shm` space before allocating rings and falls back cleanly when container `/dev/shm` is small (such as Docker's default 64 MiB).
