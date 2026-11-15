# Configuration

Vertebra configures both your Python application and the Rust HTTP server using typed config objects and environment variables.

## Worker Modes

Vertebra automatically selects the worker mode from `--workers` and your Python build (`--topology auto`), or you can override it explicitly with `--topology` on `vtb run` or `VERTEBRA_TOPOLOGY`:

| Mode | Automatic Selection | Explicit Override | Description |
| --- | --- | --- | --- |
| **Embedded** | Single worker (`<= 1`) or free-threaded Python (`3.14t`/`3.15t`) | `--topology embedded` / `VERTEBRA_TOPOLOGY=embedded` | Runs the Rust HTTP server and Python in a single OS process. Ideal for development, single-worker services, and free-threaded Python (`3.14t`/`3.15t`). |
| **Shared-Memory (`shm`)** | `--workers > 1` on standard GIL Python (Linux/macOS) | `--topology shm` / `VERTEBRA_TOPOLOGY=shm` | Spawns separate Python worker processes connected to the Rust server over shared memory. Ideal for multi-process scaling on standard GIL Python. |

## Server Environment Variables

`vtb run` and `vtb dev` export the following runtime environment variables to the Rust HTTP server and Python workers:

| Variable | Default | Description |
| --- | --- | --- |
| `VERTEBRA_APP` | - | Application import path (`module:attribute`). |
| `VERTEBRA_HOST` | `"127.0.0.1"` | Bind address for the HTTP listener. |
| `VERTEBRA_PORT` | `"8000"` | TCP/UDP port for HTTP/1.1, HTTP/2, and optional HTTP/3. |
| `VERTEBRA_TOPOLOGY` | `"auto"` | Worker mode (`auto`, `embedded`, or `shm`). |
| `VERTEBRA_WORKERS` | `"0"` (`embedded`) / `"1"` (`shm`) | Worker process count (`0` runs in-process). |
| `VERTEBRA_THREADS` | CPU quota | Rust HTTP worker thread count (automatically respects container CPU limits). |
| `VERTEBRA_LOG_LEVEL` | `"info"` | Log level (`debug`, `info`, `warning`, `error`). |
| `PYTHON_JIT` | - | Set to `"1"` or `"0"` when `--jit` / `--no-jit` is passed to `vtb run`. |
| `VERTEBRA_STRICT_FREE_THREADING` | `"0"` | Set to `"1"` to fail startup if an imported C extension re-enables the GIL on `3.14t`/`3.15t`. |
| `VERTEBRA_TLS_CERT` | - | Path to the PEM TLS certificate chain for HTTPS and HTTP/3 termination. |
| `VERTEBRA_TLS_KEY` | - | Path to the PEM private key matching `VERTEBRA_TLS_CERT`. |
| `VERTEBRA_HTTP3` | `"false"` | Set to `"1"` or `"true"` to enable the HTTP/3 over QUIC UDP listener (requires TLS and `--features http3`). |
| `VERTEBRA_HTTP3_ALT_SVC_MAX_AGE` | `"86400"` | `ma=` max-age in seconds advertised in `Alt-Svc` headers when QUIC is bound. |
| `VERTEBRA_HTTP3_MAX_STREAMS` | `"256"` | Maximum concurrent bidirectional QUIC streams per HTTP/3 connection (`> 0`). |
| `VERTEBRA_HTTP3_KEEP_ALIVE_MS` | `"15000"` | QUIC transport keep-alive interval in milliseconds (`0` disables keep-alive). |

## Backpressure & Load Shedding Variables

Tune request queueing and circuit breaking on the HTTP server with:

| Variable | Default | Description |
| --- | --- | --- |
| `VERTEBRA_BP_MAX_IN_FLIGHT` | `1024` | Maximum concurrent active requests before queueing. |
| `VERTEBRA_BP_MAX_QUEUE_DEPTH` | `1024` | Maximum queued requests before returning `503 Service Unavailable`. |
| `VERTEBRA_BP_MAX_QUEUE_TIME_MS` | `100` | Maximum queue wait time in milliseconds before returning `503`. |
| `VERTEBRA_BP_CB_FAILURE_RATE` | `0.5` | Error rate (`0.0`–`1.0`) that trips the circuit breaker (`429`). |
| `VERTEBRA_BP_CB_MIN_SAMPLES` | `20` | Minimum completed requests in the window before evaluating the error rate. |
| `VERTEBRA_BP_CB_WINDOW_MS` | `10000` | Sliding window duration (ms) before the circuit breaker tests recovery. |
| `VERTEBRA_BP_CB_HALF_OPEN_PROBES` | `5` | Consecutive successful probe requests required to close the circuit breaker. |

## Container CPU & Memory Detection

Vertebra reads Linux cgroup v2 (`/sys/fs/cgroup/cpu.max`, `memory.max`) and cgroup v1 limits at startup:

- **CPU Sizing**: Defaults `VERTEBRA_THREADS` to the container's CPU limit rather than the host node's total core count.
- **Shared-Memory Sizing**: Checks available `/dev/shm` space before allocating shared memory and falls back cleanly when container `/dev/shm` mounts are small (such as Docker's default `64 MiB`).
