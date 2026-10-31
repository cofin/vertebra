# Configuration

Vertebra configures both the Python application layer (`App`, `OpenAPIConfig`, `SecurityConfig`, `QueueConfig`, `SsrConfig`) and the Rust HTTP/IPC runtime (`ServerLaunchEnv` and environment variables).

## Runtime Topologies

Vertebra automatically selects the execution topology based on `--workers` and the active Python interpreter (`--topology auto`), or you can override it explicitly with `--topology` on `vtb run` / `VERTEBRA_TOPOLOGY`:

| Topology | Automatic Selection | Explicit Override | Description |
| --- | --- | --- | --- |
| **Embedded** | Single worker (`<= 1`) or free-threaded Python (`3.14t`/`3.15t`) | `--topology embedded` / `VERTEBRA_TOPOLOGY=embedded` | Runs the Rust Tokio/Hyper server and Python interpreter in a single OS process via `BatchBridge`. Ideal for development and free-threaded Python (`3.14t`/`3.15t`). |
| **Shared-Memory (`shm`)** | `--workers > 1` on standard GIL Python (POSIX) | `--topology shm` / `VERTEBRA_TOPOLOGY=shm` | Spawns independent Python worker processes connected to the Rust ingress parent over lock-free SPSC shared-memory rings in `/dev/shm` (or POSIX shm). Ideal for multi-process isolation on standard GIL Python. |

## Server Environment Variables

`vtb run` and `vtb dev` populate `ServerLaunchEnv` and export the following runtime environment variables to the Rust HTTP ingress and Python workers:

| Variable | Default | Description |
| --- | --- | --- |
| `VERTEBRA_APP` | - | Application import path (`module:attribute`). |
| `VERTEBRA_HOST` | `"127.0.0.1"` | Bind address for the HTTP listener. |
| `VERTEBRA_PORT` | `"8000"` | TCP/UDP port for HTTP/1.1, HTTP/2, and optional HTTP/3. |
| `VERTEBRA_TOPOLOGY` | `"auto"` -> `"shm"` (`workers > 1` on GIL POSIX) or `"embedded"` | Execution topology (`auto`, `embedded`, or `shm`). |
| `VERTEBRA_WORKERS` | `"0"` (`embedded`) / `"1"` (`shm`) | Worker process count (`0` runs in-process). |
| `VERTEBRA_THREADS` | CPU quota | Tokio runtime worker thread count (automatically respects container cgroup v1/v2 quotas via `CgroupQuota.effective_cpu_count()`). |
| `VERTEBRA_LOG_LEVEL` | `"info"` | Log level (`debug`, `info`, `warning`, `error`). |
| `PYTHON_JIT` | - | Set to `"1"` or `"0"` when `--jit` / `--no-jit` is passed to `vtb run`. |

## Backpressure & Admission Control Variables

When `BackpressureController` is enabled on the ingress dispatcher, tune admission control and circuit breaking with:

| Variable | Default | Description |
| --- | --- | --- |
| `VERTEBRA_BP_MAX_IN_FLIGHT` | `1024` | Maximum concurrent in-flight request permits before queueing. |
| `VERTEBRA_BP_MAX_QUEUE_DEPTH` | `1024` | Maximum queued waiters before immediate `503 Service Unavailable` load shedding. |
| `VERTEBRA_BP_MAX_QUEUE_TIME_MS` | `100` | Maximum queue wait duration in milliseconds before `503`. |
| `VERTEBRA_BP_CB_FAILURE_RATE` | `0.5` | Failure ratio (`0.0`–`1.0`) that trips the circuit breaker to `Open` (`429`). |
| `VERTEBRA_BP_CB_MIN_SAMPLES` | `20` | Minimum completed requests in the sliding window before circuit evaluation. |
| `VERTEBRA_BP_CB_WINDOW_MS` | `10000` | Sliding window duration (ms) for failure decay and `Open` -> `HalfOpen` transition. |
| `VERTEBRA_BP_CB_HALF_OPEN_PROBES` | `5` | Consecutive successful probes in `HalfOpen` required to close the circuit. |

## Container & Cgroup Awareness

Vertebra inspects Linux cgroup v2 (`/sys/fs/cgroup/cpu.max`, `memory.max`) and cgroup v1 quotas at startup via `vertebra.server.platform.CgroupQuota` and `ShmAllocationPolicy`:

- **CPU Sizing**: Defaults `VERTEBRA_THREADS` to the container's effective CPU ceiling rather than the host node's physical core count.
- **SHM Ring Sizing**: Verifies `/dev/shm` free capacity before allocating shared-memory rings and falls back cleanly when container `/dev/shm` mounts are constrained (such as Docker's default `64 MiB`).
