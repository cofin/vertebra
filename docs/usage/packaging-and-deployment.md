# Packaging & Deployment

Deploy Vertebra applications as standard Python packages (`vtb run`), containers with automatic cgroup sizing, or self-contained standalone and single-file binaries.

## Standalone & Single-File Binaries

Build a relocatable directory or a SHA-256 verified single-file executable containing your Python application, compiled extensions, Python runtime, and frontend assets:

```bash
vtb build --standalone --onefile
```

Pass `--include-agent-skills` to bundle `.agents/skills` inside the output (`AGENTS.md` and `CLAUDE.md` are excluded, and `VERTEBRA_APP_ROOT` is set at launch).

You can also drive builds from Python:

```python
from pathlib import Path
from vertebra.build import BuildConfig
from vertebra.build.packaging import BuildTarget, OnefileBuilder, StandaloneBuilder

config = BuildConfig.load(Path("."))
target = BuildTarget.detect_host()
standalone = StandaloneBuilder(
    project_dir=Path("."),
    target=target,
    entrypoint=config.app or "app:app",
)
dist_dir = standalone.build(Path("dist/standalone"))
onefile = OnefileBuilder(dist_dir=dist_dir, target=target)
```

## TypeScript, Zod, OpenAPI & Protobuf Codegen

Generate TypeScript route types, Zod or Valibot validation schemas, RPC clients, OpenAPI 3.1 specs, and `.proto` definitions from your Python routes and `msgspec.Struct` models:

```bash
vtb js generate-types
```

## Release Wheels & Shared Python

Release wheels for `vertebra-core` on Linux (`x86_64`, `aarch64`) and macOS (`arm64`, `x86_64`) include the pre-compiled `vertebra-server` binary. Running it requires a Python installation built with a shared `libpython` (such as `uv python install`), unless you override the binary path with `VERTEBRA_SERVER_BINARY`.

## Container Deployment & Process Supervision

When running inside Docker, Kubernetes, or Cloud Run:

1. **CPU limits**: Reads container cgroup quotas automatically so `VERTEBRA_THREADS` matches the container's CPU limit.
2. **Shared memory**: Uses anonymous memory descriptors (or `/dev/shm` when available), so `--topology shm` works on read-only container filesystems.
3. **Process supervision**: With `--workers > 1`, the supervisor monitors worker processes, coordinates graceful `SIGTERM` drains, and restarts crashed workers.
4. **Google Cloud TLS certificates**: Spanner Data and BigQuery Storage gRPC clients trust both system roots and bundled roots. Spanner admin DDL runs over REST using the OS certificate store, so minimal container images running Spanner DDL must include a CA bundle such as `ca-certificates`. Standard proxy variables (`HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`, `NO_PROXY`) apply to REST calls.

## WebAssembly Plugin Sandboxes

Run untrusted plugins or custom data transforms inside WASI WebAssembly sandboxes bounded by memory, fuel, and timeout limits:

```python
from vertebra.wasm import WASMModuleConfig, WASMPluginHost, WASMSandboxPolicy

policy = WASMSandboxPolicy(
    max_memory_bytes=32 * 1024 * 1024,
    max_fuel=5_000_000,
    epoch_timeout_ms=250,
    allow_network=False,
)
module = WASMModuleConfig(
    name="transform",
    wasm_source="plugins/transform.wasm",
    policy=policy,
)
host = WASMPluginHost()
```
