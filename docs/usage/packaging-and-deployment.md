# Packaging & Deployment

Deploy Vertebra applications as standard Python wheels (`vtb run ...`), containers with automatic CPU and shared-memory sizing, or self-contained standalone and single-file binaries (`StandaloneBuilder`, `OnefileBuilder`).

## Standalone & Single-File Binaries (`vtb build`)

`vertebra.build.packaging` bundles your Python application, compiled Rust extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`), Python Build Standalone runtime, and frontend assets into a relocatable directory (`StandaloneBuilder`) or a self-extracting single-file binary with SHA-256 verification (`OnefileBuilder`):

```bash
vtb build --standalone --onefile
```

To stage `.agents/skills` for application repositories inside a standalone or onefile bundle, opt in with `--include-agent-skills` (`AGENTS.md` and `CLAUDE.md` are excluded, and `VERTEBRA_APP_ROOT` is exported at launch).

Programmatic configuration via `BuildConfig`, `BuildTarget`, `StandaloneBuilder`, and `OnefileBuilder`:

```python
from pathlib import Path
from vertebra.build import BuildConfig
from vertebra.build.packaging import BuildTarget, OnefileBuilder, StandaloneBuilder

config = BuildConfig.load(Path("."))
standalone = StandaloneBuilder(
    project_dir=Path("."),
    target=BuildTarget.current(),
    entrypoint=config.app or "app:app",
)
onefile = OnefileBuilder(project_dir=Path("."), entrypoint=config.app or "app:app")
```

## TypeScript, Zod, OpenAPI & Protobuf Codegen (`vertebra.build.codegen`)

Generate TypeScript route types (`TypeScriptGenerator`), Zod or Valibot validation schemas (`ZodGenerator`, `ValibotGenerator`), RPC procedure clients (`ProcedureClientGenerator`), OpenAPI 3.1 specs (`OpenAPIGenerator`), and `.proto` definitions (`ProtobufGenerator`) from your Python routes and `msgspec.Struct` models:

```bash
vtb js generate-types
```

## Production Performance & Compiler Optimization

Vertebra official release distribution wheels (`vertebra-core` across `linux-x86_64`, `linux-aarch64`, and macOS `arm64`/`x86_64`) ship the pre-compiled relocatable `vertebra/bin/vertebra-server` binary optimized with LLVM Profile-Guided Optimization (PGO) using `$ORIGIN`/`@loader_path` relative rpaths. At runtime, `EmbeddedPythonRuntime.require_shared_library` verifies a shared `libpython` installation (`ENABLE_SHARED=1`, such as `uv python install`) unless `VERTEBRA_SERVER_BINARY` overrides the binary path. Wheel packaging and relocatable `vtb run` smoke execution can be verified with `make core-wheel-check`.

## Container Deployment & Process Supervision

When running inside Docker, Kubernetes, or Cloud Run:

1. **Automatic CPU & Memory Detection**: `CgroupQuota.effective_cpu_count()` reads `/sys/fs/cgroup/cpu.max` so `VERTEBRA_THREADS` matches the container's CPU limit automatically.
2. **Shared-Memory Allocation (`ShmAllocationPolicy`)**: Uses anonymous `memfd_create` descriptors passed over `SCM_RIGHTS` UNIX sockets (or `/dev/shm` when available) so `--topology shm` works cleanly even on read-only container root filesystems.
3. **Process Supervision (`vertebra.supervisor`)**: In multi-worker deployments (`--workers > 1`), `run_supervisor` monitors worker processes, coordinates graceful `SIGTERM` drains, and restarts crashed workers.
4. **TLS Trust Store for Google Cloud Clients**: The Spanner Data and BigQuery Storage gRPC clients trust the operating system certificate store plus bundled Mozilla roots (`webpki-roots`). Spanner Database Admin DDL (`update_database_ddl`) runs over REST and uses the operating system trust store (`rustls-native-certs`), so minimal container images that execute Spanner DDL must include a system CA bundle (such as `ca-certificates`). Standard proxy environment variables (`HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`, `NO_PROXY`) apply to REST calls.

## WebAssembly Plugin Sandboxes (`vertebra.wasm`)

Run untrusted plugins or custom data transforms inside memory-, fuel-, and timeout-bounded WASI WebAssembly sandboxes using `WASMSandboxPolicy`, `WASMModuleConfig`, and `WASMPluginHost`:

```python
from vertebra.wasm import WASMModuleConfig, WASMPluginHost, WASMSandboxPolicy

policy = WASMSandboxPolicy(
    max_memory_bytes=32 * 1024 * 1024,
    max_fuel=5_000_000,
    epoch_timeout_ms=250,
    allow_network=False,
)
host = WASMPluginHost()
```
