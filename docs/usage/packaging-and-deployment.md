# Packaging & Deployment

Vertebra applications can be deployed as standard Python wheels (`uv run vtb run ...`), OCI containers with automatic cgroup quota detection, or self-contained standalone and single-file executables via `StandaloneBuilder` and `OnefileBuilder`.

## Standalone Binary Builds (`vtb build`)

`vertebra.build.packaging` packages your Python application, compiled Rust extensions (`_vertebra`, `_bundler`, `_storage`), and bundled frontend assets into a relocatable directory or single-file executable:

```bash
uv run vtb build --standalone --onefile
```

Programmatic configuration is available via `BuildConfig`, `BuildOrchestrator`, `StandaloneBuilder`, and `OnefileBuilder`:

```python
from pathlib import Path
from vertebra.build import BuildConfig
from vertebra.build.packaging import BuildTarget, StandaloneBuilder

config = BuildConfig.load(Path("."))
builder = StandaloneBuilder(
    project_dir=Path("."),
    target=BuildTarget.current(),
    entrypoint=config.app or "app:app",
)
```

## Container & Kubernetes Deployment

When running inside Docker, Kubernetes, or Cloud Run:

1. **Automatic CPU & Memory Detection**: `CgroupQuota.effective_cpu_count()` reads `/sys/fs/cgroup/cpu.max` so `VERTEBRA_THREADS` matches the pod's CPU limit automatically.
2. **Shared-Memory Volume Sizing**: When using `--topology shm` in containers, mount `/dev/shm` with sufficient capacity (`emptyDir: { medium: Memory, sizeLimit: 256Mi }` in Kubernetes or `--shm-size=256m` in Docker), or use `--topology embedded` to avoid `/dev/shm` allocation altogether.

## Process Supervision (`vertebra.supervisor`)

In multi-worker deployments (`--topology shm --workers N`), the supervisor monitors worker health, enforces graceful drain timeouts on `SIGTERM`/`SIGINT`, and restarts crashed worker processes with bounded backoff.

## WebAssembly Sandboxes (`vertebra.wasm`)

Execute untrusted plugin modules or edge transforms safely inside memory- and fuel-bounded WebAssembly sandboxes configured via `WasmSandboxPolicy` and `WasmModuleConfig`.
