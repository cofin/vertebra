---
type: Task
task_id: t06-vtb-ai-cli-and-colocated-workers
flow_id: shm-vector-embeddings
title: "vtb ai (models + mcp) CLI Consolidation & Co-Located WorkerRunner in vtb run / vtb dev"
description: "Create vtb ai CLI with nested models (list, pull, stage, warm) and mcp (serve, bridge) subcommands replacing top-level vtb mcp, add co-located WorkerRunner flags and pyproject config to vtb run and vtb dev, add --oneshot to vtb workers run, and sync CLI reference docs."
scope: task
status: draft
state: pending
strategy: tdd
depends_on:
  - t01-gcp-adc-token-cache-and-refresh
  - t05-shm-embedding-broker-and-local-models
traces_to:
  - REQ-06
  - NFR-02
tags:
  - cli
  - ai
  - models
  - mcp
  - workers
  - server
generated:
  by: flow/0.26.0
  at: 2026-11-01T18:37:00Z
verified:
  - by: agent:plan-generator
    at: 2026-11-01T18:37:00Z
    commit: d3d5f8b
sources:
  - id: cli-root
    resource: src/py/vertebra/cli/__init__.py::cli
    title: Vertebra vtb CLI entrypoint and subcommand registration
  - id: cli-mcp
    resource: src/py/vertebra/cli/mcp.py::mcp
    title: Existing vtb mcp CLI group
  - id: cli-run
    resource: src/py/vertebra/cli/run.py::run
    title: vtb run production server command
  - id: cli-dev
    resource: src/py/vertebra/cli/dev.py::dev
    title: vtb dev development server command
  - id: cli-workers
    resource: src/py/vertebra/cli/workers.py::workers
    title: vtb workers background queue runner CLI
  - id: cli-launch
    resource: src/py/vertebra/cli/_launch.py::ServerLaunchEnv
    title: Shared server launch environment configuration for vtb run and vtb dev
  - id: server-lanes
    resource: src/py/vertebra/server/lanes.py::WorkerRunner
    title: Multi-lane background queue worker runner and LaneConfig
  - id: cli-docs-gen
    resource: tools/docs/generate_cli_docs.py::main
    title: Automated CLI documentation generator and drift checker
---

# Task `t06`: `vtb ai` (`models` + `mcp`) CLI Consolidation & Co-Located `WorkerRunner` in `vtb run` / `vtb dev`

<!-- truth: start -->
- **Clean-Break `vtb ai` CLI Consolidation**: `src/py/vertebra/cli/ai.py` registers `vtb ai` (`ai`) with two nested command groups—`vtb ai models` (`list`, `pull`, `stage`, `warm`) and `vtb ai mcp` (`serve`, `bridge`)—and removes top-level `vtb mcp` from `src/py/vertebra/cli/__init__.py`[^cli-root].
- **Zero-Account Model Pulling & Cloud Run GCS Staging**: `vtb ai models pull`, `stage`, and `warm` use `ObjectStore` (`RefreshingGcpCredentialProvider` from `t01`) to fetch models from non-gated mirrors (`onnx-community/*`, `litert-community/*`, `storage.googleapis.com/qdrant-fastembed/`, `minishlab/*`), upload verified SHA-256 `manifest.json` bundles to `gs://` / `s3://` / `r2://` / `oci://`, and warm `/dev/shm` (`ShmRegion`) on container startup.
- **Co-Located `WorkerRunner` in `vtb run` & `vtb dev`**: `vtb run`[^cli-run], `vtb dev`[^cli-dev], `ServerLaunchEnv`[^cli-launch], and `WorkerRunner`[^server-lanes] support `--with-workers`, `--queues`, and `--lane-concurrency` (plus `[tool.vertebra.workers]` in `pyproject.toml`) to co-locate background queue workers alongside HTTP/gRPC traffic, while `vtb workers run`[^cli-workers] gains `--oneshot <job>`.
<!-- truth: end -->

---

## 1. Objective & Scope

### 1.1 Goal

Consolidate AI-related CLI subcommands under `vtb ai` (`vtb ai models` + `vtb ai mcp`), add configurable co-located background queue workers to `vtb run` and `vtb dev`, add `--oneshot` to `vtb workers run`, and synchronize [`tools/docs/generate_cli_docs.py`](../../../../../tools/docs/generate_cli_docs.py)[^cli-docs-gen] and [`docs/reference/cli.md`](../../../../../docs/reference/cli.md).

### 1.2 In-Scope Files

- `src/py/vertebra/cli/ai.py` (new module replacing `src/py/vertebra/cli/mcp.py`[^cli-mcp])
- [`src/py/vertebra/cli/__init__.py`](../../../../../src/py/vertebra/cli/__init__.py)
- [`src/py/vertebra/cli/run.py`](../../../../../src/py/vertebra/cli/run.py)
- [`src/py/vertebra/cli/dev.py`](../../../../../src/py/vertebra/cli/dev.py)
- [`src/py/vertebra/cli/_launch.py`](../../../../../src/py/vertebra/cli/_launch.py)
- [`src/py/vertebra/cli/workers.py`](../../../../../src/py/vertebra/cli/workers.py)
- [`src/py/vertebra/server/lanes.py`](../../../../../src/py/vertebra/server/lanes.py)
- [`src/py/vertebra/server/worker.py`](../../../../../src/py/vertebra/server/worker.py)
- [`tools/docs/generate_cli_docs.py`](../../../../../tools/docs/generate_cli_docs.py)
- [`docs/reference/cli.md`](../../../../../docs/reference/cli.md)
- `src/py/tests/unit/cli/test_ai_cli.py`
- `src/py/tests/unit/cli/test_run_with_workers.py`

---

## 2. Public Seam & Implementation Specification

### 2.1 `vtb ai` Command Group (`src/py/vertebra/cli/ai.py`)

1. **`vtb ai` Root Group (`ai`)**:
   - Registered in [`src/py/vertebra/cli/__init__.py`](../../../../../src/py/vertebra/cli/__init__.py)[^cli-root] (`cli.add_command(ai)`) in place of `mcp`, and `src/py/vertebra/cli/mcp.py`[^cli-mcp] is removed (clean break).
   - Sub-groups: `models` and `mcp`.
2. **`vtb ai models` Subcommands**:
   - `vtb ai models list [--provider vertex|gemini|gemma|fastembed|static] [--json]`:
     - Displays the built-in `ModelSpec` catalog (`gemini-embedding-001`, `gemini-embedding-2`, `text-embedding-005`, `embeddinggemma-2-270m`, `embeddinggemma-300m`, `BAAI/bge-small-en-v1.5`, `nomic-ai/nomic-embed-text-v1.5`, `minishlab/potion-base-8M`), supported MRL dimensions, quantizations, and whether each local model is cached on disk or warmed in `/dev/shm`.
   - `vtb ai models pull <model> [--quant int8|q4|bf16|fp32] [--cache-dir <dir>]`:
     - Resolves the unauthenticated mirror or cloud URL from `ModelSpec`, downloads model artifacts + `tokenizer.json` via `ObjectStore` (`vertebra.storage`), verifies SHA-256 digests, and writes `manifest.json` in the local cache directory without requiring a Hugging Face account or token.
   - `vtb ai models stage <model> --to <cloud_uri> [--quant int8|q4|bf16|fp32] [--cache-dir <dir>]`:
     - Ensures the model is pulled locally (or streams directly), computes a deterministic `manifest.json` (`model`, `quantization`, `files: [{path, size, sha256}]`), and uploads the model bundle to `gs://<bucket>/<prefix>` (or `s3://`, `r2://`, `oci://`) via `ObjectStore` using `RefreshingGcpCredentialProvider`.
   - `vtb ai models warm <model> [--from <cloud_uri>] [--shm] [--cache-dir <dir>]`:
     - Fetches the staged `manifest.json` and model files from `--from` (or `VERTEBRA_MODEL_SOURCE`), and when `--shm` is set, maps the model artifacts into a named `/dev/shm` region (`vtb-model-<slug>`) so all spawned worker processes read the same physical RAM pages (`1x` RAM).
3. **`vtb ai mcp` Subcommands (`serve`, `bridge`)**:
   - `vtb ai mcp serve [--name <server_name>]`
   - `vtb ai mcp bridge --url <endpoint> [--token <bearer_token>] [--timeout <seconds>]`
   - Preserves the exact options (`--url`, `--token`, `--timeout`, `--name`) and helper functions (`extract_mcp_headers_from_jsonrpc`, `run_mcp_stdio_bridge`) of the former `src/py/vertebra/cli/mcp.py`[^cli-mcp] inside `src/py/vertebra/cli/ai.py`.

### 2.2 Co-Located `WorkerRunner` in `vtb run` & `vtb dev` and `--oneshot` in `vtb workers run`

1. **`ServerLaunchEnv`, `WorkerRunner` & `run_supervisor` Updates (`_launch.py`, `lanes.py`, `worker.py`, `run.py`, `dev.py`)**:
   - Extend `ServerLaunchEnv` in [`src/py/vertebra/cli/_launch.py`](../../../../../src/py/vertebra/cli/_launch.py)[^cli-launch] (and `serve()` in [`src/py/vertebra/server/worker.py`](../../../../../src/py/vertebra/server/worker.py)) with:
     - `with_workers: bool = False`
     - `worker_queues: tuple[str, ...] = ("default",)`
     - `worker_lane_concurrency: dict[str, int] = msgspec.field(default_factory=dict)`
   - Support `[tool.vertebra.workers]` in `pyproject.toml`:
     ```toml
     [tool.vertebra.workers]
     enabled_in_run = true
     queues = ["default", "embeddings"]
     lanes = { default = 8, embeddings = 1 }
     ```
   - Add CLI flags `--with-workers / --no-workers`, `--queues <csv>`, and `--lane-concurrency <lane=count,...>` to `vtb run` (`src/py/vertebra/cli/run.py`[^cli-run]) and `vtb dev` (`src/py/vertebra/cli/dev.py`[^cli-dev]).
   - When `with_workers` is enabled (via CLI flag or `enabled_in_run = true` in `pyproject.toml`), `serve()` / `ServerLaunchEnv` starts the configured `WorkerRunner` (`LaneConfig`[^server-lanes]) queue consumers alongside the HTTP/gRPC server and drains them gracefully on `SIGTERM` / `SIGINT`.
2. **`vtb workers run --oneshot <job_name>` (`src/py/vertebra/cli/workers.py`)**:
   - Add `--oneshot <job_name>` and `--payload <json>` to `vtb workers run <app>` (`src/py/vertebra/cli/workers.py`[^cli-workers]) so Cloud Run Jobs and Kubernetes CronJobs can execute a single registered background task (such as bulk vector backfills) and exit with code `0` on completion.

### 2.3 CLI Documentation Sync (`tools/docs/generate_cli_docs.py` & `docs/reference/cli.md`)

- Update [`tools/docs/generate_cli_docs.py`](../../../../../tools/docs/generate_cli_docs.py)[^cli-docs-gen] so it renders recursive nested Click groups (`vtb ai models ...` and `vtb ai mcp ...`) and regenerate [`docs/reference/cli.md`](../../../../../docs/reference/cli.md).

---

## 3. TDD Verification Plan

### 3.1 Red-Green-Refactor Sequence

1. **Red**:
   - Write Python unit tests in `src/py/tests/unit/cli/test_ai_cli.py` verifying:
     - `vtb ai --help`, `vtb ai models --help`, and `vtb ai mcp --help` succeed, and top-level `vtb mcp` no longer exists.
     - `vtb ai models list --json` outputs valid JSON covering `gemini-embedding-001`, `embeddinggemma-2-270m`, `embeddinggemma-300m`, `BAAI/bge-small-en-v1.5`, and `minishlab/potion-base-8M`.
     - `vtb ai models pull`, `stage`, and `warm --shm` roundtrip model files and `manifest.json` using a local `file://` or mock `ObjectStore` target and verify SHA-256 digests.
     - `vtb ai mcp serve --name test` and `vtb ai mcp bridge --url http://127.0.0.1:8000/mcp` invoke the MCP server/bridge entrypoints and preserve `extract_mcp_headers_from_jsonrpc` and `run_mcp_stdio_bridge`.
   - Write Python unit tests in `src/py/tests/unit/cli/test_run_with_workers.py` verifying:
     - `vtb run app:app --with-workers --queues default,embeddings --lane-concurrency default=4,embeddings=1` and `[tool.vertebra.workers]` in `pyproject.toml` populate `ServerLaunchEnv` and start/drain the co-located `WorkerRunner`.
     - `vtb workers run app:app --oneshot backfill_vectors --payload '{"batch":100}'` executes the single job and exits `0`.
   - Update existing `src/py/tests/unit/cli/` tests that reference `vtb mcp` to use `vtb ai mcp`.
2. **Green**:
   - Implement `src/py/vertebra/cli/ai.py`, update `cli/__init__.py`, `run.py`, `dev.py`, `_launch.py`, `workers.py`, `lanes.py`, `worker.py`, update `tools/docs/generate_cli_docs.py`, and regenerate `docs/reference/cli.md`.
3. **Refactor & Verify**:
   - Run `pytest src/py/tests/unit/cli/ -v` and `.venv/bin/python tools/docs/generate_cli_docs.py --check`.

### 3.2 Verification Commands

```bash
.venv/bin/pytest src/py/tests/unit/cli/ -v
.venv/bin/python tools/docs/generate_cli_docs.py --check
```

---

## 4. Task Notes

*(Populated during `/flow:implement` execution.)*

---

## Provenance Footnotes

[^cli-root]: `src/py/vertebra/cli/__init__.py::cli` — Vertebra `vtb` CLI entrypoint and subcommand registration.
[^cli-mcp]: `src/py/vertebra/cli/mcp.py::mcp` — Existing `vtb mcp` CLI group.
[^cli-run]: `src/py/vertebra/cli/run.py::run` — `vtb run` production server command.
[^cli-dev]: `src/py/vertebra/cli/dev.py::dev` — `vtb dev` development server command.
[^cli-workers]: `src/py/vertebra/cli/workers.py::workers` — `vtb workers` background queue runner CLI.
[^cli-launch]: `src/py/vertebra/cli/_launch.py::ServerLaunchEnv` — Shared server launch environment configuration for `vtb run` and `vtb dev`.
[^server-lanes]: `src/py/vertebra/server/lanes.py::WorkerRunner` — Multi-lane background queue worker runner and `LaneConfig`.
[^cli-docs-gen]: `tools/docs/generate_cli_docs.py::main` — Automated CLI documentation generator and drift checker.
