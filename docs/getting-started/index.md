# Getting Started

Vertebra pairs a typed Python web framework with a Rust HTTP server and a bundled TypeScript client.

## Guides in This Section

| Guide | Summary |
| --- | --- |
| **[Installation](installation.md)** | Install `vertebra-core`, pick optional extras, set up free-threaded Python, and link the TypeScript client. |
| **[Quickstart](quickstart.md)** | Create your first app, run the dev and production servers, and test routes in `pytest`. |
| **[Configuration](configuration.md)** | Configure worker modes, thread counts, backpressure, TLS, and environment variables. |
| **[CLI](cli.md)** | Common `vtb` commands for development, servers, workers, frontend assets, MCP bridges, and standalone builds. |
| **[Examples](../examples/index.md)** | Runnable Python scripts covering routing, queues, live state, storage, and agent tools. |
| **[Changelog](../changelog/index.md)** | Release history across Vertebra versions. |

## Worker Modes at a Glance

- **Embedded mode (`--topology embedded`)**: Runs the Rust HTTP server and Python in one process. Used automatically for single-worker runs and on free-threaded Python.
- **Shared-memory mode (`--topology shm`)**: Spawns separate Python worker processes connected over shared memory. Used automatically when `--workers > 1` on standard Python.
