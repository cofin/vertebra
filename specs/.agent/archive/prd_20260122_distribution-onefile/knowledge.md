# Knowledge Capture: Distribution & Packaging for Vertebra 1.0

## Patterns Discovered
- **PyO3 GIL Release**: The `py.allow_threads` API is deprecated in favor of `py.detach`. This is critical for avoiding warnings and ensuring future compatibility.
- **Process Supervision**: Rust's `std::process::Command` is robust but requires careful handling of arguments compared to Python's `subprocess.Popen(shell=False)`.
- **Testing Native Extensions**: Mocking native extensions in Python tests (`unittest.mock.patch("vertebra._vertebra.Supervisor")`) works effectively for testing the Python wrapper logic without needing to compile the extension for every test run (though integration tests still need the binary).
- **CLI Composition**: Splitting a large `click` CLI into modules (`serve.py`, `build.py`, `init.py`) and aggregating them in `__init__.py` keeps the codebase manageable.
- **Config Loading**: Using `tomllib` (Python 3.11+) allows native TOML parsing for `pyproject.toml` without external dependencies in the build toolchain itself (though Vertebra requires 3.12+, so it's safe).
- **Offline Verification**: Using `unshare --net` (Linux) in CI is a reliable way to verify that builds don't access the network unexpectedly.

## Lessons Learned
- **Rust/Python API Alignment**: The initial Rust implementation of `Supervisor` lacked argument passing for the engine command, whereas the Python `run_supervisor` logic heavily relied on constructing a command list. Aligning these required updating the Rust struct signature.
- **Test Dependencies**: Adding `uvloop` to the `test` dependency group in `pyproject.toml` was necessary to verify event loop compatibility.
- **Bun.build API Quirks**: `Bun.build({ compile: true })` with `target: "bun"` (native) appears to ignore the `outfile` option and defaults to creating the binary next to the entrypoint (e.g., `src/ts/server` from `src/ts/src/server/index.ts`). Cross-compilation targets might behave differently, but native builds need post-processing (move) if a specific output location is required.
- **pyembed Incompatibility**: The `pyembed` crate (part of PyOxidizer) depends on older `pyo3` versions incompatible with `pyo3 0.27`. We deviated from the PRD by skipping `pyembed` integration and relying on `python-build-standalone` manual management (via `vertebra.build.standalone`) and potentially `pyapp` for framework distribution.

## Decisions Made
- **Native Supervisor**: We replaced the Python `subprocess`-based supervisor with a Rust `NativeSupervisor` to handle signals and process lifecycles more reliably (especially avoiding GIL contention during shutdowns).
- **Argument Passing**: Updated the Rust `start` method to accept `engine_args` (Vec<String>) to support flexible `bun` invocations (e.g., `bun run server.ts`).
- **Build CLI**: Implemented a TypeScript CLI wrapper (`tools/build-cli.ts`) around `Bun.build` to provide a consistent interface for building the engine across platforms, rather than relying on multiple package.json scripts.
- **Template Engine**: Used simple string replacement instead of adding `jinja2` as a dependency for `vertebra init`, keeping the installation footprint small.
- **Build Orchestration**: Created a `Builder` class to orchestrate the build process, delegating to specialized components like `Bundler` for one-file builds.
- **Standalone Runtime**: Implemented `StandaloneManager` to download and cache `python-build-standalone` releases, replacing the need for `pyembed` crate logic for runtime acquisition.

## Reusable Components
- `src/rs/vertebra-py/src/supervisor.rs`: A generic process group manager that could be extracted into a standalone Rust crate for Python apps.
- `tools/build-engine.ts`: A reusable builder for Bun single-file executables.
- `src/py/vertebra/cli/init.py`: Scaffolding logic that can be extended for other project types.
- `src/py/vertebra/build/config.py`: Reusable logic for parsing tool configuration from `pyproject.toml`.
- `src/py/vertebra/build/standalone.py`: A utility to download and manage PBS releases.

## Gotchas & Warnings
- **Windows Support**: The current Windows implementation in `supervisor.rs` uses a placeholder wait. Full Job Object support is enabled via the `windows` crate dependency but needs to be fleshed out in the implementation logic to ensure child processes are killed when the parent dies (avoiding orphans).
- **Bun Output Paths**: Be aware of `Bun.build` output paths when `target` is native.
- **File Ignores**: Tooling might ignore directories named `build` if configured in `.geminiignore` or `.gitignore`, requiring explicit whitelisting.
