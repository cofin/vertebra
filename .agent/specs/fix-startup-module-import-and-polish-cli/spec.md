# Specification: Fix Startup Module Import and Polish CLI

## Overview
The user is encountering a `ModuleNotFoundError` when trying to run the `examples/hello-world/app.py` using `uv run vtb serve`. The import system seems to be failing to resolve the path correctly. Additionally, we want to improve the CLI diagnostics to better report such issues.

## Problem Description
- **Error:** `ModuleNotFoundError: No module named 'examples/hello-world/app'`
- **Context:** Running `uv run vtb serve examples/hello-world/app.py`
- **Root Cause Analysis (Hypothesis):** The python import logic in `src/py/vertebra/server/worker.py` is likely not handling relative file paths correctly or not adding the current working directory (or the file's directory) to `sys.path`.

## Goals
1. **Fix Import Error:** Ensure `vtb serve` can correctly load python files from relative paths like `examples/hello-world/app.py`.
2. **Enhance CLI Diagnostics:** Improve error reporting during startup to be more descriptive than a raw traceback if possible, or at least guide the user.

## Technical Requirements
- Update `_import_app` in `src/py/vertebra/server/worker.py` to correctly handle file paths.
- Ensure `sys.path` includes the necessary directories.
- Add test case to verify `vtb serve` works with a sample app file.
