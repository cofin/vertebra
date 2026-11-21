---
type: Task
id: "native-cursor-data-path:cleanup:t03"
chapter_id: cleanup-and-checks
title: "Oracle relay option on benchmark capture"
description: "Let the adapter benchmark capture Oracle with the in-process TNS relay turned off, so the relay-off acceptance row can be measured."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:cleanup:t02"
read_set:
  - "tools/benchmarks/adapters/sql.py:620-726"
  - "tools/benchmarks/adapters/sql.py:1271-1341"
  - "src/py/vertebra/db/adapters/oracle/config.py::OracleConfig"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py:115-151"
files:
  - tools/benchmarks/adapters/sql.py
tests:
  - src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
estimated_loc: 80
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: bench-oracle
    resource: tools/benchmarks/adapters/sql.py::capture_oracle_report
    title: "Oracle adapter benchmark capture"
  - id: oracle-config
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: "Oracle driver configuration with enable_reset_relay"
tags: [benchmarks, oracle, tooling]
---

# t03 — Oracle relay option on benchmark capture

## Objective & Scope Boundary

`capture --engine oracle --oracle-reset-relay off` measures the Vertebra Oracle drivers with `OracleConfig.enable_reset_relay=False`. The acceptance row "Oracle sync, relay off" is measured this way.[^bench-oracle]

This task shares `tools/benchmarks/adapters/sql.py` with t02 and runs after it. `OracleConfig.enable_reset_relay` exists before this task starts; `network-engines` adds it.[^oracle-config]

Out of scope: the Oracle driver, the dialer, and the comparator drivers, which never use the relay.

## Bounded Read Manifest

- `tools/benchmarks/adapters/sql.py:620-726` for `capture_oracle_report`.
- `tools/benchmarks/adapters/sql.py:1271-1341` for `_dispatch_capture` and the `capture` arguments. Line numbers shift by the size of the t02 edit; locate by function name.
- `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` for the field name and `from_url`.
- `src/py/tests/unit/tools/test_adapter_benchmarks_sql.py:115-151` for the monkeypatch style the file already uses.

## Contracts & Edge Cases

```python
async def capture_oracle_report(
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
    enable_reset_relay: bool = True,
) -> AdapterBenchmarkReport: ...
```

- Build the config as `msgspec.structs.replace(OracleConfig.from_url(dsn), enable_reset_relay=enable_reset_relay)`. Both the sync and the async driver receive that config.
- Driver labels stay `vertebra-oracle-sync` and `vertebra-oracle-async`. The report `engine` stays `"oracle"`.
- Extend the docstring with one sentence on the relay argument.

CLI: add `--oracle-reset-relay` to the `capture` subcommand with `choices=("on", "off")`, `default="on"`, and help text `"In-process TNS relay for Vertebra Oracle drivers when --engine=oracle."`.

`_dispatch_capture` passes `enable_reset_relay=args.oracle_reset_relay == "on"` to `capture_oracle_report` only. Other engines ignore the argument.

Edge cases:

- The default keeps today's measurement unchanged.
- The flag with a non-Oracle engine is accepted and has no effect.
- A URL that already sets the relay is overridden by the argument, because the argument is applied after `from_url`.

## Test Matrix

Add to `src/py/tests/unit/tools/test_adapter_benchmarks_sql.py`. Replace `sql_bench.SyncOracleDriver` and `sql_bench.AsyncOracleDriver` with small recording stand-ins through `monkeypatch`. Each stand-in stores the config it receives, supports the context-manager form the capture uses, and returns a one-row result from `select_one`.

| Test | Call | Assert |
| --- | --- | --- |
| `test_capture_oracle_report_passes_relay_switch_to_config` | `capture_oracle_report("oracle://u:p@127.0.0.1:1521/FREEPDB1", iterations=2, warmup=0, include_external=False, enable_reset_relay=False)` | Both stand-ins received a config with `enable_reset_relay is False`; the report has the two Vertebra driver labels |
| `test_capture_oracle_report_keeps_relay_on_by_default` | Same call without the argument | Both configs have `enable_reset_relay is True` |
| `test_sql_cli_oracle_reset_relay_flag` | `main(["capture", "--engine", "oracle", "--dsn", ..., "--iterations", "2", "--warmup", "0", "--no-external", "--oracle-reset-relay", "off", "--output", str(path)])` | Exit code `0`; the recorded config has the relay off; the output file parses |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/tools/test_adapter_benchmarks_sql.py -q
uv run --no-sync python -m tools.benchmarks.adapters.sql capture --help
uv run --no-sync ruff check tools/benchmarks/adapters/sql.py src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
uv run --no-sync mypy tools/benchmarks/adapters/sql.py
```

## Acceptance Criteria

- [ ] The three tests pass and the rest of the file still passes.
- [ ] `capture --help` lists `--oracle-reset-relay {on,off}`.
- [ ] No driver label or report field changes.
- [ ] `ruff` and `mypy` report nothing for the touched files.

[^bench-oracle]: `tools/benchmarks/adapters/sql.py::capture_oracle_report`
[^oracle-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig`
