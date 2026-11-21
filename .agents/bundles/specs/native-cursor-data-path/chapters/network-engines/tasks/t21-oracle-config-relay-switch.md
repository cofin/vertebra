---
type: Task
id: "native-cursor-data-path:network:t21"
chapter_id: network-engines
title: "OracleConfig.enable_reset_relay"
description: "OracleConfig gains enable_reset_relay, default True, and passes it to the native client and pool."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t20"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:197-200"
  - "src/py/vertebra/db/adapters/oracle/config.py:279-346"
  - "src/py/vertebra/db/adapters/oracle/config.py:455-560"
files:
  - "src/py/vertebra/db/adapters/oracle/config.py"
tests:
  - "src/py/tests/unit/db/test_oracle_adapter.py"
estimated_loc: 60
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: oracle-config
    resource: "src/py/vertebra/db/adapters/oracle/config.py::OracleConfig"
    title: "Config struct that gains the switch"
tags: [oracle, python, config]
---

# OracleConfig.enable_reset_relay

## Objective & Scope Boundary

A user turns the in-process TNS relay off with `OracleConfig(enable_reset_relay=False)`. The default is `True`. With `False` the driver connects to the resolved address directly.

Out of scope:

- User documentation for the field. The cleanup chapter writes it.
- Benchmark configuration for the relay-off row. Task t41 passes the flag when it samples.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/oracle/config.py:279-346` lists the `OracleConfig` fields; `:464-518` is `to_client_kwargs`; `:520-560` is `resolve_pool_kwargs`.

## Contracts & Edge Cases

- Add the field `enable_reset_relay: bool = True` to `OracleConfig`, placed after `statement_cache_size` and its docstring. Follow it with a one-line attribute docstring like the neighbouring fields have: `Route TCP sessions through the in-process TNS relay that recovers from reset markers; False connects directly.`
- `to_client_kwargs` adds `"enable_reset_relay": self.enable_reset_relay`.
- `resolve_pool_kwargs` passes the value through to the pool constructor keyword arguments the same way it passes the other client keyword arguments.
- `pool_key` is not changed. Two configs that differ only in this flag share a pool key; the first pool created wins, which matches how other per-connection options behave today.
- `from_url` is not changed. It already copies keyword overrides into the config, so `OracleConfig.from_url(url, enable_reset_relay=False)` works. URL query parameters are not parsed by this method today and none is added.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Default | `OracleConfig().enable_reset_relay` | `True` |
| Client kwargs on | `OracleConfig().to_client_kwargs()['enable_reset_relay']` | `True` |
| Client kwargs off | `OracleConfig(enable_reset_relay=False).to_client_kwargs()['enable_reset_relay']` | `False` |
| Pool kwargs | `resolve_pool_kwargs(OracleConfig(enable_reset_relay=False), {})` | Pool kwargs carry `False` |
| Override | `resolve_pool_kwargs(None, {'enable_reset_relay': False})` | Resolved config has `False` |
| Native accepts | `SyncOracleClient(**OracleConfig(enable_reset_relay=False).to_client_kwargs())` | Constructs |
| from_url override | `OracleConfig.from_url('oracle://u:p@h:1521/svc', enable_reset_relay=False)` | `enable_reset_relay` is `False` |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -q -k "relay"`
- `uv run --no-sync ruff check src/py/vertebra/db/adapters/oracle/config.py`
- `uv run --no-sync mypy src/py/vertebra/db/adapters/oracle/config.py`
- `make oracle-test`

## Acceptance Criteria

- [ ] New tests named `test_oracle_config_relay_*` cover the matrix for both values.
- [ ] `make oracle-test` passes.
