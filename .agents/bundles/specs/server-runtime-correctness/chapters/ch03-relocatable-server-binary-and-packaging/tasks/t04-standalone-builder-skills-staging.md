---
type: Task
id: "server-runtime-correctness:ch03:t04"
flow_id: server-runtime-correctness
chapter_id: ch03-relocatable-server-binary-and-packaging
title: "Stage Opt-In User .agents/skills in StandaloneBuilder (#115)"
description: "Add opt-in include_agent_skills staging to StandaloneBuilder and vtb build --include-agent-skills, refusing Vertebra's own repo .agents/ and anchoring relative skill lookup."
status: stable
state: completed
commit: 7a5cbc61
last_verified_commit: 7a5cbc61
last_verified_at: "2026-11-13T05:08:00Z"
verification_evidence: "cargo fmt/clippy/build + readelf 0 rpath + ruff + mypy + pytest (121 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/build/packaging/standalone.py
  - src/py/vertebra/cli/build.py
  - src/py/vertebra/agent/mcp/skills.py
tests:
  - src/py/tests/unit/build/test_packaging_and_pgo.py
read_set:
  - src/py/vertebra/build/packaging/standalone.py
  - src/py/vertebra/build/packaging/onefile.py
  - src/py/vertebra/cli/build.py
  - src/py/vertebra/agent/mcp/skills.py
  - src/py/tests/unit/build/test_packaging_and_pgo.py
estimated_loc: 140
tags:
  - packaging
  - standalone
  - skills
  - cli
sources:
  - id: standalone-py
    resource: "src/py/vertebra/build/packaging/standalone.py::StandaloneBuilder"
    title: Standalone staging loop skipping dot-prefixed entries
  - id: cli-build-py
    resource: "src/py/vertebra/cli/build.py::build"
    title: vtb build --standalone and --onefile CLI options
  - id: skills-py
    resource: "src/py/vertebra/agent/mcp/skills.py::MCPSkillsConfig"
    title: Default .agents/skills lookup path
---

# Task `t04`: Stage Opt-In User .agents/skills in StandaloneBuilder (#115)

## Objective & Scope Boundary

Resolve [#115](https://github.com/cofin/vertebra/issues/115) with opt-in user `.agents/skills` staging in `StandaloneBuilder` (automatically inherited by `OnefileBuilder`) per [`../spec.md` §2](../spec.md) [^standalone-py] [^cli-build-py] [^skills-py].

## Bounded Read Manifest

- `src/py/vertebra/build/packaging/standalone.py` & `onefile.py` — directory staging and `__main__.py` bootstrap.
- `src/py/vertebra/cli/build.py` — `--include-agent-skills` flag wiring.
- `src/py/vertebra/agent/mcp/skills.py` — `FilesystemSkillSource` relative path resolution against `VERTEBRA_APP_ROOT` when set.

## Contracts & Edge Cases

- `StandaloneBuilder(..., include_agent_skills: bool = False)` defaults to `False` (opt-in via `vtb build --include-agent-skills`).
- When `include_agent_skills=True`: raise `ValueError` if `project_root` is Vertebra's own repository (`(project_root / 'src/rs/vertebra-core').exists()` and `(project_root / 'src/py/vertebra').exists()`); otherwise copy `(project_root / '.agents' / 'skills')` (when present) to `app_stage / '.agents' / 'skills'`, never copying sibling `.agents/*` subdirectories, and record `'agent_skills_staged': True` in `manifest.json`.
- `_render_bootstrap` sets `os.environ.setdefault('VERTEBRA_APP_ROOT', str(app_dir))`, and `MCPSkillsConfig` / `FilesystemSkillSource` resolves relative skill paths against `VERTEBRA_APP_ROOT` when set so standalone and `OnefileBuilder` binaries work from any working directory.

## Test Matrix

- Deterministic `tmp_path` tests in `test_packaging_and_pgo.py` verifying: default `include_agent_skills=False` omits `.agents`; `include_agent_skills=True` stages `.agents/skills/demo/SKILL.md` while excluding sibling `.agents/bundles` and `.agents/worktrees`; invoking against a simulated Vertebra repo layout raises `ValueError`; and running `__main__.py` / `OnefileBuilder` archive from an external `cwd` resolves the staged skill.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/build/packaging/standalone.py src/py/vertebra/cli/build.py src/py/vertebra/agent/mcp/skills.py && uv run --no-sync mypy src/py/vertebra/build/packaging/standalone.py src/py/vertebra/cli/build.py src/py/vertebra/agent/mcp/skills.py && uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q
```

## Acceptance Criteria

- [x] User `.agents/skills` stage deterministically when opted in, and sibling `.agents/*` state is never bundled.
- [x] Vertebra's own repository `.agents/` is deterministically rejected when `include_agent_skills=True`.

[^standalone-py]: `src/py/vertebra/build/packaging/standalone.py::StandaloneBuilder` — Standalone staging loop skipping dot-prefixed entries.
[^cli-build-py]: `src/py/vertebra/cli/build.py::build` — vtb build --standalone and --onefile CLI options.
[^skills-py]: `src/py/vertebra/agent/mcp/skills.py::MCPSkillsConfig` — Default .agents/skills lookup path.
