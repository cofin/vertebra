---
type: Task
id: "mcp-protocol-completeness:ch06:t04"
chapter_id: ch06-skills-extension
title: Wire skills through Flask and Django mounts
description: Add skills parameters to Flask and Django attach_mcp_routes and Django dispatch_mcp_request, pass the coerced catalog to MCPCore, keep the catalog across Django dispatch cache rebuilds, and render well-known skill names.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch06:t03"
read_set:
  - src/py/vertebra/extensions/flask/mcp.py
  - src/py/vertebra/extensions/django/mcp.py
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/agent/mcp/skills.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/extensions/flask/mcp.py
  - src/py/vertebra/extensions/django/mcp.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 110
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: flask-mount
    resource: src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes
    title: Flask mount
  - id: django-mount
    resource: src/py/vertebra/extensions/django/mcp.py::dispatch_mcp_request
    title: Django dispatch cache keyed only by config
tags:
  - skills
  - wiring
---

# Task t04: Wire Skills through Flask and Django Mounts

## Objective & Scope Boundary

WSGI-framework users enable skills the same way as ASGI users [^flask-mount] [^django-mount]. `dispatch_mcp_request` currently rebuilds its cached `WsgiMcpApp` whenever the config differs, which would drop any catalog. Out of scope: `ch07` validation calls in these files.

## Bounded Read Manifest

- `src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes`.
- `src/py/vertebra/extensions/django/mcp.py::dispatch_mcp_request`, `attach_mcp_routes`.
- `src/py/vertebra/agent/mcp/routes.py::render_well_known_mcp_json` (`skill_names` from `t03`).
- `src/py/vertebra/agent/mcp/skills.py::SkillCatalog.coerce`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- Flask: `attach_mcp_routes(app: Any, config: MCPConfig | None = None, *, skills: MCPSkillsConfig | SkillCatalog | None = None) -> MCPRegistry`; core built with `skill_catalog=SkillCatalog.coerce(skills)`; `app.extensions["vertebra_mcp_app"]` unchanged; well-known view renders `skill_names` per request when a catalog is present.
- Django: `attach_mcp_routes(compiler_or_views=None, config=None, *, skills=None) -> tuple[MCPRegistry, dict[str, Response]]` and `dispatch_mcp_request(registry, request, config=None, *, skills=None)`; the cached app is rebuilt only when `config` or the catalog identity changes; when rebuilding without an explicit `skills`, the previously cached catalog is reused; static well-known bytes include skill names at attach time.

## Edge Cases & Error Semantics

- `skills=None` keeps current behaviour.
- Passing a different `SkillCatalog` instance to `dispatch_mcp_request` replaces the cached app.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_skill_wiring_wsgi_mounts`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Flask | `_DummyFlask` from the existing test; `attach_flask_mcp(app, skills=SkillCatalog([Skill.build("lint", "Lint", body="b")]))`; `app.extensions["vertebra_mcp_app"].core.handle(<skills/list modern>)` | One item `lint` |
| Flask well-known | Call `flask_views["vertebra_mcp_well_known"]()` | Decoded `skills == ["lint"]` |
| Django attach | `attach_django_mcp([], skills=SkillCatalog([...lint...]))` | Static well-known `skills == ["lint"]`; `registry._mcp_app.core.skill_catalog is not None` |
| Django dispatch keeps catalog | `dispatch_mcp_request(registry, <bytes skills/list>, config=MCPConfig(server_name="other"))` | Response lists `lint` |
| Django new catalog | `dispatch_mcp_request(registry, <bytes>, skills=SkillCatalog([...docs...]))` | Response lists `docs` only |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "skill_wiring_wsgi"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/flask/mcp.py src/py/vertebra/extensions/django/mcp.py && uv run --no-sync mypy src/py/vertebra/extensions/flask/mcp.py src/py/vertebra/extensions/django/mcp.py`.

## Acceptance Criteria

- [ ] R12.4: Flask and Django skill wiring matches the matrix and survives Django cache rebuilds.

## Notes & Discoveries

- [2026-11-12 00:30] Django's static well-known bytes are rendered once at attach time; catalog refreshes after attach are not reflected there (recorded in the limitations chapter).

[^flask-mount]: `src/py/vertebra/extensions/flask/mcp.py::attach_mcp_routes` — Flask mount.
[^django-mount]: `src/py/vertebra/extensions/django/mcp.py::dispatch_mcp_request` — Django dispatch cache keyed only by config.
