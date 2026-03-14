# Chapter 9: Frontend Integration
## Vertebra 0.1.0 Ghost Framework Rewrite

**Beads ID:** vertebra-w4i.9
**Flow ID:** frontend-integration
**Depends on:** Ch3 (project-buildout — CLI), Ch6 (native-ffi — server)
**Consolidates:** build-orchestrator, type-bridge-codegen, hmr-dx-experience, frontend-integration stubs

---

## 1. Overview and Goals

Chapter 9 delivers the complete frontend tooling surface for Vertebra 0.1.0. It connects Python-defined types and routes to the browser, enables Rolldown/Vite builds driven from `pyproject.toml`, and provides a Rust-backed WebSocket HMR channel so frontend dev servers receive live signals when Python types change.

The four constituent subsystems are:

1. **Build Orchestrator** — Python orchestration layer that discovers and invokes Rolldown or Vite builds. Reads `[tool.vertebra.build]` from `pyproject.toml`. Exposes `build()` and `dev()` entry points. Wired into `vertebra build` CLI command.
2. **Type Bridge and Codegen** — Extends the existing `TypeExtractor` / `TypeScriptGenerator` pipeline with a new `ZodEmitter`, a `RouteManifest` generator, and a unified `BuildOrchestrator` class in `src/py/vertebra/core/build/orchestrator.py` that drives type generation as part of the build pipeline.
3. **HMR Signaling** — A Rust WebSocket server (`src/rs/vertebra-server/src/hmr.rs`) listening on `/__vertebra/hmr`. Python sends an IPC "reload" signal (via a Unix domain socket or shared-memory flag) whenever types are regenerated; the Rust endpoint broadcasts that signal to connected browser clients.
4. **pyproject.toml Integration** — All configuration lives in `[tool.vertebra.build]`. No external config file is needed.

### Non-goals

- Bundling the Rolldown or Vite binary inside the Vertebra wheel (they are external dev dependencies).
- Replacing the existing `TypeBridge` / `TypeBridgeApp` ASGI endpoints (those remain for runtime type serving).
- Implementing `npm:` resolution inside `vertebra-js` (that is future work per the Litecore research).
- Subinterpreter mode (tracked separately).

---

## 2. Architecture

### 2.1 Build pipeline

```
pyproject.toml
  [tool.vertebra.build]
        │
        ▼
BuildConfig.load()           ← src/py/vertebra/core/build/config.py (extended)
        │
        ▼
BuildOrchestrator            ← src/py/vertebra/core/build/orchestrator.py  (NEW)
  ├── generate_types()
  │     └── TypeExtractor → ObjectType → TypeScriptGenerator → .d.ts
  │                                    → ZodEmitter → zod schemas
  │                                    → RouteManifest → manifest.json
  ├── run_bundler()          ← spawns rolldown / vite subprocess
  └── dev()                 ← starts watcher + dev server proxy
```

### 2.2 HMR signal path

```
Python watcher (watchfiles/watchdog)
        │  file change detected
        ▼
TypeBridgeWatcher.invalidate()
        │  writes 1-byte flag to Unix socket / named pipe
        ▼
Rust HMR endpoint (/__vertebra/hmr)
        │  reads flag, broadcasts WS message
        ▼
Vite / Rolldown dev server plugin  ← listens on /__vertebra/hmr
        │  "vertebra:types-changed" event
        ▼
Browser HMR runtime               ← re-fetches /__vertebra/types.d.ts
```

### 2.3 Module layout (new files only)

```
src/py/vertebra/
  core/
    build/
      orchestrator.py          NEW  BuildOrchestrator class
      plugins/
        __init__.py            NEW
        rolldown.py            NEW  RolldownPlugin
        vite.py                NEW  ViteCompatPlugin
    codegen/
      emitters/
        __init__.py            NEW
        typescript.py          NEW  TypeScriptEmitter (wraps TypeScriptGenerator)
        zod.py                 NEW  ZodEmitter
      manifest.py              NEW  RouteManifest generator

src/rs/
  vertebra-server/
    src/
      hmr.rs                   NEW  WebSocket HMR endpoint

src/py/tests/unit/codegen/
  test_codegen_extractor.py    NEW
  test_codegen_typescript.py   NEW
  test_codegen_zod.py          NEW
  test_codegen_manifest.py     NEW
  test_codegen_watch.py        NEW

src/py/tests/unit/build/
  test_build_orchestrator.py   NEW
  __init__.py                  NEW
```

---

## 3. Phase 1: Build Orchestrator

### 3.1 Extended `BuildConfig`

The existing `src/py/vertebra/core/build/config.py` must be extended to carry frontend build fields.

**File: `src/py/vertebra/core/build/config.py` (complete replacement)**

```python
"""Build configuration for Vertebra applications."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TargetConfig:
    """Per-target build profile override."""

    profile: str | None = None


@dataclass
class BuildConfig:
    """Parsed [tool.vertebra.build] configuration block."""

    # Existing fields (preserved)
    app: str | None = None
    profile: str = "modern"
    targets: dict[str, TargetConfig] = field(default_factory=dict)

    # Frontend build fields (new)
    entry: str | None = None
    outdir: str = "static/dist"
    codegen: bool = False
    codegen_output: str = "src/frontend/generated"
    bundler: str = "rolldown"   # "rolldown" | "vite"
    dev_port: int = 5173
    plugins: list[str] = field(default_factory=list)

    @classmethod
    def load(cls, project_dir: Path) -> "BuildConfig":
        """Load config from pyproject.toml in project_dir."""
        pyproject = project_dir / "pyproject.toml"
        if not pyproject.exists():
            return cls()

        with pyproject.open("rb") as f:
            data = tomllib.load(f)

        tool = data.get("tool", {}).get("vertebra", {})
        build_section = tool.get("build", {})

        targets: dict[str, TargetConfig] = {}
        for name, cfg in build_section.get("targets", {}).items():
            targets[name] = TargetConfig(profile=cfg.get("profile"))

        return cls(
            app=tool.get("app"),
            profile=tool.get("profile", "modern"),
            targets=targets,
            entry=build_section.get("entry"),
            outdir=build_section.get("outdir", "static/dist"),
            codegen=bool(build_section.get("codegen", False)),
            codegen_output=build_section.get("codegen_output", "src/frontend/generated"),
            bundler=build_section.get("bundler", "rolldown"),
            dev_port=int(build_section.get("dev_port", 5173)),
            plugins=list(build_section.get("plugins", [])),
        )
```

**Pass criteria:** `BuildConfig.load()` parses a pyproject.toml containing the full `[tool.vertebra.build]` block and all fields are populated correctly. Unknown fields are silently ignored.
**Fail criteria:** `KeyError`, `AttributeError`, or `TypeError` on valid pyproject input.

### 3.2 Plugin interfaces

**File: `src/py/vertebra/core/build/plugins/__init__.py`**

```python
"""Plugin interfaces for Vertebra build system."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from vertebra.core.build.config import BuildConfig


class BuildPlugin(ABC):
    """Abstract base class for build system plugins."""

    @abstractmethod
    def name(self) -> str:
        """Human-readable plugin name."""

    @abstractmethod
    def pre_build(self, config: "BuildConfig", project_dir: "Path") -> None:
        """Called before the bundler subprocess is invoked."""

    @abstractmethod
    def post_build(self, config: "BuildConfig", project_dir: "Path") -> None:
        """Called after the bundler subprocess completes successfully."""


__all__ = ("BuildPlugin",)
```

**File: `src/py/vertebra/core/build/plugins/rolldown.py`**

```python
"""Rolldown bundler plugin for Vertebra builds."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from vertebra.core.build.plugins import BuildPlugin

if TYPE_CHECKING:
    from pathlib import Path

    from vertebra.core.build.config import BuildConfig

logger = logging.getLogger(__name__)


class RolldownPlugin(BuildPlugin):
    """Plugin that validates Rolldown is available and sets env vars."""

    def name(self) -> str:
        return "rolldown"

    def pre_build(self, config: "BuildConfig", project_dir: "Path") -> None:
        import shutil

        if not shutil.which("rolldown"):
            logger.warning(
                "rolldown binary not found on PATH; "
                "install with: npm install -g rolldown"
            )

    def post_build(self, config: "BuildConfig", project_dir: "Path") -> None:
        outdir = project_dir / config.outdir
        if not outdir.exists():
            logger.warning("rolldown outdir %s does not exist after build", outdir)
```

**File: `src/py/vertebra/core/build/plugins/vite.py`**

```python
"""Vite compatibility plugin for Vertebra builds."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from vertebra.core.build.plugins import BuildPlugin

if TYPE_CHECKING:
    from pathlib import Path

    from vertebra.core.build.config import BuildConfig

logger = logging.getLogger(__name__)


class ViteCompatPlugin(BuildPlugin):
    """Plugin that ensures a Vite-compatible config is present in dev mode."""

    def name(self) -> str:
        return "vite"

    def pre_build(self, config: "BuildConfig", project_dir: "Path") -> None:
        import shutil

        if not shutil.which("vite"):
            logger.warning(
                "vite binary not found on PATH; "
                "install with: npm install -g vite"
            )

    def post_build(self, config: "BuildConfig", project_dir: "Path") -> None:
        pass
```

**Pass criteria:** Both plugins can be instantiated and `name()` / `pre_build()` / `post_build()` are callable without error even when the underlying binary is absent.
**Fail criteria:** ImportError, AttributeError, or hard crash when binary is not on PATH.

### 3.3 `BuildOrchestrator`

**File: `src/py/vertebra/core/build/orchestrator.py`**

```python
"""BuildOrchestrator: coordinates type codegen and frontend bundling."""

from __future__ import annotations

import importlib
import logging
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from vertebra.core.build.config import BuildConfig

if TYPE_CHECKING:
    from vertebra.core.build.plugins import BuildPlugin

logger = logging.getLogger(__name__)


class BuildOrchestrator:
    """Orchestrate frontend builds, type codegen, and dev mode for Vertebra.

    Responsibilities:
    - Load BuildConfig from pyproject.toml
    - Run type extraction and TypeScript / Zod / manifest codegen
    - Invoke Rolldown or Vite via subprocess
    - Wire the BuildPlugin lifecycle (pre_build / post_build)
    """

    def __init__(
        self,
        project_dir: Path,
        config: BuildConfig | None = None,
        *,
        plugins: list["BuildPlugin"] | None = None,
    ) -> None:
        self.project_dir = project_dir.resolve()
        self.config = config or BuildConfig.load(self.project_dir)
        self._plugins: list[BuildPlugin] = plugins or self._default_plugins()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self) -> None:
        """Run a production frontend build (Rolldown preferred)."""
        logger.info("Starting Vertebra frontend build (bundler=%s)", self.config.bundler)
        self._run_plugins_pre()
        if self.config.codegen:
            self._run_codegen()
        self._run_bundler(dev=False)
        self._run_plugins_post()
        logger.info("Frontend build complete.")

    def dev(self) -> None:
        """Start dev mode: type watcher + bundler dev server."""
        logger.info("Starting Vertebra dev mode (bundler=%s)", self.config.bundler)
        self._run_plugins_pre()
        if self.config.codegen:
            self._run_codegen()
        self._run_bundler(dev=True)

    def codegen_only(self) -> Path:
        """Run only the type codegen step and return the output directory."""
        output_dir = self.project_dir / self.config.codegen_output
        output_dir.mkdir(parents=True, exist_ok=True)
        self._run_codegen(output_dir=output_dir)
        return output_dir

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _default_plugins(self) -> list["BuildPlugin"]:
        if self.config.bundler == "rolldown":
            from vertebra.core.build.plugins.rolldown import RolldownPlugin

            return [RolldownPlugin()]
        from vertebra.core.build.plugins.vite import ViteCompatPlugin

        return [ViteCompatPlugin()]

    def _run_plugins_pre(self) -> None:
        for plugin in self._plugins:
            try:
                plugin.pre_build(self.config, self.project_dir)
            except Exception:
                logger.exception("Plugin %s pre_build failed", plugin.name())

    def _run_plugins_post(self) -> None:
        for plugin in self._plugins:
            try:
                plugin.post_build(self.config, self.project_dir)
            except Exception:
                logger.exception("Plugin %s post_build failed", plugin.name())

    def _run_codegen(self, output_dir: Path | None = None) -> None:
        """Extract types, emit TypeScript declarations, Zod schemas, and route manifest."""
        if not self.config.app:
            logger.warning("codegen=true but [tool.vertebra].app is not set; skipping")
            return

        # Import app module so @procedure decorators register themselves
        module_path, _, _ = self.config.app.partition(":")
        try:
            importlib.import_module(module_path)
        except ImportError:
            logger.exception("Could not import app module %r; skipping codegen", module_path)
            return

        from vertebra.core.codegen.emitters.typescript import TypeScriptEmitter
        from vertebra.core.codegen.emitters.zod import ZodEmitter
        from vertebra.core.codegen.manifest import RouteManifest
        from vertebra.core.procedures.registry import ProcedureRegistry

        registry = ProcedureRegistry.instance()
        output = output_dir or (self.project_dir / self.config.codegen_output)
        output.mkdir(parents=True, exist_ok=True)

        ts_emitter = TypeScriptEmitter()
        ts_content = ts_emitter.emit(registry.list())
        (output / "types.d.ts").write_text(ts_content, encoding="utf-8")
        logger.info("Wrote %s/types.d.ts", output)

        zod_emitter = ZodEmitter()
        zod_content = zod_emitter.emit(registry.list())
        (output / "schemas.ts").write_text(zod_content, encoding="utf-8")
        logger.info("Wrote %s/schemas.ts", output)

        manifest = RouteManifest()
        manifest_content = manifest.generate(registry.list())
        (output / "manifest.json").write_text(manifest_content, encoding="utf-8")
        logger.info("Wrote %s/manifest.json", output)

    def _run_bundler(self, *, dev: bool) -> None:
        if not self.config.entry:
            logger.info("No entry point configured; skipping bundler step.")
            return

        bundler = self.config.bundler
        entry = self.config.entry
        outdir = str(self.project_dir / self.config.outdir)

        if bundler == "rolldown":
            cmd = self._rolldown_command(entry=entry, outdir=outdir, dev=dev)
        else:
            cmd = self._vite_command(dev=dev)

        logger.info("Running: %s", " ".join(cmd))
        env = os.environ.copy()
        result = subprocess.run(cmd, cwd=self.project_dir, env=env)
        if result.returncode != 0:
            msg = f"Bundler exited with code {result.returncode}"
            raise RuntimeError(msg)

    def _rolldown_command(self, *, entry: str, outdir: str, dev: bool) -> list[str]:
        if dev:
            # Rolldown does not have a native dev server; fall back to Vite for dev
            return self._vite_command(dev=True)
        return [
            "rolldown",
            entry,
            "--dir", outdir,
            "--format", "esm",
        ]

    def _vite_command(self, *, dev: bool) -> list[str]:
        if dev:
            return ["vite", "--port", str(self.config.dev_port)]
        return ["vite", "build"]


__all__ = ("BuildOrchestrator",)
```

**Pass criteria:**
- `BuildOrchestrator(project_dir).build()` runs without error on a project with no `entry` configured (bundler step is skipped gracefully).
- `BuildOrchestrator(project_dir).codegen_only()` returns a `Path` to the output directory and writes `types.d.ts`, `schemas.ts`, and `manifest.json` when `app` is set and the module is importable.
- `_rolldown_command()` and `_vite_command()` return lists with the correct binary name and flags.

**Fail criteria:** Any uncaught exception when `entry` is missing or the app module is not importable.

### 3.4 `__init__.py` update

**File: `src/py/vertebra/core/build/__init__.py` (updated)**

```python
"""Build toolchain for Vertebra."""

from vertebra.core.build.builder import Builder
from vertebra.core.build.config import BuildConfig
from vertebra.core.build.orchestrator import BuildOrchestrator

__all__ = ("Builder", "BuildConfig", "BuildOrchestrator")
```

---

## 4. Phase 2: Type Bridge and Codegen Emitters

The existing `TypeExtractor`, `TypeNode` IR, and `TypeScriptGenerator` are already solid (see `extractor.py`, `ir.py`, `typescript.py`). This phase adds:

1. A thin `TypeScriptEmitter` wrapper that matches the new emitter interface contract.
2. A new `ZodEmitter` that converts the `ObjectType` IR into Zod schema strings.
3. A new `RouteManifest` generator that produces a typed JSON manifest from `ProcedureMetadata`.

### 4.1 Emitter interface

**File: `src/py/vertebra/core/codegen/emitters/__init__.py`**

```python
"""Codegen emitters package — TypeScript, Zod, and manifest generation."""

from vertebra.core.codegen.emitters.typescript import TypeScriptEmitter
from vertebra.core.codegen.emitters.zod import ZodEmitter

__all__ = ("TypeScriptEmitter", "ZodEmitter")
```

### 4.2 `TypeScriptEmitter` wrapper

**File: `src/py/vertebra/core/codegen/emitters/typescript.py`**

```python
"""TypeScript emitter: thin adapter over TypeScriptGenerator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.core.codegen.typescript import TypeScriptGenerator, format_typescript

if TYPE_CHECKING:
    from collections.abc import Iterable

    from vertebra.core.procedures.registry import ProcedureMetadata


class TypeScriptEmitter:
    """Emit a TypeScript declaration file from a collection of ProcedureMetadata.

    This adapter wraps TypeScriptGenerator to present the unified emitter
    interface used by BuildOrchestrator.
    """

    def emit(self, procedures: "Iterable[ProcedureMetadata]") -> str:
        """Generate a .d.ts string from registered procedures.

        Args:
            procedures: Iterable of ProcedureMetadata from ProcedureRegistry.

        Returns:
            Valid TypeScript declaration file content, prettier-formatted if
            prettier is available on PATH.
        """
        generator = TypeScriptGenerator()
        raw = generator.generate(procedures)
        return format_typescript(raw)


__all__ = ("TypeScriptEmitter",)
```

**Pass criteria:** `TypeScriptEmitter().emit([])` returns a non-empty string that starts with `// DO NOT EDIT`.
**Fail criteria:** ImportError or AttributeError.

### 4.3 `ZodEmitter`

The `ZodEmitter` converts `ObjectType` IR nodes (from the `ProcedureRegistry`) into Zod schema declarations. It walks the same type graph as `TypeScriptGenerator` but emits Zod `z.object()` / `z.string()` / `z.number()` / etc.

**File: `src/py/vertebra/core/codegen/emitters/zod.py`**

```python
"""Zod schema emitter for Vertebra type bridge."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.core.codegen.ir import (
    AnnotatedType,
    ArrayType,
    GenericType,
    LiteralType,
    NamedTypeRef,
    ObjectType,
    PrimitiveType,
    RawType,
    StreamType,
    SubscriptionType,
    TupleType,
    TypeNode,
    UnionType,
    UnknownType,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from vertebra.core.procedures.registry import ProcedureMetadata


class ZodEmitter:
    """Emit Zod schema declarations from ProcedureMetadata.

    Produces a TypeScript module that can be imported in the frontend for
    runtime request/response validation.

    Example output:
        import { z } from "zod";

        export const UserSchema = z.object({
          id: z.number(),
          name: z.string(),
          email: z.string().optional(),
        });
    """

    def emit(self, procedures: "Iterable[ProcedureMetadata]") -> str:
        """Generate a Zod schema module from registered procedures.

        Args:
            procedures: Iterable of ProcedureMetadata from ProcedureRegistry.

        Returns:
            TypeScript source string with Zod schema declarations.
        """
        declared: dict[str, ObjectType] = {}
        procedures_list = list(procedures)

        # Collect all named object types referenced by procedures
        for procedure in procedures_list:
            for node in list(procedure.params.values()) + [procedure.return_type]:
                self._collect_named(node, declared)

        lines: list[str] = [
            "// DO NOT EDIT: generated by Vertebra type bridge.",
            'import { z } from "zod";',
            "",
        ]

        # Emit named schemas first (topologically sorted for forward refs)
        for name in self._toposort(declared):
            obj = declared[name]
            lines.append(self._render_object_schema(name, obj))
            lines.append("")

        # Emit procedure-level request/response schemas
        for procedure in procedures_list:
            safe_id = procedure.procedure_id.replace(".", "_")
            params_obj = ObjectType(fields=dict(procedure.params))
            lines.append(
                f"export const {safe_id}ParamsSchema = {self._node_to_zod(params_obj)};"
            )
            lines.append(
                f"export const {safe_id}ResponseSchema = {self._node_to_zod(procedure.return_type)};"
            )
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _collect_named(self, node: TypeNode, declared: dict[str, ObjectType]) -> None:
        if isinstance(node, ObjectType) and node.name and node.name not in declared:
            declared[node.name] = node
        for child in node.iter_children():
            self._collect_named(child, declared)

    def _toposort(self, declared: dict[str, ObjectType]) -> list[str]:
        graph: dict[str, set[str]] = {}
        for name, obj in declared.items():
            deps: set[str] = set()
            for child in obj.iter_children():
                deps.update(self._named_deps(child))
            deps.discard(name)
            graph[name] = deps

        ordered: list[str] = []
        ready = sorted(n for n, deps in graph.items() if not deps)
        while ready:
            name = ready.pop(0)
            ordered.append(name)
            for other, deps in graph.items():
                if name in deps:
                    deps.discard(name)
                    if not deps and other not in ordered and other not in ready:
                        ready.append(other)
                        ready.sort()
        remaining = sorted(n for n in graph if n not in ordered)
        ordered.extend(remaining)
        return ordered

    def _named_deps(self, node: TypeNode) -> set[str]:
        deps: set[str] = set()
        if isinstance(node, ObjectType) and node.name:
            deps.add(node.name)
        if isinstance(node, NamedTypeRef):
            deps.add(node.name)
        for child in node.iter_children():
            deps.update(self._named_deps(child))
        return deps

    def _render_object_schema(self, name: str, obj: ObjectType) -> str:
        inner = self._object_fields_zod(obj)
        return f"export const {name}Schema = {inner};"

    def _object_fields_zod(self, obj: ObjectType) -> str:
        lines = ["z.object({"]
        for field_name, field_type in obj.fields.items():
            zod_type = self._node_to_zod(field_type)
            if field_name in obj.optional_fields:
                zod_type = f"{zod_type}.optional()"
            lines.append(f"  {field_name}: {zod_type},")
        lines.append("})")
        return "\n".join(lines)

    def _node_to_zod(self, node: TypeNode) -> str:  # noqa: PLR0911
        if isinstance(node, PrimitiveType):
            return self._primitive_to_zod(node.kind)
        if isinstance(node, ArrayType):
            return f"z.array({self._node_to_zod(node.item)})"
        if isinstance(node, ObjectType):
            if node.name:
                return f"{node.name}Schema"
            return self._object_fields_zod(node)
        if isinstance(node, UnionType):
            variants = [self._node_to_zod(v) for v in node.variants]
            return f"z.union([{', '.join(variants)}])"
        if isinstance(node, LiteralType):
            if node.value is None:
                return "z.null()"
            if isinstance(node.value, bool):
                return f"z.literal({str(node.value).lower()})"
            if isinstance(node.value, str):
                escaped = node.value.replace("\\", "\\\\").replace('"', '\\"')
                return f'z.literal("{escaped}")'
            return f"z.literal({node.value})"
        if isinstance(node, GenericType):
            if node.name in ("Set",):
                return f"z.set({self._node_to_zod(node.params[0])})"
            if node.name == "Record":
                key = self._node_to_zod(node.params[0]) if node.params else "z.string()"
                val = self._node_to_zod(node.params[1]) if len(node.params) > 1 else "z.unknown()"
                return f"z.record({key}, {val})"
            return "z.unknown()"
        if isinstance(node, TupleType):
            items = ", ".join(self._node_to_zod(item) for item in node.items)
            return f"z.tuple([{items}])"
        if isinstance(node, NamedTypeRef):
            return f"{node.name}Schema"
        if isinstance(node, AnnotatedType):
            return self._node_to_zod(node.base)
        if isinstance(node, (StreamType, SubscriptionType)):
            return f"z.array({self._node_to_zod(node.item)})"
        if isinstance(node, (RawType, UnknownType)):
            return "z.unknown()"
        return "z.unknown()"

    def _primitive_to_zod(self, kind: str) -> str:
        mapping = {
            "string": "z.string()",
            "number": "z.number()",
            "boolean": "z.boolean()",
            "null": "z.null()",
            "Uint8Array": "z.instanceof(Uint8Array)",
            "unknown": "z.unknown()",
        }
        return mapping.get(kind, "z.unknown()")


__all__ = ("ZodEmitter",)
```

**Pass criteria:**
- `ZodEmitter().emit([])` returns a string containing `import { z } from "zod"`.
- A procedure with a `PrimitiveType("string")` return type produces `z.string()` in the output.
- A procedure with an `ObjectType(fields={"id": PrimitiveType("number")}, name="User")` produces `UserSchema = z.object({ id: z.number(), })`.
- Optional fields produce `.optional()` suffix.

**Fail criteria:** Any `KeyError`, `AttributeError`, or `RecursionError` on the standard IR node types.

### 4.4 `RouteManifest`

**File: `src/py/vertebra/core/codegen/manifest.py`**

```python
"""Route manifest generator for Vertebra type bridge."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from vertebra.core.procedures.registry import ProcedureMetadata


class RouteEntry:
    """One entry in the route manifest."""

    __slots__ = ("procedure_id", "route", "method", "params_schema", "response_schema")

    def __init__(
        self,
        procedure_id: str,
        route: str | None,
        method: str | None,
        params_schema: str,
        response_schema: str,
    ) -> None:
        self.procedure_id = procedure_id
        self.route = route
        self.method = method
        self.params_schema = params_schema
        self.response_schema = response_schema

    def to_dict(self) -> dict[str, object]:
        return {
            "procedureId": self.procedure_id,
            "route": self.route,
            "method": self.method or "POST",
            "paramsSchema": self.params_schema,
            "responseSchema": self.response_schema,
        }


class RouteManifest:
    """Generate a JSON route manifest from registered procedures.

    The manifest maps procedure IDs to typed client method descriptors.
    Frontend build tools consume this file to generate typed fetch clients
    without manual URL construction.

    Manifest format:
    {
      "version": 1,
      "routes": {
        "user.create": {
          "procedureId": "user.create",
          "route": "/api/users",
          "method": "POST",
          "paramsSchema": "user_createParamsSchema",
          "responseSchema": "user_createResponseSchema"
        }
      }
    }
    """

    def generate(self, procedures: "Iterable[ProcedureMetadata]") -> str:
        """Generate the manifest JSON string.

        Args:
            procedures: Iterable of ProcedureMetadata from ProcedureRegistry.

        Returns:
            Indented JSON string.
        """
        routes: dict[str, object] = {}
        for procedure in procedures:
            safe_id = procedure.procedure_id.replace(".", "_")
            entry = RouteEntry(
                procedure_id=procedure.procedure_id,
                route=procedure.route,
                method=procedure.method,
                params_schema=f"{safe_id}ParamsSchema",
                response_schema=f"{safe_id}ResponseSchema",
            )
            routes[procedure.procedure_id] = entry.to_dict()

        manifest = {
            "version": 1,
            "generated": True,
            "routes": routes,
        }
        return json.dumps(manifest, indent=2)


__all__ = ("RouteEntry", "RouteManifest")
```

**Pass criteria:**
- `RouteManifest().generate([])` returns `{"version": 1, "generated": true, "routes": {}}` (valid JSON).
- A procedure with `procedure_id="user.create"`, `route="/api/users"`, `method="POST"` produces a manifest entry with correct `procedureId`, `route`, `method`, `paramsSchema`, and `responseSchema` keys.
- `paramsSchema` is the camel-to-underscore version: `user_createParamsSchema`.

**Fail criteria:** Invalid JSON output or `KeyError` on standard `ProcedureMetadata` fields.

---

## 5. Phase 3: HMR Signaling

### 5.1 Rust WebSocket endpoint

The HMR endpoint is a lightweight WebSocket server embedded in `vertebra-server`. It listens on `/__vertebra/hmr`. When the Python side sends a signal (via a Unix domain socket or a simple `POST /__vertebra/hmr/signal` on loopback), the Rust endpoint broadcasts a `{"type":"vertebra:types-changed"}` JSON message to all connected WebSocket clients.

**File: `src/rs/vertebra-server/src/hmr.rs`**

```rust
//! HMR (Hot Module Replacement) WebSocket endpoint for Vertebra dev mode.
//!
//! Listens on `/__vertebra/hmr`. Broadcasts `{"type":"vertebra:types-changed"}`
//! to all connected clients when signalled by the Python watcher.
//!
//! Signal mechanism: a `POST /__vertebra/hmr/signal` request on loopback
//! triggers a broadcast. The Python watcher calls this after regenerating types.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode};
use tokio::sync::{broadcast, RwLock};

/// Shared HMR state passed into the request handler.
#[derive(Clone)]
pub struct HmrState {
    /// Broadcast sender — one message fans out to all connected WS clients.
    pub sender: broadcast::Sender<String>,
    /// Connection counter for logging.
    pub connection_count: Arc<AtomicU64>,
}

impl HmrState {
    /// Create a new HmrState with the given broadcast channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            connection_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Send a types-changed notification to all connected clients.
    pub fn signal_types_changed(&self) {
        let msg = r#"{"type":"vertebra:types-changed"}"#.to_string();
        // Ignore send errors — they only occur when there are no subscribers.
        let _ = self.sender.send(msg);
    }
}

/// Handle `POST /__vertebra/hmr/signal` — triggers a HMR broadcast.
///
/// This is called by the Python watcher after regenerating types.
pub async fn handle_signal(
    _req: Request<hyper::body::Incoming>,
    state: Arc<HmrState>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    state.signal_types_changed();
    let count = state.connection_count.load(Ordering::Relaxed);
    let body = format!(
        r#"{{"ok":true,"clients":{}}}"#,
        count
    );
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap())
}

/// Handle `GET /__vertebra/hmr` WebSocket upgrade.
///
/// For each connected client, spawns a task that:
/// 1. Sends an initial `{"type":"vertebra:connected"}` keepalive.
/// 2. Relays each broadcast message from `HmrState.sender` to the client.
/// 3. Exits cleanly on client disconnect or channel lag.
pub async fn handle_hmr_upgrade(
    req: Request<hyper::body::Incoming>,
    state: Arc<HmrState>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    // Real WebSocket upgrade requires the `tokio-tungstenite` or similar crate.
    // This function is a placeholder that returns 426 Upgrade Required when
    // the Upgrade header is absent (e.g. plain HTTP health checks), and
    // returns 101 Switching Protocols for actual WS upgrade requests.
    //
    // Full implementation is deferred to the vertebra-server crate once
    // `tokio-tungstenite` is added to Cargo.toml.
    let is_upgrade = req
        .headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);

    if !is_upgrade {
        // Health / status check — return 200 with connection count.
        let count = state.connection_count.load(Ordering::Relaxed);
        let body = format!(
            r#"{{"endpoint":"/__vertebra/hmr","clients":{}}}"#,
            count
        );
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap());
    }

    // Placeholder: signal the upgrade path is recognised.
    // Full WebSocket frame handling added in the implementation task.
    state.connection_count.fetch_add(1, Ordering::Relaxed);

    Ok(Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header("upgrade", "websocket")
        .body(Full::new(Bytes::new()))
        .unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmr_state_signal_with_no_subscribers() {
        let state = HmrState::new(16);
        // Should not panic even with zero subscribers.
        state.signal_types_changed();
    }

    #[test]
    fn hmr_state_broadcast_received_by_subscriber() {
        let state = HmrState::new(16);
        let mut rx = state.sender.subscribe();
        state.signal_types_changed();
        let msg = rx.try_recv().expect("should receive broadcast");
        assert!(msg.contains("vertebra:types-changed"));
    }
}
```

**Integration into `vertebra-server/src/lib.rs`:**

The `handle_request` function must route `/__vertebra/hmr` and `/__vertebra/hmr/signal` before the IPC dispatch path. The `ServerState` must carry `hmr: Arc<HmrState>`. The Cargo.toml for `vertebra-server` must add a `tokio-tungstenite` dependency once full WS frames are needed.

The routing addition is:

```rust
// In handle_request, before the IPC dispatch block:
if path == "/__vertebra/hmr/signal" && req.method() == hyper::Method::POST {
    return hmr::handle_signal(req, state.hmr.clone()).await;
}
if path == "/__vertebra/hmr" {
    return hmr::handle_hmr_upgrade(req, state.hmr.clone()).await;
}
```

**Pass criteria (Rust tests):**
- `hmr_state_signal_with_no_subscribers` passes: no panic.
- `hmr_state_broadcast_received_by_subscriber` passes: message contains `vertebra:types-changed`.
- `cargo test --package vertebra-server` exits 0.

**Fail criteria:** Compilation error, panic in signal path, or missing `pub mod hmr` in `lib.rs`.

### 5.2 Python watcher integration

The existing `TypeBridgeWatcher` in `src/py/vertebra/core/codegen/watch.py` uses `watchdog`. The new `watch.py` adds a `watchfiles`-based alternative and a `HmrNotifier` that pings `/__vertebra/hmr/signal` after invalidation.

**File: `src/py/vertebra/core/codegen/watch.py` (extended — append after existing content)**

The strategy: keep the existing `TypeBridgeWatcher` as-is. Add a `HmrNotifier` class that is composed with either watcher via callback.

```python
# Append to existing watch.py — after the TypeBridgeWatcher class


class HmrNotifier:
    """Notify the Rust HMR endpoint after TypeBridge invalidation.

    Sends a POST to /__vertebra/hmr/signal on loopback so the Rust endpoint
    can broadcast to connected browser dev-server clients.

    This class is intentionally dependency-free (uses urllib.request) so it
    works even when httpx or aiohttp are absent.
    """

    def __init__(self, base_url: str = "http://127.0.0.1:8000") -> None:
        self._url = f"{base_url.rstrip('/')}/__vertebra/hmr/signal"

    def notify(self) -> None:
        """POST to the HMR signal endpoint. Errors are logged but not raised."""
        import urllib.request

        try:
            req = urllib.request.Request(  # noqa: S310
                self._url,
                data=b"",
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=1) as _resp:  # noqa: S310
                pass
        except Exception:
            import logging

            logging.getLogger(__name__).debug(
                "HMR signal to %s failed (is the server running?)", self._url
            )
```

The CLI wires `HmrNotifier` into the `TypeBridgeWatcher` by registering `notifier.notify` as a callback on the bridge:

```python
bridge.add_callback(notifier.notify)
```

This requires no changes to `TypeBridge` itself — the callback mechanism already exists.

**Pass criteria:**
- `HmrNotifier("http://127.0.0.1:9999").notify()` does not raise when the server is not running (error is logged at DEBUG level only).
- When the server is running and the endpoint returns 200, the notifier makes exactly one POST to `/__vertebra/hmr/signal`.

**Fail criteria:** Any exception propagated to the caller, or `urllib.request` import error.

### 5.3 `vertebra run --dev` integration

The `run` CLI command in `src/py/vertebra/cli/run.py` must accept a `--dev` flag that:

1. Sets `VERTEBRA_DEV=1` in the subprocess environment.
2. After the server process starts, instantiates `BuildOrchestrator(project_dir).dev()` in a background thread.
3. Registers a `HmrNotifier` callback on the global `TypeBridge` instance.

The `--dev` flag is additive — it does not change the existing `run` behaviour for non-dev mode.

**CLI change (diff-style description only; full implementation in the build task):**

```python
@click.option("--dev", is_flag=True, help="Enable dev mode: HMR, type watcher, and bundler dev server")
def run(..., dev: bool) -> None:
    if dev:
        env["VERTEBRA_DEV"] = "1"
        # Start BuildOrchestrator.dev() in background thread
        import threading
        from vertebra.core.build.orchestrator import BuildOrchestrator
        t = threading.Thread(
            target=lambda: BuildOrchestrator(Path(".")).dev(),
            daemon=True,
        )
        t.start()
```

**Pass criteria:** `vertebra run myapp:app --dev` starts without error. The `--dev` flag is listed in `vertebra run --help` output.
**Fail criteria:** `SystemExit` or `ClickException` when `--dev` is passed.

---

## 6. Phase 4: pyproject.toml Integration

### 6.1 Full schema

```toml
[tool.vertebra]
app = "myapp:app"

[tool.vertebra.build]
entry = "src/frontend/main.ts"
outdir = "static/dist"
bundler = "rolldown"           # "rolldown" | "vite". Default: "rolldown"
codegen = true
codegen_output = "src/frontend/generated"
dev_port = 5173
plugins = []                   # Future: plugin names to activate

[tool.vertebra.build.targets.linux-x64]
profile = "modern"

[tool.vertebra.build.targets.darwin-arm64]
profile = "modern"
```

### 6.2 Field reference

| Field | Type | Default | Description |
|---|---|---|---|
| `entry` | string | — | Path to frontend entry point relative to project root |
| `outdir` | string | `"static/dist"` | Output directory for bundled assets |
| `bundler` | string | `"rolldown"` | Bundler to use: `rolldown` or `vite` |
| `codegen` | bool | `false` | Enable TypeScript / Zod / manifest codegen |
| `codegen_output` | string | `"src/frontend/generated"` | Output directory for generated files |
| `dev_port` | int | `5173` | Port for the Vite dev server |
| `plugins` | list[str] | `[]` | Plugin names (reserved for future use) |

### 6.3 Config loading contract

- `BuildConfig.load()` MUST return a default `BuildConfig()` instance when `pyproject.toml` is absent.
- Unknown keys in `[tool.vertebra.build]` MUST be silently ignored.
- Type coercion errors (e.g. `dev_port = "not-a-number"`) MUST raise `ValueError` with a descriptive message.

---

## 7. Tests

All tests live under `src/py/tests/unit/` and use `pytest` with the `pytest.mark.unit` marker (consistent with existing test files).

### 7.1 `test_build_orchestrator.py`

**File: `src/py/tests/unit/build/__init__.py`** — empty

**File: `src/py/tests/unit/build/test_build_orchestrator.py`**

```python
"""Tests for BuildOrchestrator and BuildConfig."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from vertebra.core.build.config import BuildConfig
from vertebra.core.build.orchestrator import BuildOrchestrator

pytestmark = pytest.mark.unit


def test_build_config_defaults() -> None:
    """BuildConfig() has sane defaults."""
    cfg = BuildConfig()
    assert cfg.bundler == "rolldown"
    assert cfg.outdir == "static/dist"
    assert cfg.codegen is False
    assert cfg.dev_port == 5173


def test_build_config_load_minimal(tmp_path: Path) -> None:
    """load() reads entry and outdir from pyproject.toml."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent("""\
            [tool.vertebra.build]
            entry = "src/main.ts"
            outdir = "dist"
        """),
        encoding="utf-8",
    )
    cfg = BuildConfig.load(tmp_path)
    assert cfg.entry == "src/main.ts"
    assert cfg.outdir == "dist"


def test_build_config_load_codegen(tmp_path: Path) -> None:
    """load() reads codegen and codegen_output fields."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent("""\
            [tool.vertebra.build]
            codegen = true
            codegen_output = "generated"
        """),
        encoding="utf-8",
    )
    cfg = BuildConfig.load(tmp_path)
    assert cfg.codegen is True
    assert cfg.codegen_output == "generated"


def test_build_config_missing_pyproject(tmp_path: Path) -> None:
    """load() returns defaults when pyproject.toml is absent."""
    cfg = BuildConfig.load(tmp_path)
    assert cfg.entry is None
    assert cfg.bundler == "rolldown"


def test_orchestrator_codegen_only_no_app(tmp_path: Path) -> None:
    """codegen_only() logs a warning and returns a path when app is not set."""
    cfg = BuildConfig(codegen=True)
    orch = BuildOrchestrator(tmp_path, config=cfg)
    out = orch.codegen_only()
    # Should not raise; output dir created even if no types generated
    assert isinstance(out, Path)


def test_orchestrator_build_no_entry(tmp_path: Path) -> None:
    """build() completes without error when entry is not configured."""
    cfg = BuildConfig(codegen=False)
    orch = BuildOrchestrator(tmp_path, config=cfg)
    orch.build()  # Should not raise


def test_rolldown_command_format(tmp_path: Path) -> None:
    """_rolldown_command returns the correct subprocess arguments."""
    cfg = BuildConfig(entry="src/main.ts", outdir="dist", bundler="rolldown")
    orch = BuildOrchestrator(tmp_path, config=cfg)
    cmd = orch._rolldown_command(entry="src/main.ts", outdir="dist", dev=False)
    assert cmd[0] == "rolldown"
    assert "--dir" in cmd
    assert "dist" in cmd


def test_vite_dev_command_format(tmp_path: Path) -> None:
    """_vite_command returns vite with --port in dev mode."""
    cfg = BuildConfig(dev_port=3000, bundler="vite")
    orch = BuildOrchestrator(tmp_path, config=cfg)
    cmd = orch._vite_command(dev=True)
    assert cmd[0] == "vite"
    assert "--port" in cmd
    assert "3000" in cmd
```

**Pass criteria:** All 8 test functions pass. No subprocess calls are made (bundler not invoked because `entry` is unset or no binary is available).
**Fail criteria:** Any `ImportError`, `AttributeError`, or subprocess invocation during the `no_entry` and `no_app` tests.

### 7.2 `test_codegen_extractor.py`

**File: `src/py/tests/unit/codegen/test_codegen_extractor.py`**

```python
"""Tests for TypeExtractor with msgspec.Struct and dataclass."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from vertebra.core.codegen.extractor import TypeExtractor
from vertebra.core.codegen.ir import ObjectType, PrimitiveType

pytestmark = pytest.mark.unit


def _write(tmp_path: Path, name: str, src: str) -> Path:
    p = tmp_path / name
    p.write_text(textwrap.dedent(src), encoding="utf-8")
    return p


def test_extract_msgspec_struct(tmp_path: Path) -> None:
    src = """\
        import msgspec

        class User(msgspec.Struct):
            id: int
            name: str
    """
    path = _write(tmp_path, "models.py", src)
    extractor = TypeExtractor()
    structs = extractor.extract_msgspec_structs(path)
    assert "User" in structs
    user = structs["User"]
    assert isinstance(user, ObjectType)
    assert "id" in user.fields
    assert "name" in user.fields


def test_extract_dataclass(tmp_path: Path) -> None:
    src = """\
        from dataclasses import dataclass

        @dataclass
        class Product:
            sku: str
            price: float
    """
    path = _write(tmp_path, "models.py", src)
    extractor = TypeExtractor()
    dcs = extractor.extract_dataclasses(path)
    assert "Product" in dcs
    product = dcs["Product"]
    assert "sku" in product.fields
    assert "price" in product.fields


def test_extract_msgspec_optional_field(tmp_path: Path) -> None:
    src = """\
        import msgspec

        class Config(msgspec.Struct):
            name: str
            debug: bool = False
    """
    path = _write(tmp_path, "models.py", src)
    extractor = TypeExtractor()
    structs = extractor.extract_msgspec_structs(path)
    config = structs["Config"]
    assert "debug" in config.optional_fields


def test_extract_dataclass_optional_field(tmp_path: Path) -> None:
    src = """\
        from dataclasses import dataclass, field

        @dataclass
        class Event:
            name: str
            tags: list[str] = field(default_factory=list)
    """
    path = _write(tmp_path, "models.py", src)
    extractor = TypeExtractor()
    dcs = extractor.extract_dataclasses(path)
    event = dcs["Event"]
    assert "tags" in event.optional_fields


def test_ast_cache_reuse(tmp_path: Path) -> None:
    src = """\
        import msgspec

        class Item(msgspec.Struct):
            value: int
    """
    path = _write(tmp_path, "models.py", src)
    extractor = TypeExtractor()
    extractor.from_source_file(path)
    digest_before = extractor.ast_cache[path].digest
    extractor.from_source_file(path)
    # Cache hit — same digest object (no re-parse)
    assert extractor.ast_cache[path].digest == digest_before


def test_from_hint_primitives() -> None:
    extractor = TypeExtractor()
    assert extractor.from_hint(int).to_typescript() == "number"
    assert extractor.from_hint(str).to_typescript() == "string"
    assert extractor.from_hint(bool).to_typescript() == "boolean"
    assert extractor.from_hint(None).to_typescript() == "null"
```

**Pass criteria:** All 6 tests pass. No file system errors.
**Fail criteria:** Any `ImportError` or `AttributeError` in the extractor.

### 7.3 `test_codegen_typescript.py`

**File: `src/py/tests/unit/codegen/test_codegen_typescript.py`**

```python
"""Tests for TypeScriptEmitter output correctness."""

from __future__ import annotations

from pathlib import Path

import pytest

from vertebra.core.codegen.emitters.typescript import TypeScriptEmitter
from vertebra.core.codegen.ir import ObjectType, PrimitiveType
from vertebra.core.procedures.registry import ProcedureMetadata, ProcedureRegistry

pytestmark = pytest.mark.unit


def _make_registry(*procedures: ProcedureMetadata) -> ProcedureRegistry:
    reg = ProcedureRegistry()
    for p in procedures:
        reg.register(p)
    return reg


def _simple_proc(proc_id: str) -> ProcedureMetadata:
    return ProcedureMetadata(
        procedure_id=proc_id,
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={"value": PrimitiveType("number")},
        return_type=PrimitiveType("string"),
        serializer="msgspec",
    )


def test_emit_returns_do_not_edit_header() -> None:
    emitter = TypeScriptEmitter()
    output = emitter.emit([])
    assert "DO NOT EDIT" in output


def test_emit_procedures_namespace() -> None:
    proc = _simple_proc("math.add")
    emitter = TypeScriptEmitter()
    output = emitter.emit([proc])
    assert "Procedures" in output
    assert "math" in output
    assert "add" in output


def test_emit_named_object_interface() -> None:
    user_type = ObjectType(
        fields={"id": PrimitiveType("number"), "name": PrimitiveType("string")},
        name="User",
    )
    proc = ProcedureMetadata(
        procedure_id="get_user",
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={},
        return_type=user_type,
        serializer="msgspec",
    )
    emitter = TypeScriptEmitter()
    output = emitter.emit([proc])
    assert "interface User" in output or "type User" in output
    assert "id:" in output
    assert "name:" in output


def test_emit_produces_valid_typescript() -> None:
    """Basic structural check: output is a single string, not empty."""
    emitter = TypeScriptEmitter()
    output = emitter.emit([_simple_proc("ping")])
    assert isinstance(output, str)
    assert len(output) > 0
```

**Pass criteria:** All 4 tests pass.
**Fail criteria:** Any exception or empty output from `TypeScriptEmitter`.

### 7.4 `test_codegen_zod.py`

**File: `src/py/tests/unit/codegen/test_codegen_zod.py`**

```python
"""Tests for ZodEmitter output correctness."""

from __future__ import annotations

from pathlib import Path

import pytest

from vertebra.core.codegen.emitters.zod import ZodEmitter
from vertebra.core.codegen.ir import (
    ArrayType,
    LiteralType,
    ObjectType,
    PrimitiveType,
    UnionType,
)
from vertebra.core.procedures.registry import ProcedureMetadata

pytestmark = pytest.mark.unit


def _make_proc(proc_id: str, **kwargs: object) -> ProcedureMetadata:
    return ProcedureMetadata(
        procedure_id=proc_id,
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params=kwargs.get("params", {}),  # type: ignore[arg-type]
        return_type=kwargs.get("return_type", PrimitiveType("null")),  # type: ignore[arg-type]
        serializer="msgspec",
    )


def test_emit_has_zod_import() -> None:
    emitter = ZodEmitter()
    output = emitter.emit([])
    assert 'from "zod"' in output


def test_primitive_string() -> None:
    proc = _make_proc("ping", return_type=PrimitiveType("string"))
    output = ZodEmitter().emit([proc])
    assert "z.string()" in output


def test_primitive_number() -> None:
    proc = _make_proc("count", return_type=PrimitiveType("number"))
    output = ZodEmitter().emit([proc])
    assert "z.number()" in output


def test_array_type() -> None:
    proc = _make_proc("list_ids", return_type=ArrayType(PrimitiveType("number")))
    output = ZodEmitter().emit([proc])
    assert "z.array(z.number())" in output


def test_named_object_schema() -> None:
    user = ObjectType(
        fields={"id": PrimitiveType("number"), "name": PrimitiveType("string")},
        name="User",
    )
    proc = _make_proc("get_user", return_type=user)
    output = ZodEmitter().emit([proc])
    assert "UserSchema" in output
    assert "z.object(" in output


def test_optional_field() -> None:
    obj = ObjectType(
        fields={"x": PrimitiveType("number")},
        name="Point",
        optional_fields={"x"},
    )
    proc = _make_proc("origin", return_type=obj)
    output = ZodEmitter().emit([proc])
    assert ".optional()" in output


def test_union_type() -> None:
    union = UnionType([PrimitiveType("string"), PrimitiveType("null")])
    proc = _make_proc("maybe", return_type=union)
    output = ZodEmitter().emit([proc])
    assert "z.union(" in output


def test_literal_type() -> None:
    lit = LiteralType("admin")
    proc = _make_proc("role", return_type=lit)
    output = ZodEmitter().emit([proc])
    assert 'z.literal("admin")' in output


def test_procedure_schemas_named() -> None:
    proc = _make_proc(
        "user.create",
        params={"name": PrimitiveType("string")},
        return_type=PrimitiveType("number"),
    )
    output = ZodEmitter().emit([proc])
    assert "user_createParamsSchema" in output
    assert "user_createResponseSchema" in output
```

**Pass criteria:** All 9 tests pass.
**Fail criteria:** Any `KeyError`, `AttributeError`, or `RecursionError`.

### 7.5 `test_codegen_manifest.py`

**File: `src/py/tests/unit/codegen/test_codegen_manifest.py`**

```python
"""Tests for RouteManifest generation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from vertebra.core.codegen.ir import PrimitiveType
from vertebra.core.codegen.manifest import RouteManifest
from vertebra.core.procedures.registry import ProcedureMetadata

pytestmark = pytest.mark.unit


def _proc(proc_id: str, route: str | None = None, method: str | None = None) -> ProcedureMetadata:
    return ProcedureMetadata(
        procedure_id=proc_id,
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={},
        return_type=PrimitiveType("null"),
        serializer="msgspec",
        route=route,
        method=method,
    )


def test_empty_manifest() -> None:
    manifest = RouteManifest()
    data = json.loads(manifest.generate([]))
    assert data["version"] == 1
    assert data["routes"] == {}


def test_route_entry_keys() -> None:
    proc = _proc("user.list", route="/api/users", method="GET")
    data = json.loads(RouteManifest().generate([proc]))
    entry = data["routes"]["user.list"]
    assert entry["procedureId"] == "user.list"
    assert entry["route"] == "/api/users"
    assert entry["method"] == "GET"


def test_default_method_is_post() -> None:
    proc = _proc("ping")
    data = json.loads(RouteManifest().generate([proc]))
    assert data["routes"]["ping"]["method"] == "POST"


def test_schema_name_dot_to_underscore() -> None:
    proc = _proc("user.create")
    data = json.loads(RouteManifest().generate([proc]))
    entry = data["routes"]["user.create"]
    assert entry["paramsSchema"] == "user_createParamsSchema"
    assert entry["responseSchema"] == "user_createResponseSchema"


def test_multiple_procedures() -> None:
    procs = [
        _proc("a.b", route="/a/b", method="GET"),
        _proc("c.d", route="/c/d", method="POST"),
    ]
    data = json.loads(RouteManifest().generate(procs))
    assert len(data["routes"]) == 2
    assert "a.b" in data["routes"]
    assert "c.d" in data["routes"]


def test_valid_json_output() -> None:
    proc = _proc("test.proc", route="/test", method="DELETE")
    output = RouteManifest().generate([proc])
    # Must not raise
    json.loads(output)
```

**Pass criteria:** All 6 tests pass. Output is always valid JSON.
**Fail criteria:** `json.JSONDecodeError`, `KeyError`, or wrong field names.

### 7.6 `test_codegen_watch.py`

**File: `src/py/tests/unit/codegen/test_codegen_watch.py`**

```python
"""Tests for HmrNotifier and TypeBridgeWatcher callback wiring."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from vertebra.core.codegen.watch import HmrNotifier

pytestmark = pytest.mark.unit


def test_hmr_notifier_does_not_raise_when_server_absent() -> None:
    """HmrNotifier.notify() must not raise when the server is not running."""
    notifier = HmrNotifier("http://127.0.0.1:19999")
    # No server on port 19999 — should silently log and return.
    notifier.notify()


def test_hmr_notifier_posts_to_signal_endpoint() -> None:
    """HmrNotifier.notify() posts to /__vertebra/hmr/signal."""
    call_log: list[str] = []

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            pass

    def fake_urlopen(req: object, timeout: float) -> FakeResponse:
        call_log.append(getattr(req, "full_url", str(req)))
        return FakeResponse()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        notifier = HmrNotifier("http://127.0.0.1:8000")
        notifier.notify()

    assert len(call_log) == 1
    assert "/__vertebra/hmr/signal" in call_log[0]


def test_hmr_notifier_callback_integration() -> None:
    """HmrNotifier.notify can be registered as a TypeBridge callback."""
    from vertebra.core.codegen.bridge import TypeBridge
    from vertebra.core.procedures.registry import ProcedureRegistry

    registry = ProcedureRegistry()
    bridge = TypeBridge(registry)

    notified: list[bool] = []
    notifier = HmrNotifier("http://127.0.0.1:19998")

    with patch.object(notifier, "notify", side_effect=lambda: notified.append(True)):
        bridge.add_callback(notifier.notify)
        bridge.invalidate()

    assert len(notified) == 1


def test_watcher_unavailable_without_watchdog() -> None:
    """TypeBridgeWatcher.available is False when watchdog is not installed."""
    from vertebra.core.codegen.watch import TypeBridgeWatcher
    from vertebra.core.procedures.registry import ProcedureRegistry
    from vertebra.core.codegen.bridge import TypeBridge

    registry = ProcedureRegistry()
    bridge = TypeBridge(registry)

    with patch("vertebra.core.codegen.watch._WATCHDOG_AVAILABLE", False):
        with patch("vertebra.core.codegen.watch.Observer", None):
            watcher = TypeBridgeWatcher(bridge, [])
            assert not watcher.available
```

**Pass criteria:** All 4 tests pass. No network connections are made in `test_hmr_notifier_does_not_raise_when_server_absent` (connection refused is caught).
**Fail criteria:** Any uncaught exception or real network call.

---

## 8. Final Gate: `make lint` and `make test`

### 8.1 Lint gate

All Python files in this chapter must pass:
- `ruff format` (no changes required)
- `ruff check` (no errors)
- `mypy` (no type errors — `TYPE_CHECKING` guards are required on all forward references)

All Rust files must pass:
- `rustfmt --check`
- `cargo clippy -- -D warnings`

### 8.2 Test gate

```bash
make test          # uv run pytest src/py/tests — must exit 0
make rs-test       # cargo test --package vertebra-server — must exit 0
```

Specifically, the new test files must all pass:

| File | Tests |
|---|---|
| `test_build_orchestrator.py` | 8 |
| `test_codegen_extractor.py` | 6 |
| `test_codegen_typescript.py` | 4 |
| `test_codegen_zod.py` | 9 |
| `test_codegen_manifest.py` | 6 |
| `test_codegen_watch.py` | 4 |
| **Total** | **37** |

Rust unit tests:

| Module | Tests |
|---|---|
| `hmr.rs` | 2 |

---

## 9. Dependency Notes

### Python

| Package | Requirement | Optional? |
|---|---|---|
| `watchdog` | Existing optional dep; used by `TypeBridgeWatcher` | Yes — guard with `WATCHDOG_INSTALLED` |
| `watchfiles` | Not currently a dep; preferred for future watcher iteration | Future only |
| `msgspec` | Required (already in project) | No |
| `tomllib` | stdlib (Python 3.11+) | No |

### Rust

| Crate | Requirement | Notes |
|---|---|---|
| `tokio` | Already in workspace | Full feature set |
| `hyper` | Already in workspace (`vertebra-server`) | |
| `tokio-tungstenite` | Required for full WS frames | Add to `vertebra-server/Cargo.toml` when implementing full WebSocket upgrade |
| `serde_json` | Already in workspace | Used in `hmr.rs` signal body |

### Frontend tooling (external, not bundled)

| Tool | Required for | Install command |
|---|---|---|
| `rolldown` | Production builds | `npm install -g rolldown` |
| `vite` | Dev server | `npm install -g vite` |
| `zod` | Runtime validation | `npm install zod` (in user's project) |

---

## 10. Implementation Order

The phases should be implemented in this order to enable incremental testing:

1. **Config extension** (`BuildConfig` fields) — no dependencies, unit-testable immediately.
2. **Plugin interfaces** — pure Python, no external dependencies.
3. **Emitters package** (`TypeScriptEmitter`, `ZodEmitter`) — depends only on existing IR.
4. **RouteManifest** — depends only on `ProcedureRegistry`.
5. **BuildOrchestrator** — depends on emitters + manifest + config.
6. **HmrNotifier** (Python) — pure stdlib, no dependencies.
7. **`hmr.rs`** (Rust) — self-contained; add to `vertebra-server` workspace.
8. **CLI `--dev` flag** — depends on `BuildOrchestrator` and `HmrNotifier`.
9. **Tests** — written alongside each phase.

---

## 11. Open Questions and Risks

### 11.1 Rolldown dev server gap

Rolldown does not have a native dev server (as of 2026-03). When `bundler = "rolldown"` and `dev=True`, `BuildOrchestrator` falls back to `vite`. This is intentional and documented in `_rolldown_command`. When Rolldown ships a dev server, `RolldownPlugin.pre_build` can be updated to check for it.

**Risk:** Low. Fallback to Vite for dev is explicitly the plan from litecore research.

### 11.2 Full WebSocket frame handling

`hmr.rs` uses `broadcast::Sender<String>` but the actual WebSocket frame serialisation is deferred behind the `tokio-tungstenite` dependency gate. The current implementation returns `101 Switching Protocols` but does not hold the TCP connection open. Full WS upgrade is a one-file addition once the crate is in Cargo.toml.

**Risk:** Medium. Frontend HMR cannot work until full WS frames are implemented. The Python `HmrNotifier` fallback (polling `/__vertebra/types.d.ts` with `X-Vertebra-Watch`) remains functional via the existing `TypeBridgeApp`.

### 11.3 `watchfiles` not yet introduced

The existing `TypeBridgeWatcher` uses `watchdog`. This chapter does not replace it with `watchfiles`. The litecore research noted Rust-native `notify`-based watching is directionally correct but not a reliability win without platform testing. This chapter keeps `watchdog` and introduces `HmrNotifier` as a thin adapter.

**Risk:** Low. `watchdog` is already in place and tested.

### 11.4 `TypeExtractor` does not handle `pydantic.BaseModel` from runtime

The static AST extractor handles Pydantic via `_is_pydantic_model`. Dynamic `pydantic` model introspection (`extract_pydantic_json_schema`) requires a runtime import. This chapter does not change the Pydantic path — it is already present and tested in `test_types.py`.

**Risk:** None for this chapter.

---

## 12. Recovery Guide

If implementation is interrupted, resume from this state:

1. **Read this spec fully** before writing any code.
2. **Check existing files** at the paths listed in section 2.3 to see what already exists.
3. **Phase status checkpoints:**
   - Phase 1 complete when `src/py/tests/unit/build/test_build_orchestrator.py` passes.
   - Phase 2 complete when `test_codegen_zod.py` and `test_codegen_manifest.py` pass.
   - Phase 3 complete when `test_codegen_watch.py` passes and `cargo test --package vertebra-server` passes for `hmr.rs`.
   - Phase 4 complete when `BuildConfig.load()` correctly parses all fields from the full schema in section 6.1.
4. **Do not modify** `src/py/vertebra/core/codegen/extractor.py`, `ir.py`, `typescript.py`, or `bridge.py` — they are prior art and must not be broken.
5. **Run `make lint` before each commit.** The linting pass is the fastest signal that file structure is correct.
