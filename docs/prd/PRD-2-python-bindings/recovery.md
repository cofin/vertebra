# Session Recovery Guide: PRD-2 Python Bindings and Type Bridge

**Purpose:** Enable any developer (human or AI) to resume work on this PRD with full context.

**Last Updated:** 2026-01-17

---

## Quick Start

### What Is This?

PRD-2 defines the Python bindings layer for Vertebra, a tRPC-like framework that provides seamless type-safe communication between Python backends and TypeScript frontends. The core innovation is automatic TypeScript type generation from Python type annotations with zero visible code generation.

### Current Status

- **PRD Status:** In Progress
- **Phase:** Phase 1 underway (decorator/registry/type IR + TS generator)
- **Dependencies:** PRD-1 complete; required for Phase 3 IPC integration

### Key Documents

1. **Main PRD:** `/home/cody/code/c/vertebra/docs/prd/PRD-2-python-bindings.md`
2. **Research:** `/home/cody/code/c/vertebra/docs/prd/PRD-2-python-bindings/research/plan.md`
3. **Tasks:** `/home/cody/code/c/vertebra/docs/prd/PRD-2-python-bindings/tasks.md`
4. **This File:** `/home/cody/code/c/vertebra/docs/prd/PRD-2-python-bindings/recovery.md`

---

## Architecture Overview (60 Second Summary)

```
Python App (Litestar)
    ↓ @procedure decorator
Procedure Registry
    ↓ Type metadata
Type Extractor (AST → Type IR)
    ↓ Type IR
TypeScript Generator (Type IR → .d.ts)
    ↓ TypeScript types
Type Bridge (Dev Server + HMR)
    ↓ HTTP endpoint
TypeScript Client (Vite plugin)
```

**Flow:**
1. Developer adds `@procedure` to Litestar handler
2. AST extractor analyzes Python types (< 50ms)
3. Type IR created (language-agnostic representation)
4. TypeScript generator creates .d.ts (< 20ms)
5. Type bridge serves types via HMR (< 100ms total)
6. TypeScript IDE autocomplete updates automatically

**Key Innovation:** No visible code generation. Types generated in-memory and served via HMR.

---

## Core Design Decisions

### 1. AST-Based Extraction (Not Runtime Reflection)

**Why:** Python type hints are complete at parse time. AST analysis is faster and works without running the app.

**How:** Use Python's `ast` module to parse source files and extract type annotations.

**Trade-off:** More complex than runtime reflection, but better performance and DX.

**Note**: Annotation resolution must handle `from __future__ import annotations` and PEP 649 deferred evaluation; use `typing.get_type_hints(..., include_extras=True)` with AST fallback.

### 2. Type IR (Intermediate Representation)

**Why:** Allows future expansion to other targets (GraphQL, OpenAPI, JSON Schema).

**How:** Define `TypeNode` hierarchy (PrimitiveType, ArrayType, ObjectType, UnionType, etc.)

**Trade-off:** Extra abstraction layer, but enables extensibility.

### 3. Pluggable Serialization (msgspec default, pydantic support)

**Why:** Users may already use pydantic or protobuf. Don't force migration.

**How:** Define `Serializer` protocol, implement MsgspecSerializer and PydanticSerializer.

**Trade-off:** More code to maintain, but better adoption.

### 4. In-Memory Type Generation (HMR-based)

**Why:** Matches tRPC developer experience. No git noise from generated files.

**How:** Generate types in Python dev server, serve via HTTP endpoint, Vite plugin polls for changes.

**Trade-off:** Requires dev server running, but acceptable for modern development.

### 5. Decorator Stacking (Not Litestar Fork)

**Why:** Non-invasive, compatible with existing Litestar apps.

**How:** `@procedure` wraps handler, preserves all Litestar metadata.

**Trade-off:** Less integrated than fork, but better for ecosystem.

---

## Implementation Phases

### Phase 1: Core Infrastructure (2 weeks)

**Goal:** Basic decorator and type extraction working.

**Key Components:**
- `@procedure` decorator
- ProcedureRegistry (thread-safe, singleton)
- Type IR foundation (PrimitiveType, ArrayType, ObjectType, etc.)
- TypeScript generator for basic types

**Deliverable:** Can register procedures and generate TypeScript for simple types.

**Implemented So Far (2026-01-17):**
- `src/py/vertebra/procedures/decorator.py` (`@procedure` + metadata wiring)
- `src/py/vertebra/procedures/registry.py` (thread-safe registry + metadata dataclass)
- `src/py/vertebra/types/ir.py` (Type IR nodes)
- `src/py/vertebra/types/extractor.py` (primitive/collection/union extraction + AST cache)
- `src/py/vertebra/types/extractor.py` (msgspec.Struct AST extraction + optional runtime metadata merge via msgspec.inspect)
- `src/py/vertebra/types/extractor.py` (pydantic.BaseModel AST extraction + optional runtime metadata merge)
- `src/py/vertebra/types/extractor.py` (pydantic model_json_schema fallback + $ref conversion)
- `src/py/vertebra/types/extractor.py` (pydantic discriminator metadata via JSON Schema)
- `src/py/vertebra/types/extractor.py` (dataclass + TypedDict AST extraction)
- `src/py/vertebra/types/extractor.py` (Generic[T] extraction + TS generics support)
- `src/py/vertebra/types/extractor.py` (forward-reference resolution + import/cache helpers)
- `src/py/vertebra/types/typescript.py` (cycle detection emits type aliases for recursive types)
- `src/py/vertebra/types/ir.py` + `types.typescript` now carry `AnnotatedType` metadata into TypeScript comments
- `src/py/vertebra/types/typescript.py` (basic .d.ts generator + namespace grouping + topo sort + validation + formatting helper)
- Tests: `src/py/tests/test_procedures.py`, `src/py/tests/test_types.py`

**Validation Note:** `validate_typescript()` uses `bunx tsc` or `tsc` when available.
If the compiler is unavailable or cannot write temp files, validation is skipped.

### Phase 2: Advanced Type System (3 weeks)

**Goal:** Full type coverage for Python constructs.

**Key Components:**
- msgspec.Struct extraction (AST + msgspec.inspect)
- Pydantic BaseModel extraction
- Advanced types (generics, unions, literals, Annotated)
- Type resolution (forward references, circular deps, imports)

**Deliverable:** Supports common Python type patterns (80%+ coverage).

### Phase 3: Serialization & IPC (2 weeks)

**Goal:** Connect to Rust IPC layer.

**Dependencies:** PRD-1 (Rust Core IPC Layer) must be complete.

**Key Components:**
- Serialization framework (msgspec, pydantic)
- PyO3 bindings to Rust channel API
- Zero-copy buffer protocol (msgspec)
- End-to-end procedure calls

**Deliverable:** Procedures callable via IPC with < 1ms serialization overhead.

### Phase 4: Developer Experience (2 weeks)

**Goal:** Production-ready HMR and DX.

**Key Components:**
- File watching (watchdog) with debouncing
- Type bridge HTTP endpoint with caching
- HMR integration (Vite plugin)
- Performance optimization (< 100ms updates)

**Deliverable:** Type changes reflect in IDE < 100ms, comprehensive docs.

---

## Key Files to Create

### Python Package Structure

```
src/py/vertebra/
├── __init__.py
├── procedures/
│   ├── __init__.py
│   ├── decorator.py         # @procedure decorator
│   ├── registry.py          # ProcedureRegistry
│   └── serialization.py     # Serializer protocol, implementations
├── types/
│   ├── __init__.py
│   ├── extractor.py         # Python AST → Type IR
│   ├── ir.py                # Type IR classes (TypeNode, etc.)
│   ├── typescript.py        # Type IR → TypeScript .d.ts
│   └── bridge.py            # Dev server endpoint + HMR
└── ipc/
    ├── __init__.py
    ├── channel.py           # Python Channel API (PyO3 wrapper)
    └── buffer.py            # Zero-copy buffer views
```

### TypeScript Package

```
packages/@vertebra/vite-plugin/
├── src/
│   └── index.ts            # Vite plugin for HMR
├── package.json
└── tsconfig.json
```

---

## Performance Targets

| Metric | Target | Why |
|--------|--------|-----|
| AST Parsing | < 10ms/file | Small files (< 500 LOC) |
| Type Extraction | < 30ms/file | Walking AST, resolving refs |
| Type IR Construction | < 5ms/procedure | Simple object construction |
| TypeScript Generation | < 15ms total | String concatenation |
| **Total Pipeline** | **< 100ms** | **HMR requirement for responsive DX** |
| Serialization (msgspec) | < 0.2ms | 1KB payload |
| Serialization (pydantic) | < 1ms | 1KB payload |

---

## Testing Strategy

### Unit Tests (Per Component)

- **Type Extractor:** Fixture Python files with various type patterns
- **Type IR:** Composition, validation, TypeScript generation
- **TypeScript Generator:** Assert exact output for known inputs
- **Serializers:** Round-trip tests (Python → bytes → Python)
- **Decorator:** Verify Litestar integration, metadata preservation

### Integration Tests (End-to-End)

- **Procedure Call:** Python → IPC → TypeScript → Response
- **File Watching:** Modify file, verify type update
- **HMR:** Type updates reach dev server
- **Multi-serializer:** Switch between msgspec/pydantic

### Performance Tests

- **Benchmarks:** Type extraction < 50ms, serialization < 1ms
- **Load Test:** 1000 procedures registered
- **Memory:** < 10MB overhead

---

## Common Pitfalls to Avoid

### 1. Breaking Litestar Compatibility

**Problem:** Decorator interferes with Litestar's route registration.

**Solution:** Always use `functools.wraps`, test decorator stacking, preserve `opt` metadata.

### 2. Type Extraction Too Slow

**Problem:** Parsing every file on every change is slow.

**Solution:** Cache ASTs by file hash, incremental updates only for changed files, parallel processing.

### 3. Incomplete Type Coverage

**Problem:** Edge cases like `ParamSpec` fail to extract.

**Solution:** TypeVar bounds/constraints are now captured; document remaining limitations (ParamSpec) and provide escape hatches (type_overrides parameter).

### 4. HMR Latency

**Problem:** Type updates take > 100ms, feels sluggish.

**Solution:** Optimize each stage (AST caching, template caching, debouncing), measure continuously.

### 5. PRD-1 Dependency Blocking

**Problem:** Waiting for Rust IPC layer delays Python work.

**Solution:** Stub the FFI interface, implement Python-side first, integrate later.

---

## Research Context

### What We Learned

1. **tRPC's DX is the gold standard** - Zero config, instant types, no build step visible to user
2. **AST extraction is proven** - mypy/pyright use it, it's fast enough
3. **msgspec is 5-10x faster** than pydantic for serialization
4. **PyO3 enables zero-copy** via buffer protocol for msgspec
5. **HMR < 100ms is achievable** with proper caching and optimization
6. **Type IR enables extensibility** - Can add GraphQL, OpenAPI later

### Key References

- [tRPC Documentation](https://trpc.io/) - Inspiration for DX
- [msgspec Docs](https://jcristharif.github.io/msgspec/) - Type introspection, performance
- [Litestar Docs](https://litestar.dev) - Controller patterns, DI
- [PyO3 User Guide](https://pyo3.rs/) - Python-Rust FFI
- [Python AST Module](https://docs.python.org/3/library/ast.html) - AST parsing

See `/home/cody/code/c/vertebra/docs/prd/PRD-2-python-bindings/research/plan.md` for detailed research notes.

---

## How to Resume Work

### For Humans

1. **Read Main PRD:** `/home/cody/code/c/vertebra/docs/prd/PRD-2-python-bindings.md`
2. **Review Architecture:** Section "Detailed Component Design" (8 components)
3. **Check Tasks:** `/home/cody/code/c/vertebra/docs/prd/PRD-2-python-bindings/tasks.md`
4. **Start Phase 1, Milestone 1.1:** Implement `@procedure` decorator
5. **Run Tests:** Create unit tests as you go

### For AI Agents

1. **Context:** This is a tRPC-like framework for Python ↔ TypeScript type safety
2. **Goal:** Implement Python bindings that auto-generate TypeScript types from Python type annotations
3. **Approach:** AST extraction → Type IR → TypeScript generation → HMR
4. **Start Here:** Phase 1, Milestone 1.1 (decorator + registry)
5. **Key Constraint:** Must not modify Litestar framework, extend via decorators only

### Commands to Get Started

```bash
# Navigate to project
cd /home/cody/code/c/vertebra

# Create Python package structure
mkdir -p src/py/vertebra/{procedures,types,ipc}
touch src/py/vertebra/procedures/{__init__.py,decorator.py,registry.py,serialization.py}
touch src/py/vertebra/types/{__init__.py,extractor.py,ir.py,typescript.py,bridge.py}
touch src/py/vertebra/ipc/{__init__.py,channel.py,buffer.py}

# Install dev dependencies
uv add msgspec pydantic watchdog --dev

# Create tests
mkdir -p src/py/tests/vertebra/{procedures,types,ipc}
touch src/py/tests/vertebra/test_decorator.py
```

---

## Questions & Clarifications

### Q: Why not use pydantic exclusively?

**A:** Performance. msgspec is 5-10x faster. But we support pydantic for existing codebases.

### Q: Why AST instead of runtime reflection?

**A:** Python type hints may not be available at runtime (`from __future__ import annotations`), and AST is faster.

### Q: How do we handle types that don't map cleanly to TypeScript?

**A:** Document limitations, provide escape hatches (type_overrides parameter), fallback to `any`/`unknown` with warnings.

### Q: What if PRD-1 (Rust IPC) is delayed?

**A:** Stub the FFI interface, implement Phases 1-2 independently, integrate in Phase 3.

### Q: How do we ensure < 100ms type updates?

**A:** Cache ASTs, incremental updates, parallel processing, benchmark continuously, optimize hot paths.

---

## Success Criteria Checklist

When implementation is complete, verify:

- [ ] `@procedure` decorator works with Litestar handlers
- [ ] Type extraction supports 80%+ of Python type constructs
- [ ] TypeScript generation produces valid .d.ts files
- [ ] Type changes reflect in IDE < 100ms
- [ ] msgspec and pydantic serialization both work
- [ ] Zero-copy buffer protocol works for msgspec
- [ ] HMR integration works with Vite
- [ ] All unit tests passing
- [ ] All integration tests passing
- [ ] Performance benchmarks met
- [ ] Documentation complete (Getting Started, API Reference, Examples)
- [ ] Example projects work (minimal, CRUD app)

---

## Contact & Support

For questions or clarifications, refer to:

1. **Main PRD:** Complete technical specification
2. **Research Notes:** Detailed background and rationale
3. **Tasks:** Step-by-step implementation checklist
4. **This Recovery Guide:** High-level context and resumption instructions

---

**End of Recovery Guide**

Ready to implement? Start with Phase 1, Milestone 1.1: `@procedure` decorator.
