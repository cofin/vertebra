# Implementation Tasks for PRD-2: Python Bindings and Type Bridge

**Status:** In Progress
**Last Updated:** 2026-01-18
**Estimated Duration:** 9 weeks
**Dependencies:** PRD-1 (Rust Core IPC Layer) - required for Phase 3

---

## Phase 1: Core Infrastructure (2 weeks)

### Milestone 1.1: Decorator & Registry (3 days)

- [x] **T1.1.1:** Implement `@procedure` decorator
  - [x] Create `vertebra/procedures/decorator.py`
  - [x] Basic decorator that wraps handler functions
  - [x] Extract function signature using `inspect.signature()`
  - [x] Extract type hints using `typing.get_type_hints(..., include_extras=True)`
  - [x] Support optional parameters (name, serializer, metadata)
  - [x] Use `functools.wraps` to preserve metadata

- [x] **T1.1.2:** Implement ProcedureRegistry
  - [x] Create `vertebra/procedures/registry.py`
  - [x] Define `ProcedureMetadata` dataclass
  - [x] Implement singleton pattern for global registry
  - [x] Add thread-safe registration methods (Lock-based)
  - [x] Add query methods (get_procedures, get_procedure)
  - [x] Add update/unregister methods for hot reload

- [ ] **T1.1.3:** Litestar Integration
  - [ ] Test decorator stacking with `@get`, `@post`, etc.
  - [ ] Verify metadata preservation
  - [x] Add procedure metadata to Litestar route `opt` dict
  - [ ] Test with Litestar dependency injection

- [x] **T1.1.4:** Unit Tests
  - [x] Test decorator basic functionality
  - [x] Test decorator parameters (name, serializer, metadata)
  - [x] Test registry registration/query operations
  - [x] Test thread safety (concurrent registration)
  - [x] Test Litestar integration

**Deliverable:** Working `@procedure` decorator and registry

---

### Milestone 1.2: Basic Type Extraction (5 days)

- [x] **T1.2.1:** Type IR Foundation
  - [x] Create `vertebra/types/ir.py`
  - [x] Implement `TypeNode` abstract base class
  - [x] Implement `PrimitiveType` (int, str, float, bool, null, bytes)
  - [x] Implement `ArrayType` with generic item type
  - [x] Implement `ObjectType` with fields dict
  - [x] Implement `UnionType` with variants list
  - [x] Implement `LiteralType` for literal values
  - [x] Add `to_typescript()` method to each type node

- [x] **T1.2.2:** Type Extractor - Primitives
  - [x] Create `vertebra/types/extractor.py`
  - [x] Implement `TypeExtractor` class
  - [x] Map Python primitives to Type IR (int→number, str→string, etc.)
  - [x] Handle `None` → `null`
  - [x] Handle `bytes` → `Uint8Array`

- [x] **T1.2.3:** Type Extractor - Collections
  - [x] Extract `list[T]` → `ArrayType`
  - [x] Extract `dict[K, V]` → `GenericType("Record")`
  - [x] Extract `set[T]` → `GenericType("Set")`
  - [x] Extract `tuple[T, ...]` → `ArrayType` (variable length)
  - [x] Extract `tuple[T1, T2]` → tuple type (fixed length)

- [x] **T1.2.4:** Type Extractor - Optional/Union
  - [x] Extract `Optional[T]` → `UnionType([T, null])`
  - [x] Extract `T | None` → `UnionType([T, null])`
  - [x] Extract `Union[T1, T2]` → `UnionType([T1, T2])`
  - [x] Extract `T1 | T2` → `UnionType([T1, T2])`

- [x] **T1.2.5:** Type Extractor - AST Setup
  - [x] Add `from_source_file(path)` method
  - [x] Parse Python file using `ast.parse()`
  - [x] Walk AST to find class definitions
  - [x] Cache parsed ASTs by file hash

- [x] **T1.2.6:** Unit Tests
  - [x] Test primitive type extraction
  - [x] Test collection type extraction
  - [x] Test optional/union type extraction
  - [x] Test Type IR node construction
  - [x] Test TypeScript generation for basic types

**Deliverable:** Type extraction for primitives, collections, optional/union types

---

### Milestone 1.3: TypeScript Generator (4 days)

- [x] **T1.3.1:** Basic TypeScript Generation
  - [x] Create `vertebra/types/typescript.py`
  - [x] Implement `TypeScriptGenerator` class
  - [x] Generate interface definitions from `ObjectType`
  - [x] Generate type aliases from `UnionType`, `LiteralType`
  - [x] Generate proper TypeScript syntax

- [x] **T1.3.2:** Procedure Signature Generation
  - [x] Generate function signatures from `ProcedureMetadata`
  - [x] Format parameters as object type
  - [x] Format return type as `Promise<T>`
  - [x] Group procedures by namespace (controller path)

- [x] **T1.3.3:** Module Assembly
  - [x] Collect all types used by procedures
  - [x] Deduplicate identical types
  - [x] Topologically sort types (handle dependencies)
  - [x] Generate header comment (DO NOT EDIT)
  - [x] Export namespace structure

- [x] **T1.3.4:** Output Validation
  - [x] Run generated .d.ts through TypeScript compiler
  - [x] Verify syntax correctness
  - [x] Add formatting (prettier-style)

- [x] **T1.3.5:** Unit Tests
  - [x] Test interface generation
  - [x] Test procedure signature generation
  - [x] Test namespace grouping
  - [x] Test type deduplication
  - [x] Validate generated TypeScript compiles

**Deliverable:** TypeScript .d.ts generation for basic procedures

---

## Phase 2: Advanced Type System (3 weeks)

### Milestone 2.1: msgspec.Struct Support (5 days)

- [x] **T2.1.1:** Struct AST Extraction
  - [x] Extract fields from `msgspec.Struct` via AST
  - [x] Walk class body for `AnnAssign` nodes
  - [x] Extract field names and type annotations
  - [x] Handle default values (field optional if has default)

- [ ] **T2.1.2:** Nested Struct Support
  - [x] Detect when field type is another Struct
  - [x] Recursively extract nested Struct definitions
  - [x] Handle circular references (detect and break cycle)

- [x] **T2.1.3:** msgspec.inspect Integration
  - [x] Use `msgspec.inspect.type_info()` for metadata
  - [x] Extract field constraints (min_length, max_length, etc.)
  - [x] Extract validation metadata
  - [x] Compare AST vs runtime introspection (prefer AST)

- [x] **T2.1.4:** Type Metadata
  - [x] Extract `Annotated[T, Meta(...)]` metadata
  - [x] Store constraints in Type IR (for future JSON Schema)
  - [x] Document in TypeScript comments

- [x] **T2.1.5:** Unit Tests
  - [x] Test simple Struct extraction
  - [x] Test nested Struct extraction
  - [x] Test circular reference handling
  - [x] Test field optionality
  - [x] Test metadata extraction

**Deliverable:** Full msgspec.Struct support

---

### Milestone 2.2: Pydantic Support (4 days)

- [x] **T2.2.1:** BaseModel Extraction
  - [x] Extract fields from `pydantic.BaseModel` via AST
  - [x] Use `model_fields` for runtime metadata
  - [x] Handle field validators
  - [x] Handle computed fields (exclude from TypeScript)

- [ ] **T2.2.2:** Pydantic v2 Features
  - [x] Support `Field()` configuration
  - [x] Extract field descriptions
  - [x] Handle field aliases
  - [x] Support discriminated unions (via `model_config`)

- [x] **T2.2.3:** model_json_schema Integration
  - [x] Use `model_json_schema()` as fallback
  - [x] Convert JSON Schema to Type IR
  - [x] Handle $ref references

- [x] **T2.2.4:** Unit Tests
  - [x] Test BaseModel extraction
  - [x] Test Field() configuration
  - [x] Test validators (ensure they don't break extraction)
  - [x] Test json_schema fallback

**Deliverable:** Full Pydantic v2 BaseModel support

---

### Milestone 2.3: Advanced Types (5 days)

- [x] **T2.3.1:** Literal Types
  - [x] Extract `Literal["a", "b"]` → union of string literals
  - [x] Extract `Literal[1, 2]` → union of numeric literals
  - [x] Extract `Literal[True, False]` → boolean literals
  - [x] Implement `LiteralType` in Type IR

- [x] **T2.3.2:** Generic Types
  - [x] Extract `Generic[T]` class definitions
  - [x] Map generic parameters to TypeScript generics
  - [x] Handle bounded generics (limited support)
  - [x] Document limitations (TypeVar constraints)

- [x] **T2.3.3:** dataclass Support
  - [x] Extract fields from `@dataclass` via AST
  - [x] Use `__dataclass_fields__` for metadata
  - [x] Handle field() configuration
  - [x] Handle InitVar and ClassVar (skip in TypeScript)

- [x] **T2.3.4:** TypedDict Support
  - [x] Extract TypedDict fields via AST
  - [x] Handle total=False (all fields optional)
  - [x] Handle Required[] and NotRequired[]

- [x] **T2.3.5:** Annotated Metadata
  - [x] Extract `Annotated[T, ...]` and unwrap to T
  - [x] Preserve metadata for future use
  - [x] Document validation constraints in TS comments

- [x] **T2.3.6:** Unit Tests
  - [x] Test literal types
  - [x] Test generic types
  - [x] Test dataclass extraction
  - [x] Test TypedDict extraction
  - [x] Test Annotated metadata

**Deliverable:** Support for advanced Python type constructs

---

### Milestone 2.4: Type Resolution (3 days)

- [x] **T2.4.1:** Forward Reference Resolution
  - [x] Detect string annotations (e.g., `"User"`)
  - [x] Resolve within same module
  - [x] Resolve from imports
  - [x] Fallback to `any` with warning if unresolved

- [x] **T2.4.2:** Circular Reference Detection
  - [x] Track type dependency graph during extraction
  - [x] Detect cycles (e.g., Node → Node)
  - [x] Use TypeScript `type` keyword for recursive types

- [x] **T2.4.3:** Import Resolution
  - [x] Parse import statements from AST
  - [x] Resolve imported types to their source files
  - [x] Cache imported type definitions
  - [x] Handle relative imports

- [x] **T2.4.4:** Caching Strategy
  - [x] Hash file contents for cache keys
  - [x] Cache parsed AST per file
  - [x] Cache extracted types per class
  - [x] Invalidate cache on file change

- [x] **T2.4.5:** Unit Tests
  - [x] Test forward reference resolution
  - [x] Test circular reference detection
  - [x] Test import resolution
  - [x] Test cache invalidation

**Deliverable:** Robust type resolution and caching

---

## Phase 3: Serialization & IPC (2 weeks)

**Dependencies:** PRD-1 (Rust Core IPC Layer) must be complete

### Milestone 3.1: Serialization Framework (3 days)

- [x] **T3.1.1:** Serializer Protocol
  - [x] Create `vertebra/procedures/serialization.py`
  - [x] Define `Serializer` Protocol class
  - [x] Add methods: `serialize()`, `deserialize()`, `get_type_metadata()`

- [x] **T3.1.2:** MsgspecSerializer
  - [x] Implement using `msgspec.json.Encoder/Decoder`
  - [x] Cache encoders/decoders per type
  - [x] Use `msgspec.inspect` for type metadata
  - [x] Optimize for Struct types

- [x] **T3.1.3:** PydanticSerializer
  - [x] Implement using `model_dump_json()` / `model_validate_json()`
  - [x] Handle validation errors
  - [x] Use `model_fields` for type metadata

- [x] **T3.1.4:** Auto-Detection
  - [x] Implement `detect_serializer(type_hint)` function
  - [x] Check if type is msgspec.Struct → "msgspec"
  - [x] Check if type is BaseModel → "pydantic"
  - [x] Default to "msgspec"

- [x] **T3.1.5:** Unit Tests
  - [x] Test msgspec roundtrip serialization
  - [x] Test pydantic roundtrip serialization
  - [x] Test auto-detection logic
  - [x] Benchmark serialization performance

**Deliverable:** Pluggable serialization framework

---

### Milestone 3.2: PyO3 Bindings (5 days)

**Depends on:** PRD-1 Rust Core IPC Layer completion

- [x] **T3.2.1:** FFI Interface Definition
  - [x] Create `src/py/vertebra/ipc/channel.py`
  - [x] Define Python Channel API
  - [x] Define method signatures matching Rust FFI

- [x] **T3.2.2:** PyO3 Bindings
  - [x] Expose Channel class (pair + named create/open)
  - [x] Expose request/response send + recv functions
  - [x] Export channel handle via `_vertebra`

- [x] **T3.2.3:** Async Integration
  - [x] Async wrappers via `asyncio.to_thread`
  - [x] Bridge blocking Rust calls into asyncio
  - [x] Test async procedure calls

- [x] **T3.2.4:** Error Handling
  - [x] Define Python exception types (ChannelError, SerializationError)
  - [x] Map Rust errors to Python exceptions
  - [x] Include error context (procedure_id, stack trace)

- [x] **T3.2.5:** Unit Tests
  - [x] Test channel creation and closing
  - [x] Test send/receive operations
  - [x] Test async integration
  - [x] Test error propagation

**Deliverable:** Working PyO3 bindings to Rust IPC

---

### Milestone 3.3: Zero-Copy Protocol (4 days)

- [x] **T3.3.1:** BufferView Implementation
  - [x] Create `vertebra/ipc/channel.py` BufferView
  - [x] Implement `BufferView` class
  - [x] Expose `as_bytes()` and `as_memoryview()`
  - [x] Expose `as_msgspec(type_hint)` for direct decoding

- [x] **T3.3.2:** Rust Buffer Management
  - [x] Expose buffer pointers via PyO3 buffer protocol
  - [x] Implement buffer view tracking for safe exports
  - [x] Ensure thread safety

- [x] **T3.3.3:** msgspec Integration
  - [x] Use msgspec's buffer protocol support
  - [x] Decode directly from memoryview (zero-copy)
  - [x] Benchmark vs copy-based deserialization

- [x] **T3.3.4:** Fallback for Non-msgspec
  - [x] Copy buffer to bytes for pydantic/other serializers
  - [x] Document when zero-copy is active vs fallback

- [ ] **T3.3.5:** Performance Tests
  - [x] Benchmark zero-copy vs copy (msgspec)
  - [x] Benchmark different payload sizes (1KB, 10KB, 100KB)
  - [ ] Verify memory usage (no leaks)

**Deliverable:** Zero-copy buffer protocol for msgspec

---

### Milestone 3.4: End-to-End Integration (2 days)

- [x] **T3.4.1:** Procedure Call Flow
  - [x] Client harness (Python) → IPC channel send
  - [x] IPC → Python Channel receive
  - [x] Python → Execute procedure
  - [x] Python → Serialize result
  - [x] IPC → Send result back
  - [x] Client harness (Python) → Receive and deserialize

- [x] **T3.4.2:** Integration Tests
  - [x] Test simple procedure call (int param, str return)
  - [x] Test complex types (nested Struct)
  - [x] Test error handling (invalid params, Python exception)
  - [x] Test multiple concurrent calls

- [x] **T3.4.3:** Performance Profiling
  - [x] Measure end-to-end latency
  - [x] Identify bottlenecks
  - [x] Optimize hot paths

**Deliverable:** Working end-to-end procedure calls

---

## Phase 4: Developer Experience (2 weeks)

### Milestone 4.1: File Watching (3 days)

- [x] **T4.1.1:** watchdog Integration
  - [x] Add watchdog dependency
  - [x] Create `SourceFileChangeHandler` class
  - [x] Watch configured paths recursively
  - [x] Filter for .py files only

- [x] **T4.1.2:** Change Detection
  - [x] Trigger type regeneration on file modify
  - [x] Debounce rapid changes (100ms window)
  - [x] Batch multiple file changes
  - [x] Ignore non-procedure files (optimization)

- [ ] **T4.1.3:** Incremental Updates
  - [ ] Only re-extract changed files
  - [ ] Invalidate dependent types
  - [ ] Merge updated types with cached types

- [ ] **T4.1.4:** Error Recovery
  - [ ] Handle file parse errors gracefully
  - [ ] Continue serving old types on error
  - [ ] Log errors with file path and line number

- [ ] **T4.1.5:** Unit Tests
  - [ ] Test file change detection
  - [ ] Test debouncing logic
  - [ ] Test incremental updates
  - [ ] Test error recovery

**Deliverable:** Automatic type regeneration on file changes

---

### Milestone 4.2: Type Bridge Endpoint (3 days)

- [x] **T4.2.1:** HTTP Endpoint
  - [x] Create `vertebra/types/bridge.py`
  - [x] Implement `TypeBridge` class
  - [x] Add `GET /__vertebra/types.d.ts` endpoint
  - [x] Serve generated TypeScript with correct Content-Type

- [x] **T4.2.2:** Cache Management
  - [x] Cache generated TypeScript in memory
  - [x] Compute ETag from types hash
  - [x] Return 304 Not Modified if ETag matches
  - [x] Add Cache-Control headers

- [x] **T4.2.3:** Compression
  - [x] Gzip large type files (> 10KB)
  - [ ] Benchmark compression overhead

- [x] **T4.2.4:** Versioning
  - [x] Add X-TypeScript-Version header (hash of types)
  - [x] Client uses version for change detection
  - [x] Include timestamp for debugging

- [x] **T4.2.5:** Unit Tests
  - [x] Test endpoint returns correct types
  - [x] Test ETag/304 behavior
  - [x] Test compression
  - [ ] Test concurrent requests

**Deliverable:** Type serving endpoint with caching

---

### Milestone 4.3: HMR Integration (4 days)

- [x] **T4.3.1:** Vite Plugin
  - [x] Create `@vertebra/vite-plugin` package
  - [x] Poll /__vertebra/types.d.ts endpoint
  - [x] Detect changes via X-TypeScript-Version header
  - [x] Fetch updated types on change

- [x] **T4.3.2:** In-Memory Type Updates
  - [x] Write types to virtual file in Vite
  - [x] Trigger TypeScript language server update
  - [x] No disk writes (truly in-memory)

- [ ] **T4.3.3:** Multi-Framework Support
  - [ ] Test with React (Vite)
  - [ ] Test with Vue (Vite)
  - [ ] Test with Svelte (Vite)
  - [ ] Document webpack support (future)

- [ ] **T4.3.4:** WebSocket Push (Optional)
  - [ ] Implement WebSocket endpoint for push updates
  - [ ] Python sends update event on type change
  - [ ] Client receives and updates immediately
  - [ ] Fallback to polling if WebSocket unavailable

- [ ] **T4.3.5:** Integration Tests
  - [ ] Test end-to-end HMR flow
  - [ ] Measure update latency (should be < 100ms)
  - [ ] Test multiple clients simultaneously

**Deliverable:** HMR integration with < 100ms updates

---

### Milestone 4.4: Performance & Polish (4 days)

- [ ] **T4.4.1:** Performance Optimization
  - [ ] Profile type extraction pipeline
  - [ ] Optimize AST parsing (parallel processing)
  - [ ] Optimize TypeScript generation (templates)
  - [ ] Achieve < 100ms end-to-end target

- [ ] **T4.4.2:** Telemetry & Logging
  - [ ] Add structured logging (using `structlog`)
  - [ ] Log type extraction times
  - [ ] Log procedure registrations
  - [ ] Add performance metrics (optional statsd)

- [ ] **T4.4.3:** Developer Documentation
  - [ ] Write Getting Started guide
  - [ ] Write API reference
  - [ ] Write migration guide (from manual types)
  - [ ] Write troubleshooting guide

- [ ] **T4.4.4:** Example Projects
  - [ ] Create minimal "Hello World" example
  - [ ] Create full CRUD app example (Todo list)
  - [ ] Create example with authentication
  - [ ] Create example with file uploads

- [ ] **T4.4.5:** Final Testing
  - [ ] Run full test suite
  - [ ] Test on Python 3.12, 3.13
  - [ ] Test with latest Litestar version
  - [ ] Performance regression tests

**Deliverable:** Production-ready, documented, optimized package

---

## Summary

**Total Tasks:** 100+
**Total Duration:** 9 weeks
**Critical Path:** Phase 3 (depends on PRD-1)

**Recommended Team Size:** 2-3 engineers

**Milestones:**
1. Phase 1 Complete: Basic type extraction working
2. Phase 2 Complete: Full type coverage
3. Phase 3 Complete: IPC integration working
4. Phase 4 Complete: Production-ready DX

**Success Criteria:**
- All unit tests passing
- Integration tests passing
- Performance benchmarks met (< 100ms updates)
- Documentation complete
- Example projects working
