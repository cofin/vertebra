# Implementation Tasks for PRD-2: Python Bindings and Type Bridge

**Status:** Ready for Implementation
**Estimated Duration:** 9 weeks
**Dependencies:** PRD-1 (Rust Core IPC Layer) - required for Phase 3

---

## Phase 1: Core Infrastructure (2 weeks)

### Milestone 1.1: Decorator & Registry (3 days)

- [ ] **T1.1.1:** Implement `@procedure` decorator
  - [ ] Create `vertebra/procedures/decorator.py`
  - [ ] Basic decorator that wraps handler functions
  - [ ] Extract function signature using `inspect.signature()`
  - [ ] Extract type hints using `typing.get_type_hints()`
  - [ ] Support optional parameters (name, serializer, metadata)
  - [ ] Use `functools.wraps` to preserve metadata

- [ ] **T1.1.2:** Implement ProcedureRegistry
  - [ ] Create `vertebra/procedures/registry.py`
  - [ ] Define `ProcedureMetadata` dataclass
  - [ ] Implement singleton pattern for global registry
  - [ ] Add thread-safe registration methods (Lock-based)
  - [ ] Add query methods (get_procedures, get_procedure)
  - [ ] Add update/unregister methods for hot reload

- [ ] **T1.1.3:** Litestar Integration
  - [ ] Test decorator stacking with `@get`, `@post`, etc.
  - [ ] Verify metadata preservation
  - [ ] Add procedure metadata to Litestar route `opt` dict
  - [ ] Test with Litestar dependency injection

- [ ] **T1.1.4:** Unit Tests
  - [ ] Test decorator basic functionality
  - [ ] Test decorator parameters (name, serializer, metadata)
  - [ ] Test registry registration/query operations
  - [ ] Test thread safety (concurrent registration)
  - [ ] Test Litestar integration

**Deliverable:** Working `@procedure` decorator and registry

---

### Milestone 1.2: Basic Type Extraction (5 days)

- [ ] **T1.2.1:** Type IR Foundation
  - [ ] Create `vertebra/types/ir.py`
  - [ ] Implement `TypeNode` abstract base class
  - [ ] Implement `PrimitiveType` (int, str, float, bool, null, bytes)
  - [ ] Implement `ArrayType` with generic item type
  - [ ] Implement `ObjectType` with fields dict
  - [ ] Implement `UnionType` with variants list
  - [ ] Implement `LiteralType` for literal values
  - [ ] Add `to_typescript()` method to each type node

- [ ] **T1.2.2:** Type Extractor - Primitives
  - [ ] Create `vertebra/types/extractor.py`
  - [ ] Implement `TypeExtractor` class
  - [ ] Map Python primitives to Type IR (int→number, str→string, etc.)
  - [ ] Handle `None` → `null`
  - [ ] Handle `bytes` → `Uint8Array`

- [ ] **T1.2.3:** Type Extractor - Collections
  - [ ] Extract `list[T]` → `ArrayType`
  - [ ] Extract `dict[K, V]` → `GenericType("Record")`
  - [ ] Extract `set[T]` → `GenericType("Set")`
  - [ ] Extract `tuple[T, ...]` → `ArrayType` (variable length)
  - [ ] Extract `tuple[T1, T2]` → tuple type (fixed length)

- [ ] **T1.2.4:** Type Extractor - Optional/Union
  - [ ] Extract `Optional[T]` → `UnionType([T, null])`
  - [ ] Extract `T | None` → `UnionType([T, null])`
  - [ ] Extract `Union[T1, T2]` → `UnionType([T1, T2])`
  - [ ] Extract `T1 | T2` → `UnionType([T1, T2])`

- [ ] **T1.2.5:** Type Extractor - AST Setup
  - [ ] Add `from_source_file(path)` method
  - [ ] Parse Python file using `ast.parse()`
  - [ ] Walk AST to find class definitions
  - [ ] Cache parsed ASTs by file hash

- [ ] **T1.2.6:** Unit Tests
  - [ ] Test primitive type extraction
  - [ ] Test collection type extraction
  - [ ] Test optional/union type extraction
  - [ ] Test Type IR node construction
  - [ ] Test TypeScript generation for basic types

**Deliverable:** Type extraction for primitives, collections, optional/union types

---

### Milestone 1.3: TypeScript Generator (4 days)

- [ ] **T1.3.1:** Basic TypeScript Generation
  - [ ] Create `vertebra/types/typescript.py`
  - [ ] Implement `TypeScriptGenerator` class
  - [ ] Generate interface definitions from `ObjectType`
  - [ ] Generate type aliases from `UnionType`, `LiteralType`
  - [ ] Generate proper TypeScript syntax

- [ ] **T1.3.2:** Procedure Signature Generation
  - [ ] Generate function signatures from `ProcedureMetadata`
  - [ ] Format parameters as object type
  - [ ] Format return type as `Promise<T>`
  - [ ] Group procedures by namespace (controller path)

- [ ] **T1.3.3:** Module Assembly
  - [ ] Collect all types used by procedures
  - [ ] Deduplicate identical types
  - [ ] Topologically sort types (handle dependencies)
  - [ ] Generate header comment (DO NOT EDIT)
  - [ ] Export namespace structure

- [ ] **T1.3.4:** Output Validation
  - [ ] Run generated .d.ts through TypeScript compiler
  - [ ] Verify syntax correctness
  - [ ] Add formatting (prettier-style)

- [ ] **T1.3.5:** Unit Tests
  - [ ] Test interface generation
  - [ ] Test procedure signature generation
  - [ ] Test namespace grouping
  - [ ] Test type deduplication
  - [ ] Validate generated TypeScript compiles

**Deliverable:** TypeScript .d.ts generation for basic procedures

---

## Phase 2: Advanced Type System (3 weeks)

### Milestone 2.1: msgspec.Struct Support (5 days)

- [ ] **T2.1.1:** Struct AST Extraction
  - [ ] Extract fields from `msgspec.Struct` via AST
  - [ ] Walk class body for `AnnAssign` nodes
  - [ ] Extract field names and type annotations
  - [ ] Handle default values (field optional if has default)

- [ ] **T2.1.2:** Nested Struct Support
  - [ ] Detect when field type is another Struct
  - [ ] Recursively extract nested Struct definitions
  - [ ] Handle circular references (detect and break cycle)

- [ ] **T2.1.3:** msgspec.inspect Integration
  - [ ] Use `msgspec.inspect.type_info()` for metadata
  - [ ] Extract field constraints (min_length, max_length, etc.)
  - [ ] Extract validation metadata
  - [ ] Compare AST vs runtime introspection (prefer AST)

- [ ] **T2.1.4:** Type Metadata
  - [ ] Extract `Annotated[T, Meta(...)]` metadata
  - [ ] Store constraints in Type IR (for future JSON Schema)
  - [ ] Document in TypeScript comments

- [ ] **T2.1.5:** Unit Tests
  - [ ] Test simple Struct extraction
  - [ ] Test nested Struct extraction
  - [ ] Test circular reference handling
  - [ ] Test field optionality
  - [ ] Test metadata extraction

**Deliverable:** Full msgspec.Struct support

---

### Milestone 2.2: Pydantic Support (4 days)

- [ ] **T2.2.1:** BaseModel Extraction
  - [ ] Extract fields from `pydantic.BaseModel` via AST
  - [ ] Use `model_fields` for runtime metadata
  - [ ] Handle field validators
  - [ ] Handle computed fields (exclude from TypeScript)

- [ ] **T2.2.2:** Pydantic v2 Features
  - [ ] Support `Field()` configuration
  - [ ] Extract field descriptions
  - [ ] Handle field aliases
  - [ ] Support discriminated unions (via `model_config`)

- [ ] **T2.2.3:** model_json_schema Integration
  - [ ] Use `model_json_schema()` as fallback
  - [ ] Convert JSON Schema to Type IR
  - [ ] Handle $ref references

- [ ] **T2.2.4:** Unit Tests
  - [ ] Test BaseModel extraction
  - [ ] Test Field() configuration
  - [ ] Test validators (ensure they don't break extraction)
  - [ ] Test json_schema fallback

**Deliverable:** Full Pydantic v2 BaseModel support

---

### Milestone 2.3: Advanced Types (5 days)

- [ ] **T2.3.1:** Literal Types
  - [ ] Extract `Literal["a", "b"]` → union of string literals
  - [ ] Extract `Literal[1, 2]` → union of numeric literals
  - [ ] Extract `Literal[True, False]` → boolean literals
  - [ ] Implement `LiteralType` in Type IR

- [ ] **T2.3.2:** Generic Types
  - [ ] Extract `Generic[T]` class definitions
  - [ ] Map generic parameters to TypeScript generics
  - [ ] Handle bounded generics (limited support)
  - [ ] Document limitations (TypeVar constraints)

- [ ] **T2.3.3:** dataclass Support
  - [ ] Extract fields from `@dataclass` via AST
  - [ ] Use `__dataclass_fields__` for metadata
  - [ ] Handle field() configuration
  - [ ] Handle InitVar and ClassVar (skip in TypeScript)

- [ ] **T2.3.4:** TypedDict Support
  - [ ] Extract TypedDict fields via AST
  - [ ] Handle total=False (all fields optional)
  - [ ] Handle Required[] and NotRequired[]

- [ ] **T2.3.5:** Annotated Metadata
  - [ ] Extract `Annotated[T, ...]` and unwrap to T
  - [ ] Preserve metadata for future use
  - [ ] Document validation constraints in TS comments

- [ ] **T2.3.6:** Unit Tests
  - [ ] Test literal types
  - [ ] Test generic types
  - [ ] Test dataclass extraction
  - [ ] Test TypedDict extraction
  - [ ] Test Annotated metadata

**Deliverable:** Support for advanced Python type constructs

---

### Milestone 2.4: Type Resolution (3 days)

- [ ] **T2.4.1:** Forward Reference Resolution
  - [ ] Detect string annotations (e.g., `"User"`)
  - [ ] Resolve within same module
  - [ ] Resolve from imports
  - [ ] Fallback to `any` with warning if unresolved

- [ ] **T2.4.2:** Circular Reference Detection
  - [ ] Track type dependency graph during extraction
  - [ ] Detect cycles (e.g., Node → Node)
  - [ ] Use TypeScript `type` keyword for recursive types

- [ ] **T2.4.3:** Import Resolution
  - [ ] Parse import statements from AST
  - [ ] Resolve imported types to their source files
  - [ ] Cache imported type definitions
  - [ ] Handle relative imports

- [ ] **T2.4.4:** Caching Strategy
  - [ ] Hash file contents for cache keys
  - [ ] Cache parsed AST per file
  - [ ] Cache extracted types per class
  - [ ] Invalidate cache on file change

- [ ] **T2.4.5:** Unit Tests
  - [ ] Test forward reference resolution
  - [ ] Test circular reference detection
  - [ ] Test import resolution
  - [ ] Test cache invalidation

**Deliverable:** Robust type resolution and caching

---

## Phase 3: Serialization & IPC (2 weeks)

**Dependencies:** PRD-1 (Rust Core IPC Layer) must be complete

### Milestone 3.1: Serialization Framework (3 days)

- [ ] **T3.1.1:** Serializer Protocol
  - [ ] Create `vertebra/procedures/serialization.py`
  - [ ] Define `Serializer` Protocol class
  - [ ] Add methods: `serialize()`, `deserialize()`, `get_type_metadata()`

- [ ] **T3.1.2:** MsgspecSerializer
  - [ ] Implement using `msgspec.json.Encoder/Decoder`
  - [ ] Cache encoders/decoders per type
  - [ ] Use `msgspec.inspect` for type metadata
  - [ ] Optimize for Struct types

- [ ] **T3.1.3:** PydanticSerializer
  - [ ] Implement using `model_dump_json()` / `model_validate_json()`
  - [ ] Handle validation errors
  - [ ] Use `model_fields` for type metadata

- [ ] **T3.1.4:** Auto-Detection
  - [ ] Implement `detect_serializer(type_hint)` function
  - [ ] Check if type is msgspec.Struct → "msgspec"
  - [ ] Check if type is BaseModel → "pydantic"
  - [ ] Default to "msgspec"

- [ ] **T3.1.5:** Unit Tests
  - [ ] Test msgspec roundtrip serialization
  - [ ] Test pydantic roundtrip serialization
  - [ ] Test auto-detection logic
  - [ ] Benchmark serialization performance

**Deliverable:** Pluggable serialization framework

---

### Milestone 3.2: PyO3 Bindings (5 days)

**Depends on:** PRD-1 Rust Core IPC Layer completion

- [ ] **T3.2.1:** FFI Interface Definition
  - [ ] Create `src/py/vertebra/ipc/channel.py`
  - [ ] Define Python Channel API
  - [ ] Define method signatures matching Rust FFI

- [ ] **T3.2.2:** PyO3 Bindings
  - [ ] Create Rust PyO3 module `vertebra_core_py`
  - [ ] Expose `create_channel(channel_id)` function
  - [ ] Expose `channel_send(handle, procedure_id, data)` function
  - [ ] Expose `channel_receive(handle)` function
  - [ ] Expose `channel_close(handle)` function

- [ ] **T3.2.3:** Async Integration
  - [ ] Use `pyo3_asyncio` for async/await support
  - [ ] Bridge Rust async → Python asyncio
  - [ ] Handle futures and coroutines correctly
  - [ ] Test async procedure calls

- [ ] **T3.2.4:** Error Handling
  - [ ] Define Python exception types (ChannelError, SerializationError)
  - [ ] Map Rust errors to Python exceptions
  - [ ] Include error context (procedure_id, stack trace)

- [ ] **T3.2.5:** Unit Tests
  - [ ] Test channel creation and closing
  - [ ] Test send/receive operations
  - [ ] Test async integration
  - [ ] Test error propagation

**Deliverable:** Working PyO3 bindings to Rust IPC

---

### Milestone 3.3: Zero-Copy Protocol (4 days)

- [ ] **T3.3.1:** BufferView Implementation
  - [ ] Create `vertebra/ipc/buffer.py`
  - [ ] Implement `BufferView` class
  - [ ] Expose `as_bytes()` method returning memoryview
  - [ ] Expose `as_msgspec(type_hint)` for direct decoding

- [ ] **T3.3.2:** Rust Buffer Management
  - [ ] Expose buffer pointers via PyO3
  - [ ] Implement `get_buffer(ptr, size)` FFI function
  - [ ] Implement `release_buffer(ptr)` for memory pool
  - [ ] Ensure thread safety

- [ ] **T3.3.3:** msgspec Integration
  - [ ] Use msgspec's buffer protocol support
  - [ ] Decode directly from memoryview (zero-copy)
  - [ ] Benchmark vs copy-based deserialization

- [ ] **T3.3.4:** Fallback for Non-msgspec
  - [ ] Copy buffer to bytes for pydantic/other serializers
  - [ ] Document when zero-copy is active vs fallback

- [ ] **T3.3.5:** Performance Tests
  - [ ] Benchmark zero-copy vs copy (msgspec)
  - [ ] Benchmark different payload sizes (1KB, 10KB, 100KB)
  - [ ] Verify memory usage (no leaks)

**Deliverable:** Zero-copy buffer protocol for msgspec

---

### Milestone 3.4: End-to-End Integration (2 days)

- [ ] **T3.4.1:** Procedure Call Flow
  - [ ] Client → TypeScript call
  - [ ] TypeScript → IPC channel send
  - [ ] IPC → Python Channel receive
  - [ ] Python → Execute procedure
  - [ ] Python → Serialize result
  - [ ] IPC → Send result back
  - [ ] TypeScript → Receive and deserialize

- [ ] **T3.4.2:** Integration Tests
  - [ ] Test simple procedure call (int param, str return)
  - [ ] Test complex types (nested Struct)
  - [ ] Test error handling (invalid params, Python exception)
  - [ ] Test multiple concurrent calls

- [ ] **T3.4.3:** Performance Profiling
  - [ ] Measure end-to-end latency
  - [ ] Identify bottlenecks
  - [ ] Optimize hot paths

**Deliverable:** Working end-to-end procedure calls

---

## Phase 4: Developer Experience (2 weeks)

### Milestone 4.1: File Watching (3 days)

- [ ] **T4.1.1:** watchdog Integration
  - [ ] Add watchdog dependency
  - [ ] Create `SourceFileChangeHandler` class
  - [ ] Watch configured paths recursively
  - [ ] Filter for .py files only

- [ ] **T4.1.2:** Change Detection
  - [ ] Trigger type regeneration on file modify
  - [ ] Debounce rapid changes (100ms window)
  - [ ] Batch multiple file changes
  - [ ] Ignore non-procedure files (optimization)

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

- [ ] **T4.2.1:** HTTP Endpoint
  - [ ] Create `vertebra/types/bridge.py`
  - [ ] Implement `TypeBridge` class
  - [ ] Add `GET /__vertebra/types.d.ts` endpoint
  - [ ] Serve generated TypeScript with correct Content-Type

- [ ] **T4.2.2:** Cache Management
  - [ ] Cache generated TypeScript in memory
  - [ ] Compute ETag from types hash
  - [ ] Return 304 Not Modified if ETag matches
  - [ ] Add Cache-Control headers

- [ ] **T4.2.3:** Compression
  - [ ] Gzip large type files (> 10KB)
  - [ ] Add Content-Encoding header
  - [ ] Benchmark compression overhead

- [ ] **T4.2.4:** Versioning
  - [ ] Add X-TypeScript-Version header (hash of types)
  - [ ] Client uses version for change detection
  - [ ] Include timestamp for debugging

- [ ] **T4.2.5:** Unit Tests
  - [ ] Test endpoint returns correct types
  - [ ] Test ETag/304 behavior
  - [ ] Test compression
  - [ ] Test concurrent requests

**Deliverable:** Type serving endpoint with caching

---

### Milestone 4.3: HMR Integration (4 days)

- [ ] **T4.3.1:** Vite Plugin
  - [ ] Create `@vertebra/vite-plugin` package
  - [ ] Poll /__vertebra/types.d.ts endpoint
  - [ ] Detect changes via X-TypeScript-Version header
  - [ ] Fetch updated types on change

- [ ] **T4.3.2:** In-Memory Type Updates
  - [ ] Write types to virtual file in Vite
  - [ ] Trigger TypeScript language server update
  - [ ] No disk writes (truly in-memory)

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
