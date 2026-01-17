# PRD-2: Python Bindings and Type Bridge

**Status:** In Progress
**Version:** 1.0
**Created:** 2026-01-17
**Owner:** Vertebra Core Team
**Dependencies:** PRD-1 (Rust Core IPC Layer)

---

## Executive Summary

PRD-2 defines the Python bindings layer for Vertebra, a tRPC-like framework that provides seamless type-safe communication between Python backends and TypeScript frontends. The core innovation is the **Type Bridge** - a system where Python type annotations automatically become TypeScript types with zero visible code generation, delivering a developer experience comparable to tRPC but for Python.

### Key Innovation

The Type Bridge eliminates the traditional code generation workflow. Instead of running `generate` commands, types are extracted from Python source via AST inspection, converted to TypeScript definitions, and served in-memory through Hot Module Replacement (HMR). When developers change Python types, their IDE autocomplete updates in under 100ms.

### Success Criteria

1. **Zero Visible Code Generation** - No manual `generate` step in developer workflow
2. **Sub-100ms Type Updates** - Type changes reflect in IDE within 100ms via HMR
3. **Framework Integration** - Works with standard Litestar patterns without forking
4. **Type Coverage** - Supports 80%+ of common Python type patterns
5. **Pluggable Architecture** - Extensible serialization (msgspec, pydantic, protobuf)

---

## Background & Context

### Problem Statement

Modern full-stack applications require type safety across language boundaries. Existing solutions force developers to:

1. Manually maintain duplicate type definitions
2. Run code generation tools as separate build steps
3. Commit generated files to version control
4. Context-switch between languages to debug type mismatches

This creates friction and slows iteration velocity.

### Inspiration: tRPC

tRPC solved this for TypeScript-only stacks by leveraging TypeScript's type inference. When you define a backend procedure, the client automatically knows its types without code generation. Vertebra brings this experience to Python backends.

### Why Python Needs a Different Approach

Unlike TypeScript, Python's type system is based on runtime annotations and static analysis (PEP 484). tRPC's runtime reflection approach doesn't work well in Python because:

- Type hints may not be available at runtime (if using `from __future__ import annotations`)
- Python's dynamic nature means type info is incomplete without AST inspection
- Running the Python app just for type extraction is slow and fragile

**Solution:** Use Abstract Syntax Tree (AST) analysis to extract types statically, similar to how mypy and pyright work.

### Assumptions and Constraints

- **Python 3.12+** baseline (aligned with `pyproject.toml`).
- **Annotation resolution** must handle `from __future__ import annotations` and deferred evaluation (PEP 649).
- **Type extraction** prioritizes runtime hints when available, falling back to AST parsing for source-level fidelity.

> **See PRD-0 Cross-PRD Architectural Decisions** for canonical project-wide choices:
> - Python 3.12+ baseline (unanimous consensus)
> - **msgspec as Python codec** - default serializer for zero-copy performance
> - Serialization layering: msgspec (Python), Protobuf (gRPC/MCP), MessagePack (drivers), bincode (internal Rust)

---

## Architecture Overview

### Component Stack

```
┌─────────────────────────────────────────────┐
│         TypeScript Client (PRD-3)           │
│    - Auto-generated types from bridge      │
│    - Type-safe procedure calls             │
└────────────────┬────────────────────────────┘
                 │ HTTP/WebSocket
┌────────────────┴────────────────────────────┐
│         Type Bridge (Dev Server)            │
│    - Serves .d.ts via HMR                  │
│    - Watches Python files                  │
│    - < 100ms update pipeline               │
└────────────────┬────────────────────────────┘
                 │ Type IR
┌────────────────┴────────────────────────────┐
│      Type Extraction & Generation           │
│    - Python AST → Type IR                  │
│    - Type IR → TypeScript .d.ts            │
│    - Caching & incremental updates         │
└────────────────┬────────────────────────────┘
                 │ Procedure Metadata
┌────────────────┴────────────────────────────┐
│         Procedure Registry                  │
│    - @procedure decorator                  │
│    - Route + type metadata storage         │
│    - Thread-safe updates                   │
└────────────────┬────────────────────────────┘
                 │ Litestar Integration
┌────────────────┴────────────────────────────┐
│        Python Application Layer             │
│    - Standard Litestar controllers         │
│    - msgspec/pydantic models               │
│    - Business logic                        │
└────────────────┬────────────────────────────┘
                 │ PyO3 FFI
┌────────────────┴────────────────────────────┐
│      Rust Core IPC Layer (PRD-1)           │
│    - Shared memory channels                │
│    - Zero-copy buffer protocol             │
│    - Multi-process coordination            │
└─────────────────────────────────────────────┘
```

### Design Principles

1. **Non-Invasive** - Extend Litestar via decorators, don't modify framework code
2. **Static Analysis** - Extract types from AST, not runtime reflection
3. **Incremental** - Only regenerate types for changed files
4. **Pluggable** - Support multiple serialization backends
5. **Performance First** - Every operation optimized for sub-100ms total

---

## Detailed Component Design

### 1. Decorator Layer (`procedures/decorator.py`)

The `@procedure` decorator is the primary API surface for developers.

#### API Design

```python
from vertebra import procedure
from litestar import get
from msgspec import Struct

class User(Struct):
    id: int
    name: str
    email: str

@get("/users/{user_id:int}")
@procedure  # <-- Single decorator marks this for type extraction
async def get_user(user_id: int) -> User:
    return await db.get_user(user_id)
```

#### Advanced Usage

```python
@procedure(
    name="customProcedureName",  # Override default name
    serializer="pydantic",        # Use pydantic instead of msgspec
    metadata={"auth": "required"}, # Custom metadata for client
    type_overrides={               # Manual type hints for edge cases
        "user_id": "string | number"
    }
)
async def complex_handler(...): ...
```

#### Implementation Strategy

The decorator must:

1. **Preserve Litestar behavior** - Wrap the handler without changing execution
2. **Extract type metadata** - Capture function signature, params, return type
3. **Register with ProcedureRegistry** - Store metadata for type extraction
4. **Add route metadata** - Attach vertebra-specific metadata to Litestar route

**Implementation Notes:**

- Use `functools.wraps` to preserve original function metadata
- Extract type hints via `typing.get_type_hints()` for runtime metadata
- Store original function for AST extraction later
- Support decorator stacking (must work before or after Litestar decorators)

### 2. Procedure Registry (`procedures/registry.py`)

Central registry storing all procedures with their type metadata.

#### Data Model

```python
@dataclass
class ProcedureMetadata:
    procedure_id: str           # Unique identifier (e.g., "users.get_user")
    route: str                   # HTTP route (e.g., "/users/{user_id}")
    method: str                  # HTTP method (GET, POST, etc.)
    handler_fn: Callable         # Original handler function
    source_file: Path            # Path to source file for AST extraction
    params: dict[str, TypeNode]  # Parameter types (Type IR)
    return_type: TypeNode        # Return type (Type IR)
    serializer: str              # Serializer backend ("msgspec", "pydantic", etc.)
    metadata: dict[str, Any]     # Custom metadata
    last_updated: datetime       # For cache invalidation
```

#### API

```python
# Singleton registry
registry = ProcedureRegistry()

# Registration (called by decorator)
def register_procedure(metadata: ProcedureMetadata) -> None: ...

# Querying (used by type bridge)
def get_procedures() -> dict[str, ProcedureMetadata]: ...
def get_procedure(procedure_id: str) -> ProcedureMetadata | None: ...

# Updates (for hot reload)
def update_procedure(procedure_id: str, metadata: ProcedureMetadata) -> None: ...
def unregister_procedure(procedure_id: str) -> None: ...
```

#### Thread Safety

The registry must be thread-safe for hot reload scenarios:

- Use `threading.Lock` for mutation operations
- Support concurrent reads without blocking
- Provide atomic update operations

### 3. Type Extractor (`types/extractor.py`)

Converts Python type annotations to language-agnostic Type IR using AST analysis.

#### Core Workflow

```python
from vertebra.types import TypeExtractor

extractor = TypeExtractor()

# Extract from type hint (for runtime-available annotations)
type_node = extractor.from_type_hint(User)

# Extract from source file (for AST analysis)
type_nodes = extractor.from_source_file("path/to/handlers.py")

# Extract from specific function
type_node = extractor.from_function(get_user)
```

#### Annotation Resolution Strategy

- Use `typing.get_type_hints(..., include_extras=True)` for runtime hints when available.
- Fall back to AST parsing for modules that defer annotations or use forward refs.
- Preserve `Annotated` metadata for validation and downstream codegen.

#### Supported Type Constructs

**Primitives:**
- `int`, `float`, `str`, `bool`, `None`
- `bytes`, `bytearray`

**Collections:**
- `list[T]`, `List[T]`
- `dict[K, V]`, `Dict[K, V]`
- `set[T]`, `Set[T]`
- `tuple[T, ...]`, `Tuple[T, ...]`

**Composite Types:**
- `msgspec.Struct` - Extract fields via AST
- `pydantic.BaseModel` - Extract via `model_fields`
- `dataclasses.dataclass` - Extract via `__dataclass_fields__`
- `typing.TypedDict` - Extract via annotations

**Advanced Types:**
- `Optional[T]`, `T | None` - Union with null
- `Union[T1, T2, ...]`, `T1 | T2` - Type unions
- `Literal["a", "b"]` - Literal types
- `Generic[T]` - Generic types (partial support)
- `Annotated[T, Meta(...)]` - Extract metadata for validation

**Edge Cases:**
- Forward references (`"User"`) - Resolve via module context
- Circular references - Detect and use TypeScript `type` keyword
- `typing.Any` - Map to `any` in TypeScript
- Unknown types - Fallback to `unknown` with warning

#### AST Parsing Strategy

```python
import ast
from typing import get_type_hints

def extract_struct_fields(source_code: str, class_name: str) -> dict[str, TypeNode]:
    """Extract fields from msgspec.Struct via AST."""
    tree = ast.parse(source_code)

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            fields = {}
            for item in node.body:
                if isinstance(item, ast.AnnAssign):
                    field_name = item.target.id
                    field_type = _ast_to_type_node(item.annotation)
                    fields[field_name] = field_type
            return fields

    raise ValueError(f"Class {class_name} not found")
```

#### Performance Optimizations

- **Cache parsed ASTs** - Don't reparse unchanged files
- **Incremental extraction** - Only extract changed classes
- **Lazy resolution** - Defer forward reference resolution until needed
- **Parallel processing** - Extract multiple files concurrently

**Target:** < 50ms per file extraction

### 4. Type IR (`types/ir.py`)

Intermediate representation for types, agnostic to source and target languages.

#### Type Node Hierarchy

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass

class TypeNode(ABC):
    """Base class for all type nodes."""

    @abstractmethod
    def to_typescript(self) -> str:
        """Generate TypeScript type definition."""
        ...

    @abstractmethod
    def to_json_schema(self) -> dict:
        """Generate JSON Schema."""
        ...

@dataclass
class PrimitiveType(TypeNode):
    kind: Literal["int", "float", "str", "bool", "null", "bytes"]

    def to_typescript(self) -> str:
        mapping = {
            "int": "number",
            "float": "number",
            "str": "string",
            "bool": "boolean",
            "null": "null",
            "bytes": "Uint8Array"
        }
        return mapping[self.kind]

@dataclass
class ArrayType(TypeNode):
    item_type: TypeNode

    def to_typescript(self) -> str:
        return f"Array<{self.item_type.to_typescript()}>"

@dataclass
class ObjectType(TypeNode):
    name: str
    fields: dict[str, tuple[TypeNode, bool]]  # (type, required)

    def to_typescript(self) -> str:
        field_defs = []
        for name, (type_node, required) in self.fields.items():
            optional = "" if required else "?"
            field_defs.append(f"  {name}{optional}: {type_node.to_typescript()}")

        fields_str = "\n".join(field_defs)
        return f"interface {self.name} {{\n{fields_str}\n}}"

@dataclass
class UnionType(TypeNode):
    variants: list[TypeNode]

    def to_typescript(self) -> str:
        types = [v.to_typescript() for v in self.variants]
        return " | ".join(types)

@dataclass
class LiteralType(TypeNode):
    value: int | str | bool

    def to_typescript(self) -> str:
        if isinstance(self.value, str):
            return f'"{self.value}"'
        return str(self.value).lower() if isinstance(self.value, bool) else str(self.value)

@dataclass
class GenericType(TypeNode):
    base: str
    params: list[TypeNode]

    def to_typescript(self) -> str:
        param_str = ", ".join(p.to_typescript() for p in self.params)
        return f"{self.base}<{param_str}>"
```

#### Type Composition

The IR supports type composition for complex scenarios:

```python
# Optional[User] -> UnionType([ObjectType("User"), PrimitiveType("null")])
# list[str] -> ArrayType(PrimitiveType("str"))
# dict[str, int] -> GenericType("Record", [PrimitiveType("str"), PrimitiveType("int")])
```

#### Extensibility

Users can define custom type nodes:

```python
class CustomType(TypeNode):
    def to_typescript(self) -> str:
        return "MyCustomType"

    def to_json_schema(self) -> dict:
        return {"type": "object", "x-custom": True}

# Register custom type mapper
from vertebra.types import register_type_mapper

@register_type_mapper
def map_custom_type(python_type: type) -> TypeNode | None:
    if isinstance(python_type, MyCustomClass):
        return CustomType()
    return None
```

### 5. TypeScript Generator (`types/typescript.py`)

Converts Type IR to TypeScript `.d.ts` declarations.

#### Generation Strategy

```python
class TypeScriptGenerator:
    def generate(self, procedures: dict[str, ProcedureMetadata]) -> str:
        """Generate complete .d.ts file for all procedures."""

        # 1. Collect all types used across procedures
        type_nodes = self._collect_types(procedures)

        # 2. Deduplicate and topologically sort (handle dependencies)
        sorted_types = self._sort_types(type_nodes)

        # 3. Generate type definitions
        type_defs = [t.to_typescript() for t in sorted_types]

        # 4. Generate procedure signatures
        procedure_defs = [self._generate_procedure(p) for p in procedures.values()]

        # 5. Combine into module
        return self._assemble_module(type_defs, procedure_defs)

    def _generate_procedure(self, proc: ProcedureMetadata) -> str:
        """Generate procedure signature."""
        param_types = ", ".join(
            f"{name}: {type_node.to_typescript()}"
            for name, type_node in proc.params.items()
        )
        return_type = proc.return_type.to_typescript()

        return f"""
export function {proc.procedure_id}(
  params: {{ {param_types} }}
): Promise<{return_type}>;
"""
```

#### Output Format

```typescript
// Generated by Vertebra Type Bridge
// DO NOT EDIT - This file is auto-generated

// Type definitions
export interface User {
  id: number;
  name: string;
  email: string;
}

export interface CreateUserInput {
  name: string;
  email: string;
}

// Procedure signatures
export namespace api {
  export namespace users {
    export function get_user(
      params: { user_id: number }
    ): Promise<User>;

    export function create_user(
      params: { data: CreateUserInput }
    ): Promise<User>;
  }
}
```

#### Optimizations

- **Type deduplication** - Reuse identical type definitions
- **Tree shaking** - Only include types used by procedures
- **Namespace grouping** - Group procedures by controller path
- **Import generation** - Generate imports for external types

**Target:** < 20ms for 100 procedures

### 6. Type Bridge (`types/bridge.py`)

Dev server integration for serving types via HMR.

#### Architecture

```python
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class TypeBridge:
    def __init__(self, config: VertebraConfig):
        self.config = config
        self.observer = Observer()
        self.type_cache: str | None = None
        self.cache_timestamp: datetime | None = None

    def start(self):
        """Start file watching and dev server."""
        # Watch Python source files
        handler = SourceFileChangeHandler(self.on_file_changed)
        for path in self.config.watch_paths:
            self.observer.schedule(handler, path, recursive=True)
        self.observer.start()

        # Generate initial types
        self._regenerate_types()

    def on_file_changed(self, event):
        """Triggered when Python source file changes."""
        if event.src_path.endswith('.py'):
            self._regenerate_types()
            self._notify_hmr_clients()

    def _regenerate_types(self):
        """Extract types and generate TypeScript."""
        start = time.perf_counter()

        # Get all procedures from registry
        procedures = registry.get_procedures()

        # Generate TypeScript
        generator = TypeScriptGenerator()
        ts_code = generator.generate(procedures)

        # Update cache
        self.type_cache = ts_code
        self.cache_timestamp = datetime.now()

        elapsed = (time.perf_counter() - start) * 1000
        if elapsed > 100:
            logger.warning(f"Type generation took {elapsed:.1f}ms (> 100ms target)")

    def get_types(self) -> str:
        """Get current TypeScript definitions."""
        if self.type_cache is None:
            self._regenerate_types()
        return self.type_cache
```

#### HTTP Endpoint

```python
from litestar import get, Response

@get("/__vertebra/types.d.ts")
async def serve_types() -> Response:
    """Serve TypeScript types for HMR."""
    ts_code = type_bridge.get_types()
    return Response(
        content=ts_code,
        media_type="text/typescript",
        headers={
            "Cache-Control": "no-cache",
            "X-TypeScript-Types": "true"
        }
    )
```

#### HMR Integration

The TypeScript client uses a Vite plugin to fetch types:

```typescript
// vite.config.ts
import { vertebraTypes } from '@vertebra/vite-plugin'

export default {
  plugins: [
    vertebraTypes({
      serverUrl: 'http://localhost:8000',
      typesEndpoint: '/__vertebra/types.d.ts',
      pollInterval: 1000  // Check for updates every 1s
    })
  ]
}
```

**HMR Protocol:**
1. Vite plugin polls `/__vertebra/types.d.ts`
2. Compares `X-TypeScript-Version` header (hash of types)
3. If changed, fetch new types and update in-memory
4. TypeScript language server picks up changes
5. IDE autocomplete updates

### 7. Serialization Layer (`procedures/serialization.py`)

Pluggable serialization supporting multiple backends.

#### Abstract Interface

```python
from typing import Protocol, Any

class Serializer(Protocol):
    """Protocol for serialization backends."""

    def serialize(self, value: Any, type_hint: type) -> bytes:
        """Serialize Python value to bytes."""
        ...

    def deserialize(self, data: bytes, type_hint: type) -> Any:
        """Deserialize bytes to Python value."""
        ...

    def get_type_metadata(self, type_hint: type) -> TypeNode:
        """Extract type metadata for Type IR."""
        ...
```

#### Msgspec Implementation (Default)

```python
import msgspec
from msgspec import Struct

class MsgspecSerializer:
    def __init__(self):
        self.encoder = msgspec.json.Encoder()
        self.decoder_cache: dict[type, msgspec.json.Decoder] = {}

    def serialize(self, value: Any, type_hint: type) -> bytes:
        return self.encoder.encode(value)

    def deserialize(self, data: bytes, type_hint: type) -> Any:
        if type_hint not in self.decoder_cache:
            self.decoder_cache[type_hint] = msgspec.json.Decoder(type_hint)
        return self.decoder_cache[type_hint].decode(data)

    def get_type_metadata(self, type_hint: type) -> TypeNode:
        """Use msgspec.inspect for metadata."""
        if isinstance(type_hint, type) and issubclass(type_hint, Struct):
            info = msgspec.inspect.type_info(type_hint)
            fields = {
                field.name: (
                    self._convert_field_type(field.type),
                    field.required
                )
                for field in info.fields
            }
            return ObjectType(name=type_hint.__name__, fields=fields)

        # Fallback to AST extraction
        return None
```

**Performance:** Msgspec is 10-50x faster than alternatives for JSON serialization.

#### Pydantic Implementation

```python
from pydantic import BaseModel

class PydanticSerializer:
    def serialize(self, value: Any, type_hint: type) -> bytes:
        if isinstance(value, BaseModel):
            return value.model_dump_json().encode()
        return json.dumps(value).encode()

    def deserialize(self, data: bytes, type_hint: type) -> Any:
        if isinstance(type_hint, type) and issubclass(type_hint, BaseModel):
            return type_hint.model_validate_json(data)
        return json.loads(data)

    def get_type_metadata(self, type_hint: type) -> TypeNode:
        """Use model_fields for metadata."""
        if isinstance(type_hint, type) and issubclass(type_hint, BaseModel):
            fields = {
                name: (
                    self._convert_field_type(field.annotation),
                    field.is_required()
                )
                for name, field in type_hint.model_fields.items()
            }
            return ObjectType(name=type_hint.__name__, fields=fields)
        return None
```

#### Auto-Detection

```python
def detect_serializer(type_hint: type) -> str:
    """Auto-detect appropriate serializer from type hint."""
    if isinstance(type_hint, type):
        if issubclass(type_hint, msgspec.Struct):
            return "msgspec"
        elif issubclass(type_hint, BaseModel):
            return "pydantic"
    return "msgspec"  # Default
```

### 8. IPC Channel (`ipc/channel.py`)

PyO3 bindings to Rust Core IPC Layer (PRD-1 dependency).

#### Python Channel API

```python
from typing import Any, Callable
from vertebra.ipc import Channel

class Channel:
    """Python wrapper around Rust IPC channel."""

    def __init__(self, channel_id: str):
        # FFI call to Rust: create_channel(channel_id)
        self._handle = _vertebra_core.create_channel(channel_id)

    async def send(self, procedure_id: str, params: dict[str, Any]) -> Any:
        """Send procedure call via IPC."""
        # Serialize params
        serializer = get_serializer()
        data = serializer.serialize(params, dict)

        # FFI call to Rust: channel_send(handle, procedure_id, data)
        response_data = await _vertebra_core.channel_send(
            self._handle,
            procedure_id,
            data
        )

        # Deserialize response
        proc = registry.get_procedure(procedure_id)
        return serializer.deserialize(response_data, proc.return_type)

    async def receive(self) -> tuple[str, dict]:
        """Receive procedure call from IPC."""
        # FFI call to Rust: channel_receive(handle)
        procedure_id, data = await _vertebra_core.channel_receive(self._handle)

        # Deserialize params
        proc = registry.get_procedure(procedure_id)
        params = get_serializer().deserialize(data, dict)

        return procedure_id, params

    def close(self):
        """Close channel."""
        _vertebra_core.channel_close(self._handle)
```

#### Zero-Copy Buffer Protocol

For msgspec (which supports buffer protocol):

```python
class BufferView:
    """Zero-copy view into shared memory buffer."""

    def __init__(self, ptr: int, size: int):
        self._ptr = ptr
        self._size = size

    def as_bytes(self) -> memoryview:
        """Get memoryview without copying."""
        # FFI call to get buffer pointer
        buffer = _vertebra_core.get_buffer(self._ptr, self._size)
        return memoryview(buffer)

    def as_msgspec(self, type_hint: type) -> Any:
        """Deserialize directly from buffer."""
        decoder = msgspec.json.Decoder(type_hint)
        return decoder.decode(self.as_bytes())

    def release(self):
        """Release buffer back to pool."""
        _vertebra_core.release_buffer(self._ptr)
```

**Performance Gain:** Zero-copy eliminates memcpy overhead, saving ~5-10% latency for large payloads.

---

## Developer Experience

### Basic Usage

```python
# app.py
from litestar import Litestar, Controller, get, post
from vertebra import procedure
from msgspec import Struct

class User(Struct):
    id: int
    name: str
    email: str

class UserController(Controller):
    path = "/users"

    @get("/{user_id:int}")
    @procedure
    async def get_user(self, user_id: int) -> User:
        return await db.get_user(user_id)

    @post("/")
    @procedure
    async def create_user(self, data: User) -> User:
        return await db.create_user(data)

app = Litestar(route_handlers=[UserController])
```

```typescript
// client.ts
import { api } from '@vertebra/client'

// Fully typed - autocomplete shows User fields
const user = await api.users.get_user({ user_id: 1 })
//    ^-- Type: User { id: number, name: string, email: string }

console.log(user.name)  // TypeScript knows this exists

// Type error if wrong params
await api.users.get_user({ userId: 1 })  // Error: unknown param 'userId'
```

### Advanced: Custom Serializer

```python
from vertebra import procedure, Serializer, TypeNode
import orjson

class OrjsonSerializer(Serializer):
    def serialize(self, value: Any, type_hint: type) -> bytes:
        return orjson.dumps(value)

    def deserialize(self, data: bytes, type_hint: type) -> Any:
        return orjson.loads(data)

    def get_type_metadata(self, type_hint: type) -> TypeNode:
        # Delegate to AST extractor
        return None

# Use custom serializer
@procedure(serializer=OrjsonSerializer())
async def fast_handler(...): ...
```

### Advanced: Type Overrides

For complex types that don't map cleanly:

```python
@procedure(
    type_overrides={
        "return": "User & { computed: string }"  # Extend type in TS
    }
)
async def enhanced_user(user_id: int):
    user = await get_user(user_id)
    user.computed = compute_value(user)
    return user
```

---

## Performance Targets & Benchmarks

### Type Extraction Pipeline

| Stage | Target | Rationale |
|-------|--------|-----------|
| AST Parsing | < 10ms/file | Small Python files (< 500 LOC) |
| Type Extraction | < 30ms/file | Walking AST, resolving references |
| Type IR Construction | < 5ms/procedure | Simple object construction |
| TypeScript Generation | < 15ms total | String concatenation, optimized |
| **Total Pipeline** | **< 100ms** | **HMR requirement for responsive DX** |

### Serialization Performance

Benchmark with 1KB payload (100 iterations):

| Serializer | Serialize | Deserialize | Total |
|------------|-----------|-------------|-------|
| msgspec | 0.05ms | 0.08ms | 0.13ms |
| pydantic | 0.25ms | 0.45ms | 0.70ms |
| orjson | 0.08ms | 0.12ms | 0.20ms |

**Target:** < 1ms serialization overhead for typical payloads.

### Memory Usage

- Procedure Registry: ~1KB per procedure
- AST Cache: ~50KB per cached file
- Type Cache: ~100KB for 500 procedures
- **Total Overhead:** < 10MB for typical application

---

## Implementation Phases

### Phase 1: Core Infrastructure (2 weeks)

**Goal:** Basic decorator and type extraction working for simple types.

**Milestones:**

1. **M1.1: Decorator & Registry** (3 days)
   - Implement `@procedure` decorator
   - ProcedureRegistry with thread-safe operations
   - Integration with Litestar route metadata
   - Unit tests for decorator stacking

2. **M1.2: Basic Type Extraction** (5 days)
   - AST-based extractor for primitives (int, str, bool, float)
   - Support for collections (list, dict, set)
   - Optional types (Optional[T], T | None)
   - Type IR foundation classes

3. **M1.3: TypeScript Generator** (4 days)
   - Generate .d.ts for basic types
   - Procedure signature generation
   - Module assembly and formatting
   - Output validation tests

**Deliverables:**
- Can register procedures with `@procedure`
- Extract types from simple handlers
- Generate valid TypeScript definitions

### Phase 2: Advanced Type System (3 weeks)

**Goal:** Support for Struct, BaseModel, and complex types.

**Milestones:**

1. **M2.1: msgspec.Struct Support** (5 days)
   - Extract Struct fields via AST
   - Handle nested Structs
   - Metadata extraction (constraints, defaults)
   - Integration with msgspec.inspect

2. **M2.2: pydantic Support** (4 days)
   - BaseModel field extraction
   - Validator support
   - model_json_schema integration
   - Migration from msgspec types

3. **M2.3: Advanced Types** (5 days)
   - Generic types (Generic[T])
   - Union types (T1 | T2)
   - Literal types
   - Annotated with metadata
   - Forward references resolution

4. **M2.4: Type Resolution** (3 days)
   - Circular reference detection
   - Import resolution across modules
   - Caching and incremental updates
   - Error handling and fallbacks

**Deliverables:**
- Full coverage for common Python type patterns
- Support for both msgspec and pydantic
- Robust error handling for edge cases

### Phase 3: Serialization & IPC (2 weeks)

**Goal:** Connect to Rust IPC layer with pluggable serialization.

**Milestones:**

1. **M3.1: Serialization Framework** (3 days)
   - Serializer protocol definition
   - MsgspecSerializer implementation
   - PydanticSerializer implementation
   - Auto-detection logic

2. **M3.2: PyO3 Bindings** (5 days)
   - Python FFI wrapper for Rust channel API
   - Async integration (asyncio ↔ Rust async)
   - Error handling across FFI boundary
   - **Depends on PRD-1 completion**

3. **M3.3: Zero-Copy Protocol** (4 days)
   - BufferView implementation
   - msgspec buffer protocol integration
   - Memory pool coordination with Rust
   - Performance benchmarks

4. **M3.4: End-to-End Integration** (2 days)
   - Procedure call flow (Python → Rust → TypeScript)
   - Error propagation
   - Integration tests
   - Performance profiling

**Deliverables:**
- Procedures can be called via IPC
- Serialization overhead < 1ms
- Zero-copy working for msgspec

### Phase 4: Developer Experience (2 weeks)

**Goal:** HMR, file watching, and production-ready DX.

**Milestones:**

1. **M4.1: File Watching** (3 days)
   - watchdog integration for source changes
   - Incremental type regeneration
   - Debouncing and batching
   - Error recovery

2. **M4.2: Type Bridge Endpoint** (3 days)
   - HTTP endpoint for types
   - Cache management
   - Version tracking (ETag headers)
   - Compression for large type files

3. **M4.3: HMR Integration** (4 days)
   - Vite plugin for type polling
   - WebSocket-based push (optional)
   - TypeScript language server integration
   - Multi-framework support (React, Vue, Svelte)

4. **M4.4: Performance & Polish** (4 days)
   - Optimize to < 100ms updates
   - Add telemetry/logging
   - Developer documentation
   - Example projects

**Deliverables:**
- Type changes reflect in IDE < 100ms
- Production-ready developer experience
- Comprehensive documentation

---

## Testing Strategy

### Unit Tests

**Type Extractor:**
```python
def test_extract_msgspec_struct():
    source = """
from msgspec import Struct

class User(Struct):
    id: int
    name: str
    email: str | None = None
"""
    extractor = TypeExtractor()
    type_node = extractor.from_source(source, "User")

    assert isinstance(type_node, ObjectType)
    assert type_node.name == "User"
    assert "id" in type_node.fields
    assert type_node.fields["id"][0] == PrimitiveType("int")
    assert type_node.fields["email"][1] == False  # Optional
```

**TypeScript Generator:**
```python
def test_generate_interface():
    user_type = ObjectType(
        name="User",
        fields={
            "id": (PrimitiveType("int"), True),
            "name": (PrimitiveType("str"), True),
            "email": (UnionType([PrimitiveType("str"), PrimitiveType("null")]), False)
        }
    )

    ts = user_type.to_typescript()
    assert "interface User" in ts
    assert "id: number" in ts
    assert "email?: string | null" in ts
```

**Serializer:**
```python
def test_msgspec_roundtrip():
    serializer = MsgspecSerializer()

    class User(Struct):
        id: int
        name: str

    user = User(id=1, name="Alice")
    data = serializer.serialize(user, User)
    result = serializer.deserialize(data, User)

    assert result == user
```

### Integration Tests

**End-to-End Procedure Call:**
```python
@pytest.mark.integration
async def test_procedure_call_e2e():
    # Setup
    class User(Struct):
        id: int
        name: str

    @procedure
    async def get_user(user_id: int) -> User:
        return User(id=user_id, name="Test")

    # Extract types
    type_bridge = TypeBridge()
    ts_code = type_bridge.get_types()

    # Verify TypeScript
    assert "function get_user" in ts_code
    assert "user_id: number" in ts_code
    assert "Promise<User>" in ts_code

    # Call via IPC
    channel = Channel("test")
    result = await channel.send("get_user", {"user_id": 1})
    assert result.id == 1
    assert result.name == "Test"
```

**File Watching:**
```python
@pytest.mark.integration
async def test_file_watch_triggers_regeneration(tmp_path):
    # Create Python file
    source_file = tmp_path / "handlers.py"
    source_file.write_text("""
from vertebra import procedure

@procedure
async def handler_v1() -> int:
    return 1
""")

    # Start type bridge
    bridge = TypeBridge(watch_paths=[str(tmp_path)])
    bridge.start()

    initial_types = bridge.get_types()
    assert "handler_v1" in initial_types

    # Modify file
    source_file.write_text("""
from vertebra import procedure

@procedure
async def handler_v2() -> str:
    return "updated"
""")

    # Wait for regeneration
    await asyncio.sleep(0.5)

    updated_types = bridge.get_types()
    assert "handler_v2" in updated_types
    assert "handler_v1" not in updated_types
```

### Performance Tests

**Type Extraction Benchmark:**
```python
@pytest.mark.benchmark
def test_extraction_performance(benchmark):
    # Large file with 50 procedures
    source = generate_large_source_file(num_procedures=50)

    extractor = TypeExtractor()

    def run_extraction():
        return extractor.from_source(source)

    result = benchmark(run_extraction)

    # Should complete in < 50ms
    assert result.stats.median < 0.050
```

**Serialization Benchmark:**
```python
@pytest.mark.benchmark
def test_msgspec_serialization(benchmark):
    class User(Struct):
        id: int
        name: str
        email: str

    serializer = MsgspecSerializer()
    user = User(id=1, name="Benchmark", email="test@example.com")

    def roundtrip():
        data = serializer.serialize(user, User)
        return serializer.deserialize(data, User)

    result = benchmark(roundtrip)

    # Should complete in < 1ms
    assert result.stats.median < 0.001
```

### Compatibility Tests

- Python 3.12, 3.13
- Litestar 2.x (latest stable)
- msgspec 0.18+
- pydantic 2.0+
- Multiple OS: Linux, macOS, Windows

---

## API Reference

### Decorator

```python
def procedure(
    name: str | None = None,
    serializer: str | Serializer | None = None,
    metadata: dict[str, Any] | None = None,
    type_overrides: dict[str, str] | None = None
) -> Callable
```

**Parameters:**

- `name`: Override procedure identifier (default: function name)
- `serializer`: Serializer backend ("msgspec", "pydantic", or custom instance)
- `metadata`: Custom metadata passed to TypeScript client
- `type_overrides`: Manual TypeScript type hints for edge cases

**Returns:** Decorator function

**Example:**
```python
@procedure(name="users.get", serializer="pydantic")
async def get_user(user_id: int) -> User: ...
```

### Registry API

```python
class ProcedureRegistry:
    def get_procedures(self) -> dict[str, ProcedureMetadata]: ...
    def get_procedure(self, procedure_id: str) -> ProcedureMetadata | None: ...
    def register_procedure(self, metadata: ProcedureMetadata) -> None: ...
    def unregister_procedure(self, procedure_id: str) -> None: ...
```

### Type Extractor

```python
class TypeExtractor:
    def from_type_hint(self, type_hint: type) -> TypeNode: ...
    def from_source_file(self, path: str | Path) -> dict[str, TypeNode]: ...
    def from_function(self, func: Callable) -> tuple[dict[str, TypeNode], TypeNode]: ...
```

### Configuration

```python
@dataclass
class VertebraConfig:
    default_serializer: str = "msgspec"
    type_generation_mode: Literal["auto", "manual"] = "auto"
    watch_paths: list[str] = field(default_factory=lambda: ["src/"])
    type_output_path: str | None = None
    enable_zero_copy: bool = True
    hmr_poll_interval: int = 1000  # ms
```

**Usage:**
```python
from vertebra import configure

configure(
    default_serializer="pydantic",
    watch_paths=["app/", "lib/"]
)
```

---

## Extension Points

### Custom Type Mapper

For types not natively supported:

```python
from vertebra.types import TypeMapper, TypeNode, register_type_mapper

class CustomTypeMapper(TypeMapper):
    def can_handle(self, python_type: type) -> bool:
        return isinstance(python_type, MyCustomClass)

    def to_type_ir(self, python_type: type) -> TypeNode:
        return ObjectType(
            name="CustomType",
            fields={"value": (PrimitiveType("str"), True)}
        )

register_type_mapper(CustomTypeMapper())
```

### Custom Serializer

```python
from vertebra import Serializer, register_serializer

class MySerializer(Serializer):
    def serialize(self, value: Any, type_hint: type) -> bytes: ...
    def deserialize(self, data: bytes, type_hint: type) -> Any: ...
    def get_type_metadata(self, type_hint: type) -> TypeNode | None: ...

register_serializer("custom", MySerializer())
```

### Type Transformation Hooks

```python
from vertebra.types import TypeTransformer, register_transformer

class AddTimestampTransformer(TypeTransformer):
    def transform(self, type_node: TypeNode, context: dict) -> TypeNode:
        """Add timestamp field to all object types."""
        if isinstance(type_node, ObjectType):
            type_node.fields["_timestamp"] = (PrimitiveType("int"), False)
        return type_node

register_transformer(AddTimestampTransformer())
```

---

## Error Handling

### Type Extraction Errors

**Scenario:** Forward reference can't be resolved

```python
# Python
@procedure
async def handler() -> "UnknownType":
    ...

# Generated TypeScript
function handler(): Promise<any>  // Fallback to 'any'

// Console warning: "Failed to resolve forward reference 'UnknownType' in handler()"
```

**Scenario:** Circular type dependency

```python
# Python
class Node(Struct):
    value: int
    next: "Node | None"

# Generated TypeScript
type Node = {
  value: number;
  next: Node | null;  // Uses 'type' keyword to handle recursion
}
```

### Serialization Errors

```python
try:
    await api.users.create_user({"name": 123})  # Wrong type
except ValidationError as e:
    print(e.errors())
    # [
    #   {
    #     "loc": ["name"],
    #     "msg": "Expected str, got int",
    #     "type": "type_error.str"
    #   }
    # ]
```

### IPC Errors

```python
try:
    await api.users.get_user({"user_id": 1})
except ChannelError as e:
    if e.code == "DISCONNECTED":
        # Retry logic
        await channel.reconnect()
    elif e.code == "TIMEOUT":
        # Handle timeout
        logger.error(f"Procedure timed out: {e}")
```

---

## Migration Guide

### From Manual Type Definitions

**Before:**
```python
# backend.py
async def get_user(user_id: int) -> dict:
    return {"id": user_id, "name": "User"}

# types.ts (manually maintained)
export interface User {
  id: number;
  name: string;
}
```

**After:**
```python
# backend.py
from vertebra import procedure
from msgspec import Struct

class User(Struct):
    id: int
    name: str

@procedure
async def get_user(user_id: int) -> User:
    return User(id=user_id, name="User")

# types.ts - AUTO-GENERATED, DO NOT EDIT
```

### From Existing Pydantic Models

```python
# No changes needed to models
class User(BaseModel):
    id: int
    name: str

# Just add @procedure decorator
@procedure(serializer="pydantic")
async def get_user(user_id: int) -> User:
    return User(id=user_id, name="User")
```

---

## Security Considerations

### Type Information Exposure

**Risk:** Type definitions may reveal internal structure

**Mitigation:**
- Type bridge endpoint only accessible in development
- Production builds serve pre-generated types (optional)
- Sensitive fields can be excluded via decorator metadata

```python
@procedure(
    metadata={"exclude_fields": ["password_hash"]}
)
async def get_user(...): ...
```

### Serialization Vulnerabilities

**Risk:** Malicious payloads exploiting deserialization

**Mitigation:**
- msgspec/pydantic provide built-in validation
- Enforce strict schema validation
- Size limits on payloads
- Sandboxed deserialization

### IPC Security

Handled by PRD-1 (Rust Core), includes:
- Process isolation
- Shared memory permissions
- Message authentication

---

## Performance Optimization Strategies

### Type Extraction

1. **AST Caching** - Cache parsed ASTs per file hash
2. **Incremental Updates** - Only regenerate changed types
3. **Parallel Extraction** - Extract multiple files concurrently
4. **Lazy Resolution** - Defer forward reference resolution

### Type Generation

1. **Deduplication** - Reuse identical type definitions
2. **Tree Shaking** - Only include types used by procedures
3. **Template Caching** - Cache TypeScript templates
4. **Minification** - Remove whitespace in production

### Serialization

1. **Zero-Copy** - Use buffer protocol for msgspec
2. **Encoder Reuse** - Cache msgspec encoders/decoders
3. **Lazy Deserialization** - Only deserialize used fields
4. **Binary Format** - Use MessagePack for large payloads

---

## Future Enhancements

### Phase 5+ (Future Work)

**GraphQL Integration:**
- Generate GraphQL schema from Python types
- Unified type system across REST, RPC, and GraphQL

**OpenAPI Schema:**
- Export procedures as OpenAPI 3.1 spec
- Integrate with Swagger UI

**Middleware Hooks:**
- Pre/post serialization hooks
- Type transformation middleware
- Custom validation logic

**Advanced TypeScript Features:**
- Branded types for nominal typing
- Template literal types for string patterns
- Conditional types for polymorphic returns

**Performance:**
- Rust-based AST parser (10x faster)
- WebAssembly type generator
- Streaming type updates (WebSocket)

**Developer Tools:**
- VSCode extension for inline type preview
- CLI for type validation
- Type coverage reporting

---

## Dependencies

### Python Dependencies

```toml
[project.dependencies]
vertebra-core = "^0.1.0"  # Rust core (PRD-1)
litestar = "^2.0"
msgspec = "^0.18"
watchdog = "^4.0"

[project.optional-dependencies]
pydantic = ["pydantic ^2.0"]
```

### External Dependencies

- **PRD-1 (Rust Core IPC Layer)** - Required for IPC channel
- **Litestar** - Web framework integration
- **msgspec** - Default serialization and type introspection
- **watchdog** - File system monitoring

---

## Success Metrics

### Developer Experience Metrics

- **Type Update Latency:** < 100ms (P95)
- **First-Time Setup:** < 5 minutes
- **Learning Curve:** < 30 minutes to first procedure
- **Migration Effort:** < 1 day for typical app

### Technical Metrics

- **Type Coverage:** > 80% of Python type constructs
- **Extraction Performance:** < 50ms per file
- **Generation Performance:** < 20ms for 100 procedures
- **Serialization Overhead:** < 1ms for 1KB payload
- **Memory Overhead:** < 10MB for 500 procedures

### Adoption Metrics

- **Framework Compatibility:** Works with all major TS frameworks
- **Python Version Support:** 3.12+
- **Breaking Changes:** < 1 per minor version

---

## Risks & Mitigation

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| AST parsing too slow | Medium | High | Cache ASTs, incremental updates, Rust fallback |
| Type IR incomplete | Low | Medium | Iterative expansion, escape hatches |
| HMR integration breaks | Medium | Medium | Support multiple bundlers, fallback to polling |
| Serialization overhead | Low | High | Benchmark early, optimize hot paths, zero-copy |
| PRD-1 delays | Medium | High | Stub FFI interface, parallel development |

### Product Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Poor adoption | Medium | High | Excellent docs, example projects, migration guide |
| Breaking changes | Low | Medium | Semantic versioning, deprecation warnings |
| Competition (tRPC clones) | Low | Low | Focus on Python ecosystem, unique features |

---

## Appendix A: Type Mapping Reference

### Python → TypeScript Type Mapping

| Python Type | TypeScript Type | Notes |
|-------------|-----------------|-------|
| `int` | `number` | - |
| `float` | `number` | - |
| `str` | `string` | - |
| `bool` | `boolean` | - |
| `None` | `null` | - |
| `bytes` | `Uint8Array` | Binary data |
| `list[T]` | `Array<T>` | Generic array |
| `dict[K, V]` | `Record<K, V>` | If K is string/number |
| `dict[str, Any]` | `{ [key: string]: any }` | Index signature |
| `set[T]` | `Set<T>` | Native Set |
| `tuple[T1, T2]` | `[T1, T2]` | Fixed-length tuple |
| `tuple[T, ...]` | `Array<T>` | Variable-length tuple |
| `Optional[T]` | `T \| null` | Union with null |
| `T \| U` | `T \| U` | Union type |
| `Literal["a", "b"]` | `"a" \| "b"` | String literal union |
| `Literal[1, 2]` | `1 \| 2` | Numeric literal union |
| `Any` | `any` | Escape hatch |
| `Struct` | `interface` | Named interface |
| `BaseModel` | `interface` | Named interface |
| `dataclass` | `interface` | Named interface |
| `TypedDict` | `interface` | Named interface |
| `Generic[T]` | `<T>` | Generic parameter (limited) |
| `Annotated[T, ...]` | `T` | Metadata stripped |

### Edge Cases

| Python Type | TypeScript Type | Limitation |
|-------------|-----------------|------------|
| `TypeVar` | `any` | No direct equivalent |
| `ParamSpec` | `...args: any[]` | Limited support |
| `Callable[[int], str]` | `(arg0: number) => string` | Basic support |
| `ClassVar[T]` | `T` | Static ignored |
| Forward ref (unresolved) | `any` | With warning |
| Circular ref | `type` keyword | Handled |

---

## Appendix B: Example Projects

### Minimal Example

```python
# app.py
from litestar import Litestar, get
from vertebra import procedure
from msgspec import Struct

class Message(Struct):
    text: str

@get("/hello")
@procedure
async def hello(name: str) -> Message:
    return Message(text=f"Hello, {name}!")

app = Litestar(route_handlers=[hello])
```

```typescript
// client.ts
import { api } from '@vertebra/client'

const message = await api.hello({ name: "World" })
console.log(message.text)  // "Hello, World!"
```

### Full-Featured Example

See `/examples/todo-app` in repository for complete CRUD application with:
- Multiple controllers
- Nested types
- Authentication
- File uploads
- Error handling

---

## Appendix C: Research Sources

### Technical References

1. **tRPC**
   - https://trpc.io/docs

2. **Python Typing & Annotation Semantics**
   - https://docs.python.org/3/library/typing.html
   - https://peps.python.org/pep-0563/
   - https://peps.python.org/pep-0649/

3. **Python AST**
   - https://docs.python.org/3/library/ast.html

4. **PyO3**
   - https://pyo3.rs/

5. **TypeScript**
   - https://www.typescriptlang.org/docs/

### Library Documentation

- [Litestar Documentation](https://litestar.dev) - Controller patterns, dependency injection
- [msgspec Documentation](https://jcristharif.github.io/msgspec/) - Type introspection, JSON schema
- [Pydantic Documentation](https://docs.pydantic.dev) - BaseModel, type validation

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-17 | Vertebra Team | Initial PRD |

---

**End of PRD-2: Python Bindings and Type Bridge**
