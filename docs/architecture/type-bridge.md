# Type Bridge Architecture

## Overview

The Type Bridge converts Python type annotations into TypeScript declarations
with no visible code generation. The current implementation focuses on the
core building blocks (decorator, registry, IR, extractor, and generator).

## Components

- `procedures.decorator` (`@procedure`)
  - Captures handler metadata and registers it in a global registry.
  - Attaches `vertebra` metadata into Litestar `opt` dict when present.
- `procedures.registry`
  - Thread-safe singleton storing `ProcedureMetadata`.
- `types.ir`
  - Intermediate representation for primitives, arrays, objects, unions,
    tuples, generics, literals, and unknown/raw types.
- `types.extractor`
  - Maps Python annotations to Type IR.
  - Caches AST parsing by file hash and collects class definitions.
- `types.typescript`
  - Generates `.d.ts` output with namespace grouping and topological ordering.
  - Optional validation through `tsc` (prefers `bun run -C src/ts tsc`; falls back to `bunx tsc`).
  - Optional formatting through `prettier` (`bunx prettier` if available).

## Current Capabilities

- Primitive/collection/union/optional extraction.
- msgspec.Struct AST extraction with nested references and circular handling.
- msgspec.inspect runtime metadata merge (best-effort when msgspec is installed).
- pydantic.BaseModel AST extraction and runtime metadata merge (Field description/alias).
- Pydantic JSON Schema fallback with `$ref` resolution to named types.
- Discriminator metadata surfaced from JSON Schema unions.
- Dataclass and TypedDict AST extraction (InitVar/ClassVar + Required/NotRequired handled).
- Generic classes (`Generic[T]`) map to TypeScript generics with TypeVar bounds/constraints emitted as `extends`.
- Forward-reference resolution via AST import maps with cached class lookups.
- Cyclic types emit `type` aliases to keep recursive references valid in TS.
- Unresolved references fall back to `unknown` and emit runtime warnings.
- Annotated metadata captured in IR and emitted as TypeScript comments.
- Named object types with dependency ordering.
- Deduplication by name with conflict detection.
- Optional TypeScript compiler validation.

## Next Steps

- Serializer protocol + IPC plumbing (PRD-2 Phase 3).
- HMR dev server + file watching for in-memory type updates.
