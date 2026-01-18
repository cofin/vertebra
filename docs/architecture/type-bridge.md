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
- `types.bridge`
  - Generates and caches TypeScript definitions.
  - Serves `/__vertebra/types.d.ts` with ETag + gzip support.
- `types.watch`
  - Debounced file watcher to invalidate cached types on `.py` changes.
- `@vertebra/vite-plugin` (TS)
  - Polls `/__vertebra/types.d.ts` and updates a virtual module for IDE types.
  - Triggers full reload on type changes.

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
- Type bridge endpoint + ASGI app for in-process serving.
- Debounced file watcher for in-memory regeneration.
- Vite plugin with polling + virtual module support (`virtual:vertebra-types`).

### Vite Plugin Usage (Dev)

```ts
import { defineConfig } from \"vite\";
import { vertebraTypes } from \"vertebra/codegen\";

export default defineConfig({
  plugins: [vertebraTypes({ endpoint: \"http://127.0.0.1:8000/__vertebra/types.d.ts\" })],
});
```

Import the virtual module once in your client entry to load types:

```ts
import type \"virtual:vertebra-types\";
```

## Next Steps

- Vite HMR plugin wiring in apps (polling or websocket).
- File watching + regeneration tuning (< 100ms updates).
