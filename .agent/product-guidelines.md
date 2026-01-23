# Product Guidelines: Vertebra

## Core Philosophy

**Elite Performance & Design:** Vertebra is built to be a technical masterpiece. We prioritize extreme performance, efficiency, and scalability above all else. The architecture must be elegant, readable, and demonstrate world-class engineering. Complexity is acceptable *only* if it unlocks superior performance (e.g., zero-copy mechanisms).

## Architectural Principles

- **Zero-Copy First:** Eliminate data copying and serialization overhead at all costs. Shared memory and Arrow are the default; serialization is a fallback only when absolutely necessary.
- **Polyglot Harmony:** Seamlessly bridge Python and TypeScript. The goal is a unified developer experience where boundaries between languages dissolve.
- **Type Safety as a Service:** Automatic, transparent type generation from Python (Pydantic, Attrs, Dataclasses, Structs) to TypeScript. The frontend must always be in sync with the backend.
- **Modular Monorepo:** Maintain strict separation of concerns (py/ts/rs) while ensuring cohesive integration. Each module should be independently testable and deployable.
- **Bun-First for Hot Paths:** Delegate CPU-intensive operations (auth validation, password hashing, CSRF) to the Bun layer. Python focuses on business logic; Bun handles ingress security and crypto ops.
- **Lazy by Default:** Defer computation until needed. User loading, header parsing, and JSON deserialization happen on first access, not eagerly.

## Design Constraints

- **Bridge-Only Communication:** All data flow between Python and TypeScript MUST go through the optimized IPC bridges. No side channels.
- **Strict Typing:** No `any` types in TypeScript or untyped Python code without explicit, documented justification.
- **Efficiency over Simplicity:** When faced with a tradeoff between a simple, slow solution and a complex, zero-copy solution, choose the zero-copy solution.

## Developer Experience (DX) Goals

- **"It Just Works" Typing:** Developers should define a model in Python and immediately see it available in their TypeScript IDE.
- **Readable Excellence:** Code should be self-documenting and aesthetically pleasing. It should serve as a reference for high-quality system design.
