# Product Guidelines: Vertebra

## Core Philosophy

**Elite Performance & Uncompromised Engineering:** Vertebra is built to be a technical masterpiece. We prioritize extreme performance, efficiency, and scalability above all else. **Performance is non-negotiable.** We never compromise on efficiency for the sake of cost, complexity, or development duration. If a pattern is 1% faster but 10x harder to implement, we choose the faster pattern.

## Architectural Principles

- **Zero-Copy Absolute Mandate:** Data copying is a failure. Shared memory and **Apache Arrow** are the primary transports for all data movement. Serialization is a last-resort fallback.
- **Polyglot Harmony:** Seamlessly bridge Python and TypeScript. The goal is a unified developer experience where boundaries between languages dissolve.
- **Modular Runtime Abstraction:** While Bun is our primary high-performance runtime, the architecture MUST remain modular. The system must support pluggable runtimes (e.g., Node.js, Deno) via standard interfaces, preventing vendor lock-in to Bun.
- **Type Safety as a Service:** Automatic, transparent type generation from Python to the frontend. The system must guarantee contract fidelity across language boundaries.
- **Lazy by Default:** Defer computation until the last responsible moment. User loading, header parsing, and deserialization happen on first access, never eagerly.

## Design Constraints

- **Bridge-Only Communication:** All data flow between Python and the Runtime (Bun/Node) MUST go through the optimized IPC bridges. No side channels.
- **Strict Typing:** No `any` types in TypeScript or untyped Python code without explicit, documented justification.
- **Efficiency over Simplicity:** When faced with a tradeoff between a simple, slow solution and a complex, zero-copy solution, **always choose the zero-copy solution.**

## Developer Experience (DX) Goals

- **"It Just Works" Typing:** Developers should define a model in Python and immediately see it available in their frontend IDE.
- **Readable Excellence:** Code should be self-documenting and aesthetically pleasing. It should serve as a reference for high-quality system design.
