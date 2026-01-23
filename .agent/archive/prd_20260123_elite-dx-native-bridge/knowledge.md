# Knowledge Capture: Elite DX & Native Bridging (Shared Memory)

## Patterns Discovered
- **Hybrid RPC/Subscription Client**: Implemented a client that supports both standard `Promise`-based RPC and `Subscription`-based streaming by detecting `AsyncGenerator` return types.
- **Native State Primitives**: `msgspec.Struct` is mutable by default and supports `__setattr__` overriding, making it an excellent base for observable state objects.
- **Asset Serving in Router**: Integrating asset serving and HMR proxying directly into the router simplifies the developer experience for hybrid apps.

## Lessons Learned
- **Type Extraction for Streams**: Extractor needed to be updated to recognize `AsyncGenerator` and friends to correctly map them to `StreamType`.
- **Double Serialization**: Care must be taken when streaming pre-serialized data (bytes) vs objects. `StreamingResponse` expects objects to serialize them. `NativeState` broadcasting pre-serialized bytes required adjustment to ensure the stream handler yielded objects or handled raw bytes correctly. We opted for yielding objects (copies) to let the serializer handle it.

## Decisions Made
- **NativeState**: Implemented as a base class inheriting from `msgspec.Struct` with `__setattr__` hook for auto-broadcasting.
- **Subscription Interface**: Added `Subscription<T>` interface to TypeScript definitions to support streaming clients.

## Reusable Components
- `NativeState`: Base class for any synchronizable state.
- `StreamingResponse`: Generic wrapper for `AsyncIterable` to NDJSON.
- `useVertebraState`: React hook for consuming state streams.

## Gotchas & Warnings
- **msgspec Immutability**: While `Struct` is mutable by default, if configured otherwise, `__setattr__` tricks might fail.
- **Circular Dependencies**: Watch out for circular imports when using `typing.TYPE_CHECKING`.