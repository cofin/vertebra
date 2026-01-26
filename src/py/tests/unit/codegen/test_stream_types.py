from collections.abc import AsyncGenerator, Iterator

from vertebra.codegen.extractor import TypeExtractor
from vertebra.codegen.ir import StreamType
from vertebra.codegen.typescript import TypeScriptGenerator
from vertebra.procedures.registry import ProcedureMetadata


def test_async_generator_extraction() -> None:
    extractor = TypeExtractor()
    node = extractor.from_hint(AsyncGenerator[str, None])
    assert isinstance(node, StreamType)
    assert node.item.kind == "string"


def test_iterator_extraction() -> None:
    extractor = TypeExtractor()
    node = extractor.from_hint(Iterator[int])
    assert isinstance(node, StreamType)
    assert node.item.kind == "number"


def test_typescript_subscription_generation() -> None:
    extractor = TypeExtractor()
    return_type = extractor.from_hint(AsyncGenerator[str, None])

    from pathlib import Path

    proc = ProcedureMetadata(
        procedure_id="api.stream_test",
        handler_fn=lambda: None,
        source_file=Path("test.py"),
        params={},
        return_type=return_type,
        serializer="msgpack",
    )

    generator = TypeScriptGenerator()
    ts = generator.generate([proc])

    assert "export interface Subscription<T>" in ts
    assert "export type stream_test = (params: { }) => Promise<AsyncIterable<string>>" in ts
    # Wait, StreamType.to_typescript returns AsyncIterable<T> by default in IR.
    # But for RPC client we might want Subscription<T> or just AsyncIterable.
    # The plan said "Update typescript.py to generate Subscription<T> interfaces".
    # And "Register as type='subscription'".

    # If the return type is StreamType, we might want to map it to Subscription in the procedure signature
    # or keep it as AsyncIterable and let the client wrap it.
    # Let's check IR implementation:
    # StreamType.to_typescript() -> "AsyncIterable<...>"
    # SubscriptionType.to_typescript() -> "Subscription<...>"

    # The extractor maps AsyncGenerator -> StreamType.
    # So by default it generates AsyncIterable.
    # If we want Subscription, we should probably update StreamType.to_typescript
    # or map it differently in the generator.

    # The requirement is: "Generate Subscription<T> types in TypeScript."
    # Let's verify what `AsyncIterable` renders to.
