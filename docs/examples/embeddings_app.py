"""Shared-memory vector embeddings, Matryoshka truncation, and top-k search example."""

import asyncio
from typing import Annotated

import msgspec

from vertebra import App, Controller, Inject, Provide, post
from vertebra.embeddings import Embedder, StaticEmbedder

__all__ = (
    "SearchHit",
    "SearchRequest",
    "SearchResponse",
    "VectorSearchController",
    "create_app",
    "main",
    "run_embedding_workflow",
)


# --8<-- [start:example]
class SearchRequest(msgspec.Struct, frozen=True, gc=False):
    """Query payload for semantic vector search."""

    query: str
    documents: list[str]
    top_k: int = 2


class SearchHit(msgspec.Struct, frozen=True, gc=False):
    """Ranked semantic search match."""

    index: int
    score: float
    document: str


class SearchResponse(msgspec.Struct, frozen=True, gc=False):
    """Top-k semantic search response."""

    dimensions: int
    hits: list[SearchHit]


def build_embedder() -> Embedder:
    """Create a deterministic shared-memory embedder for local execution."""
    return StaticEmbedder(dimensions=512)


class VectorSearchController(Controller):
    """Controller exposing semantic search over shared-memory vectors."""

    path = "/search"
    dependencies = {"embedder": Provide(build_embedder, scope="singleton")}

    @post("/")
    async def search(
        self,
        body: SearchRequest,
        embedder: Annotated[Embedder, Inject()],
    ) -> SearchResponse:
        """Embed query and documents into SHM and return top-k matches."""
        query_view = await embedder.embed_query(body.query)
        doc_batch = await embedder.embed_many(body.documents)

        short_query = query_view.truncate(256)
        short_docs = doc_batch.truncate(256)
        ranked = embedder.similarity_search(
            short_query,
            short_docs,
            top_k=body.top_k,
        )
        hits = [
            SearchHit(
                index=match.index,
                score=match.score,
                document=body.documents[match.index],
            )
            for match in ranked
        ]
        return SearchResponse(dimensions=short_docs.dimensions, hits=hits)


def create_app() -> App:
    """Mount VectorSearchController onto a Vertebra App."""
    return App(route_handlers=[VectorSearchController])


async def run_embedding_workflow() -> None:
    """Verify zero-copy PEP 3118 buffer views, truncation, and top-k search."""
    embedder = build_embedder()
    docs = [
        "Shared-memory ring buffers eliminate IPC serialization copies.",
        "Cloud SQL and AlloyDB mTLS dialers rotate ephemeral certificates.",
        "Matryoshka truncation slices leading dimensions and re-normalizes.",
    ]
    query_view = await embedder.embed_query("shared-memory IPC rings")
    batch_view = await embedder.embed_many(docs)

    raw_view = memoryview(batch_view)
    assert raw_view.ndim == 2
    assert raw_view.shape == (3, 512)

    truncated_query = query_view.truncate(256)
    truncated_batch = batch_view.truncate(256)
    assert truncated_query.dimensions == 256
    assert truncated_batch.dimensions == 256

    pg_wire = truncated_query.to_pgvector_bytes()
    oracle_wire = truncated_query.to_oracle_bytes()
    assert len(pg_wire) == 4 + 256 * 4
    assert oracle_wire[:4] == b"\xdb\x01\x02\x00"

    hits = embedder.similarity_search(truncated_query, truncated_batch, top_k=2)
    assert len(hits) == 2
    assert hits[0].index == 0
    assert hits[0].score >= hits[1].score


def main() -> None:
    """Mount the vector search app and run the shared-memory workflow."""
    app = create_app()
    assert len(app.routes) == 1
    asyncio.run(run_embedding_workflow())


# --8<-- [end:example]


if __name__ == "__main__":
    main()
