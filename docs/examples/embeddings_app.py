"""Shared-memory vector embeddings, Matryoshka truncation, and top-k search example."""

__all__ = ("SearchHit", "SearchRequest", "SearchResponse", "VectorSearchController", "app")

# --8<-- [start:example]
from typing import Annotated

import msgspec

from vertebra import App, Controller, Inject, Provide, post
from vertebra.embeddings import Embedder, StaticEmbedder


class SearchRequest(msgspec.Struct):
    """Query payload for semantic vector search."""

    query: str
    documents: list[str]
    top_k: int = 2


class SearchHit(msgspec.Struct):
    """Ranked semantic search match."""

    index: int
    score: float
    document: str


class SearchResponse(msgspec.Struct):
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


app = App(route_handlers=[VectorSearchController])

# --8<-- [end:example]
