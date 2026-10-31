"""Modular Router composition, msgspec DTOs, and typed RPC ProcedureRouter example."""

import asyncio

import msgspec

from vertebra import App, Router, get, post
from vertebra.framework.procedures import ProcedureRouter

__all__ = (
    "CreateItemRequest",
    "ItemResponse",
    "build_items_router",
    "build_procedures",
    "main",
)


# --8<-- [start:example]
class CreateItemRequest(msgspec.Struct, frozen=True, gc=False):
    """Incoming payload for creating an inventory item."""

    sku: str
    quantity: int


class ItemResponse(msgspec.Struct, frozen=True, gc=False):
    """Serialized inventory item returned to callers."""

    id: int
    sku: str
    quantity: int


def build_items_router() -> Router:
    """Build a modular router for inventory item endpoints."""
    router = Router(prefix="/api/v1/items")

    @post("/")
    def create_item(body: CreateItemRequest) -> ItemResponse:
        return ItemResponse(id=1, sku=body.sku, quantity=body.quantity)

    @get("/{item_id:int}")
    def get_item(item_id: int) -> ItemResponse:
        return ItemResponse(id=item_id, sku="VTB-100", quantity=5)

    router.register(create_item)
    router.register(get_item)
    return router


def build_procedures() -> ProcedureRouter:
    """Build a typed RPC router with query and mutation procedures."""
    rpc = ProcedureRouter(prefix="/_rpc")

    @rpc.mutation(input_type=CreateItemRequest, output_type=ItemResponse)
    async def reserve_item(payload: CreateItemRequest) -> ItemResponse:
        return ItemResponse(id=9, sku=payload.sku, quantity=payload.quantity)

    return rpc


def main() -> None:
    """Mount the items router on an App and invoke the typed RPC procedure."""
    app = App()
    app.register(build_items_router())
    assert len(app.routes) == 2

    rpc = build_procedures()
    req = CreateItemRequest(sku="VTB-001", quantity=12)
    result = asyncio.run(rpc.call("reserve_item", req))
    assert result == ItemResponse(id=9, sku="VTB-001", quantity=12)


# --8<-- [end:example]


if __name__ == "__main__":
    main()
