"""Controller, Router composition, dependency injection, msgspec DTOs, and ProcedureRouter example."""

import asyncio

import msgspec

from vertebra import App, Controller, Provide, Router, get, post
from vertebra.framework.procedures import ProcedureRouter

__all__ = (
    "CreateItemRequest",
    "InventoryService",
    "ItemController",
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


class InventoryService:
    """Domain service injected into controllers and routes."""

    def fetch(self, item_id: int) -> ItemResponse:
        """Fetch an item by identifier."""
        return ItemResponse(id=item_id, sku="VTB-100", quantity=5)


class ItemController(Controller):
    """Controller grouping item routes under a shared path and dependencies."""

    path = "/items"
    dependencies = {"service": Provide(InventoryService, scope="singleton")}

    @post("/", status_code=201)
    def create_item(self, body: CreateItemRequest) -> ItemResponse:
        """Create a new item from a typed msgspec payload."""
        return ItemResponse(id=1, sku=body.sku, quantity=body.quantity)

    @get("/{item_id:int}")
    def get_item(self, item_id: int, service: InventoryService) -> ItemResponse:
        """Return an item using the injected InventoryService."""
        return service.fetch(item_id)


def build_items_router() -> Router:
    """Build a modular router mounting ItemController under /api/v1."""
    return Router(prefix="/api/v1", route_handlers=[ItemController])


def build_procedures() -> ProcedureRouter:
    """Build a typed RPC router with query and mutation procedures."""
    rpc = ProcedureRouter(prefix="/_rpc")

    @rpc.mutation(input_type=CreateItemRequest, output_type=ItemResponse)
    async def reserve_item(payload: CreateItemRequest) -> ItemResponse:
        return ItemResponse(id=9, sku=payload.sku, quantity=payload.quantity)

    return rpc


def main() -> None:
    """Mount the items router on an App and invoke the typed RPC procedure."""
    app = App(route_handlers=[build_items_router()])
    assert len(app.routes) == 2

    rpc = build_procedures()
    req = CreateItemRequest(sku="VTB-001", quantity=12)
    result = asyncio.run(rpc.call("reserve_item", req))
    assert result == ItemResponse(id=9, sku="VTB-001", quantity=12)


# --8<-- [end:example]


if __name__ == "__main__":
    main()
