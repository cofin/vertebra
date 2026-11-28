"""Inventory routes with typed request bodies, dependency injection, and guards."""

# --8<-- [start:example]
import os
from hmac import compare_digest

import msgspec

from vertebra import App, Controller, Provide, Router, get, post
from vertebra._vertebra import Request
from vertebra.framework.security import Principal, requires_role, requires_scope


class CreateItemRequest(msgspec.Struct):
    """Fields clients supply when registering an item."""

    sku: str
    quantity: int


class ItemResponse(msgspec.Struct):
    """Inventory item fields returned to the client."""

    id: int
    sku: str
    quantity: int


class InventoryService:
    """Inventory operations for the local example."""

    def fetch(self, item_id: int) -> ItemResponse:
        """Return the sample item for the requested ID."""
        return ItemResponse(id=item_id, sku="VTB-100", quantity=5)

    def register(self, body: CreateItemRequest) -> ItemResponse:
        """Build the item representation for a registration request."""
        return ItemResponse(id=1, sku=body.sku, quantity=body.quantity)


class ItemController(Controller):
    """Group inventory routes under a common path and provider."""

    path = "/items"
    dependencies = {"inventory": Provide(InventoryService, scope="singleton")}

    @get("/{item_id:int}")
    async def get_item(self, item_id: int, inventory: InventoryService) -> ItemResponse:
        """Return an item by ID."""
        return inventory.fetch(item_id)

    @post("/", status_code=201, guards=[requires_scope("inventory:write")])
    async def create_item(self, body: CreateItemRequest, inventory: InventoryService) -> ItemResponse:
        """Register an inventory item."""
        return inventory.register(body)


@get("/health")
async def health() -> dict[str, str]:
    """Report the service status on a public route."""
    return {"status": "ok"}


def create_app(token: str | None = None) -> App:
    """Assemble a fresh app using the local inventory credential."""
    secret = token if token is not None else os.environ.get("VERTEBRA_INVENTORY_TOKEN", "")

    def provide_principal(request: Request) -> Principal | None:
        """Authenticate the local bearer token and supply the inventory principal."""
        authorization = request.headers.get("authorization") or ""
        scheme, _, supplied = authorization.partition(" ")
        if secret and scheme.lower() == "bearer" and compare_digest(supplied.encode(), secret.encode()):
            return Principal(
                subject="local-inventory",
                roles=frozenset({"inventory"}),
                scopes=frozenset({"inventory:write"}),
            )
        return None

    version = Router(prefix="/v1", route_handlers=[ItemController])
    api = Router(prefix="/api", route_handlers=[version], guards=[requires_role("inventory")])
    return App(
        route_handlers=[health, api],
        dependencies={"principal": Provide(provide_principal, scope="request")},
    )


app = create_app()
# --8<-- [end:example]


__all__ = (
    "CreateItemRequest",
    "InventoryService",
    "ItemController",
    "ItemResponse",
    "app",
    "create_app",
    "health",
)
