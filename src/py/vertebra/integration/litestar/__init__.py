from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from litestar import Litestar
    from litestar.routes import HTTPRoute, WebSocketRoute, ASGIRoute

@dataclass
class LitestarRoute:
    path: str
    methods: set[str]
    handler: Callable
    handler_id: int
    is_websocket: bool = False
    guards: list[Callable] = field(default_factory=list)
    dependencies: dict[str, Any] = field(default_factory=dict)

def reflect_routes(app: Litestar) -> list[LitestarRoute]:
    """Reflect all routes from a Litestar application.
    
    Traverses the application's route tree and extracts metadata for each handler.
    Assigns a stable integer ID to each route handler for use in the Rust router.
    """
    from litestar.routes import HTTPRoute, WebSocketRoute
    
    routes: list[LitestarRoute] = []
    
    # Litestar's app.routes contains the flattened list of routes
    for route in app.routes:
        if isinstance(route, HTTPRoute):
            for method, handler_tuple in route.route_handler_map.items():
                handler = handler_tuple[0]
                # In Litestar, the handler object itself is what we want.
                # The handler_id should be stable. We can use hash of the handler or id().
                # For now, we'll use id() but in a real app we might want something more stable across restarts if possible.
                # However, since this is built at startup, id() is fine for the session.
                
                # Check if we already have this handler for this path (different methods might share handlers)
                existing = next((r for r in routes if r.path == route.path and r.handler == handler.fn), None)
                if existing:
                    existing.methods.add(method)
                    continue

                routes.append(LitestarRoute(
                    path=route.path,
                    methods={method},
                    handler=handler.fn,
                    handler_id=id(handler.fn),
                    is_websocket=False,
                    guards=handler.resolve_guards(),
                    dependencies=handler.resolve_dependencies(),
                ))
        elif isinstance(route, WebSocketRoute):
            handler = route.route_handler
            routes.append(LitestarRoute(
                path=route.path,
                methods={"WEBSOCKET"},
                handler=handler.fn,
                handler_id=id(handler.fn),
                is_websocket=True,
                guards=handler.resolve_guards(),
                dependencies=handler.resolve_dependencies(),
            ))
            
    return routes
