"""Web module for Vertebra.

Provides:
- middleware: ASGI middleware components
- guards: Route guards and access control
- controllers: Controller base classes
- routing: Route decorators and path handling
"""

from vertebra.web import controllers, guards, middleware, routing

__all__ = ["controllers", "guards", "middleware", "routing"]
