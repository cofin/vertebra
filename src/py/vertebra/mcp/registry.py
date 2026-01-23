from dataclasses import dataclass, field
from typing import Callable, Any, Optional

@dataclass
class Tool:
    name: str
    handler: Callable[..., Any]
    description: Optional[str] = None
    input_schema: Optional[dict] = None
    params: dict = field(default_factory=dict)
    return_type: Any = None

@dataclass
class Resource:
    uri_template: str
    handler: Callable[..., Any]
    name: str
    description: Optional[str] = None
    mime_type: Optional[str] = None

class ProcedureRegistry:
    _instance = None

    def __init__(self):
        self._tools: dict[str, Tool] = {}
        self._resources: dict[str, Resource] = {}

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = ProcedureRegistry()
        return cls._instance

    def register_tool(self, name: str, handler: Callable, description: Optional[str] = None):
        self._tools[name] = Tool(
            name=name,
            handler=handler,
            description=description
        )

    def register_resource(
        self, 
        uri_template: str, 
        handler: Callable, 
        name: str, 
        description: Optional[str] = None,
        mime_type: Optional[str] = None
    ):
        self._resources[uri_template] = Resource(
            uri_template=uri_template,
            handler=handler,
            name=name,
            description=description,
            mime_type=mime_type
        )

    def get_tool(self, name: str) -> Optional[Tool]:
        return self._tools.get(name)

    def get_resource(self, uri_template: str) -> Optional[Resource]:
        return self._resources.get(uri_template)

    def list_tools(self) -> list[Tool]:
        return list(self._tools.values())

    def list_resources(self) -> list[Resource]:
        return list(self._resources.values())

    def clear(self):
        self._tools.clear()
        self._resources.clear()
