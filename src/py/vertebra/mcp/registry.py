from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Tool:
    name: str
    handler: Callable[..., Any]
    description: str | None = None
    input_schema: dict | None = None
    params: dict = field(default_factory=dict)
    return_type: Any = None


@dataclass
class Resource:
    uri_template: str
    handler: Callable[..., Any]
    name: str
    description: str | None = None
    mime_type: str | None = None


class ProcedureRegistry:
    _instance = None

    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}
        self._resources: dict[str, Resource] = {}

    @classmethod
    def get_instance(cls) -> "ProcedureRegistry":
        if cls._instance is None:
            cls._instance = ProcedureRegistry()
        return cls._instance

    def register_tool(self, name: str, handler: Callable, description: str | None = None) -> None:
        self._tools[name] = Tool(name=name, handler=handler, description=description)

    def register_resource(
        self,
        uri_template: str,
        handler: Callable,
        name: str,
        description: str | None = None,
        mime_type: str | None = None,
    ) -> None:
        self._resources[uri_template] = Resource(
            uri_template=uri_template, handler=handler, name=name, description=description, mime_type=mime_type
        )

    def get_tool(self, name: str) -> Tool | None:
        return self._tools.get(name)

    def get_resource(self, uri_template: str) -> Resource | None:
        return self._resources.get(uri_template)

    def list_tools(self) -> list[Tool]:
        return list(self._tools.values())

    def list_resources(self) -> list[Resource]:
        return list(self._resources.values())

    def clear(self) -> None:
        self._tools.clear()
        self._resources.clear()
