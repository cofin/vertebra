from typing import Callable, Any, Optional
from dataclasses import dataclass, field

@dataclass
class Tool:
    name: str
    handler: Callable[..., Any]
    description: Optional[str] = None
    input_schema: Optional[dict] = None
    params: dict = field(default_factory=dict)
    return_type: Any = None

class ProcedureRegistry:
    _instance = None

    def __init__(self):
        self._tools: dict[str, Tool] = {}

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

    def get_tool(self, name: str) -> Optional[Tool]:
        return self._tools.get(name)

    def list_tools(self) -> list[Tool]:
        return list(self._tools.values())

    def clear(self):
        self._tools.clear()