"""Registry for MCP tools, resources, templates, prompts, and schema caching.

This module maintains the registered catalog of capabilities exposed by an MCP server,
computing schema digests and ETags across mutations.
"""

import re
from collections.abc import Callable
from typing import Any

from vertebra.agent.core.schema import SchemaDigestCache, Tool
from vertebra.agent.mcp.models import (
    PromptEntry,
    ResourceEntry,
    ResourceTemplateEntry,
)
from vertebra.utils.serializers import encode_json_bytes


class MCPRegistry:
    """Central catalog of tools, resources, resource templates, and prompts."""

    def __init__(self, schema_cache: SchemaDigestCache | None = None) -> None:
        self._tools: dict[str, Tool] = {}
        self._resources: dict[str, ResourceEntry] = {}
        self._templates: dict[str, ResourceTemplateEntry] = {}
        self._prompts: dict[str, PromptEntry] = {}
        self._completions: dict[str, Callable[..., Any]] = {}
        self._skill_controllers: dict[str, Any] = {}
        self._listeners: list[Callable[[], None]] = []
        self.schema_cache = schema_cache if schema_cache is not None else SchemaDigestCache()

    @property
    def skill_controllers(self) -> dict[str, Any]:
        """Return registered skill controllers."""
        return self._skill_controllers

    def add_listener(self, listener: Callable[[], None]) -> None:
        """Register a callback invoked whenever the registry is mutated."""
        self._listeners.append(listener)

    def _notify(self) -> None:
        for listener in self._listeners:
            listener()

    def register_tool(self, tool: Tool) -> None:
        """Register a tool and cache its schema digest."""
        self._tools[tool.name] = tool
        schema_bytes = encode_json_bytes(tool.input_schema)
        self.schema_cache.put(f"tool:{tool.name}", schema_bytes)
        self._notify()

    def get_tool(self, name: str) -> Tool | None:
        """Retrieve a tool descriptor by name."""
        return self._tools.get(name)

    def list_tools(self) -> list[Tool]:
        """Return all registered tools sorted by name."""
        return [self._tools[k] for k in sorted(self._tools.keys())]

    def register_resource(self, resource: ResourceEntry) -> None:
        """Register a static resource descriptor."""
        self._resources[resource.uri] = resource
        res_dict = {
            "uri": resource.uri,
            "name": resource.name,
            "description": resource.description,
            "mimeType": resource.mimeType,
        }
        self.schema_cache.put(f"resource:{resource.uri}", encode_json_bytes(res_dict))
        self._notify()

    def get_resource(self, uri: str) -> ResourceEntry | None:
        """Retrieve a static resource descriptor by exact URI."""
        return self._resources.get(uri)

    def list_resources(self) -> list[ResourceEntry]:
        """Return all registered static resources sorted by URI."""
        return [self._resources[k] for k in sorted(self._resources.keys())]

    def register_resource_template(self, template: ResourceTemplateEntry) -> None:
        """Register a dynamic URI template resource descriptor."""
        self._templates[template.uriTemplate] = template
        tmpl_dict = {
            "uriTemplate": template.uriTemplate,
            "name": template.name,
            "description": template.description,
            "mimeType": template.mimeType,
        }
        self.schema_cache.put(f"template:{template.uriTemplate}", encode_json_bytes(tmpl_dict))
        self._notify()

    def list_resource_templates(self) -> list[ResourceTemplateEntry]:
        """Return all registered resource templates sorted by template string."""
        return [self._templates[k] for k in sorted(self._templates.keys())]

    def match_resource_template(self, uri: str) -> tuple[ResourceTemplateEntry, dict[str, str]] | None:
        """Match an incoming URI against registered URI templates and extract variables."""
        for pattern_str, template in self._templates.items():
            param_names: list[str] = re.findall(r"\{([a-zA-Z0-9_]+)\}", pattern_str)
            regex_pattern = "^" + re.sub(r"\{[a-zA-Z0-9_]+\}", r"([^/]+)", pattern_str) + "$"
            match = re.match(regex_pattern, uri)
            if match:
                extracted = dict(zip(param_names, match.groups(), strict=False))
                return template, extracted
        return None

    def register_prompt(self, prompt: PromptEntry) -> None:
        """Register a prompt template descriptor."""
        self._prompts[prompt.name] = prompt
        prompt_dict = {
            "name": prompt.name,
            "description": prompt.description,
            "arguments": [
                {"name": a.name, "description": a.description, "required": a.required} for a in prompt.arguments
            ],
        }
        self.schema_cache.put(f"prompt:{prompt.name}", encode_json_bytes(prompt_dict))
        self._notify()

    def get_prompt(self, name: str) -> PromptEntry | None:
        """Retrieve a prompt template by name."""
        return self._prompts.get(name)

    def list_prompts(self) -> list[PromptEntry]:
        """Return all registered prompt templates sorted by name."""
        return [self._prompts[k] for k in sorted(self._prompts.keys())]

    def register_completion(self, name: str, callback: Callable[..., Any]) -> None:
        """Register a completion handler for an argument or resource."""
        self._completions[name] = callback

    def get_completion(self, name: str) -> Callable[..., Any] | None:
        """Retrieve a completion callback by name."""
        return self._completions.get(name)

    def register_controller(self, controller: Any) -> None:
        """Discover and register all tools, prompts, and resources from a controller."""
        inst = controller() if isinstance(controller, type) else controller
        if hasattr(inst, "get_tools") and callable(inst.get_tools):
            for t in inst.get_tools():
                self.register_tool(t)
        if hasattr(inst, "get_resources") and callable(inst.get_resources):
            for r in inst.get_resources():
                self.register_resource(r)
        if hasattr(inst, "get_prompts") and callable(inst.get_prompts):
            for p in inst.get_prompts():
                self.register_prompt(p)
        if hasattr(inst, "to_agent_skill") and callable(inst.to_agent_skill):
            ctrl_id = getattr(inst, "skill_id", None) or getattr(inst, "prefix", None) or inst.__class__.__name__
            self._skill_controllers[ctrl_id] = inst


__all__ = [
    "MCPRegistry",
]
