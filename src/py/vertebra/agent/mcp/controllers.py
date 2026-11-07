"""Class-based controllers for MCP prompts, resources, and skills.

This module provides PromptController and SkillController alongside @prompt and @resource
decorators for grouping tools, prompts, and resources with shared guards and instructions.
"""

import functools
import inspect
from collections.abc import Callable, Sequence
from typing import Any

from vertebra.agent.core.guards import GuardChain, GuardProtocol
from vertebra.agent.core.schema import Tool
from vertebra.agent.mcp.models import (
    PromptArgument,
    PromptEntry,
    PromptMessage,
    ResourceEntry,
    ResourceTemplateEntry,
    TextContent,
)

__all__ = [
    "PromptController",
    "SkillController",
    "mcp_prompt",
    "mcp_resource",
    "mcp_tool",
    "prompt",
    "resource",
]


def mcp_tool(
    name: str | None = None,
    description: str | None = None,
    *,
    title: str | None = None,
    requires_confirmation: bool = False,
    timeout_seconds: float | None = None,
    annotations: dict[str, Any] | None = None,
    webmcp: bool = False,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator marking a route handler or function as an explicit MCP tool without wrapping its type.

    Args:
        name: Optional override for the MCP tool name.
        description: Optional override for the MCP tool description.
        title: Optional human-readable title for the tool.
        requires_confirmation: Require a human confirmation ticket before invoking the tool.
        timeout_seconds: Optional execution timeout in seconds.
        annotations: Optional metadata dictionary attached to the tool descriptor.
        webmcp: List the tool in the browser WebMCP manifest. Defaults to `False`; browser
            exposure is opt-in per tool.
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        target_fn: Any = fn
        target_fn._mcp_tool_meta = {
            "name": name or fn.__name__,
            "title": title or "",
            "description": description or inspect.getdoc(fn) or "",
            "requires_confirmation": requires_confirmation,
            "timeout_seconds": timeout_seconds,
            "annotations": dict(annotations or {}),
            "webmcp": webmcp,
        }
        return fn

    return decorator


def mcp_resource(
    uri: str,
    name: str | None = None,
    description: str | None = None,
    mime_type: str = "text/plain",
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator marking a route handler or method as an MCP resource or resource template."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        res_name = name or fn.__name__
        res_desc = description or inspect.getdoc(fn) or ""
        target_fn: Any = fn
        if "{" in uri and "}" in uri:
            target_fn._mcp_resource_template = {
                "uriTemplate": uri,
                "name": res_name,
                "description": res_desc,
                "mimeType": mime_type,
            }
        else:
            target_fn._mcp_resource = {
                "uri": uri,
                "name": res_name,
                "description": res_desc,
                "mimeType": mime_type,
            }
        return fn

    return decorator


def mcp_prompt(
    name: str | None = None,
    description: str | None = None,
    arguments: Sequence[PromptArgument] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator marking a route handler or method as an MCP prompt template."""
    return prompt(name=name, description=description, arguments=arguments)


def prompt(
    name: str | None = None,
    description: str | None = None,
    arguments: Sequence[PromptArgument] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator marking a method as an MCP prompt template."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        prompt_name = name or fn.__name__
        prompt_desc = description or inspect.getdoc(fn) or ""
        target_fn: Any = fn
        target_fn._mcp_prompt = {
            "name": prompt_name,
            "description": prompt_desc,
            "arguments": tuple(arguments or ()),
        }
        return fn

    return decorator


def resource(
    uri: str,
    name: str | None = None,
    description: str | None = None,
    mime_type: str = "text/plain",
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator marking a method as an MCP resource provider."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        res_name = name or fn.__name__
        res_desc = description or inspect.getdoc(fn) or ""
        target_fn: Any = fn
        target_fn._mcp_resource = {
            "uri": uri,
            "name": res_name,
            "description": res_desc,
            "mimeType": mime_type,
        }
        return fn

    return decorator


class PromptController:
    """Class grouping prompt templates with shared prefix, instructions, and guards."""

    prefix: str | None = None
    instructions: str | None = None
    guards: Sequence[GuardProtocol] = ()

    def get_prompts(self) -> list[PromptEntry]:
        """Discover and instantiate PromptEntry descriptors for all @prompt methods."""
        entries: list[PromptEntry] = []
        for attr_name in dir(self):
            val = getattr(self, attr_name)
            if callable(val) and hasattr(val, "_mcp_prompt"):
                meta = val._mcp_prompt
                base_name = meta["name"]
                full_name = f"{self.prefix}_{base_name}" if self.prefix else base_name
                desc = meta["description"]
                args = meta["arguments"]

                @functools.wraps(val)
                async def wrapped_getter(
                    *call_args: Any, _fn: Callable[..., Any] = val, **call_kwargs: Any
                ) -> list[PromptMessage]:
                    res = _fn(*call_args, **call_kwargs)
                    if inspect.isawaitable(res):
                        res = await res
                    messages: list[PromptMessage] = list(res)
                    if self.instructions:
                        if messages and messages[0].role == "user":
                            first = messages[0]
                            existing_text = first.content.text if isinstance(first.content, TextContent) else ""
                            combined = f"{self.instructions}\n\n{existing_text}" if existing_text else self.instructions
                            messages[0] = PromptMessage(
                                role=first.role,
                                content=TextContent(text=combined),
                            )
                        else:
                            messages.insert(
                                0,
                                PromptMessage(
                                    role="user",
                                    content=TextContent(text=self.instructions),
                                ),
                            )
                    return messages

                entries.append(
                    PromptEntry(
                        name=full_name,
                        description=desc,
                        arguments=args,
                        getter=wrapped_getter,
                    )
                )
        return entries


class SkillController(PromptController):
    """Controller grouping tools, resources, and prompts into a cohesive agent skill."""

    skill_id: str | None = None
    skill_name: str | None = None
    description: str | None = None
    tags: tuple[str, ...] = ()
    examples: tuple[str, ...] = ()

    def get_guard_chain(self) -> GuardChain:
        """Construct the resolved guard chain for this controller."""
        return GuardChain(self.guards)

    def get_tools(self) -> list[Tool]:
        """Discover and bind all Tool descriptors defined on this controller."""
        tools: list[Tool] = []
        cls = type(self)
        for attr_name in dir(cls):
            val = getattr(cls, attr_name, None)
            if isinstance(val, Tool):
                bound_tool = getattr(self, attr_name)
                if isinstance(bound_tool, Tool):
                    if self.guards:
                        bound_tool = Tool(
                            bound_tool.fn,
                            annotations=bound_tool.annotations,
                            guards=(*self.guards, *bound_tool.guards),
                            spec=bound_tool.spec,
                            instance=bound_tool._instance,
                        )
                    tools.append(bound_tool)
            elif callable(val) and (hasattr(val, "_tool_meta") or hasattr(val, "_mcp_tool_meta")):
                bound_tool = getattr(self, attr_name)
                if isinstance(bound_tool, Tool):
                    if self.guards:
                        bound_tool = Tool(
                            bound_tool.fn,
                            annotations=bound_tool.annotations,
                            guards=(*self.guards, *bound_tool.guards),
                            spec=bound_tool.spec,
                            instance=bound_tool._instance,
                        )
                    tools.append(bound_tool)
                elif callable(bound_tool):
                    meta = getattr(val, "_mcp_tool_meta", {})
                    tools.append(
                        Tool(
                            bound_tool,
                            name=meta.get("name") or attr_name,
                            title=meta.get("title") or "",
                            description=meta.get("description") or inspect.getdoc(bound_tool) or "",
                            requires_confirmation=bool(meta.get("requires_confirmation", False)),
                            timeout_seconds=meta.get("timeout_seconds"),
                            webmcp=bool(meta.get("webmcp", False)),
                            annotations=dict(meta.get("annotations", {})),
                            guards=tuple(self.guards),
                        )
                    )
        return tools

    def get_resource_templates(self) -> list[ResourceTemplateEntry]:
        """Discover all @mcp_resource(uri='.../{param}') template methods on this controller."""
        entries: list[ResourceTemplateEntry] = []
        for attr_name in dir(self):
            val = getattr(self, attr_name)
            if callable(val) and hasattr(val, "_mcp_resource_template"):
                meta = val._mcp_resource_template
                entries.append(
                    ResourceTemplateEntry(
                        uriTemplate=meta["uriTemplate"],
                        name=meta["name"],
                        description=meta["description"],
                        mimeType=meta["mimeType"],
                        reader=val,
                    )
                )
        return entries

    def get_resources(self) -> list[ResourceEntry]:
        """Discover all @resource decorated methods on this controller."""
        entries: list[ResourceEntry] = []
        for attr_name in dir(self):
            val = getattr(self, attr_name)
            if callable(val) and hasattr(val, "_mcp_resource"):
                target_val: Any = val
                meta = target_val._mcp_resource
                entries.append(
                    ResourceEntry(
                        uri=meta["uri"],
                        name=meta["name"],
                        description=meta["description"],
                        mimeType=meta["mimeType"],
                        reader=val,
                    )
                )
        return entries

    def to_agent_skill(self) -> dict[str, Any]:
        """Project this controller into an A2A-compatible AgentSkill dictionary."""
        effective_id = self.skill_id or self.prefix or self.__class__.__name__.lower().removesuffix("controller")
        effective_name = self.skill_name or self.__class__.__name__
        effective_desc = self.description or inspect.getdoc(self.__class__) or ""
        return {
            "id": effective_id,
            "name": effective_name,
            "description": effective_desc,
            "tags": list(self.tags),
            "examples": list(self.examples),
        }
