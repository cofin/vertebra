"""Configuration models for the Vertebra Agentic Harness."""

from collections.abc import Sequence

import msgspec


class AgentHarnessConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration settings for the agentic harness runtime and protocols."""

    mcp_enabled: bool = True
    a2a_enabled: bool = True
    mcp_prefix: str = "/mcp"
    a2a_prefix: str = "/a2a"
    skill_paths: Sequence[str] = (".agents/skills",)
    default_model: str = "gemini-flash-next"
    max_turn_steps: int = 16
    max_tool_calls_per_turn: int = 32
    telemetry_enabled: bool = True


__all__ = [
    "AgentHarnessConfig",
]
