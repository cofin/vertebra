"""Agent-to-Agent (A2A) 1.0 protocol frozen data models.

This module provides 100% spec-conformant A2A 1.0 data models as msgspec.Struct
instances with camelCase wire serialization and zero GC overhead.
"""

import re
import urllib.parse
from typing import Any, Literal, cast

import msgspec

from vertebra.utils.serializers import decode_json, encode_json_bytes

TaskState = Literal[
    "submitted",
    "working",
    "input-required",
    "auth-required",
    "completed",
    "failed",
    "canceled",
    "rejected",
]

TERMINAL_A2A_STATES: frozenset[TaskState] = frozenset(
    {
        "completed",
        "failed",
        "canceled",
        "rejected",
    }
)


class TextPart(msgspec.Struct, tag_field="kind", tag="text", frozen=True, gc=False, kw_only=True, rename="camel"):
    """Plain text content item in an A2A message or artifact."""

    text: str = ""

    @property
    def kind(self) -> Literal["text"]:
        """Discriminator kind tag."""
        return "text"


class FileContent(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """File payload or remote URI pointer."""

    data: str | None = None
    uri: str | None = None
    mime_type: str | None = None


class FilePart(msgspec.Struct, tag_field="kind", tag="file", frozen=True, gc=False, kw_only=True, rename="camel"):
    """Binary or document file item in an A2A message or artifact."""

    file: FileContent

    @property
    def kind(self) -> Literal["file"]:
        """Discriminator kind tag."""
        return "file"


class DataPart(msgspec.Struct, tag_field="kind", tag="data", frozen=True, gc=False, kw_only=True, rename="camel"):
    """Structured data dictionary item in an A2A message or artifact."""

    data: dict[str, Any] = msgspec.field(default_factory=dict)

    @property
    def kind(self) -> Literal["data"]:
        """Discriminator kind tag."""
        return "data"


type Part = TextPart | FilePart | DataPart


class Message(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """Message exchanged between caller and agent."""

    message_id: str
    role: Literal["user", "agent"]
    parts: tuple[Part, ...]
    task_id: str | None = None
    context_id: str | None = None
    metadata: dict[str, Any] | None = None


class Artifact(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """Durable artifact produced during task execution."""

    artifact_id: str
    parts: tuple[Part, ...]
    name: str | None = None
    description: str | None = None
    metadata: dict[str, Any] | None = None


class TaskStatus(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """Current progress and state of an A2A task."""

    state: TaskState
    message: Message | None = None
    timestamp: str | None = None


class Task(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """Immutable representation of an A2A durable task."""

    task_id: str
    status: TaskStatus
    context_id: str | None = None
    artifacts: tuple[Artifact, ...] = ()
    metadata: dict[str, Any] | None = None
    owner_id: str | None = None


class TaskStatusUpdateEvent(
    msgspec.Struct, tag_field="kind", tag="status", frozen=True, gc=False, kw_only=True, rename="camel"
):
    """Streaming event notifying subscribers of a task state transition."""

    task_id: str
    status: TaskStatus
    final: bool = False

    @property
    def kind(self) -> Literal["status"]:
        """Discriminator kind tag."""
        return "status"


class TaskArtifactUpdateEvent(
    msgspec.Struct, tag_field="kind", tag="artifact", frozen=True, gc=False, kw_only=True, rename="camel"
):
    """Streaming event publishing a newly generated or updated task artifact."""

    task_id: str
    artifact: Artifact

    @property
    def kind(self) -> Literal["artifact"]:
        """Discriminator kind tag."""
        return "artifact"


class PushNotificationAuthenticationInfo(
    msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True
):
    """Authentication schemes supported by the push notification webhook endpoint."""

    schemes: tuple[str, ...] = ("bearer",)


class PushNotificationConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """Configuration for webhook push notification delivery."""

    url: str
    token: str | None = None
    authentication: PushNotificationAuthenticationInfo | None = None


class TaskPushNotificationConfig(
    msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True
):
    """Task-specific push notification binding."""

    task_id: str
    config: PushNotificationConfig


class AgentInterface(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """Transport interface declaration in an A2A 1.0 AgentCard."""

    url: str
    protocol_binding: Literal["JSONRPC", "GRPC", "HTTP+JSON"] = "JSONRPC"
    protocol_version: str = "1.0"


class AgentCapabilities(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """Capabilities advertised by an A2A 1.0 agent."""

    streaming: bool = True
    push_notifications: bool = True
    extended_card: bool = True


class AgentProvider(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """Organization or author providing the A2A agent."""

    organization: str = ""
    url: str | None = None


class AgentSkill(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """Discrete skill capability exposed by an agent."""

    id: str
    name: str
    description: str = ""
    tags: tuple[str, ...] = ()
    examples: tuple[str, ...] = ()


class AgentCardSignature(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """RFC 7515 JWS signature envelope verifying AgentCard authenticity."""

    protected: str
    signature: str
    header: dict[str, Any] | None = None


class AgentCard(msgspec.Struct, frozen=True, gc=False, kw_only=True, rename="camel", omit_defaults=True):
    """A2A 1.0 Agent Card discoverable at /.well-known/agent-card.json."""

    name: str
    description: str = ""
    version: str = "1.0"
    supported_interfaces: tuple[AgentInterface, ...] = ()
    capabilities: AgentCapabilities = msgspec.field(default_factory=AgentCapabilities)
    default_input_modes: tuple[str, ...] = ("text", "data")
    default_output_modes: tuple[str, ...] = ("text", "data")
    skills: tuple[AgentSkill, ...] = ()
    provider: AgentProvider | None = None
    signatures: tuple[AgentCardSignature, ...] = ()

    def validate_for_path(self, route_path: str = "/a2a") -> None:
        """Validate that the agent card advertises a valid 1.0 JSONRPC interface matching route_path."""
        normalized_path = "/" + route_path.strip("/")
        matched = False

        for iface in self.supported_interfaces:
            if iface.protocol_binding != "JSONRPC":
                continue
            if not re.match(r"^1\.0(\.[0-9]+)?$", iface.protocol_version):
                continue
            if not (iface.url.startswith("http://") or iface.url.startswith("https://")):
                continue
            parsed = urllib.parse.urlparse(iface.url)
            if parsed.path.rstrip("/") == normalized_path or parsed.path.endswith(normalized_path):
                matched = True
                break

        if not matched:
            raise ValueError(
                f"AgentCard '{self.name}' has no supportedInterfaces with JSONRPC 1.0 matching path '{route_path}'"
            )

    def to_dict(self) -> dict[str, Any]:
        """Serialize AgentCard to a camelCase Python dictionary."""
        return cast(dict[str, Any], decode_json(encode_json_bytes(self)))

    def to_json_bytes(self) -> bytes:
        """Serialize AgentCard to canonical JSON bytes."""
        return encode_json_bytes(self)


__all__ = [
    "TERMINAL_A2A_STATES",
    "AgentCapabilities",
    "AgentCard",
    "AgentCardSignature",
    "AgentInterface",
    "AgentProvider",
    "AgentSkill",
    "Artifact",
    "DataPart",
    "FileContent",
    "FilePart",
    "Message",
    "Part",
    "PushNotificationAuthenticationInfo",
    "PushNotificationConfig",
    "Task",
    "TaskArtifactUpdateEvent",
    "TaskPushNotificationConfig",
    "TaskState",
    "TaskStatus",
    "TaskStatusUpdateEvent",
    "TextPart",
]
