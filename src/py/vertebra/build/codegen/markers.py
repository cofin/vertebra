"""Litestar v2/v3 compatible parameter, body, and dependency markers for route codegen."""

from typing import Annotated, Any, Literal, TypeVar

import msgspec

__all__ = (
    "Body",
    "BodyMarker",
    "Dependency",
    "FromBody",
    "FromCookie",
    "FromHeader",
    "FromPath",
    "FromQuery",
    "JSONBody",
    "LocationMarker",
    "MsgPackBody",
    "MultipartBody",
    "NamedDependency",
    "NamedDependencyMarker",
    "Parameter",
    "ParameterLocation",
    "ParameterMarker",
    "Provide",
    "SkipValidation",
    "SkipValidationMarker",
    "URLEncodedBody",
)

T = TypeVar("T")

type ParameterLocation = Literal["path", "query", "header", "cookie", "body"]


class LocationMarker(msgspec.Struct, frozen=True, gc=False):
    """Litestar v3 parameter location marker attached via typing.Annotated."""

    location: ParameterLocation
    media_type: str | None = None


class NamedDependencyMarker(msgspec.Struct, frozen=True, gc=False):
    """Litestar v3 marker indicating a dependency-injected parameter excluded from schemas."""

    key: str | None = None


class SkipValidationMarker(msgspec.Struct, frozen=True, gc=False):
    """Litestar v3 marker indicating runtime validation is skipped while retaining schema type."""


class ParameterMarker(msgspec.Struct, frozen=True, gc=False):
    """Litestar v2 compatible Parameter() specification for query, header, path, and cookie constraints."""

    query: str | None = None
    header: str | None = None
    cookie: str | None = None
    title: str | None = None
    description: str | None = None
    ge: int | float | None = None
    gt: int | float | None = None
    le: int | float | None = None
    lt: int | float | None = None
    multiple_of: int | float | None = None
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None
    default: Any = None
    required: bool = True
    include_in_schema: bool = True


class BodyMarker(msgspec.Struct, frozen=True, gc=False):
    """Litestar v2 compatible Body() specification for request payload encoding and constraints."""

    media_type: str = "application/json"
    title: str | None = None
    description: str | None = None
    include_in_schema: bool = True


def Parameter(
    *,
    query: str | None = None,
    header: str | None = None,
    cookie: str | None = None,
    title: str | None = None,
    description: str | None = None,
    ge: int | float | None = None,
    gt: int | float | None = None,
    le: int | float | None = None,
    lt: int | float | None = None,
    multiple_of: int | float | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    pattern: str | None = None,
    default: Any = None,
    required: bool = True,
    include_in_schema: bool = True,
) -> ParameterMarker:
    """Construct a Litestar v2 compatible Parameter constraint marker."""
    return ParameterMarker(
        query=query,
        header=header,
        cookie=cookie,
        title=title,
        description=description,
        ge=ge,
        gt=gt,
        le=le,
        lt=lt,
        multiple_of=multiple_of,
        min_length=min_length,
        max_length=max_length,
        pattern=pattern,
        default=default,
        required=required,
        include_in_schema=include_in_schema,
    )


def Body(
    *,
    media_type: str = "application/json",
    title: str | None = None,
    description: str | None = None,
    include_in_schema: bool = True,
) -> BodyMarker:
    """Construct a Litestar v2 compatible Body specification marker."""
    return BodyMarker(
        media_type=media_type,
        title=title,
        description=description,
        include_in_schema=include_in_schema,
    )


FromPath = Annotated[T, LocationMarker("path")]
FromQuery = Annotated[T, LocationMarker("query")]
FromHeader = Annotated[T, LocationMarker("header")]
FromCookie = Annotated[T, LocationMarker("cookie")]
FromBody = Annotated[T, LocationMarker("body")]
JSONBody = Annotated[T, LocationMarker("body", "application/json")]
MsgPackBody = Annotated[T, LocationMarker("body", "application/x-msgpack")]
MultipartBody = Annotated[T, LocationMarker("body", "multipart/form-data")]
URLEncodedBody = Annotated[T, LocationMarker("body", "application/x-www-form-urlencoded")]
Dependency = Annotated[T, NamedDependencyMarker()]
NamedDependency = Annotated[T, NamedDependencyMarker()]
Provide = Annotated[T, NamedDependencyMarker()]
SkipValidation = Annotated[T, SkipValidationMarker()]
