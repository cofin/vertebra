"""DB-API 2.0 compatible exception hierarchy."""

from __future__ import annotations


class Error(Exception):
    """Base error for all driver exceptions."""


class Warning(Exception):  # noqa: A001, N818
    """Non-fatal warning."""


class InterfaceError(Error):
    """Errors related to driver interface misuse."""


class DatabaseError(Error):
    """Errors related to the database."""


class DataError(DatabaseError):
    """Errors due to data processing."""


class OperationalError(DatabaseError):
    """Operational errors such as network failures."""


class IntegrityError(DatabaseError):
    """Relational integrity errors."""


class InternalError(DatabaseError):
    """Internal database errors."""


class ProgrammingError(DatabaseError):
    """Programming errors such as syntax issues."""


class NotSupportedError(DatabaseError):
    """Unsupported feature errors."""


ERROR_CODE_MAP: dict[str, type[Error]] = {
    "interface": InterfaceError,
    "data": DataError,
    "operational": OperationalError,
    "integrity": IntegrityError,
    "internal": InternalError,
    "programming": ProgrammingError,
    "unsupported": NotSupportedError,
}


def map_error_code(code: str) -> type[Error]:
    """Map a backend error code string to a DB-API exception class."""

    return ERROR_CODE_MAP.get(code.lower(), DatabaseError)


def format_error_message(code: str | None, message: str) -> str:
    """Format an error message with a normalized prefix."""

    if code:
        return f"[{code}] {message}"
    return message


__all__ = (
    "ERROR_CODE_MAP",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Warning",
    "format_error_message",
    "map_error_code",
)
