from __future__ import annotations

import pytest

from vertebra.drivers import (
    ERROR_CODE_MAP,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    format_error_message,
    map_error_code,
)
from vertebra.drivers import Warning as DriverWarning

pytestmark = pytest.mark.unit


def test_exception_hierarchy() -> None:
    assert issubclass(DriverWarning, Exception)
    assert issubclass(Error, Exception)
    assert issubclass(InterfaceError, Error)
    assert issubclass(DatabaseError, Error)
    assert issubclass(DataError, DatabaseError)
    assert issubclass(OperationalError, DatabaseError)
    assert issubclass(IntegrityError, DatabaseError)
    assert issubclass(InternalError, DatabaseError)
    assert issubclass(ProgrammingError, DatabaseError)
    assert issubclass(NotSupportedError, DatabaseError)


def test_error_code_mapping() -> None:
    assert map_error_code("data") is DataError
    assert map_error_code("unsupported") is NotSupportedError
    assert map_error_code("unknown") is DatabaseError
    assert "data" in ERROR_CODE_MAP


def test_error_message_formatting() -> None:
    assert format_error_message("data", "bad payload") == "[data] bad payload"
    assert format_error_message(None, "oops") == "oops"
