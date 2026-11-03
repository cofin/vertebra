"""Unit tests for vertebra.utils.uuids, config_tools, and module_loader."""

from __future__ import annotations

import types
import uuid
from pathlib import Path
from typing import Any

import pytest

from vertebra.typing import MissingDependencyError
from vertebra.utils import uuids as uuids_mod
from vertebra.utils.config_tools import (
    SingletonMeta,
    find_pyproject_toml,
    get_config_val,
    get_config_val_with_aliases,
    get_env,
    get_env_with_aliases,
    is_env_set,
    normalize_connection_config,
    parse_mysql_dsn,
    parse_odbc_connection_string,
    parse_pyproject_config,
    read_pyproject_section,
    resolve_config_async,
    resolve_config_sync,
)
from vertebra.utils.module_loader import (
    ensure_module,
    ensure_numpy,
    ensure_pandas,
    ensure_pyarrow,
    import_optional_attr,
    module_to_os_path,
    resolve_optional_attr,
)
from vertebra.utils.uuids import (
    nanoid,
    uuid3,
    uuid4,
    uuid5,
    uuid6,
    uuid7,
    uuid_from_bytes,
    uuid_from_int,
    uuid_from_string,
)


class _SampleSingleton(metaclass=SingletonMeta):
    def __init__(self, label: str = "default") -> None:
        self.label = label


@pytest.fixture(params=["native", "python"])
def uuid_backend(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """Parameterize UUID and NanoID tests across compiled Rust and pure-Python fallbacks."""
    mode = str(request.param)
    if mode == "python":
        monkeypatch.setattr(uuids_mod, "_native_uuid4_u128", None)
        monkeypatch.setattr(uuids_mod, "_native_uuid7_u128", None)
        monkeypatch.setattr(uuids_mod, "_native_nanoid_generate", None)
    return mode


@pytest.fixture(
    params=[
        (
            '[tool.vertebra]\nconfig = "app.cfg"\n[tool.vertebra.workers]\nqueues = ["a", "b"]\n',
            "workers",
            {"queues": ["a", "b"]},
            "app.cfg",
        ),
        (
            '[tool.vtb]\nconfig = ["one.toml", "two.toml"]\n[tool.vtb.workers]\nqueues = ["legacy"]\n',
            "workers",
            {"queues": ["legacy"]},
            "one.toml,two.toml",
        ),
    ],
)
def pyproject_workspace(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> tuple[Path, Path, str, dict[str, Any], str]:
    """Create a temporary workspace containing pyproject.toml and a nested package directory."""
    toml_content, section, expected_section, expected_config_str = request.param
    pyproject_file = tmp_path / "pyproject.toml"
    pyproject_file.write_text(toml_content, encoding="utf-8")
    nested_dir = tmp_path / "src" / "pkg"
    nested_dir.mkdir(parents=True)
    return pyproject_file, nested_dir, section, expected_section, expected_config_str


@pytest.fixture
def port_pyproject_dir(tmp_path: Path) -> Path:
    """Create a temporary directory containing a [tool.vertebra] port configuration."""
    pyproject_file = tmp_path / "pyproject.toml"
    pyproject_file.write_text("[tool.vertebra]\nport = 9000\n", encoding="utf-8")
    return tmp_path


@pytest.fixture(
    params=[
        (ensure_pyarrow, "Table"),
        (ensure_numpy, "ndarray"),
        (ensure_pandas, "DataFrame"),
    ],
)
def optional_data_module_case(
    request: pytest.FixtureRequest,
) -> tuple[Any, str]:
    """Parameterize optional data science module loaders (pyarrow, numpy, pandas)."""
    return request.param  # type: ignore[no-any-return]


@pytest.mark.parametrize(
    ("fn", "args", "expected_version", "is_deterministic"),
    [
        (uuid3, ("vertebra.dev",), 3, True),
        (uuid5, ("vertebra.dev",), 5, True),
        (uuid4, (), 4, False),
        (uuid6, (), 6, False),
        (uuid7, (), 7, False),
    ],
)
def test_uuid_versions_and_determinism(
    uuid_backend: str,
    fn: Any,
    args: tuple[Any, ...],
    expected_version: int,
    is_deterministic: bool,
) -> None:
    """Verify uuid3, uuid4, uuid5, uuid6, and uuid7 version bits and determinism."""
    assert uuid_backend in {"native", "python"}
    first = fn(*args)
    second = fn(*args)
    assert isinstance(first, uuid.UUID)
    assert first.version == expected_version
    if is_deterministic:
        assert first == second
    else:
        assert first != second


def test_uuid7_monotonicity_and_conversion_helpers(uuid_backend: str) -> None:
    """Verify uuid7 monotonicity, uuid_from_* helpers, and nanoid generation."""
    assert uuid_backend in {"native", "python"}
    u7_list = [uuid7() for _ in range(200)]
    assert u7_list == sorted(u7_list)

    sample = uuid4()
    assert uuid_from_string(str(sample)) == sample
    assert uuid_from_bytes(sample.bytes) == sample
    assert uuid_from_int(sample.int) == sample

    nid = nanoid()
    assert len(nid) == 21
    assert all(ch.isalnum() or ch in {"_", "-"} for ch in nid)
    assert len(nanoid(12)) == 12
    with pytest.raises(ValueError):
        nanoid(0)


@pytest.fixture
def patched_uuid_utils(monkeypatch: pytest.MonkeyPatch) -> uuid.UUID:
    """Monkeypatch _load_uuid_utils with a deterministic uuid4 stub."""
    fixed_u4 = uuid.UUID("12345678-1234-4678-8234-567812345678")
    fake_mod = types.SimpleNamespace(
        NAMESPACE_DNS=uuid.NAMESPACE_DNS,
        UUID=uuid.UUID,
        uuid3=lambda ns, name: uuid.uuid3(ns, name),
        uuid4=lambda: fixed_u4,
        uuid5=lambda ns, name: uuid.uuid5(ns, name),
    )
    monkeypatch.setattr(uuids_mod, "_load_uuid_utils", lambda: fake_mod)
    return fixed_u4


def test_uuid_monkeypatched_backend_fallback(patched_uuid_utils: uuid.UUID) -> None:
    """Verify _load_uuid_utils monkeypatching works for test overrides."""
    assert uuid4() == patched_uuid_utils


def test_singleton_meta_reuses_instance() -> None:
    """Verify SingletonMeta returns the same instance across repeated calls."""
    first = _SampleSingleton("first")
    second = _SampleSingleton("second")
    assert first is second
    assert first.label == "first"


def test_pyproject_discovery_and_section_reading(
    pyproject_workspace: tuple[Path, Path, str, dict[str, Any], str],
) -> None:
    """Verify find_pyproject_toml, read_pyproject_section, and parse_pyproject_config."""
    pyproject_file, nested_dir, section, expected_section, expected_config_str = pyproject_workspace
    assert find_pyproject_toml(nested_dir) == pyproject_file
    assert read_pyproject_section(section, start_path=nested_dir) == expected_section
    assert read_pyproject_section("tool", "vertebra", section, start_path=nested_dir) == expected_section
    assert parse_pyproject_config(pyproject_file) == expected_config_str


@pytest.mark.parametrize(
    ("raw_env", "type_hint", "expected"),
    [
        ("true", bool, True),
        ("0", bool, False),
        ("42", int, 42),
        ("2.5", float, 2.5),
        ("/var/run/vtb", Path, Path("/var/run/vtb")),
        ("a, b, c", list, ["a", "b", "c"]),
        ("[1, 2, 3]", list[int], [1, 2, 3]),
        ("10,20,30", list[int], [10, 20, 30]),
        ("k1=v1,k2=v2", dict, {"k1": "v1", "k2": "v2"}),
        ('{"host": "localhost", "port": 5432}', dict[str, Any], {"host": "localhost", "port": 5432}),
    ],
)
def test_get_env_and_aliases_type_coercions(
    monkeypatch: pytest.MonkeyPatch,
    raw_env: str,
    type_hint: Any,
    expected: Any,
) -> None:
    """Verify get_env, get_env_with_aliases, and is_env_set across scalar and collection types."""
    monkeypatch.delenv("VTB_PRIMARY", raising=False)
    monkeypatch.setenv("VTB_ALIAS", raw_env)

    assert is_env_set("VTB_PRIMARY") is False
    assert get_env("VTB_ALIAS", None, type_hint) == expected
    assert get_env_with_aliases("VTB_PRIMARY", ("VTB_ALIAS",), default=None, type_hint=type_hint) == expected


@pytest.mark.parametrize(
    ("raw_val", "expected"),
    [
        ("1", True),
        ("true", True),
        ("yes", True),
        ("0", False),
        ("false", False),
        ("", False),
    ],
)
def test_is_env_set_values(
    monkeypatch: pytest.MonkeyPatch,
    raw_val: str,
    expected: bool,
) -> None:
    """Verify is_env_set across truthy, falsy, and empty values."""
    monkeypatch.setenv("VTB_FLAG", raw_val)
    assert is_env_set("VTB_FLAG") is expected


def test_get_config_val_precedence(
    port_pyproject_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify get_config_val and get_config_val_with_aliases precedence order."""
    monkeypatch.delenv("VERTEBRA_PORT", raising=False)
    assert (
        get_config_val(
            "port",
            default=8000,
            env_key="VERTEBRA_PORT",
            type_hint=int,
            start_path=port_pyproject_dir,
        )
        == 9000
    )

    monkeypatch.setenv("VERTEBRA_PORT", "9100")
    assert (
        get_config_val_with_aliases(
            "port",
            env_keys=("VERTEBRA_PORT",),
            default=8000,
            type_hint=int,
            start_path=port_pyproject_dir,
        )
        == 9100
    )
    assert (
        get_config_val(
            "port",
            value=9200,
            default=8000,
            env_key="VERTEBRA_PORT",
            type_hint=int,
            start_path=port_pyproject_dir,
        )
        == 9200
    )


@pytest.mark.parametrize(
    ("dsn", "expected_host", "expected_port", "expected_db"),
    [
        ("mysql://root:secret@db.internal:3307/analytics?charset=utf8mb4", "db.internal", 3307, "analytics"),
        ("mysql+pymysql://app@localhost/testdb", "localhost", 3306, "testdb"),
    ],
)
def test_parse_mysql_dsn_and_normalize_connection_config(
    dsn: str,
    expected_host: str,
    expected_port: int,
    expected_db: str,
) -> None:
    """Verify parse_mysql_dsn and normalize_connection_config."""
    parsed = parse_mysql_dsn(dsn)
    assert parsed["host"] == expected_host
    assert parsed["port"] == expected_port
    assert parsed["database"] == expected_db

    norm = normalize_connection_config(dsn, extra_key="val")
    assert norm["dsn"] == dsn
    assert norm["extra_key"] == "val"


def test_parse_odbc_and_resolve_config_sync_and_async() -> None:
    """Verify parse_odbc_connection_string and resolve_config_sync."""
    odbc = parse_odbc_connection_string("Driver={ODBC Driver 18};Server=localhost,1433;Database=app;")
    assert odbc["Driver"] == "{ODBC Driver 18}"
    assert odbc["Server"] == "localhost,1433"
    assert odbc["Database"] == "app"

    assert resolve_config_sync({"mode": "sync"}) == {"mode": "sync"}
    assert resolve_config_sync(lambda: {"mode": "callable"}) == {"mode": "callable"}


@pytest.mark.anyio
async def test_resolve_config_async_awaitable_and_callable() -> None:
    """Verify resolve_config_async resolves dicts, sync callables, and async callables."""

    async def _load() -> dict[str, str]:
        return {"mode": "async"}

    assert await resolve_config_async(_load) == {"mode": "async"}
    assert await resolve_config_async({"mode": "direct"}) == {"mode": "direct"}


def test_module_loader_optional_attrs_and_os_path() -> None:
    """Verify import_optional_attr, resolve_optional_attr, and module_to_os_path."""
    path_cls = import_optional_attr("pathlib", "Path")
    assert path_cls is Path
    assert import_optional_attr("nonexistent_vertebra_pkg_xyz", "Thing") is None

    resolved_fn, available = resolve_optional_attr("pathlib", "Path", fallback="fallback_val")
    assert available is True
    assert resolved_fn is Path

    fallback_val, missing_avail = resolve_optional_attr(
        "nonexistent_vertebra_pkg_xyz",
        "Thing",
        fallback="fallback_val",
    )
    assert missing_avail is False
    assert fallback_val == "fallback_val"

    mod_path = module_to_os_path("vertebra.utils")
    assert mod_path.is_dir()
    assert (mod_path / "module_loader.py").is_file()


def test_ensure_optional_data_modules(
    optional_data_module_case: tuple[Any, str],
) -> None:
    """Verify ensure_pyarrow, ensure_numpy, and ensure_pandas return modules or raise MissingDependencyError."""
    ensure_fn, attr_name = optional_data_module_case
    try:
        mod = ensure_fn()
        assert hasattr(mod, attr_name)
    except MissingDependencyError:
        pass


@pytest.mark.parametrize(
    "submodule_name",
    [
        "config_tools",
        "correlation",
        "deprecation",
        "dispatch",
        "humanize",
        "module_loader",
        "portal",
        "schema",
        "serializers",
        "sync_tools",
        "text",
        "type_converters",
        "type_guards",
        "uuids",
    ],
)
def test_vertebra_utils_package_submodule_imports(submodule_name: str) -> None:
    """Verify vertebra.utils submodules import cleanly while keeping vertebra.utils thin."""
    submod = ensure_module(f"vertebra.utils.{submodule_name}")
    assert isinstance(submod, types.ModuleType)
