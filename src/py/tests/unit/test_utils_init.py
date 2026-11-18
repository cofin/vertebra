import subprocess
import sys
import types
from typing import Any

import pytest

import vertebra
import vertebra.framework
import vertebra.typing
import vertebra.utils.uuids
from vertebra import utils
from vertebra.typing import MissingDependencyError, reset_dependency_cache
from vertebra.utils.module_loader import ensure_numpy, ensure_pandas


def test_utils_package_is_thin() -> None:
    """Verify vertebra.utils is a thin package with no re-exports."""
    assert utils.__doc__ is not None
    assert "explicitly" in utils.__doc__
    public_names = [
        n for n in dir(utils) if not n.startswith("_") and not isinstance(getattr(utils, n), types.ModuleType)
    ]
    assert public_names == [], f"utils.__init__ should not re-export: {public_names}"


def test_framework_lazy_import_does_not_load_submodules() -> None:
    """Verify importing vertebra.framework does not eagerly import submodules or heavy extensions."""
    code = (
        "import sys, time\n"
        "t0 = time.perf_counter()\n"
        "import vertebra.framework\n"
        "duration = time.perf_counter() - t0\n"
        "forbidden = ['vertebra.framework.inertia', 'vertebra.framework.ssr', "
        "'vertebra.framework.live', 'vertebra.extensions.litestar']\n"
        "loaded = [m for m in forbidden if m in sys.modules]\n"
        "assert not loaded, f'Eagerly loaded: {loaded}'\n"
        "assert 'mcp' not in vertebra.framework.__all__, 'mcp in __all__'\n"
        "assert 'a2a' not in vertebra.framework.__all__, 'a2a in __all__'\n"
        "assert duration < 0.20, f'Import took too long: {duration}s'\n"
    )
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert res.returncode == 0, f"Subprocess failed:\n{res.stderr}\n{res.stdout}"


@pytest.mark.parametrize(
    "mod",
    [vertebra, vertebra.typing, vertebra.framework],
    ids=["vertebra", "vertebra.typing", "vertebra.framework"],
)
def test_lazy_exports_dir_contains_all_symbols(mod: types.ModuleType) -> None:
    """Verify __dir__ contains every symbol listed in __all__."""
    mod_dir = dir(mod)
    for sym in getattr(mod, "__all__", ()):
        assert sym in mod_dir, f"Symbol {sym!r} missing from dir({mod.__name__})"


def test_lazy_exports_cache_and_dir() -> None:
    """Verify attribute access caches lazy exports into module __dict__."""
    app_cls = vertebra.framework.App
    assert "App" in vertebra.framework.__dict__
    assert vertebra.framework.__dict__["App"] is app_cls

    struct_cls = vertebra.typing.Struct
    assert "Struct" in vertebra.typing.__dict__
    assert vertebra.typing.__dict__["Struct"] is struct_cls


def test_framework_channels_and_stores_do_not_import_litestar_extension() -> None:
    """Verify framework.channels and framework.stores do not depend on vertebra.extensions.litestar."""
    code = (
        "import sys\n"
        "import vertebra.framework.channels\n"
        "import vertebra.framework.stores\n"
        "assert 'vertebra.extensions.litestar' not in sys.modules, "
        "'litestar extension imported by framework channels/stores'\n"
    )
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert res.returncode == 0, f"Subprocess failed:\n{res.stderr}\n{res.stdout}"


@pytest.mark.parametrize(
    ("module_name", "extra_name", "loader_fn"),
    [
        ("numpy", "numpy", ensure_numpy),
        ("pandas", "pandas", ensure_pandas),
    ],
)
def test_optional_array_loaders_surface_extra_hints(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    extra_name: str,
    loader_fn: Any,
) -> None:
    """ensure_numpy and ensure_pandas include matching [extra] hints in MissingDependencyError."""
    monkeypatch.setitem(sys.modules, module_name, None)
    reset_dependency_cache(module_name)
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            loader_fn()
        expected_dist = f"vertebra-core[{extra_name}]"
        assert exc_info.value.install_package == expected_dist
        assert expected_dist in exc_info.value.hint
    finally:
        reset_dependency_cache(module_name)


@pytest.mark.parametrize(
    ("attr_name", "module_name", "extra_name"),
    [
        ("BaseModel", "pydantic", "pydantic"),
        ("ndarray", "numpy", "numpy"),
        ("Table", "pyarrow", "arrow"),
    ],
)
def test_typing_lazy_attributes_raise_with_extra_hints(
    monkeypatch: pytest.MonkeyPatch,
    attr_name: str,
    module_name: str,
    extra_name: str,
) -> None:
    """Lazy attributes on vertebra.typing raise MissingDependencyError naming their extra."""
    monkeypatch.delitem(vertebra.typing.__dict__, attr_name, raising=False)
    monkeypatch.setitem(sys.modules, module_name, None)
    reset_dependency_cache(module_name)
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            getattr(vertebra.typing, attr_name)
        expected_dist = f"vertebra-core[{extra_name}]"
        assert exc_info.value.install_package == expected_dist
        assert expected_dist in exc_info.value.hint
    finally:
        reset_dependency_cache(module_name)


@pytest.mark.parametrize(
    "mod",
    [vertebra.typing, vertebra.utils.uuids],
    ids=["vertebra.typing", "vertebra.utils.uuids"],
)
def test_dead_nanoid_flag_removed_while_active_flags_retained(mod: types.ModuleType) -> None:
    """Dead NANOID_INSTALLED flag is absent while ATTRS_INSTALLED and UUID_UTILS_INSTALLED remain."""
    assert not hasattr(mod, "NANOID_INSTALLED")
    assert "NANOID_INSTALLED" not in getattr(mod, "__all__", ())
    assert hasattr(vertebra.typing, "ATTRS_INSTALLED")
    assert hasattr(vertebra.utils.uuids, "UUID_UTILS_INSTALLED")
