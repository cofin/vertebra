import subprocess
import sys
import types

import vertebra
import vertebra.framework
import vertebra.typing
from vertebra import utils


def test_utils_package_is_thin():
    """Verify vertebra.utils is a thin package with no re-exports."""
    assert "explicitly" in utils.__doc__
    public_names = [
        n for n in dir(utils) if not n.startswith("_") and not isinstance(getattr(utils, n), types.ModuleType)
    ]
    assert public_names == [], f"utils.__init__ should not re-export: {public_names}"


def test_framework_lazy_import_does_not_load_submodules():
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


def test_lazy_exports_cache_and_dir():
    """Verify __dir__ contains __all__ symbols and attribute access caches into __dict__."""
    for mod in (vertebra, vertebra.typing, vertebra.framework):
        mod_dir = dir(mod)
        for sym in getattr(mod, "__all__", ()):
            assert sym in mod_dir, f"Symbol {sym!r} missing from dir({mod.__name__})"

    app_cls = vertebra.framework.App
    assert "App" in vertebra.framework.__dict__
    assert vertebra.framework.__dict__["App"] is app_cls

    struct_cls = vertebra.typing.Struct
    assert "Struct" in vertebra.typing.__dict__
    assert vertebra.typing.__dict__["Struct"] is struct_cls


def test_framework_channels_and_stores_do_not_import_litestar_extension():
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
