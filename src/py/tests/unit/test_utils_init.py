import types

from vertebra import utils


def test_utils_package_is_thin():
    """Verify vertebra.utils is a thin package with no re-exports."""
    assert "explicitly" in utils.__doc__
    public_names = [
        n for n in dir(utils) if not n.startswith("_") and not isinstance(getattr(utils, n), types.ModuleType)
    ]
    assert public_names == [], f"utils.__init__ should not re-export: {public_names}"
