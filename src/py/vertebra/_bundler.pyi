"""Type stubs for the vertebra._bundler native extension module."""

from types import TracebackType
from typing import Self, final

FREE_THREADED: bool
GIL_USED: bool

__all__ = [
    "BundleConfig",
    "BundleDevConfig",
    "BundleOutput",
    "BundleResult",
    "Bundler",
    "FREE_THREADED",
    "GIL_USED",
    "IsolatedDeclarationsGenerator",
    "SSRPool",
    "SSRResult",
    "SourceWatcher",
    "minify_css",
]

@final
class BundleOutput:
    @property
    def path(self) -> str: ...
    @property
    def size(self) -> int: ...
    @property
    def is_entry(self) -> bool: ...
    def __new__(cls, path: str, size: int, is_entry: bool) -> BundleOutput: ...
    def __repr__(self) -> str: ...

@final
class BundleResult:
    @property
    def outputs(self) -> list[BundleOutput]: ...
    @property
    def duration_ms(self) -> int: ...
    @property
    def warnings(self) -> list[str]: ...
    def __new__(
        cls,
        outputs: list[BundleOutput],
        duration_ms: int,
        warnings: list[str],
    ) -> BundleResult: ...
    def __repr__(self) -> str: ...

@final
class BundleConfig:
    entry: str
    outdir: str
    minify: bool
    sourcemap: bool
    target: str
    def __new__(
        cls,
        entry: str,
        outdir: str = ...,
        minify: bool = True,
        sourcemap: bool = False,
        target: str = ...,
    ) -> BundleConfig: ...
    def entry_path(self) -> str: ...
    def output_path(self) -> str: ...

@final
class BundleDevConfig:
    entry: str
    watch_dirs: list[str]
    hmr_port: int
    extensions: list[str]
    debounce_ms: int
    def __new__(
        cls,
        entry: str,
        watch_dirs: list[str] = ...,
        hmr_port: int = 5173,
        extensions: list[str] = ...,
        debounce_ms: int = 100,
    ) -> BundleDevConfig: ...

@final
class SourceWatcher:
    def __new__(
        cls,
        watch_dirs: list[str],
        extensions: list[str] | None = None,
        debounce_ms: int = 100,
    ) -> SourceWatcher: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def poll_events(self, timeout_ms: int | None = None) -> list[str]: ...
    def notify_type_change(self, version: str) -> None: ...
    def notify_reload(self) -> None: ...

@final
class SSRResult:
    @property
    def head(self) -> list[str]: ...
    @property
    def body(self) -> str: ...
    def __new__(cls, head: list[str], body: str) -> SSRResult: ...

@final
class SSRPool:
    @property
    def generation(self) -> int: ...
    @property
    def cache_len(self) -> int: ...
    def __new__(cls, bundle_path: str, pool_size: int = 4, timeout_ms: int = 1000) -> SSRPool: ...
    def render(self, component: str, page_json: str) -> SSRResult: ...
    def invalidate(self) -> None: ...
    def shutdown(self) -> None: ...

@final
class IsolatedDeclarationsGenerator:
    def __new__(cls, strip_internal: bool = True) -> IsolatedDeclarationsGenerator: ...
    def emit_dts(self, filename: str, source_text: str) -> str: ...

@final
class Bundler:
    @property
    def config(self) -> BundleConfig | None: ...
    @property
    def dev_config(self) -> BundleDevConfig | None: ...
    def __new__(
        cls,
        config: BundleConfig | None = None,
        *,
        dev_config: BundleDevConfig | None = None,
    ) -> Bundler: ...
    @classmethod
    def from_config(
        cls,
        config: BundleConfig,
        *,
        dev_config: BundleDevConfig | None = None,
    ) -> Bundler: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        _exc_type: type[BaseException] | None = None,
        _exc_value: BaseException | None = None,
        _traceback: TracebackType | None = None,
    ) -> bool: ...
    def build(self, config: BundleConfig | None = None) -> BundleResult: ...
    def start_dev(self, config: BundleDevConfig | None = None) -> None: ...
    def stop_dev(self) -> None: ...
    def poll_events(self, timeout_ms: int | None = None) -> list[str]: ...
    def transform_file(self, path: str, source: str) -> str: ...
    def check_file(self, path: str, source: str) -> list[str]: ...
    def notify_type_change(self, version: str | None = None) -> None: ...
    def notify_reload(self) -> None: ...
    def hmr_url(self) -> str | None: ...
    def is_dev_mode(self) -> bool: ...

def minify_css(css_source: str) -> str: ...
