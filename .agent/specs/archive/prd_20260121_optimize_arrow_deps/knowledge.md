# Knowledge Capture: Adopt SQLSpec Typing Patterns for Optional Dependencies

## Patterns Discovered
- **Lazy Dependency Flags:** The `OptionalDependencyFlag` class combined with `importlib.util.find_spec` provides a robust way to check for installed packages without incurring import overhead at module level. This is critical for keeping startup time fast while supporting optional features.
- **Protocol Stubs for Optional Deps:** Defining `Protocol` classes (e.g., `ArrowTableProtocol`) that mirror the API of optional library classes allows static type checkers (mypy, pyright) to validate code paths even when the library is not installed in the dev environment.
- **Conditional Type Aliases:** Using `try/except ImportError` to assign either the real class or the Protocol stub to a common name (e.g., `ArrowTable`) enables seamless type hinting that works in both presence and absence of the dependency.
- **Consolidated Typing Facade:** Centralizing all optional dependency checks and types in `_internal.typing` prevents circular imports and provides a single source of truth for the codebase.

## Lessons Learned
- **Type Checker "Unused Ignore" Noise:** When using conditional imports with `type: ignore` comments (essential for the missing dependency case), type checkers running in an environment *with* the dependency will flag the ignores as unused. This is an unavoidable trade-off unless using complex config overrides, but acceptable for the benefit of dual support. **Update:** Adding `unused-ignore` to the list of ignored codes (e.g. `type: ignore[..., unused-ignore]`) effectively silences this noise.
- **Extension Hooks:** Implementing extension hooks (like `register_json_codecs`) requires careful handling of async context and dependency checks to avoid runtime errors during connection initialization.

## Decisions Made
- **Abandon Nanoarrow:** We stuck with `pyarrow` as the single source of truth for Arrow operations to align with SQLSpec's patterns and reduce maintenance burden, rather than trying to support multiple Arrow implementations.
- **Drop Pgvector (Temporarily):** We removed explicit `pgvector` support in Phase 3 due to potential compatibility risks with the specific drivers in use. It's safer to rely on standard JSON codecs for now and revisit vector support with a dedicated PRD.

## Reusable Components
- `vertebra._internal.module_loader`: The `module_available`, `ensure_dependency`, and `dependency_flag` utilities are highly reusable for any future optional integration.
- `vertebra._internal.arrow`: The Arrow Protocol stubs can be reused for any other Arrow-interfacing components.

## Gotchas & Warnings
- **Mypy strictness:** Be aware of "unused type ignore" errors in CI environments that install "all" dependencies. Use `type: ignore[unused-ignore]` to handle this.
- **Runtime Imports:** Always guard imports of optional dependencies behind `if FLAG:` checks inside functions/methods, never at module level (except for type aliases in `if TYPE_CHECKING:` blocks).
