# Knowledge Capture: Vertebra 1.0 Release Path

## Patterns Discovered
- **Portable Runtime Distribution:** Using `python-build-standalone` (PBS) allows us to ship a full Python environment without relying on the system Python. This decouples our "User Application Distribution" from the "Framework Distribution".
- **Asset Caching:** Downloading large artifacts (like PBS or the Engine) requires robust caching in `~/.cache/vertebra` or XDG equivalent to avoid re-downloading on every build.
- **Async CLI Commands:** Wrapping async logic (like downloading/bundling) with `asyncio.run()` inside synchronous `click` commands provides a clean bridge between the CLI interface and async core logic.

## Lessons Learned
- **Async Downloading:** Using `httpx` with `stream()` is essential for large file downloads to keep memory usage low and provide progress feedback capabilities (even if minimal initially).
- **Test Mocking:** Mocking async iterators (like `response.aiter_bytes`) requires a specific pattern (async generator function) rather than a simple `MagicMock` return value.
- **Patching Modules vs Imports:** When testing code that uses `from module import Class`, you must patch `module.Class` where it is *imported*, not where it is *defined*, if the import happens at the top level. However, if the import is inside a function (to avoid circular deps or startup cost), you patch the source module path.

## Decisions Made
- **HTTP Client:** Chose `httpx` for downloading PBS releases because it supports async/await and streaming out of the box, aligning with our modern Python stack.
- **Profile Strategy:** Adopted a `modern` (x86-64-v3) vs `baseline` (x86-64) profile strategy to balance performance on new hardware with compatibility on legacy systems.
- **User App Distribution:** Implemented via `vertebra build --one-file` as the primary mechanism for users to package their apps, distinct from the framework's PyPI distribution.

## Reusable Components
- `vertebra.build.standalone.StandaloneManager`: Can be reused for any tool that needs to download and manage PBS versions.
- `vertebra.build.bundler.Bundler`: Generic orchestration for assembling Python applications with an embedded runtime.

## Gotchas & Warnings
- **Platform Mapping:** ARM64 mappings on PBS don't always distinguish between `baseline` and `modern` in the same way x64 does. We currently map them to the same artifact, but this might change.
