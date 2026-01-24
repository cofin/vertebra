# Tasks: Server Startup Banner System

## Phase 1: Core Banner Infrastructure

- [x] 1. Create `src/py/vertebra/cli/banner/` module structure
  - [x] 1.1 Create `__init__.py` with public exports
  - [x] 1.2 Create `styles.py` with COLORS and CHARS dicts
  - [x] 1.3 Create `logos.py` with LOGO_FULL, LOGO_COMPACT, LOGO_MINIMAL

- [x] 2. Implement terminal detection in `renderer.py`
  - [x] 2.1 Create `TerminalCaps` dataclass
  - [x] 2.2 Implement `detect_terminal_capabilities()` function
  - [x] 2.3 Create abstract `Renderer` base class

- [x] 3. Implement `TTYRenderer` class
  - [x] 3.1 Logo selection based on width
  - [x] 3.2 Section header rendering with underlines
  - [x] 3.3 Key-value pair rendering with alignment
  - [x] 3.4 Status indicator rendering with colors

- [x] 4. Implement `BasicRenderer` class
  - [x] 4.1 JSON output format for non-TTY
  - [x] 4.2 Simple text fallback option

- [x] 5. Write unit tests for Phase 1
  - [x] 5.1 Test TTY detection
  - [x] 5.2 Test logo variant selection
  - [x] 5.3 Test color mode detection
  - [x] 5.4 Test rendering output

## Phase 2: Status Collection

- [x] 6. Add `/internal/status` endpoint to Bun server
  - [x] 6.1 Create `src/ts/src/server/status.ts`
  - [x] 6.2 Implement `createStatusHandler()` function
  - [x] 6.3 Register endpoint in `index.ts` router
  - [x] 6.4 Write TypeScript tests for status endpoint

- [x] 7. Implement `StatusCollector` in Python
  - [x] 7.1 Create `collector.py` module
  - [x] 7.2 Define status dataclasses
  - [x] 7.3 Implement `fetch()` method with httpx
  - [x] 7.4 Handle connection failures gracefully

- [x] 8. Write integration tests for status collection
  - [x] 8.1 Test successful status fetch
  - [x] 8.2 Test timeout handling
  - [x] 8.3 Test connection refused handling

## Phase 3: Pane Rendering

- [x] 9. Implement Server info pane
  - [x] 9.1 Display listening URL
  - [x] 9.2 Display worker count
  - [x] 9.3 Display log level
  - [x] 9.4 Format for different widths

- [x] 10. Implement Components pane
  - [x] 10.1 Display Bun Engine status with version
  - [x] 10.2 Display Rust IPC status with channel count
  - [x] 10.3 Display Broker status with pool info
  - [x] 10.4 Dotted leader lines for alignment

- [x] 11. Apply Litestar color scheme
  - [x] 11.1 Logo in default terminal color (gold reserved for accents)
  - [x] 11.2 Headers in default terminal color
  - [x] 11.3 Status indicators color-coded
  - [x] 11.4 Fallback colors for basic terminals

## Phase 4: User Extensibility

- [x] 12. Implement `HookRegistry` class
  - [x] 12.1 Create `hooks.py` module
  - [x] 12.2 Implement section registration
  - [x] 12.3 Implement ordering
  - [x] 12.4 Implement collection with error handling

- [x] 13. Create `@banner_section` decorator
  - [x] 13.1 Decorator function with name/order params
  - [x] 13.2 Mode filtering (all/serve/dev)
  - [ ] 13.3 Callback timeout protection (deferred)

- [x] 14. Integrate extensions into render pipeline
  - [x] 14.1 Collect extensions during render
  - [x] 14.2 Render extension sections after built-in
  - [x] 14.3 Handle empty extension results

- [x] 15. Write extension system tests
  - [x] 15.1 Test section registration
  - [x] 15.2 Test ordering
  - [x] 15.3 Test error handling
  - [x] 15.4 Test mode filtering

## Phase 5: Dev Mode and Polish

- [ ] 16. Create `vertebra dev` command (deferred to future PR)
  - [ ] 16.1 Create `src/py/vertebra/cli/dev.py`
  - [ ] 16.2 Add to CLI group in `__init__.py`
  - [ ] 16.3 Configure single worker with auto-reload
  - [ ] 16.4 Set debug log level by default

- [x] 17. Implement Development pane
  - [x] 17.1 Hot reload status display
  - [x] 17.2 Watched paths display
  - [x] 17.3 Debug endpoint URLs
  - [x] 17.4 Only show in dev mode

- [x] 18. Add `--no-banner` flag
  - [x] 18.1 Add flag to `serve` command
  - [ ] 18.2 Add flag to `dev` command (when dev command is added)
  - [x] 18.3 Skip banner render when flag set

- [x] 19. Update `serve.py` to use new banner
  - [x] 19.1 Import `render_banner` function
  - [x] 19.2 Replace old `print_banner()` call
  - [x] 19.3 Remove old BANNER constant
  - [x] 19.4 Pass mode and config to renderer

- [x] 20. Create visual verification tests
  - [x] 20.1 Create `tools/test_banner_widths.py`
  - [x] 20.2 Test at 120, 100, 80, 60, 40 columns
  - [ ] 20.3 Capture screenshots for documentation (manual)

- [ ] 21. Final documentation (deferred to docs PR)
  - [ ] 21.1 Document extension API
  - [ ] 21.2 Document color scheme
  - [ ] 21.3 Document `--no-banner` flag
  - [ ] 21.4 Add banner examples to docs

## Quality Gates

- [x] All unit tests passing (`make test`)
- [x] All TypeScript tests passing (`make js-test`)
- [x] Lint passing (`make lint-all`)
- [x] Test coverage >= 90% for banner module (95 tests)
- [x] Visual verification at all breakpoints
- [x] Non-TTY JSON output validated
