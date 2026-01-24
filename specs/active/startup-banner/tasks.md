# Tasks: Server Startup Banner System

## Phase 1: Core Banner Infrastructure

- [ ] 1. Create `src/py/vertebra/cli/banner/` module structure
  - [ ] 1.1 Create `__init__.py` with public exports
  - [ ] 1.2 Create `styles.py` with COLORS and CHARS dicts
  - [ ] 1.3 Create `logos.py` with LOGO_FULL, LOGO_COMPACT, LOGO_MINIMAL

- [ ] 2. Implement terminal detection in `renderer.py`
  - [ ] 2.1 Create `TerminalCaps` dataclass
  - [ ] 2.2 Implement `detect_terminal_capabilities()` function
  - [ ] 2.3 Create abstract `Renderer` base class

- [ ] 3. Implement `TTYRenderer` class
  - [ ] 3.1 Logo selection based on width
  - [ ] 3.2 Section header rendering with underlines
  - [ ] 3.3 Key-value pair rendering with alignment
  - [ ] 3.4 Status indicator rendering with colors

- [ ] 4. Implement `BasicRenderer` class
  - [ ] 4.1 JSON output format for non-TTY
  - [ ] 4.2 Simple text fallback option

- [ ] 5. Write unit tests for Phase 1
  - [ ] 5.1 Test TTY detection
  - [ ] 5.2 Test logo variant selection
  - [ ] 5.3 Test color mode detection
  - [ ] 5.4 Test rendering output

## Phase 2: Status Collection

- [ ] 6. Add `/internal/status` endpoint to Bun server
  - [ ] 6.1 Create `src/ts/src/server/status.ts`
  - [ ] 6.2 Implement `createStatusHandler()` function
  - [ ] 6.3 Register endpoint in `index.ts` router
  - [ ] 6.4 Write TypeScript tests for status endpoint

- [ ] 7. Implement `StatusCollector` in Python
  - [ ] 7.1 Create `collector.py` module
  - [ ] 7.2 Define status dataclasses
  - [ ] 7.3 Implement `fetch()` method with httpx
  - [ ] 7.4 Handle connection failures gracefully

- [ ] 8. Write integration tests for status collection
  - [ ] 8.1 Test successful status fetch
  - [ ] 8.2 Test timeout handling
  - [ ] 8.3 Test connection refused handling

## Phase 3: Pane Rendering

- [ ] 9. Implement Server info pane
  - [ ] 9.1 Display listening URL
  - [ ] 9.2 Display worker count
  - [ ] 9.3 Display log level
  - [ ] 9.4 Format for different widths

- [ ] 10. Implement Components pane
  - [ ] 10.1 Display Bun Engine status with version
  - [ ] 10.2 Display Rust IPC status with channel count
  - [ ] 10.3 Display Broker status with pool info
  - [ ] 10.4 Dotted leader lines for alignment

- [ ] 11. Apply Litestar color scheme
  - [ ] 11.1 Logo in gold
  - [ ] 11.2 Headers in slate blue
  - [ ] 11.3 Status indicators color-coded
  - [ ] 11.4 Fallback colors for basic terminals

## Phase 4: User Extensibility

- [ ] 12. Implement `HookRegistry` class
  - [ ] 12.1 Create `hooks.py` module
  - [ ] 12.2 Implement section registration
  - [ ] 12.3 Implement ordering
  - [ ] 12.4 Implement collection with error handling

- [ ] 13. Create `@banner_section` decorator
  - [ ] 13.1 Decorator function with name/order params
  - [ ] 13.2 Mode filtering (all/serve/dev)
  - [ ] 13.3 Callback timeout protection

- [ ] 14. Integrate extensions into render pipeline
  - [ ] 14.1 Collect extensions during render
  - [ ] 14.2 Render extension sections after built-in
  - [ ] 14.3 Handle empty extension results

- [ ] 15. Write extension system tests
  - [ ] 15.1 Test section registration
  - [ ] 15.2 Test ordering
  - [ ] 15.3 Test error handling
  - [ ] 15.4 Test mode filtering

## Phase 5: Dev Mode and Polish

- [ ] 16. Create `vertebra dev` command
  - [ ] 16.1 Create `src/py/vertebra/cli/dev.py`
  - [ ] 16.2 Add to CLI group in `__init__.py`
  - [ ] 16.3 Configure single worker with auto-reload
  - [ ] 16.4 Set debug log level by default

- [ ] 17. Implement Development pane
  - [ ] 17.1 Hot reload status display
  - [ ] 17.2 Watched paths display
  - [ ] 17.3 Debug endpoint URLs
  - [ ] 17.4 Only show in dev mode

- [ ] 18. Add `--no-banner` flag
  - [ ] 18.1 Add flag to `serve` command
  - [ ] 18.2 Add flag to `dev` command
  - [ ] 18.3 Skip banner render when flag set

- [ ] 19. Update `serve.py` to use new banner
  - [ ] 19.1 Import `render_banner` function
  - [ ] 19.2 Replace old `print_banner()` call
  - [ ] 19.3 Remove old BANNER constant
  - [ ] 19.4 Pass mode and config to renderer

- [ ] 20. Create visual verification tests
  - [ ] 20.1 Create `tools/test_banner_widths.py`
  - [ ] 20.2 Test at 120, 100, 80, 60, 40 columns
  - [ ] 20.3 Capture screenshots for documentation

- [ ] 21. Final documentation
  - [ ] 21.1 Document extension API
  - [ ] 21.2 Document color scheme
  - [ ] 21.3 Document `--no-banner` flag
  - [ ] 21.4 Add banner examples to docs

## Quality Gates

- [ ] All unit tests passing (`make test`)
- [ ] All TypeScript tests passing (`make js-test`)
- [ ] Lint passing (`make lint-all`)
- [ ] Test coverage >= 90% for banner module
- [ ] Visual verification at all breakpoints
- [ ] Non-TTY JSON output validated
