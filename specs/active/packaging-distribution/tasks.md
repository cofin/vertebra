# Tasks: Packaging and Distribution System

## Phase 1: Template Enhancement

- [ ] 1. Create web template directory structure
  - [ ] 1.1 Create `src/py/vertebra/templates/web/` directory
  - [ ] 1.2 Create `templates/web/templates/` subdirectory
  - [ ] 1.3 Create `templates/web/static/` subdirectory

- [ ] 2. Create web template files
  - [ ] 2.1 Create `pyproject.toml.jinja2` with hatch backend
  - [ ] 2.2 Create `app.py.jinja2` with Vertebra instance and plugins
  - [ ] 2.3 Create `handlers.py.jinja2` with example endpoints
  - [ ] 2.4 Create `templates/base.html.jinja2` for page layout
  - [ ] 2.5 Create `templates/index.html.jinja2` for landing page
  - [ ] 2.6 Create `static/styles.css` with Litestar colors

- [ ] 3. Enhance init command
  - [ ] 3.1 Add `--template` option (default, api, web)
  - [ ] 3.2 Implement template directory selection logic
  - [ ] 3.3 Copy static files (not just jinja templates)
  - [ ] 3.4 Update help text and documentation

- [ ] 4. Write template unit tests
  - [ ] 4.1 Test template rendering with context variables
  - [ ] 4.2 Test directory structure creation
  - [ ] 4.3 Test static file copying
  - [ ] 4.4 Test generated project runs successfully

## Phase 2: Build Command Enhancement

- [ ] 5. Refactor build command
  - [ ] 5.1 Rename `--one-file` to `--onefile` for consistency
  - [ ] 5.2 Add directory build (default mode when no --onefile)
  - [ ] 5.3 Add `--target` option with platform detection
  - [ ] 5.4 Add progress output with rich console

- [ ] 6. Create DirectoryBuilder class
  - [ ] 6.1 Create `src/py/vertebra/build/directory.py`
  - [ ] 6.2 Implement PBS download and extraction
  - [ ] 6.3 Implement dependency installation
  - [ ] 6.4 Implement engine bundling
  - [ ] 6.5 Implement application code copying

- [ ] 7. Create launch script generators
  - [ ] 7.1 Generate `run.sh` for Unix
  - [ ] 7.2 Generate `run.bat` for Windows
  - [ ] 7.3 Set correct permissions on Unix scripts

- [ ] 8. Write directory build tests
  - [ ] 8.1 Test output directory creation
  - [ ] 8.2 Test PBS presence
  - [ ] 8.3 Test engine binary presence
  - [ ] 8.4 Test launch script generation

## Phase 3: PyApp Integration

- [ ] 9. Create OneFileBuilder class
  - [ ] 9.1 Create `src/py/vertebra/build/onefile.py`
  - [ ] 9.2 Implement distribution archive creation
  - [ ] 9.3 Implement PyApp invocation with correct env vars
  - [ ] 9.4 Handle PYAPP_SKIP_INSTALL=true for pre-bundled deps

- [ ] 10. Implement cross-platform targeting
  - [ ] 10.1 Validate target platform string
  - [ ] 10.2 Download correct PBS for target
  - [ ] 10.3 Build engine for target platform
  - [ ] 10.4 Configure PyApp for target platform

- [ ] 11. Add build caching
  - [ ] 11.1 Cache PBS downloads in `~/.cache/vertebra/pbs/`
  - [ ] 11.2 Cache engine builds in `~/.cache/vertebra/engines/`
  - [ ] 11.3 Add `--no-cache` flag to force rebuild

- [ ] 12. Write one-file build tests
  - [ ] 12.1 Test executable creation
  - [ ] 12.2 Test executable runs and serves HTTP
  - [ ] 12.3 Test first-run extraction behavior
  - [ ] 12.4 Test subsequent run performance

## Phase 4: Engine Management

- [ ] 13. Create EngineManager class
  - [ ] 13.1 Create `src/py/vertebra/build/engine.py`
  - [ ] 13.2 Implement get_or_build method
  - [ ] 13.3 Implement cache key generation
  - [ ] 13.4 Implement cache invalidation on version change

- [ ] 14. Add engine install command
  - [ ] 14.1 Create `vertebra engine install` subcommand
  - [ ] 14.2 Download pre-built engine from GitHub releases
  - [ ] 14.3 Verify checksum before installation
  - [ ] 14.4 Display progress during download

- [ ] 15. Write engine management tests
  - [ ] 15.1 Test cache hit behavior
  - [ ] 15.2 Test cache miss triggers build
  - [ ] 15.3 Test version-based cache invalidation

## Phase 5: CI/CD Pipeline

> **CHECKPOINT**: Cross-compilation is complex. If major issues arise with Rust/Bun/PyApp cross-compilation, glibc compatibility, or code signing, **STOP and discuss alternatives** with the user before proceeding. Options include native-only builds, platform subset, or CI-only cross builds.

- [ ] 16. Create release workflow
  - [ ] 16.1 Create `.github/workflows/release-binaries.yml`
  - [ ] 16.2 Define 5-platform matrix
  - [ ] 16.3 Setup Rust, Bun, Python, uv
  - [ ] 16.4 Build wheel with maturin
  - [ ] 16.5 Build binary with hatch

- [ ] 17. Implement cross-compilation support
  - [ ] 17.1 Setup aarch64-linux-gnu toolchain
  - [ ] 17.2 Configure cargo for cross-compilation
  - [ ] 17.3 Test Linux ARM64 builds from x64 runner

- [ ] 18. Add artifact handling
  - [ ] 18.1 Upload binaries to GitHub Release
  - [ ] 18.2 Generate SHA256 checksums
  - [ ] 18.3 Upload checksum files

- [ ] 19. Test workflow on PR (dry run)
  - [ ] 19.1 Add workflow dispatch trigger for testing
  - [ ] 19.2 Verify all platforms build successfully
  - [ ] 19.3 Verify artifacts are correct size/format

## Phase 6: Documentation and Polish

- [ ] 20. Add Sphinx documentation
  - [ ] 20.1 Create `docs/guides/packaging.rst`
  - [ ] 20.2 Document build command options
  - [ ] 20.3 Document template system
  - [ ] 20.4 Add troubleshooting section

- [ ] 21. Update CLAUDE.md with build patterns
  - [ ] 21.1 Add build command reference
  - [ ] 21.2 Document template locations
  - [ ] 21.3 Add CI workflow overview

- [ ] 22. Create example project
  - [ ] 22.1 Add `examples/hello-vertebra/` project
  - [ ] 22.2 Demonstrate build commands
  - [ ] 22.3 Include README with instructions

- [ ] 23. Final validation
  - [ ] 23.1 Run full test suite (`make test-all`)
  - [ ] 23.2 Build executables for all platforms
  - [ ] 23.3 Verify lint passes (`make lint-all`)
  - [ ] 23.4 Test init → build → run workflow end-to-end

## Quality Gates

- [ ] All unit tests passing (`make test`)
- [ ] All TypeScript tests passing (`make js-test`)
- [ ] Lint passing (`make lint-all`)
- [ ] Test coverage >= 90% for new modules
- [ ] Built executables run on all 5 platforms
- [ ] CI workflow succeeds on test run
- [ ] Documentation renders correctly
