# Learnings — `pgo-packaging-and-docs-sync`

- **PyO3 Multi-Extension Maturin Layout (`pyproject.toml` vs Crate-Local `pyproject.toml`)**: `src/rs/vertebra-bundler/pyproject.toml` and `src/rs/vertebra-storage/pyproject.toml` configure `module-name` locally (requiring `-m <manifest>`), whereas `src/rs/vertebra-py` relies on the repository-root `pyproject.toml` (`[tool.maturin] module-name = "vertebra._vertebra"`, `python-source = "src/py"`), so `maturin develop` for `vertebra-py` must run from the repository root without `-m src/rs/vertebra-py/Cargo.toml`.
