---
type: Task
id: critical-correctness-fixes:t07
title: Local embedders run the configured model on free-threaded builds
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
files:
  - src/py/vertebra/embeddings/_broker.py
  - src/py/vertebra/embeddings/_local.py
  - src/py/vertebra/embeddings/__init__.py
  - docs/usage/embeddings-and-vectors.md
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
  - docs/changelog/index.md
tests:
  - src/py/tests/unit/embeddings/test_embedding_broker.py
plan_revision: 2
plan_commit: 24e2931
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-03T17:05:55Z
last_verified_commit: b2bfa30
verification_evidence: "RED: 1 collection ImportError; GREEN: 22 passed in 0.62s; ruff check/format and mypy passed on 9 source files"
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T17:05:55Z
commit: null
tags: [bugfix, embeddings, free-threaded, broker]
---

# t07 — Local embedders run the configured model on free-threaded builds

## Objective

`EmbeddingBroker` never produces hash vectors: it runs either a caller-supplied `backend_fn` or a `BackendFactory` that names the real model, in-process or in the free-threaded sidecar, and `FastEmbedder`/`GemmaEmbedder` hand their broker the fastembed factory.

## Context

- **Strategy**: `regression_tdd`. The defect reproduces at `142cc49`.
- **Defects confirmed at HEAD**:
  - `src/py/vertebra/embeddings/_broker.py::EmbeddingBroker.__init__` sets `self._backend_fn = backend_fn or self._deterministic_hash_backend`, so a broker without a backend silently returns SHA-256 vectors.
  - `_run_sidecar_worker_main` always calls `EmbeddingBroker._deterministic_hash_backend`, so every free-threaded quarantine request returns hash vectors regardless of the configured model.
  - `src/py/vertebra/embeddings/_local.py::FastEmbedder.__init__` builds `fe_instance = fe_mod.TextEmbedding(...)` and then, on free-threaded builds, passes `None` to the broker, discarding the instance. `GemmaEmbedder.__init__` passes `None` on free-threaded builds.
- **Why a factory**: the sidecar is a separate interpreter (`sys.executable -c ...`). Closures and ONNX sessions do not pickle, so the parent sends an importable `module:qualname` target and keyword options; the sidecar imports it and builds the backend once. The same factory builds the in-process backend, so both paths run the same model. Running the model in-process on free-threaded builds is rejected because importing `onnxruntime` re-enables the GIL.
- **Public additions** (spec §2.2): `BackendFactory` (exported from `vertebra.embeddings`), `EmbeddingBroker(backend_fn=None, *, dimensions, config, backend_factory=None)`, `EmbeddingBroker.backend_factory` property, `FastEmbedder.build_backend` staticmethod. `EmbeddingBroker` with neither backend raises `ImproperConfigurationError` (from `vertebra.typing`).
- **Test environment facts**: `fastembed` is not installed in the dev venv, and `src/py` is on the venv path through the maturin `.pth` files, so the sidecar can import `tests.unit.embeddings.test_embedding_broker`. In-process `FastEmbedder` tests substitute a fake `fastembed` module in `sys.modules` (external boundary); `ensure_module` checks `sys.modules` first.
- **Existing tests that change** (`src/py/tests/unit/embeddings/test_embedding_broker.py::test_subprocess_quarantine_shm_slab_exchange_and_fastembed_optional_guard`): its first block builds `EmbeddingBroker(backend_fn=None, ...)` and relies on the hash sidecar; its last block runs `GemmaEmbedder(...)` inference through the sidecar without `fastembed`. The first block switches to a test `BackendFactory`; the last block asserts the configured factory instead of running inference.
- **Canonical test module**: `src/py/tests/unit/embeddings/test_embedding_broker.py`.
- **Code rules**: `msgspec.Struct(frozen=True, gc=False)` instead of dataclasses; PEP 585/604/695 types; Google-style docstrings; zero inline `#` comments; top-level imports only (except `TYPE_CHECKING` and optional-dependency helpers); class layout attributes → `__init__` → properties → public methods → private methods; JSON through `vertebra.utils.serializers`; `@pytest.mark.parametrize` instead of loops over cases; no compatibility aliases or `try/except` fallbacks for test seeds; no finding IDs in code, tests, or commits.
- **Knowledge**: [embeddings-and-vectors](../../../knowledge/domains/agent/embeddings-and-vectors.md).

## Steps

### 1. Write failing tests (RED)

In `src/py/tests/unit/embeddings/test_embedding_broker.py`:

1. Imports: add `import types` and `from typing import Any` to the standard-library group; add `BackendFactory` and `BackendInferenceCallable` to the existing `from vertebra.embeddings import (...)` block; change `from vertebra.typing import MissingDependencyError` to `from vertebra.typing import ImproperConfigurationError, MissingDependencyError`.
2. Add module-level helpers after the imports:

   ```python
   _LENGTH_BACKEND_TARGET = "tests.unit.embeddings.test_embedding_broker:build_length_backend"
   _FASTEMBED_BACKEND_TARGET = "vertebra.embeddings._local:FastEmbedder.build_backend"


   def build_length_backend(scale: float = 1.0) -> BackendInferenceCallable:
       """Return a backend that encodes each text as ``[len(text) * scale, 1.0, 0.0, 0.0]``."""

       def _infer(texts: list[str], dims: int) -> list[list[float]]:
           return [[float(len(text)) * scale, 1.0, 0.0, 0.0][:dims] for text in texts]

       return _infer
   ```

3. In `test_subprocess_quarantine_shm_slab_exchange_and_fastembed_optional_guard`:
   - Replace the first broker construction with:

   ```python
   broker = EmbeddingBroker(
       dimensions=4,
       config=EmbeddingBrokerConfig(isolate_gil_subprocess=True, coalesce_window_ms=0.0),
       backend_factory=BackendFactory(target=_LENGTH_BACKEND_TARGET),
   )
   ```

   (the unit-norm loop assertions stay valid).
   - Delete the line `monkeypatch.setattr("vertebra.embeddings._local.is_free_threaded", lambda: True)`; `_local.py` no longer imports `is_free_threaded`, and the quarantine decision lives in `vertebra.embeddings._broker` (whose patch line stays).
   - Replace the final block (from `ft_quarantined_gemma = GemmaEmbedder(...)` through its `finally`) with:

   ```python
   ft_quarantined_gemma = GemmaEmbedder("embeddinggemma-2-270m", dimensions=4, precision="int8", source="/models/gemma")
   assert ft_quarantined_gemma.broker.should_quarantine_subprocess() is True
   assert ft_quarantined_gemma.broker.backend_factory == BackendFactory(
       target=_FASTEMBED_BACKEND_TARGET,
       options={"model_name": "embeddinggemma-2-270m", "specific_model_path": "/models/gemma"},
   )
   ```

4. Append new tests:

   ```python
   @pytest.mark.anyio
   async def test_sidecar_runs_configured_backend_factory() -> None:
       """The quarantined sidecar builds and runs the configured backend factory."""
       broker = EmbeddingBroker(
           dimensions=4,
           config=EmbeddingBrokerConfig(isolate_gil_subprocess=True, coalesce_window_ms=0.0),
           backend_factory=BackendFactory(target=_LENGTH_BACKEND_TARGET, options={"scale": 2.0}),
       )
       try:
           batch = await broker.submit_batch_in_subprocess(["ab", "abcd"])
           assert batch.to_list() == [
               pytest.approx([0.9701425, 0.2425356, 0.0, 0.0], rel=1e-5),
               pytest.approx([0.9922779, 0.1240347, 0.0, 0.0], rel=1e-5),
           ]
       finally:
           await broker.close()


   def test_embedding_broker_requires_backend() -> None:
       """A broker without a backend callable or factory is a configuration error."""
       with pytest.raises(ImproperConfigurationError, match="backend_fn or backend_factory"):
           EmbeddingBroker(dimensions=4)


   @pytest.mark.anyio
   async def test_fast_embedder_runs_fastembed_backend_in_process(monkeypatch: pytest.MonkeyPatch) -> None:
       """FastEmbedder runs fastembed.TextEmbedding in-process with the configured options."""
       init_calls: list[dict[str, Any]] = []

       class FakeTextEmbedding:
           def __init__(self, **kwargs: Any) -> None:
               init_calls.append(kwargs)

           def embed(self, texts: list[str]) -> list[list[float]]:
               return [[3.0, 4.0] if text == "a" else [0.0, 2.0] for text in texts]

       fake_module = types.ModuleType("fastembed")
       monkeypatch.setattr(fake_module, "TextEmbedding", FakeTextEmbedding, raising=False)
       monkeypatch.setitem(sys.modules, "fastembed", fake_module)

       embedder = FastEmbedder(
           "BAAI/bge-small-en-v1.5",
           dimensions=2,
           threads=2,
           broker_config=EmbeddingBrokerConfig(isolate_gil_subprocess=False, coalesce_window_ms=0.0),
       )
       assert init_calls == []
       batch = await embedder.embed_many(["a", "bb"])
       assert batch.to_list() == [pytest.approx([0.6, 0.8]), pytest.approx([0.0, 1.0])]
       assert init_calls == [{"model_name": "BAAI/bge-small-en-v1.5", "threads": 2}]


   def test_local_embedders_configure_fastembed_backend_factory(monkeypatch: pytest.MonkeyPatch) -> None:
       """FastEmbedder hands its broker the fastembed factory, which the free-threaded sidecar runs."""
       monkeypatch.setitem(sys.modules, "fastembed", types.ModuleType("fastembed"))
       monkeypatch.setattr("vertebra.embeddings._broker.is_free_threaded", lambda: True)
       embedder = FastEmbedder("BAAI/bge-small-en-v1.5", dimensions=4, cache_dir="/tmp/fe-cache")
       assert embedder.broker.should_quarantine_subprocess() is True
       assert embedder.broker.backend_factory == BackendFactory(
           target=_FASTEMBED_BACKEND_TARGET,
           options={
               "model_name": "BAAI/bge-small-en-v1.5",
               "cache_dir": "/tmp/fe-cache",
               "specific_model_path": None,
               "threads": None,
           },
       )
   ```

5. Run RED:

   ```bash
   uv run pytest src/py/tests/unit/embeddings/test_embedding_broker.py -q
   ```

   Expected: collection fails with `ImportError: cannot import name 'BackendFactory' from 'vertebra.embeddings'`.

### 2. Implement (GREEN)

1. `src/py/vertebra/embeddings/_broker.py`:
   - Imports: remove `hashlib`; add `from typing import Any, cast`; add `from vertebra.typing import ImproperConfigurationError`; add `from vertebra.utils.module_loader import ensure_module`.
   - `__all__ = ("BackendFactory", "BackendInferenceCallable", "EmbeddingBroker", "EmbeddingBrokerConfig")`.
   - Add after `EmbeddingBrokerConfig`:

   ```python
   class BackendFactory(msgspec.Struct, frozen=True, gc=False, kw_only=True):
       """Importable factory that builds an embedding inference callable.

       The quarantined sidecar receives this value over its JSON pipe and imports ``target`` itself,
       so the factory must be importable in a fresh interpreter.

       Attributes:
           target: ``"package.module:qualname"`` of a callable returning a ``BackendInferenceCallable``.
           options: Keyword arguments passed to the target callable.
       """

       target: str
       options: dict[str, Any] = msgspec.field(default_factory=dict)

       def build(self) -> BackendInferenceCallable:
           """Import ``target`` and call it with ``options``.

           Raises:
               ImproperConfigurationError: When ``target`` is not ``module:qualname``.
               MissingDependencyError: When the target module cannot be imported.
           """
           module_name, _, qualname = self.target.partition(":")
           if not module_name or not qualname:
               msg = f"BackendFactory target must be 'module:qualname', got {self.target!r}"
               raise ImproperConfigurationError(msg)
           resolved: Any = ensure_module(module_name)
           for attr in qualname.split("."):
               resolved = getattr(resolved, attr)
           return cast("BackendInferenceCallable", resolved(**self.options))
   ```

   - `_SubprocessRpcRequest`: add field `factory: BackendFactory` after `dimensions`.
   - `EmbeddingBroker.__init__(self, backend_fn: BackendInferenceCallable | None = None, *, dimensions: int = 768, config: EmbeddingBrokerConfig | None = None, backend_factory: BackendFactory | None = None)`:

   ```python
   if backend_fn is None and backend_factory is None:
       msg = "EmbeddingBroker requires backend_fn or backend_factory"
       raise ImproperConfigurationError(msg)
   self._has_custom_backend = backend_fn is not None
   self._backend_fn: BackendInferenceCallable | None = backend_fn
   self._backend_factory = backend_factory
   ```

   (keep the remaining attribute assignments). Document both parameters and the `Raises:` section in the docstring.
   - Add property after `config`: `def backend_factory(self) -> BackendFactory | None: """Return the factory used for in-process and sidecar inference.""" return self._backend_factory`.
   - `submit_batch_in_subprocess`: before acquiring the lock, add

   ```python
   if self._backend_factory is None:
       msg = "Subprocess inference requires a backend_factory"
       raise ImproperConfigurationError(msg)
   ```

   and pass `factory=self._backend_factory` to `_SubprocessRpcRequest(...)`.
   - `_execute_length_sorted_rows` (single-text path) and `_run_sub_batch`: call `self._resolve_backend()(...)` instead of `self._backend_fn(...)`.
   - Add private method at the end of the class:

   ```python
   def _resolve_backend(self) -> BackendInferenceCallable:
       """Return the in-process backend, building it from ``backend_factory`` on first use."""
       if self._backend_fn is None:
           self._backend_fn = cast("BackendFactory", self._backend_factory).build()
       return self._backend_fn
   ```

   - Delete `_deterministic_hash_backend`.
   - `_run_sidecar_worker_main`: before the loop add `backends: dict[tuple[str, str], BackendInferenceCallable] = {}`; inside `try`, after decoding `req`, replace the hash call with

   ```python
   cache_key = (req.factory.target, encode_json(req.factory.options))
   backend = backends.get(cache_key)
   if backend is None:
       backend = req.factory.build()
       backends[cache_key] = backend
   rows = backend(req.texts, req.dimensions)
   ```

2. `src/py/vertebra/embeddings/_local.py`:
   - Import `BackendFactory` from `vertebra.embeddings._broker`; remove the now-unused `is_free_threaded` import only if nothing else uses it (after the change nothing does).
   - Add module constant `_FASTEMBED_BACKEND_TARGET = "vertebra.embeddings._local:FastEmbedder.build_backend"`.
   - `GemmaEmbedder.__init__`: replace the `quarantine_default` / `resolved_backend` logic and broker construction with

   ```python
   self._broker = broker or EmbeddingBroker(
       backend_fn=backend_fn,
       dimensions=dimensions,
       config=broker_config,
       backend_factory=BackendFactory(
           target=_FASTEMBED_BACKEND_TARGET,
           options={"model_name": model, "specific_model_path": self._source},
       ),
   )
   ```

   and delete `_build_default_backend`.
   - `FastEmbedder.__init__`: replace the `if backend_fn is None:` block and broker construction with

   ```python
   if backend_fn is None:
       ensure_module("fastembed", extra="embeddings")
   self._broker = broker or EmbeddingBroker(
       backend_fn=backend_fn,
       dimensions=dimensions,
       config=broker_config,
       backend_factory=BackendFactory(
           target=_FASTEMBED_BACKEND_TARGET,
           options={
               "model_name": model,
               "cache_dir": self._cache_dir,
               "specific_model_path": self._specific_model_path,
               "threads": self._threads,
           },
       ),
   )
   ```

   - Add to `FastEmbedder` (after properties, before `_embed_single_impl`):

   ```python
   @staticmethod
   def build_backend(
       model_name: str,
       *,
       cache_dir: str | None = None,
       specific_model_path: str | None = None,
       threads: int | None = None,
   ) -> BackendInferenceCallable:
       """Return an inference callable that lazily loads ``fastembed.TextEmbedding``.

       The model loads on the first call, so building the callable never downloads weights.
       Only options that are not ``None`` are passed to ``TextEmbedding``.
       """
       init_kwargs: dict[str, Any] = {"model_name": model_name}
       if cache_dir is not None:
           init_kwargs["cache_dir"] = cache_dir
       if specific_model_path is not None:
           init_kwargs["specific_model_path"] = specific_model_path
       if threads is not None:
           init_kwargs["threads"] = threads
       loaded: list[Any] = []

       def _infer(texts: list[str], dims: int) -> list[list[float]]:
           if not loaded:
               fe_mod = ensure_module("fastembed", extra="embeddings")
               loaded.append(fe_mod.TextEmbedding(**init_kwargs))
           return [[float(v) for v in row[:dims]] for row in loaded[0].embed(texts)]

       return _infer
   ```

3. `src/py/vertebra/embeddings/__init__.py`: import `BackendFactory` from `vertebra.embeddings._broker` and add `"BackendFactory"` to `__all__` (alphabetical, before `"BackendInferenceCallable"`).
4. Docs `docs/usage/embeddings-and-vectors.md`, section `## Local 1x RAM Broker & EmbeddingGemma`: append `` On free-threaded Python (`3.14t`/`3.15t`) the broker runs the configured model in a quarantined subprocess. Custom brokers pass either `backend_fn` (in-process callable) or `BackendFactory(target="package.module:callable", options={...})`, which the subprocess imports and runs. ``
5. Knowledge `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md`, replace the "Free-Threaded 3.14t/3.15t Subprocess Quarantine" bullet text after the bold label with: `` When `is_free_threaded()` is `True`, `isolate_gil_subprocess` is enabled, and no in-process `backend_fn` was passed, `EmbeddingBroker` sends batches to a child process (`submit_batch_in_subprocess`) together with its `BackendFactory` (`target="module:qualname"`, `options`); the child imports the factory once, runs the real model, and returns a `/dev/shm` `VectorSlab`. `FastEmbedder` and `GemmaEmbedder` pass `FastEmbedder.build_backend`. A broker without `backend_fn` or `backend_factory` raises `ImproperConfigurationError`. ``
6. Changelog `docs/changelog/index.md`, `## 0.3.0`:
   - `### Added`: `` - `BackendFactory` in `vertebra.embeddings` and `FastEmbedder.build_backend` for importable embedding backends. ``
   - `### Fixed`: `` - `EmbeddingBroker` runs the configured model in the free-threaded sidecar instead of a hash backend, requires `backend_fn` or `backend_factory`, and `FastEmbedder` no longer builds a discarded `TextEmbedding` at construction. ``

### 3. Refactor & verify

1. Run the GREEN commands; run `make lint`.
2. Commit:

   ```bash
   git add src/py/vertebra/embeddings/_broker.py src/py/vertebra/embeddings/_local.py src/py/vertebra/embeddings/__init__.py src/py/tests/unit/embeddings/test_embedding_broker.py docs/usage/embeddings-and-vectors.md .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md docs/changelog/index.md
   git commit -S -m "fix(embeddings): run the configured model in the free-threaded sidecar"
   ```

## Verification

- **RED**: the Step 1.5 command exits `2` with the collection `ImportError`.
- **GREEN**:

  ```bash
  uv run pytest src/py/tests/unit/embeddings -q
  uv run ruff check src/py/vertebra/embeddings src/py/tests/unit/embeddings/test_embedding_broker.py
  uv run ruff format --check src/py/vertebra/embeddings src/py/tests/unit/embeddings/test_embedding_broker.py
  uv run mypy src/py/vertebra/embeddings
  ```

  All exit `0`.
- **Phase gate**: Phase 2 checkpoint in `spec.md`.

## Acceptance Criteria

- [x] R11: No hash backend remains in `_broker.py`; a broker without `backend_fn` or `backend_factory` raises `ImproperConfigurationError`.
- [x] R11: The sidecar builds and caches the configured `BackendFactory`; vectors match the factory output.
- [x] R11: `FastEmbedder` and `GemmaEmbedder` pass the fastembed `BackendFactory`; `FastEmbedder` builds `TextEmbedding` lazily on first inference.
- [x] Docs, knowledge, and changelog updated; targeted checks pass; one signed commit.

## Notes & Discoveries

- [2026-11-03 15:30] Planning: the existing quarantine test only passed because the sidecar ran the hash backend; `fastembed` is absent from the dev venv.
- [2026-11-03 17:05] Verified RED (`1 error in 0.17s` at collection) and GREEN (`22 passed in 0.62s`, `ruff check`, `ruff format --check`, and `mypy` passed on 9 source files).
