"""Local shared-memory embedders: GemmaEmbedder, FastEmbedder, and StaticEmbedder."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from vertebra._vertebra import (
    StaticEmbedderCore,
    VectorBatchView,
    VectorCache,
    VectorView,
    format_gemma_prompt,
)
from vertebra.embeddings._base import Embedder
from vertebra.embeddings._broker import (
    BackendFactory,
    BackendInferenceCallable,
    EmbeddingBroker,
    EmbeddingBrokerConfig,
)
from vertebra.embeddings._models import get_model_spec
from vertebra.utils.module_loader import ensure_module

__all__ = (
    "FastEmbedder",
    "GemmaEmbedder",
    "StaticEmbedder",
)

_FASTEMBED_BACKEND_TARGET = "vertebra.embeddings._local:FastEmbedder.build_backend"


class GemmaEmbedder(Embedder):
    """Local EmbeddingGemma / EmbeddingGemma 2 embedder with `fp16` overflow guard and `EmbeddingBroker` pooling."""

    def __init__(
        self,
        model: str = "embeddinggemma-2-270m",
        *,
        dimensions: int = 768,
        precision: str = "int8",
        source: str | Path | None = None,
        cache: VectorCache | None = None,
        broker: EmbeddingBroker | None = None,
        broker_config: EmbeddingBrokerConfig | None = None,
        backend_fn: BackendInferenceCallable | None = None,
    ) -> None:
        """Initialize the local Gemma embedder and enforce precision invariants."""
        spec = get_model_spec(model)
        norm_prec = precision.lower().strip()
        forbid_fp16 = (spec is not None and spec.forbid_fp16) or "gemma-2" in model.lower()
        if norm_prec in ("fp16", "float16") and forbid_fp16:
            raise ValueError(
                f"Model {model!r} (EmbeddingGemma 2) does not support float16 (fp16) due to activation overflow; "
                "use 'int8', 'q4', 'bf16', or 'fp32'."
            )
        allowed_precisions = {"int8", "q4", "bf16", "fp32"}
        if not forbid_fp16:
            allowed_precisions.add("fp16")
            if norm_prec == "float16":
                norm_prec = "fp16"
        if norm_prec not in allowed_precisions:
            supported = ", ".join(sorted(allowed_precisions))
            raise ValueError(f"Unsupported precision {precision!r} for {model!r}; expected one of: {supported}")
        super().__init__(model=model, dimensions=dimensions, cache=cache)
        self._precision = norm_prec
        self._source = str(source) if source is not None else None
        self._broker = broker or EmbeddingBroker(
            backend_fn=backend_fn,
            dimensions=dimensions,
            config=broker_config,
            backend_factory=BackendFactory(
                target=_FASTEMBED_BACKEND_TARGET,
                options={
                    "model_name": model,
                    "specific_model_path": self._source,
                    "precision": self._precision,
                },
            ),
        )

    @property
    def precision(self) -> str:
        """Return the active quantization/precision mode (`int8`, `q4`, `bf16`, or `fp32`)."""
        return self._precision

    @property
    def source(self) -> str | None:
        """Return the optional local path, `/dev/shm` region, or `gs://` URI for model weights."""
        return self._source

    @property
    def broker(self) -> EmbeddingBroker:
        """Return the shared `EmbeddingBroker` instance."""
        return self._broker

    async def _embed_single_impl(
        self,
        text: str,
        *,
        task: str = "query",
        title: str | None = None,
    ) -> VectorView:
        """Format the Gemma task-steered prompt and submit to `EmbeddingBroker`."""
        self._record_usage((text,))
        formatted = format_gemma_prompt(task, text, title=title)
        return await self._broker.submit_single(formatted)

    async def _embed_batch_impl(
        self,
        texts: list[str],
        *,
        task: str = "document",
        titles: list[str | None] | None = None,
    ) -> VectorBatchView:
        """Format a batch of Gemma task-steered prompts and submit to `EmbeddingBroker`."""
        resolved_titles = titles if titles is not None else [None] * len(texts)
        formatted_texts = [format_gemma_prompt(task, txt, title=resolved_titles[idx]) for idx, txt in enumerate(texts)]
        return await self._broker.submit_batch(formatted_texts)


class FastEmbedder(Embedder):
    """Local ONNX `fastembed.TextEmbedding` embedder pooled through `EmbeddingBroker`."""

    def __init__(
        self,
        model: str = "BAAI/bge-small-en-v1.5",
        *,
        dimensions: int = 384,
        cache_dir: str | Path | None = None,
        specific_model_path: str | Path | None = None,
        threads: int | None = None,
        cache: VectorCache | None = None,
        broker: EmbeddingBroker | None = None,
        broker_config: EmbeddingBrokerConfig | None = None,
        backend_fn: BackendInferenceCallable | None = None,
    ) -> None:
        """Initialize the `FastEmbedder` and verify the optional `fastembed` dependency when needed."""
        super().__init__(model=model, dimensions=dimensions, cache=cache)
        self._cache_dir = str(cache_dir) if cache_dir is not None else None
        self._specific_model_path = str(specific_model_path) if specific_model_path is not None else None
        self._threads = threads

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

    @property
    def broker(self) -> EmbeddingBroker:
        """Return the underlying `EmbeddingBroker`."""
        return self._broker

    @staticmethod
    def build_backend(
        model_name: str,
        *,
        cache_dir: str | None = None,
        specific_model_path: str | None = None,
        threads: int | None = None,
        precision: str | None = None,
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
        quantize_setting: bool | None = None
        if precision is not None:
            norm_prec = precision.lower().strip()
            if norm_prec in ("int8", "q4"):
                quantize_setting = True
            elif norm_prec in ("bf16", "fp32", "fp16", "float16"):
                quantize_setting = False
        loaded: list[Any] = []

        def _infer(texts: list[str], dims: int) -> list[list[float]]:
            if not loaded:
                fe_mod = ensure_module("fastembed", extra="embeddings")
                call_kwargs = dict(init_kwargs)
                if quantize_setting is not None:
                    accepts_quantize = False
                    with contextlib.suppress(TypeError, ValueError):
                        params = inspect.signature(fe_mod.TextEmbedding).parameters.values()
                        accepts_quantize = any(
                            p.name == "quantize" or p.kind is inspect.Parameter.VAR_KEYWORD for p in params
                        )
                    if accepts_quantize:
                        call_kwargs["quantize"] = quantize_setting
                loaded.append(fe_mod.TextEmbedding(**call_kwargs))
            return [[float(v) for v in row[:dims]] for row in loaded[0].embed(texts)]

        return _infer

    async def _embed_single_impl(
        self,
        text: str,
        *,
        task: str = "query",
        title: str | None = None,
    ) -> VectorView:
        """Submit a singular text to the `EmbeddingBroker`."""
        self._record_usage((text,))
        return await self._broker.submit_single(text)

    async def _embed_batch_impl(
        self,
        texts: list[str],
        *,
        task: str = "document",
        titles: list[str | None] | None = None,
    ) -> VectorBatchView:
        """Submit a batch of texts to the `EmbeddingBroker`."""
        return await self._broker.submit_batch(texts)


class StaticEmbedder(Embedder):
    """Pure-Rust static token embedding lookup (`StaticEmbedderCore`) with `< 10 us` mean-pooling."""

    def __init__(
        self,
        model: str = "minishlab/potion-base-8M",
        *,
        dimensions: int = 256,
        vocab: Mapping[str, int] | None = None,
        weights: Sequence[float] | Sequence[Sequence[float]] | None = None,
        vocab_size: int = 8192,
        seed: int = 42,
        cache: VectorCache | None = None,
    ) -> None:
        """Initialize the pure-Rust static token embedder."""
        super().__init__(model=model, dimensions=dimensions, cache=cache)
        vocab_dict = {str(k): int(v) for k, v in vocab.items()} if vocab is not None else None
        weights_2d: list[list[float]] | None = None
        if weights is not None:
            first = weights[0] if len(weights) > 0 else None
            if isinstance(first, (int, float)):
                flat_seq = cast("Sequence[float]", weights)
                flat = [float(x) for x in flat_seq]
                weights_2d = [flat[i : i + dimensions] for i in range(0, len(flat), dimensions)]
            else:
                nested_seq = cast("Sequence[Sequence[float]]", weights)
                weights_2d = [[float(x) for x in row] for row in nested_seq]
        self._core = StaticEmbedderCore(
            vocab=vocab_dict,
            weights=weights_2d,
            vocab_size=vocab_size,
            dimensions=dimensions,
            seed=seed,
        )

    async def _embed_single_impl(
        self,
        text: str,
        *,
        task: str = "query",
        title: str | None = None,
    ) -> VectorView:
        """Compute a static embedding in pure Rust with GIL released."""
        self._record_usage((text,))
        return self._core.embed_text(text, dimensions=self._dimensions)

    async def _embed_batch_impl(
        self,
        texts: list[str],
        *,
        task: str = "document",
        titles: list[str | None] | None = None,
    ) -> VectorBatchView:
        """Compute a batch of static embeddings in pure Rust with GIL released."""
        return self._core.embed_batch(texts, dimensions=self._dimensions)
