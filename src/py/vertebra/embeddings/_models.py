"""Built-in embedding model catalog and ModelSpec metadata."""

from __future__ import annotations

import msgspec

__all__ = (
    "MODEL_CATALOG",
    "ModelSpec",
    "get_model_spec",
    "list_model_specs",
)


class ModelSpec(msgspec.Struct, frozen=True, gc=False):
    """Metadata specification for a supported Google or local embedding model."""

    name: str
    provider: str
    default_dimensions: int
    supported_dimensions: tuple[int, ...]
    max_tokens: int
    quantizations: tuple[str, ...] = ("fp32",)
    mirror_url: str | None = None
    forbid_fp16: bool = False
    task_prefixes: bool = False
    default_files: tuple[str, ...] = ()


MODEL_CATALOG: dict[str, ModelSpec] = {
    "gemini-embedding-001": ModelSpec(
        name="gemini-embedding-001",
        provider="vertex",
        default_dimensions=768,
        supported_dimensions=(128, 256, 512, 768, 1536, 3072),
        max_tokens=2048,
        quantizations=("fp32", "int8", "binary"),
    ),
    "gemini-embedding-2": ModelSpec(
        name="gemini-embedding-2",
        provider="gemini",
        default_dimensions=768,
        supported_dimensions=(128, 256, 512, 768, 1536, 3072),
        max_tokens=8192,
        quantizations=("fp32", "int8", "binary"),
    ),
    "text-embedding-005": ModelSpec(
        name="text-embedding-005",
        provider="vertex",
        default_dimensions=768,
        supported_dimensions=(128, 256, 512, 768),
        max_tokens=2048,
        quantizations=("fp32", "int8", "binary"),
    ),
    "embeddinggemma-2-270m": ModelSpec(
        name="embeddinggemma-2-270m",
        provider="gemma",
        default_dimensions=768,
        supported_dimensions=(128, 256, 512, 768),
        max_tokens=8192,
        quantizations=("int8", "q4", "bf16", "fp32"),
        mirror_url="https://huggingface.co/onnx-community/embeddinggemma-2-270m-ONNX/resolve/main",
        forbid_fp16=True,
        task_prefixes=True,
        default_files=("tokenizer.json", "onnx/model_quantized.onnx"),
    ),
    "embeddinggemma-300m": ModelSpec(
        name="embeddinggemma-300m",
        provider="gemma",
        default_dimensions=768,
        supported_dimensions=(128, 256, 512, 768),
        max_tokens=2048,
        quantizations=("int8", "q4", "fp32"),
        mirror_url="https://huggingface.co/onnx-community/embeddinggemma-300m-ONNX/resolve/main",
        forbid_fp16=False,
        task_prefixes=True,
        default_files=("tokenizer.json", "onnx/model_quantized.onnx"),
    ),
    "BAAI/bge-small-en-v1.5": ModelSpec(
        name="BAAI/bge-small-en-v1.5",
        provider="fastembed",
        default_dimensions=384,
        supported_dimensions=(128, 256, 384),
        max_tokens=512,
        quantizations=("fp32", "int8"),
        mirror_url="https://storage.googleapis.com/qdrant-fastembed/fast-bge-small-en-v1.5.tar.gz",
        default_files=("tokenizer.json", "model_optimized.onnx"),
    ),
    "nomic-ai/nomic-embed-text-v1.5": ModelSpec(
        name="nomic-ai/nomic-embed-text-v1.5",
        provider="fastembed",
        default_dimensions=768,
        supported_dimensions=(64, 128, 256, 512, 768),
        max_tokens=8192,
        quantizations=("fp32", "int8"),
        mirror_url="https://storage.googleapis.com/qdrant-fastembed/fast-nomic-embed-text-v1.5.tar.gz",
        default_files=("tokenizer.json", "model_quantized.onnx"),
    ),
    "minishlab/potion-base-8M": ModelSpec(
        name="minishlab/potion-base-8M",
        provider="static",
        default_dimensions=256,
        supported_dimensions=(64, 128, 256),
        max_tokens=8192,
        quantizations=("fp32", "int8"),
        mirror_url="https://huggingface.co/minishlab/potion-base-8M/resolve/main",
        default_files=("tokenizer.json", "model.safetensors"),
    ),
}


def get_model_spec(name: str) -> ModelSpec | None:
    """Look up a `ModelSpec` by canonical model name."""
    return MODEL_CATALOG.get(name)


def list_model_specs(provider: str | None = None) -> list[ModelSpec]:
    """Return all registered `ModelSpec` definitions, optionally filtered by provider."""
    if provider is None:
        return list(MODEL_CATALOG.values())
    norm = provider.lower()
    return [spec for spec in MODEL_CATALOG.values() if spec.provider.lower() == norm]
