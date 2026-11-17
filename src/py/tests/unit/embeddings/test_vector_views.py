"""Unit tests for shared-memory VectorSlab, VectorCache, SIMD kernels, and PEP 3118 VectorView/VectorBatchView."""

from __future__ import annotations

import math

import pytest

from vertebra._vertebra import (
    SimilarityMatch,
    StaticEmbedderCore,
    VectorBatchView,
    VectorCache,
    VectorView,
    format_gemma_prompt,
    splice_prefix_tokens,
)


def test_vector_view_1d_pep3118_zero_copy_and_l2_normalization() -> None:
    """Verify VectorView exposes a 1D read-only float32 PEP 3118 memoryview and L2-normalizes in place."""
    raw = [float(i + 1) for i in range(768)]
    vec = VectorView.from_f32(raw, normalize=True)
    assert vec.dimensions == 768
    assert vec.dtype == "float32"
    assert len(vec) == 768
    assert vec.nbytes == 768 * 4
    assert math.isclose(vec.norm(), 1.0, rel_tol=1e-5, abs_tol=1e-5)

    mv = memoryview(vec)
    assert mv.ndim == 1
    assert mv.shape == (768,)
    assert mv.strides == (4,)
    assert mv.format == "f"
    assert mv.readonly is True
    assert mv.nbytes == 768 * 4

    with pytest.raises(TypeError):
        mv[0] = 1


def test_vector_batch_view_2d_pep3118_and_row_slicing() -> None:
    """Verify VectorBatchView exposes a 2D read-only float32 PEP 3118 buffer and row indexing."""
    rows = [[float((r + 1) * (c + 1)) for c in range(768)] for r in range(16)]
    batch = VectorBatchView.from_f32_rows(rows, normalize=True)
    assert batch.count == 16
    assert batch.dimensions == 768
    assert batch.dtype == "float32"
    assert len(batch) == 16

    mv = memoryview(batch)
    assert mv.ndim == 2
    assert mv.shape == (16, 768)
    assert mv.format == "f"
    assert mv.readonly is True

    row_0 = batch[0]
    assert isinstance(row_0, VectorView)
    assert row_0.dimensions == 768
    assert math.isclose(row_0.norm(), 1.0, rel_tol=1e-5, abs_tol=1e-5)

    row_last = batch[-1]
    assert isinstance(row_last, VectorView)
    assert row_last.dimensions == 768

    with pytest.raises(IndexError):
        _ = batch[16]


def test_mrl_truncation_and_int8_binary_quantization() -> None:
    """Verify int8 and binary quantization preserve expected dtype, shape, and byte counts."""
    raw = [math.sin(i * 0.1) for i in range(768)]
    vec = VectorView.from_f32(raw, normalize=True)

    q_i8 = vec.quantize_int8()
    assert q_i8.dtype == "int8"
    assert q_i8.dimensions == 768
    mv_i8 = memoryview(q_i8)
    assert mv_i8.format == "b"
    assert mv_i8.shape == (768,)
    assert mv_i8.nbytes == 768

    q_bin = vec.quantize_binary()
    assert q_bin.dtype == "binary"
    assert q_bin.dimensions == 768
    assert q_bin.nbytes == 96
    mv_bin = memoryview(q_bin)
    assert mv_bin.format == "B"
    assert mv_bin.shape == (96,)
    assert mv_bin.nbytes == 96


@pytest.mark.parametrize(
    "target_dim",
    [512, 256, 128],
    ids=["dim-512", "dim-256", "dim-128"],
)
def test_mrl_truncation_renormalizes_to_unit_norm(target_dim: int) -> None:
    """Verify MRL truncation (768 -> 512/256/128) renormalizes to unit norm."""
    raw = [math.sin(i * 0.1) for i in range(768)]
    vec = VectorView.from_f32(raw, normalize=True)

    truncated = vec.truncate(target_dim)
    assert truncated.dimensions == target_dim
    assert math.isclose(truncated.norm(), 1.0, rel_tol=1e-5, abs_tol=1e-5)
    mv = memoryview(truncated)
    assert mv.shape == (target_dim,)


def test_batch_mrl_quantization_and_top_k_similarity() -> None:
    """Verify VectorBatchView MRL truncation, quantization, and SIMD top_k search."""
    rows = [
        [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [-1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    ]
    batch = VectorBatchView.from_f32_rows(rows, normalize=True)
    query = VectorView.from_f32([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], normalize=True)

    matches = batch.top_k(query, k=2, metric="cosine")
    assert len(matches) == 2
    assert isinstance(matches[0], SimilarityMatch)
    assert matches[0].index == 0
    assert math.isclose(matches[0].score, 1.0, rel_tol=1e-5, abs_tol=1e-5)
    assert matches[1].index == 1
    assert matches[1].score > 0.95

    bin_batch = batch.quantize_binary()
    bin_query = query.quantize_binary()
    assert memoryview(bin_batch).shape == (4, 1)
    assert bin_query.hamming_distance(bin_batch[0]) == 0
    assert bin_query.hamming_distance(bin_batch[3]) >= 1


def test_shm_named_vector_batch_roundtrip() -> None:
    """Verify VectorBatchView can be created in a named ShmRegion and reopened zero-copy."""
    shm_name = "/vtb-test-vec-slab-t02"
    rows = [[float(r * 4 + c + 1) for c in range(16)] for r in range(4)]
    writer = VectorBatchView.create_shm(shm_name, rows, normalize=True)
    assert writer.shm_name == shm_name

    reader = VectorBatchView.open_shm(shm_name)
    assert reader.count == 4
    assert reader.dimensions == 16
    assert reader.dtype == "float32"
    assert reader[2].to_list() == pytest.approx(writer[2].to_list(), rel=1e-6)


def test_seqlock_vector_cache_and_static_embedder_and_gemma_splicer() -> None:
    """Verify VectorCache seqlock hit/miss, Gemma prompt formatter/token splicer, and StaticEmbedderCore."""
    cache = VectorCache(capacity=64, dimensions=8)
    v0 = VectorView.from_f32([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], normalize=True)
    v1 = VectorView.from_f32([0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], normalize=True)

    assert cache.lookup("gemini-embedding-001", "RETRIEVAL_QUERY", "hello world") is None
    cache.insert("gemini-embedding-001", "RETRIEVAL_QUERY", "hello world", v0)
    cache.insert("gemini-embedding-001", "RETRIEVAL_QUERY", "other text", v1)

    hit = cache.lookup("gemini-embedding-001", "RETRIEVAL_QUERY", "hello world")
    assert hit is not None
    assert hit.to_list() == pytest.approx(v0.to_list(), rel=1e-6)

    top = cache.similarity_search(v0, k=1, metric="cosine")
    assert len(top) == 1
    assert math.isclose(top[0].score, 1.0, rel_tol=1e-5, abs_tol=1e-5)

    prompt_q = format_gemma_prompt("search_query", "how does futex work?", None)
    assert prompt_q == "task: search result | query: how does futex work?"
    prompt_d = format_gemma_prompt("search_document", "Futex wakes sleepers.", "IPC Guide")
    assert prompt_d == "title: IPC Guide | text: Futex wakes sleepers."

    spliced = splice_prefix_tokens([10, 11, 12], [20, 21, 22, 23], eos_id=2, max_seq_len=6)
    assert spliced == [10, 11, 12, 20, 21, 2]

    static_emb = StaticEmbedderCore(
        vocab={"futex": 0, "shm": 1, "ring": 2},
        weights=[
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0],
        ],
    )
    out_vec = static_emb.embed_text("futex shm", dimensions=4)
    assert out_vec.dimensions == 4
    assert math.isclose(out_vec.norm(), 1.0, rel_tol=1e-5, abs_tol=1e-5)
    out_batch = static_emb.embed_batch(["futex", "shm ring"], dimensions=4)
    assert out_batch.count == 2
    assert out_batch.dimensions == 4
