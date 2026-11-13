"""Unit tests for binary pgvector wire codecs, zero-copy PyBuffer Oracle 26ai 0xDB, to_arrow_shm, and SQLAlchemy Vector pushdown."""

from __future__ import annotations

import array
import struct

import pytest

from vertebra import _vertebra as native_vtb
from vertebra._vertebra import (
    ArrowCursor,
    VectorBatchView,
    VectorView,
    decode_gemini_embed_grpc_frame_to_batch,
    decode_oracle_vector_f32,
    decode_pgvector_bit,
    decode_pgvector_f16,
    decode_pgvector_f32,
    encode_gemini_embed_grpc_frame,
    encode_gemini_embed_grpc_response_frame,
    encode_oracle_vector_f32,
    encode_oracle_vector_i8,
    encode_pgvector_bit,
    encode_pgvector_f16,
    encode_pgvector_f32,
)
from vertebra.db.arrow import read_arrow_shm
from vertebra.extensions.sqlalchemy.types import Vector
from vertebra.utils.serializers import _zero_copy


def test_pgvector_f32_f16_and_bit_wire_roundtrip() -> None:
    """Verify binary pgvector vector, halfvec, and bit wire codecs roundtrip and reject NaN/Inf."""
    vals = [0.25, -0.5, 1.0, -2.0]
    raw_f32 = encode_pgvector_f32(vals)
    assert len(raw_f32) == 4 + 4 * 4
    dim, unused = struct.unpack(">HH", raw_f32[:4])
    assert dim == 4
    assert unused == 0
    assert decode_pgvector_f32(raw_f32) == pytest.approx(vals, rel=1e-6)

    raw_f16 = encode_pgvector_f16(vals)
    assert len(raw_f16) == 4 + 4 * 2
    assert decode_pgvector_f16(raw_f16) == pytest.approx(vals, rel=1e-3)

    packed_bits = bytes([0b1010_0101, 0b1100_0011])
    raw_bit = encode_pgvector_bit(packed_bits, 16)
    assert len(raw_bit) == 4 + 2
    decoded_bits, bit_len = decode_pgvector_bit(raw_bit)
    assert decoded_bits == packed_bits
    assert bit_len == 16

    with pytest.raises(ValueError, match="NaN"):
        encode_pgvector_f32([1.0, float("nan")])


def test_zero_copy_pybuffer_oracle_and_pgvector_encoding() -> None:
    """Verify encode_oracle_vector_* and encode_pgvector_* accept VectorView, memoryview, and array.array."""
    vals = [0.1, 0.2, 0.3, 0.4, -0.5, 0.6, -0.7, 0.8]
    vec = VectorView.from_f32(vals, normalize=True)
    norm_vals = vec.to_list()

    ora_from_view = encode_oracle_vector_f32(vec)
    ora_from_mv = encode_oracle_vector_f32(memoryview(vec))
    ora_from_arr = encode_oracle_vector_f32(array.array("f", norm_vals))
    ora_from_method = vec.to_oracle_bytes()
    assert ora_from_view == ora_from_mv == ora_from_arr == ora_from_method
    assert decode_oracle_vector_f32(ora_from_view) == pytest.approx(norm_vals, rel=1e-6)

    pg_from_view = encode_pgvector_f32(vec)
    pg_from_mv = encode_pgvector_f32(memoryview(vec))
    pg_from_method = vec.to_pgvector_bytes()
    assert pg_from_view == pg_from_mv == pg_from_method
    assert decode_pgvector_f32(pg_from_view) == pytest.approx(norm_vals, rel=1e-6)

    sqlite_blob = vec.to_sqlite_blob()
    assert len(sqlite_blob) == 8 * 4
    unpacked = list(struct.unpack("<8f", sqlite_blob))
    assert unpacked == pytest.approx(norm_vals, rel=1e-6)

    vec_i8 = vec.quantize_int8()
    ora_i8 = encode_oracle_vector_i8(vec_i8)
    assert ora_i8[0] == 0xDB


def test_vector_batch_view_to_arrow_shm_roundtrip() -> None:
    """Verify VectorBatchView.to_arrow_shm frames FixedSizeList<Float32, D> into a VTBARROW ShmRegion cursor."""
    rows = [[float(r * 4 + c + 1) for c in range(8)] for r in range(4)]
    batch = VectorBatchView.from_f32_rows(rows, normalize=True)
    cursor = batch.to_arrow_shm(column_name="embedding")
    assert isinstance(cursor, ArrowCursor)
    try:
        payload = cursor.read_bytes()
        assert len(payload) > 0
        hydrated = read_arrow_shm(payload)
        assert hydrated.row_count == 4
        pylist = hydrated.to_pylist()
        assert len(pylist) == 4
        assert pylist[0]["embedding"] == pytest.approx(batch[0].to_list(), rel=1e-6)
    finally:
        cursor.release()


def test_sqlalchemy_vector_bind_and_result_pushdown() -> None:
    """Verify SQLAlchemy Vector column type binds VectorView and decodes binary pgvector/Oracle/SQLite results."""
    vec = VectorView.from_f32([0.6, 0.8, 0.0, 0.0], normalize=True)
    col_type = Vector(4)

    pg_text_bound = col_type.process_bind_param(vec, dialect_name="postgresql")
    assert isinstance(pg_text_bound, str)
    assert pg_text_bound.startswith("[")

    col_type_bin = Vector(4, binary_format=True)
    pg_bin_bound = col_type_bin.process_bind_param(vec, dialect_name="postgresql")
    assert isinstance(pg_bin_bound, bytes)
    assert pg_bin_bound == vec.to_pgvector_bytes()
    assert col_type_bin.process_result_value(pg_bin_bound, dialect_name="postgresql") == pytest.approx(
        [0.6, 0.8, 0.0, 0.0], rel=1e-6
    )

    ora_bound = col_type_bin.process_bind_param(vec, dialect_name="oracle")
    assert isinstance(ora_bound, bytes)
    assert ora_bound == vec.to_oracle_bytes()
    assert col_type_bin.process_result_value(ora_bound, dialect_name="oracle") == pytest.approx(
        [0.6, 0.8, 0.0, 0.0], rel=1e-6
    )

    sqlite_bound = col_type.process_bind_param(vec, dialect_name="sqlite")
    assert isinstance(sqlite_bound, bytes)
    assert sqlite_bound == vec.to_sqlite_blob()
    assert col_type.process_result_value(sqlite_bound, dialect_name="sqlite") == pytest.approx(
        [0.6, 0.8, 0.0, 0.0], rel=1e-6
    )


def test_gemini_grpc_proto_frame_roundtrip_and_malformed_varint_rejection() -> None:
    """Verify vertebra_core::proto varint/tag helpers roundtrip gRPC LPM embedding frames and reject overflow."""
    req_frame = encode_gemini_embed_grpc_frame(
        "gemini-embedding-001",
        ["alpha", "beta"],
        task="query",
        titles=[None, "Doc Beta"],
        output_dimensionality=256,
    )
    assert req_frame[0] == 0
    body_len = int.from_bytes(req_frame[1:5], "big")
    assert len(req_frame) == 5 + body_len

    src_batch = VectorBatchView.from_f32_rows([[3.0, 4.0, 0.0, 0.0], [0.0, 0.0, 5.0, 12.0]], normalize=False)
    resp_frame = encode_gemini_embed_grpc_response_frame(src_batch)
    decoded = decode_gemini_embed_grpc_frame_to_batch(resp_frame, dimensions=2, normalize=True)
    assert len(decoded) == 2
    assert decoded[0].to_list() == pytest.approx([0.6, 0.8], rel=1e-5)

    overflow_varint = bytes([0xFF] * 9 + [0x02])
    with pytest.raises(ValueError, match="overflow"):
        decode_gemini_embed_grpc_frame_to_batch(overflow_varint)


def test_zero_copy_binds_native_helpers_directly_without_python_fallbacks() -> None:
    """Verify _zero_copy binds its vectorcall and gRPC LPM helpers directly from vertebra._vertebra."""
    for helper_name in (
        "hydrate_one_vectorcall",
        "hydrate_rows_vectorcall",
        "instantiate_struct_vectorcall",
        "orm_to_struct_vectorcall",
        "pack_grpc_lpm_frame",
    ):
        assert getattr(_zero_copy, helper_name) is getattr(native_vtb, helper_name)

    for fallback_name in (
        "_py_hydrate_one_vectorcall",
        "_py_hydrate_rows_vectorcall",
        "_py_instantiate_struct_vectorcall",
        "_py_orm_to_struct_vectorcall",
        "_py_pack_grpc_lpm_frame",
    ):
        assert not hasattr(_zero_copy, fallback_name)
