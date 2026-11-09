//! Public-seam behavioral tests for incremental PostgreSQL binary `COPY` framing
//! (`BinaryCopyDecoder`, `BinaryCopyEncoder`) across arbitrary split boundaries (`ch18:t01`).

extern crate _postgres as vertebra_postgres;

use bytes::{BufMut, Bytes, BytesMut};
use vertebra_postgres::codecs::numeric::decode_numeric_text;
use vertebra_postgres::codecs::{
    BYTEA_OID, CodecContext, INT2_OID, INT4_OID, INT8_OID, NUMERIC_OID, TEXT_OID, TIMESTAMPTZ_OID,
    VARBIT_OID, decode_value_with_catalog,
};
use vertebra_postgres::copy::{
    BinaryCopyDecoder, BinaryCopyEncoder, DEFAULT_MAX_COPY_EXTENSION_BYTES,
    DEFAULT_MAX_COPY_FIELD_BYTES, DEFAULT_MAX_COPY_ROW_BYTES, PGCOPY_SIGNATURE,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Format, PgValue, TemporalKind, VectorKind};
use vertebra_postgres::types::{TypeCatalog, TypeInfo, TypeKind};

const INT4_ARRAY_OID: u32 = 1007;

fn build_header(flags: u32, ext_len: u32) -> BytesMut {
    let mut b = BytesMut::with_capacity(19);
    b.extend_from_slice(&PGCOPY_SIGNATURE);
    b.put_u32(flags);
    b.put_u32(ext_len);
    b
}

#[test]
fn copy_codec_zero_rows_and_zero_column_tuples() {
    let mut enc = BinaryCopyEncoder::new(3).expect("valid encoder");
    let mut empty_stream = BytesMut::new();
    enc.finish_trailer(&mut empty_stream)
        .expect("0-row trailer writes header + -1i16");
    assert_eq!(empty_stream.len(), 21);
    assert_eq!(&empty_stream[..11], &PGCOPY_SIGNATURE);
    assert_eq!(&empty_stream[19..21], &[0xFF, 0xFF]);
    assert!(matches!(
        enc.finish_trailer(&mut empty_stream),
        Err(PostgresError::Data(_))
    ));

    let mut dec = BinaryCopyDecoder::new(3);
    dec.push(&empty_stream).expect("push 0-row stream");
    assert_eq!(dec.next_row().expect("next_row"), None);
    assert_eq!(dec.finish().expect("finish"), 0);

    let mut zenc = BinaryCopyEncoder::new(0).expect("0-col encoder");
    let mut zero_col_stream = BytesMut::new();
    zenc.encode_raw_row(&[], &mut zero_col_stream)
        .expect("row 1");
    zenc.encode_raw_row(&[], &mut zero_col_stream)
        .expect("row 2");
    zenc.finish_trailer(&mut zero_col_stream).expect("trailer");

    for split in 0..=zero_col_stream.len() {
        let mut zdec = BinaryCopyDecoder::new(0);
        let mut seen = Vec::new();
        zdec.push(&zero_col_stream[..split]).expect("chunk 1");
        while let Some(r) = zdec.next_row().expect("drain 1") {
            seen.push(r);
        }
        zdec.push(&zero_col_stream[split..]).expect("chunk 2");
        while let Some(r) = zdec.next_row().expect("drain 2") {
            seen.push(r);
        }
        assert_eq!(seen, vec![Vec::<Option<Bytes>>::new(), Vec::new()]);
        assert_eq!(zdec.finish().expect("finish 2 rows"), 2);
    }
}

#[test]
fn copy_codec_null_empty_scalar_and_container_widths_at_every_split_boundary() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let vec_oid = 91_001_u32;
    catalog.insert_custom_type(TypeInfo {
        oid: vec_oid,
        schema: "public".to_string(),
        name: "vector".to_string(),
        delimiter: b',',
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "vector".to_string(),
        },
    });
    let ctx = CodecContext::default();

    let column_oids = [
        TEXT_OID,
        BYTEA_OID,
        INT2_OID,
        INT4_OID,
        INT8_OID,
        NUMERIC_OID,
        INT4_ARRAY_OID,
        vec_oid,
        VARBIT_OID,
    ];
    let numeric_val = decode_numeric_text("1.2300").expect("valid numeric");
    let array_val = PgValue::Array {
        values: vec![PgValue::Int4(1), PgValue::Null],
        element_oid: INT4_OID,
        dimensions: vec![2],
        lower_bounds: vec![0],
    };
    let row = vec![
        PgValue::Null,
        PgValue::Bytes(Vec::new()),
        PgValue::Int2(-7),
        PgValue::Int4(42),
        PgValue::Int8(-9_000_000_000),
        PgValue::Numeric(numeric_val.clone()),
        array_val.clone(),
        PgValue::Vector {
            values: vec![1.5, -0.25],
            kind: VectorKind::Vector,
        },
        PgValue::BitString {
            data: vec![0b1011_0000],
            bit_length: 4,
        },
    ];

    let mut enc = BinaryCopyEncoder::new(column_oids.len()).expect("encoder");
    let mut wire = BytesMut::new();
    enc.encode_row_values(&column_oids, &row, &ctx, &catalog, &mut wire)
        .expect("encode row");
    enc.finish_trailer(&mut wire).expect("trailer");

    let assert_exact_row = |decoded: &[Option<Bytes>]| {
        assert_eq!(decoded.len(), column_oids.len());
        assert_eq!(decoded[0], None);
        assert_eq!(decoded[1], Some(Bytes::new()));
        assert_eq!(decoded[2].as_deref(), Some([0xFF_u8, 0xF9].as_slice()));
        assert_eq!(
            decoded[3].as_deref(),
            Some([0x00_u8, 0x00, 0x00, 0x2A].as_slice())
        );
        assert_eq!(
            decoded[4].as_deref(),
            Some((-9_000_000_000_i64).to_be_bytes().as_slice())
        );
        let num_bytes = decoded[5].as_ref().expect("numeric bytes");
        assert_eq!(num_bytes.len(), 12);
        assert_eq!(
            decode_value_with_catalog(NUMERIC_OID, Format::Binary, Some(num_bytes), &ctx, &catalog)
                .expect("decode numeric"),
            PgValue::Numeric(numeric_val.clone())
        );

        let arr_bytes = decoded[6].as_ref().expect("array bytes");
        assert_eq!(arr_bytes.len(), 32);
        assert_eq!(
            decode_value_with_catalog(
                INT4_ARRAY_OID,
                Format::Binary,
                Some(arr_bytes),
                &ctx,
                &catalog
            )
            .expect("decode array"),
            array_val
        );

        let vec_bytes = decoded[7].as_ref().expect("vector bytes");
        assert_eq!(vec_bytes.len(), 12);
        let bit_bytes = decoded[8].as_ref().expect("varbit bytes");
        assert_eq!(bit_bytes.len(), 5);
    };

    for split in 0..=wire.len() {
        let mut dec = BinaryCopyDecoder::new(column_oids.len());
        let mut rows = Vec::new();
        dec.push(&wire[..split]).expect("split part 1");
        while let Some(r) = dec.next_row().expect("next_row part 1") {
            rows.push(r);
        }
        dec.push(&wire[split..]).expect("split part 2");
        while let Some(r) = dec.next_row().expect("next_row part 2") {
            rows.push(r);
        }
        assert_eq!(rows.len(), 1, "split={split}");
        assert_exact_row(&rows[0]);
        assert_eq!(dec.finish().expect("finish at split"), 1);
    }

    let mut byte_by_byte = BinaryCopyDecoder::new(column_oids.len());
    let mut collected = Vec::new();
    for &b in wire.iter() {
        byte_by_byte
            .push(std::slice::from_ref(&b))
            .expect("1-byte push");
        if let Some(r) = byte_by_byte.next_row().expect("1-byte next_row") {
            collected.push(r);
        }
    }
    assert_eq!(collected.len(), 1);
    assert_exact_row(&collected[0]);
    assert_eq!(byte_by_byte.finish().expect("1-byte finish"), 1);

    let mut single_pull = BinaryCopyDecoder::new(column_oids.len());
    single_pull.push(&wire).expect("push whole");
    let first = single_pull
        .next_row()
        .expect("next_row")
        .expect("1 row present");
    assert_exact_row(&first);
    assert_eq!(
        single_pull
            .finish()
            .expect("finish consumes buffered -1 trailer"),
        1
    );
}

#[test]
fn copy_decoder_accepts_reserved_low_flags_and_extension_but_rejects_critical_and_oid_flags() {
    let mut low_flags = build_header(0x0000_0005, 12);
    low_flags.extend_from_slice(&[0xAB; 12]);
    low_flags.put_i16(1);
    low_flags.put_i32(4);
    low_flags.put_i32(99);
    low_flags.put_i16(-1);

    let mut dec = BinaryCopyDecoder::new(1);
    dec.push(&low_flags).expect("reserved low flags succeed");
    let r = dec.next_row().expect("row ok").expect("row present");
    assert_eq!(r, vec![Some(Bytes::copy_from_slice(&99_i32.to_be_bytes()))]);
    assert_eq!(dec.finish().expect("finish ok"), 1);

    for bad_flags in [0x0001_0000_u32, 0x8000_0000_u32, 0x0002_0005_u32] {
        let mut frame = build_header(bad_flags, 0);
        frame.put_i16(-1);
        let mut bad_dec = BinaryCopyDecoder::new(1);
        let err = bad_dec
            .push(&frame)
            .expect_err("critical/OID flag must fail");
        assert!(
            matches!(err, PostgresError::Data(_)),
            "flags=0x{bad_flags:08x}: {err:?}"
        );
    }

    let mut bad_sig = BytesMut::from(&b"PGCOPY\n\x00\r\n\0"[..]);
    bad_sig.put_u32(0);
    bad_sig.put_u32(0);
    bad_sig.put_i16(-1);
    let mut sig_dec = BinaryCopyDecoder::new(0);
    assert!(matches!(
        sig_dec.push(&bad_sig),
        Err(PostgresError::Data(_))
    ));
}

#[test]
fn copy_codec_enforces_extension_field_and_row_byte_ceilings_before_allocation() {
    let mut over_ext = build_header(0, (DEFAULT_MAX_COPY_EXTENSION_BYTES as u32) + 1);
    over_ext.put_i16(-1);
    let mut dec_ext = BinaryCopyDecoder::new(1);
    assert!(matches!(
        dec_ext.push(&over_ext),
        Err(PostgresError::Data(_))
    ));

    let neg_ext = build_header(0, 0xFFFF_FFFF);
    let mut dec_neg_ext = BinaryCopyDecoder::new(1);
    assert!(matches!(
        dec_neg_ext.push(&neg_ext),
        Err(PostgresError::Data(_))
    ));

    let mut over_field = build_header(0, 0);
    over_field.put_i16(1);
    over_field.put_i32((DEFAULT_MAX_COPY_FIELD_BYTES as i32) + 1);
    let mut dec_field = BinaryCopyDecoder::new(1);
    assert!(matches!(
        dec_field.push(&over_field),
        Err(PostgresError::Data(_))
    ));

    let nine_mib = 9 * 1024 * 1024;
    let payload_9m = vec![0x42_u8; nine_mib];
    let mut over_row = build_header(0, 0);
    over_row.put_i16(2);
    over_row.put_i32(nine_mib as i32);
    over_row.extend_from_slice(&payload_9m);
    over_row.put_i32(nine_mib as i32);

    assert!(2 * nine_mib > DEFAULT_MAX_COPY_ROW_BYTES);
    let mut dec_row = BinaryCopyDecoder::new(2);
    assert!(matches!(
        dec_row.push(&over_row),
        Err(PostgresError::Data(_))
    ));

    let mut enc_cap = BinaryCopyEncoder::with_limits(2, 16, 10).expect("bounded encoder");
    let mut out = BytesMut::new();
    assert!(matches!(
        enc_cap.encode_raw_row(&[Some(&[0u8; 11]), Some(&[])], &mut out),
        Err(PostgresError::Data(_))
    ));
    assert!(out.is_empty());
    assert!(matches!(
        enc_cap.encode_raw_row(&[Some(&[0u8; 9]), Some(&[0u8; 9])], &mut out),
        Err(PostgresError::Data(_))
    ));
    assert!(out.is_empty());
}

#[test]
fn copy_decoder_rejects_arity_mismatch_truncation_and_trailing_bytes() {
    let mut bad_arity = build_header(0, 0);
    bad_arity.put_i16(2);
    bad_arity.put_i32(-1);
    bad_arity.put_i32(-1);
    bad_arity.put_i16(-1);
    let mut dec_arity = BinaryCopyDecoder::new(3);
    assert!(matches!(
        dec_arity.push(&bad_arity),
        Err(PostgresError::Data(_))
    ));

    let mut neg_arity = build_header(0, 0);
    neg_arity.put_i16(-2);
    let mut dec_neg = BinaryCopyDecoder::new(1);
    assert!(matches!(
        dec_neg.push(&neg_arity),
        Err(PostgresError::Data(_))
    ));

    let mut neg_flen = build_header(0, 0);
    neg_flen.put_i16(1);
    neg_flen.put_i32(-2);
    let mut dec_nflen = BinaryCopyDecoder::new(1);
    assert!(matches!(
        dec_nflen.push(&neg_flen),
        Err(PostgresError::Data(_))
    ));

    let mut dec_trunc_hdr = BinaryCopyDecoder::new(1);
    dec_trunc_hdr
        .push(&PGCOPY_SIGNATURE[..7])
        .expect("partial sig");
    assert!(matches!(
        dec_trunc_hdr.finish(),
        Err(PostgresError::Data(_))
    ));

    let mut trunc_field = build_header(0, 0);
    trunc_field.put_i16(1);
    trunc_field.put_i32(4);
    trunc_field.extend_from_slice(&[1, 2]);
    let mut dec_trunc_field = BinaryCopyDecoder::new(1);
    dec_trunc_field.push(&trunc_field).expect("partial field");
    assert_eq!(dec_trunc_field.next_row().expect("incomplete"), None);
    assert!(matches!(
        dec_trunc_field.finish(),
        Err(PostgresError::Data(_))
    ));

    let mut missing_trailer = build_header(0, 0);
    missing_trailer.put_i16(1);
    missing_trailer.put_i32(-1);
    let mut dec_no_trailer = BinaryCopyDecoder::new(1);
    dec_no_trailer.push(&missing_trailer).expect("push 1 row");
    assert_eq!(dec_no_trailer.next_row().expect("row"), Some(vec![None]));
    assert!(matches!(
        dec_no_trailer.finish(),
        Err(PostgresError::Data(_))
    ));

    let mut trailing_same_chunk = build_header(0, 0);
    trailing_same_chunk.put_i16(-1);
    trailing_same_chunk.put_u8(0);
    let mut dec_trail1 = BinaryCopyDecoder::new(1);
    assert!(matches!(
        dec_trail1.push(&trailing_same_chunk),
        Err(PostgresError::Data(_))
    ));

    let mut valid_with_row = build_header(0, 0);
    valid_with_row.put_i16(1);
    valid_with_row.put_i32(-1);
    valid_with_row.put_i16(-1);
    let mut dec_trail2 = BinaryCopyDecoder::new(1);
    dec_trail2.push(&valid_with_row).expect("push");
    assert_eq!(dec_trail2.next_row().expect("row"), Some(vec![None]));
    assert_eq!(dec_trail2.next_row().expect("trailer"), None);
    assert!(matches!(
        dec_trail2.push(&[0x00]),
        Err(PostgresError::Data(_))
    ));
}

#[test]
fn copy_encoder_rejects_unresolved_temporal_text_before_mutating_output() {
    let catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();
    let mut enc = BinaryCopyEncoder::new(2).expect("encoder");
    let mut out = BytesMut::new();

    let bad_row = vec![
        PgValue::Int4(7),
        PgValue::TemporalText {
            wire_text: "2024-11-03 01:30:00 CST".to_string(),
            kind: TemporalKind::TimestampTz,
            date_style: "ISO, MDY".to_string(),
            time_zone: "America/Chicago".to_string(),
        },
    ];
    let err = enc
        .encode_row_values(
            &[INT4_OID, TIMESTAMPTZ_OID],
            &bad_row,
            &ctx,
            &catalog,
            &mut out,
        )
        .expect_err("TemporalText must fail binary COPY encode");
    assert!(matches!(err, PostgresError::Data(_)));
    assert!(
        out.is_empty(),
        "failed first row must not write header or partial field bytes"
    );

    let ok_row = vec![PgValue::Int4(7), PgValue::TimestampTz(0)];
    enc.encode_row_values(
        &[INT4_OID, TIMESTAMPTZ_OID],
        &ok_row,
        &ctx,
        &catalog,
        &mut out,
    )
    .expect("resolved TimestampTz succeeds");
    let snapshot_len = out.len();
    assert!(snapshot_len > 19);

    let err2 = enc
        .encode_row_values(
            &[INT4_OID, TIMESTAMPTZ_OID],
            &bad_row,
            &ctx,
            &catalog,
            &mut out,
        )
        .expect_err("second row TemporalText must leave out untouched");
    assert!(matches!(err2, PostgresError::Data(_)));
    assert_eq!(out.len(), snapshot_len);
}
