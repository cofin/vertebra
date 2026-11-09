//! Integration tests for `TypeCatalog`-resolved `vector`/`halfvec` and built-in `BIT`/`VARBIT` codecs (`ch12:t01`).

extern crate _postgres as vertebra_postgres;

use bytes::BytesMut;
use vertebra_postgres::codecs::{
    BIT_OID, CodecContext, VARBIT_OID, decode_value_with_catalog, encode_value_with_catalog,
    format_for_oid_with_catalog,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Format, PgValue, VectorKind};
use vertebra_postgres::types::{TypeCatalog, TypeInfo, TypeKind};

#[test]
fn dynamic_vector_halfvec_vector_array_and_wrapper_mismatch() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();

    catalog.insert_custom_type(TypeInfo {
        oid: 42001,
        schema: "public".to_string(),
        name: "vector".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "vector".to_string(),
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 42002,
        schema: "public".to_string(),
        name: "halfvec".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "halfvec".to_string(),
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 42003,
        schema: "public".to_string(),
        name: "_vector".to_string(),
        kind: TypeKind::Array {
            element_oid: 42001,
            delimiter: b',',
        },
        delimiter: b',',
    });

    assert_eq!(format_for_oid_with_catalog(42001, &catalog), Format::Binary);
    assert_eq!(format_for_oid_with_catalog(42002, &catalog), Format::Binary);
    assert_eq!(format_for_oid_with_catalog(42003, &catalog), Format::Binary);
    assert_eq!(format_for_oid_with_catalog(91000, &catalog), Format::Text);

    let vec_val = PgValue::Vector {
        values: vec![1.25, -0.0, 3.5],
        kind: VectorKind::Vector,
    };
    let mut vec_bin = BytesMut::new();
    encode_value_with_catalog(
        &vec_val,
        42001,
        Format::Binary,
        &mut vec_bin,
        &ctx,
        &catalog,
    )
    .unwrap();
    let vec_dec =
        decode_value_with_catalog(42001, Format::Binary, Some(&vec_bin), &ctx, &catalog).unwrap();
    assert_eq!(vec_dec, vec_val);

    let mut vec_txt = BytesMut::new();
    encode_value_with_catalog(&vec_val, 42001, Format::Text, &mut vec_txt, &ctx, &catalog).unwrap();
    assert_eq!(
        decode_value_with_catalog(42001, Format::Text, Some(&vec_txt), &ctx, &catalog).unwrap(),
        vec_val
    );

    let half_val = PgValue::Vector {
        values: vec![1.0, -0.5, 65504.0],
        kind: VectorKind::HalfVec,
    };
    let mut half_bin = BytesMut::new();
    encode_value_with_catalog(
        &half_val,
        42002,
        Format::Binary,
        &mut half_bin,
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(
        decode_value_with_catalog(42002, Format::Binary, Some(&half_bin), &ctx, &catalog).unwrap(),
        half_val
    );

    let mut mismatch_buf = BytesMut::new();
    assert!(matches!(
        encode_value_with_catalog(
            &half_val,
            42001,
            Format::Binary,
            &mut mismatch_buf,
            &ctx,
            &catalog
        ),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        encode_value_with_catalog(
            &vec_val,
            42002,
            Format::Binary,
            &mut mismatch_buf,
            &ctx,
            &catalog
        ),
        Err(PostgresError::Data(_))
    ));

    let vec_arr = PgValue::Array {
        values: vec![vec_val, PgValue::Null],
        dimensions: vec![2],
        lower_bounds: vec![1],
        element_oid: 42001,
    };
    let mut arr_bin = BytesMut::new();
    encode_value_with_catalog(
        &vec_arr,
        42003,
        Format::Binary,
        &mut arr_bin,
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(
        decode_value_with_catalog(42003, Format::Binary, Some(&arr_bin), &ctx, &catalog).unwrap(),
        vec_arr
    );

    assert_eq!(
        decode_value_with_catalog(91000, Format::Text, Some(b"[1,2]"), &ctx, &catalog).unwrap(),
        PgValue::Text("[1,2]".to_string())
    );
}

#[test]
fn bit_and_varbit_binary_and_text_roundtrip_and_padding_validation() {
    let catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();

    for oid in [BIT_OID, VARBIT_OID] {
        assert_eq!(format_for_oid_with_catalog(oid, &catalog), Format::Binary);
    }

    let zero_bits = PgValue::BitString {
        data: vec![],
        bit_length: 0,
    };
    let mut zero_bin = BytesMut::new();
    encode_value_with_catalog(
        &zero_bits,
        VARBIT_OID,
        Format::Binary,
        &mut zero_bin,
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(&zero_bin[..], &[0, 0, 0, 0]);
    assert_eq!(
        decode_value_with_catalog(VARBIT_OID, Format::Binary, Some(&zero_bin), &ctx, &catalog)
            .unwrap(),
        zero_bits
    );
    assert_eq!(
        decode_value_with_catalog(VARBIT_OID, Format::Text, Some(b""), &ctx, &catalog).unwrap(),
        zero_bits
    );

    let three_bits = PgValue::BitString {
        data: vec![0xa0],
        bit_length: 3,
    };
    let mut three_bin = BytesMut::new();
    encode_value_with_catalog(
        &three_bits,
        BIT_OID,
        Format::Binary,
        &mut three_bin,
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(
        decode_value_with_catalog(BIT_OID, Format::Binary, Some(&three_bin), &ctx, &catalog)
            .unwrap(),
        three_bits
    );
    let mut three_txt = BytesMut::new();
    encode_value_with_catalog(
        &three_bits,
        BIT_OID,
        Format::Text,
        &mut three_txt,
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(&three_txt[..], b"101");
    assert_eq!(
        decode_value_with_catalog(BIT_OID, Format::Text, Some(&three_txt), &ctx, &catalog).unwrap(),
        three_bits
    );

    let big_bits = PgValue::BitString {
        data: vec![0xaa; 2500],
        bit_length: 20_000,
    };
    let mut big_bin = BytesMut::new();
    encode_value_with_catalog(
        &big_bits,
        VARBIT_OID,
        Format::Binary,
        &mut big_bin,
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(
        decode_value_with_catalog(VARBIT_OID, Format::Binary, Some(&big_bin), &ctx, &catalog)
            .unwrap(),
        big_bits
    );

    let bad_pad = PgValue::BitString {
        data: vec![0xa8],
        bit_length: 3,
    };
    let mut bad_out = BytesMut::new();
    assert!(matches!(
        encode_value_with_catalog(
            &bad_pad,
            BIT_OID,
            Format::Binary,
            &mut bad_out,
            &ctx,
            &catalog
        ),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        decode_value_with_catalog(
            BIT_OID,
            Format::Binary,
            Some(&[0, 0, 0, 3, 0xa8]),
            &ctx,
            &catalog
        ),
        Err(PostgresError::Data(_))
    ));
}
