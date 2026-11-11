//! Integration tests for PostgreSQL binary array codecs (`ch11:t02`).

extern crate _postgres as vertebra_postgres;

use bytes::{BufMut, BytesMut};
use vertebra_postgres::codecs::array::{
    decode_array_binary, encode_array_binary, normalize_convenience_array,
};
use vertebra_postgres::codecs::{
    CodecContext, decode_value_with_catalog, encode_value_with_catalog,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Format, PgNumeric, PgValue};
use vertebra_postgres::types::{TypeCatalog, TypeInfo, TypeKind};

#[test]
fn binary_array_0d_1d_2d_3d_negative_bounds_and_null_cells() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();

    let empty_uuid = PgValue::Array {
        values: vec![],
        dimensions: vec![],
        lower_bounds: vec![],
        element_oid: 2950,
    };
    let mut out = BytesMut::new();
    encode_array_binary(&empty_uuid, 2951, 2950, &mut out, &ctx, &catalog, 0).unwrap();
    assert_eq!(out.len(), 12);
    let dec_empty = decode_array_binary(2951, 2950, &out, &ctx, &catalog, 0).unwrap();
    assert_eq!(dec_empty, empty_uuid);

    assert_eq!(
        decode_value_with_catalog(1009, Format::Binary, None, &ctx, &catalog).unwrap(),
        PgValue::Null
    );
    let arr_with_null = PgValue::Array {
        values: vec![PgValue::Null],
        dimensions: vec![1],
        lower_bounds: vec![1],
        element_oid: 25,
    };
    let mut out_null = BytesMut::new();
    encode_value_with_catalog(
        &arr_with_null,
        1009,
        Format::Binary,
        &mut out_null,
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(
        decode_value_with_catalog(1009, Format::Binary, Some(&out_null), &ctx, &catalog).unwrap(),
        arr_with_null
    );

    let arr_2d = PgValue::Array {
        values: vec![
            PgValue::Int4(10),
            PgValue::Null,
            PgValue::Int4(30),
            PgValue::Int4(-40),
            PgValue::Int4(50),
            PgValue::Int4(60),
        ],
        dimensions: vec![2, 3],
        lower_bounds: vec![-1, 5],
        element_oid: 23,
    };
    let mut out_2d = BytesMut::new();
    encode_value_with_catalog(&arr_2d, 1007, Format::Binary, &mut out_2d, &ctx, &catalog).unwrap();
    let dec_2d =
        decode_value_with_catalog(1007, Format::Binary, Some(&out_2d), &ctx, &catalog).unwrap();
    assert_eq!(dec_2d, arr_2d);

    let arr_num = PgValue::Array {
        values: vec![PgValue::Numeric(PgNumeric::Finite {
            negative: false,
            digits: vec![1, 2500],
            weight: 0,
            dscale: 4,
        })],
        dimensions: vec![1],
        lower_bounds: vec![1],
        element_oid: 1700,
    };
    let mut out_num = BytesMut::new();
    encode_value_with_catalog(&arr_num, 1231, Format::Binary, &mut out_num, &ctx, &catalog)
        .unwrap();
    assert_eq!(
        decode_value_with_catalog(1231, Format::Binary, Some(&out_num), &ctx, &catalog).unwrap(),
        arr_num
    );

    catalog.insert_custom_type(TypeInfo {
        oid: 41010,
        schema: "public".to_string(),
        name: "pos_int".to_string(),
        kind: TypeKind::Domain { base_oid: 23 },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 41011,
        schema: "public".to_string(),
        name: "_pos_int".to_string(),
        kind: TypeKind::Array {
            element_oid: 41010,
            delimiter: b',',
        },
        delimiter: b',',
    });
    let dom_arr = PgValue::Array {
        values: vec![PgValue::Int4(7), PgValue::Int4(8)],
        dimensions: vec![2],
        lower_bounds: vec![1],
        element_oid: 41010,
    };
    let mut out_dom = BytesMut::new();
    encode_value_with_catalog(
        &dom_arr,
        41011,
        Format::Binary,
        &mut out_dom,
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(
        decode_value_with_catalog(41011, Format::Binary, Some(&out_dom), &ctx, &catalog).unwrap(),
        dom_arr
    );
}

#[test]
fn convenience_sequence_normalization_and_ragged_or_untyped_rejection() {
    let catalog = TypeCatalog::new_with_builtins();
    let rect = vec![
        PgValue::Array {
            values: vec![PgValue::Int4(1), PgValue::Int4(2)],
            dimensions: vec![2],
            lower_bounds: vec![1],
            element_oid: 23,
        },
        PgValue::Array {
            values: vec![PgValue::Int4(3), PgValue::Int4(4)],
            dimensions: vec![2],
            lower_bounds: vec![1],
            element_oid: 23,
        },
    ];
    let norm = normalize_convenience_array(&rect, 1007, &catalog).unwrap();
    assert_eq!(
        norm,
        PgValue::Array {
            values: vec![
                PgValue::Int4(1),
                PgValue::Int4(2),
                PgValue::Int4(3),
                PgValue::Int4(4),
            ],
            dimensions: vec![2, 2],
            lower_bounds: vec![1, 1],
            element_oid: 23,
        }
    );

    let ragged = vec![
        PgValue::Array {
            values: vec![PgValue::Int4(1), PgValue::Int4(2)],
            dimensions: vec![2],
            lower_bounds: vec![1],
            element_oid: 23,
        },
        PgValue::Array {
            values: vec![PgValue::Int4(3)],
            dimensions: vec![1],
            lower_bounds: vec![1],
            element_oid: 23,
        },
    ];
    assert!(matches!(
        normalize_convenience_array(&ragged, 1007, &catalog),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        normalize_convenience_array(&[], 0, &catalog),
        Err(PostgresError::Data(_))
    ));
}

#[test]
fn hostile_headers_bound_overflows_and_corrupted_elements_rejected() {
    let catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();

    let mut hostile_3d = BytesMut::new();
    hostile_3d.put_i32(3);
    hostile_3d.put_i32(0);
    hostile_3d.put_u32(23);
    for _ in 0..3 {
        hostile_3d.put_i32(65536);
        hostile_3d.put_i32(1);
    }
    assert!(matches!(
        decode_array_binary(1007, 23, &hostile_3d, &ctx, &catalog, 0),
        Err(PostgresError::Data(_))
    ));

    let mut ndim_7 = BytesMut::new();
    ndim_7.put_i32(7);
    ndim_7.put_i32(0);
    ndim_7.put_u32(23);
    assert!(matches!(
        decode_array_binary(1007, 23, &ndim_7, &ctx, &catalog, 0),
        Err(PostgresError::Data(_))
    ));

    let mut bad_lbound = BytesMut::new();
    bad_lbound.put_i32(1);
    bad_lbound.put_i32(0);
    bad_lbound.put_u32(23);
    bad_lbound.put_i32(2);
    bad_lbound.put_i32(i32::MAX);
    assert!(matches!(
        decode_array_binary(1007, 23, &bad_lbound, &ctx, &catalog, 0),
        Err(PostgresError::Data(_))
    ));

    let mut bad_flags = BytesMut::new();
    bad_flags.put_i32(0);
    bad_flags.put_i32(2);
    bad_flags.put_u32(23);
    assert!(matches!(
        decode_array_binary(1007, 23, &bad_flags, &ctx, &catalog, 0),
        Err(PostgresError::Data(_))
    ));

    let mut null_without_flag = BytesMut::new();
    null_without_flag.put_i32(1);
    null_without_flag.put_i32(0);
    null_without_flag.put_u32(23);
    null_without_flag.put_i32(1);
    null_without_flag.put_i32(1);
    null_without_flag.put_i32(-1);
    assert!(matches!(
        decode_array_binary(1007, 23, &null_without_flag, &ctx, &catalog, 0),
        Err(PostgresError::Data(_))
    ));

    let mut bad_uuid = BytesMut::new();
    bad_uuid.put_i32(2);
    bad_uuid.put_i32(0);
    bad_uuid.put_u32(2950);
    bad_uuid.put_i32(2);
    bad_uuid.put_i32(1);
    bad_uuid.put_i32(2);
    bad_uuid.put_i32(1);
    bad_uuid.put_i32(16);
    bad_uuid.put_slice(&[0u8; 16]);
    bad_uuid.put_i32(15);
    bad_uuid.put_slice(&[1u8; 15]);
    let err = decode_array_binary(2951, 2950, &bad_uuid, &ctx, &catalog, 0).unwrap_err();
    assert!(
        err.to_string().contains("array[0][1]") && err.to_string().contains("element 1"),
        "unexpected error message: {err}"
    );
}
