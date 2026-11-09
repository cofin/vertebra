//! Integration tests for PostgreSQL multidimensional text arrays and container scanning (`ch11:t03`).

extern crate _postgres as vertebra_postgres;

use bytes::BytesMut;
use vertebra_postgres::codecs::array_text::{decode_array_text, encode_array_text};
use vertebra_postgres::codecs::{
    CodecContext, decode_value_with_catalog, encode_value_with_catalog,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Format, PgValue};
use vertebra_postgres::types::{CompositeField, TypeCatalog, TypeInfo, TypeKind};

#[test]
fn text_array_null_vs_quoted_null_vs_empty_and_binary_parity() {
    let catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();

    let raw = r#"{NULL,"NULL","","a,b","c\"d\\e","{brace}"," lead "}"#;
    let decoded = decode_array_text(1009, 25, b',', raw, &ctx, &catalog, 0).unwrap();
    let expected = PgValue::Array {
        values: vec![
            PgValue::Null,
            PgValue::Text("NULL".to_string()),
            PgValue::Text("".to_string()),
            PgValue::Text("a,b".to_string()),
            PgValue::Text("c\"d\\e".to_string()),
            PgValue::Text("{brace}".to_string()),
            PgValue::Text(" lead ".to_string()),
        ],
        dimensions: vec![7],
        lower_bounds: vec![1],
        element_oid: 25,
    };
    assert_eq!(decoded, expected);

    let mut text_out = BytesMut::new();
    encode_array_text(&decoded, 25, b',', &mut text_out, &ctx, &catalog, 0).unwrap();
    let redecoded = decode_array_text(
        1009,
        25,
        b',',
        std::str::from_utf8(&text_out).unwrap(),
        &ctx,
        &catalog,
        0,
    )
    .unwrap();
    assert_eq!(redecoded, expected);

    let mut bin_out = BytesMut::new();
    encode_value_with_catalog(
        &expected,
        1009,
        Format::Binary,
        &mut bin_out,
        &ctx,
        &catalog,
    )
    .unwrap();
    let bin_decoded =
        decode_value_with_catalog(1009, Format::Binary, Some(&bin_out), &ctx, &catalog).unwrap();
    assert_eq!(bin_decoded, decoded);
}

#[test]
fn nonstandard_lower_bounds_box_delimiter_and_nested_json_or_composite() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();

    let raw_2d = "[0:1][-2:0]={{10,NULL,30},{40,50,60}}";
    let dec_2d = decode_array_text(1007, 23, b',', raw_2d, &ctx, &catalog, 0).unwrap();
    assert_eq!(
        dec_2d,
        PgValue::Array {
            values: vec![
                PgValue::Int4(10),
                PgValue::Null,
                PgValue::Int4(30),
                PgValue::Int4(40),
                PgValue::Int4(50),
                PgValue::Int4(60),
            ],
            dimensions: vec![2, 3],
            lower_bounds: vec![0, -2],
            element_oid: 23,
        }
    );
    let mut out_2d = BytesMut::new();
    encode_array_text(&dec_2d, 23, b',', &mut out_2d, &ctx, &catalog, 0).unwrap();
    assert_eq!(std::str::from_utf8(&out_2d).unwrap(), raw_2d);

    let empty_uuid = decode_array_text(2951, 2950, b',', "{}", &ctx, &catalog, 0).unwrap();
    assert_eq!(
        empty_uuid,
        PgValue::Array {
            values: vec![],
            dimensions: vec![],
            lower_bounds: vec![],
            element_oid: 2950,
        }
    );

    let box_arr = decode_value_with_catalog(
        1020,
        Format::Text,
        Some(b"{(1,2),(0,0);(3,4),(1,1)}"),
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(
        box_arr,
        PgValue::Array {
            values: vec![
                PgValue::Text("(1,2),(0,0)".to_string()),
                PgValue::Text("(3,4),(1,1)".to_string()),
            ],
            dimensions: vec![2],
            lower_bounds: vec![1],
            element_oid: 603,
        }
    );

    let jsonb_arr = decode_value_with_catalog(
        3807,
        Format::Text,
        Some(br#"{"{\"a\":[1,2]}",NULL}"#),
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(
        jsonb_arr,
        PgValue::Array {
            values: vec![PgValue::Json(b"{\"a\":[1,2]}".to_vec()), PgValue::Null],
            dimensions: vec![2],
            lower_bounds: vec![1],
            element_oid: 3802,
        }
    );

    catalog.insert_custom_type(TypeInfo {
        oid: 41060,
        schema: "public".to_string(),
        name: "person_t".to_string(),
        kind: TypeKind::Composite {
            fields: vec![
                CompositeField {
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                CompositeField {
                    name: "label".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 41061,
        schema: "public".to_string(),
        name: "_person_t".to_string(),
        kind: TypeKind::Array {
            element_oid: 41060,
            delimiter: b',',
        },
        delimiter: b',',
    });

    let comp_arr = decode_value_with_catalog(
        41061,
        Format::Text,
        Some(br#"{"(1,\"x\"\"y\")",NULL,"(,)"}"#),
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(
        comp_arr,
        PgValue::Array {
            values: vec![
                PgValue::Composite {
                    type_oid: 41060,
                    field_names: vec!["id".to_string(), "label".to_string()],
                    field_oids: vec![23, 25],
                    values: vec![PgValue::Int4(1), PgValue::Text("x\"y".to_string())],
                },
                PgValue::Null,
                PgValue::Composite {
                    type_oid: 41060,
                    field_names: vec!["id".to_string(), "label".to_string()],
                    field_oids: vec![23, 25],
                    values: vec![PgValue::Null, PgValue::Null],
                },
            ],
            dimensions: vec![3],
            lower_bounds: vec![1],
            element_oid: 41060,
        }
    );

    assert_eq!(
        decode_value_with_catalog(55001, Format::Text, Some(b"{1,2}"), &ctx, &catalog).unwrap(),
        PgValue::Text("{1,2}".to_string())
    );
}

#[test]
fn composite_empty_slots_zero_vs_one_arity_and_malformed_arrays() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();

    catalog.insert_custom_type(TypeInfo {
        oid: 41070,
        schema: "public".to_string(),
        name: "zero_t".to_string(),
        kind: TypeKind::Composite { fields: vec![] },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 41071,
        schema: "public".to_string(),
        name: "one_t".to_string(),
        kind: TypeKind::Composite {
            fields: vec![CompositeField {
                name: "only".to_string(),
                type_oid: 25,
                type_modifier: -1,
            }],
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 41072,
        schema: "public".to_string(),
        name: "two_t".to_string(),
        kind: TypeKind::Composite {
            fields: vec![
                CompositeField {
                    name: "a".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
                CompositeField {
                    name: "b".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        },
        delimiter: b',',
    });

    assert_eq!(
        decode_value_with_catalog(41070, Format::Text, Some(b"()"), &ctx, &catalog).unwrap(),
        PgValue::Composite {
            type_oid: 41070,
            field_names: vec![],
            field_oids: vec![],
            values: vec![],
        }
    );
    assert_eq!(
        decode_value_with_catalog(41071, Format::Text, Some(b"()"), &ctx, &catalog).unwrap(),
        PgValue::Composite {
            type_oid: 41071,
            field_names: vec!["only".to_string()],
            field_oids: vec![25],
            values: vec![PgValue::Null],
        }
    );
    assert!(matches!(
        decode_value_with_catalog(41072, Format::Text, Some(b"()"), &ctx, &catalog),
        Err(PostgresError::Data(_))
    ));
    assert_eq!(
        decode_value_with_catalog(41072, Format::Text, Some(b"(a,)"), &ctx, &catalog).unwrap(),
        PgValue::Composite {
            type_oid: 41072,
            field_names: vec!["a".to_string(), "b".to_string()],
            field_oids: vec![25, 25],
            values: vec![PgValue::Text("a".to_string()), PgValue::Null],
        }
    );
    assert_eq!(
        decode_value_with_catalog(41072, Format::Text, Some(b"(,b)"), &ctx, &catalog).unwrap(),
        PgValue::Composite {
            type_oid: 41072,
            field_names: vec!["a".to_string(), "b".to_string()],
            field_oids: vec![25, 25],
            values: vec![PgValue::Null, PgValue::Text("b".to_string())],
        }
    );

    for bad in [
        "{{1,2},{3}}",
        "[1:3]={1,2}",
        "{1,,2}",
        "{a,}",
        "{\"unterminated}",
        "{{{{{{{1}}}}}}}",
    ] {
        assert!(
            matches!(
                decode_array_text(1007, 23, b',', bad, &ctx, &catalog, 0),
                Err(PostgresError::Data(_))
            ),
            "expected error on {bad}"
        );
    }
}
