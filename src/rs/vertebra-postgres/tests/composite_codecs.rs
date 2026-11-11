//! Integration tests for PostgreSQL binary and text composite codecs (`ch11:t04`).

extern crate _postgres as vertebra_postgres;

use bytes::{BufMut, BytesMut};
use vertebra_postgres::codecs::composite::{
    RECORD_OID, decode_composite_binary, decode_composite_text, encode_composite_binary,
    encode_composite_text,
};
use vertebra_postgres::codecs::{
    CodecContext, decode_value_with_catalog, encode_value_with_catalog,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Format, PgNumeric, PgValue};
use vertebra_postgres::types::{CompositeField, TypeCatalog, TypeInfo, TypeKind};

#[test]
fn named_composite_binary_and_text_parity_with_nested_array_jsonb_and_numeric() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();

    let fields = vec![
        CompositeField {
            name: "id".to_string(),
            type_oid: 23,
            type_modifier: -1,
        },
        CompositeField {
            name: "tags".to_string(),
            type_oid: 1009,
            type_modifier: -1,
        },
        CompositeField {
            name: "meta".to_string(),
            type_oid: 3802,
            type_modifier: -1,
        },
        CompositeField {
            name: "total".to_string(),
            type_oid: 1700,
            type_modifier: -1,
        },
    ];
    catalog.insert_custom_type(TypeInfo {
        oid: 41050,
        schema: "public".to_string(),
        name: "order_t".to_string(),
        kind: TypeKind::Composite {
            fields: fields.clone(),
        },
        delimiter: b',',
    });

    let comp = PgValue::Composite {
        type_oid: 41050,
        field_names: vec![
            "id".to_string(),
            "tags".to_string(),
            "meta".to_string(),
            "total".to_string(),
        ],
        field_oids: vec![23, 1009, 3802, 1700],
        values: vec![
            PgValue::Int4(42),
            PgValue::Array {
                values: vec![PgValue::Text("vip".to_string()), PgValue::Null],
                dimensions: vec![2],
                lower_bounds: vec![1],
                element_oid: 25,
            },
            PgValue::Json(b"{\"ok\":true}".to_vec()),
            PgValue::Numeric(PgNumeric::Finite {
                negative: false,
                digits: vec![19, 9900],
                weight: 0,
                dscale: 2,
            }),
        ],
    };

    let mut bin_out = BytesMut::new();
    encode_composite_binary(&comp, 41050, &mut bin_out, &ctx, &catalog, 0).unwrap();
    let bin_dec = decode_composite_binary(41050, &bin_out, &ctx, &catalog, 0).unwrap();
    assert_eq!(bin_dec, comp);

    let mut txt_out = BytesMut::new();
    encode_composite_text(&comp, 41050, &mut txt_out, &ctx, &catalog, 0).unwrap();
    let txt_dec = decode_composite_text(
        41050,
        &fields,
        std::str::from_utf8(&txt_out).unwrap(),
        &ctx,
        &catalog,
        0,
    )
    .unwrap();
    assert_eq!(txt_dec, comp);
}

#[test]
fn composite_null_vs_empty_string_anonymous_record_2249_and_stale_schema_invalidation() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();

    let fields_4 = vec![
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
        CompositeField {
            name: "c".to_string(),
            type_oid: 25,
            type_modifier: -1,
        },
        CompositeField {
            name: "d".to_string(),
            type_oid: 25,
            type_modifier: -1,
        },
    ];
    catalog.insert_custom_type(TypeInfo {
        oid: 41051,
        schema: "public".to_string(),
        name: "quad_t".to_string(),
        kind: TypeKind::Composite {
            fields: fields_4.clone(),
        },
        delimiter: b',',
    });

    let raw = r#"(,"",NULL,"a,b(c)\"d\\e")"#;
    let dec = decode_composite_text(41051, &fields_4, raw, &ctx, &catalog, 0).unwrap();
    assert_eq!(
        dec,
        PgValue::Composite {
            type_oid: 41051,
            field_names: vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string()
            ],
            field_oids: vec![25, 25, 25, 25],
            values: vec![
                PgValue::Null,
                PgValue::Text("".to_string()),
                PgValue::Text("NULL".to_string()),
                PgValue::Text("a,b(c)\"d\\e".to_string()),
            ],
        }
    );

    assert_eq!(
        decode_value_with_catalog(41051, Format::Binary, None, &ctx, &catalog).unwrap(),
        PgValue::Null
    );

    let mut rec_bin = BytesMut::new();
    rec_bin.put_i32(2);
    rec_bin.put_u32(23);
    rec_bin.put_i32(4);
    rec_bin.put_i32(7);
    rec_bin.put_u32(25);
    rec_bin.put_i32(2);
    rec_bin.put_slice(b"ok");
    let rec_val =
        decode_value_with_catalog(RECORD_OID, Format::Binary, Some(&rec_bin), &ctx, &catalog)
            .unwrap();
    assert_eq!(
        rec_val,
        PgValue::Composite {
            type_oid: RECORD_OID,
            field_names: vec!["f1".to_string(), "f2".to_string()],
            field_oids: vec![23, 25],
            values: vec![PgValue::Int4(7), PgValue::Text("ok".to_string())],
        }
    );

    assert_eq!(
        decode_value_with_catalog(RECORD_OID, Format::Text, Some(b"(7,ok)"), &ctx, &catalog)
            .unwrap(),
        PgValue::Text("(7,ok)".to_string())
    );
    assert_eq!(
        decode_value_with_catalog(49999, Format::Text, Some(b"(7,ok)"), &ctx, &catalog).unwrap(),
        PgValue::Text("(7,ok)".to_string())
    );

    let stale = PgValue::Composite {
        type_oid: 41051,
        field_names: vec!["a".to_string(), "b".to_string()],
        field_oids: vec![25, 25],
        values: vec![PgValue::Null, PgValue::Null],
    };
    let mut out = BytesMut::new();
    assert!(matches!(
        encode_value_with_catalog(&stale, 41051, Format::Binary, &mut out, &ctx, &catalog),
        Err(PostgresError::Data(_))
    ));
    assert!(
        catalog
            .validate_composite_schema(41051, None, &[25, 25])
            .is_err()
    );
    assert!(catalog.type_info(41051).is_none());
}
