//! Integration tests for built-in scalar binary and text codecs (`ch09:t03`).

extern crate _postgres as vertebra_postgres;

use bytes::BytesMut;
use vertebra_postgres::codecs::{
    BOOL_OID, BYTEA_OID, CodecContext, FLOAT4_OID, FLOAT8_OID, INT2_OID, INT4_OID, INT8_OID,
    OID_OID, TEXT_OID, UUID_OID, decode_value, encode_value, format_for_oid,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Format, PgValue};

#[test]
fn scalar_integers_bools_and_oids_round_trip_and_reject_invalid_widths() {
    let ctx = CodecContext::default();
    assert_eq!(format_for_oid(INT4_OID), Format::Binary);
    assert_eq!(format_for_oid(99_999), Format::Text);

    for (oid, val) in [
        (BOOL_OID, PgValue::Bool(true)),
        (BOOL_OID, PgValue::Bool(false)),
        (INT2_OID, PgValue::Int2(i16::MIN)),
        (INT2_OID, PgValue::Int2(i16::MAX)),
        (INT4_OID, PgValue::Int4(i32::MIN)),
        (INT4_OID, PgValue::Int4(i32::MAX)),
        (INT8_OID, PgValue::Int8(i64::MIN)),
        (INT8_OID, PgValue::Int8(i64::MAX)),
        (OID_OID, PgValue::Int8(4_000_000_000)),
    ] {
        for fmt in [Format::Binary, Format::Text] {
            let mut buf = BytesMut::new();
            assert!(encode_value(&val, oid, fmt, &mut buf, &ctx).unwrap());
            let decoded = decode_value(oid, fmt, Some(&buf), &ctx).unwrap();
            assert_eq!(decoded, val);
        }
    }

    assert!(matches!(
        decode_value(BOOL_OID, Format::Binary, Some(&[2]), &ctx),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        decode_value(INT2_OID, Format::Binary, Some(&[1, 2, 3]), &ctx),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        decode_value(INT2_OID, Format::Text, Some(b"40000"), &ctx),
        Err(PostgresError::Data(_))
    ));
}

#[test]
fn scalar_floats_preserve_signed_zero_infinities_and_nan() {
    let ctx = CodecContext::default();

    for fmt in [Format::Binary, Format::Text] {
        let mut buf4 = BytesMut::new();
        encode_value(&PgValue::Float4(-0.0_f32), FLOAT4_OID, fmt, &mut buf4, &ctx).unwrap();
        let PgValue::Float4(f4) = decode_value(FLOAT4_OID, fmt, Some(&buf4), &ctx).unwrap() else {
            panic!("expected Float4");
        };
        assert_eq!(f4, 0.0);
        assert!(f4.is_sign_negative());

        let mut buf8 = BytesMut::new();
        encode_value(&PgValue::Float8(-0.0_f64), FLOAT8_OID, fmt, &mut buf8, &ctx).unwrap();
        let PgValue::Float8(f8) = decode_value(FLOAT8_OID, fmt, Some(&buf8), &ctx).unwrap() else {
            panic!("expected Float8");
        };
        assert_eq!(f8, 0.0);
        assert!(f8.is_sign_negative());

        let nan_bytes = 0x7ff8_0000_0000_0000_u64.to_be_bytes();
        let raw_nan: &[u8] = if fmt == Format::Binary {
            &nan_bytes
        } else {
            b"NaN"
        };
        let PgValue::Float8(nan) = decode_value(FLOAT8_OID, fmt, Some(raw_nan), &ctx).unwrap()
        else {
            panic!("expected Float8 NaN");
        };
        assert!(nan.is_nan());
    }
}

#[test]
fn bytea_uuid_text_nul_and_unknown_oid_fallback_invariants() {
    let ctx = CodecContext::default();

    assert_eq!(
        decode_value(BYTEA_OID, Format::Text, Some(br"\x00ff41"), &ctx).unwrap(),
        PgValue::Bytes(vec![0x00, 0xff, 0x41])
    );
    assert_eq!(
        decode_value(BYTEA_OID, Format::Text, Some(br"a\000\377\\b"), &ctx).unwrap(),
        PgValue::Bytes(vec![b'a', 0x00, 0xff, b'\\', b'b'])
    );
    assert_eq!(
        decode_value(BYTEA_OID, Format::Text, Some(br"\x"), &ctx).unwrap(),
        PgValue::Bytes(Vec::new())
    );
    assert_eq!(
        decode_value(BYTEA_OID, Format::Text, None, &ctx).unwrap(),
        PgValue::Null
    );
    assert!(matches!(
        decode_value(BYTEA_OID, Format::Text, Some(br"\x0g"), &ctx),
        Err(PostgresError::Data(_))
    ));

    let uuid_text = b"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11";
    let PgValue::Uuid(u) = decode_value(UUID_OID, Format::Text, Some(uuid_text), &ctx).unwrap()
    else {
        panic!("expected Uuid");
    };
    assert_eq!(
        decode_value(UUID_OID, Format::Binary, Some(&u), &ctx).unwrap(),
        PgValue::Uuid(u)
    );
    assert!(matches!(
        decode_value(UUID_OID, Format::Binary, Some(&u[..15]), &ctx),
        Err(PostgresError::Data(_))
    ));

    let mut out = BytesMut::new();
    assert!(matches!(
        encode_value(
            &PgValue::Text("ab\0cd".to_string()),
            TEXT_OID,
            Format::Text,
            &mut out,
            &ctx
        ),
        Err(PostgresError::Data(_))
    ));

    assert_eq!(
        decode_value(99_999, Format::Text, Some(b"custom_val"), &ctx).unwrap(),
        PgValue::Text("custom_val".to_string())
    );
    assert!(matches!(
        decode_value(99_999, Format::Binary, Some(&[1, 2]), &ctx),
        Err(PostgresError::Data(_))
    ));
}
