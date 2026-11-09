//! Integration tests for arbitrary-precision PostgreSQL `NUMERIC` (`OID 1700`) codecs (`ch09:t04`).

extern crate _postgres as vertebra_postgres;

use bytes::BytesMut;
use vertebra_postgres::codecs::numeric::{
    decode_numeric_binary, decode_numeric_text, encode_numeric_binary, encode_numeric_text,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::PgNumeric;

#[test]
fn numeric_pg18_golden_trailing_scale_and_sub_unit_weight() {
    let golden_1_2300: [u8; 12] = [
        0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x01, 0x08, 0xfc,
    ];
    let decoded = decode_numeric_binary(&golden_1_2300).unwrap();
    assert_eq!(
        decoded,
        PgNumeric::Finite {
            negative: false,
            weight: 0,
            dscale: 4,
            digits: vec![1, 2300],
        }
    );
    assert_eq!(encode_numeric_text(&decoded), "1.2300");
    assert_eq!(decode_numeric_text("1.2300").unwrap(), decoded);

    let mut reencoded = BytesMut::new();
    encode_numeric_binary(&decoded, &mut reencoded).unwrap();
    assert_eq!(&reencoded[..], &golden_1_2300);

    let sub = decode_numeric_text("-0.000056").unwrap();
    assert_eq!(
        sub,
        PgNumeric::Finite {
            negative: true,
            weight: -2,
            dscale: 6,
            digits: vec![5600],
        }
    );
    assert_eq!(encode_numeric_text(&sub), "-0.000056");

    let zero_scaled = decode_numeric_text("-0.000").unwrap();
    assert_eq!(
        zero_scaled,
        PgNumeric::Finite {
            negative: false,
            weight: 0,
            dscale: 3,
            digits: Vec::new(),
        }
    );
    assert_eq!(encode_numeric_text(&zero_scaled), "0.000");
}

#[test]
fn numeric_50_digit_precision_specials_and_malformed_wire_rejection() {
    let high_prec = "123456789012345678901234567890.00009876543210987654";
    let parsed = decode_numeric_text(high_prec).unwrap();
    assert_eq!(encode_numeric_text(&parsed), high_prec);
    let mut bin = BytesMut::new();
    encode_numeric_binary(&parsed, &mut bin).unwrap();
    assert_eq!(decode_numeric_binary(&bin).unwrap(), parsed);

    let big_exp = decode_numeric_text("1e1000").unwrap();
    let rendered = encode_numeric_text(&big_exp);
    assert_eq!(rendered.len(), 1001);
    assert!(rendered.starts_with('1'));

    for (raw, expected) in [
        ([0, 0, 0, 0, 0xc0, 0, 0, 0], PgNumeric::NaN),
        ([0, 0, 0, 0, 0xd0, 0, 0, 0], PgNumeric::PosInf),
        ([0, 0, 0, 0, 0xf0, 0, 0, 0], PgNumeric::NegInf),
    ] {
        assert_eq!(decode_numeric_binary(&raw).unwrap(), expected);
    }

    assert!(matches!(
        PgNumeric::from_decimal_parts(false, &[1], 0, true),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        decode_numeric_text("sNaN"),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        decode_numeric_binary(&[0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x10]),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        decode_numeric_binary(&[0, 0, 0, 0, 0x80, 0, 0, 0]),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        decode_numeric_binary(&[0, 1, 0, 0, 0xc0, 0, 0, 0, 0, 1]),
        Err(PostgresError::Data(_))
    ));
}
