//! Integration tests for PostgreSQL `IntervalStyle` text codecs (`ch10:t03`).

extern crate _postgres as vertebra_postgres;

use bytes::BytesMut;
use vertebra_postgres::codecs::CodecContext;
use vertebra_postgres::codecs::interval_text::{
    IntervalFormatStyle, decode_interval_text, encode_interval_text, parse_interval_style,
};
use vertebra_postgres::codecs::temporal::{
    INTERVAL_OID, decode_interval_binary, encode_interval_binary,
};
use vertebra_postgres::codecs::{decode_value, encode_value};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Format, PG_INTERVAL_NEG_INF, PG_INTERVAL_POS_INF, PgValue};

fn ctx_with_style(style: &str) -> CodecContext {
    CodecContext {
        interval_style: style.to_string(),
        ..Default::default()
    }
}

#[test]
fn mixed_sign_triple_decodes_identically_across_all_four_interval_styles_and_binary() {
    let expected = (1, -2, 11_045_000_006i64);
    let mut bin_buf = BytesMut::new();
    encode_interval_binary(expected.0, expected.1, expected.2, &mut bin_buf).unwrap();
    assert_eq!(decode_interval_binary(&bin_buf).unwrap(), expected);

    let cases = [
        ("postgres", "1 mon -2 days +03:04:05.000006"),
        (
            "postgres_verbose",
            "@ 1 mon -2 days 3 hours 4 mins 5.000006 secs",
        ),
        ("sql_standard", "+0-1 -2 +3:04:05.000006"),
        ("iso_8601", "P1M-2DT3H4M5.000006S"),
    ];

    for (style, text) in cases {
        let ctx = ctx_with_style(style);
        assert_eq!(
            decode_interval_text(text, &ctx).unwrap(),
            expected,
            "failed for style={style}, text={text}"
        );
        let val = decode_value(INTERVAL_OID, Format::Text, Some(text.as_bytes()), &ctx).unwrap();
        assert_eq!(
            val,
            PgValue::Interval {
                months: 1,
                days: -2,
                microseconds: 11_045_000_006,
            }
        );
    }

    assert_eq!(
        parse_interval_style("postgres_verbose").unwrap(),
        IntervalFormatStyle::PostgresVerbose
    );
}

#[test]
fn negative_zero_hour_fractions_and_verbose_ago_preserve_signs() {
    let pg_ctx = ctx_with_style("postgres");
    assert_eq!(
        decode_interval_text("-00:00:00.5", &pg_ctx).unwrap(),
        (0, 0, -500_000)
    );
    assert_eq!(
        decode_interval_text("-00:00:00.000006", &pg_ctx).unwrap(),
        (0, 0, -6)
    );

    let verbose_ctx = ctx_with_style("postgres_verbose");
    assert_eq!(
        decode_interval_text("@ 1 year -2 days 0.5 secs ago", &verbose_ctx).unwrap(),
        (-12, 2, -500_000)
    );
    assert_eq!(
        decode_interval_text(
            "@ 1 year -2 days 3 hours 4 mins 5.000006 secs ago",
            &verbose_ctx
        )
        .unwrap(),
        (-12, 2, -11_045_000_006)
    );

    let sql_ctx = ctx_with_style("sql_standard");
    assert_eq!(
        decode_interval_text("-1-2 +3 -04:05:06.5", &sql_ctx).unwrap(),
        (-14, 3, -14_706_500_000)
    );
}

#[test]
fn zero_spellings_and_pg18_infinities_round_trip() {
    let ctx = ctx_with_style("postgres");
    for zero_str in ["00:00:00", "@ 0", "0", "PT0S"] {
        assert_eq!(decode_interval_text(zero_str, &ctx).unwrap(), (0, 0, 0));
    }
    assert_eq!(encode_interval_text(0, 0, 0), "PT0S");

    assert_eq!(
        decode_interval_text("infinity", &ctx).unwrap(),
        PG_INTERVAL_POS_INF
    );
    assert_eq!(
        decode_interval_text("+infinity", &ctx).unwrap(),
        PG_INTERVAL_POS_INF
    );
    assert_eq!(
        decode_interval_text("-infinity", &ctx).unwrap(),
        PG_INTERVAL_NEG_INF
    );
    assert_eq!(
        encode_interval_text(
            PG_INTERVAL_POS_INF.0,
            PG_INTERVAL_POS_INF.1,
            PG_INTERVAL_POS_INF.2
        ),
        "infinity"
    );
    assert_eq!(
        encode_interval_text(
            PG_INTERVAL_NEG_INF.0,
            PG_INTERVAL_NEG_INF.1,
            PG_INTERVAL_NEG_INF.2
        ),
        "-infinity"
    );
}

#[test]
fn large_multi_hundred_thousand_hour_intervals_and_canonical_encode_round_trip() {
    let ctx = ctx_with_style("iso_8601");
    let orig = (-14, 3, -900_000_000_001i64);
    let rendered = encode_interval_text(orig.0, orig.1, orig.2);
    assert_eq!(decode_interval_text(&rendered, &ctx).unwrap(), orig);

    let mut out = BytesMut::new();
    let wrote = encode_value(
        &PgValue::Interval {
            months: orig.0,
            days: orig.1,
            microseconds: orig.2,
        },
        INTERVAL_OID,
        Format::Text,
        &mut out,
        &ctx,
    )
    .unwrap();
    assert!(wrote);
    assert_eq!(
        decode_value(INTERVAL_OID, Format::Text, Some(&out), &ctx).unwrap(),
        PgValue::Interval {
            months: orig.0,
            days: orig.1,
            microseconds: orig.2,
        }
    );
}

#[test]
fn malformed_designators_and_integer_overflow_fail_with_data_error() {
    let ctx = ctx_with_style("postgres");
    for bad in [
        "",
        "P",
        "PT",
        "P10X",
        "PT10Q",
        "1 fortnight",
        "@ 2000000000 years",
        "+1-2-3",
        "00:60:00",
        "00:00:60",
        "00:00:01.1234567",
    ] {
        assert!(
            matches!(decode_interval_text(bad, &ctx), Err(PostgresError::Data(_))),
            "expected error for {bad:?}"
        );
    }
    let bad_style_ctx = ctx_with_style("custom_unknown_style");
    assert!(matches!(
        decode_interval_text("PT0S", &bad_style_ctx),
        Err(PostgresError::Data(_))
    ));
}
