//! Integration tests for PostgreSQL `2000-01-01` epoch temporal and interval binary codecs (`ch10:t01`).

extern crate _postgres as vertebra_postgres;

use bytes::BytesMut;
use vertebra_postgres::codecs::{
    CodecContext, DATE_OID, INTERVAL_OID, TIME_OID, TIMESTAMP_OID, TIMESTAMPTZ_OID, decode_value,
    encode_value,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{
    Format, PG_DATE_NEG_INF, PG_DATE_POS_INF, PG_INTERVAL_POS_INF, PG_TIME_MAX_MICROS,
    PG_TS_POS_INF, PgValue,
};

#[test]
fn temporal_and_interval_binary_epochs_infinities_and_bounds() {
    let ctx = CodecContext::default();

    for days in [
        0,
        -10_957,
        60,
        -730_485,
        3_652_425,
        PG_DATE_POS_INF,
        PG_DATE_NEG_INF,
    ] {
        let mut buf = BytesMut::new();
        encode_value(
            &PgValue::Date(days),
            DATE_OID,
            Format::Binary,
            &mut buf,
            &ctx,
        )
        .unwrap();
        assert_eq!(
            decode_value(DATE_OID, Format::Binary, Some(&buf), &ctx).unwrap(),
            PgValue::Date(days)
        );
    }

    assert!(matches!(
        decode_value(
            DATE_OID,
            Format::Binary,
            Some(&(i32::MAX - 1).to_be_bytes()),
            &ctx
        ),
        Err(PostgresError::Data(_))
    ));

    for t in [0, PG_TIME_MAX_MICROS] {
        let mut buf = BytesMut::new();
        encode_value(&PgValue::Time(t), TIME_OID, Format::Binary, &mut buf, &ctx).unwrap();
        assert_eq!(
            decode_value(TIME_OID, Format::Binary, Some(&buf), &ctx).unwrap(),
            PgValue::Time(t)
        );
    }
    assert!(matches!(
        decode_value(
            TIME_OID,
            Format::Binary,
            Some(&(PG_TIME_MAX_MICROS + 1).to_be_bytes()),
            &ctx
        ),
        Err(PostgresError::Data(_))
    ));

    for (oid, val) in [
        (TIMESTAMP_OID, PgValue::Timestamp(-1)),
        (TIMESTAMPTZ_OID, PgValue::TimestampTz(PG_TS_POS_INF)),
    ] {
        let mut buf = BytesMut::new();
        encode_value(&val, oid, Format::Binary, &mut buf, &ctx).unwrap();
        assert_eq!(
            decode_value(oid, Format::Binary, Some(&buf), &ctx).unwrap(),
            val
        );
    }

    let mut mismatch = BytesMut::new();
    assert!(matches!(
        encode_value(
            &PgValue::Timestamp(0),
            TIMESTAMPTZ_OID,
            Format::Binary,
            &mut mismatch,
            &ctx
        ),
        Err(PostgresError::Data(_))
    ));

    for (m, d, us) in [(1, -2, 11_045_000_006), PG_INTERVAL_POS_INF] {
        let val = PgValue::Interval {
            months: m,
            days: d,
            microseconds: us,
        };
        let mut buf = BytesMut::new();
        encode_value(&val, INTERVAL_OID, Format::Binary, &mut buf, &ctx).unwrap();
        assert_eq!(buf.len(), 16);
        assert_eq!(
            decode_value(INTERVAL_OID, Format::Binary, Some(&buf), &ctx).unwrap(),
            val
        );
    }
}
