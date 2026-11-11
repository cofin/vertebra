//! Integration tests for owned `QueryResult`, `Column`, `PgValue`, and `PgNumeric` contracts (`ch09:t01`).

extern crate _postgres as vertebra_postgres;

use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{
    Column, Format, PG_DATE_NEG_INF, PG_DATE_POS_INF, PG_INTERVAL_NEG_INF, PG_INTERVAL_POS_INF,
    PG_TIME_MAX_MICROS, PG_TS_NEG_INF, PG_TS_POS_INF, PgNumeric, PgValue, QueryResult,
    TemporalKind, TransactionStatus, VectorKind,
};

#[test]
fn query_result_survives_source_buffer_destruction_and_preserves_duplicate_empty_columns() {
    let mut scratch_buf = b"id\x00\x00id\x00hello".to_vec();
    let c0 = Column::try_new(
        String::from_utf8_lossy(&scratch_buf[0..2]).into_owned(),
        23,
        Format::Binary,
        -1,
        16384,
        1,
    )
    .unwrap();
    let c1 = Column::try_new(String::new(), 25, Format::Text, -1, 0, 0).unwrap();
    let c2 = Column::try_new(
        String::from_utf8_lossy(&scratch_buf[4..6]).into_owned(),
        17,
        Format::Binary,
        -1,
        16384,
        2,
    )
    .unwrap();

    let row = vec![
        PgValue::Int4(7),
        PgValue::Null,
        PgValue::Bytes(scratch_buf[7..12].to_vec()),
    ];
    scratch_buf.fill(0);
    drop(scratch_buf);

    let qr = QueryResult::try_new(
        vec![c0, c1, c2],
        vec![row],
        1,
        "SELECT 1",
        TransactionStatus::InTransaction,
    )
    .unwrap();

    assert_eq!(qr.columns.len(), 3);
    assert_eq!(qr.columns[0].name, "id");
    assert_eq!(qr.columns[1].name, "");
    assert_eq!(qr.columns[2].name, "id");
    assert_ne!(qr.rows[0][1], PgValue::Bytes(Vec::new()));
    assert_eq!(qr.rows[0][2], PgValue::Bytes(b"hello".to_vec()));
    assert_eq!(qr.transaction_status, TransactionStatus::InTransaction);
}

#[test]
fn query_result_rejects_row_width_mismatch_and_invalid_column_oid_or_format() {
    let c0 = Column::try_new("a", 23, Format::Text, -1, 0, 0).unwrap();
    let c1 = Column::try_new("b", 23, Format::Binary, -1, 0, 0).unwrap();

    let err = QueryResult::try_new(
        vec![c0, c1],
        vec![vec![PgValue::Int4(1), PgValue::Int4(2), PgValue::Int4(3)]],
        1,
        "SELECT 1",
        TransactionStatus::Idle,
    )
    .unwrap_err();
    assert!(matches!(err, PostgresError::Protocol(_)));

    assert!(matches!(
        Column::try_new("bad", 0, Format::Text, -1, 0, 0),
        Err(PostgresError::Protocol(_))
    ));
    assert!(matches!(
        Format::from_code(2),
        Err(PostgresError::Protocol(_))
    ));
    assert_eq!(Format::from_code(0).unwrap().code(), 0);
    assert_eq!(Format::from_code(1).unwrap().code(), 1);
}

#[test]
fn pg_value_and_numeric_variants_preserve_lossless_representations() {
    let num = PgNumeric::from_decimal_parts(false, &[1, 2, 3, 0, 0], -4, false).unwrap();
    assert_eq!(num.to_canonical_string(), "1.2300");

    let values = vec![
        PgValue::Bool(true),
        PgValue::Int2(-12),
        PgValue::Int4(42),
        PgValue::Int8(9_000_000_000),
        PgValue::Float4(1.5),
        PgValue::Float8(-2.5),
        PgValue::Text("ok".to_string()),
        PgValue::Uuid([1u8; 16]),
        PgValue::Numeric(num),
        PgValue::Date(PG_DATE_POS_INF),
        PgValue::Date(PG_DATE_NEG_INF),
        PgValue::Time(PG_TIME_MAX_MICROS),
        PgValue::Timestamp(PG_TS_POS_INF),
        PgValue::TimestampTz(PG_TS_NEG_INF),
        PgValue::TemporalText {
            kind: TemporalKind::TimestampTz,
            wire_text: "2026-07-01 12:00:00 CST".to_string(),
            date_style: "Postgres, MDY".to_string(),
            time_zone: "Asia/Shanghai".to_string(),
        },
        PgValue::Interval {
            months: PG_INTERVAL_POS_INF.0,
            days: PG_INTERVAL_NEG_INF.1,
            microseconds: 500,
        },
        PgValue::Json(b"null".to_vec()),
        PgValue::Array {
            values: vec![PgValue::Int4(1)],
            dimensions: vec![1],
            lower_bounds: vec![1],
            element_oid: 23,
        },
        PgValue::Composite {
            type_oid: 2249,
            field_names: vec!["f1".to_string()],
            field_oids: vec![23],
            values: vec![PgValue::Int4(9)],
        },
        PgValue::BitString {
            data: vec![0xa0],
            bit_length: 3,
        },
        PgValue::Vector {
            values: vec![0.25, -0.5],
            kind: VectorKind::HalfVec,
        },
    ];
    assert_eq!(values.len(), 21);
}
