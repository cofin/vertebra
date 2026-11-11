//! Integration tests for PostgreSQL session-aware `DateStyle` and `TimeZone` temporal text codecs (`ch10:t02`).

extern crate _postgres as vertebra_postgres;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::codecs::CodecContext;
use vertebra_postgres::codecs::datetime_text::{
    DateFormatStyle, DateOrder, decode_date_text, decode_time_text, decode_timestamp_text,
    encode_temporal_text, parse_date_style,
};
use vertebra_postgres::codecs::temporal::{
    DATE_OID, TIME_OID, TIMESTAMP_OID, TIMESTAMPTZ_OID, decode_date_binary,
    decode_timestamp_binary, ymd_to_pg_days,
};
use vertebra_postgres::codecs::{decode_value, encode_value};
use vertebra_postgres::config::RawPostgresOptions;
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{
    Format, PG_DATE_NEG_INF, PG_DATE_POS_INF, PG_TIME_MAX_MICROS, PG_TS_NEG_INF, PG_TS_POS_INF,
    PgValue, TemporalKind,
};
use vertebra_postgres::session::SessionHandle;

fn ctx_with(date_style: &str, time_zone: &str) -> CodecContext {
    CodecContext {
        date_style: date_style.to_string(),
        time_zone: time_zone.to_string(),
        ..Default::default()
    }
}

#[test]
fn date_style_and_order_combinations_decode_to_identical_epoch_days() {
    let expected_days = ymd_to_pg_days(2026, 7, 4).unwrap();
    assert_eq!(expected_days, 9681);

    let cases = [
        ("ISO, MDY", "2026-07-04"),
        ("ISO, DMY", "2026-07-04"),
        ("ISO, YMD", "2026-07-04"),
        ("SQL, MDY", "07/04/2026"),
        ("SQL, DMY", "04/07/2026"),
        ("SQL, YMD", "2026/07/04"),
        ("Postgres, MDY", "07-04-2026"),
        ("Postgres, DMY", "04-07-2026"),
        ("Postgres, YMD", "2026-07-04"),
        ("German, DMY", "04.07.2026"),
    ];

    for (style_str, input) in cases {
        let ctx = ctx_with(style_str, "UTC");
        assert_eq!(
            decode_date_text(input, &ctx).unwrap(),
            expected_days,
            "failed for style={style_str}, input={input}"
        );
        let decoded = decode_value(DATE_OID, Format::Text, Some(input.as_bytes()), &ctx).unwrap();
        assert_eq!(decoded, PgValue::Date(expected_days));
    }

    assert_eq!(
        parse_date_style("German, MDY"),
        (DateFormatStyle::German, DateOrder::Mdy)
    );
}

#[test]
fn bc_leap_years_and_large_years_match_binary_temporal_fixtures() {
    let ctx = ctx_with("ISO, MDY", "UTC");

    let bc_year1 = decode_date_text("0001-01-01 BC", &ctx).unwrap();
    assert_eq!(bc_year1, -730_485);
    let bin_bc1 = decode_date_binary(&(-730_485i32).to_be_bytes()).unwrap();
    assert_eq!(bc_year1, bin_bc1);

    let bc_year2 = decode_date_text("0002-01-01 BC", &ctx).unwrap();
    assert_eq!(bc_year2, -730_850);

    let bc_leap = decode_date_text("0005-02-29 BC", &ctx).unwrap();
    assert_eq!(bc_leap, ymd_to_pg_days(-4, 2, 29).unwrap());

    assert!(matches!(
        decode_date_text("0004-02-29 BC", &ctx),
        Err(PostgresError::Data(_))
    ));

    let bc_4713 = decode_date_text("4713-11-24 BC", &ctx).unwrap();
    assert_eq!(bc_4713, ymd_to_pg_days(-4712, 11, 24).unwrap());
    let min_date = decode_date_text("4714-11-24 BC", &ctx).unwrap();
    assert_eq!(min_date, -2_451_545);

    let max_ts = decode_timestamp_text(
        "294276-12-31 23:59:59.999999+00",
        TemporalKind::TimestampTz,
        &ctx,
    )
    .unwrap();
    let max_days = ymd_to_pg_days(294_276, 12, 31).unwrap() as i64;
    let expected_micros = max_days * 86_400_000_000 + 86_399_999_999;
    assert_eq!(max_ts, PgValue::TimestampTz(expected_micros));
    assert_eq!(
        decode_timestamp_binary(&expected_micros.to_be_bytes()).unwrap(),
        expected_micros
    );
}

#[test]
fn postgres_and_sql_timestamptz_resolve_numeric_and_utc_while_tagging_abbreviations() {
    let pg_ctx = ctx_with("Postgres, MDY", "Asia/Shanghai");
    let expected_days = ymd_to_pg_days(2026, 7, 1).unwrap() as i64;
    let base_utc = expected_days * 86_400_000_000 + 12 * 3_600_000_000;

    let with_sec_offset = decode_timestamp_text(
        "Wed Jul 01 17:30:15.25 2026 +05:30:15",
        TemporalKind::TimestampTz,
        &pg_ctx,
    )
    .unwrap();
    assert_eq!(with_sec_offset, PgValue::TimestampTz(base_utc + 250_000));

    let with_utc = decode_timestamp_text(
        "Wed Jul 01 12:00:00 2026 UTC",
        TemporalKind::TimestampTz,
        &pg_ctx,
    )
    .unwrap();
    assert_eq!(with_utc, PgValue::TimestampTz(base_utc));

    let with_abbrev = decode_timestamp_text(
        "Wed Jul 01 12:00:00 2026 CST",
        TemporalKind::TimestampTz,
        &pg_ctx,
    )
    .unwrap();
    assert_eq!(
        with_abbrev,
        PgValue::TimestampTz(expected_days * 86_400_000_000 + 4 * 3_600_000_000)
    );

    let mismatched_abbrev = decode_timestamp_text(
        "Wed Jul 01 12:00:00 2026 EDT",
        TemporalKind::TimestampTz,
        &pg_ctx,
    )
    .unwrap();
    assert_eq!(
        mismatched_abbrev,
        PgValue::TemporalText {
            kind: TemporalKind::TimestampTz,
            wire_text: "Wed Jul 01 12:00:00 2026 EDT".to_string(),
            date_style: "Postgres, MDY".to_string(),
            time_zone: "Asia/Shanghai".to_string(),
        }
    );

    let dmy_ctx = ctx_with("Postgres, DMY", "Europe/London");
    let dmy_bst = decode_timestamp_text(
        "Wed 01 Jul 12:00:00 2026 BST",
        TemporalKind::TimestampTz,
        &dmy_ctx,
    )
    .unwrap();
    assert_eq!(
        dmy_bst,
        PgValue::TimestampTz(expected_days * 86_400_000_000 + 11 * 3_600_000_000)
    );
}

#[test]
fn end_of_day_infinities_and_temporal_text_rebind_refusal() {
    let ctx = ctx_with("ISO, MDY", "UTC");

    assert_eq!(decode_time_text("24:00:00").unwrap(), PG_TIME_MAX_MICROS);
    assert_eq!(
        decode_time_text("24:00:00.000000").unwrap(),
        PG_TIME_MAX_MICROS
    );
    assert!(matches!(
        decode_time_text("24:00:00.000001"),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        decode_time_text("25:00:00"),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        decode_date_text("2025-02-29", &ctx),
        Err(PostgresError::Data(_))
    ));

    assert_eq!(decode_date_text("infinity", &ctx).unwrap(), PG_DATE_POS_INF);
    assert_eq!(
        decode_date_text("-infinity", &ctx).unwrap(),
        PG_DATE_NEG_INF
    );
    assert_eq!(
        decode_timestamp_text("infinity", TemporalKind::Timestamp, &ctx).unwrap(),
        PgValue::Timestamp(PG_TS_POS_INF)
    );
    assert_eq!(
        decode_timestamp_text("-infinity", TemporalKind::TimestampTz, &ctx).unwrap(),
        PgValue::TimestampTz(PG_TS_NEG_INF)
    );

    assert_eq!(
        encode_temporal_text(&PgValue::Time(PG_TIME_MAX_MICROS)).unwrap(),
        "24:00:00"
    );
    assert_eq!(
        encode_temporal_text(&PgValue::Date(PG_DATE_POS_INF)).unwrap(),
        "infinity"
    );
    assert_eq!(
        encode_temporal_text(&PgValue::TimestampTz(PG_TS_NEG_INF)).unwrap(),
        "-infinity"
    );

    let temporal_text = PgValue::TemporalText {
        kind: TemporalKind::TimestampTz,
        wire_text: "Wed Jul 01 12:00:00 2026 CST".to_string(),
        date_style: "Postgres, MDY".to_string(),
        time_zone: "UTC".to_string(),
    };
    assert!(matches!(
        encode_temporal_text(&temporal_text),
        Err(PostgresError::Data(_))
    ));
    let mut out = BytesMut::new();
    assert!(matches!(
        encode_value(
            &temporal_text,
            TIMESTAMPTZ_OID,
            Format::Text,
            &mut out,
            &ctx
        ),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        encode_value(&temporal_text, TIMESTAMP_OID, Format::Text, &mut out, &ctx),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        encode_value(&temporal_text, TIME_OID, Format::Text, &mut out, &ctx),
        Err(PostgresError::Data(_))
    ));
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn param_status(name: &str, val: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(val.as_bytes());
    body.push(0);
    frame(b'S', &body)
}

fn row_desc(cols: &[(&str, u32, i16)]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(cols.len() as i16);
    for &(name, oid, fmt) in cols {
        body.put_slice(name.as_bytes());
        body.put_u8(0);
        body.put_u32(0);
        body.put_i16(0);
        body.put_u32(oid);
        body.put_i16(-1);
        body.put_i32(-1);
        body.put_i16(fmt);
    }
    frame(b'T', &body)
}

fn data_row(cells: &[Option<&[u8]>]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(cells.len() as i16);
    for cell in cells {
        match cell {
            None => body.put_i32(-1),
            Some(bytes) => {
                body.put_i32(bytes.len() as i32);
                body.put_slice(bytes);
            }
        }
    }
    frame(b'D', &body)
}

fn cmd_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    frame(b'C', &body)
}

#[tokio::test]
async fn simple_query_parameter_status_date_style_transition_updates_decoder_live() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut len_buf = [0u8; 4];
        sock.read_exact(&mut len_buf).await.unwrap();
        let len = i32::from_be_bytes(len_buf) as usize;
        let mut startup = vec![0u8; len - 4];
        sock.read_exact(&mut startup).await.unwrap();

        let mut handshake = Vec::new();
        handshake.extend_from_slice(&frame(b'R', &0i32.to_be_bytes()));
        handshake.extend_from_slice(&param_status("DateStyle", "ISO, MDY"));
        handshake.extend_from_slice(&param_status("TimeZone", "UTC"));
        handshake.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
        handshake.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&handshake).await.unwrap();

        let mut qhdr = [0u8; 5];
        sock.read_exact(&mut qhdr).await.unwrap();
        let qlen = i32::from_be_bytes([qhdr[1], qhdr[2], qhdr[3], qhdr[4]]) as usize;
        let mut qbody = vec![0u8; qlen - 4];
        sock.read_exact(&mut qbody).await.unwrap();

        let mut resp = Vec::new();
        resp.extend_from_slice(&param_status("DateStyle", "German, DMY"));
        resp.extend_from_slice(&cmd_complete("SET"));
        resp.extend_from_slice(&row_desc(&[("d", DATE_OID, 0)]));
        resp.extend_from_slice(&data_row(&[Some(b"05.11.2026")]));
        resp.extend_from_slice(&cmd_complete("SELECT 1"));
        resp.extend_from_slice(&param_status("DateStyle", "SQL, MDY"));
        resp.extend_from_slice(&cmd_complete("SET"));
        resp.extend_from_slice(&row_desc(&[("d", DATE_OID, 0)]));
        resp.extend_from_slice(&data_row(&[Some(b"11/05/2026")]));
        resp.extend_from_slice(&cmd_complete("SELECT 1"));
        resp.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp).await.unwrap();
    });

    let mut raw = RawPostgresOptions::new("127.0.0.1", "alice", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    let mut handle = SessionHandle::new(raw).unwrap();
    let conn = handle.connect().await.unwrap();

    let results = conn
        .simple_query(
            "SET DateStyle = 'German, DMY'; SELECT '2026-11-05'::date; SET DateStyle = 'SQL, MDY'; SELECT '2026-11-05'::date",
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results[1].rows, vec![vec![PgValue::Date(9805)]]);
    assert_eq!(results[3].rows, vec![vec![PgValue::Date(9805)]]);

    server.await.unwrap();
}
