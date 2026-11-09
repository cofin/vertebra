//! Integration tests for `jiff`-backed (`=0.2.39`, `tzdb-bundle-always`) non-ISO `timestamptz`
//! text resolution, DST fold/gap and LMT `TemporalText` preservation, civil-range guards,
//! and unconditional `DataError` typed-rebind refusal (`ch17:t04`).

extern crate _postgres as vertebra_postgres;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::codecs::datetime_text::timezone_text::resolve_or_capture_temporal;
use vertebra_postgres::codecs::datetime_text::{
    ParsedTimestampFields, decode_timestamp_text, encode_temporal_text,
};
use vertebra_postgres::codecs::temporal::{
    DATE_OID, TIME_OID, TIMESTAMP_OID, TIMESTAMPTZ_OID, ymd_to_pg_days,
};
use vertebra_postgres::codecs::{CodecContext, TypeCatalog, encode_value};
use vertebra_postgres::config::RawPostgresOptions;
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::extended::{Parameter, encode_bind};
use vertebra_postgres::pipeline::{PipelineBatch, PipelineOperation};
use vertebra_postgres::result::{Format, PgValue, TemporalKind};
use vertebra_postgres::session::SessionHandle;

fn ctx_with(date_style: &str, time_zone: &str) -> CodecContext {
    CodecContext {
        date_style: date_style.to_string(),
        time_zone: time_zone.to_string(),
        ..Default::default()
    }
}

fn pg_utc_micros(year: i32, month: u8, day: u8, hour: i64, minute: i64, second: i64) -> i64 {
    let days = i64::from(ymd_to_pg_days(year, month, day).unwrap());
    days * 86_400_000_000 + (hour * 3600 + minute * 60 + second) * 1_000_000
}

#[test]
fn unique_iana_and_posix_dst_fold_distinct_abbreviations_resolve_exact_utc_instants() {
    let ny_ctx = ctx_with("Postgres, MDY", "America/New_York");

    let fold_edt = decode_timestamp_text(
        "Sun Nov 01 01:30:00 2026 EDT",
        TemporalKind::TimestampTz,
        &ny_ctx,
    )
    .unwrap();
    assert_eq!(
        fold_edt,
        PgValue::TimestampTz(pg_utc_micros(2026, 11, 1, 5, 30, 0))
    );

    let fold_est = decode_timestamp_text(
        "Sun Nov 01 01:30:00 2026 EST",
        TemporalKind::TimestampTz,
        &ny_ctx,
    )
    .unwrap();
    assert_eq!(
        fold_est,
        PgValue::TimestampTz(pg_utc_micros(2026, 11, 1, 6, 30, 0))
    );

    let summer = decode_timestamp_text(
        "Wed Jul 08 07:30:00.125 2026 EDT",
        TemporalKind::TimestampTz,
        &ny_ctx,
    )
    .unwrap();
    assert_eq!(
        summer,
        PgValue::TimestampTz(pg_utc_micros(2026, 7, 8, 11, 30, 0) + 125_000)
    );

    let posix_ctx = ctx_with("Postgres, MDY", "EST5EDT,M3.2.0,M11.1.0");
    let posix_edt = decode_timestamp_text(
        "Sun Nov 01 01:30:00 2026 EDT",
        TemporalKind::TimestampTz,
        &posix_ctx,
    )
    .unwrap();
    let posix_est = decode_timestamp_text(
        "Sun Nov 01 01:30:00 2026 EST",
        TemporalKind::TimestampTz,
        &posix_ctx,
    )
    .unwrap();
    assert_eq!(posix_edt, fold_edt);
    assert_eq!(posix_est, fold_est);

    let parsed = ParsedTimestampFields {
        year_astro: 2026,
        month: 11,
        day: 1,
        pg_days: ymd_to_pg_days(2026, 11, 1).unwrap(),
        time_micros: (3600 + 30 * 60) * 1_000_000,
        tz_abbrev: Some("EST".to_string()),
        wire_text: "Sun Nov 01 01:30:00 2026 EST".to_string(),
    };
    assert_eq!(
        resolve_or_capture_temporal(&parsed, "Postgres, MDY", "America/New_York").unwrap(),
        fold_est
    );
}

#[test]
fn same_abbreviation_fold_dst_gap_and_historical_lmt_capture_temporal_text() {
    let same_abbr_posix = ctx_with("Postgres, MDY", "CST6CST5,M3.2.0,M11.1.0");
    let fold_same = decode_timestamp_text(
        "Sun Nov 01 01:30:00 2026 CST",
        TemporalKind::TimestampTz,
        &same_abbr_posix,
    )
    .unwrap();
    assert_eq!(
        fold_same,
        PgValue::TemporalText {
            kind: TemporalKind::TimestampTz,
            wire_text: "Sun Nov 01 01:30:00 2026 CST".to_string(),
            date_style: "Postgres, MDY".to_string(),
            time_zone: "CST6CST5,M3.2.0,M11.1.0".to_string(),
        }
    );

    let ny_ctx = ctx_with("Postgres, MDY", "America/New_York");
    for gap_input in [
        "Sun Mar 08 02:30:00 2026 EDT",
        "Sun Mar 08 02:30:00 2026 EST",
    ] {
        let gap_val = decode_timestamp_text(gap_input, TemporalKind::TimestampTz, &ny_ctx).unwrap();
        assert_eq!(
            gap_val,
            PgValue::TemporalText {
                kind: TemporalKind::TimestampTz,
                wire_text: gap_input.to_string(),
                date_style: "Postgres, MDY".to_string(),
                time_zone: "America/New_York".to_string(),
            }
        );
    }

    let lmt_input = "Sat Nov 17 12:00:00 1883 LMT";
    let lmt_val = decode_timestamp_text(lmt_input, TemporalKind::TimestampTz, &ny_ctx).unwrap();
    assert_eq!(
        lmt_val,
        PgValue::TemporalText {
            kind: TemporalKind::TimestampTz,
            wire_text: lmt_input.to_string(),
            date_style: "Postgres, MDY".to_string(),
            time_zone: "America/New_York".to_string(),
        }
    );
}

#[test]
fn out_of_jiff_range_years_preserve_temporal_text_while_numeric_offsets_resolve() {
    let ny_pg = ctx_with("Postgres, MDY", "America/New_York");
    let far_future_abbr = decode_timestamp_text(
        "Wed Jul 09 07:30:00 12000 EDT",
        TemporalKind::TimestampTz,
        &ny_pg,
    )
    .unwrap();
    assert_eq!(
        far_future_abbr,
        PgValue::TemporalText {
            kind: TemporalKind::TimestampTz,
            wire_text: "Wed Jul 09 07:30:00 12000 EDT".to_string(),
            date_style: "Postgres, MDY".to_string(),
            time_zone: "America/New_York".to_string(),
        }
    );

    let ny_iso = ctx_with("ISO, MDY", "America/New_York");
    let bc_abbr = decode_timestamp_text(
        "0044-03-15 12:00:00 EST BC",
        TemporalKind::TimestampTz,
        &ny_iso,
    )
    .unwrap();
    assert_eq!(
        bc_abbr,
        PgValue::TemporalText {
            kind: TemporalKind::TimestampTz,
            wire_text: "0044-03-15 12:00:00 EST BC".to_string(),
            date_style: "ISO, MDY".to_string(),
            time_zone: "America/New_York".to_string(),
        }
    );

    let far_future_numeric = decode_timestamp_text(
        "12000-07-09 07:30:00-04",
        TemporalKind::TimestampTz,
        &ny_iso,
    )
    .unwrap();
    assert_eq!(
        far_future_numeric,
        PgValue::TimestampTz(pg_utc_micros(12000, 7, 9, 11, 30, 0))
    );
}

#[test]
fn global_abbreviation_guessing_is_forbidden_across_mismatched_and_unknown_zones() {
    for tz_name in [
        "UTC",
        "America/Havana",
        "Europe/London",
        "Custom/UnknownZone",
    ] {
        let ctx = ctx_with("Postgres, MDY", tz_name);
        let input = "Wed Jul 09 07:30:00 2026 CST";
        let val = decode_timestamp_text(input, TemporalKind::TimestampTz, &ctx).unwrap();
        assert_eq!(
            val,
            PgValue::TemporalText {
                kind: TemporalKind::TimestampTz,
                wire_text: input.to_string(),
                date_style: "Postgres, MDY".to_string(),
                time_zone: tz_name.to_string(),
            },
            "unexpectedly guessed CST under TimeZone={tz_name}"
        );
    }
}

#[tokio::test]
async fn matching_context_temporal_text_rebind_fails_with_data_error_before_any_wire_submission() {
    let ctx = ctx_with("Postgres, MDY", "UTC");
    let temporal_text = PgValue::TemporalText {
        kind: TemporalKind::TimestampTz,
        wire_text: "Wed Jul 09 07:30:00 2026 CST".to_string(),
        date_style: "Postgres, MDY".to_string(),
        time_zone: "UTC".to_string(),
    };

    assert!(matches!(
        encode_temporal_text(&temporal_text),
        Err(PostgresError::Data(_))
    ));

    let mut out = BytesMut::new();
    for (oid, fmt) in [
        (TIMESTAMPTZ_OID, Format::Text),
        (TIMESTAMPTZ_OID, Format::Binary),
        (TIMESTAMP_OID, Format::Text),
        (DATE_OID, Format::Binary),
        (TIME_OID, Format::Text),
    ] {
        assert!(
            matches!(
                encode_value(&temporal_text, oid, fmt, &mut out, &ctx),
                Err(PostgresError::Data(_))
            ),
            "expected Data error for oid={oid}, fmt={fmt:?}"
        );
    }

    let catalog = TypeCatalog::new_with_builtins();
    let param = Parameter::with_oid(temporal_text.clone(), TIMESTAMPTZ_OID).unwrap();
    assert!(matches!(
        encode_bind(
            "",
            "",
            std::slice::from_ref(&param),
            &[TIMESTAMPTZ_OID],
            &[Format::Text],
            &[Format::Text],
            &ctx,
            &catalog,
            &mut out,
        ),
        Err(PostgresError::Data(_))
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let query_frames = Arc::new(AtomicUsize::new(0));
    let qframes_srv = Arc::clone(&query_frames);

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut len_buf = [0u8; 4];
        sock.read_exact(&mut len_buf).await.unwrap();
        let len = i32::from_be_bytes(len_buf) as usize;
        let mut rest = vec![0u8; len - 4];
        sock.read_exact(&mut rest).await.unwrap();

        let frame_s = |tag: u8, body: &[u8]| {
            let mut v = Vec::with_capacity(5 + body.len());
            v.push(tag);
            v.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
            v.extend_from_slice(body);
            v
        };
        let mut handshake = Vec::new();
        handshake.extend_from_slice(&frame_s(b'R', &0_i32.to_be_bytes()));
        handshake.extend_from_slice(&frame_s(b'S', b"DateStyle\0Postgres, MDY\0"));
        handshake.extend_from_slice(&frame_s(b'S', b"TimeZone\0UTC\0"));
        handshake.extend_from_slice(&frame_s(b'K', &[0, 0, 1, 1, 0, 0, 2, 2]));
        handshake.extend_from_slice(&frame_s(b'Z', b"I"));
        sock.write_all(&handshake).await.unwrap();

        let mut hdr = [0u8; 5];
        sock.read_exact(&mut hdr).await.unwrap();
        let tag = hdr[0];
        let flen = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; flen - 4];
        sock.read_exact(&mut body).await.unwrap();
        if tag != b'X' {
            qframes_srv.fetch_add(1, Ordering::SeqCst);
        }
        assert_eq!(tag, b'X');
    });

    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.channel_binding = "disable".to_string();
    raw.connect_timeout = Duration::from_secs(2).as_secs_f64();
    let mut session = SessionHandle::new(raw).unwrap();
    let conn = session.connect().await.unwrap();

    let err_q = conn
        .query("SELECT $1::timestamptz", std::slice::from_ref(&param), None)
        .await
        .unwrap_err();
    assert!(matches!(err_q, PostgresError::Data(_)));

    let batch = PipelineBatch::new(vec![PipelineOperation::with_parameters(
        "SELECT $1::timestamptz",
        vec![Parameter::new(temporal_text)],
    )]);
    let err_p = conn.pipeline_results(&batch).await.unwrap_err();
    assert_eq!(err_p.failing_index, 0);
    assert!(matches!(err_p.first_error, PostgresError::Data(_)));
    assert!(err_p.outcomes.is_empty());

    session.close().await.unwrap();
    srv.await.unwrap();
    assert_eq!(query_frames.load(Ordering::SeqCst), 0);
}
