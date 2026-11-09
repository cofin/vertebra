//! Integration tests for PostgreSQL `JSON` (`OID 114`) and version-validated `JSONB` (`OID 3802`) codecs (`ch10:t04`).

extern crate _postgres as vertebra_postgres;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::codecs::CodecContext;
use vertebra_postgres::codecs::json::{
    JSON_OID, JSONB_OID, decode_json_binary, decode_json_text, decode_jsonb_binary,
    encode_json_binary, encode_jsonb_binary, validate_json_bytes,
};
use vertebra_postgres::codecs::{decode_value, encode_value, format_for_oid};
use vertebra_postgres::config::RawPostgresOptions;
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Format, PgValue};
use vertebra_postgres::session::SessionHandle;

#[test]
fn sql_null_vs_json_null_vs_json_string_null_stay_distinct() {
    let ctx = CodecContext::default();
    assert_eq!(format_for_oid(JSON_OID), Format::Binary);
    assert_eq!(format_for_oid(JSONB_OID), Format::Binary);

    assert_eq!(
        decode_value(JSONB_OID, Format::Binary, None, &ctx).unwrap(),
        PgValue::Null
    );
    assert_eq!(
        decode_value(JSONB_OID, Format::Binary, Some(b"\x01null"), &ctx).unwrap(),
        PgValue::Json(b"null".to_vec())
    );
    assert_eq!(
        decode_value(JSONB_OID, Format::Binary, Some(b"\x01\"null\""), &ctx).unwrap(),
        PgValue::Json(b"\"null\"".to_vec())
    );
    assert_eq!(
        decode_value(JSONB_OID, Format::Binary, Some(b"\x01[null,{},[]]"), &ctx).unwrap(),
        PgValue::Json(b"[null,{},[]]".to_vec())
    );

    let mut out_jsonb = BytesMut::new();
    assert!(
        encode_value(
            &PgValue::Json(b"null".to_vec()),
            JSONB_OID,
            Format::Binary,
            &mut out_jsonb,
            &ctx
        )
        .unwrap()
    );
    assert_eq!(&out_jsonb[..], b"\x01null");

    let mut out_json = BytesMut::new();
    assert!(
        encode_value(
            &PgValue::Json(b"null".to_vec()),
            JSON_OID,
            Format::Binary,
            &mut out_json,
            &ctx
        )
        .unwrap()
    );
    assert_eq!(&out_json[..], b"null");
}

#[test]
fn large_integers_beyond_f64_and_unicode_surrogate_pairs_preserve_exact_bytes() {
    let ctx = CodecContext::default();
    let payload = br#"{"u64":18446744073709551615,"big":99999999999999999999999999999,"s":"\uD83D\uDE80 \"ok\""}"#;
    let mut wire = Vec::with_capacity(1 + payload.len());
    wire.push(0x01);
    wire.extend_from_slice(payload);

    let decoded = decode_jsonb_binary(&wire, &ctx).unwrap();
    assert_eq!(decoded, payload);
    assert_eq!(decode_json_binary(payload, &ctx).unwrap(), payload);
    assert_eq!(decode_json_text(payload, &ctx).unwrap(), payload);
    assert_eq!(
        validate_json_bytes(payload, 32).unwrap(),
        std::str::from_utf8(payload).unwrap()
    );
}

#[test]
fn invalid_jsonb_version_and_truncated_or_malformed_json_fail() {
    let ctx = CodecContext::default();

    for bad_jsonb in [
        &b""[..],
        &b"\x01"[..],
        &b"\x00{}"[..],
        &b"\x02{\"a\":1}"[..],
        &b"\x01\xff"[..],
        &b"\x01{\"s\":\"\\u0000\"}"[..],
    ] {
        assert!(
            matches!(
                decode_jsonb_binary(bad_jsonb, &ctx),
                Err(PostgresError::Data(_))
            ),
            "expected error for JSONB payload {bad_jsonb:?}"
        );
    }

    for bad_json in [
        &b""[..],
        &b"{\"a\":01}"[..],
        &b"{\"a\":NaN}"[..],
        &b"{\"a\":Infinity}"[..],
        &b"[1,]"[..],
        &b"{\"a\":1,}"[..],
        &b"\"\\uD800\""[..],
        &b"\"\\uDC00\""[..],
        &b"{\"a\":1} trailing"[..],
    ] {
        assert!(
            matches!(
                decode_json_binary(bad_json, &ctx),
                Err(PostgresError::Data(_))
            ),
            "expected error for JSON payload {bad_json:?}"
        );
    }

    let deep = format!("{}{}", "[".repeat(64), "]".repeat(64));
    let shallow_ctx = CodecContext {
        max_nesting_depth: 32,
        ..Default::default()
    };
    assert!(matches!(
        decode_json_binary(deep.as_bytes(), &shallow_ctx),
        Err(PostgresError::Data(_))
    ));

    let mut out = BytesMut::new();
    assert!(matches!(
        encode_jsonb_binary(b"{\"s\":\"\\u0000\"}", &mut out, &ctx),
        Err(PostgresError::Data(_))
    ));
    assert!(encode_json_binary(b"{\"s\":\"\\u0000\"}", &mut out, &ctx).is_ok());
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
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
async fn bytewise_fragmented_datarow_over_loopback_reassembles_json_and_jsonb() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let json_doc = format!(
        "{{\"items\":[{}],\"unicode\":\" café 🚀 \"}}",
        (0..200)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let json_bytes = json_doc.as_bytes().to_vec();
    let mut jsonb_bytes = Vec::with_capacity(1 + json_bytes.len());
    jsonb_bytes.push(0x01);
    jsonb_bytes.extend_from_slice(&json_bytes);

    let json_expected = json_bytes.clone();
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut len_buf = [0u8; 4];
        sock.read_exact(&mut len_buf).await.unwrap();
        let len = i32::from_be_bytes(len_buf) as usize;
        let mut startup = vec![0u8; len - 4];
        sock.read_exact(&mut startup).await.unwrap();

        let mut handshake = Vec::new();
        handshake.extend_from_slice(&frame(b'R', &0i32.to_be_bytes()));
        handshake.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
        handshake.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&handshake).await.unwrap();

        let mut qhdr = [0u8; 5];
        sock.read_exact(&mut qhdr).await.unwrap();
        let qlen = i32::from_be_bytes([qhdr[1], qhdr[2], qhdr[3], qhdr[4]]) as usize;
        let mut qbody = vec![0u8; qlen - 4];
        sock.read_exact(&mut qbody).await.unwrap();

        let mut resp = Vec::new();
        resp.extend_from_slice(&row_desc(&[
            ("j", JSON_OID, 1),
            ("jb", JSONB_OID, 1),
            ("jb_null", JSONB_OID, 1),
        ]));
        resp.extend_from_slice(&data_row(&[
            Some(&json_bytes),
            Some(&jsonb_bytes),
            Some(b"\x01null"),
        ]));
        resp.extend_from_slice(&cmd_complete("SELECT 1"));
        resp.extend_from_slice(&frame(b'Z', b"I"));

        for chunk in resp.chunks(1) {
            sock.write_all(chunk).await.unwrap();
        }
    });

    let mut raw = RawPostgresOptions::new("127.0.0.1", "alice", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    let mut handle = SessionHandle::new(raw).unwrap();
    let conn = handle.connect().await.unwrap();

    let results = conn.simple_query("SELECT j, jb, jb_null").await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].rows[0],
        vec![
            PgValue::Json(json_expected.clone()),
            PgValue::Json(json_expected),
            PgValue::Json(b"null".to_vec()),
        ]
    );

    server.await.unwrap();
}
