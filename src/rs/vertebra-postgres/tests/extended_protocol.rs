//! Integration tests for extended-query execution (`Parse`/`Describe`/`Bind`/`Execute`/`Close`/`Sync`),
//! typed `NULL`s, warm mixed formats, and streaming row sinks (`ch12:t03`).

extern crate _postgres as vertebra_postgres;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::config::RawPostgresOptions;
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::extended::{Parameter, PreparedDescription};
use vertebra_postgres::result::{
    Column, Format, PgValue, TemporalKind, TransactionStatus, VectorKind,
};
use vertebra_postgres::session::SessionHandle;
use vertebra_postgres::types::{CompositeField, TypeInfo, TypeKind};

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth_ok_and_ready() -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&frame(b'R', &0i32.to_be_bytes()));
    out.extend_from_slice(&frame(b'S', b"client_encoding\0UTF8\0"));
    out.extend_from_slice(&frame(b'S', b"DateStyle\0ISO, MDY\0"));
    out.extend_from_slice(&frame(b'S', b"TimeZone\0UTC\0"));
    out.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn param_desc(oids: &[u32]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(oids.len() as i16);
    for &oid in oids {
        body.put_u32(oid);
    }
    frame(b't', &body)
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

fn err_response(code: &str, msg: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(msg.as_bytes());
    body.push(0);
    body.push(0);
    frame(b'E', &body)
}

async fn read_startup(sock: &mut tokio::net::TcpStream) {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await.unwrap();
}

async fn read_frontend_frames_until_sync(sock: &mut tokio::net::TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut frames = Vec::new();
    loop {
        let mut hdr = [0u8; 5];
        sock.read_exact(&mut hdr).await.unwrap();
        let tag = hdr[0];
        let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await.unwrap();
        frames.push((tag, body));
        if tag == b'S' || tag == b'Q' {
            break;
        }
    }
    frames
}

fn loopback_handle(port: u16) -> SessionHandle {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "alice", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    SessionHandle::new(raw).unwrap()
}

#[tokio::test]
async fn cold_vacuum_and_custom_query_plus_warm_mixed_formats() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        let vacuum_group = read_frontend_frames_until_sync(&mut sock).await;
        let tags: Vec<u8> = vacuum_group.iter().map(|(t, _)| *t).collect();
        assert_eq!(tags, vec![b'P', b'D', b'B', b'D', b'E', b'C', b'S']);
        let mut vac_resp = Vec::new();
        vac_resp.extend_from_slice(&frame(b'1', b""));
        vac_resp.extend_from_slice(&param_desc(&[]));
        vac_resp.extend_from_slice(&frame(b'n', b""));
        vac_resp.extend_from_slice(&frame(b'2', b""));
        vac_resp.extend_from_slice(&frame(b'n', b""));
        vac_resp.extend_from_slice(&cmd_complete("VACUUM"));
        vac_resp.extend_from_slice(&frame(b'3', b""));
        vac_resp.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&vac_resp).await.unwrap();

        let warm_group = read_frontend_frames_until_sync(&mut sock).await;
        let warm_tags: Vec<u8> = warm_group.iter().map(|(t, _)| *t).collect();
        assert_eq!(warm_tags, vec![b'P', b'D', b'B', b'D', b'E', b'C', b'S']);
        let mut warm_resp = Vec::new();
        warm_resp.extend_from_slice(&frame(b'1', b""));
        warm_resp.extend_from_slice(&param_desc(&[23, 3802]));
        warm_resp.extend_from_slice(&row_desc(&[
            ("id", 23, Format::Text.code()),
            ("u", 2950, Format::Text.code()),
            ("v", 42001, Format::Text.code()),
            ("custom_txt", 59999, Format::Text.code()),
        ]));
        warm_resp.extend_from_slice(&frame(b'2', b""));
        warm_resp.extend_from_slice(&row_desc(&[
            ("id", 23, Format::Binary.code()),
            ("u", 2950, Format::Binary.code()),
            ("v", 42001, Format::Binary.code()),
            ("custom_txt", 59999, Format::Text.code()),
        ]));
        let mut vec_bytes = BytesMut::new();
        vec_bytes.put_u16(2);
        vec_bytes.put_u16(0);
        vec_bytes.put_f32(1.5);
        vec_bytes.put_f32(-2.0);
        warm_resp.extend_from_slice(&data_row(&[
            Some(&99i32.to_be_bytes()),
            Some(&[7u8; 16]),
            Some(&vec_bytes),
            Some(b"untyped-59999"),
        ]));
        warm_resp.extend_from_slice(&cmd_complete("SELECT 1"));
        warm_resp.extend_from_slice(&frame(b'3', b""));
        warm_resp.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&warm_resp).await.unwrap();
    });

    let mut handle = loopback_handle(port);
    let conn = handle.connect().await.unwrap();
    conn.type_catalog_mut().insert_custom_type(TypeInfo {
        oid: 42001,
        schema: "public".to_string(),
        name: "vector".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "vector".to_string(),
        },
        delimiter: b',',
    });
    conn.type_catalog_mut().insert_custom_type(TypeInfo {
        oid: 41050,
        schema: "public".to_string(),
        name: "pair_t".to_string(),
        kind: TypeKind::Composite {
            fields: vec![CompositeField {
                name: "a".to_string(),
                type_oid: 23,
                type_modifier: -1,
            }],
        },
        delimiter: b',',
    });

    let vac = conn.query("VACUUM", &[], None).await.unwrap();
    assert_eq!(vac.command_tag, "VACUUM");
    assert_eq!(vac.affected_rows, 0);
    assert_eq!(vac.transaction_status, TransactionStatus::Idle);

    let warm_desc = PreparedDescription {
        parameter_oids: vec![23, 3802],
        result_columns: vec![
            Column::try_new("id", 23, Format::Text, -1, 0, 0).unwrap(),
            Column::try_new("u", 2950, Format::Text, -1, 0, 0).unwrap(),
            Column::try_new("v", 42001, Format::Text, -1, 0, 0).unwrap(),
            Column::try_new("custom_txt", 59999, Format::Text, -1, 0, 0).unwrap(),
        ],
    };
    let warm_res = conn
        .query_with_description(
            "SELECT $1, $2",
            &[
                Parameter::with_oid(PgValue::Int4(99), 23).unwrap(),
                Parameter::with_oid(PgValue::Null, 3802).unwrap(),
            ],
            None,
            &warm_desc,
        )
        .await
        .unwrap();
    assert_eq!(warm_res.columns[0].format, Format::Binary);
    assert_eq!(warm_res.columns[1].format, Format::Binary);
    assert_eq!(warm_res.columns[2].format, Format::Binary);
    assert_eq!(warm_res.columns[3].format, Format::Text);
    assert_eq!(warm_res.columns[3].type_oid, 59999);
    assert_eq!(
        warm_res.rows[0],
        vec![
            PgValue::Int4(99),
            PgValue::Uuid([7u8; 16]),
            PgValue::Vector {
                values: vec![1.5, -2.0],
                kind: VectorKind::Vector,
            },
            PgValue::Text("untyped-59999".to_string()),
        ]
    );

    srv.await.unwrap();
}

#[tokio::test]
async fn temporal_text_oid_conflict_and_ambiguous_null_or_sink_draining() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        let _ = read_frontend_frames_until_sync(&mut sock).await;
        let mut resp_42p18 = Vec::new();
        resp_42p18.extend_from_slice(&err_response(
            "42P18",
            "could not determine data type of parameter $1",
        ));
        resp_42p18.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp_42p18).await.unwrap();

        let _ = read_frontend_frames_until_sync(&mut sock).await;
        let mut resp_sink = Vec::new();
        resp_sink.extend_from_slice(&frame(b'1', b""));
        resp_sink.extend_from_slice(&param_desc(&[]));
        resp_sink.extend_from_slice(&row_desc(&[("n", 23, Format::Text.code())]));
        resp_sink.extend_from_slice(&frame(b'2', b""));
        resp_sink.extend_from_slice(&row_desc(&[("n", 23, Format::Binary.code())]));
        resp_sink.extend_from_slice(&data_row(&[Some(&1i32.to_be_bytes())]));
        resp_sink.extend_from_slice(&data_row(&[Some(&2i32.to_be_bytes())]));
        resp_sink.extend_from_slice(&data_row(&[Some(&3i32.to_be_bytes())]));
        resp_sink.extend_from_slice(&cmd_complete("SELECT 3"));
        resp_sink.extend_from_slice(&frame(b'3', b""));
        resp_sink.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp_sink).await.unwrap();
    });

    let mut handle = loopback_handle(port);
    let conn = handle.connect().await.unwrap();

    let wire_text_param = Parameter::new(PgValue::TemporalText {
        kind: TemporalKind::TimestampTz,
        wire_text: "2026-07-01 12:00:00+00".to_string(),
        date_style: "ISO, MDY".to_string(),
        time_zone: "UTC".to_string(),
    });
    assert!(matches!(
        conn.query("SELECT $1", &[wire_text_param], None).await,
        Err(PostgresError::Data(_))
    ));

    assert!(matches!(
        conn.query(
            "SELECT $1",
            &[Parameter::with_oid(PgValue::Int4(1), 23).unwrap()],
            Some(&[20])
        )
        .await,
        Err(PostgresError::Data(_))
    ));

    let err_42p18 = conn
        .query("SELECT $1", &[Parameter::new(PgValue::Null)], None)
        .await
        .unwrap_err();
    assert_eq!(err_42p18.sqlstate(), Some("42P18"));
    assert!(!conn.is_closed());

    let mut seen_rows = 0usize;
    let sink_err = conn
        .query_with_row_sink(
            "SELECT n FROM generate_series(1,3) n",
            &[],
            &[Format::Binary],
            |cols| {
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].format, Format::Binary);
                Ok(())
            },
            |_row| {
                seen_rows += 1;
                if seen_rows == 2 {
                    Err(PostgresError::Data("stop on row 2".to_string()))
                } else {
                    Ok(())
                }
            },
        )
        .await
        .unwrap_err();
    assert!(sink_err.to_string().contains("stop on row 2"));
    assert!(!conn.is_closed());
    assert_eq!(conn.transaction_status(), TransactionStatus::Idle);

    srv.await.unwrap();
}
