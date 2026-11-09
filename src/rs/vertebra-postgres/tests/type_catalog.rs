//! Integration tests for `TypeCatalog`, transitive `pg_catalog` discovery, and format negotiation (`ch11:t01`).

extern crate _postgres as vertebra_postgres;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::codecs::{
    CodecContext, decode_value_with_catalog, encode_value_with_catalog, format_for_oid_with_catalog,
};
use vertebra_postgres::config::RawPostgresOptions;
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Format, PgValue};
use vertebra_postgres::session::{ExchangeState, SessionHandle};
use vertebra_postgres::types::{CompositeField, TypeCatalog, TypeInfo, TypeKind};

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
    out.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn catalog_row_desc() -> Vec<u8> {
    let cols = [
        ("oid", 26u32),
        ("nspname", 19),
        ("typname", 19),
        ("typtype", 25),
        ("typdelim", 25),
        ("typelem", 26),
        ("typbasetype", 26),
        ("extname", 19),
        ("attname", 19),
        ("atttypid", 26),
        ("atttypmod", 23),
        ("attnum", 21),
        ("attisdropped", 16),
    ];
    let mut body = BytesMut::new();
    body.put_i16(cols.len() as i16);
    for (name, oid) in cols {
        body.put_slice(name.as_bytes());
        body.put_u8(0);
        body.put_u32(0);
        body.put_i16(0);
        body.put_u32(oid);
        body.put_i16(-1);
        body.put_i32(-1);
        body.put_i16(Format::Text.code());
    }
    frame(b'T', &body)
}

fn text_row(cells: &[Option<&str>]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(cells.len() as i16);
    for cell in cells {
        match cell {
            None => body.put_i32(-1),
            Some(s) => {
                body.put_i32(s.len() as i32);
                body.put_slice(s.as_bytes());
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

async fn read_startup(sock: &mut tokio::net::TcpStream) {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await.unwrap();
}

async fn read_query_frame(sock: &mut tokio::net::TcpStream) -> String {
    let mut hdr = [0u8; 5];
    sock.read_exact(&mut hdr).await.unwrap();
    assert_eq!(hdr[0], b'Q');
    let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await.unwrap();
    assert_eq!(body.pop(), Some(0));
    String::from_utf8(body).unwrap()
}

fn loopback_handle(port: u16) -> SessionHandle {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "alice", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    SessionHandle::new(raw).unwrap()
}

#[test]
fn builtin_scalars_arrays_domains_and_unknown_fallback() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();

    let int4 = catalog.type_info(23).unwrap();
    assert_eq!(int4.name, "int4");
    assert_eq!(int4.kind, TypeKind::Scalar);
    assert_eq!(int4.delimiter, b',');

    let int4_arr = catalog.type_info(1007).unwrap();
    assert_eq!(
        int4_arr.kind,
        TypeKind::Array {
            element_oid: 23,
            delimiter: b','
        }
    );

    let box_arr = catalog.type_info(1020).unwrap();
    assert_eq!(
        box_arr.kind,
        TypeKind::Array {
            element_oid: 603,
            delimiter: b';'
        }
    );

    catalog.insert_custom_type(TypeInfo {
        oid: 41010,
        schema: "public".to_string(),
        name: "pos_int".to_string(),
        kind: TypeKind::Domain { base_oid: 23 },
        delimiter: b',',
    });
    assert_eq!(catalog.resolve_domain_base(41010).unwrap(), 23);
    assert_eq!(format_for_oid_with_catalog(41010, &catalog), Format::Binary);
    let val = decode_value_with_catalog(
        41010,
        Format::Binary,
        Some(&42_i32.to_be_bytes()),
        &ctx,
        &catalog,
    )
    .unwrap();
    assert_eq!(val, PgValue::Int4(42));
    let mut out = BytesMut::new();
    assert!(
        encode_value_with_catalog(
            &PgValue::Int4(42),
            41010,
            Format::Binary,
            &mut out,
            &ctx,
            &catalog
        )
        .unwrap()
    );
    assert_eq!(&out[..], &42_i32.to_be_bytes());

    let snap = catalog.snapshot_for_batch();
    assert_eq!(format_for_oid_with_catalog(99999, &snap), Format::Text);
    assert_eq!(
        decode_value_with_catalog(99999, Format::Text, Some(b"raw"), &ctx, &snap).unwrap(),
        PgValue::Text("raw".to_string())
    );
    assert!(matches!(
        decode_value_with_catalog(99999, Format::Binary, Some(b"\x00\x01"), &ctx, &snap),
        Err(PostgresError::Data(_))
    ));
}

#[tokio::test]
async fn transitive_discovery_dropped_columns_extension_binding_and_invalidation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        let q1 = read_query_frame(&mut sock).await;
        assert!(q1.contains("41002"));
        assert!(q1.contains("'vector'"));
        assert!(q1.contains("'halfvec'"));
        let mut resp1 = Vec::new();
        resp1.extend_from_slice(&catalog_row_desc());
        resp1.extend_from_slice(&text_row(&[
            Some("41002"),
            Some("public"),
            Some("order_comp"),
            Some("c"),
            Some(","),
            Some("0"),
            Some("0"),
            None,
            Some("id"),
            Some("23"),
            Some("-1"),
            Some("1"),
            Some("f"),
        ]));
        resp1.extend_from_slice(&text_row(&[
            Some("41002"),
            Some("public"),
            Some("order_comp"),
            Some("c"),
            Some(","),
            Some("0"),
            Some("0"),
            None,
            Some("........pg.dropped.2........"),
            Some("0"),
            Some("-1"),
            Some("2"),
            Some("t"),
        ]));
        resp1.extend_from_slice(&text_row(&[
            Some("41002"),
            Some("public"),
            Some("order_comp"),
            Some("c"),
            Some(","),
            Some("0"),
            Some("0"),
            None,
            Some("amounts"),
            Some("41001"),
            Some("-1"),
            Some("3"),
            Some("f"),
        ]));
        resp1.extend_from_slice(&text_row(&[
            Some("42001"),
            Some("public"),
            Some("vector"),
            Some("b"),
            Some(","),
            Some("0"),
            Some("0"),
            Some("vector"),
            None,
            None,
            None,
            None,
            None,
        ]));
        resp1.extend_from_slice(&text_row(&[
            Some("42002"),
            Some("public"),
            Some("halfvec"),
            Some("b"),
            Some(","),
            Some("0"),
            Some("0"),
            Some("vector"),
            None,
            None,
            None,
            None,
            None,
        ]));
        resp1.extend_from_slice(&cmd_complete("SELECT 5"));
        resp1.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp1).await.unwrap();

        let q2 = read_query_frame(&mut sock).await;
        assert!(q2.contains("41001"));
        let mut resp2 = Vec::new();
        resp2.extend_from_slice(&catalog_row_desc());
        resp2.extend_from_slice(&text_row(&[
            Some("41001"),
            Some("public"),
            Some("amount_arr_dom"),
            Some("d"),
            Some(","),
            Some("0"),
            Some("1231"),
            None,
            None,
            None,
            None,
            None,
            None,
        ]));
        resp2.extend_from_slice(&cmd_complete("SELECT 1"));
        resp2.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp2).await.unwrap();
    });

    let mut handle = loopback_handle(port);
    let conn = handle.connect().await.unwrap();
    conn.set_unnamed_statement_valid(true);

    let mut catalog = TypeCatalog::new_with_builtins();
    catalog
        .discover(conn, &[0, 41002], &["vector", "halfvec"])
        .await
        .unwrap();

    assert!(!conn.is_unnamed_statement_valid());
    let comp = catalog.type_info(41002).unwrap();
    assert_eq!(
        comp.kind,
        TypeKind::Composite {
            fields: vec![
                CompositeField {
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1
                },
                CompositeField {
                    name: "amounts".to_string(),
                    type_oid: 41001,
                    type_modifier: -1
                },
            ]
        }
    );
    assert_eq!(catalog.resolve_domain_base(41001).unwrap(), 1231);
    assert_eq!(catalog.lookup_extension_type("vector").unwrap().oid, 42001);
    assert_eq!(catalog.lookup_extension_type("halfvec").unwrap().oid, 42002);

    catalog.invalidate_oid(41002);
    assert!(catalog.type_info(41002).is_none());
    assert!(catalog.type_info(23).is_some());

    srv.await.unwrap();
}

#[tokio::test]
async fn non_idle_guard_and_cyclic_domain_or_oversized_catalog_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        let _ = read_query_frame(&mut sock).await;
        let mut resp = Vec::new();
        resp.extend_from_slice(&catalog_row_desc());
        resp.extend_from_slice(&text_row(&[
            Some("50001"),
            Some("public"),
            Some("dom_a"),
            Some("d"),
            Some(","),
            Some("0"),
            Some("50002"),
            None,
            None,
            None,
            None,
            None,
            None,
        ]));
        resp.extend_from_slice(&text_row(&[
            Some("50002"),
            Some("public"),
            Some("dom_b"),
            Some("d"),
            Some(","),
            Some("0"),
            Some("50001"),
            None,
            None,
            None,
            None,
            None,
            None,
        ]));
        resp.extend_from_slice(&cmd_complete("SELECT 2"));
        resp.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp).await.unwrap();
    });

    let mut handle = loopback_handle(port);
    let conn = handle.connect().await.unwrap();
    let mut catalog = TypeCatalog::new_with_builtins();

    conn.set_exchange_state(ExchangeState::PipelineActive);
    assert!(matches!(
        catalog.discover(conn, &[50001], &[]).await,
        Err(PostgresError::Interface(_))
    ));
    conn.set_exchange_state(ExchangeState::Idle);

    assert!(matches!(
        catalog.discover(conn, &[50001], &[]).await,
        Err(PostgresError::Data(_))
    ));
    srv.await.unwrap();
}
