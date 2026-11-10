//! Integration and loopback wire tests for typed Arrow-to-`COPY` field import
//! (`PostgresArrowImportPlan`, `parse_postgres_qualified_table`, `decode_bounded_ipc_batches`,
//! `SessionHandle::copy_from_arrow_with_generation_as`, `CopyFromStream::write_arrow_ipc`,
//! and `vertebra.db.adapters.postgres.arrow`, `ch20:t01`).

extern crate _postgres as vertebra_postgres;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::{
    ArrayRef, BinaryArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array,
    Int8Array, Int32Array, ListArray, RecordBatch, StringArray, StructArray, Time32SecondArray,
    Time64NanosecondArray, TimestampNanosecondArray, UInt16Array, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer, i256};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use bytes::{BufMut, BytesMut};
use half::f16;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use vertebra_postgres::arrow::{
    DEFAULT_ARROW_BATCH_BYTES, DEFAULT_ARROW_BATCH_ROWS, MAX_ARROW_COPY_BATCH_BYTES,
    MAX_ARROW_COPY_CALL_BYTES, POSTGRES_ARROW_VERSION, POSTGRES_ARROW_VERSION_KEY,
    POSTGRES_FIELD_ENCODING_KEY, POSTGRES_FIELD_OID_KEY, POSTGRES_FIELD_TYPE_NAME_KEY,
    POSTGRES_FIELD_TYPMOD_KEY, PostgresArrowBatchBuilder, PostgresArrowImportPlan,
    PostgresArrowSchema, decode_bounded_ipc_batches, parse_postgres_qualified_table,
};
use vertebra_postgres::codecs::numeric::decode_numeric_text;
use vertebra_postgres::codecs::{
    BIT_OID, BOOL_OID, BOX_OID, BPCHAR_OID, BYTEA_OID, CID_OID, CodecContext, DATE_OID, FLOAT4_OID,
    FLOAT8_OID, INT2_OID, INT4_OID, INT8_OID, INTERVAL_OID, JSON_OID, JSONB_OID, NAME_OID,
    NUMERIC_OID, OID_OID, RECORD_OID, REGPROC_OID, TEXT_OID, TIME_OID, TIMESTAMP_OID,
    TIMESTAMPTZ_OID, UNKNOWN_OID, UUID_OID, VARBIT_OID, VARCHAR_OID, XID_OID, XML_OID,
    decode_value_with_catalog,
};
use vertebra_postgres::config::{PostgresConfig, RawPostgresOptions};
use vertebra_postgres::copy::{BinaryCopyDecoder, BinaryCopyEncoder, PGCOPY_SIGNATURE};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{
    Column, Format, PG_DATE_NEG_INF, PG_DATE_POS_INF, PG_INTERVAL_NEG_INF, PG_INTERVAL_POS_INF,
    PG_TS_NEG_INF, PG_TS_POS_INF, PgNumeric, PgValue, TemporalKind, TransactionStatus, VectorKind,
};
use vertebra_postgres::session::SessionHandle;
use vertebra_postgres::session::cancel::CANCEL_REQUEST_CODE;
use vertebra_postgres::transaction::TransactionOwner;
use vertebra_postgres::types::{CompositeField, TypeCatalog, TypeInfo, TypeKind};

const CUSTOM_ENUM_OID: u32 = 41_001;
const CUSTOM_ENUM_ARRAY_OID: u32 = 41_002;
const CUSTOM_RANGE_OID: u32 = 41_003;
const CUSTOM_DOMAIN_INNER_OID: u32 = 41_004;
const CUSTOM_DOMAIN_OUTER_OID: u32 = 41_005;
const CUSTOM_COMPOSITE_OID: u32 = 41_006;
const CUSTOM_RANGE_COMP_OID: u32 = 41_007;
const CUSTOM_VECTOR_OID: u32 = 41_008;
const CUSTOM_HALFVEC_OID: u32 = 41_009;
const INT4_ARRAY_OID: u32 = 1007;

fn build_rich_catalog() -> TypeCatalog {
    let mut cat = TypeCatalog::new_with_builtins();
    cat.insert_custom_type(TypeInfo {
        oid: CUSTOM_ENUM_OID,
        schema: "public".to_string(),
        name: "order_state".to_string(),
        kind: TypeKind::Enum,
        delimiter: b',',
    });
    cat.insert_custom_type(TypeInfo {
        oid: CUSTOM_ENUM_ARRAY_OID,
        schema: "public".to_string(),
        name: "_order_state".to_string(),
        kind: TypeKind::Array {
            element_oid: CUSTOM_ENUM_OID,
            delimiter: b',',
        },
        delimiter: b',',
    });
    cat.insert_custom_type(TypeInfo {
        oid: CUSTOM_RANGE_OID,
        schema: "pg_catalog".to_string(),
        name: "int4range".to_string(),
        kind: TypeKind::Range,
        delimiter: b',',
    });
    cat.insert_custom_type(TypeInfo {
        oid: CUSTOM_VECTOR_OID,
        schema: "public".to_string(),
        name: "vector".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "vector".to_string(),
        },
        delimiter: b',',
    });
    cat.insert_custom_type(TypeInfo {
        oid: CUSTOM_HALFVEC_OID,
        schema: "public".to_string(),
        name: "halfvec".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "halfvec".to_string(),
        },
        delimiter: b',',
    });
    cat.insert_custom_type(TypeInfo {
        oid: CUSTOM_COMPOSITE_OID,
        schema: "public".to_string(),
        name: "shipping_addr".to_string(),
        kind: TypeKind::Composite {
            fields: vec![
                CompositeField {
                    name: "city".to_string(),
                    type_oid: TEXT_OID,
                    type_modifier: -1,
                },
                CompositeField {
                    name: "zip".to_string(),
                    type_oid: INT4_OID,
                    type_modifier: -1,
                },
            ],
        },
        delimiter: b',',
    });
    cat.insert_custom_type(TypeInfo {
        oid: CUSTOM_DOMAIN_INNER_OID,
        schema: "public".to_string(),
        name: "addr_dom_inner".to_string(),
        kind: TypeKind::Domain {
            base_oid: CUSTOM_COMPOSITE_OID,
        },
        delimiter: b',',
    });
    cat.insert_custom_type(TypeInfo {
        oid: CUSTOM_DOMAIN_OUTER_OID,
        schema: "public".to_string(),
        name: "addr_dom_outer".to_string(),
        kind: TypeKind::Domain {
            base_oid: CUSTOM_DOMAIN_INNER_OID,
        },
        delimiter: b',',
    });
    cat.insert_custom_type(TypeInfo {
        oid: CUSTOM_RANGE_COMP_OID,
        schema: "public".to_string(),
        name: "window_pair".to_string(),
        kind: TypeKind::Composite {
            fields: vec![CompositeField {
                name: "span".to_string(),
                type_oid: CUSTOM_RANGE_OID,
                type_modifier: -1,
            }],
        },
        delimiter: b',',
    });
    cat
}

fn col(name: &str, oid: u32, typmod: i32) -> Column {
    Column::try_new(name, oid, Format::Binary, typmod, 0, 0).expect("valid column")
}

fn addr_comp(city: PgValue, zip: PgValue) -> PgValue {
    PgValue::Composite {
        type_oid: CUSTOM_COMPOSITE_OID,
        field_names: vec!["city".to_string(), "zip".to_string()],
        field_oids: vec![TEXT_OID, INT4_OID],
        values: vec![city, zip],
    }
}

fn serialize_ipc_batches(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Vec<u8> {
    let mut writer = StreamWriter::try_new(Vec::new(), schema.as_ref()).expect("ipc writer");
    for batch in batches {
        writer.write(batch).expect("write batch");
    }
    writer.finish().expect("finish ipc");
    writer.into_inner().expect("ipc bytes")
}

fn count_pgcopy_headers(wire: &[u8]) -> usize {
    if wire.len() < PGCOPY_SIGNATURE.len() {
        return 0;
    }
    wire.windows(PGCOPY_SIGNATURE.len())
        .filter(|w| *w == PGCOPY_SIGNATURE)
        .count()
}

fn decode_pgcopy_values(
    wire: &[u8],
    columns: &[Column],
    ctx: &CodecContext,
    catalog: &TypeCatalog,
) -> Vec<Vec<PgValue>> {
    let mut dec = BinaryCopyDecoder::new(columns.len());
    dec.push(wire).expect("push pgcopy wire");
    let mut out = Vec::new();
    while let Some(raw_fields) = dec.next_row().expect("decode row") {
        assert_eq!(raw_fields.len(), columns.len());
        let mut row = Vec::with_capacity(columns.len());
        for (cell, column) in raw_fields.iter().zip(columns.iter()) {
            match cell {
                None => row.push(PgValue::Null),
                Some(bytes) => {
                    if column.type_oid == CUSTOM_ENUM_OID {
                        let s = std::str::from_utf8(bytes).expect("enum utf8").to_owned();
                        row.push(PgValue::Text(s));
                    } else {
                        let v = decode_value_with_catalog(
                            column.type_oid,
                            Format::Binary,
                            Some(bytes.as_ref()),
                            ctx,
                            catalog,
                        )
                        .expect("decode binary value");
                        row.push(v);
                    }
                }
            }
        }
        out.push(row);
    }
    let count = dec.finish().expect("clean -1i16 trailer");
    assert_eq!(count as usize, out.len());
    out
}

#[test]
fn parse_qualified_table_and_bounded_ipc_caps_enforce_literal_and_quoted_contracts() {
    assert_eq!(MAX_ARROW_COPY_CALL_BYTES, 16 * 1024 * 1024);
    assert_eq!(MAX_ARROW_COPY_BATCH_BYTES, 8 * 1024 * 1024);

    assert_eq!(
        parse_postgres_qualified_table("odd.table", Some("analytics")).unwrap(),
        (Some("analytics".to_string()), "odd.table".to_string())
    );
    assert_eq!(
        parse_postgres_qualified_table("with\"quote.and.dots", Some("my \"schema\"")).unwrap(),
        (
            Some("my \"schema\"".to_string()),
            "with\"quote.and.dots".to_string()
        )
    );
    assert_eq!(
        parse_postgres_qualified_table("\"my \"\"schema\"\"\".\"odd.table\"", None).unwrap(),
        (Some("my \"schema\"".to_string()), "odd.table".to_string())
    );
    assert_eq!(
        parse_postgres_qualified_table("events", None).unwrap(),
        (None, "events".to_string())
    );
    assert_eq!(
        parse_postgres_qualified_table("analytics.events", None).unwrap(),
        (Some("analytics".to_string()), "events".to_string())
    );

    for bad in [
        "",
        ".a",
        "a.",
        "a..b",
        "a.b.c",
        "\"unclosed",
        "\"my_schema\".\"unclosed",
        "\"\"",
        "\"a\".\"\"",
        "\"a\"extra.b",
        "mid\"quote",
        "bad\0table",
        "has space",
    ] {
        assert!(
            matches!(
                parse_postgres_qualified_table(bad, None),
                Err(PostgresError::Data(_))
            ),
            "expected DataError for table_name={bad:?}"
        );
    }
    for (tbl, sch) in [
        ("", Some("analytics")),
        ("events", Some("")),
        ("bad\0tbl", Some("analytics")),
        ("events", Some("bad\0sch")),
    ] {
        assert!(matches!(
            parse_postgres_qualified_table(tbl, sch),
            Err(PostgresError::Data(_))
        ));
    }

    let oversized = vec![0u8; MAX_ARROW_COPY_CALL_BYTES + 1];
    assert!(matches!(
        decode_bounded_ipc_batches(&oversized),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        decode_bounded_ipc_batches(b"not-valid-arrow-ipc"),
        Err(PostgresError::Data(_))
    ));
}

#[test]
fn full_post_ch19_lossless_roundtrip_and_single_pgcopy_header_across_batches() {
    let catalog = build_rich_catalog();
    let ctx = CodecContext::default();

    let source_columns = vec![
        col("c_bool", BOOL_OID, -1),
        col("c_i2", INT2_OID, -1),
        col("c_i4", INT4_OID, -1),
        col("c_i8", INT8_OID, -1),
        col("c_f4", FLOAT4_OID, -1),
        col("c_f8", FLOAT8_OID, -1),
        col("c_bytea", BYTEA_OID, -1),
        col("c_uuid", UUID_OID, -1),
        col("c_json", JSON_OID, -1),
        col("c_jsonb", JSONB_OID, -1),
        col("c_num", NUMERIC_OID, -1),
        col("c_date", DATE_OID, -1),
        col("c_time", TIME_OID, -1),
        col("c_ts", TIMESTAMP_OID, -1),
        col("c_tstz", TIMESTAMPTZ_OID, -1),
        col("c_iv", INTERVAL_OID, -1),
        col("c_enum", CUSTOM_ENUM_OID, -1),
        col("c_oid", OID_OID, -1),
        col("c_regproc", REGPROC_OID, -1),
        col("c_xid", XID_OID, -1),
        col("c_cid", CID_OID, -1),
        col("c_xml", XML_OID, -1),
        col("c_unk_text", UNKNOWN_OID, -1),
        col("c_unk_vchar", UNKNOWN_OID, -1),
        col("c_unk_bpchar", UNKNOWN_OID, -1),
        col("c_unk_name", UNKNOWN_OID, -1),
        col("c_unk_xml", UNKNOWN_OID, -1),
        col("c_dom_comp", CUSTOM_DOMAIN_OUTER_OID, -1),
        col("c_arr2d", INT4_ARRAY_OID, -1),
        col("c_comp", CUSTOM_COMPOSITE_OID, -1),
        col("c_vec3", CUSTOM_VECTOR_OID, 3),
        col("c_hvec", CUSTOM_HALFVEC_OID, -1),
        col("c_vbit", VARBIT_OID, -1),
        col("c_bit", BIT_OID, 3),
    ];

    let mut target_columns = source_columns.clone();
    target_columns[22] = col("c_unk_text", TEXT_OID, -1);
    target_columns[23] = col("c_unk_vchar", VARCHAR_OID, -1);
    target_columns[24] = col("c_unk_bpchar", BPCHAR_OID, -1);
    target_columns[25] = col("c_unk_name", NAME_OID, -1);
    target_columns[26] = col("c_unk_xml", XML_OID, -1);

    let schema = PostgresArrowSchema::from_columns(&source_columns, &catalog).unwrap();
    assert_eq!(
        schema
            .metadata()
            .get(POSTGRES_ARROW_VERSION_KEY)
            .map(String::as_str),
        Some(POSTGRES_ARROW_VERSION)
    );

    let row1 = vec![
        PgValue::Bool(true),
        PgValue::Int2(-32_000),
        PgValue::Int4(123_456),
        PgValue::Int8(-9_876_543_210),
        PgValue::Float4(-0.0_f32),
        PgValue::Float8(f64::INFINITY),
        PgValue::Bytes(b"\x00\xffabc".to_vec()),
        PgValue::Uuid(*b"\x01\x23\x45\x67\x89\xab\xcd\xef\xfe\xdc\xba\x98\x76\x54\x32\x10"),
        PgValue::Json(b"{\"k\":  1}".to_vec()),
        PgValue::Json(b"{\"a\":null}".to_vec()),
        PgValue::Numeric(decode_numeric_text("12345.6700").unwrap()),
        PgValue::Date(PG_DATE_NEG_INF),
        PgValue::Time(86_400_000_000),
        PgValue::Timestamp(PG_TS_POS_INF),
        PgValue::TimestampTz(PG_TS_NEG_INF),
        PgValue::Interval {
            months: -14,
            days: 3,
            microseconds: -4_500_006,
        },
        PgValue::Text("shipped".to_string()),
        PgValue::Int8(i64::from(u32::MAX)),
        PgValue::Int8(2_468),
        PgValue::Int8(4_000_000_001),
        PgValue::Int8(77),
        PgValue::Text("<doc attr=\"1\">ok</doc>".to_string()),
        PgValue::Text("u_text".to_string()),
        PgValue::Text("u_vchar".to_string()),
        PgValue::Text("u_bp".to_string()),
        PgValue::Text("u_name".to_string()),
        PgValue::Text("<u/>".to_string()),
        addr_comp(PgValue::Text("Austin".to_string()), PgValue::Int4(78_701)),
        PgValue::Array {
            element_oid: INT4_OID,
            dimensions: vec![2, 2],
            lower_bounds: vec![-2, 0],
            values: vec![
                PgValue::Int4(10),
                PgValue::Null,
                PgValue::Int4(-30),
                PgValue::Int4(40),
            ],
        },
        addr_comp(PgValue::Null, PgValue::Int4(10_001)),
        PgValue::Vector {
            kind: VectorKind::Vector,
            values: vec![1.25, -0.0, 3.5],
        },
        PgValue::Vector {
            kind: VectorKind::HalfVec,
            values: vec![f16::from_f32(0.5).to_f32(), f16::from_f32(-2.25).to_f32()],
        },
        PgValue::BitString {
            bit_length: 5,
            data: vec![0b1011_0000],
        },
        PgValue::BitString {
            bit_length: 3,
            data: vec![0xa0],
        },
    ];

    let (neg_m, neg_d, neg_u) = PG_INTERVAL_NEG_INF;
    let (pos_m, pos_d, pos_u) = PG_INTERVAL_POS_INF;

    let row2 = vec![
        PgValue::Null,
        PgValue::Int2(42),
        PgValue::Null,
        PgValue::Int8(i64::MAX),
        PgValue::Float4(f32::NAN),
        PgValue::Float8(-0.0_f64),
        PgValue::Bytes(Vec::new()),
        PgValue::Null,
        PgValue::Json(b"null".to_vec()),
        PgValue::Json(b"[true,false]".to_vec()),
        PgValue::Numeric(PgNumeric::NaN),
        PgValue::Date(PG_DATE_POS_INF),
        PgValue::Time(0),
        PgValue::Timestamp(0),
        PgValue::TimestampTz(PG_TS_POS_INF),
        PgValue::Interval {
            months: neg_m,
            days: neg_d,
            microseconds: neg_u,
        },
        PgValue::Text("delivered".to_string()),
        PgValue::Int8(0),
        PgValue::Int8(0),
        PgValue::Int8(1),
        PgValue::Int8(0),
        PgValue::Null,
        PgValue::Null,
        PgValue::Text(String::new()),
        PgValue::Text("x".to_string()),
        PgValue::Text("sys".to_string()),
        PgValue::Text("<empty/>".to_string()),
        PgValue::Null,
        PgValue::Array {
            element_oid: INT4_OID,
            dimensions: vec![],
            lower_bounds: vec![],
            values: vec![],
        },
        addr_comp(PgValue::Text("Boston".to_string()), PgValue::Null),
        PgValue::Vector {
            kind: VectorKind::Vector,
            values: vec![0.0, 1.0, -2.0],
        },
        PgValue::Null,
        PgValue::BitString {
            bit_length: 0,
            data: Vec::new(),
        },
        PgValue::Null,
    ];

    let mut row3 = row2.clone();
    row3[10] = PgValue::Numeric(PgNumeric::PosInf);
    row3[11] = PgValue::Date(9_500);
    row3[14] = PgValue::TimestampTz(1_234_567_890);
    row3[15] = PgValue::Interval {
        months: pos_m,
        days: pos_d,
        microseconds: pos_u,
    };

    let mut row4 = row3.clone();
    row4[10] = PgValue::Numeric(PgNumeric::NegInf);

    let mut b1 = PostgresArrowBatchBuilder::try_new(
        &source_columns,
        &catalog,
        DEFAULT_ARROW_BATCH_ROWS,
        DEFAULT_ARROW_BATCH_BYTES,
    )
    .unwrap();
    b1.append_pg_row(&row1).unwrap();
    let batch1 = b1.flush_batch().unwrap().unwrap();

    let mut b2 = PostgresArrowBatchBuilder::try_new(
        &source_columns,
        &catalog,
        DEFAULT_ARROW_BATCH_ROWS,
        DEFAULT_ARROW_BATCH_BYTES,
    )
    .unwrap();
    b2.append_pg_row(&row2).unwrap();
    b2.append_pg_row(&row3).unwrap();
    b2.append_pg_row(&row4).unwrap();
    let batch2 = b2.flush_batch().unwrap().unwrap();

    let plan = PostgresArrowImportPlan::from_schema(&schema, &target_columns, &catalog).unwrap();
    let mut encoder = BinaryCopyEncoder::new(target_columns.len()).unwrap();
    let mut wire = BytesMut::new();

    plan.encode_batch(&batch1, &mut encoder, &ctx, &catalog, &mut wire)
        .unwrap();
    assert!(wire.starts_with(&PGCOPY_SIGNATURE));
    assert_eq!(count_pgcopy_headers(&wire), 1);

    plan.encode_batch(&batch2, &mut encoder, &ctx, &catalog, &mut wire)
        .unwrap();
    encoder.finish_trailer(&mut wire).unwrap();

    assert_eq!(count_pgcopy_headers(&wire), 1);
    assert_eq!(&wire[..11], &PGCOPY_SIGNATURE);
    assert_eq!(&wire[11..19], &[0u8; 8]);
    assert_eq!(&wire[wire.len() - 2..], &(-1_i16).to_be_bytes());

    let decoded = decode_pgcopy_values(&wire, &target_columns, &ctx, &catalog);
    assert_eq!(decoded.len(), 4);
    for idx in [
        0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 28, 29, 30, 31, 32, 33,
    ] {
        assert_eq!(decoded[0][idx], row1[idx], "row1 mismatch at col {idx}");
    }
    assert!(matches!(decoded[0][4], PgValue::Float4(v) if v == 0.0 && v.is_sign_negative()));
    assert!(matches!(decoded[1][4], PgValue::Float4(v) if v.is_nan()));
    assert_eq!(decoded[1][10], PgValue::Numeric(PgNumeric::NaN));
    assert_eq!(decoded[1][15], row2[15]);
    assert_eq!(decoded[1][28], row2[28]);
    assert_eq!(decoded[2][10], PgValue::Numeric(PgNumeric::PosInf));
    assert_eq!(decoded[2][14], row3[14]);
    assert_eq!(decoded[2][15], row3[15]);
    assert_eq!(decoded[3][10], PgValue::Numeric(PgNumeric::NegInf));
}

#[test]
fn ordinary_arrow_types_dictionaries_and_zero_row_first_batch_preserve_single_header() {
    let catalog = build_rich_catalog();
    let ctx = CodecContext::default();

    let target_columns = vec![
        col("u16_to_i4", INT4_OID, -1),
        col("f16_to_f4", FLOAT4_OID, -1),
        col("dec128", NUMERIC_OID, -1),
        col("dec256", NUMERIC_OID, -1),
        col("d32", DATE_OID, -1),
        col("d64", DATE_OID, -1),
        col("tstz_ns", TIMESTAMPTZ_OID, -1),
        col("t32", TIME_OID, -1),
        col("t64", TIME_OID, -1),
        col("dict_txt", TEXT_OID, -1),
        col("dict_enum", CUSTOM_ENUM_OID, -1),
        col("list_i4", INT4_ARRAY_OID, -1),
        col("ord_comp", CUSTOM_COMPOSITE_OID, -1),
        col("ord_vec", CUSTOM_VECTOR_OID, 3),
        col("uuid_bin", UUID_OID, -1),
    ];

    let comp_fields = Fields::from(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("zip", DataType::Int32, true),
    ]);
    let list_item = Arc::new(Field::new("item", DataType::Int32, true));
    let vec_item = Arc::new(Field::new("item", DataType::Float32, false));

    let schema = Arc::new(Schema::new(vec![
        Field::new("u16_to_i4", DataType::UInt16, true),
        Field::new("f16_to_f4", DataType::Float16, true),
        Field::new("dec128", DataType::Decimal128(18, 4), true),
        Field::new("dec256", DataType::Decimal256(40, 2), true),
        Field::new("d32", DataType::Date32, true),
        Field::new("d64", DataType::Date64, true),
        Field::new(
            "tstz_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into())),
            true,
        ),
        Field::new("t32", DataType::Time32(TimeUnit::Second), true),
        Field::new("t64", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new(
            "dict_txt",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "dict_enum",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("list_i4", DataType::List(Arc::clone(&list_item)), true),
        Field::new("ord_comp", DataType::Struct(comp_fields.clone()), true),
        Field::new(
            "ord_vec",
            DataType::FixedSizeList(Arc::clone(&vec_item), 3),
            true,
        ),
        Field::new("uuid_bin", DataType::FixedSizeBinary(16), true),
    ]));

    let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
    assert_eq!(empty_batch.num_rows(), 0);

    let unix_ts_ns: i64 = 1_700_000_000_123_456_000;
    let expected_pg_us: i64 = (unix_ts_ns / 1_000) - 946_684_800_000_000;

    let dec128_arr = Decimal128Array::from(vec![Some(-123_450_i128), Some(0_i128)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let dec256_arr = Decimal256Array::from(vec![Some(i256::from_i128(987_654_321_099_i128)), None])
        .with_precision_and_scale(40, 2)
        .unwrap();

    let dict_vals = Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")]));
    let dict_txt = DictionaryArray::new(Int8Array::from(vec![Some(1), None]), dict_vals);
    let dict_enum = DictionaryArray::new(
        Int8Array::from(vec![Some(0), Some(1)]),
        Arc::new(StringArray::from(vec![Some("pending"), Some("shipped")])) as ArrayRef,
    );

    let list_vals = Arc::new(Int32Array::from(vec![Some(5), None, Some(9)])) as ArrayRef;
    let list_offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2, 3]));
    let list_arr = ListArray::new(Arc::clone(&list_item), list_offsets, list_vals, None);

    let comp_arr = StructArray::new(
        comp_fields,
        vec![
            Arc::new(StringArray::from(vec![Some("Denver"), None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(80_202), Some(0)])) as ArrayRef,
        ],
        Some(NullBuffer::from(vec![true, false])),
    );

    let vec_vals = Arc::new(Float32Array::from(vec![1.0_f32, 2.0, 3.0, 4.0, 5.0, 6.0])) as ArrayRef;
    let vec_arr = FixedSizeListArray::new(
        vec_item,
        3,
        vec_vals,
        Some(NullBuffer::from(vec![true, false])),
    );

    let uuid_bytes: [u8; 16] = *b"0123456789abcdef";
    let uuid_arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        vec![Some(uuid_bytes.as_slice()), None].into_iter(),
        16,
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt16Array::from(vec![Some(u16::MAX), None])),
            Arc::new(Float16Array::from(vec![
                Some(f16::from_f32(1.5)),
                Some(f16::from_f32(-0.5)),
            ])),
            Arc::new(dec128_arr),
            Arc::new(dec256_arr),
            Arc::new(Date32Array::from(vec![Some(10_957), Some(10_958)])),
            Arc::new(Date64Array::from(vec![Some(10_957_i64 * 86_400_000), None])),
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(unix_ts_ns), None])
                    .with_timezone("America/New_York"),
            ),
            Arc::new(Time32SecondArray::from(vec![Some(3_661), Some(0)])),
            Arc::new(Time64NanosecondArray::from(vec![
                Some(86_400_000_000_000),
                None,
            ])),
            Arc::new(dict_txt),
            Arc::new(dict_enum),
            Arc::new(list_arr),
            Arc::new(comp_arr),
            Arc::new(vec_arr),
            Arc::new(uuid_arr),
        ],
    )
    .unwrap();

    let plan = PostgresArrowImportPlan::from_schema(&schema, &target_columns, &catalog).unwrap();

    let mut enc_a = BinaryCopyEncoder::new(target_columns.len()).unwrap();
    let mut wire_a = BytesMut::new();
    plan.encode_batch(&empty_batch, &mut enc_a, &ctx, &catalog, &mut wire_a)
        .unwrap();
    assert!(wire_a.is_empty());
    plan.encode_batch(&batch2, &mut enc_a, &ctx, &catalog, &mut wire_a)
        .unwrap();
    enc_a.finish_trailer(&mut wire_a).unwrap();
    assert_eq!(count_pgcopy_headers(&wire_a), 1);

    let rows = decode_pgcopy_values(&wire_a, &target_columns, &ctx, &catalog);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], PgValue::Int4(i32::from(u16::MAX)));
    assert_eq!(rows[0][1], PgValue::Float4(1.5));
    assert_eq!(
        rows[0][2],
        PgValue::Numeric(decode_numeric_text("-12.3450").unwrap())
    );
    assert_eq!(
        rows[1][2],
        PgValue::Numeric(decode_numeric_text("0.0000").unwrap())
    );
    assert_eq!(
        rows[0][3],
        PgValue::Numeric(decode_numeric_text("9876543210.99").unwrap())
    );
    assert_eq!(rows[0][4], PgValue::Date(0));
    assert_eq!(rows[1][4], PgValue::Date(1));
    assert_eq!(rows[0][5], PgValue::Date(0));
    assert_eq!(rows[0][6], PgValue::TimestampTz(expected_pg_us));
    assert_eq!(rows[0][7], PgValue::Time(3_661_000_000));
    assert_eq!(rows[0][8], PgValue::Time(86_400_000_000));
    assert_eq!(rows[0][9], PgValue::Text("beta".to_string()));
    assert_eq!(rows[1][9], PgValue::Null);
    assert_eq!(rows[0][10], PgValue::Text("pending".to_string()));
    assert_eq!(
        rows[0][11],
        PgValue::Array {
            element_oid: INT4_OID,
            dimensions: vec![2],
            lower_bounds: vec![1],
            values: vec![PgValue::Int4(5), PgValue::Null],
        }
    );
    assert_eq!(
        rows[1][11],
        PgValue::Array {
            element_oid: INT4_OID,
            dimensions: vec![1],
            lower_bounds: vec![1],
            values: vec![PgValue::Int4(9)],
        }
    );
    assert_eq!(
        rows[0][12],
        addr_comp(PgValue::Text("Denver".to_string()), PgValue::Int4(80_202))
    );
    assert_eq!(rows[1][12], PgValue::Null);
    assert_eq!(
        rows[0][13],
        PgValue::Vector {
            kind: VectorKind::Vector,
            values: vec![1.0, 2.0, 3.0],
        }
    );
    assert_eq!(rows[1][13], PgValue::Null);
    assert_eq!(rows[0][14], PgValue::Uuid(uuid_bytes));

    let mut enc_b = BinaryCopyEncoder::new(target_columns.len()).unwrap();
    let mut wire_b = BytesMut::new();
    plan.encode_batch(&empty_batch, &mut enc_b, &ctx, &catalog, &mut wire_b)
        .unwrap();
    enc_b.finish_trailer(&mut wire_b).unwrap();
    assert_eq!(wire_b.len(), 21);
    assert_eq!(count_pgcopy_headers(&wire_b), 1);
    assert_eq!(
        decode_pgcopy_values(&wire_b, &target_columns, &ctx, &catalog).len(),
        0
    );
}

#[test]
fn pre_copy_schema_and_value_rejections_cover_text_only_pseudo_non_binary_and_loss_guards() {
    let catalog = build_rich_catalog();
    let ctx = CodecContext::default();

    let field_with_meta = |name: &str, dt: DataType, oid: u32, type_name: &str, enc: &str| {
        let mut m = HashMap::new();
        m.insert(POSTGRES_FIELD_OID_KEY.to_string(), oid.to_string());
        m.insert(POSTGRES_FIELD_TYPMOD_KEY.to_string(), "-1".to_string());
        m.insert(
            POSTGRES_FIELD_TYPE_NAME_KEY.to_string(),
            type_name.to_string(),
        );
        m.insert(POSTGRES_FIELD_ENCODING_KEY.to_string(), enc.to_string());
        Field::new(name, dt, true).with_metadata(m)
    };

    let mut ver1 = HashMap::new();
    ver1.insert(
        POSTGRES_ARROW_VERSION_KEY.to_string(),
        POSTGRES_ARROW_VERSION.to_string(),
    );

    let schema_rejections: Vec<(&str, Schema, Vec<Column>)> = vec![
        (
            "regproc-text",
            Schema::new_with_metadata(
                vec![field_with_meta(
                    "f",
                    DataType::Utf8,
                    REGPROC_OID,
                    "regproc",
                    "regproc-text",
                )],
                ver1.clone(),
            ),
            vec![col("f", REGPROC_OID, -1)],
        ),
        (
            "pg-range",
            Schema::new_with_metadata(
                vec![field_with_meta(
                    "f",
                    DataType::Utf8,
                    CUSTOM_RANGE_OID,
                    "int4range",
                    "pg-range",
                )],
                ver1.clone(),
            ),
            vec![col("f", CUSTOM_RANGE_OID, -1)],
        ),
        (
            "record-text",
            Schema::new_with_metadata(
                vec![field_with_meta(
                    "f",
                    DataType::Utf8,
                    RECORD_OID,
                    "record",
                    "record-text",
                )],
                ver1.clone(),
            ),
            vec![col("f", RECORD_OID, -1)],
        ),
        (
            "unknown-text / BOX_OID",
            Schema::new_with_metadata(
                vec![field_with_meta(
                    "f",
                    DataType::Utf8,
                    BOX_OID,
                    "box",
                    "unknown-text",
                )],
                ver1.clone(),
            ),
            vec![col("f", BOX_OID, -1)],
        ),
        (
            "ordinary Utf8 -> BOX_OID",
            Schema::new(vec![Field::new("f", DataType::Utf8, true)]),
            vec![col("f", BOX_OID, -1)],
        ),
        (
            "ordinary Utf8 -> RECORD_OID",
            Schema::new(vec![Field::new("f", DataType::Utf8, true)]),
            vec![col("f", RECORD_OID, -1)],
        ),
        (
            "target UNKNOWN_OID (705)",
            Schema::new_with_metadata(
                vec![field_with_meta(
                    "f",
                    DataType::Utf8,
                    UNKNOWN_OID,
                    "unknown",
                    "scalar",
                )],
                ver1,
            ),
            vec![col("f", UNKNOWN_OID, -1)],
        ),
        (
            "enum-array (supports_binary_with_catalog == false)",
            PostgresArrowSchema::from_columns(
                &[Column::try_new("f", CUSTOM_ENUM_ARRAY_OID, Format::Text, -1, 0, 0).unwrap()],
                &catalog,
            )
            .unwrap()
            .as_ref()
            .clone(),
            vec![col("f", CUSTOM_ENUM_ARRAY_OID, -1)],
        ),
        (
            "range-composite (supports_binary_with_catalog == false)",
            PostgresArrowSchema::from_columns(
                &[Column::try_new("f", CUSTOM_RANGE_COMP_OID, Format::Text, -1, 0, 0).unwrap()],
                &catalog,
            )
            .unwrap()
            .as_ref()
            .clone(),
            vec![col("f", CUSTOM_RANGE_COMP_OID, -1)],
        ),
        (
            "tz-aware Timestamp -> naive timestamp",
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            )]),
            vec![col("ts", TIMESTAMP_OID, -1)],
        ),
        (
            "naive Timestamp -> timestamptz",
            Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )]),
            vec![col("ts", TIMESTAMPTZ_OID, -1)],
        ),
        (
            "column count mismatch",
            Schema::new(vec![Field::new("a", DataType::Int32, true)]),
            vec![col("a", INT4_OID, -1), col("b", INT4_OID, -1)],
        ),
        (
            "duplicate target columns",
            Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("a", DataType::Int32, true),
            ]),
            vec![col("dup", INT4_OID, -1), col("dup", INT4_OID, -1)],
        ),
        (
            "Struct missing composite attribute",
            Schema::new(vec![Field::new(
                "c",
                DataType::Struct(Fields::from(vec![Field::new("city", DataType::Utf8, true)])),
                true,
            )]),
            vec![col("c", CUSTOM_COMPOSITE_OID, -1)],
        ),
        (
            "ordinary List<Utf8> into enum array",
            Schema::new(vec![Field::new(
                "arr",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            )]),
            vec![col("arr", CUSTOM_ENUM_ARRAY_OID, -1)],
        ),
        (
            "ordinary Struct into range-composite",
            Schema::new(vec![Field::new(
                "rc",
                DataType::Struct(Fields::from(vec![
                    Field::new("id", DataType::Int32, true),
                    Field::new("span", DataType::Utf8, true),
                ])),
                true,
            )]),
            vec![col("rc", CUSTOM_RANGE_COMP_OID, -1)],
        ),
    ];

    for (label, bad_schema, cols) in schema_rejections {
        assert!(
            matches!(
                PostgresArrowImportPlan::from_schema(&bad_schema, &cols, &catalog),
                Err(PostgresError::Data(_))
            ),
            "expected DataError at schema construction for case: {label}"
        );
    }

    let tstz_cols = vec![Column::try_new("ts", TIMESTAMPTZ_OID, Format::Text, -1, 0, 0).unwrap()];
    let tstz_schema = PostgresArrowSchema::from_columns(&tstz_cols, &catalog).unwrap();
    let mut tb =
        PostgresArrowBatchBuilder::try_new(&tstz_cols, &catalog, 16, DEFAULT_ARROW_BATCH_BYTES)
            .unwrap();
    tb.append_pg_row(&[PgValue::TemporalText {
        kind: TemporalKind::TimestampTz,
        wire_text: "2026-03-29 01:30:00 MET".to_string(),
        date_style: "Postgres, MDY".to_string(),
        time_zone: "Europe/Berlin".to_string(),
    }])
    .unwrap();
    let wire_text_batch = tb.flush_batch().unwrap().unwrap();
    let tstz_target = vec![col("ts", TIMESTAMPTZ_OID, -1)];
    let tstz_plan =
        PostgresArrowImportPlan::from_schema(&tstz_schema, &tstz_target, &catalog).unwrap();
    let mut enc = BinaryCopyEncoder::new(1).unwrap();
    let mut out = BytesMut::new();
    assert!(matches!(
        tstz_plan.encode_batch(&wire_text_batch, &mut enc, &ctx, &catalog, &mut out),
        Err(PostgresError::Data(_))
    ));
    assert!(out.is_empty());

    let u64_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::UInt64, true)]));
    let u64_batch = RecordBatch::try_new(
        Arc::clone(&u64_schema),
        vec![Arc::new(UInt64Array::from(vec![Some(u64::MAX)]))],
    )
    .unwrap();
    let u64_plan =
        PostgresArrowImportPlan::from_schema(&u64_schema, &[col("v", INT8_OID, -1)], &catalog)
            .unwrap();
    assert!(matches!(
        u64_plan.encode_batch(&u64_batch, &mut enc, &ctx, &catalog, &mut out),
        Err(PostgresError::Data(_))
    ));
    assert!(out.is_empty());

    let ns_schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        true,
    )]));
    let ns_batch = RecordBatch::try_new(
        Arc::clone(&ns_schema),
        vec![Arc::new(
            TimestampNanosecondArray::from(vec![Some(1_700_000_000_000_000_500)])
                .with_timezone("UTC"),
        )],
    )
    .unwrap();
    let ns_plan = PostgresArrowImportPlan::from_schema(
        &ns_schema,
        &[col("ts", TIMESTAMPTZ_OID, -1)],
        &catalog,
    )
    .unwrap();
    assert!(matches!(
        ns_plan.encode_batch(&ns_batch, &mut enc, &ctx, &catalog, &mut out),
        Err(PostgresError::Data(_))
    ));
    assert!(out.is_empty());

    let tns_schema = Arc::new(Schema::new(vec![Field::new(
        "t",
        DataType::Time64(TimeUnit::Nanosecond),
        true,
    )]));
    let tns_batch = RecordBatch::try_new(
        Arc::clone(&tns_schema),
        vec![Arc::new(Time64NanosecondArray::from(vec![Some(1_500)]))],
    )
    .unwrap();
    let tns_plan =
        PostgresArrowImportPlan::from_schema(&tns_schema, &[col("t", TIME_OID, -1)], &catalog)
            .unwrap();
    assert!(matches!(
        tns_plan.encode_batch(&tns_batch, &mut enc, &ctx, &catalog, &mut out),
        Err(PostgresError::Data(_))
    ));

    let bin_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, true)]));
    let big_payload = vec![0xAB_u8; MAX_ARROW_COPY_BATCH_BYTES + 1024];
    let big_batch = RecordBatch::try_new(
        Arc::clone(&bin_schema),
        vec![Arc::new(BinaryArray::from(vec![Some(
            big_payload.as_slice(),
        )]))],
    )
    .unwrap();
    let bin_plan =
        PostgresArrowImportPlan::from_schema(&bin_schema, &[col("b", BYTEA_OID, -1)], &catalog)
            .unwrap();
    assert!(matches!(
        bin_plan.encode_batch(&big_batch, &mut enc, &ctx, &catalog, &mut out),
        Err(PostgresError::Data(_))
    ));
    assert!(out.is_empty());
}

fn wire_frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth_ok_and_ready(pid: i32, secret: i32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&wire_frame(b'R', &0_i32.to_be_bytes()));
    out.extend_from_slice(&wire_frame(b'S', b"application_name\0vertebra\0"));
    out.extend_from_slice(&wire_frame(b'S', b"client_encoding\0UTF8\0"));
    out.extend_from_slice(&wire_frame(b'S', b"DateStyle\0ISO, MDY\0"));
    out.extend_from_slice(&wire_frame(b'S', b"TimeZone\0UTC\0"));
    let mut k = Vec::with_capacity(8);
    k.extend_from_slice(&pid.to_be_bytes());
    k.extend_from_slice(&secret.to_be_bytes());
    out.extend_from_slice(&wire_frame(b'K', &k));
    out.extend_from_slice(&wire_frame(b'Z', b"I"));
    out
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    wire_frame(b'C', &body)
}

fn error_response(sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"SERROR\0VERROR\0C");
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    wire_frame(b'E', &body)
}

fn copy_in_response(col_count: usize) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u8(1);
    body.put_i16(col_count as i16);
    for _ in 0..col_count {
        body.put_u16(1);
    }
    wire_frame(b'G', &body)
}

fn param_desc(oids: &[u32]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(oids.len() as i16);
    for &oid in oids {
        body.put_u32(oid);
    }
    wire_frame(b't', &body)
}

fn row_desc_text(cols: &[(&str, u32)]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(cols.len() as i16);
    for &(name, oid) in cols {
        body.put_slice(name.as_bytes());
        body.put_u8(0);
        body.put_u32(0);
        body.put_i16(0);
        body.put_u32(oid);
        body.put_i16(-1);
        body.put_i32(-1);
        body.put_i16(0);
    }
    wire_frame(b'T', &body)
}

fn data_row_text(cells: &[Option<&str>]) -> Vec<u8> {
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
    wire_frame(b'D', &body)
}

enum InitialPacket {
    Startup,
    Cancel { pid: i32, secret: i32 },
}

async fn read_initial_packet(sock: &mut TcpStream) -> std::io::Result<InitialPacket> {
    let mut hdr = [0u8; 8];
    sock.read_exact(&mut hdr).await?;
    let total_len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    let code = i32::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
    let mut rest = vec![0u8; total_len - 8];
    sock.read_exact(&mut rest).await?;
    if code == CANCEL_REQUEST_CODE {
        let pid = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
        let secret = i32::from_be_bytes([rest[4], rest[5], rest[6], rest[7]]);
        Ok(InitialPacket::Cancel { pid, secret })
    } else {
        Ok(InitialPacket::Startup)
    }
}

async fn read_frontend_frame(sock: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut hdr = [0u8; 5];
    sock.read_exact(&mut hdr).await?;
    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await?;
    Ok((hdr[0], body))
}

async fn read_extended_batch(sock: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut frames = Vec::new();
    loop {
        let f = read_frontend_frame(sock).await.expect("extended frame");
        let is_sync = f.0 == b'S';
        frames.push(f);
        if is_sync {
            break;
        }
    }
    frames
}

fn parse_frame_sql(parse_body: &[u8]) -> String {
    let first_nul = parse_body.iter().position(|&b| b == 0).unwrap();
    let rest = &parse_body[first_nul + 1..];
    let second_nul = rest.iter().position(|&b| b == 0).unwrap();
    String::from_utf8(rest[..second_nul].to_vec()).unwrap()
}

fn bind_text_params(bind_body: &[u8]) -> Vec<String> {
    let portal_nul = bind_body.iter().position(|&b| b == 0).unwrap();
    let rest = &bind_body[portal_nul + 1..];
    let stmt_nul = rest.iter().position(|&b| b == 0).unwrap();
    let mut p = &rest[stmt_nul + 1..];
    let fmt_count = i16::from_be_bytes([p[0], p[1]]) as usize;
    p = &p[2 + fmt_count * 2..];
    let val_count = i16::from_be_bytes([p[0], p[1]]) as usize;
    p = &p[2..];
    let mut out = Vec::with_capacity(val_count);
    for _ in 0..val_count {
        let len = i32::from_be_bytes([p[0], p[1], p[2], p[3]]);
        p = &p[4..];
        assert!(len >= 0);
        let len_u = len as usize;
        out.push(String::from_utf8(p[..len_u].to_vec()).unwrap());
        p = &p[len_u..];
    }
    out
}

fn query_sql(body: &[u8]) -> String {
    let nul = body.iter().position(|&b| b == 0).unwrap_or(body.len());
    String::from_utf8_lossy(&body[..nul]).into_owned()
}

fn pg_catalog_columns_response(
    param_oids: &[u32],
    rows: &[(&str, &str, &str, u32, i32, i16)],
    tx_status: u8,
) -> Vec<u8> {
    let desc = row_desc_text(&[
        ("nspname", NAME_OID),
        ("oid", OID_OID),
        ("attname", NAME_OID),
        ("atttypid", OID_OID),
        ("atttypmod", INT4_OID),
        ("attnum", INT2_OID),
    ]);
    let mut out = Vec::new();
    out.extend_from_slice(&wire_frame(b'1', &[]));
    out.extend_from_slice(&param_desc(param_oids));
    out.extend_from_slice(&desc);
    out.extend_from_slice(&wire_frame(b'2', &[]));
    out.extend_from_slice(&desc);
    for (nsp, rel_oid, attname, atttypid, typmod, attnum) in rows {
        let typid_s = atttypid.to_string();
        let typmod_s = typmod.to_string();
        let attnum_s = attnum.to_string();
        out.extend_from_slice(&data_row_text(&[
            Some(nsp),
            Some(rel_oid),
            Some(attname),
            Some(&typid_s),
            Some(&typmod_s),
            Some(&attnum_s),
        ]));
    }
    out.extend_from_slice(&command_complete(&format!("SELECT {}", rows.len())));
    out.extend_from_slice(&wire_frame(b'3', &[]));
    out.extend_from_slice(&wire_frame(b'Z', &[tx_status]));
    out
}

async fn connect_session(port: u16, timeout: Duration) -> Arc<SessionHandle> {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.channel_binding = "disable".to_string();
    raw.connect_timeout = timeout.as_secs_f64();
    let cfg = PostgresConfig::validate(raw).unwrap();
    let mut handle = SessionHandle::from_config(cfg);
    handle.connect().await.unwrap();
    Arc::new(handle)
}

#[tokio::test]
async fn wire_literal_schema_name_custom_discovery_and_zero_row_first_batch_single_header() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        assert!(matches!(
            read_initial_packet(&mut sock).await.unwrap(),
            InitialPacket::Startup
        ));
        sock.write_all(&auth_ok_and_ready(5101, 9101))
            .await
            .unwrap();

        let ext = read_extended_batch(&mut sock).await;
        let parse_sql = parse_frame_sql(&ext[0].1);
        assert!(parse_sql.contains("pg_class"));
        assert!(parse_sql.contains("pg_namespace"));
        assert!(parse_sql.contains("pg_attribute"));
        assert!(!parse_sql.to_ascii_lowercase().contains("(select "));
        let bind_frame = ext.iter().find(|(t, _)| *t == b'B').unwrap();
        assert_eq!(
            bind_text_params(&bind_frame.1),
            vec!["analytics".to_string(), "odd.table".to_string()]
        );

        let cat_resp = pg_catalog_columns_response(
            &[TEXT_OID, TEXT_OID],
            &[
                ("analytics", "50001", "id", INT4_OID, -1, 1),
                ("analytics", "50001", "status", CUSTOM_ENUM_OID, -1, 2),
            ],
            b'I',
        );
        sock.write_all(&cat_resp).await.unwrap();
        sock.flush().await.unwrap();

        let (q_tag, q_body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(q_tag, b'Q');
        assert!(query_sql(&q_body).contains(&CUSTOM_ENUM_OID.to_string()));
        let mut disc = row_desc_text(&[
            ("oid", OID_OID),
            ("nspname", NAME_OID),
            ("typname", NAME_OID),
            ("typtype", TEXT_OID),
            ("typdelim", TEXT_OID),
            ("typelem", OID_OID),
            ("typbasetype", OID_OID),
            ("extname", NAME_OID),
            ("attname", NAME_OID),
            ("atttypid", OID_OID),
            ("atttypmod", INT4_OID),
            ("attnum", INT2_OID),
            ("attisdropped", BOOL_OID),
        ]);
        let oid_s = CUSTOM_ENUM_OID.to_string();
        disc.extend_from_slice(&data_row_text(&[
            Some(&oid_s),
            Some("analytics"),
            Some("order_state"),
            Some("e"),
            Some(","),
            Some("0"),
            Some("0"),
            None,
            None,
            None,
            None,
            None,
            None,
        ]));
        disc.extend_from_slice(&command_complete("SELECT 1"));
        disc.extend_from_slice(&wire_frame(b'Z', b"I"));
        sock.write_all(&disc).await.unwrap();
        sock.flush().await.unwrap();

        let (b_tag, b_body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(b_tag, b'Q');
        assert_eq!(query_sql(&b_body), "BEGIN");
        let mut b_resp = command_complete("BEGIN");
        b_resp.extend_from_slice(&wire_frame(b'Z', b"T"));
        sock.write_all(&b_resp).await.unwrap();

        let (c_tag, c_body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(c_tag, b'Q');
        assert_eq!(
            query_sql(&c_body),
            "COPY \"analytics\".\"odd.table\" (\"id\", \"status\") FROM STDIN (FORMAT binary)"
        );
        sock.write_all(&copy_in_response(2)).await.unwrap();
        sock.flush().await.unwrap();

        let mut copy_bytes = Vec::new();
        loop {
            let (tag, body) = read_frontend_frame(&mut sock).await.unwrap();
            match tag {
                b'd' => copy_bytes.extend_from_slice(&body),
                b'c' => break,
                other => panic!("unexpected COPY frame tag {}", other as char),
            }
        }
        let mut done_resp = command_complete("COPY 2");
        done_resp.extend_from_slice(&wire_frame(b'Z', b"T"));
        sock.write_all(&done_resp).await.unwrap();
        sock.flush().await.unwrap();

        let (cm_tag, cm_body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(cm_tag, b'Q');
        assert_eq!(query_sql(&cm_body), "COMMIT");
        let mut cm_resp = command_complete("COMMIT");
        cm_resp.extend_from_slice(&wire_frame(b'Z', b"I"));
        sock.write_all(&cm_resp).await.unwrap();
        sock.flush().await.unwrap();

        copy_bytes
    });

    let session = connect_session(port, Duration::from_secs(3)).await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("status", DataType::Utf8, true),
    ]));
    let empty_first =
        serialize_ipc_batches(&schema, &[RecordBatch::new_empty(Arc::clone(&schema))]);
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![Some(10), Some(20)])),
            Arc::new(StringArray::from(vec![Some("paid"), Some("shipped")])),
        ],
    )
    .unwrap();
    let ipc2 = serialize_ipc_batches(&schema, &[batch2]);

    let mut stream = session
        .copy_from_arrow("odd.table", Some("analytics"), &empty_first, None)
        .await
        .unwrap();
    stream.write_arrow_ipc(&ipc2).await.unwrap();
    let inserted = stream.finish().await.unwrap();
    assert_eq!(inserted, 2);

    let wire = server.await.unwrap();
    assert_eq!(count_pgcopy_headers(&wire), 1);
    let cols = vec![col("id", INT4_OID, -1), col("status", CUSTOM_ENUM_OID, -1)];
    let decoded = decode_pgcopy_values(
        &wire,
        &cols,
        &CodecContext::default(),
        &build_rich_catalog(),
    );
    assert_eq!(
        decoded,
        vec![
            vec![PgValue::Int4(10), PgValue::Text("paid".to_string())],
            vec![PgValue::Int4(20), PgValue::Text("shipped".to_string())],
        ]
    );
}

#[tokio::test]
async fn wire_pre_copy_zero_row_schema_rejection_emits_zero_copy_frames() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let copy_frames_seen = Arc::new(AtomicUsize::new(0));
    let copy_frames_peer = Arc::clone(&copy_frames_seen);

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        assert!(matches!(
            read_initial_packet(&mut sock).await.unwrap(),
            InitialPacket::Startup
        ));
        sock.write_all(&auth_ok_and_ready(5201, 9201))
            .await
            .unwrap();

        let ext = read_extended_batch(&mut sock).await;
        let parse_sql = parse_frame_sql(&ext[0].1);
        assert!(parse_sql.contains("current_schemas(false)"));
        assert!(parse_sql.contains("array_position"));
        assert!(!parse_sql.to_ascii_lowercase().contains("(select "));

        let cat_resp = pg_catalog_columns_response(
            &[TEXT_OID],
            &[("public", "60001", "b", BOX_OID, -1, 1)],
            b'I',
        );
        sock.write_all(&cat_resp).await.unwrap();
        sock.flush().await.unwrap();

        if let Ok((tag, body)) = read_frontend_frame(&mut sock).await
            && tag == b'Q'
            && query_sql(&body).starts_with("COPY ")
        {
            copy_frames_peer.fetch_add(1, Ordering::SeqCst);
        }
    });

    let session = connect_session(port, Duration::from_secs(3)).await;
    let mut meta = HashMap::new();
    meta.insert(POSTGRES_FIELD_OID_KEY.to_string(), BOX_OID.to_string());
    meta.insert(POSTGRES_FIELD_TYPMOD_KEY.to_string(), "-1".to_string());
    meta.insert(POSTGRES_FIELD_TYPE_NAME_KEY.to_string(), "box".to_string());
    meta.insert(
        POSTGRES_FIELD_ENCODING_KEY.to_string(),
        "unknown-text".to_string(),
    );
    let mut smeta = HashMap::new();
    smeta.insert(
        POSTGRES_ARROW_VERSION_KEY.to_string(),
        POSTGRES_ARROW_VERSION.to_string(),
    );
    let bad_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("b", DataType::Utf8, true).with_metadata(meta)],
        smeta,
    ));
    let zero_row_ipc = serialize_ipc_batches(&bad_schema, &[]);

    let res = session
        .copy_from_arrow("boxes", None, &zero_row_ipc, None)
        .await;
    assert!(matches!(res, Err(PostgresError::Data(_))));
    assert!(session.is_ready());
    let _ = session.close_shared().await;
    let _ = server.await;
    assert_eq!(copy_frames_seen.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn wire_quoted_schema_table_batch2_wire_text_triggers_copy_fail_and_rollback() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        assert!(matches!(
            read_initial_packet(&mut sock).await.unwrap(),
            InitialPacket::Startup
        ));
        sock.write_all(&auth_ok_and_ready(5301, 9301))
            .await
            .unwrap();

        let ext = read_extended_batch(&mut sock).await;
        let bind_frame = ext.iter().find(|(t, _)| *t == b'B').unwrap();
        assert_eq!(
            bind_text_params(&bind_frame.1),
            vec!["my \"schema\"".to_string(), "odd.table".to_string()]
        );

        let cat_resp = pg_catalog_columns_response(
            &[TEXT_OID, TEXT_OID],
            &[("my \"schema\"", "70001", "ts", TIMESTAMPTZ_OID, -1, 1)],
            b'I',
        );
        sock.write_all(&cat_resp).await.unwrap();
        sock.flush().await.unwrap();

        let (b_tag, b_body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(b_tag, b'Q');
        assert_eq!(query_sql(&b_body), "BEGIN");
        let mut b_resp = command_complete("BEGIN");
        b_resp.extend_from_slice(&wire_frame(b'Z', b"T"));
        sock.write_all(&b_resp).await.unwrap();

        let (c_tag, c_body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(c_tag, b'Q');
        assert_eq!(
            query_sql(&c_body),
            "COPY \"my \"\"schema\"\"\".\"odd.table\" (\"ts\") FROM STDIN (FORMAT binary)"
        );
        sock.write_all(&copy_in_response(1)).await.unwrap();
        sock.flush().await.unwrap();

        let mut copy_data_frames = Vec::new();
        let fail_reason: String;
        loop {
            let (tag, body) = read_frontend_frame(&mut sock).await.unwrap();
            match tag {
                b'd' => copy_data_frames.push(body),
                b'f' => {
                    fail_reason = query_sql(&body);
                    break;
                }
                other => panic!("expected CopyData or CopyFail, got {}", other as char),
            }
        }
        let mut err_out = error_response("57014", &fail_reason);
        err_out.extend_from_slice(&wire_frame(b'Z', b"E"));
        sock.write_all(&err_out).await.unwrap();
        sock.flush().await.unwrap();

        let (rb_tag, rb_body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(rb_tag, b'Q');
        assert_eq!(query_sql(&rb_body), "ROLLBACK");
        let mut rb_out = command_complete("ROLLBACK");
        rb_out.extend_from_slice(&wire_frame(b'Z', b"I"));
        sock.write_all(&rb_out).await.unwrap();
        sock.flush().await.unwrap();

        copy_data_frames
    });

    let session = connect_session(port, Duration::from_secs(3)).await;
    let catalog = TypeCatalog::new_with_builtins();
    let cols = vec![Column::try_new("ts", TIMESTAMPTZ_OID, Format::Text, -1, 0, 0).unwrap()];
    let schema = PostgresArrowSchema::from_columns(&cols, &catalog).unwrap();

    let mut b1 =
        PostgresArrowBatchBuilder::try_new(&cols, &catalog, 16, DEFAULT_ARROW_BATCH_BYTES).unwrap();
    b1.append_pg_row(&[PgValue::TimestampTz(0)]).unwrap();
    let ipc1 = serialize_ipc_batches(&schema, &[b1.flush_batch().unwrap().unwrap()]);

    let mut b2 =
        PostgresArrowBatchBuilder::try_new(&cols, &catalog, 16, DEFAULT_ARROW_BATCH_BYTES).unwrap();
    b2.append_pg_row(&[PgValue::TemporalText {
        kind: TemporalKind::TimestampTz,
        wire_text: "2026-03-29 01:30:00 MET".to_string(),
        date_style: "Postgres, MDY".to_string(),
        time_zone: "Europe/Berlin".to_string(),
    }])
    .unwrap();
    let ipc2 = serialize_ipc_batches(&schema, &[b2.flush_batch().unwrap().unwrap()]);

    let mut stream = session
        .copy_from_arrow("\"my \"\"schema\"\"\".\"odd.table\"", None, &ipc1, None)
        .await
        .unwrap();
    let err = stream.write_arrow_ipc(&ipc2).await.unwrap_err();
    assert!(matches!(err, PostgresError::Data(_)));
    assert!(stream.is_closed());
    assert_eq!(session.transaction_status(), TransactionStatus::Idle);

    let frames = server.await.unwrap();
    let total_sent: Vec<u8> = frames.into_iter().flatten().collect();
    assert_eq!(total_sent.len(), 33);
}

#[tokio::test]
async fn wire_ambient_transaction_owner_preserved_and_batch2_failure_leaves_tx_aborted_without_rollback()
 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = read_initial_packet(&mut sock).await.unwrap();
        sock.write_all(&auth_ok_and_ready(5351, 9351))
            .await
            .unwrap();

        let (b_tag, b_body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(b_tag, b'Q');
        assert_eq!(query_sql(&b_body), "BEGIN");
        let mut b_resp = command_complete("BEGIN");
        b_resp.extend_from_slice(&wire_frame(b'Z', b"T"));
        sock.write_all(&b_resp).await.unwrap();

        let _ = read_extended_batch(&mut sock).await;
        let cat_resp = pg_catalog_columns_response(
            &[TEXT_OID, TEXT_OID],
            &[
                ("public", "71001", "a", INT4_OID, -1, 1),
                ("public", "71001", "b", INT8_OID, -1, 2),
            ],
            b'T',
        );
        sock.write_all(&cat_resp).await.unwrap();

        let (c_tag, c_body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(c_tag, b'Q');
        assert_eq!(
            query_sql(&c_body),
            "COPY \"public\".\"items\" (\"b\") FROM STDIN (FORMAT binary)"
        );
        sock.write_all(&copy_in_response(1)).await.unwrap();
        sock.flush().await.unwrap();

        loop {
            let (tag, _) = read_frontend_frame(&mut sock).await.unwrap();
            if tag == b'f' {
                break;
            }
        }
        let mut err_out = error_response("57014", "aborted");
        err_out.extend_from_slice(&wire_frame(b'Z', b"E"));
        sock.write_all(&err_out).await.unwrap();
        sock.flush().await.unwrap();

        let (rb_tag, rb_body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(rb_tag, b'Q');
        assert_eq!(query_sql(&rb_body), "ROLLBACK");
        let mut rb_out = command_complete("ROLLBACK");
        rb_out.extend_from_slice(&wire_frame(b'Z', b"I"));
        sock.write_all(&rb_out).await.unwrap();
        sock.flush().await.unwrap();
    });

    let session = connect_session(port, Duration::from_secs(3)).await;
    let owner = TransactionOwner::new(99, session.generation());
    session.begin_as(owner).await.unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::UInt64, true)]));
    let ok_ipc = serialize_ipc_batches(
        &schema,
        &[RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![Some(7_u64)]))],
        )
        .unwrap()],
    );
    let bad_ipc = serialize_ipc_batches(
        &schema,
        &[RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![Some(u64::MAX)]))],
        )
        .unwrap()],
    );

    assert!(matches!(
        session
            .copy_from_arrow("items", Some("public"), &ok_ipc, Some(&["b".to_string()]))
            .await,
        Err(PostgresError::Interface(_))
    ));

    let mut stream = session
        .copy_from_arrow_as(
            "items",
            Some("public"),
            &ok_ipc,
            Some(&["b".to_string()]),
            Some(owner),
        )
        .await
        .unwrap();
    assert!(matches!(
        stream.write_arrow_ipc(&bad_ipc).await,
        Err(PostgresError::Data(_))
    ));
    assert_eq!(session.transaction_status(), TransactionStatus::Failed);
    session.rollback_as(owner).await.unwrap();
    assert_eq!(session.transaction_status(), TransactionStatus::Idle);
    server.await.unwrap();
}

#[tokio::test]
async fn wire_generation_cancellation_fences_pg_catalog_lookup_and_start_copy_in() {
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let in_catalog = Arc::new(Notify::new());
        let in_catalog_peer = Arc::clone(&in_catalog);

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = read_initial_packet(&mut sock).await.unwrap();
            sock.write_all(&auth_ok_and_ready(5401, 9401))
                .await
                .unwrap();

            let _ = read_extended_batch(&mut sock).await;
            in_catalog_peer.notify_one();

            let (mut cancel_sock, _) = listener.accept().await.unwrap();
            match read_initial_packet(&mut cancel_sock).await.unwrap() {
                InitialPacket::Cancel { pid, secret } => {
                    assert_eq!((pid, secret), (5401, 9401));
                }
                InitialPacket::Startup => panic!("expected CancelRequest"),
            }
            drop(cancel_sock);

            let mut out = error_response("57014", "canceling statement due to user request");
            out.extend_from_slice(&wire_frame(b'Z', b"I"));
            sock.write_all(&out).await.unwrap();
            sock.flush().await.unwrap();
        });

        let session = connect_session(port, Duration::from_secs(3)).await;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let ipc = serialize_ipc_batches(&schema, &[]);

        let op_gen = session.reserve_operation_generation();
        let cancel_handle = session.fenced_cancel_handle(op_gen);
        let session_task = Arc::clone(&session);
        let fut = tokio::spawn(async move {
            session_task
                .copy_from_arrow_with_generation_as(
                    op_gen,
                    "events",
                    Some("public"),
                    &ipc,
                    None,
                    None,
                )
                .await
        });

        in_catalog.notified().await;
        let _ = cancel_handle.cancel_with_disposition().await;
        let res = fut.await.unwrap();
        assert!(res.is_err());
        assert!(session.will_retire() || session.is_closed());
        server.await.unwrap();
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let in_copy_start = Arc::new(Notify::new());
        let in_copy_start_peer = Arc::clone(&in_copy_start);

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = read_initial_packet(&mut sock).await.unwrap();
            sock.write_all(&auth_ok_and_ready(5402, 9402))
                .await
                .unwrap();

            let _ = read_extended_batch(&mut sock).await;
            let cat_resp = pg_catalog_columns_response(
                &[TEXT_OID, TEXT_OID],
                &[("public", "80001", "id", INT4_OID, -1, 1)],
                b'I',
            );
            sock.write_all(&cat_resp).await.unwrap();
            sock.flush().await.unwrap();

            let (b_tag, b_body) = read_frontend_frame(&mut sock).await.unwrap();
            assert_eq!((b_tag, query_sql(&b_body).as_str()), (b'Q', "BEGIN"));
            let mut b_resp = command_complete("BEGIN");
            b_resp.extend_from_slice(&wire_frame(b'Z', b"T"));
            sock.write_all(&b_resp).await.unwrap();

            let (c_tag, c_body) = read_frontend_frame(&mut sock).await.unwrap();
            assert_eq!(c_tag, b'Q');
            assert!(query_sql(&c_body).starts_with("COPY "));
            in_copy_start_peer.notify_one();

            let (mut cancel_sock, _) = listener.accept().await.unwrap();
            match read_initial_packet(&mut cancel_sock).await.unwrap() {
                InitialPacket::Cancel { pid, secret } => {
                    assert_eq!((pid, secret), (5402, 9402));
                }
                InitialPacket::Startup => panic!("expected CancelRequest"),
            }
            drop(cancel_sock);

            let mut out = error_response("57014", "canceling COPY due to user request");
            out.extend_from_slice(&wire_frame(b'Z', b"E"));
            sock.write_all(&out).await.unwrap();
            sock.flush().await.unwrap();
        });

        let session = connect_session(port, Duration::from_secs(3)).await;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let ipc = serialize_ipc_batches(
            &schema,
            &[RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![Some(1)]))],
            )
            .unwrap()],
        );

        let op_gen = session.reserve_operation_generation();
        let cancel_handle = session.fenced_cancel_handle(op_gen);
        let session_task = Arc::clone(&session);
        let fut = tokio::spawn(async move {
            session_task
                .copy_from_arrow_with_generation_as(
                    op_gen,
                    "events",
                    Some("public"),
                    &ipc,
                    None,
                    None,
                )
                .await
        });

        in_copy_start.notified().await;
        let _ = cancel_handle.cancel_with_disposition().await;
        let res = fut.await.unwrap();
        assert!(res.is_err());
        assert!(session.will_retire() || session.is_closed());
        server.await.unwrap();
    }
}

#[test]
fn python_arrow_ipc_chunkers_and_loaders_stream_without_row_materialization() {
    Python::initialize();
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item("manifest_dir", env!("CARGO_MANIFEST_DIR"))
            .unwrap();
        let code = pyo3::ffi::c_str!(
            r#"
import asyncio
import pathlib
import site
import sys

_repo_root = pathlib.Path(manifest_dir).resolve().parents[2]
_py_ver = f"python{sys.version_info.major}.{sys.version_info.minor}"
for _sp in (
    _repo_root / ".venv" / "lib" / _py_ver / "site-packages",
    _repo_root / ".venv" / "Lib" / "site-packages",
):
    if _sp.is_dir():
        site.addsitedir(str(_sp))
_src_py = str(_repo_root / "src" / "py")
if _src_py not in sys.path:
    sys.path.insert(0, _src_py)

import pyarrow as pa

from vertebra.db.adapters.postgres.arrow import (
    aiter_arrow_ipc_batches,
    iter_arrow_ipc_batches,
    load_arrow_async,
    load_arrow_sync,
)
from vertebra.db.arrow import ArrowCursor, read_arrow_shm, write_arrow_shm
from vertebra.db.driver import DataError

orig_cursor_pylist = ArrowCursor.to_pylist
orig_cursor_to_table = ArrowCursor.to_arrow_table
orig_profile = sys.getprofile()

def _forbidden(*args, **kwargs):
    raise AssertionError("Python row materialization must never be called")

def _profile_guard(frame, event, arg):
    if event == "c_call" and getattr(arg, "__name__", None) in ("to_pylist", "to_pydict"):
        raise AssertionError(f"forbidden C-extension row materialization: {arg!r}")
    if event == "call" and frame.f_code.co_name in ("to_pylist", "to_pydict"):
        raise AssertionError(f"forbidden Python row materialization: {frame.f_code.co_name}")

ArrowCursor.to_pylist = _forbidden
ArrowCursor.to_arrow_table = _forbidden
sys.setprofile(_profile_guard)

try:
    schema = pa.schema([("id", pa.int32()), ("payload", pa.binary())])

    empty_tbl = pa.Table.from_arrays([pa.array([], type=pa.int32()), pa.array([], type=pa.binary())], schema=schema)
    empty_chunks = list(iter_arrow_ipc_batches(empty_tbl))
    assert len(empty_chunks) == 1
    with pa.ipc.open_stream(empty_chunks[0]) as rdr:
        assert rdr.schema == schema
        assert sum(b.num_rows for b in rdr) == 0

    rows_n = 16
    batch = pa.RecordBatch.from_arrays(
        [pa.array(list(range(rows_n)), type=pa.int32()), pa.array([b"x" * 512] * rows_n, type=pa.binary())],
        schema=schema,
    )
    sliced = list(iter_arrow_ipc_batches(batch, max_rows=64, max_bytes=3000))
    assert len(sliced) > 1
    recovered_ids = []
    for chunk in sliced:
        assert len(chunk) <= 3000
        with pa.ipc.open_stream(chunk) as rdr:
            for b in rdr:
                recovered_ids.extend(b.column(0)[i].as_py() for i in range(b.num_rows))
    assert recovered_ids == list(range(rows_n))

    huge_row = pa.RecordBatch.from_arrays(
        [pa.array([1], type=pa.int32()), pa.array([b"z" * 4096], type=pa.binary())],
        schema=schema,
    )
    try:
        list(iter_arrow_ipc_batches(huge_row, max_bytes=1024))
        raise AssertionError("expected DataError for oversized 1-row batch")
    except DataError:
        pass

    shm_view = write_arrow_shm(pa.Table.from_batches([batch]))
    shm_chunks = list(iter_arrow_ipc_batches(shm_view, max_bytes=4096))
    assert sum(pa.ipc.open_stream(c).read_all().num_rows for c in shm_chunks) == rows_n
    arrow_cur = read_arrow_shm(shm_view)
    cursor_chunks = list(iter_arrow_ipc_batches(arrow_cur, max_bytes=4096))
    assert sum(pa.ipc.open_stream(c).read_all().num_rows for c in cursor_chunks) == rows_n
    arrow_cur.close()
    del arrow_cur, shm_view

    class CStreamProvider:
        def __init__(self, tbl):
            self._tbl = tbl
        def __arrow_c_stream__(self, requested_schema=None):
            return self._tbl.__arrow_c_stream__(requested_schema)

    class FakeSyncCopyIn:
        def __init__(self):
            self.batches = []
            self.closed = False
            self.finished = False
        def write_arrow_ipc(self, b):
            self.batches.append(bytes(b))
        def finish(self):
            self.finished = True
            return sum(pa.ipc.open_stream(c).read_all().num_rows for c in self.batches)
        def close(self):
            self.closed = True

    class FakeSyncClient:
        def __init__(self):
            self.calls = []
            self.last_copy = None
        def copy_from_arrow(self, table_name, first_batch_ipc, *, columns=None, schema_name=None):
            copy_in = FakeSyncCopyIn()
            copy_in.batches.append(bytes(first_batch_ipc))
            self.calls.append((table_name, columns, schema_name))
            self.last_copy = copy_in
            return copy_in

    sync_client = FakeSyncClient()
    tbl2 = pa.Table.from_batches([batch, batch])
    count = load_arrow_sync(
        sync_client,
        "odd.table",
        CStreamProvider(tbl2),
        columns=("id", "payload"),
        schema_name="analytics",
    )
    assert count == rows_n * 2
    assert sync_client.calls == [("odd.table", ("id", "payload"), "analytics")]
    assert sync_client.last_copy.finished

    class FailingAsyncCopyIn:
        def __init__(self, first):
            self.batches = [bytes(first)]
            self.aclosed = False
        async def write_arrow_ipc(self, b):
            raise RuntimeError("boom on batch 2")
        async def finish(self):
            return 0
        async def aclose(self):
            self.aclosed = True

    class FakeAsyncClient:
        def __init__(self, fail=False):
            self.fail = fail
            self.calls = []
            self.last_copy = None
        async def copy_from_arrow(self, table_name, first_batch_ipc, *, columns=None, schema_name=None):
            self.calls.append((table_name, columns, schema_name))
            if self.fail:
                c = FailingAsyncCopyIn(first_batch_ipc)
            else:
                class OkAsyncCopyIn:
                    def __init__(self, first):
                        self.batches = [bytes(first)]
                        self.aclosed = False
                    async def write_arrow_ipc(self, b):
                        self.batches.append(bytes(b))
                    async def finish(self):
                        return sum(pa.ipc.open_stream(x).read_all().num_rows for x in self.batches)
                    async def aclose(self):
                        self.aclosed = True
                c = OkAsyncCopyIn(first_batch_ipc)
            self.last_copy = c
            return c

    async def _run_async():
        ok_client = FakeAsyncClient(fail=False)
        n = await load_arrow_async(
            ok_client,
            "events",
            pa.RecordBatchReader.from_batches(schema, [batch, batch]),
            columns=["id", "payload"],
            schema_name="reporting",
        )
        assert n == rows_n * 2
        assert ok_client.calls == [("events", ["id", "payload"], "reporting")]

        achunks = [c async for c in aiter_arrow_ipc_batches(empty_tbl)]
        assert len(achunks) == 1

        bad_client = FakeAsyncClient(fail=True)
        try:
            await load_arrow_async(
                bad_client,
                "events",
                pa.RecordBatchReader.from_batches(schema, [batch, batch]),
                schema_name="reporting",
            )
            raise AssertionError("expected RuntimeError")
        except RuntimeError:
            assert bad_client.last_copy.aclosed

    asyncio.run(_run_async())
finally:
    sys.setprofile(orig_profile)
    ArrowCursor.to_pylist = orig_cursor_pylist
    ArrowCursor.to_arrow_table = orig_cursor_to_table
"#
        );
        py.run(code, Some(&locals), Some(&locals))
            .unwrap_or_else(|err| {
                err.print(py);
                panic!("embedded Python Arrow import test failed: {err}");
            });
    });
}

#[tokio::test]
async fn pooled_copy_from_dropped_finish_retains_pool_slot_and_marks_ambient_failed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let copy_done_seen = Arc::new(Notify::new());
    let copy_done_srv = Arc::clone(&copy_done_seen);
    let release_srv = Arc::new(Notify::new());
    let release_srv_clone = Arc::clone(&release_srv);
    let startups = Arc::new(AtomicUsize::new(0));
    let startups_srv = Arc::clone(&startups);

    let server = tokio::spawn(async move {
        let (mut sock1, _) = listener.accept().await.unwrap();
        startups_srv.fetch_add(1, Ordering::SeqCst);
        let _ = read_initial_packet(&mut sock1).await.unwrap();
        sock1
            .write_all(&auth_ok_and_ready(8101, 9101))
            .await
            .unwrap();

        let (b_tag, b_body) = read_frontend_frame(&mut sock1).await.unwrap();
        assert_eq!(b_tag, b'Q');
        assert_eq!(query_sql(&b_body), "BEGIN");
        let mut b_resp = command_complete("BEGIN");
        b_resp.extend_from_slice(&wire_frame(b'Z', b"T"));
        sock1.write_all(&b_resp).await.unwrap();

        let _ = read_extended_batch(&mut sock1).await;
        let cat_resp = pg_catalog_columns_response(
            &[TEXT_OID, TEXT_OID],
            &[("public", "81001", "v", INT4_OID, -1, 1)],
            b'T',
        );
        sock1.write_all(&cat_resp).await.unwrap();

        let (c_tag, _) = read_frontend_frame(&mut sock1).await.unwrap();
        assert_eq!(c_tag, b'Q');
        sock1.write_all(&copy_in_response(1)).await.unwrap();
        sock1.flush().await.unwrap();

        loop {
            let (tag, _) = read_frontend_frame(&mut sock1).await.unwrap();
            if tag == b'c' {
                copy_done_srv.notify_one();
                break;
            }
            assert_eq!(tag, b'd');
        }
        release_srv_clone.notified().await;
        drop(sock1);

        let (mut sock2, _) = listener.accept().await.unwrap();
        startups_srv.fetch_add(1, Ordering::SeqCst);
        let _ = read_initial_packet(&mut sock2).await.unwrap();
        sock2
            .write_all(&auth_ok_and_ready(8102, 9102))
            .await
            .unwrap();
        loop {
            let Ok((tag2, body2)) = read_frontend_frame(&mut sock2).await else {
                break;
            };
            if tag2 == b'X' {
                break;
            }
            assert_eq!(tag2, b'Q');
            let sql2 = query_sql(&body2);
            let mut resp2 = command_complete(sql2.split_whitespace().next().unwrap_or("OK"));
            resp2.extend_from_slice(&wire_frame(b'Z', b"I"));
            sock2.write_all(&resp2).await.unwrap();
            sock2.flush().await.unwrap();
        }
    });

    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.channel_binding = "disable".to_string();
    raw.connect_timeout = 0.15;
    let config = PostgresConfig::validate(raw).unwrap();
    let pool_opts = vertebra_postgres::pool::PostgresPoolOptions {
        min_size: 0,
        max_size: 1,
        acquire_timeout: Duration::from_millis(60),
        reset_timeout: Duration::from_millis(150),
        close_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    let pool = vertebra_postgres::pool::PostgresPool::new(config, pool_opts).unwrap();

    let s1 = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
    let owner = TransactionOwner::new(1, s1.generation());
    s1.begin_as(owner).await.unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![Some(42)]))],
    )
    .unwrap();
    let ipc = serialize_ipc_batches(&schema, &[batch]);

    let mut stream = s1
        .copy_from_arrow_as("items", Some("public"), &ipc, None, Some(owner))
        .await
        .unwrap();
    let finish_timed = {
        let finish_fut = stream.finish();
        tokio::pin!(finish_fut);
        tokio::select! {
            res = &mut finish_fut => panic!("unexpected finish completion: {res:?}"),
            () = async {
                copy_done_seen.notified().await;
                tokio::time::sleep(Duration::from_millis(15)).await;
            } => false,
        }
    };
    assert!(!finish_timed);
    drop(stream);

    assert!(matches!(
        pool.acquire().await,
        Err(PostgresError::PoolExhausted(_))
    ));
    let commit_err = s1.commit_as(owner).await.unwrap_err();
    assert!(
        matches!(&commit_err, PostgresError::Interface(msg) if msg.contains("ambient transaction failed")),
        "expected ambient_cancel_failed error, got {commit_err:?}"
    );
    s1.rollback_as(owner).await.unwrap();
    assert!(matches!(
        pool.acquire().await,
        Err(PostgresError::PoolExhausted(_))
    ));

    s1.close_shared().await.unwrap();
    release_srv.notify_one();

    let s2 = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
    assert_eq!(s2.generation(), 2);
    s2.close_shared().await.unwrap();
    pool.close(Duration::from_millis(300)).await.unwrap();
    server.await.unwrap();
    assert_eq!(startups.load(Ordering::SeqCst), 2);
}
