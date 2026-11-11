//! Behavioral integration tests for incremental `pgoutput` v1 relation, type, and four-state tuple
//! decoding (`_postgres::replication::pgoutput`), metadata custom-type preloading, and pure-Python
//! replication value/envelope contracts (`vertebra.db.adapters.postgres.types`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use _postgres::PoolerMode;
use _postgres::codecs::{
    BYTEA_OID, CodecContext, INT4_OID, INT8_OID, JSONB_OID, NUMERIC_OID, OID_OID, TEXT_OID,
};
use _postgres::config::{PostgresConfig, RawPostgresOptions};
use _postgres::errors::PostgresError;
use _postgres::python_values::{extract_parameters, extract_pg_value};
use _postgres::replication::pgoutput::{
    DEFAULT_MAX_RECORD_BYTES, DEFAULT_MAX_RELATIONS, PgOutputColumn, PgOutputDecoder,
    PgOutputFrame, PgOutputRelationSchema, PgOutputTupleValue,
};
use _postgres::replication::{
    PostgresReplicationConnection, VerifiedReplicationIdentity, pgoutput_tuple_to_python_dict,
    pgoutput_tuple_value_to_python, verified_identity_to_python,
};
use _postgres::result::{PgValue, VectorKind};
use _postgres::types::{CompositeField, TypeCatalog, TypeInfo, TypeKind};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;

const BOOL_OID_I32: i32 = 16;
const NAME_OID_I32: i32 = 19;
const INT2_OID_I32: i32 = 21;
const INT4_OID_I32: i32 = 23;
const TEXT_OID_I32: i32 = 25;
const OID_OID_I32: i32 = 26;
const NAME_ARRAY_OID_I32: i32 = 1003;

enum RawTupleCell<'a> {
    Null,
    UnchangedToast,
    Text(&'a [u8]),
    Binary(&'a [u8]),
    RawTag(u8),
    RawLen(u8, i32, &'a [u8]),
}

fn push_cstring(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

fn encode_relation_frame(
    relation_oid: u32,
    namespace: &str,
    name: &str,
    replica_identity: u8,
    columns: &[(u8, &str, u32, i32)],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'R');
    out.extend_from_slice(&relation_oid.to_be_bytes());
    push_cstring(&mut out, namespace);
    push_cstring(&mut out, name);
    out.push(replica_identity);
    out.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for &(flags, col_name, type_oid, type_modifier) in columns {
        out.push(flags);
        push_cstring(&mut out, col_name);
        out.extend_from_slice(&type_oid.to_be_bytes());
        out.extend_from_slice(&type_modifier.to_be_bytes());
    }
    out
}

fn encode_type_frame(type_oid: u32, namespace: &str, name: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'Y');
    out.extend_from_slice(&type_oid.to_be_bytes());
    push_cstring(&mut out, namespace);
    push_cstring(&mut out, name);
    out
}

fn encode_tuple_data(out: &mut Vec<u8>, cells: &[RawTupleCell<'_>]) {
    out.extend_from_slice(&(cells.len() as u16).to_be_bytes());
    for cell in cells {
        match cell {
            RawTupleCell::Null => out.push(b'n'),
            RawTupleCell::UnchangedToast => out.push(b'u'),
            RawTupleCell::Text(bytes) => {
                out.push(b't');
                out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                out.extend_from_slice(bytes);
            }
            RawTupleCell::Binary(bytes) => {
                out.push(b'b');
                out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                out.extend_from_slice(bytes);
            }
            RawTupleCell::RawTag(tag) => out.push(*tag),
            RawTupleCell::RawLen(tag, len, bytes) => {
                out.push(*tag);
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(bytes);
            }
        }
    }
}

fn encode_insert_frame(relation_oid: u32, subtag: u8, cells: &[RawTupleCell<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'I');
    out.extend_from_slice(&relation_oid.to_be_bytes());
    out.push(subtag);
    encode_tuple_data(&mut out, cells);
    out
}

#[test]
fn pgoutput_four_state_tuple_fidelity_omits_dropped_and_hydrates_python_toast() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let codec_ctx = CodecContext::default();
    let mut decoder = PgOutputDecoder::new(DEFAULT_MAX_RELATIONS, DEFAULT_MAX_RECORD_BYTES);

    let numeric_typmod = ((10_i32) << 16) | 4 | 4;
    let rel_bytes = encode_relation_frame(
        16384,
        "public",
        "accounts",
        b'd',
        &[
            (1, "id", INT8_OID, -1),
            (0, "bio", TEXT_OID, -1),
            (0, "meta", JSONB_OID, -1),
            (0, "amount", NUMERIC_OID, numeric_typmod),
            (0, "raw_blob", BYTEA_OID, -1),
            (0, "note", TEXT_OID, -1),
        ],
    );

    // Exact PostgreSQL 18 `numeric_send` payload for `1.2300` (ndigits=2, weight=0, sign=0, dscale=4, [1, 2300]).
    let numeric_1_2300: [u8; 12] = [
        0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x01, 0x08, 0xfc,
    ];
    let ins_bytes = encode_insert_frame(
        16384,
        b'N',
        &[
            RawTupleCell::Text(b"42"),
            RawTupleCell::UnchangedToast,
            RawTupleCell::Null,
            RawTupleCell::Binary(&numeric_1_2300),
            RawTupleCell::Binary(b""),
            RawTupleCell::Text(b""),
        ],
    );

    decoder.push(&rel_bytes).expect("push relation");
    decoder.push(&ins_bytes).expect("push insert");

    let rel_frame = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode relation")
        .expect("relation frame present");
    let schema = match rel_frame {
        PgOutputFrame::Relation(s) => s,
        other => panic!("expected Relation frame, got {other:?}"),
    };
    assert_eq!(schema.relation_oid, 16384);
    assert_eq!(schema.namespace, "public");
    assert_eq!(schema.name, "accounts");
    assert_eq!(schema.replica_identity, b'd');
    assert_eq!(
        &*schema.columns,
        &[
            PgOutputColumn {
                is_key: true,
                name: "id".to_string(),
                type_oid: INT8_OID,
                type_modifier: -1,
            },
            PgOutputColumn {
                is_key: false,
                name: "bio".to_string(),
                type_oid: TEXT_OID,
                type_modifier: -1,
            },
            PgOutputColumn {
                is_key: false,
                name: "meta".to_string(),
                type_oid: JSONB_OID,
                type_modifier: -1,
            },
            PgOutputColumn {
                is_key: false,
                name: "amount".to_string(),
                type_oid: NUMERIC_OID,
                type_modifier: numeric_typmod,
            },
            PgOutputColumn {
                is_key: false,
                name: "raw_blob".to_string(),
                type_oid: BYTEA_OID,
                type_modifier: -1,
            },
            PgOutputColumn {
                is_key: false,
                name: "note".to_string(),
                type_oid: TEXT_OID,
                type_modifier: -1,
            },
        ]
    );

    let cached_ref: &Arc<PgOutputRelationSchema> =
        decoder.relation(16384).expect("cached relation 16384");
    assert!(Arc::ptr_eq(&schema, cached_ref));
    assert!(decoder.relation(99999).is_none());

    let ins_frame = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode insert")
        .expect("insert frame present");
    let (ins_rel, tuple) = match ins_frame {
        PgOutputFrame::Insert {
            relation,
            new_tuple,
        } => (relation, new_tuple),
        other => panic!("expected Insert frame, got {other:?}"),
    };
    assert!(Arc::ptr_eq(&schema, &ins_rel));
    assert_eq!(tuple.len(), 6);
    assert_eq!(tuple[0], PgOutputTupleValue::Value(PgValue::Int8(42)));
    assert_eq!(tuple[1], PgOutputTupleValue::UnchangedToast);
    assert_eq!(tuple[2], PgOutputTupleValue::Null);
    match &tuple[3] {
        PgOutputTupleValue::Value(PgValue::Numeric(num)) => {
            assert_eq!(num.to_canonical_string(), "1.2300");
        }
        other => panic!("expected Numeric(1.2300), got {other:?}"),
    }
    assert_eq!(tuple[4], PgOutputTupleValue::Value(PgValue::Bytes(vec![])));
    assert_eq!(
        tuple[5],
        PgOutputTupleValue::Value(PgValue::Text(String::new()))
    );
    assert!(
        decoder
            .decode_next_frame(&mut catalog, &codec_ctx)
            .expect("drained")
            .is_none()
    );
    decoder.finish_clean_eof().expect("clean EOF");

    Python::attach(|py| {
        let types_mod = py
            .import("vertebra.db.adapters.postgres.types")
            .expect("import types");
        let toast_singleton = types_mod
            .getattr("PG_UNCHANGED_TOAST")
            .expect("PG_UNCHANGED_TOAST");
        let toast_cls = types_mod
            .getattr("PgUnchangedToast")
            .expect("PgUnchangedToast");

        let single_toast = pgoutput_tuple_value_to_python(py, &PgOutputTupleValue::UnchangedToast)
            .expect("hydrate UnchangedToast");
        assert!(single_toast.bind(py).is(&toast_singleton));
        assert!(
            single_toast
                .bind(py)
                .is_instance(&toast_cls)
                .expect("isinstance")
        );

        let py_dict =
            pgoutput_tuple_to_python_dict(py, &ins_rel, &tuple).expect("hydrate row dict");
        let bound_dict = py_dict.bind(py);
        assert_eq!(bound_dict.len(), 6);
        assert!(!bound_dict.contains("dropped_legacy_col").expect("contains"));
        assert_eq!(
            bound_dict
                .get_item("id")
                .expect("get")
                .expect("id")
                .extract::<i64>()
                .expect("i64"),
            42
        );
        let bio = bound_dict.get_item("bio").expect("get").expect("bio");
        assert!(bio.is(&toast_singleton));
        let meta = bound_dict.get_item("meta").expect("get").expect("meta");
        assert!(meta.is_none());
        let amount = bound_dict.get_item("amount").expect("get").expect("amount");
        let amount_str: String = amount.str().expect("str").extract().expect("string");
        assert_eq!(amount_str, "1.2300");
        let exp: i32 = amount
            .call_method0("as_tuple")
            .expect("as_tuple")
            .getattr("exponent")
            .expect("exponent")
            .extract()
            .expect("i32 exp");
        assert_eq!(exp, -4);
        let raw_blob: Vec<u8> = bound_dict
            .get_item("raw_blob")
            .expect("get")
            .expect("raw_blob")
            .extract()
            .expect("bytes");
        assert!(raw_blob.is_empty());
        let note: String = bound_dict
            .get_item("note")
            .expect("get")
            .expect("note")
            .extract()
            .expect("str");
        assert_eq!(note, "");

        let direct_err = extract_pg_value(py, &toast_singleton, Some(TEXT_OID))
            .expect_err("PG_UNCHANGED_TOAST must fail SQL bind extraction");
        assert!(matches!(direct_err, PostgresError::Data(_)));

        let params_tup = PyTuple::new(py, [&toast_singleton]).expect("tuple");
        let param_err = extract_parameters(py, params_tup.as_any(), None)
            .expect_err("PG_UNCHANGED_TOAST parameter list must fail preflight");
        assert!(matches!(param_err, PostgresError::Data(_)));

        let nested_list = PyList::new(py, [&toast_singleton]).expect("list");
        let nested_err = extract_pg_value(py, nested_list.as_any(), None)
            .expect_err("nested PG_UNCHANGED_TOAST must fail SQL bind extraction");
        assert!(matches!(nested_err, PostgresError::Data(_)));

        let pg_param_cls = types_mod.getattr("PgParameter").expect("PgParameter");
        assert!(pg_param_cls.call1((&toast_singleton, TEXT_OID)).is_err());
        let pg_json_cls = types_mod.getattr("PgJson").expect("PgJson");
        assert!(pg_json_cls.call1((&toast_singleton,)).is_err());
        assert!(pgoutput_tuple_to_python_dict(py, &ins_rel, &tuple[..2]).is_err());
    });
}

#[test]
fn pgoutput_type_frame_retains_matching_custom_types_invalidates_mismatches_and_tracks_gc() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let mut identities: HashMap<u32, (String, String)> = HashMap::new();

    let vec_oid = 41001_u32;
    let halfvec_oid = 41002_u32;
    let enum_oid = 41003_u32;
    let domain_oid = 41004_u32;
    let range_oid = 41005_u32;
    let comp_oid = 41006_u32;
    let comp_arr_oid = 41007_u32;
    let dom_arr_oid = 41008_u32;

    let custom_defs = [
        TypeInfo {
            oid: vec_oid,
            schema: "public".to_string(),
            name: "vector".to_string(),
            kind: TypeKind::Extension {
                schema: "public".to_string(),
                name: "vector".to_string(),
            },
            delimiter: b',',
        },
        TypeInfo {
            oid: halfvec_oid,
            schema: "public".to_string(),
            name: "halfvec".to_string(),
            kind: TypeKind::Extension {
                schema: "public".to_string(),
                name: "halfvec".to_string(),
            },
            delimiter: b',',
        },
        TypeInfo {
            oid: enum_oid,
            schema: "public".to_string(),
            name: "order_state".to_string(),
            kind: TypeKind::Enum,
            delimiter: b',',
        },
        TypeInfo {
            oid: domain_oid,
            schema: "public".to_string(),
            name: "positive_cents".to_string(),
            kind: TypeKind::Domain { base_oid: INT8_OID },
            delimiter: b',',
        },
        TypeInfo {
            oid: range_oid,
            schema: "public".to_string(),
            name: "cents_range".to_string(),
            kind: TypeKind::Range,
            delimiter: b',',
        },
        TypeInfo {
            oid: comp_oid,
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
        },
        TypeInfo {
            oid: comp_arr_oid,
            schema: "public".to_string(),
            name: "_shipping_addr".to_string(),
            kind: TypeKind::Array {
                element_oid: comp_oid,
                delimiter: b',',
            },
            delimiter: b',',
        },
        TypeInfo {
            oid: dom_arr_oid,
            schema: "public".to_string(),
            name: "_positive_cents".to_string(),
            kind: TypeKind::Array {
                element_oid: domain_oid,
                delimiter: b',',
            },
            delimiter: b',',
        },
    ];

    for info in custom_defs {
        identities.insert(info.oid, (info.schema.clone(), info.name.clone()));
        catalog.insert_custom_type(info);
    }

    let codec_ctx = CodecContext::default();
    let mut decoder = PgOutputDecoder::with_type_identities(
        DEFAULT_MAX_RELATIONS,
        DEFAULT_MAX_RECORD_BYTES,
        identities,
    );

    let mut batch1 = Vec::new();
    for &(oid, ns, name) in &[
        (vec_oid, "public", "vector"),
        (halfvec_oid, "public", "halfvec"),
        (enum_oid, "public", "order_state"),
        (domain_oid, "public", "positive_cents"),
        (range_oid, "public", "cents_range"),
        (comp_oid, "public", "shipping_addr"),
        (comp_arr_oid, "public", "_shipping_addr"),
        (dom_arr_oid, "public", "_positive_cents"),
    ] {
        batch1.extend_from_slice(&encode_type_frame(oid, ns, name));
    }
    batch1.extend_from_slice(&encode_relation_frame(
        24001,
        "public",
        "orders",
        b'd',
        &[
            (1, "emb", vec_oid, -1),
            (0, "hemb", halfvec_oid, -1),
            (0, "state", enum_oid, -1),
            (0, "cents", domain_oid, -1),
            (0, "band", range_oid, -1),
            (0, "addr", comp_oid, -1),
            (0, "alt_addrs", comp_arr_oid, -1),
            (0, "splits", dom_arr_oid, -1),
        ],
    ));
    batch1.extend_from_slice(&encode_insert_frame(
        24001,
        b'N',
        &[
            RawTupleCell::Text(b"[1.5,-2.25,0]"),
            RawTupleCell::Text(b"[0.5,-1.5]"),
            RawTupleCell::Text(b"shipped"),
            RawTupleCell::Text(b"9500"),
            RawTupleCell::Text(b"[100,10000)"),
            RawTupleCell::Text(b"(Austin,78701)"),
            RawTupleCell::Text(b"{\"(Austin,78701)\",NULL}"),
            RawTupleCell::Text(b"{4000,5500}"),
        ],
    ));

    decoder.push(&batch1).expect("push matching batch");
    let mut frames1 = Vec::new();
    while let Some(f) = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode matching")
    {
        frames1.push(f);
    }
    assert_eq!(frames1.len(), 10);

    for &oid in &[
        vec_oid,
        halfvec_oid,
        enum_oid,
        domain_oid,
        range_oid,
        comp_oid,
        comp_arr_oid,
        dom_arr_oid,
    ] {
        assert!(
            catalog.type_info(oid).is_some(),
            "matching Type ('Y') must preserve OID {oid} in TypeCatalog"
        );
    }

    let PgOutputFrame::Insert {
        relation: rel1,
        new_tuple: tup1,
    } = &frames1[9]
    else {
        panic!("expected Insert frame at index 9");
    };
    assert_eq!(
        tup1[0],
        PgOutputTupleValue::Value(PgValue::Vector {
            values: vec![1.5, -2.25, 0.0],
            kind: VectorKind::Vector,
        })
    );
    assert_eq!(
        tup1[1],
        PgOutputTupleValue::Value(PgValue::Vector {
            values: vec![0.5, -1.5],
            kind: VectorKind::HalfVec,
        })
    );
    assert_eq!(
        tup1[2],
        PgOutputTupleValue::Value(PgValue::Text("shipped".to_string()))
    );
    assert_eq!(tup1[3], PgOutputTupleValue::Value(PgValue::Int8(9500)));
    assert_eq!(
        tup1[4],
        PgOutputTupleValue::Value(PgValue::Text("[100,10000)".to_string()))
    );
    assert_eq!(
        tup1[5],
        PgOutputTupleValue::Value(PgValue::Composite {
            type_oid: comp_oid,
            field_names: vec!["city".to_string(), "zip".to_string()],
            field_oids: vec![TEXT_OID, INT4_OID],
            values: vec![PgValue::Text("Austin".to_string()), PgValue::Int4(78701)],
        })
    );
    assert_eq!(
        tup1[6],
        PgOutputTupleValue::Value(PgValue::Array {
            values: vec![
                PgValue::Composite {
                    type_oid: comp_oid,
                    field_names: vec!["city".to_string(), "zip".to_string()],
                    field_oids: vec![TEXT_OID, INT4_OID],
                    values: vec![PgValue::Text("Austin".to_string()), PgValue::Int4(78701)],
                },
                PgValue::Null,
            ],
            dimensions: vec![2],
            lower_bounds: vec![1],
            element_oid: comp_oid,
        })
    );
    assert_eq!(
        tup1[7],
        PgOutputTupleValue::Value(PgValue::Array {
            values: vec![PgValue::Int8(4000), PgValue::Int8(5500)],
            dimensions: vec![2],
            lower_bounds: vec![1],
            element_oid: domain_oid,
        })
    );

    // Feed mismatching `'Y'` for `comp_oid` (name changed) and `domain_oid` (namespace changed),
    // plus an unknown newly created OID `59999`.
    let mut batch2 = Vec::new();
    batch2.extend_from_slice(&encode_type_frame(comp_oid, "public", "renamed_composite"));
    batch2.extend_from_slice(&encode_type_frame(domain_oid, "audit", "positive_cents"));
    batch2.extend_from_slice(&encode_type_frame(comp_oid, "public", "shipping_addr"));
    batch2.extend_from_slice(&encode_insert_frame(
        24001,
        b'N',
        &[
            RawTupleCell::Text(b"[3.0,4.0,5.0]"),
            RawTupleCell::Text(b"[2.0,1.0]"),
            RawTupleCell::Text(b"delivered"),
            RawTupleCell::Text(b"recreated_as_text"),
            RawTupleCell::Text(b"[200,300)"),
            RawTupleCell::Text(b"(Dallas,75201,US)"),
            RawTupleCell::Text(b"{\"(Dallas,75201,US)\",NULL}"),
            RawTupleCell::Text(b"{alpha,beta}"),
        ],
    ));
    batch2.extend_from_slice(&encode_type_frame(59999, "public", "live_new_type"));
    batch2.extend_from_slice(&encode_relation_frame(
        24002,
        "public",
        "dyn_events",
        b'n',
        &[(0, "custom_val", 59999, -1)],
    ));
    batch2.extend_from_slice(&encode_insert_frame(
        24002,
        b'N',
        &[RawTupleCell::Text(b"opaque-custom-literal")],
    ));

    decoder.push(&batch2).expect("push mismatch batch");
    let mut frames2 = Vec::new();
    while let Some(f) = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode mismatch batch")
    {
        frames2.push(f);
    }
    assert_eq!(frames2.len(), 7);
    assert!(catalog.type_info(comp_oid).is_none());
    assert!(catalog.type_info(domain_oid).is_none());
    assert!(catalog.type_info(vec_oid).is_some());
    assert!(catalog.type_info(halfvec_oid).is_some());
    assert!(catalog.type_info(enum_oid).is_some());
    assert!(catalog.type_info(range_oid).is_some());
    assert!(catalog.type_info(comp_arr_oid).is_some());

    let PgOutputFrame::Insert {
        relation: rel2,
        new_tuple: tup2,
    } = &frames2[3]
    else {
        panic!("expected post-mismatch Insert");
    };
    assert_eq!(rel2.columns[5].type_oid, comp_oid);
    assert_eq!(rel2.columns[3].type_oid, domain_oid);
    assert_eq!(
        tup2[0],
        PgOutputTupleValue::Value(PgValue::Vector {
            values: vec![3.0, 4.0, 5.0],
            kind: VectorKind::Vector,
        })
    );
    assert_eq!(
        tup2[3],
        PgOutputTupleValue::Value(PgValue::Text("recreated_as_text".to_string()))
    );
    assert_eq!(
        tup2[5],
        PgOutputTupleValue::Value(PgValue::Text("(Dallas,75201,US)".to_string()))
    );
    assert_eq!(
        tup2[6],
        PgOutputTupleValue::Value(PgValue::Array {
            values: vec![
                PgValue::Text("(Dallas,75201,US)".to_string()),
                PgValue::Null
            ],
            dimensions: vec![2],
            lower_bounds: vec![1],
            element_oid: comp_oid,
        })
    );
    assert_eq!(
        tup2[7],
        PgOutputTupleValue::Value(PgValue::Array {
            values: vec![
                PgValue::Text("alpha".to_string()),
                PgValue::Text("beta".to_string()),
            ],
            dimensions: vec![2],
            lower_bounds: vec![1],
            element_oid: domain_oid,
        })
    );

    let PgOutputFrame::Insert {
        relation: rel_dyn,
        new_tuple: tup_dyn,
    } = &frames2[6]
    else {
        panic!("expected unknown-OID Insert");
    };
    assert_eq!(rel_dyn.columns[0].type_oid, 59999);
    assert_eq!(
        tup_dyn[0],
        PgOutputTupleValue::Value(PgValue::Text("opaque-custom-literal".to_string()))
    );

    let bin_unknown = encode_insert_frame(24002, b'N', &[RawTupleCell::Binary(&[0xDE, 0xAD])]);
    let bin_invalidated = encode_insert_frame(
        24001,
        b'N',
        &[
            RawTupleCell::Null,
            RawTupleCell::Null,
            RawTupleCell::Null,
            RawTupleCell::Binary(&[0x01, 0x02, 0x03]),
            RawTupleCell::Null,
            RawTupleCell::Binary(&[0xAA, 0xBB]),
            RawTupleCell::Null,
            RawTupleCell::Null,
        ],
    );
    decoder
        .push(&bin_unknown)
        .expect("push binary unknown-OID tuple");
    decoder
        .push(&bin_invalidated)
        .expect("push binary invalidated-OID tuple");
    let PgOutputFrame::Insert {
        new_tuple: tup_bin_unk,
        ..
    } = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode binary unknown-OID tuple")
        .expect("frame")
    else {
        panic!("expected binary unknown-OID Insert");
    };
    assert_eq!(
        tup_bin_unk[0],
        PgOutputTupleValue::Value(PgValue::Bytes(vec![0xDE, 0xAD]))
    );
    let PgOutputFrame::Insert {
        new_tuple: tup_bin_inv,
        ..
    } = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode binary invalidated-OID tuple")
        .expect("frame")
    else {
        panic!("expected binary invalidated-OID Insert");
    };
    assert_eq!(
        tup_bin_inv[3],
        PgOutputTupleValue::Value(PgValue::Bytes(vec![0x01, 0x02, 0x03]))
    );
    assert_eq!(
        tup_bin_inv[5],
        PgOutputTupleValue::Value(PgValue::Bytes(vec![0xAA, 0xBB]))
    );

    Python::attach(|py| {
        let gc = py.import("gc").expect("import gc");
        let types_mod = py
            .import("vertebra.db.adapters.postgres.types")
            .expect("import types");
        let py_row1 = pgoutput_tuple_to_python_dict(py, rel1, tup1).expect("hydrate rich row");
        let addr_obj = py_row1
            .bind(py)
            .get_item("addr")
            .expect("get")
            .expect("addr");
        assert_eq!(
            addr_obj
                .getattr("type_oid")
                .expect("oid")
                .extract::<u32>()
                .expect("u32"),
            comp_oid
        );
        let emb_obj = py_row1.bind(py).get_item("emb").expect("get").expect("emb");
        assert_eq!(
            emb_obj
                .getattr("kind")
                .expect("kind")
                .extract::<String>()
                .expect("str"),
            "vector"
        );

        let rec_cls = types_mod
            .getattr("PostgresReplicationRecord")
            .expect("PostgresReplicationRecord");
        let id_cls = types_mod
            .getattr("PostgresReplicationIdentity")
            .expect("PostgresReplicationIdentity");
        let toast_cls = types_mod
            .getattr("PgUnchangedToast")
            .expect("PgUnchangedToast");
        let toast_singleton = types_mod
            .getattr("PG_UNCHANGED_TOAST")
            .expect("PG_UNCHANGED_TOAST");

        let payload = PyDict::new(py);
        payload.set_item("row", py_row1).expect("set row");
        let rec = rec_cls
            .call1(("insert", &payload, (1_u128 << 64) - 1, 7_u64))
            .expect("construct record");
        assert!(
            gc.call_method1("is_tracked", (&rec,))
                .expect("gc.is_tracked")
                .extract::<bool>()
                .expect("bool"),
            "PostgresReplicationRecord MUST remain GC-tracked because payload holds mutable containers"
        );
        // Form an explicit reference cycle through `payload` and verify `gc.collect()` reclaims it cleanly.
        payload.set_item("cycle_back", &rec).expect("create cycle");
        drop(payload);
        drop(rec);
        let _ = gc.call_method0("collect").expect("gc.collect");

        assert!(
            !gc.call_method1("is_tracked", (&toast_singleton,))
                .expect("gc.is_tracked")
                .extract::<bool>()
                .expect("bool")
        );
        let fresh_toast = toast_cls.call0().expect("PgUnchangedToast()");
        assert!(
            !gc.call_method1("is_tracked", (&fresh_toast,))
                .expect("gc.is_tracked")
                .extract::<bool>()
                .expect("bool")
        );

        let rust_id = VerifiedReplicationIdentity {
            system_identifier: "7345678901234567890".to_string(),
            timeline: 2,
            database: "appdb".to_string(),
            slot: "vertebra_slot".to_string(),
            publication_fingerprint: "ab".repeat(32),
            confirmed_flush_lsn: 0x100,
            restart_lsn: 0x80,
        };
        let py_id = verified_identity_to_python(py, &rust_id).expect("hydrate identity");
        assert!(
            !gc.call_method1("is_tracked", (py_id.bind(py),))
                .expect("gc.is_tracked")
                .extract::<bool>()
                .expect("bool")
        );
        let id_key: (String, String, String, String) = py_id
            .bind(py)
            .getattr("identity_key")
            .expect("identity_key")
            .extract()
            .expect("tuple4");
        assert_eq!(
            id_key,
            (
                "7345678901234567890".to_string(),
                "appdb".to_string(),
                "vertebra_slot".to_string(),
                "ab".repeat(32),
            )
        );

        // Validate PostgresReplicationIdentity rejections.
        assert!(id_cls.call1(("", "db", "slot", "fp")).is_err());
        assert!(id_cls.call1(("sys", "   ", "slot", "fp")).is_err());
        assert!(id_cls.call1(("sys", "db", "bad\0slot", "fp")).is_err());
        assert!(id_cls.call1((123, "db", "slot", "fp")).is_err());

        // Validate PostgresReplicationRecord valid forms & invalid bounds/types.
        for valid_kind in [
            "control", "begin", "insert", "update", "delete", "truncate", "message", "commit",
        ] {
            let d = PyDict::new(py);
            let ok_rec = rec_cls.call1((valid_kind, d)).expect("valid kind");
            assert!(ok_rec.getattr("commit_end_lsn").expect("lsn").is_none());
            assert_eq!(
                ok_rec
                    .getattr("generation")
                    .expect("gen")
                    .extract::<u64>()
                    .expect("u64"),
                0
            );
        }
        let zero_lsn = rec_cls
            .call1(("commit", PyDict::new(py), 0_u64, 0_u64))
            .expect("zero commit_end_lsn");
        assert_eq!(
            zero_lsn
                .getattr("commit_end_lsn")
                .expect("lsn")
                .extract::<u64>()
                .expect("u64"),
            0
        );

        assert!(rec_cls.call1(("drop", PyDict::new(py))).is_err());
        assert!(rec_cls.call1((42, PyDict::new(py))).is_err());
        assert!(rec_cls.call1(("begin", PyList::empty(py))).is_err());
        assert!(rec_cls.call1(("begin", PyDict::new(py), true, 0)).is_err());
        assert!(
            rec_cls
                .call1(("begin", PyDict::new(py), py.None(), false))
                .is_err()
        );
        assert!(
            rec_cls
                .call1(("begin", PyDict::new(py), -1_i64, 0))
                .is_err()
        );
        assert!(
            rec_cls
                .call1(("begin", PyDict::new(py), 1_u128 << 64, 0))
                .is_err()
        );
        assert!(
            rec_cls
                .call1(("begin", PyDict::new(py), py.None(), -1_i64))
                .is_err()
        );
        let bad_key_dict = PyDict::new(py);
        bad_key_dict.set_item(1_i32, "val").expect("set int key");
        assert!(rec_cls.call1(("insert", bad_key_dict)).is_err());
    });
}

#[test]
fn pgoutput_arbitrary_byte_splits_and_coalesced_frames_decode_identically() {
    let vec_oid = 45001_u32;
    let make_catalog = || {
        let mut cat = TypeCatalog::new_with_builtins();
        cat.insert_custom_type(TypeInfo {
            oid: vec_oid,
            schema: "public".to_string(),
            name: "vector".to_string(),
            kind: TypeKind::Extension {
                schema: "public".to_string(),
                name: "vector".to_string(),
            },
            delimiter: b',',
        });
        cat
    };

    let mut coalesced = Vec::new();
    coalesced.extend_from_slice(&encode_type_frame(vec_oid, "public", "vector"));
    coalesced.extend_from_slice(&encode_relation_frame(
        30001,
        "schémà",
        "café_items",
        b'f',
        &[
            (1, "id", INT4_OID, -1),
            (0, "title_é", TEXT_OID, -1),
            (0, "bio_toast", TEXT_OID, -1),
            (0, "opt", JSONB_OID, -1),
            (0, "vec", vec_oid, -1),
        ],
    ));
    let bin_42 = 42_i32.to_be_bytes();
    coalesced.extend_from_slice(&encode_insert_frame(
        30001,
        b'N',
        &[
            RawTupleCell::Binary(&bin_42),
            RawTupleCell::Text("héllo 🌍".as_bytes()),
            RawTupleCell::UnchangedToast,
            RawTupleCell::Null,
            RawTupleCell::Text(b"[0.25,0.75]"),
        ],
    ));
    coalesced.extend_from_slice(&encode_insert_frame(
        30001,
        b'N',
        &[
            RawTupleCell::Text(b"-7"),
            RawTupleCell::Text(b"second"),
            RawTupleCell::Text(b"expanded"),
            RawTupleCell::Text(b"{\"ok\":true}"),
            RawTupleCell::Null,
        ],
    ));

    let codec_ctx = CodecContext::default();

    let mut single_cat = make_catalog();
    let mut single_dec = PgOutputDecoder::new(DEFAULT_MAX_RELATIONS, DEFAULT_MAX_RECORD_BYTES);
    single_dec.push(&coalesced).expect("single push");
    let mut baseline = Vec::new();
    while let Some(frame) = single_dec
        .decode_next_frame(&mut single_cat, &codec_ctx)
        .expect("decode single")
    {
        baseline.push(frame);
    }
    assert_eq!(baseline.len(), 4);
    assert_eq!(single_dec.buffered_bytes(), 0);
    single_dec.finish_clean_eof().expect("clean eof");

    // Every two-chunk split boundary `0..=N`.
    for split in 0..=coalesced.len() {
        let mut cat = make_catalog();
        let mut dec = PgOutputDecoder::new(DEFAULT_MAX_RELATIONS, DEFAULT_MAX_RECORD_BYTES);
        let mut got = Vec::new();
        for chunk in [&coalesced[..split], &coalesced[split..]] {
            dec.push(chunk).expect("push split chunk");
            while let Some(f) = dec
                .decode_next_frame(&mut cat, &codec_ctx)
                .expect("decode split chunk")
            {
                got.push(f);
            }
        }
        assert_eq!(got, baseline, "mismatch at split index {split}");
        dec.finish_clean_eof().expect("clean eof at split");
    }

    // Strict 1-byte-at-a-time streaming feed across all `N` bytes.
    let mut byte_cat = make_catalog();
    let mut byte_dec = PgOutputDecoder::new(DEFAULT_MAX_RELATIONS, DEFAULT_MAX_RECORD_BYTES);
    let mut byte_frames = Vec::new();
    for &b in &coalesced {
        byte_dec.push(&[b]).expect("push 1 byte");
        while let Some(f) = byte_dec
            .decode_next_frame(&mut byte_cat, &codec_ctx)
            .expect("decode 1-byte step")
        {
            byte_frames.push(f);
        }
    }
    assert_eq!(byte_frames, baseline);
    assert_eq!(byte_dec.buffered_bytes(), 0);
    byte_dec
        .finish_clean_eof()
        .expect("clean eof after 1-byte feed");
}

#[test]
fn pgoutput_atomic_relation_schema_evolution_preserves_captured_snapshots() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let codec_ctx = CodecContext::default();
    let mut decoder = PgOutputDecoder::new(DEFAULT_MAX_RELATIONS, DEFAULT_MAX_RECORD_BYTES);

    let r_v1 = encode_relation_frame(
        16384,
        "public",
        "users",
        b'i',
        &[(1, "id", INT8_OID, -1), (0, "email", TEXT_OID, -1)],
    );
    let i_v1 = encode_insert_frame(
        16384,
        b'N',
        &[
            RawTupleCell::Text(b"1"),
            RawTupleCell::Text(b"a@example.com"),
        ],
    );
    decoder.push(&r_v1).expect("push r_v1");
    decoder.push(&i_v1).expect("push i_v1");

    let PgOutputFrame::Relation(snap_v1) = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode")
        .expect("frame")
    else {
        panic!("expected Relation v1");
    };
    let PgOutputFrame::Insert {
        relation: ins_snap_v1,
        ..
    } = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode")
        .expect("frame")
    else {
        panic!("expected Insert v1");
    };
    assert!(Arc::ptr_eq(&snap_v1, &ins_snap_v1));
    assert_eq!(snap_v1.columns.len(), 2);
    assert_eq!(snap_v1.replica_identity, b'i');
    assert_eq!(decoder.relation_count(), 1);

    let r_v2 = encode_relation_frame(
        16384,
        "public",
        "users",
        b'f',
        &[
            (1, "id", INT8_OID, -1),
            (1, "email", TEXT_OID, -1),
            (1, "tier", TEXT_OID, -1),
        ],
    );
    let i_v2 = encode_insert_frame(
        16384,
        b'N',
        &[
            RawTupleCell::Text(b"2"),
            RawTupleCell::Text(b"b@example.com"),
            RawTupleCell::Text(b"pro"),
        ],
    );
    decoder.push(&r_v2).expect("push r_v2");
    decoder.push(&i_v2).expect("push i_v2");

    let PgOutputFrame::Relation(snap_v2) = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode")
        .expect("frame")
    else {
        panic!("expected Relation v2");
    };
    let PgOutputFrame::Insert {
        relation: ins_snap_v2,
        new_tuple: tup_v2,
    } = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode")
        .expect("frame")
    else {
        panic!("expected Insert v2");
    };

    // Previously emitted Arc snapshots remain completely immutable!
    assert_eq!(snap_v1.columns.len(), 2);
    assert_eq!(snap_v1.replica_identity, b'i');
    assert!(!snap_v1.columns[1].is_key);

    assert!(!Arc::ptr_eq(&snap_v1, &snap_v2));
    assert!(Arc::ptr_eq(&snap_v2, &ins_snap_v2));
    assert_eq!(snap_v2.columns.len(), 3);
    assert_eq!(snap_v2.replica_identity, b'f');
    assert_eq!(tup_v2.len(), 3);
    assert_eq!(decoder.relation_count(), 1);

    // Stale 2-column tuple after schema update to 3 columns fails ProtocolError.
    let stale_two_col = encode_insert_frame(
        16384,
        b'N',
        &[
            RawTupleCell::Text(b"3"),
            RawTupleCell::Text(b"c@example.com"),
        ],
    );
    decoder.push(&stale_two_col).expect("push stale 2-col");
    let err = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect_err("2-col tuple against 3-col relation must fail");
    assert!(matches!(err, PostgresError::Protocol(_)), "got {err:?}");

    // Also verify valid PostgreSQL zero-column table (`CREATE TABLE empty_tbl ()`) + `pg_catalog` empty namespace `""`.
    decoder.reset_on_disconnect();
    let r_zero = encode_relation_frame(16999, "", "empty_tbl", b'n', &[]);
    let i_zero = encode_insert_frame(16999, b'N', &[]);
    decoder.push(&r_zero).expect("push zero-col R");
    decoder.push(&i_zero).expect("push zero-col I");
    let _ = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("zero-col R")
        .expect("frame");
    let PgOutputFrame::Insert { new_tuple, .. } = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("zero-col I")
        .expect("frame")
    else {
        panic!("expected zero-col Insert");
    };
    assert!(new_tuple.is_empty());
}

#[test]
fn pgoutput_relation_cache_cap_unknown_relation_record_limits_and_malformed_frames() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let codec_ctx = CodecContext::default();
    let mut decoder = PgOutputDecoder::new(DEFAULT_MAX_RELATIONS, DEFAULT_MAX_RECORD_BYTES);

    for oid in 1..=(DEFAULT_MAX_RELATIONS as u32) {
        let frame = encode_relation_frame(oid, "public", "t", b'd', &[(1, "id", INT4_OID, -1)]);
        decoder.push(&frame).expect("push");
        assert!(
            decoder
                .decode_next_frame(&mut catalog, &codec_ctx)
                .expect("valid relation up to 4096")
                .is_some()
        );
    }
    assert_eq!(decoder.relation_count(), DEFAULT_MAX_RELATIONS);

    // Updating an existing relation at full capacity (`4096/4096`) succeeds in place.
    let update_existing = encode_relation_frame(
        DEFAULT_MAX_RELATIONS as u32,
        "public",
        "t",
        b'f',
        &[(1, "id", INT4_OID, -1), (1, "v", TEXT_OID, -1)],
    );
    decoder.push(&update_existing).expect("push update");
    assert!(
        decoder
            .decode_next_frame(&mut catalog, &codec_ctx)
            .expect("in-place update at 4096 cap succeeds")
            .is_some()
    );
    assert_eq!(decoder.relation_count(), DEFAULT_MAX_RELATIONS);

    // Inserting 4097th distinct relation fails with resource-limit OperationalError.
    let rel_4097 = encode_relation_frame(
        (DEFAULT_MAX_RELATIONS as u32) + 1,
        "public",
        "overflow_tbl",
        b'd',
        &[(1, "id", INT4_OID, -1)],
    );
    decoder.push(&rel_4097).expect("push 4097");
    let cap_err = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect_err("4097th relation must fail");
    assert!(
        matches!(cap_err, PostgresError::Operational(_)),
        "expected OperationalError on 4097th relation, got {cap_err:?}"
    );
    assert_eq!(decoder.relation_count(), DEFAULT_MAX_RELATIONS);

    // Unregistered relation_oid = 99999 returns ProtocolError.
    let mut dec_unreg = PgOutputDecoder::new(DEFAULT_MAX_RELATIONS, DEFAULT_MAX_RECORD_BYTES);
    let unknown_ins = encode_insert_frame(99999, b'N', &[RawTupleCell::Null]);
    dec_unreg.push(&unknown_ins).expect("push unknown rel");
    let unreg_err = dec_unreg
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect_err("unregistered rel_oid must fail");
    assert!(matches!(unreg_err, PostgresError::Protocol(_)));

    // Record byte limit checks (`push` overflow and declared length prefix overflow).
    let mut small_dec = PgOutputDecoder::new(16, 128);
    let r1 = encode_relation_frame(100, "public", "s", b'd', &[(1, "body", TEXT_OID, -1)]);
    small_dec.push(&r1).expect("push small r1");
    let _ = small_dec
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("decode r1");
    let over_push_err = small_dec
        .push(&[b'x'; 129])
        .expect_err("push > max_record_bytes must fail");
    assert!(matches!(over_push_err, PostgresError::Operational(_)));
    assert_eq!(small_dec.buffered_bytes(), 0);

    let mut declared_over = Vec::new();
    declared_over.push(b'I');
    declared_over.extend_from_slice(&100_u32.to_be_bytes());
    declared_over.push(b'N');
    declared_over.extend_from_slice(&1_u16.to_be_bytes());
    declared_over.push(b't');
    declared_over.extend_from_slice(&500_i32.to_be_bytes());
    small_dec.push(&declared_over).expect("push small header");
    let decl_err = small_dec
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect_err("declared cell exceeding max_record_bytes must fail immediately");
    assert!(matches!(decl_err, PostgresError::Operational(_)));

    // Malformed protocol frames -> ProtocolError.
    let base_rel = encode_relation_frame(500, "public", "m", b'd', &[(1, "c", TEXT_OID, -1)]);
    let bad_frames: Vec<Vec<u8>> = vec![
        encode_relation_frame(0, "public", "m", b'd', &[]),
        encode_relation_frame(501, "public", "", b'd', &[]),
        encode_relation_frame(501, "public", "m", b'z', &[]),
        encode_relation_frame(501, "public", "m", b'd', &[(2, "c", TEXT_OID, -1)]),
        encode_relation_frame(501, "public", "m", b'd', &[(0, "", TEXT_OID, -1)]),
        encode_relation_frame(501, "public", "m", b'd', &[(0, "c", 0, -1)]),
        {
            let mut bad_utf8_rel = vec![b'R'];
            bad_utf8_rel.extend_from_slice(&501_u32.to_be_bytes());
            bad_utf8_rel.extend_from_slice(&[0xFF, 0xFE, 0]);
            push_cstring(&mut bad_utf8_rel, "m");
            bad_utf8_rel.push(b'd');
            bad_utf8_rel.extend_from_slice(&0_u16.to_be_bytes());
            bad_utf8_rel
        },
        encode_type_frame(0, "public", "bad_oid"),
        encode_type_frame(50000, "public", ""),
        encode_insert_frame(500, b'O', &[RawTupleCell::Null]),
        encode_insert_frame(500, b'N', &[RawTupleCell::RawTag(b'x')]),
        encode_insert_frame(500, b'N', &[RawTupleCell::RawLen(b't', -1, b"")]),
        encode_insert_frame(500, b'N', &[RawTupleCell::RawLen(b'b', -5, b"")]),
        encode_insert_frame(500, b'N', &[RawTupleCell::Text(&[0xFF, 0xFE])]),
        encode_insert_frame(500, b'N', &[RawTupleCell::Text(b"a\0b")]),
        vec![b'S', 0, 0, 0, 1],
    ];

    for (idx, bad) in bad_frames.iter().enumerate() {
        let mut d = PgOutputDecoder::new(DEFAULT_MAX_RELATIONS, DEFAULT_MAX_RECORD_BYTES);
        d.push(&base_rel).expect("push base_rel");
        let _ = d
            .decode_next_frame(&mut catalog, &codec_ctx)
            .expect("decode base_rel");
        d.push(bad).expect("push bad");
        let err = d
            .decode_next_frame(&mut catalog, &codec_ctx)
            .expect_err("malformed frame must fail");
        assert!(
            matches!(err, PostgresError::Protocol(_)),
            "case {idx} expected ProtocolError, got {err:?}"
        );
    }
}

#[test]
fn pgoutput_clean_copydone_truncation_vs_disconnect_reset() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let codec_ctx = CodecContext::default();
    let mut decoder = PgOutputDecoder::new(DEFAULT_MAX_RELATIONS, DEFAULT_MAX_RECORD_BYTES);

    let full_rel = encode_relation_frame(
        16384,
        "public",
        "items",
        b'd',
        &[(1, "id", INT8_OID, -1), (0, "label", TEXT_OID, -1)],
    );
    decoder.push(&full_rel[..5]).expect("push 5-byte prefix");
    assert_eq!(decoder.buffered_bytes(), 5);
    assert!(
        decoder
            .decode_next_frame(&mut catalog, &codec_ctx)
            .expect("incomplete returns Ok(None)")
            .is_none()
    );

    let eof_err = decoder
        .finish_clean_eof()
        .expect_err("clean CopyDone with partial record must fail");
    assert!(matches!(eof_err, PostgresError::Protocol(_)));

    decoder.reset_on_disconnect();
    assert_eq!(decoder.buffered_bytes(), 0);
    assert_eq!(decoder.relation_count(), 0);
    decoder
        .finish_clean_eof()
        .expect("empty buffer after reset passes clean EOF");

    let ins = encode_insert_frame(
        16384,
        b'N',
        &[RawTupleCell::Text(b"99"), RawTupleCell::UnchangedToast],
    );
    decoder.push(&full_rel).expect("push fresh relation");
    decoder.push(&ins).expect("push fresh insert");

    assert!(matches!(
        decoder
            .decode_next_frame(&mut catalog, &codec_ctx)
            .expect("fresh R"),
        Some(PgOutputFrame::Relation(_))
    ));
    let Some(PgOutputFrame::Insert { new_tuple, .. }) = decoder
        .decode_next_frame(&mut catalog, &codec_ctx)
        .expect("fresh I")
    else {
        panic!("expected fresh Insert after reconnect reset");
    };
    assert_eq!(new_tuple[0], PgOutputTupleValue::Value(PgValue::Int8(99)));
    assert_eq!(new_tuple[1], PgOutputTupleValue::UnchangedToast);
    decoder.finish_clean_eof().expect("clean EOF");
}

#[tokio::test]
async fn pgoutput_connect_logical_preloads_custom_types_recursively_before_start_replication() {
    fn build_frame(tag: u8, payload: &[u8]) -> Vec<u8> {
        let len = (4 + payload.len()) as u32;
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(tag);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn row_desc(cols: &[(&str, i32)]) -> Vec<u8> {
        let mut rd = Vec::new();
        rd.extend_from_slice(&(cols.len() as i16).to_be_bytes());
        for &(name, oid) in cols {
            push_cstring(&mut rd, name);
            rd.extend_from_slice(&0_i32.to_be_bytes());
            rd.extend_from_slice(&0_i16.to_be_bytes());
            rd.extend_from_slice(&oid.to_be_bytes());
            rd.extend_from_slice(&(-1_i16).to_be_bytes());
            rd.extend_from_slice(&(-1_i32).to_be_bytes());
            rd.extend_from_slice(&0_i16.to_be_bytes());
        }
        build_frame(b'T', &rd)
    }

    fn data_row(cells: &[Option<&str>]) -> Vec<u8> {
        let mut dr = Vec::new();
        dr.extend_from_slice(&(cells.len() as i16).to_be_bytes());
        for c in cells {
            match c {
                Some(s) => {
                    dr.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    dr.extend_from_slice(s.as_bytes());
                }
                None => dr.extend_from_slice(&(-1_i32).to_be_bytes()),
            }
        }
        build_frame(b'D', &dr)
    }

    fn cmd_complete(tag: &str) -> Vec<u8> {
        let mut b = Vec::new();
        push_cstring(&mut b, tag);
        build_frame(b'C', &b)
    }

    struct ExtBatch {
        sql: String,
        param_oids: Vec<u32>,
        params: Vec<String>,
        res_fmts: Vec<i16>,
        saw_parse: bool,
        saw_desc_stmt: bool,
        saw_desc_portal: bool,
        saw_close: bool,
    }

    fn row_desc_with_fmts(cols: &[(&str, i32)], res_fmts: &[i16]) -> Vec<u8> {
        let mut rd = Vec::new();
        rd.extend_from_slice(&(cols.len() as i16).to_be_bytes());
        for (idx, &(name, oid)) in cols.iter().enumerate() {
            let fmt = if res_fmts.len() == 1 {
                res_fmts[0]
            } else {
                res_fmts.get(idx).copied().unwrap_or(0)
            };
            push_cstring(&mut rd, name);
            rd.extend_from_slice(&0_i32.to_be_bytes());
            rd.extend_from_slice(&0_i16.to_be_bytes());
            rd.extend_from_slice(&oid.to_be_bytes());
            rd.extend_from_slice(&(-1_i16).to_be_bytes());
            rd.extend_from_slice(&(-1_i32).to_be_bytes());
            rd.extend_from_slice(&fmt.to_be_bytes());
        }
        build_frame(b'T', &rd)
    }

    fn data_row_formatted(
        cols: &[(&str, i32)],
        res_fmts: &[i16],
        cells: &[Option<&str>],
    ) -> Vec<u8> {
        let mut dr = Vec::new();
        dr.extend_from_slice(&(cells.len() as i16).to_be_bytes());
        for (idx, c) in cells.iter().enumerate() {
            let fmt = if res_fmts.len() == 1 {
                res_fmts[0]
            } else {
                res_fmts.get(idx).copied().unwrap_or(0)
            };
            let oid = cols[idx].1;
            match c {
                Some(s) => {
                    let encoded: Vec<u8> = if fmt == 1 {
                        match oid {
                            OID_OID_I32 => s.parse::<u32>().expect("u32").to_be_bytes().to_vec(),
                            INT4_OID_I32 => s.parse::<i32>().expect("i32").to_be_bytes().to_vec(),
                            INT2_OID_I32 => s.parse::<i16>().expect("i16").to_be_bytes().to_vec(),
                            BOOL_OID_I32 => vec![u8::from(*s == "t")],
                            _ => s.as_bytes().to_vec(),
                        }
                    } else {
                        s.as_bytes().to_vec()
                    };
                    dr.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                    dr.extend_from_slice(&encoded);
                }
                None => dr.extend_from_slice(&(-1_i32).to_be_bytes()),
            }
        }
        build_frame(b'D', &dr)
    }

    fn ext_response(batch: &ExtBatch, cols: &[(&str, i32)], rows: &[Vec<Option<&str>>]) -> Vec<u8> {
        let mut out = Vec::new();
        if batch.saw_parse {
            out.extend_from_slice(&build_frame(b'1', &[]));
        }
        if batch.saw_desc_stmt {
            let mut pd = Vec::new();
            pd.extend_from_slice(&(batch.param_oids.len() as i16).to_be_bytes());
            for &o in &batch.param_oids {
                pd.extend_from_slice(&o.to_be_bytes());
            }
            out.extend_from_slice(&build_frame(b't', &pd));
            out.extend_from_slice(&row_desc(cols));
        }
        out.extend_from_slice(&build_frame(b'2', &[]));
        if batch.saw_desc_portal {
            out.extend_from_slice(&row_desc_with_fmts(cols, &batch.res_fmts));
        }
        for r in rows {
            out.extend_from_slice(&data_row_formatted(cols, &batch.res_fmts, r));
        }
        out.extend_from_slice(&cmd_complete(&format!("SELECT {}", rows.len())));
        if batch.saw_close {
            out.extend_from_slice(&build_frame(b'3', &[]));
        }
        out.extend_from_slice(&build_frame(b'Z', b"I"));
        out
    }

    async fn complete_startup(sock: &mut TcpStream) -> BTreeMap<String, String> {
        let mut first8 = [0_u8; 8];
        sock.read_exact(&mut first8).await.expect("startup hdr");
        let mut len = u32::from_be_bytes([first8[0], first8[1], first8[2], first8[3]]) as usize;
        let mut code = u32::from_be_bytes([first8[4], first8[5], first8[6], first8[7]]);
        if len == 8 && code == 80_877_103 {
            sock.write_all(b"N").await.expect("N");
            sock.flush().await.expect("flush");
            sock.read_exact(&mut first8).await.expect("startup2");
            len = u32::from_be_bytes([first8[0], first8[1], first8[2], first8[3]]) as usize;
            code = u32::from_be_bytes([first8[4], first8[5], first8[6], first8[7]]);
        }
        assert_eq!(code, 0x0003_0000);
        let mut body = vec![0_u8; len - 8];
        sock.read_exact(&mut body).await.expect("startup body");
        let mut parts = body[..body.len() - 1].split(|&b| b == 0);
        let mut map = BTreeMap::new();
        while let Some(k) = parts.next() {
            if k.is_empty() {
                break;
            }
            let v = parts.next().expect("val");
            map.insert(
                String::from_utf8(k.to_vec()).expect("k"),
                String::from_utf8(v.to_vec()).expect("v"),
            );
        }
        let mut banner = Vec::new();
        banner.extend_from_slice(&build_frame(b'R', &0_i32.to_be_bytes()));
        for (k, v) in [
            ("server_version", "16.4"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("IntervalStyle", "postgres"),
            ("TimeZone", "UTC"),
        ] {
            let mut ps = Vec::new();
            push_cstring(&mut ps, k);
            push_cstring(&mut ps, v);
            banner.extend_from_slice(&build_frame(b'S', &ps));
        }
        banner.extend_from_slice(&build_frame(b'K', &[0, 0, 1, 1, 0, 0, 2, 2]));
        banner.extend_from_slice(&build_frame(b'Z', b"I"));
        sock.write_all(&banner).await.expect("banner");
        sock.flush().await.expect("flush");
        map
    }

    async fn read_ext_batch(sock: &mut TcpStream) -> ExtBatch {
        let mut sql = String::new();
        let mut param_oids = Vec::new();
        let mut params = Vec::new();
        let mut res_fmts = Vec::new();
        let mut saw_parse = false;
        let mut saw_desc_stmt = false;
        let mut saw_desc_portal = false;
        let mut saw_close = false;
        loop {
            let mut hdr = [0_u8; 5];
            sock.read_exact(&mut hdr).await.expect("hdr");
            let tag = hdr[0];
            let flen = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
            let mut body = vec![0_u8; flen - 4];
            if !body.is_empty() {
                sock.read_exact(&mut body).await.expect("body");
            }
            match tag {
                b'P' => {
                    saw_parse = true;
                    let mut it = body.split(|&b| b == 0);
                    let _stmt = it.next().expect("stmt");
                    let sql_bytes = it.next().expect("sql");
                    sql = String::from_utf8(sql_bytes.to_vec()).expect("utf8 sql");
                    let pos = _stmt.len() + 1 + sql_bytes.len() + 1;
                    let n = i16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
                    for i in 0..n {
                        let o = pos + 2 + i * 4;
                        param_oids.push(u32::from_be_bytes([
                            body[o],
                            body[o + 1],
                            body[o + 2],
                            body[o + 3],
                        ]));
                    }
                }
                b'B' => {
                    let mut p = 0;
                    while body[p] != 0 {
                        p += 1;
                    }
                    p += 1;
                    while body[p] != 0 {
                        p += 1;
                    }
                    p += 1;
                    let fmt_cnt = i16::from_be_bytes([body[p], body[p + 1]]) as usize;
                    let mut bin_flags = Vec::new();
                    p += 2;
                    for _ in 0..fmt_cnt {
                        bin_flags.push(i16::from_be_bytes([body[p], body[p + 1]]));
                        p += 2;
                    }
                    let val_cnt = i16::from_be_bytes([body[p], body[p + 1]]) as usize;
                    p += 2;
                    for idx in 0..val_cnt {
                        let vlen =
                            i32::from_be_bytes([body[p], body[p + 1], body[p + 2], body[p + 3]])
                                as usize;
                        p += 4;
                        let fmt = if bin_flags.len() == 1 {
                            bin_flags[0]
                        } else if idx < bin_flags.len() {
                            bin_flags[idx]
                        } else {
                            0
                        };
                        if fmt == 1 && vlen == 4 {
                            let u = u32::from_be_bytes([
                                body[p],
                                body[p + 1],
                                body[p + 2],
                                body[p + 3],
                            ]);
                            params.push(u.to_string());
                        } else {
                            params.push(
                                String::from_utf8(body[p..p + vlen].to_vec()).expect("param str"),
                            );
                        }
                        p += vlen;
                    }
                    let rfmt_cnt = i16::from_be_bytes([body[p], body[p + 1]]) as usize;
                    p += 2;
                    for _ in 0..rfmt_cnt {
                        res_fmts.push(i16::from_be_bytes([body[p], body[p + 1]]));
                        p += 2;
                    }
                }
                b'D' => match body.first().copied() {
                    Some(b'S') => saw_desc_stmt = true,
                    Some(b'P') => saw_desc_portal = true,
                    _ => {}
                },
                b'C' => {
                    if body.first().copied() == Some(b'P') {
                        saw_close = true;
                    }
                }
                b'E' => {}
                b'S' => {
                    return ExtBatch {
                        sql,
                        param_oids,
                        params,
                        res_fmts,
                        saw_parse,
                        saw_desc_stmt,
                        saw_desc_portal,
                        saw_close,
                    };
                }
                other => panic!("unexpected tag in extended query: {}", other as char),
            }
        }
    }

    fn assert_single_select_no_subquery(sql: &str) {
        let upper = sql.to_ascii_uppercase();
        let rest = upper.strip_prefix("SELECT ").expect("SELECT prefix");
        assert!(
            !rest.contains("SELECT "),
            "correlated/nested SELECT forbidden: {sql}"
        );
    }

    let type_cols = [
        ("oid", OID_OID_I32),
        ("nspname", NAME_OID_I32),
        ("typname", NAME_OID_I32),
        ("typtype", TEXT_OID_I32),
        ("typcategory", TEXT_OID_I32),
        ("typdelim", TEXT_OID_I32),
        ("typelem", OID_OID_I32),
        ("typbasetype", OID_OID_I32),
        ("rngsubtype", OID_OID_I32),
        ("mrngtypid", OID_OID_I32),
        ("mrngsubtype", OID_OID_I32),
        ("extname", NAME_OID_I32),
        ("attname", NAME_OID_I32),
        ("atttypid", OID_OID_I32),
        ("atttypmod", INT4_OID_I32),
        ("attnum", INT2_OID_I32),
    ];

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let meta_terminated = Arc::new(AtomicBool::new(false));
    let meta_terminated_bg = Arc::clone(&meta_terminated);

    let srv = tokio::spawn(async move {
        let (mut meta, _) = listener.accept().await.expect("meta accept");
        let startup = complete_startup(&mut meta).await;
        assert!(!startup.contains_key("replication"));

        // 1. Slot preflight.
        let q_slot = read_ext_batch(&mut meta).await;
        assert!(q_slot.saw_parse);
        assert_single_select_no_subquery(&q_slot.sql);
        assert_eq!(q_slot.params, vec!["vertebra_slot".to_string()]);
        let slot_cols = [
            ("current_database", NAME_OID_I32),
            ("database", NAME_OID_I32),
            ("slot_type", TEXT_OID_I32),
            ("plugin", NAME_OID_I32),
            ("active", BOOL_OID_I32),
            ("restart_lsn", TEXT_OID_I32),
            ("confirmed_flush_lsn", TEXT_OID_I32),
            ("wal_status", TEXT_OID_I32),
        ];
        meta.write_all(&ext_response(
            &q_slot,
            &slot_cols,
            &[vec![
                Some("appdb"),
                Some("appdb"),
                Some("logical"),
                Some("pgoutput"),
                Some("f"),
                Some("1/100"),
                Some("1/200"),
                Some("reserved"),
            ]],
        ))
        .await
        .expect("write slot");
        meta.flush().await.expect("flush");

        // 2. Publication preflight.
        let q_pub = read_ext_batch(&mut meta).await;
        assert!(q_pub.saw_parse);
        assert_single_select_no_subquery(&q_pub.sql);
        assert_eq!(q_pub.params, vec!["vertebra_pub".to_string()]);
        let pub_cols = [
            ("pubname", NAME_OID_I32),
            ("puballtables", BOOL_OID_I32),
            ("pubinsert", BOOL_OID_I32),
            ("pubupdate", BOOL_OID_I32),
            ("pubdelete", BOOL_OID_I32),
            ("pubtruncate", BOOL_OID_I32),
            ("pubviaroot", BOOL_OID_I32),
            ("schemaname", NAME_OID_I32),
            ("tablename", NAME_OID_I32),
            ("attnames", NAME_ARRAY_OID_I32),
            ("rowfilter", TEXT_OID_I32),
        ];
        meta.write_all(&ext_response(
            &q_pub,
            &pub_cols,
            &[vec![
                Some("vertebra_pub"),
                Some("f"),
                Some("t"),
                Some("t"),
                Some("t"),
                Some("f"),
                Some("f"),
                Some("public"),
                Some("embeddings"),
                None,
                None,
            ]],
        ))
        .await
        .expect("write pub");
        meta.flush().await.expect("flush");

        // 3. Custom-type preload Round 0: built-in `20`, `_doc_bundle` array `42001`, multirange `42008`.
        let q_t0 = read_ext_batch(&mut meta).await;
        assert!(q_t0.saw_parse);
        assert_single_select_no_subquery(&q_t0.sql);
        assert!(q_t0.sql.contains("pg_publication_tables"));
        assert_eq!(q_t0.param_oids, vec![TEXT_OID]);
        assert_eq!(q_t0.params, vec!["vertebra_pub".to_string()]);
        meta.write_all(&ext_response(
            &q_t0,
            &type_cols,
            &[
                vec![
                    Some("20"),
                    Some("pg_catalog"),
                    Some("int8"),
                    Some("b"),
                    Some("N"),
                    Some(","),
                    Some("0"),
                    Some("0"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
                vec![
                    Some("42001"),
                    Some("public"),
                    Some("_doc_bundle"),
                    Some("b"),
                    Some("A"),
                    Some(","),
                    Some("42002"),
                    Some("0"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
                vec![
                    Some("42008"),
                    Some("public"),
                    Some("score_multirange"),
                    Some("m"),
                    Some("R"),
                    Some(","),
                    Some("0"),
                    Some("0"),
                    None,
                    Some("42003"),
                    Some("42004"),
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
            ],
        ))
        .await
        .expect("write t0");
        meta.flush().await.expect("flush");

        // 4. Custom-type preload Round 1: element composite `42002`, underlying range `42003`, + range subtype domain `42004`.
        let mut q_t1 = read_ext_batch(&mut meta).await;
        assert!(q_t1.saw_parse);
        assert_single_select_no_subquery(&q_t1.sql);
        assert!(q_t1.param_oids.iter().all(|&o| o == OID_OID));
        let mut params_t1 = q_t1.params.clone();
        params_t1.sort();
        assert_eq!(
            params_t1,
            vec![
                "42002".to_string(),
                "42003".to_string(),
                "42004".to_string()
            ]
        );
        meta.write_all(&ext_response(
            &q_t1,
            &type_cols,
            &[
                vec![
                    Some("42002"),
                    Some("public"),
                    Some("doc_bundle"),
                    Some("c"),
                    Some("C"),
                    Some(","),
                    Some("0"),
                    Some("0"),
                    None,
                    None,
                    None,
                    None,
                    Some("vec"),
                    Some("42005"),
                    Some("-1"),
                    Some("1"),
                ],
                vec![
                    Some("42002"),
                    Some("public"),
                    Some("doc_bundle"),
                    Some("c"),
                    Some("C"),
                    Some(","),
                    Some("0"),
                    Some("0"),
                    None,
                    None,
                    None,
                    None,
                    Some("half"),
                    Some("42006"),
                    Some("-1"),
                    Some("2"),
                ],
                vec![
                    Some("42002"),
                    Some("public"),
                    Some("doc_bundle"),
                    Some("c"),
                    Some("C"),
                    Some(","),
                    Some("0"),
                    Some("0"),
                    None,
                    None,
                    None,
                    None,
                    Some("status"),
                    Some("42007"),
                    Some("-1"),
                    Some("3"),
                ],
                vec![
                    Some("42003"),
                    Some("public"),
                    Some("score_range"),
                    Some("r"),
                    Some("R"),
                    Some(","),
                    Some("0"),
                    Some("0"),
                    Some("42004"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
                vec![
                    Some("42004"),
                    Some("public"),
                    Some("score_dom"),
                    Some("d"),
                    Some("N"),
                    Some(","),
                    Some("0"),
                    Some("23"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
            ],
        ))
        .await
        .expect("write t1");
        meta.flush().await.expect("flush");
        q_t1.params.clear();

        // 5. Custom-type preload Round 2 (warm statement cache hit for 3-OID query!): `42005` vector, `42006` halfvec, `42007` enum.
        let q_t2 = read_ext_batch(&mut meta).await;
        if q_t2.saw_parse {
            assert_single_select_no_subquery(&q_t2.sql);
        }
        let mut params_t2 = q_t2.params.clone();
        params_t2.sort();
        assert_eq!(
            params_t2,
            vec![
                "42005".to_string(),
                "42006".to_string(),
                "42007".to_string()
            ]
        );
        meta.write_all(&ext_response(
            &q_t2,
            &type_cols,
            &[
                vec![
                    Some("42005"),
                    Some("ext"),
                    Some("vector"),
                    Some("b"),
                    Some("U"),
                    Some(","),
                    Some("0"),
                    Some("0"),
                    None,
                    None,
                    None,
                    Some("vector"),
                    None,
                    None,
                    None,
                    None,
                ],
                vec![
                    Some("42006"),
                    Some("ext"),
                    Some("halfvec"),
                    Some("b"),
                    Some("U"),
                    Some(","),
                    Some("0"),
                    Some("0"),
                    None,
                    None,
                    None,
                    Some("vector"),
                    None,
                    None,
                    None,
                    None,
                ],
                vec![
                    Some("42007"),
                    Some("public"),
                    Some("doc_status"),
                    Some("e"),
                    Some("E"),
                    Some(","),
                    Some("0"),
                    Some("0"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
            ],
        ))
        .await
        .expect("write t2");
        meta.flush().await.expect("flush");

        // Metadata socket terminates BEFORE replication socket is accepted.
        let mut term = [0_u8; 5];
        meta.read_exact(&mut term).await.expect("read Terminate");
        assert_eq!(&term, &[b'X', 0, 0, 0, 4]);
        let mut eof_chk = [0_u8; 1];
        assert_eq!(meta.read(&mut eof_chk).await.expect("eof"), 0);
        meta_terminated_bg.store(true, Ordering::SeqCst);

        // Replication socket receives ONLY IDENTIFY_SYSTEM, START_REPLICATION, and CopyDone — ZERO SQL!
        let (mut repl, _) = listener.accept().await.expect("repl accept");
        assert!(meta_terminated_bg.load(Ordering::SeqCst));
        let repl_startup = complete_startup(&mut repl).await;
        assert_eq!(
            repl_startup.get("replication").map(String::as_str),
            Some("database")
        );

        let mut hdr = [0_u8; 5];
        repl.read_exact(&mut hdr).await.expect("identify hdr");
        assert_eq!(hdr[0], b'Q');
        let ilen = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize - 4;
        let mut ibody = vec![0_u8; ilen];
        repl.read_exact(&mut ibody).await.expect("identify body");
        assert_eq!(&ibody[..ibody.len() - 1], b"IDENTIFY_SYSTEM");

        let mut id_out = Vec::new();
        id_out.extend_from_slice(&row_desc(&[
            ("systemid", TEXT_OID_I32),
            ("timeline", INT4_OID_I32),
            ("xlogpos", TEXT_OID_I32),
            ("dbname", TEXT_OID_I32),
        ]));
        id_out.extend_from_slice(&data_row(&[
            Some("77777"),
            Some("1"),
            Some("1/900"),
            Some("appdb"),
        ]));
        id_out.extend_from_slice(&cmd_complete("IDENTIFY_SYSTEM"));
        id_out.extend_from_slice(&build_frame(b'Z', b"I"));
        repl.write_all(&id_out).await.expect("write identify");
        repl.flush().await.expect("flush");

        repl.read_exact(&mut hdr).await.expect("start hdr");
        assert_eq!(hdr[0], b'Q');
        let slen = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize - 4;
        let mut sbody = vec![0_u8; slen];
        repl.read_exact(&mut sbody).await.expect("start body");
        let start_str = String::from_utf8(sbody[..sbody.len() - 1].to_vec()).expect("utf8");
        assert!(start_str.starts_with("START_REPLICATION SLOT \"vertebra_slot\" LOGICAL"));

        repl.write_all(&build_frame(b'W', &[0, 0, 0]))
            .await
            .expect("W");
        repl.flush().await.expect("flush W");

        repl.read_exact(&mut hdr).await.expect("copydone hdr");
        assert_eq!(&hdr, &[b'c', 0, 0, 0, 4]);
        let mut end = Vec::new();
        end.extend_from_slice(&build_frame(b'c', &[]));
        end.extend_from_slice(&cmd_complete("COPY 0"));
        end.extend_from_slice(&build_frame(b'Z', b"I"));
        repl.write_all(&end).await.expect("end");
        repl.flush().await.expect("flush end");
        let _ = repl.read(&mut hdr).await;
    });

    let mut raw = RawPostgresOptions::new("127.0.0.1", "alice", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.connect_timeout = 3.0;
    raw.pooler_mode = PoolerMode::Direct.as_str().to_string();
    let cfg = PostgresConfig::validate(raw).expect("config");

    let mut repl = PostgresReplicationConnection::connect_logical(
        &cfg,
        "vertebra_slot",
        "vertebra_pub",
        Some(0x0000_0001_0000_0200),
    )
    .await
    .expect("connect_logical with recursive custom-type preloading");

    assert!(!repl.type_identities().contains_key(&INT8_OID));
    assert!(repl.type_catalog().type_info(42008).is_some());
    assert_eq!(
        repl.type_identities().get(&42008),
        Some(&("public".to_string(), "score_multirange".to_string()))
    );
    assert_eq!(
        repl.type_identities().get(&42001),
        Some(&("public".to_string(), "_doc_bundle".to_string()))
    );
    assert_eq!(
        repl.type_identities().get(&42002),
        Some(&("public".to_string(), "doc_bundle".to_string()))
    );
    assert_eq!(
        repl.type_identities().get(&42004),
        Some(&("public".to_string(), "score_dom".to_string()))
    );
    assert_eq!(
        repl.type_identities().get(&42005),
        Some(&("ext".to_string(), "vector".to_string()))
    );
    assert_eq!(
        repl.type_identities().get(&42006),
        Some(&("ext".to_string(), "halfvec".to_string()))
    );
    assert_eq!(
        repl.type_identities().get(&42007),
        Some(&("public".to_string(), "doc_status".to_string()))
    );

    let codec_ctx = repl.codec_context().clone();
    let mut decoder = PgOutputDecoder::with_type_identities(
        DEFAULT_MAX_RELATIONS,
        DEFAULT_MAX_RECORD_BYTES,
        repl.type_identities().clone(),
    );
    let mut wire = Vec::new();
    wire.extend_from_slice(&encode_type_frame(42005, "ext", "vector"));
    wire.extend_from_slice(&encode_type_frame(42002, "public", "doc_bundle"));
    wire.extend_from_slice(&encode_type_frame(42001, "public", "_doc_bundle"));
    wire.extend_from_slice(&encode_relation_frame(
        50001,
        "public",
        "embeddings",
        b'd',
        &[
            (0, "bundles", 42001, -1),
            (0, "score_span", 42003, -1),
            (0, "score", 42004, -1),
        ],
    ));
    wire.extend_from_slice(&encode_insert_frame(
        50001,
        b'N',
        &[
            RawTupleCell::Text(b"{\"(\\\"[1,2]\\\",\\\"[0.5]\\\",ready)\"}"),
            RawTupleCell::Text(b"[1,10)"),
            RawTupleCell::Text(b"7"),
        ],
    ));
    decoder.push(&wire).expect("push preloaded wire");
    let mut decoded = Vec::new();
    while let Some(f) = decoder
        .decode_next_frame(repl.type_catalog_mut(), &codec_ctx)
        .expect("decode with preloaded catalog")
    {
        decoded.push(f);
    }
    let Some(PgOutputFrame::Insert { new_tuple, .. }) = decoded.pop() else {
        panic!("expected Insert");
    };
    assert_eq!(
        new_tuple[0],
        PgOutputTupleValue::Value(PgValue::Array {
            values: vec![PgValue::Composite {
                type_oid: 42002,
                field_names: vec!["vec".to_string(), "half".to_string(), "status".to_string()],
                field_oids: vec![42005, 42006, 42007],
                values: vec![
                    PgValue::Vector {
                        values: vec![1.0, 2.0],
                        kind: VectorKind::Vector,
                    },
                    PgValue::Vector {
                        values: vec![0.5],
                        kind: VectorKind::HalfVec,
                    },
                    PgValue::Text("ready".to_string()),
                ],
            }],
            dimensions: vec![1],
            lower_bounds: vec![1],
            element_oid: 42002,
        })
    );
    assert_eq!(
        new_tuple[1],
        PgOutputTupleValue::Value(PgValue::Text("[1,10)".to_string()))
    );
    assert_eq!(new_tuple[2], PgOutputTupleValue::Value(PgValue::Int4(7)));

    repl.shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .expect("shutdown");
    srv.await.expect("server passed");
}
