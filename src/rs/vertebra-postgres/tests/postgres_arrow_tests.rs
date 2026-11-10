//! Lossless PostgreSQL Arrow schema, bounded `RecordBatch`, and `VTBARROW` SHM tests (`ch19`).

extern crate _postgres as vertebra_postgres;

use arrow_array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray, UInt32Array,
};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::DataType;
use bytes::BytesMut;
use std::io::Cursor;
use vertebra_db::postgres::{encode_pgvector_bit, encode_pgvector_f16, encode_pgvector_f32};
use vertebra_postgres::arrow::{
    CID_OID, DEFAULT_ARROW_BATCH_BYTES, DEFAULT_ARROW_BATCH_ROWS, MAX_ARROW_NESTING_DEPTH,
    POSTGRES_ARROW_VERSION_KEY, PostgresArrowBatchBuilder, PostgresArrowSchema, REGPROC_OID,
    UNKNOWN_OID, XID_OID, XML_OID,
};
use vertebra_postgres::codecs::numeric::{decode_numeric_text, encode_numeric_binary};
use vertebra_postgres::codecs::temporal::{PG_DATE_MIN_FINITE, PG_TS_MAX_FINITE, PG_TS_MIN_FINITE};
use vertebra_postgres::codecs::{
    BIT_OID, BOOL_OID, BPCHAR_OID, BYTEA_OID, CodecContext, DATE_OID, FLOAT4_OID, FLOAT8_OID,
    INT2_OID, INT4_OID, INT8_OID, INTERVAL_OID, JSON_OID, JSONB_OID, NAME_OID, NUMERIC_OID,
    OID_OID, RECORD_OID, TEXT_OID, TIME_OID, TIMESTAMP_OID, TIMESTAMPTZ_OID, UUID_OID, VARBIT_OID,
    VARCHAR_OID, encode_value_with_catalog,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{
    Column, Format, PG_DATE_NEG_INF, PG_INTERVAL_POS_INF, PG_TIME_MAX_MICROS, PG_TS_NEG_INF,
    PG_TS_POS_INF, PgNumeric, PgValue, TemporalKind, VectorKind,
};
use vertebra_postgres::types::{
    CompositeField, INT4_ARRAY_OID, NUMERIC_ARRAY_OID, TEXT_ARRAY_OID, TypeCatalog, TypeInfo,
    TypeKind,
};

fn col(name: &str, type_oid: u32, format: Format, type_modifier: i32) -> Column {
    Column {
        name: name.to_string(),
        type_oid,
        format,
        type_modifier,
        table_oid: 0,
        attribute_number: 0,
    }
}

#[test]
fn schema_preserves_duplicate_and_empty_column_names_and_version_metadata() {
    let catalog = TypeCatalog::new_with_builtins();
    let empty_schema = PostgresArrowSchema::from_columns(&[], &catalog).unwrap();
    assert_eq!(empty_schema.fields().len(), 0);
    assert_eq!(
        empty_schema
            .metadata()
            .get(POSTGRES_ARROW_VERSION_KEY)
            .map(String::as_str),
        Some("1")
    );

    let columns = vec![
        Column {
            name: "id".to_string(),
            type_oid: INT4_OID,
            format: Format::Binary,
            type_modifier: -1,
            table_oid: 100,
            attribute_number: 1,
        },
        Column {
            name: "id".to_string(),
            type_oid: INT8_OID,
            format: Format::Binary,
            type_modifier: -1,
            table_oid: 100,
            attribute_number: 2,
        },
        Column {
            name: String::new(),
            type_oid: TEXT_OID,
            format: Format::Text,
            type_modifier: -1,
            table_oid: 0,
            attribute_number: 0,
        },
    ];

    let schema = PostgresArrowSchema::from_columns(&columns, &catalog).unwrap();
    assert_eq!(
        schema
            .metadata()
            .get(POSTGRES_ARROW_VERSION_KEY)
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(schema.fields().len(), 3);

    let f0 = &schema.fields()[0];
    assert_eq!(f0.name(), "id");
    assert_eq!(f0.data_type(), &DataType::Int32);
    assert!(f0.is_nullable());
    assert_eq!(
        f0.metadata().get("postgres.oid").map(String::as_str),
        Some("23")
    );
    assert_eq!(
        f0.metadata().get("postgres.typmod").map(String::as_str),
        Some("-1")
    );
    assert_eq!(
        f0.metadata().get("postgres.type_name").map(String::as_str),
        Some("int4")
    );
    assert!(f0.metadata().contains_key("postgres.encoding"));

    let f1 = &schema.fields()[1];
    assert_eq!(f1.name(), "id");
    assert_eq!(f1.data_type(), &DataType::Int64);
    assert!(f1.is_nullable());
    assert_eq!(
        f1.metadata().get("postgres.oid").map(String::as_str),
        Some("20")
    );
    assert_eq!(
        f1.metadata().get("postgres.type_name").map(String::as_str),
        Some("int8")
    );

    let f2 = &schema.fields()[2];
    assert_eq!(f2.name(), "");
    assert_eq!(f2.data_type(), &DataType::Utf8);
    assert!(f2.is_nullable());
    assert_eq!(
        f2.metadata().get("postgres.oid").map(String::as_str),
        Some("25")
    );
    assert_eq!(
        f2.metadata().get("postgres.type_name").map(String::as_str),
        Some("text")
    );

    let empty_batch = RecordBatch::new_empty(schema);
    assert_eq!(empty_batch.num_rows(), 0);
    assert_eq!(empty_batch.num_columns(), 3);
}

#[test]
fn schema_maps_scalars_numeric_json_and_full_range_temporals() {
    let catalog = TypeCatalog::new_with_builtins();
    let numeric_typmod = ((38 << 16) | 10) + 4;
    let columns = vec![
        col("c_bool", BOOL_OID, Format::Binary, -1),
        col("c_i2", INT2_OID, Format::Binary, -1),
        col("c_i4", INT4_OID, Format::Binary, -1),
        col("c_i8", INT8_OID, Format::Binary, -1),
        col("c_oid", OID_OID, Format::Binary, -1),
        col("c_regproc", 24, Format::Binary, -1),
        col("c_xid", 28, Format::Binary, -1),
        col("c_cid", 29, Format::Binary, -1),
        col("c_f4", FLOAT4_OID, Format::Binary, -1),
        col("c_f8", FLOAT8_OID, Format::Binary, -1),
        col("c_text", TEXT_OID, Format::Binary, -1),
        col("c_varchar", VARCHAR_OID, Format::Binary, 68),
        col("c_bpchar", BPCHAR_OID, Format::Binary, 14),
        col("c_name", NAME_OID, Format::Binary, -1),
        col("c_xml", 142, Format::Text, -1),
        col("c_unknown", 705, Format::Text, -1),
        col("c_bytea", BYTEA_OID, Format::Binary, -1),
        col("c_uuid", UUID_OID, Format::Binary, -1),
        col("c_num", NUMERIC_OID, Format::Binary, numeric_typmod),
        col("c_json", JSON_OID, Format::Binary, -1),
        col("c_jsonb", JSONB_OID, Format::Binary, -1),
        col("c_date", DATE_OID, Format::Binary, -1),
        col("c_time", TIME_OID, Format::Binary, -1),
        col("c_ts", TIMESTAMP_OID, Format::Binary, 6),
        col("c_tstz", TIMESTAMPTZ_OID, Format::Binary, 6),
        col("c_iv", INTERVAL_OID, Format::Binary, -1),
    ];

    let schema = PostgresArrowSchema::from_columns(&columns, &catalog).unwrap();
    let field_by_name = |name: &str| schema.field_with_name(name).unwrap();

    assert_eq!(field_by_name("c_bool").data_type(), &DataType::Boolean);
    assert_eq!(field_by_name("c_i2").data_type(), &DataType::Int16);
    assert_eq!(field_by_name("c_i4").data_type(), &DataType::Int32);
    assert_eq!(field_by_name("c_i8").data_type(), &DataType::Int64);
    assert_eq!(field_by_name("c_oid").data_type(), &DataType::UInt32);
    assert_eq!(field_by_name("c_regproc").data_type(), &DataType::UInt32);
    assert_eq!(field_by_name("c_xid").data_type(), &DataType::UInt32);
    assert_eq!(field_by_name("c_cid").data_type(), &DataType::UInt32);
    assert_eq!(field_by_name("c_f4").data_type(), &DataType::Float32);
    assert_eq!(field_by_name("c_f8").data_type(), &DataType::Float64);
    assert_eq!(field_by_name("c_text").data_type(), &DataType::Utf8);
    assert_eq!(field_by_name("c_varchar").data_type(), &DataType::Utf8);
    assert_eq!(field_by_name("c_bpchar").data_type(), &DataType::Utf8);
    assert_eq!(field_by_name("c_name").data_type(), &DataType::Utf8);
    assert_eq!(field_by_name("c_xml").data_type(), &DataType::Utf8);
    assert_eq!(field_by_name("c_unknown").data_type(), &DataType::Utf8);
    assert_eq!(field_by_name("c_bytea").data_type(), &DataType::Binary);
    assert_eq!(
        field_by_name("c_uuid").data_type(),
        &DataType::FixedSizeBinary(16)
    );

    let f_num = field_by_name("c_num");
    assert_eq!(f_num.data_type(), &DataType::Utf8);
    assert_eq!(
        f_num
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("numeric-decimal")
    );
    assert_eq!(
        f_num.metadata().get("postgres.typmod").map(String::as_str),
        Some(numeric_typmod.to_string().as_str())
    );

    assert_eq!(field_by_name("c_json").data_type(), &DataType::Utf8);
    assert_eq!(
        field_by_name("c_json")
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("json")
    );
    assert_eq!(field_by_name("c_jsonb").data_type(), &DataType::Utf8);
    assert_eq!(
        field_by_name("c_jsonb")
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("jsonb")
    );

    let check_temporal_struct = |name: &str, expected_val_dt: DataType| {
        let DataType::Struct(children) = field_by_name(name).data_type() else {
            panic!("{name} must be Struct");
        };
        let names_types: Vec<(&str, &DataType)> = children
            .iter()
            .map(|f| (f.name().as_str(), f.data_type()))
            .collect();
        assert_eq!(
            names_types,
            vec![
                ("kind", &DataType::Int8),
                ("value", &expected_val_dt),
                ("wire_text", &DataType::Utf8),
                ("date_style", &DataType::Utf8),
                ("time_zone", &DataType::Utf8),
            ]
        );
    };
    check_temporal_struct("c_date", DataType::Int32);
    check_temporal_struct("c_ts", DataType::Int64);
    check_temporal_struct("c_tstz", DataType::Int64);

    let f_time = field_by_name("c_time");
    assert_eq!(f_time.data_type(), &DataType::Int64);
    assert_eq!(
        f_time
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("pg-time-micros")
    );

    let DataType::Struct(iv_fields) = field_by_name("c_iv").data_type() else {
        panic!("interval must be Struct");
    };
    let iv_shape: Vec<(&str, &DataType)> = iv_fields
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();
    assert_eq!(
        iv_shape,
        vec![
            ("months", &DataType::Int32),
            ("days", &DataType::Int32),
            ("microseconds", &DataType::Int64),
        ]
    );
}

#[test]
fn schema_maps_nested_array_composite_domain_enum_vector_halfvec_and_varbit() {
    let mut catalog = TypeCatalog::new_with_builtins();
    let num_arr_typmod = ((12 << 16) | 2) + 4;

    catalog.insert_custom_type(TypeInfo {
        oid: 42001,
        schema: "public".to_string(),
        name: "vector".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "vector".to_string(),
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 42002,
        schema: "public".to_string(),
        name: "halfvec".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "halfvec".to_string(),
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 50001,
        schema: "public".to_string(),
        name: "pair_t".to_string(),
        kind: TypeKind::Composite {
            fields: vec![
                CompositeField {
                    name: "a".to_string(),
                    type_oid: INT4_OID,
                    type_modifier: -1,
                },
                CompositeField {
                    name: "b".to_string(),
                    type_oid: NUMERIC_ARRAY_OID,
                    type_modifier: num_arr_typmod,
                },
            ],
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 50002,
        schema: "public".to_string(),
        name: "user_uuid".to_string(),
        kind: TypeKind::Domain { base_oid: UUID_OID },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 50003,
        schema: "public".to_string(),
        name: "mood".to_string(),
        kind: TypeKind::Enum,
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 50004,
        schema: "public".to_string(),
        name: "vec_dom".to_string(),
        kind: TypeKind::Domain { base_oid: 42001 },
        delimiter: b',',
    });

    let columns = vec![
        col("comp", 50001, Format::Binary, -1),
        col("dom_uuid", 50002, Format::Binary, -1),
        col("dom_vec", 50004, Format::Binary, 4),
        col("enum_col", 50003, Format::Text, -1),
        col("vec_fixed", 42001, Format::Binary, 3),
        col("vec_max", 42001, Format::Binary, 16_000),
        col("half_dyn", 42002, Format::Binary, -1),
        col("bit_col", BIT_OID, Format::Binary, 8),
        col("varbit_col", VARBIT_OID, Format::Binary, -1),
        col("anon_rec", RECORD_OID, Format::Text, -1),
    ];

    let schema = PostgresArrowSchema::from_columns(&columns, &catalog).unwrap();

    let comp_field = schema.field_with_name("comp").unwrap();
    assert_eq!(
        comp_field
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("pg-composite")
    );
    let DataType::Struct(comp_children) = comp_field.data_type() else {
        panic!("expected Struct for composite");
    };
    assert_eq!(comp_children.len(), 2);
    assert_eq!(comp_children[0].name(), "a");
    assert_eq!(comp_children[0].data_type(), &DataType::Int32);
    assert_eq!(comp_children[1].name(), "b");
    assert_eq!(
        comp_children[1]
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("pg-array")
    );
    let DataType::Struct(arr_children) = comp_children[1].data_type() else {
        panic!("expected array Struct");
    };
    assert_eq!(arr_children.len(), 3);
    assert_eq!(arr_children[0].name(), "dimensions");
    assert!(
        matches!(arr_children[0].data_type(), DataType::List(f) if f.data_type() == &DataType::Int32)
    );
    assert_eq!(arr_children[1].name(), "lower_bounds");
    assert!(
        matches!(arr_children[1].data_type(), DataType::List(f) if f.data_type() == &DataType::Int32)
    );
    assert_eq!(arr_children[2].name(), "values");
    let DataType::List(elem_field) = arr_children[2].data_type() else {
        panic!("expected List for array values");
    };
    assert_eq!(elem_field.data_type(), &DataType::Utf8);
    assert_eq!(
        elem_field
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("numeric-decimal")
    );
    assert_eq!(
        elem_field
            .metadata()
            .get("postgres.typmod")
            .map(String::as_str),
        Some(num_arr_typmod.to_string().as_str())
    );

    let dom = schema.field_with_name("dom_uuid").unwrap();
    assert_eq!(dom.data_type(), &DataType::FixedSizeBinary(16));
    assert_eq!(
        dom.metadata().get("postgres.oid").map(String::as_str),
        Some("50002")
    );
    assert_eq!(
        dom.metadata().get("postgres.base_oid").map(String::as_str),
        Some("2950")
    );

    let dom_vec = schema.field_with_name("dom_vec").unwrap();
    assert!(matches!(
        dom_vec.data_type(),
        DataType::FixedSizeList(f, 4) if f.data_type() == &DataType::Float32
    ));
    assert_eq!(
        dom_vec.metadata().get("postgres.oid").map(String::as_str),
        Some("50004")
    );
    assert_eq!(
        dom_vec
            .metadata()
            .get("postgres.base_oid")
            .map(String::as_str),
        Some("42001")
    );

    let enum_col = schema.field_with_name("enum_col").unwrap();
    assert_eq!(enum_col.data_type(), &DataType::Utf8);
    assert_eq!(
        enum_col
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("pg-enum")
    );

    let vec_fixed = schema.field_with_name("vec_fixed").unwrap();
    assert!(matches!(
        vec_fixed.data_type(),
        DataType::FixedSizeList(f, 3) if f.data_type() == &DataType::Float32
    ));
    assert_eq!(
        vec_fixed
            .metadata()
            .get("postgres.type_name")
            .map(String::as_str),
        Some("vector")
    );
    assert_eq!(
        vec_fixed
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("pgvector")
    );

    let vec_max = schema.field_with_name("vec_max").unwrap();
    assert!(matches!(
        vec_max.data_type(),
        DataType::FixedSizeList(f, 16_000) if f.data_type() == &DataType::Float32
    ));

    let half_dyn = schema.field_with_name("half_dyn").unwrap();
    assert!(matches!(
        half_dyn.data_type(),
        DataType::List(f) if f.data_type() == &DataType::Float32
    ));
    assert_eq!(
        half_dyn
            .metadata()
            .get("postgres.type_name")
            .map(String::as_str),
        Some("halfvec")
    );
    assert_eq!(
        half_dyn
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("pghalfvec")
    );

    for bit_name in ["bit_col", "varbit_col"] {
        let bf = schema.field_with_name(bit_name).unwrap();
        assert_eq!(
            bf.metadata().get("postgres.encoding").map(String::as_str),
            Some("pg-bits")
        );
        let DataType::Struct(bit_children) = bf.data_type() else {
            panic!("{bit_name} must be Struct");
        };
        assert_eq!(bit_children.len(), 2);
        assert_eq!(bit_children[0].name(), "bytes");
        assert_eq!(bit_children[0].data_type(), &DataType::Binary);
        assert_eq!(bit_children[1].name(), "bit_length");
        assert_eq!(bit_children[1].data_type(), &DataType::Int32);
    }

    let anon = schema.field_with_name("anon_rec").unwrap();
    assert_eq!(anon.data_type(), &DataType::Utf8);
}

#[test]
fn schema_rejects_unknown_binary_oid_anonymous_binary_record_and_invalid_vector_typmod() {
    let mut catalog = TypeCatalog::new_with_builtins();
    catalog.insert_custom_type(TypeInfo {
        oid: 42001,
        schema: "public".to_string(),
        name: "vector".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "vector".to_string(),
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 42002,
        schema: "public".to_string(),
        name: "halfvec".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "halfvec".to_string(),
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 99_998,
        schema: "public".to_string(),
        name: "_mystery".to_string(),
        kind: TypeKind::Array {
            element_oid: 99_999,
            delimiter: b',',
        },
        delimiter: b',',
    });

    let ok_schema = PostgresArrowSchema::from_columns(
        &[
            col("u", 99_999, Format::Text, -1),
            col("ua", 99_998, Format::Text, -1),
        ],
        &catalog,
    )
    .unwrap();
    let uf = ok_schema.field_with_name("u").unwrap();
    assert_eq!(uf.data_type(), &DataType::Utf8);
    assert_eq!(
        uf.metadata().get("postgres.oid").map(String::as_str),
        Some("99999")
    );
    assert_eq!(
        uf.metadata().get("postgres.encoding").map(String::as_str),
        Some("unknown-text")
    );

    for bad_col in [
        col("bin_unk", 99_999, Format::Binary, -1),
        col("bin_unk_arr", 99_998, Format::Binary, -1),
        col("bin_rec", RECORD_OID, Format::Binary, -1),
        col("zero_oid", 0, Format::Text, -1),
        col("vec_zero", 42001, Format::Binary, 0),
        col("vec_neg2", 42001, Format::Binary, -2),
        col("vec_huge", 42001, Format::Binary, 16_001),
        col("half_zero", 42002, Format::Binary, 0),
        col("half_huge", 42002, Format::Binary, 16_001),
    ] {
        let err = PostgresArrowSchema::from_columns(std::slice::from_ref(&bad_col), &catalog)
            .expect_err(&format!("column {} must fail", bad_col.name));
        assert!(
            matches!(err, PostgresError::Data(_)),
            "expected Data error for {}, got {err:?}",
            bad_col.name
        );
    }
}

#[test]
fn schema_enforces_catalog_cycle_and_depth_32_guards() {
    let mut self_cycle = TypeCatalog::new_with_builtins();
    self_cycle.insert_custom_type(TypeInfo {
        oid: 60001,
        schema: "public".to_string(),
        name: "node_t".to_string(),
        kind: TypeKind::Composite {
            fields: vec![CompositeField {
                name: "next".to_string(),
                type_oid: 60001,
                type_modifier: -1,
            }],
        },
        delimiter: b',',
    });
    assert!(matches!(
        PostgresArrowSchema::from_columns(&[col("c", 60001, Format::Text, -1)], &self_cycle),
        Err(PostgresError::Data(_))
    ));

    let mut indirect_cycle = TypeCatalog::new_with_builtins();
    indirect_cycle.insert_custom_type(TypeInfo {
        oid: 60010,
        schema: "public".to_string(),
        name: "tree_t".to_string(),
        kind: TypeKind::Composite {
            fields: vec![CompositeField {
                name: "children".to_string(),
                type_oid: 60011,
                type_modifier: -1,
            }],
        },
        delimiter: b',',
    });
    indirect_cycle.insert_custom_type(TypeInfo {
        oid: 60011,
        schema: "public".to_string(),
        name: "_tree_t".to_string(),
        kind: TypeKind::Array {
            element_oid: 60010,
            delimiter: b',',
        },
        delimiter: b',',
    });
    assert!(matches!(
        PostgresArrowSchema::from_columns(&[col("c", 60010, Format::Text, -1)], &indirect_cycle),
        Err(PostgresError::Data(_))
    ));

    let mut domain_cycle = TypeCatalog::new_with_builtins();
    domain_cycle.insert_custom_type(TypeInfo {
        oid: 60020,
        schema: "public".to_string(),
        name: "d1".to_string(),
        kind: TypeKind::Domain { base_oid: 60021 },
        delimiter: b',',
    });
    domain_cycle.insert_custom_type(TypeInfo {
        oid: 60021,
        schema: "public".to_string(),
        name: "d2".to_string(),
        kind: TypeKind::Domain { base_oid: 60020 },
        delimiter: b',',
    });
    assert!(matches!(
        PostgresArrowSchema::from_columns(&[col("d", 60020, Format::Text, -1)], &domain_cycle),
        Err(PostgresError::Data(_))
    ));

    let build_chain_catalog = |depth: usize| -> (TypeCatalog, u32) {
        let mut cat = TypeCatalog::new_with_builtins();
        let mut prev_oid = INT4_OID;
        for step in 0..depth {
            let oid = 70_000 + step as u32;
            if step % 2 == 0 {
                cat.insert_custom_type(TypeInfo {
                    oid,
                    schema: "public".to_string(),
                    name: format!("wrap_{step}"),
                    kind: TypeKind::Composite {
                        fields: vec![CompositeField {
                            name: "inner".to_string(),
                            type_oid: prev_oid,
                            type_modifier: -1,
                        }],
                    },
                    delimiter: b',',
                });
            } else {
                cat.insert_custom_type(TypeInfo {
                    oid,
                    schema: "public".to_string(),
                    name: format!("_wrap_{step}"),
                    kind: TypeKind::Array {
                        element_oid: prev_oid,
                        delimiter: b',',
                    },
                    delimiter: b',',
                });
            }
            prev_oid = oid;
        }
        (cat, prev_oid)
    };

    let (cat_ok, oid_32) = build_chain_catalog(MAX_ARROW_NESTING_DEPTH);
    PostgresArrowSchema::from_columns(&[col("ok32", oid_32, Format::Binary, -1)], &cat_ok)
        .expect("depth 32 should succeed");

    let (cat_too_deep, oid_33) = build_chain_catalog(MAX_ARROW_NESTING_DEPTH + 1);
    assert!(matches!(
        PostgresArrowSchema::from_columns(
            &[col("deep33", oid_33, Format::Binary, -1)],
            &cat_too_deep
        ),
        Err(PostgresError::Data(_))
    ));
}

#[test]
fn scalar_decodes_integers_unsigned_oids_floats_uuid_bytea_and_unicode() {
    let mut catalog = TypeCatalog::new_with_builtins();
    catalog.insert_custom_type(TypeInfo {
        oid: 50010,
        schema: "public".to_string(),
        name: "mood".to_string(),
        kind: TypeKind::Enum,
        delimiter: b',',
    });

    let columns = vec![
        col("c_bool", BOOL_OID, Format::Binary, -1),
        col("c_i2", INT2_OID, Format::Binary, -1),
        col("c_i4", INT4_OID, Format::Binary, -1),
        col("c_i8", INT8_OID, Format::Binary, -1),
        col("c_oid", OID_OID, Format::Binary, -1),
        col("c_regproc", REGPROC_OID, Format::Binary, -1),
        col("c_xid", XID_OID, Format::Text, -1),
        col("c_cid", CID_OID, Format::Binary, -1),
        col("c_f4", FLOAT4_OID, Format::Binary, -1),
        col("c_f8", FLOAT8_OID, Format::Binary, -1),
        col("c_text", TEXT_OID, Format::Binary, -1),
        col("c_xml", XML_OID, Format::Text, -1),
        col("c_unk_lit", UNKNOWN_OID, Format::Text, -1),
        col("c_bytea", BYTEA_OID, Format::Binary, -1),
        col("c_uuid", UUID_OID, Format::Binary, -1),
        col("c_enum", 50010, Format::Binary, -1),
        col("c_custom", 99_999, Format::Text, -1),
    ];

    let mut builder = PostgresArrowBatchBuilder::try_new(
        &columns,
        &catalog,
        DEFAULT_ARROW_BATCH_ROWS,
        DEFAULT_ARROW_BATCH_BYTES,
    )
    .unwrap();
    let ctx = CodecContext::default();

    let i2_min = i16::MIN.to_be_bytes();
    let i4_min = i32::MIN.to_be_bytes();
    let i8_min = i64::MIN.to_be_bytes();
    let u32_max = u32::MAX.to_be_bytes();
    let regproc_val = 2_500_000_000u32.to_be_bytes();
    let cid_val = 42u32.to_be_bytes();
    let f4_nan = f32::NAN.to_be_bytes();
    let f8_ninf = f64::NEG_INFINITY.to_be_bytes();
    let uuid_bytes: [u8; 16] = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32,
        0x10,
    ];

    assert!(
        builder
            .append_data_row(
                &[
                    Some(&[1u8][..]),
                    Some(&i2_min),
                    Some(&i4_min),
                    Some(&i8_min),
                    Some(&u32_max),
                    Some(&regproc_val),
                    Some(b"4294967295"),
                    Some(&cid_val),
                    Some(&f4_nan),
                    Some(&f8_ninf),
                    Some(b""),
                    Some(b"<doc attr=\"1\"/>"),
                    Some("héllo 🚀".as_bytes()),
                    Some(b""),
                    Some(&uuid_bytes),
                    Some(b"happy"),
                    Some(b"(1,2)"),
                ],
                &ctx,
                &catalog,
            )
            .unwrap()
            .is_none()
    );

    assert!(
        builder
            .append_pg_row(&[
                PgValue::Bool(false),
                PgValue::Int2(i16::MAX),
                PgValue::Int4(i32::MAX),
                PgValue::Int8(i64::MAX),
                PgValue::Int8(0),
                PgValue::Int8(7),
                PgValue::Int8(i64::from(u32::MAX)),
                PgValue::Int8(0),
                PgValue::Float4(-0.0),
                PgValue::Float8(f64::INFINITY),
                PgValue::Null,
                PgValue::Null,
                PgValue::Text("plain".to_string()),
                PgValue::Null,
                PgValue::Null,
                PgValue::Text("sad".to_string()),
                PgValue::Null,
            ])
            .unwrap()
            .is_none()
    );

    let mut bad_row = vec![PgValue::Null; columns.len()];
    bad_row[4] = PgValue::Int8(-1);
    assert!(matches!(
        builder.append_pg_row(&bad_row),
        Err(PostgresError::Data(_))
    ));
    assert_eq!(builder.buffered_rows(), 2);

    let batch = builder
        .flush_batch()
        .unwrap()
        .expect("expected 2-row batch");
    assert_eq!(batch.num_rows(), 2);

    let c_bool = batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(c_bool.value(0));
    assert!(!c_bool.value(1));

    let c_i2 = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int16Array>()
        .unwrap();
    assert_eq!(c_i2.value(0), i16::MIN);
    assert_eq!(c_i2.value(1), i16::MAX);

    let c_i4 = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(c_i4.value(0), i32::MIN);
    assert_eq!(c_i4.value(1), i32::MAX);

    let c_i8 = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(c_i8.value(0), i64::MIN);
    assert_eq!(c_i8.value(1), i64::MAX);

    let c_oid = batch
        .column(4)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(c_oid.value(0), u32::MAX);
    assert_eq!(c_oid.value(1), 0);

    let c_regproc = batch
        .column(5)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(c_regproc.value(0), 2_500_000_000);
    assert_eq!(c_regproc.value(1), 7);

    let c_xid = batch
        .column(6)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(c_xid.value(0), u32::MAX);
    assert_eq!(c_xid.value(1), u32::MAX);

    let c_f4 = batch
        .column(8)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert!(c_f4.value(0).is_nan());
    assert_eq!(c_f4.value(1), 0.0);
    assert!(c_f4.value(1).is_sign_negative());

    let c_f8 = batch
        .column(9)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(c_f8.value(0), f64::NEG_INFINITY);
    assert_eq!(c_f8.value(1), f64::INFINITY);

    let c_text = batch
        .column(10)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(c_text.is_valid(0));
    assert_eq!(c_text.value(0), "");
    assert!(c_text.is_null(1));

    let c_unk_lit = batch
        .column(12)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(c_unk_lit.value(0), "héllo 🚀");
    assert_eq!(c_unk_lit.value(1), "plain");

    let c_bytea = batch
        .column(13)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert!(c_bytea.is_valid(0));
    assert_eq!(c_bytea.value(0), b"");
    assert!(c_bytea.is_null(1));

    let c_uuid = batch
        .column(14)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!(c_uuid.is_valid(0));
    assert_eq!(c_uuid.value(0), uuid_bytes.as_slice());
    assert!(c_uuid.is_null(1));

    let c_enum = batch
        .column(15)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(c_enum.value(0), "happy");
    assert_eq!(c_enum.value(1), "sad");
}

#[test]
fn scalar_preserves_canonical_numeric_scale_specials_and_json_null_vs_sql_null() {
    let catalog = TypeCatalog::new_with_builtins();
    let columns = vec![
        col("c_num", NUMERIC_OID, Format::Binary, ((38 << 16) | 4) + 4),
        col("c_json", JSON_OID, Format::Binary, -1),
        col("c_jsonb", JSONB_OID, Format::Binary, -1),
    ];
    let mut builder =
        PostgresArrowBatchBuilder::try_new(&columns, &catalog, 64, 1024 * 1024).unwrap();
    let ctx = CodecContext::default();

    let encode_num = |lit: &str| -> Vec<u8> {
        let n = decode_numeric_text(lit).unwrap();
        let mut buf = BytesMut::new();
        encode_numeric_binary(&n, &mut buf).unwrap();
        buf.to_vec()
    };

    let n_zero = encode_num("0.0000");
    let n_big = encode_num("-123456789012345678901234567890.0010");
    let n_nan = encode_num("NaN");

    builder
        .append_data_row(
            &[Some(n_zero.as_slice()), Some(b"null"), Some(b"\x01null")],
            &ctx,
            &catalog,
        )
        .unwrap();
    builder
        .append_data_row(&[Some(n_big.as_slice()), None, None], &ctx, &catalog)
        .unwrap();
    builder
        .append_data_row(
            &[
                Some(n_nan.as_slice()),
                Some(b"{\"k\":1}"),
                Some(b"\x01[true,false]"),
            ],
            &ctx,
            &catalog,
        )
        .unwrap();
    builder
        .append_pg_row(&[
            PgValue::Numeric(PgNumeric::PosInf),
            PgValue::Json(b"null".to_vec()),
            PgValue::Json(b"0".to_vec()),
        ])
        .unwrap();
    builder
        .append_pg_row(&[
            PgValue::Numeric(PgNumeric::NegInf),
            PgValue::Null,
            PgValue::Null,
        ])
        .unwrap();
    builder
        .append_pg_row(&[PgValue::Null, PgValue::Null, PgValue::Null])
        .unwrap();

    let batch = builder.flush_batch().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 6);

    let nums = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(nums.value(0), "0.0000");
    assert_eq!(nums.value(1), "-123456789012345678901234567890.0010");
    assert_eq!(nums.value(2), "NaN");
    assert_eq!(nums.value(3), "Infinity");
    assert_eq!(nums.value(4), "-Infinity");
    assert!(nums.is_null(5));

    let jsons = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(jsons.is_valid(0));
    assert_eq!(jsons.value(0), "null");
    assert!(jsons.is_null(1));
    assert_eq!(jsons.value(2), "{\"k\":1}");
    assert!(jsons.is_valid(3));
    assert_eq!(jsons.value(3), "null");

    let jsonbs = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(jsonbs.is_valid(0));
    assert_eq!(jsonbs.value(0), "null");
    assert!(jsonbs.is_null(1));
    assert_eq!(jsonbs.value(2), "[true,false]");
    assert_eq!(jsonbs.value(3), "0");
    assert!(jsonbs.is_null(4));
}

#[test]
fn scalar_preserves_full_range_temporals_time_24h_intervals_and_wire_text() {
    let catalog = TypeCatalog::new_with_builtins();
    let columns = vec![
        col("c_date", DATE_OID, Format::Binary, -1),
        col("c_time", TIME_OID, Format::Binary, -1),
        col("c_ts", TIMESTAMP_OID, Format::Binary, 6),
        col("c_tstz", TIMESTAMPTZ_OID, Format::Text, 6),
        col("c_iv", INTERVAL_OID, Format::Binary, -1),
    ];

    let mut builder =
        PostgresArrowBatchBuilder::try_new(&columns, &catalog, 32, 1024 * 1024).unwrap();
    let ctx = CodecContext {
        date_style: "Postgres, MDY".to_string(),
        interval_style: "postgres".to_string(),
        time_zone: "Europe/Paris".to_string(),
        ..CodecContext::default()
    };

    let d_ninf = PG_DATE_NEG_INF.to_be_bytes();
    let t_24h = PG_TIME_MAX_MICROS.to_be_bytes();
    let ts_min = PG_TS_MIN_FINITE.to_be_bytes();
    let mut iv_mixed = [0u8; 16];
    iv_mixed[0..8].copy_from_slice(&(-900_000i64).to_be_bytes());
    iv_mixed[8..12].copy_from_slice(&3i32.to_be_bytes());
    iv_mixed[12..16].copy_from_slice(&(-14i32).to_be_bytes());

    builder
        .append_data_row(
            &[
                Some(d_ninf.as_slice()),
                Some(&t_24h),
                Some(&ts_min),
                Some(b"infinity"),
                Some(&iv_mixed),
            ],
            &ctx,
            &catalog,
        )
        .unwrap();

    builder
        .append_data_row(
            &[
                Some(PG_DATE_MIN_FINITE.to_be_bytes().as_slice()),
                Some(&0i64.to_be_bytes()),
                Some(&PG_TS_NEG_INF.to_be_bytes()),
                Some(b"Wed Jul 09 07:30:00 2026 MET"),
                None,
            ],
            &ctx,
            &catalog,
        )
        .unwrap();

    builder
        .append_pg_row(&[
            PgValue::Null,
            PgValue::Null,
            PgValue::Timestamp(PG_TS_POS_INF),
            PgValue::TimestampTz(PG_TS_MAX_FINITE),
            PgValue::Interval {
                months: PG_INTERVAL_POS_INF.0,
                days: PG_INTERVAL_POS_INF.1,
                microseconds: PG_INTERVAL_POS_INF.2,
            },
        ])
        .unwrap();

    builder
        .append_pg_row(&[
            PgValue::TemporalText {
                kind: TemporalKind::Date,
                wire_text: "epoch-custom".to_string(),
                date_style: "German, DMY".to_string(),
                time_zone: "UTC".to_string(),
            },
            PgValue::Time(123_456),
            PgValue::Null,
            PgValue::Null,
            PgValue::Null,
        ])
        .unwrap();

    let batch = builder.flush_batch().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 4);

    let c_date = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(c_date.is_valid(0));
    assert!(c_date.is_valid(1));
    assert!(c_date.is_null(2));
    assert!(c_date.is_valid(3));
    let date_kind = c_date
        .column_by_name("kind")
        .unwrap()
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    let date_val = c_date
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let date_wt = c_date
        .column_by_name("wire_text")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(date_kind.value(0), -1);
    assert!(date_val.is_valid(0));
    assert_eq!(date_val.value(0), 0);
    assert!(date_wt.is_null(0));

    assert_eq!(date_kind.value(1), 0);
    assert_eq!(date_val.value(1), PG_DATE_MIN_FINITE);

    assert!(date_kind.is_null(2));
    assert!(date_val.is_null(2));

    assert_eq!(date_kind.value(3), 2);
    assert!(date_val.is_null(3));
    assert_eq!(date_wt.value(3), "epoch-custom");

    let c_time = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(c_time.value(0), PG_TIME_MAX_MICROS);
    assert_eq!(c_time.value(1), 0);
    assert!(c_time.is_null(2));
    assert_eq!(c_time.value(3), 123_456);

    let c_tstz = batch
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(c_tstz.is_valid(0));
    assert!(c_tstz.is_valid(1));
    assert!(c_tstz.is_valid(2));
    assert!(c_tstz.is_null(3));
    let tstz_kind = c_tstz
        .column_by_name("kind")
        .unwrap()
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    let tstz_val = c_tstz
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let tstz_wt = c_tstz
        .column_by_name("wire_text")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let tstz_ds = c_tstz
        .column_by_name("date_style")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let tstz_tz = c_tstz
        .column_by_name("time_zone")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(tstz_kind.value(0), 1);
    assert_eq!(tstz_val.value(0), 0);
    assert!(tstz_wt.is_null(0));

    assert_eq!(tstz_kind.value(1), 2);
    assert!(tstz_val.is_null(1));
    assert_eq!(tstz_wt.value(1), "Wed Jul 09 07:30:00 2026 MET");
    assert_eq!(tstz_ds.value(1), "Postgres, MDY");
    assert_eq!(tstz_tz.value(1), "Europe/Paris");

    assert_eq!(tstz_kind.value(2), 0);
    assert_eq!(tstz_val.value(2), PG_TS_MAX_FINITE);

    assert!(tstz_kind.is_null(3));
    assert!(tstz_wt.is_null(3));

    let c_iv = batch
        .column(4)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(c_iv.is_valid(0));
    assert!(c_iv.is_null(1));
    assert!(c_iv.is_valid(2));
    assert!(c_iv.is_null(3));
    let iv_m = c_iv
        .column_by_name("months")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let iv_d = c_iv
        .column_by_name("days")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let iv_us = c_iv
        .column_by_name("microseconds")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        (iv_m.value(0), iv_d.value(0), iv_us.value(0)),
        (-14, 3, -900_000)
    );
    assert!(iv_m.is_null(1) && iv_d.is_null(1) && iv_us.is_null(1));
    assert_eq!(
        (iv_m.value(2), iv_d.value(2), iv_us.value(2)),
        PG_INTERVAL_POS_INF
    );
}

#[test]
fn batch_bounds_flush_by_row_count_byte_budget_reject_oversize_and_preserve_empty_duplicates() {
    let catalog = TypeCatalog::new_with_builtins();
    let dup_cols = vec![
        col("dup", INT4_OID, Format::Binary, -1),
        col("dup", TEXT_OID, Format::Binary, -1),
        col("", BYTEA_OID, Format::Binary, -1),
    ];

    assert!(matches!(
        PostgresArrowBatchBuilder::try_new(&dup_cols, &catalog, 0, 1024),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        PostgresArrowBatchBuilder::try_new(&dup_cols, &catalog, 10, 0),
        Err(PostgresError::Data(_))
    ));

    let mut empty_builder =
        PostgresArrowBatchBuilder::try_new(&dup_cols, &catalog, 4, 8192).unwrap();
    let empty_direct = empty_builder.finish_empty();
    assert_eq!(empty_direct.num_rows(), 0);
    assert_eq!(empty_direct.num_columns(), 3);
    assert_eq!(empty_direct.schema().field(0).name(), "dup");
    assert_eq!(empty_direct.schema().field(1).name(), "dup");
    assert_eq!(empty_direct.schema().field(2).name(), "");
    let empty_once = empty_builder.finish_empty_or_tail().unwrap().unwrap();
    assert_eq!(empty_once.num_rows(), 0);
    assert_eq!(empty_once.num_columns(), 3);
    assert!(empty_builder.finish_empty_or_tail().unwrap().is_none());

    let mut row_builder = PostgresArrowBatchBuilder::try_new(
        &[col("n", INT4_OID, Format::Binary, -1)],
        &catalog,
        2,
        8192,
    )
    .unwrap();
    assert!(matches!(
        row_builder.append_pg_row(&[]),
        Err(PostgresError::Data(_))
    ));

    let mut counts = Vec::new();
    for v in 1..=5 {
        if let Some(b) = row_builder.append_pg_row(&[PgValue::Int4(v)]).unwrap() {
            counts.push(b.num_rows());
        }
    }
    if let Some(tail) = row_builder.finish_empty_or_tail().unwrap() {
        counts.push(tail.num_rows());
    }
    assert!(row_builder.finish_empty_or_tail().unwrap().is_none());
    assert_eq!(counts, vec![2, 2, 1]);

    let ctx = CodecContext::default();
    let mut byte_builder = PostgresArrowBatchBuilder::try_new(
        &[col("payload", BYTEA_OID, Format::Binary, -1)],
        &catalog,
        64,
        7 * 1024,
    )
    .unwrap();
    let chunk_3k = vec![0xABu8; 3 * 1024];
    assert!(
        byte_builder
            .append_data_row(&[Some(&chunk_3k)], &ctx, &catalog)
            .unwrap()
            .is_none()
    );
    assert!(
        byte_builder
            .append_data_row(&[Some(&chunk_3k)], &ctx, &catalog)
            .unwrap()
            .is_none()
    );
    assert_eq!(byte_builder.buffered_rows(), 2);
    assert!(byte_builder.would_exceed_bytes(3 * 1024).unwrap());

    let flushed = byte_builder
        .append_data_row(&[Some(&chunk_3k)], &ctx, &catalog)
        .unwrap()
        .expect("3rd 3KiB row must flush prior 2-row batch");
    assert_eq!(flushed.num_rows(), 2);
    assert_eq!(byte_builder.buffered_rows(), 1);

    assert!(
        byte_builder
            .append_data_row(&[Some(&chunk_3k)], &ctx, &catalog)
            .unwrap()
            .is_none()
    );
    let second = byte_builder.flush_batch().unwrap().unwrap();
    assert_eq!(second.num_rows(), 2);
    assert_eq!(byte_builder.buffered_rows(), 0);
    assert_eq!(byte_builder.buffered_bytes(), 0);

    let mut cap_builder = PostgresArrowBatchBuilder::try_new(
        &[col("s", TEXT_OID, Format::Binary, -1)],
        &catalog,
        16,
        8 * 1024,
    )
    .unwrap();
    assert!(
        cap_builder
            .append_pg_row(&[PgValue::Text("keep-me".to_string())])
            .unwrap()
            .is_none()
    );
    let oversize = "x".repeat(9 * 1024);
    assert!(matches!(
        cap_builder.would_exceed_bytes(oversize.len()),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        cap_builder.append_data_row(&[Some(oversize.as_bytes())], &ctx, &catalog),
        Err(PostgresError::Data(_))
    ));
    assert!(matches!(
        cap_builder.append_pg_row(&[PgValue::Text(oversize)]),
        Err(PostgresError::Data(_))
    ));
    assert_eq!(cap_builder.buffered_rows(), 1);
    let kept = cap_builder.flush_batch().unwrap().unwrap();
    assert_eq!(kept.num_rows(), 1);
    assert_eq!(
        kept.column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "keep-me"
    );
}

#[test]
fn nested_builds_pg_arrays_null_empty_null_elements_and_2d_negative_lower_bounds() {
    let catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();
    let columns = vec![
        col("arr_bin", INT4_ARRAY_OID, Format::Binary, -1),
        col("arr_txt", INT4_ARRAY_OID, Format::Text, -1),
    ];
    let mut builder =
        PostgresArrowBatchBuilder::try_new(&columns, &catalog, 64, 1024 * 1024).unwrap();

    let encode_bin = |v: &PgValue| -> Vec<u8> {
        let mut out = BytesMut::new();
        assert!(
            encode_value_with_catalog(v, INT4_ARRAY_OID, Format::Binary, &mut out, &ctx, &catalog)
                .unwrap()
        );
        out.to_vec()
    };

    let arr_empty = PgValue::Array {
        values: vec![],
        dimensions: vec![],
        lower_bounds: vec![],
        element_oid: INT4_OID,
    };
    let arr_with_null = PgValue::Array {
        values: vec![PgValue::Null, PgValue::Int4(7)],
        dimensions: vec![2],
        lower_bounds: vec![1],
        element_oid: INT4_OID,
    };
    let arr_2d_neg_lb = PgValue::Array {
        values: vec![
            PgValue::Int4(10),
            PgValue::Int4(20),
            PgValue::Int4(30),
            PgValue::Int4(40),
        ],
        dimensions: vec![2, 2],
        lower_bounds: vec![-2, 3],
        element_oid: INT4_OID,
    };

    let b_empty = encode_bin(&arr_empty);
    let b_with_null = encode_bin(&arr_with_null);
    let b_2d = encode_bin(&arr_2d_neg_lb);

    builder
        .append_data_row(&[None, None], &ctx, &catalog)
        .unwrap();
    builder
        .append_data_row(&[Some(b_empty.as_slice()), Some(b"{}")], &ctx, &catalog)
        .unwrap();
    builder
        .append_data_row(
            &[Some(b_with_null.as_slice()), Some(b"{NULL,7}")],
            &ctx,
            &catalog,
        )
        .unwrap();
    builder
        .append_data_row(
            &[
                Some(b_2d.as_slice()),
                Some(b"[-2:-1][3:4]={{10,20},{30,40}}"),
            ],
            &ctx,
            &catalog,
        )
        .unwrap();

    for bad_arr in [
        PgValue::Array {
            values: vec![PgValue::Int4(1)],
            dimensions: vec![1, 1],
            lower_bounds: vec![1],
            element_oid: INT4_OID,
        },
        PgValue::Array {
            values: vec![PgValue::Int4(1)],
            dimensions: vec![1; 7],
            lower_bounds: vec![1; 7],
            element_oid: INT4_OID,
        },
        PgValue::Array {
            values: vec![PgValue::Int4(1), PgValue::Int4(2), PgValue::Int4(3)],
            dimensions: vec![2, 2],
            lower_bounds: vec![1, 1],
            element_oid: INT4_OID,
        },
        PgValue::Array {
            values: vec![],
            dimensions: vec![0],
            lower_bounds: vec![1],
            element_oid: INT4_OID,
        },
        PgValue::Array {
            values: vec![PgValue::Int4(1), PgValue::Int4(2)],
            dimensions: vec![2],
            lower_bounds: vec![i32::MAX],
            element_oid: INT4_OID,
        },
    ] {
        assert!(matches!(
            builder.append_pg_row(&[bad_arr, PgValue::Null]),
            Err(PostgresError::Data(_))
        ));
    }
    assert_eq!(builder.buffered_rows(), 4);

    let batch = builder.flush_batch().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 4);

    for col_idx in [0, 1] {
        let s = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(s.is_null(0));
        assert!(s.is_valid(1));
        assert!(s.is_valid(2));
        assert!(s.is_valid(3));

        let dims = s
            .column_by_name("dimensions")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let lbs = s
            .column_by_name("lower_bounds")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let vals = s
            .column_by_name("values")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert!(dims.is_null(0));
        assert!(lbs.is_null(0));
        assert!(vals.is_null(0));

        assert!(dims.is_valid(1));
        assert_eq!(dims.value(1).len(), 0);
        assert_eq!(lbs.value(1).len(), 0);
        assert_eq!(vals.value(1).len(), 0);

        let d2 = dims.value(2);
        let d2_i = d2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(d2_i.values().as_ref(), &[2]);
        let lb2 = lbs.value(2);
        let lb2_i = lb2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(lb2_i.values().as_ref(), &[1]);
        let v2 = vals.value(2);
        let v2_i = v2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v2_i.len(), 2);
        assert!(v2_i.is_null(0));
        assert_eq!(v2_i.value(1), 7);

        let d3 = dims.value(3);
        let d3_i = d3.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(d3_i.values().as_ref(), &[2, 2]);
        let lb3 = lbs.value(3);
        let lb3_i = lb3.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(lb3_i.values().as_ref(), &[-2, 3]);
        let v3 = vals.value(3);
        let v3_i = v3.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v3_i.values().as_ref(), &[10, 20, 30, 40]);
    }
}

#[test]
fn nested_preserves_composite_null_vs_all_null_fields_domains_enums_across_batches_and_ipc() {
    let mut catalog = TypeCatalog::new_with_builtins();
    catalog.insert_custom_type(TypeInfo {
        oid: 50010,
        schema: "public".to_string(),
        name: "mood".to_string(),
        kind: TypeKind::Enum,
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 50020,
        schema: "public".to_string(),
        name: "user_uuid".to_string(),
        kind: TypeKind::Domain { base_oid: UUID_OID },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 50021,
        schema: "public".to_string(),
        name: "price_list".to_string(),
        kind: TypeKind::Domain {
            base_oid: NUMERIC_ARRAY_OID,
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 50001,
        schema: "public".to_string(),
        name: "pair_t".to_string(),
        kind: TypeKind::Composite {
            fields: vec![
                CompositeField {
                    name: "label".to_string(),
                    type_oid: TEXT_OID,
                    type_modifier: -1,
                },
                CompositeField {
                    name: "score".to_string(),
                    type_oid: INT4_OID,
                    type_modifier: -1,
                },
            ],
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 50002,
        schema: "public".to_string(),
        name: "_pair_t".to_string(),
        kind: TypeKind::Array {
            element_oid: 50001,
            delimiter: b',',
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 50003,
        schema: "public".to_string(),
        name: "audit_t".to_string(),
        kind: TypeKind::Composite {
            fields: vec![
                CompositeField {
                    name: "prices".to_string(),
                    type_oid: 50021,
                    type_modifier: ((12 << 16) | 2) + 4,
                },
                CompositeField {
                    name: "status".to_string(),
                    type_oid: 50010,
                    type_modifier: -1,
                },
                CompositeField {
                    name: "actor".to_string(),
                    type_oid: 50020,
                    type_modifier: -1,
                },
                CompositeField {
                    name: "pairs".to_string(),
                    type_oid: 50002,
                    type_modifier: -1,
                },
            ],
        },
        delimiter: b',',
    });

    let columns = vec![
        col("pair", 50001, Format::Binary, -1),
        col("audit", 50003, Format::Text, -1),
    ];
    let mut builder =
        PostgresArrowBatchBuilder::try_new(&columns, &catalog, 2, 1024 * 1024).unwrap();
    let expected_schema = builder.schema().clone();
    let ctx = CodecContext::default();

    let encode_comp = |val: &PgValue, oid: u32, fmt: Format| -> Vec<u8> {
        let mut out = BytesMut::new();
        assert!(encode_value_with_catalog(val, oid, fmt, &mut out, &ctx, &catalog).unwrap());
        out.to_vec()
    };

    let uuid_a: [u8; 16] = [0xAA; 16];
    let audit_row0 = PgValue::Composite {
        type_oid: 50003,
        field_names: vec![
            "prices".to_string(),
            "status".to_string(),
            "actor".to_string(),
            "pairs".to_string(),
        ],
        field_oids: vec![50021, 50010, 50020, 50002],
        values: vec![
            PgValue::Array {
                values: vec![
                    PgValue::Numeric(decode_numeric_text("12.30").unwrap()),
                    PgValue::Numeric(PgNumeric::NaN),
                ],
                dimensions: vec![2],
                lower_bounds: vec![1],
                element_oid: NUMERIC_OID,
            },
            PgValue::Text("happy".to_string()),
            PgValue::Uuid(uuid_a),
            PgValue::Array {
                values: vec![
                    PgValue::Composite {
                        type_oid: 50001,
                        field_names: vec!["label".to_string(), "score".to_string()],
                        field_oids: vec![TEXT_OID, INT4_OID],
                        values: vec![PgValue::Text("a".to_string()), PgValue::Int4(1)],
                    },
                    PgValue::Null,
                    PgValue::Composite {
                        type_oid: 50001,
                        field_names: vec!["label".to_string(), "score".to_string()],
                        field_oids: vec![TEXT_OID, INT4_OID],
                        values: vec![PgValue::Null, PgValue::Null],
                    },
                ],
                dimensions: vec![3],
                lower_bounds: vec![0],
                element_oid: 50001,
            },
        ],
    };
    let pair_all_null = PgValue::Composite {
        type_oid: 50001,
        field_names: vec!["label".to_string(), "score".to_string()],
        field_oids: vec![TEXT_OID, INT4_OID],
        values: vec![PgValue::Null, PgValue::Null],
    };
    let audit_all_null = PgValue::Composite {
        type_oid: 50003,
        field_names: vec![
            "prices".to_string(),
            "status".to_string(),
            "actor".to_string(),
            "pairs".to_string(),
        ],
        field_oids: vec![50021, 50010, 50020, 50002],
        values: vec![PgValue::Null, PgValue::Null, PgValue::Null, PgValue::Null],
    };

    let b_audit0 = encode_comp(&audit_row0, 50003, Format::Text);
    let b_pair_all_null = encode_comp(&pair_all_null, 50001, Format::Binary);
    let b_audit_all_null = encode_comp(&audit_all_null, 50003, Format::Text);

    assert!(
        builder
            .append_data_row(&[None, Some(b_audit0.as_slice())], &ctx, &catalog)
            .unwrap()
            .is_none()
    );

    assert!(matches!(
        builder.append_pg_row(&[
            PgValue::Composite {
                type_oid: 50001,
                field_names: vec!["label".to_string()],
                field_oids: vec![TEXT_OID],
                values: vec![PgValue::Text("wrong_arity".to_string())],
            },
            PgValue::Null,
        ]),
        Err(PostgresError::Data(_))
    ));
    assert_eq!(builder.buffered_rows(), 1);

    let batch1 = builder
        .append_data_row(
            &[
                Some(b_pair_all_null.as_slice()),
                Some(b_audit_all_null.as_slice()),
            ],
            &ctx,
            &catalog,
        )
        .unwrap()
        .expect("max_rows=2 must flush batch 1");

    assert!(
        builder
            .append_pg_row(&[
                PgValue::Composite {
                    type_oid: 50001,
                    field_names: vec!["label".to_string(), "score".to_string()],
                    field_oids: vec![TEXT_OID, INT4_OID],
                    values: vec![PgValue::Text("z".to_string()), PgValue::Int4(-9)],
                },
                PgValue::Null,
            ])
            .unwrap()
            .is_none()
    );
    let batch2 = builder.finish_empty_or_tail().unwrap().unwrap();

    let mut ipc_buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buf, &expected_schema).unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        writer.finish().unwrap();
    }

    let reader = StreamReader::try_new(Cursor::new(ipc_buf), None).unwrap();
    assert_eq!(reader.schema(), expected_schema);
    let decoded_batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    assert_eq!(decoded_batches.len(), 2);
    assert_eq!(decoded_batches[0].num_rows(), 2);
    assert_eq!(decoded_batches[1].num_rows(), 1);

    let b0_pair = decoded_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(b0_pair.is_null(0));
    assert!(b0_pair.is_valid(1));
    assert!(b0_pair.column_by_name("label").unwrap().is_null(1));
    assert!(b0_pair.column_by_name("score").unwrap().is_null(1));

    let b0_audit = decoded_batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(b0_audit.is_valid(0));
    assert!(b0_audit.is_valid(1));
    for attr_name in ["prices", "status", "actor", "pairs"] {
        assert!(b0_audit.column_by_name(attr_name).unwrap().is_null(1));
    }

    let prices_struct = b0_audit
        .column_by_name("prices")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let prices_vals = prices_struct
        .column_by_name("values")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let prices_str = prices_vals.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(prices_str.value(0), "12.30");
    assert_eq!(prices_str.value(1), "NaN");

    let status_arr = b0_audit
        .column_by_name("status")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(status_arr.value(0), "happy");

    let actor_arr = b0_audit
        .column_by_name("actor")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(actor_arr.value(0), uuid_a.as_slice());

    let pairs_arr = b0_audit
        .column_by_name("pairs")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let pairs_lbs = pairs_arr
        .column_by_name("lower_bounds")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    assert_eq!(
        pairs_lbs
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[0]
    );
    let pairs_items_ref = pairs_arr
        .column_by_name("values")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let pairs_items = pairs_items_ref
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(pairs_items.len(), 3);
    assert!(pairs_items.is_valid(0));
    assert!(pairs_items.is_null(1));
    assert!(pairs_items.is_valid(2));
    assert_eq!(
        pairs_items
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
    assert_eq!(
        pairs_items
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        1
    );
    assert!(pairs_items.column_by_name("label").unwrap().is_null(2));
    assert!(pairs_items.column_by_name("score").unwrap().is_null(2));

    let b1_pair = decoded_batches[1]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(b1_pair.is_valid(0));
    assert_eq!(
        b1_pair
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "z"
    );
    assert_eq!(
        b1_pair
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        -9
    );
    let b1_audit = decoded_batches[1]
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(b1_audit.is_null(0));
}

#[test]
fn vector_enforces_fixed_and_dynamic_pgvector_and_halfvec_dimensions_and_f16_fidelity() {
    let mut catalog = TypeCatalog::new_with_builtins();
    catalog.insert_custom_type(TypeInfo {
        oid: 42001,
        schema: "public".to_string(),
        name: "vector".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "vector".to_string(),
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 42002,
        schema: "public".to_string(),
        name: "halfvec".to_string(),
        kind: TypeKind::Extension {
            schema: "public".to_string(),
            name: "halfvec".to_string(),
        },
        delimiter: b',',
    });
    catalog.insert_custom_type(TypeInfo {
        oid: 42003,
        schema: "public".to_string(),
        name: "_vector".to_string(),
        kind: TypeKind::Array {
            element_oid: 42001,
            delimiter: b',',
        },
        delimiter: b',',
    });

    let columns = vec![
        col("v_fixed", 42001, Format::Binary, 3),
        col("v_dyn", 42001, Format::Binary, -1),
        col("h_fixed", 42002, Format::Binary, 2),
        col("h_dyn", 42002, Format::Binary, -1),
        col("v_arr", 42003, Format::Binary, 2),
    ];
    let mut builder =
        PostgresArrowBatchBuilder::try_new(&columns, &catalog, 16, 1024 * 1024).unwrap();
    let ctx = CodecContext::default();

    let f16_max = 65504.0f32;
    let f16_min_pos = f32::from_bits(0x3880_0000);
    let f16_subnormal = f32::from_bits(0x3380_0000);

    let wf_v0 = encode_pgvector_f32(&[1.0, -0.0, 0.5]).unwrap();
    let wd_v0 = encode_pgvector_f32(&[42.25]).unwrap();
    let hf_v0 = encode_pgvector_f16(&[f16_max, f16_min_pos]).unwrap();
    let hd_v0 = encode_pgvector_f16(&[-0.0, f16_subnormal]).unwrap();
    let mut varr_buf = BytesMut::new();
    encode_value_with_catalog(
        &PgValue::Array {
            values: vec![
                PgValue::Vector {
                    values: vec![1.0, 2.0],
                    kind: VectorKind::Vector,
                },
                PgValue::Vector {
                    values: vec![3.0, 4.0],
                    kind: VectorKind::Vector,
                },
            ],
            dimensions: vec![2],
            lower_bounds: vec![1],
            element_oid: 42001,
        },
        42003,
        Format::Binary,
        &mut varr_buf,
        &ctx,
        &catalog,
    )
    .unwrap();

    builder
        .append_data_row(
            &[
                Some(wf_v0.as_slice()),
                Some(wd_v0.as_slice()),
                Some(hf_v0.as_slice()),
                Some(hd_v0.as_slice()),
                Some(varr_buf.as_ref()),
            ],
            &ctx,
            &catalog,
        )
        .unwrap();

    builder
        .append_pg_row(&[
            PgValue::Null,
            PgValue::Vector {
                values: vec![1.0, 2.0, 3.0, 4.0],
                kind: VectorKind::Vector,
            },
            PgValue::Null,
            PgValue::Vector {
                values: vec![1.5, -2.5, 3.25],
                kind: VectorKind::HalfVec,
            },
            PgValue::Null,
        ])
        .unwrap();

    for bad_row in [
        vec![
            PgValue::Vector {
                values: vec![1.0, 2.0],
                kind: VectorKind::Vector,
            },
            PgValue::Null,
            PgValue::Null,
            PgValue::Null,
            PgValue::Null,
        ],
        vec![
            PgValue::Vector {
                values: vec![1.0, f32::NAN, 0.0],
                kind: VectorKind::Vector,
            },
            PgValue::Null,
            PgValue::Null,
            PgValue::Null,
            PgValue::Null,
        ],
        vec![
            PgValue::Null,
            PgValue::Vector {
                values: vec![],
                kind: VectorKind::Vector,
            },
            PgValue::Null,
            PgValue::Null,
            PgValue::Null,
        ],
        vec![
            PgValue::Null,
            PgValue::Vector {
                values: vec![0.0; 16_001],
                kind: VectorKind::Vector,
            },
            PgValue::Null,
            PgValue::Null,
            PgValue::Null,
        ],
        vec![
            PgValue::Null,
            PgValue::Null,
            PgValue::Null,
            PgValue::Vector {
                values: vec![70_000.0],
                kind: VectorKind::HalfVec,
            },
            PgValue::Null,
        ],
        vec![
            PgValue::Null,
            PgValue::Null,
            PgValue::Null,
            PgValue::Vector {
                values: vec![1.0],
                kind: VectorKind::Vector,
            },
            PgValue::Null,
        ],
    ] {
        assert!(matches!(
            builder.append_pg_row(&bad_row),
            Err(PostgresError::Data(_))
        ));
    }
    assert_eq!(builder.buffered_rows(), 2);

    let batch = builder.flush_batch().unwrap().unwrap();
    assert_eq!(
        batch
            .schema()
            .field(3)
            .metadata()
            .get("postgres.encoding")
            .map(String::as_str),
        Some("pghalfvec")
    );

    let vf = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert!(vf.is_valid(0));
    assert!(vf.is_null(1));
    let vf0 = vf.value(0);
    let vf0_f = vf0.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(vf0_f.value(0), 1.0);
    assert_eq!(vf0_f.value(1), 0.0);
    assert!(vf0_f.value(1).is_sign_negative());
    assert_eq!(vf0_f.value(2), 0.5);

    let vd = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(
        vd.value(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[42.25]
    );
    assert_eq!(
        vd.value(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1.0, 2.0, 3.0, 4.0]
    );

    let hf = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert!(hf.is_valid(0));
    assert!(hf.is_null(1));
    assert_eq!(
        hf.value(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[f16_max, f16_min_pos]
    );

    let hd = batch
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let hd0 = hd.value(0);
    let hd0_f = hd0.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(hd0_f.value(0), 0.0);
    assert!(hd0_f.value(0).is_sign_negative());
    assert_eq!(hd0_f.value(1), f16_subnormal);
    assert_eq!(
        hd.value(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1.5, -2.5, 3.25]
    );
}

#[test]
fn nested_validates_bit_and_varbit_lengths_and_rejects_nonzero_padding_and_oversize_nested_rows() {
    let catalog = TypeCatalog::new_with_builtins();
    let ctx = CodecContext::default();
    let columns = vec![
        col("b_col", BIT_OID, Format::Binary, 9),
        col("vb_col", VARBIT_OID, Format::Text, -1),
    ];
    let mut builder =
        PostgresArrowBatchBuilder::try_new(&columns, &catalog, 16, 1024 * 1024).unwrap();

    let b0 = encode_pgvector_bit(b"", 0).unwrap();
    let b1 = encode_pgvector_bit(b"\x80", 1).unwrap();
    let b9 = encode_pgvector_bit(b"\xff\x80", 9).unwrap();

    builder
        .append_data_row(&[Some(b0.as_slice()), Some(b"")], &ctx, &catalog)
        .unwrap();
    builder
        .append_data_row(&[Some(b1.as_slice()), Some(b"1")], &ctx, &catalog)
        .unwrap();
    builder
        .append_data_row(&[Some(b9.as_slice()), Some(b"111111111")], &ctx, &catalog)
        .unwrap();
    builder
        .append_pg_row(&[PgValue::Null, PgValue::Null])
        .unwrap();

    for bad_bits in [
        PgValue::BitString {
            data: vec![0x81],
            bit_length: 1,
        },
        PgValue::BitString {
            data: vec![0x80, 0x00],
            bit_length: 1,
        },
        PgValue::BitString {
            data: vec![],
            bit_length: -1,
        },
    ] {
        assert!(matches!(
            builder.append_pg_row(&[bad_bits, PgValue::Null]),
            Err(PostgresError::Data(_))
        ));
    }
    assert_eq!(builder.buffered_rows(), 4);

    let batch = builder.flush_batch().unwrap().unwrap();
    for col_idx in [0, 1] {
        let bs = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(bs.is_valid(0));
        assert!(bs.is_valid(1));
        assert!(bs.is_valid(2));
        assert!(bs.is_null(3));

        let bytes_col = bs
            .column_by_name("bytes")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let len_col = bs
            .column_by_name("bit_length")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(bytes_col.value(0), b"");
        assert_eq!(len_col.value(0), 0);
        assert_eq!(bytes_col.value(1), b"\x80");
        assert_eq!(len_col.value(1), 1);
        assert_eq!(bytes_col.value(2), b"\xff\x80");
        assert_eq!(len_col.value(2), 9);
    }

    let mut small_arr_builder = PostgresArrowBatchBuilder::try_new(
        &[col("tags", TEXT_ARRAY_OID, Format::Binary, -1)],
        &catalog,
        16,
        256,
    )
    .unwrap();
    let big_arr = PgValue::Array {
        values: (0..8)
            .map(|_| PgValue::Text("x".repeat(64)))
            .collect::<Vec<_>>(),
        dimensions: vec![8],
        lower_bounds: vec![1],
        element_oid: TEXT_OID,
    };
    assert!(matches!(
        small_arr_builder.append_pg_row(&[big_arr]),
        Err(PostgresError::Data(_))
    ));
    assert_eq!(small_arr_builder.buffered_rows(), 0);

    let mut deep_cat = TypeCatalog::new_with_builtins();
    let mut cur_oid = INT4_OID;
    let mut cur_val = PgValue::Int4(99);
    for step in 0..MAX_ARROW_NESTING_DEPTH {
        let oid = 75_000 + step as u32;
        if step % 2 == 0 {
            deep_cat.insert_custom_type(TypeInfo {
                oid,
                schema: "public".to_string(),
                name: format!("c_{step}"),
                kind: TypeKind::Composite {
                    fields: vec![CompositeField {
                        name: "v".to_string(),
                        type_oid: cur_oid,
                        type_modifier: -1,
                    }],
                },
                delimiter: b',',
            });
            cur_val = PgValue::Composite {
                type_oid: oid,
                field_names: vec!["v".to_string()],
                field_oids: vec![cur_oid],
                values: vec![cur_val],
            };
        } else {
            deep_cat.insert_custom_type(TypeInfo {
                oid,
                schema: "public".to_string(),
                name: format!("_c_{step}"),
                kind: TypeKind::Array {
                    element_oid: cur_oid,
                    delimiter: b',',
                },
                delimiter: b',',
            });
            cur_val = PgValue::Array {
                values: vec![cur_val],
                dimensions: vec![1],
                lower_bounds: vec![1],
                element_oid: cur_oid,
            };
        }
        cur_oid = oid;
    }
    let mut deep_builder = PostgresArrowBatchBuilder::try_new(
        &[col("deep32", cur_oid, Format::Binary, -1)],
        &deep_cat,
        8,
        64 * 1024,
    )
    .unwrap();
    deep_builder.append_pg_row(&[cur_val]).unwrap();
    deep_builder.append_pg_row(&[PgValue::Null]).unwrap();
    let deep_batch = deep_builder.flush_batch().unwrap().unwrap();
    assert_eq!(deep_batch.num_rows(), 2);
    assert!(deep_batch.column(0).is_valid(0));
    assert!(deep_batch.column(0).is_null(1));
}
