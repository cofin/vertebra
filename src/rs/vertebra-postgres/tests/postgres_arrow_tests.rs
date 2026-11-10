//! Lossless PostgreSQL Arrow schema, bounded `RecordBatch`, and `VTBARROW` SHM tests (`ch19`).

extern crate _postgres as vertebra_postgres;

use arrow_array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, StructArray, UInt32Array,
};
use arrow_schema::DataType;
use bytes::BytesMut;
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
    VARCHAR_OID,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{
    Column, Format, PG_DATE_NEG_INF, PG_INTERVAL_POS_INF, PG_TIME_MAX_MICROS, PG_TS_NEG_INF,
    PG_TS_POS_INF, PgNumeric, PgValue, TemporalKind,
};
use vertebra_postgres::types::{
    CompositeField, NUMERIC_ARRAY_OID, TypeCatalog, TypeInfo, TypeKind,
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
