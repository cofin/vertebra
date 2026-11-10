//! Lossless PostgreSQL Arrow schema, bounded `RecordBatch`, and `VTBARROW` SHM tests (`ch19`).

extern crate _postgres as vertebra_postgres;

use arrow_array::RecordBatch;
use arrow_schema::DataType;
use vertebra_postgres::arrow::{
    MAX_ARROW_NESTING_DEPTH, POSTGRES_ARROW_VERSION_KEY, PostgresArrowSchema,
};
use vertebra_postgres::codecs::{
    BIT_OID, BOOL_OID, BPCHAR_OID, BYTEA_OID, DATE_OID, FLOAT4_OID, FLOAT8_OID, INT2_OID, INT4_OID,
    INT8_OID, INTERVAL_OID, JSON_OID, JSONB_OID, NAME_OID, NUMERIC_OID, OID_OID, RECORD_OID,
    TEXT_OID, TIME_OID, TIMESTAMP_OID, TIMESTAMPTZ_OID, UUID_OID, VARBIT_OID, VARCHAR_OID,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Column, Format};
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
