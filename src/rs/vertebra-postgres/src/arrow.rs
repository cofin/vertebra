//! Lossless PostgreSQL-to-Arrow schemas, bounded `RecordBatch` builders, and `VTBARROW` SHM serialization.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};

use crate::codecs::{
    BIT_OID, BOOL_OID, BOX_OID, BPCHAR_OID, BYTEA_OID, DATE_OID, FLOAT4_OID, FLOAT8_OID, INT2_OID,
    INT4_OID, INT8_OID, INTERVAL_OID, JSON_OID, JSONB_OID, NAME_OID, NUMERIC_OID, OID_OID,
    RECORD_OID, TEXT_OID, TIME_OID, TIMESTAMP_OID, TIMESTAMPTZ_OID, UUID_OID, VARBIT_OID,
    VARCHAR_OID,
};
use crate::errors::PostgresError;
use crate::result::{Column, Format};
use crate::types::{CompositeField, MAX_DOMAIN_DEPTH, TypeCatalog, TypeInfo, TypeKind};

/// PostgreSQL `regproc` scalar OID (`24`, unsigned 32-bit procedure OID).
pub const REGPROC_OID: u32 = 24;
/// PostgreSQL `xid` scalar OID (`28`, unsigned 32-bit transaction ID).
pub const XID_OID: u32 = 28;
/// PostgreSQL `cid` scalar OID (`29`, unsigned 32-bit command ID).
pub const CID_OID: u32 = 29;
/// PostgreSQL `xml` scalar OID (`142`, UTF-8 XML document).
pub const XML_OID: u32 = 142;
/// PostgreSQL `unknown` literal OID (`705`, UTF-8 string literal).
pub const UNKNOWN_OID: u32 = 705;

/// Arrow `Schema::metadata` key stamping the lossless PostgreSQL Arrow contract version.
pub const POSTGRES_ARROW_VERSION_KEY: &str = "postgres.arrow.version";
/// Current lossless PostgreSQL Arrow contract version (`"1"`).
pub const POSTGRES_ARROW_VERSION: &str = "1";
/// Per-field metadata key recording the outer PostgreSQL type OID.
pub const POSTGRES_FIELD_OID_KEY: &str = "postgres.oid";
/// Per-field metadata key recording the PostgreSQL `atttypmod` (`-1` when unconstrained).
pub const POSTGRES_FIELD_TYPMOD_KEY: &str = "postgres.typmod";
/// Per-field metadata key recording the PostgreSQL type name.
pub const POSTGRES_FIELD_TYPE_NAME_KEY: &str = "postgres.type_name";
/// Per-field metadata key recording the lossless structural Arrow encoding tag.
pub const POSTGRES_FIELD_ENCODING_KEY: &str = "postgres.encoding";
/// Per-field metadata key recording the resolved non-domain base OID for domain columns.
pub const POSTGRES_FIELD_BASE_OID_KEY: &str = "postgres.base_oid";

/// Maximum recursive catalog composite/array/domain nesting depth (`32`).
pub const MAX_ARROW_NESTING_DEPTH: usize = 32;
/// Maximum `pgvector` `vector` / `halfvec` fixed dimension (`16_000`).
pub const MAX_PGVECTOR_ARROW_DIM: i32 = 16_000;

/// Lossless PostgreSQL-to-Arrow schema resolver (`postgres.arrow.version = "1"`).
#[derive(Clone, Copy, Debug, Default)]
pub struct PostgresArrowSchema;

impl PostgresArrowSchema {
    /// Resolve an ordered slice of PostgreSQL [`Column`] descriptors into a lossless Arrow [`Schema`].
    ///
    /// Preserves column order, duplicate column names, and empty (`""`) names verbatim without
    /// map deduplication. Every outer and nested [`Field`] is marked `nullable = true` so empty
    /// and all-`NULL` batches retain identical structural types and `"postgres.*"` metadata.
    pub fn from_columns(
        columns: &[Column],
        catalog: &TypeCatalog,
    ) -> Result<Arc<Schema>, PostgresError> {
        let mut fields: Vec<FieldRef> = Vec::with_capacity(columns.len());
        let mut active_lineage: Vec<u32> = Vec::new();
        for column in columns {
            let field = resolve_arrow_field(
                &column.name,
                column.type_oid,
                column.type_modifier,
                column.format,
                catalog,
                0,
                &mut active_lineage,
            )?;
            debug_assert!(active_lineage.is_empty());
            fields.push(Arc::new(field));
        }

        let mut schema_meta = HashMap::with_capacity(1);
        schema_meta.insert(
            POSTGRES_ARROW_VERSION_KEY.to_string(),
            POSTGRES_ARROW_VERSION.to_string(),
        );
        Ok(Arc::new(Schema::new_with_metadata(fields, schema_meta)))
    }
}

pub(crate) fn resolve_domain_with_lineage(
    outer_oid: u32,
    catalog: &TypeCatalog,
    active_lineage: &[u32],
) -> Result<(u32, Option<String>, Vec<u32>), PostgresError> {
    if outer_oid == 0 {
        return Err(PostgresError::Data(
            "PostgreSQL Arrow column type_oid must be positive (>= 1), got 0".to_string(),
        ));
    }

    let mut current = outer_oid;
    let mut outer_domain_name: Option<String> = None;
    let mut domain_chain: Vec<u32> = Vec::new();

    for step in 0..=MAX_DOMAIN_DEPTH {
        if active_lineage.contains(&current) || domain_chain.contains(&current) {
            return Err(PostgresError::Data(format!(
                "cyclic PostgreSQL catalog type lineage detected at OID {current}"
            )));
        }
        match catalog.type_info(current) {
            Some(TypeInfo {
                name,
                kind: TypeKind::Domain { base_oid },
                ..
            }) => {
                if *base_oid == 0 {
                    return Err(PostgresError::Data(format!(
                        "PostgreSQL domain OID {current} has invalid base_oid 0"
                    )));
                }
                if step == MAX_DOMAIN_DEPTH {
                    return Err(PostgresError::Data(format!(
                        "PostgreSQL domain chain for OID {outer_oid} exceeds max depth {MAX_DOMAIN_DEPTH}"
                    )));
                }
                if outer_domain_name.is_none() {
                    outer_domain_name = Some(name.clone());
                }
                domain_chain.push(current);
                current = *base_oid;
            }
            _ => return Ok((current, outer_domain_name, domain_chain)),
        }
    }

    Err(PostgresError::Data(format!(
        "PostgreSQL domain chain for OID {outer_oid} exceeds max depth {MAX_DOMAIN_DEPTH}"
    )))
}

fn temporal_struct_data_type(value_type: DataType) -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("kind", DataType::Int8, true),
        Field::new("value", value_type, true),
        Field::new("wire_text", DataType::Utf8, true),
        Field::new("date_style", DataType::Utf8, true),
        Field::new("time_zone", DataType::Utf8, true),
    ]))
}

fn interval_struct_data_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("months", DataType::Int32, true),
        Field::new("days", DataType::Int32, true),
        Field::new("microseconds", DataType::Int64, true),
    ]))
}

fn bitstring_struct_data_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("bytes", DataType::Binary, true),
        Field::new("bit_length", DataType::Int32, true),
    ]))
}

fn array_struct_data_type(element_field: Field) -> DataType {
    let dim_item = Arc::new(Field::new("item", DataType::Int32, true));
    let lb_item = Arc::new(Field::new("item", DataType::Int32, true));
    DataType::Struct(Fields::from(vec![
        Field::new("dimensions", DataType::List(dim_item), true),
        Field::new("lower_bounds", DataType::List(lb_item), true),
        Field::new("values", DataType::List(Arc::new(element_field)), true),
    ]))
}

fn vector_data_type(type_name: &str, typmod: i32) -> Result<DataType, PostgresError> {
    let item_field = Arc::new(Field::new("item", DataType::Float32, true));
    if typmod == -1 {
        return Ok(DataType::List(item_field));
    }
    if !(1..=MAX_PGVECTOR_ARROW_DIM).contains(&typmod) {
        return Err(PostgresError::Data(format!(
            "invalid {type_name} typmod dimension {typmod} (expected -1 or 1..={MAX_PGVECTOR_ARROW_DIM})"
        )));
    }
    Ok(DataType::FixedSizeList(item_field, typmod))
}

fn resolve_composite_data_type(
    base_oid: u32,
    fields: &[CompositeField],
    format: Format,
    catalog: &TypeCatalog,
    depth: usize,
    active_lineage: &mut Vec<u32>,
) -> Result<(DataType, &'static str), PostgresError> {
    if base_oid == RECORD_OID && fields.is_empty() {
        return match format {
            Format::Text => Ok((DataType::Utf8, "record-text")),
            Format::Binary => Err(PostgresError::Data(
                "anonymous PostgreSQL record (OID 2249) requires text format (0) for lossless Arrow projection"
                    .to_string(),
            )),
        };
    }

    let mut arrow_fields: Vec<FieldRef> = Vec::with_capacity(fields.len());
    for attr in fields {
        let child = resolve_arrow_field(
            &attr.name,
            attr.type_oid,
            attr.type_modifier,
            format,
            catalog,
            depth + 1,
            active_lineage,
        )?;
        arrow_fields.push(Arc::new(child));
    }
    Ok((DataType::Struct(Fields::from(arrow_fields)), "pg-composite"))
}

fn resolve_base_type(
    base_oid: u32,
    typmod: i32,
    format: Format,
    catalog: &TypeCatalog,
    depth: usize,
    active_lineage: &mut Vec<u32>,
) -> Result<(DataType, String, &'static str), PostgresError> {
    match base_oid {
        BOOL_OID => Ok((DataType::Boolean, "bool".to_string(), "scalar")),
        INT2_OID => Ok((DataType::Int16, "int2".to_string(), "scalar")),
        INT4_OID => Ok((DataType::Int32, "int4".to_string(), "scalar")),
        INT8_OID => Ok((DataType::Int64, "int8".to_string(), "scalar")),
        OID_OID => Ok((DataType::UInt32, "oid".to_string(), "scalar")),
        REGPROC_OID => Ok((DataType::UInt32, "regproc".to_string(), "scalar")),
        XID_OID => Ok((DataType::UInt32, "xid".to_string(), "scalar")),
        CID_OID => Ok((DataType::UInt32, "cid".to_string(), "scalar")),
        FLOAT4_OID => Ok((DataType::Float32, "float4".to_string(), "scalar")),
        FLOAT8_OID => Ok((DataType::Float64, "float8".to_string(), "scalar")),
        TEXT_OID => Ok((DataType::Utf8, "text".to_string(), "scalar")),
        VARCHAR_OID => Ok((DataType::Utf8, "varchar".to_string(), "scalar")),
        BPCHAR_OID => Ok((DataType::Utf8, "bpchar".to_string(), "scalar")),
        NAME_OID => Ok((DataType::Utf8, "name".to_string(), "scalar")),
        XML_OID => Ok((DataType::Utf8, "xml".to_string(), "scalar")),
        UNKNOWN_OID => Ok((DataType::Utf8, "unknown".to_string(), "scalar")),
        BYTEA_OID => Ok((DataType::Binary, "bytea".to_string(), "scalar")),
        UUID_OID => Ok((DataType::FixedSizeBinary(16), "uuid".to_string(), "scalar")),
        JSON_OID => Ok((DataType::Utf8, "json".to_string(), "json")),
        JSONB_OID => Ok((DataType::Utf8, "jsonb".to_string(), "jsonb")),
        NUMERIC_OID => Ok((DataType::Utf8, "numeric".to_string(), "numeric-decimal")),
        DATE_OID => Ok((
            temporal_struct_data_type(DataType::Int32),
            "date".to_string(),
            "pg-date",
        )),
        TIME_OID => Ok((DataType::Int64, "time".to_string(), "pg-time-micros")),
        TIMESTAMP_OID => Ok((
            temporal_struct_data_type(DataType::Int64),
            "timestamp".to_string(),
            "pg-timestamp",
        )),
        TIMESTAMPTZ_OID => Ok((
            temporal_struct_data_type(DataType::Int64),
            "timestamptz".to_string(),
            "pg-timestamptz",
        )),
        INTERVAL_OID => Ok((
            interval_struct_data_type(),
            "interval".to_string(),
            "pg-interval",
        )),
        BIT_OID => Ok((bitstring_struct_data_type(), "bit".to_string(), "pg-bits")),
        VARBIT_OID => Ok((
            bitstring_struct_data_type(),
            "varbit".to_string(),
            "pg-bits",
        )),
        BOX_OID => match format {
            Format::Text => Ok((DataType::Utf8, "box".to_string(), "unknown-text")),
            Format::Binary => Err(PostgresError::Data(
                "PostgreSQL box (OID 603) requires text format (0) for lossless Arrow projection"
                    .to_string(),
            )),
        },
        _ => match catalog.type_info(base_oid) {
            Some(TypeInfo {
                name,
                kind: TypeKind::Array { element_oid, .. },
                ..
            }) => {
                let element_field = resolve_arrow_field(
                    "item",
                    *element_oid,
                    typmod,
                    format,
                    catalog,
                    depth + 1,
                    active_lineage,
                )?;
                Ok((
                    array_struct_data_type(element_field),
                    name.clone(),
                    "pg-array",
                ))
            }
            Some(TypeInfo {
                name,
                kind: TypeKind::Composite { fields },
                ..
            }) => {
                let (dt, enc) = resolve_composite_data_type(
                    base_oid,
                    fields,
                    format,
                    catalog,
                    depth,
                    active_lineage,
                )?;
                Ok((dt, name.clone(), enc))
            }
            Some(TypeInfo {
                kind: TypeKind::Extension { name, .. },
                ..
            }) if name == "vector" => Ok((
                vector_data_type("vector", typmod)?,
                "vector".to_string(),
                "pgvector",
            )),
            Some(TypeInfo {
                kind: TypeKind::Extension { name, .. },
                ..
            }) if name == "halfvec" => Ok((
                vector_data_type("halfvec", typmod)?,
                "halfvec".to_string(),
                "pghalfvec",
            )),
            Some(TypeInfo {
                name,
                kind: TypeKind::Enum,
                ..
            }) => Ok((DataType::Utf8, name.clone(), "pg-enum")),
            Some(TypeInfo {
                name,
                kind: TypeKind::Range,
                ..
            }) => match format {
                Format::Text => Ok((DataType::Utf8, name.clone(), "pg-range")),
                Format::Binary => Err(PostgresError::Data(format!(
                    "PostgreSQL range type `{name}` (OID {base_oid}) requires text format (0) for Arrow projection"
                ))),
            },
            Some(info) => match format {
                Format::Text => Ok((DataType::Utf8, info.name.clone(), "unknown-text")),
                Format::Binary => Err(PostgresError::Data(format!(
                    "PostgreSQL type `{}` (OID {base_oid}) cannot be projected to Arrow from binary format without a lossless binary decoder",
                    info.name
                ))),
            },
            None => match format {
                Format::Text => Ok((DataType::Utf8, format!("oid_{base_oid}"), "unknown-text")),
                Format::Binary => Err(PostgresError::Data(format!(
                    "unknown PostgreSQL OID {base_oid} in binary format (1) cannot be projected to Arrow without text format (0)"
                ))),
            },
        },
    }
}

fn resolve_arrow_field(
    field_name: &str,
    outer_oid: u32,
    typmod: i32,
    format: Format,
    catalog: &TypeCatalog,
    depth: usize,
    active_lineage: &mut Vec<u32>,
) -> Result<Field, PostgresError> {
    if depth > MAX_ARROW_NESTING_DEPTH {
        return Err(PostgresError::Data(format!(
            "PostgreSQL Arrow schema nesting depth {depth} exceeds maximum {MAX_ARROW_NESTING_DEPTH}"
        )));
    }

    let (base_oid, domain_name, domain_chain) =
        resolve_domain_with_lineage(outer_oid, catalog, active_lineage)?;

    if active_lineage.contains(&base_oid) {
        return Err(PostgresError::Data(format!(
            "cyclic PostgreSQL catalog type lineage detected at OID {base_oid}"
        )));
    }

    let lineage_mark = active_lineage.len();
    active_lineage.extend(domain_chain);
    active_lineage.push(base_oid);

    let resolved = resolve_base_type(base_oid, typmod, format, catalog, depth, active_lineage);
    active_lineage.truncate(lineage_mark);
    let (data_type, base_type_name, encoding) = resolved?;

    let type_name = domain_name.unwrap_or(base_type_name);
    let mut metadata = HashMap::with_capacity(if base_oid != outer_oid { 5 } else { 4 });
    metadata.insert(POSTGRES_FIELD_OID_KEY.to_string(), outer_oid.to_string());
    metadata.insert(POSTGRES_FIELD_TYPMOD_KEY.to_string(), typmod.to_string());
    metadata.insert(POSTGRES_FIELD_TYPE_NAME_KEY.to_string(), type_name);
    metadata.insert(
        POSTGRES_FIELD_ENCODING_KEY.to_string(),
        encoding.to_string(),
    );
    if base_oid != outer_oid {
        metadata.insert(
            POSTGRES_FIELD_BASE_OID_KEY.to_string(),
            base_oid.to_string(),
        );
    }

    Ok(Field::new(field_name, data_type, true).with_metadata(metadata))
}
