//! Lossless PostgreSQL-to-Arrow schemas, bounded `RecordBatch` builders, and `VTBARROW` SHM serialization.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder,
    Int8Builder, Int16Builder, Int32Builder, Int64Builder, NullBufferBuilder, StringBuilder,
    UInt32Builder,
};
use arrow_array::{ArrayRef, FixedSizeListArray, ListArray, RecordBatch, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use postgres_protocol::types as protocol_types;

use crate::codecs::array::MAX_ARRAY_DIMS;
use crate::codecs::temporal::{
    validate_date_days, validate_interval_parts, validate_time_micros, validate_timestamp_micros,
};
use crate::codecs::{
    BIT_OID, BOOL_OID, BOX_OID, BPCHAR_OID, BYTEA_OID, CodecContext, DATE_OID, FLOAT4_OID,
    FLOAT8_OID, INT2_OID, INT4_OID, INT8_OID, INTERVAL_OID, JSON_OID, JSONB_OID, NAME_OID,
    NUMERIC_OID, OID_OID, RECORD_OID, TEXT_OID, TIME_OID, TIMESTAMP_OID, TIMESTAMPTZ_OID, UUID_OID,
    VARBIT_OID, VARCHAR_OID, decode_value_with_catalog,
};
use crate::errors::PostgresError;
use crate::result::{
    Column, Format, PG_DATE_NEG_INF, PG_DATE_POS_INF, PG_TS_NEG_INF, PG_TS_POS_INF, PgValue,
    TemporalKind, VectorKind,
};
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

/// Default bounded `RecordBatch` row count ceiling (`65_536` rows).
pub const DEFAULT_ARROW_BATCH_ROWS: usize = 65_536;
/// Default bounded `RecordBatch` decoded value byte budget (`8 MiB`).
pub const DEFAULT_ARROW_BATCH_BYTES: usize = 8 * 1024 * 1024;
/// Maximum recursive catalog composite/array/domain nesting depth (`32`).
pub const MAX_ARROW_NESTING_DEPTH: usize = 32;
/// Maximum `pgvector` `vector` / `halfvec` fixed dimension (`16_000`).
pub const MAX_PGVECTOR_ARROW_DIM: i32 = 16_000;

const MAX_I32_OFFSET_BYTES: usize = i32::MAX as usize;
const INITIAL_BUILDER_CAPACITY_CAP: usize = 1024;

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

#[derive(Clone, Debug)]
struct ColumnWireMeta {
    type_oid: u32,
    base_oid: u32,
    format: Format,
}

enum PreparedTemporalValue<T> {
    Null,
    Epoch {
        kind: i8,
        value: T,
    },
    WireText {
        wire_text: String,
        date_style: String,
        time_zone: String,
    },
}

enum PreparedArrowCell {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt32(u32),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Binary(Vec<u8>),
    Uuid([u8; 16]),
    Date(PreparedTemporalValue<i32>),
    Timestamp(PreparedTemporalValue<i64>),
    Interval {
        months: i32,
        days: i32,
        microseconds: i64,
    },
    BitString {
        data: Vec<u8>,
        bit_length: i32,
    },
    Vector(Vec<f32>),
    Array {
        dimensions: Vec<i32>,
        lower_bounds: Vec<i32>,
        values: Vec<PreparedArrowCell>,
    },
    Composite(Vec<PreparedArrowCell>),
}

enum ArrowColumnBuilder {
    Boolean(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64 {
        builder: Int64Builder,
        is_pg_time: bool,
    },
    UInt32(UInt32Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8 {
        builder: StringBuilder,
        encoding: String,
        offset_bytes: usize,
    },
    Binary {
        builder: BinaryBuilder,
        offset_bytes: usize,
    },
    Uuid {
        builder: FixedSizeBinaryBuilder,
        row_cap: usize,
    },
    Date {
        fields: Fields,
        nulls: NullBufferBuilder,
        kind: Int8Builder,
        value: Int32Builder,
        wire_text: StringBuilder,
        date_style: StringBuilder,
        time_zone: StringBuilder,
        wt_bytes: usize,
        ds_bytes: usize,
        tz_bytes: usize,
    },
    Timestamp {
        fields: Fields,
        expected_kind: TemporalKind,
        nulls: NullBufferBuilder,
        kind: Int8Builder,
        value: Int64Builder,
        wire_text: StringBuilder,
        date_style: StringBuilder,
        time_zone: StringBuilder,
        wt_bytes: usize,
        ds_bytes: usize,
        tz_bytes: usize,
    },
    Interval {
        fields: Fields,
        nulls: NullBufferBuilder,
        months: Int32Builder,
        days: Int32Builder,
        microseconds: Int64Builder,
    },
    BitString {
        fields: Fields,
        nulls: NullBufferBuilder,
        bytes: BinaryBuilder,
        bytes_offset: usize,
        bit_length: Int32Builder,
    },
    VectorFixed {
        item_field: FieldRef,
        dim: i32,
        expected_kind: VectorKind,
        nulls: NullBufferBuilder,
        values: Float32Builder,
    },
    VectorDynamic {
        item_field: FieldRef,
        expected_kind: VectorKind,
        nulls: NullBufferBuilder,
        offsets: Vec<i32>,
        values: Float32Builder,
        elem_count: usize,
    },
    PgArray {
        fields: Fields,
        dim_item_field: FieldRef,
        lb_item_field: FieldRef,
        val_item_field: FieldRef,
        nulls: NullBufferBuilder,
        dim_nulls: NullBufferBuilder,
        dim_offsets: Vec<i32>,
        dim_values: Int32Builder,
        dim_count: usize,
        lb_nulls: NullBufferBuilder,
        lb_offsets: Vec<i32>,
        lb_values: Int32Builder,
        lb_count: usize,
        val_nulls: NullBufferBuilder,
        val_offsets: Vec<i32>,
        val_builder: Box<ArrowColumnBuilder>,
        val_count: usize,
    },
    Composite {
        fields: Fields,
        nulls: NullBufferBuilder,
        children: Vec<ArrowColumnBuilder>,
    },
}

fn validate_bitstring_payload(
    data: &[u8],
    bit_length: i32,
    max_bytes: usize,
) -> Result<usize, PostgresError> {
    if bit_length < 0 {
        return Err(PostgresError::Data(format!(
            "PostgreSQL bit/varbit bit_length must be >= 0, got {bit_length}"
        )));
    }
    let bit_len = bit_length as usize;
    let expected_bytes = bit_len.div_ceil(8);
    if data.len() != expected_bytes {
        return Err(PostgresError::Data(format!(
            "PostgreSQL bit/varbit byte length {} does not match bit_length {bit_length} (expected {expected_bytes})",
            data.len()
        )));
    }
    let rem = bit_len % 8;
    if rem != 0
        && let Some(&last) = data.last()
        && (last & ((1u8 << (8 - rem)) - 1)) != 0
    {
        return Err(PostgresError::Data(format!(
            "PostgreSQL bit/varbit has nonzero unused low-order padding bits in 0x{last:02x} for bit_length {bit_length}"
        )));
    }
    if data.len() > MAX_I32_OFFSET_BYTES {
        return Err(PostgresError::Data(format!(
            "bitstring bytes exceed 32-bit Arrow i32 offset ceiling ({MAX_I32_OFFSET_BYTES} bytes)"
        )));
    }
    let cell_bytes = data.len().saturating_add(4);
    if cell_bytes > max_bytes {
        return Err(PostgresError::Data(format!(
            "bit/varbit value of {cell_bytes} bytes exceeds Arrow batch max_bytes ({max_bytes})"
        )));
    }
    Ok(cell_bytes)
}

fn validate_vector_payload(
    values: &[f32],
    kind: VectorKind,
    expected_kind: VectorKind,
    fixed_dim: Option<i32>,
    max_bytes: usize,
) -> Result<(Vec<f32>, usize), PostgresError> {
    let type_label = match expected_kind {
        VectorKind::Vector => "vector",
        VectorKind::HalfVec => "halfvec",
    };
    if kind != expected_kind {
        return Err(PostgresError::Data(format!(
            "PgValue::Vector kind {kind:?} does not match Arrow `{type_label}` column ({expected_kind:?})"
        )));
    }
    let max_dim = MAX_PGVECTOR_ARROW_DIM as usize;
    if !(1..=max_dim).contains(&values.len()) {
        return Err(PostgresError::Data(format!(
            "{type_label} dimension {} outside valid range 1..={max_dim}",
            values.len()
        )));
    }
    if let Some(dim) = fixed_dim
        && values.len() != dim as usize
    {
        return Err(PostgresError::Data(format!(
            "{type_label}({dim}) column rejected vector of mismatched dimension {}",
            values.len()
        )));
    }
    let cell_bytes = values
        .len()
        .checked_mul(4)
        .ok_or_else(|| PostgresError::Data(format!("{type_label} byte length overflow")))?;
    if cell_bytes > max_bytes {
        return Err(PostgresError::Data(format!(
            "{type_label} value of {cell_bytes} bytes exceeds Arrow batch max_bytes ({max_bytes})"
        )));
    }

    let mut normalized = Vec::with_capacity(values.len());
    for (idx, &v) in values.iter().enumerate() {
        if !v.is_finite() {
            return Err(PostgresError::Data(format!(
                "{type_label} element [{idx}] must be finite (got {v})"
            )));
        }
        match expected_kind {
            VectorKind::Vector => normalized.push(v),
            VectorKind::HalfVec => {
                let h = half::f16::from_f32(v);
                if !h.is_finite() {
                    return Err(PostgresError::Data(format!(
                        "halfvec element [{idx}] value {v} overflows finite IEEE 754 binary16"
                    )));
                }
                normalized.push(h.to_f32());
            }
        }
    }
    Ok((normalized, cell_bytes))
}

impl ArrowColumnBuilder {
    fn from_field(field: &Field, row_cap: usize, depth: usize) -> Result<Self, PostgresError> {
        if depth > MAX_ARROW_NESTING_DEPTH {
            return Err(PostgresError::Data(format!(
                "PostgreSQL Arrow builder nesting depth {depth} exceeds maximum {MAX_ARROW_NESTING_DEPTH}"
            )));
        }
        let encoding = field
            .metadata()
            .get(POSTGRES_FIELD_ENCODING_KEY)
            .cloned()
            .unwrap_or_default();

        match (field.data_type(), encoding.as_str()) {
            (DataType::Boolean, _) => Ok(Self::Boolean(BooleanBuilder::with_capacity(row_cap))),
            (DataType::Int16, _) => Ok(Self::Int16(Int16Builder::with_capacity(row_cap))),
            (DataType::Int32, _) => Ok(Self::Int32(Int32Builder::with_capacity(row_cap))),
            (DataType::Int64, enc) => Ok(Self::Int64 {
                builder: Int64Builder::with_capacity(row_cap),
                is_pg_time: enc == "pg-time-micros",
            }),
            (DataType::UInt32, _) => Ok(Self::UInt32(UInt32Builder::with_capacity(row_cap))),
            (DataType::Float32, _) => Ok(Self::Float32(Float32Builder::with_capacity(row_cap))),
            (DataType::Float64, _) => Ok(Self::Float64(Float64Builder::with_capacity(row_cap))),
            (DataType::Utf8, _) => Ok(Self::Utf8 {
                builder: StringBuilder::with_capacity(row_cap, row_cap.saturating_mul(16)),
                encoding,
                offset_bytes: 0,
            }),
            (DataType::Binary, _) => Ok(Self::Binary {
                builder: BinaryBuilder::with_capacity(row_cap, row_cap.saturating_mul(16)),
                offset_bytes: 0,
            }),
            (DataType::FixedSizeBinary(16), _) => Ok(Self::Uuid {
                builder: FixedSizeBinaryBuilder::with_capacity(row_cap, 16),
                row_cap,
            }),
            (DataType::Struct(fields), "pg-date") => Ok(Self::Date {
                fields: fields.clone(),
                nulls: NullBufferBuilder::new(row_cap),
                kind: Int8Builder::with_capacity(row_cap),
                value: Int32Builder::with_capacity(row_cap),
                wire_text: StringBuilder::with_capacity(row_cap, 0),
                date_style: StringBuilder::with_capacity(row_cap, 0),
                time_zone: StringBuilder::with_capacity(row_cap, 0),
                wt_bytes: 0,
                ds_bytes: 0,
                tz_bytes: 0,
            }),
            (DataType::Struct(fields), "pg-timestamp" | "pg-timestamptz") => Ok(Self::Timestamp {
                fields: fields.clone(),
                expected_kind: if encoding == "pg-timestamptz" {
                    TemporalKind::TimestampTz
                } else {
                    TemporalKind::Timestamp
                },
                nulls: NullBufferBuilder::new(row_cap),
                kind: Int8Builder::with_capacity(row_cap),
                value: Int64Builder::with_capacity(row_cap),
                wire_text: StringBuilder::with_capacity(row_cap, 0),
                date_style: StringBuilder::with_capacity(row_cap, 0),
                time_zone: StringBuilder::with_capacity(row_cap, 0),
                wt_bytes: 0,
                ds_bytes: 0,
                tz_bytes: 0,
            }),
            (DataType::Struct(fields), "pg-interval") => Ok(Self::Interval {
                fields: fields.clone(),
                nulls: NullBufferBuilder::new(row_cap),
                months: Int32Builder::with_capacity(row_cap),
                days: Int32Builder::with_capacity(row_cap),
                microseconds: Int64Builder::with_capacity(row_cap),
            }),
            (DataType::Struct(fields), "pg-bits") => Ok(Self::BitString {
                fields: fields.clone(),
                nulls: NullBufferBuilder::new(row_cap),
                bytes: BinaryBuilder::with_capacity(row_cap, row_cap.saturating_mul(8)),
                bytes_offset: 0,
                bit_length: Int32Builder::with_capacity(row_cap),
            }),
            (DataType::FixedSizeList(item_field, dim), enc @ ("pgvector" | "pghalfvec")) => {
                if !(1..=MAX_PGVECTOR_ARROW_DIM).contains(dim) {
                    return Err(PostgresError::Data(format!(
                        "invalid fixed {enc} Arrow dimension {dim} on field `{}`",
                        field.name()
                    )));
                }
                let expected_kind = if enc == "pghalfvec" {
                    VectorKind::HalfVec
                } else {
                    VectorKind::Vector
                };
                let elem_cap = row_cap.saturating_mul((*dim as usize).min(64));
                Ok(Self::VectorFixed {
                    item_field: Arc::clone(item_field),
                    dim: *dim,
                    expected_kind,
                    nulls: NullBufferBuilder::new(row_cap),
                    values: Float32Builder::with_capacity(elem_cap),
                })
            }
            (DataType::List(item_field), enc @ ("pgvector" | "pghalfvec")) => {
                let expected_kind = if enc == "pghalfvec" {
                    VectorKind::HalfVec
                } else {
                    VectorKind::Vector
                };
                let mut offsets = Vec::with_capacity(row_cap.saturating_add(1));
                offsets.push(0);
                Ok(Self::VectorDynamic {
                    item_field: Arc::clone(item_field),
                    expected_kind,
                    nulls: NullBufferBuilder::new(row_cap),
                    offsets,
                    values: Float32Builder::with_capacity(row_cap.saturating_mul(8)),
                    elem_count: 0,
                })
            }
            (DataType::Struct(fields), "pg-array") => {
                if fields.len() != 3 {
                    return Err(PostgresError::Data(format!(
                        "invalid pg-array Struct field count {} on `{}` (expected 3)",
                        fields.len(),
                        field.name()
                    )));
                }
                let DataType::List(dim_item_field) = fields[0].data_type() else {
                    return Err(PostgresError::Data(format!(
                        "pg-array `{}` dimensions field must be List<Int32>",
                        field.name()
                    )));
                };
                let DataType::List(lb_item_field) = fields[1].data_type() else {
                    return Err(PostgresError::Data(format!(
                        "pg-array `{}` lower_bounds field must be List<Int32>",
                        field.name()
                    )));
                };
                let DataType::List(val_item_field) = fields[2].data_type() else {
                    return Err(PostgresError::Data(format!(
                        "pg-array `{}` values field must be List",
                        field.name()
                    )));
                };
                let val_builder = Box::new(Self::from_field(
                    val_item_field.as_ref(),
                    row_cap,
                    depth + 1,
                )?);
                let mut dim_offsets = Vec::with_capacity(row_cap.saturating_add(1));
                dim_offsets.push(0);
                let mut lb_offsets = Vec::with_capacity(row_cap.saturating_add(1));
                lb_offsets.push(0);
                let mut val_offsets = Vec::with_capacity(row_cap.saturating_add(1));
                val_offsets.push(0);
                Ok(Self::PgArray {
                    fields: fields.clone(),
                    dim_item_field: Arc::clone(dim_item_field),
                    lb_item_field: Arc::clone(lb_item_field),
                    val_item_field: Arc::clone(val_item_field),
                    nulls: NullBufferBuilder::new(row_cap),
                    dim_nulls: NullBufferBuilder::new(row_cap),
                    dim_offsets,
                    dim_values: Int32Builder::with_capacity(row_cap),
                    dim_count: 0,
                    lb_nulls: NullBufferBuilder::new(row_cap),
                    lb_offsets,
                    lb_values: Int32Builder::with_capacity(row_cap),
                    lb_count: 0,
                    val_nulls: NullBufferBuilder::new(row_cap),
                    val_offsets,
                    val_builder,
                    val_count: 0,
                })
            }
            (DataType::Struct(fields), "pg-composite") => {
                let mut children = Vec::with_capacity(fields.len());
                for child_field in fields {
                    children.push(Self::from_field(child_field.as_ref(), row_cap, depth + 1)?);
                }
                Ok(Self::Composite {
                    fields: fields.clone(),
                    nulls: NullBufferBuilder::new(row_cap),
                    children,
                })
            }
            (dt, enc) => Err(PostgresError::Data(format!(
                "unsupported PostgreSQL Arrow column `{}` ({dt:?}, encoding `{enc}`)",
                field.name()
            ))),
        }
    }

    fn prepare_cell(
        &self,
        val: &PgValue,
        max_bytes: usize,
    ) -> Result<(PreparedArrowCell, usize), PostgresError> {
        if matches!(val, PgValue::Null) {
            return Ok((PreparedArrowCell::Null, 0));
        }

        let check_single_cap = |bytes: usize, label: &str| -> Result<usize, PostgresError> {
            if bytes > max_bytes {
                Err(PostgresError::Data(format!(
                    "{label} value of {bytes} bytes exceeds Arrow batch max_bytes ({max_bytes})"
                )))
            } else {
                Ok(bytes)
            }
        };

        let check_i32_offset = |add: usize, label: &str| -> Result<(), PostgresError> {
            if add <= MAX_I32_OFFSET_BYTES {
                Ok(())
            } else {
                Err(PostgresError::Data(format!(
                    "{label} buffer exceeds 32-bit Arrow i32 offset ceiling ({MAX_I32_OFFSET_BYTES} bytes)"
                )))
            }
        };

        match self {
            Self::Boolean(_) => match val {
                PgValue::Bool(v) => Ok((PreparedArrowCell::Bool(*v), check_single_cap(1, "bool")?)),
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Bool for Boolean Arrow column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::Int16(_) => match val {
                PgValue::Int2(v) => {
                    Ok((PreparedArrowCell::Int16(*v), check_single_cap(2, "int2")?))
                }
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Int2 for Int16 Arrow column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::Int32(_) => match val {
                PgValue::Int4(v) => {
                    Ok((PreparedArrowCell::Int32(*v), check_single_cap(4, "int4")?))
                }
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Int4 for Int32 Arrow column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::Int64 { is_pg_time, .. } => {
                if *is_pg_time {
                    match val {
                        PgValue::Time(us) => {
                            let valid = validate_time_micros(*us)?;
                            Ok((
                                PreparedArrowCell::Int64(valid),
                                check_single_cap(8, "time")?,
                            ))
                        }
                        other => Err(PostgresError::Data(format!(
                            "expected PgValue::Time for pg-time-micros column, got {}",
                            pg_value_tag(other)
                        ))),
                    }
                } else {
                    match val {
                        PgValue::Int8(v) => {
                            Ok((PreparedArrowCell::Int64(*v), check_single_cap(8, "int8")?))
                        }
                        other => Err(PostgresError::Data(format!(
                            "expected PgValue::Int8 for Int64 Arrow column, got {}",
                            pg_value_tag(other)
                        ))),
                    }
                }
            }
            Self::UInt32(_) => {
                let parsed = match val {
                    PgValue::Int8(v) => u32::try_from(*v).map_err(|_| {
                        PostgresError::Data(format!(
                            "PostgreSQL unsigned 32-bit OID/xid/cid value {v} outside 0..={}",
                            u32::MAX
                        ))
                    })?,
                    PgValue::Text(s) => s.trim().parse::<u32>().map_err(|_| {
                        PostgresError::Data(format!(
                            "invalid PostgreSQL unsigned 32-bit OID/xid/cid text {s:?}"
                        ))
                    })?,
                    other => {
                        return Err(PostgresError::Data(format!(
                            "expected PgValue::Int8 or Text for UInt32 Arrow column, got {}",
                            pg_value_tag(other)
                        )));
                    }
                };
                Ok((
                    PreparedArrowCell::UInt32(parsed),
                    check_single_cap(4, "uint32")?,
                ))
            }
            Self::Float32(_) => match val {
                PgValue::Float4(v) => Ok((
                    PreparedArrowCell::Float32(*v),
                    check_single_cap(4, "float4")?,
                )),
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Float4 for Float32 Arrow column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::Float64(_) => match val {
                PgValue::Float8(v) => Ok((
                    PreparedArrowCell::Float64(*v),
                    check_single_cap(8, "float8")?,
                )),
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Float8 for Float64 Arrow column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::Utf8 { encoding, .. } => {
                let text = match (encoding.as_str(), val) {
                    ("numeric-decimal", PgValue::Numeric(num)) => {
                        let rendered = num.to_canonical_string();
                        check_single_cap(rendered.len(), "numeric")?;
                        rendered
                    }
                    ("json" | "jsonb", PgValue::Json(raw)) => {
                        check_single_cap(raw.len(), encoding)?;
                        let s = std::str::from_utf8(raw).map_err(|e| {
                            PostgresError::Data(format!("invalid UTF-8 in PgValue::Json: {e}"))
                        })?;
                        s.to_owned()
                    }
                    ("numeric-decimal" | "json" | "jsonb", other) => {
                        return Err(PostgresError::Data(format!(
                            "incompatible PgValue {} for `{encoding}` Utf8 Arrow column",
                            pg_value_tag(other)
                        )));
                    }
                    (_, PgValue::Text(s)) => {
                        check_single_cap(s.len(), "text")?;
                        s.clone()
                    }
                    (_, other) => {
                        return Err(PostgresError::Data(format!(
                            "expected PgValue::Text for Utf8 Arrow column, got {}",
                            pg_value_tag(other)
                        )));
                    }
                };
                check_i32_offset(text.len(), "Utf8")?;
                let len = text.len();
                Ok((PreparedArrowCell::Utf8(text), len))
            }
            Self::Binary { .. } => match val {
                PgValue::Bytes(raw) => {
                    check_single_cap(raw.len(), "bytea")?;
                    check_i32_offset(raw.len(), "Binary")?;
                    Ok((PreparedArrowCell::Binary(raw.clone()), raw.len()))
                }
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Bytes for Binary Arrow column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::Uuid { .. } => match val {
                PgValue::Uuid(bytes) => Ok((
                    PreparedArrowCell::Uuid(*bytes),
                    check_single_cap(16, "uuid")?,
                )),
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Uuid for FixedSizeBinary(16) Arrow column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::Date { .. } => match val {
                PgValue::Date(days) => {
                    let d = validate_date_days(*days)?;
                    let (kind, raw_val) = if d == PG_DATE_NEG_INF {
                        (-1i8, 0i32)
                    } else if d == PG_DATE_POS_INF {
                        (1i8, 0i32)
                    } else {
                        (0i8, d)
                    };
                    Ok((
                        PreparedArrowCell::Date(PreparedTemporalValue::Epoch {
                            kind,
                            value: raw_val,
                        }),
                        check_single_cap(5, "date")?,
                    ))
                }
                PgValue::TemporalText {
                    kind: TemporalKind::Date,
                    wire_text,
                    date_style,
                    time_zone,
                } => {
                    let bytes = 5usize
                        .saturating_add(wire_text.len())
                        .saturating_add(date_style.len())
                        .saturating_add(time_zone.len());
                    check_single_cap(bytes, "date wire_text")?;
                    check_i32_offset(wire_text.len(), "date wire_text")?;
                    check_i32_offset(date_style.len(), "date date_style")?;
                    check_i32_offset(time_zone.len(), "date time_zone")?;
                    Ok((
                        PreparedArrowCell::Date(PreparedTemporalValue::WireText {
                            wire_text: wire_text.clone(),
                            date_style: date_style.clone(),
                            time_zone: time_zone.clone(),
                        }),
                        bytes,
                    ))
                }
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Date or Date TemporalText for pg-date column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::Timestamp { expected_kind, .. } => match (*expected_kind, val) {
                (TemporalKind::Timestamp, PgValue::Timestamp(us))
                | (TemporalKind::TimestampTz, PgValue::TimestampTz(us)) => {
                    let v = validate_timestamp_micros(*us)?;
                    let (kind, raw_val) = if v == PG_TS_NEG_INF {
                        (-1i8, 0i64)
                    } else if v == PG_TS_POS_INF {
                        (1i8, 0i64)
                    } else {
                        (0i8, v)
                    };
                    Ok((
                        PreparedArrowCell::Timestamp(PreparedTemporalValue::Epoch {
                            kind,
                            value: raw_val,
                        }),
                        check_single_cap(9, "timestamp")?,
                    ))
                }
                (
                    exp,
                    PgValue::TemporalText {
                        kind,
                        wire_text,
                        date_style,
                        time_zone,
                    },
                ) if exp == *kind => {
                    let bytes = 9usize
                        .saturating_add(wire_text.len())
                        .saturating_add(date_style.len())
                        .saturating_add(time_zone.len());
                    check_single_cap(bytes, "timestamp wire_text")?;
                    check_i32_offset(wire_text.len(), "timestamp wire_text")?;
                    check_i32_offset(date_style.len(), "timestamp date_style")?;
                    check_i32_offset(time_zone.len(), "timestamp time_zone")?;
                    Ok((
                        PreparedArrowCell::Timestamp(PreparedTemporalValue::WireText {
                            wire_text: wire_text.clone(),
                            date_style: date_style.clone(),
                            time_zone: time_zone.clone(),
                        }),
                        bytes,
                    ))
                }
                (_, other) => Err(PostgresError::Data(format!(
                    "expected matching Timestamp/TemporalText ({expected_kind:?}), got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::Interval { .. } => match val {
                PgValue::Interval {
                    months,
                    days,
                    microseconds,
                } => {
                    let (m, d, us) = validate_interval_parts(*months, *days, *microseconds)?;
                    Ok((
                        PreparedArrowCell::Interval {
                            months: m,
                            days: d,
                            microseconds: us,
                        },
                        check_single_cap(16, "interval")?,
                    ))
                }
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Interval for pg-interval column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::BitString { .. } => match val {
                PgValue::BitString { data, bit_length } => {
                    let bytes = validate_bitstring_payload(data, *bit_length, max_bytes)?;
                    Ok((
                        PreparedArrowCell::BitString {
                            data: data.clone(),
                            bit_length: *bit_length,
                        },
                        bytes,
                    ))
                }
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::BitString for pg-bits column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::VectorFixed {
                dim, expected_kind, ..
            } => match val {
                PgValue::Vector { values, kind } => {
                    let (normalized, bytes) = validate_vector_payload(
                        values,
                        *kind,
                        *expected_kind,
                        Some(*dim),
                        max_bytes,
                    )?;
                    Ok((PreparedArrowCell::Vector(normalized), bytes))
                }
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Vector for fixed pgvector/halfvec column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::VectorDynamic { expected_kind, .. } => match val {
                PgValue::Vector { values, kind } => {
                    let (normalized, bytes) =
                        validate_vector_payload(values, *kind, *expected_kind, None, max_bytes)?;
                    Ok((PreparedArrowCell::Vector(normalized), bytes))
                }
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Vector for dynamic pgvector/halfvec column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::PgArray { val_builder, .. } => match val {
                PgValue::Array {
                    values,
                    dimensions,
                    lower_bounds,
                    element_oid,
                } => {
                    if *element_oid == 0 {
                        return Err(PostgresError::Data(
                            "PostgreSQL array element_oid must be >= 1".to_string(),
                        ));
                    }
                    if dimensions.len() != lower_bounds.len() {
                        return Err(PostgresError::Data(format!(
                            "PostgreSQL array dimensions length {} does not match lower_bounds length {}",
                            dimensions.len(),
                            lower_bounds.len()
                        )));
                    }
                    if dimensions.len() > MAX_ARRAY_DIMS {
                        return Err(PostgresError::Data(format!(
                            "PostgreSQL array rank {} exceeds maximum {MAX_ARRAY_DIMS}",
                            dimensions.len()
                        )));
                    }
                    if dimensions.is_empty() {
                        if !values.is_empty() {
                            return Err(PostgresError::Data(
                                "0D PostgreSQL array must contain zero elements".to_string(),
                            ));
                        }
                        return Ok((
                            PreparedArrowCell::Array {
                                dimensions: Vec::new(),
                                lower_bounds: Vec::new(),
                                values: Vec::new(),
                            },
                            0,
                        ));
                    }

                    let mut expected_len = 1usize;
                    for (&dim, &lb) in dimensions.iter().zip(lower_bounds.iter()) {
                        if dim < 1 {
                            return Err(PostgresError::Data(format!(
                                "PostgreSQL array dimension must be >= 1, got {dim}"
                            )));
                        }
                        if lb.checked_add(dim - 1).is_none() {
                            return Err(PostgresError::Data(format!(
                                "PostgreSQL array upper bound overflows i32 (lower_bound={lb}, dim={dim})"
                            )));
                        }
                        expected_len = expected_len.checked_mul(dim as usize).ok_or_else(|| {
                            PostgresError::Data(
                                "PostgreSQL array dimensions product overflow".to_string(),
                            )
                        })?;
                    }
                    if expected_len != values.len() {
                        return Err(PostgresError::Data(format!(
                            "PostgreSQL array values length {} does not match dimensions product {expected_len}",
                            values.len()
                        )));
                    }
                    check_i32_offset(values.len(), "pg-array element list")?;

                    let mut total_bytes =
                        dimensions.len().saturating_mul(8).min(MAX_I32_OFFSET_BYTES);
                    check_single_cap(total_bytes, "pg-array header")?;

                    let mut prepared_vals = Vec::with_capacity(values.len());
                    for elem in values {
                        let (p_elem, elem_bytes) = val_builder.prepare_cell(elem, max_bytes)?;
                        total_bytes = total_bytes.checked_add(elem_bytes).ok_or_else(|| {
                            PostgresError::Data(
                                "pg-array aggregate byte count overflow".to_string(),
                            )
                        })?;
                        check_single_cap(total_bytes, "pg-array")?;
                        prepared_vals.push(p_elem);
                    }
                    Ok((
                        PreparedArrowCell::Array {
                            dimensions: dimensions.clone(),
                            lower_bounds: lower_bounds.clone(),
                            values: prepared_vals,
                        },
                        total_bytes,
                    ))
                }
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Array for pg-array column, got {}",
                    pg_value_tag(other)
                ))),
            },
            Self::Composite {
                fields, children, ..
            } => match val {
                PgValue::Composite {
                    field_names,
                    values,
                    ..
                } => {
                    if values.len() != children.len() {
                        return Err(PostgresError::Data(format!(
                            "PgValue::Composite attribute count {} does not match catalog composite field count {}",
                            values.len(),
                            children.len()
                        )));
                    }
                    if !field_names.is_empty() && field_names.len() != fields.len() {
                        return Err(PostgresError::Data(format!(
                            "PgValue::Composite field_names length {} does not match catalog composite field count {}",
                            field_names.len(),
                            fields.len()
                        )));
                    }
                    let mut total_bytes = 0usize;
                    let mut prepared_children = Vec::with_capacity(children.len());
                    for (child_builder, child_val) in children.iter().zip(values.iter()) {
                        let (p_child, child_bytes) =
                            child_builder.prepare_cell(child_val, max_bytes)?;
                        total_bytes = total_bytes.checked_add(child_bytes).ok_or_else(|| {
                            PostgresError::Data(
                                "pg-composite aggregate byte count overflow".to_string(),
                            )
                        })?;
                        check_single_cap(total_bytes, "pg-composite")?;
                        prepared_children.push(p_child);
                    }
                    Ok((PreparedArrowCell::Composite(prepared_children), total_bytes))
                }
                other => Err(PostgresError::Data(format!(
                    "expected PgValue::Composite for pg-composite column, got {}",
                    pg_value_tag(other)
                ))),
            },
        }
    }

    fn validate_cells_offsets(
        &self,
        cells: &[&PreparedArrowCell],
        reset_current: bool,
    ) -> Result<(), PostgresError> {
        let check_sum = |base: usize, add: usize, label: &str| -> Result<usize, PostgresError> {
            match base.checked_add(add) {
                Some(sum) if sum <= MAX_I32_OFFSET_BYTES => Ok(sum),
                _ => Err(PostgresError::Data(format!(
                    "{label} buffer exceeds 32-bit Arrow i32 offset ceiling ({MAX_I32_OFFSET_BYTES} bytes)"
                ))),
            }
        };

        match self {
            Self::Utf8 { offset_bytes, .. } => {
                let mut cur = if reset_current { 0 } else { *offset_bytes };
                for c in cells {
                    if let PreparedArrowCell::Utf8(s) = c {
                        cur = check_sum(cur, s.len(), "Utf8")?;
                    }
                }
                Ok(())
            }
            Self::Binary { offset_bytes, .. } => {
                let mut cur = if reset_current { 0 } else { *offset_bytes };
                for c in cells {
                    if let PreparedArrowCell::Binary(b) = c {
                        cur = check_sum(cur, b.len(), "Binary")?;
                    }
                }
                Ok(())
            }
            Self::Date {
                wt_bytes,
                ds_bytes,
                tz_bytes,
                ..
            } => {
                let mut wt = if reset_current { 0 } else { *wt_bytes };
                let mut ds = if reset_current { 0 } else { *ds_bytes };
                let mut tz = if reset_current { 0 } else { *tz_bytes };
                for c in cells {
                    if let PreparedArrowCell::Date(PreparedTemporalValue::WireText {
                        wire_text,
                        date_style,
                        time_zone,
                    }) = c
                    {
                        wt = check_sum(wt, wire_text.len(), "date wire_text")?;
                        ds = check_sum(ds, date_style.len(), "date date_style")?;
                        tz = check_sum(tz, time_zone.len(), "date time_zone")?;
                    }
                }
                Ok(())
            }
            Self::Timestamp {
                wt_bytes,
                ds_bytes,
                tz_bytes,
                ..
            } => {
                let mut wt = if reset_current { 0 } else { *wt_bytes };
                let mut ds = if reset_current { 0 } else { *ds_bytes };
                let mut tz = if reset_current { 0 } else { *tz_bytes };
                for c in cells {
                    if let PreparedArrowCell::Timestamp(PreparedTemporalValue::WireText {
                        wire_text,
                        date_style,
                        time_zone,
                    }) = c
                    {
                        wt = check_sum(wt, wire_text.len(), "timestamp wire_text")?;
                        ds = check_sum(ds, date_style.len(), "timestamp date_style")?;
                        tz = check_sum(tz, time_zone.len(), "timestamp time_zone")?;
                    }
                }
                Ok(())
            }
            Self::BitString { bytes_offset, .. } => {
                let mut cur = if reset_current { 0 } else { *bytes_offset };
                for c in cells {
                    if let PreparedArrowCell::BitString { data, .. } = c {
                        cur = check_sum(cur, data.len(), "bitstring bytes")?;
                    }
                }
                Ok(())
            }
            Self::VectorDynamic { elem_count, .. } => {
                let mut cur = if reset_current { 0 } else { *elem_count };
                for c in cells {
                    if let PreparedArrowCell::Vector(v) = c {
                        cur = check_sum(cur, v.len(), "dynamic vector list")?;
                    }
                }
                Ok(())
            }
            Self::PgArray {
                dim_count,
                lb_count,
                val_count,
                val_builder,
                ..
            } => {
                let mut dc = if reset_current { 0 } else { *dim_count };
                let mut lc = if reset_current { 0 } else { *lb_count };
                let mut vc = if reset_current { 0 } else { *val_count };
                let mut flat_refs: Vec<&PreparedArrowCell> = Vec::new();
                for c in cells {
                    if let PreparedArrowCell::Array {
                        dimensions,
                        lower_bounds,
                        values,
                    } = c
                    {
                        dc = check_sum(dc, dimensions.len(), "pg-array dimensions")?;
                        lc = check_sum(lc, lower_bounds.len(), "pg-array lower_bounds")?;
                        vc = check_sum(vc, values.len(), "pg-array values")?;
                        flat_refs.extend(values.iter());
                    }
                }
                if !flat_refs.is_empty() {
                    val_builder.validate_cells_offsets(&flat_refs, reset_current)?;
                }
                Ok(())
            }
            Self::Composite { children, .. } => {
                for (idx, cb) in children.iter().enumerate() {
                    let mut col_refs: Vec<&PreparedArrowCell> = Vec::new();
                    for c in cells {
                        if let PreparedArrowCell::Composite(attrs) = c {
                            col_refs.push(&attrs[idx]);
                        }
                    }
                    if !col_refs.is_empty() {
                        cb.validate_cells_offsets(&col_refs, reset_current)?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn append_prepared(&mut self, cell: PreparedArrowCell) {
        match (self, cell) {
            (Self::Boolean(b), PreparedArrowCell::Null) => b.append_null(),
            (Self::Boolean(b), PreparedArrowCell::Bool(v)) => b.append_value(v),

            (Self::Int16(b), PreparedArrowCell::Null) => b.append_null(),
            (Self::Int16(b), PreparedArrowCell::Int16(v)) => b.append_value(v),

            (Self::Int32(b), PreparedArrowCell::Null) => b.append_null(),
            (Self::Int32(b), PreparedArrowCell::Int32(v)) => b.append_value(v),

            (Self::Int64 { builder, .. }, PreparedArrowCell::Null) => builder.append_null(),
            (Self::Int64 { builder, .. }, PreparedArrowCell::Int64(v)) => builder.append_value(v),

            (Self::UInt32(b), PreparedArrowCell::Null) => b.append_null(),
            (Self::UInt32(b), PreparedArrowCell::UInt32(v)) => b.append_value(v),

            (Self::Float32(b), PreparedArrowCell::Null) => b.append_null(),
            (Self::Float32(b), PreparedArrowCell::Float32(v)) => b.append_value(v),

            (Self::Float64(b), PreparedArrowCell::Null) => b.append_null(),
            (Self::Float64(b), PreparedArrowCell::Float64(v)) => b.append_value(v),

            (Self::Utf8 { builder, .. }, PreparedArrowCell::Null) => builder.append_null(),
            (
                Self::Utf8 {
                    builder,
                    offset_bytes,
                    ..
                },
                PreparedArrowCell::Utf8(s),
            ) => {
                *offset_bytes += s.len();
                builder.append_value(&s);
            }

            (Self::Binary { builder, .. }, PreparedArrowCell::Null) => builder.append_null(),
            (
                Self::Binary {
                    builder,
                    offset_bytes,
                },
                PreparedArrowCell::Binary(raw),
            ) => {
                *offset_bytes += raw.len();
                builder.append_value(&raw);
            }

            (Self::Uuid { builder, .. }, PreparedArrowCell::Null) => builder.append_null(),
            (Self::Uuid { builder, .. }, PreparedArrowCell::Uuid(bytes)) => {
                builder
                    .append_value(bytes)
                    .expect("validated 16-byte UUID matches FixedSizeBinaryBuilder(16)");
            }

            (
                Self::Date {
                    nulls,
                    kind,
                    value,
                    wire_text,
                    date_style,
                    time_zone,
                    wt_bytes,
                    ds_bytes,
                    tz_bytes,
                    ..
                },
                cell,
            ) => {
                let tv = match cell {
                    PreparedArrowCell::Null => PreparedTemporalValue::Null,
                    PreparedArrowCell::Date(t) => t,
                    _ => unreachable!("prepared cell matches Date builder"),
                };
                match tv {
                    PreparedTemporalValue::Null => {
                        nulls.append_null();
                        kind.append_null();
                        value.append_null();
                        wire_text.append_null();
                        date_style.append_null();
                        time_zone.append_null();
                    }
                    PreparedTemporalValue::Epoch {
                        kind: k,
                        value: raw,
                    } => {
                        nulls.append_non_null();
                        kind.append_value(k);
                        value.append_value(raw);
                        wire_text.append_null();
                        date_style.append_null();
                        time_zone.append_null();
                    }
                    PreparedTemporalValue::WireText {
                        wire_text: wt,
                        date_style: ds,
                        time_zone: tz,
                    } => {
                        nulls.append_non_null();
                        kind.append_value(2);
                        value.append_null();
                        *wt_bytes += wt.len();
                        *ds_bytes += ds.len();
                        *tz_bytes += tz.len();
                        wire_text.append_value(&wt);
                        date_style.append_value(&ds);
                        time_zone.append_value(&tz);
                    }
                }
            }

            (
                Self::Timestamp {
                    nulls,
                    kind,
                    value,
                    wire_text,
                    date_style,
                    time_zone,
                    wt_bytes,
                    ds_bytes,
                    tz_bytes,
                    ..
                },
                cell,
            ) => {
                let tv = match cell {
                    PreparedArrowCell::Null => PreparedTemporalValue::Null,
                    PreparedArrowCell::Timestamp(t) => t,
                    _ => unreachable!("prepared cell matches Timestamp builder"),
                };
                match tv {
                    PreparedTemporalValue::Null => {
                        nulls.append_null();
                        kind.append_null();
                        value.append_null();
                        wire_text.append_null();
                        date_style.append_null();
                        time_zone.append_null();
                    }
                    PreparedTemporalValue::Epoch {
                        kind: k,
                        value: raw,
                    } => {
                        nulls.append_non_null();
                        kind.append_value(k);
                        value.append_value(raw);
                        wire_text.append_null();
                        date_style.append_null();
                        time_zone.append_null();
                    }
                    PreparedTemporalValue::WireText {
                        wire_text: wt,
                        date_style: ds,
                        time_zone: tz,
                    } => {
                        nulls.append_non_null();
                        kind.append_value(2);
                        value.append_null();
                        *wt_bytes += wt.len();
                        *ds_bytes += ds.len();
                        *tz_bytes += tz.len();
                        wire_text.append_value(&wt);
                        date_style.append_value(&ds);
                        time_zone.append_value(&tz);
                    }
                }
            }

            (
                Self::Interval {
                    nulls,
                    months,
                    days,
                    microseconds,
                    ..
                },
                PreparedArrowCell::Null,
            ) => {
                nulls.append_null();
                months.append_null();
                days.append_null();
                microseconds.append_null();
            }
            (
                Self::Interval {
                    nulls,
                    months,
                    days,
                    microseconds,
                    ..
                },
                PreparedArrowCell::Interval {
                    months: m,
                    days: d,
                    microseconds: us,
                },
            ) => {
                nulls.append_non_null();
                months.append_value(m);
                days.append_value(d);
                microseconds.append_value(us);
            }

            (
                Self::BitString {
                    nulls,
                    bytes,
                    bit_length,
                    ..
                },
                PreparedArrowCell::Null,
            ) => {
                nulls.append_null();
                bytes.append_null();
                bit_length.append_null();
            }
            (
                Self::BitString {
                    nulls,
                    bytes,
                    bytes_offset,
                    bit_length,
                    ..
                },
                PreparedArrowCell::BitString {
                    data,
                    bit_length: bl,
                },
            ) => {
                nulls.append_non_null();
                *bytes_offset += data.len();
                bytes.append_value(&data);
                bit_length.append_value(bl);
            }

            (
                Self::VectorFixed {
                    dim, nulls, values, ..
                },
                PreparedArrowCell::Null,
            ) => {
                nulls.append_null();
                for _ in 0..*dim {
                    values.append_null();
                }
            }
            (Self::VectorFixed { nulls, values, .. }, PreparedArrowCell::Vector(vec)) => {
                nulls.append_non_null();
                values.append_slice(&vec);
            }

            (
                Self::VectorDynamic {
                    nulls,
                    offsets,
                    elem_count,
                    ..
                },
                PreparedArrowCell::Null,
            ) => {
                nulls.append_null();
                offsets.push(*elem_count as i32);
            }
            (
                Self::VectorDynamic {
                    nulls,
                    offsets,
                    values,
                    elem_count,
                    ..
                },
                PreparedArrowCell::Vector(vec),
            ) => {
                nulls.append_non_null();
                *elem_count += vec.len();
                offsets.push(*elem_count as i32);
                values.append_slice(&vec);
            }

            (
                Self::PgArray {
                    nulls,
                    dim_nulls,
                    dim_offsets,
                    dim_count,
                    lb_nulls,
                    lb_offsets,
                    lb_count,
                    val_nulls,
                    val_offsets,
                    val_count,
                    ..
                },
                PreparedArrowCell::Null,
            ) => {
                nulls.append_null();
                dim_nulls.append_null();
                dim_offsets.push(*dim_count as i32);
                lb_nulls.append_null();
                lb_offsets.push(*lb_count as i32);
                val_nulls.append_null();
                val_offsets.push(*val_count as i32);
            }
            (
                Self::PgArray {
                    nulls,
                    dim_nulls,
                    dim_offsets,
                    dim_values,
                    dim_count,
                    lb_nulls,
                    lb_offsets,
                    lb_values,
                    lb_count,
                    val_nulls,
                    val_offsets,
                    val_builder,
                    val_count,
                    ..
                },
                PreparedArrowCell::Array {
                    dimensions,
                    lower_bounds,
                    values,
                },
            ) => {
                nulls.append_non_null();
                dim_nulls.append_non_null();
                *dim_count += dimensions.len();
                dim_offsets.push(*dim_count as i32);
                dim_values.append_slice(&dimensions);

                lb_nulls.append_non_null();
                *lb_count += lower_bounds.len();
                lb_offsets.push(*lb_count as i32);
                lb_values.append_slice(&lower_bounds);

                val_nulls.append_non_null();
                *val_count += values.len();
                val_offsets.push(*val_count as i32);
                for v in values {
                    val_builder.append_prepared(v);
                }
            }

            (
                Self::Composite {
                    nulls, children, ..
                },
                PreparedArrowCell::Null,
            ) => {
                nulls.append_null();
                for child in children {
                    child.append_prepared(PreparedArrowCell::Null);
                }
            }
            (
                Self::Composite {
                    nulls, children, ..
                },
                PreparedArrowCell::Composite(attrs),
            ) => {
                nulls.append_non_null();
                for (child, attr) in children.iter_mut().zip(attrs) {
                    child.append_prepared(attr);
                }
            }

            _ => unreachable!("prepared cell variant matches ArrowColumnBuilder variant"),
        }
    }

    fn finish_array(&mut self, row_count: usize) -> Result<ArrayRef, PostgresError> {
        match self {
            Self::Boolean(b) => Ok(Arc::new(b.finish())),
            Self::Int16(b) => Ok(Arc::new(b.finish())),
            Self::Int32(b) => Ok(Arc::new(b.finish())),
            Self::Int64 { builder, .. } => Ok(Arc::new(builder.finish())),
            Self::UInt32(b) => Ok(Arc::new(b.finish())),
            Self::Float32(b) => Ok(Arc::new(b.finish())),
            Self::Float64(b) => Ok(Arc::new(b.finish())),
            Self::Utf8 {
                builder,
                offset_bytes,
                ..
            } => {
                *offset_bytes = 0;
                Ok(Arc::new(builder.finish()))
            }
            Self::Binary {
                builder,
                offset_bytes,
            } => {
                *offset_bytes = 0;
                Ok(Arc::new(builder.finish()))
            }
            Self::Uuid { builder, row_cap } => {
                let fresh = FixedSizeBinaryBuilder::with_capacity(*row_cap, 16);
                let finished = std::mem::replace(builder, fresh).finish();
                Ok(Arc::new(finished))
            }
            Self::Date {
                fields,
                nulls,
                kind,
                value,
                wire_text,
                date_style,
                time_zone,
                wt_bytes,
                ds_bytes,
                tz_bytes,
            } => {
                *wt_bytes = 0;
                *ds_bytes = 0;
                *tz_bytes = 0;
                let children: Vec<ArrayRef> = vec![
                    Arc::new(kind.finish()),
                    Arc::new(value.finish()),
                    Arc::new(wire_text.finish()),
                    Arc::new(date_style.finish()),
                    Arc::new(time_zone.finish()),
                ];
                let arr = StructArray::try_new_with_length(
                    fields.clone(),
                    children,
                    nulls.finish(),
                    row_count,
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build date StructArray: {e}"))
                })?;
                Ok(Arc::new(arr))
            }
            Self::Timestamp {
                fields,
                nulls,
                kind,
                value,
                wire_text,
                date_style,
                time_zone,
                wt_bytes,
                ds_bytes,
                tz_bytes,
                ..
            } => {
                *wt_bytes = 0;
                *ds_bytes = 0;
                *tz_bytes = 0;
                let children: Vec<ArrayRef> = vec![
                    Arc::new(kind.finish()),
                    Arc::new(value.finish()),
                    Arc::new(wire_text.finish()),
                    Arc::new(date_style.finish()),
                    Arc::new(time_zone.finish()),
                ];
                let arr = StructArray::try_new_with_length(
                    fields.clone(),
                    children,
                    nulls.finish(),
                    row_count,
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build timestamp StructArray: {e}"))
                })?;
                Ok(Arc::new(arr))
            }
            Self::Interval {
                fields,
                nulls,
                months,
                days,
                microseconds,
            } => {
                let children: Vec<ArrayRef> = vec![
                    Arc::new(months.finish()),
                    Arc::new(days.finish()),
                    Arc::new(microseconds.finish()),
                ];
                let arr = StructArray::try_new_with_length(
                    fields.clone(),
                    children,
                    nulls.finish(),
                    row_count,
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build interval StructArray: {e}"))
                })?;
                Ok(Arc::new(arr))
            }
            Self::BitString {
                fields,
                nulls,
                bytes,
                bytes_offset,
                bit_length,
            } => {
                *bytes_offset = 0;
                let children: Vec<ArrayRef> =
                    vec![Arc::new(bytes.finish()), Arc::new(bit_length.finish())];
                let arr = StructArray::try_new_with_length(
                    fields.clone(),
                    children,
                    nulls.finish(),
                    row_count,
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build bitstring StructArray: {e}"))
                })?;
                Ok(Arc::new(arr))
            }
            Self::VectorFixed {
                item_field,
                dim,
                nulls,
                values,
                ..
            } => {
                let vals_arr: ArrayRef = Arc::new(values.finish());
                let arr = FixedSizeListArray::try_new(
                    Arc::clone(item_field),
                    *dim,
                    vals_arr,
                    nulls.finish(),
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build vector FixedSizeListArray: {e}"))
                })?;
                Ok(Arc::new(arr))
            }
            Self::VectorDynamic {
                item_field,
                nulls,
                offsets,
                values,
                elem_count,
                ..
            } => {
                *elem_count = 0;
                let offsets_vec = std::mem::replace(offsets, vec![0]);
                let vals_arr: ArrayRef = Arc::new(values.finish());
                let arr = ListArray::try_new(
                    Arc::clone(item_field),
                    OffsetBuffer::new(offsets_vec.into()),
                    vals_arr,
                    nulls.finish(),
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build dynamic vector ListArray: {e}"))
                })?;
                Ok(Arc::new(arr))
            }
            Self::PgArray {
                fields,
                dim_item_field,
                lb_item_field,
                val_item_field,
                nulls,
                dim_nulls,
                dim_offsets,
                dim_values,
                dim_count,
                lb_nulls,
                lb_offsets,
                lb_values,
                lb_count,
                val_nulls,
                val_offsets,
                val_builder,
                val_count,
            } => {
                *dim_count = 0;
                *lb_count = 0;
                let cur_vals = std::mem::replace(val_count, 0);

                let dims_arr = ListArray::try_new(
                    Arc::clone(dim_item_field),
                    OffsetBuffer::new(std::mem::replace(dim_offsets, vec![0]).into()),
                    Arc::new(dim_values.finish()),
                    dim_nulls.finish(),
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build pg-array dimensions: {e}"))
                })?;

                let lbs_arr = ListArray::try_new(
                    Arc::clone(lb_item_field),
                    OffsetBuffer::new(std::mem::replace(lb_offsets, vec![0]).into()),
                    Arc::new(lb_values.finish()),
                    lb_nulls.finish(),
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build pg-array lower_bounds: {e}"))
                })?;

                let elem_arr = val_builder.finish_array(cur_vals)?;
                let vals_arr = ListArray::try_new(
                    Arc::clone(val_item_field),
                    OffsetBuffer::new(std::mem::replace(val_offsets, vec![0]).into()),
                    elem_arr,
                    val_nulls.finish(),
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build pg-array values: {e}"))
                })?;

                let arr = StructArray::try_new_with_length(
                    fields.clone(),
                    vec![Arc::new(dims_arr), Arc::new(lbs_arr), Arc::new(vals_arr)],
                    nulls.finish(),
                    row_count,
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build pg-array StructArray: {e}"))
                })?;
                Ok(Arc::new(arr))
            }
            Self::Composite {
                fields,
                nulls,
                children,
            } => {
                let mut child_arrays = Vec::with_capacity(children.len());
                for child in children {
                    child_arrays.push(child.finish_array(row_count)?);
                }
                let arr = StructArray::try_new_with_length(
                    fields.clone(),
                    child_arrays,
                    nulls.finish(),
                    row_count,
                )
                .map_err(|e| {
                    PostgresError::Data(format!("failed to build pg-composite StructArray: {e}"))
                })?;
                Ok(Arc::new(arr))
            }
        }
    }
}

fn pg_value_tag(val: &PgValue) -> &'static str {
    match val {
        PgValue::Null => "Null",
        PgValue::Bool(_) => "Bool",
        PgValue::Int2(_) => "Int2",
        PgValue::Int4(_) => "Int4",
        PgValue::Int8(_) => "Int8",
        PgValue::Float4(_) => "Float4",
        PgValue::Float8(_) => "Float8",
        PgValue::Text(_) => "Text",
        PgValue::Bytes(_) => "Bytes",
        PgValue::Uuid(_) => "Uuid",
        PgValue::Numeric(_) => "Numeric",
        PgValue::Date(_) => "Date",
        PgValue::Time(_) => "Time",
        PgValue::Timestamp(_) => "Timestamp",
        PgValue::TimestampTz(_) => "TimestampTz",
        PgValue::TemporalText { .. } => "TemporalText",
        PgValue::Interval { .. } => "Interval",
        PgValue::Json(_) => "Json",
        PgValue::Array { .. } => "Array",
        PgValue::Composite { .. } => "Composite",
        PgValue::BitString { .. } => "BitString",
        PgValue::Vector { .. } => "Vector",
    }
}

fn validate_wire_utf8_no_nul(raw: &[u8]) -> Result<String, PostgresError> {
    if raw.contains(&0) {
        return Err(PostgresError::Data(
            "PostgreSQL text payload cannot contain embedded NUL bytes".to_string(),
        ));
    }
    std::str::from_utf8(raw)
        .map(str::to_owned)
        .map_err(|e| PostgresError::Data(format!("invalid UTF-8 in PostgreSQL text field: {e}")))
}

fn decode_wire_cell_for_arrow(
    wire: &ColumnWireMeta,
    raw: Option<&[u8]>,
    ctx: &CodecContext,
    catalog: &TypeCatalog,
    max_bytes: usize,
) -> Result<PgValue, PostgresError> {
    let Some(bytes) = raw else {
        return Ok(PgValue::Null);
    };
    let effective_field_cap = ctx
        .max_field_bytes
        .min(max_bytes.saturating_mul(2).max(max_bytes));
    if matches!(
        wire.base_oid,
        TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | XML_OID | UNKNOWN_OID | BYTEA_OID
    ) && wire.format == Format::Binary
        && bytes.len() > max_bytes
    {
        return Err(PostgresError::Data(format!(
            "wire field of {} bytes exceeds Arrow batch max_bytes ({max_bytes})",
            bytes.len()
        )));
    }
    if bytes.len() > ctx.max_field_bytes && bytes.len() > effective_field_cap {
        return Err(PostgresError::Data(format!(
            "field length {} exceeds max_field_bytes {}",
            bytes.len(),
            ctx.max_field_bytes
        )));
    }

    match (wire.base_oid, wire.format) {
        (REGPROC_OID | XID_OID | CID_OID, Format::Binary) => {
            if bytes.len() > ctx.max_field_bytes {
                return Err(PostgresError::Data(format!(
                    "field length {} exceeds max_field_bytes {}",
                    bytes.len(),
                    ctx.max_field_bytes
                )));
            }
            let u = protocol_types::oid_from_sql(bytes).map_err(|e| {
                PostgresError::Data(format!(
                    "invalid binary unsigned 32-bit OID {} payload: {e}",
                    wire.base_oid
                ))
            })?;
            Ok(PgValue::Int8(i64::from(u)))
        }
        (REGPROC_OID | XID_OID | CID_OID, Format::Text) => {
            if bytes.len() > ctx.max_field_bytes {
                return Err(PostgresError::Data(format!(
                    "field length {} exceeds max_field_bytes {}",
                    bytes.len(),
                    ctx.max_field_bytes
                )));
            }
            let s = validate_wire_utf8_no_nul(bytes)?;
            let u = s.trim().parse::<u32>().map_err(|_| {
                PostgresError::Data(format!(
                    "invalid unsigned 32-bit text {s:?} for OID {}",
                    wire.base_oid
                ))
            })?;
            Ok(PgValue::Int8(i64::from(u)))
        }
        (XML_OID | UNKNOWN_OID, Format::Binary | Format::Text) => {
            if bytes.len() > ctx.max_field_bytes {
                return Err(PostgresError::Data(format!(
                    "field length {} exceeds max_field_bytes {}",
                    bytes.len(),
                    ctx.max_field_bytes
                )));
            }
            let s = validate_wire_utf8_no_nul(bytes)?;
            if s.len() > max_bytes {
                return Err(PostgresError::Data(format!(
                    "text value of {} bytes exceeds Arrow batch max_bytes ({max_bytes})",
                    s.len()
                )));
            }
            Ok(PgValue::Text(s))
        }
        _ => {
            if ctx.max_nesting_depth > MAX_ARROW_NESTING_DEPTH {
                let mut clamped = ctx.clone();
                clamped.max_nesting_depth = MAX_ARROW_NESTING_DEPTH;
                decode_value_with_catalog(
                    wire.type_oid,
                    wire.format,
                    Some(bytes),
                    &clamped,
                    catalog,
                )
            } else {
                decode_value_with_catalog(wire.type_oid, wire.format, Some(bytes), ctx, catalog)
            }
        }
    }
}

/// Bounded pure-Rust PostgreSQL `RecordBatch` builder with zero Python row materialization.
pub struct PostgresArrowBatchBuilder {
    schema: Arc<Schema>,
    wire_columns: Vec<ColumnWireMeta>,
    builders: Vec<ArrowColumnBuilder>,
    max_rows: usize,
    max_bytes: usize,
    buffered_rows: usize,
    buffered_bytes: usize,
    emitted_any_batch: bool,
}

impl PostgresArrowBatchBuilder {
    /// Construct a bounded [`PostgresArrowBatchBuilder`] over `columns` and `catalog`.
    pub fn try_new(
        columns: &[Column],
        catalog: &TypeCatalog,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self, PostgresError> {
        if max_rows == 0 {
            return Err(PostgresError::Data(
                "PostgresArrowBatchBuilder max_rows must be >= 1, got 0".to_string(),
            ));
        }
        if max_bytes == 0 {
            return Err(PostgresError::Data(
                "PostgresArrowBatchBuilder max_bytes must be >= 1, got 0".to_string(),
            ));
        }

        let schema = PostgresArrowSchema::from_columns(columns, catalog)?;
        let row_cap = max_rows.min(INITIAL_BUILDER_CAPACITY_CAP);
        let mut wire_columns = Vec::with_capacity(columns.len());
        let mut builders = Vec::with_capacity(columns.len());
        for (col, field) in columns.iter().zip(schema.fields().iter()) {
            let base_oid = catalog.resolve_domain_base(col.type_oid)?;
            wire_columns.push(ColumnWireMeta {
                type_oid: col.type_oid,
                base_oid,
                format: col.format,
            });
            builders.push(ArrowColumnBuilder::from_field(field.as_ref(), row_cap, 0)?);
        }

        Ok(Self {
            schema,
            wire_columns,
            builders,
            max_rows,
            max_bytes,
            buffered_rows: 0,
            buffered_bytes: 0,
            emitted_any_batch: false,
        })
    }

    /// Return the resolved lossless Arrow [`Schema`].
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Return the number of rows currently buffered in the in-flight batch.
    pub const fn buffered_rows(&self) -> usize {
        self.buffered_rows
    }

    /// Return the accumulated decoded value bytes buffered in the in-flight batch.
    pub const fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    /// Check whether adding `row_bytes` requires flushing the current buffered batch first.
    ///
    /// Fails with [`PostgresError::Data`] before growing any Arrow buffer when `row_bytes`
    /// alone exceeds `max_bytes`.
    pub fn would_exceed_bytes(&self, row_bytes: usize) -> Result<bool, PostgresError> {
        if row_bytes > self.max_bytes {
            return Err(PostgresError::Data(format!(
                "single PostgreSQL Arrow row/value of {row_bytes} bytes exceeds max_bytes ({})",
                self.max_bytes
            )));
        }
        Ok(self.buffered_rows > 0
            && self
                .buffered_bytes
                .checked_add(row_bytes)
                .is_none_or(|sum| sum > self.max_bytes))
    }

    /// Decode a raw `DataRow` slice directly into positional Arrow column builders.
    pub fn append_data_row(
        &mut self,
        fields: &[Option<&[u8]>],
        ctx: &CodecContext,
        catalog: &TypeCatalog,
    ) -> Result<Option<RecordBatch>, PostgresError> {
        if fields.len() != self.wire_columns.len() {
            return Err(PostgresError::Data(format!(
                "DataRow field count {} does not match Arrow schema column count {}",
                fields.len(),
                self.wire_columns.len()
            )));
        }
        let mut decoded = Vec::with_capacity(fields.len());
        for (wire, raw) in self.wire_columns.iter().zip(fields.iter()) {
            decoded.push(decode_wire_cell_for_arrow(
                wire,
                *raw,
                ctx,
                catalog,
                self.max_bytes,
            )?);
        }
        self.append_pg_row(&decoded)
    }

    /// Append a decoded [`PgValue`] slice directly into positional Arrow column builders.
    pub fn append_pg_row(&mut self, row: &[PgValue]) -> Result<Option<RecordBatch>, PostgresError> {
        if row.len() != self.builders.len() {
            return Err(PostgresError::Data(format!(
                "PgValue row width {} does not match Arrow schema column count {}",
                row.len(),
                self.builders.len()
            )));
        }

        let mut prepared = Vec::with_capacity(row.len());
        let mut row_bytes = 0usize;
        for (builder, val) in self.builders.iter().zip(row.iter()) {
            let (cell, cell_bytes) = builder.prepare_cell(val, self.max_bytes)?;
            row_bytes = row_bytes.checked_add(cell_bytes).ok_or_else(|| {
                PostgresError::Data("PostgreSQL Arrow row byte count overflow".to_string())
            })?;
            prepared.push(cell);
        }

        let flush_first = self.would_exceed_bytes(row_bytes)?;
        for (builder, cell) in self.builders.iter().zip(prepared.iter()) {
            builder.validate_cells_offsets(&[cell], flush_first)?;
        }

        let prior_flushed = if flush_first {
            self.flush_batch()?
        } else {
            None
        };

        for (builder, cell) in self.builders.iter_mut().zip(prepared) {
            builder.append_prepared(cell);
        }
        self.buffered_rows += 1;
        self.buffered_bytes += row_bytes;

        if self.buffered_rows == self.max_rows {
            debug_assert!(prior_flushed.is_none());
            return self.flush_batch();
        }
        Ok(prior_flushed)
    }

    /// Emit `Some(RecordBatch)` when `buffered_rows > 0`, resetting column builders in place.
    pub fn flush_batch(&mut self) -> Result<Option<RecordBatch>, PostgresError> {
        if self.buffered_rows == 0 {
            return Ok(None);
        }
        let row_count = self.buffered_rows;
        let mut arrays = Vec::with_capacity(self.builders.len());
        for builder in &mut self.builders {
            arrays.push(builder.finish_array(row_count)?);
        }
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.emitted_any_batch = true;

        if arrays.is_empty() {
            let opts = arrow_array::RecordBatchOptions::new().with_row_count(Some(row_count));
            let batch = RecordBatch::try_new_with_options(Arc::clone(&self.schema), arrays, &opts)
                .map_err(|e| {
                    PostgresError::Data(format!("failed to construct zero-column RecordBatch: {e}"))
                })?;
            return Ok(Some(batch));
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|e| PostgresError::Data(format!("failed to construct RecordBatch: {e}")))?;
        Ok(Some(batch))
    }

    /// Construct an explicit `0`-row [`RecordBatch`] retaining the full schema and duplicate names.
    pub fn finish_empty(&self) -> RecordBatch {
        RecordBatch::new_empty(Arc::clone(&self.schema))
    }

    /// Flush any remaining tail rows, or emit a single `0`-row [`RecordBatch`] if zero rows were ever appended.
    pub fn finish_empty_or_tail(&mut self) -> Result<Option<RecordBatch>, PostgresError> {
        if self.buffered_rows > 0 {
            return self.flush_batch();
        }
        if !self.emitted_any_batch {
            self.emitted_any_batch = true;
            return Ok(Some(self.finish_empty()));
        }
        Ok(None)
    }
}
