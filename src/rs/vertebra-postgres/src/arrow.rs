//! Lossless PostgreSQL-to-Arrow schemas, bounded `RecordBatch` builders, and `VTBARROW` SHM serialization.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder,
    Int8Builder, Int16Builder, Int32Builder, Int64Builder, NullBufferBuilder, StringBuilder,
    UInt32Builder,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Decimal256Array, DictionaryArray, DurationMicrosecondArray,
    DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
    RecordBatch, StringArray, StringViewArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, types as arrow_types,
};
use arrow_buffer::OffsetBuffer;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit};
use bytes::BytesMut;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyTuple, PyType};
use vertebra_core::shm::ShmRegion;
use vertebra_db::arrow_shm::{ARROW_IPC_EOS_MARKER, ShmArrowFrameHeader, ShmArrowStreamWriter};
use vertebra_db::py_async::construct_py_arrow_cursor;

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
pub use crate::codecs::{CID_OID, REGPROC_OID, UNKNOWN_OID, XID_OID, XML_OID};
use crate::errors::PostgresError;
use crate::result::{
    Column, Format, PG_DATE_NEG_INF, PG_DATE_POS_INF, PG_TS_NEG_INF, PG_TS_POS_INF, PgValue,
    TemporalKind, VectorKind,
};
use crate::types::{CompositeField, MAX_DOMAIN_DEPTH, TypeCatalog, TypeInfo, TypeKind};

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
/// Default bounded Arrow IPC stream result ceiling (`256 MiB`).
pub const DEFAULT_MAX_ARROW_RESULT_BYTES: usize = 256 * 1024 * 1024;
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
        REGPROC_OID => match format {
            Format::Binary => Ok((DataType::UInt32, "regproc".to_string(), "scalar")),
            Format::Text => Ok((DataType::Utf8, "regproc".to_string(), "regproc-text")),
        },
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WireCapKind {
    ExactWireLen,
    BinaryJsonb,
    TextBytea,
    BinaryBitString,
    TextBitString,
    PostDecode,
}

#[derive(Clone, Debug)]
struct ColumnWireMeta {
    type_oid: u32,
    format: Format,
    wire_cap: WireCapKind,
}

impl ColumnWireMeta {
    fn from_column_and_field(base_oid: u32, col: &Column, field: &Field) -> Self {
        let enc = field
            .metadata()
            .get(POSTGRES_FIELD_ENCODING_KEY)
            .map(String::as_str)
            .unwrap_or("");
        let wire_cap = match (field.data_type(), base_oid, col.format, enc) {
            (_, JSONB_OID, Format::Binary, _) => WireCapKind::BinaryJsonb,
            (_, BYTEA_OID, Format::Binary, _) => WireCapKind::ExactWireLen,
            (_, BYTEA_OID, Format::Text, _) => WireCapKind::TextBytea,
            (_, BIT_OID | VARBIT_OID, Format::Binary, _) => WireCapKind::BinaryBitString,
            (_, BIT_OID | VARBIT_OID, Format::Text, _) => WireCapKind::TextBitString,
            (DataType::Utf8, _, _, enc) if enc != "numeric-decimal" => WireCapKind::ExactWireLen,
            _ => WireCapKind::PostDecode,
        };
        Self {
            type_oid: col.type_oid,
            format: col.format,
            wire_cap,
        }
    }
}

fn checked_i32_offset_sum(base: usize, add: usize, label: &str) -> Result<usize, PostgresError> {
    match base.checked_add(add) {
        Some(sum) if sum <= MAX_I32_OFFSET_BYTES => Ok(sum),
        _ => Err(PostgresError::Data(format!(
            "{label} buffer exceeds 32-bit Arrow i32 offset ceiling ({MAX_I32_OFFSET_BYTES} bytes)"
        ))),
    }
}

struct TemporalMetaBuilders {
    fields: Fields,
    nulls: NullBufferBuilder,
    kind: Int8Builder,
    wire_text: StringBuilder,
    date_style: StringBuilder,
    time_zone: StringBuilder,
    wt_bytes: usize,
    ds_bytes: usize,
    tz_bytes: usize,
}

impl TemporalMetaBuilders {
    fn with_capacity(fields: Fields, row_cap: usize) -> Self {
        Self {
            fields,
            nulls: NullBufferBuilder::new(row_cap),
            kind: Int8Builder::with_capacity(row_cap),
            wire_text: StringBuilder::with_capacity(row_cap, 0),
            date_style: StringBuilder::with_capacity(row_cap, 0),
            time_zone: StringBuilder::with_capacity(row_cap, 0),
            wt_bytes: 0,
            ds_bytes: 0,
            tz_bytes: 0,
        }
    }

    fn prepare_wire_text<T>(
        base_overhead: usize,
        wire_text: &str,
        date_style: &str,
        time_zone: &str,
        max_bytes: usize,
        labels: (&'static str, &'static str, &'static str),
    ) -> Result<(PreparedTemporalValue<T>, usize), PostgresError> {
        let bytes = base_overhead
            .saturating_add(wire_text.len())
            .saturating_add(date_style.len())
            .saturating_add(time_zone.len());
        if bytes > max_bytes {
            return Err(PostgresError::Data(format!(
                "{} value of {bytes} bytes exceeds Arrow batch max_bytes ({max_bytes})",
                labels.0
            )));
        }
        checked_i32_offset_sum(0, wire_text.len(), labels.0)?;
        checked_i32_offset_sum(0, date_style.len(), labels.1)?;
        checked_i32_offset_sum(0, time_zone.len(), labels.2)?;
        Ok((
            PreparedTemporalValue::WireText {
                wire_text: wire_text.to_owned(),
                date_style: date_style.to_owned(),
                time_zone: time_zone.to_owned(),
            },
            bytes,
        ))
    }

    fn validate_wire_text_offsets<'a, T: 'a>(
        &self,
        temporals: impl Iterator<Item = &'a PreparedTemporalValue<T>>,
        reset_current: bool,
        labels: (&'static str, &'static str, &'static str),
    ) -> Result<(), PostgresError> {
        let mut wt = if reset_current { 0 } else { self.wt_bytes };
        let mut ds = if reset_current { 0 } else { self.ds_bytes };
        let mut tz = if reset_current { 0 } else { self.tz_bytes };
        for tv in temporals {
            if let PreparedTemporalValue::WireText {
                wire_text,
                date_style,
                time_zone,
            } = tv
            {
                wt = checked_i32_offset_sum(wt, wire_text.len(), labels.0)?;
                ds = checked_i32_offset_sum(ds, date_style.len(), labels.1)?;
                tz = checked_i32_offset_sum(tz, time_zone.len(), labels.2)?;
            }
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.nulls.append_null();
        self.kind.append_null();
        self.wire_text.append_null();
        self.date_style.append_null();
        self.time_zone.append_null();
    }

    fn append_epoch(&mut self, kind: i8) {
        self.nulls.append_non_null();
        self.kind.append_value(kind);
        self.wire_text.append_null();
        self.date_style.append_null();
        self.time_zone.append_null();
    }

    fn append_wire_text(&mut self, wt: &str, ds: &str, tz: &str) {
        self.nulls.append_non_null();
        self.kind.append_value(2);
        self.wt_bytes += wt.len();
        self.ds_bytes += ds.len();
        self.tz_bytes += tz.len();
        self.wire_text.append_value(wt);
        self.date_style.append_value(ds);
        self.time_zone.append_value(tz);
    }

    fn finish_struct(
        &mut self,
        value_array: ArrayRef,
        row_count: usize,
        label: &str,
    ) -> Result<ArrayRef, PostgresError> {
        self.wt_bytes = 0;
        self.ds_bytes = 0;
        self.tz_bytes = 0;
        let children: Vec<ArrayRef> = vec![
            Arc::new(self.kind.finish()),
            value_array,
            Arc::new(self.wire_text.finish()),
            Arc::new(self.date_style.finish()),
            Arc::new(self.time_zone.finish()),
        ];
        let arr = StructArray::try_new_with_length(
            self.fields.clone(),
            children,
            self.nulls.finish(),
            row_count,
        )
        .map_err(|e| PostgresError::Data(format!("failed to build {label} StructArray: {e}")))?;
        Ok(Arc::new(arr))
    }
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
        meta: TemporalMetaBuilders,
        value: Int32Builder,
    },
    Timestamp {
        meta: TemporalMetaBuilders,
        expected_kind: TemporalKind,
        value: Int64Builder,
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

fn init_list_offsets(row_cap: usize) -> Vec<i32> {
    let mut offsets = Vec::with_capacity(row_cap.saturating_add(1));
    offsets.push(0);
    offsets
}

fn vector_kind_from_encoding(enc: &str) -> VectorKind {
    if enc == "pghalfvec" {
        VectorKind::HalfVec
    } else {
        VectorKind::Vector
    }
}

fn extract_list_item_field(
    field: &Field,
    parent_name: &str,
    expected: &str,
) -> Result<FieldRef, PostgresError> {
    match field.data_type() {
        DataType::List(item_field) => Ok(Arc::clone(item_field)),
        _ => Err(PostgresError::Data(format!(
            "pg-array `{parent_name}` {expected}"
        ))),
    }
}

fn check_single_cell_cap(
    bytes: usize,
    max_bytes: usize,
    label: &str,
) -> Result<usize, PostgresError> {
    if bytes > max_bytes {
        Err(PostgresError::Data(format!(
            "{label} value of {bytes} bytes exceeds Arrow batch max_bytes ({max_bytes})"
        )))
    } else {
        Ok(bytes)
    }
}

fn accumulate_child_bytes(
    total: usize,
    child_bytes: usize,
    max_bytes: usize,
    label: &str,
) -> Result<usize, PostgresError> {
    let next = total
        .checked_add(child_bytes)
        .ok_or_else(|| PostgresError::Data(format!("{label} aggregate byte count overflow")))?;
    check_single_cell_cap(next, max_bytes, label)?;
    Ok(next)
}

fn prepare_temporal_epoch<T: Copy + PartialEq>(
    val: T,
    neg_inf: T,
    pos_inf: T,
    zero: T,
) -> PreparedTemporalValue<T> {
    let (kind, value) = if val == neg_inf {
        (-1i8, zero)
    } else if val == pos_inf {
        (1i8, zero)
    } else {
        (0i8, val)
    };
    PreparedTemporalValue::Epoch { kind, value }
}

fn prepare_vector_cell(
    val: &PgValue,
    expected_kind: VectorKind,
    fixed_dim: Option<i32>,
    max_bytes: usize,
    mode_label: &str,
) -> Result<(PreparedArrowCell, usize), PostgresError> {
    match val {
        PgValue::Vector { values, kind } => {
            let (normalized, bytes) =
                validate_vector_payload(values, *kind, expected_kind, fixed_dim, max_bytes)?;
            Ok((PreparedArrowCell::Vector(normalized), bytes))
        }
        other => Err(PostgresError::Data(format!(
            "expected PgValue::Vector for {mode_label} pgvector/halfvec column, got {}",
            pg_value_tag(other)
        ))),
    }
}

fn push_null_list(nulls: &mut NullBufferBuilder, offsets: &mut Vec<i32>, count: usize) {
    nulls.append_null();
    offsets.push(count as i32);
}

fn push_i32_list_slice(
    nulls: &mut NullBufferBuilder,
    offsets: &mut Vec<i32>,
    values: &mut Int32Builder,
    count: &mut usize,
    slice: &[i32],
) {
    nulls.append_non_null();
    *count += slice.len();
    offsets.push(*count as i32);
    values.append_slice(slice);
}

fn finish_struct_array(
    fields: &Fields,
    children: Vec<ArrayRef>,
    nulls: &mut NullBufferBuilder,
    row_count: usize,
    label: &str,
) -> Result<ArrayRef, PostgresError> {
    let arr = StructArray::try_new_with_length(fields.clone(), children, nulls.finish(), row_count)
        .map_err(|e| PostgresError::Data(format!("failed to build {label}: {e}")))?;
    Ok(Arc::new(arr))
}

fn finish_list_array(
    item_field: &FieldRef,
    offsets: &mut Vec<i32>,
    values: ArrayRef,
    nulls: &mut NullBufferBuilder,
    label: &str,
) -> Result<ArrayRef, PostgresError> {
    let arr = ListArray::try_new(
        Arc::clone(item_field),
        OffsetBuffer::new(std::mem::replace(offsets, vec![0]).into()),
        values,
        nulls.finish(),
    )
    .map_err(|e| PostgresError::Data(format!("failed to build {label}: {e}")))?;
    Ok(Arc::new(arr))
}

macro_rules! match_simple_from_field {
    (
        $dt:expr, $enc:expr, $row_cap:expr;
        simple: [ $( $arrow_dt:path => $variant:ident ($builder:ty) ),* $(,)? ];
        rest: { $( $pat:pat => $body:expr ),* $(,)? }
    ) => {
        match ($dt, $enc) {
            $(
                ($arrow_dt, _) => Ok(ArrowColumnBuilder::$variant(<$builder>::with_capacity($row_cap))),
            )*
            $(
                $pat => $body,
            )*
        }
    };
}

macro_rules! match_prepare_cell {
    (
        $self_:expr, $val:expr, $max_bytes:expr;
        scalars: [ $( $b_pat:pat => ($pg_pat:path, $cell_ctor:path, $size:expr, $label:expr, $expected:expr) ),* $(,)? ];
        rest: { $( $pat:pat => $body:expr ),* $(,)? }
    ) => {
        match $self_ {
            $(
                $b_pat => match $val {
                    $pg_pat(v) => Ok(($cell_ctor(*v), check_single_cell_cap($size, $max_bytes, $label)?)),
                    other => Err(PostgresError::Data(format!(
                        "expected {}, got {}",
                        $expected,
                        pg_value_tag(other)
                    ))),
                },
            )*
            $(
                $pat => $body,
            )*
        }
    };
}

macro_rules! validate_offset_cells {
    ($cells:expr, $base:expr, $reset:expr, $pat:pat => $len:expr, $label:expr) => {{
        let mut cur = if $reset { 0 } else { *$base };
        for c in $cells {
            if let $pat = c {
                cur = checked_i32_offset_sum(cur, $len, $label)?;
            }
        }
        Ok(())
    }};
}

macro_rules! append_temporal_cell {
    ($meta:expr, $value:expr, $cell:expr, $variant:path, $label:expr) => {{
        let tv = match $cell {
            PreparedArrowCell::Null => PreparedTemporalValue::Null,
            $variant(t) => t,
            _ => unreachable!(concat!("prepared cell matches ", $label, " builder")),
        };
        match tv {
            PreparedTemporalValue::Null => {
                $meta.append_null();
                $value.append_null();
            }
            PreparedTemporalValue::Epoch {
                kind: k,
                value: raw,
            } => {
                $meta.append_epoch(k);
                $value.append_value(raw);
            }
            PreparedTemporalValue::WireText {
                wire_text: wt,
                date_style: ds,
                time_zone: tz,
            } => {
                $value.append_null();
                $meta.append_wire_text(&wt, &ds, &tz);
            }
        }
    }};
}

macro_rules! match_append_prepared {
    (
        $builder:expr, $cell:expr;
        primitives: [ $( $b_var:ident => $c_var:ident ),* $(,)? ];
        rest: { $( $pat:pat => $body:expr ),* $(,)? }
    ) => {
        match ($builder, $cell) {
            $(
                (ArrowColumnBuilder::$b_var(b), PreparedArrowCell::Null) => b.append_null(),
                (ArrowColumnBuilder::$b_var(b), PreparedArrowCell::$c_var(v)) => b.append_value(v),
            )*
            $(
                $pat => $body,
            )*
        }
    };
}

macro_rules! match_finish_array {
    (
        $builder:expr;
        simple: [ $( $s_var:ident ),* $(,)? ];
        rest: { $( $pat:pat => $body:expr ),* $(,)? }
    ) => {
        match $builder {
            $(
                ArrowColumnBuilder::$s_var(b) => Ok(Arc::new(b.finish())),
            )*
            $(
                $pat => $body,
            )*
        }
    };
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

        match_simple_from_field!(
            field.data_type(), encoding.as_str(), row_cap;
            simple: [
                DataType::Boolean => Boolean(BooleanBuilder),
                DataType::Int16 => Int16(Int16Builder),
                DataType::Int32 => Int32(Int32Builder),
                DataType::UInt32 => UInt32(UInt32Builder),
                DataType::Float32 => Float32(Float32Builder),
                DataType::Float64 => Float64(Float64Builder),
            ];
            rest: {
                (DataType::Int64, enc) => Ok(Self::Int64 {
                    builder: Int64Builder::with_capacity(row_cap),
                    is_pg_time: enc == "pg-time-micros",
                }),
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
                    meta: TemporalMetaBuilders::with_capacity(fields.clone(), row_cap),
                    value: Int32Builder::with_capacity(row_cap),
                }),
                (DataType::Struct(fields), "pg-timestamp" | "pg-timestamptz") => Ok(Self::Timestamp {
                    meta: TemporalMetaBuilders::with_capacity(fields.clone(), row_cap),
                    expected_kind: if encoding == "pg-timestamptz" {
                        TemporalKind::TimestampTz
                    } else {
                        TemporalKind::Timestamp
                    },
                    value: Int64Builder::with_capacity(row_cap),
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
                    let elem_cap = row_cap.saturating_mul((*dim as usize).min(64));
                    Ok(Self::VectorFixed {
                        item_field: Arc::clone(item_field),
                        dim: *dim,
                        expected_kind: vector_kind_from_encoding(enc),
                        nulls: NullBufferBuilder::new(row_cap),
                        values: Float32Builder::with_capacity(elem_cap),
                    })
                },
                (DataType::List(item_field), enc @ ("pgvector" | "pghalfvec")) => Ok(Self::VectorDynamic {
                    item_field: Arc::clone(item_field),
                    expected_kind: vector_kind_from_encoding(enc),
                    nulls: NullBufferBuilder::new(row_cap),
                    offsets: init_list_offsets(row_cap),
                    values: Float32Builder::with_capacity(row_cap.saturating_mul(8)),
                    elem_count: 0,
                }),
                (DataType::Struct(fields), "pg-array") => {
                    if fields.len() != 3 {
                        return Err(PostgresError::Data(format!(
                            "invalid pg-array Struct field count {} on `{}` (expected 3)",
                            fields.len(),
                            field.name()
                        )));
                    }
                    let dim_item_field = extract_list_item_field(
                        &fields[0],
                        field.name(),
                        "dimensions field must be List<Int32>",
                    )?;
                    let lb_item_field = extract_list_item_field(
                        &fields[1],
                        field.name(),
                        "lower_bounds field must be List<Int32>",
                    )?;
                    let val_item_field =
                        extract_list_item_field(&fields[2], field.name(), "values field must be List")?;
                    let val_builder = Box::new(Self::from_field(
                        val_item_field.as_ref(),
                        row_cap,
                        depth + 1,
                    )?);
                    Ok(Self::PgArray {
                        fields: fields.clone(),
                        dim_item_field,
                        lb_item_field,
                        val_item_field,
                        nulls: NullBufferBuilder::new(row_cap),
                        dim_nulls: NullBufferBuilder::new(row_cap),
                        dim_offsets: init_list_offsets(row_cap),
                        dim_values: Int32Builder::with_capacity(row_cap),
                        dim_count: 0,
                        lb_nulls: NullBufferBuilder::new(row_cap),
                        lb_offsets: init_list_offsets(row_cap),
                        lb_values: Int32Builder::with_capacity(row_cap),
                        lb_count: 0,
                        val_nulls: NullBufferBuilder::new(row_cap),
                        val_offsets: init_list_offsets(row_cap),
                        val_builder,
                        val_count: 0,
                    })
                },
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
                },
                (dt, enc) => Err(PostgresError::Data(format!(
                    "unsupported PostgreSQL Arrow column `{}` ({dt:?}, encoding `{enc}`)",
                    field.name()
                ))),
            }
        )
    }

    fn prepare_cell(
        &self,
        val: &PgValue,
        max_bytes: usize,
    ) -> Result<(PreparedArrowCell, usize), PostgresError> {
        if matches!(val, PgValue::Null) {
            return Ok((PreparedArrowCell::Null, 0));
        }

        match_prepare_cell!(
            self, val, max_bytes;
            scalars: [
                Self::Boolean(_) => (PgValue::Bool, PreparedArrowCell::Bool, 1, "bool", "PgValue::Bool for Boolean Arrow column"),
                Self::Int16(_) => (PgValue::Int2, PreparedArrowCell::Int16, 2, "int2", "PgValue::Int2 for Int16 Arrow column"),
                Self::Int32(_) => (PgValue::Int4, PreparedArrowCell::Int32, 4, "int4", "PgValue::Int4 for Int32 Arrow column"),
                Self::Int64 { is_pg_time: false, .. } => (PgValue::Int8, PreparedArrowCell::Int64, 8, "int8", "PgValue::Int8 for Int64 Arrow column"),
                Self::Float32(_) => (PgValue::Float4, PreparedArrowCell::Float32, 4, "float4", "PgValue::Float4 for Float32 Arrow column"),
                Self::Float64(_) => (PgValue::Float8, PreparedArrowCell::Float64, 8, "float8", "PgValue::Float8 for Float64 Arrow column"),
                Self::Uuid { .. } => (PgValue::Uuid, PreparedArrowCell::Uuid, 16, "uuid", "PgValue::Uuid for FixedSizeBinary(16) Arrow column"),
            ];
            rest: {
                Self::Int64 { is_pg_time: true, .. } => match val {
                    PgValue::Time(us) => Ok((
                        PreparedArrowCell::Int64(validate_time_micros(*us)?),
                        check_single_cell_cap(8, max_bytes, "time")?,
                    )),
                    other => Err(PostgresError::Data(format!(
                        "expected PgValue::Time for pg-time-micros column, got {}",
                        pg_value_tag(other)
                    ))),
                },
                Self::UInt32(_) => {
                    let parsed = match val {
                        PgValue::Int8(v) => u32::try_from(*v).map_err(|_| {
                            PostgresError::Data(format!(
                                "PostgreSQL unsigned 32-bit OID/regproc/xid/cid value {v} outside 0..={}",
                                u32::MAX
                            ))
                        })?,
                        PgValue::Text(s) => s.trim().parse::<u32>().map_err(|_| {
                            PostgresError::Data(format!(
                                "invalid PostgreSQL unsigned 32-bit OID/regproc/xid/cid text {s:?}"
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
                        check_single_cell_cap(4, max_bytes, "uint32")?,
                    ))
                },
                Self::Utf8 { encoding, .. } => {
                    let text = match (encoding.as_str(), val) {
                        ("numeric-decimal", PgValue::Numeric(num)) => {
                            let rendered = num.to_canonical_string();
                            check_single_cell_cap(rendered.len(), max_bytes, "numeric")?;
                            rendered
                        }
                        ("json" | "jsonb", PgValue::Json(raw)) => {
                            check_single_cell_cap(raw.len(), max_bytes, encoding)?;
                            std::str::from_utf8(raw)
                                .map_err(|e| {
                                    PostgresError::Data(format!("invalid UTF-8 in PgValue::Json: {e}"))
                                })?
                                .to_owned()
                        }
                        ("numeric-decimal" | "json" | "jsonb", other) => {
                            return Err(PostgresError::Data(format!(
                                "incompatible PgValue {} for `{encoding}` Utf8 Arrow column",
                                pg_value_tag(other)
                            )));
                        }
                        (_, PgValue::Text(s)) => {
                            check_single_cell_cap(s.len(), max_bytes, "text")?;
                            s.clone()
                        }
                        (_, other) => {
                            return Err(PostgresError::Data(format!(
                                "expected PgValue::Text for Utf8 Arrow column, got {}",
                                pg_value_tag(other)
                            )));
                        }
                    };
                    checked_i32_offset_sum(0, text.len(), "Utf8")?;
                    let len = text.len();
                    Ok((PreparedArrowCell::Utf8(text), len))
                },
                Self::Binary { .. } => match val {
                    PgValue::Bytes(raw) => {
                        check_single_cell_cap(raw.len(), max_bytes, "bytea")?;
                        checked_i32_offset_sum(0, raw.len(), "Binary")?;
                        Ok((PreparedArrowCell::Binary(raw.clone()), raw.len()))
                    }
                    other => Err(PostgresError::Data(format!(
                        "expected PgValue::Bytes for Binary Arrow column, got {}",
                        pg_value_tag(other)
                    ))),
                },
                Self::Date { .. } => match val {
                    PgValue::Date(days) => Ok((
                        PreparedArrowCell::Date(prepare_temporal_epoch(
                            validate_date_days(*days)?,
                            PG_DATE_NEG_INF,
                            PG_DATE_POS_INF,
                            0i32,
                        )),
                        check_single_cell_cap(5, max_bytes, "date")?,
                    )),
                    PgValue::TemporalText {
                        kind: TemporalKind::Date,
                        wire_text,
                        date_style,
                        time_zone,
                    } => {
                        let (tv, bytes) = TemporalMetaBuilders::prepare_wire_text(
                            5,
                            wire_text,
                            date_style,
                            time_zone,
                            max_bytes,
                            ("date wire_text", "date date_style", "date time_zone"),
                        )?;
                        Ok((PreparedArrowCell::Date(tv), bytes))
                    }
                    other => Err(PostgresError::Data(format!(
                        "expected PgValue::Date or Date TemporalText for pg-date column, got {}",
                        pg_value_tag(other)
                    ))),
                },
                Self::Timestamp { expected_kind, .. } => match (*expected_kind, val) {
                    (TemporalKind::Timestamp, PgValue::Timestamp(us))
                    | (TemporalKind::TimestampTz, PgValue::TimestampTz(us)) => Ok((
                        PreparedArrowCell::Timestamp(prepare_temporal_epoch(
                            validate_timestamp_micros(*us)?,
                            PG_TS_NEG_INF,
                            PG_TS_POS_INF,
                            0i64,
                        )),
                        check_single_cell_cap(9, max_bytes, "timestamp")?,
                    )),
                    (
                        exp,
                        PgValue::TemporalText {
                            kind,
                            wire_text,
                            date_style,
                            time_zone,
                        },
                    ) if exp == *kind => {
                        let (tv, bytes) = TemporalMetaBuilders::prepare_wire_text(
                            9,
                            wire_text,
                            date_style,
                            time_zone,
                            max_bytes,
                            (
                                "timestamp wire_text",
                                "timestamp date_style",
                                "timestamp time_zone",
                            ),
                        )?;
                        Ok((PreparedArrowCell::Timestamp(tv), bytes))
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
                            check_single_cell_cap(16, max_bytes, "interval")?,
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
                Self::VectorFixed { dim, expected_kind, .. } => {
                    prepare_vector_cell(val, *expected_kind, Some(*dim), max_bytes, "fixed")
                },
                Self::VectorDynamic { expected_kind, .. } => {
                    prepare_vector_cell(val, *expected_kind, None, max_bytes, "dynamic")
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
                        checked_i32_offset_sum(0, values.len(), "pg-array element list")?;

                        let mut total_bytes =
                            dimensions.len().saturating_mul(8).min(MAX_I32_OFFSET_BYTES);
                        check_single_cell_cap(total_bytes, max_bytes, "pg-array header")?;

                        let mut prepared_vals = Vec::with_capacity(values.len());
                        for elem in values {
                            let (p_elem, elem_bytes) = val_builder.prepare_cell(elem, max_bytes)?;
                            total_bytes =
                                accumulate_child_bytes(total_bytes, elem_bytes, max_bytes, "pg-array")?;
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
                Self::Composite { fields, children, .. } => match val {
                    PgValue::Composite { field_names, values, .. } => {
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
                            total_bytes = accumulate_child_bytes(
                                total_bytes,
                                child_bytes,
                                max_bytes,
                                "pg-composite",
                            )?;
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
        )
    }

    fn validate_cells_offsets(
        &self,
        cells: &[&PreparedArrowCell],
        reset_current: bool,
    ) -> Result<(), PostgresError> {
        match self {
            Self::Utf8 { offset_bytes, .. } => validate_offset_cells!(
                cells,
                offset_bytes,
                reset_current,
                PreparedArrowCell::Utf8(s) => s.len(),
                "Utf8"
            ),
            Self::Binary { offset_bytes, .. } => validate_offset_cells!(
                cells,
                offset_bytes,
                reset_current,
                PreparedArrowCell::Binary(b) => b.len(),
                "Binary"
            ),
            Self::BitString { bytes_offset, .. } => validate_offset_cells!(
                cells,
                bytes_offset,
                reset_current,
                PreparedArrowCell::BitString { data, .. } => data.len(),
                "bitstring bytes"
            ),
            Self::VectorDynamic { elem_count, .. } => validate_offset_cells!(
                cells,
                elem_count,
                reset_current,
                PreparedArrowCell::Vector(v) => v.len(),
                "dynamic vector list"
            ),
            Self::Date { meta, .. } => meta.validate_wire_text_offsets(
                cells.iter().filter_map(|c| match c {
                    PreparedArrowCell::Date(tv) => Some(tv),
                    _ => None,
                }),
                reset_current,
                ("date wire_text", "date date_style", "date time_zone"),
            ),
            Self::Timestamp { meta, .. } => meta.validate_wire_text_offsets(
                cells.iter().filter_map(|c| match c {
                    PreparedArrowCell::Timestamp(tv) => Some(tv),
                    _ => None,
                }),
                reset_current,
                (
                    "timestamp wire_text",
                    "timestamp date_style",
                    "timestamp time_zone",
                ),
            ),
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
                        dc = checked_i32_offset_sum(dc, dimensions.len(), "pg-array dimensions")?;
                        lc = checked_i32_offset_sum(
                            lc,
                            lower_bounds.len(),
                            "pg-array lower_bounds",
                        )?;
                        vc = checked_i32_offset_sum(vc, values.len(), "pg-array values")?;
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
        match_append_prepared!(
            self, cell;
            primitives: [
                Boolean => Bool,
                Int16 => Int16,
                Int32 => Int32,
                UInt32 => UInt32,
                Float32 => Float32,
                Float64 => Float64,
            ];
            rest: {
                (Self::Int64 { builder, .. }, PreparedArrowCell::Null) => builder.append_null(),
                (Self::Int64 { builder, .. }, PreparedArrowCell::Int64(v)) => builder.append_value(v),
                (Self::Utf8 { builder, .. }, PreparedArrowCell::Null) => builder.append_null(),
                (Self::Utf8 { builder, offset_bytes, .. }, PreparedArrowCell::Utf8(s)) => {
                    *offset_bytes += s.len();
                    builder.append_value(&s);
                },
                (Self::Binary { builder, .. }, PreparedArrowCell::Null) => builder.append_null(),
                (Self::Binary { builder, offset_bytes }, PreparedArrowCell::Binary(raw)) => {
                    *offset_bytes += raw.len();
                    builder.append_value(&raw);
                },
                (Self::Uuid { builder, .. }, PreparedArrowCell::Null) => builder.append_null(),
                (Self::Uuid { builder, .. }, PreparedArrowCell::Uuid(bytes)) => {
                    builder
                        .append_value(bytes)
                        .expect("validated 16-byte UUID matches FixedSizeBinaryBuilder(16)");
                },
                (Self::Date { meta, value }, cell) => {
                    append_temporal_cell!(meta, value, cell, PreparedArrowCell::Date, "Date");
                },
                (Self::Timestamp { meta, value, .. }, cell) => {
                    append_temporal_cell!(meta, value, cell, PreparedArrowCell::Timestamp, "Timestamp");
                },
                (Self::Interval { nulls, months, days, microseconds, .. }, PreparedArrowCell::Null) => {
                    nulls.append_null();
                    months.append_null();
                    days.append_null();
                    microseconds.append_null();
                },
                (
                    Self::Interval { nulls, months, days, microseconds, .. },
                    PreparedArrowCell::Interval { months: m, days: d, microseconds: us },
                ) => {
                    nulls.append_non_null();
                    months.append_value(m);
                    days.append_value(d);
                    microseconds.append_value(us);
                },
                (Self::BitString { nulls, bytes, bit_length, .. }, PreparedArrowCell::Null) => {
                    nulls.append_null();
                    bytes.append_null();
                    bit_length.append_null();
                },
                (
                    Self::BitString { nulls, bytes, bytes_offset, bit_length, .. },
                    PreparedArrowCell::BitString { data, bit_length: bl },
                ) => {
                    nulls.append_non_null();
                    *bytes_offset += data.len();
                    bytes.append_value(&data);
                    bit_length.append_value(bl);
                },
                (Self::VectorFixed { dim, nulls, values, .. }, PreparedArrowCell::Null) => {
                    nulls.append_null();
                    for _ in 0..*dim {
                        values.append_null();
                    }
                },
                (Self::VectorFixed { nulls, values, .. }, PreparedArrowCell::Vector(vec)) => {
                    nulls.append_non_null();
                    values.append_slice(&vec);
                },
                (Self::VectorDynamic { nulls, offsets, elem_count, .. }, PreparedArrowCell::Null) => {
                    push_null_list(nulls, offsets, *elem_count);
                },
                (
                    Self::VectorDynamic { nulls, offsets, values, elem_count, .. },
                    PreparedArrowCell::Vector(vec),
                ) => {
                    nulls.append_non_null();
                    *elem_count += vec.len();
                    offsets.push(*elem_count as i32);
                    values.append_slice(&vec);
                },
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
                    push_null_list(dim_nulls, dim_offsets, *dim_count);
                    push_null_list(lb_nulls, lb_offsets, *lb_count);
                    push_null_list(val_nulls, val_offsets, *val_count);
                },
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
                    PreparedArrowCell::Array { dimensions, lower_bounds, values },
                ) => {
                    nulls.append_non_null();
                    push_i32_list_slice(dim_nulls, dim_offsets, dim_values, dim_count, &dimensions);
                    push_i32_list_slice(lb_nulls, lb_offsets, lb_values, lb_count, &lower_bounds);
                    val_nulls.append_non_null();
                    *val_count += values.len();
                    val_offsets.push(*val_count as i32);
                    for v in values {
                        val_builder.append_prepared(v);
                    }
                },
                (Self::Composite { nulls, children, .. }, PreparedArrowCell::Null) => {
                    nulls.append_null();
                    for child in children {
                        child.append_prepared(PreparedArrowCell::Null);
                    }
                },
                (Self::Composite { nulls, children, .. }, PreparedArrowCell::Composite(attrs)) => {
                    nulls.append_non_null();
                    for (child, attr) in children.iter_mut().zip(attrs) {
                        child.append_prepared(attr);
                    }
                },
                _ => unreachable!("prepared cell variant matches ArrowColumnBuilder variant"),
            }
        );
    }

    fn finish_array(&mut self, row_count: usize) -> Result<ArrayRef, PostgresError> {
        match_finish_array!(
            self;
            simple: [Boolean, Int16, Int32, UInt32, Float32, Float64];
            rest: {
                Self::Int64 { builder, .. } => Ok(Arc::new(builder.finish())),
                Self::Utf8 { builder, offset_bytes, .. } => {
                    *offset_bytes = 0;
                    Ok(Arc::new(builder.finish()))
                },
                Self::Binary { builder, offset_bytes } => {
                    *offset_bytes = 0;
                    Ok(Arc::new(builder.finish()))
                },
                Self::Uuid { builder, row_cap } => {
                    let fresh = FixedSizeBinaryBuilder::with_capacity(*row_cap, 16);
                    let finished = std::mem::replace(builder, fresh).finish();
                    Ok(Arc::new(finished))
                },
                Self::Date { meta, value } => {
                    meta.finish_struct(Arc::new(value.finish()), row_count, "date")
                },
                Self::Timestamp { meta, value, .. } => {
                    meta.finish_struct(Arc::new(value.finish()), row_count, "timestamp")
                },
                Self::Interval { fields, nulls, months, days, microseconds } => finish_struct_array(
                    fields,
                    vec![
                        Arc::new(months.finish()),
                        Arc::new(days.finish()),
                        Arc::new(microseconds.finish()),
                    ],
                    nulls,
                    row_count,
                    "interval StructArray",
                ),
                Self::BitString { fields, nulls, bytes, bytes_offset, bit_length } => {
                    *bytes_offset = 0;
                    finish_struct_array(
                        fields,
                        vec![Arc::new(bytes.finish()), Arc::new(bit_length.finish())],
                        nulls,
                        row_count,
                        "bitstring StructArray",
                    )
                },
                Self::VectorFixed { item_field, dim, nulls, values, .. } => {
                    let arr = FixedSizeListArray::try_new(
                        Arc::clone(item_field),
                        *dim,
                        Arc::new(values.finish()),
                        nulls.finish(),
                    )
                    .map_err(|e| {
                        PostgresError::Data(format!("failed to build vector FixedSizeListArray: {e}"))
                    })?;
                    Ok(Arc::new(arr))
                },
                Self::VectorDynamic { item_field, nulls, offsets, values, elem_count, .. } => {
                    *elem_count = 0;
                    finish_list_array(
                        item_field,
                        offsets,
                        Arc::new(values.finish()),
                        nulls,
                        "dynamic vector ListArray",
                    )
                },
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
                    let dims_arr = finish_list_array(
                        dim_item_field,
                        dim_offsets,
                        Arc::new(dim_values.finish()),
                        dim_nulls,
                        "pg-array dimensions",
                    )?;
                    let lbs_arr = finish_list_array(
                        lb_item_field,
                        lb_offsets,
                        Arc::new(lb_values.finish()),
                        lb_nulls,
                        "pg-array lower_bounds",
                    )?;
                    let elem_arr = val_builder.finish_array(cur_vals)?;
                    let vals_arr = finish_list_array(
                        val_item_field,
                        val_offsets,
                        elem_arr,
                        val_nulls,
                        "pg-array values",
                    )?;
                    finish_struct_array(
                        fields,
                        vec![dims_arr, lbs_arr, vals_arr],
                        nulls,
                        row_count,
                        "pg-array StructArray",
                    )
                },
                Self::Composite { fields, nulls, children } => {
                    let mut child_arrays = Vec::with_capacity(children.len());
                    for child in children {
                        child_arrays.push(child.finish_array(row_count)?);
                    }
                    finish_struct_array(fields, child_arrays, nulls, row_count, "pg-composite StructArray")
                },
            }
        )
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

fn wire_payload_exceeds_max_bytes(wire_cap: WireCapKind, bytes: &[u8], max_bytes: usize) -> bool {
    match wire_cap {
        WireCapKind::ExactWireLen => bytes.len() > max_bytes,
        WireCapKind::BinaryJsonb => bytes.len().saturating_sub(1) > max_bytes,
        WireCapKind::TextBytea => {
            if let Some(hex_body) = bytes.strip_prefix(b"\\x") {
                let hex_digits = hex_body
                    .iter()
                    .filter(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
                    .count();
                hex_digits / 2 > max_bytes
            } else {
                bytes.len().div_ceil(4) > max_bytes
            }
        }
        WireCapKind::BinaryBitString => bytes.len() >= 4 && bytes.len() > max_bytes,
        WireCapKind::TextBitString => bytes.len().div_ceil(8).saturating_add(4) > max_bytes,
        WireCapKind::PostDecode => false,
    }
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
    if wire_payload_exceeds_max_bytes(wire.wire_cap, bytes, max_bytes) {
        return Err(PostgresError::Data(format!(
            "wire field of {} bytes exceeds Arrow batch max_bytes ({max_bytes})",
            bytes.len()
        )));
    }
    decode_value_with_catalog(wire.type_oid, wire.format, Some(bytes), ctx, catalog)
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
            wire_columns.push(ColumnWireMeta::from_column_and_field(
                base_oid,
                col,
                field.as_ref(),
            ));
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
        let clamped_ctx = if ctx.max_nesting_depth > MAX_ARROW_NESTING_DEPTH {
            let mut c = ctx.clone();
            c.max_nesting_depth = MAX_ARROW_NESTING_DEPTH;
            Cow::Owned(c)
        } else {
            Cow::Borrowed(ctx)
        };
        let mut decoded = Vec::with_capacity(fields.len());
        for (wire, raw) in self.wire_columns.iter().zip(fields.iter()) {
            decoded.push(decode_wire_cell_for_arrow(
                wire,
                *raw,
                clamped_ctx.as_ref(),
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

static PG_ARROW_SHM_COUNTER: AtomicU64 = AtomicU64::new(1);
static POSTGRES_ARROW_RESULT_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

#[inline]
fn next_postgres_arrow_shm_name() -> String {
    let seq = PG_ARROW_SHM_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("/vtb-pg-{pid:x}-{seq:x}")
}

#[inline]
fn checked_align8(len: usize) -> Result<usize, PostgresError> {
    len.checked_add(7)
        .map(|n| n & !7)
        .ok_or_else(|| PostgresError::Data("Arrow IPC 8-byte frame alignment overflow".to_string()))
}

fn resolve_postgres_arrow_result_type(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    let cls = POSTGRES_ARROW_RESULT_TYPE.get_or_try_init(py, || -> PyResult<Py<PyType>> {
        let mod_types = py.import("vertebra.db.adapters.postgres.types")?;
        Ok(mod_types
            .getattr("PostgresArrowResult")?
            .cast_into::<PyType>()?
            .unbind())
    })?;
    Ok(cls.bind(py))
}

/// Adaptive inline vs forced `VTBARROW` shared-memory output mode for [`PostgresArrowIpcWriter`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArrowStorageMode {
    /// Buffer serialized IPC frames inline while `total_rows <= threshold_rows`, promoting the
    /// accumulated schema and batch groups to a `VTBARROW` shared-memory segment once exceeded.
    Adaptive { threshold_rows: usize },
    /// Immediately stream the schema and every batch group into a `VTBARROW` shared-memory segment.
    AlwaysShm,
}

/// Completed Arrow IPC payload produced by [`PostgresArrowIpcWriter::finish`].
#[derive(Debug)]
pub enum ArrowOutput {
    /// Self-contained Arrow IPC stream buffered inline (`total_rows <= threshold_rows`).
    Inline {
        ipc_bytes: Vec<u8>,
        row_count: usize,
        columns: Vec<String>,
    },
    /// Unregistered `VTBARROW` POSIX shared-memory mapping ready for `ShmCapsule` handoff.
    Shm {
        shm_name: String,
        byte_len: usize,
        row_count: usize,
        columns: Vec<String>,
        region: Arc<ShmRegion>,
    },
}

enum ArrowIpcSink {
    Inline { buf: Vec<u8>, threshold_rows: usize },
    Shm(ShmArrowStreamWriter),
    Poisoned,
}

/// Single-stream Arrow IPC serializer promoting complete 8-byte-aligned message groups into
/// unregistered `VTBARROW` shared memory without splitting FlatBuffers message boundaries.
pub struct PostgresArrowIpcWriter {
    stream_writer: StreamWriter<Vec<u8>>,
    columns: Vec<String>,
    sink: ArrowIpcSink,
    max_result_bytes: usize,
    committed_ipc_bytes: usize,
    row_count: usize,
}

impl PostgresArrowIpcWriter {
    /// Initialize a single [`StreamWriter<Vec<u8>>`] for `schema`, validating `max_result_bytes`
    /// before buffering inline or creating an unregistered `VTBARROW` shared-memory writer.
    pub fn try_new(
        schema: Arc<Schema>,
        columns: Vec<String>,
        mode: ArrowStorageMode,
        max_result_bytes: usize,
    ) -> Result<Self, PostgresError> {
        if max_result_bytes == 0 {
            return Err(PostgresError::Data(
                "max_result_bytes must be >= 1".to_string(),
            ));
        }

        let mut stream_writer =
            StreamWriter::try_new(Vec::new(), schema.as_ref()).map_err(|e| {
                PostgresError::Data(format!("failed to initialize Arrow IPC StreamWriter: {e}"))
            })?;
        let mut schema_frame = std::mem::take(stream_writer.get_mut());
        let padded_schema = checked_align8(schema_frame.len())?;
        let min_stream_bytes = padded_schema
            .checked_add(ARROW_IPC_EOS_MARKER.len())
            .ok_or_else(|| PostgresError::Data("Arrow IPC stream size overflow".to_string()))?;
        if min_stream_bytes > max_result_bytes {
            return Err(PostgresError::Data(format!(
                "Arrow IPC minimum stream size {min_stream_bytes} bytes exceeds max_result_bytes ({max_result_bytes})"
            )));
        }

        let sink = match mode {
            ArrowStorageMode::Adaptive { threshold_rows } => {
                if padded_schema > schema_frame.len() {
                    schema_frame.resize(padded_schema, 0);
                }
                ArrowIpcSink::Inline {
                    buf: schema_frame,
                    threshold_rows,
                }
            }
            ArrowStorageMode::AlwaysShm => {
                let shm_name = next_postgres_arrow_shm_name();
                let shm_writer = ShmArrowStreamWriter::create(&shm_name, 0, &schema_frame)
                    .map_err(PostgresError::Operational)?;
                ArrowIpcSink::Shm(shm_writer)
            }
        };

        Ok(Self {
            stream_writer,
            columns,
            sink,
            max_result_bytes,
            committed_ipc_bytes: padded_schema,
            row_count: 0,
        })
    }

    /// Encode `batch` through the shared [`StreamWriter`], enforce `max_result_bytes`, and either
    /// append inline or promote/append the complete message group into `VTBARROW` shared memory.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PostgresError> {
        if matches!(self.sink, ArrowIpcSink::Poisoned) {
            return Err(PostgresError::Operational(
                "PostgresArrowIpcWriter cannot write after a previous error".to_string(),
            ));
        }
        let res = self.write_batch_inner(batch);
        if res.is_err() {
            self.sink = ArrowIpcSink::Poisoned;
            self.stream_writer.get_mut().clear();
        }
        res
    }

    fn write_batch_inner(&mut self, batch: &RecordBatch) -> Result<(), PostgresError> {
        let batch_rows = batch.num_rows();
        let next_rows = self
            .row_count
            .checked_add(batch_rows)
            .ok_or_else(|| PostgresError::Data("Arrow IPC row count overflow".to_string()))?;

        self.stream_writer.write(batch).map_err(|e| {
            PostgresError::Data(format!(
                "failed to encode Arrow RecordBatch into IPC stream: {e}"
            ))
        })?;
        let mut frame = std::mem::take(self.stream_writer.get_mut());
        let padded_len = checked_align8(frame.len())?;
        let next_committed = self
            .committed_ipc_bytes
            .checked_add(padded_len)
            .ok_or_else(|| {
                PostgresError::Data("Arrow IPC stream byte size overflow".to_string())
            })?;
        let total_with_eos = next_committed
            .checked_add(ARROW_IPC_EOS_MARKER.len())
            .ok_or_else(|| {
                PostgresError::Data("Arrow IPC stream byte size overflow".to_string())
            })?;
        if total_with_eos > self.max_result_bytes {
            return Err(PostgresError::Data(format!(
                "Arrow IPC result size {total_with_eos} bytes exceeds max_result_bytes ({})",
                self.max_result_bytes
            )));
        }

        match &mut self.sink {
            ArrowIpcSink::Inline {
                buf,
                threshold_rows,
            } => {
                if next_rows <= *threshold_rows {
                    if padded_len > frame.len() {
                        frame.resize(padded_len, 0);
                    }
                    buf.extend_from_slice(&frame);
                } else {
                    let shm_name = next_postgres_arrow_shm_name();
                    let init_capacity = ShmArrowFrameHeader::SIZE.saturating_add(total_with_eos);
                    let prior_inline = std::mem::take(buf);
                    let mut shm_writer =
                        ShmArrowStreamWriter::create(&shm_name, init_capacity, &prior_inline)
                            .map_err(PostgresError::Operational)?;
                    shm_writer
                        .append_ipc_frame(&frame, batch_rows)
                        .map_err(PostgresError::Operational)?;
                    self.sink = ArrowIpcSink::Shm(shm_writer);
                }
            }
            ArrowIpcSink::Shm(shm_writer) => {
                shm_writer
                    .append_ipc_frame(&frame, batch_rows)
                    .map_err(PostgresError::Operational)?;
            }
            ArrowIpcSink::Poisoned => unreachable!("checked before write_batch_inner"),
        }

        self.committed_ipc_bytes = next_committed;
        self.row_count = next_rows;
        Ok(())
    }

    /// Finalize the single [`StreamWriter`] and emit either inline IPC bytes or an unregistered
    /// `VTBARROW` [`ShmRegion`] terminated by exactly one 8-byte Arrow IPC EOS marker.
    pub fn finish(mut self) -> Result<ArrowOutput, PostgresError> {
        let sink = std::mem::replace(&mut self.sink, ArrowIpcSink::Poisoned);
        match sink {
            ArrowIpcSink::Poisoned => Err(PostgresError::Operational(
                "PostgresArrowIpcWriter cannot finish after a previous error".to_string(),
            )),
            ArrowIpcSink::Inline { mut buf, .. } => {
                self.stream_writer.finish().map_err(|e| {
                    PostgresError::Data(format!("failed to finalize Arrow IPC StreamWriter: {e}"))
                })?;
                let eos_tail = self.stream_writer.into_inner().map_err(|e| {
                    PostgresError::Data(format!("failed to drain Arrow IPC tail buffer: {e}"))
                })?;
                let final_len = buf.len().checked_add(eos_tail.len()).ok_or_else(|| {
                    PostgresError::Data("Arrow IPC inline length overflow".to_string())
                })?;
                if final_len > self.max_result_bytes {
                    return Err(PostgresError::Data(format!(
                        "Arrow IPC result size {final_len} bytes exceeds max_result_bytes ({})",
                        self.max_result_bytes
                    )));
                }
                buf.extend_from_slice(&eos_tail);
                Ok(ArrowOutput::Inline {
                    ipc_bytes: buf,
                    row_count: self.row_count,
                    columns: self.columns,
                })
            }
            ArrowIpcSink::Shm(mut shm_writer) => {
                self.stream_writer.finish().map_err(|e| {
                    PostgresError::Data(format!("failed to finalize Arrow IPC StreamWriter: {e}"))
                })?;
                let eos_tail = self.stream_writer.into_inner().map_err(|e| {
                    PostgresError::Data(format!("failed to drain Arrow IPC tail buffer: {e}"))
                })?;
                let pre_eos = eos_tail
                    .strip_suffix(&ARROW_IPC_EOS_MARKER)
                    .unwrap_or(&eos_tail);
                if !pre_eos.is_empty() {
                    shm_writer
                        .append_ipc_frame(pre_eos, 0)
                        .map_err(PostgresError::Operational)?;
                }
                let (shm_name, byte_len, region) = shm_writer
                    .finish_unregistered()
                    .map_err(PostgresError::Operational)?;
                if byte_len > self.max_result_bytes {
                    return Err(PostgresError::Data(format!(
                        "Arrow IPC result size {byte_len} bytes exceeds max_result_bytes ({})",
                        self.max_result_bytes
                    )));
                }
                Ok(ArrowOutput::Shm {
                    shm_name,
                    byte_len,
                    row_count: self.row_count,
                    columns: self.columns,
                    region,
                })
            }
        }
    }
}

/// Transfer an [`ArrowOutput::Shm`] payload into a `vertebra._vertebra.ArrowCursor` via the C-ABI
/// `c"vertebra.ShmCapsule"` producer drop callback contract on the caller's Python thread.
pub fn shm_output_into_py_cursor(py: Python<'_>, output: ArrowOutput) -> PyResult<Py<PyAny>> {
    match output {
        ArrowOutput::Shm {
            shm_name,
            byte_len,
            row_count,
            region,
            ..
        } => construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region),
        ArrowOutput::Inline { .. } => Err(crate::errors::postgres_error_to_py(
            py,
            PostgresError::Interface(
                "shm_output_into_py_cursor requires ArrowOutput::Shm".to_string(),
            ),
        )),
    }
}

/// Convert a finished [`ArrowOutput`] into a frozen `PostgresArrowResult` (`types.py`) on the
/// caller's Python thread, transferring SHM ownership across `.so` boundaries via `ShmCapsule`.
pub fn arrow_output_into_py_result(py: Python<'_>, output: ArrowOutput) -> PyResult<Py<PyAny>> {
    let result_cls = resolve_postgres_arrow_result_type(py)?;
    match output {
        ArrowOutput::Inline {
            ipc_bytes,
            row_count,
            columns,
        } => {
            let py_bytes = PyBytes::new_with(py, ipc_bytes.len(), |buf| {
                buf.copy_from_slice(&ipc_bytes);
                Ok(())
            })?;
            let py_cols = PyTuple::new(py, &columns)?;
            let obj = result_cls.call1((py_bytes, row_count, py_cols, false))?;
            Ok(obj.unbind())
        }
        ArrowOutput::Shm {
            shm_name,
            byte_len,
            row_count,
            columns,
            region,
        } => {
            let py_cols = PyTuple::new(py, &columns)?;
            let py_cursor = construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region)?;
            let obj = result_cls.call1((py_cursor, row_count, py_cols, true))?;
            Ok(obj.unbind())
        }
    }
}

/// Hard per-call Arrow IPC stream byte ceiling accepted by `copy_from_arrow` and `write_arrow_ipc` (`16 MiB`).
pub const MAX_ARROW_COPY_CALL_BYTES: usize = 16 * 1024 * 1024;
/// Hard per-batch decoded Arrow array memory ceiling accepted during `COPY` import (`8 MiB`).
pub const MAX_ARROW_COPY_BATCH_BYTES: usize = 8 * 1024 * 1024;

const UNIX_TO_PG_EPOCH_DAYS: i64 = 10_957;
const UNIX_TO_PG_EPOCH_MICROS: i128 = 946_684_800_000_000;
const MILLIS_PER_DAY: i64 = 86_400_000;

/// Quote a non-empty, NUL-free PostgreSQL identifier (`"..."` with `"` doubled to `""`).
pub(crate) fn quote_postgres_identifier(ident: &str) -> Result<String, PostgresError> {
    if ident.is_empty() {
        return Err(PostgresError::Data(
            "PostgreSQL identifier cannot be empty".to_string(),
        ));
    }
    if ident.as_bytes().contains(&0) {
        return Err(PostgresError::Data(
            "PostgreSQL identifier cannot contain embedded NUL byte".to_string(),
        ));
    }
    let mut out = String::with_capacity(ident.len() + 2);
    out.push('"');
    for ch in ident.chars() {
        if ch == '"' {
            out.push_str("\"\"");
        } else {
            out.push(ch);
        }
    }
    out.push('"');
    Ok(out)
}

/// Parse and validate a `COPY` target relation identifier into `(schema_name, table_name)`.
///
/// When `schema_name` is `Some(s)`, both `s` and `table_name` are validated strictly as literal
/// PostgreSQL identifiers without splitting `.` or altering `"` characters. When `schema_name` is
/// `None`, `table_name` is parsed with a strict PostgreSQL double-quote-aware `1`-or-`2`-segment
/// splitter rejecting empty segments, unclosed quotes, embedded NULs, whitespace in unquoted
/// segments, or `> 2` segments.
pub fn parse_postgres_qualified_table(
    table_name: &str,
    schema_name: Option<&str>,
) -> Result<(Option<String>, String), PostgresError> {
    if let Some(schema) = schema_name {
        if schema.is_empty() {
            return Err(PostgresError::Data(
                "PostgreSQL schema_name cannot be empty".to_string(),
            ));
        }
        if schema.as_bytes().contains(&0) {
            return Err(PostgresError::Data(
                "PostgreSQL schema_name cannot contain embedded NUL byte".to_string(),
            ));
        }
        if table_name.is_empty() {
            return Err(PostgresError::Data(
                "PostgreSQL table_name cannot be empty".to_string(),
            ));
        }
        if table_name.as_bytes().contains(&0) {
            return Err(PostgresError::Data(
                "PostgreSQL table_name cannot contain embedded NUL byte".to_string(),
            ));
        }
        return Ok((Some(schema.to_owned()), table_name.to_owned()));
    }

    if table_name.is_empty() {
        return Err(PostgresError::Data(
            "PostgreSQL table_name cannot be empty".to_string(),
        ));
    }
    if table_name.as_bytes().contains(&0) {
        return Err(PostgresError::Data(
            "PostgreSQL table_name cannot contain embedded NUL byte".to_string(),
        ));
    }

    let mut chars = table_name.chars().peekable();
    let mut segments: Vec<String> = Vec::with_capacity(2);

    loop {
        match chars.peek().copied() {
            None => {
                return Err(PostgresError::Data(format!(
                    "empty identifier segment in qualified table name {table_name:?}"
                )));
            }
            Some('.') => {
                return Err(PostgresError::Data(format!(
                    "empty identifier segment before '.' in table name {table_name:?}"
                )));
            }
            Some('"') => {
                chars.next();
                let mut seg = String::new();
                let mut closed = false;
                while let Some(ch) = chars.next() {
                    if ch == '"' {
                        if chars.peek() == Some(&'"') {
                            chars.next();
                            seg.push('"');
                        } else {
                            closed = true;
                            break;
                        }
                    } else {
                        seg.push(ch);
                    }
                }
                if !closed {
                    return Err(PostgresError::Data(format!(
                        "unclosed double quote in PostgreSQL identifier {table_name:?}"
                    )));
                }
                if seg.is_empty() {
                    return Err(PostgresError::Data(format!(
                        "empty double-quoted PostgreSQL identifier in {table_name:?}"
                    )));
                }
                segments.push(seg);
                match chars.next() {
                    None => break,
                    Some('.') => {
                        if segments.len() >= 2 {
                            return Err(PostgresError::Data(format!(
                                "qualified PostgreSQL table name {table_name:?} exceeds 2 segments"
                            )));
                        }
                    }
                    Some(extra) => {
                        return Err(PostgresError::Data(format!(
                            "unexpected character {extra:?} after quoted identifier in {table_name:?}"
                        )));
                    }
                }
            }
            Some(_) => {
                let mut seg = String::new();
                let mut hit_dot = false;
                for ch in chars.by_ref() {
                    if ch == '.' {
                        hit_dot = true;
                        break;
                    }
                    if ch == '"'
                        || ch.is_whitespace()
                        || ch.is_control()
                        || matches!(ch, ';' | '(' | ')' | ',')
                    {
                        return Err(PostgresError::Data(format!(
                            "invalid unquoted character {ch:?} in PostgreSQL identifier {table_name:?}"
                        )));
                    }
                    seg.push(ch);
                }
                if seg.is_empty() {
                    return Err(PostgresError::Data(format!(
                        "empty identifier segment in {table_name:?}"
                    )));
                }
                segments.push(seg);
                if hit_dot {
                    if segments.len() >= 2 {
                        return Err(PostgresError::Data(format!(
                            "qualified PostgreSQL table name {table_name:?} exceeds 2 segments"
                        )));
                    }
                } else {
                    break;
                }
            }
        }
    }

    match segments.len() {
        1 => Ok((None, segments.pop().expect("len == 1"))),
        2 => {
            let tbl = segments.pop().expect("len == 2");
            let sch = segments.pop().expect("len == 2");
            Ok((Some(sch), tbl))
        }
        other => Err(PostgresError::Data(format!(
            "invalid segment count {other} in PostgreSQL table name {table_name:?}"
        ))),
    }
}

/// Decode a self-contained Arrow IPC stream subject to [`MAX_ARROW_COPY_CALL_BYTES`] (`16 MiB`) and
/// per-batch [`MAX_ARROW_COPY_BATCH_BYTES`] (`8 MiB`) caps before allocating unbounded reader state.
pub fn decode_bounded_ipc_batches(
    ipc_bytes: &[u8],
) -> Result<(Arc<Schema>, Vec<RecordBatch>), PostgresError> {
    if ipc_bytes.is_empty() {
        return Err(PostgresError::Data(
            "Arrow IPC stream payload cannot be empty".to_string(),
        ));
    }
    if ipc_bytes.len() > MAX_ARROW_COPY_CALL_BYTES {
        return Err(PostgresError::Data(format!(
            "Arrow IPC payload size {} bytes exceeds MAX_ARROW_COPY_CALL_BYTES ({MAX_ARROW_COPY_CALL_BYTES})",
            ipc_bytes.len()
        )));
    }
    let mut cursor = std::io::Cursor::new(ipc_bytes);
    let (schema, batches) = {
        let mut reader = StreamReader::try_new(&mut cursor, None)
            .map_err(|e| PostgresError::Data(format!("invalid Arrow IPC stream header: {e}")))?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for item in &mut reader {
            let batch = item.map_err(|e| {
                PostgresError::Data(format!("invalid Arrow IPC RecordBatch frame: {e}"))
            })?;
            let mem = batch.get_array_memory_size();
            if mem > MAX_ARROW_COPY_BATCH_BYTES {
                return Err(PostgresError::Data(format!(
                    "Arrow RecordBatch memory size {mem} bytes exceeds MAX_ARROW_COPY_BATCH_BYTES ({MAX_ARROW_COPY_BATCH_BYTES})"
                )));
            }
            batches.push(batch);
        }
        (schema, batches)
    };
    let consumed = cursor.position() as usize;
    if consumed < ipc_bytes.len() && ipc_bytes[consumed..].iter().any(|&b| b != 0) {
        return Err(PostgresError::Data(
            "unexpected trailing bytes after Arrow IPC end-of-stream marker".to_string(),
        ));
    }
    Ok((schema, batches))
}

#[derive(Clone, Debug)]
enum DictKeyType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
}

#[derive(Clone, Copy, Debug)]
enum IntTargetKind {
    Int2,
    Int4,
    Int8,
    OidU32,
    Numeric,
}

#[derive(Clone, Copy, Debug)]
enum FloatTargetKind {
    Float4,
    Float8,
}

#[derive(Clone, Copy, Debug)]
enum StringTargetKind {
    Text,
    Json,
    NumericDecimal,
}

#[derive(Clone, Copy, Debug)]
enum BinaryTargetKind {
    Bytea,
    Uuid,
}

#[derive(Clone, Copy, Debug)]
enum TemporalStructTarget {
    Date,
    Timestamp,
    TimestampTz,
}

#[derive(Clone, Copy, Debug)]
enum ListOffsetsKind {
    I32,
    I64,
    Fixed(i32),
}

#[derive(Clone, Debug)]
enum FieldImporter {
    Null,
    Bool,
    Int8(IntTargetKind),
    Int16(IntTargetKind),
    Int32(IntTargetKind),
    Int64(IntTargetKind),
    UInt8(IntTargetKind),
    UInt16(IntTargetKind),
    UInt32(IntTargetKind),
    UInt64(IntTargetKind),
    Float16(FloatTargetKind),
    Float32(FloatTargetKind),
    Float64(FloatTargetKind),
    Utf8(StringTargetKind),
    LargeUtf8(StringTargetKind),
    Utf8View(StringTargetKind),
    Binary(BinaryTargetKind),
    LargeBinary,
    BinaryView,
    FixedSizeBinary {
        size: i32,
        target: BinaryTargetKind,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    Date32,
    Date64,
    Time32Sec,
    Time32Ms,
    Time64Us,
    Time64Ns,
    Timestamp {
        unit: TimeUnit,
        is_tz: bool,
    },
    IntervalYearMonth,
    IntervalDayTime,
    IntervalMonthDayNano,
    Duration(TimeUnit),
    PgTemporalStruct(TemporalStructTarget),
    PgIntervalStruct,
    PgBitStringStruct {
        fixed_bit_typmod: Option<i32>,
    },
    PgVector {
        offsets: ListOffsetsKind,
        kind: VectorKind,
        expected_dim: Option<usize>,
    },
    PgLosslessArray {
        element_oid: u32,
        element: Box<FieldImporter>,
    },
    OrdinaryArray {
        offsets: ListOffsetsKind,
        element_oid: u32,
        element: Box<FieldImporter>,
    },
    CompositeStruct {
        type_oid: u32,
        field_names: Vec<String>,
        field_oids: Vec<u32>,
        children: Vec<FieldImporter>,
    },
    Dictionary {
        key_type: DictKeyType,
        values: Box<FieldImporter>,
    },
}

/// Validated Arrow-to-PostgreSQL binary `COPY` import plan compiled from an Arrow [`Schema`] and
/// ordered target [`Column`] metadata before issuing `COPY ... FROM STDIN (FORMAT binary)`.
#[derive(Clone, Debug)]
pub struct PostgresArrowImportPlan {
    expected_schema: Schema,
    column_oids: Vec<u32>,
    importers: Vec<FieldImporter>,
}

impl PostgresArrowImportPlan {
    /// Compile and validate a lossless or ordinary Arrow [`Schema`] against target PostgreSQL
    /// [`Column`] metadata and discovered [`TypeCatalog`] entries (`0`-row safe).
    pub fn from_schema(
        schema: &Schema,
        target_columns: &[Column],
        catalog: &TypeCatalog,
    ) -> Result<Self, PostgresError> {
        if target_columns.is_empty() {
            return Err(PostgresError::Data(
                "COPY FROM Arrow requires at least one target column".to_string(),
            ));
        }
        if i16::try_from(target_columns.len()).is_err() {
            return Err(PostgresError::Data(format!(
                "COPY FROM Arrow target column count {} exceeds i16::MAX ({})",
                target_columns.len(),
                i16::MAX
            )));
        }
        let mut seen_names = std::collections::HashSet::with_capacity(target_columns.len());
        for col in target_columns {
            if col.name.is_empty() || col.name.as_bytes().contains(&0) {
                return Err(PostgresError::Data(format!(
                    "invalid PostgreSQL COPY target column name {:?}",
                    col.name
                )));
            }
            if !seen_names.insert(col.name.as_str()) {
                return Err(PostgresError::Data(format!(
                    "duplicate target column `{}` in COPY FROM Arrow plan",
                    col.name
                )));
            }
        }
        if schema.fields().len() != target_columns.len() {
            return Err(PostgresError::Data(format!(
                "Arrow schema column count {} does not match COPY target column count {}",
                schema.fields().len(),
                target_columns.len()
            )));
        }
        if let Some(ver) = schema.metadata().get(POSTGRES_ARROW_VERSION_KEY)
            && ver != POSTGRES_ARROW_VERSION
        {
            return Err(PostgresError::Data(format!(
                "unsupported {POSTGRES_ARROW_VERSION_KEY}={ver:?} (expected {POSTGRES_ARROW_VERSION:?})"
            )));
        }

        let mut column_oids = Vec::with_capacity(target_columns.len());
        let mut importers = Vec::with_capacity(target_columns.len());
        for (arrow_field, col) in schema.fields().iter().zip(target_columns.iter()) {
            let importer = compile_field_importer(
                arrow_field.as_ref(),
                col.type_oid,
                col.type_modifier,
                catalog,
                0,
            )?;
            column_oids.push(col.type_oid);
            importers.push(importer);
        }

        Ok(Self {
            expected_schema: schema.clone(),
            column_oids,
            importers,
        })
    }

    /// Encode a single [`RecordBatch`] into PostgreSQL binary `COPY` frames via `encoder`, leaving
    /// both `out` and `encoder` untouched on any schema, cap, overflow, or `wire_text` validation error.
    pub fn encode_batch(
        &self,
        batch: &RecordBatch,
        encoder: &mut crate::copy::BinaryCopyEncoder,
        ctx: &CodecContext,
        catalog: &TypeCatalog,
        out: &mut BytesMut,
    ) -> Result<(), PostgresError> {
        let mem_bytes = batch.get_array_memory_size();
        if mem_bytes > MAX_ARROW_COPY_BATCH_BYTES {
            return Err(PostgresError::Data(format!(
                "Arrow RecordBatch memory size {mem_bytes} bytes exceeds MAX_ARROW_COPY_BATCH_BYTES ({MAX_ARROW_COPY_BATCH_BYTES})"
            )));
        }
        let batch_schema = batch.schema();
        if batch.num_columns() != self.expected_schema.fields().len()
            || batch_schema.fields().len() != self.expected_schema.fields().len()
        {
            return Err(PostgresError::Data(format!(
                "Arrow RecordBatch column count {} does not match plan column count {}",
                batch.num_columns(),
                self.expected_schema.fields().len()
            )));
        }
        if batch_schema.metadata() != self.expected_schema.metadata() {
            return Err(PostgresError::Data(
                "Arrow RecordBatch schema metadata changed across batches".to_string(),
            ));
        }
        for (idx, (got, exp)) in batch_schema
            .fields()
            .iter()
            .zip(self.expected_schema.fields().iter())
            .enumerate()
        {
            if got.data_type() != exp.data_type()
                || got.metadata() != exp.metadata()
                || batch.column(idx).data_type() != exp.data_type()
            {
                return Err(PostgresError::Data(format!(
                    "Arrow RecordBatch column {idx} (`{}`) schema disagrees with compiled import plan",
                    exp.name()
                )));
            }
        }

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let saved_out_len = out.len();
        let saved_header_written = encoder.is_header_written();
        let mut row_vals = vec![PgValue::Null; self.importers.len()];

        for row_idx in 0..num_rows {
            for (col_idx, importer) in self.importers.iter().enumerate() {
                let col_arr = batch.column(col_idx);
                match extract_field_value(importer, col_arr.as_ref(), row_idx) {
                    Ok(v) => row_vals[col_idx] = v,
                    Err(err) => {
                        out.truncate(saved_out_len);
                        encoder.set_header_written(saved_header_written);
                        return Err(err);
                    }
                }
            }
            if let Err(err) =
                encoder.encode_row_values(&self.column_oids, &row_vals, ctx, catalog, out)
            {
                out.truncate(saved_out_len);
                encoder.set_header_written(saved_header_written);
                return Err(err);
            }
        }

        Ok(())
    }
}

fn validate_import_target_oid(
    target_oid: u32,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<u32, PostgresError> {
    if depth > MAX_ARROW_NESTING_DEPTH {
        return Err(PostgresError::Data(format!(
            "Arrow COPY import nesting depth {depth} exceeds MAX_ARROW_NESTING_DEPTH ({MAX_ARROW_NESTING_DEPTH})"
        )));
    }
    if target_oid == 0 {
        return Err(PostgresError::Data(
            "COPY target column OID must be positive (>= 1)".to_string(),
        ));
    }
    let base_oid = catalog.resolve_domain_base(target_oid)?;
    if base_oid == UNKNOWN_OID {
        return Err(PostgresError::Data(
            "PostgreSQL pseudo-type `unknown` (OID 705) cannot be a binary COPY target column"
                .to_string(),
        ));
    }
    if base_oid == BOX_OID {
        return Err(PostgresError::Data(
            "PostgreSQL `box` (OID 603) is not supported for binary COPY import".to_string(),
        ));
    }
    if base_oid == RECORD_OID {
        return Err(PostgresError::Data(
            "PostgreSQL anonymous `record` (OID 2249) cannot be encoded for binary COPY import"
                .to_string(),
        ));
    }

    match catalog.type_info(base_oid) {
        Some(TypeInfo {
            name,
            kind: TypeKind::Range,
            ..
        }) => {
            return Err(PostgresError::Data(format!(
                "PostgreSQL range type `{name}` (OID {base_oid}) is not supported for binary COPY import"
            )));
        }
        Some(TypeInfo {
            name,
            kind: TypeKind::Enum,
            ..
        }) => {
            if depth > 0 {
                return Err(PostgresError::Data(format!(
                    "nested PostgreSQL enum `{name}` (OID {base_oid}) is not supported inside binary COPY containers"
                )));
            }
            return Ok(base_oid);
        }
        Some(TypeInfo {
            name,
            kind: TypeKind::Array { .. } | TypeKind::Composite { .. },
            ..
        }) => {
            if !crate::codecs::supports_binary_with_catalog(base_oid, catalog) {
                return Err(PostgresError::Data(format!(
                    "PostgreSQL container type `{name}` (OID {base_oid}) contains non-binary fields unsupported by binary COPY"
                )));
            }
            return Ok(base_oid);
        }
        Some(TypeInfo {
            kind: TypeKind::Extension { name, .. },
            ..
        }) if name == "vector" || name == "halfvec" => {
            return Ok(base_oid);
        }
        _ => {}
    }

    if !crate::codecs::supports_binary_with_catalog(base_oid, catalog) {
        return Err(PostgresError::Data(format!(
            "PostgreSQL target OID {target_oid} (base {base_oid}) does not support binary COPY import"
        )));
    }
    Ok(base_oid)
}

fn validate_arrow_timezone(tz: &str) -> Result<(), PostgresError> {
    if tz.is_empty() || tz.as_bytes().contains(&0) {
        return Err(PostgresError::Data(
            "Arrow Timestamp timezone string cannot be empty or contain NUL".to_string(),
        ));
    }
    if tz.eq_ignore_ascii_case("UTC") || tz == "Z" {
        return Ok(());
    }
    if let Some(rest) = tz.strip_prefix('+').or_else(|| tz.strip_prefix('-'))
        && parse_fixed_tz_offset(rest)
    {
        return Ok(());
    }
    if jiff::tz::TimeZone::get(tz).is_ok() {
        return Ok(());
    }
    Err(PostgresError::Data(format!(
        "unrecognized Arrow Timestamp timezone {tz:?}"
    )))
}

fn parse_fixed_tz_offset(s: &str) -> bool {
    let parts: Vec<&str> = if s.contains(':') {
        s.split(':').collect()
    } else if s.len() == 4 {
        vec![&s[..2], &s[2..]]
    } else if s.len() == 2 {
        vec![s, "00"]
    } else {
        return false;
    };
    if parts.len() != 2 && parts.len() != 3 {
        return false;
    }
    if parts.iter().any(|p| p.len() != 2) {
        return false;
    }
    let Ok(hh) = parts[0].parse::<u8>() else {
        return false;
    };
    let Ok(mm) = parts[1].parse::<u8>() else {
        return false;
    };
    let ss = if parts.len() == 3 {
        let Ok(v) = parts[2].parse::<u8>() else {
            return false;
        };
        v
    } else {
        0
    };
    hh <= 23 && mm <= 59 && ss <= 59
}

fn compile_field_importer(
    field: &Field,
    target_oid: u32,
    target_typmod: i32,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<FieldImporter, PostgresError> {
    let field_name = field.name();
    if let Some(enc) = field.metadata().get(POSTGRES_FIELD_ENCODING_KEY) {
        match enc.as_str() {
            "regproc-text" | "pg-range" | "record-text" | "unknown-text" => {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` carries text-only `{enc}` metadata incompatible with binary COPY"
                )));
            }
            _ => {}
        }
    }

    let base_target_oid = validate_import_target_oid(target_oid, catalog, depth)?;

    if let Some(enc) = field.metadata().get(POSTGRES_FIELD_ENCODING_KEY) {
        return compile_pg_tagged_importer(
            field,
            enc,
            target_oid,
            base_target_oid,
            target_typmod,
            catalog,
            depth,
        );
    }

    if let Some(raw_oid) = field.metadata().get(POSTGRES_FIELD_OID_KEY) {
        let parsed = raw_oid.parse::<u32>().map_err(|_| {
            PostgresError::Data(format!(
                "invalid {POSTGRES_FIELD_OID_KEY}={raw_oid:?} on Arrow field `{field_name}`"
            ))
        })?;
        let src_base = catalog.resolve_domain_base(parsed).unwrap_or(parsed);
        let unk_to_text = src_base == UNKNOWN_OID
            && matches!(
                base_target_oid,
                TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | XML_OID
            );
        if src_base != base_target_oid && !unk_to_text {
            return Err(PostgresError::Data(format!(
                "Arrow field `{field_name}` metadata OID {parsed} disagrees with COPY target OID {target_oid}"
            )));
        }
    }

    compile_ordinary_importer(
        field_name,
        field.data_type(),
        target_oid,
        base_target_oid,
        target_typmod,
        catalog,
        depth,
    )
}

fn compile_pg_tagged_importer(
    field: &Field,
    enc: &str,
    target_oid: u32,
    base_target_oid: u32,
    target_typmod: i32,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<FieldImporter, PostgresError> {
    let field_name = field.name();
    let raw_oid = field.metadata().get(POSTGRES_FIELD_OID_KEY).ok_or_else(|| {
        PostgresError::Data(format!(
            "Arrow field `{field_name}` has `{POSTGRES_FIELD_ENCODING_KEY}={enc}` without `{POSTGRES_FIELD_OID_KEY}`"
        ))
    })?;
    let src_oid = raw_oid.parse::<u32>().map_err(|_| {
        PostgresError::Data(format!(
            "Arrow field `{field_name}` has invalid `{POSTGRES_FIELD_OID_KEY}`={raw_oid:?}"
        ))
    })?;
    if src_oid == 0 {
        return Err(PostgresError::Data(format!(
            "Arrow field `{field_name}` has zero `{POSTGRES_FIELD_OID_KEY}`"
        )));
    }
    let src_base_oid = if let Some(raw_base) = field.metadata().get(POSTGRES_FIELD_BASE_OID_KEY) {
        let parsed = raw_base.parse::<u32>().map_err(|_| {
            PostgresError::Data(format!(
                "Arrow field `{field_name}` has invalid `{POSTGRES_FIELD_BASE_OID_KEY}`={raw_base:?}"
            ))
        })?;
        if parsed == 0 {
            return Err(PostgresError::Data(format!(
                "Arrow field `{field_name}` has zero `{POSTGRES_FIELD_BASE_OID_KEY}`"
            )));
        }
        parsed
    } else {
        catalog.resolve_domain_base(src_oid).unwrap_or(src_oid)
    };

    let unk_to_text = src_base_oid == UNKNOWN_OID
        && enc == "scalar"
        && matches!(
            base_target_oid,
            TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | XML_OID
        );
    if src_base_oid != base_target_oid && !unk_to_text {
        return Err(PostgresError::Data(format!(
            "Arrow field `{field_name}` source OID {src_oid} (base {src_base_oid}) is incompatible with COPY target OID {target_oid} (base {base_target_oid})"
        )));
    }

    let dt = field.data_type();
    match enc {
        "scalar" => match (base_target_oid, dt) {
            (BOOL_OID, DataType::Boolean) => Ok(FieldImporter::Bool),
            (INT2_OID, DataType::Int16) => Ok(FieldImporter::Int16(IntTargetKind::Int2)),
            (INT4_OID, DataType::Int32) => Ok(FieldImporter::Int32(IntTargetKind::Int4)),
            (INT8_OID, DataType::Int64) => Ok(FieldImporter::Int64(IntTargetKind::Int8)),
            (OID_OID | REGPROC_OID | XID_OID | CID_OID, DataType::UInt32) => {
                Ok(FieldImporter::UInt32(IntTargetKind::OidU32))
            }
            (FLOAT4_OID, DataType::Float32) => Ok(FieldImporter::Float32(FloatTargetKind::Float4)),
            (FLOAT8_OID, DataType::Float64) => Ok(FieldImporter::Float64(FloatTargetKind::Float8)),
            (TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | XML_OID, DataType::Utf8) => {
                Ok(FieldImporter::Utf8(StringTargetKind::Text))
            }
            (BYTEA_OID, DataType::Binary) => Ok(FieldImporter::Binary(BinaryTargetKind::Bytea)),
            (UUID_OID, DataType::FixedSizeBinary(16)) => Ok(FieldImporter::FixedSizeBinary {
                size: 16,
                target: BinaryTargetKind::Uuid,
            }),
            _ => Err(PostgresError::Data(format!(
                "Arrow field `{field_name}` DataType {dt:?} is invalid for `scalar` OID {base_target_oid}"
            ))),
        },
        "pg-enum" => {
            if depth > 0 {
                return Err(PostgresError::Data(format!(
                    "nested `pg-enum` field `{field_name}` is not supported in binary COPY"
                )));
            }
            let is_enum = matches!(
                catalog.type_info(base_target_oid),
                Some(TypeInfo {
                    kind: TypeKind::Enum,
                    ..
                })
            );
            if !is_enum || *dt != DataType::Utf8 {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-enum`, {dt:?}) does not match enum target OID {base_target_oid}"
                )));
            }
            Ok(FieldImporter::Utf8(StringTargetKind::Text))
        }
        "json" => {
            if base_target_oid != JSON_OID || *dt != DataType::Utf8 {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`json`, {dt:?}) requires JSON_OID + Utf8"
                )));
            }
            Ok(FieldImporter::Utf8(StringTargetKind::Json))
        }
        "jsonb" => {
            if base_target_oid != JSONB_OID || *dt != DataType::Utf8 {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`jsonb`, {dt:?}) requires JSONB_OID + Utf8"
                )));
            }
            Ok(FieldImporter::Utf8(StringTargetKind::Json))
        }
        "numeric-decimal" => {
            if base_target_oid != NUMERIC_OID || *dt != DataType::Utf8 {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`numeric-decimal`, {dt:?}) requires NUMERIC_OID + Utf8"
                )));
            }
            Ok(FieldImporter::Utf8(StringTargetKind::NumericDecimal))
        }
        "pg-date" => {
            if base_target_oid != DATE_OID || *dt != temporal_struct_data_type(DataType::Int32) {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-date`) has mismatched target OID or struct layout"
                )));
            }
            Ok(FieldImporter::PgTemporalStruct(TemporalStructTarget::Date))
        }
        "pg-time-micros" => {
            if base_target_oid != TIME_OID || *dt != DataType::Int64 {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-time-micros`, {dt:?}) requires TIME_OID + Int64"
                )));
            }
            Ok(FieldImporter::Time64Us)
        }
        "pg-timestamp" => {
            if base_target_oid != TIMESTAMP_OID || *dt != temporal_struct_data_type(DataType::Int64)
            {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-timestamp`) has mismatched target OID or struct layout"
                )));
            }
            Ok(FieldImporter::PgTemporalStruct(
                TemporalStructTarget::Timestamp,
            ))
        }
        "pg-timestamptz" => {
            if base_target_oid != TIMESTAMPTZ_OID
                || *dt != temporal_struct_data_type(DataType::Int64)
            {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-timestamptz`) has mismatched target OID or struct layout"
                )));
            }
            Ok(FieldImporter::PgTemporalStruct(
                TemporalStructTarget::TimestampTz,
            ))
        }
        "pg-interval" => {
            if base_target_oid != INTERVAL_OID || *dt != interval_struct_data_type() {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-interval`) has mismatched target OID or struct layout"
                )));
            }
            Ok(FieldImporter::PgIntervalStruct)
        }
        "pg-bits" => {
            if !matches!(base_target_oid, BIT_OID | VARBIT_OID)
                || *dt != bitstring_struct_data_type()
            {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-bits`) has mismatched target OID or struct layout"
                )));
            }
            let fixed_bit_typmod =
                (base_target_oid == BIT_OID && target_typmod > 0).then_some(target_typmod);
            Ok(FieldImporter::PgBitStringStruct { fixed_bit_typmod })
        }
        "pgvector" | "pghalfvec" => {
            let (expected_ext, vec_kind) = if enc == "pgvector" {
                ("vector", VectorKind::Vector)
            } else {
                ("halfvec", VectorKind::HalfVec)
            };
            let matches_ext = matches!(
                catalog.type_info(base_target_oid),
                Some(TypeInfo {
                    kind: TypeKind::Extension { name, .. },
                    ..
                }) if name == expected_ext
            );
            if !matches_ext {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`{enc}`) does not match target extension OID {base_target_oid}"
                )));
            }
            compile_vector_list_importer(field_name, dt, vec_kind, target_typmod)
        }
        "pg-array" => {
            let Some(TypeInfo {
                kind: TypeKind::Array { element_oid, .. },
                ..
            }) = catalog.type_info(base_target_oid)
            else {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-array`) targets non-array OID {base_target_oid}"
                )));
            };
            let DataType::Struct(children) = dt else {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-array`) must be a 3-field Struct, got {dt:?}"
                )));
            };
            if children.len() != 3
                || children[0].name() != "dimensions"
                || children[1].name() != "lower_bounds"
                || children[2].name() != "values"
            {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-array`) struct fields must be (dimensions, lower_bounds, values)"
                )));
            }
            let valid_dims = matches!(
                children[0].data_type(),
                DataType::List(f) if *f.data_type() == DataType::Int32
            );
            let valid_lbs = matches!(
                children[1].data_type(),
                DataType::List(f) if *f.data_type() == DataType::Int32
            );
            let DataType::List(val_item) = children[2].data_type() else {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-array`) `values` field must be DataType::List"
                )));
            };
            if !valid_dims || !valid_lbs {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-array`) dimensions/lower_bounds must be List(Int32)"
                )));
            }
            let resolved_elem_oid = catalog.resolve_domain_base(*element_oid)?;
            let elem_imp = compile_field_importer(
                val_item.as_ref(),
                *element_oid,
                target_typmod,
                catalog,
                depth + 1,
            )?;
            Ok(FieldImporter::PgLosslessArray {
                element_oid: resolved_elem_oid,
                element: Box::new(elem_imp),
            })
        }
        "pg-composite" => {
            let Some(TypeInfo {
                kind: TypeKind::Composite { fields },
                ..
            }) = catalog.type_info(base_target_oid)
            else {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-composite`) targets non-composite OID {base_target_oid}"
                )));
            };
            if fields.is_empty() {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` targets composite OID {base_target_oid} with zero catalog attributes"
                )));
            }
            let DataType::Struct(arrow_children) = dt else {
                return Err(PostgresError::Data(format!(
                    "Arrow field `{field_name}` (`pg-composite`) requires DataType::Struct, got {dt:?}"
                )));
            };
            if arrow_children.len() != fields.len() {
                return Err(PostgresError::Data(format!(
                    "Arrow composite `{field_name}` field count {} does not match PostgreSQL composite attribute count {}",
                    arrow_children.len(),
                    fields.len()
                )));
            }
            let mut field_names = Vec::with_capacity(fields.len());
            let mut field_oids = Vec::with_capacity(fields.len());
            let mut child_importers = Vec::with_capacity(fields.len());
            for (arrow_child, pg_attr) in arrow_children.iter().zip(fields.iter()) {
                if arrow_child.name() != &pg_attr.name {
                    return Err(PostgresError::Data(format!(
                        "Arrow composite `{field_name}` attribute `{}` does not match expected `{}`",
                        arrow_child.name(),
                        pg_attr.name
                    )));
                }
                let imp = compile_field_importer(
                    arrow_child.as_ref(),
                    pg_attr.type_oid,
                    pg_attr.type_modifier,
                    catalog,
                    depth + 1,
                )?;
                field_names.push(pg_attr.name.clone());
                field_oids.push(pg_attr.type_oid);
                child_importers.push(imp);
            }
            Ok(FieldImporter::CompositeStruct {
                type_oid: base_target_oid,
                field_names,
                field_oids,
                children: child_importers,
            })
        }
        other => Err(PostgresError::Data(format!(
            "unrecognized `{POSTGRES_FIELD_ENCODING_KEY}`={other:?} on Arrow field `{field_name}`"
        ))),
    }
}

fn compile_vector_list_importer(
    field_name: &str,
    dt: &DataType,
    kind: VectorKind,
    target_typmod: i32,
) -> Result<FieldImporter, PostgresError> {
    let expected_dim = if target_typmod == -1 {
        None
    } else if (1..=MAX_PGVECTOR_ARROW_DIM).contains(&target_typmod) {
        Some(target_typmod as usize)
    } else {
        return Err(PostgresError::Data(format!(
            "invalid vector typmod {target_typmod} on column `{field_name}`"
        )));
    };
    let (offsets, item_dt) = match dt {
        DataType::List(item) => (ListOffsetsKind::I32, item.data_type()),
        DataType::LargeList(item) => (ListOffsetsKind::I64, item.data_type()),
        DataType::FixedSizeList(item, dim) => {
            if !(*dim >= 1 && *dim <= MAX_PGVECTOR_ARROW_DIM) {
                return Err(PostgresError::Data(format!(
                    "Arrow FixedSizeList dimension {dim} on `{field_name}` is outside 1..={MAX_PGVECTOR_ARROW_DIM}"
                )));
            }
            if let Some(exp) = expected_dim
                && (*dim as usize) != exp
            {
                return Err(PostgresError::Data(format!(
                    "Arrow FixedSizeList dimension {dim} on `{field_name}` does not match vector({exp})"
                )));
            }
            (ListOffsetsKind::Fixed(*dim), item.data_type())
        }
        _ => {
            return Err(PostgresError::Data(format!(
                "Arrow field `{field_name}` cannot map {dt:?} to PostgreSQL {kind:?}"
            )));
        }
    };
    if !matches!(
        item_dt,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    ) {
        return Err(PostgresError::Data(format!(
            "Arrow vector field `{field_name}` requires Float16/Float32/Float64 elements, got {item_dt:?}"
        )));
    }
    Ok(FieldImporter::PgVector {
        offsets,
        kind,
        expected_dim,
    })
}

fn int_target_for_oid(base_target_oid: u32) -> Option<IntTargetKind> {
    match base_target_oid {
        INT2_OID => Some(IntTargetKind::Int2),
        INT4_OID => Some(IntTargetKind::Int4),
        INT8_OID => Some(IntTargetKind::Int8),
        OID_OID | REGPROC_OID | XID_OID | CID_OID => Some(IntTargetKind::OidU32),
        NUMERIC_OID => Some(IntTargetKind::Numeric),
        _ => None,
    }
}

fn string_target_for_oid(
    base_target_oid: u32,
    catalog: &TypeCatalog,
    depth: usize,
) -> Option<StringTargetKind> {
    match base_target_oid {
        TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | XML_OID => Some(StringTargetKind::Text),
        JSON_OID | JSONB_OID => Some(StringTargetKind::Json),
        _ => match catalog.type_info(base_target_oid) {
            Some(TypeInfo {
                kind: TypeKind::Enum,
                ..
            }) if depth == 0 => Some(StringTargetKind::Text),
            _ => None,
        },
    }
}

fn float_target_for_oid(base_target_oid: u32) -> Option<FloatTargetKind> {
    match base_target_oid {
        FLOAT4_OID => Some(FloatTargetKind::Float4),
        FLOAT8_OID => Some(FloatTargetKind::Float8),
        _ => None,
    }
}

fn dict_key_type_for_dt(dt: &DataType, field_name: &str) -> Result<DictKeyType, PostgresError> {
    macro_rules! map_dict_keys {
        ($dt:expr, [ $( $v:ident ),* $(,)? ]) => {
            match $dt {
                $( DataType::$v => Ok(DictKeyType::$v), )*
                other => Err(PostgresError::Data(format!(
                    "unsupported Arrow Dictionary key DataType {other:?} on `{field_name}`"
                ))),
            }
        };
    }
    map_dict_keys!(
        dt,
        [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64]
    )
}

macro_rules! match_ordinary_importer {
    (
        $dt:expr, $field_name:expr, $target_oid:expr, $base_target_oid:expr, $catalog:expr, $depth:expr;
        ints: [ $( $int_v:ident ),* $(,)? ];
        floats: [ $( $flt_v:ident ),* $(,)? ];
        strings: [ $( $str_v:ident ),* $(,)? ];
        rest: { $( $pat:pat $(if $guard:expr)? => $body:expr ),* $(,)? }
    ) => {
        match $dt {
            $(
                DataType::$int_v => int_target_for_oid($base_target_oid)
                    .map(FieldImporter::$int_v)
                    .ok_or_else(|| incompatible_arrow_type($field_name, $dt, $target_oid)),
            )*
            $(
                DataType::$flt_v => float_target_for_oid($base_target_oid)
                    .map(FieldImporter::$flt_v)
                    .ok_or_else(|| incompatible_arrow_type($field_name, $dt, $target_oid)),
            )*
            $(
                DataType::$str_v => string_target_for_oid($base_target_oid, $catalog, $depth)
                    .map(FieldImporter::$str_v)
                    .ok_or_else(|| incompatible_arrow_type($field_name, $dt, $target_oid)),
            )*
            $(
                $pat $(if $guard)? => $body,
            )*
        }
    };
}

fn compile_ordinary_importer(
    field_name: &str,
    dt: &DataType,
    target_oid: u32,
    base_target_oid: u32,
    target_typmod: i32,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<FieldImporter, PostgresError> {
    if depth > MAX_ARROW_NESTING_DEPTH {
        return Err(PostgresError::Data(format!(
            "Arrow schema nesting depth {depth} exceeds MAX_ARROW_NESTING_DEPTH ({MAX_ARROW_NESTING_DEPTH})"
        )));
    }

    match_ordinary_importer!(
        dt, field_name, target_oid, base_target_oid, catalog, depth;
        ints: [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64];
        floats: [Float16, Float32, Float64];
        strings: [Utf8, LargeUtf8, Utf8View];
        rest: {
            DataType::Null => Ok(FieldImporter::Null),
            DataType::Boolean if base_target_oid == BOOL_OID => Ok(FieldImporter::Bool),
            DataType::Binary if base_target_oid == BYTEA_OID => {
                Ok(FieldImporter::Binary(BinaryTargetKind::Bytea))
            },
            DataType::LargeBinary if base_target_oid == BYTEA_OID => Ok(FieldImporter::LargeBinary),
            DataType::BinaryView if base_target_oid == BYTEA_OID => Ok(FieldImporter::BinaryView),
            DataType::FixedSizeBinary(size) => match (base_target_oid, *size) {
                (UUID_OID, 16) => Ok(FieldImporter::FixedSizeBinary {
                    size: 16,
                    target: BinaryTargetKind::Uuid,
                }),
                (BYTEA_OID, n) if n >= 0 => Ok(FieldImporter::FixedSizeBinary {
                    size: n,
                    target: BinaryTargetKind::Bytea,
                }),
                _ => Err(incompatible_arrow_type(field_name, dt, target_oid)),
            },
            DataType::Decimal128(precision, scale) if base_target_oid == NUMERIC_OID => {
                if *precision == 0 || *precision > 38 {
                    return Err(PostgresError::Data(format!(
                        "invalid Arrow Decimal128 precision {precision} on `{field_name}`"
                    )));
                }
                Ok(FieldImporter::Decimal128 {
                    precision: *precision,
                    scale: *scale,
                })
            },
            DataType::Decimal256(precision, scale) if base_target_oid == NUMERIC_OID => {
                if *precision == 0 || *precision > 76 {
                    return Err(PostgresError::Data(format!(
                        "invalid Arrow Decimal256 precision {precision} on `{field_name}`"
                    )));
                }
                Ok(FieldImporter::Decimal256 {
                    precision: *precision,
                    scale: *scale,
                })
            },
            DataType::Date32 if base_target_oid == DATE_OID => Ok(FieldImporter::Date32),
            DataType::Date64 if base_target_oid == DATE_OID => Ok(FieldImporter::Date64),
            DataType::Time32(TimeUnit::Second) if base_target_oid == TIME_OID => {
                Ok(FieldImporter::Time32Sec)
            },
            DataType::Time32(TimeUnit::Millisecond) if base_target_oid == TIME_OID => {
                Ok(FieldImporter::Time32Ms)
            },
            DataType::Time64(TimeUnit::Microsecond) if base_target_oid == TIME_OID => {
                Ok(FieldImporter::Time64Us)
            },
            DataType::Time64(TimeUnit::Nanosecond) if base_target_oid == TIME_OID => {
                Ok(FieldImporter::Time64Ns)
            },
            DataType::Timestamp(unit, tz_opt) => match (tz_opt.as_deref(), base_target_oid) {
                (Some(tz), TIMESTAMPTZ_OID) => {
                    validate_arrow_timezone(tz)?;
                    Ok(FieldImporter::Timestamp {
                        unit: *unit,
                        is_tz: true,
                    })
                }
                (Some(tz), TIMESTAMP_OID) => Err(PostgresError::Data(format!(
                    "refusing timezone-losing import of tz-aware Arrow Timestamp({unit:?}, {tz:?}) into naive `timestamp` column `{field_name}`"
                ))),
                (None, TIMESTAMP_OID) => Ok(FieldImporter::Timestamp {
                    unit: *unit,
                    is_tz: false,
                }),
                (None, TIMESTAMPTZ_OID) => Err(PostgresError::Data(format!(
                    "refusing implicit-timezone import of naive Arrow Timestamp({unit:?}, None) into `timestamptz` column `{field_name}`"
                ))),
                _ => Err(incompatible_arrow_type(field_name, dt, target_oid)),
            },
            DataType::Interval(IntervalUnit::YearMonth) if base_target_oid == INTERVAL_OID => {
                Ok(FieldImporter::IntervalYearMonth)
            },
            DataType::Interval(IntervalUnit::DayTime) if base_target_oid == INTERVAL_OID => {
                Ok(FieldImporter::IntervalDayTime)
            },
            DataType::Interval(IntervalUnit::MonthDayNano) if base_target_oid == INTERVAL_OID => {
                Ok(FieldImporter::IntervalMonthDayNano)
            },
            DataType::Duration(unit) if base_target_oid == INTERVAL_OID => {
                Ok(FieldImporter::Duration(*unit))
            },
            DataType::Dictionary(key_dt, val_dt) => {
                if matches!(val_dt.as_ref(), DataType::Dictionary(_, _)) {
                    return Err(PostgresError::Data(format!(
                        "nested Arrow Dictionary values are not supported on `{field_name}`"
                    )));
                }
                let key_type = dict_key_type_for_dt(key_dt.as_ref(), field_name)?;
                let val_imp = compile_ordinary_importer(
                    field_name,
                    val_dt.as_ref(),
                    target_oid,
                    base_target_oid,
                    target_typmod,
                    catalog,
                    depth,
                )?;
                Ok(FieldImporter::Dictionary {
                    key_type,
                    values: Box::new(val_imp),
                })
            },
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                match catalog.type_info(base_target_oid) {
                    Some(TypeInfo {
                        kind: TypeKind::Extension { name, .. },
                        ..
                    }) if name == "vector" => {
                        compile_vector_list_importer(field_name, dt, VectorKind::Vector, target_typmod)
                    }
                    Some(TypeInfo {
                        kind: TypeKind::Extension { name, .. },
                        ..
                    }) if name == "halfvec" => {
                        compile_vector_list_importer(field_name, dt, VectorKind::HalfVec, target_typmod)
                    }
                    Some(TypeInfo {
                        kind: TypeKind::Array { element_oid, .. },
                        ..
                    }) => {
                        if !crate::codecs::supports_binary_with_catalog(base_target_oid, catalog) {
                            return Err(PostgresError::Data(format!(
                                "PostgreSQL array target OID {target_oid} (base {base_target_oid}) does not support binary COPY encoding on `{field_name}`"
                            )));
                        }
                        let (offsets, item_field) = match dt {
                            DataType::List(f) => (ListOffsetsKind::I32, f.as_ref()),
                            DataType::LargeList(f) => (ListOffsetsKind::I64, f.as_ref()),
                            DataType::FixedSizeList(f, d) => {
                                if *d < 0 {
                                    return Err(PostgresError::Data(format!(
                                        "negative FixedSizeList length {d} on `{field_name}`"
                                    )));
                                }
                                (ListOffsetsKind::Fixed(*d), f.as_ref())
                            }
                            _ => unreachable!(),
                        };
                        let resolved_elem_oid = catalog.resolve_domain_base(*element_oid)?;
                        let elem_imp = compile_field_importer(
                            item_field,
                            *element_oid,
                            target_typmod,
                            catalog,
                            depth + 1,
                        )?;
                        Ok(FieldImporter::OrdinaryArray {
                            offsets,
                            element_oid: resolved_elem_oid,
                            element: Box::new(elem_imp),
                        })
                    }
                    _ => Err(incompatible_arrow_type(field_name, dt, target_oid)),
                }
            },
            DataType::Struct(arrow_children) => match catalog.type_info(base_target_oid) {
                Some(TypeInfo {
                    kind: TypeKind::Composite { fields },
                    ..
                }) if !fields.is_empty() => {
                    if !crate::codecs::supports_binary_with_catalog(base_target_oid, catalog) {
                        return Err(PostgresError::Data(format!(
                            "PostgreSQL composite target OID {target_oid} (base {base_target_oid}) does not support binary COPY encoding on `{field_name}`"
                        )));
                    }
                    if arrow_children.len() != fields.len() {
                        return Err(PostgresError::Data(format!(
                            "Arrow Struct `{field_name}` attribute count {} does not match composite attribute count {}",
                            arrow_children.len(),
                            fields.len()
                        )));
                    }
                    let mut field_names = Vec::with_capacity(fields.len());
                    let mut field_oids = Vec::with_capacity(fields.len());
                    let mut child_importers = Vec::with_capacity(fields.len());
                    for (arrow_child, pg_attr) in arrow_children.iter().zip(fields.iter()) {
                        if arrow_child.name() != &pg_attr.name {
                            return Err(PostgresError::Data(format!(
                                "Arrow Struct `{field_name}` attribute `{}` does not match expected composite attribute `{}`",
                                arrow_child.name(),
                                pg_attr.name
                            )));
                        }
                        let imp = compile_field_importer(
                            arrow_child.as_ref(),
                            pg_attr.type_oid,
                            pg_attr.type_modifier,
                            catalog,
                            depth + 1,
                        )?;
                        field_names.push(pg_attr.name.clone());
                        field_oids.push(pg_attr.type_oid);
                        child_importers.push(imp);
                    }
                    Ok(FieldImporter::CompositeStruct {
                        type_oid: base_target_oid,
                        field_names,
                        field_oids,
                        children: child_importers,
                    })
                }
                _ => Err(incompatible_arrow_type(field_name, dt, target_oid)),
            },
            _ => Err(incompatible_arrow_type(field_name, dt, target_oid)),
        }
    )
}

fn incompatible_arrow_type(field_name: &str, dt: &DataType, target_oid: u32) -> PostgresError {
    PostgresError::Data(format!(
        "Arrow field `{field_name}` with DataType {dt:?} is incompatible with PostgreSQL COPY target OID {target_oid}"
    ))
}

fn convert_i128_to_int_target(val: i128, target: IntTargetKind) -> Result<PgValue, PostgresError> {
    match target {
        IntTargetKind::Int2 => i16::try_from(val)
            .map(PgValue::Int2)
            .map_err(|_| PostgresError::Data(format!("Arrow integer {val} overflows INT2"))),
        IntTargetKind::Int4 => i32::try_from(val)
            .map(PgValue::Int4)
            .map_err(|_| PostgresError::Data(format!("Arrow integer {val} overflows INT4"))),
        IntTargetKind::Int8 => i64::try_from(val)
            .map(PgValue::Int8)
            .map_err(|_| PostgresError::Data(format!("Arrow integer {val} overflows INT8"))),
        IntTargetKind::OidU32 => u32::try_from(val)
            .map(|u| PgValue::Int8(i64::from(u)))
            .map_err(|_| {
                PostgresError::Data(format!(
                    "Arrow integer {val} overflows unsigned 32-bit PostgreSQL OID/XID/CID"
                ))
            }),
        IntTargetKind::Numeric => {
            crate::codecs::numeric::decode_numeric_text(&val.to_string()).map(PgValue::Numeric)
        }
    }
}

fn convert_string_to_target(s: &str, target: StringTargetKind) -> Result<PgValue, PostgresError> {
    if s.as_bytes().contains(&0) {
        return Err(PostgresError::Data(
            "PostgreSQL text/json value in Arrow batch cannot contain embedded NUL byte"
                .to_string(),
        ));
    }
    match target {
        StringTargetKind::Text => Ok(PgValue::Text(s.to_owned())),
        StringTargetKind::Json => Ok(PgValue::Json(s.as_bytes().to_vec())),
        StringTargetKind::NumericDecimal => {
            crate::codecs::numeric::decode_numeric_text(s).map(PgValue::Numeric)
        }
    }
}

fn convert_timestamp_tick_to_pg(
    raw: i64,
    unit: TimeUnit,
    is_tz: bool,
) -> Result<PgValue, PostgresError> {
    let unix_us: i128 = match unit {
        TimeUnit::Second => i128::from(raw) * 1_000_000,
        TimeUnit::Millisecond => i128::from(raw) * 1_000,
        TimeUnit::Microsecond => i128::from(raw),
        TimeUnit::Nanosecond => {
            if raw % 1_000 != 0 {
                return Err(PostgresError::Data(format!(
                    "refusing sub-microsecond truncation for Arrow Timestamp(Nanosecond) value {raw} (ns % 1000 != 0)"
                )));
            }
            i128::from(raw / 1_000)
        }
    };
    let pg_us_128 = unix_us - UNIX_TO_PG_EPOCH_MICROS;
    let Ok(pg_us) = i64::try_from(pg_us_128) else {
        return Err(PostgresError::Data(format!(
            "Arrow Timestamp value {raw} ({unit:?}) overflows PostgreSQL i64 microsecond bounds"
        )));
    };
    if !(crate::codecs::temporal::PG_TS_MIN_FINITE..=crate::codecs::temporal::PG_TS_MAX_FINITE)
        .contains(&pg_us)
    {
        return Err(PostgresError::Data(format!(
            "Arrow Timestamp value {pg_us}us is outside PostgreSQL finite timestamp range"
        )));
    }
    if is_tz {
        Ok(PgValue::TimestampTz(pg_us))
    } else {
        Ok(PgValue::Timestamp(pg_us))
    }
}

fn downcast_err(expected: &str, actual: &DataType) -> PostgresError {
    PostgresError::Data(format!(
        "Arrow array downcast mismatch: expected {expected}, got {actual:?}"
    ))
}

fn downcast_array<'a, T: 'static>(
    array: &'a dyn Array,
    expected: &str,
) -> Result<&'a T, PostgresError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| downcast_err(expected, array.data_type()))
}

fn extract_list_offsets<O: TryInto<usize> + Copy>(
    offs: &[O],
    row_idx: usize,
    invalid_msg: &str,
    non_mono_msg: &str,
) -> Result<(usize, usize), PostgresError> {
    let start: usize = offs[row_idx]
        .try_into()
        .map_err(|_| PostgresError::Data(invalid_msg.to_string()))?;
    let end: usize = offs[row_idx + 1]
        .try_into()
        .map_err(|_| PostgresError::Data(invalid_msg.to_string()))?;
    if end < start {
        return Err(PostgresError::Data(non_mono_msg.to_string()));
    }
    Ok((start, end))
}

fn list_row_slice_bounds(
    array: &dyn Array,
    offsets: ListOffsetsKind,
    row_idx: usize,
) -> Result<(ArrayRef, usize, usize), PostgresError> {
    match offsets {
        ListOffsetsKind::I32 => {
            let list = downcast_array::<ListArray>(array, "ListArray")?;
            let (start, end) = extract_list_offsets(
                list.value_offsets(),
                row_idx,
                "negative Arrow ListArray offset",
                "non-monotonic Arrow ListArray offsets",
            )?;
            Ok((Arc::clone(list.values()), start, end))
        }
        ListOffsetsKind::I64 => {
            let list = downcast_array::<LargeListArray>(array, "LargeListArray")?;
            let (start, end) = extract_list_offsets(
                list.value_offsets(),
                row_idx,
                "invalid Arrow LargeListArray offset",
                "non-monotonic Arrow LargeListArray offsets",
            )?;
            Ok((Arc::clone(list.values()), start, end))
        }
        ListOffsetsKind::Fixed(dim) => {
            let list = downcast_array::<FixedSizeListArray>(array, "FixedSizeListArray")?;
            let step = usize::try_from(dim).map_err(|_| {
                PostgresError::Data("negative FixedSizeListArray dimension".to_string())
            })?;
            let start = row_idx.checked_mul(step).ok_or_else(|| {
                PostgresError::Data("FixedSizeListArray offset overflow".to_string())
            })?;
            let end = start.checked_add(step).ok_or_else(|| {
                PostgresError::Data("FixedSizeListArray offset overflow".to_string())
            })?;
            Ok((Arc::clone(list.values()), start, end))
        }
    }
}

fn float32_to_pg_target(f: f32, target: FloatTargetKind) -> PgValue {
    match target {
        FloatTargetKind::Float4 => PgValue::Float4(f),
        FloatTargetKind::Float8 => PgValue::Float8(f64::from(f)),
    }
}

fn checked_pg_date_days(pg_days_i64: i64, label: &str) -> Result<PgValue, PostgresError> {
    let pg_days = i32::try_from(pg_days_i64)
        .map_err(|_| PostgresError::Data(format!("{label} overflows PostgreSQL i32 days")))?;
    if !(crate::codecs::temporal::PG_DATE_MIN_FINITE..=crate::codecs::temporal::PG_DATE_MAX_FINITE)
        .contains(&pg_days)
    {
        return Err(PostgresError::Data(format!(
            "{label} -> {pg_days} is outside PostgreSQL finite date bounds"
        )));
    }
    Ok(PgValue::Date(pg_days))
}

fn validated_interval(months: i32, days: i32, micros: i64) -> Result<PgValue, PostgresError> {
    let (m, d, u) = validate_interval_parts(months, days, micros)?;
    Ok(PgValue::Interval {
        months: m,
        days: d,
        microseconds: u,
    })
}

fn decimal_str_to_pg_value(
    rendered: &str,
    precision: u8,
    scale: i8,
    label: &str,
) -> Result<PgValue, PostgresError> {
    let (negative, digit_str) = match rendered.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, rendered),
    };
    if digit_str != "0" && digit_str.len() > usize::from(precision) {
        return Err(PostgresError::Data(format!(
            "Arrow {label} value {rendered} exceeds declared precision {precision}"
        )));
    }
    let digits: Vec<u8> = digit_str
        .bytes()
        .map(|b| {
            if b.is_ascii_digit() {
                Ok(b - b'0')
            } else {
                Err(PostgresError::Data(format!(
                    "unexpected character in {label} rendering {rendered:?}"
                )))
            }
        })
        .collect::<Result<Vec<u8>, PostgresError>>()?;
    let num = crate::codecs::numeric::numeric_from_decimal_parts(
        negative,
        &digits,
        -i32::from(scale),
        false,
    )?;
    Ok(PgValue::Numeric(num))
}

macro_rules! match_arrow_importers {
    (
        $importer:expr, $array:expr, $row_idx:expr;
        ints: [ $( $int_var:ident => $int_arr:ty ),* $(,)? ];
        strings: [ $( $str_var:ident => $str_arr:ty ),* $(,)? ];
        bytes: [ $( $bin_pat:pat => $bin_arr:ty ),* $(,)? ];
        rest: { $( $rest_pat:pat $(if $guard:expr)? => $rest_body:expr ),* $(,)? }
    ) => {
        match $importer {
            $(
                FieldImporter::$int_var(target) => {
                    let arr = downcast_array::<$int_arr>($array, stringify!($int_arr))?;
                    convert_i128_to_int_target(i128::from(arr.value($row_idx)), *target)
                }
            )*
            $(
                FieldImporter::$str_var(target) => {
                    let arr = downcast_array::<$str_arr>($array, stringify!($str_arr))?;
                    convert_string_to_target(arr.value($row_idx), *target)
                }
            )*
            $(
                $bin_pat => {
                    let arr = downcast_array::<$bin_arr>($array, stringify!($bin_arr))?;
                    Ok(PgValue::Bytes(arr.value($row_idx).to_vec()))
                }
            )*
            $(
                $rest_pat $(if $guard)? => $rest_body,
            )*
        }
    };
}

macro_rules! extract_vector_slice {
    ($values_arr:expr, $arr_ty:ty, $start:expr, $end:expr, $kind:expr, $out:expr, |$raw:ident| $to_f32:expr) => {{
        let typed = downcast_array::<$arr_ty>($values_arr.as_ref(), stringify!($arr_ty))?;
        for idx in $start..$end {
            if typed.is_null(idx) {
                return Err(PostgresError::Data(
                    "pgvector elements cannot be NULL".to_string(),
                ));
            }
            let $raw = typed.value(idx);
            let f: f32 = $to_f32;
            $out.push(validate_vector_elem(f, $kind)?);
        }
    }};
}

macro_rules! dispatch_dict_key {
    ($key_type:expr, $array:expr, $row_idx:expr; [ $( $var:ident => $key_ty:ty ),* $(,)? ]) => {
        match $key_type {
            $( DictKeyType::$var => extract_dict_entry::<$key_ty>($array, $row_idx)?, )*
        }
    };
}

fn extract_field_value(
    importer: &FieldImporter,
    array: &dyn Array,
    row_idx: usize,
) -> Result<PgValue, PostgresError> {
    if array.is_null(row_idx) {
        return Ok(PgValue::Null);
    }

    match_arrow_importers!(
        importer, array, row_idx;
        ints: [
            Int8 => Int8Array,
            Int16 => Int16Array,
            Int32 => Int32Array,
            Int64 => Int64Array,
            UInt8 => UInt8Array,
            UInt16 => UInt16Array,
            UInt32 => UInt32Array,
            UInt64 => UInt64Array,
        ];
        strings: [
            Utf8 => StringArray,
            LargeUtf8 => LargeStringArray,
            Utf8View => StringViewArray,
        ];
        bytes: [
            FieldImporter::Binary(BinaryTargetKind::Bytea) => BinaryArray,
            FieldImporter::LargeBinary => LargeBinaryArray,
            FieldImporter::BinaryView => BinaryViewArray,
        ];
        rest: {
            FieldImporter::Null => Ok(PgValue::Null),
            FieldImporter::Bool => {
                let arr = downcast_array::<BooleanArray>(array, "BooleanArray")?;
                Ok(PgValue::Bool(arr.value(row_idx)))
            },
            FieldImporter::Float16(target) => {
                let arr = downcast_array::<Float16Array>(array, "Float16Array")?;
                Ok(float32_to_pg_target(arr.value(row_idx).to_f32(), *target))
            },
            FieldImporter::Float32(target) => {
                let arr = downcast_array::<Float32Array>(array, "Float32Array")?;
                Ok(float32_to_pg_target(arr.value(row_idx), *target))
            },
            FieldImporter::Float64(target) => {
                let v = downcast_array::<Float64Array>(array, "Float64Array")?.value(row_idx);
                match target {
                    FloatTargetKind::Float8 => Ok(PgValue::Float8(v)),
                    FloatTargetKind::Float4 => {
                        let narrowed = v as f32;
                        if v.is_finite() && !narrowed.is_finite() {
                            return Err(PostgresError::Data(format!(
                                "Arrow Float64 value {v} overflows PostgreSQL FLOAT4"
                            )));
                        }
                        Ok(PgValue::Float4(narrowed))
                    }
                }
            },
            FieldImporter::Binary(BinaryTargetKind::Uuid) => unreachable!("UUID uses FixedSizeBinary"),
            FieldImporter::FixedSizeBinary { size, target } => {
                let arr = downcast_array::<FixedSizeBinaryArray>(array, "FixedSizeBinaryArray")?;
                let slice = arr.value(row_idx);
                if slice.len() != (*size as usize) {
                    return Err(PostgresError::Data(format!(
                        "FixedSizeBinary slice length {} does not match size {size}",
                        slice.len()
                    )));
                }
                match target {
                    BinaryTargetKind::Uuid => {
                        let bytes: [u8; 16] = slice.try_into().map_err(|_| {
                            PostgresError::Data("UUID FixedSizeBinary must be 16 bytes".to_string())
                        })?;
                        Ok(PgValue::Uuid(bytes))
                    }
                    BinaryTargetKind::Bytea => Ok(PgValue::Bytes(slice.to_vec())),
                }
            },
            FieldImporter::Decimal128 { precision, scale } => {
                let raw = downcast_array::<Decimal128Array>(array, "Decimal128Array")?.value(row_idx);
                decimal_str_to_pg_value(&raw.to_string(), *precision, *scale, "Decimal128")
            },
            FieldImporter::Decimal256 { precision, scale } => {
                let raw = downcast_array::<Decimal256Array>(array, "Decimal256Array")?.value(row_idx);
                decimal_str_to_pg_value(&raw.to_string(), *precision, *scale, "Decimal256")
            },
            FieldImporter::Date32 => {
                let unix_days =
                    i64::from(downcast_array::<Date32Array>(array, "Date32Array")?.value(row_idx));
                checked_pg_date_days(
                    unix_days - UNIX_TO_PG_EPOCH_DAYS,
                    &format!("Arrow Date32 {unix_days}"),
                )
            },
            FieldImporter::Date64 => {
                let unix_ms = downcast_array::<Date64Array>(array, "Date64Array")?.value(row_idx);
                if unix_ms % MILLIS_PER_DAY != 0 {
                    return Err(PostgresError::Data(format!(
                        "Arrow Date64 value {unix_ms}ms is not an exact multiple of 86_400_000ms"
                    )));
                }
                checked_pg_date_days(
                    (unix_ms / MILLIS_PER_DAY) - UNIX_TO_PG_EPOCH_DAYS,
                    &format!("Arrow Date64 {unix_ms}ms"),
                )
            },
            FieldImporter::Time32Sec => {
                let raw = downcast_array::<Time32SecondArray>(array, "Time32SecondArray")?
                    .value(row_idx);
                validate_time_micros(i64::from(raw) * 1_000_000).map(PgValue::Time)
            },
            FieldImporter::Time32Ms => {
                let raw = downcast_array::<Time32MillisecondArray>(array, "Time32MillisecondArray")?
                    .value(row_idx);
                validate_time_micros(i64::from(raw) * 1_000).map(PgValue::Time)
            },
            FieldImporter::Time64Us => {
                let micros = if let Some(arr) = array.as_any().downcast_ref::<Time64MicrosecondArray>() {
                    arr.value(row_idx)
                } else if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                    arr.value(row_idx)
                } else {
                    return Err(downcast_err(
                        "Time64MicrosecondArray or Int64Array",
                        array.data_type(),
                    ));
                };
                validate_time_micros(micros).map(PgValue::Time)
            },
            FieldImporter::Time64Ns => {
                let ns = downcast_array::<Time64NanosecondArray>(array, "Time64NanosecondArray")?
                    .value(row_idx);
                if ns % 1_000 != 0 {
                    return Err(PostgresError::Data(format!(
                        "refusing sub-microsecond truncation for Arrow Time64(Nanosecond) value {ns} (ns % 1000 != 0)"
                    )));
                }
                validate_time_micros(ns / 1_000).map(PgValue::Time)
            },
            FieldImporter::Timestamp { unit, is_tz } => {
                let raw = match unit {
                    TimeUnit::Second => {
                        downcast_array::<TimestampSecondArray>(array, "TimestampSecondArray")?
                            .value(row_idx)
                    }
                    TimeUnit::Millisecond => downcast_array::<TimestampMillisecondArray>(
                        array,
                        "TimestampMillisecondArray",
                    )?
                    .value(row_idx),
                    TimeUnit::Microsecond => downcast_array::<TimestampMicrosecondArray>(
                        array,
                        "TimestampMicrosecondArray",
                    )?
                    .value(row_idx),
                    TimeUnit::Nanosecond => downcast_array::<TimestampNanosecondArray>(
                        array,
                        "TimestampNanosecondArray",
                    )?
                    .value(row_idx),
                };
                convert_timestamp_tick_to_pg(raw, *unit, *is_tz)
            },
            FieldImporter::IntervalYearMonth => {
                let months =
                    downcast_array::<IntervalYearMonthArray>(array, "IntervalYearMonthArray")?
                        .value(row_idx);
                validated_interval(months, 0, 0)
            },
            FieldImporter::IntervalDayTime => {
                let dt_val =
                    downcast_array::<IntervalDayTimeArray>(array, "IntervalDayTimeArray")?
                        .value(row_idx);
                validated_interval(0, dt_val.days, i64::from(dt_val.milliseconds) * 1_000)
            },
            FieldImporter::IntervalMonthDayNano => {
                let v = downcast_array::<IntervalMonthDayNanoArray>(
                    array,
                    "IntervalMonthDayNanoArray",
                )?
                .value(row_idx);
                if v.nanoseconds % 1_000 != 0 {
                    return Err(PostgresError::Data(format!(
                        "refusing sub-microsecond truncation for Arrow Interval(MonthDayNano) nanoseconds {}",
                        v.nanoseconds
                    )));
                }
                validated_interval(v.months, v.days, v.nanoseconds / 1_000)
            },
            FieldImporter::Duration(unit) => {
                let micros: i64 = match unit {
                    TimeUnit::Second => {
                        let raw =
                            downcast_array::<DurationSecondArray>(array, "DurationSecondArray")?
                                .value(row_idx);
                        raw.checked_mul(1_000_000).ok_or_else(|| {
                            PostgresError::Data(format!(
                                "Arrow Duration(Second) {raw} overflows i64us"
                            ))
                        })?
                    }
                    TimeUnit::Millisecond => {
                        let raw = downcast_array::<DurationMillisecondArray>(
                            array,
                            "DurationMillisecondArray",
                        )?
                        .value(row_idx);
                        raw.checked_mul(1_000).ok_or_else(|| {
                            PostgresError::Data(format!(
                                "Arrow Duration(Millisecond) {raw} overflows i64us"
                            ))
                        })?
                    }
                    TimeUnit::Microsecond => downcast_array::<DurationMicrosecondArray>(
                        array,
                        "DurationMicrosecondArray",
                    )?
                    .value(row_idx),
                    TimeUnit::Nanosecond => {
                        let ns = downcast_array::<DurationNanosecondArray>(
                            array,
                            "DurationNanosecondArray",
                        )?
                        .value(row_idx);
                        if ns % 1_000 != 0 {
                            return Err(PostgresError::Data(format!(
                                "refusing sub-microsecond truncation for Arrow Duration(Nanosecond) {ns}"
                            )));
                        }
                        ns / 1_000
                    }
                };
                validated_interval(0, 0, micros)
            },
            FieldImporter::PgTemporalStruct(target) => {
                let st = downcast_array::<StructArray>(array, "StructArray (temporal)")?;
                let kind_arr = downcast_array::<Int8Array>(
                    st.column(0).as_ref(),
                    "Int8Array (temporal.kind)",
                )?;
                if kind_arr.is_null(row_idx) {
                    return Err(PostgresError::Data(
                        "non-null PostgreSQL Arrow temporal struct has NULL `kind`".to_string(),
                    ));
                }
                let kind = kind_arr.value(row_idx);
                if kind == 2 {
                    return Err(PostgresError::Data(
                        "unresolved PostgreSQL temporal `wire_text` (kind = 2) cannot be imported via binary COPY"
                            .to_string(),
                    ));
                }
                match target {
                    TemporalStructTarget::Date => match kind {
                        -1 => Ok(PgValue::Date(PG_DATE_NEG_INF)),
                        1 => Ok(PgValue::Date(PG_DATE_POS_INF)),
                        0 => {
                            let val_arr = downcast_array::<Int32Array>(
                                st.column(1).as_ref(),
                                "Int32Array (date.value)",
                            )?;
                            if val_arr.is_null(row_idx) {
                                return Err(PostgresError::Data(
                                    "PostgreSQL Arrow `pg-date` kind=0 has NULL `value`"
                                        .to_string(),
                                ));
                            }
                            let days = val_arr.value(row_idx);
                            if !(crate::codecs::temporal::PG_DATE_MIN_FINITE
                                ..=crate::codecs::temporal::PG_DATE_MAX_FINITE)
                                .contains(&days)
                            {
                                return Err(PostgresError::Data(format!(
                                    "PostgreSQL Arrow `pg-date` finite value {days} is out of range"
                                )));
                            }
                            Ok(PgValue::Date(days))
                        }
                        other => Err(PostgresError::Data(format!(
                            "invalid PostgreSQL Arrow temporal `kind` {other}"
                        ))),
                    },
                    TemporalStructTarget::Timestamp | TemporalStructTarget::TimestampTz => {
                        let wrap_ts = |us: i64| match target {
                            TemporalStructTarget::Timestamp => PgValue::Timestamp(us),
                            TemporalStructTarget::TimestampTz => PgValue::TimestampTz(us),
                            TemporalStructTarget::Date => unreachable!(),
                        };
                        match kind {
                            -1 => Ok(wrap_ts(PG_TS_NEG_INF)),
                            1 => Ok(wrap_ts(PG_TS_POS_INF)),
                            0 => {
                                let val_arr = downcast_array::<Int64Array>(
                                    st.column(1).as_ref(),
                                    "Int64Array (timestamp.value)",
                                )?;
                                if val_arr.is_null(row_idx) {
                                    return Err(PostgresError::Data(
                                        "PostgreSQL Arrow timestamp kind=0 has NULL `value`"
                                            .to_string(),
                                    ));
                                }
                                let us = val_arr.value(row_idx);
                                if !(crate::codecs::temporal::PG_TS_MIN_FINITE
                                    ..=crate::codecs::temporal::PG_TS_MAX_FINITE)
                                    .contains(&us)
                                {
                                    return Err(PostgresError::Data(format!(
                                        "PostgreSQL Arrow timestamp finite value {us} is out of range"
                                    )));
                                }
                                Ok(wrap_ts(us))
                            }
                            other => Err(PostgresError::Data(format!(
                                "invalid PostgreSQL Arrow temporal `kind` {other}"
                            ))),
                        }
                    }
                }
            },
            FieldImporter::PgIntervalStruct => {
                let st = downcast_array::<StructArray>(array, "StructArray (pg-interval)")?;
                let m_arr =
                    downcast_array::<Int32Array>(st.column(0).as_ref(), "Int32Array (months)")?;
                let d_arr =
                    downcast_array::<Int32Array>(st.column(1).as_ref(), "Int32Array (days)")?;
                let u_arr = downcast_array::<Int64Array>(
                    st.column(2).as_ref(),
                    "Int64Array (microseconds)",
                )?;
                if m_arr.is_null(row_idx) || d_arr.is_null(row_idx) || u_arr.is_null(row_idx) {
                    return Err(PostgresError::Data(
                        "non-null PostgreSQL Arrow `pg-interval` struct contains NULL component"
                            .to_string(),
                    ));
                }
                validated_interval(
                    m_arr.value(row_idx),
                    d_arr.value(row_idx),
                    u_arr.value(row_idx),
                )
            },
            FieldImporter::PgBitStringStruct { fixed_bit_typmod } => {
                let st = downcast_array::<StructArray>(array, "StructArray (pg-bits)")?;
                let b_arr =
                    downcast_array::<BinaryArray>(st.column(0).as_ref(), "BinaryArray (bytes)")?;
                let len_arr = downcast_array::<Int32Array>(
                    st.column(1).as_ref(),
                    "Int32Array (bit_length)",
                )?;
                if b_arr.is_null(row_idx) || len_arr.is_null(row_idx) {
                    return Err(PostgresError::Data(
                        "non-null PostgreSQL Arrow `pg-bits` struct contains NULL component"
                            .to_string(),
                    ));
                }
                let bit_length = len_arr.value(row_idx);
                if bit_length < 0 {
                    return Err(PostgresError::Data(format!(
                        "negative `pg-bits` bit_length {bit_length}"
                    )));
                }
                if let Some(exp_bits) = fixed_bit_typmod
                    && bit_length != *exp_bits
                {
                    return Err(PostgresError::Data(format!(
                        "BIT({exp_bits}) target requires exact bit_length {exp_bits}, got {bit_length}"
                    )));
                }
                let raw_bytes = b_arr.value(row_idx);
                let expected_bytes = (bit_length as usize).div_ceil(8);
                if raw_bytes.len() != expected_bytes {
                    return Err(PostgresError::Data(format!(
                        "`pg-bits` bytes length {} does not match bit_length {bit_length} ({expected_bytes} bytes)",
                        raw_bytes.len()
                    )));
                }
                let rem = (bit_length as usize) % 8;
                if rem != 0 {
                    let mask = (1u8 << (8 - rem)) - 1;
                    if let Some(&last) = raw_bytes.last()
                        && last & mask != 0
                    {
                        return Err(PostgresError::Data(
                            "`pg-bits` payload contains non-zero trailing padding bits".to_string(),
                        ));
                    }
                }
                Ok(PgValue::BitString {
                    data: raw_bytes.to_vec(),
                    bit_length,
                })
            },
            FieldImporter::PgVector {
                offsets,
                kind,
                expected_dim,
            } => {
                let (values_arr, start, end) = list_row_slice_bounds(array, *offsets, row_idx)?;
                let len = end - start;
                if len == 0 || len > MAX_PGVECTOR_ARROW_DIM as usize {
                    return Err(PostgresError::Data(format!(
                        "PostgreSQL {kind:?} dimension {len} must be within 1..={MAX_PGVECTOR_ARROW_DIM}"
                    )));
                }
                if let Some(exp) = expected_dim
                    && len != *exp
                {
                    return Err(PostgresError::Data(format!(
                        "PostgreSQL {kind:?} dimension {len} does not match column typmod {exp}"
                    )));
                }
                let mut out_vals = Vec::with_capacity(len);
                match values_arr.data_type() {
                    DataType::Float32 => {
                        extract_vector_slice!(values_arr, Float32Array, start, end, *kind, out_vals, |v| v);
                    }
                    DataType::Float16 => {
                        extract_vector_slice!(values_arr, Float16Array, start, end, *kind, out_vals, |v| v.to_f32());
                    }
                    DataType::Float64 => {
                        extract_vector_slice!(values_arr, Float64Array, start, end, *kind, out_vals, |v64| {
                            let narrowed = v64 as f32;
                            if !v64.is_finite() || !narrowed.is_finite() {
                                return Err(PostgresError::Data(format!(
                                    "non-finite or overflowing Float64 element {v64} in pgvector"
                                )));
                            }
                            narrowed
                        });
                    }
                    other => {
                        return Err(PostgresError::Data(format!(
                            "unsupported vector element DataType {other:?}"
                        )));
                    }
                }
                Ok(PgValue::Vector {
                    values: out_vals,
                    kind: *kind,
                })
            },
            FieldImporter::PgLosslessArray {
                element_oid,
                element,
            } => {
                let st = downcast_array::<StructArray>(array, "StructArray (pg-array)")?;
                if st.column(0).is_null(row_idx)
                    || st.column(1).is_null(row_idx)
                    || st.column(2).is_null(row_idx)
                {
                    return Err(PostgresError::Data(
                        "non-null PostgreSQL Arrow `pg-array` struct contains NULL child list"
                            .to_string(),
                    ));
                }
                let (dim_vals, d_start, d_end) =
                    list_row_slice_bounds(st.column(0).as_ref(), ListOffsetsKind::I32, row_idx)?;
                let (lb_vals, lb_start, lb_end) =
                    list_row_slice_bounds(st.column(1).as_ref(), ListOffsetsKind::I32, row_idx)?;
                let (elem_vals, v_start, v_end) =
                    list_row_slice_bounds(st.column(2).as_ref(), ListOffsetsKind::I32, row_idx)?;

                let dim_i32 =
                    downcast_array::<Int32Array>(dim_vals.as_ref(), "Int32Array (dimensions)")?;
                let lb_i32 =
                    downcast_array::<Int32Array>(lb_vals.as_ref(), "Int32Array (lower_bounds)")?;

                let ndim = d_end - d_start;
                if ndim != lb_end - lb_start {
                    return Err(PostgresError::Data(format!(
                        "`pg-array` dimensions length {ndim} does not match lower_bounds length {}",
                        lb_end - lb_start
                    )));
                }
                if ndim > MAX_ARRAY_DIMS {
                    return Err(PostgresError::Data(format!(
                        "`pg-array` ndim {ndim} exceeds MAX_ARRAY_DIMS ({MAX_ARRAY_DIMS})"
                    )));
                }

                let mut dimensions = Vec::with_capacity(ndim);
                let mut lower_bounds = Vec::with_capacity(ndim);
                let mut expected_total: usize = if ndim == 0 { 0 } else { 1 };
                for i in 0..ndim {
                    if dim_i32.is_null(d_start + i) || lb_i32.is_null(lb_start + i) {
                        return Err(PostgresError::Data(
                            "`pg-array` dimensions and lower_bounds cannot contain NULL"
                                .to_string(),
                        ));
                    }
                    let d = dim_i32.value(d_start + i);
                    let lb = lb_i32.value(lb_start + i);
                    if d <= 0 {
                        return Err(PostgresError::Data(format!(
                            "`pg-array` non-empty dimension must be >= 1, got {d}"
                        )));
                    }
                    if lb.checked_add(d - 1).is_none() {
                        return Err(PostgresError::Data(format!(
                            "`pg-array` lower_bound {lb} + dim {d} overflows i32"
                        )));
                    }
                    expected_total = expected_total
                        .checked_mul(d as usize)
                        .ok_or_else(|| PostgresError::Data("`pg-array` cardinality overflow".into()))?;
                    dimensions.push(d);
                    lower_bounds.push(lb);
                }

                let actual_total = v_end - v_start;
                if actual_total != expected_total {
                    return Err(PostgresError::Data(format!(
                        "`pg-array` values length {actual_total} does not match dimensions product {expected_total}"
                    )));
                }

                let mut values = Vec::with_capacity(actual_total);
                for idx in v_start..v_end {
                    values.push(extract_field_value(element, elem_vals.as_ref(), idx)?);
                }
                Ok(PgValue::Array {
                    values,
                    dimensions,
                    lower_bounds,
                    element_oid: *element_oid,
                })
            },
            FieldImporter::OrdinaryArray {
                offsets,
                element_oid,
                element,
            } => {
                let (elem_vals, start, end) = list_row_slice_bounds(array, *offsets, row_idx)?;
                let len = end - start;
                if len == 0 {
                    return Ok(PgValue::Array {
                        values: Vec::new(),
                        dimensions: Vec::new(),
                        lower_bounds: Vec::new(),
                        element_oid: *element_oid,
                    });
                }
                let len_i32 = i32::try_from(len).map_err(|_| {
                    PostgresError::Data(format!("Arrow list length {len} overflows PostgreSQL i32"))
                })?;
                let mut values = Vec::with_capacity(len);
                for idx in start..end {
                    values.push(extract_field_value(element, elem_vals.as_ref(), idx)?);
                }
                Ok(PgValue::Array {
                    values,
                    dimensions: vec![len_i32],
                    lower_bounds: vec![1],
                    element_oid: *element_oid,
                })
            },
            FieldImporter::CompositeStruct {
                type_oid,
                field_names,
                field_oids,
                children,
            } => {
                let st = downcast_array::<StructArray>(array, "StructArray (composite)")?;
                if st.num_columns() != children.len() {
                    return Err(PostgresError::Data(format!(
                        "composite StructArray column count {} does not match expected {}",
                        st.num_columns(),
                        children.len()
                    )));
                }
                let mut values = Vec::with_capacity(children.len());
                for (idx, child_imp) in children.iter().enumerate() {
                    values.push(extract_field_value(
                        child_imp,
                        st.column(idx).as_ref(),
                        row_idx,
                    )?);
                }
                Ok(PgValue::Composite {
                    type_oid: *type_oid,
                    field_names: field_names.clone(),
                    field_oids: field_oids.clone(),
                    values,
                })
            },
            FieldImporter::Dictionary { key_type, values } => {
                let (dict_values, val_idx) = dispatch_dict_key!(
                    key_type, array, row_idx;
                    [
                        Int8 => arrow_types::Int8Type,
                        Int16 => arrow_types::Int16Type,
                        Int32 => arrow_types::Int32Type,
                        Int64 => arrow_types::Int64Type,
                        UInt8 => arrow_types::UInt8Type,
                        UInt16 => arrow_types::UInt16Type,
                        UInt32 => arrow_types::UInt32Type,
                        UInt64 => arrow_types::UInt64Type,
                    ]
                );
                extract_field_value(values, dict_values.as_ref(), val_idx)
            },
        }
    )
}

fn validate_vector_elem(val: f32, kind: VectorKind) -> Result<f32, PostgresError> {
    if !val.is_finite() {
        return Err(PostgresError::Data(format!(
            "PostgreSQL {kind:?} does not accept non-finite float value {val}"
        )));
    }
    if kind == VectorKind::HalfVec {
        let h = half::f16::from_f32(val);
        if !h.is_finite() {
            return Err(PostgresError::Data(format!(
                "Float32 value {val} overflows IEEE-754 binary16 `halfvec` range"
            )));
        }
        return Ok(h.to_f32());
    }
    Ok(val)
}

fn extract_dict_entry<K: arrow_types::ArrowDictionaryKeyType>(
    array: &dyn Array,
    row_idx: usize,
) -> Result<(ArrayRef, usize), PostgresError> {
    let dict = downcast_array::<DictionaryArray<K>>(array, "DictionaryArray")?;
    let val_idx = dict.keys().value(row_idx);
    use arrow_buffer::ArrowNativeType;
    let idx = val_idx.to_usize().ok_or_else(|| {
        PostgresError::Data("invalid negative or overflowing Arrow dictionary key".to_string())
    })?;
    let vals = dict.values();
    if idx >= vals.len() {
        return Err(PostgresError::Data(format!(
            "Arrow dictionary key index {idx} out of bounds for dictionary of length {}",
            vals.len()
        )));
    }
    Ok((Arc::clone(vals), idx))
}
