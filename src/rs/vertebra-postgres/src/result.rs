//! Owned Rust result and lossless PostgreSQL value models (`QueryResult`, `Column`, `PgValue`).

use crate::errors::PostgresError;
pub use crate::session::TransactionStatus;

/// Maximum valid PostgreSQL `TIME` value (`24:00:00.000000` in microseconds).
pub const PG_TIME_MAX_MICROS: i64 = 86_400_000_000;
/// Positive infinity sentinel for PostgreSQL `DATE` (`i32::MAX`).
pub const PG_DATE_POS_INF: i32 = i32::MAX;
/// Negative infinity sentinel for PostgreSQL `DATE` (`i32::MIN`).
pub const PG_DATE_NEG_INF: i32 = i32::MIN;
/// Positive infinity sentinel for PostgreSQL `TIMESTAMP` and `TIMESTAMPTZ` (`i64::MAX`).
pub const PG_TS_POS_INF: i64 = i64::MAX;
/// Negative infinity sentinel for PostgreSQL `TIMESTAMP` and `TIMESTAMPTZ` (`i64::MIN`).
pub const PG_TS_NEG_INF: i64 = i64::MIN;
/// Positive infinity sentinel triple `(months, days, microseconds)` for PostgreSQL 18 `INTERVAL`.
pub const PG_INTERVAL_POS_INF: (i32, i32, i64) = (i32::MAX, i32::MAX, i64::MAX);
/// Negative infinity sentinel triple `(months, days, microseconds)` for PostgreSQL 18 `INTERVAL`.
pub const PG_INTERVAL_NEG_INF: (i32, i32, i64) = (i32::MIN, i32::MIN, i64::MIN);

/// PostgreSQL wire format code (`0` = text, `1` = binary).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i16)]
pub enum Format {
    /// Text wire representation (`0`).
    Text = 0,
    /// Binary wire representation (`1`).
    Binary = 1,
}

impl Format {
    /// Parse a wire format code (`0` or `1`).
    pub fn from_code(code: i16) -> Result<Self, PostgresError> {
        match code {
            0 => Ok(Self::Text),
            1 => Ok(Self::Binary),
            other => Err(PostgresError::Protocol(format!(
                "invalid PostgreSQL column format code {other}"
            ))),
        }
    }

    /// Return the wire integer code (`0` or `1`).
    pub const fn code(self) -> i16 {
        self as i16
    }
}

/// Owned column descriptor from a PostgreSQL `RowDescription` frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    /// Column name (may be empty or duplicated across a projection).
    pub name: String,
    /// Positive PostgreSQL type OID (`>= 1`).
    pub type_oid: u32,
    /// Negotiated column wire format.
    pub format: Format,
    /// PostgreSQL `atttypmod` value (`-1` when unspecified).
    pub type_modifier: i32,
    /// Originating table OID (`0` if computed expression).
    pub table_oid: u32,
    /// Originating table attribute number (`0` if computed expression).
    pub attribute_number: i16,
}

impl Column {
    /// Construct and validate a [`Column`] descriptor.
    pub fn try_new(
        name: impl Into<String>,
        type_oid: u32,
        format: Format,
        type_modifier: i32,
        table_oid: u32,
        attribute_number: i16,
    ) -> Result<Self, PostgresError> {
        if type_oid == 0 {
            return Err(PostgresError::Protocol(
                "RowDescription column type_oid must be positive (>= 1)".to_string(),
            ));
        }
        Ok(Self {
            name: name.into(),
            type_oid,
            format,
            type_modifier,
            table_oid,
            attribute_number,
        })
    }
}

/// Owned query result payload independent of any socket buffer or GIL state.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryResult {
    /// Ordered column metadata.
    pub columns: Vec<Column>,
    /// Owned decoded rows (`row.len() == columns.len()` for every row).
    pub rows: Vec<Vec<PgValue>>,
    /// Affected row count parsed from `CommandComplete` (`0` for DDL/empty commands).
    pub affected_rows: u64,
    /// Exact server `CommandComplete` tag (`""` for `EmptyQueryResponse`).
    pub command_tag: String,
    /// Final `ReadyForQuery` transaction state (`Idle`, `InTransaction`, or `Failed`).
    pub transaction_status: TransactionStatus,
}

impl QueryResult {
    /// Validate row widths against `columns.len()` and construct an owned [`QueryResult`].
    pub fn try_new(
        columns: Vec<Column>,
        rows: Vec<Vec<PgValue>>,
        affected_rows: u64,
        command_tag: impl Into<String>,
        transaction_status: TransactionStatus,
    ) -> Result<Self, PostgresError> {
        let width = columns.len();
        for (idx, row) in rows.iter().enumerate() {
            if row.len() != width {
                return Err(PostgresError::Protocol(format!(
                    "row {idx} width {} does not match column count {width}",
                    row.len()
                )));
            }
        }
        Ok(Self {
            columns,
            rows,
            affected_rows,
            command_tag: command_tag.into(),
            transaction_status,
        })
    }
}

/// Arbitrary-precision PostgreSQL `NUMERIC` value in base-10000 digit groups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgNumeric {
    /// Finite decimal value with base-10000 `digits` (`0..=9999`), base-10000 `weight`, and display scale `dscale`.
    Finite {
        negative: bool,
        weight: i16,
        dscale: u16,
        digits: Vec<i16>,
    },
    /// Quiet `NaN`.
    NaN,
    /// Positive `Infinity`.
    PosInf,
    /// Negative `-Infinity`.
    NegInf,
}

impl PgNumeric {
    /// Construct a [`PgNumeric`] from base-10 digits and a base-10 exponent (matching Python `Decimal.as_tuple()`).
    pub fn from_decimal_parts(
        negative: bool,
        base10_digits: &[u8],
        exponent: i32,
        signaling_nan: bool,
    ) -> Result<Self, PostgresError> {
        crate::codecs::numeric::numeric_from_decimal_parts(
            negative,
            base10_digits,
            exponent,
            signaling_nan,
        )
    }

    /// Render the exact canonical decimal string preserving `dscale` trailing zeros.
    pub fn to_canonical_string(&self) -> String {
        crate::codecs::numeric::encode_numeric_text(self)
    }
}

/// Discriminant for PostgreSQL temporal types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemporalKind {
    /// PostgreSQL `DATE` (`OID 1082`).
    Date,
    /// PostgreSQL `TIME` (`OID 1083`).
    Time,
    /// PostgreSQL `TIMESTAMP WITHOUT TIME ZONE` (`OID 1114`).
    Timestamp,
    /// PostgreSQL `TIMESTAMP WITH TIME ZONE` (`OID 1184`).
    TimestampTz,
}

/// Discriminant for `pgvector` vector types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorKind {
    /// 32-bit float `vector`.
    Vector,
    /// 16-bit IEEE-754 float `halfvec`.
    HalfVec,
}

/// Owned, lossless PostgreSQL value representation.
#[derive(Clone, Debug, PartialEq)]
pub enum PgValue {
    /// SQL `NULL`.
    Null,
    /// `BOOL` (`OID 16`).
    Bool(bool),
    /// `INT2` (`OID 21`).
    Int2(i16),
    /// `INT4` (`OID 23`).
    Int4(i32),
    /// `INT8` (`OID 20`) or widened unsigned 32-bit `OID` (`OID 26`).
    Int8(i64),
    /// `FLOAT4` (`OID 700`).
    Float4(f32),
    /// `FLOAT8` (`OID 701`).
    Float8(f64),
    /// `TEXT`, `VARCHAR`, `BPCHAR`, `NAME`, or unknown-OID text fallback.
    Text(String),
    /// `BYTEA` (`OID 17`).
    Bytes(Vec<u8>),
    /// `UUID` (`OID 2950`).
    Uuid([u8; 16]),
    /// `NUMERIC` (`OID 1700`).
    Numeric(PgNumeric),
    /// `DATE` (`OID 1082`) days since `2000-01-01`.
    Date(i32),
    /// `TIME` (`OID 1083`) microseconds since `00:00:00` (`0..=86_400_000_000`).
    Time(i64),
    /// `TIMESTAMP` (`OID 1114`) microseconds since `2000-01-01 00:00:00`.
    Timestamp(i64),
    /// `TIMESTAMPTZ` (`OID 1184`) microseconds since `2000-01-01 00:00:00 UTC`.
    TimestampTz(i64),
    /// Unresolved abbreviation-bearing temporal text output with captured session context.
    TemporalText {
        kind: TemporalKind,
        wire_text: String,
        date_style: String,
        time_zone: String,
    },
    /// `INTERVAL` (`OID 1186`) independent signed `(months, days, microseconds)`.
    Interval {
        months: i32,
        days: i32,
        microseconds: i64,
    },
    /// Validated UTF-8 RFC 8259 JSON payload bytes (`OID 114` or `OID 3802` without `0x01` prefix).
    Json(Vec<u8>),
    /// Multidimensional PostgreSQL array with explicit dimensions, lower bounds, and element OID.
    Array {
        values: Vec<PgValue>,
        dimensions: Vec<i32>,
        lower_bounds: Vec<i32>,
        element_oid: u32,
    },
    /// Named composite or anonymous `record` (`OID 2249`) with ordered field names and OIDs.
    Composite {
        type_oid: u32,
        field_names: Vec<String>,
        field_oids: Vec<u32>,
        values: Vec<PgValue>,
    },
    /// `BIT` (`OID 1560`) or `VARBIT` (`OID 1562`) MSB-first bitstring.
    BitString { data: Vec<u8>, bit_length: i32 },
    /// `pgvector` `vector` or `halfvec` with finite `f32`-widened components.
    Vector { values: Vec<f32>, kind: VectorKind },
}
