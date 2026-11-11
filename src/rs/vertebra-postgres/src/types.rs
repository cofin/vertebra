//! Session-local PostgreSQL `pg_catalog` type registry (`TypeCatalog`, `TypeInfo`, `TypeKind`, `CompositeField`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::codecs::{
    BOOL_OID, BPCHAR_OID, BYTEA_OID, CID_OID, DATE_OID, FLOAT4_OID, FLOAT8_OID, INT2_OID, INT4_OID,
    INT8_OID, INTERVAL_OID, JSON_OID, JSONB_OID, NAME_OID, NUMERIC_OID, OID_OID, REGPROC_OID,
    TEXT_OID, TIME_OID, TIMESTAMP_OID, TIMESTAMPTZ_OID, UNKNOWN_OID, UUID_OID, VARCHAR_OID,
    XID_OID, XML_OID,
};
use crate::errors::PostgresError;
use crate::result::PgValue;
use crate::session::{ExchangeState, PostgresConnection};

/// PostgreSQL `BIT` type OID (`1560`).
pub const BIT_OID: u32 = 1560;
/// PostgreSQL `VARBIT` type OID (`1562`).
pub const VARBIT_OID: u32 = 1562;
/// PostgreSQL `BOX` type OID (`603`).
pub const BOX_OID: u32 = 603;
/// PostgreSQL anonymous `record` type OID (`2249`).
pub const RECORD_OID: u32 = 2249;

/// PostgreSQL `_xml` array OID (`143`).
pub const XML_ARRAY_OID: u32 = 143;
/// PostgreSQL `_bool` array OID (`1000`).
pub const BOOL_ARRAY_OID: u32 = 1000;
/// PostgreSQL `_bytea` array OID (`1001`).
pub const BYTEA_ARRAY_OID: u32 = 1001;
/// PostgreSQL `_char` / `_name` legacy array OID (`1002`).
pub const CHAR_ARRAY_OID: u32 = 1002;
/// PostgreSQL `_name` array OID (`1003`).
pub const NAME_ARRAY_OID: u32 = 1003;
/// PostgreSQL `_int2` array OID (`1005`).
pub const INT2_ARRAY_OID: u32 = 1005;
/// PostgreSQL `_int4` array OID (`1007`).
pub const INT4_ARRAY_OID: u32 = 1007;
/// PostgreSQL `_regproc` array OID (`1008`).
pub const REGPROC_ARRAY_OID: u32 = 1008;
/// PostgreSQL `_text` array OID (`1009`).
pub const TEXT_ARRAY_OID: u32 = 1009;
/// PostgreSQL `_xid` array OID (`1011`).
pub const XID_ARRAY_OID: u32 = 1011;
/// PostgreSQL `_cid` array OID (`1012`).
pub const CID_ARRAY_OID: u32 = 1012;
/// PostgreSQL `_bpchar` array OID (`1014`).
pub const BPCHAR_ARRAY_OID: u32 = 1014;
/// PostgreSQL `_varchar` array OID (`1015`).
pub const VARCHAR_ARRAY_OID: u32 = 1015;
/// PostgreSQL `_int8` array OID (`1016`).
pub const INT8_ARRAY_OID: u32 = 1016;
/// PostgreSQL `_box` array OID (`1020`, `delimiter = b';'`).
pub const BOX_ARRAY_OID: u32 = 1020;
/// PostgreSQL `_float4` array OID (`1021`).
pub const FLOAT4_ARRAY_OID: u32 = 1021;
/// PostgreSQL `_float8` array OID (`1022`).
pub const FLOAT8_ARRAY_OID: u32 = 1022;
/// PostgreSQL `_oid` array OID (`1028`).
pub const OID_ARRAY_OID: u32 = 1028;
/// PostgreSQL `_json` array OID (`199`).
pub const JSON_ARRAY_OID: u32 = 199;
/// PostgreSQL `_timestamp` array OID (`1115`).
pub const TIMESTAMP_ARRAY_OID: u32 = 1115;
/// PostgreSQL `_date` array OID (`1182`).
pub const DATE_ARRAY_OID: u32 = 1182;
/// PostgreSQL `_time` array OID (`1183`).
pub const TIME_ARRAY_OID: u32 = 1183;
/// PostgreSQL `_timestamptz` array OID (`1185`).
pub const TIMESTAMPTZ_ARRAY_OID: u32 = 1185;
/// PostgreSQL `_interval` array OID (`1187`).
pub const INTERVAL_ARRAY_OID: u32 = 1187;
/// PostgreSQL `_numeric` array OID (`1231`).
pub const NUMERIC_ARRAY_OID: u32 = 1231;
/// PostgreSQL `_bit` array OID (`1561`).
pub const BIT_ARRAY_OID: u32 = 1561;
/// PostgreSQL `_varbit` array OID (`1563`).
pub const VARBIT_ARRAY_OID: u32 = 1563;
/// PostgreSQL `_record` array OID (`2287`).
pub const RECORD_ARRAY_OID: u32 = 2287;
/// PostgreSQL `_uuid` array OID (`2951`).
pub const UUID_ARRAY_OID: u32 = 2951;
/// PostgreSQL `_jsonb` array OID (`3807`).
pub const JSONB_ARRAY_OID: u32 = 3807;

/// Maximum domain inheritance chain depth (`16`).
pub const MAX_DOMAIN_DEPTH: usize = 16;
/// Maximum transitive catalog discovery rounds (`8`).
pub const MAX_DISCOVERY_ROUNDS: usize = 8;
/// Maximum total catalog rows processed during a single discovery (`1024`).
pub const MAX_DISCOVERY_ROWS: usize = 1024;

/// Non-dropped attribute metadata for a named PostgreSQL composite type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompositeField {
    /// Attribute name (`pg_attribute.attname`).
    pub name: String,
    /// Positive attribute type OID (`pg_attribute.atttypid`).
    pub type_oid: u32,
    /// Attribute type modifier (`pg_attribute.atttypmod`).
    pub type_modifier: i32,
}

/// Classification of a PostgreSQL `pg_type` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeKind {
    /// Built-in or base scalar type.
    Scalar,
    /// 1D..=6D PostgreSQL array type with element OID and `typdelim` byte (`b','` or `b';'`).
    Array { element_oid: u32, delimiter: u8 },
    /// Named composite or anonymous `record` (`OID 2249`) with ordered non-dropped attributes.
    Composite { fields: Vec<CompositeField> },
    /// Domain type aliasing `base_oid`.
    Domain { base_oid: u32 },
    /// PostgreSQL `ENUM` type (`typtype = 'e'`).
    Enum,
    /// PostgreSQL range or multirange type (`typtype = 'r'` / `'m'`).
    Range,
    /// Extension-owned base type (such as `pgvector` `"vector"` or `"halfvec"`).
    Extension { schema: String, name: String },
}

/// Complete metadata descriptor for a single PostgreSQL type OID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeInfo {
    /// Positive PostgreSQL `pg_type.oid`.
    pub oid: u32,
    /// Schema name (`pg_namespace.nspname`).
    pub schema: String,
    /// Type name (`pg_type.typname`).
    pub name: String,
    /// Structural classification.
    pub kind: TypeKind,
    /// Array/element delimiter (`pg_type.typdelim`, usually `b','` or `b';'`).
    pub delimiter: u8,
}

/// Per-session PostgreSQL type catalog seeded with built-in OIDs and populated via `pg_catalog` discovery.
#[derive(Clone, Debug)]
pub struct TypeCatalog {
    types: HashMap<u32, TypeInfo>,
    builtin_oids: HashSet<u32>,
    extension_oids: HashMap<String, u32>,
    generation: u64,
}

impl Default for TypeCatalog {
    fn default() -> Self {
        Self::new_with_builtins()
    }
}

impl TypeCatalog {
    /// Construct a new [`TypeCatalog`] pre-seeded with all built-in scalar, temporal, JSON, bit, record, and array OIDs.
    pub fn new_with_builtins() -> Self {
        let mut types = HashMap::with_capacity(64);
        let mut builtin_oids = HashSet::with_capacity(64);

        let mut add_scalar = |oid: u32, name: &str, delim: u8| {
            builtin_oids.insert(oid);
            types.insert(
                oid,
                TypeInfo {
                    oid,
                    schema: "pg_catalog".to_string(),
                    name: name.to_string(),
                    kind: TypeKind::Scalar,
                    delimiter: delim,
                },
            );
        };

        add_scalar(BOOL_OID, "bool", b',');
        add_scalar(BYTEA_OID, "bytea", b',');
        add_scalar(NAME_OID, "name", b',');
        add_scalar(INT8_OID, "int8", b',');
        add_scalar(INT2_OID, "int2", b',');
        add_scalar(INT4_OID, "int4", b',');
        add_scalar(REGPROC_OID, "regproc", b',');
        add_scalar(TEXT_OID, "text", b',');
        add_scalar(OID_OID, "oid", b',');
        add_scalar(XID_OID, "xid", b',');
        add_scalar(CID_OID, "cid", b',');
        add_scalar(JSON_OID, "json", b',');
        add_scalar(XML_OID, "xml", b',');
        add_scalar(BOX_OID, "box", b';');
        add_scalar(FLOAT4_OID, "float4", b',');
        add_scalar(FLOAT8_OID, "float8", b',');
        add_scalar(UNKNOWN_OID, "unknown", b',');
        add_scalar(BPCHAR_OID, "bpchar", b',');
        add_scalar(VARCHAR_OID, "varchar", b',');
        add_scalar(DATE_OID, "date", b',');
        add_scalar(TIME_OID, "time", b',');
        add_scalar(TIMESTAMP_OID, "timestamp", b',');
        add_scalar(TIMESTAMPTZ_OID, "timestamptz", b',');
        add_scalar(INTERVAL_OID, "interval", b',');
        add_scalar(BIT_OID, "bit", b',');
        add_scalar(VARBIT_OID, "varbit", b',');
        add_scalar(NUMERIC_OID, "numeric", b',');
        add_scalar(UUID_OID, "uuid", b',');
        add_scalar(JSONB_OID, "jsonb", b',');

        builtin_oids.insert(RECORD_OID);
        types.insert(
            RECORD_OID,
            TypeInfo {
                oid: RECORD_OID,
                schema: "pg_catalog".to_string(),
                name: "record".to_string(),
                kind: TypeKind::Composite { fields: Vec::new() },
                delimiter: b',',
            },
        );

        let mut add_array = |oid: u32, name: &str, element_oid: u32, delimiter: u8| {
            builtin_oids.insert(oid);
            types.insert(
                oid,
                TypeInfo {
                    oid,
                    schema: "pg_catalog".to_string(),
                    name: name.to_string(),
                    kind: TypeKind::Array {
                        element_oid,
                        delimiter,
                    },
                    delimiter,
                },
            );
        };

        add_array(XML_ARRAY_OID, "_xml", XML_OID, b',');
        add_array(BOOL_ARRAY_OID, "_bool", BOOL_OID, b',');
        add_array(BYTEA_ARRAY_OID, "_bytea", BYTEA_OID, b',');
        add_array(CHAR_ARRAY_OID, "_char", NAME_OID, b',');
        add_array(NAME_ARRAY_OID, "_name", NAME_OID, b',');
        add_array(INT2_ARRAY_OID, "_int2", INT2_OID, b',');
        add_array(INT4_ARRAY_OID, "_int4", INT4_OID, b',');
        add_array(REGPROC_ARRAY_OID, "_regproc", REGPROC_OID, b',');
        add_array(TEXT_ARRAY_OID, "_text", TEXT_OID, b',');
        add_array(XID_ARRAY_OID, "_xid", XID_OID, b',');
        add_array(CID_ARRAY_OID, "_cid", CID_OID, b',');
        add_array(OID_ARRAY_OID, "_oid", OID_OID, b',');
        add_array(JSON_ARRAY_OID, "_json", JSON_OID, b',');
        add_array(BOX_ARRAY_OID, "_box", BOX_OID, b';');
        add_array(FLOAT4_ARRAY_OID, "_float4", FLOAT4_OID, b',');
        add_array(FLOAT8_ARRAY_OID, "_float8", FLOAT8_OID, b',');
        add_array(BPCHAR_ARRAY_OID, "_bpchar", BPCHAR_OID, b',');
        add_array(VARCHAR_ARRAY_OID, "_varchar", VARCHAR_OID, b',');
        add_array(INT8_ARRAY_OID, "_int8", INT8_OID, b',');
        add_array(DATE_ARRAY_OID, "_date", DATE_OID, b',');
        add_array(TIME_ARRAY_OID, "_time", TIME_OID, b',');
        add_array(TIMESTAMP_ARRAY_OID, "_timestamp", TIMESTAMP_OID, b',');
        add_array(TIMESTAMPTZ_ARRAY_OID, "_timestamptz", TIMESTAMPTZ_OID, b',');
        add_array(INTERVAL_ARRAY_OID, "_interval", INTERVAL_OID, b',');
        add_array(BIT_ARRAY_OID, "_bit", BIT_OID, b',');
        add_array(VARBIT_ARRAY_OID, "_varbit", VARBIT_OID, b',');
        add_array(NUMERIC_ARRAY_OID, "_numeric", NUMERIC_OID, b',');
        add_array(RECORD_ARRAY_OID, "_record", RECORD_OID, b',');
        add_array(UUID_ARRAY_OID, "_uuid", UUID_OID, b',');
        add_array(JSONB_ARRAY_OID, "_jsonb", JSONB_OID, b',');

        Self {
            types,
            builtin_oids,
            extension_oids: HashMap::new(),
            generation: 0,
        }
    }

    /// Return the current catalog invalidation generation (`0` initially).
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Look up a [`TypeInfo`] entry by PostgreSQL `oid`.
    pub fn type_info(&self, oid: u32) -> Option<&TypeInfo> {
        self.types.get(&oid)
    }

    /// Insert or update a custom [`TypeInfo`] entry in this catalog.
    pub fn insert_custom_type(&mut self, info: TypeInfo) {
        if let TypeKind::Extension {
            ref schema,
            ref name,
        } = info.kind
        {
            self.extension_oids.insert(name.clone(), info.oid);
            self.extension_oids
                .insert(format!("{schema}.{name}"), info.oid);
        }
        self.types.insert(info.oid, info);
    }

    /// Resolve a domain chain (`TypeKind::Domain`) to its underlying non-domain base OID (`<= 16` hops).
    pub fn resolve_domain_base(&self, oid: u32) -> Result<u32, PostgresError> {
        let mut current = oid;
        let mut visited = [0u32; MAX_DOMAIN_DEPTH + 1];
        for step in 0..=MAX_DOMAIN_DEPTH {
            if visited[..step].contains(&current) {
                return Err(PostgresError::Data(format!(
                    "cyclic PostgreSQL domain definition detected at OID {current}"
                )));
            }
            visited[step] = current;
            match self.types.get(&current) {
                Some(TypeInfo {
                    kind: TypeKind::Domain { base_oid },
                    ..
                }) => {
                    if *base_oid == 0 {
                        return Err(PostgresError::Data(format!(
                            "domain OID {current} has invalid base_oid 0"
                        )));
                    }
                    if step == MAX_DOMAIN_DEPTH {
                        return Err(PostgresError::Data(format!(
                            "domain chain for OID {oid} exceeds maximum depth {MAX_DOMAIN_DEPTH}"
                        )));
                    }
                    current = *base_oid;
                }
                _ => return Ok(current),
            }
        }
        Err(PostgresError::Data(format!(
            "domain chain for OID {oid} exceeds maximum depth {MAX_DOMAIN_DEPTH}"
        )))
    }

    /// Look up a discovered extension type (`"vector"`, `"halfvec"`, or `"<schema>.<name>"`).
    pub fn lookup_extension_type(&self, ext_type_name: &str) -> Option<&TypeInfo> {
        let oid = self.extension_oids.get(ext_type_name).copied()?;
        self.types.get(&oid)
    }

    /// Clear all dynamically discovered custom types and extension bindings while retaining built-in OIDs.
    pub fn invalidate(&mut self) {
        self.types.retain(|oid, _| self.builtin_oids.contains(oid));
        self.extension_oids.clear();
        self.generation = self.generation.saturating_add(1);
    }

    /// Alias for [`TypeCatalog::invalidate`] that clears discovered types and increments [`TypeCatalog::generation`].
    pub fn invalidate_all(&mut self) {
        self.invalidate();
    }

    /// Remove a single dynamically discovered custom `oid` (no-op for built-ins).
    pub fn invalidate_oid(&mut self, oid: u32) {
        if !self.builtin_oids.contains(&oid) {
            self.types.remove(&oid);
            self.extension_oids.retain(|_, &mut v| v != oid);
            self.generation = self.generation.saturating_add(1);
        }
    }

    /// Clear unqualified extension type bindings when session `search_path` changes while preserving verified OIDs.
    pub fn invalidate_extension_names(&mut self) {
        self.extension_oids.retain(|k, _| k.contains('.'));
        self.generation = self.generation.saturating_add(1);
    }

    /// Validate a composite schema against the catalog and invalidate the stale OID on mismatch.
    pub fn validate_composite_schema(
        &mut self,
        type_oid: u32,
        field_names: Option<&[String]>,
        field_oids: &[u32],
    ) -> Result<Vec<CompositeField>, PostgresError> {
        let resolved_oid = self.resolve_domain_base(type_oid)?;
        let Some(fields) = self
            .types
            .get(&resolved_oid)
            .and_then(|info| match &info.kind {
                TypeKind::Composite { fields } => Some(fields.clone()),
                _ => None,
            })
        else {
            return Err(PostgresError::Data(format!(
                "OID {type_oid} is not a known composite type in TypeCatalog"
            )));
        };
        if fields.len() != field_oids.len() {
            let expected_len = fields.len();
            self.invalidate_oid(resolved_oid);
            return Err(PostgresError::Data(format!(
                "composite OID {type_oid} field count mismatch: expected {expected_len}, got {}",
                field_oids.len()
            )));
        }
        if let Some(names) = field_names
            && names.len() != fields.len()
        {
            let expected_len = fields.len();
            self.invalidate_oid(resolved_oid);
            return Err(PostgresError::Data(format!(
                "composite OID {type_oid} field_names count {} does not match {expected_len}",
                names.len(),
            )));
        }
        for (idx, field) in fields.iter().enumerate() {
            let wire_oid = field_oids[idx];
            let compatible = wire_oid == field.type_oid
                || self.resolve_domain_base(field.type_oid).ok() == Some(wire_oid);
            if !compatible {
                let fname = field.name.clone();
                let foid = field.type_oid;
                self.invalidate_oid(resolved_oid);
                return Err(PostgresError::Data(format!(
                    "composite OID {type_oid} field {idx} (`{fname}`) OID mismatch: expected {foid}, got {wire_oid}"
                )));
            }
            if let Some(names) = field_names
                && names[idx] != field.name
            {
                let fname = field.name.clone();
                self.invalidate_oid(resolved_oid);
                return Err(PostgresError::Data(format!(
                    "composite OID {type_oid} field {idx} name mismatch: expected `{fname}`, got `{}`",
                    names[idx]
                )));
            }
        }
        Ok(fields)
    }

    /// Capture an immutable [`Arc<TypeCatalog>`] snapshot for cold pipelines or batch execution.
    pub fn snapshot_for_batch(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }

    /// Merge all custom types and extension bindings from `other` into `self`.
    pub fn merge_from(&mut self, other: &Self) {
        for (&oid, info) in &other.types {
            if !self.builtin_oids.contains(&oid) {
                self.types.insert(oid, info.clone());
            }
        }
        for (k, &v) in &other.extension_oids {
            self.extension_oids.insert(k.clone(), v);
        }
    }

    /// Discover missing custom OIDs and extension types (`"vector"`, `"halfvec"`) via `conn.simple_query` at idle exchange boundaries.
    pub async fn discover(
        &mut self,
        conn: &mut PostgresConnection,
        missing_oids: &[u32],
        extension_names: &[&str],
    ) -> Result<(), PostgresError> {
        if conn.is_closed() {
            return Err(PostgresError::Interface(
                "cannot run TypeCatalog::discover on a closed PostgresConnection".to_string(),
            ));
        }
        if conn.exchange_state() != ExchangeState::Idle {
            return Err(PostgresError::Interface(format!(
                "cannot run TypeCatalog::discover while session exchange state is {:?}",
                conn.exchange_state()
            )));
        }

        let mut ext_targets: Vec<&str> = Vec::new();
        for &ext in extension_names {
            if !matches!(ext, "vector" | "halfvec") {
                return Err(PostgresError::Data(format!(
                    "unsupported extension type name {ext:?} in TypeCatalog::discover"
                )));
            }
            if !self.extension_oids.contains_key(ext) && !ext_targets.contains(&ext) {
                ext_targets.push(ext);
            }
        }

        let mut pending_oids: Vec<u32> = Vec::new();
        let mut queried_oids: HashSet<u32> = HashSet::new();
        for &oid in missing_oids {
            if oid == 0 {
                continue;
            }
            if !self.types.contains_key(&oid) && queried_oids.insert(oid) {
                pending_oids.push(oid);
            }
        }

        if pending_oids.is_empty() && ext_targets.is_empty() {
            return Ok(());
        }

        let mut total_rows = 0usize;
        let mut round = 0usize;

        while !pending_oids.is_empty() || !ext_targets.is_empty() {
            if round >= MAX_DISCOVERY_ROUNDS {
                return Err(PostgresError::Data(format!(
                    "TypeCatalog::discover exceeded maximum transitive rounds ({MAX_DISCOVERY_ROUNDS})"
                )));
            }
            round += 1;

            let sql = build_discovery_query(&pending_oids, &ext_targets);
            pending_oids.clear();
            ext_targets.clear();

            let results = conn.simple_query(&sql).await?;
            let mut next_candidates: Vec<u32> = Vec::new();

            for result in &results {
                total_rows = total_rows.saturating_add(result.rows.len());
                if total_rows > MAX_DISCOVERY_ROWS {
                    return Err(PostgresError::Data(format!(
                        "TypeCatalog::discover exceeded maximum catalog row cap ({MAX_DISCOVERY_ROWS})"
                    )));
                }
                parse_catalog_result(self, result, &mut next_candidates)?;
            }

            for child_oid in next_candidates {
                if child_oid != 0
                    && !self.types.contains_key(&child_oid)
                    && queried_oids.insert(child_oid)
                {
                    pending_oids.push(child_oid);
                }
            }
        }

        let all_oids: Vec<u32> = self.types.keys().copied().collect();
        for oid in all_oids {
            let _ = self.resolve_domain_base(oid)?;
        }

        conn.type_catalog_mut().merge_from(self);
        Ok(())
    }
}

fn build_discovery_query(oids: &[u32], ext_names: &[&str]) -> String {
    let mut predicates = Vec::new();
    if !oids.is_empty() {
        let oid_list = oids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        predicates.push(format!("t.oid IN ({oid_list})"));
    }
    if !ext_names.is_empty() {
        let ext_list = ext_names
            .iter()
            .map(|name| match *name {
                "vector" => "'vector'",
                "halfvec" => "'halfvec'",
                _ => unreachable!("validated allowlist"),
            })
            .collect::<Vec<_>>()
            .join(", ");
        predicates.push(format!(
            "(t.typname IN ({ext_list}) AND ext.extname IS NOT NULL)"
        ));
    }
    let where_clause = predicates.join(" OR ");
    format!(
        "SELECT t.oid, n.nspname, t.typname, t.typtype, t.typdelim, t.typelem, t.typbasetype, \
         ext.extname, a.attname, a.atttypid, a.atttypmod, a.attnum, a.attisdropped \
         FROM pg_catalog.pg_type t \
         JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
         LEFT JOIN pg_catalog.pg_depend dep ON dep.classid = 1247 AND dep.objid = t.oid AND dep.deptype = 'e' \
         LEFT JOIN pg_catalog.pg_extension ext ON ext.oid = dep.refobjid \
         LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped \
         WHERE {where_clause} \
         ORDER BY t.oid, a.attnum"
    )
}

fn cell_as_u32(val: Option<&PgValue>) -> Result<u32, PostgresError> {
    match val {
        None | Some(PgValue::Null) => Ok(0),
        Some(PgValue::Int2(v)) => {
            u32::try_from(*v).map_err(|_| PostgresError::Data(format!("negative catalog OID {v}")))
        }
        Some(PgValue::Int4(v)) => {
            u32::try_from(*v).map_err(|_| PostgresError::Data(format!("negative catalog OID {v}")))
        }
        Some(PgValue::Int8(v)) => u32::try_from(*v)
            .map_err(|_| PostgresError::Data(format!("out-of-range catalog OID {v}"))),
        Some(PgValue::Text(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(0)
            } else {
                trimmed.parse::<u32>().map_err(|_| {
                    PostgresError::Data(format!("invalid catalog u32 literal {trimmed:?}"))
                })
            }
        }
        other => Err(PostgresError::Data(format!(
            "unexpected catalog OID cell value {other:?}"
        ))),
    }
}

fn cell_as_i32(val: Option<&PgValue>, default: i32) -> Result<i32, PostgresError> {
    match val {
        None | Some(PgValue::Null) => Ok(default),
        Some(PgValue::Int2(v)) => Ok(i32::from(*v)),
        Some(PgValue::Int4(v)) => Ok(*v),
        Some(PgValue::Int8(v)) => i32::try_from(*v)
            .map_err(|_| PostgresError::Data(format!("out-of-range catalog i32 {v}"))),
        Some(PgValue::Text(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(default)
            } else {
                trimmed.parse::<i32>().map_err(|_| {
                    PostgresError::Data(format!("invalid catalog i32 literal {trimmed:?}"))
                })
            }
        }
        other => Err(PostgresError::Data(format!(
            "unexpected catalog i32 cell value {other:?}"
        ))),
    }
}

fn cell_as_opt_str(val: Option<&PgValue>) -> Option<&str> {
    match val {
        Some(PgValue::Text(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn cell_as_bool(val: Option<&PgValue>) -> bool {
    match val {
        Some(PgValue::Bool(b)) => *b,
        Some(PgValue::Text(s)) => matches!(
            s.trim().to_ascii_lowercase().as_str(),
            "t" | "true" | "1" | "y" | "yes"
        ),
        _ => false,
    }
}

fn parse_catalog_result(
    catalog: &mut TypeCatalog,
    result: &crate::result::QueryResult,
    next_candidates: &mut Vec<u32>,
) -> Result<(), PostgresError> {
    let col_index = |name: &str, fallback: usize| -> Option<usize> {
        result
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .or_else(|| (fallback < result.columns.len()).then_some(fallback))
    };

    let idx_oid = col_index("oid", 0);
    let idx_nsp = col_index("nspname", 1);
    let idx_name = col_index("typname", 2);
    let idx_typtype = col_index("typtype", 3);
    let idx_delim = col_index("typdelim", 4);
    let idx_elem = col_index("typelem", 5);
    let idx_base = col_index("typbasetype", 6);
    let idx_ext = col_index("extname", 7);
    let idx_attname = col_index("attname", 8);
    let idx_atttypid = col_index("atttypid", 9);
    let idx_atttypmod = col_index("atttypmod", 10);
    let idx_attnum = col_index("attnum", 11);
    let idx_attdropped = col_index("attisdropped", 12);

    let mut composite_fields: HashMap<u32, Vec<(i32, CompositeField)>> = HashMap::new();
    let mut composite_headers: HashMap<u32, (String, String, u8)> = HashMap::new();

    for row in &result.rows {
        let oid = cell_as_u32(idx_oid.and_then(|i| row.get(i)))?;
        if oid == 0 {
            return Err(PostgresError::Data(
                "pg_catalog discovery row returned invalid type oid 0".to_string(),
            ));
        }
        let schema = idx_nsp
            .and_then(|i| cell_as_opt_str(row.get(i)))
            .unwrap_or("public")
            .to_string();
        let name = idx_name
            .and_then(|i| cell_as_opt_str(row.get(i)))
            .unwrap_or("")
            .to_string();
        let typtype = idx_typtype
            .and_then(|i| cell_as_opt_str(row.get(i)))
            .unwrap_or("b");
        let delimiter = idx_delim
            .and_then(|i| cell_as_opt_str(row.get(i)))
            .and_then(|s| s.as_bytes().first().copied())
            .unwrap_or(b',');
        let typelem = cell_as_u32(idx_elem.and_then(|i| row.get(i)))?;
        let typbasetype = cell_as_u32(idx_base.and_then(|i| row.get(i)))?;
        let extname = idx_ext
            .and_then(|i| cell_as_opt_str(row.get(i)))
            .filter(|s| !s.is_empty());

        match typtype {
            "c" => {
                composite_headers
                    .entry(oid)
                    .or_insert_with(|| (schema.clone(), name.clone(), delimiter));
                let entry = composite_fields.entry(oid).or_default();
                let dropped = idx_attdropped
                    .and_then(|i| row.get(i))
                    .is_some_and(|v| cell_as_bool(Some(v)));
                let attnum = cell_as_i32(idx_attnum.and_then(|i| row.get(i)), 1)?;
                if !dropped
                    && attnum > 0
                    && let Some(att_name) = idx_attname
                        .and_then(|i| cell_as_opt_str(row.get(i)))
                        .filter(|s| !s.is_empty())
                {
                    let atttypid = cell_as_u32(idx_atttypid.and_then(|i| row.get(i)))?;
                    if atttypid == 0 {
                        return Err(PostgresError::Data(format!(
                            "composite OID {oid} attribute `{att_name}` has invalid atttypid 0"
                        )));
                    }
                    let atttypmod = cell_as_i32(idx_atttypmod.and_then(|i| row.get(i)), -1)?;
                    next_candidates.push(atttypid);
                    entry.push((
                        attnum,
                        CompositeField {
                            name: att_name.to_string(),
                            type_oid: atttypid,
                            type_modifier: atttypmod,
                        },
                    ));
                }
            }
            "d" => {
                if typbasetype == 0 {
                    return Err(PostgresError::Data(format!(
                        "domain OID {oid} (`{name}`) has invalid typbasetype 0"
                    )));
                }
                next_candidates.push(typbasetype);
                catalog.insert_custom_type(TypeInfo {
                    oid,
                    schema,
                    name,
                    kind: TypeKind::Domain {
                        base_oid: typbasetype,
                    },
                    delimiter,
                });
            }
            "e" => {
                catalog.insert_custom_type(TypeInfo {
                    oid,
                    schema,
                    name,
                    kind: TypeKind::Enum,
                    delimiter,
                });
            }
            "r" | "m" => {
                catalog.insert_custom_type(TypeInfo {
                    oid,
                    schema,
                    name,
                    kind: TypeKind::Range,
                    delimiter,
                });
            }
            _ => {
                if typelem != 0 && (name.starts_with('_') || extname.is_none()) {
                    next_candidates.push(typelem);
                    catalog.insert_custom_type(TypeInfo {
                        oid,
                        schema,
                        name,
                        kind: TypeKind::Array {
                            element_oid: typelem,
                            delimiter,
                        },
                        delimiter,
                    });
                } else if extname.is_some() || matches!(name.as_str(), "vector" | "halfvec") {
                    catalog.insert_custom_type(TypeInfo {
                        oid,
                        schema: schema.clone(),
                        name: name.clone(),
                        kind: TypeKind::Extension { schema, name },
                        delimiter,
                    });
                } else {
                    catalog.insert_custom_type(TypeInfo {
                        oid,
                        schema,
                        name,
                        kind: TypeKind::Scalar,
                        delimiter,
                    });
                }
            }
        }
    }

    for (oid, (schema, name, delimiter)) in composite_headers {
        let mut numbered = composite_fields.remove(&oid).unwrap_or_default();
        numbered.sort_by_key(|(attnum, _)| *attnum);
        let fields = numbered.into_iter().map(|(_, f)| f).collect();
        catalog.insert_custom_type(TypeInfo {
            oid,
            schema,
            name,
            kind: TypeKind::Composite { fields },
            delimiter,
        });
    }

    Ok(())
}
