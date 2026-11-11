//! Incremental PostgreSQL `pgoutput` v1 relation metadata, custom-type mismatch invalidation, and
//! lossless four-state tuple decoder.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::{Buf, BytesMut};

use crate::codecs::{
    CodecContext, RECORD_OID, decode_value_with_catalog, supports_binary_with_catalog,
};
use crate::errors::PostgresError;
use crate::result::{Format, PgValue};
use crate::types::{TypeCatalog, TypeInfo, TypeKind};

/// Default bounded capacity of session-cached `pgoutput` relation schemas (`4096`).
pub const DEFAULT_MAX_RELATIONS: usize = 4096;

/// Default maximum byte size of a single buffered `pgoutput` record (`16 MiB`).
pub const DEFAULT_MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// Column descriptor emitted by a `pgoutput` v1 `Relation` (`'R'`) frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputColumn {
    /// Whether this column participates in the published relation's replica identity key (`flags & 0x01`).
    pub is_key: bool,
    /// Non-dropped PostgreSQL attribute name.
    pub name: String,
    /// Column PostgreSQL `pg_type.oid`.
    pub type_oid: u32,
    /// Column PostgreSQL `pg_attribute.atttypmod` (`-1` when unspecified).
    pub type_modifier: i32,
}

/// Immutable relation metadata snapshot cached from a `pgoutput` v1 `Relation` (`'R'`) message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputRelationSchema {
    /// Published table OID (`pg_class.oid`).
    pub relation_oid: u32,
    /// Schema namespace (`""` when PostgreSQL encodes `pg_catalog` relations).
    pub namespace: String,
    /// Relation name (`pg_class.relname`).
    pub name: String,
    /// Replica identity setting (`b'd'`, `b'f'`, `b'n'`, or `b'i'`).
    pub replica_identity: u8,
    /// Ordered non-dropped published columns captured at this schema version.
    pub columns: Arc<[PgOutputColumn]>,
}

/// Lossless `pgoutput` tuple cell distinguishing SQL `NULL`, unchanged TOAST values, and decoded values.
#[derive(Debug, Clone, PartialEq)]
pub enum PgOutputTupleValue {
    /// Explicit SQL `NULL` column (`'n'`).
    Null,
    /// Unchanged external TOAST column omitted on the wire (`'u'`).
    UnchangedToast,
    /// Decoded text (`'t'`) or binary (`'b'`) value preserving shared `PgValue` fidelity.
    Value(PgValue),
}

/// Decoded `pgoutput` v1 wire frame emitted by [`PgOutputDecoder::decode_next_frame`].
#[derive(Debug, Clone, PartialEq)]
pub enum PgOutputFrame {
    /// Relation metadata declaration or in-place schema replacement (`'R'`).
    Relation(Arc<PgOutputRelationSchema>),
    /// Custom data type declaration (`'Y'`).
    Type {
        /// Declared `pg_type.oid`.
        type_oid: u32,
        /// Type schema namespace.
        namespace: String,
        /// Type name.
        name: String,
    },
    /// Row insertion payload carrying its immutable relation snapshot and four-state new tuple (`'I'`).
    Insert {
        /// Relation schema active when this tuple arrived.
        relation: Arc<PgOutputRelationSchema>,
        /// Decoded column cells aligned positionally with `relation.columns`.
        new_tuple: Vec<PgOutputTupleValue>,
    },
}

/// Incremental streaming `pgoutput` v1 decoder with bounded accumulator and `RelationCache`.
#[derive(Debug)]
pub struct PgOutputDecoder {
    buf: BytesMut,
    relations: HashMap<u32, Arc<PgOutputRelationSchema>>,
    type_identities: HashMap<u32, (String, String)>,
    invalidated_types: HashSet<u32>,
    max_relations: usize,
    max_record_bytes: usize,
}

impl Default for PgOutputDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_RELATIONS, DEFAULT_MAX_RECORD_BYTES)
    }
}

impl PgOutputDecoder {
    /// Construct a new [`PgOutputDecoder`] enforcing `max_relations` and `max_record_bytes`.
    pub fn new(max_relations: usize, max_record_bytes: usize) -> Self {
        Self {
            buf: BytesMut::new(),
            relations: HashMap::new(),
            type_identities: HashMap::new(),
            invalidated_types: HashSet::new(),
            max_relations,
            max_record_bytes,
        }
    }

    /// Construct a [`PgOutputDecoder`] pre-seeded with catalog `(namespace, name)` identities per custom type OID.
    pub fn with_type_identities(
        max_relations: usize,
        max_record_bytes: usize,
        type_identities: HashMap<u32, (String, String)>,
    ) -> Self {
        Self {
            buf: BytesMut::new(),
            relations: HashMap::new(),
            type_identities,
            invalidated_types: HashSet::new(),
            max_relations,
            max_record_bytes,
        }
    }

    /// Replace the preloaded `(namespace, name)` custom-type map (for example after reconnect preflight).
    pub fn set_type_identities(&mut self, type_identities: HashMap<u32, (String, String)>) {
        self.type_identities = type_identities;
        self.invalidated_types.clear();
    }

    /// Maximum distinct relation entries permitted in the session `RelationCache`.
    pub const fn max_relations(&self) -> usize {
        self.max_relations
    }

    /// Maximum byte size permitted for a single buffered `pgoutput` record.
    pub const fn max_record_bytes(&self) -> usize {
        self.max_record_bytes
    }

    /// Lookup the current immutable schema snapshot for `relation_oid`, if registered.
    pub fn relation(&self, relation_oid: u32) -> Option<&Arc<PgOutputRelationSchema>> {
        self.relations.get(&relation_oid)
    }

    /// Return the number of distinct relations currently stored in `RelationCache`.
    pub fn relation_count(&self) -> usize {
        self.relations.len()
    }

    /// Return the number of unconsumed `pgoutput` bytes currently buffered.
    pub fn buffered_bytes(&self) -> usize {
        self.buf.len()
    }

    /// Append incoming `XLogData` payload bytes, failing before allocation if buffered bytes exceed `max_record_bytes`.
    pub fn push(&mut self, payload: &[u8]) -> Result<(), PostgresError> {
        if payload.is_empty() {
            return Ok(());
        }
        let next_len = self.buf.len().checked_add(payload.len()).ok_or_else(|| {
            PostgresError::Operational("pgoutput decoder buffer length overflowed usize".to_owned())
        })?;
        if next_len > self.max_record_bytes {
            return Err(PostgresError::Operational(format!(
                "pgoutput buffered record size {next_len} bytes exceeds maximum limit {} bytes",
                self.max_record_bytes
            )));
        }
        self.buf.extend_from_slice(payload);
        Ok(())
    }

    /// Non-destructively parse and decode the next complete `pgoutput` v1 frame from the accumulator.
    ///
    /// Returns `Ok(None)` without mutating cached schemas, `catalog`, or buffered bytes when an
    /// incomplete prefix awaits further `XLogData` chunks, and consumes the exact frame length once
    /// complete so coalesced messages decode sequentially.
    pub fn decode_next_frame(
        &mut self,
        catalog: &mut TypeCatalog,
        codec_ctx: &CodecContext,
    ) -> Result<Option<PgOutputFrame>, PostgresError> {
        if self.buf.is_empty() {
            return Ok(None);
        }

        let step = {
            let mut cursor = FrameCursor::new(&self.buf, self.max_record_bytes);
            let tag = match cursor.read_u8() {
                Ok(t) => t,
                Err(ParseStepError::NeedMoreBytes) => return Ok(None),
                Err(ParseStepError::Fatal(err)) => return Err(err),
            };
            match tag {
                b'R' => parse_relation_frame(&mut cursor)
                    .map(|rel| (cursor.position(), ParsedBody::Relation(rel))),
                b'Y' => parse_type_frame(&mut cursor)
                    .map(|(oid, ns, name)| (cursor.position(), ParsedBody::Type(oid, ns, name))),
                b'I' => parse_insert_frame(&mut cursor, &self.relations, catalog, codec_ctx)
                    .map(|(rel, tup)| (cursor.position(), ParsedBody::Insert(rel, tup))),
                other => Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
                    "unsupported or unnegotiated pgoutput v1 message tag 0x{other:02x} ({:?})",
                    other as char
                )))),
            }
        };

        let (consumed, parsed) = match step {
            Ok(done) => done,
            Err(ParseStepError::NeedMoreBytes) => {
                if self.buf.len() >= self.max_record_bytes {
                    return Err(PostgresError::Operational(format!(
                        "incomplete pgoutput frame spans {} bytes and reaches record limit {} bytes",
                        self.buf.len(),
                        self.max_record_bytes
                    )));
                }
                return Ok(None);
            }
            Err(ParseStepError::Fatal(err)) => return Err(err),
        };

        if consumed > self.max_record_bytes {
            return Err(PostgresError::Operational(format!(
                "decoded pgoutput frame size {consumed} bytes exceeds maximum limit {} bytes",
                self.max_record_bytes
            )));
        }

        match parsed {
            ParsedBody::Relation(schema) => {
                let relation_oid = schema.relation_oid;
                if self.relations.len() >= self.max_relations
                    && !self.relations.contains_key(&relation_oid)
                {
                    return Err(PostgresError::Operational(format!(
                        "pgoutput RelationCache reached capacity ({} relations) before registering relation OID {relation_oid}",
                        self.max_relations
                    )));
                }
                self.buf.advance(consumed);
                let arc_schema = Arc::new(schema);
                self.relations.insert(relation_oid, Arc::clone(&arc_schema));
                Ok(Some(PgOutputFrame::Relation(arc_schema)))
            }
            ParsedBody::Type(type_oid, namespace, name) => {
                self.buf.advance(consumed);
                self.reconcile_type_declaration(catalog, type_oid, &namespace, &name);
                Ok(Some(PgOutputFrame::Type {
                    type_oid,
                    namespace,
                    name,
                }))
            }
            ParsedBody::Insert(relation, new_tuple) => {
                self.buf.advance(consumed);
                Ok(Some(PgOutputFrame::Insert {
                    relation,
                    new_tuple,
                }))
            }
        }
    }

    /// Clear partial accumulator bytes and session-scoped relation state on transport disconnect.
    pub fn reset_on_disconnect(&mut self) {
        self.buf.clear();
        self.relations.clear();
    }

    /// Verify that no truncated partial `pgoutput` record remains when backend `CopyDone` arrives cleanly.
    pub fn finish_clean_eof(&self) -> Result<(), PostgresError> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(PostgresError::Protocol(format!(
                "clean pgoutput CopyDone received with {} trailing incomplete record bytes",
                self.buf.len()
            )))
        }
    }

    fn reconcile_type_declaration(
        &mut self,
        catalog: &mut TypeCatalog,
        type_oid: u32,
        namespace: &str,
        name: &str,
    ) {
        if self.invalidated_types.contains(&type_oid) {
            catalog.invalidate_oid(type_oid);
            return;
        }
        if let Some((expected_ns, expected_name)) = self.type_identities.get(&type_oid) {
            if identity_matches(expected_ns, expected_name, namespace, name) {
                return;
            }
            self.type_identities.remove(&type_oid);
            self.invalidated_types.insert(type_oid);
            catalog.invalidate_oid(type_oid);
            return;
        }
        if let Some(info) = catalog.type_info(type_oid)
            && !identity_matches(&info.schema, &info.name, namespace, name)
        {
            self.invalidated_types.insert(type_oid);
            catalog.invalidate_oid(type_oid);
        }
    }
}

enum ParsedBody {
    Relation(PgOutputRelationSchema),
    Type(u32, String, String),
    Insert(Arc<PgOutputRelationSchema>, Vec<PgOutputTupleValue>),
}

enum ParseStepError {
    NeedMoreBytes,
    Fatal(PostgresError),
}

enum RawWireCell<'a> {
    Null,
    UnchangedToast,
    Text(&'a [u8]),
    Binary(&'a [u8]),
}

struct FrameCursor<'a> {
    slice: &'a [u8],
    pos: usize,
    max_record_bytes: usize,
}

impl<'a> FrameCursor<'a> {
    const fn new(slice: &'a [u8], max_record_bytes: usize) -> Self {
        Self {
            slice,
            pos: 0,
            max_record_bytes,
        }
    }

    const fn position(&self) -> usize {
        self.pos
    }

    const fn remaining(&self) -> usize {
        self.slice.len().saturating_sub(self.pos)
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], ParseStepError> {
        let end = self.pos.checked_add(len).ok_or_else(|| {
            ParseStepError::Fatal(PostgresError::Operational(
                "pgoutput frame cursor offset overflowed usize".to_owned(),
            ))
        })?;
        if end > self.max_record_bytes {
            return Err(ParseStepError::Fatal(PostgresError::Operational(format!(
                "pgoutput frame field end {end} exceeds maximum record limit {} bytes",
                self.max_record_bytes
            ))));
        }
        if end > self.slice.len() {
            return Err(ParseStepError::NeedMoreBytes);
        }
        let out = &self.slice[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn read_u8(&mut self) -> Result<u8, ParseStepError> {
        let bytes = self.read_slice(1)?;
        Ok(bytes[0])
    }

    fn read_u16(&mut self) -> Result<u16, ParseStepError> {
        let bytes = self.read_slice(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&mut self) -> Result<i32, ParseStepError> {
        let bytes = self.read_slice(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u32(&mut self) -> Result<u32, ParseStepError> {
        let bytes = self.read_slice(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_cstring(&mut self, field_label: &str) -> Result<&'a str, ParseStepError> {
        let tail = &self.slice[self.pos..];
        let Some(rel_nul) = tail.iter().position(|&b| b == 0) else {
            return Err(ParseStepError::NeedMoreBytes);
        };
        let end = self.pos.checked_add(rel_nul + 1).ok_or_else(|| {
            ParseStepError::Fatal(PostgresError::Operational(
                "pgoutput cstring offset overflowed usize".to_owned(),
            ))
        })?;
        if end > self.max_record_bytes {
            return Err(ParseStepError::Fatal(PostgresError::Operational(format!(
                "pgoutput {field_label} exceeds maximum record limit {} bytes",
                self.max_record_bytes
            ))));
        }
        let raw = &tail[..rel_nul];
        let parsed = std::str::from_utf8(raw).map_err(|err| {
            ParseStepError::Fatal(PostgresError::Protocol(format!(
                "invalid UTF-8 in pgoutput {field_label}: {err}"
            )))
        })?;
        self.pos = end;
        Ok(parsed)
    }
}

fn identity_matches(
    expected_ns: &str,
    expected_name: &str,
    wire_ns: &str,
    wire_name: &str,
) -> bool {
    let ns_ok = expected_ns == wire_ns || (wire_ns.is_empty() && expected_ns == "pg_catalog");
    ns_ok && expected_name == wire_name
}

fn parse_relation_frame(
    cursor: &mut FrameCursor<'_>,
) -> Result<PgOutputRelationSchema, ParseStepError> {
    let relation_oid = cursor.read_u32()?;
    if relation_oid == 0 {
        return Err(ParseStepError::Fatal(PostgresError::Protocol(
            "pgoutput Relation ('R') reported invalid relation_oid 0".to_owned(),
        )));
    }
    let namespace = cursor.read_cstring("Relation.namespace")?.to_owned();
    let name = cursor.read_cstring("Relation.name")?;
    if name.is_empty() {
        return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
            "pgoutput Relation ('R') OID {relation_oid} carried an empty relation name"
        ))));
    }
    let name = name.to_owned();

    let replica_identity = cursor.read_u8()?;
    if !matches!(replica_identity, b'd' | b'f' | b'n' | b'i') {
        return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
            "pgoutput Relation ('R') OID {relation_oid} carried invalid replica_identity 0x{replica_identity:02x}"
        ))));
    }

    let num_cols = usize::from(cursor.read_u16()?);
    let prealloc = num_cols.min(cursor.remaining() / 11);
    let mut columns = Vec::with_capacity(prealloc);
    for col_idx in 0..num_cols {
        let flags = cursor.read_u8()?;
        if flags > 0x01 {
            return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
                "pgoutput Relation ('R') OID {relation_oid} column {col_idx} carried invalid flags 0x{flags:02x}"
            ))));
        }
        let col_name = cursor.read_cstring("Relation.column.name")?;
        if col_name.is_empty() {
            return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
                "pgoutput Relation ('R') OID {relation_oid} column {col_idx} carried an empty name"
            ))));
        }
        let type_oid = cursor.read_u32()?;
        if type_oid == 0 {
            return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
                "pgoutput Relation ('R') OID {relation_oid} column {col_name:?} carried invalid type_oid 0"
            ))));
        }
        let type_modifier = cursor.read_i32()?;
        columns.push(PgOutputColumn {
            is_key: (flags & 0x01) != 0,
            name: col_name.to_owned(),
            type_oid,
            type_modifier,
        });
    }

    Ok(PgOutputRelationSchema {
        relation_oid,
        namespace,
        name,
        replica_identity,
        columns: Arc::from(columns),
    })
}

fn parse_type_frame(cursor: &mut FrameCursor<'_>) -> Result<(u32, String, String), ParseStepError> {
    let type_oid = cursor.read_u32()?;
    if type_oid == 0 {
        return Err(ParseStepError::Fatal(PostgresError::Protocol(
            "pgoutput Type ('Y') reported invalid type_oid 0".to_owned(),
        )));
    }
    let namespace = cursor.read_cstring("Type.namespace")?.to_owned();
    let name = cursor.read_cstring("Type.name")?;
    if name.is_empty() {
        return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
            "pgoutput Type ('Y') OID {type_oid} carried an empty type name"
        ))));
    }
    Ok((type_oid, namespace, name.to_owned()))
}

fn parse_insert_frame(
    cursor: &mut FrameCursor<'_>,
    relations: &HashMap<u32, Arc<PgOutputRelationSchema>>,
    catalog: &TypeCatalog,
    codec_ctx: &CodecContext,
) -> Result<(Arc<PgOutputRelationSchema>, Vec<PgOutputTupleValue>), ParseStepError> {
    let relation_oid = cursor.read_u32()?;
    let Some(relation) = relations.get(&relation_oid).cloned() else {
        return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
            "pgoutput tuple referenced unregistered relation OID {relation_oid}"
        ))));
    };
    let subtag = cursor.read_u8()?;
    if subtag != b'N' {
        return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
            "pgoutput Insert ('I') on relation OID {relation_oid} expected 'N' tuple marker, got 0x{subtag:02x}"
        ))));
    }
    let raw_cells = parse_tuple_data_slices(cursor, relation.columns.len())?;
    let values = decode_tuple_cells(&relation.columns, &raw_cells, catalog, codec_ctx)
        .map_err(ParseStepError::Fatal)?;
    Ok((relation, values))
}

fn parse_tuple_data_slices<'a>(
    cursor: &mut FrameCursor<'a>,
    expected_cols: usize,
) -> Result<Vec<RawWireCell<'a>>, ParseStepError> {
    let n_cols = usize::from(cursor.read_u16()?);
    if n_cols != expected_cols {
        return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
            "pgoutput TupleData column count {n_cols} does not match relation schema width {expected_cols}"
        ))));
    }
    let mut cells = Vec::with_capacity(n_cols);
    for col_idx in 0..n_cols {
        let tag = cursor.read_u8()?;
        match tag {
            b'n' => cells.push(RawWireCell::Null),
            b'u' => cells.push(RawWireCell::UnchangedToast),
            b't' | b'b' => {
                let raw_len = cursor.read_i32()?;
                let Ok(len) = usize::try_from(raw_len) else {
                    return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
                        "pgoutput TupleData column {col_idx} reported negative byte length {raw_len}"
                    ))));
                };
                let data = cursor.read_slice(len)?;
                if tag == b't' {
                    if data.contains(&0) {
                        return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
                            "pgoutput TupleData text column {col_idx} contains an embedded NUL byte"
                        ))));
                    }
                    if let Err(err) = std::str::from_utf8(data) {
                        return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
                            "pgoutput TupleData text column {col_idx} contains invalid UTF-8: {err}"
                        ))));
                    }
                    cells.push(RawWireCell::Text(data));
                } else {
                    cells.push(RawWireCell::Binary(data));
                }
            }
            other => {
                return Err(ParseStepError::Fatal(PostgresError::Protocol(format!(
                    "unknown pgoutput TupleData column tag 0x{other:02x} at column {col_idx}"
                ))));
            }
        }
    }
    Ok(cells)
}

fn decode_tuple_cells(
    columns: &[PgOutputColumn],
    raw_cells: &[RawWireCell<'_>],
    catalog: &TypeCatalog,
    codec_ctx: &CodecContext,
) -> Result<Vec<PgOutputTupleValue>, PostgresError> {
    let mut out = Vec::with_capacity(columns.len());
    for (col, raw) in columns.iter().zip(raw_cells.iter()) {
        let cell = match raw {
            RawWireCell::Null => PgOutputTupleValue::Null,
            RawWireCell::UnchangedToast => PgOutputTupleValue::UnchangedToast,
            RawWireCell::Text(bytes) => PgOutputTupleValue::Value(decode_value_with_catalog(
                col.type_oid,
                Format::Text,
                Some(bytes),
                codec_ctx,
                catalog,
            )?),
            RawWireCell::Binary(bytes) => PgOutputTupleValue::Value(decode_binary_cell(
                col.type_oid,
                bytes,
                codec_ctx,
                catalog,
            )?),
        };
        out.push(cell);
    }
    Ok(out)
}

fn decode_binary_cell(
    type_oid: u32,
    bytes: &[u8],
    codec_ctx: &CodecContext,
    catalog: &TypeCatalog,
) -> Result<PgValue, PostgresError> {
    let base_oid = catalog.resolve_domain_base(type_oid)?;
    let can_decode_rich = supports_binary_with_catalog(base_oid, catalog)
        || base_oid == RECORD_OID
        || matches!(
            catalog.type_info(base_oid),
            Some(TypeInfo {
                kind: TypeKind::Enum,
                ..
            })
        );
    if can_decode_rich {
        decode_value_with_catalog(type_oid, Format::Binary, Some(bytes), codec_ctx, catalog)
    } else {
        Ok(PgValue::Bytes(bytes.to_vec()))
    }
}
