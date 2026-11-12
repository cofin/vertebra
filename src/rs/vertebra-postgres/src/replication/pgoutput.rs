//! Incremental PostgreSQL `pgoutput` v1 decoder, ordered transaction envelopes, and bounded delivery.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};

use super::VerifiedReplicationIdentity;

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
    /// Committed transaction header ('B').
    Begin {
        final_lsn: u64,
        commit_time_pg_us: i64,
        xid: u32,
    },
    /// Replication origin associated with the active transaction ('O').
    Origin {
        origin_lsn: u64,
        origin_name: String,
    },
    /// Updated row with optional prior key or full row ('U').
    Update {
        relation: Arc<PgOutputRelationSchema>,
        old_kind: Option<OldTupleKind>,
        old_tuple: Option<Vec<PgOutputTupleValue>>,
        new_tuple: Vec<PgOutputTupleValue>,
    },
    /// Removed row with its prior key or full row ('D').
    Delete {
        relation: Arc<PgOutputRelationSchema>,
        old_kind: OldTupleKind,
        old_tuple: Vec<PgOutputTupleValue>,
    },
    /// Relation truncation and PostgreSQL truncate options ('T').
    Truncate {
        relations: Vec<Arc<PgOutputRelationSchema>>,
        cascade: bool,
        restart_identity: bool,
    },
    /// Application logical message ('M').
    Message {
        transactional: bool,
        message_lsn: u64,
        prefix: String,
        content: Bytes,
    },
    /// Commit boundary ('C').
    Commit {
        flags: u8,
        commit_lsn: u64,
        commit_end_lsn: u64,
        commit_time_pg_us: i64,
    },
}

/// Default maximum number of queued replication records.
pub const DEFAULT_MAX_QUEUE_RECORDS: usize = 1024;
/// Default aggregate replication queue byte budget.
pub const DEFAULT_MAX_QUEUE_BYTES: usize = 8 * 1024 * 1024;

/// Replica identity representation preceding an old tuple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldTupleKind {
    Key,
    Full,
}

/// Transaction header retained while individual changes stream to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveLogicalTx {
    pub xid: u32,
    pub begin_final_lsn: u64,
    pub commit_time_pg_us: i64,
    pub origin_name: Option<String>,
    pub origin_lsn: Option<u64>,
    pub next_ordinal: u64,
}

/// Stable replay coordinate scoped to a verified server and caller-owned slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationDedupKey {
    pub system_identifier: String,
    pub slot: String,
    pub begin_final_lsn: u64,
    pub transaction_ordinal: u64,
}

/// Owned, incrementally emitted logical replication event.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationRecord {
    Begin {
        dedup: ReplicationDedupKey,
        xid: u32,
        begin_final_lsn: u64,
        commit_time_pg_us: i64,
    },
    Insert {
        dedup: ReplicationDedupKey,
        xid: u32,
        relation: Arc<PgOutputRelationSchema>,
        new_tuple: Vec<PgOutputTupleValue>,
    },
    Update {
        dedup: ReplicationDedupKey,
        xid: u32,
        relation: Arc<PgOutputRelationSchema>,
        old_kind: Option<OldTupleKind>,
        old_tuple: Option<Vec<PgOutputTupleValue>>,
        new_tuple: Vec<PgOutputTupleValue>,
    },
    Delete {
        dedup: ReplicationDedupKey,
        xid: u32,
        relation: Arc<PgOutputRelationSchema>,
        old_kind: OldTupleKind,
        old_tuple: Vec<PgOutputTupleValue>,
    },
    Truncate {
        dedup: ReplicationDedupKey,
        xid: u32,
        relations: Vec<Arc<PgOutputRelationSchema>>,
        cascade: bool,
        restart_identity: bool,
    },
    Message {
        dedup: Option<ReplicationDedupKey>,
        xid: Option<u32>,
        transactional: bool,
        message_lsn: u64,
        prefix: String,
        content: Bytes,
    },
    Commit {
        dedup: ReplicationDedupKey,
        xid: u32,
        flags: u8,
        commit_lsn: u64,
        commit_end_lsn: u64,
        commit_time_pg_us: i64,
        origin_name: Option<String>,
        origin_lsn: Option<u64>,
    },
}

impl ReplicationRecord {
    /// Only a committed transaction exposes an acknowledgeable boundary.
    pub const fn commit_end_lsn(&self) -> Option<u64> {
        match self {
            Self::Commit { commit_end_lsn, .. } => Some(*commit_end_lsn),
            _ => None,
        }
    }

    /// Conservative retained-memory estimate, charging shared schemas to each record.
    pub fn approx_bytes(&self) -> usize {
        let (dedup, payload) = match self {
            Self::Begin { dedup, .. } => (Some(dedup), 0),
            Self::Insert {
                dedup,
                relation,
                new_tuple,
                ..
            } => (Some(dedup), schema_bytes(relation) + tuple_bytes(new_tuple)),
            Self::Update {
                dedup,
                relation,
                old_tuple,
                new_tuple,
                ..
            } => (
                Some(dedup),
                schema_bytes(relation)
                    + old_tuple.as_deref().map_or(0, tuple_bytes)
                    + tuple_bytes(new_tuple),
            ),
            Self::Delete {
                dedup,
                relation,
                old_tuple,
                ..
            } => (Some(dedup), schema_bytes(relation) + tuple_bytes(old_tuple)),
            Self::Truncate {
                dedup, relations, ..
            } => (
                Some(dedup),
                relations
                    .iter()
                    .map(|r| schema_bytes(r) + size_of::<Arc<PgOutputRelationSchema>>())
                    .sum(),
            ),
            Self::Message {
                dedup,
                prefix,
                content,
                ..
            } => (dedup.as_ref(), prefix.len() + content.len()),
            Self::Commit {
                dedup, origin_name, ..
            } => (Some(dedup), origin_name.as_ref().map_or(0, String::len)),
        };
        size_of::<Self>() + payload + dedup.map_or(0, |d| d.system_identifier.len() + d.slot.len())
    }
}

fn schema_bytes(schema: &PgOutputRelationSchema) -> usize {
    size_of::<PgOutputRelationSchema>()
        + schema.namespace.len()
        + schema.name.len()
        + schema
            .columns
            .iter()
            .map(|c| size_of::<PgOutputColumn>() + c.name.len())
            .sum::<usize>()
}
fn tuple_bytes(tuple: &[PgOutputTupleValue]) -> usize {
    std::mem::size_of_val(tuple)
        + tuple
            .iter()
            .map(|v| match v {
                PgOutputTupleValue::Value(v) => value_heap_bytes(v),
                _ => 0,
            })
            .sum::<usize>()
}
fn value_heap_bytes(value: &PgValue) -> usize {
    match value {
        PgValue::Text(v) => v.len(),
        PgValue::Bytes(v) | PgValue::Json(v) | PgValue::BitString { data: v, .. } => v.len(),
        PgValue::Numeric(crate::result::PgNumeric::Finite { digits, .. }) => {
            digits.len() * size_of::<i16>()
        }
        PgValue::TemporalText {
            wire_text,
            date_style,
            time_zone,
            ..
        } => wire_text.len() + date_style.len() + time_zone.len(),
        PgValue::Array {
            values,
            dimensions,
            lower_bounds,
            ..
        } => {
            values.len() * size_of::<PgValue>()
                + values.iter().map(value_heap_bytes).sum::<usize>()
                + (dimensions.len() + lower_bounds.len()) * size_of::<i32>()
        }
        PgValue::Composite {
            field_names,
            field_oids,
            values,
            ..
        } => {
            values.len() * size_of::<PgValue>()
                + values.iter().map(value_heap_bytes).sum::<usize>()
                + field_names
                    .iter()
                    .map(|n| size_of::<String>() + n.len())
                    .sum::<usize>()
                + field_oids.len() * size_of::<u32>()
        }
        PgValue::Vector { values, .. } => values.len() * size_of::<f32>(),
        _ => 0,
    }
}

/// FIFO with independent record-count, aggregate-byte, and individual-record limits.
#[derive(Debug)]
pub struct BoundedReplicationQueue {
    records: VecDeque<(ReplicationRecord, usize)>,
    bytes: usize,
    max_records: usize,
    max_bytes: usize,
    max_record_bytes: usize,
}
impl BoundedReplicationQueue {
    /// Construct an empty bounded queue; zero capacity refuses all records.
    pub fn new(max_records: usize, max_bytes: usize, max_record_bytes: usize) -> Self {
        Self {
            records: VecDeque::new(),
            bytes: 0,
            max_records,
            max_bytes,
            max_record_bytes,
        }
    }
    /// An empty queue admits one individually valid record even above the aggregate byte budget.
    pub fn can_accept(&self, approx_bytes: usize) -> bool {
        approx_bytes <= self.max_record_bytes
            && self.records.len() < self.max_records
            && (self.records.is_empty()
                || self
                    .bytes
                    .checked_add(approx_bytes)
                    .is_some_and(|n| n <= self.max_bytes))
    }
    /// Refuse overflow explicitly without removing any queued record.
    pub fn push(&mut self, record: ReplicationRecord) -> Result<(), PostgresError> {
        let bytes = record.approx_bytes();
        if !self.can_accept(bytes) {
            return Err(PostgresError::Operational(
                "replication queue capacity or record size exceeded".into(),
            ));
        }
        self.bytes += bytes;
        self.records.push_back((record, bytes));
        Ok(())
    }
    /// Remove the next record and release its byte reservation.
    pub fn pop(&mut self) -> Option<ReplicationRecord> {
        self.records.pop_front().map(|(r, bytes)| {
            self.bytes -= bytes;
            r
        })
    }
    /// Number of queued records.
    pub fn len(&self) -> usize {
        self.records.len()
    }
    /// Whether no records remain.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
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
    active_tx: Option<ActiveLogicalTx>,
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
            active_tx: None,
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
            active_tx: None,
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
        self.decode_next_frame_inner(catalog, codec_ctx, true)
            .map(|value| value.map(|(frame, _)| frame))
    }

    fn decode_next_frame_inner(
        &mut self,
        catalog: &mut TypeCatalog,
        codec_ctx: &CodecContext,
        consume_changes: bool,
    ) -> Result<Option<(PgOutputFrame, usize)>, PostgresError> {
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
                b'B' | b'O' | b'U' | b'D' | b'T' | b'M' | b'C' => {
                    parse_change_frame(tag, &mut cursor, &self.relations, catalog, codec_ctx)
                        .map(|frame| (cursor.position(), ParsedBody::Change(frame)))
                }
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
            ParsedBody::Change(frame) => {
                if consume_changes {
                    self.buf.advance(consumed);
                }
                Ok(Some((frame, consumed)))
            }
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
                Ok(Some((PgOutputFrame::Relation(arc_schema), consumed)))
            }
            ParsedBody::Type(type_oid, namespace, name) => {
                self.buf.advance(consumed);
                self.reconcile_type_declaration(catalog, type_oid, &namespace, &name);
                Ok(Some((
                    PgOutputFrame::Type {
                        type_oid,
                        namespace,
                        name,
                    },
                    consumed,
                )))
            }
            ParsedBody::Insert(relation, new_tuple) => {
                if consume_changes {
                    self.buf.advance(consumed);
                }
                Ok(Some((
                    PgOutputFrame::Insert {
                        relation,
                        new_tuple,
                    },
                    consumed,
                )))
            }
        }
    }

    /// Current transaction header, without buffering its already-emitted changes.
    pub fn active_transaction(&self) -> Option<&ActiveLogicalTx> {
        self.active_tx.as_ref()
    }

    /// Decode one ordered event, consuming only metadata and origin frames internally.
    ///
    /// Transaction ordering errors preserve the active transaction and pending frame bytes.
    pub fn next_record(
        &mut self,
        identity: &VerifiedReplicationIdentity,
        catalog: &mut TypeCatalog,
        codec_ctx: &CodecContext,
    ) -> Result<Option<ReplicationRecord>, PostgresError> {
        loop {
            let Some((frame, consumed)) =
                self.decode_next_frame_inner(catalog, codec_ctx, false)?
            else {
                return Ok(None);
            };
            let metadata = matches!(
                frame,
                PgOutputFrame::Relation(_) | PgOutputFrame::Type { .. }
            );
            let record = self.record_from_frame(frame, identity)?;
            if !metadata {
                self.buf.advance(consumed);
            }
            if record.is_some() {
                return Ok(record);
            }
        }
    }

    fn record_from_frame(
        &mut self,
        frame: PgOutputFrame,
        identity: &VerifiedReplicationIdentity,
    ) -> Result<Option<ReplicationRecord>, PostgresError> {
        if matches!(
            frame,
            PgOutputFrame::Relation(_) | PgOutputFrame::Type { .. }
        ) {
            return Ok(None);
        }
        if let PgOutputFrame::Begin {
            final_lsn,
            commit_time_pg_us,
            xid,
        } = frame
        {
            if self.active_tx.is_some() {
                return Err(PostgresError::Protocol("nested pgoutput Begin".into()));
            }
            let dedup = ReplicationDedupKey {
                system_identifier: identity.system_identifier.clone(),
                slot: identity.slot.clone(),
                begin_final_lsn: final_lsn,
                transaction_ordinal: 0,
            };
            let record = ReplicationRecord::Begin {
                dedup,
                xid,
                begin_final_lsn: final_lsn,
                commit_time_pg_us,
            };
            ensure_record_size(&record, self.max_record_bytes)?;
            self.active_tx = Some(ActiveLogicalTx {
                xid,
                begin_final_lsn: final_lsn,
                commit_time_pg_us,
                origin_name: None,
                origin_lsn: None,
                next_ordinal: 1,
            });
            return Ok(Some(record));
        }
        if let PgOutputFrame::Message {
            transactional: false,
            message_lsn,
            prefix,
            content,
        } = frame
        {
            let record = ReplicationRecord::Message {
                dedup: None,
                xid: self.active_tx.as_ref().map(|tx| tx.xid),
                transactional: false,
                message_lsn,
                prefix,
                content,
            };
            ensure_record_size(&record, self.max_record_bytes)?;
            return Ok(Some(record));
        }
        let tx = self.active_tx.as_mut().ok_or_else(|| {
            PostgresError::Protocol("pgoutput change outside Begin/Commit transaction".into())
        })?;
        if let PgOutputFrame::Origin {
            origin_lsn,
            origin_name,
        } = frame
        {
            tx.origin_lsn = Some(origin_lsn);
            tx.origin_name = Some(origin_name);
            return Ok(None);
        }
        if let PgOutputFrame::Commit { commit_lsn, .. } = &frame
            && *commit_lsn != tx.begin_final_lsn
        {
            return Err(PostgresError::Protocol(
                "pgoutput Commit LSN does not match Begin final LSN".into(),
            ));
        }
        let next_ordinal = tx.next_ordinal.checked_add(1).ok_or_else(|| {
            PostgresError::Operational("pgoutput transaction ordinal overflow".into())
        })?;
        let dedup = ReplicationDedupKey {
            system_identifier: identity.system_identifier.clone(),
            slot: identity.slot.clone(),
            begin_final_lsn: tx.begin_final_lsn,
            transaction_ordinal: tx.next_ordinal,
        };
        let xid = tx.xid;
        let record = match frame {
            PgOutputFrame::Insert {
                relation,
                new_tuple,
            } => ReplicationRecord::Insert {
                dedup,
                xid,
                relation,
                new_tuple,
            },
            PgOutputFrame::Update {
                relation,
                old_kind,
                old_tuple,
                new_tuple,
            } => ReplicationRecord::Update {
                dedup,
                xid,
                relation,
                old_kind,
                old_tuple,
                new_tuple,
            },
            PgOutputFrame::Delete {
                relation,
                old_kind,
                old_tuple,
            } => ReplicationRecord::Delete {
                dedup,
                xid,
                relation,
                old_kind,
                old_tuple,
            },
            PgOutputFrame::Truncate {
                relations,
                cascade,
                restart_identity,
            } => ReplicationRecord::Truncate {
                dedup,
                xid,
                relations,
                cascade,
                restart_identity,
            },
            PgOutputFrame::Message {
                transactional,
                message_lsn,
                prefix,
                content,
            } => ReplicationRecord::Message {
                dedup: Some(dedup),
                xid: Some(xid),
                transactional,
                message_lsn,
                prefix,
                content,
            },
            PgOutputFrame::Commit {
                flags,
                commit_lsn,
                commit_end_lsn,
                commit_time_pg_us,
            } => ReplicationRecord::Commit {
                dedup,
                xid,
                flags,
                commit_lsn,
                commit_end_lsn,
                commit_time_pg_us,
                origin_name: tx.origin_name.clone(),
                origin_lsn: tx.origin_lsn,
            },
            _ => unreachable!("metadata and Begin handled before change emission"),
        };
        ensure_record_size(&record, self.max_record_bytes)?;
        if matches!(record, ReplicationRecord::Commit { .. }) {
            self.active_tx = None;
        } else {
            tx.next_ordinal = next_ordinal;
        }
        Ok(Some(record))
    }

    /// Clear partial accumulator bytes and session-scoped relation state on transport disconnect.
    pub fn reset_on_disconnect(&mut self) {
        self.buf.clear();
        self.relations.clear();
        self.active_tx = None;
    }

    /// Verify that no truncated partial `pgoutput` record remains when backend `CopyDone` arrives cleanly.
    pub fn finish_clean_eof(&self) -> Result<(), PostgresError> {
        if self.buf.is_empty() && self.active_tx.is_none() {
            Ok(())
        } else {
            Err(PostgresError::Protocol(format!(
                "clean pgoutput CopyDone received with {} trailing incomplete record bytes or an unfinished transaction",
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
    Change(PgOutputFrame),
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

    fn read_u64(&mut self) -> Result<u64, ParseStepError> {
        let bytes = self.read_slice(8)?;
        Ok(u64::from_be_bytes(
            bytes.try_into().expect("eight-byte slice"),
        ))
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

fn protocol_error(message: &str) -> ParseStepError {
    ParseStepError::Fatal(PostgresError::Protocol(message.to_owned()))
}

fn parse_change_frame(
    tag: u8,
    cursor: &mut FrameCursor<'_>,
    relations: &HashMap<u32, Arc<PgOutputRelationSchema>>,
    catalog: &TypeCatalog,
    codec_ctx: &CodecContext,
) -> Result<PgOutputFrame, ParseStepError> {
    Ok(match tag {
        b'B' => PgOutputFrame::Begin {
            final_lsn: cursor.read_u64()?,
            commit_time_pg_us: cursor.read_u64()? as i64,
            xid: cursor.read_u32()?,
        },
        b'O' => PgOutputFrame::Origin {
            origin_lsn: cursor.read_u64()?,
            origin_name: cursor.read_cstring("Origin.name")?.to_owned(),
        },
        b'C' => {
            let flags = cursor.read_u8()?;
            if flags != 0 {
                return Err(protocol_error("pgoutput Commit flags must be zero"));
            }
            let commit_lsn = cursor.read_u64()?;
            let commit_end_lsn = cursor.read_u64()?;
            if commit_end_lsn < commit_lsn {
                return Err(protocol_error(
                    "pgoutput commit end LSN precedes commit LSN",
                ));
            }
            PgOutputFrame::Commit {
                flags,
                commit_lsn,
                commit_end_lsn,
                commit_time_pg_us: cursor.read_u64()? as i64,
            }
        }
        b'M' => {
            let flags = cursor.read_u8()?;
            if flags > 1 {
                return Err(protocol_error("pgoutput Message has reserved flags"));
            }
            let message_lsn = cursor.read_u64()?;
            let prefix = cursor.read_cstring("Message.prefix")?.to_owned();
            let len = cursor.read_u32()? as usize;
            let content = Bytes::copy_from_slice(cursor.read_slice(len)?);
            PgOutputFrame::Message {
                transactional: flags == 1,
                message_lsn,
                prefix,
                content,
            }
        }
        b'T' => {
            let count = cursor.read_u32()? as usize;
            let flags = cursor.read_u8()?;
            if count == 0 || flags > 3 {
                return Err(protocol_error(
                    "pgoutput Truncate needs relations and valid option bits",
                ));
            }
            // Validate the complete OID list before allocating or resolving relation schemas.
            let raw =
                cursor.read_slice(count.checked_mul(4).ok_or_else(|| {
                    protocol_error("pgoutput Truncate relation count overflow")
                })?)?;
            let mut schemas = Vec::with_capacity(count);
            for bytes in raw.as_chunks::<4>().0 {
                let oid = u32::from_be_bytes(*bytes);
                schemas.push(relations.get(&oid).cloned().ok_or_else(|| {
                    protocol_error("pgoutput Truncate referenced unregistered relation OID")
                })?);
            }
            PgOutputFrame::Truncate {
                relations: schemas,
                cascade: flags & 1 != 0,
                restart_identity: flags & 2 != 0,
            }
        }
        b'U' | b'D' => {
            let oid = cursor.read_u32()?;
            let relation = relations.get(&oid).cloned().ok_or_else(|| {
                protocol_error("pgoutput tuple referenced unregistered relation OID")
            })?;
            let marker = cursor.read_u8()?;
            let old_kind = match marker {
                b'K' => Some(OldTupleKind::Key),
                b'O' => Some(OldTupleKind::Full),
                b'N' if tag == b'U' => None,
                _ => return Err(protocol_error("invalid pgoutput old/new tuple marker")),
            };
            let old_tuple = if old_kind.is_some() {
                let raw = parse_tuple_data_slices(cursor, relation.columns.len())?;
                Some(
                    decode_tuple_cells(&relation.columns, &raw, catalog, codec_ctx)
                        .map_err(ParseStepError::Fatal)?,
                )
            } else {
                None
            };
            if tag == b'D' {
                PgOutputFrame::Delete {
                    relation,
                    old_kind: old_kind.expect("Delete has old tuple"),
                    old_tuple: old_tuple.expect("Delete has old tuple"),
                }
            } else {
                if old_kind.is_some() && cursor.read_u8()? != b'N' {
                    return Err(protocol_error("pgoutput Update expected new tuple marker"));
                }
                let raw = parse_tuple_data_slices(cursor, relation.columns.len())?;
                let new_tuple = decode_tuple_cells(&relation.columns, &raw, catalog, codec_ctx)
                    .map_err(ParseStepError::Fatal)?;
                PgOutputFrame::Update {
                    relation,
                    old_kind,
                    old_tuple,
                    new_tuple,
                }
            }
        }
        _ => unreachable!("change frame tag selected by decoder"),
    })
}

fn ensure_record_size(record: &ReplicationRecord, max_bytes: usize) -> Result<(), PostgresError> {
    if record.approx_bytes() > max_bytes {
        return Err(PostgresError::Operational(
            "decoded replication record exceeds maximum byte limit".into(),
        ));
    }
    Ok(())
}
