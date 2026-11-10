//! Incremental PostgreSQL Protocol 3.0 binary `COPY` framing (`BinaryCopyDecoder`,
//! `BinaryCopyEncoder`), exclusive `COPY TO STDOUT` streaming (`CopyOut`), and duplex
//! `COPY FROM STDIN` chunked transmission with `CopyFail` abort recovery (`CopyFromStream`).

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_protocol::message::backend::{Message, NotificationResponseBody, ParameterStatusBody};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::time::Instant;

use crate::codecs::{CodecContext, TEXT_OID, encode_value_with_catalog};
use crate::errors::PostgresError;
use crate::extended::Parameter;
use crate::protocol::{MAX_BACKEND_FRAME_LENGTH, read_message};
use crate::query::parse_error_response;
use crate::result::{Column, Format, PgValue, TransactionStatus};
use crate::session::{
    CopySessionLease, ExchangeState, OperationGeneration, PostgresConnection, SessionAccess,
    SessionHandle, validate_single_copy_sql,
};
use crate::transaction::TransactionOwner;
use crate::types::TypeCatalog;

/// Canonical 11-byte PostgreSQL binary `COPY` file signature (`PGCOPY\n\xff\r\n\0`).
pub const PGCOPY_SIGNATURE: [u8; 11] = *b"PGCOPY\n\xff\r\n\0";

/// Default maximum decoded or encoded payload bytes per `COPY` row (`16 MiB`).
pub const DEFAULT_MAX_COPY_ROW_BYTES: usize = 16 * 1024 * 1024;

/// Default maximum byte length of a single binary `COPY` column payload (`16 MiB`).
pub const DEFAULT_MAX_COPY_FIELD_BYTES: usize = 16 * 1024 * 1024;

/// Default maximum `PGCOPY` binary header extension length (`64 KiB`).
pub const DEFAULT_MAX_COPY_EXTENSION_BYTES: usize = 64 * 1024;

/// Maximum payload size of a single outbound `CopyData` (`'d'`) wire frame (`64 KiB`).
pub const COPY_DATA_CHUNK_BYTES: usize = 64 * 1024;

/// Maximum UTF-8 byte length sent inside a `CopyFail` (`'f'`) message (`256` bytes).
pub const MAX_COPY_FAIL_REASON_BYTES: usize = 256;

const PGCOPY_HEADER_BYTES: usize = 19;
const CRITICAL_COPY_FLAGS_MASK: u32 = 0xFFFF_0000;
const DECODER_PUSH_WINDOW_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DecoderState {
    Header,
    Extension {
        remaining: usize,
    },
    FieldCount,
    FieldLength {
        remaining_fields: usize,
    },
    FieldBytes {
        remaining_fields: usize,
        remaining_bytes: usize,
    },
    Done,
    Failed,
}

/// Incremental PostgreSQL binary `COPY` (`PGCOPY`) row decoder retaining at most one in-progress row.
#[derive(Debug)]
pub struct BinaryCopyDecoder {
    expected_columns: usize,
    max_row_bytes: usize,
    max_field_bytes: usize,
    max_ext_bytes: usize,
    state: DecoderState,
    buf: BytesMut,
    field_buf: BytesMut,
    current_row: Vec<Option<Bytes>>,
    current_row_bytes: usize,
    ready_row: Option<Vec<Option<Bytes>>>,
    rows_decoded: u64,
}

impl BinaryCopyDecoder {
    /// Construct an incremental decoder expecting `expected_columns` per tuple with default caps
    /// (`16 MiB` row/field, `64 KiB` header extension).
    pub fn new(expected_columns: usize) -> Self {
        Self::with_limits(
            expected_columns,
            DEFAULT_MAX_COPY_ROW_BYTES,
            DEFAULT_MAX_COPY_FIELD_BYTES,
            DEFAULT_MAX_COPY_EXTENSION_BYTES,
        )
    }

    /// Construct an incremental decoder with explicit row, field, and extension byte ceilings.
    pub fn with_limits(
        expected_columns: usize,
        max_row_bytes: usize,
        max_field_bytes: usize,
        max_ext_bytes: usize,
    ) -> Self {
        Self {
            expected_columns,
            max_row_bytes,
            max_field_bytes,
            max_ext_bytes,
            state: DecoderState::Header,
            buf: BytesMut::new(),
            field_buf: BytesMut::new(),
            current_row: Vec::new(),
            current_row_bytes: 0,
            ready_row: None,
            rows_decoded: 0,
        }
    }

    /// Feed a `CopyData` payload chunk into the incremental state machine.
    pub fn push(&mut self, chunk: &[u8]) -> Result<(), PostgresError> {
        if self.state == DecoderState::Failed {
            return Err(PostgresError::Data(
                "BinaryCopyDecoder cannot accept data after a prior framing failure".to_string(),
            ));
        }
        if chunk.is_empty() {
            return Ok(());
        }
        if self.state == DecoderState::Done {
            self.state = DecoderState::Failed;
            return Err(PostgresError::Data(
                "unexpected trailing bytes after PGCOPY binary trailer (-1)".to_string(),
            ));
        }

        let mut offset = 0usize;
        while offset < chunk.len() {
            if self.ready_row.is_some() {
                let remaining = &chunk[offset..];
                self.buf.extend_from_slice(remaining);
                return Ok(());
            }
            let take = (chunk.len() - offset).min(DECODER_PUSH_WINDOW_BYTES);
            self.buf.extend_from_slice(&chunk[offset..offset + take]);
            offset += take;
            self.advance_until_row_or_need_more()?;
        }
        Ok(())
    }

    /// Pull the next decoded row (`Some(fields)`) if complete, or `Ok(None)` when awaiting more
    /// `CopyData` bytes or once the `-1i16` trailer has been consumed.
    pub fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, PostgresError> {
        if let Some(row) = self.ready_row.take() {
            return Ok(Some(row));
        }
        if self.state == DecoderState::Failed {
            return Err(PostgresError::Data(
                "BinaryCopyDecoder is in failed state".to_string(),
            ));
        }
        self.advance_until_row_or_need_more()?;
        Ok(self.ready_row.take())
    }

    /// Verify that the `PGCOPY` header, all tuples, and the exact `-1i16` trailer completed with
    /// zero truncated or trailing bytes, returning the total decoded row count.
    pub fn finish(mut self) -> Result<u64, PostgresError> {
        if self.state == DecoderState::Failed {
            return Err(PostgresError::Data(
                "BinaryCopyDecoder cannot finish after a framing error".to_string(),
            ));
        }
        if self.ready_row.is_some() {
            return Err(PostgresError::Data(
                "BinaryCopyDecoder finished with an unconsumed decoded row".to_string(),
            ));
        }
        if !self.buf.is_empty() && self.state != DecoderState::Done {
            self.advance_until_row_or_need_more()?;
            if self.ready_row.is_some() {
                return Err(PostgresError::Data(
                    "BinaryCopyDecoder finished with unread row data in buffer".to_string(),
                ));
            }
        }
        match self.state {
            DecoderState::Done => {
                if !self.buf.is_empty() {
                    return Err(PostgresError::Data(
                        "unexpected trailing bytes after PGCOPY binary trailer (-1)".to_string(),
                    ));
                }
                Ok(self.rows_decoded)
            }
            DecoderState::Header => Err(PostgresError::Data(
                "truncated PGCOPY binary header before CopyDone".to_string(),
            )),
            DecoderState::Extension { .. } => Err(PostgresError::Data(
                "truncated PGCOPY binary header extension before CopyDone".to_string(),
            )),
            DecoderState::FieldCount => Err(PostgresError::Data(
                "missing or truncated PGCOPY -1 trailer before CopyDone".to_string(),
            )),
            DecoderState::FieldLength { .. } | DecoderState::FieldBytes { .. } => Err(
                PostgresError::Data("truncated PGCOPY binary row before CopyDone".to_string()),
            ),
            DecoderState::Failed => Err(PostgresError::Data(
                "BinaryCopyDecoder cannot finish after failure".to_string(),
            )),
        }
    }

    fn fail(&mut self, msg: impl Into<String>) -> Result<(), PostgresError> {
        self.state = DecoderState::Failed;
        self.buf.clear();
        self.field_buf.clear();
        self.current_row.clear();
        self.ready_row = None;
        Err(PostgresError::Data(msg.into()))
    }

    fn complete_current_field(
        &mut self,
        value: Option<Bytes>,
        remaining_fields: usize,
    ) -> Result<(), PostgresError> {
        self.current_row.push(value);
        let next_remaining = remaining_fields - 1;
        if next_remaining == 0 {
            self.rows_decoded = self.rows_decoded.checked_add(1).ok_or_else(|| {
                self.state = DecoderState::Failed;
                PostgresError::Data("PGCOPY decoded row counter overflowed u64".to_string())
            })?;
            self.current_row_bytes = 0;
            self.state = DecoderState::FieldCount;
            self.ready_row = Some(std::mem::take(&mut self.current_row));
        } else {
            self.state = DecoderState::FieldLength {
                remaining_fields: next_remaining,
            };
        }
        Ok(())
    }

    fn advance_until_row_or_need_more(&mut self) -> Result<(), PostgresError> {
        while self.ready_row.is_none() {
            match self.state {
                DecoderState::Header => {
                    let check_sig = self.buf.len().min(PGCOPY_SIGNATURE.len());
                    if self.buf[..check_sig] != PGCOPY_SIGNATURE[..check_sig] {
                        return self.fail("invalid PGCOPY binary signature");
                    }
                    if self.buf.len() < PGCOPY_HEADER_BYTES {
                        return Ok(());
                    }
                    let flags = u32::from_be_bytes([
                        self.buf[11],
                        self.buf[12],
                        self.buf[13],
                        self.buf[14],
                    ]);
                    if flags & CRITICAL_COPY_FLAGS_MASK != 0 {
                        return self.fail(format!(
                            "unsupported critical or OID flags 0x{flags:08x} in PGCOPY binary header"
                        ));
                    }
                    let raw_ext = i32::from_be_bytes([
                        self.buf[15],
                        self.buf[16],
                        self.buf[17],
                        self.buf[18],
                    ]);
                    if raw_ext < 0 {
                        return self
                            .fail(format!("negative PGCOPY header extension length {raw_ext}"));
                    }
                    let ext_len = raw_ext as usize;
                    if ext_len > self.max_ext_bytes {
                        return self.fail(format!(
                            "PGCOPY header extension length {ext_len} exceeds cap {}",
                            self.max_ext_bytes
                        ));
                    }
                    self.buf.advance(PGCOPY_HEADER_BYTES);
                    self.state = if ext_len == 0 {
                        DecoderState::FieldCount
                    } else {
                        DecoderState::Extension { remaining: ext_len }
                    };
                }
                DecoderState::Extension { mut remaining } => {
                    if self.buf.is_empty() {
                        return Ok(());
                    }
                    let drop_n = remaining.min(self.buf.len());
                    self.buf.advance(drop_n);
                    remaining -= drop_n;
                    self.state = if remaining == 0 {
                        DecoderState::FieldCount
                    } else {
                        DecoderState::Extension { remaining }
                    };
                }
                DecoderState::FieldCount => {
                    if self.buf.len() < 2 {
                        return Ok(());
                    }
                    let count = i16::from_be_bytes([self.buf[0], self.buf[1]]);
                    self.buf.advance(2);
                    if count == -1 {
                        if !self.buf.is_empty() {
                            return self.fail(
                                "unexpected trailing bytes after PGCOPY binary trailer (-1)",
                            );
                        }
                        self.state = DecoderState::Done;
                        return Ok(());
                    }
                    if count < -1 {
                        return self.fail(format!("invalid negative PGCOPY field count {count}"));
                    }
                    let cols = count as usize;
                    if cols != self.expected_columns {
                        return self.fail(format!(
                            "PGCOPY tuple field count {cols} does not match expected column count {}",
                            self.expected_columns
                        ));
                    }
                    self.current_row.clear();
                    self.current_row.reserve(cols.min(64));
                    self.current_row_bytes = 0;
                    if cols == 0 {
                        self.rows_decoded = self.rows_decoded.checked_add(1).ok_or_else(|| {
                            self.state = DecoderState::Failed;
                            PostgresError::Data(
                                "PGCOPY decoded row counter overflowed u64".to_string(),
                            )
                        })?;
                        self.ready_row = Some(Vec::new());
                        return Ok(());
                    }
                    self.state = DecoderState::FieldLength {
                        remaining_fields: cols,
                    };
                }
                DecoderState::FieldLength { remaining_fields } => {
                    if self.buf.len() < 4 {
                        return Ok(());
                    }
                    let flen =
                        i32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
                    self.buf.advance(4);
                    if flen == -1 {
                        self.complete_current_field(None, remaining_fields)?;
                    } else if flen < -1 {
                        return self.fail(format!("invalid negative PGCOPY field length {flen}"));
                    } else if flen == 0 {
                        self.complete_current_field(Some(Bytes::new()), remaining_fields)?;
                    } else {
                        let flen_usize = flen as usize;
                        if flen_usize > self.max_field_bytes {
                            return self.fail(format!(
                                "PGCOPY field length {flen_usize} exceeds max_field_bytes {}",
                                self.max_field_bytes
                            ));
                        }
                        let Some(next_row_bytes) = self.current_row_bytes.checked_add(flen_usize)
                        else {
                            return self.fail("PGCOPY cumulative row byte length overflow");
                        };
                        if next_row_bytes > self.max_row_bytes {
                            return self.fail(format!(
                                "PGCOPY row payload size {next_row_bytes} exceeds max_row_bytes {}",
                                self.max_row_bytes
                            ));
                        }
                        self.current_row_bytes = next_row_bytes;
                        if self.buf.len() >= flen_usize {
                            let bytes = self.buf.split_to(flen_usize).freeze();
                            self.complete_current_field(Some(bytes), remaining_fields)?;
                        } else {
                            self.field_buf.clear();
                            if !self.buf.is_empty() {
                                let avail = self.buf.len();
                                self.field_buf.extend_from_slice(&self.buf);
                                self.buf.clear();
                                self.state = DecoderState::FieldBytes {
                                    remaining_fields,
                                    remaining_bytes: flen_usize - avail,
                                };
                            } else {
                                self.state = DecoderState::FieldBytes {
                                    remaining_fields,
                                    remaining_bytes: flen_usize,
                                };
                            }
                            return Ok(());
                        }
                    }
                }
                DecoderState::FieldBytes {
                    remaining_fields,
                    mut remaining_bytes,
                } => {
                    if self.buf.is_empty() {
                        return Ok(());
                    }
                    let take = remaining_bytes.min(self.buf.len());
                    self.field_buf.extend_from_slice(&self.buf[..take]);
                    self.buf.advance(take);
                    remaining_bytes -= take;
                    if remaining_bytes == 0 {
                        let completed = self.field_buf.split().freeze();
                        self.complete_current_field(Some(completed), remaining_fields)?;
                    } else {
                        self.state = DecoderState::FieldBytes {
                            remaining_fields,
                            remaining_bytes,
                        };
                        return Ok(());
                    }
                }
                DecoderState::Done => {
                    if !self.buf.is_empty() {
                        return self
                            .fail("unexpected trailing bytes after PGCOPY binary trailer (-1)");
                    }
                    return Ok(());
                }
                DecoderState::Failed => {
                    return Err(PostgresError::Data(
                        "BinaryCopyDecoder is in failed state".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Incremental PostgreSQL binary `COPY` (`PGCOPY`) tuple encoder delegating field serialization
/// to [`encode_value_with_catalog`] in [`Format::Binary`] without `DataRow` framing.
#[derive(Debug)]
pub struct BinaryCopyEncoder {
    expected_columns: usize,
    max_row_bytes: usize,
    max_field_bytes: usize,
    header_written: bool,
    finished: bool,
    row_scratch: BytesMut,
    field_scratch: BytesMut,
}

impl BinaryCopyEncoder {
    /// Construct a binary `COPY` encoder for `expected_columns` (`0..=i16::MAX`).
    pub fn new(expected_columns: usize) -> Result<Self, PostgresError> {
        Self::with_limits(
            expected_columns,
            DEFAULT_MAX_COPY_ROW_BYTES,
            DEFAULT_MAX_COPY_FIELD_BYTES,
        )
    }

    /// Construct a binary `COPY` encoder with explicit row and field byte caps.
    pub fn with_limits(
        expected_columns: usize,
        max_row_bytes: usize,
        max_field_bytes: usize,
    ) -> Result<Self, PostgresError> {
        if i16::try_from(expected_columns).is_err() {
            return Err(PostgresError::Data(format!(
                "PGCOPY expected_columns {expected_columns} exceeds i16::MAX ({})",
                i16::MAX
            )));
        }
        Ok(Self {
            expected_columns,
            max_row_bytes,
            max_field_bytes,
            header_written: false,
            finished: false,
            row_scratch: BytesMut::new(),
            field_scratch: BytesMut::new(),
        })
    }

    /// Idempotently write the 19-byte `PGCOPY` header (`signature + 0u32 flags + 0u32 ext_len`).
    pub fn write_header(&mut self, out: &mut BytesMut) {
        if self.header_written {
            return;
        }
        out.reserve(PGCOPY_HEADER_BYTES);
        out.extend_from_slice(&PGCOPY_SIGNATURE);
        out.put_u32(0);
        out.put_u32(0);
        self.header_written = true;
    }

    /// Encode one row of [`PgValue`] cells using [`encode_value_with_catalog`] in [`Format::Binary`],
    /// validating arity, binary type compatibility, `TemporalText` refusal, and byte ceilings before
    /// appending to `out`.
    pub fn encode_row_values(
        &mut self,
        column_oids: &[u32],
        row: &[PgValue],
        ctx: &CodecContext,
        catalog: &TypeCatalog,
        out: &mut BytesMut,
    ) -> Result<(), PostgresError> {
        if self.finished {
            return Err(PostgresError::Data(
                "cannot encode row after PGCOPY binary trailer was written".to_string(),
            ));
        }
        if column_oids.len() != self.expected_columns {
            return Err(PostgresError::Data(format!(
                "COPY column_oids length {} does not match expected column count {}",
                column_oids.len(),
                self.expected_columns
            )));
        }
        if row.len() != self.expected_columns {
            return Err(PostgresError::Data(format!(
                "COPY row length {} does not match expected column count {}",
                row.len(),
                self.expected_columns
            )));
        }

        let mut field_ctx = ctx.clone();
        field_ctx.max_field_bytes = field_ctx.max_field_bytes.min(self.max_field_bytes);

        self.row_scratch.clear();
        self.row_scratch
            .put_i16(i16::try_from(self.expected_columns).expect("validated <= i16::MAX"));
        let mut row_payload_bytes = 0usize;

        for (val, &oid) in row.iter().zip(column_oids.iter()) {
            if matches!(val, PgValue::Null) {
                self.row_scratch.put_i32(-1);
                continue;
            }
            self.field_scratch.clear();
            encode_value_with_catalog(
                val,
                oid,
                Format::Binary,
                &mut self.field_scratch,
                &field_ctx,
                catalog,
            )?;
            let flen = self.field_scratch.len();
            if flen > self.max_field_bytes {
                self.field_scratch.clear();
                self.row_scratch.clear();
                return Err(PostgresError::Data(format!(
                    "encoded COPY field length {flen} exceeds max_field_bytes {}",
                    self.max_field_bytes
                )));
            }
            let flen_i32 = i32::try_from(flen).map_err(|_| {
                PostgresError::Data(format!("encoded COPY field length {flen} exceeds i32::MAX"))
            })?;
            row_payload_bytes = row_payload_bytes.checked_add(flen).ok_or_else(|| {
                PostgresError::Data("encoded COPY row payload size overflowed".to_string())
            })?;
            if row_payload_bytes > self.max_row_bytes {
                self.field_scratch.clear();
                self.row_scratch.clear();
                return Err(PostgresError::Data(format!(
                    "encoded COPY row payload {row_payload_bytes} exceeds max_row_bytes {}",
                    self.max_row_bytes
                )));
            }
            self.row_scratch.put_i32(flen_i32);
            self.row_scratch.extend_from_slice(&self.field_scratch);
        }

        self.write_header(out);
        out.extend_from_slice(&self.row_scratch);
        self.row_scratch.clear();
        self.field_scratch.clear();
        Ok(())
    }

    /// Encode one row of pre-serialized binary field payloads (`None` -> `-1i32` NULL) into `out`.
    pub fn encode_raw_row(
        &mut self,
        fields: &[Option<&[u8]>],
        out: &mut BytesMut,
    ) -> Result<(), PostgresError> {
        if self.finished {
            return Err(PostgresError::Data(
                "cannot encode raw row after PGCOPY binary trailer was written".to_string(),
            ));
        }
        if fields.len() != self.expected_columns {
            return Err(PostgresError::Data(format!(
                "COPY raw row length {} does not match expected column count {}",
                fields.len(),
                self.expected_columns
            )));
        }

        let mut row_payload_bytes = 0usize;
        let mut wire_tuple_bytes = 2usize;
        for field in fields {
            wire_tuple_bytes = wire_tuple_bytes
                .checked_add(4)
                .ok_or_else(|| PostgresError::Data("COPY tuple frame overflow".to_string()))?;
            if let Some(slice) = field {
                if slice.len() > self.max_field_bytes {
                    return Err(PostgresError::Data(format!(
                        "COPY raw field length {} exceeds max_field_bytes {}",
                        slice.len(),
                        self.max_field_bytes
                    )));
                }
                if i32::try_from(slice.len()).is_err() {
                    return Err(PostgresError::Data(format!(
                        "COPY raw field length {} exceeds i32::MAX",
                        slice.len()
                    )));
                }
                row_payload_bytes =
                    row_payload_bytes.checked_add(slice.len()).ok_or_else(|| {
                        PostgresError::Data("COPY cumulative row byte length overflow".to_string())
                    })?;
                if row_payload_bytes > self.max_row_bytes {
                    return Err(PostgresError::Data(format!(
                        "COPY raw row payload {row_payload_bytes} exceeds max_row_bytes {}",
                        self.max_row_bytes
                    )));
                }
                wire_tuple_bytes = wire_tuple_bytes
                    .checked_add(slice.len())
                    .ok_or_else(|| PostgresError::Data("COPY tuple frame overflow".to_string()))?;
            }
        }

        self.write_header(out);
        out.reserve(wire_tuple_bytes);
        out.put_i16(i16::try_from(self.expected_columns).expect("validated <= i16::MAX"));
        for field in fields {
            match field {
                None => out.put_i32(-1),
                Some(slice) => {
                    out.put_i32(i32::try_from(slice.len()).expect("validated <= i32::MAX"));
                    out.extend_from_slice(slice);
                }
            }
        }
        Ok(())
    }

    /// Write the `PGCOPY` header if no rows were written yet and append the single `-1i16` (`0xFFFF`) trailer.
    pub fn finish_trailer(&mut self, out: &mut BytesMut) -> Result<(), PostgresError> {
        if self.finished {
            return Err(PostgresError::Data(
                "PGCOPY binary trailer (-1) has already been written".to_string(),
            ));
        }
        self.write_header(out);
        out.put_i16(-1);
        self.finished = true;
        Ok(())
    }

    pub(crate) const fn is_header_written(&self) -> bool {
        self.header_written
    }

    pub(crate) fn set_header_written(&mut self, written: bool) {
        self.header_written = written;
    }
}

/// Parse an exact `"COPY <n>"` command completion tag into its `u64` row count.
pub(crate) fn parse_copy_command_tag(tag: &str) -> Result<u64, PostgresError> {
    let rest = tag.strip_prefix("COPY ").ok_or_else(|| {
        PostgresError::Protocol(format!(
            "expected `COPY <n>` CommandComplete tag, got {tag:?}"
        ))
    })?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return Err(PostgresError::Protocol(format!(
            "malformed row count in COPY CommandComplete tag {tag:?}"
        )));
    }
    rest.parse::<u64>().map_err(|e| {
        PostgresError::Protocol(format!(
            "row count overflow in COPY CommandComplete tag {tag:?}: {e}"
        ))
    })
}

/// Sanitize a `CopyFail` abort diagnostic to NUL-free valid UTF-8 of at most [`MAX_COPY_FAIL_REASON_BYTES`] (`256`) bytes.
pub(crate) fn sanitize_copy_fail_reason(reason: &str) -> String {
    let cleaned = if reason.is_empty() {
        "COPY FROM aborted".to_string()
    } else if reason.as_bytes().contains(&0) {
        reason.replace('\0', "?")
    } else {
        reason.to_owned()
    };
    if cleaned.len() <= MAX_COPY_FAIL_REASON_BYTES {
        return cleaned;
    }
    let mut end = MAX_COPY_FAIL_REASON_BYTES;
    while end > 0 && !cleaned.is_char_boundary(end) {
        end -= 1;
    }
    cleaned[..end].to_owned()
}

/// Exclusive streaming handle over `COPY ... TO STDOUT (FORMAT binary)`.
pub struct CopyOut {
    lease: Option<CopySessionLease>,
    decoder: Option<BinaryCopyDecoder>,
    column_count: usize,
    rows_delivered: u64,
    exhausted: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
}

impl std::fmt::Debug for CopyOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyOut")
            .field("column_count", &self.column_count)
            .field("rows_delivered", &self.rows_delivered)
            .field("exhausted", &self.is_exhausted())
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl CopyOut {
    pub(crate) fn new(lease: CopySessionLease, column_count: usize) -> Self {
        Self {
            lease: Some(lease),
            decoder: Some(BinaryCopyDecoder::new(column_count)),
            column_count,
            rows_delivered: 0,
            exhausted: Arc::new(AtomicBool::new(false)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Number of binary columns reported by `CopyOutResponse`.
    pub const fn column_count(&self) -> usize {
        self.column_count
    }

    /// Cumulative count of decoded rows yielded by [`CopyOut::next_row`].
    pub const fn rows_delivered(&self) -> u64 {
        self.rows_delivered
    }

    /// Return `true` when `CopyDone` + `CommandComplete` + `ReadyForQuery` completed cleanly.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Acquire)
    }

    /// Return `true` when the stream has finished, errored, or been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Shared lock-free `(exhausted, closed)` flags for Python facade inspection.
    pub fn status_flags(&self) -> (Arc<AtomicBool>, Arc<AtomicBool>) {
        (Arc::clone(&self.exhausted), Arc::clone(&self.closed))
    }

    /// Yield the next decoded row of optional column byte slices, or `Ok(None)` when the `COPY TO`
    /// stream finishes and verifies its `COPY <n>` completion tag.
    pub async fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, PostgresError> {
        if self.is_exhausted() {
            return Ok(None);
        }
        if self.is_closed() || self.lease.is_none() {
            return Err(PostgresError::Interface(
                "cannot read from a closed COPY TO stream".to_string(),
            ));
        }

        if let Some(ref mut decoder) = self.decoder {
            match decoder.next_row() {
                Ok(Some(row)) => {
                    self.rows_delivered += 1;
                    return Ok(Some(row));
                }
                Ok(None) => {}
                Err(err) => {
                    self.abort_and_retire_on_error().await;
                    return Err(err);
                }
            }
        }

        loop {
            let msg = {
                let lease = self.lease.as_mut().expect("checked lease active");
                let cancel_notify = lease.cancel_notify();
                let connect_timeout = lease.connect_timeout();
                let conn = lease.connection_mut();
                let read_fut = read_message(&mut conn.stream, &mut conn.read_buf, None);
                tokio::pin!(read_fut);
                let cancel_timeout = async {
                    cancel_notify.notified().await;
                    tokio::time::sleep(connect_timeout).await;
                };
                tokio::pin!(cancel_timeout);
                tokio::select! {
                    biased;
                    res = &mut read_fut => res,
                    () = &mut cancel_timeout => Err(PostgresError::Operational(
                        "cancelled COPY TO stream timed out while draining wire response".to_string(),
                    )),
                }
            };

            let msg = match msg {
                Ok(m) => m,
                Err(err) => {
                    self.retire_without_cancel().await;
                    return Err(err);
                }
            };

            match msg {
                Message::CopyData(body) => {
                    let decoder = self
                        .decoder
                        .as_mut()
                        .expect("decoder present before CopyDone");
                    if let Err(err) = decoder.push(body.data()) {
                        self.abort_and_retire_on_error().await;
                        return Err(err);
                    }
                    match decoder.next_row() {
                        Ok(Some(row)) => {
                            self.rows_delivered += 1;
                            return Ok(Some(row));
                        }
                        Ok(None) => {}
                        Err(err) => {
                            self.abort_and_retire_on_error().await;
                            return Err(err);
                        }
                    }
                }
                Message::CopyDone => {
                    return self.finish_after_copy_done().await;
                }
                Message::ErrorResponse(body) => {
                    let parsed = match parse_error_response(&body) {
                        Ok(srv) => PostgresError::Server(srv),
                        Err(err) => {
                            self.retire_without_cancel().await;
                            return Err(err);
                        }
                    };
                    self.drain_after_server_error().await;
                    return Err(parsed);
                }
                Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    let res = self
                        .lease
                        .as_mut()
                        .expect("checked lease")
                        .connection_mut()
                        .handle_parameter_status_frame(&body);
                    if let Err(err) = res {
                        self.retire_without_cancel().await;
                        return Err(err);
                    }
                }
                Message::NotificationResponse(body) => {
                    let res = self
                        .lease
                        .as_mut()
                        .expect("checked lease")
                        .connection_mut()
                        .handle_notification_frame(&body);
                    if let Err(err) = res {
                        self.retire_without_cancel().await;
                        return Err(err);
                    }
                }
                _ => {
                    self.retire_without_cancel().await;
                    return Err(PostgresError::Protocol(
                        "unexpected backend frame during COPY TO STDOUT stream".to_string(),
                    ));
                }
            }
        }
    }

    async fn finish_after_copy_done(
        &mut self,
    ) -> Result<Option<Vec<Option<Bytes>>>, PostgresError> {
        let decode_res = self
            .decoder
            .take()
            .ok_or_else(|| PostgresError::Protocol("duplicate CopyDone frame".to_string()))
            .and_then(BinaryCopyDecoder::finish);

        let mut lease = self.lease.take().expect("lease present at CopyDone");
        let deadline = Instant::now() + lease.connect_timeout();
        let conn = lease.connection_mut();

        let mut tag_rows: Option<u64> = None;
        let mut server_err: Option<PostgresError> = None;
        let mut proto_err: Option<PostgresError> = None;

        loop {
            let msg = match read_message(&mut conn.stream, &mut conn.read_buf, Some(deadline)).await
            {
                Ok(m) => m,
                Err(err) => {
                    conn.mark_closed();
                    self.closed.store(true, Ordering::Release);
                    lease.finish_retired().await;
                    return Err(decode_res.err().unwrap_or(err));
                }
            };
            match msg {
                Message::CommandComplete(body) => match body.tag() {
                    Ok(tag) => match parse_copy_command_tag(tag) {
                        Ok(n) => tag_rows = Some(n),
                        Err(e) => proto_err = Some(e),
                    },
                    Err(e) => {
                        proto_err = Some(PostgresError::Protocol(format!(
                            "invalid COPY CommandComplete UTF-8: {e}"
                        )));
                    }
                },
                Message::ErrorResponse(body) => match parse_error_response(&body) {
                    Ok(srv) => {
                        if server_err.is_none() {
                            server_err = Some(PostgresError::Server(srv));
                        }
                    }
                    Err(e) => {
                        conn.mark_closed();
                        self.closed.store(true, Ordering::Release);
                        lease.finish_retired().await;
                        return Err(e);
                    }
                },
                Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    if let Err(e) = conn.handle_parameter_status_frame(&body) {
                        self.closed.store(true, Ordering::Release);
                        lease.finish_retired().await;
                        return Err(e);
                    }
                }
                Message::NotificationResponse(body) => {
                    if let Err(e) = conn.handle_notification_frame(&body) {
                        self.closed.store(true, Ordering::Release);
                        lease.finish_retired().await;
                        return Err(e);
                    }
                }
                Message::ReadyForQuery(body) => {
                    conn.set_exchange_state(ExchangeState::Idle);
                    let status = match TransactionStatus::from_wire_tag(body.status()) {
                        Ok(s) => s,
                        Err(e) => {
                            conn.mark_closed();
                            self.closed.store(true, Ordering::Release);
                            lease.finish_retired().await;
                            return Err(e);
                        }
                    };
                    conn.set_transaction_status_from_wire(status);
                    break;
                }
                _ => {
                    conn.mark_closed();
                    self.closed.store(true, Ordering::Release);
                    lease.finish_retired().await;
                    return Err(PostgresError::Protocol(
                        "unexpected backend frame after CopyDone before ReadyForQuery".to_string(),
                    ));
                }
            }
        }

        if let Some(err) = server_err {
            self.closed.store(true, Ordering::Release);
            if lease.cancel_claimed() {
                lease.finish_retired().await;
            } else {
                lease.finish_clean().await;
            }
            return Err(err);
        }
        if let Err(err) = decode_res {
            self.closed.store(true, Ordering::Release);
            lease.finish_retired().await;
            return Err(err);
        }
        let decoded_rows = decode_res.expect("checked Ok");
        if let Some(err) = proto_err {
            self.closed.store(true, Ordering::Release);
            lease.finish_retired().await;
            return Err(err);
        }
        let Some(expected_tag_rows) = tag_rows else {
            self.closed.store(true, Ordering::Release);
            lease.finish_retired().await;
            return Err(PostgresError::Protocol(
                "missing CommandComplete(`COPY <n>`) after CopyDone".to_string(),
            ));
        };
        if decoded_rows != expected_tag_rows || self.rows_delivered != expected_tag_rows {
            self.closed.store(true, Ordering::Release);
            lease.finish_retired().await;
            return Err(PostgresError::Protocol(format!(
                "COPY TO row count mismatch: decoded {decoded_rows} (delivered {}), server tag reported COPY {expected_tag_rows}",
                self.rows_delivered
            )));
        }

        self.exhausted.store(true, Ordering::Release);
        self.closed.store(true, Ordering::Release);
        if lease.cancel_claimed() {
            lease.finish_retired().await;
        } else {
            lease.finish_clean().await;
        }
        Ok(None)
    }

    async fn drain_after_server_error(&mut self) {
        self.decoder = None;
        self.closed.store(true, Ordering::Release);
        let Some(mut lease) = self.lease.take() else {
            return;
        };
        let deadline = Instant::now() + lease.connect_timeout();
        let drained = lease
            .connection_mut()
            .drain_to_ready_for_query_until(Some(deadline))
            .await
            .is_ok();
        if !drained || lease.cancel_claimed() {
            lease.finish_retired().await;
        } else {
            lease.finish_clean().await;
        }
    }

    async fn abort_and_retire_on_error(&mut self) {
        self.decoder = None;
        self.closed.store(true, Ordering::Release);
        if let Some(lease) = self.lease.take() {
            lease.cancel_drain_and_retire().await;
        }
    }

    async fn retire_without_cancel(&mut self) {
        self.decoder = None;
        self.closed.store(true, Ordering::Release);
        if let Some(mut lease) = self.lease.take() {
            lease.connection_mut().mark_closed();
            lease.finish_retired().await;
        }
    }

    /// Idempotently close the `COPY TO` stream. If not yet exhausted, issues a generation-fenced
    /// `CancelRequest`, drains backend frames under `connect_timeout`, and retires the physical socket.
    pub async fn close(&mut self) -> Result<(), PostgresError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.decoder = None;
        if let Some(lease) = self.lease.take() {
            lease.cancel_drain_and_retire().await;
        }
        Ok(())
    }
}

impl Drop for CopyOut {
    fn drop(&mut self) {
        self.decoder = None;
        let Some(lease) = self.lease.take() else {
            return;
        };
        self.closed.store(true, Ordering::Release);
        lease.spawn_cancel_and_retire_on_drop();
    }
}

#[derive(Debug)]
enum DuplexWriteOutcome {
    Completed,
    BackendError {
        err: PostgresError,
        frame_torn: bool,
    },
}

enum DeferredAsyncFrame {
    Param {
        name: String,
        val: String,
    },
    Notification {
        pid: i32,
        channel: String,
        payload: String,
    },
}

fn parse_buffered_copy_in_message(
    read_buf: &mut BytesMut,
    deferred: &mut Vec<DeferredAsyncFrame>,
) -> Result<Option<PostgresError>, PostgresError> {
    loop {
        if read_buf.len() < 5 {
            return Ok(None);
        }
        let raw_len =
            u32::from_be_bytes([read_buf[1], read_buf[2], read_buf[3], read_buf[4]]) as usize;
        if !(4..=MAX_BACKEND_FRAME_LENGTH).contains(&raw_len) {
            return Err(PostgresError::Protocol(format!(
                "backend frame length {raw_len} is outside valid bounds 4..={MAX_BACKEND_FRAME_LENGTH}"
            )));
        }
        if read_buf.len() < 1 + raw_len {
            return Ok(None);
        }
        let msg = match Message::parse(read_buf) {
            Ok(Some(m)) => m,
            Ok(None) => return Ok(None),
            Err(e) => {
                return Err(PostgresError::Protocol(format!(
                    "malformed backend frame during COPY FROM: {e}"
                )));
            }
        };
        match msg {
            Message::NoticeResponse(_) => {}
            Message::ParameterStatus(body) => {
                process_param_body(&body, deferred)?;
            }
            Message::NotificationResponse(body) => {
                process_notify_body(&body, deferred)?;
            }
            Message::ErrorResponse(body) => {
                let srv = parse_error_response(&body)?;
                return Ok(Some(PostgresError::Server(srv)));
            }
            _ => {
                return Err(PostgresError::Protocol(
                    "unexpected backend message while streaming COPY FROM STDIN".to_string(),
                ));
            }
        }
    }
}

fn process_param_body(
    body: &ParameterStatusBody,
    deferred: &mut Vec<DeferredAsyncFrame>,
) -> Result<(), PostgresError> {
    let name = body
        .name()
        .map_err(|e| PostgresError::Protocol(format!("invalid ParameterStatus name UTF-8: {e}")))?
        .to_owned();
    let val = body
        .value()
        .map_err(|e| PostgresError::Protocol(format!("invalid ParameterStatus value UTF-8: {e}")))?
        .to_owned();
    deferred.push(DeferredAsyncFrame::Param { name, val });
    Ok(())
}

fn process_notify_body(
    body: &NotificationResponseBody,
    deferred: &mut Vec<DeferredAsyncFrame>,
) -> Result<(), PostgresError> {
    let channel = body
        .channel()
        .map_err(|e| {
            PostgresError::Protocol(format!("invalid NotificationResponse channel UTF-8: {e}"))
        })?
        .to_owned();
    let payload = body
        .message()
        .map_err(|e| {
            PostgresError::Protocol(format!("invalid NotificationResponse payload UTF-8: {e}"))
        })?
        .to_owned();
    deferred.push(DeferredAsyncFrame::Notification {
        pid: body.process_id(),
        channel,
        payload,
    });
    Ok(())
}

fn apply_deferred_frames(conn: &mut PostgresConnection, deferred: Vec<DeferredAsyncFrame>) {
    for item in deferred {
        match item {
            DeferredAsyncFrame::Param { name, val } => {
                conn.apply_parameter_status_kv(name, val);
            }
            DeferredAsyncFrame::Notification {
                pid,
                channel,
                payload,
            } => {
                conn.dispatch_notification(pid, channel, payload);
            }
        }
    }
}

/// Write a single complete PostgreSQL frontend frame (`frame`) while concurrently polling
/// `conn.stream` for inbound `ErrorResponse` / `NoticeResponse` / `ParameterStatus` frames so
/// full TCP windows never deadlock when the server aborts `COPY FROM` early.
async fn write_frame_duplex(
    conn: &mut PostgresConnection,
    frame: &[u8],
    flush_after: bool,
) -> Result<DuplexWriteOutcome, PostgresError> {
    let mut deferred: Vec<DeferredAsyncFrame> = Vec::new();
    if let Some(srv_err) = parse_buffered_copy_in_message(&mut conn.read_buf, &mut deferred)? {
        apply_deferred_frames(conn, deferred);
        return Ok(DuplexWriteOutcome::BackendError {
            err: srv_err,
            frame_torn: false,
        });
    }

    let mut written = 0usize;
    let mut flushed = !flush_after;
    let mut scratch = [0u8; 8192];

    let res = poll_fn(|cx| {
        loop {
            let mut rbuf = ReadBuf::new(&mut scratch);
            match Pin::new(&mut conn.stream).poll_read(cx, &mut rbuf) {
                Poll::Ready(Ok(())) => {
                    let filled = rbuf.filled();
                    if filled.is_empty() {
                        match parse_buffered_copy_in_message(&mut conn.read_buf, &mut deferred) {
                            Ok(Some(srv_err)) => {
                                return Poll::Ready(Ok(DuplexWriteOutcome::BackendError {
                                    err: srv_err,
                                    frame_torn: written > 0 && written < frame.len(),
                                }));
                            }
                            Ok(None) => {
                                return Poll::Ready(Err(PostgresError::Transport(
                                    "unexpected EOF from PostgreSQL backend during COPY FROM"
                                        .to_string(),
                                )));
                            }
                            Err(e) => return Poll::Ready(Err(e)),
                        }
                    }
                    conn.read_buf.extend_from_slice(filled);
                    match parse_buffered_copy_in_message(&mut conn.read_buf, &mut deferred) {
                        Ok(Some(srv_err)) => {
                            return Poll::Ready(Ok(DuplexWriteOutcome::BackendError {
                                err: srv_err,
                                frame_torn: written > 0 && written < frame.len(),
                            }));
                        }
                        Ok(None) => {}
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                Poll::Ready(Err(io_err)) => {
                    return Poll::Ready(Err(PostgresError::Transport(format!(
                        "socket read error during COPY FROM: {io_err}"
                    ))));
                }
                Poll::Pending => break,
            }
        }

        while written < frame.len() {
            match Pin::new(&mut conn.stream).poll_write(cx, &frame[written..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(PostgresError::Transport(
                        "socket write returned 0 bytes during COPY FROM".to_string(),
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    written += n;
                }
                Poll::Ready(Err(io_err)) => {
                    match parse_buffered_copy_in_message(&mut conn.read_buf, &mut deferred) {
                        Ok(Some(srv_err)) => {
                            return Poll::Ready(Ok(DuplexWriteOutcome::BackendError {
                                err: srv_err,
                                frame_torn: true,
                            }));
                        }
                        _ => {
                            return Poll::Ready(Err(PostgresError::Transport(format!(
                                "socket write error during COPY FROM: {io_err}"
                            ))));
                        }
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        if !flushed {
            match Pin::new(&mut conn.stream).poll_flush(cx) {
                Poll::Ready(Ok(())) => flushed = true,
                Poll::Ready(Err(io_err)) => {
                    return Poll::Ready(Err(PostgresError::Transport(format!(
                        "socket flush error during COPY FROM: {io_err}"
                    ))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(DuplexWriteOutcome::Completed))
    })
    .await;

    apply_deferred_frames(conn, deferred);
    res
}

/// Exclusive duplex streaming handle over `COPY ... FROM STDIN (FORMAT binary)`.
pub struct CopyFromStream {
    lease: Option<CopySessionLease>,
    encoder: BinaryCopyEncoder,
    arrow_plan: Option<crate::arrow::PostgresArrowImportPlan>,
    column_count: usize,
    owns_transaction: bool,
    codec_context: CodecContext,
    type_catalog: TypeCatalog,
    row_buf: BytesMut,
    chunk_frame_buf: BytesMut,
    closed: Arc<AtomicBool>,
    busy: Arc<AtomicBool>,
}

impl std::fmt::Debug for CopyFromStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyFromStream")
            .field("column_count", &self.column_count)
            .field("owns_transaction", &self.owns_transaction)
            .field("has_arrow_plan", &self.arrow_plan.is_some())
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl CopyFromStream {
    pub(crate) fn new(
        lease: CopySessionLease,
        column_count: usize,
        owns_transaction: bool,
        codec_context: CodecContext,
        type_catalog: TypeCatalog,
    ) -> Result<Self, PostgresError> {
        let encoder = BinaryCopyEncoder::new(column_count)?;
        Ok(Self::new_with_encoder_and_plan(
            lease,
            column_count,
            owns_transaction,
            codec_context,
            type_catalog,
            encoder,
            None,
        ))
    }

    pub(crate) fn new_with_encoder_and_plan(
        lease: CopySessionLease,
        column_count: usize,
        owns_transaction: bool,
        codec_context: CodecContext,
        type_catalog: TypeCatalog,
        encoder: BinaryCopyEncoder,
        arrow_plan: Option<crate::arrow::PostgresArrowImportPlan>,
    ) -> Self {
        Self {
            lease: Some(lease),
            encoder,
            arrow_plan,
            column_count,
            owns_transaction,
            codec_context,
            type_catalog,
            row_buf: BytesMut::new(),
            chunk_frame_buf: BytesMut::with_capacity(5 + COPY_DATA_CHUNK_BYTES),
            closed: Arc::new(AtomicBool::new(false)),
            busy: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Number of binary columns reported by `CopyInResponse`.
    pub const fn column_count(&self) -> usize {
        self.column_count
    }

    /// Return `true` once `finish()`, `abort()`, or a terminal error has closed the stream.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Shared lock-free closed flag for Python facade inspection.
    pub fn closed_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.closed)
    }

    fn enter_busy(&self) -> Result<BusyResetGuard, PostgresError> {
        if self.busy.swap(true, Ordering::AcqRel) {
            return Err(PostgresError::Interface(
                "overlapping COPY FROM operation is not allowed".to_string(),
            ));
        }
        Ok(BusyResetGuard {
            flag: Arc::clone(&self.busy),
        })
    }

    /// Encode `rows` one tuple at a time into `<= 64 KiB` `CopyData` frames while polling backend
    /// `ErrorResponse` frames concurrently to prevent full-window deadlocks.
    pub async fn write_rows(
        &mut self,
        column_oids: &[u32],
        rows: &[Vec<PgValue>],
    ) -> Result<(), PostgresError> {
        let _busy = self.enter_busy()?;
        if self.is_closed() || self.lease.is_none() {
            return Err(PostgresError::Interface(
                "cannot call write_rows on a closed COPY FROM stream".to_string(),
            ));
        }
        if column_oids.len() != self.column_count {
            let err = PostgresError::Data(format!(
                "COPY FROM column_oids length {} does not match column_count {}",
                column_oids.len(),
                self.column_count
            ));
            let _ = self.abort_internal("COPY FROM column_oids mismatch").await;
            return Err(err);
        }

        let mut stage = BytesMut::with_capacity(COPY_DATA_CHUNK_BYTES);
        for row in rows {
            self.row_buf.clear();
            if let Err(err) = self.encoder.encode_row_values(
                column_oids,
                row,
                &self.codec_context,
                &self.type_catalog,
                &mut self.row_buf,
            ) {
                if !stage.is_empty() {
                    stage.clear();
                }
                let _ = self.abort_internal("COPY FROM row encoding failed").await;
                return Err(err);
            }

            if self.row_buf.len() >= COPY_DATA_CHUNK_BYTES {
                if !stage.is_empty() {
                    self.flush_encoded_slice_internal(&stage, false).await?;
                    stage.clear();
                }
                let large_row = self.row_buf.split().freeze();
                self.flush_encoded_slice_internal(&large_row, false).await?;
                continue;
            }

            if stage.len() + self.row_buf.len() > COPY_DATA_CHUNK_BYTES {
                self.flush_encoded_slice_internal(&stage, false).await?;
                stage.clear();
            }
            stage.extend_from_slice(&self.row_buf);
            self.row_buf.clear();
        }

        if !stage.is_empty() {
            self.flush_encoded_slice_internal(&stage, true).await?;
        }
        Ok(())
    }

    /// Decode a self-contained Arrow IPC stream chunk (`<= 16 MiB` call, `<= 8 MiB` per batch),
    /// encode rows against the prevalidated [`crate::arrow::PostgresArrowImportPlan`] preserving
    /// `PGCOPY` header state, and flush via `CopyData`; aborts via `CopyFail` on any error.
    pub async fn write_arrow_ipc(&mut self, batch_ipc: &[u8]) -> Result<(), PostgresError> {
        let _busy = self.enter_busy()?;
        if self.is_closed() || self.lease.is_none() {
            return Err(PostgresError::Interface(
                "cannot call write_arrow_ipc on a closed COPY FROM stream".to_string(),
            ));
        }
        if self.arrow_plan.is_none() {
            let _ = self
                .abort_internal("write_arrow_ipc called on non-Arrow COPY stream")
                .await;
            return Err(PostgresError::Interface(
                "write_arrow_ipc requires a CopyFromStream opened via copy_from_arrow".to_string(),
            ));
        }

        let (schema, batches) = match crate::arrow::decode_bounded_ipc_batches(batch_ipc) {
            Ok(decoded) => decoded,
            Err(err) => {
                let _ = self
                    .abort_internal("COPY FROM Arrow IPC decode failed")
                    .await;
                return Err(err);
            }
        };

        let mut payload = BytesMut::new();
        let encode_res: Result<(), PostgresError> = {
            let plan = self.arrow_plan.as_ref().expect("checked arrow_plan");
            if batches.is_empty() {
                let empty = arrow_array::RecordBatch::new_empty(schema);
                plan.encode_batch(
                    &empty,
                    &mut self.encoder,
                    &self.codec_context,
                    &self.type_catalog,
                    &mut payload,
                )
            } else {
                let mut status = Ok(());
                for batch in &batches {
                    if let Err(err) = plan.encode_batch(
                        batch,
                        &mut self.encoder,
                        &self.codec_context,
                        &self.type_catalog,
                        &mut payload,
                    ) {
                        status = Err(err);
                        break;
                    }
                }
                status
            }
        };

        if let Err(err) = encode_res {
            payload.clear();
            let _ = self
                .abort_internal("COPY FROM Arrow batch encoding failed")
                .await;
            return Err(err);
        }

        if !payload.is_empty() {
            self.flush_encoded_slice_internal(&payload, false).await?;
        }
        Ok(())
    }

    async fn flush_encoded_slice_internal(
        &mut self,
        payload: &[u8],
        flush_end: bool,
    ) -> Result<(), PostgresError> {
        let mut offset = 0usize;
        while offset < payload.len() {
            let end = (offset + COPY_DATA_CHUNK_BYTES).min(payload.len());
            let chunk = &payload[offset..end];
            let is_last = end == payload.len();
            offset = end;

            self.chunk_frame_buf.clear();
            frontend::CopyData::new(chunk)
                .map_err(|e| PostgresError::Protocol(format!("failed to encode CopyData: {e}")))?
                .write(&mut self.chunk_frame_buf);

            let outcome = {
                let lease = self.lease.as_mut().expect("checked lease active");
                write_frame_duplex(
                    lease.connection_mut(),
                    &self.chunk_frame_buf,
                    flush_end && is_last,
                )
                .await
            };
            self.chunk_frame_buf.clear();

            match outcome {
                Ok(DuplexWriteOutcome::Completed) => {}
                Ok(DuplexWriteOutcome::BackendError { err, frame_torn }) => {
                    self.recover_after_backend_error(frame_torn).await;
                    return Err(err);
                }
                Err(transport_err) => {
                    self.retire_on_failure().await;
                    return Err(transport_err);
                }
            }
        }
        Ok(())
    }

    async fn recover_after_backend_error(&mut self, frame_torn: bool) {
        self.closed.store(true, Ordering::Release);
        let Some(mut lease) = self.lease.take() else {
            return;
        };
        let deadline = Instant::now() + lease.connect_timeout();
        let drained = lease
            .connection_mut()
            .drain_to_ready_for_query_until(Some(deadline))
            .await
            .is_ok();

        if !drained || frame_torn || lease.cancel_claimed() {
            if !self.owns_transaction {
                lease.mark_ambient_failed_on_retire();
            }
            lease.finish_retired().await;
            return;
        }

        if self.owns_transaction {
            let rb_budget = lease.connect_timeout().min(Duration::from_millis(250));
            let conn = lease.connection_mut();
            if conn.transaction_status() != TransactionStatus::Idle {
                match tokio::time::timeout(rb_budget, conn.rollback()).await {
                    Ok(Ok(_)) => lease.finish_clean().await,
                    _ => {
                        conn.mark_closed();
                        lease.finish_retired().await;
                    }
                }
            } else {
                lease.finish_clean().await;
            }
        } else {
            lease.finish_clean().await;
        }
    }

    async fn retire_on_failure(&mut self) {
        self.closed.store(true, Ordering::Release);
        if let Some(mut lease) = self.lease.take() {
            lease.connection_mut().mark_closed();
            if !self.owns_transaction {
                lease.mark_ambient_failed_on_retire();
            }
            lease.finish_retired().await;
        }
    }

    /// Write the `PGCOPY` header (if zero rows were written), single `-1i16` trailer, and
    /// `CopyDone`, verify `CommandComplete("COPY <n>")` + `ReadyForQuery`, commit only
    /// COPY-owned transactions, and return the inserted row count.
    pub async fn finish(&mut self) -> Result<u64, PostgresError> {
        let _busy = self.enter_busy()?;
        if self.is_closed() || self.lease.is_none() {
            return Err(PostgresError::Interface(
                "cannot call finish() on a closed COPY FROM stream".to_string(),
            ));
        }

        let mut trailer_payload = BytesMut::with_capacity(PGCOPY_HEADER_BYTES + 2);
        self.encoder.finish_trailer(&mut trailer_payload)?;

        self.chunk_frame_buf.clear();
        frontend::CopyData::new(trailer_payload.as_ref())
            .map_err(|e| {
                PostgresError::Protocol(format!("failed to encode CopyData trailer: {e}"))
            })?
            .write(&mut self.chunk_frame_buf);
        frontend::copy_done(&mut self.chunk_frame_buf);

        let write_outcome = {
            let lease = self.lease.as_mut().expect("checked lease");
            write_frame_duplex(lease.connection_mut(), &self.chunk_frame_buf, true).await
        };
        self.chunk_frame_buf.clear();

        match write_outcome {
            Ok(DuplexWriteOutcome::Completed) => {}
            Ok(DuplexWriteOutcome::BackendError { err, frame_torn }) => {
                self.recover_after_backend_error(frame_torn).await;
                return Err(err);
            }
            Err(err) => {
                self.retire_on_failure().await;
                return Err(err);
            }
        }

        self.closed.store(true, Ordering::Release);
        let mut lease = self.lease.take().expect("checked lease");

        let mut copy_rows: Option<u64> = None;
        let mut server_err: Option<PostgresError> = None;
        let mut proto_err: Option<PostgresError> = None;

        {
            let conn = lease.connection_mut();
            loop {
                let msg = match read_message(&mut conn.stream, &mut conn.read_buf, None).await {
                    Ok(m) => m,
                    Err(err) => {
                        conn.mark_closed();
                        if !self.owns_transaction {
                            lease.mark_ambient_failed_on_retire();
                        }
                        lease.finish_retired().await;
                        return Err(err);
                    }
                };
                match msg {
                    Message::CommandComplete(body) => match body.tag() {
                        Ok(tag) => match parse_copy_command_tag(tag) {
                            Ok(n) => copy_rows = Some(n),
                            Err(e) => proto_err = Some(e),
                        },
                        Err(e) => {
                            proto_err = Some(PostgresError::Protocol(format!(
                                "invalid COPY FROM CommandComplete UTF-8: {e}"
                            )));
                        }
                    },
                    Message::ErrorResponse(body) => match parse_error_response(&body) {
                        Ok(srv) => {
                            if server_err.is_none() {
                                server_err = Some(PostgresError::Server(srv));
                            }
                        }
                        Err(e) => {
                            conn.mark_closed();
                            lease.finish_retired().await;
                            return Err(e);
                        }
                    },
                    Message::NoticeResponse(_) => {}
                    Message::ParameterStatus(body) => {
                        if let Err(e) = conn.handle_parameter_status_frame(&body) {
                            lease.finish_retired().await;
                            return Err(e);
                        }
                    }
                    Message::NotificationResponse(body) => {
                        if let Err(e) = conn.handle_notification_frame(&body) {
                            lease.finish_retired().await;
                            return Err(e);
                        }
                    }
                    Message::ReadyForQuery(body) => {
                        conn.set_exchange_state(ExchangeState::Idle);
                        let status = match TransactionStatus::from_wire_tag(body.status()) {
                            Ok(s) => s,
                            Err(e) => {
                                conn.mark_closed();
                                lease.finish_retired().await;
                                return Err(e);
                            }
                        };
                        conn.set_transaction_status_from_wire(status);
                        break;
                    }
                    _ => {
                        conn.mark_closed();
                        lease.finish_retired().await;
                        return Err(PostgresError::Protocol(
                            "unexpected backend frame after COPY FROM CopyDone".to_string(),
                        ));
                    }
                }
            }
        }

        if let Some(err) = server_err {
            if lease.cancel_claimed() {
                if !self.owns_transaction {
                    lease.mark_ambient_failed_on_retire();
                }
                lease.finish_retired().await;
            } else if self.owns_transaction {
                let rolled_back = {
                    let conn = lease.connection_mut();
                    if conn.transaction_status() != TransactionStatus::Idle {
                        if conn.rollback().await.is_ok() {
                            true
                        } else {
                            conn.mark_closed();
                            false
                        }
                    } else {
                        true
                    }
                };
                if rolled_back {
                    lease.finish_clean().await;
                } else {
                    lease.finish_retired().await;
                }
            } else {
                lease.finish_clean().await;
            }
            return Err(err);
        }

        if let Some(err) = proto_err {
            {
                let conn = lease.connection_mut();
                if self.owns_transaction && conn.transaction_status() != TransactionStatus::Idle {
                    let _ = conn.rollback().await;
                }
                conn.mark_closed();
            }
            lease.finish_retired().await;
            return Err(err);
        }

        let Some(rows) = copy_rows else {
            {
                let conn = lease.connection_mut();
                if self.owns_transaction && conn.transaction_status() != TransactionStatus::Idle {
                    let _ = conn.rollback().await;
                }
                conn.mark_closed();
            }
            lease.finish_retired().await;
            return Err(PostgresError::Protocol(
                "missing CommandComplete(`COPY <n>`) after COPY FROM CopyDone".to_string(),
            ));
        };

        if self.owns_transaction {
            let commit_res = lease.connection_mut().commit().await;
            if let Err(commit_err) = commit_res {
                let is_closed = lease.connection_mut().is_closed();
                if is_closed || lease.cancel_claimed() {
                    lease.finish_retired().await;
                } else {
                    let _ = lease.connection_mut().rollback().await;
                    lease.finish_clean().await;
                }
                return Err(commit_err);
            }
        }

        if lease.cancel_claimed() {
            lease.finish_retired().await;
        } else {
            lease.finish_clean().await;
        }
        Ok(rows)
    }

    /// Abort the active `COPY FROM` stream idempotently using sanitized `frontend::copy_fail`
    /// (`<= 256` UTF-8 bytes, NUL -> `'?'`), draining `ErrorResponse` + `ReadyForQuery` under
    /// `connect_timeout`, rolling back COPY-owned transactions, and leaving ambient transactions in `'E'`.
    pub async fn abort(&mut self, reason: &str) -> Result<(), PostgresError> {
        let _busy = self.enter_busy()?;
        self.abort_internal(reason).await
    }

    async fn abort_internal(&mut self, reason: &str) -> Result<(), PostgresError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let Some(lease) = self.lease.take() else {
            return Ok(());
        };
        abort_copy_in_lease(lease, self.owns_transaction, reason).await;
        Ok(())
    }
}

impl Drop for CopyFromStream {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        self.closed.store(true, Ordering::Release);
        let owns_transaction = self.owns_transaction;
        lease.begin_drop_cleanup();
        let cleanup = async move {
            abort_copy_in_lease(lease, owns_transaction, "COPY FROM dropped before finish").await;
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(cleanup);
        } else {
            vertebra_db::py_async::db_runtime_handle().spawn(cleanup);
        }
    }
}

struct BusyResetGuard {
    flag: Arc<AtomicBool>,
}

impl Drop for BusyResetGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

pub(crate) async fn abort_copy_in_lease(
    mut lease: CopySessionLease,
    owns_transaction: bool,
    reason: &str,
) {
    let sanitized = sanitize_copy_fail_reason(reason);
    let deadline = Instant::now() + lease.connect_timeout();

    let mut frame = BytesMut::with_capacity(6 + sanitized.len());
    if frontend::copy_fail(&sanitized, &mut frame).is_err() {
        lease.connection_mut().mark_closed();
        if !owns_transaction {
            lease.mark_ambient_failed_on_retire();
        }
        lease.finish_retired().await;
        return;
    }

    let send_and_drain = {
        let conn = lease.connection_mut();
        tokio::time::timeout_at(deadline, async {
            match write_frame_duplex(conn, &frame, true).await {
                Ok(DuplexWriteOutcome::Completed) => {
                    conn.drain_to_ready_for_query_until(Some(deadline)).await
                }
                Ok(DuplexWriteOutcome::BackendError { frame_torn, .. }) => {
                    if frame_torn {
                        Err(PostgresError::Transport(
                            "torn CopyFail frame during concurrent backend ErrorResponse"
                                .to_string(),
                        ))
                    } else {
                        conn.drain_to_ready_for_query_until(Some(deadline)).await
                    }
                }
                Err(e) => Err(e),
            }
        })
        .await
    };

    if !matches!(send_and_drain, Ok(Ok(()))) || lease.cancel_claimed() {
        lease.connection_mut().mark_closed();
        if !owns_transaction {
            lease.mark_ambient_failed_on_retire();
        }
        lease.finish_retired().await;
        return;
    }

    if owns_transaction {
        let needs_rb = lease.connection_mut().transaction_status() != TransactionStatus::Idle;
        if needs_rb {
            let rb = tokio::time::timeout_at(deadline, lease.connection_mut().rollback()).await;
            if !matches!(rb, Ok(Ok(_))) {
                lease.connection_mut().mark_closed();
                lease.finish_retired().await;
                return;
            }
        }
    }

    lease.finish_clean().await;
}

/// Abort a `CopyIn` subprotocol exchange opened directly on [`PostgresConnection`] (e.g. when
/// `CopyInResponse` reported non-binary format), draining `ErrorResponse` + `ReadyForQuery` boundedly.
pub(crate) async fn abort_copy_in_on_connection(
    conn: &mut PostgresConnection,
    reason: &str,
    deadline: Instant,
) -> Result<(), PostgresError> {
    let sanitized = sanitize_copy_fail_reason(reason);
    conn.write_buf.clear();
    frontend::copy_fail(&sanitized, &mut conn.write_buf)
        .map_err(|e| PostgresError::Protocol(format!("failed to encode CopyFail: {e}")))?;
    let write_res = tokio::time::timeout_at(deadline, async {
        conn.stream.write_all(&conn.write_buf).await?;
        conn.stream.flush().await
    })
    .await;
    conn.write_buf.clear();
    match write_res {
        Ok(Ok(())) => conn.drain_to_ready_for_query_until(Some(deadline)).await,
        Ok(Err(e)) => {
            conn.mark_closed();
            Err(PostgresError::Transport(format!(
                "CopyFail write failed: {e}"
            )))
        }
        Err(_) => {
            conn.mark_closed();
            Err(PostgresError::Transport(
                "CopyFail write timed out".to_string(),
            ))
        }
    }
}

const QUALIFIED_COPY_COLUMNS_SQL: &str = "SELECT n.nspname, c.oid, a.attname, a.atttypid, a.atttypmod, a.attnum \
FROM pg_catalog.pg_class c \
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid \
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid \
WHERE n.nspname = $1 AND c.relname = $2 \
AND c.relkind IN ('r', 'p', 'v', 'm', 'f') \
AND a.attnum > 0 AND NOT a.attisdropped \
ORDER BY c.oid, a.attnum";

const UNQUALIFIED_COPY_COLUMNS_SQL: &str = "SELECT n.nspname, c.oid, a.attname, a.atttypid, a.atttypmod, a.attnum \
FROM pg_catalog.pg_class c \
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid \
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid \
WHERE c.relname = $1 \
AND n.nspname = ANY(pg_catalog.current_schemas(false)) \
AND c.relkind IN ('r', 'p', 'v', 'm', 'f') \
AND a.attnum > 0 AND NOT a.attisdropped \
ORDER BY array_position(pg_catalog.current_schemas(false), n.nspname), c.oid, a.attnum";

fn validate_requested_copy_columns(
    columns: Option<&[String]>,
) -> Result<Option<Vec<String>>, PostgresError> {
    let Some(cols) = columns else {
        return Ok(None);
    };
    if cols.is_empty() {
        return Err(PostgresError::Data(
            "COPY FROM Arrow explicit column list cannot be empty".to_string(),
        ));
    }
    if i16::try_from(cols.len()).is_err() {
        return Err(PostgresError::Data(format!(
            "COPY FROM Arrow column list length {} exceeds i16::MAX ({})",
            cols.len(),
            i16::MAX
        )));
    }
    let mut seen = std::collections::HashSet::with_capacity(cols.len());
    for col_name in cols {
        let _ = crate::arrow::quote_postgres_identifier(col_name)?;
        if !seen.insert(col_name.as_str()) {
            return Err(PostgresError::Data(format!(
                "duplicate column `{col_name}` in COPY FROM Arrow column list"
            )));
        }
    }
    Ok(Some(cols.to_vec()))
}

fn extract_pg_oid_cell(val: &PgValue, label: &str) -> Result<u32, PostgresError> {
    match val {
        PgValue::Int8(n) => u32::try_from(*n).map_err(|_| {
            PostgresError::Protocol(format!("invalid {label} OID {n} in pg_catalog response"))
        }),
        PgValue::Int4(n) => u32::try_from(*n).map_err(|_| {
            PostgresError::Protocol(format!("invalid {label} OID {n} in pg_catalog response"))
        }),
        PgValue::Text(s) => s.parse::<u32>().map_err(|_| {
            PostgresError::Protocol(format!("invalid {label} OID {s:?} in pg_catalog response"))
        }),
        other => Err(PostgresError::Protocol(format!(
            "unexpected {label} cell {other:?} in pg_catalog response"
        ))),
    }
}

fn extract_pg_i32_cell(val: &PgValue, label: &str) -> Result<i32, PostgresError> {
    match val {
        PgValue::Int4(n) => Ok(*n),
        PgValue::Int2(n) => Ok(i32::from(*n)),
        PgValue::Int8(n) => i32::try_from(*n).map_err(|_| {
            PostgresError::Protocol(format!("overflowing {label} {n} in pg_catalog response"))
        }),
        PgValue::Text(s) => s.parse::<i32>().map_err(|_| {
            PostgresError::Protocol(format!("invalid {label} {s:?} in pg_catalog response"))
        }),
        other => Err(PostgresError::Protocol(format!(
            "unexpected {label} cell {other:?} in pg_catalog response"
        ))),
    }
}

async fn resolve_arrow_copy_target_columns(
    conn: &mut PostgresConnection,
    parsed_schema: Option<&str>,
    parsed_table: &str,
    requested_columns: Option<&[String]>,
) -> Result<(String, Vec<Column>), PostgresError> {
    let qr = if let Some(schema) = parsed_schema {
        let params = [
            Parameter::with_oid(PgValue::Text(schema.to_owned()), TEXT_OID)?,
            Parameter::with_oid(PgValue::Text(parsed_table.to_owned()), TEXT_OID)?,
        ];
        let oids = [TEXT_OID, TEXT_OID];
        conn.query(QUALIFIED_COPY_COLUMNS_SQL, &params, Some(&oids))
            .await?
    } else {
        let params = [Parameter::with_oid(
            PgValue::Text(parsed_table.to_owned()),
            TEXT_OID,
        )?];
        let oids = [TEXT_OID];
        conn.query(UNQUALIFIED_COPY_COLUMNS_SQL, &params, Some(&oids))
            .await?
    };

    if qr.rows.is_empty() {
        return Err(PostgresError::Data(match parsed_schema {
            Some(sch) => format!(
                "PostgreSQL COPY target relation `{sch}`.`{parsed_table}` was not found or has zero columns"
            ),
            None => format!(
                "PostgreSQL COPY target relation `{parsed_table}` was not found on current_schemas(false) or has zero columns"
            ),
        }));
    }

    let mut chosen_schema: Option<String> = None;
    let mut chosen_rel_oid: Option<u32> = None;
    let mut table_cols: Vec<Column> = Vec::with_capacity(qr.rows.len());

    for row in &qr.rows {
        if row.len() != 6 {
            return Err(PostgresError::Protocol(format!(
                "expected 6 columns from pg_catalog COPY lookup, got {}",
                row.len()
            )));
        }
        let PgValue::Text(ref nsp) = row[0] else {
            return Err(PostgresError::Protocol(
                "expected Text nspname in pg_catalog COPY lookup".to_string(),
            ));
        };
        let rel_oid = extract_pg_oid_cell(&row[1], "pg_class.oid")?;
        if let Some(first_oid) = chosen_rel_oid {
            if rel_oid != first_oid {
                continue;
            }
        } else {
            chosen_rel_oid = Some(rel_oid);
            chosen_schema = Some(nsp.clone());
        }
        let PgValue::Text(ref attname) = row[2] else {
            return Err(PostgresError::Protocol(
                "expected Text attname in pg_catalog COPY lookup".to_string(),
            ));
        };
        let atttypid = extract_pg_oid_cell(&row[3], "pg_attribute.atttypid")?;
        let atttypmod = extract_pg_i32_cell(&row[4], "pg_attribute.atttypmod")?;
        let attnum_i32 = extract_pg_i32_cell(&row[5], "pg_attribute.attnum")?;
        let attnum = i16::try_from(attnum_i32).unwrap_or(0);
        table_cols.push(Column::try_new(
            attname.clone(),
            atttypid,
            Format::Binary,
            atttypmod,
            rel_oid,
            attnum,
        )?);
    }

    let resolved_schema = chosen_schema.ok_or_else(|| {
        PostgresError::Data("pg_catalog COPY lookup resolved no relation".to_string())
    })?;

    let selected_cols = if let Some(req) = requested_columns {
        let mut ordered = Vec::with_capacity(req.len());
        for want in req {
            let found = table_cols.iter().find(|c| c.name == *want).ok_or_else(|| {
                PostgresError::Data(format!(
                    "column `{want}` does not exist on PostgreSQL relation `{resolved_schema}`.`{parsed_table}`"
                ))
            })?;
            ordered.push(found.clone());
        }
        ordered
    } else {
        table_cols
    };

    let mut missing_oids: Vec<u32> = Vec::new();
    for col in &selected_cols {
        if conn.type_catalog.type_info(col.type_oid).is_none()
            && !missing_oids.contains(&col.type_oid)
        {
            missing_oids.push(col.type_oid);
        }
    }
    if !missing_oids.is_empty() {
        let mut catalog = conn.type_catalog.clone();
        catalog.discover(conn, &missing_oids, &[]).await?;
        conn.type_catalog = catalog;
    }

    let quoted_schema = crate::arrow::quote_postgres_identifier(&resolved_schema)?;
    let quoted_table = crate::arrow::quote_postgres_identifier(parsed_table)?;
    let mut quoted_cols = Vec::with_capacity(selected_cols.len());
    for col in &selected_cols {
        quoted_cols.push(crate::arrow::quote_postgres_identifier(&col.name)?);
    }
    let copy_sql = format!(
        "COPY {quoted_schema}.{quoted_table} ({}) FROM STDIN (FORMAT binary)",
        quoted_cols.join(", ")
    );
    validate_single_copy_sql(&copy_sql)?;
    Ok((copy_sql, selected_cols))
}

impl SessionHandle {
    /// Resolve target PostgreSQL columns via a single bound `pg_catalog` join, compile a
    /// [`crate::arrow::PostgresArrowImportPlan`], pre-encode `first_batch_ipc` before starting
    /// `COPY ... FROM STDIN (FORMAT binary)`, and return an active [`CopyFromStream`].
    pub async fn copy_from_arrow(
        self: &Arc<Self>,
        table_name: &str,
        schema_name: Option<&str>,
        first_batch_ipc: &[u8],
        columns: Option<&[String]>,
    ) -> Result<CopyFromStream, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.copy_from_arrow_with_generation_as(
            op_gen,
            table_name,
            schema_name,
            first_batch_ipc,
            columns,
            None,
        )
        .await
    }

    /// Open a binary `COPY FROM STDIN` Arrow import stream inside an optional explicit [`TransactionOwner`].
    pub async fn copy_from_arrow_as(
        self: &Arc<Self>,
        table_name: &str,
        schema_name: Option<&str>,
        first_batch_ipc: &[u8],
        columns: Option<&[String]>,
        owner: Option<TransactionOwner>,
    ) -> Result<CopyFromStream, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.copy_from_arrow_with_generation_as(
            op_gen,
            table_name,
            schema_name,
            first_batch_ipc,
            columns,
            owner,
        )
        .await
    }

    /// Generation-fenced implementation of [`SessionHandle::copy_from_arrow_as`] fencing both the
    /// bound `pg_catalog` + [`TypeCatalog::discover`] preflight and `CopySessionLease` startup.
    pub async fn copy_from_arrow_with_generation_as(
        self: &Arc<Self>,
        op_gen: OperationGeneration,
        table_name: &str,
        schema_name: Option<&str>,
        first_batch_ipc: &[u8],
        columns: Option<&[String]>,
        owner: Option<TransactionOwner>,
    ) -> Result<CopyFromStream, PostgresError> {
        if let Err(err) = self.ensure_not_in_pipeline_for_copy() {
            self.unreserve_operation_generation(op_gen);
            return Err(err);
        }
        let (parsed_schema, parsed_table) =
            match crate::arrow::parse_postgres_qualified_table(table_name, schema_name) {
                Ok(parsed) => parsed,
                Err(err) => {
                    self.unreserve_operation_generation(op_gen);
                    return Err(err);
                }
            };
        let requested_cols = match validate_requested_copy_columns(columns) {
            Ok(cols) => cols,
            Err(err) => {
                self.unreserve_operation_generation(op_gen);
                return Err(err);
            }
        };
        let (arrow_schema, first_batches) =
            match crate::arrow::decode_bounded_ipc_batches(first_batch_ipc) {
                Ok(decoded) => decoded,
                Err(err) => {
                    self.unreserve_operation_generation(op_gen);
                    return Err(err);
                }
            };

        let (copy_sql, expected_cols, plan, encoder, first_payload) = self
            .run_cancellable_operation(op_gen, SessionAccess::Ordinary, owner, move |conn, meta| {
                Box::pin(async move {
                    if conn.transaction_status() == TransactionStatus::Failed {
                        return Err(PostgresError::Interface(
                            "cannot start COPY FROM Arrow while transaction is aborted ('E')"
                                .to_string(),
                        ));
                    }
                    let (sql, target_cols) = resolve_arrow_copy_target_columns(
                        conn,
                        parsed_schema.as_deref(),
                        &parsed_table,
                        requested_cols.as_deref(),
                    )
                    .await?;
                    let plan = crate::arrow::PostgresArrowImportPlan::from_schema(
                        &arrow_schema,
                        &target_cols,
                        &conn.type_catalog,
                    )?;
                    let mut encoder = BinaryCopyEncoder::new(target_cols.len())?;
                    let ctx = CodecContext::from_parameters(&conn.parameters);
                    let mut payload = BytesMut::new();
                    for batch in &first_batches {
                        plan.encode_batch(
                            batch,
                            &mut encoder,
                            &ctx,
                            &conn.type_catalog,
                            &mut payload,
                        )?;
                    }
                    meta.rereserve_op_gen_on_ok = true;
                    Ok((sql, target_cols.len(), plan, encoder, payload))
                })
            })
            .await?;

        let mut lease = self.checkout_for_copy(op_gen, owner).await?;
        let start_res = {
            let cancel_notify = lease.cancel_notify();
            let timeout = lease.connect_timeout();
            let conn = lease.connection_mut();
            let start_fut = conn.start_copy_in(&copy_sql);
            tokio::pin!(start_fut);
            let cancel_timeout = async {
                cancel_notify.notified().await;
                tokio::time::sleep(timeout).await;
            };
            tokio::pin!(cancel_timeout);
            tokio::select! {
                biased;
                res = &mut start_fut => res,
                () = &mut cancel_timeout => Err(PostgresError::Operational(
                    "cancelled COPY FROM Arrow start timed out while draining wire response"
                        .to_string(),
                )),
            }
        };

        let mut stream = match start_res {
            Ok(wire_cols) => {
                let (owns_tx, ctx, catalog) = {
                    let conn = lease.connection_mut();
                    let owns = conn.copy_in_owned_tx;
                    conn.copy_in_owned_tx = false;
                    let ctx = CodecContext::from_parameters(&conn.parameters);
                    let cat = conn.type_catalog.clone();
                    (owns, ctx, cat)
                };
                if lease.cancel_claimed() {
                    abort_copy_in_lease(lease, owns_tx, "COPY FROM cancelled").await;
                    return Err(PostgresError::Operational(
                        "COPY FROM Arrow was cancelled during start".to_string(),
                    ));
                }
                if wire_cols != expected_cols {
                    abort_copy_in_lease(lease, owns_tx, "COPY FROM column count mismatch").await;
                    return Err(PostgresError::Protocol(format!(
                        "CopyInResponse column count {wire_cols} does not match expected {expected_cols}"
                    )));
                }
                CopyFromStream::new_with_encoder_and_plan(
                    lease,
                    wire_cols,
                    owns_tx,
                    ctx,
                    catalog,
                    encoder,
                    Some(plan),
                )
            }
            Err(err) => {
                if lease.connection_mut().is_closed() || lease.cancel_claimed() {
                    lease.finish_retired().await;
                } else {
                    lease.finish_clean().await;
                }
                return Err(err);
            }
        };

        if !first_payload.is_empty() {
            stream
                .flush_encoded_slice_internal(&first_payload, false)
                .await?;
            if stream
                .lease
                .as_ref()
                .is_some_and(CopySessionLease::cancel_claimed)
            {
                let _ = stream.abort_internal("COPY FROM cancelled").await;
                return Err(PostgresError::Operational(
                    "COPY FROM Arrow was cancelled during initial batch flush".to_string(),
                ));
            }
        }
        Ok(stream)
    }
}
