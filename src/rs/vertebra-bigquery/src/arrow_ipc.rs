//! BigQuery Storage Read/Write shared-memory `VTBARROW` IPC writers and Arrow frame planners.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use google_cloud_bigquery::model as bq_model;
use vertebra_core::shm::ShmRegion;
use vertebra_db::arrow_shm::{ARROW_IPC_EOS_MARKER, ShmArrowStreamWriter};

use crate::client::BigQueryError;
use crate::codec::BigQueryWriteMode;

/// Maximum Arrow payload size per `AppendRowsRequest` message (`9 MiB` safe bound under `10 MiB` limit).
pub const MAX_APPEND_ROWS_CHUNK_BYTES: usize = 9 * 1024 * 1024;

static SHM_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a unique process-scoped shared-memory segment name for BigQuery `VTBARROW` transfers.
pub fn next_bigquery_shm_name() -> String {
    let seq = SHM_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("/vtb-bq-{pid:x}-{seq:x}")
}

#[allow(deprecated)]
pub(crate) fn build_arrow_record_batch(req: &ArrowAppendRequest) -> bq_model::ArrowRecordBatch {
    bq_model::ArrowRecordBatch::new()
        .set_serialized_record_batch(req.record_batch.clone())
        .set_row_count(req.row_count)
}

pub(crate) fn write_read_rows_batches_to_shm(
    schema_bytes: &[u8],
    mut results_by_stream: Vec<(usize, Vec<(Bytes, usize)>)>,
) -> Result<(String, usize, usize, Arc<ShmRegion>), BigQueryError> {
    let shm_name = next_bigquery_shm_name();
    let mut writer = ShmArrowStreamWriter::create(&shm_name, 64 * 1024, schema_bytes)
        .map_err(BigQueryError::Shm)?;

    results_by_stream.sort_by_key(|(idx, _)| *idx);
    for (_, batches) in results_by_stream {
        for (batch_bytes, batch_rows) in batches {
            writer
                .append_raw_stream_chunk(&batch_bytes, batch_rows)
                .map_err(BigQueryError::Shm)?;
        }
    }

    let total_rows = writer.row_count();
    let (final_shm_name, byte_len, region) =
        writer.finish_unregistered().map_err(BigQueryError::Shm)?;
    Ok((final_shm_name, byte_len, total_rows, region))
}

/// One `AppendRowsRequest` worth of Arrow data: a single encapsulated record-batch message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArrowAppendRequest {
    pub record_batch: Bytes,
    pub row_count: i64,
    pub offset: Option<i64>,
}

/// Ordered Storage Write requests derived from one Arrow IPC stream.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ArrowAppendPlan {
    pub schema: Bytes,
    pub requests: Vec<ArrowAppendRequest>,
}

impl ArrowAppendPlan {
    /// Parse an Arrow IPC stream into its schema message and one [`ArrowAppendRequest`] per record batch.
    pub fn build(stream: &[u8], mode: BigQueryWriteMode) -> Result<Self, BigQueryError> {
        if stream.is_empty() {
            return Ok(Self::default());
        }

        let mut pos = 0usize;
        let mut schema: Option<Bytes> = None;
        let mut requests = Vec::new();
        let mut running_rows = 0i64;

        while pos < stream.len() {
            if stream[pos..].starts_with(&ARROW_IPC_EOS_MARKER) {
                break;
            }
            if stream.len() - pos < 8 {
                return Err(BigQueryError::InvalidArgument(format!(
                    "truncated Arrow IPC message header at byte {pos}"
                )));
            }
            if stream[pos..pos + 4] != [0xFF, 0xFF, 0xFF, 0xFF] {
                return Err(BigQueryError::InvalidArgument(format!(
                    "Arrow IPC message at byte {pos} lacks the 0xFFFFFFFF continuation marker"
                )));
            }
            let meta_size = u32::from_le_bytes([
                stream[pos + 4],
                stream[pos + 5],
                stream[pos + 6],
                stream[pos + 7],
            ]) as usize;
            if meta_size == 0 {
                break;
            }
            let padded_meta = meta_size.checked_add(7).map(|n| n & !7).ok_or_else(|| {
                BigQueryError::InvalidArgument(format!("truncated Arrow IPC message at byte {pos}"))
            })?;
            let meta_start = pos + 8;
            let meta_end = meta_start
                .checked_add(padded_meta)
                .filter(|&end| end <= stream.len())
                .ok_or_else(|| {
                    BigQueryError::InvalidArgument(format!(
                        "truncated Arrow IPC message at byte {pos}"
                    ))
                })?;

            let fb = &stream[meta_start..meta_start + meta_size];
            let header = ArrowMessageHeader::parse(fb)?;
            let padded_body = header
                .body_len
                .checked_add(7)
                .map(|n| n & !7)
                .ok_or_else(|| {
                    BigQueryError::InvalidArgument(format!(
                        "truncated Arrow IPC message at byte {pos}"
                    ))
                })?;
            let frame_end = meta_end
                .checked_add(padded_body)
                .filter(|&end| end <= stream.len())
                .ok_or_else(|| {
                    BigQueryError::InvalidArgument(format!(
                        "truncated Arrow IPC message at byte {pos}"
                    ))
                })?;

            let frame_bytes = Bytes::copy_from_slice(&stream[pos..frame_end]);
            match header.header_type {
                1 => {
                    if schema.is_some() {
                        return Err(BigQueryError::InvalidArgument(
                            "Arrow IPC stream contains more than one Schema message".to_string(),
                        ));
                    }
                    schema = Some(frame_bytes);
                }
                2 => {
                    return Err(BigQueryError::InvalidArgument(
                        "dictionary-encoded Arrow columns are not supported by AppendRows; cast dictionary columns to their value type before appending".to_string(),
                    ));
                }
                3 => {
                    if schema.is_none() {
                        return Err(BigQueryError::InvalidArgument(
                            "Arrow IPC stream must start with a Schema message".to_string(),
                        ));
                    }
                    let index = requests.len();
                    let len = frame_bytes.len();
                    if len > MAX_APPEND_ROWS_CHUNK_BYTES {
                        return Err(BigQueryError::InvalidArgument(format!(
                            "Arrow record batch {index} is {len} bytes; AppendRows accepts at most {MAX_APPEND_ROWS_CHUNK_BYTES} bytes per request; split the batch"
                        )));
                    }
                    let offset = match mode {
                        BigQueryWriteMode::Default => None,
                        BigQueryWriteMode::Committed
                        | BigQueryWriteMode::Pending
                        | BigQueryWriteMode::Buffered => Some(running_rows),
                    };
                    running_rows += header.row_count;
                    requests.push(ArrowAppendRequest {
                        record_batch: frame_bytes,
                        row_count: header.row_count,
                        offset,
                    });
                }
                t => {
                    return Err(BigQueryError::InvalidArgument(format!(
                        "unsupported Arrow IPC message type {t}"
                    )));
                }
            }
            pos = frame_end;
        }

        let Some(schema) = schema else {
            return Ok(Self::default());
        };
        Ok(Self { schema, requests })
    }

    /// Return the total number of rows across all planned record-batch requests.
    pub fn total_rows(&self) -> i64 {
        self.requests.iter().map(|r| r.row_count).sum()
    }

    /// Verify that `AppendRows` returned one response per request with matching offsets.
    pub fn verify_responses(&self, offsets: &[Option<i64>]) -> Result<u64, BigQueryError> {
        let got = offsets.len();
        let expected = self.requests.len();
        if got != expected {
            return Err(BigQueryError::AppendVerification(format!(
                "AppendRows returned {got} responses for {expected} requests"
            )));
        }
        for (i, (req, got_off)) in self.requests.iter().zip(offsets.iter()).enumerate() {
            if let Some(expected_off) = req.offset
                && *got_off != Some(expected_off)
            {
                return Err(BigQueryError::AppendVerification(format!(
                    "AppendRows response {i} reported offset {got_off:?}; expected {expected_off}"
                )));
            }
        }
        Ok(self.total_rows().max(0) as u64)
    }
}

struct ArrowMessageHeader {
    header_type: u8,
    body_len: usize,
    row_count: i64,
}

impl ArrowMessageHeader {
    fn parse(fb: &[u8]) -> Result<Self, BigQueryError> {
        let root_offset = read_u32_le(fb, 0)? as usize;
        let vtable_rel = read_i32_le(fb, root_offset)? as i64;
        let vtable_pos_i64 = (root_offset as i64) - vtable_rel;
        if vtable_pos_i64 < 0 || (vtable_pos_i64 as usize) >= fb.len() {
            return Err(BigQueryError::InvalidArgument(
                "malformed Arrow flatbuffer: vtable offset out of range".to_string(),
            ));
        }
        let vtable_pos = vtable_pos_i64 as usize;
        let vtable_len = read_u16_le(fb, vtable_pos)? as usize;
        if vtable_len < 4
            || vtable_pos
                .checked_add(vtable_len)
                .is_none_or(|end| end > fb.len())
        {
            return Err(BigQueryError::InvalidArgument(
                "malformed Arrow flatbuffer: vtable length out of range".to_string(),
            ));
        }

        let read_vtable_slot = |slot_offset: usize| -> Result<usize, BigQueryError> {
            if slot_offset + 2 > vtable_len {
                return Ok(0);
            }
            Ok(read_u16_le(fb, vtable_pos + slot_offset)? as usize)
        };

        let header_type_off = read_vtable_slot(6)?;
        if header_type_off == 0 {
            return Err(BigQueryError::InvalidArgument(
                "malformed Arrow flatbuffer: header_type slot unset".to_string(),
            ));
        }
        let header_type_pos = root_offset.checked_add(header_type_off).ok_or_else(|| {
            BigQueryError::InvalidArgument(
                "malformed Arrow flatbuffer: header_type offset overflow".to_string(),
            )
        })?;
        let header_type = *fb.get(header_type_pos).ok_or_else(|| {
            BigQueryError::InvalidArgument(format!(
                "malformed Arrow flatbuffer: offset {header_type_pos} out of range"
            ))
        })?;

        let body_len_off = read_vtable_slot(10)?;
        let body_len_i64 = if body_len_off == 0 {
            0
        } else {
            let p = root_offset.checked_add(body_len_off).ok_or_else(|| {
                BigQueryError::InvalidArgument(
                    "malformed Arrow flatbuffer: bodyLength offset overflow".to_string(),
                )
            })?;
            read_i64_le(fb, p)?
        };
        if body_len_i64 < 0 {
            return Err(BigQueryError::InvalidArgument(format!(
                "malformed Arrow flatbuffer: negative bodyLength {body_len_i64}"
            )));
        }
        let body_len = usize::try_from(body_len_i64).map_err(|_| {
            BigQueryError::InvalidArgument(
                "malformed Arrow flatbuffer: bodyLength overflows usize".to_string(),
            )
        })?;

        let mut row_count = 0i64;
        if header_type == 3 {
            let header_off = read_vtable_slot(8)?;
            if header_off > 0 {
                let union_loc = root_offset.checked_add(header_off).ok_or_else(|| {
                    BigQueryError::InvalidArgument(
                        "malformed Arrow flatbuffer: header offset overflow".to_string(),
                    )
                })?;
                let rel = read_u32_le(fb, union_loc)? as i64;
                let rb_table_i64 = (union_loc as i64) + rel;
                if rb_table_i64 < 0 || (rb_table_i64 as usize) >= fb.len() {
                    return Err(BigQueryError::InvalidArgument(
                        "malformed Arrow flatbuffer: RecordBatch table offset out of range"
                            .to_string(),
                    ));
                }
                let rb_table = rb_table_i64 as usize;
                let rb_vtable_rel = read_i32_le(fb, rb_table)? as i64;
                let rb_vtable_i64 = (rb_table as i64) - rb_vtable_rel;
                if rb_vtable_i64 < 0 || (rb_vtable_i64 as usize) >= fb.len() {
                    return Err(BigQueryError::InvalidArgument(
                        "malformed Arrow flatbuffer: RecordBatch vtable offset out of range"
                            .to_string(),
                    ));
                }
                let rb_vtable = rb_vtable_i64 as usize;
                let rb_vtable_len = read_u16_le(fb, rb_vtable)? as usize;
                if rb_vtable_len < 4
                    || rb_vtable
                        .checked_add(rb_vtable_len)
                        .is_none_or(|end| end > fb.len())
                {
                    return Err(BigQueryError::InvalidArgument(
                        "malformed Arrow flatbuffer: RecordBatch vtable length out of range"
                            .to_string(),
                    ));
                }
                if 6 <= rb_vtable_len {
                    let len_off = read_u16_le(fb, rb_vtable + 4)? as usize;
                    if len_off > 0 {
                        let lp = rb_table.checked_add(len_off).ok_or_else(|| {
                            BigQueryError::InvalidArgument(
                                "malformed Arrow flatbuffer: RecordBatch.length offset overflow"
                                    .to_string(),
                            )
                        })?;
                        row_count = read_i64_le(fb, lp)?;
                    }
                }
            }
            if row_count < 0 {
                return Err(BigQueryError::InvalidArgument(format!(
                    "malformed Arrow flatbuffer: negative RecordBatch length {row_count}"
                )));
            }
        }

        Ok(Self {
            header_type,
            body_len,
            row_count,
        })
    }
}

fn read_u16_le(fb: &[u8], at: usize) -> Result<u16, BigQueryError> {
    let slice = fb.get(at..at + 2).ok_or_else(|| {
        BigQueryError::InvalidArgument(format!(
            "malformed Arrow flatbuffer: offset {at} out of range"
        ))
    })?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u32_le(fb: &[u8], at: usize) -> Result<u32, BigQueryError> {
    let slice = fb.get(at..at + 4).ok_or_else(|| {
        BigQueryError::InvalidArgument(format!(
            "malformed Arrow flatbuffer: offset {at} out of range"
        ))
    })?;
    Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_i32_le(fb: &[u8], at: usize) -> Result<i32, BigQueryError> {
    let slice = fb.get(at..at + 4).ok_or_else(|| {
        BigQueryError::InvalidArgument(format!(
            "malformed Arrow flatbuffer: offset {at} out of range"
        ))
    })?;
    Ok(i32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_i64_le(fb: &[u8], at: usize) -> Result<i64, BigQueryError> {
    let slice = fb.get(at..at + 8).ok_or_else(|| {
        BigQueryError::InvalidArgument(format!(
            "malformed Arrow flatbuffer: offset {at} out of range"
        ))
    })?;
    Ok(i64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_append_plan_empty_and_truncated_headers() {
        let empty = ArrowAppendPlan::build(&[], BigQueryWriteMode::Default).expect("empty plan");
        assert!(empty.requests.is_empty());
        assert_eq!(empty.total_rows(), 0);

        let err = ArrowAppendPlan::build(&[0xFF, 0xFF, 0xFF], BigQueryWriteMode::Default)
            .expect_err("truncated header must fail");
        assert!(matches!(err, BigQueryError::InvalidArgument(_)));
    }
}
