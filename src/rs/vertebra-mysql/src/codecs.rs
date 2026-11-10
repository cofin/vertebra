//! MySQL binary and text protocol value/row codecs (`COM_STMT_PREPARE` / `COM_STMT_EXECUTE` / `0x00` rows).

use mysql_async::consts::{ColumnFlags, ColumnType};

use crate::client::DecodedCell;
use crate::errors::MySQLError;

/// Encode an unsigned 64-bit integer using MySQL length-encoded integer framing.
pub fn encode_lenenc_int(val: u64, out: &mut Vec<u8>) {
    if val < 251 {
        out.push(val as u8);
    } else if val < (1 << 16) {
        out.push(0xFC);
        out.extend_from_slice(&(val as u16).to_le_bytes());
    } else if val < (1 << 24) {
        out.push(0xFD);
        let b = (val as u32).to_le_bytes();
        out.extend_from_slice(&b[..3]);
    } else {
        out.push(0xFE);
        out.extend_from_slice(&val.to_le_bytes());
    }
}

/// Decode a MySQL length-encoded integer from the front of `buf`.
pub fn decode_lenenc_int(buf: &mut &[u8]) -> Result<u64, MySQLError> {
    if buf.is_empty() {
        return Err(MySQLError::protocol(
            "Unexpected EOF while decoding length-encoded integer",
        ));
    }
    let first = buf[0];
    *buf = &buf[1..];
    match first {
        0..=250 => Ok(first as u64),
        0xFC => {
            if buf.len() < 2 {
                return Err(MySQLError::protocol("Truncated 2-byte lenenc integer"));
            }
            let v = u16::from_le_bytes([buf[0], buf[1]]) as u64;
            *buf = &buf[2..];
            Ok(v)
        }
        0xFD => {
            if buf.len() < 3 {
                return Err(MySQLError::protocol("Truncated 3-byte lenenc integer"));
            }
            let v = u32::from_le_bytes([buf[0], buf[1], buf[2], 0]) as u64;
            *buf = &buf[3..];
            Ok(v)
        }
        0xFE => {
            if buf.len() < 8 {
                return Err(MySQLError::protocol("Truncated 8-byte lenenc integer"));
            }
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&buf[..8]);
            *buf = &buf[8..];
            Ok(u64::from_le_bytes(arr))
        }
        0xFB => Err(MySQLError::protocol(
            "Unexpected NULL marker (0xFB) in length-encoded integer",
        )),
        0xFF => Err(MySQLError::protocol(
            "Unexpected 0xFF marker in length-encoded integer",
        )),
    }
}

/// Encode a byte slice as a MySQL length-encoded byte string.
pub fn encode_lenenc_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    encode_lenenc_int(bytes.len() as u64, out);
    out.extend_from_slice(bytes);
}

/// Decode a MySQL length-encoded byte string from `buf`.
pub fn decode_lenenc_bytes(buf: &mut &[u8]) -> Result<Vec<u8>, MySQLError> {
    let len = decode_lenenc_int(buf)? as usize;
    if buf.len() < len {
        return Err(MySQLError::protocol(format!(
            "Truncated length-encoded bytes: expected {len}, got {}",
            buf.len()
        )));
    }
    let out = buf[..len].to_vec();
    *buf = &buf[len..];
    Ok(out)
}

/// Build a MySQL `ColumnDefinition41` packet payload for a column.
pub fn encode_column_definition_41(
    schema: &str,
    table: &str,
    name: &str,
    col_type: u8,
    col_flags: u16,
    decimals: u8,
) -> Vec<u8> {
    let mut pkt = Vec::with_capacity(64);
    encode_lenenc_bytes(b"def", &mut pkt);
    encode_lenenc_bytes(schema.as_bytes(), &mut pkt);
    encode_lenenc_bytes(table.as_bytes(), &mut pkt);
    encode_lenenc_bytes(table.as_bytes(), &mut pkt);
    encode_lenenc_bytes(name.as_bytes(), &mut pkt);
    encode_lenenc_bytes(name.as_bytes(), &mut pkt);
    pkt.push(0x0C);
    let charset: u16 = if (col_flags & ColumnFlags::BINARY_FLAG.bits()) != 0
        || col_type == ColumnType::MYSQL_TYPE_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_TINY_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_MEDIUM_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_LONG_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_BIT as u8
        || col_type == ColumnType::MYSQL_TYPE_VECTOR as u8
    {
        63 // binary collation
    } else {
        45 // utf8mb4_general_ci
    };
    pkt.extend_from_slice(&charset.to_le_bytes());
    pkt.extend_from_slice(&1024u32.to_le_bytes());
    pkt.push(col_type);
    pkt.extend_from_slice(&col_flags.to_le_bytes());
    pkt.push(decimals);
    pkt.extend_from_slice(&0u16.to_le_bytes());
    pkt
}

/// Encode a `DecodedCell` into its MySQL binary-protocol `(ColumnType, is_unsigned, payload)`.
pub fn encode_binary_cell(cell: &DecodedCell) -> (u8, bool, Vec<u8>) {
    match cell {
        DecodedCell::Null => (ColumnType::MYSQL_TYPE_NULL as u8, false, Vec::new()),
        DecodedCell::Bool(b) => (
            ColumnType::MYSQL_TYPE_TINY as u8,
            false,
            vec![if *b { 1 } else { 0 }],
        ),
        DecodedCell::Int(i) => (
            ColumnType::MYSQL_TYPE_LONGLONG as u8,
            false,
            i.to_le_bytes().to_vec(),
        ),
        DecodedCell::UInt(u) => (
            ColumnType::MYSQL_TYPE_LONGLONG as u8,
            true,
            u.to_le_bytes().to_vec(),
        ),
        DecodedCell::Float(f) => (
            ColumnType::MYSQL_TYPE_DOUBLE as u8,
            false,
            f.to_le_bytes().to_vec(),
        ),
        DecodedCell::Decimal(s) => {
            let mut out = Vec::new();
            encode_lenenc_bytes(s.as_bytes(), &mut out);
            (ColumnType::MYSQL_TYPE_NEWDECIMAL as u8, false, out)
        }
        DecodedCell::Json(s) => {
            let mut out = Vec::new();
            encode_lenenc_bytes(s.as_bytes(), &mut out);
            (ColumnType::MYSQL_TYPE_JSON as u8, false, out)
        }
        DecodedCell::Text(s) => {
            let mut out = Vec::new();
            encode_lenenc_bytes(s.as_bytes(), &mut out);
            (ColumnType::MYSQL_TYPE_VAR_STRING as u8, false, out)
        }
        DecodedCell::Bytes(b) => {
            let mut out = Vec::new();
            encode_lenenc_bytes(b, &mut out);
            (ColumnType::MYSQL_TYPE_BLOB as u8, false, out)
        }
        DecodedCell::Date(y, m, d) => {
            if *y == 0 && *m == 0 && *d == 0 {
                (ColumnType::MYSQL_TYPE_DATE as u8, false, vec![0])
            } else {
                let mut out = Vec::with_capacity(5);
                out.push(4);
                out.extend_from_slice(&(*y as u16).to_le_bytes());
                out.push(*m as u8);
                out.push(*d as u8);
                (ColumnType::MYSQL_TYPE_DATE as u8, false, out)
            }
        }
        DecodedCell::DateTime(y, m, d, hh, mm, ss, us) => {
            if *y == 0 && *m == 0 && *d == 0 && *hh == 0 && *mm == 0 && *ss == 0 && *us == 0 {
                (ColumnType::MYSQL_TYPE_DATETIME as u8, false, vec![0])
            } else if *hh == 0 && *mm == 0 && *ss == 0 && *us == 0 {
                let mut out = Vec::with_capacity(5);
                out.push(4);
                out.extend_from_slice(&(*y as u16).to_le_bytes());
                out.push(*m as u8);
                out.push(*d as u8);
                (ColumnType::MYSQL_TYPE_DATETIME as u8, false, out)
            } else if *us == 0 {
                let mut out = Vec::with_capacity(8);
                out.push(7);
                out.extend_from_slice(&(*y as u16).to_le_bytes());
                out.push(*m as u8);
                out.push(*d as u8);
                out.push(*hh as u8);
                out.push(*mm as u8);
                out.push(*ss as u8);
                (ColumnType::MYSQL_TYPE_DATETIME as u8, false, out)
            } else {
                let mut out = Vec::with_capacity(12);
                out.push(11);
                out.extend_from_slice(&(*y as u16).to_le_bytes());
                out.push(*m as u8);
                out.push(*d as u8);
                out.push(*hh as u8);
                out.push(*mm as u8);
                out.push(*ss as u8);
                out.extend_from_slice(&us.to_le_bytes());
                (ColumnType::MYSQL_TYPE_DATETIME as u8, false, out)
            }
        }
        DecodedCell::Time(is_neg, days, hh, mm, ss, us) => {
            if !*is_neg && *days == 0 && *hh == 0 && *mm == 0 && *ss == 0 && *us == 0 {
                (ColumnType::MYSQL_TYPE_TIME as u8, false, vec![0])
            } else if *us == 0 {
                let mut out = Vec::with_capacity(9);
                out.push(8);
                out.push(if *is_neg { 1 } else { 0 });
                out.extend_from_slice(&days.to_le_bytes());
                out.push(*hh as u8);
                out.push(*mm as u8);
                out.push(*ss as u8);
                (ColumnType::MYSQL_TYPE_TIME as u8, false, out)
            } else {
                let mut out = Vec::with_capacity(13);
                out.push(12);
                out.push(if *is_neg { 1 } else { 0 });
                out.extend_from_slice(&days.to_le_bytes());
                out.push(*hh as u8);
                out.push(*mm as u8);
                out.push(*ss as u8);
                out.extend_from_slice(&us.to_le_bytes());
                (ColumnType::MYSQL_TYPE_TIME as u8, false, out)
            }
        }
    }
}

/// Encode a `DecodedCell` for a specific column type in a MySQL Binary Protocol ResultsetRow (`0x00`).
pub fn encode_binary_cell_for_column(cell: &DecodedCell, col_type: u8, out: &mut Vec<u8>) {
    match cell {
        DecodedCell::Null => {}
        DecodedCell::Bool(b) => {
            out.push(if *b { 1 } else { 0 });
        }
        DecodedCell::Int(i) => {
            if col_type == ColumnType::MYSQL_TYPE_TINY as u8 {
                out.push(*i as i8 as u8);
            } else if col_type == ColumnType::MYSQL_TYPE_SHORT as u8
                || col_type == ColumnType::MYSQL_TYPE_YEAR as u8
            {
                out.extend_from_slice(&(*i as i16).to_le_bytes());
            } else if col_type == ColumnType::MYSQL_TYPE_LONG as u8
                || col_type == ColumnType::MYSQL_TYPE_INT24 as u8
            {
                out.extend_from_slice(&(*i as i32).to_le_bytes());
            } else {
                out.extend_from_slice(&i.to_le_bytes());
            }
        }
        DecodedCell::UInt(u) => {
            if col_type == ColumnType::MYSQL_TYPE_TINY as u8 {
                out.push(*u as u8);
            } else if col_type == ColumnType::MYSQL_TYPE_SHORT as u8
                || col_type == ColumnType::MYSQL_TYPE_YEAR as u8
            {
                out.extend_from_slice(&(*u as u16).to_le_bytes());
            } else if col_type == ColumnType::MYSQL_TYPE_LONG as u8
                || col_type == ColumnType::MYSQL_TYPE_INT24 as u8
            {
                out.extend_from_slice(&(*u as u32).to_le_bytes());
            } else {
                out.extend_from_slice(&u.to_le_bytes());
            }
        }
        DecodedCell::Float(f) => {
            if col_type == ColumnType::MYSQL_TYPE_FLOAT as u8 {
                out.extend_from_slice(&(*f as f32).to_le_bytes());
            } else {
                out.extend_from_slice(&f.to_le_bytes());
            }
        }
        _ => {
            let (_, _, bytes) = encode_binary_cell(cell);
            out.extend_from_slice(&bytes);
        }
    }
}

/// Decode a single non-NULL value from a MySQL binary protocol stream (`COM_STMT_EXECUTE` or `0x00` row).
pub fn decode_binary_cell(
    col_type: u8,
    is_unsigned: bool,
    buf: &mut &[u8],
) -> Result<DecodedCell, MySQLError> {
    if col_type == ColumnType::MYSQL_TYPE_NULL as u8 {
        return Ok(DecodedCell::Null);
    }
    if col_type == ColumnType::MYSQL_TYPE_TINY as u8 {
        if buf.is_empty() {
            return Err(MySQLError::protocol("Truncated MYSQL_TYPE_TINY"));
        }
        let b = buf[0];
        *buf = &buf[1..];
        return if is_unsigned {
            Ok(DecodedCell::UInt(b as u64))
        } else {
            Ok(DecodedCell::Int((b as i8) as i64))
        };
    }
    if col_type == ColumnType::MYSQL_TYPE_SHORT as u8
        || col_type == ColumnType::MYSQL_TYPE_YEAR as u8
    {
        if buf.len() < 2 {
            return Err(MySQLError::protocol("Truncated MYSQL_TYPE_SHORT/YEAR"));
        }
        let raw = [buf[0], buf[1]];
        *buf = &buf[2..];
        return if is_unsigned || col_type == ColumnType::MYSQL_TYPE_YEAR as u8 {
            Ok(DecodedCell::Int(u16::from_le_bytes(raw) as i64))
        } else {
            Ok(DecodedCell::Int(i16::from_le_bytes(raw) as i64))
        };
    }
    if col_type == ColumnType::MYSQL_TYPE_LONG as u8
        || col_type == ColumnType::MYSQL_TYPE_INT24 as u8
    {
        if buf.len() < 4 {
            return Err(MySQLError::protocol("Truncated MYSQL_TYPE_LONG/INT24"));
        }
        let raw = [buf[0], buf[1], buf[2], buf[3]];
        *buf = &buf[4..];
        return if is_unsigned {
            Ok(DecodedCell::UInt(u32::from_le_bytes(raw) as u64))
        } else {
            Ok(DecodedCell::Int(i32::from_le_bytes(raw) as i64))
        };
    }
    if col_type == ColumnType::MYSQL_TYPE_LONGLONG as u8 {
        if buf.len() < 8 {
            return Err(MySQLError::protocol("Truncated MYSQL_TYPE_LONGLONG"));
        }
        let mut raw = [0u8; 8];
        raw.copy_from_slice(&buf[..8]);
        *buf = &buf[8..];
        return if is_unsigned {
            Ok(DecodedCell::UInt(u64::from_le_bytes(raw)))
        } else {
            Ok(DecodedCell::Int(i64::from_le_bytes(raw)))
        };
    }
    if col_type == ColumnType::MYSQL_TYPE_FLOAT as u8 {
        if buf.len() < 4 {
            return Err(MySQLError::protocol("Truncated MYSQL_TYPE_FLOAT"));
        }
        let raw = [buf[0], buf[1], buf[2], buf[3]];
        *buf = &buf[4..];
        return Ok(DecodedCell::Float(f32::from_le_bytes(raw) as f64));
    }
    if col_type == ColumnType::MYSQL_TYPE_DOUBLE as u8 {
        if buf.len() < 8 {
            return Err(MySQLError::protocol("Truncated MYSQL_TYPE_DOUBLE"));
        }
        let mut raw = [0u8; 8];
        raw.copy_from_slice(&buf[..8]);
        *buf = &buf[8..];
        return Ok(DecodedCell::Float(f64::from_le_bytes(raw)));
    }
    if col_type == ColumnType::MYSQL_TYPE_DATE as u8
        || col_type == ColumnType::MYSQL_TYPE_NEWDATE as u8
        || col_type == ColumnType::MYSQL_TYPE_DATETIME as u8
        || col_type == ColumnType::MYSQL_TYPE_TIMESTAMP as u8
        || col_type == ColumnType::MYSQL_TYPE_DATETIME2 as u8
        || col_type == ColumnType::MYSQL_TYPE_TIMESTAMP2 as u8
    {
        if buf.is_empty() {
            return Err(MySQLError::protocol(
                "Truncated binary DATE/DATETIME length",
            ));
        }
        let len = buf[0] as usize;
        *buf = &buf[1..];
        if buf.len() < len {
            return Err(MySQLError::protocol(
                "Truncated binary DATE/DATETIME payload",
            ));
        }
        let data = &buf[..len];
        *buf = &buf[len..];
        let (y, m, d, hh, mm, ss, us) = match len {
            0 => (0i32, 0u32, 0u32, 0u32, 0u32, 0u32, 0u32),
            4 => (
                u16::from_le_bytes([data[0], data[1]]) as i32,
                data[2] as u32,
                data[3] as u32,
                0,
                0,
                0,
                0,
            ),
            7 => (
                u16::from_le_bytes([data[0], data[1]]) as i32,
                data[2] as u32,
                data[3] as u32,
                data[4] as u32,
                data[5] as u32,
                data[6] as u32,
                0,
            ),
            11 => (
                u16::from_le_bytes([data[0], data[1]]) as i32,
                data[2] as u32,
                data[3] as u32,
                data[4] as u32,
                data[5] as u32,
                data[6] as u32,
                u32::from_le_bytes([data[7], data[8], data[9], data[10]]),
            ),
            other => {
                return Err(MySQLError::protocol(format!(
                    "Invalid MySQL binary DATE/DATETIME length: {other}"
                )));
            }
        };
        if col_type == ColumnType::MYSQL_TYPE_DATE as u8
            || col_type == ColumnType::MYSQL_TYPE_NEWDATE as u8
        {
            return Ok(DecodedCell::Date(y, m, d));
        }
        return Ok(DecodedCell::DateTime(y, m, d, hh, mm, ss, us));
    }
    if col_type == ColumnType::MYSQL_TYPE_TIME as u8
        || col_type == ColumnType::MYSQL_TYPE_TIME2 as u8
    {
        if buf.is_empty() {
            return Err(MySQLError::protocol("Truncated binary TIME length"));
        }
        let len = buf[0] as usize;
        *buf = &buf[1..];
        if buf.len() < len {
            return Err(MySQLError::protocol("Truncated binary TIME payload"));
        }
        let data = &buf[..len];
        *buf = &buf[len..];
        return match len {
            0 => Ok(DecodedCell::Time(false, 0, 0, 0, 0, 0)),
            8 => Ok(DecodedCell::Time(
                data[0] != 0,
                u32::from_le_bytes([data[1], data[2], data[3], data[4]]),
                data[5] as u32,
                data[6] as u32,
                data[7] as u32,
                0,
            )),
            12 => Ok(DecodedCell::Time(
                data[0] != 0,
                u32::from_le_bytes([data[1], data[2], data[3], data[4]]),
                data[5] as u32,
                data[6] as u32,
                data[7] as u32,
                u32::from_le_bytes([data[8], data[9], data[10], data[11]]),
            )),
            other => Err(MySQLError::protocol(format!(
                "Invalid MySQL binary TIME length: {other}"
            ))),
        };
    }

    let bytes = decode_lenenc_bytes(buf)?;
    if col_type == ColumnType::MYSQL_TYPE_DECIMAL as u8
        || col_type == ColumnType::MYSQL_TYPE_NEWDECIMAL as u8
    {
        let s = String::from_utf8(bytes)
            .map_err(|e| MySQLError::data(format!("Invalid UTF-8 in DECIMAL: {e}")))?;
        return Ok(DecodedCell::Decimal(s));
    }
    if col_type == ColumnType::MYSQL_TYPE_JSON as u8 {
        let s = String::from_utf8(bytes)
            .map_err(|e| MySQLError::data(format!("Invalid UTF-8 in JSON: {e}")))?;
        return Ok(DecodedCell::Json(s));
    }
    if col_type == ColumnType::MYSQL_TYPE_BIT as u8
        || col_type == ColumnType::MYSQL_TYPE_VECTOR as u8
        || col_type == ColumnType::MYSQL_TYPE_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_TINY_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_MEDIUM_BLOB as u8
        || col_type == ColumnType::MYSQL_TYPE_LONG_BLOB as u8
    {
        return Ok(DecodedCell::Bytes(bytes));
    }
    match String::from_utf8(bytes) {
        Ok(s) => Ok(DecodedCell::Text(s)),
        Err(err) => Ok(DecodedCell::Bytes(err.into_bytes())),
    }
}

/// Encode a full MySQL Binary Protocol ResultsetRow (`0x00` header + NULL bitmap with `+2` offset + values).
pub fn encode_binary_row_packet(cells: &[DecodedCell], columns: &[(String, u8)]) -> Vec<u8> {
    let num_cols = columns.len();
    let bitmap_len = (num_cols + 7 + 2) / 8;
    let mut pkt = Vec::with_capacity(1 + bitmap_len + num_cols * 8);
    pkt.push(0x00);
    pkt.resize(1 + bitmap_len, 0u8);

    for (idx, cell) in cells.iter().enumerate() {
        if matches!(cell, DecodedCell::Null) {
            let bit_pos = idx + 2;
            pkt[1 + (bit_pos / 8)] |= 1 << (bit_pos % 8);
        }
    }

    for (idx, cell) in cells.iter().enumerate() {
        let col_type = columns
            .get(idx)
            .map(|(_, t)| *t)
            .unwrap_or(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
        encode_binary_cell_for_column(cell, col_type, &mut pkt);
    }
    pkt
}

/// Decode a MySQL Binary Protocol ResultsetRow (`0x00` header + NULL bitmap with `+2` offset + values).
pub fn decode_binary_row_packet(
    packet: &[u8],
    columns: &[(String, u8, bool)],
) -> Result<Vec<DecodedCell>, MySQLError> {
    if packet.is_empty() || packet[0] != 0x00 {
        return Err(MySQLError::protocol(
            "Binary resultset row packet must start with 0x00 header",
        ));
    }
    let num_cols = columns.len();
    let bitmap_len = (num_cols + 7 + 2) / 8;
    if packet.len() < 1 + bitmap_len {
        return Err(MySQLError::protocol(
            "Truncated NULL bitmap in binary resultset row packet",
        ));
    }
    let null_bitmap = &packet[1..1 + bitmap_len];
    let mut cursor = &packet[1 + bitmap_len..];
    let mut cells = Vec::with_capacity(num_cols);

    for (idx, (_, col_type, is_unsigned)) in columns.iter().enumerate() {
        let bit_pos = idx + 2;
        let is_null = (null_bitmap[bit_pos / 8] & (1 << (bit_pos % 8))) != 0;
        if is_null {
            cells.push(DecodedCell::Null);
        } else {
            cells.push(decode_binary_cell(*col_type, *is_unsigned, &mut cursor)?);
        }
    }
    Ok(cells)
}

/// Encode a `COM_STMT_EXECUTE` (`0x17`) command packet payload.
pub fn encode_com_stmt_execute(stmt_id: u32, params: &[DecodedCell]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16 + params.len() * 8);
    payload.push(0x17);
    payload.extend_from_slice(&stmt_id.to_le_bytes());
    payload.push(0x00); // CURSOR_TYPE_NO_CURSOR
    payload.extend_from_slice(&1u32.to_le_bytes()); // iteration_count = 1

    if !params.is_empty() {
        let bitmap_len = params.len().div_ceil(8);
        let bitmap_start = payload.len();
        payload.resize(bitmap_start + bitmap_len, 0u8);

        let mut encoded_params = Vec::with_capacity(params.len());
        for (idx, param) in params.iter().enumerate() {
            if matches!(param, DecodedCell::Null) {
                payload[bitmap_start + (idx / 8)] |= 1 << (idx % 8);
            }
            encoded_params.push(encode_binary_cell(param));
        }

        payload.push(0x01); // new-params-bound-flag = 1
        for (col_type, is_unsigned, _) in &encoded_params {
            payload.push(*col_type);
            payload.push(if *is_unsigned { 0x80 } else { 0x00 });
        }
        for (_, _, bytes) in encoded_params {
            payload.extend_from_slice(&bytes);
        }
    }
    payload
}

/// Decode a `COM_STMT_EXECUTE` (`0x17`) command packet payload on the server side.
pub fn decode_com_stmt_execute(
    payload: &[u8],
    param_count: usize,
    bound_types: &mut Vec<(u8, bool)>,
) -> Result<(u32, Vec<DecodedCell>), MySQLError> {
    if payload.len() < 10 || payload[0] != 0x17 {
        return Err(MySQLError::protocol("Invalid COM_STMT_EXECUTE packet"));
    }
    let stmt_id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
    if param_count == 0 {
        return Ok((stmt_id, Vec::new()));
    }
    let bitmap_len = param_count.div_ceil(8);
    if payload.len() < 10 + bitmap_len + 1 {
        return Err(MySQLError::protocol(
            "Truncated COM_STMT_EXECUTE NULL bitmap or bound flag",
        ));
    }
    let null_bitmap = &payload[10..10 + bitmap_len];
    let new_params_bound = payload[10 + bitmap_len];
    let mut cursor = &payload[10 + bitmap_len + 1..];

    if new_params_bound == 1 {
        if cursor.len() < param_count * 2 {
            return Err(MySQLError::protocol(
                "Truncated COM_STMT_EXECUTE parameter type descriptors",
            ));
        }
        bound_types.clear();
        for i in 0..param_count {
            let col_type = cursor[i * 2];
            let is_unsigned = (cursor[i * 2 + 1] & 0x80) != 0;
            bound_types.push((col_type, is_unsigned));
        }
        cursor = &cursor[param_count * 2..];
    } else if bound_types.len() != param_count {
        return Err(MySQLError::protocol(
            "COM_STMT_EXECUTE sent new_params_bound=0 without prior parameter types",
        ));
    }

    let mut params = Vec::with_capacity(param_count);
    for (idx, (col_type, is_unsigned)) in bound_types.iter().enumerate() {
        let is_null = (null_bitmap[idx / 8] & (1 << (idx % 8))) != 0;
        if is_null {
            params.push(DecodedCell::Null);
        } else {
            params.push(decode_binary_cell(*col_type, *is_unsigned, &mut cursor)?);
        }
    }
    Ok((stmt_id, params))
}

/// Encode a Text Protocol ResultsetRow packet (`lenenc_bytes` per cell, `0xFB` for `NULL`).
pub fn encode_text_row_packet(cells: &[DecodedCell]) -> Vec<u8> {
    let mut pkt = Vec::with_capacity(cells.len() * 8);
    for cell in cells {
        match cell {
            DecodedCell::Null => pkt.push(0xFB),
            DecodedCell::Bool(b) => encode_lenenc_bytes(if *b { b"1" } else { b"0" }, &mut pkt),
            DecodedCell::Int(i) => encode_lenenc_bytes(i.to_string().as_bytes(), &mut pkt),
            DecodedCell::UInt(u) => encode_lenenc_bytes(u.to_string().as_bytes(), &mut pkt),
            DecodedCell::Float(f) => encode_lenenc_bytes(f.to_string().as_bytes(), &mut pkt),
            DecodedCell::Decimal(s) | DecodedCell::Json(s) | DecodedCell::Text(s) => {
                encode_lenenc_bytes(s.as_bytes(), &mut pkt);
            }
            DecodedCell::Bytes(b) => encode_lenenc_bytes(b, &mut pkt),
            DecodedCell::Date(y, m, d) => {
                let s = format!("{y:04}-{m:02}-{d:02}");
                encode_lenenc_bytes(s.as_bytes(), &mut pkt);
            }
            DecodedCell::DateTime(y, m, d, hh, mm, ss, us) => {
                let s = if *us > 0 {
                    format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{us:06}")
                } else {
                    format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}")
                };
                encode_lenenc_bytes(s.as_bytes(), &mut pkt);
            }
            DecodedCell::Time(is_neg, days, hh, mm, ss, us) => {
                let sign = if *is_neg { "-" } else { "" };
                let total_hh = days * 24 + hh;
                let s = if *us > 0 {
                    format!("{sign}{total_hh:02}:{mm:02}:{ss:02}.{us:06}")
                } else {
                    format!("{sign}{total_hh:02}:{mm:02}:{ss:02}")
                };
                encode_lenenc_bytes(s.as_bytes(), &mut pkt);
            }
        }
    }
    pkt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_row_and_stmt_execute_roundtrip_all_types() {
        let columns = vec![
            ("c_null".to_string(), ColumnType::MYSQL_TYPE_NULL as u8),
            ("c_tiny".to_string(), ColumnType::MYSQL_TYPE_TINY as u8),
            ("c_short".to_string(), ColumnType::MYSQL_TYPE_SHORT as u8),
            ("c_long".to_string(), ColumnType::MYSQL_TYPE_LONG as u8),
            (
                "c_longlong".to_string(),
                ColumnType::MYSQL_TYPE_LONGLONG as u8,
            ),
            (
                "c_ulonglong".to_string(),
                ColumnType::MYSQL_TYPE_LONGLONG as u8,
            ),
            ("c_double".to_string(), ColumnType::MYSQL_TYPE_DOUBLE as u8),
            (
                "c_decimal".to_string(),
                ColumnType::MYSQL_TYPE_NEWDECIMAL as u8,
            ),
            ("c_date".to_string(), ColumnType::MYSQL_TYPE_DATE as u8),
            (
                "c_datetime".to_string(),
                ColumnType::MYSQL_TYPE_DATETIME as u8,
            ),
            ("c_time".to_string(), ColumnType::MYSQL_TYPE_TIME as u8),
            ("c_json".to_string(), ColumnType::MYSQL_TYPE_JSON as u8),
            ("c_blob".to_string(), ColumnType::MYSQL_TYPE_BLOB as u8),
            ("c_vector".to_string(), ColumnType::MYSQL_TYPE_VECTOR as u8),
            ("c_bit".to_string(), ColumnType::MYSQL_TYPE_BIT as u8),
            (
                "c_enum".to_string(),
                ColumnType::MYSQL_TYPE_VAR_STRING as u8,
            ),
        ];

        let cells = vec![
            DecodedCell::Null,
            DecodedCell::Int(-12),
            DecodedCell::Int(32000),
            DecodedCell::Int(-2_000_000),
            DecodedCell::Int(-9_000_000_000_000),
            DecodedCell::UInt(18_000_000_000_000_000_000),
            DecodedCell::Float(42.125),
            DecodedCell::Decimal("1234567890.0123456789".to_string()),
            DecodedCell::Date(2026, 11, 9),
            DecodedCell::DateTime(2026, 11, 9, 14, 30, 45, 654321),
            DecodedCell::Time(true, 1, 12, 34, 56, 789012),
            DecodedCell::Json("{\"k\":[1,2,3]}".to_string()),
            DecodedCell::Bytes(vec![0x00, 0xFF, 0x42]),
            DecodedCell::Bytes(vec![0x00, 0x00, 0x80, 0x3F]),
            DecodedCell::Bytes(vec![0b0000_0101]),
            DecodedCell::Text("active".to_string()),
        ];

        let row_pkt = encode_binary_row_packet(&cells, &columns);
        let col_meta: Vec<(String, u8, bool)> = columns
            .iter()
            .enumerate()
            .map(|(idx, (name, ct))| (name.clone(), *ct, idx == 5))
            .collect();
        let decoded = decode_binary_row_packet(&row_pkt, &col_meta).unwrap();
        assert_eq!(decoded, cells);

        let exec_pkt = encode_com_stmt_execute(42, &cells);
        let mut bound_types = Vec::new();
        let (stmt_id, exec_params) =
            decode_com_stmt_execute(&exec_pkt, cells.len(), &mut bound_types).unwrap();
        assert_eq!(stmt_id, 42);
        assert_eq!(exec_params.len(), cells.len());
        assert_eq!(exec_params[0], DecodedCell::Null);
        assert_eq!(
            exec_params[7],
            DecodedCell::Decimal("1234567890.0123456789".to_string())
        );
        assert_eq!(exec_params[8], DecodedCell::Date(2026, 11, 9));
        assert_eq!(
            exec_params[9],
            DecodedCell::DateTime(2026, 11, 9, 14, 30, 45, 654321)
        );
        assert_eq!(
            exec_params[10],
            DecodedCell::Time(true, 1, 12, 34, 56, 789012)
        );
    }
}
