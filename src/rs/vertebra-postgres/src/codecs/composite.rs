//! PostgreSQL composite (`record_send`/`record_recv`, `record_out`/`record_in`) binary and text codecs.

use bytes::{BufMut, BytesMut};

use crate::codecs::array_text::{ContainerNullRule, ScannedToken, scan_delimited_container};
use crate::codecs::{
    CodecContext, decode_value_with_catalog_at_depth, encode_value_with_catalog_at_depth,
    format_for_oid_with_catalog,
};
use crate::errors::PostgresError;
use crate::result::{Format, PgValue};
use crate::types::{CompositeField, TypeCatalog, TypeInfo, TypeKind};

/// PostgreSQL anonymous `record` type OID (`2249`).
pub const RECORD_OID: u32 = 2249;
/// PostgreSQL `_record` array type OID (`2287`).
pub const RECORD_ARRAY_OID: u32 = 2287;

/// Decode a PostgreSQL binary composite or anonymous `record` (`2249`) payload into [`PgValue::Composite`].
pub fn decode_composite_binary(
    type_oid: u32,
    raw: &[u8],
    ctx: &CodecContext,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<PgValue, PostgresError> {
    if depth > ctx.max_nesting_depth {
        return Err(PostgresError::Data(format!(
            "composite nesting depth {depth} exceeds max_nesting_depth {}",
            ctx.max_nesting_depth
        )));
    }
    if raw.len() < 4 {
        return Err(PostgresError::Data(format!(
            "binary composite payload too short ({} bytes < 4)",
            raw.len()
        )));
    }

    let nfields_i32 = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
    if nfields_i32 < 0 {
        return Err(PostgresError::Data(format!(
            "binary composite has negative field count {nfields_i32}"
        )));
    }
    let nfields = nfields_i32 as usize;
    let remaining = raw.len() - 4;
    let min_bytes = nfields.checked_mul(8).ok_or_else(|| {
        PostgresError::Data("binary composite field count overflowed usize".to_string())
    })?;
    if remaining < min_bytes {
        return Err(PostgresError::Data(format!(
            "binary composite header claims {nfields} fields requiring at least {min_bytes} bytes, but only {remaining} bytes remain"
        )));
    }

    let base_oid = catalog.resolve_domain_base(type_oid)?;
    let expected_fields: Option<&[CompositeField]> = if base_oid == RECORD_OID {
        None
    } else {
        match catalog.type_info(base_oid) {
            Some(TypeInfo {
                kind: TypeKind::Composite { fields },
                ..
            }) => {
                if fields.len() != nfields {
                    return Err(PostgresError::Data(format!(
                        "binary composite OID {type_oid} field count mismatch: catalog has {}, wire has {nfields}",
                        fields.len()
                    )));
                }
                Some(fields.as_slice())
            }
            _ => {
                return Err(PostgresError::Data(format!(
                    "cannot decode unregistered composite OID {type_oid} from binary format"
                )));
            }
        }
    };

    let mut field_names = Vec::with_capacity(nfields);
    let mut field_oids = Vec::with_capacity(nfields);
    let mut values = Vec::with_capacity(nfields);
    let mut pos = 4usize;

    for idx in 0..nfields {
        if pos + 8 > raw.len() {
            return Err(PostgresError::Data(format!(
                "binary composite truncated reading field {idx} header"
            )));
        }
        let wire_field_oid =
            u32::from_be_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]);
        let field_len =
            i32::from_be_bytes([raw[pos + 4], raw[pos + 5], raw[pos + 6], raw[pos + 7]]);
        pos += 8;

        if wire_field_oid == 0 {
            return Err(PostgresError::Data(format!(
                "binary composite field {idx} has invalid type OID 0"
            )));
        }

        let (field_name, decode_oid) = if let Some(schema_fields) = expected_fields {
            let expected = &schema_fields[idx];
            let expected_base = catalog.resolve_domain_base(expected.type_oid)?;
            if wire_field_oid != expected.type_oid && wire_field_oid != expected_base {
                return Err(PostgresError::Data(format!(
                    "binary composite OID {type_oid} field {idx} (`{}`) OID mismatch: expected {}, got {wire_field_oid}",
                    expected.name, expected.type_oid
                )));
            }
            (expected.name.clone(), expected.type_oid)
        } else {
            (format!("f{}", idx + 1), wire_field_oid)
        };

        if field_len == -1 {
            field_names.push(field_name);
            field_oids.push(decode_oid);
            values.push(PgValue::Null);
        } else if field_len < -1 {
            return Err(PostgresError::Data(format!(
                "binary composite field {idx} has invalid negative length {field_len}"
            )));
        } else {
            let len = field_len as usize;
            if pos + len > raw.len() {
                return Err(PostgresError::Data(format!(
                    "binary composite field {idx} payload truncated (expected {len} bytes, {} remaining)",
                    raw.len() - pos
                )));
            }
            let slice = &raw[pos..pos + len];
            pos += len;
            let val = decode_value_with_catalog_at_depth(
                decode_oid,
                Format::Binary,
                Some(slice),
                ctx,
                catalog,
                depth + 1,
            )
            .map_err(|err| {
                PostgresError::Data(format!(
                    "failed to decode composite field {idx} (`{field_name}`): {err}"
                ))
            })?;
            field_names.push(field_name);
            field_oids.push(decode_oid);
            values.push(val);
        }
    }

    if pos != raw.len() {
        return Err(PostgresError::Data(format!(
            "binary composite has {} trailing bytes after {nfields} fields",
            raw.len() - pos
        )));
    }

    Ok(PgValue::Composite {
        type_oid,
        field_names,
        field_oids,
        values,
    })
}

fn validate_composite_for_encode<'a>(
    value: &'a PgValue,
    target_oid: u32,
    catalog: &'a TypeCatalog,
) -> Result<(u32, &'a [CompositeField], &'a [PgValue]), PostgresError> {
    let PgValue::Composite {
        type_oid,
        field_names,
        field_oids,
        values,
    } = value
    else {
        return Err(PostgresError::Data(format!(
            "expected PgValue::Composite for composite encoding, got {value:?}"
        )));
    };

    let effective_oid = if target_oid != 0 {
        target_oid
    } else {
        *type_oid
    };
    if effective_oid == 0 || effective_oid == RECORD_OID {
        return Err(PostgresError::Data(
            "cannot bind anonymous record (OID 2249) without a concrete composite target OID"
                .to_string(),
        ));
    }
    if *type_oid != 0 && *type_oid != RECORD_OID && *type_oid != effective_oid {
        let val_base = catalog.resolve_domain_base(*type_oid)?;
        let tgt_base = catalog.resolve_domain_base(effective_oid)?;
        if val_base != tgt_base {
            return Err(PostgresError::Data(format!(
                "PgComposite type_oid {type_oid} does not match target_oid {effective_oid}"
            )));
        }
    }

    let base_oid = catalog.resolve_domain_base(effective_oid)?;
    let Some(TypeInfo {
        kind: TypeKind::Composite { fields },
        ..
    }) = catalog.type_info(base_oid)
    else {
        return Err(PostgresError::Data(format!(
            "target OID {effective_oid} is not a known composite type in TypeCatalog"
        )));
    };

    if values.len() != fields.len()
        || field_names.len() != fields.len()
        || field_oids.len() != fields.len()
    {
        return Err(PostgresError::Data(format!(
            "PgComposite attribute count ({}) does not match catalog composite OID {effective_oid} field count ({})",
            values.len(),
            fields.len()
        )));
    }

    for (idx, field) in fields.iter().enumerate() {
        if field_names[idx] != field.name {
            return Err(PostgresError::Data(format!(
                "PgComposite field {idx} name `{}` does not match catalog field `{}`",
                field_names[idx], field.name
            )));
        }
        let f_base = catalog.resolve_domain_base(field_oids[idx])?;
        let c_base = catalog.resolve_domain_base(field.type_oid)?;
        if field_oids[idx] != field.type_oid && f_base != c_base {
            return Err(PostgresError::Data(format!(
                "PgComposite field {idx} (`{}`) OID {} does not match catalog OID {}",
                field.name, field_oids[idx], field.type_oid
            )));
        }
    }

    Ok((effective_oid, fields.as_slice(), values.as_slice()))
}

/// Encode a [`PgValue::Composite`] into PostgreSQL binary `record_recv` format.
pub fn encode_composite_binary(
    value: &PgValue,
    target_oid: u32,
    out: &mut BytesMut,
    ctx: &CodecContext,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<(), PostgresError> {
    if depth > ctx.max_nesting_depth {
        return Err(PostgresError::Data(format!(
            "composite nesting depth {depth} exceeds max_nesting_depth {}",
            ctx.max_nesting_depth
        )));
    }
    let (_effective_oid, fields, values) =
        validate_composite_for_encode(value, target_oid, catalog)?;

    out.put_i32(fields.len() as i32);
    for (idx, (field, val)) in fields.iter().zip(values.iter()).enumerate() {
        if format_for_oid_with_catalog(field.type_oid, catalog) != Format::Binary {
            return Err(PostgresError::Data(format!(
                "composite field {idx} (`{}`) OID {} does not support binary encoding",
                field.name, field.type_oid
            )));
        }
        let wire_field_oid = catalog.resolve_domain_base(field.type_oid)?;
        out.put_u32(wire_field_oid);
        if matches!(val, PgValue::Null) {
            out.put_i32(-1);
        } else {
            let len_pos = out.len();
            out.put_i32(0);
            let payload_start = out.len();
            encode_value_with_catalog_at_depth(
                val,
                field.type_oid,
                Format::Binary,
                out,
                ctx,
                catalog,
                depth + 1,
            )?;
            let payload_len = out.len() - payload_start;
            let len_i32 = i32::try_from(payload_len).map_err(|_| {
                PostgresError::Data(format!(
                    "encoded composite field {idx} length {payload_len} exceeds i32::MAX"
                ))
            })?;
            out[len_pos..len_pos + 4].copy_from_slice(&len_i32.to_be_bytes());
        }
    }

    Ok(())
}

/// Decode a PostgreSQL text composite (`record_out`) literal using known ordered [`CompositeField`] metadata.
pub fn decode_composite_text(
    type_oid: u32,
    fields: &[CompositeField],
    text: &str,
    ctx: &CodecContext,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<PgValue, PostgresError> {
    if depth > ctx.max_nesting_depth {
        return Err(PostgresError::Data(format!(
            "composite nesting depth {depth} exceeds max_nesting_depth {}",
            ctx.max_nesting_depth
        )));
    }

    let tokens = scan_delimited_container(
        text,
        b'(',
        b')',
        b',',
        ContainerNullRule::CompositeEmptyUnquoted,
        Some(fields.len()),
        1,
    )?;

    let mut field_names = Vec::with_capacity(fields.len());
    let mut field_oids = Vec::with_capacity(fields.len());
    let mut values = Vec::with_capacity(fields.len());

    for (idx, (tok, field)) in tokens.into_iter().zip(fields.iter()).enumerate() {
        field_names.push(field.name.clone());
        field_oids.push(field.type_oid);
        match tok {
            ScannedToken::Null => values.push(PgValue::Null),
            ScannedToken::Value(s) => {
                let decoded = decode_value_with_catalog_at_depth(
                    field.type_oid,
                    Format::Text,
                    Some(s.as_bytes()),
                    ctx,
                    catalog,
                    depth + 1,
                )
                .map_err(|err| {
                    PostgresError::Data(format!(
                        "failed to decode text composite field {idx} (`{}`): {err}",
                        field.name
                    ))
                })?;
                values.push(decoded);
            }
            ScannedToken::SubArray(_) => {
                return Err(PostgresError::Data(
                    "unexpected sub-array token inside composite literal".to_string(),
                ));
            }
        }
    }

    Ok(PgValue::Composite {
        type_oid,
        field_names,
        field_oids,
        values,
    })
}

fn needs_composite_quoting(s: &str) -> bool {
    if s.is_empty() {
        return true;
    }
    s.as_bytes().iter().any(|&b| {
        b == b'(' || b == b')' || b == b',' || b == b'"' || b == b'\\' || b.is_ascii_whitespace()
    })
}

/// Encode a [`PgValue::Composite`] into PostgreSQL `record_in` text representation.
pub fn encode_composite_text(
    value: &PgValue,
    target_oid: u32,
    out: &mut BytesMut,
    ctx: &CodecContext,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<(), PostgresError> {
    if depth > ctx.max_nesting_depth {
        return Err(PostgresError::Data(format!(
            "composite nesting depth {depth} exceeds max_nesting_depth {}",
            ctx.max_nesting_depth
        )));
    }
    let (_effective_oid, fields, values) =
        validate_composite_for_encode(value, target_oid, catalog)?;

    out.put_u8(b'(');
    let mut field_buf = BytesMut::new();
    for (idx, (field, val)) in fields.iter().zip(values.iter()).enumerate() {
        if idx > 0 {
            out.put_u8(b',');
        }
        if matches!(val, PgValue::Null) {
            continue;
        }
        field_buf.clear();
        encode_value_with_catalog_at_depth(
            val,
            field.type_oid,
            Format::Text,
            &mut field_buf,
            ctx,
            catalog,
            depth + 1,
        )?;
        let field_str = std::str::from_utf8(&field_buf).map_err(|e| {
            PostgresError::Data(format!("encoded composite field is not valid UTF-8: {e}"))
        })?;
        if needs_composite_quoting(field_str) {
            out.put_u8(b'"');
            for &b in field_str.as_bytes() {
                if b == b'"' {
                    out.put_slice(b"\"\"");
                } else if b == b'\\' {
                    out.put_slice(b"\\\\");
                } else {
                    out.put_u8(b);
                }
            }
            out.put_u8(b'"');
        } else {
            out.put_slice(field_str.as_bytes());
        }
    }
    out.put_u8(b')');
    Ok(())
}
