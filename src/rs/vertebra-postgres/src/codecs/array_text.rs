//! PostgreSQL multidimensional text array codecs (`array_in` / `array_out`) and shared bounded container scanner.

use bytes::{BufMut, BytesMut};

use crate::codecs::array::{MAX_ARRAY_DIMS, validate_array_shape};
use crate::codecs::{
    CodecContext, decode_value_with_catalog_at_depth, encode_value_with_catalog_at_depth,
};
use crate::errors::PostgresError;
use crate::result::{Format, PgValue};
use crate::types::TypeCatalog;

/// Container `NULL` and empty-slot lexical policy for [`scan_delimited_container`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ContainerNullRule {
    /// PostgreSQL `array_in` rule: unquoted `NULL` (case-insensitive) is `Null`; empty unquoted slots are syntax errors.
    ArrayUnquotedNullKeyword,
    /// PostgreSQL `record_in` rule: empty unquoted slots are `Null`; unquoted `NULL` is the literal string `"NULL"`.
    CompositeEmptyUnquoted,
}

/// Token emitted by [`scan_delimited_container`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ScannedToken {
    /// SQL `NULL` element/field.
    Null,
    /// Unescaped string value.
    Value(String),
    /// Nested braced sub-array (`{...}`).
    SubArray(Vec<ScannedToken>),
}

/// Scan a bounded PostgreSQL array (`{...}`) or composite (`(...)`) literal into [`ScannedToken`] items.
pub(crate) fn scan_delimited_container(
    input: &str,
    open: u8,
    close: u8,
    delimiter: u8,
    null_rule: ContainerNullRule,
    expected_arity: Option<usize>,
    max_depth: usize,
) -> Result<Vec<ScannedToken>, PostgresError> {
    if max_depth == 0 {
        return Err(PostgresError::Data(
            "container nesting depth exceeds maximum allowed limit".to_string(),
        ));
    }

    let trimmed = match null_rule {
        ContainerNullRule::ArrayUnquotedNullKeyword => {
            input.trim_matches(|c: char| c.is_ascii_whitespace())
        }
        ContainerNullRule::CompositeEmptyUnquoted => input,
    };
    let bytes = trimmed.as_bytes();
    if bytes.len() < 2 || bytes[0] != open || bytes[bytes.len() - 1] != close {
        return Err(PostgresError::Data(format!(
            "container literal does not start with {:?} and end with {:?}",
            open as char, close as char
        )));
    }

    if bytes.len() == 2 {
        return match null_rule {
            ContainerNullRule::ArrayUnquotedNullKeyword => {
                if let Some(arity) = expected_arity
                    && arity != 0
                {
                    return Err(PostgresError::Data(format!(
                        "empty container {{}} does not match expected_arity {arity}"
                    )));
                }
                Ok(Vec::new())
            }
            ContainerNullRule::CompositeEmptyUnquoted => match expected_arity {
                Some(0) => Ok(Vec::new()),
                Some(1) => Ok(vec![ScannedToken::Null]),
                Some(n) => Err(PostgresError::Data(format!(
                    "empty composite literal () does not match expected field count {n}"
                ))),
                None => Err(PostgresError::Data(
                    "ambiguous empty composite literal () without expected_arity".to_string(),
                )),
            },
        };
    }

    let tokens = match null_rule {
        ContainerNullRule::CompositeEmptyUnquoted => {
            scan_composite_interior(&trimmed[1..trimmed.len() - 1], open, close, delimiter)?
        }
        ContainerNullRule::ArrayUnquotedNullKeyword => scan_array_interior(
            &trimmed[1..trimmed.len() - 1],
            open,
            close,
            delimiter,
            max_depth,
        )?,
    };

    if let Some(arity) = expected_arity
        && tokens.len() != arity
    {
        return Err(PostgresError::Data(format!(
            "container token count {} does not match expected_arity {arity}",
            tokens.len()
        )));
    }

    Ok(tokens)
}

fn scan_composite_interior(
    interior: &str,
    open: u8,
    close: u8,
    delimiter: u8,
) -> Result<Vec<ScannedToken>, PostgresError> {
    let bytes = interior.as_bytes();
    let mut tokens = Vec::new();
    let mut pos = 0usize;

    loop {
        if pos == bytes.len() {
            tokens.push(ScannedToken::Null);
            break;
        }
        if bytes[pos] == delimiter {
            tokens.push(ScannedToken::Null);
            pos += 1;
            continue;
        }
        if bytes[pos] == b'"' {
            pos += 1;
            let mut val = String::new();
            let mut closed = false;
            while pos < bytes.len() {
                let b = bytes[pos];
                if b == b'"' {
                    if pos + 1 < bytes.len() && bytes[pos + 1] == b'"' {
                        val.push('"');
                        pos += 2;
                    } else {
                        pos += 1;
                        closed = true;
                        break;
                    }
                } else if b == b'\\' {
                    if pos + 1 >= bytes.len() {
                        return Err(PostgresError::Data(
                            "unterminated backslash escape inside quoted composite field"
                                .to_string(),
                        ));
                    }
                    let ch = interior[pos + 1..].chars().next().ok_or_else(|| {
                        PostgresError::Data("invalid UTF-8 after backslash".to_string())
                    })?;
                    val.push(ch);
                    pos += 1 + ch.len_utf8();
                } else {
                    let ch = interior[pos..].chars().next().ok_or_else(|| {
                        PostgresError::Data(
                            "invalid UTF-8 inside quoted composite field".to_string(),
                        )
                    })?;
                    val.push(ch);
                    pos += ch.len_utf8();
                }
            }
            if !closed {
                return Err(PostgresError::Data(
                    "unterminated double-quoted composite field".to_string(),
                ));
            }
            tokens.push(ScannedToken::Value(val));
            if pos == bytes.len() {
                break;
            }
            if bytes[pos] != delimiter {
                return Err(PostgresError::Data(
                    "unexpected trailing characters after quoted composite field".to_string(),
                ));
            }
            pos += 1;
        } else {
            let start = pos;
            while pos < bytes.len() && bytes[pos] != delimiter {
                if bytes[pos] == b'"' || bytes[pos] == open || bytes[pos] == close {
                    return Err(PostgresError::Data(format!(
                        "unexpected unquoted character {:?} in composite field",
                        bytes[pos] as char
                    )));
                }
                pos += 1;
            }
            tokens.push(ScannedToken::Value(interior[start..pos].to_string()));
            if pos == bytes.len() {
                break;
            }
            pos += 1;
        }
    }

    Ok(tokens)
}

fn scan_array_interior(
    interior: &str,
    open: u8,
    close: u8,
    delimiter: u8,
    max_depth: usize,
) -> Result<Vec<ScannedToken>, PostgresError> {
    let bytes = interior.as_bytes();
    let mut tokens = Vec::new();
    let mut pos = 0usize;

    loop {
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= bytes.len() {
            return Err(PostgresError::Data(
                "empty or trailing-delimiter slot in PostgreSQL array literal".to_string(),
            ));
        }
        if bytes[pos] == delimiter {
            return Err(PostgresError::Data(
                "empty unquoted slot is invalid in PostgreSQL array literal".to_string(),
            ));
        }

        if bytes[pos] == open {
            if max_depth <= 1 {
                return Err(PostgresError::Data(
                    "array nesting depth exceeds maximum allowed limit".to_string(),
                ));
            }
            let sub_start = pos;
            let mut depth = 0usize;
            let mut in_quotes = false;
            while pos < bytes.len() {
                let b = bytes[pos];
                if in_quotes {
                    if b == b'\\' {
                        if pos + 1 >= bytes.len() {
                            return Err(PostgresError::Data(
                                "unterminated escape in nested array".to_string(),
                            ));
                        }
                        let ch = interior[pos + 1..].chars().next().unwrap();
                        pos += 1 + ch.len_utf8();
                        continue;
                    } else if b == b'"' {
                        if pos + 1 < bytes.len() && bytes[pos + 1] == b'"' {
                            pos += 2;
                            continue;
                        }
                        in_quotes = false;
                    }
                } else if b == b'"' {
                    in_quotes = true;
                } else if b == open {
                    depth += 1;
                } else if b == close {
                    depth -= 1;
                    if depth == 0 {
                        pos += 1;
                        break;
                    }
                }
                let ch = interior[pos..].chars().next().unwrap();
                pos += ch.len_utf8();
            }
            if depth != 0 || in_quotes {
                return Err(PostgresError::Data(
                    "unclosed nested sub-array or quote in PostgreSQL array literal".to_string(),
                ));
            }
            let sub_slice = &interior[sub_start..pos];
            let sub_tokens = scan_delimited_container(
                sub_slice,
                open,
                close,
                delimiter,
                ContainerNullRule::ArrayUnquotedNullKeyword,
                None,
                max_depth - 1,
            )?;
            if sub_tokens.is_empty() {
                return Err(PostgresError::Data(
                    "nested sub-array cannot be empty ({}) inside a multidimensional PostgreSQL array".to_string(),
                ));
            }
            tokens.push(ScannedToken::SubArray(sub_tokens));
        } else if bytes[pos] == b'"' {
            pos += 1;
            let mut val = String::new();
            let mut closed = false;
            while pos < bytes.len() {
                let b = bytes[pos];
                if b == b'\\' {
                    if pos + 1 >= bytes.len() {
                        return Err(PostgresError::Data(
                            "unterminated backslash escape in quoted array element".to_string(),
                        ));
                    }
                    let ch = interior[pos + 1..].chars().next().unwrap();
                    val.push(ch);
                    pos += 1 + ch.len_utf8();
                } else if b == b'"' {
                    if pos + 1 < bytes.len() && bytes[pos + 1] == b'"' {
                        val.push('"');
                        pos += 2;
                    } else {
                        pos += 1;
                        closed = true;
                        break;
                    }
                } else {
                    let ch = interior[pos..].chars().next().unwrap();
                    val.push(ch);
                    pos += ch.len_utf8();
                }
            }
            if !closed {
                return Err(PostgresError::Data(
                    "unterminated double-quoted array element".to_string(),
                ));
            }
            tokens.push(ScannedToken::Value(val));
        } else {
            let start = pos;
            let mut val = String::new();
            let mut had_escape = false;
            while pos < bytes.len() && bytes[pos] != delimiter {
                let b = bytes[pos];
                if b == b'"' || b == open || b == close {
                    return Err(PostgresError::Data(format!(
                        "unexpected character {:?} in unquoted array element",
                        b as char
                    )));
                }
                if b == b'\\' {
                    had_escape = true;
                    if pos + 1 >= bytes.len() {
                        return Err(PostgresError::Data(
                            "unterminated backslash escape in unquoted array element".to_string(),
                        ));
                    }
                    let ch = interior[pos + 1..].chars().next().unwrap();
                    val.push(ch);
                    pos += 1 + ch.len_utf8();
                } else {
                    let ch = interior[pos..].chars().next().unwrap();
                    val.push(ch);
                    pos += ch.len_utf8();
                }
            }
            let raw_trimmed = interior[start..pos].trim_matches(|c: char| c.is_ascii_whitespace());
            if raw_trimmed.is_empty() {
                return Err(PostgresError::Data(
                    "empty unquoted array element".to_string(),
                ));
            }
            let trimmed_val = if !had_escape {
                raw_trimmed.to_string()
            } else {
                val.trim_end_matches(|c: char| c.is_ascii_whitespace())
                    .to_string()
            };
            if !had_escape && trimmed_val.eq_ignore_ascii_case("NULL") {
                tokens.push(ScannedToken::Null);
            } else {
                tokens.push(ScannedToken::Value(trimmed_val));
            }
        }

        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos == bytes.len() {
            break;
        }
        if bytes[pos] != delimiter {
            return Err(PostgresError::Data(format!(
                "expected array delimiter {:?}, found {:?}",
                delimiter as char, bytes[pos] as char
            )));
        }
        pos += 1;
        if pos == bytes.len() {
            return Err(PostgresError::Data(
                "trailing delimiter in PostgreSQL array literal".to_string(),
            ));
        }
    }

    Ok(tokens)
}

type ParsedDimensionPrefix<'a> = (Option<(Vec<i32>, Vec<i32>)>, &'a str);

fn parse_dimension_prefix(text: &str) -> Result<ParsedDimensionPrefix<'_>, PostgresError> {
    let trimmed = text.trim_matches(|c: char| c.is_ascii_whitespace());
    if !trimmed.starts_with('[') {
        return Ok((None, trimmed));
    }
    let Some(eq_idx) = trimmed.find('=') else {
        return Err(PostgresError::Data(
            "malformed array dimension prefix (missing '=')".to_string(),
        ));
    };
    let prefix = &trimmed[..eq_idx];
    let body = &trimmed[eq_idx + 1..];
    if !body.starts_with('{') {
        return Err(PostgresError::Data(
            "array body after '[lower:upper]=' must start with '{'".to_string(),
        ));
    }

    let mut dims = Vec::new();
    let mut lbounds = Vec::new();
    let mut rem = prefix;
    while !rem.is_empty() {
        if !rem.starts_with('[') {
            return Err(PostgresError::Data(format!(
                "invalid dimension prefix syntax in {prefix:?}"
            )));
        }
        let Some(close_idx) = rem.find(']') else {
            return Err(PostgresError::Data(format!(
                "unclosed '[' in array dimension prefix {prefix:?}"
            )));
        };
        let spec = &rem[1..close_idx];
        let Some((lo_str, hi_str)) = spec.split_once(':') else {
            return Err(PostgresError::Data(format!(
                "missing ':' in array dimension bound [{spec}]"
            )));
        };
        let lo: i32 = lo_str.parse().map_err(|_| {
            PostgresError::Data(format!("invalid array lower bound {lo_str:?} in [{spec}]"))
        })?;
        let hi: i32 = hi_str.parse().map_err(|_| {
            PostgresError::Data(format!("invalid array upper bound {hi_str:?} in [{spec}]"))
        })?;
        let span = i64::from(hi) - i64::from(lo) + 1;
        if span <= 0 {
            return Err(PostgresError::Data(format!(
                "inverted or non-positive array dimension bounds [{lo}:{hi}]"
            )));
        }
        let dim = i32::try_from(span).map_err(|_| {
            PostgresError::Data(format!("array dimension span [{lo}:{hi}] overflows i32"))
        })?;
        if lo.checked_add(dim - 1) != Some(hi) {
            return Err(PostgresError::Data(format!(
                "array dimension bounds [{lo}:{hi}] overflow i32"
            )));
        }
        dims.push(dim);
        lbounds.push(lo);
        rem = &rem[close_idx + 1..];
    }

    if dims.is_empty() || dims.len() > MAX_ARRAY_DIMS {
        return Err(PostgresError::Data(format!(
            "array dimension prefix rank {} outside 1..={MAX_ARRAY_DIMS}",
            dims.len()
        )));
    }

    Ok((Some((dims, lbounds)), body))
}

fn flatten_scanned_array<'a>(
    tokens: &'a [ScannedToken],
    axis: usize,
    inferred_dims: &mut Vec<i32>,
    leaves: &mut Vec<&'a ScannedToken>,
) -> Result<(), PostgresError> {
    if axis >= MAX_ARRAY_DIMS {
        return Err(PostgresError::Data(format!(
            "text array rank exceeds MAX_ARRAY_DIMS ({MAX_ARRAY_DIMS})"
        )));
    }
    let len_i32 = i32::try_from(tokens.len())
        .map_err(|_| PostgresError::Data("text array dimension exceeds i32::MAX".to_string()))?;
    if inferred_dims.len() == axis {
        inferred_dims.push(len_i32);
    } else if inferred_dims[axis] != len_i32 {
        return Err(PostgresError::Data(format!(
            "ragged text array at dimension {}: expected length {}, got {len_i32}",
            axis, inferred_dims[axis]
        )));
    }

    let first_is_sub = matches!(tokens.first(), Some(ScannedToken::SubArray(_)));
    for tok in tokens {
        match (first_is_sub, tok) {
            (true, ScannedToken::SubArray(sub)) => {
                flatten_scanned_array(sub, axis + 1, inferred_dims, leaves)?;
            }
            (true, _) | (false, ScannedToken::SubArray(_)) => {
                return Err(PostgresError::Data(
                    "ragged text array mixes sub-arrays and leaf elements at the same depth"
                        .to_string(),
                ));
            }
            (false, leaf) => {
                if inferred_dims.len() > axis + 1 {
                    return Err(PostgresError::Data(
                        "ragged text array has inconsistent nesting depth".to_string(),
                    ));
                }
                leaves.push(leaf);
            }
        }
    }
    Ok(())
}

/// Decode a PostgreSQL text array (`array_out`) literal into [`PgValue::Array`].
pub fn decode_array_text(
    _array_oid: u32,
    element_oid: u32,
    delimiter: u8,
    text: &str,
    ctx: &CodecContext,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<PgValue, PostgresError> {
    if depth > ctx.max_nesting_depth {
        return Err(PostgresError::Data(format!(
            "array nesting depth {depth} exceeds max_nesting_depth {}",
            ctx.max_nesting_depth
        )));
    }

    let (prefix_bounds, body) = parse_dimension_prefix(text)?;
    let max_scan_depth = ctx
        .max_nesting_depth
        .saturating_sub(depth)
        .min(MAX_ARRAY_DIMS);
    let tokens = scan_delimited_container(
        body,
        b'{',
        b'}',
        delimiter,
        ContainerNullRule::ArrayUnquotedNullKeyword,
        None,
        max_scan_depth,
    )?;

    if tokens.is_empty() {
        if prefix_bounds.is_some() {
            return Err(PostgresError::Data(
                "dimension prefix is invalid on an empty PostgreSQL array {}".to_string(),
            ));
        }
        return Ok(PgValue::Array {
            values: Vec::new(),
            dimensions: Vec::new(),
            lower_bounds: Vec::new(),
            element_oid,
        });
    }

    let mut inferred_dims = Vec::new();
    let mut leaves = Vec::new();
    flatten_scanned_array(&tokens, 0, &mut inferred_dims, &mut leaves)?;

    let (dimensions, lower_bounds) = if let Some((pref_dims, pref_lbs)) = prefix_bounds {
        if pref_dims != inferred_dims {
            return Err(PostgresError::Data(format!(
                "text array dimension prefix {pref_dims:?} does not match braced extents {inferred_dims:?}"
            )));
        }
        (pref_dims, pref_lbs)
    } else {
        let rank = inferred_dims.len();
        (inferred_dims, vec![1i32; rank])
    };

    let mut values = Vec::with_capacity(leaves.len());
    for (idx, leaf) in leaves.into_iter().enumerate() {
        match leaf {
            ScannedToken::Null => values.push(PgValue::Null),
            ScannedToken::Value(s) => {
                let decoded = decode_value_with_catalog_at_depth(
                    element_oid,
                    Format::Text,
                    Some(s.as_bytes()),
                    ctx,
                    catalog,
                    depth + 1,
                )
                .map_err(|err| {
                    PostgresError::Data(format!("failed to decode text array element {idx}: {err}"))
                })?;
                values.push(decoded);
            }
            ScannedToken::SubArray(_) => unreachable!("leaves cannot be SubArray"),
        }
    }

    Ok(PgValue::Array {
        values,
        dimensions,
        lower_bounds,
        element_oid,
    })
}

fn needs_array_quoting(s: &str, delimiter: u8) -> bool {
    if s.is_empty() || s.eq_ignore_ascii_case("null") {
        return true;
    }
    let bytes = s.as_bytes();
    if bytes[0].is_ascii_whitespace() || bytes[bytes.len() - 1].is_ascii_whitespace() {
        return true;
    }
    bytes.iter().any(|&b| {
        b == b'{'
            || b == b'}'
            || b == b'"'
            || b == b'\\'
            || b == delimiter
            || b.is_ascii_whitespace()
    })
}

#[allow(clippy::too_many_arguments)]
fn write_array_dimension(
    values: &[PgValue],
    dims: &[i32],
    element_oid: u32,
    delimiter: u8,
    idx: &mut usize,
    out: &mut BytesMut,
    ctx: &CodecContext,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<(), PostgresError> {
    out.put_u8(b'{');
    let count = dims[0] as usize;
    if dims.len() == 1 {
        let mut elem_buf = BytesMut::new();
        for i in 0..count {
            if i > 0 {
                out.put_u8(delimiter);
            }
            let val = &values[*idx];
            *idx += 1;
            if matches!(val, PgValue::Null) {
                out.put_slice(b"NULL");
            } else {
                elem_buf.clear();
                encode_value_with_catalog_at_depth(
                    val,
                    element_oid,
                    Format::Text,
                    &mut elem_buf,
                    ctx,
                    catalog,
                    depth + 1,
                )?;
                let elem_str = std::str::from_utf8(&elem_buf).map_err(|e| {
                    PostgresError::Data(format!("encoded array element is not valid UTF-8: {e}"))
                })?;
                if needs_array_quoting(elem_str, delimiter) {
                    out.put_u8(b'"');
                    for &b in elem_str.as_bytes() {
                        if b == b'"' || b == b'\\' {
                            out.put_u8(b'\\');
                        }
                        out.put_u8(b);
                    }
                    out.put_u8(b'"');
                } else {
                    out.put_slice(elem_str.as_bytes());
                }
            }
        }
    } else {
        for i in 0..count {
            if i > 0 {
                out.put_u8(delimiter);
            }
            write_array_dimension(
                values,
                &dims[1..],
                element_oid,
                delimiter,
                idx,
                out,
                ctx,
                catalog,
                depth,
            )?;
        }
    }
    out.put_u8(b'}');
    Ok(())
}

/// Encode a [`PgValue::Array`] into PostgreSQL `array_in` text representation.
pub fn encode_array_text(
    value: &PgValue,
    expected_element_oid: u32,
    delimiter: u8,
    out: &mut BytesMut,
    ctx: &CodecContext,
    catalog: &TypeCatalog,
    depth: usize,
) -> Result<(), PostgresError> {
    if depth > ctx.max_nesting_depth {
        return Err(PostgresError::Data(format!(
            "array nesting depth {depth} exceeds max_nesting_depth {}",
            ctx.max_nesting_depth
        )));
    }
    let PgValue::Array {
        values,
        dimensions,
        lower_bounds,
        element_oid,
    } = value
    else {
        return Err(PostgresError::Data(format!(
            "expected PgValue::Array for text array encoding, got {value:?}"
        )));
    };

    let resolved_elem_oid = validate_array_shape(
        values,
        dimensions,
        lower_bounds,
        *element_oid,
        expected_element_oid,
        catalog,
    )?;

    if dimensions.is_empty() {
        out.put_slice(b"{}");
        return Ok(());
    }

    if lower_bounds.iter().any(|&lb| lb != 1) {
        for (&dim, &lb) in dimensions.iter().zip(lower_bounds.iter()) {
            let ub = lb.checked_add(dim - 1).ok_or_else(|| {
                PostgresError::Data("array upper bound overflowed i32".to_string())
            })?;
            out.put_slice(format!("[{lb}:{ub}]").as_bytes());
        }
        out.put_u8(b'=');
    }

    let mut idx = 0usize;
    write_array_dimension(
        values,
        dimensions,
        resolved_elem_oid,
        delimiter,
        &mut idx,
        out,
        ctx,
        catalog,
        depth,
    )
}
