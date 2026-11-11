//! PostgreSQL binary array codecs (`array_send` / `array_recv`, rank `0..=6`) and convenience sequence normalization.

use bytes::{BufMut, BytesMut};

use crate::codecs::{
    CodecContext, decode_value_with_catalog_at_depth, encode_value_with_catalog_at_depth,
    supports_binary_with_catalog,
};
use crate::errors::PostgresError;
use crate::result::{Format, PgValue};
use crate::types::{TypeCatalog, TypeInfo, TypeKind};

/// Maximum PostgreSQL array dimensionality (`MAXDIM = 6`).
pub const MAX_ARRAY_DIMS: usize = 6;

fn format_multi_index(flat_idx: usize, dims: &[i32]) -> String {
    if dims.is_empty() {
        return format!("[{flat_idx}]");
    }
    let mut rem = flat_idx;
    let mut coords = vec![0usize; dims.len()];
    for axis in (0..dims.len()).rev() {
        let d = (dims[axis] as usize).max(1);
        coords[axis] = rem % d;
        rem /= d;
    }
    coords
        .into_iter()
        .map(|c| format!("[{c}]"))
        .collect::<Vec<_>>()
        .join("")
}

/// Validate a [`PgValue::Array`] shape, rank (`0..=6`), checked `i32` bounds, and element count.
pub fn validate_array_shape(
    values: &[PgValue],
    dimensions: &[i32],
    lower_bounds: &[i32],
    element_oid: u32,
    expected_element_oid: u32,
    catalog: &TypeCatalog,
) -> Result<u32, PostgresError> {
    if dimensions.len() != lower_bounds.len() {
        return Err(PostgresError::Data(format!(
            "array dimensions length {} does not match lower_bounds length {}",
            dimensions.len(),
            lower_bounds.len()
        )));
    }
    if dimensions.len() > MAX_ARRAY_DIMS {
        return Err(PostgresError::Data(format!(
            "array rank {} exceeds MAX_ARRAY_DIMS ({MAX_ARRAY_DIMS})",
            dimensions.len()
        )));
    }
    let resolved_elem_oid = if expected_element_oid != 0 {
        expected_element_oid
    } else {
        element_oid
    };
    if resolved_elem_oid == 0 || element_oid == 0 {
        return Err(PostgresError::Data(
            "array element_oid must be positive (>= 1)".to_string(),
        ));
    }
    let expected_base = catalog.resolve_domain_base(resolved_elem_oid)?;
    let actual_base = catalog.resolve_domain_base(element_oid)?;
    if element_oid != resolved_elem_oid && actual_base != expected_base {
        return Err(PostgresError::Data(format!(
            "array element_oid {element_oid} does not match target element_oid {resolved_elem_oid}"
        )));
    }

    if dimensions.is_empty() {
        if !values.is_empty() {
            return Err(PostgresError::Data(
                "0D PostgreSQL array must have empty values".to_string(),
            ));
        }
        return Ok(resolved_elem_oid);
    }

    if values.is_empty() {
        return Err(PostgresError::Data(
            "empty PostgreSQL array must have rank 0 (dimensions = [], lower_bounds = [])"
                .to_string(),
        ));
    }

    let mut product = 1usize;
    for (&dim, &lbound) in dimensions.iter().zip(lower_bounds.iter()) {
        if dim <= 0 {
            return Err(PostgresError::Data(format!(
                "non-empty PostgreSQL array dimension must be >= 1, got {dim}"
            )));
        }
        if lbound.checked_add(dim - 1).is_none() {
            return Err(PostgresError::Data(format!(
                "array lower_bound {lbound} + dim {dim} - 1 overflows i32"
            )));
        }
        product = product.checked_mul(dim as usize).ok_or_else(|| {
            PostgresError::Data("array dimension product overflowed usize".into())
        })?;
    }

    if product != values.len() {
        return Err(PostgresError::Data(format!(
            "array flat values length {} does not match dimension product {product}",
            values.len()
        )));
    }

    Ok(resolved_elem_oid)
}

/// Decode a PostgreSQL binary array (`array_send`) payload into [`PgValue::Array`].
pub fn decode_array_binary(
    array_oid: u32,
    expected_element_oid: u32,
    raw: &[u8],
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
    if raw.len() < 12 {
        return Err(PostgresError::Data(format!(
            "binary array payload too short ({} bytes < 12-byte header)",
            raw.len()
        )));
    }

    let ndim = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
    let flags = i32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]);
    let wire_element_oid = u32::from_be_bytes([raw[8], raw[9], raw[10], raw[11]]);

    if ndim < 0 || (ndim as usize) > MAX_ARRAY_DIMS {
        return Err(PostgresError::Data(format!(
            "binary array ndim {ndim} is outside valid range 0..={MAX_ARRAY_DIMS}"
        )));
    }
    if flags != 0 && flags != 1 {
        return Err(PostgresError::Data(format!(
            "binary array flags must be 0 or 1, got {flags}"
        )));
    }
    if wire_element_oid == 0 {
        return Err(PostgresError::Data(
            "binary array header has invalid wire_element_oid 0".to_string(),
        ));
    }

    let target_elem_oid = if expected_element_oid != 0 {
        expected_element_oid
    } else if array_oid != 0 {
        let base_array_oid = catalog.resolve_domain_base(array_oid)?;
        match catalog.type_info(base_array_oid) {
            Some(TypeInfo {
                kind: TypeKind::Array { element_oid, .. },
                ..
            }) => *element_oid,
            _ => wire_element_oid,
        }
    } else {
        wire_element_oid
    };

    let expected_base = catalog.resolve_domain_base(target_elem_oid)?;
    if wire_element_oid != target_elem_oid && wire_element_oid != expected_base {
        return Err(PostgresError::Data(format!(
            "binary array wire element OID {wire_element_oid} does not match expected {target_elem_oid} (base {expected_base})"
        )));
    }

    let rank = ndim as usize;
    if rank == 0 {
        if raw.len() != 12 {
            return Err(PostgresError::Data(format!(
                "0D binary array has {} trailing bytes after 12-byte header",
                raw.len() - 12
            )));
        }
        return Ok(PgValue::Array {
            values: Vec::new(),
            dimensions: Vec::new(),
            lower_bounds: Vec::new(),
            element_oid: target_elem_oid,
        });
    }

    let header_end = 12 + rank * 8;
    if raw.len() < header_end {
        return Err(PostgresError::Data(format!(
            "binary array truncated inside {rank}D dimension header ({} < {header_end})",
            raw.len()
        )));
    }

    let mut dimensions = Vec::with_capacity(rank);
    let mut lower_bounds = Vec::with_capacity(rank);
    let mut product = 1usize;

    for i in 0..rank {
        let off = 12 + i * 8;
        let dim = i32::from_be_bytes([raw[off], raw[off + 1], raw[off + 2], raw[off + 3]]);
        let lbound = i32::from_be_bytes([raw[off + 4], raw[off + 5], raw[off + 6], raw[off + 7]]);
        if dim <= 0 {
            return Err(PostgresError::Data(format!(
                "binary array axis {i} has non-positive dimension {dim}"
            )));
        }
        if lbound.checked_add(dim - 1).is_none() {
            return Err(PostgresError::Data(format!(
                "binary array axis {i} upper bound overflows i32 (lbound={lbound}, dim={dim})"
            )));
        }
        product = product.checked_mul(dim as usize).ok_or_else(|| {
            PostgresError::Data("binary array dimension product overflowed usize".to_string())
        })?;
        dimensions.push(dim);
        lower_bounds.push(lbound);
    }

    let remaining = raw.len() - header_end;
    let min_required_bytes = product.checked_mul(4).ok_or_else(|| {
        PostgresError::Data("binary array minimum element byte size overflowed usize".to_string())
    })?;
    if remaining < min_required_bytes {
        return Err(PostgresError::Data(format!(
            "binary array header claims {product} elements requiring at least {min_required_bytes} bytes, but only {remaining} bytes remain"
        )));
    }

    let mut values = Vec::with_capacity(product);
    let mut pos = header_end;

    for idx in 0..product {
        if pos + 4 > raw.len() {
            return Err(PostgresError::Data(format!(
                "binary array truncated reading length of element {idx}"
            )));
        }
        let elem_len = i32::from_be_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]);
        pos += 4;
        if elem_len == -1 {
            if flags == 0 {
                return Err(PostgresError::Data(format!(
                    "binary array element {idx} is NULL (-1) but header flags == 0"
                )));
            }
            values.push(PgValue::Null);
        } else if elem_len < -1 {
            return Err(PostgresError::Data(format!(
                "binary array element {idx} has invalid negative length {elem_len}"
            )));
        } else {
            let len = elem_len as usize;
            if pos + len > raw.len() {
                return Err(PostgresError::Data(format!(
                    "binary array element {idx} payload truncated (expected {len} bytes, {} remaining)",
                    raw.len() - pos
                )));
            }
            let slice = &raw[pos..pos + len];
            pos += len;
            let val = decode_value_with_catalog_at_depth(
                target_elem_oid,
                Format::Binary,
                Some(slice),
                ctx,
                catalog,
                depth + 1,
            )
            .map_err(|err| {
                let path = format_multi_index(idx, &dimensions);
                PostgresError::Data(format!(
                    "failed to decode array{path} (element {idx}): {err}"
                ))
            })?;
            values.push(val);
        }
    }

    if pos != raw.len() {
        return Err(PostgresError::Data(format!(
            "binary array has {} trailing bytes after {product} elements",
            raw.len() - pos
        )));
    }

    Ok(PgValue::Array {
        values,
        dimensions,
        lower_bounds,
        element_oid: target_elem_oid,
    })
}

/// Encode a [`PgValue::Array`] into PostgreSQL binary `array_recv` format.
pub fn encode_array_binary(
    value: &PgValue,
    target_array_oid: u32,
    expected_element_oid: u32,
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
            "expected PgValue::Array for binary array encoding, got {value:?}"
        )));
    };

    let catalog_elem_oid = if expected_element_oid != 0 {
        expected_element_oid
    } else if target_array_oid != 0 {
        let base_oid = catalog.resolve_domain_base(target_array_oid)?;
        match catalog.type_info(base_oid) {
            Some(TypeInfo {
                kind: TypeKind::Array { element_oid: e, .. },
                ..
            }) => *e,
            _ => {
                return Err(PostgresError::Data(format!(
                    "target OID {target_array_oid} is not a registered array type"
                )));
            }
        }
    } else {
        *element_oid
    };

    let resolved_elem_oid = validate_array_shape(
        values,
        dimensions,
        lower_bounds,
        *element_oid,
        catalog_elem_oid,
        catalog,
    )?;

    if !supports_binary_with_catalog(resolved_elem_oid, catalog) {
        return Err(PostgresError::Data(format!(
            "array element OID {resolved_elem_oid} does not support binary encoding"
        )));
    }

    let wire_elem_oid = catalog.resolve_domain_base(resolved_elem_oid)?;
    let has_null = i32::from(values.iter().any(|v| matches!(v, PgValue::Null)));

    out.put_i32(dimensions.len() as i32);
    out.put_i32(has_null);
    out.put_u32(wire_elem_oid);

    for (&dim, &lbound) in dimensions.iter().zip(lower_bounds.iter()) {
        out.put_i32(dim);
        out.put_i32(lbound);
    }

    for (idx, elem) in values.iter().enumerate() {
        if matches!(elem, PgValue::Null) {
            out.put_i32(-1);
        } else {
            let len_pos = out.len();
            out.put_i32(0);
            let payload_start = out.len();
            encode_value_with_catalog_at_depth(
                elem,
                resolved_elem_oid,
                Format::Binary,
                out,
                ctx,
                catalog,
                depth + 1,
            )
            .map_err(|err| {
                let path = format_multi_index(idx, dimensions);
                PostgresError::Data(format!(
                    "failed to encode array{path} (element {idx}): {err}"
                ))
            })?;
            let payload_len = out.len() - payload_start;
            let len_i32 = i32::try_from(payload_len).map_err(|_| {
                PostgresError::Data(format!(
                    "encoded array element {idx} length {payload_len} exceeds i32::MAX"
                ))
            })?;
            out[len_pos..len_pos + 4].copy_from_slice(&len_i32.to_be_bytes());
        }
    }

    Ok(())
}

/// Normalize a nested convenience sequence of [`PgValue`] items into a canonical [`PgValue::Array`] with `1`-based lower bounds.
pub fn normalize_convenience_array(
    elements: &[PgValue],
    target_array_oid: u32,
    catalog: &TypeCatalog,
) -> Result<PgValue, PostgresError> {
    if target_array_oid == 0 {
        return Err(PostgresError::Data(
            "convenience sequence bind requires an explicit target array OID".to_string(),
        ));
    }
    let base_array_oid = catalog.resolve_domain_base(target_array_oid)?;
    let Some(TypeInfo {
        kind: TypeKind::Array { element_oid, .. },
        ..
    }) = catalog.type_info(base_array_oid)
    else {
        return Err(PostgresError::Data(format!(
            "target OID {target_array_oid} is not a known PostgreSQL array type"
        )));
    };
    let expected_element_oid = *element_oid;
    if expected_element_oid == 0 {
        return Err(PostgresError::Data(
            "target array type has invalid element_oid 0".to_string(),
        ));
    }

    if elements.is_empty() {
        return Ok(PgValue::Array {
            values: Vec::new(),
            dimensions: Vec::new(),
            lower_bounds: Vec::new(),
            element_oid: expected_element_oid,
        });
    }

    let elem_base_oid = catalog.resolve_domain_base(expected_element_oid)?;
    let elem_is_composite = matches!(
        catalog.type_info(elem_base_oid),
        Some(TypeInfo {
            kind: TypeKind::Composite { .. },
            ..
        })
    );

    let mut dimensions = Vec::new();
    let mut flat_values = Vec::new();
    flatten_convenience_level(
        elements,
        expected_element_oid,
        elem_is_composite,
        1,
        &mut dimensions,
        &mut flat_values,
    )?;

    let rank = dimensions.len();
    let lower_bounds = vec![1i32; rank];
    validate_array_shape(
        &flat_values,
        &dimensions,
        &lower_bounds,
        expected_element_oid,
        expected_element_oid,
        catalog,
    )?;

    Ok(PgValue::Array {
        values: flat_values,
        dimensions,
        lower_bounds,
        element_oid: expected_element_oid,
    })
}

fn flatten_convenience_level(
    level: &[PgValue],
    expected_element_oid: u32,
    elem_is_composite: bool,
    depth: usize,
    dimensions: &mut Vec<i32>,
    flat_values: &mut Vec<PgValue>,
) -> Result<(), PostgresError> {
    if depth > MAX_ARRAY_DIMS {
        return Err(PostgresError::Data(format!(
            "convenience sequence nesting depth {depth} exceeds MAX_ARRAY_DIMS ({MAX_ARRAY_DIMS})"
        )));
    }
    if level.is_empty() {
        return Err(PostgresError::Data(
            "nested convenience sub-array cannot be empty inside a multidimensional array"
                .to_string(),
        ));
    }
    let dim_i32 = i32::try_from(level.len()).map_err(|_| {
        PostgresError::Data("convenience sequence dimension exceeds i32::MAX".to_string())
    })?;
    if dimensions.len() < depth {
        dimensions.push(dim_i32);
    } else if dimensions[depth - 1] != dim_i32 {
        return Err(PostgresError::Data(format!(
            "ragged convenience sequence at axis {}: expected length {}, got {dim_i32}",
            depth - 1,
            dimensions[depth - 1]
        )));
    }

    let first_is_subarray = matches!(level.first(), Some(PgValue::Array { .. }));
    for item in level {
        match (first_is_subarray, item) {
            (
                true,
                PgValue::Array {
                    values,
                    dimensions: sub_dims,
                    lower_bounds: sub_lbs,
                    element_oid,
                },
            ) => {
                if *element_oid != 0 && *element_oid != expected_element_oid {
                    return Err(PostgresError::Data(format!(
                        "nested convenience sub-array element_oid {element_oid} does not match {expected_element_oid}"
                    )));
                }
                if sub_lbs.iter().any(|&lb| lb != 1) {
                    return Err(PostgresError::Data(
                        "nested convenience sub-array cannot specify non-1 lower_bounds; pass a top-level PgArray instead".to_string(),
                    ));
                }
                if sub_dims.len() <= 1 {
                    flatten_convenience_level(
                        values,
                        expected_element_oid,
                        elem_is_composite,
                        depth + 1,
                        dimensions,
                        flat_values,
                    )?;
                } else {
                    if depth + sub_dims.len() - 1 > MAX_ARRAY_DIMS {
                        return Err(PostgresError::Data(format!(
                            "convenience array rank exceeds MAX_ARRAY_DIMS ({MAX_ARRAY_DIMS})"
                        )));
                    }
                    for (sub_axis, &sd) in sub_dims.iter().enumerate() {
                        let target_axis = depth + sub_axis;
                        if dimensions.len() < target_axis {
                            dimensions.push(sd);
                        } else if dimensions[target_axis - 1] != sd {
                            return Err(PostgresError::Data(format!(
                                "ragged convenience sub-array at axis {}: expected {}, got {sd}",
                                target_axis - 1,
                                dimensions[target_axis - 1]
                            )));
                        }
                    }
                    flat_values.extend(values.iter().cloned());
                }
            }
            (true, _) => {
                return Err(PostgresError::Data(
                    "ragged convenience sequence mixes sub-arrays and scalar elements at the same depth".to_string(),
                ));
            }
            (false, PgValue::Array { .. }) => {
                return Err(PostgresError::Data(
                    "ragged convenience sequence mixes scalar elements and sub-arrays at the same depth".to_string(),
                ));
            }
            (false, PgValue::Composite { .. }) if !elem_is_composite => {
                return Err(PostgresError::Data(
                    "cannot bind PgComposite inside convenience array whose target element type is not composite".to_string(),
                ));
            }
            (false, leaf) => {
                if dimensions.len() > depth {
                    return Err(PostgresError::Data(
                        "ragged convenience sequence has shallower branch than sibling".to_string(),
                    ));
                }
                flat_values.push(leaf.clone());
            }
        }
    }

    Ok(())
}
