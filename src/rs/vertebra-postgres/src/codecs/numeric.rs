//! Arbitrary-precision PostgreSQL `NUMERIC` (`OID 1700`) binary (base-10000) and text codecs.

use bytes::{BufMut, BytesMut};

use crate::errors::PostgresError;
use crate::result::PgNumeric;

/// Wire sign flag for positive finite `NUMERIC`.
pub const NUMERIC_POS: u16 = 0x0000;
/// Wire sign flag for negative finite `NUMERIC`.
pub const NUMERIC_NEG: u16 = 0x4000;
/// Wire sign flag for `NUMERIC` quiet `NaN`.
pub const NUMERIC_NAN: u16 = 0xC000;
/// Wire sign flag for `NUMERIC` `+Infinity` (PG14+).
pub const NUMERIC_PINF: u16 = 0xD000;
/// Wire sign flag for `NUMERIC` `-Infinity` (PG14+).
pub const NUMERIC_NINF: u16 = 0xF000;
/// Maximum PostgreSQL `NUMERIC` display scale (`0x3FFF` = `16_383`).
pub const MAX_DSCALE: u16 = 0x3FFF;
/// Maximum number of base-10000 digit groups accepted on the wire.
pub const MAX_NUMERIC_DIGITS: usize = 32_768;

const POW10: [i16; 4] = [1, 10, 100, 1000];

/// Normalize leading and trailing zero base-10000 digit groups while preserving `dscale`.
pub fn normalize_finite_parts(
    negative: bool,
    mut weight: i16,
    dscale: u16,
    digits: &[i16],
) -> Result<PgNumeric, PostgresError> {
    if dscale > MAX_DSCALE {
        return Err(PostgresError::Data(format!(
            "NUMERIC dscale {dscale} exceeds maximum {MAX_DSCALE}"
        )));
    }
    if digits.len() > MAX_NUMERIC_DIGITS {
        return Err(PostgresError::Data(format!(
            "NUMERIC ndigits {} exceeds maximum {MAX_NUMERIC_DIGITS}",
            digits.len()
        )));
    }
    for &d in digits {
        if !(0..=9999).contains(&d) {
            return Err(PostgresError::Data(format!(
                "NUMERIC base-10000 digit {d} is outside 0..=9999"
            )));
        }
    }

    let mut slice = digits;
    while let Some(&0) = slice.first() {
        slice = &slice[1..];
        weight = weight
            .checked_sub(1)
            .ok_or_else(|| PostgresError::Data("NUMERIC weight underflow".to_string()))?;
    }
    while let Some(&0) = slice.last() {
        slice = &slice[..slice.len() - 1];
    }

    if slice.is_empty() {
        Ok(PgNumeric::Finite {
            negative: false,
            weight: 0,
            dscale,
            digits: Vec::new(),
        })
    } else {
        Ok(PgNumeric::Finite {
            negative,
            weight,
            dscale,
            digits: slice.to_vec(),
        })
    }
}

/// Construct a [`PgNumeric`] from base-10 digits (`0..=9`) and a base-10 exponent.
pub fn numeric_from_decimal_parts(
    negative: bool,
    base10_digits: &[u8],
    exponent: i32,
    signaling_nan: bool,
) -> Result<PgNumeric, PostgresError> {
    if signaling_nan {
        return Err(PostgresError::Data(
            "PostgreSQL NUMERIC does not support signaling NaN (sNaN)".to_string(),
        ));
    }
    for &d in base10_digits {
        if d > 9 {
            return Err(PostgresError::Data(format!(
                "invalid base-10 digit {d} in Decimal tuple"
            )));
        }
    }

    let dscale_i32 = (-exponent).max(0);
    if dscale_i32 > i32::from(MAX_DSCALE) {
        return Err(PostgresError::Data(format!(
            "NUMERIC decimal scale {dscale_i32} exceeds maximum {MAX_DSCALE}"
        )));
    }
    let dscale = dscale_i32 as u16;

    let mut first_nz = 0;
    while first_nz < base10_digits.len() && base10_digits[first_nz] == 0 {
        first_nz += 1;
    }
    if first_nz == base10_digits.len() {
        return Ok(PgNumeric::Finite {
            negative: false,
            weight: 0,
            dscale,
            digits: Vec::new(),
        });
    }

    let mut last_nz = base10_digits.len();
    while last_nz > first_nz && base10_digits[last_nz - 1] == 0 {
        last_nz -= 1;
    }
    let trimmed = &base10_digits[first_nz..last_nz];
    let trailing_zeros = (base10_digits.len() - last_nz) as i32;
    let adj_exp = exponent
        .checked_add(trailing_zeros)
        .ok_or_else(|| PostgresError::Data("NUMERIC exponent overflow".to_string()))?;
    let num_digits = i32::try_from(trimmed.len())
        .map_err(|_| PostgresError::Data("NUMERIC digit length overflow".to_string()))?;
    let int_len = num_digits
        .checked_add(adj_exp)
        .ok_or_else(|| PostgresError::Data("NUMERIC integer length overflow".to_string()))?;

    let p10_max = int_len
        .checked_sub(1)
        .ok_or_else(|| PostgresError::Data("NUMERIC power underflow".to_string()))?;
    let p10_min = adj_exp;
    let g_max = p10_max.div_euclid(4);
    let g_min = p10_min.div_euclid(4);

    let weight = i16::try_from(g_max).map_err(|_| {
        PostgresError::Data(format!(
            "NUMERIC base-10000 weight {g_max} overflows signed 16-bit integer"
        ))
    })?;
    let group_span = g_max
        .checked_sub(g_min)
        .and_then(|v| v.checked_add(1))
        .ok_or_else(|| PostgresError::Data("NUMERIC group span overflow".to_string()))?;
    let group_count = usize::try_from(group_span)
        .map_err(|_| PostgresError::Data("NUMERIC invalid group count".to_string()))?;
    if group_count > MAX_NUMERIC_DIGITS {
        return Err(PostgresError::Data(format!(
            "NUMERIC requires {group_count} base-10000 groups, exceeding maximum {MAX_NUMERIC_DIGITS}"
        )));
    }

    let mut groups = vec![0i16; group_count];
    for (idx, &digit) in trimmed.iter().enumerate() {
        if digit == 0 {
            continue;
        }
        let j = idx as i32;
        let p10 = int_len - 1 - j;
        let g = p10.div_euclid(4);
        let r = p10.rem_euclid(4) as usize;
        let group_idx = (g_max - g) as usize;
        groups[group_idx] += i16::from(digit) * POW10[r];
    }

    normalize_finite_parts(negative, weight, dscale, &groups)
}

/// Decode a PostgreSQL binary `NUMERIC` (`numeric_send`) frame.
pub fn decode_numeric_binary(raw: &[u8]) -> Result<PgNumeric, PostgresError> {
    if raw.len() < 8 {
        return Err(PostgresError::Data(format!(
            "NUMERIC binary payload too short ({} bytes, expected >= 8)",
            raw.len()
        )));
    }
    let ndigits_i16 = i16::from_be_bytes([raw[0], raw[1]]);
    let weight = i16::from_be_bytes([raw[2], raw[3]]);
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = u16::from_be_bytes([raw[6], raw[7]]);

    if ndigits_i16 < 0 {
        return Err(PostgresError::Data(format!(
            "NUMERIC binary ndigits {ndigits_i16} must be non-negative"
        )));
    }
    let ndigits = ndigits_i16 as usize;
    if ndigits > MAX_NUMERIC_DIGITS {
        return Err(PostgresError::Data(format!(
            "NUMERIC binary ndigits {ndigits} exceeds maximum {MAX_NUMERIC_DIGITS}"
        )));
    }
    let expected_len = 8 + ndigits * 2;
    if raw.len() != expected_len {
        return Err(PostgresError::Data(format!(
            "NUMERIC binary length {} does not match header ndigits {ndigits} ({expected_len} bytes)",
            raw.len()
        )));
    }

    match sign {
        NUMERIC_NAN => {
            if ndigits != 0 || weight != 0 || dscale != 0 {
                return Err(PostgresError::Data(
                    "NUMERIC NaN frame must have zero ndigits, weight, and dscale".to_string(),
                ));
            }
            Ok(PgNumeric::NaN)
        }
        NUMERIC_PINF => {
            if ndigits != 0 || weight != 0 || dscale != 0 {
                return Err(PostgresError::Data(
                    "NUMERIC +Infinity frame must have zero ndigits, weight, and dscale"
                        .to_string(),
                ));
            }
            Ok(PgNumeric::PosInf)
        }
        NUMERIC_NINF => {
            if ndigits != 0 || weight != 0 || dscale != 0 {
                return Err(PostgresError::Data(
                    "NUMERIC -Infinity frame must have zero ndigits, weight, and dscale"
                        .to_string(),
                ));
            }
            Ok(PgNumeric::NegInf)
        }
        NUMERIC_POS | NUMERIC_NEG => {
            if dscale > MAX_DSCALE {
                return Err(PostgresError::Data(format!(
                    "NUMERIC binary dscale {dscale} exceeds maximum {MAX_DSCALE}"
                )));
            }
            let mut digits = Vec::with_capacity(ndigits);
            for &chunk in raw[8..].as_chunks::<2>().0 {
                let d = i16::from_be_bytes(chunk);
                if !(0..=9999).contains(&d) {
                    return Err(PostgresError::Data(format!(
                        "NUMERIC binary base-10000 digit {d} is outside 0..=9999"
                    )));
                }
                digits.push(d);
            }
            normalize_finite_parts(sign == NUMERIC_NEG, weight, dscale, &digits)
        }
        other => Err(PostgresError::Data(format!(
            "unknown NUMERIC binary sign word 0x{other:04x}"
        ))),
    }
}

/// Encode a [`PgNumeric`] into PostgreSQL binary `numeric_recv` wire format.
pub fn encode_numeric_binary(num: &PgNumeric, out: &mut BytesMut) -> Result<(), PostgresError> {
    match num {
        PgNumeric::NaN => {
            out.reserve(8);
            out.put_i16(0);
            out.put_i16(0);
            out.put_u16(NUMERIC_NAN);
            out.put_u16(0);
            Ok(())
        }
        PgNumeric::PosInf => {
            out.reserve(8);
            out.put_i16(0);
            out.put_i16(0);
            out.put_u16(NUMERIC_PINF);
            out.put_u16(0);
            Ok(())
        }
        PgNumeric::NegInf => {
            out.reserve(8);
            out.put_i16(0);
            out.put_i16(0);
            out.put_u16(NUMERIC_NINF);
            out.put_u16(0);
            Ok(())
        }
        PgNumeric::Finite {
            negative,
            weight,
            dscale,
            digits,
        } => {
            let normalized = normalize_finite_parts(*negative, *weight, *dscale, digits)?;
            if let PgNumeric::Finite {
                negative: norm_neg,
                weight: norm_weight,
                dscale: norm_dscale,
                digits: norm_digits,
            } = normalized
            {
                let ndigits = i16::try_from(norm_digits.len()).map_err(|_| {
                    PostgresError::Data("NUMERIC ndigits overflows i16".to_string())
                })?;
                let sign = if norm_neg && !norm_digits.is_empty() {
                    NUMERIC_NEG
                } else {
                    NUMERIC_POS
                };
                out.reserve(8 + norm_digits.len() * 2);
                out.put_i16(ndigits);
                out.put_i16(if norm_digits.is_empty() {
                    0
                } else {
                    norm_weight
                });
                out.put_u16(sign);
                out.put_u16(norm_dscale);
                for d in norm_digits {
                    out.put_i16(d);
                }
            }
            Ok(())
        }
    }
}

/// Decode a PostgreSQL text `NUMERIC` string without floating-point or fixed-width intermediaries.
pub fn decode_numeric_text(text: &str) -> Result<PgNumeric, PostgresError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(PostgresError::Data(
            "empty string is not a valid PostgreSQL NUMERIC".to_string(),
        ));
    }

    if trimmed.eq_ignore_ascii_case("snan")
        || trimmed.eq_ignore_ascii_case("-snan")
        || trimmed.eq_ignore_ascii_case("+snan")
    {
        return Err(PostgresError::Data(
            "PostgreSQL NUMERIC does not support signaling NaN (sNaN)".to_string(),
        ));
    }
    if trimmed.eq_ignore_ascii_case("nan") {
        return Ok(PgNumeric::NaN);
    }
    if trimmed.eq_ignore_ascii_case("infinity")
        || trimmed.eq_ignore_ascii_case("+infinity")
        || trimmed.eq_ignore_ascii_case("inf")
        || trimmed.eq_ignore_ascii_case("+inf")
    {
        return Ok(PgNumeric::PosInf);
    }
    if trimmed.eq_ignore_ascii_case("-infinity") || trimmed.eq_ignore_ascii_case("-inf") {
        return Ok(PgNumeric::NegInf);
    }

    let (negative, rest) = if let Some(after) = trimmed.strip_prefix('-') {
        (true, after)
    } else if let Some(after) = trimmed.strip_prefix('+') {
        (false, after)
    } else {
        (false, trimmed)
    };

    if rest.is_empty() {
        return Err(PostgresError::Data(format!(
            "invalid PostgreSQL NUMERIC literal {text:?}"
        )));
    }

    let (mantissa, exp_val) = if let Some(exp_idx) = rest.find(['e', 'E']) {
        let m = &rest[..exp_idx];
        let e_str = &rest[exp_idx + 1..];
        if e_str.is_empty() {
            return Err(PostgresError::Data(format!(
                "missing exponent in NUMERIC literal {text:?}"
            )));
        }
        let exp: i32 = e_str.parse().map_err(|_| {
            PostgresError::Data(format!("invalid NUMERIC exponent in literal {text:?}"))
        })?;
        (m, exp)
    } else {
        (rest, 0i32)
    };

    let (int_part, frac_part) = if let Some(dot_idx) = mantissa.find('.') {
        let after_dot = &mantissa[dot_idx + 1..];
        if after_dot.contains('.') {
            return Err(PostgresError::Data(format!(
                "multiple decimal points in NUMERIC literal {text:?}"
            )));
        }
        (&mantissa[..dot_idx], after_dot)
    } else {
        (mantissa, "")
    };

    if int_part.is_empty() && frac_part.is_empty() {
        return Err(PostgresError::Data(format!(
            "NUMERIC literal {text:?} has no digits"
        )));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(PostgresError::Data(format!(
            "invalid non-digit character in NUMERIC literal {text:?}"
        )));
    }

    let frac_len = i32::try_from(frac_part.len())
        .map_err(|_| PostgresError::Data("NUMERIC fractional length overflow".to_string()))?;
    let total_exponent = exp_val
        .checked_sub(frac_len)
        .ok_or_else(|| PostgresError::Data("NUMERIC exponent underflow".to_string()))?;

    let mut base10_digits = Vec::with_capacity(int_part.len() + frac_part.len());
    for b in int_part.bytes().chain(frac_part.bytes()) {
        base10_digits.push(b - b'0');
    }

    numeric_from_decimal_parts(negative, &base10_digits, total_exponent, false)
}

/// Render a [`PgNumeric`] into its exact canonical decimal text representation.
pub fn encode_numeric_text(num: &PgNumeric) -> String {
    match num {
        PgNumeric::NaN => "NaN".to_string(),
        PgNumeric::PosInf => "Infinity".to_string(),
        PgNumeric::NegInf => "-Infinity".to_string(),
        PgNumeric::Finite {
            negative,
            weight,
            dscale,
            digits,
        } => {
            let normalized = normalize_finite_parts(*negative, *weight, *dscale, digits).unwrap_or(
                PgNumeric::Finite {
                    negative: false,
                    weight: 0,
                    dscale: *dscale,
                    digits: Vec::new(),
                },
            );
            let PgNumeric::Finite {
                negative: norm_neg,
                weight: norm_weight,
                dscale: norm_dscale,
                digits: norm_digits,
            } = normalized
            else {
                return "NaN".to_string();
            };

            if norm_digits.is_empty() {
                if norm_dscale == 0 {
                    return "0".to_string();
                }
                let mut out = String::with_capacity(2 + usize::from(norm_dscale));
                out.push_str("0.");
                for _ in 0..norm_dscale {
                    out.push('0');
                }
                return out;
            }

            let mut out = String::new();
            if norm_neg {
                out.push('-');
            }

            if norm_weight < 0 {
                out.push('0');
            } else {
                for g in (0..=norm_weight).rev() {
                    let idx = (norm_weight - g) as usize;
                    let d = norm_digits.get(idx).copied().unwrap_or(0);
                    if g == norm_weight {
                        out.push_str(&d.to_string());
                    } else {
                        out.push_str(&format!("{d:04}"));
                    }
                }
            }

            let min_digit_group = (i32::from(norm_weight)) - (norm_digits.len() as i32) + 1;
            let dscale_groups = -((i32::from(norm_dscale) + 3) / 4);
            let lowest_group = min_digit_group.min(dscale_groups);

            if lowest_group < 0 {
                let mut frac = String::with_capacity(((-lowest_group) as usize) * 4);
                for g in (lowest_group..=-1).rev() {
                    let idx = i32::from(norm_weight) - g;
                    let d = if idx >= 0 {
                        norm_digits.get(idx as usize).copied().unwrap_or(0)
                    } else {
                        0
                    };
                    frac.push_str(&format!("{d:04}"));
                }
                let min_frac_len = usize::from(norm_dscale);
                while frac.len() > min_frac_len && frac.ends_with('0') {
                    frac.pop();
                }
                if !frac.is_empty() {
                    out.push('.');
                    out.push_str(&frac);
                }
            }

            out
        }
    }
}
