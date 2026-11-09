//! Lossless PostgreSQL `INTERVAL` (`OID 1186`) text codecs across all four `IntervalStyle` formats
//! (`postgres`, `postgres_verbose`, `sql_standard`, `iso_8601`).

use crate::codecs::CodecContext;
use crate::codecs::temporal::validate_interval_parts;
use crate::errors::PostgresError;
use crate::result::{PG_INTERVAL_NEG_INF, PG_INTERVAL_POS_INF};

const MICROS_PER_SEC: i64 = 1_000_000;
const MICROS_PER_MIN: i64 = 60 * MICROS_PER_SEC;
const MICROS_PER_HOUR: i64 = 3600 * MICROS_PER_SEC;

/// PostgreSQL `IntervalStyle` output setting (`postgres`, `postgres_verbose`, `sql_standard`, `iso_8601`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntervalFormatStyle {
    Postgres,
    PostgresVerbose,
    SqlStandard,
    Iso8601,
}

/// Parse a `ParameterStatus("IntervalStyle")` string.
pub fn parse_interval_style(raw: &str) -> Result<IntervalFormatStyle, PostgresError> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("postgres") {
        Ok(IntervalFormatStyle::Postgres)
    } else if trimmed.eq_ignore_ascii_case("postgres_verbose") {
        Ok(IntervalFormatStyle::PostgresVerbose)
    } else if trimmed.eq_ignore_ascii_case("sql_standard") {
        Ok(IntervalFormatStyle::SqlStandard)
    } else if trimmed.eq_ignore_ascii_case("iso_8601") {
        Ok(IntervalFormatStyle::Iso8601)
    } else {
        Err(PostgresError::Data(format!(
            "unrecognized PostgreSQL IntervalStyle {raw:?}"
        )))
    }
}

fn parse_signed_hms_micros(token: &str) -> Result<i64, PostgresError> {
    let (neg, body) = if let Some(rest) = token.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = token.strip_prefix('+') {
        (false, rest)
    } else {
        (false, token)
    };
    let parts: Vec<&str> = body.split(':').collect();
    if parts.len() != 2 && parts.len() != 3 {
        return Err(PostgresError::Data(format!(
            "invalid interval time token {token:?}"
        )));
    }
    if parts[0].is_empty() || !parts[0].bytes().all(|b| b.is_ascii_digit()) {
        return Err(PostgresError::Data(format!(
            "invalid hours in interval time token {token:?}"
        )));
    }
    if parts[1].is_empty() || !parts[1].bytes().all(|b| b.is_ascii_digit()) {
        return Err(PostgresError::Data(format!(
            "invalid minutes in interval time token {token:?}"
        )));
    }
    let hours: i64 = parts[0]
        .parse()
        .map_err(|_| PostgresError::Data(format!("hours overflow in {token:?}")))?;
    let mins: i64 = parts[1]
        .parse()
        .map_err(|_| PostgresError::Data(format!("minutes overflow in {token:?}")))?;
    if mins >= 60 {
        return Err(PostgresError::Data(format!(
            "minutes >= 60 in interval time token {token:?}"
        )));
    }
    let (secs, frac_micros) = if parts.len() == 3 {
        parse_unsigned_sec_and_frac(parts[2])?
    } else {
        (0i64, 0i64)
    };
    if secs >= 60 {
        return Err(PostgresError::Data(format!(
            "seconds >= 60 in interval time token {token:?}"
        )));
    }
    let mag = hours
        .checked_mul(MICROS_PER_HOUR)
        .and_then(|v| v.checked_add(mins * MICROS_PER_MIN))
        .and_then(|v| v.checked_add(secs * MICROS_PER_SEC))
        .and_then(|v| v.checked_add(frac_micros))
        .ok_or_else(|| PostgresError::Data(format!("interval time overflow in {token:?}")))?;
    if neg {
        mag.checked_neg()
            .ok_or_else(|| PostgresError::Data(format!("interval time overflow in {token:?}")))
    } else {
        Ok(mag)
    }
}

fn parse_unsigned_sec_and_frac(raw: &str) -> Result<(i64, i64), PostgresError> {
    if let Some((s, f)) = raw.split_once('.') {
        if s.is_empty()
            || !s.bytes().all(|b| b.is_ascii_digit())
            || f.is_empty()
            || f.len() > 6
            || !f.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(PostgresError::Data(format!(
                "invalid seconds/fraction {raw:?}"
            )));
        }
        let secs: i64 = s
            .parse()
            .map_err(|_| PostgresError::Data(format!("invalid seconds {raw:?}")))?;
        let mut padded = [b'0'; 6];
        padded[..f.len()].copy_from_slice(f.as_bytes());
        let frac: i64 = std::str::from_utf8(&padded)
            .expect("ASCII")
            .parse()
            .expect("6 digits");
        Ok((secs, frac))
    } else {
        if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
            return Err(PostgresError::Data(format!("invalid seconds {raw:?}")));
        }
        let secs: i64 = raw
            .parse()
            .map_err(|_| PostgresError::Data(format!("invalid seconds {raw:?}")))?;
        Ok((secs, 0))
    }
}

fn parse_signed_sec_micros(token: &str) -> Result<i64, PostgresError> {
    let (neg, body) = if let Some(rest) = token.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = token.strip_prefix('+') {
        (false, rest)
    } else {
        (false, token)
    };
    let (secs, frac) = parse_unsigned_sec_and_frac(body)?;
    let mag = secs
        .checked_mul(MICROS_PER_SEC)
        .and_then(|v| v.checked_add(frac))
        .ok_or_else(|| PostgresError::Data(format!("seconds overflow in {token:?}")))?;
    if neg {
        mag.checked_neg()
            .ok_or_else(|| PostgresError::Data(format!("seconds overflow in {token:?}")))
    } else {
        Ok(mag)
    }
}

fn decode_iso8601_interval(text: &str) -> Result<(i32, i32, i64), PostgresError> {
    let body = text
        .strip_prefix('P')
        .or_else(|| text.strip_prefix('p'))
        .ok_or_else(|| PostgresError::Data(format!("invalid ISO-8601 interval {text:?}")))?;
    if body.is_empty() {
        return Err(PostgresError::Data(format!(
            "empty ISO-8601 interval {text:?}"
        )));
    }
    let (date_part, time_part) = if let Some((d, t)) = body.split_once(['T', 't']) {
        if t.is_empty() {
            return Err(PostgresError::Data(format!(
                "empty T section in ISO-8601 interval {text:?}"
            )));
        }
        (d, Some(t))
    } else {
        (body, None)
    };

    let mut months: i32 = 0;
    let mut days: i32 = 0;
    let mut microseconds: i64 = 0;
    let mut saw_any = false;

    let mut idx = 0;
    let dbytes = date_part.as_bytes();
    while idx < dbytes.len() {
        let start = idx;
        if dbytes[idx] == b'+' || dbytes[idx] == b'-' {
            idx += 1;
        }
        let num_start = idx;
        while idx < dbytes.len() && dbytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if idx == num_start || idx >= dbytes.len() {
            return Err(PostgresError::Data(format!(
                "malformed date section in ISO-8601 interval {text:?}"
            )));
        }
        let num_str = &date_part[start..idx];
        let designator = dbytes[idx];
        idx += 1;
        let val: i32 = num_str.parse().map_err(|_| {
            PostgresError::Data(format!("integer overflow in ISO-8601 interval {text:?}"))
        })?;
        saw_any = true;
        match designator {
            b'Y' | b'y' => {
                let ym = val.checked_mul(12).ok_or_else(|| {
                    PostgresError::Data(format!("year overflow in ISO-8601 interval {text:?}"))
                })?;
                months = months.checked_add(ym).ok_or_else(|| {
                    PostgresError::Data(format!("month overflow in ISO-8601 interval {text:?}"))
                })?;
            }
            b'M' | b'm' => {
                months = months.checked_add(val).ok_or_else(|| {
                    PostgresError::Data(format!("month overflow in ISO-8601 interval {text:?}"))
                })?;
            }
            b'W' | b'w' => {
                let wd = val.checked_mul(7).ok_or_else(|| {
                    PostgresError::Data(format!("week overflow in ISO-8601 interval {text:?}"))
                })?;
                days = days.checked_add(wd).ok_or_else(|| {
                    PostgresError::Data(format!("day overflow in ISO-8601 interval {text:?}"))
                })?;
            }
            b'D' | b'd' => {
                days = days.checked_add(val).ok_or_else(|| {
                    PostgresError::Data(format!("day overflow in ISO-8601 interval {text:?}"))
                })?;
            }
            _ => {
                return Err(PostgresError::Data(format!(
                    "unknown date designator in ISO-8601 interval {text:?}"
                )));
            }
        }
    }

    if let Some(tpart) = time_part {
        let mut tidx = 0;
        let tbytes = tpart.as_bytes();
        while tidx < tbytes.len() {
            let start = tidx;
            if tbytes[tidx] == b'+' || tbytes[tidx] == b'-' {
                tidx += 1;
            }
            let num_start = tidx;
            while tidx < tbytes.len() && (tbytes[tidx].is_ascii_digit() || tbytes[tidx] == b'.') {
                tidx += 1;
            }
            if tidx == num_start || tidx >= tbytes.len() {
                return Err(PostgresError::Data(format!(
                    "malformed time section in ISO-8601 interval {text:?}"
                )));
            }
            let num_str = &tpart[start..tidx];
            let designator = tbytes[tidx];
            tidx += 1;
            saw_any = true;
            match designator {
                b'H' | b'h' => {
                    let h: i64 = num_str.parse().map_err(|_| {
                        PostgresError::Data(format!("invalid hours in ISO-8601 interval {text:?}"))
                    })?;
                    let add = h.checked_mul(MICROS_PER_HOUR).ok_or_else(|| {
                        PostgresError::Data(format!("hours overflow in ISO-8601 interval {text:?}"))
                    })?;
                    microseconds = microseconds.checked_add(add).ok_or_else(|| {
                        PostgresError::Data(format!("time overflow in ISO-8601 interval {text:?}"))
                    })?;
                }
                b'M' | b'm' => {
                    let m: i64 = num_str.parse().map_err(|_| {
                        PostgresError::Data(format!(
                            "invalid minutes in ISO-8601 interval {text:?}"
                        ))
                    })?;
                    let add = m.checked_mul(MICROS_PER_MIN).ok_or_else(|| {
                        PostgresError::Data(format!(
                            "minutes overflow in ISO-8601 interval {text:?}"
                        ))
                    })?;
                    microseconds = microseconds.checked_add(add).ok_or_else(|| {
                        PostgresError::Data(format!("time overflow in ISO-8601 interval {text:?}"))
                    })?;
                }
                b'S' | b's' => {
                    let s_micros = parse_signed_sec_micros(num_str)?;
                    microseconds = microseconds.checked_add(s_micros).ok_or_else(|| {
                        PostgresError::Data(format!(
                            "seconds overflow in ISO-8601 interval {text:?}"
                        ))
                    })?;
                }
                _ => {
                    return Err(PostgresError::Data(format!(
                        "unknown time designator in ISO-8601 interval {text:?}"
                    )));
                }
            }
        }
    }

    if !saw_any {
        return Err(PostgresError::Data(format!(
            "empty ISO-8601 interval {text:?}"
        )));
    }
    validate_interval_parts(months, days, microseconds)
}

fn try_parse_ym_token(token: &str) -> Option<Result<i32, PostgresError>> {
    let (neg, rest) = if let Some(r) = token.strip_prefix('-') {
        (true, r)
    } else if let Some(r) = token.strip_prefix('+') {
        (false, r)
    } else {
        (false, token)
    };
    let (y_str, m_str) = rest.split_once('-')?;
    if y_str.is_empty()
        || m_str.is_empty()
        || m_str.contains('-')
        || !y_str.bytes().all(|b| b.is_ascii_digit())
        || !m_str.bytes().all(|b| b.is_ascii_digit())
    {
        return Some(Err(PostgresError::Data(format!(
            "invalid SQL year-month interval token {token:?}"
        ))));
    }
    let parsed = (|| {
        let y: i32 = y_str
            .parse()
            .map_err(|_| PostgresError::Data(format!("year overflow in {token:?}")))?;
        let m: i32 = m_str
            .parse()
            .map_err(|_| PostgresError::Data(format!("month overflow in {token:?}")))?;
        if m >= 12 {
            return Err(PostgresError::Data(format!(
                "months >= 12 in SQL year-month token {token:?}"
            )));
        }
        let mag = y
            .checked_mul(12)
            .and_then(|v| v.checked_add(m))
            .ok_or_else(|| PostgresError::Data(format!("month overflow in {token:?}")))?;
        if neg {
            mag.checked_neg()
                .ok_or_else(|| PostgresError::Data(format!("month overflow in {token:?}")))
        } else {
            Ok(mag)
        }
    })();
    Some(parsed)
}

fn decode_unit_pairs_and_hms(
    tokens: &[&str],
    negate_all: bool,
) -> Result<(i32, i32, i64), PostgresError> {
    let mut months: i32 = 0;
    let mut days: i32 = 0;
    let mut microseconds: i64 = 0;
    let mut i = 0;

    while i < tokens.len() {
        let tok = tokens[i];
        if tok.contains(':') {
            let t = parse_signed_hms_micros(tok)?;
            microseconds = microseconds
                .checked_add(t)
                .ok_or_else(|| PostgresError::Data("interval time overflow".to_string()))?;
            i += 1;
            continue;
        }
        if i + 1 < tokens.len() {
            let unit = tokens[i + 1].to_ascii_lowercase();
            match unit.as_str() {
                "year" | "years" | "yr" | "yrs" => {
                    let y: i32 = tok.parse().map_err(|_| {
                        PostgresError::Data(format!("invalid interval years {tok:?}"))
                    })?;
                    let ym = y.checked_mul(12).ok_or_else(|| {
                        PostgresError::Data(format!("interval year overflow {tok:?}"))
                    })?;
                    months = months.checked_add(ym).ok_or_else(|| {
                        PostgresError::Data("interval month overflow".to_string())
                    })?;
                    i += 2;
                    continue;
                }
                "mon" | "mons" | "month" | "months" => {
                    let m: i32 = tok.parse().map_err(|_| {
                        PostgresError::Data(format!("invalid interval months {tok:?}"))
                    })?;
                    months = months.checked_add(m).ok_or_else(|| {
                        PostgresError::Data("interval month overflow".to_string())
                    })?;
                    i += 2;
                    continue;
                }
                "day" | "days" => {
                    let d: i32 = tok.parse().map_err(|_| {
                        PostgresError::Data(format!("invalid interval days {tok:?}"))
                    })?;
                    days = days
                        .checked_add(d)
                        .ok_or_else(|| PostgresError::Data("interval day overflow".to_string()))?;
                    i += 2;
                    continue;
                }
                "hour" | "hours" | "hr" | "hrs" => {
                    let h: i64 = tok.parse().map_err(|_| {
                        PostgresError::Data(format!("invalid interval hours {tok:?}"))
                    })?;
                    let hm = h
                        .checked_mul(MICROS_PER_HOUR)
                        .ok_or_else(|| PostgresError::Data("interval hour overflow".to_string()))?;
                    microseconds = microseconds
                        .checked_add(hm)
                        .ok_or_else(|| PostgresError::Data("interval time overflow".to_string()))?;
                    i += 2;
                    continue;
                }
                "min" | "mins" | "minute" | "minutes" => {
                    let m: i64 = tok.parse().map_err(|_| {
                        PostgresError::Data(format!("invalid interval minutes {tok:?}"))
                    })?;
                    let mm = m.checked_mul(MICROS_PER_MIN).ok_or_else(|| {
                        PostgresError::Data("interval minute overflow".to_string())
                    })?;
                    microseconds = microseconds
                        .checked_add(mm)
                        .ok_or_else(|| PostgresError::Data("interval time overflow".to_string()))?;
                    i += 2;
                    continue;
                }
                "sec" | "secs" | "second" | "seconds" => {
                    let sm = parse_signed_sec_micros(tok)?;
                    microseconds = microseconds.checked_add(sm).ok_or_else(|| {
                        PostgresError::Data("interval seconds overflow".to_string())
                    })?;
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        return Err(PostgresError::Data(format!(
            "unrecognized interval token {tok:?}"
        )));
    }

    if negate_all {
        months = months
            .checked_neg()
            .ok_or_else(|| PostgresError::Data("interval month negation overflow".to_string()))?;
        days = days
            .checked_neg()
            .ok_or_else(|| PostgresError::Data("interval day negation overflow".to_string()))?;
        microseconds = microseconds
            .checked_neg()
            .ok_or_else(|| PostgresError::Data("interval time negation overflow".to_string()))?;
    }
    validate_interval_parts(months, days, microseconds)
}

fn decode_sql_standard_interval(tokens: &[&str]) -> Result<(i32, i32, i64), PostgresError> {
    if tokens == ["0"] {
        return Ok((0, 0, 0));
    }
    let mut months: i32 = 0;
    let mut days: i32 = 0;
    let mut microseconds: i64 = 0;
    let mut idx = 0;

    if idx < tokens.len()
        && let Some(ym_res) = try_parse_ym_token(tokens[idx])
    {
        months = ym_res?;
        idx += 1;
    }

    if idx < tokens.len() && !tokens[idx].contains(':') {
        let day_tok = tokens[idx];
        let day_neg = day_tok.starts_with('-');
        let parsed_days: i32 = day_tok.parse().map_err(|_| {
            PostgresError::Data(format!("invalid SQL interval day token {day_tok:?}"))
        })?;
        days = parsed_days;
        idx += 1;
        if idx < tokens.len() && tokens[idx].contains(':') {
            let hms_tok = tokens[idx];
            let has_explicit_sign = hms_tok.starts_with('+') || hms_tok.starts_with('-');
            let mut t = parse_signed_hms_micros(hms_tok)?;
            if !has_explicit_sign && day_neg {
                t = t
                    .checked_neg()
                    .ok_or_else(|| PostgresError::Data("interval time overflow".to_string()))?;
            }
            microseconds = t;
            idx += 1;
        }
    } else if idx < tokens.len() && tokens[idx].contains(':') {
        microseconds = parse_signed_hms_micros(tokens[idx])?;
        idx += 1;
    }

    if idx != tokens.len() {
        return Err(PostgresError::Data(format!(
            "unexpected trailing tokens in SQL interval {tokens:?}"
        )));
    }
    validate_interval_parts(months, days, microseconds)
}

/// Decode a PostgreSQL `INTERVAL` text representation across all four `IntervalStyle` formats.
pub fn decode_interval_text(
    text: &str,
    ctx: &CodecContext,
) -> Result<(i32, i32, i64), PostgresError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(PostgresError::Data(
            "empty string is not a valid PostgreSQL INTERVAL".to_string(),
        ));
    }
    if trimmed.eq_ignore_ascii_case("infinity") || trimmed.eq_ignore_ascii_case("+infinity") {
        return Ok(PG_INTERVAL_POS_INF);
    }
    if trimmed.eq_ignore_ascii_case("-infinity") {
        return Ok(PG_INTERVAL_NEG_INF);
    }
    let _ = parse_interval_style(&ctx.interval_style)?;

    if trimmed.starts_with('P') || trimmed.starts_with('p') {
        return decode_iso8601_interval(trimmed);
    }
    if let Some(after_at) = trimmed.strip_prefix('@') {
        let inner = after_at.trim();
        if inner == "0" {
            return Ok((0, 0, 0));
        }
        let (body, is_ago) = if let Some(before_ago) = inner
            .strip_suffix(" ago")
            .or_else(|| inner.strip_suffix(" AGO"))
        {
            (before_ago.trim(), true)
        } else {
            (inner, false)
        };
        let tokens: Vec<&str> = body.split_whitespace().collect();
        if tokens.is_empty() {
            return Err(PostgresError::Data(format!(
                "empty verbose interval {text:?}"
            )));
        }
        return decode_unit_pairs_and_hms(&tokens, is_ago);
    }

    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    if tokens == ["0"] {
        return Ok((0, 0, 0));
    }

    let has_unit_words = tokens
        .iter()
        .any(|t| t.bytes().any(|b| b.is_ascii_alphabetic()));
    if has_unit_words || (tokens.len() == 1 && tokens[0].contains(':')) {
        decode_unit_pairs_and_hms(&tokens, false)
    } else {
        decode_sql_standard_interval(&tokens)
    }
}

/// Render a PostgreSQL `INTERVAL` `(months, days, microseconds)` into canonical ISO-8601 text.
pub fn encode_interval_text(months: i32, days: i32, microseconds: i64) -> String {
    if (months, days, microseconds) == PG_INTERVAL_POS_INF {
        return "infinity".to_string();
    }
    if (months, days, microseconds) == PG_INTERVAL_NEG_INF {
        return "-infinity".to_string();
    }
    if months == 0 && days == 0 && microseconds == 0 {
        return "PT0S".to_string();
    }

    let mut out = String::from("P");
    if months != 0 {
        out.push_str(&format!("{months}M"));
    }
    if days != 0 {
        out.push_str(&format!("{days}D"));
    }
    if microseconds != 0 {
        out.push('T');
        let hours = microseconds / MICROS_PER_HOUR;
        let rem_after_h = microseconds % MICROS_PER_HOUR;
        let mins = rem_after_h / MICROS_PER_MIN;
        let rem_after_m = rem_after_h % MICROS_PER_MIN;
        let secs = rem_after_m / MICROS_PER_SEC;
        let frac = rem_after_m % MICROS_PER_SEC;

        if hours != 0 {
            out.push_str(&format!("{hours}H"));
        }
        if mins != 0 {
            out.push_str(&format!("{mins}M"));
        }
        if secs != 0 || frac != 0 {
            if frac == 0 {
                out.push_str(&format!("{secs}S"));
            } else {
                let sign = if secs < 0 || frac < 0 { "-" } else { "" };
                let abs_secs = secs.unsigned_abs();
                let abs_frac = frac.unsigned_abs();
                let mut frac_str = format!("{abs_frac:06}");
                while frac_str.ends_with('0') {
                    frac_str.pop();
                }
                out.push_str(&format!("{sign}{abs_secs}.{frac_str}S"));
            }
        }
    }
    out
}
