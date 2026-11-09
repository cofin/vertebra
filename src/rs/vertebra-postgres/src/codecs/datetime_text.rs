//! Session-aware PostgreSQL `DateStyle` and `TimeZone` temporal text codecs (`DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`).

use crate::codecs::CodecContext;
use crate::codecs::temporal::{
    PG_DATE_MAX_FINITE, PG_DATE_MIN_FINITE, pg_days_to_ymd, validate_date_days,
    validate_time_micros, validate_timestamp_micros, ymd_to_pg_days,
};
use crate::errors::PostgresError;
use crate::result::{
    PG_DATE_NEG_INF, PG_DATE_POS_INF, PG_TIME_MAX_MICROS, PG_TS_NEG_INF, PG_TS_POS_INF, PgValue,
    TemporalKind,
};

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// PostgreSQL `DateStyle` output style (`ISO`, `SQL`, `Postgres`, `German`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateFormatStyle {
    Iso,
    Sql,
    Postgres,
    German,
}

/// PostgreSQL `DateStyle` field ordering (`MDY`, `DMY`, `YMD`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateOrder {
    Mdy,
    Dmy,
    Ymd,
}

/// Parse a `ParameterStatus("DateStyle")` value (e.g. `"ISO, MDY"`, `"Postgres, DMY"`, `"German, DMY"`).
pub fn parse_date_style(raw: &str) -> (DateFormatStyle, DateOrder) {
    let mut style = DateFormatStyle::Iso;
    let mut order = DateOrder::Mdy;
    for part in raw
        .split([',', ' '])
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        if part.eq_ignore_ascii_case("iso") {
            style = DateFormatStyle::Iso;
        } else if part.eq_ignore_ascii_case("sql") {
            style = DateFormatStyle::Sql;
        } else if part.eq_ignore_ascii_case("postgres") {
            style = DateFormatStyle::Postgres;
        } else if part.eq_ignore_ascii_case("german") {
            style = DateFormatStyle::German;
            order = DateOrder::Dmy;
        } else if part.eq_ignore_ascii_case("mdy") || part.eq_ignore_ascii_case("us") {
            order = DateOrder::Mdy;
        } else if part.eq_ignore_ascii_case("dmy") || part.eq_ignore_ascii_case("euro") {
            order = DateOrder::Dmy;
        } else if part.eq_ignore_ascii_case("ymd") {
            order = DateOrder::Ymd;
        }
    }
    (style, order)
}

fn strip_era(input: &str) -> (&str, bool) {
    let trimmed = input.trim();
    if let Some(before) = trimmed
        .strip_suffix(" BC")
        .or_else(|| trimmed.strip_suffix(" bc"))
    {
        (before.trim_end(), true)
    } else if let Some(before) = trimmed
        .strip_suffix(" AD")
        .or_else(|| trimmed.strip_suffix(" ad"))
    {
        (before.trim_end(), false)
    } else {
        (trimmed, false)
    }
}

fn to_astro_year(year_pos: i32, is_bc: bool) -> Result<i32, PostgresError> {
    if year_pos < 1 {
        return Err(PostgresError::Data(format!(
            "calendar year {year_pos} must be >= 1"
        )));
    }
    if is_bc {
        Ok(1 - year_pos)
    } else {
        Ok(year_pos)
    }
}

fn parse_month_abbrev(mon: &str) -> Result<u8, PostgresError> {
    match mon.to_ascii_lowercase().as_str() {
        "jan" => Ok(1),
        "feb" => Ok(2),
        "mar" => Ok(3),
        "apr" => Ok(4),
        "may" => Ok(5),
        "jun" => Ok(6),
        "jul" => Ok(7),
        "aug" => Ok(8),
        "sep" => Ok(9),
        "oct" => Ok(10),
        "nov" => Ok(11),
        "dec" => Ok(12),
        _ => Err(PostgresError::Data(format!(
            "invalid month abbreviation {mon:?}"
        ))),
    }
}

fn parse_date_triple(
    body: &str,
    style: DateFormatStyle,
    order: DateOrder,
    is_bc: bool,
) -> Result<(i32, u8, u8), PostgresError> {
    if let Some((a, rest)) = body.split_once('.') {
        let (b, c) = rest
            .split_once('.')
            .ok_or_else(|| PostgresError::Data(format!("invalid German date literal {body:?}")))?;
        let day: u8 = a
            .parse()
            .map_err(|_| PostgresError::Data(format!("invalid day in {body:?}")))?;
        let month: u8 = b
            .parse()
            .map_err(|_| PostgresError::Data(format!("invalid month in {body:?}")))?;
        let year_pos: i32 = c
            .parse()
            .map_err(|_| PostgresError::Data(format!("invalid year in {body:?}")))?;
        return Ok((to_astro_year(year_pos, is_bc)?, month, day));
    }

    if let Some((a, rest)) = body.split_once('/') {
        let (b, c) = rest
            .split_once('/')
            .ok_or_else(|| PostgresError::Data(format!("invalid SQL date literal {body:?}")))?;
        let (year_pos, month, day): (i32, u8, u8) = match order {
            DateOrder::Ymd => (
                a.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid year in {body:?}")))?,
                b.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid month in {body:?}")))?,
                c.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid day in {body:?}")))?,
            ),
            DateOrder::Dmy => (
                c.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid year in {body:?}")))?,
                b.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid month in {body:?}")))?,
                a.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid day in {body:?}")))?,
            ),
            DateOrder::Mdy => (
                c.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid year in {body:?}")))?,
                a.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid month in {body:?}")))?,
                b.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid day in {body:?}")))?,
            ),
        };
        return Ok((to_astro_year(year_pos, is_bc)?, month, day));
    }

    if let Some((a, rest)) = body.split_once('-') {
        let (b, c) = rest
            .split_once('-')
            .ok_or_else(|| PostgresError::Data(format!("invalid dash date literal {body:?}")))?;
        let use_ymd = matches!(style, DateFormatStyle::Iso)
            || matches!(order, DateOrder::Ymd)
            || a.len() >= 4;
        let (year_pos, month, day): (i32, u8, u8) = if use_ymd {
            (
                a.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid year in {body:?}")))?,
                b.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid month in {body:?}")))?,
                c.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid day in {body:?}")))?,
            )
        } else if matches!(order, DateOrder::Dmy) {
            (
                c.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid year in {body:?}")))?,
                b.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid month in {body:?}")))?,
                a.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid day in {body:?}")))?,
            )
        } else {
            (
                c.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid year in {body:?}")))?,
                a.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid month in {body:?}")))?,
                b.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid day in {body:?}")))?,
            )
        };
        return Ok((to_astro_year(year_pos, is_bc)?, month, day));
    }

    Err(PostgresError::Data(format!(
        "unrecognized PostgreSQL date literal {body:?}"
    )))
}

/// Decode a PostgreSQL `DATE` text representation across all four `DateStyle` formats.
pub fn decode_date_text(text: &str, ctx: &CodecContext) -> Result<i32, PostgresError> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("infinity") || trimmed.eq_ignore_ascii_case("+infinity") {
        return Ok(PG_DATE_POS_INF);
    }
    if trimmed.eq_ignore_ascii_case("-infinity") {
        return Ok(PG_DATE_NEG_INF);
    }
    let (style, order) = parse_date_style(&ctx.date_style);
    let (body, is_bc) = strip_era(trimmed);
    let (year_astro, month, day) = parse_date_triple(body, style, order, is_bc)?;
    ymd_to_pg_days(year_astro, month, day)
}

/// Decode a PostgreSQL `TIME` text representation (`HH:MM:SS[.ffffff]`, including `24:00:00`).
pub fn decode_time_text(text: &str) -> Result<i64, PostgresError> {
    let trimmed = text.trim();
    let mut parts = trimmed.split(':');
    let h_str = parts
        .next()
        .ok_or_else(|| PostgresError::Data(format!("invalid TIME literal {text:?}")))?;
    let m_str = parts
        .next()
        .ok_or_else(|| PostgresError::Data(format!("invalid TIME literal {text:?}")))?;
    let s_part = parts
        .next()
        .ok_or_else(|| PostgresError::Data(format!("invalid TIME literal {text:?}")))?;
    if parts.next().is_some() {
        return Err(PostgresError::Data(format!(
            "too many colon-separated fields in TIME literal {text:?}"
        )));
    }
    if h_str.len() < 2 || !h_str.bytes().all(|b| b.is_ascii_digit()) {
        return Err(PostgresError::Data(format!(
            "invalid hours in TIME literal {text:?}"
        )));
    }
    if m_str.len() != 2 || !m_str.bytes().all(|b| b.is_ascii_digit()) {
        return Err(PostgresError::Data(format!(
            "invalid minutes in TIME literal {text:?}"
        )));
    }
    let hours: i64 = h_str
        .parse()
        .map_err(|_| PostgresError::Data(format!("invalid hours in TIME literal {text:?}")))?;
    let mins: i64 = m_str
        .parse()
        .map_err(|_| PostgresError::Data(format!("invalid minutes in TIME literal {text:?}")))?;

    let (sec_str, frac_micros) = if let Some((s, f)) = s_part.split_once('.') {
        if f.is_empty() || f.len() > 6 || !f.bytes().all(|b| b.is_ascii_digit()) {
            return Err(PostgresError::Data(format!(
                "invalid fractional seconds in TIME literal {text:?}"
            )));
        }
        let mut padded = [b'0'; 6];
        padded[..f.len()].copy_from_slice(f.as_bytes());
        let frac: i64 = std::str::from_utf8(&padded)
            .expect("ASCII digits")
            .parse()
            .expect("6-digit integer");
        (s, frac)
    } else {
        (s_part, 0i64)
    };

    if sec_str.len() != 2 || !sec_str.bytes().all(|b| b.is_ascii_digit()) {
        return Err(PostgresError::Data(format!(
            "invalid seconds in TIME literal {text:?}"
        )));
    }
    let secs: i64 = sec_str
        .parse()
        .map_err(|_| PostgresError::Data(format!("invalid seconds in TIME literal {text:?}")))?;
    if mins >= 60 || secs >= 60 || hours > 24 {
        return Err(PostgresError::Data(format!(
            "TIME fields out of range in {text:?}"
        )));
    }
    let total = ((hours * 3600) + (mins * 60) + secs) * 1_000_000 + frac_micros;
    validate_time_micros(total)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ParsedTz<'a> {
    None,
    NumericMicros(i64),
    Abbrev(&'a str),
}

fn parse_numeric_offset_micros(raw: &str) -> Result<i64, PostgresError> {
    let bytes = raw.as_bytes();
    if bytes.len() < 3 || (bytes[0] != b'+' && bytes[0] != b'-') {
        return Err(PostgresError::Data(format!("invalid UTC offset {raw:?}")));
    }
    let sign: i64 = if bytes[0] == b'-' { -1 } else { 1 };
    let body = &raw[1..];
    let (h, m, s): (i64, i64, i64) = if body.contains(':') {
        let parts: Vec<&str> = body.split(':').collect();
        match parts.as_slice() {
            [hh, mm] if hh.len() == 2 && mm.len() == 2 => (
                hh.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid offset {raw:?}")))?,
                mm.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid offset {raw:?}")))?,
                0,
            ),
            [hh, mm, ss] if hh.len() == 2 && mm.len() == 2 && ss.len() == 2 => (
                hh.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid offset {raw:?}")))?,
                mm.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid offset {raw:?}")))?,
                ss.parse()
                    .map_err(|_| PostgresError::Data(format!("invalid offset {raw:?}")))?,
            ),
            _ => {
                return Err(PostgresError::Data(format!(
                    "invalid colon UTC offset {raw:?}"
                )));
            }
        }
    } else if body.len() == 2 && body.bytes().all(|b| b.is_ascii_digit()) {
        (
            body.parse()
                .map_err(|_| PostgresError::Data(format!("invalid offset {raw:?}")))?,
            0,
            0,
        )
    } else if body.len() == 4 && body.bytes().all(|b| b.is_ascii_digit()) {
        (
            body[0..2]
                .parse()
                .map_err(|_| PostgresError::Data(format!("invalid offset {raw:?}")))?,
            body[2..4]
                .parse()
                .map_err(|_| PostgresError::Data(format!("invalid offset {raw:?}")))?,
            0,
        )
    } else {
        return Err(PostgresError::Data(format!("invalid UTC offset {raw:?}")));
    };

    if h > 16 || m >= 60 || s >= 60 {
        return Err(PostgresError::Data(format!(
            "UTC offset out of range in {raw:?}"
        )));
    }
    Ok(sign * ((h * 3600 + m * 60 + s) * 1_000_000))
}

fn classify_tz_token(token: &str) -> Result<ParsedTz<'_>, PostgresError> {
    if token.eq_ignore_ascii_case("utc")
        || token.eq_ignore_ascii_case("gmt")
        || token.eq_ignore_ascii_case("z")
    {
        Ok(ParsedTz::NumericMicros(0))
    } else if token.starts_with('+') || token.starts_with('-') {
        Ok(ParsedTz::NumericMicros(parse_numeric_offset_micros(token)?))
    } else if !token.is_empty()
        && token
            .bytes()
            .all(|b| b.is_ascii_alphabetic() || b == b'/' || b == b'_' || b == b'-' || b == b'+')
    {
        Ok(ParsedTz::Abbrev(token))
    } else {
        Err(PostgresError::Data(format!(
            "invalid timezone token {token:?}"
        )))
    }
}

fn split_attached_offset(time_with_tz: &str) -> Result<(&str, ParsedTz<'_>), PostgresError> {
    if let Some(stripped) = time_with_tz
        .strip_suffix('Z')
        .or_else(|| time_with_tz.strip_suffix('z'))
    {
        return Ok((stripped, ParsedTz::NumericMicros(0)));
    }
    if let Some(idx) = time_with_tz.rfind(['+', '-'])
        && idx >= 8
    {
        let time_part = &time_with_tz[..idx];
        let offset_part = &time_with_tz[idx..];
        let micros = parse_numeric_offset_micros(offset_part)?;
        return Ok((time_part, ParsedTz::NumericMicros(micros)));
    }
    Ok((time_with_tz, ParsedTz::None))
}

const VALID_DOW: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

/// Decode a PostgreSQL `TIMESTAMP` (`OID 1114`) or `TIMESTAMPTZ` (`OID 1184`) text representation.
pub fn decode_timestamp_text(
    text: &str,
    kind: TemporalKind,
    ctx: &CodecContext,
) -> Result<PgValue, PostgresError> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("infinity") || trimmed.eq_ignore_ascii_case("+infinity") {
        return Ok(match kind {
            TemporalKind::Timestamp => PgValue::Timestamp(PG_TS_POS_INF),
            TemporalKind::TimestampTz => PgValue::TimestampTz(PG_TS_POS_INF),
            _ => {
                return Err(PostgresError::Data(
                    "invalid temporal kind for timestamp decoder".to_string(),
                ));
            }
        });
    }
    if trimmed.eq_ignore_ascii_case("-infinity") {
        return Ok(match kind {
            TemporalKind::Timestamp => PgValue::Timestamp(PG_TS_NEG_INF),
            TemporalKind::TimestampTz => PgValue::TimestampTz(PG_TS_NEG_INF),
            _ => {
                return Err(PostgresError::Data(
                    "invalid temporal kind for timestamp decoder".to_string(),
                ));
            }
        });
    }

    let (style, order) = parse_date_style(&ctx.date_style);
    let (body, mut is_bc) = strip_era(trimmed);
    let mut tokens: Vec<&str> = Vec::new();
    for tok in body.split_whitespace() {
        if tok.eq_ignore_ascii_case("bc") {
            is_bc = true;
        } else if tok.eq_ignore_ascii_case("ad") {
            is_bc = false;
        } else {
            tokens.push(tok);
        }
    }
    if tokens.is_empty() {
        return Err(PostgresError::Data(format!(
            "empty TIMESTAMP literal {text:?}"
        )));
    }

    let (pg_days, time_micros, tz) =
        if tokens.len() >= 4 && VALID_DOW.contains(&tokens[0].to_ascii_lowercase().as_str()) {
            let (month, day, time_str, year_str) = if matches!(order, DateOrder::Dmy) {
                let d: u8 = tokens[1]
                    .parse()
                    .map_err(|_| PostgresError::Data(format!("invalid day in {text:?}")))?;
                let m = parse_month_abbrev(tokens[2])?;
                (m, d, tokens[3], tokens[4])
            } else {
                let m = parse_month_abbrev(tokens[1])?;
                let d: u8 = tokens[2]
                    .parse()
                    .map_err(|_| PostgresError::Data(format!("invalid day in {text:?}")))?;
                (m, d, tokens[3], tokens[4])
            };
            if tokens.len() < 5 || tokens.len() > 6 {
                return Err(PostgresError::Data(format!(
                    "invalid Postgres-style timestamp token count in {text:?}"
                )));
            }
            let year_pos: i32 = year_str
                .parse()
                .map_err(|_| PostgresError::Data(format!("invalid year in {text:?}")))?;
            let year_astro = to_astro_year(year_pos, is_bc)?;
            let days = ymd_to_pg_days(year_astro, month, day)?;
            let t_micros = decode_time_text(time_str)?;
            let parsed_tz = if tokens.len() == 6 {
                classify_tz_token(tokens[5])?
            } else {
                ParsedTz::None
            };
            (days, t_micros, parsed_tz)
        } else if tokens.len() == 1 {
            let (date_str, time_and_tz) = tokens[0].split_once(['T', 't']).ok_or_else(|| {
                PostgresError::Data(format!("invalid TIMESTAMP literal {text:?}"))
            })?;
            let (year_astro, month, day) = parse_date_triple(date_str, style, order, is_bc)?;
            let days = ymd_to_pg_days(year_astro, month, day)?;
            let (time_str, parsed_tz) = split_attached_offset(time_and_tz)?;
            let t_micros = decode_time_text(time_str)?;
            (days, t_micros, parsed_tz)
        } else if tokens.len() == 2 || tokens.len() == 3 {
            let (year_astro, month, day) = parse_date_triple(tokens[0], style, order, is_bc)?;
            let days = ymd_to_pg_days(year_astro, month, day)?;
            let (time_str, attached_tz) = split_attached_offset(tokens[1])?;
            let t_micros = decode_time_text(time_str)?;
            let parsed_tz = if tokens.len() == 3 {
                if attached_tz != ParsedTz::None {
                    return Err(PostgresError::Data(format!(
                        "duplicate timezone offsets in {text:?}"
                    )));
                }
                classify_tz_token(tokens[2])?
            } else {
                attached_tz
            };
            (days, t_micros, parsed_tz)
        } else {
            return Err(PostgresError::Data(format!(
                "invalid TIMESTAMP literal {text:?}"
            )));
        };

    let local_micros = i64::from(pg_days)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|d| d.checked_add(time_micros))
        .ok_or_else(|| PostgresError::Data(format!("TIMESTAMP overflow in {text:?}")))?;

    match kind {
        TemporalKind::Timestamp => {
            validate_timestamp_micros(local_micros)?;
            Ok(PgValue::Timestamp(local_micros))
        }
        TemporalKind::TimestampTz => match tz {
            ParsedTz::NumericMicros(offset_micros) => {
                let utc_micros = local_micros.checked_sub(offset_micros).ok_or_else(|| {
                    PostgresError::Data(format!("TIMESTAMPTZ overflow in {text:?}"))
                })?;
                validate_timestamp_micros(utc_micros)?;
                Ok(PgValue::TimestampTz(utc_micros))
            }
            ParsedTz::Abbrev(_) | ParsedTz::None => Ok(PgValue::TemporalText {
                kind: TemporalKind::TimestampTz,
                wire_text: text.to_string(),
                date_style: ctx.date_style.clone(),
                time_zone: ctx.time_zone.clone(),
            }),
        },
        _ => Err(PostgresError::Data(
            "invalid temporal kind for timestamp decoder".to_string(),
        )),
    }
}

fn format_date_iso(days: i32) -> (String, bool) {
    let (year_astro, month, day) = pg_days_to_ymd(days);
    let (year_pos, is_bc) = if year_astro <= 0 {
        (1 - year_astro, true)
    } else {
        (year_astro, false)
    };
    (format!("{year_pos:04}-{month:02}-{day:02}"), is_bc)
}

fn format_time_iso(micros: i64) -> String {
    if micros == PG_TIME_MAX_MICROS {
        return "24:00:00".to_string();
    }
    let total_secs = micros / 1_000_000;
    let rem_micros = micros % 1_000_000;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    if rem_micros == 0 {
        format!("{hours:02}:{mins:02}:{secs:02}")
    } else {
        let mut frac = format!("{rem_micros:06}");
        while frac.ends_with('0') {
            frac.pop();
        }
        format!("{hours:02}:{mins:02}:{secs:02}.{frac}")
    }
}

/// Render a temporal [`PgValue`] (`Date`, `Time`, `Timestamp`, `TimestampTz`) into canonical ISO-8601 text.
///
/// Rejects [`PgValue::TemporalText`] (`wire_text`) before wire submission.
pub fn encode_temporal_text(value: &PgValue) -> Result<String, PostgresError> {
    match value {
        PgValue::Date(days) => {
            validate_date_days(*days)?;
            if *days == PG_DATE_POS_INF {
                return Ok("infinity".to_string());
            }
            if *days == PG_DATE_NEG_INF {
                return Ok("-infinity".to_string());
            }
            let (ymd, is_bc) = format_date_iso(*days);
            if is_bc {
                Ok(format!("{ymd} BC"))
            } else {
                Ok(ymd)
            }
        }
        PgValue::Time(micros) => {
            validate_time_micros(*micros)?;
            Ok(format_time_iso(*micros))
        }
        PgValue::Timestamp(micros) | PgValue::TimestampTz(micros) => {
            let is_tz = matches!(value, PgValue::TimestampTz(_));
            validate_timestamp_micros(*micros)?;
            if *micros == PG_TS_POS_INF {
                return Ok("infinity".to_string());
            }
            if *micros == PG_TS_NEG_INF {
                return Ok("-infinity".to_string());
            }
            let days_i64 = micros.div_euclid(MICROS_PER_DAY);
            let time_micros = micros.rem_euclid(MICROS_PER_DAY);
            let days = i32::try_from(days_i64).map_err(|_| {
                PostgresError::Data("TIMESTAMP day component overflows i32".to_string())
            })?;
            if !(PG_DATE_MIN_FINITE..=PG_DATE_MAX_FINITE).contains(&days) {
                return Err(PostgresError::Data(
                    "TIMESTAMP day component outside finite bounds".to_string(),
                ));
            }
            let (ymd, is_bc) = format_date_iso(days);
            let hms = format_time_iso(time_micros);
            let tz_suffix = if is_tz { "+00" } else { "" };
            let era_suffix = if is_bc { " BC" } else { "" };
            Ok(format!("{ymd} {hms}{tz_suffix}{era_suffix}"))
        }
        PgValue::TemporalText { .. } => Err(PostgresError::Data(
            "unresolved TemporalText (wire_text) values are output-only and cannot be bound as typed temporal parameters".to_string(),
        )),
        other => Err(PostgresError::Data(format!(
            "cannot encode non-temporal value {other:?} as temporal text"
        ))),
    }
}
