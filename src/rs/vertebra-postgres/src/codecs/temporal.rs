//! Lossless PostgreSQL `2000-01-01` epoch temporal and interval binary codecs (`DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`).

use bytes::{BufMut, BytesMut};

use crate::errors::PostgresError;
use crate::result::{
    PG_DATE_NEG_INF, PG_DATE_POS_INF, PG_INTERVAL_NEG_INF, PG_INTERVAL_POS_INF, PG_TIME_MAX_MICROS,
    PG_TS_NEG_INF, PG_TS_POS_INF,
};

/// PostgreSQL `DATE` type OID (`1082`).
pub const DATE_OID: u32 = 1082;
/// PostgreSQL `TIME WITHOUT TIME ZONE` type OID (`1083`).
pub const TIME_OID: u32 = 1083;
/// PostgreSQL `TIMESTAMP WITHOUT TIME ZONE` type OID (`1114`).
pub const TIMESTAMP_OID: u32 = 1114;
/// PostgreSQL `TIMESTAMP WITH TIME ZONE` type OID (`1184`).
pub const TIMESTAMPTZ_OID: u32 = 1184;
/// PostgreSQL `INTERVAL` type OID (`1186`).
pub const INTERVAL_OID: u32 = 1186;

/// Minimum finite PostgreSQL `DATE` value (`-2_451_545` days from `2000-01-01`).
pub const PG_DATE_MIN_FINITE: i32 = -2_451_545;
/// Maximum finite PostgreSQL `DATE` value (`2_147_483_493` days from `2000-01-01`, `5874897-12-31 AD`).
pub const PG_DATE_MAX_FINITE: i32 = 2_147_483_493;
/// Minimum finite PostgreSQL `TIMESTAMP` / `TIMESTAMPTZ` value in microseconds from `2000-01-01 00:00:00`.
pub const PG_TS_MIN_FINITE: i64 = -211_813_488_000_000_000;
/// Maximum finite PostgreSQL `TIMESTAMP` / `TIMESTAMPTZ` value in microseconds (`294276-12-31 23:59:59.999999 AD`).
pub const PG_TS_MAX_FINITE: i64 = 9_223_371_331_200_000_000;

/// Return `true` if `year_astro` (where `1 AD = 1`, `1 BC = 0`, `5 BC = -4`) is a proleptic Gregorian leap year.
pub const fn is_leap_year(year_astro: i32) -> bool {
    year_astro.rem_euclid(4) == 0
        && (year_astro.rem_euclid(100) != 0 || year_astro.rem_euclid(400) == 0)
}

/// Return the number of days in `month` (`1..=12`) for astronomical year `year_astro`.
pub fn days_in_month(year_astro: i32, month: u8) -> Result<u8, PostgresError> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Ok(31),
        4 | 6 | 9 | 11 => Ok(30),
        2 => Ok(if is_leap_year(year_astro) { 29 } else { 28 }),
        other => Err(PostgresError::Data(format!(
            "invalid calendar month {other} (expected 1..=12)"
        ))),
    }
}

/// Convert an astronomical proleptic Gregorian `(year_astro, month, day)` to days since `2000-01-01`.
pub fn ymd_to_pg_days(year_astro: i32, month: u8, day: u8) -> Result<i32, PostgresError> {
    let max_day = days_in_month(year_astro, month)?;
    if day < 1 || day > max_day {
        return Err(PostgresError::Data(format!(
            "invalid calendar day {day} for year_astro={year_astro}, month={month}"
        )));
    }
    let y = i64::from(year_astro) - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let m_adj = if month > 2 {
        i64::from(month - 3)
    } else {
        i64::from(month + 9)
    };
    let doy = (153 * m_adj + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let pg_days_i64 = era * 146_097 + doe - 730_425;
    let pg_days = i32::try_from(pg_days_i64).map_err(|_| {
        PostgresError::Data(format!(
            "calendar date ({year_astro}, {month}, {day}) overflows PostgreSQL i32 days"
        ))
    })?;
    if !(PG_DATE_MIN_FINITE..=PG_DATE_MAX_FINITE).contains(&pg_days) {
        return Err(PostgresError::Data(format!(
            "calendar date ({year_astro}, {month}, {day}) -> {pg_days} is outside PostgreSQL finite date bounds"
        )));
    }
    Ok(pg_days)
}

/// Convert finite PostgreSQL days since `2000-01-01` into astronomical `(year_astro, month, day)`.
pub const fn pg_days_to_ymd(days: i32) -> (i32, u8, u8) {
    let z = (days as i64) + 730_425;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year_astro = (y + if m <= 2 { 1 } else { 0 }) as i32;
    (year_astro, m as u8, d as u8)
}

/// Validate a PostgreSQL `DATE` (`i32` days since `2000-01-01` or `±infinity`).
pub fn validate_date_days(days: i32) -> Result<i32, PostgresError> {
    if days == PG_DATE_NEG_INF
        || days == PG_DATE_POS_INF
        || (PG_DATE_MIN_FINITE..=PG_DATE_MAX_FINITE).contains(&days)
    {
        Ok(days)
    } else {
        Err(PostgresError::Data(format!(
            "PostgreSQL DATE value {days} is outside valid finite server range and not ±infinity"
        )))
    }
}

/// Validate a PostgreSQL `TIME` value (`0..=86_400_000_000` microseconds).
pub fn validate_time_micros(micros: i64) -> Result<i64, PostgresError> {
    if (0..=PG_TIME_MAX_MICROS).contains(&micros) {
        Ok(micros)
    } else {
        Err(PostgresError::Data(format!(
            "PostgreSQL TIME value {micros} is outside 0..={PG_TIME_MAX_MICROS}"
        )))
    }
}

/// Validate a PostgreSQL `TIMESTAMP` or `TIMESTAMPTZ` value (`i64` microseconds since `2000-01-01` or `±infinity`).
pub fn validate_timestamp_micros(micros: i64) -> Result<i64, PostgresError> {
    if micros == PG_TS_NEG_INF
        || micros == PG_TS_POS_INF
        || (PG_TS_MIN_FINITE..=PG_TS_MAX_FINITE).contains(&micros)
    {
        Ok(micros)
    } else {
        Err(PostgresError::Data(format!(
            "PostgreSQL TIMESTAMP value {micros} is outside valid finite server range and not ±infinity"
        )))
    }
}

/// Validate a PostgreSQL `INTERVAL` `(months, days, microseconds)` triple, rejecting partial infinity sentinels.
pub fn validate_interval_parts(
    months: i32,
    days: i32,
    microseconds: i64,
) -> Result<(i32, i32, i64), PostgresError> {
    let tuple = (months, days, microseconds);
    if tuple == PG_INTERVAL_POS_INF || tuple == PG_INTERVAL_NEG_INF {
        return Ok(tuple);
    }
    let has_pos_extreme = months == i32::MAX || days == i32::MAX || microseconds == i64::MAX;
    let has_neg_extreme = months == i32::MIN || days == i32::MIN || microseconds == i64::MIN;
    if has_pos_extreme || has_neg_extreme {
        return Err(PostgresError::Data(format!(
            "PostgreSQL INTERVAL ({months}, {days}, {microseconds}) has partial infinity sentinel components"
        )));
    }
    Ok(tuple)
}

/// Decode a 4-byte big-endian PostgreSQL `DATE` (`OID 1082`).
pub fn decode_date_binary(raw: &[u8]) -> Result<i32, PostgresError> {
    let bytes: [u8; 4] = raw.try_into().map_err(|_| {
        PostgresError::Data(format!(
            "DATE binary payload must be 4 bytes, got {}",
            raw.len()
        ))
    })?;
    validate_date_days(i32::from_be_bytes(bytes))
}

/// Encode a 4-byte big-endian PostgreSQL `DATE` (`OID 1082`).
pub fn encode_date_binary(days: i32, out: &mut BytesMut) -> Result<(), PostgresError> {
    validate_date_days(days)?;
    out.put_i32(days);
    Ok(())
}

/// Decode an 8-byte big-endian PostgreSQL `TIME` (`OID 1083`).
pub fn decode_time_binary(raw: &[u8]) -> Result<i64, PostgresError> {
    let bytes: [u8; 8] = raw.try_into().map_err(|_| {
        PostgresError::Data(format!(
            "TIME binary payload must be 8 bytes, got {}",
            raw.len()
        ))
    })?;
    validate_time_micros(i64::from_be_bytes(bytes))
}

/// Encode an 8-byte big-endian PostgreSQL `TIME` (`OID 1083`).
pub fn encode_time_binary(micros: i64, out: &mut BytesMut) -> Result<(), PostgresError> {
    validate_time_micros(micros)?;
    out.put_i64(micros);
    Ok(())
}

/// Decode an 8-byte big-endian PostgreSQL `TIMESTAMP` (`OID 1114`) or `TIMESTAMPTZ` (`OID 1184`).
pub fn decode_timestamp_binary(raw: &[u8]) -> Result<i64, PostgresError> {
    let bytes: [u8; 8] = raw.try_into().map_err(|_| {
        PostgresError::Data(format!(
            "TIMESTAMP binary payload must be 8 bytes, got {}",
            raw.len()
        ))
    })?;
    validate_timestamp_micros(i64::from_be_bytes(bytes))
}

/// Encode an 8-byte big-endian PostgreSQL `TIMESTAMP` (`OID 1114`) or `TIMESTAMPTZ` (`OID 1184`).
pub fn encode_timestamp_binary(micros: i64, out: &mut BytesMut) -> Result<(), PostgresError> {
    validate_timestamp_micros(micros)?;
    out.put_i64(micros);
    Ok(())
}

/// Decode a 16-byte big-endian PostgreSQL `INTERVAL` (`[i64 microseconds][i32 days][i32 months]`).
pub fn decode_interval_binary(raw: &[u8]) -> Result<(i32, i32, i64), PostgresError> {
    let bytes: [u8; 16] = raw.try_into().map_err(|_| {
        PostgresError::Data(format!(
            "INTERVAL binary payload must be 16 bytes, got {}",
            raw.len()
        ))
    })?;
    let microseconds = i64::from_be_bytes(bytes[0..8].try_into().expect("8B slice"));
    let days = i32::from_be_bytes(bytes[8..12].try_into().expect("4B slice"));
    let months = i32::from_be_bytes(bytes[12..16].try_into().expect("4B slice"));
    validate_interval_parts(months, days, microseconds)
}

/// Encode a 16-byte big-endian PostgreSQL `INTERVAL` (`[i64 microseconds][i32 days][i32 months]`).
pub fn encode_interval_binary(
    months: i32,
    days: i32,
    microseconds: i64,
    out: &mut BytesMut,
) -> Result<(), PostgresError> {
    validate_interval_parts(months, days, microseconds)?;
    out.reserve(16);
    out.put_i64(microseconds);
    out.put_i32(days);
    out.put_i32(months);
    Ok(())
}
