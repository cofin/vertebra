//! Single-pass ASCII and UTF-8 text case conversion, slugification, SQL identifier
//! quoting/splitting/validation, UTF-8 byte boundary truncation, and humanize formatting.

use std::fmt::Write;

/// Convert a string (`camelCase`, `PascalCase`, `kebab-case`, spaces, or acronyms) to `snake_case`.
pub fn snake_case(value: &str) -> String {
    let words = split_identifier_words(value);
    if words.is_empty() {
        return String::new();
    }
    let cap = value.len() + words.len();
    let mut out = String::with_capacity(cap);
    for (idx, word) in words.iter().enumerate() {
        if idx > 0 {
            out.push('_');
        }
        for ch in word.chars() {
            for lower in ch.to_lowercase() {
                out.push(lower);
            }
        }
    }
    out
}

/// Convert a string to `camelCase`.
pub fn camelize(value: &str) -> String {
    let words = split_identifier_words(value);
    if words.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(value.len());
    for (idx, word) in words.iter().enumerate() {
        let mut chars = word.chars();
        if let Some(first) = chars.next() {
            if idx == 0 {
                for lower in first.to_lowercase() {
                    out.push(lower);
                }
            } else {
                for upper in first.to_uppercase() {
                    out.push(upper);
                }
            }
            for ch in chars {
                for lower in ch.to_lowercase() {
                    out.push(lower);
                }
            }
        }
    }
    out
}

/// Convert a string to `PascalCase`.
pub fn pascalize(value: &str) -> String {
    let words = split_identifier_words(value);
    if words.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(value.len());
    for word in &words {
        let mut chars = word.chars();
        if let Some(first) = chars.next() {
            for upper in first.to_uppercase() {
                out.push(upper);
            }
            for ch in chars {
                for lower in ch.to_lowercase() {
                    out.push(lower);
                }
            }
        }
    }
    out
}

/// Convert a string to `kebab-case`.
pub fn kebabize(value: &str) -> String {
    let words = split_identifier_words(value);
    if words.is_empty() {
        return String::new();
    }
    let cap = value.len() + words.len();
    let mut out = String::with_capacity(cap);
    for (idx, word) in words.iter().enumerate() {
        if idx > 0 {
            out.push('-');
        }
        for ch in word.chars() {
            for lower in ch.to_lowercase() {
                out.push(lower);
            }
        }
    }
    out
}

/// Convert text into a URL- and filename-safe ASCII slug using `separator` (typically `"-"` or `"_"`).
pub fn slugify(value: &str, separator: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut pending_sep = false;

    for ch in value.chars() {
        if ('\u{0300}'..='\u{036F}').contains(&ch) {
            continue;
        }
        if ch.is_ascii_alphanumeric() {
            if pending_sep && !out.is_empty() {
                out.push_str(separator);
            }
            pending_sep = false;
            out.push(ch.to_ascii_lowercase());
            continue;
        }
        let folded = fold_latin_char(ch);
        if !folded.is_empty() {
            if pending_sep && !out.is_empty() {
                out.push_str(separator);
            }
            pending_sep = false;
            out.push_str(folded);
        } else if !out.is_empty() {
            pending_sep = true;
        }
    }

    out
}

/// Quote a single SQL identifier with double quotes (`"ident"`), escaping internal `"` as `""`.
pub fn quote_identifier(identifier: &str) -> String {
    let clean = identifier.trim();
    if clean.len() >= 2 && clean.starts_with('"') && clean.ends_with('"') {
        return clean.to_string();
    }
    let escaped = clean.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// Quote a single SQL identifier with backticks (`` `ident` ``), escaping internal `` ` `` as `` `` ``.
pub fn quote_backtick_identifier(identifier: &str) -> String {
    let clean = identifier.trim();
    if clean.len() >= 2 && clean.starts_with('`') && clean.ends_with('`') {
        return clean.to_string();
    }
    let escaped = clean.replace('`', "``");
    format!("`{escaped}`")
}

/// Quote a single SQL Server / T-SQL identifier with brackets (`[ident]`), escaping internal `]` as `]]`.
pub fn quote_bracket_identifier(identifier: &str) -> String {
    let clean = identifier.trim();
    if clean.len() >= 2 && clean.starts_with('[') && clean.ends_with(']') {
        return clean.to_string();
    }
    let escaped = clean.replace(']', "]]");
    format!("[{escaped}]")
}

/// Split a potentially qualified SQL identifier (`schema.table.col`) while respecting `"..."`, `` `...` ``, and `[...]`.
pub fn split_qualified_identifier(
    identifier: &str,
    quote_chars: &str,
    allow_bracket_quotes: bool,
) -> Vec<String> {
    let text = identifier.trim();
    if text.is_empty() {
        return Vec::new();
    }

    let chars: Vec<char> = text.chars().collect();
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut quote_char: Option<char> = None;
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];
        if let Some(active_quote) = quote_char {
            if ch == active_quote {
                if i + 1 < chars.len() && chars[i + 1] == active_quote {
                    current.push(active_quote);
                    i += 2;
                    continue;
                }
                quote_char = None;
                i += 1;
                continue;
            }
            current.push(ch);
            i += 1;
            continue;
        }

        if quote_chars.contains(ch) {
            quote_char = Some(ch);
            i += 1;
            continue;
        }

        if allow_bracket_quotes && ch == '[' {
            quote_char = Some(']');
            i += 1;
            continue;
        }

        if ch == '.' {
            let candidate = current.trim();
            if !candidate.is_empty() {
                parts.push(candidate.to_string());
            }
            current.clear();
            i += 1;
            continue;
        }

        current.push(ch);
        i += 1;
    }

    let tail = current.trim();
    if !tail.is_empty() {
        parts.push(tail.to_string());
    }
    parts
}

/// Quote a single or dot-qualified SQL identifier using the target dialect's quoting style.
pub fn quote_dialect_identifier(identifier: &str, dialect: &str) -> String {
    let parts = split_qualified_identifier(identifier, "\"`", true);
    if parts.is_empty() {
        return quote_part_for_dialect(identifier.trim(), dialect);
    }
    parts
        .iter()
        .map(|part| quote_part_for_dialect(part, dialect))
        .collect::<Vec<_>>()
        .join(".")
}

/// Validate and normalize a single or dot-qualified SQL identifier, optionally quoting per dialect.
pub fn normalize_identifier(identifier: &str, dialect: Option<&str>) -> Result<String, String> {
    let clean = identifier.trim();
    if clean.is_empty() {
        return Err("Identifier cannot be empty".to_string());
    }

    let raw_parts = split_qualified_with_quote_flag(clean);
    if raw_parts.is_empty() {
        return Err(format!("Invalid identifier: {identifier}"));
    }

    let mut normalized_parts = Vec::with_capacity(raw_parts.len());
    for (part, was_quoted) in raw_parts {
        if part.is_empty() {
            return Err(format!("Invalid identifier: {identifier}"));
        }
        if !was_quoted && !is_valid_identifier_segment(&part, false) {
            return Err(format!("Invalid identifier part: {part}"));
        }
        normalized_parts.push(part);
    }

    match dialect.map(str::trim).filter(|d| !d.is_empty()) {
        None => Ok(normalized_parts.join(".")),
        Some(target_dialect) => Ok(normalized_parts
            .iter()
            .map(|part| quote_part_for_dialect(part, target_dialect))
            .collect::<Vec<_>>()
            .join(".")),
    }
}

/// Validate a SQL, PL/SQL, channel, or storage identifier against injection characters.
pub fn validate_identifier(
    identifier: &str,
    field_name: &str,
    allow_dots: bool,
    allow_hyphens: bool,
) -> Result<String, String> {
    if identifier.is_empty() {
        return Err(format!(
            "Invalid SQL identifier for {field_name}: {identifier:?}"
        ));
    }

    let valid = if allow_dots {
        identifier
            .split('.')
            .all(|segment| is_valid_identifier_segment(segment, allow_hyphens))
    } else {
        is_valid_identifier_segment(identifier, allow_hyphens)
    };

    if !valid {
        return Err(format!(
            "Invalid SQL identifier for {field_name}: {identifier:?}"
        ));
    }
    Ok(identifier.to_string())
}

/// Truncate `value` to at most `max_bytes` UTF-8 bytes without splitting any multi-byte codepoint.
#[inline]
pub fn truncate_utf8_bytes(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let boundary = value.floor_char_boundary(max_bytes);
    &value[..boundary]
}

/// Truncate `value` to at most `max_chars` Unicode scalar values, appending `suffix` when truncated.
pub fn truncate(value: &str, max_chars: usize, suffix: &str) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let total_chars = value.chars().count();
    if total_chars <= max_chars {
        return value.to_string();
    }
    let suffix_chars = suffix.chars().count();
    if suffix_chars >= max_chars {
        return suffix.chars().take(max_chars).collect();
    }
    let keep = max_chars - suffix_chars;
    let mut out: String = value.chars().take(keep).collect();
    out.push_str(suffix);
    out
}

/// Convert an identifier (`user_profile_id`, `getUserById`, `mcp-tool`) into a human-readable phrase.
pub fn humanize(value: &str, capitalize: bool) -> String {
    let mut words = split_identifier_words(value);
    if words.len() > 1
        && let Some(last) = words.last()
        && (last.eq_ignore_ascii_case("id") || last.eq_ignore_ascii_case("ids"))
    {
        words.pop();
    }
    if words.is_empty() {
        return String::new();
    }

    let mut out = String::with_capacity(value.len());
    for (idx, word) in words.iter().enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        let mut chars = word.chars();
        if let Some(first) = chars.next() {
            if idx == 0 && capitalize {
                for upper in first.to_uppercase() {
                    out.push(upper);
                }
            } else {
                for lower in first.to_lowercase() {
                    out.push(lower);
                }
            }
            for ch in chars {
                for lower in ch.to_lowercase() {
                    out.push(lower);
                }
            }
        }
    }
    out
}

/// Convert an identifier or phrase into `Title Case` (`"get_user_by_id"` -> `"Get User By Id"`).
pub fn titleize(value: &str) -> String {
    let words = split_identifier_words(value);
    if words.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(value.len());
    for (idx, word) in words.iter().enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        let mut chars = word.chars();
        if let Some(first) = chars.next() {
            for upper in first.to_uppercase() {
                out.push(upper);
            }
            for ch in chars {
                for lower in ch.to_lowercase() {
                    out.push(lower);
                }
            }
        }
    }
    out
}

/// Inflect an English noun to its plural (or singular when `count == Some(1)` / `Some(-1)`).
pub fn pluralize(word: &str, count: Option<i64>, include_count: bool) -> String {
    let trimmed = word.trim();
    if trimmed.is_empty() {
        return match (count, include_count) {
            (Some(n), true) => n.to_string(),
            _ => String::new(),
        };
    }

    let inflected = match count {
        Some(1 | -1) => singularize(trimmed),
        _ => pluralize_word(trimmed),
    };

    match (count, include_count) {
        (Some(n), true) => format!("{n} {inflected}"),
        _ => inflected,
    }
}

/// Inflect an English plural noun to its singular form.
pub fn singularize(word: &str) -> String {
    let trimmed = word.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let lower = trimmed.to_ascii_lowercase();
    if is_uncountable(&lower) {
        return trimmed.to_string();
    }

    for &(singular, plural) in IRREGULAR_NOUNS {
        if lower == plural {
            return match_casing(trimmed, singular);
        }
        if lower == singular {
            return trimmed.to_string();
        }
    }

    if lower.ends_with("analyses")
        || lower.ends_with("bases")
        || lower.ends_with("diagnoses")
        || lower.ends_with("parentheses")
        || lower.ends_with("synopses")
        || lower.ends_with("theses")
        || lower.ends_with("crises")
    {
        let stem = &trimmed[..trimmed.len() - 2];
        return format!("{stem}is");
    }

    if lower.ends_with("vertices") || lower.ends_with("matrices") || lower.ends_with("indices") {
        let stem = &trimmed[..trimmed.len() - 4];
        return format!("{stem}ex");
    }

    if lower.ends_with("ies") && trimmed.len() > 3 {
        let stem = &trimmed[..trimmed.len() - 3];
        return format!("{stem}y");
    }

    if (lower.ends_with("knives") || lower.ends_with("wives") || lower.ends_with("lives"))
        && trimmed.len() > 3
    {
        let stem = &trimmed[..trimmed.len() - 3];
        return format!("{stem}fe");
    }

    if (lower.ends_with("leaves")
        || lower.ends_with("halves")
        || lower.ends_with("wolves")
        || lower.ends_with("shelves")
        || lower.ends_with("elves")
        || lower.ends_with("loaves")
        || lower.ends_with("thieves"))
        && trimmed.len() > 3
    {
        let stem = &trimmed[..trimmed.len() - 3];
        return format!("{stem}f");
    }

    if lower.ends_with("statuses")
        || lower.ends_with("buses")
        || lower.ends_with("aliases")
        || lower.ends_with("campuses")
        || lower.ends_with("bonuses")
        || lower.ends_with("viruses")
        || lower.ends_with("ches")
        || lower.ends_with("shes")
        || lower.ends_with("xes")
        || lower.ends_with("zes")
        || lower.ends_with("sses")
    {
        return trimmed[..trimmed.len() - 2].to_string();
    }

    if lower.ends_with('s') && !lower.ends_with("ss") && !lower.ends_with("us") && trimmed.len() > 1
    {
        return trimmed[..trimmed.len() - 1].to_string();
    }

    trimmed.to_string()
}

/// Format an integer as an English ordinal string (`1 -> "1st"`, `2 -> "2nd"`, `11 -> "11th"`).
pub fn ordinalize(value: i64) -> String {
    let rem100 = value.unsigned_abs() % 100;
    let suffix = if (11..=13).contains(&rem100) {
        "th"
    } else {
        match rem100 % 10 {
            1 => "st",
            2 => "nd",
            3 => "rd",
            _ => "th",
        }
    };
    format!("{value}{suffix}")
}

/// Format a byte count using IEC binary (`KiB`, `MiB`, `GiB`) or SI decimal (`KB`, `MB`, `GB`) units.
pub fn humanize_bytes(num_bytes: u64, binary: bool, precision: usize) -> String {
    let base: f64 = if binary { 1024.0 } else { 1000.0 };
    let base_u64: u64 = if binary { 1024 } else { 1000 };
    if num_bytes < base_u64 {
        return format!("{num_bytes} B");
    }
    let units: [&str; 6] = if binary {
        ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    } else {
        ["B", "KB", "MB", "GB", "TB", "PB"]
    };

    let mut value = num_bytes as f64;
    let mut unit_idx = 0;
    while value >= base && unit_idx + 1 < units.len() {
        value /= base;
        unit_idx += 1;
    }
    format!("{value:.precision$} {}", units[unit_idx])
}

/// Parse a human-formatted byte size string (`"64MiB"`, `"64 MB"`, `"512K"`, `"1024"`) into bytes.
pub fn parse_bytes(value: &str) -> Result<u64, String> {
    let clean = value.trim();
    if clean.is_empty() {
        return Err("Byte size string cannot be empty".to_string());
    }

    let split_idx = clean
        .find(|ch: char| !ch.is_ascii_digit() && ch != '.' && ch != '_' && ch != '+' && ch != '-')
        .unwrap_or(clean.len());

    let num_part = clean[..split_idx].trim().replace('_', "");
    let unit_part = clean[split_idx..].trim().to_ascii_lowercase();

    let number: f64 = num_part
        .parse()
        .map_err(|_| format!("Invalid byte size number in {value:?}"))?;
    if !number.is_finite() || number < 0.0 {
        return Err(format!("Byte size must be non-negative in {value:?}"));
    }

    let multiplier: f64 = match unit_part.as_str() {
        "" | "b" | "byte" | "bytes" => 1.0,
        "k" | "kib" => 1024.0,
        "m" | "mib" => 1024.0_f64.powi(2),
        "g" | "gib" => 1024.0_f64.powi(3),
        "t" | "tib" => 1024.0_f64.powi(4),
        "p" | "pib" => 1024.0_f64.powi(5),
        "kb" => 1_000.0,
        "mb" => 1_000.0_f64.powi(2),
        "gb" => 1_000.0_f64.powi(3),
        "tb" => 1_000.0_f64.powi(4),
        "pb" => 1_000.0_f64.powi(5),
        _ => return Err(format!("Unsupported byte unit {unit_part:?} in {value:?}")),
    };

    let product = (number * multiplier).round();
    if product > (u64::MAX as f64) {
        return Err(format!("Byte size overflow in {value:?}"));
    }
    Ok(product as u64)
}

/// Format a duration in seconds into human-readable units (`ns`, `µs`, `ms`, `s`, `m`, `h`, `d`).
pub fn humanize_duration(seconds: f64, precision: usize) -> String {
    if !seconds.is_finite() {
        return "0 ms".to_string();
    }
    let sign = if seconds < 0.0 { "-" } else { "" };
    let abs = seconds.abs();
    if abs == 0.0 {
        return "0 ms".to_string();
    }
    if abs < 1e-6 {
        return format!("{sign}{:.precision$} ns", abs * 1e9);
    }
    if abs < 1e-3 {
        return format!("{sign}{:.precision$} µs", abs * 1e6);
    }
    if abs < 1.0 {
        return format!("{sign}{:.precision$} ms", abs * 1e3);
    }
    if abs < 60.0 {
        return format!("{sign}{abs:.precision$} s");
    }

    let total_secs = abs.round() as u64;
    let days = total_secs / 86_400;
    let hours = (total_secs % 86_400) / 3_600;
    let minutes = (total_secs % 3_600) / 60;
    let secs = total_secs % 60;

    let mut parts = Vec::with_capacity(4);
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 {
        parts.push(format!("{minutes}m"));
    }
    if secs > 0 || parts.is_empty() {
        parts.push(format!("{secs}s"));
    }
    format!("{sign}{}", parts.join(" "))
}

/// Parse a single or compound duration string (`"500ms"`, `"1.5s"`, `"1m 30s"`, `"100us"`) into seconds.
pub fn parse_duration(value: &str) -> Result<f64, String> {
    let clean = value.trim();
    if clean.is_empty() {
        return Err("Duration string cannot be empty".to_string());
    }

    if let Ok(plain_seconds) = clean.parse::<f64>() {
        if !plain_seconds.is_finite() || plain_seconds < 0.0 {
            return Err(format!("Duration must be non-negative in {value:?}"));
        }
        return Ok(plain_seconds);
    }

    let clean = clean.strip_prefix('+').map_or(clean, str::trim);
    let chars: Vec<char> = clean.chars().collect();
    let mut i = 0;
    let mut total_seconds = 0.0_f64;
    let mut parsed_any = false;

    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }

        let num_start = i;
        while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.' || chars[i] == '_') {
            i += 1;
        }
        if i == num_start {
            return Err(format!("Expected duration number in {value:?}"));
        }
        let num_str: String = chars[num_start..i]
            .iter()
            .copied()
            .filter(|&c| c != '_')
            .collect();
        let amount: f64 = num_str
            .parse()
            .map_err(|_| format!("Invalid duration number {num_str:?} in {value:?}"))?;

        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        let unit_start = i;
        while i < chars.len() && chars[i].is_alphabetic() {
            i += 1;
        }
        let unit: String = chars[unit_start..i].iter().collect();
        let factor = match unit.to_lowercase().as_str() {
            "ns" | "nanosecond" | "nanoseconds" => 1e-9,
            "us" | "µs" | "μs" | "microsecond" | "microseconds" => 1e-6,
            "ms" | "millisecond" | "milliseconds" => 1e-3,
            "" | "s" | "sec" | "secs" | "second" | "seconds" => 1.0,
            "m" | "min" | "mins" | "minute" | "minutes" => 60.0,
            "h" | "hr" | "hrs" | "hour" | "hours" => 3_600.0,
            "d" | "day" | "days" => 86_400.0,
            _ => return Err(format!("Unsupported duration unit {unit:?} in {value:?}")),
        };

        total_seconds += amount * factor;
        parsed_any = true;
    }

    if !parsed_any {
        return Err(format!("Invalid duration {value:?}"));
    }
    Ok(total_seconds)
}

/// Format a number with compact magnitude suffixes (`K`, `M`, `B`, `T`).
pub fn humanize_number(value: f64, precision: usize) -> String {
    if !value.is_finite() {
        return "0".to_string();
    }
    let abs = value.abs();
    let sign = if value < 0.0 { "-" } else { "" };
    if abs >= 1e12 {
        return format!("{sign}{:.precision$}T", abs / 1e12);
    }
    if abs >= 1e9 {
        return format!("{sign}{:.precision$}B", abs / 1e9);
    }
    if abs >= 1e6 {
        return format!("{sign}{:.precision$}M", abs / 1e6);
    }
    if abs >= 1e3 {
        return format!("{sign}{:.precision$}K", abs / 1e3);
    }
    if (abs - abs.round()).abs() < 1e-9 {
        return format!("{sign}{}", abs.round() as i64);
    }
    format!("{sign}{abs:.precision$}")
}

/// Format a throughput rate (`count / seconds`) with compact magnitude suffix (`"123.8K req/s"`).
pub fn humanize_rate(count: f64, seconds: f64, unit: &str, precision: usize) -> String {
    let rate = if seconds > 0.0 && count.is_finite() && seconds.is_finite() {
        count / seconds
    } else {
        0.0
    };
    let formatted = humanize_number(rate, precision);
    let mut out = String::with_capacity(formatted.len() + unit.len() + 3);
    let _ = write!(out, "{formatted} {unit}/s");
    out
}

const IRREGULAR_NOUNS: &[(&str, &str)] = &[
    ("person", "people"),
    ("child", "children"),
    ("man", "men"),
    ("woman", "women"),
    ("mouse", "mice"),
    ("goose", "geese"),
    ("tooth", "teeth"),
    ("foot", "feet"),
    ("criterion", "criteria"),
    ("phenomenon", "phenomena"),
    ("index", "indices"),
    ("matrix", "matrices"),
    ("vertex", "vertices"),
    ("quiz", "quizzes"),
];

const UNCOUNTABLE_NOUNS: &[&str] = &[
    "data",
    "metadata",
    "series",
    "species",
    "equipment",
    "information",
    "news",
    "traffic",
    "feedback",
    "hardware",
    "software",
    "health",
];

fn is_uncountable(lower: &str) -> bool {
    UNCOUNTABLE_NOUNS.contains(&lower)
}

fn pluralize_word(word: &str) -> String {
    let lower = word.to_ascii_lowercase();
    if is_uncountable(&lower) {
        return word.to_string();
    }

    for &(singular, plural) in IRREGULAR_NOUNS {
        if lower == singular || lower == plural {
            return match_casing(word, plural);
        }
    }

    if lower.ends_with("analysis")
        || lower.ends_with("basis")
        || lower.ends_with("diagnosis")
        || lower.ends_with("parenthesis")
        || lower.ends_with("synopsis")
        || lower.ends_with("thesis")
        || lower.ends_with("crisis")
    {
        let stem = &word[..word.len() - 2];
        return format!("{stem}es");
    }

    if (lower.ends_with("knife") || lower.ends_with("wife") || lower.ends_with("life"))
        && word.len() > 2
    {
        let stem = &word[..word.len() - 2];
        return format!("{stem}ves");
    }

    if (lower.ends_with("leaf")
        || lower.ends_with("half")
        || lower.ends_with("wolf")
        || lower.ends_with("shelf")
        || lower.ends_with("elf")
        || lower.ends_with("loaf")
        || lower.ends_with("thief"))
        && !word.is_empty()
    {
        let stem = &word[..word.len() - 1];
        return format!("{stem}ves");
    }

    if lower.ends_with('y') && word.len() > 1 {
        let prev = lower.as_bytes()[lower.len() - 2];
        if !matches!(prev, b'a' | b'e' | b'i' | b'o' | b'u') {
            let stem = &word[..word.len() - 1];
            return format!("{stem}ies");
        }
    }

    if lower.ends_with('s')
        || lower.ends_with("sh")
        || lower.ends_with("ch")
        || lower.ends_with('x')
        || lower.ends_with('z')
    {
        return format!("{word}es");
    }

    format!("{word}s")
}

fn match_casing(template: &str, target_lower: &str) -> String {
    if template
        .chars()
        .all(|c| !c.is_alphabetic() || c.is_uppercase())
    {
        return target_lower.to_uppercase();
    }
    if let Some(first) = template.chars().next()
        && first.is_uppercase()
    {
        let mut chars = target_lower.chars();
        if let Some(t_first) = chars.next() {
            let mut out = String::with_capacity(target_lower.len());
            for upper in t_first.to_uppercase() {
                out.push(upper);
            }
            out.push_str(chars.as_str());
            return out;
        }
    }
    target_lower.to_string()
}

fn split_identifier_words(value: &str) -> Vec<String> {
    let chars: Vec<char> = value.chars().collect();
    let mut words: Vec<String> = Vec::new();
    let mut current = String::new();

    for i in 0..chars.len() {
        let ch = chars[i];
        if ch == '_' || ch == '-' || ch.is_whitespace() || ch == '.' || ch == '/' {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            continue;
        }

        if ch.is_uppercase() && !current.is_empty() {
            let prev = chars[i - 1];
            let next_is_lower = i + 1 < chars.len() && chars[i + 1].is_lowercase();
            if prev.is_lowercase()
                || prev.is_ascii_digit()
                || (prev.is_uppercase() && next_is_lower)
            {
                words.push(std::mem::take(&mut current));
            }
        }

        current.push(ch);
    }

    if !current.is_empty() {
        words.push(current);
    }
    words
}

fn quote_part_for_dialect(part: &str, dialect: &str) -> String {
    match dialect.trim().to_ascii_lowercase().as_str() {
        "mysql" | "mariadb" | "bigquery" | "googlesql" | "spanner" => {
            quote_backtick_identifier(part)
        }
        "mssql" | "tsql" | "sqlserver" => quote_bracket_identifier(part),
        _ => quote_identifier(part),
    }
}

fn split_qualified_with_quote_flag(identifier: &str) -> Vec<(String, bool)> {
    let chars: Vec<char> = identifier.chars().collect();
    let mut parts: Vec<(String, bool)> = Vec::new();
    let mut current = String::new();
    let mut quote_char: Option<char> = None;
    let mut segment_was_quoted = false;
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];
        if let Some(active_quote) = quote_char {
            if ch == active_quote {
                if i + 1 < chars.len() && chars[i + 1] == active_quote {
                    current.push(active_quote);
                    i += 2;
                    continue;
                }
                quote_char = None;
                i += 1;
                continue;
            }
            current.push(ch);
            i += 1;
            continue;
        }

        if ch == '"' || ch == '`' {
            quote_char = Some(ch);
            segment_was_quoted = true;
            i += 1;
            continue;
        }
        if ch == '[' {
            quote_char = Some(']');
            segment_was_quoted = true;
            i += 1;
            continue;
        }
        if ch == '.' {
            parts.push((current.trim().to_string(), segment_was_quoted));
            current.clear();
            segment_was_quoted = false;
            i += 1;
            continue;
        }

        current.push(ch);
        i += 1;
    }

    if quote_char.is_some() {
        return Vec::new();
    }
    parts.push((current.trim().to_string(), segment_was_quoted));
    parts
}

fn is_valid_identifier_segment(segment: &str, allow_hyphens: bool) -> bool {
    let bytes = segment.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let first = bytes[0];
    if !first.is_ascii_alphabetic() && first != b'_' {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || b == b'_' || (allow_hyphens && b == b'-'))
}

fn fold_latin_char(ch: char) -> &'static str {
    match ch {
        'À' | 'Á' | 'Â' | 'Ã' | 'Ä' | 'Å' | 'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'Ā' | 'ā' | 'Ă'
        | 'ă' | 'Ą' | 'ą' => "a",
        'Æ' | 'æ' => "ae",
        'Ç' | 'ç' | 'Ć' | 'ć' | 'Č' | 'č' => "c",
        'Ď' | 'ď' | 'Đ' | 'đ' | 'Ð' | 'ð' => "d",
        'È' | 'É' | 'Ê' | 'Ë' | 'è' | 'é' | 'ê' | 'ë' | 'Ē' | 'ē' | 'Ė' | 'ė' | 'Ę' | 'ę' | 'Ě'
        | 'ě' => "e",
        'Ğ' | 'ğ' | 'Ģ' | 'ģ' => "g",
        'Ì' | 'Í' | 'Î' | 'Ï' | 'ì' | 'í' | 'î' | 'ï' | 'Ī' | 'ī' | 'Į' | 'į' | 'İ' | 'ı' => {
            "i"
        }
        'Ķ' | 'ķ' => "k",
        'Ĺ' | 'ĺ' | 'Ļ' | 'ļ' | 'Ľ' | 'ľ' | 'Ł' | 'ł' => "l",
        'Ñ' | 'ñ' | 'Ń' | 'ń' | 'Ņ' | 'ņ' | 'Ň' | 'ň' => "n",
        'Ò' | 'Ó' | 'Ô' | 'Õ' | 'Ö' | 'Ø' | 'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'Ō' | 'ō' | 'Ő'
        | 'ő' => "o",
        'Œ' | 'œ' => "oe",
        'Ŕ' | 'ŕ' | 'Ř' | 'ř' => "r",
        'Ś' | 'ś' | 'Š' | 'š' | 'Ş' | 'ş' | 'Ș' | 'ș' => "s",
        'ß' => "ss",
        'Ť' | 'ť' | 'Ţ' | 'ţ' | 'Ț' | 'ț' | 'Þ' | 'þ' => "t",
        'Ù' | 'Ú' | 'Û' | 'Ü' | 'ù' | 'ú' | 'û' | 'ü' | 'Ū' | 'ū' | 'Ů' | 'ů' | 'Ű' | 'ű' | 'Ų'
        | 'ų' => "u",
        'Ý' | 'ý' | 'ÿ' | 'Ÿ' => "y",
        'Ź' | 'ź' | 'Ż' | 'ż' | 'Ž' | 'ž' => "z",
        _ => "",
    }
}

fn parse_dollar_tag(bytes: &[u8], start: usize) -> Option<usize> {
    let len = bytes.len();
    let mut idx = start + 1;
    if idx < len && bytes[idx] == b'$' {
        return Some(2);
    }
    while idx < len {
        let b = bytes[idx];
        if b == b'$' {
            return (idx > start + 1).then_some(idx - start + 1);
        }
        if !(b.is_ascii_alphanumeric() || b == b'_') {
            return None;
        }
        if idx == start + 1 && b.is_ascii_digit() {
            return None;
        }
        idx += 1;
    }
    None
}

fn is_keyword_in(token: &str, list: &[&str]) -> bool {
    list.iter().any(|kw| token.eq_ignore_ascii_case(kw))
}

/// Return the first top-level SQL keyword token outside whitespace, comments, quotes, and bind parameters.
#[must_use]
pub fn first_sql_keyword(operation: &str) -> Option<&str> {
    classify_sql_scan(operation, false).3
}

/// Classify a SQL statement returning `(is_row_returning, is_insert, is_read_only)` without heap allocation.
#[must_use]
pub fn classify_sql_statement(operation: &str) -> (bool, bool, bool) {
    let (row_ret, is_ins, is_ro, _) = classify_sql_scan(operation, false);
    (row_ret, is_ins, is_ro)
}

/// Classify a T-SQL / MSSQL statement including lexical `OUTPUT` clauses without heap allocation.
#[must_use]
pub fn classify_mssql_statement(operation: &str) -> (bool, bool, bool) {
    let (row_ret, is_ins, is_ro, _) = classify_sql_scan(operation, true);
    (row_ret, is_ins, is_ro)
}

fn classify_sql_scan(
    operation: &str,
    detect_tsql_output: bool,
) -> (bool, bool, bool, Option<&str>) {
    const READ_ONLY: &[&str] = &[
        "SELECT", "WITH", "VALUES", "SHOW", "EXPLAIN", "PRAGMA", "DESCRIBE",
    ];
    const ROW_RETURNING: &[&str] = &[
        "SELECT", "WITH", "PRAGMA", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "VALUES",
    ];
    const NON_ROW_PREFIXES: &[&str] = &["INSERT", "UPDATE", "DELETE", "MERGE"];

    let bytes = operation.as_bytes();
    let len = bytes.len();
    let mut idx = 0usize;
    let mut first_keyword: Option<&str> = None;
    let mut is_insert = false;
    let mut is_read_only = false;

    while idx < len {
        let b = bytes[idx];
        if b.is_ascii_whitespace() {
            idx += 1;
            continue;
        }
        if b >= 0x80
            && let Some(ch) = operation[idx..].chars().next()
            && ch.is_whitespace()
        {
            idx += ch.len_utf8();
            continue;
        }
        if b == b'-' && idx + 1 < len && bytes[idx + 1] == b'-' {
            idx += 2;
            while idx < len && bytes[idx] != b'\n' {
                idx += 1;
            }
            if idx < len {
                idx += 1;
            }
            continue;
        }
        if b == b'/' && idx + 1 < len && bytes[idx + 1] == b'*' {
            let mut depth = 1usize;
            idx += 2;
            while idx < len && depth > 0 {
                if bytes[idx] == b'/' && idx + 1 < len && bytes[idx + 1] == b'*' {
                    depth += 1;
                    idx += 2;
                } else if bytes[idx] == b'*' && idx + 1 < len && bytes[idx + 1] == b'/' {
                    depth -= 1;
                    idx += 2;
                } else {
                    idx += 1;
                }
            }
            continue;
        }
        if b == b'$'
            && let Some(tag_len) = parse_dollar_tag(bytes, idx)
        {
            let tag = &bytes[idx..idx + tag_len];
            idx += tag_len;
            if let Some(pos) = bytes[idx..].windows(tag_len).position(|w| w == tag) {
                idx += pos + tag_len;
            } else {
                idx = len;
            }
            continue;
        }
        if matches!(b, b'\'' | b'"' | b'`') {
            let quote = b;
            idx += 1;
            while idx < len {
                let cur = bytes[idx];
                if cur == quote {
                    if idx + 1 < len && bytes[idx + 1] == quote {
                        idx += 2;
                        continue;
                    }
                    idx += 1;
                    break;
                }
                if cur == b'\\' && quote == b'\'' {
                    idx = (idx + 2).min(len);
                    continue;
                }
                idx += 1;
            }
            continue;
        }
        if first_keyword.is_none() && (b == b'(' || b == b';') {
            idx += 1;
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' {
            let start = idx;
            idx += 1;
            while idx < len && (bytes[idx].is_ascii_alphanumeric() || bytes[idx] == b'_') {
                idx += 1;
            }
            if start > 0 && matches!(bytes[start - 1], b':' | b'@' | b'$') {
                continue;
            }
            let token = &operation[start..idx];
            let mut prev_sig_idx = start;
            while prev_sig_idx > 0 && bytes[prev_sig_idx - 1].is_ascii_whitespace() {
                prev_sig_idx -= 1;
            }
            let is_qualified = prev_sig_idx > 0 && bytes[prev_sig_idx - 1] == b'.';
            let mut next_sig_idx = idx;
            while next_sig_idx < len && bytes[next_sig_idx].is_ascii_whitespace() {
                next_sig_idx += 1;
            }
            let is_assignment_target = next_sig_idx < len && bytes[next_sig_idx] == b'=';
            if first_keyword.is_none() {
                first_keyword = Some(token);
                is_insert = token.eq_ignore_ascii_case("INSERT");
                is_read_only = is_keyword_in(token, READ_ONLY);
                if is_keyword_in(token, ROW_RETURNING) {
                    return (true, is_insert, is_read_only, first_keyword);
                }
                let might_have_clause = bytes
                    .windows(9)
                    .any(|w| w.eq_ignore_ascii_case(b"returning"))
                    || (detect_tsql_output
                        && bytes.windows(6).any(|w| w.eq_ignore_ascii_case(b"output")));
                if is_keyword_in(token, NON_ROW_PREFIXES) && !might_have_clause {
                    return (false, is_insert, is_read_only, first_keyword);
                }
            } else if !is_qualified
                && !is_assignment_target
                && (token.eq_ignore_ascii_case("RETURNING")
                    || (detect_tsql_output && token.eq_ignore_ascii_case("OUTPUT")))
            {
                return (true, is_insert, is_read_only, first_keyword);
            }
            continue;
        }
        idx += if b < 0x80 {
            1
        } else {
            operation[idx..].chars().next().map_or(1, char::len_utf8)
        };
    }

    (false, is_insert, is_read_only, first_keyword)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_conversions_handle_camel_pascal_kebab_and_acronyms() {
        assert_eq!(snake_case("HTTPResponseCode"), "http_response_code");
        assert_eq!(snake_case("getUserById"), "get_user_by_id");
        assert_eq!(snake_case("user-profile-id"), "user_profile_id");
        assert_eq!(camelize("user_profile_id"), "userProfileId");
        assert_eq!(camelize("UserProfileId"), "userProfileId");
        assert_eq!(pascalize("user_profile_id"), "UserProfileId");
        assert_eq!(pascalize("get-user-by-id"), "GetUserById");
        assert_eq!(kebabize("UserProfileId"), "user-profile-id");
        assert_eq!(kebabize("HTTPResponseCode"), "http-response-code");
    }

    #[test]
    fn slugify_folds_diacritics_and_normalizes_separators() {
        assert_eq!(slugify("Café Résumé 2026!", "-"), "cafe-resume-2026");
        assert_eq!(slugify("  /api/v1/users_list/ ", "_"), "api_v1_users_list");
        assert_eq!(slugify("Über Straße", "-"), "uber-strasse");
    }

    #[test]
    fn sql_identifier_quoting_splitting_and_validation() {
        assert_eq!(quote_identifier("user\"name"), "\"user\"\"name\"");
        assert_eq!(quote_backtick_identifier("my`table"), "`my``table`");
        assert_eq!(quote_bracket_identifier("my]table"), "[my]]table]");
        assert_eq!(
            split_qualified_identifier("\"my.schema\".\"my\"\"table\".col", "\"`", true),
            vec!["my.schema", "my\"table", "col"]
        );
        assert_eq!(
            quote_dialect_identifier("proj.dataset.tbl", "bigquery"),
            "`proj`.`dataset`.`tbl`"
        );
        assert_eq!(
            quote_dialect_identifier("dbo.users", "mssql"),
            "[dbo].[users]"
        );
        assert_eq!(
            normalize_identifier("schema.users", Some("postgres")).unwrap(),
            "\"schema\".\"users\""
        );
        assert!(normalize_identifier("users; DROP TABLE", None).is_err());
        assert_eq!(
            validate_identifier("public.users_v1", "table", true, false).unwrap(),
            "public.users_v1"
        );
        assert!(validate_identifier("public.users_v1", "table", false, false).is_err());
        assert!(validate_identifier("users; DROP", "table", true, false).is_err());
    }

    #[test]
    fn utf8_and_char_truncation_respect_boundaries() {
        let multi = "client_🔥_id";
        assert_eq!(truncate_utf8_bytes(multi, 7), "client_");
        assert_eq!(truncate_utf8_bytes(multi, 8), "client_");
        assert_eq!(truncate_utf8_bytes(multi, 10), "client_");
        assert_eq!(truncate_utf8_bytes(multi, 11), "client_🔥");
        assert_eq!(truncate("hello world", 8, "..."), "hello...");
        assert_eq!(truncate("hello", 8, "..."), "hello");
    }

    #[test]
    fn humanize_inflection_and_unit_formatting() {
        assert_eq!(humanize("user_profile_id", true), "User profile");
        assert_eq!(humanize("getUserById", false), "get user by");
        assert_eq!(titleize("get_user_by_id"), "Get User By Id");

        assert_eq!(pluralize("worker", Some(1), true), "1 worker");
        assert_eq!(pluralize("worker", Some(4), true), "4 workers");
        assert_eq!(pluralize("Category", None, false), "Categories");
        assert_eq!(pluralize("status", None, false), "statuses");
        assert_eq!(pluralize("person", None, false), "people");
        assert_eq!(pluralize("matrix", None, false), "matrices");
        assert_eq!(pluralize("metadata", None, false), "metadata");

        assert_eq!(singularize("Categories"), "Category");
        assert_eq!(singularize("statuses"), "status");
        assert_eq!(singularize("people"), "person");
        assert_eq!(singularize("matrices"), "matrix");
        assert_eq!(singularize("analyses"), "analysis");

        assert_eq!(ordinalize(1), "1st");
        assert_eq!(ordinalize(2), "2nd");
        assert_eq!(ordinalize(3), "3rd");
        assert_eq!(ordinalize(11), "11th");
        assert_eq!(ordinalize(21), "21st");

        assert_eq!(humanize_bytes(0, true, 1), "0 B");
        assert_eq!(humanize_bytes(512, true, 1), "512 B");
        assert_eq!(humanize_bytes(1024, true, 1), "1.0 KiB");
        assert_eq!(humanize_bytes(67_108_864, true, 1), "64.0 MiB");
        assert_eq!(humanize_bytes(1_000_000, false, 1), "1.0 MB");

        assert_eq!(parse_bytes("64MiB").unwrap(), 67_108_864);
        assert_eq!(parse_bytes("1 MB").unwrap(), 1_000_000);
        assert_eq!(parse_bytes("4096").unwrap(), 4096);

        assert_eq!(humanize_duration(0.000_044_4, 1), "44.4 µs");
        assert_eq!(humanize_duration(0.0125, 1), "12.5 ms");
        assert_eq!(humanize_duration(65.0, 1), "1m 5s");
        assert_eq!(parse_duration("500ms").unwrap(), 0.5);
        assert_eq!(parse_duration("1m 30s").unwrap(), 90.0);

        assert_eq!(humanize_number(123_790.0, 1), "123.8K");
        assert_eq!(humanize_rate(123_790.0, 1.0, "req", 1), "123.8K req/s");
    }

    #[test]
    fn sql_statement_classification_handles_comments_quotes_and_returning() {
        assert_eq!(classify_sql_statement("SELECT 1"), (true, false, true));
        assert_eq!(
            classify_sql_statement("/* lead /* nested */ */ (INSERT INTO t VALUES (1))"),
            (false, true, false)
        );
        assert_eq!(
            classify_sql_statement("-- comment\nINSERT INTO t (a) VALUES (?) RETURNING id"),
            (true, true, false)
        );
        assert_eq!(
            classify_sql_statement("UPDATE t SET returning = 'RETURNING' WHERE t.returning = 1"),
            (false, false, false)
        );
        assert_eq!(
            classify_sql_statement("UPDATE t SET v = $tag$RETURNING$tag$ WHERE id = 1"),
            (false, false, false)
        );
        assert_eq!(
            classify_sql_statement("DELETE FROM t WHERE id = :returning RETURNING id"),
            (true, false, false)
        );
        assert_eq!(
            classify_sql_statement("PRAGMA journal_mode"),
            (true, false, true)
        );
        assert_eq!(first_sql_keyword("/* hint */ SELECT 1"), Some("SELECT"));
        assert_eq!(
            classify_mssql_statement("INSERT INTO t OUTPUT INSERTED.id VALUES (1)"),
            (true, true, false)
        );
        assert_eq!(
            classify_mssql_statement("UPDATE t SET output = ' OUTPUT ' WHERE t.output = 1"),
            (false, false, false)
        );
    }
}
