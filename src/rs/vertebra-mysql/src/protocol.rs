//! MySQL 8.4/9 and MariaDB wire framing, Handshake v10, `caching_sha2_password` /
//! `mysql_native_password` authentication scrambles, RSA OAEP encryption, and SQL placeholder
//! normalization.

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rsa::pkcs8::DecodePublicKey;
use rsa::{Oaep, RsaPrivateKey, RsaPublicKey};
use sha1::Sha1;
use sha2::{Digest, Sha256};

use crate::errors::MySQLError;

pub const CAP_LONG_PASSWORD: u32 = 0x0000_0001;
pub const CAP_FOUND_ROWS: u32 = 0x0000_0002;
pub const CAP_LONG_FLAG: u32 = 0x0000_0004;
pub const CAP_CONNECT_WITH_DB: u32 = 0x0000_0008;
pub const CAP_PROTOCOL_41: u32 = 0x0000_0200;
pub const CAP_SSL: u32 = 0x0000_0800;
pub const CAP_TRANSACTIONS: u32 = 0x0000_2000;
pub const CAP_SECURE_CONNECTION: u32 = 0x0000_8000;
pub const CAP_MULTI_STATEMENTS: u32 = 0x0001_0000;
pub const CAP_MULTI_RESULTS: u32 = 0x0002_0000;
pub const CAP_PS_MULTI_RESULTS: u32 = 0x0004_0000;
pub const CAP_PLUGIN_AUTH: u32 = 0x0008_0000;
pub const CAP_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;
pub const CAP_DEPRECATE_EOF: u32 = 0x0100_0000;

/// Encodes a MySQL 4-byte packet header (`24-bit LE payload_len` + `1-byte sequence_id`) and payload.
pub fn encode_mysql_packet(sequence_id: u8, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut out = Vec::with_capacity(4 + len);
    out.push((len & 0xFF) as u8);
    out.push(((len >> 8) & 0xFF) as u8);
    out.push(((len >> 16) & 0xFF) as u8);
    out.push(sequence_id);
    out.extend_from_slice(payload);
    out
}

/// Decodes a MySQL packet from `buf`, returning `(sequence_id, payload, total_frame_len)`.
pub fn decode_mysql_packet(buf: &[u8]) -> Option<(u8, &[u8], usize)> {
    if buf.len() < 4 {
        return None;
    }
    let payload_len = (buf[0] as usize) | ((buf[1] as usize) << 8) | ((buf[2] as usize) << 16);
    let total = 4 + payload_len;
    if buf.len() < total {
        return None;
    }
    Some((buf[3], &buf[4..total], total))
}

/// Encodes a MySQL 4.1+ OK packet payload (`0x00` header + lenenc `affected_rows` + lenenc `last_insert_id` + `status_flags` + `warnings`).
pub fn encode_ok_packet(
    affected_rows: u64,
    last_insert_id: u64,
    status_flags: u16,
    warnings: u16,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.push(0x00);
    crate::codecs::encode_lenenc_int(affected_rows, &mut out);
    crate::codecs::encode_lenenc_int(last_insert_id, &mut out);
    out.extend_from_slice(&status_flags.to_le_bytes());
    out.extend_from_slice(&warnings.to_le_bytes());
    out
}

/// Encodes a MySQL 4.1+ EOF packet payload (`0xFE` + `warnings` + `status_flags`).
pub fn encode_eof_packet(warnings: u16, status_flags: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(0xFE);
    out.extend_from_slice(&warnings.to_le_bytes());
    out.extend_from_slice(&status_flags.to_le_bytes());
    out
}

/// Encodes a MySQL 4.1+ ERR packet payload (`0xFF` + `errno` + `#` + 5-char `sqlstate` + `message`).
pub fn encode_err_packet(errno: u16, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + message.len());
    out.push(0xFF);
    out.extend_from_slice(&errno.to_le_bytes());
    out.push(b'#');
    let state_bytes = sqlstate.as_bytes();
    for i in 0..5 {
        out.push(*state_bytes.get(i).unwrap_or(&b'0'));
    }
    out.extend_from_slice(message.as_bytes());
    out
}

/// Parsed MySQL Handshake v10 greeting packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeV10 {
    pub protocol_version: u8,
    pub server_version: String,
    pub connection_id: u32,
    pub nonce: [u8; 20],
    pub capability_flags: u32,
    pub character_set: u8,
    pub status_flags: u16,
    pub auth_plugin_name: String,
}

impl HandshakeV10 {
    /// Encodes this Handshake v10 greeting into a raw packet payload (without the 4-byte header).
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(96);
        payload.push(self.protocol_version);
        payload.extend_from_slice(self.server_version.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&self.connection_id.to_le_bytes());
        payload.extend_from_slice(&self.nonce[..8]);
        payload.push(0);
        let cap_low = (self.capability_flags & 0xFFFF) as u16;
        let cap_high = ((self.capability_flags >> 16) & 0xFFFF) as u16;
        payload.extend_from_slice(&cap_low.to_le_bytes());
        payload.push(self.character_set);
        payload.extend_from_slice(&self.status_flags.to_le_bytes());
        payload.extend_from_slice(&cap_high.to_le_bytes());
        payload.push(21);
        payload.extend_from_slice(&[0u8; 10]);
        payload.extend_from_slice(&self.nonce[8..20]);
        payload.push(0);
        payload.extend_from_slice(self.auth_plugin_name.as_bytes());
        payload.push(0);
        payload
    }

    /// Parses a MySQL Handshake v10 greeting payload.
    pub fn parse_payload(payload: &[u8]) -> Result<Self, MySQLError> {
        if payload.is_empty() || payload[0] != 10 {
            return Err(MySQLError::connection(
                "Invalid or unsupported MySQL handshake protocol version",
            ));
        }
        let nul_pos = payload[1..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| MySQLError::connection("Truncated server version in HandshakeV10"))?;
        let server_version = String::from_utf8_lossy(&payload[1..1 + nul_pos]).into_owned();
        let mut pos = 1 + nul_pos + 1;
        if payload.len() < pos + 4 + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10 + 13 {
            return Err(MySQLError::connection("Truncated HandshakeV10 packet"));
        }
        let connection_id = u32::from_le_bytes(
            payload[pos..pos + 4]
                .try_into()
                .expect("4-byte connection id"),
        );
        pos += 4;
        let mut nonce = [0u8; 20];
        nonce[..8].copy_from_slice(&payload[pos..pos + 8]);
        pos += 8;
        pos += 1;
        let cap_low = u16::from_le_bytes(payload[pos..pos + 2].try_into().expect("cap_low"));
        pos += 2;
        let character_set = payload[pos];
        pos += 1;
        let status_flags =
            u16::from_le_bytes(payload[pos..pos + 2].try_into().expect("status_flags"));
        pos += 2;
        let cap_high = u16::from_le_bytes(payload[pos..pos + 2].try_into().expect("cap_high"));
        pos += 2;
        let capability_flags = (cap_low as u32) | ((cap_high as u32) << 16);
        let _auth_plugin_data_len = payload[pos];
        pos += 1;
        pos += 10;
        nonce[8..20].copy_from_slice(&payload[pos..pos + 12]);
        pos += 13;
        let plugin_end = payload[pos..]
            .iter()
            .position(|&b| b == 0)
            .map(|idx| pos + idx)
            .unwrap_or(payload.len());
        let auth_plugin_name = String::from_utf8_lossy(&payload[pos..plugin_end]).into_owned();

        Ok(Self {
            protocol_version: 10,
            server_version,
            connection_id,
            nonce,
            capability_flags,
            character_set,
            status_flags,
            auth_plugin_name,
        })
    }
}

/// Computes the 32-byte MySQL `caching_sha2_password` fast-authentication scramble:
/// `XOR(SHA256(password), SHA256(SHA256(SHA256(password)) || nonce[..20]))`.
pub fn compute_caching_sha2_scramble_bytes(
    password: &[u8],
    nonce: &[u8],
) -> Result<[u8; 32], MySQLError> {
    if nonce.len() < 20 {
        return Err(MySQLError::programming(
            "caching_sha2_password nonce must be at least 20 bytes",
        ));
    }
    let digest1 = Sha256::digest(password);
    let digest2 = Sha256::digest(digest1);
    let mut hasher = Sha256::new();
    hasher.update(digest2);
    hasher.update(&nonce[..20]);
    let digest3 = hasher.finalize();

    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = digest1[i] ^ digest3[i];
    }
    Ok(out)
}

/// Verifies a client's 32-byte `caching_sha2_password` scramble against a known password and 20-byte nonce.
pub fn verify_caching_sha2_scramble(scramble: &[u8], password: &[u8], nonce: &[u8]) -> bool {
    if scramble.len() != 32 || nonce.len() < 20 {
        return false;
    }
    match compute_caching_sha2_scramble_bytes(password, nonce) {
        Ok(expected) => expected == scramble,
        Err(_) => false,
    }
}

/// Computes the 20-byte `mysql_native_password` scramble:
/// `XOR(SHA1(password), SHA1(nonce[..20] || SHA1(SHA1(password))))`.
pub fn compute_mysql_native_scramble_bytes(
    password: &[u8],
    nonce: &[u8],
) -> Result<[u8; 20], MySQLError> {
    if nonce.len() < 20 {
        return Err(MySQLError::programming(
            "mysql_native_password nonce must be at least 20 bytes",
        ));
    }
    let stage1 = Sha1::digest(password);
    let stage2 = Sha1::digest(stage1);
    let mut hasher = Sha1::new();
    hasher.update(&nonce[..20]);
    hasher.update(stage2);
    let stage3 = hasher.finalize();

    let mut out = [0u8; 20];
    for i in 0..20 {
        out[i] = stage1[i] ^ stage3[i];
    }
    Ok(out)
}

/// Verifies a client's 20-byte `mysql_native_password` scramble against a known password and 20-byte nonce.
pub fn verify_mysql_native_scramble(scramble: &[u8], password: &[u8], nonce: &[u8]) -> bool {
    if scramble.len() != 20 || nonce.len() < 20 {
        return false;
    }
    match compute_mysql_native_scramble_bytes(password, nonce) {
        Ok(expected) => expected == scramble,
        Err(_) => false,
    }
}

/// Encrypts a password for the `caching_sha2_password` full-authentication RSA exchange (`0x02`):
/// appends a trailing NUL byte to `password`, XORs with `nonce[..20]` cyclically, and encrypts
/// with RSA-OAEP-SHA1 using the server's PEM public key.
pub fn encrypt_caching_sha2_rsa(
    password: &[u8],
    nonce: &[u8],
    public_key_pem: &str,
) -> Result<Vec<u8>, MySQLError> {
    if nonce.is_empty() {
        return Err(MySQLError::programming(
            "RSA scramble nonce must not be empty",
        ));
    }
    let nonce_slice = if nonce.len() >= 20 {
        &nonce[..20]
    } else {
        nonce
    };
    let mut plain = Vec::with_capacity(password.len() + 1);
    plain.extend_from_slice(password);
    plain.push(0);
    for (idx, byte) in plain.iter_mut().enumerate() {
        *byte ^= nonce_slice[idx % nonce_slice.len()];
    }

    let pub_key = RsaPublicKey::from_public_key_pem(public_key_pem.trim()).map_err(|e| {
        MySQLError::connection(format!("Invalid MySQL server RSA public key PEM: {e}"))
    })?;
    let mut rng = rsa::rand_core::OsRng;
    pub_key
        .encrypt(&mut rng, Oaep::new::<Sha1>(), &plain)
        .map_err(|e| MySQLError::connection(format!("RSA-OAEP-SHA1 encryption failed: {e}")))
}

/// Decrypts a `caching_sha2_password` RSA-OAEP-SHA1 ciphertext and strips the cyclic nonce XOR
/// and trailing NUL byte.
pub fn decrypt_caching_sha2_rsa(
    ciphertext: &[u8],
    nonce: &[u8],
    private_key: &RsaPrivateKey,
) -> Result<Vec<u8>, MySQLError> {
    let nonce_slice = if nonce.len() >= 20 {
        &nonce[..20]
    } else {
        nonce
    };
    let mut decrypted = private_key
        .decrypt(Oaep::new::<Sha1>(), ciphertext)
        .map_err(|e| MySQLError::connection(format!("RSA-OAEP-SHA1 decryption failed: {e}")))?;
    for (idx, byte) in decrypted.iter_mut().enumerate() {
        *byte ^= nonce_slice[idx % nonce_slice.len()];
    }
    if let Some(nul_pos) = decrypted.iter().position(|&b| b == 0) {
        decrypted.truncate(nul_pos);
    }
    Ok(decrypted)
}

/// Python-exposed helper for computing the 32-byte `caching_sha2_password` scramble.
#[pyfunction]
pub fn compute_caching_sha2_scramble<'py>(
    py: Python<'py>,
    password: &[u8],
    nonce: &[u8],
) -> PyResult<Bound<'py, PyBytes>> {
    let out = compute_caching_sha2_scramble_bytes(password, nonce)?;
    Ok(PyBytes::new(py, &out))
}

/// Python-exposed helper for computing the 20-byte `mysql_native_password` scramble.
#[pyfunction]
pub fn compute_mysql_native_scramble<'py>(
    py: Python<'py>,
    password: &[u8],
    nonce: &[u8],
) -> PyResult<Bound<'py, PyBytes>> {
    let out = compute_mysql_native_scramble_bytes(password, nonce)?;
    Ok(PyBytes::new(py, &out))
}

/// Python-exposed helper for encrypting a password with the server's RSA public key (`caching_sha2_password` full auth).
#[pyfunction]
pub fn encrypt_caching_sha2_rsa_password<'py>(
    py: Python<'py>,
    password: &[u8],
    nonce: &[u8],
    public_key_pem: &str,
) -> PyResult<Bound<'py, PyBytes>> {
    let encrypted = encrypt_caching_sha2_rsa(password, nonce, public_key_pem)?;
    Ok(PyBytes::new(py, &encrypted))
}

/// Placeholder style detected in a SQL query string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaceholderPlan {
    Positional,
    Named(Vec<String>),
}

/// Rewrites `%s`, `%(name)s`, and `:name` placeholders into MySQL positional `?` markers while
/// preserving string literals, backtick/double-quoted identifiers, `%%` escapes, `:=(...)`
/// assignment operators, and SQL comments.
pub fn normalize_mysql_placeholders(sql: &str) -> (String, PlaceholderPlan) {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut named_order: Vec<String> = Vec::new();
    let mut i = 0;

    while i < len {
        let ch = bytes[i];
        if ch == b'\'' || ch == b'"' || ch == b'`' {
            let quote = ch;
            out.push(quote as char);
            i += 1;
            while i < len {
                let c = bytes[i];
                if c == b'\\' && quote != b'`' && i + 1 < len {
                    let next_ch = sql[i + 1..].chars().next().unwrap_or('\0');
                    out.push('\\');
                    out.push(next_ch);
                    i += 1 + next_ch.len_utf8();
                    continue;
                }
                let ch_utf8 = sql[i..].chars().next().unwrap_or('\0');
                out.push(ch_utf8);
                i += ch_utf8.len_utf8();
                if c == quote {
                    if i < len && bytes[i] == quote {
                        out.push(quote as char);
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }

        if ch == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            while i < len && bytes[i] != b'\n' {
                let ch_utf8 = sql[i..].chars().next().unwrap_or('\0');
                out.push(ch_utf8);
                i += ch_utf8.len_utf8();
            }
            continue;
        }

        if ch == b'#' {
            while i < len && bytes[i] != b'\n' {
                let ch_utf8 = sql[i..].chars().next().unwrap_or('\0');
                out.push(ch_utf8);
                i += ch_utf8.len_utf8();
            }
            continue;
        }

        if ch == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            out.push_str("/*");
            i += 2;
            while i < len {
                if bytes[i] == b'*' && i + 1 < len && bytes[i + 1] == b'/' {
                    out.push_str("*/");
                    i += 2;
                    break;
                }
                let ch_utf8 = sql[i..].chars().next().unwrap_or('\0');
                out.push(ch_utf8);
                i += ch_utf8.len_utf8();
            }
            continue;
        }

        if ch == b'%' && i + 1 < len {
            if bytes[i + 1] == b'%' {
                out.push('%');
                i += 2;
                continue;
            }
            if bytes[i + 1] == b's' {
                out.push('?');
                i += 2;
                continue;
            }
            if bytes[i + 1] == b'('
                && let Some(end_paren) = sql[i + 2..].find(")s")
            {
                let name = &sql[i + 2..i + 2 + end_paren];
                if !name.is_empty() {
                    named_order.push(name.to_string());
                    out.push('?');
                    i += 2 + end_paren + 2;
                    continue;
                }
            }
        }

        if ch == b':' && i + 1 < len {
            let next = bytes[i + 1];
            if next == b':' || next == b'=' {
                out.push(':');
                out.push(next as char);
                i += 2;
                continue;
            }
            if next.is_ascii_alphabetic() || next == b'_' {
                let start = i + 1;
                let mut end = start;
                while end < len && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                    end += 1;
                }
                let name = &sql[start..end];
                named_order.push(name.to_string());
                out.push('?');
                i = end;
                continue;
            }
        }

        let ch_utf8 = sql[i..].chars().next().unwrap_or('\0');
        out.push(ch_utf8);
        i += ch_utf8.len_utf8();
    }

    let plan = if named_order.is_empty() {
        PlaceholderPlan::Positional
    } else {
        PlaceholderPlan::Named(named_order)
    };
    (out, plan)
}

/// Returns `true` if `sql` must be executed with the MySQL text protocol (`COM_QUERY`) rather
/// than `COM_STMT_PREPARE` (`0x16`).
pub fn is_text_only_statement(sql: &str) -> bool {
    let trimmed = strip_leading_sql_comments(sql).trim();
    if trimmed.is_empty() {
        return true;
    }
    let first_word = trimmed
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(
        first_word.as_str(),
        "BEGIN"
            | "START"
            | "COMMIT"
            | "ROLLBACK"
            | "SAVEPOINT"
            | "RELEASE"
            | "SET"
            | "USE"
            | "LOCK"
            | "UNLOCK"
            | "KILL"
            | "CREATE"
            | "ALTER"
            | "DROP"
            | "TRUNCATE"
            | "RENAME"
            | "GRANT"
            | "REVOKE"
    )
}

/// Splits a multi-statement SQL script on top-level `;` terminators outside string literals,
/// quoted identifiers, and comments.
pub fn split_sql_statements(script: &str) -> Vec<String> {
    let bytes = script.as_bytes();
    let len = bytes.len();
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut i = 0;

    while i < len {
        let ch = bytes[i];
        if ch == b'\'' || ch == b'"' || ch == b'`' {
            let quote = ch;
            current.push(quote as char);
            i += 1;
            while i < len {
                let c = bytes[i];
                if c == b'\\' && quote != b'`' && i + 1 < len {
                    let next_ch = script[i + 1..].chars().next().unwrap_or('\0');
                    current.push('\\');
                    current.push(next_ch);
                    i += 1 + next_ch.len_utf8();
                    continue;
                }
                let ch_utf8 = script[i..].chars().next().unwrap_or('\0');
                current.push(ch_utf8);
                i += ch_utf8.len_utf8();
                if c == quote {
                    if i < len && bytes[i] == quote {
                        current.push(quote as char);
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }

        if (ch == b'-' && i + 1 < len && bytes[i + 1] == b'-') || ch == b'#' {
            while i < len && bytes[i] != b'\n' {
                let ch_utf8 = script[i..].chars().next().unwrap_or('\0');
                current.push(ch_utf8);
                i += ch_utf8.len_utf8();
            }
            continue;
        }

        if ch == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            current.push_str("/*");
            i += 2;
            while i < len {
                if bytes[i] == b'*' && i + 1 < len && bytes[i + 1] == b'/' {
                    current.push_str("*/");
                    i += 2;
                    break;
                }
                let ch_utf8 = script[i..].chars().next().unwrap_or('\0');
                current.push(ch_utf8);
                i += ch_utf8.len_utf8();
            }
            continue;
        }

        if ch == b';' {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
            current.clear();
            i += 1;
            continue;
        }

        let ch_utf8 = script[i..].chars().next().unwrap_or('\0');
        current.push(ch_utf8);
        i += ch_utf8.len_utf8();
    }

    let tail = current.trim();
    if !tail.is_empty() {
        statements.push(tail.to_string());
    }
    statements
}

fn strip_leading_sql_comments(mut sql: &str) -> &str {
    loop {
        sql = sql.trim_start();
        if let Some(rest) = sql.strip_prefix("--") {
            sql = rest.split_once('\n').map_or("", |(_, tail)| tail);
        } else if let Some(rest) = sql.strip_prefix('#') {
            sql = rest.split_once('\n').map_or("", |(_, tail)| tail);
        } else if let Some(rest) = sql.strip_prefix("/*") {
            sql = rest.split_once("*/").map_or("", |(_, tail)| tail);
        } else {
            return sql;
        }
    }
}

#[cfg(test)]
mod tests {
    use rsa::pkcs8::{EncodePublicKey, LineEnding};

    use super::*;

    #[test]
    fn test_handshake_v10_roundtrip() {
        let hs = HandshakeV10 {
            protocol_version: 10,
            server_version: "8.4.3-Vertebra".to_string(),
            connection_id: 42,
            nonce: *b"0123456789abcdefghij",
            capability_flags: CAP_PROTOCOL_41
                | CAP_SECURE_CONNECTION
                | CAP_PLUGIN_AUTH
                | CAP_MULTI_RESULTS
                | CAP_DEPRECATE_EOF,
            character_set: 255,
            status_flags: 0x0002,
            auth_plugin_name: "caching_sha2_password".to_string(),
        };
        let encoded = encode_mysql_packet(0, &hs.encode_payload());
        let (seq, payload, total) = decode_mysql_packet(&encoded).unwrap();
        assert_eq!(seq, 0);
        assert_eq!(total, encoded.len());
        let parsed = HandshakeV10::parse_payload(payload).unwrap();
        assert_eq!(parsed, hs);
    }

    #[test]
    fn test_caching_sha2_and_native_scrambles_and_rsa_roundtrip() {
        let nonce = b"12345678901234567890";
        let password = b"correct-horse-battery-staple";

        let sha2_scramble = compute_caching_sha2_scramble_bytes(password, nonce).unwrap();
        assert_eq!(sha2_scramble.len(), 32);
        assert!(verify_caching_sha2_scramble(
            &sha2_scramble,
            password,
            nonce
        ));
        assert!(!verify_caching_sha2_scramble(
            &sha2_scramble,
            b"wrong",
            nonce
        ));

        let native_scramble = compute_mysql_native_scramble_bytes(password, nonce).unwrap();
        assert_eq!(native_scramble.len(), 20);
        assert!(verify_mysql_native_scramble(
            &native_scramble,
            password,
            nonce
        ));
        assert!(!verify_mysql_native_scramble(
            &native_scramble,
            b"wrong",
            nonce
        ));

        let mut rng = rsa::rand_core::OsRng;
        let priv_key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pub_pem = RsaPublicKey::from(&priv_key)
            .to_public_key_pem(LineEnding::LF)
            .unwrap();
        let cipher = encrypt_caching_sha2_rsa(password, nonce, &pub_pem).unwrap();
        let decrypted = decrypt_caching_sha2_rsa(&cipher, nonce, &priv_key).unwrap();
        assert_eq!(decrypted, password);
    }

    #[test]
    fn test_placeholder_normalization_preserves_quotes_and_comments() {
        let (norm, plan) = normalize_mysql_placeholders(
            "SELECT ':literal', /* :comment */ :id, %(name)s, %s FROM `tbl:1` WHERE a := 1 -- :tail",
        );
        assert_eq!(
            norm,
            "SELECT ':literal', /* :comment */ ?, ?, ? FROM `tbl:1` WHERE a := 1 -- :tail"
        );
        assert_eq!(
            plan,
            PlaceholderPlan::Named(vec!["id".to_string(), "name".to_string()])
        );
    }
}
