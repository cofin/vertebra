//! TNS Connect Descriptor resolution, in-memory Autonomous Database Wallet mTLS, and IAM token dialer.

use std::collections::HashMap;
use std::fmt;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use vertebra_core::gcp_auth::{DEFAULT_GCP_SCOPES, GcpAuthSource, GcpTokenCache};
use vertebra_db::oracle::tns::{OracleConnectDescriptor, parse_tnsnames_ora, resolve_tns_alias};

use crate::config::{DEFAULT_ORACLE_DSN, NativeOracleConfig, OracleAuthMode, OracleProtocol};
use crate::errors::OracleError;

/// In-memory Oracle Autonomous Database Cloud Wallet material (`tnsnames.ora`, `sqlnet.ora`, `ewallet.pem`, `cwallet.sso`).
#[derive(Clone, PartialEq, Eq)]
pub struct OracleWalletMaterial {
    pub tnsnames_ora: String,
    pub sqlnet_ora: String,
    pub ewallet_pem: Vec<u8>,
    pub cwallet_sso: Vec<u8>,
    pub cert_der_count: usize,
    pub has_private_key: bool,
    pub aliases: HashMap<String, OracleConnectDescriptor>,
}

impl fmt::Debug for OracleWalletMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OracleWalletMaterial")
            .field("tnsnames_ora_len", &self.tnsnames_ora.len())
            .field("sqlnet_ora_len", &self.sqlnet_ora.len())
            .field(
                "ewallet_pem",
                &if self.ewallet_pem.is_empty() {
                    None
                } else {
                    Some("<redacted>")
                },
            )
            .field(
                "cwallet_sso",
                &if self.cwallet_sso.is_empty() {
                    None
                } else {
                    Some("<redacted>")
                },
            )
            .field("cert_der_count", &self.cert_der_count)
            .field("has_private_key", &self.has_private_key)
            .field("aliases", &self.aliases.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn validate_pem_bundle(pem_bytes: &[u8]) -> Result<(usize, bool), OracleError> {
    if pem_bytes.is_empty() {
        return Ok((0, false));
    }
    let mut cert_reader = Cursor::new(pem_bytes);
    let mut cert_count = 0usize;
    for item in rustls_pemfile::certs(&mut cert_reader) {
        item.map_err(|_| {
            OracleError::Config("invalid X.509 certificate in Oracle ewallet.pem".to_owned())
        })?;
        cert_count += 1;
    }
    let mut key_reader = Cursor::new(pem_bytes);
    let has_key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|_| OracleError::Config("invalid private key in Oracle ewallet.pem".to_owned()))?
        .is_some();
    if cert_count == 0 && !has_key {
        return Err(OracleError::Config(
            "Oracle ewallet.pem did not contain any valid PEM certificates or private keys"
                .to_owned(),
        ));
    }
    Ok((cert_count, has_key))
}

fn build_alias_map(tnsnames_ora: &str) -> HashMap<String, OracleConnectDescriptor> {
    let raw_map = parse_tnsnames_ora(tnsnames_ora);
    let mut aliases = HashMap::with_capacity(raw_map.len());
    for (alias, desc_str) in raw_map {
        if let Some(desc) = OracleConnectDescriptor::from_descriptor_str(&desc_str) {
            aliases.insert(alias, desc);
        }
    }
    aliases
}

impl OracleWalletMaterial {
    /// Construct [`OracleWalletMaterial`] from raw in-memory buffers and validate PEM contents.
    pub fn from_raw_buffers(
        tnsnames_ora: Option<&str>,
        sqlnet_ora: Option<&str>,
        pem_data: Option<&[u8]>,
        sso_data: Option<&[u8]>,
    ) -> Result<Self, OracleError> {
        let tns_str = tnsnames_ora.unwrap_or("").to_owned();
        let sqlnet_str = sqlnet_ora.unwrap_or("").to_owned();
        let pem_vec = pem_data.unwrap_or(&[]).to_vec();
        let sso_vec = sso_data.unwrap_or(&[]).to_vec();
        let (cert_der_count, has_private_key) = validate_pem_bundle(&pem_vec)?;
        let aliases = build_alias_map(&tns_str);
        Ok(Self {
            tnsnames_ora: tns_str,
            sqlnet_ora: sqlnet_str,
            ewallet_pem: pem_vec,
            cwallet_sso: sso_vec,
            cert_der_count,
            has_private_key,
            aliases,
        })
    }

    /// Extract [`OracleWalletMaterial`] in memory from an Autonomous Database `.zip` archive without writing temporary files.
    pub fn from_zip_bytes(zip_bytes: &[u8]) -> Result<Self, OracleError> {
        let cursor = Cursor::new(zip_bytes);
        let mut archive = zip::ZipArchive::new(cursor)
            .map_err(|e| OracleError::Config(format!("invalid Oracle wallet .zip archive: {e}")))?;
        let mut tnsnames_ora = String::new();
        let mut sqlnet_ora = String::new();
        let mut ewallet_pem = Vec::new();
        let mut cwallet_sso = Vec::new();

        for idx in 0..archive.len() {
            let mut file = archive.by_index(idx).map_err(|e| {
                OracleError::Config(format!("failed to read Oracle wallet zip entry: {e}"))
            })?;
            let entry_name = Path::new(file.name())
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            match entry_name.as_str() {
                "tnsnames.ora" => {
                    let mut buf = Vec::new();
                    file.read_to_end(&mut buf).map_err(|e| {
                        OracleError::Config(format!("failed reading tnsnames.ora from zip: {e}"))
                    })?;
                    tnsnames_ora = String::from_utf8_lossy(&buf).into_owned();
                }
                "sqlnet.ora" => {
                    let mut buf = Vec::new();
                    file.read_to_end(&mut buf).map_err(|e| {
                        OracleError::Config(format!("failed reading sqlnet.ora from zip: {e}"))
                    })?;
                    sqlnet_ora = String::from_utf8_lossy(&buf).into_owned();
                }
                "ewallet.pem" => {
                    file.read_to_end(&mut ewallet_pem).map_err(|e| {
                        OracleError::Config(format!("failed reading ewallet.pem from zip: {e}"))
                    })?;
                }
                "cwallet.sso" => {
                    file.read_to_end(&mut cwallet_sso).map_err(|e| {
                        OracleError::Config(format!("failed reading cwallet.sso from zip: {e}"))
                    })?;
                }
                _ => {}
            }
        }

        Self::from_raw_buffers(
            Some(&tnsnames_ora),
            Some(&sqlnet_ora),
            (!ewallet_pem.is_empty()).then_some(ewallet_pem.as_slice()),
            (!cwallet_sso.is_empty()).then_some(cwallet_sso.as_slice()),
        )
    }

    /// Load [`OracleWalletMaterial`] from a `.zip` file path or an extracted wallet directory.
    pub fn from_location(path: &Path) -> Result<Self, OracleError> {
        if !path.exists() {
            return Err(OracleError::Config(format!(
                "Oracle wallet_location does not exist: {}",
                path.display()
            )));
        }
        if path.is_file() {
            let bytes = std::fs::read(path).map_err(|e| {
                OracleError::Config(format!(
                    "failed to read Oracle wallet file {}: {e}",
                    path.display()
                ))
            })?;
            return Self::from_zip_bytes(&bytes);
        }

        let tns_path = path.join("tnsnames.ora");
        let sqlnet_path = path.join("sqlnet.ora");
        let pem_path = path.join("ewallet.pem");
        let sso_path = path.join("cwallet.sso");

        let tnsnames_ora = if tns_path.is_file() {
            std::fs::read_to_string(&tns_path).map_err(|e| {
                OracleError::Config(format!("failed to read {}: {e}", tns_path.display()))
            })?
        } else {
            String::new()
        };
        let sqlnet_ora = if sqlnet_path.is_file() {
            std::fs::read_to_string(&sqlnet_path).map_err(|e| {
                OracleError::Config(format!("failed to read {}: {e}", sqlnet_path.display()))
            })?
        } else {
            String::new()
        };
        let ewallet_pem = if pem_path.is_file() {
            std::fs::read(&pem_path).map_err(|e| {
                OracleError::Config(format!("failed to read {}: {e}", pem_path.display()))
            })?
        } else {
            Vec::new()
        };
        let cwallet_sso = if sso_path.is_file() {
            std::fs::read(&sso_path).map_err(|e| {
                OracleError::Config(format!("failed to read {}: {e}", sso_path.display()))
            })?
        } else {
            Vec::new()
        };

        Self::from_raw_buffers(
            Some(&tnsnames_ora),
            Some(&sqlnet_ora),
            (!ewallet_pem.is_empty()).then_some(ewallet_pem.as_slice()),
            (!cwallet_sso.is_empty()).then_some(cwallet_sso.as_slice()),
        )
    }

    /// Resolve a case-insensitive TNS service alias from this wallet's `tnsnames.ora`.
    #[must_use]
    pub fn resolve_alias(&self, alias: &str) -> Option<&OracleConnectDescriptor> {
        let key = alias.trim().to_ascii_uppercase();
        self.aliases.get(&key)
    }
}

/// Resolved endpoint, TLS/wallet material, and authentication parameters ready to dial.
#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedOracleEndpoint {
    pub connect_string: String,
    pub protocol: OracleProtocol,
    pub ssl_server_dn_match: bool,
    pub ssl_server_cert_dn: Option<String>,
    pub wallet: Option<OracleWalletMaterial>,
    pub bearer_token: Option<String>,
}

impl fmt::Debug for ResolvedOracleEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedOracleEndpoint")
            .field("connect_string", &self.connect_string)
            .field("protocol", &self.protocol)
            .field("ssl_server_dn_match", &self.ssl_server_dn_match)
            .field("ssl_server_cert_dn", &self.ssl_server_cert_dn)
            .field("wallet", &self.wallet)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

fn normalize_easy_connect_uri(raw: &str) -> String {
    let trimmed = raw.trim();
    for prefix in ["oracle+vtb://", "oracle://", "tcps://", "tcp://"] {
        if let Some(rest) = trimmed
            .get(..prefix.len())
            .filter(|p| p.eq_ignore_ascii_case(prefix))
            .and_then(|_| trimmed.get(prefix.len()..))
        {
            let after_at = match rest.find('@') {
                Some(idx) => &rest[idx + 1..],
                None => rest,
            };
            let without_query = match after_at.find('?') {
                Some(idx) => &after_at[..idx],
                None => after_at,
            };
            if prefix.eq_ignore_ascii_case("tcps://") {
                return format!("tcps:{without_query}");
            }
            return without_query.to_owned();
        }
    }
    trimmed.to_owned()
}

fn looks_like_alias(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && !trimmed.starts_with('(')
        && !trimmed.contains('/')
        && !trimmed.contains(':')
}

fn lookup_alias_in_sources(
    alias: &str,
    wallet: Option<&OracleWalletMaterial>,
    tns_admin: Option<&str>,
) -> Result<Option<OracleConnectDescriptor>, OracleError> {
    if let Some(w) = wallet
        && let Some(desc) = w.resolve_alias(alias)
    {
        return Ok(Some(desc.clone()));
    }
    if let Some(dir) = tns_admin {
        let tns_path = Path::new(dir).join("tnsnames.ora");
        if tns_path.is_file() {
            let contents = std::fs::read_to_string(&tns_path).map_err(|e| {
                OracleError::Config(format!("failed to read {}: {e}", tns_path.display()))
            })?;
            if let Some(desc) = resolve_tns_alias(&contents, alias) {
                return Ok(Some(desc));
            }
        }
    }
    Ok(None)
}

fn resolve_bearer_token(cfg: &NativeOracleConfig) -> Result<Option<String>, OracleError> {
    if cfg.auth_mode == OracleAuthMode::Password {
        return Ok(None);
    }
    if let Some(tok) = cfg
        .access_token
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        return Ok(Some(tok.to_owned()));
    }
    if let Some(ref opts_btree) = cfg.token_auth_options {
        let opts_map: HashMap<String, String> = opts_btree
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if let Some(tok) = opts_map
            .get("access_token")
            .or_else(|| opts_map.get("bearer_token"))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            let source = GcpAuthSource::Static {
                access_token: tok.to_owned(),
                expires_at_unix_ms: None,
                quota_project_id: opts_map.get("quota_project_id").cloned(),
            };
            let cache = GcpTokenCache::new(source, 300_000);
            let _ = cache.generation();
            return Ok(Some(tok.to_owned()));
        }
        if let Some(cache) = GcpTokenCache::from_options(&opts_map, DEFAULT_GCP_SCOPES) {
            if let Some(snap) = cache.get_cached(0) {
                return Ok(Some(snap.access_token.clone()));
            }
            if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                && let Ok(snap) = rt.block_on(cache.get_or_refresh())
            {
                return Ok(Some(snap.access_token.clone()));
            }
        }
    }
    Err(OracleError::Config(
        "Oracle auth_mode='token' requires a non-empty access_token or valid token_auth_options"
            .to_owned(),
    ))
}

/// Resolve the effective Oracle connect string, wallet mTLS material, and IAM/OAuth2 bearer token.
pub fn resolve_endpoint(cfg: &NativeOracleConfig) -> Result<ResolvedOracleEndpoint, OracleError> {
    let wallet = if let Some(ref loc) = cfg.wallet_location {
        Some(OracleWalletMaterial::from_location(Path::new(loc))?)
    } else if cfg.pem_cert_data.is_some() || cfg.cwallet_sso_data.is_some() {
        Some(OracleWalletMaterial::from_raw_buffers(
            None,
            None,
            cfg.pem_cert_data.as_deref(),
            cfg.cwallet_sso_data.as_deref(),
        )?)
    } else {
        None
    };

    let bearer_token = resolve_bearer_token(cfg)?;

    let mut effective_protocol = cfg.protocol;
    let mut effective_cert_dn = cfg.ssl_server_cert_dn.clone();

    let has_non_default_endpoint = cfg.host != "localhost"
        || cfg.port != 1521
        || cfg.service_name != "FREEPDB1"
        || cfg.sid.is_some();

    let connect_string = if let Some(ref explicit_desc) = cfg.connect_descriptor {
        if looks_like_alias(explicit_desc) {
            let resolved =
                lookup_alias_in_sources(explicit_desc, wallet.as_ref(), cfg.tns_admin.as_deref())?
                    .ok_or_else(|| OracleError::Server {
                        code: 12154,
                        message: format!(
                            "ORA-12154: TNS:could not resolve the connect identifier specified: {explicit_desc}"
                        ),
                    })?;
            if resolved.security_cert_dn.is_some() || resolved.port == 1522 {
                effective_protocol = OracleProtocol::Tcps;
            }
            if effective_cert_dn.is_none() {
                effective_cert_dn = resolved.security_cert_dn.clone();
            }
            resolved.to_tns_descriptor()
        } else {
            normalize_easy_connect_uri(explicit_desc)
        }
    } else if let Some(ref dsn) = cfg.dsn
        && (dsn != DEFAULT_ORACLE_DSN || !has_non_default_endpoint)
    {
        if looks_like_alias(dsn) {
            let resolved = lookup_alias_in_sources(dsn, wallet.as_ref(), cfg.tns_admin.as_deref())?
                .ok_or_else(|| OracleError::Server {
                    code: 12154,
                    message: format!(
                        "ORA-12154: TNS:could not resolve the connect identifier specified: {dsn}"
                    ),
                })?;
            if resolved.security_cert_dn.is_some() || resolved.port == 1522 {
                effective_protocol = OracleProtocol::Tcps;
            }
            if effective_cert_dn.is_none() {
                effective_cert_dn = resolved.security_cert_dn.clone();
            }
            resolved.to_tns_descriptor()
        } else {
            if dsn.to_ascii_lowercase().starts_with("tcps://")
                || dsn.to_ascii_uppercase().contains("PROTOCOL=TCPS")
            {
                effective_protocol = OracleProtocol::Tcps;
            }
            normalize_easy_connect_uri(dsn)
        }
    } else if let Some(ref w) = wallet
        && let Some(resolved) = w.resolve_alias(&cfg.service_name)
    {
        if resolved.security_cert_dn.is_some() || resolved.port == 1522 {
            effective_protocol = OracleProtocol::Tcps;
        }
        if effective_cert_dn.is_none() {
            effective_cert_dn = resolved.security_cert_dn.clone();
        }
        resolved.to_tns_descriptor()
    } else if wallet.is_some()
        && cfg.dsn.is_none()
        && cfg.connect_descriptor.is_none()
        && !w_has_empty_aliases(wallet.as_ref())
    {
        return Err(OracleError::Server {
            code: 12154,
            message: format!(
                "ORA-12154: TNS:could not resolve the connect identifier specified: {}",
                cfg.service_name
            ),
        });
    } else {
        if wallet.is_some() || effective_cert_dn.is_some() || cfg.port == 1522 {
            effective_protocol = OracleProtocol::Tcps;
        }
        let desc = OracleConnectDescriptor {
            host: cfg.host.clone(),
            port: cfg.port,
            service_name: if cfg.sid.is_some() && cfg.service_name == "FREEPDB1" {
                None
            } else {
                Some(cfg.service_name.clone())
            },
            sid: cfg.sid.clone(),
            security_cert_dn: effective_cert_dn.clone(),
            retry_count: None,
            retry_delay: None,
            transport_connect_timeout: Some(cfg.connect_timeout.as_secs().max(1) as u32),
        };
        let mut rendered = desc.to_tns_descriptor();
        if effective_protocol == OracleProtocol::Tcps && rendered.contains("(PROTOCOL=TCP)") {
            rendered = rendered.replace("(PROTOCOL=TCP)", "(PROTOCOL=TCPS)");
        }
        rendered
    };

    Ok(ResolvedOracleEndpoint {
        connect_string,
        protocol: effective_protocol,
        ssl_server_dn_match: cfg.ssl_server_dn_match,
        ssl_server_cert_dn: effective_cert_dn,
        wallet,
        bearer_token,
    })
}

fn w_has_empty_aliases(wallet: Option<&OracleWalletMaterial>) -> bool {
    wallet.is_none_or(|w| w.aliases.is_empty())
}

const TNS_PACKET_ACCEPT: u8 = 2;
const TNS_PACKET_MARKER: u8 = 12;
const TNS_MARKER_RESET: u8 = 2;
const TNS_MARKER_INTERRUPT: u8 = 3;
const TNS_PROTOCOL_VERSION_32BIT: u16 = 315;
const TNS_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Physical `oracledb::Connection` paired with an optional upstream TCP handle for fallback break shutdown.
pub struct DialedOracleSession {
    pub conn: oracledb::Connection,
    pub upstream_socket: Option<Arc<TcpStream>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedTcpTarget {
    host: String,
    port: u16,
    is_descriptor: bool,
    host_val_range: (usize, usize),
    port_val_range: Option<(usize, usize)>,
    easy_suffix: String,
}

fn find_ascii_ci(haystack: &str, needle_upper: &str, from: usize) -> Option<usize> {
    let bytes = haystack.as_bytes();
    let nlen = needle_upper.len();
    if from + nlen > bytes.len() {
        return None;
    }
    bytes[from..]
        .windows(nlen)
        .position(|w| w.eq_ignore_ascii_case(needle_upper.as_bytes()))
        .map(|off| from + off)
}

fn parse_single_tcp_target(connect_string: &str) -> Option<ParsedTcpTarget> {
    let trimmed = connect_string.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('(') {
        if find_ascii_ci(trimmed, "PROTOCOL=TCPS", 0).is_some() {
            return None;
        }
        let first_host_kw = find_ascii_ci(trimmed, "HOST=", 0)?;
        let host_val_start = first_host_kw + "HOST=".len();
        let host_val_end = trimmed[host_val_start..]
            .find(')')
            .map(|i| host_val_start + i)?;
        if find_ascii_ci(trimmed, "HOST=", host_val_end).is_some() {
            return None;
        }
        let host = trimmed[host_val_start..host_val_end].trim().to_owned();
        if host.is_empty() {
            return None;
        }
        let port_kw = find_ascii_ci(trimmed, "PORT=", 0)?;
        let port_val_start = port_kw + "PORT=".len();
        let port_val_end = trimmed[port_val_start..]
            .find(')')
            .map(|i| port_val_start + i)?;
        if find_ascii_ci(trimmed, "PORT=", port_val_end).is_some() {
            return None;
        }
        let port: u16 = trimmed[port_val_start..port_val_end].trim().parse().ok()?;
        return Some(ParsedTcpTarget {
            host,
            port,
            is_descriptor: true,
            host_val_range: (host_val_start, host_val_end),
            port_val_range: Some((port_val_start, port_val_end)),
            easy_suffix: String::new(),
        });
    }

    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("tcps:") {
        return None;
    }
    let rest = if let Some(s) = trimmed.strip_prefix("//") {
        s
    } else if lower.starts_with("tcp://") {
        &trimmed[6..]
    } else if lower.starts_with("tcp:") {
        trimmed[4..].strip_prefix("//").unwrap_or(&trimmed[4..])
    } else {
        trimmed
    };

    let (authority, slash_suffix) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, ""),
    };

    if let Some(after_open) = authority.strip_prefix('[') {
        let close_idx = after_open.find(']')?;
        let host = after_open[..close_idx].to_owned();
        let after_bracket = &after_open[close_idx + 1..];
        let (port, extra) = if after_bracket.is_empty() {
            (1521u16, "")
        } else {
            let p_rest = after_bracket.strip_prefix(':')?;
            match p_rest.find(':') {
                Some(colon2) => (p_rest[..colon2].parse().ok()?, &p_rest[colon2..]),
                None => (p_rest.parse().ok()?, ""),
            }
        };
        return Some(ParsedTcpTarget {
            host,
            port,
            is_descriptor: false,
            host_val_range: (0, 0),
            port_val_range: None,
            easy_suffix: format!("{extra}{slash_suffix}"),
        });
    }

    let parts: Vec<&str> = authority.split(':').collect();
    let (host, port, extra) = match parts.as_slice() {
        [h] if !h.is_empty() => ((*h).to_owned(), 1521u16, String::new()),
        [h, p] if !h.is_empty() => ((*h).to_owned(), p.parse().ok()?, String::new()),
        [h, p, sid] if !h.is_empty() && slash_suffix.is_empty() => {
            ((*h).to_owned(), p.parse().ok()?, format!(":{sid}"))
        }
        _ => return None,
    };
    Some(ParsedTcpTarget {
        host,
        port,
        is_descriptor: false,
        host_val_range: (0, 0),
        port_val_range: None,
        easy_suffix: format!("{extra}{slash_suffix}"),
    })
}

fn rewrite_target_to_local_port(
    connect_string: &str,
    parsed: &ParsedTcpTarget,
    local_port: u16,
) -> String {
    if !parsed.is_descriptor {
        return format!("127.0.0.1:{local_port}{}", parsed.easy_suffix);
    }
    let trimmed = connect_string.trim();
    let mut edits = vec![(
        parsed.host_val_range.0,
        parsed.host_val_range.1,
        "127.0.0.1".to_owned(),
    )];
    if let Some((ps, pe)) = parsed.port_val_range {
        edits.push((ps, pe, local_port.to_string()));
    }
    edits.sort_by_key(|(start, _, _)| *start);
    let mut out = String::with_capacity(trimmed.len() + 16);
    let mut cursor = 0usize;
    for (start, end, rep) in edits {
        out.push_str(&trimmed[cursor..start]);
        out.push_str(&rep);
        cursor = end;
    }
    out.push_str(&trimmed[cursor..]);
    out
}

fn read_tns_packet(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
    full_flag: &AtomicBool,
) -> std::io::Result<usize> {
    let mut hdr = [0u8; 8];
    match stream.read_exact(&mut hdr) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(0),
        Err(e) => return Err(e),
    }
    let use_full = full_flag.load(Ordering::Acquire);
    let pkt_len = if use_full {
        u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize
    } else {
        u16::from_be_bytes([hdr[0], hdr[1]]) as usize
    };
    if !(8..=TNS_MAX_FRAME_BYTES).contains(&pkt_len) {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid Oracle TNS packet length {pkt_len}"),
        ));
    }
    if buf.len() < pkt_len {
        buf.resize(pkt_len, 0);
    }
    buf[..8].copy_from_slice(&hdr);
    if pkt_len > 8 {
        stream.read_exact(&mut buf[8..pkt_len])?;
    }
    Ok(pkt_len)
}

fn relay_tns_connection(csock: TcpStream, usock: TcpStream) {
    let _ = csock.set_nodelay(true);
    let _ = usock.set_nodelay(true);
    let _ = csock.set_read_timeout(None);
    let _ = usock.set_read_timeout(None);

    let Ok(mut csock_tx) = csock.try_clone() else {
        let _ = csock.shutdown(Shutdown::Both);
        let _ = usock.shutdown(Shutdown::Both);
        return;
    };
    let Ok(mut usock_tx) = usock.try_clone() else {
        let _ = csock.shutdown(Shutdown::Both);
        let _ = usock.shutdown(Shutdown::Both);
        return;
    };

    let full_flag = Arc::new(AtomicBool::new(false));
    let client_interrupt = Arc::new(AtomicBool::new(false));

    let full_tx = Arc::clone(&full_flag);
    let interrupt_tx = Arc::clone(&client_interrupt);
    let _ = thread::Builder::new()
        .name("vtb-ora-tns-tx".to_owned())
        .spawn(move || {
            let mut buf = vec![0u8; 8192];
            while let Ok(pkt_len) = read_tns_packet(&mut csock_tx, &mut buf, &full_tx) {
                if pkt_len == 0 {
                    break;
                }
                if full_tx.load(Ordering::Acquire)
                    && pkt_len == 11
                    && buf[4] == TNS_PACKET_MARKER
                    && buf[10] == TNS_MARKER_INTERRUPT
                {
                    interrupt_tx.store(true, Ordering::Release);
                }
                if usock_tx.write_all(&buf[..pkt_len]).is_err() {
                    break;
                }
            }
            let _ = usock_tx.shutdown(Shutdown::Both);
        });

    let mut csock_rx = csock;
    let mut usock_rx = usock;
    let mut buf = vec![0u8; 8192];
    let mut reset_active = false;
    while let Ok(pkt_len) = read_tns_packet(&mut usock_rx, &mut buf, &full_flag) {
        if pkt_len == 0 {
            break;
        }
        let ptype = buf[4];
        let mut is_full = full_flag.load(Ordering::Acquire);
        if !is_full && ptype == TNS_PACKET_ACCEPT && pkt_len >= 10 {
            let proto_ver = u16::from_be_bytes([buf[8], buf[9]]);
            if proto_ver >= TNS_PROTOCOL_VERSION_32BIT {
                full_flag.store(true, Ordering::Release);
                is_full = true;
            }
        }
        if is_full && ptype == TNS_PACKET_MARKER && pkt_len == 11 && buf[10] == TNS_MARKER_RESET {
            if client_interrupt.swap(false, Ordering::AcqRel) {
                if csock_rx.write_all(&buf[..pkt_len]).is_err() {
                    break;
                }
            } else if !reset_active {
                reset_active = true;
                if csock_rx.write_all(&buf[..pkt_len]).is_err()
                    || csock_rx.write_all(&buf[..pkt_len]).is_err()
                {
                    break;
                }
            } else if csock_rx.write_all(&buf[..pkt_len]).is_err() {
                break;
            }
            continue;
        }
        if ptype != TNS_PACKET_MARKER {
            reset_active = false;
        }
        if csock_rx.write_all(&buf[..pkt_len]).is_err() {
            break;
        }
    }
    let _ = csock_rx.shutdown(Shutdown::Both);
    let _ = usock_rx.shutdown(Shutdown::Both);
}

struct RelayAcceptGuard {
    local_port: u16,
    abort_flag: Arc<AtomicBool>,
    armed: bool,
}

impl Drop for RelayAcceptGuard {
    fn drop(&mut self) {
        if self.armed {
            self.abort_flag.store(true, Ordering::Release);
            let _ = TcpStream::connect(("127.0.0.1", self.local_port));
        }
    }
}

fn connect_tcp_with_timeout(
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<TcpStream, OracleError> {
    let addrs: Vec<_> = (host, port)
        .to_socket_addrs()
        .map_err(|e| {
            OracleError::operational(format!(
                "Oracle TCP address resolution failed for {host}:{port}: {e}"
            ))
        })?
        .collect();
    if addrs.is_empty() {
        return Err(OracleError::operational(format!(
            "Oracle TCP address resolution returned no addresses for {host}:{port}"
        )));
    }
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(sock) => return Ok(sock),
            Err(e) => last_err = Some(e),
        }
    }
    Err(OracleError::operational(format!(
        "Oracle TCP connect to {host}:{port} failed: {}",
        last_err.map_or_else(|| "unreachable".to_owned(), |e| e.to_string())
    )))
}

fn start_tns_reset_relay(
    connect_string: &str,
    parsed: &ParsedTcpTarget,
    connect_timeout: Duration,
) -> Result<(String, Arc<TcpStream>, RelayAcceptGuard), OracleError> {
    let usock = connect_tcp_with_timeout(&parsed.host, parsed.port, connect_timeout)?;
    let usock_handle =
        Arc::new(usock.try_clone().map_err(|e| {
            OracleError::operational(format!("failed cloning Oracle TCP socket: {e}"))
        })?);
    let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(|e| {
        OracleError::operational(format!("failed binding local Oracle TNS relay socket: {e}"))
    })?;
    let local_port = listener
        .local_addr()
        .map_err(|e| OracleError::operational(format!("failed reading relay local_addr: {e}")))?
        .port();
    let local_connect_str = rewrite_target_to_local_port(connect_string, parsed, local_port);
    let abort_flag = Arc::new(AtomicBool::new(false));
    let abort_for_thread = Arc::clone(&abort_flag);

    thread::Builder::new()
        .name("vtb-ora-tns-rx".to_owned())
        .spawn(move || {
            if let Ok((csock, _)) = listener.accept() {
                drop(listener);
                if abort_for_thread.load(Ordering::Acquire) {
                    let _ = csock.shutdown(Shutdown::Both);
                    let _ = usock.shutdown(Shutdown::Both);
                    return;
                }
                relay_tns_connection(csock, usock);
            } else {
                let _ = usock.shutdown(Shutdown::Both);
            }
        })
        .map_err(|e| {
            OracleError::Interface(format!("failed spawning Oracle TNS relay thread: {e}"))
        })?;

    Ok((
        local_connect_str,
        usock_handle,
        RelayAcceptGuard {
            local_port,
            abort_flag,
            armed: true,
        },
    ))
}

fn dial_oracledb_inner(
    cfg: &NativeOracleConfig,
    enable_reset_relay: bool,
) -> Result<DialedOracleSession, OracleError> {
    let resolved = resolve_endpoint(cfg)?;
    let user = cfg.user.as_deref().unwrap_or("system");
    let password = resolved
        .bearer_token
        .as_deref()
        .or(cfg.password.as_deref())
        .unwrap_or("");

    let mut ora_cfg = oracledb::Config::default()
        .set_user(user)
        .set_password(password)
        .set_stmtcachesize(cfg.stmtcachesize as usize)
        .set_transport_connect_timeout(Some(cfg.connect_timeout));

    if let Some(ref dir) = cfg.tns_admin {
        ora_cfg = ora_cfg.set_config_dir(dir);
    }
    if let Some(ref cclass) = cfg.cclass {
        ora_cfg = ora_cfg.set_cclass(cclass.clone());
    }
    if let Some(ref wallet_loc) = cfg.wallet_location {
        ora_cfg = ora_cfg.set_wallet_location(wallet_loc.clone());
    }
    if let Some(ref wallet_pw) = cfg.wallet_password {
        ora_cfg = ora_cfg.set_wallet_password(wallet_pw);
    }

    let mut relay_bundle = None;
    let effective_connect_str = if enable_reset_relay
        && resolved.protocol == OracleProtocol::Tcp
        && let Some(parsed) = parse_single_tcp_target(&resolved.connect_string)
    {
        let (local_str, upstream_sock, guard) =
            start_tns_reset_relay(&resolved.connect_string, &parsed, cfg.connect_timeout)?;
        relay_bundle = Some((upstream_sock, guard));
        local_str
    } else {
        resolved.connect_string.clone()
    };

    ora_cfg = ora_cfg
        .set_connect_string(&effective_connect_str)
        .map_err(|e| OracleError::from_oracledb(&e))?;

    if let Some((_, ref mut guard)) = relay_bundle {
        guard.armed = false;
    }

    let conn_res = oracledb::connect(ora_cfg);
    let conn = match conn_res {
        Ok(c) => c,
        Err(e) => {
            if let Some((ref sock, ref guard)) = relay_bundle {
                guard.abort_flag.store(true, Ordering::Release);
                let _ = sock.shutdown(Shutdown::Both);
                let _ = TcpStream::connect(("127.0.0.1", guard.local_port));
            }
            return Err(OracleError::from_oracledb(&e));
        }
    };

    let upstream_socket = relay_bundle.map(|(sock, _)| sock);

    if let Some(timeout_ms) = cfg.call_timeout_ms {
        conn.set_call_timeout(Some(Duration::from_millis(timeout_ms)))
            .map_err(|e| OracleError::from_oracledb(&e))?;
    }
    if let Some(ref cid) = cfg.client_identifier {
        conn.set_client_identifier(cid)
            .map_err(|e| OracleError::from_oracledb(&e))?;
    }
    if let Some(ref module) = cfg.module {
        conn.set_module(module)
            .map_err(|e| OracleError::from_oracledb(&e))?;
    }
    if let Some(ref action) = cfg.action {
        conn.set_action(action)
            .map_err(|e| OracleError::from_oracledb(&e))?;
    }

    conn.execute("ALTER SESSION SET TIME_ZONE = '+00:00'", &[])
        .map_err(|e| OracleError::from_oracledb(&e))?;

    Ok(DialedOracleSession {
        conn,
        upstream_socket,
    })
}

/// Establish a cancellable [`DialedOracleSession`] wrapping TNS `MARKER_TYPE_RESET` recovery over TCP.
pub fn connect_oracledb_session(
    cfg: &NativeOracleConfig,
) -> Result<DialedOracleSession, OracleError> {
    dial_oracledb_inner(cfg, true)
}

/// Establish a direct physical `oracledb::Connection` from a validated [`NativeOracleConfig`].
pub fn connect_oracledb(cfg: &NativeOracleConfig) -> Result<oracledb::Connection, OracleError> {
    dial_oracledb_inner(cfg, false).map(|s| s.conn)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::thread;

    use zip::write::SimpleFileOptions;

    use super::{
        OracleWalletMaterial, TNS_MARKER_INTERRUPT, TNS_MARKER_RESET, TNS_PACKET_ACCEPT,
        TNS_PACKET_MARKER, parse_single_tcp_target, relay_tns_connection, resolve_endpoint,
        rewrite_target_to_local_port,
    };
    use crate::config::{NativeOracleConfig, OracleProtocol, RawOracleOptions};
    use crate::errors::OracleError;

    const SAMPLE_EWALLET_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\n\
MIIBkTCB+wIJAKHHCgVZU6pJMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl\n\
c3RjYTAeFw0yNDAxMDEwMDAwMDBaFw0zNDAxMDEwMDAwMDBaMBExDzANBgNVBAMM\n\
BnRlc3RjYTBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC7o96n3tW2tB6n0vV5b2tM\n\
1rK+4q2f4yH5gR5m0nC2xG7hJ8kL9mN0pQ1rS2tU3vW4xY5zA6bC7dE8fG9hAgMB\n\
AAGjUzBRMB0GA1UdDgQWBBT1234567890abcdef1234567890TAfBgNVHSMEGDAW\n\
gBT1234567890abcdef1234567890TAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEB\n\
CwUAA0EAkL9mN0pQ1rS2tU3vW4xY5zA6bC7dE8fG9hI0jK1lM2nO3pQ4rS5tU6vW\n\
7xY8zA9bC0dE1fG2hI3jK4lM5nO6pQ==\n\
-----END CERTIFICATE-----\n\
-----BEGIN PRIVATE KEY-----\n\
MIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEAu6Pep97VtrQep9L1\n\
eW9rTNa9vuKtn+Mh+YEeZtJwtcR+4SfJCy5nTtK1ItbVOO1+MWOcwOmwu3RPHxvo\n\
QIDAQABAkB5b2tM1rK+4q2f4yH5gR5m0nC2xG7hJ8kL9mN0pQ1rS2tU3vW4xY5z\n\
A6bC7dE8fG9hI0jK1lM2nO3pQ4rS5tU5AiEA5tU6vW7xY8zA9bC0dE1fG2hI3jK4\n\
lM5nO6pQ7rS8tU9vAiEA4xY5zA6bC7dE8fG9hI0jK1lM2nO3pQ4rS5tU6vW7xY8C\n\
IQD1rS2tU3vW4xY5zA6bC7dE8fG9hI0jK1lM2nO3pQ4rS5tUQiEAmN0pQ1rS2tU3\n\
vW4xY5zA6bC7dE8fG9hI0jK1lM2nO3pQAiEA2tU3vW4xY5zA6bC7dE8fG9hI0jK1\n\
lM2nO3pQ4rS5tU6v\n\
-----END PRIVATE KEY-----\n";

    fn build_synthetic_wallet_zip() -> Vec<u8> {
        let mut buf = Cursor::new(Vec::new());
        {
            let mut zip = zip::ZipWriter::new(&mut buf);
            let opts = SimpleFileOptions::default();
            zip.start_file("tnsnames.ora", opts).expect("start file");
            zip.write_all(
                b"mydb_high = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(PORT=1522)(HOST=adb.us-ashburn-1.oraclecloud.com))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=g1_mydb_high.adb.oraclecloud.com))(SECURITY=(SSL_SERVER_CERT_DN=\"CN=adb.us-ashburn-1.oraclecloud.com\")))\n",
            )
            .expect("write tnsnames");
            zip.start_file("ewallet.pem", opts).expect("start pem");
            zip.write_all(SAMPLE_EWALLET_PEM).expect("write pem");
            zip.finish().expect("finish zip");
        }
        buf.into_inner()
    }

    #[test]
    fn wallet_material_extracts_zip_in_memory_and_resolves_alias() {
        let zip_bytes = build_synthetic_wallet_zip();
        let wallet = OracleWalletMaterial::from_zip_bytes(&zip_bytes).expect("valid wallet zip");
        assert_eq!(wallet.cert_der_count, 1);
        assert!(wallet.has_private_key);
        let desc = wallet.resolve_alias("mydb_high").expect("mydb_high alias");
        assert_eq!(desc.host, "adb.us-ashburn-1.oraclecloud.com");
        assert_eq!(desc.port, 1522);
        assert_eq!(
            desc.service_name.as_deref(),
            Some("g1_mydb_high.adb.oraclecloud.com")
        );
    }

    #[test]
    fn resolve_endpoint_handles_wallet_alias_and_ora_12154_on_unknown_alias() {
        let temp_dir =
            std::env::temp_dir().join(format!("vtb-ora-wallet-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).expect("create tempdir");
        let zip_path = temp_dir.join("Wallet_MYDB.zip");
        std::fs::write(&zip_path, build_synthetic_wallet_zip()).expect("write zip");

        let cfg = NativeOracleConfig::validate(RawOracleOptions {
            dsn: Some("mydb_high".to_owned()),
            wallet_location: Some(zip_path.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .expect("valid cfg");
        let resolved = resolve_endpoint(&cfg).expect("resolved");
        assert_eq!(resolved.protocol, OracleProtocol::Tcps);
        assert!(
            resolved
                .connect_string
                .contains("g1_mydb_high.adb.oraclecloud.com")
        );

        let bad_cfg = NativeOracleConfig::validate(RawOracleOptions {
            dsn: Some("unknown_alias".to_owned()),
            wallet_location: Some(zip_path.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .expect("valid cfg");
        let err = resolve_endpoint(&bad_cfg).expect_err("unknown alias must fail");
        assert!(matches!(err, OracleError::Server { code: 12154, .. }));
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn resolve_endpoint_supports_token_auth_and_rejects_empty_token() {
        let ok_cfg = NativeOracleConfig::validate(RawOracleOptions {
            auth_mode: "token".to_owned(),
            access_token: Some("mock-oauth-token".to_owned()),
            ..Default::default()
        })
        .expect("valid cfg");
        let resolved = resolve_endpoint(&ok_cfg).expect("resolved");
        assert_eq!(resolved.bearer_token.as_deref(), Some("mock-oauth-token"));

        let missing_cfg = NativeOracleConfig::validate(RawOracleOptions {
            auth_mode: "token".to_owned(),
            access_token: None,
            ..Default::default()
        })
        .expect("valid cfg");
        assert!(matches!(
            resolve_endpoint(&missing_cfg),
            Err(OracleError::Config(_))
        ));
    }

    #[test]
    fn parse_and_rewrite_single_tcp_targets_preserves_service_and_skips_tcps() {
        let easy = parse_single_tcp_target("db.internal:5531/FREEPDB1:POOLED")
            .expect("easy connect should parse");
        assert_eq!(easy.host, "db.internal");
        assert_eq!(easy.port, 5531);
        assert_eq!(
            rewrite_target_to_local_port("db.internal:5531/FREEPDB1:POOLED", &easy, 41234),
            "127.0.0.1:41234/FREEPDB1:POOLED"
        );

        let ipv6 = parse_single_tcp_target("//[::1]:1521/FREEPDB1").expect("ipv6 easy connect");
        assert_eq!(ipv6.host, "::1");
        assert_eq!(ipv6.port, 1521);
        assert_eq!(
            rewrite_target_to_local_port("//[::1]:1521/FREEPDB1", &ipv6, 42345),
            "127.0.0.1:42345/FREEPDB1"
        );

        let desc_str = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=ora.internal)(PORT=1525))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))";
        let desc = parse_single_tcp_target(desc_str).expect("single-host tcp descriptor");
        assert_eq!(desc.host, "ora.internal");
        assert_eq!(desc.port, 1525);
        assert_eq!(
            rewrite_target_to_local_port(desc_str, &desc, 43456),
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=43456))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))"
        );

        assert!(parse_single_tcp_target("tcps://adb.example.com:1522/mydb_high").is_none());
        assert!(parse_single_tcp_target(
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=adb.example.com)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=s)))"
        )
        .is_none());
        assert!(parse_single_tcp_target(
            "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=h1)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=h2)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=s)))"
        )
        .is_none());
    }

    #[test]
    fn relay_tns_connection_frames_32bit_accept_and_duplicates_unsolicited_reset() {
        let client_listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind client side");
        let upstream_listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind upstream side");
        let client_port = client_listener.local_addr().expect("addr").port();
        let upstream_port = upstream_listener.local_addr().expect("addr").port();

        let mut client_end = TcpStream::connect(("127.0.0.1", client_port)).expect("client conn");
        let (relay_csock, _) = client_listener.accept().expect("accept client");
        let relay_usock = TcpStream::connect(("127.0.0.1", upstream_port)).expect("upstream conn");
        let (mut server_end, _) = upstream_listener.accept().expect("accept upstream");

        let relay_thread = thread::spawn(move || {
            relay_tns_connection(relay_csock, relay_usock);
        });

        // 1. 16-bit CONNECT frame (client -> server) followed by ACCEPT v319 (server -> client).
        let connect_pkt = [0x00, 0x0A, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB];
        client_end.write_all(&connect_pkt).expect("write connect");
        let mut srv_buf = [0u8; 10];
        server_end.read_exact(&mut srv_buf).expect("read connect");
        assert_eq!(srv_buf, connect_pkt);

        let accept_pkt = [
            0x00,
            0x0C,
            0x00,
            0x00,
            TNS_PACKET_ACCEPT,
            0x00,
            0x00,
            0x00,
            0x01,
            0x3F,
            0x00,
            0x00,
        ];
        server_end.write_all(&accept_pkt).expect("write accept");
        let mut cli_accept = [0u8; 12];
        client_end.read_exact(&mut cli_accept).expect("read accept");
        assert_eq!(cli_accept, accept_pkt);

        // 2. Post-ACCEPT 32-bit DATA frame (client -> server) switches framing cleanly after block.
        let data_pkt = [
            0x00, 0x00, 0x00, 0x0C, 0x06, 0x00, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44,
        ];
        client_end.write_all(&data_pkt).expect("write 32-bit data");
        let mut srv_data = [0u8; 12];
        server_end
            .read_exact(&mut srv_data)
            .expect("read 32-bit data");
        assert_eq!(srv_data, data_pkt);

        // 3. Unsolicited server MARKER_TYPE_RESET (OOB ALTER SYSTEM CANCEL SQL) is duplicated once.
        let reset_marker = [
            0x00,
            0x00,
            0x00,
            0x0B,
            TNS_PACKET_MARKER,
            0x20,
            0x00,
            0x00,
            0x01,
            0x00,
            TNS_MARKER_RESET,
        ];
        server_end
            .write_all(&reset_marker)
            .expect("write unsolicited reset");
        let mut doubled = [0u8; 22];
        client_end
            .read_exact(&mut doubled)
            .expect("read duplicated reset");
        assert_eq!(&doubled[..11], &reset_marker);
        assert_eq!(&doubled[11..], &reset_marker);

        // Subsequent server DATA resets `reset_active`.
        server_end.write_all(&data_pkt).expect("write server data");
        let mut cli_data = [0u8; 12];
        client_end
            .read_exact(&mut cli_data)
            .expect("read server data");
        assert_eq!(cli_data, data_pkt);

        // 4. Client-initiated MARKER_TYPE_INTERRUPT -> server MARKER_TYPE_RESET forwarded 1x only, followed by DATA.
        let interrupt_marker = [
            0x00,
            0x00,
            0x00,
            0x0B,
            TNS_PACKET_MARKER,
            0x20,
            0x00,
            0x00,
            0x01,
            0x00,
            TNS_MARKER_INTERRUPT,
        ];
        client_end
            .write_all(&interrupt_marker)
            .expect("write interrupt");
        let mut srv_int = [0u8; 11];
        server_end.read_exact(&mut srv_int).expect("read interrupt");
        assert_eq!(srv_int, interrupt_marker);

        server_end
            .write_all(&reset_marker)
            .expect("write ack reset");
        server_end
            .write_all(&data_pkt)
            .expect("write trailing data");
        let mut single_and_data = [0u8; 23];
        client_end
            .read_exact(&mut single_and_data)
            .expect("read single reset + data");
        assert_eq!(&single_and_data[..11], &reset_marker);
        assert_eq!(&single_and_data[11..], &data_pkt);

        let _ = client_end.shutdown(Shutdown::Both);
        let _ = server_end.shutdown(Shutdown::Both);
        relay_thread.join().expect("relay shuts down cleanly");
    }
}
