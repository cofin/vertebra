//! TNS Connect Descriptor resolution, in-memory Autonomous Database Wallet mTLS, and IAM token dialer.

use std::collections::HashMap;
use std::fmt;
use std::io::{Cursor, Read};
use std::path::Path;
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

/// Establish a physical `oracledb::Connection` from a validated [`NativeOracleConfig`].
pub fn connect_oracledb(cfg: &NativeOracleConfig) -> Result<oracledb::Connection, OracleError> {
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

    ora_cfg = ora_cfg
        .set_connect_string(&resolved.connect_string)
        .map_err(|e| OracleError::from_oracledb(&e))?;

    let conn = oracledb::connect(ora_cfg).map_err(|e| OracleError::from_oracledb(&e))?;

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

    Ok(conn)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use zip::write::SimpleFileOptions;

    use super::{OracleWalletMaterial, resolve_endpoint};
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
            access_token: Some("ya29-test-token".to_owned()),
            ..Default::default()
        })
        .expect("valid cfg");
        let resolved = resolve_endpoint(&ok_cfg).expect("resolved");
        assert_eq!(resolved.bearer_token.as_deref(), Some("ya29-test-token"));

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
}
