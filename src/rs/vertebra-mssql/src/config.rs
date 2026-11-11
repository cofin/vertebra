//! Configuration types for `vertebra-mssql` TDS 7.3+ connections, TLS encryption modes,
//! transaction isolation levels, and Cloud SQL for SQL Server (`:3307`) mTLS dialing.

use std::net::SocketAddr;
use std::time::Duration;

use tiberius::{AuthMethod, Config as TiberiusConfig, EncryptionLevel, IsolationLevel};
use vertebra_db::connection::CloudPeerIdentity;
use vertebra_db::{
    AuthType, CloudSqlDialer, CloudSqlInstanceUri, ConnectOptions, EndpointTarget, IpType, TlsMode,
};

use crate::errors::{MssqlError, MssqlErrorKind};

/// Default SQL Server TCP port (`1433`).
pub const DEFAULT_MSSQL_PORT: u16 = 1433;

/// Default connection timeout (`10` seconds).
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 10_000;

/// Default query execution timeout (`30` seconds).
pub const DEFAULT_QUERY_TIMEOUT_MS: u64 = 30_000;

/// TDS `PRELOGIN` encryption negotiation mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MssqlEncryptionMode {
    /// Encrypt only the `LOGIN7` credentials packet (`ENCRYPT_OFF`).
    LoginOnly,
    /// Encrypt the entire TDS session when supported by the server (`ENCRYPT_ON` / `Required`).
    #[default]
    Required,
    /// Strict TLS wrapping where TLS is established before any TDS packet (`Strict` / Cloud SQL `:3307` mTLS).
    Strict,
    /// Do not encrypt (`ENCRYPT_NOT_SUP`, only for local loopback/testing).
    Disabled,
}

impl MssqlEncryptionMode {
    /// Parse an encryption mode string (`"required"`, `"optional"`, `"strict"`, `"login_only"`, `"not_supported"`, `"disabled"`).
    pub fn parse(raw: &str) -> Result<Self, MssqlError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "required" | "require" | "true" | "on" | "yes" | "mandatory" => Ok(Self::Required),
            "strict" | "verify-full" | "verify_full" | "verify-ca" | "verify_ca" => {
                Ok(Self::Strict)
            }
            "login_only" | "login-only" | "prefer" | "optional" => Ok(Self::LoginOnly),
            "disabled" | "disable" | "false" | "off" | "no" | "not_supported" => Ok(Self::Disabled),
            other => Err(MssqlError::config(format!(
                "unsupported MSSQL encryption mode `{other}`; expected required, optional, strict, login_only, or not_supported"
            ))),
        }
    }

    /// Return the corresponding [`tiberius::EncryptionLevel`] for standard TDS connections.
    ///
    /// Note: When [`Self::Strict`] or Cloud SQL `:3307` mTLS is used, TLS is established on the
    /// outer [`vertebra_db::TransportStream`] prior to TDS `PRELOGIN`, so the inner `tiberius`
    /// framer operates with [`EncryptionLevel::NotSupported`] to avoid double TLS handshakes.
    pub const fn to_tiberius_level(self, pre_encrypted_stream: bool) -> EncryptionLevel {
        if pre_encrypted_stream {
            return EncryptionLevel::NotSupported;
        }
        match self {
            Self::LoginOnly => EncryptionLevel::Off,
            Self::Required | Self::Strict => EncryptionLevel::Required,
            Self::Disabled => EncryptionLevel::NotSupported,
        }
    }

    /// Canonical string representation of this encryption mode.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LoginOnly => "login_only",
            Self::Required => "required",
            Self::Strict => "strict",
            Self::Disabled => "disabled",
        }
    }
}

/// SQL Server transaction isolation level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MssqlIsolationLevel {
    ReadUncommitted,
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

impl MssqlIsolationLevel {
    /// Parse a transaction isolation level string.
    pub fn parse(raw: &str) -> Result<Self, MssqlError> {
        let normalized = raw.trim().to_ascii_uppercase().replace(['_', '-'], " ");
        match normalized.as_str() {
            "READ UNCOMMITTED" => Ok(Self::ReadUncommitted),
            "READ COMMITTED" => Ok(Self::ReadCommitted),
            "REPEATABLE READ" => Ok(Self::RepeatableRead),
            "SERIALIZABLE" => Ok(Self::Serializable),
            "SNAPSHOT" => Ok(Self::Snapshot),
            other => Err(MssqlError::programming(format!(
                "unsupported SQL Server isolation level `{other}`"
            ))),
        }
    }

    /// Convert to [`tiberius::IsolationLevel`].
    pub const fn to_tiberius(self) -> IsolationLevel {
        match self {
            Self::ReadUncommitted => IsolationLevel::ReadUncommitted,
            Self::ReadCommitted => IsolationLevel::ReadCommitted,
            Self::RepeatableRead => IsolationLevel::RepeatableRead,
            Self::Serializable => IsolationLevel::Serializable,
            Self::Snapshot => IsolationLevel::Snapshot,
        }
    }

    /// Canonical T-SQL `SET TRANSACTION ISOLATION LEVEL <level>` clause.
    pub const fn as_tsql(self) -> &'static str {
        match self {
            Self::ReadUncommitted => "READ UNCOMMITTED",
            Self::ReadCommitted => "READ COMMITTED",
            Self::RepeatableRead => "REPEATABLE READ",
            Self::Serializable => "SERIALIZABLE",
            Self::Snapshot => "SNAPSHOT",
        }
    }
}

/// Optional Cloud SQL for SQL Server (`:3307`) mTLS dialer configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MssqlCloudSqlConfig {
    pub instance_uri: CloudSqlInstanceUri,
    pub ip_type: IpType,
    pub auth_type: AuthType,
    pub proxy_addr: Option<SocketAddr>,
    pub server_ca_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
}

impl MssqlCloudSqlConfig {
    /// Build a [`CloudSqlDialer`] for this configuration.
    pub fn dialer(&self) -> CloudSqlDialer {
        CloudSqlDialer {
            uri: self.instance_uri.clone(),
            ip_type: self.ip_type,
            auth_type: self.auth_type,
            server_port: CloudSqlInstanceUri::default_server_port(),
        }
    }
}

/// Validated configuration for a native `vertebra._mssql` client.
#[derive(Clone)]
pub struct MssqlClientConfig {
    pub dsn: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub application_name: String,
    pub encryption: MssqlEncryptionMode,
    pub trust_server_certificate: bool,
    pub ca_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    pub server_name: Option<String>,
    pub connect_timeout: Duration,
    pub query_timeout: Duration,
    pub pool_size: u32,
    pub max_overflow: u32,
    pub statement_cache_capacity: u32,
    pub read_only_intent: bool,
    pub cloudsql: Option<MssqlCloudSqlConfig>,
}

impl std::fmt::Debug for MssqlClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlClientConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("application_name", &self.application_name)
            .field("encryption", &self.encryption)
            .field("trust_server_certificate", &self.trust_server_certificate)
            .field("server_name", &self.server_name)
            .field("connect_timeout", &self.connect_timeout)
            .field("query_timeout", &self.query_timeout)
            .field("pool_size", &self.pool_size)
            .field("max_overflow", &self.max_overflow)
            .field("statement_cache_capacity", &self.statement_cache_capacity)
            .field("read_only_intent", &self.read_only_intent)
            .field("cloudsql", &self.cloudsql.as_ref().map(|c| &c.instance_uri))
            .finish()
    }
}

impl std::fmt::Display for MssqlClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "mssql://{}:[REDACTED]@{}:{}/{}",
            self.user, self.host, self.port, self.database
        )
    }
}

impl Default for MssqlClientConfig {
    fn default() -> Self {
        Self {
            dsn: "mssql://localhost:1433/master".to_string(),
            host: "127.0.0.1".to_string(),
            port: DEFAULT_MSSQL_PORT,
            database: "master".to_string(),
            user: "sa".to_string(),
            password: String::new(),
            application_name: "vertebra-mssql".to_string(),
            encryption: MssqlEncryptionMode::Required,
            trust_server_certificate: false,
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            server_name: None,
            connect_timeout: Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS),
            query_timeout: Duration::from_millis(DEFAULT_QUERY_TIMEOUT_MS),
            pool_size: 5,
            max_overflow: 10,
            statement_cache_capacity: 128,
            read_only_intent: false,
            cloudsql: None,
        }
    }
}

impl MssqlClientConfig {
    /// Validate port bounds (`1..=65535`), NUL-byte invariants, and PEM inputs.
    pub fn validate(&self) -> Result<(), MssqlError> {
        if self.port == 0 {
            return Err(MssqlError::config(
                "MSSQL TCP port must be between 1 and 65535",
            ));
        }
        for (field, value) in [
            ("dsn", self.dsn.as_str()),
            ("host", self.host.as_str()),
            ("database", self.database.as_str()),
            ("user", self.user.as_str()),
            ("application_name", self.application_name.as_str()),
            ("server_name", self.server_name.as_deref().unwrap_or("")),
        ] {
            if value.contains('\0') {
                return Err(MssqlError::config(format!(
                    "MSSQL `{field}` must not contain NUL bytes"
                )));
            }
        }
        if self.host.trim().is_empty() {
            return Err(MssqlError::config("MSSQL host must not be empty"));
        }
        if let Some(ref ca) = self.ca_pem {
            validate_pem_bytes("ca_pem", ca)?;
        }
        if let Some(ref cert) = self.client_cert_pem {
            validate_pem_bytes("client_cert_pem", cert)?;
        }
        if let Some(ref key) = self.client_key_pem {
            validate_pem_bytes("client_key_pem", key)?;
        }
        if self.client_cert_pem.is_some() != self.client_key_pem.is_some() {
            return Err(MssqlError::config(
                "MSSQL mTLS requires both client_cert_pem and client_key_pem",
            ));
        }
        Ok(())
    }

    /// Build a [`tiberius::Config`] for the given target host/port and stream encryption state.
    pub fn to_tiberius_config(
        &self,
        host: &str,
        port: u16,
        pre_encrypted_stream: bool,
    ) -> TiberiusConfig {
        let mut cfg = TiberiusConfig::new();
        cfg.host(host);
        cfg.port(port);
        cfg.database(&self.database);
        cfg.application_name(&self.application_name);
        cfg.authentication(AuthMethod::sql_server(&self.user, &self.password));
        cfg.encryption(self.encryption.to_tiberius_level(pre_encrypted_stream));
        if self.trust_server_certificate {
            cfg.trust_cert();
        }
        if self.read_only_intent {
            cfg.readonly(true);
        }
        cfg
    }

    /// Build [`vertebra_db::ConnectOptions`] for establishing the underlying [`vertebra_db::TransportStream`].
    pub fn to_connect_options(&self, host: &str, port: u16) -> Result<ConnectOptions, MssqlError> {
        if let Some(ref csql) = self.cloudsql
            && let Some(addr) = csql.proxy_addr
        {
            let server_ca = csql
                .server_ca_pem
                .clone()
                .or_else(|| self.ca_pem.clone())
                .ok_or_else(|| {
                    MssqlError::new(
                        MssqlErrorKind::ConnectionFailure,
                        "Cloud SQL for SQL Server mTLS requires server_ca_pem",
                        Some("08001"),
                        None,
                        false,
                    )
                })?;
            let (client_cert, client_key) = match (&csql.client_cert_pem, &csql.client_key_pem) {
                (Some(c), Some(k)) => (c.clone(), k.clone()),
                _ => {
                    let (cert_str, key_str) = csql
                        .dialer()
                        .generate_ephemeral_keypair()
                        .map_err(MssqlError::config)?;
                    (cert_str.into_bytes(), key_str.into_bytes())
                }
            };
            return Ok(ConnectOptions {
                target: EndpointTarget::CloudSql {
                    instance: csql.instance_uri.clone(),
                    addr,
                    server_ca_pem: server_ca,
                    client_cert_pem: client_cert,
                    client_key_pem: client_key,
                    peer_identity: CloudPeerIdentity::CloudSql {
                        server_name: self.server_name.clone(),
                        expected_cn: format!(
                            "{}:{}",
                            csql.instance_uri.project, csql.instance_uri.instance
                        ),
                    },
                },
                tls_mode: TlsMode::VerifyCa,
                ca_pem: self.ca_pem.clone(),
                client_cert_pem: self.client_cert_pem.clone(),
                client_key_pem: self.client_key_pem.clone(),
                server_name: self.server_name.clone(),
                connect_timeout: self.connect_timeout,
                tcp_nodelay: true,
                application_name: Some(self.application_name.clone()),
            });
        }

        let tls_mode = match self.encryption {
            MssqlEncryptionMode::Strict => {
                if self.trust_server_certificate {
                    TlsMode::Require
                } else if self.ca_pem.is_some() {
                    TlsMode::VerifyCa
                } else {
                    TlsMode::VerifyFull
                }
            }
            MssqlEncryptionMode::Required
            | MssqlEncryptionMode::LoginOnly
            | MssqlEncryptionMode::Disabled => TlsMode::Disable,
        };

        let mut opts = ConnectOptions::tcp(host.to_string(), port)
            .with_tls_mode(tls_mode)
            .with_connect_timeout(self.connect_timeout);
        if let Some(ref ca) = self.ca_pem {
            opts = opts.with_ca_pem(ca.clone());
        }
        if let (Some(cert), Some(key)) = (&self.client_cert_pem, &self.client_key_pem) {
            opts = opts.with_client_identity(cert.clone(), key.clone());
        }
        if let Some(ref sni) = self.server_name {
            opts = opts.with_server_name(sni.clone());
        }
        Ok(opts)
    }

    /// Parse a SQL Server DSN (`mssql://user:pass@host:port/database?...`) into a [`MssqlClientConfig`].
    pub fn from_dsn(dsn: &str) -> Result<Self, MssqlError> {
        let trimmed = dsn.trim();
        if trimmed.is_empty() {
            return Err(MssqlError::config("MSSQL DSN must not be empty"));
        }
        if trimmed.starts_with("memory://") || trimmed == ":memory:" {
            return Err(MssqlError::config(
                "MSSQL driver requires a network TCP/TLS or Cloud SQL endpoint; `memory://` stand-in URIs are not supported",
            ));
        }

        let mut cfg = Self {
            dsn: trimmed.to_string(),
            ..Self::default()
        };

        let rest = if let Some(stripped) = trimmed.strip_prefix("mssql+vertebra://") {
            stripped
        } else if let Some(stripped) = trimmed.strip_prefix("mssql://") {
            stripped
        } else if let Some(stripped) = trimmed.strip_prefix("sqlserver://") {
            stripped
        } else {
            return Err(MssqlError::config(format!(
                "invalid MSSQL DSN scheme in `{trimmed}`; expected mssql:// or sqlserver://"
            )));
        };

        let (authority_and_path, query_opt) = match rest.split_once('?') {
            Some((ap, q)) => (ap, Some(q)),
            None => (rest, None),
        };

        let (authority, path_db) = match authority_and_path.split_once('/') {
            Some((a, p)) => (a, p),
            None => (authority_and_path, ""),
        };

        if !path_db.trim().is_empty() {
            cfg.database = path_db.trim().to_string();
        }

        let host_port = if let Some((userinfo, hp)) = authority.rsplit_once('@') {
            if let Some((u, p)) = userinfo.split_once(':') {
                if !u.is_empty() {
                    cfg.user = percent_decode(u);
                }
                cfg.password = percent_decode(p);
            } else if !userinfo.is_empty() {
                cfg.user = percent_decode(userinfo);
            }
            hp
        } else {
            authority
        };

        if !host_port.is_empty() {
            if let Some((h, p_str)) = host_port.rsplit_once(':') {
                if !h.is_empty() {
                    cfg.host = h.trim_matches(['[', ']']).to_string();
                }
                if !p_str.is_empty() {
                    cfg.port = p_str.parse::<u16>().map_err(|_| {
                        MssqlError::config(format!("invalid MSSQL TCP port `{p_str}` in DSN"))
                    })?;
                }
            } else {
                cfg.host = host_port.trim_matches(['[', ']']).to_string();
            }
        }

        if let Some(query) = query_opt {
            for pair in query.split('&') {
                if pair.is_empty() {
                    continue;
                }
                let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
                let key = k.trim().to_ascii_lowercase();
                let val = percent_decode(v.trim());
                match key.as_str() {
                    "encrypt" | "encryption" => {
                        cfg.encryption = MssqlEncryptionMode::parse(&val)?;
                    }
                    "trustservercertificate" | "trust_server_certificate" | "trust_cert" => {
                        cfg.trust_server_certificate = matches!(
                            val.to_ascii_lowercase().as_str(),
                            "true" | "1" | "yes" | "on"
                        );
                    }
                    "applicationname" | "application_name" | "app_name" => {
                        if !val.is_empty() {
                            cfg.application_name = val;
                        }
                    }
                    "applicationintent" | "application_intent" => {
                        cfg.read_only_intent = val.eq_ignore_ascii_case("readonly");
                    }
                    "connect_timeout" | "login_timeout" => {
                        if let Ok(secs) = val.parse::<u64>() {
                            cfg.connect_timeout = Duration::from_secs(secs.max(1));
                        }
                    }
                    "query_timeout" | "command_timeout" => {
                        if let Ok(secs) = val.parse::<u64>() {
                            cfg.query_timeout = Duration::from_secs(secs.max(1));
                        }
                    }
                    "cloudsql_instance" | "instance_connection_name" => {
                        let uri = CloudSqlInstanceUri::parse(&val).map_err(MssqlError::config)?;
                        cfg.cloudsql = Some(MssqlCloudSqlConfig {
                            instance_uri: uri,
                            ip_type: IpType::Public,
                            auth_type: AuthType::Password,
                            proxy_addr: None,
                            server_ca_pem: None,
                            client_cert_pem: None,
                            client_key_pem: None,
                        });
                    }
                    _ => {}
                }
            }
        }

        cfg.validate()?;
        Ok(cfg)
    }
}

fn validate_pem_bytes(field: &str, bytes: &[u8]) -> Result<(), MssqlError> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        MssqlError::config(format!(
            "invalid MSSQL `{field}`: expected UTF-8 PEM-encoded block"
        ))
    })?;
    if !text.contains("-----BEGIN ") || !text.contains("-----END ") {
        return Err(MssqlError::config(format!(
            "invalid MSSQL `{field}`: expected PEM header/footer (`-----BEGIN ...`)"
        )));
    }
    Ok(())
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

const fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsn_parsing_and_encryption_modes() {
        let cfg = MssqlClientConfig::from_dsn(
            "mssql://sa:Str0ng%21Pass@sql.example.internal:14330/sales?encrypt=strict&trust_server_certificate=true&application_name=vtb-test&application_intent=ReadOnly",
        )
        .expect("parse dsn");
        assert_eq!(cfg.user, "sa");
        assert_eq!(cfg.password, "Str0ng!Pass");
        assert_eq!(cfg.host, "sql.example.internal");
        assert_eq!(cfg.port, 14330);
        assert_eq!(cfg.database, "sales");
        assert_eq!(cfg.encryption, MssqlEncryptionMode::Strict);
        assert!(cfg.trust_server_certificate);
        assert_eq!(cfg.application_name, "vtb-test");
        assert!(cfg.read_only_intent);

        assert!(MssqlClientConfig::from_dsn("memory://mssql").is_err());
        assert!(MssqlClientConfig::from_dsn(":memory:").is_err());
        assert!(MssqlClientConfig::from_dsn("mssql://sa:pw@127.0.0.1:0/master").is_err());
        assert!(MssqlClientConfig::from_dsn("mssql://sa:pw@127.0.0.1:1433/mas\0ter").is_err());
        assert_eq!(
            MssqlEncryptionMode::parse("optional").unwrap(),
            MssqlEncryptionMode::LoginOnly
        );

        let dbg_str = format!("{cfg:?}");
        let disp_str = format!("{cfg}");
        assert!(dbg_str.contains("[REDACTED]"));
        assert!(!dbg_str.contains("Str0ng!Pass"));
        assert!(disp_str.contains("[REDACTED]"));
        assert!(!disp_str.contains("Str0ng!Pass"));
    }
}
