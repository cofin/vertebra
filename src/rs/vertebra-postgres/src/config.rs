//! Pure in-memory PostgreSQL connection configuration validation and secret redaction.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use vertebra_db::connection::TlsMode;

use crate::errors::PostgresError;

/// SCRAM channel-binding negotiation policy (`disable`, `prefer`, `require`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ChannelBindingPolicy {
    Disable,
    #[default]
    Prefer,
    Require,
}

impl ChannelBindingPolicy {
    /// Parse a canonical lowercase channel-binding policy string.
    pub fn parse(raw: &str) -> Result<Self, PostgresError> {
        match raw {
            "disable" => Ok(Self::Disable),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            other => Err(PostgresError::Config(format!(
                "unsupported channel_binding `{other}` (expected `disable`, `prefer`, or `require`)"
            ))),
        }
    }

    /// Return the canonical lowercase policy name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
        }
    }
}

/// Parse one of the exact five canonical PostgreSQL `sslmode` spellings (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`).
pub fn parse_strict_sslmode(raw: &str) -> Result<TlsMode, PostgresError> {
    match raw {
        "disable" => Ok(TlsMode::Disable),
        "prefer" => Ok(TlsMode::Prefer),
        "require" => Ok(TlsMode::Require),
        "verify-ca" => Ok(TlsMode::VerifyCa),
        "verify-full" => Ok(TlsMode::VerifyFull),
        other => Err(PostgresError::Config(format!(
            "unsupported sslmode `{other}` (expected `disable`, `prefer`, `require`, `verify-ca`, or `verify-full`)"
        ))),
    }
}

/// Unvalidated owned connection constructor options matching the public 25-parameter signature.
#[derive(Clone)]
pub struct RawPostgresOptions {
    pub user: String,
    pub database: String,
    pub host: String,
    pub port: i64,
    pub password: Option<String>,
    pub unix_socket: Option<String>,
    pub sslmode: String,
    pub ca_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    pub server_name: Option<String>,
    pub channel_binding: String,
    pub connect_timeout: f64,
    pub application_name: String,
    pub session_gucs: HashMap<String, String>,
    pub keepalives: bool,
    pub keepalives_idle: Option<i64>,
    pub keepalives_interval: Option<i64>,
    pub keepalives_count: Option<i64>,
    pub cloud_sql_instance: Option<String>,
    pub cloud_sql_ip_type: String,
    pub alloydb_instance_uri: Option<String>,
    pub alloydb_ip_type: String,
    pub enable_iam_auth: bool,
    pub cloud_auth_options: HashMap<String, String>,
    pub iam_auth_options: HashMap<String, String>,
}

impl Default for RawPostgresOptions {
    fn default() -> Self {
        Self {
            user: String::new(),
            database: String::new(),
            host: "localhost".to_string(),
            port: 5432,
            password: None,
            unix_socket: None,
            sslmode: "verify-full".to_string(),
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            server_name: None,
            channel_binding: "prefer".to_string(),
            connect_timeout: 10.0,
            application_name: "vertebra".to_string(),
            session_gucs: HashMap::new(),
            keepalives: true,
            keepalives_idle: None,
            keepalives_interval: None,
            keepalives_count: None,
            cloud_sql_instance: None,
            cloud_sql_ip_type: "PRIVATE".to_string(),
            alloydb_instance_uri: None,
            alloydb_ip_type: "PRIVATE".to_string(),
            enable_iam_auth: false,
            cloud_auth_options: HashMap::new(),
            iam_auth_options: HashMap::new(),
        }
    }
}

impl fmt::Debug for RawPostgresOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawPostgresOptions")
            .field("user", &self.user)
            .field("database", &self.database)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("unix_socket", &self.unix_socket)
            .field("sslmode", &self.sslmode)
            .field("ca_pem_len", &self.ca_pem.as_ref().map(Vec::len))
            .field(
                "client_cert_pem_len",
                &self.client_cert_pem.as_ref().map(Vec::len),
            )
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .field("server_name", &self.server_name)
            .field("channel_binding", &self.channel_binding)
            .field("connect_timeout", &self.connect_timeout)
            .field("application_name", &self.application_name)
            .field("session_gucs", &self.session_gucs)
            .field("keepalives", &self.keepalives)
            .field("keepalives_idle", &self.keepalives_idle)
            .field("keepalives_interval", &self.keepalives_interval)
            .field("keepalives_count", &self.keepalives_count)
            .field("cloud_sql_instance", &self.cloud_sql_instance)
            .field("cloud_sql_ip_type", &self.cloud_sql_ip_type)
            .field("alloydb_instance_uri", &self.alloydb_instance_uri)
            .field("alloydb_ip_type", &self.alloydb_ip_type)
            .field("enable_iam_auth", &self.enable_iam_auth)
            .field("cloud_auth_options", &"<redacted>")
            .field("iam_auth_options", &"<redacted>")
            .finish()
    }
}

/// Validated, immutable PostgreSQL connection configuration owned by `vertebra-postgres`.
#[derive(Clone, PartialEq, Eq)]
pub struct PostgresConfig {
    pub user: String,
    pub database: String,
    pub host: String,
    pub port: u16,
    pub password: Option<String>,
    pub unix_socket: Option<String>,
    pub sslmode: TlsMode,
    pub ca_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    pub server_name: Option<String>,
    pub channel_binding: ChannelBindingPolicy,
    pub connect_timeout: Duration,
    pub application_name: String,
    pub session_gucs: HashMap<String, String>,
    pub keepalives: bool,
    pub keepalives_idle: Option<i64>,
    pub keepalives_interval: Option<i64>,
    pub keepalives_count: Option<i64>,
    pub cloud_sql_instance: Option<String>,
    pub cloud_sql_ip_type: String,
    pub alloydb_instance_uri: Option<String>,
    pub alloydb_ip_type: String,
    pub enable_iam_auth: bool,
    pub cloud_auth_options: HashMap<String, String>,
    pub iam_auth_options: HashMap<String, String>,
}

impl PostgresConfig {
    /// Validate `RawPostgresOptions` purely in memory without DNS, filesystem, or socket I/O.
    pub fn validate(raw: RawPostgresOptions) -> Result<Self, PostgresError> {
        validate_non_empty_no_nul("user", &raw.user)?;
        validate_non_empty_no_nul("database", &raw.database)?;
        validate_non_empty_no_nul("host", &raw.host)?;
        if raw.host.contains("://") || raw.host.contains('/') {
            return Err(PostgresError::Config(
                "TCP host must not be a URI or contain path separators `/`".to_string(),
            ));
        }

        if !(1..=65535).contains(&raw.port) {
            return Err(PostgresError::Config(format!(
                "port must lie in 1..=65535, got {}",
                raw.port
            )));
        }
        let port = raw.port as u16;

        if let Some(ref pw) = raw.password
            && pw.contains('\0')
        {
            return Err(PostgresError::Config(
                "password must not contain NUL bytes".to_string(),
            ));
        }

        if let Some(ref unix_path) = raw.unix_socket {
            validate_non_empty_no_nul("unix_socket", unix_path)?;
        }

        let sslmode = parse_strict_sslmode(&raw.sslmode)?;
        let channel_binding = ChannelBindingPolicy::parse(&raw.channel_binding)?;

        if let Some(ref ca) = raw.ca_pem
            && ca.is_empty()
        {
            return Err(PostgresError::Config(
                "ca_pem must not be empty when provided".to_string(),
            ));
        }

        match (&raw.client_cert_pem, &raw.client_key_pem) {
            (Some(cert), Some(key)) => {
                if cert.is_empty() || key.is_empty() {
                    return Err(PostgresError::Config(
                        "client_cert_pem and client_key_pem must not be empty".to_string(),
                    ));
                }
            }
            (None, None) => {}
            _ => {
                return Err(PostgresError::Config(
                    "client_cert_pem and client_key_pem must be provided together".to_string(),
                ));
            }
        }

        if let Some(ref sni) = raw.server_name {
            validate_non_empty_no_nul("server_name", sni)?;
        }

        if !raw.connect_timeout.is_finite() || raw.connect_timeout <= 0.0 {
            return Err(PostgresError::Config(
                "connect_timeout must be a finite positive number of seconds".to_string(),
            ));
        }
        let connect_timeout = Duration::try_from_secs_f64(raw.connect_timeout).map_err(|_| {
            PostgresError::Config("connect_timeout overflowed Duration bounds".to_string())
        })?;
        if connect_timeout.is_zero() {
            return Err(PostgresError::Config(
                "connect_timeout must be greater than zero".to_string(),
            ));
        }

        validate_non_empty_no_nul("application_name", &raw.application_name)?;

        for (k, v) in &raw.session_gucs {
            validate_non_empty_no_nul("session_gucs key", k)?;
            if v.contains('\0') {
                return Err(PostgresError::Config(format!(
                    "session_gucs value for `{k}` must not contain NUL bytes"
                )));
            }
        }

        validate_ip_type("cloud_sql_ip_type", &raw.cloud_sql_ip_type)?;
        validate_ip_type("alloydb_ip_type", &raw.alloydb_ip_type)?;

        if let Some(ref inst) = raw.cloud_sql_instance {
            validate_non_empty_no_nul("cloud_sql_instance", inst)?;
        }
        if let Some(ref uri) = raw.alloydb_instance_uri {
            validate_non_empty_no_nul("alloydb_instance_uri", uri)?;
        }

        Ok(Self {
            user: raw.user,
            database: raw.database,
            host: raw.host,
            port,
            password: raw.password,
            unix_socket: raw.unix_socket,
            sslmode,
            ca_pem: raw.ca_pem,
            client_cert_pem: raw.client_cert_pem,
            client_key_pem: raw.client_key_pem,
            server_name: raw.server_name,
            channel_binding,
            connect_timeout,
            application_name: raw.application_name,
            session_gucs: raw.session_gucs,
            keepalives: raw.keepalives,
            keepalives_idle: raw.keepalives_idle,
            keepalives_interval: raw.keepalives_interval,
            keepalives_count: raw.keepalives_count,
            cloud_sql_instance: raw.cloud_sql_instance,
            cloud_sql_ip_type: raw.cloud_sql_ip_type,
            alloydb_instance_uri: raw.alloydb_instance_uri,
            alloydb_ip_type: raw.alloydb_ip_type,
            enable_iam_auth: raw.enable_iam_auth,
            cloud_auth_options: raw.cloud_auth_options,
            iam_auth_options: raw.iam_auth_options,
        })
    }
}

impl fmt::Debug for PostgresConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConfig")
            .field("user", &self.user)
            .field("database", &self.database)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("unix_socket", &self.unix_socket)
            .field("sslmode", &self.sslmode)
            .field("ca_pem_len", &self.ca_pem.as_ref().map(Vec::len))
            .field(
                "client_cert_pem_len",
                &self.client_cert_pem.as_ref().map(Vec::len),
            )
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .field("server_name", &self.server_name)
            .field("channel_binding", &self.channel_binding)
            .field("connect_timeout", &self.connect_timeout)
            .field("application_name", &self.application_name)
            .field("session_gucs", &self.session_gucs)
            .field("keepalives", &self.keepalives)
            .field("keepalives_idle", &self.keepalives_idle)
            .field("keepalives_interval", &self.keepalives_interval)
            .field("keepalives_count", &self.keepalives_count)
            .field("cloud_sql_instance", &self.cloud_sql_instance)
            .field("cloud_sql_ip_type", &self.cloud_sql_ip_type)
            .field("alloydb_instance_uri", &self.alloydb_instance_uri)
            .field("alloydb_ip_type", &self.alloydb_ip_type)
            .field("enable_iam_auth", &self.enable_iam_auth)
            .field("cloud_auth_options", &"<redacted>")
            .field("iam_auth_options", &"<redacted>")
            .finish()
    }
}

fn validate_non_empty_no_nul(field: &str, value: &str) -> Result<(), PostgresError> {
    if value.is_empty() {
        return Err(PostgresError::Config(format!("{field} must not be empty")));
    }
    if value.contains('\0') {
        return Err(PostgresError::Config(format!(
            "{field} must not contain NUL bytes"
        )));
    }
    Ok(())
}

fn validate_ip_type(field: &str, value: &str) -> Result<(), PostgresError> {
    match value {
        "PUBLIC" | "PRIVATE" | "PSC" => Ok(()),
        other => Err(PostgresError::Config(format!(
            "unsupported {field} `{other}` (expected `PUBLIC`, `PRIVATE`, or `PSC`)"
        ))),
    }
}
