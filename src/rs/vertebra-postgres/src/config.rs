//! Pure constructor validation for owned native PostgreSQL options.

use std::{collections::HashMap, fmt, time::Duration};
use vertebra_db::connection::TlsMode;

use crate::errors::PostgresError;

/// SCRAM channel-binding preference.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ChannelBindingPolicy {
    Disable,
    #[default]
    Prefer,
    Require,
}

impl ChannelBindingPolicy {
    /// Parse an exact documented spelling without normalizing caller input.
    pub fn parse(raw: &str) -> Result<Self, PostgresError> {
        match raw {
            "disable" => Ok(Self::Disable),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            _ => Err(PostgresError::Config("unsupported channel_binding".into())),
        }
    }
}

/// Parse only the five native TLS modes, excluding shared-transport aliases.
pub fn parse_strict_sslmode(raw: &str) -> Result<TlsMode, PostgresError> {
    match raw {
        "disable" => Ok(TlsMode::Disable),
        "prefer" => Ok(TlsMode::Prefer),
        "require" => Ok(TlsMode::Require),
        "verify-ca" => Ok(TlsMode::VerifyCa),
        "verify-full" => Ok(TlsMode::VerifyFull),
        _ => Err(PostgresError::Config("unsupported sslmode".into())),
    }
}

/// Unvalidated primitive options copied across the native boundary.
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
            host: "localhost".into(),
            port: 5432,
            password: None,
            unix_socket: None,
            sslmode: "verify-full".into(),
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            server_name: None,
            channel_binding: "prefer".into(),
            connect_timeout: 10.0,
            application_name: "vertebra".into(),
            session_gucs: HashMap::new(),
            keepalives: true,
            keepalives_idle: None,
            keepalives_interval: None,
            keepalives_count: None,
            cloud_sql_instance: None,
            cloud_sql_ip_type: "PRIVATE".into(),
            alloydb_instance_uri: None,
            alloydb_ip_type: "PRIVATE".into(),
            enable_iam_auth: false,
            cloud_auth_options: HashMap::new(),
            iam_auth_options: HashMap::new(),
        }
    }
}

/// Owned validated constructor options; construction performs no I/O.
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
    /// Validate caller-owned values before transport or authentication begins.
    pub fn validate(raw: RawPostgresOptions) -> Result<Self, PostgresError> {
        for (name, value) in [
            ("user", raw.user.as_str()),
            ("database", &raw.database),
            ("host", &raw.host),
        ] {
            validate_text(name, value, true)?;
        }
        if let Some(value) = &raw.server_name {
            validate_text("server_name", value, true)?;
        }
        if let Some(value) = &raw.password {
            validate_text("password", value, false)?;
        }
        validate_text("application_name", &raw.application_name, false)?;
        if raw.host.contains("://") || raw.host.contains('/') {
            return Err(PostgresError::Config(
                "host must be a TCP hostname or address".into(),
            ));
        }
        if !(1..=65535).contains(&raw.port) {
            return Err(PostgresError::Config("port must be in 1..=65535".into()));
        }
        if !raw.connect_timeout.is_finite() || raw.connect_timeout <= 0.0 {
            return Err(PostgresError::Config(
                "connect_timeout must be finite and positive".into(),
            ));
        }
        let connect_timeout = Duration::try_from_secs_f64(raw.connect_timeout).map_err(|_| {
            PostgresError::Config("connect_timeout exceeds supported duration".into())
        })?;
        for (name, bytes) in [
            ("ca_pem", &raw.ca_pem),
            ("client_cert_pem", &raw.client_cert_pem),
            ("client_key_pem", &raw.client_key_pem),
        ] {
            if bytes.as_ref().is_some_and(Vec::is_empty) {
                return Err(PostgresError::Config(format!("{name} must not be empty")));
            }
        }
        if raw.client_cert_pem.is_some() != raw.client_key_pem.is_some() {
            return Err(PostgresError::Config(
                "client_cert_pem and client_key_pem must be supplied together".into(),
            ));
        }
        let sslmode = parse_strict_sslmode(&raw.sslmode)?;
        let channel_binding = ChannelBindingPolicy::parse(&raw.channel_binding)?;
        Ok(Self {
            user: raw.user,
            database: raw.database,
            host: raw.host,
            port: raw.port as u16,
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

fn validate_text(name: &str, value: &str, required: bool) -> Result<(), PostgresError> {
    if (required && value.is_empty()) || value.contains('\0') {
        return Err(PostgresError::Config(format!(
            "{name} must be {}NUL-free",
            if required { "nonempty and " } else { "" }
        )));
    }
    Ok(())
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
            .field("ca_pem", &self.ca_pem)
            .field("client_cert_pem", &self.client_cert_pem)
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
