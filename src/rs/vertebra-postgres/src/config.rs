//! Pure in-memory PostgreSQL connection configuration validation and secret redaction.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::time::Duration;

use vertebra_db::connection::{TcpKeepaliveConfig, TlsMode};
use vertebra_db::dialer::{AlloyDbClusterUri, CloudSqlInstanceUri};

use crate::credentials::{CLOUDSQL_LOGIN_SCOPE, extract_explicit_scopes, has_static_token_key};
use crate::errors::PostgresError;
use crate::session::statements::PoolerMode;

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
    pub pooler_mode: String,
    pub protocol_level_pooler_cache: bool,
    pub statement_cache_size: i64,
}

impl RawPostgresOptions {
    /// Construct options with `host`, `user`, and `database`, using contract defaults for all other fields.
    pub fn new(
        host: impl Into<String>,
        user: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            user: user.into(),
            database: database.into(),
            ..Self::default()
        }
    }
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
            pooler_mode: "direct".to_string(),
            protocol_level_pooler_cache: false,
            statement_cache_size: 256,
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
    pub pooler_mode: PoolerMode,
    pub protocol_level_pooler_cache: bool,
    pub statement_cache_size: usize,
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
        if sslmode == TlsMode::Disable && channel_binding == ChannelBindingPolicy::Require {
            return Err(PostgresError::Config(
                "channel_binding='require' is incompatible with sslmode='disable'".to_string(),
            ));
        }

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

        validate_keepalive_bound("keepalives_idle", raw.keepalives_idle)?;
        validate_keepalive_bound("keepalives_interval", raw.keepalives_interval)?;
        validate_keepalive_bound("keepalives_count", raw.keepalives_count)?;
        if !raw.keepalives
            && (raw.keepalives_idle.is_some()
                || raw.keepalives_interval.is_some()
                || raw.keepalives_count.is_some())
        {
            return Err(PostgresError::Config(
                "keepalive tuning parameters cannot be set when keepalives is false".to_string(),
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
            let lower = k.to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "user" | "database" | "options" | "replication"
            ) {
                return Err(PostgresError::Config(format!(
                    "reserved startup parameter `{k}` must not be set in session_gucs"
                )));
            }
            if lower == "client_encoding" && !v.trim().eq_ignore_ascii_case("UTF8") {
                return Err(PostgresError::Config(format!(
                    "session_gucs client_encoding must be `UTF8`, got `{v}`"
                )));
            }
        }

        let cloud_sql_ip_type = normalize_ip_type("cloud_sql_ip_type", &raw.cloud_sql_ip_type)?;
        let alloydb_ip_type = normalize_ip_type("alloydb_ip_type", &raw.alloydb_ip_type)?;

        if let Some(ref inst) = raw.cloud_sql_instance {
            validate_non_empty_no_nul("cloud_sql_instance", inst)?;
            CloudSqlInstanceUri::parse(inst).map_err(|e| PostgresError::Config(e.to_string()))?;
        }
        if let Some(ref uri) = raw.alloydb_instance_uri {
            validate_non_empty_no_nul("alloydb_instance_uri", uri)?;
            AlloyDbClusterUri::parse(uri).map_err(|e| PostgresError::Config(e.to_string()))?;
        }

        let endpoint_count = u8::from(raw.unix_socket.is_some())
            + u8::from(raw.cloud_sql_instance.is_some())
            + u8::from(raw.alloydb_instance_uri.is_some());
        if endpoint_count > 1 {
            return Err(PostgresError::Config(
                "unix_socket, cloud_sql_instance, and alloydb_instance_uri are mutually exclusive"
                    .to_string(),
            ));
        }

        if raw.unix_socket.is_some() {
            if sslmode != TlsMode::Disable {
                return Err(PostgresError::Config(
                    "unix_socket requires sslmode='disable'".to_string(),
                ));
            }
            if raw.ca_pem.is_some()
                || raw.client_cert_pem.is_some()
                || raw.client_key_pem.is_some()
                || raw.server_name.is_some()
            {
                return Err(PostgresError::Config(
                    "unix_socket is incompatible with TLS certificate or server_name options"
                        .to_string(),
                ));
            }
            if channel_binding == ChannelBindingPolicy::Require {
                return Err(PostgresError::Config(
                    "unix_socket is incompatible with channel_binding='require'".to_string(),
                ));
            }
            if raw.keepalives_idle.is_some()
                || raw.keepalives_interval.is_some()
                || raw.keepalives_count.is_some()
            {
                return Err(PostgresError::Config(
                    "unix_socket is incompatible with TCP keepalive tuning parameters".to_string(),
                ));
            }
        }

        let requires_login_token = raw.enable_iam_auth || raw.alloydb_instance_uri.is_some();
        if requires_login_token
            && raw.iam_auth_options.is_empty()
            && has_static_token_key(&raw.cloud_auth_options)
        {
            return Err(PostgresError::Config(
                "opaque static access_token in cloud_auth_options cannot satisfy database IAM or AlloyDB login without designated iam_auth_options".to_string(),
            ));
        }

        let pooler_mode = PoolerMode::parse(&raw.pooler_mode)?;
        if raw.statement_cache_size < 0 {
            return Err(PostgresError::Config(format!(
                "statement_cache_size must be >= 0, got {}",
                raw.statement_cache_size
            )));
        }
        let statement_cache_size = usize::try_from(raw.statement_cache_size).map_err(|_| {
            PostgresError::Config("statement_cache_size overflowed usize".to_string())
        })?;

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
            cloud_sql_ip_type,
            alloydb_instance_uri: raw.alloydb_instance_uri,
            alloydb_ip_type,
            enable_iam_auth: raw.enable_iam_auth,
            cloud_auth_options: raw.cloud_auth_options,
            iam_auth_options: raw.iam_auth_options,
            pooler_mode,
            protocol_level_pooler_cache: raw.protocol_level_pooler_cache,
            statement_cache_size,
        })
    }

    /// Return the validated [`TcpKeepaliveConfig`] for physical TCP/TLS sockets.
    pub fn tcp_keepalive(&self) -> TcpKeepaliveConfig {
        TcpKeepaliveConfig {
            enabled: self.keepalives,
            idle: self.keepalives_idle.map(|s| Duration::from_secs(s as u64)),
            interval: self
                .keepalives_interval
                .map(|s| Duration::from_secs(s as u64)),
            retries: self.keepalives_count.map(|c| c as u32),
        }
    }

    /// Build the deterministic startup parameter map (`application_name` overlaid by `session_gucs`, excluding `client_encoding`).
    pub fn startup_gucs(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        out.insert(
            "application_name".to_string(),
            self.application_name.clone(),
        );
        for (k, v) in &self.session_gucs {
            if k.eq_ignore_ascii_case("client_encoding") {
                continue;
            }
            if k.eq_ignore_ascii_case("application_name") {
                out.insert("application_name".to_string(), v.clone());
            } else {
                out.insert(k.clone(), v.clone());
            }
        }
        out
    }

    /// Return the authoritative direct-host IAM OAuth scopes (`sqlservice.login` by default or explicit `iam_auth_options["scopes"]`).
    pub fn direct_iam_default_scopes(&self) -> Vec<String> {
        extract_explicit_scopes(&self.iam_auth_options)
            .unwrap_or_else(|| vec![CLOUDSQL_LOGIN_SCOPE.to_string()])
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

fn validate_keepalive_bound(field: &str, value: Option<i64>) -> Result<(), PostgresError> {
    if let Some(v) = value
        && !(1..=i64::from(i32::MAX)).contains(&v)
    {
        return Err(PostgresError::Config(format!(
            "{field} must lie in 1..={}, got {v}",
            i32::MAX
        )));
    }
    Ok(())
}

fn normalize_ip_type(field: &str, value: &str) -> Result<String, PostgresError> {
    match value.trim().to_ascii_uppercase().as_str() {
        "PUBLIC" => Ok("PUBLIC".to_string()),
        "PRIVATE" => Ok("PRIVATE".to_string()),
        "PSC" => Ok("PSC".to_string()),
        _ => Err(PostgresError::Config(format!(
            "unsupported {field} `{value}` (expected `PUBLIC`, `PRIVATE`, or `PSC`)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_option_validation() {
        let mut unix_tls = RawPostgresOptions::new("localhost", "postgres", "appdb");
        unix_tls.unix_socket = Some("/tmp/.s.PGSQL.5432".to_string());
        unix_tls.sslmode = "prefer".to_string();
        assert!(matches!(
            PostgresConfig::validate(unix_tls),
            Err(PostgresError::Config(_))
        ));

        let mut unix_ca = RawPostgresOptions::new("localhost", "postgres", "appdb");
        unix_ca.unix_socket = Some("/tmp/.s.PGSQL.5432".to_string());
        unix_ca.sslmode = "disable".to_string();
        unix_ca.ca_pem = Some(b"-----BEGIN CERTIFICATE-----\n".to_vec());
        assert!(matches!(
            PostgresConfig::validate(unix_ca),
            Err(PostgresError::Config(_))
        ));

        let mut unix_ka = RawPostgresOptions::new("localhost", "postgres", "appdb");
        unix_ka.unix_socket = Some("/tmp/.s.PGSQL.5432".to_string());
        unix_ka.sslmode = "disable".to_string();
        unix_ka.keepalives_idle = Some(10);
        assert!(matches!(
            PostgresConfig::validate(unix_ka),
            Err(PostgresError::Config(_))
        ));

        let mut unix_cb = RawPostgresOptions::new("localhost", "postgres", "appdb");
        unix_cb.unix_socket = Some("/tmp/.s.PGSQL.5432".to_string());
        unix_cb.sslmode = "disable".to_string();
        unix_cb.channel_binding = "require".to_string();
        assert!(matches!(
            PostgresConfig::validate(unix_cb),
            Err(PostgresError::Config(_))
        ));

        let mut both_cloud = RawPostgresOptions::new("localhost", "postgres", "appdb");
        both_cloud.cloud_sql_instance = Some("proj:us-central1:inst".to_string());
        both_cloud.alloydb_instance_uri =
            Some("projects/p/locations/r/clusters/c/instances/i".to_string());
        assert!(matches!(
            PostgresConfig::validate(both_cloud),
            Err(PostgresError::Config(_))
        ));

        let mut opaque_admin_iam = RawPostgresOptions::new("localhost", "postgres", "appdb");
        opaque_admin_iam.cloud_sql_instance = Some("proj:us-central1:inst".to_string());
        opaque_admin_iam.enable_iam_auth = true;
        opaque_admin_iam
            .cloud_auth_options
            .insert("access_token".to_string(), "opaque".to_string());
        assert!(matches!(
            PostgresConfig::validate(opaque_admin_iam),
            Err(PostgresError::Config(_))
        ));

        for bad_guc in [
            ("client_encoding", "LATIN1"),
            ("replication", "database"),
            ("user", "other"),
            ("database", "other"),
            ("options", "-c work_mem=4MB"),
        ] {
            let mut guc_raw = RawPostgresOptions::new("localhost", "postgres", "appdb");
            guc_raw
                .session_gucs
                .insert(bad_guc.0.to_string(), bad_guc.1.to_string());
            assert!(matches!(
                PostgresConfig::validate(guc_raw),
                Err(PostgresError::Config(_))
            ));
        }

        let mut valid = RawPostgresOptions::new("localhost", "postgres", "appdb");
        valid.application_name = "vtb-base".to_string();
        valid
            .session_gucs
            .insert("application_name".to_string(), "vtb-override".to_string());
        valid
            .session_gucs
            .insert("app.role".to_string(), "reader one".to_string());
        valid
            .session_gucs
            .insert("client_encoding".to_string(), "UTF8".to_string());
        let cfg = PostgresConfig::validate(valid).expect("valid option overlay");
        let startup = cfg.startup_gucs();
        assert_eq!(
            startup.get("application_name").map(String::as_str),
            Some("vtb-override")
        );
        assert_eq!(
            startup.get("app.role").map(String::as_str),
            Some("reader one")
        );
        assert!(!startup.contains_key("client_encoding"));
    }
}
