//! MySQL and MariaDB connection configuration, TLS modes, DSN parsing, and Cloud SQL / IAM wiring.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use mysql_async::{ClientIdentity, Opts, OptsBuilder, PoolConstraints, PoolOpts, SslOpts};
use url::Url;
use vertebra_core::gcp_auth::GcpTokenCache;
use vertebra_db::dialer::{AuthType, CloudSqlDialer, CloudSqlInstanceUri, IpType};

use crate::errors::MySQLError;

const CLOUD_SQL_IAM_SCOPE: &str = "https://www.googleapis.com/auth/sqlservice.login";

/// TLS negotiation policy for MySQL and MariaDB connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySQLSslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl MySQLSslMode {
    pub fn parse(value: &str) -> Result<Self, MySQLError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "disable" | "disabled" | "false" | "0" | "off" | "none" => Ok(Self::Disable),
            "prefer" | "preferred" => Ok(Self::Prefer),
            "require" | "required" | "true" | "1" | "on" => Ok(Self::Require),
            "verify-ca" | "verify_ca" => Ok(Self::VerifyCa),
            "verify-full" | "verify_full" | "verify-identity" | "verify_identity" => {
                Ok(Self::VerifyFull)
            }
            other => Err(MySQLError::config(format!(
                "Unsupported MySQL ssl_mode '{other}'; expected disable, prefer, require, verify-ca, or verify-full"
            ))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify-ca",
            Self::VerifyFull => "verify-full",
        }
    }
}

/// Validated configuration for [`crate::client::SyncMySQLClient`] and [`crate::client::AsyncMySQLClient`].
#[derive(Clone)]
pub struct MySQLConnectConfig {
    pub dsn: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub unix_socket: Option<String>,
    pub ssl_mode: MySQLSslMode,
    pub ssl_ca: Option<String>,
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub pool_min_size: usize,
    pub pool_max_size: usize,
    pub statement_cache_size: usize,
    pub charset: String,
    pub autocommit: bool,
    pub init_command: Option<String>,
    pub cloud_sql_instance: Option<String>,
    pub cloud_sql_ip_type: IpType,
    pub enable_iam_auth: bool,
    pub credentials_path: Option<String>,
    pub credentials_json: Option<String>,
    pub target_service_account: Option<String>,
    token_cache: Option<Arc<GcpTokenCache>>,
}

impl std::fmt::Debug for MySQLConnectConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySQLConnectConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("ssl_mode", &self.ssl_mode)
            .field("pool_min_size", &self.pool_min_size)
            .field("pool_max_size", &self.pool_max_size)
            .field("statement_cache_size", &self.statement_cache_size)
            .field("cloud_sql_instance", &self.cloud_sql_instance)
            .field("enable_iam_auth", &self.enable_iam_auth)
            .finish()
    }
}

impl std::fmt::Display for MySQLConnectConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.redacted_repr("MySQLConnectConfig"))
    }
}

#[allow(clippy::too_many_arguments)]
impl MySQLConnectConfig {
    pub fn from_dsn_and_kwargs(
        dsn: &str,
        host: Option<String>,
        port: Option<u16>,
        user: Option<String>,
        password: Option<String>,
        database: Option<String>,
        unix_socket: Option<String>,
        ssl_mode: Option<&str>,
        ssl_ca: Option<String>,
        ssl_cert: Option<String>,
        ssl_key: Option<String>,
        connect_timeout_ms: u64,
        read_timeout_ms: u64,
        write_timeout_ms: u64,
        pool_min_size: usize,
        pool_max_size: usize,
        statement_cache_size: usize,
        charset: &str,
        autocommit: bool,
        init_command: Option<String>,
        cloud_sql_instance: Option<String>,
        cloud_sql_ip_type: &str,
        enable_iam_auth: bool,
        credentials_path: Option<String>,
        credentials_json: Option<String>,
        target_service_account: Option<String>,
    ) -> Result<Self, MySQLError> {
        let trimmed_dsn = dsn.trim();
        let lower_dsn = trimmed_dsn.to_ascii_lowercase();
        if trimmed_dsn.is_empty() {
            return Err(MySQLError::config("MySQL DSN must not be empty"));
        }
        if dsn.contains('\0') {
            return Err(MySQLError::config("MySQL DSN must not contain NUL bytes"));
        }
        if port == Some(0) {
            return Err(MySQLError::config("MySQL port must be in 1..=65535"));
        }
        for (field_name, field_val) in [
            ("host", host.as_deref()),
            ("user", user.as_deref()),
            ("database", database.as_deref()),
            ("unix_socket", unix_socket.as_deref()),
            ("ssl_ca", ssl_ca.as_deref()),
            ("ssl_cert", ssl_cert.as_deref()),
            ("ssl_key", ssl_key.as_deref()),
            ("init_command", init_command.as_deref()),
            ("cloud_sql_instance", cloud_sql_instance.as_deref()),
        ] {
            if field_val.is_some_and(|v| v.contains('\0')) {
                return Err(MySQLError::config(format!(
                    "MySQL {field_name} must not contain NUL bytes"
                )));
            }
        }
        if lower_dsn == "memory"
            || lower_dsn == ":memory:"
            || lower_dsn.starts_with("memory://")
            || lower_dsn.contains("://memory")
            || lower_dsn.contains("/:memory:")
            || database.as_deref().is_some_and(|db| {
                db.eq_ignore_ascii_case(":memory:") || db.eq_ignore_ascii_case("memory")
            })
        {
            return Err(MySQLError::config(
                "MySQL does not support in-memory ('memory://') execution; configure a MySQL or MariaDB server endpoint",
            ));
        }

        let mut resolved_host = "127.0.0.1".to_string();
        let mut resolved_port = 3306u16;
        let mut resolved_user = "root".to_string();
        let mut resolved_password: Option<String> = None;
        let mut resolved_database = String::new();
        let mut resolved_socket = unix_socket;
        let mut resolved_ssl_mode: Option<MySQLSslMode> = None;
        let mut resolved_ssl_ca = ssl_ca;
        let mut resolved_ssl_cert = ssl_cert;
        let mut resolved_ssl_key = ssl_key;
        let mut resolved_cloud_sql = cloud_sql_instance;
        let mut resolved_iam_auth = enable_iam_auth;

        if let Some(rest) = trimmed_dsn.strip_prefix("cloudsql://") {
            let (instance_part, db_part) = match rest.split_once('/') {
                Some((inst, db)) => (inst, db),
                None => (rest, ""),
            };
            let (db_clean, query_str) = match db_part.split_once('?') {
                Some((db, q)) => (db, Some(q)),
                None => (db_part, None),
            };
            if !instance_part.is_empty() {
                resolved_cloud_sql = Some(instance_part.to_string());
            }
            if !db_clean.is_empty() {
                resolved_database = db_clean.to_string();
            }
            if let Some(q) = query_str {
                for (k, v) in url::form_urlencoded::parse(q.as_bytes()) {
                    apply_query_param(
                        &k,
                        &v,
                        &mut resolved_socket,
                        &mut resolved_ssl_mode,
                        &mut resolved_ssl_ca,
                        &mut resolved_ssl_cert,
                        &mut resolved_ssl_key,
                        &mut resolved_cloud_sql,
                        &mut resolved_iam_auth,
                    )?;
                }
            }
        } else {
            let normalized = normalize_mysql_scheme(trimmed_dsn)?;
            let parsed = Url::parse(&normalized).map_err(|e| {
                MySQLError::config(format!("Invalid MySQL DSN '{trimmed_dsn}': {e}"))
            })?;

            if let Some(h) = parsed.host_str()
                && !h.is_empty()
            {
                if h.eq_ignore_ascii_case("memory") {
                    return Err(MySQLError::config(
                        "MySQL does not support in-memory ('memory://') execution; configure a MySQL or MariaDB server endpoint",
                    ));
                }
                resolved_host = h.to_string();
            }
            if let Some(p) = parsed.port() {
                resolved_port = p;
            }
            if !parsed.username().is_empty() {
                resolved_user = percent_decode_str(parsed.username());
            }
            if let Some(pw) = parsed.password() {
                resolved_password = Some(percent_decode_str(pw));
            }
            let path_db = parsed.path().trim_start_matches('/');
            if !path_db.is_empty() {
                if path_db.eq_ignore_ascii_case(":memory:") {
                    return Err(MySQLError::config(
                        "MySQL does not support in-memory ('memory://') execution; configure a MySQL or MariaDB server endpoint",
                    ));
                }
                resolved_database = percent_decode_str(path_db);
            }

            for (k, v) in parsed.query_pairs() {
                apply_query_param(
                    &k,
                    &v,
                    &mut resolved_socket,
                    &mut resolved_ssl_mode,
                    &mut resolved_ssl_ca,
                    &mut resolved_ssl_cert,
                    &mut resolved_ssl_key,
                    &mut resolved_cloud_sql,
                    &mut resolved_iam_auth,
                )?;
            }
        }

        if let Some(h) = host.filter(|s| !s.is_empty()) {
            resolved_host = h;
        }
        if let Some(p) = port.filter(|&p| p > 0) {
            resolved_port = p;
        }
        if let Some(u) = user.filter(|s| !s.is_empty()) {
            resolved_user = u;
        }
        if password.is_some() {
            resolved_password = password;
        }
        if let Some(db) = database.filter(|s| !s.is_empty()) {
            resolved_database = db;
        }
        if let Some(mode_str) = ssl_mode {
            resolved_ssl_mode = Some(MySQLSslMode::parse(mode_str)?);
        }

        let final_ssl_mode = resolved_ssl_mode.unwrap_or(MySQLSslMode::Prefer);
        if let Some(ref ca_val) = resolved_ssl_ca {
            if is_inline_pem(ca_val) {
                validate_inline_pem_cert(ca_val, "ssl_ca")?;
            } else if matches!(
                final_ssl_mode,
                MySQLSslMode::VerifyCa | MySQLSslMode::VerifyFull
            ) && !PathBuf::from(ca_val).exists()
            {
                return Err(MySQLError::config(format!(
                    "MySQL ssl_ca file does not exist: '{ca_val}'"
                )));
            }
        }
        if let Some(ref cert_val) = resolved_ssl_cert
            && is_inline_pem(cert_val)
        {
            validate_inline_pem_cert(cert_val, "ssl_cert")?;
        }
        if let Some(ref key_val) = resolved_ssl_key
            && is_inline_pem(key_val)
        {
            validate_inline_pem_key(key_val, "ssl_key")?;
        }

        if let Some(ref inst) = resolved_cloud_sql {
            CloudSqlInstanceUri::parse(inst).map_err(|e| {
                MySQLError::config(format!("Invalid Cloud SQL instance URI '{inst}': {e}"))
            })?;
        }

        let parsed_ip_type = match cloud_sql_ip_type.trim().to_ascii_uppercase().as_str() {
            "PUBLIC" | "" => IpType::Public,
            "PRIVATE" => IpType::Private,
            "PSC" => IpType::Psc,
            other => {
                return Err(MySQLError::config(format!(
                    "Unsupported Cloud SQL ip_type '{other}'; expected PUBLIC, PRIVATE, or PSC"
                )));
            }
        };

        if pool_min_size == 0 || pool_max_size < pool_min_size {
            return Err(MySQLError::config(format!(
                "Invalid MySQL pool bounds: pool_min_size={pool_min_size}, pool_max_size={pool_max_size}"
            )));
        }
        let max_size = pool_max_size;
        let min_size = pool_min_size;

        let token_cache = if resolved_iam_auth || resolved_cloud_sql.is_some() {
            let mut opts = HashMap::new();
            if let Some(ref p) = credentials_path {
                opts.insert("service_account_path".to_string(), p.clone());
            }
            if let Some(ref j) = credentials_json {
                opts.insert("service_account_key".to_string(), j.clone());
            }
            if let Some(ref sa) = target_service_account {
                opts.insert("target_service_account".to_string(), sa.clone());
            }
            GcpTokenCache::from_options(&opts, &[CLOUD_SQL_IAM_SCOPE])
        } else {
            None
        };

        Ok(Self {
            dsn: trimmed_dsn.to_string(),
            host: resolved_host,
            port: resolved_port,
            user: resolved_user,
            password: resolved_password,
            database: resolved_database,
            unix_socket: resolved_socket,
            ssl_mode: final_ssl_mode,
            ssl_ca: resolved_ssl_ca,
            ssl_cert: resolved_ssl_cert,
            ssl_key: resolved_ssl_key,
            connect_timeout_ms: connect_timeout_ms.max(1),
            read_timeout_ms: read_timeout_ms.max(1),
            write_timeout_ms: write_timeout_ms.max(1),
            pool_min_size: min_size,
            pool_max_size: max_size,
            statement_cache_size,
            charset: if charset.is_empty() {
                "utf8mb4".to_string()
            } else {
                charset.to_string()
            },
            autocommit,
            init_command,
            cloud_sql_instance: resolved_cloud_sql,
            cloud_sql_ip_type: parsed_ip_type,
            enable_iam_auth: resolved_iam_auth,
            credentials_path,
            credentials_json,
            target_service_account,
            token_cache,
        })
    }

    /// Resolves the effective password, refreshing an OAuth2 IAM access token when
    /// `enable_iam_auth` is enabled and no explicit override password is set.
    pub async fn resolve_effective_password(&self) -> Result<Option<String>, MySQLError> {
        if self.enable_iam_auth {
            if let Some(ref pw) = self.password
                && !pw.is_empty()
            {
                return Ok(Some(pw.clone()));
            }
            let cache = self.token_cache.as_ref().ok_or_else(|| {
                MySQLError::config(
                    "enable_iam_auth=True requires a configured GcpTokenCache or explicit IAM token password",
                )
            })?;
            let snapshot = cache.get_or_refresh().await.map_err(|e| {
                MySQLError::connection(format!("Failed to acquire GCP IAM database token: {e}"))
            })?;
            return Ok(Some(snapshot.access_token.clone()));
        }
        Ok(self.password.clone())
    }

    /// Builds a [`CloudSqlDialer`] when `cloud_sql_instance` is configured.
    pub fn build_cloud_sql_dialer(&self) -> Result<Option<CloudSqlDialer>, MySQLError> {
        let Some(ref inst_str) = self.cloud_sql_instance else {
            return Ok(None);
        };
        let auth_type = if self.enable_iam_auth {
            AuthType::Iam
        } else {
            AuthType::Password
        };
        let dialer =
            CloudSqlDialer::new(inst_str, self.cloud_sql_ip_type, auth_type).map_err(|e| {
                MySQLError::config(format!("Invalid Cloud SQL instance '{inst_str}': {e}"))
            })?;
        Ok(Some(dialer))
    }

    /// Constructs `mysql_async::Opts` for connecting to `(override_host, override_port)` or
    /// the configured endpoint.
    pub fn build_mysql_opts(
        &self,
        override_host_port: Option<(&str, u16)>,
        effective_password: Option<String>,
        include_pool_constraints: bool,
    ) -> Result<Opts, MySQLError> {
        vertebra_db::install_rustls_ring_provider();

        let (target_host, target_port, force_disable_tls) = match override_host_port {
            Some((h, p)) => (h, p, true),
            None => (self.host.as_str(), self.port, false),
        };

        let mut builder = OptsBuilder::default()
            .ip_or_hostname(target_host)
            .tcp_port(target_port)
            .user(Some(self.user.clone()))
            .pass(effective_password)
            .db_name(if self.database.is_empty() {
                None
            } else {
                Some(self.database.clone())
            })
            .stmt_cache_size(self.statement_cache_size)
            .wait_timeout(Some(
                (self.read_timeout_ms.max(self.write_timeout_ms) / 1000).max(1) as usize,
            ));

        if override_host_port.is_none()
            && let Some(ref sock) = self.unix_socket
            && !sock.is_empty()
        {
            builder = builder.socket(Some(sock.clone())).prefer_socket(true);
        } else {
            builder = builder.prefer_socket(false);
        }

        let mut init_queries = Vec::new();
        if !self.autocommit {
            init_queries.push("SET autocommit = 0".to_string());
        }
        if let Some(ref init_cmd) = self.init_command
            && !init_cmd.trim().is_empty()
        {
            init_queries.push(init_cmd.clone());
        }
        if !init_queries.is_empty() {
            builder = builder.init(init_queries);
        }

        let ssl_opts = if force_disable_tls {
            None
        } else {
            self.build_ssl_opts()?
        };
        builder = builder.ssl_opts(ssl_opts);

        if include_pool_constraints {
            let constraints = PoolConstraints::new(self.pool_min_size, self.pool_max_size)
                .unwrap_or(PoolConstraints::new(1, 10).expect("valid default constraints"));
            let pool_opts = PoolOpts::default()
                .with_constraints(constraints)
                .with_reset_connection(false);
            builder = builder.pool_opts(pool_opts);
        } else {
            let constraints =
                PoolConstraints::new(0, 1).expect("single-connection control pool constraints");
            let pool_opts = PoolOpts::default()
                .with_constraints(constraints)
                .with_reset_connection(false);
            builder = builder.pool_opts(pool_opts);
        }

        Ok(Opts::from(builder))
    }

    fn build_ssl_opts(&self) -> Result<Option<SslOpts>, MySQLError> {
        match self.ssl_mode {
            MySQLSslMode::Disable => Ok(None),
            MySQLSslMode::Prefer => {
                if self.ssl_ca.is_none() && self.ssl_cert.is_none() && is_local_host(&self.host) {
                    Ok(None)
                } else {
                    Ok(Some(self.configure_ssl_opts(true, true)?))
                }
            }
            MySQLSslMode::Require => Ok(Some(self.configure_ssl_opts(true, true)?)),
            MySQLSslMode::VerifyCa => Ok(Some(self.configure_ssl_opts(false, true)?)),
            MySQLSslMode::VerifyFull => Ok(Some(self.configure_ssl_opts(false, false)?)),
        }
    }

    fn configure_ssl_opts(
        &self,
        accept_invalid_certs: bool,
        skip_domain_validation: bool,
    ) -> Result<SslOpts, MySQLError> {
        let mut ssl = SslOpts::default()
            .with_danger_accept_invalid_certs(accept_invalid_certs)
            .with_danger_skip_domain_validation(skip_domain_validation);

        if let Some(ref ca) = self.ssl_ca {
            let root_cert = if is_inline_pem(ca) {
                inline_pem_bytes(ca).into()
            } else {
                PathBuf::from(ca).into()
            };
            ssl = ssl
                .with_root_certs(vec![root_cert])
                .with_disable_built_in_roots(true);
        }

        if let (Some(cert), Some(key)) = (&self.ssl_cert, &self.ssl_key) {
            let identity = ClientIdentity::new(
                if is_inline_pem(cert) {
                    inline_pem_bytes(cert).into()
                } else {
                    PathBuf::from(cert).into()
                },
                if is_inline_pem(key) {
                    inline_pem_bytes(key).into()
                } else {
                    PathBuf::from(key).into()
                },
            );
            ssl = ssl.with_client_identity(Some(identity));
        }

        if !skip_domain_validation && !self.host.is_empty() {
            ssl = ssl.with_danger_tls_hostname_override(Some(Cow::Owned(self.host.clone())));
        }

        Ok(ssl)
    }

    pub fn redacted_repr(&self, class_name: &str) -> String {
        format!(
            "{class_name}(host={:?}, port={}, user={:?}, database={:?}, ssl_mode={:?}, password='***')",
            self.host,
            self.port,
            self.user,
            self.database,
            self.ssl_mode.as_str(),
        )
    }
}

/// Returns true when a TLS option carries PEM text instead of a filesystem path.
fn is_inline_pem(value: &str) -> bool {
    value.trim_start().starts_with("-----BEGIN")
}

/// Returns the in-memory bytes of an inline PEM TLS option without leading whitespace.
fn inline_pem_bytes(value: &str) -> Vec<u8> {
    value.trim_start().as_bytes().to_vec()
}

fn validate_inline_pem_cert(pem: &str, field: &str) -> Result<(), MySQLError> {
    let mut reader = std::io::BufReader::new(pem.trim_start().as_bytes());
    let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut reader).collect();
    match certs {
        Ok(list) if !list.is_empty() => Ok(()),
        Ok(_) => Err(MySQLError::config(format!(
            "Invalid MySQL {field}: no PEM certificates found"
        ))),
        Err(e) => Err(MySQLError::config(format!(
            "Invalid MySQL {field} PEM certificate: {e}"
        ))),
    }
}

fn validate_inline_pem_key(pem: &str, field: &str) -> Result<(), MySQLError> {
    let mut reader = std::io::BufReader::new(pem.trim_start().as_bytes());
    match rustls_pemfile::private_key(&mut reader) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(MySQLError::config(format!(
            "Invalid MySQL {field}: no PEM private key found"
        ))),
        Err(e) => Err(MySQLError::config(format!(
            "Invalid MySQL {field} PEM private key: {e}"
        ))),
    }
}

fn is_local_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

fn normalize_mysql_scheme(dsn: &str) -> Result<String, MySQLError> {
    let Some((scheme, rest)) = dsn.split_once("://") else {
        return Err(MySQLError::config(format!(
            "MySQL DSN '{dsn}' is missing a URL scheme (expected mysql:// or mariadb://)"
        )));
    };
    let lower = scheme.to_ascii_lowercase();
    match lower.as_str() {
        "mysql"
        | "mariadb"
        | "mysql+vtb"
        | "mysql+vertebra"
        | "mysql+vtb_async"
        | "mysql+vertebra_async"
        | "mysql+pymysql"
        | "mysql+asyncmy"
        | "mariadb+vtb"
        | "mariadb+vertebra" => Ok(format!("mysql://{rest}")),
        other => Err(MySQLError::config(format!(
            "Unsupported MySQL URL scheme '{other}://'"
        ))),
    }
}

fn percent_decode_str(input: &str) -> String {
    url::form_urlencoded::parse(format!("v={input}").as_bytes())
        .next()
        .map(|(_, val)| val.into_owned())
        .unwrap_or_else(|| input.to_string())
}

#[allow(clippy::too_many_arguments)]
fn apply_query_param(
    key: &str,
    value: &str,
    unix_socket: &mut Option<String>,
    ssl_mode: &mut Option<MySQLSslMode>,
    ssl_ca: &mut Option<String>,
    ssl_cert: &mut Option<String>,
    ssl_key: &mut Option<String>,
    cloud_sql_instance: &mut Option<String>,
    enable_iam_auth: &mut bool,
) -> Result<(), MySQLError> {
    match key.to_ascii_lowercase().as_str() {
        "unix_socket" | "socket" => {
            if let Some(inst) = value.strip_prefix("/cloudsql/") {
                *cloud_sql_instance = Some(inst.to_string());
            } else {
                *unix_socket = Some(value.to_string());
            }
        }
        "ssl-mode" | "ssl_mode" | "sslmode" | "ssl" => {
            *ssl_mode = Some(MySQLSslMode::parse(value)?);
        }
        "ssl-ca" | "ssl_ca" | "sslca" => {
            *ssl_ca = Some(value.to_string());
        }
        "ssl-cert" | "ssl_cert" | "sslcert" => {
            *ssl_cert = Some(value.to_string());
        }
        "ssl-key" | "ssl_key" | "sslkey" => {
            *ssl_key = Some(value.to_string());
        }
        "cloud_sql_instance" | "cloudsql_instance" => {
            *cloud_sql_instance = Some(value.to_string());
        }
        "enable_iam_auth" => {
            *enable_iam_auth = matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            );
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rejects_memory_dsn_and_database() {
        for bad in [
            "mysql://memory",
            "mysql:///:memory:",
            "mysql://root@localhost/:memory:",
            "memory://",
        ] {
            let err = MySQLConnectConfig::from_dsn_and_kwargs(
                bad, None, None, None, None, None, None, None, None, None, None, 5000, 5000, 5000,
                1, 5, 64, "utf8mb4", true, None, None, "PUBLIC", false, None, None, None,
            )
            .unwrap_err();
            assert_eq!(
                err.kind,
                crate::errors::MySQLErrorKind::ImproperConfiguration
            );
        }
    }

    #[test]
    fn test_parses_cloudsql_uri_and_tls_modes() {
        let cfg = MySQLConnectConfig::from_dsn_and_kwargs(
            "cloudsql://proj:us-central1:inst/app_db?ssl_mode=verify-ca&enable_iam_auth=true",
            None,
            None,
            Some("iam-user".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            5000,
            5000,
            5000,
            2,
            8,
            128,
            "utf8mb4",
            true,
            None,
            None,
            "PRIVATE",
            false,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            cfg.cloud_sql_instance.as_deref(),
            Some("proj:us-central1:inst")
        );
        assert_eq!(cfg.database, "app_db");
        assert_eq!(cfg.user, "iam-user");
        assert_eq!(cfg.ssl_mode, MySQLSslMode::VerifyCa);
        assert!(cfg.enable_iam_auth);
        assert_eq!(cfg.cloud_sql_ip_type, IpType::Private);
    }

    const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIUQ3Jvc3NDaGVja0NBMAoGCCqGSM49BAMCMA0xCzAJBgNV\n-----END CERTIFICATE-----\n";

    fn tls_config(ssl_mode: &str, ssl_ca: Option<String>) -> MySQLConnectConfig {
        MySQLConnectConfig::from_dsn_and_kwargs(
            "mysql://app@db.example.internal:3306/app",
            None,
            None,
            None,
            None,
            None,
            None,
            Some(ssl_mode),
            ssl_ca,
            None,
            None,
            5000,
            5000,
            5000,
            1,
            4,
            64,
            "utf8mb4",
            true,
            None,
            None,
            "PUBLIC",
            false,
            None,
            None,
            None,
        )
        .unwrap()
    }

    fn read_root_cert(cfg: &MySQLConnectConfig) -> std::io::Result<Vec<u8>> {
        let ssl = cfg.build_ssl_opts().unwrap().unwrap();
        assert!(ssl.disable_built_in_roots());
        assert_eq!(ssl.root_certs().len(), 1);
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                ssl.root_certs()[0]
                    .read()
                    .await
                    .map(|data| data.into_owned())
            })
    }

    #[test]
    fn test_inline_pem_ssl_ca_is_loaded_from_memory() {
        let large_pem = TEST_CA_PEM.repeat(600);
        assert!(large_pem.len() > 4096);
        for mode in ["require", "verify-ca", "verify-full"] {
            for pem in [
                TEST_CA_PEM.to_string(),
                format!("\n  {TEST_CA_PEM}"),
                large_pem.clone(),
            ] {
                let cfg = tls_config(mode, Some(pem.clone()));
                let loaded = read_root_cert(&cfg).unwrap();
                assert_eq!(loaded, pem.trim_start().as_bytes());
            }
        }
    }

    #[test]
    fn test_path_ssl_ca_is_read_from_disk() {
        let path = std::env::temp_dir().join(format!(
            "vtb-mysql-ssl-ca-{}-{:?}.pem",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, TEST_CA_PEM).unwrap();
        let cfg = tls_config("verify-full", Some(path.to_string_lossy().into_owned()));
        let loaded = read_root_cert(&cfg);
        std::fs::remove_file(&path).unwrap();
        assert_eq!(loaded.unwrap(), TEST_CA_PEM.as_bytes());
    }

    #[test]
    fn test_ssl_modes_map_to_verification_flags() {
        for (mode, accept_invalid, skip_domain) in [
            ("require", true, true),
            ("verify-ca", false, true),
            ("verify-full", false, false),
        ] {
            let ssl = tls_config(mode, Some(TEST_CA_PEM.to_string()))
                .build_ssl_opts()
                .unwrap()
                .unwrap();
            assert_eq!(ssl.accept_invalid_certs(), accept_invalid, "{mode}");
            assert_eq!(ssl.skip_domain_validation(), skip_domain, "{mode}");
        }
        assert!(
            tls_config("disable", None)
                .build_ssl_opts()
                .unwrap()
                .is_none()
        );
    }
}
