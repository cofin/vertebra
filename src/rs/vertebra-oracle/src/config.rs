//! Pure in-memory configuration validation and secret redaction for `vertebra-oracle`.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use crate::errors::OracleError;

/// Default Oracle DSN when no endpoint overrides are supplied.
pub const DEFAULT_ORACLE_DSN: &str = "oracle://localhost:1521/FREEPDB1";

/// Authentication mode for an Oracle Thin connection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OracleAuthMode {
    /// Traditional O5LOGON password verifier.
    #[default]
    Password,
    /// IAM or OAuth2 bearer token authentication.
    Token,
}

impl OracleAuthMode {
    /// Parse an authentication mode string (`"password"` or `"token"`).
    pub fn parse(value: &str) -> Result<Self, OracleError> {
        match value.trim() {
            v if v.eq_ignore_ascii_case("password") => Ok(Self::Password),
            v if v.eq_ignore_ascii_case("token") => Ok(Self::Token),
            other => Err(OracleError::Config(format!(
                "invalid Oracle auth_mode {other:?}; expected 'password' or 'token'"
            ))),
        }
    }

    /// Return the canonical string identifier for this authentication mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Password => "password",
            Self::Token => "token",
        }
    }
}

/// Database Resident Connection Pooling (DRCP) session purity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DrcpPurity {
    /// Reuse a pooled server process (`ATTR_PURITY_SELF`).
    #[default]
    SelfPurity,
    /// Require a fresh server process (`ATTR_PURITY_NEW`).
    NewPurity,
}

impl DrcpPurity {
    /// Parse a DRCP purity value (`"SELF"` or `"NEW"`).
    pub fn parse(value: &str) -> Result<Self, OracleError> {
        match value.trim() {
            v if v.eq_ignore_ascii_case("SELF") => Ok(Self::SelfPurity),
            v if v.eq_ignore_ascii_case("NEW") => Ok(Self::NewPurity),
            other => Err(OracleError::Config(format!(
                "invalid Oracle DRCP purity {other:?}; expected 'SELF' or 'NEW'"
            ))),
        }
    }

    /// Return the canonical uppercase string (`"SELF"` or `"NEW"`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SelfPurity => "SELF",
            Self::NewPurity => "NEW",
        }
    }
}

/// Transport protocol for an Oracle TNS listener endpoint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OracleProtocol {
    /// Plain TCP listener (`PROTOCOL=tcp`).
    #[default]
    Tcp,
    /// Mutual-TLS / TLS listener (`PROTOCOL=tcps`).
    Tcps,
}

impl OracleProtocol {
    /// Parse a transport protocol identifier (`"tcp"` or `"tcps"`).
    pub fn parse(value: &str) -> Result<Self, OracleError> {
        match value.trim() {
            v if v.eq_ignore_ascii_case("tcp") => Ok(Self::Tcp),
            v if v.eq_ignore_ascii_case("tcps") => Ok(Self::Tcps),
            other => Err(OracleError::Config(format!(
                "invalid Oracle protocol {other:?}; expected 'tcp' or 'tcps'"
            ))),
        }
    }

    /// Return the canonical lowercase protocol name (`"tcp"` or `"tcps"`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Tcps => "tcps",
        }
    }
}

/// Unvalidated keyword options passed into `SyncOracleClient`, `AsyncOracleClient`, or pool constructors.
#[derive(Clone)]
pub struct RawOracleOptions {
    pub user: Option<String>,
    pub password: Option<String>,
    pub dsn: Option<String>,
    pub host: String,
    pub port: i32,
    pub service_name: String,
    pub sid: Option<String>,
    pub tns_admin: Option<String>,
    pub connect_descriptor: Option<String>,
    pub protocol: String,
    pub ssl_server_dn_match: bool,
    pub ssl_server_cert_dn: Option<String>,
    pub wallet_location: Option<String>,
    pub wallet_password: Option<String>,
    pub pem_cert_data: Option<Vec<u8>>,
    pub cwallet_sso_data: Option<Vec<u8>>,
    pub auth_mode: String,
    pub access_token: Option<String>,
    pub token_auth_options: Option<BTreeMap<String, String>>,
    pub cclass: Option<String>,
    pub purity: String,
    pub connection_tag: Option<String>,
    pub stmtcachesize: i32,
    pub prefetchrows: i64,
    pub arraysize: i64,
    pub call_timeout_ms: Option<i64>,
    pub connect_timeout: f64,
    pub enable_lowercase_column_names: bool,
    pub client_identifier: Option<String>,
    pub module: Option<String>,
    pub action: Option<String>,
    pub ecid: Option<String>,
}

impl Default for RawOracleOptions {
    fn default() -> Self {
        Self {
            user: None,
            password: None,
            dsn: None,
            host: "localhost".to_owned(),
            port: 1521,
            service_name: "FREEPDB1".to_owned(),
            sid: None,
            tns_admin: None,
            connect_descriptor: None,
            protocol: "tcp".to_owned(),
            ssl_server_dn_match: true,
            ssl_server_cert_dn: None,
            wallet_location: None,
            wallet_password: None,
            pem_cert_data: None,
            cwallet_sso_data: None,
            auth_mode: "password".to_owned(),
            access_token: None,
            token_auth_options: None,
            cclass: None,
            purity: "SELF".to_owned(),
            connection_tag: None,
            stmtcachesize: 64,
            prefetchrows: 256,
            arraysize: 256,
            call_timeout_ms: None,
            connect_timeout: 10.0,
            enable_lowercase_column_names: true,
            client_identifier: None,
            module: None,
            action: None,
            ecid: None,
        }
    }
}

impl fmt::Debug for RawOracleOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawOracleOptions")
            .field("user", &self.user)
            .field("password", &redact_opt(self.password.as_ref()))
            .field("dsn", &self.dsn)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("service_name", &self.service_name)
            .field("sid", &self.sid)
            .field("tns_admin", &self.tns_admin)
            .field("connect_descriptor", &self.connect_descriptor)
            .field("protocol", &self.protocol)
            .field("ssl_server_dn_match", &self.ssl_server_dn_match)
            .field("ssl_server_cert_dn", &self.ssl_server_cert_dn)
            .field("wallet_location", &self.wallet_location)
            .field("wallet_password", &redact_opt(self.wallet_password.as_ref()))
            .field("pem_cert_data", &redact_opt(self.pem_cert_data.as_ref()))
            .field("cwallet_sso_data", &redact_opt(self.cwallet_sso_data.as_ref()))
            .field("auth_mode", &self.auth_mode)
            .field("access_token", &redact_opt(self.access_token.as_ref()))
            .field(
                "token_auth_options",
                &redact_opt(self.token_auth_options.as_ref()),
            )
            .field("cclass", &self.cclass)
            .field("purity", &self.purity)
            .field("connection_tag", &self.connection_tag)
            .field("stmtcachesize", &self.stmtcachesize)
            .field("prefetchrows", &self.prefetchrows)
            .field("arraysize", &self.arraysize)
            .field("call_timeout_ms", &self.call_timeout_ms)
            .field("connect_timeout", &self.connect_timeout)
            .field(
                "enable_lowercase_column_names",
                &self.enable_lowercase_column_names,
            )
            .field("client_identifier", &self.client_identifier)
            .field("module", &self.module)
            .field("action", &self.action)
            .field("ecid", &self.ecid)
            .finish()
    }
}

/// Validated, immutable Oracle client configuration with secret-redacted `Debug` formatting.
#[derive(Clone, PartialEq, Eq)]
pub struct NativeOracleConfig {
    pub user: Option<String>,
    pub password: Option<String>,
    pub dsn: Option<String>,
    pub host: String,
    pub port: u16,
    pub service_name: String,
    pub sid: Option<String>,
    pub tns_admin: Option<String>,
    pub connect_descriptor: Option<String>,
    pub protocol: OracleProtocol,
    pub ssl_server_dn_match: bool,
    pub ssl_server_cert_dn: Option<String>,
    pub wallet_location: Option<String>,
    pub wallet_password: Option<String>,
    pub pem_cert_data: Option<Vec<u8>>,
    pub cwallet_sso_data: Option<Vec<u8>>,
    pub auth_mode: OracleAuthMode,
    pub access_token: Option<String>,
    pub token_auth_options: Option<BTreeMap<String, String>>,
    pub cclass: Option<String>,
    pub purity: DrcpPurity,
    pub connection_tag: Option<String>,
    pub stmtcachesize: u32,
    pub prefetchrows: u32,
    pub arraysize: u32,
    pub call_timeout_ms: Option<u64>,
    pub connect_timeout: Duration,
    pub enable_lowercase_column_names: bool,
    pub client_identifier: Option<String>,
    pub module: Option<String>,
    pub action: Option<String>,
    pub ecid: Option<String>,
}

fn check_no_nul(field_name: &str, value: &str) -> Result<(), OracleError> {
    if value.contains('\0') {
        return Err(OracleError::Config(format!(
            "Oracle option '{field_name}' must not contain NUL bytes"
        )));
    }
    Ok(())
}

fn check_opt_no_nul(field_name: &str, value: Option<&String>) -> Result<(), OracleError> {
    if let Some(v) = value {
        check_no_nul(field_name, v)?;
    }
    Ok(())
}

const fn redact_opt<T>(opt: Option<&T>) -> Option<&'static str> {
    if opt.is_some() {
        Some("<redacted>")
    } else {
        None
    }
}

impl NativeOracleConfig {
    /// Validate raw constructor options purely in memory without DNS, filesystem, or socket I/O.
    pub fn validate(raw: RawOracleOptions) -> Result<Self, OracleError> {
        check_opt_no_nul("user", raw.user.as_ref())?;
        check_opt_no_nul("password", raw.password.as_ref())?;
        check_opt_no_nul("dsn", raw.dsn.as_ref())?;
        check_no_nul("host", &raw.host)?;
        check_no_nul("service_name", &raw.service_name)?;
        check_opt_no_nul("sid", raw.sid.as_ref())?;
        check_opt_no_nul("tns_admin", raw.tns_admin.as_ref())?;
        check_opt_no_nul("connect_descriptor", raw.connect_descriptor.as_ref())?;
        check_no_nul("protocol", &raw.protocol)?;
        check_opt_no_nul("ssl_server_cert_dn", raw.ssl_server_cert_dn.as_ref())?;
        check_opt_no_nul("wallet_location", raw.wallet_location.as_ref())?;
        check_opt_no_nul("wallet_password", raw.wallet_password.as_ref())?;
        check_no_nul("auth_mode", &raw.auth_mode)?;
        check_opt_no_nul("access_token", raw.access_token.as_ref())?;
        check_opt_no_nul("cclass", raw.cclass.as_ref())?;
        check_no_nul("purity", &raw.purity)?;
        check_opt_no_nul("connection_tag", raw.connection_tag.as_ref())?;
        check_opt_no_nul("client_identifier", raw.client_identifier.as_ref())?;
        check_opt_no_nul("module", raw.module.as_ref())?;
        check_opt_no_nul("action", raw.action.as_ref())?;
        check_opt_no_nul("ecid", raw.ecid.as_ref())?;

        if let Some(opts) = raw.token_auth_options.as_ref() {
            for (k, v) in opts {
                check_no_nul("token_auth_options.key", k)?;
                check_no_nul("token_auth_options.value", v)?;
            }
        }

        let port = u16::try_from(raw.port)
            .ok()
            .filter(|&p| p >= 1)
            .ok_or_else(|| {
                OracleError::Config(format!(
                    "Oracle port must be in 1..=65535; got {}",
                    raw.port
                ))
            })?;

        if raw.dsn.is_none() && raw.connect_descriptor.is_none() && raw.host.trim().is_empty() {
            return Err(OracleError::Config(
                "Oracle host must not be empty when dsn and connect_descriptor are unset"
                    .to_owned(),
            ));
        }

        if raw.dsn.is_none()
            && raw.connect_descriptor.is_none()
            && raw.sid.as_deref().unwrap_or("").trim().is_empty()
            && raw.service_name.trim().is_empty()
        {
            return Err(OracleError::Config(
                "Oracle service_name or sid must not be empty".to_owned(),
            ));
        }

        if !raw.connect_timeout.is_finite() || raw.connect_timeout <= 0.0 {
            return Err(OracleError::Config(format!(
                "Oracle connect_timeout must be finite and > 0.0; got {}",
                raw.connect_timeout
            )));
        }
        let connect_timeout = Duration::from_secs_f64(raw.connect_timeout);

        let stmtcachesize = u32::try_from(raw.stmtcachesize).map_err(|_| {
            OracleError::Config(format!(
                "Oracle stmtcachesize must be >= 0; got {}",
                raw.stmtcachesize
            ))
        })?;

        let prefetchrows = u32::try_from(raw.prefetchrows)
            .ok()
            .filter(|&v| v > 0)
            .ok_or_else(|| {
                OracleError::Config(format!(
                    "Oracle prefetchrows must be > 0; got {}",
                    raw.prefetchrows
                ))
            })?;

        let arraysize = u32::try_from(raw.arraysize)
            .ok()
            .filter(|&v| v > 0)
            .ok_or_else(|| {
                OracleError::Config(format!(
                    "Oracle arraysize must be > 0; got {}",
                    raw.arraysize
                ))
            })?;

        let call_timeout_ms = match raw.call_timeout_ms {
            Some(ms) if ms <= 0 => {
                return Err(OracleError::Config(format!(
                    "Oracle call_timeout_ms must be > 0 when set; got {ms}"
                )));
            }
            Some(ms) => Some(ms as u64),
            None => None,
        };

        let protocol = OracleProtocol::parse(&raw.protocol)?;
        let auth_mode = OracleAuthMode::parse(&raw.auth_mode)?;
        let purity = DrcpPurity::parse(&raw.purity)?;

        Ok(Self {
            user: raw.user,
            password: raw.password,
            dsn: raw.dsn,
            host: raw.host,
            port,
            service_name: raw.service_name,
            sid: raw.sid,
            tns_admin: raw.tns_admin,
            connect_descriptor: raw.connect_descriptor,
            protocol,
            ssl_server_dn_match: raw.ssl_server_dn_match,
            ssl_server_cert_dn: raw.ssl_server_cert_dn,
            wallet_location: raw.wallet_location,
            wallet_password: raw.wallet_password,
            pem_cert_data: raw.pem_cert_data,
            cwallet_sso_data: raw.cwallet_sso_data,
            auth_mode,
            access_token: raw.access_token,
            token_auth_options: raw.token_auth_options,
            cclass: raw.cclass,
            purity,
            connection_tag: raw.connection_tag,
            stmtcachesize,
            prefetchrows,
            arraysize,
            call_timeout_ms,
            connect_timeout,
            enable_lowercase_column_names: raw.enable_lowercase_column_names,
            client_identifier: raw.client_identifier,
            module: raw.module,
            action: raw.action,
            ecid: raw.ecid,
        })
    }

    /// Return `connect_timeout` in floating-point seconds.
    #[must_use]
    pub fn connect_timeout_secs(&self) -> f64 {
        self.connect_timeout.as_secs_f64()
    }
}

impl fmt::Debug for NativeOracleConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeOracleConfig")
            .field("user", &self.user)
            .field("password", &redact_opt(self.password.as_ref()))
            .field("dsn", &self.dsn)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("service_name", &self.service_name)
            .field("sid", &self.sid)
            .field("tns_admin", &self.tns_admin)
            .field("connect_descriptor", &self.connect_descriptor)
            .field("protocol", &self.protocol)
            .field("ssl_server_dn_match", &self.ssl_server_dn_match)
            .field("ssl_server_cert_dn", &self.ssl_server_cert_dn)
            .field("wallet_location", &self.wallet_location)
            .field("wallet_password", &redact_opt(self.wallet_password.as_ref()))
            .field("pem_cert_data", &redact_opt(self.pem_cert_data.as_ref()))
            .field("cwallet_sso_data", &redact_opt(self.cwallet_sso_data.as_ref()))
            .field("auth_mode", &self.auth_mode)
            .field("access_token", &redact_opt(self.access_token.as_ref()))
            .field(
                "token_auth_options",
                &redact_opt(self.token_auth_options.as_ref()),
            )
            .field("cclass", &self.cclass)
            .field("purity", &self.purity)
            .field("connection_tag", &self.connection_tag)
            .field("stmtcachesize", &self.stmtcachesize)
            .field("prefetchrows", &self.prefetchrows)
            .field("arraysize", &self.arraysize)
            .field("call_timeout_ms", &self.call_timeout_ms)
            .field("connect_timeout", &self.connect_timeout)
            .field(
                "enable_lowercase_column_names",
                &self.enable_lowercase_column_names,
            )
            .field("client_identifier", &self.client_identifier)
            .field("module", &self.module)
            .field("action", &self.action)
            .field("ecid", &self.ecid)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        DrcpPurity, NativeOracleConfig, OracleAuthMode, OracleProtocol, RawOracleOptions,
    };
    use crate::errors::OracleError;

    #[test]
    fn default_options_validate_cleanly() {
        let cfg = NativeOracleConfig::validate(RawOracleOptions::default()).expect("default valid");
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 1521);
        assert_eq!(cfg.service_name, "FREEPDB1");
        assert_eq!(cfg.protocol, OracleProtocol::Tcp);
        assert_eq!(cfg.auth_mode, OracleAuthMode::Password);
        assert_eq!(cfg.purity, DrcpPurity::SelfPurity);
        assert_eq!(cfg.stmtcachesize, 64);
        assert_eq!(cfg.prefetchrows, 256);
        assert_eq!(cfg.arraysize, 256);
        assert!(cfg.enable_lowercase_column_names);
    }

    #[test]
    fn invalid_options_rejected_with_config_error() {
        let bad_purity = RawOracleOptions {
            purity: "INVALID".to_owned(),
            ..Default::default()
        };
        assert!(matches!(
            NativeOracleConfig::validate(bad_purity),
            Err(OracleError::Config(_))
        ));

        let bad_port_zero = RawOracleOptions {
            port: 0,
            ..Default::default()
        };
        assert!(matches!(
            NativeOracleConfig::validate(bad_port_zero),
            Err(OracleError::Config(_))
        ));

        let bad_port_high = RawOracleOptions {
            port: 70_000,
            ..Default::default()
        };
        assert!(matches!(
            NativeOracleConfig::validate(bad_port_high),
            Err(OracleError::Config(_))
        ));

        let bad_timeout = RawOracleOptions {
            connect_timeout: -1.0,
            ..Default::default()
        };
        assert!(matches!(
            NativeOracleConfig::validate(bad_timeout),
            Err(OracleError::Config(_))
        ));

        let nan_timeout = RawOracleOptions {
            connect_timeout: f64::NAN,
            ..Default::default()
        };
        assert!(matches!(
            NativeOracleConfig::validate(nan_timeout),
            Err(OracleError::Config(_))
        ));

        let nul_user = RawOracleOptions {
            user: Some("scott\0tiger".to_owned()),
            ..Default::default()
        };
        assert!(matches!(
            NativeOracleConfig::validate(nul_user),
            Err(OracleError::Config(_))
        ));

        let bad_auth = RawOracleOptions {
            auth_mode: "kerberos".to_owned(),
            ..Default::default()
        };
        assert!(matches!(
            NativeOracleConfig::validate(bad_auth),
            Err(OracleError::Config(_))
        ));

        let bad_prefetch = RawOracleOptions {
            prefetchrows: 0,
            ..Default::default()
        };
        assert!(matches!(
            NativeOracleConfig::validate(bad_prefetch),
            Err(OracleError::Config(_))
        ));

        let bad_arraysize = RawOracleOptions {
            arraysize: 0,
            ..Default::default()
        };
        assert!(matches!(
            NativeOracleConfig::validate(bad_arraysize),
            Err(OracleError::Config(_))
        ));

        let bad_stmtcache = RawOracleOptions {
            stmtcachesize: -1,
            ..Default::default()
        };
        assert!(matches!(
            NativeOracleConfig::validate(bad_stmtcache),
            Err(OracleError::Config(_))
        ));
    }

    #[test]
    fn debug_output_redacts_all_secret_fields() {
        let mut token_opts = BTreeMap::new();
        token_opts.insert("client_secret".to_owned(), "super-secret-value".to_owned());
        let cfg = NativeOracleConfig::validate(RawOracleOptions {
            user: Some("system".to_owned()),
            password: Some("s3cr3t".to_owned()),
            wallet_password: Some("w4ll3t-p4ss".to_owned()),
            pem_cert_data: Some(vec![1, 2, 3]),
            cwallet_sso_data: Some(vec![4, 5, 6]),
            access_token: Some("tok-123".to_owned()),
            token_auth_options: Some(token_opts),
            ..Default::default()
        })
        .expect("valid config");

        let rendered = format!("{cfg:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("s3cr3t"));
        assert!(!rendered.contains("w4ll3t-p4ss"));
        assert!(!rendered.contains("tok-123"));
        assert!(!rendered.contains("super-secret-value"));
    }
}
