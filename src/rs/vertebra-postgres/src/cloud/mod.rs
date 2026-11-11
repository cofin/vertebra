//! Google Cloud SQL (`v1beta4`, `:3307`) and AlloyDB (`v1alpha`, `:5433`) ephemeral certificate
//! generation, Admin API discovery, single-flight material caching, and pre-startup mTLS dialers.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::time::Instant;
use vertebra_db::connection::{
    ConnectOptions, EndpointTarget, TlsMode, TransportError, TransportStream,
    finish_alloydb_metadata,
};
use vertebra_db::dialer::{AlloyDbClusterUri, AuthType, CloudSqlInstanceUri, IpType};

use crate::binding::tls_server_end_point;
use crate::cloud::material::{
    CloudConnector, CloudConnectorConfig, CloudResourceTarget, PreparedCloudMaterial,
};
use crate::config::PostgresConfig;
use crate::credentials::ScopedLoginSnapshot;
use crate::errors::PostgresError;

pub mod alloydb;
pub mod cloudsql;
pub mod keys;
pub mod material;

/// Pre-startup cloud mTLS transport returned by [`dial_cloudsql`] and [`dial_alloydb`].
pub struct DialedCloudTransport {
    pub stream: TransportStream,
    pub tls_server_end_point: Option<Vec<u8>>,
    pub(crate) login_snapshot: Option<ScopedLoginSnapshot>,
}

impl std::fmt::Debug for DialedCloudTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DialedCloudTransport")
            .field("is_tls", &self.stream.is_tls())
            .field("has_channel_binding", &self.tls_server_end_point.is_some())
            .finish()
    }
}

/// Dial a Cloud SQL PostgreSQL instance on `:3307` over verified TLS 1.3 mTLS and return a
/// pre-startup [`DialedCloudTransport`] without sending PostgreSQL `SSLRequest` or `StartupMessage`.
pub async fn dial_cloudsql(
    config: &PostgresConfig,
    connector: &CloudConnector,
    absolute_deadline: Instant,
) -> Result<DialedCloudTransport, PostgresError> {
    let instance_raw = config.cloud_sql_instance.as_deref().ok_or_else(|| {
        PostgresError::Config("cloud_sql_instance is required for dial_cloudsql".to_string())
    })?;
    let uri = CloudSqlInstanceUri::parse(instance_raw)
        .map_err(|e| PostgresError::Config(e.to_string()))?;
    let ip_type = parse_ip_type(&config.cloud_sql_ip_type)?;
    let auth_type = if config.enable_iam_auth {
        AuthType::Iam
    } else {
        AuthType::Password
    };

    let connector_cfg = build_connector_config(
        CloudResourceTarget::CloudSql(uri),
        ip_type,
        auth_type,
        config,
    )?;
    let prepared = connector.prepare(&connector_cfg, absolute_deadline).await?;
    if config.enable_iam_auth
        && let Some(ref snap) = prepared.login_snapshot
    {
        snap.ensure_usable(connector.now_unix_ms())?;
    }

    let connect_opts = build_cloud_connect_options(&prepared, config, absolute_deadline)?;
    let stream =
        tokio::time::timeout_at(absolute_deadline, TransportStream::connect(&connect_opts))
            .await
            .map_err(|_| PostgresError::Transport("Cloud SQL mTLS dial timed out".to_string()))?
            .map_err(map_transport_error)?;

    let tls_server_end_point = match stream.peer_leaf_der() {
        Some(der) => Some(tls_server_end_point(der)?),
        None => None,
    };

    Ok(DialedCloudTransport {
        stream,
        tls_server_end_point,
        login_snapshot: if config.enable_iam_auth {
            prepared.login_snapshot
        } else {
            None
        },
    })
}

/// Dial an AlloyDB PostgreSQL instance on `:5433` across split TLS 1.3 and `MetadataExchangeRequest`
/// stages, verifying `ScopedLoginSnapshot::ensure_usable` immediately after TLS before emitting metadata bytes.
pub async fn dial_alloydb(
    config: &PostgresConfig,
    connector: &CloudConnector,
    absolute_deadline: Instant,
) -> Result<DialedCloudTransport, PostgresError> {
    let uri_raw = config.alloydb_instance_uri.as_deref().ok_or_else(|| {
        PostgresError::Config("alloydb_instance_uri is required for dial_alloydb".to_string())
    })?;
    let uri =
        AlloyDbClusterUri::parse(uri_raw).map_err(|e| PostgresError::Config(e.to_string()))?;
    let ip_type = parse_ip_type(&config.alloydb_ip_type)?;
    let auth_type = if config.enable_iam_auth {
        AuthType::Iam
    } else {
        AuthType::Password
    };

    let connector_cfg = build_connector_config(
        CloudResourceTarget::AlloyDb(uri),
        ip_type,
        auth_type,
        config,
    )?;
    let prepared = connector.prepare(&connector_cfg, absolute_deadline).await?;
    let login_snap = prepared.login_snapshot.clone().ok_or_else(|| {
        PostgresError::Auth(
            "AlloyDB prepared material is missing scoped login snapshot".to_string(),
        )
    })?;
    login_snap.ensure_usable(connector.now_unix_ms())?;

    let connect_opts = build_cloud_connect_options(&prepared, config, absolute_deadline)?;
    let mut stream = tokio::time::timeout_at(
        absolute_deadline,
        TransportStream::connect_alloydb_tls(&connect_opts),
    )
    .await
    .map_err(|_| PostgresError::Transport("AlloyDB TLS 1.3 handshake timed out".to_string()))?
    .map_err(map_transport_error)?;

    if let Some(delay_ms) = config
        .cloud_auth_options
        .get("test_post_tls_advance_clock_ms")
        .and_then(|v| v.parse::<u64>().ok())
    {
        let _ = delay_ms;
    }

    login_snap.ensure_usable(connector.now_unix_ms())?;

    finish_alloydb_metadata(&mut stream, &connect_opts, absolute_deadline)
        .await
        .map_err(map_transport_error)?;

    let tls_server_end_point = match stream.peer_leaf_der() {
        Some(der) => Some(tls_server_end_point(der)?),
        None => None,
    };

    Ok(DialedCloudTransport {
        stream,
        tls_server_end_point,
        login_snapshot: config.enable_iam_auth.then_some(login_snap),
    })
}

fn build_connector_config(
    target: CloudResourceTarget,
    ip_type: IpType,
    auth_type: AuthType,
    config: &PostgresConfig,
) -> Result<CloudConnectorConfig, PostgresError> {
    let mut cloud_opts = config.cloud_auth_options.clone();
    let admin_ep_override = cloud_opts.remove("admin_endpoint");
    cloud_opts.remove("proxy_port");

    let mut cfg =
        CloudConnectorConfig::new(target, ip_type, auth_type).with_cloud_auth_options(cloud_opts);
    if !config.iam_auth_options.is_empty() {
        cfg = cfg.with_iam_auth_options(Some(config.iam_auth_options.clone()));
    }
    if let Some(ep) = admin_ep_override {
        cfg = cfg.with_admin_endpoint(ep)?;
    }
    Ok(cfg)
}

fn build_cloud_connect_options(
    prepared: &PreparedCloudMaterial,
    config: &PostgresConfig,
    absolute_deadline: Instant,
) -> Result<ConnectOptions, PostgresError> {
    let now = Instant::now();
    if now >= absolute_deadline {
        return Err(PostgresError::Transport(
            "cloud connection deadline exceeded before socket dial".to_string(),
        ));
    }
    let remaining = absolute_deadline
        .saturating_duration_since(now)
        .max(Duration::from_millis(1));

    let port_override = config
        .cloud_auth_options
        .get("proxy_port")
        .and_then(|v| v.trim().parse::<u16>().ok());

    let target = match &prepared.endpoint_target {
        EndpointTarget::CloudSql {
            instance,
            addr,
            server_ca_pem,
            client_cert_pem,
            client_key_pem,
            peer_identity,
        } => {
            let effective_addr = match port_override {
                Some(p) => SocketAddr::new(addr.ip(), p),
                None => *addr,
            };
            EndpointTarget::CloudSql {
                instance: instance.clone(),
                addr: effective_addr,
                server_ca_pem: server_ca_pem.clone(),
                client_cert_pem: client_cert_pem.clone(),
                client_key_pem: client_key_pem.clone(),
                peer_identity: peer_identity.clone(),
            }
        }
        EndpointTarget::AlloyDb {
            cluster,
            addr,
            server_ca_pem,
            client_cert_pem,
            client_key_pem,
            user_agent,
            oauth2_token,
            auth_type,
            peer_identity,
        } => {
            let effective_addr = match port_override {
                Some(p) => SocketAddr::new(addr.ip(), p),
                None => *addr,
            };
            EndpointTarget::AlloyDb {
                cluster: cluster.clone(),
                addr: effective_addr,
                server_ca_pem: server_ca_pem.clone(),
                client_cert_pem: client_cert_pem.clone(),
                client_key_pem: client_key_pem.clone(),
                user_agent: user_agent.clone(),
                oauth2_token: oauth2_token.clone(),
                auth_type: *auth_type,
                peer_identity: peer_identity.clone(),
            }
        }
        other => other.clone(),
    };

    Ok(ConnectOptions {
        target,
        connect_timeout: remaining,
        tls_mode: TlsMode::VerifyFull,
        ca_pem: Some(prepared.server_ca_pem.clone()),
        client_cert_pem: Some(prepared.client_cert_chain_pem.clone()),
        client_key_pem: Some(prepared.client_key_pem.clone()),
        server_name: prepared.peer_dns_name.clone(),
        tcp_nodelay: true,
        application_name: None,
    })
}

fn parse_ip_type(raw: &str) -> Result<IpType, PostgresError> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "PUBLIC" => Ok(IpType::Public),
        "PRIVATE" => Ok(IpType::Private),
        "PSC" => Ok(IpType::Psc),
        other => Err(PostgresError::Config(format!(
            "unsupported cloud IP type `{other}`"
        ))),
    }
}

fn map_transport_error(err: TransportError) -> PostgresError {
    match err {
        TransportError::InvalidConfig(msg) => PostgresError::Config(msg),
        other => PostgresError::Transport(other.to_string()),
    }
}
