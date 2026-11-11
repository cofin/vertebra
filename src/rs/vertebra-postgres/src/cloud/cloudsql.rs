//! Cloud SQL Admin `v1beta4` (`connectSettings` and `:generateEphemeralCert`) discovery client.

use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use vertebra_db::dialer::{CloudSqlInstanceUri, IpType};

use crate::cloud::keys::{
    ValidatedClientIdentity, generate_ephemeral_rsa_keypair, validate_issued_certificate_chain,
};
use crate::cloud::material::{CloudConnector, encode_uri_segment};
use crate::errors::PostgresError;

#[derive(Debug, Deserialize)]
struct SslCert {
    #[serde(default)]
    cert: String,
}

#[derive(Debug, Deserialize)]
struct IpMapping {
    #[serde(default, rename = "type")]
    ip_type: String,
    #[serde(default, rename = "ipAddress")]
    ip_address: String,
}

#[derive(Debug, Deserialize)]
struct DnsNameEntry {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "connectionType")]
    connection_type: String,
}

#[derive(Debug, Deserialize)]
struct ConnectSettingsResponse {
    #[serde(default, rename = "serverCaCert")]
    server_ca_cert: Option<SslCert>,
    #[serde(default, rename = "ipAddresses")]
    ip_addresses: Vec<IpMapping>,
    #[serde(default)]
    region: String,
    #[serde(default, rename = "databaseVersion")]
    database_version: String,
    #[serde(default, rename = "pscEnabled")]
    psc_enabled: bool,
    #[serde(default, rename = "dnsName")]
    dns_name: Option<String>,
    #[serde(default, rename = "dnsNames")]
    dns_names: Vec<DnsNameEntry>,
}

#[derive(Debug, Serialize)]
struct GenerateEphemeralCertRequest<'a> {
    public_key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_token: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct GenerateEphemeralCertResponse {
    #[serde(default, rename = "ephemeralCert")]
    ephemeral_cert: Option<SslCert>,
}

/// Raw Cloud SQL `v1beta4` discovery output consumed by [`CloudConnector::prepare`].
#[derive(Clone, Debug)]
pub(crate) struct CloudSqlDiscovery {
    pub host: String,
    pub port: u16,
    pub peer_dns_name: Option<String>,
    pub expected_cn: String,
    pub server_ca_pem: Vec<u8>,
    pub client_identity: ValidatedClientIdentity,
}

/// Discover a Cloud SQL PostgreSQL instance via `v1beta4` `connectSettings` and `:generateEphemeralCert`.
pub(crate) async fn fetch_cloudsql_discovery(
    connector: &CloudConnector,
    uri: &CloudSqlInstanceUri,
    ip_type: IpType,
    admin_endpoint: &str,
    admin_token: &str,
    iam_access_token: Option<&str>,
    absolute_deadline: Instant,
) -> Result<CloudSqlDiscovery, PostgresError> {
    let base = admin_endpoint.trim_end_matches('/');
    let enc_project = encode_uri_segment(&uri.project)?;
    let enc_instance = encode_uri_segment(&uri.instance)?;
    let instance_base =
        format!("{base}/sql/v1beta4/projects/{enc_project}/instances/{enc_instance}");

    let settings_url = format!("{instance_base}/connectSettings");
    let settings: ConnectSettingsResponse = connector
        .get_json(&settings_url, admin_token, absolute_deadline)
        .await?;

    if settings.region != uri.region {
        return Err(PostgresError::Config(format!(
            "Cloud SQL instance region mismatch: expected `{}`, got `{}`",
            uri.region, settings.region
        )));
    }
    if !settings.database_version.starts_with("POSTGRES") {
        return Err(PostgresError::Config(format!(
            "Cloud SQL instance databaseVersion `{}` is not a PostgreSQL engine",
            settings.database_version
        )));
    }

    let server_ca_str = settings
        .server_ca_cert
        .map(|c| c.cert)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            PostgresError::Transport(
                "Cloud SQL connectSettings response is missing serverCaCert.cert".to_string(),
            )
        })?;

    let advertised_dns = settings
        .dns_names
        .iter()
        .find(|d| {
            d.connection_type
                .eq_ignore_ascii_case("PRIVATE_SERVICE_CONNECT")
                && !d.name.trim().is_empty()
        })
        .map(|d| d.name.trim().trim_end_matches('.').to_string())
        .or_else(|| {
            settings
                .dns_name
                .as_deref()
                .map(|s| s.trim().trim_end_matches('.'))
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        });

    let (host, peer_dns_name) = match ip_type {
        IpType::Public => {
            let ip = settings
                .ip_addresses
                .iter()
                .find(|m| m.ip_type == "PRIMARY" && !m.ip_address.trim().is_empty())
                .map(|m| m.ip_address.trim().to_string())
                .ok_or_else(|| {
                    PostgresError::Transport(
                        "Cloud SQL instance has no PRIMARY (public) IP address".to_string(),
                    )
                })?;
            (ip, advertised_dns)
        }
        IpType::Private => {
            let ip = settings
                .ip_addresses
                .iter()
                .find(|m| m.ip_type == "PRIVATE" && !m.ip_address.trim().is_empty())
                .map(|m| m.ip_address.trim().to_string())
                .ok_or_else(|| {
                    PostgresError::Transport(
                        "Cloud SQL instance has no PRIVATE IP address".to_string(),
                    )
                })?;
            (ip, advertised_dns)
        }
        IpType::Psc => {
            let psc_dns = settings
                .dns_names
                .iter()
                .find(|d| {
                    d.connection_type
                        .eq_ignore_ascii_case("PRIVATE_SERVICE_CONNECT")
                        && !d.name.trim().is_empty()
                })
                .map(|d| d.name.trim().trim_end_matches('.').to_string())
                .or_else(|| {
                    if settings.psc_enabled {
                        settings
                            .dns_name
                            .as_deref()
                            .map(|s| s.trim().trim_end_matches('.'))
                            .filter(|s| !s.is_empty())
                            .map(str::to_string)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    PostgresError::Transport(
                        "Cloud SQL instance does not have Private Service Connect (PSC) enabled"
                            .to_string(),
                    )
                })?;
            (psc_dns.clone(), Some(psc_dns))
        }
    };

    let keypair = generate_ephemeral_rsa_keypair(absolute_deadline).await?;
    let cert_url = format!("{instance_base}:generateEphemeralCert");
    let req_body = GenerateEphemeralCertRequest {
        public_key: keypair.public_key_pem(),
        access_token: iam_access_token,
    };
    let cert_resp: GenerateEphemeralCertResponse = connector
        .post_json(&cert_url, admin_token, &req_body, absolute_deadline)
        .await?;

    let leaf_pem = cert_resp
        .ephemeral_cert
        .map(|c| c.cert)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            PostgresError::Transport(
                "Cloud SQL generateEphemeralCert response is missing ephemeralCert.cert"
                    .to_string(),
            )
        })?;

    let now_ms = connector.now_unix_ms();
    let client_identity = validate_issued_certificate_chain(&[leaf_pem], &keypair, now_ms)?;

    Ok(CloudSqlDiscovery {
        host,
        port: CloudSqlInstanceUri::default_server_port(),
        peer_dns_name,
        expected_cn: uri.expected_server_cn(),
        server_ca_pem: server_ca_str.into_bytes(),
        client_identity,
    })
}
