//! AlloyDB Admin `v1alpha` (`connectionInfo` and `:generateClientCertificate`) discovery client.

use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use vertebra_db::dialer::{AlloyDbClusterUri, IpType};

use crate::cloud::keys::{
    ValidatedClientIdentity, generate_ephemeral_rsa_keypair, validate_issued_certificate_chain,
};
use crate::cloud::material::{CloudConnector, encode_uri_segment};
use crate::errors::PostgresError;

/// Default AlloyDB auth proxy mTLS port (`5433`).
pub const ALLOYDB_SERVER_PORT: u16 = 5433;

#[derive(Debug, Deserialize)]
struct ConnectionInfoResponse {
    #[serde(default, rename = "ipAddress")]
    ip_address: Option<String>,
    #[serde(default, rename = "publicIpAddress")]
    public_ip_address: Option<String>,
    #[serde(default, rename = "pscDnsName")]
    psc_dns_name: Option<String>,
    #[serde(default, rename = "pscAutoDnsName")]
    psc_auto_dns_name: Option<String>,
    #[allow(dead_code)]
    #[serde(default, rename = "instanceUid")]
    instance_uid: Option<String>,
}

#[derive(Debug, Serialize)]
struct GenerateClientCertificateRequest<'a> {
    #[serde(rename = "publicKey")]
    public_key: &'a str,
    #[serde(rename = "certDuration")]
    cert_duration: &'a str,
    #[serde(rename = "useMetadataExchange")]
    use_metadata_exchange: bool,
}

#[derive(Debug, Deserialize)]
struct GenerateClientCertificateResponse {
    #[serde(default, rename = "pemCertificateChain")]
    pem_certificate_chain: Vec<String>,
    #[serde(default, rename = "caCert")]
    ca_cert: String,
}

/// Raw AlloyDB `v1alpha` discovery output consumed by [`CloudConnector::prepare`].
#[derive(Clone, Debug)]
pub(crate) struct AlloyDbDiscovery {
    pub host: String,
    pub port: u16,
    pub peer_dns_name: Option<String>,
    pub server_ca_pem: Vec<u8>,
    pub client_identity: ValidatedClientIdentity,
}

/// Discover an AlloyDB instance via `v1alpha` `connectionInfo` and `:generateClientCertificate`.
pub(crate) async fn fetch_alloydb_discovery(
    connector: &CloudConnector,
    uri: &AlloyDbClusterUri,
    ip_type: IpType,
    admin_endpoint: &str,
    admin_token: &str,
    absolute_deadline: Instant,
) -> Result<AlloyDbDiscovery, PostgresError> {
    let base = admin_endpoint.trim_end_matches('/');
    let enc_project = encode_uri_segment(&uri.project)?;
    let enc_location = encode_uri_segment(&uri.location)?;
    let enc_cluster = encode_uri_segment(&uri.cluster)?;
    let enc_instance = encode_uri_segment(&uri.instance)?;

    let cluster_path = format!(
        "{base}/v1alpha/projects/{enc_project}/locations/{enc_location}/clusters/{enc_cluster}"
    );
    let conn_info_url = format!("{cluster_path}/instances/{enc_instance}/connectionInfo");

    let info: ConnectionInfoResponse = connector
        .get_json(&conn_info_url, admin_token, absolute_deadline)
        .await?;

    let (host, peer_dns_name) = match ip_type {
        IpType::Private => {
            let ip = info
                .ip_address
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    PostgresError::Transport(
                        "AlloyDB connectionInfo response has no private ipAddress".to_string(),
                    )
                })?;
            (ip, None)
        }
        IpType::Public => {
            let ip = info
                .public_ip_address
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    PostgresError::Transport(
                        "AlloyDB connectionInfo response has no publicIpAddress".to_string(),
                    )
                })?;
            (ip, None)
        }
        IpType::Psc => {
            let dns = info
                .psc_dns_name
                .as_deref()
                .or(info.psc_auto_dns_name.as_deref())
                .map(|s| s.trim().trim_end_matches('.'))
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    PostgresError::Transport(
                        "AlloyDB connectionInfo response has no pscDnsName".to_string(),
                    )
                })?;
            (dns.clone(), Some(dns))
        }
    };

    let keypair = generate_ephemeral_rsa_keypair(absolute_deadline).await?;
    let cert_url = format!("{cluster_path}:generateClientCertificate");
    let req_body = GenerateClientCertificateRequest {
        public_key: keypair.public_key_pem(),
        cert_duration: "3600s",
        use_metadata_exchange: true,
    };
    let cert_resp: GenerateClientCertificateResponse = connector
        .post_json(&cert_url, admin_token, &req_body, absolute_deadline)
        .await?;

    if cert_resp.ca_cert.trim().is_empty() {
        return Err(PostgresError::Transport(
            "AlloyDB generateClientCertificate response is missing caCert".to_string(),
        ));
    }
    if cert_resp.pem_certificate_chain.is_empty() {
        return Err(PostgresError::Transport(
            "AlloyDB generateClientCertificate response has empty pemCertificateChain".to_string(),
        ));
    }

    let now_ms = connector.now_unix_ms();
    let client_identity =
        validate_issued_certificate_chain(&cert_resp.pem_certificate_chain, &keypair, now_ms)?;

    Ok(AlloyDbDiscovery {
        host,
        port: ALLOYDB_SERVER_PORT,
        peer_dns_name,
        server_ca_pem: cert_resp.ca_cert.into_bytes(),
        client_identity,
    })
}
