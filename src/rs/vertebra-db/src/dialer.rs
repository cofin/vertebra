//! Zero-sidecar Google Cloud SQL (`:3307`) and AlloyDB (`:5433`) mTLS dialers and metadata exchange.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use vertebra_core::proto::{
    WIRE_TYPE_LENGTH_DELIMITED, WIRE_TYPE_VARINT, put_bytes_field, put_varint_field,
    read_length_delimited, read_tag, read_varint, skip_field,
};

/// Authentication mode for Cloud SQL and AlloyDB connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u32)]
pub enum AuthType {
    /// Built-in database password authentication (`1` in AlloyDB `MetadataExchangeRequest`).
    Password = 1,
    /// Google Cloud IAM OAuth2 token authentication (`2` in AlloyDB `MetadataExchangeRequest`).
    Iam = 2,
}

impl AuthType {
    /// Parse an authentication mode from a string (`"iam"`, `"password"`, `"built_in"`).
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "iam" | "oauth2" | "2" => Self::Iam,
            _ => Self::Password,
        }
    }

    /// Convert a numeric code (`1` or `2`) to `AuthType`.
    pub fn from_code(code: u32) -> Self {
        match code {
            2 => Self::Iam,
            _ => Self::Password,
        }
    }
}

/// Network IP selection mode for Cloud SQL and AlloyDB instances.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IpType {
    /// Public IPv4 endpoint.
    Public,
    /// Private VPC IPv4 endpoint.
    Private,
    /// Private Service Connect (PSC) DNS endpoint.
    Psc,
}

impl IpType {
    /// Parse an IP type string (`"PUBLIC"`, `"PRIVATE"`, `"PSC"`).
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "PUBLIC" => Ok(Self::Public),
            "PRIVATE" => Ok(Self::Private),
            "PSC" => Ok(Self::Psc),
            other => Err(format!(
                "invalid IP type `{other}`; expected PUBLIC, PRIVATE, or PSC"
            )),
        }
    }
}

/// Parsed Google Cloud SQL instance connection name (`project:region:instance`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CloudSqlInstanceUri {
    pub project: String,
    pub region: String,
    pub instance: String,
}

impl CloudSqlInstanceUri {
    /// Parse `project:region:instance` (supporting domain-scoped projects like `google.com:proj:region:inst`).
    pub fn parse(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        let parts: Vec<&str> = trimmed.split(':').collect();
        match parts.as_slice() {
            [project, region, instance]
                if !project.is_empty() && !region.is_empty() && !instance.is_empty() =>
            {
                Ok(Self {
                    project: (*project).to_string(),
                    region: (*region).to_string(),
                    instance: (*instance).to_string(),
                })
            }
            [domain, project, region, instance]
                if !domain.is_empty()
                    && !project.is_empty()
                    && !region.is_empty()
                    && !instance.is_empty() =>
            {
                Ok(Self {
                    project: format!("{domain}:{project}"),
                    region: (*region).to_string(),
                    instance: (*instance).to_string(),
                })
            }
            _ => Err(format!(
                "invalid Cloud SQL instance URI `{trimmed}`: expected 'project:region:instance'"
            )),
        }
    }

    /// Cloud SQL Admin API v1beta4 `generateEphemeralCert` path.
    pub fn admin_api_path(&self) -> String {
        format!(
            "/sql/v1beta4/projects/{}/instances/{}:generateEphemeralCert",
            self.project, self.instance
        )
    }

    /// Cloud SQL Admin API v1beta4 `generateEphemeralCert` full endpoint URL.
    pub fn generate_ephemeral_cert_url(&self, base_endpoint: &str) -> String {
        let base = base_endpoint.trim_end_matches('/');
        format!("{base}{}", self.admin_api_path())
    }

    /// Expected server certificate Subject CN (`project:instance`) on Cloud SQL port `:3307`.
    pub fn expected_server_cn(&self) -> String {
        format!("{}:{}", self.project, self.instance)
    }

    /// Default Cloud SQL server proxy mTLS port (`3307`).
    pub const fn default_server_port() -> u16 {
        3307
    }
}

/// Parsed Google Cloud AlloyDB instance URI (`projects/<p>/locations/<l>/clusters/<c>/instances/<i>`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AlloyDbClusterUri {
    pub project: String,
    pub location: String,
    pub cluster: String,
    pub instance: String,
}

impl AlloyDbClusterUri {
    /// Parse `projects/<p>/locations/<l>/clusters/<c>/instances/<i>`.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim().trim_matches('/');
        let parts: Vec<&str> = trimmed.split('/').collect();
        if parts.len() == 8
            && parts[0] == "projects"
            && parts[2] == "locations"
            && parts[4] == "clusters"
            && parts[6] == "instances"
            && !parts[1].is_empty()
            && !parts[3].is_empty()
            && !parts[5].is_empty()
            && !parts[7].is_empty()
        {
            Ok(Self {
                project: parts[1].to_string(),
                location: parts[3].to_string(),
                cluster: parts[5].to_string(),
                instance: parts[7].to_string(),
            })
        } else {
            Err(format!(
                "invalid AlloyDB cluster URI `{raw}`: expected 'projects/<p>/locations/<l>/clusters/<c>/instances/<i>'"
            ))
        }
    }

    /// AlloyDB API v1beta `generateClientCertificate` path.
    pub fn admin_api_path(&self) -> String {
        format!(
            "/v1beta/projects/{}/locations/{}/clusters/{}:generateClientCertificate",
            self.project, self.location, self.cluster
        )
    }

    /// AlloyDB API v1beta `generateClientCertificate` full endpoint URL.
    pub fn generate_client_cert_url(&self, base_endpoint: &str) -> String {
        let base = base_endpoint.trim_end_matches('/');
        format!("{base}{}", self.admin_api_path())
    }

    /// Default AlloyDB server proxy mTLS port (`5433`).
    pub const fn default_server_port() -> u16 {
        5433
    }
}

/// Encode a 4-byte big-endian length-prefixed AlloyDB `MetadataExchangeRequest` wire frame.
///
/// Protobuf schema:
/// - field 1 (tag `0x0a`, string): `user_agent`
/// - field 2 (tag `0x10`, varint): `auth_type` (`1` = `DB_NATIVE` / `Password`, `2` = `AUTO_IAM` / `Iam`)
/// - field 3 (tag `0x1a`, string): `oauth2_token`
pub fn encode_alloydb_metadata_exchange(
    user_agent: &str,
    oauth2_token: &str,
    auth_type: AuthType,
) -> Bytes {
    let mut proto = BytesMut::with_capacity(user_agent.len() + oauth2_token.len() + 16);
    if !user_agent.is_empty() {
        put_bytes_field(&mut proto, 1, user_agent.as_bytes());
    }
    put_varint_field(&mut proto, 2, auth_type as u64);
    if !oauth2_token.is_empty() {
        put_bytes_field(&mut proto, 3, oauth2_token.as_bytes());
    }

    let mut framed = BytesMut::with_capacity(4 + proto.len());
    framed.put_u32(proto.len() as u32);
    framed.extend_from_slice(&proto);
    framed.freeze()
}

/// Encode a 4-byte big-endian length-prefixed AlloyDB `MetadataExchangeResponse` wire frame.
///
/// Protobuf schema:
/// - field 1 (tag `0x08`, varint): `response_code` (`1` = `OK`, `2` = `ERROR`)
/// - field 2 (tag `0x12`, string): `error_message`
pub fn encode_alloydb_metadata_response(response_code: u32, error_message: &str) -> Bytes {
    let mut proto = BytesMut::with_capacity(error_message.len() + 12);
    put_varint_field(&mut proto, 1, response_code as u64);
    if !error_message.is_empty() {
        put_bytes_field(&mut proto, 2, error_message.as_bytes());
    }

    let mut framed = BytesMut::with_capacity(4 + proto.len());
    framed.put_u32(proto.len() as u32);
    framed.extend_from_slice(&proto);
    framed.freeze()
}

/// Decode a 4-byte big-endian length-prefixed AlloyDB `MetadataExchangeResponse` wire frame.
///
/// Returns `(response_code, error_message)`.
pub fn decode_alloydb_metadata_response(buf: &[u8]) -> Result<(u32, String), String> {
    if buf.len() < 4 {
        return Err("frame too short for 4-byte length prefix".to_string());
    }
    let body_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + body_len {
        return Err(format!(
            "truncated MetadataExchangeResponse body: expected {body_len} bytes, got {}",
            buf.len() - 4
        ));
    }
    decode_alloydb_metadata_response_body(&buf[4..4 + body_len])
}

fn decode_alloydb_metadata_response_body(body: &[u8]) -> Result<(u32, String), String> {
    let mut response_code = 1u32;
    let mut error_message = String::new();
    let mut idx = 0usize;

    while idx < body.len() {
        let (field_num, wire_type) = read_tag(body, &mut idx)?;
        match wire_type {
            WIRE_TYPE_VARINT => {
                let val = read_varint(body, &mut idx)?;
                if field_num == 1 {
                    response_code = val as u32;
                }
            }
            WIRE_TYPE_LENGTH_DELIMITED => {
                let slice = read_length_delimited(body, &mut idx).map_err(|_| {
                    "truncated string field in MetadataExchangeResponse".to_string()
                })?;
                if field_num == 2 {
                    error_message = String::from_utf8_lossy(slice).into_owned();
                }
            }
            other => {
                skip_field(body, &mut idx, other).map_err(|_| {
                    format!("unsupported protobuf wire type {other} in MetadataExchangeResponse")
                })?;
            }
        }
    }

    Ok((response_code, error_message))
}

/// Generate an ephemeral X.509 client certificate and private key PEM pair using `rcgen`.
pub fn generate_ephemeral_keypair_pem() -> Result<(String, String), String> {
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .map_err(|e| format!("failed to initialize certificate params: {e}"))?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "vertebra-ephemeral-client");
    let key_pair =
        rcgen::KeyPair::generate().map_err(|e| format!("failed to generate keypair: {e}"))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| format!("failed to self-sign ephemeral certificate: {e}"))?;
    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Execute an AlloyDB `MetadataExchangeRequest` / `MetadataExchangeResponse` handshake over an async stream.
pub async fn exchange_alloydb_metadata_over_stream<S>(
    stream: &mut S,
    user_agent: &str,
    oauth2_token: &str,
    auth_type: AuthType,
) -> Result<(u32, String), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let req = encode_alloydb_metadata_exchange(user_agent, oauth2_token, auth_type);
    stream
        .write_all(&req)
        .await
        .map_err(|e| format!("failed to write AlloyDB MetadataExchangeRequest: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("failed to flush AlloyDB MetadataExchangeRequest: {e}"))?;

    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|e| format!("failed to read AlloyDB MetadataExchangeResponse header: {e}"))?;
    let body_len = u32::from_be_bytes(header) as usize;
    if body_len > 64 * 1024 {
        return Err(format!(
            "AlloyDB MetadataExchangeResponse body exceeds 64KiB limit ({body_len} bytes)"
        ));
    }
    let mut body = vec![0u8; body_len];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|e| format!("failed to read AlloyDB MetadataExchangeResponse body: {e}"))?;

    let (code, err_msg) = decode_alloydb_metadata_response_body(&body)?;
    if code != 1 {
        return Err(format!(
            "AlloyDB MetadataExchange rejected (code={code}): {err_msg}"
        ));
    }
    Ok((code, err_msg))
}

/// Zero-sidecar Google Cloud SQL (`:3307`) mTLS dialer.
#[derive(Clone, Debug)]
pub struct CloudSqlDialer {
    pub uri: CloudSqlInstanceUri,
    pub ip_type: IpType,
    pub auth_type: AuthType,
    pub server_port: u16,
}

impl CloudSqlDialer {
    /// Create a new `CloudSqlDialer` from an instance URI (`project:region:instance`).
    pub fn new(instance_uri: &str, ip_type: IpType, auth_type: AuthType) -> Result<Self, String> {
        let uri = CloudSqlInstanceUri::parse(instance_uri)?;
        Ok(Self {
            uri,
            ip_type,
            auth_type,
            server_port: CloudSqlInstanceUri::default_server_port(),
        })
    }

    /// Return the Cloud SQL Admin API path for `generateEphemeralCert`.
    pub fn admin_api_path(&self) -> String {
        self.uri.admin_api_path()
    }

    /// Generate an ephemeral X.509 certificate and private key PEM pair.
    pub fn generate_ephemeral_keypair(&self) -> Result<(String, String), String> {
        generate_ephemeral_keypair_pem()
    }
}

/// Zero-sidecar Google Cloud AlloyDB (`:5433`) mTLS dialer.
#[derive(Clone, Debug)]
pub struct AlloyDbDialer {
    pub uri: AlloyDbClusterUri,
    pub ip_type: IpType,
    pub auth_type: AuthType,
    pub server_port: u16,
}

impl AlloyDbDialer {
    /// Create a new `AlloyDbDialer` from a cluster instance URI.
    pub fn new(cluster_uri: &str, ip_type: IpType, auth_type: AuthType) -> Result<Self, String> {
        let uri = AlloyDbClusterUri::parse(cluster_uri)?;
        Ok(Self {
            uri,
            ip_type,
            auth_type,
            server_port: AlloyDbClusterUri::default_server_port(),
        })
    }

    /// Encode a length-prefixed `MetadataExchangeRequest` frame for this dialer's `auth_type`.
    pub fn encode_metadata_exchange(&self, user_agent: &str, oauth2_token: &str) -> Bytes {
        encode_alloydb_metadata_exchange(user_agent, oauth2_token, self.auth_type)
    }

    /// Execute the `MetadataExchangeRequest` / `MetadataExchangeResponse` handshake over `stream`.
    pub async fn exchange_metadata<S>(
        &self,
        stream: &mut S,
        user_agent: &str,
        oauth2_token: &str,
    ) -> Result<(u32, String), String>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        exchange_alloydb_metadata_over_stream(stream, user_agent, oauth2_token, self.auth_type)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloudsql_uri_parsing_and_paths() {
        let uri = CloudSqlInstanceUri::parse("my-project:us-central1:prod-pg").unwrap();
        assert_eq!(uri.project, "my-project");
        assert_eq!(uri.region, "us-central1");
        assert_eq!(uri.instance, "prod-pg");
        assert_eq!(uri.expected_server_cn(), "my-project:prod-pg");
        assert_eq!(CloudSqlInstanceUri::default_server_port(), 3307);
        assert_eq!(
            uri.admin_api_path(),
            "/sql/v1beta4/projects/my-project/instances/prod-pg:generateEphemeralCert"
        );

        let domain_uri =
            CloudSqlInstanceUri::parse("example.com:corp-proj:europe-west1:analytics").unwrap();
        assert_eq!(domain_uri.project, "example.com:corp-proj");
        assert_eq!(domain_uri.region, "europe-west1");
        assert_eq!(domain_uri.instance, "analytics");

        assert!(CloudSqlInstanceUri::parse("invalid-uri").is_err());
    }

    #[test]
    fn test_alloydb_uri_parsing_and_metadata_framing() {
        let uri =
            AlloyDbClusterUri::parse("projects/p1/locations/us-central1/clusters/c1/instances/i1")
                .unwrap();
        assert_eq!(uri.project, "p1");
        assert_eq!(uri.location, "us-central1");
        assert_eq!(uri.cluster, "c1");
        assert_eq!(uri.instance, "i1");
        assert_eq!(AlloyDbClusterUri::default_server_port(), 5433);

        let req =
            encode_alloydb_metadata_exchange("vertebra/0.1", "mock-oauth-token", AuthType::Iam);
        let body_len = u32::from_be_bytes([req[0], req[1], req[2], req[3]]) as usize;
        assert_eq!(body_len, req.len() - 4);
        assert_eq!(req[4], 0x0a);

        let resp_ok = encode_alloydb_metadata_response(1, "");
        let (code, msg) = decode_alloydb_metadata_response(&resp_ok).unwrap();
        assert_eq!(code, 1);
        assert_eq!(msg, "");

        let resp_err = encode_alloydb_metadata_response(2, "IAM token expired");
        let (code_err, msg_err) = decode_alloydb_metadata_response(&resp_err).unwrap();
        assert_eq!(code_err, 2);
        assert_eq!(msg_err, "IAM token expired");
    }

    #[test]
    fn test_ephemeral_keypair_generation() {
        let dialer =
            CloudSqlDialer::new("proj:us-central1:inst", IpType::Public, AuthType::Iam).unwrap();
        let (cert_pem, key_pem) = dialer.generate_ephemeral_keypair().unwrap();
        assert!(cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(key_pem.contains("BEGIN PRIVATE KEY"));
    }

    #[tokio::test]
    async fn test_alloydb_metadata_exchange_over_duplex_and_tcp() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let server_task = tokio::spawn(async move {
            let mut hdr = [0u8; 4];
            server.read_exact(&mut hdr).await.unwrap();
            let len = u32::from_be_bytes(hdr) as usize;
            let mut body = vec![0u8; len];
            server.read_exact(&mut body).await.unwrap();
            let resp = encode_alloydb_metadata_response(1, "");
            server.write_all(&resp).await.unwrap();
        });

        let (code, err) = exchange_alloydb_metadata_over_stream(
            &mut client,
            "vertebra-test",
            "mock-oauth-token",
            AuthType::Iam,
        )
        .await
        .unwrap();
        assert_eq!(code, 1);
        assert!(err.is_empty());
        server_task.await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tcp_server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; 4];
            sock.read_exact(&mut hdr).await.unwrap();
            let len = u32::from_be_bytes(hdr) as usize;
            let mut body = vec![0u8; len];
            sock.read_exact(&mut body).await.unwrap();
            let resp = encode_alloydb_metadata_response(1, "");
            sock.write_all(&resp).await.unwrap();
        });

        let mut tcp_client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let dialer = AlloyDbDialer::new(
            "projects/p/locations/l/clusters/c/instances/i",
            IpType::Private,
            AuthType::Iam,
        )
        .unwrap();
        let (code2, _) = dialer
            .exchange_metadata(&mut tcp_client, "vertebra-test", "mock-oauth-token")
            .await
            .unwrap();
        assert_eq!(code2, 1);
        tcp_server.await.unwrap();
    }
}
