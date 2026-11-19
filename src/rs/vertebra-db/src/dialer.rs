//! Cloud database mTLS and IAM dialers for Google Cloud SQL, AlloyDB, AWS RDS/Aurora/DSQL, and Azure Entra ID.

use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use chrono::DateTime;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use vertebra_core::proto::{
    WIRE_TYPE_LENGTH_DELIMITED, WIRE_TYPE_VARINT, put_bytes_field, put_varint_field,
    read_length_delimited, read_tag, read_varint, skip_field,
};

type HmacSha256 = Hmac<Sha256>;

const AWS_RDS_ROOT_CA_PEM: &str = include_str!("aws_rds_root_cas.pem");

/// Canonical Azure Entra OAuth2 scope for Azure Database for PostgreSQL and MySQL.
pub const AZURE_OSSRDBMS_SCOPE: &str = "https://ossrdbms-aad.database.windows.net/.default";

/// Canonical Azure Entra OAuth2 scope for Azure SQL Database and Managed Instance.
pub const AZURE_SQL_DATABASE_SCOPE: &str = "https://database.windows.net/.default";

/// Maximum SigV4 token lifetime in seconds for AWS RDS IAM database authentication (`900`).
pub const AWS_RDS_MAX_TTL_SECONDS: u32 = 900;

/// Maximum SigV4 token lifetime in seconds for AWS Aurora DSQL IAM authentication (`604_800`).
pub const AWS_DSQL_MAX_TTL_SECONDS: u32 = 604_800;

/// Return the bundled official AWS RDS Global Root CA PEM certificates (`rds-ca-2019`,
/// `rds-ca-rsa2048-g1`, `rds-ca-rsa4096-g1`, `rds-ca-ecc384-g1`).
pub fn aws_rds_root_ca_pem() -> &'static str {
    AWS_RDS_ROOT_CA_PEM
}

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

/// Response status code decoded from an AlloyDB `MetadataExchangeResponse` frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetadataExchangeResponseCode {
    /// `0` (`RESPONSE_CODE_UNSPECIFIED`).
    Unspecified,
    /// `1` (`OK`).
    Ok,
    /// `2` (`ERROR`).
    Error,
    /// Non-standard numeric response code returned by the proxy.
    Unknown(u32),
}

impl MetadataExchangeResponseCode {
    /// Convert a raw protobuf varint code (`1` = `OK`, `2` = `ERROR`) into [`MetadataExchangeResponseCode`].
    pub const fn from_raw(code: u32) -> Self {
        match code {
            0 => Self::Unspecified,
            1 => Self::Ok,
            2 => Self::Error,
            other => Self::Unknown(other),
        }
    }

    /// Return the numeric wire code.
    pub const fn as_u32(self) -> u32 {
        match self {
            Self::Unspecified => 0,
            Self::Ok => 1,
            Self::Error => 2,
            Self::Unknown(code) => code,
        }
    }

    /// Return the canonical uppercase AlloyDB protocol status label (`OK`, `ERROR`, `UNSPECIFIED`, `UNKNOWN`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::Ok => "OK",
            Self::Error => "ERROR",
            Self::Unknown(_) => "UNKNOWN",
        }
    }
}

/// Structured AlloyDB `MetadataExchangeResponse` payload retaining status code and full diagnostic text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataExchangeResponse {
    pub response_code: MetadataExchangeResponseCode,
    pub raw_code: u32,
    pub error_message: String,
}

impl MetadataExchangeResponse {
    /// Decode a 4-byte big-endian length-prefixed AlloyDB `MetadataExchangeResponse` wire frame.
    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        decode_alloydb_metadata_response_details(buf)
    }

    /// Return `true` when `response_code == MetadataExchangeResponseCode::Ok` and `error_message` is empty.
    pub fn is_ok(&self) -> bool {
        self.response_code == MetadataExchangeResponseCode::Ok && self.error_message.is_empty()
    }
}

/// Decode a 4-byte big-endian length-prefixed AlloyDB `MetadataExchangeResponse` wire frame
/// into a structured [`MetadataExchangeResponse`].
pub fn decode_alloydb_metadata_response_details(
    buf: &[u8],
) -> Result<MetadataExchangeResponse, String> {
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
    decode_alloydb_metadata_response_body_struct(&buf[4..4 + body_len])
}

/// Decode a 4-byte big-endian length-prefixed AlloyDB `MetadataExchangeResponse` wire frame.
///
/// Returns `(response_code, error_message)`.
pub fn decode_alloydb_metadata_response(buf: &[u8]) -> Result<(u32, String), String> {
    let resp = decode_alloydb_metadata_response_details(buf)?;
    Ok((resp.raw_code, resp.error_message))
}

fn decode_alloydb_metadata_response_body_struct(
    body: &[u8],
) -> Result<MetadataExchangeResponse, String> {
    let mut saw_code = false;
    let mut raw_code = 0u32;
    let mut error_message = String::new();
    let mut idx = 0usize;

    while idx < body.len() {
        let (field_num, wire_type) = read_tag(body, &mut idx)?;
        match wire_type {
            WIRE_TYPE_VARINT => {
                let val = read_varint(body, &mut idx)?;
                if field_num == 1 {
                    raw_code = u32::try_from(val).unwrap_or(u32::MAX);
                    saw_code = true;
                }
            }
            WIRE_TYPE_LENGTH_DELIMITED => {
                let slice = read_length_delimited(body, &mut idx).map_err(|_| {
                    "truncated string field in MetadataExchangeResponse".to_string()
                })?;
                if field_num == 2 {
                    let text = String::from_utf8_lossy(slice);
                    if error_message.is_empty() {
                        error_message = text.into_owned();
                    } else {
                        error_message.push_str("; ");
                        error_message.push_str(&text);
                    }
                }
            }
            other => {
                skip_field(body, &mut idx, other).map_err(|_| {
                    format!("unsupported protobuf wire type {other} in MetadataExchangeResponse")
                })?;
            }
        }
    }

    if !saw_code && !error_message.is_empty() {
        raw_code = MetadataExchangeResponseCode::Error.as_u32();
    }

    Ok(MetadataExchangeResponse {
        response_code: MetadataExchangeResponseCode::from_raw(raw_code),
        raw_code,
        error_message,
    })
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

    let resp = decode_alloydb_metadata_response_body_struct(&body)?;
    if !resp.is_ok() {
        let label = if resp.response_code == MetadataExchangeResponseCode::Ok {
            MetadataExchangeResponseCode::Error.as_str()
        } else {
            resp.response_code.as_str()
        };
        let detail = if resp.error_message.is_empty() {
            "no diagnostic message provided"
        } else {
            resp.error_message.as_str()
        };
        return Err(format!(
            "AlloyDB MetadataExchange rejected ({label}, code={}): {detail}",
            resp.raw_code
        ));
    }
    Ok((resp.raw_code, resp.error_message))
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

fn is_valid_dns_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 63
        && !label.starts_with('-')
        && !label.ends_with('-')
        && label
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

fn is_valid_dns_hostname(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && !host.starts_with('.')
        && !host.ends_with('.')
        && host.split('.').all(is_valid_dns_label)
}

fn is_valid_aws_region_label(region: &str, require_hyphen: bool) -> bool {
    is_valid_dns_label(region) && (!require_hyphen || region.contains('-'))
}

fn parse_validated_port(raw_port: &str, context: &str) -> Result<u16, String> {
    let parsed = raw_port
        .parse::<u32>()
        .map_err(|_| format!("invalid port `{raw_port}` in {context}"))?;
    if parsed == 0 || parsed > u16::MAX as u32 {
        return Err(format!(
            "invalid port `{raw_port}` in {context}: port must be between 1 and 65535"
        ));
    }
    Ok(parsed as u16)
}

fn split_validated_endpoint_and_port(
    raw_endpoint: &str,
    explicit_port: Option<u16>,
    default_port: u16,
    service_label: &str,
) -> Result<(String, u16), String> {
    if raw_endpoint.is_empty() {
        return Err(format!("{service_label} endpoint host must not be empty"));
    }
    if raw_endpoint.chars().any(char::is_whitespace) {
        return Err(format!(
            "{service_label} endpoint `{raw_endpoint}` must not contain whitespace"
        ));
    }
    if raw_endpoint.contains("://") {
        return Err(format!(
            "{service_label} endpoint `{raw_endpoint}` must not include a URL scheme"
        ));
    }
    if raw_endpoint
        .chars()
        .any(|c| matches!(c, '/' | '?' | '#' | '@' | '\\') || c.is_control())
    {
        return Err(format!(
            "{service_label} endpoint `{raw_endpoint}` must be a bare hostname without path, query, or userinfo"
        ));
    }
    if explicit_port == Some(0) {
        return Err(format!("{service_label} port must be between 1 and 65535"));
    }

    let (host_part, embedded_port) = if let Some(rest) = raw_endpoint.strip_prefix('[') {
        let Some((ipv6_str, tail)) = rest.split_once(']') else {
            return Err(format!(
                "invalid bracketed IPv6 {service_label} endpoint `{raw_endpoint}`"
            ));
        };
        ipv6_str.parse::<Ipv6Addr>().map_err(|_| {
            format!("invalid IPv6 literal `{ipv6_str}` in {service_label} endpoint")
        })?;
        let port_opt = if tail.is_empty() {
            None
        } else if let Some(port_str) = tail.strip_prefix(':') {
            Some(parse_validated_port(port_str, service_label)?)
        } else {
            return Err(format!(
                "unexpected trailing characters `{tail}` in {service_label} endpoint `{raw_endpoint}`"
            ));
        };
        (format!("[{}]", ipv6_str.to_ascii_lowercase()), port_opt)
    } else if raw_endpoint.parse::<Ipv6Addr>().is_ok() {
        (raw_endpoint.to_ascii_lowercase(), None)
    } else if let Some((h, p)) = raw_endpoint.rsplit_once(':') {
        if h.contains(':') || h.is_empty() {
            return Err(format!("invalid {service_label} endpoint `{raw_endpoint}`"));
        }
        let parsed_port = parse_validated_port(p, service_label)?;
        (h.to_ascii_lowercase(), Some(parsed_port))
    } else {
        (raw_endpoint.to_ascii_lowercase(), None)
    };

    let is_ip = host_part.starts_with('[')
        || host_part.parse::<Ipv6Addr>().is_ok()
        || host_part.parse::<Ipv4Addr>().is_ok();
    if !is_ip && !is_valid_dns_hostname(&host_part) {
        return Err(format!("invalid {service_label} hostname `{raw_endpoint}`"));
    }

    let resolved_port = match (embedded_port, explicit_port) {
        (Some(ep), Some(xp)) if ep != xp => {
            return Err(format!(
                "conflicting {service_label} ports: endpoint specifies {ep} while port={xp}"
            ));
        }
        (Some(ep), _) => ep,
        (None, Some(xp)) => xp,
        (None, None) => default_port,
    };
    if resolved_port == 0 {
        return Err(format!("{service_label} port must be between 1 and 65535"));
    }

    Ok((host_part, resolved_port))
}

fn uri_encode_sigv4(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "%{b:02X}");
        }
    }
    out
}

fn hmac_sha256_bytes(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac =
        HmacSha256::new_from_slice(key).expect("HMAC-SHA256 accepts arbitrary key length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

fn current_unix_timestamp_f64() -> Result<f64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .map_err(|e| format!("system clock precedes UNIX_EPOCH: {e}"))
}

#[allow(clippy::too_many_arguments)]
fn sign_sigv4_presigned_token_rs(
    host_header: &str,
    region: &str,
    service: &str,
    action: &str,
    ttl_seconds: u32,
    max_ttl_seconds: u32,
    now_ts: f64,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: Option<&str>,
    db_user: Option<&str>,
) -> Result<String, String> {
    if access_key_id.trim().is_empty() || secret_access_key.trim().is_empty() {
        return Err(
            "AWS IAM token signing requires non-empty access_key_id and secret_access_key"
                .to_string(),
        );
    }
    if ttl_seconds == 0 || ttl_seconds > max_ttl_seconds {
        return Err(format!(
            "ttl_seconds must be between 1 and {max_ttl_seconds} for {service}, got {ttl_seconds}"
        ));
    }
    if !now_ts.is_finite() || !(0.0..=253_402_300_799.0).contains(&now_ts) {
        return Err(format!(
            "now_ts must be a finite valid UTC epoch timestamp, got {now_ts}"
        ));
    }

    let total_micros = (now_ts * 1_000_000.0).round() as i64;
    let secs = total_micros.div_euclid(1_000_000);
    let nanos = (total_micros.rem_euclid(1_000_000) as u32) * 1_000;
    let dt = DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| format!("out-of-range UTC timestamp {now_ts}"))?;

    let amz_date = dt.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = dt.format("%Y%m%d").to_string();
    let credential_scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let credential_value = format!("{access_key_id}/{credential_scope}");
    let expires_str = ttl_seconds.to_string();

    let mut params: Vec<(&str, &str)> = Vec::with_capacity(8);
    params.push(("Action", action));
    params.push(("X-Amz-Algorithm", "AWS4-HMAC-SHA256"));
    params.push(("X-Amz-Credential", &credential_value));
    params.push(("X-Amz-Date", &amz_date));
    params.push(("X-Amz-Expires", &expires_str));
    params.push(("X-Amz-SignedHeaders", "host"));
    if let Some(user) = db_user {
        params.push(("DBUser", user));
    }
    if let Some(tok) = session_token.filter(|s| !s.is_empty()) {
        params.push(("X-Amz-Security-Token", tok));
    }

    let mut encoded_pairs: Vec<(String, String)> = params
        .into_iter()
        .map(|(k, v)| (uri_encode_sigv4(k), uri_encode_sigv4(v)))
        .collect();
    encoded_pairs.sort();

    let canonical_query = encoded_pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    const EMPTY_SHA256_HEX: &str =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let canonical_request =
        format!("GET\n/\n{canonical_query}\nhost:{host_header}\n\nhost\n{EMPTY_SHA256_HEX}");
    let hashed_canonical = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let string_to_sign =
        format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashed_canonical}");

    let mut secret_prefixed = Vec::with_capacity(4 + secret_access_key.len());
    secret_prefixed.extend_from_slice(b"AWS4");
    secret_prefixed.extend_from_slice(secret_access_key.as_bytes());
    let k_date = hmac_sha256_bytes(&secret_prefixed, date_stamp.as_bytes());
    let k_region = hmac_sha256_bytes(&k_date, region.as_bytes());
    let k_service = hmac_sha256_bytes(&k_region, service.as_bytes());
    let k_signing = hmac_sha256_bytes(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256_bytes(&k_signing, string_to_sign.as_bytes()));

    Ok(format!(
        "{host_header}/?{canonical_query}&X-Amz-Signature={signature}"
    ))
}

/// Native AWS RDS, Aurora, and RDS Proxy IAM authentication and TLS dialer metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AwsRdsDialer {
    pub endpoint_host: String,
    pub server_port: u16,
    pub region: String,
    pub username: String,
    pub identifier: Option<String>,
    pub cluster_or_proxy: Option<String>,
    pub is_cluster_endpoint: bool,
    pub is_proxy_endpoint: bool,
}

impl AwsRdsDialer {
    /// Default PostgreSQL server port (`5432`) when omitted.
    pub const fn default_server_port() -> u16 {
        5432
    }

    /// Construct an `AwsRdsDialer` from `<id>.<cluster-or-proxy>.<region>.rds.amazonaws.com[.cn]`
    /// or explicit `(endpoint_host, port, region, username)`.
    pub fn new(
        endpoint_host: &str,
        port: Option<u16>,
        region: Option<&str>,
        username: &str,
    ) -> Result<Self, String> {
        let (host, resolved_port) = split_validated_endpoint_and_port(
            endpoint_host,
            port,
            Self::default_server_port(),
            "AWS RDS",
        )?;

        let trimmed_user = username.trim();
        if trimmed_user.is_empty() {
            return Err("AWS RDS username must not be empty".to_string());
        }

        let explicit_region = match region {
            Some(r) => {
                if r.is_empty() || r.chars().any(char::is_whitespace) {
                    return Err(
                        "AWS RDS region must be a non-empty string without whitespace".to_string(),
                    );
                }
                let lower = r.to_ascii_lowercase();
                if !is_valid_aws_region_label(&lower, false) {
                    return Err(format!("invalid AWS RDS region `{r}`"));
                }
                Some(lower)
            }
            None => None,
        };

        let rds_prefix = host
            .strip_suffix(".rds.amazonaws.com.cn")
            .or_else(|| host.strip_suffix(".rds.amazonaws.com"));

        let (identifier, cluster_or_proxy, parsed_region) = if let Some(prefix) = rds_prefix {
            let labels: Vec<&str> = prefix.split('.').collect();
            if labels.len() < 3 {
                return Err(format!(
                    "invalid AWS RDS endpoint `{endpoint_host}`: expected '<id>.<cluster-or-proxy>.<region>.rds.amazonaws.com[.cn]'"
                ));
            }
            let dns_region = labels[labels.len() - 1];
            if !is_valid_aws_region_label(dns_region, true) {
                return Err(format!(
                    "invalid AWS region `{dns_region}` in RDS endpoint `{endpoint_host}`"
                ));
            }
            let id = labels[0].to_string();
            let middle = labels[1..labels.len() - 1].join(".");
            (Some(id), Some(middle), Some(dns_region.to_string()))
        } else if host == "rds.amazonaws.com" || host == "rds.amazonaws.com.cn" {
            return Err(format!(
                "invalid AWS RDS endpoint `{endpoint_host}`: missing instance and region labels"
            ));
        } else {
            (None, None, None)
        };

        let resolved_region = match (explicit_region, parsed_region) {
            (Some(exp), _) => exp,
            (None, Some(parsed)) => parsed,
            (None, None) => {
                return Err(format!(
                    "cannot infer AWS region from RDS endpoint `{endpoint_host}`; pass explicit region"
                ));
            }
        };

        let is_cluster_endpoint = cluster_or_proxy
            .as_deref()
            .is_some_and(|mid| mid.starts_with("cluster-") || mid.contains(".cluster-"));
        let is_proxy_endpoint = cluster_or_proxy
            .as_deref()
            .is_some_and(|mid| mid.starts_with("proxy-") || mid.contains(".proxy-"));

        Ok(Self {
            endpoint_host: host,
            server_port: resolved_port,
            region: resolved_region,
            username: trimmed_user.to_string(),
            identifier,
            cluster_or_proxy,
            is_cluster_endpoint,
            is_proxy_endpoint,
        })
    }

    /// Return the bundled AWS RDS Global Root CA PEM bundle.
    pub fn ca_pem(&self) -> &'static str {
        aws_rds_root_ca_pem()
    }

    /// Compute an HMAC-SHA256 SigV4 `rds-db` `Action=connect` presigned IAM authentication token.
    pub fn sign_iam_token(
        &self,
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<&str>,
        ttl_seconds: u32,
        now_ts: Option<f64>,
    ) -> Result<String, String> {
        let ts = match now_ts {
            Some(v) => v,
            None => current_unix_timestamp_f64()?,
        };
        let host_header = format!("{}:{}", self.endpoint_host, self.server_port);
        sign_sigv4_presigned_token_rs(
            &host_header,
            &self.region,
            "rds-db",
            "connect",
            ttl_seconds,
            AWS_RDS_MAX_TTL_SECONDS,
            ts,
            access_key_id,
            secret_access_key,
            session_token,
            Some(&self.username),
        )
    }
}

/// Native AWS Aurora DSQL IAM (`DbConnect` / `DbConnectAdmin`) and TLS dialer metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AwsAuroraDsqlDialer {
    pub endpoint_host: String,
    pub server_port: u16,
    pub region: String,
    pub cluster_id: Option<String>,
    pub admin: bool,
}

impl AwsAuroraDsqlDialer {
    /// Default Aurora DSQL PostgreSQL-compatible server port (`5432`).
    pub const fn default_server_port() -> u16 {
        5432
    }

    /// Construct an `AwsAuroraDsqlDialer` from `<cluster>.dsql.<region>.on.aws` or explicit `(endpoint_host, port, region, admin)`.
    pub fn new(
        endpoint_host: &str,
        port: Option<u16>,
        region: Option<&str>,
        admin: bool,
    ) -> Result<Self, String> {
        let (host, resolved_port) = split_validated_endpoint_and_port(
            endpoint_host,
            port,
            Self::default_server_port(),
            "AWS Aurora DSQL",
        )?;

        let explicit_region = match region {
            Some(r) => {
                if r.is_empty() || r.chars().any(char::is_whitespace) {
                    return Err(
                        "AWS Aurora DSQL region must be a non-empty string without whitespace"
                            .to_string(),
                    );
                }
                let lower = r.to_ascii_lowercase();
                if !is_valid_aws_region_label(&lower, false) {
                    return Err(format!("invalid AWS Aurora DSQL region `{r}`"));
                }
                Some(lower)
            }
            None => None,
        };

        let (cluster_id, parsed_region) = if let Some(prefix) = host.strip_suffix(".on.aws") {
            let labels: Vec<&str> = prefix.split('.').collect();
            if labels.len() != 3 {
                return Err(format!(
                    "invalid AWS Aurora DSQL endpoint `{endpoint_host}`: expected '<cluster>.dsql.<region>.on.aws'"
                ));
            }
            let dsql_tag = labels[1];
            let valid_dsql_tag = dsql_tag == "dsql"
                || dsql_tag
                    .strip_prefix("dsql-")
                    .is_some_and(|suffix| !suffix.is_empty());
            if !valid_dsql_tag {
                return Err(format!(
                    "invalid AWS Aurora DSQL service label `{dsql_tag}` in `{endpoint_host}`; expected 'dsql'"
                ));
            }
            let dns_region = labels[2];
            if !is_valid_aws_region_label(dns_region, true) {
                return Err(format!(
                    "invalid AWS region `{dns_region}` in Aurora DSQL endpoint `{endpoint_host}`"
                ));
            }
            (Some(labels[0].to_string()), Some(dns_region.to_string()))
        } else if host == "on.aws" {
            return Err(format!(
                "invalid AWS Aurora DSQL endpoint `{endpoint_host}`"
            ));
        } else {
            (None, None)
        };

        let resolved_region = match (explicit_region, parsed_region) {
            (Some(exp), _) => exp,
            (None, Some(parsed)) => parsed,
            (None, None) => {
                return Err(format!(
                    "cannot infer AWS region from Aurora DSQL endpoint `{endpoint_host}`; expected '<cluster>.dsql.<region>.on.aws' or explicit region"
                ));
            }
        };

        Ok(Self {
            endpoint_host: host,
            server_port: resolved_port,
            region: resolved_region,
            cluster_id,
            admin,
        })
    }

    /// Return the SigV4 `dsql` action (`"DbConnectAdmin"` when `admin` is true, otherwise `"DbConnect"`).
    pub const fn action(&self) -> &'static str {
        if self.admin {
            "DbConnectAdmin"
        } else {
            "DbConnect"
        }
    }

    /// Compute an HMAC-SHA256 SigV4 `dsql` presigned IAM authentication token (`DBUser` omitted).
    pub fn sign_iam_token(
        &self,
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<&str>,
        ttl_seconds: u32,
        now_ts: Option<f64>,
    ) -> Result<String, String> {
        let ts = match now_ts {
            Some(v) => v,
            None => current_unix_timestamp_f64()?,
        };
        sign_sigv4_presigned_token_rs(
            &self.endpoint_host,
            &self.region,
            "dsql",
            self.action(),
            ttl_seconds,
            AWS_DSQL_MAX_TTL_SECONDS,
            ts,
            access_key_id,
            secret_access_key,
            session_token,
            None,
        )
    }
}

/// Supported database engines for Azure Entra ID database authentication.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AzureDatabaseEngine {
    /// Azure Database for PostgreSQL (`5432`, `ossrdbms-aad`).
    Postgres,
    /// Azure Database for MySQL (`3306`, `ossrdbms-aad`).
    Mysql,
    /// Azure SQL Database / Managed Instance (`1433`, `database.windows.net`).
    Mssql,
}

impl AzureDatabaseEngine {
    /// Parse an engine identifier (`"postgres"`, `"mysql"`, `"mssql"`).
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.is_empty() || raw.chars().any(char::is_whitespace) {
            return Err(
                "Azure database engine must be a non-empty string without whitespace".to_string(),
            );
        }
        match raw.to_ascii_lowercase().as_str() {
            "postgres" | "postgresql" | "pg" => Ok(Self::Postgres),
            "mysql" | "mariadb" => Ok(Self::Mysql),
            "mssql" | "sqlserver" | "azure_sql" | "azuresql" | "tsql" => Ok(Self::Mssql),
            other => Err(format!(
                "unsupported Azure database engine `{other}`; expected postgres, mysql, or mssql"
            )),
        }
    }

    /// Canonical lowercase engine identifier (`"postgres"`, `"mysql"`, `"mssql"`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
            Self::Mssql => "mssql",
        }
    }

    /// Default TCP port for the Azure database engine (`5432`, `3306`, or `1433`).
    pub const fn default_port(self) -> u16 {
        match self {
            Self::Postgres => 5432,
            Self::Mysql => 3306,
            Self::Mssql => 1433,
        }
    }

    /// Canonical Microsoft Entra ID OAuth2 scope for the Azure database engine.
    pub const fn scope(self) -> &'static str {
        match self {
            Self::Postgres | Self::Mysql => AZURE_OSSRDBMS_SCOPE,
            Self::Mssql => AZURE_SQL_DATABASE_SCOPE,
        }
    }
}

/// Native Azure Entra ID database endpoint parser and scope/port dialer metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AzureEntraDialer {
    pub endpoint_host: String,
    pub server_port: u16,
    pub engine: AzureDatabaseEngine,
    pub server_name: Option<String>,
}

impl AzureEntraDialer {
    /// Construct an `AzureEntraDialer` from `*.postgres.database.azure.com`,
    /// `*.mysql.database.azure.com`, `*.database.windows.net`, or explicit `(endpoint_host, port, engine)`.
    pub fn new(
        endpoint_host: &str,
        port: Option<u16>,
        engine: Option<&str>,
    ) -> Result<Self, String> {
        let explicit_engine = match engine {
            Some(e) => Some(AzureDatabaseEngine::parse(e)?),
            None => None,
        };

        let provisional_default_port =
            explicit_engine.map_or(5432, AzureDatabaseEngine::default_port);
        let (host, parsed_or_provisional_port) = split_validated_endpoint_and_port(
            endpoint_host,
            port,
            provisional_default_port,
            "Azure Entra",
        )?;

        let inferred = Self::infer_engine_and_server(&host)?;
        let (resolved_engine, server_name) = match (explicit_engine, inferred) {
            (Some(exp), Some((inf_eng, _srv))) if exp != inf_eng => {
                return Err(format!(
                    "conflicting Azure database engine: endpoint `{endpoint_host}` implies `{}` while engine=`{}`",
                    inf_eng.as_str(),
                    exp.as_str()
                ));
            }
            (Some(exp), Some((_, srv))) => (exp, Some(srv)),
            (Some(exp), None) => (exp, None),
            (None, Some((inf_eng, srv))) => (inf_eng, Some(srv)),
            (None, None) => {
                return Err(format!(
                    "cannot infer Azure database engine from endpoint `{endpoint_host}`; expected '*.postgres.database.azure.com', '*.mysql.database.azure.com', '*.database.windows.net', or explicit engine"
                ));
            }
        };

        let has_embedded_or_explicit_port = port.is_some() || {
            if endpoint_host.starts_with('[') {
                endpoint_host.contains("]:")
            } else if endpoint_host.parse::<Ipv6Addr>().is_ok() {
                false
            } else {
                endpoint_host.contains(':')
            }
        };
        let server_port = if has_embedded_or_explicit_port {
            parsed_or_provisional_port
        } else {
            resolved_engine.default_port()
        };

        Ok(Self {
            endpoint_host: host,
            server_port,
            engine: resolved_engine,
            server_name,
        })
    }

    fn infer_engine_and_server(
        host: &str,
    ) -> Result<Option<(AzureDatabaseEngine, String)>, String> {
        const PG_SUFFIXES: &[&str] = &[
            ".postgres.database.azure.com",
            ".postgres.cosmos.azure.com",
            ".postgres.database.chinacloudapi.cn",
            ".postgres.database.usgovcloudapi.net",
        ];
        const MYSQL_SUFFIXES: &[&str] = &[
            ".mysql.database.azure.com",
            ".mysql.database.chinacloudapi.cn",
            ".mysql.database.usgovcloudapi.net",
        ];
        const MSSQL_SUFFIXES: &[&str] = &[
            ".database.windows.net",
            ".sql.azuresynapse.net",
            ".database.chinacloudapi.cn",
            ".database.usgovcloudapi.net",
        ];

        const BARE_AZURE_DOMAINS: &[&str] = &[
            "postgres.database.azure.com",
            "postgres.cosmos.azure.com",
            "postgres.database.chinacloudapi.cn",
            "postgres.database.usgovcloudapi.net",
            "mysql.database.azure.com",
            "mysql.database.chinacloudapi.cn",
            "mysql.database.usgovcloudapi.net",
            "database.windows.net",
            "sql.azuresynapse.net",
            "database.azure.com",
            "ossrdbms-aad.database.windows.net",
        ];
        if BARE_AZURE_DOMAINS.contains(&host) {
            return Err(format!(
                "invalid Azure database endpoint `{host}`: missing server hostname label"
            ));
        }

        for suffix in PG_SUFFIXES {
            if let Some(prefix) = host.strip_suffix(suffix) {
                let first = prefix.split('.').next().unwrap_or("");
                if first.is_empty() {
                    return Err(format!("invalid Azure PostgreSQL endpoint `{host}`"));
                }
                return Ok(Some((AzureDatabaseEngine::Postgres, first.to_string())));
            }
        }
        for suffix in MYSQL_SUFFIXES {
            if let Some(prefix) = host.strip_suffix(suffix) {
                let first = prefix.split('.').next().unwrap_or("");
                if first.is_empty() {
                    return Err(format!("invalid Azure MySQL endpoint `{host}`"));
                }
                return Ok(Some((AzureDatabaseEngine::Mysql, first.to_string())));
            }
        }
        for suffix in MSSQL_SUFFIXES {
            if let Some(prefix) = host.strip_suffix(suffix) {
                let first = prefix.split('.').next().unwrap_or("");
                if first.is_empty() {
                    return Err(format!("invalid Azure SQL endpoint `{host}`"));
                }
                return Ok(Some((AzureDatabaseEngine::Mssql, first.to_string())));
            }
        }
        Ok(None)
    }

    /// Return the canonical Entra OAuth2 scope for this Azure database endpoint.
    pub const fn scope(&self) -> &'static str {
        self.engine.scope()
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

        let details_ok = MetadataExchangeResponse::decode(&resp_ok).unwrap();
        assert_eq!(details_ok.response_code, MetadataExchangeResponseCode::Ok);
        assert_eq!(details_ok.response_code.as_str(), "OK");
        assert!(details_ok.is_ok());

        let details_err = MetadataExchangeResponse::decode(&resp_err).unwrap();
        assert_eq!(
            details_err.response_code,
            MetadataExchangeResponseCode::Error
        );
        assert_eq!(details_err.response_code.as_str(), "ERROR");
        assert_eq!(details_err.error_message, "IAM token expired");
        assert!(!details_err.is_ok());

        let mut omitted_code_proto = BytesMut::new();
        put_bytes_field(&mut omitted_code_proto, 2, b"missing IAM credential");
        let mut omitted_frame = BytesMut::with_capacity(4 + omitted_code_proto.len());
        omitted_frame.put_u32(omitted_code_proto.len() as u32);
        omitted_frame.extend_from_slice(&omitted_code_proto);
        let omitted_details = MetadataExchangeResponse::decode(&omitted_frame).unwrap();
        assert_eq!(
            omitted_details.response_code,
            MetadataExchangeResponseCode::Error
        );
        assert_eq!(omitted_details.raw_code, 2);
        assert_eq!(omitted_details.error_message, "missing IAM credential");
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

    #[test]
    fn test_aws_rds_root_ca_pem_loads_cleanly_into_rustls_root_store() {
        let pem = aws_rds_root_ca_pem();
        assert!(pem.contains("-----BEGIN CERTIFICATE-----"));
        let mut reader = std::io::BufReader::new(pem.as_bytes());
        let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(certs.len() >= 3);
        assert_eq!(certs.len(), 114);

        let mut store = rustls::RootCertStore::empty();
        let (added, ignored) = store.add_parsable_certificates(certs);
        assert_eq!(added, 114);
        assert_eq!(ignored, 0);
    }

    #[test]
    fn test_aws_rds_dialer_parsing_and_sigv4_signing() {
        let standard = AwsRdsDialer::new(
            "mydb.c9akciq32.us-east-1.rds.amazonaws.com",
            None,
            None,
            "iam_user",
        )
        .unwrap();
        assert_eq!(
            standard.endpoint_host,
            "mydb.c9akciq32.us-east-1.rds.amazonaws.com"
        );
        assert_eq!(standard.server_port, 5432);
        assert_eq!(standard.region, "us-east-1");
        assert_eq!(standard.username, "iam_user");
        assert_eq!(standard.identifier.as_deref(), Some("mydb"));
        assert!(!standard.is_cluster_endpoint);
        assert!(!standard.is_proxy_endpoint);

        let aurora_cn = AwsRdsDialer::new(
            "orders.cluster-ro-abc123.cn-north-1.rds.amazonaws.com.cn",
            Some(3306),
            None,
            "app_reader",
        )
        .unwrap();
        assert_eq!(aurora_cn.server_port, 3306);
        assert_eq!(aurora_cn.region, "cn-north-1");
        assert!(aurora_cn.is_cluster_endpoint);
        assert!(!aurora_cn.is_proxy_endpoint);

        let proxy = AwsRdsDialer::new(
            "ro-ep.endpoint.proxy-xyz789.eu-west-1.rds.amazonaws.com",
            None,
            None,
            "proxy_user",
        )
        .unwrap();
        assert_eq!(proxy.region, "eu-west-1");
        assert!(proxy.is_proxy_endpoint);
        assert!(!proxy.is_cluster_endpoint);

        let custom =
            AwsRdsDialer::new("127.0.0.1", Some(15432), Some("us-west-2"), "local_iam").unwrap();
        assert_eq!(custom.endpoint_host, "127.0.0.1");
        assert_eq!(custom.server_port, 15432);
        assert_eq!(custom.region, "us-west-2");

        let token = standard
            .sign_iam_token(
                "test-access-key-placeholder",
                "test-secret-key-placeholder",
                Some("test-token-placeholder/abc+123=="),
                900,
                Some(1_700_000_000.0),
            )
            .unwrap();
        assert!(token.starts_with("mydb.c9akciq32.us-east-1.rds.amazonaws.com:5432/?"));
        assert!(token.contains("Action=connect"));
        assert!(token.contains("DBUser=iam_user"));
        assert!(token.contains("X-Amz-Security-Token=test-token-placeholder%2Fabc%2B123%3D%3D"));
        assert!(token.contains("&X-Amz-Signature="));

        for bad in [
            "https://mydb.c9akciq32.us-east-1.rds.amazonaws.com",
            "postgres://mydb.c9akciq32.us-east-1.rds.amazonaws.com",
            " mydb.c9akciq32.us-east-1.rds.amazonaws.com",
            "mydb.c9akciq32.us-east-1.rds.amazonaws.com ",
            "my db.c9akciq32.us-east-1.rds.amazonaws.com",
            "mydb.us-east-1.rds.amazonaws.com",
            "rds.amazonaws.com",
            "mydb.c9akciq32.us-east-1.rds.amazonaws.com:0",
            "custom.internal.corp",
        ] {
            assert!(
                AwsRdsDialer::new(bad, None, None, "iam_user").is_err(),
                "expected error for `{bad}`"
            );
        }
        assert!(
            AwsRdsDialer::new(
                "mydb.c9akciq32.us-east-1.rds.amazonaws.com",
                Some(0),
                None,
                "iam_user"
            )
            .is_err()
        );
        assert!(
            AwsRdsDialer::new(
                "mydb.c9akciq32.us-east-1.rds.amazonaws.com",
                None,
                None,
                "   "
            )
            .is_err()
        );
        assert!(
            standard
                .sign_iam_token(
                    "test-access-key-placeholder",
                    "test-secret-key-placeholder",
                    None,
                    901,
                    Some(1_700_000_000.0),
                )
                .is_err()
        );
    }

    #[test]
    fn test_aws_aurora_dsql_dialer_parsing_and_sigv4_signing() {
        let user_dialer =
            AwsAuroraDsqlDialer::new("abc123def456.dsql.us-east-1.on.aws", None, None, false)
                .unwrap();
        assert_eq!(user_dialer.server_port, 5432);
        assert_eq!(user_dialer.region, "us-east-1");
        assert_eq!(user_dialer.cluster_id.as_deref(), Some("abc123def456"));
        assert_eq!(user_dialer.action(), "DbConnect");

        let admin_dialer =
            AwsAuroraDsqlDialer::new("abc123def456.dsql-fnh4.us-west-2.on.aws", None, None, true)
                .unwrap();
        assert_eq!(admin_dialer.region, "us-west-2");
        assert_eq!(admin_dialer.action(), "DbConnectAdmin");

        let user_tok = user_dialer
            .sign_iam_token(
                "test-access-key-placeholder",
                "test-secret-key-placeholder",
                None,
                3600,
                Some(1_700_000_000.0),
            )
            .unwrap();
        assert!(user_tok.starts_with("abc123def456.dsql.us-east-1.on.aws/?"));
        assert!(user_tok.contains("Action=DbConnect&"));
        assert!(!user_tok.contains("DBUser="));

        let admin_tok = admin_dialer
            .sign_iam_token(
                "test-access-key-placeholder",
                "test-secret-key-placeholder",
                None,
                604_800,
                Some(1_700_000_000.0),
            )
            .unwrap();
        assert!(admin_tok.contains("Action=DbConnectAdmin&"));
        assert!(!admin_tok.contains("DBUser="));

        for bad in [
            "https://abc123def456.dsql.us-east-1.on.aws",
            "abc123def456.s3.us-east-1.on.aws",
            "dsql.us-east-1.on.aws",
            "abc123def456.dsql.us-east-1.on.aws:0",
            " abc123def456.dsql.us-east-1.on.aws",
        ] {
            assert!(
                AwsAuroraDsqlDialer::new(bad, None, None, false).is_err(),
                "expected error for `{bad}`"
            );
        }
        assert!(
            admin_dialer
                .sign_iam_token(
                    "test-access-key-placeholder",
                    "test-secret-key-placeholder",
                    None,
                    604_801,
                    Some(1_700_000_000.0),
                )
                .is_err()
        );
    }

    #[test]
    fn test_azure_entra_dialer_parsing_and_scopes() {
        let pg = AzureEntraDialer::new("my-pg.postgres.database.azure.com", None, None).unwrap();
        assert_eq!(pg.server_port, 5432);
        assert_eq!(pg.engine, AzureDatabaseEngine::Postgres);
        assert_eq!(pg.scope(), AZURE_OSSRDBMS_SCOPE);
        assert_eq!(pg.server_name.as_deref(), Some("my-pg"));

        let mysql = AzureEntraDialer::new("my-mysql.mysql.database.azure.com", None, None).unwrap();
        assert_eq!(mysql.server_port, 3306);
        assert_eq!(mysql.engine, AzureDatabaseEngine::Mysql);
        assert_eq!(mysql.scope(), AZURE_OSSRDBMS_SCOPE);
        assert_eq!(mysql.server_name.as_deref(), Some("my-mysql"));

        let mssql = AzureEntraDialer::new("my-sql.database.windows.net", None, None).unwrap();
        assert_eq!(mssql.server_port, 1433);
        assert_eq!(mssql.engine, AzureDatabaseEngine::Mssql);
        assert_eq!(mssql.scope(), AZURE_SQL_DATABASE_SCOPE);
        assert_eq!(mssql.server_name.as_deref(), Some("my-sql"));

        let explicit_mssql = AzureEntraDialer::new("127.0.0.1", None, Some("mssql")).unwrap();
        assert_eq!(explicit_mssql.server_port, 1433);
        assert_eq!(explicit_mssql.scope(), AZURE_SQL_DATABASE_SCOPE);

        for bad in [
            "https://my-pg.postgres.database.azure.com",
            " my-pg.postgres.database.azure.com",
            "postgres.database.azure.com",
            "database.windows.net",
            "ossrdbms-aad.database.windows.net",
            "custom.internal.corp",
            "my-pg.postgres.database.azure.com:0",
        ] {
            assert!(
                AzureEntraDialer::new(bad, None, None).is_err(),
                "expected error for `{bad}`"
            );
        }
        assert!(
            AzureEntraDialer::new("my-pg.postgres.database.azure.com", None, Some("mssql"))
                .is_err()
        );
        assert!(AzureEntraDialer::new("127.0.0.1", Some(0), Some("postgres")).is_err());
        assert!(AzureEntraDialer::new("127.0.0.1", None, Some("oracle")).is_err());
    }
}
