//! `vertebra-db` — Native Rust database dialer, TNS parser, RESP3 codec, and pool primitives.
//!
//! Provides:
//! - Zero-sidecar Google Cloud SQL (`:3307`) and AlloyDB (`:5433`) URI parsing, metadata exchange
//!   framing, and IAM token caching (`dialer`).
//! - Pure-Rust Oracle `tnsnames.ora` / `sqlnet.ora` S-expression parser and Exadata/RAC/SCAN/DRCP/TAC
//!   connect descriptor builder (`tns`).
//! - PgBouncer / PgCat / pgdog pooler mode rules and cross-replica connection budgeting (`pool`).
//! - Redis and Valkey RESP3 command pipelining and `HELLO 3` handshake encoding (`resp`).
//! - Zero-copy PostgreSQL binary `JSONB` (`\x01` header) slicing (`jsonb`).

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors returned by `vertebra-db` primitives.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DbError {
    #[error("invalid cloud instance URI `{0}`: {1}")]
    InvalidInstanceUri(String, String),
    #[error("TNS parse error: {0}")]
    TnsParse(String),
    #[error("TNS alias not found: `{0}`")]
    TnsAliasNotFound(String),
    #[error("invalid binary JSONB payload: {0}")]
    InvalidJsonb(String),
    #[error("RESP3 protocol error: {0}")]
    RespProtocol(String),
}

/// Parsed Google Cloud SQL instance connection name (`project:region:instance`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudSqlInstanceUri {
    pub project: String,
    pub region: String,
    pub instance: String,
}

impl CloudSqlInstanceUri {
    /// Parse `project:region:instance` (supporting domain-scoped projects like `google.com:proj:region:inst`).
    pub fn parse(raw: &str) -> Result<Self, DbError> {
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
            _ => Err(DbError::InvalidInstanceUri(
                trimmed.to_string(),
                "expected 'project:region:instance'".to_string(),
            )),
        }
    }

    /// Expected server certificate Subject CN (`project:instance`) on Cloud SQL port `:3307`.
    pub fn expected_server_cn(&self) -> String {
        format!("{}:{}", self.project, self.instance)
    }

    /// Cloud SQL Admin API v1beta4 `generateEphemeralCert` endpoint URL.
    pub fn generate_ephemeral_cert_url(&self, base_endpoint: &str) -> String {
        let base = base_endpoint.trim_end_matches('/');
        format!(
            "{base}/sql/v1beta4/projects/{}/instances/{}:generateEphemeralCert",
            self.project, self.instance
        )
    }

    /// Default Cloud SQL Auth Proxy TLS port (`3307`).
    pub const fn default_tls_port() -> u16 {
        3307
    }
}

/// Parsed Google Cloud AlloyDB instance URI (`projects/<p>/locations/<l>/clusters/<c>/instances/<i>`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlloyDbClusterUri {
    pub project: String,
    pub location: String,
    pub cluster: String,
    pub instance: String,
}

impl AlloyDbClusterUri {
    /// Parse `projects/<p>/locations/<l>/clusters/<c>/instances/<i>`.
    pub fn parse(raw: &str) -> Result<Self, DbError> {
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
            Err(DbError::InvalidInstanceUri(
                raw.to_string(),
                "expected 'projects/<p>/locations/<l>/clusters/<c>/instances/<i>'".to_string(),
            ))
        }
    }

    /// AlloyDB API v1beta `generateClientCertificate` endpoint URL.
    pub fn generate_client_cert_url(&self, base_endpoint: &str) -> String {
        let base = base_endpoint.trim_end_matches('/');
        format!(
            "{base}/v1beta/projects/{}/locations/{}/clusters/{}:generateClientCertificate",
            self.project, self.location, self.cluster
        )
    }

    /// Default AlloyDB server proxy mTLS port (`5433`).
    pub const fn default_mtls_port() -> u16 {
        5433
    }
}

/// AlloyDB MetadataExchange authentication mode on port `:5433`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlloyDbAuthType {
    BuiltIn = 1,
    Iam = 2,
}

/// Encode a 4-byte big-endian length-prefixed AlloyDB `MetadataExchangeRequest` wire frame.
pub fn encode_alloydb_metadata_exchange(
    user_agent: &str,
    auth_type: AlloyDbAuthType,
    oauth2_token: &str,
) -> Bytes {
    let mut proto = BytesMut::with_capacity(user_agent.len() + oauth2_token.len() + 16);
    if !user_agent.is_empty() {
        proto.put_u8(0x0a);
        put_varint(&mut proto, user_agent.len() as u64);
        proto.extend_from_slice(user_agent.as_bytes());
    }
    proto.put_u8(0x10);
    put_varint(&mut proto, auth_type as u64);
    if !oauth2_token.is_empty() {
        proto.put_u8(0x1a);
        put_varint(&mut proto, oauth2_token.len() as u64);
        proto.extend_from_slice(oauth2_token.as_bytes());
    }

    let mut framed = BytesMut::with_capacity(4 + proto.len());
    framed.put_u32(proto.len() as u32);
    framed.extend_from_slice(&proto);
    framed.freeze()
}

fn put_varint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        buf.put_u8(((value as u8) & 0x7f) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

/// External connection pooler mode (`direct`, `transaction`, or `session`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PoolerMode {
    Direct,
    Transaction,
    Session,
}

impl PoolerMode {
    /// Parse a pooler mode name (`direct`, `transaction`, `session`).
    pub fn parse(mode: &str) -> Self {
        match mode.trim().to_ascii_lowercase().as_str() {
            "transaction" | "pgbouncer_transaction" | "pgcat" | "pgdog" => Self::Transaction,
            "session" | "pgbouncer_session" => Self::Session,
            _ => Self::Direct,
        }
    }

    /// Effective prepared statement cache size given the pooler mode.
    pub fn effective_statement_cache_capacity(
        self,
        configured: Option<usize>,
        protocol_level_pooler_cache: bool,
    ) -> usize {
        match self {
            Self::Transaction if !protocol_level_pooler_cache => 0,
            Self::Transaction | Self::Session | Self::Direct => configured.unwrap_or(256),
        }
    }
}

/// Compute per-worker database pool size from a total cluster connection budget.
pub fn compute_worker_pool_budget(
    total_budget: usize,
    replica_count: usize,
    workers_per_replica: usize,
    reserved_admin_connections: usize,
) -> usize {
    let usable = total_budget.saturating_sub(reserved_admin_connections);
    let total_shards = replica_count
        .max(1)
        .saturating_mul(workers_per_replica.max(1));
    (usable / total_shards).max(1)
}

/// Strip the 1-byte PostgreSQL binary `JSONB` version header (`\x01`) with zero heap copy.
pub fn strip_pg_jsonb_header(raw: Bytes) -> Result<Bytes, DbError> {
    if raw.is_empty() {
        return Err(DbError::InvalidJsonb("empty JSONB payload".to_string()));
    }
    if raw[0] != 0x01 {
        return Err(DbError::InvalidJsonb(format!(
            "unsupported PostgreSQL JSONB version byte: 0x{:02x}",
            raw[0]
        )));
    }
    Ok(raw.slice(1..))
}

/// Parsed S-expression node from an Oracle `tnsnames.ora` or `sqlnet.ora` file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TnsNode {
    Value(String),
    List(Vec<(String, TnsNode)>),
}

/// Parse an Oracle `tnsnames.ora` file into a map of uppercase alias -> descriptor string.
pub fn parse_tnsnames_ora(contents: &str) -> Result<HashMap<String, String>, DbError> {
    let mut cleaned = String::with_capacity(contents.len());
    for line in contents.lines() {
        let without_comment = match line.find('#') {
            Some(idx) => &line[..idx],
            None => line,
        };
        cleaned.push_str(without_comment);
        cleaned.push('\n');
    }

    let mut entries = HashMap::new();
    let bytes = cleaned.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let alias_start = i;
        while i < bytes.len() && bytes[i] != b'=' {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let raw_aliases = cleaned[alias_start..i].trim();
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'(' {
            return Err(DbError::TnsParse(format!(
                "expected '(' after '=' for alias `{raw_aliases}`"
            )));
        }
        let desc_start = i;
        let mut depth = 0usize;
        while i < bytes.len() {
            if bytes[i] == b'(' {
                depth += 1;
            } else if bytes[i] == b')' {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    i += 1;
                    break;
                }
            }
            i += 1;
        }
        if depth != 0 {
            return Err(DbError::TnsParse(format!(
                "unbalanced parentheses in descriptor for `{raw_aliases}`"
            )));
        }
        let descriptor: String = cleaned[desc_start..i]
            .chars()
            .filter(|c| !c.is_ascii_whitespace())
            .collect();
        for alias in raw_aliases.split(',') {
            let trimmed = alias.trim();
            if !trimmed.is_empty() {
                entries.insert(trimmed.to_ascii_uppercase(), descriptor.clone());
            }
        }
    }
    Ok(entries)
}

/// Resolve a specific alias from `tnsnames.ora` content.
pub fn resolve_tns_alias(contents: &str, alias: &str) -> Result<String, DbError> {
    let map = parse_tnsnames_ora(contents)?;
    let key = alias.trim().to_ascii_uppercase();
    map.get(&key)
        .cloned()
        .ok_or_else(|| DbError::TnsAliasNotFound(alias.to_string()))
}

/// Structured Oracle high-availability connect descriptor builder for Exadata, RAC, SCAN, DRCP, and TAC.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleConnectDescriptor {
    pub hosts: Vec<(String, u16)>,
    pub protocol: String,
    pub service_name: String,
    pub server_mode: String,
    pub connect_timeout: u32,
    pub transport_connect_timeout: u32,
    pub retry_count: u32,
    pub retry_delay: u32,
    pub load_balance: bool,
    pub failover: bool,
    pub sdu: Option<u32>,
    pub ssl_server_dn_match: Option<bool>,
}

impl Default for OracleConnectDescriptor {
    fn default() -> Self {
        Self {
            hosts: vec![("localhost".to_string(), 1521)],
            protocol: "TCP".to_string(),
            service_name: "FREEPDB1".to_string(),
            server_mode: "DEDICATED".to_string(),
            connect_timeout: 5,
            transport_connect_timeout: 3,
            retry_count: 3,
            retry_delay: 1,
            load_balance: true,
            failover: true,
            sdu: None,
            ssl_server_dn_match: None,
        }
    }
}

impl OracleConnectDescriptor {
    /// Render the canonical Oracle `(DESCRIPTION=...)` connect descriptor string.
    pub fn to_tns_descriptor(&self) -> String {
        let lb = if self.load_balance { "ON" } else { "OFF" };
        let fo = if self.failover { "ON" } else { "OFF" };
        let sdu_part = self.sdu.map(|v| format!("(SDU={v})")).unwrap_or_default();
        let addresses: String = self
            .hosts
            .iter()
            .map(|(h, p)| format!("(ADDRESS=(PROTOCOL={})(HOST={h})(PORT={p}))", self.protocol))
            .collect();
        let security_part = match self.ssl_server_dn_match {
            Some(true) => "(SECURITY=(SSL_SERVER_CERT_DN=\"*\")(SSL_SERVER_DN_MATCH=YES))",
            Some(false) => "(SECURITY=(SSL_SERVER_DN_MATCH=NO))",
            None => "",
        };
        format!(
            "(DESCRIPTION=(CONNECT_TIMEOUT={})(TRANSPORT_CONNECT_TIMEOUT={})(RETRY_COUNT={})(RETRY_DELAY={}){sdu_part}(ADDRESS_LIST=(LOAD_BALANCE={lb})(FAILOVER={fo}){addresses})(CONNECT_DATA=(SERVER={})(SERVICE_NAME={})){security_part})",
            self.connect_timeout,
            self.transport_connect_timeout,
            self.retry_count,
            self.retry_delay,
            self.server_mode.to_ascii_uppercase(),
            self.service_name,
        )
    }
}

/// Encode a Redis or Valkey RESP3 bulk-string array command.
pub fn encode_resp_command(args: &[&[u8]]) -> Bytes {
    let mut buf = BytesMut::with_capacity(64);
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    buf.freeze()
}

/// Encode a Redis / Valkey `HELLO 3` handshake frame (with optional `AUTH` and `SETNAME`).
pub fn encode_hello3_handshake(
    username: Option<&str>,
    password: Option<&str>,
    client_name: Option<&str>,
) -> Bytes {
    let mut parts: Vec<&[u8]> = vec![b"HELLO", b"3"];
    if let Some(pw) = password {
        parts.push(b"AUTH");
        parts.push(username.unwrap_or("default").as_bytes());
        parts.push(pw.as_bytes());
    }
    if let Some(name) = client_name {
        parts.push(b"SETNAME");
        parts.push(name.as_bytes());
    }
    encode_resp_command(&parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloudsql_and_alloydb_uri_parsing() {
        let csql = CloudSqlInstanceUri::parse("my-project:us-central1:prod-pg").unwrap();
        assert_eq!(csql.project, "my-project");
        assert_eq!(csql.region, "us-central1");
        assert_eq!(csql.instance, "prod-pg");
        assert_eq!(csql.expected_server_cn(), "my-project:prod-pg");
        assert_eq!(CloudSqlInstanceUri::default_tls_port(), 3307);

        let alloy = AlloyDbClusterUri::parse(
            "projects/my-proj/locations/us-central1/clusters/main-cluster/instances/primary-inst",
        )
        .unwrap();
        assert_eq!(alloy.project, "my-proj");
        assert_eq!(alloy.location, "us-central1");
        assert_eq!(alloy.cluster, "main-cluster");
        assert_eq!(alloy.instance, "primary-inst");
        assert_eq!(AlloyDbClusterUri::default_mtls_port(), 5433);
    }

    #[test]
    fn test_alloydb_metadata_exchange_frame_encoding() {
        let frame = encode_alloydb_metadata_exchange(
            "vertebra-rs/0.1",
            AlloyDbAuthType::Iam,
            "mock-oauth-token",
        );
        let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        assert_eq!(len, frame.len() - 4);
        assert_eq!(frame[4], 0x0a);
    }

    #[test]
    fn test_parse_tnsnames_ora_and_descriptor_builder() {
        let sample = r#"
# Oracle Autonomous Database Wallet tnsnames.ora
myatp_tp, myatp_high =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCPS)(PORT = 1522)(HOST = adb.us-ashburn-1.oraclecloud.com))
    (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = g12345_myatp_tp.adb.oraclecloud.com))
    (SECURITY = (SSL_SERVER_DN_MATCH = YES))
  )
"#;
        let resolved = resolve_tns_alias(sample, "myatp_tp").unwrap();
        assert!(resolved.contains("(HOST=adb.us-ashburn-1.oraclecloud.com)"));
        assert!(resolved.contains("(SERVICE_NAME=g12345_myatp_tp.adb.oraclecloud.com)"));

        let desc = OracleConnectDescriptor {
            hosts: vec![
                ("exadata-scan1.internal".to_string(), 1521),
                ("exadata-scan2.internal".to_string(), 1521),
            ],
            protocol: "TCPS".to_string(),
            service_name: "OLTP_SVC".to_string(),
            server_mode: "POOLED".to_string(),
            sdu: Some(65535),
            ssl_server_dn_match: Some(true),
            ..Default::default()
        };
        let rendered = desc.to_tns_descriptor();
        assert!(rendered.contains("(SDU=65535)"));
        assert!(rendered.contains("(SERVER=POOLED)"));
        assert!(rendered.contains("(LOAD_BALANCE=ON)(FAILOVER=ON)"));
        assert!(rendered.contains("(SSL_SERVER_DN_MATCH=YES)"));
    }

    #[test]
    fn test_pg_jsonb_strip_and_pooler_mode() {
        let mut raw = BytesMut::from(&b"\x01{\"id\":42,\"status\":\"ok\"}"[..]);
        let payload = strip_pg_jsonb_header(raw.split().freeze()).unwrap();
        assert_eq!(&payload[..], b"{\"id\":42,\"status\":\"ok\"}");

        assert_eq!(
            PoolerMode::parse("transaction").effective_statement_cache_capacity(Some(128), false),
            0
        );
        assert_eq!(
            PoolerMode::parse("transaction").effective_statement_cache_capacity(Some(128), true),
            128
        );
        assert_eq!(compute_worker_pool_budget(200, 5, 4, 20), 9);
    }

    #[test]
    fn test_resp3_hello_encoding() {
        let frame = encode_hello3_handshake(Some("default"), Some("secret"), Some("vertebra"));
        assert!(frame.starts_with(b"*7\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"));
    }
}
