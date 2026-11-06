//! Pure-Rust Oracle `tnsnames.ora`, `sqlnet.ora`, and Autonomous Database Wallet parser.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Parsed Oracle connect descriptor (`(DESCRIPTION=...)`) extracted from `tnsnames.ora` or built programmatically.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleConnectDescriptor {
    pub host: String,
    pub port: u16,
    pub service_name: Option<String>,
    pub sid: Option<String>,
    pub security_cert_dn: Option<String>,
    pub retry_count: Option<u32>,
    pub retry_delay: Option<u32>,
    pub transport_connect_timeout: Option<u32>,
}

impl Default for OracleConnectDescriptor {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 1521,
            service_name: Some("FREEPDB1".to_string()),
            sid: None,
            security_cert_dn: None,
            retry_count: Some(3),
            retry_delay: Some(1),
            transport_connect_timeout: Some(3),
        }
    }
}

impl OracleConnectDescriptor {
    /// Parse an `(DESCRIPTION=...)` S-expression string into an `OracleConnectDescriptor`.
    pub fn from_descriptor_str(raw: &str) -> Option<Self> {
        let host = extract_tns_param(raw, "HOST")?;
        let port = extract_tns_param(raw, "PORT")
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(1521);
        let service_name = extract_tns_param(raw, "SERVICE_NAME");
        let sid = extract_tns_param(raw, "SID");
        let security_cert_dn = extract_tns_param(raw, "SSL_SERVER_CERT_DN");
        let retry_count = extract_tns_param(raw, "RETRY_COUNT").and_then(|v| v.parse::<u32>().ok());
        let retry_delay = extract_tns_param(raw, "RETRY_DELAY").and_then(|v| v.parse::<u32>().ok());
        let transport_connect_timeout =
            extract_tns_param(raw, "TRANSPORT_CONNECT_TIMEOUT").and_then(|v| v.parse::<u32>().ok());

        Some(Self {
            host,
            port,
            service_name,
            sid,
            security_cert_dn,
            retry_count,
            retry_delay,
            transport_connect_timeout,
        })
    }

    /// Render the canonical Oracle `(DESCRIPTION=...)` connect descriptor string.
    pub fn to_tns_descriptor(&self) -> String {
        let protocol = if self.security_cert_dn.is_some() || self.port == 1522 || self.port == 2484
        {
            "TCPS"
        } else {
            "TCP"
        };
        let timeout_part = self
            .transport_connect_timeout
            .map(|t| format!("(TRANSPORT_CONNECT_TIMEOUT={t})"))
            .unwrap_or_default();
        let retry_count_part = self
            .retry_count
            .map(|c| format!("(RETRY_COUNT={c})"))
            .unwrap_or_default();
        let retry_delay_part = self
            .retry_delay
            .map(|d| format!("(RETRY_DELAY={d})"))
            .unwrap_or_default();

        let connect_data = if let Some(ref svc) = self.service_name {
            format!("(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME={svc}))")
        } else if let Some(ref sid) = self.sid {
            format!("(CONNECT_DATA=(SERVER=DEDICATED)(SID={sid}))")
        } else {
            "(CONNECT_DATA=(SERVER=DEDICATED))".to_string()
        };

        let security_part = if let Some(ref dn) = self.security_cert_dn {
            format!("(SECURITY=(SSL_SERVER_CERT_DN=\"{dn}\")(SSL_SERVER_DN_MATCH=YES))")
        } else {
            String::new()
        };

        format!(
            "(DESCRIPTION={timeout_part}{retry_count_part}{retry_delay_part}(ADDRESS=(PROTOCOL={protocol})(HOST={})(PORT={})){connect_data}{security_part})",
            self.host, self.port
        )
    }
}

fn extract_tns_param(descriptor: &str, key: &str) -> Option<String> {
    let bytes = descriptor.as_bytes();
    let key_bytes = key.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'(' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j + key_bytes.len() <= bytes.len()
                && bytes[j..j + key_bytes.len()].eq_ignore_ascii_case(key_bytes)
            {
                let mut k = j + key_bytes.len();
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                if k < bytes.len() && bytes[k] == b'=' {
                    k += 1;
                    while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                        k += 1;
                    }
                    let val_start = k;
                    let mut in_quotes = false;
                    while k < bytes.len() {
                        if bytes[k] == b'"' {
                            in_quotes = !in_quotes;
                        } else if !in_quotes && (bytes[k] == b')' || bytes[k] == b'(') {
                            break;
                        }
                        k += 1;
                    }
                    let raw_val = descriptor[val_start..k].trim().trim_matches('"').trim();
                    if !raw_val.is_empty() {
                        return Some(raw_val.to_string());
                    }
                }
            }
        }
        i += 1;
    }
    None
}

/// Parse an Oracle `tnsnames.ora` file into a map of uppercase alias -> normalized descriptor string.
pub fn parse_tnsnames_ora(contents: &str) -> HashMap<String, String> {
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
    let mut i = 0usize;
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
            continue;
        }
        let desc_start = i;
        let mut depth = 0usize;
        let mut in_quotes = false;
        while i < bytes.len() {
            if bytes[i] == b'"' {
                in_quotes = !in_quotes;
            } else if !in_quotes {
                if bytes[i] == b'(' {
                    depth += 1;
                } else if bytes[i] == b')' {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        i += 1;
                        break;
                    }
                }
            }
            i += 1;
        }
        if depth != 0 {
            break;
        }

        let mut descriptor = String::with_capacity(i - desc_start);
        let mut q = false;
        for ch in cleaned[desc_start..i].chars() {
            if ch == '"' {
                q = !q;
                descriptor.push(ch);
            } else if q || !ch.is_ascii_whitespace() {
                descriptor.push(ch);
            }
        }

        for alias in raw_aliases.split(',') {
            let trimmed = alias.trim();
            if !trimmed.is_empty() {
                entries.insert(trimmed.to_ascii_uppercase(), descriptor.clone());
            }
        }
    }
    entries
}

/// Resolve a TNS alias from `tnsnames.ora` content into a structured `OracleConnectDescriptor`.
pub fn resolve_tns_alias(contents: &str, alias: &str) -> Option<OracleConnectDescriptor> {
    let map = parse_tnsnames_ora(contents);
    let key = alias.trim().to_ascii_uppercase();
    let raw_desc = map.get(&key)?;
    OracleConnectDescriptor::from_descriptor_str(raw_desc)
}

/// Parsed Oracle Autonomous Database Wallet material (`tnsnames.ora`, `sqlnet.ora`, `ewallet.pem`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleWalletMaterial {
    pub tnsnames_ora: String,
    pub sqlnet_ora: String,
    pub ewallet_pem: Option<String>,
    pub aliases: HashMap<String, OracleConnectDescriptor>,
}

impl OracleWalletMaterial {
    /// Load and parse an extracted Autonomous Database Wallet directory (`tnsnames.ora`, `sqlnet.ora`, `ewallet.pem`).
    pub fn from_directory(path: &Path) -> Result<Self, String> {
        let tns_path = path.join("tnsnames.ora");
        if !tns_path.exists() {
            return Err(format!(
                "missing tnsnames.ora in wallet directory `{}`",
                path.display()
            ));
        }
        let tnsnames_ora = std::fs::read_to_string(&tns_path)
            .map_err(|e| format!("failed to read `{}`: {e}", tns_path.display()))?;

        let sqlnet_path = path.join("sqlnet.ora");
        let sqlnet_ora = if sqlnet_path.exists() {
            std::fs::read_to_string(&sqlnet_path)
                .map_err(|e| format!("failed to read `{}`: {e}", sqlnet_path.display()))?
        } else {
            String::new()
        };

        let pem_path = path.join("ewallet.pem");
        let ewallet_pem = if pem_path.exists() {
            Some(
                std::fs::read_to_string(&pem_path)
                    .map_err(|e| format!("failed to read `{}`: {e}", pem_path.display()))?,
            )
        } else {
            None
        };

        let raw_map = parse_tnsnames_ora(&tnsnames_ora);
        let mut aliases = HashMap::with_capacity(raw_map.len());
        for (alias, desc_str) in raw_map {
            if let Some(desc) = OracleConnectDescriptor::from_descriptor_str(&desc_str) {
                aliases.insert(alias, desc);
            }
        }

        Ok(Self {
            tnsnames_ora,
            sqlnet_ora,
            ewallet_pem,
            aliases,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tnsnames_and_resolve_alias() {
        let sample = r#"
# Oracle Autonomous Database Wallet tnsnames.ora
myatp_high, myatp_tp =
  (DESCRIPTION =
    (RETRY_COUNT=20)(RETRY_DELAY=3)(TRANSPORT_CONNECT_TIMEOUT=3)
    (ADDRESS = (PROTOCOL = TCPS)(PORT = 1522)(HOST = adb.us-ashburn-1.oraclecloud.com))
    (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = g12345_myatp_tp.adb.oraclecloud.com))
    (SECURITY = (SSL_SERVER_CERT_DN = "CN=adb.us-ashburn-1.oraclecloud.com, OU=Oracle ADB, O=Oracle Corporation")(SSL_SERVER_DN_MATCH = YES))
  )
"#;
        let map = parse_tnsnames_ora(sample);
        assert!(map.contains_key("MYATP_HIGH"));
        assert!(map.contains_key("MYATP_TP"));

        let desc = resolve_tns_alias(sample, "myatp_tp").unwrap();
        assert_eq!(desc.host, "adb.us-ashburn-1.oraclecloud.com");
        assert_eq!(desc.port, 1522);
        assert_eq!(
            desc.service_name.as_deref(),
            Some("g12345_myatp_tp.adb.oraclecloud.com")
        );
        assert_eq!(
            desc.security_cert_dn.as_deref(),
            Some("CN=adb.us-ashburn-1.oraclecloud.com, OU=Oracle ADB, O=Oracle Corporation")
        );
        assert_eq!(desc.retry_count, Some(20));
        assert_eq!(desc.retry_delay, Some(3));
        assert_eq!(desc.transport_connect_timeout, Some(3));

        let rendered = desc.to_tns_descriptor();
        assert!(rendered.contains("(PROTOCOL=TCPS)"));
        assert!(rendered.contains("(PORT=1522)"));
    }

    #[test]
    fn test_oracle_wallet_from_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("tnsnames.ora"),
            "db_tp = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=adb.example.com)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=svc_tp)))",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("sqlnet.ora"),
            "WALLET_LOCATION = (SOURCE = (METHOD = file))",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("ewallet.pem"),
            "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n",
        )
        .unwrap();

        let wallet = OracleWalletMaterial::from_directory(dir.path()).unwrap();
        assert!(wallet.ewallet_pem.is_some());
        let desc = wallet.aliases.get("DB_TP").unwrap();
        assert_eq!(desc.host, "adb.example.com");
        assert_eq!(desc.port, 1522);
        assert_eq!(desc.service_name.as_deref(), Some("svc_tp"));
    }
}
