//! Valkey Sentinel primary discovery (`SENTINEL get-master-addr-by-name`, `ROLE`, `+switch-master`).

use crate::codec::RespValue;
use crate::error::ValkeyError;

/// Parse a `host[:port]` or `valkey://host:port` Sentinel address specification.
pub fn parse_sentinel_addr(spec: &str, default_port: u16) -> Result<(String, u16), ValkeyError> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Err(ValkeyError::config("Sentinel node address cannot be empty"));
    }
    let clean = trimmed
        .strip_prefix("valkeys://")
        .or_else(|| trimmed.strip_prefix("valkey://"))
        .unwrap_or(trimmed);
    let authority = clean.split('/').next().unwrap_or(clean);
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, right)| right);

    if host_port.starts_with('[')
        && let Some(end_bracket) = host_port.find(']')
    {
        let host = host_port[1..end_bracket].to_string();
        let rest = &host_port[end_bracket + 1..];
        let port = if let Some(port_part) = rest.strip_prefix(':') {
            port_part
                .parse::<u16>()
                .map_err(|_| ValkeyError::config(format!("Invalid Sentinel port in '{spec}'")))?
        } else {
            default_port
        };
        if host.is_empty() || port == 0 {
            return Err(ValkeyError::config(format!(
                "Invalid Sentinel address '{spec}'"
            )));
        }
        return Ok((host, port));
    }

    if let Some((h, p)) = host_port.rsplit_once(':')
        && !h.contains(':')
    {
        if h.trim().is_empty() {
            return Err(ValkeyError::config(format!(
                "Hostless Sentinel address '{spec}'"
            )));
        }
        let port = p
            .parse::<u16>()
            .map_err(|_| ValkeyError::config(format!("Invalid Sentinel port in '{spec}'")))?;
        if port == 0 {
            return Err(ValkeyError::config(format!(
                "Invalid port 0 in Sentinel address '{spec}'"
            )));
        }
        return Ok((h.trim().to_string(), port));
    }

    Ok((host_port.to_string(), default_port))
}

/// Build `SENTINEL get-master-addr-by-name <service_name>`.
#[must_use]
pub fn build_get_master_addr_cmd(service_name: &str) -> Vec<Vec<u8>> {
    vec![
        b"SENTINEL".to_vec(),
        b"get-master-addr-by-name".to_vec(),
        service_name.as_bytes().to_vec(),
    ]
}

/// Parse the 2-element reply from `SENTINEL get-master-addr-by-name`.
pub fn parse_get_master_addr_reply(resp: &RespValue) -> Result<(String, u16), ValkeyError> {
    let val = resp.unattributed();
    if let RespValue::SimpleError(msg) | RespValue::BulkError(msg) = val {
        return Err(ValkeyError::from_server_error(msg));
    }
    if matches!(val, RespValue::Null) {
        return Err(ValkeyError::operational(
            "Sentinel returned null for unknown master service name",
            true,
        ));
    }
    let Some(items) = val.as_slice() else {
        return Err(ValkeyError::data(
            "Unexpected non-array response from SENTINEL get-master-addr-by-name",
        ));
    };
    if items.len() < 2 {
        return Err(ValkeyError::data(
            "SENTINEL get-master-addr-by-name returned fewer than 2 elements",
        ));
    }
    let host = items[0]
        .as_str()
        .map(std::borrow::Cow::into_owned)
        .filter(|h| !h.trim().is_empty())
        .ok_or_else(|| ValkeyError::data("Invalid master host in Sentinel reply"))?;
    let port_i64 = items[1]
        .as_i64()
        .ok_or_else(|| ValkeyError::data("Invalid master port in Sentinel reply"))?;
    let port = u16::try_from(port_i64)
        .ok()
        .filter(|&p| p > 0)
        .ok_or_else(|| ValkeyError::data("Out-of-range master port in Sentinel reply"))?;
    Ok((host, port))
}

/// Verify that a candidate primary responds to `ROLE` with `"master"` or `"primary"`.
pub fn verify_role_is_primary(resp: &RespValue) -> Result<(), ValkeyError> {
    let val = resp.unattributed();
    if let RespValue::SimpleError(msg) | RespValue::BulkError(msg) = val {
        return Err(ValkeyError::from_server_error(msg));
    }
    let Some(items) = val.as_slice() else {
        return Err(ValkeyError::data("Unexpected non-array ROLE reply"));
    };
    let role = items
        .first()
        .and_then(RespValue::as_str)
        .ok_or_else(|| ValkeyError::data("Missing role element in ROLE reply"))?;
    if role.eq_ignore_ascii_case("master") || role.eq_ignore_ascii_case("primary") {
        Ok(())
    } else {
        Err(ValkeyError::operational(
            format!("Sentinel candidate endpoint has non-primary ROLE '{role}'"),
            true,
        ))
    }
}

/// Parse a Sentinel `+switch-master` pub/sub payload: `<name> <old_host> <old_port> <new_host> <new_port>`.
#[must_use]
pub fn parse_switch_master_payload(payload: &str) -> Option<(String, String, u16, String, u16)> {
    let mut parts = payload.split_whitespace();
    let master_name = parts.next()?.to_string();
    let old_host = parts.next()?.to_string();
    let old_port: u16 = parts.next()?.parse().ok()?;
    let new_host = parts.next()?.to_string();
    let new_port: u16 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((master_name, old_host, old_port, new_host, new_port))
}
