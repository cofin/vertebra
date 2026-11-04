//! Pure-Rust Redis 7.2+/8.0+ & Valkey 8.0+/9.0+ RESP3 wire framing, recursive parser, push invalidation, and Cluster CRC16.

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// Encode a Redis or Valkey RESP3 bulk-string array command.
pub fn encode_resp_command(args: &[&[u8]]) -> Bytes {
    let total_payload: usize = args.iter().map(|a| a.len() + 12).sum();
    let mut buf = BytesMut::with_capacity(16 + total_payload);
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    buf.freeze()
}

/// Encode a Redis / Valkey `HELLO 3` handshake frame (with optional `AUTH`, `SETNAME`, and pipelined `CLIENT TRACKING ON ... BCAST`).
pub fn encode_hello3_handshake(
    client_name: &str,
    username: Option<&str>,
    password: Option<&str>,
    enable_tracking_bcast: bool,
    tracking_prefixes: &[&str],
) -> Bytes {
    let mut hello_args: Vec<&[u8]> = vec![b"HELLO", b"3"];
    if let Some(pw) = password {
        hello_args.push(b"AUTH");
        hello_args.push(username.unwrap_or("default").as_bytes());
        hello_args.push(pw.as_bytes());
    }
    if !client_name.is_empty() {
        hello_args.push(b"SETNAME");
        hello_args.push(client_name.as_bytes());
    }
    let hello_frame = encode_resp_command(&hello_args);
    if !enable_tracking_bcast {
        return hello_frame;
    }

    let mut tracking_args: Vec<&[u8]> = vec![b"CLIENT", b"TRACKING", b"ON"];
    for prefix in tracking_prefixes {
        tracking_args.push(b"PREFIX");
        tracking_args.push(prefix.as_bytes());
    }
    tracking_args.push(b"BCAST");
    let tracking_frame = encode_resp_command(&tracking_args);

    let mut combined = BytesMut::with_capacity(hello_frame.len() + tracking_frame.len());
    combined.extend_from_slice(&hello_frame);
    combined.extend_from_slice(&tracking_frame);
    combined.freeze()
}

/// Encode a `SENTINEL get-master-addr-by-name <master_name>` RESP command.
pub fn encode_sentinel_get_master_cmd(master_name: &str) -> Bytes {
    encode_resp_command(&[
        b"SENTINEL",
        b"get-master-addr-by-name",
        master_name.as_bytes(),
    ])
}

/// Parsed RESP3 wire protocol frame.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Resp3Frame {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(String),
    VerbatimString { format: [u8; 3], data: Vec<u8> },
    Array(Vec<Resp3Frame>),
    Set(Vec<Resp3Frame>),
    Map(Vec<(Resp3Frame, Resp3Frame)>),
    Push(Vec<Resp3Frame>),
}

fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    if start + 1 >= buf.len() {
        return None;
    }
    let mut i = start;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Parse a single RESP3 frame from `buf`.
///
/// Returns:
/// - `Ok(None)` if `buf` contains an incomplete frame and more bytes are needed.
/// - `Ok(Some((frame, consumed_bytes)))` when a complete frame is decoded.
/// - `Err(reason)` if the frame syntax is malformed.
pub fn parse_resp3_frame(buf: &[u8]) -> Result<Option<(Resp3Frame, usize)>, String> {
    if buf.is_empty() {
        return Ok(None);
    }
    let marker = buf[0];
    let Some(line_end) = find_crlf(buf, 1) else {
        return Ok(None);
    };
    let header_line = &buf[1..line_end];
    let next_pos = line_end + 2;

    match marker {
        b'+' => {
            let s = String::from_utf8_lossy(header_line).into_owned();
            Ok(Some((Resp3Frame::SimpleString(s), next_pos)))
        }
        b'-' => {
            let s = String::from_utf8_lossy(header_line).into_owned();
            Ok(Some((Resp3Frame::SimpleError(s), next_pos)))
        }
        b':' => {
            let s = std::str::from_utf8(header_line)
                .map_err(|e| format!("invalid UTF-8 in RESP3 integer: {e}"))?;
            let val = s
                .parse::<i64>()
                .map_err(|e| format!("invalid RESP3 integer `{s}`: {e}"))?;
            Ok(Some((Resp3Frame::Integer(val), next_pos)))
        }
        b'_' => Ok(Some((Resp3Frame::Null, next_pos))),
        b'#' => match header_line {
            b"t" => Ok(Some((Resp3Frame::Boolean(true), next_pos))),
            b"f" => Ok(Some((Resp3Frame::Boolean(false), next_pos))),
            other => Err(format!(
                "invalid RESP3 boolean payload: `{}`",
                String::from_utf8_lossy(other)
            )),
        },
        b',' => {
            let s = std::str::from_utf8(header_line)
                .map_err(|e| format!("invalid UTF-8 in RESP3 double: {e}"))?;
            let val = match s.to_ascii_lowercase().as_str() {
                "inf" | "+inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                "nan" => f64::NAN,
                other => other
                    .parse::<f64>()
                    .map_err(|e| format!("invalid RESP3 double `{s}`: {e}"))?,
            };
            Ok(Some((Resp3Frame::Double(val), next_pos)))
        }
        b'(' => {
            let s = String::from_utf8_lossy(header_line).into_owned();
            Ok(Some((Resp3Frame::BigNumber(s), next_pos)))
        }
        b'$' | b'!' | b'=' => {
            let s = std::str::from_utf8(header_line)
                .map_err(|e| format!("invalid UTF-8 in RESP3 bulk length: {e}"))?;
            let len = s
                .parse::<isize>()
                .map_err(|e| format!("invalid RESP3 bulk length `{s}`: {e}"))?;
            if len < 0 {
                return Ok(Some((Resp3Frame::Null, next_pos)));
            }
            let byte_len = len as usize;
            let total_needed = next_pos + byte_len + 2;
            if buf.len() < total_needed {
                return Ok(None);
            }
            if &buf[next_pos + byte_len..total_needed] != b"\r\n" {
                return Err("missing trailing CRLF after RESP3 bulk payload".to_string());
            }
            let data = &buf[next_pos..next_pos + byte_len];
            match marker {
                b'$' => Ok(Some((Resp3Frame::BulkString(data.to_vec()), total_needed))),
                b'!' => Ok(Some((
                    Resp3Frame::SimpleError(String::from_utf8_lossy(data).into_owned()),
                    total_needed,
                ))),
                b'=' => {
                    if data.len() < 4 || data[3] != b':' {
                        return Err(
                            "RESP3 verbatim string missing 3-byte format prefix `fmt:`".to_string()
                        );
                    }
                    let format = [data[0], data[1], data[2]];
                    Ok(Some((
                        Resp3Frame::VerbatimString {
                            format,
                            data: data[4..].to_vec(),
                        },
                        total_needed,
                    )))
                }
                _ => unreachable!(),
            }
        }
        b'*' | b'~' | b'>' => {
            let s = std::str::from_utf8(header_line)
                .map_err(|e| format!("invalid UTF-8 in RESP3 aggregate count: {e}"))?;
            let count = s
                .parse::<isize>()
                .map_err(|e| format!("invalid RESP3 aggregate count `{s}`: {e}"))?;
            if count < 0 {
                return Ok(Some((Resp3Frame::Null, next_pos)));
            }
            let count = count as usize;
            let mut items = Vec::with_capacity(count);
            let mut cursor = next_pos;
            for _ in 0..count {
                match parse_resp3_frame(&buf[cursor..])? {
                    Some((item, consumed)) => {
                        items.push(item);
                        cursor += consumed;
                    }
                    None => return Ok(None),
                }
            }
            let frame = match marker {
                b'*' => Resp3Frame::Array(items),
                b'~' => Resp3Frame::Set(items),
                b'>' => Resp3Frame::Push(items),
                _ => unreachable!(),
            };
            Ok(Some((frame, cursor)))
        }
        b'%' => {
            let s = std::str::from_utf8(header_line)
                .map_err(|e| format!("invalid UTF-8 in RESP3 map count: {e}"))?;
            let pairs = s
                .parse::<isize>()
                .map_err(|e| format!("invalid RESP3 map count `{s}`: {e}"))?;
            if pairs < 0 {
                return Ok(Some((Resp3Frame::Null, next_pos)));
            }
            let pairs = pairs as usize;
            let mut entries = Vec::with_capacity(pairs);
            let mut cursor = next_pos;
            for _ in 0..pairs {
                let (k, k_len) = match parse_resp3_frame(&buf[cursor..])? {
                    Some(res) => res,
                    None => return Ok(None),
                };
                cursor += k_len;
                let (v, v_len) = match parse_resp3_frame(&buf[cursor..])? {
                    Some(res) => res,
                    None => return Ok(None),
                };
                cursor += v_len;
                entries.push((k, v));
            }
            Ok(Some((Resp3Frame::Map(entries), cursor)))
        }
        other => Err(format!("unsupported RESP3 type marker byte: 0x{other:02x}")),
    }
}

/// Extract invalidated keys from a RESP3 client-side caching `Push` frame (`>2\r\n$10\r\ninvalidate\r\n...`).
///
/// Returns `Some(keys)` when `frame` is an `invalidate` push notification (with an empty `Vec` if the
/// server sent a `Null` flush-all invalidation payload), or `None` for non-invalidation frames.
pub fn extract_invalidation_keys(frame: &Resp3Frame) -> Option<Vec<Vec<u8>>> {
    let Resp3Frame::Push(items) = frame else {
        return None;
    };
    if items.len() < 2 {
        return None;
    }
    let is_invalidate = match &items[0] {
        Resp3Frame::BulkString(b) => b.as_slice() == b"invalidate",
        Resp3Frame::SimpleString(s) => s == "invalidate",
        _ => false,
    };
    if !is_invalidate {
        return None;
    }
    match &items[1] {
        Resp3Frame::Null => Some(Vec::new()),
        Resp3Frame::Array(raw_keys) => {
            let mut keys = Vec::with_capacity(raw_keys.len());
            for k in raw_keys {
                match k {
                    Resp3Frame::BulkString(b) => keys.push(b.clone()),
                    Resp3Frame::SimpleString(s) => keys.push(s.as_bytes().to_vec()),
                    _ => {}
                }
            }
            Some(keys)
        }
        _ => None,
    }
}

/// Compute CRC16-XMODEM (polynomial `0x1021`, initial value `0x0000`) used by Redis/Valkey Cluster.
pub fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Compute the Redis / Valkey Cluster hash slot (`0..16383`) for `key`, respecting `{hash_tag}` substrings.
pub fn redis_hash_slot(key: &[u8]) -> u16 {
    let effective = if let Some(open) = key.iter().position(|&b| b == b'{') {
        if let Some(rel_close) = key[open + 1..].iter().position(|&b| b == b'}') {
            if rel_close > 0 {
                &key[open + 1..open + 1 + rel_close]
            } else {
                key
            }
        } else {
            key
        }
    } else {
        key
    };
    crc16_xmodem(effective) & 0x3FFF
}

/// Cluster redirection kind (`MOVED` permanent slot migration vs `ASK` transient importing slot).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RedirectKind {
    Moved,
    Ask,
}

impl RedirectKind {
    /// Return canonical uppercase string (`"MOVED"` or `"ASK"`).
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Moved => "MOVED",
            Self::Ask => "ASK",
        }
    }
}

/// Parsed Redis / Valkey Cluster `MOVED` or `ASK` redirection error.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterRedirect {
    pub kind: RedirectKind,
    pub slot: u16,
    pub host: String,
    pub port: u16,
}

/// Parse a `MOVED <slot> <host>:<port>` or `ASK <slot> <host>:<port>` error message.
pub fn parse_cluster_redirect(err_msg: &str) -> Option<ClusterRedirect> {
    let trimmed = err_msg.trim().trim_start_matches('-').trim();
    let mut parts = trimmed.split_whitespace();
    let kind = match parts.next()? {
        "MOVED" => RedirectKind::Moved,
        "ASK" => RedirectKind::Ask,
        _ => return None,
    };
    let slot = parts.next()?.parse::<u16>().ok()?;
    let addr = parts.next()?;
    let (raw_host, port_str) = addr.rsplit_once(':')?;
    let host = raw_host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let port = port_str.parse::<u16>().ok()?;
    if host.is_empty() {
        return None;
    }
    Some(ClusterRedirect {
        kind,
        slot,
        host,
        port,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16_and_redis_hash_slot() {
        assert_eq!(crc16_xmodem(b"123456789"), 0x31C3);
        assert_eq!(redis_hash_slot(b"123456789"), 0x31C3 & 0x3FFF);
        assert_eq!(
            redis_hash_slot(b"{user:1000}.following"),
            redis_hash_slot(b"{user:1000}.followers")
        );
        // Empty braces `{}` hash the whole key per Redis Cluster specification
        assert_eq!(
            redis_hash_slot(b"foo{}{bar}"),
            crc16_xmodem(b"foo{}{bar}") & 0x3FFF
        );
    }

    #[test]
    fn test_resp3_frame_parser_and_invalidation_push() {
        let push_wire = b">2\r\n$10\r\ninvalidate\r\n*2\r\n$6\r\nkey:a1\r\n$6\r\nkey:b2\r\n";
        let (frame, consumed) = parse_resp3_frame(push_wire).unwrap().unwrap();
        assert_eq!(consumed, push_wire.len());
        let keys = extract_invalidation_keys(&frame).unwrap();
        assert_eq!(keys, vec![b"key:a1".to_vec(), b"key:b2".to_vec()]);

        // Incomplete buffer returns Ok(None)
        assert_eq!(parse_resp3_frame(&push_wire[..10]).unwrap(), None);

        // Map + VerbatimString + Double + Boolean + Null
        let map_wire = b"%2\r\n+ok\r\n#t\r\n$3\r\nval\r\n,3.25\r\n";
        let (map_frame, map_consumed) = parse_resp3_frame(map_wire).unwrap().unwrap();
        assert_eq!(map_consumed, map_wire.len());
        assert_eq!(
            map_frame,
            Resp3Frame::Map(vec![
                (
                    Resp3Frame::SimpleString("ok".to_string()),
                    Resp3Frame::Boolean(true)
                ),
                (
                    Resp3Frame::BulkString(b"val".to_vec()),
                    Resp3Frame::Double(3.25)
                ),
            ])
        );
    }

    #[test]
    fn test_parse_cluster_redirect_and_sentinel_cmd() {
        let moved = parse_cluster_redirect("MOVED 3999 10.0.0.12:6380").unwrap();
        assert_eq!(moved.kind, RedirectKind::Moved);
        assert_eq!(moved.slot, 3999);
        assert_eq!(moved.host, "10.0.0.12");
        assert_eq!(moved.port, 6380);

        let ask = parse_cluster_redirect("-ASK 12000 [::1]:6381").unwrap();
        assert_eq!(ask.kind, RedirectKind::Ask);
        assert_eq!(ask.slot, 12000);
        assert_eq!(ask.host, "::1");
        assert_eq!(ask.port, 6381);

        let sentinel = encode_sentinel_get_master_cmd("mymaster");
        assert!(sentinel.starts_with(b"*3\r\n$8\r\nSENTINEL\r\n"));
    }

    #[test]
    fn test_hello3_with_bcast_tracking() {
        let buf = encode_hello3_handshake(
            "vertebra-valkey",
            Some("default"),
            Some("mock-oauth-token"),
            true,
            &["cache:", "user:"],
        );
        let (first, c1) = parse_resp3_frame(&buf).unwrap().unwrap();
        let (second, c2) = parse_resp3_frame(&buf[c1..]).unwrap().unwrap();
        assert_eq!(c1 + c2, buf.len());
        assert!(matches!(first, Resp3Frame::Array(_)));
        assert!(matches!(second, Resp3Frame::Array(_)));
    }
}
