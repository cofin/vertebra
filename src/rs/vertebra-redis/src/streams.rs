//! Redis Streams and Sorted Sets operations for distributed queues and task scheduling.

use redis::Value;

use crate::client_core::RedisCoreClient;
use crate::errors::RedisDriverError;

/// Create a consumer group on a stream.
pub async fn xgroup_create(
    client: &RedisCoreClient,
    stream: &str,
    group: &str,
    id: &str,
    mkstream: bool,
) -> Result<bool, RedisDriverError> {
    let mut cmd = redis::cmd("XGROUP");
    cmd.arg("CREATE").arg(stream).arg(group).arg(id);
    if mkstream {
        cmd.arg("MKSTREAM");
    }
    match client.execute_cmd(&cmd).await {
        Ok(_) => Ok(true),
        Err(e) => {
            if e.message().contains("BUSYGROUP") {
                Ok(false)
            } else {
                Err(e)
            }
        }
    }
}

/// Append an entry to a stream.
pub async fn xadd(
    client: &RedisCoreClient,
    stream: &str,
    fields: &[(Vec<u8>, Vec<u8>)],
    id: &str,
    maxlen: Option<usize>,
    approximate: bool,
) -> Result<Vec<u8>, RedisDriverError> {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(stream);
    if let Some(ml) = maxlen {
        cmd.arg("MAXLEN");
        if approximate {
            cmd.arg("~");
        }
        cmd.arg(ml);
    }
    cmd.arg(id);
    for (k, v) in fields {
        cmd.arg(k.as_slice()).arg(v.as_slice());
    }
    let val = client.execute_cmd(&cmd).await?;
    match val {
        Value::BulkString(bytes) => Ok(bytes),
        Value::SimpleString(s) => Ok(s.into_bytes()),
        other => Err(RedisDriverError::data(
            format!("Unexpected response for XADD: {other:?}"),
            false,
        )),
    }
}

/// Read entries from streams via consumer group.
pub async fn xreadgroup(
    client: &RedisCoreClient,
    group: &str,
    consumer: &str,
    streams: &[(String, String)],
    count: Option<usize>,
    block_ms: Option<usize>,
) -> Result<Value, RedisDriverError> {
    let mut cmd = redis::cmd("XREADGROUP");
    cmd.arg("GROUP").arg(group).arg(consumer);
    if let Some(cnt) = count {
        cmd.arg("COUNT").arg(cnt);
    }
    if let Some(blk) = block_ms {
        cmd.arg("BLOCK").arg(blk);
    }
    cmd.arg("STREAMS");
    for (s, _) in streams {
        cmd.arg(s.as_str());
    }
    for (_, id) in streams {
        cmd.arg(id.as_str());
    }
    let timeout = match block_ms {
        None => Some(std::time::Duration::from_secs(2)),
        Some(0) => None,
        Some(ms) => Some(
            std::time::Duration::from_millis(ms as u64)
                .saturating_add(std::time::Duration::from_secs(2)),
        ),
    };
    client.execute_cmd_with_timeout(&cmd, timeout).await
}

/// Acknowledge processed stream entries.
pub async fn xack(
    client: &RedisCoreClient,
    stream: &str,
    group: &str,
    ids: &[Vec<u8>],
) -> Result<i64, RedisDriverError> {
    let mut cmd = redis::cmd("XACK");
    cmd.arg(stream).arg(group);
    for id in ids {
        cmd.arg(id.as_slice());
    }
    let val = client.execute_cmd(&cmd).await?;
    match val {
        Value::Int(i) => Ok(i),
        _ => Ok(0),
    }
}

/// Reclaim pending entries exceeding idle time.
pub async fn xautoclaim(
    client: &RedisCoreClient,
    stream: &str,
    group: &str,
    consumer: &str,
    min_idle_ms: usize,
    start_id: &str,
    count: Option<usize>,
) -> Result<Value, RedisDriverError> {
    let mut cmd = redis::cmd("XAUTOCLAIM");
    cmd.arg(stream)
        .arg(group)
        .arg(consumer)
        .arg(min_idle_ms)
        .arg(start_id);
    if let Some(cnt) = count {
        cmd.arg("COUNT").arg(cnt);
    }
    client.execute_cmd(&cmd).await
}

/// Read range of stream entries.
pub async fn xrange(
    client: &RedisCoreClient,
    stream: &str,
    start: &str,
    end: &str,
    count: Option<usize>,
) -> Result<Value, RedisDriverError> {
    let mut cmd = redis::cmd("XRANGE");
    cmd.arg(stream).arg(start).arg(end);
    if let Some(cnt) = count {
        cmd.arg("COUNT").arg(cnt);
    }
    client.execute_cmd(&cmd).await
}

/// Delete stream entries by ID.
pub async fn xdel(
    client: &RedisCoreClient,
    stream: &str,
    ids: &[Vec<u8>],
) -> Result<i64, RedisDriverError> {
    let mut cmd = redis::cmd("XDEL");
    cmd.arg(stream);
    for id in ids {
        cmd.arg(id.as_slice());
    }
    let val = client.execute_cmd(&cmd).await?;
    match val {
        Value::Int(i) => Ok(i),
        _ => Ok(0),
    }
}

/// Add members to a sorted set.
pub async fn zadd(
    client: &RedisCoreClient,
    key: &str,
    mapping: &[(f64, Vec<u8>)],
) -> Result<i64, RedisDriverError> {
    let mut cmd = redis::cmd("ZADD");
    cmd.arg(key);
    for (score, member) in mapping {
        cmd.arg(*score).arg(member.as_slice());
    }
    let val = client.execute_cmd(&cmd).await?;
    match val {
        Value::Int(i) => Ok(i),
        _ => Ok(0),
    }
}

/// Read members from a sorted set by score range.
pub async fn zrangebyscore(
    client: &RedisCoreClient,
    key: &str,
    min: &str,
    max: &str,
    withscores: bool,
    offset: Option<usize>,
    count: Option<usize>,
) -> Result<Value, RedisDriverError> {
    let mut cmd = redis::cmd("ZRANGEBYSCORE");
    cmd.arg(key).arg(min).arg(max);
    if withscores {
        cmd.arg("WITHSCORES");
    }
    if let (Some(off), Some(cnt)) = (offset, count) {
        cmd.arg("LIMIT").arg(off).arg(cnt);
    }
    client.execute_cmd(&cmd).await
}

/// Remove members from a sorted set.
pub async fn zrem(
    client: &RedisCoreClient,
    key: &str,
    members: &[Vec<u8>],
) -> Result<i64, RedisDriverError> {
    let mut cmd = redis::cmd("ZREM");
    cmd.arg(key);
    for m in members {
        cmd.arg(m.as_slice());
    }
    let val = client.execute_cmd(&cmd).await?;
    match val {
        Value::Int(i) => Ok(i),
        _ => Ok(0),
    }
}
