//! Asynchronous TCP, Rustls TLS/mTLS, and Unix socket RESP2/RESP3 transport.

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::codec::{
    RespValue, build_auth, build_client_capa_redirect, build_hello_3, encode_owned_command,
    try_decode_frame,
};
use crate::config::{RespConfig, RespTarget};
use crate::error::RespError;
use crate::tracking::{TrackingCache, build_client_tracking_cmd};

/// Unified async stream supporting plain TCP, Rustls TLS/mTLS TCP, and Unix domain sockets.
pub enum RespStream {
    Tcp(TcpStream),
    TlsTcp(Box<TlsStream<TcpStream>>),
    #[cfg(unix)]
    Unix(UnixStream),
}

/// Backward-compatible alias for Valkey callers.
pub type ValkeyStream = RespStream;

impl AsyncRead for RespStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            Self::TlsTcp(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for RespStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_write(cx, data),
            Self::TlsTcp(s) => Pin::new(s.as_mut()).poll_write(cx, data),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_write(cx, data),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
            Self::TlsTcp(s) => Pin::new(s.as_mut()).poll_flush(cx),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            Self::TlsTcp(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Connect a [`RespStream`] to `target` respecting TLS/mTLS and connect timeout.
pub async fn connect_target(
    target: &RespTarget,
    config: &RespConfig,
) -> Result<RespStream, RespError> {
    let dial_fut = async move {
        match target {
            RespTarget::Unix { path } => {
                #[cfg(unix)]
                {
                    let stream = UnixStream::connect(path).await.map_err(|e| {
                        RespError::connection(format!(
                            "Failed to connect Unix socket {}: {e}",
                            path.display()
                        ))
                    })?;
                    Ok(RespStream::Unix(stream))
                }
                #[cfg(not(unix))]
                {
                    let _ = path;
                    Err(RespError::not_supported(
                        "unix:// sockets are not supported on this platform",
                    ))
                }
            }
            RespTarget::Tcp { host, port } => {
                let tcp = TcpStream::connect((host.as_str(), *port))
                    .await
                    .map_err(|e| {
                        RespError::connection(format!(
                            "Failed to connect to RESP server {host}:{port}: {e}"
                        ))
                    })?;
                let _ = tcp.set_nodelay(true);
                if let Some(ref tls_cfg) = config.tls_config {
                    let sni_host = config.tls_server_name.as_deref().unwrap_or(host.as_str());
                    let server_name = ServerName::try_from(sni_host.to_string()).map_err(|e| {
                        RespError::config(format!("Invalid TLS SNI hostname '{sni_host}': {e}"))
                    })?;
                    let connector = TlsConnector::from(Arc::clone(tls_cfg));
                    let tls_stream = connector.connect(server_name, tcp).await.map_err(|e| {
                        RespError::connection(format!(
                            "RESP TLS handshake failed for {host}:{port}: {e}"
                        ))
                    })?;
                    Ok(RespStream::TlsTcp(Box::new(tls_stream)))
                } else {
                    Ok(RespStream::Tcp(tcp))
                }
            }
        }
    };

    tokio::time::timeout(config.connect_timeout, dial_fut)
        .await
        .map_err(|_| {
            RespError::connection(format!(
                "Timed out connecting to RESP server after {} ms",
                config.connect_timeout.as_millis()
            ))
        })?
}

/// Active framed RESP2/RESP3 connection owning buffers and handshake state.
pub struct RespConnection {
    pub stream: RespStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
    pub resp3_negotiated: bool,
    pub capa_redirect_supported: bool,
    pub tracking_negotiated: bool,
    pub is_readonly_replica: bool,
    command_timeout: Duration,
    tracking_cache: Option<Arc<TrackingCache>>,
}

impl RespConnection {
    /// Wrap a freshly connected stream and perform `HELLO 3` / `CLIENT CAPA redirect` handshake.
    pub async fn establish(
        target: &RespTarget,
        config: &RespConfig,
        is_replica: bool,
        use_auth: bool,
        tracking_cache: Option<Arc<TrackingCache>>,
    ) -> Result<Self, RespError> {
        let stream = connect_target(target, config).await?;
        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(4096),
            resp3_negotiated: false,
            capa_redirect_supported: false,
            tracking_negotiated: false,
            is_readonly_replica: false,
            command_timeout: config.command_timeout,
            tracking_cache,
        };
        if use_auth {
            conn.handshake(config, is_replica).await?;
        }
        Ok(conn)
    }

    async fn handshake(&mut self, config: &RespConfig, is_replica: bool) -> Result<(), RespError> {
        let (user, mut pass, token_gen) = config.resolve_auth(None).await?;

        if config.protocol_resp3 {
            let hello = build_hello_3(
                user.as_deref(),
                pass.as_deref(),
                config.client_name.as_deref(),
            );
            match self.execute_single(&hello).await {
                Ok(_) => {
                    self.resp3_negotiated = true;
                }
                Err(err) if err.is_auth_error() => {
                    if config.enable_gcp_memorystore_iam && token_gen.is_some() {
                        let (retry_user, retry_pass, _retry_gen) =
                            config.resolve_auth(token_gen).await?;
                        pass = retry_pass;
                        let hello_retry = build_hello_3(
                            retry_user.as_deref(),
                            pass.as_deref(),
                            config.client_name.as_deref(),
                        );
                        self.execute_single(&hello_retry).await?;
                        self.resp3_negotiated = true;
                    } else {
                        return Err(err);
                    }
                }
                Err(err) if is_hello_unsupported(&err) => {
                    self.resp3_negotiated = false;
                    self.fallback_resp2_auth(config, user.as_deref(), pass.as_deref(), token_gen)
                        .await?;
                }
                Err(err) => return Err(err),
            }
        } else {
            self.fallback_resp2_auth(config, user.as_deref(), pass.as_deref(), token_gen)
                .await?;
        }

        if config.db > 0 && !config.cluster_enabled {
            let select_cmd = vec![b"SELECT".to_vec(), config.db.to_string().into_bytes()];
            self.execute_single(&select_cmd).await?;
        }

        if config.enable_capa_redirect {
            let capa_cmd = build_client_capa_redirect();
            match self.execute_single(&capa_cmd).await {
                Ok(_) => {
                    self.capa_redirect_supported = true;
                }
                Err(err) if is_capa_unsupported(&err) => {
                    self.capa_redirect_supported = false;
                }
                Err(err) => return Err(err),
            }
        }

        if is_replica {
            let ro = vec![b"READONLY".to_vec()];
            self.execute_single(&ro).await?;
            self.is_readonly_replica = true;
        }

        if config.client_tracking_enabled
            && !is_replica
            && self.resp3_negotiated
            && self.tracking_cache.is_some()
        {
            let track_cmd = build_client_tracking_cmd(&config.client_tracking_prefixes, true);
            match self.execute_single(&track_cmd).await {
                Ok(_) => {
                    self.tracking_negotiated = true;
                }
                Err(err) if is_capa_unsupported(&err) => {
                    self.tracking_negotiated = false;
                }
                Err(err) => return Err(err),
            }
        }

        if let Some(ref cache) = self.tracking_cache {
            let _ = cache.drain_invalidations();
        }

        Ok(())
    }

    async fn fallback_resp2_auth(
        &mut self,
        config: &RespConfig,
        user: Option<&str>,
        pass: Option<&str>,
        token_gen: Option<u64>,
    ) -> Result<(), RespError> {
        if let Some(p) = pass {
            let auth_cmd = build_auth(user, p);
            match self.execute_single(&auth_cmd).await {
                Ok(_) => {}
                Err(err)
                    if err.is_auth_error()
                        && config.enable_gcp_memorystore_iam
                        && token_gen.is_some() =>
                {
                    let (u2, p2, _) = config.resolve_auth(token_gen).await?;
                    if let Some(ref p2_str) = p2 {
                        let retry_cmd = build_auth(u2.as_deref(), p2_str);
                        self.execute_single(&retry_cmd).await?;
                    }
                }
                Err(err) => return Err(err),
            }
        }
        if let Some(ref name) = config.client_name
            && !name.is_empty()
        {
            let cmd = vec![
                b"CLIENT".to_vec(),
                b"SETNAME".to_vec(),
                name.as_bytes().to_vec(),
            ];
            let _ = self.execute_single(&cmd).await;
        }
        Ok(())
    }

    /// Execute one command and return the validated [`RespValue`] reply.
    pub async fn execute_single(&mut self, args: &[Vec<u8>]) -> Result<RespValue, RespError> {
        self.write_buf.clear();
        encode_owned_command(&mut self.write_buf, args);
        let write_timeout = self.command_timeout;
        let bytes_to_send = self.write_buf.split().freeze();
        tokio::time::timeout(write_timeout, async {
            self.stream.write_all(&bytes_to_send).await?;
            self.stream.flush().await
        })
        .await
        .map_err(|_| {
            RespError::connection(format!(
                "RESP write timed out after {} ms",
                write_timeout.as_millis()
            ))
        })?
        .map_err(RespError::from)?;

        let read_timeout = self.effective_read_timeout(args);
        let reply = self.read_reply_with_timeout(read_timeout).await?;
        reply.into_result()
    }

    /// Execute a pipeline batch in one network write and read `cmds.len()` replies.
    pub async fn execute_batch(
        &mut self,
        cmds: &[Vec<Vec<u8>>],
    ) -> Result<Vec<RespValue>, RespError> {
        if cmds.is_empty() {
            return Ok(Vec::new());
        }
        self.write_buf.clear();
        for cmd in cmds {
            encode_owned_command(&mut self.write_buf, cmd);
        }
        let payload = self.write_buf.split().freeze();
        let timeout = self.command_timeout;
        tokio::time::timeout(timeout, async {
            self.stream.write_all(&payload).await?;
            self.stream.flush().await
        })
        .await
        .map_err(|_| {
            RespError::connection(format!(
                "RESP batch write timed out after {} ms",
                timeout.as_millis()
            ))
        })?
        .map_err(RespError::from)?;

        let mut replies = Vec::with_capacity(cmds.len());
        for cmd in cmds {
            let read_timeout = self.effective_read_timeout(cmd);
            replies.push(self.read_reply_with_timeout(read_timeout).await?);
        }
        Ok(replies)
    }

    fn effective_read_timeout(&self, args: &[Vec<u8>]) -> Option<Duration> {
        let Some(first) = args.first() else {
            return Some(self.command_timeout);
        };
        if first.eq_ignore_ascii_case(b"XREADGROUP") || first.eq_ignore_ascii_case(b"XREAD") {
            let mut idx = if first.eq_ignore_ascii_case(b"XREADGROUP")
                && args
                    .get(1)
                    .is_some_and(|v| v.eq_ignore_ascii_case(b"GROUP"))
            {
                4
            } else {
                1
            };
            while idx < args.len() {
                let tok = &args[idx];
                if tok.eq_ignore_ascii_case(b"STREAMS") {
                    break;
                }
                if tok.eq_ignore_ascii_case(b"COUNT") {
                    idx += 2;
                    continue;
                }
                if tok.eq_ignore_ascii_case(b"BLOCK") {
                    if let Some(raw_ms) = args.get(idx + 1)
                        && let Ok(s) = std::str::from_utf8(raw_ms)
                        && let Ok(ms) = s.parse::<u64>()
                    {
                        return if ms == 0 {
                            None
                        } else {
                            Some(
                                self.command_timeout
                                    .saturating_add(Duration::from_millis(ms)),
                            )
                        };
                    }
                    break;
                }
                idx += 1;
            }
        } else if (first.eq_ignore_ascii_case(b"BLPOP")
            || first.eq_ignore_ascii_case(b"BRPOP")
            || first.eq_ignore_ascii_case(b"BRPOPLPUSH")
            || first.eq_ignore_ascii_case(b"BLMOVE")
            || first.eq_ignore_ascii_case(b"BZPOPMIN")
            || first.eq_ignore_ascii_case(b"BZPOPMAX"))
            && args.len() >= 3
            && let Some(raw_secs) = args.last()
            && let Ok(s) = std::str::from_utf8(raw_secs)
            && let Ok(secs) = s.parse::<f64>()
            && secs.is_finite()
            && secs >= 0.0
        {
            return if secs == 0.0 {
                None
            } else {
                Some(
                    self.command_timeout
                        .saturating_add(Duration::from_secs_f64(secs)),
                )
            };
        }
        Some(self.command_timeout)
    }

    /// Non-blocking drain of any already-arrived RESP3 push frames (such as `>invalidate`).
    pub async fn try_drain_pushes_nonblocking(&mut self) -> Result<(), RespError> {
        let mut tmp = [0u8; 4096];
        loop {
            while let Some(frame) = try_decode_frame(&mut self.read_buf)? {
                if let Some(ref cache) = self.tracking_cache {
                    let _ = cache.handle_push_frame(&frame);
                }
            }
            let maybe_n = match &mut self.stream {
                RespStream::Tcp(s) => match s.try_read(&mut tmp) {
                    Ok(n) => Some(Ok(n)),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
                    Err(e) => Some(Err(e)),
                },
                #[cfg(unix)]
                RespStream::Unix(s) => match s.try_read(&mut tmp) {
                    Ok(n) => Some(Ok(n)),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
                    Err(e) => Some(Err(e)),
                },
                RespStream::TlsTcp(s) => {
                    poll_fn(|cx| {
                        let mut rbuf = ReadBuf::new(&mut tmp);
                        match Pin::new(s.as_mut()).poll_read(cx, &mut rbuf) {
                            Poll::Ready(Ok(())) => Poll::Ready(Some(Ok(rbuf.filled().len()))),
                            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                            Poll::Pending => Poll::Ready(None),
                        }
                    })
                    .await
                }
            };
            match maybe_n {
                Some(Ok(0)) => {
                    return Err(RespError::connection(
                        "RESP connection closed by peer while draining push frames",
                    ));
                }
                Some(Ok(n)) => {
                    self.read_buf.put_slice(&tmp[..n]);
                }
                Some(Err(e)) => return Err(RespError::from(e)),
                None => break,
            }
        }
        Ok(())
    }

    /// Read the next non-invalidation RESP3 reply frame, forwarding `>invalidate` pushes to tracking.
    pub async fn read_reply(&mut self) -> Result<RespValue, RespError> {
        self.read_reply_with_timeout(Some(self.command_timeout))
            .await
    }

    /// Read the next non-invalidation RESP3 reply frame with an optional deadline (`None` = un-timed).
    pub async fn read_reply_with_timeout(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<RespValue, RespError> {
        match timeout {
            Some(dur) => tokio::time::timeout(dur, self.read_reply_inner())
                .await
                .map_err(|_| {
                    RespError::connection(format!(
                        "Timed out waiting for RESP response after {} ms",
                        dur.as_millis()
                    ))
                })?,
            None => self.read_reply_inner().await,
        }
    }

    async fn read_reply_inner(&mut self) -> Result<RespValue, RespError> {
        let mut scratch = [0u8; 8192];
        loop {
            while let Some(frame) = try_decode_frame(&mut self.read_buf)? {
                if let Some(ref cache) = self.tracking_cache
                    && cache.handle_push_frame(&frame)
                {
                    continue;
                }
                return Ok(frame);
            }
            let n = self.stream.read(&mut scratch).await?;
            if n == 0 {
                return Err(RespError::connection(
                    "Unexpected EOF reading response from RESP server",
                ));
            }
            self.read_buf.put_slice(&scratch[..n]);
        }
    }

    /// Split the underlying transport preserving any already-buffered incoming bytes.
    pub fn into_split(
        self,
    ) -> (
        tokio::io::ReadHalf<RespStream>,
        tokio::io::WriteHalf<RespStream>,
        BytesMut,
    ) {
        let (reader, writer) = tokio::io::split(self.stream);
        (reader, writer, self.read_buf)
    }
}

fn is_hello_unsupported(err: &RespError) -> bool {
    let lower = err.message.to_ascii_lowercase();
    lower.contains("unknown command") || lower.contains("noproto")
}

fn is_capa_unsupported(err: &RespError) -> bool {
    let lower = err.message.to_ascii_lowercase();
    lower.contains("unknown subcommand")
        || lower.contains("unknown command")
        || lower.contains("syntax")
}
