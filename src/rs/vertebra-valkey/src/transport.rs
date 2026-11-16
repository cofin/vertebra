//! Asynchronous TCP, Rustls TLS/mTLS, and Unix socket RESP3 transport for `vertebra-valkey`.

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
use crate::config::{ValkeyConfigInner, ValkeyTarget};
use crate::error::ValkeyError;
use crate::tracking::{TrackingCache, build_client_tracking_cmd};

/// Unified async stream supporting plain TCP, Rustls TLS/mTLS TCP, and Unix domain sockets.
pub enum ValkeyStream {
    Tcp(TcpStream),
    TlsTcp(Box<TlsStream<TcpStream>>),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl AsyncRead for ValkeyStream {
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

impl AsyncWrite for ValkeyStream {
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

/// Connect a [`ValkeyStream`] to `target` respecting TLS/mTLS and connect timeout.
pub async fn connect_target(
    target: &ValkeyTarget,
    config: &ValkeyConfigInner,
) -> Result<ValkeyStream, ValkeyError> {
    let dial_fut = async move {
        match target {
            ValkeyTarget::Unix { path } => {
                #[cfg(unix)]
                {
                    let stream = UnixStream::connect(path).await.map_err(|e| {
                        ValkeyError::connection(format!(
                            "Failed to connect Unix socket {}: {e}",
                            path.display()
                        ))
                    })?;
                    Ok(ValkeyStream::Unix(stream))
                }
                #[cfg(not(unix))]
                {
                    let _ = path;
                    Err(ValkeyError::not_supported(
                        "unix:// sockets are not supported on this platform",
                    ))
                }
            }
            ValkeyTarget::Tcp { host, port } => {
                let tcp = TcpStream::connect((host.as_str(), *port))
                    .await
                    .map_err(|e| {
                        ValkeyError::connection(format!(
                            "Failed to connect to Valkey {host}:{port}: {e}"
                        ))
                    })?;
                let _ = tcp.set_nodelay(true);
                if let Some(ref tls_cfg) = config.tls_config {
                    let sni_host = config.tls_server_name.as_deref().unwrap_or(host.as_str());
                    let server_name = ServerName::try_from(sni_host.to_string()).map_err(|e| {
                        ValkeyError::config(format!("Invalid TLS SNI hostname '{sni_host}': {e}"))
                    })?;
                    let connector = TlsConnector::from(Arc::clone(tls_cfg));
                    let tls_stream = connector.connect(server_name, tcp).await.map_err(|e| {
                        ValkeyError::connection(format!(
                            "Valkey TLS handshake failed for {host}:{port}: {e}"
                        ))
                    })?;
                    Ok(ValkeyStream::TlsTcp(Box::new(tls_stream)))
                } else {
                    Ok(ValkeyStream::Tcp(tcp))
                }
            }
        }
    };

    tokio::time::timeout(config.connect_timeout, dial_fut)
        .await
        .map_err(|_| {
            ValkeyError::connection(format!(
                "Timed out connecting to Valkey after {} ms",
                config.connect_timeout.as_millis()
            ))
        })?
}

/// Active framed RESP2/RESP3 connection owning buffers and handshake state.
pub struct RespConnection {
    pub stream: ValkeyStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
    pub resp3_negotiated: bool,
    pub capa_redirect_supported: bool,
    pub is_readonly_replica: bool,
    command_timeout: Duration,
    tracking_cache: Option<Arc<TrackingCache>>,
}

impl RespConnection {
    /// Wrap a freshly connected stream and perform Valkey `HELLO 3` / `CLIENT CAPA redirect` handshake.
    pub async fn establish(
        target: &ValkeyTarget,
        config: &ValkeyConfigInner,
        is_replica: bool,
        use_auth: bool,
        tracking_cache: Option<Arc<TrackingCache>>,
    ) -> Result<Self, ValkeyError> {
        let stream = connect_target(target, config).await?;
        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(4096),
            resp3_negotiated: false,
            capa_redirect_supported: false,
            is_readonly_replica: false,
            command_timeout: config.command_timeout,
            tracking_cache,
        };
        if use_auth {
            conn.handshake(config, is_replica).await?;
        }
        Ok(conn)
    }

    async fn handshake(
        &mut self,
        config: &ValkeyConfigInner,
        is_replica: bool,
    ) -> Result<(), ValkeyError> {
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
            let _ = self.execute_single(&track_cmd).await?;
        }

        if let Some(ref cache) = self.tracking_cache {
            let _ = cache.drain_invalidations();
        }

        Ok(())
    }

    async fn fallback_resp2_auth(
        &mut self,
        config: &ValkeyConfigInner,
        user: Option<&str>,
        pass: Option<&str>,
        token_gen: Option<u64>,
    ) -> Result<(), ValkeyError> {
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
    pub async fn execute_single(&mut self, args: &[Vec<u8>]) -> Result<RespValue, ValkeyError> {
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
            ValkeyError::connection(format!(
                "Valkey write timed out after {} ms",
                write_timeout.as_millis()
            ))
        })?
        .map_err(ValkeyError::from)?;

        let reply = self.read_reply().await?;
        reply.into_result()
    }

    /// Execute a pipeline batch in one network write and read `cmds.len()` replies.
    pub async fn execute_batch(
        &mut self,
        cmds: &[Vec<Vec<u8>>],
    ) -> Result<Vec<RespValue>, ValkeyError> {
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
            ValkeyError::connection(format!(
                "Valkey batch write timed out after {} ms",
                timeout.as_millis()
            ))
        })?
        .map_err(ValkeyError::from)?;

        let mut replies = Vec::with_capacity(cmds.len());
        for _ in 0..cmds.len() {
            replies.push(self.read_reply().await?);
        }
        Ok(replies)
    }

    /// Non-blocking drain of any already-arrived RESP3 push frames (such as `>invalidate`).
    pub async fn try_drain_pushes_nonblocking(&mut self) -> Result<(), ValkeyError> {
        let mut tmp = [0u8; 4096];
        loop {
            while let Some(frame) = try_decode_frame(&mut self.read_buf)? {
                if let Some(ref cache) = self.tracking_cache {
                    let _ = cache.handle_push_frame(&frame);
                }
            }
            let maybe_n = match &mut self.stream {
                ValkeyStream::Tcp(s) => match s.try_read(&mut tmp) {
                    Ok(n) => Some(Ok(n)),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
                    Err(e) => Some(Err(e)),
                },
                #[cfg(unix)]
                ValkeyStream::Unix(s) => match s.try_read(&mut tmp) {
                    Ok(n) => Some(Ok(n)),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
                    Err(e) => Some(Err(e)),
                },
                ValkeyStream::TlsTcp(s) => {
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
                    return Err(ValkeyError::connection(
                        "Valkey connection closed by peer while draining push frames",
                    ));
                }
                Some(Ok(n)) => {
                    self.read_buf.put_slice(&tmp[..n]);
                }
                Some(Err(e)) => return Err(ValkeyError::from(e)),
                None => break,
            }
        }
        Ok(())
    }

    /// Read the next non-invalidation RESP3 reply frame, forwarding `>invalidate` pushes to tracking.
    pub async fn read_reply(&mut self) -> Result<RespValue, ValkeyError> {
        let timeout = self.command_timeout;
        tokio::time::timeout(timeout, self.read_reply_inner())
            .await
            .map_err(|_| {
                ValkeyError::connection(format!(
                    "Timed out waiting for Valkey response after {} ms",
                    timeout.as_millis()
                ))
            })?
    }

    async fn read_reply_inner(&mut self) -> Result<RespValue, ValkeyError> {
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
                return Err(ValkeyError::connection(
                    "Unexpected EOF reading response from Valkey server",
                ));
            }
            self.read_buf.put_slice(&scratch[..n]);
        }
    }
    /// Split the underlying transport preserving any already-buffered incoming bytes.
    pub fn into_split(
        self,
    ) -> (
        tokio::io::ReadHalf<ValkeyStream>,
        tokio::io::WriteHalf<ValkeyStream>,
        BytesMut,
    ) {
        let (reader, writer) = tokio::io::split(self.stream);
        (reader, writer, self.read_buf)
    }
}

fn is_hello_unsupported(err: &ValkeyError) -> bool {
    let lower = err.message.to_ascii_lowercase();
    lower.contains("unknown command") || lower.contains("noproto")
}

fn is_capa_unsupported(err: &ValkeyError) -> bool {
    let lower = err.message.to_ascii_lowercase();
    lower.contains("unknown subcommand")
        || lower.contains("unknown command")
        || lower.contains("syntax")
}
