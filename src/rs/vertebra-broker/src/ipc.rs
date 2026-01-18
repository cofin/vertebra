use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

#[cfg(unix)]
use tokio::net::UnixListener;

use crate::config::IpcConfig;
use crate::error::BrokerError;
use crate::framing::{read_frame, write_frame};
use crate::protocol::{decode_request, encode_response, BrokerRequest, BrokerResponse};

pub struct IpcServer {
    config: IpcConfig,
}

impl IpcServer {
    pub fn new(config: IpcConfig) -> Self {
        Self { config }
    }

    pub async fn start<F>(&self, handler: F) -> Result<(), BrokerError>
    where
        F: Fn(BrokerRequest) -> BrokerResponse + Send + Sync + Clone + 'static,
    {
        let codec = self
            .config
            .codec
            .clone()
            .unwrap_or_else(|| "bincode".to_string());
        match self.config.transport.as_str() {
            "tcp" => self.start_tcp(handler, codec).await,
            "uds" => self.start_uds(handler, codec).await,
            "named-pipe" | "pipe" => self.start_named_pipe(handler, codec).await,
            other => Err(BrokerError::Ipc(format!("unsupported transport: {other}"))),
        }
    }

    async fn start_tcp<F>(&self, handler: F, codec: String) -> Result<(), BrokerError>
    where
        F: Fn(BrokerRequest) -> BrokerResponse + Send + Sync + Clone + 'static,
    {
        let host = self
            .config
            .host
            .clone()
            .unwrap_or_else(|| "127.0.0.1".into());
        let port = self.config.port.unwrap_or(7001);
        let listener = TcpListener::bind((host.as_str(), port))
            .await
            .map_err(|err| BrokerError::Ipc(err.to_string()))?;
        loop {
            let (socket, _) = listener
                .accept()
                .await
                .map_err(|err| BrokerError::Ipc(err.to_string()))?;
            let handler_clone = handler.clone();
            let codec_clone = codec.clone();
            tokio::spawn(async move {
                let _ = handle_connection(socket, handler_clone, codec_clone).await;
            });
        }
    }

    async fn start_uds<F>(&self, handler: F, codec: String) -> Result<(), BrokerError>
    where
        F: Fn(BrokerRequest) -> BrokerResponse + Send + Sync + Clone + 'static,
    {
        #[cfg(unix)]
        {
            let path = self
                .config
                .path
                .clone()
                .ok_or_else(|| BrokerError::Ipc("UDS path required".to_string()))?;
            let _ = std::fs::remove_file(&path);
            let listener =
                UnixListener::bind(&path).map_err(|err| BrokerError::Ipc(err.to_string()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Err(err) =
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                {
                    if err.kind() != std::io::ErrorKind::NotFound {
                        return Err(BrokerError::Ipc(err.to_string()));
                    }
                }
            }
            loop {
                let (socket, _) = listener
                    .accept()
                    .await
                    .map_err(|err| BrokerError::Ipc(err.to_string()))?;
                let handler_clone = handler.clone();
                let codec_clone = codec.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(socket, handler_clone, codec_clone).await;
                });
            }
        }
        #[cfg(not(unix))]
        {
            let _ = handler;
            Err(BrokerError::Ipc(
                "UDS transport unsupported on this platform".to_string(),
            ))
        }
    }

    async fn start_named_pipe<F>(&self, handler: F, _codec: String) -> Result<(), BrokerError>
    where
        F: Fn(BrokerRequest) -> BrokerResponse + Send + Sync + Clone + 'static,
    {
        #[cfg(windows)]
        {
            use tokio::net::windows::named_pipe::ServerOptions;
            let pipe_name = self
                .config
                .path
                .clone()
                .unwrap_or_else(|| r"\\.\pipe\vertebra-broker".to_string());
            loop {
                let server = ServerOptions::new()
                    .first_pipe_instance(true)
                    .create(&pipe_name)
                    .map_err(|err| BrokerError::Ipc(err.to_string()))?;
                server
                    .connect()
                    .await
                    .map_err(|err| BrokerError::Ipc(err.to_string()))?;
                let handler_clone = handler.clone();
                let codec_clone = codec.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(server, handler_clone, codec_clone).await;
                });
            }
        }
        #[cfg(not(windows))]
        {
            let _ = handler;
            Err(BrokerError::Ipc(
                "named pipe transport unsupported on this platform".to_string(),
            ))
        }
    }
}

async fn handle_connection<F, S>(
    mut socket: S,
    handler: F,
    codec: String,
) -> Result<(), BrokerError>
where
    F: Fn(BrokerRequest) -> BrokerResponse + Send + Sync + Clone + 'static,
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let payload = match read_frame(&mut socket).await {
            Ok(payload) => payload,
            Err(err) => return Err(err),
        };
        let request = decode_request(&codec, &payload)?;
        let response = handler(request);
        let encoded = encode_response(&codec, &response)?;
        write_frame(&mut socket, &encoded).await?;
    }
}
