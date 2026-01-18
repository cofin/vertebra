use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use vertebra_broker::{BrokerRequest, BrokerResponse, IpcConfig, IpcServer};

#[tokio::test]
async fn tcp_echo_roundtrip() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let config = IpcConfig {
        transport: "tcp".into(),
        path: None,
        host: Some("127.0.0.1".into()),
        port: Some(port),
        codec: None,
    };

    let server = IpcServer::new(config);
    let handler = |request: BrokerRequest| match request {
        BrokerRequest::Health { pool } => BrokerResponse::Health {
            ok: pool == "default",
        },
        _ => BrokerResponse::Error {
            message: "unsupported".into(),
        },
    };

    tokio::spawn(async move {
        let _ = server.start(handler).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");

    let request = BrokerRequest::Health {
        pool: "default".into(),
    };
    let payload = bincode::serialize(&request).expect("serialize");
    let size = payload.len() as u32;
    stream.write_all(&size.to_be_bytes()).await.expect("write");
    stream.write_all(&payload).await.expect("write");

    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await.expect("read");
    let response_size = u32::from_be_bytes(header) as usize;
    let mut response_payload = vec![0u8; response_size];
    stream
        .read_exact(&mut response_payload)
        .await
        .expect("read");

    let response: BrokerResponse = bincode::deserialize(&response_payload).expect("decode");
    assert_eq!(response, BrokerResponse::Health { ok: true });
}
