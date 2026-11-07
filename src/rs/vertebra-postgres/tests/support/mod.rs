use std::time::Duration;

use _postgres::config::{PostgresConfig, RawPostgresOptions};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub async fn listener() -> (TcpListener, PostgresConfig) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = PostgresConfig::validate(RawPostgresOptions {
        user: "app".into(),
        database: "prod".into(),
        host: "127.0.0.1".into(),
        port: listener.local_addr().unwrap().port().into(),
        sslmode: "disable".into(),
        ..Default::default()
    })
    .unwrap();
    (listener, config)
}

pub fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![tag];
    bytes.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}

pub fn parameter(name: &str, value: &str) -> Vec<u8> {
    frame(b'S', format!("{name}\0{value}\0").as_bytes())
}

pub fn trust() -> Vec<u8> {
    [
        frame(b'R', &[0, 0, 0, 0]),
        parameter("client_encoding", "UTF8"),
        frame(b'Z', b"I"),
    ]
    .concat()
}

pub async fn accept(listener: TcpListener) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    startup(&mut stream).await;
    stream
}

pub async fn startup<S: AsyncRead + Unpin>(stream: &mut S) {
    let len = stream.read_i32().await.unwrap();
    assert!((8..1024).contains(&len));
    let mut startup = vec![0; len as usize - 4];
    stream.read_exact(&mut startup).await.unwrap();
    assert_eq!(&startup[..4], &[0, 3, 0, 0]);
    assert_eq!(
        &startup[4..],
        b"user\0app\0database\0prod\0application_name\0vertebra\0client_encoding\0UTF8\0\0"
    );
}

pub async fn eof<S: AsyncRead + Unpin>(stream: &mut S) {
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), stream.read_u8())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}

pub async fn send<S: AsyncWrite + Unpin>(stream: &mut S, frames: &[u8]) {
    stream.write_all(frames).await.unwrap();
}
