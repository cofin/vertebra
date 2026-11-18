//! Cleartext HTTP/2 (`h2c`) prior-knowledge detection.

use std::io;

use tokio::net::TcpStream;

/// Standard 24-byte HTTP/2 connection preface (`"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"`).
pub const H2C_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Detected wire protocol on a cleartext TCP connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleartextProtocol {
    /// Standard HTTP/1.1 connection.
    Http1,
    /// Cleartext HTTP/2 prior-knowledge connection starting with `H2C_PREFACE`.
    H2cPriorKnowledge,
}

/// Peek at the leading bytes of a `TcpStream` to determine whether the client
/// sent the 24-byte HTTP/2 connection preface (`H2C_PREFACE`).
pub async fn detect_cleartext_protocol(stream: &TcpStream) -> io::Result<CleartextProtocol> {
    let mut buf = [0u8; 24];
    let n = stream.peek(&mut buf).await?;
    if n >= 4
        && buf[..n.min(24)] == H2C_PREFACE[..n.min(24)]
        && (n == 24 || buf.starts_with(b"PRI * "))
    {
        return Ok(CleartextProtocol::H2cPriorKnowledge);
    }
    Ok(CleartextProtocol::Http1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn detects_prior_knowledge_preface_and_http1() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let h2_client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(H2C_PREFACE).await.unwrap();
            stream
        });
        let (server_h2, _) = listener.accept().await.unwrap();
        let _h2_guard = h2_client.await.unwrap();
        assert_eq!(
            detect_cleartext_protocol(&server_h2).await.unwrap(),
            CleartextProtocol::H2cPriorKnowledge
        );

        let h1_client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
            stream
        });
        let (server_h1, _) = listener.accept().await.unwrap();
        let _h1_guard = h1_client.await.unwrap();
        assert_eq!(
            detect_cleartext_protocol(&server_h1).await.unwrap(),
            CleartextProtocol::Http1
        );
    }
}
