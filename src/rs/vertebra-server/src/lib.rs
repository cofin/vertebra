use std::convert::Infallible;
use std::net::SocketAddr;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

pub mod worker;

async fn hello(_: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::new(Full::new(Bytes::from("Hello from Vertebra Rust"))))
}

pub async fn serve(host: &str, port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr_str = format!("{}:{}", host, port);
    let addr: SocketAddr = addr_str.parse()?;

    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);

    loop {
        let (stream, _) = tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Accept error: {}", e);
                        continue;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\nShutdown signal received.");
                break;
            }
        };

        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(hello))
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }

    Ok(())
}