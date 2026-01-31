use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use vertebra_engine::embedded::{init_python, EmbeddedWorker};
use vertebra_engine::executor::EmbeddedService;
use pyo3::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_python(&[])?;

    let app_code = c"
async def app(scope, receive, send):
    assert scope['type'] == 'http'
    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
    await send({'type': 'http.response.body', 'body': b'Hello from Python'})
";

    let worker = Python::with_gil(|py| -> PyResult<EmbeddedWorker> {
         let module = PyModule::from_code(py, app_code, c"app.py", c"app")?;
         let app = module.getattr("app")?;
         Ok(EmbeddedWorker::new(app))
    })?;

    let service = EmbeddedService::new(worker);

    let addr = "127.0.0.1:3000";
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let service = service.clone();

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
