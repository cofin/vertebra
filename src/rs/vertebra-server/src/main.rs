use clap::Parser;
use vertebra_server::{serve, ServerConfig};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1", env = "VERTEBRA_HOST")]
    host: String,

    #[arg(short, long, default_value_t = 8000, env = "VERTEBRA_PORT")]
    port: u16,

    #[arg(long, default_value = "app.py", env = "VERTEBRA_PYTHON_APP")]
    app: String,

    #[arg(long, default_value = "python")]
    python: String,

    #[arg(long, default_value_t = 1, env = "VERTEBRA_WORKERS")]
    workers: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let config = ServerConfig {
        host: args.host,
        port: args.port,
        app_path: args.app,
        python_path: args.python,
        workers: args.workers,
    };

    serve(config).await
}
