use tracing_subscriber::EnvFilter;

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json_logs = std::env::var("VERTEBRA_LOG_JSON")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if json_logs {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    } else {
        let subscriber = tracing_subscriber::fmt().with_env_filter(filter).finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }
}
