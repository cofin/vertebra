//! `vertebra-server` binary entrypoint.
//!
//! With the `python` feature (default), embeds a Python worker thread that
//! imports the user's VertebraApp, extracts routes, and runs the batch worker
//! loop. Without it, serves hardcoded test routes.

use std::sync::Arc;

#[cfg(feature = "python")]
use pyo3::append_to_inittab;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyDict;
use tokio::signal;
#[cfg(feature = "python")]
use tracing::error;
use tracing::info;
use tracing_subscriber::EnvFilter;

use vertebra_core::bridge::BatchBridge;

use vertebra_http::{
    collector::{BatchCollector, PendingResponse},
    config::ServerConfig,
    dispatcher::Dispatcher,
    policy::DrainPolicy,
    response::encode_response_headers,
    route_table::RouteTable,
    router::NativeRouter,
    server,
};

/// Extract routes from a Python VertebraApp and build the Rust RouteTable.
#[cfg(feature = "python")]
fn build_route_table_from_app(_py: Python<'_>, app: &Bound<'_, PyAny>) -> PyResult<RouteTable> {
    let mut table = RouteTable::new();

    let handler_index = app.getattr("handler_index")?;
    let dict = handler_index.cast::<PyDict>()?;

    for (_, info) in dict.iter() {
        let method: String = info.getattr("method")?.extract()?;
        let path: String = info.getattr("path")?.extract()?;
        table.insert(&method, &path);
        info!(method = %method, path = %path, "registered route");
    }

    // Register mount prefixes as catch-all routes.
    let mounts = app.getattr("mounts")?;
    for mount in mounts.try_iter()? {
        let mount = mount?;
        let prefix: String = mount.get_item(0)?.extract()?;
        let trimmed = prefix.trim_end_matches('/');
        let pattern = if trimmed.is_empty() {
            "/*rest".to_string()
        } else {
            format!("{trimmed}/*rest")
        };
        for method in &["GET", "POST", "PUT", "DELETE", "PATCH"] {
            table.insert(method, &pattern);
        }
        info!(prefix = %prefix, "registered mount prefix");
    }

    Ok(table)
}

/// Spawn the response reader task that receives from the bridge and completes Hyper futures.
fn spawn_response_reader(dispatcher: Arc<Dispatcher>) {
    tokio::spawn(async move {
        let bridge = BatchBridge::get().expect("bridge not initialized");
        while let Some(resps) = bridge.recv_responses().await {
            for resp in resps {
                dispatcher.complete(
                    resp.request_id,
                    PendingResponse {
                        status: resp.status,
                        headers_mp: encode_response_headers(&resp.headers),
                        body: bytes::Bytes::from(resp.body),
                    },
                );
            }
        }
    });
}

/// Spawn the Python worker thread.
#[cfg(feature = "python")]
fn spawn_python_worker(app_path: String) {
    std::thread::spawn(move || {
        Python::attach(|py| {
            let result: PyResult<()> = (|| {
                // Register the built-in _vertebra module as "vertebra._vertebra"
                // in sys.modules so Python imports resolve to the embedded module
                // (sharing the same BatchBridge singleton) instead of loading a
                // separate .so with its own static state.
                let native_mod = py.import("_vertebra")?;
                let sys = py.import("sys")?;
                let modules = sys.getattr("modules")?;
                modules.set_item("vertebra._vertebra", &native_mod)?;

                let parts: Vec<&str> = app_path.splitn(2, ':').collect();
                let module = py.import(parts[0])?;
                let app = module.getattr(parts[1])?;

                // Initialize the transport (bridge already created by main).
                native_mod
                    .getattr("init_transport")?
                    .call((), Some(&PyDict::new(py)))?;

                let handlers = app.getattr("_handlers")?;
                let builtins = py.import("builtins")?;
                let mounts_list = builtins.getattr("list")?.call1((app.getattr("mounts")?,))?;

                let runner = py.import("vertebra.server._native")?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("mounts", mounts_list)?;

                // Pass worker_count from VERTEBRA_WORKERS env var (default: 1).
                let worker_count: usize = std::env::var("VERTEBRA_WORKERS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(1);
                kwargs.set_item("worker_count", worker_count)?;

                runner
                    .getattr("run_worker")?
                    .call((&handlers,), Some(&kwargs))?;

                Ok(())
            })();

            if let Err(e) = result {
                error!("Python worker thread failed: {e}");
                e.print(py);
            }
        });
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = Arc::new(ServerConfig::from_env());

    let worker_count: usize = std::env::var("VERTEBRA_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    // --- Initialize bridge ---
    // arena_capacity=256 nodes, slab_capacity=1MB, channel_depth=4
    BatchBridge::init(256, 1_048_576, 4, worker_count).expect("failed to initialize batch bridge");

    // --- Register the _vertebra extension module before Python starts ---
    // This ensures embedded Python shares the same BatchBridge singleton
    // as the server binary (avoids duplicated static state from cdylib loading).
    #[cfg(feature = "python")]
    {
        use _vertebra::pymodule_init;
        append_to_inittab!(pymodule_init);
    }

    // --- Build route table ---
    #[cfg(feature = "python")]
    let (table, app_path) = {
        let app_path = std::env::var("VERTEBRA_APP").unwrap_or_else(|_| "app:app".to_string());
        let table = Python::attach(|py| -> PyResult<RouteTable> {
            let parts: Vec<&str> = app_path.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "VERTEBRA_APP must be 'module:attr', got: {app_path}"
                )));
            }
            let module = py.import(parts[0])?;
            let app_obj = module.getattr(parts[1])?;
            build_route_table_from_app(py, &app_obj)
        })?;
        (table, app_path)
    };

    #[cfg(not(feature = "python"))]
    let table = {
        let mut t = RouteTable::new();
        t.insert("GET", "/hello");
        t.insert("POST", "/echo");
        t
    };

    let router = Arc::new(NativeRouter::from_entries(table.into_entries())?);
    info!("route table built");

    // --- Worker spawning + Dispatcher setup ---
    #[cfg(all(feature = "python", unix))]
    let mut shm_children: Option<Vec<std::process::Child>> = None;

    #[cfg(all(feature = "python", unix))]
    let shm_shutdown: Option<tokio_util::sync::CancellationToken> = None;

    let dispatcher;

    #[cfg(feature = "python")]
    {
        if config.workers.count > 0 {
            // Multi-process mode: SHM dispatcher + response reader
            #[cfg(unix)]
            {
                use vertebra_core::transport::ring_pair::RingPairConfig;
                use vertebra_http::response_reader;
                use vertebra_http::supervisor::SupervisorRingFactory;

                let factory =
                    SupervisorRingFactory::new(config.workers.count, RingPairConfig::default())
                        .expect("failed to create supervisor ring factory");

                info!(
                    workers = config.workers.count,
                    "spawning multi-process workers with SHM transport"
                );

                // Create SHM dispatcher with ring pairs
                let ring_pairs: Vec<Arc<vertebra_core::transport::ring_pair::RingPair>> =
                    factory.all_ring_pairs().to_vec();
                dispatcher = Arc::new(Dispatcher::new_shm(ring_pairs.clone(), 65536));

                // Spawn response reader
                let shutdown_token = tokio_util::sync::CancellationToken::new();
                #[allow(unused_assignments)]
                {
                    let _ = &shm_shutdown;
                } // suppress warning
                tokio::spawn(response_reader::run_response_reader(
                    ring_pairs,
                    Arc::clone(&dispatcher),
                    shutdown_token.clone(),
                ));

                // Spawn worker processes
                let children = factory
                    .spawn_all(&app_path, &config.workers.python_bin)
                    .expect("failed to spawn workers");

                shm_children = Some(children);
            }

            #[cfg(not(unix))]
            {
                dispatcher = Arc::new(Dispatcher::new());
                spawn_response_reader(Arc::clone(&dispatcher));
            }
        } else {
            // In-process mode: bridge dispatcher + bridge response reader
            dispatcher = Arc::new(Dispatcher::new());
            spawn_response_reader(Arc::clone(&dispatcher));
            spawn_python_worker(app_path.clone());
        }
        info!(app = %app_path, bind = %config.bind, "vertebra-server starting");
    }

    #[cfg(not(feature = "python"))]
    {
        dispatcher = Arc::new(Dispatcher::new());
        spawn_response_reader(Arc::clone(&dispatcher));
        info!(bind = %config.bind, "vertebra-server starting (no python)");
    }

    // --- Collector ---
    let (collector, collector_tx) =
        BatchCollector::new(Arc::clone(&dispatcher), DrainPolicy::default_fixed(), 1024);
    tokio::spawn(collector.run());

    // --- Start server ---
    let shutdown = async {
        signal::ctrl_c().await.expect("failed to listen for ctrl-c");
        info!("ctrl-c received");
    };

    server::run_with_collector(
        Arc::clone(&config),
        router,
        dispatcher,
        collector_tx,
        shutdown,
    )
    .await?;

    // --- Graceful shutdown of SHM workers ---
    #[cfg(all(feature = "python", unix))]
    if let Some(ref mut children) = shm_children {
        use vertebra_http::supervisor::SupervisorRingFactory;

        info!("shutting down multi-process workers");
        SupervisorRingFactory::shutdown_workers(children, std::time::Duration::from_secs(5));
    }

    Ok(())
}
