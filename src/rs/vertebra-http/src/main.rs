//! `vertebra-server` binary entrypoint.
//!
//! With the `python` feature (default), embeds a Python worker thread that
//! imports the user's App, extracts routes, and runs the batch worker
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
    batcher::{BatchLimit, PendingResponse, RequestBatcher},
    config::ServerConfig,
    dispatcher::Dispatcher,
    response::encode_response_headers,
    routing::{RouteTable, Router},
    server,
};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Extract routes from a Python App and build the Rust RouteTable.
#[cfg(feature = "python")]
fn build_route_table_from_app(py: Python<'_>, app: &Bound<'_, PyAny>) -> PyResult<RouteTable> {
    let mut table = RouteTable::new();

    // If the app is a raw ASGI function/callable instead of an App, wrap it.
    let app_obj = if app.getattr("handler_index").is_err() && app.is_callable() {
        info!("wrapping raw callable app in ASGI adapter");
        let app_mod = py.import("vertebra.framework._app")?;
        let wrapped = app_mod.getattr("App")?.call0()?;
        wrapped.getattr("mount")?.call1(("/", app))?;
        wrapped
    } else {
        app.clone()
    };

    let handler_index = app_obj.getattr("handler_index")?;
    let dict = handler_index.cast::<PyDict>()?;

    for (_, info) in dict.iter() {
        let method: String = info.getattr("method")?.extract()?;
        let path: String = info.getattr("path")?.extract()?;
        table.insert(&method, &path);
        info!(method = %method, path = %path, "registered route");
    }

    // Register mount prefixes as catch-all routes.
    let mounts = app_obj.getattr("mounts")?;
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
                        body: resp.body,
                        stream_rx: None,
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

                // If the app is a raw ASGI function/callable instead of an App, wrap it.
                let app_obj = if app.getattr("handler_index").is_err() && app.is_callable() {
                    let app_mod = py.import("vertebra.framework._app")?;
                    let wrapped = app_mod.getattr("App")?.call0()?;
                    wrapped.getattr("mount")?.call1(("/", &app))?;
                    wrapped
                } else {
                    app
                };

                // Initialize the transport (bridge already created by main).
                native_mod
                    .getattr("init_transport")?
                    .call((), Some(&PyDict::new(py)))?;

                let handlers = app_obj.getattr("_handlers")?;
                let builtins = py.import("builtins")?;
                let mounts_list = builtins
                    .getattr("list")?
                    .call1((app_obj.getattr("mounts")?,))?;

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

    #[cfg(unix)]
    vertebra_http::workers::cleanup_stale_shm();
    vertebra_http::workers::install_panic_cleanup_hook();

    let server_shutdown_token = tokio_util::sync::CancellationToken::new();
    let config = Arc::new(ServerConfig::from_env());

    // --- Initialize bridge ---
    // arena_capacity=256 nodes, slab_capacity=1MB, channel_depth=4
    // Multi-process SHM uses its own transport, but we still initialize the
    // bridge singleton for in-process compatibility.
    BatchBridge::init(256, 1_048_576, 4, config.workers.count)
        .expect("failed to initialize batch bridge");

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

    let router = Arc::new(Router::from_entries(table.into_entries())?);
    info!("route table built");

    // --- Worker spawning + Dispatcher setup ---
    #[cfg(all(feature = "python", unix))]
    let mut shm_factory: Option<Arc<std::sync::Mutex<vertebra_http::workers::RingFactory>>> = None;

    let dispatcher;

    #[cfg(feature = "python")]
    {
        if config.topology == vertebra_http::config::Topology::Shm {
            // Multi-process mode: SHM dispatcher + response reader + supervisor
            #[cfg(unix)]
            {
                use std::sync::Mutex;
                use vertebra_core::transport::ring_pair::RingPairConfig;
                use vertebra_http::dispatcher::{ShmDispatcher, shm_reader};
                use vertebra_http::workers::{RingFactory, Supervisor};

                let worker_count = if config.workers.count > 0 {
                    config.workers.count
                } else {
                    1
                };

                let mut factory = RingFactory::new(worker_count, RingPairConfig::default())
                    .expect("failed to create ring factory");

                info!(
                    workers = worker_count,
                    "spawning multi-process workers with SHM transport"
                );

                // Create SHM dispatcher with ring pairs
                let ring_pairs = factory.all_ring_pairs().to_vec();
                let shm = ShmDispatcher::new(ring_pairs.clone(), 65536);
                dispatcher = Arc::new(
                    Dispatcher::from_shm(Arc::clone(&shm))
                        .with_backpressure(config.backpressure.clone()),
                );

                // Spawn sweeper
                shm.spawn_sweeper(server_shutdown_token.clone());

                // Spawn response reader
                shm_reader::spawn_response_readers(
                    ring_pairs,
                    Arc::clone(&dispatcher),
                    server_shutdown_token.clone(),
                );

                // Spawn worker processes
                let children = factory
                    .spawn_all(&app_path, &config.workers.python_bin)
                    .expect("failed to spawn workers");

                factory.attach_children(children, &app_path, &config.workers.python_bin);

                let factory = Arc::new(Mutex::new(factory));
                Supervisor::spawn(
                    Arc::clone(&factory),
                    Arc::clone(&shm),
                    server_shutdown_token.clone(),
                );

                shm_factory = Some(factory);
            }

            #[cfg(not(unix))]
            {
                dispatcher =
                    Arc::new(Dispatcher::new().with_backpressure(config.backpressure.clone()));
                spawn_response_reader(Arc::clone(&dispatcher));
            }
        } else {
            // In-process mode: bridge dispatcher + bridge response reader
            dispatcher = Arc::new(Dispatcher::new().with_backpressure(config.backpressure.clone()));
            spawn_response_reader(Arc::clone(&dispatcher));
            spawn_python_worker(app_path.clone());
        }
        info!(app = %app_path, bind = %config.bind, topology = ?config.topology, "vertebra-server starting");
    }

    #[cfg(not(feature = "python"))]
    {
        dispatcher = Arc::new(Dispatcher::new().with_backpressure(config.backpressure.clone()));
        spawn_response_reader(Arc::clone(&dispatcher));
        info!(bind = %config.bind, "vertebra-server starting (no python)");
    }

    // --- Collector ---
    // Shard the collector to break the single-threaded bottleneck.
    // 8 shards provides better scale for many-core systems.
    let shard_count = 8;
    let collector_shards = RequestBatcher::spawn_pool(
        Arc::clone(&dispatcher),
        BatchLimit::default(),
        shard_count,
        16384,
    );

    // --- Start server ---
    let shutdown = async {
        signal::ctrl_c().await.expect("failed to listen for ctrl-c");
        info!("ctrl-c received");
    };

    server::run_with_collector(
        Arc::clone(&config),
        router,
        dispatcher,
        collector_shards,
        shutdown,
    )
    .await?;

    server_shutdown_token.cancel();

    // --- Graceful shutdown of SHM workers ---
    #[cfg(all(feature = "python", unix))]
    if let Some(factory) = shm_factory {
        use vertebra_http::workers::RingFactory;

        info!("shutting down multi-process workers");
        let mut factory = factory.lock().unwrap();
        let lifecycles = factory.lifecycles_mut();
        let mut children: Vec<_> = lifecycles
            .iter_mut()
            .filter_map(|l| l.child.take())
            .collect();
        RingFactory::shutdown_workers(&mut children, std::time::Duration::from_secs(5));
    }

    Ok(())
}
