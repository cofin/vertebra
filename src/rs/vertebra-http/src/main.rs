//! `vertebra-server` binary entrypoint.
//!
//! Builds an explicit single-threaded (`current_thread`) or multi-threaded
//! (`multi_thread`) Tokio runtime sized to `config.worker_threads`, supervises
//! `SO_REUSEPORT` embedded worker processes when `Topology::Embedded` runs with
//! `workers > 1`, and spawns free-threaded Python worker pools on `3.14t`/`3.15t`.

use std::sync::Arc;
#[cfg(feature = "python")]
use std::thread::JoinHandle;

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

#[cfg(not(feature = "python"))]
use vertebra_http::batcher::PendingResponse;
use vertebra_http::{
    batcher::{BatchLimit, RequestBatcher},
    config::{ServerConfig, Topology},
    dispatcher::Dispatcher,
    routing::{RouteTable, Router},
    server,
};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "python")]
struct EmbeddedPythonWorkers {
    stop_event: Py<PyAny>,
    handles: Vec<JoinHandle<()>>,
}

#[cfg(feature = "python")]
impl EmbeddedPythonWorkers {
    fn shutdown(self) {
        Python::attach(|py| {
            let _ = self.stop_event.bind(py).call_method0("set");
        });
        for handle in self.handles {
            let _ = handle.join();
        }
    }
}

/// Extract routes from a Python App and build the Rust RouteTable.
#[cfg(feature = "python")]
fn build_route_table_from_app(py: Python<'_>, app: &Bound<'_, PyAny>) -> PyResult<RouteTable> {
    let mut table = RouteTable::new();

    let app_obj = if app.getattr("handler_index").is_err() && app.is_callable() {
        info!("wrapping raw callable app in ASGI adapter");
        let app_mod = py.import("vertebra.framework._app")?;
        let wrapped = app_mod.getattr("App")?.call0()?;
        wrapped.getattr("mount")?.call1(("/", app))?;
        wrapped
    } else {
        app.clone()
    };

    if let Ok(ensure_fn) = app_obj.getattr("ensure_dev_welcome_route") {
        let _ = ensure_fn.call0()?;
    }

    let handler_index = app_obj.getattr("handler_index")?;
    let dict = handler_index.cast::<PyDict>()?;

    for (_, info) in dict.iter() {
        let method: String = info.getattr("method")?.extract()?;
        let path: String = info.getattr("path")?.extract()?;
        if let Ok(explicit_id) = info.getattr("id").and_then(|v| v.extract::<u64>()) {
            table.insert_with_id(&method, &path, explicit_id);
        } else {
            table.insert(&method, &path);
        }
        info!(method = %method, path = %path, "registered route");
    }

    let mounts = app_obj.getattr("mounts")?;
    for (mount_idx, mount) in mounts.try_iter()?.enumerate() {
        let mount = mount?;
        let prefix: String = mount.get_item(0)?.extract()?;
        table.insert_mount_prefix_indexed(&prefix, mount_idx);
        info!(prefix = %prefix, mount_idx, "registered mount prefix");
    }

    Ok(table)
}

/// Spawn the fallback response reader task when running without the Python feature.
#[cfg(not(feature = "python"))]
fn spawn_response_reader(dispatcher: Arc<Dispatcher>) {
    tokio::spawn(async move {
        let bridge = BatchBridge::get().expect("bridge not initialized");
        while let Some(resps) = bridge.recv_responses().await {
            for resp in resps {
                dispatcher.complete(
                    resp.request_id,
                    PendingResponse {
                        status: resp.status,
                        headers_mp: resp.headers_raw,
                        body: resp.body,
                        stream_rx: None,
                    },
                );
            }
        }
    });
}

#[cfg(feature = "python")]
struct PreparedEmbeddedApp {
    handlers: Py<PyAny>,
    mounts: Py<PyAny>,
    stop_event: Py<PyAny>,
    worker_threads: usize,
}

/// Prepend `.venv/bin` and `VIRTUAL_ENV/bin` to `PATH` while single-threaded before CPython initialization.
#[cfg(feature = "python")]
fn prepare_embedded_python_env(python_bin: &str) {
    let mut prepend_dirs = Vec::new();
    let py_path = std::path::Path::new(python_bin);
    if py_path.components().count() > 1
        && let Some(parent) = py_path.parent()
    {
        prepend_dirs.push(parent.to_path_buf());
    }
    if let Ok(venv) = std::env::var("VIRTUAL_ENV") {
        let root = std::path::PathBuf::from(venv);
        for sub in ["bin", "Scripts"] {
            let dir = root.join(sub);
            if dir.is_dir() {
                prepend_dirs.push(dir);
            }
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        let dot_venv = cwd.join(".venv");
        for sub in ["bin", "Scripts"] {
            let dir = dot_venv.join(sub);
            if dir.is_dir() {
                prepend_dirs.push(dir);
            }
        }
    }
    if !prepend_dirs.is_empty() {
        let existing = std::env::var_os("PATH").unwrap_or_default();
        let mut combined = prepend_dirs;
        combined.extend(std::env::split_paths(&existing));
        if let Ok(joined) = std::env::join_paths(combined) {
            // SAFETY: invoked in `main()` before spawning the Tokio runtime or any worker threads.
            unsafe {
                std::env::set_var("PATH", joined);
            }
        }
    }
}

/// Ensure the embedded Python interpreter includes `.`, `PYTHONPATH`, and active `.venv`
/// `site-packages` directories on `sys.path`.
#[cfg(feature = "python")]
fn ensure_embedded_python_path(py: Python<'_>) -> PyResult<()> {
    let sys = py.import("sys")?;
    let sys_path = sys.getattr("path")?;
    let site = py.import("site")?;

    let add_path = |p: &str| -> PyResult<()> {
        if p.is_empty() {
            return Ok(());
        }
        let contains: bool = sys_path.call_method1("__contains__", (p,))?.extract()?;
        if !contains {
            sys_path.call_method1("insert", (0, p))?;
        }
        Ok(())
    };

    if let Ok(cwd) = std::env::current_dir()
        && let Some(cwd_str) = cwd.to_str()
    {
        add_path(cwd_str)?;
    }

    if let Ok(py_path) = std::env::var("PYTHONPATH") {
        for entry in std::env::split_paths(&py_path) {
            if let Some(s) = entry.to_str() {
                add_path(s)?;
            }
        }
    }

    let mut venv_roots = Vec::new();
    if let Ok(venv) = std::env::var("VIRTUAL_ENV") {
        venv_roots.push(std::path::PathBuf::from(venv));
    }
    if let Ok(cwd) = std::env::current_dir() {
        let dot_venv = cwd.join(".venv");
        if dot_venv.is_dir() {
            venv_roots.push(dot_venv);
        }
    }

    for root in venv_roots {
        let win_site = root.join("Lib").join("site-packages");
        if win_site.is_dir()
            && let Some(s) = win_site.to_str()
        {
            let _ = site.call_method1("addsitedir", (s,));
        }
        let lib_dir = root.join("lib");
        if let Ok(entries) = std::fs::read_dir(&lib_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                if name.to_string_lossy().starts_with("python") {
                    let site_pkgs = entry.path().join("site-packages");
                    if site_pkgs.is_dir()
                        && let Some(s) = site_pkgs.to_str()
                    {
                        let _ = site.call_method1("addsitedir", (s,));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Initialize the embedded transport and spawn 1 (GIL) or `thread_count` (3.14t/3.15t) Python worker threads.
#[cfg(feature = "python")]
fn spawn_python_workers(
    app_path: String,
    thread_count: usize,
    shutdown_token: tokio_util::sync::CancellationToken,
    worker_failed: Arc<std::sync::atomic::AtomicBool>,
) -> PyResult<EmbeddedPythonWorkers> {
    let prepared = Python::attach(|py| -> PyResult<PreparedEmbeddedApp> {
        ensure_embedded_python_path(py)?;
        let native_mod = py.import("_vertebra")?;
        let sys = py.import("sys")?;
        sys.getattr("modules")?
            .set_item("vertebra._vertebra", &native_mod)?;
        let os_mod = py.import("os")?;
        os_mod
            .getattr("environ")?
            .call_method1("setdefault", ("VERTEBRA_SERVER_ACTIVE", "1"))?;

        let parts: Vec<&str> = app_path.splitn(2, ':').collect();
        let module = py.import(parts[0])?;
        let app = module.getattr(parts[1])?;

        let app_obj = if app.getattr("handler_index").is_err() && app.is_callable() {
            let app_mod = py.import("vertebra.framework._app")?;
            let wrapped = app_mod.getattr("App")?.call0()?;
            wrapped.getattr("mount")?.call1(("/", &app))?;
            wrapped
        } else {
            app
        };

        if let Ok(ensure_fn) = app_obj.getattr("ensure_dev_welcome_route") {
            let _ = ensure_fn.call0()?;
        }

        native_mod
            .getattr("init_transport")?
            .call((), Some(&PyDict::new(py)))?;

        let handlers = app_obj.getattr("_handlers")?.unbind();
        let builtins = py.import("builtins")?;
        let mounts = builtins
            .getattr("list")?
            .call1((app_obj.getattr("mounts")?,))?
            .unbind();

        let threading = py.import("threading")?;
        let stop_event = threading.getattr("Event")?.call0()?.unbind();

        let free_threaded = native_mod
            .getattr("FREE_THREADED")
            .and_then(|v| v.extract::<bool>())
            .unwrap_or(false);
        let available_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let worker_threads = if free_threaded {
            thread_count.min(available_cpus).max(1)
        } else {
            1
        };

        if let Some(bridge) = BatchBridge::get() {
            bridge.set_consumer_count(worker_threads);
        }

        Ok(PreparedEmbeddedApp {
            handlers,
            mounts,
            stop_event,
            worker_threads,
        })
    })?;

    let mut handles = Vec::with_capacity(prepared.worker_threads);
    for idx in 0..prepared.worker_threads {
        let handlers_ref = Python::attach(|py| prepared.handlers.clone_ref(py));
        let mounts_ref = Python::attach(|py| prepared.mounts.clone_ref(py));
        let stop_ref = Python::attach(|py| prepared.stop_event.clone_ref(py));
        let worker_threads = prepared.worker_threads;
        let shutdown_token = shutdown_token.clone();
        let worker_failed = Arc::clone(&worker_failed);

        let handle = std::thread::Builder::new()
            .name(format!("vtb-py-worker-{idx}"))
            .spawn(move || {
                Python::attach(|py| {
                    let result: PyResult<()> = (|| {
                        let runner = py.import("vertebra.server.worker")?;
                        let kwargs = PyDict::new(py);
                        kwargs.set_item("mounts", mounts_ref.bind(py))?;
                        kwargs.set_item("worker_count", worker_threads)?;
                        kwargs.set_item("stop_event", stop_ref.bind(py))?;
                        runner
                            .getattr("run_worker")?
                            .call((handlers_ref.bind(py),), Some(&kwargs))?;
                        Ok(())
                    })();

                    let stop_requested = stop_ref
                        .bind(py)
                        .call_method0("is_set")
                        .and_then(|v| v.extract::<bool>())
                        .unwrap_or(false);

                    match result {
                        Err(e) => {
                            error!("Python worker thread {idx} failed: {e}");
                            e.print(py);
                            worker_failed.store(true, std::sync::atomic::Ordering::Release);
                            let _ = stop_ref.bind(py).call_method0("set");
                            shutdown_token.cancel();
                        }
                        Ok(()) if !stop_requested => {
                            error!(
                                "Python worker thread {idx} exited unexpectedly before shutdown"
                            );
                            worker_failed.store(true, std::sync::atomic::Ordering::Release);
                            let _ = stop_ref.bind(py).call_method0("set");
                            shutdown_token.cancel();
                        }
                        Ok(()) => {}
                    }
                });
            })
            .expect("failed to spawn embedded Python worker thread");
        handles.push(handle);
    }

    Ok(EmbeddedPythonWorkers {
        stop_event: prepared.stop_event,
        handles,
    })
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = signal::ctrl_c() => info!("ctrl-c received"),
                    _ = sigterm.recv() => info!("SIGTERM received"),
                }
            }
            Err(_) => {
                let _ = signal::ctrl_c().await;
                info!("ctrl-c received");
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
        info!("ctrl-c received");
    }
}

fn build_tokio_runtime(worker_threads: usize) -> std::io::Result<tokio::runtime::Runtime> {
    if worker_threads <= 1 {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()
    }
}

#[cfg(feature = "python")]
fn extract_offload_json_from_app(app_obj: &Bound<'_, PyAny>) -> Option<String> {
    let state = app_obj.getattr("state").ok()?;
    if let Ok(plugin) = state.call_method1("get", ("vertebra_plugin",))
        && !plugin.is_none()
    {
        let _ = plugin.call_method1("ensure_finalized", (app_obj,));
    }
    let val = state
        .call_method1("get", ("vertebra_offload_config_json",))
        .ok()?;
    if val.is_none() {
        return None;
    }
    val.extract::<String>().ok()
}

async fn run_server(
    #[allow(unused_mut)] mut config: ServerConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_shutdown_token = tokio_util::sync::CancellationToken::new();
    let embedded_worker_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));

    if config.topology == Topology::Embedded {
        BatchBridge::init(256, 1_048_576, 16, config.thread_count.max(1))
            .expect("failed to initialize batch bridge");
    }

    #[cfg(feature = "python")]
    let (table, app_path) = {
        let app_path = std::env::var("VERTEBRA_APP").unwrap_or_else(|_| "app:app".to_string());
        let (table, offload_json) =
            Python::attach(|py| -> PyResult<(RouteTable, Option<String>)> {
                ensure_embedded_python_path(py)?;
                let native_mod = py.import("_vertebra")?;
                let sys = py.import("sys")?;
                sys.getattr("modules")?
                    .set_item("vertebra._vertebra", &native_mod)?;
                let os_mod = py.import("os")?;
                os_mod
                    .getattr("environ")?
                    .call_method1("setdefault", ("VERTEBRA_SERVER_ACTIVE", "1"))?;

                let parts: Vec<&str> = app_path.splitn(2, ':').collect();
                if parts.len() != 2 {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "VERTEBRA_APP must be 'module:attr', got: {app_path}"
                    )));
                }
                let module = py.import(parts[0])?;
                let app_obj = module.getattr(parts[1])?;
                let extracted_offload = extract_offload_json_from_app(&app_obj);
                let route_table = build_route_table_from_app(py, &app_obj)?;
                Ok((route_table, extracted_offload))
            })?;
        if std::env::var_os("VERTEBRA_OFFLOAD_CONFIG").is_none()
            && let Some(raw_json) = offload_json
            && let Err(e) = config.apply_offload_json(&raw_json)
        {
            error!("failed to apply extracted Litestar offload config: {e}");
        }
        (table, app_path)
    };

    let config = Arc::new(config);

    #[cfg(not(feature = "python"))]
    let table = {
        let mut t = RouteTable::new();
        t.insert("GET", "/hello");
        t.insert("POST", "/echo");
        t
    };

    let router = Arc::new(Router::from_entries(table.into_entries())?);
    info!("route table built");

    #[cfg(all(feature = "python", unix))]
    let mut shm_factory: Option<Arc<std::sync::Mutex<vertebra_http::workers::RingFactory>>> = None;
    #[cfg(feature = "python")]
    let mut embedded_workers: Option<EmbeddedPythonWorkers> = None;

    let dispatcher;

    #[cfg(feature = "python")]
    {
        if config.topology == Topology::Shm {
            #[cfg(unix)]
            {
                use std::sync::Mutex;
                use vertebra_core::transport::ring_pair::RingPairConfig;
                use vertebra_http::dispatcher::{ShmDispatcher, shm_reader};
                use vertebra_http::workers::{RingFactory, Supervisor};

                let worker_count = config.workers.count.max(1);
                let mut factory = RingFactory::with_broker_config(
                    worker_count,
                    RingPairConfig::for_environment(worker_count),
                    config.workers.broker_pool_size,
                    config.workers.broker_max_overflow,
                )
                .expect("failed to create ring factory");

                info!(
                    workers = worker_count,
                    tokio_threads = config.worker_threads,
                    "spawning multi-process workers with SHM transport"
                );

                let ring_pairs = factory.all_ring_pairs().to_vec();
                let shm = ShmDispatcher::new(ring_pairs.clone(), 65536);
                dispatcher = Arc::new(
                    Dispatcher::from_shm(Arc::clone(&shm))
                        .with_backpressure(config.backpressure.clone()),
                );

                shm.spawn_sweeper(server_shutdown_token.clone());

                shm_reader::spawn_response_readers(
                    ring_pairs,
                    Arc::clone(&dispatcher),
                    server_shutdown_token.clone(),
                );

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
                BatchBridge::init(256, 1_048_576, 16, config.thread_count.max(1))
                    .expect("failed to initialize batch bridge");
                dispatcher =
                    Arc::new(Dispatcher::new().with_backpressure(config.backpressure.clone()));
                dispatcher.spawn_bridge_sweeper(server_shutdown_token.clone());
                embedded_workers = Some(spawn_python_workers(
                    app_path.clone(),
                    config.thread_count.max(1),
                    server_shutdown_token.clone(),
                    Arc::clone(&embedded_worker_failed),
                )?);
            }
        } else {
            dispatcher = Arc::new(Dispatcher::new().with_backpressure(config.backpressure.clone()));
            dispatcher.spawn_bridge_sweeper(server_shutdown_token.clone());
            embedded_workers = Some(spawn_python_workers(
                app_path.clone(),
                config.thread_count.max(1),
                server_shutdown_token.clone(),
                Arc::clone(&embedded_worker_failed),
            )?);
        }
        info!(
            app = %app_path,
            bind = %config.bind,
            topology = ?config.topology,
            tokio_threads = config.worker_threads,
            reuse_port = config.reuse_port,
            "vertebra-server starting"
        );
    }

    #[cfg(not(feature = "python"))]
    {
        if config.topology != Topology::Embedded {
            BatchBridge::init(256, 1_048_576, 16, config.thread_count.max(1))
                .expect("failed to initialize batch bridge");
        }
        dispatcher = Arc::new(Dispatcher::new().with_backpressure(config.backpressure.clone()));
        spawn_response_reader(Arc::clone(&dispatcher));
        info!(bind = %config.bind, "vertebra-server starting (no python)");
    }

    let shard_count = if config.topology == Topology::Shm {
        config.workers.count.clamp(1, 8)
    } else {
        config.worker_threads.clamp(1, 8)
    };
    let collector_shards = RequestBatcher::spawn_pool(
        Arc::clone(&dispatcher),
        BatchLimit::default(),
        shard_count,
        16384,
    );

    let shutdown_token_for_loop = server_shutdown_token.clone();
    server::run_with_collector(
        Arc::clone(&config),
        router,
        dispatcher,
        collector_shards,
        async move {
            tokio::select! {
                _ = wait_for_shutdown_signal() => {}
                _ = shutdown_token_for_loop.cancelled() => {
                    error!("stopping accept loop due to worker failure");
                }
            }
        },
    )
    .await?;

    server_shutdown_token.cancel();

    #[cfg(feature = "python")]
    if let Some(pool) = embedded_workers {
        pool.shutdown();
    }

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

    if embedded_worker_failed.load(std::sync::atomic::Ordering::Acquire) {
        return Err("embedded Python worker thread terminated unexpectedly".into());
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    #[cfg(unix)]
    let is_embedded_child = std::env::var_os("VERTEBRA_EMBEDDED_WORKER").is_some();

    #[cfg(unix)]
    if !is_embedded_child {
        vertebra_http::workers::cleanup_stale_shm();
    }
    vertebra_http::workers::install_panic_cleanup_hook();

    let config = ServerConfig::from_env();

    #[cfg(all(feature = "python", unix))]
    if config.topology == Topology::Embedded && config.workers.count > 1 && !is_embedded_child {
        let rt = build_tokio_runtime(1)?;
        return rt.block_on(async move {
            let mut supervisor = vertebra_http::workers::EmbeddedSupervisor::new(
                config.workers.count,
                config.workers.broker_pool_size,
                config.workers.broker_max_overflow,
                config.workers.broker_stmt_capacity,
            )?;
            supervisor.spawn_all()?;
            supervisor.run(wait_for_shutdown_signal()).await;
            Ok(())
        });
    }

    #[cfg(feature = "python")]
    {
        prepare_embedded_python_env(&config.workers.python_bin);
        use _vertebra::pymodule_init;
        append_to_inittab!(pymodule_init);
    }

    let rt = build_tokio_runtime(config.worker_threads)?;
    rt.block_on(run_server(config))
}
