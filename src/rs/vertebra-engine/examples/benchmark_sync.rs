use vertebra_engine::embedded::{init_python, EmbeddedWorker};
use pyo3::prelude::*;
use std::time::Instant;
use pyo3::types::{IntoPyDict, PyDict};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize without background loop for raw sync test
    init_python(&[])?;

    let app_code = c"
async def app(scope, receive, send):
    pass

def sync_wrapper(scope):
    # Just validate scope access to simulate work
    assert scope['type'] == 'http'
";

    let (worker, wrapper) = Python::with_gil(|py| -> PyResult<(EmbeddedWorker, Py<PyAny>)> {
         let module = PyModule::from_code(py, app_code, c"app.py", c"app")?;
         let app = module.getattr("app")?;
         let wrapper = module.getattr("sync_wrapper")?;
         Ok((EmbeddedWorker::new(app), wrapper.unbind()))
    })?;

    println!("Starting raw PyO3 sync benchmark...");
    
    let iterations = 1_000_000;
    let start = Instant::now();
    
    Python::with_gil(|py| {
        let wrapper = wrapper.bind(py);
        let scope = PyDict::new(py);
        scope.set_item("type", "http").unwrap();
        
        for _ in 0..iterations {
            wrapper.call1((scope.clone(),)).unwrap();
        }
    });
    
    let duration = start.elapsed();
    let rps = iterations as f64 / duration.as_secs_f64();
    
    println!("Completed {} iterations in {:?}", iterations, duration);
    println!("Raw Sync Call RPS: {:.2}", rps);
    
    // Now benchmark the async bridge overhead (without actually running loop, just the creation)
    // This estimates the cost of 'call1' returning a coroutine object.
    
    println!("Starting raw coroutine creation benchmark...");
    let start_async = Instant::now();
    
    Python::with_gil(|py| {
        let scope = PyDict::new(py);
        scope.set_item("type", "http").unwrap();
        // Mock receive/send
        let none = py.None();
        
        for _ in 0..iterations {
            // We call execute, but since we don't have a background loop initialized in this specific
            // test setup (we skipped it in init_python if we wanted, but init_python enables it),
            // let's just call the underlying app directly to get the coroutine.
            let app = worker.get_app(py);
            let _coro = app.call1((scope.clone(), &none, &none)).unwrap();
        }
    });
    
    let duration_async = start_async.elapsed();
    let rps_async = iterations as f64 / duration_async.as_secs_f64();
    
    println!("Completed {} iterations in {:?}", iterations, duration_async);
    println!("Coroutine Creation RPS: {:.2}", rps_async);

    Ok(())
}
