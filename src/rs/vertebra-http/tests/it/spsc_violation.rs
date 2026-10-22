use http::uri::PathAndQuery;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::oneshot;
use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};
use vertebra_http::batcher::{Batch, PendingRequest};
use vertebra_http::dispatcher::Dispatcher;

#[test]
fn test_concurrent_dispatch_to_single_worker() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    let worker_count = 1;
    let mut pairs = Vec::new();
    for _ in 0..worker_count {
        pairs.push(Arc::new(
            RingPair::create(RingPairConfig::default()).unwrap(),
        ));
    }

    let dispatcher = Arc::new(Dispatcher::new_shm(pairs.clone(), 65536));

    let thread_count = 4;
    let iterations = 1000;
    let total_expected = iterations * thread_count;

    // Start consumer thread
    let ring_clone = Arc::clone(&pairs[0].request_ring);
    let consumer_handle = thread::spawn(move || {
        let mut count = 0;
        let mut buf = vec![0u8; ring_clone.payload_capacity()];
        let start = std::time::Instant::now();
        while count < total_expected && start.elapsed() < Duration::from_secs(10) {
            match ring_clone.try_recv(&mut buf) {
                Ok(_) => count += 1,
                Err(_) => thread::yield_now(),
            }
        }
        count
    });

    let mut handles = Vec::new();
    for t in 0..thread_count {
        let dispatcher = Arc::clone(&dispatcher);
        handles.push(thread::spawn(move || {
            let start = std::time::Instant::now();
            for i in 0..iterations {
                let mut success = false;
                while !success {
                    assert!(
                        start.elapsed() < Duration::from_secs(10),
                        "producer thread {t} timed out at iteration {i}"
                    );
                    let (tx, _rx) = oneshot::channel();
                    let req = PendingRequest {
                        handler_id: (t * 10000 + i) as u64,
                        method_tag: 0,
                        http_version_tag: 1,
                        flags: 0,
                        path_and_query: PathAndQuery::from_static("/hello"),
                        headers_mp: bytes::Bytes::from_static(b"host: localhost\r\n"),
                        body: bytes::Bytes::new(),
                        reply: tx,
                    };
                    let batch = Batch {
                        requests: vec![req],
                    };

                    if dispatcher.dispatch(batch) > 0 {
                        success = true;
                    } else {
                        thread::yield_now();
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let final_count = consumer_handle.join().unwrap();
    println!("Received count: {}", final_count);
    assert_eq!(final_count, total_expected);
}
