use std::sync::Arc;
use std::time::Duration;

use vertebra_http::backpressure::{
    BackpressureConfig, BackpressureController, CircuitBreakerConfig, CircuitState, RejectionReason,
};

fn test_config() -> BackpressureConfig {
    BackpressureConfig {
        max_in_flight: 2,
        max_queue_depth: 0,
        max_queue_time: Duration::from_millis(25),
        max_active_streams: 4,
        high_watermark: 4,
        low_watermark: 2,
        priority_routes: vec!["/healthz".to_string(), "/readyz".to_string()],
        circuit_breaker: CircuitBreakerConfig {
            failure_rate_threshold: 0.5,
            min_samples: 2,
            window: Duration::from_millis(80),
            half_open_probe_count: 2,
        },
    }
}

#[tokio::test]
async fn active_streams_are_isolated_from_unary_in_flight_permits() {
    let controller = BackpressureController::new(test_config());

    let _s1 = controller.acquire_stream().expect("stream 1");
    let _s2 = controller.acquire_stream().expect("stream 2");
    let _s3 = controller.acquire_stream().expect("stream 3");
    let _s4 = controller.acquire_stream().expect("stream 4");
    assert_eq!(
        controller.acquire_stream().unwrap_err(),
        RejectionReason::StreamLimitExceeded
    );

    assert_eq!(controller.snapshot().active_streams, 4);
    assert_eq!(controller.snapshot().in_flight, 0);

    let _u1 = controller
        .try_acquire_for_path("/api/items")
        .await
        .expect("unary 1");
    let _u2 = controller
        .try_acquire_for_path("/api/items")
        .await
        .expect("unary 2");
    assert_eq!(controller.snapshot().in_flight, 2);
}

#[tokio::test]
async fn priority_routes_bypass_unary_saturation() {
    let controller = BackpressureController::new(test_config());
    let _u1 = controller.try_acquire_for_path("/api/a").await.unwrap();
    let _u2 = controller.try_acquire_for_path("/api/b").await.unwrap();

    let normal_err = controller
        .try_acquire_for_path("/api/c")
        .await
        .expect_err("saturated");
    assert_eq!(normal_err, RejectionReason::Saturated);

    let health_permit = controller
        .try_acquire_for_path("/healthz")
        .await
        .expect("priority route bypasses saturation");
    drop(health_permit);
}

#[tokio::test]
async fn watermark_pauses_at_high_and_notifies_on_low_watermark_drain() {
    let controller = Arc::new(BackpressureController::new(test_config()));

    for _ in 0..3 {
        assert!(!controller.on_stream_chunk_enqueued());
    }
    assert!(controller.on_stream_chunk_enqueued());
    assert!(controller.is_paused());

    let c_wait = Arc::clone(&controller);
    let waiter = tokio::spawn(async move {
        c_wait.wait_until_resumed().await;
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(controller.on_stream_chunk_dequeued());
    assert!(controller.is_paused());

    assert!(!controller.on_stream_chunk_dequeued());
    assert!(!controller.is_paused());

    tokio::time::timeout(Duration::from_millis(100), waiter)
        .await
        .expect("resumed notification fired")
        .expect("task joined");
}

#[tokio::test]
async fn half_open_circuit_strictly_bounds_probe_permits() {
    let controller = BackpressureController::new(test_config());

    controller.record_failure();
    controller.record_failure();
    assert_eq!(controller.snapshot().circuit_state, CircuitState::Open);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let _p1 = controller
        .try_acquire_for_path("/api/probe1")
        .await
        .expect("probe 1 admitted");
    let _p2 = controller
        .try_acquire_for_path("/api/probe2")
        .await
        .expect("probe 2 admitted");

    let extra = controller.try_acquire_for_path("/api/probe3").await;
    assert_eq!(extra.unwrap_err(), RejectionReason::CircuitOpen);

    controller.record_success();
    controller.record_success();
    assert_eq!(controller.snapshot().circuit_state, CircuitState::Closed);
}
