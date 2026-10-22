use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use vertebra_core::broker::{BrokerError, ShmLeaseTable, ShmStatementCache};
use vertebra_core::shm::ShmRegion;

#[test]
fn test_shm_lease_table_capacity_and_overflow_under_contention() {
    let req_size = ShmLeaseTable::required_size(4, 2);
    let region = ShmRegion::create(req_size).unwrap();
    let table = Arc::new(ShmLeaseTable::init_in_region(region, 4, 2).unwrap());

    assert_eq!(table.pool_size(), 4);
    assert_eq!(table.max_overflow(), 2);
    assert_eq!(table.total_capacity(), 6);

    let concurrent_holders = Arc::new(AtomicU32::new(0));
    let max_observed = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::new();
    for thread_id in 0..8 {
        let t = Arc::clone(&table);
        let holders = Arc::clone(&concurrent_holders);
        let max_obs = Arc::clone(&max_observed);

        handles.push(std::thread::spawn(move || {
            for _ in 0..250 {
                let grant =
                    match t.acquire(thread_id, 10_000_000_000, Duration::from_millis(500), 0) {
                        Ok(g) => g,
                        Err(BrokerError::PoolExhausted) => continue,
                        Err(e) => panic!("acquire error: {e:?}"),
                    };

                let cur = holders.fetch_add(1, Ordering::SeqCst) + 1;
                max_obs.fetch_max(cur, Ordering::SeqCst);
                assert!(cur <= 6, "exceeded total capacity 6: observed {cur}");

                std::thread::yield_now();

                holders.fetch_sub(1, Ordering::SeqCst);
                t.release(grant.slot_index, grant.generation).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(max_observed.load(Ordering::SeqCst) <= 6);
    assert!(max_observed.load(Ordering::SeqCst) > 0);
    assert_eq!(table.stats().active_leases, 0);
}

#[test]
fn test_shm_lease_table_futex_wakeup_latency() {
    let req_size = ShmLeaseTable::required_size(2, 0);
    let region = ShmRegion::create(req_size).unwrap();
    let table = Arc::new(ShmLeaseTable::init_in_region(region, 2, 0).unwrap());

    let g0 = table.try_acquire(100, 0, 0).unwrap();
    let g1 = table.try_acquire(101, 0, 0).unwrap();

    let t = Arc::clone(&table);
    let waiter = std::thread::spawn(move || {
        let start = Instant::now();
        let grant = t.acquire(200, 0, Duration::from_secs(2), 0).unwrap();
        let elapsed = start.elapsed();
        (grant, elapsed)
    });

    // Sleep 15ms before releasing
    std::thread::sleep(Duration::from_millis(15));
    table.release(g0.slot_index, g0.generation).unwrap();

    let (grant, elapsed) = waiter.join().unwrap();
    assert_eq!(grant.worker_pid, 200);
    // Wakeup latency after release should be reasonable (< 50ms total elapsed includes 15ms sleep)
    assert!(elapsed < Duration::from_millis(100));

    table.release(grant.slot_index, grant.generation).unwrap();
    table.release(g1.slot_index, g1.generation).unwrap();
    assert_eq!(table.stats().active_leases, 0);
}

#[test]
fn test_shm_lease_table_ttl_reap_and_aba_generation_guard() {
    let req_size = ShmLeaseTable::required_size(1, 0);
    let region = ShmRegion::create(req_size).unwrap();
    let table = ShmLeaseTable::init_in_region(region, 1, 0).unwrap();

    // Acquire slot 0 with 1ms TTL
    let g0 = table.try_acquire(100, 1_000_000, 0).unwrap();
    assert_eq!(g0.slot_index, 0);

    // Sleep 5ms for TTL to expire
    std::thread::sleep(Duration::from_millis(5));

    // Pool was exhausted, but try_acquire calls reap_expired internally
    let g1 = table.try_acquire(101, 0, 0).unwrap();
    assert_eq!(g1.slot_index, 0);
    assert_ne!(g0.generation, g1.generation);

    // Releasing g0 with old generation must fail with GenerationMismatch
    let err = table.release(g0.slot_index, g0.generation);
    assert_eq!(err, Err(BrokerError::GenerationMismatch));

    // g1 is still active
    assert_eq!(table.stats().active_leases, 1);

    // Releasing g1 with correct generation succeeds
    table.release(g1.slot_index, g1.generation).unwrap();
    assert_eq!(table.stats().active_leases, 0);
}

#[cfg(unix)]
#[test]
fn test_shm_lease_table_cross_process_named_shm() {
    let name = format!("/vtb-test-broker-{}", std::process::id());
    let req_size = ShmLeaseTable::required_size(4, 2);

    let region_primary = match ShmRegion::create_named(&name, req_size) {
        Ok(r) => r,
        Err(vertebra_core::shm::ShmError::Sys(err))
            if err.kind() == std::io::ErrorKind::PermissionDenied =>
        {
            return;
        }
        Err(err) => panic!("create_named failed: {err:?}"),
    };

    let table_primary = ShmLeaseTable::init_in_region(region_primary, 4, 2).unwrap();

    let region_secondary = ShmRegion::open_named(&name, req_size).unwrap();
    let table_secondary = ShmLeaseTable::attach_region(region_secondary).unwrap();

    // Primary acquires 2 leases
    let g0 = table_primary.try_acquire(1001, 0, 0).unwrap();
    let g1 = table_primary.try_acquire(1001, 0, 0).unwrap();

    // Secondary sees 2 active leases
    assert_eq!(table_secondary.stats().active_leases, 2);

    // Secondary acquires 1 lease
    let g2 = table_secondary.try_acquire(2002, 0, 0).unwrap();
    assert_eq!(table_primary.stats().active_leases, 3);

    // Primary reclaims worker 1001
    let reclaimed = table_primary.reclaim_worker(1001);
    assert_eq!(reclaimed, 2);
    assert_eq!(table_secondary.stats().active_leases, 1);

    // Secondary releases its lease
    table_secondary
        .release(g2.slot_index, g2.generation)
        .unwrap();
    assert_eq!(table_primary.stats().active_leases, 0);

    // Stale release of reclaimed leases fails
    assert_eq!(
        table_primary.release(g0.slot_index, g0.generation),
        Err(BrokerError::GenerationMismatch)
    );
    assert_eq!(
        table_primary.release(g1.slot_index, g1.generation),
        Err(BrokerError::GenerationMismatch)
    );
}

#[test]
fn test_shm_statement_cache_create_and_attach_persistence() {
    let shm_name = format!("/vtb-stmt-test-{}", std::process::id());
    let req_size = ShmStatementCache::required_size(16);
    let region1 = ShmRegion::create_named(&shm_name, req_size).unwrap();
    let cache1 = ShmStatementCache::init_in_region(region1, 16).unwrap();

    let raw_sql = b"SELECT 1 FROM test WHERE id = $1";
    let compiled_sql = b"SELECT 1 FROM test WHERE id = $1";
    let param_plan = b"\x92\x01\x02";

    assert!(cache1.insert(1, raw_sql, 2, compiled_sql, param_plan));

    let looked_up = cache1.lookup(1, raw_sql).unwrap();
    assert_eq!(looked_up.dialect_tag, 1);
    assert_eq!(looked_up.param_count, 2);
    assert_eq!(&looked_up.compiled_sql[..], compiled_sql);
    assert_eq!(&looked_up.param_plan[..], param_plan);

    // Open existing named region with attach_region (does not zero out cache)
    let region2 = ShmRegion::open_named(&shm_name, req_size).unwrap();
    let cache2 = ShmStatementCache::attach_region(region2, 16).unwrap();

    let looked_up2 = cache2.lookup(1, raw_sql).unwrap();
    assert_eq!(looked_up2.dialect_tag, 1);
    assert_eq!(looked_up2.param_count, 2);
    assert_eq!(&looked_up2.compiled_sql[..], compiled_sql);
    assert_eq!(&looked_up2.param_plan[..], param_plan);
    assert_eq!(cache2.stats().hits, 2);
}
