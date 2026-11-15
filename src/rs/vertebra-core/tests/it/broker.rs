use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use vertebra_core::broker::{
    BrokerError, PoolPartitionSpec, ShmLeaseTable, ShmStatementCache, stable_hash64,
};
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
        let grant = t.acquire(200, 0, Duration::from_secs(5), 0).unwrap();
        let elapsed = start.elapsed();
        (grant, elapsed)
    });

    // Synchronize until the waiter thread has registered on the full pool
    let poll_start = Instant::now();
    while table.stats().waiting_threads == 0 {
        if poll_start.elapsed() > Duration::from_secs(5) {
            panic!("timed out waiting for waiter thread to register in waiters count");
        }
        std::thread::yield_now();
    }

    let release_time = Instant::now();
    table.release(g0.slot_index, g0.generation).unwrap();

    let (grant, elapsed) = waiter.join().unwrap();
    assert_eq!(grant.worker_pid, 200);
    let wake_latency = release_time.elapsed();
    // Wakeup latency after release should be prompt (< 750ms allows for CI virtualization jitter while ensuring futex wake)
    assert!(
        wake_latency < Duration::from_millis(750),
        "wake latency exceeded bound: {wake_latency:?} (total elapsed: {elapsed:?})"
    );

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

#[cfg(unix)]
#[test]
fn test_shm_lease_table_colocated_statement_cache_and_channel_ring_across_named_shm() {
    let name = format!("/vtb-colocated-it-{}", std::process::id());
    let (primary, primary_stmt) = match ShmLeaseTable::create_with_statements(&name, 4, 2, 16) {
        Ok(pair) => pair,
        Err(_) => return,
    };
    let (secondary, secondary_stmt) = ShmLeaseTable::open_with_statements(&name).unwrap();

    assert_eq!(primary.stmt_capacity(), 16);
    assert_eq!(secondary.stmt_capacity(), 16);
    assert_eq!(primary.stmt_offset(), secondary.stmt_offset());

    assert!(primary_stmt.insert(
        1,
        b"SELECT * FROM accounts WHERE id = :id",
        1,
        b"SELECT * FROM accounts WHERE id = $1",
        b"\x91\xa2id",
    ));
    let plan = secondary_stmt
        .lookup(1, b"SELECT * FROM accounts WHERE id = :id")
        .expect("secondary lookup");
    assert_eq!(plan.param_count, 1);
    assert_eq!(
        &plan.compiled_sql[..],
        b"SELECT * FROM accounts WHERE id = $1"
    );

    let g0 = primary.try_acquire(9001, 60_000_000_000, 123).unwrap();
    assert_eq!(
        secondary.active_leases_for_worker(9001),
        vec![(123, g0.slot_index, g0.generation)]
    );

    assert!(primary.publish_channel(b"events", b"payload-alpha", 123_456, Some(b"0:1")));
    let drained = secondary.drain_channel(b"events");
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].channel, b"events");
    assert_eq!(drained[0].payload, b"payload-alpha");
    assert_eq!(drained[0].timestamp_ns, 123_456);
    assert_eq!(drained[0].lease_id.as_deref(), Some(&b"0:1"[..]));
    assert!(primary.drain_channel(b"events").is_empty());

    assert_eq!(vertebra_core::broker::DIALECT_TAG_POSTGRES, 1);
    assert_eq!(vertebra_core::broker::DIALECT_TAG_MYSQL, 2);
    assert_eq!(vertebra_core::broker::DIALECT_TAG_SQLITE, 3);
    assert_eq!(vertebra_core::broker::DIALECT_TAG_ORACLE, 4);
    assert_eq!(vertebra_core::broker::DIALECT_TAG_MSSQL, 5);
    assert_eq!(vertebra_core::broker::DIALECT_TAG_DUCKDB, 6);
    assert_eq!(vertebra_core::broker::DIALECT_TAG_BIGQUERY, 7);
    assert_eq!(vertebra_core::broker::DIALECT_TAG_SPANNER, 8);
    assert_eq!(vertebra_core::broker::DIALECT_TAG_SPANGRES, 9);
}

#[test]
fn test_stable_hash64_and_partition_admission_bounds() {
    use vertebra_core::broker::{MAX_POOL_PARTITIONS, PoolPartitionSpec, stable_hash64};

    let empty = stable_hash64(b"");
    assert_eq!(empty, stable_hash64(b""));
    assert_ne!(empty, stable_hash64(b"primary"));
    assert_eq!(
        ShmStatementCache::compute_sql_hash(1, b"SELECT 1"),
        stable_hash64(b"\x01SELECT 1")
    );

    let req_size = ShmLeaseTable::required_size(8, 2);
    let region = ShmRegion::create(req_size).unwrap();
    let table = ShmLeaseTable::init_in_region(region, 8, 2).unwrap();

    let h_a = stable_hash64(b"part_a");
    let h_b = stable_hash64(b"part_b");
    let p_a = table
        .register_partition(PoolPartitionSpec {
            pool_key_hash: h_a,
            pool_size: 1,
            max_overflow: 0,
        })
        .unwrap();
    assert_eq!(
        table
            .register_partition(PoolPartitionSpec {
                pool_key_hash: h_a,
                pool_size: 1,
                max_overflow: 0,
            })
            .unwrap(),
        p_a
    );
    let p_b = table
        .register_partition(PoolPartitionSpec {
            pool_key_hash: h_b,
            pool_size: 1,
            max_overflow: 0,
        })
        .unwrap();
    assert_ne!(p_a, p_b);

    let g_a = table.try_acquire_in(p_a, 1001, 0, h_a).unwrap().unwrap();
    assert!(table.try_acquire_in(p_a, 1002, 0, h_a).unwrap().is_none());
    let g_b = table.try_acquire_in(p_b, 1003, 0, h_b).unwrap().unwrap();

    table.release(g_a.slot_index, g_a.generation).unwrap();
    table.release(g_b.slot_index, g_b.generation).unwrap();

    for idx in 2..MAX_POOL_PARTITIONS {
        let key = format!("extra_part_{idx}");
        table
            .register_partition(PoolPartitionSpec {
                pool_key_hash: stable_hash64(key.as_bytes()),
                pool_size: 1,
                max_overflow: 0,
            })
            .unwrap();
    }
    assert_eq!(
        table.register_partition(PoolPartitionSpec {
            pool_key_hash: stable_hash64(b"overflow_65th"),
            pool_size: 1,
            max_overflow: 0,
        }),
        Err(BrokerError::PartitionsExhausted)
    );
}

#[test]
fn test_heartbeat_extends_lease_and_rejects_stale_generation() {
    let req_size = ShmLeaseTable::required_size(2, 0);
    let region = ShmRegion::create(req_size).unwrap();
    let table = ShmLeaseTable::init_in_region(region, 2, 0).unwrap();

    let g = table.try_acquire(2001, 30_000_000, 0).unwrap();
    std::thread::sleep(Duration::from_millis(18));
    table.heartbeat(g.slot_index, g.generation).unwrap();
    std::thread::sleep(Duration::from_millis(18));
    assert_eq!(
        table.reap_expired(vertebra_core::broker::monotonic_now_ns()),
        0
    );

    std::thread::sleep(Duration::from_millis(38));
    assert_eq!(
        table.reap_expired(vertebra_core::broker::monotonic_now_ns()),
        1
    );
    assert_eq!(
        table.heartbeat(g.slot_index, g.generation),
        Err(BrokerError::StaleGeneration)
    );
}

#[test]
fn test_quarantine_blocks_slot_until_dispose_and_reclaim_worker_leases_isolates_pids() {
    let req_size = ShmLeaseTable::required_size(2, 0);
    let region = ShmRegion::create(req_size).unwrap();
    let table = ShmLeaseTable::init_in_region(region, 2, 0).unwrap();

    let h = stable_hash64(b"pool-quarantine-test");
    let part = table
        .register_partition(PoolPartitionSpec {
            pool_key_hash: h,
            pool_size: 1,
            max_overflow: 0,
        })
        .unwrap();

    let dead_grant = table.try_acquire_in(part, 7001, 0, h).unwrap().unwrap();
    let live_grant = table.try_acquire(7002, 0, 0).unwrap();

    // Quarantining the dead_grant slot keeps partition capacity (1) reserved.
    table
        .quarantine(dead_grant.slot_index, dead_grant.generation)
        .unwrap();
    assert_eq!(table.quarantined_count(), 1);
    assert_eq!(table.stats().quarantined, 1);
    assert_eq!(table.stats().total_quarantined, 1);
    assert!(table.try_acquire_in(part, 7003, 0, h).unwrap().is_none());

    // Wrong generation fails with GenerationMismatch; disposing Free/Leased fails with InvalidState.
    assert_eq!(
        table.quarantine(live_grant.slot_index, live_grant.generation + 9),
        Err(BrokerError::GenerationMismatch)
    );
    assert_eq!(
        table.dispose_quarantined(live_grant.slot_index, live_grant.generation),
        Err(BrokerError::InvalidState)
    );

    // Explicit dispose returns the slot to Free so a new acquisition succeeds.
    table
        .dispose_quarantined(dead_grant.slot_index, dead_grant.generation)
        .unwrap();
    assert_eq!(table.quarantined_count(), 0);
    assert_eq!(table.stats().total_disposed, 1);
    let reused = table.try_acquire_in(part, 7003, 0, h).unwrap().unwrap();

    // Reclaiming pid 7003 isolates its leased slots into Quarantined without touching live pid 7002.
    let reclaimed = table.reclaim_worker_leases(7003);
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].slot_index, reused.slot_index);
    assert_eq!(reclaimed[0].generation, reused.generation + 1);
    assert_eq!(reclaimed[0].pool_id, Some(part));
    assert_eq!(table.quarantined_count(), 1);
    assert!(table.try_acquire_in(part, 7004, 0, h).unwrap().is_none());

    // Reclaim completes cleanup via dispose_quarantined after rollback, while live_grant stays intact.
    table
        .dispose_quarantined(reclaimed[0].slot_index, reclaimed[0].generation)
        .unwrap();
    assert_eq!(table.quarantined_count(), 0);
    table
        .release(live_grant.slot_index, live_grant.generation)
        .unwrap();
    assert_eq!(table.stats().active_leases, 0);
}

#[test]
fn test_lookup_partition_partition_stats_and_unpartitioned_reclaim() {
    let req_size = ShmLeaseTable::required_size(4, 0);
    let region = ShmRegion::create(req_size).unwrap();
    let table = ShmLeaseTable::init_in_region(region, 4, 0).unwrap();

    let h_a = stable_hash64(b"partition-a");
    let h_missing = stable_hash64(b"partition-missing");
    assert_eq!(table.lookup_partition(h_a), None);
    assert_eq!(table.partition_stats(h_a), None);

    let part_a = table
        .register_partition(PoolPartitionSpec {
            pool_key_hash: h_a,
            pool_size: 2,
            max_overflow: 0,
        })
        .unwrap();
    assert_eq!(table.lookup_partition(h_a), Some(part_a));
    assert_eq!(table.lookup_partition(h_missing), None);

    let g1 = table.try_acquire_in(part_a, 8001, 0, h_a).unwrap().unwrap();
    table.release(g1.slot_index, g1.generation).unwrap();
    let g2 = table.try_acquire_in(part_a, 8002, 0, h_a).unwrap().unwrap();
    let unpart_small = table.try_acquire(8003, 0, 42).unwrap();
    let unpart_large = table.try_acquire(8003, 0, 123_456_789).unwrap();

    let stats_mid = table
        .partition_stats(h_a)
        .expect("registered partition stats");
    assert_eq!(stats_mid.pool_id, part_a);
    assert_eq!(stats_mid.active_leases, 1);
    assert_eq!(stats_mid.total_acquired, 2);
    assert_eq!(stats_mid.total_released, 1);

    let reclaimed_a = table.reclaim_worker_leases(8002);
    assert_eq!(reclaimed_a.len(), 1);
    assert_eq!(reclaimed_a[0].pool_id, Some(part_a));
    let stats_q = table.partition_stats(h_a).unwrap();
    assert_eq!(stats_q.active_leases, 0);
    assert_eq!(stats_q.quarantined, 1);
    assert_eq!(stats_q.total_quarantined, 1);
    assert_eq!(stats_q.total_reclaimed, 1);

    table
        .dispose_quarantined(reclaimed_a[0].slot_index, reclaimed_a[0].generation)
        .unwrap();
    let stats_end = table.partition_stats(h_a).unwrap();
    assert_eq!(stats_end.active_leases, 0);
    assert_eq!(stats_end.quarantined, 0);
    assert_eq!(stats_end.total_disposed, 1);

    let reclaimed_unpart = table.reclaim_worker_leases(8003);
    assert_eq!(reclaimed_unpart.len(), 2);
    assert!(reclaimed_unpart.iter().all(|item| item.pool_id.is_none()));
    assert_eq!(g2.worker_pid, 8002);
    assert_eq!(unpart_small.worker_pid, 8003);
    assert_eq!(unpart_large.worker_pid, 8003);
    for item in reclaimed_unpart {
        table
            .dispose_quarantined(item.slot_index, item.generation)
            .unwrap();
    }
}

#[test]
fn test_shm_lease_table_direct_quarantine_partition_stats() {
    let req_size = ShmLeaseTable::required_size(2, 0);
    let region = ShmRegion::create(req_size).unwrap();
    let table = ShmLeaseTable::init_in_region(region, 2, 0).unwrap();

    let key_hash = 0xCAFE_BABE_0001_0002;
    let pool_id = table
        .register_partition(PoolPartitionSpec {
            pool_key_hash: key_hash,
            pool_size: 2,
            max_overflow: 0,
        })
        .unwrap();

    let g1 = table
        .try_acquire_in(pool_id, 9101, 0, key_hash)
        .unwrap()
        .unwrap();
    table.quarantine(g1.slot_index, g1.generation).unwrap();

    let during = table.partition_stats(key_hash).unwrap();
    assert_eq!(during.active_leases, 0);
    assert_eq!(during.quarantined, 1);
    assert_eq!(during.total_acquired, 1);
    assert_eq!(during.total_quarantined, 1);
    assert_eq!(during.total_disposed, 0);

    table
        .dispose_quarantined(g1.slot_index, g1.generation)
        .unwrap();
    let after = table.partition_stats(key_hash).unwrap();
    assert_eq!(after.active_leases, 0);
    assert_eq!(after.quarantined, 0);
    assert_eq!(after.total_disposed, 1);
}

#[test]
fn test_shm_lease_table_aba_cross_partition_release_and_partition_wakeup() {
    let req_size = ShmLeaseTable::required_size(2, 0);
    let region = ShmRegion::create(req_size).unwrap();
    let table = Arc::new(ShmLeaseTable::init_in_region(region, 2, 0).unwrap());

    let key_a = 0x1111_2222_3333_4444;
    let key_b = 0x5555_6666_7777_8888;
    let part_a = table
        .register_partition(PoolPartitionSpec {
            pool_key_hash: key_a,
            pool_size: 1,
            max_overflow: 0,
        })
        .unwrap();
    let part_b = table
        .register_partition(PoolPartitionSpec {
            pool_key_hash: key_b,
            pool_size: 1,
            max_overflow: 0,
        })
        .unwrap();

    // Acquire and release slot in partition A, then same worker PID acquires slot in partition B.
    let g_a1 = table
        .try_acquire_in(part_a, 7777, 0, key_a)
        .unwrap()
        .unwrap();
    table.release(g_a1.slot_index, g_a1.generation).unwrap();

    let g_b = table
        .try_acquire_in(part_b, 7777, 0, key_b)
        .unwrap()
        .unwrap();
    if g_b.slot_index == g_a1.slot_index {
        // Stale release with old generation must fail without touching partition B's active count.
        assert_eq!(
            table.release(g_a1.slot_index, g_a1.generation),
            Err(BrokerError::GenerationMismatch)
        );
        assert_eq!(table.partition_stats(key_b).unwrap().active_leases, 1);
    }
    table.release(g_b.slot_index, g_b.generation).unwrap();
    assert_eq!(table.partition_stats(key_b).unwrap().active_leases, 0);

    // Partition A waiter wakes promptly while Partition B also holds/releases leases.
    let held_a = table
        .acquire_in(part_a, 8001, 0, Duration::from_millis(50), key_a)
        .unwrap();
    let held_b = table
        .acquire_in(part_b, 8002, 0, Duration::from_millis(50), key_b)
        .unwrap();

    let t_wait = Arc::clone(&table);
    let ready = Arc::new(std::sync::Barrier::new(2));
    let ready_worker = Arc::clone(&ready);
    let waiter = std::thread::spawn(move || {
        ready_worker.wait();
        t_wait.acquire_in(part_a, 8003, 0, Duration::from_secs(2), key_a)
    });

    ready.wait();
    let spin_start = Instant::now();
    while table.stats().waiting_threads == 0 {
        assert!(spin_start.elapsed() < Duration::from_secs(1));
        std::thread::yield_now();
    }
    table.release(held_b.slot_index, held_b.generation).unwrap();
    table.release(held_a.slot_index, held_a.generation).unwrap();

    let acquired = waiter.join().unwrap().unwrap();
    assert_eq!(acquired.worker_pid, 8003);
    table
        .release(acquired.slot_index, acquired.generation)
        .unwrap();
}
