//! Integration tests for `vertebra_core::batch::BatchArena` and `BatchNode`.

use vertebra_core::batch::{BatchArena, BatchError, BatchNode};
use vertebra_core::proxy::{HttpMethod, RequestInfo};

fn make_request_info(correlation_id: u64) -> RequestInfo {
    RequestInfo {
        method: HttpMethod::GET,
        _pad: [0u8; 7],
        handler_id: 42,
        path_offset: 0,
        path_len: 1,
        headers_offset: 0,
        body_offset: 0,
        body_len: 0,
        correlation_id,
    }
}

#[test]
fn arena_alloc_layout() {
    // BatchNode must be 128 bytes (RequestInfo=64 + fields + alignment padding)
    // and 64-byte aligned.
    assert_eq!(std::mem::size_of::<BatchNode>(), 128);
    assert_eq!(std::mem::align_of::<BatchNode>(), 64);
}

#[test]
fn arena_push_returns_distinct_ptrs() {
    let arena = BatchArena::new(4).unwrap();
    let mut ptrs = Vec::new();
    for i in 0..4 {
        let p = arena
            .push(BatchNode::new(
                make_request_info(i),
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                i,
            ))
            .unwrap();
        ptrs.push(p);
    }
    // All pointers must be unique.
    for i in 0..ptrs.len() {
        for j in (i + 1)..ptrs.len() {
            assert_ne!(ptrs[i], ptrs[j], "pointers {i} and {j} must differ");
        }
    }
    // Pointers must be 64-byte aligned.
    for (i, p) in ptrs.iter().enumerate() {
        assert_eq!(
            (*p as usize) % 64,
            0,
            "pointer {i} is not 64-byte aligned: {p:p}"
        );
    }
}

#[test]
fn arena_preserves_correlation_ids() {
    let arena = BatchArena::new(8).unwrap();
    for i in 0..8u64 {
        arena
            .push(BatchNode::new(
                make_request_info(i),
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                i,
            ))
            .unwrap();
    }
    let slice = unsafe { arena.as_slice() };
    for (i, node) in slice.iter().enumerate() {
        assert_eq!(node.correlation_id, i as u64);
    }
}

#[test]
fn arena_full_error() {
    let arena = BatchArena::new(1).unwrap();
    arena
        .push(BatchNode::new(
            make_request_info(0),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            0,
        ))
        .unwrap();
    let err = arena
        .push(BatchNode::new(
            make_request_info(1),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            1,
        ))
        .unwrap_err();
    assert!(
        matches!(err, BatchError::Full { capacity: 1 }),
        "expected Full{{capacity:1}}, got {err:?}"
    );
}

#[test]
fn arena_reset_recycles_slots() {
    let arena = BatchArena::new(2).unwrap();
    let p1 = arena
        .push(BatchNode::new(
            make_request_info(10),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            10,
        ))
        .unwrap();
    arena.reset();
    let p2 = arena
        .push(BatchNode::new(
            make_request_info(20),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            20,
        ))
        .unwrap();
    // After reset, first allocation should be at the same address.
    assert_eq!(p1, p2, "reset should recycle slot 0");
    // And the data should be the new node's data.
    let node = unsafe { &*p2 };
    assert_eq!(node.correlation_id, 20);
}

#[test]
fn arena_len_tracking() {
    let arena = BatchArena::new(10).unwrap();
    assert_eq!(arena.len(), 0);
    for i in 0..5 {
        arena
            .push(BatchNode::new(
                make_request_info(i),
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                i,
            ))
            .unwrap();
        assert_eq!(arena.len(), i as usize + 1);
    }
}
